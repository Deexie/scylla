/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cdc/generation_service.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/boot_strapper.hh"
#include "gms/gossiper.hh"
#include "node_ops/task_manager_module.hh"
#include "service/raft/raft_group0.hh"
#include "service/storage_service.hh"
#include "supervisor.hh"

using versioned_value = gms::versioned_value;

extern logging::logger cdc_log;
namespace node_ops {

node_ops_task_impl::node_ops_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id id,
        unsigned sequence_number,
        std::string scope,
        std::string entity,
        tasks::task_id parent_id,
        streaming::stream_reason reason,
        service::storage_service& ss) noexcept
    : tasks::task_manager::task::impl(std::move(module), id, sequence_number,
        std::move(scope), "", "", std::move(entity), parent_id)
    , _reason(reason)
    , _ss(ss)
{
    // FIXME: add progress units
}

std::string node_ops_task_impl::type() const {
    return fmt::format("{}", _reason);
}

join_token_ring_task_impl::join_token_ring_task_impl(tasks::task_manager::module_ptr module,
        std::string entity,
        service::storage_service& ss,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<service::storage_proxy>& proxy,
        std::unordered_set<gms::inet_address> initial_contact_nodes,
        std::unordered_set<gms::inet_address> loaded_endpoints,
        std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
        std::chrono::milliseconds delay) noexcept
    : node_ops_task_impl(std::move(module), tasks::task_id::create_random_id(), ss.get_task_manager_module().new_sequence_number(),
        "coordinator node", std::move(entity), tasks::task_id::create_null_id(), streaming::stream_reason::bootstrap, ss)
    , _sys_dist_ks(sys_dist_ks)
    , _proxy(proxy)
    , _initial_contact_nodes(std::move(initial_contact_nodes))
    , _loaded_endpoints(std::move(loaded_endpoints))
    , _loaded_peer_features(std::move(loaded_peer_features))
    , _delay(delay)
{}

gms::inet_address join_token_ring_task_impl::get_broadcast_address() const {
    return _ss.get_broadcast_address();
}

bool join_token_ring_task_impl::should_bootstrap() {
    return _ss.should_bootstrap();
}

bool join_token_ring_task_impl::is_replacing() {
    return _ss.is_replacing();
}

locator::token_metadata_ptr join_token_ring_task_impl::get_token_metadata_ptr() const noexcept {
    return _ss.get_token_metadata_ptr();
}
const locator::token_metadata& join_token_ring_task_impl::get_token_metadata() const noexcept {
    return _ss.get_token_metadata();
}

future<> join_token_ring_task_impl::run() {
    auto& _sys_ks = _ss._sys_ks;
    auto& _db = _ss._db;
    auto& _gossiper = _ss._gossiper;
    auto& _raft_topology_change_enabled = _ss._raft_topology_change_enabled;
    auto& _snitch = _ss._snitch;
    auto& _feature_service = _ss._feature_service;
    auto& _group0 = _ss._group0;
    std::unordered_set<dht::token> bootstrap_tokens;
    std::map<gms::application_state, gms::versioned_value> app_states;
    /* The timestamp of the CDC streams generation that this node has proposed when joining.
     * This value is nullopt only when:
     * 1. this node is being upgraded from a non-CDC version,
     * 2. this node is starting for the first time or restarting with CDC previously disabled,
     *    in which case the value should become populated before we leave the join_token_ring procedure.
     *
     * Important: this variable is using only during the startup procedure. It is moved out from
     * at the end of `join_token_ring`; the responsibility handling of CDC generations is passed
     * to cdc::generation_service.
     *
     * DO NOT use this variable after `join_token_ring` (i.e. after we call `generation_service::after_join`
     * and pass it the ownership of the timestamp.
     */
    std::optional<cdc::generation_id> cdc_gen_id;

    if (_sys_ks.local().was_decommissioned()) {
        if (_db.local().get_config().override_decommission() && !_db.local().get_config().consistent_cluster_management()) {
            tasks::tmlogger.warn("This node was decommissioned, but overriding by operator request.");
            co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        } else {
            auto msg = sstring("This node was decommissioned and will not rejoin the ring unless override_decommission=true has been set and consistent cluster management is not in use,"
                               "or all existing data is removed and the node is bootstrapped again");
            tasks::tmlogger.error("{}", msg);
            throw std::runtime_error(msg);
        }
    }

    bool replacing_a_node_with_same_ip = false;
    bool replacing_a_node_with_diff_ip = false;
    std::optional<service::storage_service::replacement_info> ri;
    std::optional<gms::inet_address> replace_address;
    std::optional<locator::host_id> replaced_host_id;
    std::optional<service::raft_group0::replace_info> raft_replace_info;
    auto tmlock = std::make_unique<locator::token_metadata_lock>(co_await _ss.get_token_metadata_lock());
    auto tmptr = co_await _ss.get_mutable_token_metadata_ptr();
    if (is_replacing()) {
        if (_sys_ks.local().bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        ri = co_await _ss.prepare_replacement_info(_initial_contact_nodes, _loaded_peer_features);
        replace_address = ri->address;
        raft_replace_info = service::raft_group0::replace_info {
            .ip_addr = *replace_address,
            .raft_id = raft::server_id{ri->host_id.uuid()},
        };
        if (!_raft_topology_change_enabled) {
            bootstrap_tokens = std::move(ri->tokens);
            replacing_a_node_with_same_ip = *replace_address == get_broadcast_address();
            replacing_a_node_with_diff_ip = *replace_address != get_broadcast_address();

            tasks::tmlogger.info("Replacing a node with {} IP address, my address={}, node being replaced={}",
                get_broadcast_address() == *replace_address ? "the same" : "a different",
                get_broadcast_address(), *replace_address);
            tmptr->update_topology(*replace_address, std::move(ri->dc_rack), locator::node::state::being_replaced);
            co_await tmptr->update_normal_tokens(bootstrap_tokens, *replace_address);
            replaced_host_id = ri->host_id;
        }
    } else if (should_bootstrap()) {
        co_await _ss.check_for_endpoint_collision(_initial_contact_nodes, _loaded_peer_features);
    } else {
        auto local_features = _feature_service.supported_feature_set();
        tasks::tmlogger.info("Checking remote features with gossip, initial_contact_nodes={}", _initial_contact_nodes);
        co_await _gossiper.do_shadow_round(_initial_contact_nodes);
        _gossiper.check_knows_remote_features(local_features, _loaded_peer_features);
        _gossiper.check_snitch_name_matches(_snitch.local()->get_name());
        // Check if the node is already removed from the cluster
        auto local_host_id = get_token_metadata().get_my_id();
        auto my_ip = get_broadcast_address();
        if (!_gossiper.is_safe_for_restart(my_ip, local_host_id)) {
            throw std::runtime_error(::format("The node {} with host_id {} is removed from the cluster. Can not restart the removed node to join the cluster again!",
                    my_ip, local_host_id));
        }
        co_await _gossiper.reset_endpoint_state_map();
        for (auto ep : _loaded_endpoints) {
            co_await _gossiper.add_saved_endpoint(ep);
        }
    }
    auto features = _feature_service.supported_feature_set();
    tasks::tmlogger.info("Save advertised features list in the 'system.{}' table", db::system_keyspace::LOCAL);
    // Save the advertised feature set to system.local table after
    // all remote feature checks are complete and after gossip shadow rounds are done.
    // At this point, the final feature set is already determined before the node joins the ring.
    co_await _sys_ks.local().save_local_supported_features(features);

    // If this is a restarting node, we should update tokens before gossip starts
    auto my_tokens = co_await _sys_ks.local().get_saved_tokens();
    bool restarting_normal_node = _sys_ks.local().bootstrap_complete() && !is_replacing() && !my_tokens.empty();
    if (restarting_normal_node) {
        tasks::tmlogger.info("Restarting a node in NORMAL status");
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore we update _token_metadata now, before gossip starts.
        tmptr->update_topology(get_broadcast_address(), _snitch.local()->get_location(), locator::node::state::normal);
        co_await tmptr->update_normal_tokens(my_tokens, get_broadcast_address());

        cdc_gen_id = co_await _sys_ks.local().get_cdc_generation_id();
        if (!cdc_gen_id) {
            // We could not have completed joining if we didn't generate and persist a CDC streams timestamp,
            // unless we are restarting after upgrading from non-CDC supported version.
            // In that case we won't begin a CDC generation: it should be done by one of the nodes
            // after it learns that it everyone supports the CDC feature.
            cdc_log.warn(
                    "Restarting node in NORMAL status with CDC enabled, but no streams timestamp was proposed"
                    " by this node according to its local tables. Are we upgrading from a non-CDC supported version?");
        }
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = get_token_metadata().get_my_id();
    if (!replacing_a_node_with_diff_ip) {
        auto endpoint = get_broadcast_address();
        auto eps = _gossiper.get_endpoint_state_ptr(endpoint);
        if (eps) {
            auto replace_host_id = _gossiper.get_host_id(get_broadcast_address());
            tasks::tmlogger.info("Host {}/{} is replacing {}/{} using the same address", local_host_id, endpoint, replace_host_id, endpoint);
        }
        tmptr->update_host_id(local_host_id, get_broadcast_address());
    }

    // Replicate the tokens early because once gossip runs other nodes
    // might send reads/writes to this node. Replicate it early to make
    // sure the tokens are valid on all the shards.
    co_await _ss.replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    // Ensure we know our own actual Schema UUID in preparation for updates
    co_await db::schema_tables::recalculate_schema_version(_sys_ks, _proxy, _feature_service);

    app_states.emplace(gms::application_state::NET_VERSION, versioned_value::network_version());
    app_states.emplace(gms::application_state::HOST_ID, versioned_value::host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, versioned_value::rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, versioned_value::release_version());
    app_states.emplace(gms::application_state::SUPPORTED_FEATURES, versioned_value::supported_features(features));
    app_states.emplace(gms::application_state::CACHE_HITRATES, versioned_value::cache_hitrates(""));
    app_states.emplace(gms::application_state::SCHEMA_TABLES_VERSION, versioned_value(db::schema_tables::version));
    app_states.emplace(gms::application_state::RPC_READY, versioned_value::cql_ready(false));
    app_states.emplace(gms::application_state::VIEW_BACKLOG, versioned_value(""));
    app_states.emplace(gms::application_state::SCHEMA, versioned_value::schema(_db.local().get_version()));
    if (restarting_normal_node) {
        // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
        // Exception: there might be no CDC streams timestamp proposed by us if we're upgrading from a non-CDC version.
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(my_tokens));
        app_states.emplace(gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id));
        app_states.emplace(gms::application_state::STATUS, versioned_value::normal(my_tokens));
    }
    if (replacing_a_node_with_same_ip || replacing_a_node_with_diff_ip) {
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens));
    }
    app_states.emplace(gms::application_state::SNITCH_NAME, versioned_value::snitch_name(_snitch.local()->get_name()));
    app_states.emplace(gms::application_state::SHARD_COUNT, versioned_value::shard_count(smp::count));
    app_states.emplace(gms::application_state::IGNORE_MSB_BITS, versioned_value::ignore_msb_bits(_db.local().get_config().murmur3_partitioner_ignore_msb_bits()));

    for (auto&& s : _snitch.local()->get_app_states()) {
        app_states.emplace(s.first, std::move(s.second));
    }

    auto schema_change_announce = _db.local().observable_schema_version().observe([&ss = _ss] (table_schema_version schema_version) mutable {
        ss._migration_manager.local().passive_announce(std::move(schema_version));
    });

    _ss._listeners.emplace_back(make_lw_shared(std::move(schema_change_announce)));

    tasks::tmlogger.info("Starting up server gossip");

    auto generation_number = gms::generation_type(co_await _sys_ks.local().increment_and_get_generation());
    auto advertise = gms::advertise_myself(!replacing_a_node_with_same_ip);
    co_await _gossiper.start_gossiping(generation_number, app_states, advertise);

    if (!_raft_topology_change_enabled && should_bootstrap()) {
        // Wait for NORMAL state handlers to finish for existing nodes now, so that connection dropping
        // (happening at the end of `handle_state_normal`: `notify_joined`) doesn't interrupt
        // group 0 joining or repair. (See #12764, #12956, #12972, #13302)
        //
        // But before we can do that, we must make sure that gossip sees at least one other node
        // and fetches the list of peers from it; otherwise `wait_for_normal_state_handled_on_boot`
        // may trivially finish without waiting for anyone.
        co_await _gossiper.wait_for_live_nodes_to_show_up(2);

        // Note: in Raft topology mode this is unnecessary.
        // Node state changes are propagated to the cluster through explicit global barriers.
        co_await _ss.wait_for_normal_state_handled_on_boot();

        // NORMAL doesn't necessarily mean UP (#14042). Wait for these nodes to be UP as well
        // to reduce flakiness (we need them to be UP to perform CDC generation write and for repair/streaming).
        //
        // This could be done in Raft topology mode as well, but the calculation of nodes to sync with
        // has to be done based on topology state machine instead of gossiper as it is here;
        // furthermore, the place in the code where we do this has to be different (it has to be coordinated
        // by the topology coordinator after it joins the node to the cluster).
        //
        // We calculate nodes to wait for based on token_metadata. Previously we would use gossiper
        // directly for this, but gossiper may still contain obsolete entries from 1. replaced nodes
        // and 2. nodes that have changed their IPs; these entries are eventually garbage-collected,
        // but here they may still be present if we're performing topology changes in quick succession.
        // `token_metadata` has all host ID / token collisions resolved so in particular it doesn't contain
        // these obsolete IPs. Refs: #14487, #14468
        auto& tm = get_token_metadata();
        auto ignore_nodes = ri
                ? _ss.parse_node_list(_db.local().get_config().ignore_dead_nodes_for_replace(), tm)
                // TODO: specify ignore_nodes for bootstrap
                : std::unordered_set<gms::inet_address>{};

        std::vector<gms::inet_address> sync_nodes;
        tm.get_topology().for_each_node([&] (const locator::node* np) {
            auto ep = np->endpoint();
            if (!ignore_nodes.contains(ep) && (!ri || ep != ri->address)) {
                sync_nodes.push_back(ep);
            }
        });

        tasks::tmlogger.info("Waiting for nodes {} to be alive", sync_nodes);
        co_await _gossiper.wait_alive(sync_nodes, std::chrono::seconds{30});
        tasks::tmlogger.info("Nodes {} are alive", sync_nodes);
    }

    assert(_group0);
    // if the node is bootstrapped the functin will do nothing since we already created group0 in main.cc
    co_await _group0->setup_group0(_sys_ks.local(), _initial_contact_nodes, raft_replace_info, _ss, *_ss._qp, _ss._migration_manager.local());

    raft::server* raft_server = co_await [&ss = _ss] () -> future<raft::server*> {
        if (!ss._raft_topology_change_enabled) {
            co_return nullptr;
        } else if (ss._sys_ks.local().bootstrap_complete()) {
            auto [upgrade_lock_holder, upgrade_state] = co_await ss._group0->client().get_group0_upgrade_state();
            co_return upgrade_state == service::group0_upgrade_state::use_post_raft_procedures ? &ss._group0->group0_server() : nullptr;
        } else {
            auto upgrade_state = (co_await ss._group0->client().get_group0_upgrade_state()).second;
            if (upgrade_state != service::group0_upgrade_state::use_post_raft_procedures) {
                on_internal_error(tasks::tmlogger, "raft topology: cluster not upgraded to use group 0 after setup_group0");
            }
            co_return &ss._group0->group0_server();
        }
    } ();

    co_await _gossiper.wait_for_gossip_to_settle();
    // TODO: Look at the group 0 upgrade state and use it to decide whether to attach or not
    if (!_raft_topology_change_enabled) {
        co_await _feature_service.enable_features_on_join(_gossiper, _sys_ks.local());
    }

    _ss.set_mode(service::storage_service::mode::JOINING);

    if (raft_server) { // Raft is enabled. Check if we need to bootstrap ourself using raft
        tasks::tmlogger.info("topology changes are using raft");

        // start topology coordinator fiber
        _ss._raft_state_monitor = _ss.raft_state_monitor_fiber(*raft_server, _sys_dist_ks);

        // Need to start system_distributed_keyspace before bootstrap because bootstraping
        // process may access those tables.
        supervisor::notify("starting system distributed keyspace");
        co_await _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);

        if (is_replacing()) {
            assert(raft_replace_info);
            co_await _ss.raft_replace(*raft_server, raft_replace_info->raft_id, raft_replace_info->ip_addr);
        } else {
            co_await _ss.raft_bootstrap(*raft_server);
        }

        // Wait until we enter one of the final states
        co_await _ss._topology_state_machine.event.when([&ss = _ss, raft_server] {
            return ss._topology_state_machine._topology.normal_nodes.contains(raft_server->id()) ||
            ss._topology_state_machine._topology.left_nodes.contains(raft_server->id());
        });

        if (_ss._topology_state_machine._topology.left_nodes.contains(raft_server->id())) {
            throw std::runtime_error("A node that already left the cluster cannot be restarted");
        }

        co_await _ss.update_topology_with_local_metadata(*raft_server);

        // Node state is enough to know that bootstrap has completed, but to make legacy code happy
        // let it know that the bootstrap is completed as well
        co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        _ss.set_mode(service::storage_service::mode::NORMAL);

        if (get_token_metadata().sorted_tokens().empty()) {
            auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
            tasks::tmlogger.error("{}", err);
            throw std::runtime_error(err);
        }

        co_await _group0->finish_setup_after_join(_ss, *_ss._qp, _ss._migration_manager.local());
        co_return;
    }

    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    if (should_bootstrap()) {
        bool resume_bootstrap = _sys_ks.local().bootstrap_in_progress();
        if (resume_bootstrap) {
            tasks::tmlogger.warn("Detected previous bootstrap failure; retrying");
        } else {
            co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS);
        }
        tasks::tmlogger.info("waiting for ring information");

        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        co_await _ss.wait_for_ring_to_settle();

        if (!replace_address) {
            auto tmptr = get_token_metadata_ptr();

            if (tmptr->is_normal_token_owner(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            tasks::tmlogger.info("getting bootstrap token");
            if (resume_bootstrap) {
                bootstrap_tokens = co_await _sys_ks.local().get_saved_tokens();
                if (!bootstrap_tokens.empty()) {
                    tasks::tmlogger.info("Using previously saved tokens = {}", bootstrap_tokens);
                } else {
                    bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(tmptr, _db.local().get_config(), dht::check_token_endpoint::yes);
                }
            } else {
                bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(tmptr, _db.local().get_config(), dht::check_token_endpoint::yes);
            }
        } else {
            if (*replace_address != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                tasks::tmlogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(2 * _ss.get_ring_delay(), _ss._abort_source);

                // check for operator errors...
                const auto tmptr = get_token_metadata_ptr();
                for (auto token : bootstrap_tokens) {
                    auto existing = tmptr->get_endpoint(token);
                    if (existing) {
                        auto eps = _gossiper.get_endpoint_state_ptr(*existing);
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - _delay) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                    } else {
                        throw std::runtime_error(::format("Cannot replace token {} which does not exist!", token));
                    }
                }
            } else {
                tasks::tmlogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(_ss.get_ring_delay(), _ss._abort_source);
            }
            tasks::tmlogger.info("Replacing a node with token(s): {}", bootstrap_tokens);
            // bootstrap_tokens was previously set using tokens gossiped by the replaced node
        }
        co_await _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        co_await _ss.mark_existing_views_as_built(_sys_dist_ks);
        co_await _sys_ks.local().update_tokens(bootstrap_tokens);
        co_await _ss.bootstrap(bootstrap_tokens, cdc_gen_id, ri);
    } else {
        supervisor::notify("starting system distributed keyspace");
        co_await _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        bootstrap_tokens = co_await _sys_ks.local().get_saved_tokens();
        if (bootstrap_tokens.empty()) {
            bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(get_token_metadata_ptr(), _db.local().get_config(), dht::check_token_endpoint::no);
            co_await _sys_ks.local().update_tokens(bootstrap_tokens);
        } else {
            size_t num_tokens = _db.local().get_config().num_tokens();
            if (bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(::format("Cannot change the number of tokens from {:d} to {:d}", bootstrap_tokens.size(), num_tokens));
            } else {
                tasks::tmlogger.info("Using saved tokens {}", bootstrap_tokens);
            }
        }
    }

    tasks::tmlogger.debug("Setting tokens to {}", bootstrap_tokens);
    co_await _ss.mutate_token_metadata([&ss = _ss, &bootstrap_tokens] (locator::mutable_token_metadata_ptr tmptr) {
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore, in case we haven't updated _token_metadata with our tokens yet, do it now.
        tmptr->update_topology(ss.get_broadcast_address(), ss._snitch.local()->get_location(), locator::node::state::normal);
        return tmptr->update_normal_tokens(bootstrap_tokens, ss.get_broadcast_address());
    });

    if (!_sys_ks.local().bootstrap_complete()) {
        // If we're not bootstrapping then we shouldn't have chosen a CDC streams timestamp yet.
        assert(should_bootstrap() || !cdc_gen_id);

        // Don't try rewriting CDC stream description tables.
        // See cdc.md design notes, `Streams description table V1 and rewriting` section, for explanation.
        co_await _sys_ks.local().cdc_set_rewritten(std::nullopt);
    }

    if (!cdc_gen_id) {
        // If we didn't observe any CDC generation at this point, then either
        // 1. we're replacing a node,
        // 2. we've already bootstrapped, but are upgrading from a non-CDC version,
        // 3. we're the first node, starting a fresh cluster.

        // In the replacing case we won't create any CDC generation: we're not introducing any new tokens,
        // so the current generation used by the cluster is fine.

        // In the case of an upgrading cluster, one of the nodes is responsible for creating
        // the first CDC generation. We'll check if it's us.

        // Finally, if we're the first node, we'll create the first generation.

        if (!is_replacing()
                && (!_sys_ks.local().bootstrap_complete()
                    || cdc::should_propose_first_generation(get_broadcast_address(), _gossiper))) {
            try {
                cdc_gen_id = co_await _ss._cdc_gens.local().legacy_make_new_generation(bootstrap_tokens, !_ss.is_first_node());
            } catch (...) {
                cdc_log.warn(
                    "Could not create a new CDC generation: {}. This may make it impossible to use CDC or cause performance problems."
                    " Use nodetool checkAndRepairCdcStreams to fix CDC.", std::current_exception());
            }
        }
    }

    // Persist the CDC streams timestamp before we persist bootstrap_state = COMPLETED.
    if (cdc_gen_id) {
        co_await _sys_ks.local().update_cdc_generation_id(*cdc_gen_id);
    }
    // If we crash now, we will choose a new CDC streams timestamp anyway (because we will also choose a new set of tokens).
    // But if we crash after setting bootstrap_state = COMPLETED, we will keep using the persisted CDC streams timestamp after restarting.

    co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
    // At this point our local tokens and CDC streams timestamp are chosen (bootstrap_tokens, cdc_gen_id) and will not be changed.

    // start participating in the ring.
    co_await service::set_gossip_tokens(_gossiper, bootstrap_tokens, cdc_gen_id);

    _ss.set_mode(service::storage_service::mode::NORMAL);

    if (get_token_metadata().sorted_tokens().empty()) {
        auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
        tasks::tmlogger.error("{}", err);
        throw std::runtime_error(err);
    }

    assert(_group0);
    co_await _group0->finish_setup_after_join(_ss, *_ss._qp, _ss._migration_manager.local());
    co_await _ss._cdc_gens.local().after_join(std::move(cdc_gen_id));
}

}
