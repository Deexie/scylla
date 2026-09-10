/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <chrono>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "service/hints_batchlog_flusher.hh"
#include "utils/updateable_value.hh"

BOOST_AUTO_TEST_SUITE(hints_batchlog_flusher_test)

using namespace service;
using namespace std::chrono_literals;

namespace {

constexpr uint32_t one_minute_in_ms = 60 * 1000;

// A cluster the test scripts: which nodes exist, which are down, and what
// each flush returns. Every flush is counted.
class fake_cluster : public hints_batchlog_flusher::cluster {
public:
    std::unordered_set<locator::host_id> members;
    std::unordered_set<locator::host_id> down;
    std::unordered_map<locator::host_id, unsigned> flushes;
    std::function<future<gc_clock::time_point>(locator::host_id)> on_flush = [] (locator::host_id) {
        return make_ready_future<gc_clock::time_point>(gc_clock::now());
    };

    std::unordered_set<locator::host_id> nodes() const override { return members; }
    bool is_alive(locator::host_id node) const override { return !down.contains(node); }
    future<gc_clock::time_point> flush(locator::host_id node) override {
        ++flushes[node];
        return on_flush(node);
    }

    unsigned total_flushes() const {
        unsigned n = 0;
        for (const auto& [_, count] : flushes) {
            n += count;
        }
        return n;
    }
};

std::vector<locator::host_id> make_nodes(unsigned count) {
    std::vector<locator::host_id> nodes;
    for (unsigned i = 0; i < count; ++i) {
        nodes.push_back(locator::host_id::create_random_id());
    }
    return nodes;
}

// The flusher owns the cluster; the test keeps a reference to script it.
struct fixture {
    fake_cluster& cluster;
    hints_batchlog_flusher flusher;

    fixture(std::unique_ptr<fake_cluster> c, utils::updateable_value<uint32_t> cache_time_in_ms)
        : cluster(*c)
        , flusher(std::move(c), std::move(cache_time_in_ms))
    {
    }
    fixture(std::unique_ptr<fake_cluster> c, uint32_t cache_time_in_ms = one_minute_in_ms)
        : fixture(std::move(c), utils::updateable_value<uint32_t>(cache_time_in_ms))
    {
    }
};

std::unique_ptr<fake_cluster> make_cluster(const std::vector<locator::host_id>& nodes) {
    auto c = std::make_unique<fake_cluster>();
    c->members.insert(nodes.begin(), nodes.end());
    return c;
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_first_round_flushes_every_node_and_returns_earliest_time) {
    auto nodes = make_nodes(3);
    fixture f(make_cluster(nodes));
    auto base = gc_clock::now();
    std::unordered_map<locator::host_id, gc_clock::time_point> reported;
    for (unsigned i = 0; i < nodes.size(); ++i) {
        reported[nodes[i]] = base + std::chrono::seconds(i);
    }
    f.cluster.on_flush = [&] (locator::host_id node) { return make_ready_future<gc_clock::time_point>(reported[node]); };

    auto time = co_await f.flusher.flush_time();
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE(*time == base);
    for (const auto& node : nodes) {
        BOOST_REQUIRE_EQUAL(f.cluster.flushes[node], 1u);
    }

    // Everything is fresh now, so nothing is flushed and the answer is the same.
    auto again = co_await f.flusher.flush_time();
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), nodes.size());
    BOOST_REQUIRE(*again == base);
    co_await f.flusher.stop();
}

SEASTAR_TEST_CASE(test_concurrent_callers_share_one_round) {
    auto nodes = make_nodes(3);
    fixture f(make_cluster(nodes));
    std::unordered_map<locator::host_id, promise<gc_clock::time_point>> pending;
    f.cluster.on_flush = [&] (locator::host_id node) { return pending[node].get_future(); };

    std::vector<future<std::optional<gc_clock::time_point>>> callers;
    for (int i = 0; i < 5; ++i) {
        callers.push_back(f.flusher.flush_time());
    }
    // Every caller is waiting on a flush that has not completed. Only one
    // round is running: each node was asked once.
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), nodes.size());
    for (auto& caller : callers) {
        BOOST_REQUIRE(!caller.available());
    }

    auto flushed_at = gc_clock::now();
    for (auto& [node, p] : pending) {
        p.set_value(flushed_at);
    }
    for (auto& caller : callers) {
        auto time = co_await std::move(caller);
        BOOST_REQUIRE(time.has_value());
        BOOST_REQUIRE(*time == flushed_at);
    }
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), nodes.size());
    co_await f.flusher.stop();
}

SEASTAR_TEST_CASE(test_only_nodes_with_an_expired_time_are_flushed_again) {
    auto nodes = make_nodes(3);
    fixture f(make_cluster(nodes));
    // The first node reports a flush older than the cache time, the others a
    // fresh one, as a node with a slow receiver-side flush would.
    auto stale = nodes[0];
    f.cluster.on_flush = [&] (locator::host_id node) {
        return make_ready_future<gc_clock::time_point>(node == stale ? gc_clock::now() - 2h : gc_clock::now());
    };
    auto first = co_await f.flusher.flush_time();
    BOOST_REQUIRE(first.has_value());
    BOOST_REQUIRE(*first <= gc_clock::now() - 2h);

    auto fresh = gc_clock::now();
    f.cluster.on_flush = [&] (locator::host_id) { return make_ready_future<gc_clock::time_point>(fresh); };
    auto second = co_await f.flusher.flush_time();
    BOOST_REQUIRE_EQUAL(f.cluster.flushes[stale], 2u);
    BOOST_REQUIRE_EQUAL(f.cluster.flushes[nodes[1]], 1u);
    BOOST_REQUIRE_EQUAL(f.cluster.flushes[nodes[2]], 1u);
    // The earliest time is now one of the fresh ones from the first round.
    BOOST_REQUIRE(second.has_value());
    BOOST_REQUIRE(*second > *first);
    BOOST_REQUIRE(*second <= fresh);
    co_await f.flusher.stop();
}

SEASTAR_TEST_CASE(test_down_node_yields_no_time_without_flushing_anything) {
    auto nodes = make_nodes(3);
    fixture f(make_cluster(nodes));
    f.cluster.down.insert(nodes[2]);

    auto time = co_await f.flusher.flush_time();
    BOOST_REQUIRE(!time.has_value());
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), 0u);

    f.cluster.down.clear();
    time = co_await f.flusher.flush_time();
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), nodes.size());

    // A node that went down after reporting a fresh flush does not matter.
    f.cluster.down.insert(nodes[2]);
    auto again = co_await f.flusher.flush_time();
    BOOST_REQUIRE(again.has_value());
    BOOST_REQUIRE(*again == *time);
    co_await f.flusher.stop();
}

SEASTAR_TEST_CASE(test_failed_flush_yields_no_time_but_keeps_the_others) {
    auto nodes = make_nodes(3);
    fixture f(make_cluster(nodes));
    auto failing = nodes[2];
    f.cluster.on_flush = [&] (locator::host_id node) {
        if (node == failing) {
            return make_exception_future<gc_clock::time_point>(std::runtime_error("no reply"));
        }
        return make_ready_future<gc_clock::time_point>(gc_clock::now());
    };

    auto time = co_await f.flusher.flush_time();
    BOOST_REQUIRE(!time.has_value());
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), nodes.size());

    // Only the node that failed is asked again.
    f.cluster.on_flush = [] (locator::host_id) { return make_ready_future<gc_clock::time_point>(gc_clock::now()); };
    time = co_await f.flusher.flush_time();
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE_EQUAL(f.cluster.flushes[failing], 2u);
    BOOST_REQUIRE_EQUAL(f.cluster.flushes[nodes[0]], 1u);
    BOOST_REQUIRE_EQUAL(f.cluster.flushes[nodes[1]], 1u);
    co_await f.flusher.stop();
}

SEASTAR_TEST_CASE(test_node_removed_from_the_cluster_is_forgotten) {
    auto nodes = make_nodes(3);
    fixture f(make_cluster(nodes));
    // The first node reports the earliest, still fresh, time.
    auto leaving = nodes[0];
    auto now = gc_clock::now();
    f.cluster.on_flush = [&] (locator::host_id node) {
        return make_ready_future<gc_clock::time_point>(node == leaving ? now - 30s : now);
    };
    auto with = co_await f.flusher.flush_time();
    BOOST_REQUIRE(with.has_value());
    BOOST_REQUIRE(*with == now - 30s);

    f.cluster.members.erase(leaving);
    auto without = co_await f.flusher.flush_time();
    BOOST_REQUIRE(without.has_value());
    BOOST_REQUIRE(*without == now);
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), nodes.size());
    co_await f.flusher.stop();
}

SEASTAR_TEST_CASE(test_zero_cache_time_flushes_on_every_call_until_raised) {
    auto nodes = make_nodes(2);
    utils::updateable_value_source<uint32_t> cache_time_in_ms(0);
    fixture f(make_cluster(nodes), utils::updateable_value<uint32_t>(cache_time_in_ms));

    co_await f.flusher.flush_time();
    co_await f.flusher.flush_time();
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), 2 * nodes.size());

    // The option is live: raising it makes the last flush fresh.
    cache_time_in_ms.set(one_minute_in_ms);
    co_await f.flusher.flush_time();
    BOOST_REQUIRE_EQUAL(f.cluster.total_flushes(), 2 * nodes.size());
    co_await f.flusher.stop();
}

BOOST_AUTO_TEST_SUITE_END()
