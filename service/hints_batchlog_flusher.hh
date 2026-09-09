/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include "gc_clock.hh"
#include "locator/host_id.hh"
#include "utils/updateable_value.hh"
#include "seastarx.hh"

namespace gms { class gossiper; }
namespace netw { class messaging_service; }
namespace locator { class shared_token_metadata; }

namespace service {

// Flushes hints and batchlog on all nodes on behalf of tablet repairs, so
// that the topology coordinator flushes once and hands the flush time to
// the repairs it starts instead of each repair asking every node itself.
class hints_batchlog_flusher {
public:
    // What the flusher asks of the cluster. Tests supply their own.
    class cluster {
    public:
        virtual ~cluster() = default;
        virtual std::unordered_set<locator::host_id> nodes() const = 0;
        virtual bool is_alive(locator::host_id node) const = 0;
        virtual future<gc_clock::time_point> flush(locator::host_id node) = 0;
    };

private:
    std::unique_ptr<cluster> _cluster;
    utils::updateable_value<uint32_t> _flush_cache_time_in_ms;

    std::unordered_map<locator::host_id, gc_clock::time_point> _flush_times;
    std::optional<shared_future<std::optional<gc_clock::time_point>>> _in_progress;
    named_gate _gate;

public:
    hints_batchlog_flusher(std::unique_ptr<cluster> cluster, utils::updateable_value<uint32_t> flush_cache_time_in_ms);
    hints_batchlog_flusher(gms::gossiper& gossiper, netw::messaging_service& messaging, const locator::shared_token_metadata& shared_tm,
            utils::updateable_value<uint32_t> flush_cache_time_in_ms);

    // Returns the time of a flush recent enough for a repair started now,
    // running one if needed. Concurrent callers share the same flush.
    // nullopt if a node that needs flushing is down or does not respond.
    future<std::optional<gc_clock::time_point>> flush_time();

    future<> stop();

private:
    std::chrono::milliseconds cache_time() const;
    bool is_fresh(locator::host_id node, gc_clock::time_point now) const;
    gc_clock::time_point earliest_flush_time(const std::unordered_set<locator::host_id>& nodes) const;
    // Flushes the nodes whose flush time is not fresh.
    future<std::optional<gc_clock::time_point>> run_flush();
};

} // namespace service
