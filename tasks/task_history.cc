/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tasks/task_handler.hh"
#include "tasks/task_manager.hh"


namespace tasks {

class task_manager::virtual_task::history::impl {
public:
    virtual future<std::vector<task_stats>> get_stats() = 0;
    virtual future<std::optional<task_status>> get_status(task_id id) = 0;
    virtual void add_task(virtual_task_status vt) = 0;

    impl(module_ptr module) noexcept;
    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;
    impl(impl&&) = delete;
    impl& operator=(impl&&) = delete;
    virtual ~impl() = default;
};

class default_virtual_task_history : public task_manager::virtual_task::history::impl {
private:
    std::unordered_map<task_id, task_manager::virtual_task::history::virtual_task_status> _statuses;
public:
    future<std::vector<task_stats>> get_stats() override;
    future<std::optional<task_status>> get_status(task_id id) override;
    void add_task(task_manager::virtual_task::history::virtual_task_status vt) override;
};

future<std::vector<task_stats>> default_virtual_task_history::get_stats() {
    return make_ready_future<std::vector<task_stats>>(_statuses | boost::adaptors::transformed([] (const auto& vt_status) {
        auto& [id, status] = vt_status;
        return task_stats{
            .task_id = id,
            .type = status.type,
            .kind = task_kind::cluster,
            .scope = status.scope,
            .state = status.state,
            .keyspace = status.keyspace,
            .table = status.table,
            .entity = status.entity
        };
    }));
}

future<std::optional<task_status>> default_virtual_task_history::get_status(task_id id) {
    // TODO: unregister?
    if (auto it = _statuses.find(id); it != _statuses.end()) {
        auto& [id, status] = *it;
        return make_ready_future<std::optional<task_status>>(task_status{
            .task_id = id,
            .type = status.type,
            .kind = task_kind::cluster,
            .scope = status.scope,
            .state = status.state,
            .is_abortable = is_abortable::no, // TODO 
            .start_time = status.start_time,
            .end_time = status.end_time,
            .error = status.error,
            .keyspace = status.keyspace,
            .table = status.table,
            .entity = status.entity,
            .children = task_manager::virtual_task::impl::
        };
        })
    }
}

void default_virtual_task_history::add_task(task_manager::virtual_task::history::virtual_task_status vt) {

}

future<std::vector<task_stats>> task_manager::virtual_task::history::get_stats() {
    return _impl->get_stats();
}

future<std::optional<task_status>> task_manager::virtual_task::history::get_status(task_id id) {
    return _impl->get_status(id);
}

void task_manager::virtual_task::history::add_task(virtual_task_status vt) {
    return _impl->add_task(std::move(vt));
}

}
