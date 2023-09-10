/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "streaming/stream_reason.hh"
#include "tasks/task_manager.hh"

namespace service {
class storage_service;
}

namespace node_ops {

class node_ops_task_impl : public tasks::task_manager::task::impl {
protected:
    streaming::stream_reason _reason;
    service::storage_service& _ss;
public:
    node_ops_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            streaming::stream_reason reason,
            service::storage_service& ss) noexcept;

    virtual std::string type() const override;
protected:
    virtual future<> run() override = 0;
};

class booststrap_node_task_impl : public node_ops_task_impl {
public:
    booststrap_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::bootstrap, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class replace_node_task_impl : public node_ops_task_impl {
public:
    replace_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::replace, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class rebuild_node_task_impl : public node_ops_task_impl {
public:
    rebuild_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::rebuild, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class decommission_node_task_impl : public node_ops_task_impl {
public:
    decommission_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::decommission, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class remove_node_task_impl : public node_ops_task_impl {
public:
    remove_node_task_impl(tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::removenode, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "node_ops") {}
};

}
