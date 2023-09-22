#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from enum import StrEnum
from typing import NewType, NamedTuple


TaskID = NewType('TaskID', str)
SequenceNum = NewType('SequenceNum', str)

# class S(NamedTuple):
#     """Server id (test local) and IP address"""
#     server_id: ServerNum
#     ip_addr: IPAddress
#     def __str__(self):
#         return f"Server({self.server_id}, {self.ip_addr})"
    
class State(StrEnum):
    created = "created"
    running = "running"
    done = "done"
    failed = "failed"

   
class TaskStats(NamedTuple):
    """Basic stats of Task Manager's tasks"""
    task_id: TaskID
    state: State
    type: str
    scope: str
    keyspace: str
    table: str
    entity: str
    sequence_number: SequenceNum

    # def __init__(self, stats: dict):
    #     for k, v in stats.items():
    #         setattr(self, k, v)  
    # def __init__(self, stats: dict):
    #     self.task_id = stats["task_id"]
    #     self.state = stats["state"]
    #     self.type = stats["type"]
    #     self.scope = stats["scope"]
    #     self.keyspace = stats["keyspace"]
    #     self.table = stats["table"]
    #     self.entity = stats["entity"]
    #     self.sequence_number = stats["sequence_number"]
 

class TaskStatus(NamedTuple):
    """Full status of Task Manager's tasks"""
    id: TaskID
    state: State
    type: str
    scope: str
    keyspace: str
    table: str
    entity: str
    sequence_number: SequenceNum
    is_abortable: bool
    start_time: str
    end_time: str
    error: str
    parent_id: TaskID
    shard: int
    progress_units: str
    progress_total: float
    progress_completed: float
    children_ids: list[TaskID] = []

    # def __init__(self, status: dict):
    #     for k, v in status.items():
    #         setattr(self, k, v)  

    # def __init__(self, status: dict):
    #     self.task_id = status["task_id"]
    #     self.state = status["state"]
    #     self.type = status["type"]
    #     self.scope = status["scope"]
    #     self.keyspace = status["keyspace"]
    #     self.table = status["table"]
    #     self.entity = status["entity"]
    #     self.sequence_number = status["sequence_number"]
    #     self.is_abortable = status["is_abortable"]
    #     self.start_time = status["start_time"]

