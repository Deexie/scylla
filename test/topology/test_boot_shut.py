#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import time
import pytest
import logging

from test.pylib.internal_types import IPAddress
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_start_bootstrapped_with_invalid_seed(manager: ManagerClient):
    injection = "repair_service_bootstrap_with_repair_wait"

    s1 = await manager.server_add(config={
        'error_injections_at_startup': [
            {
                'name': injection,
            },
        ],
    }, start=False)

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async def continue_server():
        # await log.wait_for("bootstrap_with_repair: started with keyspace=", from_mark=mark)
        # time.sleep(3)
        await log.wait_for("init - Shutting down repair service was successful", from_mark=mark)
        await manager.api.message_injection(s1.ip_addr, injection)

    async def stop_server():
        await log.wait_for("bootstrap_with_repair: started with keyspace=", from_mark=mark)
        await manager.server_stop_gracefully(s1.server_id)

    async def start_server():
        try:
            await manager.server_start(s1.server_id, wait_others=False)
        except:
            pass

    await asyncio.gather(start_server(), stop_server(), continue_server())
