# Copyright (c) 2016 - for information on the respective copyright owner
# see the NOTICE file and/or the repository https://github.com/boschresearch/assets2036py.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import socket
import threading
import time

import pytest


def get_free_port() -> int:
    """Find a free TCP port dynamically."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def mqtt_broker():
    """Start an embedded amqtt MQTT broker on a dynamic port for the test session."""
    from amqtt.broker import Broker

    port = get_free_port()
    config = {
        "listeners": {
            "default": {"type": "tcp", "bind": f"127.0.0.1:{port}"},
        },
        "sys_interval": 0,
        "auth": {"allow-anonymous": True, "plugins": ["auth_anonymous"]},
        "topic-check": {"enabled": False},
    }
    loop = asyncio.new_event_loop()
    broker = Broker(config, loop=loop)
    started = threading.Event()
    error_container = []

    def run_broker():
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(broker.start())
            started.set()
            loop.run_forever()
        except Exception as e:
            error_container.append(e)
            started.set()

    thread = threading.Thread(target=run_broker, daemon=True)
    thread.start()
    if not started.wait(timeout=10) or error_container:
        err = error_container[0] if error_container else "Broker failed to start"
        raise RuntimeError(f"amqtt broker failed to start: {err}")
    time.sleep(0.3)  # Give the broker a moment to fully initialize

    yield {"host": "127.0.0.1", "port": port}

    # Shutdown
    async def _shutdown():
        await broker.shutdown()

    asyncio.run_coroutine_threadsafe(_shutdown(), loop)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
