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

import json
import logging
import threading
import time

import pytest

from assets2036py import AssetManager

from .test_utils import get_msgs_for_n_secs, wipe_retained_msgs

logger = logging.getLogger(__name__)

NAMESPACE = "test_assetmgr_on_off_arena2036"


@pytest.fixture(autouse=True)
def wipe_retained(mqtt_broker):
    wipe_retained_msgs(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, seconds=1)


@pytest.mark.network
def test_on_off_detection(mqtt_broker):
    mgr1 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr1")
    mgr2 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr2")
    mgr3 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr3")

    _asset1 = mgr1.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )
    _asset2 = mgr2.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )
    _asset3 = mgr3.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )

    time.sleep(1)
    msgs = get_msgs_for_n_secs(f"{NAMESPACE}/+/_endpoint/online/#", 1, port=mqtt_broker["port"])
    logger.debug([msg.payload for msg in msgs])
    endpoints = {msg.topic: json.loads(msg.payload) for msg in msgs}

    assert len(msgs) == 3
    assert all(endpoints.values())

    mgr2.disconnect()
    time.sleep(1)

    msgs = get_msgs_for_n_secs(f"{NAMESPACE}/+/_endpoint/online/#", 1, port=mqtt_broker["port"])
    endpoints = {msg.topic: json.loads(msg.payload) for msg in msgs}

    assert len(msgs) == 3
    assert not all(endpoints.values())

    mgr1.client.disconnect()
    mgr3.client.disconnect()
    time.sleep(1)
    msgs = get_msgs_for_n_secs(f"{NAMESPACE}/+/_endpoint/online/#", 1, port=mqtt_broker["port"])
    endpoints = {msg.topic: json.loads(msg.payload) for msg in msgs}

    assert len(msgs) == 3
    assert all(not v for v in endpoints.values())


@pytest.mark.network
def test_healthy_flag(mqtt_broker):
    mgr1 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr1")

    _asset = mgr1.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )

    msgs = get_msgs_for_n_secs(f"{NAMESPACE}/mgr1/_endpoint/healthy/#", 1, port=mqtt_broker["port"])
    assert len(msgs) == 1
    assert msgs[0].payload == b"false"

    mgr1.healthy = True

    msgs2 = get_msgs_for_n_secs(f"{NAMESPACE}/mgr1/_endpoint/healthy/#", 1, port=mqtt_broker["port"])
    assert len(msgs2) == 1
    assert msgs2[0].payload == b"true"
    mgr1.disconnect()


@pytest.mark.network
def test_query_assets(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr")
    endpoints = mgr.query_assets("arena2036", "_endpoint")
    assert len(endpoints) >= 0
    mgr.disconnect()


@pytest.mark.network
def test_query_all(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr")
    assets = mgr.query_assets("+", "+")
    assert len(assets) >= 0
    mgr.disconnect()


@pytest.mark.network
def test_create_asset_proxy(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr")
    _asset = mgr.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )
    asset_proxy = mgr.create_asset_proxy(NAMESPACE, "test_asset_123")
    assert "location" in dir(asset_proxy)
    mgr.disconnect()


@pytest.mark.network
def test_self_ping(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr")
    _asset = mgr.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )
    assert mgr._self_ping()
    mgr.disconnect()
    assert not mgr._self_ping()


@pytest.mark.network
def test_shutdown(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr")
    _asset = mgr.create_asset(
        "test_asset_123",
        "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json",
    )
    mgr.set_healthy_callback(lambda: True)
    monitor_threads = [t for t in threading.enumerate() if t.name[:3] == "mgr"]
    assert len(monitor_threads) == 1
    monitor_thread = monitor_threads[0]
    assert monitor_thread.is_alive()
    mgr.disconnect()
    time.sleep(1)
    assert not monitor_thread.is_alive()


def test_logging_handler(mqtt_broker):
    _logger = logging.getLogger("testing_logger")
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], NAMESPACE, "mgr")
    lh = mgr.get_logging_handler()
    _logger.addHandler(lh)
    _logger.setLevel(logging.DEBUG)
    _logger.debug("Hallo Test")
    mgr.disconnect()
