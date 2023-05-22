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
import os
from unittest import TestCase
from assets2036py import AssetManager
from .test_utils import get_msgs_for_n_secs, wipe_retained_msgs

logger = logging.getLogger(__name__)

HOST = os.getenv("MQTT_BROKER_URL", "localhost")
PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))

NAMESPACE = "test_assetmgr_on_off_arena2036"


class TestAssetManager(TestCase):

    @classmethod
    def tearDownClass(cls):
        wipe_retained_msgs(HOST, NAMESPACE)

    def setUp(self):
        wipe_retained_msgs(HOST, NAMESPACE)
        time.sleep(2)

    def test_on_off_detection(self):

        mgr1 = AssetManager(HOST, PORT, NAMESPACE, "mgr1")
        mgr2 = AssetManager(HOST, PORT, NAMESPACE, "mgr2")
        mgr3 = AssetManager(HOST, PORT, NAMESPACE, "mgr3")
        _asset1 = mgr1.create_asset(
            "test_asset_123", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json")
        _asset2 = mgr2.create_asset(
            "test_asset_123", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json")
        _asset3 = mgr3.create_asset(
            "test_asset_123", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json")

        time.sleep(2)
        msgs = get_msgs_for_n_secs(f"{NAMESPACE}/+/_endpoint/online/#", 2)
        logger.debug([msg.payload for msg in msgs])
        endpoints = {msg.topic: json.loads(msg.payload)for msg in msgs}

        self.assertEqual(len(msgs), 3)
        self.assertTrue(all(endpoints.values()))

        mgr2.disconnect()
        time.sleep(1)

        msgs = get_msgs_for_n_secs(f"{NAMESPACE}/+/_endpoint/online/#", 2)
        endpoints = {msg.topic: json.loads(msg.payload) for msg in msgs}

        self.assertEqual(len(msgs), 3)
        self.assertFalse(all(endpoints.values()))

        mgr1.client.disconnect()
        mgr3.client.disconnect()
        time.sleep(1)
        msgs = get_msgs_for_n_secs(f"{NAMESPACE}/+/_endpoint/online/#", 2)
        endpoints = {msg.topic: json.loads(msg.payload) for msg in msgs}

        self.assertEqual(len(msgs), 3)
        self.assertTrue(all(not v for v in endpoints.values()))

    def test_healthy_flag(self):

        mgr1 = AssetManager(HOST, PORT, NAMESPACE, "mgr1")
        _asset = mgr1.create_asset(
            "test_asset_123", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/location.json")
        msgs = get_msgs_for_n_secs(f"{NAMESPACE}/mgr1/_endpoint/healthy/#", 2)
        self.assertTrue(len(msgs), 1)
        self.assertEqual(msgs[0].payload, b"false")
        logger.debug(msgs)

        mgr1.healthy = True

        msgs2 = get_msgs_for_n_secs(
            f"{NAMESPACE}/mgr1/_endpoint/healthy/#", 2)
        self.assertTrue(len(msgs2), 1)
        self.assertEqual(msgs2[0].payload, b"true")

    def test_query_assets(self):
        mgr = AssetManager(HOST, PORT, NAMESPACE, "mgr")
        endpoints = mgr.query_assets("arena2036", "_endpoint")
        self.assertTrue(len(endpoints) > 0)

    def test_query_all(self):
        mgr = AssetManager(HOST, PORT, NAMESPACE, "mgr")
        assets = mgr.query_assets("+", "+")
        self.assertTrue(len(assets) > 0)

    def test_create_asset_proxy(self):
        mgr = AssetManager(HOST, PORT, NAMESPACE, "mgr")
        _asset = mgr.create_asset(
            "test_asset_123", "https://arena2036-infrastructure.saz.bosch-si.com/arena2036_public/assets2036_submodels/raw/master/powerstate.json")
        asset_proxy = mgr.create_asset_proxy(NAMESPACE, "test_asset_123")
        self.assertIn("powerstate", dir(asset_proxy))

    def test_self_ping(self):
        # pylint: disable=protected-access
        mgr = AssetManager(HOST, PORT, NAMESPACE, "mgr")
        _asset = mgr.create_asset(
            "test_asset_123", "https://arena2036-infrastructure.saz.bosch-si.com/arena2036_public/assets2036_submodels/raw/master/powerstate.json")
        self.assertTrue(mgr._self_ping())
        mgr.disconnect()
        self.assertFalse(mgr._self_ping())

    def test_shutdown(self):
        mgr = AssetManager(HOST, PORT, NAMESPACE, "mgr")
        _asset = mgr.create_asset(
            "test_asset_123", "https://arena2036-infrastructure.saz.bosch-si.com/arena2036_public/assets2036_submodels/raw/master/powerstate.json")
        mgr.set_healthy_callback(lambda: True)
        monitor_threads = [
            t for t in threading.enumerate() if t.name[:3] == "mgr"]
        self.assertEqual(len(monitor_threads), 1)
        monitor_thread = monitor_threads[0]
        self.assertTrue(monitor_thread.is_alive())
        mgr.disconnect()
        time.sleep(3)
        self.assertFalse(monitor_thread.is_alive())
