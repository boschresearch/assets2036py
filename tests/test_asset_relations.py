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

import os
from unittest import TestCase

from assets2036py import AssetManager

HOST = os.getenv("MQTT_BROKER_URL", "10.163.31.2")
PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
NAMESPACE = os.getenv("MQTT_NAMESPACE", "test_arena2036")
ENDPOINT = os.getenv("MQTT_ENDPOINT", "test_arena2036mgr")


class TestAssetRelations(TestCase):
    def test_create_children(self):
        mgr = AssetManager(HOST, PORT, NAMESPACE, ENDPOINT)
        root_asset = mgr.create_asset(
            "root_asset",
            "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/pose3d.json",
        )
        child_asset = root_asset.create_child_asset(
            NAMESPACE,
            "child_asset1",
            "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/pose3d.json",
        )
        self.assertEqual(
            child_asset._relations.belongs_to.value["asset_name"], "root_asset"
        )
        self.assertTrue("pose3d" in dir(child_asset))

    def test_get_asset_proxy_children(self):
        mgr = AssetManager(HOST, PORT, NAMESPACE, ENDPOINT)
        root_asset = mgr.create_asset(
            "root_asset",
            "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/pose3d.json",
        )
        for i in range(3):
            child_asset = root_asset.create_child_asset(
                NAMESPACE,
                "child_asset" + str(i),
                "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/pose3d.json",
            )
            for j in range(2):
                child_asset.create_child_asset(
                    NAMESPACE,
                    "child_asset" + str(i) + str(j),
                    "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/pose3d.json",
                )
        root_proxy = mgr.create_asset_proxy(NAMESPACE, "root_asset")
        asset_info = root_proxy.get_child_assets()
        self.assertEqual(len(asset_info), 3)
        for i in range(3):
            self.assertTrue(
                any(
                    asset["asset_name"] == "child_asset" + str(i)
                    and asset["namespace"] == NAMESPACE
                    for asset in asset_info
                )
            )
