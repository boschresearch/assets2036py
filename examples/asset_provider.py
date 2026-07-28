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
"""Example for providing an Asset.

Run this first, then run asset_consumer.py in another terminal.
Requires an MQTT broker running (e.g., mosquitto on localhost:1883).
"""
import logging
import os
import signal
import time

from assets2036py import AssetManager

logger = logging.getLogger(__name__)

BROKER_URL = os.getenv("MQTT_BROKER_URL", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
NAMESPACE = "assets2036pyexample"
ENDPOINT = "assetproviderexample"
SUBMODEL_URL = "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/light.json"


class Light:
    """Emulate a light device (could be a real hardware driver)."""

    def __init__(self) -> None:
        self._state = False

    @property
    def is_on(self) -> bool:
        return self._state

    def switch_on(self) -> bool:
        self._state = True
        return True

    def switch_off(self) -> bool:
        self._state = False
        return True


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Connecting to broker %s:%s", BROKER_URL, BROKER_PORT)

    with AssetManager(BROKER_URL, BROKER_PORT, NAMESPACE, ENDPOINT) as mgr:
        lamp = mgr.create_asset("lamp_1", SUBMODEL_URL)
        light = Light()

        def switch_light(state: bool) -> bool:
            """Implementation of the switch_light operation."""
            success = light.switch_on() if state else light.switch_off()
            if success:
                lamp.light.light_switched(state=light.is_on)
                lamp.light.light_on.value = light.is_on
            return success

        lamp.light.bind_switch_light(switch_light)
        logger.info("Asset 'lamp_1' is online. Press Ctrl+C to exit.")

        try:
            signal.pause()  # Wait for signals (Ctrl+C)
        except (KeyboardInterrupt, AttributeError):
            # AttributeError: signal.pause() not available on Windows
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

    logger.info("Disconnected.")


if __name__ == "__main__":
    main()
