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
"""Example for consumption of an Asset.

Requires asset_provider.py to be running!
"""
import logging
import os
import time

from assets2036py import AssetManager
from assets2036py.exceptions import AssetNotFoundError

logger = logging.getLogger(__name__)

BROKER_URL = os.getenv("MQTT_BROKER_URL", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
NAMESPACE = "assets2036pyexample"
ENDPOINT = "assetconsumerexample"


def on_light_switched_event(timestamp, state):
    """Callback for the 'light_switched' event.

    Must accept a timestamp and either **kwargs or the exact parameter names
    from the event specification.
    """
    logger.info("Event received: light switched to %s at %s", state, timestamp)


def on_light_on_property_change(value):
    """Callback for property change notifications."""
    logger.info("Property 'light_on' changed to %s", value)


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Connecting to broker %s:%s", BROKER_URL, BROKER_PORT)

    with AssetManager(BROKER_URL, BROKER_PORT, NAMESPACE, ENDPOINT) as mgr:
        try:
            lamp = mgr.create_asset_proxy(NAMESPACE, "lamp_1")
        except AssetNotFoundError:
            logger.error("Asset 'lamp_1' not found. Is asset_provider.py running?")
            return

        lamp.light.light_switched.on_event(on_light_switched_event)
        lamp.light.light_on.on_change(on_light_on_property_change)

        logger.info("Connected to 'lamp_1'. Toggling light every second...")
        state = True
        try:
            while True:
                lamp.light.switch_light(state=state)
                state = not state
                time.sleep(1)
                logger.info("light_on = %s", lamp.light.light_on.value)
        except KeyboardInterrupt:
            pass

    logger.info("Disconnected.")


if __name__ == "__main__":
    main()
