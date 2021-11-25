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
'''
Example for consumption of an Asset.
Requires asset_provider.py to run!

'''
import sys
import time
import logging
import os

p = os.path.abspath('.')
sys.path.insert(1, p)
from assets2036py import AssetManager  # noqa
from assets2036py.exceptions import AssetNotFoundError  # noqa


logger = logging.getLogger(__name__)

BROKER_URL = os.getenv("MQTT_BROKER_URL", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
NAMESPACE = "assets2036pyexample"
ENDPOINT = "assetconsumerexample"


def on_light_switched_event(timestamp, state):
    '''callback for the "light_switched" event
    Must have mandatory parameter to receive the timestamp and either **kwargs to collect all
    keyworded parameters or every parameter described 
    in event specification with the exact same name
    '''
    logger.debug(
        "Received an Event! At %s light has been switched to %s", timestamp, state)


def on_light_on_property_change(value):
    '''callback for property change
    Must have one parameter to receive the new value
    '''
    logger.debug("Property 'light_on' has changed to %s", value)


def main():
    logger.debug("Connecting to Broker %s:%s", BROKER_URL, BROKER_PORT)
    try:
        mgr = AssetManager(BROKER_URL, BROKER_PORT, NAMESPACE, ENDPOINT)
        lamp_1 = mgr.create_asset_proxy(NAMESPACE, "lamp_1")
        # register callback for event "light_switched"
        lamp_1.light.light_switched.on_event(on_light_switched_event)

        # register callback to get notified when property "light_on" changes
        lamp_1.light.light_on.on_change(on_light_on_property_change)

        state = True
        while True:
            # call operation "switch_light"
            lamp_1.light.switch_light(state=state)
            state = not state
            time.sleep(1)
            # read property "light_on"
            logger.debug("Property light_on has value %s",
                         lamp_1.light.light_on.value)
    except AssetNotFoundError:
        logger.error(
            "No asset of name 'lamp_1' found on broker. Did you start asset_provider.py?")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
