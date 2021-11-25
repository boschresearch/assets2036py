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
Example for providing an Asset.
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
ENDPOINT = "assetproviderexample"
SUBMODEL_URL = "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/light.json"


class Light:
    '''
    Emulate light, could be real driver or legacy interface
    '''

    def __init__(self) -> None:
        self._light_state = False

    def get_light_state(self) -> bool:
        return self._light_state

    def switch_on(self) -> bool:
        self._light_state = True
        return True

    def switch_off(self) -> bool:
        self._light_state = False
        return True


class LightAdapter:
    '''
    Connector between legacy light and assets2036
    '''

    def __init__(self) -> None:
        mgr = AssetManager(BROKER_URL, BROKER_PORT, NAMESPACE, ENDPOINT)

        # create new asset in default namespace
        self._lamp_1 = mgr.create_asset("lamp_1", SUBMODEL_URL)
        self._light = Light()

        # register callback for operation calls
        self._lamp_1.light.bind_switch_light(self._switch_light)

    def _switch_light(self, state: bool) -> bool:
        '''
        implementation of switch_light operation
        '''
        if state:
            success = self._light.switch_on()
        else:
            success = self._light.switch_off()
        if success:
            # emit event
            self._lamp_1.light.light_switched(
                state=self._light.get_light_state())
            # set property
            self._lamp_1.light.light_on.value = self._light.get_light_state()
        return success

    def run(self):
        while True:
            time.sleep(1)


def main():
    logger.debug("Connecting to Broker %s:%s", BROKER_URL, BROKER_PORT)
    light_adapter = LightAdapter()
    light_adapter.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
