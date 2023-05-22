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

import logging
import time
import os
from os import path
from urllib.request import pathname2url
import paho.mqtt.client as mqtt
logger = logging.getLogger(__name__)

res_path = path.abspath(path.dirname(__file__)) + "/resources/"
res_url = "file:" + pathname2url(res_path)
HOST = os.getenv("MQTT_BROKER_URL", "localhost")


def get_msgs_for_n_secs(topic, seconds, host=HOST):
    msgs = []

    def _message_callback(_client, _userdata, message):
        logger.debug("Callback received %s on %s",
                     message.payload, message.topic)
        msgs.append(message)

    def _on_connect(client, _userdata, _flags, _rc):
        client.subscribe(topic)

    client = mqtt.Client()
    client.on_message = _message_callback
    client.on_connect = _on_connect
    client.connect(host)
    client.loop_start()
    time.sleep(seconds)
    client.loop_stop()
    client.disconnect()
    return msgs


def wipe_retained_msgs(host, namespace=None, seconds=3):

    def _message_callback(client, _userdata, message):
        if message.payload != b'':
            logger.debug("Wiping %s from %s", message.payload, message.topic)
            client.publish(message.topic, "", retain=True)

    def _on_connect(client, _userdata, _flags, _rc):
        topic = namespace + "/#" if namespace else "#"
        logger.debug("subscribing to %s", topic)
        client.subscribe(topic)

    client = mqtt.Client()
    client.on_message = _message_callback
    client.on_connect = _on_connect
    client.connect(host)
    client.loop_start()
    time.sleep(seconds)
    client.loop_stop()
    client.disconnect()
