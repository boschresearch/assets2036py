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
import sys
import uuid
import time
import re
import traceback
from abc import ABC, abstractmethod
import logging
from collections import defaultdict
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil import tz
import paho.mqtt.client as mqtt
from assets2036py.exceptions import OperationTimeoutException, InvalidParameterException
from assets2036py import context


logger = logging.getLogger(__name__)


def create_name_regexp(namespace, assetnname):
    namespace_sanitized = namespace.replace("+", "\w+?")
    assetname_sanitized = assetnname.replace("+", "\w+?")
    return f"{namespace_sanitized}/{assetname_sanitized}/([A-Za-z0-9._-]*)/_meta"


def create_submodel_regexp(namespace, submodelname):
    namespace_sanitized = namespace.replace("+", "\w+?")
    submodelname_sanitized = submodelname.replace("+", "\w+?")
    return f"{namespace_sanitized}/([A-Za-z0-9._-]*)/{submodelname_sanitized}/_meta"


class CommunicationClient(ABC):

    @abstractmethod
    def connect(self, **config):
        pass

    @abstractmethod
    def join(self, timeout=None):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def publish(self, topic, payload, **config):
        pass

    @abstractmethod
    def subscribe(self, topic, callback):
        pass

    @abstractmethod
    def on_connect(self, cb):
        pass

    @abstractmethod
    def on_disconnect(self, cb):
        pass

    @abstractmethod
    def on_message(self, cb):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def invoke_operation(self, operation, parameter, timeout):
        pass

    @abstractmethod
    def bind_operation(self, operation, cb):
        pass

    @abstractmethod
    def trigger_event(self, event, parameter):
        pass

    @abstractmethod
    def subscribe_event(self, event, callback):
        pass


class MockClient(CommunicationClient):

    def subscribe_event(self, event, callback):
        pass

    def trigger_event(self, event, parameter):
        pass

    def bind_operation(self, operation, cb):
        pass

    def invoke_operation(self, operation, parameter, timeout):
        pass

    def join(self, timeout=None):
        pass

    def connect(self, **config):
        logger.debug("Connecting with config %s", config)

    def disconnect(self):
        logger.debug("Disconnecting!")

    def publish(self, topic, payload, **config):
        logger.debug("Sending %s to %s with %s",
                     payload, topic, config)

    def subscribe(self, topic, callback):
        logger.debug("Subscribed to %s", topic)

    def on_connect(self, cb):
        pass

    def on_disconnect(self, cb):
        pass

    def on_message(self, cb):
        pass

    def run(self):
        pass


class MQTTClient(CommunicationClient):

    def __init__(self, client_id):
        self._queues = defaultdict(Queue)
        self.client = mqtt.Client(
            f"assets2036py_{client_id}_{uuid.uuid4().hex}", clean_session=False)
        self._executor = ThreadPoolExecutor(max_workers=10)

    def join(self, timeout=None):
        #pylint: disable=protected-access
        self.client._thread.join(timeout)

    def _get_msgs_for_n_secs(self, topic, seconds):
        msgs = defaultdict(list)

        def _message_callback(_client, _userdata, message):
            logger.debug(
                "Callback received %s from %s", message.payload, message.topic)
            msgs[message.topic].append(message.payload)

        self.subscribe(topic, _message_callback)
        time.sleep(seconds)
        self.client.unsubscribe(topic)
        return msgs

    def unsubscribe(self, topic):
        self.client.unsubscribe(topic)

    def trigger_event(self, event, parameter):
        payload = {
            "timestamp": datetime.now(tz.tzlocal()).isoformat(),
            "params": parameter
        }
        self.publish(event, json.dumps(payload), retain=False)

    def query_submodels_for_asset(self, namespace, name, seconds=1):
        msgs = self._get_msgs_for_n_secs(
            f"{namespace}/{name}/+/_meta", seconds)
        submodels = {}
        for topic, meta in msgs.items():
            regexp = create_name_regexp(namespace, name)
            match = re.search(
                regexp, topic)
            if not match:
                continue
            submodel_name = match[1]
            schema = json.loads(meta[0])
            submodels[submodel_name] = schema
        return submodels

    def query_asset_names(self, namespace, *submodel_names, seconds=3):

        asset_names = []
        for sm in submodel_names:

            regexp = create_submodel_regexp(namespace, sm)
            msgs = self._get_msgs_for_n_secs(
                f"{namespace}/+/{sm}/_meta", seconds)
            asset_names_for_sm = set()
            for topic in msgs:
                match = re.search(regexp, topic)
                if not match:
                    continue
                asset_name = match[1]
                asset_names_for_sm.add(
                    asset_name)
            asset_names.append(asset_names_for_sm)
        if not asset_names:
            return set()
        if len(asset_names) == 1:
            return asset_names[0]
        return asset_names[0].intersection(*asset_names[1:])

    def invoke_operation(self, operation, parameter, timeout):
        topic = operation + "/RESP"
        self.subscribe(topic, self._on_response)
        req_id = uuid.uuid4().hex
        payload = {
            "req_id": req_id,
            "params": parameter
        }

        self.publish(operation + "/REQ", json.dumps(payload),
                     retain=False)  # Don't resend method invocations!
        logger.debug("%s invoked with payload %s", operation, payload)
        try:
            start = time.perf_counter()
            response = self._queues[req_id].get(timeout=timeout)
            end = time.perf_counter()
            logger.debug(
                "got response \"%s\" after %s seconds", response, end-start)
            return response
        except Empty:
            # pylint: disable=raise-missing-from
            raise OperationTimeoutException(
                f"{operation} timed out")

    def _on_response(self, payload):
        try:
            payload_obj = json.loads(payload)
            req_id = payload_obj["req_id"]
            self._queues[req_id].put(payload_obj["resp"])
        except KeyError as e:
            logger.error("Received invalid payload: %s", e)
        except json.JSONDecodeError as e:
            logger.error("Received invalid json: %s", e)

    def bind_operation(self, operation, cb):
        def callback_func(payload):
            # pylint: disable=bare-except
            try:
                payload_obj = json.loads(payload)
                req_id = payload_obj["req_id"]
                context.set("req_id", req_id)
                res = cb(payload_obj["params"])
                context.free()
                response = {
                    "req_id": req_id,
                    "resp": res
                }
                self.publish(operation + "/RESP", json.dumps(response),
                             retain=False)  # Don't resend results
            except InvalidParameterException as e:
                logger.error("Callback got invalid parameters: %s", e)
            except KeyError as e:
                logger.error("Received invalid payload: %s", e)
            except json.JSONDecodeError as e:
                logger.error("Received invalid json: %s", e)
            except TypeError as e:
                logger.error(
                    "Callback got wrong number of parameters: %s", e)
            except:
                logger.error(
                    " Exception occured during callback execution:\n %s", traceback.format_exc())

        self.subscribe(
            operation + "/REQ", lambda payload: self._executor.submit(callback_func, payload))

    def subscribe_event(self, event, callback):
        def callback_func(payload):
            # pylint: disable=bare-except
            try:
                payload_obj = json.loads(payload)
                timestamp = payload_obj["timestamp"]
                params = payload_obj["params"]
                callback(params, timestamp)
            except InvalidParameterException as e:
                logger.error("Callback got invalid parameters: %s", e)
            except KeyError as e:
                logger.error("Received invalid payload: %s", e)
            except json.JSONDecodeError as e:
                logger.error("Received invalid json: %s", e)
            except TypeError as e:
                logger.error(
                    "Callback got wrong number of parameters: %s", e)
            except:
                logger.error(
                    " Exception occured during callback execution:\n%s", traceback.format_exc())

        self.subscribe(event, lambda payload: self._executor.submit(
            callback_func, payload))

    def disconnect(self):
        # pylint: disable=protected-access
        if self.client._will_topic:
            # noinspection PyProtectedMember
            self.client.publish(self.client._will_topic.decode(
            ), self.client._will_payload.decode(), retain=True)
        self.client.disconnect()
        self.client.loop_stop()

    def on_connect(self, cb):
        self.client.on_connect = cb

    def on_disconnect(self, cb):
        self.client.on_disconnect = cb

    def on_message(self, cb):
        self.client.on_message = cb

    def connect(self, **config):
        self.client.will_set(
            f"{config['namespace']}/{config['endpoint_name']}/_endpoint/online", "false", retain=True)
        self.client.connect(config["host"], config["port"], keepalive=10)
        self.client.loop_start()

    def publish(self, topic, payload, **config):
        logger.debug("Sending %s to %s", payload, topic)
        self.client.publish(topic, payload, config.get(
            "qos", 2), config.get("retain", True))

    def subscribe(self, topic, callback):

        def callback_func(client, userdata, message):
            logger.debug(
                "Received %s from %s", message.payload, message.topic)
            if message.payload:

                # support callbacks for full info as well as payload only
                if callback.__code__.co_argcount < 3:
                    callback(message.payload)
                else:
                    callback(client, userdata, message)
            else:
                logger.debug("Ignored empty message on %s", message.topic)

        self.client.message_callback_add(
            topic, lambda c, u, m: self._executor.submit(
                callback_func, c, u, m)
        )
        self.client.subscribe(topic, 2)
        logger.debug("Subscribed to %s", topic)

    def run(self):
        pass
        # self.client.loop_start()
