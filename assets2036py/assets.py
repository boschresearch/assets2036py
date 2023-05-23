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
from enum import Enum
from os import path
import json
import ssl
import traceback
from json import JSONDecodeError
from urllib.request import urlopen
from abc import ABC, abstractmethod
from numbers import Number
import jsonschema
from assets2036py.exceptions import InvalidParameterException, NotWritableError
from .utilities import sanitize, get_resource_path

# pylint: disable=protected-access, line-too-long


logger = logging.getLogger(__name__)
schema_file = path.abspath(path.dirname(__file__)) + \
    "/resources/submodel_schema.json"

with open(schema_file) as f:
    submodel_schema = json.load(f)

# pylint: disable=no-member


class PropertyType(Enum):
    BOOLEAN = 0
    INTEGER = 1
    NUMBER = 2
    STRING = 3
    OBJECT = 5
    NULL = 6
    ARRAY = 7

    def native_type(self):
        type_map = {
            self.BOOLEAN: bool,
            self.INTEGER: int,
            self.NUMBER: Number,
            self.OBJECT: dict,
            self.STRING: str,
            self.ARRAY: list
        }
        if self in type_map:
            return type_map[self]
        else:
            raise Exception(f"No native Type for {self.name}")


class Mode(Enum):
    OWNER = 0
    CONSUMER = 1


class Property(ABC):
    def __init__(self, name, parent, property_definition):
        self._val = None
        self.schema = property_definition
        self.property_type = PropertyType[property_definition["type"].upper()]
        self.parent = parent
        self.communication_client = parent.communication_client
        self.name = name
        self.callbacks = []

    # noinspection PyProtectedMember

    def _get_topic(self):
        return self.parent._get_topic() + f"/{self.name}"

    def assert_valid_type(self, value):
        jsonschema.validate(value, self.schema)

    @property
    @abstractmethod
    def value(self):
        return self._val

    @value.setter
    @abstractmethod
    def value(self, value):

        pass

    def on_change(self, callback):
        self.callbacks.append(callback)


class WritableProperty(Property):

    @property
    def value(self):
        return self._val

    @value.setter
    def value(self, value):
        self.assert_valid_type(value)
        self._val = value
        self.communication_client.publish(
            self._get_topic(), json.dumps(value))

    def delete(self):
        logger.debug("deleting %s", self.name)
        self.communication_client.publish(self._get_topic(), "")


class ReadOnlyProperty(Property):

    def __init__(self, name, parent, property_definition):
        super().__init__(name, parent, property_definition)
        self.communication_client.subscribe(self._get_topic(), self._update)

    @property
    def value(self):
        return self._val

    @value.setter
    def value(self, value):
        raise NotWritableError("Writing to a ReadonlyProperty not allowed.")

    def _update(self, payload):
        logger.debug("Updating %s to %s", self.name, payload)
        try:
            updated_value = json.loads(payload)
            self.assert_valid_type(updated_value)
            self._val = updated_value
            for cb in self.callbacks:
                cb(self._val)
        except JSONDecodeError as jsonexc:
            logger.error("Payload '%s' was invalid JSON: %s", payload, jsonexc)
        except AssertionError as assertexc:
            logger.error("Parsed payload '%s' has wrong type: %s",
                         payload, assertexc)


class Event(ABC):
    def __init__(self, name, parent, event_definition):
        self.name = name
        self.parent = parent
        self.communication_client = parent.communication_client
        if "parameters" in event_definition:
            self._parameters = {name: PropertyType[schema["type"].upper()] for name, schema in
                                event_definition["parameters"].items()}

    def _validate_parameters(self, params):
        if params.keys() == self._parameters.keys():
            return all(isinstance(val, self._parameters[name].native_type()) for name, val in params.items())
        return False

    def _get_topic(self):
        return self.parent._get_topic() + f"/{self.name}"


class SubscribableEvent(Event):

    def on_event(self, callback):
        def callback_func(parameters, timestamp):
            if not self._validate_parameters(parameters):
                raise InvalidParameterException(
                    f" expected {','.join(self._parameters.keys())} but received {parameters}")
            callback(timestamp, **parameters)

        self.communication_client.subscribe_event(
            self._get_topic(), callback_func)


class TriggerableEvent(Event):

    def trigger(self, **params):
        if not self._validate_parameters(params):
            raise InvalidParameterException(
                f" expected {self._parameters.keys()} but got {params}")
        self.communication_client.trigger_event(self._get_topic(), params)


class Operation(ABC):
    def __init__(self, name, parent, operation_definition):
        self.name = name
        self.parent = parent
        self.communication_client = parent.communication_client
        self._response_schema = None
        self._parameters = {}
        if "parameters" in operation_definition:
            self._parameters = {name: PropertyType[schema["type"].upper()] for name, schema in
                                operation_definition["parameters"].items()}
        if "response" in operation_definition:
            self._response_type = PropertyType[operation_definition["response"]["type"].upper(
            )]
            self._response_schema = operation_definition["response"]

    @abstractmethod
    def invoke(self, timeout=None, **params):
        raise NotImplementedError("You can only call remote operations..")

    def _validate_parameters(self, params):
        if params.keys() == self._parameters.keys():
            return all(isinstance(val, self._parameters[name].native_type()) for name, val in params.items())
        return False

    # noinspection PyProtectedMember
    def _get_topic(self):
        return self.parent._get_topic() + f"/{self.name}"


class CallableOperation(Operation):

    def invoke(self, timeout=None, **params):
        if not self._validate_parameters(params):
            raise InvalidParameterException(
                f" expected {self._parameters} but got {params}")
        logger.debug("Calling %s", self.name)
        res = self.communication_client.invoke_operation(
            self._get_topic(), params, timeout)
        logger.debug("%s got %s", self.name, res)
        if hasattr(self, "_response_type"):
            assert isinstance(
                res, self._response_type.native_type())
            return res


class BindableOperation(Operation):
    def invoke(self, timeout=None, **params):
        super().invoke(timeout=timeout, **params)

    def bind(self, callback):
        def callback_func(parameters):
            if not self._validate_parameters(parameters):
                raise InvalidParameterException(
                    f" expected list with {self._parameters.keys()} but received {parameters}")
            try:
                res = callback(**parameters)
                logger.debug("callback executed, response is %s", res)
            # pylint: disable=broad-except
            except Exception:
                logger.error(
                    "operation '%s' caused exception:\n%s", callback, traceback.format_exc())
            if hasattr(self, "_response_type"):
                jsonschema.validate(res, self._response_schema)
                return res

        self.communication_client.bind_operation(
            self._get_topic(), callback_func)


class SubModel:
    meta_property = {
        "type": "object",
        "properties": {
            "source": {"type": "string"},
            "submodel_schema": {"type": "object"},
            "submodel_url": {"type": "string"}}
    }

    def __init__(self, asset, submodel_definition):
        # pylint: disable=no-member
        self.parent = asset
        self.communication_client = asset.communication_client
        self.name = sanitize(submodel_definition["name"])
        self._disconnect_callback = None

        if asset.access_mode == Mode.OWNER:

            meta_prop = WritableProperty("_meta", self, self.meta_property)

            setattr(self, "_meta", meta_prop)

            if "properties" in submodel_definition:
                self._create_writable_properties(submodel_definition)
            if "operations" in submodel_definition:
                self._create_bindable_operations(submodel_definition)
            if "events" in submodel_definition:
                self._create_triggerable_events(submodel_definition)
        else:

            if "properties" in submodel_definition:
                self._create_readonly_properties(submodel_definition)
            if "operations" in submodel_definition:
                self._create_callable_operations(submodel_definition)
            if "events" in submodel_definition:
                self._create_subscribable_events(submodel_definition)

    def is_online(self, seconds_to_wait):
        num_tries = 0
        if self.name == "_endpoint":
            online_prop = self.online
        else:
            online_prop = self.endpoint_asset._endpoint.online
        while num_tries < seconds_to_wait * 2:
            online_state = online_prop.value
            if online_state == None:
                num_tries += 1
                time.sleep(0.5)
            else:
                return online_state
        return False

    def _raise_offline_exception(self, online):
        logger.debug("CALLBACK GOT %s", online)
        if not online and self._disconnect_callback != None:
            logger.debug("SEND DISCONNECT FOR %s", self.name)
            self._disconnect_callback(self.name)

    def on_disconnect(self, callback):
        self._disconnect_callback = callback

    def register_source(self, source_address):
        if "/" in source_address:
            namespace, source_asset = source_address.split("/")
        else:
            namespace = self.parent.namespace
            source_asset = source_address
        if source_asset == self.parent.name and namespace == self.parent.namespace:
            # this asset is endpoint for itsself.
            self.endpoint_asset = self.parent
        else:
            with open(get_resource_path("_endpoint.json")) as file:
                endpoint_sm_definition = json.load(file)
            self.endpoint_asset = Asset(source_asset, namespace,
                                        endpoint_sm_definition, mode=Mode.CONSUMER, communication_client=self.communication_client, endpoint_name="")
        self.endpoint_asset._endpoint.online.on_change(
            self._raise_offline_exception)

    def delete(self):
        deletables = [getattr(self, prop) for prop in dir(
            self) if type(getattr(self, prop)) == WritableProperty]
        deletables.extend(getattr(self, prop) for prop in dir(
            self) if type(getattr(self, prop)) == BindableOperation)
        for d in deletables:
            d.delete()

    # noinspection PyProtectedMember
    def _get_topic(self):
        return self.parent._get_topic() + f"/{self.name}"

    def _create_callable_operations(self, submodel_definition):
        for name, schema in submodel_definition["operations"].items():
            new_op = CallableOperation(name, self, schema)
            setattr(self, sanitize(name), new_op.invoke)

    def _create_readonly_properties(self, submodel_definition):
        for name, schema in submodel_definition["properties"].items():
            new_prop = ReadOnlyProperty(name, self, schema)
            setattr(self, sanitize(name), new_prop)

    def _create_writable_properties(self, submodel_definition):
        for name, schema in submodel_definition["properties"].items():
            new_prop = WritableProperty(name, self, schema)
            setattr(self, sanitize(name), new_prop)

    def _create_bindable_operations(self, submodel_definition):
        for name, schema in submodel_definition["operations"].items():
            new_op = BindableOperation(name, self, schema)
            setattr(self, "bind_" + sanitize(name), new_op.bind)

    def _create_subscribable_events(self, submodel_definition):
        for name, schema in submodel_definition["events"].items():
            new_event = SubscribableEvent(name, self, schema)
            setattr(self, sanitize(name), new_event)

    def _create_triggerable_events(self, submodel_definition):
        for name, schema in submodel_definition["events"].items():
            new_event = TriggerableEvent(name, self, schema)
            setattr(self, sanitize(name), new_event.trigger)


class Asset:
    """Core element of assets2036py. Assets are automatically generated during runtime from given submodel descriptions.

    For each submodel description given an asset receives a attribute of the same name.
    Each submodel then implements the operations, properties and events specified by the submodel.

    You shouldn't instanciate assets by yourself, make use of :class: `assets2036py.assetmanager`

    """

    def __init__(self, name: str, namespace: str, *sub_models, mode=Mode.CONSUMER, communication_client, endpoint_name):
        self.name = name
        self.endpoint_name = endpoint_name
        self.namespace = namespace
        self.access_mode = mode
        self.communication_client = communication_client
        self.sub_models = sub_models
        self.sub_model_names = []
        for sm in sub_models:
            self.implement_sub_model(sm)

    def disconnect(self):
        logger.debug("%s disconnects", self.name)
        self.communication_client.disconnect()

    def delete(self):
        for submodel in self.sub_model_names:
            getattr(self, submodel).delete()

    def implement_sub_model(self, submodel):
        if isinstance(submodel, str):
            try:
                # try parsing as json string
                submodel_def = json.loads(submodel)
                submodel_url = "file://localhost"
            except JSONDecodeError:
                # try to load as a web resource
                try:
                    with urlopen(submodel, context=ssl._create_unverified_context()) as response:
                        submodel_def = json.load(response)
                    submodel_url = submodel
                except ValueError as valexc:
                    logger.error(
                        "Could not parse submodel definition of %s:\n%s", submodel, valexc)
                    return
        else:
            submodel_def = submodel
            submodel_url = "file://localhost"
            try:
                jsonschema.validate(submodel_def, submodel_schema)
            except jsonschema.exceptions.ValidationError as valexc:
                logger.error("%s is malformed:\n%s", submodel_def, valexc)

        new_sm = SubModel(self, submodel_def)
        if self.access_mode == Mode.OWNER:
            new_sm._meta.value = {"source": f"{self.namespace}/{self.endpoint_name}",
                                  "submodel_definition": submodel_def,
                                  "submodel_url": submodel_url if submodel_url else "file://localhost"}
        submodel_name = submodel_def['name'].replace("-", "_").replace(" ", "")
        self.sub_model_names.append(submodel_name)
        setattr(self, submodel_name, new_sm)

    def _get_topic(self):
        return f"{self.namespace}/{self.name}"


class ProxyAsset(Asset):

    def __init__(self, name: str, namespace: str, *meta_infos, mode=Mode.CONSUMER, communication_client, endpoint_name) -> None:
        submodel_defs = []
        sources = {}
        for meta_info in meta_infos:
            sm_def = meta_info["submodel_definition"]
            submodel_defs.append(sm_def)
            sources[sm_def["name"]] = meta_info["source"]

        super().__init__(name, namespace, *submodel_defs, mode=mode,
                         communication_client=communication_client, endpoint_name=endpoint_name)
        for name, source in sources.items():
            if name != "_endpoint":
                getattr(self, name).register_source(source)
                # this is some hacky shit, we need some nicer refactoring here

    @property
    def is_online(self):
        return self.wait_for_online(3)

    def wait_for_online(self, seconds_to_wait):
        for submodel in self.sub_model_names:
            if not getattr(self, submodel).is_online(seconds_to_wait):
                return False
        return True

    def on_disconnect(self, callback):
        for submodel in self.sub_model_names:
            getattr(self, submodel).on_disconnect(callback)
