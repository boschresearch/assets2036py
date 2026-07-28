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
from multiprocessing import Pool
from threading import Thread

import pytest
from jsonschema import ValidationError

from assets2036py import Asset, AssetManager, Mode
from assets2036py.communication import MockClient
from assets2036py.exceptions import AssetNotOnlineError, NotWritableError

from .test_utils import get_msgs_for_n_secs, res_url, wipe_retained_msgs

logger = logging.getLogger(__name__)

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 18830

@pytest.fixture(scope="module", autouse=True)
def wipe_retained(mqtt_broker):
    wipe_retained_msgs(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036")
    yield
    wipe_retained_msgs(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036")

def create_client_and_ask(args):
    """Helper function to test parallel operations"""
    idx, host, port = args
    mgr = AssetManager(host, port, "test_arena2036", f"test_endpoint_{idx}")
    client = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.CONSUMER
    )
    res = client.simple_operation.addnumbers(a=0, b=idx)
    mgr.disconnect()
    return res



def test_implement_sub_model_consumer():

    a = Asset(
        "simpleasset",
        "test_arena2036",
        res_url + "simplebool.json",
        mode=Mode.CONSUMER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )

    assert "simple_bool" in dir(a)
    assert "switch" in dir(a.simple_bool)
    assert "value" in dir(a.simple_bool.switch)
    topic = a.simple_bool.switch._get_topic()
    assert topic == "test_arena2036/simpleasset/simple_bool/switch"

def test_implement_sub_model_owner():

    a = Asset(
        "simpleasset",
        "test_arena2036",
        res_url + "simplebool.json",
        mode=Mode.OWNER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )

    assert "simple_bool" in dir(a)
    assert "switch" in dir(a.simple_bool)
    assert "value" in dir(a.simple_bool.switch)
    topic = a.simple_bool.switch._get_topic()
    assert topic == "test_arena2036/simpleasset/simple_bool/switch"

def test_put_value():

    a = Asset(
        "simpleasset",
        "test_arena2036",
        res_url + "simplebool.json",
        mode=Mode.OWNER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )

    a.simple_bool.switch.value = True
    assert a.simple_bool.switch.value
    a.simple_bool.switch.value = False
    assert not a.simple_bool.switch.value

@pytest.mark.network
def test_load_from_unverified_ssl(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    source = mgr.create_asset(
        "source",
        "https://arena2036-infrastructure.saz.bosch-si.com/arena2036_public/assets2036_submodels/raw/master/powerstate.json",
    )
    source.powerstate.power_state.value = True

def test_implement_complex_sub_model_consumer():
    a = Asset(
        "activeshuttle",
        "test_arena2036",
        res_url + "activeshuttle.json",
        mode=Mode.CONSUMER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )


    assert "active_shuttle" in dir(a)
    assert "pose" in dir(a.active_shuttle)
    assert "state" in dir(a.active_shuttle)
    assert "waypoints" in dir(a.active_shuttle)

    assert isinstance(a.active_shuttle.pose.value, type(None))

    def evil_func():

        a.active_shuttle.pose.value = {"x": 5.7, "y": 3.5, "z": 0.1}


    with pytest.raises(NotWritableError):
        evil_func()
    assert isinstance(a.active_shuttle.pose.value, type(None))

def test_all_property_types():

    a = Asset(
        "PropertyTest",
        "test_arena2036",
        res_url + "all_property_model.json",
        mode=Mode.OWNER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )

    a.all_properties.a_switch.value = True
    assert a.all_properties.a_switch.value
    a.all_properties.a_float.value = 4.2
    assert abs(a.all_properties.a_float.value - 4.2) < 1e-5
    a.all_properties.a_text.value = "Hullo"
    assert a.all_properties.a_text.value == "Hullo"
    a.all_properties.an_object.value = {"x": 5, "y": 6}
    assert a.all_properties.an_object.value == {"x": 5, "y": 6}
    a.all_properties.a_list.value = [1, 2, 3, "a"]
    assert a.all_properties.a_list.value == [1, 2, 3, "a"]

    def evil_func():
        a.all_properties.an_object.value = {"x": 5, "y": "booh"}

    with pytest.raises(ValidationError):
        evil_func()
    a.all_properties.an_int.value = 42
    assert a.all_properties.an_int.value == 42

def test_type_validation():

    a = Asset(
        "PropertyTest",
        "test_arena2036",
        res_url + "all_property_model.json",
        mode=Mode.OWNER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )

    with pytest.raises(ValidationError):
        a.all_properties.a_switch.value = 42
    assert a.all_properties.a_switch.value is None

    with pytest.raises(ValidationError):
        a.all_properties.an_int.value = 42.5

    a.all_properties.a_float.value = 42

    with pytest.raises(ValidationError):
        a.all_properties.a_text.value = 42

    with pytest.raises(ValidationError):
        a.all_properties.an_object.value = True

    with pytest.raises(ValidationError):
        a.all_properties.an_object.value = {"x": 5, "z": 6}

    with pytest.raises(ValidationError):
        a.all_properties.an_int_object.value = {"x": 5.1, "y": 6.0}

    with pytest.raises(ValidationError):
        a.all_properties.a_list.value = {"x": 5, "z": 6}

def test_implement_complex_sub_model_owner():

    a = Asset(
        "activeshuttle",
        "test_arena2036",
        res_url + "activeshuttle.json",
        mode=Mode.OWNER,
        communication_client=MockClient(),
        endpoint_name="test_endpoint_1",
    )


    assert "active_shuttle" in dir(a)
    assert "pose" in dir(a.active_shuttle)
    assert "state" in dir(a.active_shuttle)
    assert "waypoints" in dir(a.active_shuttle)

def test_communication(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    source = mgr.create_asset("source", res_url + "all_property_model.json")
    sink = mgr.create_asset(
        "source", res_url + "all_property_model.json", mode=Mode.CONSUMER
    )


    time.sleep(2)
    source.all_properties.a_switch.value = True
    source.all_properties.an_int.value = 42
    source.all_properties.a_float.value = 4.2
    source.all_properties.a_text.value = "Hullo"
    source.all_properties.an_object.value = {"x": 5, "y": 6}
    source.all_properties.a_list.value = [1, 2, 3, 4]

    logger.debug("Sleeping 2 secs")
    time.sleep(2)

    assert sink.all_properties.a_switch.value
    assert sink.all_properties.an_int.value == 42
    assert abs(4.2 - sink.all_properties.a_float.value) < 1e-5
    assert sink.all_properties.a_text.value == "Hullo"
    assert sink.all_properties.an_object.value == {"x": 5, "y": 6}
    assert sink.all_properties.a_list.value == [1, 2, 3, 4]
    mgr.disconnect()

def test_delete(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")

    source = mgr.create_asset("deletetest", res_url + "all_property_model.json")

    time.sleep(2)
    source.all_properties.a_switch.value = True
    source.all_properties.an_int.value = 42
    source.all_properties.a_float.value = 4.2
    source.all_properties.a_text.value = "Hullo"
    source.all_properties.an_object.value = {"x": 5, "y": 6}
    source.all_properties.a_list.value = [1, 2, 3, 4]

    time.sleep(2)
    msgs = get_msgs_for_n_secs("test_arena2036/deletetest/#", 3, port=mqtt_broker["port"])
    assert len(msgs) > 0
    source.delete()
    logger.debug("Sleeping 3 secs")
    time.sleep(2)
    msgs = get_msgs_for_n_secs("test_arena2036/deletetest/#", 3, port=mqtt_broker["port"])
    logger.debug("Received messages:")
    assert len(msgs) == 0
    mgr.disconnect()

def test_retained_communication(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")

    source = mgr.create_asset("source", res_url + "all_property_model.json")

    time.sleep(2)
    source.all_properties.a_switch.value = True
    source.all_properties.an_int.value = 42
    source.all_properties.a_float.value = 4.2
    source.all_properties.a_text.value = "Hullo"
    source.all_properties.an_object.value = {"x": 5, "y": 6}
    source.all_properties.a_list.value = [1, 2, 3, 4]


    sink = mgr.create_asset(
        "source", res_url + "all_property_model.json", mode=Mode.CONSUMER
    )


    time.sleep(2)

    assert sink.all_properties.a_switch.value
    assert sink.all_properties.an_int.value == 42
    assert abs(4.2 - sink.all_properties.a_float.value) < 1e-5
    assert sink.all_properties.a_text.value == "Hullo"
    assert sink.all_properties.an_object.value == {"x": 5, "y": 6}
    assert sink.all_properties.a_list.value == [1, 2, 3, 4]
    mgr.disconnect()

def test_communication_updates(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    source = mgr.create_asset(
        "source", res_url + "all_property_model.json", mode=Mode.OWNER
    )
    sink = mgr.create_asset(
        "source", res_url + "all_property_model.json", mode=Mode.CONSUMER
    )


    returns = []

    def cb(x):
        returns.append(x)

    sink.all_properties.an_int.on_change(cb)

    time.sleep(1)

    for idx in range(10):
        source.all_properties.an_int.value = idx
    time.sleep(3)
    logger.debug(returns)
    assert abs(len(returns) - 10) <= 2  # VerneMQ sends the last received message twice? --> Sometimes 11 msgs...
    mgr.disconnect()

def test_submodel_meta_property(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    asset1 = mgr.create_asset("asset", res_url + "simplebool.json")
    assert "simple_bool" in dir(asset1)
    assert "_meta" in dir(asset1.simple_bool)
    assert asset1.simple_bool._meta.value["source"] == "test_arena2036/test_endpoint_1"
    assert asset1.simple_bool._meta.value["submodel_url"] == f"{res_url}simplebool.json"

    asset2 = mgr.create_asset(
        "asset", res_url + "simplebool.json", mode=Mode.CONSUMER
    )

    assert "simple_bool" in dir(asset2)
    assert "_meta" not in dir(asset2.simple_bool)
    mgr.disconnect()

def test_simple_operation(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    sink = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.CONSUMER
    )

    assert "simple_operation" in dir(sink)
    assert "do_it" in dir(sink.simple_operation)
    assert callable(sink.simple_operation.do_it)
    assert "addnumbers" in dir(sink.simple_operation)
    assert callable(sink.simple_operation.addnumbers)


    owner = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.OWNER
    )

    assert "bind_do_it" in dir(owner.simple_operation)
    assert "bind_addnumbers" in dir(owner.simple_operation)

    def do_addnumbers(**kwargs):
        return kwargs["a"] + kwargs["b"]

    # noinspection PyUnusedLocal
    def do_it():
        logger.debug("DOOOOING IT")

    def do_concat(**kwargs):
        return kwargs["a"] + kwargs["b"]

    owner.simple_operation.bind_concat(do_concat)
    owner.simple_operation.bind_addnumbers(do_addnumbers)
    owner.simple_operation.bind_do_it(do_it)
    owner.simple_operation.bind_do_it(lambda: logger.debug("DOING IT Lambda_style"))

    time.sleep(1)

    res = sink.simple_operation.addnumbers(a=5, b=6, timeout=3)
    res2 = sink.simple_operation.concat(a="hello", b="world", timeout=3)
    sink.simple_operation.do_it()
    assert 11 == res
    assert "helloworld" == res2
    mgr.disconnect()

def test_multi_parameter_operation(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    caller = mgr.create_asset(
        "source", res_url + "multi_param_operation.json", mode=Mode.CONSUMER
    )
    owner = mgr.create_asset(
        "source", res_url + "multi_param_operation.json", mode=Mode.OWNER
    )


    def merge_into_list_samename(first, second, third):
        return [first, second, third]

    def merge_into_list_distorted(second, third, first):
        return [first, second, third]

    def merge_into_list_kwargs(**kwargs):
        return [kwargs["first"], kwargs["second"], kwargs["third"]]

    owner.multiparam.bind_multiparam_1(merge_into_list_samename)
    owner.multiparam.bind_multiparam_2(merge_into_list_kwargs)
    owner.multiparam.bind_multiparam_3(merge_into_list_distorted)
    time.sleep(1)

    res1 = caller.multiparam.multiparam_1(
        first=1, second="hello", third={"a": 1}, timeout=3
    )
    res2 = caller.multiparam.multiparam_2(
        first=1, second="hello", third={"a": 1}, timeout=3
    )
    res3 = caller.multiparam.multiparam_3(
        first=1, second="hello", third={"a": 1}, timeout=3
    )
    assert res1 == [1, "hello", {"a": 1}]
    assert res2 == [1, "hello", {"a": 1}]
    assert res3 == [1, "hello", {"a": 1}]
    mgr.disconnect()

def test_object_operation(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    sink = mgr.create_asset(
        "source", res_url + "object_operation.json", mode=Mode.CONSUMER
    )
    owner = mgr.create_asset(
        "source", res_url + "object_operation.json", mode=Mode.OWNER
    )


    def do_split(a):
        return {"firstletter": a[0], "lastletter": a[-1]}

    owner.object_operation.bind_splitstring(do_split)

    time.sleep(1)

    res = sink.object_operation.splitstring(a="IO")
    assert res == {"firstletter": "I", "lastletter": "O"}
    mgr.disconnect()

def test_array_operation(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    sink = mgr.create_asset(
        "source", res_url + "array_operation.json", mode=Mode.CONSUMER
    )
    owner = mgr.create_asset(
        "source", res_url + "array_operation.json", mode=Mode.OWNER
    )


    def do_give_array(length):
        return [1] * length

    owner.array_operation.bind_give_array(do_give_array)

    time.sleep(1)

    res = sink.array_operation.give_array(length=2, timeout=3)
    assert res == [1, 1]
    mgr.disconnect()

def testMultiOperationRequests(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    owner = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.OWNER
    )

    assert "bind_do_it" in dir(owner.simple_operation)
    assert "bind_addnumbers" in dir(owner.simple_operation)

    def do_addnumbers(a, b):
        return a + b

    # noinspection PyUnusedLocal
    def do_it():
        logger.debug("DOOOOING IT")

    def do_concat(a, b):
        return a + b

    owner.simple_operation.bind_concat(do_concat)
    owner.simple_operation.bind_addnumbers(do_addnumbers)
    owner.simple_operation.bind_do_it(do_it)

    owner.simple_operation.bind_do_it(lambda: logger.debug("DOING IT Lambda_style"))
    consumers = [
        mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.CONSUMER
        )
        for _ in range(10)
    ]


    for idx, c in enumerate(consumers):
        logger.info("Invoking consumer %s...", idx)
        res = c.simple_operation.addnumbers(a=3 + idx, b=4 + idx)
        logger.info("Consumer %s got %s", idx, res)
        assert res == 3 + idx + 4 + idx
    mgr.disconnect()

@pytest.mark.skip(reason="multiprocessing cannot share amqtt fixture")
def testParallelOperationRequests(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    owner = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.OWNER
    )

    assert "bind_do_it" in dir(owner.simple_operation)
    assert "bind_addnumbers" in dir(owner.simple_operation)

    def do_addnumbers(a, b):
        return a + b

    # noinspection PyUnusedLocal
    def do_it():
        logger.debug("DOOOOING IT")

    def do_concat(a, b):
        return a + b

    owner.simple_operation.bind_concat(do_concat)
    owner.simple_operation.bind_addnumbers(do_addnumbers)
    owner.simple_operation.bind_do_it(do_it)
    owner.simple_operation.bind_do_it(lambda: logger.debug("DOING IT Lambda_style"))

    with Pool(10) as p:
        result = p.map(create_client_and_ask, list(range(10)))
        assert result == list(range(10))
    mgr.disconnect()

def testEvents(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    eventer = mgr.create_asset(
        "eventer", res_url + "simpleevent.json", mode=Mode.OWNER
    )
    listener = mgr.create_asset(
        "eventer", res_url + "simpleevent.json", mode=Mode.CONSUMER
    )


    cb_counter = []

    def bool_cb(ts, **kwargs):
        cb_counter.append(1)

    def string_cb(ts, **kwargs):
        cb_counter.append(1)

    def object_cb(ts, **kwargs):
        cb_counter.append(1)

    listener.simpleevent.bool_happened.on_event(bool_cb)
    listener.simpleevent.string_happened.on_event(string_cb)
    listener.simpleevent.object_happened.on_event(object_cb)

    eventer.simpleevent.bool_happened(bool_payload=True)
    eventer.simpleevent.string_happened(string_payload="it'samee")
    eventer.simpleevent.object_happened(object_payload={"x": 42.1, "y": 3.14159})

    time.sleep(2)
    logger.debug("Waited 2 secs")
    assert len(cb_counter) == 3
    mgr.disconnect()

def test_events_named_callbacks(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    eventer = mgr.create_asset(
        "eventer", res_url + "simpleevent.json", mode=Mode.OWNER
    )

    listener2 = mgr.create_asset(
        "eventer", res_url + "simpleevent.json", mode=Mode.CONSUMER
    )


    cb_counter = []

    # pylint: disable=unused-argument

    def bool_cb_named(_ts, bool_payload):
        cb_counter.append(1)

    def string_cb_named(_ts, string_payload):
        cb_counter.append(1)

    def object_cb_named(_ts, object_payload):
        cb_counter.append(1)

    listener2.simpleevent.bool_happened.on_event(bool_cb_named)
    listener2.simpleevent.string_happened.on_event(string_cb_named)
    listener2.simpleevent.object_happened.on_event(object_cb_named)

    eventer.simpleevent.bool_happened(bool_payload=True)
    eventer.simpleevent.string_happened(string_payload="it'samee")
    eventer.simpleevent.object_happened(object_payload={"x": 42.1, "y": 3.14159})
    time.sleep(2)

    assert len(cb_counter) == 3
    mgr.disconnect()

def test_empty_event(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    eventer = mgr.create_asset(
        "eventer", res_url + "simpleevent.json", mode=Mode.OWNER
    )

    listener2 = mgr.create_asset(
        "eventer", res_url + "simpleevent.json", mode=Mode.CONSUMER
    )

    callback_called = False

    def empty_callback(_ts):
        nonlocal callback_called
        callback_called = True

    listener2.simpleevent.empty_happened.on_event(empty_callback)
    eventer.simpleevent.empty_happened()
    time.sleep(2)
    assert callback_called
    mgr.disconnect()

def test_class_callbacks(mqtt_broker):
    class AdderDoerClass:
        def __init__(self):
            self.val = 0

        def addnumbers(self, a, b):
            self.val = a + b
            return a + b

        def do_it(self):
            self.val += 1


    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    owner = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.OWNER
    )
    adder_doer = AdderDoerClass()
    owner.simple_operation.bind_addnumbers(adder_doer.addnumbers)
    owner.simple_operation.bind_do_it(adder_doer.do_it)

    consumer = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.CONSUMER
    )

    consumer.simple_operation.addnumbers(a=5, b=3)
    consumer.simple_operation.do_it()

    time.sleep(2)

    assert adder_doer.val == 9
    mgr.disconnect()


def test_integer_numbers(mqtt_broker):
    def add_anything(a, b):
        return a + b


    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_num_int", "test_num_int")
    asset = mgr.create_asset(
        "test_num_int", res_url + "integer_number_operation.json"
    )

    asset.simple_operation.bind_addintegers(add_anything)
    asset.simple_operation.bind_addnumbers(add_anything)

    asset_proxy = mgr.create_asset_proxy("test_num_int", "test_num_int")
    assert asset_proxy.simple_operation.addintegers(a=2, b=2) == 4
    assert asset_proxy.simple_operation.addnumbers(a=2.0, b=2.0) == 4.0
    mgr.disconnect()

def test_different_namespaces(mqtt_broker):

    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")

    d1_a1 = mgr.create_asset("d1_a1", res_url + "simpleint.json", namespace="d1")
    d1_a2 = mgr.create_asset("d1_a2", res_url + "simpleint.json", namespace="d1")
    d2_a1 = mgr.create_asset("d2_a1", res_url + "simpleint.json", namespace="d2")
    d2_a2 = mgr.create_asset("d2_a2", res_url + "simpleint.json", namespace="d2")
    inherit_a2 = mgr.create_asset("i_a2", res_url + "simpleint.json")


    d1_a1_c = mgr.create_asset(
        "d1_a1", res_url + "simpleint.json", mode=Mode.CONSUMER, namespace="d1"
    )
    d1_a2_c = mgr.create_asset(
        "d1_a2", res_url + "simpleint.json", mode=Mode.CONSUMER, namespace="d1"
    )
    d2_a1_c = mgr.create_asset(
        "d2_a1", res_url + "simpleint.json", mode=Mode.CONSUMER, namespace="d2"
    )
    d2_a2_c = mgr.create_asset(
        "d2_a2", res_url + "simpleint.json", mode=Mode.CONSUMER, namespace="d2"
    )
    explicit_a2_c = mgr.create_asset(
        "i_a2",
        res_url + "simpleint.json",
        mode=Mode.CONSUMER,
        namespace="test_arena2036",
    )


    d1_a1.simpleint.num.value = 42
    d2_a1.simpleint.num.value = 42
    d1_a2.simpleint.num.value = 42
    d2_a2.simpleint.num.value = 42
    inherit_a2.simpleint.num.value = 42

    time.sleep(2)

    assert d1_a1_c.simpleint.num.value == 42
    assert d1_a2_c.simpleint.num.value == 42
    assert d2_a1_c.simpleint.num.value == 42
    assert d2_a2_c.simpleint.num.value == 42
    assert explicit_a2_c.simpleint.num.value == 42
    mgr.disconnect()

def test_operation_context(mqtt_broker):
    from assets2036py import context


    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    sink = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.CONSUMER
    )

    owner = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.OWNER
    )


    def do_addnumbers(a, b):
        req_id = context.req_id
        assert req_id is not None
        return a + b

    owner.simple_operation.bind_addnumbers(do_addnumbers)

    time.sleep(1)

    res = sink.simple_operation.addnumbers(a=5, b=6)
    assert res == 11
    mgr.disconnect()

def test_asset_disconnect_callback(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    mgr2 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_2")
    asset = mgr.create_asset("test_asset", res_url + "simple_prop.json")
    callback_called = False

    def disconnect_callback(*args):
        nonlocal callback_called
        callback_called = True
        logger.debug("DISCONNECTED: %s", args)

    def run_asset():
        for i in range(5):
            asset.simple_prop_json.my_property.value = i
            time.sleep(0.5)
        asset.disconnect()

    Thread(target=run_asset, daemon=True).start()

    asset_proxy = mgr2.create_asset_proxy("test_arena2036", "test_asset")
    asset_proxy.on_disconnect(disconnect_callback)
    for _ in range(20):
        if callback_called:
            break
        time.sleep(0.5)

    assert callback_called

def test_is_proxy_asset_online(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    mgr2 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_2")
    asset = mgr.create_asset("test_asset", res_url + "simple_prop.json")
    asset.simple_prop_json.my_property.value = 42

    asset_proxy = mgr2.create_asset_proxy("test_arena2036", "test_asset")
    assert asset_proxy.is_online
    asset.disconnect()
    time.sleep(1)
    assert not asset_proxy.is_online

def test_proxy_not_online_exception(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    asset = mgr.create_asset("test_asset", res_url + "simple_prop.json")
    asset.simple_prop_json.my_property.value = 42
    time.sleep(1)

    def create_proxy():

        mgr2 = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_2")

        mgr2.create_asset_proxy("test_arena2036", "test_asset", 3)
        return True

    assert create_proxy()
    asset.disconnect()
    time.sleep(1)
    with pytest.raises(AssetNotOnlineError):
        create_proxy()

def test_lazy_load_events(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    _source = mgr.create_asset("source", res_url + "simpleevent.json")
    sink = mgr.create_asset_proxy("test_arena2036", "source", lazy_loading=True)

    assert "bool_happened" not in dir(sink.simpleevent)

    # access event name
    _ = sink.simpleevent.bool_happened.name

    assert "bool_happened" in dir(sink.simpleevent)


def test_lazy_load_properties(mqtt_broker):
    mgr = AssetManager(mqtt_broker["host"], mqtt_broker["port"], "test_arena2036", "test_endpoint_1")
    source = mgr.create_asset("source", res_url + "all_property_model.json")
    sink = mgr.create_asset(
        "source",
        res_url + "all_property_model.json",
        mode=Mode.CONSUMER,
        lazy_loading=True,
    )


    # Set the properties in the source node
    source.all_properties.a_switch.value = True
    source.all_properties.an_int.value = 42
    source.all_properties.a_float.value = 4.2
    source.all_properties.a_text.value = "Hullo"
    source.all_properties.an_object.value = {"x": 5, "y": 6}
    source.all_properties.a_list.value = [1, 2, 3, 4]

    # Check that the sink does not have the properties set up yet
    assert "a_switch" not in dir(sink.all_properties)
    assert "an_int" not in dir(sink.all_properties)
    assert "a_float" not in dir(sink.all_properties)
    assert "a_text" not in dir(sink.all_properties)
    assert "an_object" not in dir(sink.all_properties)
    assert "a_list" not in dir(sink.all_properties)

    # Access all values once
    _ = sink.all_properties.a_switch.value
    _ = sink.all_properties.an_int.value
    _ = sink.all_properties.a_float.value
    _ = sink.all_properties.a_text.value
    _ = sink.all_properties.an_object.value
    _ = sink.all_properties.a_list.value

    # Check that now all properties are available as attributes
    assert "a_switch" in dir(sink.all_properties)
    assert "an_int" in dir(sink.all_properties)
    assert "a_float" in dir(sink.all_properties)
    assert "a_text" in dir(sink.all_properties)
    assert "an_object" in dir(sink.all_properties)
    assert "a_list" in dir(sink.all_properties)

    # Sleep shortly for MQTT to update
    time.sleep(2)

    # Now assert that the properties also have the right value

    assert sink.all_properties.a_switch.value
    assert sink.all_properties.an_int.value == 42
    assert abs(4.2 - sink.all_properties.a_float.value) < 1e-5
    assert sink.all_properties.a_text.value == "Hullo"
    assert sink.all_properties.an_object.value == {"x": 5, "y": 6}
    assert sink.all_properties.a_list.value == [1, 2, 3, 4]
    mgr.disconnect()
