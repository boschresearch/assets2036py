
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
from unittest import TestCase
from threading import Thread
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor

from jsonschema import ValidationError
from assets2036py import Asset, Mode, AssetManager
from assets2036py.communication import MockClient
from assets2036py.exceptions import NotWritableError, AssetNotOnlineError
from .test_utils import get_msgs_for_n_secs, wipe_retained_msgs, res_url
logger = logging.getLogger(__name__)

HOST = os.getenv("MQTT_BROKER_URL", "localhost")
PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
# HINT: Add an .env file to the root of your project to set the environment variables


# pylint: disable=protected-access


def create_client_and_ask(idx):
    '''Helper function to test parallel operations
    '''
    mgr = AssetManager(HOST, PORT,
                       "test_arena2036", f"test_endpoint_{idx}")
    client = mgr.create_asset(
        "source", res_url + "simple_operation.json", mode=Mode.CONSUMER)
    return client.simple_operation.addnumbers(a=0, b=idx)


class TestAsset(TestCase):

    @classmethod
    def tearDownClass(cls):
        wipe_retained_msgs(HOST, "test_arena2036")

    def test_implement_sub_model_consumer(self):
        a = Asset("simpleasset", "test_arena2036", res_url + "simplebool.json",
                  communication_client=MockClient(),
                  mode=Mode.CONSUMER,
                  endpoint_name="test_endpoint_1")
        self.assertTrue("simple_bool" in dir(a))
        self.assertTrue("switch" in dir(a.simple_bool))
        self.assertTrue("value" in dir(a.simple_bool.switch))
        topic = a.simple_bool.switch._get_topic()
        self.assertEqual(
            topic, "test_arena2036/simpleasset/simple_bool/switch")

    def test_implement_sub_model_owner(self):
        a = Asset("simpleasset", "test_arena2036", res_url + "simplebool.json", communication_client=MockClient(),
                  mode=Mode.OWNER, endpoint_name="test_endpoint_1")
        self.assertTrue("simple_bool" in dir(a))
        self.assertTrue("switch" in dir(a.simple_bool))
        self.assertTrue("value" in dir(a.simple_bool.switch))
        topic = a.simple_bool.switch._get_topic()
        self.assertEqual(
            topic, "test_arena2036/simpleasset/simple_bool/switch")

    def test_put_value(self):
        a = Asset("simpleasset", "test_arena2036", res_url + "simplebool.json", communication_client=MockClient(),
                  mode=Mode.OWNER, endpoint_name="test_endpoint_1")
        a.simple_bool.switch.value = True
        self.assertEqual(a.simple_bool.switch.value, True)
        a.simple_bool.switch.value = False
        self.assertEqual(a.simple_bool.switch.value, False)

    def test_load_from_unverified_ssl(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        source = mgr.create_asset(
            "source", "https://arena2036-infrastructure.saz.bosch-si.com/arena2036_public/assets2036_submodels/raw/master/powerstate.json")
        source.powerstate.power_state.value = True

    def test_implement_complex_sub_model_consumer(self):
        a = Asset("activeshuttle", "test_arena2036", res_url + "activeshuttle.json",
                  communication_client=MockClient(),
                  mode=Mode.CONSUMER, endpoint_name="test_endpoint_1")

        self.assertTrue("active_shuttle" in dir(a))
        self.assertTrue("pose" in dir(a.active_shuttle))
        self.assertTrue("state" in dir(a.active_shuttle))
        self.assertTrue("waypoints" in dir(a.active_shuttle))

        self.assertIsInstance(a.active_shuttle.pose.value, type(None))

        def evil_func():
            a.active_shuttle.pose.value = {'x': 5.7, 'y': 3.5, 'z': 0.1}
        self.assertRaises(NotWritableError, evil_func)
        self.assertIsInstance(a.active_shuttle.pose.value, type(None))

    def test_all_property_types(self):
        a = Asset("PropertyTest", "test_arena2036", res_url + "all_property_model.json",
                  communication_client=MockClient(),
                  mode=Mode.OWNER, endpoint_name="test_endpoint_1")
        a.all_properties.a_switch.value = True
        self.assertEqual(a.all_properties.a_switch.value, True)
        a.all_properties.a_float.value = 4.2
        self.assertAlmostEqual(a.all_properties.a_float.value, 4.2, places=5)
        a.all_properties.a_text.value = "Hullo"
        self.assertEqual(a.all_properties.a_text.value, "Hullo")
        a.all_properties.an_object.value = {"x": 5, "y": 6}
        self.assertDictEqual(
            a.all_properties.an_object.value, {"x": 5, "y": 6})
        a.all_properties.a_list.value = [1, 2, 3, "a"]
        self.assertListEqual(a.all_properties.a_list.value, [1, 2, 3, "a"])

        def evil_func():
            a.all_properties.an_object.value = {"x": 5, "y": "booh"}

        self.assertRaises(ValidationError, evil_func)
        a.all_properties.an_int.value = 42
        self.assertEqual(a.all_properties.an_int.value, 42)

    def test_type_validation(self):
        a = Asset("PropertyTest", "test_arena2036", res_url + "all_property_model.json",
                  communication_client=MockClient(),
                  mode=Mode.OWNER, endpoint_name="test_endpoint_1")
        with self.assertRaises(ValidationError):
            a.all_properties.a_switch.value = 42
        self.assertEqual(a.all_properties.a_switch.value, None)

        with self.assertRaises(ValidationError):
            a.all_properties.an_int.value = 42.5

        a.all_properties.a_float.value = 42

        with self.assertRaises(ValidationError):
            a.all_properties.a_text.value = 42

        with self.assertRaises(ValidationError):
            a.all_properties.an_object.value = True

        with self.assertRaises(ValidationError):
            a.all_properties.an_object.value = {"x": 5, "z": 6}

        with self.assertRaises(ValidationError):
            a.all_properties.an_int_object.value = {"x": 5.1, "y": 6.0}

        with self.assertRaises(ValidationError):
            a.all_properties.a_list.value = {"x": 5, "z": 6}

    def test_implement_complex_sub_model_owner(self):
        a = Asset("activeshuttle", "test_arena2036", res_url + "activeshuttle.json",
                  communication_client=MockClient(),
                  mode=Mode.OWNER, endpoint_name="test_endpoint_1")

        self.assertTrue("active_shuttle" in dir(a))
        self.assertTrue("pose" in dir(a.active_shuttle))
        self.assertTrue("state" in dir(a.active_shuttle))
        self.assertTrue("waypoints" in dir(a.active_shuttle))

    def test_communication(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        source = mgr.create_asset(
            "source", res_url + "all_property_model.json")
        sink = mgr.create_asset(
            "source", res_url + "all_property_model.json", mode=Mode.CONSUMER)

        time.sleep(2)
        source.all_properties.a_switch.value = True
        source.all_properties.an_int.value = 42
        source.all_properties.a_float.value = 4.2
        source.all_properties.a_text.value = "Hullo"
        source.all_properties.an_object.value = {"x": 5, "y": 6}
        source.all_properties.a_list.value = [1, 2, 3, 4]

        logger.debug("Sleeping 2 secs")
        time.sleep(2)

        self.assertEqual(True, sink.all_properties.a_switch.value)
        self.assertEqual(42, sink.all_properties.an_int.value)
        self.assertAlmostEqual(
            4.2, sink.all_properties.a_float.value, places=5)
        self.assertEqual("Hullo", sink.all_properties.a_text.value)
        self.assertDictEqual(
            {"x": 5, "y": 6}, sink.all_properties.an_object.value)
        self.assertListEqual([1, 2, 3, 4], sink.all_properties.a_list.value)
        mgr.disconnect()

    def test_delete(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        source = mgr.create_asset(
            "deletetest", res_url + "all_property_model.json")

        time.sleep(2)
        source.all_properties.a_switch.value = True
        source.all_properties.an_int.value = 42
        source.all_properties.a_float.value = 4.2
        source.all_properties.a_text.value = "Hullo"
        source.all_properties.an_object.value = {"x": 5, "y": 6}
        source.all_properties.a_list.value = [1, 2, 3, 4]

        time.sleep(2)
        msgs = get_msgs_for_n_secs("test_arena2036/deletetest/#", 3)
        self.assertTrue(len(msgs) > 0)
        source.delete()
        logger.debug("Sleeping 3 secs")
        time.sleep(2)
        msgs = get_msgs_for_n_secs("test_arena2036/deletetest/#", 3)
        logger.debug("Received messages:")
        self.assertEqual(len(msgs), 0)
        mgr.disconnect()

    def test_retained_communication(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        source = mgr.create_asset(
            "source", res_url + "all_property_model.json")

        time.sleep(2)
        source.all_properties.a_switch.value = True
        source.all_properties.an_int.value = 42
        source.all_properties.a_float.value = 4.2
        source.all_properties.a_text.value = "Hullo"
        source.all_properties.an_object.value = {"x": 5, "y": 6}
        source.all_properties.a_list.value = [1, 2, 3, 4]

        sink = mgr.create_asset(
            "source", res_url + "all_property_model.json", mode=Mode.CONSUMER)

        time.sleep(2)

        self.assertEqual(True, sink.all_properties.a_switch.value)
        self.assertEqual(42, sink.all_properties.an_int.value)
        self.assertAlmostEqual(
            4.2, sink.all_properties.a_float.value, places=5)
        self.assertEqual("Hullo", sink.all_properties.a_text.value)
        self.assertDictEqual(
            {"x": 5, "y": 6}, sink.all_properties.an_object.value)
        self.assertListEqual([1, 2, 3, 4], sink.all_properties.a_list.value)
        mgr.disconnect()

    def test_communication_updates(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        source = mgr.create_asset(
            "source", res_url + "all_property_model.json", mode=Mode.OWNER)
        sink = mgr.create_asset(
            "source", res_url + "all_property_model.json", mode=Mode.CONSUMER)

        returns = []

        def cb(x):
            returns.append(x)

        sink.all_properties.an_int.on_change(cb)

        time.sleep(1)

        for idx in range(10):
            source.all_properties.an_int.value = idx
        time.sleep(3)
        logger.debug(returns)
        self.assertAlmostEqual(10, len(returns),
                               delta=2)  # VerneMQ sends the last received message twice? --> Sometimes 11 msgs...

    def test_submodel_meta_property(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        asset1 = mgr.create_asset("asset", res_url + "simplebool.json")
        self.assertTrue("simple_bool" in dir(asset1))
        self.assertTrue("_meta" in dir(asset1.simple_bool))
        self.assertEqual("test_arena2036/test_endpoint_1",
                         asset1.simple_bool._meta.value["source"])
        self.assertEqual(f'{res_url}simplebool.json',
                         asset1.simple_bool._meta.value["submodel_url"])

        asset2 = mgr.create_asset(
            "asset", res_url + "simplebool.json", mode=Mode.CONSUMER)
        self.assertTrue("simple_bool" in dir(asset2))
        self.assertFalse("_meta" in dir(asset2.simple_bool))

    def test_simple_operation(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        sink = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.CONSUMER)
        self.assertIn("simple_operation", dir(sink))
        self.assertIn("do_it", dir(sink.simple_operation))
        self.assertTrue(callable(sink.simple_operation.do_it))
        self.assertIn("addnumbers", dir(sink.simple_operation))
        self.assertTrue(callable(sink.simple_operation.addnumbers))

        owner = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.OWNER)
        self.assertIn("bind_do_it", dir(owner.simple_operation))
        self.assertIn("bind_addnumbers", dir(owner.simple_operation))

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
        owner.simple_operation.bind_do_it(
            lambda: logger.debug("DOING IT Lambda_style"))

        time.sleep(1)

        res = sink.simple_operation.addnumbers(a=5, b=6, timeout=3)
        res2 = sink.simple_operation.concat(a="hello", b="world", timeout=3)
        sink.simple_operation.do_it()
        self.assertEqual(11, res)
        self.assertEqual("helloworld", res2)

    def test_multi_parameter_operation(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        caller = mgr.create_asset(
            "source", res_url + "multi_param_operation.json", mode=Mode.CONSUMER)
        owner = mgr.create_asset(
            "source", res_url + "multi_param_operation.json", mode=Mode.OWNER)

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
            first=1, second="hello", third={"a": 1}, timeout=3)
        res2 = caller.multiparam.multiparam_2(
            first=1, second="hello", third={"a": 1}, timeout=3)
        res3 = caller.multiparam.multiparam_3(
            first=1, second="hello", third={"a": 1}, timeout=3)
        self.assertEqual([1, "hello", {"a": 1}], res1)
        self.assertEqual([1, "hello", {"a": 1}], res2)
        self.assertEqual([1, "hello", {"a": 1}], res3)

    def test_object_operation(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        sink = mgr.create_asset(
            "source", res_url + "object_operation.json", mode=Mode.CONSUMER)
        owner = mgr.create_asset(
            "source", res_url + "object_operation.json", mode=Mode.OWNER)

        def do_split(a):
            return {"firstletter": a[0], "lastletter": a[-1]}

        owner.object_operation.bind_splitstring(do_split)

        time.sleep(1)

        res = sink.object_operation.splitstring(a="IO")
        self.assertEqual({"firstletter": "I", "lastletter": "O"}, res)

    def test_array_operation(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        sink = mgr.create_asset(
            "source", res_url + "array_operation.json", mode=Mode.CONSUMER)
        owner = mgr.create_asset(
            "source", res_url + "array_operation.json", mode=Mode.OWNER)

        def do_give_array(length):
            return [1]*length

        owner.array_operation.bind_give_array(do_give_array)

        time.sleep(1)

        res = sink.array_operation.give_array(length=2, timeout=3)
        self.assertEqual([1, 1], res)

    def testMultiOperationRequests(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        owner = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.OWNER)
        self.assertIn("bind_do_it", dir(owner.simple_operation))
        self.assertIn("bind_addnumbers", dir(owner.simple_operation))

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
        owner.simple_operation.bind_do_it(
            lambda: logger.debug("DOING IT Lambda_style"))
        consumers = [mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.CONSUMER) for i in range(10)]

        for idx, c in enumerate(consumers):
            res = c.simple_operation.addnumbers(a=3+idx, b=4+idx)
            self.assertEqual(res, 3+idx + 4+idx)

    def testParallelOperationRequests(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        owner = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.OWNER)
        self.assertIn("bind_do_it", dir(owner.simple_operation))
        self.assertIn("bind_addnumbers", dir(owner.simple_operation))

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
        owner.simple_operation.bind_do_it(
            lambda: logger.debug("DOING IT Lambda_style"))

        with Pool(10) as p:
            result = p.map(create_client_and_ask, list(range(10)))
            self.assertEqual(result, list(range(10)))

    def testEvents(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        eventer = mgr.create_asset(
            "eventer", res_url + "simpleevent.json", mode=Mode.OWNER)
        listener = mgr.create_asset(
            "eventer", res_url + "simpleevent.json", mode=Mode.CONSUMER)

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
        eventer.simpleevent.object_happened(
            object_payload={"x": 42.1, "y": 3.14159})

        time.sleep(5)
        logger.debug("Waited 5 secs")
        self.assertEqual(len(cb_counter), 3)

    def test_events_named_callbacks(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        eventer = mgr.create_asset(
            "eventer", res_url + "simpleevent.json", mode=Mode.OWNER)

        listener2 = mgr.create_asset(
            "eventer", res_url + "simpleevent.json", mode=Mode.CONSUMER)

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
        eventer.simpleevent.object_happened(
            object_payload={"x": 42.1, "y": 3.14159})
        time.sleep(3)

        self.assertEqual(len(cb_counter), 3)

    def test_empty_event(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        eventer = mgr.create_asset(
            "eventer", res_url + "simpleevent.json", mode=Mode.OWNER)

        listener2 = mgr.create_asset(
            "eventer", res_url + "simpleevent.json", mode=Mode.CONSUMER)
        callback_called = False

        def empty_callback(_ts):
            nonlocal callback_called
            callback_called = True
        listener2.simpleevent.empty_happened.on_event(empty_callback)
        eventer.simpleevent.empty_happened()
        time.sleep(3)
        self.assertTrue(callback_called)

    def test_class_callbacks(self):

        class AdderDoerClass:
            def __init__(self):
                self.val = 0

            def addnumbers(self, a, b):
                self.val = a + b
                return a + b

            def do_it(self):
                self.val += 1

        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        owner = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.OWNER)
        adderDoer = AdderDoerClass()
        owner.simple_operation.bind_addnumbers(adderDoer.addnumbers)
        owner.simple_operation.bind_do_it(adderDoer.do_it)

        consumer = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.CONSUMER)
        consumer.simple_operation.addnumbers(a=5, b=3)
        consumer.simple_operation.do_it()

        time.sleep(2)
        self.assertEqual(adderDoer.val, 9)

    def test_parallel_healthy_flag(self):
        # logging.getLogger("assets2036py").setLevel(logging.ERROR)
        # init
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        provider = mgr.create_asset(
            "provider", res_url + "simple_operation.json", mode=Mode.OWNER)
        caller = mgr.create_asset(
            "provider", res_url + "simple_operation.json", mode=Mode.CONSUMER)

        def addnumbers(a, b):
            logger.debug("addnumbers called, waiting 5 secs..")
            time.sleep(5)
            logger.debug("Waited enough, returning.")
            return a + b

        provider.simple_operation.bind_addnumbers(addnumbers)

        # cause external operation call
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(
                caller.simple_operation.addnumbers, a=i, b=3) for i in range(10)]

            logger.debug("submitted calls!")
            # set healthy flag during operation execution
            while not all(f.done() for f in futures):
                time.sleep(1)
                mgr.healthy = True
                logger.debug("set the healthy flag true")
                time.sleep(1)
                mgr.healthy = False
                logger.debug("set the healthy flag false")
            for idx, future in enumerate(futures):
                self.assertEqual(future.result(), idx + 3)
            logger.debug("assertions were met!")

    def test_integer_numbers(self):
        def add_anything(a, b):
            return a + b

        mgr = AssetManager(HOST, PORT,
                           "test_num_int", "test_num_int")
        asset = mgr.create_asset(
            "test_num_int", res_url + "integer_number_operation.json")
        asset.simple_operation.bind_addintegers(add_anything)
        asset.simple_operation.bind_addnumbers(add_anything)

        asset_proxy = mgr.create_asset_proxy("test_num_int", "test_num_int")
        self.assertEqual(asset_proxy.simple_operation.addintegers(a=2, b=2), 4)
        self.assertEqual(
            asset_proxy.simple_operation.addnumbers(a=2.0, b=2.0), 4.0)

    def test_different_namespaces(self):
        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        d1_a1 = mgr.create_asset(
            "d1_a1", res_url + "simpleint.json", namespace="d1")
        d1_a2 = mgr.create_asset(
            "d1_a2", res_url + "simpleint.json", namespace="d1")
        d2_a1 = mgr.create_asset(
            "d2_a1", res_url + "simpleint.json", namespace="d2")
        d2_a2 = mgr.create_asset(
            "d2_a2", res_url + "simpleint.json", namespace="d2")
        inherit_a2 = mgr.create_asset(
            "i_a2", res_url + "simpleint.json")

        d1_a1_c = mgr.create_asset(
            "d1_a1", res_url + "simpleint.json", namespace="d1", mode=Mode.CONSUMER)
        d1_a2_c = mgr.create_asset(
            "d1_a2", res_url + "simpleint.json", namespace="d1", mode=Mode.CONSUMER)
        d2_a1_c = mgr.create_asset(
            "d2_a1", res_url + "simpleint.json", namespace="d2", mode=Mode.CONSUMER)
        d2_a2_c = mgr.create_asset(
            "d2_a2", res_url + "simpleint.json", namespace="d2", mode=Mode.CONSUMER)
        explicit_a2_c = mgr.create_asset(
            "i_a2", res_url + "simpleint.json", namespace="test_arena2036", mode=Mode.CONSUMER)

        d1_a1.simpleint.num.value = 42
        d2_a1.simpleint.num.value = 42
        d1_a2.simpleint.num.value = 42
        d2_a2.simpleint.num.value = 42
        inherit_a2.simpleint.num.value = 42

        time.sleep(4)

        self.assertEqual(d1_a1_c.simpleint.num.value, 42)
        self.assertEqual(d1_a2_c.simpleint.num.value, 42)
        self.assertEqual(d2_a1_c.simpleint.num.value, 42)
        self.assertEqual(d2_a2_c.simpleint.num.value, 42)
        self.assertEqual(explicit_a2_c.simpleint.num.value, 42)

    def test_operation_context(self):
        from assets2036py import context

        mgr = AssetManager(HOST, PORT,
                           "test_arena2036", "test_endpoint_1")
        sink = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.CONSUMER)

        owner = mgr.create_asset(
            "source", res_url + "simple_operation.json", mode=Mode.OWNER)

        def do_addnumbers(a, b):
            req_id = context.req_id
            self.assertIsNotNone(req_id)
            return a+b

        owner.simple_operation.bind_addnumbers(do_addnumbers)

        time.sleep(1)

        res = sink.simple_operation.addnumbers(a=5, b=6)
        self.assertEqual(res, 11)

    def test_asset_disconnect_callback(self):
        mgr = AssetManager(HOST, PORT, "test_arena2036", "test_endpoint_1")
        mgr2 = AssetManager(HOST, PORT, "test_arena2036", "test_endpoint_2")
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

        asset_proxy = mgr2.create_asset_proxy(
            "test_arena2036", "test_asset")
        asset_proxy.on_disconnect(disconnect_callback)
        for _ in range(20):
            my_prop = asset_proxy.simple_prop_json.my_property.value
            if callback_called:
                break
            time.sleep(0.5)

        self.assertTrue(callback_called)

    def test_is_proxy_asset_online(self):
        mgr = AssetManager(HOST, PORT, "test_arena2036", "test_endpoint_1")
        mgr2 = AssetManager(HOST, PORT, "test_arena2036", "test_endpoint_2")
        asset = mgr.create_asset("test_asset", res_url + "simple_prop.json")
        asset.simple_prop_json.my_property.value = 42

        asset_proxy = mgr2.create_asset_proxy(
            "test_arena2036", "test_asset")
        self.assertTrue(asset_proxy.is_online)
        asset.disconnect()
        time.sleep(1)
        self.assertFalse(asset_proxy.is_online)

    def test_proxy_not_online_exception(self):
        mgr = AssetManager(HOST, PORT, "test_arena2036", "test_endpoint_1")
        asset = mgr.create_asset("test_asset", res_url + "simple_prop.json")
        asset.simple_prop_json.my_property.value = 42
        time.sleep(1)

        def create_proxy():
            mgr2 = AssetManager(
                HOST, PORT, "test_arena2036", "test_endpoint_2")
            asset_proxy = mgr2.create_asset_proxy(
                "test_arena2036", "test_asset", 3)
            return True

        self.assertTrue(create_proxy())
        asset.disconnect()
        time.sleep(1)
        self.assertRaises(AssetNotOnlineError, create_proxy)
