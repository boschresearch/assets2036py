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

import ssl
import json
import os
import logging
from urllib.request import urlopen
from threading import Thread, Event
from assets2036py import Asset, Mode, ProxyAsset
from assets2036py.exceptions import AssetNotFoundError, OperationTimeoutException, AssetNotOnlineError
from assets2036py.communication import CommunicationClient, MQTTClient
from assets2036py.utilities import get_resource_path


logger = logging.getLogger(__name__)

# pylint: disable=line-too-long


class AssetManager:
    """Primary entrypoint for working with assets.
    Manages the creation of assets and asset proxies (the latter are used to talk to remote assets).

    """

    def __init__(self, host: str, port: int, namespace: str, endpoint_name: str, communication_client: CommunicationClient = MQTTClient):
        """
        Args:
            host (str): URL of MQTT broker or registry
            port (int): Port of MQTT broker or registry
            namespace (str): default namespace to use for asset creation
            endpoint_name (str): name of the endpoint to use
            communication_client (CommunicationClient, optional): Backend to use for communication. Defaults to MQTTClient.
        """
        self.endpoint_name = endpoint_name
        self.port = port
        self.host = host
        self.namespace = namespace
        self.client = communication_client(client_id=endpoint_name)
        self.client.connect(host=host, port=port,
                            namespace=namespace, endpoint_name=endpoint_name)
        self._endpoint = None
        self._shutdown = Event()

    def disconnect(self):
        """disconnect underlying communication client
        """
        self._shutdown.set()
        self.client.disconnect()

    @property
    def healthy(self) -> bool:
        """get/set healthy state of endpoint

        Returns:
            bool: healthy flag state
        """
        # pylint: disable=protected-access
        if self._endpoint:
            return self._endpoint._endpoint.healthy.value
        else:
            return False

    @healthy.setter
    def healthy(self, val: bool):
        """set health state

        Args:
            val (bool):  healthy flag state
        """
        # pylint: disable=protected-access
        if self._endpoint:
            self._endpoint._endpoint.healthy.value = val

    def set_healthy_callback(self, callback: callable, interval: int = 30, kill_on_unhealthy: bool = False):
        """Regularly calls the given callback and sets the healthy state accordingly

        Optionally kills the complete application if healthy resolves to False

        Args:
            callback (function): callback that will be called each <interval> seconds. Receives no parameters, must return True or False
            interval (int, optional): callback will be called every <interval> seconds. Defaults to 30.
            kill_on_unhealthy (bool, optional): If set to True, os._exit(-1) is called and app is terminated. Defaults to False.
        """
        def monitor_loop():
            while not self._shutdown.is_set():
                self.healthy = self._self_ping() and callback()
                if kill_on_unhealthy and not self.healthy:
                    # pylint: disable=protected-access
                    os._exit(-1)
                self._shutdown.wait(interval)
        Thread(target=monitor_loop,
               name=f"{self.endpoint_name}_Monitor").start()

    def _self_ping(self) -> bool:
        """internal helper to check if connection to broker is still up
        Returns:
            bool: True if ping successfully, else False
        """
        if not self._endpoint:
            return False
        try:
            proxy_self = self.create_asset_proxy(
                self.namespace, self.endpoint_name)
            # pylint: disable=protected-access
            proxy_self._endpoint.ping(timeout=2)
            return True
        except (OperationTimeoutException, AssetNotFoundError) as ote:
            logger.error("self ping failed: %s", ote)
            return False

    def _create_endpoint_asset(self) -> Asset:
        """create the endpoint asset (for internal use)

        Returns:
            Asset: Endpoint asset
        """
        with open(get_resource_path("_endpoint.json")) as file:
            endpoint_sm_definition = json.load(file)
        endpoint = self.create_asset(
            self.endpoint_name, endpoint_sm_definition, create_endpoint=False)

        # pylint: disable=protected-access
        endpoint._endpoint.online.value = True
        endpoint._endpoint.healthy.value = False
        endpoint._endpoint.bind_shutdown(self.shutdown)
        endpoint._endpoint.bind_restart(self.restart)
        endpoint._endpoint.bind_ping(lambda: None)

        # quick hack
        def set_online(_client, _userdata, _flags, _rc):
            endpoint._endpoint.online.value = True
        self.client.on_connect(set_online)

        self._endpoint = endpoint

    def shutdown(self):
        """Disconnect from broker and shut down (gracelessly)
        """
        logger.debug("Shutting down.")
        self.disconnect()
        # pylint: disable=protected-access
        os._exit(0)

    def restart(self):
        """placeholder for endpoint operation "restart"
        """
        logger.debug("Restarting. TBI")
        self.shutdown()

    def create_asset(self, name: str, *sub_models: str, mode=Mode.OWNER, namespace=None, create_endpoint=True) -> Asset:
        """Create a new Asset

        Args:
            name (str): Name of the asset to create
            mode (int, optional): set mode to consumer or owner. Defaults to Mode.OWNER.
            namespace (str, optional): Namespace in which asset is created. If not set, namespace of asset manager is used. Defaults to None.

        Returns:
            Asset: Newly created asset
        """

        if not namespace:
            namespace = self.namespace
        asset = Asset(name, namespace, *sub_models, mode=mode, communication_client=self.client,
                      endpoint_name=self.endpoint_name)
        if create_endpoint and not self._endpoint:
            self._create_endpoint_asset()
        return asset

    def join(self, timeout=None):
        """Wait for the background tasks to complete
        """
        self.client.join(timeout)

    # pylint: disable=keyword-arg-before-vararg
    def query_assets(self, namespace=None, *submodel_names: str) -> set:
        """return a set of those assets that implement all the given submodels

        Args:
            namespace (str, optional): Namespace to search. Defaults to None.

        Returns:
            list: list of found asset names
        """

        return self.client.query_asset_names(namespace, *submodel_names)

    def create_asset_proxy(self, namespace: str, name: str, wait_seconds_until_online: int = -1) -> ProxyAsset:
        """Returns the asset with the given name if found, raises AssetNotFoundError otherwise

        Args:
            namespace (str): namespace to use
            name (str): name of asset to use
            wait_seconds_until_online (int): seconds to wait for the asset to register as online.
                -1 ignores online state and initiates asset (default: -1)

        Raises:
            AssetNotFoundError: raised if Asset cannot be found
            AssetNotOnlineError: raised if Asset doesn't come online in set time

        Returns:
            Asset: returns Asset proxy to be used to communicate with remote asset
        """
        submodels = self.client.query_submodels_for_asset(namespace, name)
        if not submodels:
            raise AssetNotFoundError
        # verified_submodels = self._get_submodel_schemata(submodels)

        proxy_asset = ProxyAsset(name, namespace, *submodels.values(), mode=Mode.CONSUMER, communication_client=self.client,
                                 endpoint_name=self.endpoint_name)
        if wait_seconds_until_online >= 0:
            online = proxy_asset.wait_for_online(wait_seconds_until_online)
            if not online:
                raise AssetNotOnlineError
        return proxy_asset

    def _get_submodel_schemata(self, sub_models):
        # pylint: disable=protected-access,broad-except
        schemata = []
        for meta in sub_models.values():
            try:
                url = meta["submodel_url"]
                with urlopen(url, context=ssl._create_unverified_context()) as response:
                    schemata.append(json.load(response))
            except Exception as loadexc:
                logger.warning(
                    "Can't load schema from url \"%s\": %s", url, loadexc)
                try:
                    schemata.append(meta["submodel_definition"])
                    logger.debug(
                        "Using schema from submodel_definition")
                except Exception as missingexc:
                    logger.error(
                        "No submodel schema retrievable: %s", missingexc)
        return schemata
