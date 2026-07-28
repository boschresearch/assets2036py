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
from logging import LogRecord

from .assets import SubModel


class AssetLoggingHandler(logging.Handler):
    """
    Custom Logging handler class to log via MQTT using the _endpoint of an asset.
    """

    def __init__(self, endpoint: SubModel, level: int = 0) -> None:
        super().__init__(level)
        self._endpoint = endpoint

    def emit(self, record: LogRecord) -> None:
        self._endpoint.log(entry=self.format(record))
