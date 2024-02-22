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
