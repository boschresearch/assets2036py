# Copyright (c) 2019 Robert Bosch GmbH
# All rights reserved.

class AssetNotFoundError(Exception):
    pass


class AssetExistsError(Exception):
    pass


class NotWritableError(Exception):
    pass

class OperationTimeoutException(Exception):
    pass


class NotReadableError(Exception):
    pass


class InvalidParameterException(Exception):
    pass