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

import contextvars
import os
import re


def sanitize(s):
    # keep minus sings as underscores
    s = re.sub("-", "_", s)
    # Remove invalid characters
    s = re.sub('[^0-9a-zA-Z_]', '', s)
    # Remove leading characters until we find a letter or underscore
    s = re.sub('^[^a-zA-Z_]+', '', s)
    return s


res_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "resources")


def get_resource_path(filename):
    return os.path.join(res_path, filename)


class Context:
    """Thread-safe context for passing request data to operation callbacks.

    Uses contextvars.ContextVar under the hood — each thread/task gets its
    own copy automatically.

    Usage::

        context.set("req_id", "abc123")
        print(context.req_id)  # "abc123"
        context.free()
        print(context.req_id)  # None
    """

    def __init__(self):
        self._vars: dict[str, contextvars.ContextVar] = {}

    def set(self, name: str, val) -> None:
        """Set a context variable.

        Args:
            name: Name of the variable.
            val: Value to store.
        """
        if name not in self._vars:
            self._vars[name] = contextvars.ContextVar(f"ctx_{name}", default=None)
        self._vars[name].set(val)

    def free(self) -> None:
        """Reset all context variables to None."""
        for var in self._vars.values():
            var.set(None)

    def __getattr__(self, name: str):
        if name.startswith("_") or name in ("set", "free"):
            raise AttributeError(name)
        if name in self._vars:
            return self._vars[name].get(None)
        return None


context = Context()
