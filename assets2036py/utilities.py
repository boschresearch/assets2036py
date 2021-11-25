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

import inspect
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
    """ Provide arbitrary data to all called functions
    Usage:
        Set arbitrary property:

        context.set("name","monty")

        All subsequently called functions (in the same stack)
        can access the data directly:

        print(context.name) # outputs "monty"

    """
    class __Context:
        """
        Inner instance class
        """

        def __init__(self):
            self.data = {}

        def __str__(self):
            return repr(self) + self.val

        def __getattr__(self, name):
            frame = inspect.currentframe()
            if name not in self.data:
                return None
            for f in inspect.getouterframes(frame):
                if f.frame in self.data[name]:
                    return self.data[name][f.frame]
            return None

        def set(self, name, val):
            """set or update attribute
            Args:
                name (str): name of the attribute
                val: value of the attribute 
            """
            frame = inspect.getouterframes(inspect.currentframe())[1].frame
            if name not in self.data:
                self.data[name] = {}
            self.data[name][frame] = val

        def free(self):
            frame = inspect.getouterframes(inspect.currentframe())[1].frame
            for var in self.data.copy():
                if frame in self.data[var]:
                    del self.data[var][frame]
                    if not self.data[var]:
                        del self.data[var]

    instance = None

    def __init__(self):
        if not Context.instance:
            Context.instance = Context.__Context()

    def __getattr__(self, name):
        return getattr(self.instance, name)


context = Context()
