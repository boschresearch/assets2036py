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

from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))


with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='assets2036py',
    version='0.0.18',
    url='https://github.com/boschresearch/assets2036spy',
    license='BIOS',
    author='Daniel Ewert (CR/APA3 G6/BD-BBI)',
    author_email='Daniel.Ewert@de.bosch.com',
    description='helper library to easily implement assets2036',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['jsonschema', 'paho-mqtt', "python-dateutil"],
    package_data={"assets2036py": ["resources/*.json"]}
)
