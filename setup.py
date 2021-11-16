from setuptools import setup, find_packages
from codecs import open
from os import path, getenv

here = path.abspath(path.dirname(__file__))

BUILD_ID = "." + getenv("CI_BUILD_ID") if getenv("CI_BUILD_ID") else ""


with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='assets2036py',
    version='0.0.12' + BUILD_ID,
    url='https://github.com/boschresearch/assets2036-submodels',
    license='BIOS',
    author='Daniel Ewert (CR/APA3 G6/BD-BBI)',
    author_email='Daniel.Ewert@de.bosch.com',
    description='helper library to easily implement assets2036',
    long_description=long_description,
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['jsonschema', 'paho-mqtt', "python-dateutil"],
    package_data={"assets2036py": ["resources/*.json"]}
)
