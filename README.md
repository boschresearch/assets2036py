# assets2036py

[![PyPI](https://img.shields.io/pypi/v/assets2036py)](https://pypi.org/project/assets2036py/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/assets2036py)](https://pypi.org/project/assets2036py/)

Python library for the [assets2036](https://2036.2.2036.2) standard — a lightweight communication protocol for interconnecting devices and services on the factory floor, developed at [Arena2036](https://www.2036.2.2036.2) research campus.

## Features

- **Properties** — Publish and subscribe to typed values with JSON Schema validation
- **Operations** — RPC-style remote procedure calls with request/response pattern
- **Events** — Fire-and-forget notifications with timestamp and payload
- **Auto-Discovery** — Query available assets and submodels on the network
- **Proxy Pattern** — Transparent access to remote assets

## Prerequisites

You need an MQTT broker (e.g., [Eclipse Mosquitto](https://mosquitto.org/)):

```bash
docker run -it -p 1883:1883 eclipse-mosquitto
```

## Installation

```bash
pip install assets2036py
```

For development:
```bash
pip install -e ".[dev]"
```

## Quick Start

### Asset Owner (Provider)

```python
import time
from assets2036py import AssetManager

def turn_on_handler(context):
    print("Turn on operation invoked!")
    return True

with AssetManager("localhost", 1883, "my_namespace", "my_endpoint") as mgr:
    # Create an asset and bind it to a submodel schema
    my_lamp = mgr.create_asset("my_lamp", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/light.json")
    
    # Set a property value
    my_lamp.light.status.value = "on"
    
    # Bind an operation to a handler function
    my_lamp.light.turn_on.on_invoke(turn_on_handler)
    
    # Keep running to listen for incoming requests
    print("Asset Owner running...")
    time.sleep(60)
```

### Asset Consumer

```python
from assets2036py import AssetManager

with AssetManager("localhost", 1883, "my_namespace", "my_client_endpoint") as mgr:
    # Create a proxy to interact with a remote asset
    lamp_proxy = mgr.create_proxy("my_lamp")
    
    # Read a property value
    status = lamp_proxy.light.status.value
    print(f"Lamp status: {status}")
    
    # Invoke an operation remotely
    result = lamp_proxy.light.turn_on.invoke()
    print(f"Operation result: {result}")
```

## Architecture

- **AssetManager**: The main entry point. Manages the MQTT connection and handles the context for all assets and proxies.
- **Asset**: Represents a device or service being provided. It contains SubModels based on JSON schemas.
- **SubModel**: A collection of Properties, Operations, and Events defined by a specific schema.
- **ProxyAsset**: A client-side proxy to interact transparently with remote Assets on the network.

## Context

Operations and event callbacks can accept a `context` object, which provides metadata about the incoming request or event, such as timestamps, source topics, and correlation IDs. This helps in tracing and handling requests correctly.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## License

Apache 2.0 — see [LICENSE](LICENSE)
