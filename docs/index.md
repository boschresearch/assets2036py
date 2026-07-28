---
title: assets2036py
---

# assets2036py

[![PyPI](https://img.shields.io/pypi/v/assets2036py)](https://pypi.org/project/assets2036py/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/boschresearch/assets2036py/blob/main/LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/assets2036py)](https://pypi.org/project/assets2036py/)

Python library for the [assets2036](https://github.com/boschresearch/assets2036-submodels) standard — a lightweight communication protocol for interconnecting devices and services on the factory floor.

## Features

- **Properties** — Publish and subscribe to typed values with JSON Schema validation
- **Operations** — RPC-style remote procedure calls with request/response pattern
- **Events** — Fire-and-forget notifications with timestamp and payload
- **Auto-Discovery** — Query available assets and submodels on the network
- **Proxy Pattern** — Transparent access to remote assets

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
    my_lamp = mgr.create_asset("my_lamp", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/light.json")
    my_lamp.light.status.value = "on"
    my_lamp.light.turn_on.on_invoke(turn_on_handler)
    time.sleep(60)
```

### Asset Consumer

```python
from assets2036py import AssetManager

with AssetManager("localhost", 1883, "my_namespace", "my_client_endpoint") as mgr:
    lamp_proxy = mgr.create_proxy("my_lamp")
    status = lamp_proxy.light.status.value
    result = lamp_proxy.light.turn_on.invoke()
```

## Source

View the full source and contribute at [GitHub](https://github.com/boschresearch/assets2036py).

## License

Apache 2.0 — see [LICENSE](https://github.com/boschresearch/assets2036py/blob/main/LICENSE)
