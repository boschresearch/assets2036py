# Assets2036Py

Helper lib for consuming and providing assets2036 assets.

## Usage
```python
from assets2036 import AssetManager

# for local MQTT broker with default port
mgr = AssetManager("localhost",1883,"my_namespace")

my_lamp = mgr.createAsset("my_lamp","https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/light.json")

my_lamp.light.light_on.value = True
```
For further examples take a look at the [tests](tests/test_asset.py).
