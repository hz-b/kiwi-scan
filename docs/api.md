# Public API

`kiwi-scan` can be embedded directly as a Python library, for example inside a Python IOC or another beamline control application.

The command-line tools use the library API: they build a `ScanConfig`, load scan/plugin implementations, and then create or execute a scan object. 
The public API is described below.

## Supported public API

For subclasses of `BaseScan`, only the documented constructor and non-private methods should be treated as public. Attributes and methods starting with `_` are internal implementation details.

### 1. Startup 

Search and load scan engines and plugins

```python
import kiwi_scan

kiwi_scan.load_all_plugins()
kiwi_scan.load_all_scan_types()
```

### 2. Configuration and YAML loading

Building scan configurations in Python:

- `kiwi_scan.datamodels.ActuatorConfig`
- `kiwi_scan.datamodels.ScanDimension`
- `kiwi_scan.datamodels.ScanConfig`
- `kiwi_scan.datamodels.TriggerAction`
- `kiwi_scan.datamodels.ScanTriggers`
- `kiwi_scan.datamodels.SubscriptionConfig`

Loading scan configurations from YAML:
- `kiwi_scan.yaml_loader.yaml_loader`
- `kiwi_scan.yaml_loader.parse_replacements`
- `kiwi_scan.yaml_loader.get_env_replacements`

### 3. Runtime scan API

Helper functions for creating scan objects:

- `kiwi_scan.scan.tools.create_scan_with_config()`
  - create a scan object without starting it
- `kiwi_scan.scan.tools.scan_with_config()`
  - create and execute a scan synchronously

The scan object can be used via its interface defined in kiwi_scan.scan,scan_abs.py.
Derived scan classes have to implement the following methods:

- `scan.execute()`
- `scan.load_data()`
- `scan.get_output_file()`
- `scan.get_value(name, with_metadata=False)`
- `scan.get_actuator(name)`
- `scan.get_actuators()`
- `scan.set_data_writing_enabled()`
- `scan.get_data_writing_enabled()`
- `scan.busy`
- `scan.position`
- `scan.stop()` 

### 4. Extension API

- `kiwi_scan.scan.registry.register_scan` - register costum scan types
- `kiwi_scan.plugin.registry.register_plugin` - register custom plugins
- `kiwi_scan.load_all_plugins()` - search load plugins, set KIWI_SCAN_PLUGIN_PATH for custom plugins
- `kiwi_scan.load_all_scan_types()` - search and load scan types, set KIWI_SCAN_SCAN_PATH for custom scan engines


### 5. Data loader API

Load scan data:

- `kiwi_scan.dataloader.DataLoader`
- `kiwi_scan.metadata_loader.parse_metadata_file()`

## What is not public API

- CLI entry-point modules such as `scan_runner`, `scanplotter_cli`, and `actuator_runner`
- implementation packages such as `scan_concrete.*`, `actuator_concrete.*`, and `monitor_concrete.*`
- raw registry dictionaries such as `SCAN_REGISTRY`,`PLUGIN_REGISTRY`, `MONITOR_TYPES`
- any name starting with `_`

## Library integration example

This is an example for embedding `kiwi-scan` in another Python process.

```python
import threading
import kiwi_scan
import logging

from kiwi_scan.datamodels import ActuatorConfig, ScanConfig, ScanDimension
from kiwi_scan.scan.tools import create_scan_with_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
)

kiwi_scan.load_all_plugins()
kiwi_scan.load_all_scan_types()

cfg = ScanConfig(
    actuators={
        "energy": ActuatorConfig(
            type="epics",
            pv="IOC:MONO:SetEnergy",
            rb_pv="IOC:MONO:GetEnergy",
            status_pv="IOC:MONO:State",
            ready_value=0,
            stop_pv="IOC:MONO:Stop",
            stop_command=1,
            in_position_band=0.01,
            dwell_time=0.05,
        )
    },
    detector_pvs=["IOC:DET:COUNTS"],
    scan_dimensions=[
        ScanDimension(
            actuator="energy",
            start=400.0,
            stop=410.0,
            steps=11,
        )
    ],
    output_file="ioc_scan.txt",
    data_dir=".",
    include_timestamps=True,
)

scan = create_scan_with_config("linear", cfg)
if scan is None:
    raise RuntimeError("Failed to create scan")

worker = threading.Thread(target=scan.execute, name="kiwi-scan-worker")
worker.start()

try:
    while worker.is_alive():
        print("busy:", scan.busy)
        print("position:", scan.position)
        print("last detector value:", scan.get_value("IOC:DET:COUNTS"))
        print("last scan timestamp:", scan.get_value("TS-ISO8601"))
        time.sleep(0.5)
finally:
    worker.join()

print("scan finished")
```
