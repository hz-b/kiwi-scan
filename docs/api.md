# Public API

`kiwi-scan` can be embedded directly as a Python library, for example inside a Python IOC or another beamline control application.

The command-line tools use the library API: they build a `ScanConfig`, load scan/plugin implementations, and then create or execute a scan object. 
The public API is described below.

## Supported public API

For subclasses of `BaseScan`, only the documented constructor and non-private methods should be treated as public. Attributes and methods starting with `_` are internal implementation details.

### 1. Startup 

Search and load scan engines and plugins:

```python
import kiwi_scan

kiwi_scan.load_all_plugins()
kiwi_scan.load_all_scan_types()
```

### 2. Configuration and YAML loading

Building scan configurations in Python:

- `kiwi_scan.datamodels.ActuatorConfig`
- `kiwi_scan.datamodels.JogConfig`
- `kiwi_scan.datamodels.ScanDimension`
- `kiwi_scan.datamodels.ScanConfig`
- `kiwi_scan.datamodels.MonitorConfig`
- `kiwi_scan.datamodels.TriggerAction`
- `kiwi_scan.datamodels.ScanTriggers`
- `kiwi_scan.datamodels.SubscriptionConfig`

Loading scan configurations from YAML:

- `kiwi_scan.yaml_loader.yaml_loader`
- `kiwi_scan.yaml_loader.parse_replacements`
- `kiwi_scan.yaml_loader.get_env_replacements`
- `kiwi_scan.yaml_loader.list_required_replacements`
- `kiwi_scan.yaml_loader.get_replacements_help_and_required`

### 3. Runtime scan API

Helper functions for creating scan objects:

- `kiwi_scan.scan.tools.create_scan_with_config()`
  - create a scan object without starting it
- `kiwi_scan.scan.tools.scan_with_config()`
  - create and execute a scan synchronously

The scan object can be used via its interface defined in kiwi_scan.scan.scan_abs.py.
Derived scan classes implement the following methods and properties:

- `scan.execute()`
- `scan.scan(positions, monitor=None)`
- `scan.load_data()`
- `scan.get_output_file()`
- `scan.get_value(name, default=None, with_metadata=False)`
- `scan.get_current_row_cache()`
- `scan.get_current_row_value(key, default=None)`
- `scan.get_actuator(name)`
- `scan.get_actuators()`
- `scan.set_data_writing_enabled(enabled)`
- `scan.get_data_writing_enabled()`
- `scan.busy`
- `scan.position`
- `scan.stop()` 

### 4. Generic IOC controller API

`kiwi_scan.ioc.controller.ScanIOCController` is the Python-only scan wrapper originally craeted for the generic scan IOC and it does not depend on `pythonSoftIOC`.
While it is not part of the core scan API, it is an reusable application layer between the EPICS records and the kiwi-scan runtime API that handles everything that is not EPICS specific.

The controller manages scan objects for each run, and exposes thread-safe state and data access methods.

#### Create a ScanIOCController

```python
from kiwi_scan.ioc.controller import ScanIOCController

controller = ScanIOCController(
    scan_type="linear",
    config_name="mono",
    config_dir="/path/to/config",
    data_dir="/path/to/data",
    replacements={"IOC_MONO": "UE52SGM1"},
)
```

The helper `kiwi_scan.ioc.controller.default_config_dir()` returns `KIWI_SCAN_CONFIG_DIR` when set, otherwise the packaged kiwi-scan configuration directory.

#### Runtime configuration

The selected configuration or scan type can only be changed while no scan is active:

- `controller.reload_config()` - reload the selected YAML configuration
- `controller.get_config_name()` / `controller.set_config_name(name)`
- `controller.get_scan_type()` / `controller.set_scan_type(scan_type)`
- `controller.dimension_defaults()` - return `(actuator, start, stop, steps, velocity)` for IOC record defaults
- `controller.make_dimension(...)` - validate values and create a `ScanDimension`

Runtime config switching with `set_config_name()` is available only when the controller was started with `config_name`. A controller created with `config_file` remains in fixed-file mode.

#### Scans

The scan lifecycle is intentionally split into preparation and execution:

- `controller.create_scan(dimensions)` - reload the base configuration, apply the requested dimensions, and create a scan object
- `controller.start(dimensions)` - prepare a scan by creating the scan object; status becomes `INITIALIZING`
- `controller.execute_current_scan()` - execute the prepared scan synchronously; status becomes `RUNNING`, then `IDLE` or `ERROR`
- `controller.stop()` - request a stop on the active scan

`execute_current_scan()` is synchronous and should normally run in a worker thread (for the IOC or similar applications like GUIs). 
`run_scan(dimensions)` combines preparation and synchronous execution.

Controller state is exposed through `kiwi_scan.ioc.datamodels.ScanIOCStatus`:

- `IDLE`
- `RUNNING`
- `INITIALIZING`
- `ERROR`

The latest human-readable state is available as `controller.message`. Scan creation and execution failures set the status to `ERROR` and store a single-line error message.

#### Scan Interface 

The following methods safely interface the current scan object and return defaults when no scan exists:

- `controller.get_busy()`
- `controller.get_position(default=float("nan"))`
- `controller.get_value(key, default=None)`
- `controller.get_output_file()`
- `controller.set_data_writing_enabled(enabled)`
- `controller.create_new_manifest()`
- `controller.get_manifest_file()`
- `controller.get_data_writing_enabled()`

The data-writing setting is kept as the default for future scans and is also forwarded to an active scan.


### 5. Extension API

- `kiwi_scan.scan.registry.register_scan` - register custom scan types
- `kiwi_scan.plugin.registry.register_plugin` - register custom plugins
- `kiwi_scan.load_all_plugins()` - search and load plugins; set `KIWI_SCAN_PLUGIN_PATH` for custom plugins
- `kiwi_scan.load_all_scan_types()` - search and load scan types; set `KIWI_SCAN_SCAN_PATH` for custom scan engines


### 6. Data loader API

Load scan data:

- `kiwi_scan.dataloader.DataLoader`
- `kiwi_scan.metadata_loader.parse_metadata_file()`

### 7. Export converters
API:

- `kiwi_scan.io.ExportScan`
- `kiwi_scan.io.ExportBundle`
- `kiwi_scan.io.load_export_bundle_from_scan_file()`
- `kiwi_scan.io.load_export_bundle_from_manifest()`
- `kiwi_scan.io.load_export_bundle_from_latest_manifest()`

Custom export converters can add support for exporting to additional file formats. The package comes with the kiwi2spec converter.
Registered converters can be selected by name.

- `kiwi_scan.export.ExportWriter` - Base class for implementing a new export format.
- `register_writer()`- Registers the converter.

## What is not public API

- CLI entry-point modules such as `scan_runner`, `scanplotter_cli`, and `actuator_runner`
- implementation packages such as `scan_concrete.*`, `actuator_concrete.*`, and `monitor_concrete.*`
- raw registry dictionaries such as `SCAN_REGISTRY`,`PLUGIN_REGISTRY`, `MONITOR_TYPES`
- any name starting with `_`

## Library integration example

This is an example for embedding `kiwi-scan` in another Python process.

```python
import threading
import time
import kiwi_scan
import logging

from kiwi_scan.datamodels import ActuatorConfig, ScanConfig, ScanDimension
from kiwi_scan.scan.tools import create_scan_with_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
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
