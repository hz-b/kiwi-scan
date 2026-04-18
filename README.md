# kiwi-scan

`kiwi-scan`: A Modular Scan Framework for Commissioning and Diagnostics in EPICS Environments

Actuators, detector PVs, triggers, subscriptions, plugins, and metadata sidecars are configured via YAML. Choose a scan engine and execute scans from the command line or API. Results are written to timestamped text files, with metadata logged in parallel for post-mortem analysis.

## Overview of Features

- **YAML-first configuration** for actuators, detectors, scan dimensions, triggers, metadata PVs/constants, subscriptions, plots and plugin configuration.
- **Pluggable scan engines** such as `linear`, `approach`, `poll`, and `cm`, plus externally registered scan types.
- **Pluggable runtime extensions** via plugins that can add computed columns or react to monitor events.
- **EPICS integration** via pyepics wrapper for real control or a **simulated actuator backend** for tests and development.
- **Structured outputs** including the main scan file, optional metadata sidecar logging and waveform support, and post-mortem plotting tools.
- **Event handling** Subscriptions route monitored events into defined roles.
- **Trigger** Triggers allow actions before, or after scan points or on monitor events.

## Public API

`kiwi-scan` can be embedded directly as a Python library, for example inside a Python IOC or another beamline control application.

The command-line tools are wrappers around the library API: they build a `ScanConfig`, load scan/plugin implementations, and then create or execute a scan object. For library users, the supported public API is described below.

### Supported public API

#### 1. Startup helpers

Use these when your application may rely on external plugins or externally registered scan types.

```python
import kiwi_scan

kiwi_scan.load_all_plugins()
kiwi_scan.load_all_scan_types()
```

#### 2. Configuration and YAML loading

These are the building blocks for constructing scan configurations in Python or from YAML:

- `kiwi_scan.datamodels.ActuatorConfig`
- `kiwi_scan.datamodels.ScanDimension`
- `kiwi_scan.datamodels.ScanConfig`
- `kiwi_scan.datamodels.TriggerAction`
- `kiwi_scan.datamodels.ScanTriggers`
- `kiwi_scan.datamodels.SubscriptionConfig`
- `kiwi_scan.yaml_loader.yaml_loader`
- `kiwi_scan.yaml_loader.parse_replacements`
- `kiwi_scan.yaml_loader.get_env_replacements`

#### 3. Runtime scan API

These functions are the recommended library entry points:

- `kiwi_scan.scan.tools.create_scan_with_config()`
  - create a scan object without starting it
- `kiwi_scan.scan.tools.scan_with_config()`
  - create and execute a scan synchronously

The returned scan object should be treated through its documented non-private interface:

- `scan.execute()`
- `scan.load_data()`
- `scan.get_output_file()`
- `scan.get_value(name, with_metadata=False)`
- `scan.get_actuator(name)`
- `scan.get_actuators()`
- `scan.busy`
- `scan.position`
- `scan.stop()` 

#### 4. Extension API

These are the supported extension points for projects that need custom behavior:

- `kiwi_scan.scan.registry.register_scan`
- `kiwi_scan.scan.registry.load_all_scan_types`
- `kiwi_scan.plugin.registry.register_plugin`
- `kiwi_scan.actuator.factory.create_actuator`
- `kiwi_scan.actuator.single.AbstractActuator`
- `kiwi_scan.scan.scan_abs.ScanABC` or 
- `kiwi_scan.scan.common.BaseScan` for custom scan implementations

For subclasses of `BaseScan`, only the documented constructor and non-private methods should be treated as public. Attributes and methods starting with `_` are internal implementation details.

#### 5. Analysis API

These helpers are useful when another application wants to inspect scan results programmatically:

- `kiwi_scan.dataloader.DataLoader`
- `kiwi_scan.metadata_loader.parse_metadata_file()`
- `kiwi_scan.postmortem.plot_postmortem()`

### What is not public API

- CLI entry-point modules such as `scan_runner`, `scanplotter_cli`, and `actuator_runner`
- implementation packages such as `scan_concrete.*`, `actuator_concrete.*`, and `monitor_concrete.*`
- raw registry dictionaries such as `SCAN_REGISTRY` and `PLUGIN_REGISTRY`
- factory lookup tables such as `MONITOR_TYPES`
- any name starting with `_`

### Library integration example

This is an example for embedding `kiwi-scan` in another Python process.

```python
import threading
import kiwi_scan

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

## Installation

Basic installation:

```bash
pip install kiwi-scan
```

Editable/development installation:

```bash
pip install -e ".[dev]"
```

For a detector-free test, use a simulated actuator (`type: sim`) and keep `detector_pvs: []`.

## Development setup

For repository development, a top-level `Makefile` and `mkvenv.sh` helper script are provided.

### Bootstrap or activate the development environment

```bash
source ./mkvenv.sh
```

`mkvenv.sh` must be **sourced**, not executed. It:

- creates `.venv` if it does not exist
- activates `.venv`
- upgrades `pip` and installs build helpers on first setup
- installs `kiwi-scan` in editable mode with development extras
- prefixes the shell prompt with `KIWI` so the active development shell is obvious

If you want the environment to remain active in your current shell, always use `source ./mkvenv.sh` directly. `make` runs recipes in subprocesses and cannot keep your interactive shell activated.

### Makefile helpers

Use the self-documenting help target to see the available development commands:

```bash
make help
```

The development targets are:

- `make help` - show help
- `make lint` - run `pylint` on `src/kiwi_scan`
- `make test` - run Python unit tests
- `make install_completion` - install bash completion snippets from `bash-completion/`
- `make uninstall_completion` - remove installed bash completion snippets
- `make cscope` - build `cscope` and `ctags` indexes used by vim. This rule can directly used by vi to update indexes
- `make tag` - create a timestamp-based tag from `HEAD`
- `make clean` - remove `.venv`, caches, tags, and generated metadata such as `*.egg-info`

## Quick start

The example below runs a tiny detector-free scan with a simulated actuator and writes the output into the current directory.

Create `sim_minimal.yaml`:

```yaml
actuators:
  theta:
    type: sim
    pv: THETA
    rb_pv: THETA:RBV
    velocity: 1.0
    dwell_time: 0.0

detector_pvs: []
data_dir: .
output_file: sim_scan.txt
include_timestamps: true
```

Run a 5-point linear scan:

```bash
export KIWI_SCAN_DATA_DIR="$PWD"

scan_runner \
  --scan_type linear \
  --config-file ./sim_minimal.yaml \
  --dim actuator=theta,start=0,stop=1,steps=5
```

- `scan_runner` loads the YAML file.
- The `--dim` arguments define the actual scan range for this run.
- A timestamped file such as `sim_scan-20260401123045.txt` is created. If the file exists, a unique id is created.
- Even without detectors, the file still contains the scan position and scan timestamp columns.

## YAML configuration example

A more realistic EPICS-oriented configuration might look like this:

```yaml
actuators:
  energy:
    type: epics
    pv: ${IOC_MONO}:SetEnergy
    rb_pv: ${IOC_MONO}:GetEnergy
    status_pv: ${IOC_MONO}:State
    stop_pv: ${IOC_MONO}:Stop
    stop_command: 1
    in_position_band: 0.01
    dwell_time: 0.05

detector_pvs:
  - ${DET_PV1}
  - ${DET_PV2}

monitor_type: print
stop_pv: ${IOC_MONO}:SCAN_STOP
output_file: energy_scan.txt
data_dir: scans
include_timestamps: true
integration_time: 1.0

triggers:
  before:
    - pv: ${IOC_MONO}:DAQ:START
      value: 1
  on_point:
    - pv: ${IOC_MONO}:DAQ:PROC
      value: 1
      delay: 0.01
  after:
    - pv: ${IOC_MONO}:DAQ:STOP
      value: 1

metadata_constants:
  beamline: ue521sgm1
  operator: commissioning
metadata_pvs:
  - ${IOC_MONO}:State
  - ${IOC_MONO}:Temperature
  - ${IOC_MONO}:RingCurrent
  - ${IOC_MONO}:cff
metadata_file: energy_scan_meta.txt

subscriptions:
  - name: energy_sync
    role: sync
    actuator: energy
    source: rbv

  - name: keithley1
    role: sync
    pv: ${IOC_MONO}:DAQ:KEITHLEY1

  - name: energy_status
    role: status
    actuator: energy
    source: status

  - name: daq_heartbeat
    role: heartbeat
    pv: ${IOC_MONO}:DAQ:HEARTBEAT

  - name: immediate_stop
    role: stop
    pv: ${IOC_MONO}:SCAN_STOP

  - name: drift_feed
    role: plugin
    pv: ${IOC_MONO}:DRIFT

plugin_configs:
  - type: DriftWatchPlugin
    name: drift_watch
    parameters:
      limit: 0.03
```

Load placeholder values from the command line:

```bash
scan_runner \
  --scan_type linear \
  --config-file ./beamline.yaml \
  --replace \
    IOC_MONO=ue521sgm1:mono \
    DET_PV1=ue521sgm1:detA \
    DET_PV2=ue521sgm1:detB \
  --dim actuator=energy,start=400,stop=410,steps=11,velocity=0.5
```

You can also inject replacements from the environment with variables of the form:

```bash
export KIWI_SCAN_REPLACE_IOC_MONO=ue521sgm1:mono
export KIWI_SCAN_REPLACE_DET_PV1=ue521sgm1:detA
export KIWI_SCAN_REPLACE_DET_PV2=ue521sgm1:detB
```

## Plugin example

Plugins are instantiated from `plugin_configs` and discovered from the built-in plugin package plus any files or directories listed in `KIWI_SCAN_PLUGIN_PATH`.

Create `plugins/drift_watch.py`:

```python
import time
from typing import Dict, Any, List
from kiwi_scan.plugin.registry import register_plugin
from kiwi_scan.plugin.base import ScanPlugin

@register_plugin("DriftWatchPlugin")
class DriftWatchPlugin(ScanPlugin):
    """
    Minimal plugin example:
    - receives (name, parameters, scan) from the plugin factory
    - listens to subscription events with role="plugin"
    - writes two extra columns on every scan point
    """

    def __init__(self, name, parameters=None, scan=None):
        super().__init__(name, parameters or {}, scan)
        self.limit = float(self.parameters.get("limit", 0.03))
        self.latest_drift = None

    def get_headers(self, timestamps: bool):
        headers = ["LatestDrift", "DriftAlarm"]
        return self.expand_headers(headers, timestamps)

    def get_values(self, idx: int, pos: Dict[str, Any]) -> List[Any]:
        if self.latest_drift is None:
            drift = float("nan")
            alarm = 0
        else:
            drift = self.latest_drift
            alarm = int(abs(drift) > self.limit)
        return [ drift, alarm ]
    
    def on_monitor(self, ev):
        self.logger.debug(f"{ev}")
        try:
            self.actuator = self.scan.get_actuator("energy");
            rbv = self.actuator.rbv
            self.latest_drift = float(ev.value)
            alarm = int(abs(self.latest_drift) > self.limit)
            if alarm and self.actuator.is_ready(): 
                # drift while actuator ready
                self.logger.warning(f"drift={self.latest_drift}, @rbv={rbv}")
        except Exception:
            self.latest_drift = None
```

Enable it:

```bash
export KIWI_SCAN_PLUGIN_PATH="$PWD/plugins"
```

Then run a scan with a subscription that feeds plugin events:

```yaml
subscriptions:
  - name: drift_feed
    role: plugin
    pv: ${IOC_MONO}:DRIFT

plugin_configs:
  - type: DriftWatchPlugin
    name: drift_watch
    parameters:
      limit: 0.03
```

Use this example with `--scan_type linear`, the built-in `LinearScan` dispatches the `plugin` subscription role to `plugin.on_monitor(...)`.

## External scan-type example

External scan types are registered with `register_scan(...)` and discovered from files or directories listed in `KIWI_SCAN_SCAN_PATH`.

Create `scan_types/triangle_scan.py`:

```python
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.registry import register_scan


@register_scan("triangle")
class TriangleScan(BaseScan):
    """
    Forward scan, then back again without repeating the end point.
    Example: 0, 1, 2, 1, 0
    """

    def execute(self):
        positions = {}

        for dim in self.scan_dimensions:
            forward = dim.compute_positions_linear()
            backward = list(reversed(forward[:-1]))
            positions[dim.actuator] = forward + backward

        self.scan(positions)
```

Enable it:

```bash
export KIWI_SCAN_SCAN_PATH="$PWD/scan_types"
```

Run it:

```bash
scan_runner \
  --scan_type triangle \
  --config-file mono.yaml \
  --dim actuator=energy,start=400,stop=402,steps=3
```

This produces a trajectory like:

```text
400.0 -> 401.0 -> 402.0 -> 401.0 -> 400.0
```

That pattern is handy for hysteresis checks, warm-up sweeps, and repeatability measurements.

## Command-line tools

After installation, the main entry points are:

- `scan_runner` — execute scans from YAML + CLI dimensions
- `actuator_runner` — send one-off actuator commands and optional monitors

Examples:

```bash
scan_runner --help
actuator_runner --help
```

## Output files

A typical run can generate two kinds of files:

1. **Main scan file**
   - timestamped file name based on `output_file`
   - position column
   - per-line timestamp
   - detector values and optional detector timestamps
   - plugin-generated columns

2. **Metadata sidecar file**
   - constants from `metadata_constants`
   - initial PV snapshots
   - change-driven CA monitor events for the configured `metadata_pvs`

The post-mortem plotting tools can combine scan files and metadata files for later analysis.

## Environment variables

Useful environment variables:

- `KIWI_SCAN_DATA_DIR` — base directory for output files
- `KIWI_SCAN_REPLACE_*` — placeholder replacement values for YAML templates
- `KIWI_SCAN_CONFIG_DIR` — where preset YAML configs are searched
- `KIWI_SCAN_PLUGIN_PATH` — extra plugin files/directories to import
- `KIWI_SCAN_SCAN_PATH` — extra scan-type files/directories to import

## Project layout


```text
kiwi-scan/
├── pyproject.toml
├── README.md
├── LICENSE
├── src/
│   └── kiwi_scan/
├── config/
│   └── scan_config/
├── bash-completion/
├── tests/
```

## Development status

This project is under active development. Configuration details and extension APIs may still evolve.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
