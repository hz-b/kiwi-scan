# Scan IOC

The generic scan IOC exposes the kiwi-scan Scan API as a pythonSoftIOC. It is as a small EPICS support layer around the existing scan classes. The IOC creates PVs for starting and stopping a scan, exposes one configurable scan dimension, and publishes selected scan data values as EPICS records.

The design is split into two layers:

- `ScanIOCController`: pure Python scan controller. It loads the scan configuration, creates scan objects, starts/stops scans, and reads scan state.
- `GenericScanIOC`: pythonSoftIOC layer. It creates EPICS records, forwards PV updates to the controller, and periodically publishes scan state and selected scan data values.

The command-line tool to start the IOC is `scanioc`.

## Basic command-line usage

```bash
scanioc \
  --prefix KIWI:SCAN \
  --scan-type linear \
  --config-file mono.yaml \
  --data-pv Position=Position:float \
  --data-pv beta=IOC_NAME:Value \
  --log-level 2
```

Using a named config from `KIWI_SCAN_CONFIG_DIR`:

```bash
scanioc \
  --prefix KIWI:SCAN \
  --scan-type linear \
  --config mono \
  --data-pv Position=Position:float \
  --data-pv beta=IOC_NAME:Value
```

## Command-line options

### Configuration

Use exactly one of:

```bash
--config NAME
--config-file PATH
```

`--config NAME` loads `NAME.yaml` from `KIWI_SCAN_CONFIG_DIR` or the default kiwi-scan config directory. `--config-file PATH` loads a specific YAML file directly.
`--config-file PATH` loads a specific YAML file directly.

Optional configuration arguments:

```bash
--config-dir PATH
--data-dir PATH
--replace KEY=VALUE
```

`--replace` can be repeated. Environment replacements using the `KIWI_SCAN_REPLACE_...` convention are also picked up.

### Scan type

```bash
--scan-type linear
```

The scan type must be one of the registered kiwi-scan scan types, for example `linear`, `cm`, `poll`, `para`, or other scan types available in the installation.

### Data PVs

Data PVs are configured with repeatable `--data-pv` arguments:

```bash
--data-pv LOCAL=KEY[:TYPE]
```

`LOCAL` is the local IOC data record name. If it does not contain a colon, the IOC creates it below `DATA:`.

`KEY` is the key passed to the scan object via:

```python
scan.get_value(KEY, default=...)
```

`TYPE` is optional and can be one of:

- `float`
- `int`
- `str`
- `bool`

If no type is given, `float` is used.

Examples:

```bash
--data-pv Position=Position:float
--data-pv beta=TESTU171PGM1:Beta
--data-pv Output=SomePluginValue:float
--data-pv Comment=SomeStringValue:str
```

EPICS PV names and scan data keys may contain colons. Therefore only the final `:TYPE` suffix is interpreted as a type when it matches a known type. For example:

```bash
--data-pv beta=TESTU171PGM1:Beta
--log-level 0-5
```

uses `TESTU171PGM1:Beta` as the scan data key and defaults to type `float`.

## Exposed IOC records

The IOC creates the following control and status records:

| PV | Direction | Purpose |
| --- | --- | --- |
| `KIWI:SCAN:Start` | write | Write `1` to start a scan. The record is reset to `0` by the IOC. |
| `KIWI:SCAN:Stop` | write | Write `1` to request scan stop. The record is reset to `0` by the IOC. |
| `KIWI:SCAN:kill` | write | Write a nonzero value to terminate the IOC process immediately. |
| `KIWI:SCAN:Status` | read | Scan state: `idle`, `running`, `initializing`, or `error`. |
| `KIWI:SCAN:Busy` | read | Boolean busy state of the active scan. |
| `KIWI:SCAN:Message` | read | Short status or error message. |
| `KIWI:SCAN:OutputFile` | read | Basename of the active output file limited to 39 characters. |
| `KIWI:SCAN:DataWritingEnabled` | read/write | Enable or disable data and metadata writing for future and active scans. |
| `KIWI:SCAN:LogLevel` | read/write | Runtime logging level using the `0..5` scale. |
| `KIWI:SCAN:Position` | read | Current scan position as reported by the active scan. |
| `KIWI:SCAN:ScanType` | read/write | Registered scan type for subsequent scans. |
| `KIWI:SCAN:Config` | read/write | Named configuration for subsequent scans. Empty in `--config-file` mode. |

`Config` and `ScanType` changes are accepted only while no scan is active. Runtime config switching is unavailable when the IOC was started with `--config-file`.
The IOC also creates one configurable scan dimension:

| PV | Direction | Purpose |
| --- | --- | --- |
| `KIWI:SCAN:Actuator` | read/write | Actuator name used for the scan dimension. |
| `KIWI:SCAN:StartPos` | read/write | Start position. |
| `KIWI:SCAN:StopPos` | read/write | Stop position. |
| `KIWI:SCAN:Steps` | read/write | Number of scan points. |
| `KIWI:SCAN:Velocity` | read/write | Optional actuator velocity for scan types that use it. |

Configured data PVs are created below `DATA:` by default:

```text
KIWI:SCAN:DATA:Position
KIWI:SCAN:DATA:beta
```

## Starting a scan from EPICS

1. Set the scan dimension records:

```bash
caput KIWI:SCAN:Actuator energy
caput KIWI:SCAN:StartPos 300
caput KIWI:SCAN:StopPos 404
caput KIWI:SCAN:Steps 105
caput KIWI:SCAN:Velocity 0
```

2. Start the scan:

```bash
caput KIWI:SCAN:Start 1
```

3. Stop the scan if required:

```bash
caput KIWI:SCAN:Stop 1
```

## Python API

The public IOC API is in `kiwi_scan.ioc`.

Typical construction from Python:

```python
from kiwi_scan.ioc import GenericScanIOCOptions, create_ioc

options = GenericScanIOCOptions(
    prefix="KIWI:SCAN",
    scan_type="linear",
    config_file="mono.yaml",
    data_pvs=[],
)

ioc = create_ioc(options)
ioc.runIOC()
```

For tests or non-EPICS logic, use the pure controller directly:

```python
from kiwi_scan.ioc import ScanIOCController

controller = ScanIOCController(
    scan_type="linear",
    config_file="mono.yaml",
)
```

The controller is independent of pythonSoftIOC. Important methods include:

- `dimension_defaults()`
- `make_dimension(...)`
- `reload_config()`
- `get_config_name()` / `set_config_name(name)`
- `get_scan_type()` / `set_scan_type(name)`
- `start(dimensions)`
- `execute_current_scan()`
- `stop()`
- `get_busy()`
- `get_position()`
- `get_value(key)`
- `get_output_file()`
- `set_data_writing_enabled(enabled)`
- `get_data_writing_enabled()`

## Monitors

The IOC uses the normal monitor configuration from the selected scan YAML file. This means the same monitor configuration used by `scan_runner` is also active when the scan is started through the IOC.

The print monitor is usually safe and useful for command-line operation. It writes scan values to stdout in the configured format.

The plot monitor can work when the IOC is started directly from an interactive command line with a working graphical desktop session. However, using the plot monitor from the IOC is not recommended for production operation. The plot monitor creates a Tk/matplotlib GUI and needs a graphical environment. On headless systems, services, remote IOC hosts, or unattended beamline operation, this makes little sense and can fail because no display is available.
Plotting can be disabled in the scan YAML.
