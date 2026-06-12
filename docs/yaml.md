# YAML Configuration Reference

YAML is used to configure actuators, detector PVs, monitors, triggers, subscriptions, plugins, metadata, and scan-related defaults. The actual scan engine and scan dimensions may also be supplied from the command line or API, depending on how the scan is started.

`kiwi-scan` is intentionally tolerant during YAML parsing: unknown fields in dataclass-based YAML blocks are generally ignored. This helps with forward compatibility when configuration files are shared across different versions.

A scan still needs at least one `scan_dimensions` entry when a scan object is created.

## Config data classes

- **ScanConfig** — Top-level scan configuration.
- **ActuatorConfig** — Configuration for one actuator or motor interface.
- **JogConfig** — Optional jog-control block attached to an actuator.
- **ScanDimension** — One scan axis with start, stop, steps, and optional velocity.
- **ScanTriggers** — Trigger groups executed in scan phases.
- **TriggerAction** — One PV write action used by a trigger.
- **SubscriptionConfig** — One event subscription bound to a role.
- **PluginConfig** — One plugin declaration with type, name, and parameters.

## Top-level structure

`ScanConfig` is the root YAML object.

```yaml
actuators: {}
detector_pvs: []
detector_pvs_monitor: True
scan_dimensions: []
parallel_scans: []
nested_scans: []
plugin_configs: []
monitor_type: null
monitor: {}
stop_pv: null
data_dir: .
output_file: scan_results.txt
include_timestamps: False
integration_time: 0.0
sample_rate_hz: 1.0
debug: False
performance_report: False
data_writing_enabled: True
manifest_mode: full
triggers: {}
metadata_pvs: []
metadata_constants: {}
metadata_file: scan_metadata.txt
subscriptions: []
```
### Monitor parameters

Use `monitor_type: print` to stream detector values to stdout. The optional `monitor` block contains monitor-specific parameters. The monitor type stays in the top-level `monitor_type` field.

```yaml
monitor_type: print
monitor:
  format: tsv              # tsv | csv | json, default: tsv
  include_header: true     # header row for tsv/csv, default: true
  include_timestamps: true # add TS-ISO8601-* columns, default: false
  float_format: ".12e"    # Python float format, default: .12e
```

For `tsv` and `csv`, one header row is written followed by one row per scan point. For `json`, one JSON object is written per scan point. Diagnostic messages use normal logging, so stdout remains a machine-readable data stream.

`monitor_type: plot` needs no extra monitor parameters.

### ActuatorConfig

| Field | Type | Meaning |
|---|---|---|
| `pv` | string | Main write PV for absolute motion. |
| `type` | string | Actuator backend type, such as `epics` or `sim`. |
| `rel_pv` | string | Relative move PV. |
| `rb_pv` | string | Readback PV. |
| `cmd_pv` | string | Commanded-position PV. |
| `cmdvel_pv` | string | Commanded-velocity PV. |
| `stop_pv` | string | Stop PV. |
| `stop_command` | float | `stop_pv` value. |
| `status_pv` | string | Status PV used for ready/moving checks. |
| `ready_value` | int or string | Status value considered ready. |
| `ready_bitmask` | int | Bitmask for status-based ready logic. |
| `queueing_delay` | float | Delay after EPICS writes. |
| `ca_timeout` | float | EPICS  CA timeout. |
| `auto_monitor` | bool | Use EPICS CA cache. |
| `startup_timeout` | float | Timeout waiting for motion to start. |
| `in_position_band` | float | Allowed tolerance. |
| `dwell_time` | float | Delay after motion completes. |
| `backlash` | float | Optional backlash compensation distance. |
| `start_pv` | string | PV used to start motion explicitly. |
| `start_command` | float | Value written to `start_pv`. |
| `velocity_pv` | string | PV used to set motion velocity. |
| `get_velocity_pv` | string | PV used to read current velocity. |
| `jog` | `JogConfig` | Jog-control configuration. |

#### JogConfig
| Field | Type | Meaning |
|---|---|---|
| `velocity_pv` | string | PV that receives jog velocity. |
| `abs_velocity` | bool | Writes absolute velocity magnitude when true. |
| `command_pv` | string | PV that starts jog motion. |
| `command_pos` | float | Command value for positive jog. |
| `command_neg` | float | Command value for negative jog. |

### ScanTriggers
Built-in phases are:

- `before`
- `on_point`
- `after_point`
- `after`
- `monitor`

Each phase contains a list of `TriggerAction` entries:

Example:

```yaml
triggers:
  before:
    - pv: TEST:ARM
      value: 1
  on_point:
    - pv: TEST:TRIG
      value: 1
      delay: 0.01
  after:
    - pv: TEST:ARM
      value: 0
```

#### TriggerAction
One PV write action used by a trigger.

| Field | Type | Meaning |
|---|---|---|
| `pv` | string | Target PV to write. |
| `value` | any | Value written to the PV. |
| `delay` | float | Optional sleep after the write. |

### SubscriptionConfig
One event subscription bound to a role.
One of `pv` or `actuator` field should be set.
When `actuator` is used, `source` selects which actuator PV is subscribed.

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Unique subscription name. |
| `role` | string | Logical dispatch role, for example `sync` or `heartbeat`. |
| `pv` | string | Direct PV subscription target. |
| `actuator` | string | Actuator name used for indirect PV lookup. |
| `source` | string | Source selector like `rbv`, `status`, `stop`, or `velocity`. |

Examples:

```yaml
subscriptions:
  - name: sync_energy
    role: sync
    actuator: energy
    source: rbv

  - name: monitor
    role: monitor
    pv: TEST:SOME:PV:NAME
```

### PluginConfig
Plugin declaration with type, name, and parameters.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | Registered plugin type name. |
| `name` | string | Instance name used in logs and runtime. |
| `parameters` | mapping | Plugin-specific untyped configuration block. |

### ScanDimension
One scan axis with start, stop, steps, and optional velocity.
Arrays are used for multiple actuators

| Field | Type | Meaning |
|---|---|---|
| `actuator` | string | Name of the actuator used for this dimension. |
| `start` | float | Scan start position. |
| `stop` | float | Scan stop position. |
| `steps` | int | Number of scan points. |
| `velocity` | float | Optional velocity for continuous-style scans. |

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

