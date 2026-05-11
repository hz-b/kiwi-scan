# Plugins

Plugins are extensions for kiwi-scan. The hook into the scan logic, react to monitor events routed through subscriptions and can add extra columns to the scan output

Plugins are configured in YAML under the top-level `plugin_configs:` section.

## Plugin concept

A plugin is a Python class derived from `ScanPlugin`.

Plugins can:

- add additional control logic to the scan engine
- add computed values to each scan point
- read additional PVs or actuator state
- log diagnostics during a scan
- react to subscription events through `on_monitor()`
- access the scan object, including actuators, through `self.scan`

## Plugin discovery

Built-in plugins are loaded by kiwi-scan. External plugins can be registered from directories listed in `KIWI_SCAN_PLUGIN_PATH`.

Example:

```bash
export KIWI_SCAN_PLUGIN_PATH="$PWD/plugins"
```

A plugin must register itself with `register_plugin()`:

```python
from kiwi_scan.plugin.registry import register_plugin
from kiwi_scan.plugin.base import ScanPlugin

@register_plugin("MyPlugin")
class MyPlugin(ScanPlugin):
    ...
```

The registered name is the value used in YAML as `type:`.

## `plugin_configs` YAML section

Example:

```yaml
plugin_configs:
  - type: LoggingPlugin
    name: scan_log
    parameters:
      enable_alarm_trace: True
      enable_point_timing: True
      enable_actuator_trace: True
      alarm_log:
        - TEST:DET:STATUS
        - TEST:MOTOR:STATUS

  - type: JogPIDPlugin
    name: jog_pid
    parameters:
      sample_time: 0.5
      kp: 0.001
      ki: 0.0
      kd: 0.0
      kvf: 0.0
      actuator:
        type: epics
        pv: TEST:JOG:SET
        rb_pv: TEST:JOG:RBV
        velocity_pv: TEST:JOG:VELO
        jog:
          velocity_pv: TEST:JOG:VELO
          command_pv: TEST:JOG:CMD
          command_pos: 1
          command_neg: -1
```

Fields:

| Field | Meaning |
|---|---|
| `type` | Registered plugin type name. |
| `name` | Instance name used for logging. |
| `parameters` | Plugin-specific configuration dictionary. |

## `ScanPlugin` lifecycle

### `__init__(name, parameters=None, scan=None)`

The plugin is created when the scan object is constructed.

The base class stores:

- `self.name`
- `self.parameters`
- `self.logger`
- `self.log_dir`
- `self.scan`

`self.scan` gives access to the running scan object. For example, a plugin can call `self.scan.get_actuator("energy")`.

### `get_headers(timestamps: bool)`

Return the output column names added by the plugin.

The number and order of headers must match the values returned by `get_values()`.

Example:

```python
def get_headers(self, timestamps: bool):
    return self.expand_headers(["LatestDrift", "DriftAlarm"], timestamps)
```

### `get_values(idx, pos)`

Return the plugin values for one scan point.

This method is called during scan acquisition. The returned values are wrapped into the same dictionary format used by detector reads and then appended to the scan row.

Example:

```python
def get_values(self, idx, pos):
    return [self.latest_drift, int(abs(self.latest_drift) > self.limit)]
```

### `on_monitor(ev)`

Optional hook called when a subscription with role `plugin` receives a PV event.

Example YAML:

```yaml
subscriptions:
  - name: drift_feed
    role: plugin
    pv: TEST:DRIFT
```

The event is passed as a `PvEvent` object with fields such as `pvname`, `value`, `timestamp`, `severity`, and `status`.

## Timestamp column handling

When the scan config has `include_timestamps: true`, plugin timestamp columns can be added next to plugin value columns.

Use `self.expand_headers(headers, timestamps)` in `get_headers()`:

```python
def get_headers(self, timestamps: bool):
    return self.expand_headers(["ValueA", "ValueB"], timestamps)
```

This expands:

```text
ValueA ValueB
```

into:

```text
ValueA TS-ValueA ValueB TS-ValueB
```

Plugin values returned by `get_values()` are wrapped by the base class with the current time. The scan writer uses these wrapped timestamps when timestamp output is enabled.

## Built-in plugins

### `LoggingPlugin`

`LoggingPlugin` adds diagnostic columns to the scan output.

Features:

- optional alarm trace over configured PVs
- optional point-to-point timing measurement
- plugin-specific log file support

Example:

```yaml
plugin_configs:
  - type: LoggingPlugin
    name: scan_log
    parameters:
      log_file: logging_plugin.log
      enable_actuator_trace: True
      enable_point_timing: True
      enable_alarm_trace: True
      alarm_log:
        - TEST:DET:STATUS
        - TEST:MOTOR:STATUS
```

Parameters:

| Parameter | Default | Meaning |
|---|---:|---|
| `log_file` | `logging_plugin.log` | Plugin log filename below the plugin log directory. |
| `log_level` | scan/default logging level | Python logging level. |
| `enable_alarm_trace` | `true` | Add alarm-state columns. |
| `enable_actuator_trace` | `true` | Add actuator columns such as staus cycle count and age.  |
| `enable_point_timing` | `true` | Add point timing column. |
| `alarm_log` | `[]` | List of PV names checked for alarm state. |

Output columns depend on enabled features:

| Feature | Columns |
|---|---|
| alarm trace | `AlarmState`, `AlarmPV`, `AlarmSeverity`, `AlarmStatus` |
| point timing | `PointDtS` |

### `JogPIDPlugin`

`JogPIDPlugin` is a simple closed-loop controller example.
It reads actuator position and velocity, computes a PID plus velocity-feed-forward setpoint, and calls `jog()` on the configured actuator.

The control calculation runs from `get_values()`, so it is executed at scan points.

Example:

```yaml
plugin_configs:
  - type: JogPIDPlugin
    name: jog_pid
    parameters:
      log_file: jogpid_plugin.txt
      sample_time: 0.5
      kp: 0.001
      ki: 0.0
      kd: 0.0
      kvf: 0.0
      actuator:
        type: epics
        pv: TEST:JOG:SET
        rb_pv: TEST:JOG:RBV
        get_velocity_pv: TEST:JOG:VELO_RBV
        jog:
          velocity_pv: TEST:JOG:VELO
          command_pv: TEST:JOG:CMD
          command_pos: 1
          command_neg: -1
```

Parameters:

| Parameter | Default | Meaning |
|---|---:|---|
| `actuator` | required | Actuator configuration used by the controller. |
| `kp` | `0.001` | Proportional gain or PV name. |
| `ki` | `0.0` | Integral gain or PV name. |
| `kd` | `0.0` | Derivative gain or PV name. |
| `kvf` | `0.0` | Velocity feed-forward gain or PV name. |
| `sample_time` | `1.0` | Minimum time between jog setpoint writes. |
| `log_file` | `jogpid_plugin.txt` | Plugin log filename. |
| `log_level` | scan/default logging level | Python logging level. |

Output column:

```text
ControllerSetpoint
```

When timestamps are enabled, a timestamp column is also added.

## Minimal custom plugin example

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
