# Trigger Manager

The trigger manager writes a sequence of EPICS PVs at defined scan phases.
It is intended for simple coordination tasks such as arming acquisition, starting a detector, poke a trigger PV, or prepare/cleanup a scan.

Triggers are configured in YAML under the top-level `triggers:` section.

## Purpose

Use triggers when a scan needs to write one or more PVs at well-defined points in the scan lifecycle.

Typical uses:

- write an arm/start PV before motion or acquisition
- trigger PVs at each scan point (before/after)
- write a cleanup/reset PV after a scan
- react to monitor-driven events via the `monitor` trigger phase

The trigger manager is intentionally lightwieight. It writes configured values to configured PVs and optionally sleeps for a given delay after each write.

## YAML configuration

Example:

```yaml
triggers:
  before:
    - pv: ${IOC_MONO}:DAQ:ARM
      value: 1

  on_point:
    - pv: ${IOC_MONO}:DAQ:TRIG
      value: 1
      delay: 0.01

  after:
    - pv: ${IOC_MONO}:DAQ:ARM
      value: 0
```

Each trigger phase contains a list of actions. Actions are executed in the order they appear in the YAML file.

## Supported phases

The default phases are:

| Phase | Meaning |
|---|---|
| `before` | Executed once before the main scan acquisition starts. |
| `on_point` | Executed for each scan point before detector/plugin values are read. |
| `after_point` | Executed by scan engines that support an extra post-point or continuous-mode timing hook after detector read. |
| `after` | Executed once after the scan finishes or during scan cleanup. |
| `monitor` | Executed when a subscription with trigger behavior feeds the trigger worker. |

Custom phase names are also accepted when they are present in the YAML trigger block. They can be fired explicitly from custom scan engines, plugins or from `scantrigger_cli --phase PHASE`.

## TriggerAction fields

Each trigger action has these fields:

| Field | Required | Meaning |
|---|---:|---|
| `pv` | yes | EPICS PV to write. |
| `value` | yes | Value written to the PV. Scalars and simple list-like values are supported. |
| `delay` | no | Delay in seconds after the write. Defaults to `0.0`. |

Example with multiple actions:

```yaml
triggers:
  before:
    - pv: TEST:DAQ:ARM
      value: 1
    - pv: TEST:DAQ:MODE
      value: scan
      delay: 0.1
```

List values can be written either as YAML lists or as simple string lists:

```yaml
triggers:
  before:
    - pv: TEST:WAVEFORM
      value: [1.0, 2.0, 3.0]

  after:
    - pv: TEST:WAVEFORM
      value: "[0 0 0]"
```

## Runtime behavior

At scan construction time, the trigger manager prepares the configured actions and creates the corresponding EPICS PV objects.

At runtime, `fire(phase)` does the following:

1. Look up the PVs configured for the phase.
2. Write each configured value to its PV.
3. Log an error if the write fails.
4. Sleep for `delay` seconds when `delay > 0`.
5. Continue with the next PV.

In normal step scans, `before`, `on_point`, and `after` are the most important phases. Continuous scans may also use `after_point`.

## Error handling

The trigger manager is designed to avoid unnecessary scan crashes:

- missing `pv` fields are ignored and logged as warnings
- PV initialization failures are logged and the broken action is skipped
- unknown trigger phases are logged as warnings and ignored
- failed PV writes are logged as errors

A failed trigger write is not raised as an exception by the trigger manager. Check the log output when debugging trigger behavior, e.g. by using the CLI tool:

## CLI tool

`scan_trigger_cli` executes one configured trigger phase without running a scan.

This is useful for testing trigger YAML, manually arming/resetting hardware, or debugging replacement values.

Examples:

```bash
scan_trigger_cli \
  --config-file ./mono.yaml \
  --phase before
```

```bash
scan_trigger_cli \
  --config mono \
  --replace IOC_MONO=ue521sgm1:mono \
  --phase on_point
```

Important options:

| Option | Meaning |
|---|---|
| `--config NAME` | Load a preset config from `KIWI_SCAN_CONFIG_DIR`. |
| `--config-file PATH` | Load an explicit YAML file. |
| `--phase PHASE` | Trigger phase to execute. |
| `--replace KEY=VALUE` | Replace `${KEY}` placeholders in the YAML file. |
| `--log-level 0-5` | Set the kiwi-scan logging level helper. |

## Examples

### Minimal detector trigger

```yaml
triggers:
  before:
    - pv: TEST:DET:ARM
      value: 1

  on_point:
    - pv: TEST:DET:TRIGGER
      value: 1
      delay: 0.01

  after:
    - pv: TEST:DET:ARM
      value: 0
```

### Continuous-mode timing hook

```yaml
triggers:
  before:
    - pv: TEST:DAQ:ENABLE
      value: 1

  after_point:
    - pv: TEST:DAQ:SAMPLE
      value: 1

  after:
    - pv: TEST:DAQ:ENABLE
      value: 0
```

### Monitor-triggered action

```yaml
subscriptions:
  - name: external_trigger
    role: trigger
    pv: TEST:EXT:EVENT

triggers:
  monitor:
    - pv: TEST:DAQ:TRIGGER
      value: 1
```

A subscription with role `trigger` feeds monitor events into the scan trigger worker. The worker then fires the `monitor` trigger phase outside the EPICS callback context.
