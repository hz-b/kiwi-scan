# Stats and data column providers

The kiwi-scan statistics support is used by step scans, e.g. `linear` and `para`. `pollstats_cli` is a command line tool for live statistics from configured subscriptions.

## Purpose

The stats feature summarizes continuously monitored numeric values. Typical sources are actuator readbacks, energy feedback signals, encoders, or other EPICS PVs configured as scan subscriptions.

For every statistics source, kiwi-scan produces one group of columns:

```text
<name>Mean    <name>Std    <name>Min    <name>Max    <name>NSamples
```

Example for an `energy` source:

```text
energyMean    energyStd    energyMin    energyMax    energyNSamples
```

In scans, the statistics are written as additional scan-file columns and `pollstats_cli` prints the statistics on the command line live, without running a scan.

## Main components

### `kiwi_scan.stats`

`kiwi_scan.stats` contains the lightweight online statistics building blocks used by the scan engine:

- `Mean`
- `Var`

Both classes provide the small interface needed by kiwi-scan:

```python
update(x, w=1.0)
get()
revert(x, w=1.0)
update_many(values)
```

This avoids a runtime dependency on `river.stats` while still allowing online mean and variance calculation.

### `StatsCollector`

`StatsCollector` lives in:

```text
kiwi_scan/scan/stats_collector.py
```

It collects per-subscription running statistics and exposes them as scan-output columns. By default it collects subscriptions with role `stat`.

The default fields are:

| Field | Column suffix | Meaning |
|---|---|---|
| `mean` | `Mean` | Mean value in the current window |
| `std` | `Std` | Standard deviation in the current window |
| `min` | `Min` | Minimum value in the current window |
| `max` | `Max` | Maximum value in the current window |
| `nsamples` | `NSamples` | Number of numeric samples in the current window |

It keeps one statistics window per configured subscription.

Column prefixes are chosen from the subscription configuration:

- If the subscription refers to a unique actuator, the actuator name is used, for example `energyMean`.
- If several subscriptions refer to the same actuator, or if the subscription is a direct PV subscription, the subscription `name` is used, for example `betaMean`.
- If a generated prefix already exists, a numeric suffix is added internally to keep the columns unique.

### `DataColumnProvider`

`DataColumnProvider` lives in:

```text
kiwi_scan/scan/column_provider.py
```

It is a protocol for objects that add dynamic columns to scan output rows:

```python
get_headers(include_timestamps=False) -> list[str]
get_values() -> list[Any]
update_last_point(last_point, include_timestamps=False) -> None
reset_window() -> None
```

`StatsCollector` implements this protocol. This design makes it possible to easily add other dynamic column providers.

## Use in scan output

`BaseScan` owns a list of data column providers. A scan type registers a provider with:

```python
self.add_column_provider(provider)
```

During header writing, `BaseScan` asks each provider for headers and inserts them after `Position` and before the row timestamp:

```text
Position    <provider columns...>    TS-ISO8601    <detector columns...>    <plugin columns...>
```

During row writing, `BaseScan` asks each provider for values and writes them in the same order.

Before each scan point, the scan engine should reset provider windows:

```python
self._reset_data_column_provider_windows()
```

## YAML configuration

Statistics are driven by `subscriptions`.

### One actuator readback as sync source

```yaml
subscriptions:
  - name: mono_energy
    role: stat
    actuator: energy
    source: rbv
```

This usually produces:

```text
energyMean    energyStd    energyMin    energyMax    energyNSamples
```

### One direct PV as sync source

```yaml
subscriptions:
  - name: beta
    role: stat
    pv: "TESTU171PGM1:Beta"
```

This produces:

```text
betaMean    betaStd    betaMin    betaMax    betaNSamples
```

### Several sync subscriptions

```yaml
subscriptions:
  - name: beta
    role: stat
    pv: "TESTU171PGM1:Beta"

  - name: theta
    role: stat
    pv: "TESTU171PGM1:Theta"

  - name: mono_energy
    role: stat
    actuator: energy
    source: rbv
```

This produces one statistics group per subscription:

```text
betaMean    betaStd    betaMin    betaMax    betaNSamples    thetaMean    thetaStd    thetaMin    thetaMax    thetaNSamples    energyMean    energyStd    energyMin    energyMax    energyNSamples
```

## `pollstats_cli`

`pollstats_cli` is a small diagnostic command line tool for live statistics. It does not run a scan and does not write a scan data file. Instead, it loads a normal kiwi-scan YAML configuration, starts the configured subscriptions for one role, feeds all received events into a `StatsCollector`, and prints the current statistics periodically.

The default role is `sync`, so the tool normally watches the same subscription sources that are used for scan-point statistics.

### Basic usage

Use an explicit YAML file:

```bash
pollstats_cli --config-file mono.yaml
```

Use a preset configuration from `KIWI_SCAN_CONFIG_DIR`:

```bash
pollstats_cli --config mono
```

Use YAML replacements:

```bash
pollstats_cli --config-file mono.yaml --replace IOC_MONO=TESTU171PGM1
```

Run for a fixed time:

```bash
pollstats_cli --config-file mono.yaml --duration 10
```

Print every 0.5 seconds:

```bash
pollstats_cli --config-file mono.yaml --interval 0.5
```

Collect another subscription role:

```bash
pollstats_cli --config-file mono.yaml --role monitor
```

Do not print the leading timestamp column:

```bash
pollstats_cli --config-file mono.yaml --no-timestamp
```

Reset the statistics window after every printed row:

```bash
pollstats_cli --config-file mono.yaml --reset-each-print
```

Increase logging verbosity:

```bash
pollstats_cli --config-file mono.yaml --log-level 1
```

The `--log-level` value uses the kiwi-scan MBBO-style scale `0..5`, mapped to Python logging levels.

### Output modes

`pollstats_cli` always prints a header first.

For one or two subscriptions, it uses compact single-line output. The line is updated in place, so it behaves like a live terminal status line:

```text
TS-ISO8601    energyMean    energyStd    energyMin    energyMax    energyNSamples
2026-05-22T06:52:01.225977+00:00    6.93023e+08    2.58083e+09    -1.00004e+08    1e+10    43
```

For more than two subscriptions, it switches to a multiline layout to avoid unreadable wrapped terminal lines:

```text
TS-ISO8601    betaMean    betaStd    betaMin    betaMax    betaNSamples    thetaMean    thetaStd    thetaMin    thetaMax    thetaNSamples    energyMean    energyStd    energyMin    energyMax    energyNSamples

2026-05-22T06:54:37.859821+00:00
  betaMean             -1.16667e-05
  betaStd              1.98645e-05
  betaMin              -3.72222e-05
  betaMax              1.69444e-05
  betaNSamples         6
  thetaMean            2.35e-05
  thetaStd             1.92837e-05
  thetaMin             -1.94444e-06
  thetaMax             5.02778e-05
  thetaNSamples        5
  energyMean           0
  energyStd            1.41428e+08
  energyMin            -1.00004e+08
  energyMax            1.00004e+08
  energyNSamples       2
```

### Command line options

| Option | Meaning |
|---|---|
| `--config NAME` | Load `NAME.yaml` from `KIWI_SCAN_CONFIG_DIR` or the package config directory. |
| `--config-file PATH` | Load an explicit YAML file. |
| `--role ROLE` | Subscription role to collect. Default: `sync`. |
| `--interval SECONDS` | Print interval. Default: `1.0`. Must be greater than zero. |
| `--duration SECONDS` | Optional total runtime. Without this, the tool runs until `Ctrl+C`. |
| `--reset-each-print` | Reset the collector window after each printed row. |
| `--no-timestamp` | Do not prepend `TS-ISO8601` to output rows. |
| `--replace KEY=VALUE ...` | Apply YAML token replacements before parsing the config. |
| `--log-level 0..5` | Set kiwi-scan logging verbosity. |

`--config` and `--config-file` are mutually exclusive; one of them is required.
