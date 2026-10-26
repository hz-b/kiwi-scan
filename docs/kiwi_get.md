# Detector readout and read timeouts

`kiwi-get` reads the configured detector PVs once using direct Channel Access
gets and prints the existing monitor format: one header and one value row for
TSV/CSV, or one object for JSON. `--timestamps` adds per-PV ISO-8601 timestamps.

```bash
kiwi-get --config-file mono.yaml --replace IOC_MONO=U171PGM1 --timeout 1.5
kiwi-get --config-file mono.yaml --format json --timestamps
```

`--timeout` is one read timeout, in seconds, applied to every detector PV.
It overrides YAML `detector_read_timeout`. If neither is supplied, this
one-shot tool uses 1 second. `--connection-timeout` is separate and defaults
to 1 second per PV. These are per-operation limits, not a whole-row deadline.

The tools function catches PV initialization errors, logs the failed name,
and continues with the other PVs. Failed connections and failed reads retain
empty columns (JSON `null`) in the original order. Exit status is `1` if any
PV is unavailable, otherwise `0`. Invalid arguments/configuration return `2`.
No actuators, triggers, plugins, scans, or output files are started.

## Reuse in Python

```python
from kiwi_scan.tools.detectors import print_detector_values

status = print_detector_values(config, read_timeout=1.5, include_timestamps=True)
```

The function owns PV creation, failure handling, the existing detector reader,
the existing monitor formatter, and cleanup. It also accepts `output_format`,
`connection_timeout`, and an optional output stream `out` which it does not
close. `kiwi-get` only parses arguments, loads the YAML, and calls this function.

## Configure scans

```yaml
detector_reader_strategy: direct
detector_pvs_monitor: false
detector_read_timeout: 1.5
```

The optional timeout applies to all detector PVs using the direct strategy.
It must be finite and greater than zero; numeric strings are also accepted.
With `detector_pvs_monitor: true`, monitor-cache behavior is preserved and the
timeout only governs a read that needs to obtain data through CA. It does not
force a new CA get. The snapshot strategy reads its local callback cache and
ignores this timeout.

Omitting the field, or setting it to `null`, preserves existing scan behavior:
normal wrapper reads keep their default `min(pv.timeout, 0.2)` timeout. Scan
connection settings and startup failure behavior are unchanged. The tool's
continue-on-initialization-error behavior is confined to its tools function.

For API callers, the factory accepts the same setting:

```python
reader = create_detector_reader("direct", pvs, use_monitor=False, read_timeout=1.5)
```

The timeout is validated and stored once during reader construction. Every
direct read passes the stored value using the same call, with no timeout
selection branch in the read loop. When unset, `timeout=None` lets the EPICS
wrapper retain its existing defaults. PV implementations must accept the
optional `timeout` keyword declared by `DetectorPV`. Snapshot acquisition and
monitor formatting are unchanged.

`EpicsPV.get_with_metadata(full=True, timeout=...)` now forwards an explicit
timeout to PyEPICS. Calling `full=True` without a timeout continues to use
the previous PyEPICS defaults.
