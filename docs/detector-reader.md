# Detector reader strategies

The `DetectorReader` can read the `detector_pvs` for each point with differerent methods (strategies).

Relevant YAML settings:

```yaml
detector_reader_strategy: direct   # direct (default) | snapshot
detector_pvs_monitor: true         # direct only, default: true
detector_read_timeout: null        # direct only, seconds, default: null
```

The scan always connects detector PVs with a CA monitor (`auto_monitor=True`), whatever the strategy.

## Strategies

| Setting | What one point read does |
|---|---|
| `snapshot` | Monitor callbacks keep a local cache of the latest value of every detector. The read copies the whole cache under one lock. No Channel Access traffic in the scan thread. |
| `direct` + `detector_pvs_monitor: true` | Reads the PVs one after another with `get_with_metadata(use_monitor=True)`. Each read returns the value from the last CA monitor event, without a network round trip. |
| `direct` + `detector_pvs_monitor: false` | Reads the PVs one after another with a real CA get per PV and point. Each read waits for the IOC to answer. |

`detector_pvs_monitor` only matters for `direct`. The `snapshot` strategy always uses monitor
callbacks. 

`detector_read_timeout` sets the CA get timeout per PV for `direct`. If it is not set,
`direct` uses at most 0.2 s per PV. `snapshot` ignores it.

## Performance Scale

Median and 95th percentile time of one `reader.read()` call, measured against a local soft
IOC that updates all PVs at 100 Hz (4 vCPU cloud VM, IOC on localhost, 1000 reads each):

| Strategy | 4 PVs | 30 PVs |
|---|---:|---:|
| `snapshot` | < 1 µs | < 1 µs |
| `direct`, `detector_pvs_monitor: true` | 20 µs (p95 39 µs) | 176 µs (p95 1.3 ms) |
| `direct`, `detector_pvs_monitor: false` | 0.7 ms (p95 1.6 ms) | 8 ms (p95 11 ms) |

These numbers show the scale, not a guarantee. A real get costs at least one network round
trip per PV, so on a beamline network with remote IOCs `detector_pvs_monitor: false` is slower
than on localhost. This feature can be used for IOC/network peroformance tests. [performance.md](performance.md) documents snapshot read times for up to 2,000 PVs
on the reference test system (19 µs for 2,000 PVs).

## When to use which

- **`snapshot`** for fast scans and many detectors: continuous and synchronized scans,
  rates of 100 Hz and more, and every scan that has to stay inside the ~100 µs point budget.
  Its read time barely grows with the number of PVs, and the values come as one consistent set.
- **`direct` with `detector_pvs_monitor: true`** (default) for normal step scans with a
  few detectors, where a few µs per PV do not matter.
- **`direct` with `detector_pvs_monitor: false`** for diagnosis and production tests:
  checking that every IOC answers on every point, finding PVs that drop out, or reading
  values that a monitor deadband would hide. It is the slowest option, so use it at low rates.
  `kiwi-get` ([kiwi_get.md](kiwi_get.md)) reads detectors the same way.
