
# Kiwi Scan Types

## Supported built-in scan types in `kiwi-scan`:

- **cm** - Continuous monochromator scan using polling or synchronized DAQ and subscriptions.
- **poll** - Poll-based scan reading detector values at fixed intervals or synchronized DAQ and subscriptions.
- **linear** - Standard multi actuator point-by-point linear scan between start and stop positions.
- **approach** - Nonlinear approach-style scan with denser points near the target.

Example:

```bash
scan_runner --scan_type linear \
  --config-file config.yaml \
  --dim actuator=energy,start=400,stop=410,steps=11
```
### Parallel and multi-actuator scans

`kiwi-scan` supports scans with more than one actuator. Multiple scan dimensions must be provided e.g. by repeating `--dim` on the command line.

For point-by-point scans, the scan engine builds one position list per actuator. During execution, the positions are aligned to the same number of points. If one actuator has fewer points, its last target is repeated.

This allows scans such as:

```bash
scan_runner \
  --scan_type linear \
  --config-file ./mono.yaml \
  --dim actuator=energy,start=400,stop=410,steps=11 \
  --dim actuator=slit,start=20,stop=25,steps=11
```

### Backlash and overshoot handling

For point-by-point scans, actuators can define `backlash`. If backlash compensation is configured, the scan preparation can add an initial overshoot step before the real scan points. Detector data is skipped for the overshoot point.

### `linear`

`linear` is the standard point-by-point scan type.

It creates evenly spaced positions from `start` to `stop` for each configured actuator. If `steps < 2`, only the start position is used.

`LinearScan` can write simple online statistics for values received through a subscription.

During a scan point, sync monitor events may arrive faster than the detector readout. Instead of writing every sync event `LinearScan` computes statistical summary of sync events for the current DAQ window (configure `integration_time`).

The following columns are added to the main scan output:

| Column | Meaning |
|---|---|
| `StatsMean` | Mean value of collected samples |
| `StatsStd` | Standard deviation of collected samples |
| `StatsMin` | Minimum value |
| `StatsMax` | Maximum value |
| `StatsNSamples` | Number of samples collected during the DAQ window |

Example configuration:

```yaml
subscriptions:
  - actuator: energy
    source: rbv
    name: mono_energy
    role: sync
```

or

```yaml
subscriptions:
  - pv: "XXX:Beta"
    name: beta
    role: sync
```
### `approach`

`approach` is a nonlinear point-by-point scan type.

It generates a nonlinear series from `start` to `stop` using a non linear progression, producing an approach-style distribution of points.

Typical use cases:

- approach sequences
- testing closed loop systems with different step sizes
- when the target region is more important part of the range

### `poll`

`poll` reads detector values while the primary actuator is moving through a range.

Unlike `linear`, `poll` does not generate a fixed list of move targets for every scan point. Instead, it samples while the first actuator is moving and while the current position is inside the configured start/stop range.

It can use heartbeat and sync subscriptions. Without events, the configured sample time acts as a timeout fallback.

Typical use cases:

- continuous readout during a move
- diagnostics of an externally started scan
- simple DAQ sampling based on actuator readback
- synchronized acquisition using heartbeat and sync subscriptions
- processing plugin logic

### `cm`

`cm` is a simple implementation of continuous a move scan type.

It moves the configured actuator or actuators to the start position, stores their original velocities, applies the configured scan velocities, starts motion to the stop position, records detector values during the move, and restores the original velocities afterward.

The DAQ loop can be driven by heartbeat subscriptions, with the sample time used as a timeout fallback. The scan position can come from a sync subscription or fall back to the actuator readback.


## External scan-types

External scan types are registered with `register_scan(...)` and discovered from files or directories listed in `KIWI_SCAN_SCAN_PATH`.
New scan classes must be derived from the interface defined in [scan abstraction](../src/kiwi_scan/scan/scan_abs.py).

### Custom type example

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
