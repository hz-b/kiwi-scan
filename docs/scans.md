
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

## External scan-types

External scan types are registered with `register_scan(...)` and discovered from files or directories listed in `KIWI_SCAN_SCAN_PATH`.
New scan classes must be derived from the interface defined in [scan abstraction](src/kiwi_scan/scan/scan_abs.py)
Create `scan_types/triangle_scan.py`:

### Custom type example

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
