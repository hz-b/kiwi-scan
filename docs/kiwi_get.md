# kiwi-get

`kiwi-get` reads the detector PVs defined in the kiwi-scan YAML configuration. It retrieves each value once using a direct Channel Access get and formats the results using the existing monitor formats.

## Example usage

```bash
kiwi-get --config-file mono.yaml --replace IOC_MONO=U171PGM1 --timeout 1.5
kiwi-get --config-file mono.yaml --format json --timestamps
```

`--timeout` sets the read timeout and overrides the YAML setting `detector_read_timeout`.

`--connection-timeout` sets how long to wait for each channel to connect when it is created.

## Python usage

```python
from kiwi_scan.tools.detectors import print_detector_values

status = print_detector_values(
    config,
    read_timeout=1.5,
    include_timestamps=True,
)
```
## kiwi-get defaults

| Setting | Selection order, highest priority first | Default when unspecified | Scope |
|---|---|---|---|
| Read timeout | CLI `--timeout`, then YAML `detector_read_timeout`, then the command default | `1.0` s | Each detector PV read |
| Connection timeout | CLI `--connection-timeout`, then the command default | `1.0` s | Each detector PV connection |

Changing the default via YAML setting:

```yaml
detector_read_timeout: 2.0
```
Override from comand line:

```bash
# Read timeout: 2.0 s per PV; connection timeout: 1.0 s per PV.
kiwi-get --config-file mono.yaml

# Override reads to 1.5 s per PV and connections to 3.0 s per PV.
kiwi-get --config-file mono.yaml --timeout 1.5 --connection-timeout 3.0
```

