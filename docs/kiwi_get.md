# kiwi-get

`kiwi-get` reads the detector PVs defined in the kiwi-scan YAML configuration. It retrieves each value once using a direct Channel Access get and formats the results using the existing monitor formats.

Example Usage:

```bash
kiwi-get --config-file mono.yaml --replace IOC_MONO=U171PGM1 --timeout 1.5
kiwi-get --config-file mono.yaml --format json --timestamps
```

`--timeout` - read timeout, overrides YAML `detector_read_timeout`. 

`--connection-timeout` - connect timeout, when channel is created


## Python usage

```python
from kiwi_scan.tools.detectors import print_detector_values

status = print_detector_values(config, read_timeout=1.5, include_timestamps=True)
```