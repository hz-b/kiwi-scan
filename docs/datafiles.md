# Output files

## Manifest files

The manifest writer can be used to track a sequence of scans across independent runs
from command line tools or API. 

Create or select a new manifest file:

```bash
scan_runner --newmanifest [optional_filename.yaml]
```
Each scan engine appends its configuration and output file reference to the active manifest.

Manifest writing can be controlled from YAML with `manifest_mode`:

```yaml
# default: full manifest entry including the full scan config
manifest_mode: full

# smaller manifest entry: data and metadata file references only, no full config block
manifest_mode: small

# do not append manifest entries for this scan
manifest_mode: off
```

If no filename is given, a timestamped file is created (in `KIWI_SCAN_DATA_DIR` if set).
`kiwi_scan.scan.common` provides `append_to_manifest(self, scan_type: str = None) -> None` for external scan types.

## Data files

A typical run can generate two kinds of files:

1. **Main scan file**
   - timestamped file name based on `output_file`
   - position column
   - per-line timestamp
   - detector values and optional detector timestamps
   - plugin-generated columns

2. **Metadata sidecar file**
   - constants from `metadata_constants`
   - initial PV snapshots
   - change-driven CA monitor events for the configured `metadata_pvs`

The post-mortem plotting tools can combine scan files and metadata files for later analysis.
### yaml config
Define output files and dirctory
```
output_file:  test.txt
metadata_file: test-meta.txt
data_dir: test
```
