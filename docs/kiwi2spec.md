# kiwi2spec

`kiwi2spec` exports kiwi scan data to SPEC-compatible text data file.

## Quick start

Convert one scan data file:

```bash
kiwi2spec \
  --data-file scan_results-20260722202204.txt \
  --out scan_results.spec
```

Convert every scan referenced by a manifest:

```bash
kiwi2spec \
  --manifest-file manifest_20260722_202204.yaml \
  --out beamtime.spec
```

Convert the newest available manifest:

```bash
kiwi2spec \
  --latest-manifest \
  --out lastbeamtime.dat
```

### Single scan file

```bash
kiwi2spec --data-file SCAN_FILE --out OUTPUT_FILE
```

### Explicit manifest

```bash
kiwi2spec --manifest-file MANIFEST.yaml --out OUTPUT_FILE
```

All valid scan entries are exported to one output file. Manifest entries become `#S 1`, `#S 2`, and so on in the same order as in the manifest file.

### Latest manifest

```bash
kiwi2spec --latest-manifest --out OUTPUT_FILE
```

Manifest discovery uses `--data-dir` when supplied, otherwise `KIWI_SCAN_DATA_DIR` or the active kiwi-scan manifest location and falls back to relative path.

`--manifest-index 0` selects the newest manifest and is the default.

## kiwi2spec Command-line options

| Option | Meaning |
|---|---|
| `--manifest-file FILE` | Convert all scans referenced by an explicit manifest. |
| `--data-file FILE` | Convert one kiwi-scan data file. |
| `--latest-manifest` | Discover and convert a manifest. |
| `--out FILE` | Required output filename. The filename is used exactly as supplied. |
| `--metadata-file FILE` | Metadata sidecar for `--data-file`. |
| `--data-dir DIR` | Directory used for `--latest-manifest` discovery. |
| `--manifest-index N` | Select the N-th newest manifest; default is `0`. |
| `--no-metadata` | Do not parse metadata sidecar files. |
| `--include-metadata-monitor` | Write metadata monitor events as `#C` comments. |
| `--log-level 0..5` | Set kiwi-scan logging verbosity. |

The three source options—`--manifest-file`, `--data-file`, and `--latest-manifest`—are mutually exclusive.

## Column conversion rules

By default, SPEC data rows are kept numeric:

- Numeric columns are written directly.
- Non-numeric columns are dropped by default.
- Boolean columns are written as `0` or `1`.
- Time stamps are converted to POSIX epoch seconds and get an `_epoch_s` column suffix.

Dropped columns are recorded in a comment:

```text
#C kiwi_spec_dropped_columns: operator, sample_name
```

## Metadata handling

Metadata sidecars are exported by default when referenced by a manifest or defined by `--metadata-file`.

Metadata constants are written as `#C` lines:

```text
#C metadata_constant beamline: UE52
#C metadata_constant user: scientist
```

Monitor-event rows are not included by default because they can make the output file very large but can be enabled by:

```bash
kiwi2spec \
  --manifest-file manifest.yaml \
  --include-metadata-monitor \
  --out beamtime-with-monitor-events.spec
```

Metadata parsing can be disabled:

```bash
kiwi2spec \
  --manifest-file manifest.yaml \
  --no-metadata \
  --out beamtime.dat
```

