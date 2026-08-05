# Output files

A typical kiwi-scan can generate two kinds of files: a main scan-data file and an optional metadata sidecar. 
In addition, an optional manifest entry can be created that links both files to the scan configuration.

## Output-related YAML parameters

```yaml
# Output directory
data_dir: test

# Base name of the main scan file.
# A timestamp is inserted before the extension.
output_file: test.txt

# Configure timestamps (true/false) for individual detector and plugin values.
include_timestamps: true

# Enable/disable data writing (true by default)
data_writing_enabled: true

# Base name for the metadata
metadata_file: test-meta.txt

# PVs recorded asynchronously
metadata_pvs:
  - TEST:TEMPERATURE
  - TEST:PRESSURE

# Constant values of metadata
metadata_constants:
  beamline: u171pgm1
  operator: user

# Manifest entry detail: full, small, or off.
manifest_mode: full
```

### `data_dir`

Defines the output subdirectory. See docs for environment variables (`KIWI_SCAN_DATA_DIR`) and command line parameters

### `output_file`

Defines the base name of the main scan file. kiwi-scan adds a timestamp before the extension:

```text
test.txt -> test-2026___.txt
```

If that file already exists, a short unique suffix is added.

### `include_timestamps`

Controls timestamps for individual detector and plugin values.

A scan-line timestamp column `TS-ISO8601` is always generated. 
When `include_timestamps` is enabled, additional timestamp columns are added:

- detector value `TEST:DET` -> `TS-ISO8601-TEST:DET`
- plugin value `CalculatedValue` -> `TS-CalculatedValue`

Example with `include_timestamps: false`:

```text
Position  TS-ISO8601  TEST:DET  CalculatedValue
```

With `include_timestamps: true` it becomes:

```text
Position  TS-ISO8601  TEST:DET  TS-ISO8601-TEST:DET  CalculatedValue  TS-CalculatedValue
```

### `data_writing_enabled`

Enables or disables scan file output.

When set to `false`:

- no main scan file is created or extended
- the metadata monitor is not started
- the scan is not appended to the active manifest
- acquired values remain available through the runtime scan API

The setting can also be changed at runtime through the scan API or the generic IOC `DataWritingEnabled` record.

### `metadata_file`

Defines the base name of the metadata sidecar. 

### `metadata_pvs`

Lists EPICS PVs recorded asynchronously. The monitor writes an initial snapshot and then records all changes via monitor events.

### `metadata_constants`

Defines constants written to the meta data file. 

### `manifest_mode`

Controls how the scan is added to the active manifest:

- `full` - file references and complete scan configuration
- `small` - file references without the complete configuration block
- `off` - no manifest entry

The default is `full`.

## Main scan file

The main scan file is a tab-separated text file. Its columns are assembled in this order:

1. `Position`
2. columns supplied by registered column providers, such as statistics
3. `TS-ISO8601`, the timestamp of the scan row
4. detector values and optional detector timestamps
5. plugin-generated values and optional plugin timestamps

A minimal detector-free scan still contains:

```text
Position  TS-ISO8601
```
Numeric values are written in scientific notation. Non-numeric values are written as text.

## Metadata sidecar file

A metadata sidecar contains optional constants followed by a tab-separated event table.

Example header:

```text
# metadata_constants
# beamline    test
# operator    user
# --- metadata above; monitor data below ---
TS-ISO8601  PV  ALUE  PV-TS-ISO8601 SEVR  STAT
```

The columns are:

- `TS-ISO8601` - local receive time in UTC
- `PV` - process-variable name
- `VALUE` - scalar, waveform, or string value
- `PV-TS-ISO8601` - timestamp reported with the PV event, when available
- `SEVR` - EPICS alarm severity, when available
- `STAT` - EPICS alarm status, when available

## Manifest files

The manifest writer tracks a sequence of scans across independent command-line or API runs.

Create or select a new manifest file:

```bash
scan_runner --newmanifest [optional_filename.yaml]
```

Each scan appends references to its main data file and metadata sidecar. Depending on `manifest_mode`, the entry may also contain the complete scan configuration.

If no filename is given, a timestamped manifest is created, normally in `KIWI_SCAN_DATA_DIR`.

External scan types can append entries through:

```python
append_to_manifest(scan_type=None)
```

## Complete example

```yaml
output_file: scan.txt
metadata_file: scan-meta.txt
data_dir: commissioning
include_timestamps: true
data_writing_enabled: true
manifest_mode: full

metadata_constants:
  beamline: TEST
  measurement: detector-check

metadata_pvs:
  - TEST:TEMPERATURE
  - TEST:PRESSURE
```

A kiwi-scan using this configuration produces:

```text
commissioning/
├── scan-2026___.txt
└── scan-meta-2026___.txt
```

The active manifest contains references to both files.
