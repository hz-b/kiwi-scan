# Point pipeline

The point pipeline coordinates the per-point data flow: 

- point snapshots 
- frame construction 
- through provider and plugin values
- file writing. 

It keeps data acquisition and file I/O out of the high-rate scan loop.

## Detector acquisition

`DetectorReader` is an abstract interface that hides the configured detector-reading strategy from the scan engines.

- **Direct:** Reads detectors sequentially via get_with_metadata(). It uses cached monitor values or direct CA gets.
- **Snapshot:** Monitor callbacks maintain a shared cache. The data is then copied under one lock for an atomic snapshot. 

## Scan points

Scan data is visible to plugins. Plugin results are available to subsequent plugins.
For each acquired point, the scan thread builds a mutable point frame from provider, detector, and plugin data. 
Then finalizes it as an immutable prepared point.  

## Runtime caches

The pipeline maintains two data sets:

- The **current row** is the point being assembled. Plugins use `get_current_row_value()` or `get_current_row_cache()` to read it.
- The **last point** is the most recently completed point. Applications and the IOC use `get_value()` to read it.

get_current_row_cache() returns a dictionary copy, while get_current_row_value() accesses a single value directly 
and therefore should be preferred when only one value is needed.

Runtime timestamp keys are independent of file formatting:

| Key | Meaning |
|---|---|
| `TS` | Raw POSIX timestamp when the complete scan row was created.|
| `TS-<PV>` | Raw detector PV EPICS timestamp. This column is created when `include_timestamps` is enabled. |
| `TS-<PluginHeader>` | Raw wrapped-plugin timestamp, created when `include_timestamps` is enabled. |

## Plugin columns

Plugin values are wrapped with timestamp metadata for runtime diagnostics, but the scan file does not add one timestamp column per plugin value. 
All plugin columns share the common row timestamp. This avoids large numbers of redundant columns and timestamp conversions in the DAQ hot path
and this is different from preivious kiwi-scan versions.

Detector timestamps can still be written individually with
`include_timestamps: true`.

## File I/O

Completing a frame creates an immutable prepared point containing:

- raw values,
- raw POSIX timestamps, and the indices of fields that require timestamp formatting. 

This data set is submitted to a FIFO writer thread.

The writer task:

- preserves point order
- renders timestamp fields as configured by `timestamp_output_format`
- formats other values for the tab-separated scan file
- records queue high-water and maximum queue-delay diagnositcs
- it will block the scan task if the queue is full, data is not lost.

During scan cleanup, all accepted requests are finished. 
Errors are reported after the other cleanup operations have been completed.

The `OutputManager` can enable/disable writing during runtime 
owns the output filename, lazy file creation, header state, and runtime `data_writing_enabled` flag. The output lock keeps a
writing-state change synchronized with point submission.

## Notes on performance reports

See [performance.md](performance.md).

- `read_detectors`: obtain direct readings or a local atomic snapshot
- `update_row_cache`: build the base/provider/detector frame
- `plugins`: calculate synchronous plugin columns
- `write:data`: freeze and enqueue the point (not the disc write)
- `write:drain`: wait for file I/O during cleanup

