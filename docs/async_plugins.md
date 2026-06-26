# AsyncScanPlugin

`AsyncScanPlugin` is the plugin base extension for plugins that need to do slow work without blocking the scan loop.

The task is submitted to a background worker thread while the scan row receives the latest completed async result. The async result columns therefore update at the rate of the background processing, not necessarily at the full scan rate.


With event-driven data streaming, the DAQ rate of a Kiwi scan can be quite high. AsyncScanPlugin is intended for use cases where a plugin performs work that may take longer than a single scan point, for example:

- Curve fitting
- Signal analysis and encoder signal correction
- Execution of backgound control logic
- AI/ML inference
- Heavy data processing
- Network communication without blocking data acquisition


`AsyncScanPlugin` inherits from `ScanPlugin` and provides access to the current scan object through the self.scan weak reference. 

This allows async plugins to access scan data, for example:

```python
row_cache = self.scan.get_current_row_cache()
```

## Methods to be implemented by subclasses

Concrete async plugin implement these methods:

```python
# Return column header strings
def get_headers(self, timestamps: bool) -> List[str]:
# Per-point values (optional)
def get_sync_values(self, idx: int, pos: Any) -> List[Any]:
# Pending/error fallback values
def get_async_default_values(self) -> List[Any]:
# Copy all data needed by the worker
def build_data_snapshot(self, idx: int, pos: Any) -> Any:
# The non blocking processing step
def process_data_snapshot(self, data_snapshot: Any) -> List[Any]:
```

## Registration

Register the plugin using the normal kiwi-scan plugin registry.

Example:

```python
from kiwi_scan.plugin.registry import register_plugin
```
## Example
```python
#
# get_headers() as in plugin base
# get_sync_values() if any sync values, must match headers
# get_async_default_values() must match headers
#
def build_data_snapshot(self, idx: int, pos: Any) -> Any:
        row_cache = {}
        if self.scan is not None:
            row_cache = self.scan.get_current_row_cache()

        return {
            "scanIndex": idx,
            "position": pos,
            "row_cache": row_cache,
        }
# return must match headers
def process_data_snapshot(self, data_snapshot: Any) -> List[Any]:
        
        
        position = data_snapshot["position"]
        row_cache = data_snapshot["row_cache"]
        index = data_snapshot["scanIndex"]

        time.sleep(1,0)
        result = position * 2.0  # some slow heavy processing
        
        return [
            "ready",
            index,
            position,
            result,
        ]
```

## Notes
- Keep `get_sync_values()` fast.
- Copy all needed data in `build_data_snapshot()`.
- Do slow work only in `process_data_snapshot()`.
- Return a stable number of values matching the headers.
- Status column: `pending`, `ready`, or `error`.
- Source index column so delayed results are traceable.
