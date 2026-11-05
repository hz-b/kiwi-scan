# Notes for contributors and AI coding assistants

The kiwi-scan package supports beamline data acquisition, automation, and diagnostics in a heterogeneous environment based primarily on EPICS.
It is a modular framework that supports AI-assisted development and can be integrated into agent-driven workflows.

There is a clear separation between common interfaces and concrete implementations.
The documentation of the contracts of monitors, actuators, plugins and scan engines is still being developed.

Scans can run at acquisition rates above 1 kHz, so the per-point processing path - the “hot path” - is optimized to take 
approximately 100 µs per point on our test system.  The guidelines below help preserve this performance.

Different trade-offs may be appropriate for specific requirements, for example when implementing a custom scan engine. 
Such decisions with their implications should be understood and documented.

This document is a work in progress. Interface contracts and contributor guidelines will be updated as the framework evolves.


## Rules that apply everywhere

- Detector reads, triggers, and print monitors intentionally perform I/O when necessary. Avoid introducing blocking operations into performance-sensitive path.
  For example hot path code (plugin `get_headers`/`get_values`, `build_data_snapshot`, monitor `update`, the scan point loop) avoids  
  `time.sleep`, `print` and f-string logging or per point logging. When diagnostics are necessary, 
  use lazy formatting and consider rate limiting (use `logger.debug("x=%s", x)`).
- Precompute static headers, lookups, and reusable configuration. Per-point lists and snapshots are sometimes necessary to preserve data ownership and freshness. 
  For example lists, header names and lookups can often be build once (`__init__`/`on_start`), not per point.
- Preserve data freshness and ordering. Changes to caching, plugin execution, or asynchronous processing must preserve which scan point each value belongs to.
- Plugin headers must remain stable throughout a scan. Every point, including fallback results, must return the same number of values in the same order as the headers.
- Handle failures deliberately. Keep acquisition, output, and cleanup errors visible. A cleanup failure should not hide the original error.
- Scans should run robust. An exception in plugin `get_values()` or monitor `update()` fails the scan.
  Return fallback values (`float("nan")`) for data problems.
- Keep the core reusable. Place site-specific hardware logic, plugins, and scan types in external packages where practical.
- Never wait with `time.sleep` in the scan thread. For example in scan engines use `self._stop_requested.wait(t)` so `stop()` stays responsive.
- Stop requests may come from another thread. Wait for execution to finish before closing. Cleanup must tolerate repeated calls. 
- Validate configuration before acquisition starts. Check shared settings in the configuration validation code and extension-specific settings when initializing 
  extensions. Raise `ValueError` with a message naming the invalid parameter and its allowed values or range.
- Code must run on Python 3.8: no `match`, no `list[int]` at runtime, no parenthesized context managers, no `str.removeprefix`.
  Check both syntax and standard-library availability.
- Keep CLI layers thin. Put reusable behavior in library functions and reuse existing configuration models, readers, and formatters.
- Manage resources. Release PVs, callbacks, subscriptions, and worker threads after use and on failure. 
  Scan engines must consistently handle plugin startup, completion, and cleanup.
- Verify the change. Add focused tests. 

## Notes on Extension contracts

- Plugins: `get_headers()` is called on every point, not only once. Build the header list in `__init__` and return the same list.
- Plugins: `on_monitor()` runs on one worker thread shared by all plugins of a scan, and every plugin receives every `role: plugin` event.
  Filter on `ev.pvname` first and keep the method short. Hand data to `get_values()` with one assignment of an immutable value.
- Async plugins: `AsyncScanPlugin.__init__` calls `get_async_default_values()`. Set the attributes it needs before `super().__init__()`.
  `process_data_snapshot()` runs in a worker thread and uses only the snapshot, never `self.scan`.
- Scan engines: a custom `scan()` runs all cleanup in `finally`, including `monitor.close()`. Without it, `execute()` blocks forever in `monitor.loop()`.
- Actuators: `move()` returns at once. `wait_until_done(position, stop_event=None)` returns promptly when `stop_event` is set.
  `stop()` is fast and safe to call from any thread.

## Checks, Finish

- Run the full test suite before submitting (`make lint && make test`).
- When changes affect acquisition, synchronization, plugin processing, or monitor performance, run one real scan with `performance_report: true` 
  and compare the `plugins` (or your loop's) timing with and without your change. A `type: sim` actuator with `detector_pvs: []` runs without any IOC;
- Document public changes. Update relevant YAML parameters, defaults, API descriptions, examples, and changelog entries.
