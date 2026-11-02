# Print and Queue Plotter Monitors

The kiwi-scan monitor layer provides optional live output for scan data while a scan is running. Monitors receive the same row values that are written by the scan engine, including detector values, subscription-derived values, and plugin-provided columns.

Two monitors are currently available:

- `print`: writes scan rows to stdout in a machine-readable format.
- `plot`: writes the same optional stdout stream and opens live matplotlib plots.

## Configuration overview

The selected monitor is configured with the top-level `monitor_type` key. Monitor-specific options are defined in the section `monitor:`.

Example:

```yaml
monitor_type: "plot"
monitor:
  print:
    enabled: true
    format: json
    include_header: true
  plots:
    - x: t
      y: [ "IOC_NAME:monoGetEnergy" ]
      title: "Energy [eV]"
    - x: "energyMean"
      y: [ "IOC_NAME:GetGA1", "IOC_NAME:GetGA2", "IOC_NAME:GetMA1", "IOC_NAME:GetMA2" ]
      title: "Analog Encoder Signals"
```

## Monitor API

Both monitor types implement the same small monitor interface:

```python
monitor.start(signal_names, headers=None)
monitor.update(vals)
monitor.loop()
monitor.close()
```

## Print monitor

The print monitor writes scan rows to stdout. It is intended for shell pipelines, debugging, logging, and machine-readable monitoring.

Example:

```yaml
monitor_type: "print"
monitor:
  print:
    enabled: true
    format: tsv
    include_header: true
```

### Print options

The print options are read from `monitor.print`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Enables or disables stdout output. |
| `format` | string | `tsv` | Output format: `tsv`, `csv`, or `json`. |
| `include_header` | bool | `true` | Writes a header row for `tsv` and `csv`. Ignored for JSON lines. |
| `include_timestamps` | bool | `false` | TS formatter option.
| `float_format` | string | `.12e` | Python float format used for scalar numeric values. |

### Output formats

#### TSV

Tab-separated text with header row.

```text
energyMean	IOC_NAME:GetGA1
3.000000000000e+02	1.234000000000e+00
```

#### CSV

Comma-separated text with header row.

```text
energyMean,IOC_NAME:GetGA1
3.000000000000e+02,1.234000000000e+00
```

#### JSON

Each row is represented by key–value pairs.

```json
{"energyMean": 300.0, "IOC_NAME:GetGA1": 1.234}
```

During a standard scan, the monitor receives the already assembled pipeline
row and exact scan header. Timestamp columns are therefore controlled by the
scan-level `include_timestamps` and `timestamp_output_format` settings, and the
monitor does not add a second set of timestamp fields.

The scan-file writer performs ISO-8601 conversion outside the DAQ thread. Live
monitor rows receive the raw POSIX timestamp values before that conversion. 

## Queue plotter monitor

The queue plotter monitor combines live plotting with optional PrintMonitor-compatible stdout output. The scan thread passes the data to a buffer (non blocking). The GUI loop converts and updates the values using a fixed-size ring buffer.
Updating the plot can be costly. Use it only for interactive scans where a live plot helps during setup or diagnostics or when timing is not critical.
Example:

```yaml
monitor_type: "plot"
monitor:
  print:
    enabled: true
    format: json
    include_header: true
  plots:
    - x: t
      y: [ "IOC_NAME:monoGetEnergy" ]
      title: "Energy [eV]"
    - x: "energyMean"
      y: [ "IOC_NAME:GetGA1", "IOC_NAME:GetGA2", "IOC_NAME:GetMA1", "IOC_NAME:GetMA2" ]
      title: "Analog Encoder Signals"
```

### Plot options

Plots are configured as a list below `monitor.plots`.

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `x` | string | no | X channel name. Defaults to `t`. |
| `y` | string or list | yes | One or more Y channel names. Multiple Y channels are drawn in the same panel. |
| `title` | string | no | Plot title. Defaults to the Y channel names. |
| `xlabel` | string | no | Override for the X axis label. |
| `ylabel` | string | no | Override for the Y axis label. |

The queue plotter automatically adds the synthetic channel `t`, which is the elapsed time in seconds since monitor start. All other plot channel names must match the scan output headers.

### Performance options

These keys sit directly below `monitor:` and control how much the live plot costs the scan.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `max_points` | int | `10000` | Points kept per channel. Older points scroll out of the plot. Also the size of the hand-off buffer between the scan and GUI threads. |
| `max_draw_points` | int | `2000` | Points drawn per line. Longer histories are decimated before drawing. |
| `refresh_interval` | float | `0.1` | Seconds between GUI refreshes. A frame is only redrawn when new points arrived. |
| `marker` | string | none | Matplotlib marker such as `o`. Markers make each frame more expensive to draw. |
| `keep_open` | bool | `false` | Keep the final plot on screen after the scan ends. The scan command returns only after the plot window is closed. |

```yaml
monitor_type: "plot"
monitor:
  print:
    enabled: false
  max_points: 20000
  max_draw_points: 2000
  refresh_interval: 0.2
  plots:
    - x: t
      y: [ "IOC_NAME:GetGA1" ]
```

The GUI runs in the same Python process as the scan. While matplotlib renders a frame it holds the GIL, so the scan thread can be delayed by a few milliseconds per frame. These delays appear in whichever scan timing block was running at the time (for example `read_detectors`), not in `monitor:update`. For high-rate scans, adjust `refresh_interval`, `max_draw_points`, and use `print.enabled: false` if possible.

If the GUI falls more than `max_points` rows behind, the oldest pending rows are dropped. 

Closing the plot window stops plotting for the rest of the scan.

### Keeping the plot after the scan

By default the plot window closes as soon as the scan finishes. Set `keep_open: true` to keep it: the window title changes to "Live Queue Plotter (scan finished)", zoom and pan keep working, and the scan command waits until you close the window. 

```yaml
monitor_type: "plot"
monitor:
  keep_open: true
  plots:
    - x: t
      y: [ "IOC_NAME:GetGA1" ]
```

### Multi-panel plotting

Each entry in `monitor.plots` creates one panel. This makes it possible to show independent live graphs in the same plotting window:

```yaml
monitor:
  plots:
    - x: t
      y: detector1
      title: Detector 1
    - x: t
      y: detector2
      title: Detector 2
```

### Multiple Y channels per panel

The `y` field may contain a list of channels. These channels are drawn against the same X channel in one panel and a legend is shown:

```yaml
monitor:
  plots:
    - x: energyMean
      y: [ga1, ga2, ma1, ma2]
      title: Analog Encoder Signals
      xlabel: Energy [eV]
      ylabel: Encoder signal
```

If no explicit `plots` list is configured and at least one data channel is available, the queue plotter falls back to a single plot of the first data channel against `t`.

[Queue plotter example for simulated monochromator data](https://github.com/hz-b/kiwi-scan/blob/master/docs/images/sim-plot.png)
