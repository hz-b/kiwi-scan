# Performance testing

`kiwi-scan` can optionally measure scan operation times and print a summary after the scan cleanup. 
Performance instrumentation and debug logging add extra load to the measured processes.
Normally, this overhead  can be ignored but must be considered for a sub-millisecond scan point pipeline (>1kHz samplerate).

## Enable performance reporting

Enable the option in the scan YAML file at top level:

```yaml
performance_report: true
```

Setting `debug: true` also enables the `PerformanceTracker`. Use an INFO or
less verbose logging level for representative throughput measurements; DEBUG
logging changes high-rate results.

## Reported measurements

Depending on the scan type, the report can include:

| Metric | Meaning |
|---|---|
| `daq:point` | Complete continuous point-processing block. |
| `read_detectors` | Detector read or atomic cache snapshot. |
| `update_row_cache` | Point-frame and cache construction. |
| `row_cache:detectors` | Detector value and metadata insertion. |
| `plugins` | Synchronous plugin processing time. |
| `write:data` | Point freeze and enqueue, not necessarily physical disk completion. |
| `monitor:update` | Live monitor publication (blocks scan task). |
| `sync:wait` | External synchronization or absolute timer wait. |
| `triggers:*` | Trigger processing phases (e.g. `on_point`). |
| `daq:run` | Complete DAQ loop. |

The report also prints non-timing diagnostics:

- Dropped metata data queueed events.
- Largest observed number of queued scan points for data writer.
- Longest time a point waited before the writer processed it.


## kiwi-scan performance 

Kiwi Scan synchronized data acquisition via subscriptions has been tested at
1.2 kHz using the [feedback-core example IOC](https://github.com/hz-b/feedback-core)
and its [performance scan configuration](https://github.com/hz-b/feedback-core/blob/main/testIoc/iocBoot/iocfeedbackTest/performance.yaml).
Those test runs acquired 10,000 points without observed loss.  
