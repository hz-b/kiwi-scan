# Performance testing

`kiwi-scan` can optionally measure the runtime of scan operations and generate a summary at the end of the scan.

For scans with update intervals of 1 ms or longer, the performance measurement overhead is negligible. The logging level should be set to `INFO==2` or higher.

## Enable performance reporting

Enable the option in the scan YAML file at top level:

```yaml
performance_report: true
```

## Reported measurements

Depending on the scan type, the report includes timings for:

- detector reads
- plugin processing
- data-file writing
- monitor updates
- triggers and synchronization waits
- actuator and position checks
- scan setup and cleanup

The metadata monitor also reports the number of events dropped because its writer queue was full.
