# EMC getter plugin

`hzb.emc_getter` adds EMC getter command readbacks to scan data. 
A backgroung worker task handles the communication via a TCP connection. 
EMC via serial line is not supported. 
Each scan point uses the latest completed group of readings, so acquisition does not block while waiting for server replies. 
The commands follow the [EMC protocol](https://github.com/hz-b/emc).

## Scan configuration

This example reads the current energy in eV (`GPE`) and the integer status (`GST`) from a local EMC simulation. Set `host` and `port` to the address of your EMC server.

```yaml
plugin_configs:
  - type: hzb.emc_getter
    parameters:
      host: localhost
      port: 5001
      reply_terminator: lf
      getters:
        - command: GPE
          column: energy_eV
        - command: GST
          column: mono_status
          value_type: int
```
Usage:


Configure at least one read-only command that replies with `t <number>`. 
Each `column` must be unique and contain no whitespace. 
The default `value_type` is `float`. 
See the [mono mq command reference](https://github.com/hz-b/emc/blob/main/docs/emc-mq-backend.md) for real applications. 

| Parameter | Plugin default | Meaning |
| --- | --- | --- |
| `host` / `port` | `localhost` / `5001` | TCP endpoint for the EMC server. |
| `reply_terminator` | `none` | Match the server setting. The EMC server defaults to `lf`. Other choices are `crlf`, `cr`, and `none` (one TCP read per reply). |
| `timeout_s` | `0.5` | Timeout in seconds for connecting and for each command exchange. |
| `min_interval_s` | `0.0` | Minimum time between the starts of two getter cycles. Zero lets each scan point request a group when the worker is free. The VxWorks mono IOC can handle about 30..60 Hz depending on configuration |
| `record_times` | `false` | Add `<column>_before` and `<column>_after` immediately after each value, in Unix seconds measured by the client. |

The example adds `energy_eV` and `mono_status`, in that order. Both start as `NaN`. 
Values are added once a reading succeeds, successive rows can repeat its values until another read finishes. 
The plugin reports `NaN` for failures.
