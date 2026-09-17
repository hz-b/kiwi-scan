# Sync Controller

`SyncController` coordinates a scan loop either with external EPICS monitor
events or with an internal absolute monotonic timer.

## External synchronization

Subscriptions with `role: sync` form one AND group. One scan cycle proceeds
only after every required source has produced a fresh event or reached its
configured per-cycle timeout:

1. `arm()` records the events for every source.
2. Subscription callbacks mark received events with `note_event()`.
3. wait until all required sync sources have updated with `wait()`

Events received before a cycle is armed do not satisfy that cycle. 
A timeout is a successful fallback for its individual source; `wait()` returns `False` only
when the supplied stop event is set.

```yaml
subscriptions:
  - name: energy_sync
    role: sync
    actuator: energy
    source: rbv
    timeout: 0.02

  - name: beta_sync
    role: sync
    pv: TEST:Beta
    timeout: 0.02
```

This lets the kiwi-scan loop wait for fresh `energy_sync` and `beta_sync` events. 
If one event is occasionally missing, that source becomes ready after 20 ms.
When a sync subscription refers to the primary actuator readback, its event
value is also used as the scan position. Otherwise the scan type polls the
actuator readback.

## Absolute timer fallback

When there are no external `sync` subscriptions, CM and Poll scans configure an
internal timer from `sample_rate_hz` through `set_samplerate()`.

The timer uses fixed `time.monotonic()` deadlines so processing time does not
accumulate as clock drift. If processing overruns one or more slots, those
slots are skipped and the next future slot is selected; the controller does not
emit catch-up points.

```yaml
sample_rate_hz: 1000.0
subscriptions: []
```

This requests 1 ms timer slots.

If neither external sources nor an internal timer is configured, `wait()` is immediately ready.

