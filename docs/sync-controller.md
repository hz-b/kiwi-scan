# Sync Controller

The `SyncController` coordinates scan loops with one or more configured
`role: sync` subscriptions.

It is used by scan engines to wait until all required synchronization sources
have produced a fresh event for the current scan cycle. This allows scans to
combine periodic polling, heartbeat-driven acquisition, and synchronized EPICS
monitor updates.

## Purpose

The sync controller provides a synchronization barrier for subscription
events:

1. collect all subscriptions with `role: sync`
2. start a new cycle with `arm()`
3. mark received events with `note_event(name)`
4. wait until all required sync sources have updated with `wait()`

If no sync subscriptions are configured, the controller is disabled and
`wait()` immediately succeeds.

## YAML example

This will block the scan loop until energy and beta updates:

```yaml
subscriptions:
  - name: energy_sync
    role: sync
    actuator: energy
    source: rbv

  - name: beta_sync
    role: sync
    pv: TEST:Beta
