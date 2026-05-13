# Changelog

All notable changes to `kiwi-scan` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

---

## [0.2.1] - 2026-05-15

### Added
- Added documentation for the trigger manager.
- Added documentation for the plugin system.
- Added documentation for the API.
- Added documentation for the actuator framework.
- Added documentation for the data models and YAML reference.
- Added documentation for kiwi scan types.
- Added documentation for vim syntax.
- Added basic version of vim syntax highlighting.
- Added `scan_trigger_cli` for executing configured trigger phases from the command line.
- Enhanced LoggingPlugin: Alarm log, actuator trace, point to point timing.

### Changed
- Improved EPICS actuator waits: short CA disconnects are handled as unknown status instead of motion.
- Improved actuator_runner: Stop active actuators on interrupt, forced exit on second Ctrl-C

---

## [0.2.0] - 2026-05-06

### Added
- First public PyPI release of `kiwi-scan`.
- Modular scan framework for EPICS-based commissioning and diagnostics.
- YAML-based scan configuration.
- Support for scan engines, actuators, detector PVs, plugins, triggers, subscriptions, metadata sidecars, and manifest files.
- Command-line tools for running scans and inspecting scan results.
