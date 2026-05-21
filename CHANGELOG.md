# Changelog

All notable changes to `kiwi-scan` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]
### Added

- Added running online statistics utilities for multiple subscriptions (role=`stat`)
- Added a new CLI tool to monitor configured subscription streams in real time including statistics (mean, standard deviation, minimum, maximum, number of collected samples)
- Add new `para` scan type for passive parasitical step scans.
  The engine observes externally moved actuators, waits for them to enter the configured scan range, waits for a running/ready transition cycle, and records stable points using the standard DAQ, trigger, plugin, monitor, metadata, and statistics pipeline.
- Added ctrl+c support to scan_runner. First ctrl+c will stop all motors and exit the scan loop immediately.

### Changed
- Fixed timestamp handling so scan and metadata files use local timezone-aware timestamps, and plot/data loaders preserve the stored timezone instead of converting timestamps to UTC.
- Add sample_rate_hz to scan config and BaseScan. Calculating rate from steps in dimensions is depricated.
- mkvenv.sh now finds the kiwi-scan repository root from the script path, so it can be sourced from any working directory. A KIWI_SCAN_REPO_ROOT override is available for custom layouts.
- Improved monitor-based EPICS data acquisition using cached CA monitor reads with polling fallback with adjustable rate.
- Linear and Parasitcal step scans now maintain scalable online sync statistics during DAQ operation (mean, standard deviation, minimum, maximum, number of collected samples)
- Warn now when velocity cannot be restored in cm scan type.
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
- Added `scantrigger_cli` for executing configured trigger phases from the command line.
- Enhanced LoggingPlugin: Alarm log, actuator trace, point to point timing.

### Changed
- Improved EPICS actuator waits: short CA disconnects are handled as unknown status instead of motion.
- Improved actuator_runner: Stop active actuators on interrupt, forced exit on second Ctrl-C.
- Include temporary manifest files in scan file lists.
---

## [0.2.0] - 2026-05-06

### Added
- First public PyPI release of `kiwi-scan`.
- Modular scan framework for EPICS-based commissioning and diagnostics.
- YAML-based scan configuration.
- Support for scan engines, actuators, detector PVs, plugins, triggers, subscriptions, metadata sidecars, and manifest files.
- Command-line tools for running scans and inspecting scan results.
