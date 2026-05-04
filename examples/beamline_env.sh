#!/usr/bin/env bash

# --- Required ---
export KIWI_SCAN_CONFIG_DIR=/opt/epics/.../scan_config
export KIWI_SCAN_DATA_DIR=${HOME}/data

# --- Optional extensions ---
export KIWI_SCAN_PLUGIN_PATH=/opt/epics/.../plugins
export KIWI_SCAN_SCAN_PATH=/opt/epics/.../scans

# --- Optional manifest control ---
# export KIWI_SCAN_MANIFEST_FILE=...
# export KIWI_SCAN_MANIFEST_STATE_FILE=...

# --- Optional YAML replacements ---
# export KIWI_SCAN_REPLACE_*=...
