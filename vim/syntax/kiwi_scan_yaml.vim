" Simple vim syntax highlighting for kiwi-scan YAML

if exists("b:current_syntax")
  finish
endif

" Load normal YAML highlighting first
runtime! syntax/yaml.vim

" Match only top-level kiwi-scan keys.
" Top-level means: beginning of line, no leading spaces.
syntax match kiwiScanTopLevelKey /^\zs\(actuators\|detector_pvs\|detector_pvs_monitor\|scan_dimensions\|plugin_configs\|monitor_type\|monitor\|stop_pv\|data_dir\|output_file\|include_timestamps\|integration_time\|debug\|performance_report\|data_writing_enabled\|manifest_mode\|triggers\|metadata_pvs\|metadata_constants\|metadata_file\|subscriptions\)\ze\s*:/

" Pick a visible default highlight group
highlight default link kiwiScanTopLevelKey Statement

let b:current_syntax = "kiwi_scan_yaml"
