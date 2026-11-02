" Simple vim syntax highlighting for kiwi-scan YAML

if exists("b:current_syntax")
  finish
endif

" Load normal YAML highlighting first
runtime! syntax/yaml.vim

" Match only top-level kiwi-scan keys.
" Top-level means: beginning of line, no leading spaces.
" Keep in sync with s:kiwi_scan_keys in ftdetect/kiwi_scan_yaml.vim.
syntax match kiwiScanTopLevelKey /^\zs\(actuators\|detector_pvs\|detector_pvs_monitor\|scan_dimensions\|plugin_configs\|monitor_type\|monitor\|stop_pv\|data_dir\|output_file\|include_timestamps\|integration_time\|debug\|performance_report\|data_writing_enabled\|manifest_mode\|triggers\|metadata_pvs\|metadata_constants\|metadata_file\|subscriptions\|sample_rate_hz\|timestamp_output_format\|detector_reader_strategy\)\ze\s*:/

" ${NAME} placeholders, replaced by kiwi-scan when the config is loaded
" (same rule as yaml_loader: "${" up to the next "}").
" Also matched inside quoted strings, but not in comments.
syntax match kiwiScanPlaceholder /\${[^}]\+}/ containedin=ALLBUT,yamlComment

" Pick visible default highlight groups
highlight default link kiwiScanTopLevelKey Statement
highlight default link kiwiScanPlaceholder PreProc

let b:current_syntax = "kiwi_scan_yaml"
