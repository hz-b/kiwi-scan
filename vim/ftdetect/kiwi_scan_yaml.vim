" Auto detect kiwi scan yaml

augroup kiwi_scan_yaml
  autocmd!
  autocmd BufRead,BufNewFile *.yaml,*.yml call s:kiwi_scan_detect()
augroup END

" Top-level keys that mark a file as a kiwi-scan config.
" Keep in sync with kiwiScanTopLevelKey in syntax/kiwi_scan_yaml.vim.
" The generic keys "debug" and "monitor" are left out here on purpose,
" so unrelated YAML files using them are not detected as kiwi-scan.
let s:kiwi_scan_keys = '^\(actuators\|detector_pvs\|detector_pvs_monitor\|scan_dimensions\|plugin_configs\|monitor_type\|stop_pv\|data_dir\|output_file\|include_timestamps\|integration_time\|performance_report\|data_writing_enabled\|manifest_mode\|triggers\|metadata_pvs\|metadata_constants\|metadata_file\|subscriptions\|sample_rate_hz\|timestamp_output_format\|detector_reader_strategy\)\s*:'

function! s:kiwi_scan_detect() abort
  for line in getline(1, 50)
    if line =~# s:kiwi_scan_keys
      setlocal filetype=kiwi_scan_yaml
      return
    endif
  endfor
endfunction
