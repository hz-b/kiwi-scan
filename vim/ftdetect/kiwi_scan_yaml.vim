" Auto detect kiwi scan yaml

augroup kiwi_scan_yaml
  autocmd!
  autocmd BufRead,BufNewFile *.yaml,*.yml call s:kiwi_scan_detect()
augroup END

function! s:kiwi_scan_detect() abort
  for line in getline(1, 50)
    if line =~# '^\(actuators\|detector_pvs\|scan_dimensions\|subscriptions\|triggers\|plugin_configs\|metadata_pvs\|metadata_constants\|monitor_type\|data_dir\|output_file\):'
      setlocal filetype=kiwi_scan_yaml
      return
    endif
  endfor
endfunction
