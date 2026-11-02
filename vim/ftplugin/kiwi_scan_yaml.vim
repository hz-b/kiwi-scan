" Filetype settings for kiwi-scan YAML
"
" kiwi_scan_yaml is YAML, so reuse Vim's own YAML settings
" (comments, indentation width, expandtab, ...).

if exists("b:did_ftplugin")
  finish
endif

runtime! ftplugin/yaml.vim
