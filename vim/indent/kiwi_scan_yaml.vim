" Indentation for kiwi-scan YAML
"
" kiwi_scan_yaml is YAML, so reuse Vim's own YAML indent rules.

if exists("b:did_indent")
  finish
endif

runtime! indent/yaml.vim
