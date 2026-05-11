# kiwi-scan Vim support

Minimal version of vim config for highlighting kiwi-scan YAML configs.

## Install

From the kiwi-scan repository root:

```bash
mkdir -p ~/.vim
cp -r vim/* ~/.vim/
```
Make sure vim syntax highlighting is enabled:

```vim
syntax on
```

## Uninstall

Simply remove the files from ~/.vim:

```bash
rm ~/.vim/ftdetect/kiwi_scan_yaml.vim
rm ~/.vim/syntax/kiwi_scan_yaml.vim
```

## Usage

Automatic activation happens for `*.yaml` buffers whose first real key looks like a kiwi-scan config, for example `actuators:`, `subscriptions:` or `triggers:`.
Highlights only top level keywords for now.
