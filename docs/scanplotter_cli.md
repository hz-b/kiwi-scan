# scanplotter_cli

`scanplotter_cli` plots saved scan data and metadata on one or more Y axes.
Choose an X column with `--x_column` and repeat `--series` for each curve.

## Examples

Plot an explicit scan file (replace column names with those in your file):

```bash
scanplotter_cli --x_column Position \
  --series 'file=scan-results.txt,column=intensity,axis=0,label=Intensity'
```

Plot the newest scan from the newest manifest in `KIWI_SCAN_DATA_DIR`:

```bash
scanplotter_cli --x_column Position \
  --series 'column=intensity'
```

Plot scan and metadata signals against time on separate Y axes:

```bash
scanplotter_cli --manifest-index 0 --scan-index 0 --x_column TS-ISO8601 \
  --series 'column=intensity,type=scan,axis=0,label=Intensity' \
  --series 'column=BEAM:CURRENT,type=meta,axis=1,label=Current'
```

For metadata, `column` is the recorded PV name and `--x_column` must name the
metadata time index, normally `TS-ISO8601`. Each curve uses its own timestamps.

## Series syntax

```text
column=NAME[,file=PATH|auto,axis=N,label=TEXT,type=scan|meta]
```

| Key | Meaning | Default |
| --- | --- | --- |
| `column` | Scan column or metadata PV name; required | — |
| `file` | Explicit path, or resolve from the selected manifest | `auto` |
| `axis` | Y-axis index; use `0`, `1`, etc. for separate axes | `0` |
| `label` | Legend label | Column name |
| `type` | `scan` or `meta` source | `scan` |

Omitting `file`, or using `file=auto` or `file=manifest`, selects the corresponding
scan or metadata file from the manifest. Explicit paths bypass manifest selection.
Quote series containing spaces; commas separate fields and cannot appear in values.

## Options

| Option | Meaning | Default |
| --- | --- | --- |
| `--x_column NAME` | X column for every series; required | — |
| `--series SPEC` | Curve specification; required and repeatable | — |
| `--manifest-index N` | Manifest index, newest first | `0` |
| `--manifest-file PATH` | Explicit manifest; overrides `--manifest-index` | — |
| `--scan-index N` | Scan entry within the manifest, newest first | `0` |
| `--join-tol SECONDS` | Accepted join tolerance; currently unused by the plotter | `0.1` |
| `--log-level LEVEL` | Mbbo style logging level, e.g. `1` (DEBUG), `2` (INFO), `3` (WARNING) | `3` |
| `-h`, `--help` | Show command help | — |
