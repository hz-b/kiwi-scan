# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

from kiwi_scan.manifestwriter import ManifestResolver
from kiwi_scan.scan.tools import set_valid_logging_level


def _manifest_paths(resolver: ManifestResolver, explicit_files: Iterable[str]) -> List[Path]:
    paths = [Path(p).expanduser() for p in explicit_files]
    if paths:
        missing = [str(p) for p in paths if not p.is_file()]
        if missing:
            raise FileNotFoundError("Manifest file not found: " + ", ".join(missing))
        return paths
    return resolver.list_manifests()


def iter_manifest_files(
    resolver: ManifestResolver,
    manifest_files: Iterable[Path],
    include_meta: bool = False,
    include_manifest: bool = False,
    missing: bool = False,
) -> Iterable[Path]:
    """Yield manifest and referenced file paths as absolute one-line paths."""
    seen = set()
    for manifest_file in manifest_files:
        for path in resolver.list_files(
            str(manifest_file),
            include_meta=include_meta,
            include_manifest=include_manifest,
            missing=missing,
        ):
            if path not in seen:
                seen.add(path)
                yield path


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="manifestfiles_cli",
        description=(
            "List files referenced in manifest files.\n\n"
            "By default, all manifest*.yaml files in KIWI_SCAN_DATA_DIR are scanned, "
            "and only scan data files are printed."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        help="Directory containing manifest*.yaml/yml files. Defaults to KIWI_SCAN_DATA_DIR.",
    )
    parser.add_argument(
        "--manifest-file",
        action="append",
        default=[],
        help="Explicit manifest file to inspect. Repeatable. Overrides manifest discovery.",
    )
    parser.add_argument(
        "--include-meta",
        action="store_true",
        help="Also print metadata files referenced by scan entries.",
    )
    parser.add_argument(
        "--include-manifest",
        action="store_true",
        help="Also print the manifest file path itself.",
    )
    parser.add_argument(
        "--missing",
        action="store_true",
        help="Also print referenced files that do not exist on disk.",
    )
    parser.add_argument(
        "--log-level",
        type=int,
        default=3,   # WARNING
        help="MBBO record level (0..5) mapped to Python logging",
    )
    
    args = parser.parse_args(argv)
    
    if args.log_level is not None:
        set_valid_logging_level(args.log_level)

    try:
        if args.data_dir or not args.manifest_file:
            resolver = ManifestResolver(args.data_dir)
        else:
            resolver = ManifestResolver.from_manifest_file(args.manifest_file[0])

        manifests = _manifest_paths(resolver, args.manifest_file)
        if not manifests:
            parser.exit(1, f"No manifest*.yaml or manifest*.yml files found in {resolver.data_dir}\n")

        count = 0
        for path in iter_manifest_files(
            resolver,
            manifests,
            include_meta=args.include_meta,
            include_manifest=args.include_manifest,
            missing=args.missing,
        ):
            print(path)
            count += 1

        if count == 0:
            parser.exit(1, "No matching file references found. Use --include-meta or --missing if needed.\n")
        return 0
    except (FileNotFoundError, ValueError, IndexError) as exc:
        parser.exit(2, f"manifestfiles_cli: error: {exc}\n")

if __name__ == "__main__":
    main()
