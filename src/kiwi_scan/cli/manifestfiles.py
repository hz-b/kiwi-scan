# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

from kiwi_scan.manifestwriter import ManifestArchiveDeleter, ManifestResolver, ManifestWriter
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
        prog="manifestfiles",
        description=(
            "List files referenced in manifest files, or create a small manifest.\n\n"
            "By default, all manifest*.yaml files in KIWI_SCAN_DATA_DIR are scanned, "
            "and only scan data files are printed.\n\n"
            "Examples:\n"
            "  manifestfiles --create scan1.txt scan2.txt\n"
            "  manifestfiles --create scan1.txt --manifest-file manifest_for_spec.yaml\n"
            "  manifestfiles --delete --manifest-file manifest.yaml --include-meta --dry-run\n"
            "  manifestfiles --delete --manifest-file manifest.yaml --include-meta"
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
        help=(
            "Explicit manifest file to inspect. Repeatable. Overrides manifest discovery. "
            "With --create, this is the output manifest path and may be used only once."
        ),
    )
    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "--create",
        nargs="+",
        metavar="DATA_FILE",
        help=(
            "Create a new small manifest that references the given data files, "
            "then print the created manifest path."
        ),
    )
    action_group.add_argument(
        "--delete",
        action="store_true",
        help=(
            "Interactively archive manifest-related files into a bundle and then delete them. "
            "Use --dry-run to only print the deletion plan."
        ),
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
        "--bundle-dir",
        default="/tmp",
        help="Directory for --delete archive bundles. Default: /tmp",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --delete, print the plan without creating an archive or deleting files.",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="With --delete, do not ask for interactive confirmation.",
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
        if args.create:
            if len(args.manifest_file) > 1:
                parser.error("--create accepts at most one --manifest-file output path")

            manifest_path = ManifestWriter.create_small_manifest(
                args.create,
                filename=args.manifest_file[0] if args.manifest_file else None,
                directory=args.data_dir,
            )
            print(manifest_path)
            return 0

        if args.data_dir or not args.manifest_file:
            resolver = ManifestResolver(args.data_dir)
        else:
            resolver = ManifestResolver.from_manifest_file(args.manifest_file[0])

        manifests = _manifest_paths(resolver, args.manifest_file)
        if not manifests:
            parser.exit(1, f"No manifest*.yaml or manifest*.yml files found in {resolver.data_dir}\n")

        if args.delete:
            plan = resolver.plan_delete_bundle(
                manifests,
                include_meta=args.include_meta,
                include_manifest=True,
                bundle_dir=args.bundle_dir,
            )

            print(f"Bundle: {plan.bundle_file}")
            print("Files to archive and delete:")
            for path in plan.files_to_delete:
                print(path)

            if plan.missing_files:
                print("Missing references:")
                for path in plan.missing_files:
                    print(path)

            if plan.skipped_files:
                print("Skipped non-regular files:")
                for path in plan.skipped_files:
                    print(path)

            if args.dry_run:
                ManifestArchiveDeleter.archive_and_delete(plan, dry_run=True)
                return 0

            if not args.yes:
                answer = input("Archive these files and delete originals? [y/N] ").strip().lower()
                if answer not in {"y", "yes"}:
                    parser.exit(1, "Aborted. No files were deleted.\n")

            result = ManifestArchiveDeleter.archive_and_delete(plan)
            print(f"Created bundle: {result.bundle_file}")
            print(f"Deleted files: {len(result.deleted_files)}")
            if result.failed_files:
                print(f"Failed files: {len(result.failed_files)}")
                return 1
            return 0

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
        parser.exit(2, f"manifestfiles: error: {exc}\n")

if __name__ == "__main__":
    main()
