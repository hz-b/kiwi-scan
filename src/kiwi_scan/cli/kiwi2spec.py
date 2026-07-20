# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Convenience CLI wrapper for SPEC export."""

from kiwi_scan.cli.convert import main as convert_main

import sys
from typing import List, Optional



def main(argv: Optional[List[str]] = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    return convert_main(["--format", "spec"] + args, prog="kiwi2spec")


if __name__ == "__main__":
    raise SystemExit(main())
