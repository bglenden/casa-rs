#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Keep Rust source files under the line limit.

Every Rust file in the workspace crate roots must fit within 1,500 lines.
Clippy's `too_many_lines` lint enforces the separate function length limit.
"""

import argparse
import pathlib
import sys

LIMIT = 1500
ROOT = pathlib.Path(__file__).resolve().parent.parent
SCAN_ROOTS = ("libs", "surfaces", "frontends", "dev", "suites")


def lengths():
    for root_name in SCAN_ROOTS:
        scan_root = ROOT / root_name
        if not scan_root.exists():
            continue
        for path in sorted(scan_root.rglob("*.rs")):
            if "target" in path.relative_to(ROOT).parts:
                continue
            with path.open(encoding="utf-8") as handle:
                yield path.relative_to(ROOT).as_posix(), sum(1 for _ in handle)


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    problems = [
        f"{path}: {count} lines, over the {LIMIT}-line limit; split it"
        for path, count in sorted(lengths())
        if count > LIMIT
    ]
    for problem in problems:
        print(f"check-file-length: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
