#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Keep Rust source files under the line limit.

A file under `crates/` longer than LIMIT lines must be listed in the
allowlist, and a listed file must leave the list once it is back under the
limit, so the list only shrinks (#705). Function length is clippy's
`too_many_lines` lint (`clippy.toml`).

  python3 scripts/check-file-length.py           # check
  python3 scripts/check-file-length.py --update  # list the files over the limit now
"""

import json
import pathlib
import sys

LIMIT = 1500
ROOT = pathlib.Path(__file__).resolve().parent.parent
ALLOWLIST = ROOT / "resources" / "file-length-allowlist.json"


def lengths():
    for path in sorted((ROOT / "crates").rglob("*.rs")):
        if "target" in path.relative_to(ROOT).parts:
            continue
        with path.open(encoding="utf-8") as handle:
            yield path.relative_to(ROOT).as_posix(), sum(1 for _ in handle)


def main():
    current = dict(lengths())
    over = sorted(path for path, count in current.items() if count > LIMIT)
    if "--update" in sys.argv[1:]:
        ALLOWLIST.write_text(json.dumps({"limit": LIMIT, "files": over}, indent=2) + "\n")
        print(f"check-file-length: {len(over)} files over {LIMIT} lines listed")
        return 0
    allowed = set(json.loads(ALLOWLIST.read_text())["files"])
    problems = [
        f"{path}: {current[path]} lines, over the {LIMIT}-line limit; split it"
        for path in over
        if path not in allowed
    ]
    problems += [
        f"{path}: within {LIMIT} lines or gone; remove it from {ALLOWLIST.name}"
        for path in sorted(allowed)
        if current.get(path, 0) <= LIMIT
    ]
    for problem in problems:
        print(f"check-file-length: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
