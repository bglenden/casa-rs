#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Refresh the imaging migration matrix's pinned baseline digests.

Recomputes every `baseline_manifest_digests` entry exactly as
`check-imaging-architecture.py` validates it, then the accepted registry digest
that covers them. Run it after editing any pinned file, and review the diff:
it only records the current content, it does not judge it.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
from pathlib import Path

CHECKER_PATH = Path(__file__).resolve().parent / "check-imaging-architecture.py"
ACCEPTED_CONSTANT = re.compile(
    r'(ACCEPTED_BASELINE_MANIFEST_DIGESTS_SHA256 = \(\n    ")([0-9a-f]{64})(")'
)


def load_checker():
    spec = importlib.util.spec_from_file_location("check_imaging_architecture", CHECKER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report stale digests without writing; exit 1 if any are stale",
    )
    args = parser.parse_args()

    checker = load_checker()
    policy = json.loads(checker.DEFAULT_POLICY.read_text(encoding="utf-8"))
    matrix_path = checker.REPO_ROOT / policy["migration_matrix"]
    matrix_text = matrix_path.read_text(encoding="utf-8")
    registry = json.loads(matrix_text)["baseline_manifest_digests"]

    stale = []
    for locator, pinned in registry.items():
        content = checker.baseline_manifest_content(locator, locator)
        current = hashlib.sha256(content).hexdigest()
        if current != pinned:
            stale.append(locator)
            matrix_text = matrix_text.replace(f'"{locator}": "{pinned}"', f'"{locator}": "{current}"', 1)
            registry[locator] = current

    checker_text = CHECKER_PATH.read_text(encoding="utf-8")
    match = ACCEPTED_CONSTANT.search(checker_text)
    if match is None:
        raise SystemExit("cannot find ACCEPTED_BASELINE_MANIFEST_DIGESTS_SHA256 in the checker")
    accepted = checker.stable_digest(registry)
    accepted_stale = match.group(2) != accepted

    for locator in stale:
        print(f"stale: {locator}")
    if accepted_stale:
        print("stale: ACCEPTED_BASELINE_MANIFEST_DIGESTS_SHA256")
    if not stale and not accepted_stale:
        print("baseline digests are current")
        return 0
    if args.check:
        return 1

    if json.loads(matrix_text)["baseline_manifest_digests"] != registry:
        raise SystemExit("matrix rewrite did not match the recomputed registry; nothing written")
    matrix_path.write_text(matrix_text, encoding="utf-8")
    CHECKER_PATH.write_text(
        ACCEPTED_CONSTANT.sub(lambda m: f"{m.group(1)}{accepted}{m.group(3)}", checker_text, count=1),
        encoding="utf-8",
    )
    print(f"refreshed {len(stale)} pinned digest(s) and the accepted registry digest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
