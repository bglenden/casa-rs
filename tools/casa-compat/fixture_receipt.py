#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run casa-rs's comparisons with checked-in CASA fixtures and write a receipt (#1053).

A fixture records what CASA returned for a set of inputs (an oracle script
under `tools/` made it, and it names its CASA version). The tests that read it
compare casa-rs with CASA without a CASA install. Running them at an exact,
clean revision, and recording which passed, gives the reviewed reference
(`resources/casa-compat/semantics/`) a receipt for a `passed` evidence item:
the CASA version, the casa-rs revision, the command and this file.

    python3 tools/casa-compat/fixture_receipt.py \\
        --title "simutil against CASA's simutil" --work-issue 1053 \\
        --rust casa-simulation:simutil_casa \\
        --fixture crates/casa-simulation/tests/data/simutil_casa.json \\
        --out resources/casa-compat/evidence/simutil-fixtures-20261103.json

`--pytest` runs Python test files with an interpreter that has `casars`
installed (`--python`). The tree must be clean, so the revision names the
code that ran.
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import hashlib
import json
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
# nextest's per-test result lines: `PASS [ 0.1s] (1/2) crate::binary test_name`.
NEXTEST = re.compile(r"^\s*(PASS|FAIL|SIGSEGV|SIGABRT|TIMEOUT)\s+\[.*?\]\s+(?:\(\s*\d+/\d+\)\s+)?(\S+)\s+(\S+)\s*$")
# pytest -rA summary lines: `PASSED path::test`.
PYTEST = re.compile(r"^(PASSED|FAILED|ERROR|SKIPPED)\s+(\S+)")


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout.strip()


def fixture_record(path: str) -> dict[str, Any]:
    """A fixture's digest and the CASA version it records."""
    file = REPO / path
    raw = file.read_bytes()
    record: dict[str, Any] = {"path": path, "sha256": hashlib.sha256(raw).hexdigest()}
    text = gzip.decompress(raw) if file.suffix == ".gz" else raw
    try:
        data = json.loads(text)
    except (ValueError, UnicodeDecodeError):
        return record
    if isinstance(data, dict):
        for key in ("casa_version", "casa"):
            if isinstance(data.get(key), str):
                record["casa_version"] = data[key]
        for key in ("generator",):
            if isinstance(data.get(key), str):
                record[key] = data[key]
    return record


def run(command: list[str], pattern: re.Pattern[str], passed: str) -> tuple[dict[str, str], int]:
    print("+", " ".join(command), file=sys.stderr)
    result = subprocess.run(command, cwd=REPO, capture_output=True, text=True)
    outcomes = {}
    for line in (result.stdout + result.stderr).splitlines():
        match = pattern.match(line)
        if match:
            groups = match.groups()
            name = " ".join(groups[1:])
            outcomes[name] = "passed" if groups[0] == passed else groups[0].lower()
    return outcomes, result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--title", required=True)
    parser.add_argument("--work-issue", type=int, required=True)
    parser.add_argument("--rust", action="append", default=[], metavar="CRATE:TEST",
                        help="a Rust integration-test binary to run with cargo nextest")
    parser.add_argument("--pytest", action="append", default=[], metavar="PATH", help="a Python test file")
    parser.add_argument("--python", default=sys.executable, help="the interpreter --pytest runs under")
    parser.add_argument("--fixture", action="append", default=[], required=True, help="a CASA fixture the tests read")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        print("fixture-receipt: the tree has changes; commit them so the revision names what ran", file=sys.stderr)
        return 2
    commands, tests, codes = [], {}, []
    for spec in args.rust:
        crate, binary = spec.split(":")
        command = ["cargo", "nextest", "run", "--no-fail-fast", "-p", crate, "--test", binary]
        outcomes, code = run(command, NEXTEST, "PASS")
        commands.append(" ".join(command))
        tests.update(outcomes)
        codes.append(code)
    for path in args.pytest:
        command = [args.python, "-m", "pytest", "-q", "-rA", "-p", "no:cacheprovider", path]
        outcomes, code = run(command, PYTEST, "PASSED")
        commands.append(" ".join(["python", *command[1:]]))
        tests.update(outcomes)
        codes.append(code)
    if not tests:
        print("fixture-receipt: no test results were read", file=sys.stderr)
        return 1
    counts: dict[str, int] = {}
    for outcome in tests.values():
        counts[outcome] = counts.get(outcome, 0) + 1
    receipt = {
        "schema": "casa-fixture-receipt/1",
        "title": args.title,
        "work_issue": args.work_issue,
        "date": datetime.date.today().isoformat(),
        "native_revision": git("rev-parse", "HEAD"),
        "host": platform.platform(),
        "fixtures": [fixture_record(path) for path in args.fixture],
        "commands": commands,
        "summary": counts,
        "all_passed": all(outcome in {"passed", "skipped"} for outcome in tests.values()) and not any(codes),
        "tests": dict(sorted(tests.items())),
    }
    args.out.write_text(json.dumps(receipt, indent=1) + "\n", encoding="utf-8")
    print(f"fixture-receipt: {counts} -> {args.out}", file=sys.stderr)
    return 0 if receipt["all_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
