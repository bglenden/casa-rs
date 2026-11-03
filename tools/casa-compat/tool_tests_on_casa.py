#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run casa-rs's tool tests against CASA's own tools, to re-probe their values (#1053).

The Python tests of `casars.tools` hold the values CASA returned for the
same calls, probed when each method was implemented. Run against CASA itself,
a test that passes shows its expected values are still CASA's, for the CASA
this runs under. The script puts a stand-in `casars` package first on the
path, whose `casars.tools.<tool>` is `casatools.<tool>`, and runs pytest on
the test files given. Tests of what only casa-rs has (its bundle, its
refusals, its loggers) fail or error under CASA by design: the receipt lists
every test's outcome, and the reference cites only the value tests that pass.

Run with CASA's Python, from the repository root:

    CASAPY -I tools/casa-compat/tool_tests_on_casa.py --tool quanta \\
        --out resources/casa-compat/evidence/qa-on-casa-20261103.json

The tree must be clean, so the revision names the tests that ran.
"""

from __future__ import annotations

import argparse
import datetime
import json
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TESTS = REPO / "crates" / "casars-python" / "python" / "tests"
STAND_IN = '''"""casatools standing in for casars.tools, to run casa-rs's tests on CASA."""
from casatools import componentlist, measures, quanta, simulator
'''
DATA = '''"""What only casa-rs has: the tests that use it fail under CASA."""
def schema_bundle():
    raise RuntimeError("casa-rs's object-surface bundle does not exist in CASA")
'''
OUTCOME = re.compile(r"^(PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS)\s+(\S+)")


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tool", required=True, choices=["quanta", "measures", "componentlist"])
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        print("tool-tests-on-casa: the tree has changes; commit them so the revision names what ran", file=sys.stderr)
        return 2
    import casatools

    with tempfile.TemporaryDirectory() as scratch:
        package = Path(scratch) / "casars"
        package.mkdir()
        (package / "__init__.py").write_text("from . import data, tools\n", encoding="utf-8")
        (package / "tools.py").write_text(STAND_IN, encoding="utf-8")
        (package / "data.py").write_text(DATA, encoding="utf-8")
        test = TESTS / f"test_tools_{args.tool}.py"
        env = {"PYTHONPATH": f"{scratch}:{TESTS}", "PATH": "/usr/bin:/bin", "HOME": str(Path.home())}
        pytest = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider"]
        collected = subprocess.run([*pytest, "-q", "--collect-only", str(test)], cwd=TESTS,
                                   capture_output=True, text=True, env=env)
        nodes = [line for line in collected.stdout.splitlines() if "::" in line]
        # One process a test: a call that crashes CASA ends only its own test.
        outcomes = {}
        for node in nodes:
            result = subprocess.run([*pytest, "-q", "-rA", node], cwd=TESTS, capture_output=True, text=True, env=env)
            found = [m.group(1).lower() for m in map(OUTCOME.match, result.stdout.splitlines()) if m]
            name = node.split("::", 1)[-1]
            if found:
                outcomes[name] = found[0]
            else:
                outcomes[name] = f"crashed (exit {result.returncode})"
    if not outcomes:
        print(collected.stdout[-3000:], collected.stderr[-3000:], file=sys.stderr)
        return 1
    counts: dict[str, int] = {}
    for outcome in outcomes.values():
        counts[outcome] = counts.get(outcome, 0) + 1
    receipt = {
        "schema": "casa-tool-tests-on-casa/1",
        "work_issue": 1053,
        "date": datetime.date.today().isoformat(),
        "tool": args.tool,
        "oracle": {"casatools": casatools.version_string()},
        "tests_revision": git("rev-parse", "HEAD"),
        "host": platform.platform(),
        "command": f"CASAPY -I tools/casa-compat/tool_tests_on_casa.py --tool {args.tool}",
        "test_file": str(test.relative_to(REPO)),
        "summary": counts,
        "tests": dict(sorted(outcomes.items())),
    }
    args.out.write_text(json.dumps(receipt, indent=1) + "\n", encoding="utf-8")
    print(f"tool-tests-on-casa: {counts} -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
