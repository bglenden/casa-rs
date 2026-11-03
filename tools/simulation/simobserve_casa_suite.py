#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run CASA's own `simobserve` test suite against casa-rs's task (#1053).

Run with CASA 6.7.6.14's Python (not `-I`: the suite's plugin is found on
`PYTHONPATH`), from the repository root:

    CASAPY tools/simulation/simobserve_casa_suite.py \\
        --test-file CASA6/casatasks/tests/tasks/test_task_simobserve.py \\
        --casatestdata /Volumes/home/casatestdata \\
        --casars-bin TARGET/release/simobserve --out OUT

CASA's `test_task_simobserve.py` checks each product's image and
MeasurementSet statistics against reference products in casatestdata, and
each refusal's message. The script runs every test case twice, each in its
own process, so a crash ends only its own test:
- **casa**, with CASA's `simobserve`: the control, since a case that fails
  in CASA itself says nothing about casa-rs;
- **casa-rs**, with `casatasks.simobserve` replaced before the suite imports
  it by a stand-in that runs casa-rs's task binary (`--json-run`) and raises
  CASA's message when the task refuses.

As in `simobserve_parity.py`, the stand-in asks for an hour angle later by
TAI − UTC at the reference date: CASA stores TAI under a UTC label and
casa-rs the requested UTC (simulation plan decision 5), so the two observe
the same sky at equal stored TIME. The receipt (`OUT/receipt.json`) records
each test's outcome under both, with the revision and CASA's version.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUTCOME = re.compile(r"^(PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS)\s+")

# The plugin each test process loads (`-p simobserve_suite_plugin`). It sets
# CASA's data path and, for casa-rs, replaces casatasks.simobserve.
PLUGIN = r'''
import json, os, subprocess

from casatools import ctsys

ctsys.setpath(ctsys.getpath() + [os.environ["SIMOBSERVE_SUITE_DATA"]])

if os.environ.get("SIMOBSERVE_SUITE_BINARY"):
    import casatasks
    from casatools import measures, quanta

    _me, _qa = measures(), quanta()

    def _hour_angle_s(hourangle):
        if hourangle in ("", "transit"):
            return 0.0
        if _qa.isquantity(hourangle):
            seconds = _qa.convert(hourangle, "s")
            if _qa.compare(seconds, "s"):
                return seconds["value"]
        return 0.0

    def _tai_minus_utc_s(refdate):
        day = refdate.split("/")[:3]
        epoch = _me.epoch("utc", "/".join(day) + "/00:00:00")
        tai = _me.measure(epoch, "tai")
        return (tai["m0"]["value"] - epoch["m0"]["value"]) * 86400.0

    def simobserve(**kwargs):
        """casa-rs's simobserve task binary in place of CASA's."""
        values = dict(kwargs)
        refdate = values.get("refdate", "2014/01/01")
        offset = _hour_angle_s(values.get("hourangle", "transit")) + _tai_minus_utc_s(refdate)
        values["hourangle"] = f"{offset!r}s"
        request = os.path.join(os.getcwd(), ".casars-simobserve-request.json")
        with open(request, "w", encoding="utf-8") as handle:
            json.dump({"surface": "simobserve", "values": values}, handle, default=str)
        result = subprocess.run([os.environ["SIMOBSERVE_SUITE_BINARY"], "--json-run", request],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        os.remove(request)
        try:
            reply = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise RuntimeError(result.stderr.strip() or f"exit status {result.returncode}")
        if reply.get("status") != "succeeded":
            raise RuntimeError(reply.get("message") or json.dumps(reply))

    casatasks.simobserve = simobserve
'''


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout.strip()


def run_suite(test_file: Path, env: dict, work: Path, only: list[str] | None) -> dict[str, str]:
    """Each test of `test_file`, in its own process: its outcome."""
    pytest = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-p", "simobserve_suite_plugin",
              "--rootdir", str(test_file.parent), "-c", "/dev/null"]
    collected = subprocess.run([*pytest, "-q", "--collect-only", str(test_file)], cwd=work,
                               capture_output=True, text=True, env=env)
    nodes = [f"{test_file}::{line.split('::', 1)[1]}" for line in collected.stdout.splitlines() if "::" in line]
    if only:
        nodes = [node for node in nodes if any(name in node for name in only)]
    if not nodes:
        raise SystemExit(f"no tests collected:\n{collected.stdout[-2000:]}\n{collected.stderr[-2000:]}")
    outcomes = {}
    for node in nodes:
        result = subprocess.run([*pytest, "-q", "-rA", node], cwd=work, capture_output=True, text=True, env=env)
        summary = result.stdout.split("short test summary info")[-1] if "short test summary info" in result.stdout else ""
        found = [m.group(1).lower() for m in map(OUTCOME.match, summary.splitlines()) if m]
        name = node.split("::", 1)[1]
        outcomes[name] = found[0] if found else f"crashed (exit {result.returncode})"
        if outcomes[name] != "passed":
            reasons = [line for line in result.stdout.splitlines() if line.startswith("E ")][:3]
            outcomes[name] += (": " + " | ".join(line[2:].strip() for line in reasons))[:400] if reasons else ""
        print(f"{name}: {outcomes[name][:160]}", file=sys.stderr)
    return outcomes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--test-file", required=True, type=Path, help="CASA's test_task_simobserve.py")
    parser.add_argument("--casatestdata", required=True, help="a casatestdata clone")
    parser.add_argument("--casars-bin", required=True, help="casa-rs's simobserve task binary")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--only", nargs="*", help="run only tests whose names contain these")
    parser.add_argument("--skip-casa", action="store_true", help="run only casa-rs")
    args = parser.parse_args()
    import casatasks
    import casatools

    args.out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as scratch:
        (Path(scratch) / "simobserve_suite_plugin.py").write_text(PLUGIN, encoding="utf-8")
        base = {**os.environ, "PYTHONPATH": scratch, "SIMOBSERVE_SUITE_DATA": args.casatestdata}
        base.pop("SIMOBSERVE_SUITE_BINARY", None)
        runs = {}
        for implementation in ([] if args.skip_casa else ["casa"]) + ["casa-rs"]:
            env = dict(base)
            if implementation == "casa-rs":
                env["SIMOBSERVE_SUITE_BINARY"] = str(Path(args.casars_bin).resolve())
            work = args.out / implementation
            work.mkdir(parents=True, exist_ok=True)
            print(f"==> {implementation}", file=sys.stderr)
            runs[implementation] = run_suite(args.test_file.resolve(), env, work, args.only)
    tests = sorted(set().union(*runs.values()))
    counts = {impl: {} for impl in runs}
    for impl, outcomes in runs.items():
        for outcome in outcomes.values():
            key = outcome.split(":")[0]
            counts[impl][key] = counts[impl].get(key, 0) + 1
    receipt = {
        "schema": "simobserve-casa-suite-receipt/1",
        "work_issue": 1053,
        "date": datetime.date.today().isoformat(),
        "oracle": {"casatasks": casatasks.version_string(), "casatools": casatools.version_string()},
        "native_revision": git("rev-parse", "HEAD"),
        "host": platform.platform(),
        "test_file": "casatasks/tests/tasks/test_task_simobserve.py (CASA 6.7.6.14 source)",
        "command": "CASAPY tools/simulation/simobserve_casa_suite.py --test-file ... --casatestdata ... --casars-bin TARGET/release/simobserve --out OUT",
        "summary": counts,
        "tests": {name: {impl: runs[impl].get(name) for impl in runs} for name in tests},
    }
    (args.out / "receipt.json").write_text(json.dumps(receipt, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(counts), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
