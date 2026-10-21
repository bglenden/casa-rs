#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulation Phase 1 acceptance (#841): CASA `sm` scripts run unchanged.

Each script in this directory is a CASA user's `sm` script with CASA's imports
between two marker lines. This runner runs every script twice:

1. with CASA's Python, as written, in `<out>/casa/`;
2. with a Python that has `casars` installed, with only the import block
   replaced (`casatools` -> `casars.tools`, `casatasks.private.simutil` ->
   `casars.simutil`), in `<out>/casars/`;

then writes `<out>/casa/manifest.json` and compares every MeasurementSet pair
with `compare_sm_ms.py` under CASA's Python (`<out>/report.json`).

**TIME.** CASA's simulator stores TAI in a TIME column labelled UTC, so its
rows lie TAI - UTC (37 s) after the requested times; casa-rs stores the
requested UTC (plan decision 5). To compare rows at equal stored TIME, the
casa-rs run requests every scan 37 s later, as
`crates/casa-simulation/examples/observe_scenarios.rs` does: before running a
script the runner replaces `casars.tools.simulator` by a delegating wrapper
(below) whose `observe` adds `--time-shift-s` to `starttime` and `stoptime`.
The reference time is unchanged, so the hour-angle offset and SOURCE.TIME are
CASA's. The script text is not touched apart from its imports.

    python3 tools/simulation/sm_scripts/run_sm_scripts.py --out /tmp/sm-acceptance \\
        --casars-python <venv>/bin/python

The exit status is 1 when any MeasurementSet differs (beyond
`compare_sm_ms.py`'s recorded known differences) or a script fails.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import runpy
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
COMPARE = os.path.join(os.path.dirname(HERE), "compare_sm_ms.py")
CASA_PYTHON = "/Applications/CASA.app/Contents/MacOS/python3"
BEGIN = "# --- imports: the only lines that differ between CASA and casa-rs ---"
END = "# --- end imports ---"
CASARS_IMPORTS = "from casars import simutil\nfrom casars.tools import measures, quanta, simulator\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="output directory (replaced)")
    parser.add_argument("--casa-python", default=CASA_PYTHON, help="CASA's Python")
    parser.add_argument("--casars-python", default=sys.executable, help="a Python with casars installed")
    parser.add_argument("--time-shift-s", type=float, default=37.0, help="TAI - UTC added to casa-rs scans")
    parser.add_argument("--only", nargs="*", help="run only these scripts (file names)")
    parser.add_argument("--engine", choices=("casa", "casars"), help=argparse.SUPPRESS)
    parser.add_argument("--script", help=argparse.SUPPRESS)
    return parser.parse_args()


def scripts(only: list[str] | None) -> list[str]:
    found = sorted(glob.glob(os.path.join(HERE, "*_sm.py")))
    return [path for path in found if not only or os.path.basename(path) in only]


def casars_text(text: str) -> str:
    """The script with its import block replaced by casa-rs's."""
    start, end = text.index(BEGIN), text.index(END)
    return text[: start + len(BEGIN)] + "\n" + CASARS_IMPORTS + text[end:]


def shift_casars_scans(shift_s: float) -> None:
    """Make `casars.tools.simulator` request every scan `shift_s` later."""
    import casars.tools
    from casars.tools import quanta

    real = casars.tools.simulator
    qa = quanta()

    def later(time: object) -> str:
        seconds = qa.convert(qa.quantity(time), "s")["value"]
        return f"{seconds + shift_s!r}s"

    class simulator:  # noqa: N801 - CASA's class name
        """casars' simulator, with each scan `shift_s` later (TAI labelled UTC)."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            self._sm = real(*args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(self._sm, name)

        def observe(self, sourcename="", spwname="", starttime=[], stoptime=[], *args, **kwargs):  # noqa: B006 - CASA's defaults
            return self._sm.observe(sourcename, spwname, later(starttime), later(stoptime), *args, **kwargs)

    casars.tools.simulator = simulator


def run_one(engine: str, script: str, shift_s: float) -> None:
    """Run one script in the current directory (the child process)."""
    with open(script, encoding="utf-8") as handle:
        text = handle.read()
    if engine == "casars":
        shift_casars_scans(shift_s)
        text = casars_text(text)
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as handle:
        handle.write(text)
        swapped = handle.name
    try:
        runpy.run_path(swapped, run_name="__main__")
    finally:
        os.unlink(swapped)


def main() -> int:
    args = parse_args()
    if args.engine:
        run_one(args.engine, args.script, args.time_shift_s)
        return 0
    out = os.path.abspath(args.out)
    shutil.rmtree(out, ignore_errors=True)
    failed = False
    for engine, python in (("casa", args.casa_python), ("casars", args.casars_python)):
        workdir = os.path.join(out, engine)
        os.makedirs(workdir)
        for script in scripts(args.only):
            command = [python, "-I", os.path.abspath(__file__), "--out", out, "--engine", engine,
                       "--script", script, "--time-shift-s", str(args.time_shift_s)]
            print(f"== {engine}: {os.path.basename(script)}", flush=True)
            if subprocess.run(command, cwd=workdir, check=False).returncode != 0:
                print(f"   {engine} run of {os.path.basename(script)} failed")
                failed = True
    casa_dir, casars_dir = os.path.join(out, "casa"), os.path.join(out, "casars")
    scenarios = [
        {"name": os.path.basename(ms)[: -len(".ms")], "ms": ms, "ignore": []}
        for ms in sorted(glob.glob(os.path.join(casa_dir, "*.ms")))
    ]
    manifest = {
        "schema_version": 1,
        "purpose": "simulation Phase 1 acceptance: CASA sm scripts run with only their imports changed (#841)",
        "casa_tai_minus_utc_s": args.time_shift_s,
        "scenarios": scenarios,
    }
    with open(os.path.join(casa_dir, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    report = os.path.join(out, "report.json")
    compare = [args.casa_python, "-I", COMPARE, "--casa", casa_dir, "--casars", casars_dir, "--json", report,
               "--verbose"]
    print("== compare", flush=True)
    failed |= subprocess.run(compare, check=False).returncode != 0
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
