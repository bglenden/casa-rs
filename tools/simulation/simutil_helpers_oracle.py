#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's small `simutil` helpers on recorded inputs, for casa-rs's binding (#1053).

Run with CASA 6.7.6.14's Python:

    CASAPY -I tools/simulation/simutil_helpers_oracle.py \\
        --inputs crates/casa-simulation/tests/data/skymodel_inputs.tar.gz \\
        --out crates/casars-python/python/tests/data/casa_simutil_helpers.json

It records what CASA's `simutil` returns, logs or raises for:
- `is4d` and `spectral` on every image of the sky-model inputs;
- `dir_s2m` on direction strings with and without a frame, and a list;
- `msg` at each priority, with and without `verbose` and an origin;
- `isreport`, `openreport` and `closereport` around a report file;
- `setcfg` against a stand-in simulator that records the call.

Each log line is `[priority, origin, message]` as CASA's log file has it.
"""

from __future__ import annotations

import argparse
import json
import os
import tarfile
import tempfile

import numpy as np

import casatools
from casatasks import casalog
from casatasks.private.simutil import simutil

DIRECTIONS = [
    "J2000 12h00m00 -30d00m00",
    "12h00m00 -30d00m00",
    "B1950 10 20",
    "GALACTIC 10deg -5.5deg",
    "J2000 23h59m59.9 +89d59m59",
    ["J2000 0h00m00 0d00m00", "J2000 1h00m00 10d00m00"],
]
PRIORITIES = [None, "info", "INFO", "warn", "WARNING", "debug", "DEBUG2", "other", "error"]
TELESCOPES = ["ALMA", "WSRT", "askap", "DRAO", "LOFAR", "lwa", "MOST", "VLA"]


def plain(value):
    """`value` as JSON: arrays as `{"ndarray", "dtype"}`, NumPy scalars as
    `{"scalar", "dtype"}`, so a test can check the types CASA returns."""
    if isinstance(value, np.ndarray):
        return {"ndarray": value.tolist(), "dtype": str(value.dtype)}
    if isinstance(value, np.generic):
        return {"scalar": value.item(), "dtype": str(value.dtype)}
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return {"tuple": [plain(item) for item in value]}
    if isinstance(value, list):
        return [plain(item) for item in value]
    return value


class LogTail:
    """The lines CASA's logger writes from here on."""

    def __init__(self, path: str):
        self.path = path
        casalog.setlogfile(path)
        self.seen = self.read_all()

    def read_all(self) -> list[list[str]]:
        casalog.post("", priority="INFO")  # flush
        with open(self.path, encoding="utf-8", errors="replace") as handle:
            lines = []
            for line in handle:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 4:
                    lines.append([parts[1], parts[2].strip(), "\t".join(parts[3:])])
            return lines

    def new(self) -> list[list[str]]:
        lines = self.read_all()
        found = [line for line in lines[len(self.seen):] if line[2] != ""]
        self.seen = lines
        return found


def call(log: LogTail, function, *args, **kwargs) -> dict:
    """What `function` returns or raises, and what it logs."""
    try:
        result = {"value": plain(function(*args, **kwargs))}
    except Exception as error:  # noqa: BLE001 - recording CASA's refusal
        result = {"raises": type(error).__name__, "message": str(error)}
    result["log"] = log.new()
    return result


class RecordingSimulator:
    """A stand-in for `sm` whose `setconfig` records its arguments."""

    def __init__(self, accept: bool):
        self.accept = accept
        self.calls = []

    def setconfig(self, **kwargs):
        self.calls.append(["setconfig", plain(kwargs)])
        return self.accept

    def close(self):
        self.calls.append(["close"])
        return True


def images(inputs: str) -> dict:
    return {name: os.path.join(inputs, name) for name in sorted(os.listdir(inputs))
            if name.endswith((".im", ".fits"))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", required=True, help="skymodel_inputs.tar.gz")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    work = tempfile.mkdtemp(prefix="simutil-helpers-")
    with tarfile.open(args.inputs) as archive:
        archive.extractall(work, filter="data")
    inputs = os.path.join(work, "inputs")
    log = LogTail(os.path.join(work, "casa.log"))
    record = {"casa_version": casatools.version_string(),
              "generator": "tools/simulation/simutil_helpers_oracle.py",
              "is4d": {}, "spectral": {}}
    u = simutil()
    for name, path in images(inputs).items():
        record["is4d"][name] = call(log, u.is4d, path)
        record["spectral"][name] = call(log, u.spectral, path)
    record["dir_s2m"] = []
    for verbose in (False, True):
        u = simutil(verbose=verbose)
        for direction in DIRECTIONS:
            record["dir_s2m"].append({"verbose": verbose, "direction": direction,
                                      "result": call(log, u.dir_s2m, direction)})
    u = simutil(direction="J2000 3h00m00 -20d00m00")
    record["dir_s2m"].append({"verbose": False, "direction": None, "self_direction": u.direction,
                              "result": call(log, u.dir_s2m)})
    record["msg"] = []
    for verbose in (False, True):
        u = simutil(verbose=verbose)
        for priority in PRIORITIES:
            for origin in (None, "simobserve"):
                record["msg"].append({
                    "verbose": verbose, "priority": priority, "origin": origin,
                    "result": call(log, u.msg, "a message with WARNING and ERROR in it",
                                   origin=origin, priority=priority)})
    record["report"] = report_cases(log, work)
    record["setcfg"] = setcfg_cases(log)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=1)
        handle.write("\n")


def report_cases(log: LogTail, work: str) -> list:
    """The report helpers in the order the tasks use them, and the file."""
    path = os.path.join(work, "project.report.txt")
    steps = []
    u = simutil(verbose=True)
    steps.append(["isreport, no reportfile", call(log, u.isreport)])
    steps.append(["openreport, no reportfile", call(log, u.openreport)])
    u.reportfile = path
    steps.append(["isreport, closed", call(log, u.isreport)])
    steps.append(["openreport", call(log, u.openreport)])
    steps.append(["isreport, open", call(log, u.isreport)])
    steps.append(["msg to the report", call(log, u.msg, "first line", origin="simanalyze")])
    steps.append(["msg without origin", call(log, u.msg, "second line")])
    steps.append(["closereport", call(log, u.closereport)])
    steps.append(["isreport, after closereport", call(log, u.isreport)])
    steps.append(["msg after closereport", call(log, u.msg, "third line")])
    steps.append(["openreport, file exists", call(log, u.openreport)])
    steps.append(["msg, reopened", call(log, u.msg, "fourth line", origin="simalma")])
    steps.append(["closereport again", call(log, u.closereport)])
    quiet = simutil()
    quiet.reportfile = path
    quiet.openreport()
    steps.append(["msg, not verbose", call(log, quiet.msg, "not in the report")])
    quiet.closereport()
    fresh = simutil()
    steps.append(["closereport, never opened", call(log, fresh.closereport)])
    with open(path, encoding="utf-8") as handle:
        contents = handle.read()
    return {"steps": steps, "report": contents}


def setcfg_cases(log: LogTail) -> list:
    x, y, z = [1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]
    diam = np.array([12.0, 12.0, 7.0])
    pads = ["A001", "A002", "A003"]
    me = casatools.measures()
    posobs = me.observatory("ALMA")
    cases = []
    for telescope in TELESCOPES:
        for mounttype in (None, "X-Y"):
            sm = RecordingSimulator(accept=True)
            result = call(log, simutil().setcfg, sm, telescope, x, y, z, diam, pads, posobs, mounttype)
            cases.append({"telescope": telescope, "mounttype": mounttype, "result": result, "calls": sm.calls})
    sm = RecordingSimulator(accept=False)
    result = call(log, simutil().setcfg, sm, "ALMA", x, y, z, diam, pads, posobs)
    cases.append({"telescope": "ALMA", "mounttype": None, "refused": True, "result": result, "calls": sm.calls})
    sm = RecordingSimulator(accept=True)
    result = call(log, simutil().setcfg, sm, "ALMA", x, y, z, [12.0, 12.0, 7.0], pads, posobs)
    cases.append({"telescope": "ALMA", "mounttype": None, "diam_list": True, "result": result,
                  "calls": sm.calls})
    return cases


if __name__ == "__main__":
    main()
