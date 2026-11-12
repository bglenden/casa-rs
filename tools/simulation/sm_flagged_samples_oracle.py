#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record independent CASA predictions of flagged existing-MS samples (#1053).

Uses the small array and image recipes from sm_multiple_models_oracle. Each
case reopens an MS with known DATA and independently chosen sample/row flags.
Raw mixed image/component results retain CASA's flagged-sample duplication.
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import shutil
import subprocess
import sys

import numpy as np

from sm_multiple_models_oracle import CENTRE, make_session, model

CASES = [
    {"name": machine + suffix, "machine": machine, "incremental": incremental}
    for machine in ("ft", "wproject", "mosaic", "sd")
    for suffix, incremental in (("_replace", False), ("_increment", True))
] + [
    {"name": kind + suffix, "kind": kind, "incremental": incremental}
    for kind in ("components", "mixed", "multiple")
    for suffix, incremental in (("_replace", False), ("_increment", True))
] + [
    {"name": "noise_only", "kind": "noise"},
    {"name": "image_noise", "noise": True},
    {"name": "image_increment_noise", "incremental": True, "noise": True},
]


def prepare_ms(path):
    from casatools import table

    sm = make_session(path)
    sm.close()
    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        for row in range(tb.nrows()):
            flags = np.zeros((2, 4), dtype=bool)
            if row % 8 == 2:
                flags[0, 0] = flags[1, 2] = True
            elif row % 8 == 3:
                flags[:] = True
            elif row % 8 == 5:
                flags[0, 1] = True
            for column, values in (
                ("FLAG", flags), ("FLAG_ROW", row % 8 in (4, 5)),
                ("DATA", np.full((2, 4), 3 + 4j, dtype=np.complex64)),
                ("MODEL_DATA", np.full((2, 4), 7 - 2j, dtype=np.complex64)),
                ("CORRECTED_DATA", np.full((2, 4), -5 + 6j, dtype=np.complex64)),
                ("WEIGHT", np.array([2, 3], dtype=np.float32)),
                ("SIGMA", np.array([0.5, 0.75], dtype=np.float32)),
            ):
                tb.putcell(column, row, values)
        tb.flush()
    finally:
        tb.close()


def read_rows(path):
    from casatools import table

    tb = table()
    tb.open(str(path))
    try:
        rows = []
        for row in range(tb.nrows()):
            record = {
                "antennas": [int(tb.getcell(c, row)) for c in ("ANTENNA1", "ANTENNA2")],
                "field": int(tb.getcell("FIELD_ID", row)),
                "flag_row": bool(tb.getcell("FLAG_ROW", row)),
                "flags": tb.getcell("FLAG", row).T.ravel().tolist(),
                "weight": tb.getcell("WEIGHT", row).tolist(),
                "sigma": tb.getcell("SIGMA", row).tolist(),
            }
            for column in ("DATA", "MODEL_DATA", "CORRECTED_DATA"):
                record[column.lower()] = [[float(v.real), float(v.imag)]
                                         for v in tb.getcell(column, row).T.ravel()]
            rows.append(record)
        return rows
    finally:
        tb.close()


def save(path, cases):
    import casatasks

    fixture = {"casa_version": casatasks.version_string(), "work_issue": 1053,
               "recipe": "tools/simulation/sm_flagged_samples_oracle.py", "cases": cases}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
        zipped.write((json.dumps(fixture, separators=(",", ":"), allow_nan=False) + "\n").encode())


def worker(spec, out, fixture):
    from casatools import componentlist, simulator

    images = {name: out / (name + ".image") for name in ("a", "b")}
    for name, path in images.items():
        model(path, name)
    component = out / "point.cl"
    cl = componentlist()
    assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad", flux=0.25, freq="230GHz")
    cl.rename(str(component))
    cl.done()
    path = out / "flagged.ms"
    prepare_ms(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    machine = spec.get("machine", "ft")
    assert sm.setoptions(ftmachine=machine, gridfunction="BOX" if machine == "sd" else "SF",
                         wprojplanes=16)
    kind = spec.get("kind", "image")
    if kind != "noise":
        arguments = {"incremental": spec.get("incremental", False)}
        if kind != "components":
            arguments["imagename"] = [str(images[n]) for n in (("a", "b") if kind == "multiple" else ("a",))]
        if kind in ("components", "mixed"):
            arguments["complist"] = str(component)
        assert sm.predict(**arguments)
    if kind == "noise" or spec.get("noise"):
        assert sm.setseed(617)
        assert sm.setnoise(mode="simplenoise", simplenoise="0.1Jy")
        assert sm.corrupt()
    case = {**spec, "rows": read_rows(path), "cleanup": "not returned"}
    save(fixture, [case])
    print(spec["name"], "rows", len(case["rows"]), flush=True)
    sm.close()
    case["cleanup"] = "returned"
    save(fixture, [case])


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", choices=[c["name"] for c in CASES])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError("Expected CASA 6.7.6.14")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    selected = [c for c in CASES if args.only is None or c["name"] == args.only]
    if args.worker:
        worker(selected[0], out, args.fixture)
        return
    cases = []
    for spec in selected:
        case_out = out / spec["name"]
        shutil.rmtree(case_out, ignore_errors=True)
        case_out.mkdir()
        child = case_out / "result.json.gz"
        log = out / (spec["name"] + ".log")
        with log.open("w") as handle:
            process = subprocess.run(
                [sys.executable, __file__, "--worker", "--only", spec["name"],
                 "--out", str(case_out), "--fixture", str(child)],
                stdout=handle, stderr=subprocess.STDOUT, check=False)
        if not child.exists():
            raise RuntimeError(f"{spec['name']}: no completed calculation; see {log}")
        with gzip.open(child, "rt") as handle:
            case = json.load(handle)["cases"][0]
        if process.returncode:
            known_abort = (spec.get("machine") == "mosaic" and case["cleanup"] == "not returned"
                           and process.returncode in (-6, 134, 255)
                           and "Pure virtual function called" in log.read_text())
            if not known_abort:
                raise RuntimeError(f"{spec['name']}: unexpected exit {process.returncode}; see {log}")
        elif case["cleanup"] != "returned":
            raise RuntimeError(f"{spec['name']}: cleanup did not finish")
        case["process_exit"] = process.returncode
        cases.append(case)
        save(args.fixture, cases)
        print(spec["name"], "rows", len(case["rows"]), "cleanup", case["cleanup"], flush=True)


if __name__ == "__main__":
    main()
