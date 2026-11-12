#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA frequency-dependent table controls with discriminating complex spectra (#1053)."""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import subprocess
import sys

import numpy as np

import sm_apply_parameters_oracle as parameters

GAIN_FACTORS = (1 + 0j, 1.1 + 0.7j, -0.6 + 1.5j, -1.4 - 0.3j)
LEAKAGE_OFFSETS = (0j, 0.06 + 0.04j, -0.16 + 0.10j, 0.24 - 0.08j)
CASES = [{"name": f"{kind}_{interp}_{probe}", "kind": kind, "interp": interp,
          "options": {}, "probe": probe}
         for kind in ("B", "Tf", "Df") for interp in ("nearest", "linear")
         for probe in ("control", "flag_channel")]
CASES += [{"name": f"{kind}_{interp.replace(',', '_')}_shifted", "kind": kind,
           "interp": interp, "options": {}, "probe": "shifted"}
          for kind in ("B", "Tf", "Df")
          for interp in ("nearest,nearest", "nearest,linear", "linear,linear")]


CASES += [{"name": f"{kind}_{interp.replace(',', '_')}_{probe}", "kind": kind,
           "interp": interp, "options": {}, "probe": probe}
          for kind in ("B", "Tf", "Df")
          for interp, probe in [
              ("nearest,cubic", "shifted"), ("linear,cubic", "shifted"),
              ("nearest,spline", "shifted"), ("linear,spline", "shifted"),
              ("nearest,nearest", "descending_shifted"),
              ("nearest,linear", "descending_shifted"),
              ("nearest,cubic", "descending_shifted"),
              ("nearest,spline", "descending_shifted"),
              ("nearest,nearest", "tie_shifted"),
              ("nearest,linear", "tie_shifted"),
              ("nearest,unrecognized", "shifted"),
          ]]
CASES += [{"name": f"{kind}_nearest_{mode}_{probe}", "kind": kind,
           "interp": "nearest," + mode, "options": {}, "probe": probe}
          for kind in ("B", "Tf")
          for mode, probe in [
              ("nearestflag", "flag_channel_shifted"),
              ("linearflag", "flag_channel_shifted"),
              ("cubicflag", "all_flagged"),
              ("splineflag", "all_flagged"),
              ("linear", "one_valid"), ("nearest", "one_valid"),
          ]]


def calibration(sm, path, kind, bounds, probe):
    from casatools import table

    info = parameters.calibration(sm, path, "D" if kind == "Df" else "G", bounds)
    tb = table()
    tb.open(str(path), nomodify=False)
    receptors = 1 if kind == "Tf" else 2
    try:
        for column in ("CPARAM", "FLAG", "PARAMERR", "SNR", "WEIGHT"):
            if column not in tb.colnames() or not tb.iscelldefined(column, 0):
                continue
            values = np.repeat(tb.getcol(column)[:receptors, :, :], 4, axis=1)
            if column == "CPARAM":
                # Explicit double intermediate followed by table Complex32,
                # keeping the recipe independent of a vector-library's promotion rules.
                base = values.astype(np.complex128)
                for channel in range(4):
                    values[:, channel, :] = (base[:, channel, :] + LEAKAGE_OFFSETS[channel]
                                             if kind == "Df" else base[:, channel, :] * GAIN_FACTORS[channel])
            if column == "FLAG":
                selected = tb.getcol("ANTENNA1") == 0
                if probe.startswith("flag_channel"):
                    values[0, 1, selected] = True
                elif probe == "all_flagged":
                    values[:, :, selected] = True
                elif probe == "one_valid":
                    values[:, :, selected] = True
                    values[:, 2, selected] = False
            tb.putcol(column, values)
        tb.putkeyword("VisCal", kind + " Jones")
        information = tb.info()
        information["subType"] = kind + " Jones"
        tb.putinfo(information)
    finally:
        tb.close()
    source, destination = table(), table()
    source.open(str(path.parent / "applied.ms" / "SPECTRAL_WINDOW"))
    destination.open(str(path / "SPECTRAL_WINDOW"), nomodify=False)
    try:
        for row in range(destination.nrows()):
            for column in destination.colnames():
                if column in source.colnames() and source.iscelldefined(column, row):
                    destination.putcell(column, row, source.getcell(column, row))
        frequencies = destination.getcol("CHAN_FREQ")
        if "shifted" in probe:
            frequencies += 7_812_500.0 if probe == "tie_shifted" else 250_000.0
        if probe == "descending_shifted":
            frequencies = frequencies[::-1, :]
        destination.putcol("CHAN_FREQ", frequencies)
        frequencies = destination.getcell("CHAN_FREQ", 0).tolist()
    finally:
        source.close()
        destination.close()
    return {**info, "channels": 4, "parameters": receptors, "frequencies_hz": frequencies}


def save(path, cases):
    import casatasks

    record = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/sm_apply_frequency_oracle.py", "cases": cases}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as stream:
        stream.write((json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n").encode())


def worker(spec, out, fixture):
    original = parameters.calibration
    def build(sm, path, kind, bounds):
        parameters.calibration = original
        try:
            return calibration(sm, path, kind, bounds, spec["probe"])
        finally:
            parameters.calibration = build
    parameters.calibration = build
    parameters.worker(spec, out, fixture)


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", choices=[case["name"] for case in CASES])
    parser.add_argument("--resume", action="store_true", help="Reuse existing successful per-case CASA results")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    args.out.mkdir(parents=True, exist_ok=True)
    selected = [case for case in CASES if not args.only or case["name"] == args.only]
    if args.worker:
        assert len(selected) == 1
        worker(selected[0], args.out, args.fixture)
        return
    results = []
    for spec in selected:
        out = args.out / spec["name"]
        out.mkdir(exist_ok=args.resume)
        fixture = out / "result.json.gz"
        if not (args.resume and fixture.is_file()):
            with (out / "worker.log").open("w") as log:
                subprocess.run([sys.executable, __file__, "--out", str(out), "--fixture", str(fixture),
                                "--only", spec["name"], "--worker"], stdout=log, stderr=subprocess.STDOUT,
                               check=True)
        with gzip.open(fixture, "rt") as handle:
            results.extend(json.load(handle)["cases"])
        save(args.fixture, results)
        print(spec["name"], "passed", flush=True)


if __name__ == "__main__":
    main()
