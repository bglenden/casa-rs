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

CASES += [{"name": f"{kind}_spw_{name}", "kind": kind, "interp": "linear,linear",
           "options": {"spwmap": mapping}, "probe": "spw_mapping", "data_window": 1}
          for kind in ("B", "Tf", "Df")
          for name, mapping in (("broadcast", [0]), ("explicit", [0, 0]), ("fanout", [-999]))]

CASES += [{"name": "Tf_spline_analytic", "kind": "Tf", "interp": "nearest,spline",
           "options": {}, "probe": "spline_analytic"}]

REFUSALS = [{"name": f"{kind}_spw_{name}_refusal", "kind": kind, "interp": "linear,linear",
             "options": {"spwmap": mapping}, "probe": "spw_mapping", "data_window": 1,
             "stage": "setapply", "refusal": True}
            for kind in ("B", "Tf", "Df")
            for name, mapping in (("negative", [-2]), ("outside", [2]), ("mixed_negative", [0, -1]), ("too_long", [0, 0, 0]))]
REFUSALS += [{"name": f"{kind}_{mode}_three_valid_refusal", "kind": kind,
              "interp": "nearest," + mode, "options": {}, "probe": "flag_channel_shifted",
              "stage": "corrupt", "refusal": True}
             for kind in ("B", "Tf") for mode in ("cubic", "cubicflag")]


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
                if probe in ("spline_analytic", "spline_sampled"):
                    amplitudes = (1.0, 2.0, 0.5, 1.25) if probe == "spline_analytic" else (1.80625, 1.26875, 0.5875, 1.25)
                    for channel, amplitude in enumerate(amplitudes):
                        values[:, channel, :] = amplitude
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
        if probe == "spw_mapping":
            tb.putcol("SPECTRAL_WINDOW_ID", np.zeros(tb.nrows(), dtype=np.int32))
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
        elif probe == "spline_analytic":
            frequencies -= 7_812_500.0
        if probe == "descending_shifted":
            frequencies = frequencies[::-1, :]
        destination.putcol("CHAN_FREQ", frequencies)
        frequencies = destination.getcell("CHAN_FREQ", 0).tolist()
    finally:
        source.close()
        destination.close()
    return {**info, "channels": 4, "parameters": receptors, "frequencies_hz": frequencies}


def save(path, cases, refusals=()):
    import casatasks

    record = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/sm_apply_frequency_oracle.py", "cases": cases, "refusals": refusals}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as stream:
        stream.write((json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n").encode())



ORIGINAL_SESSION = parameters.make_session

def make_second_window(path, stokes):
    from casatools import simulator, table
    sm = ORIGINAL_SESSION(path, stokes=stokes)
    sm.close()
    for subtable, column in (("SPECTRAL_WINDOW", "NAME"), ("DATA_DESCRIPTION", "SPECTRAL_WINDOW_ID")):
        tb = table()
        tb.open(str(path / subtable), nomodify=False)
        record = {name: tb.getcell(name, 0) for name in tb.colnames() if tb.iscelldefined(name, 0)}
        tb.addrows(1)
        for name, value in record.items():
            tb.putcell(name, 1, value)
        tb.putcell(column, 1, "second" if column == "NAME" else 1)
        tb.close()
    tb = table()
    tb.open(str(path), nomodify=False)
    tb.putcol("DATA_DESC_ID", np.ones(tb.nrows(), dtype=np.int32))
    tb.close()
    sm = simulator()
    assert sm.openfromms(str(path))
    return sm


def worker(spec, out, fixture):
    original = parameters.calibration
    if spec.get("data_window") == 1:
        parameters.make_session = make_second_window
    info = {}
    def build(sm, path, kind, bounds):
        parameters.calibration = original
        try:
            result = calibration(sm, path, kind, bounds, spec["probe"])
            info.update(result)
            return result
        finally:
            parameters.calibration = build
    parameters.calibration = build
    if not spec.get("refusal"):
        parameters.worker(spec, out, fixture)
        return
    try:
        parameters.worker(spec, out, fixture)
    except RuntimeError as error:
        from sm_flagged_samples_oracle import read_rows
        rows = read_rows(out / "applied.ms")
        assert rows and all(value == [3.0, 4.0] for row in rows for value in row["data"])
        save(fixture, [], [{**spec, "calibration": info, "outcome": "raises",
                            "exception_class": type(error).__name__, "error": str(error),
                            "unchanged_data": True}])
        print(spec["name"], "expected RuntimeError; DATA unchanged", flush=True)
    else:
        raise AssertionError("Expected CASA RuntimeError for " + spec["name"])


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", choices=[case["name"] for case in CASES + REFUSALS])
    parser.add_argument("--resume", action="store_true", help="Reuse existing successful per-case CASA results")
    parser.add_argument("--sampled", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    args.out.mkdir(parents=True, exist_ok=True)
    selected = [case for case in CASES + REFUSALS if not args.only or case["name"] == args.only]
    if args.worker:
        assert len(selected) == 1
        spec = dict(selected[0])
        if args.sampled:
            assert spec["probe"] == "spline_analytic"
            spec.update(probe="spline_sampled", interp="nearest,nearest")
        worker(spec, args.out, args.fixture)
        return
    results, refusals = [], []
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
            record = json.load(handle)
        if spec["probe"] == "spline_analytic":
            control = args.out / (spec["name"] + "_sampled")
            control.mkdir(exist_ok=args.resume)
            control_fixture = control / "result.json.gz"
            if not (args.resume and control_fixture.is_file()):
                with (control / "worker.log").open("w") as log:
                    subprocess.run([sys.executable, __file__, "--out", str(control), "--fixture", str(control_fixture),
                                    "--only", spec["name"], "--worker", "--sampled"], stdout=log, stderr=subprocess.STDOUT, check=True)
            with gzip.open(control_fixture, "rt") as handle:
                reference = json.load(handle)["cases"][0]["rows"]
            case = record["cases"][0]
            case["casa_unmodified_rows"] = case["rows"]
            case["rows"] = reference
            case["exception"] = "Natural spline back-substitution includes the first interior knot; see docs/CASA (C++) bugs.md."
            # The independent closed-form natural-spline solution has
            # second derivatives (-4.9,4.6) at its interior unit-spaced knots.
            # Its half-knot amplitudes are 289/160,203/160,47/80.
            for row in reference:
                for channel, amplitude in enumerate((289/160,203/160,47/80,5/4)):
                    expected = (3+4j)*np.float32(amplitude)**2
                    actual = complex(*row["data"][channel*4])
                    assert abs(actual-expected) <= 2e-6*abs(expected)
            assert any(a["data"] != b["data"] for a,b in zip(reference,case["casa_unmodified_rows"]))
        results.extend(record["cases"])
        refusals.extend(record.get("refusals", []))
        save(args.fixture, results, refusals)
        print(spec["name"], "passed", flush=True)


if __name__ == "__main__":
    main()
