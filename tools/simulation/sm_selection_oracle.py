#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 selection and ordered DATA operations (#1053).

Two fields, two otherwise identical spectral windows and four integrations
per window distinguish row selection from image/frequency differences.
Each case runs in its own CASA process and preserves every visibility and
scratch-column sample. No native output is used to construct the reference.
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import shutil
import subprocess
import sys
import time

import numpy as np

from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import CENTRE, make_session, model


def select(field=(), window=(), query="", returned=True):
    return {"op": "setdata", "fieldid": list(field), "spwid": list(window),
            "msselect": query, "expected_return": returned}


def predict(images=("a",), incremental=False, components=False):
    return {"op": "predict", "images": list(images), "incremental": incremental,
            "components": components}


GAINS = [{"op": "fixedgain", "receptor_gain": 2.0}, {"op": "corrupt"}]
NOISE = [{"op": "setseed", "seed": 617},
         {"op": "setnoise", "mode": "simplenoise", "simplenoise": "0.1Jy"},
         {"op": "corrupt"}]
CASES = [
    {"name": "all_replace", "steps": [predict()]},
    {"name": "all_increment", "steps": [predict(incremental=True)]},
    {"name": "field_replace", "steps": [select(field=[1]), predict()]},
    {"name": "field_increment", "steps": [select(field=[1]), predict(incremental=True)]},
    {"name": "window_replace", "steps": [select(window=[1]), predict()]},
    {"name": "intersection_replace", "steps": [select(field=[1], window=[1]), predict()]},
    {"name": "invalid_field_fallback", "steps": [select(field=[99]), predict()]},
    {"name": "invalid_window_keeps_field", "steps": [select(field=[1], window=[99]), predict()]},
    {"name": "empty_query_keeps_field", "steps": [select(field=[1], query="ANTENNA1 < 0"), predict()]},
    {"name": "invalid_query_keeps_field", "steps": [select(field=[1], query="NOT A VALID QUERY", returned=False), predict()]},
    {"name": "cross_correlations", "steps": [select(query="ANTENNA1 != ANTENNA2"), predict()]},
    {"name": "within_scan_times", "steps": [select(query="TIME >= MID_TIME"), predict()]},
    {"name": "selection_restarts_from_original", "steps": [select(field=[1]), predict(), select(field=[0]), predict(["b"])]},
    {"name": "replacement_preserves_other_rows", "steps": [predict(), select(field=[1]), predict(["b"])]},
    {"name": "disjoint_replacements", "steps": [select(field=[0]), predict(), select(field=[1]), predict(["b"])]},
    {"name": "increment_subset", "steps": [predict(), select(field=[1]), predict(["b"], incremental=True)]},
    {"name": "reset_selection", "steps": [select(field=[1]), predict(), select(), predict(["b"])]},
    {"name": "selected_components", "steps": [select(field=[1], window=[1]), predict([], components=True)]},
    {"name": "selected_multiple_images", "steps": [select(field=[1]), predict(["a", "b"])]},
    {"name": "selected_image_then_components", "steps": [predict(), select(field=[1]), predict([], components=True)]},
    {"name": "selected_components_then_image", "steps": [predict([], components=True), select(field=[1]), predict()]},
    {"name": "selected_noise", "noise": True, "steps": [select(field=[1], window=[1]), *NOISE]},
    {"name": "selected_prediction_noise", "noise": True, "steps": [predict(), select(field=[1], window=[1]), *NOISE]},
    {"name": "replacement_after_noise", "steps": [predict(), select(field=[1]), *NOISE, predict(["b"])]},
    {"name": "fresh_subset", "fresh": True, "steps": [select(field=[1]), predict()]},
    {"name": "observe_resets_selection", "fresh": True, "steps": [select(field=[1]), predict(),
     {"op": "observe", "sourcename": "target", "spwname": "band6b", "starttime": "43s", "stoptime": "63s"},
     predict(["b"], incremental=True)]},
    {"name": "absolute_cross_correlations", "steps": [select(query="abs(ANTENNA1 - ANTENNA2) > 0"), predict()]},
    {"name": "near_integration", "steps": [select(query="near(TIME, MID_TIME, 1e-13)"), predict()]},
    {"name": "nearabs_integration", "steps": [select(query="nearabs(TIME, MID_TIME, 0.001)"), predict()]},
    {"name": "numeric_predicate_keeps_field", "steps": [select(field=[1], query="1", returned=False), predict()]},
    {"name": "lowercase_column_keeps_field", "steps": [select(field=[1], query="antenna1 != antenna2", returned=False), predict()]},
    {"name": "true_predicate_keeps_field", "steps": [select(field=[1], query="TRUE"), predict()]},
    {"name": "query_replacement_overlaps_fields", "steps": [select(query="ANTENNA1 != ANTENNA2"), predict(), select(field=[1]), predict(["b"])]},
    {"name": "query_increment_preserves_other_rows", "steps": [predict(), select(query="ANTENNA1 != ANTENNA2"), predict(["b"], incremental=True)]},
    {"name": "observe_resets_row_query", "fresh": True, "steps": [select(query="ANTENNA1 != ANTENNA2"), predict(),
     {"op": "observe", "sourcename": "target", "spwname": "band6b", "starttime": "43s", "stoptime": "63s"},
     predict(["b"], incremental=True)]},
    {"name": "query_noise", "noise": True, "steps": [select(query="ANTENNA1 != ANTENNA2"), *NOISE]},
    {"name": "gains_then_increment", "steps": [predict(), *GAINS, predict(["b"], incremental=True)]},
    {"name": "selected_gains_then_increment", "steps": [predict(), select(field=[1]), *GAINS, predict(["b"], incremental=True)]},
    {"name": "query_gains_then_increment", "steps": [predict(), select(query="ANTENNA1 != ANTENNA2"), *GAINS, predict(["b"], incremental=True)]},
    {"name": "gains_then_subset_replacement", "steps": [predict(), *GAINS, select(field=[1]), predict(["b"])]},
    {"name": "fractional_integer_division", "steps": [select(query="ANTENNA1/2>0"), predict()]},
    {"name": "negative_floor_division", "steps": [select(query="-ANTENNA1//2 == -1"), predict()]},
    {"name": "negative_integer_modulo", "steps": [select(query="-ANTENNA1%3 == 2"), predict()]},
    {"name": "negative_divisor_modulo", "steps": [select(query="ANTENNA1%-3 == -1"), predict()]},
    {"name": "negative_float_modulo", "steps": [select(query="(-ANTENNA1*1.0)%3.0 == 2.0"), predict()]},
    {"name": "boolean_arithmetic_keeps_field", "steps": [select(field=[1], query="TRUE+TRUE>0", returned=False), predict()]},
    {"name": "mixed_boolean_arithmetic_keeps_field", "steps": [select(field=[1], query="TRUE+1>0", returned=False), predict()]},
    {"name": "short_circuit_type_error_keeps_field", "steps": [select(field=[1], query="TRUE OR (TRUE+TRUE>0)", returned=False), predict()]},
    {"name": "division_zero_is_ieee", "steps": [select(query="ANTENNA1/0>0"), predict()]},
    {"name": "division_zero_nan_ordering", "steps": [select(query="ANTENNA1/ANTENNA1 >= 0"), predict()]},
    {"name": "integer_power_is_real", "steps": [select(query="(ANTENNA1+2)**24 > 1e18"), predict()]},
]


def observed(path):
    sm = make_session(path)
    assert sm.setspwindow(spwname="band6b", freq="230GHz", deltafreq="15.625MHz",
                          freqresolution="15.625MHz", refcode="TOPO", nchannels=4, stokes="XX YY")
    assert sm.observe(sourcename="target", spwname="band6b", starttime="3s", stoptime="23s")
    assert sm.observe(sourcename="offset", spwname="band6b", starttime="23s", stoptime="43s")
    return sm


def initialize(path):
    from casatools import table

    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        for row in range(tb.nrows()):
            for column, values in (("DATA", 3 + 4j), ("MODEL_DATA", 7 - 2j), ("CORRECTED_DATA", -5 + 6j)):
                tb.putcell(column, row, np.full((2, 4), values, dtype=np.complex64))
            tb.putcell("FLAG", row, np.zeros((2, 4), dtype=bool))
            tb.putcell("FLAG_ROW", row, False)
        return float(np.min(tb.getcol("TIME")) + 10.0)
    finally:
        tb.close()


def save(path, cases):
    import casatasks

    fixture = {"casa_version": casatasks.version_string(), "work_issue": 1053,
               "recipe": "tools/simulation/sm_selection_oracle.py", "cases": cases}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
        zipped.write((json.dumps(fixture, separators=(",", ":"), allow_nan=False) + "\n").encode())


def worker(spec, out, fixture):
    from casatools import componentlist, simulator, table

    images = {name: out / (name + ".image") for name in ("a", "b")}
    for name, path in images.items():
        model(path, name)
    component = out / "point.cl"
    cl = componentlist()
    assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad", flux=0.25, freq="230GHz")
    cl.rename(str(component))
    cl.done()
    path = out / "selected.ms"
    sm = observed(path)
    if not spec.get("fresh"):
        sm.close()
        mid_time = initialize(path)
        sm = simulator()
        assert sm.openfromms(str(path))
    else:
        mid_time = None
    calls = []
    calculation_started = time.perf_counter()
    for step in spec["steps"]:
        args = dict(step)
        operation = args.pop("op")
        expected = args.pop("expected_return", True)
        if operation == "fixedgain":
            calibration = out / "fixed.gcal"
            assert sm.setgain(mode="fbm", table=str(calibration), amplitude=[0.0, 0.0])
            tb = table()
            tb.open(str(calibration), nomodify=False)
            try:
                values = tb.getcol("CPARAM")
                tb.putcol("CPARAM", np.full_like(values, args["receptor_gain"]))
            finally:
                tb.close()
            sm.reset()
            assert sm.setapply(table=str(calibration), interp="nearest", calwt=False)
            calls.append({**step, "returned": True})
            continue
        if operation == "predict":
            names = args.pop("images")
            if names:
                args["imagename"] = [str(images[name]) for name in names]
            if args.pop("components"):
                args["complist"] = str(component)
        if operation == "setdata":
            args["msselect"] = args["msselect"].replace("MID_TIME", repr(mid_time))
        returned = getattr(sm, operation)(**args)
        calls.append({**step, "returned": returned})
        assert returned == expected, (step, returned)
    sm.close()
    calculation_seconds = time.perf_counter() - calculation_started
    rows = read_rows(path)
    tb = table()
    tb.open(str(path))
    try:
        for row, record in enumerate(rows):
            record.update(spw=int(tb.getcell("DATA_DESC_ID", row)),
                          time_s=float(tb.getcell("TIME", row)), scan=int(tb.getcell("SCAN_NUMBER", row)))
    finally:
        tb.close()
    save(fixture, [{**spec, "calls": calls, "rows": rows, "cleanup": "returned",
                    "calculation_seconds": calculation_seconds}])
    print(spec["name"], "rows", len(rows), flush=True)


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", nargs="+", choices=[case["name"] for case in CASES])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError("Expected CASA 6.7.6.14")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    selected = [case for case in CASES if not args.only or case["name"] in args.only]
    if args.worker:
        assert len(selected) == 1
        worker(selected[0], out, args.fixture)
        return
    results = []
    for case in selected:
        case_out = out / case["name"]
        shutil.rmtree(case_out, ignore_errors=True)
        case_out.mkdir()
        child_fixture = case_out / "result.json.gz"
        with (out / (case["name"] + ".log")).open("w") as log:
            result = subprocess.run([sys.executable, __file__, "--out", str(case_out),
                                     "--fixture", str(child_fixture), "--only", case["name"], "--worker"],
                                    stdout=log, stderr=subprocess.STDOUT, check=False)
        if result.returncode:
            raise RuntimeError(f"{case['name']}: exit {result.returncode}; see {out / (case['name'] + '.log')}")
        with gzip.open(child_fixture, "rt") as handle:
            results.extend(json.load(handle)["cases"])
        save(args.fixture, results)
        print(case["name"], "rows", len(results[-1]["rows"]), flush=True)


if __name__ == "__main__":
    main()
