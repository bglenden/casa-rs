#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Read native MSs and consume native G/D tables with CASA C++ (#1053)."""

import argparse
import gzip
import json
import pathlib

import numpy as np

from sm_apply_parameters_oracle import initialize, receptor_values
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session


def complex_values(values):
    values = np.asarray(values)
    return values[..., 0] + 1j * values[..., 1]


def compare(path, case):
    from casatools import ms, table

    tool = ms()
    assert tool.open(str(path))
    tool.close()
    rows = read_rows(path)
    assert len(rows) == len(case["rows"])
    actual = complex_values([row["data"] for row in rows])
    expected = complex_values([row["data"] for row in case["rows"]])
    assert actual.shape == expected.shape == (144, 16) and np.isfinite(actual).all()
    for row, reference in zip(rows, case["rows"]):
        for key in ("antennas", "field", "flag_row", "flags", "weight", "sigma", "model_data"):
            assert row[key] == reference[key], (case["name"], key)
        assert row["corrected_data"] == row["data"]
    tb = table()
    tb.open(str(path))
    try:
        np.testing.assert_array_equal(tb.getcol("TIME"), [row["time_s"] for row in case["rows"]])
    finally:
        tb.close()
    nrms = float(np.sqrt(np.sum(np.abs(actual - expected) ** 2) / np.sum(np.abs(expected) ** 2)))
    assert nrms <= 1e-3, (case["name"], nrms)
    return nrms


def check_table(path, case):
    from casatools import table

    tb = table()
    tb.open(str(path))
    try:
        assert tb.nrows() == 16
        assert tb.getkeyword("VisCal") == case["kind"] + " Jones"
        times, antennas = tb.getcol("TIME"), tb.getcol("ANTENNA1")
        fields = tb.getcol("FIELD_ID")
        parameters = tb.getcol("CPARAM")
        assert parameters.shape == (2, 1, 16)
        first, last = case["calibration"]["first_time_s"], case["calibration"]["last_time_s"]
        assert set(times) == {first, last} and set(antennas) == set(range(8))
        np.testing.assert_array_equal(tb.getcol("INTERVAL"), np.zeros(16))
        for row, (time, antenna) in enumerate(zip(times, antennas)):
            assert fields[row] == int(time == last)
            expected = np.asarray(receptor_values(case["kind"], int(antenna), float(time - first)),
                                  dtype=np.complex64)
            np.testing.assert_array_equal(parameters[:, 0, row], expected)
    finally:
        tb.close()


def consume(path, case, out):
    from casatools import simulator

    out.mkdir(parents=True)
    output = out / "casa.ms"
    sm = make_session(output, stokes="XX XY YX YY")
    sm.close()
    initialize(output)
    sm = simulator()
    assert sm.openfromms(str(output))
    parameters = {"calwt": False, "t": 0.0, "opacity": 0.0, **case["options"]}
    assert sm.setapply(table=str(path.resolve()), interp=case["interp"], **parameters)
    assert sm.corrupt()
    sm.close()
    return compare(output, case)


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    with gzip.open(args.fixture, "rt") as handle:
        fixture = json.load(handle)
    assert fixture["casa_version"] == casatasks.version_string()
    cases = {case["name"]: case for case in fixture["cases"]}
    assert len(cases) == 28
    expected = {(name, workers) for name in cases for workers in (1, 3)}
    seen, reports = set(), []
    args.out.mkdir(parents=True, exist_ok=True)
    for manifest in sorted(args.root.glob("*/case.json")):
        run = json.loads(manifest.read_text())
        key = run["name"], run["workers"]
        assert key in expected and key not in seen, key
        seen.add(key)
        case = cases[run["name"]]
        nrms = compare(manifest.parent / "applied.ms", case)
        check_table(manifest.parent / "terms.cal", case)
        report = {**run, "native_ms_cpp_nrms": nrms}
        if run["workers"] == 1:
            report["casa_consumes_native_table_nrms"] = consume(
                manifest.parent / "terms.cal", case, args.out / case["name"])
        reports.append(report)
    assert seen == expected, ("missing", sorted(expected - seen))
    (args.out / "report.json").write_text(json.dumps({"casa_version": casatasks.version_string(),
                                                   "runs": reports}, indent=1, allow_nan=False) + "\n")
    print("C++ readback:", len(reports), "native MS/table outputs passed;28 native tables consumed",
          flush=True)


if __name__ == "__main__":
    main()
