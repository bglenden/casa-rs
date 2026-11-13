#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Read native invalid-solution tables/MSs, retaining CASA's D defect (#1053)."""

import argparse
import gzip
import json
import pathlib

import numpy as np

from sm_apply_parameters_oracle import receptor_values
from sm_apply_parameters_readback import compare, consume


def check_table(path, case):
    from casatools import table

    tb = table()
    tb.open(str(path))
    try:
        count = 14 if case["probe"] == "missing_antenna" else 16
        assert tb.nrows() == count == case["calibration"]["rows"]
        assert tb.getkeyword("VisCal") == case["kind"] + " Jones"
        times, antennas = tb.getcol("TIME"), tb.getcol("ANTENNA1")
        fields = tb.getcol("FIELD_ID")
        values, flags = tb.getcol("CPARAM"), tb.getcol("FLAG")
        assert values.shape == flags.shape == (2, 1, count)
        first, last = (case["calibration"][key] for key in ("first_time_s", "last_time_s"))
        expected_antennas = set(range(7 if count == 14 else 8))
        assert set(times) == {first, last} and set(antennas) == expected_antennas
        np.testing.assert_array_equal(tb.getcol("INTERVAL"), np.zeros(count))
        for row, (time, antenna) in enumerate(zip(times, antennas)):
            assert fields[row] == int(time == last)
            expected = np.asarray(receptor_values(case["kind"], int(antenna), float(time - first)),
                                  dtype=np.complex64)
            np.testing.assert_array_equal(values[:, 0, row], expected)
            active = antenna == 0 and case["probe"].startswith("flag")
            active &= "first" not in case["probe"] or time == first
            active &= "last" not in case["probe"] or time == last
            receptors = [0, 1] if case["probe"] == "flag_all_both" else [int(case["probe"].endswith("y"))]
            np.testing.assert_array_equal(flags[:, 0, row],
                                          [bool(active and receptor in receptors) for receptor in (0, 1)])
    finally:
        tb.close()


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
    assert len(cases) == 24
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
            # Actual CASA consumption must reproduce its original defect,
            # independently of the corrected native output's acceptance.
            raw = {**case, "rows": case.get("casa_unmodified_rows", case["rows"])}
            report["casa_consumes_native_table_nrms"] = consume(
                manifest.parent / "terms.cal", raw, args.out / case["name"])
        reports.append(report)
    assert seen == expected, ("missing", sorted(expected - seen))
    (args.out / "report.json").write_text(json.dumps({"casa_version": casatasks.version_string(),
                                                   "runs": reports}, indent=1, allow_nan=False) + "\n")
    print("C++ readback:", len(reports), "MS/table pairs; 24 native tables consumed", flush=True)


if __name__ == "__main__":
    main()
