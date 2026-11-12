#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""C++ reads native frequency-calibrated MSs and consumes native B/Tf/Df tables."""
import argparse
import gzip
import json
from pathlib import Path
import numpy as np
import casatasks
from casatools import simulator, table
from sm_apply_frequency_oracle import GAIN_FACTORS, LEAKAGE_OFFSETS, make_second_window
from sm_apply_parameters_oracle import initialize, receptor_values
from sm_apply_parameters_readback import compare
from sm_multiple_models_oracle import make_session


def check_table(path, case):
    tool = table()
    tool.open(str(path))
    try:
        assert tool.nrows() == 16
        kind = case["kind"]
        receptors = 1 if kind == "Tf" else 2
        assert tool.getkeyword("VisCal") == tool.info()["subType"] == kind + " Jones"
        parameters, flags = tool.getcol("CPARAM"), tool.getcol("FLAG")
        assert parameters.shape == flags.shape == (receptors, 4, 16)
        first, last = (case["calibration"][key] for key in ("first_time_s", "last_time_s"))
        times, antennas, fields = (tool.getcol(key) for key in ("TIME", "ANTENNA1", "FIELD_ID"))
        assert set(times) == {first, last} and set(antennas) == set(range(8))
        np.testing.assert_array_equal(tool.getcol("SPECTRAL_WINDOW_ID"), np.zeros(16))
        np.testing.assert_array_equal(tool.getcol("INTERVAL"), np.zeros(16))
        for row, (time, antenna) in enumerate(zip(times, antennas)):
            assert fields[row] == int(time == last)
            base = np.asarray(receptor_values("D" if kind == "Df" else "G", int(antenna), float(time-first)), dtype=np.complex64)[:receptors].astype(np.complex128)
            expected = np.asarray([base + offset if kind == "Df" else base * factor
                                   for factor, offset in zip(GAIN_FACTORS, LEAKAGE_OFFSETS)], dtype=np.complex64).T
            np.testing.assert_array_equal(parameters[:, :, row], expected)
            expected_flags = np.zeros((receptors, 4), dtype=bool)
            if antenna == 0:
                if case["probe"].startswith("flag_channel"):
                    expected_flags[0, 1] = True
                elif case["probe"] == "all_flagged":
                    expected_flags[:] = True
                elif case["probe"] == "one_valid":
                    expected_flags[:] = True
                    expected_flags[:, 2] = False
            np.testing.assert_array_equal(flags[:, :, row], expected_flags)
    finally:
        tool.close()
    tool.open(str(path / "SPECTRAL_WINDOW"))
    try:
        np.testing.assert_array_equal(tool.getcell("CHAN_FREQ", 0), case["calibration"]["frequencies_hz"])
    finally:
        tool.close()


def consume(path, case, out):
    out.mkdir(parents=True)
    output = out / "casa.ms"
    factory = make_second_window if case.get("data_window") == 1 else make_session
    sm = factory(output, stokes="XX XY YX YY")
    sm.close()
    initialize(output)
    sm = simulator()
    assert sm.openfromms(str(output))
    options = {"calwt": False, "t": 0.0, "opacity": 0.0, **case["options"]}
    assert sm.setapply(table=str(path.resolve()), interp=case["interp"], **options)
    assert sm.corrupt()
    sm.close()
    return compare(output, case)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    with gzip.open(args.fixture, "rt") as handle:
        fixture = json.load(handle)
    assert fixture["casa_version"] == casatasks.version_string()
    cases = {case["name"]: case for case in fixture["cases"]}
    assert len(cases) == 75
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
            report["casa_consumes_native_table_nrms"] = consume(manifest.parent / "terms.cal", case, args.out / case["name"])
        reports.append(report)
    assert seen == expected, ("missing", sorted(expected-seen))
    (args.out / "report.json").write_text(json.dumps({"casa_version":casatasks.version_string(),"runs":reports},indent=1,allow_nan=False)+"\n")
    print("C++ readback:", len(reports), "native MS/table pairs;75 native tables consumed", flush=True)


if __name__ == "__main__":
    main()
