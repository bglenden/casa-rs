#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA reads native MSs after supplied-table simulation corruption (SIM-16).

Use CASA 6.7.6.14 Python, --root matching the two native export directories,
and --fixtures pointing to crates/casa-simulation/tests/data.
"""
import argparse
import gzip
import json
from pathlib import Path

import numpy as np
from casatasks import version_string
from casatools import ms, table

from sm_flagged_samples_oracle import read_rows


def compare(path, case):
    tool = ms()
    assert tool.open(str(path))
    tool.close()
    actual = read_rows(path)
    expected = case["rows"]
    assert len(actual) == len(expected)
    for row, reference in zip(actual, expected):
        for key in ("antennas", "field", "flag_row", "flags", "weight", "sigma", "model_data"):
            assert row[key] == reference[key], (case["name"], key)
        assert row["corrected_data"] == row["data"]
    tb = table()
    tb.open(str(path))
    np.testing.assert_array_equal(tb.getcol("TIME"), [row["time_s"] for row in expected])
    tb.close()
    values = lambda rows: np.asarray([[complex(*value) for value in row["data"]] for row in rows])
    actual, expected = values(actual), values(expected)
    assert np.isfinite(actual).all()
    nrms = float(np.linalg.norm(actual - expected) / np.linalg.norm(expected))
    assert nrms <= 1e-3, (case["name"], nrms)
    return nrms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    cases = {}
    for name in ("polynomial", "baseline", "remaining_priors"):
        fixture = json.load(gzip.open(args.fixtures / f"sm_apply_{name}_casa.json.gz", "rt"))
        cases.update((case["name"], case) for case in fixture["cases"]
                     if case.get("outcome", "returned") == "returned")
    reports = []
    for manifest in sorted((args.root / "parameters").glob("*/case.json")):
        run = json.loads(manifest.read_text())
        reports.append(dict(**run, nrms=compare(manifest.parent / "applied.ms", cases[run["name"]])))
    for path in sorted((args.root / "polynomial").glob("*.ms")):
        reports.append(dict(name=path.stem, workers=1, nrms=compare(path, cases[path.stem])))
    assert {report["name"] for report in reports} == set(cases)
    (args.root / "cpp-readback.json").write_text(json.dumps(dict(casa_version=version_string(), runs=reports), indent=1) + "\n")
    print("C++ read", len(reports), "native MSs; maximum DATA NRMS", max(r["nrms"] for r in reports))


if __name__ == "__main__":
    main()
