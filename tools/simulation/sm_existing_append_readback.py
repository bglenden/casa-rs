#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA C++ reads native simulator MSs and compares actual CASA references."""
import argparse
import gzip
import json
from pathlib import Path

import numpy as np
from casatasks import version_string
from casatools import simulator, table

from sm_existing_append_oracle import snapshot
from sm_data_descriptions_oracle import description_metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    with gzip.open(args.fixture, "rt") as stream:
        fixture = json.load(stream)
    count = 0
    for case in fixture["cases"]:
        prefixes = ["w1", "w3"]
        if (args.native / f"metal3-{case['name']}.ms").exists():
            prefixes.append("metal3")
        for prefix in prefixes:
            path = args.native / f"{prefix}-{case['name']}.ms"
            actual = snapshot(path)
            expected = case["after"]
            if case.get("native_created"):
                tb = table()
                tb.open(str(path))
                assert "MODEL_DATA" not in tb.colnames() and "CORRECTED_DATA" not in tb.colnames(), path
                tb.close()
            if "mapping" in case:
                tb = table()
                tb.open(str(path / "DATA_DESCRIPTION"))
                mapping = tb.getcol("SPECTRAL_WINDOW_ID").tolist()
                tb.close()
                assert mapping == case["mapping"], path
                assert description_metadata(path) == case["description_metadata"], path
                before = len(case["before"]["rows"])
            if "frequencies_hz" in case:
                tb = table()
                tb.open(str(path / "SPECTRAL_WINDOW"))
                assert np.array_equal(tb.getcell("CHAN_FREQ", 0), case["frequencies_hz"]), path
                tb.close()
            assert len(actual["rows"]) == len(expected["rows"]), path
            worst = 0.
            for row, (observed, reference) in enumerate(zip(actual["rows"], expected["rows"])):
                for name, value in reference.items():
                    if case.get("native_created") and name in ("model_data", "corrected_data"):
                        continue
                    if "pointing_cache_corrected_rows" in case and name in ("data", "corrected_data", "model_data"):
                        value = case["pointing_cache_corrected_rows"][row][name]
                    if "direction_frame_corrected_rows" in case and name in ("data", "corrected_data", "model_data"):
                        value = case["direction_frame_corrected_rows"][row][name]
                    if "polarization_map_corrected_rows" in case and name in ("data", "corrected_data", "model_data"):
                        value = case["polarization_map_corrected_rows"][row][name]
                    if (case.get("native_single_dish") and reference["field"] == 0
                            and reference["antennas"][0] != reference["antennas"][1]
                            and name in ("data", "corrected_data")):
                        # The documented SDGrid uvw-sign defect: native
                        # total power belongs to autocorrelations only.
                        value = np.zeros_like(value)
                        assert np.array_equal(observed[name], value), (path, row, name)
                    if ("mapping" in case and case["operation"] == "append"
                            and row >= before and name == "data_desc_id"):
                        # CASA labels appended rows with the physical SPW ID,
                        # even when it means another window or is out of range.
                        # Check the corrected label against the input DD map.
                        assert 0 <= observed[name] < len(mapping), (path, row)
                        assert mapping[observed[name]] == case["requested_spw"], (path, row)
                        value = case["native_append_description"]
                    if (case.get("native_reversed_gain") and reference["data_desc_id"] == 2
                            and reference["field"] == 0 and name in ("data", "corrected_data")):
                        # Analytic diagonal Jones correction for reversed
                        # CORR_PRODUCT: each channel's receptor pairs reverse.
                        value = np.asarray(value).reshape(-1, 4, 2)[:, ::-1, :].reshape(-1, 2)
                    left, right = np.asarray(observed[name]), np.asarray(value)
                    assert left.shape == right.shape, (path, row, name)
                    if name in ("data", "model_data", "corrected_data"):
                        error = np.linalg.norm(left - right) / max(np.linalg.norm(right), 1.)
                        worst = max(worst, float(error))
                        assert error < 1e-3, (path, row, name, error)
                    elif name == "uvw":
                        assert np.max(np.abs(left - right)) < 1e-3, (path, row, name)
                    else:
                        assert np.array_equal(left, right), (path, row, name)
            for name, reference in expected["metadata"].items():
                observed = actual["metadata"][name]
                assert observed["rows"] == reference["rows"], (path, name)
                for column, value in reference["columns"].items():
                    left, right = np.asarray(observed["columns"][column]), np.asarray(value)
                    if left.dtype.kind == "f":
                        limit = 1e-9 if column in ("DIRECTION", "TARGET") else 1e-6
                        assert np.allclose(left, right, rtol=0., atol=limit), (path, name, column)
                    else:
                        assert np.array_equal(left, right), (path, name, column)
            print(f"CASA C++ reads {path.name}: DATA NRMS <= {worst:.6g}, quality and metadata agree")
            sm = simulator()
            assert sm.openfromms(str(path)), path
            sm.close()
            count += 1
    print(f"CASA C++ read {count} native simulator MeasurementSets")
    empty = args.native / "empty.ms"
    if empty.exists():
        tb = table()
        tb.open(str(empty))
        assert tb.nrows() == 0 and "DATA" in tb.colnames(), empty
        tb.close()
        for name, rows in (("ANTENNA", 1), ("SPECTRAL_WINDOW", 2), ("DATA_DESCRIPTION", 2)):
            tb.open(str(empty/name))
            assert tb.nrows() == rows, (empty, name)
            tb.close()
        sm = simulator()
        assert sm.openfromms(str(empty))
        sm.close()
        print("CASA C++ reads/reopens empty mixed-shape MS")


if __name__ == "__main__":
    main()
