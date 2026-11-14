# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA C++ reads/reopens native finite-distance simulation outputs."""
import argparse
import gzip
import json
from pathlib import Path

import numpy as np
from casatasks import version_string
from casatools import simulator, table


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    fixture = json.load(gzip.open(args.fixture, "rt"))
    count = 0
    for case in fixture["cases"]:
        for prefix in ("w1", "w3", "metal3"):
            path = args.native / f"{prefix}-{case['name']}.ms"
            if prefix == "metal3" and case["spec"].get("machine") == "wproject":
                continue
            tb = table()
            tb.open(str(path))
            assert tb.nrows() == len(case["after"]["rows"]), path
            worst = 0.
            for row, reference in enumerate(case["after"]["rows"]):
                actual = tb.getcell("DATA", row).T.ravel()
                science = case.get("field_distance_corrected_rows", case["after"]["rows"])[row]
                expected = np.asarray(science["data"])
                expected = expected[:, 0] + 1j * expected[:, 1]
                error = np.linalg.norm(actual - expected) / max(np.linalg.norm(expected), 1.)
                assert error < 1e-3, (path, row, error)
                if case["spec"].get("selected") and not (reference["field"] == 0
                        and reference["antennas"][0] == 0 and reference["antennas"][1] > 2):
                    np.testing.assert_array_equal(actual, np.zeros_like(actual))
                worst = max(worst, float(error))
                for name in ("ANTENNA1", "ANTENNA2"):
                    assert int(tb.getcell(name, row)) == reference["antennas"][int(name[-1])-1]
                for column, key in (("FIELD_ID", "field"), ("FLAG_ROW", "flag_row"),
                                    ("TIME", "time"), ("DATA_DESC_ID", "data_desc_id"),
                                    ("WEIGHT", "weight"), ("SIGMA", "sigma")):
                    np.testing.assert_array_equal(tb.getcell(column, row), reference[key])
                np.testing.assert_array_equal(tb.getcell("FLAG", row).T.ravel(), reference["flags"])
                np.testing.assert_allclose(tb.getcell("UVW", row), reference["uvw"], rtol=0., atol=1e-3)
            tb.close()
            sm = simulator()
            assert sm.openfromms(str(path)), path
            assert sm.close()
            count += 1
            print(path.name, "CASA C++ reads/reopens, worst row NRMS", worst, flush=True)
    print("CASA C++ read/reopened", count, "native MSs")


if __name__ == "__main__":
    main()
