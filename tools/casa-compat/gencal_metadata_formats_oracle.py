#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reopen explicit calibration prescriptions from both MS formats with CASA."""

import argparse
import json
from pathlib import Path
import tarfile

import casatasks as casa
import numpy as np

from calibration_gencal_specify_oracle import compare_tables, generate, product, with_second_window


TRIAL = (
    Path(__file__).resolve().parents[2]
    / "libs/casacore/casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz"
)
CASES = {
    "gain": [
        ("amp", {
            "antenna": "3,0", "spw": "1,0", "pol": "Y,X",
            "parameter": [0.8, 1.1, 1.2, 1.3, 0.9, 1.0, 1.2, 1.4],
        }),
        ("ph", {"antenna": "3", "spw": "1", "pol": "X", "parameter": [20.0]}),
    ],
    "sbd": [("sbd", {"antenna": "1", "spw": "1", "pol": "Y", "parameter": [2.0]})],
    "mbd": [("mbd", {"antenna": "2", "spw": "0", "pol": "X", "parameter": [3.0]})],
    "opacity": [("opac", {"antenna": "1", "spw": "1", "parameter": [0.02]})],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--msv2-only", action="store_true")
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir()
    ms_v2 = root / "MSv2" / "ms_trial.ms"
    ms_v2.parent.mkdir()
    with tarfile.open(TRIAL) as archive:
        archive.extractall(ms_v2.parent, filter="data")
    with_second_window(ms_v2)
    sources = {"MSv2": ms_v2}
    if not args.msv2_only:
        # The basename is part of the CASA product, irrespective of format.
        ms_v4 = root / "MSv4" / ms_v2.name
        ms_v4.parent.mkdir()
        converted = ms_v4.parent / "paired.ps.zarr"
        native.msconvert(vis=str(ms_v2), outputvis=str(converted), outformat="msv4", withpointing=False)
        converted.rename(ms_v4)
        sources["MSv4"] = ms_v4

    results = {}
    for case, stages in CASES.items():
        expected = root / f"{case}-CASA.cal"
        generate(casa, ms_v2, expected, stages)
        _, rows, _ = product(expected)
        observed_ids = sorted(zip(rows["SPECTRAL_WINDOW_ID"], rows["ANTENNA1"]))
        assert observed_ids == [(spw, antenna) for spw in range(2) for antenna in range(5)]
        assert not np.asarray(rows["FLAG"]).any()
        results[case] = {}
        for label, source in sources.items():
            candidate = root / f"{case}-{label}.cal"
            generate(native, source, candidate, stages)
            results[case][label] = {
                "rows": len(observed_ids),
                "parameter_nrms": compare_tables(candidate, expected),
            }
    if not args.msv2_only:
        cross = root / "gain-CASA-create-MSv4-append.cal"
        generate(casa, ms_v2, cross, CASES["gain"][:1])
        generate(native, sources["MSv4"], cross, CASES["gain"][1:])
        results["gain"]["CASA-create-MSv4-append"] = {
            "rows": 10,
            "parameter_nrms": compare_tables(cross, root / "gain-CASA.cal"),
        }
    print(json.dumps(results))


if __name__ == "__main__":
    main()
