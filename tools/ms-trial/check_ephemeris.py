# SPDX-License-Identifier: LGPL-3.0-or-later
"""XRADIO side of the trial's ephemeris check (docs/measurement-set/trial/README.md,
"Ephemerides in MSv4"). Python here is only the oracle.

    check_ephemeris.py --ours RUST.ps.zarr --theirs XRADIO.ps.zarr

Opens the converter's processing set of the moving-field MSv2 with XRADIO,
runs XRADIO's schema check, and compares its `field_and_source_ephemeris`
with XRADIO's own conversion of the same MSv2 (`ephemeris_interpolate=False`):
the phase centre at every time, the samples both hold, and the coverage of
the last time's TDB (UTC + 69.184 s since 2017). Prints a JSON report and exits
non-zero on any failure.
"""

import argparse
import json
import sys

import numpy as np
from xradio.measurement_set import open_processing_set
from xradio.schema.check import check_datatree

TDB_MINUS_UTC_S = 69.184
SAMPLE_TIME_RESOLUTION_S = 1.0e-3


def fields(store):
    ps = open_processing_set(store)
    [node] = list(ps.children.values())
    return ps, node.xr_ms.get_field_and_source_xds()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ours", required=True)
    parser.add_argument("--theirs", required=True)
    args = parser.parse_args()
    checks = []

    def record(name, ok, detail=None):
        checks.append({"check": name, "ok": bool(ok), **({"detail": detail} if detail is not None else {})})

    ours_ps, ours = fields(args.ours)
    _, theirs = fields(args.theirs)
    issues = check_datatree(ours_ps)
    record("XRADIO schema check of the Rust store", len(issues) == 0, str(issues) if issues else None)
    record("dataset type", ours.attrs["type"] == "field_and_source_ephemeris", ours.attrs["type"])
    record(
        "observer is the Earth's centre",
        ours["OBSERVER_POSITION"].attrs["coordinate_system"] == "geocentric"
        and np.all(ours["OBSERVER_POSITION"].values == 0.0),
        ours["OBSERVER_POSITION"].values.tolist(),
    )
    for name in ["FIELD_PHASE_CENTER_DIRECTION", "FIELD_PHASE_CENTER_DISTANCE"]:
        a, b = ours[name].values, theirs[name].values
        scale = 1.0 if name.endswith("DIRECTION") else np.abs(b).max()
        worst = float(np.abs(a - b).max() / scale)
        record(f"{name} equals XRADIO's at every time", a.shape == b.shape and worst <= 1.0e-14, worst)
        record(f"{name} frame", ours[name].attrs["frame"] == theirs[name].attrs["frame"], ours[name].attrs["frame"])
    mine, theirs_t = ours.time_ephemeris.values, theirs.time_ephemeris.values
    pairs = [(i, int(np.argmin(np.abs(theirs_t - t)))) for i, t in enumerate(mine)]
    pairs = [(i, j) for i, j in pairs if abs(mine[i] - theirs_t[j]) <= SAMPLE_TIME_RESOLUTION_S]
    record("every XRADIO sample is one of ours", len(pairs) == theirs_t.size, {"ours": int(mine.size), "theirs": int(theirs_t.size), "common": len(pairs)})
    for name, tolerance in [("SOURCE_DIRECTION", 1.0e-15), ("SOURCE_DISTANCE", 1.0e-15), ("SOURCE_RADIAL_VELOCITY", 1.0e-13)]:
        a = np.array([ours[name].values[i] for i, _ in pairs])
        b = np.array([theirs[name].values[j] for _, j in pairs])
        scale = 1.0 if name == "SOURCE_DIRECTION" else np.abs(b).max()
        worst = float(np.abs(a - b).max() / scale)
        record(f"{name} equals XRADIO's at the samples both hold", worst <= tolerance, worst)
    last = float(ours.time.values.max())
    record("ours cover the last time's TDB", mine.max() >= last + TDB_MINUS_UTC_S, float(mine.max() - last))
    covered = theirs_t.max() >= last + TDB_MINUS_UTC_S
    checks.append({"check": "XRADIO's cover the last time's TDB (finding, not a failure)", "ok": True, "detail": {"covered": bool(covered), "reach_s": float(theirs_t.max() - last)}})
    failed = sum(not check["ok"] for check in checks)
    print(json.dumps({"checks": len(checks), "failed": failed, "results": checks}, indent=1))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
