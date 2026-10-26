# SPDX-License-Identifier: LGPL-3.0-or-later
"""XRADIO side of the trial converter's measurements
(docs/measurement-set/trial/README.md, "Converter"). Python here is only the
oracle.

    check_convert.py xradio-convert --ms MS --out XRADIO.ps.zarr
        Time XRADIO's own conversion of an MSv2 (one dataset per partition,
        no pointing), for comparison with `ms_trial_convert to-msv4`.

    check_convert.py compare --ours RUST.ps.zarr --theirs XRADIO.ps.zarr
        Open the converter's processing set with XRADIO, run XRADIO's schema
        check, and compare every dataset with XRADIO's conversion of the
        same MSv2: datasets matched by spectral window, polarization, fields
        and first time; every payload variable and coordinate exactly.

Each prints a JSON report and exits non-zero on any mismatch.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
from xradio.measurement_set import open_processing_set
from xradio.schema.check import check_datatree

VARIABLES = ["VISIBILITY", "FLAG", "WEIGHT", "UVW", "TIME_CENTROID", "EFFECTIVE_INTEGRATION_TIME"]
COORDINATES = [
    "time",
    "baseline_antenna1_name",
    "baseline_antenna2_name",
    "frequency",
    "polarization",
    "field_name",
    "scan_name",
]


def xradio_convert(ms, out):
    from xradio.measurement_set import convert_msv2_to_processing_set

    started = time.perf_counter()
    convert_msv2_to_processing_set(
        in_file=str(ms),
        out_file=str(out),
        partition_scheme=[],
        with_pointing=False,
        persistence_mode="w",
    )
    seconds = time.perf_counter() - started
    print(json.dumps({"xradio_convert_seconds": seconds, "datasets": len(open_processing_set(str(out)).children)}))
    return True


def key(node):
    ds = node.ds
    return (
        ds.frequency.attrs["spectral_window_name"],
        tuple(ds.polarization.values),
        tuple(np.unique(ds.field_name.values)),
        float(ds.time.values[0]),
    )


def compare(ours, theirs):
    results = []

    def record(check, ok, detail=None):
        results.append({"check": check, "ok": bool(ok), **({"detail": detail} if detail else {})})

    mine = open_processing_set(str(ours))
    reference = open_processing_set(str(theirs))
    issues = check_datatree(mine)
    record("XRADIO schema check", len(issues) == 0, {"issues": str(issues)[:2000]} if issues else None)
    by_key = {}
    for name in sorted(reference.children):
        by_key.setdefault(key(reference[name]), []).append(name)
    record("XRADIO's datasets are told apart by the key", all(len(v) == 1 for v in by_key.values()))
    record("the same number of datasets", len(mine.children) == len(reference.children),
           {"ours": len(mine.children), "theirs": len(reference.children)})
    mismatches = []
    for name in sorted(mine.children):
        node = mine[name]
        match = by_key.get(key(node))
        if not match:
            mismatches.append({"dataset": name, "problem": "no XRADIO dataset with its key"})
            continue
        ds = node.ds.load()
        expected = reference[match[0]].ds.load()
        for coord in COORDINATES:
            if not np.array_equal(ds[coord].values, expected[coord].values):
                mismatches.append({"dataset": name, "coordinate": coord})
        for var in VARIABLES:
            a, b = ds[var].values, expected[var].values
            same = a.shape == b.shape and np.array_equal(a, b, equal_nan=np.issubdtype(a.dtype, np.inexact))
            if not same or ds[var].dims != expected[var].dims or ds[var].dtype != expected[var].dtype:
                mismatches.append({"dataset": name, "variable": var})
    record("every dataset equals XRADIO's, variable for variable", not mismatches, {"mismatches": mismatches[:20]} if mismatches else None)
    print(json.dumps({"datasets": len(mine.children), "results": results}, indent=1))
    return all(r["ok"] for r in results)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    convert = sub.add_parser("xradio-convert")
    convert.add_argument("--ms", type=Path, required=True)
    convert.add_argument("--out", type=Path, required=True)
    comparison = sub.add_parser("compare")
    comparison.add_argument("--ours", type=Path, required=True)
    comparison.add_argument("--theirs", type=Path, required=True)
    args = parser.parse_args()
    ok = xradio_convert(args.ms, args.out) if args.mode == "xradio-convert" else compare(args.ours, args.theirs)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
