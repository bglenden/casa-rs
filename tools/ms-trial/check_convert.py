# SPDX-License-Identifier: LGPL-3.0-or-later
"""XRADIO side of the trial converter's measurements
(docs/measurement-set/trial/README.md, "Converter"). Python here is only the
oracle.

    check_convert.py xradio-convert --ms MS --out XRADIO.ps.zarr
            [--partition-scheme KEY ...] [--with-pointing]
        Time XRADIO's own conversion of an MSv2 (one dataset per partition,
        no pointing unless asked, XRADIO's default chunks), for comparison
        with `ms_trial_convert to-msv4` or `msconvert`.

    check_convert.py compare --ours RUST.ps.zarr --theirs XRADIO.ps.zarr
            [--chunks]
        Open the converter's processing set with XRADIO, run XRADIO's schema
        check, and compare every dataset with XRADIO's conversion of the
        same MSv2: datasets matched by spectral window, polarization, fields
        and first time; every payload variable and coordinate exactly; and
        XRADIO's names: each dataset's own, its antennas', and its base
        group's fields' and sources'. Where XRADIO found no SOURCE row for a
        field in a window it names the source "Unknown" (or "Unknown_ID")
        and the converter may name it from the field; those are counted,
        not mismatches. Each dataset must hold pointing exactly when
        XRADIO's does; with --chunks, its payload variables must be chunked
        as XRADIO's are.

Each prints a JSON report and exits non-zero on any mismatch.
"""

import argparse
import json
import re
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


def xradio_convert(ms, out, partition_scheme=(), with_pointing=False):
    from xradio.measurement_set import convert_msv2_to_processing_set

    started = time.perf_counter()
    convert_msv2_to_processing_set(
        in_file=str(ms),
        out_file=str(out),
        partition_scheme=list(partition_scheme),
        with_pointing=with_pointing,
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


def names(node):
    """The names a dataset carries: its antennas, and its base group's fields
    and sources."""
    field_and_source = node.xr_ms.get_field_and_source_xds()
    return {
        "antenna_name": node["antenna_xds"].ds.antenna_name.values,
        "field_name": field_and_source.field_name.values,
        "source_name": field_and_source.source_name.values,
    }


def compare_names(name, node, expected_name, expected, mismatches):
    """Record where `node`'s names differ from XRADIO's; return the sources
    XRADIO calls "Unknown" that the converter names."""
    if name != expected_name:
        mismatches.append({"dataset": name, "problem": f"XRADIO names it {expected_name}"})
    ours, theirs = names(node), names(expected)
    unknown = 0
    for label in ours:
        a, b = ours[label], theirs[label]
        if label == "source_name" and a.shape == b.shape:
            placeholder = np.array([re.fullmatch(r"Unknown(_-?\d+)?", str(source)) is not None for source in b], dtype=bool)
            unknown += int(np.sum(placeholder & (a != b)))
            b = np.where(placeholder, a, b)
        if a.shape != b.shape or not np.array_equal(a, b):
            mismatches.append({"dataset": name, "names": label, "ours": list(a)[:8], "theirs": list(b)[:8]})
    return unknown


def chunk_shape(store, dataset, variable):
    """The chunk shape of `variable` in `dataset` of the processing set at
    `store`, as its zarr.json declares it."""
    metadata = json.loads((Path(store) / dataset / variable / "zarr.json").read_text())
    return metadata["chunk_grid"]["configuration"]["chunk_shape"]


def compare(ours, theirs, chunks=False):
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
    shared = {name for names in by_key.values() if len(names) > 1 for name in names}
    record("XRADIO's datasets told apart by the key, or by name where they share one",
           not shared or shared <= set(mine.children),
           {"sharing a key": sorted(shared)} if shared else None)
    record("the same number of datasets", len(mine.children) == len(reference.children),
           {"ours": len(mine.children), "theirs": len(reference.children)})
    mismatches = []
    unknown = 0
    for name in sorted(mine.children):
        node = mine[name]
        match = by_key.get(key(node))
        if not match:
            mismatches.append({"dataset": name, "problem": "no XRADIO dataset with its key"})
            continue
        # XRADIO's datasets can share a key (partitions that differ by what
        # the key leaves out); the one of the same name is then the match.
        if name in match:
            match = [name]
        unknown += compare_names(name, node, match[0], reference[match[0]], mismatches)
        if ("pointing_xds" in node.children) != ("pointing_xds" in reference[match[0]].children):
            mismatches.append({"dataset": name, "problem": "pointing_xds where XRADIO's has none, or none where it has one"})
        if chunks:
            for var in VARIABLES:
                a, b = chunk_shape(ours, name, var), chunk_shape(theirs, match[0], var)
                if a != b:
                    mismatches.append({"dataset": name, "chunks": var, "ours": a, "theirs": b})
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
    record("every dataset equals XRADIO's, names and variables", not mismatches, {"mismatches": mismatches[:20]} if mismatches else None)
    print(json.dumps({"datasets": len(mine.children), "sources_xradio_calls_unknown": unknown, "results": results}, indent=1))
    return all(r["ok"] for r in results)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    convert = sub.add_parser("xradio-convert")
    convert.add_argument("--ms", type=Path, required=True)
    convert.add_argument("--out", type=Path, required=True)
    convert.add_argument("--partition-scheme", nargs="*", default=[])
    convert.add_argument("--with-pointing", action="store_true")
    comparison = sub.add_parser("compare")
    comparison.add_argument("--ours", type=Path, required=True)
    comparison.add_argument("--theirs", type=Path, required=True)
    comparison.add_argument("--chunks", action="store_true")
    args = parser.parse_args()
    if args.mode == "xradio-convert":
        ok = xradio_convert(args.ms, args.out, args.partition_scheme, args.with_pointing)
    else:
        ok = compare(args.ours, args.theirs, args.chunks)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
