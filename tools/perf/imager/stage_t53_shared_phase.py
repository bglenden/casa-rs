"""Stage the preserved #478 shared-phase multifield fixture without changing visibilities.

Run with CASA Python. This retains the integration-by-integration field split
from imager_casa_parity.rs at 376c7d87bb995f295a696995ad8292bc39582529.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import shutil

from casatools import table
import numpy as np

from perf_harness.tree_identity import tree_identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    source_identity = tree_identity(args.source, excluded_names={"table.lock"})
    if args.destination.exists():
        raise FileExistsError(args.destination)
    shutil.copytree(args.source, args.destination)
    tb = table()
    tb.open(str(args.destination), nomodify=False)
    try:
        description = tb.getcoldesc("UVW")
        description.update(dataManagerType="TiledColumnStMan", dataManagerGroup="T53UVW")
        tb.renamecol("UVW", "T53_ORIGINAL_UVW")
        manager = {
            "TYPE": "TiledColumnStMan", "NAME": "T53UVW", "COLUMNS": ["UVW"],
            "SPEC": {"DEFAULTTILESHAPE": [3, 4096]},
        }
        tb.addcols({"UVW": description}, {"t53_uvw": manager})
        for start in range(0, tb.nrows(), 4096):
            count = min(4096, tb.nrows() - start)
            values = tb.getcol("T53_ORIGINAL_UVW", start, count)
            tb.putcol("UVW", values, start, count)
            if not np.array_equal(values, tb.getcol("UVW", start, count)):
                raise ValueError(f"UVW storage conversion changed values at row {start}")
    finally:
        tb.close()
    tb.open(str(args.destination / "FIELD"), nomodify=False)
    try:
        if tb.nrows() != 1:
            raise ValueError("shared-phase source must contain exactly one FIELD row")
        row = {name: tb.getcell(name, 0) for name in tb.colnames()}
        tb.addrows(1)
        for name, value in row.items():
            tb.putcell(name, 1, "FIELD1" if name == "NAME" else value)
    finally:
        tb.close()
    tb.open(str(args.destination), nomodify=False)
    try:
        original = tb.getcol("FIELD_ID")
        if not np.all(original == 0):
            raise ValueError("shared-phase source MAIN must use FIELD zero")
        times = tb.getcol("TIME")
        by_time = {}
        for time in times.tolist():
            if time not in by_time:
                by_time[time] = len(by_time) % 2
        fields = np.array([by_time[time] for time in times], dtype=original.dtype)
        counts = dict(Counter(int(value) for value in fields))
        if set(counts) != {0, 1}:
            raise ValueError("both fields must contain complete integrations")
        tb.putcol("FIELD_ID", fields)
    finally:
        tb.close()
    if tree_identity(args.source, excluded_names={"table.lock"}) != source_identity:
        raise ValueError("source MeasurementSet changed during staging")
    receipt = {
        "source": str(args.source.resolve()),
        "source_identity": source_identity,
        "destination": str(args.destination.resolve()),
        "destination_identity": tree_identity(args.destination, excluded_names={"table.lock"}),
        "field_rows": counts,
        "unique_integrations": len(by_time),
        "changed_columns": ["FIELD row 1 copied from row 0", "MAIN.FIELD_ID"],
        "storage_change": "UVW copied losslessly to TiledColumnStMan; original retained as T53_ORIGINAL_UVW",
        "uvw_values_equal": True,
        "comparison_chunk_rows": 4096,
        "visibility_payload_changed": False,
    }
    args.destination.with_suffix(".staging.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
