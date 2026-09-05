#!/usr/bin/env python3
"""Freeze issue #607's joint fixture with an existing CORRECTED_DATA column."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import numpy as np


SCHEMA = "casa-rs-issue607-contsub-fixture-v1"
SOURCE_SHA256 = "978667029e3843ce49ab704a7b01b5662b6a493750fa3af021b5be385f01d586"


def tree_sha256(root: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    count = 0
    total = 0
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "table.lock":
            continue
        payload = path.read_bytes()
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        count += 1
        total += len(payload)
    return digest.hexdigest(), count, total


def derive(source: Path, output: Path) -> dict[str, object]:
    from casatasks import version_string
    from casatools import table as table_tool

    source_digest, source_files, source_bytes = tree_sha256(source)
    if source_digest != SOURCE_SHA256:
        raise RuntimeError(f"source identity changed: {source_digest}")
    if output.exists():
        raise RuntimeError(f"output directory must not already exist: {output}")
    output.mkdir(parents=True)
    target = output / "continuum-subtraction-shaped.ms"
    shutil.copytree(source, target)
    (target / "table.lock").unlink(missing_ok=True)

    table = table_tool()
    try:
        table.open(str(target), nomodify=False)
        if "CORRECTED_DATA" in table.colnames():
            raise RuntimeError("source unexpectedly already has CORRECTED_DATA")
        descriptor = table.getcoldesc("DATA")
        descriptor["dataManagerGroup"] = "TiledCorrectedData"
        data_manager = next(
            value
            for value in table.getdminfo().values()
            if "DATA" in value["COLUMNS"]
        )
        manager = {
            "TYPE": data_manager["TYPE"],
            "NAME": "TiledCorrectedData",
            "SPEC": {
                "DEFAULTTILESHAPE": data_manager["SPEC"]["DEFAULTTILESHAPE"],
            },
        }
        table.addcols({"CORRECTED_DATA": descriptor}, manager)
        row_count = table.nrows()
        chunk_rows = 512
        for start in range(0, row_count, chunk_rows):
            count = min(chunk_rows, row_count - start)
            data = np.asarray(table.getcol("DATA", startrow=start, nrow=count))
            table.putcol("CORRECTED_DATA", data, startrow=start, nrow=count)
        table.flush()
    finally:
        table.close()

    derived_digest, derived_files, derived_bytes = tree_sha256(target)
    return {
        "schema": SCHEMA,
        "role": "representative_scientific_acceptance_fixture",
        "casa_version": str(version_string()),
        "source": {
            "tree_sha256_excluding_table_lock": source_digest,
            "file_count_excluding_table_lock": source_files,
            "bytes_excluding_table_lock": source_bytes,
        },
        "derived": {
            "tree_sha256_excluding_table_lock": derived_digest,
            "file_count_excluding_table_lock": derived_files,
            "bytes_excluding_table_lock": derived_bytes,
            "selected_rows": 2_400,
            "channels": 256,
            "correlations": ["XX", "YY"],
            "selected_correlation_channel_samples": 1_228_800,
            "corrected_data_initially_equals_data": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = derive(args.source.resolve(), args.output.resolve())
        receipt = args.output / "fixture.json"
        receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        sys.__stdout__.write(f"issue607_contsub_fixture {receipt}\n")
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue607_contsub_fixture: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
