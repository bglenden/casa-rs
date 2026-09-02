#!/usr/bin/env python3
"""Build and freeze issue #607's representative full-Stokes CASA oracle."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


SCHEMA = "casa-rs-issue607-full-stokes-oracle-v2"
SOURCE_SHA256 = "4e7ef4e66cc3499d3c923c1ea0111a7c02eba674e144041089c932e6bda86935"
PRODUCTS = ("psf", "residual", "model", "image", "sumwt", "pb")


def tree_sha256(root: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    count = 0
    total = 0
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "table.lock":
            continue
        payload = path.read_bytes()
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        count += 1
        total += len(payload)
    return digest.hexdigest(), count, total


def derive_fixture(source: Path, target: Path) -> dict[str, Any]:
    from casatools import table as table_tool

    source_digest, source_files, source_bytes = tree_sha256(source)
    if source_digest != SOURCE_SHA256:
        raise RuntimeError(f"source identity changed: {source_digest}")
    shutil.copytree(source, target)
    (target / "table.lock").unlink(missing_ok=True)

    table = table_tool()
    try:
        table.open(str(target), nomodify=False)
        fields = np.asarray(table.getcol("FIELD_ID"), dtype=np.int32)
        selected_rows = np.flatnonzero(fields == 0)
        flags = np.asarray(table.getcol("FLAG"), dtype=np.bool_)
        weights = np.asarray(table.getcol("WEIGHT"), dtype=np.float32)
        if flags.shape != (4, 8, 83520) or weights.shape != (4, 83520):
            raise RuntimeError(
                f"unexpected source shapes FLAG={flags.shape} WEIGHT={weights.shape}"
            )
        flags[1, 0, selected_rows[::97]] = True
        flags[2, 1, selected_rows[::89]] = True
        weights[:, selected_rows] *= np.asarray([1.0, 0.7, 1.3, 0.9])[:, None]
        table.putcol("FLAG", flags)
        table.putcol("WEIGHT", weights)
        table.flush()
    finally:
        table.close()

    derived_digest, derived_files, derived_bytes = tree_sha256(target)
    return {
        "source": {
            "relative_path": "measurementset/alma/polcal_LINEAR_BASIS.ms",
            "tree_sha256_excluding_table_lock": source_digest,
            "file_count_excluding_table_lock": source_files,
            "bytes_excluding_table_lock": source_bytes,
        },
        "derived": {
            "tree_sha256_excluding_table_lock": derived_digest,
            "file_count_excluding_table_lock": derived_files,
            "bytes_excluding_table_lock": derived_bytes,
            "selected_field": 0,
            "selected_rows": int(selected_rows.size),
            "selected_correlation_channel_samples": int(selected_rows.size * 4 * 8),
            "cross_hand_flag_pattern": {
                "XY": "channel 0 on every 97th selected row",
                "YX": "channel 1 on every 89th selected row",
            },
            "weight_multipliers_by_correlation": [1.0, 0.7, 1.3, 0.9],
        },
    }


def task_summary(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"type": type(result).__name__, "value": str(result)}
    summary: dict[str, Any] = {}
    for key in ("iterdone", "nmajordone", "stopcode", "stopDescription"):
        if key in result:
            value = result[key]
            summary[key] = value.item() if hasattr(value, "item") else value
    return summary


def run(source: Path, output: Path) -> dict[str, Any]:
    if not source.is_dir():
        raise RuntimeError(f"source MeasurementSet is missing: {source}")
    if output.exists():
        raise RuntimeError(f"output directory must not already exist: {output}")

    from casatasks import casalog, tclean, version_string

    output.mkdir(parents=True)
    fixture = output / "full-stokes-shaped.ms"
    fixture_identity = derive_fixture(source, fixture)
    casalog.setlogfile(str(output / "casa.log"))
    prefix = output / "casa"
    dirty_prefix = output / "casa-dirty"
    started = time.monotonic()
    common = dict(
        vis=str(fixture),
        datacolumn="data",
        field="0",
        spw="0~3",
        phasecenter=0,
        stokes="IQUV",
        specmode="mfs",
        gridder="standard",
        imsize=512,
        cell="1.0arcsec",
        weighting="natural",
        deconvolver="hogbom",
        gain=0.1,
        threshold="0Jy",
        cyclefactor=1.0,
        minpsffraction=0.1,
        maxpsffraction=0.8,
        interactive=False,
        parallel=False,
        pbcor=False,
        savemodel="none",
    )
    dirty_result = tclean(imagename=str(dirty_prefix), niter=0, cycleniter=25, **common)
    result = tclean(imagename=str(prefix), niter=25, cycleniter=25, **common)
    elapsed = time.monotonic() - started
    product_identities = {}
    for product in PRODUCTS:
        path = output / f"casa.{product}"
        if not path.is_dir():
            raise RuntimeError(f"CASA product is missing: {path}")
        digest, files, size = tree_sha256(path)
        product_identities[f".{product}"] = {
            "tree_sha256_excluding_table_lock": digest,
            "file_count_excluding_table_lock": files,
            "bytes_excluding_table_lock": size,
        }
    dirty_product_identities = {}
    for product in ("psf", "residual", "sumwt", "pb"):
        path = output / f"casa-dirty.{product}"
        if not path.is_dir():
            raise RuntimeError(f"CASA dirty product is missing: {path}")
        digest, files, size = tree_sha256(path)
        dirty_product_identities[f".{product}"] = {
            "tree_sha256_excluding_table_lock": digest,
            "file_count_excluding_table_lock": files,
            "bytes_excluding_table_lock": size,
        }
    return {
        "schema": SCHEMA,
        "role": "representative_scientific_acceptance",
        "casa_version": str(version_string()),
        "fixture": fixture_identity,
        "selection": {
            "field": "0",
            "spectral_windows": "0~3",
            "channels_per_window": 8,
            "correlations": ["XX", "XY", "YX", "YY"],
        },
        "imaging": {
            "image_shape": [512, 512],
            "cell": "1.0arcsec",
            "stokes": ["I", "Q", "U", "V"],
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 25,
            "cycleniter": 25,
            "gain": 0.1,
            "threshold": "0Jy",
        },
        "timing_seconds": elapsed,
        "task_result": task_summary(result),
        "products": product_identities,
        "dirty_task_result": task_summary(dirty_result),
        "dirty_products": dirty_product_identities,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run(args.source.resolve(), args.output.resolve())
        receipt = args.output / "oracle.json"
        receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        sys.__stdout__.write(f"issue607_full_stokes_oracle {receipt}\n")
        sys.__stdout__.flush()
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue607_full_stokes_oracle: {error}\n")
        sys.__stderr__.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
