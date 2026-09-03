#!/usr/bin/env python3
"""Freeze issue #607's representative multi-SPW MT-MFS CASA oracle."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


SCHEMA = "casa-rs-issue607-mtmfs-oracle-v1"
SOURCE_SHA256 = "06a6668898d0c193fe55f83e9bf0226bcbd02a1da07d278243c9dfe56c2eb8b7"
PRODUCTS = (
    "psf.tt0",
    "psf.tt1",
    "psf.tt2",
    "residual.tt0",
    "residual.tt1",
    "model.tt0",
    "model.tt1",
    "image.tt0",
    "image.tt1",
    "sumwt.tt0",
    "sumwt.tt1",
    "sumwt.tt2",
    "mask",
    "pb.tt0",
    "pb.tt1",
    "image.tt0.pbcor",
    "image.tt1.pbcor",
    "alpha",
    "alpha.error",
)


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


def source_contract(source: Path) -> dict[str, Any]:
    from casatools import table as table_tool

    digest, files, size = tree_sha256(source)
    if digest != SOURCE_SHA256:
        raise RuntimeError(f"source identity changed: {digest}")
    table = table_tool()
    try:
        table.open(str(source), nomodify=True)
        fields = np.asarray(table.getcol("FIELD_ID"), dtype=np.int32)
        selected_rows = int(np.count_nonzero(fields == 0))
        table.close()
        table.open(str(source / "SPECTRAL_WINDOW"), nomodify=True)
        frequencies = np.asarray(table.getcol("CHAN_FREQ"), dtype=np.float64)
    finally:
        table.close()
    minimum_hz = float(np.min(frequencies))
    maximum_hz = float(np.max(frequencies))
    return {
        "tree_sha256_excluding_table_lock": digest,
        "file_count_excluding_table_lock": files,
        "bytes_excluding_table_lock": size,
        "selected_rows": selected_rows,
        "selected_correlation_channel_samples": selected_rows * 4 * 8,
        "frequency_min_hz": minimum_hz,
        "frequency_max_hz": maximum_hz,
        "fractional_frequency_span": (maximum_hz - minimum_hz)
        / ((maximum_hz + minimum_hz) / 2.0),
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

    source_identity = source_contract(source)
    output.mkdir(parents=True)
    casalog.setlogfile(str(output / "casa.log"))
    prefix = output / "casa"
    started = time.monotonic()
    result = tclean(
        vis=str(source),
        imagename=str(prefix),
        datacolumn="data",
        field="0",
        spw="0~3",
        phasecenter=0,
        stokes="I",
        specmode="mfs",
        gridder="standard",
        imsize=512,
        cell="1.0arcsec",
        weighting="natural",
        deconvolver="mtmfs",
        nterms=2,
        scales=[0, 5],
        smallscalebias=0.0,
        niter=8,
        cycleniter=2,
        gain=0.1,
        threshold="0Jy",
        cyclefactor=1.0,
        minpsffraction=0.05,
        maxpsffraction=0.8,
        pblimit=0.2,
        pbcor=True,
        interactive=False,
        parallel=False,
        savemodel="none",
    )
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
    return {
        "schema": SCHEMA,
        "role": "representative_scientific_acceptance",
        "casa_version": str(version_string()),
        "source": source_identity,
        "selection": {
            "field": "0",
            "spectral_windows": "0~3",
            "channels_per_window": 8,
            "correlations": ["XX", "XY", "YX", "YY"],
        },
        "imaging": {
            "image_shape": [512, 512],
            "cell": "1.0arcsec",
            "stokes": ["I"],
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "scales_px": [0, 5],
            "small_scale_bias": 0.0,
            "niter": 8,
            "cycleniter": 2,
            "gain": 0.1,
            "threshold": "0Jy",
            "pblimit": 0.2,
            "pbcor": True,
        },
        "timing_seconds": elapsed,
        "task_result": task_summary(result),
        "products": product_identities,
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
        sys.__stdout__.write(f"issue607_mtmfs_oracle {receipt}\n")
        sys.__stdout__.flush()
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue607_mtmfs_oracle: {error}\n")
        sys.__stderr__.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
