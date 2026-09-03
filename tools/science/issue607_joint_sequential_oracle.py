#!/usr/bin/env python3
"""Freeze representative sequential CASA continuum and line products for issue #607."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

import numpy as np


FITSPW = "0:0~123;132~255"
PRODUCTS = (".psf", ".residual", ".model", ".image", ".mask", ".sumwt")


def json_safe(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def tree_identity(root: Path) -> dict[str, object]:
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
    return {
        "tree_sha256_excluding_table_lock": digest.hexdigest(),
        "file_count_excluding_table_lock": count,
        "bytes_excluding_table_lock": total,
    }


def product_identities(prefix: Path) -> dict[str, object]:
    return {
        suffix: tree_identity(Path(f"{prefix}{suffix}"))
        for suffix in PRODUCTS
        if Path(f"{prefix}{suffix}").is_dir()
    }


def run(source: Path, output: Path) -> dict[str, object]:
    from casatasks import tclean, uvcontsub, version_string

    if output.exists():
        raise RuntimeError(f"output directory must not already exist: {output}")
    output.mkdir(parents=True)
    staged = output / "input.ms"
    shutil.copytree(source, staged)
    (staged / "table.lock").unlink(missing_ok=True)

    continuum = output / "continuum"
    started = time.perf_counter()
    continuum_result = tclean(
        vis=str(staged),
        imagename=str(continuum),
        field="0",
        spw=FITSPW,
        datacolumn="data",
        specmode="mfs",
        gridder="standard",
        imsize=512,
        cell="0.1arcsec",
        weighting="natural",
        deconvolver="hogbom",
        niter=64,
        cycleniter=32,
        gain=0.1,
        threshold="1e-5Jy",
        usemask="user",
        mask="box[[192pix,192pix],[319pix,319pix]]",
        pblimit=-0.2,
        parallel=False,
    )
    continuum_seconds = time.perf_counter() - started

    started = time.perf_counter()
    contsub = Path(f"{staged}.contsub")
    uvcontsub(
        vis=str(staged),
        outputvis=str(contsub),
        field="0",
        spw="0",
        fitspec=FITSPW,
        fitorder=1,
        datacolumn="data",
    )
    uvcontsub_seconds = time.perf_counter() - started
    line = output / "line"
    started = time.perf_counter()
    line_result = tclean(
        vis=str(contsub),
        imagename=str(line),
        field="0",
        spw="0",
        datacolumn="data",
        specmode="cube",
        nchan=256,
        start=0,
        width=1,
        interpolation="nearest",
        gridder="standard",
        imsize=512,
        cell="0.1arcsec",
        weighting="natural",
        perchanweightdensity=True,
        deconvolver="hogbom",
        niter=32,
        cycleniter=32,
        gain=0.1,
        threshold="1e-5Jy",
        usemask="user",
        mask="box[[192pix,192pix],[319pix,319pix]]",
        pblimit=-0.2,
        restoringbeam="common",
        parallel=False,
    )
    line_seconds = time.perf_counter() - started
    return {
        "schema": "casa-rs-issue607-joint-sequential-oracle-v1",
        "role": "representative_scientific_acceptance",
        "casa_version": str(version_string()),
        "source": tree_identity(source),
        "selection": {
            "field": 0,
            "spectral_window": 0,
            "channels": 256,
            "selected_rows": 2400,
            "selected_correlation_channel_samples": 1228800,
            "fitspw": FITSPW,
            "fitorder": 1,
        },
        "imaging": {"image_shape": [512, 512], "cell_arcsec": 0.1},
        "continuum": {
            "timing_seconds": continuum_seconds,
            "task_result": json_safe(continuum_result),
            "products": product_identities(continuum),
        },
        "continuum_subtraction": {
            "timing_seconds": uvcontsub_seconds,
            "measurement_set": tree_identity(contsub),
        },
        "line_cube": {
            "timing_seconds": line_seconds,
            "task_result": json_safe(line_result),
            "products": product_identities(line),
        },
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
        sys.__stdout__.write(f"issue607_joint_sequential_oracle {receipt}\n")
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue607_joint_sequential_oracle: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
