#!/usr/bin/env python3
"""Generate the frozen CASA sequential reference for issue #597.

CASA does not implement ADR-0011's joint same-support solver. This recipe
therefore freezes only the applicable sequential reference: continuum imaging
on the declared anchor channels, followed by uvcontsub and line imaging.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import sys
import time
import zipfile
from pathlib import Path
from typing import Any


SCHEMA = "casa-rs-issue597-sequential-oracle-v1"
SOURCE_CHANNELS = "0:52~67"
ANCHOR_CHANNELS = "0:52~59"
LINE_CHANNELS = "0:60~67"
IMAGE_SIZE = 32
CELL = "0.01arcsec"
THRESHOLD = "0.002Jy"
PRODUCTS = ("model", "residual", "image", "psf", "mask")


def tree_sha256(root: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    count = 0
    total = 0
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if path.name == "table.lock":
            continue
        payload = path.read_bytes()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        count += 1
        total += len(payload)
    return digest.hexdigest(), count, total


def task_summary(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"type": type(result).__name__, "value": str(result)}
    summary: dict[str, Any] = {}
    for key in ("iterdone", "nmajordone", "stopcode", "stopDescription"):
        if key not in result:
            continue
        value = result[key]
        if hasattr(value, "item"):
            value = value.item()
        summary[key] = value
    return summary


def freeze_products(output: Path) -> Path:
    """Export only the small numeric observables needed by the normal gate."""
    import numpy as np
    from casatools import image as image_tool

    products: dict[str, Any] = {}
    tool = image_tool()
    try:
        for workflow in ("continuum", "line"):
            for product in PRODUCTS:
                path = output / f"{workflow}.{product}"
                if not tool.open(str(path)):
                    raise RuntimeError(f"CASA image could not be opened: {path}")
                products[f"{workflow}_{product}"] = np.asarray(
                    tool.getchunk(), dtype=np.float32
                )
                tool.close()
    finally:
        tool.close()
    target = output / "sequential-products.npz"
    with zipfile.ZipFile(target, "w") as archive:
        for name in sorted(products):
            encoded = io.BytesIO()
            np.lib.format.write_array(encoded, products[name], allow_pickle=False)
            member = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            member.compress_type = zipfile.ZIP_DEFLATED
            member.external_attr = 0o644 << 16
            archive.writestr(member, encoded.getvalue())
    return target


def run(source: Path, output: Path) -> dict[str, Any]:
    if not source.is_dir():
        raise RuntimeError(f"source MeasurementSet is missing: {source}")
    if output.exists():
        raise RuntimeError(f"output directory must not already exist: {output}")

    from casatasks import casalog, tclean, uvcontsub, version_string

    output.mkdir(parents=True)
    staged = output / "input.ms"
    shutil.copytree(source, staged)
    (staged / "table.lock").unlink(missing_ok=True)
    source_digest, source_file_count, source_bytes = tree_sha256(source)

    common = {
        "field": "0",
        "imsize": IMAGE_SIZE,
        "cell": CELL,
        "phasecenter": 0,
        "stokes": "I",
        "gridder": "standard",
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 512,
        "cycleniter": 16,
        "gain": 0.1,
        "threshold": THRESHOLD,
        "cyclefactor": 1.0,
        "minpsffraction": 0.05,
        "maxpsffraction": 0.8,
        "interactive": False,
        "parallel": False,
        "pbcor": False,
        "savemodel": "none",
    }

    casalog.setlogfile(str(output / "casa.log"))
    started = time.monotonic()
    continuum_result = tclean(
        vis=str(staged),
        imagename=str(output / "continuum"),
        datacolumn="data",
        spw=ANCHOR_CHANNELS,
        specmode="mfs",
        **common,
    )
    continuum_seconds = time.monotonic() - started

    started = time.monotonic()
    uvcontsub_result = uvcontsub(
        vis=str(staged),
        outputvis=str(output / "continuum-subtracted.ms"),
        field="0",
        spw="0",
        fitspec=ANCHOR_CHANNELS,
        fitorder=0,
        datacolumn="data",
    )
    uvcontsub_seconds = time.monotonic() - started

    line_common = dict(common)
    line_common.update(
        {
            "usemask": "user",
            "mask": "box[[15pix,15pix],[16pix,16pix]]",
            "restoringbeam": "common",
        }
    )
    started = time.monotonic()
    line_result = tclean(
        vis=str(output / "continuum-subtracted.ms"),
        imagename=str(output / "line"),
        datacolumn="data",
        spw=LINE_CHANNELS,
        specmode="cubedata",
        nchan=8,
        start=60,
        width=1,
        **line_common,
    )
    line_seconds = time.monotonic() - started
    frozen_products = freeze_products(output)

    return {
        "schema": SCHEMA,
        "role": "sequential_casa_reference_not_a_joint_solver",
        "casa_version": str(version_string()),
        "source": {
            "relative_path": "unittest/uvcontsub/sim_alma_cont_poly_order_0_nonoise.ms",
            "tree_sha256_excluding_table_lock": source_digest,
            "file_count_excluding_table_lock": source_file_count,
            "bytes_excluding_table_lock": source_bytes,
        },
        "selection": {
            "source_channels": SOURCE_CHANNELS,
            "continuum_anchors": ANCHOR_CHANNELS,
            "line_support": LINE_CHANNELS,
        },
        "imaging": {
            "image_size": IMAGE_SIZE,
            "cell": CELL,
            "weighting": "natural",
            "deconvolver": "hogbom",
            "threshold": THRESHOLD,
            "niter": common["niter"],
            "cycleniter": common["cycleniter"],
            "gain": common["gain"],
        },
        "timings_seconds": {
            "continuum_tclean": continuum_seconds,
            "uvcontsub": uvcontsub_seconds,
            "line_tclean": line_seconds,
        },
        "results": {
            "continuum": task_summary(continuum_result),
            "uvcontsub": task_summary(uvcontsub_result),
            "line": task_summary(line_result),
        },
        "products": {
            "numeric_archive": frozen_products.name,
            "numeric_archive_sha256": hashlib.sha256(frozen_products.read_bytes()).hexdigest(),
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
        sys.__stdout__.write(f"issue597_casa_oracle {receipt}\n")
        sys.__stdout__.flush()
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue597_casa_oracle: {error}\n")
        sys.__stderr__.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
