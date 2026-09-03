#!/usr/bin/env python3
"""Freeze representative CASA main/outlier products for issue #607."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

import numpy as np


PRODUCTS = (".psf", ".residual", ".model", ".image", ".mask", ".sumwt")
DISTANT_OUTLIER_OFFSET_ARCSEC = -220.0
DISTANT_OUTLIER_FLUX_JY = 400.0
OVERLAPPING_OUTLIER_PHASECENTER = "J2000 4.71239123rad -0.40152075rad"
DISTANT_OUTLIER_PHASECENTER = "J2000 4.71239123rad -0.40249038rad"
SPEED_OF_LIGHT_M_S = 299_792_458.0
SELECTED_CHANNELS = 24


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


def inject_outlier_source(measurement_set: Path) -> None:
    from casatools import table as table_tool

    table = table_tool()
    try:
        table.open(str(measurement_set), nomodify=False)
        data = np.asarray(table.getcol("DATA"), dtype=np.complex64)
        uvw = np.asarray(table.getcol("UVW"), dtype=np.float64)
        data_description_ids = np.asarray(table.getcol("DATA_DESC_ID"), dtype=np.int32)
        selected_rows = np.flatnonzero(data_description_ids == 0)
        spectral_window = table_tool()
        try:
            spectral_window.open(str(measurement_set / "SPECTRAL_WINDOW"), nomodify=True)
            frequencies_hz = np.asarray(spectral_window.getcell("CHAN_FREQ", 0), dtype=np.float64)
        finally:
            spectral_window.close()
        if data.shape != (2, 512, 130923) or uvw.shape != (3, 130923):
            raise RuntimeError(f"unexpected DATA/UVW shape: {data.shape}/{uvw.shape}")
        if frequencies_hz.shape != (512,) or selected_rows.size != 130923:
            raise RuntimeError(
                f"unexpected frequency/selection shape: {frequencies_hz.shape}/{selected_rows.size}"
            )
        m = np.deg2rad(DISTANT_OUTLIER_OFFSET_ARCSEC / 3600.0)
        phase = (
            -2.0
            * np.pi
            * uvw[1, selected_rows, None]
            * m
            * frequencies_hz[None, :SELECTED_CHANNELS]
            / SPEED_OF_LIGHT_M_S
        )
        visibility = np.asarray(DISTANT_OUTLIER_FLUX_JY * np.exp(1j * phase), dtype=np.complex64)
        data[:, :SELECTED_CHANNELS, selected_rows] += visibility.T[None, :, :]
        table.putcol("DATA", data)
        table.flush()
    finally:
        table.close()


def run(source: Path, output: Path, inject_distant_outlier: bool) -> dict[str, object]:
    from casatasks import tclean, version_string

    if output.exists():
        raise RuntimeError(f"output directory must not already exist: {output}")
    output.mkdir(parents=True)
    staged = output / "input.ms"
    shutil.copytree(source, staged)
    (staged / "table.lock").unlink(missing_ok=True)
    source_identity = tree_identity(source)
    if inject_distant_outlier:
        inject_outlier_source(staged)
    fixture_identity = tree_identity(staged)
    outlier_phasecenter = (
        DISTANT_OUTLIER_PHASECENTER
        if inject_distant_outlier
        else OVERLAPPING_OUTLIER_PHASECENTER
    )
    scenarios: dict[str, object] = {}
    for label, niter in (("dirty", 0), ("clean", 25)):
        root = output / label
        root.mkdir()
        main = root / "main"
        outlier = root / "outlier"
        outlier_file = root / "outlier.txt"
        outlier_file.write_text(
            "\n".join(
                (
                    f"imagename={outlier}",
                    "nchan=1",
                    "imsize=[512,512]",
                    "cell=[0.35arcsec,0.35arcsec]",
                    f"phasecenter={outlier_phasecenter}",
                    "usemask=user",
                    "mask=circle[[256pix,256pix],64pix]",
                    "",
                )
            )
        )
        started = time.perf_counter()
        result = tclean(
            vis=str(staged),
            imagename=str(main),
            field="0",
            spw="0:0~23",
            datacolumn="data",
            specmode="mfs",
            gridder="standard",
            imsize=512,
            cell="0.35arcsec",
            weighting="natural",
            deconvolver="hogbom",
            niter=niter,
            cycleniter=max(niter, 1),
            gain=0.1,
            threshold="0Jy",
            usemask="user",
            mask="box[[192pix,192pix],[319pix,319pix]]",
            outlierfile=str(outlier_file),
            restoration=True,
            calcpsf=True,
            calcres=True,
            pblimit=-0.2,
            parallel=False,
        )
        products: dict[str, object] = {}
        for role, prefix in (("main", main), ("outlier", outlier)):
            products[role] = {
                suffix: tree_identity(Path(f"{prefix}{suffix}"))
                for suffix in PRODUCTS
                if Path(f"{prefix}{suffix}").is_dir()
            }
        scenarios[label] = {
            "timing_seconds": time.perf_counter() - started,
            "task_result": json_safe(result),
            "products": products,
        }
    return {
        "schema": "casa-rs-issue607-outlier-oracle-v1",
        "role": "representative_scientific_acceptance",
        "casa_version": str(version_string()),
        "source": source_identity,
        "fixture": {
            **fixture_identity,
            "preserves_source_geometry_flags_and_weights": True,
            "injected_outlier": (
                {
                    "offset_arcsec": [0.0, DISTANT_OUTLIER_OFFSET_ARCSEC],
                    "flux_jy": DISTANT_OUTLIER_FLUX_JY,
                }
                if inject_distant_outlier
                else None
            ),
        },
        "selection": {
            "field": 0,
            "spectral_window": 0,
            "channels": SELECTED_CHANNELS,
            "selected_rows": 130923,
            "selected_correlation_channel_samples": 6284304,
        },
        "imaging": {
            "main_shape": [512, 512, 1, 1],
            "outlier_shape": [512, 512, 1, 1],
            "cell_arcsec": 0.35,
            "outlier_phasecenter": outlier_phasecenter,
        },
        "scenarios": scenarios,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inject-distant-outlier", action="store_true")
    args = parser.parse_args()
    try:
        result = run(
            args.source.resolve(),
            args.output.resolve(),
            args.inject_distant_outlier,
        )
        receipt = args.output / "oracle.json"
        receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        sys.__stdout__.write(f"issue607_outlier_oracle {receipt}\n")
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue607_outlier_oracle: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
