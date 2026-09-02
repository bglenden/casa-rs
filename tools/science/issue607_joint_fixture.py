#!/usr/bin/env python3
"""Freeze issue #607's real-observation-shaped joint continuum-line fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np


SCHEMA = "casa-rs-issue607-joint-fixture-v2"
SOURCE_SHA256 = "e39cbfd898885c8726497b9ee637ddad834bfc189ff6c4774eff671736d5b5a0"
LINE_CHANNELS = tuple(range(124, 132))
LINE_FLUX_JY = (0.2, 0.4, 0.6, 0.8, 1.0, 0.8, 0.6, 0.4)
CELL_ARCSEC = 0.1
CONTINUUM_COMPONENTS = (
    {"offset_pixels": (0, 0), "flux_jy": 0.65},
    {"offset_pixels": (24, -17), "flux_jy": 0.25},
    {"offset_pixels": (-31, 22), "flux_jy": 0.10},
)
LINE_OFFSET_PIXELS = (11, 15)
SPEED_OF_LIGHT_M_S = 299_792_458.0


def direction_cosines(offset_pixels: tuple[int, int]) -> tuple[float, float]:
    radians_per_pixel = np.deg2rad(CELL_ARCSEC / 3600.0)
    return offset_pixels[0] * radians_per_pixel, offset_pixels[1] * radians_per_pixel


def point_visibility(
    uvw_m: np.ndarray,
    frequencies_hz: np.ndarray,
    offset_pixels: tuple[int, int],
    flux_jy: float,
) -> np.ndarray:
    l, m = direction_cosines(offset_pixels)
    geometric_delay_m = uvw_m[0, :, None] * l + uvw_m[1, :, None] * m
    phase = -2.0 * np.pi * geometric_delay_m * frequencies_hz[None, :] / SPEED_OF_LIGHT_M_S
    return flux_jy * np.exp(1j * phase)


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


def derive(source: Path, output: Path) -> dict[str, Any]:
    from casatools import table as table_tool
    from casatasks import version_string

    source_digest, source_files, source_bytes = tree_sha256(source)
    if source_digest != SOURCE_SHA256:
        raise RuntimeError(f"source identity changed: {source_digest}")
    if output.exists():
        raise RuntimeError(f"output directory must not already exist: {output}")
    output.mkdir(parents=True)
    target = output / "joint-shaped.ms"
    shutil.copytree(source, target)
    (target / "table.lock").unlink(missing_ok=True)

    table = table_tool()
    try:
        table.open(str(target), nomodify=False)
        data_description_ids = np.asarray(table.getcol("DATA_DESC_ID"), dtype=np.int32)
        selected_rows = np.flatnonzero(data_description_ids == 0)
        data = np.asarray(table.getcol("DATA"), dtype=np.complex64)
        flags = np.asarray(table.getcol("FLAG"), dtype=np.bool_)
        weights = np.asarray(table.getcol("WEIGHT"), dtype=np.float32)
        uvw = np.asarray(table.getcol("UVW"), dtype=np.float64)
        if data.shape != (2, 256, 9600) or flags.shape != data.shape:
            raise RuntimeError(f"unexpected source DATA/FLAG shape: {data.shape}/{flags.shape}")
        if weights.shape != (2, 9600) or selected_rows.size != 2400:
            raise RuntimeError(
                f"unexpected source WEIGHT/selection shape: {weights.shape}/{selected_rows.size}"
            )
        spectral_window = table_tool()
        try:
            spectral_window.open(str(target / "SPECTRAL_WINDOW"), nomodify=True)
            frequencies_hz = np.asarray(spectral_window.getcell("CHAN_FREQ", 0), dtype=np.float64)
        finally:
            spectral_window.close()
        if frequencies_hz.shape != (256,) or uvw.shape != (3, 9600):
            raise RuntimeError(
                f"unexpected CHAN_FREQ/UVW shape: {frequencies_hz.shape}/{uvw.shape}"
            )
        selected_uvw = uvw[:, selected_rows]
        visibility = np.zeros((selected_rows.size, frequencies_hz.size), dtype=np.complex128)
        for component in CONTINUUM_COMPONENTS:
            visibility += point_visibility(
                selected_uvw,
                frequencies_hz,
                component["offset_pixels"],
                component["flux_jy"],
            )
        line_visibility = point_visibility(
            selected_uvw,
            frequencies_hz,
            LINE_OFFSET_PIXELS,
            1.0,
        )
        for channel, flux in zip(LINE_CHANNELS, LINE_FLUX_JY, strict=True):
            visibility[:, channel] += flux * line_visibility[:, channel]
        data[:, :, selected_rows] = np.asarray(visibility.T[None, :, :], dtype=np.complex64)
        table.putcol("DATA", data)
        table.flush()
    finally:
        table.close()

    derived_digest, derived_files, derived_bytes = tree_sha256(target)
    return {
        "schema": SCHEMA,
        "role": "representative_scientific_acceptance_fixture",
        "casa_version": str(version_string()),
        "source": {
            "relative_path": "measurementset/alma/uid___A002_Xd7be9d_X4838-spw16-18-20-22.ms",
            "tree_sha256_excluding_table_lock": source_digest,
            "file_count_excluding_table_lock": source_files,
            "bytes_excluding_table_lock": source_bytes,
        },
        "derived": {
            "tree_sha256_excluding_table_lock": derived_digest,
            "file_count_excluding_table_lock": derived_files,
            "bytes_excluding_table_lock": derived_bytes,
            "selected_spectral_window": 0,
            "selected_rows": int(selected_rows.size),
            "channels": 256,
            "correlations": ["XX", "YY"],
            "selected_correlation_channel_samples": int(selected_rows.size * 2 * 256),
            "flags_preserved": True,
            "weights_preserved": True,
            "uvw_time_baselines_preserved": True,
        },
        "analytic_sky": {
            "cell_arcsec": CELL_ARCSEC,
            "continuum_components": list(CONTINUUM_COMPONENTS),
            "continuum_total_flux_jy": sum(
                component["flux_jy"] for component in CONTINUUM_COMPONENTS
            ),
            "continuum_anchor_channels": [0, 123, 132, 255],
            "line_offset_pixels": list(LINE_OFFSET_PIXELS),
            "line_channels": list(LINE_CHANNELS),
            "line_flux_jy_above_continuum": list(LINE_FLUX_JY),
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
        sys.__stdout__.write(f"issue607_joint_fixture {receipt}\n")
        sys.__stdout__.flush()
        return 0
    except Exception as error:
        sys.__stderr__.write(f"issue607_joint_fixture: {error}\n")
        sys.__stderr__.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
