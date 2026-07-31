#!/usr/bin/env python3
"""Emit a conservative local-facet W bound from a VLASS MeasurementSet.

The bound includes every row for the selected field and every requested
channel before flags and the production UV-range cut. It is therefore an upper
bound for the accepted imaging samples, not a sample-count receipt.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-localized-facet-uvw-bound/v1"
ARCSEC_TO_RAD = math.pi / (180.0 * 3600.0)
SPEED_OF_LIGHT_M_S = 299_792_458.0


class UvwBoundError(RuntimeError):
    """The selected MS geometry cannot produce a conservative W bound."""


def facet_offset_rad(
    center_pixel: tuple[float, float],
    *,
    image_reference_pixel: float,
    cell_arcsec: float,
) -> tuple[float, float]:
    return tuple(
        (pixel - image_reference_pixel) * cell_arcsec * ARCSEC_TO_RAD
        for pixel in center_pixel
    )


def uvw_rotation_upper_bound(
    uvw_m: np.ndarray,
    frequency_hz: np.ndarray,
    *,
    facet_offset: tuple[float, float],
) -> dict[str, float]:
    if uvw_m.ndim != 2 or uvw_m.shape[0] != 3:
        raise UvwBoundError("UVW must have shape [3, rows]")
    if frequency_hz.ndim != 1 or frequency_hz.size == 0:
        raise UvwBoundError("frequency vector must be non-empty")
    if not np.all(np.isfinite(uvw_m)) or not np.all(np.isfinite(frequency_hz)):
        raise UvwBoundError("UVW and frequency values must be finite")
    if np.any(frequency_hz <= 0.0):
        raise UvwBoundError("frequencies must be positive")
    theta = math.hypot(*facet_offset)
    scale = frequency_hz[None, :] / SPEED_OF_LIGHT_M_S
    abs_w = np.abs(uvw_m[2, :, None]) * scale
    uv_radius = np.hypot(uvw_m[0, :, None], uvw_m[1, :, None]) * scale
    baseline = np.sqrt(np.sum(uvw_m**2, axis=0))[:, None] * scale
    return {
        "facet_center_offset_rad": theta,
        "raw_abs_w_lambda_max": float(np.max(abs_w)),
        "rotated_abs_w_lambda_upper_bound": float(
            np.max(abs_w + uv_radius * math.sin(theta))
        ),
        "baseline_lambda_max": float(np.max(baseline)),
    }


def read_bound(
    ms: pathlib.Path,
    *,
    field_id: int,
    spw_start: int,
    spw_end: int,
    channel_start: int,
    channel_count: int,
    center_pixel: tuple[float, float],
    image_reference_pixel: float,
    cell_arcsec: float,
    dataset_archive_sha256: str,
) -> dict[str, Any]:
    if not ms.is_dir():
        raise UvwBoundError(f"MeasurementSet does not exist: {ms}")
    if field_id < 0 or spw_start < 0 or spw_end < spw_start:
        raise UvwBoundError("field and SPW selection must be non-negative")
    if channel_start < 0 or channel_count <= 0:
        raise UvwBoundError("channel selection must be non-negative and non-empty")
    if len(dataset_archive_sha256) != 64 or any(
        value not in "0123456789abcdef" for value in dataset_archive_sha256.lower()
    ):
        raise UvwBoundError("dataset archive SHA-256 must contain 64 hexadecimal digits")

    try:
        from casatools import table
    except ImportError as error:
        raise UvwBoundError("casatools is required to read the MeasurementSet") from error

    tool = table()
    tool.open(str(ms))
    selected = tool.query(f"FIELD_ID=={field_id}")
    uvw_m = np.asarray(selected.getcol("UVW"), dtype=np.float64)
    data_description_ids = np.asarray(
        selected.getcol("DATA_DESC_ID"), dtype=np.int64
    )
    selected.close()
    tool.close()
    if uvw_m.shape[1] == 0:
        raise UvwBoundError("field selection contains no rows")

    tool.open(str(ms / "DATA_DESCRIPTION"))
    spectral_window_ids = np.asarray(
        tool.getcol("SPECTRAL_WINDOW_ID"), dtype=np.int64
    )
    tool.close()
    tool.open(str(ms / "SPECTRAL_WINDOW"))
    channel_frequencies = [
        np.asarray(tool.getcell("CHAN_FREQ", row), dtype=np.float64)
        for row in range(tool.nrows())
    ]
    tool.close()

    facet_offset = facet_offset_rad(
        center_pixel,
        image_reference_pixel=image_reference_pixel,
        cell_arcsec=cell_arcsec,
    )
    metrics: list[dict[str, float]] = []
    channel_samples = 0
    selected_ddids: list[int] = []
    for ddid in sorted(set(int(value) for value in data_description_ids)):
        spw = int(spectral_window_ids[ddid])
        if not spw_start <= spw <= spw_end:
            continue
        frequencies = channel_frequencies[spw][
            channel_start : channel_start + channel_count
        ]
        if frequencies.size != channel_count:
            raise UvwBoundError(
                f"SPW {spw} does not contain the requested channel selection"
            )
        rows = data_description_ids == ddid
        metrics.append(
            uvw_rotation_upper_bound(
                uvw_m[:, rows],
                frequencies,
                facet_offset=facet_offset,
            )
        )
        channel_samples += int(np.count_nonzero(rows) * frequencies.size)
        selected_ddids.append(ddid)
    if not metrics:
        raise UvwBoundError("SPW selection contains no rows")

    return {
        "schema": SCHEMA,
        "role": "production-inert-architecture-discriminator",
        "measurement_set": str(ms),
        "dataset_archive_sha256": dataset_archive_sha256.lower(),
        "selection": {
            "field_id": field_id,
            "spw": [spw_start, spw_end],
            "channel_start": channel_start,
            "channel_count": channel_count,
            "data_description_ids": selected_ddids,
            "rows_before_flags_and_uvrange": int(uvw_m.shape[1]),
            "channel_samples_before_flags_and_uvrange": channel_samples,
        },
        "facet": {
            "center_pixel": list(center_pixel),
            "image_reference_pixel": image_reference_pixel,
            "cell_arcsec": cell_arcsec,
            "offset_rad": list(facet_offset),
        },
        "bound": {
            "facet_center_offset_rad": math.hypot(*facet_offset),
            "raw_abs_w_lambda_max": max(
                row["raw_abs_w_lambda_max"] for row in metrics
            ),
            "rotated_abs_w_lambda_upper_bound": max(
                row["rotated_abs_w_lambda_upper_bound"] for row in metrics
            ),
            "baseline_lambda_max": max(row["baseline_lambda_max"] for row in metrics),
            "scope": (
                "all selected field/SPW/channel rows before flags and the "
                "production UV-range cut"
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("measurement_set", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--field", type=int, default=1525)
    parser.add_argument("--spw-start", type=int, default=2)
    parser.add_argument("--spw-end", type=int, default=17)
    parser.add_argument("--channel-start", type=int, default=0)
    parser.add_argument("--channel-count", type=int, default=64)
    parser.add_argument("--facet-center", default="606.5,2156.5")
    parser.add_argument("--image-reference-pixel", type=float, default=2048.0)
    parser.add_argument("--cell-arcsec", type=float, default=0.6)
    parser.add_argument("--dataset-archive-sha256", required=True)
    args = parser.parse_args()
    try:
        center = tuple(float(value) for value in args.facet_center.split(","))
    except ValueError as error:
        raise SystemExit("--facet-center must contain two comma-separated numbers") from error
    if len(center) != 2:
        raise SystemExit("--facet-center must contain two comma-separated numbers")
    try:
        receipt = read_bound(
            args.measurement_set,
            field_id=args.field,
            spw_start=args.spw_start,
            spw_end=args.spw_end,
            channel_start=args.channel_start,
            channel_count=args.channel_count,
            center_pixel=center,
            image_reference_pixel=args.image_reference_pixel,
            cell_arcsec=args.cell_arcsec,
            dataset_archive_sha256=args.dataset_archive_sha256,
        )
    except UvwBoundError as error:
        raise SystemExit(f"VLASS localized facet UVW bound: {error}") from error
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
