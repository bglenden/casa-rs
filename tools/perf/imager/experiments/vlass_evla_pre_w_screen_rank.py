#!/usr/bin/env python3
"""Reduce exact CASA EVLA pre-W screens into bounded rank evidence.

The forward family is assessed only on the requested output-image footprint,
including the largest multiscale support. The wideband normal/product family
is assessed wherever its amplitude reaches the configured PB limit. Both
families are reconstructed independently, matching their distinct runtime
roles in a screen-separated RIME implementation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-evla-pre-w-screen-rank/v1"
SOURCE_SCHEMA = "casa-rs-vlass-evla-pre-w-screens/v1"
ARCSEC_TO_RAD = math.pi / (180.0 * 3600.0)
DEFAULT_IMAGE_SIDE = 4096
DEFAULT_CELL_ARCSEC = 0.6
DEFAULT_SCALE_PIXELS = 12
DEFAULT_PB_LIMIT = 1.0e-4
RMS_LIMIT = 2.0e-5
MAX_LIMIT = 2.0e-4
WORST_STATE_RMS_LIMIT = 6.0e-5
PREFERRED_RANK = {"forward": 4, "normal": 6}
CONDITIONAL_RANK = {"forward": 8, "normal": 10}


class RankError(RuntimeError):
    """The screen-rank evidence is malformed or violates its contract."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RankError(f"cannot read screen manifest {path}: {error}") from error
    if payload.get("schema") != SOURCE_SCHEMA:
        raise RankError(f"screen manifest must use schema {SOURCE_SCHEMA}")
    states = payload.get("states")
    crop_shape = payload.get("crop_shape")
    full_shape = payload.get("full_shape")
    crop_start = payload.get("crop_start")
    if not isinstance(states, list) or not states:
        raise RankError("screen manifest states must be a non-empty list")
    for label, value in (
        ("crop_shape", crop_shape),
        ("full_shape", full_shape),
        ("crop_start", crop_start),
    ):
        if (
            not isinstance(value, list)
            or len(value) != 2
            or not all(isinstance(item, int) for item in value)
        ):
            raise RankError(f"screen manifest {label} must contain two integers")
    if crop_shape[0] != crop_shape[1] or full_shape[0] != full_shape[1]:
        raise RankError("only square screen receipts are supported")
    return payload


def resolve_artifact(manifest_path: pathlib.Path, value: Any, label: str) -> pathlib.Path:
    if not isinstance(value, str) or not value:
        raise RankError(f"screen manifest {label} must be a non-empty path")
    path = pathlib.Path(value)
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.is_file():
        raise RankError(f"screen artifact does not exist: {path}")
    return path


def load_family(
    path: pathlib.Path, *, state_count: int, crop_side: int
) -> np.memmap:
    expected_bytes = state_count * crop_side * crop_side * np.dtype(np.complex64).itemsize
    actual_bytes = path.stat().st_size
    if actual_bytes != expected_bytes:
        raise RankError(
            f"{path} has {actual_bytes} bytes, expected exactly {expected_bytes}"
        )
    return np.memmap(
        path,
        dtype=np.complex64,
        mode="r",
        shape=(state_count, crop_side, crop_side),
    )


def forward_domain_mask(
    manifest: dict[str, Any],
    *,
    image_side: int,
    cell_arcsec: float,
    scale_pixels: int,
) -> np.ndarray:
    if image_side <= 0 or not math.isfinite(cell_arcsec) or cell_arcsec <= 0.0:
        raise RankError("image geometry must be finite and positive")
    if scale_pixels < 0:
        raise RankError("scale_pixels must be non-negative")
    increments = manifest.get("derived_sky_increment_rad")
    reference = manifest.get("uv_reference_pixel")
    crop_start = manifest["crop_start"]
    crop_shape = manifest["crop_shape"]
    if (
        not isinstance(increments, list)
        or len(increments) != 2
        or not all(isinstance(value, (int, float)) for value in increments)
        or not isinstance(reference, list)
        or len(reference) != 2
        or not all(isinstance(value, (int, float)) for value in reference)
    ):
        raise RankError("screen manifest lacks numeric increments/reference pixels")

    half_extent_rad = (
        (image_side / 2.0 + scale_pixels) * cell_arcsec * ARCSEC_TO_RAD
    )
    x_global = np.arange(crop_shape[0], dtype=np.float64) + crop_start[0]
    y_global = np.arange(crop_shape[1], dtype=np.float64) + crop_start[1]
    x_rad = np.abs((x_global - float(reference[0])) * float(increments[0]))
    y_rad = np.abs((y_global - float(reference[1])) * float(increments[1]))
    return (y_rad[:, None] <= half_extent_rad) & (
        x_rad[None, :] <= half_extent_rad
    )


def product_domain_mask(normal: np.ndarray, pb_limit: float) -> np.ndarray:
    if not math.isfinite(pb_limit) or pb_limit <= 0.0:
        raise RankError("pb_limit must be finite and positive")
    return np.max(np.abs(normal), axis=0) >= pb_limit


def _rank_metrics(
    family: np.ndarray,
    mask: np.ndarray,
    *,
    max_rank: int,
) -> dict[str, Any]:
    if family.ndim != 3 or mask.shape != family.shape[1:]:
        raise RankError("family and domain mask shapes are incompatible")
    if not np.any(mask):
        raise RankError("rank domain contains no pixels")
    values = np.asarray(family[:, mask], dtype=np.complex128)
    if not np.all(np.isfinite(values.real)) or not np.all(np.isfinite(values.imag)):
        raise RankError("screen family contains non-finite values")

    gram = values @ values.conj().T
    eigenvalues, eigenvectors = np.linalg.eigh(gram)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = np.maximum(eigenvalues[order].real, 0.0)
    eigenvectors = eigenvectors[:, order]
    total_energy = float(np.sum(np.abs(values) ** 2))
    if not math.isfinite(total_energy) or total_energy <= 0.0:
        raise RankError("screen family has no finite positive energy")
    row_energy = np.sum(np.abs(values) ** 2, axis=1)
    if np.any(row_energy <= 0.0):
        raise RankError("one or more screen states have zero domain energy")

    ranks: list[dict[str, Any]] = []
    for rank in range(1, min(max_rank, values.shape[0]) + 1):
        basis = eigenvectors[:, :rank]
        approximation = basis @ (basis.conj().T @ values)
        error = values - approximation
        error_energy = np.sum(np.abs(error) ** 2, axis=1)
        worst_state = int(np.argmax(error_energy / row_energy))
        ranks.append(
            {
                "rank": rank,
                "relative_rms": float(
                    math.sqrt(float(np.sum(error_energy)) / total_energy)
                ),
                "max_abs_error": float(np.max(np.abs(error))),
                "worst_state_relative_rms": float(
                    math.sqrt(float(error_energy[worst_state] / row_energy[worst_state]))
                ),
                "worst_state_index": worst_state,
                "retained_energy_fraction": float(
                    np.sum(eigenvalues[:rank]) / total_energy
                ),
            }
        )
    return {
        "states": int(values.shape[0]),
        "domain_pixels": int(values.shape[1]),
        "singular_values": [float(math.sqrt(value)) for value in eigenvalues],
        "ranks": ranks,
    }


def classify_family(metrics: dict[str, Any], family: str) -> dict[str, Any]:
    passing = [
        row
        for row in metrics["ranks"]
        if row["relative_rms"] <= RMS_LIMIT
        and row["max_abs_error"] <= MAX_LIMIT
        and row["worst_state_relative_rms"] <= WORST_STATE_RMS_LIMIT
    ]
    minimum_passing_rank = passing[0]["rank"] if passing else None
    preferred = (
        minimum_passing_rank is not None
        and minimum_passing_rank <= PREFERRED_RANK[family]
    )
    conditional = (
        minimum_passing_rank is not None
        and minimum_passing_rank <= CONDITIONAL_RANK[family]
    )
    return {
        "minimum_passing_rank": minimum_passing_rank,
        "preferred_rank_ceiling": PREFERRED_RANK[family],
        "conditional_rank_ceiling": CONDITIONAL_RANK[family],
        "preferred_gate_passed": preferred,
        "conditional_gate_passed": conditional,
        "limits": {
            "relative_rms": RMS_LIMIT,
            "max_abs_error": MAX_LIMIT,
            "worst_state_relative_rms": WORST_STATE_RMS_LIMIT,
        },
    }


def reduce_screens(
    manifest_path: pathlib.Path,
    *,
    image_side: int = DEFAULT_IMAGE_SIDE,
    cell_arcsec: float = DEFAULT_CELL_ARCSEC,
    scale_pixels: int = DEFAULT_SCALE_PIXELS,
    pb_limit: float = DEFAULT_PB_LIMIT,
    max_rank: int = 12,
) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    states = manifest["states"]
    crop_side = manifest["crop_shape"][0]
    forward_path = resolve_artifact(
        manifest_path, manifest.get("forward_path"), "forward_path"
    )
    normal_path = resolve_artifact(
        manifest_path, manifest.get("normal_path"), "normal_path"
    )
    forward = load_family(
        forward_path, state_count=len(states), crop_side=crop_side
    )
    normal = load_family(normal_path, state_count=len(states), crop_side=crop_side)

    forward_mask = forward_domain_mask(
        manifest,
        image_side=image_side,
        cell_arcsec=cell_arcsec,
        scale_pixels=scale_pixels,
    )
    normal_mask = product_domain_mask(normal, pb_limit)
    maximum_outside_normal = max(
        float(state["normal_outside_crop_peak"]) for state in states
    )
    if maximum_outside_normal >= pb_limit:
        raise RankError(
            "normal/product crop excludes values at or above the PB limit: "
            f"{maximum_outside_normal} >= {pb_limit}"
        )

    forward_metrics = _rank_metrics(forward, forward_mask, max_rank=max_rank)
    normal_metrics = _rank_metrics(normal, normal_mask, max_rank=max_rank)
    forward_gate = classify_family(forward_metrics, "forward")
    normal_gate = classify_family(normal_metrics, "normal")
    if not (
        forward_gate["conditional_gate_passed"]
        and normal_gate["conditional_gate_passed"]
    ):
        recommendation = "reject-screen-low-rank-and-pivot-to-rephased-facets"
    elif not (
        forward_gate["preferred_gate_passed"]
        and normal_gate["preferred_gate_passed"]
    ):
        recommendation = "conditional-screen-rank-proceed-only-to-12-atom-probe"
    else:
        recommendation = "promote-screen-rank-to-12-atom-semantic-probe"

    return {
        "schema": SCHEMA,
        "role": "production-inert-architecture-discriminator",
        "source_manifest": str(manifest_path),
        "source_manifest_sha256": sha256_file(manifest_path),
        "forward_sha256": sha256_file(forward_path),
        "normal_sha256": sha256_file(normal_path),
        "geometry": {
            "image_shape": [image_side, image_side],
            "cell_arcsec": cell_arcsec,
            "largest_scale_pixels": scale_pixels,
            "pb_limit": pb_limit,
            "crop_shape": manifest["crop_shape"],
            "derived_sky_increment_rad": manifest["derived_sky_increment_rad"],
            "maximum_normal_outside_crop": maximum_outside_normal,
        },
        "forward": {**forward_metrics, "gate": forward_gate},
        "normal": {**normal_metrics, "gate": normal_gate},
        "recommendation": recommendation,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--image-side", type=int, default=DEFAULT_IMAGE_SIDE)
    parser.add_argument("--cell-arcsec", type=float, default=DEFAULT_CELL_ARCSEC)
    parser.add_argument("--scale-pixels", type=int, default=DEFAULT_SCALE_PIXELS)
    parser.add_argument("--pb-limit", type=float, default=DEFAULT_PB_LIMIT)
    parser.add_argument("--max-rank", type=int, default=12)
    args = parser.parse_args()
    if args.max_rank <= 0:
        raise SystemExit("--max-rank must be positive")
    try:
        receipt = reduce_screens(
            args.manifest,
            image_side=args.image_side,
            cell_arcsec=args.cell_arcsec,
            scale_pixels=args.scale_pixels,
            pb_limit=args.pb_limit,
            max_rank=args.max_rank,
        )
    except RankError as error:
        raise SystemExit(f"VLASS EVLA pre-W screen rank: {error}") from error
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
