#!/usr/bin/env python3
"""Render a signed-log full-frame VLASS CASA/casa-rs image comparison panel."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import SymLogNorm
import numpy as np


def max_absolute_pool(data: np.memmap, factor: int) -> np.ndarray:
    """Downsample without averaging away beam-sized signed extrema."""
    nx, ny = data.shape
    pooled_nx = nx // factor
    pooled_ny = ny // factor
    output = np.empty((pooled_nx, pooled_ny), dtype=np.float64)
    trimmed_y = pooled_ny * factor
    for pooled_x in range(pooled_nx):
        x_start = pooled_x * factor
        block = np.asarray(
            data[x_start : x_start + factor, :trimmed_y],
            dtype=np.float64,
        )
        candidates = (
            block.reshape(factor, pooled_ny, factor)
            .transpose(1, 0, 2)
            .reshape(pooled_ny, factor * factor)
        )
        indices = np.argmax(np.abs(candidates), axis=1)
        output[pooled_x] = candidates[np.arange(pooled_ny), indices]
    return output


def symlog_norm(*planes: np.ndarray, linthresh: float) -> SymLogNorm:
    peak = max(float(np.nanmax(np.abs(plane))) for plane in planes)
    return SymLogNorm(
        linthresh=linthresh,
        linscale=1.0,
        vmin=-peak,
        vmax=peak,
        base=10,
    )


def render(
    workspace: Path,
    output: Path,
    shape: tuple[int, int],
    pool: int,
    image_linthresh: float,
    difference_linthresh: float,
    source_bounds: tuple[int, int, int, int],
) -> None:
    left = np.memmap(workspace / "left.f64", mode="r", dtype=np.float64, shape=shape)
    right = np.memmap(workspace / "right.f64", mode="r", dtype=np.float64, shape=shape)
    difference = np.memmap(
        workspace / "diff.f64",
        mode="r",
        dtype=np.float64,
        shape=shape,
    )

    full_left = max_absolute_pool(left, pool)
    full_right = max_absolute_pool(right, pool)
    full_difference = max_absolute_pool(difference, pool)
    full_image_norm = symlog_norm(
        full_left,
        full_right,
        linthresh=image_linthresh,
    )
    full_difference_norm = symlog_norm(
        full_difference,
        linthresh=difference_linthresh,
    )

    x0, x1, y0, y1 = source_bounds
    source_left = np.asarray(left[x0:x1, y0:y1])
    source_right = np.asarray(right[x0:x1, y0:y1])
    source_difference = np.asarray(difference[x0:x1, y0:y1])
    source_image_norm = symlog_norm(
        source_left,
        source_right,
        linthresh=image_linthresh,
    )
    source_difference_norm = symlog_norm(
        source_difference,
        linthresh=difference_linthresh,
    )

    fig, axes = plt.subplots(2, 3, figsize=(16, 10), constrained_layout=True)
    fig.suptitle(
        "VLASS 63-field raster patch: 12,150² .image.tt0 (signed logarithmic display)",
        fontsize=15,
    )
    titles = ("casa-rs", "CASA", "difference (casa-rs − CASA)")
    top_planes = (full_left, full_right, full_difference)
    top_norms = (full_image_norm, full_image_norm, full_difference_norm)
    bottom_planes = (source_left, source_right, source_difference)
    bottom_norms = (source_image_norm, source_image_norm, source_difference_norm)

    for column, (title, plane, norm) in enumerate(zip(titles, top_planes, top_norms)):
        artist = axes[0, column].imshow(
            plane.T,
            origin="lower",
            cmap="RdBu_r",
            norm=norm,
            interpolation="nearest",
            aspect="equal",
        )
        axes[0, column].set_title(title)
        axes[0, column].set_xlabel(f"display pixels (max-|value| {pool}×{pool} pooling)")
        axes[0, column].set_ylabel("display pixels")
        fig.colorbar(
            artist,
            ax=axes[0, column],
            fraction=0.046,
            pad=0.04,
            label="Jy/beam",
        )

    for column, (title, plane, norm) in enumerate(
        zip(titles, bottom_planes, bottom_norms)
    ):
        artist = axes[1, column].imshow(
            plane.T,
            origin="lower",
            cmap="RdBu_r",
            norm=norm,
            interpolation="nearest",
            aspect="equal",
        )
        axes[1, column].set_title(f"{title} — native 64×64 bright-source crop")
        axes[1, column].set_xlabel(f"x pixel + {x0}")
        axes[1, column].set_ylabel(f"y pixel + {y0}")
        fig.colorbar(
            artist,
            ax=axes[1, column],
            fraction=0.046,
            pad=0.04,
            label="Jy/beam",
        )

    fig.text(
        0.5,
        0.005,
        (
            f"Image SymLog linear threshold: {image_linthresh:.1e} Jy/beam; "
            f"difference threshold: {difference_linthresh:.1e} Jy/beam. "
            "Pooling is display-only and preserves the signed pixel with largest "
            "absolute value in each block."
        ),
        ha="center",
        fontsize=10,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--shape", default="12150x12150")
    parser.add_argument("--pool", type=int, default=12)
    parser.add_argument("--image-linthresh", type=float, default=2.0e-4)
    parser.add_argument("--difference-linthresh", type=float, default=1.0e-9)
    parser.add_argument("--source-bounds", default="6243:6307,6003:6067")
    args = parser.parse_args()

    shape = tuple(int(value) for value in args.shape.split("x", maxsplit=1))
    x_bounds, y_bounds = args.source_bounds.split(",", maxsplit=1)
    x0, x1 = (int(value) for value in x_bounds.split(":", maxsplit=1))
    y0, y1 = (int(value) for value in y_bounds.split(":", maxsplit=1))
    render(
        args.workspace,
        args.output,
        shape,
        args.pool,
        args.image_linthresh,
        args.difference_linthresh,
        (x0, x1, y0, y1),
    )


if __name__ == "__main__":
    main()
