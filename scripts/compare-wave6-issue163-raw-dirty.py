#!/usr/bin/env python3
"""Compare Wave 6 #163 raw dirty CASA and casa-rs image products."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image as image_tool


PRODUCTS = [
    ("image", ".image", "Jy/beam"),
    ("image-pbcor", ".image.pbcor", "Jy/beam"),
    ("residual", ".residual", "Jy/beam"),
    ("psf", ".psf", ""),
    ("pb", ".pb", ""),
    ("weight", ".weight", ""),
    ("sumwt", ".sumwt", ""),
]


def read_image(path: Path) -> tuple[np.ndarray, np.ndarray]:
    ia = image_tool()
    ia.open(str(path))
    try:
        data = np.asarray(ia.getchunk(), dtype=np.float64)
        mask = np.asarray(ia.getchunk(getmask=True), dtype=bool)
    finally:
        ia.close()
    return data, mask


def channel_count(data: np.ndarray) -> int:
    if data.ndim >= 4:
        return data.shape[3]
    if data.ndim == 3:
        return data.shape[2]
    if data.ndim == 1:
        return data.shape[0]
    return 1


def channel_plane(data: np.ndarray, chan: int) -> np.ndarray:
    if data.ndim >= 4:
        return np.asarray(data[:, :, 0, chan])
    if data.ndim == 3:
        return np.asarray(data[:, :, chan])
    if data.ndim == 2:
        return np.asarray(data)
    if data.ndim == 1:
        return np.asarray([[data[chan]]])
    return np.asarray([[float(np.squeeze(data))]])


def finite_shared(
    casa_data: np.ndarray,
    casa_mask: np.ndarray,
    rust_data: np.ndarray,
    rust_mask: np.ndarray,
    chan: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    casa_plane = channel_plane(casa_data, chan)
    rust_plane = channel_plane(rust_data, chan)
    casa_valid = channel_plane(casa_mask, chan).astype(bool)
    rust_valid = channel_plane(rust_mask, chan).astype(bool)
    shared = (
        casa_valid
        & rust_valid
        & np.isfinite(casa_plane)
        & np.isfinite(rust_plane)
    )
    return casa_plane, rust_plane, shared


def metric_block(casa_plane: np.ndarray, rust_plane: np.ndarray, shared: np.ndarray) -> dict:
    if not np.any(shared):
        return {
            "shared_pixels": 0,
            "max_abs": None,
            "p99_abs": None,
            "rms_abs": None,
            "max_frac_peak": None,
            "p99_frac_peak": None,
            "rms_frac_peak": None,
            "top1pct_max_frac_peak": None,
            "top1pct_p99_frac_peak": None,
            "peak_abs_casa": None,
        }
    casa = casa_plane[shared]
    rust = rust_plane[shared]
    diff = rust - casa
    abs_diff = np.abs(diff)
    peak = max(float(np.nanmax(np.abs(casa))), 1.0e-30)
    top_threshold = float(np.nanpercentile(np.abs(casa), 99.0))
    top = shared & (np.abs(casa_plane) >= top_threshold)
    top_abs = np.abs(rust_plane[top] - casa_plane[top]) if np.any(top) else np.array([])
    return {
        "shared_pixels": int(shared.sum()),
        "max_abs": float(abs_diff.max()),
        "p99_abs": float(np.nanpercentile(abs_diff, 99.0)),
        "rms_abs": float(np.sqrt(np.nanmean(diff * diff))),
        "max_frac_peak": float(abs_diff.max() / peak),
        "p99_frac_peak": float(np.nanpercentile(abs_diff, 99.0) / peak),
        "rms_frac_peak": float(np.sqrt(np.nanmean(diff * diff)) / peak),
        "top1pct_max_frac_peak": float(top_abs.max() / peak) if top_abs.size else None,
        "top1pct_p99_frac_peak": (
            float(np.nanpercentile(top_abs, 99.0) / peak) if top_abs.size else None
        ),
        "peak_abs_casa": peak,
    }


def write_panel(
    outdir: Path,
    product_name: str,
    chan: int,
    casa_plane: np.ndarray,
    casa_mask: np.ndarray,
    rust_plane: np.ndarray,
    rust_mask: np.ndarray,
    units: str,
) -> str:
    shared_mask = ~(casa_mask & rust_mask)
    casa_ma = np.ma.array(casa_plane, mask=~casa_mask)
    rust_ma = np.ma.array(rust_plane, mask=~rust_mask)
    diff_ma = np.ma.array(rust_plane - casa_plane, mask=shared_mask)
    casa_values = casa_ma.compressed()
    diff_values = np.abs(diff_ma.compressed())
    if casa_values.size:
        vmin = float(np.nanpercentile(casa_values, 1.0))
        vmax = float(np.nanpercentile(casa_values, 99.0))
    else:
        vmin, vmax = 0.0, 1.0
    dmax = max(float(np.nanpercentile(diff_values, 99.0)), 1.0e-12) if diff_values.size else 1.0

    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.2), constrained_layout=True)
    image_artist = axes[0].imshow(
        casa_ma.T, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax
    )
    rust_artist = axes[1].imshow(
        rust_ma.T, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax
    )
    diff_artist = axes[2].imshow(
        diff_ma.T, origin="lower", cmap="RdBu_r", vmin=-dmax, vmax=dmax
    )
    axes[0].set_title("CASA C++")
    axes[1].set_title("casa-rs")
    axes[2].set_title("casa-rs - CASA")
    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    label = units or "pixel value"
    fig.colorbar(image_artist, ax=axes[0], fraction=0.046, pad=0.04, label=label)
    fig.colorbar(rust_artist, ax=axes[1], fraction=0.046, pad=0.04, label=label)
    fig.colorbar(diff_artist, ax=axes[2], fraction=0.046, pad=0.04, label=f"delta {label}")
    fig.suptitle(f"{product_name} channel {chan}")
    panel = outdir / f"{product_name}-chan{chan}-panel.png"
    fig.savefig(panel, dpi=140)
    plt.close(fig)
    return str(panel)


def parse_panel_channels(text: str | None) -> set[int] | None:
    if text is None:
        return None
    channels: set[int] = set()
    for part in text.split(","):
        value = part.strip()
        if not value:
            continue
        channels.add(int(value))
    return channels


def compare(
    outdir: Path,
    prefix: str,
    write_panels: bool = True,
    panel_channels: set[int] | None = None,
) -> dict:
    summary: dict[str, object] = {
        "issue": 163,
        "dataset": "alma/m100/band3-combine/raw/split-parity",
        "prefix": prefix,
        "products": {},
    }
    casa_prefix = outdir / "casa" / prefix
    rust_prefix = outdir / "rust" / prefix
    for product_name, suffix, units in PRODUCTS:
        casa_data, casa_mask = read_image(casa_prefix.with_name(casa_prefix.name + suffix))
        rust_data, rust_mask = read_image(rust_prefix.with_name(rust_prefix.name + suffix))
        if casa_data.shape != rust_data.shape:
            raise RuntimeError(f"{product_name} shape mismatch: {casa_data.shape} vs {rust_data.shape}")
        product_summary: dict[str, object] = {
            "shape": list(casa_data.shape),
            "valid_casa": int(casa_mask.sum()),
            "valid_rust": int(rust_mask.sum()),
            "mask_mismatch_pixels": int(np.count_nonzero(casa_mask != rust_mask)),
            "channels": {},
            "units": units,
        }
        for chan in range(channel_count(casa_data)):
            casa_plane, rust_plane, shared = finite_shared(
                casa_data, casa_mask, rust_data, rust_mask, chan
            )
            casa_valid = channel_plane(casa_mask, chan).astype(bool)
            rust_valid = channel_plane(rust_mask, chan).astype(bool)
            metrics = metric_block(casa_plane, rust_plane, shared)
            if write_panels and (panel_channels is None or chan in panel_channels):
                metrics["panel"] = write_panel(
                    outdir,
                    product_name,
                    chan,
                    casa_plane,
                    casa_valid,
                    rust_plane,
                    rust_valid,
                    units,
                )
            if casa_plane.size <= 16:
                metrics["casa_values"] = casa_plane.reshape(-1).tolist()
                metrics["rust_values"] = rust_plane.reshape(-1).tolist()
            product_summary["channels"][str(chan)] = metrics
        summary["products"][product_name] = product_summary
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("outdir", type=Path)
    parser.add_argument(
        "--prefix", default="M100_combine_CO_cube_dirty_2chan", help="CASA image prefix"
    )
    parser.add_argument(
        "--no-panels",
        action="store_true",
        help="write only JSON/README metrics, without per-channel PNG panels",
    )
    parser.add_argument(
        "--panel-channels",
        help="comma-separated channel indexes to render as PNG panels while still computing all metrics",
    )
    args = parser.parse_args()
    summary = compare(
        args.outdir,
        args.prefix,
        write_panels=not args.no_panels,
        panel_channels=parse_panel_channels(args.panel_channels),
    )
    summary_path = args.outdir / "combined-raw-dirty-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    readme = args.outdir / "README.md"
    readme.write_text(
        "# Wave 6 #163 Raw Dirty M100 Combine Evidence\n\n"
        f"Summary JSON: `{summary_path.name}`\n\n"
        "Panels compare CASA C++ and casa-rs products from the same split 12m+7m inputs. "
        "Each panel includes CASA C++, casa-rs, and difference images with colorbars.\n"
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
