#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Review full-field and beam-scale differences for C-array CLEAN alerts."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import SymLogNorm
from matplotlib.patches import Circle, Rectangle
import numpy as np

from t55_c_array_clean_gate import CLEAN_RADIUS_ARCSEC, CELL_ARCSEC, read_masked_plane


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--science", type=Path, required=True)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    args = parser.parse_args()
    if args.panel.exists() or args.metrics.exists():
        raise FileExistsError("review evidence must have fresh output paths")
    science = json.loads(args.science.read_text())
    rows = [row for row in science["channels"]
            if row.get("review_triggers", row.get("failed", []))]
    if not rows:
        raise ValueError("science report has no numerical-review channels")
    size = 1024
    pixels = np.arange(size)
    x, y = np.meshgrid(pixels, pixels, indexing="ij")
    clean = np.hypot(x - size / 2, y - size / 2) * CELL_ARCSEC <= CLEAN_RADIUS_ARCSEC
    fig, axes = plt.subplots(len(rows), 3, figsize=(13, max(3, 2.7 * len(rows))),
                             squeeze=False, layout="constrained")
    difference_norm = SymLogNorm(linthresh=10, vmin=-600, vmax=600)
    restored_norm = SymLogNorm(linthresh=5, vmin=-150, vmax=150)
    summary = []
    for index, row in enumerate(rows):
        plane = row["display_plane"]
        prefixes = row["prefixes"]
        residuals = {label: read_masked_plane(prefix + ".residual", plane)
                     for label, prefix in prefixes.items()}
        images = {label: read_masked_plane(prefix + ".image", plane)
                  for label, prefix in prefixes.items()}
        if (not np.array_equal(residuals["native"][1], residuals["casa"][1])
                or not np.array_equal(images["native"][1], images["casa"][1])):
            raise ValueError(f"channel {row['channel']} has unequal pixel-mask topology")
        residual_delta = residuals["native"][0] - residuals["casa"][0]
        image_delta = images["native"][0] - images["casa"][0]
        valid = clean & residuals["casa"][1]
        if not valid.any() or not np.isfinite(residual_delta).all() or not np.isfinite(image_delta).all():
            raise ValueError(f"channel {row['channel']} lacks finite CLEAN support")
        location = tuple(int(v) for v in np.unravel_index(
            np.argmax(np.where(valid, np.abs(residual_delta), -1)), residual_delta.shape))
        radius = 40
        x0, x1 = max(0, location[0] - radius), min(size, location[0] + radius + 1)
        y0, y1 = max(0, location[1] - radius), min(size, location[1] + radius + 1)
        cutout = np.s_[x0:x1, y0:y1]
        full = residual_delta * 1e6
        fields = (full.T, full[cutout].T, (image_delta[cutout] * 1e6).T)
        extents = (None, (x0 - 0.5, x1 - 0.5, y0 - 0.5, y1 - 0.5),
                   (x0 - 0.5, x1 - 0.5, y0 - 0.5, y1 - 0.5))
        for column, (field, extent) in enumerate(zip(fields, extents)):
            ax = axes[index, column]
            artist = ax.imshow(field, origin="lower", extent=extent, cmap="coolwarm",
                               norm=restored_norm if column == 2 else difference_norm,
                               interpolation="nearest")
            if column == 0:
                ax.add_patch(Circle((512, 512), CLEAN_RADIUS_ARCSEC / CELL_ARCSEC,
                                    edgecolor="black", fill=False, linestyle="--", linewidth=0.6))
                ax.add_patch(Rectangle((x0, y0), x1 - x0, y1 - y0,
                                       edgecolor="black", fill=False, linewidth=0.9))
            else:
                ax.plot(*location, marker="x", color="black", markersize=4)
            if index == 0:
                ax.set_title(("Residual difference: full field", "Residual difference: 81-pixel cutout",
                              "Restored-image difference: same cutout")[column])
            ax.set_ylabel(f"ch {row['channel']} y pixel" if column == 0 else "y pixel")
            if index == len(rows) - 1:
                ax.set_xlabel("x pixel")
            fig.colorbar(artist, ax=ax, fraction=0.04, label="µJy/beam")
        summary.append({
            "channel": row["channel"], "maximum_residual_difference_xy": location,
            "maximum_residual_difference_jy_beam": float(residual_delta[location]),
            "casa_residual_at_maximum_jy_beam": float(residuals["casa"][0][location]),
            "native_residual_at_maximum_jy_beam": float(residuals["native"][0][location]),
            "maximum_restored_difference_in_cutout_jy_beam": float(np.max(np.abs(image_delta[cutout]))),
            "cutout_xy": [x0, x1, y0, y1],
            "review_triggers": row.get("review_triggers", row.get("failed", [])),
        })
    fig.suptitle("T55 C-array CLEAN numerical review — casa-rs W4 minus CASA, shared scales")
    fig.savefig(args.panel, dpi=125)
    plt.close(fig)
    args.metrics.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"channels": [row["channel"] for row in summary],
                      "panel": str(args.panel), "metrics": str(args.metrics)}), flush=True)


if __name__ == "__main__":
    main()
