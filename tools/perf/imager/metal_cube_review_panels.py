#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Display first/middle/last restored planes; reuse the unchanged science reader."""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import SymLogNorm
import numpy as np

from t55_c_array_clean_gate import image_shape, read_masked_plane


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("metal", "cpu", "casa"):
        parser.add_argument(f"--{label}", required=True)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--first-channel", type=int, default=240)
    parser.add_argument("--channels", type=int, default=32)
    parser.add_argument("--product", choices=("image", "residual"), default="image")
    args = parser.parse_args()
    if args.panel.exists() or args.metrics.exists():
        raise FileExistsError("use fresh evidence paths")
    if args.first_channel < 0 or args.channels < 1 or args.first_channel + args.channels > 512:
        parser.error("channel range must lie within the existing 512-channel input")
    shapes = [image_shape(getattr(args, label) + "." + args.product)
              for label in ("metal", "cpu", "casa")]
    assert shapes[0] == shapes[1] == shapes[2] and shapes[0][3] == args.channels
    fig, axes = plt.subplots(3, 5, figsize=(20, 12), layout="constrained")
    summary = []
    for row, plane in enumerate((0, args.channels // 2, args.channels - 1)):
        images = [read_masked_plane(getattr(args, label) + "." + args.product, plane)
                  for label in ("metal", "cpu", "casa")]
        assert all(np.array_equal(images[0][1], image[1]) for image in images)
        metal, cpu, casa = [image[0] for image in images]
        valid = images[0][1]
        assert all(np.isfinite(image[0][valid]).all() for image in images)
        deltas = (metal - casa, cpu - casa)
        maximum = max(float(np.max(np.abs(image[0][valid]))) for image in images)
        difference_max = max(float(np.max(np.abs(delta[valid]))) for delta in deltas)
        shared = SymLogNorm(linthresh=max(maximum * 1e-4, 1e-12),
                            vmin=-maximum, vmax=maximum)
        difference = SymLogNorm(linthresh=max(difference_max * 1e-3, 1e-12),
                                vmin=-difference_max, vmax=difference_max)
        metrics = {"channel": args.first_channel + plane, "display_plane": plane}
        for name, delta in zip(("metal_minus_casa", "cpu_minus_casa"), deltas):
            rms = lambda value: float(np.sqrt(np.mean(np.square(value[valid], dtype=np.float64))))
            metrics[name] = {"normalized_rms": rms(delta) / rms(casa),
                             "maximum_absolute_difference_jy_beam": float(np.max(np.abs(delta[valid])))}
        summary.append(metrics)
        for column, (field, title) in enumerate(zip(
                (metal, cpu, casa, *deltas),
                ("Metal", "CPU W4", "CASA", "Metal − CASA", "CPU − CASA"))):
            ax = axes[row, column]
            artist = ax.imshow(np.ma.array(field, mask=~valid).T, origin="lower",
                               cmap="coolwarm", norm=shared if column < 3 else difference,
                               interpolation="nearest")
            ax.set_title(f"ch{args.first_channel + plane}: {title}")
            ax.set_xlabel("x pixel")
            ax.set_ylabel("y pixel")
            fig.colorbar(artist, ax=ax, fraction=0.04, label="Jy/beam")
    fig.suptitle(f"Full-row {args.channels}-channel {args.product} — shared image and difference scales per row")
    fig.savefig(args.panel, dpi=125)
    plt.close(fig)
    with args.metrics.open("x") as output:
        json.dump(summary, output, indent=2)
    print(json.dumps({"panel": str(args.panel), "metrics": str(args.metrics)}), flush=True)


if __name__ == "__main__":
    main()
