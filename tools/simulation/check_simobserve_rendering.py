#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Render complete native documents and compare their artists with CASA."""

from __future__ import annotations

import argparse
import gzip
import json
import mmap
import os
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np
from matplotlib import colors, pyplot as plt

from casars._plot_document import render_document
from simobserve_graphics_oracle import artists


def close(actual, expected, tolerance, label):
    a, e = np.asarray(actual), np.asarray(expected)
    assert a.shape == e.shape, f"{label}: {a.shape} != {e.shape}"
    assert np.all(np.isfinite(a)) and np.all(np.isfinite(e)), f"{label}: nonfinite data"
    difference = float(np.max(np.abs(a - e), initial=0))
    assert difference <= tolerance, f"{label}: {difference} > {tolerance}"
    return difference


def compare(actual, expected):
    assert len(actual) == len(expected), "panel/colour-scale count"
    maximum = 0.0
    for a, e in zip(actual, expected, strict=True):
        for name in ("title", "xlabel", "ylabel", "aspect"):
            assert a[name] == e[name], f"{name}: {a[name]!r} != {e[name]!r}"
        if "frame_on" in e:
            assert a["frame_on"] == e["frame_on"], "panel frame"
        for name in ("xlim", "ylim"):
            maximum = max(maximum, close(a[name], e[name], 1e-3, name))
        for name in ("lines", "images", "circles", "texts"):
            assert len(a[name]) == len(e[name]), f"{name}: full artist count"
        for line, reference in zip(a["lines"], e["lines"], strict=True):
            for name in ("x", "y"):
                maximum = max(maximum, close(line[name], reference[name], 1e-3, name))
            for name in ("marker", "linestyle"):
                assert line[name] == reference[name], name
            if "line_width" in reference:
                assert line["line_width"] == reference["line_width"], "line width"
            close(colors.to_rgba(line["color"]), colors.to_rgba(reference["color"]), 1e-12, "line colour")
        for image, reference in zip(a["images"], e["images"], strict=True):
            pixels, truth = np.asarray(image["pixels"]), np.asarray(reference["pixels"])
            assert pixels.shape == truth.shape and np.all(np.isfinite(pixels))
            nrms = float(np.linalg.norm(pixels - truth) / np.linalg.norm(truth))
            assert nrms <= 1e-3, f"full image NRMS {nrms}"
            close(image["extent"], reference["extent"], 1e-9, "image extent")
            close(image["clim"], reference["clim"], 1e-5, "colour limits")
            for name in ("origin", "interpolation", "cmap"):
                assert image[name] == reference[name], name
        for circle, reference in zip(a["circles"], e["circles"], strict=True):
            close(circle["center"], reference["center"], 1e-3, "circle centre")
            assert abs(circle["radius"] - reference["radius"]) <= 1e-9
            assert circle["fill"] == reference["fill"]
        for text, reference in zip(a["texts"], e["texts"], strict=True):
            close(text["position"], reference["position"], 1e-3, "text position")
            assert text["text"] == reference["text"]
    return maximum


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    with gzip.open(args.reference, "rt") as source:
        oracle = json.load(source)
    args.out.mkdir(parents=True, exist_ok=True)
    results = []
    for metadata in sorted(args.native.glob("*/plots.json")):
        plots = json.loads(metadata.read_text())
        with metadata.with_name("arrays.bin").open("rb") as source, mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as payload:
            for plot in plots:
                name = Path(plot["output_file"]).name
                truth = next(case["figures"][name] for case in oracle["cases"] if name in case["figures"])
                figure, _ = render_document(plot["document"], payload=payload)
                try:
                    maximum = compare(artists(figure), truth)
                    output = args.out / metadata.parent.name / name
                    output.parent.mkdir(exist_ok=True)
                    figure.savefig(output)
                    assert output.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
                    results.append({"name": name, "native_run": metadata.parent.name, "panels": len(truth), "maximum_coordinate_difference": maximum, "png": str(output)})
                finally:
                    plt.close(figure)
    assert results, "no complete native plot documents"
    (args.out / "receipt.json").write_text(json.dumps({"casa_version": oracle["casa_version"], "passed": len(results), "results": results}, indent=1) + "\n")
    print(f"{len(results)} native figures agree with independent CASA artists and save as PNG")


if __name__ == "__main__":
    main()
