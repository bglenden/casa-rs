#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair static MeasurementSet point styles and mesh fill categories."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import compare_text, fixture, opened, plotms_launcher, reference, request, rows


def images(root, name, task, controls):
    path = root / f"{name}.png"
    assert task(**controls, width=500, height=360, dpi=100, expformat="png", plotfile=str(path)) is None
    with Image.open(path) as image:
        assert image.size == (500, 360)
        return np.asarray(image.convert("RGB"), dtype=np.int16)


def fill_density(pixels):
    ink = np.max(np.abs(pixels - [221, 34, 85]), axis=-1) < 50
    y, x = np.nonzero(ink)
    assert x.size and y.size
    return x.size / ((np.ptp(x) + 1) * (np.ptp(y) + 1))


def compare_meshes(root, native):
    root.mkdir()
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        time = table.getcol("TIME")
        table.putcol("SCAN_NUMBER", (1 + np.searchsorted(np.unique(time), time)).astype(np.int32))
    controls = request(vis) | dict(
        scan="1", xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="data",
        spw="0:1", customsymbol=True, symbolshape="square", symbolsize=24,
        symbolcolor="dd2255", symboloutline=False, plotrange=[0, 2, 0, 4],
    )
    text = [root / f"mesh-{engine}.txt" for engine in ("casa", "native")]
    for task, path in zip((reference, native), text, strict=True):
        assert task(**controls, expformat="txt", plotfile=str(path)) is None
    assert compare_text(text[1], text[0]) == 1
    assert tuple(float(value) for value in rows(text[0])[0][:2]) == (1, 2)
    for task, engine in ((reference, "casa"), (native, "native")):
        fills = tuple(fill_density(images(root, f"{kind}-{engine}", task, controls | dict(symbolfill=kind)))
                      for kind in ("fill", "mesh1", "mesh2", "mesh3", "nofill"))
        solid, dense, half, sparse, ring = fills
        assert solid > 0.95
        assert solid > dense > 0.76
        assert 0.47 < half < 0.71
        assert 0.17 < sparse < 0.39
        assert dense > half > sparse > ring
        print(f"{engine}: distinct solid and mesh coverage {fills}", flush=True)


def compare_ordinate_styles(root, native):
    from casars.tasks import _plotms_plot
    from matplotlib.colors import to_hex

    root.mkdir()
    vis, _ = fixture(root)
    controls = request(vis) | dict(
        xaxis="channel", xdatacolumn="", yaxis=["real", "imag"],
        ydatacolumn=["data", "corrected"], yaxislocation=["left", "right"],
        spw="0:0~3", customsymbol=[True, True], symbolsize=[7, 13],
        symbolshape=["circle", "square"], symbolcolor=["purple", "00aa44"],
        symbolfill=["nofill", "mesh2"], symboloutline=[True, False],
    )
    text = [root / f"side-markers-{engine}.txt" for engine in ("casa", "native")]
    for task, path in zip((reference, native), text, strict=True):
        assert task(**controls, expformat="txt", plotfile=str(path)) is None
    assert compare_text(text[1], text[0]) == 36
    configure = _plotms_plot.configure
    observed = []

    def capture(figure, axes, document, options):
        configure(figure, axes, document, options)
        observed.extend((axis.yaxis.get_label_position(), axis.lines[0]) for axis in figure.axes)

    images(root, "sides-casa", reference, controls)
    with patch.object(_plotms_plot, "configure", capture):
        images(root, "sides-native", native, controls)
    assert [side for side, _ in observed] == ["left", "right"]
    assert [line.get_marker() for _, line in observed] == ["o", "s"]
    assert [line.get_markersize() for _, line in observed] == [7, 13]
    assert [to_hex(line.get_markeredgecolor()) for _, line in observed] == ["#000000", "#00aa44"]
    assert observed[0][1].get_markerfacecolor() == "none"
    source = np.array([row[:2] for row in rows(text[0])], dtype=float)
    for (_, line), points in zip(observed, np.split(source, 2), strict=True):
        np.testing.assert_allclose(line.get_xydata(), points, rtol=1.2e-5)
    print("independent ordinate styles retain 36 ordered coordinates, titles, marker sizes and outlines", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("mesh", "ordinates"))
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native
    with chdir(root), plotms_launcher(args.app_run):
        if args.only in (None, "mesh"):
            compare_meshes(root / "mesh", native)
        if args.only in (None, "ordinates"):
            compare_ordinate_styles(root / "ordinates", native)


if __name__ == "__main__":
    main()
