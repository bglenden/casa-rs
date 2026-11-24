#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare static MeasurementSet PlotMS page placement with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import casa, compare_text, fixture, plotms_launcher, reference, request, rows


def check_markers_in_cell(path, row, column, grid):
    with Image.open(path) as image:
        assert image.size == (500, 360), (path, image.size)
        pixels = np.asarray(image.convert("RGB"))
    y, x = np.nonzero(np.all(pixels == [0xdd, 0x22, 0x55], axis=-1))
    assert len(x) > 0, path
    assert np.all(x * grid[1] // 500 == column) and np.all(y * grid[0] // 360 == row), (path, x, y)


def compare_cell(root, native, base, row, column):
    from casars.tasks import _plotms_plot

    name = f"grid-{row}-{column}"
    controls = base | dict(rowindex=row, colindex=column)
    grid = (controls["gridrows"], controls["gridcols"])
    expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, expformat="txt", plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 12
    assert {(point[2], point[12]) for point in selected} == {("1", "XX"), ("3", "XX")}
    assert native(**controls, expformat="txt", plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    coordinates = np.array([point[:2] for point in selected], dtype=float)

    configure = _plotms_plot.configure

    def record(figure, axis, document, options):
        configure(figure, axis, document, options)
        assert figure.axes == [axis]
        cell = axis.get_subplotspec()
        assert cell.get_gridspec().get_geometry() == grid
        assert (cell.rowspan.start, cell.colspan.start) == (row, column)
        assert axis.get_title() == controls["title"]
        assert not any(text.get_text() == controls["title"] for text in figure.texts)
        if "titlefont" in controls:
            assert axis.title.get_fontsize() == controls["titlefont"]
        np.testing.assert_allclose(axis.lines[0].get_xydata(), coordinates, rtol=1.2e-5, atol=1e-7)

    for page in ("current", "all"):
        expected, actual = (root / f"{name}-{page}.{engine}.png" for engine in ("casa", "native"))
        image = controls | dict(exprange=page, expformat="png", width=500, height=360, dpi=100)
        assert reference(**image, plotfile=str(expected)) is None
        with patch.object(_plotms_plot, "configure", side_effect=record) as presentation:
            assert native(**image, plotfile=str(actual)) is None
        presentation.assert_called_once()
        for path in (expected, actual):
            check_markers_in_cell(path, row, column, grid)
    print(f"{name}: 12 matching points in both page exports", flush=True)


def check_output_controls(root, native, base):
    for engine, task in (("casa", reference), ("native", native)):
        invalid = root / f"grid-invalid.{engine}.png"
        try:
            task(**base, rowindex=2, colindex=0, plotfile=str(invalid))
        except ValueError as error:
            assert "row/col index out of range" in str(error), error
        else:
            raise AssertionError(f"{engine} accepted an invalid grid cell")
        assert not invalid.exists()

        existing = root / f"grid-1-1-current.{engine}.png"
        before = existing.read_bytes()
        try:
            task(**(base | dict(overwrite=False)), rowindex=1, colindex=1, plotfile=str(existing))
        except Exception as error:
            assert "exist" in str(error).lower() or "overwrite" in str(error).lower(), error
        else:
            raise AssertionError(f"{engine} accepted an existing output with overwrite disabled")
        assert existing.read_bytes() == before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    calibrate = args.calibrate_bin or args.native_bin.with_name("calibrate")
    os.environ["CASARS_CALIBRATE_BIN"] = str(calibrate.resolve())
    from casars.tasks import plotms as native

    with chdir(root), plotms_launcher(args.app_run):
        vis, _ = fixture(root)
        base = request(vis) | dict(gridrows=2, gridcols=2, title="Selected visibility grid",
                                   customsymbol=True, symbolshape="circle", symbolcolor="dd2255",
                                   symbolsize=5, symbolfill="fill", symboloutline=False)
        for row, column in ((1, 1), (0, 0), (0, 1), (1, 0)):
            compare_cell(root, native, base, row, column)
        compare_cell(root, native, base | dict(gridcols=3, titlefont=14), 1, 2)
        check_output_controls(root, native, base)


if __name__ == "__main__":
    main()
