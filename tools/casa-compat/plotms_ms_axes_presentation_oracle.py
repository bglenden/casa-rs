#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair static MeasurementSet axes, ranges and rendered labels through PlotMS."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import opened, plotms_launcher, reference, request
from plotms_ms_iterated_ordinates_oracle import compare_text_pages
from plotms_ms_iteration_axes_oracle import data_fixture


COLOR_NAMES = ("dd2255", "22aa55")
FILE_SUFFIXES = ("Scan1,2,3,4", "Scan5_2")


def ranges_fixture(root):
    vis, _ = data_fixture(root)
    with opened(vis, True) as table:
        scans = table.getcol("SCAN_NUMBER")
        shape = table.getcol("DATA").shape
        _, channel, row = np.indices(shape)
        variation = (channel + row / 64).astype(np.float32)
        for column, values in (
            ("DATA", 12 * scans + 2 * variation),
            ("CORRECTED_DATA", 25 * scans + 3 * variation),
            ("MODEL_DATA", 1j * (600 - 90 * scans - 4 * variation)),
        ):
            table.putcol(column, values.astype(np.complex64))
    return vis


def marker_spans(path):
    with Image.open(path) as image:
        assert image.size == (600, 450)
        colors = np.asarray(image.convert("RGB"))
    extents = []
    for marker in COLOR_NAMES:
        rgb = [int(marker[index:index + 2], 16) for index in (0, 2, 4)]
        y, x = np.where(np.all(colors == rgb, axis=-1))
        panels = []
        for row, col in ((0, 0), (0, 1), (1, 0), (1, 1)):
            keep = (2 * y // 450 == row) & (2 * x // 600 == col)
            if np.any(keep):
                panels.append((np.ptp(x[keep]), np.ptp(y[keep])))
        extents.append(panels)
    return extents


def compare_ranges(root, native):
    from casars.tasks import _plotms_plot

    vis = ranges_fixture(root)
    common = request(vis) | dict(
        xaxis="real", xdatacolumn="data", yaxis=["real", "imag"],
        ydatacolumn=["corrected", "model"], yaxislocation=["left", "right"],
        field="", spw="0:0~1", iteraxis="scan", exprange="all",
    )
    samples = compare_text_pages(
        root, native, "scans-all-ordinates", common | dict(gridrows=1, gridcols=5),
        tuple((3, str(scan), count) for scan, count in enumerate((6, 6, 6, 2, 4), 1)),
    )
    options = common | dict(
        gridrows=2, gridcols=2, expformat="png", width=600, height=450, dpi=100,
        customsymbol=[True, True], symbolcolor=list(COLOR_NAMES), symbolshape=["circle", "circle"],
        symbolsize=4, title="Selected ranges",
    )
    configure = _plotms_plot.configure
    pages = []

    def capture(figure, primary, document, arguments):
        configure(figure, primary, document, arguments)
        axes = primary if isinstance(primary, list) else [primary]
        page = []
        for axis in axes:
            cell = axis.get_subplotspec()
            sides = [a for a in figure.axes if a.get_subplotspec() == cell and a.lines]
            assert len(sides) == 2
            page.append((axis, sides))
        pages.append(page)

    last = {}
    limits = [-8, 85, 0, 700]
    shared = dict(xselfscale=True, yselfscale=True, xsharedaxis=True, ysharedaxis=True)
    cases = (
        ("independent", {}),
        ("x-self-scale", dict(xselfscale=True)),
        ("y-self-scale", dict(yselfscale=True)),
        ("shared-axes", shared),
        ("explicit-range", shared | dict(plotrange=limits)),
    )
    for name, controls in cases:
        arguments = options | controls
        expected, actual = (root / f"{name}-{engine}.png" for engine in ("casa", "native"))
        assert reference(**arguments, plotfile=str(expected)) is None
        pages.clear()
        with patch.object(_plotms_plot, "configure", capture):
            assert native(**arguments, plotfile=str(actual)) is None
        assert [len(page) for page in pages] == [4, 1]
        for path in (expected, actual):
            for suffix, count in zip(FILE_SUFFIXES, (4, 1), strict=True):
                spans = marker_spans(path.with_name(f"{path.stem}_{suffix}.png"))
                assert list(map(len, spans)) == [count, count], (name, path, spans)
        spans = np.array(marker_spans(expected.with_name(f"{expected.stem}_{FILE_SUFFIXES[0]}.png")))
        if name == "independent":
            last["independent"] = spans
        elif name in ("x-self-scale", "y-self-scale"):
            axis = int(name.startswith("y"))
            assert np.max(spans[..., axis] / last["independent"][..., axis]) < 0.45, spans
        check_panel_ranges(pages, samples, name, controls, limits)
    for dimension in ("x", "y"):
        for task in (reference, native):
            path = root / "noop.png"
            try:
                task(**options, **{f"{dimension}sharedaxis": True}, plotfile=str(path))
            except ValueError as error:
                assert str(error) == f"plots cannot share an {dimension}-axis unless they use the same {dimension}-axis scale."
            else:
                raise AssertionError(f"shared {dimension} without self-scale was accepted")
            assert not path.exists()
    print("shared ranges: five paired modes and two paired refusals", flush=True)


def check_panel_ranges(pages, samples, name, controls, limits):
    expected_labels = ("Real:corrected", "Imag:model")
    bounds = [[], [], []]
    panels = [panel for page in pages for panel in page]
    for number, ((primary, sides), points) in enumerate(zip(panels, samples, strict=True), 1):
        assert primary.get_title() == f"Selected ranges Scan: {number}"
        assert [a.yaxis.get_label_position() for a in sides] == ["left", "right"]
        for axis, series in zip(sides, np.split(points, 2), strict=True):
            np.testing.assert_allclose(axis.lines[0].get_xydata(), series, rtol=1.2e-5)
            np.testing.assert_array_less(axis.get_xlim()[0], max(series[:, 0]))
            np.testing.assert_array_less(min(series[:, 0]), axis.get_xlim()[1])
            np.testing.assert_array_less(axis.get_ylim()[0], max(series[:, 1]))
            np.testing.assert_array_less(min(series[:, 1]), axis.get_ylim()[1])
        for scale, values in zip(bounds, (primary.get_xlim(), *(a.get_ylim() for a in sides)), strict=True):
            scale.append(values)
        if not controls.get("xsharedaxis"):
            assert primary.get_xlabel() == "Real"
        if not controls.get("ysharedaxis"):
            assert tuple(a.get_ylabel() for a in sides) == expected_labels
    for axis, values in enumerate(bounds):
        dimension = "x" if axis == 0 else "y"
        count = 1 if controls.get(f"{dimension}selfscale") else 5
        assert len(set(values)) == count, (name, dimension, values)
    if name == "explicit-range":
        assert [values[0] for values in bounds] == [tuple(limits[:2]), tuple(limits[2:]), tuple(limits[2:])]
    else:
        assert bounds[1] != bounds[2], (name, bounds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native
    with chdir(root), plotms_launcher(args.app_run):
        compare_ranges(root, native)


if __name__ == "__main__":
    main()
