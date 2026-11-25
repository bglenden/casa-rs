#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair static MeasurementSet axes, ranges and rendered labels through PlotMS."""

import argparse
from contextlib import chdir
from datetime import datetime, timezone
from importlib.metadata import version
from itertools import combinations
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import (
    compare_text, fixture, opened, plotms_launcher, reference, request, rows, time_average_fixture,
)
from plotms_ms_iterated_ordinates_oracle import compare_text_pages
from plotms_ms_iteration_axes_oracle import data_fixture
from plotms_ms_scalar_time_oracle import combined_scalar_time_fixture


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
            ("DATA", 12 * scans + 2 * variation + 1j * (600 - 90 * scans - 4 * variation)),
            ("CORRECTED_DATA", 25 * scans + 3 * variation),
        ):
            table.putcol(column, values.astype(np.complex64))
    return vis


def marker_spans(path, panel_boxes=None):
    with Image.open(path) as image:
        assert image.size == (600, 450)
        colors = np.asarray(image.convert("RGB"), dtype=np.int16)
    split_x, split_y = 300, 225
    if "-casa_" in path.name:
        # In Qt the common external axes occupy part of the canvas. The grid
        # separators therefore move when either shared-axis flag is enabled.
        dark = np.all(colors < 32, axis=-1)
        split_x = 240 + np.argmax(dark[:, 240:360].sum(axis=0))
        split_y = 165 + np.argmax(dark[165:260, :].sum(axis=1))
    extents = []
    for marker in COLOR_NAMES:
        rgb = [int(marker[index:index + 2], 16) for index in (0, 2, 4)]
        y, x = np.where(np.max(np.abs(colors - rgb), axis=-1) < 50)
        panels = []
        cells = (
            (((y >= split_y) == row) & ((x >= split_x) == col))
            for row, col in ((0, 0), (0, 1), (1, 0), (1, 1))
        ) if panel_boxes is None else (
            (x >= box.x0) & (x <= box.x1) & (450 - y >= box.y0) & (450 - y <= box.y1)
            for box in panel_boxes
        )
        for keep in cells:
            if np.any(keep):
                panels.append((np.ptp(x[keep]), np.ptp(y[keep])))
        extents.append(panels)
    return extents


def compare_ranges(root, native):
    from casars.tasks import _plotms_plot

    vis = ranges_fixture(root)
    common = request(vis) | dict(
        xaxis="real", xdatacolumn="data", yaxis=["real", "imag"],
        ydatacolumn=["corrected", "data"], yaxislocation=["left", "right"],
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
        figure.canvas.draw()
        axes = primary if isinstance(primary, list) else [primary]
        page = []
        for axis in axes:
            cell = axis.get_subplotspec()
            sides = [a for a in figure.axes if a.get_subplotspec() == cell and a.lines]
            assert len(sides) == 2
            page.append((axis, sides))
        check_grid_labels(figure, page)
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
        elif name == "shared-axes":
            assert np.max(spans / last["independent"]) < 0.45, spans
        check_panel_ranges(pages, samples, name, controls, limits)
    for dimension in ("x", "y"):
        article = "an" if dimension == "x" else "a"
        for task in (reference, native):
            path = root / "noop.png"
            try:
                task(**options, **{f"{dimension}sharedaxis": True}, plotfile=str(path))
            except ValueError as error:
                assert str(error) == f"plots cannot share {article} {dimension}-axis unless they use the same {dimension}-axis scale."
            else:
                raise AssertionError(f"shared {dimension} without self-scale was accepted")
            assert not path.exists()
    single = options | shared | dict(yaxis="real", ydatacolumn="corrected", yaxislocation="left",
                                     customsymbol=True, symbolcolor="dd2255", symbolshape="circle")
    single_pages = []

    def capture_single(figure, primary, document, arguments):
        configure(figure, primary, document, arguments)
        axes = primary if isinstance(primary, list) else [primary]
        for index, axis in enumerate(axes):
            cell = axis.get_subplotspec()
            assert cell.get_gridspec().get_geometry() == (2, 2)
            assert (cell.rowspan.start, cell.colspan.start) == divmod(index, 2)
        single_pages.append([axis.get_window_extent() for axis in axes])

    assert reference(**single, plotfile=str(root / "single-casa.png")) is None
    with patch.object(_plotms_plot, "configure", capture_single):
        assert native(**single, plotfile=str(root / "single-native.png")) is None
    for suffix, count, boxes in zip(FILE_SUFFIXES, (4, 1), single_pages, strict=True):
        assert len(boxes) == count
        assert list(map(len, marker_spans(root / f"single-casa_{suffix}.png"))) == [count, 0]
        assert list(map(len, marker_spans(root / f"single-native_{suffix}.png", boxes))) == [count, 0]
    print("shared ranges: five paired modes, a single ordinate and two paired refusals", flush=True)


def check_panel_ranges(pages, samples, name, controls, limits):
    expected_labels = ("Real:corrected", "Imag")
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
    for page in pages:
        for primary, sides in page:
            cell = primary.get_subplotspec()
            bottom = len(page) == 1 or cell.is_last_row()
            x_visible = not controls.get("xsharedaxis") or bottom
            assert primary.get_xlabel() == ("Real" if x_visible else "")
            assert bool(primary.get_xticklabels()) == x_visible
            edges = (cell.is_first_col(), cell.is_last_col()) if len(page) > 1 else (True, True)
            for axis, label, edge in zip(sides, expected_labels, edges, strict=True):
                y_visible = not controls.get("ysharedaxis") or edge
                assert axis.get_ylabel() == (label if y_visible else "")
                assert bool(axis.get_yticklabels()) == y_visible
    for axis, values in enumerate(bounds):
        dimension = "x" if axis == 0 else "y"
        count = 1 if controls.get(f"{dimension}selfscale") else 5
        assert len(set(values)) == count, (name, dimension, values)
    if name == "explicit-range":
        assert [values[0] for values in bounds] == [tuple(limits[:2]), tuple(limits[2:]), tuple(limits[2:])]
    else:
        assert bounds[1] != bounds[2], (name, bounds)
        for dimension, expected, measured in zip("xyy", ((10, 70), (20, 140), (100, 550)), bounds, strict=True):
            if controls.get(f"{dimension}selfscale"):
                assert measured == [expected] * 5, (name, dimension, measured)


def check_grid_labels(figure, page):
    label_boxes = []
    for axis, sides in page:
        texts = [axis.title]
        for direction in (axis.xaxis, *(side.yaxis for side in sides)):
            texts.append(direction.label)
            for tick in direction._update_ticks():
                texts.extend((tick.label1, tick.label2))
        boxes = [text.get_window_extent() for text in texts if text.get_visible() and text.get_text()]
        assert all(figure.bbox.contains(*box.p0) and figure.bbox.contains(*box.p1) for box in boxes)
        label_boxes.append(boxes)
    for first, second in combinations(label_boxes, 2):
        assert all(not a.overlaps(b) for a in first for b in second)


def paired_axis(root, native, name, controls, dimension, expected, ticks=()):
    from casars.tasks import _plotms_plot

    text = [root / f"{name}-{engine}.txt" for engine in ("casa", "native")]
    images = [root / f"{name}-{engine}.png" for engine in ("casa", "native")]
    paint = dict(expformat="png", width=760, height=480, dpi=100,
                 customsymbol=True, symbolshape="circle", symbolcolor="dd2255", symbolsize=5)
    configure = _plotms_plot.configure
    captured = []

    def record(figure, axis, document, options):
        configure(figure, axis, document, options)
        figure.canvas.draw()
        captured.append(axis)

    for task, path in zip((reference, native), text, strict=True):
        assert task(**controls, expformat="txt", plotfile=str(path)) is None
    count = compare_text(text[1], text[0])
    assert reference(**(controls | paint), plotfile=str(images[0])) is None
    with patch.object(_plotms_plot, "configure", record):
        assert native(**(controls | paint), plotfile=str(images[1])) is None
    [axis] = captured
    values = np.array([row[:2] for row in rows(text[0])], dtype=float)
    np.testing.assert_allclose(axis.lines[0].get_xydata(), values, rtol=1.2e-5)
    assert getattr(axis, f"get_{dimension}label")() == expected
    label_bounds = getattr(axis, f"{dimension}axis").label.get_window_extent()
    assert axis.figure.bbox.contains(*label_bounds.p0) and axis.figure.bbox.contains(*label_bounds.p1)
    assert axis.get_title() == "" and axis.figure._suptitle.get_text() == controls["title"]
    for path in images:
        with Image.open(path) as image:
            assert image.size == (760, 480)
            assert np.count_nonzero(np.all(np.asarray(image.convert("RGB")) == [221, 34, 85], axis=-1)) > 0
    if ticks:
        index = int(dimension == "y")
        np.testing.assert_allclose(axis.lines[0].get_xydata()[:, index], values[:, index], rtol=0, atol=0.00051)
        formatter = getattr(axis, f"{dimension}axis").get_major_formatter()
        for timestamp, label in ticks:
            assert formatter(timestamp) == label, (timestamp, formatter(timestamp), label)
        limits = getattr(axis, f"get_{dimension}lim")()
        np.testing.assert_allclose(limits, [min(values[:, index]) - 1, max(values[:, index]) + 1],
                                   rtol=0, atol=0.00051)
    print(f"{name}: {count} paired coordinates and rendered {dimension.upper()} label {expected!r}", flush=True)


def compare_clock(root, native):
    root.mkdir()
    vis, _ = time_average_fixture(root)
    base = request(vis) | dict(xaxis="time", yaxis="amp", ydatacolumn="data", spw="0:1",
                               title="Clock presentation")
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    midnight = (datetime(2020, 8, 22, tzinfo=timezone.utc) - epoch).total_seconds()
    date_label = "Time (from 2020/08/22) (hh:mm:ss)"
    for name, start, settings, dimension, label, ticks in (
        ("same-day-clock", 33_315, {}, "x", date_label, ((33_320, "09:15:20"),)),
        ("midnight-clock-x", 86_360, {}, "x", date_label, ((86_410, "24:00:10"),)),
        ("midnight-clock-y", 86_360, dict(xaxis="amp", yaxis="time", ydatacolumn=""), "y", date_label,
         ((86_390, "2020/08/22\n23:59:50"), (86_410, "2020/08/23\n00:00:10"))),
        ("custom-clock", 86_360, dict(xlabel="Elapsed UTC"), "x", "Elapsed UTC", ((86_410, "24:00:10"),)),
        ("averaged-clock", 86_360, dict(averagedata=True, avgtime="28"), "x", f"Average {date_label}",
         ((86_410, "24:00:10"),)),
        ("first-average-next-day", 86_395, dict(averagedata=True, avgtime="28"), "x",
         "Average Time (from 2020/08/23) (hh:mm:ss)", ((86_410, "00:00:10"),)),
    ):
        with opened(vis, True) as table:
            times = table.getcol("TIME")
            offset = np.searchsorted(np.unique(times), times) * 15.0
            for column in ("TIME", "TIME_CENTROID"):
                table.putcol(column, midnight + start + offset)
        paired_axis(root, native, name, base | settings, dimension, label,
                    tuple((midnight + dt, text) for dt, text in ticks))


def compare_channel_labels(root, native):
    root.mkdir()
    vis, _, _ = combined_scalar_time_fixture(root)
    base = request(vis) | dict(xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="corrected",
                               spw="0:0~3", title="Binned visibility")
    averaged = dict(averagedata=True, avgchannel="2")
    for name, settings, label in (
        ("raw-channel", {}, "Channel"),
        ("binned-channel", averaged, "Average Channel"),
        ("custom-binned-channel", averaged | dict(xlabel="Selected samples"), "Average Selected samples"),
    ):
        paired_axis(root, native, name, base | settings, "x", label)


def compare_grid_style(root, native):
    from casars.tasks import _plotms_plot
    from matplotlib.colors import to_hex

    root.mkdir()
    vis, _ = fixture(root)
    base = request(vis) | dict(xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="data",
                               spw="0:0~3", correlation="XX,YY", coloraxis="corr",
                               title="Selected channels", titlefont=15,
                               xlabel="Channel ordinal", xaxisfont=13,
                               ylabel="Visibility amplitude", yaxisfont=12,
                               showlegend=True, expformat="png", width=500, height=360, dpi=100)
    configure = _plotms_plot.configure
    recorded = []

    def record(figure, axis, document, arguments):
        configure(figure, axis, document, arguments)
        figure.canvas.draw()
        recorded.append((figure, axis))

    for name, controls, position, grid, width, rgb in (
        ("major-grid", dict(showmajorgrid=True, majorstyle="solid", majorwidth=2,
                            majorcolor="red", plotrange=[-1, 4, 0, 10]),
         "lowerLeft", "-", 2, "ff0000"),
        ("minor-grid", dict(showmajorgrid=True, majorstyle="none", showminorgrid=True, minorstyle="dot", minorwidth=1,
                            minorcolor="#338844", plotrange=[-1, 4, 0, 0]),
         "exteriorRight", ":", 1, "338844"),
    ):
        expected, actual = (root / f"{name}-{engine}.png" for engine in ("casa", "native"))
        options = base | controls | dict(legendposition=position)
        assert reference(**options, plotfile=str(expected)) is None
        with patch.object(_plotms_plot, "configure", record):
            assert native(**options, plotfile=str(actual)) is None
        figure, axis = recorded.pop()
        assert axis.get_title() == ""
        assert figure._suptitle.get_text() == base["title"]
        assert figure._suptitle.get_fontsize() == base["titlefont"]
        assert axis.get_xlim() == (-1, 4)
        for coord in "xy":
            ticks = getattr(axis, f"{coord}axis")
            assert ticks.label.get_text() == base[f"{coord}label"]
            assert ticks.label.get_fontsize() == base[f"{coord}axisfont"]
            label_box = ticks.label.get_window_extent()
            assert figure.bbox.contains(*label_box.p0) and figure.bbox.contains(*label_box.p1)
            ticklist = ticks.get_major_ticks() if name == "major-grid" else ticks.get_minor_ticks()
            active = [tick.gridline for tick in ticklist if tick.gridline.get_visible()]
            assert active
            assert all(line.get_linestyle() == grid and line.get_linewidth() == width
                       and to_hex(line.get_color()) == "#" + rgb for line in active)
        if name == "major-grid":
            assert axis.get_ylim() == (0, 10)
        else:
            points = np.concatenate([line.get_ydata() for line in axis.lines])
            lo, hi = axis.get_ylim()
            assert 0 < lo <= float(min(points)) < float(max(points)) <= hi < 10
        legend = axis.get_legend()
        assert {text.get_text() for text in legend.get_texts()} == {"XX", "YY"}
        legend_bbox = legend.get_window_extent()
        if position == "exteriorRight":
            assert axis.bbox.xmax <= legend_bbox.xmin < legend_bbox.xmax <= figure.bbox.xmax
            assert 0 <= axis.bbox.ymax - legend_bbox.ymax <= 14
        else:
            assert axis.bbox.contains(*legend_bbox.p0)
            assert axis.bbox.contains(*legend_bbox.p1)
            assert legend_bbox.x0 < axis.bbox.x0 + axis.bbox.width * 0.1
            assert legend_bbox.y0 < axis.bbox.y0 + axis.bbox.height * 0.1
        for path in (expected, actual):
            with Image.open(path) as image:
                assert image.size == (500, 360)
                pixels = np.asarray(image.convert("RGB"), dtype=np.int16)
            color = [int(rgb[i:i + 2], 16) for i in (0, 2, 4)]
            count = np.count_nonzero(np.max(np.abs(pixels - color), axis=2) < 35)
            if name == "major-grid" and path == expected:
                # The pinned task renders its gray major pen even with an
                # admitted named color. The native artist honors the request.
                assert count == 0
                grid_pixels = pixels[40:295, 70:450]
                assert np.count_nonzero(np.all(grid_pixels == [128, 128, 128], axis=-1)) > 1000
            else:
                assert count > 100, path
    for task, name in ((reference, "casa"), (native, "native")):
        path = root / f"malformed-{name}.png"
        try:
            task(**base, plotrange=[1, 2, 3], plotfile=str(path))
        except ValueError as error:
            assert str(error) == "plotrange parameter has incorrect number of elements."
        else:
            raise AssertionError(f"{name} accepted an invalid plot range")
        assert not path.exists()
    print("grid styles: paired solid major and dotted minor grids, range, label/font and legend controls", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("ranges", "clock", "channel", "style"))
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native
    with chdir(root), plotms_launcher(args.app_run):
        if args.only in (None, "ranges"):
            compare_ranges(root, native)
        if args.only in (None, "clock"):
            compare_clock(root / "clock", native)
        if args.only in (None, "channel"):
            compare_channel_labels(root / "channel", native)
        if args.only in (None, "style"):
            compare_grid_style(root / "style", native)


if __name__ == "__main__":
    main()
