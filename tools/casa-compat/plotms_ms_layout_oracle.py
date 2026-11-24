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

from plotms_ms_columns_oracle import (
    casa, compare_text, copy_ms, fixture, opened, plotms_launcher, reference, request, rows,
)


def occupied_marker_cells(path, grid):
    with Image.open(path) as image:
        assert image.size == (500, 360), (path, image.size)
        pixels = np.asarray(image.convert("RGB"))
    y, x = np.nonzero(np.all(pixels == [0xdd, 0x22, 0x55], axis=-1))
    assert len(x) > 0, path
    return set(zip(y * grid[0] // 360, x * grid[1] // 500, strict=True))


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
            assert occupied_marker_cells(path, grid) == {(row, column)}, path
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


def compare_offset_scan_pages(root, native, vis, base, scan_count=4):
    from casars import _plot_document

    root = root / f"offset-{scan_count}-scans"
    root.mkdir()
    source = root / "scans.ms"
    copy_ms(vis, source)
    samples_per_scan = 12 // scan_count
    with opened(source, True) as table:
        time = table.getcol("TIME")
        integration = np.round((time - time.min()) / 10).astype(int)
        scans = (integration // samples_per_scan + 1).astype(np.int32)
        assert np.array_equal(np.unique(scans), np.arange(1, scan_count + 1))
        table.putcol("SCAN_NUMBER", scans)
        within = integration % samples_per_scan
        amplitude = np.where(scans % 2 == 1, 1 + within, 10 - within)
        data = table.getcol("DATA")
        table.putcol("DATA", np.broadcast_to(amplitude, data.shape).astype(data.dtype))

    controls = base | dict(vis=str(source), field="", spw="0:1", xaxis="time", xdatacolumn="",
                          yaxis="amp", ydatacolumn="data", iteraxis="scan", gridrows=1, gridcols=2,
                          rowindex=0, colindex=1, title="")
    complete = root / "all-scan-coordinates.casa.txt"
    assert reference(**(controls | dict(gridcols=scan_count, colindex=0)),
                     expformat="txt", plotfile=str(complete)) is None
    points = rows(complete)
    expected_coords = {scan: np.array([point[:2] for point in points if point[3] == str(scan)], dtype=float)
                       for scan in range(1, scan_count + 1)}
    for scan in expected_coords:
        index = np.arange(samples_per_scan)
        expected = 1 + index if scan % 2 else 10 - index
        np.testing.assert_array_equal(expected_coords[scan][:, 1], expected)

    expected, actual = (root / f"unshifted-current.{engine}.txt" for engine in ("casa", "native"))
    unshifted = controls | dict(colindex=0, exprange="current", expformat="txt")
    assert reference(**unshifted, plotfile=str(expected)) is None
    assert [point[3] for point in rows(expected)] == ["1"] * samples_per_scan + ["2"] * samples_per_scan
    assert native(**unshifted, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2 * samples_per_scan

    pages = (
        ("_Scan1", ((1, (0, 1)),)),
        ("_Scan2,3_2", ((2, (0, 0)), (3, (0, 1)))),
        ("_Scan4_3", ((4, (0, 0)),)),
    )
    if scan_count == 2:
        pages = (pages[0], ("_2", ((2, (0, 1)),)))
    render = _plot_document.render_document
    rendered = []

    def record(*args, **kwargs):
        figure, axes = render(*args, **kwargs)
        page = []
        for axis in figure.axes:
            slot = axis.get_subplotspec()
            assert slot.get_gridspec().get_geometry() == (1, 2)
            page.append(((slot.rowspan.start, slot.colspan.start), axis.lines[0].get_xydata()))
        rendered.append(page)
        return figure, axes

    for export, plan in (("current", pages[:1]), ("all", pages)):
        expected, actual = (root / f"offset-{export}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, exprange=export, expformat="txt", plotfile=str(expected)) is None
        assert [point[3] for point in rows(expected)] == ["1"] * samples_per_scan + ["2"] * samples_per_scan
        assert native(**controls, exprange=export, expformat="txt", plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 2 * samples_per_scan

        rendered.clear()
        image = controls | dict(exprange=export, expformat="png", width=500, height=360, dpi=100)
        for engine, task in (("casa", reference), ("native", native)):
            stem = f"offset-{export}.{engine}"
            with patch.object(_plot_document, "render_document", record):
                assert task(**image, plotfile=str(root / f"{stem}.png")) is None
            outputs = {root / f"{stem}{suffix}.png" for suffix, _ in plan}
            assert set(root.glob(f"{stem}*.png")) == outputs
            for suffix, panels in plan:
                output = root / f"{stem}{suffix}.png"
                assert occupied_marker_cells(output, (1, 2)) == {cell for _, cell in panels}, output
        for page, (_, panels) in zip(rendered, plan, strict=True):
            for (slot, coordinates), (scan, expected_slot) in zip(page, panels, strict=True):
                assert slot == expected_slot
                np.testing.assert_allclose(coordinates[:, 0], expected_coords[scan][:, 0], rtol=0, atol=0.00051)
                np.testing.assert_array_equal(coordinates[:, 1], expected_coords[scan][:, 1])
        print(f"offset-{scan_count}-{export}: {2 * samples_per_scan} matching text points, "
              f"{len(plan)} matching image pages", flush=True)


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
        for scan_count in (4, 2):
            compare_offset_scan_pages(root, native, vis, base, scan_count)


if __name__ == "__main__":
    main()
