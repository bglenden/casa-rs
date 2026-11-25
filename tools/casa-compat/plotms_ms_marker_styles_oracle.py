#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair static MeasurementSet marker styles and connector controls."""

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
    with opened(vis, True) as table:
        corrected = table.getcol("CORRECTED_DATA")
        corrected.imag[:, 1::2, :] *= -1
        table.putcol("CORRECTED_DATA", corrected)
    controls = request(vis) | dict(
        xaxis="channel", xdatacolumn="", yaxis=["real", "imag"],
        ydatacolumn=["data", "corrected"], yaxislocation=["left", "right"],
        spw="0:0~3", customsymbol=[True, True], symbolsize=[7, 13],
        symbolshape=["circle", "square"], symbolcolor=["purple", "00aa44"],
        symbolfill=["fill", "mesh2"], symboloutline=[True, False],
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

    pinned = images(root, "sides-casa", reference, controls)
    with patch.object(_plotms_plot, "configure", capture):
        actual = images(root, "sides-native", native, controls)
    assert [side for side, _ in observed] == ["left", "right"]
    assert [line.get_marker() for _, line in observed] == ["o", "s"]
    assert [line.get_markersize() for _, line in observed] == [7, 13]
    assert [to_hex(line.get_markeredgecolor()) for _, line in observed] == ["#000000", "#00aa44"]
    assert to_hex(observed[0][1].get_markerfacecolor()) == "#800080"
    for pixels in (pinned, actual):
        for rgb in ([128, 0, 128], [0, 170, 68]):
            assert np.count_nonzero(np.max(np.abs(pixels - rgb), axis=-1) < 45) > 5
    source = np.array([row[:2] for row in rows(text[0])], dtype=float)
    for (_, line), points in zip(observed, np.split(source, 2), strict=True):
        np.testing.assert_allclose(line.get_xydata(), points, rtol=1.2e-5)
    print("independent ordinate styles retain 36 ordered coordinates, titles, marker sizes and outlines", flush=True)


def compare_color_groups(root, native):
    from casars.tasks import _plotms_plot
    from matplotlib.colors import to_hex

    root.mkdir()
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        times = table.getcol("TIME")
        scans = (1 + (np.searchsorted(np.unique(times), times) >= 3)).astype(np.int32)
        flags = table.getcol("FLAG")
        table.putcol("SCAN_NUMBER", scans)
        data = table.getcol("DATA")
        data += 16 * (scans == 2)
        table.putcol("DATA", data)
    controls = request(vis) | dict(
        xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="data",
        spw="0:1~3", correlation="XX,YY", showlegend=True,
        customflaggedsymbol=[True], flaggedsymbolshape=["diamond"],
        flaggedsymbolsize=[6], flaggedsymbolcolor=["dd2255"],
        flaggedsymbolfill=["fill"], flaggedsymboloutline=[False],
    )
    configure = _plotms_plot.configure
    figures = []

    def capture(figure, axis, doc, arguments):
        configure(figure, axis, doc, arguments)
        figures.append(axis)

    for dimension, field, groups, palette in (
        ("uncolored", None, {}, ([221, 34, 85],)),
        ("scan", 3, {"Scan 1": "1", "Scan 2": "2"}, ([224, 0, 102], [224, 118, 0])),
        ("corr", 12, {"XX": "XX", "YY": "YY"}, ([168, 104, 216], [224, 118, 0])),
    ):
        options = controls | dict(coloraxis=dimension if groups else "")
        text = [root / f"{dimension}-{engine}.txt" for engine in ("casa", "native")]
        for task, path in zip((reference, native), text, strict=True):
            assert task(**options, expformat="txt", plotfile=str(path)) is None
        assert compare_text(text[1], text[0]) == 36
        pixels = images(root, f"{dimension}-casa", reference, options)
        for color in palette:
            assert np.count_nonzero(np.max(np.abs(pixels - color), axis=-1) < 25) > 20
        with patch.object(_plotms_plot, "configure", capture):
            images(root, f"{dimension}-native", native, options)
        axis = figures.pop()
        selected = rows(text[0])
        assert sum(row[2] == "2" and row[12] == "XX" for row in selected) == 6
        clear_colors = set()
        for line in axis.lines:
            label = line.get_label()
            flagged = label.lower() == "flagged"
            if groups:
                assert label in groups and line.get_marker() == "o"
                clear_colors.add(line.get_color())
                points = [row for row in selected if row[field] == groups[label]]
            elif flagged:
                assert line.get_marker() == "D" and line.get_markersize() == 6
                assert to_hex(line.get_markerfacecolor()) == "#dd2255"
                points = [row for row in selected if row[2] == "2" and row[12] == "XX"]
            else:
                points = [row for row in selected if row[2] != "2" or row[12] != "XX"]
            coords = np.array([row[:2] for row in points], dtype=float)
            np.testing.assert_allclose(line.get_xydata(), coords, rtol=1.2e-5)
        assert len(axis.lines) == 2 and len(clear_colors) == len(groups)
        if groups:
            assert {label.get_text() for label in axis.get_legend().get_texts()} == set(groups)
        print(f"{dimension}: 36 ordered coordinates include 30 clear and 6 flagged points", flush=True)
    with opened(vis) as table:
        np.testing.assert_array_equal(table.getcol("FLAG"), flags)


def compare_connectors(root, native):
    from casars.tasks import _plotms_plot

    root.mkdir()
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        time = table.getcol("TIME")
        epochs, slots = np.unique(time, return_inverse=True)
        reverse = epochs[-1 - slots]
        table.putcol("TIME", reverse)
        table.putcol("TIME_CENTROID", reverse)
        data = table.getcol("DATA")
        phase = np.asarray([-145, 40, 130, -30, 75, -85, -135, 60, 150, -20, 110, -60])
        data *= np.exp(1j * np.deg2rad(phase[slots]))
        table.putcol("DATA", data)
        flags = table.getcol("FLAG")
        flags[:, :, slots == 2] = True
        table.putcol("FLAG", flags)
    controls = request(vis) | dict(
        xaxis="phase", xdatacolumn="data", yaxis="real", ydatacolumn="corrected",
        correlation="XX,YY", spw="0:1~3", coloraxis="corr",
    )
    cases = (
        ("blank", dict(xconnector="")),
        ("line", dict(xconnector="line")),
        ("step", dict(xconnector="step")),
        ("time-only", dict(xconnector="", timeconnector=True)),
        ("line-time", dict(xconnector="line", timeconnector=True)),
        ("step-time", dict(xconnector="step", timeconnector=True)),
    )
    pinned = {}
    for name, extra in cases:
        path = root / f"{name}-casa.txt"
        assert reference(**(controls | extra), expformat="txt", plotfile=str(path)) is None
        pinned[name] = (path, images(root, f"{name}-casa", reference, controls | extra))
    blank, pinned_blank = pinned["blank"]
    points = rows(blank)
    assert len(points) == 25
    coords = np.array([p[:2] for p in points], dtype=float)
    assert np.any(np.diff(coords[:, 0]) > 0) and np.any(np.diff(coords[:, 0]) < 0)
    assert len({p[9] for p in points}) == 5 and {p[12] for p in points} == {"XX", "YY"}
    for path, raster in pinned.values():
        assert path.read_bytes() == blank.read_bytes()
        np.testing.assert_array_equal(raster, pinned_blank)

    configure = _plotms_plot.configure

    def capture(figure, axis, document, options):
        configure(figure, axis, document, options)
        assert len(axis.lines) == 2
        for line in axis.lines:
            assert line.get_linestyle() in ("None", "", " ")
            selected = coords[[p[12] == line.get_label() for p in points]]
            np.testing.assert_allclose(line.get_xydata(), selected, rtol=1.2e-5)

    baseline = None
    for name, extra in cases:
        path = root / f"{name}-native.txt"
        assert native(**(controls | extra), expformat="txt", plotfile=str(path)) is None
        assert compare_text(path, pinned[name][0]) == len(points)
        with patch.object(_plotms_plot, "configure", capture):
            raster = images(root, f"{name}-native", native, controls | extra)
        if baseline is None:
            baseline = raster
        else:
            np.testing.assert_array_equal(raster, baseline)
    print(f"MeasurementSet connector modes: six pairs retain {len(points)} samples and draw no links", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("mesh", "ordinates", "groups", "connectors"))
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
        if args.only in (None, "groups"):
            compare_color_groups(root / "groups", native)
        if args.only in (None, "connectors"):
            compare_connectors(root / "connectors", native)


if __name__ == "__main__":
    main()
