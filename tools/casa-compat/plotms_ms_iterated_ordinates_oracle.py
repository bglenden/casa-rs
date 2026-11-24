#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare unaveraged PlotMS visibility ordinates across iterated MS panels."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_channel_ordinates_oracle import make_fixture
from plotms_ms_columns_oracle import compare_text, opened, plotms_launcher, reference, request


def compare_text_pages(root, native, name, controls, masks):
    expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, expformat="txt", plotfile=str(expected)) is None
    sections = [[]]
    for line in expected.read_text().splitlines():
        if line.startswith("# From plot") and "iteration" in line:
            sections.append([])
        elif line and not line.startswith("#"):
            sections[-1].append(line.split())
    ordinates = controls["yaxis"]
    for panel, (metadata_index, label, count) in zip(sections, masks, strict=True):
        assert len(panel) == len(ordinates) * count
        assert {p[metadata_index] for p in panel} == {label}
        assert all(int(p[2]) != 2 or p[12] == "XX" for p in panel)
    count = sum(map(len, sections))
    assert native(**controls, expformat="txt", plotfile=str(actual)) is None
    assert compare_text(actual, expected) == count
    print(f"{name}: {count} matching coordinates in {len(sections)} panels", flush=True)
    return [np.array([point[:2] for point in section], dtype=float) for section in sections]


def compare_figures(root, native, name, controls, pages, file_tags, sides):
    from casars import _plot_document

    captured = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, primary = render(*args, **kwargs)
        axes = primary if isinstance(primary, list) else [primary]
        panels = []
        for axis in axes:
            local_axes = [drawn for drawn in figure.axes if drawn.get_subplotspec() == axis.get_subplotspec()]
            panels.append((axis.get_title(), axis.get_xlabel(), [
                (drawn.yaxis.get_label_position(), drawn.get_ylabel(),
                 [line.get_xydata() for line in drawn.lines]) for drawn in local_axes
            ]))
        captured.append(panels)
        return figure, primary

    options = controls | dict(expformat="png", width=500, height=420, dpi=100)
    expected, actual = (root / engine for engine in ("casa", "native"))
    expected.mkdir(exist_ok=True)
    actual.mkdir(exist_ok=True)
    assert reference(**options, plotfile=str(expected / f"{name}.png")) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**options, plotfile=str(actual / f"{name}.png")) is None
    names = sorted(f"{name}_{suffix}.png" for suffix in file_tags)
    for folder in (expected, actual):
        assert sorted(path.name for path in folder.glob(f"{name}*.png")) == names
        for filename in names:
            with Image.open(folder / filename) as image:
                assert image.size == (500, 420)
    for rendered, page in zip(captured, pages, strict=True):
        for (title, xlabel, axes), (label, coordinates) in zip(rendered, page, strict=True):
            assert title == label
            assert xlabel == "Channel"
            assert [(side, label) for side, label, _ in axes] == [(side, label) for side, label, _ in sides]
            layers = np.split(coordinates, len(controls["yaxis"]))
            for (_, _, lines), (_, _, order) in zip(axes, sides, strict=True):
                for line, index in zip(lines, order, strict=True):
                    np.testing.assert_allclose(line, layers[index], rtol=1.2e-5, atol=1e-7)
    print(f"{name}: matched rendered coordinates, sides and {len(names)} filenames", flush=True)


def compare(root, native):
    vis = make_fixture(root)
    with opened(vis, True) as table:
        flags = table.getcol("FLAG")
        flags[1, 2, :] = True
        table.putcol("FLAG", flags)
        table.putcell("FLAG_ROW", 0, True)
    base = request(vis) | dict(xaxis="channel", xdatacolumn="", spw="0:0~3",
                               correlation="", iteraxis="corr", exprange="all",
                               yaxis=["real", "real", "real"],
                               ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    both = base | dict(gridrows=2)
    polarizations = ((12, "XX", 24), (12, "YY", 18))
    three = compare_text_pages(root, native, "three-correlation", both, polarizations)
    compare_text_pages(root, native, "two-correlation",
                       both | dict(yaxis=["real", "amp"], ydatacolumn=["data", "corrected"],
                                   yaxislocation=["right", "left"]), polarizations)
    compare_text_pages(root, native, "four-correlation",
                       both | dict(yaxis=["real", "real", "amp", "phase"],
                                   ydatacolumn=["data", "corrected", "model", "model"],
                                   yaxislocation=["left", "right", "left", "right"]), polarizations)
    only_first = compare_text_pages(root, native, "first-text-page", base, polarizations[:1])
    np.testing.assert_array_equal(only_first[0], three[0])
    for axis, metadata, values in (("scan", 3, ("1", "2")), ("field", 4, ("0", "1"))):
        controls = both | dict(field="", correlation="XX", iteraxis=axis)
        compare_text_pages(root, native, f"three-{axis}", controls,
                           tuple((metadata, value, 24) for value in values))
    sides = (("left", "Real, Real:model", (0, 2)), ("right", "Real:corrected", (1,)))
    panels = list(zip(("XX", "YY"), three, strict=True))
    compare_figures(root, native, "both-correlations", both, [panels], ["CorrXX,YY"], sides)
    compare_figures(root, native, "correlation-pages", base, [[panel] for panel in panels],
                    ["CorrXX", "CorrYY_2"], sides)


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
        compare(root, native)


if __name__ == "__main__":
    main()
