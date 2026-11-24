#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare raw and averaged PlotMS visibility ordinates across iterated MS panels."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_channel_ordinates_oracle import compare_model_provider, make_fixture, virtual_fixtures
from plotms_ms_columns_oracle import compare_text, copy_ms, opened, plotms_launcher, reference, request, rows


def compare_text_pages(root, native, name, controls, masks, *, flagged_correlation="YY"):
    expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, expformat="txt", plotfile=str(expected)) is None
    text = expected.read_text()
    sections = [[]]
    for line in text.splitlines():
        if line.startswith("# From plot") and "iteration" in line:
            sections.append([])
        elif line and not line.startswith("#"):
            sections[-1].append(line.split())
    ordinates = controls["yaxis"]
    for panel, (metadata_index, label, count) in zip(sections, masks, strict=True):
        assert len(panel) == len(ordinates) * count
        assert {p[metadata_index] for p in panel} == ({label} if count else set())
        assert all(int(p[2]) != 2 or p[12] != flagged_correlation for p in panel)
    assert text.count("# No data to export") == len(ordinates) * sum(count == 0 for _, _, count in masks)
    count = sum(map(len, sections))
    assert native(**controls, expformat="txt", plotfile=str(actual)) is None
    assert compare_text(actual, expected) == count
    if controls["xaxis"] == "time":
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                   [float(point[0]) for section in sections for point in section],
                                   rtol=0, atol=.00051)
    print(f"{name}: {count} matching coordinates in {len(sections)} panels", flush=True)
    return [np.array([point[:2] for point in section], dtype=float).reshape(-1, 2) for section in sections]


def compare_figures(root, native, name, controls, pages, file_tags, sides, *, shared_right=None):
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
                (drawn, drawn.yaxis.get_label_position(), drawn.get_ylabel(),
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
    iteration = {"corr": "Corr", "scan": "Scan"}[controls["iteraxis"]]
    for rendered, page in zip(captured, pages, strict=True):
        for (title, xlabel, axes), (label, coordinates) in zip(rendered, page, strict=True):
            if len(coordinates) == 0:
                assert all(not len(line) for _, _, _, lines in axes for line in lines)
                continue
            assert title == f"{controls['title']} {iteration}: {label}"
            assert xlabel == controls.get("xlabel", "Channel")
            assert [(side, label) for _, side, label, _ in axes] == [(side, label) for side, label, _ in sides]
            layers = np.split(coordinates, len(controls["yaxis"]))
            for (axis, side, _, lines), (_, _, order) in zip(axes, sides, strict=True):
                if shared_right is not None and side == "right":
                    np.testing.assert_array_equal(axis.get_ylim(), shared_right)
                for line, index in zip(lines, order, strict=True):
                    np.testing.assert_allclose(line, layers[index], rtol=1.2e-5, atol=1e-7)
                    if controls["xaxis"] == "time":
                        np.testing.assert_allclose(line[:, 0], layers[index][:, 0], rtol=0, atol=.00051)
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
                               title="Visibility",
                               yaxis=["real", "real", "real"],
                               ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    both = base | dict(gridrows=2)
    polarizations = ((12, "XX", 24), (12, "YY", 18))
    three = compare_text_pages(root, native, "three-correlation", both, polarizations)
    two_axes = both | dict(yaxis=["real", "amp"], ydatacolumn=["data", "corrected"],
                           yaxislocation=["right", "left"])
    two = compare_text_pages(root, native, "two-correlation", two_axes, polarizations)
    compare_text_pages(root, native, "same-column-correlation",
                       two_axes | dict(yaxis=["amp", "phase"], ydatacolumn=["data", "data"]), polarizations)
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

    _, mixed, stored_model = virtual_fixtures(root, vis)
    model_axes = both | dict(vis=str(mixed))
    virtual = compare_text_pages(root, native, "iterated-virtual-model", model_axes, polarizations)
    for panel in virtual:
        np.testing.assert_array_equal(np.split(panel, 3)[2][:, 1], 7.25)
    compare_model_provider(root, native, model_axes | dict(expformat="txt"))
    with opened(mixed) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), stored_model)

    sides = (("left", "Real, Real:model", (0, 2)), ("right", "Real:corrected", (1,)))
    panels = list(zip(("XX", "YY"), three, strict=True))
    compare_figures(root, native, "both-correlations", both, [panels], ["CorrXX,YY"], sides)
    compare_figures(root, native, "correlation-pages", base, [[panel] for panel in panels],
                    ["CorrXX", "CorrYY_2"], sides)
    compare_figures(root, native, "current-correlation", base | dict(exprange="current"),
                    [[panels[0]]], ["CorrXX"], sides)
    two_sides = (("left", "Amp:corrected", (1,)), ("right", "Real", (0,)))
    compare_figures(root, native, "two-correlation-sides", two_axes,
                    [list(zip(("XX", "YY"), two, strict=True))], ["CorrXX,YY"], two_sides)

    compare_review_cases(root, native, vis, base, polarizations, sides)


def compare_review_cases(root, native, vis, base, polarizations, sides):
    fields, corrs, scans, shared = (root / f"{name}.ms" for name in
                                   ("reordered-fields", "reordered-corrs", "five-scans", "shared"))
    for destination in (fields, corrs, scans, shared):
        copy_ms(vis, destination)
    with opened(fields, True) as table:
        table.putcol("FIELD_ID", 1 - table.getcol("FIELD_ID"))
    reverse_correlation_slots(corrs)
    with opened(scans, True) as table:
        _, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        table.putcol("SCAN_NUMBER", (1 + integration % 5).astype(np.int32))
    with opened(shared, True) as table:
        corrected = table.getcol("CORRECTED_DATA")
        corrected[1, :, :] *= 10
        table.putcol("CORRECTED_DATA", corrected)

    compare_text_pages(root, native, "reordered-fields",
                       base | dict(vis=str(fields), iteraxis="field", field="", correlation="XX"),
                       ((4, "0", 24),))
    compare_text_pages(root, native, "reordered-corrs", base | dict(vis=str(corrs)),
                       ((12, "XX", 18),), flagged_correlation="XX")

    five_scans = base | dict(vis=str(scans), iteraxis="scan", field="", correlation="XX",
                             gridrows=2, gridcols=2)
    first = compare_text_pages(root, native, "first-four-scans", five_scans,
                               ((3, "1", 12), (3, "2", 12), (3, "3", 8), (3, "4", 8)))
    final = compare_text_pages(root, native, "fifth-scan", five_scans | dict(scan="5"),
                               ((3, "5", 8),))
    page = list(zip(("1", "2", "3", "4"), first, strict=True))
    compare_figures(root, native, "five-scan-pages", five_scans,
                    [page, [("5", final[0])]], ["Scan1,2,3,4", "Scan5_2"], sides)

    shared_axes = base | dict(vis=str(shared), gridcols=2, yselfscale=True, ysharedaxis=True)
    coordinates = compare_text_pages(root, native, "shared-ordinates", shared_axes, polarizations)
    # Share the union of the two panel ranges, including their ordinary 4% padding.
    right_bounds = (10 - 0.04 * (70 - 10), 700 + 0.04 * (700 - 100))
    compare_figures(root, native, "shared-ordinates", shared_axes,
                    [list(zip(("XX", "YY"), coordinates, strict=True))], ["CorrXX,YY"], sides,
                    shared_right=right_bounds)


def reverse_correlation_slots(vis):
    with opened(vis / "POLARIZATION", True) as table:
        table.putcell("CORR_TYPE", 0, table.getcell("CORR_TYPE", 0)[::-1])
        table.putcell("CORR_PRODUCT", 0, table.getcell("CORR_PRODUCT", 0)[:, ::-1])


def compare_averaged_ordinates(root, native):
    root.mkdir()
    vis = make_fixture(root)
    with opened(vis, True) as table:
        flags = table.getcol("FLAG")
        flags[1, 2:4, ::2] = True
        table.putcol("FLAG", flags)
        table.putcell("FLAG_ROW", 0, True)
    base = request(vis) | dict(xaxis="channel", xdatacolumn="", spw="0:0~3", correlation="",
                               iteraxis="corr", gridrows=2, exprange="all", title="Visibility",
                               yaxis=["real"] * 3, ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    channel = base | dict(avgchannel="2", xlabel="Channel bin")
    corr = ((12, "XX", 12), (12, "YY", 6))
    reference_values = ((2.6, 6.6), (28, 68), (280, 680))
    points = compare_text_pages(root, native, "channel-correlation", channel, corr)
    for panel, allowed in zip(points, ((0, 1), (0,)), strict=True):
        for layer, values in zip(np.split(panel, 3), reference_values, strict=True):
            assert set(layer[:, 0]) == set(allowed)
            np.testing.assert_allclose(layer[:, 1], np.asarray(values)[layer[:, 0].astype(int)], rtol=1.2e-5)
    for axis, metadata, labels in (("scan", 3, ("1", "2")), ("field", 4, ("0", "1"))):
        controls = channel | dict(iteraxis=axis, field="", correlation="XX")
        panels = compare_text_pages(root, native, f"channel-{axis}", controls,
                                   tuple((metadata, label, 12) for label in labels))
        for panel in panels:
            for layer, values in zip(np.split(panel, 3), reference_values, strict=True):
                np.testing.assert_allclose(layer[:, 1], np.asarray(values)[layer[:, 0].astype(int)], rtol=1.2e-5)
    sides = (("left", "Real, Real:model", (0, 2)), ("right", "Real:corrected", (1,)))
    panels = list(zip(("XX", "YY"), points, strict=True))
    compare_figures(root, native, "channel-correlation-grid", channel, [panels], ["CorrXX,YY"], sides)
    compare_figures(root, native, "channel-correlation-pages", channel | dict(gridrows=1),
                    [[panel] for panel in panels], ["CorrXX", "CorrYY_2"], sides)

    virtual, mixed, stored = virtual_fixtures(root, vis)
    for name, source in (("virtual-channel", virtual), ("virtual-precedes-stored-channel", mixed)):
        controls = channel | dict(vis=str(source))
        predicted = compare_text_pages(root, native, name, controls, corr)
        for panel in predicted:
            np.testing.assert_array_equal(np.split(panel, 3)[2][:, 1], 7.25)
    with opened(virtual) as table:
        assert "MODEL_DATA" not in table.colnames()
    with opened(mixed) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), stored)

    # Both time averaging modes use the same iterated page contract. The
    # scalar mode keeps its own closed visibility-buffer bins.
    time = base | dict(xaxis="time", avgtime="20")
    compare_text_pages(root, native, "ordinary-vector-time-correlation", time, corr)
    compare_text_pages(root, native, "ordinary-scalar-time-correlation", time | dict(scalar=True),
                       ((12, "XX", 8), (12, "YY", 4)))
    compare_text_pages(root, native, "time-and-channel-correlation", time | dict(avgchannel="2"),
                       ((12, "XX", 6), (12, "YY", 3)))
    compare_virtual_time_iteration(root, native, time, virtual, mixed, stored)
    compare_time_iteration_layout(root, native, vis, time, sides)
    compare_fully_flagged_panel(root, native, vis, base, sides)


def compare_virtual_time_iteration(root, native, time, virtual, mixed, stored):
    for name, source, averaging, (xx, yy) in (
        ("virtual-vector-time", virtual, {}, (12, 6)),
        ("virtual-scalar-time", mixed, dict(scalar=True), (8, 4)),
        ("virtual-time-channel", mixed, dict(avgchannel="2"), (6, 3)),
    ):
        controls = time | averaging | dict(vis=str(source))
        panels = compare_text_pages(root, native, name, controls, ((12, "XX", xx), (12, "YY", yy)))
        for panel in panels:
            np.testing.assert_array_equal(np.split(panel, 3)[2][:, 1], 7.25)
    with opened(virtual) as table:
        assert "MODEL_DATA" not in table.colnames()
    with opened(mixed) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), stored)


def compare_fully_flagged_panel(root, native, vis, base, sides):
    masked = root / "fully-flagged-correlation.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[1] = True
        table.putcol("FLAG", flags)
    time = dict(xaxis="time", avgtime="20", xlabel="Time (MJD seconds)")
    for name, averaging, count in (
        ("raw", {}, 24),
        ("channel", dict(avgchannel="2"), 12),
        ("vector-time", time, 12),
        ("scalar-time", time | dict(scalar=True), 8),
        ("time-channel", time | dict(avgchannel="2"), 6),
    ):
        controls = base | averaging | dict(vis=str(masked))
        panels = compare_text_pages(root, native, f"empty-correlation-{name}", controls,
                                   ((12, "XX", count), (12, "YY", 0)))
        if name == "scalar-time":
            pages = [[(label, panel)] for label, panel in zip(("XX", "YY"), panels, strict=True)]
            compare_figures(root, native, "empty-correlation-scalar-pages", controls | dict(gridrows=1),
                            pages, ["CorrXX", "CorrYY_2"], sides)


def compare_time_iteration_layout(root, native, vis, time, sides):
    ordered = time | dict(field="", correlation="XX")
    for axis, column, ceiling, metadata, labels, count, scalar in (
        ("scan", "SCAN_NUMBER", 3, 3, ("1", "2"), 12, False),
        ("field", "FIELD_ID", 1, 4, ("0", "1"), 8, True),
    ):
        reordered = root / f"time-reordered-{axis}.ms"
        copy_ms(vis, reordered)
        with opened(reordered, True) as table:
            table.putcol(column, ceiling - table.getcol(column))
        controls = ordered | dict(vis=str(reordered), iteraxis=axis, scalar=scalar)
        panels = compare_text_pages(root, native, f"time-reordered-{axis}", controls,
                                   tuple((metadata, label, count) for label in labels))
        assert panels[0][:, 0].min() > panels[1][:, 0].max()

    corrs = root / "time-reordered-corrs.ms"
    copy_ms(vis, corrs)
    reverse_correlation_slots(corrs)
    compare_text_pages(root, native, "time-reordered-corrs", time | dict(vis=str(corrs)),
                       ((12, "XX", 6), (12, "YY", 12)), flagged_correlation="XX")

    scans = root / "time-five-scans.ms"
    copy_ms(vis, scans)
    with opened(scans, True) as table:
        _, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        table.putcol("SCAN_NUMBER", np.minimum(1 + integration // 2, 5).astype(np.int32))
    five = ordered | dict(vis=str(scans), iteraxis="scan", gridcols=2, xlabel="Time (MJD seconds)")
    labels = ("1", "2", "3", "4")
    first = compare_text_pages(root, native, "time-first-four-scans", five,
                               tuple((3, label, 4) for label in labels))
    last = compare_text_pages(root, native, "time-fifth-scan", five | dict(scan="5"), ((3, "5", 8),))
    compare_figures(root, native, "time-five-scans", five, [list(zip(labels, first, strict=True)), [("5", last[0])]],
                    ["Scan1,2,3,4", "Scan5_2"], sides)


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
        compare_averaged_ordinates(root / "averaged", native)


if __name__ == "__main__":
    main()
