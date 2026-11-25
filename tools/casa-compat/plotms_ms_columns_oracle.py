#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare independent visibility columns through both public PlotMS tasks."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from time import perf_counter
from unittest.mock import patch

import casatasks as casa
from casaplotms import plotms as reference
import numpy as np
from PIL import Image

from calibration_smoothcal_oracle import copy_ms, opened
from imager_prediction_options_oracle import write_model
from plotms_calibration_oracle import compare_text, plotms_launcher
from record_casa_returns import make_cal_ms


def fixture(root):
    vis = root / "columns.ms"
    make_cal_ms(str(vis))
    casa.clearcal(str(vis), addmodel=True)
    with opened(vis, True) as table:
        shape = table.getcol("DATA").shape
        corr, channel, row = np.indices(shape)
        raw = (1 + channel + row / 256 + 2 * corr).astype(np.complex64)
        corrected = (raw * (channel + 2j)).astype(np.complex64)
        model = raw * (3 - 4j)
        flags = (channel == 2) & (corr == 0)
        flag_rows = np.zeros(shape[2], dtype=bool)
        columns = dict(DATA=raw, CORRECTED_DATA=corrected, MODEL_DATA=model,
                       FLAG=flags, FLAG_ROW=flag_rows)
        for name, values in columns.items():
            table.putcol(name, values)
    return vis, columns


def request(vis):
    return dict(vis=str(vis), xaxis="amp", yaxis="phase", xdatacolumn="data",
                ydatacolumn="corrected", field="FLUX", antenna="A0&A1",
                spw="0:1~3", correlation="XX", showgui=False, overwrite=True)


def rows(path):
    return [line.split() for line in path.read_text().splitlines() if not line.startswith("#")]


def compare_figure(root, native, controls, expected, labels, *,
                   reference_task=reference, ordinate_atol=None):
    from casars import _plot_document

    capture = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        capture.append((axis.get_title(), axis.get_xlabel(), axis.get_ylabel(),
                        axis.lines[0].get_xydata(), axis.get_xlim(), axis.get_ylim()))
        return figure, axis

    outputs = [root / engine / expected.name.replace(".casa.txt", ".png") for engine in ("casa", "native")]
    for output in outputs:
        output.parent.mkdir(exist_ok=True)
    figure = dict(controls, expformat="png", width=500, height=360, dpi=100)
    assert reference_task(**figure, plotfile=str(outputs[0])) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**figure, plotfile=str(outputs[1])) is None
    [(title, xlabel, ylabel, points, xlim, ylim)] = capture
    assert (title, xlabel, ylabel) == labels, (title, xlabel, ylabel)
    coordinates = np.array([point[:2] for point in rows(expected)], dtype=float)
    np.testing.assert_allclose(points, coordinates, rtol=1.2e-5, atol=1e-7)
    if ordinate_atol is not None:
        np.testing.assert_allclose(points[:, 1], coordinates[:, 1], rtol=0, atol=ordinate_atol)
    if controls["xaxis"] == "time":
        np.testing.assert_allclose(points[:, 0], coordinates[:, 0], rtol=0, atol=0.00051)
    visible = points[np.isfinite(points).all(axis=1)]
    for values, limits in zip(visible.T, (xlim, ylim), strict=True):
        assert min(limits) <= min(values) <= max(values) <= max(limits), (values, limits)
    for output in outputs:
        with Image.open(output) as image:
            assert image.size == (500, 360), (output, image.size)
    print(f"{outputs[0].name}: {len(points)} matching rendered coordinates, labels and dimensions", flush=True)


def compare_channel_vector_residual(root, native, vis):
    controls = request(vis) | dict(xdatacolumn="corrected-model_vector", avgchannel="3", expformat="txt")
    expected, actual = (root / f"averaged-vector.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 6


def compare_raw_flag_symbols(root, vis, native):
    flagged_ms = root / "flagged-displays.ms"
    copy_ms(vis, flagged_ms)
    with opened(flagged_ms, True) as table:
        timestamp = f"{table.getcell('TIME', 0):.3f}"
        table.putcell("FLAG_ROW", 0, True)
    base = request(flagged_ms) | dict(title="Flagged visibility")
    marker_color = "dd2255"
    marked = base | dict(customflaggedsymbol=True, flaggedsymbolshape="diamond",
                         flaggedsymbolcolor=marker_color, flaggedsymbolsize=4,
                         flaggedsymbolfill="fill", flaggedsymboloutline=False)
    marked_list = marked | dict(customflaggedsymbol=[True], flaggedsymbolshape=["diamond"],
                                flaggedsymbolcolor=[marker_color], flaggedsymbolsize=[4])
    for name, controls, count, channels in (
        ("hidden-flags", base, 12, {"1", "3"}),
        ("visible-flags", marked, 18, {"1", "2", "3"}),
        ("no-flag-marker", marked | dict(flaggedsymbolshape="nosymbol"), 12, {"1", "3"}),
        ("list-flag-marker", marked_list, 18, {"1", "2", "3"}),
        ("flagged-only", marked | dict(customsymbol=True, symbolshape="nosymbol"), 6, {"2"}),
    ):
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        points = rows(expected)
        assert len(points) == count
        assert {point[2] for point in points} == channels
        assert timestamp in {point[9] for point in points}
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching raw visibility coordinates", flush=True)

    compare_flagged_figure(root, native, "list-flag-marker", marked_list, marker_color)

    reversed_ms = root / "flagged-reversed.ms"
    copy_ms(flagged_ms, reversed_ms)
    with opened(reversed_ms, True) as table:
        times = table.getcol("TIME")
        times = times.min() + times.max() - times
        table.putcol("TIME", times)
        table.putcol("TIME_CENTROID", times)
    reversed_selection = dict(vis=str(reversed_ms), correlation="XX,YY")
    reversed_hidden = base | reversed_selection
    reversed_marked = marked | reversed_selection
    for name, controls, count in (("reversed-hidden", reversed_hidden, 30),
                                  ("reversed-marked", reversed_marked, 36)):
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching visibility coordinates in iterator order", flush=True)
    compare_figure(root, native, reversed_hidden, root / "reversed-hidden.casa.txt",
                   ("Flagged visibility", "Amp", "Phase:corrected (degrees)"))
    compare_flagged_figure(root, native, "reversed-marked", reversed_marked, marker_color)
    compare_empty_export(root, native, "empty-raw", marked | dict(spw="0:2", flaggedsymbolshape="nosymbol"),
                         ("Amp", "Phase:corrected (degrees)"))


def compare_channel_averaged_flag_symbols(root, vis, native):
    work = root / "channel-averaged-flag-symbols"
    work.mkdir()
    selected_ms = work / "flagged-channels.ms"
    copy_ms(vis, selected_ms)
    with opened(selected_ms, True) as table:
        flags = table.getcol("FLAG")
        fields = table.getcol("FIELD_ID")
        antenna1, antenna2 = [table.getcol(column) for column in ("ANTENNA1", "ANTENNA2")]
        selected = (fields == 0) & (antenna1 == 0) & (antenna2 == 1)
        flags[:, :, selected] = False
        # The three two-channel bins are clear, partly flagged and fully flagged.
        flags[0, 2, selected] = True
        flags[0, 4:6, selected] = True
        table.putcol("FLAG", flags)
        flag_rows = table.getcol("FLAG_ROW")
    base = request(selected_ms) | dict(spw="0:0~5", averagedata=True, avgchannel="2",
                                       title="Flagged visibility", expformat="txt")
    marker_color = "dd2255"
    marked = base | dict(customflaggedsymbol=True, flaggedsymbolshape="diamond",
                         flaggedsymbolcolor=marker_color, flaggedsymbolsize=4,
                         flaggedsymbolfill="fill", flaggedsymboloutline=False)
    first_x = [1.5, 4.0, 5.5]
    modes = (
        ("channel-averages-clear", base, [0, 1]),
        ("channel-averages-marked", marked, [0, 1, 2]),
        ("channel-averages-flagged", marked | dict(customsymbol=True, symbolshape="nosymbol"), [2]),
    )
    for name, options, channels in modes:
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        points = rows(expected)
        assert [int(point[2]) for point in points] == channels * 6
        np.testing.assert_allclose([float(point[0]) for point in points[:len(channels)]],
                                   [first_x[channel] for channel in channels], rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(points)
        print(f"{name}: {len(points)} matching channel-average coordinates and flags", flush=True)
    compare_flagged_figure(work, native, "channel-averages-marked", marked, marker_color)
    with opened(selected_ms) as table:
        np.testing.assert_array_equal(table.getcol("FLAG"), flags)
        np.testing.assert_array_equal(table.getcol("FLAG_ROW"), flag_rows)
    weighted = work / "weighted-flagged-channels.ms"
    copy_ms(selected_ms, weighted)
    spectra = {}
    with opened(weighted, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, pair in (("WEIGHT_SPECTRUM", [1, 9]), ("SIGMA_SPECTRUM", [1, 0.5])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            values = np.resize(np.array(pair, dtype=np.float32), shape[1])[None, :, None]
            spectra[name] = np.broadcast_to(values, shape)
            table.putcol(name, spectra[name])
    expected, actual = (work / f"weighted-marked.{engine}.txt" for engine in ("casa", "native"))
    options = marked | dict(vis=str(weighted))
    assert reference(**options, plotfile=str(expected)) is None
    np.testing.assert_allclose([float(point[0]) for point in rows(expected)[:3]],
                               [1.8, 4.0, 5.8], rtol=1.2e-5)
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 18
    with opened(weighted) as table:
        np.testing.assert_array_equal(table.getcol("FLAG"), flags)
        for name, values in spectra.items():
            np.testing.assert_array_equal(table.getcol(name), values)
    print("weighted-marked: 18 matching channel averages with flagged spectra", flush=True)
    compare_flagged_channel_edge_values(work, weighted, native, marked)


def compare_flagged_channel_edge_values(work, weighted, native, marked):
    from plotms_callibrary_oracle import snapshot, verify_source

    vis = work / "flagged-channel-edge-values.ms"
    copy_ms(weighted, vis)
    with opened(vis, True) as table:
        selected = ((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
                    & (table.getcol("ANTENNA2") == 1))
        sigma = table.getcol("SIGMA_SPECTRUM")
        sigma[0, :2, selected] = 0
        table.putcol("SIGMA_SPECTRUM", sigma)
        data = table.getcol("DATA")
        data[0, 4, np.flatnonzero(selected)[0]] = np.complex64(np.nan + 0j)
        table.putcol("DATA", data)
    before = snapshot(vis)
    modes = (
        ("clear", dict(customflaggedsymbol=False), [1]),
        ("both", {}, [0, 1, 2]),
        ("flagged", dict(customsymbol=True, symbolshape="nosymbol"), [0, 2]),
    )
    for mode, style, channels in modes:
        expected, actual = (work / f"edge-values-{mode}.{engine}.txt" for engine in ("casa", "native"))
        options = marked | dict(vis=str(vis)) | style
        assert reference(**options, plotfile=str(expected)) is None
        points = rows(expected)
        assert [int(point[2]) for point in points] == channels * 6
        if mode == "both":
            assert float(points[0][0]) == 0 and np.isnan(float(points[2][0]))
            np.testing.assert_allclose([float(points[index][1]) for index in (0, 2)],
                                       [64.6538, 22.1412], rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(points)
        print(f"edge-values-{mode}: {len(points)} matching zero-weight and nonfinite coordinates",
              flush=True)
    verify_source(vis, before)


def compare_flagged_figure(root, native, name, controls, marker_color, *,
                           labels=("Flagged visibility", "Amp", "Phase:corrected (degrees)"),
                           is_flagged=lambda point: point[2] == "2" and point[12] == "XX"):
    from casars.tasks import _plotms_plot
    from matplotlib.colors import to_rgba

    captured = []
    configure = _plotms_plot.configure

    def record(figure, axes, document, options):
        configure(figure, axes, document, options)
        [axis] = figure.axes
        assert (axis.get_title(), axis.get_xlabel(), axis.get_ylabel()) == labels
        captured.extend(axis.lines)

    figure = controls | dict(width=500, height=360, dpi=100, expformat="png")
    expected, actual = (root / f"{name}.{engine}.png" for engine in ("casa", "native"))
    assert reference(**figure, plotfile=str(expected)) is None
    with patch.object(_plotms_plot, "configure", record):
        assert native(**figure, plotfile=str(actual)) is None
    [ordinary, flagged] = sorted(captured, key=lambda line: -len(line.get_xydata()))
    assert flagged.get_marker() == "D" and flagged.get_markersize() == 4
    assert to_rgba(flagged.get_markerfacecolor()) == to_rgba(f"#{marker_color}")
    points = rows(root / f"{name}.casa.txt")
    for artist, masked in ((ordinary, False), (flagged, True)):
        coordinates = np.array([point[:2] for point in points if is_flagged(point) == masked],
                               dtype=float)
        np.testing.assert_allclose(artist.get_xydata(), coordinates, rtol=1.2e-5, atol=1e-7)
    rgb = np.round(np.array(to_rgba(f"#{marker_color}")[:3]) * 255).astype(np.uint8)
    for path in (expected, actual):
        with Image.open(path) as image:
            assert image.size == (500, 360)
            assert np.any(np.all(np.asarray(image.convert("RGB")) == rgb, axis=-1)), path
    print(f"{name}.png: {len(ordinary.get_xydata())} clear and {len(flagged.get_xydata())} flagged points",
          flush=True)


def compare_empty_export(root, native, name, controls, labels):
    from casars import _plot_document

    for verbose in (True, False):
        expected, actual = (root / f"{name}-{verbose}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), verbose=verbose, expformat="txt") is None
        assert not rows(expected) and expected.read_text().count("# No data to export") == 1
        assert native(**controls, plotfile=str(actual), verbose=verbose, expformat="txt") is None
        assert actual.read_bytes() == expected.read_bytes()

    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        assert not (axis.lines or axis.collections)
        assert (axis.get_xlabel(), axis.get_ylabel()) == labels
        return figure, axis

    figure = controls | dict(width=500, height=360, dpi=100, expformat="png")
    expected, actual = (root / f"{name}.{engine}.png" for engine in ("casa", "native"))
    assert reference(**figure, plotfile=str(expected)) is None
    with patch.object(_plot_document, "render_document", side_effect=record) as renderer:
        assert native(**figure, plotfile=str(actual)) is None
    renderer.assert_called_once()
    for path in (expected, actual):
        with Image.open(path) as image:
            assert image.size == (500, 360)
    print(f"{name}: matching empty text sections and 500 by 360 PNG", flush=True)


def compare(root, vis, native):
    base = request(vis)
    cases = {
        "data-and-corrected": {},
        "ordinary-amp-phase": {"ydatacolumn": ""},
        "ordinary-channel-amp": {"xaxis": "channel", "yaxis": "amp",
                                 "xdatacolumn": "", "ydatacolumn": ""},
        "ordinary-time-amp": {"xaxis": "time", "yaxis": "amp",
                              "xdatacolumn": "", "ydatacolumn": ""},
        "model-and-default": {"xdatacolumn": "model", "ydatacolumn": ""},
        "default-and-corrected": {"xdatacolumn": ""},
        "same-axis-distinct-columns": {"xaxis": "real", "yaxis": "real", "xdatacolumn": "model"},
        "explicit-vector": {"xdatacolumn": "data-model_vector", "ydatacolumn": "corrected-model_vector"},
        "implicit-vector": {"xdatacolumn": "data-model", "ydatacolumn": "corrected-model"},
        "residual-alias": {"xdatacolumn": "residual", "ydatacolumn": "data"},
        "residual-vs-time": {"xaxis": "time", "yaxis": "amp", "ydatacolumn": "residual"},
        "zero-channel-and-time": {"xdatacolumn": "data-model_vector", "avgchannel": "0", "avgtime": "0"},
        "vector-components": {"xaxis": "imag", "yaxis": "real", "xdatacolumn": "data-model_vector",
                              "ydatacolumn": "corrected-model_vector"},
        "implicit-vector-ratio": {"xdatacolumn": "data/model", "ydatacolumn": "corrected/model"},
        "explicit-vector-ratio": {"xdatacolumn": "data/model_vector", "ydatacolumn": "corrected/model_vector"},
        "vector-ratio-components": {"xaxis": "imag", "yaxis": "real", "xdatacolumn": "data/model_vector",
                                    "ydatacolumn": "corrected/model_vector"},
        "scalar-amp-phase-ratio": {"xdatacolumn": "data/model_scalar", "ydatacolumn": "corrected/model_scalar"},
        "scalar-ratio-components": {"xaxis": "real", "yaxis": "imag", "xdatacolumn": "data/model_scalar",
                                    "ydatacolumn": "corrected/model_scalar"},
        "scalar-ratio-vs-time": {"xaxis": "time", "yaxis": "amp", "ydatacolumn": "data/model_scalar"},
        "scalar-amp-phase": {"xdatacolumn": "data-model_scalar", "ydatacolumn": "corrected-model_scalar"},
        "scalar-components": {"xaxis": "imag", "yaxis": "real", "xdatacolumn": "data-model_scalar",
                              "ydatacolumn": "corrected-model_scalar"},
        "scalar-vs-time": {"xaxis": "time", "yaxis": "amp", "ydatacolumn": "data-model_scalar"},
    }
    for name, options in cases.items():
        controls = base | options
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        start = perf_counter()
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        casa_seconds = perf_counter() - start
        selected = rows(expected)
        assert len(selected) == 12, selected
        assert {sample[2] for sample in selected} == {"1", "3"}, selected
        assert {sample[12] for sample in selected} == {"XX"}, selected
        if name == "scalar-amp-phase":
            assert all(float(point[0]) < 0 for point in selected)
        start = perf_counter()
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        native_seconds = perf_counter() - start
        count = compare_text(actual, expected)
        if controls["xaxis"] == "time":
            native_times = [float(point[0]) for point in rows(actual)]
            casa_times = [float(point[0]) for point in selected]
            np.testing.assert_allclose(native_times, casa_times, rtol=0, atol=0.00051)
        print(f"{name}: {count} points, CASA {casa_seconds:.3f}s, casa-rs {native_seconds:.3f}s",
              flush=True)
    compare_figure(root, native, base, root / "data-and-corrected.casa.txt",
                   ("Phase:corrected vs. Amp", "Amp", "Phase:corrected (degrees)"))
    compare_figure(root, native, base | cases["ordinary-amp-phase"], root / "ordinary-amp-phase.casa.txt",
                   ("Phase vs. Amp", "Amp", "Phase (degrees)"))
    compare_figure(root, native, base | cases["ordinary-channel-amp"], root / "ordinary-channel-amp.casa.txt",
                   ("Amp vs. Channel", "Channel", "Amp"))
    compare_figure(root, native, base | cases["ordinary-time-amp"], root / "ordinary-time-amp.casa.txt",
                   ("Amp vs. Time", "Time (MJD seconds)", "Amp"))
    real = base | cases["same-axis-distinct-columns"] | dict(xlabel="Model real part")
    compare_figure(root, native, real, root / "same-axis-distinct-columns.casa.txt",
                   ("Real:corrected vs. Real:model", "Model real part", "Real:corrected"))
    compare_figure(root, native, base | cases["explicit-vector"], root / "explicit-vector.casa.txt",
                   ("Phase:corrected-model (vector) vs. Amp:data-model (vector)",
                    "Amp:data-model (vector)", "Phase:corrected-model (vector) (degrees)"))
    compare_figure(root, native, base | cases["scalar-amp-phase"], root / "scalar-amp-phase.casa.txt",
                   ("Phase:corrected-model (scalar) vs. Amp:data-model (scalar)",
                    "Amp:data-model (scalar)", "Phase:corrected-model (scalar) (degrees)"))
    compare_figure(root, native, base | cases["explicit-vector-ratio"], root / "explicit-vector-ratio.casa.txt",
                   ("Phase:corrected/model (vector) vs. Amp:data/model (vector)",
                    "Amp:data/model (vector)", "Phase:corrected/model (vector) (degrees)"))
    compare_figure(root, native, base | cases["scalar-amp-phase-ratio"], root / "scalar-amp-phase-ratio.casa.txt",
                   ("Phase:corrected/model (scalar) vs. Amp:data/model (scalar)",
                    "Amp:data/model (scalar)", "Phase:corrected/model (scalar) (degrees)"))
    compare_channel_vector_residual(root, native, vis)
    compare_raw_flag_symbols(root, vis, native)
    compare_channel_averaged_flag_symbols(root, vis, native)


def compare_missing_corrected(root, vis, source, native):
    fallback = root / "missing-corrected.ms"
    copy_ms(vis, fallback)
    with opened(fallback, True) as table:
        table.removecols(["CORRECTED_DATA"])
    cases = {
        "corrected-on-y": {},
        "corrected-on-x": dict(xdatacolumn="corrected", ydatacolumn="model"),
        "residual-on-x": dict(xdatacolumn="residual", ydatacolumn="data"),
        "residual-on-y": dict(ydatacolumn="corrected-model_vector"),
        "corrected-vs-time": dict(xaxis="time", xdatacolumn=""),
        "vector-ratio-without-corrected": dict(xdatacolumn="corrected/model_vector", ydatacolumn="data/model_vector"),
        "scalar-ratio-without-corrected": dict(xdatacolumn="data/model_scalar", ydatacolumn="corrected/model_scalar"),
        "scalar-without-corrected": dict(xdatacolumn="corrected-model_scalar", ydatacolumn="data-model_scalar"),
        "ordinate-without-corrected": dict(xaxis="channel", yaxis=["amp", "amp"],
                                            xdatacolumn="", ydatacolumn=["data", "corrected"]),
    }
    for name, options in cases.items():
        controls = request(fallback) | options | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        assert native(**controls, plotfile=str(actual)) is None
        count = compare_text(actual, expected)
        assert count == (24 if name == "ordinate-without-corrected" else 12)
        print(f"{name}: {count} matches with no stored CORRECTED_DATA", flush=True)
    compare_figure(root, native, request(fallback), root / "corrected-on-y.casa.txt",
                   ("Phase vs. Amp", "Amp", "Phase (degrees)"))
    with opened(fallback) as table:
        assert "CORRECTED_DATA" not in table.colnames()
        for name, values in source.items():
            if name != "CORRECTED_DATA":
                np.testing.assert_array_equal(table.getcol(name), values, err_msg=name)


def compare_scalar_phase_boundary(root, vis, source, native):
    boundary = root / "scalar-phase-boundary.ms"
    copy_ms(vis, boundary)
    with opened(boundary, True) as table:
        table.putcol("CORRECTED_DATA", source["DATA"] * np.complex64(-1 + 0.125j))
        table.putcol("MODEL_DATA", source["DATA"] * np.complex64(-1 - 0.125j))
    controls = request(boundary) | dict(xdatacolumn="data-model_scalar", ydatacolumn="corrected-model_scalar")
    expected, actual = (root / f"scalar-phase-boundary.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    assert all(float(sample[1]) > 180 for sample in rows(expected))
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 12
    compare_figure(root, native, controls, expected,
                   ("Phase:corrected-model (scalar) vs. Amp:data-model (scalar)",
                    "Amp:data-model (scalar)", "Phase:corrected-model (scalar) (degrees)"))


def compare_zero_model(root, vis, source, native):
    zero_model = root / "zero-model.ms"
    copy_ms(vis, zero_model)
    model = source["MODEL_DATA"].copy()
    model[0, 1, 0] = 0
    model[0, 3, 0] = 0
    with opened(zero_model, True) as table:
        table.putcol("MODEL_DATA", model)
    cases = (
        ("vector-zero-phase", "amp", "phase", "data/model_vector", np.inf, np.nan),
        ("vector-zero-complex", "real", "imag", "corrected/model_vector", np.inf, np.inf),
        ("scalar-zero-phase", "amp", "phase", "corrected/model_scalar", np.inf, np.inf),
        ("scalar-zero-components", "real", "imag", "data/model_scalar", 2, 0),
    )
    for name, x, y, column, expected_x, expected_y in cases:
        controls = request(zero_model) | dict(xaxis=x, yaxis=y, xdatacolumn=column, ydatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        np.testing.assert_array_equal(np.array(rows(expected)[0][:2], dtype=float), [expected_x, expected_y])
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 12
        print(f"{name}: matched defined and nonfinite values for 12 selected points", flush=True)
    controls = request(zero_model) | dict(xdatacolumn="data/model_vector", ydatacolumn="data/model_vector")
    compare_figure(root, native, controls, root / "vector-zero-phase.casa.txt",
                   ("Phase:data/model (vector) vs. Amp:data/model (vector)",
                    "Amp:data/model (vector)", "Phase:data/model (vector) (degrees)"))


def compare_ratio_float_range(root, vis, source, native):
    bounded = root / "ratio-float-range.ms"
    copy_ms(vis, bounded)
    data, model = (source[column].copy() for column in ("DATA", "MODEL_DATA"))
    data[0, 1, 0], model[0, 1, 0] = 8 + 4j, 2e-38
    data[0, 3, 0], model[0, 3, 0] = 1e-30 + 2e-30j, 1e30
    with opened(bounded, True) as table:
        second = np.flatnonzero((table.getcol("ANTENNA1") == 0)
                                & (table.getcol("ANTENNA2") == 1)
                                & (table.getcol("FIELD_ID") == 0))[1]
        data[0, 1, second], model[0, 1, second] = 8 + 4j, 1e-37
        table.putcol("DATA", data)
        table.putcol("MODEL_DATA", model)
    phases = (("vector", (0.0, 0.0, 26.5651)), ("scalar", (np.inf, 6.34349e-29, np.inf)))
    for method, expected_phases in phases:
        name = f"{method}-ratio-float-range"
        column = f"data/model_{method}"
        controls = request(bounded) | dict(xdatacolumn=column, ydatacolumn=column, expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 12
        for export in (expected, actual):
            extremes = np.array([sample[:2] for sample in rows(export)[:3]], dtype=float)
            np.testing.assert_allclose(extremes, np.column_stack(([np.inf, 0.0, 8.94427e37], expected_phases)),
                                       rtol=1.2e-5, atol=0)
        print(f"{name}: matched the float range for 12 selected points", flush=True)


def compare_ordinate_columns(root, vis, native):
    base = request(vis) | dict(xaxis="channel", xdatacolumn="", yaxislocation=["left", "right"])
    cases = {
        "ord-distinct-axes": dict(yaxis=["amp", "phase"], ydatacolumn=["data", "corrected"]),
        "ord-same-axis": dict(yaxis=["amp", "amp"], ydatacolumn=["data", "corrected"]),
        "ord-same-axis-short-list": dict(yaxis=["amp", "amp"], ydatacolumn=["corrected"]),
        "ord-short-list": dict(yaxis=["amp", "phase"], ydatacolumn=["corrected"]),
        "ord-scalar-column": dict(yaxis=["amp", "phase"], ydatacolumn="corrected",
                                  avgchannel="0", avgtime="0"),
        "ord-equal-coordinates": dict(yaxis=["amp", "real"], ydatacolumn=["data", "data"]),
        "ord-reverse-axes": dict(yaxis=["phase", "amp"], ydatacolumn=["corrected", "data"]),
        "ord-shares-x-column": dict(xaxis="amp", xdatacolumn="corrected", yaxis=["amp", "phase"],
                                     ydatacolumn=["data", "corrected"], avgchannel="0", avgtime="0"),
    }
    for name, options in cases.items():
        controls = base | options | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 24
        assert [point[2:] for point in selected[:12]] == [point[2:] for point in selected[12:]]
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 24
        print(f"{name}: matched both ordinates for 12 selected points", flush=True)

    duplicates = (("identical", "data", "data"), ("aliases", "residual", "corrected-model_vector"))
    for name, first, second in duplicates:
        for engine, task in (("casa", reference), ("native", native)):
            duplicate = base | dict(yaxis=["amp", "amp"], ydatacolumn=[first, second], expformat="txt")
            output = root / f"ord-duplicate-{name}.{engine}.txt"
            try:
                task(**duplicate, plotfile=str(output))
            except RuntimeError as error:
                assert "Remove duplicate y-axes." in str(error), error
            else:
                raise AssertionError(f"{engine} accepted duplicate ordinates with the same column")
            assert not output.exists()

    for name, invalid in (("scalar", "typo"), ("empty-entry", ["data", ""]),
                          ("trailing-entry", ["data", "corrected", "typo"])):
        controls = base | dict(yaxis=["amp", "phase"], ydatacolumn=invalid, expformat="txt")
        for engine, task in (("casa", reference), ("native", native)):
            output = root / f"ord-invalid-{name}.{engine}.txt"
            try:
                task(**controls, plotfile=str(output))
            except ValueError as error:
                assert "invalid ydatacolumn" in str(error), error
            else:
                raise AssertionError(f"{engine} accepted the invalid {name} selector")
            assert not output.exists()

    controls = base | dict(yaxis=["amp", "amp"], ydatacolumn=["data"], expformat="txt")
    for engine, task, message in (("casa", reference, "plot parameters failed"),
                                  ("native", native, "Remove duplicate y-axes.")):
        output = root / f"ord-default-duplicate.{engine}.txt"
        try:
            task(**controls, plotfile=str(output))
        except Exception as error:
            assert message in str(error), error
        else:
            raise AssertionError(f"{engine} accepted duplicate effective DATA ordinates")
        assert not output.exists()

    for name, labels in (("ord-same-axis", ["Amp", "Amp:corrected"]),
                         ("ord-equal-coordinates", ["Amp", "Real"])):
        compare_ordinate_figure(root, name, native, base | cases[name], "Channel", labels,
                                shared_limits=name == "ord-equal-coordinates")


def compare_ordinate_figure(root, name, native, controls, xlabel, labels, *, shared_limits=False,
                            color_groups=()):
    from casars import _plot_document

    capture = []
    captured_groups = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, primary = render(*args, **kwargs)
        layers = []
        for axis in figure.axes:
            lines = list(axis.lines) if color_groups else [axis.lines[0]]
            layers.append((axis.get_ylabel(), np.vstack([line.get_xydata() for line in lines]), axis.get_ylim()))
            if color_groups:
                captured_groups.append([(line.get_label(), line.get_color(), len(line.get_xydata())) for line in lines])
        capture.append((primary.get_xlabel(), layers))
        return figure, primary

    figure = controls | dict(expformat="png", width=500, height=360, dpi=100)
    expected, actual = (root / f"{name}.{engine}.png" for engine in ("casa", "native"))
    assert reference(**figure, plotfile=str(expected)) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**figure, plotfile=str(actual)) is None
    [(actual_x, layers)] = capture
    assert actual_x == xlabel, actual_x
    assert [label for label, _, _ in layers] == labels, layers
    coordinates = np.array([point[:2] for point in rows(root / f"{name}.casa.txt")], dtype=float)
    for (_, layer, _), section in zip(layers, np.split(coordinates, len(labels)), strict=True):
        np.testing.assert_allclose(layer, section, rtol=1.2e-5, atol=1e-7)
    if shared_limits:
        assert layers[0][2] == layers[1][2], (layers[0][2], layers[1][2])
    if color_groups:
        for label, grouped in zip(labels, captured_groups, strict=True):
            assert [name for name, _, _ in grouped] == [f"{label} · {name}" for name in color_groups], grouped
            assert len({color for _, color, _ in grouped}) == len(color_groups), grouped
            assert all(size == 1 for _, _, size in grouped), grouped
        assert [color for _, color, _ in captured_groups[0]] == [color for _, color, _ in captured_groups[1]]
    for output in (expected, actual):
        with Image.open(output) as image:
            assert image.size == (500, 360), (output, image.size)


def compare_multiple_ordinates(root, vis, native):
    from casars import _plot_document

    base = request(vis) | dict(xaxis="channel", xdatacolumn="", yaxis=["real", "phase", "amp"],
                              ydatacolumn=["model", "data", "corrected"],
                              yaxislocation=["left", "right", "left"])
    virtual = root / "multiple-ordinates-virtual.ms"
    copy_ms(vis, virtual)
    with opened(virtual, True) as table:
        table.removecols(["MODEL_DATA"])
    assert casa.setjy(str(virtual), field="FLUX", standard="manual", usescratch=False,
                      fluxdensity=[7.25, 0, 0, 0]) is not None
    cases = (
        ("three-ordinates", {}, [(1, 6), (1, 0), (1, 4.47213595)]),
        ("three-ordinates-short-columns",
         dict(ydatacolumn=["model"]), [(1, 6), (1, 0), (1, 2)]),
        ("four-ordinates",
         dict(xaxis="amp", xdatacolumn="corrected", yaxis=["real", "real", "amp", "phase"],
              ydatacolumn=["model", "data", "corrected", "data"],
              yaxislocation=["left", "left", "right", "right"]),
         [(4.47213595, 6), (4.47213595, 2), (4.47213595, 4.47213595), (4.47213595, 0)]),
        ("virtual-model-on-third-ordinate",
         dict(vis=str(virtual), yaxis=["amp", "phase", "real"], ydatacolumn=["data", "data", "model"]),
         [(1, 2), (1, 0), (1, 7.25)]),
    )
    for name, options, first in cases:
        controls = base | options
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        points = rows(expected)
        assert len(points) == 12 * len(first)
        assert {point[2] for point in points} == {"1", "3"}
        assert {point[12] for point in points} == {"XX"}
        np.testing.assert_allclose(np.array([point[:2] for point in points[::12]], dtype=float),
                                   first, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(points)
        print(f"{name}: {len(points)} matching coordinates in ordinate order", flush=True)

    capture = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, primary = render(*args, **kwargs)
        capture.append([(axis.get_yaxis().get_label_position(), axis.get_ylabel(),
                         [(layer.get_label(), layer.get_xydata()) for layer in axis.lines])
                        for axis in figure.axes])
        return figure, primary

    dense = base | dict(correlation="")
    expected, actual = (root / f"three-ordinates-two-correlations.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**dense, plotfile=str(expected), expformat="txt") is None
    assert native(**dense, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 90
    assert [point[0] for point in rows(expected)[:5]] == ["1", "1", "2", "3", "3"]

    split = [("left", "Real:model, Amp:corrected", (0, 2)), ("right", "Phase (degrees)", (1,))]
    figures = (
        ("three-ordinates", base, "three-ordinates", split, 3),
        ("three-ordinates-scalar-side", base | dict(yaxislocation="right"), "three-ordinates",
         [("left", "Real:model, Phase (degrees), Amp:corrected", (0, 1, 2))], 3),
        ("three-ordinates-short-sides", base | dict(yaxislocation=["right"]), "three-ordinates",
         [("left", "Phase (degrees), Amp:corrected", (1, 2)), ("right", "Real:model", (0,))], 3),
        ("three-ordinates-two-correlations", dense | dict(xconnector="line"),
         "three-ordinates-two-correlations", split, 3),
    )
    multiple_baselines = root / "mixed-ordinate-baselines.ms"
    copy_ms(vis, multiple_baselines)
    with opened(multiple_baselines, True) as table:
        flags = table.getcol("FLAG")
        first_baseline = (table.getcol("ANTENNA1") == 0) & (table.getcol("ANTENNA2") == 1)
        flags[:, 2, first_baseline] = True
        table.putcol("FLAG", flags)
        flag_rows = table.getcol("FLAG_ROW")
        flag_rows[np.flatnonzero(first_baseline)[0]] = True
        table.putcol("FLAG_ROW", flag_rows)
        times = table.getcol("TIME")
        times = times.min() + times.max() - times
        table.putcol("TIME", times)
        table.putcol("TIME_CENTROID", times)
    both_baselines = dict(vis=str(multiple_baselines), antenna="A0&A1;A0&A2", xconnector="line")
    first_baseline_ids = {("1", "1", "XX"), ("2", "1", "YY"), ("3", "1", "XX")}
    mixed_cases = (
        ("u", "U (m)", {}, 30, 18, first_baseline_ids),
        ("elevation", "Elevation (deg)", {}, 30, 18, first_baseline_ids),
        ("u-baselines", "U (m)", both_baselines, 54, 30, {
            ("1", "1", "XX"), ("3", "1", "XX"), ("1", "2", "YY"),
            ("2", "2", "XX"), ("3", "2", "YY"),
        }),
        ("elevation-baselines", "Elevation (deg)", both_baselines, 54, 18, first_baseline_ids),
    )
    for case, label, overrides, visibility_count, metadata_count, identifiers in mixed_cases:
        axis = case.split("-")[0]
        name = f"three-ordinate-mixed-{case}"
        mixed = dense | overrides | dict(yaxis=["real", "phase", axis], expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**mixed, plotfile=str(expected)) is None
        points = rows(expected)
        assert len(points) == 2 * visibility_count + metadata_count
        real = points[:visibility_count]
        phase = points[visibility_count:2 * visibility_count]
        metadata = points[2 * visibility_count:]
        assert [point[2:] for point in real] == [point[2:] for point in phase]
        assert {(point[2], point[6], point[12]) for point in metadata} == identifiers
        assert native(**mixed, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(points)
        print(f"{name}: {visibility_count} Real and Phase, {metadata_count} {axis} coordinates",
              flush=True)
        sides = [("left", f"Real:model, {label}", (0, 2)), ("right", "Phase (degrees)", (1,))]
        figures += ((name, mixed, name, sides, (visibility_count, 2 * visibility_count)),)

    for name, controls, text, expectations, sections in figures:
        figure = controls | dict(expformat="png", width=500, height=360, dpi=100)
        expected, actual = (root / f"{name}.{engine}.png" for engine in ("casa", "native"))
        assert reference(**figure, plotfile=str(expected)) is None
        with patch.object(_plot_document, "render_document", record):
            assert native(**figure, plotfile=str(actual)) is None
        [axes] = capture
        capture.clear()
        coordinates = np.array([point[:2] for point in rows(root / f"{text}.casa.txt")], dtype=float)
        ordinates = np.split(coordinates, sections)
        assert len(axes) == len(expectations)
        for (side, label, layers), (expected_side, expected_label, indices) in zip(axes, expectations, strict=True):
            assert (side, label, len(layers)) == (expected_side, expected_label, len(indices))
            for (_, layer), index in zip(layers, indices, strict=True):
                np.testing.assert_allclose(layer, ordinates[index], rtol=1.2e-5, atol=1e-7)
        for output in (expected, actual):
            with Image.open(output) as image:
                assert image.size == (500, 360)

    colored = dense | both_baselines | dict(yaxis=["real", "phase", "elevation"],
                                            coloraxis="corr", expformat="png",
                                            width=500, height=360, dpi=100)
    expected, actual = (root / f"mixed-correlation-colors.{engine}.png" for engine in ("casa", "native"))
    assert reference(**colored, plotfile=str(expected)) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**colored, plotfile=str(actual)) is None
    [axes] = capture
    plotted_groups = dict(layer for _, _, layers in axes for layer in layers)
    points = rows(root / "three-ordinate-mixed-elevation-baselines.casa.txt")
    labels = ("Real:model", "Phase", "Elevation")
    assert len(plotted_groups) == 2 * len(labels)
    for label, section in zip(labels, np.split(points, (54, 108)), strict=True):
        for correlation in ("XX", "YY"):
            coordinates = np.array([point[:2] for point in section if point[12] == correlation], dtype=float)
            np.testing.assert_allclose(plotted_groups[f"{label} · {correlation}"], coordinates,
                                       rtol=1.2e-5, atol=1e-7)
    for output in (expected, actual):
        with Image.open(output) as image:
            assert image.size == (500, 360)
    print(f"mixed-correlation-colors: {len(points)} matching rendered coordinates in six groups",
          flush=True)
    with opened(virtual) as table:
        assert "MODEL_DATA" not in table.colnames()

    duplicate = base | dict(yaxis=["real", "amp", "real"], ydatacolumn=["model", "data", "model"])
    for engine, task in (("casa", reference), ("native", native)):
        output = root / f"three-ordinate-duplicate.{engine}.txt"
        try:
            task(**duplicate, plotfile=str(output), expformat="txt")
        except RuntimeError as error:
            assert "Remove duplicate y-axes." in str(error), error
        else:
            raise AssertionError(f"{engine} accepted duplicate ordinate and column pairs")
        assert not output.exists()

    invalid = base | dict(yaxislocation=["left", "right", "left", "top"])
    for engine, task in (("casa", reference), ("native", native)):
        output = root / f"three-ordinate-invalid-location.{engine}.txt"
        try:
            task(**invalid, plotfile=str(output), expformat="txt")
        except ValueError as error:
            assert "invalid yaxislocation value 'top'" in str(error), error
        else:
            raise AssertionError(f"{engine} accepted an invalid ordinate location before export")
        assert not output.exists()

    averaged = base | dict(avgchannel="2", expformat="txt")
    expected, actual = (root / f"three-ordinate-averaged.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**averaged, plotfile=str(expected)) is None
    assert native(**averaged, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 18


def compare_averaged_columns(root, native):
    work = root / "weighted-columns"
    work.mkdir()
    vis, _ = fixture(work)
    with opened(vis, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, spectrum in (("WEIGHT_SPECTRUM", [1, 9]), ("SIGMA_SPECTRUM", [1, 0.5])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                        dataManagerGroup=name)})
            values = np.resize(np.array(spectrum, dtype=np.float32), shape[1])[None, :, None]
            table.putcol(name, np.broadcast_to(values, shape))
        values = np.resize(np.array([1, 3, 5, 7], dtype=np.complex64), shape[1])[None, :, None]
        columns = {name: np.broadcast_to(10**factor * values, shape)
                   for factor, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA"))}
        flags = np.zeros(shape, dtype=bool)
        flags[:, 2, :] = True
        for name, data in (columns | dict(FLAG=flags)).items():
            table.putcol(name, data)

    without_corrected = work / "without-corrected.ms"
    without_weight = work / "without-weight-spectrum.ms"
    without_sigma = work / "without-sigma-spectrum.ms"
    for path, column in ((without_corrected, "CORRECTED_DATA"),
                         (without_weight, "WEIGHT_SPECTRUM"), (without_sigma, "SIGMA_SPECTRUM")):
        copy_ms(vis, path)
        with opened(path, True) as table:
            table.removecols([column])
    opposite_phase = work / "opposite-phase.ms"
    copy_ms(vis, opposite_phase)
    with opened(opposite_phase, True) as table:
        observed = table.getcol("DATA")
        observed[:, 1, :] *= -1
        table.putcol("DATA", observed)

    undefined_weight = work / "undefined-weight-spectrum.ms"
    copy_ms(vis, undefined_weight)
    with opened(undefined_weight, True) as table:
        name = "WEIGHT_SPECTRUM"
        descriptor = table.getcoldesc(name)
        table.removecols([name])
        table.addcols({name: descriptor})
        assert not table.iscelldefined(name, 0)
    zero_observed = work / "zero-observed-weight.ms"
    zero_corrected = work / "zero-corrected-weight.ms"
    for path, name in ((zero_observed, "SIGMA_SPECTRUM"), (zero_corrected, "WEIGHT_SPECTRUM")):
        copy_ms(vis, path)
        with opened(path, True) as table:
            spectrum = table.getcol(name)
            spectrum[:, :2, :] = 0
            table.putcol(name, spectrum)

    base = request(vis) | dict(xaxis="real", xdatacolumn="data", yaxis=["real", "real"],
                              ydatacolumn=["corrected", "model"], yaxislocation=["left", "right"],
                              avgchannel="2", spw="0:0~3")
    cases = (
        ("avg-independent-weighted", {}, ((2.6, 28), (2.6, 280))),
        ("avg-data-model-without-corrected", dict(vis=str(without_corrected),
                                                  ydatacolumn=["data", "model"]), ((2.6, 2.6), (2.6, 260))),
        ("avg-only-sigma-spectrum", dict(vis=str(without_weight)), ((2, 20), (2, 200))),
        ("avg-only-weight-spectrum", dict(vis=str(without_sigma)), ((2, 28), (2, 280))),
        ("avg-observed-corrected", dict(yaxis="real", ydatacolumn="corrected", yaxislocation="left"), ((2.6, 28),)),
        ("avg-model-only", dict(xaxis="channel", xdatacolumn="", yaxis="real", ydatacolumn="model",
                                yaxislocation="left"), ((0, 200),)),
        ("avg-secondary-shares-x", dict(xdatacolumn="corrected", yaxis=["real", "amp"],
                                       ydatacolumn=["data", "corrected"]), ((28, 2.6), (28, 28))),
        ("avg-scalar-without-time", dict(vis=str(opposite_phase), xaxis="amp", scalar=True),
         ((2.2, 28), (2.2, 280))),
        ("avg-undefined-weight-spectrum", dict(vis=str(undefined_weight)), ((2, 20), (2, 200))),
        ("avg-zero-primary-weight", dict(vis=str(zero_observed)), ((7, 70), (7, 700))),
        ("avg-zero-secondary-weights", dict(vis=str(zero_corrected)), ((2.6, 0), (2.6, 0))),
    )
    for name, overrides, first in cases:
        controls = base | overrides
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        channels = {"1"} if name == "avg-zero-primary-weight" else {"0", "1"}
        per_axis = 6 * len(channels)
        assert len(selected) == per_axis * len(first)
        assert {point[2] for point in selected} == channels
        assert {point[12] for point in selected} == {"XX"}
        np.testing.assert_allclose(np.array([point[:2] for point in selected[::per_axis]], dtype=float),
                                   first, rtol=1.2e-5)
        if name == "avg-independent-weighted":
            np.testing.assert_allclose(np.array([point[:2] for point in selected[1::12]], dtype=float),
                                       [(7, 70), (7, 700)], rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching channel-averaged coordinates", flush=True)
    compare_ordinate_figure(work, "avg-independent-weighted", native, base, "Real",
                            ["Real:corrected", "Real:model"])

    row_flag = work / "statistics-row-flag-only.ms"
    copy_ms(vis, row_flag)
    with opened(row_flag, True) as table:
        table.putcell("FLAG_ROW", 0, True)
    statistics = base | dict(xdatacolumn="model", ydatacolumn="", yaxislocation="left")
    statistic_cases = (
        ("model", {}, (10, 9), (5, 4), {0, 1}),
        ("model-only", dict(ydatacolumn="model"), (2, 1), (2, 1), {0, 1}),
        ("data", dict(xdatacolumn="data", ydatacolumn="data"), (5, 4), (5, 4), {0, 1}),
        ("corrected", dict(xdatacolumn="corrected", ydatacolumn="corrected"), (10, 9), (10, 9), {0, 1}),
        ("without-weight-spectrum", dict(vis=str(without_weight)), (2, 1), (5, 4), {0, 1}),
        ("without-sigma-spectrum", dict(vis=str(without_sigma)), (10, 9), (2, 1), {0, 1}),
        ("without-corrected", dict(vis=str(without_corrected)), (5, 4), (5, 4), {0, 1}),
        ("zero-observed", dict(vis=str(zero_observed)), (10, 9), (0, 4), {1}),
        ("row-flag-only", dict(vis=str(row_flag)), (10, 9), (5, 4), {0, 1}),
        ("single-channel", dict(spw="0:3"), (9,), (4,), {0}),
    )
    for name, overrides, weights, inverse_variances, bins in statistic_cases:
        sigmas = [1 / np.sqrt(value) if value > 0 else -1 for value in inverse_variances]
        coordinates = dict(
            weight=[np.mean(weights)] * len(weights),
            weightspectrum=weights,
            sigma=[np.mean(sigmas)] * len(sigmas),
            sigmaspectrum=sigmas,
        )
        for axis, values in coordinates.items():
            options = statistics | overrides | dict(yaxis=axis, expformat="txt")
            expected, actual = (work / f"avg-{name}-{axis}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected)) is None
            selected = rows(expected)
            channels = [int(point[2]) for point in selected]
            assert len(selected) == 6 * len(bins)
            assert set(channels) == bins
            np.testing.assert_allclose([float(point[1]) for point in selected],
                                       [values[channel] for channel in channels], rtol=1.2e-5)
            assert native(**options, plotfile=str(actual)) is None
            assert compare_text(actual, expected) == len(selected)
            print(f"avg-{name}-{axis}: {len(selected)} matching channel statistics", flush=True)
    for name, source, column, coordinates in (
        ("masked-model", vis, "model", [700, 1]),
        ("masked-data", without_weight, "data", [7, 4]),
    ):
        masked = work / f"{name}.ms"
        copy_ms(source, masked)
        with opened(masked, True) as table:
            flags_for_case = table.getcol("FLAG")
            flags_for_case[:, :2, :] = True
            table.putcol("FLAG", flags_for_case)
        options = statistics | dict(vis=str(masked), xaxis="channel", xdatacolumn="",
                                    yaxis=["real", "weightspectrum"], ydatacolumn=[column, column],
                                    yaxislocation=["left", "right"], expformat="txt")
        expected, actual = (work / f"avg-{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 12
        assert {point[2] for point in selected} == {"1"}
        np.testing.assert_allclose([float(point[1]) for point in selected],
                                   np.repeat(coordinates, 6), rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"avg-{name}: {len(selected)} matching channel statistics", flush=True)
    compare_figure(work, native, statistics | dict(yaxis="weight"), work / "avg-model-weight.casa.txt",
                   ("Wt vs. Real:model", "Real:model", "Weight"))

    average_time = base | dict(avgtime="20", expformat="txt")
    expected, actual = (work / f"avg-with-time.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**average_time, plotfile=str(expected)) is None
    assert len(rows(expected)) == 12
    assert native(**average_time, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    with opened(vis) as table:
        for name, data in (columns | dict(FLAG=flags)).items():
            np.testing.assert_array_equal(table.getcol(name), data)


def time_average_fixture(root):
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        _, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        shape = table.getcol("DATA").shape
        base = np.where(integration % 2 == 0, 1, 3)
        for factor, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            values = (10 ** factor * base).astype(np.complex64)[None, None, :]
            table.putcol(name, np.broadcast_to(values, shape))
        for name, later in (("WEIGHT", 9), ("SIGMA", 0.5)):
            values = np.where(integration % 2 == 0, 1, later).astype(np.float32)
            table.putcol(name, np.broadcast_to(values, (shape[0], shape[2])))
    return vis, integration


def compare_time_averaged_flag_symbols(root, native):
    from plotms_callibrary_oracle import snapshot, verify_source

    work = root / "time-averaged-flag-symbols"
    work.mkdir()
    vis, integration = time_average_fixture(work)
    with opened(vis, True) as table:
        later = integration % 2 != 0
        for name in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(name)
            values[:, :, later] *= 1j
            table.putcol(name, values)
        flags = np.zeros_like(table.getcol("FLAG"))
        flags[:, 2, :] = ~later ^ (integration >= 4)
        flags[:, 3, :] = True
        table.putcol("FLAG", flags)
    before = snapshot(vis)
    base = request(vis) | dict(yaxis="amp", avgtime="20", averagedata=True,
                               title="Flagged visibility")
    marker_color = "dd2255"
    styles = dict(customflaggedsymbol=True, flaggedsymbolshape="diamond",
                  flaggedsymbolcolor=marker_color, flaggedsymbolsize=4,
                  flaggedsymbolfill="fill", flaggedsymboloutline=False)
    variants = (
        ("vector", False, 3, [2.40832, 27.0185], [5, 25, 45]),
        ("scalar", True, 2, [2.63636, 26.3636], [10, 40]),
    )
    with opened(vis) as table:
        origin = np.unique(table.getcol("TIME"))[0]
    for kind, scalar, groups, first, offsets in variants:
        clear = base | dict(scalar=scalar)
        marked = clear | styles
        modes = (
            ("clear", clear, [1, 2]),
            ("both", marked, [1, 2, 3]),
            ("flagged", marked | dict(customsymbol=True, symbolshape="nosymbol"), [3]),
        )
        for mode, options, channels in modes:
            name = f"time-{kind}-symbols-{mode}"
            expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected), expformat="txt") is None
            points = rows(expected)
            assert [int(point[2]) for point in points] == channels * groups
            assert np.allclose([float(point[9]) for point in points[::len(channels)]],
                               origin + np.asarray(offsets), rtol=0, atol=0.00051)
            if mode == "both":
                np.testing.assert_allclose([float(points[1][0]), float(points[1][1])],
                                           [3, 30], rtol=1.2e-5)
                for slot in (0, 2):
                    np.testing.assert_allclose([float(value) for value in points[slot][:2]],
                                               first, rtol=1.2e-5)
                if not scalar:
                    np.testing.assert_allclose([float(value) for value in points[-2][:2]], [1, 10])
            assert native(**options, plotfile=str(actual), expformat="txt") is None
            assert compare_text(actual, expected) == len(points)
            print(f"{name}: {len(points)} matching time-averaged coordinates and flags", flush=True)
        compare_flagged_figure(work, native, f"time-{kind}-symbols-both", marked, marker_color,
                               labels=("Flagged visibility", "Amp", "Amp:corrected"),
                               is_flagged=lambda point: point[2] == "3")
    compare_empty_export(work, native, "time-all-flagged", base | dict(spw="0:3"),
                         ("Amp", "Amp:corrected"))
    verify_source(vis, before)


def compare_reduced_flag_symbols(root, native):
    from plotms_callibrary_oracle import snapshot, verify_source
    from plotms_ms_spw_average_oracle import make_fixture

    work = root / "reduced-flag-symbols"
    work.mkdir()
    vis = make_fixture(work, antenna_count=3)
    with opened(vis, True) as table:
        first_window = table.getcol("DATA_DESC_ID") == 0
        first_baseline = table.getcol("ANTENNA2") == 1
        flags = np.zeros_like(table.getcol("FLAG"))
        flags[:, 2, first_window & first_baseline] = True
        flags[:, 3, :] = True
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", first_window & first_baseline)
        for name in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(name)
            values *= np.where(first_baseline, 1, 2)
            table.putcol(name, values)
    before = snapshot(vis)
    base = request(vis) | dict(yaxis="amp", averagedata=True, expformat="txt",
                               title="Flagged visibility")
    marker_color = "dd2255"
    styles = dict(customflaggedsymbol=True, flaggedsymbolshape="diamond",
                  flaggedsymbolcolor=marker_color, flaggedsymbolsize=4,
                  flaggedsymbolfill="fill", flaggedsymboloutline=False)
    cases = (
        ("antenna", dict(avgantenna=True, antenna="A0&A1;A0&A2"),
         "0", "-1", ((1, 0), (2, 0), (3, 0), (1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (3, 2)),
         {(3, 0), (2, 1), (3, 1), (3, 2)}),
        ("spw", dict(avgspw=True, spw="0:1~3,1:1~3"),
         "0", "1", ((1, 0), (2, 0), (3, 0)), {(3, 0)}),
    )
    def identity(point):
        return int(point[2]), int(point[5])

    for reduction, averaging, window, antenna2, identities, flagged in cases:
        for kind, scalar in (("vector", False), ("scalar", True)):
            clear = base | averaging | dict(scalar=scalar)
            marked = clear | styles
            names = (
                ("clear", clear, tuple(key for key in identities if key not in flagged)),
                ("both", marked, identities),
                ("flagged", marked | dict(customsymbol=True, symbolshape="nosymbol"),
                 tuple(key for key in identities if key in flagged)),
            )
            for mode, options, members in names:
                name = f"{reduction}-{kind}-symbols-{mode}"
                expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
                assert reference(**options, plotfile=str(expected)) is None
                points = rows(expected)
                assert tuple(map(identity, points)) == members * 2
                assert {(point[11], point[6], point[12]) for point in points} == {(window, antenna2, "XX")}
                assert native(**options, plotfile=str(actual)) is None
                assert compare_text(actual, expected) == len(points)
                print(f"{name}: {len(points)} matching coordinates, identifiers and flags", flush=True)
            if not scalar:
                compare_flagged_figure(work, native, f"{reduction}-{kind}-symbols-both", marked, marker_color,
                                       labels=("Flagged visibility", "Amp", "Amp:corrected"),
                                       is_flagged=lambda point: identity(point) in flagged)
    verify_source(vis, before)


def compare_time_averaged_statistics(root, native):
    from casars.tasks import TaskParameterError
    from plotms_callibrary_oracle import snapshot, verify_source

    work = root / "time-visibility-statistics"
    work.mkdir()
    vis, integrations = time_average_fixture(work)
    fallback = work / "without-spectra.ms"
    copy_ms(vis, fallback)
    even = integrations % 2 == 0
    with opened(vis, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        channel = np.arange(shape[1])[None, :, None]
        spectra = (
            ("WEIGHT_SPECTRUM", (1 + 2 * channel) * np.where(even, 3, 1)),
            ("SIGMA_SPECTRUM", np.where((channel % 2 == 0) ^ even, 1, .5)),
        )
        for name, values in spectra:
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            table.putcol(name, np.broadcast_to(values.astype(np.float32), shape).copy())
        flags = table.getcol("FLAG")
        flags[0, 0, :] = ~even
        flags[0, 2, :] = True
        table.putcol("FLAG", flags)
    with opened(fallback, True) as table:
        table.putcol("FLAG", flags)
    before = {source: snapshot(source) for source in (vis, fallback)}

    base = request(vis) | dict(xaxis="real", xdatacolumn="data", ydatacolumn="data", spw="0:0~3",
                               avgtime="20", avgchannel="2", expformat="txt")
    observed = [17 / 9, 2.6]
    no_spectra = [7 / 3, 2.6]
    cases = (
        ("observed-weight", dict(yaxis="wtsp"), (0, 1), observed, (9, 5)),
        ("corrected-weight", dict(yaxis="wtsp", ydatacolumn="corrected"), (0, 1), observed, (15, 28)),
        ("observed-sigma", dict(yaxis="sigmasp"), (0, 1), observed, (1 / 3, 1 / np.sqrt(5))),
        ("mixed-sigma", dict(yaxis="sigmasp", ydatacolumn="corrected"),
         (0, 1), observed, (1 / 3, 1 / np.sqrt(5))),
        ("corrected-sigma", dict(yaxis="sigmasp", xdatacolumn="corrected", ydatacolumn="corrected"),
         (0, 1), (14, 15), (1 / np.sqrt(15), 1 / np.sqrt(28))),
        ("row-weight-fallback", dict(vis=str(fallback), yaxis="wtsp", ydatacolumn="corrected"),
         (0, 1), no_spectra, (11, 10)),
        ("row-sigma-fallback", dict(vis=str(fallback), yaxis="sigmasp"),
         (0, 1), no_spectra, (1 / np.sqrt(6), 1 / np.sqrt(5))),
        ("mixed-weighted-amplitude-fallback",
         dict(vis=str(fallback), xaxis="wt*amp", yaxis="sigmasp", ydatacolumn="corrected"),
         (0, 1), (24.5, 27.3), (1 / np.sqrt(6), 1 / np.sqrt(5))),
        ("weight-spectrum-abscissa", dict(xaxis="wtsp", xdatacolumn="corrected", yaxis="amp"),
         (0, 1), (15, 28), observed),
        ("time-only-weight", dict(yaxis="wtsp", avgchannel=""), (0, 1, 3), (1, 2.6, 2.6), (4, 5, 5)),
        ("time-channel-weighted-amplitude", dict(yaxis="wt*amp"),
         (0, 1), observed, (119 / 9, 18.2)),
        ("time-weighted-amplitude", dict(xaxis="amp", yaxis="wt*amp", avgchannel=""),
         (0, 1, 3), (1, 2.6, 2.6), (14 / 3, 2.6 * 14 / 3, 2.6 * 14 / 3)),
        ("channel-weighted-amplitude", dict(xaxis="amp", yaxis="wt*amp", avgtime=""),
         (0, 1, 0, 1), (1, 1, 3, 3), (3, 3, 12, 12)),
        ("raw-weighted-amplitude", dict(xaxis="amp", yaxis="wt*amp", ydatacolumn="corrected",
                                        avgtime="", avgchannel=""),
         (0, 1, 3, 1, 3), (1, 1, 1, 3, 3), (10, 10, 10, 270, 270)),
        ("raw-weighted-abscissa", dict(xaxis="wt*amp", yaxis="amp", ydatacolumn="corrected",
                                      avgtime="", avgchannel=""),
         (0, 1, 3, 1, 3), (1, 1, 1, 27, 27), (10, 10, 10, 30, 30)),
    )
    for name, changes, channels, x, y in cases:
        options = base | changes
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert [int(point[2]) for point in selected] == list(channels) * 3
        assert {point[12] for point in selected} == {"XX"}
        coordinates = np.array([point[:2] for point in selected], dtype=float)
        np.testing.assert_allclose(coordinates, np.tile(np.column_stack((x, y)), (3, 1)), rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching weighted visibility/statistic coordinates", flush=True)

    title = "Time and channel weighted amplitude"
    compare_figure(work, native, base | dict(yaxis="wt*amp", title=title),
                   work / "time-channel-weighted-amplitude.casa.txt", (title, "Real", "Wt*Amp"))
    title = "Time and weight spectrum"
    default_time = base | dict(xaxis="time", xdatacolumn="", yaxis="wtsp", title=title)
    expected, actual = (work / f"time-weight-spectrum.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**default_time, plotfile=str(expected)) is None
    selected = rows(expected)
    assert [int(point[2]) for point in selected] == [0, 1] * 3
    assert [float(point[1]) for point in selected] == [9, 5] * 3
    for point in selected:
        assert float(point[0]) == float(point[9])
    assert native(**default_time, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 6
    np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                               [float(point[0]) for point in selected], rtol=0, atol=0.00051)
    print("time-weight-spectrum: six matching default Time/WtSp coordinates and time anchors", flush=True)
    compare_figure(work, native, default_time, expected, (title, "Time (MJD seconds)", "Weight Spectrum"))
    for mode, axis, options in (
        ("antenna", "wtsp", dict(avgantenna=True, avgchannel="", antenna="A0&A1;A0&A2")),
        ("baseline", "wtsp", dict(avgbaseline=True)),
        ("spectral-window", "sigmasp", dict(avgspw=True)),
        ("scalar", "wt*amp", dict(scalar=True)),
    ):
        expected, actual = (work / f"{mode}-weight-refusal.{engine}.txt" for engine in ("casa", "native"))
        controls = base | options | dict(yaxis=axis)
        try:
            reference(**controls, plotfile=str(expected))
        except Exception as error:
            assert callable(getattr(error, "details", None)) and error.details() == "plot parameters failed", error
        else:
            raise AssertionError(f"CASA accepted {mode} weight averaging")
        try:
            native(**controls, plotfile=str(actual))
        except TaskParameterError as error:
            assert "Selected averaging does not yet support Weight and Sigma axes." in str(error), error
        else:
            raise AssertionError(f"casa-rs accepted {mode} weight averaging")
        assert not expected.exists() and not actual.exists()
    for source, previous in before.items():
        verify_source(source, previous)
    print("weight statistics: four paired averaging refusals before export", flush=True)


def compare_time_averaged_columns(root, native):
    work = root / "time-averaged-columns"
    work.mkdir()
    vis, integration = time_average_fixture(work)
    mixed_flags, spectra, zero_weights, zero_corrected, no_corrected = (
        work / f"{name}.ms" for name in ("mixed-flags", "spectra", "zero-weight", "zero-corrected", "no-corrected")
    )
    for path in (mixed_flags, spectra, zero_weights, zero_corrected, no_corrected):
        copy_ms(vis, path)
    with opened(mixed_flags, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, :] = np.isin(integration, [0, 2, 3])
        table.putcol("FLAG", flags)
    with opened(spectra, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, earlier, later in (("WEIGHT_SPECTRUM", 3, 1), ("SIGMA_SPECTRUM", 0.5, 1)):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            values = np.where(integration % 2 == 0, earlier, later).astype(np.float32)
            table.putcol(name, np.broadcast_to(values, shape))
    for path, columns in ((zero_weights, ("WEIGHT", "SIGMA")), (zero_corrected, ("WEIGHT",))):
        with opened(path, True) as table:
            for name in columns:
                table.putcol(name, np.zeros_like(table.getcol(name)))
    with opened(no_corrected, True) as table:
        table.removecols(["CORRECTED_DATA"])
    trailing, leading, row_flag_only, reversed_time, weighted_uvw = (
        work / f"{name}.ms" for name in ("trailing", "leading", "row-flag-only", "reversed", "weighted-uvw")
    )
    odd = integration % 2 == 1
    for path in (trailing, leading, row_flag_only, reversed_time, weighted_uvw):
        copy_ms(vis, path)
        with opened(path, True) as table:
            if path == trailing:
                flag = table.getcol("FLAG")
                flag[0, 1, odd] = True
                table.putcol("FLAG", flag)
            elif path == leading:
                flag = table.getcol("FLAG")
                flag[:, :, ~odd] = True
                table.putcol("FLAG", flag)
                table.putcol("FLAG_ROW", ~odd)
            elif path == row_flag_only:
                table.putcol("FLAG_ROW", odd)
            elif path == reversed_time:
                times = np.unique(table.getcol("TIME"))
                reversed_times = times[-1 - integration]
                table.putcol("TIME", reversed_times)
                table.putcol("TIME_CENTROID", reversed_times)
                data = table.getcol("DATA")
                data[:, :, odd] = -1
                table.putcol("DATA", data)
                table.putcol("SIGMA", np.ones_like(table.getcol("SIGMA")))
            else:
                uvw = np.zeros_like(table.getcol("UVW"))
                uvw[0, odd] = 10
                uvw[1] = -2 * uvw[0] - 1
                uvw[2] = 3 * uvw[0] + 2
                table.putcol("UVW", uvw)
                weights = table.getcol("WEIGHT")
                weights[1] = np.where(odd, 1, 5)
                table.putcol("WEIGHT", weights)

    base = request(vis) | dict(xaxis="real", xdatacolumn="data", yaxis=["real", "real"],
                               ydatacolumn=["corrected", "model"], yaxislocation=["left", "right"],
                               spw="0:1", avgtime="20")
    independent = [(2.6, 28)] * 3 + [(2.6, 280)] * 3
    corrected = [(7 / 3, 290 / 11), (25 / 9, 550 / 19)]
    model = [(7 / 3, 2900 / 11), (25 / 9, 5500 / 19)]
    cases = (
        ("time-independent", {}, independent, 20),
        ("time-flagged-bins", dict(vis=str(mixed_flags)), [(3, 30), (2.6, 28), (3, 300), (2.6, 280)], 40),
        ("time-spectral-weights", dict(vis=str(spectra)), [(1.4, 15)] * 3 + [(1.4, 150)] * 3, 20),
        ("time-zero-weight", dict(vis=str(zero_weights)), [(0, 0)] * 6, 20),
        ("time-zero-corrected", dict(vis=str(zero_corrected)), [(2.6, 0)] * 6, 20),
        ("time-no-corrected", dict(vis=str(no_corrected), ydatacolumn=["data", "model"]),
         [(2.6, 2.6)] * 3 + [(2.6, 260)] * 3, 20),
        ("time-shared-abscissa", dict(xdatacolumn="corrected", yaxis=["real", "amp"],
                                      ydatacolumn=["data", "corrected"]), [(28, 2.6)] * 3 + [(28, 28)] * 3, 20),
        ("time-short-window", dict(avgtime="4"), [(1, 10), (3, 30)] * 3 + [(1, 100), (3, 300)] * 3, 10),
        ("time-15-second-window", dict(avgtime="15"), independent, 20),
        ("time-25-second-window", dict(avgtime="25"), corrected + model, 30),
        ("time-scan-boundary", dict(avgtime="25", field=""), corrected * 2 + model * 2, 30),
        ("time-trailing-cell-flag", dict(vis=str(trailing)), [(1, 10)] * 3 + [(1, 100)] * 3, 20),
        ("time-leading-row-flag", dict(vis=str(leading)), [(3, 30)] * 3 + [(3, 300)] * 3, 20),
        ("time-row-flag-only", dict(vis=str(row_flag_only)), independent, 20),
        ("time-reversed-and-vector", dict(vis=str(reversed_time), xaxis="amp"),
         [(0, 28)] * 3 + [(0, 280)] * 3, 20),
    )
    for name, overrides, coordinates, spacing in cases:
        controls = base | overrides
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == len(coordinates)
        assert {row[2] for row in selected} == {"1"}
        assert {row[12] for row in selected} == {"XX"}
        np.testing.assert_allclose(np.array([row[:2] for row in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        times = [float(row[9]) for row in selected[:len(selected) // 2]]
        np.testing.assert_allclose(np.diff(times), spacing, rtol=0, atol=0.001)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching time-averaged coordinates", flush=True)
    for axis in ("time", "channel", "frequency"):
        controls = base | dict(xaxis=axis, xdatacolumn="", expformat="txt")
        expected, actual = (work / f"average-{axis}-axis.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 6
        if axis == "time":
            for path in (expected, actual):
                assert all(float(row[0]) == float(row[9]) for row in rows(path))
    flagged_uvw = work / "flagged-uvw.ms"
    copy_ms(weighted_uvw, flagged_uvw)
    with opened(flagged_uvw, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, ~odd] = True
        table.putcol("FLAG", flags)
    uvw_controls = base | dict(vis=str(weighted_uvw), xaxis="u", xdatacolumn="")
    single_ordinate = dict(yaxis="real", yaxislocation="left")
    coordinate_cases = (
        ("u", {}, 9, 0, 6),
        ("v", dict(xaxis="v"), -19, 0, 6),
        ("w", dict(xaxis="w"), 29, 0, 6),
        ("uvdist", dict(xaxis="uvdist"), np.hypot(9, -19), 0, 6),
        ("model-only", single_ordinate | dict(ydatacolumn="model"), 5, 0, 3),
        ("observed-only", single_ordinate | dict(ydatacolumn="data"), 8, 0, 3),
        ("both-correlations", single_ordinate | dict(ydatacolumn="corrected", correlation="XX,YY"), 6.25, 0, 6),
        ("ordinate", single_ordinate | dict(xaxis="real", xdatacolumn="corrected", yaxis="u", ydatacolumn=""),
         9, 1, 3),
        ("distance-ordinate",
         single_ordinate | dict(xaxis="real", xdatacolumn="corrected", yaxis="uvdist", ydatacolumn=""),
         np.hypot(9, -19), 1, 3),
        ("one-clear-integration", dict(vis=str(flagged_uvw)), 10, 0, 6),
        ("one-clear-channel", dict(vis=str(flagged_uvw), spw="0:0~1"), 9, 0, 12),
    )
    for name, overrides, coordinate, side, count in coordinate_cases:
        options = uvw_controls | overrides | dict(expformat="txt")
        expected, actual = (work / f"time-weighted-{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == count
        np.testing.assert_allclose([float(point[side]) for point in selected], coordinate, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        print(f"time-weighted-{name}: {count} matching time-averaged UVW coordinates", flush=True)
    compare_ordinate_figure(work, "time-weighted-u", native, uvw_controls, "U (m)",
                            ["Real:corrected", "Real:model"])
    distance_ordinate = uvw_controls | single_ordinate | dict(
        xaxis="real", xdatacolumn="corrected", yaxis="uvdist", ydatacolumn="", avgtime="", expformat="txt",
    )
    reordered_uvw = work / "reordered-uvw.ms"
    copy_ms(weighted_uvw, reordered_uvw)
    with opened(reordered_uvw, True) as table:
        times = table.getcol("TIME")
        reversed_times = times.min() + times.max() - times
        table.putcol("TIME", reversed_times)
        table.putcol("TIME_CENTROID", reversed_times)
        table.putcell("FLAG_ROW", 0, True)
    unaveraged_distances = [1, np.hypot(10, 21)]
    channel_distance = dict(spw="0:0~1", avgchannel="2")
    reordered_distance = dict(vis=str(reordered_uvw))
    for name, overrides, distances in (
        ("raw", {}, unaveraged_distances),
        ("channel", channel_distance, unaveraged_distances),
        ("raw-reordered", reordered_distance, unaveraged_distances[::-1]),
        ("channel-reordered", channel_distance | reordered_distance, unaveraged_distances[::-1]),
    ):
        options = distance_ordinate | overrides
        expected, actual = (work / f"{name}-distance-ordinate.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        np.testing.assert_allclose([float(point[1]) for point in rows(expected)],
                                   np.tile(distances, 3), rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 6
        print(f"{name}-distance-ordinate: 6 matching UV distance coordinates", flush=True)
    from casars.tasks import TaskExecutionError, TaskParameterError
    from plotms_callibrary_oracle import library_entry

    output = work / "metadata-distance-ordinate.native.txt"
    try:
        native(**(distance_ordinate | dict(xaxis="time", xdatacolumn="")), plotfile=str(output))
    except TaskParameterError as error:
        assert "UV distance ordinate combination is pending (#1032)" in str(error)
    else:
        raise AssertionError("PlotMS admitted a pending UV distance and metadata pair")
    assert not output.exists()

    unity = work / "uvw-unity.cal"
    casa.gencal(str(weighted_uvw), str(unity), "amp", parameter=[1.0])
    calibrated = distance_ordinate | dict(avgtime="20", callib=library_entry(unity))
    reference_path = work / "callib-distance-ordinate.casa.txt"
    assert reference(**calibrated, plotfile=str(reference_path)) is None
    selected = rows(reference_path)
    assert len(selected) == 3
    np.testing.assert_allclose([float(point[1]) for point in selected], np.hypot(8, 17), rtol=1.2e-5)
    output = work / "callib-distance-ordinate.native.txt"
    try:
        native(**calibrated, plotfile=str(output))
    except TaskExecutionError as error:
        assert "UV distance ordinates with a callibrary or direct msexplore are pending (#1032)" in str(error)
    else:
        raise AssertionError("PlotMS admitted a pending time-averaged callibrary UV distance ordinate")
    assert not output.exists()
    compare_ordinate_figure(work, "time-independent", native, base, "Real", ["Real:corrected", "Real:model"])


def compare_combined_averaged_columns(root, native):
    work = root / "combined-averaged-columns"
    work.mkdir()
    source, integration = time_average_fixture(work)
    base = request(source) | dict(xaxis="real", xdatacolumn="data", yaxis=["real", "real"],
                                  ydatacolumn=["corrected", "model"], yaxislocation=["left", "right"],
                                  spw="0:0~1", avgtime="20", avgchannel="2")
    even = integration % 2 == 0
    cases = (
        ("spectra", {}, 0, 25 / 7, 550 / 15),
        ("spectra-mixed", {}, 0, 3, 480 / 14),
        ("row-mixed", {}, 0, 4, 490 / 11),
        ("zero-primary-one-channel", {}, 0, 3.8, 550 / 15),
        ("zero-primary-first-bin", dict(spw="0:0~3"), 1, 25 / 7, 550 / 15),
        ("zero-secondary", {}, 0, 25 / 7, 0),
        ("discarded-tail", dict(spw="0:0~2"), 0, 2.6, 28),
    )
    for name, overrides, channel, observed, corrected in cases:
        vis = work / f"{name}.ms"
        copy_ms(source, vis)
        with opened(vis, True) as table:
            shape = table.getcol("DATA").shape
            odd_channel = np.arange(shape[1])[None, :, None] % 2 == 1
            amplitude = (1 + 4 * (~even)[None, None, :] + 2 * odd_channel).astype(np.complex64)
            for factor, column in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
                table.putcol(column, np.broadcast_to(amplitude * 10**factor, shape).copy())
            flags = np.zeros(shape, dtype=bool)
            if name.endswith("mixed"):
                flags[0, 1, ~even] = True
            if name == "discarded-tail":
                flags[0, :2, ~even] = True
            table.putcol("FLAG", flags)
            if name != "row-mixed":
                descriptor = table.getcoldesc("DATA")
                for column, earlier, later in (("WEIGHT_SPECTRUM", [1, 9], [4, 1]),
                                                ("SIGMA_SPECTRUM", [1, 0.5], [1, 1])):
                    table.addcols({column: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                                dataManagerGroup=column)})
                    pattern = np.where(even[None, :], np.resize(earlier, shape[1])[:, None],
                                       np.resize(later, shape[1])[:, None]).astype(np.float32)
                    if name == "zero-primary-first-bin" and column == "SIGMA_SPECTRUM":
                        pattern[:2, :] = 0
                    if name == "zero-primary-one-channel" and column == "SIGMA_SPECTRUM":
                        pattern[0, :] = 0
                    if name == "zero-secondary" and column == "WEIGHT_SPECTRUM":
                        pattern.fill(0)
                    table.putcol(column, np.broadcast_to(pattern, shape).copy())
        controls = base | dict(vis=str(vis)) | overrides
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, expformat="txt", plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 6
        assert {point[2] for point in selected} == {str(channel)}
        assert {point[10] for point in selected} == {f"{100.0005 + channel * 0.002:.9f}"}
        assert {point[12] for point in selected} == {"XX"}
        coordinates = [(observed, corrected)] * 3 + [(observed, 10 * corrected)] * 3
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        assert native(**controls, expformat="txt", plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching channel-and-time coordinates", flush=True)
    figure = base | dict(vis=str(work / "spectra.ms"))
    compare_ordinate_figure(work, "spectra", native, figure, "Real", ["Real:corrected", "Real:model"])
    compare_mixed_float_time_normalization(work, source, native, base)
    flagged = work / "all-cube-flags.ms"
    copy_ms(work / "spectra.ms", flagged)
    with opened(flagged, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
        assert not table.getcol("FLAG_ROW").any()
    for name, vis in (("zero-primary-empty", work / "zero-primary-first-bin.ms"), ("cube-masked-empty", flagged)):
        compare_empty_combined_average(work, native, name, base | dict(vis=str(vis)))


def compare_empty_combined_average(work, native, name, controls):
    from casars import _plot_document

    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        assert axis.get_xlabel() == "Real"
        assert [item.get_ylabel() for item in figure.axes] == ["Real:corrected", "Real:model"]
        assert all(not (item.lines or item.collections) for item in figure.axes)
        return figure, axis

    for verbose in (True, False):
        expected, actual = (work / f"{name}-{verbose}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), verbose=verbose, expformat="txt") is None
        assert not rows(expected) and expected.read_text().count("# No data to export") == 2
        assert native(**controls, plotfile=str(actual), verbose=verbose, expformat="txt") is None
        assert actual.read_bytes() == expected.read_bytes()

    figure = controls | dict(width=500, height=360, dpi=100, expformat="png")
    expected, actual = (work / f"{name}.{engine}.png" for engine in ("casa", "native"))
    assert reference(**figure, plotfile=str(expected)) is None
    with patch.object(_plot_document, "render_document", side_effect=record) as renderer:
        assert native(**figure, plotfile=str(actual)) is None
    renderer.assert_called_once()
    for path in (expected, actual):
        with Image.open(path) as image:
            assert image.size == (500, 360), (path, image.size)
    print(f"{name}: two matching empty text exports and a 500 by 360 PNG", flush=True)


def compare_mixed_float_time_normalization(work, source, native, base):
    vis = work / "unused-float.ms"
    copy_ms(source, vis)
    with opened(vis, True) as table:
        for name in ("DATA", "CORRECTED_DATA", "MODEL_DATA", "WEIGHT", "SIGMA"):
            table.putcol(name, np.ones_like(table.getcol(name)))
        descriptor = table.getcoldesc("DATA")
        table.addcols({"FLOAT_DATA": dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                         dataManagerGroup="FLOAT_DATA")})
        table.putcol("FLOAT_DATA", np.ones(table.getcol("DATA").shape, dtype=np.float32))
    for width, count in (("", 12), ("2", 6)):
        controls = base | dict(vis=str(vis), xaxis="channel", xdatacolumn="", avgchannel=width, expformat="txt")
        name = f"unused-float-time-{width or 'only'}"
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        assert native(**controls, plotfile=str(actual)) is None
        casa_rows, native_rows = rows(expected), rows(actual)
        assert len(casa_rows) == len(native_rows) == count
        np.testing.assert_allclose([float(point[1]) for point in casa_rows], 2 / 3, rtol=1.2e-5)
        assert {float(point[1]) for point in native_rows} == {1.0}
        assert [point[:1] + point[2:] for point in native_rows] == [point[:1] + point[2:] for point in casa_rows]
        print(f"{name}: native preserves {count} constant unit visibilities", flush=True)


def time_scan_field_fixture(work):
    vis, integration = time_average_fixture(work)
    with opened(vis, True) as table:
        times = np.unique(table.getcol("TIME"))
        shape = table.getcol("DATA").shape
        first_scan = integration < 6
        amplitude = np.where(first_scan, 1, 3).astype(np.complex64)
        for factor, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(name, np.broadcast_to(amplitude * 10**factor, shape).copy())
        for name, later in (("WEIGHT", 9), ("SIGMA", 0.5)):
            values = np.where(first_scan, 1, later).astype(np.float32)
            table.putcol(name, np.broadcast_to(values, (shape[0], shape[2])).copy())
    scan, field, reversed_time = (work / f"{name}.ms" for name in ("scans", "fields", "reversed"))
    for path in (scan, field, reversed_time):
        copy_ms(vis, path)
        with opened(path, True) as table:
            if path == reversed_time:
                reversed_times = times[-1 - integration]
                table.putcol("TIME", reversed_times)
                table.putcol("TIME_CENTROID", reversed_times)
            else:
                name = "FIELD_ID" if path == scan else "SCAN_NUMBER"
                values = table.getcol(name)
                table.putcol(name, np.full_like(values, values[0]))
    return vis, times, scan, field, reversed_time


def compare_time_scan_field_columns(root, native):
    work = root / "scan-field-time-columns"
    work.mkdir()
    vis, times, scan, field, reversed_time = time_scan_field_fixture(work)
    base = request(vis) | dict(field="", xaxis="real", xdatacolumn="data", yaxis=["real", "real"],
                               ydatacolumn=["corrected", "model"], yaxislocation=["left", "right"],
                               spw="0:1", avgtime="200")
    separate = [(1, 10), (3, 30), (1, 100), (3, 300)]
    joined = [(2.6, 28), (2.6, 280)]
    separate_ids = [("1", "0"), ("2", "1")] * 2
    joined_ids = [("1", "0")] * 2
    split = [(15 / 7, 25), (3, 30), (15 / 7, 250), (3, 300)]
    color_controls = dict(avgscan=True, avgfield=True, avgtime="75")
    cases = (
        ("separate-boundaries", {}, separate, separate_ids),
        ("cross-scan", dict(vis=str(scan), avgscan=True), joined, joined_ids),
        ("cross-field", dict(vis=str(field), avgfield=True), joined, joined_ids),
        ("cross-scan-color", dict(vis=str(scan), avgscan=True, coloraxis="scan"), joined, joined_ids),
        ("cross-field-color", dict(vis=str(field), avgfield=True, coloraxis="field"), joined, joined_ids),
        ("cross-scan-retains-field", dict(avgscan=True), separate, separate_ids),
        ("cross-field-retains-scan", dict(avgfield=True), separate, separate_ids),
        ("cross-both", dict(avgscan=True, avgfield=True), joined, joined_ids),
        ("cross-both-channel", dict(avgscan=True, avgfield=True, avgchannel="2", spw="0:0~1"), joined, joined_ids),
        ("cross-both-reversed", dict(vis=str(reversed_time), avgscan=True, avgfield=True), joined, [("2", "1")] * 2),
        ("two-scan-colors", color_controls | dict(coloraxis="scan"), split, separate_ids),
        ("two-field-colors", color_controls | dict(coloraxis="field"), split, separate_ids),
    )
    for name, overrides, coordinates, identifiers in cases:
        controls = base | overrides
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [tuple(point[3:5]) for point in selected] == identifiers
        np.testing.assert_allclose(np.array([row[:2] for row in selected], dtype=float), coordinates, rtol=1.2e-5)
        if len(selected) == 2:
            midpoint = times[0] + (times[-1] - times[0]) / 2
            np.testing.assert_allclose([float(row[9]) for row in selected], midpoint, rtol=0, atol=0.00051)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching independently weighted coordinates", flush=True)
    figure = base | dict(avgscan=True, avgfield=True)
    compare_ordinate_figure(work, "cross-both", native, figure, "Real", ["Real:corrected", "Real:model"])
    for axis, groups in (("scan", ("Scan 1", "Scan 2")), ("field", ("FLUX", "PHASE"))):
        figure = base | color_controls | dict(coloraxis=axis)
        compare_ordinate_figure(work, f"two-{axis}-colors", native, figure, "Real",
                                ["Real:corrected", "Real:model"], color_groups=groups)
    flagged = work / "masked-cross-both.ms"
    copy_ms(vis, flagged)
    with opened(flagged, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
        assert not table.getcol("FLAG_ROW").any()
    compare_empty_combined_average(
        work, native, "cross-both-channel-empty",
        base | dict(vis=str(flagged), avgscan=True, avgfield=True, avgchannel="2", spw="0:0~1"),
    )
    from casars.tasks import TaskParameterError

    transformed = base | dict(vis=str(field), xaxis="frequency", xdatacolumn="",
                              transform=True, freqframe="BARY", expformat="txt")
    ordinary = work / "ordinary-frequency-transform.native.txt"
    assert native(**transformed, plotfile=str(ordinary)) is None
    assert len(rows(ordinary)) == 4
    actual = work / "cross-field-frequency-transform.native.txt"
    try:
        native(**transformed, avgfield=True, plotfile=str(actual))
    except TaskParameterError as error:
        assert "averaging of independent MeasurementSet ordinate columns is pending #1032" in str(error), error
    else:
        raise AssertionError("PlotMS admitted spectral frame conversion across averaged fields")
    assert not actual.exists()


def compare_baseline_averaged_columns(root, native):
    work = root / "baseline-averaged-columns"
    work.mkdir()
    source, integration = time_average_fixture(work)
    with opened(source, True) as table:
        high = table.getcol("ANTENNA2") == 2
        shape = table.getcol("DATA").shape
        amplitude = np.where(high, 3, 1).astype(np.complex64)
        for factor, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(name, np.broadcast_to(amplitude * 10**factor, shape).copy())
        for name, high_value in (("WEIGHT", 9), ("SIGMA", 0.5)):
            values = np.where(high, high_value, 1).astype(np.float32)
            table.putcol(name, np.broadcast_to(values, (shape[0], shape[2])).copy())
    controls = request(source) | dict(antenna="A0&A1;A0&A2", xaxis="real", xdatacolumn="data",
                                      yaxis=["real", "real"], ydatacolumn=["corrected", "model"],
                                      yaxislocation=["left", "right"], spw="0:1", avgbaseline=True)
    cases = (
        ("baseline-independent", "XX", [(2.8, 28)] * 6),
        ("baseline-axis-order", "XX", [(2.8, 28)] * 6),
        ("baseline-both-correlations", "XX,YY", [(2.8, 28)] * 12),
        ("baseline-selected-yy", "YY", [(1.2, 12)] * 6),
        ("baseline-mixed-flags", "XX", [(1, 10), (2.8, 28)] * 3),
        ("baseline-row-flags-only", "XX", [(2.8, 28)] * 6),
        ("baseline-zero-row-weights", "XX", [(2, 20)] * 6),
        ("baseline-ignores-spectral-weights", "XX", [(2.8, 28)] * 6),
    )
    for name, correlation, corrected in cases:
        vis = work / f"{name}.ms"
        copy_ms(source, vis)
        with opened(vis, True) as table:
            if name in ("baseline-both-correlations", "baseline-selected-yy"):
                values = table.getcol("WEIGHT")
                values[1, :] = np.where(high, 1, 9)
                table.putcol("WEIGHT", values)
            if name == "baseline-mixed-flags":
                flags = table.getcol("FLAG")
                flags[:, 1, high & (integration % 2 == 0)] = True
                table.putcol("FLAG", flags)
            if name == "baseline-row-flags-only":
                table.putcol("FLAG_ROW", high & (integration % 2 == 0))
            if name == "baseline-zero-row-weights":
                table.putcol("WEIGHT", np.zeros_like(table.getcol("WEIGHT")))
            if name == "baseline-ignores-spectral-weights":
                column = "WEIGHT_SPECTRUM"
                descriptor = table.getcoldesc("DATA")
                table.addcols({column: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                            dataManagerGroup=column)})
                spectrum = np.where(high, 1, 9).astype(np.float32)
                table.putcol(column, np.broadcast_to(spectrum, shape).copy())
        options = controls | dict(vis=str(vis), correlation=correlation)
        if name == "baseline-axis-order":
            options["yaxis"] = ["real", "amp"]
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, expformat="txt", plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 2 * len(corrected)
        assert {tuple(point[5:9]) for point in selected} == {("-1", "-1", "*", "*")}
        correlations = correlation.split(",")
        assert [point[12] for point in selected] == correlations * (len(selected) // len(correlations))
        model = [(x, 10 * y) for x, y in corrected]
        np.testing.assert_allclose(np.array([row[:2] for row in selected], dtype=float),
                                   corrected + model, rtol=1.2e-5)
        assert native(**options, expformat="txt", plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching baseline-averaged coordinates", flush=True)
        if name in ("baseline-both-correlations", "baseline-row-flags-only", "baseline-ignores-spectral-weights"):
            one_ordinate = options | dict(yaxis="real", ydatacolumn="corrected", yaxislocation="left")
            expected, actual = (work / f"{name}-one-ordinate.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**one_ordinate, expformat="txt", plotfile=str(expected)) is None
            assert native(**one_ordinate, expformat="txt", plotfile=str(actual)) is None
            assert compare_text(actual, expected) == len(corrected)
    figure = controls | dict(vis=str(work / "baseline-independent.ms"))
    compare_ordinate_figure(work, "baseline-independent", native, figure, "Real", ["Real:corrected", "Real:model"])
    compare_baselines_in_observation_order(work, source, controls, native)


def compare_baselines_in_observation_order(work, source, controls, native):
    vis = work / "overlapping-observations.ms"
    copy_ms(source, vis)
    with opened(vis / "OBSERVATION", True) as table:
        other = table.nrows()
        table.addrows(1)
        for name in table.colnames():
            if table.iscelldefined(name, 0):
                table.putcell(name, other, table.getcell(name, 0))
    with opened(vis, True) as table:
        times, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        second_observation = integration >= 6
        reversed_times = times[5 - integration % 6]
        table.putcol("TIME", reversed_times)
        table.putcol("TIME_CENTROID", reversed_times)
        table.putcol("OBSERVATION_ID", np.where(second_observation, other, 0).astype(np.int32))
        for name, value in (("FIELD_ID", 0), ("SCAN_NUMBER", 1), ("WEIGHT", 1)):
            table.putcol(name, np.full_like(table.getcol(name), value))
        high = table.getcol("ANTENNA2") == 2
        for name, values in (("DATA", np.where(second_observation, 9, 1)),
                             ("CORRECTED_DATA", np.where(second_observation, 9, np.where(high, -1, 1))),
                             ("MODEL_DATA", np.where(second_observation, 15, 5))):
            shape = table.getcol(name).shape
            table.putcol(name, np.broadcast_to(values.astype(np.complex64), shape).copy())
    options = controls | dict(vis=str(vis), yaxis=["amp", "real"], expformat="txt")
    expected, actual = (work / f"observation-order.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    selected = rows(expected)
    assert [point[13] for point in selected] == (["0"] * 6 + [str(other)] * 6) * 2
    assert [point[9] for point in selected] == [f"{time:.3f}" for time in times[:6]] * 4
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               [(1, 0)] * 6 + [(9, 9)] * 6 + [(1, 5)] * 6 + [(9, 15)] * 6)
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    print(f"observation-order: {len(selected)} matching baseline-averaged coordinates", flush=True)


def compare_float_columns(root, vis, source, native):
    float_only = root / "float-only.ms"
    mixed = root / "float-and-complex.ms"
    floating = source["DATA"].real.astype(np.float32) - 6
    for destination in (float_only, mixed):
        copy_ms(vis, destination)
        with opened(destination, True) as table:
            desc = table.getcoldesc("DATA")
            desc.update(name="FLOAT_DATA", valueType="float",
                        dataManagerType="StandardStMan", dataManagerGroup="FloatVis")
            table.addcols({"FLOAT_DATA": desc})
            table.putcol("FLOAT_DATA", floating)
            if destination == float_only:
                table.removecols(["DATA", "CORRECTED_DATA", "MODEL_DATA"])

    cases = (
        ("float-explicit", float_only, {}, (1, -4)),
        ("float-default", float_only, {"ydatacolumn": ""}, (1, -4)),
        ("float-corrected-fallback", float_only, {"ydatacolumn": "corrected"}, (1, -4)),
        ("float-x", float_only, dict(xaxis="amp", yaxis="channel",
                                     xdatacolumn="float", ydatacolumn=""), (-4, 1)),
        ("float-mixed-explicit", mixed, {}, (1, -4)),
        ("float-mixed-observed", mixed, {"ydatacolumn": "data"}, (1, -4)),
        ("float-mixed-with-corrected", mixed,
         dict(xaxis="amp", xdatacolumn="float", ydatacolumn="corrected"), (-4, 4.47214)),
        ("float-mixed-default-with-corrected", mixed,
         dict(xaxis="amp", xdatacolumn="", ydatacolumn="corrected"), (-4, 4.47214)),
        ("float-mixed-data-phase", mixed, dict(yaxis="phase", ydatacolumn="data"), (1, 180)),
        ("float-mixed-vector-residual", mixed,
         dict(xaxis="amp", xdatacolumn="data-model_vector"), (12.8062, -4)),
    )
    for name, destination, overrides, first in cases:
        controls = request(destination) | dict(xaxis="channel", yaxis="amp",
                                                xdatacolumn="", ydatacolumn="float") | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        np.testing.assert_allclose(np.array(rows(expected)[0][:2], dtype=float), first, rtol=1.2e-5, atol=1e-7)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 12
        print(f"{name}: 12 matching points", flush=True)

    for name, column in (("float-explicit", "float"), ("float-default", "")):
        controls = request(float_only) | dict(xaxis="channel", yaxis="amp",
                                               xdatacolumn="", ydatacolumn=column)
        compare_figure(root, native, controls, root / f"{name}.casa.txt",
                       ("Amp:float vs. Channel", "Channel", "Amp:float"))

    for destination in (float_only, mixed):
        with opened(destination) as table:
            np.testing.assert_array_equal(table.getcol("FLOAT_DATA"), floating)
            if destination == float_only:
                assert {"DATA", "CORRECTED_DATA", "MODEL_DATA"}.isdisjoint(table.colnames())
            for name, expected in source.items():
                if name in table.colnames():
                    np.testing.assert_array_equal(table.getcol(name), expected, err_msg=name)

    float_model = root / "float-and-model.ms"
    copy_ms(mixed, float_model)
    with opened(float_model, True) as table:
        table.removecols(["DATA", "CORRECTED_DATA"])
    invalid = (
        ("missing-float", vis, "amp", "float", "FLOAT_DATA is unavailable"),
        ("phase-float", float_only, "phase", "float", "requires an amplitude or real axis"),
        ("vector-float-residual", float_model, "amp", "data-model_vector", "Data residuals not valid"),
        ("scalar-float-residual", float_model, "amp", "data-model_scalar", "Data residuals not valid"),
    )
    for name, destination, axis, column, diagnostic in invalid:
        controls = request(destination) | dict(xaxis="channel", yaxis=axis, xdatacolumn="", ydatacolumn=column)
        for engine, task, message in (("casa", reference, "plot parameters failed"), ("native", native, diagnostic)):
            output = root / f"{name}.{engine}.txt"
            try:
                task(**controls, plotfile=str(output), expformat="txt")
            except Exception as error:
                assert message in str(error), error
            else:
                raise AssertionError(f"{engine} accepted {name}")
            assert not output.exists()

    from casars.tasks import TaskParameterError

    controls = request(mixed) | dict(xaxis="channel", yaxis="amp", xdatacolumn="",
                                     ydatacolumn="data", avgchannel="2", expformat="txt")
    expected, actual = (root / f"average-mixed-float.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 6 and all(float(point[1]) < 0 for point in selected)
    try:
        native(**controls, plotfile=str(actual))
    except TaskParameterError as error:
        assert "averaging MeasurementSet FLOAT_DATA is pending #1032" in str(error), error
    else:
        raise AssertionError("Observed FLOAT_DATA averaging silently exported complex DATA")
    assert not actual.exists()

    controls = request(float_only) | dict(xaxis="channel", yaxis="real", xdatacolumn="", ydatacolumn="float")
    expected, actual = (root / f"real-float.{engine}.txt" for engine in ("casa", "native"))
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    # For real-valued cells, CASA's signed amplitude already gives the real coordinates.
    amp_reference = rows(root / "float-explicit.casa.txt")
    actual_real = rows(actual)
    np.testing.assert_allclose(np.array([p[:2] for p in actual_real], dtype=float),
                               np.array([p[:2] for p in amp_reference], dtype=float),
                               rtol=1.2e-5, atol=1e-7)
    assert [p[2:] for p in actual_real] == [p[2:] for p in amp_reference]
    assert any(line.startswith("# Channel Real ") for line in actual.read_text().splitlines())
    try:
        reference(**controls, plotfile=str(expected), expformat="txt")
    except Exception as error:
        assert "plot parameters failed" in str(error) or "Stream removed (Socket closed)" in str(error), error
        assert not expected.exists()
    else:
        assert compare_text(actual, expected) == 12


def compare_default_model(root, native):
    vis = root / "default-model.ms"
    make_cal_ms(str(vis), stokes="XX XY YX YY")
    with opened(vis, True) as table:
        corr, channel, row = np.indices(table.getcol("DATA").shape)
        observed = (1 + channel + row / 256 + 2 * corr).astype(np.complex64)
        flags = (channel == 2)
        table.putcol("DATA", observed)
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", np.zeros(observed.shape[2], dtype=bool))
        absent = {"MODEL_DATA", "CORRECTED_DATA"}
        table.removecols(sorted(absent.intersection(table.colnames())))

    cases = (
        ("unit-model-parallel", dict(xdatacolumn="model", yaxis="amp", ydatacolumn="data"), (1, 2)),
        ("unit-model-cross", dict(xdatacolumn="model", yaxis="amp", ydatacolumn="data",
                                  correlation="XY"), (0, 4)),
        ("unit-model-corrected-residual", dict(xaxis="channel", xdatacolumn="", yaxis="real",
                                                ydatacolumn="corrected-model_vector"), (1, 1)),
        ("unit-model-cross-residual", dict(xaxis="channel", xdatacolumn="", yaxis="real",
                                            ydatacolumn="data-model_vector", correlation="XY"), (1, 4)),
    )
    for name, overrides, first in cases:
        controls = request(vis) | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == 12
        assert {sample[2] for sample in selected} == {"1", "3"}
        np.testing.assert_allclose(np.array(selected[0][:2], dtype=float), first, rtol=1.2e-5, atol=1e-7)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 12
        print(f"{name}: 12 matching points without MODEL_DATA", flush=True)

    controls = request(vis) | cases[0][1]
    compare_figure(root, native, controls, root / "unit-model-parallel.casa.txt",
                   ("Amp vs. Amp:model", "Amp:model", "Amp"))
    with opened(vis) as table:
        assert absent.isdisjoint(table.colnames())
        np.testing.assert_array_equal(table.getcol("DATA"), observed)
        np.testing.assert_array_equal(table.getcol("FLAG"), flags)

    corrected_only = root / "corrected-only-model.ms"
    copy_ms(vis, corrected_only)
    casa.clearcal(str(corrected_only), addmodel=False)
    with opened(corrected_only, True) as table:
        assert "CORRECTED_DATA" in table.colnames()
        table.removecols([column for column in ("DATA", "MODEL_DATA") if column in table.colnames()])
    controls = request(corrected_only) | dict(xaxis="amp", xdatacolumn="model", yaxis="amp",
                                              ydatacolumn="corrected", expformat="txt")
    expected, actual = (root / f"unit-with-corrected-only.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    assert {float(point[0]) for point in rows(expected)} == {1.0}

    virtual = root / "virtual-model.ms"
    copy_ms(vis, virtual)
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION"):
        with opened(virtual / name, True) as table:
            table.addrows(1)
            for column in table.colnames():
                if table.iscelldefined(column, 0):
                    table.putcell(column, 1, table.getcell(column, 0))
            if name == "DATA_DESCRIPTION":
                table.putcell("SPECTRAL_WINDOW_ID", 1, 1)
            else:
                table.putcell("NAME", 1, "SPW1")
                table.putcell("CHAN_FREQ", 1, table.getcell("CHAN_FREQ", 0) + 2e7)
                table.putcell("REF_FREQUENCY", 1, table.getcell("REF_FREQUENCY", 0) + 2e7)
    with opened(virtual, True) as table:
        phase = table.getcol("FIELD_ID") == 1
        times = table.getcol("TIME")
        integrations = np.unique(times[phase])
        descriptions = table.getcol("DATA_DESC_ID")
        descriptions[phase & (times >= integrations[len(integrations) // 2])] = 1
        table.putcol("DATA_DESC_ID", descriptions)
    casa.setjy(str(virtual), field="PHASE", spw="0", standard="manual",
               fluxdensity=[7.25, 0, 0, 0], usescratch=False)
    with opened(virtual, True) as table:
        assert "MODEL_DATA" not in table.colnames()
        table.putkeyword("definedmodel_field_0", "model_missing")
    controls = request(virtual) | dict(xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="model",
                                       expformat="txt")
    expected, actual = (root / f"unit-with-other-virtual-field.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    assert {float(point[1]) for point in rows(expected)} == {1.0}

    unmatched = controls | dict(field="PHASE", spw="1:1~3")
    expected, actual = (root / f"unit-unmatched-virtual-window.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**unmatched, plotfile=str(expected)) is None
    assert native(**unmatched, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 6
    assert {float(point[1]) for point in rows(expected)} == {1.0}

    controls |= dict(field="PHASE")
    expected, actual = (root / f"selected-virtual-model.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    assert {float(point[1]) for point in rows(expected)} == {7.25}
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 6
    figure_controls = {key: value for key, value in controls.items() if key != "expformat"}
    compare_figure(root, native, figure_controls, expected,
                   ("Amp:model vs. Channel", "Channel", "Amp:model"))
    residual_controls = controls | dict(yaxis="real", ydatacolumn="data-model_vector")
    expected, actual = (root / f"selected-virtual-residual.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**residual_controls, plotfile=str(expected)) is None
    assert all(float(point[1]) < 0 for point in rows(expected))
    assert native(**residual_controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 6

    average = controls | dict(avgchannel="3")
    expected, actual = (root / f"selected-virtual-averaged.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**average, plotfile=str(expected)) is None
    assert len(rows(expected)) == 3
    assert {float(point[1]) for point in rows(expected)} == {7.25}
    assert native(**average, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 3
    with opened(virtual) as table:
        assert "MODEL_DATA" not in table.colnames()
        np.testing.assert_array_equal(table.getcol("DATA"), observed)
        np.testing.assert_array_equal(table.getcol("FLAG"), flags)

    schema_root = root / "stored-model-schema"
    schema_root.mkdir()
    schema_fixture, _ = fixture(schema_root)
    with opened(schema_fixture) as table:
        descriptor = table.getcoldesc("MODEL_DATA")
    stored = root / "stored-and-virtual-model.ms"
    copy_ms(virtual, stored)
    with opened(stored, True) as table:
        table.addcols({"MODEL_DATA": descriptor})
        physical_model = np.full(observed.shape, 19, dtype=np.complex64)
        table.putcol("MODEL_DATA", physical_model)
        table.removekeyword("definedmodel_field_0")
    for name, field, spw, amplitude, count in (
        ("virtual-and-stored", "PHASE", "0:1~3", 7.25, 6),
        ("unmodeled-window-and-stored", "PHASE", "1:1~3", 1, 6),
        ("stored-only-field", "FLUX", "0:1~3", 19, 12),
    ):
        selected = controls | dict(vis=str(stored), field=field, spw=spw)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**selected, plotfile=str(expected)) is None
        assert {float(point[1]) for point in rows(expected)} == {amplitude}
        assert native(**selected, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
    average = controls | dict(vis=str(stored), xaxis="channel", xdatacolumn="",
                              yaxis=["amp", "amp"], ydatacolumn=["data", "model"], avgtime="20")
    for name, field, spw, amplitude in (
        ("average-virtual-and-stored", "PHASE", "0:1", 7.25),
        ("average-unmodeled-window-and-stored", "PHASE", "1:1", 1),
        ("average-stored-only-field", "FLUX", "0:1", 19),
    ):
        selected = average | dict(field=field, spw=spw)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**selected, plotfile=str(expected)) is None
        coordinates = rows(expected)
        assert coordinates and len(coordinates) % 2 == 0
        assert {float(point[1]) for point in coordinates[len(coordinates) // 2:]} == {amplitude}
        assert native(**selected, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(coordinates)
    with opened(stored) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), physical_model)


def virtual_fourier_fixture(work):
    vis, columns = fixture(work)
    with opened(vis / "FIELD") as field:
        direction = field.getcell("PHASE_DIR", 0)[:, 0]
    with opened(vis / "SPECTRAL_WINDOW") as window:
        frequency = window.getcell("CHAN_FREQ", 0)[0]
    model = work / "point.im"
    write_model(model, direction, frequency, 2.0, cell=5)
    assert casa.ft(vis=str(vis), model=str(model), field="FLUX", usescratch=False) is None
    with opened(vis) as table:
        columns["MODEL_DATA"] = table.getcol("MODEL_DATA")
    return vis, columns


def varying_fourier_fixture(vis, time_source):
    copy_ms(vis, time_source)
    with opened(time_source, True) as table:
        _, slots = np.unique(table.getcol("TIME"), return_inverse=True)
        uvw = table.getcol("UVW")
        uvw[:, slots % 2 == 1] *= 3
        table.putcol("UVW", uvw)
        weights = table.getcol("WEIGHT")
        weights[:] = np.where(slots % 2 == 0, 1, 9)
        table.putcol("WEIGHT", weights)


def compare_virtual_fourier_model(root, native):
    work = root / "fourier-model"
    work.mkdir()
    vis, columns = virtual_fourier_fixture(work)
    controls = request(vis) | dict(xaxis="real", yaxis="imag", xdatacolumn="model", ydatacolumn="model")
    expected, actual = (work / f"fourier.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    first = np.array(rows(expected)[0][:2], dtype=float)
    # The pinned Fourier gridder gives a slightly different norm from the
    # image pixel flux for this off-center point. Compare its complex output.
    np.testing.assert_allclose(first, [-1.76778, 0.938018], rtol=1.2e-5)
    assert np.all(np.abs(first) > 0.5)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 12

    # Give successive integrations distinct Fourier phases in the time test.
    time_source = work / "time-varying-fourier.ms"
    varying_fourier_fixture(vis, time_source)
    samples = controls | dict(vis=str(time_source), spw="0:1")
    raw = work / "fourier-time-input.casa.txt"
    assert reference(**samples, plotfile=str(raw), expformat="txt") is None
    raw_points = rows(raw)
    integrations = np.array([point[:2] for point in raw_points], dtype=float).reshape(3, 2, 2)
    assert np.max(np.abs(integrations[:, 0] - integrations[:, 1])) > 1e-4
    means = integrations.mean(axis=1)
    times = np.array([float(point[9]) for point in raw_points]).reshape(3, 2).mean(axis=1)
    for name, axes, coordinates in (
        ("fourier-time", {}, means),
        ("fourier-time-model-x", dict(yaxis="channel", ydatacolumn=""),
         np.column_stack((means[:, 0], np.ones(3)))),
        ("fourier-time-model-y", dict(xaxis="time", xdatacolumn="", yaxis="real"),
         np.column_stack((times, means[:, 0]))),
    ):
        average = samples | axes | dict(avgtime="20")
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**average, plotfile=str(expected), expformat="txt") is None
        selected = np.array([point[:2] for point in rows(expected)], dtype=float)
        np.testing.assert_allclose(selected, coordinates, rtol=1.2e-5)
        if average["xaxis"] == "time":
            np.testing.assert_allclose(selected[:, 0], times, rtol=0, atol=0.00051)
        assert native(**average, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 3
        print(f"{name}: 3 matching averages from distinct virtual model samples", flush=True)

    for source in (vis, time_source):
        with opened(source) as table:
            for name, values in columns.items():
                np.testing.assert_array_equal(table.getcol(name), values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("time-statistics", "channel-flag-symbols", "averaged-flag-symbols"))
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
        if args.only == "time-statistics":
            compare_time_averaged_statistics(root, native)
            return
        if args.only == "averaged-flag-symbols":
            compare_time_averaged_flag_symbols(root, native)
            compare_reduced_flag_symbols(root, native)
            return
        vis, source = fixture(root)
        if args.only == "channel-flag-symbols":
            compare_channel_averaged_flag_symbols(root, vis, native)
            return
        compare(root, vis, native)
        with opened(vis) as table:
            for name, values in source.items():
                np.testing.assert_array_equal(table.getcol(name), values, err_msg=name)
        compare_missing_corrected(root, vis, source, native)
        compare_scalar_phase_boundary(root, vis, source, native)
        compare_zero_model(root, vis, source, native)
        compare_ratio_float_range(root, vis, source, native)
        compare_ordinate_columns(root, vis, native)
        compare_multiple_ordinates(root, vis, native)
        compare_averaged_columns(root, native)
        compare_time_averaged_columns(root, native)
        compare_time_averaged_flag_symbols(root, native)
        compare_reduced_flag_symbols(root, native)
        compare_time_averaged_statistics(root, native)
        compare_combined_averaged_columns(root, native)
        compare_time_scan_field_columns(root, native)
        compare_baseline_averaged_columns(root, native)
        compare_float_columns(root, vis, source, native)
        compare_default_model(root, native)
        compare_virtual_fourier_model(root, native)


if __name__ == "__main__":
    main()
