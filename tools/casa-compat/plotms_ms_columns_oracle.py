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


def compare_figure(root, native, controls, expected, labels):
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
    assert reference(**figure, plotfile=str(outputs[0])) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**figure, plotfile=str(outputs[1])) is None
    [(title, xlabel, ylabel, points, xlim, ylim)] = capture
    assert (title, xlabel, ylabel) == labels, (title, xlabel, ylabel)
    coordinates = np.array([point[:2] for point in rows(expected)], dtype=float)
    np.testing.assert_allclose(points, coordinates, rtol=1.2e-5, atol=1e-7)
    if controls["xaxis"] == "time":
        np.testing.assert_allclose(points[:, 0], coordinates[:, 0], rtol=0, atol=0.00051)
    visible = points[np.isfinite(points).all(axis=1)]
    for values, limits in zip(visible.T, (xlim, ylim), strict=True):
        assert min(limits) <= min(values) <= max(values) <= max(limits), (values, limits)
    for output in outputs:
        with Image.open(output) as image:
            assert image.size == (500, 360), (output, image.size)
    print(f"{outputs[0].name}: {len(points)} matching rendered coordinates, labels and dimensions", flush=True)


def pending_cases(root, native, vis):
    from casars.tasks import TaskParameterError

    cases = (
        ("averaged-vector", dict(xdatacolumn="corrected-model_vector", avgchannel="3"),
         6, "averaging derived data columns"),
        ("visible-flags", dict(customflaggedsymbol=True), 18, "displaying flagged points"),
    )
    for name, options, point_count, diagnostic in cases:
        controls = request(vis) | options | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == point_count
        if name == "visible-flags":
            assert {sample[2] for sample in selected} == {"1", "2", "3"}
        try:
            native(**controls, plotfile=str(actual))
        except TaskParameterError as error:
            assert diagnostic in str(error) and "#1032" in str(error)
        else:
            raise AssertionError(f"An unsupported {name} plot was exported")
        assert not actual.exists()


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
    pending_cases(root, native, vis)


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


def compare_ordinate_figure(root, name, native, controls, xlabel, labels, *, shared_limits=False):
    from casars import _plot_document

    capture = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, primary = render(*args, **kwargs)
        capture.append((primary.get_xlabel(), [(axis.get_ylabel(), axis.lines[0].get_xydata(), axis.get_ylim())
                                               for axis in figure.axes]))
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
                         [layer.get_xydata() for layer in axis.lines]) for axis in figure.axes])
        return figure, primary

    dense = base | dict(correlation="")
    expected, actual = (root / f"three-ordinates-two-correlations.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**dense, plotfile=str(expected), expformat="txt") is None
    assert native(**dense, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 90
    assert [point[0] for point in rows(expected)[:5]] == ["1", "1", "2", "3", "3"]

    split = [("left", "Real:model, Amp:corrected", (0, 2)), ("right", "Phase (degrees)", (1,))]
    figures = (
        ("three-ordinates", base, "three-ordinates", split),
        ("three-ordinates-scalar-side", base | dict(yaxislocation="right"), "three-ordinates",
         [("left", "Real:model, Phase (degrees), Amp:corrected", (0, 1, 2))]),
        ("three-ordinates-short-sides", base | dict(yaxislocation=["right"]), "three-ordinates",
         [("left", "Phase (degrees), Amp:corrected", (1, 2)), ("right", "Real:model", (0,))]),
        ("three-ordinates-two-correlations", dense | dict(xconnector="line"),
         "three-ordinates-two-correlations", split),
    )
    for name, controls, text, expectations in figures:
        figure = controls | dict(expformat="png", width=500, height=360, dpi=100)
        expected, actual = (root / f"{name}.{engine}.png" for engine in ("casa", "native"))
        assert reference(**figure, plotfile=str(expected)) is None
        with patch.object(_plot_document, "render_document", record):
            assert native(**figure, plotfile=str(actual)) is None
        [axes] = capture
        capture.clear()
        coordinates = np.array([point[:2] for point in rows(root / f"{text}.casa.txt")], dtype=float)
        ordinates = np.split(coordinates, 3)
        assert len(axes) == len(expectations)
        for (side, label, layers), (expected_side, expected_label, indices) in zip(axes, expectations, strict=True):
            assert (side, label, len(layers)) == (expected_side, expected_label, len(indices))
            for layer, index in zip(layers, indices, strict=True):
                np.testing.assert_allclose(layer, ordinates[index], rtol=1.2e-5, atol=1e-7)
        for output in (expected, actual):
            with Image.open(output) as image:
                assert image.size == (500, 360)
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

    from casars.tasks import TaskParameterError

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

    for axis in ("u", "elevation"):
        mixed = dense | dict(yaxis=["real", "phase", axis], expformat="txt")
        expected, actual = (root / f"three-ordinate-mixed-{axis}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**mixed, plotfile=str(expected)) is None
        assert len(rows(expected)) == 78
        try:
            native(**mixed, plotfile=str(actual))
        except TaskParameterError as error:
            assert "mixed MeasurementSet visibility and metadata ordinates or geometry abscissae are pending #1032" in str(error), error
        else:
            raise AssertionError(f"PlotMS used the visibility-only sample mask for the mixed {axis} plot")
        assert not actual.exists()

    averaged = base | dict(avgchannel="2", expformat="txt")
    expected, actual = (root / f"three-ordinate-averaged.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**averaged, plotfile=str(expected)) is None
    assert len(rows(expected)) == 18
    try:
        native(**averaged, plotfile=str(actual))
    except TaskParameterError as error:
        assert "more than two MeasurementSet ordinates with averaging" in str(error), error
    else:
        raise AssertionError("PlotMS used the unaveraged ordinate path for an averaged request")
    assert not actual.exists()


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

    # Weight axes also select an input cache column. Their own averaged
    # coordinate coverage is tracked in #1032.
    weight_axis = base | dict(xdatacolumn="model", yaxis="weight", ydatacolumn="",
                              yaxislocation="left", expformat="txt")
    expected, actual = (work / f"avg-model-vs-weight.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**weight_axis, plotfile=str(expected)) is None
    assert native(**weight_axis, plotfile=str(actual)) is None
    casa_rows, native_rows = rows(expected), rows(actual)
    assert len(casa_rows) == len(native_rows) == 12
    assert float(casa_rows[0][0]) == 280
    np.testing.assert_allclose([float(point[0]) for point in native_rows],
                               [float(point[0]) for point in casa_rows], rtol=1.2e-5)
    assert [point[2:] for point in native_rows] == [point[2:] for point in casa_rows]

    from casars.tasks import TaskParameterError

    average_time = base | dict(avgtime="20", expformat="txt")
    expected, actual = (work / f"avg-with-time.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**average_time, plotfile=str(expected)) is None
    assert len(rows(expected)) == 12
    try:
        native(**average_time, plotfile=str(actual))
    except TaskParameterError as error:
        assert "averaging of independent MeasurementSet ordinate columns is pending #1032" in str(error), error
    else:
        raise AssertionError("PlotMS used a common column for independent time averages")
    assert not actual.exists()
    with opened(vis) as table:
        for name, data in (columns | dict(FLAG=flags)).items():
            np.testing.assert_array_equal(table.getcol(name), data)


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

    from casars.tasks import TaskExecutionError

    average = controls | dict(avgchannel="3")
    expected, actual = (root / f"selected-virtual-averaged.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**average, plotfile=str(expected)) is None
    assert len(rows(expected)) == 3
    assert {float(point[1]) for point in rows(expected)} == {7.25}
    try:
        native(**average, plotfile=str(actual))
    except TaskExecutionError as error:
        assert "virtual MeasurementSet models are pending #1032" in str(error), error
    else:
        raise AssertionError("PlotMS applied the unaveraged model path to an averaged request")
    assert not actual.exists()
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
    with opened(stored) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), physical_model)


def compare_virtual_fourier_model(root, native):
    work = root / "fourier-model"
    work.mkdir()
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
    with opened(vis) as table:
        for name, values in columns.items():
            np.testing.assert_array_equal(table.getcol(name), values)


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
        vis, source = fixture(root)
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
        compare_float_columns(root, vis, source, native)
        compare_default_model(root, native)
        compare_virtual_fourier_model(root, native)


if __name__ == "__main__":
    main()
