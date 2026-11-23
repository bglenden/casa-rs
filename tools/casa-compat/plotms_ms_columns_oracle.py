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
        print(f"{name}: {count} points, CASA {casa_seconds:.3f}s, casa-rs {native_seconds:.3f}s",
              flush=True)
    compare_figure(root, native, base, root / "data-and-corrected.casa.txt",
                   ("Phase:corrected vs. Amp", "Amp", "Phase:corrected (degrees)"))
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
    }
    for name, options in cases.items():
        controls = request(fallback) | options | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        assert native(**controls, plotfile=str(actual)) is None
        count = compare_text(actual, expected)
        assert count == 12
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
        vis, source = fixture(root)
        compare(root, vis, native)
        with opened(vis) as table:
            for name, values in source.items():
                np.testing.assert_array_equal(table.getcol(name), values, err_msg=name)
        compare_missing_corrected(root, vis, source, native)
        compare_scalar_phase_boundary(root, vis, source, native)
        compare_zero_model(root, vis, source, native)


if __name__ == "__main__":
    main()
