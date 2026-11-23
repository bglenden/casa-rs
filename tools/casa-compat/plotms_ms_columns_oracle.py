#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare independent stored visibility columns through both public PlotMS tasks."""

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

from calibration_smoothcal_oracle import opened
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


def compare_figures(root, native, controls, expected):
    from casars import _plot_document

    capture = []
    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        capture.append((axis.get_xlabel(), axis.get_ylabel(), axis.lines[0].get_xydata()))
        return figure, axis

    outputs = [root / engine / "columns.png" for engine in ("casa", "native")]
    for output in outputs:
        output.parent.mkdir()
    figure = dict(controls, expformat="png", width=500, height=360, dpi=100)
    assert reference(**figure, plotfile=str(outputs[0])) is None
    with patch.object(_plot_document, "render_document", record):
        assert native(**figure, plotfile=str(outputs[1])) is None
    [(xlabel, ylabel, points)] = capture
    # CASA's amplitude axis displays the visibility magnitude without a
    # unit suffix. The phase coordinates and label both use degrees.
    assert xlabel == "Amplitude" and ylabel == "Phase (deg)", (xlabel, ylabel)
    coordinates = np.array([point[:2] for point in expected], dtype=float)
    np.testing.assert_allclose(points, coordinates, rtol=1.2e-5, atol=1e-7)
    for output in outputs:
        with Image.open(output) as image:
            assert image.size == (500, 360), (output, image.size)
    print(f"PNG: {len(points)} rendered coordinates, matching names and dimensions", flush=True)


def compare(root, vis, native):
    base = request(vis)
    cases = {
        "data-and-corrected": {},
        "model-and-default": {"xdatacolumn": "model", "ydatacolumn": ""},
        "default-and-corrected": {"xdatacolumn": ""},
        "same-axis-distinct-columns": {"xaxis": "real", "yaxis": "real", "xdatacolumn": "model"},
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
        start = perf_counter()
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        native_seconds = perf_counter() - start
        count = compare_text(actual, expected)
        print(f"{name}: {count} points, CASA {casa_seconds:.3f}s, casa-rs {native_seconds:.3f}s",
              flush=True)
    compare_figures(root, native, base, rows(root / "data-and-corrected.casa.txt"))


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


if __name__ == "__main__":
    main()
