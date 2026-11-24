#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare vector residuals and ratios after MeasurementSet channel averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import (
    casa, compare_channel_vector_residual, compare_figure, compare_text, fixture, opened,
    plotms_launcher, reference, request, rows,
)


def weighted_fixture(root):
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, spectrum in (("WEIGHT_SPECTRUM", [1, 9]), ("SIGMA_SPECTRUM", [1, 0.5])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            values = np.resize(np.array(spectrum, dtype=np.float32), shape[1])[None, :, None]
            table.putcol(name, np.broadcast_to(values, shape))
        for name, spectrum in (
            ("DATA", [1 + 1j, 3 + 6j, 7 + 5j, 11 + 1j]),
            ("CORRECTED_DATA", [2 + 3j, 8 - 1j, 3 + 3j, 12 + 4j]),
            ("MODEL_DATA", [1 - 1j, 2 + 2j, 4 + 4j, 5 - 3j]),
        ):
            values = np.resize(np.array(spectrum, dtype=np.complex64), shape[1])[None, :, None]
            table.putcol(name, np.broadcast_to(values, shape))
    return vis


def compare_weighted(root, native, vis):
    base = request(vis) | dict(xaxis="real", yaxis="imag", ydatacolumn="data",
                               avgchannel="2", spw="0:0~3", title="Weighted visibility")
    # Corrected and model use the calibrated spectrum (1, 9), while the
    # observed ordinate uses inverse sigma squared (1, 4). Channel 2 is
    # flagged, leaving channel 3 alone in the second bin.
    corrected = (7.4 - 0.6j, 12 + 4j)
    model = (1.9 + 1.7j, 5 - 3j)
    for name, column, expected_x in (
        ("residual", "corrected-model_vector", [a - b for a, b in zip(corrected, model)]),
        ("ratio", "corrected/model_vector", [a / b for a, b in zip(corrected, model)]),
    ):
        controls = base | dict(xdatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == 12
        assert [(point[2], point[12]) for point in selected] == [("0", "XX"), ("1", "XX")] * 6
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   [(expected_x[0].real, 5), (expected_x[1].real, 1)] * 6, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching weighted selected coordinates", flush=True)
        if name == "ratio":
            compare_figure(root, native, controls, expected,
                           ("Weighted visibility", "Real:corrected/model (vector)", "Imag"))


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
        compare_weighted(root, native, weighted_fixture(root))
        ordinary = root / "ordinary"
        ordinary.mkdir()
        vis, _ = fixture(ordinary)
        compare_channel_vector_residual(ordinary, native, vis)


if __name__ == "__main__":
    main()
