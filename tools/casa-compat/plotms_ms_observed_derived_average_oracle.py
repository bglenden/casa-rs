#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare observed residuals and ratios after selected channel averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import casa, compare_figure, copy_ms, opened, plotms_launcher, request
from plotms_ms_derived_average_oracle import weighted_fixture
from plotms_ms_scalar_derived_average_oracle import compare_case


def compare_weighted(root, native, vis):
    observed = np.array([2.6 + 5j, 11 + 1j])
    model = np.array([1.9 + 1.7j, 5 - 3j])
    corrected_imag = [-0.6, 4]
    base = request(vis) | dict(avgchannel="2", spw="0:0~3", title="Observed channel averages")
    for name, axis, column, coordinates in (
        ("observed-residual", "real", "data-model_vector", (observed - model).real),
        ("observed-ratio", "real", "data/model_vector", (observed / model).real),
        ("observed-scalar-amp", "amp", "data-model_scalar", np.abs(observed) - np.abs(model)),
        ("observed-scalar-phase", "phase", "data/model_scalar", np.degrees(np.angle(observed) / np.abs(model))),
    ):
        controls = base | dict(xaxis=axis, xdatacolumn=column, yaxis="imag", ydatacolumn="corrected")
        expected = compare_case(root, native, name, controls, np.column_stack([coordinates, corrected_imag]))
        if name == "observed-scalar-phase":
            compare_figure(root, native, controls, expected,
                           (base["title"], "Phase:data/model (scalar) (degrees)", "Imag:corrected"))
    controls = base | dict(xaxis="imag", xdatacolumn="data", yaxis="real", ydatacolumn="data/model_vector")
    compare_case(root, native, "observed-ratio-ordinate", controls,
                 np.column_stack([observed.imag, (observed / model).real]))

    without_corrected = root / "without-corrected.ms"
    copy_ms(vis, without_corrected)
    with opened(without_corrected, True) as table:
        table.removecols(["CORRECTED_DATA"])
    observed_model = np.array([1.8 + 1.4j, model[1]])
    controls = base | dict(vis=str(without_corrected), xaxis="real", xdatacolumn="data/model_vector",
                           yaxis="imag", ydatacolumn="data")
    coordinates = np.column_stack([(observed / observed_model).real, observed.imag])
    compare_case(root, native, "observed-ratio-without-corrected", controls, coordinates)
    compare_case(root, native, "corrected-ratio-fallback",
                 controls | dict(xdatacolumn="corrected/model_vector"), coordinates)


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


if __name__ == "__main__":
    main()
