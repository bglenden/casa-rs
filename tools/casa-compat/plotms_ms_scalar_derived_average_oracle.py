#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare scalar corrected residuals and ratios after channel averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import (
    casa, compare_figure, compare_text, copy_ms, opened, plotms_launcher, reference, request, rows,
)
from plotms_ms_derived_average_oracle import weighted_fixture


def compare_case(root, native, name, controls, coordinates):
    expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[2], point[12]) for point in selected] == [("0", "XX"), ("1", "XX")] * 6
    values = np.array([point[:2] for point in selected], dtype=float)
    np.testing.assert_allclose(values, np.tile(coordinates, (6, 1)), rtol=1.2e-5, atol=1e-7)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 12
    print(f"{name}: 12 matching selected coordinates", flush=True)
    return expected


def compare_weighted(root, native, vis):
    base = request(vis) | dict(avgchannel="2", spw="0:0~3", title="Channel-averaged scalar visibility")
    corrected = np.array([7.4 - 0.6j, 12 + 4j])
    model = np.array([1.9 + 1.7j, 5 - 3j])
    observed = [5, 1]
    amplitude = np.abs(corrected) - np.abs(model)
    ratio_phase = np.degrees(np.angle(corrected) / np.abs(model))
    residual_phase = np.degrees(np.angle(corrected) - np.angle(model))
    for name, axes, coordinates in (
        ("residual-amp", ("amp", "corrected-model_scalar", "imag", "data"), [amplitude, observed]),
        ("ratio-phase", ("phase", "corrected/model_scalar", "imag", "data"), [ratio_phase, observed]),
        ("residual-phase-ordinate", ("imag", "data", "phase", "corrected-model_scalar"),
         [observed, residual_phase]),
    ):
        controls = base | dict(zip(("xaxis", "xdatacolumn", "yaxis", "ydatacolumn"), axes, strict=True))
        expected = compare_case(root, native, name, controls, np.column_stack(coordinates))
        if name == "ratio-phase":
            compare_figure(root, native, controls, expected,
                           (base["title"], "Phase:corrected/model (scalar) (degrees)", "Imag"))

    equal_norms = root / "equal-rounded-norms.ms"
    copy_ms(vis, equal_norms)
    with opened(equal_norms, True) as table:
        shape = table.getcol("DATA").shape
        table.putcol("CORRECTED_DATA", np.full(shape, 1024 + 0.125j, dtype=np.complex64))
        table.putcol("MODEL_DATA", np.full(shape, 1024, dtype=np.complex64))
    controls = base | dict(vis=str(equal_norms), xaxis="amp", xdatacolumn="corrected-model_scalar",
                           yaxis="imag", ydatacolumn="data")
    compare_case(root, native, "equal-rounded-norms", controls, [(0, 5), (0, 1)])


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
