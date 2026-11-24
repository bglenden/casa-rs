#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare channel-averaged derived visibilities against Channel and Time."""

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
from plotms_ms_scalar_derived_average_oracle import compare_case


def compare_metadata(root, native, vis):
    base = request(vis) | dict(xdatacolumn="", avgchannel="2", spw="0:0~3")
    corrected = np.array([7.4 - 0.6j, 12 + 4j])
    observed = np.array([2.6 + 5j, 11 + 1j])
    model = np.array([1.9 + 1.7j, 5 - 3j])
    controls = base | dict(xaxis="channel", yaxis="amp", ydatacolumn="corrected-model_vector",
                           title="Channel residual amplitude")
    expected = compare_case(root, native, "channel-residual", controls,
                            np.column_stack([[0, 1], np.abs(corrected - model)]))
    compare_figure(root, native, controls, expected,
                   (controls["title"], "Channel", "Amp:corrected-model (vector)"))

    controls = base | dict(xaxis="time", yaxis="phase", ydatacolumn="data/model_scalar")
    expected, actual = (root / f"time-ratio.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[2], point[12]) for point in selected] == [("0", "XX"), ("1", "XX")] * 6
    np.testing.assert_array_equal([float(point[0]) for point in selected],
                                  [float(point[9]) for point in selected])
    np.testing.assert_allclose([float(point[1]) for point in selected],
                               np.tile(np.degrees(np.angle(observed) / np.abs(model)), 6), rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 12
    print("time-ratio: 12 matching selected times and phase coordinates", flush=True)


def compare_primary_mask(root, native, vis):
    source = root / "zero-primary-spectrum.ms"
    copy_ms(vis, source)
    with opened(source, True) as table:
        spectrum = table.getcol("SIGMA_SPECTRUM")
        spectrum[:, :2, :] = 0
        table.putcol("SIGMA_SPECTRUM", spectrum)
    controls = request(source) | dict(xaxis="channel", xdatacolumn="", yaxis="amp",
                                      ydatacolumn="corrected-model_vector", avgchannel="2",
                                      spw="0:0~3", expformat="txt")
    expected, actual = (root / f"zero-primary.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    selected = rows(expected)
    assert [(point[2], point[12]) for point in selected] == [("1", "XX")] * 6
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               [(1, np.sqrt(98))] * 6, rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 6
    print("zero-primary: 6 matching points after masking the first channel bin", flush=True)


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
        vis = weighted_fixture(root)
        compare_metadata(root, native, vis)
        compare_primary_mask(root, native, vis)


if __name__ == "__main__":
    main()
