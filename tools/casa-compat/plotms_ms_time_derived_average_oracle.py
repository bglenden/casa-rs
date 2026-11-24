#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare corrected visibility residuals and ratios after vector time averaging."""

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


def prepare_time_fixture(root):
    vis = weighted_fixture(root)
    with opened(vis, True) as table:
        times = table.getcol("TIME")
        integration = np.rint((times - times.min()) / 10).astype(int)
        for column, step in (("DATA", 1 + 2j), ("CORRECTED_DATA", 3 - 1j), ("MODEL_DATA", .5 + .25j)):
            table.putcol(column, table.getcol(column) + step * integration)
        weights = table.getcol("WEIGHT_SPECTRUM")
        weights[0, 1] = np.where(integration % 2 == 0, 1, 3)
        table.putcol("WEIGHT_SPECTRUM", weights)
        # FLAG_ROW alone does not exclude a clear visibility cube cell.
        table.putcol("FLAG_ROW", integration % 2 == 0)
    return vis, integration, times.min()


def compare_time_derived(root, native):
    vis, integration, start = prepare_time_fixture(root)
    masked = root / "masked-time.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1] = np.isin(integration, [3, 4, 5])
        table.putcol("FLAG", flags)

    base = request(vis) | dict(xaxis="time", xdatacolumn="", yaxis="real", spw="0:1", avgtime="20",
                               title="Time averaged visibility")
    cases = (
        ("residual", vis, "corrected-model_vector", [0, 1, 2], [.75, 2.75, 4.75]),
        ("ratio", vis, "corrected/model_vector", [0, 1, 2], [.75, 2.75, 4.75]),
        ("masked-ratio", masked, "corrected/model_vector", [0, 1], [.75, 2.0]),
    )
    for name, source, column, bins, mean_integrations in cases:
        controls = base | dict(vis=str(source), ydatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[2], point[12]) for point in selected] == [("1", "XX")] * len(bins)
        times = start + 5 + 20 * np.array(bins)
        if name == "masked-ratio":
            # Only the first integration of the second bin remains clear.
            times[1] -= 5
        assert [point[0] for point in selected] == [point[9] for point in selected]
        np.testing.assert_allclose([float(point[0]) for point in selected], times, rtol=0, atol=.00051)
        offsets = np.asarray(mean_integrations)
        corrected = 8 - 1j + (3 - 1j) * offsets
        model = 2 + 2j + (.5 + .25j) * offsets
        values = (corrected - model if name == "residual" else corrected / model).real
        np.testing.assert_allclose([float(point[1]) for point in selected], values, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                   [float(point[0]) for point in selected], rtol=0, atol=.00051)
        print(f"{name}: {len(selected)} matching time-averaged coordinates", flush=True)
        if name == "masked-ratio":
            compare_figure(root, native, controls, expected,
                           (controls["title"], "Time", "Real:corrected/model (vector)"))


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
        compare_time_derived(root, native)


if __name__ == "__main__":
    main()
