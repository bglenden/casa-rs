#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare time averaging across lists of independent PlotMS visibility ordinates."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_channel_ordinates_oracle import compare_sides
from plotms_ms_columns_oracle import (
    compare_text, copy_ms, opened, plotms_launcher, reference, request, rows, time_average_fixture,
)


def compare(root, native):
    vis, integration = time_average_fixture(root)
    masked = root / "masked-time.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[:, :, integration % 2 == 0] = True
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", integration % 2 == 0)
        start = np.min(table.getcol("TIME"))

    base = request(vis) | dict(xaxis="real", xdatacolumn="data", spw="0:1", avgtime="20",
                               yaxis=["real", "real", "real"], ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    vector = (2.6,) * 3
    times = tuple(start + offset for offset in (5, 25, 45))
    scalar = (29 / 11, 55 / 19)
    cases = (
        ("three-ordinates", {}, vector, [vector, (28,) * 3, (280,) * 3]),
        ("four-ordinates", dict(yaxis=["real", "real", "amp", "phase"],
                                ydatacolumn=["data", "corrected", "model", "model"],
                                yaxislocation=["left", "right", "left", "right"]),
         vector, [vector, (28,) * 3, (280,) * 3, (0,) * 3]),
        ("late-corrected", dict(xaxis="time", xdatacolumn="", yaxis=["real", "amp", "real"],
                               ydatacolumn=["data", "model", "corrected"]),
         times, [vector, (280,) * 3, (28,) * 3]),
        ("late-observed", dict(xaxis="time", xdatacolumn="", yaxis=["real", "amp", "real"],
                              ydatacolumn=["corrected", "model", "data"]),
         times, [(28,) * 3, (280,) * 3, vector]),
        ("cube-masked-integrations", dict(vis=str(masked)),
         (3,) * 3, [(3,) * 3, (30,) * 3, (300,) * 3]),
        ("time-and-channel", dict(avgchannel="2", spw="0:0~3"),
         vector * 2, [vector * 2, (28,) * 6, (280,) * 6]),
        ("scalar-time", dict(xaxis="time", xdatacolumn="", scalar=True),
         (start + 10, start + 40),
         [tuple(value * scale for value in scalar) for scale in (1, 10, 100)]),
    )
    for name, overrides, x_values, ordinates in cases:
        controls = base | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        count = len(x_values) * len(ordinates)
        assert len(selected) == count, (name, len(selected))
        pairs = np.array([point[:2] for point in selected], dtype=float)
        is_time = controls["xaxis"] == "time"
        np.testing.assert_allclose(pairs[:, 0], x_values * len(ordinates),
                                   rtol=0 if is_time else 1.2e-5, atol=0.00051 if is_time else 1e-7)
        np.testing.assert_allclose(pairs[:, 1], np.asarray(ordinates).ravel(), rtol=1.2e-5, atol=1e-7)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        if is_time:
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], pairs[:, 0],
                                       rtol=0, atol=0.00051)
        print(f"{name}: {count} matching time-averaged coordinates", flush=True)
    compare_sides(root, native, base, expected_x="Real")


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


if __name__ == "__main__":
    main()
