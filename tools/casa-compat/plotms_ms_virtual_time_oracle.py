#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare read-only virtual MeasurementSet model prediction before PlotMS time averages."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_channel_ordinates_oracle import compare_model_provider, virtual_fixtures
from plotms_ms_columns_oracle import (
    compare_ordinate_figure, compare_text, opened, plotms_launcher, reference, request, rows,
    time_average_fixture,
)


def compare(root, native):
    vis, _ = time_average_fixture(root)
    virtual, mixed, stored_model = virtual_fixtures(root, vis)
    with opened(vis) as table:
        start = np.min(table.getcol("TIME"))

    base = request(virtual) | dict(xaxis="time", xdatacolumn="", spw="0:1", avgtime="20",
                                   yaxis=["real", "real"], ydatacolumn=["data", "model"],
                                   yaxislocation=["left", "right"])
    times = tuple(start + offset for offset in (5, 25, 45))
    vector = [(2.6,) * 3, (7.25,) * 3]
    cases = (
        ("virtual-time", {}, times, vector),
        ("virtual-overrides-stored-time", dict(vis=str(mixed)), times, vector),
        ("virtual-time-third-ordinate", dict(vis=str(mixed), yaxis=["real"] * 3,
                                             ydatacolumn=["data", "corrected", "model"],
                                             yaxislocation=["left", "right", "left"]),
         times, [(2.6,) * 3, (28,) * 3, (7.25,) * 3]),
        ("virtual-time-channel", dict(vis=str(mixed), spw="0:0~3", avgchannel="2"),
         tuple(time for time in times for _ in range(2)), [(2.6,) * 6, (7.25,) * 6]),
        ("virtual-scalar-time", dict(vis=str(mixed), scalar=True),
         (start + 10, start + 40), [(29 / 11, 55 / 19), (7.25, 7.25)]),
        ("virtual-time-model-x", dict(vis=str(mixed), xaxis="real", xdatacolumn="model",
                                      yaxis="channel", ydatacolumn="", yaxislocation="left"),
         (7.25,) * 3, [(1,) * 3]),
        ("virtual-scalar-model-x", dict(vis=str(mixed), xaxis="real", xdatacolumn="model",
                                       yaxis="channel", ydatacolumn="", yaxislocation="left", scalar=True),
         (7.25,) * 2, [(1,) * 2]),
    )
    for name, overrides, x_values, ordinates in cases:
        controls = base | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        count = len(x_values) * len(ordinates)
        assert len(selected) == count, (name, len(selected))
        pairs = np.array([point[:2] for point in selected], dtype=float)
        np.testing.assert_allclose(pairs[:, 0], x_values * len(ordinates), rtol=0, atol=0.00051)
        np.testing.assert_allclose(pairs[:, 1], np.asarray(ordinates).ravel(), rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)], pairs[:, 0],
                                   rtol=0, atol=0.00051)
        print(f"{name}: {count} matching coordinates with virtual MODEL", flush=True)
    assert all(
        np.array_equal(rows(root / f"virtual-time.{engine}.txt"),
                       rows(root / f"virtual-overrides-stored-time.{engine}.txt"))
        for engine in ("casa", "native")
    )
    compare_ordinate_figure(root, "virtual-overrides-stored-time", native,
                           base | dict(vis=str(mixed)), "Time (MJD seconds)", ["Real", "Real:model"])
    compare_model_provider(root, native, base | dict(vis=str(mixed), expformat="txt"))
    with opened(virtual) as table:
        assert "MODEL_DATA" not in table.colnames()
    with opened(mixed) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), stored_model)


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
