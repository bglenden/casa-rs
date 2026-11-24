#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare independent PlotMS visibility ordinates after per-antenna averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_baseline_channels_oracle import make_fixture
from plotms_ms_channel_ordinates_oracle import compare_sides
from plotms_ms_columns_oracle import compare_text, plotms_launcher, reference, request, rows


def compare_antenna_ordinate_lists(root, native):
    root.mkdir()
    vis, _, _, spectra_flags = make_fixture(root)
    controls = request(vis) | dict(antenna="A0&A1;A0&A2", xaxis="channel", xdatacolumn="", spw="0:0~3",
                                    avgantenna=True, yaxis=["real"] * 3,
                                    ydatacolumn=["data", "corrected", "model"],
                                    yaxislocation=["left", "right", "left"])
    time_channel = dict(vis=str(spectra_flags), avgtime="20", avgchannel="2")
    for name, overrides, count, first in (
        ("vector-antenna", {}, 162, (3.7, 37., 370.)),
        ("scalar-antenna", dict(scalar=True), 162, (3.7, 37., 370.)),
        ("vector-time-channel-antenna", time_channel, 36, (3.3, 30., 300.)),
        ("scalar-time-channel-antenna", time_channel | dict(scalar=True), 36, (2.84, 26., 260.)),
    ):
        options = controls | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == count
        assert {tuple(point[5:9]) for point in selected} == {
            (str(antenna), "-1", f"A{antenna}@P{antenna}", "*") for antenna in (0, 1, 2)
        }
        assert {point[12] for point in selected} == {"XX"}
        np.testing.assert_allclose([float(selected[index * (count // 3)][1]) for index in range(3)],
                                   first, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching antenna-averaged ordinate coordinates", flush=True)
        if name == "vector-time-channel-antenna":
            figure = controls | overrides | dict(xlabel="Channel bin")
            compare_sides(root, native, figure, expected_x="Channel bin", name=name)


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
        compare_antenna_ordinate_lists(root / "ordinate-lists", native)


if __name__ == "__main__":
    main()
