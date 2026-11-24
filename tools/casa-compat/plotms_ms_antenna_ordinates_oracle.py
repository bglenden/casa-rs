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
from plotms_ms_columns_oracle import compare_text, copy_ms, opened, plotms_launcher, reference, request, rows


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
    compare_antenna_ordinate_edges(root, native, vis, controls)


def compare_antenna_ordinate_edges(root, native, vis, controls):
    from casars.tasks import TaskParameterError

    complex_data, flagged = (root / f"{name}.ms" for name in ("complex-ordinates", "all-cube-flagged"))
    copy_ms(vis, complex_data)
    copy_ms(vis, flagged)
    with opened(complex_data, True) as table:
        high = table.getcol("ANTENNA2") == 2
        for column, low, upper in (
            ("DATA", 1 + 2j, 4 + 3j),
            ("CORRECTED_DATA", 2 - 3j, -5 + 7j),
            ("MODEL_DATA", 6 - 2j, -7 - 6j),
        ):
            values = np.where(high, upper, low).astype(np.complex64)
            table.putcol(column, np.broadcast_to(values, table.getcol(column).shape))
    with opened(flagged, True) as table:
        flags = table.getcol("FLAG")
        flags[:] = True
        table.putcol("FLAG", flags)

    four = dict(vis=str(complex_data), xaxis="real", xdatacolumn="corrected",
                yaxis=["real", "imag", "amp", "phase"],
                ydatacolumn=["data", "model", "corrected", "data"],
                yaxislocation=["left", "right", "left", "right"])
    for mode, scalar in (("vector", False), ("scalar", True)):
        for case, overrides, count in (("complex", four, 216), ("empty", dict(vis=str(flagged)), 0)):
            name = f"{mode}-{case}"
            options = controls | overrides | dict(scalar=scalar, expformat="txt")
            expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected)) is None
            assert len(rows(expected)) == count
            assert native(**options, plotfile=str(actual)) is None
            if count:
                assert compare_text(actual, expected) == count
            else:
                assert expected.read_text().count("# No data to export") == 3
                assert actual.read_bytes() == expected.read_bytes()
            print(f"{name}: {count if count else 'three empty sections'} matched", flush=True)

    for name, overrides in (("overlay", dict(showatm=True)), ("iteration", dict(iteraxis="corr"))):
        output = root / f"antenna-list-{name}.txt"
        options = controls | overrides | dict(plotfile=str(output), expformat="txt")
        try:
            native(**options)
        except TaskParameterError as error:
            assert "#1032" in str(error), error
        else:
            raise AssertionError(f"PlotMS accepted the pending antenna-averaged list with {name}")
        assert not output.exists()
        print(f"antenna-list-{name}: existing refusal preserved", flush=True)


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
