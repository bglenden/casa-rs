#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public PlotMS scalar time averages with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_columns_oracle import (
    compare_ordinate_figure,
    compare_text,
    copy_ms,
    opened,
    plotms_launcher,
    reference,
    request,
    rows,
    time_average_fixture,
)


def make_fixture(root):
    vis, integration = time_average_fixture(root)
    with opened(vis, True) as table:
        cycle = integration % 3
        shape = table.getcol("DATA").shape
        for name, amplitudes, phases in (
            ("DATA", (1, 3, 5), (170, -170, 90)),
            ("CORRECTED_DATA", (2, 4, 6), (90, -90, 179)),
            ("MODEL_DATA", (2, 4, 7), (60, -120, 150)),
        ):
            values = np.asarray(amplitudes) * np.exp(1j * np.deg2rad(phases))
            table.putcol(name, np.broadcast_to(values[cycle].astype(np.complex64), shape).copy())
        weight = table.getcol("WEIGHT")
        weight[0] = cycle + 1
        weight[1] = 10 - cycle
        table.putcol("WEIGHT", weight)
        start = table.getcol("TIME").min()
    return vis, integration, start


def compare(root, native):
    source, integration, start = make_fixture(root)
    cube_flags, row_flags = (root / f"{name}-flags.ms" for name in ("cube", "row"))
    copy_ms(source, cube_flags)
    copy_ms(source, row_flags)
    with opened(cube_flags, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, integration == 1] = True
        table.putcol("FLAG", flags)
    with opened(row_flags, True) as table:
        table.putcol("FLAG_ROW", integration == 2)
    controls = request(source) | dict(
        xaxis="amp", xdatacolumn="data", yaxis=["phase", "real"], ydatacolumn=["corrected", "model"],
        yaxislocation=["left", "right"], spw="0:1", avgtime="20", scalar=True,
    )
    amplitude = [(11 / 3, 74.5)] * 2 + [(11 / 3, 31 / 6)] * 2
    cases = (
        ("scalar-amplitude", {}, amplitude),
        ("scalar-phase", dict(xaxis="phase"), [(50 / 3, 74.5)] * 2 + [(50 / 3, 31 / 6)] * 2),
        ("scalar-phase-radians", dict(xaxis="imag"),
         [(np.deg2rad(50 / 3), 74.5)] * 2 + [(np.deg2rad(50 / 3), 31 / 6)] * 2),
        ("scalar-cube-flags", dict(vis=str(cube_flags)),
         [(4, 156.75), (11 / 3, 74.5), (4, 23 / 4), (11 / 3, 31 / 6)]),
        ("scalar-row-flags", dict(vis=str(row_flags)), amplitude),
        ("scalar-single-ordinate", dict(yaxis="phase", ydatacolumn="corrected", yaxislocation="left"),
         amplitude[:2]),
    )
    for name, options, coordinates in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | options | dict(expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == len(coordinates)
        assert [point[9] for point in selected] == [f"{start + offset:.3f}" for offset in (10, 40)] * (len(selected) // 2)
        assert {tuple(point[5:9]) for point in selected} == {("0", "1", "A0@P0", "A1@P1")}
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching scalar time coordinates", flush=True)
    compare_ordinate_figure(root, "scalar-amplitude", native, controls, "Amp",
                           ["Phase:corrected (degrees)", "Real:model"])


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
