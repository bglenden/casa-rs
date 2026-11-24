#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare selected MS integration and wavelength coordinates with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_columns_oracle import (
    compare_figure,
    compare_text,
    fixture,
    opened,
    plotms_launcher,
    reference,
    request,
    rows,
)

SPEED_OF_LIGHT = 299_792_458.0


def make_fixture(root, *, reverse_times=False):
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        index = np.arange(table.nrows())
        uvw = np.array([3 + index / 32, 4 + index / 16, -12 - index / 64])
        interval = 6 + index / 256
        table.putcol("UVW", uvw)
        table.putcol("INTERVAL", interval)
        times = table.getcol("TIME")
        if reverse_times:
            times = times.min() + times.max() - times
            table.putcol("TIME", times)
            table.putcol("TIME_CENTROID", times)
        selected = (
            (table.getcol("FIELD_ID") == 0)
            & (table.getcol("ANTENNA1") == 0)
            & np.isin(table.getcol("ANTENNA2"), [1, 2])
            & (table.getcol("DATA_DESC_ID") == 0)
        )
        antennas = table.getcol("ANTENNA2")
        lookup = {(f"{times[row]:.3f}", str(antennas[row])): row for row in index[selected]}
        first_intervals = {}
        for row in index[selected]:
            first_intervals.setdefault(f"{times[row]:.3f}", interval[row])
    with opened(vis / "SPECTRAL_WINDOW") as table:
        frequencies = table.getcell("CHAN_FREQ", 0)
    return vis, uvw, interval, lookup, first_intervals, frequencies


def compare(root, native):
    fixture_data = make_fixture(root)
    reordered = root / "reordered"
    reordered.mkdir()
    reversed_data = make_fixture(reordered, reverse_times=True)
    cases = (
        ("interval", "interval", "Interval"),
        ("uvwave", "uvwave", "UVwave"),
        ("uwave", "uwave", "Uwave"),
        ("vwave", "vwave", "Vwave"),
        ("wwave", "wwave", "Wwave"),
        ("interval-buffer", "interval", "Interval"),
        ("wavelength-ordinate", "wwave", "Wwave"),
        ("reordered-interval", "time_interval", "Interval"),
        ("reordered-uvwave", "uvdist_l", "UVwave"),
    )
    def controls(vis):
        return request(vis) | dict(yaxis="amp", spw="0:1~3^2", xdatacolumn="", ydatacolumn="")

    for name, axis, label in cases:
        vis, uvw, intervals, lookup, first_intervals, frequencies = (
            reversed_data if name.startswith("reordered-") else fixture_data
        )
        options = controls(vis) | dict(xaxis=axis, expformat="txt")
        if name == "interval-buffer":
            options["antenna"] = "A0&A1;A0&A2"
        ordinate = name == "wavelength-ordinate"
        if ordinate:
            # Channel 2 is selected but flagged in XX.
            options |= dict(xaxis="amp", yaxis=axis, spw="0:1~3")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == (24 if name == "interval-buffer" else 12)
        header = f"# Amp {label} " if ordinate else f"# {label} Amp "
        assert header in expected.read_text()
        assert {point[12] for point in selected} == {"XX"}
        assert {point[2] for point in selected} == {"1", "3"}
        if name == "interval-buffer":
            coordinate = [first_intervals[point[9]] for point in selected]
        else:
            physical = [lookup[point[9], point[6]] for point in selected]
            if label == "Interval":
                coordinate = intervals[physical]
            else:
                metres = {
                    "UVwave": np.hypot(uvw[0], uvw[1]),
                    "Uwave": uvw[0],
                    "Vwave": uvw[1],
                    "Wwave": uvw[2],
                }[label]
                channels = [int(point[2]) for point in selected]
                coordinate = metres[physical] * frequencies[channels] / SPEED_OF_LIGHT
        side = int(ordinate)
        np.testing.assert_allclose([float(point[side]) for point in selected], coordinate, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching metadata coordinates", flush=True)
    for name, xlabel in (("uwave", "Uwave (λ)"), ("interval", "Interval (s)")):
        label = xlabel.split()[0]
        compare_figure(
            root, native, controls(fixture_data[0]) | dict(xaxis=name), root / f"{name}.casa.txt",
            (f"Amp vs. {label}", xlabel, "Amp"),
        )


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
