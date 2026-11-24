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

from calibration_smoothcal_oracle import copy_ms
from plotms_atmosphere_oracle import atmosphere_inputs
from plotms_callibrary_oracle import library_entry
from plotms_ms_atmosphere_oracle import paired_text
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


def controls(vis):
    return request(vis) | dict(yaxis="amp", spw="0:1~3^2", xdatacolumn="", ydatacolumn="")


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

    compare_flags_and_calibration(root, native, fixture_data[0])
    check_frame_refusal(root, native, fixture_data[0])
    compare_feed_selection(root, native)


def compare_flags_and_calibration(root, native, vis):
    base = controls(vis) | dict(xaxis="uwave", expformat="txt")

    caltable = root / "unity.cal"
    casa.gencal(str(vis), str(caltable), "amp", parameter=[1.0])
    options = base | dict(correlation="XX,YY", callib=library_entry(caltable), ydatacolumn="corrected")
    expected, actual = (root / f"callibrary.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    assert [(point[2], point[12]) for point in rows(expected)[:4]] == [
        ("1", "XX"), ("1", "YY"), ("3", "XX"), ("3", "YY"),
    ]
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 24
    print("callibrary: 24 matching metadata coordinates and correlation order", flush=True)

    with opened(vis, True) as table:
        timestamp = f"{table.getcell('TIME', 0):.3f}"
        table.putcell("FLAG_ROW", 0, True)
    expected, actual = (root / f"row-flag-only.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**base, plotfile=str(expected)) is None
    assert [point[9] for point in rows(expected)[:2]] == [timestamp, timestamp]
    assert native(**base, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    print("row-flag-only: 12 matching coordinates with the cube flag mask", flush=True)


def check_frame_refusal(root, native, vis):
    from casars.tasks import TaskParameterError

    for name in ("interval", "uwave"):
        output = root / f"{name}-frame.native.txt"
        options = controls(vis) | dict(xaxis=name, expformat="txt", transform=True, freqframe="BARY")
        try:
            native(**options, plotfile=str(output))
        except TaskParameterError as error:
            assert "reporting-frame transforms for interval and wavelength plots" in str(error)
            assert "#1032" in str(error)
        else:
            raise AssertionError(f"Unsupported {name} reporting frame was exported")
        assert not output.exists()


def compare_feed_selection(root, native):
    work = root / "feed-selection"
    work.mkdir()
    vis, _ = fixture(work)
    feed_table = vis / "FEED"
    with opened(feed_table, True) as table:
        count = table.nrows()
        table.copyrows(str(feed_table), startrowout=count, nrow=count)
        table.putcol("FEED_ID", np.ones(count, dtype=np.int32), startrow=count, nrow=count)
    with opened(vis, True) as table:
        times = table.getcol("TIME")
        _, integrations = np.unique(times, return_inverse=True)
        antenna1 = table.getcol("ANTENNA1")
        antenna2 = table.getcol("ANTENNA2")
        feed1 = (antenna2 == 2).astype(np.int32)
        feed2 = (integrations % 2).astype(np.int32)
        table.putcol("FEED1", feed1)
        table.putcol("FEED2", feed2)
        table.putcol("ARRAY_ID", feed2)
        selected = (table.getcol("FIELD_ID") == 0) & (antenna1 == 0) & np.isin(antenna2, [1, 2])
        flagged = np.flatnonzero(selected & (feed1 == 1) & (feed2 == 0))[0]
        table.putcell("FLAG_ROW", int(flagged), True)

    base = request(vis) | dict(xaxis="channel", yaxis="real", xdatacolumn="", ydatacolumn="data",
                               antenna="A0&A1;A0&A2", spw="0:0~4^2", correlation="XX,YY",
                               title="Feed selection")
    cases = (
        ("single-feed", "1", feed1 != feed2, 30),
        ("cross-feed-pair", "0&1", feed1 != feed2, 30),
        ("same-feed-pair", "1&&&", (feed1 == 1) & (feed2 == 1), 15),
    )
    for name, feed, matches, count in cases:
        options = base | dict(feed=feed)
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected), expformat="txt") is None
        points = rows(expected)
        assert len(points) == count
        assert {(point[6], point[9]) for point in points} == {
            (str(antenna2[row]), f"{times[row]:.3f}") for row in np.flatnonzero(selected & matches)
        }
        assert {(point[2], point[12]) for point in points} == {
            ("0", "XX"), ("0", "YY"), ("2", "YY"), ("4", "XX"), ("4", "YY"),
        }
        assert native(**options, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching selected feed coordinates", flush=True)
    compare_figure(work, native, base | dict(feed="1"), work / "single-feed.casa.txt",
                   ("Feed selection", "Channel", "Real"))
    compare_feed_atmosphere(work, native, vis)


def compare_feed_atmosphere(work, native, source):
    vis = work / "feed-atmosphere.ms"
    copy_ms(source, vis)
    atmosphere_inputs(vis)
    with opened(vis, True) as table:
        times = table.getcol("TIME")
        first_field = table.getcol("FIELD_ID") == 0
        timestamp = times[first_field].max()
        table.putcol("FEED1", (times >= timestamp).astype(np.int32))
        table.putcol("FEED2", np.zeros(len(times), dtype=np.int32))
    with opened(vis / "WEATHER", True) as table:
        table.putcell("TIME", 1, timestamp)
        table.putcell("PRESSURE", 1, 70000.0)
        table.putcell("REL_HUMIDITY", 1, 45.0)
    controls = request(vis) | dict(xaxis="frequency", yaxis="amp", xdatacolumn="", ydatacolumn="",
                                  feed="1", spw="0", showatm=True)
    _, count, _ = paired_text(work, native, "feed-atmosphere", controls)
    assert count == 14


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
