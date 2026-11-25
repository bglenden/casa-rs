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
from plotms_callibrary_oracle import library_entry, snapshot, verify_source
from plotms_ms_atmosphere_oracle import paired_text
from plotms_ms_columns_oracle import (
    compare_empty_export,
    compare_figure,
    compare_text,
    fixture,
    opened,
    plotms_launcher,
    reference,
    request,
    rows,
)
from record_casa_returns import make_cal_ms

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
    feed_fixture = compare_feed_selection(root, native)
    row_fixture = compare_row_axes(root, native, feed_fixture)
    compare_metadata_pairs(root, native, row_fixture)
    compare_averaged_row_axes(root, native, row_axis_request(row_fixture))


def compare_averaged_coordinates(root, native):
    work = root / "time-averaged-coordinates"
    work.mkdir()
    vis, uvw, intervals, _, _, frequencies = make_fixture(work, reverse_times=True)
    with opened(vis, True) as table:
        timestamps = table.getcol("TIME")
        antenna2 = table.getcol("ANTENNA2")
        selected = (table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
        baselines = {}
        for antenna in (1, 2):
            candidates = np.flatnonzero(selected & (antenna2 == antenna))
            baselines[antenna] = candidates[np.argsort(timestamps[candidates])]
            assert len(candidates) == 6
        for antenna, indices in baselines.items():
            integration = np.arange(len(indices))
            uvw[0, indices] = 1 + 2 * integration + 20 * (antenna - 1)
            uvw[2, indices] = -4 - 5 * integration
        table.putcol("UVW", uvw)
        weights = table.getcol("WEIGHT")
        sigmas = table.getcol("SIGMA")
        observed = np.arange(1, 7)
        calibrated = 2 * observed - 1
        for indices in baselines.values():
            weights[:, indices] = calibrated
            sigmas[:, indices] = 1 / np.sqrt(observed)
        table.putcol("WEIGHT", weights)
        table.putcol("SIGMA", sigmas)
        flags = table.getcol("FLAG")
        flags[:, :, baselines[1][2:4]] = True
        table.putcol("FLAG", flags)
    before = snapshot(vis)
    title = "Averaged MeasurementSet coordinates"
    base = controls(vis) | dict(antenna="A0&A1;A0&A2", spw="0:1~3", title=title, expformat="txt")
    cases = (
        ("averaged-interval", dict(xaxis="interval"), 2, 0, None, None),
        ("single-integration-interval", dict(xaxis="interval", ydatacolumn="corrected"), 1, 0, None, None),
        ("averaged-uwave", dict(xaxis="uwave"), 2, 0, 0, observed),
        ("calibrated-uwave", dict(xaxis="uwave", ydatacolumn="corrected"), 2, 0, 0, calibrated),
        ("averaged-wwave-ordinate", dict(xaxis="real", xdatacolumn="data", yaxis="wwave"), 2, 1, 2, observed),
    )
    for name, changes, width, side, component, role_weights in cases:
        options = base | changes | dict(avgtime=str(10 * width if width == 2 else 5))
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        points = rows(expected)
        expected_samples, expected_coordinates = [], []
        for start in range(0, 6, width):
            stop = start + width
            first, last = baselines[1][[start, stop - 1]]
            timestamp = timestamps[first] + (timestamps[last] - timestamps[first]) / 2
            interval = timestamps[last] - timestamps[first] + (intervals[first] + intervals[last]) / 2
            for antenna, indices in baselines.items():
                if antenna == 1 and 2 <= start < 4:
                    continue
                for channel in (1, 3):
                    expected_samples.append((channel, antenna, timestamp))
                    coordinate = interval
                    if component is not None:
                        metres = np.average(uvw[component, indices[start:stop]], weights=role_weights[start:stop])
                        coordinate = metres / SPEED_OF_LIGHT * frequencies[channel]
                    expected_coordinates.append(coordinate)
        assert [(int(row[2]), int(row[6])) for row in points] == [key[:2] for key in expected_samples]
        assert {row[12] for row in points} == {"XX"}
        np.testing.assert_allclose([float(row[9]) for row in points], [key[2] for key in expected_samples],
                                   rtol=0, atol=0.00051)
        np.testing.assert_allclose([float(row[side]) for row in points], expected_coordinates, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(expected_samples)
        print(f"{name}: {len(points)} matching averaged geometry coordinates", flush=True)
    for axis, label in (("interval", "Interval (s)"), ("uwave", "Uwave (λ)")):
        compare_figure(work, native, base | dict(xaxis=axis, avgtime="20"),
                       work / f"averaged-{axis}.casa.txt", (title, label, "Amp"))
    verify_source(vis, before)
    with opened(vis) as table:
        np.testing.assert_array_equal(table.getcol("UVW"), uvw)
        np.testing.assert_array_equal(table.getcol("INTERVAL"), intervals)
        np.testing.assert_array_equal(table.getcol("TIME"), timestamps)


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


def make_feed_selection_fixture(work):
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

    return vis


def compare_feed_selection(root, native):
    work = root / "feed-selection"
    work.mkdir()
    vis = make_feed_selection_fixture(work)
    with opened(vis) as table:
        times = table.getcol("TIME")
        antenna2 = table.getcol("ANTENNA2")
        feed1, feed2 = (table.getcol(column) for column in ("FEED1", "FEED2"))
        selected = (table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
        selected &= np.isin(antenna2, [1, 2])

    base = request(vis) | dict(xaxis="channel", yaxis="real", xdatacolumn="", ydatacolumn="data",
                               antenna="A0&A1;A0&A2", spw="0:0~4^2", correlation="XX,YY",
                               title="Feed selection")
    cases = (
        ("single-feed", "1", "", feed1 != feed2, 30),
        ("cross-feed-pair", "0&1", "", feed1 != feed2, 30),
        ("same-feed-pair", "1&&&", "", (feed1 == 1) & (feed2 == 1), 15),
        ("array-and-feed", "1", "1", (feed1 != feed2) & (feed2 == 1), 15),
    )
    for name, feed, array, matches, count in cases:
        options = base | dict(feed=feed, array=array)
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
    caltable = work / "feed-unity.cal"
    casa.gencal(str(vis), str(caltable), "amp", parameter=[1.0])
    calibrated = base | dict(feed="0&1", ydatacolumn="corrected", callib=library_entry(caltable))
    expected, actual = (work / f"feed-callibrary.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**calibrated, plotfile=str(expected), expformat="txt") is None
    assert native(**calibrated, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 30
    print("feed-callibrary: 30 matching selected feed coordinates", flush=True)
    compare_feed_atmosphere(work, native, vis)
    return vis


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


def make_row_axes_fixture(work, source):
    vis = work / "rows.ms"
    copy_ms(source, vis)
    with opened(vis / "STATE", True) as table:
        first_state = table.nrows()
        table.addrows(3)
        modes = np.array(["CALIBRATE_PHASE#ON_SOURCE", "OBSERVE_TARGET#ON_SOURCE",
                          "CALIBRATE_PHASE#ON_SOURCE"])
        table.putcol("OBS_MODE", modes, startrow=first_state, nrow=len(modes))
    observation = vis / "OBSERVATION"
    with opened(observation, True) as table:
        next_observation = table.nrows()
        table.copyrows(str(observation), startrowout=next_observation, nrow=1)
    with opened(vis, True) as table:
        times, integrations = np.unique(table.getcol("TIME"), return_inverse=True)
        second_baseline = table.getcol("ANTENNA2") == 2
        states = first_state + np.where(second_baseline, 1, integrations % 3)
        table.putcol("STATE_ID", states.astype(np.int32))
        table.putcol("OBSERVATION_ID", np.where(integrations % 6 < 3, 0, next_observation).astype(np.int32))

    return vis, times, next_observation


def row_axis_request(vis):
    return request(vis) | dict(xaxis="row", yaxis="real", xdatacolumn="", ydatacolumn="data",
                               antenna="A0&A1;A0&A2", spw="0:1~5^2", correlation="XX,YY",
                               title="Selected visibility rows")


def compare_row_axes(root, native, source):
    from casatools import quanta

    work = root / "row-axes"
    work.mkdir()
    vis, times, next_observation = make_row_axes_fixture(work, source)
    base = row_axis_request(vis)
    units = quanta()
    bounds = times[[1, 3]] + [-1.0, 1.0]
    timerange = "~".join(units.time(units.quantity(value, "s"), form="ymd", prec=10)[0]
                         for value in bounds)
    cases = (
        ("row", {}, 72, set(range(12))),
        ("antenna1", dict(xaxis="ant1", antenna="A0&A1;A0&A2;A1&A2", spw="0:0~4^2"), 90, {0, 1}),
        ("antenna2", dict(xaxis="antenna2"), 72, {1, 2}),
        ("observation", dict(xaxis="observation"), 72, {0, next_observation}),
        ("intent", dict(xaxis="intent"), 72, {1, 2}),
        ("feed1", dict(xaxis="feed1"), 72, {0, 1}),
        ("feed2", dict(xaxis="feed2"), 72, {0, 1}),
        ("observation-selected", dict(xaxis="observation", observation=str(next_observation)),
         36, {next_observation}),
        ("intent-selected", dict(xaxis="intent", intent="OBSERVE_TARGET*"), 48, {2}),
        ("feed-selected", dict(xaxis="feed2", feed="1"), 36, {0, 1}),
        ("time-selected", dict(timerange=timerange), 36, set(range(6))),
        ("uv-selected", dict(uvrange=">29.9m"), 36, set(range(6))),
        ("taql-selected", dict(msselect="ARRAY_ID = 1 AND ANTENNA2 = 2"), 18, {0, 1, 2}),
        ("row-ordinate", dict(xaxis="real", yaxis="row", xdatacolumn="data", ydatacolumn=""),
         72, set(range(12))),
    )
    for name, options, count, identifiers in cases:
        controls = base | options
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        points = rows(expected)
        axis_slot = int(name == "row-ordinate")
        assert len(points) == count, (name, len(points), count)
        assert {int(point[axis_slot]) for point in points} == identifiers, name
        channel_correlations = {(point[2], point[12]) for point in points}
        if name == "antenna1":
            assert channel_correlations == {
                ("0", "XX"), ("0", "YY"), ("2", "YY"), ("4", "XX"), ("4", "YY"),
            }
        else:
            assert channel_correlations == {(chan, corr) for chan in ("1", "3", "5") for corr in ("XX", "YY")}
        if name == "row":
            assert [int(point[0]) for point in points[::6]] == [0, 1, 5, 4, 2, 3, 8, 9, 6, 7, 11, 10]
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching row axis coordinates", flush=True)
    compare_figure(work, native, base, work / "row.casa.txt", ("Selected visibility rows", "Row", "Real"))
    compare_unaveraged_row_axes(work, native, base)
    return vis


def compare_unaveraged_row_axes(root, native, base):
    from plotms_ms_iterated_ordinates_oracle import compare_figures

    source = Path(base["vis"])
    before = snapshot(source)
    mixed = base | dict(xaxis="real", yaxis=["row", "elevation"], xdatacolumn="data", ydatacolumn="")
    row_only = root / "row-ordinate.casa.txt"
    if not row_only.exists():
        assert reference(**(mixed | dict(yaxis="row")), plotfile=str(row_only), expformat="txt") is None
    expected = root / "row-and-elevation.casa.txt"
    actual = root / "row-and-elevation.native.txt"
    assert reference(**mixed, plotfile=str(expected), expformat="txt") is None
    assert native(**mixed, plotfile=str(actual), expformat="txt") is None
    row_points = rows(row_only)
    assert len(row_points) == 72 and rows(expected)[:72] == row_points
    assert compare_text(actual, expected) == 144
    assert "# Real Elevation " in actual.read_text()
    print("row-and-elevation: 144 matching points with independent ordinate masks", flush=True)

    separated = root / "separated-antennas.ms"
    copy_ms(source, separated)
    with opened(separated / "ANTENNA", True) as table:
        # Moving A1 in longitude distinguishes observatory elevation from its
        # topocentric elevation along the selected A1&A2 baseline.
        x, y, z = table.getcell("POSITION", 1)
        cosine, sine = np.cos(0.08), np.sin(0.08)
        table.putcell("POSITION", 1, np.array([cosine * x - sine * y, sine * x + cosine * y, z]))
    before_separated = snapshot(separated)
    separated_controls = mixed | dict(vis=str(separated), antenna="A0&A1;A1&A2")
    expected, actual = (root / f"row-observatory-elevation.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**separated_controls, plotfile=str(expected), expformat="txt") is None
    assert native(**separated_controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 144
    assert {point[5] for point in rows(expected)[72:]} == {"0", "1"}
    verify_source(separated, before_separated)
    print("row-observatory-elevation: 144 matching points with two first-antenna positions", flush=True)

    two, _, _ = make_compound_selection_fixture(root)
    before_two = snapshot(two)
    scans = root / "two-scan-rows.ms"
    copy_ms(source, scans)
    with opened(scans, True) as table:
        times = table.getcol("TIME")
        first_field = table.getcol("FIELD_ID") == 0
        boundary = np.unique(times[first_field])[3]
        scan_numbers = table.getcol("SCAN_NUMBER")
        scan_numbers[first_field & (times >= boundary)] = 2
        table.putcol("SCAN_NUMBER", scan_numbers)
    before_scans = snapshot(scans)
    amplitude = root / "row-amplitude.cal"
    casa.gencal(str(source), str(amplitude), "amp", parameter=[2.0])
    cases = (
        ("row-two-windows", dict(vis=str(two), spw="0:1~5^2,1:1~5^2", correlation="XX"), 47),
        ("time-row-scan", dict(vis=str(scans), xaxis="time", yaxis=["row"], ydatacolumn="",
                              iteraxis="scan", gridrows=2, exprange="all", xlabel="Time (MJD seconds)"), 12),
        ("row-callibrary", dict(ydatacolumn="corrected", callib=library_entry(amplitude)), 72),
    )
    for name, overrides, count in cases:
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        if name == "row-two-windows":
            assert {point[11] for point in rows(actual)} == {"0", "1"}
            assert {int(point[0]) for point in rows(actual)} == set(range(16))
        if name == "time-row-scan":
            points = rows(expected)
            assert [point[3] for point in points] == ["1"] * 6 + ["2"] * 6
            values = np.array([point[:2] for point in points], dtype=float)
            np.testing.assert_allclose(np.array([point[0] for point in rows(actual)], dtype=float),
                                       values[:, 0], rtol=0, atol=0.00051)
            pages = [[(scan, coords)] for scan, coords in zip(("1", "2"), np.split(values, 2), strict=True)]
            compare_figures(root, native, "time-row-scan-pages", controls | dict(gridrows=1), pages,
                            ["Scan1", "Scan2_2"], [("left", "Row", [0])])
        print(f"{name}: {count} matching selected Row coordinates", flush=True)
    verify_source(source, before)
    verify_source(two, before_two)
    verify_source(scans, before_scans)


def compare_metadata_pairs(root, native, source):
    work = root / "metadata-pairs"
    work.mkdir()
    vis = work / "metadata.ms"
    copy_ms(source, vis)
    with opened(vis, True) as table:
        selection = (table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
        selection &= np.isin(table.getcol("ANTENNA2"), [1, 2])
        flagged_row, flagged_channel = np.flatnonzero(selection)[:2]
        flags = table.getcol("FLAG")
        flags[:, :, flagged_row] = True
        flags[:, 2, flagged_channel] = True
        table.putcol("FLAG", flags)
    base = request(vis) | dict(xdatacolumn="", ydatacolumn="", antenna="A0&A1;A0&A2",
                               spw="0:0~4^2", correlation="XX,YY", title="Selected metadata")
    cases = (
        ("channel-row", dict(xaxis="channel", yaxis="row", title=""), 32,
         ("Row vs. Channel", "Channel", "Row")),
        ("time-row", dict(xaxis="time", yaxis="row"), 11, None),
        ("channel-observation", dict(xaxis="channel", yaxis="observation"), 26, None),
        ("time-observation", dict(xaxis="time", yaxis="observation"), 9,
         ("Selected metadata", "Time (MJD seconds)", "Observation")),
        ("default-window-time-row", dict(xaxis="time", yaxis="row", spw="", correlation="YY"), 11, None),
        ("zero-average-time-row", dict(xaxis="time", yaxis="row", averagedata=True, avgchannel="0", avgtime="0"),
         11, None),
    )
    for name, options, count, labels in cases:
        controls = base | options
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        points = rows(expected)
        assert len(points) == count, (name, len(points), count)
        if controls["yaxis"] == "row":
            assert {point[1] for point in points} == {str(row) for row in range(1, 12)}
        if name == "channel-row":
            assert not any(point[2] == "2" and point[1] == "1" for point in points)
            assert any((point[2], point[1], point[12]) == ("2", "3", "XX") for point in points)
        if name == "default-window-time-row":
            assert {(point[2], point[12]) for point in points} == {("0", "YY"), ("1", "YY")}
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching collapsed metadata coordinates", flush=True)
        if labels:
            compare_figure(work, native, controls, expected, labels)
    check_metadata_flagged_symbols(work, native, base)


def check_metadata_flagged_symbols(work, native, base):
    from casars.tasks import TaskParameterError

    controls = base | dict(xaxis="channel", yaxis="row", customflaggedsymbol=True)
    expected = work / "flagged-symbols.casa.txt"
    actual = work / "flagged-symbols.native.txt"
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    assert len(rows(expected)) == 36
    assert {point[1] for point in rows(expected)} == {str(row) for row in range(12)}
    try:
        native(**controls, plotfile=str(actual), expformat="txt")
    except TaskParameterError as error:
        assert "displaying flagged points" in str(error) and "#1032" in str(error)
    else:
        raise AssertionError("Metadata flag symbols were accepted without exporting flagged points")
    assert not actual.exists()


def compare_pointing_axes(root, native):
    work = root / "pointing-axes"
    work.mkdir()
    vis = work / "pointing.ms"
    make_cal_ms(str(vis), autocorrelation_weight=1.0)
    with opened(vis, True) as table:
        auto = np.flatnonzero((table.getcol("ANTENNA1") == 0) & (table.getcol("ANTENNA2") == 0))
        assert len(auto) == 12
        epoch = table.getcell("TIME", int(auto[0]))
        flags = table.getcol("FLAG")
        flags[1, [1, 3], auto[2]] = True
        flags[1, 1, auto[3]] = True
        table.putcol("FLAG", flags)
        table.putcell("FLAG_ROW", int(auto[4]), True)
    with opened(vis / "POINTING", True) as table:
        a0 = np.flatnonzero(table.getcol("ANTENNA_ID") == 0)
        table.removerows(a0[4:])
        # Store four distinct pointing epochs out of order. MAIN spans both
        # endpoints and queries after the midpoint of two different segments.
        samples = [(85, 99, -39), (5, -10, -28), (65, 47, -32), (25, 12, -25)]
        for row, (offset, lon, lat) in zip(a0[:4], samples, strict=True):
            table.putcell("TIME", int(row), epoch + offset)
            table.putcell("DIRECTION", int(row), np.deg2rad([[lon], [lat]]))

    base = dict(vis=str(vis), xaxis="time", yaxis="ant-ra", antenna="A0&&&", yframe="j2000",
                yinterp="nearest", showgui=False, overwrite=True)
    sparse = work / "sparse-pointing.ms"
    copy_ms(vis, sparse)
    with opened(sparse / "POINTING", True) as table:
        a0 = np.flatnonzero(table.getcol("ANTENNA_ID") == 0)
        table.removerows(a0[-1:])
    missing = work / "missing-galactic-pointing.ms"
    copy_ms(vis, missing)
    with opened(missing / "POINTING", True) as table:
        table.removerows(np.flatnonzero(table.getcol("ANTENNA_ID") == 0))
        keyword = table.getcolkeyword("DIRECTION", "MEASINFO")
        table.putcolkeyword("DIRECTION", "MEASINFO", keyword | {"Ref": "GALACTIC"})
    meridian = work / "meridian-pointing.ms"
    copy_ms(vis, meridian)
    with opened(meridian / "POINTING", True) as table:
        a0 = np.flatnonzero(table.getcol("ANTENNA_ID") == 0)
        order = a0[np.argsort(table.getcol("TIME")[a0])]
        for row, longitude in zip(order, range(-180, -176), strict=True):
            table.putcell("DIRECTION", int(row), np.deg2rad([[longitude], [-30]]))
    tai = work / "tai-pointing.ms"
    copy_ms(vis, tai)
    with opened(tai, True) as table:
        keyword = table.getcolkeyword("TIME", "MEASINFO")
        table.putcolkeyword("TIME", "MEASINFO", keyword | {"Ref": "TAI"})
    lon = [350] * 3 + [12] * 4 + [47] * 5
    lat = [-28] * 3 + [-25] * 4 + [-32] * 5
    cases = (
        ("nearest-ra", {}, 12),
        ("nearest-dec", dict(xaxis="ant-dec", xframe="j2000", xinterp="nearest",
                             yaxis="time", yframe="", yinterp=""), 12),
        ("nearest-ra-dec", dict(xaxis="ant-ra", xframe="j2000", xinterp="nearest", yaxis="ant-dec"), 12),
        ("nearest-selected", dict(field="FLUX", spw="0:1~3^2", correlation="YY"), 5),
        ("nearest-two-baselines", dict(antenna="A0&A1;A1&A2"), 24),
        ("nearest-galactic", dict(yframe="galactic"), 12),
        ("nearest-azelgeo", dict(yaxis="ant-dec", yframe="azelgeo"), 12),
        ("nearest-azelgeo-tai", dict(vis=str(tai), yaxis="ant-dec", yframe="azelgeo"), 12),
        ("nearest-b1950", dict(yframe="b1950"), 12),
        ("independent-pointing-axes", dict(xaxis="ant-ra", xframe="j2000", xinterp="nearest",
                                         yaxis="ant-dec", yframe="galactic", yinterp="spline"), 12),
        ("default-pointing", dict(yframe="", yinterp=""), 12),
        ("spline-pointing", dict(yinterp="spline"), 12),
        ("sparse-pointing", dict(vis=str(sparse)), 12),
        ("missing-galactic-pointing", dict(vis=str(missing), yinterp="spline"), 12),
        ("signed-meridian", dict(vis=str(meridian), yinterp="spline"), 12),
    )
    for name, overrides, count in cases:
        controls = base | overrides
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        points = rows(expected)
        assert len(points) == count
        antennas = {(p[5], p[6], p[7], p[8]) for p in points}
        if name == "nearest-two-baselines":
            assert antennas == {("0", "1", "A0@P0", "A1@P1"), ("1", "2", "A1@P1", "A2@P2")}
            np.testing.assert_allclose([float(p[1]) for p in points if p[5] == "0"], lon)
            np.testing.assert_allclose([float(p[1]) for p in points if p[5] == "1"], [0] * 6 + [15] * 6)
        else:
            assert antennas == {("0", "0", "A0@P0", "A0@P0")}
        if name == "nearest-ra":
            np.testing.assert_allclose([float(p[1]) for p in points], lon)
        if name == "nearest-dec":
            np.testing.assert_allclose([float(p[0]) for p in points], lat)
        if name == "independent-pointing-axes":
            np.testing.assert_allclose([float(p[0]) for p in points], lon)
        if name == "sparse-pointing":
            assert all(float(p[1]) == 0 for p in points)
        if name == "missing-galactic-pointing":
            np.testing.assert_allclose([float(p[1]) for p in points], 266.405, atol=0.001, rtol=0)
        if name == "signed-meridian":
            assert all(179 < float(p[1]) < 186 for p in points)
        if name == "nearest-selected":
            assert {(p[2], p[12]) for p in points} == {("1", "YY")}
            np.testing.assert_allclose([float(p[1]) for p in points], [350, 350, 12, 12, 12])
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        np.testing.assert_allclose(np.array([p[:2] for p in rows(actual)], dtype=float),
                                   np.array([p[:2] for p in points], dtype=float), rtol=0, atol=2.1e-5)
        print(f"{name}: {count} matching pointing coordinates and identifiers", flush=True)

    figure = base | dict(xaxis="ant-ra", xframe="j2000", xinterp="nearest", yaxis="ant-dec",
                         title="Antenna pointing")
    compare_figure(work, native, figure, work / "nearest-ra-dec.casa.txt",
                   ("Antenna pointing", "J2000 Right Ascension (degrees)", "J2000 Declination (degrees)"))
    masked = work / "all-pointings-flagged.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
    labels = ("Time (MJD seconds)", "J2000 Right Ascension (degrees)")
    compare_empty_export(work, native, "flagged-pointing", base | dict(vis=str(masked)), labels)
    compare_empty_export(work, native, "hidden-pointing", base | dict(customsymbol=True, symbolshape="nosymbol"),
                         labels)
    check_pointing_observatory(work, native, base, vis)


def check_pointing_observatory(work, native, controls, vis):
    from casars.tasks import TaskExecutionError

    missing = work / "no-observatory.ms"
    copy_ms(vis, missing)
    with opened(missing / "OBSERVATION", True) as table:
        table.removerows(list(range(table.nrows())))
    actual = work / "no-observatory.native.txt"
    try:
        native(**(controls | dict(vis=str(missing))), plotfile=str(actual), expformat="txt")
    except TaskExecutionError as error:
        assert "antenna directions require an observatory" in str(error), error
    else:
        raise AssertionError("A pointing plot was exported without an observatory")
    assert not actual.exists()


def make_compound_selection_fixture(work):
    from plotms_ms_spw_average_oracle import make_fixture as make_spw_fixture

    vis = make_spw_fixture(work, antenna_count=3, channel_count=8, integrations=5)
    feed = vis / 'FEED'
    with opened(feed, True) as table:
        count = table.nrows()
        table.copyrows(str(feed), startrowout=count, nrow=count)
        table.putcol('FEED_ID', np.ones(count, dtype=np.int32), startrow=count, nrow=count)
    field = vis / 'FIELD'
    with opened(field, True) as table:
        alternate = table.nrows()
        table.copyrows(str(field), startrowout=alternate, nrow=1)
        table.putcell('NAME', alternate, 'PHASE')
    observation = vis / 'OBSERVATION'
    with opened(observation, True) as table:
        obs = table.nrows()
        table.copyrows(str(observation), startrowout=obs, nrow=1)
    with opened(vis / 'STATE', True) as table:
        target = table.nrows()
        table.addrows(2)
        table.putcol('OBS_MODE', np.array(['OBSERVE_TARGET#ON_SOURCE', 'CALIBRATE_PHASE#ON_SOURCE']),
                     startrow=target, nrow=2)
    with opened(vis, True) as table:
        time = table.getcol('TIME')
        integrations, slot = np.unique(time, return_inverse=True)
        window = table.getcol('DATA_DESC_ID')
        ant2 = table.getcol('ANTENNA2')
        table.putcol('SCAN_NUMBER', np.where((slot > 0) & (slot < 4), 2, 1).astype(np.int32))
        table.putcol('FIELD_ID', np.where(slot == 4, alternate, 0).astype(np.int32))
        table.putcol('STATE_ID', (target + (slot == 3)).astype(np.int32))
        table.putcol('OBSERVATION_ID', np.where(slot == 0, 0, obs).astype(np.int32))
        table.putcol('ARRAY_ID', (slot < 4).astype(np.int32))
        table.putcol('FEED1', (ant2 == 2).astype(np.int32))
        table.putcol('FEED2', np.zeros(len(time), dtype=np.int32))
        uvw = np.zeros((3, len(time)))
        uvw[0] = np.where((window == 0) & (slot == 2), 15, 50)
        table.putcol('UVW', uvw)
        clear = (table.getcol('ANTENNA1') == 0) & (ant2 == 2)
        flags = table.getcol('FLAG')
        flags[0, 3, clear & (window == 1) & (slot == 2)] = True
        table.putcol('FLAG', flags)
        flag_rows = clear & (window == 0) & (slot == 1)
        table.putcol('FLAG_ROW', flag_rows)
    return vis, integrations, obs


def compare_compound_selection(root, native):
    from casars.tasks import TaskExecutionError
    from casatools import quanta
    from grpc import RpcError, StatusCode

    work = root / "compound-selection"
    work.mkdir()
    vis, integrations, obs = make_compound_selection_fixture(work)
    units = quanta()
    bounds = integrations[[1, 3]] + [-1.0, 1.0]
    timerange = '~'.join(units.time(units.quantity(value, 's'), form='ymd', prec=10)[0] for value in bounds)
    base = dict(vis=str(vis), showgui=False, overwrite=True, expformat='txt', xaxis='time', yaxis='real',
                ydatacolumn='data', field='FLUX', spw='0:1~5^2,1:1~5^2', antenna='A0&A1;A0&A2',
                timerange=timerange, scan='2', correlation='XX', intent='OBSERVE_TARGET*', feed='1',
                array='1', observation=str(obs), uvrange='>29.9m', msselect='ANTENNA2 = 2')
    before = snapshot(vis)
    membership = [(integrations[1], window, channel) for window in (0, 1) for channel in (1, 3, 5)]
    membership += [(integrations[2], 1, channel) for channel in (1, 5)]
    expected_times, expected_spws, expected_channels = zip(*membership, strict=True)
    for name, selectors in (("correlation", {}), ("polarization", dict(correlation="", polarization="XX"))):
        request = base | selectors
        expected, actual = (work / f"compound-{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**request, plotfile=str(expected)) is None
        assert native(**request, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 8
        for path in (expected, actual):
            points = rows(path)
            assert [int(point[2]) for point in points] == list(expected_channels)
            assert [int(point[11]) for point in points] == list(expected_spws)
            assert {tuple(point[3:7]) for point in points} == {("2", "0", "0", "2")}
            assert {(point[12], point[13]) for point in points} == {("XX", str(obs))}
            np.testing.assert_allclose([float(point[0]) for point in points], expected_times, rtol=0, atol=0.00051)
            np.testing.assert_allclose([float(point[9]) for point in points], expected_times, rtol=0, atol=0.00051)
        print(f"compound-{name}: 8 ordered visibility points and absolute MJD matched", flush=True)

    conflict = work / "compound-conflicting-selectors.txt"
    for task in (reference, native):
        try:
            task(**base, polarization="XX", plotfile=str(conflict))
        except RuntimeError as error:
            assert str(error) == "Cannot select both correlation and polarization."
        else:
            raise AssertionError("PlotMS accepted both correlation and polarization")
        assert not conflict.exists()

    empty = base | dict(msselect="ANTENNA2 = 1 AND ANTENNA2 = 2")
    casa_output = work / "compound-empty.casa.txt"
    native_output = work / "compound-empty.native.txt"
    try:
        reference(**empty, plotfile=str(casa_output))
    except RpcError as error:
        assert error.code() == StatusCode.INVALID_ARGUMENT and error.details() == "plot parameters failed"
    else:
        raise AssertionError("CASA accepted the empty selector intersection")
    try:
        native(**empty, plotfile=str(native_output))
    except TaskExecutionError as error:
        assert "no unflagged visibility points for the current selection" in str(error)
    else:
        raise AssertionError("casa-rs accepted the empty selector intersection")
    assert not casa_output.exists() and not native_output.exists()
    verify_source(vis, before)
    print("compound selection: both refusal controls matched and input arrays are unchanged", flush=True)


def compare_averaged_row_axes(root, native, base):
    from plotms_ms_spw_average_oracle import make_fixture as make_spw_fixture

    work = root / "averaged-row-axes"
    work.mkdir()
    multi = Path(base["vis"])
    with opened(multi) as table:
        selected = table.getcol("FIELD_ID") == 0
        times = np.unique(table.getcol("TIME")[selected])
    simple = make_spw_fixture(work, integrations=5, channel_count=8)
    with opened(simple, True) as table:
        epochs, slots = np.unique(table.getcol("TIME"), return_inverse=True)
        data = table.getcol("DATA")
        data.real += 10 * slots
        table.putcol("DATA", data)
    before = {vis: snapshot(vis) for vis in (multi, simple)}
    average = dict(avgtime="20")
    cases = (
        ("chunk-local-row-ids", base | average,
         np.repeat([0, 1, 2, 3, 0, 1, 0, 1, 0, 1, 2, 3], 6),
         np.repeat(times[[0, 0, 2, 2, 1, 1, 4, 4, 3, 3, 5, 5]], 6), None),
        ("merged-row-ids", base | average | dict(vis=str(simple), antenna="A0&A1", correlation="XX"),
         np.repeat([0, 1, 2], 3), np.repeat(epochs[[0, 2, 4]] + [5, 5, 0], 3),
         [7, 9, 11, 27, 29, 31, 42, 44, 46]),
    )
    for name, controls, identifiers, timestamps, visibility in cases:
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(identifiers)
        for path in (expected, actual):
            points = rows(path)
            np.testing.assert_array_equal([int(point[0]) for point in points], identifiers)
            np.testing.assert_allclose([float(point[9]) for point in points], timestamps, rtol=0, atol=0.00051)
            if visibility is not None:
                np.testing.assert_allclose([float(point[1]) for point in points], visibility, rtol=1.2e-5)
        print(f"{name}: {len(identifiers)} matching time-averaged visibility samples and Row IDs", flush=True)
    for vis, previous in before.items():
        verify_source(vis, previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("compound-selection", "raw-row-axes", "averaged-row-axes", "averaged-coordinates"))
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
        if args.only is None:
            compare(root, native)
            compare_pointing_axes(root, native)
        if args.only in (None, "averaged-coordinates"):
            compare_averaged_coordinates(root, native)
        if args.only in ("raw-row-axes", "averaged-row-axes"):
            feed = make_feed_selection_fixture(root)
            vis, _, _ = make_row_axes_fixture(root, feed)
            if args.only == "raw-row-axes":
                compare_unaveraged_row_axes(root, native, row_axis_request(vis))
            else:
                compare_averaged_row_axes(root, native, row_axis_request(vis))
        elif args.only in (None, "compound-selection"):
            compare_compound_selection(root, native)


if __name__ == "__main__":
    main()
