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
from plotms_pointing_controls_oracle import (
    compare_pointing_averaging,
    compare_pointing_lists,
    compare_pointing_partners,
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
    check_interval_frame_refusal(root, native, fixture_data[0])
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
    mixed = base | dict(xaxis="real", xdatacolumn="data", yaxis=["interval", "u"],
                        ydatacolumn=["data", "data"], yaxislocation=["left", "right"], avgtime="20")
    expected, actual = (work / f"mixed-time-geometry.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**mixed, plotfile=str(expected)) is None
    points = rows(expected)
    interval_points = rows(work / "averaged-interval.casa.txt")
    wavelengths = rows(work / "averaged-uwave.casa.txt")
    assert [row[2:] for row in points] == [row[2:] for row in interval_points + wavelengths]
    expected_ordinates = [float(row[0]) for row in interval_points] + [
        float(row[0]) / frequencies[int(row[2])] * SPEED_OF_LIGHT for row in wavelengths
    ]
    np.testing.assert_allclose([float(row[1]) for row in points], expected_ordinates, rtol=1.2e-5)
    assert native(**mixed, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 20
    print("mixed-time-geometry: 20 matching paired Interval and U ordinates", flush=True)
    for axis, label in (("interval", "Interval (s)"), ("uwave", "Uwave (λ)")):
        compare_figure(work, native, base | dict(xaxis=axis, avgtime="20"),
                       work / f"averaged-{axis}.casa.txt", (title, label, "Amp"))
    verify_source(vis, before)
    with opened(vis) as table:
        np.testing.assert_array_equal(table.getcol("UVW"), uvw)
        np.testing.assert_array_equal(table.getcol("INTERVAL"), intervals)
        np.testing.assert_array_equal(table.getcol("TIME"), timestamps)
    compare_wavelength_frame_selection(work, native)


def compare_wavelength_frame_selection(work, native):
    from plotms_ms_spw_average_oracle import make_fixture as make_spw_fixture

    topo = make_spw_fixture(work)
    with opened(topo, True) as table:
        uvw = np.zeros_like(table.getcol("UVW"))
        uvw[0, :] = 10
        table.putcol("UVW", uvw)
    mixed = work / "mixed-native-frames.ms"
    copy_ms(topo, mixed)
    with opened(mixed / "SPECTRAL_WINDOW", True) as table:
        np.testing.assert_array_equal(table.getcol("MEAS_FREQ_REF"), [5, 5])  # TOPO
        table.putcell("MEAS_FREQ_REF", 1, 1)  # LSRK
    selection = controls(topo) | dict(xaxis="uwave", spw="0:1,1:1", avgtime="20", expformat="txt")
    expected, actual = (work / f"same-wavelength-frame.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**selection, plotfile=str(expected)) is None
    assert native(**selection, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2

    selection["vis"] = str(mixed)
    expected, actual = (work / f"mixed-wavelength-frame.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**selection, plotfile=str(expected)) is None
    points = rows(expected)
    assert [row[11] for row in points] == ["0", "1"]
    # CASA keeps the first window TOPO and converts the second window from LSRK.
    np.testing.assert_allclose([float(row[10]) for row in points], [90.001, 100.003287289], rtol=0, atol=1e-6)
    np.testing.assert_allclose([float(row[0]) for row in points],
                               [10 * float(row[10]) * 1e9 / SPEED_OF_LIGHT for row in points], rtol=1.2e-5)
    assert native(**selection, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2
    np.testing.assert_allclose([float(point[10]) for point in rows(actual)],
                               [float(point[10]) for point in points], rtol=0, atol=1e-7)
    print("wavelength-frames: two paired same-frame and two mixed-frame points", flush=True)

    with opened(mixed, True) as table:
        second_window = table.getcol("DATA_DESC_ID") == 1
        for column in ("TIME", "TIME_CENTROID"):
            times = table.getcol(column)
            times[second_window] -= 120
            table.putcol(column, times)
    expected, actual = (work / f"mixed-earlier-lsrk.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**selection, plotfile=str(expected)) is None
    points = rows(expected)
    assert [p[11] for p in points] == ["0", "1"] and float(points[0][9]) > float(points[1][9])
    assert points[0][10] == "90.001000000"
    assert native(**selection, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2
    print("mixed-earlier-lsrk: first iterator window still supplies the reporting frame", flush=True)


def make_horizontal_fixture(work):
    vis, *_ = make_fixture(work, reverse_times=True)
    with opened(vis / "ANTENNA", True) as table:
        position = table.getcell("POSITION", 1)
        cosine, sine = np.cos(0.08), np.sin(0.08)
        position[:2] = np.array([[cosine, -sine], [sine, cosine]]) @ position[:2]
        table.putcell("POSITION", 1, position)
        positions = table.getcol("POSITION")
    with opened(vis, True) as table:
        selected = ((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
                    & (table.getcol("ANTENNA2") == 1))
        rows_in_baseline = np.flatnonzero(selected)
        times = table.getcol("TIME")
        first = rows_in_baseline[np.argmin(times[rows_in_baseline])]
        flags = table.getcol("FLAG")
        # The same channel is still clear on the second selected baseline.
        flags[0, 1, first] = True
        table.putcol("FLAG", flags)
    return vis, positions


def compare_array_horizontal_axes(root, native):
    from casars.tasks import TaskParameterError
    from plotms_ms_iteration_axes_oracle import compare_case

    work = root / "array-horizontal-axes"
    work.mkdir()
    vis, positions = make_horizontal_fixture(work)
    before = snapshot(vis)
    title = "Array horizontal coordinates"
    base = controls(vis) | dict(xaxis="channel", antenna="A0&A1;A0&A2", field="",
                                spw="0:1~3", title=title, expformat="txt")
    cases = (
        ("azimuth", [-171.14955, 122.20082], None),
        ("elevation", [82.912, 75.66891], "Elevation (deg)"),
        ("hourangle", [0.0836523, -0.932087], "Hour Angle (hours)"),
        ("parangle", [9.40932, 295.949], None),
    )
    for axis, first, label in cases:
        options = base | dict(yaxis=axis)
        expected, actual = (work / f"channel-{axis}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        points = rows(expected)
        assert [int(row[2]) for row in points] == [1, 3] * 12
        assert {tuple(row[5:7]) for row in points} == {("0", "1")}
        assert {row[12] for row in points} == {"XX"}
        assert all(left[9] == right[9] for left, right in zip(points[::2], points[1::2], strict=True))
        np.testing.assert_allclose([float(points[0][1]), float(points[12][1])], first, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(points)
        print(f"channel-{axis}: {len(points)} matching horizontal coordinates", flush=True)
        if label:
            compare_figure(work, native, options, expected, (title, "Channel", label))

    equatorial = work / "equatorial-first.ms"
    copy_ms(vis, equatorial)
    with opened(equatorial / "ANTENNA", True) as table:
        table.putcell("MOUNT", 0, "equatorial")
    options = base | dict(vis=str(equatorial), yaxis="parangle")
    expected, actual = (work / f"first-mount-parangle.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    assert [float(row[1]) for row in rows(expected)] == [0.0] * 24
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 24
    print("first-mount-parangle: 24 matching equatorial array angles", flush=True)

    bwg = work / "beam-waveguide-first.ms"
    copy_ms(vis, bwg)
    with opened(bwg / "ANTENNA", True) as table:
        centre = 6_370_000 / np.sqrt(2) * np.array([1.0, 0.0, -1.0])
        offsets = table.getcol("POSITION")
        offsets -= offsets.mean(axis=1, keepdims=True)
        table.putcol("POSITION", centre[:, None] + 0.0001 * offsets)
        table.putcell("MOUNT", 0, "ALT-AZ+BWG-R")
    with opened(bwg / "FIELD", True) as table:
        # At the first selected integration, the southern field has apparent
        # declination -80 degrees and an hour angle close to +179 degrees.
        j2000 = np.array([[-1.9453210515279018], [-1.3956305669400868]])
        for column in ("DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"):
            table.putcell(column, 0, j2000)
    options = base | dict(vis=str(bwg), field="FLUX", yaxis="parangle")
    expected, actual = (work / f"beam-waveguide-parangle.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    coordinates = [float(row[1]) for row in rows(expected)]
    assert len(coordinates) == 12 and min(coordinates) > 360
    np.testing.assert_allclose(coordinates[0], 393.926, rtol=1.2e-5)
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    print("beam-waveguide-parangle: 12 matching array angles above 360 degrees", flush=True)

    name = "array-correlation-iteration"
    options = base | dict(yaxis="elevation", iteraxis="corr", correlation="XX,YY",
                          gridcols=2, exprange="all")
    xx, yy = compare_case(work, native, name, options, "Corr", ("XX", "YY"), [24, 36])
    assert {(point[2], point[12]) for point in xx} == {("1", "XX"), ("3", "XX")}
    assert {(point[2], point[12]) for point in yy} == {("1", "YY"), ("2", "YY"), ("3", "YY")}

    options = base | dict(yaxis="elevation", iteraxis="antenna")
    left, first, second = compare_case(work, native, "array-antenna-iteration", options, "Antenna",
                                      ("A0@P0", "A1@P1", "A2@P2"), [47, 23, 24])
    assert {tuple(point[5:7]) for point in left} == {("0", "1"), ("0", "2")}
    assert {tuple(point[5:7]) for point in first} == {("0", "1")}
    assert {tuple(point[5:7]) for point in second} == {("0", "2")}

    compare_metadata_flag_modes(work, native, base | dict(yaxis="elevation"), "array-symbols",
                                (24, 36, 12), ("Marked metadata", "Channel", "Elevation (deg)"))

    poln = base | dict(xaxis="poln", yaxis="amp", correlation="XX,YY")
    expected, actual = (work / f"poln-amplitude.{engine}.txt" for engine in ("casa", "native"))
    try:
        reference(**poln, plotfile=str(expected))
    except Exception as error:
        assert callable(getattr(error, "details", None)) and error.details() == "plot parameters failed", error
    else:
        raise AssertionError("CASA accepted a Poln axis on this MeasurementSet")
    try:
        native(**poln, plotfile=str(actual))
    except TaskParameterError as error:
        assert 'unsupported msexplore axis "poln"' in str(error)
    else:
        raise AssertionError("casa-rs accepted a Poln axis on this MeasurementSet")
    assert not expected.exists() and not actual.exists()
    verify_source(vis, before)
    with opened(vis / "ANTENNA") as table:
        np.testing.assert_array_equal(table.getcol("POSITION"), positions)
    print("poln-amplitude: both public tasks refuse the MeasurementSet axis before export", flush=True)


def compare_antenna_horizontal_axes(root, native):
    work = root / "antenna-horizontal-axes"
    work.mkdir()
    vis, positions = make_horizontal_fixture(work)
    with opened(vis / "ANTENNA", True) as table:
        table.putcell("MOUNT", 3, "ALT-AZ+BWG-R")
    with opened(vis / "FEED", True) as table:
        angles = table.getcol("RECEPTOR_ANGLE")
        antennas = table.getcol("ANTENNA_ID")
        feeds = table.getcol("FEED_ID")
        angles[0, (antennas == 1) & (feeds == 0)] += 0.075
        table.putcol("RECEPTOR_ANGLE", angles)
    before = snapshot(vis)
    title = "Antenna horizontal coordinates"
    base = controls(vis) | dict(xaxis="channel", antenna="A0&A1;A0&A2", field="",
                                spw="0:1~3", title=title, expformat="txt")
    cases = (
        ("ant-azimuth", "XX", [-177.59806, 120.90605], None),
        ("ant-elevation", "XX", [82.98457, 74.9439], "Ant-Elevation (deg)"),
        ("ant-parang", "XX", [2.55258, -65.7585], None),
        ("ant-parallacticangle", "XX,YY", [2.55258, -65.7585], None),
    )
    for axis, correlation, anchors, label in cases:
        options = base | dict(yaxis=axis, correlation=correlation)
        name = f"channel-{axis}-{correlation.replace(',', '-')}"
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert [int(point[2]) for point in selected] == [1, 2, 3] * 60
        for start in range(0, len(selected), 15):
            buffer = selected[start:start + 15]
            assert len({point[9] for point in buffer}) == 1
            assert [point[6] for point in buffer[::3]] == ["1", "2", "1", "2", "1"]
            pattern = (["XX", "YY"] * 8)[:15] if correlation == "XX,YY" else ["XX"] * 15
            assert [point[12] for point in buffer] == pattern
        np.testing.assert_allclose([float(selected[0][1]), float(selected[90][1])],
                                   anchors, rtol=1.2e-5)
        if axis.startswith("ant-par"):
            np.testing.assert_allclose([float(selected[3][1]), float(selected[93][1])],
                                       [37.5998, -50.8959], rtol=1.2e-5)
            assert float(selected[9][1]) > 180.0
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 180
        print(f"{name}: 180 matching antenna horizontal coordinates and identities", flush=True)
        if label:
            compare_figure(work, native, options, expected, (title, "Channel", label))
    verify_source(vis, before)
    with opened(vis / "ANTENNA") as table:
        np.testing.assert_array_equal(table.getcol("POSITION"), positions)
        assert table.getcell("MOUNT", 3) == "ALT-AZ+BWG-R"
    with opened(vis / "FEED") as table:
        np.testing.assert_array_equal(table.getcol("RECEPTOR_ANGLE"), angles)


def compare_same_epoch_antenna_geometry(root, native):
    work = root / "antenna-horizontal-same-epoch"
    work.mkdir()
    source, _ = make_horizontal_fixture(work)
    vis = work / "same-epoch.ms"
    with opened(source) as table:
        fields = table.getcol("FIELD_ID")
        times = table.getcol("TIME")
        first, second = [min(times[fields == field]) for field in (0, 1)]
        keep = ((fields == 0) & (times == first)) | ((fields == 1) & (times == second))
        selected = table.selectrows(np.flatnonzero(keep).tolist())
        try:
            selected.copy(str(vis), deep=True, valuecopy=True).close()
        finally:
            selected.close()
    with opened(vis, True) as table:
        for column in ("TIME", "TIME_CENTROID"):
            table.putcol(column, np.full(table.nrows(), first))
    before = snapshot(vis)
    options = controls(vis) | dict(xaxis="channel", yaxis="ant-elevation", field="",
                                   antenna="A0&A1;A0&A2", spw="0:1~3", expformat="txt")
    expected, actual = (work / f"together.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    assert native(**options, plotfile=str(actual)) is None
    joint_casa, joint_native = rows(expected), rows(actual)
    assert len(joint_casa) == len(joint_native) == 30
    assert {point[9] for point in joint_native} == {f"{first:.3f}"}
    isolated = []
    cached = []
    for field in ("0", "1"):
        separate = work / f"field-{field}.casa.txt"
        assert reference(**(options | dict(field=field)), plotfile=str(separate)) is None
        selected = rows(separate)
        native_field = [point for point in joint_native if point[4] == field]
        assert len(native_field) == len(selected) == 15
        for actual_point, expected_point in zip(native_field, selected, strict=True):
            assert (actual_point[0], actual_point[2:]) == (expected_point[0], expected_point[2:])
        isolated.append([float(point[1]) for point in selected])
        cached.append([float(point[1]) for point in joint_casa if point[4] == field])
        np.testing.assert_allclose([float(point[1]) for point in native_field], isolated[-1],
                                   rtol=1.2e-5, atol=1e-7)
    np.testing.assert_allclose([values[0] for values in isolated], [82.98457, 75.14170],
                               rtol=1.2e-5)
    np.testing.assert_array_equal(cached, [isolated[0], isolated[0]])
    verify_source(vis, before)
    print("equal-time-fields: 30 native points agree with the separate CASA selections; "
          "CASA reuses the first field's coordinates in a joint selection", flush=True)


def casa_ephemeris_export(app_run, options, *, expect_refusal=False):
    import json
    import subprocess
    import sys

    # A pinned CASA PlotMS session can retain stale ephemeris state across
    # exports. Run each reference invocation in a fresh session.
    command = """
import json
from pathlib import Path
import sys
from grpc import RpcError, StatusCode
sys.path.insert(0, sys.argv[1])
from plotms_calibration_oracle import plotms_launcher, reference
with plotms_launcher(Path(sys.argv[2]) if sys.argv[2] else None):
    try:
        assert reference(**json.loads(sys.argv[3])) is None
    except RpcError as error:
        assert sys.argv[4] == "refuse" and error.code() == StatusCode.INVALID_ARGUMENT
    else:
        assert sys.argv[4] == "export"
"""
    subprocess.run([sys.executable, "-c", command, str(Path(__file__).resolve().parent),
                    str(app_run.resolve()) if app_run else "", json.dumps(options),
                    "refuse" if expect_refusal else "export"], check=True)


def add_ephemeris_field_polynomial(vis):
    with opened(vis / "FIELD", True) as table:
        for column in ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"):
            direction = table.getcell(column, 0)
            table.putcell(column, 0, np.column_stack((direction[:, 0], [1e-10, -2e-10])))
        table.putcell("NUM_POLY", 0, 1)


def flag_ephemeris_samples(vis):
    with opened(vis, True) as table:
        times = table.getcol("TIME")
        antenna2 = table.getcol("ANTENNA2")
        selected = ((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
                    & np.isin(antenna2, [1, 2]))
        ordered = np.unique(times[selected])
        assert len(ordered) == 6
        flags = table.getcol("FLAG")
        flags[:, :, selected & (times == ordered[0])] = True
        flags[:, 2, selected & (times == ordered[1])] = True
        third = selected & (times == ordered[2])
        flags[:, 1, third] = True
        flags[1, 1, third & (antenna2 == 2)] = False
        table.putcol("FLAG", flags)
    return ordered[1:]


def compare_ephemeris_axes(root, native, app_run):
    from casars.tasks import TaskExecutionError, TaskParameterError
    from gaincal_oracle import ephemeris_observation

    def fresh_reference(**options):
        return casa_ephemeris_export(app_run, options)

    work = root / "ephemeris-axes"
    work.mkdir()
    source, *_ = make_fixture(work)
    title = "Uranus ephemeris coordinates"
    cases = (
        ("channel-radial-velocity", "channel", "Radial Velocity", 18,
         [27.4322, 27.4352], "Channel", "Radial Velocity (km/s)"),
        ("time-distance", "time", "Distance (rho)", 6,
         [2.90480e9, 2.90483e9], "Time (MJD seconds)", "Distance (rho) (km)"),
    )
    variants = [(case, mode) for mode in ("plain", "masked") for case in cases]
    variants.append((cases[0], "polynomial"))
    for (name, x, y, count, anchors, xlabel, ylabel), mode in variants:
        if mode != "plain":
            name += f"-{mode}"
        masked = mode == "masked"
        if masked:
            count = 14 if x == "channel" else 5
        case = work / name
        case.mkdir()
        vis, _ = ephemeris_observation(case, source, offset=(0.002, -0.004))
        if mode == "polynomial":
            add_ephemeris_field_polynomial(vis)
        ordered = flag_ephemeris_samples(vis) if masked else None
        before = snapshot(vis)
        options = controls(vis) | dict(
            xaxis=x, yaxis=y, antenna="A0&A1;A0&A2", spw="0:1~3", title=title, expformat="txt",
        )
        if masked:
            options["correlation"] = "XX,YY"
        expected, actual = (case / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert fresh_reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == count
        assert len({point[9] for point in selected}) == (5 if masked else 6)
        assert {point[6] for point in selected} == {"1"}
        channels = [int(point[2]) for point in selected]
        if masked:
            assert {point[9] for point in selected} == {f"{time:.3f}" for time in ordered}
            assert channels == ([1, 3] + [1, 2, 3] * 4 if x == "channel" else [1] * 5)
            if x == "channel":
                assert (channels[2:5], [point[12] for point in selected[2:5]]) == (
                    [1, 2, 3], ["XX", "YY", "XX"],
                )
        else:
            assert {point[12] for point in selected} == {"XX"}
            assert channels == ([1, 2, 3] if x == "channel" else [1]) * 6
            np.testing.assert_allclose([float(selected[0][1]), float(selected[-1][1])],
                                       anchors, rtol=1.2e-5)
        assert float(selected[0][1]) < float(selected[-1][1])
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        actual_values = np.asarray([float(point[1]) for point in rows(actual)])
        expected_values = np.asarray([float(point[1]) for point in selected])
        np.testing.assert_array_equal(actual_values, expected_values)
        print(f"{name}: {count} matching ephemeris coordinates and metadata", flush=True)
        if mode != "polynomial":
            compare_figure(case, native, options, expected, (title, xlabel, ylabel),
                           reference_task=fresh_reference,
                           ordinate_atol=5.001e-5 if x == "channel" else 5000.001)
        else:
            with opened(vis / "FIELD") as table:
                assert table.getcell("NUM_POLY", 0) == 1
                np.testing.assert_array_equal(table.getcell("PHASE_DIR", 0)[:, 1], [1e-10, -2e-10])
        verify_source(vis, before)

    fixed = controls(source) | dict(xaxis="channel", yaxis="Radial Velocity", expformat="txt")
    expected, actual = (work / f"fixed-field.{engine}.txt" for engine in ("casa", "native"))
    casa_ephemeris_export(app_run, fixed | dict(plotfile=str(expected)), expect_refusal=True)
    try:
        native(**fixed, plotfile=str(actual))
    except (TaskExecutionError, TaskParameterError) as error:
        assert "ephemeris data is not available" in str(error)
    else:
        raise AssertionError("Native exported a moving ephemeris axis for a fixed MS")
    assert not expected.exists() and not actual.exists()
    print("fixed-field: both tasks require ephemeris data before export", flush=True)


def compare_static_metadata_coordinates(root, native):
    from plotms_ms_columns_oracle import compare_empty_export
    from plotms_ms_iterated_ordinates_oracle import compare_figures
    from plotms_ms_reporting_frames_oracle import compare_channel_frequency_transform

    work = root / "static-metadata-coordinates"
    work.mkdir()
    vis, _ = make_horizontal_fixture(work)
    with opened(vis, True) as table:
        fields = table.getcol("FIELD_ID")
        antenna1, antenna2 = [table.getcol(column) for column in ("ANTENNA1", "ANTENNA2")]
        times = table.getcol("TIME")
        selected = (fields == 0) & (antenna1 == 0) & np.isin(antenna2, [1, 2])
        ordered = np.unique(times[selected])
        assert len(ordered) == 6
        scans = table.getcol("SCAN_NUMBER")
        scans[(fields == 0) & (times <= ordered[3])] = 11
        scans[(fields == 0) & (times > ordered[3])] = 19
        flags = table.getcol("FLAG")
        first = selected & (times == ordered[0])
        flags[:, :, first] = True
        flags[:, 0, first] = False  # This clear channel is outside the selection.
        second = selected & (times == ordered[1])
        flags[:, 2, second] = True
        flags[:, 1, second & (antenna2 == 1)] = True
        flags[:, 1, second & (antenna2 == 2)] = False
        table.putcol("FLAG", flags)
        table.putcol("SCAN_NUMBER", scans)
    before = snapshot(vis)
    base = controls(vis) | dict(antenna="A0&A1;A0&A2", spw="0:1~3", correlation="XX,YY",
                               expformat="txt", title="Metadata coordinate masks")
    cases = (
        ("channel-frequency", "channel", "frequency", 14, (8, 6), "Channel", "Frequency (GHz)"),
        ("elevation-time", "elevation", "time", 5, (3, 2), "Elevation (deg)", "Time (MJD seconds)"),
    )
    empty = work / "all-flagged.ms"
    copy_ms(vis, empty)
    with opened(empty, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
    for name, x, y, count, counts, xlabel, ylabel in cases:
        options = base | dict(xaxis=x, yaxis=[y])
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == count
        assert {point[6] for point in selected} == {"1"}
        assert [point[9] for point in selected] == sorted(point[9] for point in selected)
        assert {point[9] for point in selected} == {f"{time:.3f}" for time in ordered[1:]}
        assert [point[3] for point in selected] == ["11"] * counts[0] + ["19"] * counts[1]
        if x == "channel":
            assert [int(point[0]) for point in selected] == [1, 3] + [1, 2, 3] * 4
            assert all(point[1] == point[10] for point in selected)
        else:
            assert {point[2] for point in selected} == {"1"}
            assert all(point[1] == point[9] for point in selected)
            np.testing.assert_allclose(float(selected[0][0]), 82.90598, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        assert [point[1] for point in rows(actual)] == [point[1] for point in selected]
        print(f"{name}: {count} matching ordered metadata coordinates and masks", flush=True)

        iteration = options | dict(iteraxis="scan", exprange="all", xlabel=xlabel)
        side = "right" if y == "frequency" else "left"
        iteration["yaxislocation"] = [side]
        expected, actual = (work / f"{name}-scans.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**(iteration | dict(gridrows=2)), plotfile=str(expected)) is None
        assert rows(expected) == selected
        assert native(**(iteration | dict(gridrows=2)), plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        assert [point[1] for point in rows(actual)] == [point[1] for point in selected]
        values = np.array([point[:2] for point in selected], dtype=float)
        pages = [[(scan, coordinates)] for scan, coordinates in zip(
            ("11", "19"), np.split(values, [counts[0]]), strict=True,
        )]
        compare_figures(work, native, f"{name}-pages", iteration, pages,
                        ["Scan11", "Scan19_2"], [(side, ylabel, [0])],
                        ordinate_atol=0.00051 if y == "time" else 5.001e-10)
        empty_options = dict(options, vis=str(empty))
        del empty_options["expformat"]
        compare_empty_export(work, native, f"{name}-all-flagged", empty_options, (xlabel, ylabel))
    compare_channel_frequency_transform(work, native, base, rows(work / "channel-frequency.casa.txt"))
    verify_source(vis, before)


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


def check_interval_frame_refusal(root, native, vis):
    from casars.tasks import TaskParameterError

    output = root / "interval-frame.native.txt"
    options = controls(vis) | dict(xaxis="interval", expformat="txt", transform=True, freqframe="BARY")
    try:
        native(**options, plotfile=str(output))
    except TaskParameterError as error:
        assert "reporting-frame transforms for interval and wavelength plots" in str(error)
        assert "#1032" in str(error)
    else:
        raise AssertionError("Unsupported Interval reporting frame was exported")
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
    compare_metadata_flag_modes(work, native, base | dict(xaxis="channel", yaxis="row"),
                                "row-symbols", (32, 36, 4), ("Marked metadata", "Channel", "Row"))


def compare_metadata_flag_modes(work, native, base, name, counts, labels):
    from plotms_ms_columns_oracle import compare_empty_export, compare_flagged_figure

    vis = Path(base["vis"])
    before = snapshot(vis)
    base = base | dict(title=labels[0], expformat="txt")
    marker_color = "dd2255"
    marked = base | dict(customflaggedsymbol=True, flaggedsymbolshape="diamond",
                         flaggedsymbolcolor=marker_color, flaggedsymbolsize=4,
                         flaggedsymbolfill="fill", flaggedsymboloutline=False)
    modes = {
        "clear": base,
        "both": marked,
        "flagged": marked | dict(customsymbol=True, symbolshape="nosymbol"),
    }
    selected = {}
    for (mode, options), count in zip(modes.items(), counts, strict=True):
        expected, actual = (work / f"{name}-{mode}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected[mode] = {tuple(point) for point in rows(expected)}
        assert len(selected[mode]) == count
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        print(f"{name}-{mode}: {count} matching selected metadata coordinates", flush=True)
    assert selected["clear"].isdisjoint(selected["flagged"])
    assert selected["clear"] | selected["flagged"] == selected["both"]
    compare_flagged_figure(work, native, f"{name}-both", marked, marker_color, labels=labels,
                           is_flagged=lambda point: tuple(point) in selected["flagged"])
    empty = work / f"{name}-all-flagged.ms"
    copy_ms(vis, empty)
    with opened(empty, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
    empty_options = dict(base, vis=str(empty))
    del empty_options["expformat"]
    compare_empty_export(work, native, f"{name}-all-flagged", empty_options, labels[1:])
    verify_source(vis, before)


def compare_selected_metadata_flag_symbols(root, native):
    identifiers = root / "flagged-row-identifiers"
    identifiers.mkdir()
    feed = make_feed_selection_fixture(identifiers)
    source, _, _ = make_row_axes_fixture(identifiers, feed)
    compare_metadata_pairs(identifiers, native, source)

    horizontal = root / "flagged-array-coordinates"
    horizontal.mkdir()
    vis, _ = make_horizontal_fixture(horizontal)
    base = controls(vis) | dict(xaxis="channel", yaxis="elevation", antenna="A0&A1;A0&A2",
                               field="", spw="0:1~3", correlation="XX")
    compare_metadata_flag_modes(horizontal, native, base, "array-symbols",
                                (24, 36, 12), ("Marked metadata", "Channel", "Elevation (deg)"))


def make_pointing_fixture(work):
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
    return vis


def compare_pointing_axes(root, native):
    work = root / "pointing-axes"
    vis = make_pointing_fixture(work)
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
    parser.add_argument("--only", choices=("compound-selection", "raw-row-axes", "averaged-row-axes",
                                           "averaged-coordinates", "array-horizontal-axes",
                                           "antenna-horizontal-axes", "antenna-horizontal-same-epoch",
                                           "ephemeris-axes", "static-metadata-coordinates",
                                           "metadata-flag-symbols", "pointing", "pointing-lists",
                                           "pointing-partners", "pointing-averaging", "reporting-frames",
                                           "reporting-wavelengths", "averaged-reporting-epochs"))
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
        if args.only in (None, "pointing"):
            compare_pointing_axes(root, native)
        for name, compare_pointing in (
            ("pointing-lists", compare_pointing_lists),
            ("pointing-partners", compare_pointing_partners),
            ("pointing-averaging", compare_pointing_averaging),
        ):
            if args.only in (None, name):
                work = root / name
                compare_pointing(work, native, make_pointing_fixture(work))
        if args.only in (None, "averaged-coordinates"):
            compare_averaged_coordinates(root, native)
        if args.only in (None, "array-horizontal-axes"):
            compare_array_horizontal_axes(root, native)
        if args.only in (None, "antenna-horizontal-axes"):
            compare_antenna_horizontal_axes(root, native)
        if args.only in (None, "antenna-horizontal-same-epoch"):
            compare_same_epoch_antenna_geometry(root, native)
        if args.only in (None, "ephemeris-axes"):
            compare_ephemeris_axes(root, native, args.app_run)
        if args.only in (None, "static-metadata-coordinates"):
            compare_static_metadata_coordinates(root, native)
        if args.only in (None, "reporting-frames"):
            from plotms_ms_reporting_frames_oracle import compare_reporting_frames

            compare_reporting_frames(root / "reporting-frames", native)
        if args.only in (None, "averaged-reporting-epochs"):
            from plotms_ms_reporting_frames_oracle import compare_averaged_reporting_epochs

            compare_averaged_reporting_epochs(root / "averaged-reporting-epochs", native)
        if args.only in (None, "reporting-wavelengths"):
            from plotms_ms_reporting_frames_oracle import compare_wavelength_reporting_frames

            compare_wavelength_reporting_frames(root / "reporting-wavelengths", native)
        if args.only == "reporting-wavelengths":
            work = root / "average-wavelength-frames"
            work.mkdir()
            compare_wavelength_frame_selection(work, native)
        if args.only == "metadata-flag-symbols":
            compare_selected_metadata_flag_symbols(root, native)
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
