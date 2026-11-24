#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare ordinary SPW averages through the public CASA and casa-rs PlotMS tasks."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
from casatools import measures, simulator
import numpy as np

from plotms_ms_columns_oracle import (
    compare_empty_export,
    compare_figure,
    compare_text,
    copy_ms,
    opened,
    plotms_launcher,
    reference,
    request,
    rows,
)


def make_fixture(root, name="two-windows", *, antenna_count=2, channel_count=4):
    vis = root / f"{name}.ms"
    ms, me = simulator(), measures()
    ms.open(str(vis))
    ms.setconfig(telescopename="VLA", x=[0.0, 100.0, 170.0][:antenna_count],
                 y=[0.0] * antenna_count, z=[0.0] * antenna_count,
                 dishdiameter=[25.0] * antenna_count, mount=["alt-az"] * antenna_count,
                 antname=[f"A{i}" for i in range(antenna_count)],
                 padname=[f"P{i}" for i in range(antenna_count)],
                 coordsystem="local", referencelocation=me.observatory("VLA"))
    for name, frequency in (("W0", "90GHz"), ("W1", "100GHz")):
        ms.setspwindow(spwname=name, freq=frequency, deltafreq="1MHz", freqresolution="1MHz",
                       nchannels=channel_count, stokes="XX YY")
    ms.setfeed(mode="perfect X Y")
    ms.setfield(sourcename="FLUX", sourcedirection=me.direction("J2000", "12h00m00s", "40d00m00s"))
    ms.setauto(autocorrwt=0.0)
    ms.settimes(integrationtime="10s", usehourangle=False,
                referencetime=me.epoch("UTC", "2017/03/01/09:00:00"))
    for window in ("W0", "W1"):
        ms.observe("FLUX", window, starttime="0s", stoptime="20s")
    ms.close()
    casa.clearcal(str(vis), addmodel=True)
    with opened(vis, True) as table:
        descriptions = table.getcol("DATA_DESC_ID")
        shape = table.getcol("DATA").shape
        channels = np.arange(1, shape[1] + 1, dtype=np.float32)[None, :, None]
        for column, low, high in (("DATA", 1 + 2j, 3 - 4j), ("CORRECTED_DATA", 5 + 6j, 7 + 8j)):
            values = channels * np.where(descriptions == 0, low, high)
            table.putcol(column, np.broadcast_to(values.astype(np.complex64), shape).copy())
        for column, value in (("SCAN_NUMBER", 1), ("OBSERVATION_ID", 0), ("STATE_ID", 0)):
            table.putcol(column, np.full_like(descriptions, value))
        table.putcol("FLAG", np.zeros(shape, dtype=bool))
        table.putcol("FLAG_ROW", np.zeros(shape[2], dtype=bool))
        weights = np.where(descriptions == 0, 1, 3).astype(np.float32)
        table.putcol("WEIGHT", np.broadcast_to(weights, table.getcol("WEIGHT").shape).copy())
        table.putcol("SIGMA", np.ones_like(table.getcol("SIGMA")))
    return vis


def export(root, name, task, controls):
    path = root / f"{name}.txt"
    assert task(**controls, plotfile=str(path), expformat="txt") is None
    return path


def compare_coordinates(root, native, base, vis):
    flipped = root / "flipped-windows.ms"
    copy_ms(vis, flipped)
    with opened(flipped, True) as table:
        table.putcol("DATA_DESC_ID", 1 - table.getcol("DATA_DESC_ID"))
    frequency = dict(xaxis="frequency", xdatacolumn="", yaxis="amp")
    cases = (
        ("frequency", frequency, [(1, "90.001000000", 0)] * 2),
        ("flipped", frequency | dict(vis=str(flipped)), [(1, "100.001000000", 1)] * 2),
        ("reordered-selector", frequency | dict(spw="1:1,0:1"), [(1, "90.001000000", 0)] * 2),
        ("channel-bins", frequency | dict(spw="0:0~3,1:0~3", avgchannel="2"),
         [(0, "90.000500000", 0), (1, "90.002500000", 0)] * 2),
        ("scalar", dict(scalar=True), [(1, "90.001000000", 0)] * 2),
        ("vector", {}, [(1, "90.001000000", 0)] * 2),
        ("time", dict(xaxis="time", xdatacolumn=""), [(1, "90.001000000", 0)] * 2),
        ("one-window", frequency | dict(spw="1:1"), [(1, "100.001000000", 1)] * 2),
    )
    for name, overrides, anchors in cases:
        controls = base | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        assert [(int(p[2]), p[10], int(p[11])) for p in selected] == anchors
        if controls["xaxis"] == "frequency":
            assert [p[0] for p in selected] == [p[10] for p in selected]
        actual = export(root, f"{name}.native", native, controls)
        assert compare_text(actual, expected) == len(anchors)
        if controls["xaxis"] == "frequency":
            assert [p[0] for p in rows(actual)] == [p[0] for p in selected]
        print(f"{name}: {len(anchors)} matching SPW-average coordinates", flush=True)
    compare_figure(root, native, base | frequency, root / "frequency.casa.txt",
                   (base["title"], "Frequency (GHz)", "Amp:corrected"))


def compare_sparse_baselines(root, native, base):
    sparse = make_fixture(root, "sparse-baselines", antenna_count=3)
    with opened(sparse, True) as table:
        w0 = table.getcol("DATA_DESC_ID") == 0
        times = table.getcol("TIME") + 100 * (w0 & (table.getcol("ANTENNA2") == 2))
        table.putcol("TIME", times)
        table.putcol("TIME_CENTROID", times)
    controls = base | dict(vis=str(sparse), antenna="", xaxis="frequency", xdatacolumn="",
                           yaxis="amp", spw="0:0~1,1:0~1")
    expected = export(root, "sparse-baselines.casa", reference, controls)
    selected = rows(expected)
    assert len(selected) == 20
    steps = ["90.000000000", "90.001000000"] * 10
    assert [p[0] for p in selected] == [p[10] for p in selected] == steps
    assert {p[11] for p in selected} == {"0"}
    baselines = [("0", "1"), ("0", "2"), ("1", "2")] * 2 + [
        ("0", "2"), ("1", "2")
    ] * 2
    assert [(p[5], p[6]) for p in selected] == [baseline for baseline in baselines for _ in range(2)]
    amplitudes = [abs(6.5 + 7.5j), abs(7 + 8j), abs(7 + 8j)] * 2 + [abs(5 + 6j)] * 4
    np.testing.assert_allclose(coordinates(selected)[:, 1],
                               [value * scale for value in amplitudes for scale in (1, 2)],
                               rtol=1.2e-5)
    actual = export(root, "sparse-baselines.native", native, controls)
    assert compare_text(actual, expected) == 20
    assert [p[0] for p in rows(actual)] == steps
    print("sparse-baselines: 20 points use first-window channels in baseline-major order", flush=True)


def compare_disjoint_channels(root, native, base):
    disjoint = make_fixture(root, "disjoint-channels", channel_count=8)
    controls = base | dict(vis=str(disjoint), xaxis="frequency", xdatacolumn="",
                           yaxis="amp", avgchannel="2")
    cases = (
        ("disjoint-channels", "0:0~2;5~7", ["90.000500000", "90.005500000"],
         [abs(5 + 6j) * scale for scale in (1.5, 4.5)]),
        ("strided-channels", "0:0~7^2,1:0~7^2", ["90.001500000", "90.005500000"],
         [abs(6.5 + 7.5j) * scale for scale in (2, 6)]),
    )
    for name, spw, frequencies, amplitudes in cases:
        options = controls | dict(spw=spw)
        expected = export(root, f"{name}.casa", reference, options)
        selected = rows(expected)
        assert [p[0] for p in selected] == [p[10] for p in selected] == frequencies * 2
        np.testing.assert_allclose(coordinates(selected)[:, 1], amplitudes * 2, rtol=1.2e-5)
        actual = export(root, f"{name}.native", native, options)
        assert compare_text(actual, expected) == 4
        assert [p[0] for p in rows(actual)] == frequencies * 2
        print(f"{name}: 4 paired compressed samples at the physical frequency-bin centers", flush=True)

    # CASA's transform can terminate when both windows have disjoint ranges.
    # The first window already established the physical grid in the paired case.
    multiple = controls | dict(spw="0:0~2;5~7,1:0~2;5~7")
    selected = rows(export(root, "disjoint-windows.native", native, multiple))
    frequencies = ["90.000500000", "90.005500000"] * 2
    assert [p[0] for p in selected] == [p[10] for p in selected] == frequencies
    assert [p[11] for p in selected] == ["0"] * 4
    np.testing.assert_allclose(coordinates(selected)[:, 1],
                               [abs(6.5 + 7.5j) * scale for scale in (1.5, 4.5)] * 2, rtol=1.2e-5)
    print("disjoint-windows: 4 native coordinates match the analytic average and CASA frequency grid", flush=True)


def compare_malformed_shape(root, native, base, vis):
    short = root / "short-channel-cell.ms"
    copy_ms(vis, short)
    with opened(short, True) as table:
        for name in ("DATA", "CORRECTED_DATA", "FLAG"):
            descriptor = table.getcoldesc(name)
            descriptor.pop("shape", None)
            descriptor.update(option=0, dataManagerType="StandardStMan", dataManagerGroup=name)
            values = table.getcol(name)
            table.removecols(name)
            table.addcols({name: descriptor})
            for row in range(table.nrows()):
                cell = values[:, :, row]
                table.putcell(name, row, cell[:, :2] if row == 0 else cell)
    options = base | dict(vis=str(short), spw="0", xaxis="frequency", xdatacolumn="", yaxis="amp")
    output = root / "short-channel-cell.native.txt"
    try:
        native(**options, plotfile=str(output), expformat="txt")
    except RuntimeError as error:
        assert "reported 4 channel frequencies for 2 data channels" in str(error), error
    else:
        raise AssertionError("Data cells must match their spectral-window channel count")
    assert not output.exists()
    print("short-channel-cell: descriptive shape error before export", flush=True)


def compare_flags(root, native, base, vis):
    flagged = root / "flags.ms"
    copy_ms(vis, flagged)
    with opened(flagged, True) as table:
        windows = table.getcol("DATA_DESC_ID")
        times = table.getcol("TIME")
        flags = table.getcol("FLAG")
        flags[0, 1, (windows == 0) | (times > times.min())] = True
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", (windows == 1) & (times == times.min()))
        weights = table.getcol("WEIGHT")
        weights[1] = np.where(windows == 0, 100, 1)
        table.putcol("WEIGHT", weights)
    controls = base | dict(vis=str(flagged), correlation="XX,YY")
    expected = export(root, "flags.casa", reference, controls)
    selected = rows(expected)
    assert [p[12] for p in selected] == ["XX", "YY", "YY"]
    np.testing.assert_allclose(coordinates(selected)[:, 0], [10, abs(5 - 5j), abs(5 - 5j)], rtol=1.2e-5)
    assert compare_text(export(root, "flags.native", native, controls), expected) == 3
    print("flags: 3 matching points with first-correlation row weights and cube flags", flush=True)


def coordinates(points):
    return np.array([point[:2] for point in points], dtype=float)


def compare_channel_weights(root, native, base, vis):
    spectral = root / "channel-spectra.ms"
    copy_ms(vis, spectral)
    with opened(spectral, True) as table:
        descriptions = table.getcol("DATA_DESC_ID")
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for column, low, high in (
            ("WEIGHT_SPECTRUM", [1, 3, 5, 7], [8, 4, 2, 1]),
            ("SIGMA_SPECTRUM", [1, 0.5, 1, 0.5], [0.5, 1, 0.5, 1]),
        ):
            table.addcols({column: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                        dataManagerGroup=column)})
            values = np.where(descriptions == 0, np.asarray(low, dtype=np.float32)[:, None],
                              np.asarray(high, dtype=np.float32)[:, None])
            table.putcol(column, np.broadcast_to(values, shape).copy())
    observed = np.array([[1.8 * (1 + 2j), 3.8 * (1 + 2j)], [1.2 * (3 - 4j), 3.2 * (3 - 4j)]])
    corrected = np.array([[1.75 * (5 + 6j), (43 / 12) * (5 + 6j)],
                          [(4 / 3) * (7 + 8j), (10 / 3) * (7 + 8j)]])
    transform_weights = [8, 7.5]  # Mean transformed WEIGHT across the two bins.
    corrected_mean = np.average(corrected, axis=0, weights=transform_weights)
    mixed_observed_mean = np.average(observed, axis=0, weights=transform_weights)
    options = base | dict(vis=str(spectral), avgchannel="2", spw="0:0~3,1:0~3", xaxis="real", yaxis="imag")
    cases = (
        ("bin-observed", dict(ydatacolumn="data"), observed.mean(axis=0).real, observed.mean(axis=0).imag),
        ("bin-corrected", dict(xdatacolumn="corrected"), corrected_mean.real, corrected_mean.imag),
        ("bin-mixed", {}, mixed_observed_mean.real, corrected_mean.imag),
        ("bin-scalar", dict(scalar=True, xaxis="amp", yaxis="phase"),
         np.average(abs(observed), axis=0, weights=transform_weights),
         np.average(np.angle(corrected, deg=True), axis=0, weights=transform_weights)),
    )
    for name, overrides, x, y in cases:
        controls = options | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        assert [p[2] for p in selected] == ["0", "1"] * 2
        np.testing.assert_allclose(coordinates(selected), np.tile(np.column_stack((x, y)), (2, 1)), rtol=1.2e-5)
        assert compare_text(export(root, f"{name}.native", native, controls), expected) == 4
        print(f"{name}: 4 matching coordinates with transformed spectra and row weights", flush=True)
    fully_flagged = root / "flagged-channel-spectra.ms"
    copy_ms(spectral, fully_flagged)
    with opened(fully_flagged, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
    compare_empty_export(root, native, "all-flagged-spectra", options | dict(vis=str(fully_flagged)),
                         ("Real", "Imag:corrected"))


def compare_selection(root, native, base, vis):
    separate = base | dict(spw="0:0,1:2", avgspw=False)
    raw = export(root, "raw.casa", reference, separate)
    selected = rows(raw)
    assert [(int(p[2]), int(p[11])) for p in selected] == [(0, 0)] * 2 + [(2, 1)] * 2
    np.testing.assert_allclose(coordinates(selected)[:, 0], [abs(1 + 2j)] * 2 + [15] * 2, rtol=1.2e-5)
    assert compare_text(export(root, "raw.native", native, separate), raw) == 4

    # CASA uses window 0's selected indices for both windows. A control places
    # window 1's explicitly selected sample at that index without changing its value.
    aligned = root / "aligned-channels.ms"
    copy_ms(vis, aligned)
    with opened(aligned, True) as table:
        high = table.getcol("DATA_DESC_ID") == 1
        for name in ("DATA", "CORRECTED_DATA", "FLAG"):
            values = table.getcol(name)
            values[:, 0, high] = values[:, 2, high]
            table.putcol(name, values)
    options = separate | dict(avgspw=True)
    original = rows(export(root, "different-channels.casa", reference, options))
    np.testing.assert_allclose(coordinates(original)[:, 0], abs((1 + 2j + 3 * (3 - 4j)) / 4), rtol=1.2e-5)
    expected = rows(export(root, "aligned-channels.casa", reference,
                           options | dict(vis=str(aligned), spw="0:0,1:0")))
    amplitude = abs((1 + 2j + 9 * (3 - 4j)) / 4)
    phase = np.angle(5 + 6j + 9 * (7 + 8j), deg=True)
    np.testing.assert_allclose(coordinates(expected), [(amplitude, phase)] * 2, rtol=1.2e-5)
    actual = export(root, "different-channels.native", native, options)
    assert "# spw: 0:0,1:2" in actual.read_text()
    selected = rows(actual)
    assert [p[2:] for p in selected] == [p[2:] for p in expected]
    np.testing.assert_allclose(coordinates(selected), coordinates(expected), rtol=1.2e-5)
    print("different-channels: 2 matching coordinates against the aligned CASA control", flush=True)

    unequal = base | dict(spw="0:0~1,1:1")
    expected = rows(export(root, "unequal.casa", reference, unequal))
    assert [(p[2], p[11]) for p in expected] == [("0", "0"), ("1", "0")] * 2
    actual = root / "unequal.native.txt"
    try:
        native(**unequal, plotfile=str(actual), expformat="txt")
    except RuntimeError as error:
        assert "spectral window averaging requires equal correlation and channel counts" in str(error), error
    else:
        raise AssertionError("Unequal selected channel counts should fail")
    assert not actual.exists()
    print("unequal: explicit shape refusal before export", flush=True)


def stagger_windows(root, vis, name):
    staggered = root / f"{name}.ms"
    copy_ms(vis, staggered)
    with opened(staggered, True) as table:
        times = table.getcol("TIME")
        start = times.min()
        delayed = times + 2 * (table.getcol("DATA_DESC_ID") == 1)
        table.putcol("TIME", delayed)
        table.putcol("TIME_CENTROID", delayed)
    return staggered, start


def compare_scalar_time(root, native, base, vis):
    staggered, start = stagger_windows(root, vis, "staggered-windows")
    scans, fields, spans, flagged, nonfinite, varied = (root / f"{name}.ms" for name in (
        "scan-boundary", "field-boundary", "both-boundaries", "flagged-time-bin", "nonfinite-time-bin",
        "varied-channel-phases",
    ))
    for source in (scans, fields, spans, flagged, nonfinite, varied):
        copy_ms(staggered, source)
    for source in (fields, spans):
        with opened(source / "FIELD", True) as table:
            table.addrows(1)
            for column in table.colnames():
                if table.iscelldefined(column, 0):
                    table.putcell(column, 1, table.getcell(column, 0))
            table.putcell("NAME", 1, "SECOND")
    for source in (scans, fields, spans):
        with opened(source, True) as table:
            second = (table.getcol("TIME") >= start + 10).astype(np.int32)
            if source in (scans, spans):
                table.putcol("SCAN_NUMBER", 1 + second)
            if source in (fields, spans):
                table.putcol("FIELD_ID", second)
    with opened(flagged, True) as table:
        times = table.getcol("TIME")
        flags = table.getcol("FLAG")
        flags[0, 1, times == start + 10] = True
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", times == start + 2)
    with opened(nonfinite, True) as table:
        data = table.getcol("CORRECTED_DATA")
        data[0, 1, table.getcol("TIME") == start + 2] = complex(np.nan, np.nan)
        table.putcol("CORRECTED_DATA", data)
    with opened(varied, True) as table:
        data = table.getcol("CORRECTED_DATA")
        w0 = table.getcol("DATA_DESC_ID") == 0
        data[:, 0, w0] = 1
        data[:, 1, w0] = 4j
        data[:, 0, ~w0] = 3 + 4j
        data[:, 1, ~w0] = 1
        table.putcol("CORRECTED_DATA", data)

    low = np.angle(5 + 6j, deg=True)
    high = np.angle(7 + 8j, deg=True)
    first_phase = (2 * low + 3 * high) / 5
    mixed_phase = (low + 3 * high) / 4
    varied_low, varied_high = np.angle([1 + 4j, 4 + 4j], deg=True)
    varied_phase = (2 * varied_low + 3 * varied_high) / 5
    # A bin retains the first SPW ID and the last attached buffer's grid.
    closed = [(5, 1, 0, 0, "90.001000000"), (12, 1, 0, 1, "100.001000000")]
    scan_barrier = [(1, 1, 0, 0, "100.001000000"), (11, 2, 0, 0, "100.001000000")]
    field_barrier = [(1, 1, 0, 0, "100.001000000"), (11, 1, 1, 0, "100.001000000")]
    options = base | dict(vis=str(staggered), xaxis="time", xdatacolumn="", avgtime="10",
                          scalar=True, title="Scalar time and SPW average")
    channels = dict(avgchannel="2", spw="0:0~3,1:0~3")
    closed_channels = [(5, 1, 0, 0, f"{frequency:.9f}") for frequency in (90.0005, 90.0025)] + [
        (12, 1, 0, 1, f"{frequency:.9f}") for frequency in (100.0005, 100.0025)
    ]
    cases = (
        ("scalar-time", {}, closed, [first_phase, high]),
        ("scalar-time-channel", channels | dict(vis=str(varied)), closed_channels,
         [varied_phase, first_phase, varied_high, high]),
        ("scalar-time-frequency", dict(xaxis="frequency"), closed, [first_phase, high]),
        ("scalar-time-columns", dict(xaxis="amp", xdatacolumn="data"), closed, [first_phase, high]),
        ("scalar-time-flags", dict(vis=str(flagged)), closed, [mixed_phase, high]),
        ("scalar-time-nonfinite", dict(vis=str(nonfinite)), closed, [np.nan, high]),
        ("scalar-scan-barrier", dict(vis=str(scans), avgtime="20"), scan_barrier, [mixed_phase] * 2),
        ("scalar-scan-span", dict(vis=str(scans), avgtime="20", avgscan=True),
         [(6, -1, 0, 0, "100.001000000")], [mixed_phase]),
        ("scalar-field-barrier", dict(vis=str(fields), avgtime="20", field=""), field_barrier, [mixed_phase] * 2),
        ("scalar-field-span", dict(vis=str(fields), avgtime="20", field="", avgfield=True),
         [(6, 1, 0, 0, "100.001000000")], [mixed_phase]),
        ("scalar-both-spans", dict(vis=str(spans), avgtime="20", field="", avgscan=True, avgfield=True) | channels,
         [(6, -1, 0, 0, "100.000500000"), (6, -1, 0, 0, "100.002500000")], [mixed_phase] * 2),
    )
    for name, overrides, bins, phases in cases:
        controls = options | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        metadata = [(f"{start + time:.3f}", str(scan), str(field), str(spw), frequency)
                    for time, scan, field, spw, frequency in bins]
        assert [(p[9], p[3], p[4], p[11], p[10]) for p in selected] == metadata
        np.testing.assert_allclose(coordinates(selected)[:, 1], phases, rtol=1.2e-5)
        match controls["xaxis"]:
            case "time":
                assert [p[0] for p in selected] == [p[9] for p in selected]
            case "frequency":
                assert [p[0] for p in selected] == [p[10] for p in selected]
            case "amp":
                np.testing.assert_allclose(coordinates(selected)[:, 0], [(2 * abs(2 + 4j) + 30) / 5, 10],
                                           rtol=1.2e-5)
        actual = export(root, f"{name}.native", native, controls)
        assert compare_text(actual, expected) == len(bins)
        if controls["xaxis"] in ("time", "frequency"):
            assert [p[0] for p in rows(actual)] == [p[0] for p in selected]
        print(f"{name}: {len(bins)} matching scalar time and SPW coordinates", flush=True)
    compare_figure(root, native, options, root / "scalar-time.casa.txt",
                   (options["title"], "Time (MJD seconds)", "Phase:corrected (degrees)"))


def compare_vector_time(root, native, base, vis):
    staggered, start = stagger_windows(root, vis, "vector-staggered-windows")
    with opened(staggered, True) as table:
        descriptions = table.getcol("DATA_DESC_ID")
        second = table.getcol("TIME") >= start + 10
        code = (1 + second) * np.where(descriptions == 0, 1, 10)
        for column, value in (("DATA", code), ("CORRECTED_DATA", 10j * code)):
            shape = table.getcol(column).shape
            table.putcol(column, np.broadcast_to(value.astype(np.complex64), shape).copy())
        sigma = np.where(second, 0.5, 1).astype(np.float32)
        table.putcol("SIGMA", np.broadcast_to(sigma, table.getcol("SIGMA").shape).copy())
    masked = root / "vector-time-mask.ms"
    copy_ms(staggered, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, table.getcol("TIME") == start + 10] = True
        table.putcol("FLAG", flags)

    options = base | dict(vis=str(staggered), xaxis="real", yaxis="imag", avgtime="10",
                          title="Vector time and SPW average")
    low = "90.001000000"
    high = "100.001000000"
    closed = [(5, 0, low)]
    cases = (
        ("vector-time-closed", {}, closed, [(11.625, 116.25)]),
        ("vector-time-long", dict(avgtime="20"), closed, [(13.95, 116.25)]),
        ("vector-time-short", dict(avgtime="4"), [(1, 0, high), (10, 0, low)], [(7.75, 77.5), (15.5, 155)]),
        ("vector-time-mask", dict(vis=str(masked)), closed, [(13, 130)]),
        ("vector-time-abscissa", dict(xaxis="time", xdatacolumn=""), closed, [(start + 5, 116.25)]),
        ("vector-time-frequency", dict(xaxis="frequency", xdatacolumn=""), closed, [(float(low), 116.25)]),
        ("vector-time-control", dict(avgtime=""), [(0, 0, low), (2, 1, high), (10, 0, low), (12, 1, high)],
         [(1, 10), (10, 100), (2, 20), (20, 200)]),
    )
    for name, overrides, anchors, values in cases:
        controls = options | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        assert [(p[9], p[11], p[10]) for p in selected] == [
            (f"{start + time:.3f}", str(spw), frequency) for time, spw, frequency in anchors
        ]
        np.testing.assert_allclose(coordinates(selected), values, rtol=1.2e-5)
        actual = export(root, f"{name}.native", native, controls)
        assert compare_text(actual, expected) == len(anchors)
        if controls["xaxis"] in ("time", "frequency"):
            assert [p[0] for p in rows(actual)] == [p[0] for p in selected]
        print(f"{name}: {len(anchors)} matching vector time and SPW coordinates", flush=True)
    compare_figure(root, native, options | dict(xaxis="frequency", xdatacolumn=""),
                   root / "vector-time-frequency.casa.txt", (options["title"], "Frequency (GHz)", "Imag:corrected"))

    source = make_fixture(root, "vector-baselines-source", antenna_count=3)
    vis, start = stagger_windows(root, source, "vector-sparse-baselines")
    controls = options | dict(vis=str(vis), xaxis="frequency", xdatacolumn="", yaxis="amp",
                              antenna="", spw="0:0~1,1:0~1", msselect="DATA_DESC_ID == 1 || ANTENNA2 == 1")
    expected = export(root, "vector-sparse-baselines.casa", reference, controls)
    selected = rows(expected)
    frequencies = ["90.000000000", "90.001000000"] * 3
    assert [p[0] for p in selected] == [p[10] for p in selected] == frequencies
    assert [(p[5], p[6]) for p in selected] == [("0", "1")] * 2 + [("0", "2")] * 2 + [("1", "2")] * 2
    assert {(p[9], p[11]) for p in selected} == {(f"{start + 5:.3f}", "0")}
    amplitudes = [abs(6.5 + 7.5j), abs(7 + 8j), abs(7 + 8j)]
    np.testing.assert_allclose(coordinates(selected)[:, 1],
                               [value * scale for value in amplitudes for scale in (1, 2)], rtol=1.2e-5)
    actual = export(root, "vector-sparse-baselines.native", native, controls)
    assert compare_text(actual, expected) == 6
    assert [p[0] for p in rows(actual)] == frequencies
    print("vector-sparse-baselines: 6 matching coordinates with retained baseline IDs", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("coordinates", "selection", "weights", "sparse", "disjoint",
                                           "malformed", "scalar-time", "vector-time"))
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
        vis = make_fixture(root)
        base = request(vis) | dict(avgspw=True, spw="0:1,1:1", title="Spectral-window average")
        if args.only in (None, "coordinates"):
            compare_coordinates(root, native, base, vis)
            compare_flags(root, native, base, vis)
        if args.only in (None, "selection"):
            compare_selection(root, native, base, vis)
        if args.only in (None, "weights"):
            compare_channel_weights(root, native, base, vis)
        if args.only in (None, "sparse"):
            compare_sparse_baselines(root, native, base)
        if args.only in (None, "disjoint"):
            compare_disjoint_channels(root, native, base)
        if args.only in (None, "malformed"):
            compare_malformed_shape(root, native, base, vis)
        if args.only in (None, "scalar-time"):
            compare_scalar_time(root, native, base, vis)
        if args.only in (None, "vector-time"):
            compare_vector_time(root, native, base, vis)


if __name__ == "__main__":
    main()
