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

from plotms_ms_channel_ordinates_oracle import compare_sides
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


def put_channel_spectra(table):
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


def compare_channel_weights(root, native, base, vis):
    spectral = root / "channel-spectra.ms"
    copy_ms(vis, spectral)
    with opened(spectral, True) as table:
        put_channel_spectra(table)
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


def add_second_field(vis):
    with opened(vis / "FIELD", True) as table:
        table.addrows(1)
        for column in table.colnames():
            if table.iscelldefined(column, 0):
                table.putcell(column, 1, table.getcell(column, 0))
        table.putcell("NAME", 1, "SECOND")


def compare_scalar_time(root, native, base, vis):
    staggered, start = stagger_windows(root, vis, "staggered-windows")
    scans, fields, spans, flagged, nonfinite, varied = (root / f"{name}.ms" for name in (
        "scan-boundary", "field-boundary", "both-boundaries", "flagged-time-bin", "nonfinite-time-bin",
        "varied-channel-phases",
    ))
    for source in (scans, fields, spans, flagged, nonfinite, varied):
        copy_ms(staggered, source)
    for source in (fields, spans):
        add_second_field(source)
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
    nonfinite = root / "vector-time-nonfinite.ms"
    copy_ms(staggered, nonfinite)
    with opened(nonfinite, True) as table:
        data = table.getcol("CORRECTED_DATA")
        data[0, 1, table.getcol("TIME") == start + 12] = complex(np.nan, np.nan)
        table.putcol("CORRECTED_DATA", data)
    signed = root / "vector-time-signed.ms"
    copy_ms(staggered, signed)
    with opened(signed, True) as table:
        data = table.getcol("CORRECTED_DATA")
        data[:, 0] = 4j
        data[:, 1] = 2j
        table.putcol("CORRECTED_DATA", data)
        descriptor = table.getcoldesc("DATA")
        table.addcols({"WEIGHT_SPECTRUM": dict(descriptor, valueType="float",
                                             dataManagerType="StandardStMan", dataManagerGroup="WEIGHT_SPECTRUM")})
        weights = np.full(data.shape, 2, dtype=np.float32)
        weights[:, 0] = np.where(table.getcol("TIME") == start, -1, 0)
        table.putcol("WEIGHT_SPECTRUM", weights)

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
        ("vector-time-nonfinite", dict(vis=str(nonfinite), avgtime="20"), closed, [(13.95, np.nan)]),
        ("vector-time-signed", dict(vis=str(signed), avgtime="20", xaxis="time", xdatacolumn="", spw="0:0~1"),
         [(5, 0, "90.000000000"), (5, 0, low)], [(start + 5, -4), (start + 5, 2)]),
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


def compare_vector_optional_weights(root, native, base):
    source = make_fixture(root, "vector-optional-spectra", antenna_count=3)
    controls = base | dict(xaxis="real", yaxis="imag", avgtime="20", antenna="")
    cases = (
        ("WEIGHT_SPECTRUM", ("data", "model"), ((4, -2), (1, 0))),
        ("SIGMA_SPECTRUM", ("corrected",), ((13, 15),)),
    )
    for spectrum, columns, coordinates_per_column in cases:
        vis = root / f"{spectrum.lower()}-sparse.ms"
        copy_ms(source, vis)
        with opened(vis, True) as table:
            descriptor = table.getcoldesc("DATA")
            table.addcols({spectrum: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                          dataManagerGroup=spectrum)})
            table.putcell(spectrum, 0, np.ones(table.getcell("DATA", 0).shape, dtype=np.float32))
            assert table.getcell("TIME", 0) == table.getcell("TIME", 1)
            assert not table.iscelldefined(spectrum, 1)
        for column, xy in zip(columns, coordinates_per_column, strict=True):
            options = controls | dict(vis=str(vis), xdatacolumn=column, ydatacolumn=column)
            name = f"vector-unused-spectrum-{column}"
            expected = export(root, f"{name}.casa", reference, options)
            selected = rows(expected)
            np.testing.assert_allclose(coordinates(selected), [xy] * 3, rtol=1.2e-5)
            assert compare_text(export(root, f"{name}.native", native, options), expected) == 3
            print(f"{name}: 3 matching coordinates with a sparse unused {spectrum}", flush=True)


def compare_vector_channel_spans(root, native, base, vis):
    source, start = stagger_windows(root, vis, "vector-layered-windows")
    with opened(source, True) as table:
        low = table.getcol("DATA_DESC_ID") == 0
        late = table.getcol("TIME") >= start + 10
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, first, second in (
            ("WEIGHT_SPECTRUM", [1, 3, 5, 7], [8, 4, 2, 1]),
            ("SIGMA_SPECTRUM", [1, 0.5, 1, 0.5], [0.5, 1, 0.5, 1]),
        ):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            spectrum = np.where(low, np.asarray(first, dtype=np.float32)[:, None],
                                np.asarray(second, dtype=np.float32)[:, None])
            spectrum = spectrum * np.where(late, 2, 1).astype(np.float32)
            table.putcol(name, np.broadcast_to(spectrum, shape).copy())
        for name in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(name) * (1 + late)
            table.putcol(name, values.astype(np.complex64))
    scans, fields, spans = (root / f"vector-{name}.ms" for name in ("scan-boundary", "field-boundary", "both"))
    for target in (scans, fields, spans):
        copy_ms(source, target)
        if target in (fields, spans):
            add_second_field(target)
        with opened(target, True) as table:
            late = (table.getcol("TIME") >= start + 10).astype(np.int32)
            if target in (scans, spans):
                table.putcol("SCAN_NUMBER", 1 + late)
            if target in (fields, spans):
                table.putcol("FIELD_ID", late)

    zero, model = (root / f"vector-layered-{name}.ms" for name in ("zero", "model"))
    for target in (zero, model):
        copy_ms(source, target)
    with opened(zero, True) as table:
        high = table.getcol("DATA_DESC_ID") == 1
        weights = table.getcol("WEIGHT_SPECTRUM")
        weights[:, :2, high] = 0.0
        table.putcol("WEIGHT_SPECTRUM", weights)
    with opened(model, True) as table:
        late = table.getcol("TIME") >= start + 10
        low = table.getcol("DATA_DESC_ID") == 0
        shape = table.getcol("MODEL_DATA").shape
        model_channels = np.arange(1, shape[1] + 1, dtype=np.float32)[None, :, None]
        values = model_channels * (1 + late) * np.where(low, 1, 5)
        table.putcol("MODEL_DATA", np.broadcast_to(values.astype(np.complex64), shape).copy())
        flags = table.getcol("FLAG")
        flags[0, 0, low & late] = True
        table.putcol("FLAG", flags)

    options = base | dict(vis=str(source), xaxis="real", yaxis="imag", avgtime="20",
                          title="Vector time, channel and SPW")
    channels = dict(avgchannel="2", spw="0:0~3,1:0~3")
    # Doubling visibility, sigma and weight at the second integration makes
    # the time means acquire factors 6/5 (observed) and 5/3 (corrected).
    left = np.array([(1.8, 10.5), (3.8, 21.5)]) * [6 / 5, 5 / 3]
    right = np.array([(3.6, 32 / 3), (9.6, 80 / 3)]) * [6 / 5, 5 / 3]
    long = np.average([left, right], axis=0, weights=[8, 7.5])
    short = long * [25 / 18, 1]
    zero_weight = [left[0], np.average([left[1], right[1]], axis=0, weights=[24, 9])]
    model_only = np.average([[2, 5.25], [11.25, 26.25]], axis=0, weights=[3.5, 4])
    early = np.array([30 / 7, 100 / 7])
    span = [early * [6 / 5, 5 / 3]]
    bins = [(5, 1, 0, 0, frequency) for frequency in ("90.000500000", "90.002500000")]
    single = [(5, 1, 0, 0, "90.001000000")]
    separate = [(0, 1, 0, 0, "90.001000000"), (10, 2, 1, 0, "90.001000000")]
    uncombined = bins + [(7, 1, 0, 1, frequency) for frequency in ("100.000500000", "100.002500000")]
    across_both = dict(vis=str(spans), field="", avgscan=True, avgfield=True)
    cases = (
        ("vector-layered-long", channels, bins, long),
        ("vector-layered-short", channels | dict(avgtime="10"), bins, short),
        ("vector-layered-control", channels | dict(avgspw=False), uncombined, np.vstack((left, right))),
        ("vector-layered-zero", channels | dict(vis=str(zero)), bins, zero_weight),
        ("vector-layered-model", channels | dict(vis=str(model), xdatacolumn="model", ydatacolumn="model"),
         bins, np.column_stack((model_only, np.zeros_like(model_only)))),
        ("vector-retained-boundaries", dict(vis=str(spans), field=""), separate, [early, 2 * early]),
        ("vector-keeps-field", dict(vis=str(spans), field="", avgscan=True), separate, [early, 2 * early]),
        ("vector-keeps-scan", dict(vis=str(spans), field="", avgfield=True), separate, [early, 2 * early]),
        ("vector-scan-span", dict(vis=str(scans), avgscan=True), single, span),
        ("vector-field-span", dict(vis=str(fields), field="", avgfield=True), single, span),
        ("vector-time-both-spans", across_both, single, span),
        ("vector-layered-both-spans", across_both | channels, bins, long),
    )
    for name, overrides, anchors, values in cases:
        controls = options | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        assert [(p[9], p[3], p[4], p[11], p[10]) for p in selected] == [
            (f"{start + time:.3f}", str(scan), str(field), str(spw), frequency)
            for time, scan, field, spw, frequency in anchors
        ]
        np.testing.assert_allclose(coordinates(selected), values, rtol=1.2e-5)
        assert compare_text(export(root, f"{name}.native", native, controls), expected) == len(anchors)
        print(f"{name}: {len(anchors)} matching vector time, channel and SPW coordinates", flush=True)
    compare_figure(root, native, options | channels, root / "vector-layered-long.casa.txt",
                   (options["title"], "Real", "Imag:corrected"))


def compare_ordinate_lists(root, native, base, source, *, only):
    root.mkdir()
    vis = root / "stored-list-model.ms"
    copy_ms(source, vis)
    with opened(vis, True) as table:
        descriptions = table.getcol("DATA_DESC_ID")
        shape = table.getcol("MODEL_DATA").shape
        channels = np.arange(1, shape[1] + 1, dtype=np.float32)[None, :, None]
        model = channels * np.where(descriptions == 0, 2 + 5j, 9 - 4j)
        table.putcol("MODEL_DATA", np.broadcast_to(model.astype(np.complex64), shape))
    staggered, _ = stagger_windows(root, vis, "list-staggered")
    options = base | dict(vis=str(vis), xaxis="frequency", xdatacolumn="",
                          yaxis=["real", "imag", "real"], ydatacolumn=["data", "corrected", "model"],
                          yaxislocation=["left", "right", "left"])
    combined = dict(vis=str(staggered), xaxis="time", avgtime="20", avgchannel="2", spw="0:0~3,1:0~3")
    if only == "ordinate-list-spans":
        compare_ordinate_list_spans(root, native, options | combined, vis)
        return
    if only == "ordinate-list-uvw":
        compare_ordinate_list_uvw(root, native, options, staggered)
        return
    for name, overrides, y in (
        ("vector-spw-ordinates", {}, [5, 5, 15, 15, 14.5, 14.5]),
        ("scalar-spw-ordinates", dict(scalar=True), [8.61803, 8.61803, .857989, .857989, 17.4659, 17.4659]),
        ("vector-time-channel-spw-ordinates", combined, [3.75, 8.75, 11.25, 26.25, 10.875, 25.375]),
        ("scalar-time-channel-spw-ordinates", combined | dict(scalar=True),
         [6.46353, 15.0816, .857989, .857989, 13.0994, 30.5653]),
    ):
        controls = options | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        assert len(selected) == 6
        assert {tuple(point[5:9]) for point in selected} == {("0", "1", "A0@P0", "A1@P1")}
        assert {tuple(point[11:13]) for point in selected} == {("0", "XX")}
        np.testing.assert_allclose(coordinates(selected)[:, 1], y, rtol=1.2e-5)
        actual = export(root, f"{name}.native", native, controls)
        assert compare_text(actual, expected) == 6
        if controls["xaxis"] == "time":
            np.testing.assert_allclose(coordinates(rows(actual))[:, 0], coordinates(selected)[:, 0],
                                       rtol=0, atol=.00051)
        else:
            assert [point[0] for point in rows(actual)] == ["90.001000000"] * 6
        print(f"{name}: six matching SPW-averaged coordinates", flush=True)
    compare_sides(root, native, options, expected_x="Frequency (GHz)", expected_right="Imag:corrected",
                  name="vector-spw-ordinates")
    compare_ordinate_list_masks(root, native, options, vis, combined)
    compare_ordinate_list_spans(root, native, options | combined, vis)
    compare_ordinate_list_uvw(root, native, options, staggered)


def compare_ordinate_list_masks(root, native, options, vis, combined):
    weighted, _ = stagger_windows(root, vis, "weighted-list-staggered")
    with opened(weighted, True) as table:
        descriptions = table.getcol("DATA_DESC_ID")
        flags = table.getcol("FLAG")
        flags[:, 2:, descriptions == 0] = True
        flags[0, 0, descriptions == 1] = True
        table.putcol("FLAG", flags)
        put_channel_spectra(table)
    complex_axes = combined | dict(vis=str(weighted), xaxis="real", xdatacolumn="corrected",
                                   yaxis=["real", "imag", "amp", "phase"],
                                   ydatacolumn=["data", "model", "corrected", "data"],
                                   yaxislocation=["left", "right", "left", "right"])
    for mode, scalar in (("vector", False), ("scalar", True)):
        name = f"{mode}-weighted-spw-ordinates"
        controls = options | complex_axes | dict(scalar=scalar)
        expected = export(root, f"{name}.casa", reference, controls)
        actual = export(root, f"{name}.native", native, controls)
        assert compare_text(actual, expected) == 8
        assert [point[2] for point in rows(expected)] == ["0", "1"] * 4
        print(f"{name}: eight matching independent coordinates with masked spectral weights", flush=True)
    for flag, count in (("FLAG_ROW", 6), ("FLAG", 0)):
        masked = root / f"all-{flag.lower()}.ms"
        copy_ms(vis, masked)
        with opened(masked, True) as table:
            table.putcol(flag, np.ones_like(table.getcol(flag)))
        for mode, scalar in (("vector", False), ("scalar", True)):
            name = f"{mode}-{flag.lower()}-spw-ordinates"
            controls = options | dict(vis=str(masked), scalar=scalar)
            expected = export(root, f"{name}.casa", reference, controls)
            actual = export(root, f"{name}.native", native, controls)
            if count:
                assert compare_text(actual, expected) == count
            else:
                assert expected.read_text().count("# No data to export") == 3
                assert actual.read_bytes() == expected.read_bytes()
            print(f"{name}: {count if count else 'three empty sections'} matched", flush=True)


def compare_ordinate_list_spans(root, native, options, source):
    for name, scan, field in (("scan", True, False), ("field", False, True), ("both", True, True)):
        vis, start = stagger_windows(root, source, f"list-span-{name}")
        if field:
            add_second_field(vis)
        with opened(vis, True) as table:
            late = (table.getcol("TIME") >= start + 10).astype(np.int32)
            if scan:
                table.putcol("SCAN_NUMBER", 1 + late)
            if field:
                table.putcol("FIELD_ID", late)
            for column in ("DATA", "CORRECTED_DATA", "MODEL_DATA"):
                table.putcol(column, (table.getcol(column) * (1 + late)).astype(np.complex64))
        for mode, scalar, y in (
            ("vector", False, [5.625, 13.125, 16.875, 39.375, 16.3125, 38.0625]),
            ("scalar", True, [9.69529, 22.6223, .857989, .857989, 19.6491, 45.8479]),
        ):
            case = f"{mode}-{name}-spw-ordinates"
            controls = options | dict(vis=str(vis), field="", avgscan=scan, avgfield=field, scalar=scalar)
            expected = export(root, f"{case}.casa", reference, controls)
            selected = rows(expected)
            scan_id = "-1" if scalar and scan else "1"
            assert [tuple(point[3:5]) for point in selected] == [(scan_id, "0")] * 6
            np.testing.assert_allclose(coordinates(selected)[:, 1], y, rtol=1.2e-5)
            actual = export(root, f"{case}.native", native, controls)
            assert compare_text(actual, expected) == 6
            np.testing.assert_allclose(coordinates(rows(actual))[:, 0], coordinates(selected)[:, 0],
                                       rtol=0, atol=.00051)
            print(f"{case}: six matching coordinates across boundaries", flush=True)


def compare_ordinate_list_uvw(root, native, options, vis):
    from casars.tasks import TaskParameterError

    controls = options | dict(vis=str(vis), xaxis="u", avgtime="20")
    expected = export(root, "spw-list-u.casa", reference, controls)
    assert len(rows(expected)) == 3
    output = root / "spw-list-u.native.txt"
    try:
        native(**controls, plotfile=str(output), expformat="txt")
    except TaskParameterError as error:
        assert "#1032" in str(error), error
    else:
        raise AssertionError("PlotMS accepted pending SPW averaging against a U abscissa")
    assert not output.exists()

    ordinary_time = controls | dict(avgspw=False)
    expected = export(root, "time-list-u.casa", reference, ordinary_time)
    actual = export(root, "time-list-u.native", native, ordinary_time)
    assert compare_text(actual, expected) == 6
    print("uvw-spw-ordinates: pending SPW U guard and six ordinary time U coordinates verified", flush=True)


def virtual_spw_models(root, name, *, spectral_index=0):
    vis = make_fixture(root, name)
    with opened(vis, True) as table:
        model = table.getcol("MODEL_DATA")
        table.putcol("MODEL_DATA", np.full_like(model, 19))
    for window, flux, frequency in ((0, 7.25, "90GHz"), (1, 3.25, "100GHz")):
        assert casa.setjy(str(vis), field="FLUX", spw=str(window), standard="manual", usescratch=False,
                          fluxdensity=[flux, 0, 0, 0], reffreq=frequency, spix=[spectral_index])
    return vis


def compare_virtual_spw_models(root, native):
    root.mkdir()
    vis = virtual_spw_models(root, "virtual-windows")
    sloped = virtual_spw_models(root, "sloped-virtual-windows", spectral_index=9)
    shifted, _ = stagger_windows(root, vis, "virtual-staggered-windows")
    reversed_windows = root / "virtual-reversed-windows.ms"
    copy_ms(vis, reversed_windows)
    with opened(reversed_windows, True) as table:
        table.putcol("DATA_DESC_ID", 1 - table.getcol("DATA_DESC_ID"))
    masked = root / "virtual-masked-windows.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, table.getcol("DATA_DESC_ID") == 0] = True
        table.putcol("FLAG", flags)
    title = "Virtual spectral-window models"
    base = request(vis) | dict(xaxis="frequency", xdatacolumn="", spw="0:1,1:1", yaxis="real",
                                ydatacolumn="model", avgspw=True, title=title)
    first, second = ("90.001000000", 7.25, "0"), ("100.001000000", 3.25, "1")
    channels = dict(avgchannel="2", spw="0:0~3,1:0~3")
    binned = [(freq, 7.25, "0") for freq in ("90.000500000", "90.002500000")]
    for name, overrides, values in (
        ("raw-virtual-model-windows", dict(avgspw=False), [first, first, second, second]),
        ("vector-virtual-model-spw", {}, [first] * 2),
        ("scalar-virtual-model-spw", dict(scalar=True), [first] * 2),
        ("selected-virtual-model-spw", dict(spw="1:1"), [second] * 2),
        ("reversed-virtual-model-spw", dict(vis=str(reversed_windows)), [second] * 2),
        ("staggered-virtual-model-spw", dict(vis=str(shifted)), [first, second] * 2),
        ("vector-time-virtual-model-spw", dict(vis=str(shifted), avgtime="20"), [(first[0], 5.25, "0")]),
        ("scalar-time-virtual-model-spw", dict(vis=str(shifted), avgtime="20", scalar=True), [(second[0], 4.25, "0")]),
        ("channel-virtual-model-spw", channels, binned * 2),
        ("time-channel-virtual-model-spw", channels | dict(avgtime="20"), binned),
        ("sloped-model-spw-channel-indices", dict(vis=str(sloped), spw="0:1,1:2"),
         [(first[0], 7.25 * (90.001 / 90) ** 9, "0")] * 2),
        ("masked-virtual-model-spw", dict(vis=str(masked)), [first] * 2),
    ):
        controls = base | overrides
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        assert [(p[0], p[11]) for p in selected] == [(freq, spw) for freq, _, spw in values], name
        np.testing.assert_allclose([float(p[1]) for p in selected], [model for _, model, _ in values], rtol=1.2e-5)
        actual = export(root, f"{name}.native", native, controls)
        assert compare_text(actual, expected) == len(values)
        print(f"{name}: {len(values)} matching virtual model coordinates", flush=True)
        if name == "channel-virtual-model-spw":
            compare_figure(root, native, controls, expected, (title, "Frequency (GHz)", "Real:model"))
    for source in (vis, sloped, shifted, reversed_windows, masked):
        with opened(source) as table:
            np.testing.assert_array_equal(table.getcol("MODEL_DATA"), 19)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("coordinates", "selection", "weights", "sparse", "disjoint",
                                           "malformed", "scalar-time", "vector-time", "vector-optional",
                                           "vector-channel-spans", "ordinate-lists", "ordinate-list-spans",
                                           "ordinate-list-uvw", "virtual-models"))
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
        if args.only == "virtual-models":
            compare_virtual_spw_models(root / "virtual-models", native)
            return
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
        if args.only in (None, "vector-optional"):
            compare_vector_optional_weights(root, native, base)
        if args.only in (None, "vector-channel-spans"):
            compare_vector_channel_spans(root, native, base, vis)
        if args.only in (None, "ordinate-lists", "ordinate-list-spans", "ordinate-list-uvw"):
            compare_ordinate_lists(root / "ordinate-lists", native, base, vis, only=args.only)
        if args.only is None:
            compare_virtual_spw_models(root / "virtual-models", native)


if __name__ == "__main__":
    main()
