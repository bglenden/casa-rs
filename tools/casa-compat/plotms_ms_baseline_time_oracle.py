#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public PlotMS time and baseline averages with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_baseline_channels_oracle import make_fixture as make_channel_baseline_fixture
from plotms_ms_baseline_channels_oracle import make_reversed_partial_spectrum
from plotms_ms_columns_oracle import (
    compare_empty_export,
    compare_figure,
    compare_ordinate_figure,
    compare_text,
    copy_ms,
    make_cal_ms,
    opened,
    plotms_launcher,
    reference,
    request,
    rows,
    time_average_fixture,
)
from plotms_ms_scalar_time_oracle import compare_scalar_shape_refusal, make_mixed_polarization


def make_fixture(root):
    source, integration = time_average_fixture(root)
    with opened(source, True) as table:
        high = table.getcol("ANTENNA2") == 2
        sequence = integration % 6
        shape = table.getcol("DATA").shape
        amplitude = np.where(high, 3 + 2 * sequence, 1 + sequence).astype(np.complex64)
        for index, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(name, np.broadcast_to(amplitude * 10**index, shape).copy())
        for name, values in (
            ("WEIGHT", np.where(high, 1 + (sequence % 3) * 2, 9 - sequence)),
            ("SIGMA", np.where(sequence % 2 == 0, 1, 0.5)),
        ):
            table.putcol(name, np.broadcast_to(values.astype(np.float32), (shape[0], shape[2])).copy())
        start = table.getcol("TIME").min()
    flagged = root / "flags.ms"
    copy_ms(source, flagged)
    with opened(flagged, True) as table:
        flag = table.getcol("FLAG")
        flag[0, 1, ~high & np.isin(sequence, [1, 3, 4])] = True
        table.putcol("FLAG", flag)
    return source, flagged, start


def compare(root, native):
    source, flagged, start = make_fixture(root)
    controls = request(source) | dict(
        antenna="A0&A1;A0&A2", xaxis="real", xdatacolumn="data",
        yaxis=["real", "real"], ydatacolumn=["corrected", "model"],
        yaxislocation=["left", "right"], spw="0:1", avgbaseline=True, avgtime="20",
    )
    late_baseline = dict(msselect=f"ANTENNA1 != 0 || ANTENNA2 != 2 || TIME > {start + 0.5:.3f}")
    late_low_antenna = f"ANTENNA1 != 0 || ANTENNA2 != 1 || TIME > {start + 0.5:.3f}"
    cases = (
        ("equal-endpoint", {}, [(3.75, 33), (9, 1470 / 17)], [15, 45]),
        ("row-flags", dict(vis=str(flagged)), [(100 / 26, 920 / 26), (10.4, 1220 / 12)], [10, 45]),
        ("row-flags-time", dict(vis=str(flagged), xaxis="time"),
         [(start + 10, 920 / 26), (start + 45, 1220 / 12)], [10, 45]),
        ("retained-time-boundary", dict(avgtime="15"),
         [(49 / 21, 430 / 21), (101 / 19, 890 / 19), (9, 1470 / 17)], [5, 25, 45]),
        ("singleton-time-stage", dict(avgtime="4"),
         [(v, 10 * v) for v in (1.2, 31 / 11, 56 / 12, 33 / 7, 58 / 8, 89 / 9)], list(range(0, 60, 10))),
        ("late-baseline", late_baseline, [(123.2 / 38, 1200 / 38), (8.6, 1560 / 18)], [15, 40]),
        ("flagged-late-baseline", late_baseline | dict(vis=str(flagged)),
         [(3.05, 800 / 24), (126.6 / 13, 1310 / 13)], [10, 40]),
        ("final-buffer-order", dict(xaxis="time", avgtime="120", msselect=late_low_antenna),
         [(start + 25, 56.25)], [25]),
    )
    for name, options, corrected, offsets in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | options | dict(expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 2 * len(offsets)
        assert {tuple(point[5:9]) for point in selected} == {("-1", "-1", "*", "*")}
        assert [point[9] for point in selected] == [f"{start + offset:.3f}" for offset in offsets] * 2
        model = [(x, 10 * y) for x, y in corrected]
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   corrected + model, rtol=1.2e-5)
        if options["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in selected],
                                       [start + offset for offset in offsets] * 2, rtol=0, atol=0.00051)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching time-and-baseline coordinates", flush=True)
    compare_ordinate_figure(root, "equal-endpoint", native, controls, "Real", ["Real:corrected", "Real:model"])

    expected, actual = (root / f"single-ordinate.{engine}.txt" for engine in ("casa", "native"))
    options = controls | dict(yaxis="real", ydatacolumn="corrected", yaxislocation="left", expformat="txt")
    assert reference(**options, plotfile=str(expected)) is None
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2
    compare_partial_spectrum(root, native, controls)


def compare_partial_spectrum(root, native, controls):
    work = root / "partial-spectrum"
    work.mkdir()
    source, *_ = make_channel_baseline_fixture(work)
    vis = make_reversed_partial_spectrum(source, work / "reversed-partial-spectrum.ms")
    expected, actual = (root / f"spectrum-availability.{engine}.txt" for engine in ("casa", "native"))
    options = controls | dict(vis=str(vis), expformat="txt")
    assert reference(**options, plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 4
    np.testing.assert_array_equal(np.array([point[:2] for point in selected], dtype=float),
                                  [(10, 45), (10, 55), (10, 450), (10, 550)])
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    print(f"spectrum-availability: {len(selected)} matching time-and-baseline coordinates", flush=True)

    with opened(vis, True) as table:
        antenna2 = table.getcol("ANTENNA2")
        first_baseline = ((table.getcol("ANTENNA1") == 0) & (antenna2 == 1)
                          & (table.getcol("FIELD_ID") == 0))
        times = table.getcol("TIME")
        first_row_time = times[np.flatnonzero(first_baseline)[0]]
        shared_buffer = (antenna2 == 2) & (times == first_row_time)
        weights = np.ones_like(table.getcol("WEIGHT"))
        weights[:, shared_buffer] = 9
        corrected = np.ones_like(table.getcol("CORRECTED_DATA")) * 10
        corrected[:, :, shared_buffer] = 100
        table.putcol("WEIGHT", weights)
        table.putcol("CORRECTED_DATA", corrected)
    options |= dict(xaxis="u", xdatacolumn="", yaxis="real", ydatacolumn="corrected",
                    yaxislocation="left", avgbaseline=False, avgchannel="", spw="0:1")
    expected, actual = (root / f"time-weighted-sparse-spectrum.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 6
    np.testing.assert_allclose([float(point[1]) for point in selected], [10, 10, 10, 10, 10, 91])
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    print(f"time-weighted-sparse-spectrum: {len(selected)} matching time-averaged UVW coordinates",
          flush=True)


def compare_time_channel_baselines(root, native):
    work = root / "time-channel-baselines"
    work.mkdir()
    plain, spectral, _, flagged = make_channel_baseline_fixture(work)
    with opened(plain) as table:
        start = table.getcol("TIME").min()
    controls = request(plain) | dict(
        antenna="A0&A1;A0&A2", xaxis="real", xdatacolumn="data",
        yaxis=["real", "real"], ydatacolumn=["corrected", "model"],
        yaxislocation=["left", "right"], spw="0:0~3", avgtime="20",
        avgchannel="2", avgbaseline=True,
    )
    zero_observed, zero_calibrated = (work / f"{name}.ms" for name in ("zero-observed", "zero-calibrated"))
    for vis, column in ((zero_observed, "SIGMA_SPECTRUM"), (zero_calibrated, "WEIGHT_SPECTRUM")):
        copy_ms(spectral, vis)
        with opened(vis, True) as table:
            weights = table.getcol(column)
            weights[0, :, table.getcol("ANTENNA2") == 2] = 0
            table.putcol(column, weights)
    cases = (
        ("scalar-spectra", plain, [(4.7, 47), (9.7, 97)]),
        ("stored-spectra", spectral, [(217 / 110, 31), (163 / 22, 815 / 11)]),
        ("masked-channel-and-time", flagged, [(53 / 30, 50 / 3), (7.5, 75)]),
        ("zero-observed", zero_observed, [(133 / 110, 31), (133 / 22, 815 / 11)]),
        ("zero-calibrated", zero_calibrated, [(1.4, 28), (7, 70)]),
    )
    for name, vis, corrected in cases:
        options = controls | dict(vis=str(vis), expformat="txt")
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 8
        assert [point[2] for point in selected] == ["0", "1"] * 4
        assert [point[10] for point in selected] == ["100.000500000", "100.002500000"] * 4
        assert [point[9] for point in selected] == [f"{start + offset:.3f}" for offset in (15, 15, 45, 45)] * 2
        assert {tuple(point[5:9]) for point in selected} == {("-1", "-1", "*", "*")}
        model = [(x, 10 * y) for x, y in corrected]
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   corrected * 2 + model * 2, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching time, channel and baseline coordinates", flush=True)
    compare_ordinate_figure(work, "masked-channel-and-time", native, controls | dict(vis=str(flagged)),
                           "Real", ["Real:corrected", "Real:model"])
    expected, actual = (work / f"single-ordinate.{engine}.txt" for engine in ("casa", "native"))
    options = controls | dict(yaxis="real", ydatacolumn="corrected", yaxislocation="left", expformat="txt")
    assert reference(**options, plotfile=str(expected)) is None
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 4
    compare_channel_time_reductions(work, native, controls, spectral)


def compare_channel_time_reductions(work, native, controls, spectral):
    model = work / "partly-flagged-model.ms"
    calibrated = work / "partly-zero-calibrated.ms"
    for path in (model, calibrated):
        copy_ms(spectral, path)
    with opened(model, True) as table:
        _, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        affected = (table.getcol("ANTENNA2") == 2) & (integration % 2 == 0)
        flags = table.getcol("FLAG")
        flags[0, 1, affected] = True
        table.putcol("FLAG", flags)
    with opened(calibrated, True) as table:
        weights = table.getcol("WEIGHT_SPECTRUM")
        weights[0, :2, table.getcol("ANTENNA2") == 2] = 0
        table.putcol("WEIGHT_SPECTRUM", weights)
    cases = (
        ("model-channel-means", model, "model", "20", 2, [(0, 3700 / 11), (1, 9200 / 11)]),
        ("calibrated-bin-weights", calibrated, "corrected", "20", 2, [(0, 28), (1, 510 / 7)]),
        ("calibrated-bin-weights-no-time", calibrated, "corrected", "", 6, [(0, 28), (1, 510 / 7)]),
    )
    for name, vis, column, interval, windows, coords in cases:
        options = controls | dict(vis=str(vis), xaxis="channel", xdatacolumn="", yaxis="real",
                                  ydatacolumn=column, yaxislocation="left", avgtime=interval, expformat="txt")
        expected, actual = (work / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == len(coords) * windows
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coords * windows, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching time, channel and baseline coordinates", flush=True)


def compare_scalar_baselines(root, native):
    source, flagged, start = make_fixture(root)
    for vis in (source, flagged):
        with opened(vis, True) as table:
            high = table.getcol("ANTENNA2") == 2
            for column, phase in (("DATA", 120), ("CORRECTED_DATA", 80), ("MODEL_DATA", 45)):
                angles = np.radians(np.where(high, -phase, phase))
                values = table.getcol(column) * np.exp(1j * angles)
                table.putcol(column, values.astype(np.complex64))

    base = request(source) | dict(
        antenna="A0&A1;A0&A2", spw="0:1", avgbaseline=True, scalar=True,
        xaxis="amp", xdatacolumn="data", yaxis="phase", ydatacolumn="corrected",
        title="Scalar baseline averages",
    )
    amplitudes = [1.2, 31 / 11, 56 / 12, 33 / 7, 58 / 8, 89 / 9]
    phases = [64, 400 / 11, 40 / 3, 400 / 7, 20, -80 / 9]
    times = [start + offset for offset in range(0, 60, 10)]
    pairs = list(zip(amplitudes, phases, strict=True))
    flagged_pairs = [pairs[0], (5, -80), pairs[2], (9, -80), (11, -80), pairs[5]]
    avgtime_pairs = [(3, 400 / 11), (7.5, 20)]
    avgtime_times = [start + 10, start + 40]
    channel_source = root / "channels.ms"
    copy_ms(source, channel_source)
    with opened(channel_source, True) as table:
        data = table.getcol("DATA")
        # The first two channels partially cancel before scalar averaging.
        data[:, 0, :] *= -0.5
        table.putcol("DATA", data)
    channels = dict(vis=str(channel_source), spw="0:0~3", avgchannel="2")

    def channel_pairs(coordinates):
        return [pair for amp, phase in coordinates for pair in ((amp / 4, phase), (amp, phase))]

    spans = root / "scan-span.ms"
    copy_ms(source, spans)
    with opened(spans, True) as table:
        scans = np.where(table.getcol("TIME") >= start + 30, 2, table.getcol("SCAN_NUMBER"))
        table.putcol("SCAN_NUMBER", scans.astype(np.int32))
    cases = (
        ("independent", {}, pairs, times),
        ("time-axis", dict(xaxis="time"), list(zip(times, phases, strict=True)), times),
        ("cube-flags", dict(vis=str(flagged)), flagged_pairs, times),
        ("signed-residual", dict(yaxis="amp", ydatacolumn="corrected-model_vector"),
         [(amp, -90 * amp) for amp in amplitudes], times),
        ("avgtime", dict(avgtime="20"), avgtime_pairs, avgtime_times),
        ("avgchannel", channels, channel_pairs(pairs), np.repeat(times, 2)),
        ("avgtime-avgchannel", channels | dict(avgtime="20"),
         channel_pairs(avgtime_pairs), np.repeat(avgtime_times, 2)),
        ("independent-ordinates", dict(yaxis=["phase", "amp"], ydatacolumn=["corrected", "model"],
                                       yaxislocation=["left", "right"]),
         pairs + [(amp, 100 * amp) for amp in amplitudes], times * 2),
        ("scan-span", dict(vis=str(spans), avgtime="200", avgscan=True),
         [(279 / 57, 1680 / 57)], [start + 25]),
    )
    for name, overrides, coordinates, centers in cases:
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == len(coordinates)
        scan = "-1" if name == "scan-span" else "1"
        assert {tuple(point[3:9]) for point in selected} == {(scan, "0", "-1", "-1", "*", "*")}
        assert {tuple(point[11:13]) for point in selected} == {("0", "XX")}
        np.testing.assert_allclose([float(point[9]) for point in selected], centers, rtol=0, atol=.00051)
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        if "avgchannel" in controls:
            assert [point[2] for point in selected] == ["0", "1"] * (len(selected) // 2)
            assert [point[10] for point in selected] == ["100.000500000", "100.002500000"] * (len(selected) // 2)
        else:
            assert {(point[2], point[10]) for point in selected} == {("1", "100.001000000")}
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        if controls["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], centers, rtol=0, atol=.00051)
        print(f"{name}: {len(coordinates)} matching scalar baseline coordinates", flush=True)
        if name == "independent":
            compare_figure(root, native, controls, expected, (base["title"], "Amp", "Phase:corrected (degrees)"))

    compare_scalar_baseline_polarizations(root, native, source, base, start)
    compare_empty_scalar_baselines(root, native, source, base)


def compare_scalar_baseline_polarizations(root, native, source, base, start):
    from grpc import RpcError, StatusCode

    reduced = make_mixed_polarization(source, root / "mixed-polarization.ms",
                                      lambda table: table.getcol("ANTENNA2") == 2, correlation_slot=1)
    with opened(reduced, True) as table:
        for row in map(int, np.flatnonzero(table.getcol("DATA_DESC_ID") == 0)):
            for column in ("DATA", "CORRECTED_DATA"):
                values = table.getcell(column, row)
                values[0] *= 100j
                table.putcell(column, row, values)
    controls = base | dict(vis=str(reduced), correlation="YY", msselect=f"TIME < {start + 1:.3f}")
    rejected = root / "different-physical-slots.casa.txt"
    try:
        reference(**controls, plotfile=str(rejected), expformat="txt")
    except RpcError as error:
        assert error.code() == StatusCode.INVALID_ARGUMENT, error
    else:
        raise AssertionError("CASA accepted YY at different physical correlation slots")
    assert not rejected.exists()

    # Reorder the full setup to give CASA a control with YY in slot zero in both setups.
    normalized = root / "matching-slots" / reduced.name
    normalized.parent.mkdir()
    copy_ms(reduced, normalized)
    with opened(normalized / "POLARIZATION", True) as pol:
        pol.putcell("CORR_TYPE", 0, pol.getcell("CORR_TYPE", 0)[::-1].copy())
        pol.putcell("CORR_PRODUCT", 0, pol.getcell("CORR_PRODUCT", 0)[:, ::-1].copy())
    with opened(normalized, True) as table:
        for row in map(int, np.flatnonzero(table.getcol("DATA_DESC_ID") == 0)):
            for column in ("DATA", "CORRECTED_DATA", "MODEL_DATA", "FLAG", "FLAG_CATEGORY", "WEIGHT", "SIGMA"):
                if table.iscelldefined(column, row):
                    table.putcell(column, row, table.getcell(column, row)[::-1].copy())
    expected, actual = (root / f"selected-yy.{engine}.txt" for engine in ("casa", "native"))
    casa_controls = controls | dict(vis=str(normalized))
    assert reference(**casa_controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[5], point[6], point[12]) for point in selected] == [("-1", "-1", "YY")]
    np.testing.assert_allclose(np.array(selected[0][:2], dtype=float), [1.2, 64], rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 1
    print("selected-yy: 1 point matching the CASA control with aligned physical correlation slots", flush=True)


def compare_empty_scalar_baselines(root, native, source, controls):
    masked = root / "all-flagged.ms"
    copy_ms(source, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, :] = True
        table.putcol("FLAG", flags)
    compare_empty_export(root, native, "all-flagged", controls | dict(vis=str(masked)),
                         ("Amp", "Phase:corrected (degrees)"))


def compare_antenna_averaged_export(root, native):
    source = root / "antenna-averages.ms"
    make_cal_ms(str(source), stokes="XX XY YX YY")
    casa.clearcal(str(source), addmodel=True)
    with opened(source, True) as table:
        shape = table.getcol("DATA").shape
        corr = np.arange(shape[0])[:, None, None]
        observed = 1 + 2 * corr + 1j * (4 + corr)
        corrected = 10 + 3 * corr + 1j * (14 + 2 * corr)
        for name, values in (("DATA", observed), ("CORRECTED_DATA", corrected)):
            table.putcol(name, np.broadcast_to(values.astype(np.complex64), shape).copy())
        table.putcol("FLAG", np.zeros(shape, dtype=bool))
        table.putcol("FLAG_ROW", np.zeros(shape[2], dtype=bool))
        start = table.getcol("TIME").min()
    base = request(source) | dict(
        avgantenna=True, spw="0:1", msselect=f"TIME < {start + 1:.3f}",
        xaxis="real", yaxis="imag", title="Per-antenna visibility averages",
    )
    names = ("XX", "XY", "YX", "YY")
    for name, correlation, slots, scalar in (
        ("cross-hands", "XY,YX", (1, 2), False),
        ("parallel-hands", "XX,YY", (0, 3), False),
        ("all-correlations", "", tuple(range(4)), False),
        ("scalar-correlations", "", tuple(range(4)), True),
        ("selected-cross-hand", "XY", (1,), False),
    ):
        controls = base | dict(correlation=correlation)
        if scalar:
            controls |= dict(xaxis="amp", yaxis="phase", scalar=True)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [tuple(point[5:9]) + (point[12],) for point in selected] == [
            (str(antenna), "-1", f"A{antenna}@P{antenna}", "*", names[slot])
            for antenna in (0, 1) for slot in slots
        ]
        x_values = [abs(complex(1 + 2 * slot, 4 + slot)) if scalar else 1 + 2 * slot for slot in slots]
        y_values = [np.degrees(np.arctan2(14 + 2 * slot, 10 + 3 * slot)) if scalar else 14 + 2 * slot
                    for slot in slots]
        coordinates = [(x, direction * y) for direction in (1, -1)
                       for x, y in zip(x_values, y_values, strict=True)]
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[9]) for point in selected], start, rtol=0, atol=.00051)
        assert {(point[2], point[10]) for point in selected} == {("1", "100.001000000")}
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        print(f"{name}: {len(coordinates)} matching per-antenna visibility coordinates", flush=True)
        if scalar:
            compare_figure(root, native, controls, expected,
                           (base["title"], "Amp", "Phase:corrected (degrees)"))

    compare_antenna_ordinate_order(root, native, base)
    weighted = compare_weighted_antenna_exports(root, native, source, start, base)
    compare_weighted_scalar_antenna(root, native, weighted, start, base)
    compare_channel_antenna_averages(root, native, weighted, start, base)
    compare_scalar_antenna_time_averages(root, native, weighted, start, base)
    compare_antenna_flagged_integration(root, native, source, start, base)
    compare_antenna_autocorrelation(root, native)


def compare_antenna_ordinate_order(root, native, base):
    controls = base | dict(correlation="XY", yaxis=["phase", "amp"], ydatacolumn=["data", "data"])
    expected, actual = (root / f"ordinate-order.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[5], point[6], point[12]) for point in selected] == [("0", "-1", "XY"), ("1", "-1", "XY")] * 2
    phase = np.degrees(np.arctan2(5, 3))
    amplitude = np.hypot(3, 5)
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               [(3, phase), (3, -phase), (3, amplitude), (3, amplitude)], rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 4
    print("ordinate-order: 4 matching points in requested phase and amplitude order", flush=True)


def compare_weighted_antenna_exports(root, native, source, start, base):
    weighted = root / "weighted-antenna-averages.ms"
    copy_ms(source, weighted)
    with opened(weighted, True) as table:
        shape = table.getcol("DATA").shape
        distant = table.getcol("ANTENNA2") == 2
        for column in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(column)
            values[:, :, distant] *= 2j
            table.putcol(column, values)
        descriptor = table.getcoldesc("DATA")
        weights = np.where(distant, 3, 1)
        for column, values in (("WEIGHT_SPECTRUM", weights), ("SIGMA_SPECTRUM", 1 / np.sqrt(weights))):
            table.addcols({column: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                        dataManagerGroup=column)})
            table.putcol(column, np.broadcast_to(values.astype(np.float32), shape).copy())
        flags = table.getcol("FLAG")
        flags[1, 1, distant] = True
        table.putcol("FLAG", flags)
    controls = base | dict(vis=str(weighted), antenna="A0&A1;A0&A2", correlation="XX,XY",
                           spw="0:0~1", msselect=f"TIME < {start + 11:.3f}")
    expected, actual = (root / f"weighted-ordered.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    samples = (
        (0, 0, "XX", -5.75, 18.5), (0, 0, "XY", -6.75, 23.5),
        (0, 1, "XX", -5.75, 18.5), (0, 1, "XY", 3, 16),
        (1, 0, "XX", 1, -14), (1, 0, "XY", 3, -16),
        (1, 1, "XX", 1, -14), (1, 1, "XY", 3, -16),
        (2, 0, "XX", -8, -20), (2, 0, "XY", -10, -26), (2, 1, "XX", -8, -20),
    )
    assert [(point[5], point[6], point[2], point[12]) for point in selected] == [
        (str(antenna), "-1", str(channel), correlation)
        for antenna, channel, correlation, _, _ in samples * 2
    ]
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               [(x, y) for _, _, _, x, y in samples * 2], rtol=1.2e-5)
    np.testing.assert_allclose([float(point[9]) for point in selected],
                               [start] * len(samples) + [start + 10] * len(samples), rtol=0, atol=.00051)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 2 * len(samples)
    print("weighted-ordered: 22 matching points in integration, antenna, channel and correlation order", flush=True)
    return weighted


def compare_antenna_flagged_integration(root, native, source, start, base):
    separated = root / "flagged-integration.ms"
    copy_ms(source, separated)
    with opened(separated, True) as table:
        times = table.getcol("TIME")
        _, integration = np.unique(times, return_inverse=True)
        flagged = integration == 1
        late_array = integration == 2
        times[late_array] = start
        table.putcol("TIME", times)
        table.putcol("TIME_CENTROID", times)
        arrays = table.getcol("ARRAY_ID")
        arrays[late_array] = 1
        table.putcol("ARRAY_ID", arrays)
        flags = table.getcol("FLAG")
        flags[:, :, flagged] = True
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", flagged)
        for column in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(column)
            values[:, :, late_array] *= 3
            table.putcol(column, values)
    controls = base | dict(vis=str(separated), correlation="XX", msselect=f"TIME < {start + 11:.3f}")
    expected, actual = (root / f"flagged-integration.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[5], point[6], point[12]) for point in selected] == [("0", "-1", "XX"), ("1", "-1", "XX")] * 2
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               [(1, 14), (1, -14), (3, 42), (3, -42)], rtol=1.2e-5)
    np.testing.assert_allclose([float(point[9]) for point in selected], start, rtol=0, atol=.00051)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 4
    print("flagged-integration: 4 matching points in the separate visible integrations", flush=True)


def compare_weighted_scalar_antenna(root, native, weighted, start, base):
    separate = root / "separate-antenna-spectra.ms"
    row_weights = root / "antenna-row-weights.ms"
    for path in (separate, row_weights):
        copy_ms(weighted, path)
        with opened(path, True) as table:
            if path == separate:
                table.putcol("SIGMA_SPECTRUM", np.ones_like(table.getcol("SIGMA_SPECTRUM")))
            else:
                table.removecols(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
                weights = np.where(table.getcol("ANTENNA2") == 2, 3, 1).astype(np.float32)
                table.putcol("WEIGHT", np.broadcast_to(weights, table.getcol("WEIGHT").shape).copy())
                table.putcol("SIGMA", np.ones_like(table.getcol("SIGMA")))

    abscissa = {"data": np.hypot(1, 4), "corrected": np.hypot(10, 14)}
    angles = {"data": np.degrees(np.arctan2(4, 1)), "corrected": np.degrees(np.arctan2(14, 10))}
    controls = base | dict(vis=str(separate), antenna="A0&A1;A0&A2", correlation="XX", spw="0:1",
                           xaxis="amp", yaxis="phase", scalar=True, title="Weighted scalar antenna averages")
    cases = (
        ("separate-spectra", separate, "data", "corrected"),
        ("observed-spectra", separate, "data", "data"),
        ("corrected-spectra", separate, "corrected", "corrected"),
        ("row-weights", row_weights, "data", "corrected"),
        ("observed-row-weights", row_weights, "data", "data"),
    )
    for name, vis, x_column, y_column in cases:
        request_args = controls | dict(vis=str(vis), xdatacolumn=x_column, ydatacolumn=y_column)
        amp = abscissa[x_column]
        phase = angles[y_column]
        coordinates = [(amp * 7 / 4, phase + 67.5), (amp, -phase), (2 * amp, -phase - 90)]
        compare_antenna_average_case(root, native, name, request_args, coordinates,
                                             [("0", "XX"), ("1", "XX"), ("2", "XX")], start)
        if name == "separate-spectra":
            compare_figure(root, native, request_args, root / f"{name}.casa.txt",
                           (controls["title"], "Amp", "Phase:corrected (degrees)"))

    request_args = controls | dict(vis=str(weighted), correlation="XX,XY")
    phase_xy = np.degrees(np.arctan2(16, 13))
    amp, angle = abscissa["data"], angles["corrected"]
    coordinates = [(amp * 7 / 4, angle + 67.5), (np.hypot(3, 5), phase_xy),
                   (amp, -angle), (np.hypot(3, 5), -phase_xy), (2 * amp, -angle - 90)]
    compare_antenna_average_case(root, native, "masked-spectrum", request_args, coordinates,
                                         [("0", "XX"), ("0", "XY"), ("1", "XX"), ("1", "XY"), ("2", "XX")], start)

    components = controls | dict(xaxis="real", yaxis="imag")
    component_coordinates = [(amp * 7 / 4, np.radians(angle + 67.5)), (amp, np.radians(-angle)),
                             (2 * amp, np.radians(-angle - 90))]
    compare_antenna_average_case(root, native, "packed-real-imag", components, component_coordinates,
                                         [("0", "XX"), ("1", "XX"), ("2", "XX")], start)

    for name, low, high, factor, offset in (
        ("zero-spectrum", 1, 0, 1, 0),
        ("signed-spectrum", 0, -3, 1.5, 45),
        ("small-spectrum", 1e-28, 3e-28, 7 / 4, 67.5),
    ):
        vis = root / f"{name}.ms"
        copy_ms(separate, vis)
        with opened(vis, True) as table:
            shape = table.getcol("WEIGHT_SPECTRUM").shape
            weights = np.where(table.getcol("ANTENNA2") == 2, high, low).astype(np.float32)
            table.putcol("WEIGHT_SPECTRUM", np.broadcast_to(weights, shape).copy())
        request_args = controls | dict(vis=str(vis))
        coordinates = [(amp * factor, angle + offset), (amp, -angle), (2 * amp, -angle - 90)]
        compare_antenna_average_case(root, native, name, request_args, coordinates,
                                             [("0", "XX"), ("1", "XX"), ("2", "XX")], start)


def compare_antenna_average_case(root, native, name, controls, coordinates, identifiers, start):
    expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[5], point[12]) for point in selected] == identifiers
    assert {point[6] for point in selected} == {"-1"}
    assert {(point[2], point[10]) for point in selected} == {("1", "100.001000000")}
    np.testing.assert_allclose([float(point[9]) for point in selected], start, rtol=0, atol=.00051)
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               coordinates, rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == len(coordinates)
    print(f"{name}: {len(coordinates)} matching antenna average coordinates", flush=True)


def compare_antenna_autocorrelation(root, native):
    source = root / "autocorrelation.ms"
    make_cal_ms(str(source), stokes="XX XY YX YY", autocorrelation_weight=1.0)
    casa.clearcal(str(source), addmodel=True)
    with opened(source, True) as table:
        shape = table.getcol("DATA").shape
        for column, values in (("DATA", (1, 3 + 5j, 3 - 5j, 7)),
                               ("CORRECTED_DATA", (10, 13 + 16j, 13 - 16j, 19))):
            array = np.asarray(values, dtype=np.complex64)[:, None, None]
            table.putcol(column, np.broadcast_to(array, shape).copy())
        table.putcol("FLAG", np.zeros(shape, dtype=bool))
        table.putcol("FLAG_ROW", np.zeros(shape[2], dtype=bool))
        start = table.getcol("TIME").min()
    base = request(source) | dict(antenna="A0&&&", avgantenna=True, spw="0:1",
                                  correlation="XY,YX", msselect=f"TIME < {start + 1:.3f}", scalar=True)
    for name, overrides, x, correlations in (
        ("scalar-autocorrelation", {}, np.hypot(3, 5), ("XY", "YX")),
        ("scalar-one-hand-autocorrelation", dict(correlation="XY"), np.hypot(3, 5), ("XY",)),
        ("vector-autocorrelation", dict(xaxis="real", yaxis="imag", scalar=False), 3, ("XY", "YX")),
    ):
        controls = base | overrides
        compare_antenna_average_case(root, native, name, controls,
                                             [(x, 0)] * len(correlations),
                                             [("0", label) for label in correlations], start)


def compare_channel_antenna_averages(root, native, weighted, start, base):
    source = root / "per-antenna-channel-averages.ms"
    copy_ms(weighted, source)
    with opened(source, True) as table:
        data = table.getcol("DATA")
        data[:, 0, :] *= -0.5
        table.putcol("DATA", data)
        table.putcol("SIGMA_SPECTRUM", np.ones_like(table.getcol("SIGMA_SPECTRUM")))
    row_weights = root / "per-antenna-channel-row-weights.ms"
    copy_ms(source, row_weights)
    with opened(row_weights, True) as table:
        table.removecols(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
        weights = np.where(table.getcol("ANTENNA2") == 2, 3, 1).astype(np.float32)
        table.putcol("WEIGHT", np.broadcast_to(weights, table.getcol("WEIGHT").shape).copy())
        table.putcol("SIGMA", np.ones_like(table.getcol("SIGMA")))
    split, signed, zero, flagged, flagged_rows = (root / f"channel-antenna-{name}.ms" for name in
                                                  ("split", "signed", "zero", "flagged", "flagged-rows"))
    for path in (split, signed, zero, flagged, flagged_rows):
        copy_ms(source, path)
        with opened(path, True) as table:
            if path in (flagged, flagged_rows):
                table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
                if path == flagged_rows:
                    table.putcol("FLAG_ROW", np.ones_like(table.getcol("FLAG_ROW")))
                continue
            weights = table.getcol("WEIGHT_SPECTRUM")
            distant = table.getcol("ANTENNA2") == 2
            if path == split:
                weights[0, 1, ~distant] = 2
                sigmas = table.getcol("SIGMA_SPECTRUM")
                sigmas[0, 1, ~distant] = .5
                table.putcol("SIGMA_SPECTRUM", sigmas)
            elif path == signed:
                weights[0, :2, distant] *= -1
            else:
                weights[0] = 0
            table.putcol("WEIGHT_SPECTRUM", weights)
    controls = base | dict(vis=str(source), antenna="A0&A1;A0&A2", correlation="XX", spw="0:0~3",
                           avgchannel="2", xaxis="amp", yaxis="phase", scalar=True,
                           title="Channel and per-antenna averages")
    amplitude = np.hypot(1, 4)
    phase = np.degrees(np.arctan2(14, 10))
    parallel = (
        ((amplitude * 7 / 16, phase + 67.5), (amplitude * 7 / 4, phase + 67.5)),
        ((amplitude / 4, -phase), (amplitude, -phase)),
        ((amplitude / 2, -phase - 90), (amplitude * 2, -phase - 90)),
    )
    amplitude_xy = np.hypot(3, 5)
    phase_xy = np.degrees(np.arctan2(16, 13))
    cross = (
        ((amplitude_xy * .7, phase_xy + 54), (amplitude_xy * 7 / 4, phase_xy + 67.5)),
        ((amplitude_xy / 4, -phase_xy), (amplitude_xy, -phase_xy)),
        ((amplitude_xy, -phase_xy - 90), (amplitude_xy * 2, -phase_xy - 90)),
    )
    ids = [(str(antenna), str(channel), "XX") for antenna in range(3) for channel in range(2)]
    values = [pair for antenna in parallel for pair in antenna]
    spectrum = (100.0005, 100.0025)
    masked_values = [pair for antenna in range(3) for channel in range(2)
                     for pair in (parallel[antenna][channel], cross[antenna][channel])]
    masked_ids = [(ant, channel, hand) for ant, channel, _ in ids for hand in ("XX", "XY")]
    cases = (
        ("scalar-channel-antenna", {}, values, ids),
        ("masked-channel-antenna", dict(correlation="XX,XY"), masked_values, masked_ids),
        ("row-weight-channel-antenna", dict(vis=str(row_weights), correlation="XX,XY"), masked_values, masked_ids),
        ("split-spectra-channel-antenna", dict(vis=str(split)),
         [(amplitude * 17 / 30, phase + 60), values[1], (amplitude * .7, -phase), *values[3:]], ids),
        ("signed-spectrum-channel-antenna", dict(vis=str(signed)),
         [(amplitude / 4, phase), *values[1:]], ids),
        ("vector-channel-antenna", dict(xaxis="real", yaxis="imag", scalar=False),
         [(-1.4375, 18.5), (-5.75, 18.5), (.25, -14), (1, -14), (-2, -20), (-8, -20)], ids),
        ("frequency-channel-antenna", dict(xaxis="frequency"),
         [(spectrum[int(channel)], y) for (_, channel, _), (_, y) in zip(ids, values, strict=True)], ids),
    )
    for name, overrides, coordinates, identifiers in cases:
        request_args = controls | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**request_args, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[5], point[2], point[12]) for point in selected] == identifiers
        assert {point[6] for point in selected} == {"-1"}
        assert [point[10] for point in selected] == [f"{spectrum[int(channel)]:.9f}" for _, channel, _ in identifiers]
        np.testing.assert_allclose([float(point[9]) for point in selected], start, rtol=0, atol=.00051)
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        assert native(**request_args, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        if request_args["xaxis"] == "frequency":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                       [x for x, _ in coordinates], rtol=0, atol=1e-7)
        print(f"{name}: {len(coordinates)} matching channel and per-antenna visibility coordinates", flush=True)
        if name == "scalar-channel-antenna":
            compare_figure(root, native, request_args, expected,
                           (controls["title"], "Amp", "Phase:corrected (degrees)"))
    for name, source in (("zero-calibrated-channel-antenna", zero),
                         ("all-flagged-channel-antenna", flagged),
                         ("row-flagged-channel-antenna", flagged_rows)):
        compare_empty_export(root, native, name, controls | dict(vis=str(source)),
                             ("Amp", "Phase:corrected (degrees)"))


def compare_scalar_antenna_time_averages(root, native, weighted, start, base):
    source = root / "scalar-antenna-times.ms"
    copy_ms(weighted, source)
    amplitudes = np.asarray([1, 2, 4, 1, 3, 7], dtype=np.float32)
    turns = np.asarray([0, 10, 20, -5, 15, 30], dtype=np.float32)
    with opened(source, True) as table:
        _, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        index = integration % len(amplitudes)
        rotations = amplitudes[index] * np.exp(1j * np.radians(turns[index]))
        for column in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(column) * rotations
            if column == "DATA":
                values[:, 0, :] *= -.5
            table.putcol(column, values.astype(np.complex64))
        table.putcol("SIGMA_SPECTRUM", np.ones_like(table.getcol("SIGMA_SPECTRUM")))
    masked = root / "scalar-antenna-flagged-time.ms"
    copy_ms(source, masked)
    with opened(masked, True) as table:
        times = table.getcol("TIME")
        flags = table.getcol("FLAG")
        flags[:, :, times == np.unique(times)[2]] = True
        table.putcol("FLAG", flags)
    row_flagged = root / "scalar-antenna-row-flagged-time.ms"
    row_only = root / "scalar-antenna-row-only-time.ms"
    for path, original in ((row_flagged, masked), (row_only, source)):
        copy_ms(original, path)
        with opened(path, True) as table:
            times = table.getcol("TIME")
            table.putcol("FLAG_ROW", times == np.unique(times)[2])
    controls = base | dict(vis=str(source), antenna="A0&A1;A0&A2", correlation="XX", spw="0:1",
                           avgtime="20", msselect="", xaxis="amp", yaxis="phase", scalar=True,
                           title="Scalar antenna time averages")
    phase = np.degrees(np.arctan2(14, 10))
    amplitude = np.hypot(1, 4)
    for name, overrides, width in (
        ("scalar-antenna-time", {}, 3),
        ("scalar-antenna-time-axis", dict(xaxis="time"), 3),
        ("scalar-antenna-time-channels", dict(avgchannel="2", spw="0:0~3"), 3),
        ("scalar-antenna-shorter-time", dict(avgtime="15"), 2),
        ("scalar-antenna-masked-time", dict(vis=str(masked)), 3),
        ("scalar-antenna-row-flagged-time", dict(vis=str(row_flagged)), 3),
        ("scalar-antenna-row-only-time", dict(vis=str(row_only)), 3),
        ("scalar-antenna-row-only-channels", dict(vis=str(row_only), avgchannel="2", spw="0:0~3"), 3),
    ):
        request_args = controls | overrides
        channels = (0, 1) if "avgchannel" in overrides else (1,)
        frequencies = (100.0005, 100.0025) if len(channels) == 2 else (100.001, 100.001)
        coordinates, identifiers, centers = [], [], []
        for begin in range(0, len(amplitudes), width):
            end = begin + width
            center = start + (begin + end - 1) * 5
            final_input_flagged = request_args["vis"] in {str(masked), str(row_flagged)} and begin == 0
            visible_end = end - int(final_input_flagged)
            mean_amplitude = amplitude * amplitudes[begin:visible_end].mean()
            mean_phase = phase + turns[begin:visible_end].mean()
            for antenna, (gain, angle) in enumerate(((7 / 4, mean_phase + 67.5),
                                                     (1, -mean_phase), (2, -mean_phase - 90))):
                for channel in channels:
                    x = center if request_args["xaxis"] == "time" else gain * mean_amplitude
                    if channel == 0:
                        x /= 4
                    coordinates.append((x, angle))
                    identifiers.append((str(antenna), str(channel), f"{frequencies[channel]:.9f}"))
                    centers.append(center)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**request_args, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[5], point[2], point[10]) for point in selected] == identifiers
        assert {(point[6], point[12]) for point in selected} == {("-1", "XX")}
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[9]) for point in selected], centers, rtol=0, atol=.00051)
        assert native(**request_args, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        if request_args["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], centers, rtol=0, atol=.00051)
            compare_figure(root, native, request_args, expected,
                           (controls["title"], "Time (MJD seconds)", "Phase:corrected (degrees)"))
        print(f"{name}: {len(coordinates)} matching scalar antenna time coordinates", flush=True)

    all_flagged = root / "scalar-antenna-all-flagged-time.ms"
    copy_ms(source, all_flagged)
    with opened(all_flagged, True) as table:
        table.putcol("FLAG", np.ones_like(table.getcol("FLAG")))
        table.putcol("FLAG_ROW", np.ones_like(table.getcol("FLAG_ROW")))
    compare_empty_export(root, native, "all-flagged-scalar-antenna-time",
                         controls | dict(vis=str(all_flagged)), ("Amp", "Phase:corrected (degrees)"))

    variable = root / "scalar-antenna-variable-rows.ms"
    copy_ms(row_only, variable)
    with opened(variable, True) as table:
        table.removecols(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
    mixed = make_mixed_polarization(variable, root / "scalar-antenna-mixed-polarization.ms",
                                    lambda table: table.getcol("FLAG_ROW"))
    compare_scalar_shape_refusal(root, native, controls | dict(vis=str(mixed), expformat="txt"))
    print("scalar-antenna-time: matching unequal-shape refusal in a row-flagged buffer", flush=True)
    compare_scalar_antenna_time_virtual_guard(root, native, source, controls)


def compare_scalar_antenna_time_virtual_guard(root, native, source, base):
    from casars.tasks import TaskExecutionError

    virtual = root / "scalar-antenna-stored-and-virtual.ms"
    copy_ms(source, virtual)
    with opened(virtual, True) as table:
        physical = np.full(table.getcol("MODEL_DATA").shape, 19, dtype=np.complex64)
        table.putcol("MODEL_DATA", physical)
    casa.setjy(str(virtual), field="FLUX", spw="0", standard="manual", usescratch=False,
               fluxdensity=[7.25, 0, 0, 0])
    request_args = base | dict(vis=str(virtual), xaxis="time", xdatacolumn="", yaxis="amp",
                               ydatacolumn="model", expformat="txt")
    expected, actual = (root / f"scalar-antenna-virtual.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**request_args, plotfile=str(expected)) is None
    assert len(rows(expected)) == 6
    assert {float(point[1]) for point in rows(expected)} == {7.25}
    try:
        native(**request_args, plotfile=str(actual))
    except TaskExecutionError as error:
        assert "PlotMS virtual MeasurementSet models are pending #1032" in str(error), error
    else:
        raise AssertionError("PlotMS used stored MODEL_DATA for an averaged virtual-model field")
    assert not actual.exists()
    with opened(virtual) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), physical)
    print("scalar-antenna-time: refusing the pending averaged virtual model", flush=True)


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
        compare_time_channel_baselines(root, native)
        scalar = root / "scalar-baselines"
        scalar.mkdir()
        compare_scalar_baselines(scalar, native)
        antennas = root / "antenna-averages"
        antennas.mkdir()
        compare_antenna_averaged_export(antennas, native)


if __name__ == "__main__":
    main()
