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
from plotms_ms_scalar_time_oracle import compare_average_shape_refusal, make_mixed_polarization


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
    reverse_first_polarization(reduced, normalized)
    expected, actual = (root / f"selected-yy.{engine}.txt" for engine in ("casa", "native"))
    casa_controls = controls | dict(vis=str(normalized))
    assert reference(**casa_controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[5], point[6], point[12]) for point in selected] == [("-1", "-1", "YY")]
    np.testing.assert_allclose(np.array(selected[0][:2], dtype=float), [1.2, 64], rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 1
    print("selected-yy: 1 point matching the CASA control with aligned physical correlation slots", flush=True)


def reverse_first_polarization(source, output):
    output.parent.mkdir()
    copy_ms(source, output)
    with opened(output / "POLARIZATION", True) as pol:
        pol.putcell("CORR_TYPE", 0, pol.getcell("CORR_TYPE", 0)[::-1].copy())
        pol.putcell("CORR_PRODUCT", 0, pol.getcell("CORR_PRODUCT", 0)[:, ::-1].copy())
    with opened(output, True) as table:
        for row in map(int, np.flatnonzero(table.getcol("DATA_DESC_ID") == 0)):
            for column in ("DATA", "CORRECTED_DATA", "MODEL_DATA", "FLAG", "FLAG_CATEGORY", "WEIGHT", "SIGMA"):
                if table.iscelldefined(column, row):
                    table.putcell(column, row, table.getcell(column, row)[::-1].copy())


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
    copy_ms(source, variable)
    with opened(variable, True) as table:
        times = table.getcol("TIME")
        table.putcol("FLAG_ROW", times == np.unique(times)[4])
        table.removecols(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
    mixed = make_mixed_polarization(variable, root / "scalar-antenna-mixed-polarization.ms",
                                    lambda table: table.getcol("FLAG_ROW"))
    compare_average_shape_refusal(root, native, controls | dict(vis=str(mixed), expformat="txt"))
    print("scalar-antenna-time: matching unequal-shape refusal in a row-flagged buffer", flush=True)
    compare_scalar_antenna_time_virtual_guard(root, native, source, controls)
    compare_vector_antenna_time_averages(root, native, source, start, controls, amplitudes, turns)
    compare_antenna_scan_field_spans(root, native, source, start, controls, amplitudes, turns)


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


def compare_vector_antenna_time_averages(root, native, source, start, base, amplitudes, turns):
    controls = base | dict(vis=str(source), xaxis="real", yaxis="imag", scalar=False,
                           title="Vector antenna time averages")
    rotations = amplitudes.astype(float) * np.exp(1j * np.radians(turns.astype(float)))
    split = root / "vector-antenna-time-split-spectra.ms"
    copy_ms(source, split)
    with opened(split, True) as table:
        _, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        distant = table.getcol("ANTENNA2") == 2
        weights = table.getcol("WEIGHT_SPECTRUM")
        sigmas = table.getcol("SIGMA_SPECTRUM")
        weights[0, 1, distant & (integration == 1)] = 9
        sigmas[0, 1, distant & (integration == 0)] = 2
        table.putcol("WEIGHT_SPECTRUM", weights)
        table.putcol("SIGMA_SPECTRUM", sigmas)
    cases = (
        ("vector-antenna-time", {}, ((0, 4), (4, 6))),
        ("vector-antenna-time-axis", dict(xaxis="time"), ((0, 4), (4, 6))),
        ("vector-antenna-shorter-time", dict(avgtime="15"), ((0, 2), (2, 4), (4, 6))),
        ("vector-antenna-row-only", dict(vis=str(root / "scalar-antenna-row-only-time.ms")),
         ((0, 4), (4, 6))),
        ("vector-antenna-split-spectra", dict(vis=str(split)), ((0, 4), (4, 6))),
        ("vector-antenna-cube-flags", dict(vis=str(root / "scalar-antenna-flagged-time.ms")),
         ((0, 4), (4, 6))),
    )
    for name, overrides, groups in cases:
        request_args = controls | overrides
        coordinates, centers, identifiers = [], [], []
        for begin, end in groups:
            center = start + (begin + end - 1) * 5
            selected_rotations = rotations[begin:end]
            if name == "vector-antenna-cube-flags" and begin == 0:
                selected_rotations = selected_rotations[[0, 1, 3]]
            averaged_rotation = selected_rotations.mean()
            observed = (1 + 4j) * averaged_rotation
            corrected = (10 + 14j) * averaged_rotation
            antenna_visibility = [(observed * factor, corrected * factor) for factor in ((1 + 6j) / 4, 1, 2j)]
            if name == "vector-antenna-split-spectra" and begin == 0:
                # The distant baseline uses distinct per-column time weights.
                # The antenna stage weights both columns by the transformed 12 and 6.
                observed_rotation = np.average(
                    (np.average(rotations[:2], weights=(.25, 1)), rotations[2:4].mean()),
                    weights=(12, 6),
                )
                corrected_rotation = np.average(rotations[:4], weights=(3, 9, 3, 3))
                distant = ((1 + 4j) * 2j * observed_rotation, (10 + 14j) * 2j * corrected_rotation)
                antenna_visibility[0] = tuple(
                    (4 * near + 18 * far) / 22
                    for near, far in zip((observed, corrected), distant, strict=True)
                )
                antenna_visibility[2] = distant
            for antenna, (antenna_observed, antenna_corrected) in enumerate(antenna_visibility):
                x = center if request_args["xaxis"] == "time" else antenna_observed.real
                y = antenna_corrected.imag
                coordinates.append((x, y if antenna == 0 else -y))
                centers.append(center)
                identifiers.append((str(antenna), "-1", "XX"))
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**request_args, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[5], point[6], point[12]) for point in selected] == identifiers
        assert {(point[2], point[10]) for point in selected} == {("1", "100.001000000")}
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[9]) for point in selected], centers, rtol=0, atol=.00051)
        assert native(**request_args, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        if request_args["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], centers, rtol=0, atol=.00051)
            compare_figure(root, native, request_args, expected,
                           (controls["title"], "Time (MJD seconds)", "Imag:corrected"))
        print(f"{name}: {len(coordinates)} matching vector antenna time coordinates", flush=True)
    compare_empty_export(root, native, "all-flagged-vector-antenna-time",
                         controls | dict(vis=str(root / "scalar-antenna-all-flagged-time.ms")),
                         ("Real", "Imag:corrected"))
    compare_vector_antenna_time_order(root, native, source, controls, start)
    compare_vector_antenna_time_polarizations(root, native, source, controls, start)
    compare_vector_antenna_time_cancellation(root, native, source, controls, start)
    compare_vector_antenna_time_channels(root, native, source, controls, start, rotations)


def compare_vector_antenna_time_order(root, native, source, controls, start):
    reordered = root / "vector-antenna-time-reordered.ms"
    copy_ms(source, reordered)
    with opened(reordered, True) as table:
        times = table.getcol("TIME")
        first, second, third = np.unique(times)[:3]
        first_rows, second_rows, third_rows = (times == time for time in (first, second, third))
        times[second_rows] = third
        times[third_rows] = second
        table.putcol("TIME", times)
        table.putcol("TIME_CENTROID", times)
        for column, unit in (("DATA", 1), ("CORRECTED_DATA", 1j)):
            values = table.getcol(column)
            values[0, 1, first_rows] = 1e8 * unit
            values[0, 1, second_rows] = -1e8 * unit
            values[0, 1, third_rows] = unit
            table.putcol(column, values)
    args = controls | dict(vis=str(reordered), antenna="A0&A1", avgtime="40",
                           msselect=f"TIME < {start + 21:.3f}", expformat="txt")
    expected, actual = (root / f"vector-antenna-time-order.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**args, plotfile=str(expected)) is None
    selected = rows(expected)
    assert [point[5:7] for point in selected] == [["0", "-1"], ["1", "-1"]]
    # The chronological Float sums cancel. Physical row order would retain 1/3.
    np.testing.assert_array_equal(np.array([point[:2] for point in selected], dtype=float), 0)
    np.testing.assert_allclose([float(point[9]) for point in selected], start + 10, rtol=0, atol=.00051)
    assert native(**args, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2
    print("vector-antenna-time-order: 2 matching chronological cancellation coordinates", flush=True)


def compare_vector_antenna_time_polarizations(root, native, source, controls, start):
    from grpc import RpcError, StatusCode

    selected = root / "vector-antenna-time-polarizations"
    selected.mkdir()
    fixture = selected / "mixed-source.ms"
    copy_ms(source, fixture)
    with opened(fixture, True) as table:
        table.removecols(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
        for column in ("WEIGHT", "SIGMA"):
            table.putcol(column, np.ones_like(table.getcol(column)))
        distant = table.getcol("ANTENNA2") == 2
        for column, hand in (("DATA", 1), ("CORRECTED_DATA", 2j)):
            cube = table.getcol(column)
            cube[1, 1, :], cube[2, 1, :] = hand, 3 * hand
            cube[3, 1, :] = np.where(distant, 3 * hand, hand)
            table.putcol(column, cube)

    args = controls | dict(vis=str(fixture), antenna="A0&A1", msselect=f"TIME < {start + 1:.3f}")
    for name, correlation, hands in (("crosshands", "XY,YX", ("XY", "YX")), ("one-crosshand", "XY", ("XY",))):
        request_args = args | dict(correlation=correlation, expformat="txt")
        expected, actual = (selected / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**request_args, plotfile=str(expected)) is None
        points = rows(expected)
        assert [(point[5], point[6], point[12]) for point in points] == [
            (str(antenna), "-1", hand) for antenna in (0, 1) for hand in hands
        ]
        coordinates = [(value, sign * value * 2) for sign in (1, -1) for value in (1, 3)[:len(hands)]]
        np.testing.assert_allclose(np.array([point[:2] for point in points], dtype=float), coordinates)
        assert native(**request_args, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(points)
        print(f"{name}: {len(points)} matching vector antenna time coordinates", flush=True)

    mixed = make_mixed_polarization(fixture, selected / "mixed.ms",
                                    lambda table: table.getcol("ANTENNA2") == 2, correlation_slot=3)
    args |= dict(vis=str(mixed), antenna="A0&A1;A0&A2", expformat="txt")
    compare_average_shape_refusal(selected, native, args)
    print("vector-antenna-time: matching within-bin shape refusal", flush=True)
    rejected = selected / "different-physical-slots.casa.txt"
    try:
        reference(**(args | dict(correlation="YY")), plotfile=str(rejected))
    except RpcError as error:
        assert error.code() == StatusCode.INVALID_ARGUMENT, error
    else:
        raise AssertionError("CASA accepted YY at different physical correlation slots")
    assert not rejected.exists()
    normalized = selected / "matching-slots" / mixed.name
    reverse_first_polarization(mixed, normalized)
    expected, actual = (selected / f"selected-yy.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**(args | dict(vis=str(normalized), correlation="YY")), plotfile=str(expected)) is None
    points = rows(expected)
    assert [(point[5], point[6], point[12]) for point in points] == [(str(a), "-1", "YY") for a in range(3)]
    np.testing.assert_allclose(np.array([point[:2] for point in points], dtype=float), [(2, 4), (1, -2), (3, -6)])
    assert native(**(args | dict(correlation="YY")), plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 3
    print("vector-antenna-time: 3 YY coordinates match the aligned-slot CASA control", flush=True)


def compare_vector_antenna_time_cancellation(root, native, source, controls, start):
    cancelled = root / "vector-antenna-cancellation.ms"
    copy_ms(source, cancelled)
    with opened(cancelled, True) as table:
        antenna2 = table.getcol("ANTENNA2")
        for column in ("DATA", "CORRECTED_DATA"):
            values = table.getcol(column)
            for antenna, value in ((1, 1e8 + 1j), (2, 1), (3, -1e8)):
                values[0, 1, antenna2 == antenna] = value
            table.putcol(column, values)
        table.putcol("WEIGHT_SPECTRUM", np.ones_like(table.getcol("WEIGHT_SPECTRUM")))
    args = controls | dict(vis=str(cancelled), antenna="A0&A1;A0&A2;A0&A3", yaxis="phase",
                           msselect=f"TIME < {start + 1:.3f}", expformat="txt")
    expected, actual = (root / f"vector-antenna-cancellation.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**args, plotfile=str(expected)) is None
    points = rows(expected)
    assert [point[5:7] for point in points] == [[str(a), "-1"] for a in range(4)]
    # Summing the three baselines in Float cancels the real component at antenna 0.
    assert np.array_equal(np.array(points[0][:2], dtype=float), [0, 90])
    assert native(**args, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 4
    print("vector-antenna-cancellation: 4 matching second-stage Float coordinates", flush=True)


def compare_vector_antenna_time_channels(root, native, source, controls, start, rotations):
    split = root / "vector-antenna-time-channel-spectra.ms"
    copy_ms(source, split)
    with opened(split, True) as table:
        distant = table.getcol("ANTENNA2") == 2
        weights = table.getcol("WEIGHT_SPECTRUM")
        sigmas = table.getcol("SIGMA_SPECTRUM")
        weights[0, 0, distant] = 15
        sigmas[0, 0, distant] = 2
        table.putcol("WEIGHT_SPECTRUM", weights)
        table.putcol("SIGMA_SPECTRUM", sigmas)

    zero_observed = root / "vector-time-channel-zero-observed.ms"
    zero_corrected = root / "vector-time-channel-zero-corrected.ms"
    for path, column in ((zero_observed, "SIGMA_SPECTRUM"), (zero_corrected, "WEIGHT_SPECTRUM")):
        copy_ms(source, path)
        with opened(path, True) as table:
            weights = table.getcol(column)
            weights[:, :4, :] = 0
            table.putcol(column, weights)

    args = controls | dict(avgchannel="2", spw="0:0~3", title="Vector time, channel and antenna averages")
    cases = (
        ("vector-time-channel-antenna", {}),
        ("vector-time-channel-antenna-frequency", dict(xaxis="frequency")),
        ("vector-time-channel-antenna-spectra", dict(vis=str(split))),
        ("vector-time-channel-antenna-zero-observed", dict(vis=str(zero_observed))),
    )
    for name, overrides in cases:
        request_args = args | overrides
        coordinates, centers, identifiers, frequencies = [], [], [], []
        for begin, end in ((0, 4), (4, 6)):
            center = start + (begin + end - 1) * 5
            rotation = rotations[begin:end].mean()
            observed = 0j if name.endswith("-zero-observed") else (1 + 4j) * rotation
            corrected = (10 + 14j) * rotation
            for antenna in range(3):
                for channel, frequency in enumerate((100.0005, 100.0025)):
                    near_observed = observed * (1 if channel else .25)
                    far_observed = near_observed * 2j
                    ratio = 3
                    if name.endswith("-spectra") and channel == 0:
                        far_observed = observed * .7 * 2j
                        ratio = 9
                    x, y = (
                        ((near_observed + ratio * far_observed) / (1 + ratio),
                         corrected * (1 + ratio * 2j) / (1 + ratio)),
                        (near_observed, corrected.conjugate()),
                        (far_observed, (corrected * 2j).conjugate()),
                    )[antenna]
                    coordinates.append((frequency if request_args["xaxis"] == "frequency" else x.real, y.imag))
                    centers.append(center)
                    identifiers.append((str(antenna), "-1", str(channel), "XX"))
                    frequencies.append(f"{frequency:.9f}")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**request_args, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[5], point[6], point[2], point[12]) for point in selected] == identifiers
        assert [point[10] for point in selected] == frequencies
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[9]) for point in selected], centers, rtol=0, atol=.00051)
        assert native(**request_args, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        if request_args["xaxis"] == "frequency":
            compare_figure(root, native, request_args, expected, (args["title"], "Frequency (GHz)", "Imag:corrected"))
        print(f"{name}: {len(coordinates)} matching vector time, channel and antenna coordinates", flush=True)
    compare_empty_export(root, native, "vector-time-channel-antenna-zero-corrected",
                         args | dict(vis=str(zero_corrected)), ("Real", "Imag:corrected"))
    compare_vector_antenna_time_channel_model(root, native, source, args, start)
    compare_vector_antenna_time_channel_observed_spectrum(root, native, source, args, start)


def compare_vector_antenna_time_channel_model(root, native, source, base, start):
    vis = root / "vector-antenna-time-channel-model.ms"
    copy_ms(source, vis)
    with opened(vis, True) as table:
        far = table.getcol("ANTENNA2") == 2
        values = table.getcol("MODEL_DATA")
        for channel, value in enumerate((2 + 2j, 4 + 4j, 8 + 8j, 10 + 10j)):
            values[:, channel, :] = value * np.where(far, 3, 1)
        table.putcol("MODEL_DATA", values)
        flags = table.getcol("FLAG")
        flags[0, 0, far & (table.getcol("TIME") == start)] = True
        table.putcol("FLAG", flags)
    controls = base | dict(vis=str(vis), xdatacolumn="model", ydatacolumn="model", expformat="txt")
    for name, overrides, values, frequencies in (
        ("model", {}, [(5.8, 18), (-3, -9), (-9, -27), (6, 18), (-3, -9), (-9, -27)], (100.0005, 100.0025)),
        ("model-single-channel", dict(spw="0:1"), [(8,), (-4,), (-12,)] * 2, (100.001,)),
    ):
        # The model uses equal channel weights. The antenna reducer uses the
        # surviving time counts, including 8 and 7 in the first two-channel bin.
        coordinates = [(abs(value), value) for channels in values for value in channels]
        expected, actual = (root / f"vector-time-channel-antenna-{name}.{engine}.txt" for engine in ("casa", "native"))
        args = controls | overrides
        assert reference(**args, plotfile=str(expected)) is None
        selected = rows(expected)
        identifiers = [(str(antenna), "-1", str(channel), "XX")
                       for _ in range(2) for antenna in range(3) for channel in range(len(frequencies))]
        assert [(point[5], point[6], point[2], point[12]) for point in selected] == identifiers
        assert [point[10] for point in selected] == [f"{frequency:.9f}" for frequency in frequencies] * 6
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float), coordinates, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[9]) for point in selected],
                                   np.repeat([start + 15, start + 45], 3 * len(frequencies)), rtol=0, atol=.00051)
        assert native(**args, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(coordinates)
        print(f"vector-time-channel-antenna-{name}: {len(coordinates)} matching model coordinates", flush=True)


def compare_vector_antenna_time_channel_observed_spectrum(root, native, source, base, start):
    tiny = root / "vector-antenna-time-channel-tiny-observed.ms"
    copy_ms(source, tiny)
    with opened(tiny, True) as table:
        far = table.getcol("ANTENNA2") == 2
        data = table.getcol("DATA")
        data[:, :2, :] = np.where(far, 3 + 3j, 1 + 1j)
        table.putcol("DATA", data)
        sigmas = table.getcol("SIGMA_SPECTRUM")
        sigmas[:, :2, :] = 2.0**62
        sigmas[0, 0, ~far] = 2.0**63
        table.putcol("SIGMA_SPECTRUM", sigmas)
    args = base | dict(vis=str(tiny), spw="0:0~1", ydatacolumn="data",
                       msselect=f"TIME < {start + 1:.3f}", expformat="txt")
    time_only = args | dict(avgantenna=False, avgchannel="", xaxis="wtsp")
    spectrum = root / "tiny-observed-time-weights.casa.txt"
    assert reference(**time_only, plotfile=str(spectrum)) is None
    np.testing.assert_allclose([float(point[0]) for point in rows(spectrum)],
                               np.finfo(np.float32).tiny * np.array([1, 4, 4, 4]), rtol=1.2e-5, atol=0)
    hidden = root / "tiny-observed-underflow.casa.txt"
    assert reference(**args, plotfile=str(hidden)) is None
    assert not rows(hidden) and "# No data to export" in hidden.read_text()

    # Scale each sigma by the same factor to bypass the CASA channel kernel's underflow.
    scaled = root / "rescaled-observed-sigmas" / tiny.name
    scaled.parent.mkdir()
    copy_ms(tiny, scaled)
    with opened(scaled, True) as table:
        table.putcol("SIGMA_SPECTRUM", table.getcol("SIGMA_SPECTRUM") / np.float32(2.0**60))
    reference_file, actual = (root / f"tiny-observed-{engine}.txt" for engine in ("scaled-casa", "native"))
    assert reference(**(args | dict(vis=str(scaled))), plotfile=str(reference_file)) is None
    means = np.array([point[:2] for point in rows(reference_file)], dtype=float)
    np.testing.assert_allclose(means, [(29 / 13, 29 / 13), (1, -1), (3, -3)], rtol=1.2e-5)
    assert native(**args, plotfile=str(actual)) is None
    assert compare_text(actual, reference_file) == 3
    print("tiny-observed: 3 matching coordinates against the rescaled CASA control", flush=True)


def compare_antenna_scan_field_spans(root, native, source, start, base, amplitudes, turns):
    selections = {}
    scan_and_field = (("SCAN_NUMBER", 7), ("FIELD_ID", 1))
    for name, columns in (
        ("scan", (("SCAN_NUMBER", 7),)),
        ("field", (("FIELD_ID", 1),)),
        ("both", scan_and_field),
        ("masked", scan_and_field),
        ("early-boundary", scan_and_field),
    ):
        vis = root / f"antenna-{name}-span.ms"
        copy_ms(source, vis)
        with opened(vis, True) as table:
            times = table.getcol("TIME")
            boundary = 19 if name == "early-boundary" else 29
            late = (times >= start + boundary) & (times <= start + 51)
            for column, value in columns:
                ids = table.getcol(column)
                ids[late] = value
                table.putcol(column, ids)
            if name == "masked":
                flags = table.getcol("FLAG")
                flags[:, :, times == np.unique(times)[2]] = True
                table.putcol("FLAG", flags)
        selections[name] = vis

    rotations = amplitudes.astype(float) * np.exp(1j * np.radians(turns.astype(float)))
    controls = base | dict(avgtime="80", field="0,1", msselect=f"TIME < {start + 51:.3f}",
                           title="Per-antenna scan and field spans")
    for scalar in (True, False):
        mode = "scalar" if scalar else "vector"
        cases = (
            ("scan-span", "scan", dict(avgscan=True), ((0, 6, -1 if scalar else 1, 0),)),
            ("field-span", "field", dict(avgfield=True), ((0, 6, 1, 0),)),
            ("both-spans", "both", dict(avgscan=True, avgfield=True), ((0, 6, -1 if scalar else 1, 0),)),
            ("scan-barrier", "both", dict(avgfield=True), ((0, 3, 1, 0), (3, 6, 7, 1))),
            ("field-barrier", "both", dict(avgscan=True), ((0, 3, 1, 0), (3, 6, 7, 1))),
            ("time-axis", "both", dict(avgscan=True, avgfield=True, xaxis="time"),
             ((0, 6, -1 if scalar else 1, 0),)),
            ("masked-spans", "masked", dict(avgscan=True, avgfield=True), ((0, 6, -1 if scalar else 1, 0),)),
            ("channel-spans", "both", dict(avgscan=True, avgfield=True, avgchannel="2", spw="0:0~3"),
             ((0, 6, -1 if scalar else 1, 0),)),
        )
        if not scalar:
            cases += (("shorter-spans", "early-boundary", dict(avgscan=True, avgfield=True, avgtime="20"),
                       ((0, 4, -1, 0), (4, 6, 7, 1))),)
        for name, selection, overrides, groups in cases:
            args = controls | overrides | dict(vis=str(selections[selection]), scalar=scalar)
            channels = ((0, "100.000500000", .25), (1, "100.002500000", 1)) if "avgchannel" in args else (
                (1, "100.001000000", 1),
            )
            coordinates, metadata, centers, spectral_coordinates = [], [], [], []
            for begin, end, scan, field in groups:
                center = start + 5 * (begin + end - 1)
                samples = np.arange(begin, end)
                if selection == "masked":
                    samples = samples[samples != 2]
                if scalar:
                    amplitude = np.hypot(1, 4) * amplitudes[samples].mean()
                    angle = np.angle(10 + 14j, deg=True) + turns[samples].mean()
                    visibilities = ((1.75 * amplitude, angle + 67.5), (amplitude, -angle),
                                    (2 * amplitude, -angle - 90))
                else:
                    rotation = rotations[samples].mean()
                    visibilities = [
                        (abs((1 + 4j) * rotation * gain),
                         direction * np.angle((10 + 14j) * rotation * gain, deg=True))
                        for gain, direction in (((1 + 6j) / 4, 1), (1, -1), (2j, -1))
                    ]
                for antenna, (amplitude, angle) in enumerate(visibilities):
                    for channel, frequency, gain in channels:
                        coordinates.append((center if args["xaxis"] == "time" else amplitude * gain, angle))
                        metadata.append([str(scan), str(field), str(antenna), "-1"])
                        centers.append(center)
                        spectral_coordinates.append((str(channel), frequency, "XX"))
            expected, actual = (root / f"{mode}-antenna-{name}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**args, plotfile=str(expected), expformat="txt") is None
            selected = rows(expected)
            assert [point[3:7] for point in selected] == metadata
            assert [(point[2], point[10], point[12]) for point in selected] == spectral_coordinates
            np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                       coordinates, rtol=1.2e-5)
            np.testing.assert_allclose([float(point[9]) for point in selected], centers, rtol=0, atol=.00051)
            assert native(**args, plotfile=str(actual), expformat="txt") is None
            assert compare_text(actual, expected) == len(coordinates)
            if name == "time-axis":
                np.testing.assert_allclose([float(point[0]) for point in rows(actual)], centers, rtol=0, atol=.00051)
                compare_figure(root, native, args, expected,
                               (controls["title"], "Time (MJD seconds)", "Phase:corrected (degrees)"))
            print(f"{mode}-antenna-{name}: {len(coordinates)} matching span coordinates", flush=True)


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
