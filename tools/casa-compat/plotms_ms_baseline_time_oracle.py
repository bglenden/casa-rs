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
    compare_figure,
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
    channels = dict(spw="0:0~3", avgchannel="2")
    cases = (
        ("independent", {}, pairs, times),
        ("time-axis", dict(xaxis="time"), list(zip(times, phases, strict=True)), times),
        ("cube-flags", dict(vis=str(flagged)), flagged_pairs, times),
        ("signed-residual", dict(yaxis="amp", ydatacolumn="corrected-model_vector"),
         [(amp, -90 * amp) for amp in amplitudes], times),
        ("avgtime", dict(avgtime="20"), avgtime_pairs, avgtime_times),
        ("avgchannel", channels, [pair for pair in pairs for _ in range(2)], np.repeat(times, 2)),
        ("avgtime-avgchannel", channels | dict(avgtime="20"),
         [pair for pair in avgtime_pairs for _ in range(2)], np.repeat(avgtime_times, 2)),
    )
    for name, overrides, coordinates, centers in cases:
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == len(coordinates)
        assert {tuple(point[3:9]) for point in selected} == {("1", "0", "-1", "-1", "*", "*")}
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
        print(f"{name}: {len(coordinates)} matching scalar baseline coordinates", flush=True)
        if name == "independent":
            compare_figure(root, native, controls, expected, (base["title"], "Amp", "Phase:corrected (degrees)"))


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


if __name__ == "__main__":
    main()
