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
                           yaxis="amp", spw="0:0~2;5~7,1:0~2;5~7", avgchannel="2")
    expected = export(root, "disjoint-channels.casa", reference, controls)
    selected = rows(expected)
    frequencies = ["90.000500000", "90.005500000"] * 2
    assert [p[0] for p in selected] == [p[10] for p in selected] == frequencies
    np.testing.assert_allclose(coordinates(selected)[:, 1],
                               [abs(6.5 + 7.5j) * scale for scale in (1.5, 4.5)] * 2, rtol=1.2e-5)
    actual = export(root, "disjoint-channels.native", native, controls)
    assert compare_text(actual, expected) == 4
    assert [p[0] for p in rows(actual)] == frequencies
    print("disjoint-channels: 4 paired compressed samples at the physical frequency-bin centers", flush=True)


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("coordinates", "selection", "weights", "sparse", "disjoint", "malformed"))
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


if __name__ == "__main__":
    main()
