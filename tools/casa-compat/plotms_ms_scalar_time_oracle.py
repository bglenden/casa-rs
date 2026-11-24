#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public PlotMS scalar time averages with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_columns_oracle import (
    compare_empty_export,
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
    vis, integration = time_average_fixture(root)
    with opened(vis, True) as table:
        cycle = integration % 3
        shape = table.getcol("DATA").shape
        for name, amplitudes, phases in (
            ("DATA", (1, 3, 5), (170, -170, 90)),
            ("CORRECTED_DATA", (2, 4, 6), (90, -90, 179)),
            ("MODEL_DATA", (2, 4, 7), (60, -120, 150)),
        ):
            values = np.asarray(amplitudes) * np.exp(1j * np.deg2rad(phases))
            table.putcol(name, np.broadcast_to(values[cycle].astype(np.complex64), shape).copy())
        weight = table.getcol("WEIGHT")
        weight[0] = cycle + 1
        weight[1] = 10 - cycle
        table.putcol("WEIGHT", weight)
        start = table.getcol("TIME").min()
    return vis, integration, start


def compare(root, native):
    source, integration, start = make_fixture(root)
    cube_flags, row_flags = (root / f"{name}-flags.ms" for name in ("cube", "row"))
    copy_ms(source, cube_flags)
    copy_ms(source, row_flags)
    with opened(cube_flags, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, integration == 1] = True
        table.putcol("FLAG", flags)
    with opened(row_flags, True) as table:
        table.putcol("FLAG_ROW", integration == 2)
    spectra = root / "spectra.ms"
    copy_ms(source, spectra)
    with opened(spectra, True) as table:
        descriptor = table.getcoldesc("DATA")
        shape = table.getcol("DATA").shape
        for name, values in (("WEIGHT_SPECTRUM", [8, 1, 2]), ("SIGMA_SPECTRUM", [0.5, 1, 2])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            spectrum = np.asarray(values, dtype=np.float32)[integration % 3]
            table.putcol(name, np.broadcast_to(spectrum, shape).copy())
    controls = request(source) | dict(
        xaxis="amp", xdatacolumn="data", yaxis=["phase", "real"], ydatacolumn=["corrected", "model"],
        yaxislocation=["left", "right"], spw="0:1", avgtime="20", scalar=True,
    )
    amplitude = [(11 / 3, 74.5)] * 2 + [(11 / 3, 31 / 6)] * 2
    cases = (
        ("scalar-amplitude", {}, amplitude),
        ("scalar-phase", dict(xaxis="phase"), [(50 / 3, 74.5)] * 2 + [(50 / 3, 31 / 6)] * 2),
        ("scalar-phase-radians", dict(xaxis="imag"),
         [(np.deg2rad(50 / 3), 74.5)] * 2 + [(np.deg2rad(50 / 3), 31 / 6)] * 2),
        ("scalar-cube-flags", dict(vis=str(cube_flags)),
         [(4, 156.75), (11 / 3, 74.5), (4, 23 / 4), (11 / 3, 31 / 6)]),
        ("scalar-row-flags", dict(vis=str(row_flags)), amplitude),
        ("scalar-row-weight", dict(vis=str(spectra)), amplitude),
        ("scalar-time-axis", dict(xaxis="time"),
         [(start + offset, y) for y in (74.5, 31 / 6) for offset in (10, 40)]),
        ("scalar-single-ordinate", dict(yaxis="phase", ydatacolumn="corrected", yaxislocation="left"),
         amplitude[:2]),
    )
    for name, options, coordinates in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | options | dict(expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == len(coordinates)
        assert [point[9] for point in selected] == [f"{start + offset:.3f}" for offset in (10, 40)] * (len(selected) // 2)
        assert {tuple(point[5:9]) for point in selected} == {("0", "1", "A0@P0", "A1@P1")}
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   coordinates, rtol=1.2e-5)
        if options["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in selected],
                                       [point[0] for point in coordinates], rtol=0, atol=0.00051)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching scalar time coordinates", flush=True)
    compare_ordinate_figure(root, "scalar-amplitude", native, controls, "Amp",
                           ["Phase:corrected (degrees)", "Real:model"])
    compare_baseline_order(root, native, controls, source, start)


def compare_baseline_order(root, native, controls, source, start):
    different_interval = root / "nonanchor-interval.ms"
    copy_ms(source, different_interval)
    with opened(different_interval, True) as table:
        intervals = table.getcol("INTERVAL")
        nonanchor = (table.getcol("TIME") == start) & (table.getcol("ANTENNA2") == 2)
        intervals[nonanchor] = 30
        table.putcol("INTERVAL", intervals)
    options = controls | dict(antenna="A0&A1;A0&A2", spw="0:0~1", correlation="XX,YY", expformat="txt")
    expected_order = [(antenna, channel, poln) for antenna in ("1", "2")
                      for channel in ("0", "1") for poln in ("XX", "YY")]
    expected_times = [f"{start + offset:.3f}" for offset in (10, 40) for _ in expected_order]
    for name, vis in (("scalar-baseline-order", source), ("scalar-buffer-interval", different_interval)):
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**(options | dict(vis=str(vis))), plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 32
        assert [(point[6], point[2], point[12]) for point in selected] == expected_order * 4
        assert [point[9] for point in selected] == expected_times * 2
        assert native(**(options | dict(vis=str(vis))), plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching scalar time coordinates", flush=True)


def compare_corrected_residuals(root, native):
    source, integration, start = make_fixture(root)
    cube_flags, row_flags, unbounded, empty = (
        root / f"{name}.ms" for name in ("cube-flags", "row-flags", "unbounded", "empty")
    )
    for path in (cube_flags, row_flags, unbounded, empty):
        copy_ms(source, path)
    for path in (cube_flags, empty):
        with opened(path, True) as table:
            flags = table.getcol("FLAG")
            flags[0, 1] = True if path == empty else integration == 1
            table.putcol("FLAG", flags)
    with opened(row_flags, True) as table:
        table.putcol("FLAG_ROW", integration == 1)
    with opened(unbounded, True) as table:
        unit = np.ones_like(table.getcol("CORRECTED_DATA"))
        phase = np.exp(1j * np.deg2rad(170))
        table.putcol("CORRECTED_DATA", unit * phase)
        table.putcol("MODEL_DATA", unit * phase.conjugate())

    base = request(source) | dict(xaxis="time", xdatacolumn="", spw="0:1", avgtime="20",
                                  scalar=True, title="Scalar-time corrected residual")
    cases = (
        ("vector-amplitude", source, "amp", "corrected-model_vector", [-.5, -.5]),
        ("vector-phase", source, "phase", "corrected-model_vector", [29.5, 29.5]),
        ("scalar-phase", source, "phase", "corrected-model_scalar", [29.5, 29.5]),
        ("scalar-imaginary", source, "imag", "corrected-model_scalar", [np.deg2rad(29.5)] * 2),
        ("cube-flag-mask", cube_flags, "amp", "corrected-model_vector", [-.75, -.5]),
        ("row-flag-only", row_flags, "amp", "corrected-model_vector", [-.5, -.5]),
        ("unbounded-vector-phase", unbounded, "phase", "corrected-model_vector", [340., 340.]),
    )
    times = start + np.array([10., 40.])
    for name, vis, axis, column, values in cases:
        controls = base | dict(vis=str(vis), yaxis=axis, ydatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[2], point[12]) for point in selected] == [("1", "XX")] * 2
        np.testing.assert_allclose([float(point[0]) for point in selected], times, rtol=0, atol=.00051)
        np.testing.assert_allclose([float(point[1]) for point in selected], values, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 2
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)], times, rtol=0, atol=.00051)
        print(f"{name}: 2 matching corrected residual coordinates under scalar time averaging", flush=True)
        if name == "unbounded-vector-phase":
            compare_figure(root, native, controls, expected,
                           (controls["title"], "Time (MJD seconds)",
                            "Phase:corrected-model (vector) (degrees)"))

    controls = base | dict(vis=str(empty), yaxis="amp", ydatacolumn="corrected-model_vector")
    compare_empty_export(root, native, "all-cube-flags", controls,
                         ("Time (MJD seconds)", "Amp:corrected-model (vector)"))


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
        residual = root / "residual"
        residual.mkdir()
        compare_corrected_residuals(residual, native)


if __name__ == "__main__":
    main()
