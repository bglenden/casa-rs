#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare visibility residuals and ratios after vector time averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import (
    casa, compare_empty_export, compare_figure, compare_text, copy_ms, opened, plotms_launcher,
    reference, request, rows,
)
from plotms_ms_derived_average_oracle import weighted_fixture


def prepare_time_fixture(root):
    vis = weighted_fixture(root)
    with opened(vis, True) as table:
        times = table.getcol("TIME")
        integration = np.rint((times - times.min()) / 10).astype(int)
        for column, step in (("DATA", 1 + 2j), ("CORRECTED_DATA", 3 - 1j), ("MODEL_DATA", .5 + .25j)):
            table.putcol(column, table.getcol(column) + step * integration)
        weights = table.getcol("WEIGHT_SPECTRUM")
        weights[0, 1] = np.where(integration % 2 == 0, 1, 3)
        table.putcol("WEIGHT_SPECTRUM", weights)
        # FLAG_ROW alone does not exclude a clear visibility cube cell.
        table.putcol("FLAG_ROW", integration % 2 == 0)
    return vis, integration, times.min()


def compare_time_derived(root, native):
    vis, integration, start = prepare_time_fixture(root)
    masked = root / "masked-time.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1] = np.isin(integration, [3, 4, 5])
        table.putcol("FLAG", flags)

    base = request(vis) | dict(xaxis="time", xdatacolumn="", yaxis="real", spw="0:1", avgtime="20",
                               title="Time averaged visibility")
    cases = (
        ("residual", vis, "corrected-model_vector", "real", [0, 1, 2], [.75, 2.75, 4.75]),
        ("ratio", vis, "corrected/model_vector", "phase", [0, 1, 2], [.75, 2.75, 4.75]),
        ("masked-ratio", masked, "corrected/model_vector", "real", [0, 1], [.75, 2.0]),
    )
    for name, source, column, axis, bins, mean_integrations in cases:
        controls = base | dict(vis=str(source), yaxis=axis, ydatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[2], point[12]) for point in selected] == [("1", "XX")] * len(bins)
        times = start + 5 + 20 * np.array(bins)
        if name == "masked-ratio":
            # Only the first integration of the second bin remains clear.
            times[1] -= 5
        assert [point[0] for point in selected] == [point[9] for point in selected]
        np.testing.assert_allclose([float(point[0]) for point in selected], times, rtol=0, atol=.00051)
        offsets = np.asarray(mean_integrations)
        corrected = 8 - 1j + (3 - 1j) * offsets
        model = 2 + 2j + (.5 + .25j) * offsets
        visibility = corrected - model if name == "residual" else corrected / model
        projected = np.angle(visibility, deg=True) if axis == "phase" else visibility.real
        np.testing.assert_allclose([float(point[1]) for point in selected], projected, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                   [float(point[0]) for point in selected], rtol=0, atol=.00051)
        print(f"{name}: {len(selected)} matching time-averaged coordinates", flush=True)
        if name == "masked-ratio":
            compare_figure(root, native, controls, expected,
                           (controls["title"], "Time (MJD seconds)", "Real:corrected/model (vector)"))

    fully_flagged = root / "fully-flagged-time.ms"
    copy_ms(vis, fully_flagged)
    with opened(fully_flagged, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1] = True
        table.putcol("FLAG", flags)
    empty = base | dict(vis=str(fully_flagged), ydatacolumn="corrected/model_vector")
    compare_empty_export(root, native, "all-flagged-time", empty,
                         ("Time (MJD seconds)", "Real:corrected/model (vector)"))
    compare_buffer_time(root, native, base, integration, start)


def compare_buffer_time(root, native, base, integration, start):
    source = root / "two-baseline-time.ms"
    copy_ms(Path(base["vis"]), source)
    with opened(source, True) as table:
        flags = table.getcol("FLAG")
        antenna2 = table.getcol("ANTENNA2")
        flags[0, 1] |= ((antenna2 == 1) & np.isin(integration, [2, 3])
                        | (antenna2 == 2) & np.isin(integration, [1, 3]))
        table.putcol("FLAG", flags)
    controls = base | dict(vis=str(source), antenna="A0&A1;A0&A2",
                           ydatacolumn="corrected/model_vector", expformat="txt")
    expected, actual = (root / f"buffer-time.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    selected = rows(expected)
    assert [point[6] for point in selected] == ["1", "2", "2", "1", "2"]
    shared_times = start + np.array([5, 5, 25, 45, 45])
    offsets = np.array([.75, 0, 2, 4.75, 4.75])
    corrected = 8 - 1j + (3 - 1j) * offsets
    model = 2 + 2j + (.5 + .25j) * offsets
    np.testing.assert_allclose([float(point[0]) for point in selected],
                               shared_times, rtol=0, atol=.00051)
    np.testing.assert_allclose([float(point[1]) for point in selected],
                               (corrected / model).real, rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 5
    np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                               shared_times, rtol=0, atol=.00051)
    print("buffer-time: 5 matching points sharing each output buffer's first baseline time", flush=True)


def compare_observed_time_derived(root, native):
    vis, integration, start = prepare_time_fixture(root)
    with opened(vis, True) as table:
        sigma = table.getcol("SIGMA_SPECTRUM")
        sigma[0, 1] = np.where(integration % 2 == 0, 1, .5)
        table.putcol("SIGMA_SPECTRUM", sigma)
    fallback = root / "without-corrected.ms"
    copy_ms(vis, fallback)
    with opened(fallback, True) as table:
        table.removecols(["CORRECTED_DATA"])
    row_weights = root / "without-spectra.ms"
    copy_ms(vis, row_weights)
    with opened(row_weights, True) as table:
        for row_column, spectrum in (("WEIGHT", "WEIGHT_SPECTRUM"), ("SIGMA", "SIGMA_SPECTRUM")):
            table.putcol(row_column, table.getcol(spectrum)[:, 1, :])
            table.removecols([spectrum])

    offset = np.arange(0, 6, 2)
    observed = 3 + 6j + (1 + 2j) * (offset + .8)
    calibrated_model = 2 + 2j + (.5 + .25j) * (offset + .75)
    observed_model = 2 + 2j + (.5 + .25j) * (offset + .8)
    times = start + 5 + 10 * offset
    base = request(vis) | dict(xaxis="time", xdatacolumn="", spw="0:1", avgtime="20",
                               title="Observed time averages")
    for name, source, column, axis, coordinates in (
        ("observed-residual", vis, "data-model_vector", "real", (observed - calibrated_model).real),
        ("observed-ratio", vis, "data/model_vector", "phase", np.angle(observed / calibrated_model, deg=True)),
        ("without-spectra", row_weights, "data-model_vector", "real", (observed - calibrated_model).real),
        ("without-corrected", fallback, "data-model_vector", "real", (observed - observed_model).real),
        ("corrected-fallback", fallback, "corrected-model_vector", "real", (observed - observed_model).real),
    ):
        controls = base | dict(vis=str(source), yaxis=axis, ydatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[2], point[12]) for point in selected] == [("1", "XX")] * 3
        np.testing.assert_allclose([float(point[1]) for point in selected], coordinates, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[0]) for point in selected], times, rtol=0, atol=.00051)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 3
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)], times, rtol=0, atol=.00051)
        print(f"{name}: 3 matching coordinates with independently averaged operands", flush=True)
        if name == "observed-ratio":
            compare_figure(root, native, controls, expected,
                           (controls["title"], "Time (MJD seconds)", "Phase:data/model (vector) (degrees)"))


def compare_scalar_expressions(root, native):
    vis, integration, start = prepare_time_fixture(root)
    with opened(vis, True) as table:
        sigma = table.getcol("SIGMA_SPECTRUM")
        sigma[0, 1] = np.where(integration % 2 == 0, 1, .5)
        table.putcol("SIGMA_SPECTRUM", sigma)
    rounded = root / "rounded-amplitudes.ms"
    copy_ms(vis, rounded)
    with opened(rounded, True) as table:
        corrected = np.ones_like(table.getcol("CORRECTED_DATA"))
        table.putcol("CORRECTED_DATA", corrected)
        table.putcol("MODEL_DATA", corrected * (1 + (2**-13) * 1j))

    wrapped = root / "phase-wrap.ms"
    copy_ms(vis, wrapped)
    with opened(wrapped, True) as table:
        unit = np.ones_like(table.getcol("DATA"))
        table.putcol("DATA", unit * (-1 + .1j))
        table.putcol("MODEL_DATA", unit * (-1 - .1j))
    wrapped_phase = np.degrees(np.angle(-1 + .1j) - np.angle(-1 - .1j))

    offset = np.arange(0, 6, 2)
    times = start + 5 + 10 * offset
    observed = 3 + 6j + (1 + 2j) * (offset + .8)
    corrected = 8 - 1j + (3 - 1j) * (offset + .75)
    model = 2 + 2j + (.5 + .25j) * (offset + .75)
    base = request(vis) | dict(xaxis="time", xdatacolumn="", spw="0:1", avgtime="20",
                               title="Scalar model expressions after time averaging")
    for name, source, column, axis, values in (
        ("corrected-scalar-residual", vis, "corrected-model_scalar", "amp", np.abs(corrected) - np.abs(model)),
        ("corrected-scalar-ratio", vis, "corrected/model_scalar", "phase", np.degrees(np.angle(corrected) / np.abs(model))),
        ("observed-scalar-residual", vis, "data-model_scalar", "phase", np.degrees(np.angle(observed) - np.angle(model))),
        ("observed-scalar-ratio", vis, "data/model_scalar", "real", (observed - model).real),
        ("rounded-amplitudes", rounded, "corrected-model_scalar", "amp", np.zeros(3)),
        ("phase-wrap", wrapped, "data-model_scalar", "phase", np.full(3, wrapped_phase)),
    ):
        controls = base | dict(vis=str(source), yaxis=axis, ydatacolumn=column)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[2], point[12]) for point in selected] == [("1", "XX")] * 3
        np.testing.assert_allclose([float(point[1]) for point in selected], values, rtol=1.2e-5)
        np.testing.assert_allclose([float(point[0]) for point in selected], times, rtol=0, atol=.00051)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 3
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)], times, rtol=0, atol=.00051)
        if name == "rounded-amplitudes":
            np.testing.assert_array_equal([float(point[1]) for point in rows(actual)], values)
        print(f"{name}: 3 matching scalar coordinates after vector time averaging", flush=True)
        if name == "corrected-scalar-ratio":
            compare_figure(root, native, controls, expected,
                           (controls["title"], "Time (MJD seconds)", "Phase:corrected/model (scalar) (degrees)"))
        if name == "phase-wrap":
            compare_figure(root, native, controls, expected,
                           (controls["title"], "Time (MJD seconds)", "Phase:data-model (scalar) (degrees)"))


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
        corrected = root / "corrected"
        corrected.mkdir()
        compare_time_derived(corrected, native)
        observed = root / "observed"
        observed.mkdir()
        compare_observed_time_derived(observed, native)
        scalar = root / "scalar"
        scalar.mkdir()
        compare_scalar_expressions(scalar, native)


if __name__ == "__main__":
    main()
