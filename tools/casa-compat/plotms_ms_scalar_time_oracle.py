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
    time_scan_field_fixture,
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


def attach_spectra(vis, integration):
    with opened(vis, True) as table:
        descriptor = table.getcoldesc("DATA")
        shape = table.getcol("DATA").shape
        for name, values in (("WEIGHT_SPECTRUM", [8, 1, 2]), ("SIGMA_SPECTRUM", [0.5, 1, 2])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            spectrum = np.asarray(values, dtype=np.float32)[integration % 3]
            table.putcol(name, np.broadcast_to(spectrum, shape).copy())


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
    attach_spectra(spectra, integration)
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


def compare_expressions(root, native):
    source, integration, start = make_fixture(root)
    fallback, spectra, zero, overflow, unbounded, cube_flags = (
        root / f"{name}.ms"
        for name in ("without-corrected", "spectra", "zero-model", "overflow-model", "unbounded", "cube-flags")
    )
    for path in (fallback, spectra, zero, overflow, unbounded, cube_flags):
        copy_ms(source, path)
    attach_spectra(spectra, integration)
    with opened(fallback, True) as table:
        table.removecols("CORRECTED_DATA")
        table.putcol("SIGMA", np.full_like(table.getcol("SIGMA"), .5))
    with opened(zero, True) as table:
        table.putcol("MODEL_DATA", np.zeros_like(table.getcol("MODEL_DATA")))
    with opened(overflow, True) as table:
        table.putcol("MODEL_DATA", np.full_like(table.getcol("MODEL_DATA"), 1e38))
    with opened(unbounded, True) as table:
        unit = np.ones_like(table.getcol("MODEL_DATA"))
        table.putcol("MODEL_DATA", unit * np.exp(1j * np.deg2rad(10)))
    with opened(cube_flags, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, :] = integration % 3 == 1
        table.putcol("FLAG", flags)

    corrected, model = 14 / 3 + 1j * np.deg2rad(74.5), 31 / 6 + 1j * np.deg2rad(45)
    quotient = corrected / model
    phase_ratio = np.rad2deg(corrected.imag / model.imag)
    base = request(source) | dict(xaxis="time", xdatacolumn="", spw="0:1", avgtime="20",
                                  scalar=True, title="Scalar-time visibility expressions")
    cases = (
        ("vector-real", source, "time", "", "real", "corrected/model_vector", None, quotient.real),
        ("vector-phase", source, "time", "", "phase", "corrected/model_vector", None,
         np.rad2deg(quotient.imag)),
        ("scalar-amplitude", source, "time", "", "amp", "corrected/model_scalar", None, 28 / 31),
        ("scalar-phase", source, "time", "", "phase", "corrected/model_scalar", None, phase_ratio),
        ("scalar-real-difference", source, "time", "", "real", "corrected/model_scalar", None, -.5),
        ("scalar-imaginary-difference", source, "time", "", "imag", "corrected/model_scalar", None,
         np.deg2rad(29.5)),
        ("vector-zero-model", zero, "time", "", "amp", "corrected/model_vector", None, np.inf),
        ("scalar-zero-phase", zero, "time", "", "phase", "corrected/model_scalar", None, np.inf),
        ("vector-overflow-model", overflow, "time", "", "real", "corrected/model_vector", None, 0),
        ("observed-residual", source, "time", "", "amp", "data-model_vector", None, -1.5),
        ("observed-ratio", source, "time", "", "phase", "data/model_scalar", None,
         np.rad2deg((50 / 3) / 45)),
        ("without-corrected", fallback, "time", "", "amp", "data-model_scalar", None, -1.5),
        ("corrected-fallback", fallback, "time", "", "amp", "corrected-model_scalar", None, -1.5),
        ("row-weight-over-spectra", spectra, "time", "", "amp", "data-model_vector", None, -1.5),
        ("channel-derived", source, "channel", "", "amp", "data-model_vector", 1, -1.5),
        ("stored-x-derived-y", source, "amp", "data", "phase", "corrected/model_scalar", 11 / 3,
         phase_ratio),
        ("derived-x-stored-y", source, "amp", "corrected-model_vector", "phase", "model", -.5, 45),
        ("derived-x-derived-y", source, "amp", "data-model_vector", "phase", "corrected/model_scalar",
         -1.5, phase_ratio),
        ("paired-cube-mask", cube_flags, "amp", "data-model_vector", "phase", "corrected/model_scalar",
         -1.75, np.rad2deg(156.75 / 127.5)),
        ("unbounded-ratio-x", unbounded, "phase", "corrected/model_scalar", "amp", "model",
         np.rad2deg(74.5 / 10), 1),
    )
    titles = {
        "derived-x-derived-y": ("Amp:data-model (vector)", "Phase:corrected/model (scalar) (degrees)"),
        "unbounded-ratio-x": ("Phase:corrected/model (scalar) (degrees)", "Amp:model"),
    }
    times = start + np.array([10., 40.])
    for name, vis, xaxis, xcolumn, yaxis, ycolumn, x, y in cases:
        controls = base | dict(vis=str(vis), xaxis=xaxis, xdatacolumn=xcolumn,
                               yaxis=yaxis, ydatacolumn=ycolumn)
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert [(point[2], point[5], point[6], point[12]) for point in selected] == [("1", "0", "1", "XX")] * 2
        np.testing.assert_allclose([float(point[9]) for point in selected], times, rtol=0, atol=.00051)
        np.testing.assert_allclose([float(point[0]) for point in selected], times if x is None else [x] * 2,
                                   rtol=0 if x is None else 1.2e-5, atol=.00051 if x is None else 1e-7)
        np.testing.assert_allclose([float(point[1]) for point in selected], [y] * 2, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 2
        if x is None:
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], times, rtol=0, atol=.00051)
        print(f"{name}: 2 matching scalar-time visibility expression coordinates", flush=True)
        if name in titles:
            compare_figure(root, native, controls, expected, (controls["title"], *titles[name]))


def compare_scalar_time_spans(root, native):
    source, times, scan, field, reversed_time = time_scan_field_fixture(root)
    start = times[0]
    masked = root / "masked-spans.ms"
    copy_ms(source, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, :] = table.getcol("TIME") >= times[6]
        table.putcol("FLAG", flags)
    base = request(source) | dict(field="", xaxis="time", xdatacolumn="", yaxis="amp",
                                  ydatacolumn="corrected-model_vector", spw="0:1", avgtime="200",
                                  scalar=True, title="Scalar averages across scans and fields")
    split = [(None, -90., 25., "1", "0"), (None, -270., 85., "2", "1")]
    joined = [(None, -252., 55., "-1", "0")]
    cross = dict(avgscan=True, avgfield=True)
    cases = (
        ("separate-boundaries", {}, split),
        ("cross-scan", dict(vis=str(scan), avgscan=True), joined),
        ("cross-field", dict(vis=str(field), avgfield=True), [(None, -252., 55., "1", "0")]),
        ("cross-scan-keeps-field", dict(avgscan=True), split),
        ("cross-field-keeps-scan", dict(avgfield=True), split),
        ("cross-both", cross, joined),
        ("cross-both-reversed", cross | dict(vis=str(reversed_time)),
         [(None, -90., 85., "1", "0"), (None, -270., 25., "2", "1")]),
        ("cross-both-shorter", cross | dict(avgtime="75"),
         [(None, -225., 35., "-1", "0"), (None, -270., 95., "2", "1")]),
        ("cross-both-masked", cross | dict(vis=str(masked)), [(None, -90., 55., "-1", "0")]),
        ("cross-both-derived-pair",
         cross | dict(xaxis="amp", xdatacolumn="data-model_vector", ydatacolumn="corrected/model_scalar"),
         [(-277.2, .1, 55., "-1", "0")]),
        ("cross-both-stored", cross | dict(ydatacolumn="corrected"), [(None, 28., 55., "-1", "0")]),
    )
    for name, overrides, coordinates in cases:
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        centers = [start + point[2] for point in coordinates]
        assert [(point[3], point[4]) for point in selected] == [(point[3], point[4]) for point in coordinates]
        np.testing.assert_allclose([float(point[9]) for point in selected], centers, rtol=0, atol=.00051)
        np.testing.assert_allclose([float(point[1]) for point in selected],
                                   [point[1] for point in coordinates], rtol=1.2e-5)
        if controls["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in selected], centers, rtol=0, atol=.00051)
        else:
            np.testing.assert_allclose([float(point[0]) for point in selected],
                                       [point[0] for point in coordinates], rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(coordinates)
        if controls["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], centers, rtol=0, atol=.00051)
        print(f"{name}: {len(coordinates)} matching scalar scan and field span coordinates", flush=True)
        if name == "cross-both":
            compare_figure(root, native, controls, expected, (controls["title"], "Time (MJD seconds)",
                                                              "Amp:corrected-model (vector)"))

    ordinates = base | cross | dict(yaxis=["amp", "real"], ydatacolumn=["corrected", "model"],
                                    yaxislocation=["left", "right"], coloraxis="scan", expformat="txt")
    expected, actual = (root / f"cross-both-stored-ordinates.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**ordinates, plotfile=str(expected)) is None
    selected = rows(expected)
    assert [(point[3], point[4]) for point in selected] == [("-1", "0")] * 2
    np.testing.assert_allclose([float(point[0]) for point in selected], start + 55., rtol=0, atol=.00051)
    np.testing.assert_allclose([float(point[1]) for point in selected], [28., 280.], rtol=1.2e-5)
    assert native(**ordinates, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 2
    np.testing.assert_allclose([float(point[0]) for point in rows(actual)], start + 55., rtol=0, atol=.00051)
    print("cross-both-stored-ordinates: 2 matching scalar scan and field span coordinates", flush=True)
    compare_scalar_span_shapes(root, native, source, base | cross, start + 55.)


def compare_scalar_span_shapes(root, native, source, controls, center):
    from casars.tasks import TaskExecutionError
    from grpc import RpcError, StatusCode

    vis = root / "mixed-polarization.ms"
    copy_ms(source, vis)
    with opened(vis / "POLARIZATION", True) as pol:
        polarization = pol.nrows()
        pol.addrows(1)
        pol.putcell("NUM_CORR", polarization, 1)
        pol.putcell("CORR_TYPE", polarization, pol.getcell("CORR_TYPE", 0)[:1])
        pol.putcell("CORR_PRODUCT", polarization, pol.getcell("CORR_PRODUCT", 0)[:, :1])
        pol.putcell("FLAG_ROW", polarization, False)
    with opened(vis / "DATA_DESCRIPTION", True) as dd:
        description = dd.nrows()
        dd.addrows(1)
        dd.putcell("SPECTRAL_WINDOW_ID", description, 0)
        dd.putcell("POLARIZATION_ID", description, polarization)
        dd.putcell("FLAG_ROW", description, False)
    with opened(vis, True) as table:
        descriptions = table.getcol("DATA_DESC_ID")
        reduced = (descriptions == 0) & (table.getcol("SCAN_NUMBER") == 2)
        for column in ("DATA", "CORRECTED_DATA", "MODEL_DATA", "FLAG", "FLAG_CATEGORY", "WEIGHT", "SIGMA"):
            descriptor = table.getcoldesc(column)
            descriptor.pop("shape", None)
            descriptor.update(option=0, dataManagerType="StandardStMan", dataManagerGroup=column)
            original = "ORIGINAL_" + column
            table.renamecol(column, original)
            table.addcols({column: descriptor})
            for row in range(table.nrows()):
                if table.iscelldefined(original, row):
                    values = table.getcell(original, row)
                    table.putcell(column, row, values[:1] if reduced[row] else values)
            table.removecols(original)
        descriptions[reduced] = description
        table.putcol("DATA_DESC_ID", descriptions)

    controls |= dict(vis=str(vis), expformat="txt")
    expected, actual = (root / f"selected-polarization.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 1
    np.testing.assert_allclose([float(point[0]) for point in rows(actual)], center, rtol=0, atol=.00051)
    assert [(point[1], point[3], point[12]) for point in rows(expected)] == [("-252", "-1", "XX")]

    controls |= dict(correlation="")
    expected, actual = (root / f"mixed-polarization.{engine}.txt" for engine in ("casa", "native"))
    try:
        reference(**controls, plotfile=str(expected))
    except RpcError as error:
        assert error.code() == StatusCode.INVALID_ARGUMENT, error
    else:
        raise AssertionError("CASA admitted unequal selected shapes into a scalar time bin")
    try:
        native(**controls, plotfile=str(actual))
    except TaskExecutionError as error:
        assert "equal correlation and channel counts within a bin" in str(error), error
    else:
        raise AssertionError("native PlotMS admitted unequal selected shapes into a scalar time bin")
    assert not expected.exists() and not actual.exists()
    print("selected-polarization: 1 matching point and matching incompatible-shape refusals", flush=True)


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
        expressions = root / "expressions"
        expressions.mkdir()
        compare_expressions(expressions, native)
        spans = root / "scan-field-spans"
        spans.mkdir()
        compare_scalar_time_spans(spans, native)


if __name__ == "__main__":
    main()
