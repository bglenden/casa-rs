#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare PlotMS averages of residuals and ratios using virtual or default models."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_channel_ordinates_oracle import virtual_fixtures
from plotms_ms_columns_oracle import (
    compare_figure, compare_ordinate_figure, compare_text, copy_ms, opened, plotms_launcher, reference, request, rows,
    time_average_fixture, time_scan_field_fixture, varying_fourier_fixture, virtual_fourier_fixture,
)


def compare_averaged_expressions(root, native):
    """Check residual and ratio operands without writing a scratch model."""
    vis, integration = time_average_fixture(root)
    virtual, mixed, stored_model = virtual_fixtures(root, vis)
    default = root / "default-point.ms"
    copy_ms(vis, default)
    with opened(default, True) as table:
        table.removecols(["MODEL_DATA"])
    masked = root / "masked-virtual.ms"
    copy_ms(virtual, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1] = (integration % 2 != 0) | (integration >= 4)
        table.putcol("FLAG", flags)
        start = table.getcol("TIME").min()

    title = "Averaged virtual model residual"
    base = request(virtual) | dict(xaxis="time", xdatacolumn="", yaxis="real", spw="0:1",
                                   ydatacolumn="corrected-model_vector", avgtime="20", title=title)
    vector_times = tuple(start + step for step in (5, 25, 45))
    scalar_times = (start + 10, start + 40)
    channel_times = tuple(time for time in vector_times for _ in range(2))
    channel_columns = dict(avgtime="", avgchannel="2", spw="0:0~3", ydatacolumn="data/model_vector")
    cases = (
        ("virtual-residual", {}, vector_times, (20.75,) * 3),
        ("virtual-precedes-stored-residual", dict(vis=str(mixed)), vector_times, (20.75,) * 3),
        ("default-point-residual", dict(vis=str(default)), vector_times, (27.,) * 3),
        ("masked-virtual-residual", dict(vis=str(masked)), (start, start + 20), (2.75, 2.75)),
        ("virtual-scalar-ratio", dict(vis=str(mixed), ydatacolumn="data/model_scalar", scalar=True),
         scalar_times, (29 / 11 - 7.25, 55 / 19 - 7.25)),
        ("virtual-time-channel-residual", dict(vis=str(mixed), avgchannel="2", spw="0:0~3"),
         channel_times, (20.75,) * 6),
        ("virtual-channel-ratio", channel_columns | dict(xaxis="channel"),
         (0, 1) * 6, (1 / 7.25, 1 / 7.25, 3 / 7.25, 3 / 7.25) * 3),
        ("virtual-ratio-x", dict(xaxis="real", xdatacolumn="data/model_vector", ydatacolumn="corrected"),
         (2.6 / 7.25,) * 3, (28.,) * 3),
        ("virtual-channel-ratio-x", channel_columns | dict(xaxis="real", xdatacolumn="data/model_vector",
                                                           ydatacolumn="corrected"),
         (1 / 7.25, 1 / 7.25, 3 / 7.25, 3 / 7.25) * 3, (10., 10., 30., 30.) * 3),
        ("virtual-scalar-baseline", dict(avgtime="", scalar=True, avgbaseline=True),
         tuple(start + 10 * i for i in range(6)), (2.75, 22.75) * 3),
    )
    for name, overrides, x_values, y_values in cases:
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == len(x_values), (name, len(selected))
        assert all(point[12] == "XX" for point in selected)
        pairs = np.array([point[:2] for point in selected], dtype=float)
        if controls["xaxis"] == "time":
            np.testing.assert_allclose(pairs[:, 0], x_values, rtol=0, atol=.00051)
        else:
            np.testing.assert_allclose(pairs[:, 0], x_values, rtol=1.2e-5)
        np.testing.assert_allclose(pairs[:, 1], y_values, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        if controls["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], x_values,
                                       rtol=0, atol=.00051)
        print(f"{name}: {len(selected)} matching averaged model expression coordinates", flush=True)
        if name == "virtual-precedes-stored-residual":
            compare_figure(root, native, controls, expected, (title, "Time (MJD seconds)",
                                                              "Real:corrected-model (vector)"))

    for source in (virtual, default, masked):
        with opened(source) as table:
            assert "MODEL_DATA" not in table.colnames()
    with opened(mixed) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), stored_model)
    compare_overlay_refusals(root, native, base | channel_columns | dict(xaxis="channel"), (virtual, mixed))


def compare_overlay_refusals(root, native, controls, sources):
    from casars.tasks import TaskExecutionError, TaskParameterError

    errors = (
        (TaskParameterError, "require stored MODEL_DATA (#1032)"),
        (TaskExecutionError, "virtual MeasurementSet models are pending #1032"),
    )
    for vis, (category, message) in zip(sources, errors, strict=True):
        output = root / f"{vis.stem}-overlay.native.txt"
        options = controls | dict(vis=str(vis), showatm=True, expformat="txt", plotfile=str(output))
        try:
            native(**options)
        except category as error:
            assert message in str(error), error
        else:
            raise AssertionError(f"PlotMS enabled an implicit-model averaged expression with an overlay for {vis.name}")
        assert not output.exists()
    print("Implicit-model averaged expression overlay controls keep both refusals", flush=True)


def compare_varying_fourier_expression(root, native):
    root.mkdir()
    vis, columns = virtual_fourier_fixture(root)
    varying = root / "time-varying-fourier.ms"
    varying_fourier_fixture(vis, varying)
    samples = request(varying) | dict(xaxis="real", yaxis="imag", spw="0:1", expformat="txt")
    operands = []
    for column in ("corrected", "model"):
        output = root / f"{column}-operand.casa.txt"
        raw = samples | dict(xdatacolumn=column, ydatacolumn=column)
        assert reference(**raw, plotfile=str(output)) is None
        operands.append(np.array([point[:2] for point in rows(output)], dtype=float).reshape(3, 2, 2))
    corrected, model = operands
    average = np.average(corrected - model, weights=[1, 9], axis=1)
    first_model = np.average(corrected, weights=[1, 9], axis=1) - model[:, 0]
    assert np.max(np.abs(average - first_model)) > 1
    column = "corrected-model_vector"
    controls = samples | dict(xdatacolumn=column, ydatacolumn=column, avgtime="20")
    expected, actual = (root / f"fourier-residual.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    selected = np.array([point[:2] for point in rows(expected)], dtype=float)
    np.testing.assert_allclose(selected, average, rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 3
    with opened(varying) as table:
        np.testing.assert_array_equal(table.getcol("MODEL_DATA"), columns["MODEL_DATA"])
    print("fourier-residual: 3 matching weighted residuals from distinct model samples", flush=True)


def compare_scan_span(root, native):
    root.mkdir()
    _, times, vis, _, _ = time_scan_field_fixture(root)
    with opened(vis, True) as table:
        table.removecols(["MODEL_DATA"])
    assert casa.setjy(str(vis), field="0", standard="manual", usescratch=False,
                      fluxdensity=[7.25, 0, 0, 0])
    controls = request(vis) | dict(field="", xaxis="time", xdatacolumn="", yaxis="real", spw="0:1",
                                   avgtime="200", avgscan=True, ydatacolumn="corrected-model_vector",
                                   expformat="txt")
    expected, actual = (root / f"virtual-scan-span.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected)) is None
    [point] = rows(expected)
    assert point[3:5] == ["1", "0"]
    np.testing.assert_allclose(float(point[0]), times[0] + 55, rtol=0, atol=.00051)
    np.testing.assert_allclose(float(point[1]), (10 + 9 * 30) / 10 - 7.25, rtol=1.2e-5)
    assert native(**controls, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 1
    np.testing.assert_allclose(float(rows(actual)[0][0]), times[0] + 55, rtol=0, atol=.00051)
    with opened(vis) as table:
        assert "MODEL_DATA" not in table.colnames()
    print("virtual-scan-span: 1 matching averaged model expression coordinate", flush=True)


def make_virtual_span_fixture(root):
    root.mkdir()
    both, times, scan_only, field, _ = time_scan_field_fixture(root)
    for vis in (field, both, scan_only):
        with opened(vis, True) as table:
            table.removecols(["MODEL_DATA"])
        for field_id, flux in ((0, 7.25), (1, 3.25)):
            if vis == scan_only and field_id == 1:
                continue
            assert casa.setjy(str(vis), field=str(field_id), standard="manual", usescratch=False,
                              fluxdensity=[flux, 0, 0, 0])
    return both, times, scan_only, field


def compare_scalar_field_spans(root, native):
    both, times, scan_only, field = make_virtual_span_fixture(root)
    title = "Scalar virtual model across fields"
    base = request(field) | dict(field="", xaxis="time", xdatacolumn="", yaxis="real", spw="0:1",
                                 avgtime="200", avgfield=True, scalar=True, title=title, expformat="txt")
    center = times[0] + 55
    model = (7.25 + 9 * 3.25) / 10
    corrected = (10 + 9 * 30) / 10
    observed = (1 + 9 * 3) / 10
    residual = dict(ydatacolumn="corrected-model_vector")
    for name, overrides, x_values, y_values in (
        ("scalar-field-model", dict(ydatacolumn="model"), (center,), (model,)),
        ("scalar-field-complex-model", dict(xaxis="amp", xdatacolumn="corrected", yaxis=["real", "imag"],
                                           ydatacolumn=["model", "model"], yaxislocation=["left", "right"]),
         (corrected, corrected), (model, 0)),
        ("scalar-field-residual", residual, (center,), (corrected - model,)),
        ("scalar-field-ratio", dict(yaxis="amp", ydatacolumn="data/model_scalar"),
         (center,), (observed / model,)),
        ("scalar-field-channel-residual", residual | dict(avgchannel="2", spw="0:0~3"),
         (center, center), (corrected - model,) * 2),
        ("scalar-scan-and-field-model", dict(vis=str(both), avgscan=True, ydatacolumn="model"),
         (center,), (model,)),
        ("scalar-scan-model", dict(vis=str(scan_only), avgscan=True, avgfield=False, ydatacolumn="model"),
         (center,), (7.25,)),
    ):
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == len(x_values)
        scan = "-1" if controls.get("avgscan") else "1"
        assert {tuple(point[3:9]) for point in selected} == {(scan, "0", "0", "1", "A0@P0", "A1@P1")}
        assert {point[12] for point in selected} == {"XX"}
        np.testing.assert_allclose([float(point[1]) for point in selected], y_values, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        for points in (selected, rows(actual)):
            relative = 0 if controls["xaxis"] == "time" else 1.2e-5
            np.testing.assert_allclose([float(point[0]) for point in points], x_values,
                                       rtol=relative, atol=.00051)
        print(f"{name}: {len(selected)} matching scalar virtual field coordinates", flush=True)
        if name == "scalar-field-residual":
            compare_figure(root, native, controls, expected, (title, "Time (MJD seconds)",
                                                              "Real:corrected-model (vector)"))
    for vis in (field, both, scan_only):
        with opened(vis) as table:
            assert "MODEL_DATA" not in table.colnames()


def compare_vector_field_spans(root, native, *, only=None):
    both, times, scan, field = make_virtual_span_fixture(root)
    if only == "vector-field-channels":
        compare_vector_field_channels(root, native, field)
        return
    if only == "vector-scan-spans":
        compare_vector_scan_spans(root, native, scan, both, times[0])
        return
    if only == "vector-buffer-references":
        compare_vector_buffer_references(root, native, field, scan, times[0])
        return
    reverse = root / "virtual-reversed-time.ms"
    copy_ms(field, reverse)
    with opened(reverse, True) as table:
        stamps = table.getcol("TIME")
        flipped = stamps.min() + (stamps.max() - stamps)
        table.putcol("TIME", flipped)
        table.putcol("TIME_CENTROID", flipped)
    base = request(field) | dict(field="", xaxis="time", xdatacolumn="", spw="0:1", avgtime="200",
                                 avgfield=True, yaxis=["real", "imag"], ydatacolumn=["model", "model"],
                                 yaxislocation=["left", "right"], expformat="txt")
    combined = (3.61076, -.225413)
    for name, overrides, offsets, values, metadata in (
        ("vector-field-model", {}, (55, 55), combined, [("1", "0")] * 2),
        ("vector-field-chunk", dict(avgtime="40"), (15, 55, 95) * 2,
         (7.25, 4.22486, -.642576, 0, -.0908225, -.585417), [("1", "0"), ("1", "0"), ("1", "1")] * 2),
        ("vector-field-scan-barrier", dict(vis=str(both)), (25, 85) * 2,
         (7.25, 3.25, 0, 0), [("1", "0"), ("2", "1")] * 2),
        ("vector-scan-and-field-model", dict(vis=str(both), avgscan=True), (55, 55), combined, [("1", "0")] * 2),
        ("vector-field-reversed", dict(vis=str(reverse)), (55, 55), (.819438, -.0185677), [("1", "1")] * 2),
        ("vector-field-late-selection", dict(field="1"), (85, 85), (3.25, 0), [("1", "1")] * 2),
        ("vector-field-single-model", dict(yaxis="real", ydatacolumn="model", yaxislocation="left"),
         (55,), (combined[0],), [("1", "0")]),
        ("vector-field-residual", dict(yaxis="real", ydatacolumn="corrected-model_vector", yaxislocation="left"),
         (55,), (27.3006,), [("1", "0")]),
    ):
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        assert [tuple(point[3:5]) for point in selected] == metadata
        assert {tuple(point[5:9]) for point in selected} == {("0", "1", "A0@P0", "A1@P1")}
        np.testing.assert_allclose([float(point[1]) for point in selected], values, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(values)
        centers = times[0] + np.asarray(offsets)
        for points in (selected, rows(actual)):
            np.testing.assert_allclose([float(point[0]) for point in points], centers, rtol=0, atol=.00051)
        print(f"{name}: {len(values)} matching coordinates in the selected chunk phase reference", flush=True)
        if name == "vector-field-chunk":
            compare_ordinate_figure(root, name, native, controls, "Time (MJD seconds)", ["Real:model", "Imag:model"])
    for vis in (field, both, reverse):
        with opened(vis) as table:
            assert "MODEL_DATA" not in table.colnames()
    compare_vector_field_channels(root, native, field)
    compare_vector_scan_spans(root, native, scan, both, times[0])
    compare_vector_buffer_references(root, native, field, scan, times[0])


def compare_vector_field_channels(root, native, field):
    masked = root / "virtual-field-masked-channels.ms"
    copy_ms(field, masked)
    with opened(masked, True) as table:
        early = table.getcol("FIELD_ID") == 0
        flags = table.getcol("FLAG")
        flags[:, :2, early] = True
        table.putcol("FLAG", flags)
    controls = request(field) | dict(field="", spw="0:0~3", xdatacolumn="", avgtime="200",
                                     avgfield=True, avgchannel="2", expformat="txt")
    for name, overrides, count in (
        ("vector-field-channel-model", dict(vis=str(masked), xaxis="frequency", yaxis=["real", "imag"],
                                            ydatacolumn=["model", "model"], yaxislocation=["left", "right"]), 4),
        ("vector-field-channel-residual", dict(xaxis="time", yaxis="real", ydatacolumn="corrected-model_vector"), 2),
    ):
        options = controls | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert [point[2] for point in selected] == ["0", "1"] * (count // 2)
        assert {point[12] for point in selected} == {"XX"}
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        if options["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                       [float(point[0]) for point in selected], rtol=0, atol=.00051)
        else:
            assert [point[0] for point in rows(actual)] == ["100.000500000", "100.002500000"] * 2
        print(f"{name}: {count} matching virtual field coordinates with channel bins", flush=True)


def compare_vector_scan_spans(root, native, scan, both, start):
    base = request(scan) | dict(field="", spw="0:1", xaxis="time", xdatacolumn="", avgtime="200",
                                avgscan=True, yaxis=["real", "imag"], ydatacolumn=["model", "model"],
                                yaxislocation=["left", "right"], expformat="txt")
    for name, vis, offsets, values, fields in (
        ("vector-scan-model", scan, (55, 55), (7.25, 0), ["0", "0"]),
        ("vector-scan-model-retains-field", both, (25, 85) * 2, (7.25, 3.25, 0, 0), ["0", "1"] * 2),
    ):
        options = base | dict(vis=str(vis))
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert [point[4] for point in selected] == fields
        np.testing.assert_allclose([float(point[1]) for point in selected], values, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(values)
        for points in (selected, rows(actual)):
            np.testing.assert_allclose([float(point[0]) for point in points],
                                       start + np.asarray(offsets), rtol=0, atol=.00051)
        print(f"{name}: {len(values)} matching virtual model coordinates across scans", flush=True)


def compare_vector_buffer_references(root, native, field, scan, start):
    tied, moving = (root / f"vector-{name}.ms" for name in ("tied-fields", "moving-reference"))
    copy_ms(field, tied)
    copy_ms(scan, moving)
    with opened(tied, True) as table:
        table.putcol("FIELD_ID", (table.getcol("ANTENNA2") == 2).astype(np.int32))
    with opened(moving / "FIELD", True) as table:
        table.putcell("NUM_POLY", 0, 1)
        table.putcell("TIME", 0, start)
        for column in ("PHASE_DIR", "REFERENCE_DIR", "DELAY_DIR"):
            table.putcell(column, 0, np.column_stack((table.getcell(column, 0), [3e-6, 0.])))
    base = request(tied) | dict(field="", spw="0:1", xaxis="time", xdatacolumn="", avgtime="200",
                                avgfield=True, yaxis=["real", "imag"], ydatacolumn=["model", "model"],
                                yaxislocation=["left", "right"], expformat="txt")
    first_time = dict(msselect=f"TIME < {start + 1:.3f}")
    for name, overrides, values, metadata, center in (
        ("tied-field-recipes", first_time | dict(antenna="A0&A1;A0&A2"), (7.25, 7.25, 0, 0),
         [("0", "0", "1"), ("0", "0", "2")] * 2, start),
        ("tied-field-single-baseline", first_time | dict(antenna="A0&A2"), (3.25, 0), [("1", "0", "2")] * 2, start),
        ("moving-field-epoch", dict(vis=str(moving), avgscan=True, avgfield=False, field="0",
                                    msselect=f"TIME > {start + 19:.3f}"),
         (-7.22626, -.452888), [("0", "0", "1")] * 2, start + 65),
    ):
        options = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert [tuple(point[4:7]) for point in selected] == metadata
        np.testing.assert_allclose([float(point[1]) for point in selected], values, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(values)
        for points in (selected, rows(actual)):
            np.testing.assert_allclose([float(point[0]) for point in points], center, rtol=0, atol=.00051)
        print(f"{name}: {len(values)} matching virtual coordinates and visibility-buffer references", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("scalar-field-spans", "vector-field-spans", "vector-field-channels",
                                           "vector-scan-spans", "vector-buffer-references"))
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
            compare_averaged_expressions(root, native)
            compare_scan_span(root / "spans", native)
            compare_varying_fourier_expression(root / "fourier-model", native)
        if args.only in (None, "scalar-field-spans"):
            compare_scalar_field_spans(root / "scalar-field-spans", native)
        if args.only != "scalar-field-spans":
            compare_vector_field_spans(root / "vector-field-spans", native, only=args.only)


if __name__ == "__main__":
    main()
