#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare time averaging across lists of independent PlotMS visibility ordinates."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
import numpy as np
from PIL import Image

from plotms_ms_baseline_channels_oracle import make_fixture as make_channel_baseline_fixture
from plotms_ms_baseline_channels_oracle import make_reversed_partial_spectrum
from plotms_ms_channel_ordinates_oracle import compare_sides
from plotms_ms_columns_oracle import (
    compare_text, copy_ms, opened, plotms_launcher, reference, request, rows, time_average_fixture,
    time_scan_field_fixture,
)


def spectral_fixture(root, source, integration):
    vis = root / "spectral-time.ms"
    copy_ms(source, vis)
    with opened(vis, True) as table:
        descriptor = table.getcoldesc("DATA")
        shape = table.getcol("DATA").shape
        early = integration % 2 == 0
        odd_channel = np.arange(shape[1]) % 2 == 1
        amplitude = (1 + 4 * (~early)[None, None, :] + 2 * odd_channel[None, :, None]).astype(np.complex64)
        for factor, column in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(column, np.broadcast_to(amplitude * 10**factor, shape).copy())
        for column, first, last in (("WEIGHT_SPECTRUM", [1, 9], [4, 1]),
                                    ("SIGMA_SPECTRUM", [1, 0.5], [1, 1])):
            table.addcols({column: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                        dataManagerGroup=column)})
            weights = np.where(early[None, :], np.resize(first, shape[1])[:, None],
                               np.resize(last, shape[1])[:, None]).astype(np.float32)
            table.putcol(column, np.broadcast_to(weights, shape).copy())
    return vis


def reversed_partial_spectrum(root):
    work = root / "partial-spectrum"
    work.mkdir()
    source, *_ = make_channel_baseline_fixture(work)
    vis = make_reversed_partial_spectrum(source, work / "reversed-partial-spectrum.ms")
    with opened(vis, True) as table:
        antenna2 = table.getcol("ANTENNA2")
        baseline = ((table.getcol("ANTENNA1") == 0) & (antenna2 == 1)
                    & (table.getcol("FIELD_ID") == 0))
        times = table.getcol("TIME")
        shared_buffer = (antenna2 == 2) & (times == times[np.flatnonzero(baseline)[0]])
        weights = np.ones_like(table.getcol("WEIGHT"))
        weights[:, shared_buffer] = 9
        corrected = np.ones_like(table.getcol("CORRECTED_DATA")) * 10
        corrected[:, :, shared_buffer] = 100
        table.putcol("WEIGHT", weights)
        table.putcol("CORRECTED_DATA", corrected)
    return vis, times[baseline].min()


def compare_empty_time(root, native, vis, base, *, scalar_modes=(False, True)):
    from casars import _plot_document

    target = root / "all-selected-cube-cells-flagged.ms"
    copy_ms(vis, target)
    with opened(target, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, :] = True
        table.putcol("FLAG", flags)
        assert not table.getcol("FLAG_ROW").any()

    render = _plot_document.render_document

    def record(*args, **kwargs):
        figure, primary = render(*args, **kwargs)
        assert primary.get_xlabel() == "Time (MJD seconds)"
        assert [(axis.yaxis.get_label_position(), axis.get_ylabel()) for axis in figure.axes] == [
            ("left", "Real, Real:model"), ("right", "Real:corrected")
        ]
        assert all(not (axis.lines or axis.collections) for axis in figure.axes)
        return figure, primary

    for scalar in scalar_modes:
        name = "empty-scalar-time" if scalar else "empty-vector-time"
        controls = base | dict(vis=str(target), xaxis="time", xdatacolumn="", scalar=scalar)
        casa_txt, native_txt = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(casa_txt), expformat="txt") is None
        assert not rows(casa_txt) and casa_txt.read_text().count("# No data to export") == 3
        assert native(**controls, plotfile=str(native_txt), expformat="txt") is None
        assert native_txt.read_bytes() == casa_txt.read_bytes()
        figure = controls | dict(width=500, height=360, dpi=100, expformat="png")
        casa_png, native_png = (root / f"{name}.{engine}.png" for engine in ("casa", "native"))
        assert reference(**figure, plotfile=str(casa_png)) is None
        with patch.object(_plot_document, "render_document", side_effect=record) as renderer:
            assert native(**figure, plotfile=str(native_png)) is None
        renderer.assert_called_once()
        for path in (casa_png, native_png):
            with Image.open(path) as image:
                assert image.size == (500, 360)
        print(f"{name}: matching empty text sections and both blank axis sides", flush=True)


def compare(root, native):
    vis, integration = time_average_fixture(root)
    masked = root / "masked-time.ms"
    row_flagged = root / "row-flagged-time.ms"
    copy_ms(vis, masked)
    copy_ms(vis, row_flagged)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[:, :, integration % 2 == 0] = True
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", integration % 2 == 0)
        start = np.min(table.getcol("TIME"))
    with opened(row_flagged, True) as table:
        table.putcol("FLAG_ROW", integration % 2 == 1)
    spectra = spectral_fixture(root, vis, integration)
    masked_spectrum = root / "masked-spectrum.ms"
    copy_ms(spectra, masked_spectrum)
    with opened(masked_spectrum, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1, integration % 2 == 1] = True
        table.putcol("FLAG", flags)
        model = table.getcol("MODEL_DATA")
        model[0, 1, integration % 2 == 0] = 900
        table.putcol("MODEL_DATA", model)
    buffer_time = root / "two-baseline-time.ms"
    copy_ms(vis, buffer_time)
    with opened(buffer_time, True) as table:
        antenna2 = table.getcol("ANTENNA2")
        flags = table.getcol("FLAG")
        flags[0, 1] |= (((antenna2 == 1) & np.isin(integration, [2, 3]))
                        | ((antenna2 == 2) & np.isin(integration, [1, 3])))
        table.putcol("FLAG", flags)
    sparse, sparse_start = reversed_partial_spectrum(root)
    signed = root / "signed-spectrum.ms"
    copy_ms(spectra, signed)
    with opened(signed, True) as table:
        shape = table.getcol("DATA").shape
        for column, scale in (("DATA", 1), ("CORRECTED_DATA", 1), ("MODEL_DATA", 100)):
            values = np.where(integration % 2 == 0, scale, 3 * scale).astype(np.complex64)
            table.putcol(column, np.broadcast_to(values[None, None, :], shape).copy())
        weights = np.where(integration % 2 == 0, 1, -1).astype(np.float32)
        table.putcol("WEIGHT_SPECTRUM", np.broadcast_to(weights[None, None, :], shape).copy())
        table.putcol("SIGMA_SPECTRUM", np.ones(shape, dtype=np.float32))

    base = request(vis) | dict(xaxis="real", xdatacolumn="data", spw="0:1", avgtime="20",
                               yaxis=["real", "real", "real"], ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    vector = (2.6,) * 3
    times = tuple(start + offset for offset in (5, 25, 45))
    scalar = (29 / 11, 55 / 19)
    model_only = dict(vis=str(masked_spectrum), xaxis="channel", xdatacolumn="",
                      spw="0:0~1", avgchannel="2")
    two_baselines = dict(xaxis="time", xdatacolumn="", antenna="A0&A1;A0&A2")
    cases = (
        ("three-ordinates", {}, vector, [vector, (28,) * 3, (280,) * 3]),
        ("four-ordinates", dict(yaxis=["real", "real", "amp", "phase"],
                                ydatacolumn=["data", "corrected", "model", "model"],
                                yaxislocation=["left", "right", "left", "right"]),
         vector, [vector, (28,) * 3, (280,) * 3, (0,) * 3]),
        ("late-corrected", dict(xaxis="time", xdatacolumn="", yaxis=["real", "amp", "real"],
                               ydatacolumn=["data", "model", "corrected"]),
         times, [vector, (280,) * 3, (28,) * 3]),
        ("late-observed", dict(xaxis="time", xdatacolumn="", yaxis=["real", "amp", "real"],
                              ydatacolumn=["corrected", "model", "data"]),
         times, [(28,) * 3, (280,) * 3, vector]),
        ("cube-masked-integrations", dict(vis=str(masked)),
         (3,) * 3, [(3,) * 3, (30,) * 3, (300,) * 3]),
        ("row-only-flags", dict(vis=str(row_flagged)), vector, [vector, (28,) * 3, (280,) * 3]),
        ("time-and-channel", dict(avgchannel="2", spw="0:0~3"),
         vector * 2, [vector * 2, (28,) * 6, (280,) * 6]),
        ("time-and-channel-spectra", dict(vis=str(spectra), avgchannel="2", spw="0:0~1"),
         (25 / 7,) * 3, [(25 / 7,) * 3, (550 / 15,) * 3, (5500 / 15,) * 3]),
        ("late-corrected-spectrum", dict(vis=str(spectra), xaxis="time", xdatacolumn="",
                                         yaxis=["real", "phase", "real"],
                                         ydatacolumn=["data", "data", "corrected"]),
         times, [(3.8,) * 3, (0,) * 3, (34,) * 3]),
        ("two-shared-model-ordinates", model_only | dict(yaxis=["amp", "real"],
             ydatacolumn=["model"] * 2, yaxislocation=["left", "right"]), (0,) * 3, [(600,) * 3] * 2),
        ("three-shared-model-ordinates", model_only | dict(yaxis=["amp", "real", "phase"],
             ydatacolumn=["model"] * 3), (0,) * 3, [(600,) * 3, (600,) * 3, (0,) * 3]),
        ("output-buffer-time", two_baselines | dict(vis=str(buffer_time)),
         tuple(start + offset for offset in (5, 5, 25, 45, 45)),
         [(2.6, 1, 1, 2.6, 2.6), (28, 10, 10, 28, 28), (280, 100, 100, 280, 280)]),
        ("input-buffer-spectrum", two_baselines | dict(vis=str(sparse)),
         tuple(sparse_start + offset for offset in (5, 5, 25, 25, 45, 45)),
         [(10,) * 6, (10, 10, 10, 10, 10, 91), (300, 600) * 3]),
        ("signed-weight-cancellation", dict(vis=str(signed), xaxis="time", xdatacolumn=""),
         times, [(2,) * 3, (-2,) * 3, (-200,) * 3]),
        ("scalar-time", dict(xaxis="time", xdatacolumn="", scalar=True),
         (start + 10, start + 40),
         [tuple(value * scale for value in scalar) for scale in (1, 10, 100)]),
        ("scalar-time-and-channel", dict(vis=str(masked_spectrum), xaxis="time", xdatacolumn="",
                                         scalar=True, spw="0:0~1", avgchannel="2"),
         (start + 10, start + 40), [(3, 11 / 3), (95 / 3, 340 / 9), (2300 / 3, 6100 / 9)]),
    )
    for name, overrides, x_values, ordinates in cases:
        controls = base | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        count = len(x_values) * len(ordinates)
        assert len(selected) == count, (name, len(selected))
        pairs = np.array([point[:2] for point in selected], dtype=float)
        is_time = controls["xaxis"] == "time"
        np.testing.assert_allclose(pairs[:, 0], x_values * len(ordinates),
                                   rtol=0 if is_time else 1.2e-5, atol=0.00051 if is_time else 1e-7)
        np.testing.assert_allclose(pairs[:, 1], np.asarray(ordinates).ravel(), rtol=1.2e-5, atol=1e-7)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        if is_time:
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)], pairs[:, 0],
                                       rtol=0, atol=0.00051)
        print(f"{name}: {count} matching time-averaged coordinates", flush=True)
    compare_sides(root, native, base, expected_x="Real")
    compare_empty_time(root, native, vis, base)
    compare_vector_span_weights(root, native, base, signed, model_only)


def compare_vector_span_weights(root, native, base, signed, model_only):
    for name, overrides, y_values in (
        ("spanned-signed-weight", dict(vis=str(signed), xaxis="time", xdatacolumn=""), (2, -2, -200)),
        ("spanned-shared-model", model_only | dict(yaxis=["amp", "real", "phase"], ydatacolumn=["model"] * 3),
         (600, 600, 0)),
    ):
        controls = base | overrides | dict(avgscan=True, expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        assert {point[3] for point in selected} == {"1"}
        np.testing.assert_allclose([float(point[1]) for point in selected], np.repeat(y_values, 3), rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        if controls["xaxis"] == "time":
            np.testing.assert_allclose([float(point[0]) for point in rows(actual)],
                                       [float(point[0]) for point in selected], rtol=0, atol=.00051)
        print(f"{name}: {len(selected)} matching vector coordinates when a single scan is spanned", flush=True)


def compare_spanned_ordinate_lists(root, native):
    root.mkdir()
    vis, times, scans, fields, _ = time_scan_field_fixture(root)
    base = request(vis) | dict(field="", xaxis="time", xdatacolumn="", spw="0:1", avgtime="200",
                               yaxis=["real"] * 3, ydatacolumn=["data", "corrected", "model"],
                               yaxislocation=["left", "right", "left"])
    for scalar in (False, True):
        compare_span_mode(root, native, base, times, scans, fields, scalar=scalar)
    compare_span_controls(root, native, vis, base, times)
    compare_empty_time(root, native, vis, base | dict(avgscan=True, avgfield=True))
    check_spanned_overlay_refusal(root, native, base)


def compare_span_mode(root, native, base, times, scans, fields, *, scalar):
    mode, observed = ("scalar", 2.8) if scalar else ("vector", 2.6)
    for name, bounds, spans_scans in (
        ("scans", dict(vis=str(scans), avgscan=True), True),
        ("fields", dict(vis=str(fields), avgfield=True), False),
        ("scans-and-fields", dict(avgscan=True, avgfield=True), True),
    ):
        controls = base | bounds | dict(scalar=scalar)
        label = f"{mode}-{name}-ordinate-list"
        expected, actual = (root / f"{label}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        scan = "-1" if scalar and spans_scans else "1"
        assert [(point[3], point[4], point[12]) for point in selected] == [(scan, "0", "XX")] * 3
        np.testing.assert_allclose([float(point[0]) for point in selected], times[0] + 55,
                                   rtol=0, atol=.00051)
        np.testing.assert_allclose([float(point[1]) for point in selected], [observed, 28., 280.], rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == 3
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)], times[0] + 55,
                                   rtol=0, atol=.00051)
        print(f"{label}: 3 matching averaged visibility ordinates", flush=True)
        if scalar and name == "scans-and-fields":
            compare_sides(root, native, controls, expected_x="Time (MJD seconds)", name=label)


def check_spanned_overlay_refusal(root, native, base):
    from casars.tasks import TaskParameterError

    output = root / "spanned-overlay.native.txt"
    controls = base | dict(xaxis="channel", avgscan=True, avgfield=True, showatm=True,
                           plotfile=str(output), expformat="txt")
    try:
        native(**controls)
    except TaskParameterError as error:
        assert "more than two MeasurementSet ordinates" in str(error) and "#1032" in str(error), error
    else:
        raise AssertionError("PlotMS enabled an overlay with a scan- and field-spanning ordinate list")
    assert not output.exists()
    print("Spanned ordinate list overlay refusal preserved", flush=True)


def compare_span_controls(root, native, vis, base, times):
    masked = root / "masked-field.ms"
    copy_ms(vis, masked)
    with opened(masked, True) as table:
        flags = table.getcol("FLAG")
        flags[0, 1] = table.getcol("TIME") >= times[6]
        table.putcol("FLAG", flags)
    split = (times[[0, 6]] + 25, [(1., 3.), (10., 30.), (100., 300.)], [("1", "0"), ("2", "1")])
    both = dict(avgscan=True, avgfield=True)
    for name, overrides, x, y, ids in (
        ("scan-averaging-retains-fields", dict(avgscan=True), *split),
        ("field-averaging-retains-scans", dict(avgfield=True), *split),
        ("spans-and-channels", both | dict(spw="0:0~1", avgchannel="2",
                                          yaxis=["real", "real", "real", "phase"],
                                          ydatacolumn=["data", "corrected", "model", "model"],
                                          yaxislocation=["left", "right", "left", "right"]),
         [times[0] + 55], [(2.6,), (28.,), (280.,), (0.,)], [("1", "0")]),
        ("spans-with-masked-field", both | dict(vis=str(masked)),
         [times[0] + 25], [(1.,), (10.,), (100.,)], [("1", "0")]),
    ):
        controls = base | overrides
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        count = len(y) * len(ids)
        assert [(point[3], point[4]) for point in selected] == ids * len(y)
        coordinates = np.array([point[:2] for point in selected], dtype=float)
        np.testing.assert_allclose(coordinates[:, 0], np.tile(x, len(y)), rtol=0, atol=.00051)
        np.testing.assert_allclose(coordinates[:, 1], np.asarray(y).ravel(), rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == count
        np.testing.assert_allclose([float(point[0]) for point in rows(actual)], coordinates[:, 0],
                                   rtol=0, atol=.00051)
        print(f"{name}: {count} matching averaged visibility ordinates", flush=True)


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
        compare_spanned_ordinate_lists(root / "spans", native)


if __name__ == "__main__":
    main()
