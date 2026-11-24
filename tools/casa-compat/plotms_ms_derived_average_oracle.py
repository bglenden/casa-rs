#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare MeasurementSet residuals and ratios after channel, SPW and antenna averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import (
    casa, compare_channel_vector_residual, compare_figure, compare_text, copy_ms, fixture, opened,
    plotms_launcher, reference, request, rows,
)


def weighted_fixture(root):
    vis, _ = fixture(root)
    with opened(vis, True) as table:
        shape = table.getcol("DATA").shape
        descriptor = table.getcoldesc("DATA")
        for name, spectrum in (("WEIGHT_SPECTRUM", [1, 9]), ("SIGMA_SPECTRUM", [1, 0.5])):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            values = np.resize(np.array(spectrum, dtype=np.float32), shape[1])[None, :, None]
            table.putcol(name, np.broadcast_to(values, shape))
        for name, spectrum in (
            ("DATA", [1 + 1j, 3 + 6j, 7 + 5j, 11 + 1j]),
            ("CORRECTED_DATA", [2 + 3j, 8 - 1j, 3 + 3j, 12 + 4j]),
            ("MODEL_DATA", [1 - 1j, 2 + 2j, 4 + 4j, 5 - 3j]),
        ):
            values = np.resize(np.array(spectrum, dtype=np.complex64), shape[1])[None, :, None]
            table.putcol(name, np.broadcast_to(values, shape))
    return vis


def compare_weighted(root, native, vis):
    base = request(vis) | dict(avgchannel="2", spw="0:0~3", title="Weighted visibility")
    # Corrected and model use the calibrated spectrum (1, 9), while the
    # observed ordinate uses inverse sigma squared (1, 4). Channel 2 is
    # flagged, leaving channel 3 alone in the second bin.
    corrected = (7.4 - 0.6j, 12 + 4j)
    model = (1.9 + 1.7j, 5 - 3j)
    residual = [(a - b).real for a, b in zip(corrected, model)]
    ratio = [(a / b).real for a, b in zip(corrected, model)]
    observed = [5, 1]
    for name, axes, coordinates in (
        ("residual", ("real", "corrected-model_vector", "imag", "data"), zip(residual, observed)),
        ("ratio", ("real", "corrected/model_vector", "imag", "data"), zip(ratio, observed)),
        ("ratio-ordinate", ("imag", "data", "real", "corrected/model_vector"), zip(observed, ratio)),
    ):
        controls = base | dict(zip(("xaxis", "xdatacolumn", "yaxis", "ydatacolumn"), axes, strict=True))
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected), expformat="txt") is None
        selected = rows(expected)
        assert len(selected) == 12
        assert [(point[2], point[12]) for point in selected] == [("0", "XX"), ("1", "XX")] * 6
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   list(coordinates) * 6, rtol=1.2e-5)
        assert native(**controls, plotfile=str(actual), expformat="txt") is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching weighted selected coordinates", flush=True)
        if name == "ratio":
            compare_figure(root, native, controls, expected,
                           ("Weighted visibility", "Real:corrected/model (vector)", "Imag"))


def check_unimplemented_averaging(root, native, vis):
    from casars.tasks import TaskParameterError

    base = request(vis) | dict(xaxis="real", yaxis="imag", ydatacolumn="data", expformat="txt")
    output = root / "iterated-ratio.native.txt"
    try:
        native(**(base | dict(xdatacolumn="data/model_scalar", avgchannel="2", iteraxis="scan")),
               plotfile=str(output))
    except TaskParameterError as error:
        assert "averaging derived data columns" in str(error) and "#1032" in str(error), error
    else:
        raise AssertionError("PlotMS accepted the pending iterated-ratio plot")
    assert not output.exists()



def compare_iterator_order(root, native, vis):
    source = root / "reversed-times.ms"
    copy_ms(vis, source)
    with opened(source, True) as table:
        times = table.getcol("TIME")
        reversed_times = times.min() + times.max() - times
        table.putcol("TIME", reversed_times)
        table.putcol("TIME_CENTROID", reversed_times)
        selected = np.flatnonzero((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
                                  & (table.getcol("ANTENNA2") == 1))
        row = int(selected[0])
        table.putcell("FLAG_ROW", row, True)
        observed = table.getcol("DATA")
        observed[1] += 10j
        table.putcol("DATA", observed)

    controls = request(source) | dict(xaxis="real", xdatacolumn="corrected/model_vector",
                                      yaxis="imag", ydatacolumn="data", avgchannel="2", spw="0:0~3",
                                      correlation="XX,YY", title="Selected channel averages")
    expected, actual = (root / f"iterator-order.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert len(selected) == 24
    assert f"{reversed_times[row]:.3f}" in {point[9] for point in selected}
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 24
    compare_figure(root, native, controls, expected,
                   ("Selected channel averages", "Real:corrected/model (vector)", "Imag"))


def compare_operand_edges(root, native, vis):
    for name, model, column, check in (
        ("nonfinite-model", [np.nan, 2, 3], "corrected-model_vector", np.isnan),
        ("all-nonfinite-model", [np.nan] * 3, "corrected-model_vector", np.isnan),
        ("cancelling-model", [1, 2**-24, -1], "corrected/model_vector", np.isinf),
    ):
        source = root / f"{name}.ms"
        copy_ms(vis, source)
        with opened(source, True) as table:
            shape = table.getcol("DATA").shape
            for spectrum in ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"):
                table.putcol(spectrum, np.ones(shape, dtype=np.float32))
            table.putcol("FLAG", np.zeros(shape, dtype=bool))
            table.putcol("CORRECTED_DATA", np.ones(shape, dtype=np.complex64))
            values = np.resize(np.array(model, dtype=np.complex64), shape[1])[None, :, None]
            table.putcol("MODEL_DATA", np.broadcast_to(values, shape))
        controls = request(source) | dict(xaxis="real", xdatacolumn=column, yaxis="imag",
                                          ydatacolumn="data", avgchannel="3", spw="0:0~2",
                                          expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**controls, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 6
        assert all(check(float(point[0])) for point in selected), selected
        assert native(**controls, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == 6
        print(f"{name}: 6 matching nonfinite coordinates with finite observed ordinates", flush=True)


def compare_reduced_expressions(root, native):
    from plotms_ms_spw_average_oracle import export, make_fixture, stagger_windows

    root.mkdir()
    original = make_fixture(root, "original", antenna_count=3)
    stored, _ = stagger_windows(root, original, "staggered-stored-model")
    with opened(stored, True) as table:
        spw = table.getcol("DATA_DESC_ID")
        shape = table.getcol("MODEL_DATA").shape
        original_model = np.broadcast_to(np.where(spw == 0, 4 + 2j, 7 - 1j), shape).copy()
        table.putcol("MODEL_DATA", original_model)
    virtual = root / "virtual-model.ms"
    copy_ms(stored, virtual)
    for window, flux, frequency in ((0, 7.25, "90GHz"), (1, 3.25, "100GHz")):
        assert casa.setjy(str(virtual), field="FLUX", spw=str(window), standard="manual", usescratch=False,
                          fluxdensity=[flux, 0, 0, 0], reffreq=frequency)

    title = "Averaged residual and model ratio"
    base = request(stored) | dict(xaxis="real", xdatacolumn="corrected-model_vector",
                                 yaxis="amp", ydatacolumn="data/model_scalar", avgtime="20", title=title)
    windows = dict(avgspw=True, spw="0:1,1:1")
    antenna = dict(avgantenna=True, antenna="", spw="0:1")
    cases = (
        ("stored-spw", windows, [(6.75, 1.13047)]),
        ("virtual-spw", windows | dict(vis=str(virtual)), [(8.75, 1.66378)]),
        ("virtual-time-channel-spw", windows | dict(vis=str(virtual), avgchannel="2", spw="0:0~3,1:0~3"),
         [(5.5, 1.24784), (18.5, 2.91162)]),
        ("stored-scalar-spw", windows | dict(scalar=True), [(13.429, 1.34209)]),
        ("stored-antenna", antenna, [(6, 1), (6, .5), (6, 1)]),
        ("virtual-antenna", antenna | dict(vis=str(virtual)), [(2.75, .616846), (2.75, .275862), (2.75, .616846)]),
        ("virtual-time-channel-antenna", antenna | dict(vis=str(virtual), avgchannel="2", spw="0:0~3"),
         [(.25, .462635), (10.25, 1.07948), (.25, .206897), (10.25, .482759), (.25, .462635), (10.25, 1.07948)]),
        ("stored-scalar-antenna", antenna | dict(scalar=True), [(11.1484, 1)] * 3),
    )
    for name, changes, values in cases:
        controls = base | changes
        expected = export(root, f"{name}.casa", reference, controls)
        selected = rows(expected)
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float), values, rtol=1.2e-5)
        assert compare_text(export(root, f"{name}.native", native, controls), expected) == len(values)
        if name == "virtual-time-channel-spw":
            compare_figure(root, native, controls, expected,
                           (title, "Real:corrected-model (vector)", "Amp:data/model (scalar)"))
        print(f"{name}: {len(values)} matching reduced residual and ratio pairs", flush=True)
    for source in (stored, virtual):
        with opened(source) as table:
            np.testing.assert_array_equal(table.getcol("MODEL_DATA"), original_model)


def compare_scalar_antenna_channel(root, native, vis):
    from plotms_ms_spw_average_oracle import export

    controls = request(vis) | dict(xaxis="real", xdatacolumn="corrected/model_vector",
                                   yaxis="imag", ydatacolumn="data", avgtime="20",
                                   avgchannel="2", scalar=True, avgantenna=True)
    name = "scalar-time-channel-antennas"
    expected = export(root, f"{name}.casa", reference, controls)
    selected = rows(expected)
    assert [(point[2], point[5], point[6]) for point in selected] == [("0", "0", "-1"), ("0", "1", "-1")] * 2
    np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                               [(2.63505, 1.10715), (2.63505, -1.10715)] * 2, rtol=1.2e-5)
    assert compare_text(export(root, f"{name}.native", native, controls), expected) == 4
    print(f"{name}: 4 matching scalar reduced ratio points", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--calibrate-bin", type=Path)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("reduced",))
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
        if args.only == "reduced":
            compare_reduced_expressions(root / "reduced-expressions", native)
            scalar = root / "scalar-antenna-channel"
            scalar.mkdir()
            compare_scalar_antenna_channel(scalar, native, weighted_fixture(scalar))
            return
        weighted = weighted_fixture(root)
        compare_weighted(root, native, weighted)
        compare_iterator_order(root, native, weighted)
        compare_operand_edges(root, native, weighted)
        check_unimplemented_averaging(root, native, weighted)
        compare_reduced_expressions(root / "reduced-expressions", native)
        compare_scalar_antenna_channel(root, native, weighted)
        ordinary = root / "ordinary"
        ordinary.mkdir()
        vis, _ = fixture(ordinary)
        compare_channel_vector_residual(ordinary, native, vis)


if __name__ == "__main__":
    main()
