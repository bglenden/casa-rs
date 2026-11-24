#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare observed residuals and ratios after selected channel averaging."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import numpy as np

from plotms_ms_columns_oracle import (
    casa, compare_figure, compare_text, copy_ms, opened, plotms_launcher, reference, request, rows,
)
from plotms_ms_derived_average_oracle import weighted_fixture
from plotms_ms_scalar_derived_average_oracle import compare_case


def compare_weighted(root, native, vis):
    observed = np.array([2.6 + 5j, 11 + 1j])
    model = np.array([1.9 + 1.7j, 5 - 3j])
    corrected_imag = [-0.6, 4]
    base = request(vis) | dict(avgchannel="2", spw="0:0~3", title="Observed channel averages")
    for name, axis, column, coordinates in (
        ("observed-residual", "real", "data-model_vector", (observed - model).real),
        ("observed-ratio", "real", "data/model_vector", (observed / model).real),
        ("observed-scalar-amp", "amp", "data-model_scalar", np.abs(observed) - np.abs(model)),
        ("observed-scalar-phase", "phase", "data/model_scalar", np.degrees(np.angle(observed) / np.abs(model))),
    ):
        controls = base | dict(xaxis=axis, xdatacolumn=column, yaxis="imag", ydatacolumn="corrected")
        expected = compare_case(root, native, name, controls, np.column_stack([coordinates, corrected_imag]))
        if name == "observed-scalar-phase":
            compare_figure(root, native, controls, expected,
                           (base["title"], "Phase:data/model (scalar) (degrees)", "Imag:corrected"))
    controls = base | dict(xaxis="imag", xdatacolumn="data", yaxis="real", ydatacolumn="data/model_vector")
    compare_case(root, native, "observed-ratio-ordinate", controls,
                 np.column_stack([observed.imag, (observed / model).real]))

    without_corrected = root / "without-corrected.ms"
    copy_ms(vis, without_corrected)
    with opened(without_corrected, True) as table:
        table.removecols(["CORRECTED_DATA"])
    observed_model = np.array([1.8 + 1.4j, model[1]])
    controls = base | dict(vis=str(without_corrected), xaxis="real", xdatacolumn="data/model_vector",
                           yaxis="imag", ydatacolumn="data")
    coordinates = np.column_stack([(observed / observed_model).real, observed.imag])
    compare_case(root, native, "observed-ratio-without-corrected", controls, coordinates)
    compare_case(root, native, "corrected-ratio-fallback",
                 controls | dict(xdatacolumn="corrected/model_vector"), coordinates)


def compare_weight_edges(root, native, vis):
    base = request(vis) | dict(avgchannel="2", spw="0:0~3", xaxis="real",
                               xdatacolumn="data/model_vector", yaxis="imag")
    zero_model_weight = root / "zero-model-weight.ms"
    copy_ms(vis, zero_model_weight)
    with opened(zero_model_weight, True) as table:
        weights = table.getcol("WEIGHT_SPECTRUM")
        weights[:, :2, :] = 0
        table.putcol("WEIGHT_SPECTRUM", weights)
    compare_case(root, native, "zero-model-weight",
                 base | dict(vis=str(zero_model_weight), ydatacolumn="data"),
                 [(np.inf, 5), (52 / 34, 1)])

    mixed = root / "mixed-storage.ms"
    copy_ms(vis, mixed)
    with opened(mixed, True) as table:
        descriptor = table.getcoldesc("DATA")
        descriptor.update(name="FLOAT_DATA", valueType="float",
                          dataManagerType="StandardStMan", dataManagerGroup="FloatVis")
        table.addcols({"FLOAT_DATA": descriptor})
        table.putcol("FLOAT_DATA", table.getcol("DATA").real - 6)
    controls = base | dict(vis=str(mixed), ydatacolumn="corrected")
    check_effective_float(root, native, controls)
    compare_case(root, native, "unused-float",
                 controls | dict(xdatacolumn="corrected/model_vector"),
                 [(13.04 / 6.5, -0.6), (48 / 34, 4)])


def check_effective_float(root, native, controls):
    from casars.tasks import TaskParameterError

    expected, actual = (root / f"effective-float.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    values = np.array([point[:2] for point in rows(expected)], dtype=float)
    # This expression consumes FLOAT_DATA even with a physical DATA column.
    np.testing.assert_allclose(values, [(-3.4 * 1.9 / 6.5, -0.6), (25 / 34, 4)] * 6, rtol=1.2e-5)
    try:
        native(**controls, plotfile=str(actual), expformat="txt")
    except TaskParameterError as error:
        assert "averaging MeasurementSet FLOAT_DATA is pending #1032" in str(error), error
    else:
        raise AssertionError("PlotMS used the stored complex DATA in a FLOAT_DATA average")
    assert not actual.exists()
    print("effective-float: reference uses weighted FLOAT_DATA and native retains its refusal", flush=True)


def compare_initial_spectrum(root, native, vis):
    source = root / "initial-spectrum-undefined.ms"
    copy_ms(vis, source)
    with opened(source, True) as table:
        times = table.getcol("TIME")
        times = times.min() + times.max() - times
        table.putcol("TIME", times)
        table.putcol("TIME_CENTROID", times)
        table.putcol("SCAN_NUMBER", np.ones_like(table.getcol("SCAN_NUMBER")))
        selected = np.flatnonzero((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 1)
                                  & (table.getcol("ANTENNA2") == 2))
        first = int(selected[0])
        assert first > 0 and times[first] > times[int(selected[-1])]
        column = "WEIGHT_SPECTRUM"
        weights = table.getcol(column)
        descriptor = table.getcoldesc(column)
        table.removecols([column])
        table.addcols({column: descriptor})
        for row in range(table.nrows()):
            if row != first:
                table.putcell(column, row, weights[:, :, row])
        assert not table.iscelldefined(column, first)
        assert table.iscelldefined(column, int(selected[-1]))

    # The selected table's first row has no spectrum, even though the
    # iterator starts with a defined spectrum after sorting by time.
    controls = request(source) | dict(antenna="A1&A2", avgchannel="2", spw="0:0~3",
                                      xaxis="real", xdatacolumn="data/model_vector",
                                      yaxis="imag", ydatacolumn="corrected")
    compare_case(root, native, "initial-spectrum-undefined", controls, [(1.9, 1), (52 / 34, 4)])


def compare_nonfinite_components(root, native, vis):
    source = root / "nonfinite-components.ms"
    copy_ms(vis, source)
    variants = (
        (complex(1, np.nan), (np.nan, np.nan)),
        (complex(np.inf, 2), (np.inf, np.nan)),
        (complex(2, np.inf), (np.nan, np.inf)),
        (complex(np.inf, np.nan), (np.inf, np.nan)),
        (complex(np.inf, np.inf), (np.inf, np.inf)),
        (complex(-np.inf, 2), (-np.inf, np.nan)),
    )
    with opened(source, True) as table:
        selected = np.flatnonzero((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
                                  & (table.getcol("ANTENNA2") == 1))
        observed, times = table.getcol("DATA"), table.getcol("TIME")
        by_time = {}
        for row, (value, mean) in zip(selected, variants, strict=True):
            observed[:, 0, row] = value
            by_time[f"{times[row]:.3f}"] = mean
        table.putcol("DATA", observed)
    controls = request(source) | dict(avgchannel="2", spw="0:0~3",
                                      xaxis="real", xdatacolumn="data-model_scalar",
                                      yaxis="imag", ydatacolumn="data")
    expected, actual = (root / f"nonfinite-components.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**controls, plotfile=str(expected), expformat="txt") is None
    selected = rows(expected)
    assert [(point[2], point[12]) for point in selected] == [("0", "XX"), ("1", "XX")] * 6
    coordinates = [by_time[point[9]] if point[2] == "0" else (6, 1) for point in selected]
    np.testing.assert_array_equal(np.array([point[:2] for point in selected], dtype=float), coordinates)
    assert native(**controls, plotfile=str(actual), expformat="txt") is None
    assert compare_text(actual, expected) == 12
    print("nonfinite-components: 12 matching selected points", flush=True)


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
        vis = weighted_fixture(root)
        compare_weighted(root, native, vis)
        compare_weight_edges(root, native, vis)
        compare_initial_spectrum(root, native, vis)
        compare_nonfinite_components(root, native, vis)


if __name__ == "__main__":
    main()
