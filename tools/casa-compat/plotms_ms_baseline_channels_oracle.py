#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public PlotMS vector channel and baseline averages with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from plotms_ms_columns_oracle import (
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
        shape = table.getcol("DATA").shape
        indices = np.arange(shape[1])
        amplitudes = 2 * indices[None, :, None] + np.where(high, 4, 1)
        for factor, name in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            table.putcol(name, np.broadcast_to((amplitudes * 10**factor).astype(np.complex64), shape).copy())
        for name, value in (("WEIGHT", 9), ("SIGMA", 0.5)):
            weights = np.where(high, value, 1).astype(np.float32)
            table.putcol(name, np.broadcast_to(weights, (shape[0], shape[2])).copy())
    spectral, spectral_flagged, plain_flagged = (
        root / f"{name}.ms" for name in ("spectral", "spectral-flagged", "plain-flagged")
    )
    copy_ms(source, spectral)
    with opened(spectral, True) as table:
        descriptor = table.getcoldesc("DATA")
        for name, high_ratio, low_ratio in (("WEIGHT_SPECTRUM", 1, 9), ("SIGMA_SPECTRUM", 0.5, 2)):
            table.addcols({name: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                      dataManagerGroup=name)})
            spectrum = np.where(indices[:, None] % 2 == 0, 1, np.where(high, high_ratio, low_ratio))
            table.putcol(name, np.broadcast_to(spectrum.astype(np.float32), shape).copy())
    for original, flagged in ((source, plain_flagged), (spectral, spectral_flagged)):
        copy_ms(original, flagged)
        with opened(flagged, True) as table:
            flags = table.getcol("FLAG")
            flags[0, 1, ~high] = True
            flags[0, :2, high & (integration % 2 == 0)] = True
            table.putcol("FLAG", flags)
    return source, spectral, plain_flagged, spectral_flagged


def compare(root, native):
    source, spectral, plain_flagged, spectral_flagged = make_fixture(root)
    controls = request(source) | dict(
        antenna="A0&A1;A0&A2", xaxis="real", xdatacolumn="data",
        yaxis=["real", "real"], ydatacolumn=["corrected", "model"],
        yaxislocation=["left", "right"], spw="0:0~3", avgchannel="2", avgbaseline=True,
    )
    cases = (
        ("row-weights", source, [(4.7, 47), (9.7, 97)] * 6),
        ("spectra", spectral, [(217 / 110, 31), (163 / 22, 815 / 11)] * 6),
        ("row-weights-with-flags", plain_flagged,
         [(1, 10), (9.7, 97), (137 / 29, 1370 / 29), (284 / 29, 2840 / 29)] * 3),
        ("spectra-with-flags", spectral_flagged,
         [(1, 10), (7.5, 75), (134 / 65, 250 / 13), (100 / 13, 1000 / 13)] * 3),
    )
    for name, vis, corrected in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | dict(vis=str(vis), expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 24
        assert [point[2] for point in selected] == ["0", "1"] * 12
        assert [point[10] for point in selected] == ["100.000500000", "100.002500000"] * 12
        assert {tuple(point[5:9]) for point in selected} == {("-1", "-1", "*", "*")}
        model = [(x, 10 * y) for x, y in corrected]
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   corrected + model, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching channel-and-baseline coordinates", flush=True)
    figure = controls | dict(vis=str(spectral_flagged))
    compare_ordinate_figure(root, "spectra-with-flags", native, figure, "Real", ["Real:corrected", "Real:model"])
    compare_single_columns(root, native, controls, source, spectral, spectral_flagged)
    compare_first_correlation(root, native, controls, source, spectral)
    compare_vector_cancellation(root, native, controls, source)
    compare_spectrum_availability(root, native, controls, source)


def compare_single_columns(root, native, controls, source, spectral, spectral_flagged):
    cases = (
        ("observed-row-sigmas", source, "data", [4.4, 9.4] * 6),
        ("observed-spectrum", spectral, "data", [5, 67 / 7] * 6),
        ("calibrated-spectrum", spectral, "corrected", [31, 815 / 11] * 6),
        ("model-unit-weights", spectral_flagged, "model", [100, 850, 340, 880] * 3),
    )
    for name, vis, column, values in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | dict(vis=str(vis), xdatacolumn=column, yaxis="real",
                                  ydatacolumn=column, yaxislocation="left", expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 12
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   np.column_stack((values, values)), rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching channel-and-baseline coordinates", flush=True)


def compare_first_correlation(root, native, controls, source, spectral):
    fully_flagged = root / "first-correlation-flagged.ms"
    copy_ms(source, fully_flagged)
    with opened(fully_flagged, True) as table:
        high = table.getcol("ANTENNA2") == 2
        flags = table.getcol("FLAG")
        flags[0, :, high] = True
        table.putcol("FLAG", flags)
    zero_observed = root / "first-correlation-zero-observed.ms"
    copy_ms(spectral, zero_observed)
    with opened(zero_observed, True) as table:
        sigma = table.getcol("SIGMA_SPECTRUM")
        sigma[0, :, high] = 0
        table.putcol("SIGMA_SPECTRUM", sigma)
    cases = (
        ("first-correlation-flagged", fully_flagged,
         [(2, 20), (62 / 13, 620 / 13), (7, 70), (114 / 13, 1140 / 13)]),
        ("zero-observed-weights", zero_observed,
         [(133 / 110, 31), (217 / 110, 31), (133 / 22, 815 / 11), (657 / 110, 71)]),
    )
    for name, vis, integration in cases:
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        options = controls | dict(vis=str(vis), correlation="XX,YY", expformat="txt")
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        assert len(selected) == 48
        assert [point[12] for point in selected] == ["XX", "YY"] * 24
        model = [(x, 10 * y) for x, y in integration]
        np.testing.assert_allclose(np.array([point[:2] for point in selected], dtype=float),
                                   integration * 6 + model * 6, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == len(selected)
        print(f"{name}: {len(selected)} matching channel-and-baseline coordinates", flush=True)


def compare_vector_cancellation(root, native, controls, source):
    vis = root / "cancellation.ms"
    copy_ms(source, vis)
    with opened(vis, True) as table:
        phase = np.where(table.getcol("ANTENNA2") == 2, 1j, 1).astype(np.complex64)
        for index, column in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            data = table.getcol(column)
            data[:, 0, :] = 10**index * phase
            data[:, 1, :] = -10**index * phase
            table.putcol(column, data)
    options = controls | dict(vis=str(vis), xaxis="amp", yaxis=["amp", "amp"],
                              spw="0:0~1", expformat="txt")
    expected, actual = (root / f"cancellation.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 12
    np.testing.assert_array_equal(np.array([point[:2] for point in selected], dtype=float), 0)
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    print(f"cancellation: {len(selected)} matching channel-and-baseline coordinates", flush=True)


def make_reversed_partial_spectrum(source, vis):
    copy_ms(source, vis)
    with opened(vis, True) as table:
        selected = ((table.getcol("FIELD_ID") == 0) & (table.getcol("ANTENNA1") == 0)
                    & np.isin(table.getcol("ANTENNA2"), [1, 2]))
        first_selected = int(np.flatnonzero(selected)[0])
        timestamps, integration = np.unique(table.getcol("TIME"), return_inverse=True)
        reversed_times = timestamps[-1 - integration]
        table.putcol("TIME", reversed_times)
        table.putcol("TIME_CENTROID", reversed_times)
        descriptor = table.getcoldesc("DATA")
        for column in ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"):
            table.addcols({column: dict(descriptor, valueType="float", dataManagerType="StandardStMan",
                                        dataManagerGroup=column)})
        shape = table.getcol("DATA").shape
        weights = np.ones(shape[:2], dtype=np.float32)
        for row in range(table.nrows()):
            if row != first_selected:
                table.putcell("WEIGHT_SPECTRUM", row, weights)
        assert not table.iscelldefined("WEIGHT_SPECTRUM", first_selected)
        data = np.zeros(shape, dtype=np.complex64)
        data[:, 1, :] = 10
        table.putcol("DATA", data)
        sigma = np.ones(shape, dtype=np.float32)
        sigma[:, 1, :] = 0.5
        table.putcol("SIGMA_SPECTRUM", sigma)
    return vis


def compare_spectrum_availability(root, native, controls, source):
    vis = make_reversed_partial_spectrum(source, root / "reversed-partial-spectrum.ms")
    options = controls | dict(vis=str(vis), xdatacolumn="data", yaxis="real", ydatacolumn="data",
                              yaxislocation="left", spw="0:0~1", expformat="txt")
    expected, actual = (root / f"spectrum-availability.{engine}.txt" for engine in ("casa", "native"))
    assert reference(**options, plotfile=str(expected)) is None
    selected = rows(expected)
    assert len(selected) == 6
    np.testing.assert_array_equal(np.array([point[:2] for point in selected], dtype=float), 5)
    assert native(**options, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == len(selected)
    print(f"spectrum-availability: {len(selected)} matching channel-and-baseline coordinates", flush=True)


def compare_baseline_ordinate_lists(root, native):
    from plotms_ms_channel_ordinates_oracle import compare_sides

    root.mkdir()
    vis, _, _, spectra_flags = make_fixture(root)
    controls = request(vis) | dict(antenna="A0&A1;A0&A2", xaxis="channel", xdatacolumn="", spw="0:0~3",
                                    avgbaseline=True, yaxis=["real"] * 3,
                                    ydatacolumn=["data", "corrected", "model"],
                                    yaxislocation=["left", "right", "left"])
    for name, overrides, channels, first in (
        ("vector-baseline", {}, (0, 1, 3) * 6, (3.7, 37, 370)),
        ("scalar-baseline", dict(scalar=True), (0, 1, 3) * 6, (3.7, 37, 370)),
        ("channel-baseline-with-masks", dict(vis=str(spectra_flags), avgchannel="2"),
         (0, 1) * 6, (1, 10, 100)),
        ("time-before-baseline", dict(avgtime="20"), (0, 1, 3) * 2, (3.7, 37, 370)),
    ):
        options = controls | overrides | dict(expformat="txt")
        expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
        assert reference(**options, plotfile=str(expected)) is None
        selected = rows(expected)
        count = 3 * len(channels)
        assert len(selected) == count
        assert tuple(int(point[0]) for point in selected) == channels * 3
        assert {tuple(point[5:9]) for point in selected} == {("-1", "-1", "*", "*")}
        assert {point[12] for point in selected} == {"XX"}
        np.testing.assert_allclose([float(selected[index * len(channels)][1]) for index in range(3)],
                                   first, rtol=1.2e-5)
        assert native(**options, plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        print(f"{name}: {count} matching baseline-averaged ordinate coordinates", flush=True)
        if name == "channel-baseline-with-masks":
            figure = controls | overrides | dict(xlabel="Channel bin")
            compare_sides(root, native, figure, expected_x="Channel bin", name=name)
    compare_baseline_ordinate_edges(root, native, vis, spectra_flags, controls)


def compare_baseline_ordinate_edges(root, native, vis, spectra_flags, controls):
    from casars.tasks import TaskParameterError

    cancellation, flagged = (root / f"{name}.ms" for name in ("opposite-phases", "all-cube-flagged"))
    copy_ms(vis, cancellation)
    copy_ms(vis, flagged)
    with opened(cancellation, True) as table:
        high = table.getcol("ANTENNA2") == 2
        amplitude = np.where(high, -1., 3.).astype(np.complex64)
        weight = table.getcol("WEIGHT")
        weight[:] = np.where(high, 3., 1.)
        table.putcol("WEIGHT", weight)
        for factor, column in enumerate(("DATA", "CORRECTED_DATA", "MODEL_DATA")):
            shape = table.getcol(column).shape
            table.putcol(column, np.broadcast_to(amplitude * 10**factor, shape))
    with opened(flagged, True) as table:
        flags = table.getcol("FLAG")
        flags[:] = True
        table.putcol("FLAG", flags)
    for mode, scalar, amplitudes in (("vector", False, (0, 0, 0)), ("scalar", True, (1.5, 15, 150))):
        for case, overrides, count in (
            ("opposite-phases", dict(vis=str(cancellation), yaxis=["amp"] * 3), 54),
            ("time-and-channel", dict(vis=str(spectra_flags), avgtime="20", avgchannel="2"), 12),
            ("all-cube-flagged", dict(vis=str(flagged)), 0),
        ):
            name = f"{mode}-{case}"
            options = controls | overrides | dict(scalar=scalar, expformat="txt")
            expected, actual = (root / f"{name}.{engine}.txt" for engine in ("casa", "native"))
            assert reference(**options, plotfile=str(expected)) is None
            selected = rows(expected)
            assert len(selected) == count
            if case == "opposite-phases":
                np.testing.assert_allclose([float(point[1]) for point in selected],
                                           np.repeat(amplitudes, 18), rtol=1.2e-5)
            assert native(**options, plotfile=str(actual)) is None
            if count:
                assert compare_text(actual, expected) == count
            else:
                assert expected.read_text().count("# No data to export") == 3
                assert actual.read_bytes() == expected.read_bytes()
            print(f"{name}: {count if count else 'three empty sections'} matched", flush=True)
    for name, overrides in (("overlay", dict(showatm=True)), ("iteration", dict(iteraxis="corr"))):
        output = root / f"baseline-list-{name}.txt"
        options = controls | overrides | dict(plotfile=str(output), expformat="txt")
        try:
            native(**options)
        except TaskParameterError as error:
            assert "#1032" in str(error), error
        else:
            raise AssertionError(f"PlotMS accepted the pending baseline-averaged list with {name}")
        assert not output.exists()
        print(f"baseline-list-{name}: existing refusal preserved", flush=True)


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
        compare_baseline_ordinate_lists(root / "ordinate-lists", native)


if __name__ == "__main__":
    main()
