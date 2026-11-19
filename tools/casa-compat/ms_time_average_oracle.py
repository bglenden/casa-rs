#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare time averaging and joint time/channel averaging with pinned CASA."""

from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_channel_average_oracle import COMPLEX, SPECTRA, add_statistics, channel_parameters, compare_array
from ms_data_columns_oracle import add_float_and_lag, clone
from ms_selection_tasks_oracle import MAIN, add_observation_and_intent, add_second_window, casa_table, make_input

TIMES = ("TIME", "TIME_CENTROID")
SCALARS = ("TIME_CENTROID", "INTERVAL", "EXPOSURE")


def compare(root, case, source, task, selector, timebin, width=1, combine="", *,
            spw="", correlation="", keepflags=True, force_spectra=False, max_motion=0):
    targets = [root / f"{task}-{case}-{side}.ms" for side in ("casa", "native")]
    args = channel_parameters(task, width, average=width > 1)
    args.update(timebin=timebin, datacolumn=selector, spw=spw, correlation=correlation,
                keepflags=keepflags)
    if task == "split":
        args["combine"] = combine
    else:
        args.update(timeaverage=True, timespan=combine, usewtspectrum=force_spectra,
                    maxuvwdistance=max_motion)
    for module, output in zip((casatasks, tasks), targets):
        result = getattr(module, task)(vis=str(source), outputvis=str(output), **args)
        assert result is None, (task, case, module.__name__, result)
    with (table(str(targets[0]), ack=False) as want,
          table(str(targets[1]), ack=False) as got, casa_table(targets[1]) as readback):
        optional = set(COMPLEX + SPECTRA + ("FLOAT_DATA",))
        names = optional.intersection(want.colnames())
        assert optional.intersection(got.colnames()) == names, (task, case)
        assert want.nrows() == got.nrows(), (task, case, want.nrows(), got.nrows())
        for name in sorted((set(MAIN + SCALARS) - {"DATA"}) | names):
            for row in range(want.nrows()):
                actual = np.asarray(got.getcell(name, row))
                expected = want.getcell(name, row)
                label = f"{task} {case} {name} row {row}"
                if name in TIMES:
                    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-5, err_msg=label)
                else:
                    compare_array(label, actual, expected)
                np.testing.assert_array_equal(np.asarray(readback.getcell(name, row)).T, actual,
                                              err_msg=label)
    for name, columns in (
        ("DATA_DESCRIPTION", ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID")),
        ("SPECTRAL_WINDOW", ("NUM_CHAN", "CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW")),
    ):
        with (table(str(targets[0] / name), ack=False) as want,
              table(str(targets[1] / name), ack=False) as got):
            assert want.nrows() == got.nrows(), (task, case, name)
            for row in range(want.nrows()):
                for column in columns:
                    np.testing.assert_array_equal(got.getcell(column, row), want.getcell(column, row),
                                                  err_msg=f"{task} {case} {name} {column} row {row}")


def reject_timebin(root, source, task, value, kind, diagnostic):
    for module in (casatasks, tasks):
        output = root / f"{task}-bad-time-{value}-{module.__name__}.ms"
        extra = {"timeaverage": True} if task == "mstransform" else {}
        try:
            getattr(module, task)(vis=str(source), outputvis=str(output), datacolumn="data",
                                  timebin=value, **extra)
        except kind as error:
            assert diagnostic in str(error), str(error)
        else:
            raise AssertionError((module.__name__, value))
        assert not output.exists(), output


def verify_time_lags(root, source, task, width):
    output = [root / f"{task}-lags-{width}-{side}.ms" for side in ("casa", "native")]
    kwargs = channel_parameters(task, width, average=width > 1)
    kwargs.update(timebin="25s", datacolumn="lag_data")
    if task == "mstransform":
        kwargs["timeaverage"] = True
    for module, target in zip((casatasks, tasks), output):
        getattr(module, task)(vis=str(source), outputvis=str(target), **kwargs)
    with (table(str(source), ack=False) as original,
          table(str(output[0]), ack=False) as reference,
          table(str(output[1]), ack=False) as native, casa_table(output[1]) as readback):
        assert not reference.iscelldefined("LAG_DATA", 0)
        assert not reference.iscelldefined("FLAG", 0)
        ids = ("SCAN_NUMBER", "ANTENNA1", "ANTENNA2")
        for name in (*ids, "TIME", "INTERVAL", "EXPOSURE", "TIME_CENTROID"):
            np.testing.assert_allclose(native.getcol(name), reference.getcol(name), rtol=0, atol=1e-5)
        input_tags = [original.getcol(name) for name in ids]
        output_tags = [native.getcol(name) for name in ids]
        original_flags = original.getcol("FLAG")
        raw_weights = original.getcol("WEIGHT")
        weight_spectrum = original.getcol("WEIGHT_SPECTRUM")
        times = original.getcol("TIME")
        for row, tags in enumerate(zip(*output_tags)):
            same_baseline = np.logical_and.reduce([column == tag for column, tag in zip(input_tags, tags)])
            baseline = np.flatnonzero(same_baseline)
            time = native.getcell("TIME", row)
            intervals = [baseline[:3], baseline[3:]]
            group_index = int(time >= times[baseline[3]] - 1e-5)
            group = intervals[group_index]
            defined_lags = [int(index) for index in group if original.iscelldefined("LAG_DATA", int(index))]
            expected = []
            for corr in range(original_flags.shape[-1]):
                flagged = original_flags[defined_lags, :, corr].all(axis=1)
                valid = [index for index, flag in zip(defined_lags, flagged) if flag == flagged.all()]
                lag_values = np.stack([original.getcell("LAG_DATA", index)[:, corr] for index in valid])
                expected.append(np.average(lag_values, axis=0, weights=raw_weights[valid, corr]))
            flags = original_flags[group].all(axis=0)
            selected = original_flags[group] == flags[None, :, :]
            weights = np.where(selected, weight_spectrum[group], 0).sum(axis=0)
            if width == 2:
                components = flags.reshape(2, 2, flags.shape[-1])
                flags = components.all(axis=1)
                weights = np.where(components == flags[:, None, :], weights.reshape(components.shape), 0).sum(axis=1)
            np.testing.assert_allclose(native.getcell("LAG_DATA", row), np.stack(expected, axis=1), rtol=1e-6)
            np.testing.assert_array_equal(native.getcell("FLAG", row), flags)
            np.testing.assert_allclose(native.getcell("WEIGHT_SPECTRUM", row), weights, rtol=1e-6)
            sigmas = np.full(weights.shape, -1, dtype=np.float32)
            np.divide(1, np.sqrt(weights), out=sigmas, where=weights > 0)
            np.testing.assert_allclose(native.getcell("SIGMA_SPECTRUM", row), sigmas, rtol=1e-6)
            included = flags == flags.all(axis=0)
            scalar_weight = np.where(included, weights, 0).sum(axis=0) / included.sum(axis=0)
            if width == 1:
                scalar_sigma = np.full(scalar_weight.shape, -1.0)
                np.divide(1, np.sqrt(scalar_weight), out=scalar_sigma, where=scalar_weight > 0)
            else:
                scalar_sigma = np.where(included, sigmas, 0).sum(axis=0) / included.sum(axis=0)
            np.testing.assert_allclose(native.getcell("WEIGHT", row), scalar_weight, rtol=1e-6)
            np.testing.assert_allclose(native.getcell("SIGMA", row), scalar_sigma, rtol=1e-6)
            for name in ("LAG_DATA", "FLAG", *SPECTRA, "WEIGHT", "SIGMA"):
                np.testing.assert_array_equal(readback.getcell(name, row).T, native.getcell(name, row))


def verify_mixed_visibilities(root, source, task, width):
    targets = [root / f"{task}-mixed-{width}-{side}.ms" for side in ("casa", "native")]
    kwargs = channel_parameters(task, width, average=width > 1)
    kwargs.update(timebin="25s", datacolumn="all")
    if task == "mstransform":
        kwargs["timeaverage"] = True
    for module, target in zip((casatasks, tasks), targets):
        getattr(module, task)(vis=str(source), outputvis=str(target), **kwargs)
    with (table(str(targets[0]), ack=False) as mixed_casa,
          table(str(targets[1]), ack=False) as native,
          table(str(root / f"{task}-all-{width}-casa.ms"), ack=False) as complex_rows,
          table(str(root / f"{task}-float-{width}-casa.ms"), ack=False) as float_rows,
          table(str(root / f"{task}-lags-{width}-native.ms"), ack=False) as lags,
          casa_table(targets[1]) as readback):
        assert not mixed_casa.iscelldefined("LAG_DATA", 0)
        for name in (*COMPLEX, "WEIGHT_SPECTRUM"):
            assert not np.allclose(mixed_casa.getcol(name), complex_rows.getcol(name)), (task, width, name)
        for reference, names in ((complex_rows, (*MAIN, *SCALARS, *COMPLEX[1:], *SPECTRA)),
                                 (float_rows, ("FLOAT_DATA",)), (lags, ("LAG_DATA",))):
            for name in names:
                actual = native.getcol(name)
                expected = reference.getcol(name)
                if name in TIMES:
                    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-5, err_msg=name)
                else:
                    compare_array(f"{task} mixed {width} {name}", actual, expected)
                np.testing.assert_array_equal(readback.getcol(name).T, actual)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="ms-time-average-") as work:
        root = Path(work)
        source = root / "input.ms"
        make_input(source, channels=4)
        add_statistics(source)
        extra = root / "optional-visibilities.ms"
        clone(source, extra)
        add_float_and_lag(extra, missing_first_lag=True)
        floats = root / "float-only.ms"
        lags = root / "lag-only.ms"
        clone(extra, floats, (*COMPLEX, "LAG_DATA"))
        clone(extra, lags, (*COMPLEX, "FLOAT_DATA"))
        scalars = root / "scalar-statistics.ms"
        clone(source, scalars, SPECTRA)
        windows = root / "windows.ms"
        clone(source, windows)
        add_second_window(windows)
        contexts = root / "contexts.ms"
        clone(source, contexts)
        add_observation_and_intent(contexts, interleaved=True)
        with table(str(source), ack=False) as ms:
            source_flags = ms.getcol("FLAG")
            source_data = ms.getcol("DATA")
        count = 0
        for task in ("split", "mstransform"):
            for selector in ("data", "corrected", "model", "all"):
                for width in (1, 2):
                    compare(root, f"{selector}-{width}", source, task, selector, "25s", width)
                    count += 1
            for width in (1, 2):
                compare(root, f"float-{width}", floats, task, "float_data", "25s", width)
                verify_time_lags(root, lags, task, width)
                verify_mixed_visibilities(root, extra, task, width)
                count += 3
            for name, duration in (("exact", "20s"), ("off-boundary", "21s"),
                                   ("minutes", "1min"), ("subintegrations", "500ms"),
                                   ("bare-value", "20"), ("sidereal-angle", "1:00:00"),
                                   ("scaled-non-time-unit", "2000cm")):
                compare(root, name, source, task, "data", duration)
                count += 1
            compare(root, "scalar", scalars, task, "all", "25s", 2)
            compare(root, "all-contexts", contexts, task, "data", "75s", combine="scan,state,field")
            compare(root, "combined-scans-fields", source, task, "corrected", "75s", combine="scan,field")
            compare(root, "multiple-spws", windows, task, "data", "25s", 2)
            compare(root, "selected-and-dropped-flag-rows", windows, task, "data", "25s", 2,
                    spw="0:1~3", correlation="XX", keepflags=False)
            count += 5
        compare(root, "forced-spectra", scalars, "mstransform", "corrected", "25s", force_spectra=True)
        compare(root, "baseline-motion", source, "mstransform", "data", "35s", max_motion=0.02)
        compare(root, "negative-inactive", source, "split", "data", "-1s")
        count += 3
        for duration in ("0s", "-1s"):
            reject_timebin(root, source, "mstransform", duration, ValueError, "Parameter timebin must be > '0s'")
        for task in ("split", "mstransform"):
            reject_timebin(root, source, task, "bogus", RuntimeError, "Illegal input units or format")
            reject_timebin(root, source, task, None, AssertionError, "null value not allowed")
            reject_timebin(root, source, task, 20, AssertionError, "must be of cStr type")
        with table(str(source), ack=False) as ms:
            np.testing.assert_array_equal(ms.getcol("FLAG"), source_flags)
            np.testing.assert_array_equal(ms.getcol("DATA"), source_data)
        print(f"{count} time-averaged products and eight time-bin rejections compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
