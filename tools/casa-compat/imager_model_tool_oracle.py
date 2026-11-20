#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the public calibration-facing imager against CASA 6.7.6.14."""

import argparse
from itertools import product
import json
from pathlib import Path
import shutil

import casatasks
from casatools import imager as reference, table
import numpy as np

from casars.tools import imager as native, ms

from bandpass_oracle import source_data
from calibration_ft_oracle import compare_outputs, component, coordinates, initial_time_range
from calibration_scratch_oracle import MAIN, prepare, state
from gaincal_oracle import relative_rms
from imager_prediction_options_oracle import configure_data, write_model


def reject(call, text):
    try:
        call()
    except RuntimeError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected RuntimeError containing {text!r}")


def attachment_cases(root, seed, point):
    fresh = []
    for factory in (reference, native):
        tool = factory()
        fresh.append([tool.close(), tool.done(), tool.selectvis(), tool.ft(),
                      tool.settaylorterms(), tool.uvrange()])
    assert fresh[0] == fresh[1] == [True, True, False, False, False, False]
    for existing, compress in product((False, True), repeat=2):
        reports = []
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"attachment-{existing}-{compress}-{label}.ms"
            prepare(seed, path, model=existing, virtual=True)
            before = state(path)
            assert sorted(before[1][1]) == ["definedmodel_field_0", "definedmodel_field_1",
                                            "model_0", "model_1"]
            tool = factory()
            assert tool.open(str(path), compress, True)
            attached = state(path)
            assert attached[1][:2] == ({}, {})
            if existing:
                np.testing.assert_array_equal(attached[0]["MODEL_DATA"], before[0]["MODEL_DATA"])
            reject(lambda: tool.uvrange(), "ROWID() IN []")
            assert tool.ft() and tool.ft(model=(str(root / "ignored.image"),))
            compare_outputs(state(path), attached)
            assert not tool.ft(complist=str(root / "absent.cl"))
            assert not tool.ft(model=[str(root / "absent.image")])
            compare_outputs(state(path), attached)
            assert tool.ft(complist=str(point))
            assert tool.close() and tool.done()
            reports.append(state(path))
        compare_outputs(*reports)
    return 4


def selected_prediction(root, seed, point):
    # Explicit model channels take precedence over a channel clause in spw.
    cases = [
        ("stepped-spw", {"spw": "0:2~8^3"}, [2, 5, 8]),
        ("explicit", {"spw": [0], "nchan": 3, "start": 2, "step": 2}, [2, 4, 6]),
        ("override", {"spw": "0:2~8^3", "nchan": [2], "start": [1], "step": [2]}, [1, 3]),
        ("zero-step", {"spw": "0", "nchan": 3, "start": 2, "step": 0}, [2, 3, 4]),
        ("float-vector", {"spw": 0, "nchan": np.array([2.9]), "start": 3, "step": 2}, [3, 5]),
        ("empty-count", {"spw": "0", "nchan": [], "start": 2, "step": 2}, list(range(12))),
    ]
    reports = {}
    for mode, (name, selection, channels) in product(("open", "select"), cases):
        results = []
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"channels-{mode}-{name}-{label}.ms"
            prepare(seed, path, model=True)
            before = state(path)
            tool = factory()
            if mode == "open":
                assert tool.open(str(path), usescratch=True)
                # Direct open keeps the previously opened MeasurementSet.
                selection_path = root / "unused.ms"
            else:
                selection_path = path
            assert tool.selectvis(str(selection_path), field=0, usescratch=True, **selection)
            reject(lambda: tool.uvrange(100., 10.), "Invalid uvmin and uvmax")
            assert tool.uvrange(0., 0.)
            assert tool.ft(complist=str(point))
            assert tool.done()
            after = state(path)
            tb = table()
            assert tb.open(str(path))
            fields = tb.getcol("FIELD_ID")
            tb.close()
            original = before[0]["MODEL_DATA"]
            actual = after[0]["MODEL_DATA"]
            changed = np.any(actual != original, axis=0)
            np.testing.assert_array_equal(changed,
                                          np.isin(np.arange(12), channels)[:, None] & (fields == 0))
            for col in MAIN:
                if col in before[0]:
                    np.testing.assert_array_equal(after[0][col], before[0][col])
            results.append(after)
        compare_outputs(*results)
        reports[f"{mode}-{name}"] = int(np.count_nonzero(changed) * actual.shape[0])
    return reports


def image_predictions(root):
    reports = {}
    for case in ("incremental", "taylor", "phase-time"):
        result = []
        for label, factory in (("casa", reference), ("native", native)):
            path = root / f"image-predict-{case}-{label}.ms"
            configure_data(path, case)
            tb = table()
            assert tb.open(str(path / "SPECTRAL_WINDOW"))
            reference_hz = float(tb.getcell("CHAN_FREQ", 0)[0])
            tb.close()
            first = root / f"{case}-{label}-term0.im"
            write_model(first, coordinates(path), reference_hz, 2.)
            tool = factory()
            assert tool.open(str(path), usescratch=True)
            assert tool.selectvis(field=0)
            if case == "taylor":
                second = root / f"{case}-{label}-term1.im"
                write_model(second, coordinates(path), reference_hz, .5)
                assert tool.settaylorterms(2, reference_hz * .99)
                assert tool.ft([str(first), str(second)])
            elif case == "phase-time":
                assert tb.open(str(path))
                epoch = float(tb.getcell("TIME", tb.nrows() - 1))
                tb.close()
                assert tool.ft(str(first), phasecentertime=epoch)
            else:
                assert tool.ft(str(first))
                before = state(path)[0]["MODEL_DATA"]
                assert not tool.ft(str(first), incremental=True)
                np.testing.assert_array_equal(state(path)[0]["MODEL_DATA"], before)
                assert tool.selectvis(field=0)
                assert tool.ft(str(first), incremental=True)
                after = state(path)[0]["MODEL_DATA"]
                assert np.max(np.abs(before)) > 1.5
                np.testing.assert_allclose(np.max(np.abs(after)), 2. * np.max(np.abs(before)),
                                           rtol=1e-4, atol=0.)
            assert tool.done()
            result.append(state(path))
        first, second = [item[0]["MODEL_DATA"] for item in result]
        assert np.max(np.abs(first)) > 1.5
        compare_outputs(*result)
        error = relative_rms(first, second)
        reports[case] = {"samples": first.size, "nrms": error}
    return reports


def access_and_reopen(root, seed, point):
    reports = []
    for label, factory in (("casa", reference), ("native", native)):
        first = root / f"access-first-{label}.ms"
        second = root / f"access-second-{label}.ms"
        prepare(seed, first, model=True)
        prepare(seed, second, model=True)
        first_before = state(first)
        tool = factory()
        assert tool.selectvis(str(first), usescratch=True, writeaccess=False)
        reject(lambda: tool.ft(complist=str(point)), "Please make sure MS is writable")
        assert tool.done()
        compare_outputs(state(first), first_before)
        reject(lambda: tool.selectvis(str(first), datainmemory=True), "NullTable::getColumn")
        assert tool.open(str(second), usescratch=True)
        assert not tool.selectvis(field="made-up-field")
        assert not tool.ft(complist=str(point))
        assert tool.selectvis(field=0, baseline="0&1", time=initial_time_range(second), scan=1,
                              intent="CALIBRATE_FLUX#ON_SOURCE", observation=0,
                              uvrange=">0m", taql="ANTENNA1!=ANTENNA2")
        assert tool.ft(complist=str(point))
        assert tool.done()
        compare_outputs(state(first), first_before)
        result = state(second)
        assert np.any(result[0]["MODEL_DATA"] != 13. + 7.j)
        reports.append(result)
    compare_outputs(*reports)
    return int(np.count_nonzero(reports[0][0]["MODEL_DATA"] != 13. + 7.j))


def multi_input(root, seed, point):
    next_point = root / "next-point.cl"
    component(next_point, coordinates(seed), 8.)
    outcomes = []
    for label, factory in (("casa", reference), ("native", native)):
        tool = factory()
        paths = [root / f"multi-{field}-{label}.ms" for field in (0, 1)]
        for field, path in enumerate(paths):
            prepare(seed, path, model=True)
            assert tool.selectvis(str(path), spw="0:2~8^3", field=field, usescratch=True)
        assert tool.ft(complist=str(point))
        both = [state(path) for path in paths]
        assert all(np.any(snapshot[0]["MODEL_DATA"] != 13. + 7.j) for snapshot in both)
        assert tool.uvrange(0., 0.)
        assert not tool.ft(complist=str(next_point))
        for path, expected in zip(paths, both, strict=True):
            compare_outputs(state(path), expected)
        assert tool.close()
        for field, path in enumerate(paths):
            assert tool.selectvis(str(path), spw="0:2~8^3", field=field, usescratch=True)
        assert tool.uvrange(0., 0.)
        assert tool.ft(complist=str(next_point))
        assert tool.done()
        last = [state(path) for path in paths]
        compare_outputs(last[0], both[0])
        assert np.any(last[1][0]["MODEL_DATA"] != both[1][0]["MODEL_DATA"])
        outcomes.append((*both, *last))
    for expected, actual in zip(*outcomes, strict=True):
        compare_outputs(actual, expected)
    return [int(np.count_nonzero(actual[0]["MODEL_DATA"] != 13. + 7.j))
            for actual in outcomes[0][:2]]


def virtual_interchange(root, seed, point):
    expected = None
    reports = {}
    for writer, factory in (("casa", reference), ("native", native)):
        saved = root / f"virtual-{writer}.ms"
        prepare(seed, saved, corrected=True, virtual=True)
        tool = factory()
        assert tool.open(str(saved), usescratch=False)
        assert tool.selectvis(field=0)
        assert tool.ft(complist=str(point))
        assert tool.done()
        before, recipes = state(saved)
        assert "MODEL_DATA" not in before
        for reader in ("casa", "native"):
            path = root / f"virtual-{writer}-{reader}.ms"
            shutil.copytree(saved, path)
            if reader == "casa":
                assert casatasks.uvsub(str(path)) is None
            else:
                with_ms = ms()
                assert with_ms.open(str(path), nomodify=False)
                assert with_ms.uvsub()
                assert with_ms.done()
            after, kept = state(path)
            np.testing.assert_equal(kept, recipes)
            for col in MAIN:
                if col in before:
                    np.testing.assert_array_equal(after[col], before[col])
            measured = before["CORRECTED_DATA"] - after["CORRECTED_DATA"]
            if expected is None:
                expected = measured
                assert np.max(np.abs(expected)) > 4.
            error = relative_rms(measured, expected)
            assert error < 1e-3, (writer, reader, error)
            reports[f"{writer}-{reader}"] = {"samples": measured.size, "nrms": error}
    return reports


def mixed_destinations(root, seed, point):
    for scratch in (False, True):
        after = []
        for label, factory in (("casa", reference), ("native", native)):
            tool = factory()
            paths = [root / f"mix-{scratch}-{label}-{field}.ms" for field in (0, 1)]
            prepare(seed, paths[0], model=True, virtual=scratch)
            prepare(seed, paths[1], model=True)
            before = [state(path) for path in paths]
            assert tool.selectvis(str(paths[0]), field=0, usescratch=not scratch)
            assert tool.selectvis(str(paths[1]), field=1, usescratch=scratch)
            assert tool.ft(complist=str(point))
            assert tool.done()
            snapshots = [state(path) for path in paths]
            for field, (original, predicted) in enumerate(zip(before, snapshots, strict=True)):
                if scratch:
                    assert np.count_nonzero(predicted[0]["MODEL_DATA"] != original[0]["MODEL_DATA"]) == 2880
                    np.testing.assert_equal(predicted[1], original[1])
                else:
                    np.testing.assert_array_equal(predicted[0]["MODEL_DATA"], original[0]["MODEL_DATA"])
                    assert f"definedmodel_field_{field}" in predicted[1][1]
            after.append(snapshots)
        for expected, actual in zip(*after, strict=True):
            compare_outputs(expected, actual)

    # CASA's iterator crashes when scratch is selected for a group containing
    # an earlier input without MODEL_DATA. Refuse before writing either input.
    first, last = (root / f"missing-model-{label}.ms" for label in ("first", "last"))
    prepare(seed, first, virtual=True)
    prepare(seed, last, model=True)
    tool = native()
    assert tool.selectvis(str(first), field=0)
    assert tool.selectvis(str(last), field=1, usescratch=True)
    before = [state(path) for path in (first, last)]
    assert not tool.ft(complist=str(point))
    assert tool.done()
    for path, original in zip((first, last), before, strict=True):
        compare_outputs(state(path), original)
    return 3


def failed_enrollments(root, seed, point):
    for label, factory in (("casa", reference), ("native", native)):
        first, other = (root / f"enrollment-{label}-{which}.ms" for which in ("first", "other"))
        prepare(seed, first, model=True)
        prepare(seed, other, model=True)
        tool = factory()
        assert tool.selectvis(str(first), field=0, usescratch=True)
        before = state(first)
        assert not tool.selectvis(str(root / "missing-enrollment.ms"), usescratch=True)
        assert not tool.ft(complist=str(point))
        compare_outputs(state(first), before)
        assert tool.close() and tool.selectvis(str(first), field=0, usescratch=True)
        reject(lambda: tool.selectvis(str(other), usescratch=True, datainmemory=True), "NullTable::getColumn")
        assert tool.ft(complist=str(point))
        assert tool.done()
        current = state(first)
        np.testing.assert_equal(current[0], before[0])
        assert "definedmodel_field_0" in current[1][1]
    return 2


def boundary_channel(root, seed, point):
    # CASA can crash on an oversized stride after a valid last channel.
    path = root / "final-channel.ms"
    prepare(seed, path, model=True)
    tool = native()
    assert tool.open(str(path), usescratch=True)
    assert tool.selectvis(field=0, spw=0, nchan=1, start=11, step=2)
    assert tool.ft(complist=str(point))
    assert tool.done()
    changed = np.any(state(path)[0]["MODEL_DATA"] != 13. + 7.j, axis=(0, 2))
    np.testing.assert_array_equal(np.flatnonzero(changed), [11])
    return int(np.count_nonzero(changed))


def batch_retry(root, seed):
    first, last, relocated = (root / f"batch-{suffix}.ms" for suffix in ("first", "last", "relocated"))
    for path in (first, last):
        prepare(seed, path, model=True)
    models = [str(root / f"taylor-native-term{term}.im") for term in (0, 1)]
    tool = native()
    assert tool.selectvis(str(first), field=0, usescratch=True)
    assert tool.selectvis(str(last), field=0, usescratch=True)
    assert tool.settaylorterms(2, 95e9)
    before = state(first)
    last.rename(relocated)
    try:
        reject(lambda: tool.ft(models, incremental=True), "batch-last.ms")
    finally:
        relocated.rename(last)
    completed = state(first)
    assert np.any(completed[0]["MODEL_DATA"] != before[0]["MODEL_DATA"])
    compare_outputs(state(last), before)
    reject(lambda: tool.settaylorterms(2, 105e9), "before changing Taylor terms")
    assert tool.ft(models, incremental=True)
    assert tool.done()
    compare_outputs(state(first), completed)
    compare_outputs(state(last), completed)
    return 2


def partial_scratch_write(root):
    path = root / "partial-scratch-write.ms"
    shutil.copytree(root / "image-predict-taylor-native.ms", path)
    # An unused fine-resolution window makes the bounded writer split even
    # this small MeasurementSet into batches.
    tool = table()
    for subtable in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION"):
        assert tool.open(str(path / subtable), nomodify=False)
        try:
            row = {name: tool.getcell(name, 0) for name in tool.colnames()
                   if tool.iscelldefined(name, 0)}
            count = tool.nrows()
            assert tool.addrows(1)
            if subtable == "SPECTRAL_WINDOW":
                row["NAME"] = "FINE"
                row["NUM_CHAN"] = 1_000_000
                for name in ("CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"):
                    row[name] = np.full(row["NUM_CHAN"], row[name][0], dtype=np.float64)
                row["TOTAL_BANDWIDTH"] = float(np.sum(row["CHAN_WIDTH"]))
            else:
                row["SPECTRAL_WINDOW_ID"] = 1
            for name, value in row.items():
                tool.putcell(name, count, value)
        finally:
            tool.close()

    assert tool.open(str(path), nomodify=False)
    try:
        descriptor = tool.getcoldesc("MODEL_DATA")
        tool.removecols("MODEL_DATA")
        descriptor.update(option=0, dataManagerType="StandardStMan", dataManagerGroup="FlexibleModel")
        descriptor.pop("shape", None)
        assert tool.addcols({"MODEL_DATA": descriptor})
        shape = tool.getcell("DATA", 0).shape
        blank = np.zeros(shape, dtype=np.complex64)
        rows = tool.nrows()
        for index in range(rows):
            tool.putcell("MODEL_DATA", index, blank)
        tool.putcell("MODEL_DATA", rows - 1, blank[:, :5].copy())
    finally:
        tool.close()

    model = str(root / "taylor-native-term0.im")
    im = native()
    assert im.open(str(path), usescratch=True)
    reject(lambda: im.ft(model, incremental=True), "MODEL_DATA")
    assert tool.open(str(path), nomodify=False)
    try:
        tool.putcell("MODEL_DATA", rows - 1, blank)
        before_retry = tool.getcol("MODEL_DATA")
    finally:
        tool.close()
    written = np.count_nonzero(np.any(np.abs(before_retry) > 1e-6, axis=(0, 1)))
    assert 0 < written < rows
    reject(lambda: im.ft(model, incremental=True), "previous scratch prediction may be partially written")
    np.testing.assert_array_equal(state(path)[0]["MODEL_DATA"], before_retry)
    assert im.close() and im.open(str(path), usescratch=True)
    assert im.ft(model) and im.done()
    recovered = state(path)[0]["MODEL_DATA"]
    np.testing.assert_allclose(recovered[..., 0], before_retry[..., 0], rtol=1e-6, atol=1e-6)
    assert np.max(np.abs(recovered[..., -1])) > 1.5
    return int(written)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    root = args.work.resolve()
    root.mkdir(parents=True)
    seed = root / "seed.ms"
    source_data(seed, stokes="XX XY YX YY")
    point = root / "point.cl"
    component(point, coordinates(seed), 5.)
    result = {
        "attachment_cases": attachment_cases(root, seed, point),
        "selected_samples": selected_prediction(root, seed, point),
        "image_predictions": image_predictions(root),
        "reopened_selection_samples": access_and_reopen(root, seed, point),
        "multi_input_samples": multi_input(root, seed, point),
        "virtual_interchange": virtual_interchange(root, seed, point),
        "mixed_destination_cases": mixed_destinations(root, seed, point),
        "failed_enrollment_cases": failed_enrollments(root, seed, point),
        "preserved_boundary_channels": boundary_channel(root, seed, point),
        "scratch_batch_retry_inputs": batch_retry(root, seed),
        "scratch_rows_protected_from_replay": partial_scratch_write(root),
    }
    (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
