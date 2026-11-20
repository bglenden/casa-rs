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
    }
    (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
