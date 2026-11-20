#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare nearest-time legacy antenna offsets from a real multi-slot EP solve."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import calibrater, imager, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import data, ft, make_input, read_virtual, recipe, reject_recipe


OFFSETS = 'casars_legacy_pbw_ep_offsets'
EPOCHS = 'casars_legacy_pbw_ep_epochs'


def solve(dataset, model, cache, offsets):
    tool = calibrater()
    assert tool.setvi(old=True, quiet=True) and tool.open(str(dataset))
    assert tool.setsolve(type='EP', t='1s', table=str(offsets),
                         cfcache=str(cache), painc=360.)
    assert tool.setmodel(str(model))
    assert tool.solve() and tool.done()
    epochs, counts = np.unique(data(offsets, 'TIME'), return_counts=True)
    assert epochs.size == 3 and np.all(counts == 28)
    return epochs


def check(label, actual, expected, results):
    error = relative_rms(actual, expected)
    assert error < 1.e-3, (label, error)
    results[label] = error
    print(label, 'NRMS', error, flush=True)


def predict(root, label, factory, original, model, cache, offsets, machine):
    output = root / f'{label}.ms'
    make_input(original, output, scratch=True)
    ft(factory, output, model, cache, scratch=True, machine=machine,
       pointing=True, ep_table=offsets)
    return data(output, 'MODEL_DATA')


def flagged_buffer(root, original, model, cache, offsets, machine, native, reference, results):
    flagged = root / 'flagged.ms'
    make_input(original, flagged, scratch=True)
    writer = table()
    assert writer.open(str(flagged), nomodify=False)
    _, groups = np.unique(writer.getcol('TIME'), return_inverse=True)
    middle = groups == 1
    assert writer.putcol('FLAG_ROW', writer.getcol('FLAG_ROW') | middle) and writer.done()
    pinned = predict(root, 'flagged-casa', imager, flagged, model, cache, offsets, machine)
    actual = predict(root, 'flagged-native', native, flagged, model, cache, offsets, machine)
    assert np.any(reference[..., middle])
    assert not np.any(pinned[..., middle]) and not np.any(actual[..., middle])
    np.testing.assert_array_equal(pinned[..., ~middle], reference[..., ~middle])
    check(f'{machine}-flagged-buffer', actual, pinned, results)


def nearest_controls(root, original, model, cache, offsets, epochs, native, results):
    # An integral time array makes the midpoint tie exactly representable.
    rounded = root / 'rounded.EP'
    shutil.copytree(offsets, rounded)
    writer = table()
    assert writer.open(str(rounded), nomodify=False)
    solution_time = np.rint(writer.getcol('TIME'))
    assert writer.putcol('TIME', solution_time) and writer.done()
    first, middle, last = np.rint(epochs)
    target_time = np.array([first - 5., (first + middle) / 2., last + 5.])
    altered = root / 'shifted-input.ms'
    make_input(original, altered, scratch=True)
    assert writer.open(str(altered), nomodify=False)
    _, slots = np.unique(writer.getcol('TIME'), return_inverse=True)
    for column in ('TIME', 'TIME_CENTROID'):
        assert writer.putcol(column, target_time[slots])
    assert writer.done()
    expected = predict(root, 'shifted-casa', imager, altered, model, cache, rounded,
                       'pbwproject')
    actual = predict(root, 'shifted-native', native, altered, model, cache, rounded,
                     'pbwproject')
    compare = ('below', 'tie', 'above')
    for index, solution in enumerate((first, middle, last)):
        constant = root / f'constant-slot-{index}.EP'
        shutil.copytree(rounded, constant)
        assert writer.open(str(constant), nomodify=False)
        assert writer.removerows(np.flatnonzero(solution_time != solution)) and writer.done()
        control = predict(root, f'constant-{index}', imager, altered, model, cache,
                          constant, 'pbwproject')
        for group, reference in enumerate((first, first, last)):
            kept = slots == group
            if reference == solution:
                np.testing.assert_array_equal(expected[..., kept], control[..., kept])
                check(compare[group], actual[..., kept], expected[..., kept], results)
            else:
                assert relative_rms(control[..., kept], expected[..., kept]) > .01


def saved_offsets(dataset, offsets, epochs):
    record = recipe(dataset)
    times = record[EPOCHS]
    packed = record[OFFSETS]
    assert times.dtype == np.float64 and packed.dtype == np.float32
    np.testing.assert_array_equal(times, epochs)
    assert packed.shape == (3, 28, 3)
    order = np.lexsort((data(offsets, 'ANTENNA1'), data(offsets, 'TIME')))
    gain = data(offsets, 'GAIN')[:, 0, order].astype(np.float32)
    np.testing.assert_array_equal(packed[0], np.ones((28, 3), dtype=np.float32))
    np.testing.assert_array_equal(packed[1], gain[0].reshape((3, 28)).T)
    np.testing.assert_array_equal(packed[2], gain[2].reshape((3, 28)).T)


def bad_records(root, dataset, native_ms):
    for label, message in (
        ('no-offsets', 'solution times have no pointing offsets'),
        ('legacy-offsets-with-times', 'single-slot saved legacy EP offsets have solution times'),
        ('no-times', 'saved legacy EP offsets have no solution times'),
        ('reversed-times', 'saved legacy EP solution times are invalid'),
        ('missing-antenna', 'saved legacy EP offsets contain an invalid antenna entry'),
    ):
        with TemporaryDirectory(dir=root) as attempt:
            target = Path(attempt) / f'{label}.ms'
            shutil.copytree(dataset, target)
            writer = table()
            assert writer.open(str(target / 'SOURCE'), nomodify=False)
            row = writer.getkeyword(writer.getkeyword('definedmodel_field_0'))
            source = writer.getcell('SOURCE_MODEL', row)
            corrupted = source['ft_0']['container']
            if label == 'no-offsets':
                del corrupted[OFFSETS]
            elif label == 'legacy-offsets-with-times':
                corrupted[OFFSETS] = corrupted[OFFSETS][:, :, 0].copy()
            elif label == 'no-times':
                del corrupted[EPOCHS]
            elif label == 'reversed-times':
                corrupted[EPOCHS] = corrupted[EPOCHS][::-1].copy()
            else:
                corrupted[OFFSETS][0, 0, -1] = 0.
            assert writer.putcell('SOURCE_MODEL', row, source) and writer.done()
            reject_recipe(native_ms, target, message)


def run(root, original, model, cached):
    from casars.tools import imager as native, ms as native_ms

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        cache = work / 'cache'
        training = work / 'training.ms'
        offsets = work / 'multi.EP'
        shutil.copytree(cached, cache)
        make_input(original, training, scratch=True)
        epochs = solve(training, model, cache, offsets)
        shutil.rmtree(training)
        for machine in ('pbwproject', 'pbmosaic'):
            with TemporaryDirectory(dir=work) as attempt:
                case = Path(attempt)
                expected = predict(case, 'casa', imager, original, model, cache, offsets, machine)
                actual = predict(case, 'native', native, original, model, cache, offsets, machine)
                check(machine, actual, expected, results)
                flagged_buffer(case, original, model, cache, offsets, machine, native, expected,
                               results)
                saved = case / 'saved.ms'
                make_input(original, saved, scratch=False)
                ft(native, saved, model, cache, scratch=False, machine=machine,
                   pointing=True, ep_table=offsets)
                saved_offsets(saved, offsets, epochs)
                bad_records(case, saved, native_ms)
                moved = offsets.rename(work / 'unavailable.EP')
                try:
                    name = 'PBMosaicFT' if machine == 'pbmosaic' else 'nPBWProjectFT'
                    check(f'{machine}-saved', read_virtual(saved, name=name), expected, results)
                finally:
                    moved.rename(offsets)
        nearest_controls(work, original, model, cache, offsets, epochs, native, results)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model', 'cache'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(*(path.resolve() for path in (args.work, args.dataset, args.model, args.cache)))
