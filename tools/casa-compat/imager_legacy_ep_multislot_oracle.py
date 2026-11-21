#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare nearest-time legacy antenna offsets using CASA and python-casacore."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table as casacore_table
from casatools import calibrater, imager, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import (
    cache_contents, configured_imager, data, ft, make_input, read_virtual, recipe, reject_recipe,
)


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


def saved_offsets(dataset, offsets, epochs, description=0):
    record = recipe(dataset)
    times = record[EPOCHS]
    packed = record[OFFSETS]
    # casatools promotes Float arrays inside a record to Double. The other
    # C++ binding exposes the stored types and reverses NumPy's axis order.
    with casacore_table(str(dataset / 'SOURCE'), ack=False) as reader:
        row = reader.getkeyword(reader.getkeyword('definedmodel_field_0'))
        typed = reader.getcell('SOURCE_MODEL', row)['ft_0']['container']
    assert typed[EPOCHS].dtype == np.float64 and typed[OFFSETS].dtype == np.float32
    np.testing.assert_array_equal(typed[EPOCHS], times)
    np.testing.assert_array_equal(typed[OFFSETS].transpose(2, 1, 0), packed)
    np.testing.assert_array_equal(times, epochs)
    assert packed.shape == (3, 28, 3)
    order = np.lexsort((data(offsets, 'ANTENNA1'), data(offsets, 'TIME')))
    order = order[data(offsets, 'CAL_DESC_ID')[order] == description]
    gain = data(offsets, 'GAIN')[:, 0, order].astype(np.float32)
    np.testing.assert_array_equal(packed[0], np.ones((28, 3), dtype=np.float32))
    np.testing.assert_array_equal(packed[1], gain[0].reshape((3, 28)).T)
    np.testing.assert_array_equal(packed[2], gain[2].reshape((3, 28)).T)


def copy_row(table_path):
    with casacore_table(str(table_path), readonly=False, ack=False) as target:
        assert target.nrows() == 1
        target.addrows(1)
        for name in target.colnames():
            if target.iscelldefined(name, 0):
                target.putcell(name, 1, target.getcell(name, 0))


def multiple_windows(root, original, model, cache, offsets, epochs, native, results):
    seed = root / 'two-windows.ms'
    make_input(original, seed, scratch=True)
    copy_row(seed / 'SPECTRAL_WINDOW')
    copy_row(seed / 'DATA_DESCRIPTION')
    with casacore_table(str(seed / 'DATA_DESCRIPTION'), readonly=False, ack=False) as target:
        target.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with casacore_table(str(seed), readonly=False, ack=False) as target:
        time = target.getcol('TIME')
        last_windows = time != time.min()
        assert last_windows.any() and not last_windows.all()
        target.putcol('DATA_DESC_ID', last_windows.astype(np.int32))

    # Put the zero-window solutions in description row 1 with different
    # offsets. Both visibility windows use these offsets in pinned CASA.
    reordered = root / 'reordered.EP'
    shutil.copytree(offsets, reordered)
    copy_row(reordered / 'CAL_DESC')
    with casacore_table(str(reordered / 'CAL_DESC'), readonly=False, ack=False) as target:
        target.putcell('SPECTRAL_WINDOW_ID', 0, np.array([1], dtype=np.int32))
        target.putcell('SPECTRAL_WINDOW_ID', 1, np.array([0], dtype=np.int32))
    with (casacore_table(str(offsets), ack=False) as source,
          casacore_table(str(reordered), readonly=False, ack=False) as target):
        rows = target.nrows()
        source.copyrows(target)
        assert target.nrows() == 2 * rows
        for row in range(rows, target.nrows()):
            target.putcell('CAL_DESC_ID', row, 1)
            target.putcell('GAIN', row, target.getcell('GAIN', row) * 2.)

    owner = imager()
    assert owner.open(str(seed), usescratch=True) and owner.done()
    original_values = data(seed, 'MODEL_DATA')
    preserved = {column: data(seed, column) for column in ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT')}
    prior_cache = cache_contents(cache)
    missing_zero = root / 'no-window-zero.EP'
    shutil.copytree(offsets, missing_zero)
    with casacore_table(str(missing_zero / 'CAL_DESC'), readonly=False, ack=False) as target:
        target.putcell('SPECTRAL_WINDOW_ID', 0, np.array([1], dtype=np.int32))
    for machine, record_name in (('pbwproject', 'nPBWProjectFT'), ('pbmosaic', 'PBMosaicFT')):
        with TemporaryDirectory(dir=root) as attempt:
            case = Path(attempt)
            def predict_windows(label, factory, path, spw, *, scratch=True):
                target = case / f'{label}.ms'
                make_input(seed, target, scratch=scratch)
                ft(factory, target, model, cache, scratch=scratch, machine=machine,
                   pointing=True, ep_table=path, spw=spw)
                return target

            single = data(predict_windows('casa-single-desc', imager, offsets, '0,1'), 'MODEL_DATA')
            native_single = predict_windows('native-single-desc', native, offsets, '0,1')
            check(f'{machine}-two-windows-one-description', data(native_single, 'MODEL_DATA'),
                  single, results)
            retry = case / 'retry.ms'
            make_input(seed, retry, scratch=True)
            tool = configured_imager(native, retry, cache, scratch=True, machine=machine,
                                     pointing=True, ep_table=missing_zero, spw='0,1')
            assert not tool.ft(str(model))
            np.testing.assert_array_equal(data(retry, 'MODEL_DATA'), original_values)
            assert tool.setoptions(ftmachine=machine, cfcachedirname=str(cache),
                                   applypointingoffsets=True, epjtablename=str(offsets),
                                   wprojplanes=1, numthreads=1)
            assert tool.ft(str(model)) and tool.done()
            check(f'{machine}-retry', data(retry, 'MODEL_DATA'), single, results)
            combined = data(predict_windows('casa-reordered', imager, reordered, '0,1'), 'MODEL_DATA')
            actual = predict_windows('native-reordered', native, reordered, '0,1')
            for window, selected in enumerate((~last_windows, last_windows)):
                split = data(predict_windows(f'casa-window-{window}', imager, reordered, str(window)),
                             'MODEL_DATA')
                np.testing.assert_array_equal(combined[..., selected], split[..., selected])
                np.testing.assert_array_equal(split[..., ~selected], original_values[..., ~selected])
                assert relative_rms(single[..., selected], combined[..., selected]) > .01
                check(f'{machine}-reordered-window-{window}', data(actual, 'MODEL_DATA')[..., selected],
                      split[..., selected], results)
            only_second = predict_windows('native-window-1', native, reordered, '1')
            np.testing.assert_array_equal(data(only_second, 'MODEL_DATA')[..., ~last_windows],
                                          original_values[..., ~last_windows])
            check(f'{machine}-native-second-only', data(only_second, 'MODEL_DATA')[..., last_windows],
                  combined[..., last_windows], results)
            for column, expected in preserved.items():
                np.testing.assert_array_equal(data(actual, column), expected)
            saved = predict_windows('native-saved', native, reordered, '0,1', scratch=False)
            saved_offsets(saved, reordered, epochs, description=1)
            hidden = reordered.rename(case / 'unavailable.EP')
            try:
                check(f'{machine}-multi-window-saved', read_virtual(saved, name=record_name),
                      combined, results)
            finally:
                hidden.rename(reordered)
    assert cache_contents(cache) == prior_cache


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
            with casacore_table(str(target / 'SOURCE'), readonly=False, ack=False) as writer:
                row = writer.getkeyword(writer.getkeyword('definedmodel_field_0'))
                source = writer.getcell('SOURCE_MODEL', row)
                # Keep the recipe header's Int32 types across the Python round trip.
                for key in ('version', 'numft', 'numcl'):
                    if key in source:
                        source[key] = np.int32(source[key])
                source['ft_0']['version'] = np.int32(source['ft_0']['version'])
                corrupted = source['ft_0']['container']
                if label == 'no-offsets':
                    del corrupted[OFFSETS]
                elif label == 'legacy-offsets-with-times':
                    corrupted[OFFSETS] = corrupted[OFFSETS][0].copy()
                elif label == 'no-times':
                    del corrupted[EPOCHS]
                elif label == 'reversed-times':
                    corrupted[EPOCHS] = corrupted[EPOCHS][::-1].copy()
                else:
                    corrupted[OFFSETS][-1, 0, 0] = 0.
                writer.putcell('SOURCE_MODEL', row, source)
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
                print(machine, 'five malformed saved records refused', flush=True)
                moved = offsets.rename(work / 'unavailable.EP')
                try:
                    name = 'PBMosaicFT' if machine == 'pbmosaic' else 'nPBWProjectFT'
                    check(f'{machine}-saved', read_virtual(saved, name=name), expected, results)
                finally:
                    moved.rename(offsets)
        nearest_controls(work, original, model, cache, offsets, epochs, native, results)
        multiple_windows(work, original, model, cache, offsets, epochs, native, results)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model', 'cache'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(*(path.resolve() for path in (args.work, args.dataset, args.model, args.cache)))
