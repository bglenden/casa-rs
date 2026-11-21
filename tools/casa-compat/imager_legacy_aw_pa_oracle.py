#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy A/W PA bins, cache extension and saved replay against CASA."""

import argparse
from pathlib import Path
import shutil

import casatasks
from casars.tools import imager, ms
from casatools import image, imager as casa_imager, table
import numpy as np

from imager_legacy_aw_oracle import compare_cells, predict
from imager_legacy_pbw_oracle import (
    cache_contents, corrected_or_data, data, make_input, recipe, relative_rms,
)


def timed_input(dataset, target, *, nearby):
    make_input(dataset, target, scratch=True)
    editor = table()
    assert editor.open(str(target), nomodify=False)
    for column in ('TIME', 'TIME_CENTROID'):
        values = editor.getcol(column)
        if nearby:
            epochs = np.unique(values)
            assert len(epochs) == 3
            values[values == epochs[1]] += 10.
            values[values == epochs[2]] += 6. * 3600.
        else:
            values[len(values) // 2:] += 6. * 3600.
        assert editor.putcol(column, values)
    assert editor.done()


def scratch(source, target, model, cache, *, factory=imager, machine='awproject', rotate=5.):
    make_input(source, target, scratch=True)
    predict(target, model, cache, machine, False, factory=factory, pastep=5.,
            rotpastep=rotate, wbawp=True)
    return data(target, 'MODEL_DATA')


def compare_scratch(actual, expected):
    np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
    return relative_rms(actual[:, 0, :], expected[:, 0, :])


def kernel_angles(cache):
    angles = []
    reader = image()
    for path in sorted(cache.glob('*CFS_*.im')):
        assert reader.open(str(path))
        angles.append(reader.miscinfo()['ParallacticAngle'])
        assert reader.done()
    return np.array(angles)


def cold_and_imported(root, source, model, reference):
    if reference is None:
        pinned, casa_cache = root / 'pinned.ms', root / 'casa-cache'
        expected = scratch(source, pinned, model, casa_cache, factory=casa_imager)
    else:
        pinned, casa_cache = reference / 'fixture.ms', reference / 'cache'
        np.testing.assert_array_equal(data(source, 'TIME'), data(pinned, 'TIME'))
        expected = data(pinned, 'MODEL_DATA')
    native_cache = root / 'native-cache'
    actual = scratch(source, root / 'native.ms', model, native_cache)
    angles = kernel_angles(casa_cache)
    assert len(angles) == 8 and np.ptp(angles) > 20.
    np.testing.assert_allclose(kernel_angles(native_cache), angles, rtol=0., atol=2.e-5)
    errors = {
        'independent-cells': compare_cells(native_cache, casa_cache, count=8),
        'scratch': compare_scratch(actual, expected),
    }
    for name, cache, factory in (('casa-reads-native', native_cache, casa_imager),
                                 ('native-reads-casa', casa_cache, imager)):
        before = cache_contents(cache)
        result = scratch(source, root / f'{name}.ms', model, cache,
                         factory=factory, machine='mawproject')
        assert cache_contents(cache) == before
        errors[name] = compare_scratch(result, expected)
    return errors, expected, casa_cache, native_cache


def extend_cache(root, source, model, full, expected):
    partial = root / 'partial-cache'
    partial.mkdir()
    for path in full.glob('*CFS_0_0_CF_*.im'):
        shutil.copytree(path, partial / path.name)
    before = cache_contents(partial)
    assert len(list(partial.glob('*CFS_*.im'))) == 4
    actual = scratch(source, root / 'extended-cache.ms', model, partial)
    after = cache_contents(partial)
    assert before and all(after.get(path) == digest for path, digest in before.items())
    return {
        'extended-cells': compare_cells(partial, full, count=8),
        'extended-scratch': compare_scratch(actual, expected),
    }


def rotate_and_replay(root, source, model, casa_cache, native_cache, reference):
    expected = {}
    native = {}
    errors = {}
    for name, step in (('small-rotation', .001), ('keep-base', 5.)):
        if reference is None:
            expected[name] = scratch(source, root / f'pinned-{name}.ms', model, casa_cache,
                                     factory=casa_imager, rotate=step)
        else:
            pinned = reference / f'{name}.ms'
            np.testing.assert_array_equal(data(source, 'TIME'), data(pinned, 'TIME'))
            expected[name] = data(pinned, 'MODEL_DATA')
        before = cache_contents(native_cache)
        native[name] = scratch(source, root / f'native-{name}.ms', model, native_cache,
                               rotate=step)
        assert cache_contents(native_cache) == before
        errors[name] = compare_scratch(native[name], expected[name])
    assert np.max(np.abs(expected['small-rotation'] - expected['keep-base'])) > 5.e-6
    assert np.max(np.abs(native['small-rotation'] - native['keep-base'])) > 5.e-6

    saved = root / 'saved.ms'
    make_input(source, saved, scratch=False)
    predict(saved, model, native_cache, 'awproject', False, scratch=False, pastep=5.,
            rotpastep=.001, wbawp=True)
    entry = recipe(saved)
    assert entry['casars_legacy_aw_compute_pa_step'] == 5.
    assert entry['casars_legacy_aw_rotate_pa_step'] == .001
    retained = Path(entry['diskimage']) / 'CASARS_LEGACY_AW_CACHE'
    errors['saved-cells'] = compare_cells(retained, casa_cache, count=8)
    native_cache.rename(root / 'moved-cache')
    prior = corrected_or_data(saved)
    reader = ms()
    assert reader.open(str(saved), nomodify=False)
    assert reader.msselect({'field': '0'}) and reader.uvsub() and reader.done()
    actual = prior - data(saved, 'CORRECTED_DATA')
    assert not np.any(actual[:, 1:, :])
    errors['saved-replay'] = relative_rms(actual[:, 0, :], expected['small-rotation'][:, 0, :])
    return errors


def timed_edge_input(seed, target, offsets):
    make_input(seed, target, scratch=True)
    editor = table()
    assert editor.open(str(target), nomodify=False)
    for column in ('TIME', 'TIME_CENTROID'):
        values = editor.getcol(column)
        epochs, indices = np.unique(values, return_inverse=True)
        assert len(epochs) == 3
        assert editor.putcol(column, epochs[0] + np.asarray(offsets)[indices])
    assert editor.done()


def selection_edge(root, seed, model, label, reference):
    work = root / label
    work.mkdir()
    offsets, step = (([0., 12600., 21630.], 20.) if label == 'overlap'
                     else ([0., 2000., 2000.], .5))
    terms = dict(pastep=step, rotpastep=180., wbawp=True)
    source = work / 'source.ms'
    timed_edge_input(seed, source, offsets)
    if reference is None:
        cold, warm = work / 'pinned-cold.ms', work / 'pinned-warm.ms'
        cold_cache, warm_cache = work / 'pinned-cold-cache', work / 'pinned-warm-cache'
        make_input(source, cold, scratch=True)
        predict(cold, model, cold_cache, 'awproject', False, factory=casa_imager, **terms)
        make_input(source, warm, scratch=True)
        shutil.copytree(cold_cache, warm_cache)
        predict(warm, model, warm_cache, 'mawproject', False, factory=casa_imager, **terms)
    else:
        cold, warm = reference / 'cold.ms', reference / 'warm.ms'
        cold_cache = reference / 'cold-cache'
        for pinned in (cold, warm):
            np.testing.assert_array_equal(data(source, 'TIME'), data(pinned, 'TIME'))
    angles = kernel_angles(cold_cache)
    assert len(angles) == 8
    first, second = angles[0], angles[2]
    separation = abs(first - second)
    assert separation > step
    if label == 'subdegree':
        assert separation < 1.

    saved, native_cache = work / 'saved.ms', work / 'native-cache'
    make_input(source, saved, scratch=False)
    predict(saved, model, native_cache, 'awproject', False, scratch=False, **terms)
    entry = recipe(saved)
    chosen = entry['casars_legacy_aw_live_pa_bases']
    expected_bases = [first, first, second] if label == 'overlap' else [first, second]
    np.testing.assert_allclose(chosen, expected_bases, rtol=0., atol=2.e-5)
    held = Path(entry['diskimage']) / 'CASARS_LEGACY_AW_CACHE'
    errors = {f'{label}-cells': compare_cells(held, cold_cache, count=8)}
    moved = work / 'moved-cache'
    native_cache.rename(moved)
    prior = corrected_or_data(saved)
    reader = ms()
    assert reader.open(str(saved), nomodify=False)
    assert reader.msselect({'field': '0'}) and reader.uvsub() and reader.done()
    subtracted = prior - data(saved, 'CORRECTED_DATA')
    assert not np.any(subtracted[:, 1:, :])
    expected = data(cold, 'MODEL_DATA')[:, 0, :]
    errors[f'{label}-cold-replay'] = relative_rms(subtracted[:, 0, :], expected)

    before = cache_contents(moved)
    target = work / 'native-warm.ms'
    make_input(source, target, scratch=True)
    predict(target, model, moved, 'mawproject', False, **terms)
    assert cache_contents(moved) == before
    errors[f'{label}-warm'] = compare_scratch(data(target, 'MODEL_DATA'), data(warm, 'MODEL_DATA'))
    return errors


def incompatible_append(root, seed, source, model, full):
    cache = root / 'different-sampling'
    cache.mkdir()
    editor = image()
    for path in full.glob('*CFS_0_0_CF_*.im'):
        target = cache / path.name
        shutil.copytree(path, target)
        assert editor.open(str(target))
        metadata = editor.miscinfo()
        metadata['Sampling'] = 19.
        assert editor.setmiscinfo(metadata) and editor.done()
    before = cache_contents(cache)
    assert len(list(cache.glob('*CFS_*.im'))) == 4
    scratch(seed, root / 'compatible-existing-angle.ms', model, cache)
    invalid = root / 'incompatible-new-angle.ms'
    make_input(source, invalid, scratch=True)
    predict(invalid, model, cache, 'awproject', False, accepts=False, pastep=5., wbawp=True)
    assert cache_contents(cache) == before
    scratch(seed, root / 'same-angle-after-refusal.ms', model, cache)
    assert cache_contents(cache) == before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{option}', type=Path, required=True)
    parser.add_argument('--cold-reference', type=Path)
    parser.add_argument('--rotation-reference', type=Path)
    parser.add_argument('--overlap-reference', type=Path)
    parser.add_argument('--subdegree-reference', type=Path)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir(parents=True)
    source, nearby = args.work / 'two-angles.ms', args.work / 'rotate-angles.ms'
    timed_input(args.dataset, source, nearby=False)
    timed_input(args.dataset, nearby, nearby=True)
    errors, expected, casa_cache, native_cache = cold_and_imported(
        args.work, source, args.model, args.cold_reference,
    )
    errors.update(extend_cache(args.work, source, args.model, casa_cache, expected))
    errors.update(rotate_and_replay(args.work, nearby, args.model, casa_cache, native_cache,
                                    args.rotation_reference))
    incompatible_append(args.work, args.dataset, source, args.model, casa_cache)
    for label, reference in (('overlap', args.overlap_reference), ('subdegree', args.subdegree_reference)):
        errors.update(selection_edge(args.work, args.dataset, args.model, label, reference))
    print('legacy A/W parallactic-angle NRMS', errors, flush=True)
    assert all(np.isfinite(error) and error < 1.e-6 for error in errors.values()), errors


if __name__ == '__main__':
    main()
