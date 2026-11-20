#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy PB/W antenna pointing with a solved CASA 6.7.6.14 EP table."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import calibrater, imager, table
import numpy as np

from calibration_ft_oracle import component, coordinates
from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import (
    cache_contents, configured_imager, data, ft, make_input, read_virtual, recipe, reject_recipe,
)


def solve(dataset, model, cache, output):
    tool = calibrater()
    assert tool.setvi(old=True, quiet=True)
    assert tool.open(str(dataset))
    assert tool.setsolve(type='EP', table=str(output), cfcache=str(cache), painc=360.)
    assert tool.setmodel(str(model))
    assert tool.solve() and tool.done()
    reader = table()
    assert reader.open(str(output))
    assert reader.info()['subType'] == 'EP Jones'
    gains = reader.getcol('GAIN')
    assert gains.shape[:2] == (4, 1) and np.abs(gains).max() > .001
    assert np.unique(reader.getcol('TIME')).size == 1
    assert reader.done()


def scaled_table(source, output, scale, factory, receptors=slice(None)):
    shutil.copytree(source, output)
    copied = factory()
    assert copied.open(str(output), nomodify=False)
    gains = copied.getcol('GAIN')
    gains[receptors] *= scale
    assert copied.putcol('GAIN', gains)
    assert copied.done()
    assert (output / 'CAL_DESC' / 'table.dat').is_file()


def compare(actual, expected, label, errors):
    assert np.isfinite(actual).all()
    assert actual.shape == expected.shape
    error = relative_rms(actual, expected)
    print(label, 'NRMS', error, flush=True)
    assert error < 1.e-3, (label, error)
    errors[label] = error


def alternate_epoch(dataset):
    tool = table()
    assert tool.open(str(dataset), nomodify=False)
    times = tool.getcol('TIME')
    last = times == times.max()
    assert not last.all()
    for column in ('TIME', 'TIME_CENTROID'):
        epochs = tool.getcol(column)
        epochs[last] += 3600.
        assert tool.putcol(column, epochs)
    assert tool.done()
    return last


def pair(root, dataset, model, cache, offsets, machine, errors, label, *, rotate=False, spw=''):
    from casars.tools import imager as native

    root.mkdir()
    pinned, actual = (root / f'{label}.ms' for label in ('pinned', 'native'))
    for target in (pinned, actual):
        make_input(dataset, target, scratch=True)
        if spw:
            owner = imager()
            assert owner.open(str(target), usescratch=True) and owner.done()
    if rotate:
        late = alternate_epoch(pinned)
        np.testing.assert_array_equal(late, alternate_epoch(actual))
    if spw:
        prior = data(pinned, 'MODEL_DATA')
    kw = dict(spw=spw, scratch=True, machine=machine, pointing=offsets is not None)
    if offsets is not None:
        kw['ep_table'] = offsets
    ft(imager, pinned, model, cache, **kw)
    ft(native, actual, model, cache, **kw)
    expected = data(pinned, 'MODEL_DATA')
    observed = data(actual, 'MODEL_DATA')
    assert np.abs(expected).max() > 1.
    compare(observed, expected, label, errors)
    if spw:
        assert np.any(expected[:, 0] != prior[:, 0])
        np.testing.assert_array_equal(expected[:, 1:], prior[:, 1:])
        np.testing.assert_array_equal(observed[:, 1:], prior[:, 1:])
        for column in ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT'):
            before = data(dataset, column)
            np.testing.assert_array_equal(data(pinned, column), before)
            np.testing.assert_array_equal(data(actual, column), before)
    if rotate:
        compare(observed[..., late], expected[..., late], f'{label}-late', errors)
    return expected, observed


def saved(root, dataset, model, cache, offsets, machine, expected, errors):
    from casars.tools import imager as native
    from casatools import ms as casa_ms

    target = root / f'{machine}-saved.ms'
    make_input(dataset, target, scratch=False)
    before = cache_contents(cache)
    ft(native, target, model, cache, scratch=False, machine=machine,
       pointing=True, ep_table=offsets)
    assert cache_contents(cache) == before
    reader = table()
    assert reader.open(str(offsets))
    gains = reader.getcol('GAIN')
    antennas = reader.getcol('ANTENNA1')
    assert reader.done()
    packed = recipe(target)['casars_legacy_pbw_ep_offsets']
    np.testing.assert_array_equal(packed[0, antennas], np.ones(antennas.size))
    np.testing.assert_array_equal(packed[1:, antennas], gains[[0, 2], 0, :])
    moved_cache = cache.with_name('unavailable-cache')
    moved_offsets = offsets.with_suffix('.unavailable')
    cache.rename(moved_cache)
    offsets.rename(moved_offsets)
    try:
        name = 'PBMosaicFT' if machine == 'pbmosaic' else 'nPBWProjectFT'
        compare(read_virtual(target, name=name), expected, f'{machine}-saved-replay', errors)
        reject_recipe(casa_ms, target, 'Unsupported FTMachine')
    finally:
        moved_offsets.rename(offsets)
        moved_cache.rename(cache)


def refusals(root, dataset, model, cache, baseline):
    from casars.tools import imager as native

    for label, factory in (('casa', imager), ('native', native)):
        for option, path in (('empty', ''), ('missing', root / 'missing.EP'),
                             ('wrong-type', dataset)):
            target = root / f'{label}-{option}.ms'
            make_input(dataset, target, scratch=True)
            owner = imager()
            assert owner.open(str(target), usescratch=True) and owner.done()
            prior = data(target, 'MODEL_DATA')
            ft(factory, target, model, cache, scratch=True, spw='0:0', pointing=True,
               ep_table=path, accepts=False)
            np.testing.assert_array_equal(data(target, 'MODEL_DATA'), prior)
            shutil.rmtree(target)
        target = root / f'{label}-ignored-missing.ms'
        make_input(dataset, target, scratch=True)
        ft(factory, target, model, cache, scratch=True, pointing=False,
           ep_table=root / 'missing.EP')
        np.testing.assert_array_equal(data(target, 'MODEL_DATA'), baseline[label])
        shutil.rmtree(target)
    print('empty, missing and non-calibration tables preserve MODEL_DATA', flush=True)


def review_lifecycle(root, dataset, model, cache, offsets, expected, errors):
    from casars.tools import imager as native

    point = root / 'component.cl'
    component(point, coordinates(dataset), 2.)
    missing = root / 'missing.EP'
    incomplete = root / 'incomplete.EP'
    shutil.copytree(offsets, incomplete)
    editor = table()
    assert editor.open(str(incomplete), nomodify=False)
    antennas = editor.getcol('ANTENNA1')
    assert editor.removerows(np.flatnonzero(antennas == antennas.max())) and editor.done()
    for engine, factory in (('casa', imager), ('native', native)):
        for scratch in (True, False):
            target = root / f'{engine}-preserved-{scratch}.ms'
            make_input(dataset, target, scratch=scratch)
            ft(factory, target, model, cache, scratch=scratch)
            before = data(target, 'MODEL_DATA') if scratch else recipe(target)
            tool = configured_imager(factory, target, cache, scratch=scratch,
                                     pointing=True, ep_table=missing)
            assert not tool.ft(complist=str(point)) and tool.done()
            after = data(target, 'MODEL_DATA') if scratch else recipe(target)
            np.testing.assert_equal(after, before)
            if not scratch:
                ft(factory, target, model, cache, scratch=False, baseline='0&1',
                   pointing=True, ep_table=incomplete, accepts=False)
                np.testing.assert_equal(recipe(target), before)
            shutil.rmtree(target)

        target = root / f'{engine}-working.ms'
        make_input(dataset, target, scratch=True)
        tool = configured_imager(factory, target, cache, scratch=True,
                                 pointing=True, ep_table=offsets)
        assert tool.ft(complist=str(point))
        assert np.abs(data(target, 'MODEL_DATA')).max() > 1.
        assert tool.done()
        tool = configured_imager(factory, target, cache, scratch=True,
                                 pointing=True, ep_table=offsets)
        assert tool.ft(str(model))
        before = data(target, 'MODEL_DATA')
        moved = offsets.rename(root / 'temporarily-moved.EP')
        try:
            assert tool.setvp(dovp=False)
            assert tool.ft(str(model))
            np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
            assert tool.selectvis(field=0)
            assert not tool.ft(str(model))
            np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
        finally:
            moved.rename(offsets)
            assert tool.done()
        shutil.rmtree(target)

    linked = root / 'linked.EP'
    shutil.copytree(offsets, linked)
    assert editor.open(str(linked), nomodify=False)
    description = (linked / 'CAL_DESC').rename(root / 'separate-cal-description')
    assert editor.putkeyword('CAL_DESC', f'Table: {description}') and editor.done()
    observed, _ = pair(root / 'linked', dataset, model, cache, linked, 'pbwproject', errors,
                       'linked-description')
    np.testing.assert_array_equal(observed, expected)

    shuffled = root / 'shuffled.EP'
    assert editor.open(str(offsets))
    sorted_rows = editor.query('TRUE', sortlist='ANTENNA1 DESC')
    copied = sorted_rows.copy(str(shuffled), deep=True)
    assert copied.done() and sorted_rows.done() and editor.done()
    observed, _ = pair(root / 'shuffled', dataset, model, cache, shuffled, 'pbwproject', errors,
                       'shuffled-antenna-rows')
    np.testing.assert_array_equal(observed, expected)

    gapped = root / 'gapped.EP'
    shutil.copytree(offsets, gapped)
    assert editor.open(str(gapped), nomodify=False)
    assert editor.removerows(np.flatnonzero(antennas == 1)) and editor.done()
    target = root / 'gapped.ms'
    make_input(dataset, target, scratch=True)
    ft(native, target, model, cache, scratch=True)
    prior = data(target, 'MODEL_DATA')
    ft(native, target, model, cache, scratch=True, pointing=True, ep_table=gapped, accepts=False)
    np.testing.assert_equal(data(target, 'MODEL_DATA'), prior)
    print('component refusal, complete antenna coverage and live imager lifetime verified', flush=True)


def run(root, dataset, model, imported_cache):
    from casars.tools import table as native_table

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    errors = {}
    with TemporaryDirectory(dir=root) as work:
        work = Path(work)
        cache = work / 'cache'
        shutil.copytree(imported_cache, cache)
        training = work / 'solve.ms'
        make_input(dataset, training, scratch=True)
        offsets = work / 'solved.EP'
        solve(training, model, cache, offsets)
        shutil.rmtree(training)
        zero, reduced = work / 'zero.EP', work / 'reduced-native.EP'
        other_hand = work / 'other-hand-native.EP'
        scaled_table(offsets, zero, 0., table)
        scaled_table(offsets, reduced, .03, native_table)
        scaled_table(offsets, other_hand, 0., native_table, receptors=[1, 3])
        for label, values, rotate in (
            ('disabled', None, False), ('zero', zero, False),
            ('reduced', reduced, False), ('solved', offsets, False),
            ('rotated', offsets, True),
        ):
            with TemporaryDirectory(dir=work) as case:
                expected, observed = pair(Path(case) / label, dataset, model, cache, values,
                                          'pbwproject', errors, label, rotate=rotate)
                if label == 'disabled':
                    baseline, native_baseline = expected, observed
                elif label == 'zero':
                    np.testing.assert_array_equal(expected, baseline)
                    np.testing.assert_array_equal(observed, native_baseline)
                elif not rotate:
                    change = relative_rms(expected, baseline)
                    assert change > (.02 if label == 'reduced' else 1.), (label, change)
                    errors[f'{label}-change'] = change
                if label == 'solved':
                    saved(Path(case), dataset, model, cache, offsets, 'pbwproject', expected, errors)
                    solved, native_solved = expected, observed

        with TemporaryDirectory(dir=work) as case:
            case = Path(case)
            pair(case / 'channel', dataset, model, cache, offsets, 'pbwproject',
                 errors, 'selected-channel', spw='0:0')
            expected, observed = pair(case / 'other-hand', dataset, model, cache, other_hand,
                                      'pbwproject', errors, 'unused-receptor')
            np.testing.assert_array_equal(expected, solved)
            np.testing.assert_array_equal(observed, native_solved)
        with TemporaryDirectory(dir=work) as case:
            case = Path(case)
            expected, _ = pair(case / 'mosaic', dataset, model, cache, offsets, 'pbmosaic',
                               errors, 'pbmosaic-solved')
            saved(case, dataset, model, cache, offsets, 'pbmosaic', expected, errors)
        refusals(work, dataset, model, cache, {'casa': baseline, 'native': native_baseline})
        review_lifecycle(work, dataset, model, cache, offsets, solved, errors)
    (root / 'results.json').write_text(json.dumps(errors, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for parameter in ('work', 'dataset', 'model', 'cache'):
        parser.add_argument(f'--{parameter}', type=Path, required=True)
    args = parser.parse_args()
    run(*(path.resolve() for path in (args.work, args.dataset, args.model, args.cache)))
