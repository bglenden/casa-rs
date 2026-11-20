#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare two selected fields through the public legacy PBMosaicFT route."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import image, imager, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import cache_contents, corrected_or_data, data, ft, make_input


def seed_fields(seed, source):
    make_input(seed, source, scratch=True)
    writer = table()
    assert writer.open(str(source / 'FIELD'), nomodify=False)
    assert writer.nrows() >= 3
    for column in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
        direction = writer.getcell(column, 0).copy()
        direction[:, 0] += np.radians(np.array([120., -75.]) / 3600.)
        assert writer.putcell(column, 1, direction)
    assert writer.done()
    tool = imager()
    assert tool.open(str(source), usescratch=True) and tool.done()
    assert writer.open(str(source), nomodify=False)
    times = writer.getcol('TIME')
    epochs = np.unique(times)
    assert len(epochs) == 3
    fields = np.searchsorted(epochs, times).astype('int32')
    assert writer.putcol('FIELD_ID', fields)
    existing = writer.getcol('MODEL_DATA')
    existing.fill(13. + 7.j)
    assert writer.putcol('MODEL_DATA', existing) and writer.done()
    return fields


def saved_model(dataset):
    from casars.tools import ms

    reader = table()
    assert reader.open(str(dataset / 'SOURCE'))
    keys = {key for key in reader.keywordnames() if key.startswith('definedmodel_field_')}
    assert keys == {'definedmodel_field_0', 'definedmodel_field_1'}, keys
    tool = image()
    for field in (0, 1):
        key = reader.getkeyword(f'definedmodel_field_{field}')
        record = reader.getcell('SOURCE_MODEL', reader.getkeyword(key))['ft_0']['container']
        assert record['name'] == 'PBMosaicFT'
        grid = Path(record['diskimage'])
        for path in (grid, grid / 'CASARS_LEGACY_PBW_KERNEL'):
            assert tool.open(str(path))
            assert np.iscomplexobj(tool.getchunk()) and tool.done()
    assert reader.done()
    prior = corrected_or_data(dataset)
    subtract = ms()
    assert subtract.open(str(dataset), nomodify=False)
    assert subtract.msselect({'field': '0,1'})
    assert subtract.uvsub() and subtract.done()
    return prior - data(dataset, 'CORRECTED_DATA')


def run(root, dataset, model, cache, ep_table):
    from casars.tools import imager as native

    source = root / 'seed.ms'
    fields = seed_fields(dataset, source)
    results = {}
    default = None
    for pointing in (False, True):
        label = 'ep' if pointing else 'default'
        active_cache = root / f'{label}-cache'
        shutil.copytree(cache, active_cache)
        initial_cache = cache_contents(active_cache)
        external = root / f'{label}.EP'
        if pointing:
            shutil.copytree(ep_table, external)
        options = {'machine': 'pbmosaic', 'pointing': pointing, 'ep_table': external if pointing else ''}
        references = {}
        for field in ('0,1', '0', '1'):
            target = root / f'{label}-casa-{field}.ms'
            make_input(source, target, scratch=True)
            ft(imager, target, model, active_cache, field=field, scratch=True, **options)
            references[field] = data(target, 'MODEL_DATA')
        expected = references['0,1']
        assert np.abs(expected[0, 0, fields == 1].imag).max() > 1.
        if pointing:
            assert relative_rms(expected[..., fields == 1], default[..., fields == 1]) > .1
        else:
            default = expected
        for field in (0, 1):
            where = fields == field
            np.testing.assert_array_equal(expected[..., where], references[str(field)][..., where])
            assert np.max(np.abs(expected[..., where])) > 1.
        np.testing.assert_array_equal(expected[..., fields == 2], 13. + 7.j)
        current, saved = (root / f'{label}-native-{kind}.ms' for kind in ('scratch', 'saved'))
        for target, scratch in ((current, True), (saved, False)):
            make_input(source, target, scratch=scratch)
            ft(native, target, model, active_cache, field='0,1', scratch=scratch, **options)
        pbw_errors = []
        for name, factory in (('casa', imager), ('native', native)):
            target = root / f'{label}-{name}-pbw.ms'
            make_input(source, target, scratch=True)
            ft(factory, target, model, active_cache, field='0,1', scratch=True,
               **(options | {'machine': 'pbwproject'}))
            pbw = data(target, 'MODEL_DATA')
            if name == 'casa':
                np.testing.assert_array_equal(pbw, expected)
            else:
                pbw_errors = [relative_rms(pbw[..., fields == i], expected[..., fields == i])
                              for i in (0, 1)]
        assert cache_contents(active_cache) == initial_cache
        if pointing:
            external.rename(root / 'moved.EP')
        actual = data(current, 'MODEL_DATA')
        restored = saved_model(saved)
        np.testing.assert_array_equal(actual[..., fields == 2], 13. + 7.j)
        np.testing.assert_array_equal(restored[..., fields == 2], 0.)
        for column in ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA'):
            original = data(source, column)
            for target in (current, saved):
                np.testing.assert_array_equal(data(target, column), original)
        errors = {name: [relative_rms(values[..., fields == i], expected[..., fields == i])
                         for i in (0, 1)] for name, values in [('scratch', actual), ('saved', restored)]}
        errors['pbwproject'] = pbw_errors
        print(label, errors, flush=True)
        assert max(value for pair in errors.values() for value in pair) < 1.e-3
        results[label] = errors
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--ep-table', type=Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=args.work) as scratch:
        evidence = run(Path(scratch), args.dataset, args.model, args.cache, args.ep_table)
    (args.work / 'results.json').write_text(json.dumps(evidence, indent=2) + '\n')
