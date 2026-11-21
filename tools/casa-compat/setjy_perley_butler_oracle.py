#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the table-backed setjy standards using the same CASA data tables."""

import argparse
from pathlib import Path

from casatools import table
import numpy as np

from imager_legacy_pbw_oracle import data, make_input
from record_casa_returns import make_cal_ms
from setjy_manual_oracle import casa_setjy, compare, compare_returns, rust_setjy, verify_case


def rename(seed, target, name, *, shift=False):
    make_input(seed, target, scratch=False)
    writer = table()
    assert writer.open(str(target / 'FIELD'), nomodify=False)
    assert writer.putcell('NAME', 0, name)
    if shift:
        for column in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
            direction = writer.getcell(column, 0)
            direction[0] += .12
            assert writer.putcell(column, 0, direction)
    assert writer.done()


def lower_frequency(dataset):
    writer = table()
    assert writer.open(str(dataset / 'SPECTRAL_WINDOW'), nomodify=False)
    frequencies = writer.getcell('CHAN_FREQ', 0)
    shifted = frequencies - frequencies[0] + 150.e6
    assert writer.putcell('CHAN_FREQ', 0, shifted)
    assert writer.putcell('REF_FREQUENCY', 0, shifted[0])
    assert writer.done()


def no_new_model(seed, root):
    """No catalog match leaves old scratch data but removes virtual models."""
    options = dict(field='0', spw='0', standard='manual', fluxdensity=2.)
    results = {}
    for engine, task in (('casa', casa_setjy), ('rust', rust_setjy)):
        for scratch in (True, False):
            target = root / f'{engine}-unrecognized-{scratch}.ms'
            rename(seed, target, 'OUTSIDE_CATALOG', shift=True)
            assert task(vis=str(target), usescratch=scratch, **options)
            before = data(target, 'MODEL_DATA') if scratch else None
            results[engine, scratch] = task(vis=str(target), field='0', spw='0', usescratch=scratch)
            assert results[engine, scratch]['0']['0']['fluxd'][0] == 1.
            if scratch:
                np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
            else:
                reader = table()
                assert reader.open(str(target / 'SOURCE'))
                assert 'definedmodel_field_0' not in reader.keywordnames()
                assert reader.done()
        assert task(vis=str(target), field='0', spw='0', usescratch=False, interpolation='') == {}
    for scratch in (True, False):
        compare_returns(results['rust', scratch], results['casa', scratch])


def mixed_scratch_fields(root):
    seed = root / 'mixed-fields.ms'
    make_cal_ms(str(seed), frequency='1.4GHz')
    unknown = data(seed, 'FIELD_ID') == 1
    fluxes, models = [], []
    for engine, task in (('casa', casa_setjy), ('rust', rust_setjy)):
        target = root / f'mixed-fields-{engine}.ms'
        rename(seed, target, '3C286')
        args = dict(vis=str(target), field='0,1', spw='0', usescratch=True)
        assert task(**args, standard='manual', fluxdensity=2.)
        before = data(target, 'MODEL_DATA')
        fluxes.append(task(**args))
        after = data(target, 'MODEL_DATA')
        np.testing.assert_array_equal(after[..., unknown], before[..., unknown])
        assert not np.array_equal(after[..., ~unknown], before[..., ~unknown])
        models.append(after)
    compare_returns(fluxes[1], fluxes[0])
    compare('mixed recognized and unrecognized scratch fields', models[1], models[0])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('vla_ms', type=Path, help='VLA fixture with phase center near 3C286')
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    verify_case(args.vla_ms, args.root, 'pipeline-alias', {}, standard=None)
    cone = args.root / 'cone-fixture.ms'
    rename(args.vla_ms, cone, 'POSITIONAL_MATCH')
    verify_case(cone, args.root, 'catalog-cone', {'scalebychan': False}, standard=None)
    variable = args.root / 'variable-fixture.ms'
    rename(args.vla_ms, variable, '3c48', shift=True)
    for interpolation in ('nearest', 'linear', 'cubic', 'spline'):
        verify_case(variable, args.root, 'variable-' + interpolation, {
            'interpolation': interpolation, 'fluxdensity': 1.,
        }, standard='Perley-Butler 2017', return_rtol=3e-7)
    verify_case(variable, args.root, 'variable-2013', {
        'interpolation': 'linear',
    }, standard='Perley-Butler 2013', return_rtol=3e-7)
    low = args.root / 'low-frequency.ms'
    rename(args.vla_ms, low, '3C286')
    lower_frequency(low)
    verify_case(low, args.root, 'scaife-heald', {
        'interpolation': '',
    }, standard='Scaife-Heald 2012')
    no_new_model(args.vla_ms, args.root)
    mixed_scratch_fields(args.root)


if __name__ == '__main__':
    main()
