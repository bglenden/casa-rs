#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare setjy on matched fluxscale dictionaries and their saved MS models."""

import argparse
from copy import deepcopy
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks as casa
from casatools import table
from casars import tasks as rust
import numpy as np

from record_casa_returns import make_cal_ms
from setjy_manual_oracle import (
    add_second_window, casa_ms, compare, compare_returns, consume, data, make_input,
    rust_ms,
)


def compare_case(seed, root, name, fluxdict, *, history_labels=(), **options):
    with TemporaryDirectory(prefix=name + '-', dir=root) as directory:
        case = Path(directory)
        for scratch in (True, False):
            paths, returned = [], []
            for label, tasks in (('casa', casa), ('rust', rust)):
                path = case / f'{label}-{scratch}.ms'
                make_input(seed, path, scratch=scratch)
                result = tasks.setjy(vis=str(path), standard='fluxscale', fluxdict=fluxdict,
                                     usescratch=scratch, **options)
                if history_labels:
                    reader = table()
                    assert reader.open(str(path / 'HISTORY'))
                    try:
                        assert any('spwName' in row and all(label in row for label in history_labels)
                                   for row in reader.getcol('MESSAGE'))
                    finally:
                        assert reader.done()
                paths.append(path)
                returned.append(result)
            compare_returns(returned[1], returned[0])
            if scratch:
                scratch_model = data(paths[0], 'MODEL_DATA')
                compare(name + ' scratch', data(paths[1], 'MODEL_DATA'), scratch_model)
                continue
            reference = None
            for writer, path in zip(('casa', 'rust'), paths):
                for reader, tool in (('casa', casa_ms), ('rust', rust_ms)):
                    read = case / f'{writer}-{reader}-read.ms'
                    shutil.copytree(path, read)
                    measured = consume(tool, read)
                    shutil.rmtree(read)
                    if reference is None:
                        reference = measured
                    compare(f'{name} {writer} writer {reader} reader', measured, reference)


def real_fluxscale(seed, root):
    for name, tasks in (('casa', casa), ('rust', rust)):
        gain = root / f'{name}-gain.cal'
        scale = root / f'{name}-scale.cal'
        tasks.gaincal(vis=str(seed), caltable=str(gain), refant='A0', solint='int', minsnr=0.)
        result = tasks.fluxscale(vis=str(seed), caltable=str(gain), fluxtable=str(scale),
                                 reference=['FLUX'], transfer=['PHASE'])
        assert result['1']['fitFluxd'] > 0 and result['1']['spidx'].shape == (2,)
        compare_case(seed, root, name + '-fluxscale-output', result, field='1')
    constant = rust.fluxscale(vis=str(seed), caltable=str(root / 'rust-gain.cal'),
                              fluxtable=str(root / 'constant-scale.cal'),
                              reference=['FLUX'], transfer=['PHASE'], fitorder=0)
    assert constant['1']['spidx'].shape == (1,)
    manual = {'standard': 'manual', 'fluxdensity': constant['1']['fitFluxd']}
    compare_manual(seed, root, 'constant-fit', constant, manual, field='1')


def compare_manual(seed, root, name, dictionary, manual, **selection):
    for scratch in (True, False):
        left, right = [root / f'{name}-{label}-{scratch}.ms' for label in ('casa', 'rust')]
        for path in (left, right):
            make_input(seed, path, scratch=scratch)
        expected = casa.setjy(str(left), usescratch=scratch, **manual, **selection)
        actual = rust.setjy(str(right), standard='fluxscale', fluxdict=dictionary,
                            usescratch=scratch, **selection)
        compare_returns(actual, expected)
        if scratch:
            compare(name + ' scratch', data(right, 'MODEL_DATA'), data(left, 'MODEL_DATA'))
        else:
            reference = consume(casa_ms, left)
            native_copy = root / f'{name}-read-rust.ms'
            shutil.copytree(right, native_copy)
            compare(name + ' CASA virtual reader', consume(casa_ms, right), reference)
            compare(name + ' native virtual reader', consume(rust_ms, native_copy), reference)


def payload():
    window = lambda flux: {'fluxd': np.array(flux, dtype=np.float64)}
    return {
        '0': {'0': window([3., .1, .2, 0.]), '1': window([8., .4, .5, .6]),
              'fitFluxd': np.float64(2.4), 'fitRefFreq': 1.4e9, 'spidx': np.array([.4, -.5, .02])},
        '1': {'0': window([5., 0., 0., 0.]), '1': window([4., 0., 0., 0.]),
              'fitFluxd': 3.4, 'fitRefFreq': 1.4e9, 'spidx': np.array([.4, .5, .02])},
        'spwID': np.array([0, 1]), 'freq': np.array([1.4e9, 1.5e9]),
    }


def synthetic_fits(seed, root):
    source = root / 'two-windows.ms'
    make_input(seed, source, scratch=True)
    add_second_window(source)
    reference = payload()
    labels = ('First-window', 'Second-window')
    with_names = dict(reference, spwName=np.array(labels, dtype='S'))
    compare_case(source, root, 'byte-window-labels', with_names, history_labels=labels)
    compare_case(source, root, 'two-field-fit', reference, fluxdensity=0., spix=10.5, reffreq='bad')
    reversed_fields = {'1': reference['1'], '0': reference['0'],
                       'spwID': reference['spwID'], 'freq': reference['freq']}
    compare_case(source, root, 'reverse-field-order', reversed_fields)
    compare_manual(source, root, 'selected-window', reference, {
        'standard': 'manual', 'fluxdensity': 2.4, 'spix': [-.5, .02], 'reffreq': '1.4GHz',
    }, field='0', spw='1:0~2')

    window = reference['0']['0']
    window['fluxd'][0] = -1.
    compare_case(source, root, 'invalid-first-window', reference, field='0')
    reference['0']['fitFluxd'] = 0.
    compare_case(source, root, 'first-valid-window-flux', reference, field='0',
                 polindex=[.1, .02], polangle=[.3, -.04], rotmeas=9.)

    reference['0']['1']['fluxd'][0] = -1.
    reference['0']['fitFluxd'] = 2.5
    for scratch in (True, False):
        path = root / f'flagged-windows-{scratch}.ms'
        make_input(source, path, scratch=scratch)
        assert rust.setjy(str(path), field='0', standard='manual', fluxdensity=7., usescratch=scratch)
        before = data(path, 'MODEL_DATA') if scratch else source_record(path)
        result = rust.setjy(str(path), field='0', standard='fluxscale', fluxdict=reference,
                            usescratch=scratch)
        assert list(result) == ['format']
        after = data(path, 'MODEL_DATA') if scratch else source_record(path)
        np.testing.assert_equal(after, before)

    reference['0']['1']['fluxd'][0] = 4.
    reference['0']['fitFluxd'] = -1.
    compare_case(source, root, 'negative-fit-catalog-fallback', reference, field='0')


def source_record(path):
    reader = table()
    assert reader.open(str(path / 'SOURCE'))
    key = reader.getkeyword('definedmodel_field_0')
    result = reader.getcell('SOURCE_MODEL', reader.getkeyword(key))
    assert reader.done()
    return result


def partial_field_order(seed, root):
    dictionary = payload()
    dictionary['0']['spidx'] = np.array([0., 11.])
    dictionary = {'1': dictionary['1'], '0': dictionary['0'],
                  'spwID': np.array([0]), 'freq': np.array([1.4e9])}
    missing = data(seed, 'FIELD_ID') == 0
    models = []
    for name, tasks in (('casa', casa), ('rust', rust)):
        target = root / f'{name}-partial-field.ms'
        make_input(seed, target, scratch=True)
        assert tasks.setjy(str(target), standard='manual', fluxdensity=7.)
        before = data(target, 'MODEL_DATA')
        try:
            tasks.setjy(str(target), standard='fluxscale', fluxdict=dictionary)
        except Exception as error:
            assert 'Field 0: the spectral index is greater than 10!' in str(error)
        else:
            raise AssertionError(f'{name} accepted the invalid later field')
        after = data(target, 'MODEL_DATA')
        np.testing.assert_array_equal(after[..., missing], before[..., missing])
        assert not np.array_equal(after[..., ~missing], before[..., ~missing])
        models.append(after)
    compare('partial field order scratch', models[1], models[0])


def refused_domains(seed, root):
    base = payload()
    missing_window_list = {'freq': [1.4e9]}
    high_index = deepcopy(base)
    high_index['0']['spidx'] = np.array([0., 10.5])
    no_common_field = deepcopy(base)
    del no_common_field['0']
    outside = '1980/01/01/00:00:00~1980/01/01/01:00:00'
    options = (
        ({}, outside, 'fluxdict is empty'),
        (missing_window_list, '', 'Input fluxdict is missing keywords:spwID'),
        (high_index, '', 'Field 0: the spectral index is greater than 10!'),
        (no_common_field, outside, 'No field was found in fluxdict for the given data selection'),
    )
    for index, (dictionary, timerange, expected) in enumerate(options):
        for name, tasks in (('casa', casa), ('rust', rust)):
            target = root / f'{name}-invalid-{index}.ms'
            make_input(seed, target, scratch=False)
            try:
                tasks.setjy(str(target), field='0', timerange=timerange, standard='fluxscale',
                            fluxdict=dictionary, usescratch=False)
            except Exception as error:
                assert expected in str(error), (name, str(error))
            else:
                raise AssertionError(f'{name} accepted malformed fluxscale inputs')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    seed = args.root / 'transfer.ms'
    make_cal_ms(str(seed), frequency='1.4GHz')
    real_fluxscale(seed, args.root)
    synthetic_fits(seed, args.root)
    partial_field_order(seed, args.root)
    refused_domains(seed, args.root)


if __name__ == '__main__':
    main()
