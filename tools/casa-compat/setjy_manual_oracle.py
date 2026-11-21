#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public manual setjy and reuse of its MS models with pinned CASA."""

import argparse
import inspect
from pathlib import Path
import shutil
from time import perf_counter

from casars.tasks import setjy as rust_setjy
from casars.tools import ms as rust_ms
from casatasks import setjy as casa_setjy
from casatools import ms as casa_ms, table
import numpy as np

from imager_legacy_pbw_oracle import data, make_input
from imager_legacy_squint_oracle import compare, consume
from record_casa_returns import make_cal_ms


def compare_returns(actual, expected, *, rtol=1e-12):
    assert list(actual) == list(expected)
    assert actual['format'] == expected['format']
    for field in actual.keys() - {'format'}:
        assert list(actual[field]) == list(expected[field])
        assert actual[field]['fieldName'] == expected[field]['fieldName']
        for spw in actual[field].keys() - {'fieldName'}:
            flux = actual[field][spw]['fluxd']
            assert flux.shape == (4,) and flux.dtype == np.float64
            np.testing.assert_allclose(flux, expected[field][spw]['fluxd'], rtol=rtol, atol=1e-12)


def read_component(dataset):
    reader = table()
    assert reader.open(str(dataset / 'SOURCE'))
    key = reader.getkeyword('definedmodel_field_0')
    record = reader.getcell('SOURCE_MODEL', reader.getkeyword(key))['cl_0']
    assert reader.done()
    return record, record['container']['component0']


def compare_components(actual_ms, expected_ms, *, rtol=1e-12):
    actual, actual_component = read_component(actual_ms)
    expected, expected_component = read_component(expected_ms)
    for key in ('type', 'version', 'indexcombination', 'channelselection'):
        np.testing.assert_array_equal(actual[key], expected[key])
    assert expected['channelselection'][0, 2] > 3
    assert actual_component['shape']['type'] == expected_component['shape']['type'] == 'Point'
    np.testing.assert_allclose(actual_component['flux']['value'], expected_component['flux']['value'],
                               rtol=rtol, atol=1e-12)
    actual_spectrum = actual_component['spectrum']
    expected_spectrum = expected_component['spectrum']
    assert actual_spectrum['type'] == expected_spectrum['type']
    assert actual_spectrum['frequency']['refer'] == expected_spectrum['frequency']['refer']


def verify_case(seed, root, name, options, *, standard='manual', return_rtol=1e-12):
    reference_model = None
    for scratch in (True, False):
        datasets = []
        returns = []
        for engine, task in (('casa', casa_setjy), ('rust', rust_setjy)):
            target = root / f'{name}-{engine}-{scratch}.ms'
            make_input(seed, target, scratch=scratch)
            before = perf_counter()
            params = dict(vis=str(target), field='0', spw='0:0~2', usescratch=scratch)
            if standard is not None:
                params['standard'] = standard
            result = task(**params, **options)
            print(name, engine, 'scratch', scratch, 'seconds', perf_counter() - before, flush=True)
            returns.append(result)
            datasets.append(target)
        expected, actual = datasets
        compare_returns(returns[1], returns[0], rtol=return_rtol)
        if scratch:
            reference_model = data(expected, 'MODEL_DATA')
            compare(name + ' scratch visibility', data(actual, 'MODEL_DATA'), reference_model)
            continue
        compare_components(actual, expected, rtol=return_rtol)
        for engine, tool in (('casa', casa_ms), ('rust', rust_ms)):
            for target in datasets:
                read_copy = root / (target.stem + '-read-' + engine + '.ms')
                shutil.copytree(target, read_copy)
                compare(f'{read_copy.stem} model subtraction', consume(tool, read_copy), reference_model)
                shutil.rmtree(read_copy)


def add_second_window(dataset):
    """Keep one field across both windows and one in just the second."""
    for subtable, edits in (
        ('SPECTRAL_WINDOW', {'NAME': 'SPW1'}),
        ('DATA_DESCRIPTION', {'SPECTRAL_WINDOW_ID': 1}),
    ):
        writer = table()
        assert writer.open(str(dataset / subtable), nomodify=False)
        values = {name: writer.getcell(name, 0) for name in writer.colnames()
                  if writer.iscelldefined(name, 0)}
        writer.addrows(1)
        for name, value in (values | edits).items():
            assert writer.putcell(name, 1, value)
        assert writer.done()
    writer = table()
    assert writer.open(str(dataset / 'FIELD'), nomodify=False)
    assert writer.putcell('SOURCE_ID', 1, -1) and writer.done()
    writer = table()
    assert writer.open(str(dataset), nomodify=False)
    fields = writer.getcol('FIELD_ID')
    times = writer.getcol('TIME')
    ddids = ((fields == 1) | (times > np.median(times[fields == 0]))).astype(np.int32)
    assert writer.putcol('DATA_DESC_ID', ddids) and writer.done()
    assert np.any((fields == 0) & (ddids == 1)) and np.all(ddids[fields == 1] == 1)
    return fields, ddids


def verify_replacement(seed, root):
    sparse = root / 'sparse-template.ms'
    make_input(seed, sparse, scratch=False)
    fields, ddids = add_second_window(sparse)
    first, second, models = [], [], []
    for engine, task in (('casa', casa_setjy), ('rust', rust_setjy)):
        target = root / f'replacement-{engine}.ms'
        make_input(sparse, target, scratch=False)
        params = dict(vis=str(target), standard='manual', usescratch=False)
        first.append(task(**params, field='0,1', spw='0,1', fluxdensity=2.))
        second.append(task(**params, field='0', spw='0:0~2', fluxdensity=3.))
        result = []
        for label, reader in (('casa', casa_ms), ('rust', rust_ms)):
            copy = root / f'replacement-{engine}-read-{label}.ms'
            shutil.copytree(target, copy)
            result.append(consume(reader, copy))
            shutil.rmtree(copy)
        models.append(result)
    compare_returns(first[1], first[0])
    compare_returns(second[1], second[0])
    assert '0' in first[0]['1'], first[0]
    expected = np.broadcast_to(np.where((fields == 0) & (ddids == 0), 3., 2.), models[0][0].shape)
    for engine, values in zip(('casa', 'rust'), models):
        for reader, result in zip(('casa', 'rust'), values):
            compare(f'replacement {engine} writer {reader} reader', result, expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('vla_ms', type=Path, help='VLA fixture with full correlation coverage')
    parser.add_argument('root', type=Path, help='new directory for the independent model writes')
    args = parser.parse_args()
    assert list(inspect.signature(rust_setjy).parameters) == list(inspect.signature(casa_setjy).parameters)
    args.root.mkdir()
    verify_case(args.vla_ms, args.root, 'literal', {'fluxdensity': 2.4})
    verify_case(args.vla_ms, args.root, 'spectral-index', {
        'fluxdensity': [2.4], 'spix': -.74, 'reffreq': '1.41GHz',
    })
    verify_case(args.vla_ms, args.root, 'polarized', {
        'fluxdensity': [2.4, .12, -.20, .03], 'spix': [-.74, .13],
        'reffreq': '1.41GHz', 'polindex': [.09, .02], 'polangle': [.3, .04], 'rotmeas': 17.,
    })
    verify_case(args.vla_ms, args.root, 'window-median', {
        'fluxdensity': [2.4], 'spix': [-.74, .13], 'reffreq': '1.41GHz', 'scalebychan': False,
    })
    verify_case(args.vla_ms, args.root, 'topocentric-reference', {
        'fluxdensity': [2.4], 'spix': -.74, 'reffreq': 'TOPO 1.41GHz',
    }, return_rtol=5e-8)
    template = args.root / '128-channels.ms'
    make_cal_ms(str(template), channels=128, frequency='1.4GHz')
    verify_case(template, args.root, '128-channel-median', {
        'fluxdensity': [2.4], 'spix': [-.74, .13], 'reffreq': '1.4GHz', 'scalebychan': False,
    })
    verify_replacement(template, args.root)


if __name__ == '__main__':
    main()
