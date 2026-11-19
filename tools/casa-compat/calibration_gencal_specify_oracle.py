#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare specified gain, phase and delay tables with CASA 6.7.6.14."""

import argparse
from array import array
from collections import deque
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from gaincal_oracle import relative_rms
from record_casa_returns import make_cal_ms


def with_second_window(vis):
    tool = table()
    for subtable in ('SPECTRAL_WINDOW', 'DATA_DESCRIPTION'):
        assert tool.open(str(vis / subtable), nomodify=False)
        try:
            tool.addrows(1)
            for column in tool.colnames():
                if tool.iscelldefined(column, 0):
                    tool.putcell(column, 1, tool.getcell(column, 0))
            if subtable == 'SPECTRAL_WINDOW':
                tool.putcell('NAME', 1, 'SPW1')
                frequencies = tool.getcell('CHAN_FREQ', 0) + 100e6
                frequencies[4:] += 2e6
                tool.putcell('CHAN_FREQ', 1, frequencies)
                tool.putcell('REF_FREQUENCY', 1, tool.getcell('REF_FREQUENCY', 0) + 100e6)
                tool.putcell('TOTAL_BANDWIDTH', 1, tool.getcell('TOTAL_BANDWIDTH', 0) + 2e6)
            else:
                tool.putcell('SPECTRAL_WINDOW_ID', 1, 1)
        finally:
            tool.close()
    assert tool.open(str(vis), nomodify=False)
    try:
        descriptions = tool.getcol('DATA_DESC_ID')
        descriptions[1::2] = 1
        tool.putcol('DATA_DESC_ID', descriptions)
    finally:
        tool.close()


def columns(path, names):
    tool = table()
    assert tool.open(str(path))
    try:
        return {name: tool.getcol(name) for name in names}
    finally:
        tool.close()


def product(path):
    tool = table()
    assert tool.open(str(path))
    try:
        keywords = {name: tool.getkeyword(name) for name in ('VisCal', 'ParType', 'PolBasis', 'MSName')}
        assert all(not tool.iscelldefined('WEIGHT', row) for row in range(tool.nrows()))
        values = {name: tool.getcol(name) for name in tool.colnames() if name != 'WEIGHT'}
    finally:
        tool.close()
    windows = columns(path / 'SPECTRAL_WINDOW', ('NUM_CHAN', 'REF_FREQUENCY', 'CHAN_FREQ',
                                                'CHAN_WIDTH', 'TOTAL_BANDWIDTH', 'EFFECTIVE_BW',
                                                'RESOLUTION', 'FLAG_ROW'))
    return keywords, values, windows


def compare_tables(native, reference):
    nat_keywords, nat_values, nat_windows = product(native)
    ref_keywords, ref_values, ref_windows = product(reference)
    assert nat_keywords == ref_keywords, (nat_keywords, ref_keywords)
    assert nat_values.keys() == ref_values.keys()
    for name, expected in ref_values.items():
        actual = nat_values[name]
        assert actual.dtype == expected.dtype
        if name == 'CPARAM':
            np.testing.assert_allclose(actual, expected, rtol=3e-7, atol=0.)
        else:
            np.testing.assert_array_equal(actual, expected, err_msg=name)
    for name, expected in ref_windows.items():
        np.testing.assert_array_equal(nat_windows[name], expected, err_msg=name)
    name = 'CPARAM' if 'CPARAM' in ref_values else 'FPARAM'
    metric_type = np.complex128 if name == 'CPARAM' else np.float64
    return relative_rms(nat_values[name].astype(metric_type), ref_values[name].astype(metric_type))


def generate(module, vis, table_path, stages):
    for kind, options in stages:
        assert module.gencal(str(vis), str(table_path), kind, **options) is None


def apply_both(root, source, casa_table, native_table, native, *, calwt=True, spectral_weights=False,
               weight_rtol=None, spw='', spwmap=None):
    common = ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'CORRECTED_DATA')
    if spectral_weights:
        common += ('WEIGHT_SPECTRUM',)
    products = {}
    mapping = {} if spwmap is None else {'spwmap': spwmap}
    for engine, module in (('casa', casa), ('native', native)):
        for writer, table_path in (('casa', casa_table), ('native', native_table)):
            dataset = root / f'apply-{engine}-{writer}.ms'
            copy_ms(source, dataset)
            module.applycal(str(dataset), spw=spw, gaintable=[str(table_path)], interp=['nearest'],
                            calwt=[calwt], **mapping)
            products[engine, writer] = columns(dataset, common)
    expected = products['casa', 'casa']
    errors = []
    for output in products.values():
        for name in ('DATA', 'FLAG', 'FLAG_ROW'):
            np.testing.assert_array_equal(output[name], expected[name], err_msg=name)
        for name in common[3:]:
            errors.append(relative_rms(output[name], expected[name]))
            assert errors[-1] < 3e-5, (name, errors[-1])
            if weight_rtol is not None and name.startswith('WEIGHT'):
                np.testing.assert_allclose(output[name], expected[name], rtol=weight_rtol, atol=0.)
    return max(errors)


def reject_smaller_grid(root, source, native):
    expanded = root / 'expanded' / source.name
    expanded.parent.mkdir()
    copy_ms(source, expanded)
    tool = table()
    assert tool.open(str(expanded / 'ANTENNA'), nomodify=False)
    try:
        extra = tool.nrows()
        tool.addrows(1)
        for column in tool.colnames():
            if tool.iscelldefined(column, 0):
                tool.putcell(column, extra, tool.getcell(column, 0))
        tool.putcell('NAME', extra, 'extra-antenna')
    finally:
        tool.close()

    reference, candidate = root / 'larger-casa.cal', root / 'larger-native.cal'
    for module, path in ((casa, reference), (native, candidate)):
        generate(module, expanded, path, [('amp', {'parameter': 2.0})])
        before = columns(path, ('ANTENNA1', 'SPECTRAL_WINDOW_ID', 'CPARAM'))
        assert extra in before['ANTENNA1']
        try:
            module.gencal(str(source), str(path), 'amp', parameter=3.0)
        except RuntimeError as error:
            assert 'Calibrater::specifycal' in str(error)
        else:
            raise AssertionError(f'{module.__name__} updated the incompatible table')
        for name, values in columns(path, before).items():
            np.testing.assert_array_equal(values, before[name])
    return {'refused': True, 'table_nrms': compare_tables(candidate, reference)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    os.chdir(root)
    source = root / 'source.ms'
    make_cal_ms(str(source), stokes='XX XY YX YY')
    with_second_window(source)
    data_columns = ('DATA', 'FLAG', 'FLAG_ROW', 'SIGMA', 'WEIGHT')
    before = columns(source, data_columns)
    cases = {
        'amp-broadcast': [('amp', {'parameter': 2.0})],
        'amp-ordered': [('amp', {'spw': '1,0', 'antenna': 'A3,A1', 'pol': 'Y,X',
                                 'parameter': [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]})],
        'amp-nonuniform': [('amp', {'antenna': 'A1,A3', 'pol': 'X', 'uniform': False,
                                     'parameter': [2.0, 3.0]})],
        'amp-boolean-list': [('amp', {'antenna': 'A1,A3', 'pol': 'X', 'parameter': [False, 2.0]})],
        'amp-boolean-object': [('amp', {'antenna': 'A1', 'parameter': np.array([False], dtype=object)})],
        'amp-neg-and-zero': [('amp', {'spw': '0', 'antenna': 'A1,A3', 'pol': 'L',
                                      'parameter': [-2.0, 0.0]})],
        'amp-baseline': [('amp', {'antenna': 'A1&A2', 'pol': 'X', 'parameter': 2.0})],
        'amp-regex-baseline': [('amp', {'antenna': '/A1&A2/', 'pol': 'X', 'parameter': 2.0})],
        'amp-channel-ranges': [('amp', {'spw': '1:0~1;3~4', 'antenna': 'A1', 'pol': 'X',
                                       'parameter': [2.0, 3.0]})],
        'phase-selected': [('ph', {'spw': 'SPW1', 'antenna': 'A1,A3', 'pol': 'Y', 'parameter': 90.0})],
        'amp-then-phase': [('amp', {'antenna': 'A1', 'pol': 'X', 'parameter': 2.0}),
                           ('ph', {'antenna': 'A1', 'pol': 'X', 'parameter': -30.0})],
        'amp-twice': [('amp', {'antenna': 'A1', 'pol': 'X', 'parameter': 2.0}),
                      ('amp', {'antenna': 'A1', 'pol': 'X', 'parameter': 3.0})],
        'amp-repeated-update': [('amp', {'antenna': 'A1', 'pol': 'X', 'spw': '1', 'parameter': 1e-30}),
                                ('amp', {'antenna': 'A1', 'pol': 'X', 'spw': '1:0~1;3~4',
                                         'parameter': [1e30, 1e30]})],
        'single-band-delay': [('sbd', {'spw': '0', 'antenna': 'A1', 'pol': 'X', 'parameter': 6.0})],
        'multi-band-delay': [('mbd', {'spw': '1', 'antenna': 'A1', 'pol': 'Y', 'parameter': -4.0})],
        'delay-update': [('sbd', {'antenna': 'A1', 'pol': 'X', 'parameter': 6.0}),
                         ('mbd', {'antenna': 'A1', 'pol': 'X', 'parameter': -4.0})],
        'delay-repeated-update': [('sbd', {'antenna': 'A1', 'pol': 'X', 'spw': '1', 'parameter': 1e8}),
                                  ('sbd', {'antenna': 'A1', 'pol': 'X', 'spw': '1:0~1;3~4',
                                           'parameter': [-1e8, 1.0]})],
    }
    application = {'amp-ordered', 'amp-then-phase', 'single-band-delay', 'multi-band-delay'}
    results = {}
    for label, stages in cases.items():
        case = root / label
        case.mkdir()
        reference, candidate = case / 'casa.cal', case / 'native.cal'
        generate(casa, source, reference, stages)
        generate(native, source, candidate, stages)
        results[label] = {'table_nrms': compare_tables(candidate, reference)}
        if label in application:
            results[label]['apply_nrms'] = apply_both(case, source, reference, candidate, native)
        if len(stages) > 1:
            for writer, reader in ((casa, native), (native, casa)):
                transfer = case / f'cross-{writer.__name__}.cal'
                generate(writer, source, transfer, stages[:1])
                generate(reader, source, transfer, stages[1:])
                compare_tables(transfer, reference)

    for label, vector in {
        'object-array': lambda: np.array([2., 3.], dtype=object),
        'range': lambda: range(2, 4),
        'generator': lambda: (value for value in (2., 3.)),
        'array': lambda: array('d', [2., 3.]),
        'deque': lambda: deque([2., 3.]),
    }.items():
        reference, candidate = root / f'{label}-casa.cal', root / f'{label}-native.cal'
        for module, path in ((casa, reference), (native, candidate)):
            generate(module, source, path,
                     [('amp', {'antenna': 'A1,A3', 'pol': 'X', 'parameter': vector()})])
        results[label] = {'table_nrms': compare_tables(candidate, reference)}

    results['different-antenna-grid'] = reject_smaller_grid(root, source, native)

    for label, options in {
        'empty-parameter': {},
        'wrong-count': {'parameter': [2.0, 3.0]},
        'bad-polarization': {'pol': 'XX', 'parameter': 2.0},
        'missing-antenna': {'antenna': 'missing', 'parameter': 2.0},
    }.items():
        for module in (casa, native):
            candidate = root / f'{label}-{module.__name__}.cal'
            try:
                module.gencal(str(source), str(candidate), 'amp', **options)
            except RuntimeError as error:
                assert 'Calibrater::specifycal' in str(error)
            else:
                raise AssertionError(f'{module.__name__} accepted {label}')
            assert not candidate.exists()
        results[label] = {'refused': True}

    for label, value in [('boolean-scalar', True), ('boolean-array', np.array([True]))]:
        for module, exception in ((casa, AssertionError), (native, TypeError)):
            candidate = root / f'{label}-{module.__name__}.cal'
            try:
                module.gencal(str(source), str(candidate), 'amp', parameter=value)
            except exception as error:
                assert "must be of cFloatVec type" in str(error)
            else:
                raise AssertionError(f'{module.__name__} accepted {label}')
            assert not candidate.exists()
        results[label] = {'refused': True}

    reference = root / 'amp-broadcast' / 'casa.cal'
    fixed = root / 'fixed-wildcards.cal'
    generate(native, source, fixed, [('amp', {'spw': '*', 'antenna': '*', 'parameter': 2.0})])
    results['wildcards'] = {'table_nrms': compare_tables(fixed, reference)}
    try:
        native.gencal(str(source), str(fixed), 'amp', antenna='A1;!A3', parameter=2.0)
    except RuntimeError as error:
        assert 'negative calibration index' in str(error)
        compare_tables(fixed, reference)
    else:
        raise AssertionError('negative calibration index accepted')
    results['negative-antenna'] = {'refused': True}

    for name, values in columns(source, data_columns).items():
        np.testing.assert_array_equal(values, before[name])
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
