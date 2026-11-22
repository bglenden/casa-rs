#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA 6.7.6.14 single-dish averaging across operations on one tool."""

import argparse
from pathlib import Path
import time

import casatasks
from casatools import singledishms
import numpy as np

from calibration_smoothcal_oracle import opened
from sdbaseline_variable_oracle import write_parameters
from sdfit_oracle import copy_rows, line_map, timed_spectra
from sdsmooth_oracle import floating_data
from singledish_import_oracle import check_readers


MODES = {
    'stokes': {'polaverage': 'stokes'},
    'geometric': {'polaverage': 'geometric'},
    'time': {'timebin': '8s', 'timespan': 'scan'},
    'both': {'timebin': '8s', 'timespan': 'scan', 'polaverage': 'geometric'},
}


def prepare(tool, source, prefix, mode, reindex):
    assert tool.open(str(source))
    assert tool.set_selection(spw='1', reindex=reindex)
    assert tool.fit_line(spw='1', nfit='0', tempoutfile=str(prefix.with_suffix('.fit.ms')),
                         **MODES[mode])
    # A new selection and a fit with omitted averaging arguments retain state.
    assert tool.set_selection(spw='1', reindex=reindex)
    assert tool.fit_line(spw='1', nfit='0', tempoutfile=str(prefix.with_suffix('.keep.ms')))


def check_product(path, mode, reindex, backend):
    with (opened(path) as table, opened(path / 'DATA_DESCRIPTION') as dd,
          opened(path / 'POLARIZATION') as pol):
        averaged_time = mode in ('time', 'both')
        nrows = 1 if averaged_time else 4
        count = 2 if averaged_time or mode == 'reopen' else 1
        assert table.nrows() == nrows, (path, table.nrows(), nrows)
        np.testing.assert_array_equal(table.getcol('DATA_DESC_ID'), [int(not reindex)] * nrows)
        for row in range(nrows):
            assert table.getcell('DATA', row).shape == (count, 96), (path, row)
            desc = table.getcell('DATA_DESC_ID', row)
            products = pol.getcell('CORR_TYPE', dd.getcell('POLARIZATION_ID', desc))
            expected = [1] if count == 1 or (mode == 'both' and backend == 'casa') else [9, 12]
            np.testing.assert_array_equal(products, expected, err_msg=str(path))


def compare_rows(a, b, *, statistics=True):
    with opened(a) as left, opened(b) as right:
        assert left.nrows() == right.nrows()
        for name in ('TIME', 'TIME_CENTROID', 'INTERVAL', 'EXPOSURE', 'SCAN_NUMBER',
                     'FLAG_ROW', 'FLAG', 'DATA', 'WEIGHT', 'SIGMA',
                     'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'):
            if not statistics and name in ('WEIGHT', 'SIGMA', 'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'):
                continue
            assert (name in left.colnames()) == (name in right.colnames()), (a, b, name)
            if name not in left.colnames():
                continue
            for row in range(left.nrows()):
                if not left.iscelldefined(name, row):
                    assert not right.iscelldefined(name, row), (a, b, name, row)
                    continue
                expect = left.getcell(name, row)
                result = right.getcell(name, row)
                if name in ('DATA', 'WEIGHT', 'SIGMA', 'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'):
                    np.testing.assert_allclose(result, expect, rtol=2e-5, atol=2e-6,
                                               err_msg=f'{a.name} {b.name} {name} row {row}')
                else:
                    np.testing.assert_array_equal(result, expect,
                                                  err_msg=f'{a.name} {b.name} {name} row {row}')


def compare_baseline_tables(a, b):
    with opened(a) as expected, opened(b) as actual:
        assert expected.nrows() == actual.nrows()
        for row in range(expected.nrows()):
            for name in ('SCANNO', 'BEAMNO', 'ANTNO', 'IFNO', 'TIME', 'APPLY',
                         'FUNC_TYPE', 'FUNC_PARAM', 'NCHAN'):
                np.testing.assert_array_equal(actual.getcell(name, row), expected.getcell(name, row),
                                              err_msg=f'{name} row {row}')
            left = expected.getcell('MASKLIST', row)
            right = actual.getcell('MASKLIST', row)
            channels = np.arange(expected.getcell('NCHAN', row))
            for a_ranges, b_ranges in zip(left, right, strict=True):
                def mask(ranges):
                    lo, hi = ranges.reshape(-1, 2).T
                    return np.any((lo[:, None] <= channels) & (channels <= hi[:, None]), axis=0)

                np.testing.assert_array_equal(mask(a_ranges), mask(b_ranges))
            for name in ('RESULT', 'RMS'):
                np.testing.assert_allclose(actual.getcell(name, row), expected.getcell(name, row),
                                           rtol=2e-5, atol=2e-6, err_msg=f'{name} row {row}')


def check_projection(source, output, mode):
    with opened(source) as original, opened(output) as result:
        time_average = mode in ('time', 'both')
        data = original.getcol('DATA')
        flagged = original.getcol('FLAG')
        if 'WEIGHT_SPECTRUM' in original.colnames():
            weight = original.getcol('WEIGHT_SPECTRUM')
        else:
            weight = original.getcol('WEIGHT')[:, None, :]
        if mode == 'stokes':
            weight = np.ones_like(weight)
        axis = 2 if time_average else 0
        output_flags = np.all(flagged, axis=axis, keepdims=True)
        use_sample = (~flagged | output_flags) if time_average else ~flagged
        weight = np.where(use_sample, weight, 0.)
        count = weight.sum(axis=axis, keepdims=True)
        total = np.sum(data * weight, axis=axis, keepdims=True)
        expected = np.divide(total, count, out=np.zeros_like(total), where=count > 0)
        np.testing.assert_allclose(result.getcol('DATA'), expected, rtol=2e-6, atol=1e-7)
        np.testing.assert_array_equal(result.getcol('FLAG'), output_flags)


def process(root, source, tools, mode, reindex):
    products, tables = {}, {}
    polarized = mode in ('stokes', 'geometric')
    for name in tools:
        prefix = root / f'{mode}-{reindex}-{name}'
        tables[name] = prefix.with_suffix('.bltable')
    for name, factory in tools.items():
        tool = factory()
        prefix = root / f'{mode}-{reindex}-{name}'
        products[name] = outputs = {}
        start = time.perf_counter()
        try:
            prepare(tool, source, prefix, mode, reindex)
            for action, kwargs in (
                ('smooth', {'width': 1, 'type': 'boxcar'}),
                ('subtract_baseline', {'order': 1, 'spw': '1', 'bloutput': f',,{tables[name]}'}),
                ('atmcor', {'config': {}}),
            ):
                output = outputs[action] = prefix.with_suffix(f'.{action}.ms')
                assert getattr(tool, action)(outfile=str(output), **kwargs)
                check_product(output, mode, reindex, name)
            check_projection(source, outputs['atmcor'], mode)
            # The reference application aborts on its own one-correlation table.
            if name == 'native' or not polarized:
                output = outputs['self-apply'] = prefix.with_suffix('.self-apply.ms')
                assert tool.apply_baseline_table(str(tables[name]), spw='1', outfile=str(output))
                check_product(output, mode, reindex, name)
                compare_rows(outputs['subtract_baseline'], output, statistics=False)
            assert tool.close()
            assert tool.open(str(source))
            assert tool.set_selection(spw='1', reindex=reindex)
            output = outputs['reopen'] = prefix.with_suffix('.reopen.ms')
            assert tool.smooth(outfile=str(output), type='boxcar', width=1)
            check_product(output, 'reopen', reindex, name)
        finally:
            assert tool.close()
        print(f'{mode} reindex={reindex} {name} {time.perf_counter() - start:.3f}s', flush=True)
    if 'native' not in tools:
        return
    compare_baseline_tables(tables['casa'], tables['native'])
    for action in products['casa']:
        compare_rows(products['casa'][action], products['native'][action])
    for name, peer in (('native', 'casa'), ('casa', 'native')):
        if name == 'casa' and polarized:
            continue
        tool = tools[name]()
        prefix = root / f'{mode}-{reindex}-{name}-cross'
        output = prefix.with_suffix('.ms')
        try:
            prepare(tool, source, prefix, mode, reindex)
            assert tool.apply_baseline_table(str(tables[peer]), spw='1', outfile=str(output))
            check_product(output, mode, reindex, name)
            compare_rows(products[name]['self-apply'], output)
        finally:
            assert tool.close()
    for reader in products.values():
        check_readers(reader.values())


def check_scalar_weights(root, source, tools):
    scalar = copy_rows(source, root / 'scalar-weights.ms', [0, 1, 2, 3])
    with opened(scalar, True) as table:
        table.removecols(['WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'])
        for row in range(table.nrows()):
            table.putcell('WEIGHT', row, np.array([2., 7.], dtype=np.float32))
            data = table.getcell('DATA', row)
            data += np.array([[1j], [3j]], dtype=np.complex64)
            table.putcell('DATA', row, data)
            flags = table.getcell('FLAG', row)
            flags[:, 42] = True
            table.putcell('FLAG', row, flags)
    outputs = {}
    for name, factory in tools.items():
        tool = factory()
        prefix = root / f'scalar-weights-{name}'
        output = outputs[name] = prefix.with_suffix('.ms')
        try:
            prepare(tool, scalar, prefix, 'geometric', True)
            assert tool.atmcor({}, outfile=str(output))
            check_product(output, 'geometric', True, name)
            check_projection(scalar, output, 'geometric')
            with opened(output) as table:
                np.testing.assert_allclose(table.getcol('WEIGHT'), np.full((1, 4), 9.), rtol=2e-7)
                np.testing.assert_allclose(table.getcol('SIGMA'), np.full((1, 4), 1. / 3.))
        finally:
            assert tool.close()
    if 'native' in outputs:
        compare_rows(outputs['casa'], outputs['native'])


def edge_tool(factory, source, prefix, mode, spw='1', datacolumn='data'):
    tool = factory()
    assert tool.open(str(source))
    assert tool.set_selection(spw=spw, reindex=False)
    assert tool.fit_line(datacolumn=datacolumn, spw=spw, nfit='0',
                         tempoutfile=str(prefix.with_suffix('.fit.ms')), **MODES[mode])
    return tool


def check_time_edges(root, source, tools):
    beams = copy_rows(source, root / 'two-beams.ms', [0, 1])
    with opened(beams, True) as main:
        for row, value in enumerate((2., 10.)):
            main.putcell('TIME', row, main.getcell('TIME', 0))
            main.putcell('TIME_CENTROID', row, main.getcell('TIME', 0))
            main.putcell('ANTENNA1', row, 0)
            main.putcell('ANTENNA2', row, 0)
            main.putcell('FEED1', row, row)
            main.putcell('FEED2', row, row)
            main.putcell('DATA', row, np.full((2, 96), value, dtype=np.complex64))
            main.putcell('FLAG', row, np.zeros((2, 96), dtype=bool))
    with opened(beams / 'FEED', True) as feed:
        row = feed.nrows()
        feed.addrows(1)
        for name in feed.colnames():
            if feed.iscelldefined(name, 0):
                feed.putcell(name, row, 1 if name == 'FEED_ID' else feed.getcell(name, 0))
    floating = floating_data(root, beams)
    with opened(floating, True) as main:
        for row, value in enumerate((0., 10.)):
            elapsed = 2. * row
            main.putcell('TIME', row, main.getcell('TIME', 0) + elapsed)
            main.putcell('TIME_CENTROID', row, main.getcell('TIME', row))
            main.putcell('FEED1', row, 0)
            main.putcell('FEED2', row, 0)
            main.putcell('FLOAT_DATA', row, np.full((2, 96), value, dtype=np.float32))
            main.putcell('SIGMA', row, np.ones(2, dtype=np.float32))
            main.putcell('SIGMA_SPECTRUM', row, np.full((2, 96), 1. - .5 * row, dtype=np.float32))
    variable = copy_rows(beams, root / 'two-descriptions.ms', [0, 1])
    with opened(variable, True) as main:
        main.putcell('DATA_DESC_ID', 1, 0)
        main.putcell('FEED1', 1, 0)
        main.putcell('FEED2', 1, 0)
    params = write_parameters(root, 'chunk-row-zero', '0,0,,0,,false,,,,,poly,0,,',
                              '0,1,,0,,false,,,,,poly,0,,')
    for name, factory in tools.items():
        prefix = root / f'time-edges-{name}'
        tool = edge_tool(factory, beams, prefix, 'time')
        try:
            smooth, atm = (prefix.with_suffix(f'.{kind}.ms') for kind in ('smooth', 'atm'))
            assert tool.smooth(type='boxcar', width=1, outfile=str(smooth))
            assert tool.atmcor({}, outfile=str(atm))
            for output in (smooth, atm):
                with opened(output) as table:
                    assert table.nrows() == 2
                    np.testing.assert_array_equal(table.getcol('FEED1'), [0, 1])
                    np.testing.assert_allclose(table.getcol('DATA')[0, 0, :], [2., 10.], atol=1e-6)
        finally:
            assert tool.close()
        tool = edge_tool(factory, floating, prefix.with_name(prefix.name + '-float'),
                         'time', datacolumn='float_data')
        try:
            output = prefix.with_suffix('.float.ms')
            assert tool.smooth(type='boxcar', width=1, datacolumn='float_data', outfile=str(output))
            with opened(output) as table:
                assert table.nrows() == 1
                np.testing.assert_allclose(table.getcell('FLOAT_DATA', 0), 8., atol=2e-6)
        finally:
            assert tool.close()
        tool = edge_tool(factory, variable, prefix.with_name(prefix.name + '-variable'), 'time', '*')
        try:
            output = prefix.with_suffix('.variable.ms')
            assert tool.subtract_baseline_variable(spw='*', blparam=str(params), bloutput=',,',
                                                   outfile=str(output))
            with opened(output) as table:
                assert table.nrows() == 2
                assert not table.getcol('FLAG').any()
                np.testing.assert_allclose(table.getcol('DATA'), 0., atol=2e-6)
        finally:
            assert tool.close()


def check_flagged_statistics(root, source, tools):
    operand = copy_rows(source, root / 'averaging-flags.ms', [0, 1, 2])
    with opened(operand, True) as table:
        table.putcell('FLAG', 0, np.ones((2, 96), dtype=bool))
        for row in (1, 2):
            table.putcell('FLAG', row, np.zeros((2, 96), dtype=bool))
        table.putcell('FLAG_ROW', 2, True)
        weights = table.getcell('WEIGHT_SPECTRUM', 1)
        weights[:, 12] = 0.
        table.putcell('WEIGHT_SPECTRUM', 1, weights)
    for name, factory in tools.items():
        prefix = root / f'averaging-flags-{name}'
        tool = edge_tool(factory, operand, prefix, 'geometric')
        try:
            output = prefix.with_suffix('.ms')
            assert tool.atmcor({}, outfile=str(output))
            with opened(output) as table:
                assert table.nrows() == 3
                assert table.getcell('FLAG_ROW', 0)
                np.testing.assert_allclose(table.getcell('WEIGHT', 0), [5.], rtol=2e-6)
                np.testing.assert_allclose(table.getcell('SIGMA', 0), [1. / np.sqrt(5.)], rtol=2e-6)
                np.testing.assert_array_equal(table.getcell('WEIGHT', 1), [0.])
                assert np.isinf(table.getcell('SIGMA', 1)).all()
                assert np.isinf(table.getcell('SIGMA_SPECTRUM', 1)[0, 12])
                retained = name == 'native'
                assert table.getcell('FLAG_ROW', 2) == retained
                assert np.all(table.getcell('FLAG', 2)) == retained
        finally:
            assert tool.close()


def check_fixed_polarization(root, source, factory):
    operand = copy_rows(source, root / 'fixed-polarization.ms', [0, 1, 2, 3])
    pol = operand / 'POLARIZATION'
    with opened(pol, True) as table:
        for name, shape in (('CORR_TYPE', [2]), ('CORR_PRODUCT', [2, 2])):
            descriptor = table.getcoldesc(name)
            descriptor.update(shape=shape, option=4, dataManagerType='StandardStMan',
                              dataManagerGroup=f'Fixed{name}')
            values = table.getcol(name)
            table.removecols(name)
            table.addcols({name: descriptor})
            table.putcol(name, values)
            np.testing.assert_array_equal(table.getcoldesc(name)['shape'], shape)
    prefix = root / 'fixed-polarization-native'
    tool = edge_tool(factory, operand, prefix, 'stokes')
    try:
        output = prefix.with_suffix('.ms')
        assert tool.atmcor({}, outfile=str(output))
        check_product(output, 'stokes', False, 'native')
        check_projection(operand, output, 'stokes')
        for product in (prefix.with_suffix('.fit.ms'), output):
            with opened(product / 'POLARIZATION') as table:
                np.testing.assert_array_equal(table.getcell('CORR_TYPE', 0), [9, 12])
                np.testing.assert_array_equal(table.getcell('CORR_TYPE', 1), [1])
    finally:
        assert tool.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casatasks.version_string() == '6.7.6.14'
    tools = {'casa': singledishms}
    if not args.reference_only:
        from casars.tools import singledishms as native_tool

        tools['native'] = native_tool
    source, _, _ = line_map(root)
    spectra = timed_spectra(root, source)
    for mode in MODES:
        for reindex in (True, False):
            process(root, spectra, tools, mode, reindex)
    check_scalar_weights(root, spectra, tools)
    check_time_edges(root, spectra, tools)
    check_flagged_statistics(root, spectra, tools)
    if 'native' in tools:
        check_fixed_polarization(root, spectra, tools['native'])
    print('Single-dish operations preserve and clear persistent averaging as expected.', flush=True)


if __name__ == '__main__':
    main()
