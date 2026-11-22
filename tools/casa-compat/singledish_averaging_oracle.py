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
from sdfit_oracle import copy_rows, line_map, timed_spectra
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
    print('Single-dish operations preserve and clear persistent averaging as expected.', flush=True)


if __name__ == '__main__':
    main()
