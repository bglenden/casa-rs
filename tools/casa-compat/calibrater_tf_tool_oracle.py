#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the direct Tf Jones solve with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater, table as casa_table
import numpy as np

from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers, compare_solutions
from calibrater_state_oracle import casa_log, native_log
from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms


def solve(factory, capture, vis, product, selection, interval, options, *, product_expected):
    tool = factory()
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        kwargs = {'refant': 'A0', 'minblperant': 3, 'minsnr': 0.} | options
        assert tool.selectvis(field=kwargs.pop('field', '0'), spw=selection)
        assert tool.setptmodel([1., 0., 0., 0.])
        kind = kwargs.pop('type', 'Tf')
        assert tool.setsolve(kind, interval, str(product), **kwargs)
        state = capture(tool.state)
        assert list(tool.returndict()['solve_tables']) == [str(product)]
        solved = result(tool.solve)
        if product_expected:
            assert solved == (True, True) and product.exists()
            return state, tool.activityrec(), tool.returndict()['solvestats']
        assert solved == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        assert not product.exists() and list(tool.returndict()['solve_tables']) == []
        return state, tool.activityrec(), None
    finally:
        assert tool.close()


def compare_window(products):
    with opened(products['casa']/'SPECTRAL_WINDOW') as expected, \
            opened(products['native']/'SPECTRAL_WINDOW') as actual:
        assert actual.nrows() == expected.nrows()
        for row in range(expected.nrows()):
            for name in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION',
                         'REF_FREQUENCY', 'TOTAL_BANDWIDTH', 'FLAG_ROW'):
                np.testing.assert_array_equal(actual.getcell(name, row), expected.getcell(name, row),
                                              err_msg=f'SPW {row} {name}')


def compare_case(work, vis, factory, label, selection, interval, product_expected, options):
    products = {engine: work/f'{label}-{engine}.cal' for engine in ('casa', 'native')}
    with casa_log(work/f'{label}-state.log') as capture:
        expected = solve(casa_calibrater, capture, vis, products['casa'], selection, interval,
                         options, product_expected=product_expected)
    with native_log() as capture:
        actual = solve(factory, capture, vis, products['native'], selection, interval,
                       options, product_expected=product_expected)
    # State describes the configured output path, which is different for each engine.
    actual_state = [line.replace(str(products['native']), str(products['casa']))
                    for line in actual[0][1]]
    assert (actual[0][0], actual_state) == expected[0], (label, actual_state, expected[0])
    same_record(actual[1], expected[1])
    same_record(actual[2], expected[2])
    if product_expected:
        compare_solutions(products)
        compare_readers(products)
        compare_window(products)
    print(label, 'matched', flush=True)


def second_window(original, path):
    copytree(original, path)
    assert duplicate_rows(casa_table(), path/'SPECTRAL_WINDOW') == 1
    assert duplicate_rows(casa_table(), path/'DATA_DESCRIPTION') == 1
    with opened(path/'SPECTRAL_WINDOW', True) as table:
        for column in ('CHAN_FREQ', 'REF_FREQUENCY'):
            assert table.putcell(column, 1, table.getcell(column, 0) + 10e9)
    with opened(path/'DATA_DESCRIPTION', True) as table:
        assert table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(path, True) as table:
        assert table.putcol('DATA_DESC_ID', table.getcol('FIELD_ID'))


def compare_application(work, vis, factories):
    wanted = {}
    columns = ('CORRECTED_DATA', 'FLAG', 'WEIGHT')
    for writer, reader, weight in product(factories, factories, (False, True)):
        path = work/f'apply-{writer}-{reader}-{weight}.ms'
        copytree(vis, path)
        tool = factories[reader]()
        assert tool.open(str(path))
        try:
            assert tool.selectvis(field='0', spw='0:3')
            assert tool.setapply(table=str(work/f'channel-{writer}.cal'), calwt=weight)
            assert tool.correct('calflag')
        finally:
            assert tool.close()
        with opened(path) as table:
            cells = {name: table.getcol(name) for name in columns}
            field = table.getcol('FIELD_ID')
            assert np.max(np.abs(cells['CORRECTED_DATA'][:, 3, field == 0])) > .9
        if writer == reader == 'casa':
            wanted[weight] = cells
        np.testing.assert_array_equal(cells['FLAG'], wanted[weight]['FLAG'])
        for name in ('CORRECTED_DATA', 'WEIGHT'):
            np.testing.assert_allclose(cells[name], wanted[weight][name], rtol=5e-5,
                                       atol=2e-6, err_msg=name)
        print(writer, reader, weight, 'correct matched', flush=True)


def compare_append(work, vis, factories):
    products = {engine: work/f'append-{engine}.cal' for engine in factories}
    for engine, factory in factories.items():
        capture = casa_log(work/'append-state.log') if engine == 'casa' else native_log()
        with capture as logger:
            solve(factory, logger, vis, products[engine], '0:3', 'inf', {}, product_expected=True)
            solve(factory, logger, vis, products[engine], '0:3', 'inf',
                  {'field': '1', 'append': True}, product_expected=True)
        with opened(products[engine]) as table:
            np.testing.assert_array_equal(np.unique(table.getcol('FIELD_ID')), [0, 1])
    compare_solutions(products)
    compare_readers(products)
    compare_window(products)
    print('append matched', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    from casars.tools import calibrater as native_calibrater

    work = args.work.resolve()
    work.mkdir()
    cases = (
        ('channel', '0:3', 'inf', True, {}),
        ('channel-interval', '0:3', '20s', True, {'type': 'Tf Jones', 'apmode': 'P'}),
        ('averaged-window', '0', 'inf,8chan', True, {}),
        ('averaged-bandwidth', '0', 'inf,8MHz', True, {}),
        ('averaged-selection', '0:2~5', 'inf,4chan', True, {}),
        ('two-channels', '0:2~3', 'inf', False, {}),
        ('two-bins', '0', 'inf,4chan', False, {}),
    )
    with chdir(work):
        vis = work/'input.ms'
        make_cal_ms(str(vis))
        for case in cases:
            compare_case(work, vis, native_calibrater, *case)
        twin = work/'two-windows.ms'
        second_window(vis, twin)
        compare_case(work, twin, native_calibrater, 'unselected-window', '0:3', 'inf', True,
                     {'type': 'Tf Noise'})
        compare_case(work, twin, native_calibrater, 'combined-windows', '0:3,1:3', 'inf', True,
                     {'field': '0,1', 'combine': 'spw'})
        factories = {'casa': casa_calibrater, 'native': native_calibrater}
        compare_application(work, vis, factories)
        compare_append(work, vis, factories)


if __name__ == '__main__':
    main()
