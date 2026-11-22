#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the direct Tf Jones solve with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers, compare_solutions
from calibrater_state_oracle import casa_log, native_log
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def solve(factory, capture, vis, product, selection, interval, options, *, product_expected):
    tool = factory()
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        assert tool.selectvis(field='0', spw=selection)
        assert tool.setptmodel([1., 0., 0., 0.])
        kind = options.get('type', 'Tf')
        kwargs = {'refant': 'A0', 'minblperant': 3, 'minsnr': 0.} | options
        kwargs.pop('type', None)
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
        for name in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION',
                     'REF_FREQUENCY', 'TOTAL_BANDWIDTH', 'FLAG_ROW'):
            np.testing.assert_array_equal(actual.getcell(name, 0), expected.getcell(name, 0),
                                          err_msg=name)


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


if __name__ == '__main__':
    main()
