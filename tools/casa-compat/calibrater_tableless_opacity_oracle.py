#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA's retained TOpac and TfOpac vector-only lifecycle."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
from casatools import table as casa_table

from applycal_oracle import copy_ms
from calibrater_apply_tool_oracle import result, same_record
from calibrater_state_oracle import casa_log, native_log
from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from polcal_leakage_solve_oracle import plant


def columns(vis):
    names = ('CORRECTED_DATA', 'MODEL_DATA', 'FLAG', 'WEIGHT', 'SIGMA')
    with opened(vis) as table:
        return {name: table.getcol(name) for name in names}


def exercise(factory, capture, vis, output, kind, legacy, operation):
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis))
    try:
        assert tool.selectvis(field='0') and tool.setptmodel([1.6, 0., 0., 0.])
        assert tool.setsolve('G', 'inf', str(output), refant='A0')
        for arguments in ({}, {'opacity': []}, {'opacity': [-.12]}, {'opacity': [0.]}):
            assert result(tool.setapply, type=kind, **arguments) == (
                False, ('RuntimeError', 'Error in Calibrater::setapply.'))
        assert tool.setapply(type=kind, opacity=.1234567, calwt=True)
        assert tool.setapply(type=kind, opacity=[-.1, .2], calwt=True)
        before = capture(tool.state)
        assert list(tool.returndict()['apply_tables']) == []
        original = columns(vis)
        method, arguments, origin = operation
        if method == 'corrupt' and not legacy:
            origin += '2'
        assert result(getattr(tool, method), *arguments) == (
            False, ('RuntimeError', f'Error in Calibrater::{origin}.'))
        assert not output.exists() and tool.activityrec() == {}
        assert list(tool.returndict()['solve_tables']) == ([] if method == 'solve' else [str(output)])
        same_record(columns(vis), original)
        return before, capture(tool.state)
    finally:
        assert tool.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    from casars.tools import calibrater as native_calibrater

    work = args.work.resolve()
    work.mkdir()
    with chdir(work):
        vis, _, _, _, _ = plant(work, 'RR RL LR LL')
        assert duplicate_rows(casa_table(), vis/'SPECTRAL_WINDOW') == 1
        products = {engine: work/f'{engine}.ms' for engine in ('casa', 'native')}
        for path in products.values():
            copy_ms(vis, path)
        output = work/'unused.cal'
        operations = [('correct', (), 'correct'), ('correct', ('trial',), 'correct'),
                      ('corrupt', (), 'corrupt'), ('solve', (), 'solve')]
        for legacy in (False, True):
            for kind in ('TOpac', 'TfOpac'):
                for method, arguments, origin in operations:
                    operation = (method, arguments, origin)
                    label = f'{kind}-{legacy}-{method}-{arguments}'
                    with casa_log(work/f'{method}-{legacy}-{kind}-{len(arguments)}.log') as capture:
                        expected = exercise(casa_calibrater, capture, products['casa'], output,
                                            kind, legacy, operation)
                    with native_log() as capture:
                        actual = exercise(native_calibrater, capture, products['native'], output,
                                          kind, legacy, operation)
                    assert actual == expected, (label, actual, expected)
                    print(label, 'matched', flush=True)


if __name__ == '__main__':
    main()
