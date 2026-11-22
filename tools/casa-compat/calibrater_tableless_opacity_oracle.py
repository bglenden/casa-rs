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


def configure_opacity(tool, capture, kind, expected, **arguments):
    outcome, logs = capture(lambda: result(tool.setapply, type=kind, **arguments))
    assert outcome == expected
    warning = [
        'Invoking opacity application without a caltable (e.g.,',
        'via opacity=[...] in calibration tasks) will soon be disabled.',
        'Please begin using gencal to generate an opacity caltable,',
        'and then supply that table in the standard manner.',
    ]
    assert [line.strip() for line in logs if line.strip() in warning] == warning


def exercise(factory, capture, vis, output, kind, legacy, operation):
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis))
    try:
        assert tool.selectvis(field='0') and tool.setptmodel([1.6, 0., 0., 0.])
        assert tool.setsolve('G', 'inf', str(output), refant='A0')
        for arguments in ({}, {'opacity': []}, {'opacity': [-.12]}, {'opacity': [0.]}):
            configure_opacity(tool, capture, kind,
                              (False, ('RuntimeError', 'Error in Calibrater::setapply.')),
                              **arguments)
        configure_opacity(tool, capture, kind, (True, True), opacity=.1234567, calwt=True)
        configure_opacity(tool, capture, kind, (True, True), opacity=[-.1, .2], calwt=True)
        configure_opacity(tool, capture, kind, (True, True), opacity=[.2, None, .4])
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


def solve_after_success(factory, vis, product, output, kind):
    tool = factory()
    assert tool.open(str(vis)) and tool.selectvis(field='0')
    try:
        assert tool.setptmodel([1.6, 0., 0., 0.])
        assert tool.setsolve('G', 'inf', str(product), refant='A0') and tool.solve()
        assert 'solvestats' in tool.returndict()
        activity = tool.activityrec()
        assert 'nSucceed' in activity
        assert tool.setsolve('G', 'inf', str(output), refant='A0')
        assert tool.setapply(type=kind, opacity=.1)
        assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        record = tool.returndict()
        assert 'solvestats' not in record and len(record['solve_tables']) == 0
        assert not output.exists()
        same_record(tool.activityrec(), activity)
    finally:
        assert tool.close()


def uncovered_operation(factory, capture, vis, prior, output, legacy, operation):
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis))
    try:
        assert tool.selectvis(spw='0,1' if operation == 'mixed' else '1')
        assert tool.setapply(table=str(prior)) and tool.setapply(type='TOpac', opacity=.2)
        original = columns(vis)
        if operation == 'solve':
            assert tool.setptmodel([1.6, 0., 0., 0.])
            assert tool.setsolve('G', 'inf', str(output), refant='A0')
            assert tool.solve() and not output.exists()
        elif operation == 'corrupt':
            assert tool.corrupt() is False
        else:
            expected = ((False, ('RuntimeError', 'Error in Calibrater::correct.'))
                        if operation == 'mixed' else (True, False))
            assert result(tool.correct, 'calflagstrict') == expected
            if operation == 'correct':
                with opened(vis) as table:
                    spw = table.getcol('DATA_DESC_ID')
                original['FLAG'][..., spw == 1] = True
        same_record(columns(vis), original)
        return capture(tool.state), tool.activityrec()
    finally:
        assert tool.close()


def missing_coverage(work, vis, native_calibrater, output, prior):
    for legacy in (False, True):
        paths = {name: work/f'coverage-{legacy}-{name}.ms' for name in ('casa', 'native')}
        for path in paths.values():
            copy_ms(vis, path)
            assert duplicate_rows(casa_table(), path/'DATA_DESCRIPTION') == 1
            with opened(path/'DATA_DESCRIPTION', True) as data:
                assert data.putcell('SPECTRAL_WINDOW_ID', 1, 1)
            with opened(path, True) as data:
                assert data.putcol('DATA_DESC_ID', data.getcol('FIELD_ID'))
        for operation in ('mixed', 'corrupt', 'solve', 'correct'):
            with casa_log(work/f'coverage-{operation}-{legacy}.log') as capture:
                wanted = uncovered_operation(casa_calibrater, capture, paths['casa'], prior,
                                              output, legacy, operation)
            with native_log() as capture:
                got = uncovered_operation(native_calibrater, capture, paths['native'], prior,
                                           output, legacy, operation)
            assert got[0] == wanted[0], (legacy, operation, got[0], wanted[0])
            same_record(got[1], wanted[1])
            print(legacy, operation, 'missing coverage matched', flush=True)


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
        for kind in ('TOpac', 'TfOpac'):
            for name, factory in (('casa', casa_calibrater), ('native', native_calibrater)):
                solve_after_success(factory, products[name], work/f'prior-{kind}-{name}.cal',
                                    output, kind)
            print(kind, 'prior statistics and activity matched', flush=True)
        missing_coverage(work, vis, native_calibrater, output, work/'prior-TOpac-casa.cal')


if __name__ == '__main__':
    main()
