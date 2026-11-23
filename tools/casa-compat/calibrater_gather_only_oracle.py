#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check direct calibration families with an available gather stage."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree
from subprocess import run
import signal
import sys

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_state_oracle import casa_log, native_log
from record_casa_returns import make_cal_ms


FAMILIES = (
    ('J', 'J Jones', 4), ('Jf', 'Jf Jones', 4),
    ('N', 'A Noise', 4), ('TEC', 'F Jones', 1),
    ('Tsys', 'B TSYS', 2), ('GainCurve', 'EGainCurve', 8),
    ('PowerCurve', 'EPowerCurve', 8),
)


def set_up(tool, vis, legacy):
    assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
    assert tool.selectvis(field='FLUX') and tool.setptmodel([1, 0, 0, 0])


def empty_case(work, vis, engines, family, subtype, count, legacy):
    states, activities, statistics = {}, {}, {}
    for engine, factory in engines.items():
        tool = factory()
        set_up(tool, vis, legacy)
        location = work/f'{family}-{legacy}-{engine}.cal'
        setup = result(tool.setsolve, family, 'int', str(location), refant='A2',
                       minsnr=2., solnorm=True)
        capture = casa_log(work/f'{family}-{legacy}.log') if engine == 'casa' else native_log()
        with capture as log:
            ok, lines = log(tool.state)
        assert ok
        states[engine] = [line.replace(str(location), '<table>') for line in lines]
        if family == 'PowerCurve' and not legacy:
            assert setup == (False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
            assert list(tool.returndict()['solve_tables']) == []
        else:
            assert setup == (True, True) and states[engine][-1].startswith(f'.   {subtype}:')
            assert tool.solve()
            returned = tool.returndict()
            assert list(returned['solve_tables']) == [str(location)]
            activities[engine] = tool.activityrec()
            np.testing.assert_array_equal(activities[engine]['nExpected'], [20])
            np.testing.assert_array_equal(activities[engine]['nAttempt'], [0])
            np.testing.assert_array_equal(activities[engine]['nSucceed'], [0])
            assert ('solvestats' in returned) != legacy
            if not legacy:
                statistics[engine] = returned['solvestats']
                np.testing.assert_array_equal(statistics[engine]['spw0']['expected'], [20]*count)
        assert tool.close() and not location.exists()
    if 'native' in states:
        assert states['native'] == states['casa'], states
        if activities:
            same_record(activities['native'], activities['casa'])
        if statistics:
            same_record(statistics['native'], statistics['casa'])
    print('fully flagged', family, legacy, flush=True)


def useful_case(work, vis, engines, family, legacy):
    location = work/f'{family}-{legacy}-usable.cal'
    child = """
from casatools import calibrater
from calibrater_apply_tool_oracle import result
import sys
family, old, vis, table = sys.argv[1:]
t = calibrater()
assert t.setvi(old == 'True', True) and t.open(vis, addcorr=False, addmodel=False)
assert t.selectvis(field='FLUX') and t.setptmodel([1, 0, 0, 0])
setup = result(t.setsolve, family, 'int', table)
print('SETUP', setup, flush=True)
if setup == (True, True):
    print('SOLVE', result(t.solve), flush=True)
    assert t.returndict()['solve_tables'].size == 0
assert t.close()
"""
    pinned = run([sys.executable, '-c', child, family, str(legacy), str(vis), str(location)],
                 capture_output=True, text=True, check=False, timeout=40)
    failed_setup = family == 'PowerCurve' and not legacy
    if family == 'Tsys' or (legacy and family in ('J', 'Jf')):
        assert pinned.returncode == -signal.SIGSEGV and 'SETUP (True, True)' in pinned.stdout, pinned
    elif failed_setup:
        assert pinned.returncode == 0 and 'Error in Calibrater::setsolve.' in pinned.stdout, pinned
    else:
        assert pinned.returncode == 0 and 'Error in Calibrater::solve.' in pinned.stdout, pinned
    assert not location.exists()
    if 'native' in engines:
        tool = engines['native']()
        set_up(tool, vis, legacy)
        configured = result(tool.setsolve, family, 'int', str(location))
        if failed_setup:
            assert configured == (False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
        else:
            assert configured == (True, True), (family, legacy, configured)
            assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        assert tool.returndict()['solve_tables'].size == 0
        assert tool.close() and not location.exists()
    print('usable', family, legacy, 'isolated reference', pinned.returncode, flush=True)


def append_cases(work, vis, engines):
    missing, scalar, full = (work/f'append-{name}.cal' for name in ('missing', 'scalar', 'full'))
    casatasks.gencal(vis=str(vis), caltable=str(scalar), caltype='amp', parameter=[1.])
    copytree(scalar, full)
    with opened(full/'SPECTRAL_WINDOW', True) as target, opened(vis/'SPECTRAL_WINDOW') as source:
        for column in ('CHAN_FREQ', 'CHAN_WIDTH', 'NUM_CHAN', 'EFFECTIVE_BW', 'RESOLUTION'):
            assert target.putcell(column, 0, source.getcell(column, 0))
    for legacy, (family, target, modern_success) in product((False, True), (
        ('J', missing, False), ('J', scalar, True),
        ('Jf', missing, False), ('Jf', scalar, False), ('Jf', full, True),
        ('TOpac', missing, False), ('TfOpac', full, True),
    )):
        for factory in engines.values():
            tool = factory()
            set_up(tool, vis, legacy)
            assert tool.setsolve(family, 'int', str(target), append=True)
            actual = result(tool.solve)
            succeeded = legacy or modern_success
            expected = (True, True) if succeeded else (
                False, ('RuntimeError', 'Error in Calibrater::solve.'))
            assert actual == expected, (legacy, family, target, actual)
            assert list(tool.returndict()['solve_tables']) == ([str(target)] if succeeded else [])
            assert tool.close()
        assert not missing.exists()
        print('append', legacy, family, target.name, flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    engines = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native

        engines['native'] = native
    work.mkdir()
    with chdir(work):
        vis, flagged = work/'usable.ms', work/'flagged.ms'
        make_cal_ms(str(vis), stokes='RR RL LR LL', channels=8, scan_seconds=80,
                    integration_seconds=4.)
        with opened(vis, True) as data:
            assert data.putcol('FLAG', np.zeros_like(data.getcol('FLAG')))
            assert data.putcol('FLAG_ROW', np.zeros_like(data.getcol('FLAG_ROW')))
        copytree(vis, flagged)
        with opened(flagged, True) as data:
            assert data.putcol('FLAG', np.ones_like(data.getcol('FLAG')))
            assert data.putcol('FLAG_ROW', np.ones_like(data.getcol('FLAG_ROW')))
        for (family, subtype, count), legacy in product(FAMILIES, (False, True)):
            empty_case(work, flagged, engines, family, subtype, count, legacy)
            useful_case(work, vis, engines, family, legacy)
        append_cases(work, flagged, engines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
