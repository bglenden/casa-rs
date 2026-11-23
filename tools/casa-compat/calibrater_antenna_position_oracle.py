#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the generic KAntPos solve against CASA and planted antenna delays."""

import argparse
from contextlib import chdir
from pathlib import Path
from shutil import copytree
import signal
from subprocess import run
import sys

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from gaincal_delay_oracle import ANTENNA_DELAYS_NS, delay_data, solutions


def configure(tool, vis, legacy, spw):
    assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
    assert tool.selectvis(field='FLUX', spw=spw) and tool.setptmodel([1, 0, 0, 0])


def solve_case(work, vis, engines, family, legacy, reference, interval, *, flagged=False):
    tables, records, activities = {}, {}, {}
    for label, factory in engines.items():
        target = work/f'{family.split()[0]}-{legacy}-{reference}-{interval}-{flagged}-{label}.cal'
        tables[label] = target
        tool = factory()
        configure(tool, vis, legacy, '0')
        assert result(tool.setsolve, family, interval, str(target)) == (
            False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
        assert tool.setsolve(family, interval, str(target), refant=f'A{reference}')
        assert tool.solve()
        returned = tool.returndict()
        assert list(returned.pop('solve_tables')) == [str(target)]
        records[label] = returned
        activity = activities[label] = tool.activityrec()
        assert ('solvestats' in returned) != legacy
        if not legacy:
            assert returned['solvestats']['expected'].shape == (3,)
        assert tool.close()
        assert target.exists() != flagged
        if flagged:
            np.testing.assert_array_equal(activity['nAttempt'], [0, 0])
            continue
        values = solutions(target)
        assert values['VisCal'] == 'KAntPos Jones'
        antennas = values['ANTENNA1']
        planted = ANTENNA_DELAYS_NS[:, antennas] - ANTENNA_DELAYS_NS[:, [reference]]
        np.testing.assert_allclose(values['FPARAM'][:2, 0], planted, rtol=0., atol=2e-4)
        np.testing.assert_array_equal(values['FPARAM'][2, 0], 0.)
        np.testing.assert_array_equal(values['FLAG'][:2, 0], False)
        np.testing.assert_array_equal(values['FLAG'][2, 0], antennas != reference)
    if 'native' in tables:
        same_record(records['native'], records['casa'])
        same_record(activities['native'], activities['casa'])
        if not flagged:
            expected, actual = map(solutions, (tables['casa'], tables['native']))
            for key in expected:
                if key == 'FPARAM':
                    np.testing.assert_allclose(actual[key], expected[key], rtol=0., atol=2e-4)
                elif key == 'TIME':
                    np.testing.assert_allclose(actual[key], expected[key], rtol=0., atol=1e-4)
                else:
                    np.testing.assert_array_equal(actual[key], expected[key], err_msg=key)
            compare_readers(tables)
    print(family, legacy, reference, interval, flagged, flush=True)


def combined_error(work, vis, engines, legacy):
    path = work/f'combined-{legacy}.cal'
    child = """
from casatools import calibrater
import sys
vis, path, legacy = sys.argv[1:]
t = calibrater()
assert t.setvi(legacy == 'True', True) and t.open(vis, addcorr=False, addmodel=False)
assert t.selectvis(field='FLUX', spw='0,1') and t.setptmodel([1, 0, 0, 0])
assert t.setsolve('KAntPos', 'inf', path, refant='A0', combine='spw')
print('setup', flush=True)
t.solve()
"""
    pinned = run([sys.executable, '-c', child, str(vis), str(path), str(legacy)],
                 capture_output=True, text=True, check=False, timeout=40)
    assert pinned.returncode == -signal.SIGABRT and 'validateConformance' in pinned.stderr, pinned
    assert not path.exists()
    if 'native' in engines:
        tool = engines['native']()
        configure(tool, vis, legacy, '0,1')
        assert tool.setsolve('KAntPos', 'inf', str(path), refant='A0', combine='spw')
        assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        assert not tool.returndict()['solve_tables'].size
        assert tool.close() and not path.exists()
    print('combined', legacy, 'isolated reference', pinned.returncode, flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    engines = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native

        engines['native'] = native
    work.mkdir()
    with chdir(work):
        vis, flagged = work/'source.ms', work/'flagged.ms'
        delay_data(vis, 'K', second_width=250e6)
        copytree(vis, flagged)
        with opened(flagged, True) as data:
            assert data.putcol('FLAG', np.ones_like(data.getcol('FLAG')))
            assert data.putcol('FLAG_ROW', np.ones_like(data.getcol('FLAG_ROW')))
        for legacy in (True, False):
            solve_case(work, vis, engines, 'KAntPos', legacy, 0, 'inf')
            solve_case(work, vis, engines, 'KAntPos Jones', legacy, 2, 'int')
            solve_case(work, flagged, engines, 'KAntPos', legacy, 0, 'int', flagged=True)
            combined_error(work, vis, engines, legacy)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
