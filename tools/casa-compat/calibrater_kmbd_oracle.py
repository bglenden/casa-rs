#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check direct KMBD solve and consumption in CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
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
from gencal_switched_power_oracle import rebind_extensions, seed


def solve(work, vis, engines, legacy, combined):
    tables, records, activities = {}, {}, {}
    name = f'{legacy}-{combined}'
    spws = '0,1' if combined else '0'
    for label, factory in engines.items():
        output = tables[label] = work/f'{name}-{label}.cal'
        tool = factory()
        assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(field='FLUX', spw=spws) and tool.setptmodel([1, 0, 0, 0])
        alias = 'KMBD Jones' if combined else 'KMBD'
        assert tool.setsolve(alias, 'inf', str(output), refant='A0',
                             combine='spw' if combined else '')
        assert tool.solve() and output.exists()
        record = records[label] = tool.returndict()
        assert list(record.pop('solve_tables')) == [str(output)]
        activities[label] = tool.activityrec()
        assert tool.close()
        response = solutions(output)
        assert response['VisCal'] == 'KMBD Jones'
        np.testing.assert_array_equal(response['FLAG'], False)
        if not (combined and legacy):
            np.testing.assert_allclose(response['FPARAM'][:, 0], ANTENNA_DELAYS_NS,
                                       rtol=0., atol=2e-4)
    if 'native' in tables:
        same_record(records['native'], records['casa'])
        same_record(activities['native'], activities['casa'])
        reference, candidate = map(solutions, (tables['casa'], tables['native']))
        for col, want in reference.items():
            if col == 'FPARAM':
                np.testing.assert_allclose(candidate[col], want, atol=2e-4, rtol=0.)
            elif col == 'TIME':
                np.testing.assert_allclose(candidate[col], want, atol=1e-4, rtol=0.)
            else:
                np.testing.assert_array_equal(candidate[col], want, err_msg=col)
        compare_readers(tables)
        apply_tables(work, vis, engines, tables, legacy, combined)
    print('KMBD', legacy, combined, flush=True)


def apply_tables(work, vis, engines, tables, legacy, combined):
    for writer, prior in tables.items():
        outputs, outputs_activity = {}, {}
        for reader, factory in engines.items():
            dataset = work/f'{legacy}-{combined}-{writer}-{reader}.ms'
            copytree(vis, dataset)
            tool = factory()
            assert tool.setvi(legacy, True) and tool.open(str(dataset), addcorr=True, addmodel=False)
            assert tool.selectvis(field='FLUX', spw='0,1' if combined else '0')
            assert tool.setapply(table=str(prior), spwmap=[0, 0] if combined else [-1], calwt=False)
            assert tool.correct('calflag')
            outputs_activity[reader] = tool.activityrec()
            assert tool.close()
            with opened(dataset) as table:
                outputs[reader] = {col: table.getcol(col) for col in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
        same_record(outputs_activity['native'], outputs_activity['casa'])
        for col, value in outputs['casa'].items():
            np.testing.assert_allclose(outputs['native'][col], value, atol=3e-5, rtol=3e-5, err_msg=col)


def refused_factories(work, vis, engines):
    for legacy, factory in product((True, False), engines.values()):
        tool = factory()
        assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(field='FLUX')
        for kind in ('P', 'P Jones', 'EVLASWP', 'G EVLASWPOW'):
            assert result(tool.setsolve, kind, 'inf', str(work/'unused.cal')) == (
                False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
            assert tool.returndict()['solve_tables'].size == 0
        assert tool.close()
    print('setup refusals without required inputs', flush=True)


def switched_power_gather(work, engines):
    source, flagged = work/'switched-power.ms', work/'switched-power-flagged.ms'
    seed(source)
    copytree(source, flagged)
    rebind_extensions(flagged)
    with opened(flagged, True) as table:
        assert table.putcol('FLAG', np.ones_like(table.getcol('FLAG')))
        assert table.putcol('FLAG_ROW', np.ones_like(table.getcol('FLAG_ROW')))
    crashing = """
from casatools import calibrater
import sys
source, output, legacy = sys.argv[1:]
tool = calibrater()
assert tool.setvi(legacy == 'True', True) and tool.open(source, addcorr=False, addmodel=False)
assert tool.selectvis(field='FLUX') and tool.setptmodel([1, 0, 0, 0])
assert tool.setsolve('EVLASWP', 'inf', output, refant='A0', minblperant=2, minsnr=0.)
print('setup', flush=True)
tool.solve()
"""
    for legacy in (True, False):
        output = work/f'evlaswp-{legacy}.cal'
        pinned = run([sys.executable, '-c', crashing, str(source), str(output), str(legacy)],
                     capture_output=True, text=True, timeout=35, check=False)
        assert pinned.returncode == -signal.SIGSEGV and 'setup' in pinned.stdout, pinned
        assert not output.exists()
        records = {}
        for name, engine in engines.items():
            tool = engine()
            assert tool.setvi(legacy, True) and tool.open(str(flagged), addcorr=False, addmodel=False)
            assert tool.selectvis(field='FLUX') and tool.setptmodel([1, 0, 0, 0])
            family = 'EVLASWP' if legacy else 'G EVLASWPOW'
            assert tool.setsolve(family, 'inf', str(output), refant='A0', minblperant=2, minsnr=0.)
            assert tool.solve() and not output.exists()
            activity, returned = tool.activityrec(), tool.returndict()
            np.testing.assert_array_equal(activity['nExpected'], [1])
            np.testing.assert_array_equal(activity['nAttempt'], [0])
            assert ('solvestats' in returned) != legacy
            records[name] = {'activity': activity, 'returned': returned}
            assert tool.close()
        if 'native' in records:
            same_record(records['native'], records['casa'])
            tool = engines['native']()
            assert tool.setvi(legacy, True) and tool.open(str(source), addcorr=False, addmodel=False)
            assert tool.selectvis(field='FLUX') and tool.setptmodel([1, 0, 0, 0])
            assert tool.setsolve(family, 'inf', str(output), refant='A0', minblperant=2, minsnr=0.)
            assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
            assert tool.close() and not output.exists()
        print('EVLASWP', legacy, 'flagged gather and isolated usable fit', flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    engines = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native

        engines['native'] = native
    work.mkdir()
    with chdir(work):
        vis = work/'source.ms'
        delay_data(vis, 'K', second_width=250e6)
        for legacy, combined in product((True, False), (False, True)):
            solve(work, vis, engines, legacy, combined)
        refused_factories(work, vis, engines)
        switched_power_gather(work, engines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
