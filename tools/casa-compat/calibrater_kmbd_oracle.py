#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check direct KMBD solve and consumption in CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from gaincal_delay_oracle import ANTENNA_DELAYS_NS, delay_data, solutions


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
    print('apply-only setup refusals', flush=True)


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
