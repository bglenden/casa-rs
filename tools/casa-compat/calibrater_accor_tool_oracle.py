#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct ACCOR solves with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from accor_oracle import compare
from blcal_oracle import copy_ms, nrms, opened, source
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers


def solve_case(work, engines, vis, name, *, selection=None, options=None,
               corrdep=False, legacy=False, append=False, outcome='product'):
    paths, records, activity = {}, {}, {}
    selection = {'field': 'FLUX', 'spw': '0'} | (selection or {})
    options = {'type': 'ACCOR'} | (options or {})
    for engine, make_tool in engines.items():
        tool = make_tool()
        path = work / f'{name}-{engine}.cal'
        paths[engine] = path
        assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(**selection) and tool.setptmodel([1., 0., 0., 0.])
        assert tool.setcorrdepflags(corrdep) and tool.setsolve(table=str(path), **options)
        if name == 'mixed':
            failure = result(tool.setsolve, type='ACCOR', table=str(path.with_suffix('.unused')),
                             refant='MISSING')
            assert failure == (False, ('RuntimeError',
                                       'Antenna Expression: No match found for token(s) "MISSING"  '))
            assert list(tool.returndict()['solve_tables']) == [str(path)]
        solved = result(tool.solve)
        if outcome == 'error':
            assert solved == (False, ('RuntimeError', 'Error in Calibrater::solve.')), (name, engine, solved)
        else:
            assert solved == (True, True), (name, engine, solved)
        if append:
            assert tool.selectvis(**(selection | {'field': 'PHASE'}))
            assert tool.setptmodel([1., 0., 0., 0.])
            assert tool.setsolve(table=str(path), append=True, **options) and tool.solve()
        assert path.is_dir() == (outcome == 'product'), (name, engine)
        records[engine], activity[engine] = tool.returndict(), tool.activityrec()
        assert list(records[engine]['solve_tables']) == ([] if outcome == 'error' else [str(path)])
        assert tool.close()
    same_record(activity['native'], activity['casa'])
    if legacy or outcome == 'error':
        assert all('solvestats' not in record for record in records.values())
    else:
        same_record(records['native']['solvestats'], records['casa']['solvestats'])
    if outcome == 'product':
        compare(paths['casa'], paths['native'])
        compare_readers(paths)
        with opened(paths['casa']) as reference, opened(paths['native']) as actual:
            error = nrms(actual.getcol('CPARAM'), reference.getcol('CPARAM'))
        print(name, 'solution NRMS', error, flush=True)
    print(name, 'activity', activity['casa'], flush=True)
    return paths


def change_flags(work, vis, label, update):
    output = work/f'{label}.ms'
    copy_ms(vis, output)
    with opened(output, True) as table:
        flags = table.getcol('FLAG')
        first, second = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2')]
        update(flags, first, second)
        assert table.putcol('FLAG', flags) and table.putcol('FLAG_ROW', flags.all(axis=(0, 1)))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    work = parser.parse_args().work.resolve()
    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    vis = source(work)
    scalar = source(work, 'RR')
    full = source(work, 'RR RL LR LL')
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    solve_case(work, engines, vis, 'mixed')
    solve_case(work, engines, vis, 'auto-only', selection={'msselect': 'ANTENNA1 == ANTENNA2'})
    cross = solve_case(work, engines, vis, 'cross-only', options={'refant': 'A0'},
                       selection={'msselect': 'ANTENNA1 != ANTENNA2'})
    for path in cross.values():
        with opened(path) as table:
            assert table.nrows() == 5 and table.getcol('FLAG').all()
            np.testing.assert_array_equal(table.getcol('CPARAM'), 1.)
            np.testing.assert_array_equal(table.getcol('ANTENNA2'), -1)
    solve_case(work, engines, scalar, 'scalar', options={'t': 'int'})
    for mode in ('A', 'P'):
        solve_case(work, engines, scalar, f'scalar-{mode}', options={'apmode': mode})
    solve_case(work, engines, vis, 'selected', selection={'spw': '0:2~9'},
               options={'t': '20s', 'preavg': 8.})
    solve_case(work, engines, vis, 'amp-median',
               options={'apmode': 'A', 'solnorm': True, 'normtype': 'median'})
    solve_case(work, engines, vis, 'phase', options={'apmode': 'P'})
    solve_case(work, engines, vis, 'append', append=True)

    varying = work/'varying-autocorrelations.ms'
    copy_ms(vis, varying)
    with opened(varying, True) as table:
        first, second, times = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2', 'TIME')]
        auto = first == second
        elapsed = times - times.min()
        data = table.getcol('DATA')
        data[..., auto] = np.where(elapsed < 8, 4, np.where(elapsed < 16, 9, 16))[auto]
        for key in ('DATA', 'CORRECTED_DATA'):
            assert table.putcol(key, data)
    for mode, expected_gain in [('AP', 2.), ('P', 1.)]:
        paths = solve_case(work, engines, varying, f'varying-{mode}',
                           options={'preavg': 8., 'apmode': mode})
        for path in paths.values():
            with opened(path) as table:
                np.testing.assert_array_equal(table.getcol('CPARAM'), expected_gain)

    with opened(full, True) as table:
        timestamps = table.getcol('TIME')
        weight = np.ones_like(table.getcol('WEIGHT'))
        weight[1:3, timestamps - timestamps.min() >= 40.] = 9.
        assert table.putcol('WEIGHT', weight)
    solve_case(work, engines, full, 'cross-hand-time-weights',
               selection={'msselect': 'ANTENNA1 == 0 && ANTENNA2 == 0'})

    def flag_auto_reference(flags, first, second):
        flags[..., (first == second) & (first == 0)] = True
        flags[1, :5, (first == second) & (first == 2)] = True

    reference = change_flags(work, vis, 'missing-reference', flag_auto_reference)
    for dependent in (False, True):
        solve_case(work, engines, reference, f'corrdep-{dependent}', corrdep=dependent)
    for mode, expected in [('flex', 2), ('strict', -1)]:
        paths = solve_case(work, engines, reference, f'ref-{mode}',
                           options={'refant': 'A0,A2', 'refantmode': mode})
        for path in paths.values():
            with opened(path) as table:
                np.testing.assert_array_equal(table.getcol('ANTENNA2'), expected)
                assert table.getcol('FLAG').all() == (mode == 'strict')
    solve_case(work, engines, reference, 'ref-strict-amplitude',
               options={'refant': 'A0', 'refantmode': 'strict', 'apmode': 'A'})
    flagged = change_flags(work, vis, 'all-flagged', lambda flags, *_: flags.fill(True))
    for legacy in (False, True):
        solve_case(work, engines, flagged, f'fully-flagged-{legacy}', legacy=legacy, outcome='empty')
        solve_case(work, engines, flagged, f'flagged-invalid-mode-{legacy}',
                   options={'apmode': 'bogus'}, legacy=legacy, outcome='empty')
    solve_case(work, engines, vis, 'fit-invalid-mode', options={'apmode': 'bogus'}, outcome='error')
    solve_case(work, engines, vis, 'legacy-fit', legacy=True, outcome='error')

    with opened(flagged, True) as table:
        times = table.getcol('TIME')
        start, second, third = np.unique(times)[:3]
        adjusted = times.copy()
        adjusted[times == second] = start + 15.
        adjusted[times == third] = start + 20.
        adjusted[times > third] += 100.
        for key in ('TIME', 'TIME_CENTROID'):
            assert table.putcol(key, adjusted)
    for legacy in (False, True):
        solve_case(work, engines, flagged, f'flagged-irregular-times-{legacy}',
                   options={'t': '10s'}, selection={'msselect': f'TIME <= {start + 20.:.9f}'},
                   legacy=legacy, outcome='empty')


if __name__ == '__main__':
    main()
