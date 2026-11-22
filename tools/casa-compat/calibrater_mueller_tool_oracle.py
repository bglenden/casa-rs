#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct M and MF baseline Mueller solves with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import compare_tables, copy_ms, opened, source, weighted_windows
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers


def solve_case(work, tools, vis, name, options, *, legacy=False, selection=None,
               corrdep=False, append=False, outcome='product'):
    paths, records, activity = {}, {}, {}
    selection = {'field': 'FLUX', 'spw': '0'} | (selection or {})
    for label, make_tool in tools.items():
        tool = make_tool()
        path = work / f'{name}-{label}.cal'
        paths[label] = path
        assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(**selection) and tool.setptmodel([1., 0., 0., 0.])
        assert tool.setcorrdepflags(corrdep)
        assert tool.setsolve(table=str(path), **options)
        if name in ('mf-default', 'm-legacy'):
            failure = result(tool.setsolve, table=str(path.with_suffix('.unused')),
                             refant='MISSING', **options)
            assert failure == (False, ('RuntimeError',
                                       'Antenna Expression: No match found for token(s) "MISSING"  '))
            assert list(tool.returndict()['solve_tables']) == [str(path)]
        solved = result(tool.solve)
        if outcome == 'error':
            assert solved == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        else:
            assert solved == (True, True), (name, label, solved)
        if append:
            assert tool.selectvis(**(selection | {'field': 'PHASE'}))
            assert tool.setptmodel([1., 0., 0., 0.])
            assert tool.setsolve(table=str(path), append=True, **options)
            assert tool.solve()
        assert path.is_dir() == (outcome == 'product'), (name, label)
        records[label] = tool.returndict()
        activity[label] = tool.activityrec()
        assert list(records[label]['solve_tables']) == ([] if outcome == 'error' else [str(path)])
        assert tool.close()
    same_record(activity['native'], activity['casa'])
    if legacy:
        assert not activity['casa']
        assert all('solvestats' not in record for record in records.values())
    else:
        same_record(records['native']['solvestats'], records['casa']['solvestats'])
    if outcome == 'product':
        # Legacy normalization also scales its flagged unity placeholders.
        flagged_rtol = 2e-6 if legacy and options.get('solnorm') else 0.
        error = compare_tables(paths['casa'], paths['native'], flagged_rtol=flagged_rtol)
        compare_readers(paths)
        print(name, 'solution NRMS', error, flush=True)
    print(name, 'activity', activity['casa'], flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    work = args.work.resolve()
    work.mkdir()
    vis = source(work)
    flagged = work/'flagged.ms'
    copy_ms(vis, flagged)
    with opened(flagged, True) as table:
        flags = table.getcol('FLAG')
        flags[:] = True
        assert table.putcol('FLAG', flags)
    tools = {'casa': casa_calibrater, 'native': native_calibrater}
    solve_case(work, tools, vis, 'mf-default', {})
    solve_case(work, tools, vis, 'm-narrow', {'type': 'M', 't': '20s'},
               selection={'spw': '0:1~12', 'msselect': 'ANTENNA1 != ANTENNA2'})
    solve_case(work, tools, vis, 'mf-binned', {'type': 'MF', 't': 'inf,2ch', 'preavg': 8.},
               corrdep=True, selection={'spw': '0:1~12'})
    solve_case(work, tools, vis, 'mf-auto', {'type': 'MF'},
               selection={'msselect': 'ANTENNA1 == ANTENNA2'})
    solve_case(work, tools, vis, 'm-amp', {'type': 'M', 'apmode': 'A', 'solnorm': True})
    solve_case(work, tools, vis, 'mf-phase', {'type': 'MF', 'apmode': 'P'})
    outliers = work/'outliers.ms'
    copy_ms(vis, outliers)
    with opened(outliers, True) as table:
        values = table.getcol('DATA')
        values[:, 7, :] *= 7
        rows = (table.getcol('ANTENNA1') == 0) & (table.getcol('ANTENNA2') == 1)
        values[..., rows] *= 7
        assert table.putcol('DATA', values)
    for family in ('M', 'MF'):
        options = {'type': family, 'solnorm': True, 'normtype': 'median'}
        solve_case(work, tools, outliers, f'{family.lower()}-median', options)
        solve_case(work, tools, outliers, f'{family.lower()}-legacy-median', options, legacy=True)
    solve_case(work, tools, vis, 'mf-append', {'type': 'MF'}, append=True)
    solve_case(work, tools, flagged, 'mf-flagged', {}, outcome='empty')
    solve_case(work, tools, vis, 'm-legacy', {'type': 'M'}, legacy=True)
    solve_case(work, tools, vis, 'm-legacy-narrow', {'type': 'M'}, legacy=True,
               selection={'spw': '0:1~12'})
    solve_case(work, tools, vis, 'mf-legacy', {'type': 'MF'}, legacy=True,
               selection={'spw': '0:1~12'})
    solve_case(work, tools, vis, 'mf-legacy-gap', {}, legacy=True,
               selection={'spw': '0:1~2;5~6'})
    solve_case(work, tools, vis, 'mf-legacy-stride', {}, legacy=True,
               selection={'spw': '0:1~9^2'})
    changing_flags = work/'changing-flags.ms'
    copy_ms(vis, changing_flags)
    with opened(changing_flags, True) as table:
        values = table.getcol('DATA')
        flags = np.zeros_like(table.getcol('FLAG'))
        times = table.getcol('TIME')
        field = table.getcol('FIELD_ID') == 0
        integrations = np.unique(times[field])
        flags[:, 7, field & (times >= integrations[len(integrations)//2])] = True
        for column, array in [('DATA', np.ones_like(values)), ('FLAG', flags),
                              ('FLAG_ROW', np.zeros_like(field))]:
            assert table.putcol(column, array)
    solve_case(work, tools, changing_flags, 'mf-legacy-changing-flags', {}, legacy=True)
    for engine in tools:
        with opened(work/f'mf-legacy-changing-flags-{engine}.cal') as table:
            np.testing.assert_allclose(table.getcol('CPARAM')[:, 7, :], 0.5, rtol=1e-6, atol=0)
            assert not table.getcol('FLAG')[:, 7, :].any()
    for family in ('M', 'MF'):
        solve_case(work, tools, vis, f'{family.lower()}-legacy-preavg',
                   {'type': family, 'preavg': 8.}, legacy=True, outcome='error')
        solve_case(work, tools, vis, f'{family.lower()}-legacy-preavg-zero',
                   {'type': family, 'preavg': 0.}, legacy=True, outcome='error')
    solve_case(work, tools, flagged, 'mf-legacy-flagged', {}, legacy=True, outcome='empty')
    solve_case(work, tools, vis, 'mf-legacy-binned', {'t': 'inf,2ch'}, legacy=True,
               selection={'spw': '0:1~12'}, outcome='error')
    windows = weighted_windows(work/'legacy-windows', vis)
    with opened(windows/'SPECTRAL_WINDOW', True) as table:
        for column, factor in (('EFFECTIVE_BW', 2.), ('RESOLUTION', 3.), ('CHAN_WIDTH', 4.)):
            assert table.putcell(column, 1, table.getcell(column, 1) * factor)
    solve_case(work, tools, windows, 'm-legacy-combined',
               {'type': 'M', 'combine': 'spw,scan,obs'}, legacy=True, selection={'spw': '0,1'})
    solve_case(work, tools, windows, 'mf-legacy-unselected-window', {}, legacy=True)


if __name__ == '__main__':
    main()
