#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct M and MF baseline Mueller solves with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater

from blcal_oracle import compare_tables, copy_ms, opened, source
from calibrater_apply_tool_oracle import same_record
from calibrater_solve_tool_oracle import compare_readers


def solve_case(work, tools, vis, name, options, *, legacy=False, selection=None,
               corrdep=False, append=False, empty=False):
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
        assert tool.solve()
        if append:
            assert tool.selectvis(**(selection | {'field': 'PHASE'}))
            assert tool.setptmodel([1., 0., 0., 0.])
            assert tool.setsolve(table=str(path), append=True, **options)
            assert tool.solve()
        assert path.is_dir() != empty, (name, label)
        records[label] = tool.returndict()
        activity[label] = tool.activityrec()
        assert list(records[label]['solve_tables']) == [str(path)]
        assert tool.close()
    same_record(activity['native'], activity['casa'])
    if legacy:
        assert not activity['casa']
        assert all('solvestats' not in record for record in records.values())
    else:
        same_record(records['native']['solvestats'], records['casa']['solvestats'])
    if not empty:
        error = compare_tables(paths['casa'], paths['native'])
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
    solve_case(work, tools, vis, 'mf-append', {'type': 'MF'}, append=True)
    solve_case(work, tools, flagged, 'mf-flagged', {}, empty=True)
    solve_case(work, tools, vis, 'm-legacy', {'type': 'M'}, legacy=True)
    solve_case(work, tools, vis, 'mf-legacy', {'type': 'MF'}, legacy=True)


if __name__ == '__main__':
    main()
