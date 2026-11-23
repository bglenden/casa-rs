#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the direct FRINGE solver and its records with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import copy_ms, opened, source as baseline_source
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from fringefit_oracle import (
    change_reference_fixture, check_global, compare_tables, duplicate_spectral_window, source,
)


def solve_case(work, engines, vis, name, *, options=None, selection=None,
               legacy=False, corrdep=False, append=False, outcome='product'):
    parameters = {
        'type': 'FRINGE', 'refant': 'A0', 't': 'inf', 'minsnr': 2.,
        'delaywindow': [-250., 250.], 'ratewindow': [-2e-12, 2e-12],
        'paramactive': [True, True, True, False],
    } | (options or {})
    selection = {'field': 'FLUX', 'spw': '0'} | (selection or {})
    paths, records, activity = {}, {}, {}
    for engine, make_tool in engines.items():
        tool = make_tool()
        path = work/f'{name}-{engine}.cal'
        paths[engine] = path
        assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(**selection) and tool.setptmodel([1., 0., 0., 0.])
        assert tool.setcorrdepflags(corrdep)
        assert tool.setsolve(table=str(path), **parameters)
        if name == 'direct':
            refusal = result(tool.setsolve, table=str(path.with_suffix('.unused')),
                             **(parameters | {'refant': 'MISSING'}))
            assert refusal == (False, ('RuntimeError',
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
            assert tool.setsolve(table=str(path), append=True, **parameters) and tool.solve()
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
        # The A fixture leaves only float32 division residue, so the formal
        # non-reference SNR exceeds 5e9 and is sensitive to its rounding.
        tolerance = .006 if name == 'amplitude' else .001
        compare_tables(paths['casa'], paths['native'], snr_tolerance=tolerance)
        compare_readers(paths)
        if name == 'amplitude':
            for path in paths.values():
                with opened(path) as table:
                    assert (table.getcol('SNR')[0::4, :, 1:] > 5e9).all()
    print(name, 'activity', activity['casa'], flush=True)
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    work = parser.parse_args().work.resolve()
    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    vis, law = source(work)
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    for name, options in [
        ('direct', {}),
        ('zero-rates', {'zerorates': True}),
        ('phase', {'apmode': 'P'}),
        ('amplitude', {'apmode': 'A'}),
        ('fft-only', {'globalsolve': False}),
        ('no-refinement-iterations', {'niter': 0}),
        ('rate-disabled', {'paramactive': [True, True, False, False]}),
        ('phase-only-mask', {'paramactive': [True]}),
        ('delay-only-mask', {'paramactive': [False, True, False, False]}),
        ('signed-delay-window', {'delaywindow': [1., 110.], 'globalsolve': False}),
        ('extra-delay-window-value', {'delaywindow': [1., 110., 999.], 'globalsolve': False}),
        ('signed-rate-window', {'ratewindow': [2e-13, 1.2e-12], 'globalsolve': False}),
        ('extra-rate-window-value', {'ratewindow': [2e-13, 1.2e-12, 999.], 'globalsolve': False}),
        ('threshold', {'minsnr': 1e8}),
        ('alias', {'type': 'Fringe Jones'}),
        ('globalfringe-alias', {'type': 'GLOBALFRINGE'}),
    ]:
        paths = solve_case(work, engines, vis, name, options=options)
        if name in ('direct', 'zero-rates'):
            for path in paths.values():
                check_global(path, law, zero_rates=name == 'zero-rates')
    for combination in ('stokes', 'parallel'):
        paths = solve_case(work, engines, vis, combination, options={'corrcomb': combination}, corrdep=True)
        for path in paths.values():
            with opened(path) as table:
                values = table.getcol('FPARAM')
                np.testing.assert_array_equal(values[:4], values[4:])
    one_hand = work/'one-hand-flagged.ms'
    copy_ms(vis, one_hand)
    with opened(one_hand, True) as table:
        flags = table.getcol('FLAG')
        flags[1, ...] = True
        assert table.putcol('FLAG', flags)
    solve_case(work, engines, one_hand, 'stokes-no-pair', corrdep=True, outcome='empty',
               options={'corrcomb': 'stokes', 'paramactive': []})
    solve_case(work, engines, one_hand, 'parallel-one-hand', corrdep=True,
               options={'corrcomb': 'parallel'})
    cross_hands = baseline_source(work, 'RR RL LR LL', label='cross-hands')
    with opened(cross_hands, True) as table:
        flags = np.zeros_like(table.getcol('FLAG'))
        flags[[0, 3], ...] = True
        assert table.putcol('FLAG', flags)
        assert table.putcol('FLAG_ROW', flags.all(axis=(0, 1)))
    solve_case(work, engines, cross_hands, 'cross-hands-only', corrdep=True, outcome='error')
    solve_case(work, engines, vis, 'finite', options={'t': '47s'}, selection={'spw': '0:2~28'})
    switched = change_reference_fixture(work, vis)
    for mode in ('flex', 'strict'):
        paths = solve_case(work, engines, switched, f'reference-{mode}', corrdep=True,
                           options={'t': '47s', 'refant': 'A0,A1', 'refantmode': mode})
        for path in paths.values():
            with opened(path) as table:
                flagged = table.getcol('FLAG').reshape(8, 4, 5).all(axis=(0, 2))
                np.testing.assert_array_equal(flagged, [False, mode == 'strict', False, False])
                expected_references = [0, -1, 0, 0] if mode == 'strict' else [0, 1, 1, 0]
                np.testing.assert_array_equal(table.getcol('ANTENNA2')[::5], expected_references)
    solve_case(work, engines, vis, 'append', append=True)
    windows = duplicate_spectral_window(work, vis, law[1])
    for concat in (False, True):
        solve_case(work, engines, windows, f'windows-{concat}',
                   options={'combine': 'spw', 'concatspws': concat}, selection={'spw': '0,1'})
    solve_case(work, engines, vis, 'legacy-fit', legacy=True, outcome='error')
    spectral_weights = work/'zero-spectral-weights.ms'
    copy_ms(vis, spectral_weights)
    with opened(spectral_weights, True) as table:
        shape = table.getcol('FLAG').shape
        for key, value in [('SIGMA_SPECTRUM', np.inf), ('WEIGHT_SPECTRUM', 0.)]:
            if key not in table.colnames():
                assert table.addcols({key: {'valueType': 'float', 'ndim': 2}})
            assert table.putcol(key, np.full(shape, value, dtype=np.float32))
    solve_case(work, engines, spectral_weights, 'legacy-row-weights', legacy=True, outcome='error')
    for name, options in [('no-reference', {'refant': ''}), ('empty-mask', {'paramactive': []}),
                          ('single-integration', {'t': 'int'})]:
        solve_case(work, engines, vis, name, options=options, outcome='error')
    flagged = work/'fully-flagged.ms'
    copy_ms(vis, flagged)
    with opened(flagged, True) as table:
        for key in ('FLAG', 'FLAG_ROW'):
            assert table.putcol(key, np.ones_like(table.getcol(key)))
    for legacy in (False, True):
        solve_case(work, engines, flagged, f'flagged-{legacy}', legacy=legacy, outcome='empty')


if __name__ == '__main__':
    main()
