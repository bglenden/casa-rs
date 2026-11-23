#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Test direct calibrater solve configuration against CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path

import casatasks
from casatools import calibrater
import numpy as np

from blcal_oracle import copy_ms, nrms, opened
from gaincal_delay_oracle import delay_data
from gaincal_oracle import amplitude_data
from record_casa_returns import make_cal_ms
from calibrater_apply_tool_oracle import compare_calls, result, same_record


CASES = {
    'gain': {'type': 'G'},
    'phase': {'type': 'G', 't': 'int', 'phaseonly': True, 'apmode': 'a'},
    'interval': {'type': 'G', 't': 20., 'preavg': 8.},
    'scalar': {'type': 'T', 't': '1min', 'apmode': 'a', 'solnorm': True, 'normtype': 'median'},
    'robust': {'type': 'G', 'solmode': 'R'},
    'robust-lowercase': {'type': 'T', 'solmode': 'r', 'rmsthresh': [7., 5.]},
    'corrdep': {'type': 'G'},
    'bandpass': {'type': 'B', 't': 'inf,2ch', 'fillgaps': 2},
    'bandpass-phase': {'type': 'B', 'phaseonly': True, 'apmode': 'A'},
    'bandpass-phase-norm': {'type': 'B', 'apmode': 'P', 'solnorm': True},
    'bandpass-amplitude': {'type': 'B', 'apmode': 'A'},
    'bandpass-median': {'type': 'B', 'solnorm': True, 'normtype': 'median'},
    'bandpass-preaverage': {'type': 'B', 't': '20s', 'preavg': 8.},
    'delay': {'type': 'K'},
    'threshold': {'type': 'G', 'minblperant': 99},
}


def compare_solutions(paths, *, formal_tolerance=1e-3):
    reference, actual = paths['casa'], paths['native']
    with opened(reference) as left, opened(actual) as right:
        assert left.info() == right.info()
        native_metadata = {'ARRAY_ID', 'TIME_EXTRA_PREC'}
        assert left.colnames() == [name for name in right.colnames() if name not in native_metadata]
        for key in ('VisCal', 'ParType', 'PolBasis'):
            assert left.getkeyword(key) == right.getkeyword(key)
        keys = ('ANTENNA1', 'TIME', 'SCAN_NUMBER', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'OBSERVATION_ID')
        expected_order, actual_order = [np.lexsort([table.getcol(key) for key in keys])
                                       for table in (left, right)]
        for key in keys:
            np.testing.assert_allclose(right.getcol(key)[actual_order], left.getcol(key)[expected_order],
                                       rtol=0, atol=1e-4, err_msg=key)
        flag = left.getcol('FLAG')[..., expected_order]
        np.testing.assert_array_equal(right.getcol('FLAG')[..., actual_order], flag)
        column = 'CPARAM' if 'CPARAM' in left.colnames() else 'FPARAM'
        for key in (column, 'PARAMERR', 'SNR'):
            expected = left.getcol(key)[..., expected_order]
            candidate = right.getcol(key)[..., actual_order]
            if np.count_nonzero(expected[~flag]):
                error = nrms(candidate[~flag], expected[~flag])
                tolerance = 1e-3 if key == column else formal_tolerance
                assert error < tolerance, (actual.name, key, error)
            else:
                np.testing.assert_allclose(candidate[~flag], expected[~flag], atol=2e-6, rtol=0.)


def compare_readers(paths):
    from casars.tools import table as native_table

    for path in paths.values():
        native = native_table()
        assert native.open(str(path))
        try:
            with opened(path) as casa:
                assert casa.info() == native.info()
                for key in ('VisCal', 'ParType', 'MSName', 'PolBasis'):
                    assert casa.getkeyword(key) == native.getkeyword(key)
                parameters = 'CPARAM' if 'CPARAM' in casa.colnames() else 'FPARAM'
                for name in ('TIME', 'FLAG', 'SNR', 'PARAMERR', parameters):
                    np.testing.assert_array_equal(native.getcol(name), casa.getcol(name))
        finally:
            assert native.close()


def solve_case(work, vis, label, legacy, engines, options, *, legacy_recovery=False, spw='0:2~9'):
    products, records, activities = {}, {}, {}
    for name, tool in engines.items():
        path = work/f'{label}-{legacy}-{name}.ms'
        copy_ms(vis, path)
        if label == 'corrdep':
            with opened(path, True) as table:
                a, b = (table.getcol(name) for name in ('ANTENNA1', 'ANTENNA2'))
                flags = table.getcol('FLAG')
                flags[0, :, (a == 1) & (b == 3)] = True
                assert table.putcol('FLAG', flags)
        if label == 'bandpass-median':
            with opened(path, True) as table:
                data = table.getcol('DATA')
                data[:, 4, :] *= 3.
                assert table.putcol('DATA', data)
        old = legacy and (not legacy_recovery or name == 'native')
        assert tool.setvi(old, True) and tool.open(str(path))
        assert tool.selectvis(field='FLUX', spw=spw)
        assert tool.setptmodel([1, 0, 0, 0])
        assert tool.setcorrdepflags(label == 'corrdep')
        products[name] = product = work/f'{label}-{legacy}-{name}.cal'
        arguments = {'refant': ['A0'], 'minblperant': 3} | options
        assert tool.setsolve(table=str(product), **arguments)
        configured = tool.returndict()
        assert configured['solve_tables'].tolist() == [str(product)]
        assert tool.solve()
        activities[name] = tool.activityrec()
        records[name] = value = tool.returndict()
        assert tool.reset()
        assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        same_record(tool.activityrec(), activities[name])
        if 'solvestats' in value:
            same_record(tool.returndict()['solvestats'], value['solvestats'])
        assert tool.open(str(path))
        if 'solvestats' in value:
            same_record(tool.returndict()['solvestats'], value['solvestats'])
        assert tool.close() and tool.activityrec() == {}
    if 'native' in engines:
        same_record(activities['native'], activities['casa'])
        if legacy_recovery:
            assert 'solvestats' not in records['native']
            assert 'solvestats' in records['casa']
        else:
            assert tuple(records['native']) == tuple(records['casa'])
            if 'solvestats' in records['casa']:
                same_record(records['native']['solvestats'], records['casa']['solvestats'])
        assert products['native'].exists() == products['casa'].exists()
        if products['casa'].exists():
            compare_solutions(products)
            compare_readers(products)
    print('direct solve', label, 'VI1' if legacy else 'VI2', activities['casa'], flush=True)


def solve_with_registered_prior(work, vis, engines):
    from calibration_leakage_apply_oracle import make_table

    prior = work/'prior.cal'
    with opened(vis) as table:
        epoch = float(table.getcol('TIME').min())
    make_table(vis, prior, 'G Jones', np.full((5, 2), 1.2, np.complex64), epoch)
    products = {}
    for name, tool in engines.items():
        path = work/f'append-{name}.ms'
        copy_ms(vis, path)
        products[name] = product = work/f'append-{name}.cal'
        assert tool.open(str(path))
        for append, field in ((False, 'FLUX'), (True, 'PHASE')):
            assert tool.selectvis(field=field, spw='0:2~9')
            assert tool.setptmodel([1, 0, 0, 0])
            assert tool.setapply(table=str(prior))
            for family in ('K', 'KCROSS'):
                assert result(tool.setsolve, type=family, table=str(product)) == (
                    False, ('RuntimeError', 'Error in Calibrater::setsolve.')
                )
                assert tool.returndict()['apply_tables'].tolist() == [str(prior)]
            setup = {'type': 'G', 'table': str(product), 'append': append, 'refantmode': 'strict',
                     'minblperant': 3, 'refant': 'A0'}
            assert tool.setsolve(**setup)
            assert result(tool.setsolve, **setup, t='invalid') == (
                False, ('RuntimeError', 'Error in Calibrater::setsolve.')
            )
            config = tool.returndict()
            assert config['apply_tables'].tolist() == [str(prior)]
            assert config['solve_tables'].size == 0
            assert tool.setsolve(**setup) and tool.solve()
            assert tool.returndict()['apply_tables'].tolist() == [str(prior)]
            assert tool.reset(False, True)
            assert tool.returndict()['apply_tables'].tolist() == [str(prior)]
        assert tool.close()
        with opened(product) as table:
            np.testing.assert_array_equal(np.unique(table.getcol('FIELD_ID')), [0, 1])
    if 'native' in engines:
        compare_solutions(products)
        compare_readers(products)
    print('direct solve reused the selected model and prior, then appended the second field', flush=True)


def solve_with_autocorrelations(work, engines):
    fixture = work/'autocorrelation-input'
    fixture.mkdir()
    vis = fixture/'source.ms'
    make_cal_ms(str(vis), autocorrelation_weight=1.)
    vis = amplitude_data(fixture, vis)
    with opened(vis) as table:
        auto = table.getcol('ANTENNA1') == table.getcol('ANTENNA2')
        assert auto.any() and not auto.all()
    zero_vis = fixture/'zero-autocorrelations.ms'
    copy_ms(vis, zero_vis)
    with opened(zero_vis, True) as table:
        data = table.getcol('DATA')
        data[..., auto] = 0.
        assert table.putcol('DATA', data)

    cases = [(mode or 'ordinary', vis, {'solmode': mode}) for mode in ('', 'R', 'L1', 'L1R')]
    cases.append(('zero-phase', zero_vis, {'apmode': 'P'}))
    for label, source, options in cases:
        products, counts, stats = {}, {}, {}
        for engine, tool in engines.items():
            product = work/f'autocorr-{label}-{engine}.cal'
            products[engine] = product
            assert tool.open(str(source), addcorr=False, addmodel=False)
            assert tool.selectvis(field='FLUX', spw='0:1~6')
            assert tool.setptmodel([1, 0, 0, 0])
            assert tool.setsolve(type='G', table=str(product), refant='A0',
                                 minblperant=3, **options)
            assert tool.solve()
            counts[engine] = tool.activityrec()
            stats[engine] = tool.returndict()['solvestats']
            assert tool.close()
        if label == 'ordinary':
            with opened(products['casa']) as table:
                assert not table.getcol('FLAG').any()
                snr = table.getcol('SNR')
                cutoff = float(snr.max()) * 1.25
                mixed = float(snr.max() + snr.min()) / 2.
            cases.extend([('all-flagged', vis, {'minsnr': cutoff}),
                          ('mixed-flags', vis, {'minsnr': mixed})])
        if 'native' in engines:
            same_record(counts['native'], counts['casa'])
            same_record(stats['native'], stats['casa'])
            tolerance = 5e-3 if options.get('solmode') else 1e-3
            compare_solutions(products, formal_tolerance=tolerance)
            with opened(products['casa']) as reference, opened(products['native']) as actual:
                flags = reference.getcol('FLAG')
                if label == 'all-flagged':
                    assert flags.all()
                if label == 'mixed-flags':
                    assert flags.any() and not flags.all()
                for column in ('PARAMERR', 'SNR'):
                    error = nrms(actual.getcol(column), reference.getcol(column))
                    assert error < tolerance, (label, column, error)
            compare_readers(products)
        print('direct G matched selected autocorrelation residuals', label, flush=True)

    ordinary_t_and_b_autocorrelations(work, vis, zero_vis, engines)
    for label, source in (('autocorr-only', vis), ('zero-phase-only', zero_vis)):
        auto_only = {'msselect': 'ANTENNA1==ANTENNA2'}
        mode = 'P' if label == 'zero-phase-only' else 'AP'
        result_records = gather_counts_without_product(work, source, label, engines,
                                                       auto_only, apmode=mode, minblperant=3)
        np.testing.assert_array_equal(result_records[0]['nAttempt'], [1])
        np.testing.assert_array_equal(result_records[0]['nSucceed'], [0])
        print('direct G reports an attempt without a caltable for', label, flush=True)


def ordinary_t_and_b_autocorrelations(work, vis, zero_vis, engines):
    second_hand = work/'autocorr-second-hand.ms'
    copy_ms(vis, second_hand)
    with opened(second_hand, True) as table:
        flags = table.getcol('FLAG')
        flags[0] = True
        assert table.putcol('FLAG', flags)
    high_auto = work/'autocorr-high-reference.ms'
    copy_ms(vis, high_auto)
    with opened(high_auto, True) as table:
        rows = (table.getcol('ANTENNA1') == 1) & (table.getcol('ANTENNA2') == 1)
        values = table.getcol('DATA')
        values[..., rows] *= 100
        assert table.putcol('DATA', values)
    cases = (
        ('scalar', 'T', vis, False, 'AP', 'A0'),
        ('scalar-second-hand', 'T', second_hand, True, 'AP', 'A0'),
        ('scalar-high-reference-auto', 'T', high_auto, False, 'AP', 'A1'),
        ('channelized', 'B', vis, False, 'AP', 'A0'),
        ('channelized-phase', 'B', zero_vis, False, 'P', 'A0'),
    )
    for case, family, source, corrdep, apmode, refant in cases:
        products, stats, activity = {}, {}, {}
        for label, tool in engines.items():
            product = work/f'autocorr-{case}-{label}.cal'
            products[label] = product
            assert tool.open(str(source), addcorr=False, addmodel=False)
            assert tool.selectvis(field='FLUX', spw='0:1~6')
            assert tool.setptmodel([1, 0, 0, 0])
            assert tool.setcorrdepflags(corrdep)
            assert tool.setsolve(type=family, table=str(product), refant=refant,
                                 minblperant=3, apmode=apmode)
            assert tool.solve()
            activity[label] = tool.activityrec()
            stats[label] = tool.returndict()['solvestats']
            assert tool.close()
        if 'native' in engines:
            same_record(activity['native'], activity['casa'])
            same_record(stats['native'], stats['casa'])
            compare_solutions(products)
            compare_readers(products)
        print('direct', family, 'matched selected autocorrelation residuals', case, flush=True)


def gather_counts_without_product(work, vis, label, engines, selection, **solve):
    counts, stats = {}, {}
    for engine, tool in engines.items():
        product = work/f'{label}-{engine}.cal'
        assert tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(field='FLUX', **selection) and tool.setcorrdepflags(True)
        assert tool.setptmodel([1., .1, .2, 0.])
        assert tool.setsolve(type='G', table=str(product), refant='A0', **solve)
        assert tool.solve()
        counts[engine] = tool.activityrec()
        stats[engine] = tool.returndict()['solvestats']
        assert tool.close() and not product.exists()
    if 'native' in engines:
        same_record(counts['native'], counts['casa'])
        same_record(stats['native'], stats['casa'])
    return counts['casa'], stats['casa']


def direct_gain_gather_counts(work, engines):
    cross_hands = work/'cross-hands-only.ms'
    make_cal_ms(str(cross_hands), stokes='XX XY YX YY', autocorrelation_weight=1.)
    with opened(cross_hands, True) as table:
        flags = table.getcol('FLAG')
        flags[0] = True
        flags[3] = True
        assert table.putcol('FLAG', flags)
        assert table.putcol('FLAG_ROW', np.zeros(table.nrows(), bool))
    counts, stats = gather_counts_without_product(
        work, cross_hands, 'cross-hands-only', engines, {'msselect': 'ANTENNA1==ANTENNA2'},
        minblperant=3,
    )
    np.testing.assert_array_equal(counts['nAttempt'], [1])
    np.testing.assert_array_equal(counts['nSucceed'], [0])
    np.testing.assert_array_equal(stats['data_unflagged'], [0, 0])

    scalar = work/'one-correlation.ms'
    make_cal_ms(str(scalar), stokes='XX', autocorrelation_weight=1.)
    counts, stats = gather_counts_without_product(
        work, scalar, 'one-correlation', engines, {}, minblperant=99,
    )
    np.testing.assert_array_equal(counts['nAttempt'], [1])
    np.testing.assert_array_equal(stats['data_unflagged'], [1, 1])
    for value in stats['spw0'].values():
        if isinstance(value, dict):
            np.testing.assert_array_equal(value['data_unflagged'], [1, 1])
    print('direct G matched cross-hand attempt and single-correlation data counts', flush=True)


def min_snr_product(factory, vis, table, gain, mode, minimum):
    tool = factory()
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        assert tool.selectvis(field='0') and tool.setptmodel([1., 0., 0., 0.])
        assert tool.setsolve(gain, 'inf', str(table), apmode=mode, minblperant=2, minsnr=minimum)
        assert tool.solve() and table.exists()
    finally:
        assert tool.close()
    with opened(table) as cal:
        return {key: cal.getcol(key) for key in ('CPARAM', 'FLAG', 'SNR')}


def min_snr_payload(work, factories):
    vis = work/'gain.ms'
    with chdir(work):
        make_cal_ms(str(vis))
        for gain, mode in product(('G', 'T'), ('AP', 'A', 'P')):
            uncut = min_snr_product(factories['casa'], vis, work/f'{gain}-{mode}-uncut.cal',
                                    gain, mode, 0.)
            present = ~uncut['FLAG']
            ordered = np.unique(uncut['SNR'][present])
            middle = len(ordered)//2
            threshold = (float(ordered[middle-1]) + float(ordered[middle]))/2
            paths = {engine: work/f'{gain}-{mode}-cut-{engine}.cal' for engine in factories}
            results = {engine: min_snr_product(factory, vis, paths[engine], gain, mode, threshold)
                       for engine, factory in factories.items()}
            cut = results['casa']
            flagged = present & cut['FLAG']
            assert 0 < np.count_nonzero(flagged) < np.count_nonzero(present)
            expected = uncut['CPARAM'][flagged] if mode == 'AP' else 1.
            np.testing.assert_array_equal(cut['CPARAM'][flagged], expected)
            if 'native' in results:
                compare_solutions(paths)
                compare_readers(paths)
                np.testing.assert_allclose(results['native']['CPARAM'][flagged], expected,
                                           rtol=1e-3, atol=1e-6)
            print(gain, mode, 'minsnr payload checked', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--only', choices=tuple(CASES))
    parser.add_argument('--legacy', action='store_true')
    parser.add_argument('--min-snr-payload', action='store_true')
    config = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    work = config.work.resolve()
    work.mkdir()
    factories = {'casa': calibrater}
    if not config.reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    if config.min_snr_payload:
        min_snr_payload(work, factories)
        return
    vis = work/'source.ms'
    make_cal_ms(str(vis), channels=16, scan_seconds=80, integration_seconds=4)
    vis = amplitude_data(work, vis)
    engines = {name: factory() for name, factory in factories.items()}
    assert compare_calls(engines, 'solve') == (True, False)
    assert compare_calls(engines, 'setcorrdepflags', False) == (True, True)
    assert compare_calls(engines, 'setcorrdepflags', 1) == (
        False, ('TypeError', "in method 'calibrater_setcorrdepflags', argument 2 of type 'bool'")
    )
    for label, options in CASES.items():
        if config.only is None or config.only == label:
            engines = {name: factory() for name, factory in factories.items()}
            if config.legacy and label != 'threshold' and config.only is None:
                continue
            solve_case(work, vis, label, config.legacy, engines, options)
    if not config.legacy and config.only is None and 'native' in factories:
        engines = {name: factory() for name, factory in factories.items()}
        solve_case(work, vis, 'legacy-gain-recovery', True, engines, CASES['gain'],
                   legacy_recovery=True)
    if not config.legacy and config.only is None:
        engines = {name: factory() for name, factory in factories.items()}
        solve_with_registered_prior(work, vis, engines)
        cross_vis = work/'cross-delay.ms'
        delay_data(cross_vis, 'KCROSS')
        engines = {name: factory() for name, factory in factories.items()}
        solve_case(work, cross_vis, 'cross-delay', False, engines, {'type': 'KCROSS'}, spw='0')
        engines = {name: factory() for name, factory in factories.items()}
        solve_with_autocorrelations(work, engines)
        direct_gain_gather_counts(work, engines)


if __name__ == '__main__':
    main()
