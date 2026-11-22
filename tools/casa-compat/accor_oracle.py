#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native and CASA sampler normalization on known autocorrelations."""

import argparse
from contextlib import chdir
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import time

import casatasks as casa
from casatools import table as casa_table
import numpy as np

from calibration_smoothcal_oracle import opened
from fringefit_oracle import compare_consumers, copy_selected_rows
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms

GAIN = np.array([[1.1, 1.2, 1.3, 1.4, 1.5], [1.9, 1.8, 1.7, 1.6, 1.5]])


def source(root, stokes='RR LL'):
    products = stokes.split()
    path = root / {1: 'one-hand.ms', 2: 'autocorrelations.ms', 4: 'four-hands.ms'}[len(products)]
    make_cal_ms(str(path), channels=32, scan_seconds=160, integration_seconds=2,
                stokes=stokes, frequency='8.4GHz', autocorrelation_weight=1.)
    with opened(path / 'STATE', True) as table:
        if table.nrows() < 2:
            table.addrows(2 - table.nrows())
        for row, mode in enumerate(('CALIBRATE_FLUX#ON_SOURCE', 'CALIBRATE_PHASE#ON_SOURCE')):
            table.putcell('OBS_MODE', row, mode)
    with opened(path, True) as table:
        table.putcol('STATE_ID', table.getcol('FIELD_ID'))
        first, second, timestamps = (table.getcol(name) for name in ('ANTENNA1', 'ANTENNA2', 'TIME'))
        channel_factor = 1. + .01 * np.arange(32)
        temporal_factor = np.where(timestamps - timestamps.min() < 60., 1., 1.6)
        hand = {'R': 0, 'L': 1}
        power = np.array([GAIN[hand[corr[0]], first] * GAIN[hand[corr[1]], second]
                          for corr in products]) * temporal_factor
        values = (channel_factor[None, :, None] * power[:, None, :]).astype(np.complex64)
        for name in ('DATA', 'CORRECTED_DATA'):
            table.putcol(name, values)
        table.putcol('MODEL_DATA', np.ones_like(values))
        flag = np.zeros(values.shape, dtype=bool)
        if stokes == 'RR LL':
            flag[1, :9, (first == second) & (first == 2)] = True
        flag[..., (first == second) & (first == 4)] = True
        table.putcol('FLAG', flag)
        table.putcol('FLAG_ROW', flag.all(axis=(0, 1)))
        print(f'{path.name}, {len(first)} rows and {(first == second).sum()} autocorrelations', flush=True)
    return path


def solve(root, vis, task, name, **options):
    output = root / f'{name}.cal'
    start = time.perf_counter()
    assert task(str(vis), str(output), **(dict(field='FLUX') | options)) is None
    assert output.is_dir()
    print(name, f'{time.perf_counter() - start:.3f}s', flush=True)
    return output


def compare(reference, actual):
    with opened(reference) as left, opened(actual) as right:
        assert left.info() == right.info()
        assert sorted(left.colnames()) == sorted(right.colnames())
        assert left.nrows() == right.nrows()
        for name in left.colnames():
            assert [left.iscelldefined(name, row) for row in range(left.nrows())] == [
                right.iscelldefined(name, row) for row in range(right.nrows())], name
            if not left.iscelldefined(name, 0):
                continue
            lhs, rhs = left.getcol(name), right.getcol(name)
            if name in ('CPARAM', 'TIME'):
                tolerance = 3e-6 if name == 'CPARAM' else 1e-5
                np.testing.assert_allclose(rhs, lhs, rtol=0., atol=tolerance, err_msg=name)
            else:
                np.testing.assert_array_equal(rhs, lhs, err_msg=name)
    with opened(reference / 'SPECTRAL_WINDOW') as left, opened(actual / 'SPECTRAL_WINDOW') as right:
        for name in ('CHAN_FREQ', 'REF_FREQUENCY', 'CHAN_WIDTH', 'FLAG_ROW', 'NUM_CHAN'):
            np.testing.assert_allclose(right.getcol(name), left.getcol(name), atol=.01, rtol=0.,
                                       err_msg=name)


def check_analytic(product, dependent):
    channels = 1. + .01 * np.arange(32)
    means = np.full((2, 5), channels.mean())
    means[:, 2] = channels[9:].mean()
    if dependent:
        means[0, 2] = channels.mean()
    gains = GAIN * np.sqrt(1.375 * means)
    gains[:, 4] = 1.
    with opened(product) as table:
        assert table.info()['subType'] == 'Accor Jones'
        assert table.getkeyword('ParType') == 'Complex'
        assert not table.iscelldefined('WEIGHT', 0)
        np.testing.assert_array_equal(table.getcol('ANTENNA1'), np.arange(5))
        np.testing.assert_array_equal(table.getcol('ANTENNA2'), -np.ones(5, int))
        np.testing.assert_array_equal(table.getcol('INTERVAL'), np.zeros(5))
        np.testing.assert_array_equal(table.getcol('SNR'), np.zeros((2, 1, 5)))
        np.testing.assert_array_equal(table.getcol('PARAMERR'), np.zeros((2, 1, 5)))
        np.testing.assert_array_equal(table.getcol('FLAG')[:, 0], np.broadcast_to(np.arange(5) == 4,
                                                                                (2, 5)))
        np.testing.assert_allclose(table.getcol('CPARAM')[:, 0], gains, atol=3e-6, rtol=0.)


def run(root, vis, solvers, appliers):
    with opened(vis) as table:
        timestamp = table.getcol('TIME')[0]
        scan = str(table.getcell('SCAN_NUMBER', 0))
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    bounds = [epoch + timedelta(seconds=timestamp + seconds) for seconds in (4., 59.)]
    timerange = '~'.join(bound.strftime('%Y/%m/%d/%H:%M:%S.%f') for bound in bounds)
    cases = [
        ('infinite', {}),
        ('dependent', dict(corrdepflags=True)),
        ('per-integration', dict(solint='int')),
        ('finite', dict(solint=60)),
        ('channels', dict(spw='0:6~16')),
        ('antenna', dict(antenna='A2&&A2')),
        ('time-selectors', dict(timerange=timerange, scan=scan, observation=0)),
        ('intent', dict(field='', intent='CALIBRATE_FLUX*')),
        ('ignored-selectors', dict(selectdata=False, scan='999', msselect='ANTENNA1<3')),
    ]
    for label, options in cases:
        products = {key: solve(root, vis, task, f'{label}-{key}', **options)
                    for key, task in solvers.items()}
        if 'native' in products:
            compare(products['casa'], products['native'])
        if label in ('infinite', 'dependent'):
            for product in products.values():
                check_analytic(product, label == 'dependent')
        if label in ('infinite', 'per-integration'):
            compare_consumers(root, vis, products, appliers, label)


def two_windows(root, vis):
    output = root / 'two-windows.ms'
    shutil.copytree(vis, output)
    for path in (output / 'SPECTRAL_WINDOW', output / 'DATA_DESCRIPTION', output):
        count = duplicate_rows(casa_table(), path)
    with opened(output / 'DATA_DESCRIPTION', True) as table:
        table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(output / 'SPECTRAL_WINDOW', True) as table:
        for name in ('CHAN_FREQ', 'REF_FREQUENCY'):
            table.putcell(name, 1, table.getcell(name, 0) + 48e6)
    with opened(output, True) as table:
        table.putcol('DATA_DESC_ID', np.ones(count, dtype=np.int32), count, count)
        for name in ('DATA', 'CORRECTED_DATA'):
            table.putcol(name, 2.25 * table.getcol(name, count, count), count, count)
    return output


def constant_spectra(vis, output, powers, correlation_codes=None):
    shutil.copytree(vis, output)
    if correlation_codes is not None:
        with opened(output / 'POLARIZATION', True) as table:
            table.putcell('CORR_TYPE', 0, np.array(correlation_codes, dtype=np.int32))
            table.putcell('CORR_PRODUCT', 0, np.array([[1, 0], [1, 0]], dtype=np.int32)
                          if len(correlation_codes) == 2 else np.zeros((2, 1), dtype=np.int32))
    with opened(output, True) as table:
        values = np.broadcast_to(np.array(powers, dtype=np.complex64)[:, None, None],
                                 table.getcol('DATA').shape).copy()
        for name in ('DATA', 'CORRECTED_DATA'):
            table.putcol(name, values)
        table.putcol('FLAG', np.zeros(values.shape, dtype=bool))
        table.putcol('FLAG_ROW', np.zeros(values.shape[-1], dtype=bool))
    return output


def varying_prior(root, vis, solvers):
    varied = root / 'varying-prior.ms'
    with opened(vis) as table:
        times = table.getcol('TIME')
        earliest = times.min()
    copy_selected_rows(vis, varied, (times == earliest) | (times == earliest + 10.))
    with opened(varied, True) as table:
        data = np.full_like(table.getcol('DATA'), 4.)
        for name in ('DATA', 'CORRECTED_DATA'):
            table.putcol(name, data)
        table.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        table.putcol('FLAG_ROW', np.zeros(data.shape[-1], dtype=bool))
    prior = solve(root, varied, solvers['casa'], 'varying-input', solint='int')
    with opened(prior, True) as table:
        gains = table.getcol('CPARAM')
        gains[:] = np.where(table.getcol('TIME') == earliest, .5, 2.)
        table.putcol('CPARAM', gains)
    for calwt in (False, True):
        library = root / f'varying-{calwt}.callib'
        library.write_text(f"caltable='{prior}' tinterp='nearest' calwt={calwt}\n")
        outputs = {key: solve(root, varied, task, f'varying-{calwt}-{key}',
                              docallib=True, callib=str(library))
                   for key, task in solvers.items()}
        for output in outputs.values():
            with opened(output) as table:
                np.testing.assert_allclose(table.getcol('CPARAM'), np.sqrt(17. / 16.0625),
                                           atol=3e-6, rtol=0.)
                np.testing.assert_allclose(table.getcol('TIME') - earliest, 160. / 16.0625,
                                           atol=1e-5, rtol=0.)
        if 'native' in outputs:
            compare(outputs['casa'], outputs['native'])


def controls(root, vis, solvers):
    selected = two_windows(root, vis)
    first_flagged = root / 'flagged-first-window.ms'
    shutil.copytree(selected, first_flagged)
    with opened(first_flagged, True) as table:
        flag = table.getcol('FLAG')
        flag[..., table.getcol('DATA_DESC_ID') == 0] = True
        table.putcol('FLAG', flag)
        table.putcol('FLAG_ROW', flag.all(axis=(0, 1)))
    scalar = source(root, 'RR')
    stokes_i = constant_spectra(scalar, root / 'stokes-i.ms', [4.], [1])
    reversed_hands = constant_spectra(vis, root / 'reversed-hands.ms', [4., 16.], [8, 5])
    tiny = constant_spectra(vis, root / 'small-power.ms', [1e-8, 1e-8])
    cross_hand = source(root, 'RR RL LR LL')
    with opened(cross_hand, True) as table:
        flag = table.getcol('FLAG')
        flag[[0, 3]] = True
        table.putcol('FLAG', flag)
        table.putcol('FLAG_ROW', flag.all(axis=(0, 1)))
    zeros = root / 'zero-power.ms'
    shutil.copytree(vis, zeros)
    with opened(zeros, True) as table:
        first, second = (table.getcol(name) for name in ('ANTENNA1', 'ANTENNA2'))
        data = table.getcol('DATA')
        data[:, :4, (first == 0) & (second == 0)] = np.array([0., 0., 4., 4.])[None, :, None]
        data[:, :4, (first == 1) & (second == 1)] = 0.
        table.putcol('DATA', data)
        table.putcol('CORRECTED_DATA', data)
    model = root / 'nonunity-model.ms'
    shutil.copytree(vis, model)
    with opened(model, True) as table:
        table.putcol('MODEL_DATA', 4. * table.getcol('MODEL_DATA'))
    prior = root / 'infinite-casa.cal'
    library = root / 'sampler.callib'
    library.write_text(f"caltable='{prior}' tinterp='nearest' calwt=True spwmap=[0,0]\n")
    cases = (
        ('spw-combined', selected, dict(combine='spw')),
        ('flagged-first-window', first_flagged, dict(combine='spw')),
        ('fields-combined', vis, dict(field='', combine='field,scan')),
        ('mapped-prior', selected, dict(spw='1', gaintable=[str(prior)], gainfield=['0'],
                                       interp=['nearest'], spwmap=[0, 0])),
        ('mapped-library', selected, dict(spw='1', docallib=True, callib=str(library))),
        ('single-hand', scalar, {}),
        ('stokes-i', stokes_i, {}),
        ('reversed-hands', reversed_hands, {}),
        ('small-power', tiny, {}),
        ('zero-power', zeros, dict(spw='0:0~3')),
        ('cross-hand-only', cross_hand, dict(corrdepflags=True)),
        ('nonunity-model', model, {}),
    )
    for label, source_ms, options in cases:
        outputs = {key: solve(root, source_ms, task, f'{label}-{key}', **options)
                   for key, task in solvers.items()}
        if label in ('mapped-prior', 'mapped-library'):
            for output in outputs.values():
                with opened(output) as table:
                    np.testing.assert_allclose(table.getcol('CPARAM')[:, 0, :4], 1.5,
                                               atol=4e-6, rtol=0.)
        if label == 'nonunity-model':
            with opened(prior) as unscaled:
                expected = .5 * unscaled.getcol('CPARAM')[:, 0, :4]
            for output in outputs.values():
                with opened(output) as table:
                    np.testing.assert_allclose(table.getcol('CPARAM')[:, 0, :4], expected,
                                               atol=3e-6, rtol=0.)
        if label in ('flagged-first-window', 'cross-hand-only'):
            with opened(outputs['casa']) as table:
                assert table.getcol('FLAG').all()
                np.testing.assert_array_equal(table.getcol('CPARAM'), 1.)
        if label == 'zero-power':
            with opened(outputs['casa']) as table:
                np.testing.assert_allclose(table.getcol('CPARAM')[:, 0, 0], np.sqrt(2.))
                assert table.getcol('FLAG')[:, 0, 1].all()
        expected = {'stokes-i': [2., 0.], 'reversed-hands': [2., 4.],
                    'small-power': [1e-4, 1e-4]}.get(label)
        if expected is not None:
            for output in outputs.values():
                with opened(output) as table:
                    values = table.getcol('CPARAM')
                    assert not table.getcol('FLAG').any()
                    np.testing.assert_allclose(values, np.broadcast_to(np.array(expected)[:, None, None],
                                                                       values.shape), rtol=2e-6)
        if 'native' in outputs:
            compare(outputs['casa'], outputs['native'])
    appended = {key: solve(root, vis, task, f'append-{key}', field='PHASE')
                for key, task in solvers.items()}
    for key, task in solvers.items():
        solve(root, vis, task, f'append-{key}', append=True)
        with opened(appended[key]) as table:
            assert table.nrows() == 10
            np.testing.assert_array_equal(np.sort(table.getcol('FIELD_ID')), np.repeat([0, 1], 5))
    if 'native' in appended:
        compare(appended['casa'], appended['native'])
    varying_prior(root, vis, solvers)

    # The CASA wrapper misspells ANTENNA1 when a TaQL selection is supplied.
    # Its antenna selector provides an independent reference for the intended intersection.
    equivalent = solve(root, vis, solvers['casa'], 'query-casa', antenna='A0,A1,A2&&&')
    if 'native' in solvers:
        queried = solve(root, vis, solvers['native'], 'query-native', msselect='ANTENNA1<3')
        compare(equivalent, queried)

    cross_only = root / 'no-autocorrelations.ms'
    with opened(vis) as table:
        cross = table.getcol('ANTENNA1') != table.getcol('ANTENNA2')
    copy_selected_rows(vis, cross_only, cross)
    for key, task in solvers.items():
        name = f'no-autocorrelations-{key}'
        try:
            solve(root, cross_only, task, name)
        except RuntimeError:
            assert not (root / f'{name}.cal').exists()
        else:
            raise AssertionError(f'{key} accepted a dataset without autocorrelations')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--controls', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    solvers, appliers = {'casa': casa.accor}, {'casa': casa.applycal}
    if not args.reference_only:
        from casars.tasks import accor, applycal

        solvers['native'], appliers['native'] = accor, applycal
    with chdir(root):
        vis = source(root)
        run(root, vis, solvers, appliers)
        if args.controls:
            controls(root, vis, solvers)
    print('ACCOR sampler amplitudes, flags and consumer checks passed.', flush=True)


if __name__ == '__main__':
    main()
