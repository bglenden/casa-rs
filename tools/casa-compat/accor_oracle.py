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
    path = root / ('autocorrelations.ms' if stokes == 'RR LL' else 'one-hand.ms')
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
        gains = GAIN[:1] if stokes == 'RR' else GAIN
        power = gains[:, first] * gains[:, second] * temporal_factor
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


def controls(root, vis, solvers):
    selected = two_windows(root, vis)
    scalar = source(root, 'RR')
    model = root / 'nonunity-model.ms'
    shutil.copytree(vis, model)
    with opened(model, True) as table:
        table.putcol('MODEL_DATA', 4. * table.getcol('MODEL_DATA'))
    prior = root / 'infinite-casa.cal'
    library = root / 'sampler.callib'
    library.write_text(f"caltable='{prior}' tinterp='nearest' calwt=True spwmap=[0,0]\n")
    cases = (
        ('spw-combined', selected, dict(combine='spw')),
        ('fields-combined', vis, dict(field='', combine='field,scan')),
        ('mapped-prior', selected, dict(spw='1', gaintable=[str(prior)], gainfield=['0'],
                                       interp=['nearest'], spwmap=[0, 0])),
        ('mapped-library', selected, dict(spw='1', docallib=True, callib=str(library))),
        ('single-hand', scalar, {}),
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
