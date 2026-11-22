#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare fringe delay, delay rate and reference phase with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path
import shutil
import time

import casatasks as casa
from casatools import table as casa_table
import numpy as np

from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms


def source(root):
    vis = root / 'source.ms'
    make_cal_ms(str(vis), channels=32, scan_seconds=160, integration_seconds=2,
                stokes='RR LL', frequency='8.4GHz')
    with opened(vis / 'SPECTRAL_WINDOW') as window:
        frequencies = window.getcell('CHAN_FREQ', 0)
    phase = np.array([[0, .15, -.4, .25, .3], [0, -.2, .1, -.25, .4]])
    delay_ns = np.array([[0, 23, -40, 80, 42], [0, 29, -32, 95, 62]])
    rate_hz = np.array([[0, .003, -.006, .011, -.004], [0, .004, .005, .008, -.005]])
    with opened(vis, True) as table:
        times, field, ant1, ant2 = (table.getcol(column) for column in
                                     ('TIME', 'FIELD_ID', 'ANTENNA1', 'ANTENNA2'))
        epoch = times[field == 0].mean()
        delta_time = times - epoch
        delta_frequency = frequencies - frequencies[0]

        def baseline(parameter):
            return (parameter[:, ant1] - parameter[:, ant2])[:, None, :]

        angle = baseline(phase) + 2. * np.pi * (
            baseline(delay_ns) * 1e-9 * delta_frequency[None, :, None]
            + baseline(rate_hz) * delta_time[None, None, :])
        random = np.random.default_rng(1521)
        noise = .0002 * (random.normal(size=angle.shape) + 1j * random.normal(size=angle.shape))
        data = (np.exp(1j * angle) + noise).astype(np.complex64)
        table.putcol('DATA', data)
        table.putcol('CORRECTED_DATA', data)
        table.putcol('MODEL_DATA', np.ones_like(data))
        table.putcol('FLAG', np.zeros(data.shape, bool))
        table.putcol('FLAG_ROW', np.zeros(len(times), bool))
    print(f'fixture {len(times)} rows and {data.size} visibility samples', flush=True)
    return vis, (phase, delay_ns, rate_hz, frequencies, epoch, times.min())


def solve(root, vis, task, name, **options):
    output = root / f'{name}.cal'
    defaults = dict(field='FLUX', refant='A0', solint='inf', minsnr=2.,
                    delaywindow=[-250., 250.], ratewindow=[-2e-12, 2e-12])
    start = time.perf_counter()
    assert task(str(vis), str(output), **(defaults | options)) is None
    assert output.is_dir()
    print(name, f'solve {time.perf_counter() - start:.3f}s', flush=True)
    return output


def read_solutions(path):
    with opened(path) as table:
        assert table.info()['subType'] == 'Fringe Jones'
        rows = table.getcol('ANTENNA1')
        np.testing.assert_array_equal(rows, np.arange(5))
        np.testing.assert_array_equal(table.getcol('ANTENNA2'), np.zeros_like(rows))
        np.testing.assert_array_equal(table.getcol('FIELD_ID'), np.zeros_like(rows))
        np.testing.assert_array_equal(table.getcol('INTERVAL'), np.zeros_like(rows))
        assert not table.getcol('FLAG').any()
        solutions = table.getcol('FPARAM').reshape(2, 4, 5)
        time = table.getcell('TIME', 0)
    with opened(path / 'SPECTRAL_WINDOW') as table:
        frequency = table.getcell('CHAN_FREQ', 0).item()
    return solutions, time, frequency


def check_global(solution, law, zero_rates):
    known_phase, known_delay, known_rate, frequencies, epoch, start = law
    params, time, center = read_solutions(solution)
    np.testing.assert_allclose(center, frequencies.mean(), atol=.01, rtol=0.)
    np.testing.assert_allclose(time, epoch, atol=1e-5, rtol=0.)
    np.testing.assert_allclose(params[:, 1, :], known_delay, atol=.00025, rtol=0.)
    rate = known_rate / frequencies[0]
    np.testing.assert_allclose(params[:, 2, :], 0. if zero_rates else rate, atol=3e-18, rtol=0.)
    np.testing.assert_array_equal(params[:, 3, :], np.zeros_like(known_delay))
    phase = known_phase + 2. * np.pi * (known_delay * (center - frequencies[0]) * 1e-9
                                         + known_rate * (start - epoch) + rate * center * (time - start))
    error = np.angle(np.exp(1j * (params[:, 0, :] - phase)))
    np.testing.assert_allclose(error, 0., atol=1e-4)


def compare_tables(reference, actual, delay_tolerance=.001, *, dispersion_tolerance=1e-6,
                   time_tolerance=2e-5, snr_tolerance=1e-3):
    with opened(reference) as left, opened(actual) as right:
        assert right.info() == left.info()
        assert sorted(right.colnames()) == sorted(left.colnames())
        for name in left.colnames():
            defined = [left.iscelldefined(name, row) for row in range(left.nrows())]
            assert defined == [right.iscelldefined(name, row) for row in range(right.nrows())], name
            if not any(defined):
                continue
            a, b = left.getcol(name), right.getcol(name)
            if name == 'TIME':
                np.testing.assert_allclose(b, a, atol=time_tolerance, rtol=0., err_msg=name)
            elif name == 'FPARAM':
                np.testing.assert_allclose(b[0::4], a[0::4], atol=2e-4, rtol=0.)
                np.testing.assert_allclose(b[1::4], a[1::4], atol=delay_tolerance, rtol=0.)
                np.testing.assert_allclose(b[2::4], a[2::4], atol=1e-17, rtol=0.)
                np.testing.assert_allclose(b[3::4], a[3::4], atol=dispersion_tolerance, rtol=1e-3)
            elif name == 'SNR':
                np.testing.assert_allclose(b, a, atol=.001, rtol=snr_tolerance, err_msg=name)
            else:
                np.testing.assert_array_equal(b, a, err_msg=name)
    with opened(reference / 'SPECTRAL_WINDOW') as left, opened(actual / 'SPECTRAL_WINDOW') as right:
        for column in ('CHAN_FREQ', 'REF_FREQUENCY', 'CHAN_WIDTH', 'NUM_CHAN'):
            np.testing.assert_allclose(right.getcol(column), left.getcol(column), rtol=0.,
                                       atol=.01, err_msg=column)


def apply(root, vis, task, solution, name, field='FLUX', field_id=0, **options):
    output = root / f'{name}.ms'
    shutil.copytree(vis, output)
    assert task(str(output), field=field, gaintable=[str(solution)], flagbackup=False,
                **options) is None
    with opened(output) as table:
        mask = table.getcol('FIELD_ID') == field_id
        return table.getcol('CORRECTED_DATA')[..., mask], table.getcol('FLAG')[..., mask]


def check_pair(root, vis, law, solvers, appliers):
    for case, controls in [('fft', dict(globalsolve=False)), ('global', dict(globalsolve=True)),
                           ('zero-rates', dict(globalsolve=True, zerorates=True))]:
        products = {name: solve(root, vis, task, f'{case}-{name}', **controls)
                    for name, task in solvers.items()}
        for solution in products.values():
            if case == 'fft':
                params, _, _ = read_solutions(solution)
                np.testing.assert_allclose(params[:, 1, :], law[1], atol=2.1, rtol=0.)
                np.testing.assert_allclose(params[:, 2, :], law[2] / law[3][0], atol=4e-14, rtol=0.)
            else:
                check_global(solution, law, case == 'zero-rates')
        if 'native' in products:
            compare_tables(products['casa'], products['native'])
        expected = apply(root, vis, appliers['casa'], products['casa'], f'{case}-casa-to-casa')
        assert not expected[1].any()
        for producer, solution in products.items():
            for consumer, task in appliers.items():
                if (producer, consumer) == ('casa', 'casa'):
                    continue
                actual = apply(root, vis, task, solution, f'{case}-{producer}-to-{consumer}')
                np.testing.assert_array_equal(actual[1], expected[1])
                rms = np.linalg.norm(actual[0] - expected[0]) / np.linalg.norm(expected[0])
                assert rms < 1e-3, (case, producer, consumer, rms)


def check_controls(root, vis, solvers):
    cases = (
        ('high-threshold', dict(minsnr=1e8)),
        ('stokes', dict(corrdepflags=True, corrcomb='stokes')),
        ('parallel', dict(corrdepflags=True, corrcomb='parallel')),
        ('phase-only', dict(paramactive=[False, False, False])),
        ('rate-disabled', dict(paramactive=[True, False, False])),
        ('zero-iterations', dict(niter=0)),
        ('separate-windows', dict(concatspws=False, globalsolve=False)),
        ('single-channel', dict(spw='0:4')),
        ('finite-intervals', dict(solint='47s')),
        ('parallactic-angle', dict(parang=True)),
        ('stepped-channels', dict(spw='0:0~31^2', globalsolve=False)),
        ('noncontiguous-channels', dict(spw='0:0~9;15~24', globalsolve=False)),
        ('signed-delay-window', dict(delaywindow=[1., 110.], globalsolve=False)),
        ('signed-rate-window', dict(ratewindow=[2e-13, 1.2e-12], globalsolve=False)),
    )
    for case, options in cases:
        files = {name: solve(root, vis, task, f'{case}-{name}', **options)
                 for name, task in solvers.items()}
        if 'native' in files:
            compare_tables(files['casa'], files['native'])
        with opened(files['casa']) as table:
            parameters = table.getcol('FPARAM')
            if case == 'high-threshold':
                assert table.getcol('FLAG').sum() == 32
            if case in ('stokes', 'parallel'):
                np.testing.assert_array_equal(parameters[:4], parameters[4:])
            if case in ('phase-only', 'rate-disabled'):
                assert not parameters[2::4].any()
            if case == 'phase-only':
                assert not parameters[1::4].any()
    for backend, task in solvers.items():
        for case, options in [('missing-reference', dict(refant='')),
                              ('single-integration', dict(solint='int'))]:
            path = root / f'{case}-{backend}.cal'
            try:
                solve(root, vis, task, f'{case}-{backend}', **options)
            except RuntimeError:
                assert not path.exists()
            else:
                raise AssertionError(f'{backend} accepted {case}')


def check_descending_axis(root, vis, solvers):
    selected = root / 'descending.ms'
    shutil.copytree(vis, selected)
    with opened(selected / 'SPECTRAL_WINDOW', True) as table:
        table.putcell('CHAN_FREQ', 0, table.getcell('CHAN_FREQ', 0)[::-1])
        table.putcell('CHAN_WIDTH', 0, -table.getcell('CHAN_WIDTH', 0)[::-1])
    with opened(selected, True) as table:
        for column in ('DATA', 'MODEL_DATA', 'CORRECTED_DATA', 'FLAG'):
            table.putcol(column, table.getcol(column)[:, ::-1])
    results = {key: solve(root, selected, run, f'descending-{key}', globalsolve=False)
               for key, run in solvers.items()}
    if 'native' in results:
        compare_tables(results['casa'], results['native'])


def change_reference_fixture(root, vis):
    output = root / 'switching-reference.ms'
    shutil.copytree(vis, output)
    with opened(output, True) as table:
        times, a, b = (table.getcol(name) for name in ('TIME', 'ANTENNA1', 'ANTENNA2'))
        delta = times - times.min()
        flags = table.getcol('FLAG')
        flags[..., ((a == 0) | (b == 0)) & (delta >= 48.) & (delta < 94.)] = True
        flags[1, 3:16] |= ((a == 3) | (b == 3))[None, :]
        table.putcol('FLAG', flags)
        table.putcol('FLAG_ROW', flags.all(axis=(0, 1)))
        weight_scale = np.array([5., .25])[:, None]
        table.putcol('WEIGHT', table.getcol('WEIGHT') * weight_scale)
        table.putcol('SIGMA', table.getcol('SIGMA') / np.sqrt(weight_scale))
    return output


def check_reference_switch(root, vis, solvers):
    selected = change_reference_fixture(root, vis)
    for label, options in [('switch', dict(solint='47s', refant='A0,A1')),
                           ('mixed-stokes', dict(corrcomb='stokes')),
                           ('mixed-parallel', dict(corrcomb='parallel'))]:
        results = {key: solve(root, selected, run, f'{label}-{key}', corrdepflags=True, **options)
                   for key, run in solvers.items()}
        if 'native' in results:
            compare_tables(results['casa'], results['native'])
        if label == 'switch':
            with opened(results['casa']) as table:
                np.testing.assert_array_equal(table.getcol('ANTENNA2')[::5], [0, 1, 1, 0])
                assert table.getcol('FLAG').sum() == 8


def duplicate_spectral_window(root, vis, delays, name='two-windows.ms'):
    output = root / name
    shutil.copytree(vis, output)
    for path in (output / 'SPECTRAL_WINDOW', output / 'DATA_DESCRIPTION', output):
        count = duplicate_rows(casa_table(), path)
    with opened(output / 'DATA_DESCRIPTION', True) as table:
        table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    separation_hz = 48e6
    with opened(output / 'SPECTRAL_WINDOW', True) as table:
        for column in ('CHAN_FREQ', 'REF_FREQUENCY'):
            table.putcell(column, 1, table.getcell(column, 0) + separation_hz)
    with opened(output, True) as table:
        table.putcol('DATA_DESC_ID', np.ones(count, dtype=np.int32), count, count)
        a, b = (table.getcol(column, 0, count) for column in ('ANTENNA1', 'ANTENNA2'))
        phase = 2j * np.pi * separation_hz * 1e-9 * (delays[:, a] - delays[:, b])
        for column in ('DATA', 'CORRECTED_DATA'):
            table.putcol(column, table.getcol(column, 0, count) * np.exp(phase)[:, None, :],
                         count, count)
    return output


def check_two_windows(root, vis, law, solvers, appliers):
    selected = duplicate_spectral_window(root, vis, law[1])
    library = root / 'fringe.callib'
    library.write_text(
        f"caltable='{root / 'channel-selection-casa.cal'}' tinterp='nearest' "
        "calwt=True fldmap=[0] spwmap=[0,0]\n")
    gain = root / 'prior-gain.cal'
    casa.gaincal(str(selected), str(gain), field='FLUX', spw='1', refant='A0', calmode='a',
                 minsnr=0., minblperant=1)
    cases = [
        ('channel-selection', dict(spw='0:5~21')),
        ('combined-fft-grid', dict(spw='0,1', combine='spw')),
        ('separate-fft-grids', dict(spw='0,1', combine='spw', concatspws=False, globalsolve=False)),
        ('separate-fft-global', dict(spw='0,1', combine='spw', concatspws=False)),
        ('prior-apply', dict(spw='0', gaintable=['channel-selection-casa.cal'], gainfield=['0'],
                             interp=['nearest'], spwmap=[0, 0])),
        ('prior-callibrary', dict(spw='0', docallib=True, callib=str(library))),
        ('mapped-chain', dict(spw='1', gaintable=['channel-selection-casa.cal', str(gain)],
                              gainfield=['0', '0'], interp=['nearest', 'linear'],
                              spwmap=[[0, 0], [1, 1]])),
    ]
    for label, options in cases:
        results = {key: solve(root, selected, run, f'{label}-{key}', **options)
                   for key, run in solvers.items()}
        if 'native' in results:
            compare_tables(results['casa'], results['native'])
        if label in ('channel-selection', 'combined-fft-grid'):
            with opened(results['casa']) as table:
                delays = table.getcol('FPARAM')[1::4, 0]
                np.testing.assert_allclose(delays, law[1], atol=.001, rtol=0.)
        if label == 'separate-fft-global':
            compare_consumers(root, selected, results, appliers, label, spwmap=[[0, 0]])


def check_append(root, vis, solvers):
    outputs = {}
    for label, task in solvers.items():
        name = f'append-{label}'
        outputs[label] = solve(root, vis, task, name, field='PHASE')
        solve(root, vis, task, name, field='FLUX', append=True)
        with opened(outputs[label]) as table:
            np.testing.assert_array_equal(np.sort(table.getcol('FIELD_ID')), np.repeat([0, 1], 5))
    if 'native' in outputs:
        compare_tables(outputs['casa'], outputs['native'])


def check_dispersion(root, vis, law, solvers, appliers):
    selected = root / 'dispersive.ms'
    shutil.copytree(vis, selected)
    dispersion = 5e6 * np.array([[0., 2., -.7, 1.2, -.4], [0., -.3, 1.4, .6, -.8]])
    frequencies = law[3].astype(np.float32)
    low, high = frequencies[[0, -1]]
    slope = (frequencies - low - high) / (low * high)
    kernel = (2. * np.pi * 1e6 * (1. / frequencies.astype(np.float64) + slope)).astype(np.float32)
    with opened(selected, True) as table:
        a, b = (table.getcol(column) for column in ('ANTENNA1', 'ANTENNA2'))
        rotation = np.exp(1j * kernel[None, :, None] * (dispersion[:, a] - dispersion[:, b])[:, None])
        for column in ('DATA', 'CORRECTED_DATA'):
            table.putcol(column, table.getcol(column) * rotation)
    results = {key: solve(root, selected, run, f'dispersion-{key}', paramactive=[True, True, True])
               for key, run in solvers.items()}
    for result in results.values():
        parameters = read_solutions(result)[0]
        np.testing.assert_allclose(parameters[:, 3], dispersion, atol=2e4, rtol=0.)
    if 'native' in results:
        compare_tables(results['casa'], results['native'], dispersion_tolerance=2.)
    compare_consumers(root, selected, results, appliers, 'dispersion')



def check_recorded(root, vis, solvers, appliers):
    """casatestdata@5e486372 measurementset/other/n08c1.ms (also used by CASA)."""
    field = '4C39.25'
    with opened(vis / 'FIELD') as table:
        field_id, = np.flatnonzero(table.getcol('NAME') == field)
    options = dict(field=field, refant='EF', zerorates=True, delaywindow=[], ratewindow=[])
    products = {name: solve(root, vis, task, f'n08c1-{name}', **options)
                for name, task in solvers.items()}
    with opened(products['casa']) as table:
        assert table.nrows() == 44
        assert (~table.getcol('FLAG')).sum() == 96
    if 'native' in products:
        compare_tables(products['casa'], products['native'], delay_tolerance=.012)
    compare_consumers(root, vis, products, appliers, 'n08c1', field, field_id)


def compare_consumers(root, vis, products, appliers, name, field='FLUX', field_id=0, **options):
    expected = apply(root, vis, appliers['casa'], products['casa'], f'{name}-casa-to-casa',
                     field, field_id, **options)
    assert (~expected[1]).any()
    for producer, solution in products.items():
        for consumer, task in appliers.items():
            if (producer, consumer) == ('casa', 'casa'):
                continue
            actual = apply(root, vis, task, solution, f'{name}-{producer}-to-{consumer}', field,
                           field_id, **options)
            np.testing.assert_array_equal(actual[1], expected[1])
            rms = np.linalg.norm(actual[0] - expected[0]) / np.linalg.norm(expected[0])
            print(f'{name} {producer} to {consumer} NRMS {rms:.8g}', flush=True)
            assert rms < 1e-3


def copy_selected_rows(source, output, mask):
    with opened(source) as table:
        view = table.selectrows(np.flatnonzero(mask).tolist())
        try:
            view.copy(str(output), deep=True, valuecopy=True)
        finally:
            view.close()


def check_flagged_intervals(root, vis, solvers):
    selected = root / 'flagged-interval.ms'
    shutil.copytree(vis, selected)
    with opened(selected, True) as table:
        time = table.getcol('TIME')
        missing = (time - time.min() >= 48.) & (time - time.min() < 94.)
        flag = table.getcol('FLAG')
        flag[:, 4:28, missing] = True
        table.putcol('FLAG', flag)
    for dependent in (False, True):
        results = {key: solve(root, selected, run, f'missing-{dependent}-{key}',
                             spw='0:4~27', solint='47s', corrdepflags=dependent)
                   for key, run in solvers.items()}
        with opened(results['casa']) as table:
            assert table.nrows() == 15
        if 'native' in results:
            compare_tables(results['casa'], results['native'])

    noisy = root / 'one-noisy-hand.ms'
    shutil.copytree(vis, noisy)
    with opened(noisy, True) as table:
        first, second = (table.getcol(name) for name in ('ANTENNA1', 'ANTENNA2'))
        affected = (first == 3) | (second == 3)
        data = table.getcol('DATA')
        rng = np.random.default_rng(221)
        data[0][:, affected] = np.exp(1j * rng.uniform(-np.pi, np.pi, (32, affected.sum())))
        table.putcol('DATA', data)
        table.putcol('CORRECTED_DATA', data)
    products = {key: solve(root, noisy, task, f'snr-threshold-{key}', minsnr=10.)
                for key, task in solvers.items()}
    with opened(products['casa']) as table:
        flag = table.getcol('FLAG')[:, 0, 3]
        np.testing.assert_array_equal(flag, np.repeat([True, False], 4))
    if 'native' in products:
        compare_tables(products['casa'], products['native'])


def check_fft_boundaries(root, vis, solvers):
    nyquist = root / 'nyquist.ms'
    shutil.copytree(vis, nyquist)
    with opened(nyquist, True) as table:
        time, first, second = (table.getcol(name) for name in ('TIME', 'ANTENNA1', 'ANTENNA2'))
        channel = (np.arange(32) % 2)[:, None]
        odd_time = (np.rint((time - time.min()) / 2.).astype(int) % 2)[None, :]
        sign = ((first == 1).astype(int) - (second == 1)) * channel
        sign += ((first == 2).astype(int) - (second == 2)) * odd_time
        visibility = np.repeat((1 - 2 * (sign % 2))[None, :, :], 2, axis=0).astype(np.complex64)
        for name in ('DATA', 'CORRECTED_DATA'):
            table.putcol(name, visibility)
    results = {key: solve(root, nyquist, run, f'nyquist-{key}', globalsolve=False,
                         delaywindow=[], ratewindow=[]) for key, run in solvers.items()}
    with opened(results['casa']) as table:
        params = table.getcol('FPARAM')
        assert abs(params[1, 0, 1]) == 500.
        assert abs(params[2, 0, 2]) > 2.9e-11
    if 'native' in results:
        compare_tables(results['casa'], results['native'])

    irregular = root / 'irregular.ms'
    with opened(vis) as table:
        times = table.getcol('TIME')
    available = np.unique(times)[:4]
    copy_selected_rows(vis, irregular, np.isin(times, available))
    with opened(irregular, True) as table:
        times = table.getcol('TIME')
        mapped = available[0] + np.array([0., 1., 2., 10.])[np.searchsorted(available, times)]
        table.putcol('TIME', mapped)
        table.putcol('TIME_CENTROID', mapped)
    for concat in (True, False):
        results = {key: solve(root, irregular, run, f'irregular-{concat}-{key}',
                             globalsolve=False, concatspws=concat) for key, run in solvers.items()}
        if 'native' in results:
            repeat = solve(root, irregular, solvers['native'], f'irregular-{concat}-repeat',
                           globalsolve=False, concatspws=concat)
            compare_tables(results['casa'], results['native'])
            compare_tables(results['native'], repeat)


def check_unequal_windows(root, vis, law, solvers, appliers):
    both = duplicate_spectral_window(root, vis, law[1], 'unequal-widths.ms')
    with opened(both / 'SPECTRAL_WINDOW', True) as table:
        for name in ('CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION', 'TOTAL_BANDWIDTH'):
            table.putcell(name, 1, 2 * table.getcell(name, 1))
    with opened(both) as table:
        time = table.getcol('TIME')
        missing = (table.getcol('DATA_DESC_ID') == 1) & (time - time.min() > 80.)
    selected = root / 'unsynchronized-windows.ms'
    copy_selected_rows(both, selected, ~missing)
    for global_solve in (False, True):
        label = f'unequal-global-{global_solve}'
        products = {name: solve(root, selected, task, f'{label}-{name}', spw='0,1', combine='spw',
                                solint='80s', globalsolve=global_solve)
                    for name, task in solvers.items()}
        with opened(products['casa'] / 'SPECTRAL_WINDOW') as table:
            np.testing.assert_allclose(table.getcell('CHAN_FREQ', 0), [8.4475e9], atol=.01, rtol=0.)
        if 'native' in products:
            compare_tables(products['casa'], products['native'])
        if global_solve:
            compare_consumers(root, selected, products, appliers, label, spwmap=[[0, 0]])
    # CASA's concatenated solver aborts when both windows provide only one
    # channel. Exercise the bounded native grid for both search modes.
    if 'native' in solvers:
        for concatenate in (True, False):
            product = solve(root, both, solvers['native'], f'single-channel-bands-{concatenate}',
                            spw='0:0,1:0', combine='spw', concatspws=concatenate, globalsolve=False)
            with opened(product) as table:
                assert table.nrows() == 5
                assert np.isfinite(table.getcol('FPARAM')).all()
                assert not table.getcol('FLAG').any()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--controls', action='store_true')
    parser.add_argument('--recorded', type=Path, help='Path to the CASA n08c1.ms test fixture')
    parser.add_argument('--controls-only', action='store_true')
    parser.add_argument('--edge-cases', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    recorded = args.recorded.resolve() if args.recorded else None
    solvers, appliers = {'casa': casa.fringefit}, {'casa': casa.applycal}
    if not args.reference_only:
        from casars.tasks import applycal, fringefit

        solvers['native'], appliers['native'] = fringefit, applycal
    with chdir(root):
        vis, law = source(root)
        if not args.controls_only:
            check_pair(root, vis, law, solvers, appliers)
        if args.controls or args.controls_only:
            check_controls(root, vis, solvers)
            check_descending_axis(root, vis, solvers)
            check_reference_switch(root, vis, solvers)
            check_two_windows(root, vis, law, solvers, appliers)
            check_append(root, vis, solvers)
            check_dispersion(root, vis, law, solvers, appliers)
        if recorded:
            check_recorded(root, recorded, solvers, appliers)
        if args.edge_cases:
            check_flagged_intervals(root, vis, solvers)
            check_fft_boundaries(root, vis, solvers)
            check_unequal_windows(root, vis, law, solvers, appliers)
    print('Fringe FFT, global solve, zero-rate and consumer checks passed.', flush=True)


if __name__ == '__main__':
    main()
