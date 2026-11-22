#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair GSPLINE coefficients and calibrated visibilities with CASA 6.7.6.14."""

import argparse
import ast
from contextlib import chdir
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time

import casatasks as casa
import numpy as np
from scipy.interpolate import BSpline

from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def source(root, mode, *, wraps=False, noisy=False, **geometry):
    vis = root / 'source.ms'
    make_cal_ms(str(vis), **(dict(channels=8, scan_seconds=120) | geometry))
    with opened(vis, True) as table:
        times = table.getcol('TIME')
        samples = table.getcol('DATA')
        first, second = (table.getcol(name)[None, :] for name in ('ANTENNA1', 'ANTENNA2'))
        x = ((times - times.min()) / np.ptp(times))[None, :]
        hand = np.array([[1.], [2.]])

        def amplitude(antenna):
            return .012 * (antenna + 1) * (1. + .8 * x + .4 * hand * x**2)

        def phase(antenna):
            if wraps:
                return 1.5 * antenna * x * (1. + .12 * hand * x)
            return .08 * antenna * (1. + x - .28 * hand * x**2)

        signal = np.ones((2, len(times)), dtype=complex)
        if 'a' in mode:
            signal *= np.exp(amplitude(first) + amplitude(second))
        if 'p' in mode:
            signal *= np.exp(1j * (phase(first) - phase(second)))
        samples[:] = signal[:, None, :]
        flags = np.zeros(samples.shape, dtype=bool)
        weights = np.ones((2, len(times)), dtype=np.float32)
        if noisy:
            random = np.random.default_rng(251536)
            noise = random.normal(size=samples.shape) + 1j * random.normal(size=samples.shape)
            samples += .001 * noise
            weights *= random.uniform(.5, 2.5, size=weights.shape)
            # Fully omit one integration and two baselines across a time span.
            flags[:, :, times == np.unique(times)[11]] = True
            flags[:, :, (first[0] == 0) & (second[0] % 2 == 1) & (x[0] > .55) & (x[0] < .7)] = True
        table.putcol('DATA', samples)
        table.putcol('CORRECTED_DATA', samples)
        table.putcol('MODEL_DATA', np.ones_like(samples))
        table.putcol('FLAG', flags)
        table.putcol('FLAG_ROW', np.all(flags, axis=(0, 1)))
        table.putcol('WEIGHT', weights)
        table.putcol('SIGMA', 1. / np.sqrt(weights))
    return vis, signal


def check_science(table_path, vis, truth, mode):
    with opened(vis) as table:
        times, field, ant1, ant2 = (table.getcol(name) for name in
                                    ('TIME', 'FIELD_ID', 'ANTENNA1', 'ANTENNA2'))
    result = np.ones_like(truth)
    with opened(table_path) as table:
        for row in range(table.nrows()):
            antenna, field_id = (table.getcell(name, row) for name in ('ANTENNA1', 'FIELD_ID'))
            in_field = field == field_id
            for term in ('AMP', 'PHASE'):
                enabled = 'a' in mode if term == 'AMP' else 'p' in mode
                if not enabled:
                    assert table.getcell(f'N_KNOTS_{term}', row) == 0
                    continue
                knots = table.getcell(f'SPLINE_KNOTS_{term}', row).ravel()
                coeff = table.getcell(f'POLY_COEFF_{term}', row).reshape(2, -1)
                value = np.stack([BSpline(knots, hand, 3)(times) for hand in coeff])
                left = (ant1 == antenna) & in_field
                right = (ant2 == antenna) & in_field
                if term == 'AMP':
                    result[:, left | right] *= np.exp(value[:, left | right])
                else:
                    result[:, left] *= np.exp(-1j * value[:, left])
                    result[:, right] *= np.exp(1j * value[:, right])
    np.testing.assert_allclose(result, truth, atol=5e-7, rtol=5e-7)


def compare_tables(reference, actual, rows=None):
    with opened(reference) as left, opened(actual) as right:
        assert left.info() == right.info()
        assert sorted(left.colnames()) == sorted(right.colnames())
        if rows is None:
            assert left.nrows() == right.nrows()
            rows = (range(left.nrows()), range(right.nrows()))
        for left_row, right_row in zip(*rows, strict=True):
            for name in left.colnames():
                defined = left.iscelldefined(name, left_row)
                assert right.iscelldefined(name, right_row) == defined, (name, left_row, right_row)
                if not defined:
                    continue
                a, b = left.getcell(name, left_row), right.getcell(name, right_row)
                if name == 'REF_FREQUENCY':
                    np.testing.assert_allclose(b, a, atol=.01, rtol=0., err_msg=name)
                elif name == 'TIME' or name.startswith('SPLINE_KNOTS_'):
                    np.testing.assert_allclose(b, a, atol=1e-5, rtol=0., err_msg=name)
                elif np.asarray(a).dtype.kind in 'fc':
                    np.testing.assert_allclose(b, a, atol=3e-6, rtol=3e-6, err_msg=name)
                else:
                    np.testing.assert_array_equal(b, a, err_msg=name)
    for name in ('CAL_DESC', 'CAL_HISTORY'):
        with opened(reference / name) as left, opened(actual / name) as right:
            assert left.nrows() == right.nrows()
            assert sorted(left.colnames()) == sorted(right.colnames())
            for row in range(left.nrows()):
                for column in left.colnames():
                    defined = left.iscelldefined(column, row)
                    assert right.iscelldefined(column, row) == defined, (name, column, row)
                    if defined:
                        np.testing.assert_array_equal(right.getcell(column, row), left.getcell(column, row))


def solve(root, vis, task, name, mode, **controls):
    path = root / f'{name}.cal'
    start = time.perf_counter()
    options = dict(gaintype='GSPLINE', calmode=mode, refant='A0', smodel=[1, 0, 0, 0])
    result = task(str(vis), str(path), **(options | controls))
    assert list(result['solve_tables']) == [str(path)]
    assert 'solvestats' not in result
    print(f'{root.name} {name} solve {time.perf_counter() - start:.3f}s', flush=True)
    return path, result


def apply(root, vis, task, table, name, **options):
    output = root / f'{name}.ms'
    shutil.copytree(vis, output)
    start = time.perf_counter()
    assert task(str(output), gaintable=[str(table)], flagbackup=False, **options) is None
    print(f'{root.name} {name} apply {time.perf_counter() - start:.3f}s', flush=True)
    with opened(vis) as original, opened(output) as calibrated:
        np.testing.assert_array_equal(calibrated.getcol('WEIGHT'), original.getcol('WEIGHT'))
        np.testing.assert_array_equal(calibrated.getcol('SIGMA'), original.getcol('SIGMA'))
        return calibrated.getcol('CORRECTED_DATA'), calibrated.getcol('FLAG')


def check_case(root, name, mode, solvers, appliers, *, wraps=False, noisy=False, **options):
    work = root / name
    work.mkdir()
    vis, truth = source(work, mode, wraps=wraps, noisy=noisy)
    with chdir(work):
        outputs = {backend: solve(work, vis, task, backend, mode, **options)
                   for backend, task in solvers.items()}
    reference, ret = outputs['casa']
    if not (noisy or wraps):
        check_science(reference, vis, truth, mode)
    if 'native' in outputs:
        actual, other = outputs['native']
        assert set(ret) == set(other)
        for field in ret['selectvis']:
            np.testing.assert_array_equal(other['selectvis'][field], ret['selectvis'][field])
        compare_tables(reference, actual)
        if not (noisy or wraps):
            check_science(actual, vis, truth, mode)
    expected = apply(work, vis, appliers['casa'], reference, 'casa-self')
    for producer, table in outputs.items():
        for consumer, task in appliers.items():
            if (producer, consumer) == ('casa', 'casa'):
                continue
            actual_data, actual_flags = apply(work, vis, task, table[0], f'{producer}-to-{consumer}')
            np.testing.assert_array_equal(actual_flags, expected[1])
            np.testing.assert_allclose(actual_data[~actual_flags], expected[0][~actual_flags],
                                       atol=3e-6, rtol=3e-6)
    if not (noisy or wraps):
        np.testing.assert_allclose(expected[0], 1., atol=5e-7, rtol=5e-7)
    if wraps:
        for backend, (table, _) in outputs.items():
            check_phase_diagnostics(table, vis, native=backend == 'native')


def check_phase_diagnostics(path, vis, *, native):
    with opened(vis) as table:
        time, data, weights, flags, first, second = (table.getcol(column) for column in
            ('TIME', 'DATA', 'WEIGHT', 'FLAG_ROW', 'ANTENNA1', 'ANTENNA2'))
    with opened(path) as table:
        knots = table.getcell('SPLINE_KNOTS_PHASE', 0).ravel()
        gains = np.stack([table.getcell('POLY_COEFF_PHASE', ant).reshape(2, -1)
                          for ant in range(5)])
    for hand in range(2):
        log = Path(f'{path}.PHASE.pol{hand}.log').read_text().splitlines()
        assert log[0] == '*****PHASE summary********'
        for antenna, line in enumerate(log[2:7]):
            number, coefficients = line.split('[', 1)
            assert int(number) == antenna + 1
            np.testing.assert_allclose(ast.literal_eval('[' + coefficients), gains[antenna, hand],
                                       atol=1e-6, rtol=5e-6)
        expected_rms, reported_rms = [], []
        for line in log[9:]:
            ant1, ant2, reported = line.split()
            left, right = int(ant1) - 1, int(ant2) - 1
            rows = (first == left) & (second == right) & ~flags
            predicted = BSpline(knots, gains[right, hand] - gains[left, hand], 3)(time[rows])
            residual = np.angle(data[hand][:, rows].mean(axis=0) * np.exp(-1j * predicted))
            expected_rms.append(np.sqrt(np.average(residual**2, weights=weights[hand][rows])))
            reported_rms.append(float(reported))
        assert len(expected_rms) == 10
        if native:
            np.testing.assert_allclose(reported_rms, expected_rms, atol=3e-7)
        else:
            # CASA's report includes autocorrelation slots under baseline names.
            assert reported_rms[0] == 0. and expected_rms[0] > 2e-4


def benchmark(root, solvers):
    with chdir(root):
        vis, _ = source(root, 'ap', channels=32, scan_seconds=1600, integration_seconds=1)
        with opened(vis) as table:
            rows = table.nrows()
            samples = rows * table.getcell('DATA', 0).size
        print(f'benchmark {rows} rows, {samples} complex samples', flush=True)
        os.environ['CASA_RS_CALIBRATION_TIMING'] = '1'
        products = {}
        for iteration in range(3):
            for backend, task in solvers.items():
                start = time.perf_counter()
                cpu = time.process_time()
                children = resource.getrusage(resource.RUSAGE_CHILDREN)
                table, _ = solve(root, vis, task, f'{backend}-{iteration}', 'ap', splinetime=180.)
                wall = time.perf_counter() - start
                used = resource.getrusage(resource.RUSAGE_CHILDREN)
                child_cpu = used.ru_utime + used.ru_stime - children.ru_utime - children.ru_stime
                print(json.dumps(dict(backend=backend, iteration=iteration, rows=rows,
                                      samples=samples, wall_s=wall, rows_per_s=rows/wall,
                                      cpu_s=time.process_time() - cpu + child_cpu,
                                      parent_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                                      child_peak_rss_kib=used.ru_maxrss)), flush=True)
                products[(backend, iteration)] = table
            if 'native' in solvers:
                compare_tables(products['casa', iteration], products['native', iteration])


def check_solve_edges(root, solvers):
    work = root / 'solve-controls'
    work.mkdir()
    vis = root / 'combined/source.ms'
    cases = (
        ('defaults', True, {'refant': ''}),
        ('reference-a2', True, {'refant': 'A2', 'splinetime': 60.}),
        ('preaverage', True, {'preavg': 20., 'splinetime': 60.}),
        ('zero-spacing', True, {'splinetime': 0.}),
        ('negative-spacing', True, {'splinetime': -5., 'npointaver': -1, 'phasewrap': -150.}),
        ('insufficient-knots', False, {'splinetime': .1}),
        ('insufficient-preavg', False, {'preavg': 100., 'splinetime': 60.}),
    )
    with chdir(work):
        for case, expected, controls in cases:
            tables = {name: solve(work, vis, task, f'{case}-{name}', 'ap', **controls)[0]
                      for name, task in solvers.items()}
            for table in tables.values():
                assert table.exists() == expected
            if expected and 'native' in tables:
                compare_tables(tables['casa'], tables['native'])
        wrapped = root / 'wrap/source.ms'
        wrapped_solutions = {}
        for points, threshold in ((2, 145.), (4, 1000.)):
            options = dict(splinetime=70., npointaver=points, phasewrap=threshold)
            tables = {name: solve(work, wrapped, task, f'wrap-{points}-{name}', 'p', **options)[0]
                      for name, task in solvers.items()}
            if 'native' in tables:
                compare_tables(tables['casa'], tables['native'])
            with opened(tables['casa']) as table:
                wrapped_solutions[points] = table.getcol('POLY_COEFF_PHASE')
        with opened(root / 'wrap/casa.cal') as table:
            nominal = table.getcol('POLY_COEFF_PHASE')
            for other in wrapped_solutions.values():
                assert np.max(np.abs(other - nominal)) > 1e-3
        tables, snapshots = {}, {}
        for name, task in solvers.items():
            table, _ = solve(work, vis, task, f'append-{name}', 'ap', field='FLUX', splinetime=60.)
            snapshots[name] = work / f'before-append-{name}.cal'
            shutil.copytree(table, snapshots[name])
            table, _ = solve(work, vis, task, f'append-{name}', 'ap',
                             field='PHASE', splinetime=60., append=True)
            with opened(table) as product:
                assert product.nrows() == 10
                np.testing.assert_array_equal(product.getcol('FIELD_ID'), [0] * 5 + [1] * 5)
            tables[name] = table
        # CASA overwrites the existing field's fit with the new field's fit.
        with opened(snapshots['casa']) as before, opened(tables['casa']) as after:
            np.testing.assert_array_equal(before.getcol('FIELD_ID'), [0] * 5 + [1] * 5)
            for name in ('SPLINE_KNOTS_AMP', 'SPLINE_KNOTS_PHASE', 'POLY_COEFF_AMP', 'POLY_COEFF_PHASE'):
                np.testing.assert_array_equal(before.getcell(name, 1), before.getcell(name, 6), err_msg=name)
                assert not np.array_equal(before.getcell(name, 1), after.getcell(name, 1)), name
                np.testing.assert_array_equal(after.getcell(name, 1), after.getcell(name, 6), err_msg=name)
        if 'native' in tables:
            compare_tables(snapshots['casa'], snapshots['native'], (range(5), range(5)))
            compare_tables(snapshots['native'], tables['native'], (range(5), range(5)))
            compare_tables(tables['casa'], tables['native'], (range(5, 10), range(5, 10)))
            shutil.copytree(tables['native'], work / 'before-replace-native.cal')
            solve(work, vis, solvers['native'], 'append-native', 'p',
                  field='PHASE', splinetime=90., append=True)
            with opened(tables['native']) as table:
                assert table.nrows() == 10
                assert table.getcell('POLY_MODE', 6) == 'PHAS'
                assert table.getcell('N_KNOTS_PHASE', 6) == 8
            compare_tables(work / 'before-replace-native.cal', tables['native'], (range(5), range(5)))


def check_single_hand(root, solvers, appliers):
    for correlation in ('XX', 'YY'):
        work = root / correlation
        work.mkdir()
        vis = work / 'source.ms'
        make_cal_ms(str(vis), channels=4, scan_seconds=80, stokes=correlation)
        with opened(vis, True) as table:
            data = table.getcol('DATA')
            data[:] = 1.2
            table.putcol('DATA', data)
            table.putcol('CORRECTED_DATA', data)
        tables = {}
        with chdir(work):
            for name, task in solvers.items():
                tables[name] = solve(work, vis, task, name, 'ap', splinetime=60.)[0]
                with opened(tables[name]) as table:
                    count = table.getcell('N_POLY_AMP', 1)
                    controls = table.getcell('POLY_COEFF_AMP', 1).reshape(2, count)
                    np.testing.assert_allclose(controls[0, :-4], .5 * np.log(1.2), atol=3e-7)
                    assert not controls[1].any()
        for name, task in appliers.items():
            data, flags = apply(work, vis, task, tables['casa'], name + '-apply')
            np.testing.assert_allclose(data[~flags], 1., atol=5e-7, rtol=5e-7)
        if 'native' in tables:
            compare_tables(tables['casa'], tables['native'])


def check_apply_edges(root, appliers):
    work = root / 'apply-controls'
    work.mkdir()
    source_ms = root / 'combined/source.ms'
    solution = root / 'combined/casa.cal'
    operand = work / 'outside.ms'
    shutil.copytree(source_ms, operand)
    with opened(operand, True) as table:
        last = table.nrows() - 1
        table.putcell('TIME', 0, table.getcell('TIME', 0) - 100.)
        table.putcell('TIME', last, table.getcell('TIME', last) + 100.)
        original = table.getcol('DATA')[..., [0, -1]]
    shifted = work / 'shifted.ms'
    shutil.copytree(source_ms, shifted)
    with opened(shifted / 'SPECTRAL_WINDOW', True) as table:
        for name in ('CHAN_FREQ', 'REF_FREQUENCY'):
            table.putcol(name, 1.03 * table.getcol(name))
    expected = {}
    for backend, task in appliers.items():
        data, flags = apply(work, operand, task, solution, f'{backend}-outside')
        assert not flags.any()
        np.testing.assert_allclose(data[..., [0, -1]], original, atol=3e-7)
        np.testing.assert_allclose(data[..., 1:-1], 1., atol=6e-7)
        data, flags = apply(work, source_ms, task, solution, f'{backend}-ignored',
                            gainfield=['1'], interp=['nonsense'], spwmap=[4, 3, 2])
        assert not flags.any()
        np.testing.assert_allclose(data, 1., atol=6e-7)
        expected[backend] = apply(work, shifted, task, solution, f'{backend}-frequency')
        callib = work / f'{backend}.callibrary'
        callib.write_text(f"caltable='{solution}' tinterp='linear' calwt=True\n")
        try:
            task(str(shifted), docallib=True, callib=str(callib), flagbackup=False)
        except RuntimeError as error:
            assert 'callib' in str(error).lower() or 'cal library' in str(error).lower()
        else:
            raise AssertionError('The legacy GSPLINE table was accepted by cal library.')
    assert np.max(np.abs(expected['casa'][0] - 1.)) > 1e-3
    if 'native' in expected:
        np.testing.assert_array_equal(expected['casa'][1], expected['native'][1])
        np.testing.assert_allclose(expected['native'][0], expected['casa'][0], atol=6e-7)


def check_selection_model_and_prior(root, solvers, appliers):
    work = root / 'selection-model-prior'
    work.mkdir()
    source_ms = root / 'combined/source.ms'
    prior = root / 'combined/casa.cal'
    physical = work / 'physical.ms'
    shutil.copytree(source_ms, physical)
    with opened(physical, True) as table:
        data = table.getcol('DATA')
        # Put a frequency-dependent physical model into scratch storage.
        model = np.broadcast_to(np.linspace(1.4, 2.1, data.shape[1])[None, :, None], data.shape)
        table.putcol('MODEL_DATA', model.astype(data.dtype))
        table.putcol('DATA', data * model)
        table.putcol('CORRECTED_DATA', data * model)
    with opened(physical / 'SPECTRAL_WINDOW') as table:
        expected_frequency = table.getcell('CHAN_FREQ', 0)[5:].mean()
    results = {}
    with chdir(work):
        for backend, task in solvers.items():
            physical_tab, _ = solve(work, physical, task, f'{backend}-physical', 'ap',
                                    spw='0:5~7', smodel=[], splinetime=60.)
            with opened(physical_tab) as table:
                np.testing.assert_allclose(table.getcell('REF_FREQUENCY', 0), expected_frequency,
                                           atol=.01, rtol=0.)
            residual, _ = solve(work, physical, task, f'{backend}-prior', 'ap',
                                smodel=[], gaintable=[str(prior)], splinetime=60.)
            with opened(residual) as table:
                for column in ('POLY_COEFF_AMP', 'POLY_COEFF_PHASE'):
                    np.testing.assert_allclose(table.getcol(column), 0., atol=8e-7)
            results[backend] = (physical_tab, residual)
    if 'native' in results:
        for reference, actual in zip(results['casa'], results['native'], strict=True):
            compare_tables(reference, actual)
    selected = {backend: apply(work, source_ms, task, prior, f'{backend}-selected-apply',
                               spw='0:5~7') for backend, task in appliers.items()}
    assert np.max(np.abs(selected['casa'][0][:, 5:, :] - 1.)) > 1e-6
    if 'native' in selected:
        np.testing.assert_array_equal(selected['native'][1], selected['casa'][1])
        np.testing.assert_allclose(selected['native'][0], selected['casa'][0], atol=6e-7, rtol=6e-7)


def check_partial_channel_flags(root, solvers):
    work = root / 'partial-channel-flags'
    work.mkdir()
    with chdir(work):
        vis, _ = source(work, 'ap', noisy=True)
        with opened(vis, True) as table:
            flags = table.getcol('FLAG')
            flags[1, :4, ::7] = True
            flags[0, 2:4, ::3] = True
            table.putcol('FLAG', flags)
            data = table.getcol('DATA')
            model = np.broadcast_to(np.linspace(1.4, 2.1, data.shape[1])[None, :, None], data.shape)
            table.putcol('MODEL_DATA', model.astype(data.dtype))
            table.putcol('DATA', data * model)
            table.putcol('CORRECTED_DATA', data * model)
        for preavg in (0., 20.):
            results = {}
            for corrdepflags in (False, True):
                tables = {name: solve(work, vis, task, f'{name}-{preavg}-{corrdepflags}', 'ap',
                                      preavg=preavg, corrdepflags=corrdepflags,
                                      smodel=[], splinetime=60.)[0] for name, task in solvers.items()}
                results[corrdepflags] = tables['casa']
                if 'native' in tables:
                    compare_tables(tables['casa'], tables['native'])
            compare_tables(results[False], results[True])


def check_no_shared_samples(root, solvers):
    work = root / 'no-shared-samples'
    work.mkdir()
    with chdir(work):
        vis, _ = source(work, 'ap')
        with opened(vis, True) as table:
            flags = table.getcol('FLAG')
            flags[1] = True
            table.putcol('FLAG', flags)
        options = dict(vis=str(vis), caltable=str(work / 'casa.cal'), gaintype='GSPLINE',
                       smodel=[1, 0, 0, 0], splinetime=60., refant='A0')
        casa_result = subprocess.run([sys.executable, '-c',
            'import json,resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
            'from casatasks import gaincal; gaincal(**json.load(sys.stdin))'],
            input=json.dumps(options), text=True, capture_output=True, timeout=30)
        assert casa_result.returncode in (-6, -11), (casa_result.returncode, casa_result.stderr)
        assert not (work / 'casa.cal').exists()
        if 'native' in solvers:
            table, _ = solve(work, vis, solvers['native'], 'native', 'ap', splinetime=60.)
            assert not table.exists()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--solve-only', action='store_true')
    parser.add_argument('--edge-cases', action='store_true')
    parser.add_argument('--benchmark', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    solvers, appliers = {'casa': casa.gaincal}, {'casa': casa.applycal}
    if not args.reference_only:
        from casars.tasks import applycal, gaincal

        solvers['native'] = gaincal
        if not args.solve_only:
            appliers['native'] = applycal
    if args.benchmark:
        benchmark(root, solvers)
        return
    for name, mode in (('amplitude', 'a'), ('phase', 'p'), ('combined', 'ap')):
        check_case(root, name, mode, solvers, appliers, splinetime=60.)
    check_case(root, 'weighted', 'ap', solvers, appliers, noisy=True, splinetime=60.)
    check_case(root, 'wrap', 'p', solvers, appliers, noisy=True, wraps=True,
               splinetime=70., npointaver=4, phasewrap=145.)
    if args.edge_cases:
        check_solve_edges(root, solvers)
        check_single_hand(root, solvers, appliers)
        check_apply_edges(root, appliers)
        check_selection_model_and_prior(root, solvers, appliers)
        check_partial_channel_flags(root, solvers)
        check_no_shared_samples(root, solvers)
    print('GSPLINE coefficient, science and interoperability checks passed.', flush=True)


if __name__ == '__main__':
    main()
