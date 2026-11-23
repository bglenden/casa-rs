#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 parity for non-closing baseline responses and their consumers."""

import argparse
from contextlib import chdir
import inspect
from pathlib import Path
import time

import casatasks as casa
from casatools import table as casa_table
import numpy as np

from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms


def source(root, stokes='RR LL', label='baseline'):
    vis = root / f'{label}-{len(stokes.split())}.ms'
    make_cal_ms(str(vis), channels=16, scan_seconds=80, integration_seconds=4,
                stokes=stokes, frequency='8.4GHz', autocorrelation_weight=1.)
    with opened(vis, True) as table:
        p, q, epochs = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2', 'TIME'))
        baseline = (p + 1) * (q + 1)
        channel = np.arange(16)[None, :, None]
        product = np.arange(len(stokes.split()))[:, None, None]
        amplitude = 1.1 + .021 * baseline + .013 * product + .008 * channel
        phase = -.25 + .017 * baseline + .014 * product + .009 * channel
        model = 1. + .02 * channel + .02 * epochs / epochs.max()
        model = np.broadcast_to(model, amplitude.shape).astype(np.complex64).copy()
        data = (model * amplitude * np.exp(1j * phase)).astype(np.complex64)
        flags = np.zeros(data.shape, bool)
        flags[0, 3:5, (p == 0) & (q == 2)] = True
        flags[..., (p == 1) & (q == 4)] = True
        for name, values in (
            ('DATA', data), ('CORRECTED_DATA', data), ('MODEL_DATA', model),
            ('FLAG', flags), ('FLAG_ROW', flags.all(axis=(0, 1))),
        ):
            table.putcol(name, values)
    return vis


def nrms(actual, expected):
    power = np.linalg.norm(expected)
    return np.linalg.norm(actual - expected) / power if power else np.linalg.norm(actual)


def compare_tables(reference, actual, *, flagged_rtol=0., time_atol=1e-5):
    with opened(reference) as expected, opened(actual) as candidate:
        assert expected.info() == candidate.info(), (expected.info(), candidate.info())
        assert expected.colnames() == candidate.colnames()
        assert candidate.getkeyword('VisCal') == expected.getkeyword('VisCal')
        assert candidate.getkeyword('ParType') == 'Complex'
        assert not candidate.iscelldefined('WEIGHT', 0)
        keys = ('ANTENNA2', 'ANTENNA1', 'TIME', 'SPECTRAL_WINDOW_ID', 'FIELD_ID', 'OBSERVATION_ID')
        left, right = [np.lexsort([table.getcol(key) for key in keys])
                       for table in (expected, candidate)]
        for name in (*keys, 'SCAN_NUMBER', 'INTERVAL', 'PARAMERR', 'SNR', 'FLAG'):
            reference_values = expected.getcol(name)[..., left]
            actual_values = candidate.getcol(name)[..., right]
            if name == 'TIME':
                np.testing.assert_allclose(actual_values, reference_values, atol=time_atol, rtol=0.)
            else:
                np.testing.assert_array_equal(actual_values, reference_values, err_msg=name)
        flag = expected.getcol('FLAG')[..., left]
        known, fit = [table.getcol('CPARAM')[..., order]
                      for table, order in ((expected, left), (candidate, right))]
        error = nrms(fit[~flag], known[~flag])
        assert error < 2e-6, (actual.name, error)
        np.testing.assert_allclose(fit[flag], known[flag], rtol=flagged_rtol, atol=0.)
    for name in ('ANTENNA', 'FIELD', 'OBSERVATION', 'HISTORY', 'SPECTRAL_WINDOW'):
        with opened(reference/name) as expected, opened(actual/name) as candidate:
            assert expected.nrows() == candidate.nrows(), name
            if name == 'SPECTRAL_WINDOW':
                for row in range(expected.nrows()):
                    for key in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'RESOLUTION',
                                'EFFECTIVE_BW', 'TOTAL_BANDWIDTH', 'FLAG_ROW'):
                        np.testing.assert_allclose(candidate.getcell(key, row),
                                                   expected.getcell(key, row), atol=1e-6, rtol=1e-12,
                                                   err_msg=f'{actual.name} {name} {row} {key}')
    return error


def solve(root, vis, tasks, label, *, positional=False, **options):
    outputs = {}
    for engine, task in tasks.items():
        path = root / f'{label}-{engine}.cal'
        start = time.perf_counter()
        if positional:
            params = inspect.signature(task).parameters
            args = {'vis': str(vis), 'caltable': str(path), **options}
            result = task(*(args.get(name, parameter.default) for name, parameter in params.items()
                            if parameter.kind is not inspect.Parameter.KEYWORD_ONLY))
        else:
            result = task(vis=str(vis), caltable=str(path), **options)
        print(label, engine, f'{time.perf_counter()-start:.6f}s', flush=True)
        assert result is None and path.is_dir(), (label, engine, result)
        outputs[engine] = path
    if 'native' in outputs:
        print(label, 'solution NRMS', compare_tables(outputs['casa'], outputs['native']), flush=True)
    return outputs


def check_frequency_law(vis, product):
    with opened(vis) as table:
        model = table.getcol('MODEL_DATA')
        data = table.getcol('DATA')
        p, q = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2')]
    with opened(product) as table:
        assert table.nrows() == 15
        first, second = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2')]
        for row, (a1, a2) in enumerate(zip(first, second)):
            index = np.flatnonzero((p == a1) & (q == a2))[0]
            gain = table.getcell('CPARAM', row)
            valid = ~table.getcell('FLAG', row)
            np.testing.assert_allclose(gain[valid], (data/model)[..., index][valid],
                                       rtol=2e-6, atol=0.)


def copy_ms(source, destination):
    with opened(source) as table:
        table.copy(str(destination), deep=True, valuecopy=True).close()


def reverse_baseline(vis):
    with opened(vis, True) as table:
        p, q = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2')]
        reversed_rows = (p == 0) & (q == 3)
        for name, data, partner in (('ANTENNA1', p, q), ('ANTENNA2', q, p.copy())):
            values = data.copy()
            values[reversed_rows] = partner[reversed_rows]
            table.putcol(name, values)
        for name in ('DATA', 'CORRECTED_DATA', 'MODEL_DATA'):
            values = table.getcol(name)
            values[..., reversed_rows] = values[..., reversed_rows].conj()
            table.putcol(name, values)
        uvw = table.getcol('UVW')
        uvw[..., reversed_rows] *= -1.
        table.putcol('UVW', uvw)


def apply(root, label, vis, products, tasks, *, reversed=False, analytic=False, **options):
    reference = None
    for producer, product in products.items():
        for consumer, task in tasks.items():
            if reversed and consumer == 'casa':
                continue
            destination = root/f'apply-{label}-{producer}-{consumer}.ms'
            copy_ms(vis, destination)
            if reversed:
                reverse_baseline(destination)
            assert task(str(destination), field='FLUX', gaintable=[str(product)], calwt=True,
                        flagbackup=False, **options) is None
            with opened(destination) as table:
                data, flag, weight = [table.getcol(name) for name
                                      in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')]
                if analytic:
                    selected = (table.getcol('FIELD_ID') == 0)[None, None, :] & ~flag
                    model = table.getcol('MODEL_DATA')
                    assert nrms(data[selected], model[selected]) < 2e-6
            if reference is None:
                reference = data, flag, weight
            else:
                expected, expected_flag, expected_weight = reference
                np.testing.assert_array_equal(flag, expected_flag)
                np.testing.assert_array_equal(weight, expected_weight)
                error = nrms(data[~flag], expected[~flag])
                assert error < 2e-6, (label, producer, consumer, error)
                print(label, producer, consumer, 'corrected NRMS', error, flush=True)


def weighted_windows(root, original):
    root.mkdir(exist_ok=True)
    output = root/'weighted-windows.ms'
    copy_ms(original, output)
    for path in (output/'SPECTRAL_WINDOW', output/'DATA_DESCRIPTION', output/'OBSERVATION', output):
        count = duplicate_rows(casa_table(), path)
    with opened(output/'DATA_DESCRIPTION', True) as table:
        table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(output/'SPECTRAL_WINDOW', True) as table:
        for name in ('CHAN_FREQ', 'REF_FREQUENCY'):
            table.putcell(name, 1, table.getcell(name, 0) + 48e6)
    with opened(output, True) as table:
        for name in ('OBSERVATION_ID', 'DATA_DESC_ID'):
            table.putcol(name, np.ones(count, dtype=np.int32), count, count)
        for name in ('TIME', 'TIME_CENTROID'):
            table.putcol(name, table.getcol(name, count, count) + 200., count, count)
        for name in ('DATA', 'CORRECTED_DATA'):
            table.putcol(name, table.getcol(name, count, count) * (1.12+.21j), count, count)
        sigma = table.getcol('SIGMA')
        sigma[..., count:] *= .5
        table.putcol('SIGMA', sigma)
        table.putcol('WEIGHT', 1./sigma**2)
    return output


def selected_inputs(root, original):
    output = root/'selected.ms'
    copy_ms(original, output)
    with opened(output/'STATE', True) as table:
        first = table.nrows()
        table.addrows(2)
        table.putcell('OBS_MODE', first, 'CALIBRATE_BANDPASS#ON_SOURCE')
        table.putcell('OBS_MODE', first+1, 'CALIBRATE_PHASE#ON_SOURCE')
    with opened(output, True) as table:
        times = table.getcol('TIME')
        is_bandpass = times < np.median(times)
        table.putcol('STATE_ID', np.where(is_bandpass, first, first+1).astype(np.int32))
    return output


def missing_terms(root, vis, source, appliers):
    # A solution whose first hand is invalid, and one whole absent pair.
    output = root/'missing-baseline.cal'
    with opened(source) as table:
        table.copy(str(output), deep=True, valuecopy=True).close()
    with opened(output, True) as table:
        p, q = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2')]
        remove = np.flatnonzero((p == 0) & (q == 3))
        row = int(np.flatnonzero((p == 0) & (q == 2))[0])
        flag = table.getcell('FLAG', row)
        flag[0, 0] = True
        table.putcell('FLAG', row, flag)
        table.removerows(remove.tolist())
    apply(root, 'missing-baseline', vis, {'casa': output}, appliers)


def cross_hand_weights(root, original, tasks):
    windows = weighted_windows(root/'cross-weighted', original)
    with opened(windows, True) as table:
        later = table.getcol('OBSERVATION_ID') == 1
        sigma = np.ones_like(table.getcol('SIGMA'))
        sigma[1:3, later] = .1
        table.putcol('SIGMA', sigma)
        table.putcol('WEIGHT', 1. / sigma**2)
        times = table.getcol('TIME')[(table.getcol('FIELD_ID') == 0) & later]
    outputs = solve(root, windows, tasks, 'cross-hand-centroid', field='FLUX',
                    freqdep=True, combine='obs,spw,scan')
    for path in outputs.values():
        with opened(path) as table:
            # Four unit-weight hands 200 s earlier; the later buffer has
            # two unit-weight hands and two with weight 100.
            expected = times.mean() - 200. * 4. / (4. + 2. + 200.)
            np.testing.assert_allclose(table.getcol('TIME'), expected, atol=2e-5, rtol=0.)

    cross_only = root/'cross-hands-only.ms'
    copy_ms(original, cross_only)
    with opened(cross_only, True) as table:
        sigma = table.getcol('SIGMA')
        sigma[[0, 3]] = 0.
        table.putcol('SIGMA', sigma)
        table.putcol('WEIGHT', np.divide(1., sigma**2, out=np.zeros_like(sigma), where=sigma > 0.))
    for engine, task in tasks.items():
        table_path = root/f'cross-only-{engine}.cal'
        assert task(str(cross_only), str(table_path), field='FLUX', freqdep=True) is None
        with opened(table_path) as table:
            assert table.nrows() == 15
            flag, gain = table.getcol('FLAG'), table.getcol('CPARAM')
            if engine == 'casa':
                assert (~flag).any() and np.isnan(gain[~flag]).all()
            else:
                assert flag.all() and np.all(gain == 0.)


def repeated_field_visits(root, original, tasks):
    output = root/'repeated-field-visits.ms'
    copy_ms(original, output)
    count = duplicate_rows(casa_table(), output)
    with opened(output, True) as table:
        for name in ('TIME', 'TIME_CENTROID'):
            table.putcol(name, table.getcol(name, count, count) + 200., count, count)
        table.putcol('SCAN_NUMBER', table.getcol('SCAN_NUMBER', count, count) + 2, count, count)
        table.putcol('DATA', 1.31 * table.getcol('DATA', count, count), count, count)
    for label, combination, interval, count in (
        ('keep-field-breaks', 'scan', 'inf', 60),
        ('finite-field-visits', 'scan', '95s', 60),
        ('combine-field-visits', 'field,scan', 'inf', 15),
        ('finite-combined-field-visits', 'field,scan', '95s', 60),
    ):
        paths = solve(root, output, tasks, label, combine=combination, solint=interval, freqdep=True)
        for path in paths.values():
            with opened(path) as table:
                assert table.nrows() == count, label
    solve(root, output, tasks, 'selected-field-visits', field='FLUX', freqdep=True)


def mixed_correlations(root, two, four, tasks):
    shifted = root/'shifted-four.ms'
    copy_ms(four, shifted)
    with opened(shifted/'SPECTRAL_WINDOW', True) as table:
        for name in ('CHAN_FREQ', 'REF_FREQUENCY'):
            table.putcell(name, 0, table.getcell(name, 0) + 48e6)
    mixed = root/'mixed-correlations.ms'
    assert casa.concat([str(two), str(shifted)], str(mixed)) is None
    for engine, task in tasks.items():
        output = root/f'mixed-correlations-{engine}.cal'
        try:
            task(str(mixed), str(output), field='FLUX', freqdep=True, combine='obs,spw,scan')
        except RuntimeError as error:
            assert str(error) == 'Error in Calibrater::solve.'
        else:
            raise AssertionError(f'{engine} accepted buffers with different correlation counts')
        assert not output.exists()


def zero_samples(root, original, tasks):
    output = root/'zero-samples.ms'
    copy_ms(original, output)
    with opened(output, True) as table:
        p, q = [table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2')]
        data, model = [table.getcol(key) for key in ('DATA', 'MODEL_DATA')]
        data[0, 0, (p == 0) & (q == 1)] = 0.
        model[1, 7, (p == 2) & (q == 3)] = 0.
        table.putcol('DATA', data)
        table.putcol('CORRECTED_DATA', data)
        table.putcol('MODEL_DATA', model)
    for label, options in (
        ('defined-zero', {}), ('defined-zero-normalized', {'solnorm': True}),
        ('defined-zero-phase', {'calmode': 'p'}),
    ):
        solve(root, output, tasks, label, field='FLUX', freqdep=True, **options)


def refusal_cases(root, vis, tasks):
    cases = (
        {'vis': ''}, {'caltable': ''}, {'caltable': None}, {'caltable': 42},
        {'calmode': 'pa'}, {'calmode': None}, {'calmode': 2},
        {'freqdep': 'yes'}, {'solint': 'abc'}, {'freqdep': True, 'solint': 'inf,bar'},
        {'spwmap': 1}, {'spwmap': {1: 2}}, {'observation': [0]},
        {'msselect': 'ANTENNA1<0'}, {'uvrange': [1, 8]},
        {'gaintable': str(root/'nonexistent.cal')},
    )
    for index, options in enumerate(cases):
        errors = {}
        output = root / f'refusal-{index}.cal'
        for name, task in tasks.items():
            arguments = {'vis': str(vis), 'caltable': str(output), **options}
            try:
                task(**arguments)
            except Exception as error:
                errors[name] = type(error).__name__, str(error)
            else:
                raise AssertionError((name, arguments, 'expected an exception'))
            assert not output.exists()
        if 'native' in errors:
            assert errors['native'] == errors['casa'], (options, errors)
    print(f'{len(cases)} paired argument and runtime refusals', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--casa-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    solvers = {'casa': casa.blcal}
    appliers = {'casa': casa.applycal}
    if not args.casa_only:
        import casars.tasks as native
        solvers['native'] = native.blcal
        appliers['native'] = native.applycal
        def signature(task):
            return [(item.name, item.kind, item.default)
                    for item in inspect.signature(task).parameters.values()
                    if item.kind is not inspect.Parameter.KEYWORD_ONLY]
        assert signature(native.blcal) == signature(casa.blcal)
    root = args.output.resolve()
    root.mkdir()
    with chdir(root):
        vis = source(root)
        scalar = solve(root, vis, solvers, 'independent', field='FLUX', positional=True)
        frequency = solve(root, vis, solvers, 'frequency', field='FLUX', freqdep=True)
        for product in frequency.values():
            check_frequency_law(vis, product)
        apply(root, 'M', vis, scalar, appliers)
        apply(root, 'Mf', vis, frequency, appliers, analytic=True)
        if 'native' in appliers:
            apply(root, 'reversed', vis, frequency, {'native': appliers['native']}, reversed=True,
                  analytic=True)
            reversed_vis = root/'reversed-solve.ms'
            copy_ms(vis, reversed_vis)
            reverse_baseline(reversed_vis)
            output = root/'reversed-solve.cal'
            assert solvers['native'](str(reversed_vis), str(output), field='FLUX',
                                     freqdep=True) is None
            print('reversed-solve solution NRMS', compare_tables(frequency['casa'], output),
                  flush=True)
        for label, options in (
            ('phase', {'calmode': 'p'}), ('amplitude', {'calmode': 'a'}),
            ('normalized-M', {'solnorm': True}),
            ('normalized-Mf', {'solnorm': True, 'freqdep': True}),
            ('time-bin', {'solint': '13s'}), ('integration', {'solint': 'int'}),
            ('channels', {'spw': '0:3~8', 'freqdep': True}),
            ('bins', {'solint': 'inf,4chan', 'freqdep': True}),
            ('frequency-width', {'solint': 'inf,3MHz', 'freqdep': True}),
            ('parang', {'parang': True, 'freqdep': True}),
            ('baseline-selection', {'antenna': '0&2'}),
            ('uvrange', {'uvrange': '>0.001m'}),
            ('taql', {'msselect': 'ANTENNA1==0 && ANTENNA2==2'}),
            ('ignored-selectors', {'selectdata': False, 'antenna': 'no-match', 'scan': '989',
                                  'observation': 99, 'msselect': 'ANTENNA1==-1',
                                  'timerange': '>2100/1/1', 'uvrange': '>100000km'}),
        ):
            solve(root, vis, solvers, label, field='FLUX', **options)
        zero_samples(root, vis, solvers)
        for stokes in ('RR', 'RR RL LL', 'RR RL LR LL'):
            source_vis = source(root, stokes)
            products = solve(root, source_vis, solvers, f'products-{len(stokes.split())}',
                             field='FLUX', freqdep=True)
            if len(stokes.split()) != 3:
                apply(root, f'products-{len(stokes.split())}', source_vis, products, appliers)
            if len(stokes.split()) == 4:
                missing_terms(root, source_vis, products['casa'], appliers)
                cross_hand_weights(root, source_vis, solvers)
                mixed_correlations(root, vis, source_vis, solvers)
        for label, options in (
            ('separate-scans-fields', {'combine': ''}),
            ('combine-field', {'combine': 'field,scan'}),
        ):
            solve(root, vis, solvers, label, **options)
        repeated_field_visits(root, vis, solvers)
        selected = selected_inputs(root, vis)
        solve(root, selected, solvers, 'intent', intent='*BANDPASS*')
        solve(root, selected, solvers, 'scan', scan='2')
        with opened(selected) as table:
            epoch = table.getcol('TIME').min()
        # MJD seconds given to the tools' measure parser, in an absolute range.
        from casatools import quanta
        qa = quanta()
        start, end = [qa.time(qa.quantity(value, 's'), form='ymd', prec=6)[0]
                      for value in (epoch + 6, epoch + 30)]
        solve(root, selected, solvers, 'time-selection', timerange=f'{start}~{end}')
        windows = weighted_windows(root, vis)
        for label, options in (
            ('observation', {'observation': 1, 'spw': '1:2~12', 'freqdep': True}),
            ('combine-obs', {'combine': 'obs,scan'}),
            ('combine-all', {'combine': 'obs,field,spw,scan'}),
            ('combine-all-Mf', {'combine': 'obs,field,spw,scan', 'freqdep': True}),
        ):
            solve(root, windows, solvers, label, **options)
        for label, prior in (
            ('baseline-prior', scalar['casa']), ('frequency-prior', frequency['casa']),
        ):
            solve(root, windows, solvers, label, field='FLUX', spw='1', freqdep=True,
                  gaintable=str(prior), gainfield='0', interp='nearest,linear', spwmap=[0, 0])
        antenna_prior = root/'antenna-prior.cal'
        casa.gencal(str(vis), str(antenna_prior), 'amp', parameter=[1.2])
        solve(root, windows, solvers, 'prior-chain', field='FLUX', spw='1', freqdep=True,
              gaintable=[str(antenna_prior), str(frequency['casa'])],
              gainfield=['', '0'], interp=['', 'nearest,linear'], spwmap=[[0, 0], [0, 0]],
              parang=True)
        refusal_cases(root, vis, solvers)
    print('BLCAL parity and interoperability passed', flush=True)


if __name__ == '__main__':
    main()
