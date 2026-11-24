#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input Python task checks for routine G/T gaincal."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_lifecycle_oracle import contents
from record_casa_returns import make_cal_ms


def solve(module, root, label, source, refant='A0', minsnr=0.,
          smodel=(1., 0., 0., 0.), **options):
    caltable = root/f'{label}.cal'
    preload = os.environ.pop('LD_PRELOAD', None) if module is not casa else None
    try:
        module.gaincal(vis=str(source), caltable=str(caltable), refant=refant,
                       smodel=list(smodel), minsnr=minsnr, **options)
    finally:
        if preload is not None:
            os.environ['LD_PRELOAD'] = preload
    return contents(caltable)


def relative_rms(a, b):
    return float(np.linalg.norm(a-b)/max(np.linalg.norm(b), 1e-30))


def alternating_fields(root, source, native, first, calmode='ap', short_interval=False):
    label = f'combined-first-field-{first}'
    case = root/f'{label}.ms'
    copy_ms(source, case)
    t = table()
    t.open(str(case), nomodify=False)
    try:
        t0, index = np.unique(t.getcol('TIME'), return_inverse=True)
        epoch = t0.min()
        times = epoch + 10.*index
        t.putcol('TIME', times)
        t.putcol('TIME_CENTROID', times)
        fields = np.zeros(len(index), dtype=np.int32) if short_interval else ((first+index)%2).astype(np.int32)
        t.putcol('FIELD_ID', fields)
        t.putcol('SCAN_NUMBER', np.full(len(index), 1, dtype=np.int32))
        data = t.getcol('DATA')
        data[:] = (1.+.2*index)[None, None, :]
        if calmode == 'a':
            a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
            phase = (np.pi/2) * (index%2) * ((a==1).astype(int)-(b==1).astype(int))
            data[:] *= (1.+.08*(a==2)*(b==4))*np.exp(1j*phase)
        t.putcol('DATA', data)
        t.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        t.putcol('FLAG_ROW', np.zeros(len(index), dtype=bool))
    finally:
        t.close()

    options = dict(gaintype='G', calmode=calmode, solint='25s', combine='field')
    if calmode == 'a':
        options['preavg'] = 20.
    if short_interval:
        options.update(solint='10.005s', combine='')
    casa_input, native_input = root/f'{label}-casa.ms', root/f'{label}-native.ms'
    copy_ms(case, casa_input)
    copy_ms(case, native_input)
    expected = solve(casa, root, label+'-casa', casa_input, **options)
    actual = solve(native, root, label+'-native', native_input, **options)
    records = {}
    for name, rows in [('casa', expected), ('native', actual)]:
        times = np.unique(rows['TIME'])
        records[name] = dict(
            interval_times_s=list(times-epoch),
            fields_by_time=[list(map(int, np.unique(rows['FIELD_ID'][rows['TIME']==time])))
                            for time in times],
            row_count=len(rows['FIELD_ID']),
        )
    # Both field orders produce the same five bins, anchored at the first
    # selected time. In each bin both fields occur. CASA labels it with field 0.
    if not short_interval:
        np.testing.assert_allclose(records['casa']['interval_times_s'], [10., 35., 60., 85., 105.], atol=1e-4)
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    for column in ['FIELD_ID', 'ANTENNA1', 'SCAN_NUMBER', 'SPECTRAL_WINDOW_ID', 'FLAG']:
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    records['cparam_relative_rms'] = relative_rms(actual['CPARAM'], expected['CPARAM'])
    assert records['cparam_relative_rms'] < 1e-3, records
    if calmode == 'a':
        for name in ['PARAMERR', 'SNR']:
            error = relative_rms(actual[name], expected[name])
            assert error < 1e-3, (name, error)
            records[name+'_relative_rms'] = error
    return records


def amplitude_data(root, source):
    path = root/'amplitude.ms'
    copy_ms(source, path)
    t = table()
    t.open(str(path), nomodify=False)
    try:
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        _, time = np.unique(t.getcol('TIME'), return_inverse=True)
        data = t.getcol('DATA')
        channel = np.arange(data.shape[1])[:, None]
        receptor = np.arange(data.shape[0])[:, None, None]
        amplitude = (1.+.13*a)*(1.+.13*b)*(1.+.05*(a==1)*(b==3))
        phase = (.09+.026*channel+.023*time)*(a-b)+.065*(a==2)*(b==4)
        data[:] = amplitude*(1.+.09*receptor)*np.exp(1j*(phase+.04*receptor*(a-b)))
        t.putcol('DATA', data)
        t.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        t.putcol('FLAG_ROW', np.zeros(len(a), dtype=bool))
    finally:
        t.close()
    return path


def gain_and_apply(root, source, native, gaintype, solint, solnorm=False, normtype='mean',
                   calmode='a', compare_flagged_gains=True, apply_options=None,
                   sort_solutions=False, native_source=None,
                   **extra):
    label = f'{calmode}-{gaintype}-{solint.replace(",", "-")}-{normtype}-norm{int(solnorm)}'
    if 'preavg' in extra:
        label += f'-preavg{extra["preavg"]:g}'
    options = dict(gaintype=gaintype, calmode=calmode, solint=solint, solnorm=solnorm,
                   normtype=normtype, **extra)
    solutions, corrected = [], []
    for engine, module in [('casa', casa), ('native', native)]:
        case = f'{label}-{engine}'
        vis = root/f'{case}.ms'
        copy_ms(native_source if engine == 'native' and native_source is not None else source, vis)
        solution = solve(module, root, case, vis, **options)
        if sort_solutions:
            # Different observing blocks can have the same solution time.
            # Compare their rows by observing identities as well as by time.
            keys = ['ANTENNA1', 'TIME', 'SCAN_NUMBER', 'FIELD_ID', 'SPECTRAL_WINDOW_ID',
                    'OBSERVATION_ID']
            order = np.lexsort([solution[key] for key in keys])
            solution = {name: values[..., order] for name, values in solution.items()}
        solutions.append(solution)
        preload = os.environ.pop('LD_PRELOAD', None) if module is native else None
        try:
            module.applycal(vis=str(vis), gaintable=[str(root/f'{case}.cal')], flagbackup=False,
                            **(apply_options or {}))
        finally:
            if preload is not None:
                os.environ['LD_PRELOAD'] = preload
        corrected.append(contents(vis))

    expected, actual = solutions
    for name in ['FIELD_ID', 'OBSERVATION_ID', 'ANTENNA1', 'ANTENNA2', 'SCAN_NUMBER',
                 'SPECTRAL_WINDOW_ID', 'FLAG']:
        np.testing.assert_array_equal(actual[name], expected[name], err_msg=f'{label} {name}')
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    assert np.any(expected['PARAMERR'] > 0.)
    if calmode == 'a':
        assert np.all(expected['CPARAM'].real > 0.)
        np.testing.assert_allclose(actual['CPARAM'].imag, 0., rtol=0., atol=1e-7)
    errors = {name: relative_rms(actual[name], expected[name])
              for name in ['CPARAM', 'PARAMERR', 'SNR']}
    raw_cparam = errors['CPARAM']
    if not compare_flagged_gains:
        valid = ~expected['FLAG']
        errors['CPARAM'] = relative_rms(actual['CPARAM'][valid], expected['CPARAM'][valid])
    if extra.get('refant') == '' and calmode != 'a':
        # With no reference, a constant phase per receptor and solution
        # interval leaves the baseline gain products invariant.
        aligned = actual['CPARAM'].copy()
        for time in np.unique(expected['TIME']):
            rows = expected['TIME'] == time
            left, right = aligned[..., rows], expected['CPARAM'][..., rows]
            product = np.where(expected['FLAG'][..., rows], 0., left.conj()*right)
            aligned[..., rows] *= np.exp(1j*np.angle(product.sum(axis=-1, keepdims=True)))
        errors['CPARAM'] = relative_rms(aligned, expected['CPARAM'])
    expected_ms, actual_ms = corrected
    errors['CORRECTED_DATA'] = relative_rms(actual_ms['CORRECTED_DATA'], expected_ms['CORRECTED_DATA'])
    errors['WEIGHT'] = relative_rms(actual_ms['WEIGHT'], expected_ms['WEIGHT'])
    np.testing.assert_array_equal(actual_ms['FLAG'], expected_ms['FLAG'], err_msg=label)
    assert max(errors.values()) < 1e-3, (label, errors)
    if not compare_flagged_gains:
        errors['CPARAM_with_flagged_values'] = raw_cparam
    if extra.get('refant') == '':
        errors['CPARAM_before_phase_alignment'] = raw_cparam
    return errors


def ranked_references(root, source, native):
    results = {}
    for name, selector, reference in [
        ('name-order', 'A3,A1', 3), ('id-order', '1,3', 1),
        ('pattern', 'A[13]', 1), ('clause-order', 'A3;A1', 3),
        ('first-side', 'A2&A3', 2), ('negated-zero', '!A0', 0),
    ]:
        case = root/name
        case.mkdir()
        results[name] = gain_and_apply(case, source, native, 'G', 'inf', calmode='ap', refant=selector)
        expected = contents(case/'ap-G-inf-mean-norm0-casa.cal')
        np.testing.assert_array_equal(expected['ANTENNA2'], reference)

    dropout = root/'reference-dropout.ms'
    copy_ms(source, dropout)
    t = table()
    t.open(str(dropout), nomodify=False)
    try:
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        fields = t.getcol('FIELD_ID')
        flags = t.getcol('FLAG')
        flags[..., ((a==0)|(b==0)) & (fields==1)] = True
        t.putcol('FLAG', flags)
    finally:
        t.close()
    for mode, reference in [('flex', 3), ('strict', -1)]:
        case = root/mode
        case.mkdir()
        results[mode] = gain_and_apply(case, dropout, native, 'G', 'inf', calmode='ap',
                                       refant='A0,A3,A1', refantmode=mode, minblperant=3)
        expected = contents(case/'ap-G-inf-mean-norm0-casa.cal')
        previous = expected['FIELD_ID'] == 0
        np.testing.assert_array_equal(expected['ANTENNA2'][previous], 0)
        np.testing.assert_array_equal(expected['ANTENNA2'][~previous], reference)
        assert bool(np.all(expected['FLAG'][..., ~previous])) == (mode == 'strict')
    return results


def spectral_window_reference(root, source, native):
    t = table()
    for subtable, overrides in [('SPECTRAL_WINDOW', {'NAME': 'SPW1'}),
                               ('DATA_DESCRIPTION', {'SPECTRAL_WINDOW_ID': 1})]:
        t.open(str(source/subtable), nomodify=False)
        try:
            t.addrows(1)
            for col in t.colnames():
                if t.iscelldefined(col, 0):
                    t.putcell(col, 1, overrides.get(col, t.getcell(col, 0)))
        finally:
            t.close()
    t.open(str(source), nomodify=False)
    try:
        fields = t.getcol('FIELD_ID')
        t.putcol('DATA_DESC_ID', fields)
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        time = t.getcol('TIME')
        zeroth = (a==0) | (b==0)
        third = (a==3) | (b==3)
        late_first = (fields==0) & (time-time.min()>=30.)
        flags = t.getcol('FLAG')
        flags[..., (late_first & (zeroth|third)) | ((fields==1) & zeroth)] = True
        t.putcol('FLAG', flags)
    finally:
        t.close()
    errors = gain_and_apply(root, source, native, 'G', '30s', calmode='ap', refant='A0,A3,A1', minblperant=2)
    expected = contents(root/'ap-G-30s-mean-norm0-casa.cal')
    times, antenna2, spws = (expected[name] for name in ['TIME', 'ANTENNA2', 'SPECTRAL_WINDOW_ID'])
    for spw in [0, 1]:
        selected = times[spws==spw]
        time = np.max(selected) if spw == 0 else np.min(selected)
        np.testing.assert_array_equal(antenna2[(spws==spw) & (times==time)], 1)
    return errors


def correlation_flags(root, source, native):
    t = table()
    t.open(str(source), nomodify=False)
    try:
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        flags = t.getcol('FLAG')
        flags[0, :, (a==1) & (b==3)] = True
        t.putcol('FLAG', flags)
    finally:
        t.close()
    results, failures = {}, []
    for gain in ['G', 'T']:
        for mode in ['ap', 'p', 'a']:
            for dependent in [False, True]:
                label = f'{gain}-{mode}-independent{int(dependent)}'
                case = root/label
                case.mkdir()
                try:
                    results[label] = gain_and_apply(case, source, native, gain, 'inf', calmode=mode,
                                                    minblperant=3, corrdepflags=dependent)
                except AssertionError as error:
                    failures.append((label, str(error)))
    assert not failures, failures
    for name in ['first-flagged', 'appended-autocorrelation', 'leading-autocorrelation']:
        case = root/name
        case.mkdir()
        alternate = case/'source.ms'
        copy_ms(source, alternate)
        t.open(str(alternate), nomodify=False)
        try:
            a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
            leading = (a==0) & (b==1)
            if name == 'first-flagged':
                flags = t.getcol('FLAG')
                flags[..., leading] = True
                t.putcol('FLAG', flags)
            else:
                for row in np.flatnonzero(leading):
                    cells = {col: t.getcell(col, int(row)) for col in t.colnames()
                             if t.iscelldefined(col, int(row))}
                    output = t.nrows()
                    t.addrows(1)
                    autocorrelation = dict(cells, ANTENNA2=cells['ANTENNA1'])
                    if name == 'leading-autocorrelation':
                        for col, value in cells.items():
                            t.putcell(col, output, value)
                        output = int(row)
                    for col, value in autocorrelation.items():
                        t.putcell(col, output, value)
        finally:
            t.close()
        results[name] = gain_and_apply(case, alternate, native, 'T', 'inf', calmode='ap', minblperant=3)
    case = root/'missing-reference-hand'
    case.mkdir()
    alternate = case/'source.ms'
    copy_ms(source, alternate)
    t.open(str(alternate), nomodify=False)
    try:
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        flags = np.zeros_like(t.getcol('FLAG'))
        flags[0, :, (a==0) | (b==0)] = True
        t.putcol('FLAG', flags)
    finally:
        t.close()
    results['missing-reference-hand'] = gain_and_apply(
        case, alternate, native, 'G', 'inf', calmode='ap', minblperant=3,
        refant='A0,A1', corrdepflags=True, compare_flagged_gains=False)
    expected = contents(case/'ap-G-inf-mean-norm0-casa.cal')
    np.testing.assert_array_equal(expected['ANTENNA2'], 1)
    np.testing.assert_array_equal(expected['FLAG'][0, 0], expected['ANTENNA1']==0)
    assert not np.any(expected['FLAG'][1])

    case = root/'first-buffer-no-reference-baselines'
    case.mkdir()
    alternate = case/'source.ms'
    copy_ms(source, alternate)
    t.open(str(alternate), nomodify=False)
    try:
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        time = t.getcol('TIME')
        flags = np.zeros_like(t.getcol('FLAG'))
        flags[..., (a==0) & (b!=1) & (time-time.min()<20.)] = True
        t.putcol('FLAG', flags)
    finally:
        t.close()
    trace_path = case/'seed.jsonl'
    previous_trace = os.environ.get('CASA_RS_GAIN_TRACE')
    os.environ['CASA_RS_GAIN_TRACE'] = str(trace_path)
    try:
        results['first-buffer-no-reference-baselines'] = gain_and_apply(
            case, alternate, native, 'T', 'inf', calmode='ap', minblperant=3, field='0', preavg=20.)
    finally:
        if previous_trace is None:
            os.environ.pop('CASA_RS_GAIN_TRACE')
        else:
            os.environ['CASA_RS_GAIN_TRACE'] = previous_trace
    events = (json.loads(line) for line in trace_path.read_text().splitlines())
    first_iteration = next(event for event in events if event['event'] == 'rust_phase_solver_iteration')
    for sample in first_iteration['antennas']:
        # CASA's TJones::guessPar skips row zero. With no remaining valid
        # reference baseline in this first buffer, it uses a gain of 0.3.
        np.testing.assert_allclose([sample['last_gain_re'], sample['last_gain_im']], [.3, 0.], atol=1e-6)
    return results


def robust_gains(root, source, native):
    t = table()
    t.open(str(source), nomodify=False)
    try:
        a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
        time = t.getcol('TIME')
        corrupt = (a==1) & (b==3) & (time==np.unique(time)[1])
        data = t.getcol('DATA')
        data[0, :, corrupt] *= 1.7*np.exp(.8j)
        t.putcol('DATA', data)
    finally:
        t.close()
    cases = {
        'ordinary': {},
        'rms-inactive': dict(rmsthresh=[.1]),
        'R-default': dict(solmode='R'),
        'R-explicit-default': dict(solmode='R', rmsthresh=[7., 5., 4., 3.5, 3., 2.8, 2.6, 2.4, 2.2, 2.5]),
        'R-custom': dict(solmode='R', rmsthresh=[3., 2.5]),
        'L1': dict(solmode='L1'),
        'L1-inactive-rms': dict(solmode='L1', rmsthresh=[.1]),
        'L1R': dict(solmode='L1R', rmsthresh=[3., 2.5]),
    }
    results = {}
    for gain in ['G', 'T']:
        for calmode in ['ap', 'p', 'a']:
            casa_solutions = {}
            for mode, extra in cases.items():
                label = f'{gain}-{calmode}-{mode}'
                case = root/label
                case.mkdir()
                results[label] = gain_and_apply(case, source, native, gain, 'inf', calmode=calmode,
                                                preavg=10., minblperant=3, **extra)
                table_path = case/f'{calmode}-{gain}-inf-mean-norm0-preavg10-casa.cal'
                casa_solutions[mode] = contents(table_path)['CPARAM']
            for mode in ['R-default', 'R-custom', 'L1', 'L1R']:
                error = relative_rms(casa_solutions[mode], casa_solutions['ordinary'])
                assert error > (.004 if calmode == 'ap' else .0001), (gain, calmode, mode, error)
            for first, second in [('ordinary', 'rms-inactive'), ('L1', 'L1-inactive-rms'),
                                  ('R-default', 'R-explicit-default')]:
                np.testing.assert_array_equal(casa_solutions[first], casa_solutions[second])
    return results


def robust_edge_cases(root, source, native):
    results, failures = {}, []
    for name in ['late-reference', 'persistent-rejection', 'small-residuals']:
        case = root/name
        case.mkdir()
        vis = case/'source.ms'
        copy_ms(source, vis)
        t = table()
        t.open(str(vis), nomodify=False)
        try:
            a, b = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
            _, integration = np.unique(t.getcol('TIME'), return_inverse=True)
            buffer = (integration % 6)//2
            blocked = (a>=4) | (b>=4)
            data = t.getcol('DATA')
            if name == 'late-reference':
                zero = (a==0) | (b==0)
                blocked |= ((buffer==0) & zero) | ((buffer==1) & ~zero) | (buffer==2)
                data[:] = np.where(((a==0)&(b==1)) | ((a==2)&(b==3)), 5., -1.)
            else:
                sign = np.select([buffer==1, buffer==2], [1., -1.])
                if name == 'persistent-rejection':
                    last = (a==3) | (b==3)
                    blocked |= (buffer==0) & last
                    data[:] = 1.+sign*np.where(last, .5, .125)
                else:
                    data[:] = 1.+sign*2.**-14
            flags = np.broadcast_to(blocked, data.shape).copy()
            t.putcol('DATA', data)
            t.putcol('FLAG', flags)
            t.putcol('SIGMA', np.ones_like(t.getcol('SIGMA')))
        finally:
            t.close()
        thresholds = [1., 10.] if name == 'persistent-rejection' else [10.]
        try:
            results[name] = gain_and_apply(
                case, vis, native, 'G', 'inf', calmode='ap', solmode='R', rmsthresh=thresholds,
                minsnr=1e5 if name == 'small-residuals' else 0.,
                field='0', preavg=20., minblperant=3, compare_flagged_gains=False)
        except AssertionError as error:
            failures.append((name, str(error)))
    assert not failures, failures
    return results


def solve_selections(root, source, native):
    t = table()
    t.open(str(source/'SPECTRAL_WINDOW'), nomodify=False)
    try:
        t.putcell('NAME', 0, 'SPW0')
    finally:
        t.close()
    t.open(str(source/'STATE'), nomodify=False)
    try:
        state_ids = [t.nrows(), t.nrows()+1]
        t.addrows(2)
        for row, name in zip(state_ids, ['CALIBRATE_FLUX', 'CALIBRATE_PHASE']):
            t.putcell('OBS_MODE', row, name+'#ON_SOURCE')
    finally:
        t.close()
    t.open(str(source/'OBSERVATION'), nomodify=False)
    try:
        observation = t.nrows()
        t.addrows(1)
        for col in t.colnames():
            if t.iscelldefined(col, 0):
                t.putcell(col, observation, t.getcell(col, 0))
    finally:
        t.close()
    t.open(str(source), nomodify=False)
    try:
        field = t.getcol('FIELD_ID')
        t.putcol('STATE_ID', np.asarray(state_ids, dtype=np.int32)[field])
        t.putcol('OBSERVATION_ID', np.where(field==0, 0, observation).astype(np.int32))
        baseline_lengths = np.linalg.norm(t.getcol('UVW')[:2], axis=0)
        assert np.any(baseline_lengths < 50.) and np.any(baseline_lengths >= 50.)
    finally:
        t.close()
    t.open(str(source/'FIELD'))
    try:
        field_name = t.getcell('NAME', 1)
    finally:
        t.close()

    results = {}
    for name, interval, expected_field, options in [
        ('intent', 'inf', [1], dict(intent='*PHASE*')),
        ('uvrange', 'inf', [0, 1], dict(uvrange='<50m')),
        ('named-field', 'inf', [1], dict(field=field_name)),
        ('observation-number', 'inf', [1], dict(observation=int(observation))),
        ('solint-number', 25.0, [0, 1], {}),
        ('taql', 'inf', [1], dict(msselect='FIELD_ID == 1')),
        ('named-spw-channels', 'inf', [0, 1], dict(spw='SPW0:0~1')),
        ('disabled', 'inf', [1], dict(selectdata=False, intent='*PHASE*', scan='4000',
                                     observation=9000, antenna='MISSING', uvrange='>10000km')),
    ]:
        case = root/name
        case.mkdir()
        results[name] = gain_and_apply(case, source, native, 'G', interval,
                                       calmode='ap', minblperant=2, **options)
        expected = contents(case/f'ap-G-{interval}-mean-norm0-casa.cal')
        np.testing.assert_array_equal(np.unique(expected['FIELD_ID']), expected_field)
        if name == 'named-spw-channels':
            spectral_metadata(case, f'ap-G-{interval}-mean-norm0')
    return results


def spectral_metadata(case, label):
    casa_window, native_window = [
        contents(case/f'{label}-{engine}.cal'/'SPECTRAL_WINDOW') for engine in ['casa', 'native']
    ]
    for col in ['CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION', 'NUM_CHAN',
                'TOTAL_BANDWIDTH', 'FLAG_ROW']:
        np.testing.assert_allclose(native_window[col], casa_window[col], rtol=1e-9, atol=0.,
                                   err_msg=col)


def solve_grouping(root, source, native):
    t = table()
    for subtable, overrides in [
        ('SPECTRAL_WINDOW', {'NAME': 'SPW1'}),
        ('DATA_DESCRIPTION', {'SPECTRAL_WINDOW_ID': 1}),
        ('OBSERVATION', {}),
    ]:
        t.open(str(source/subtable), nomodify=False)
        try:
            assert t.nrows() == 1
            t.addrows(1)
            for col in t.colnames():
                if t.iscelldefined(col, 0):
                    value = overrides.get(col, t.getcell(col, 0))
                    if subtable == 'SPECTRAL_WINDOW' and col in ['CHAN_FREQ', 'REF_FREQUENCY']:
                        value += 4e8
                    t.putcell(col, 1, value)
        finally:
            t.close()
    t.open(str(source), nomodify=False)
    try:
        _, integration = np.unique(t.getcol('TIME'), return_inverse=True)
        t.putcol('DATA_DESC_ID', (integration % 2).astype(np.int32))
        t.putcol('OBSERVATION_ID', ((integration // 2) % 2).astype(np.int32))
        t.putcol('SCAN_NUMBER', np.ones(len(integration), dtype=np.int32))
    finally:
        t.close()
    results = {}
    for gain in ['G', 'T']:
        for mode in ['p', 'ap', 'a']:
            for axes, intervals in [('', 4), ('obs', 2), ('spw', 2), ('obs,spw', 1)]:
                name = f'{gain}-{mode}-{axes.replace(",", "-") or "none"}'
                case = root/name
                case.mkdir()
                results[name] = gain_and_apply(
                    case, source, native, gain, 'inf', calmode=mode, combine=axes,
                    field='0', minblperant=3, sort_solutions=True,
                    apply_options={'spwmap': [0, 0]} if 'spw' in axes else None)
                label = f'{mode}-{gain}-inf-mean-norm0'
                expected = contents(case/f'{label}-casa.cal')
                assert len(expected['TIME']) == 5*intervals, (name, expected['TIME'])
                spectral_metadata(case, label)
    results.update(grouping_edge_cases(root, source, native))
    return results


def grouping_edge_cases(root, source, native):
    results = {}
    for name in ['unequal-widths', 'later-first-spw', 'sparse-windows', 'simultaneous-windows']:
        case = root/name
        case.mkdir()
        vis = case/'source.ms'
        copy_ms(source, vis)
        combine, interval, extra = 'spw', 'inf', {}
        if name == 'unequal-widths':
            extra['spw'] = '0:0~1,1:0~3'
        else:
            t = table()
            t.open(str(vis), nomodify=False)
            try:
                times = t.getcol('TIME')
                _, integration = np.unique(times, return_inverse=True)
                if name == 'later-first-spw':
                    spw = (1 - integration % 2).astype(np.int32)
                    t.putcol('OBSERVATION_ID', spw)
                    combine = 'obs,spw'
                    extra['preavg'] = 20.
                elif name == 'sparse-windows':
                    spw = (times > np.median(times)).astype(np.int32)
                    t.putcol('FIELD_ID', np.zeros_like(spw))
                    t.putcol('OBSERVATION_ID', np.zeros_like(spw))
                    t.putcol('SCAN_NUMBER', spw+1)
                else:
                    spw = (integration % 2).astype(np.int32)
                    simultaneous = times.min() + 20 * (integration // 2)
                    t.putcol('TIME', simultaneous)
                    t.putcol('TIME_CENTROID', simultaneous)
                    t.putcol('FIELD_ID', np.zeros_like(spw))
                    t.putcol('OBSERVATION_ID', np.zeros_like(spw))
                    t.putcol('INTERVAL', np.where(spw == 0, 10., 20.))
                    t.putcol('EXPOSURE', np.where(spw == 0, 10., 20.))
                    interval = 'int'
                t.putcol('DATA_DESC_ID', spw)
            finally:
                t.close()
        gain = 'T' if name == 'later-first-spw' else 'G'
        results[name] = gain_and_apply(
            case, vis, native, gain, interval, calmode='ap', field='0', combine=combine,
            minblperant=3, sort_solutions=True,
            apply_options=None if name == 'sparse-windows' else {'spwmap': [0, 0]}, **extra)
        label = f'ap-{gain}-{interval}-mean-norm0' + ('-preavg20' if 'preavg' in extra else '')
        expected = contents(case/f'{label}-casa.cal')
        if name == 'sparse-windows':
            np.testing.assert_array_equal(np.unique(expected['SPECTRAL_WINDOW_ID']), [0, 1])
        if name == 'later-first-spw':
            np.testing.assert_array_equal(expected['OBSERVATION_ID'], 0)
        if name == 'simultaneous-windows':
            assert len(expected['TIME']) == 5*len(np.unique(simultaneous))
        spectral_metadata(case, label)
    return results


def point_models(root, source, native):
    results = {}
    for basis, stokes, model in [
        ('linear', 'XX YY', [1.5, .3, .2, .1]),
        ('circular', 'RR LL', [1.5, .3, .2, -.2]),
        ('linear-full', 'XX XY YX YY', [1.5, .3]),
        ('linear-full-cross', 'XX XY YX YY', [1.5, .3, .2, .1]),
        ('circular-full-cross', 'RR RL LR LL', [1.5, .3, .2, -.2]),
    ]:
        fixture = root/basis
        fixture.mkdir()
        make_cal_ms(str(fixture/'source.ms'), stokes=stokes)
        vis = amplitude_data(fixture, fixture/'source.ms')
        for gain in ['G', 'T']:
            for mode in ['p', 'a', 'ap']:
                name = f'{basis}-{gain}-{mode}'
                case = root/name
                case.mkdir()
                results[name] = gain_and_apply(case, vis, native, gain, 'inf', calmode=mode,
                                               smodel=model, minblperant=2)
                if gain == 'G' and basis.endswith('-full-cross'):
                    parang_case = root/f'{name}-parang'
                    parang_case.mkdir()
                    results[parang_case.name] = gain_and_apply(
                        parang_case, vis, native, gain, 'inf', calmode=mode, smodel=model,
                        parang=True, minblperant=2, apply_options={'parang': True})
    for name, fixture_source, write_model in [
        ('stored-column', source, True),
        ('stored-column-full', root/'linear-full'/'amplitude.ms', True),
        ('point-fallback', source, False),
    ]:
        fixture = root/name
        fixture.mkdir()
        vis = fixture/'source.ms'
        copy_ms(fixture_source, vis)
        if write_model:
            casa.setjy(vis=str(vis), field='0', standard='manual',
                       fluxdensity=[1.7, 0., 0., 0.], usescratch=True)
        else:
            t = table()
            t.open(str(vis), nomodify=False)
            try:
                t.removecols('MODEL_DATA')
            finally:
                t.close()
        for gain in ['G', 'T']:
            results[f'{name}-{gain}'] = gain_and_apply(fixture, vis, native, gain, 'inf',
                calmode='ap', field='0', smodel=[], minblperant=2)
    results.update(point_model_edges(root, root/'linear-full'/'amplitude.ms', native))
    results.update(nonpositive_stokes_models(root, root/'linear-full'/'amplitude.ms', native))
    return results


def nonpositive_stokes_models(root, source, native):
    results = {}
    for name, model, signs in [('negative-stokes-i', [-1.], [-1, 1, 1, -1]),
                               ('zero-stokes', [0.], [1, 1, 1, 1]),
                               ('stokes-q-only', [0., 1.], [1, 1, 1, -1])]:
        outputs = []
        for label, api in [('casa', casa), ('native', native)]:
            vis = root/f'{name}-{label}.ms'
            product = root/f'{name}-{label}.cal'
            copy_ms(source, vis)
            t = table()
            assert t.open(str(vis), nomodify=False)
            try:
                observations = t.getcol('DATA') * np.asarray(signs, dtype=np.float32)[:, None, None]
                t.putcol('DATA', observations)
            finally:
                t.close()
            api.gaincal(str(vis), caltable=str(product), field='0', spw='0', refant='0',
                        calmode='ap', smodel=model, minblperant=2, minsnr=0.)
            if name == 'zero-stokes':
                assert not product.exists()
            else:
                outputs.append(contents(product))
        if name == 'zero-stokes':
            results[name] = {'no_product': True}
        else:
            expected, actual = outputs
            for column in ('FLAG', 'ANTENNA1'):
                np.testing.assert_equal(expected[column], actual[column])
            good = ~expected['FLAG']
            assert np.any(good)
            error = relative_rms(actual['CPARAM'][good], expected['CPARAM'][good])
            assert error < 1e-3, (name, error)
            results[name] = {'gain_nrms': float(error)}
    return results


def point_model_edges(root, source, native):
    results = {}
    for name in ['mixed-model-column', 'mixed-model-robust', 'weak-cross-hands', 'nominal-baseline']:
        case = root/name
        case.mkdir()
        vis = case/'source.ms'
        copy_ms(source, vis)
        options = dict(calmode='ap', corrdepflags=True, minblperant=2)
        t = table()
        t.open(str(vis), nomodify=False)
        try:
            if name.startswith('mixed-model'):
                model = t.getcol('MODEL_DATA')
                model[0], model[3], model[1:3] = 1.2, .8, 0.
                model[1, ::2, 1::2] = .2 + .1j
                model[2, ::2, 1::2] = .2 - .1j
                t.putcol('MODEL_DATA', model)
                flags = t.getcol('FLAG')
                flags[1:3, ::2, 1::4] = True
                t.putcol('FLAG', flags)
                options['smodel'] = []
                if name == 'mixed-model-robust':
                    options.update(solmode='R', rmsthresh=[5., 4.])
            elif name == 'weak-cross-hands':
                options.update(smodel=[1., 0., .0001, 0.], minsnr=4., compare_flagged_gains=False)
            else:
                first, second = t.getcol('ANTENNA1'), t.getcol('ANTENNA2')
                baseline = (first == 0) & (second == 1)
                flags = t.getcol('FLAG')
                flags[0, :, baseline] = True
                flags[3, :, baseline] = True
                t.putcol('FLAG', flags)
                options['minblperant'] = 4
        finally:
            t.close()
        results[name] = gain_and_apply(case, vis, native, 'T', 'inf', **options)
        expected = contents(case/'ap-T-inf-mean-norm0-casa.cal')
        if name == 'weak-cross-hands':
            assert np.any(expected['FLAG']) and np.any(~expected['FLAG'])
        if name == 'nominal-baseline':
            assert not np.any(expected['FLAG'])
    return results


def spectral_weights(root, source, native):
    results = {}
    failure_cases = {'sigma-later-undefined', 'sigma-second-scan', 'sigma-combined-scans'}
    for name in ['absent', 'defined', 'sigma-undefined', 'weight-undefined',
                 'sigma-first-undefined', 'weight-first-undefined', *sorted(failure_cases)]:
        case = root/name
        case.mkdir()
        vis = case/'source.ms'
        copy_ms(source, vis)
        t = table()
        t.open(str(vis), nomodify=False)
        try:
            shape = t.getcell('DATA', 0).shape
            missing_sigma = {'sigma-first-undefined': 0, 'sigma-later-undefined': 3,
                             'sigma-combined-scans': 1}.get(name)
            if name == 'sigma-second-scan':
                scans = t.getcol('SCAN_NUMBER')
                missing_sigma = int(np.flatnonzero(scans != scans[0])[0])
            if name == 'sigma-combined-scans':
                t.putcell('SCAN_NUMBER', 1, 3)
            for column in ['SIGMA_SPECTRUM', 'WEIGHT_SPECTRUM']:
                if column in t.colnames():
                    t.removecols(column)
                if name != 'absent':
                    description = t.getcoldesc('DATA')
                    description.update(valueType='float', dataManagerType='StandardStMan',
                                       dataManagerGroup='StandardStMan', option=0)
                    description.pop('shape', None)
                    t.addcols({column: description})
            if name != 'absent':
                for row in range(t.nrows()):
                    if name != 'weight-undefined' and not (name == 'weight-first-undefined' and row == 0):
                        t.putcell('WEIGHT_SPECTRUM', row, np.ones(shape, dtype=np.float32))
                    if name != 'sigma-undefined' and row != missing_sigma:
                        sigma = .4 + (np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
                                      + .9*(row % 6))/12
                        t.putcell('SIGMA_SPECTRUM', row, sigma)
        finally:
            t.close()
        if name in failure_cases:
            combine = 'scan' if name == 'sigma-combined-scans' else ''
            for engine, module, diagnostic in [
                ('casa', casa, 'Error in Calibrater::solve'),
                ('native', native, 'SIGMA_SPECTRUM'),
            ]:
                try:
                    solve(module, case, engine, vis, gaintype='G', calmode='ap', minblperant=2,
                          combine=combine)
                except RuntimeError as error:
                    assert diagnostic in str(error), (engine, error)
                else:
                    raise AssertionError(f'{engine} accepted an undefined sigma inside a buffer')
                assert not (case/f'{engine}.cal').exists()
            continue
        results[name] = gain_and_apply(case, vis, native, 'G', 'inf', calmode='ap',
                                       minblperant=2, apply_options={'calwt': [False]})
    return results


def virtual_components(root, source, native):
    vis = root/'setjy.ms'
    copy_ms(source, vis)
    flux = [1.7, 0., 0., 0.]
    casa.setjy(vis=str(vis), field='0', standard='manual', fluxdensity=flux, usescratch=False)
    t = table()
    t.open(str(vis))
    try:
        assert 'MODEL_DATA' in t.colnames()
    finally:
        t.close()
    variants = {'overrides-column': (vis, '0'), 'unrelated-field': (vis, '1')}
    for name in ['without-column', 'offset-component', 'unmatched-entry', 'legacy-record']:
        variant = root/f'{name}.ms'
        copy_ms(vis, variant)
        if name == 'without-column':
            t.open(str(variant), nomodify=False)
            try:
                t.removecols('MODEL_DATA')
            finally:
                t.close()
        else:
            t.open(str(variant/'SOURCE'), nomodify=False)
            try:
                row = t.getkeyword('model_0')
                record = t.getcell('SOURCE_MODEL', row)
                entry = record['cl_0']
                if name == 'offset-component':
                    longitude = entry['container']['component0']['shape']['direction']['m0']
                    assert longitude['unit'] == 'rad'
                    longitude['value'] += .00015
                elif name == 'unmatched-entry':
                    entry['indexcombination'][..., 2] += 1000
                else:
                    record.pop('version')
                    record['cl_0'] = dict(
                        fields=np.array([0], dtype=np.int32),
                        spws=np.array([0], dtype=np.int32),
                        start=np.array([0], dtype=np.int32),
                        nchan=np.array([8], dtype=np.int32),
                        incr=np.array([1], dtype=np.int32),
                        type=entry['type'], container=entry['container'],
                    )
                t.putcell('SOURCE_MODEL', row, record)
            finally:
                t.close()
        variants[name] = (variant, '0')

    results = {}
    for name, (variant, field) in variants.items():
        for gain in ['G', 'T']:
            case = root/f'{name}-{gain}'
            case.mkdir()
            results[case.name] = gain_and_apply(case, variant, native, gain, 'inf',
                                               calmode='ap', field=field, smodel=[], minblperant=2)
            if name == 'overrides-column':
                explicit = solve(casa, case, 'explicit', variant, gaintype=gain,
                                 calmode='ap', field=field, smodel=flux, minblperant=2)
                sky = contents(case/f'ap-{gain}-inf-mean-norm0-casa.cal')
                assert relative_rms(explicit['CPARAM'], sky['CPARAM']) < 1e-6
                unit = solve(casa, case, 'unit', variant, gaintype=gain, calmode='ap',
                             field=field, minblperant=2)
                assert relative_rms(unit['CPARAM'], sky['CPARAM']) > .2
    results.update(virtual_component_geometry(root, vis, native))
    return results


def virtual_component_geometry(root, vis, native):
    results = {}
    linear = root/'linear'
    linear.mkdir()
    make_cal_ms(str(linear/'source.ms'), stokes='XX XY YX YY')
    source = amplitude_data(linear, linear/'source.ms')
    for name, scratch in [('parang-virtual', False), ('parang-scratch', True)]:
        case = root/name
        case.mkdir()
        polarized = case/'source.ms'
        copy_ms(source, polarized)
        casa.setjy(vis=str(polarized), field='0', standard='manual',
                   fluxdensity=[1.5, .3, .2, .1], usescratch=scratch)
        results[name] = gain_and_apply(
            case, polarized, native, 'G', 'inf', calmode='ap', field='0',
            smodel=[], parang=True, minblperant=2, apply_options={'parang': True})

    moving = root/'polynomial.ms'
    copy_ms(vis, moving)
    t = table()
    t.open(str(moving))
    try:
        first_time = float(t.getcol('TIME').min())
    finally:
        t.close()
    t.open(str(moving/'FIELD'), nomodify=False)
    try:
        t.putcell('NUM_POLY', 0, 1)
        t.putcell('TIME', 0, first_time)
        for column in ['PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR']:
            constant = t.getcell(column, 0)[:, 0]
            t.putcell(column, 0, np.column_stack([constant, [1e-6, 0.]]))
    finally:
        t.close()
    for interval in ['int', 'inf']:
        case = root/f'polynomial-{interval}'
        case.mkdir()
        results[case.name] = gain_and_apply(
            case, moving, native, 'G', interval, calmode='ap',
            field='0', smodel=[], minblperant=2)
    return results


def copy_with_channel_spacing(source, destination, channel_width):
    copy_ms(source, destination)
    t = table()
    t.open(str(destination/'SPECTRAL_WINDOW'), nomodify=False)
    try:
        freq = t.getcell('CHAN_FREQ', 0)
        t.putcell('CHAN_FREQ', 0, freq[0] + channel_width*np.arange(freq.size))
        for name in ['CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION']:
            t.putcell(name, 0, np.full(freq.size, channel_width))
        t.putcell('TOTAL_BANDWIDTH', 0, channel_width*freq.size)
    finally:
        t.close()


def virtual_fourier(root, source, native):
    results = {}
    wide = root/'wide-window.ms'
    copy_with_channel_spacing(source, wide, 1e9)
    long_w = root/'long-w.ms'
    copy_ms(source, long_w)
    t = table()
    t.open(str(long_w), nomodify=False)
    try:
        uvw = t.getcol('UVW')
        uvw[2] *= 5
        t.putcol('UVW', uvw)
    finally:
        t.close()
    cases = [
        ('continuum', source, 'GridFT', {}),
        ('off-centre', source, 'GridFT', {'phasecenter': 'J2000 0h0m0.10s -30d00m00s'}),
        ('cube', source, 'GridFT', {'specmode': 'cube', 'nchan': 3, 'start': 0, 'width': 1}),
        ('partially-flagged', source, 'GridFT', {}),
        ('faceted', source, 'GridFT', {'facets': 2}),
        ('faceted-east-west', source, 'GridFT', {'facets': 2}),
        ('polarized', source, 'GridFT', {'stokes': 'IQUV'}),
        ('finite-distance', source, 'GridFT', {'stokes': 'IQUV'}),
        ('finite-distance-combined', source, 'GridFT', {'stokes': 'IQUV'}),
        ('virtual-scan-buffer', source, 'GridFT', {}),
        ('wproject', long_w, 'WProjectFT', {'gridder': 'wproject', 'wprojplanes': 8}),
        ('wproject-facets', long_w, 'WProjectFT', {'gridder': 'wproject', 'wprojplanes': 8, 'facets': 2}),
        ('wproject-measured', long_w, 'WProjectFT', {'gridder': 'wproject', 'wprojplanes': 8}),
        ('mosaic', source, 'MosaicFTNew', {'gridder': 'mosaic'}),
        ('mosaic-image-beam', source, 'MosaicFTNew', {'gridder': 'mosaic'}),
        ('mosaic-saved-plane', source, 'MosaicFTNew', {'gridder': 'mosaic'}),
        ('mosaic-legacy-name', source, 'MosaicFTNew', {'gridder': 'mosaic'}),
        ('mosaic-shifted', source, 'MosaicFTNew',
         {'gridder': 'mosaic', 'phasecenter': 'J2000 0h0m0.10s -30d00m00s'}),
        ('mosaic-pointing-table', source, 'MosaicFTNew', {'gridder': 'mosaic', 'usepointing': True}),
        ('mosaic-cube', source, 'MosaicFTNew',
         {'gridder': 'mosaic', 'specmode': 'cube', 'nchan': 3, 'start': 0, 'width': 1}),
        ('setjy-grid', source, 'SetJyGridFT', {}),
        ('setjy-grid-varying', source, 'SetJyGridFT', {}),
        ('taylor', wide, 'MultiTermFTNew', {'deconvolver': 'mtmfs', 'nterms': 2}),
        ('taylor-outside', source, 'MultiTermFTNew', {'deconvolver': 'mtmfs', 'nterms': 2}),
    ]
    for name, original, machine, arguments in cases:
        folder = root/name
        folder.mkdir()
        vis = folder/'input.ms'
        copy_ms(original, vis)
        if name == 'mosaic-pointing-table':
            offset_mosaic_pointings(vis)
        if name == 'mosaic-image-beam':
            arguments = {**arguments, 'vptable': str(mosaic_image_beam(folder, root/'mosaic'/'image.pb'))}
        from_setjy = name.startswith('setjy-grid')
        varying_setjy = name == 'setjy-grid-varying'
        finite_distance = name.startswith('finite-distance')
        combined_scans = name in ['finite-distance-combined', 'virtual-scan-buffer']
        casa.tclean(
            vis=str(vis), imagename=str(folder/'image'), field='0', spw='0',
            imsize=64, cell='1arcsec', niter=5, datacolumn='data',
            savemodel='none' if from_setjy else 'virtual', **arguments)
        if from_setjy:
            casa.setjy(vis=str(vis), field='0', spw='0', model=str(folder/'image.model'),
                       standard='manual', fluxdensity=[1., 0., 0., 0.], usescratch=False)
        t = table()
        measured_w = name == 'wproject-measured'
        legacy_mosaic = name == 'mosaic-legacy-name'
        saved_mosaic = name == 'mosaic-saved-plane'
        t.open(str(vis/'SOURCE'), nomodify=not (measured_w or legacy_mosaic or varying_setjy
                                                or finite_distance or saved_mosaic))
        try:
            source_row = t.getkeyword('model_0')
            record = t.getcell('SOURCE_MODEL', source_row)
            assert record['ft_0']['container']['name'] == machine
            if name == 'mosaic-image-beam':
                assert record['ft_0']['container']['pbconvfunc']['pbclass'] == 8
            if name == 'mosaic-pointing-table':
                assert record['ft_0']['container']['usepointingtable']
            # A restored WProjectFT keeps its chosen plane count but can
            # rebuild the kernels using the measured maximum stored with it.
            if measured_w:
                record['ft_0']['container']['maxw'] = 30000.
                t.putcell('SOURCE_MODEL', source_row, record)
            if legacy_mosaic:
                record['ft_0']['container']['name'] = 'MosaicFT'
                t.putcell('SOURCE_MODEL', source_row, record)
            if varying_setjy:
                # Give the persisted flux spectrum a measurable slope even on
                # this narrow band, before either engine reopens the model.
                scales = record['ft_0']['container']['scaleamp']
                record['ft_0']['container']['scaleamp'] = scales*np.linspace(.9, 1.15, scales.size)
                t.putcell('SOURCE_MODEL', source_row, record)
            if finite_distance:
                record['ft_0']['container']['distance'] = 4e4 if combined_scans else 5e6
                if combined_scans:
                    # The same finite-distance sky applies to both scan labels.
                    entry = record['ft_0']
                    index = entry['indexcombination'].copy()
                    index[:, 2] = 2
                    entry['indexcombination'] = np.concatenate([entry['indexcombination'], index])
                t.putcell('SOURCE_MODEL', source_row, record)
            if saved_mosaic:
                beam = record['ft_0']['container']['pbconvfunc']
                assert np.all(beam['donemainconv'])
                beam['convfunctions0'] *= .5
                t.putcell('SOURCE_MODEL', source_row, record)
        finally:
            t.close()
        if name == 'taylor-outside':
            verify_no_fourier_solution(folder, vis, native)
            continue
        if combined_scans:
            t.open(str(vis), nomodify=False)
            try:
                scans = (t.getcol('ANTENNA1') + t.getcol('ANTENNA2'))%2 + 1
                t.putcol('SCAN_NUMBER', scans.astype(np.int32))
            finally:
                t.close()
        if name == 'faceted-east-west':
            east_west_facet(t, vis)
        partial_flags = name == 'partially-flagged'
        if partial_flags:
            t.open(str(vis), nomodify=False)
            try:
                flags = t.getcol('FLAG')
                flags[0, ::2, :] = True
                t.putcol('FLAG', flags)
            finally:
                t.close()
        for gain in ['G', 'T']:
            case = folder/gain
            case.mkdir()
            results[f'{name}-{gain}'] = gain_and_apply(
                case, vis, native, gain, 'inf', calmode='ap', field='0', spw='0',
                smodel=[], minblperant=2, corrdepflags=partial_flags,
                combine='scan' if combined_scans else '')
            explicit = solve(casa, case, 'unit', vis, gaintype=gain, calmode='ap',
                             field='0', spw='0', minblperant=2,
                             combine='scan' if combined_scans else '')
            sky = contents(case/f'ap-{gain}-inf-mean-norm0-casa.cal')
            difference = relative_rms(explicit['CPARAM'], sky['CPARAM'])
            if name == 'virtual-scan-buffer':
                assert difference < 1e-5
            else:
                assert difference > .01
            if name == 'mosaic-image-beam':
                selected = folder/f'{gain}-selected'
                selected.mkdir()
                results[f'{name}-{gain}-selected'] = gain_and_apply(
                    selected, vis, native, gain, 'inf', calmode='ap', field='0', spw='0:0~3',
                    smodel=[], minblperant=2)
    verify_missing_maximum_w(root, native)
    results.update(rebuilt_image_beam(root, source, native))
    results.update(rebuilt_image_beam(root, source, native, heterogeneous=True))
    return results


def rebuilt_image_beam(root, source, native, heterogeneous=False):
    folder = root/('mixed-image-beams' if heterogeneous else 'rebuilt-image-beam')
    folder.mkdir()
    vis = folder/'input.ms'
    copy_with_channel_spacing(source, vis, 250e6)
    vptable = (
        mosaic_image_beam(folder, root/'mosaic'/'image.pb', heterogeneous=True)
        if heterogeneous else root/'mosaic-image-beam'/'beam.tab'
    )
    casa.tclean(vis=str(vis), imagename=str(folder/'image'), field='0', spw='0',
                imsize=64, cell='1arcsec', niter=5, datacolumn='data',
                savemodel='virtual', gridder='mosaic', vptable=str(vptable))
    t = table()
    t.open(str(vis/'SOURCE'))
    try:
        beam = t.getcell('SOURCE_MODEL', t.getkeyword('model_0'))['ft_0']['container']['pbconvfunc']
        assert beam['key0'] == '0_0_8' and beam['ndefined'] == 1
        assert beam['convfunctions0'].shape[3:] == (3, 3 if heterogeneous else 1)
    finally:
        t.close()
    results = {}
    selections = [('rebuilt', '0:4~7')]
    if heterogeneous:
        selections.append(('cached', '0'))
    for label, selection in selections:
        for gain in ('G', 'T'):
            case = folder/f'{label}-{gain}'
            case.mkdir()
            results[f'{folder.name}-{label}-{gain}'] = gain_and_apply(
                case, vis, native, gain, 'inf', calmode='ap', field='0', spw=selection,
                smodel=[], minblperant=2)
    return results


def offset_mosaic_pointings(vis):
    t = table()
    t.open(str(vis/'POINTING'), nomodify=False)
    try:
        direction = t.getcol('DIRECTION')
        _, integration = np.unique(t.getcol('TIME'), return_inverse=True)
        direction[0, 0, :] = 6e-5*(1.+t.getcol('ANTENNA_ID')/4.)
        direction[1, 0, :] = -np.pi/6+(integration%3-1)*2e-5
        t.putcol('DIRECTION', direction)
        t.putcol('TARGET', direction)
    finally:
        t.close()


def mosaic_image_beam(folder, template, heterogeneous=False):
    from casatools import image, vpmanager

    tool = image()
    tool.open(str(template))
    try:
        coords = tool.coordsys()
        try:
            coords.setstokes(['XX', 'XY', 'YX', 'YY'])
            record = coords.torecord()
        finally:
            coords.done()
        power = tool.getchunk()
    finally:
        tool.close()
    voltage = np.zeros((*power.shape[:2], 4, power.shape[3]), dtype=np.float32)
    voltage[:, :, 0, :] = np.sqrt(np.maximum(power[:, :, 0, :], 0.))
    voltage[:, :, 3, :] = .96*voltage[:, :, 0, :]
    beam = folder/'beam.image'
    tool.fromarray(str(beam), voltage, csys=record)
    if heterogeneous:
        x = np.linspace(-1., 1., voltage.shape[0])[:, None, None, None]
        second = folder/'second.image'
        imaginary = folder/'imaginary.image'
        tool.close()
        tool.fromarray(str(second), voltage*(1.+.10*x), csys=record)
        coords = tool.coordsys()
        try:
            coords.setstokes(['YY', 'YX', 'XY', 'XX'])
            imaginary_coords = coords.torecord()
        finally:
            coords.done()
        tool.close()
        imaginary_voltage = voltage*.07*x
        imaginary_voltage[:, :, 0, :] = .4*voltage[:, :, 0, :]*(1.+x[:, :, 0, :])
        tool.fromarray(str(imaginary), imaginary_voltage, csys=imaginary_coords)
    tool.done()
    manager = vpmanager()
    manager.reset()
    table_path = folder/'beam.tab'
    try:
        names = ['A0', 'A1'] if heterogeneous else ['*']
        assert manager.setpbimage(telescope='ALMA', realimage=str(beam), antnames=names)
        if heterogeneous:
            assert manager.setpbimage(telescope='ALMA', realimage=str(second),
                                      imagimage=str(imaginary), antnames=['A2', 'A3', 'A4'])
        assert manager.setuserdefault(vplistnum=0, telescope='ALMA')
        assert manager.saveastable(str(table_path))
    finally:
        manager.reset()
    return table_path


def east_west_facet(t, vis):
    t.open(str(vis/'OBSERVATION'), nomodify=False)
    try:
        t.putcell('TELESCOPE_NAME', 0, 'ATCA')
    finally:
        t.close()
    t.open(str(vis/'FIELD'), nomodify=False)
    try:
        for column in ['PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR']:
            direction = t.getcell(column, 0)
            direction[0, 0] += .003
            t.putcell(column, 0, direction)
    finally:
        t.close()


def verify_missing_maximum_w(root, native):
    case = root/'missing-maximum-w'
    case.mkdir()
    vis = case/'input.ms'
    copy_ms(root/'wproject'/'input.ms', vis)
    t = table()
    t.open(str(vis/'SOURCE'), nomodify=False)
    try:
        source_row = t.getkeyword('model_0')
        record = t.getcell('SOURCE_MODEL', source_row)
        del record['ft_0']['container']['maxw']
        t.putcell('SOURCE_MODEL', source_row, record)
    finally:
        t.close()
    for module, name, message in [(casa, 'casa', 'Calibrater::solve'),
                                  (native, 'native', 'maximum W')]:
        try:
            solve(module, case, name, vis, smodel=[], field='0', spw='0', minblperant=2)
        except (RuntimeError, ValueError) as error:
            assert message in str(error), error
        else:
            raise AssertionError(f'{name} accepted WProjectFT without maxw')
        assert not (case/f'{name}.cal').exists()


def verify_no_fourier_solution(root, vis, native):
    """The reopened Taylor machine matches raw frequencies to its image axis."""
    statistics = []
    for name, module in [('casa', casa), ('native', native)]:
        destination = root/f'{name}.cal'
        preload = os.environ.pop('LD_PRELOAD', None) if module is native else None
        try:
            report = module.gaincal(
                vis=str(vis), caltable=str(destination), smodel=[], gaintype='G',
                calmode='ap', field='0', spw='0', refant='A0', minsnr=0., minblperant=2)
        finally:
            if preload is not None:
                os.environ['LD_PRELOAD'] = preload
        assert not destination.exists()
        statistics.append(report['solvestats'])
    for key in ['expected', 'data_unflagged', 'above_minblperant', 'above_minsnr']:
        np.testing.assert_array_equal(statistics[0][key], statistics[1][key], err_msg=key)


def shared_window_descriptions(root, source, native):
    results = {}
    for layout in ['alternating', 'simultaneous']:
        vis = root/f'{layout}.ms'
        copy_ms(source, vis)
        t = table()
        t.open(str(vis/'DATA_DESCRIPTION'), nomodify=False)
        try:
            t.addrows(1)
            for column in t.colnames():
                t.putcell(column, 1, t.getcell(column, 0))
        finally:
            t.close()
        t.open(str(vis), nomodify=False)
        try:
            _, time = np.unique(t.getcol('TIME'), return_inverse=True)
            antennas = t.getcol('ANTENNA1') + t.getcol('ANTENNA2')
            description = (time if layout == 'alternating' else antennas) % 2
            t.putcol('DATA_DESC_ID', description.astype(np.int32))
            t.putcol('DATA', t.getcol('DATA')*(1.+.7*description))
            if layout == 'simultaneous':
                # Distinct centroids let the caltable interpolation select
                # each solution even when the visibility timestamps match.
                t.putcol('TIME_CENTROID', t.getcol('TIME_CENTROID') + .25*description)
        finally:
            t.close()
        for gain in ['G', 'T']:
            for combine in ['', 'spw']:
                name = f'{layout}-{gain}-{combine or "split"}'
                case = root/name
                case.mkdir()
                results[name] = gain_and_apply(
                    case, vis, native, gain, 'inf', calmode='ap', field='0',
                    combine=combine, minblperant=2, sort_solutions=True,
                    compare_flagged_gains=False)
                label = f'ap-{gain}-inf-mean-norm0'
                expected = contents(case/f'{label}-casa.cal')
                assert len(expected['TIME']) == (5 if combine else 10)
                spectral_metadata(case, label)
    results.update(coincident_description_references(root, native))
    results.update(simultaneous_distinct_windows(root, native))
    results.update(separate_arrays(root, source, native))
    for gain in ['G', 'T']:
        for interval in ['inf,2ch', 'int,5MHz', '30s,unused']:
            case = root/f'interval-{gain}-{interval.replace(",", "-")}'
            case.mkdir()
            results[case.name] = gain_and_apply(
                case, source, native, gain, interval, calmode='ap', field='0',
                minblperant=2)
    return results


def coincident_description_references(root, native):
    case = root/'coincident-reference'
    case.mkdir()
    vis = case/'input.ms'
    copy_ms(root/'alternating.ms', vis)
    t = table()
    t.open(str(vis), nomodify=False)
    try:
        # Two descriptions at each integration with integer times and
        # uniform weights. The calibrated timestamps are exactly equal.
        times, integration = np.unique(t.getcol('TIME'), return_inverse=True)
        coincident = np.floor(times[0]) + 10. * (integration // 2)
        t.putcol('TIME', coincident)
        t.putcol('TIME_CENTROID', coincident)
        t.putcol('SIGMA', np.ones_like(t.getcol('SIGMA')))
        second = t.getcol('DATA_DESC_ID') == 1
        uses_reference = (t.getcol('ANTENNA1') == 0) | (t.getcol('ANTENNA2') == 0)
        flags = t.getcol('FLAG')
        flags[..., second & uses_reference] = True
        t.putcol('FLAG', flags)
    finally:
        t.close()
    options = dict(gaintype='G', solint='int', calmode='ap', field='0',
                   minblperant=2, refantmode='strict')
    expected = solve(casa, case, 'reference', vis, **options)
    actual = solve(native, case, 'native', vis, **options)
    _, count = np.unique(expected['TIME'], return_counts=True)
    np.testing.assert_array_equal(count, [10, 10, 10])
    np.testing.assert_array_equal(expected['ANTENNA2'], 0)
    for column in ['TIME', 'ANTENNA1', 'ANTENNA2', 'FLAG']:
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    valid = ~expected['FLAG']
    errors = {key: relative_rms(actual[key][valid], expected[key][valid])
              for key in ['CPARAM', 'PARAMERR', 'SNR']}
    assert max(errors.values()) < 1e-3, errors
    return {case.name: errors}


def simultaneous_distinct_windows(root, native):
    case = root/'simultaneous-distinct-windows'
    case.mkdir()
    vis = case/'input.ms'
    copy_ms(root/'simultaneous.ms', vis)
    t = table()
    t.open(str(vis/'SPECTRAL_WINDOW'), nomodify=False)
    try:
        t.addrows(1)
        for column in t.colnames():
            if t.iscelldefined(column, 0):
                t.putcell(column, 1, t.getcell(column, 0))
    finally:
        t.close()
    t.open(str(vis/'DATA_DESCRIPTION'), nomodify=False)
    try:
        t.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    finally:
        t.close()
    errors = gain_and_apply(case, vis, native, 'G', 'inf', calmode='ap',
                            field='0', combine='spw', minblperant=2,
                            apply_options={'spwmap': [0, 0]})
    return {case.name: errors}


def solve_preapply(root, source, native):
    prior = root/'prior.cal'
    solve(casa, root, 'prior', source, gaintype='G', calmode='ap', solint='int',
          field='0', minblperant=2)
    library = root/'prior.callib'
    library.write_text(f"caltable='{prior}' tinterp='nearest' fldmap=[0] calwt=True\n")
    results = {}
    cases = [
        ('scalar-chain', dict(gaintable=str(prior), gainfield='0', interp='nearest', spwmap=[0])),
        ('nested-chain', dict(gaintable=[str(prior)], gainfield=['0'],
                              interp=['linear'], spwmap=[[0]])),
        ('callibrary', dict(docallib=True, callib=str(library),
                             gaintable=[str(root/'ignored.cal')])),
        ('traditional', dict(docallib=False, callib=str(root/'ignored.callib'),
                             gaintable=[str(prior)], interp=['nearest'])),
    ]
    for name, options in cases:
        case = root/name
        case.mkdir()
        results[name] = gain_and_apply(case, source, native, 'T', 'inf', calmode='ap',
                                       field='0', minblperant=2, **options)
    flagged = root/'reference-flagged.cal'
    copy_ms(prior, flagged)
    tb = table()
    assert tb.open(str(flagged), nomodify=False)
    try:
        flags = tb.getcol('FLAG')
        flags[1, :, tb.getcol('ANTENNA1') == 0] = True
        assert tb.putcol('FLAG', flags)
    finally:
        assert tb.done()
    for mode in ('flex', 'strict'):
        options = dict(gaintable=[str(flagged)], field='0', minblperant=2,
                       solint='inf', refantmode=mode, corrdepflags=False)
        expected = solve(casa, root, f'prior-refant-{mode}-casa', source, **options)
        actual = solve(native, root, f'prior-refant-{mode}-native', source, **options)
        for column in ('ANTENNA1', 'ANTENNA2', 'FLAG'):
            np.testing.assert_array_equal(actual[column], expected[column])
        good = ~expected['FLAG']
        if mode == 'flex':
            assert good[1].any() and np.all(expected['ANTENNA2'] > 0)
            assert relative_rms(actual['CPARAM'][good], expected['CPARAM'][good]) < 1e-3
        else:
            assert not good.any()
    return results


def single_hand_gain(root, source, native):
    options = dict(gaintype='G', calmode='a', field='0', solint='inf', minblperant=2, refant='')
    original = solve(native, root, 'single-hand', source, **options)
    expected = solve(casa, root, 'single-hand-casa', source, **options)
    np.testing.assert_array_equal(original['FLAG'], expected['FLAG'])
    assert np.any(~original['FLAG'][0]) and np.all(original['FLAG'][1])

    current = root/'single-hand.cal'
    legacy = root/'single-hand-legacy.cal'
    copy_ms(current, legacy)
    tool = table()
    assert tool.open(str(legacy), nomodify=False)
    try:
        # Older native G tables stored the single YY solution in slot one.
        assert tool.putkeyword('CASA_Version', 'casa-rs')
        for column in ('CPARAM', 'FLAG', 'PARAMERR', 'SNR'):
            assert tool.putcol(column, tool.getcol(column)[::-1].copy())
    finally:
        assert tool.done()

    application_options = dict(field='0', applymode='calflag', calwt=[False], flagbackup=False)
    applied = []
    for name, calibration, engine in (
        ('casa', root/'single-hand-casa.cal', casa), ('casa-reads-native', current, casa),
        ('native-reads-casa', root/'single-hand-casa.cal', native),
        ('native', current, native), ('native-reads-legacy', legacy, native),
    ):
        destination = root/name/source.name
        destination.parent.mkdir()
        copy_ms(source, destination)
        engine.applycal(str(destination), gaintable=[str(calibration)], **application_options)
        applied.append(contents(destination))
    reference = applied[0]
    good = ~reference['FLAG']
    assert good.any()
    for data in applied[1:]:
        np.testing.assert_array_equal(data['FLAG'], reference['FLAG'])
        assert relative_rms(data['CORRECTED_DATA'][good], reference['CORRECTED_DATA'][good]) < 1e-3

    earlier = contents(legacy)
    native.gaincal(str(source), str(legacy), **(options | {'field': '1', 'append': True}))
    appended = contents(legacy)
    new = appended['FIELD_ID'] == 1
    assert new.any() and np.all(appended['FLAG'][0, :, new])
    assert np.any(~appended['FLAG'][1, :, new])
    for column in ('CPARAM', 'FLAG', 'PARAMERR', 'SNR'):
        np.testing.assert_array_equal(appended[column][..., ~new], earlier[column])
    return {'unflagged': int(good.sum()), 'legacy_rows': int(new.sum())}


def separate_arrays(root, source, native):
    vis = root/'arrays.ms'
    copy_ms(source, vis)
    t = table()
    t.open(str(vis), nomodify=False)
    try:
        _, integration = np.unique(t.getcol('TIME'), return_inverse=True)
        array_ids = (integration%2).astype(np.int32)
        t.putcol('ARRAY_ID', array_ids)
        t.putcol('DATA', t.getcol('DATA')*(1.+.6*array_ids))
        t.putcol('SCAN_NUMBER', np.ones(t.nrows(), np.int32))
    finally:
        t.close()
    results = {}
    for gain in ['G', 'T']:
        case = root/f'arrays-{gain}'
        case.mkdir()
        results[case.name] = gain_and_apply(case, vis, native, gain, 'inf', calmode='ap',
                                            field='0', combine='scan', minblperant=2)
        expected = contents(case/f'ap-{gain}-inf-mean-norm0-casa.cal')
        assert len(expected['TIME']) == 10
    return results


def ephemeris_observation(root, source, offset=(1e-6, -2e-6)):
    from casaconfig import config
    from casatools import ms

    vis = amplitude_data(root, source)
    t = table()
    t.open(str(vis), nomodify=False)
    try:
        times = t.getcol('TIME')
        origin = times.min()
        stretched = origin + 25. * (times-origin)
        t.putcol('TIME', stretched)
        t.putcol('TIME_CENTROID', stretched)
    finally:
        t.close()
    t.open(str(vis/'FIELD'), nomodify=False)
    try:
        t.putcell('TIME', 0, origin)
        for column in ['DELAY_DIR', 'PHASE_DIR', 'REFERENCE_DIR']:
            t.putcell(column, 0, np.asarray(offset).reshape(2, 1))
    finally:
        t.close()
    ephemeris = Path(config.measurespath)/'ephemerides'/'JPL-Horizons'/'Uranus_55197-59214dUTC_J2000.tab'
    assert ephemeris.is_dir(), ephemeris
    tool = ms()
    tool.open(str(vis), nomodify=False)
    try:
        assert tool.addephemeris(0, str(ephemeris), 'Uranus', [0])
    finally:
        tool.done()
    return vis, ephemeris


def ephemeris_parallactic_angle(root, source, native):
    vis, ephemeris = ephemeris_observation(root, source)
    case = root/'uranus-parallactic-angle'
    case.mkdir()
    errors = gain_and_apply(case, vis, native, 'G', 'int', calmode='ap', field='0', spw='0',
                            minblperant=2, smodel=[1., .15, .1, 0.], parang=True,
                            apply_options={'parang': True})
    tracked = {'phasecenter': 'TRACKFIELD'}
    return {
        case.name: errors,
        **tracked_image(root, vis, native, 'grid', 'GridFT', tracked, ('int', 'inf')),
        **tracked_image(root, vis, native, 'mosaic', 'MosaicFTNew',
                        {**tracked, 'gridder': 'mosaic'}, ('int',)),
        **tracked_image(root, vis, native, 'ephemeris-table', 'GridFT',
                        {'phasecenter': str(ephemeris)}, ('int',)),
        **tracked_image(root, vis, native, 'planet', 'GridFT',
                        {'phasecenter': 'URANUS'}, ('int',)),
        **fixed_tracking_epoch(root, vis, native, ephemeris),
    }


def tracked_image(root, source, native, name, machine, arguments, intervals):
    folder = root/f'tracked-{name}'
    folder.mkdir()
    scratch, virtual = folder/'scratch.ms', folder/'virtual.ms'
    for vis, storage in [(scratch, 'modelcolumn'), (virtual, 'virtual')]:
        copy_ms(source, vis)
        casa.tclean(vis=str(vis), imagename=str(folder/storage), field='0', spw='0',
                    niter=5, imsize=64, cell='1arcsec', datacolumn='data',
                    savemodel=storage, **arguments)
    t = table()
    t.open(str(virtual/'SOURCE'))
    try:
        grid = t.getcell('SOURCE_MODEL', t.getkeyword('model_0'))['ft_0']['container']
        assert grid['name'] == machine and grid['fixmovingsource']
        assert Path(grid['diskimage']).is_dir()
    finally:
        t.close()
    t.open(str(scratch))
    try:
        model = t.getcol('MODEL_DATA')
        assert np.any(np.abs(model - 1.) > .01) and np.ptp(np.angle(model)) > .1
    finally:
        t.close()
    # CASA 6.7.6.14 crashes while reopening the synthetic virtual record.
    # Its live predictor writes the same tracked image's model to scratch.
    results = {}
    for gain in ('G', 'T'):
        for interval in intervals:
            case = folder/f'{gain}-{interval}'
            case.mkdir()
            results[f'{folder.name}-{case.name}'] = gain_and_apply(
                case, scratch, native, gain, interval, calmode='ap', field='0', spw='0',
                minblperant=2, smodel=[], native_source=virtual)
    return results


def fixed_tracking_epoch(root, source, native, ephemeris):
    from casatools import measures

    folder = root/'fixed-tracking-epoch'
    folder.mkdir()
    scratch, virtual = folder/'scratch.ms', folder/'virtual.ms'
    planet = root/'tracked-planet'
    copy_ms(source, scratch)
    copy_ms(planet/'virtual.ms', virtual)
    t = table()
    t.open(str(scratch))
    try:
        times = t.getcol('TIME')
    finally:
        t.close()
    epoch = float(times.min() + 100.)
    integration = float(times[times >= epoch].min())
    assert integration > epoch
    t.open(str(virtual/'SOURCE'), nomodify=False)
    try:
        row = t.getkeyword('model_0')
        record = t.getcell('SOURCE_MODEL', row)
        grid = record['ft_0']['container']
        assert grid['movingdir_rec']['refer'] == 'URANUS'
        grid['phasecentertime'] = epoch
        t.putcell('SOURCE_MODEL', row, record)
    finally:
        t.close()

    # VisBufferUtil::getPhaseCenter uses the next integration for a
    # requested epoch between samples. Freeze the scratch predictor's
    # FIELD direction independently using CASA Measures at that time.
    tool = measures()
    assert tool.doframe(tool.observatory('ALMA'))
    assert tool.doframe(tool.epoch('UTC', f'{integration}s'))
    assert tool.framecomet(str(ephemeris))
    direction = tool.measure(tool.measure(tool.direction('COMET'), 'AZEL'), 'J2000')
    t.open(str(scratch/'FIELD'), nomodify=False)
    try:
        dl, db = t.getcell('REFERENCE_DIR', 0)[:, 0]
        longitude, latitude = direction['m0']['value'], direction['m1']['value'] + db
        x = np.cos(dl)*np.cos(latitude)*np.cos(longitude)-np.sin(dl)*np.sin(longitude)
        y = np.cos(dl)*np.cos(latitude)*np.sin(longitude)+np.sin(dl)*np.cos(longitude)
        z = np.cos(dl)*np.sin(latitude)
        frozen = np.array([[np.arctan2(y, x)], [np.arctan2(z, np.hypot(x, y))]])
        t.putcell('EPHEMERIS_ID', 0, -1)
        for column in ('PHASE_DIR', 'REFERENCE_DIR', 'DELAY_DIR'):
            t.putcell(column, 0, frozen)
    finally:
        t.close()

    casa.tclean(vis=str(scratch), imagename=str(folder/'fixed'), field='0', spw='0',
                niter=0, imsize=64, cell='1arcsec', datacolumn='data',
                phasecenter='URANUS', startmodel=str(planet/'virtual.model'),
                savemodel='modelcolumn')
    results = {}
    for gain in ('G', 'T'):
        case = folder/gain
        case.mkdir()
        results[f'{folder.name}-{gain}'] = gain_and_apply(
            case, scratch, native, gain, 'int', calmode='ap', field='0', spw='0',
            minblperant=2, smodel=[], native_source=virtual)
    original = contents(planet/'G-int'/'ap-G-int-mean-norm0-casa.cal')
    fixed = contents(folder/'G'/'ap-G-int-mean-norm0-casa.cal')
    assert relative_rms(fixed['CPARAM'], original['CPARAM']) > .01
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--case', choices=['intervals', 'combined-amplitude', 'finite-averaging', 'amplitude', 'median', 'preavg', 'refant', 'ranked-refant', 'spw-refant', 'corrflags', 'robust', 'robust-edges', 'selection', 'grouping', 'point-models', 'spectral-weights', 'virtual-components', 'virtual-fourier', 'descriptions', 'preapply', 'single-hand', 'ephemeris'], default='intervals')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    os.chdir(root)
    source = root/'source.ms'
    stokes = {'ephemeris': 'XX XY YX YY', 'single-hand': 'YY'}
    make_cal_ms(str(source), stokes=stokes.get(args.case, 'XX YY'))
    if args.case == 'amplitude':
        source = amplitude_data(root, source)
        results = {f'{gain}-{interval}-norm{int(norm)}': gain_and_apply(root, source, native, gain, interval, norm)
                   for gain in ['G', 'T'] for interval, norm in [('int', False), ('inf', False), ('inf', True)]}
    elif args.case == 'median':
        source = amplitude_data(root, source)
        results = {f'{gain}-norm{int(norm)}': gain_and_apply(root, source, native, gain, 'inf', norm, 'median')
                   for gain in ['G', 'T'] for norm in [True, False]}
        for gain in ['G', 'T']:
            baseline = contents(root/f'a-{gain}-inf-median-norm0-casa.cal')
            normalized = contents(root/f'a-{gain}-inf-median-norm1-casa.cal')
            power = np.abs(baseline['CPARAM'][~baseline['FLAG']])**2
            assert abs(np.median(power)-np.mean(power)) > 0.01
            np.testing.assert_allclose(normalized['CPARAM'], baseline['CPARAM']/np.sqrt(np.median(power)),
                                       rtol=1e-6, atol=0.)
    elif args.case == 'preavg':
        source = amplitude_data(root, source)
        results = {f'{gain}-{mode}-{interval}-{average:g}': gain_and_apply(
                       root, source, native, gain, interval, calmode=mode, preavg=average)
                   for gain in ['G', 'T'] for mode in ['a', 'p', 'ap']
                   for interval, average in [('inf', 20.), ('25s', 20.), ('int', 20.), ('inf', 0.), ('inf', 1000.)]}
        for gain in ['G', 'T']:
            baseline = contents(root/f'a-{gain}-inf-mean-norm0-preavg0-casa.cal')
            averaged = contents(root/f'a-{gain}-inf-mean-norm0-preavg20-casa.cal')
            assert relative_rms(averaged['CPARAM'], baseline['CPARAM']) > 0.002
    elif args.case == 'refant':
        source = amplitude_data(root, source)
        results = {f'{gain}-{mode}': gain_and_apply(root, source, native, gain, 'inf', calmode=mode, refant='')
                   for gain in ['G', 'T'] for mode in ['a', 'p', 'ap']}
        for gain in ['G', 'T']:
            expected = contents(root/f'ap-{gain}-inf-mean-norm0-casa.cal')
            np.testing.assert_array_equal(expected['ANTENNA2'], -1)
    elif args.case == 'ranked-refant':
        results = ranked_references(root, amplitude_data(root, source), native)
    elif args.case == 'combined-amplitude':
        results = {str(first): alternating_fields(root, source, native, first, 'a') for first in [1, 0]}
    elif args.case == 'finite-averaging':
        results = alternating_fields(root, source, native, 0, 'a', short_interval=True)
    elif args.case == 'spw-refant':
        results = spectral_window_reference(root, amplitude_data(root, source), native)
    elif args.case == 'corrflags':
        results = correlation_flags(root, amplitude_data(root, source), native)
    elif args.case == 'robust':
        results = robust_gains(root, amplitude_data(root, source), native)
    elif args.case == 'robust-edges':
        results = robust_edge_cases(root, amplitude_data(root, source), native)
    elif args.case == 'selection':
        results = solve_selections(root, amplitude_data(root, source), native)
    elif args.case == 'grouping':
        results = solve_grouping(root, amplitude_data(root, source), native)
    elif args.case == 'point-models':
        results = point_models(root, amplitude_data(root, source), native)
    elif args.case == 'spectral-weights':
        results = spectral_weights(root, amplitude_data(root, source), native)
    elif args.case == 'virtual-components':
        results = virtual_components(root, amplitude_data(root, source), native)
    elif args.case == 'virtual-fourier':
        results = virtual_fourier(root, amplitude_data(root, source), native)
    elif args.case == 'descriptions':
        results = shared_window_descriptions(root, amplitude_data(root, source), native)
    elif args.case == 'preapply':
        results = solve_preapply(root, amplitude_data(root, source), native)
    elif args.case == 'single-hand':
        results = single_hand_gain(root, amplitude_data(root, source), native)
    elif args.case == 'ephemeris':
        results = ephemeris_parallactic_angle(root, source, native)
    else:
        results = {str(first): alternating_fields(root, source, native, first) for first in [1, 0]}
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
