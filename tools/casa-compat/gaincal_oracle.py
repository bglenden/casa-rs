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


def solve(module, root, label, source, refant='A0', minsnr=0., **options):
    caltable = root/f'{label}.cal'
    preload = os.environ.pop('LD_PRELOAD', None) if module is not casa else None
    try:
        module.gaincal(vis=str(source), caltable=str(caltable), refant=refant,
                       smodel=[1., 0., 0., 0.], minsnr=minsnr, **options)
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


def gain_and_apply(root, source, native, gaintype, solint, solnorm=False, normtype='mean', calmode='a', compare_flagged_gains=True, **extra):
    label = f'{calmode}-{gaintype}-{solint}-{normtype}-norm{int(solnorm)}'
    if 'preavg' in extra:
        label += f'-preavg{extra["preavg"]:g}'
    options = dict(gaintype=gaintype, calmode=calmode, solint=solint, solnorm=solnorm,
                   normtype=normtype, **extra)
    solutions, corrected = [], []
    for engine, module in [('casa', casa), ('native', native)]:
        case = f'{label}-{engine}'
        vis = root/f'{case}.ms'
        copy_ms(source, vis)
        solutions.append(solve(module, root, case, vis, **options))
        preload = os.environ.pop('LD_PRELOAD', None) if module is native else None
        try:
            module.applycal(vis=str(vis), gaintable=[str(root/f'{case}.cal')], flagbackup=False)
        finally:
            if preload is not None:
                os.environ['LD_PRELOAD'] = preload
        corrected.append(contents(vis))

    expected, actual = solutions
    for name in ['FIELD_ID', 'ANTENNA1', 'ANTENNA2', 'SCAN_NUMBER', 'SPECTRAL_WINDOW_ID', 'FLAG']:
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
    finally:
        t.close()

    results = {}
    for name, interval, expected_field, options in [
        ('intent', 'inf', [1], dict(intent='*PHASE*')),
        ('uvrange', 'inf', [0, 1], dict(uvrange='>20m')),
        ('observation-number', 'inf', [1], dict(observation=int(observation))),
        ('solint-number', 25.0, [0, 1], {}),
        ('taql', 'inf', [1], dict(msselect='FIELD_ID == 1')),
        ('disabled', 'inf', [1], dict(selectdata=False, intent='*PHASE*', scan='4000',
                                     observation=9000, antenna='MISSING', uvrange='>10000km')),
    ]:
        case = root/name
        case.mkdir()
        results[name] = gain_and_apply(case, source, native, 'G', interval,
                                       calmode='ap', minblperant=2, **options)
        expected = contents(case/f'ap-G-{interval}-mean-norm0-casa.cal')
        np.testing.assert_array_equal(np.unique(expected['FIELD_ID']), expected_field)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--case', choices=['intervals', 'combined-amplitude', 'finite-averaging', 'amplitude', 'median', 'preavg', 'refant', 'ranked-refant', 'spw-refant', 'corrflags', 'robust', 'robust-edges', 'selection'], default='intervals')
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
    make_cal_ms(str(source))
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
    else:
        results = {str(first): alternating_fields(root, source, native, first) for first in [1, 0]}
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
