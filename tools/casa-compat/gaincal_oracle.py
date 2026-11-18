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


def solve(module, root, label, source, **options):
    caltable = root/f'{label}.cal'
    preload = os.environ.pop('LD_PRELOAD', None) if module is not casa else None
    try:
        module.gaincal(vis=str(source), caltable=str(caltable), refant='A0',
                       smodel=[1., 0., 0., 0.], minsnr=0., **options)
    finally:
        if preload is not None:
            os.environ['LD_PRELOAD'] = preload
    return contents(caltable)


def relative_rms(a, b):
    return float(np.linalg.norm(a-b)/max(np.linalg.norm(b), 1e-30))


def alternating_fields(root, source, native, first):
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
        t.putcol('FIELD_ID', ((first+index)%2).astype(np.int32))
        t.putcol('SCAN_NUMBER', np.full(len(index), 1, dtype=np.int32))
        data = t.getcol('DATA')
        data[:] = (1.+.2*index)[None, None, :]
        t.putcol('DATA', data)
        t.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        t.putcol('FLAG_ROW', np.zeros(len(index), dtype=bool))
    finally:
        t.close()

    options = dict(gaintype='G', calmode='ap', solint='25s', combine='field')
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
    np.testing.assert_allclose(records['casa']['interval_times_s'], [10., 35., 60., 85., 105.], atol=1e-4)
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    for column in ['FIELD_ID', 'ANTENNA1', 'SCAN_NUMBER', 'SPECTRAL_WINDOW_ID', 'FLAG']:
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    records['cparam_relative_rms'] = relative_rms(actual['CPARAM'], expected['CPARAM'])
    assert records['cparam_relative_rms'] < 1e-3, records
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
        phase = (.09+.026*channel+.023*time)*(a-b)
        data[:] = amplitude*(1.+.09*receptor)*np.exp(1j*(phase+.04*receptor*(a-b)))
        t.putcol('DATA', data)
        t.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        t.putcol('FLAG_ROW', np.zeros(len(a), dtype=bool))
    finally:
        t.close()
    return path


def amplitude_only(root, source, native, gaintype, solint, solnorm):
    label = f'amplitude-{gaintype}-{solint}-norm{int(solnorm)}'
    options = dict(gaintype=gaintype, calmode='a', solint=solint, solnorm=solnorm)
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
    for name in ['FIELD_ID', 'ANTENNA1', 'SCAN_NUMBER', 'SPECTRAL_WINDOW_ID', 'FLAG']:
        np.testing.assert_array_equal(actual[name], expected[name], err_msg=f'{label} {name}')
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    assert np.all(expected['CPARAM'].real > 0.) and np.any(expected['PARAMERR'] > 0.)
    np.testing.assert_allclose(actual['CPARAM'].imag, 0., rtol=0., atol=1e-7)
    errors = {name: relative_rms(actual[name], expected[name])
              for name in ['CPARAM', 'PARAMERR', 'SNR']}
    expected_ms, actual_ms = corrected
    errors['CORRECTED_DATA'] = relative_rms(actual_ms['CORRECTED_DATA'], expected_ms['CORRECTED_DATA'])
    errors['WEIGHT'] = relative_rms(actual_ms['WEIGHT'], expected_ms['WEIGHT'])
    np.testing.assert_array_equal(actual_ms['FLAG'], expected_ms['FLAG'], err_msg=label)
    assert max(errors.values()) < 1e-3, (label, errors)
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--case', choices=['intervals', 'amplitude'], default='intervals')
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
        results = {f'{gain}-{interval}-norm{int(norm)}': amplitude_only(root, source, native, gain, interval, norm)
                   for gain in ['G', 'T'] for interval, norm in [('int', False), ('inf', False), ('inf', True)]}
    else:
        results = {str(first): alternating_fields(root, source, native, first) for first in [1, 0]}
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
