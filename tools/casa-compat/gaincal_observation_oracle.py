#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise time-binned point-source gaincal through the installed task."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tarfile

import casatasks as casa
from casatools import table
import numpy as np

from calibration_lifecycle_oracle import contents
from gaincal_oracle import relative_rms, solve
from record_casa_returns import make_cal_ms


TRIAL = (Path(__file__).resolve().parents[2]
         / 'libs/casacore/casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz')
TIME_BINNED_G = dict(gaintype='G', calmode='ap', solint='60s', refant='0',
                     refantmode='strict', minsnr=0., minblperant=1)
IDENTITY_COLUMNS = ('TIME', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID', 'OBSERVATION_ID',
                    'SCAN_NUMBER', 'SPECTRAL_WINDOW_ID')


@contextmanager
def editable(path):
    tool = table()
    assert tool.open(str(path), nomodify=False)
    try:
        yield tool
    finally:
        tool.close()


def extract_trial(root):
    with tarfile.open(TRIAL) as archive:
        archive.extractall(root, filter='data')
    return root/'ms_trial.ms'


def compare_gains(actual, expected):
    keys = ['ANTENNA1', 'TIME', 'SCAN_NUMBER', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'OBSERVATION_ID']
    actual_order = np.lexsort([actual[key] for key in keys])
    expected_order = np.lexsort([expected[key] for key in keys])
    for name in ['ANTENNA1', 'ANTENNA2', 'FIELD_ID', 'OBSERVATION_ID', 'SCAN_NUMBER',
                 'SPECTRAL_WINDOW_ID', 'FLAG']:
        np.testing.assert_array_equal(actual[name][..., actual_order],
                                      expected[name][..., expected_order], err_msg=name)
    valid = ~expected['FLAG'][..., expected_order]
    error = relative_rms(actual['CPARAM'][..., actual_order][valid],
                         expected['CPARAM'][..., expected_order][valid])
    assert error < 1e-3, f'CPARAM relative RMS: {error:.6g}'
    np.testing.assert_allclose(actual['TIME'][actual_order], expected['TIME'][expected_order],
                               rtol=0., atol=2e-6)
    return {'rows': int(expected_order.size), 'gain_relative_rms': error}


def mixed_weights(root, native, options):
    source = extract_trial(root)
    with editable(source) as tool:
        shape = tool.getcell('DATA', 0).shape
        for column in ('SIGMA_SPECTRUM', 'WEIGHT_SPECTRUM'):
            if column in tool.colnames():
                tool.removecols(column)
            desc = tool.getcoldesc('DATA')
            desc.update(valueType='float', dataManagerType='StandardStMan',
                        dataManagerGroup=column, option=0)
            desc.pop('shape', None)
            assert tool.addcols({column: desc})
            sigma = column == 'SIGMA_SPECTRUM'
            spectrum = np.ones(shape, dtype=np.float32)
            if sigma:
                spectrum[:, ::2] = 12.
            for row in range(int(sigma), tool.nrows()):
                assert tool.putcell(column, row, spectrum)
        data = tool.getcol('DATA')
        data[:, ::2, :] *= 3.
        if options['calmode'] == 'p':
            baselines = tool.getcol('ANTENNA1') - tool.getcol('ANTENNA2')
            data[:, ::2, :] *= np.exp(0.27j * baselines)
        assert tool.putcol('DATA', data)

    expected = solve(casa, root, 'casa', source, **options)
    actual = solve(native, root, 'native', source, **options)
    return compare_gains(actual, expected)


def legacy_yy_append(root, native, options):
    source = root/'yy.ms'
    make_cal_ms(str(source), stokes='YY')
    options = options | dict(refant='', refantmode='flex', minblperant=2)
    expected = solve(casa, root, 'casa', source, **options)
    return append_legacy_yy(root, native, source, expected, options)


def append_legacy_yy(root, native, vis, expected, options):
    solve(native, root, 'native', vis, **options)
    destination = root/'native.cal'
    with editable(destination) as tool:
        # Older native G tables stored YY in slot one.
        assert tool.putkeyword('CASA_Version', 'casa-rs')
        for column in ('CPARAM', 'FLAG', 'PARAMERR', 'SNR'):
            assert tool.putcol(column, tool.getcol(column)[::-1].copy())

    initial = contents(destination)
    appended = solve(native, root, 'native', vis, append=True, **options)
    rows = initial['FLAG'].shape[-1]
    assert rows == 10
    assert appended['FLAG'].shape[-1] == 2 * rows
    # The CASA writer sorts by row identity at append. Original and added
    # rows with the same identity are adjacent, with the original first.
    for column in (*IDENTITY_COLUMNS, 'CPARAM', 'FLAG', 'PARAMERR', 'SNR'):
        np.testing.assert_array_equal(appended[column][..., ::2], initial[column])
    legacy = {name: appended[name][..., 1::2] for name in (*IDENTITY_COLUMNS, 'FLAG', 'CPARAM')}
    np.testing.assert_array_equal(np.sum(~legacy['FLAG'], axis=(1, 2)), [0, rows])
    if options['refant'] == '0':
        np.testing.assert_array_equal(legacy['ANTENNA2'], np.zeros(rows, dtype=np.int32))
    for column in ('FLAG', 'CPARAM'):
        legacy[column] = legacy[column][::-1]
    return compare_gains(legacy, expected)


def legacy_yy_reference(root, native, options):
    source = root/'yy.ms'
    make_cal_ms(str(source), stokes='YY')
    converted = root/'yy.ps.zarr'
    native.msconvert(vis=str(source), outputvis=str(converted), outformat='msv4', withpointing=False)
    options = options | dict(refant='0', minblperant=2)
    results = {}
    for refantmode in ('strict', 'flex'):
        settings = options | dict(refantmode=refantmode)
        expected = solve(casa, root, f'casa-{refantmode}', source, **settings)
        for fmt, vis in [('MSv2', source), ('MSv4', converted)]:
            case = root/f'{fmt}-{refantmode}'
            case.mkdir()
            results[case.name] = append_legacy_yy(case, native, vis, expected, settings)
    return results


def observed_field_scan_pairs(root, native, options):
    source = extract_trial(root)
    for subtable, column, value in [('FIELD', 'NAME', 'second-field'),
                                    ('STATE', 'OBS_MODE', 'CALIBRATE_PHASE.ON_SOURCE')]:
        path = source/subtable
        with editable(path) as tool:
            assert tool.nrows() == 1
            assert tool.copyrows(str(path), startrowin=0, startrowout=1, nrow=1)
            assert tool.putcell(column, 1, value)
    with editable(source) as tool:
        _, times = np.unique(tool.getcol('TIME'), return_inverse=True)
        positions = times % 4
        # Both intent partitions have both fields and scans. Their actual
        # field/scan pairs are disjoint.
        for column, pattern in [('STATE_ID', [0, 0, 1, 1]), ('FIELD_ID', [0, 1, 0, 1]),
                                ('SCAN_NUMBER', [1, 2, 2, 1])]:
            assert tool.putcol(column, np.array(pattern, dtype=np.int32)[positions])

    converted = root/'converted.ps.zarr'
    native.msconvert(vis=str(source), outputvis=str(converted), outformat='msv4', withpointing=False)
    options = options | dict(solint='15s')
    expected = solve(casa, root, 'casa', source, **options)
    assert set(zip(expected['FIELD_ID'], expected['SCAN_NUMBER'])) == {(0, 1), (0, 2), (1, 1), (1, 2)}
    assert expected['TIME'].size == 60
    return {fmt: compare_gains(solve(native, root, fmt, vis, **options), expected)
            for fmt, vis in [('MSv2', source), ('MSv4', converted)]}


def phase_flags(root, native, options):
    source = extract_trial(root)
    converted = root/'converted.ps.zarr'
    native.msconvert(vis=str(source), outputvis=str(converted), outformat='msv4', withpointing=False)
    options = options | dict(calmode='p', minsnr=22., minblperant=4)
    expected = solve(casa, root, 'casa', source, **options)
    flags = expected['FLAG']
    assert flags.any() and (~flags).any()
    results = {}
    for fmt, vis in [('MSv2', source), ('MSv4', converted)]:
        actual = solve(native, root, fmt, vis, **options)
        results[fmt] = compare_gains(actual, expected)
        np.testing.assert_array_equal(actual['CPARAM'][actual['FLAG']], 1. + 0.j)
        results[fmt]['flagged_solutions'] = int(actual['FLAG'].sum())
    return results


CASES = (mixed_weights, legacy_yy_append, legacy_yy_reference, observed_field_scan_pairs, phase_flags)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--case', choices=[case.__name__ for case in CASES])
    parser.add_argument('--calmode', choices=['ap', 'p', 'a'], default='ap')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    os.chdir(root)
    options = TIME_BINNED_G | dict(calmode=args.calmode)
    for case in CASES:
        if case is phase_flags and args.case is None and args.calmode != 'p':
            continue
        if args.case is None or args.case == case.__name__:
            directory = root/case.__name__
            directory.mkdir()
            print(json.dumps({case.__name__: case(directory, native, options)}), flush=True)


if __name__ == '__main__':
    main()
