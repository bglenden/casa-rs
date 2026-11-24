#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check application of leakage calibration to polarized CASA visibilities."""

import argparse
from pathlib import Path
import signal

import casatasks as casa
from casatools import calibrater
import numpy as np

from applycal_oracle import copy_ms, isolated_applycal
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms

LEAKAGE = np.array([
    [.07+.055j, -.022+.061j], [.04+.015j, .055-.02j], [.01+.04j, .03+.02j],
    [.028+.017j, .046-.02j], [.04+.02j, .03+.01j],
], np.complex64)
GAIN = np.array([
    [1.1+.1j, .92-.09j], [.89+.14j, .96-.11j], [1.12+.02j, .93-.05j],
    [.98+.04j, 1.03-.04j], [1.+.09j, 1.09-.09j],
], np.complex64)
INPUT = np.array([1.1+.2j, .12+.07j, -.17+.03j, .8-.11j], np.complex64)
CHANNELS = 8


def plant_ms(path, basis):
    make_cal_ms(str(path), stokes=basis)
    with opened(path, True) as table:
        rows = table.nrows()
        data = np.broadcast_to(INPUT[:, None, None], (4, CHANNELS, rows)).copy()
        data *= (1.0 + .02j*np.arange(CHANNELS))[None, :, None]
        table.putcol('DATA', data)
        flags = np.zeros(data.shape, bool)
        flags[0, 1, 0] = True
        flags[1, 2, 0] = True
        table.putcol('FLAG', flags)
        table.putcol('FLAG_ROW', np.zeros(rows, bool))
        sigma = np.broadcast_to(np.array([.8, .7, .6, .9])[:, None], (4, rows)).copy()
        table.putcol('SIGMA', sigma)
        table.putcol('WEIGHT', 1. / sigma**2)
        return float(table.getcol('TIME').min())


def make_table(vis, table, subtype, gains, epoch, *, flagged=False, missing=False):
    tool = calibrater()
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    assert tool.createcaltable(str(table), 'Complex', subtype, True)
    assert tool.close()
    channelized = subtype.startswith('Df')
    channel_scale = (1.0 + .07*np.arange(CHANNELS) if channelized else np.ones(1))
    values = gains.T[:, None, :] * channel_scale[None, :, None]
    if missing:
        values = values[:, :, 1:]
    rows = values.shape[2]
    flags = np.zeros(values.shape, bool)
    if flagged:
        flags[0, 0, 0] = True
    with opened(table, True) as tab:
        tab.addrows(rows)
        for name in ('FIELD_ID', 'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'OBSERVATION_ID'):
            tab.putcol(name, np.zeros(rows, np.int32))
        tab.putcol('ANTENNA1', np.arange(int(missing), rows + int(missing), dtype=np.int32))
        tab.putcol('ANTENNA2', np.full(rows, -1, np.int32))
        tab.putcol('TIME', np.full(rows, epoch))
        tab.putcol('INTERVAL', np.full(rows, 10.))
        tab.putcol('CPARAM', values)
        tab.putcol('FLAG', flags)
        tab.putcol('PARAMERR', np.zeros(values.shape, np.float32))
        tab.putcol('SNR', np.full(values.shape, 10., np.float32))
    if channelized:
        with opened(vis/'SPECTRAL_WINDOW') as origin, opened(table/'SPECTRAL_WINDOW', True) as spw:
            for name in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
                spw.putcell(name, 0, origin.getcell(name, 0))


def result(path):
    with opened(path) as table:
        data = table.getcol('CORRECTED_DATA') if 'CORRECTED_DATA' in table.colnames() else None
        spectrum = table.getcol('WEIGHT_SPECTRUM') if 'WEIGHT_SPECTRUM' in table.colnames() else None
        return data, table.getcol('FLAG'), table.getcol('WEIGHT'), spectrum


def compare_application(root, source, native, label, tables, weights, **options):
    outcomes = []
    for prefix, api in (('casa', casa), ('native', native)):
        ms = root/f'{prefix}-{label}.ms'
        copy_ms(source, ms)
        assert api.applycal(str(ms), gaintable=[str(table) for table in tables],
                            calwt=weights, flagbackup=False, **options) is None
        outcomes.append(result(ms))
    (want, want_flags, want_weight, want_spectrum), (got, got_flags, got_weight, got_spectrum) = outcomes
    np.testing.assert_array_equal(got_flags, want_flags, err_msg=f'{label} FLAG')
    np.testing.assert_allclose(got_weight, want_weight, rtol=3e-6, atol=2e-7, err_msg=f'{label} WEIGHT')
    if want_spectrum is None:
        assert got_spectrum is None
    else:
        np.testing.assert_allclose(got_spectrum, want_spectrum, rtol=3e-6, atol=2e-7,
                                   err_msg=f'{label} WEIGHT_SPECTRUM')
    if want is None:
        assert got is None
        return 0., None
    np.testing.assert_allclose(got, want, rtol=3e-6, atol=3e-7, err_msg=f'{label} CORRECTED_DATA')
    return float(np.linalg.norm(got-want) / np.linalg.norm(want)), want


def leakage_law(value, left, right, full):
    if full:
        inverse = lambda pair: np.linalg.inv([[1., pair[0]], [pair[1], 1.]])
        return (inverse(left) @ value.reshape(2, 2) @ inverse(right).conj().T).ravel()
    xx, xy, yx, yy = value
    return np.array([xx, xy-left[0]*yy-right[1].conjugate()*xx,
                     yx-left[1]*xx-right[0].conjugate()*yy, yy])


def model_preapply(root, source, native, tables, parang, *, full_matrix=True, corrdepflags=False):
    vis = root/'solve.ms'
    copy_ms(source, vis)
    tool = calibrater()
    assert tool.open(str(vis), addcorr=False, addmodel=True)
    assert tool.close()
    with opened(vis, True) as table:
        model = np.broadcast_to(INPUT[:, None, None], (4, CHANNELS, table.nrows())).copy()
        model *= (1.+.03*np.arange(CHANNELS))[None, :, None]
        table.putcol('MODEL_DATA', model)
        antennas = [table.getcol(column) for column in ('ANTENNA1', 'ANTENNA2')]
        flags = np.zeros(model.shape, bool)
        table.putcol('FLAG', flags)
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    for term in tables:
        assert tool.setapply(table=str(term))
    if parang:
        assert tool.setapply(type='P')
    assert tool.corrupt()
    assert tool.close()
    with opened(vis, True) as table:
        corrupted = table.getcol('MODEL_DATA')
        if full_matrix:
            assert np.max(np.abs(corrupted[[0, 3]] - model[[0, 3]])) > .005
        left, right = [GAIN[indices].T for indices in antennas]
        factor = (left[:, None, :]*right[None, :, :].conj()).reshape(4, -1)
        table.putcol('DATA', corrupted * factor[:, None, :])
        table.putcol('MODEL_DATA', model)
    outcomes = []
    for name, api in (('casa', casa), ('native', native)):
        product = root/f'{name}-solve.cal'
        api.gaincal(str(vis), str(product), field='0', gaintype='G', calmode='ap',
                    minblperant=3, minsnr=0., refant='A0', solint='inf', parang=parang,
                    corrdepflags=corrdepflags,
                    gaintable=[str(term) for term in tables], interp=['linear']*len(tables))
        with opened(product) as table:
            gains = table.getcol('CPARAM')
            flags = table.getcol('FLAG')
            antenna = table.getcol('ANTENNA1')
            assert gains.shape == (2, 1, len(GAIN)) and not flags.any()
            expected = GAIN[antenna].T * np.exp(-1j*np.angle(GAIN[0]))[:, None]
            np.testing.assert_allclose(gains[:, 0, :], expected, rtol=1e-4, atol=8e-5,
                                       err_msg=f'{name} polarized MODEL_DATA preapply')
            outcomes.append(gains)
    return float(np.linalg.norm(outcomes[1]-outcomes[0]) / np.linalg.norm(outcomes[0]))


def run_basis(root, basis, native):
    root.mkdir()
    vis = root/'source.ms'
    epoch = plant_ms(vis, basis)
    gain = root/'gain.cal'
    make_table(vis, gain, 'G Jones', GAIN, epoch)
    with opened(vis) as table:
        first = table.getcell('DATA', 0)[:, 0]
        i, j = [int(table.getcell(column, 0)) for column in ('ANTENNA1', 'ANTENNA2')]
    errors = []
    for subtype in ('D Jones', 'Df Jones', 'DLLS Jones', 'DfLLS Jones', 'Dgen Jones', 'Dfgen Jones'):
        label = subtype.partition(' ')[0].lower()
        leakage = root/f'{label}.cal'
        make_table(vis, leakage, subtype, LEAKAGE, epoch)
        error, reference = compare_application(root, vis, native, label, [leakage], [True])
        errors.append(error)
        expected = leakage_law(first, LEAKAGE[i], LEAKAGE[j], subtype not in ('D Jones', 'Df Jones'))
        np.testing.assert_allclose(reference[:, 0, 0], expected, rtol=3e-6, atol=3e-7)
    linear = root/'d.cal'
    expected = leakage_law(first/(GAIN[i, :, None]*GAIN[j, None, :].conj()).ravel(),
                            LEAKAGE[i], LEAKAGE[j], False)
    for name, order, weights in [('gd', [gain, linear], [True, True]),
                                 ('dg', [linear, gain], [True, True])]:
        error, reference = compare_application(root, vis, native, name, order, weights)
        errors.append(error)
        np.testing.assert_allclose(reference[:, 0, 0], expected, rtol=3e-6, atol=3e-7)
        error, _ = compare_application(root, vis, native, name+'p', order, weights, parang=True)
        errors.append(error)

    for subtype in ('D Jones', 'DLLS Jones', 'Df Jones', 'DfLLS Jones'):
        label = subtype.partition(' ')[0].lower()
        flagged = root/f'{label}-flagged.cal'
        make_table(vis, flagged, subtype, LEAKAGE, epoch, flagged=True)
        for mode in ('calflag', 'calonly', 'flagonly'):
            error, _ = compare_application(root, vis, native, f'{label}-flagged-{mode}',
                                           [flagged], [True], applymode=mode)
            errors.append(error)
    missing = root/'missing.cal'
    make_table(vis, missing, 'D Jones', LEAKAGE, epoch, missing=True)
    error, _ = compare_application(root, vis, native, 'missing', [missing], [True])
    errors.append(error)
    error, _ = compare_application(root, vis, native, 'channel-range', [root/'df.cal'], [True],
                                   spw='0:2~6')
    errors.append(error)
    time_variable = root/'df-time-variable.cal'
    copy_ms(root/'df.cal', time_variable)
    with opened(time_variable, True) as table:
        size = table.nrows()
        cells = {name: table.getcol(name) for name in table.colnames()
                 if all(table.iscelldefined(name, row) for row in range(size))}
        table.addrows(size)
        for name, first in cells.items():
            last = first
            if name == 'TIME':
                last = first + 180.
            elif name == 'CPARAM':
                last = first * (.85+.30j)
            table.putcol(name, np.concatenate([first, last], axis=-1))
    error, _ = compare_application(root, vis, native, 'df-time-variable', [time_variable], [True])
    errors.append(error)
    spectral = root/'spectral.ms'
    copy_ms(vis, spectral)
    with opened(spectral, True) as table:
        shape = table.getcol('DATA').shape
        sigma = np.broadcast_to(np.linspace(1., 3., CHANNELS)[None, :, None], shape).astype(np.float32)
        for name, values in [('SIGMA_SPECTRUM', sigma), ('WEIGHT_SPECTRUM', 1./sigma**2)]:
            info = table.getcoldesc('DATA')
            info.update(name=name, valueType='float', dataManagerType='StandardStMan', dataManagerGroup=name)
            table.addcols({name: info})
            table.putcol(name, values)
    error, _ = compare_application(root, spectral, native, 'spectral-leakage', [linear], [True])
    errors.append(error)
    error, _ = compare_application(root, spectral, native, 'spectral-gains', [linear, gain], [True, True])
    errors.append(error)
    nonfinite = root/'dlls-nan.cal'
    copy_ms(root/'dlls.cal', nonfinite)
    with opened(nonfinite, True) as table:
        row = table.getcell('CPARAM', 0)
        row[0, 0] = np.nan
        table.putcell('CPARAM', 0, row)
    error, reference = compare_application(root, vis, native, 'dlls-nan', [nonfinite], [True])
    assert np.isfinite(reference[:, 0, 0]).all()
    assert result(root/'native-dlls-nan.ms')[1][:, 0, 0].all()
    errors.append(error)
    print(f'{basis}: {len(errors)} cases, maximum visibility normalized RMS {max(errors):.6g}')
    for parang in (False, True):
        solve = root/f'model-solve-parang-{int(parang)}'
        solve.mkdir()
        priors = [root/'dlls.cal', gain] if parang else [root/'dlls.cal']
        error = model_preapply(solve, vis, native, priors, parang)
        print(f'{basis}, ordered MODEL_DATA prior solve, parang={parang}: gain normalized RMS {error:.6g}')
    for corrdepflags in (False, True):
        solve = root/f'model-solve-calibration-flags-{int(corrdepflags)}'
        solve.mkdir()
        error = model_preapply(solve, vis, native, [root/'d-flagged.cal'], False,
                               full_matrix=False, corrdepflags=corrdepflags)
        print(f'{basis}, cross-hand prior FLAG, corrdepflags={corrdepflags}: gain normalized RMS {error:.6g}')


def unsupported_layout(root, native):
    for name, api in [('casa', casa), ('native', native)]:
        vis = root/f'{name}-parallel-only.ms'
        make_cal_ms(str(vis), stokes='XX YY')
        try:
            api.applycal(str(vis), gaintable=[str(root/'linear'/'d.cal')], flagbackup=False)
        except RuntimeError:
            continue
        raise AssertionError(f'{name} accepted D Jones with missing cross-hand products')


def permuted_layout(root, native):
    source = root/'linear'/'source.ms'
    caltable = root/'linear'/'d.cal'
    expected, flags, _, _ = result(root/'linear'/'casa-d.ms')
    order = [0, 3, 1, 2]
    for name, api in [('casa', casa), ('native', native)]:
        vis = root/f'{name}-permuted.ms'
        copy_ms(source, vis)
        with opened(vis/'POLARIZATION', True) as table:
            table.putcell('CORR_TYPE', 0, table.getcell('CORR_TYPE', 0)[order])
            table.putcell('CORR_PRODUCT', 0, table.getcell('CORR_PRODUCT', 0)[:, order])
        with opened(vis, True) as table:
            for column in ('DATA', 'CORRECTED_DATA', 'FLAG', 'SIGMA', 'WEIGHT'):
                if column in table.colnames():
                    table.putcol(column, table.getcol(column)[order])
        api.applycal(str(vis), gaintable=[str(caltable)], calwt=False, flagbackup=False)
    got, got_flags, _, _ = result(root/'native-permuted.ms')
    positional, positional_flags, _, _ = result(root/'casa-permuted.ms')
    np.testing.assert_allclose(got, expected[order], rtol=3e-6, atol=3e-7)
    np.testing.assert_array_equal(got_flags, flags[order])
    assert np.linalg.norm(positional-expected[order]) / np.linalg.norm(expected) > .05
    assert np.any(positional_flags != flags[order])


def malformed_table(root, native):
    caltable = root/'one-receptor.cal'
    copy_ms(root/'linear'/'d.cal', caltable)
    with opened(caltable, True) as table:
        for column in ('CPARAM', 'FLAG', 'PARAMERR', 'SNR'):
            for row in range(table.nrows()):
                table.putcell(column, row, table.getcell(column, row)[:1])
    casa_ms, native_ms = [root/f'{name}-malformed.ms' for name in ('casa', 'native')]
    for vis in (casa_ms, native_ms):
        copy_ms(root/'linear'/'source.ms', vis)
    args = {'vis': str(casa_ms), 'gaintable': [str(caltable)], 'flagbackup': False}
    probe = isolated_applycal(args)
    assert probe.returncode == -signal.SIGABRT and 'ArrayShapeError' in probe.stderr
    try:
        native.applycal(**dict(args, vis=str(native_ms)))
    except RuntimeError as error:
        assert 'leakage requires 2 CPARAM elements, found shape [1, 1] at row 0' in str(error)
    else:
        raise AssertionError('Native applycal accepted a one-receptor leakage table')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    root = args.work.resolve()
    root.mkdir()
    for basis, label in [('RR RL LR LL', 'circular'), ('XX XY YX YY', 'linear')]:
        run_basis(root/label, basis, tasks)
    unsupported_layout(root, tasks)
    permuted_layout(root, tasks)
    malformed_table(root, tasks)
    print('Missing cross-hands, permuted correlation metadata, and malformed leakage table controls passed.')


if __name__ == '__main__':
    main()
