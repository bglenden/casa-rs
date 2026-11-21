#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA's gain-ratio Stokes estimates and corrected complex gains."""

import argparse
import inspect
from pathlib import Path

import casatasks as casa
from casatools import calibrater, measures, msmetadata
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms

Q_U = np.array([[[.084, -.037], [.061, .052]], [[-.051, .029], [.037, -.064]]])


def prepare_metadata(vis):
    make_cal_ms(str(vis))
    with opened(vis / 'SPECTRAL_WINDOW', True) as spw:
        spw.addrows(1)
        for name in spw.colnames():
            if spw.iscelldefined(name, 0):
                spw.putcell(name, 1, spw.getcell(name, 0))
        spw.putcell('NAME', 1, 'SPW1')
    with opened(vis / 'FEED', True) as feed:
        antennas = feed.getcol('ANTENNA_ID')
        angles = .17 + .02 * antennas
        feed.putcol('SPECTRAL_WINDOW_ID', np.full(feed.nrows(), -1, np.int32))
        feed.putcol('RECEPTOR_ANGLE', np.stack((angles, angles + np.pi / 2)))
    return dict(zip(antennas.tolist(), angles.tolist()))


def sample_angle(me, latitude, epoch, direction, feed_angle):
    last = me.measure(me.epoch('UTC', f'{epoch}s'), 'LAST')['m0']['value']
    ra, dec = direction
    ha = (last % 1.) * 2 * np.pi - ra
    angle = np.arctan2(np.cos(latitude) * np.sin(ha),
                       np.sin(latitude) * np.cos(dec) - np.cos(latitude) * np.sin(dec) * np.cos(ha))
    return angle + feed_angle


def plant_gains(vis, gains, feed_angles):
    cb = calibrater()
    assert cb.open(str(vis), addcorr=False, addmodel=False)
    assert cb.createcaltable(str(gains), 'Complex', 'G Jones', True)
    assert cb.close()
    with opened(gains / 'FIELD') as table:
        directions = table.getcol('DELAY_DIR')[:, 0, :].T
    with opened(vis) as table:
        first = min(table.getcol('TIME'))
    md = msmetadata()
    me = measures()
    assert md.open(str(vis))
    indices = []
    values = []
    flags = []
    angles = []
    for antenna in range(5):
        position = md.antennaposition(antenna)
        latitude = me.measure(position, 'WGS84')['m1']['value']
        assert me.doframe(position)
        for field in range(2):
            for window in range(2):
                for slot in range(6):
                    time = first + 4200. * slot
                    angle = sample_angle(me, latitude, time, directions[field],
                                         feed_angles[antenna] if window == 1 else 0.)
                    q, u = Q_U[field, window]
                    if antenna == 4:
                        q, u = q + .045, u + .023
                    ratio = 1.25 * (1 + 2*q*np.cos(2*angle) + 2*u*np.sin(2*angle))
                    polar = np.array([[np.sqrt(ratio)*np.exp(.07j*slot), 2+.3j, .9+.2j],
                                      [np.exp(-.12j*slot), 1+.8j, 4+.2j]], dtype=np.complex64)
                    flagged = np.zeros((2, 3), bool)
                    flagged[slot % 2, 0] = (antenna == 4 and slot > 1) or (antenna == 2 and slot == 3)
                    flagged[0, 2] = slot % 2 == 0
                    indices.append((time, field, window, antenna))
                    values.append(polar)
                    flags.append(flagged)
                    angles.append(angle)
    md.done()
    me.done()
    data = np.stack(values, axis=-1)
    with opened(gains, True) as table:
        nrows = len(indices)
        table.addrows(nrows)
        time, fld, spw, ant = zip(*indices)
        table.putcol('TIME', np.array(time))
        for name, column in (('FIELD_ID', fld), ('SPECTRAL_WINDOW_ID', spw), ('ANTENNA1', ant),
                             ('ANTENNA2', [-1] * nrows), ('SCAN_NUMBER', [0] * nrows),
                             ('OBSERVATION_ID', [0] * nrows)):
            table.putcol(name, np.array(column, dtype=np.int32))
        table.putcol('INTERVAL', np.full(nrows, 4200.))
        table.putcol('CPARAM', data)
        table.putcol('FLAG', np.stack(flags, axis=-1))
        table.putcol('PARAMERR', np.full(data.shape, .01, dtype=np.float32))
        table.putcol('SNR', np.full(data.shape, 20., dtype=np.float32))
    return np.array(indices), data, np.array(angles)


def compare_record(actual, reference):
    assert actual.keys() == reference.keys()
    for source, windows in reference.items():
        assert actual[source].keys() == windows.keys()
        for window, values in windows.items():
            np.testing.assert_allclose(actual[source][window], values, rtol=3e-5, atol=1e-7)


def planted_result(record, offset):
    for field, name in enumerate(('FLUX', 'PHASE')):
        results = []
        for window in (0, 1):
            q, u = Q_U[field, window]
            turn = np.deg2rad(2*offset)
            expected = [1., q*np.cos(turn)-u*np.sin(turn), q*np.sin(turn)+u*np.cos(turn), 0.]
            np.testing.assert_allclose(record[name][f'Spw{window}'], expected, atol=2e-7)
            results.append(expected)
        np.testing.assert_allclose(record[name]['SpwAve'], np.mean(results, axis=0), atol=2e-7)


def compare_tables(actual, reference, original, rows, angles, original_data):
    with opened(reference) as want, opened(actual) as got, opened(original) as source:
        expected = want.getcol('CPARAM')
        values = got.getcol('CPARAM')
        for col in source.colnames():
            if col != 'CPARAM':
                present = [source.iscelldefined(col, row) for row in range(source.nrows())]
                assert present == [want.iscelldefined(col, row) for row in range(want.nrows())]
                assert present == [got.iscelldefined(col, row) for row in range(got.nrows())]
                if all(present):
                    np.testing.assert_array_equal(got.getcol(col), want.getcol(col), err_msg=col)
                    np.testing.assert_array_equal(got.getcol(col), source.getcol(col), err_msg=col)
                else:
                    for row, defined in enumerate(present):
                        if defined:
                            np.testing.assert_array_equal(got.getcell(col, row), source.getcell(col, row), err_msg=col)
        np.testing.assert_array_equal(source.getcol('CPARAM'), original_data)
    np.testing.assert_allclose(values, expected, rtol=3e-6, atol=2e-7)
    np.testing.assert_array_equal(values[:, 1:, :], original_data[:, 1:, :])
    q, u = Q_U[rows[:, 1].astype(int), rows[:, 2].astype(int)].T
    fraction = q*np.cos(2*angles) + u*np.sin(2*angles)
    factors = np.sqrt(1. + np.stack((fraction, -fraction)))
    np.testing.assert_allclose(values[:, 0, :], original_data[:, 0, :] / factors,
                               rtol=3e-6, atol=2e-7)
    assert np.count_nonzero(values[:, 0, :] != original_data[:, 0, :]) == 2 * len(rows)
    return float(np.linalg.norm(values - expected) / np.linalg.norm(expected))


def rank_deficient(root, vis, gains, angles, native):
    rank_one = root / 'rank-one.cal'
    copy_ms(gains, rank_one)
    with opened(rank_one, True) as table:
        group = (table.getcol('FIELD_ID') == 0) & (table.getcol('SPECTRAL_WINDOW_ID') == 0)
        active = group & (table.getcol('ANTENNA1') == 0)
        first = np.flatnonzero(active)[0]
        data = table.getcol('CPARAM')
        flags = table.getcol('FLAG')
        times = table.getcol('TIME')
        flags[:, 0, group & ~active] = True
        times[active] = times[first]
        data[:, 0, active] = data[:, 0, first, None]
        table.putcol('CPARAM', data)
        table.putcol('FLAG', flags)
        table.putcol('TIME', times)
    expected = casa.polfromgain(str(vis), str(rank_one), minpacov=0.)
    actual = native.polfromgain(str(vis), str(rank_one), minpacov=0.)
    compare_record(actual, expected)
    angle = angles[first]
    planted = [1., .5*np.cos(2*angle), .5*np.sin(2*angle), 0.]
    np.testing.assert_allclose(actual['FLUX']['Spw0'], planted, atol=2e-7)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    vis, gains = root / 'polarization.ms', root / 'gains.cal'
    rows, data, angles = plant_gains(vis, gains, prepare_metadata(vis))
    expected = root / 'casa-corrected.cal'
    reference_reports = {}
    for offset, minimum in ((0., 30.), (0., 10.), (25., 10.), (0., 720.)):
        output = str(expected) if offset == 0. and minimum == 10. else ''
        values = (casa.polfromgain(str(vis), str(gains)) if minimum == 30.
                  else casa.polfromgain(str(vis), str(gains), output, offset, minimum))
        if minimum < 720.:
            planted_result(values, offset)
        else:
            assert values == {'FLUX': {}, 'PHASE': {}}
        reference_reports[offset, minimum] = values
    if args.reference_only:
        print('CASA reference recovered planted Stokes vectors, angles, and coverage refusal.')
        return
    from casars import tasks

    expected_params = inspect.signature(casa.polfromgain).parameters
    native_params = inspect.signature(tasks.polfromgain).parameters
    assert list(native_params)[:len(expected_params)] == list(expected_params)
    assert all(native_params[name].default == parameter.default for name, parameter in expected_params.items())
    actual = root / 'native-corrected.cal'
    for (offset, minimum), values in reference_reports.items():
        output = str(actual) if offset == 0. and minimum == 10. else ''
        report = (tasks.polfromgain(str(vis), str(gains)) if minimum == 30.
                  else tasks.polfromgain(str(vis), str(gains), output, offset, minimum))
        compare_record(report, values)
    compare_tables(actual, expected, gains, rows, angles, data)
    for api, output in ((casa, expected), (tasks, actual)):
        try:
            api.polfromgain(str(vis), str(gains), str(output), minpacov=10.)
        except ValueError:
            pass
        else:
            raise AssertionError('polfromgain overwrote an existing output')
    error = compare_tables(actual, expected, gains, rows, angles, data)
    print(f'Corrected gain normalized RMS {error:.6g}')
    rank_deficient(root, vis, gains, angles, tasks)
    print('Two fields and windows, 240 first-plane complex gains, angles, flagged and undersampled antennas, coverage and rank passed.')


if __name__ == '__main__':
    main()
