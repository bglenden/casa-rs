#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Solve known-source leakage with CASA and casa-rs and apply both products."""

import argparse
import inspect
from pathlib import Path

import casatasks as casa
from casatools import calibrater
import numpy as np

from applycal_oracle import copy_ms
from calibration_leakage_apply_oracle import CHANNELS, GAIN, LEAKAGE, make_table, result
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def plant(root, basis, *, circular_stokes_v=0.):
    vis = root/'polarization.ms'
    make_cal_ms(str(vis), stokes=basis, scan_seconds=7200, integration_seconds=600)
    model_tool = calibrater()
    assert model_tool.open(str(vis), addcorr=False, addmodel=True)
    assert model_tool.close()
    linear = basis.startswith('XX')
    i, q, u, v = stokes = [1.6, .18, -.11, .04 if linear else circular_stokes_v]
    vector = ([i+q, u+1j*v, u-1j*v, i-q] if linear else [i+v, q+1j*u, q-1j*u, i-v])
    with opened(vis, True) as table:
        sky = np.broadcast_to(np.array(vector, np.complex64)[:, None, None],
                              (4, CHANNELS, table.nrows())).copy()
        epoch = float(table.getcol('TIME').min())
        table.putcol('MODEL_DATA', sky)
        table.putcol('FLAG', np.zeros(sky.shape, bool))
        table.putcol('FLAG_ROW', np.zeros(table.nrows(), bool))
    leak = root/'planted-leakage.cal'
    gains = root/'prior-gains.cal'
    make_table(vis, leak, 'D Jones', LEAKAGE, epoch)
    make_table(vis, gains, 'G Jones', GAIN, epoch)
    return vis, leak, gains, stokes, sky


def put_observations(vis, sky, priors):
    with opened(vis, True) as table:
        table.putcol('MODEL_DATA', sky)
    tool = calibrater()
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    for prior in priors:
        assert tool.setapply(table=str(prior), calwt=False)
    assert tool.setapply(type='P')
    assert tool.corrupt()
    assert tool.close()
    with opened(vis, True) as table:
        apparent = table.getcol('MODEL_DATA')
        table.putcol('DATA', apparent)
        table.putcol('MODEL_DATA', sky)
    return apparent


def compare_table(reference, candidate):
    with opened(reference) as want, opened(candidate) as got:
        assert got.info()['subType'] == want.info()['subType']
        assert got.nrows() == want.nrows() > 0
        for column in ('FIELD_ID', 'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'OBSERVATION_ID',
                       'ANTENNA1', 'ANTENNA2', 'FLAG', 'SNR', 'PARAMERR'):
            np.testing.assert_array_equal(got.getcol(column), want.getcol(column), err_msg=column)
        for column in ('TIME', 'INTERVAL'):
            np.testing.assert_allclose(got.getcol(column), want.getcol(column), atol=2e-5, rtol=0.)
        expected, actual = want.getcol('CPARAM'), got.getcol('CPARAM')
        error = np.linalg.norm(actual-expected) / np.linalg.norm(expected)
        assert error < 1e-3, ('CPARAM', error)
        assert np.isfinite(actual).all()
    with opened(reference/'SPECTRAL_WINDOW') as want, opened(candidate/'SPECTRAL_WINDOW') as got:
        for column in ('NUM_CHAN', 'CHAN_FREQ', 'REF_FREQUENCY', 'FLAG_ROW'):
            np.testing.assert_allclose(got.getcell(column, 0), want.getcell(column, 0), atol=1e-4, rtol=0.)
    return float(error)


def compare_apply(root, vis, products, native, priors):
    products_seen = []
    for producer in ('casa', 'native'):
        product = products[producer]
        for reader, apply in [('casa', casa.applycal), ('native', native.applycal)]:
            target = root/f'{producer}-table-{reader}-reader.ms'
            copy_ms(vis, target)
            apply(str(target), gaintable=[str(path) for path in [*priors, product]],
                  parang=True, calwt=False, flagbackup=False, field='0')
            data, flags, _, _ = result(target)
            products_seen.append((data, flags))
    base, flags = products_seen[0]
    for data, other_flags in products_seen[1:]:
        np.testing.assert_array_equal(other_flags, flags)
        assert np.linalg.norm(data-base) / np.linalg.norm(base) < 1e-3


def solve_case(root, vis, native, stokes, priors=(), **overrides):
    root.mkdir()
    options = dict(field='0', poltype='Dlls', refant='A0', smodel=stokes,
                   gaintable=[str(path) for path in priors])
    options.update(overrides)
    products = {name: root/f'{name}.cal' for name in ('casa', 'native')}
    for name, api in [('casa', casa), ('native', native)]:
        assert api.polcal(str(vis), str(products[name]), **options) is None
    error = compare_table(products['casa'], products['native'])
    print(root.name, f'CPARAM normalized RMS {error:.6g}')
    return products


def compare_basis(root, basis, native):
    root.mkdir()
    vis, leak, gains, stokes, sky = plant(root, basis)
    put_observations(vis, sky, [leak])
    products = solve_case(root/'known-source', vis, native, stokes, minsnr=1e20, minblperant=20)
    compare_apply(root/'known-source', vis, products, native, [])
    solve_case(root/'model-column', vis, native, stokes, smodel=[])
    append_path = root/'append'
    append_path.mkdir()
    for name, api in [('casa', casa), ('native', native)]:
        product = append_path/f'{name}.cal'
        copy_ms(products[name], product)
        assert api.polcal(str(vis), str(product), field='1', poltype='Dlls', refant='A0',
                          smodel=stokes, append=True) is None
    assert compare_table(append_path/'casa.cal', append_path/'native.cal') < 1e-3
    solve_case(root/'channels', vis, native, stokes, poltype='Dflls', spw='0:2~5',
               selectdata=False, timerange='disregarded')
    for unit in ('2chan', '2MHz'):
        solve_case(root/f'frequency-interval-{unit}', vis, native, stokes, poltype='Dflls',
                   spw='0:2~6', solint=f'inf,{unit}')
    for label, api in [('casa', casa), ('native', native)]:
        bad = root/f'{label}-bad-interval.cal'
        try:
            api.polcal(str(vis), str(bad), poltype='Dflls', solint='inf,banana')
        except RuntimeError as error:
            assert str(error) == 'Error in Calibrater::setsolve.'
        else:
            raise AssertionError(f'{label} accepted an invalid frequency interval')
        assert not bad.exists()
    put_observations(vis, sky, [gains, leak])
    solve_case(root/'traditional-prior', vis, native, stokes, [gains], gainfield=['0'],
               interp=['nearest'], spwmap=[0])
    library = root/'prior.callib'
    library.write_text(f"caltable='{gains}' tinterp='linear' calwt=True spwmap=[0]\n")
    products = solve_case(root/'callib', vis, native, stokes, [gains], docallib=True, callib=str(library))
    compare_apply(root/'callib', vis, products, native, [gains])


def rank_deficient(root, native):
    root.mkdir()
    vis, leak, _, stokes, sky = plant(root, 'XX XY YX YY')
    model = put_observations(vis, sky, [])
    data = put_observations(vis, sky, [leak])
    with opened(vis) as table:
        field = table.getcol('FIELD_ID') == 0
        times = table.getcol('TIME')[field]
        left = table.getcol('ANTENNA1')[field]
        right = table.getcol('ANTENNA2')[field]
    model = model[:, :, field]
    data = data[:, :, field]
    residuals = {}
    for label, api in [('casa', casa), ('native', native)]:
        product = root/f'{label}.cal'
        assert api.polcal(str(vis), str(product), field='0', poltype='Dlls', smodel=stokes,
                          combine='', solint='int', preavg=0., refant='A0') is None
        with opened(product) as table:
            gain = table.getcol('CPARAM')[:, 0, :]
            sol_time = table.getcol('TIME')
            antennas = table.getcol('ANTENNA1')
            assert not table.getcol('FLAG').any()
        if label == 'native':
            assert np.isfinite(gain).all() and np.max(np.abs(gain)) < .25
            np.testing.assert_array_equal(gain[0, antennas == 0], 0.)
        terms = []
        for station in (left, right):
            rows = [np.flatnonzero((antennas == antenna) & (np.abs(sol_time-time) < 2e-5))[0]
                    for antenna, time in zip(station, times)]
            terms.append(gain[:, rows])
        (dx_i, dy_i), (dx_j, dy_j) = terms
        xy = model[1] + dx_i[None, :]*model[3] + dy_j.conj()[None, :]*model[0]
        yx = model[2] + dy_i[None, :]*model[0] + dx_j.conj()[None, :]*model[3]
        residuals[label] = float(np.linalg.norm(np.stack((xy, yx))-data[[1, 2]])
                                 / np.linalg.norm(data[[1, 2]]))
    assert residuals['native'] < 1e-5, residuals
    assert residuals['casa'] > 1e-3, residuals
    print('Rank-deficient integration cross-hand residuals', residuals)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    expected = inspect.signature(casa.polcal).parameters
    actual = inspect.signature(tasks.polcal).parameters
    assert list(expected) == list(actual)
    assert all(actual[name].default == entry.default for name, entry in expected.items())
    for api in (casa, tasks):
        try:
            api.polcal()
        except AssertionError as error:
            assert 'the vis parameter must be a path that exists' in str(error)
        else:
            raise AssertionError('polcal requires an existing MeasurementSet')
    root = args.work.resolve()
    root.mkdir()
    compare_basis(root/'linear', 'XX XY YX YY', tasks)
    compare_basis(root/'circular', 'RR RL LR LL', tasks)
    rank_deficient(root/'rank-deficient', tasks)


if __name__ == '__main__':
    main()
