#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the PosAng Jones solve and application with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
from casatools import calibrater
import numpy as np

from calibration_leakage_apply_oracle import CHANNELS, compare_application
from calibration_smoothcal_oracle import opened
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations


def plant_angle(vis, output, angles, epoch):
    tool = calibrater()
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    assert tool.createcaltable(str(output), 'Float', 'PosAng Jones', True)
    assert tool.close()
    rows = angles.shape[-1]
    epochs = np.atleast_1d(epoch)
    antennas = rows // len(epochs)
    with opened(output, True) as table:
        table.addrows(rows)
        for column in ('FIELD_ID', 'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'OBSERVATION_ID'):
            table.putcol(column, np.zeros(rows, np.int32))
        table.putcol('ANTENNA1', np.tile(np.arange(antennas, dtype=np.int32), len(epochs)))
        table.putcol('ANTENNA2', np.full(rows, -1, np.int32))
        table.putcol('TIME', np.repeat(epochs, antennas))
        table.putcol('INTERVAL', np.zeros(rows))
        table.putcol('FPARAM', angles)
        table.putcol('FLAG', np.zeros(angles.shape, bool))
        table.putcol('PARAMERR', np.zeros(angles.shape, np.float32))
        table.putcol('SNR', np.zeros(angles.shape, np.float32))
    with opened(vis/'SPECTRAL_WINDOW') as ms, opened(output/'SPECTRAL_WINDOW', True) as table:
        for column in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
            table.putcell(column, 0, ms.getcell(column, 0))


def solve(root, vis, api, **options):
    root.mkdir()
    products = {label: root/f'{label}.cal' for label in ('casa', 'native')}
    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        assert task(str(vis), str(products[label]), field='0', poltype='PosAng', **options) is None
    with opened(products['casa']) as want, opened(products['native']) as got:
        assert got.info()['subType'] == want.info()['subType'] == 'PosAng Jones'
        assert got.getkeyword('ParType') == want.getkeyword('ParType') == 'Float'
        for column in ('FLAG', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID', 'OBSERVATION_ID',
                       'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'INTERVAL', 'PARAMERR', 'SNR'):
            np.testing.assert_array_equal(got.getcol(column), want.getcol(column), err_msg=column)
        np.testing.assert_allclose(got.getcol('TIME'), want.getcol('TIME'), atol=1e-4, rtol=0.)
        angles, reference = got.getcol('FPARAM'), want.getcol('FPARAM')
        np.testing.assert_allclose(angles, reference, atol=3e-5, rtol=0.)
        print(root.parent.name, root.name, 'maximum angle difference', np.max(np.abs(angles-reference)))
    with (opened(products['casa']/'SPECTRAL_WINDOW') as want,
          opened(products['native']/'SPECTRAL_WINDOW') as got):
        for column in ('NUM_CHAN', 'CHAN_FREQ', 'REF_FREQUENCY', 'FLAG_ROW'):
            np.testing.assert_allclose(got.getcell(column, 0), want.getcell(column, 0),
                                       atol=1e-4, rtol=0., err_msg=column)
    return products, angles


def compare_basis(root, basis, api):
    root.mkdir()
    vis, leak, gains, stokes, sky = plant(root, basis)
    with opened(vis) as ms:
        time, fields = ms.getcol('TIME'), ms.getcol('FIELD_ID')
    with opened(vis/'ANTENNA') as table:
        antennas = table.nrows()
    angles = np.broadcast_to(np.linspace(-.31, .47, CHANNELS, dtype=np.float32)[None, :, None],
                             (1, CHANNELS, antennas)).copy()
    position = root/'planted-angle.cal'
    plant_angle(vis, position, angles, float(time.min()))
    put_observations(vis, sky, [position])
    products, actual = solve(root/'position-angle', vis, api, smodel=stokes, refant='A3',
                             minsnr=1e20, minblperant=20)
    np.testing.assert_allclose(actual, angles, atol=3e-5, rtol=0.)
    compare_apply(root/'position-angle', vis, products, api, [], cross_hand_tolerance=1e-5)

    flux = sky.copy()
    flux[:, 0, time > np.median(time[fields == 0])] *= 3
    phase = root/'prior-phase.cal'
    plant_cross_hand(vis, phase, antennas, float(time.min()))
    chain = [gains, leak, phase]
    put_observations(vis, flux, [*chain, position])
    products, _ = solve(root/'selected-priors', vis, api, gaintable=[str(t) for t in chain],
                        spw='0:2~6', solint='inf,2chan', preavg=0.)
    compare_apply(root/'selected-priors', vis, products, api, chain, cross_hand_tolerance=1e-5)
    for label, table in products.items():
        difference, _ = compare_application(root/'selected-priors', vis, api, f'calwt-{label}',
                                            [table], True, field='0', interp=['linear,linear'])
        assert difference < 1e-5

    interpolation = root/'temporal-angles.cal'
    antenna_angles = angles + .23 * np.arange(antennas, dtype=np.float32)
    epochs = np.unique(time[fields == 0])[[0, -1]]
    plant_angle(vis, interpolation, np.concatenate((antenna_angles, -.6 * antenna_angles), axis=2),
                epochs)
    difference, _ = compare_application(root, vis, api, 'signed-time-interpolation',
                                        [interpolation], False, field='0', interp=['linear,linear'])
    assert difference < 1e-5

    with opened(interpolation, True) as table:
        flags = table.getcol('FLAG')
        flags[:, 3, 1] = True
        table.putcol('FLAG', flags)
    difference, _ = compare_application(root, vis, api, 'invalid-receptor',
                                        [interpolation], True, field='0', interp=['nearest,nearest'])
    assert difference < 1e-5

    if basis.startswith('RR'):
        with opened(vis, True) as ms:
            ms.putcol('DATA', np.zeros_like(sky))
        products, actual = solve(root/'zero-cross-hands', vis, api, smodel=stokes)
        assert not np.any(actual)
        with opened(products['native']) as table:
            assert table.getcol('FLAG').all()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    root = args.work.resolve()
    root.mkdir()
    compare_basis(root/'circular', 'RR RL LR LL', tasks)
    compare_basis(root/'linear', 'XX XY YX YY', tasks)


if __name__ == '__main__':
    main()
