#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare full D Jones solves and table application with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
import numpy as np

from calibration_leakage_apply_oracle import LEAKAGE, make_table
from calibration_smoothcal_oracle import opened
from polcal_joint_leakage_oracle import phase_without_last_cross_hand
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations
from polcal_standard_leakage_oracle import solve


def compare_basis(root, basis, api):
    root.mkdir()
    vis, _, gains, stokes, sky = plant(root, basis)
    with opened(vis) as ms:
        epoch = float(ms.getcol('TIME').min())
    leak = root/'general-leakage.cal'
    make_table(vis, leak, 'Dfgen Jones', LEAKAGE, epoch)
    data = put_observations(vis, sky, [leak])
    rng = np.random.default_rng(645)
    noise = (.003 * (rng.normal(size=data.shape) + 1j*rng.normal(size=data.shape))).astype(np.complex64)
    with opened(vis, True) as ms:
        ms.putcol('DATA', data + noise)
    for poltype in ('Dgen', 'Dfgen', 'Dgen+QU', 'Dfgen+QU', 'Dgen+X', 'Dfgen+X'):
        products = solve(root/poltype, vis, api, stokes, poltype=poltype)
        compare_apply(root/poltype, vis, products, api, [])
    data = put_observations(vis, sky, [gains, leak])
    with opened(vis, True) as ms:
        ms.putcol('DATA', data + noise)
    products = solve(root/'Dfgen+QU-prior', vis, api, [], poltype='Dfgen+QU',
                     gaintable=[str(gains)], spw='0:2~6', solint='inf,2MHz')
    compare_apply(root/'Dfgen+QU-prior', vis, products, api, [gains])
    if basis.startswith('RR'):
        actual_sky = sky.copy()
        phase = np.exp(np.complex64(.85j))
        actual_sky[1] *= phase
        actual_sky[2] *= phase.conjugate()
        data = put_observations(vis, actual_sky, [leak])
        phase_without_last_cross_hand(root, vis, data + noise, sky, poltype='Dfgen+X')


def invalid_receptor(root, api):
    root.mkdir()
    vis, _, _, _, sky = plant(root, 'RR RL LR LL')
    sky.fill(0.)
    sky[::3] = 1.
    with opened(vis) as ms:
        first = ms.getcol('ANTENNA1')
        epoch = float(ms.getcol('TIME').min())
    leak = root/'planted-full.cal'
    make_table(vis, leak, 'Dfgen Jones', LEAKAGE, epoch)
    data = put_observations(vis, sky, [leak])
    rng = np.random.default_rng(5406)
    noise = (.001 * (rng.normal(size=data.shape) + 1j*rng.normal(size=data.shape))).astype(np.complex64)
    with opened(vis, True) as ms:
        sigma = ms.getcol('SIGMA')
        weight = ms.getcol('WEIGHT')
        sigma[2, first == 0] = 0.
        weight[2, first == 0] = 0.
        ms.putcol('SIGMA', sigma)
        ms.putcol('WEIGHT', weight)
        ms.putcol('DATA', data + noise)
    stokes = [1., 0., 0., 0.]
    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        product = root/f'{label}-scalar.cal'
        assert task(str(vis), str(product), field='0', poltype='Dgen', smodel=stokes,
                    minsnr=0., refant='') is None
        assert not product.exists()
    products = solve(root/'Dfgen-mask', vis, api, stokes, poltype='Dfgen', minsnr=0., refant='')
    with opened(products['native']) as table:
        np.testing.assert_array_equal(table.getcol('FLAG')[:, -1, 0], [False, True])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    root = args.work.resolve()
    root.mkdir()
    compare_basis(root/'linear', 'XX XY YX YY', tasks)
    compare_basis(root/'circular', 'RR RL LR LL', tasks)
    invalid_receptor(root/'receptor-mask', tasks)


if __name__ == '__main__':
    main()
