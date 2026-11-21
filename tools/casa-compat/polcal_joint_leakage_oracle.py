#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare joint source polarization and leakage solves with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
import numpy as np

from calibration_smoothcal_oracle import opened
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations
from polcal_standard_leakage_oracle import solve


def compare_basis(root, basis, native):
    root.mkdir()
    vis, leak, gains, stokes, sky = plant(root, basis)
    rng = np.random.default_rng(34501)
    actual_sky = sky.copy()
    if basis.startswith('RR'):
        ratio = np.complex64(1.32 * np.exp(.38j))
        actual_sky[1] *= ratio
        actual_sky[2] *= ratio.conjugate()
    data = put_observations(vis, actual_sky, [leak])
    noise = (.003 * (rng.normal(size=data.shape) + 1j*rng.normal(size=data.shape))).astype(np.complex64)
    with opened(vis, True) as ms:
        ms.putcol('DATA', data + noise)
        ms.putcol('MODEL_DATA', sky)
    # Weakly constrained iterative directions can stop at slightly different
    # coefficients. Guard the corrected cross-hands separately.
    default = solve(root/'D+QU-default', vis, native, stokes,
                    gain_tolerance=2.5e-3 if basis.startswith('XX') else 1e-3)
    compare_apply(root/'D+QU-default', vis, default, native, [], cross_hand_tolerance=4e-3)
    resolved = np.linspace(.8, 1.2, sky.shape[2], dtype=np.float32) * np.exp(
        1j * np.linspace(-.3, .1, sky.shape[2], dtype=np.float32)
    )
    scaled_sky = sky * resolved
    data = put_observations(vis, actual_sky * resolved, [gains, leak])
    with opened(vis, True) as ms:
        ms.putcol('DATA', data + noise)
        ms.putcol('MODEL_DATA', scaled_sky)
    products = solve(root/'Df+QU-resolved-prior', vis, native, [], poltype='Df+QU',
                     gaintable=[str(gains)], spw='0:2~6', solint='inf,2chan')
    compare_apply(root/'Df+QU-resolved-prior', vis, products, native, [gains])

    data = put_observations(vis, actual_sky, [leak])
    with opened(vis, True) as ms:
        ms.putcol('DATA', data + noise)
        ms.putcol('MODEL_DATA', sky)
    products = solve(root/'D+X', vis, native, stokes, poltype='D+X')
    compare_apply(root/'D+X', vis, products, native, [])
    products = solve(root/'Df+X', vis, native, [], poltype='Df+X',
                     gain_tolerance=8e-3 if basis.startswith('RR') else 1e-3)
    compare_apply(root/'Df+X', vis, products, native, [], cross_hand_tolerance=4e-3)
    for label, task in [('casa', casa.polcal), ('native', native.polcal)]:
        with opened(vis, True) as ms:
            ms.putcol('MODEL_DATA', np.zeros_like(sky))
        empty = root/f'{label}-no-intensity.cal'
        assert task(str(vis), str(empty), field='0', poltype='D+QU') is None
        assert not empty.exists()
        empty = root/f'{label}-no-angle-model.cal'
        assert task(str(vis), str(empty), field='0', poltype='D+X') is None
        assert not empty.exists()

    if basis.startswith('RR'):
        # At one integration with identical antenna positions, the model
        # supplies just one cross-hand state for both source-fit terms.
        with opened(vis/'ANTENNA', True) as antennas:
            positions = antennas.getcol('POSITION')
            positions[:] = positions[:, 0, None]
            antennas.putcol('POSITION', positions)
        with opened(vis) as ms:
            time = ms.getcol('TIME').min()
        for poltype in ('D+QU', 'D+X'):
            for label, task in [('casa', casa.polcal), ('native', native.polcal)]:
                product = root/f'{label}-singular-{poltype}.cal'
                try:
                    task(str(vis), str(product), field='0', poltype=poltype, smodel=stokes,
                         msselect=f'abs(TIME-{time:.9f}) < 0.001')
                except RuntimeError:
                    assert not product.exists()
                else:
                    raise AssertionError(f'{label} accepted a rank-one source fit for {poltype}')


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


if __name__ == '__main__':
    main()
