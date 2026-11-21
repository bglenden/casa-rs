#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare full D Jones solves and table application with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
import numpy as np

from calibration_leakage_apply_oracle import LEAKAGE, make_table
from calibration_smoothcal_oracle import opened
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
