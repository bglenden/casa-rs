#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the public XY+QU and XYf+QU gaincal modes with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
import numpy as np

from calibration_leakage_apply_oracle import compare_application
from calibration_smoothcal_oracle import opened
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations
from polcal_standard_leakage_oracle import compare


def compare_record(want, got):
    assert got.keys() == want.keys()
    for key, value in want.items():
        if isinstance(value, dict):
            compare_record(value, got[key])
        elif key == 'solve_tables':
            assert got[key].shape == value.shape == (1,)
            assert all(Path(paths[0]).exists() for paths in (value, got[key]))
        else:
            assert got[key].dtype.kind == value.dtype.kind
            np.testing.assert_array_equal(got[key], value, err_msg=key)


def solve(root, vis, api, mode, **overrides):
    root.mkdir()
    inputs = dict(field='0', smodel=[1.6, 0., 0., 0.], refant='A3', preavg=300.)
    inputs.update(overrides)
    tables = {label: root/f'{label}.cal' for label in ('casa', 'native')}
    results = {}
    for name, task in (('casa', casa.gaincal), ('native', api.gaincal)):
        results[name] = task(str(vis), str(tables[name]), gaintype=mode, **inputs)
    compare_record(results['casa'], results['native'])
    errors = compare(tables['casa'], tables['native'], gain_tolerance=3e-4, time_tolerance=1e-4)
    with opened(tables['casa']) as want, opened(tables['native']) as got:
        qu = got.getkeyword('QU')['QU']
        np.testing.assert_allclose(qu, want.getkeyword('QU')['QU'], atol=1e-4, rtol=0.)
        np.testing.assert_array_equal(got.getcol('ANTENNA2'), -1)
        gain = got.getcol('CPARAM')
        np.testing.assert_array_equal(gain[1], 1.)
    print(root.parent.name, root.name, 'gain/error/SNR normalized RMS', errors)
    return tables, gain


def rejected_intervals(root, vis, api, linear):
    for label, task in (('casa', casa.gaincal), ('native', api.gaincal)):
        for name, values in (
            ('default', {}),
            ('full-preaverage', {'preavg': 0.}),
            ('integration', {'preavg': 300., 'solint': 'int'}),
        ):
            output = root/f'{label}-{name}.cal'
            try:
                task(str(vis), str(output), field='0', gaintype='XYf+QU', **values)
            except RuntimeError:
                assert not output.exists()
            else:
                raise AssertionError(f'{label} accepted {name}')
    if linear:
        for label, task in (('casa', casa.gaincal), ('native', api.gaincal)):
            output = root/f'{label}-rank-deficient.cal'
            try:
                task(str(vis), str(output), field='0', gaintype='XYf+QU', preavg=3601.,
                     smodel=[1.6, 0., 0., 0.])
            except RuntimeError:
                assert label == 'native' and not output.exists()
            else:
                assert label == 'casa' and output.exists()


def compare_basis(root, basis, api):
    root.mkdir()
    vis, leakage, gains, _, sky = plant(root, basis)
    with opened(vis) as table:
        time = table.getcol('TIME')
    phase = root/'planted-phase.cal'
    wanted = plant_cross_hand(vis, phase, 5, time.min())
    put_observations(vis, sky, [phase])

    products, _ = solve(root/'scalar', vis, api, 'XY+QU')
    compare_apply(root/'scalar', vis, products, api, [])
    products, values = solve(root/'spectrum', vis, api, 'XYf+QU',
                             minblperant=50, minsnr=1e20)
    if basis.startswith('XX'):
        np.testing.assert_allclose(values[0], wanted[0], atol=3e-5, rtol=0.)
    compare_apply(root/'spectrum', vis, products, api, [])

    put_observations(vis, sky, [gains, phase])
    products, _ = solve(root/'prior', vis, api, 'XYf+QU', gaintable=[str(gains)],
                        spw='0:2~6', solint='inf,2chan', preavg=1300.)
    compare_apply(root/'prior', vis, products, api, [gains])
    for label, table in products.items():
        diff, _ = compare_application(root/'prior', vis, api, f'calwt-{label}',
                                      [table], True, field='0')
        assert diff < 1e-5

    put_observations(vis, sky, [phase])
    solve(root/'parang', vis, api, 'XYf+QU', parang=True)
    if basis.startswith('XX'):
        solve(root/'amplitude-mode', vis, api, 'XYf+QU', calmode='a', solnorm=True)
        solve(root/'phase-mode', vis, api, 'XYf+QU', calmode='p')
        solve(root/'one-baseline', vis, api, 'XYf+QU', antenna='A0&A1')
    with opened(vis, True) as table:
        flags = table.getcol('FLAG')
        flags[1, 3, ::6] = True
        table.putcol('FLAG', flags)
    solve(root/'dependent-flags', vis, api, 'XYf+QU', corrdepflags=True)
    rejected_intervals(root, vis, api, basis.startswith('XX'))


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
