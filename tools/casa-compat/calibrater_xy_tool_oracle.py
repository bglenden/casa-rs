#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare retained XY+QU and XYf+QU calibrater solves with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibrater_apply_tool_oracle import result
from calibrater_cross_hand_tool_oracle import case
from calibration_smoothcal_oracle import opened
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import plant, put_observations


def reject_solve(work, engines, vis, label, *, old=False, selection=None, **controls):
    for engine, factory in engines.items():
        tool = factory()
        output = work/f'{label}-{engine}.cal'
        assert tool.setvi(old, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        try:
            assert tool.selectvis(**({'field': '0'} | (selection or {})))
            assert tool.setptmodel([1.6, 0., 0., 0.])
            assert tool.setsolve('XYf+QU', table=str(output), refant='A3', **controls)
            assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
            assert not output.exists()
        finally:
            assert tool.close()
    print(work.name, label, 'matched rejection', flush=True)


def compare_xy(work, engines, vis, label, kind, stokes, **controls):
    options = {'preavg': 300.} | controls.pop('options', {})
    products = case(work, engines, vis, label, kind, stokes, parang=False,
                    options=options, **controls)
    with opened(products['casa']) as expected, opened(products['native']) as actual:
        np.testing.assert_allclose(actual.getkeyword('QU')['QU'], expected.getkeyword('QU')['QU'],
                                   atol=1e-4, rtol=0.)
        np.testing.assert_array_equal(actual.getcol('ANTENNA2'), -1)
    return products['native']


def compare_basis(work, basis, engines):
    work.mkdir()
    vis, _, gain, stokes, sky = plant(work, basis)
    with opened(vis) as table:
        epoch = table.getcol('TIME').min()
    phase = work/'planted-phase.cal'
    expected = plant_cross_hand(vis, phase, 5, epoch)
    data = put_observations(vis, sky, [phase])
    unpolarized = [1.6, 0., 0., 0.]

    for kind in ('XY+QU', 'XYf+QU'):
        for old in (False, True):
            for model, values in (('unit-I', unpolarized), ('known-polarization', stokes)):
                product = compare_xy(work, engines, vis, f'{kind}-{old}-{model}', kind, values,
                                     old=old)
                with opened(product) as table:
                    if old and model == 'unit-I':
                        assert table.getcol('FLAG').all()
                        np.testing.assert_array_equal(table.getcol('CPARAM'), 1.)
                    if not old and model == 'unit-I' and basis.startswith('XX'):
                        phase_values = expected[0] if kind == 'XYf+QU' else np.exp(.55j)
                        np.testing.assert_allclose(table.getcol('CPARAM')[0], phase_values,
                                                   atol=3e-5, rtol=0.)

    compare_xy(work, engines, vis, 'XYf-ignored-cuts', 'XYf+QU', unpolarized,
               options={'minblperant': 50, 'minsnr': 1e20})
    compare_xy(work, engines, vis, 'XYf-append', 'XYf+QU', unpolarized, append=True)
    for mode in ('A', 'P'):
        compare_xy(work, engines, vis, f'XYf-mode-{mode}', 'XYf+QU', unpolarized,
                   options={'apmode': mode, 'solnorm': True})
    reject_solve(work, engines, vis, 'XYf-one-modern-segment', preavg=0.)
    with opened(vis, True) as table:
        flags = table.getcol('FLAG')
        flags[1, 3, ::6] = True
        assert table.putcol('FLAG', flags)
    compare_xy(work, engines, vis, 'XYf-dependent-flags', 'XYf+QU', unpolarized, corrdep=True)
    with opened(vis, True) as table:
        assert table.putcol('FLAG', np.zeros_like(flags))
    put_observations(vis, sky, [gain, phase])
    compare_xy(work, engines, vis, 'XYf-selected-prior', 'XYf+QU', unpolarized, prior=(gain,),
               selection={'spw': '0:2~6'}, options={'t': 'inf,2chan', 'preavg': 1300.})
    with opened(vis, True) as table:
        assert table.putcol('DATA', data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    from casars.tools import calibrater as native_calibrater

    root = args.work.resolve()
    root.mkdir()
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    with chdir(root):
        compare_basis(root/'linear', 'XX XY YX YY', engines)
        compare_basis(root/'circular', 'RR RL LR LL', engines)


if __name__ == '__main__':
    main()
