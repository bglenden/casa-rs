#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct X Mueller solutions and correction with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from applycal_oracle import copy_ms
from calibrater_apply_tool_oracle import result
from calibrater_solve_tool_oracle import compare_readers, compare_solutions
from calibration_smoothcal_oracle import opened
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import plant, put_observations


def solve(work, engines, vis, label, stokes, *, old=True, options=None):
    products = {engine: work/f'{label}-{engine}.cal' for engine in engines}
    for engine, factory in engines.items():
        product = products[engine]
        tool = factory()
        assert tool.setvi(old, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        try:
            assert tool.selectvis(field='0') and tool.setptmodel(stokes)
            setup = {'t': 'inf', 'refant': 'A3'} | (options or {})
            assert tool.setsolve('X', table=str(product), **setup)
            assert list(tool.returndict()['solve_tables']) == [str(product)]
            if old:
                assert tool.solve() and product.exists()
                assert tool.activityrec() == {} and 'solvestats' not in tool.returndict()
                assert tool.reset(False, True) and len(tool.returndict()['solve_tables']) == 0
            else:
                assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
                assert not product.exists()
        finally:
            assert tool.close()
    if old:
        compare_solutions(products)
        compare_readers(products)
        with opened(products['casa']) as expected, opened(products['native']) as actual:
            np.testing.assert_array_equal(actual.getcol('ANTENNA2'), expected.getcol('ANTENNA2'))
            flags, parameters = (actual.getcol(name) for name in ('FLAG', 'CPARAM'))
            np.testing.assert_array_equal(parameters[flags], 1.)
    print(work.name, label, 'matched', flush=True)
    return products


def correct(work, engines, vis, products, label):
    for source, product in products.items():
        for old in (False, True):
            outputs = {}
            for engine, factory in engines.items():
                output = work/f'{label}-{source}-{old}-{engine}.ms'
                copy_ms(vis, output)
                tool = factory()
                assert tool.setvi(old, True) and tool.open(str(output), addcorr=True, addmodel=False)
                try:
                    assert tool.selectvis(field='0')
                    assert tool.setapply(table=str(product), calwt=True) and tool.correct()
                finally:
                    assert tool.close()
                with opened(output) as table:
                    selected = table.getcol('FIELD_ID') == 0
                    data, corrected = (table.getcol(name)[..., selected]
                                       for name in ('DATA', 'CORRECTED_DATA'))
                    np.testing.assert_array_equal(corrected[[0, 3]], data[[0, 3]])
                    outputs[engine] = [corrected] + [table.getcol(name)
                                                     for name in ('FLAG', 'WEIGHT', 'SIGMA')]
            np.testing.assert_allclose(outputs['native'][0], outputs['casa'][0], atol=2e-5, rtol=0.)
            for actual, expected in zip(outputs['native'][1:], outputs['casa'][1:]):
                np.testing.assert_array_equal(actual, expected)
            print(work.name, label, source, old, 'matched correction', flush=True)


def compare_basis(work, basis, engines, solutions_only):
    work.mkdir()
    vis, _, _, stokes, sky = plant(work, basis)
    with opened(vis) as table:
        epoch = table.getcol('TIME').min()
    phase = work/'planted-phase.cal'
    plant_cross_hand(vis, phase, 5, epoch)
    put_observations(vis, sky, [phase])

    solve(work, engines, vis, 'modern-rejected', stokes, old=False)
    positive = solve(work, engines, vis, 'legacy-default', stokes)
    solve(work, engines, vis, 'legacy-long-preaverage', stokes, options={'preavg': 3601.})
    solve(work, engines, vis, 'legacy-amplitude-mode', stokes,
          options={'apmode': 'A', 'solnorm': True})
    solve(work, engines, vis, 'legacy-integration', stokes, options={'t': 'int'})
    flagged = solve(work, engines, vis, 'legacy-unpolarized', [1.6, 0., 0., 0.])
    with opened(flagged['native']) as table:
        assert table.getcol('FLAG').all()
    if not solutions_only:
        correct(work, engines, vis, positive, 'correct-positive')
        correct(work, engines, vis, flagged, 'correct-flagged')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--solutions-only', action='store_true')
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    from casars.tools import calibrater as native_calibrater

    root = args.work.resolve()
    root.mkdir()
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    with chdir(root):
        compare_basis(root/'linear', 'XX XY YX YY', engines, args.solutions_only)
        compare_basis(root/'circular', 'RR RL LR LL', engines, args.solutions_only)


if __name__ == '__main__':
    main()
