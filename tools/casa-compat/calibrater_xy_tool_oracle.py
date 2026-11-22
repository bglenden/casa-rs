#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare retained XY+QU and XYf+QU calibrater solves with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
from casatools import table as casa_table
import numpy as np

from applycal_oracle import copy_ms
from calibrater_apply_tool_oracle import result, same_record
from calibrater_cross_hand_tool_oracle import case, solve
from calibrater_solve_tool_oracle import compare_readers
from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
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


def amplitude_only(work, engines, vis, model, data, times, *, circular):
    if circular:
        # Constant amplitude has no resolvable phase after amplitude enforcement.
        # Keep the CASA result as evidence of its rounding-dependent phase.
        values = {}
        products = {engine: work/f'XYf-constant-A-{engine}.cal' for engine in engines}
        for engine, factory in engines.items():
            values[engine] = solve(factory, vis, products[engine], 'XYf+QU', model,
                                   parang=False, options={'preavg': 300., 'apmode': 'A'})
            with opened(products[engine]) as table:
                assert not table.getcol('FLAG').any()
                np.testing.assert_allclose(table.getkeyword('QU')['QU'], 0., atol=1e-7, rtol=0.)
                if engine == 'native':
                    np.testing.assert_array_equal(table.getcol('CPARAM'), 1.)
        for actual, expected in zip(values['native'], values['casa'], strict=True):
            same_record(actual, expected)
        compare_readers(products)
        print(work.name, 'XYf-constant-A degenerate phase documented', flush=True)
    varied = data.copy()
    factor = .4 + 1.7 * (times - times.min()) / np.ptp(times)
    varied[1] *= factor
    varied[2] *= factor**2
    with opened(vis, True) as table:
        assert table.putcol('DATA', varied)
    try:
        compare_xy(work, engines, vis, 'XYf-mode-A', 'XYf+QU', model,
                   options={'apmode': 'A', 'solnorm': True})
    finally:
        with opened(vis, True) as table:
            assert table.putcol('DATA', data)


def legacy_insufficient_angle(work, engines, vis, stokes):
    products = {engine: work/f'XYf-legacy-single-time-{engine}.cal' for engine in engines}
    values = {}
    for engine, factory in engines.items():
        values[engine] = solve(factory, vis, products[engine], 'XYf+QU', stokes,
                               old=True, parang=False)
        with opened(products[engine]) as table:
            assert table.nrows() == 5
            flags = table.getcol('FLAG')
            if engine == 'native':
                assert flags.all()
            else:
                assert not flags.any()
            np.testing.assert_array_equal(table.getkeyword('QU')['QU'], 0.)
    for actual, expected in zip(values['native'], values['casa'], strict=True):
        same_record(actual, expected)
    compare_readers(products)
    print(work.name, 'XYf-legacy-single-time unconstrained phase documented', flush=True)


def compare_autos(work, engines, vis, stokes):
    path = work/'with-autos.ms'
    copy_ms(vis, path)
    rows = duplicate_rows(casa_table(), path)
    with opened(path, True) as table:
        antenna = table.getcol('ANTENNA1', rows, rows)
        assert table.putcol('ANTENNA2', antenna, rows, rows)
        data = table.getcol('DATA', rows, rows)
        data[1] *= 20j
        data[2] *= -20j
        assert table.putcol('DATA', data, rows, rows)
    for old in (False, True):
        compare_xy(work, engines, path, f'XYf-autos-{old}', 'XYf+QU', stokes, old=old)


def compare_basis(work, basis, engines):
    work.mkdir()
    vis, _, gain, stokes, sky = plant(work, basis)
    with opened(vis) as table:
        times = table.getcol('TIME')
    phase = work/'planted-phase.cal'
    expected = plant_cross_hand(vis, phase, 5, times.min())
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
    amplitude_only(work, engines, vis, unpolarized, data, times, circular=basis.startswith('RR'))
    compare_xy(work, engines, vis, 'XYf-mode-P', 'XYf+QU', unpolarized,
               options={'apmode': 'P', 'solnorm': True})
    reject_solve(work, engines, vis, 'XYf-one-modern-segment', preavg=0.)
    compare_xy(work, engines, vis, 'XYf-legacy-default-I', 'XYf+QU', unpolarized, old=True,
               options={'preavg': -1.})
    compare_xy(work, engines, vis, 'XYf-legacy-integration-I', 'XYf+QU', unpolarized, old=True,
               options={'t': 'int'})
    legacy_insufficient_angle(work, engines, vis, stokes)
    compare_autos(work, engines, vis, stokes)
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
