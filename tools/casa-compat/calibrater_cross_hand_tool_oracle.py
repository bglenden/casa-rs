#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct cross-hand and position-angle solves with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers, compare_solutions
from calibration_smoothcal_oracle import opened
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import plant, put_observations
from polcal_position_angle_oracle import plant_angle


def solve(factory, vis, product, kind, stokes, *, old=False, prior=(), options=None,
          selection=None, append=False, parang=True):
    tool = factory()
    assert tool.setvi(old, True) and tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        assert tool.selectvis(**({'field': '0'} | (selection or {})))
        if stokes is not None:
            assert tool.setptmodel(stokes)
        if parang:
            assert tool.setapply(type='P')
        for table in prior:
            assert tool.setapply(table=str(table), calwt=False)
        setup = {'t': 'inf', 'refant': 'A3'} | (options or {})
        assert tool.setsolve(kind, table=str(product), **setup)
        assert tool.solve()
        if append:
            assert tool.selectvis(field='1')
            assert tool.setsolve(kind, table=str(product), append=True, **setup)
            assert tool.solve()
        record = tool.returndict()
        assert list(record['solve_tables']) == [str(product)]
        assert ('solvestats' in record) == (not old)
        activity = tool.activityrec()
        assert tool.reset(False, True)
        assert list(tool.returndict()['solve_tables']) == []
        if prior:
            assert list(tool.returndict()['apply_tables']) == sorted(map(str, prior))
        return activity, record.get('solvestats')
    finally:
        assert tool.close()


def compare_activity(actual, expected):
    assert tuple(actual) == tuple(expected), (tuple(actual), tuple(expected))
    for key, wanted in expected.items():
        if key != 'fStokes':
            same_record(actual[key], wanted)
            continue
        got = actual[key]
        assert tuple(got) == tuple(wanted)
        for field, windows in wanted.items():
            assert tuple(got[field]) == tuple(windows)
            for window, values in windows.items():
                fitted = got[field][window]
                assert fitted.dtype == values.dtype
                np.testing.assert_allclose(fitted, values, atol=8e-5, rtol=0.)


def case(work, engines, vis, label, kind, stokes, *, no_product=False, **controls):
    tables = {engine: work/f'{label}-{engine}.cal' for engine in engines}
    results = {
        engine: solve(factory, vis, tables[engine], kind, stokes, **controls)
        for engine, factory in engines.items()
    }
    for table in tables.values():
        assert table.exists() == (not no_product), (label, table)
    compare_activity(results['native'][0], results['casa'][0])
    same_record(results['native'][1], results['casa'][1])
    if not no_product:
        compare_solutions(tables)
        compare_readers(tables)
        with opened(tables['casa']) as expected, opened(tables['native']) as actual:
            np.testing.assert_array_equal(actual.getcol('ANTENNA2'), expected.getcol('ANTENNA2'))
            if 'fStokes' in results['casa'][0]:
                np.testing.assert_allclose(actual.getkeyword('QU')['QU'],
                                           expected.getkeyword('QU')['QU'], atol=8e-5, rtol=0.)
    print(work.name, label, 'matched', flush=True)
    return tables


def reject_legacy(work, engines, vis):
    for engine, factory in engines.items():
        tool = factory()
        assert tool.setvi(True, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        try:
            assert tool.selectvis(field='0')
            for kind in ('Xparang', 'Xfparang', 'PosAng'):
                output = work/f'{kind}-legacy-rejected-{engine}.cal'
                assert result(tool.setsolve, kind, 'inf', str(output)) == (
                    False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
                assert not output.exists()
        finally:
            assert tool.close()


def compare_basis(work, basis, engines):
    work.mkdir()
    vis, leakage, gain, stokes, sky = plant(work, basis)
    with opened(vis) as table:
        epoch = table.getcol('TIME').min()
    phase = work/'planted-phase.cal'
    expected = plant_cross_hand(vis, phase, 5, epoch)
    put_observations(vis, sky, [phase])

    for kind in ('Xj', 'Xf'):
        for old in (False, True):
            products = case(work, engines, vis, f'{kind}-old{old}', kind, stokes, old=old)
            with opened(products['native']) as table:
                gain_values = expected if kind == 'Xf' else np.exp(.55j)
                np.testing.assert_allclose(table.getcol('CPARAM'), gain_values, atol=2e-5, rtol=0.)
    for kind in ('Xparang', 'Xfparang'):
        case(work, engines, vis, kind, kind, stokes)
    reject_legacy(work, engines, vis)

    case(work, engines, vis, 'Xf-ignored-cuts', 'Xf', stokes,
         options={'minblperant': 50, 'minsnr': 1e20})
    case(work, engines, vis, 'Xf-phase-only', 'Xf', stokes, options={'phaseonly': True})
    case(work, engines, vis, 'Xf-amplitude-only', 'Xf', stokes,
         options={'apmode': 'A', 'solnorm': True, 'normtype': 'median'})
    case(work, engines, vis, 'Xf-append', 'Xf', stokes, append=True)
    case(work, engines, vis, 'Xf-without-P', 'Xf', stokes, parang=False)

    put_observations(vis, sky, [gain, leakage, phase])
    case(work, engines, vis, 'Xf-selected-priors', 'Xf', stokes, prior=(gain, leakage),
         selection={'spw': '0:2~6'}, options={'t': 'inf,2chan', 'preavg': 1300.})
    data = put_observations(vis, sky, [phase])
    with opened(vis, True) as table:
        flags = table.getcol('FLAG')
        flags[:, 4] = True
        data[1:3, 3] = 0.
        assert table.putcol('FLAG', flags) and table.putcol('DATA', data)
    case(work, engines, vis, 'Xf-flagged-channels', 'Xf', stokes)
    with opened(vis, True) as table:
        assert table.putcol('FLAG', np.ones_like(flags))
    case(work, engines, vis, 'Xf-no-data', 'Xf', stokes, no_product=True)
    with opened(vis, True) as table:
        assert table.putcol('FLAG', np.zeros_like(flags))

    angles = np.broadcast_to(np.linspace(-.31, .47, sky.shape[1], dtype=np.float32)[None, :, None],
                             expected.shape).copy()
    position = work/'planted-angle.cal'
    plant_angle(vis, position, angles, epoch)
    put_observations(vis, sky, [position])
    products = case(work, engines, vis, 'PosAng', 'PosAng', stokes)
    with opened(products['native']) as table:
        np.testing.assert_allclose(table.getcol('FPARAM'), angles, atol=3e-5, rtol=0.)


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
        compare_basis(root/'circular', 'RR RL LR LL', engines)
        compare_basis(root/'linear', 'XX XY YX YY', engines)


if __name__ == '__main__':
    main()
