#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct X Mueller solutions and correction with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
from casatools import table as casa_table
import numpy as np

from applycal_oracle import copy_ms
from calibrater_apply_tool_oracle import result
from calibrater_solve_tool_oracle import compare_readers, compare_solutions
from calibrater_state_oracle import casa_log, native_log
from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import plant, put_observations


def solve(work, engines, vis, label, stokes, *, old=True, options=None, selection=None, writes=True):
    products = {engine: work/f'{label}-{engine}.cal' for engine in engines}
    for engine, factory in engines.items():
        product = products[engine]
        tool = factory()
        assert tool.setvi(old, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        try:
            assert tool.selectvis(**({'field': '0'} | (selection or {})))
            if stokes is not None:
                assert tool.setptmodel(stokes)
            setup = {'t': 'inf', 'refant': 'A3'} | (options or {})
            assert tool.setsolve('X', table=str(product), **setup)
            assert list(tool.returndict()['solve_tables']) == [str(product)]
            if old:
                assert tool.solve() and product.exists() == writes
                assert tool.activityrec() == {} and 'solvestats' not in tool.returndict()
                assert tool.reset(False, True) and len(tool.returndict()['solve_tables']) == 0
            else:
                assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
                assert not product.exists()
        finally:
            assert tool.close()
    if old and writes:
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
            outputs, states = {}, {}
            for engine, factory in engines.items():
                output = work/f'{label}-{source}-{old}-{engine}.ms'
                copy_ms(vis, output)
                tool = factory()
                assert tool.setvi(old, True) and tool.open(str(output), addcorr=True, addmodel=False)
                try:
                    assert tool.selectvis(field='0')
                    assert tool.setapply(table=str(product), calwt=True)
                    log = (casa_log(work/f'{label}-{source}-{old}-state.log')
                           if engine == 'casa' else native_log())
                    with log as capture:
                        states[engine] = capture(tool.state)
                    assert tool.correct()
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
            assert states['native'] == states['casa']
            assert 'calWt=false' in states['casa'][1][1]
            for actual, expected in zip(outputs['native'][1:], outputs['casa'][1:]):
                np.testing.assert_array_equal(actual, expected)
            print(work.name, label, source, old, 'matched correction', flush=True)


def changed_first_baseline(work, original):
    product = work/'first-baseline.cal'
    with opened(original) as table:
        copied = table.copy(str(product), deep=True, valuecopy=True)
        assert copied.close()
    with opened(product, True) as table:
        gains = table.getcol('CPARAM')
        flags = table.getcol('FLAG')
        gains[...] = 1.5 - .7j
        gains[..., 0] = .7 + .3j
        flags[..., 0] = True
        assert table.putcol('CPARAM', gains) and table.putcol('FLAG', flags)
    return product


def compare_scans(work, engines, vis, stokes):
    data = work/'two-scans.ms'
    copy_ms(vis, data)
    with opened(data, True) as table:
        times, scans = (table.getcol(name) for name in ('TIME', 'SCAN_NUMBER'))
        field = table.getcol('FIELD_ID') == 0
        late = field & (times > np.median(times[field]))
        scans[late] = scans.max() + 3
        assert table.putcol('SCAN_NUMBER', scans)
    solve(work, engines, data, 'legacy-scans', stokes)
    with opened(data, True) as table:
        flags = table.getcol('FLAG')
        flags[..., late] = True
        assert table.putcol('FLAG', flags)
    products = solve(work, engines, data, 'legacy-flagged-scan', stokes)
    with opened(products['native']) as table:
        assert table.nrows() == 30 and table.getcol('FLAG').sum() == 15
    with opened(data, True) as table:
        flags[..., field] = True
        assert table.putcol('FLAG', flags)
    solve(work, engines, data, 'legacy-all-flagged', stokes, writes=False)


def compare_combined(work, engines, vis, axis):
    data = work/f'combined-{axis}.ms'
    copy_ms(vis, data)
    if axis == 'spw':
        assert duplicate_rows(casa_table(), data/'SPECTRAL_WINDOW') == 1
        assert duplicate_rows(casa_table(), data/'DATA_DESCRIPTION') == 1
        with opened(data/'DATA_DESCRIPTION', True) as table:
            assert table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    added = duplicate_rows(casa_table(), data)
    with opened(data, True) as table:
        fields = table.getcol('FIELD_ID')
        times = table.getcol('TIME')
        first = times[fields == 0].min()
        scans = table.getcol('SCAN_NUMBER')
        assert table.putcol('SCAN_NUMBER', np.full_like(scans, scans.min()))
        column = 'DATA_DESC_ID' if axis == 'spw' else 'FIELD_ID'
        assert table.putcol(column, np.ones(added, np.int32), added, added)
        for name in ('DATA', 'MODEL_DATA'):
            values = table.getcol(name)
            values[1:3] = 1.
            values[1:3, :, added:] = 2.
            if name == 'DATA':
                values[1, :, added:] *= 1j
                values[2, :, added:] *= -1j
            assert table.putcol(name, values)
    selection = {'field': '0' if axis == 'spw' else '0,1', 'baseline': 'A0&A1',
                 'msselect': f'abs(TIME-{first}) < 0.01'}
    result = solve(work, engines, data, f'legacy-combine-{axis}', None,
                   options={'combine': axis}, selection=selection)
    with opened(result['native']) as table:
        np.testing.assert_allclose(table.getcol('CPARAM'), (1 + 2j) / np.sqrt(5.), atol=1e-6)


def compare_zero_parallel(work, engines, vis):
    data = work/'zero-parallel.ms'
    copy_ms(vis, data)
    with opened(data, True) as table:
        chosen = (table.getcol('ANTENNA1') == 0) & (table.getcol('ANTENNA2') == 2)
        for name in ('DATA', 'MODEL_DATA'):
            values = table.getcol(name)
            values[1:3] = 1.
            if name == 'DATA':
                values[1, :, chosen] *= 1j
                values[2, :, chosen] *= -1j
            assert table.putcol(name, values)
        for name in ('SIGMA', 'WEIGHT'):
            values = np.ones_like(table.getcol(name))
            values[0, chosen] = values[3, chosen] = 0.
            assert table.putcol(name, values)
    result = solve(work, engines, data, 'legacy-zero-parallel', None)
    with opened(result['native']) as table:
        np.testing.assert_array_equal(table.getcol('CPARAM'), 1.)


def corrupt_zero(work, engines, vis, original):
    product = work/'zero-gain.cal'
    with opened(original) as table:
        assert table.copy(str(product), deep=True, valuecopy=True).close()
    with opened(product, True) as table:
        assert table.putcell('CPARAM', 0, np.zeros((1, 1), np.complex64))
    for engine, factory in engines.items():
        data = work/f'zero-model-{engine}.ms'
        copy_ms(vis, data)
        with opened(data) as table:
            field = table.getcol('FIELD_ID') == 0
            before = table.getcol('MODEL_DATA')[..., field]
            assert np.any(before[1:3])
        tool = factory()
        assert tool.setvi(True, True) and tool.open(str(data), addcorr=False, addmodel=False)
        try:
            assert tool.selectvis(field='0') and tool.setapply(table=str(product)) and tool.corrupt()
        finally:
            assert tool.close()
        with opened(data) as table:
            after = table.getcol('MODEL_DATA')[..., field]
            np.testing.assert_array_equal(after[[0, 3]], before[[0, 3]])
            np.testing.assert_array_equal(after[1:3], 0.)
    print(work.name, 'zero-forward matched', flush=True)


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
    compare_scans(work, engines, vis, stokes)
    for axis in ('field', 'spw'):
        compare_combined(work, engines, vis, axis)
    compare_zero_parallel(work, engines, vis)
    if not solutions_only:
        correct(work, engines, vis, positive, 'correct-positive')
        correct(work, engines, vis, flagged, 'correct-flagged')
        correct(work, engines, vis, {'changed': changed_first_baseline(work, positive['casa'])},
                'correct-first-baseline')
        corrupt_zero(work, engines, vis, positive['casa'])


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
