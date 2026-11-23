#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate direct additive Mueller solves on calibrated-table baselines."""

import argparse
from contextlib import chdir
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers, compare_solutions
from calibrater_state_oracle import casa_log, native_log
from record_casa_returns import make_cal_ms


def fit(factory, capture, dataset, table, old, spw, kind, *, mode='AP', prior=None):
    tool = factory()
    assert tool.setvi(old, True) and tool.open(str(dataset))
    try:
        assert tool.selectvis(field='0', spw=spw)
        assert tool.setptmodel([2., .1, .07, .04])
        if prior is not None:
            assert tool.setapply(table=str(prior), calwt=False)
        assert tool.setsolve(kind, 'inf', str(table), apmode=mode)
        state = capture(tool.state)
        state = state[0], [line.replace(str(table), '<output>') for line in state[1]]
        if old:
            assert tool.solve() and table.is_dir()
            assert list(tool.returndict()['solve_tables']) == [str(table)]
        else:
            assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
            assert not table.exists() and list(tool.returndict()['solve_tables']) == []
        return state, tool.activityrec()
    finally:
        assert tool.close()


def mean_by_baseline(dataset, table, channels, mode, scale):
    with opened(dataset) as source, opened(table) as cal:
        assert cal.info() == {'readme': '', 'subType': 'A Mueller', 'type': 'Calibration'}
        field = source.getcol('FIELD_ID')
        a, b = [source.getcol(f'ANTENNA{i}') for i in (1, 2)]
        data = source.getcol('DATA')[:, channels, :]
        hands = [0, data.shape[0] - 1]
        flags = source.getcol('FLAG')[:, channels, :].any(axis=0)
        weights = source.getcol('WEIGHT')[hands, None, :] * ~flags
        data = data[hands]
        weights[..., source.getcol('FLAG_ROW')] = 0.
        actual = cal.getcol('CPARAM')[:, 0, :]
        actual_flags = cal.getcol('FLAG')[:, 0, :]
        assert len(np.unique(cal.getcol('TIME'))) == 1
        assert np.any(cal.getcol('ANTENNA1') == cal.getcol('ANTENNA2'))
        for index, (first, second) in enumerate(zip(cal.getcol('ANTENNA1'), cal.getcol('ANTENNA2'))):
            selected = (field == 0) & (a == first) & (b == second)
            w = weights[..., selected]
            total = w.sum(axis=(1, 2))
            valid = total > 0.
            expected = np.divide((data[..., selected] * w).sum(axis=(1, 2)) * scale, total,
                                 out=np.ones(actual.shape[0], dtype=actual.dtype), where=valid)
            if mode == 'A':
                expected = np.abs(expected)
            elif mode == 'P':
                expected = expected / np.where(np.abs(expected) > 0., np.abs(expected), 1.)
            np.testing.assert_array_equal(actual_flags[:, index], ~valid)
            np.testing.assert_allclose(actual[:, index], expected, rtol=2e-5, atol=2e-6)


def run(args):
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir()
    source = args.work/'selected.ms'
    copytree(args.dataset, source)
    factories = {'casa': casa_calibrater}
    if not args.reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    with chdir(args.work):
        four = args.work/'four.ms'
        make_cal_ms(str(four), stokes='XX XY YX YY')
        prior = args.work/'amplitude.cal'
        casatasks.gencal(vis=str(source), caltable=str(prior), caltype='amp', parameter=[1.2])
        for label, dataset, old, spw, kind, channels, mode, apply in (
            ('single', source, True, '0:3', 'A', [3], 'AP', None),
            ('average', source, True, '0:0~8', 'A Mueller', list(range(9)), 'AP', None),
            ('amplitude', source, True, '0:3', 'A', [3], 'A', None),
            ('phase', source, True, '0:3', 'A', [3], 'P', None),
            ('with-prior', source, True, '0:3', 'A', [3], 'AP', prior),
            ('four-correlations', four, True, '0:3', 'A', [3], 'AP', None),
            ('modern', source, False, '0:3', 'A', [3], 'AP', None),
        ):
            products = {engine: args.work/f'{label}-{engine}.cal' for engine in factories}
            states = {}
            for engine, factory in factories.items():
                log = casa_log(args.work/f'{label}.log') if engine == 'casa' else native_log()
                with log as capture:
                    states[engine] = fit(factory, capture, dataset, products[engine], old, spw, kind,
                                         mode=mode, prior=apply)
                if old:
                    mean_by_baseline(dataset, products[engine], channels, mode,
                                     1. / 1.2**2 if apply is not None else 1.)
            if 'native' in products:
                assert states['casa'][0] == states['native'][0]
                same_record(states['casa'][1], states['native'][1])
                if old:
                    compare_solutions(products)
                    compare_readers(products)
            print(label, 'reference' if args.reference_only else 'matched', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--dataset', required=True, type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    run(parser.parse_args())
