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


def fit(factory, capture, dataset, table, old, spw, kind):
    tool = factory()
    assert tool.setvi(old, True) and tool.open(str(dataset))
    try:
        assert tool.selectvis(field='0', spw=spw)
        assert tool.setptmodel([1., 0., 0., 0.])
        assert tool.setsolve(kind, 'inf', str(table))
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


def mean_by_baseline(dataset, table, channels):
    with opened(dataset) as source, opened(table) as cal:
        assert cal.info() == {'readme': '', 'subType': 'A Mueller', 'type': 'Calibration'}
        field = source.getcol('FIELD_ID')
        a, b = [source.getcol(f'ANTENNA{i}') for i in (1, 2)]
        data = source.getcol('DATA')[:, channels, :]
        flags = source.getcol('FLAG')[:, channels, :]
        weights = source.getcol('WEIGHT')[:, None, :] * ~flags
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
            expected = np.divide((data[..., selected] * w).sum(axis=(1, 2)), total,
                                 out=np.ones(actual.shape[0], dtype=actual.dtype), where=valid)
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
        for label, old, spw, kind, channels in (
            ('single', True, '0:3', 'A', [3]),
            ('average', True, '0:0~8', 'A Mueller', list(range(9))),
            ('modern', False, '0:3', 'A', [3]),
        ):
            products = {engine: args.work/f'{label}-{engine}.cal' for engine in factories}
            states = {}
            for engine, factory in factories.items():
                log = casa_log(args.work/f'{label}.log') if engine == 'casa' else native_log()
                with log as capture:
                    states[engine] = fit(factory, capture, source, products[engine], old, spw, kind)
                if old:
                    mean_by_baseline(source, products[engine], channels)
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
