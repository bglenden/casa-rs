#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare additive Mueller corrections and corruption with pinned CASA."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import copy_ms, source
from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import same_record


def products(work, vis):
    tool = casa_calibrater()
    assert tool.setvi(True, True) and tool.open(str(vis), addmodel=True)
    assert tool.selectvis(field='FLUX', spw='0:2~5')
    tables = {name: work/f'{name}.cal' for name in ('inf', 'int', 'altered')}
    for name, epochs in (('inf', 1), ('int', 20)):
        assert tool.setsolve('A', name, str(tables[name])) and tool.solve()
        with opened(tables[name]) as table:
            assert table.info()['subType'] == 'A Mueller'
            assert len(np.unique(table.getcol('TIME'))) == epochs
    assert tool.close()
    copy_ms(tables['inf'], tables['altered'])
    with opened(tables['altered'], True) as table:
        p, q = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        baseline = (p == 0) & (q == 1)
        flags, offsets = (table.getcol(key) for key in ('FLAG', 'CPARAM'))
        flags[0, :, baseline] = True
        offsets[1, :, baseline] = 0.
        assert table.putcol('FLAG', flags) and table.putcol('CPARAM', offsets)
    return tables


def run_operations(factory, vis, product, legacy, order, calwt):
    with opened(vis, True) as table:
        field, p, q = (table.getcol(key) for key in ('FIELD_ID', 'ANTENNA1', 'ANTENNA2'))
        rows = np.flatnonzero((field == 0) & (p == 0) & (q == 1))
        data = table.getcol('DATA')
        flags = np.zeros(data.shape, dtype=bool)
        flags[-1, 4, rows] = True
        for name, values in (
            ('FLAG', flags), ('FLAG_ROW', np.zeros_like(field, dtype=bool)),
            ('CORRECTED_DATA', data), ('MODEL_DATA', np.full_like(data, 2.+.25j)),
            ('WEIGHT', np.full_like(table.getcol('WEIGHT'), 7.)),
            ('SIGMA', np.full_like(table.getcol('SIGMA'), 2.)),
        ):
            assert table.putcol(name, values)
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis), addmodel=True)
    assert tool.selectvis(field='FLUX', spw='0:2~5')
    assert tool.setapply(table=str(product), calwt=calwt)
    activity = []
    for operation in order:
        assert (tool.correct('calflag') if operation == 'correct' else tool.corrupt())
        activity.append(tool.activityrec())
    assert tool.close()
    with opened(vis) as table:
        columns = {key: table.getcol(key) for key in (
            'DATA', 'CORRECTED_DATA', 'MODEL_DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA',
        )}
    return activity, columns, rows


def check_law(columns, rows, product, interval, legacy, order):
    with opened(product) as table:
        p, q = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        baseline = (p == 0) & (q == 1)
        offsets = table.getcol('CPARAM')[:, 0, baseline]
        offsets[table.getcol('FLAG')[:, 0, baseline]] = 0.
    data, corrected, model = (columns[key] for key in ('DATA', 'CORRECTED_DATA', 'MODEL_DATA'))
    hands = [0, data.shape[0] - 1]
    np.testing.assert_array_equal(columns['WEIGHT'], 7.)
    for unselected in (slice(2), slice(6, None)):
        np.testing.assert_array_equal(corrected[:, unselected], data[:, unselected])
        np.testing.assert_array_equal(model[:, unselected], 2.+.25j)
    if data.shape[0] == 4:
        np.testing.assert_array_equal(corrected[1:3, 2:6, rows], 0.)
        np.testing.assert_array_equal(model[1:3, 2:6, rows], 0.)
    if interval == 'int':
        correct_sign, corrupt_sign = -1., 1.
    else:
        correct_sign = corrupt_sign = 1. if order[0] == 'corrupt' else -1.
    offsets = np.broadcast_to(offsets, (2, len(rows)))[:, None]
    samples = np.ix_(hands, [3], rows)
    np.testing.assert_allclose(corrected[samples] - data[samples], correct_sign * offsets,
                               atol=3e-6, rtol=0.)
    np.testing.assert_allclose(model[samples], (0. if legacy else 2.+.25j) + corrupt_sign * offsets,
                               atol=3e-6, rtol=0.)
    if interval == 'altered':
        np.testing.assert_array_equal(columns['FLAG'][0, 2:6, rows], True)
        np.testing.assert_array_equal(columns['FLAG'][-1, 3, rows], False)


def check_model_prior(work, product, factories, datasets):
    offset = work/'solve-prior.cal'
    copy_ms(product, offset)
    with opened(offset, True) as table:
        assert table.putcol('CPARAM', np.full_like(table.getcol('CPARAM'), 3.))
        assert table.putcol('FLAG', np.zeros_like(table.getcol('FLAG')))

    expected = 1.11 + .028 * np.arange(5)
    for dataset in datasets.values():
        with opened(dataset, True) as table:
            p, q = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
            template = table.getcol('DATA')
            noise = .003 * np.random.default_rng(1528).normal(size=template.shape)
            data = np.broadcast_to(5. * expected[p] * expected[q], template.shape) + noise
            assert table.putcol('DATA', data.astype('complex64'))
            assert table.putcol('MODEL_DATA', np.full_like(template, 2.))
            assert table.putcol('FLAG', np.zeros_like(template, dtype=bool))

    for use_point in (False, True):
        values = {}
        for label, factory in factories.items():
            tool = factory()
            output = work/f'gain-prior-{use_point}-{label}.cal'
            assert tool.open(str(datasets[label]), addmodel=True) and tool.selectvis(field='FLUX')
            if use_point:
                assert tool.setptmodel([2., 0., 0., 0.])
            assert tool.setapply(table=str(offset), calwt=False)
            assert tool.setsolve('G', 'inf', str(output), refant='A0', minblperant=2)
            assert tool.solve() and tool.close()
            with opened(output) as table:
                antennas = table.getcol('ANTENNA1')
                gains = table.getcol('CPARAM')[:, 0, np.argsort(antennas)]
                np.testing.assert_allclose(gains, np.broadcast_to(expected, gains.shape),
                                           atol=.001, rtol=0.)
                values[label] = gains
        if 'native' in values:
            np.testing.assert_allclose(values['native'], values['casa'], atol=1e-5, rtol=0.)
        print('point-model' if use_point else 'physical-model', 'additive prior matched', flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    factories = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    for stokes in ('RR LL', 'RR RL LR LL'):
        case = work/f'{len(stokes.split())}-correlations'
        case.mkdir()
        with chdir(case):
            reference = source(case, stokes)
            tables = products(case, reference)
            native = case/'native.ms'
            if not reference_only:
                copy_ms(reference, native)
            for legacy in (True, False):
                for interval, product in tables.items():
                    for order in (('correct', 'corrupt'), ('corrupt', 'correct')):
                        calwt = order[0] == 'correct'
                        activity, values, rows = run_operations(
                            casa_calibrater, reference, product, legacy, order, calwt)
                        check_law(values, rows, product, interval, legacy, order)
                        if not reference_only:
                            recorded, candidate, _ = run_operations(
                                native_calibrater, native, product, legacy, order, calwt)
                            for actual, expected in zip(recorded, activity):
                                same_record(actual, expected)
                            for key, actual in candidate.items():
                                if key in ('CORRECTED_DATA', 'MODEL_DATA'):
                                    np.testing.assert_allclose(actual, values[key], atol=3e-6, rtol=0.)
                                else:
                                    np.testing.assert_array_equal(actual, values[key])
                        print(case.name, legacy, interval, order[0],
                              'reference' if reference_only else 'matched', flush=True)
            if stokes == 'RR LL':
                datasets = {'casa': reference}
                if not reference_only:
                    datasets['native'] = native
                check_model_prior(case, tables['inf'], factories, datasets)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
