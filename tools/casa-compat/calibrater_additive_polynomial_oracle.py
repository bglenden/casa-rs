#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the CASA A Mueller polynomial domain, coefficients and application."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import copy_ms, source
from bpoly_oracle import multiwindow_data
from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers

FIT_CHANNELS = slice(2, 10)
MODEL_START = 2. + .25j


def polynomial_fixture(work):
    vis = source(work, 'RR RL LR LL')
    with opened(vis/'SPECTRAL_WINDOW') as table:
        frequency = table.getcol('CHAN_FREQ')[:, 0]
    low, high = frequency[FIT_CHANNELS][[0, -1]]
    coordinates = 2. * (frequency - low) / (high - low) - 1.
    with opened(vis, True) as table:
        p, q = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        baseline_scale = (1. + (p + q)[None, None, :] * .1)
        hand_scale = (1. + np.arange(4)[:, None, None] * .05)
        x = coordinates[None, :, None]
        signal = (.4 + .02j) + (.13 - .026j) * x + (-.07 + .08j) * x**2
        data = (signal * baseline_scale * hand_scale).astype(np.complex64)
        weight = np.ones_like(table.getcol('WEIGHT'))
        for key, value in (
            ('DATA', data), ('CORRECTED_DATA', data), ('MODEL_DATA', np.full_like(data, MODEL_START)),
            ('FLAG', np.zeros(data.shape, dtype=bool)), ('FLAG_ROW', np.zeros(len(p), dtype=bool)),
            ('WEIGHT', weight), ('SIGMA', weight),
        ):
            assert table.putcol(key, value)
    return vis, (low, high), coordinates


def calibrate(factory, vis, product, mode, order, legacy, **options):
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis), addmodel=True)
    assert tool.selectvis(field='FLUX', spw='0:2~9')
    assert tool.setptmodel([2., .1, .07, .04])
    assert tool.setsolve('A', 'inf', str(product), fitorder=order, apmode=mode, **options)
    actual = result(tool.solve)
    if order < 0 or not legacy:
        assert actual == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
        assert not product.exists()
    else:
        assert actual == (True, True)
        assert product.is_dir()
    assert tool.close()


def check_coefficients(vis, product, domain, coordinates, mode):
    with opened(vis) as table:
        p, q, field = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2', 'FIELD_ID'))
        data = table.getcol('DATA')
    with opened(product) as table, opened(product/'SPECTRAL_WINDOW') as spw:
        x = coordinates[FIT_CHANNELS]
        assert table.info()['subType'] == 'A Mueller' and table.nrows() == 15
        assert spw.getcell('NUM_CHAN', 0) == 3 and not spw.getcell('FLAG_ROW', 0)
        np.testing.assert_array_equal(spw.getcell('CHAN_FREQ', 0), domain)
        np.testing.assert_array_equal(spw.getcell('CHAN_WIDTH', 0), [16000000.])
        first, second = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        for index, (a, b) in enumerate(zip(first, second, strict=True)):
            row = np.flatnonzero((field == 0) & (p == a) & (q == b))[0]
            target = data[:, FIT_CHANNELS, row]
            if mode == 'A':
                target = np.abs(target)
            elif mode == 'P':
                target = target / np.abs(target)
            expected = np.polynomial.polynomial.polyfit(x, target.T, 2).T
            np.testing.assert_allclose(table.getcell('CPARAM', index), expected,
                                       atol=2e-6, rtol=0.)
            np.testing.assert_array_equal(table.getcell('FLAG', index), False)


def check_apply(factory, vis, product, legacy):
    with opened(vis, True) as table:
        data = table.getcol('DATA')
        field = table.getcol('FIELD_ID')
        assert table.putcol('CORRECTED_DATA', data)
        assert table.putcol('MODEL_DATA', np.full_like(data, MODEL_START))
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis), addmodel=True)
    assert tool.selectvis(field='FLUX', spw='0:2~9')
    assert tool.setapply(table=str(product), calwt=True)
    assert tool.correct('calflag')
    activity = tool.activityrec()
    assert tool.corrupt() and tool.close()
    with opened(vis) as table:
        corrected, model = (table.getcol(key) for key in ('CORRECTED_DATA', 'MODEL_DATA'))
        chosen = field == 0
        np.testing.assert_allclose(corrected[:, FIT_CHANNELS, chosen], 0., rtol=0., atol=3e-6)
        np.testing.assert_allclose(model[:, FIT_CHANNELS, chosen], data[:, FIT_CHANNELS, chosen],
                                   rtol=0., atol=3e-6)
        for unselected in (slice(2), slice(10, None)):
            np.testing.assert_array_equal(corrected[:, unselected], data[:, unselected])
            np.testing.assert_array_equal(model[:, unselected], MODEL_START)
        np.testing.assert_array_equal(table.getcol('WEIGHT'), 1.)
        np.testing.assert_array_equal(table.getcol('FLAG'), False)
    return activity


def check_partial_time_flags(work, factories, datasets, coordinates):
    # The last channel is missing from one of the two retained integrations.
    # VI1 divides that channel's collected sum by the full baseline weight.
    samples = np.ones(8, dtype=np.complex64)
    samples[-1] = 1.5 + .125j
    expected = np.polynomial.polynomial.polyfit(coordinates[FIT_CHANNELS], samples, 2)
    for name, factory in factories.items():
        vis = datasets[name]
        with opened(vis, True) as table:
            p, q, field = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2', 'FIELD_ID'))
            rows = np.flatnonzero((p == 0) & (q == 1) & (field == 0))
            data = table.getcol('DATA')
            flags = np.zeros_like(data, dtype=bool)
            flags[..., rows] = True
            flags[..., rows[0]] = False
            flags[:, 2:9, rows[-1]] = False
            data[..., rows[0]] = 3. + .25j
            data[..., rows[-1]] = -1. - .25j
            assert table.putcol('DATA', data) and table.putcol('FLAG', flags)
        product = work/f'partial-flags-{name}.cal'
        calibrate(factory, vis, product, 'AP', 2, True, preavg=.1)
        with opened(product) as table:
            a, b = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
            selected = np.flatnonzero((a == 0) & (b == 1))[0]
            coefficients = table.getcell('CPARAM', int(selected))
            np.testing.assert_allclose(coefficients, np.broadcast_to(expected, coefficients.shape),
                                       atol=2e-6, rtol=0.)
    print('partial time flags and full-interval preaveraging matched', flush=True)


def check_flagged_model(work, product, factories, datasets):
    flagged = work/'flagged-constant-coefficient.cal'
    copy_ms(product, flagged)
    with opened(flagged, True) as table:
        first, second = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        flags = table.getcol('FLAG')
        flags[0, :, (first == 0) & (second == 1)] = True
        assert table.putcol('FLAG', flags)
    for legacy in (False, True):
        for name, factory in factories.items():
            vis = datasets[name]
            with opened(vis, True) as table:
                data = table.getcol('DATA')
                assert table.putcol('MODEL_DATA', np.full_like(data, MODEL_START))
                field, p, q = (table.getcol(key) for key in ('FIELD_ID', 'ANTENNA1', 'ANTENNA2'))
                baseline = (field == 0) & (p == 0) & (q == 1)
            tool = factory()
            assert tool.setvi(legacy, True) and tool.open(str(vis), addmodel=True)
            assert tool.selectvis(field='FLUX', spw='0:2~9')
            assert tool.setapply(table=str(flagged)) and tool.corrupt() and tool.close()
            with opened(vis) as table:
                model = table.getcol('MODEL_DATA')
                np.testing.assert_array_equal(model[0, FIT_CHANNELS, baseline],
                                              0. if legacy else MODEL_START)
                np.testing.assert_allclose(model[1:, FIT_CHANNELS, baseline],
                                           data[1:, FIT_CHANNELS, baseline], rtol=0., atol=3e-6)
        print('flagged polynomial model', legacy, 'matched', flush=True)


def check_combined_domain(work, factories, reference):
    dataset = work/'combined.ms'
    multiwindow_data(reference, dataset)
    with opened(dataset, True) as table:
        field, dd, time = (table.getcol(k) for k in ('FIELD_ID', 'DATA_DESC_ID', 'TIME'))
        scan = np.where(time < np.median(time[field == 0]), 1, 2)
        data = np.broadcast_to(1. + 2. * dd, table.getcol('DATA').shape).astype('complex64')
        flags = np.zeros(data.shape, dtype=bool)
        flags[..., (scan == 1) & (dd == 1)] = True
        for key, value in (('DATA', data), ('SCAN_NUMBER', scan.astype('int32')),
                           ('FLAG', flags), ('FLAG_ROW', flags.all(axis=(0, 1)))):
            assert table.putcol(key, value)
    with opened(dataset/'SPECTRAL_WINDOW') as table:
        domain = table.getcol('CHAN_FREQ')[3, :]
    for name, factory in factories.items():
        product = work/f'combined-{name}.cal'
        tool = factory()
        assert tool.setvi(True, True) and tool.open(str(dataset))
        assert tool.selectvis(field='FLUX', spw='0:3,1:3')
        assert tool.setsolve('A', 'inf', str(product), fitorder=1, combine='spw')
        assert tool.solve() and tool.close()
        with opened(product) as table, opened(product/'SPECTRAL_WINDOW') as spw:
            p, q = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
            rows = (p == 0) & (q == 1)
            np.testing.assert_array_equal(table.getcol('SCAN_NUMBER')[rows], [1, 2])
            np.testing.assert_array_equal(table.getcol('FLAG')[0, :, rows],
                                          [[False, True], [False, False]])
            if name == 'native':
                np.testing.assert_array_equal(spw.getcell('CHAN_FREQ', 0), domain)
                np.testing.assert_allclose(table.getcol('CPARAM')[0, :, rows],
                                           [[1., 0.], [2., 1.]], atol=2e-6, rtol=0.)
            else:
                np.testing.assert_array_equal(spw.getcell('CHAN_FREQ', 0), [domain[0]] * 2)
        print('initially flagged combined window', name, 'checked', flush=True)


def check_reversed_table(work, product, vis, factory):
    reversed_table = work/'reversed-calibration.cal'
    copy_ms(product, reversed_table)
    with opened(reversed_table, True) as table:
        p, q = (table.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        row = int(np.flatnonzero((p == 0) & (q == 1))[0])
        coefficients, flags = (table.getcell(key, row) for key in ('CPARAM', 'FLAG'))
        order = [0, 2, 1, 3]
        for key, value in (('ANTENNA1', 1), ('ANTENNA2', 0),
                           ('CPARAM', coefficients[order].conj()), ('FLAG', flags[order])):
            assert table.putcell(key, row, value)
    check_apply(factory, vis, reversed_table, False)
    print('reversed coefficient row and cross hands matched', flush=True)


def run(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    factories = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    with chdir(work):
        reference, domain, coordinates = polynomial_fixture(work)
        datasets = {'casa': reference}
        if not reference_only:
            datasets['native'] = work/'native.ms'
            copy_ms(reference, datasets['native'])
        for label, mode, order, legacy in (
            ('complex', 'AP', 2, True), ('amplitude', 'A', 2, True), ('phase', 'P', 2, True),
            ('negative', 'AP', -1, True), ('modern', 'AP', 2, False),
        ):
            products = {name: work/f'{label}-{name}.cal' for name in factories}
            for writer, factory in factories.items():
                calibrate(factory, datasets[writer], products[writer], mode, order, legacy)
                if order == 2 and legacy:
                    check_coefficients(datasets[writer], products[writer], domain, coordinates, mode)
            if order == 2 and legacy and not reference_only:
                compare_readers(products)
            if label == 'complex':
                for old in (True, False):
                    for writer, product in products.items():
                        activities = {reader: check_apply(factory, datasets[reader], product, old)
                                      for reader, factory in factories.items()}
                        if not reference_only:
                            same_record(activities['native'], activities['casa'])
                        print(label, writer, old, 'application matched', flush=True)
            print(label, 'reference' if reference_only else 'matched', flush=True)
        check_flagged_model(work, work/'complex-casa.cal', factories, datasets)
        check_combined_domain(work, factories, reference)
        if 'native' in factories:
            check_reversed_table(work, work/'complex-casa.cal', datasets['native'], factories['native'])
        check_partial_time_flags(work, factories, datasets, coordinates)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    run(args.work, args.reference_only)
