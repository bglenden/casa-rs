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
        check_partial_time_flags(work, factories, datasets, coordinates)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    run(args.work, args.reference_only)
