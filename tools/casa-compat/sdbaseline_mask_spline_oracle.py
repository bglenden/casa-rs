#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired automatic line masks and cubic-spline baselines with CASA 6.7.6.14."""

import argparse
from pathlib import Path
import time

import casatasks as casa
from casatools import singledishms
import numpy as np

from calibration_smoothcal_oracle import opened
from applycal_oracle import copy_ms
from sdbaseline_oracle import check_application, compare_baselines, compare_reports, tool_apply, tool_fit
from sdsmooth_oracle import compare_outputs, spectral_map


CASES = [
    (5., 4, 4, [3, 5], [[3, 19, 21, 28, 43, 61, 74, 90]] * 2),
    (5., 1, 4, [3, 5], [[3, 19, 21, 28, 43, 61, 72, 90]] * 2),
    (4., 4, 3, [2, 7], [[2, 14, 21, 28, 43, 61, 74, 88],
                         [2, 19, 21, 27, 43, 61, 74, 88]]),
    (9., 16, 5, [3, 5], [[3, 19, 21, 28, 43, 61, 74, 90]] * 2),
    (5., 4, 4, [2], [[2, 19, 21, 28, 43, 61, 74, 93]] * 2),
    (5., 0, 4, [], [[0, 19, 21, 28, 43, 61, 72, 95]] * 2),
]


def spectral_lines(root):
    source = spectral_map(root, (96, 96))
    channel = np.arange(96, dtype=np.float32)
    noise = np.tile(np.array([-.12, .08, -.06, .02, .11, -.04, .05, 0],
                             dtype=np.float32), 12)
    base = 7. + .003 * channel + noise
    spectra = np.stack([base, base + 2.])
    spectra[:, 30:42] += 7.
    spectra[:, 63:72] -= 5.
    spectra[0, 15:18] += .65
    flags = np.zeros_like(spectra, dtype=bool)
    flags[:, 20] = True
    with opened(source, True) as vis:
        for row in range(vis.nrows()):
            vis.putcell('DATA', row, spectra.astype(np.complex64))
            vis.putcell('FLAG', row, flags)
    return source, spectra, flags


def check_line_models(table, spectra, ranges):
    x = np.arange(spectra.shape[1])
    coefficients = []
    for values, mask in zip(spectra, ranges):
        included = np.zeros_like(x, dtype=bool)
        for start, stop in np.array(mask).reshape(-1, 2):
            included[start:stop + 1] = True
        coefficients.append(np.polynomial.polynomial.polyfit(x[included], values[included], 1))
    with opened(table) as models:
        assert models.nrows() == 240
        for row in range(models.nrows()):
            np.testing.assert_array_equal(models.getcell('MASKLIST', row), ranges)
            np.testing.assert_allclose(models.getcell('RESULT', row), coefficients, atol=2e-6)
            # In CASA these columns are populated by variable baselines.
            np.testing.assert_array_equal(models.getcell('USE_LF', row), False)
            np.testing.assert_array_equal(models.getcell('LF_EDGE', row), 0)
    return spectra - np.array([np.polynomial.polynomial.polyval(x, terms) for terms in coefficients])


def paired_masks(root, source, spectra, flags, tasks, tools):
    for index, (threshold, average, width, edges, ranges) in enumerate(CASES):
        tables = {}
        for name, task in tasks.items():
            table = root / f'auto-{index}-{name}.bltable'
            output = root / f'auto-{index}-{name}.unused.ms'
            assert task(
                str(source), outfile=str(output), dosubtract=False, maskmode='auto',
                thresh=threshold, avg_limit=average, minwidth=width, edge=edges,
                blfunc='poly', order=1, blformat='table', bloutput=str(table),
            ) is None
            assert not output.exists()
            check_line_models(table, spectra, ranges)
            tables[name] = table
        if 'native' in tables:
            compare_baselines(tables['casa'], tables['native'])

    products = {}
    threshold, average, width, edges, ranges = CASES[0]
    for name, factory in tools.items():
        table = root / f'direct-auto-{name}.bltable'
        output = root / f'direct-auto-{name}.ms'
        tool_fit(factory, source, output, table, 'poly', linefinding=True,
                 threshold=threshold, avg_limit=average, minwidth=width, edge=edges)
        expected = check_line_models(table, spectra, ranges)
        np.testing.assert_array_less(.4, np.abs(expected[:, 30:42]))
        with opened(output) as result:
            for row in range(result.nrows()):
                np.testing.assert_allclose(result.getcell('DATA', row), expected, atol=1e-5)
                np.testing.assert_array_equal(result.getcell('FLAG', row), flags)
        products[name] = output
    if 'native' in products:
        compare_outputs(products['casa'], products['native'])

    narrow = {}
    invalid = [
        ({'minwidth': 0}, 'minwidth > 0'),
        ({'minwidth': 96}, 'maxwidth > static_cast<size_t>(minwidth)'),
        ({'avg_limit': -1}, 'avg_limit >= 0'),
        ({'edge': [-1]}, 'edge[0] >= 0'),
        ({'edge': [-1, 2]}, 'edge[0] >= 0 && edge[1] >= 0'),
        ({'edge': [50, 48]}, 'edge.first+edge.second<num_data-min_valid_elem'),
        ({'edge': [47, 43]}, 'num_valid_elem >= min_valid_elem+edge.first+edge.second'),
        ({'avg_limit': 256}, 'status == LIBSAKURA_SYMBOL(Status_kOK)'),
    ]
    for name, task in tasks.items():
        output = root / f'auto-narrow-{name}.ms'
        assert task(str(source), outfile=str(output), spw='*:0~1', order=1,
                    maskmode='auto', blformat='') is None
        narrow[name] = output
        for index, (parameters, reason) in enumerate(invalid):
            try:
                task(str(source), outfile=str(root / f'auto-error-{index}-{name}.ms'),
                     order=1, maskmode='auto', blformat='', **parameters)
            except RuntimeError as error:
                assert str(error).endswith(f'Failed AlwaysAssert {reason}'), str(error)
            else:
                raise AssertionError(f'accepted invalid automatic mask parameters {parameters}')
    if 'native' in narrow:
        compare_outputs(narrow['casa'], narrow['native'])


def spline_input(root, source, pieces):
    infile = root / f'spline-{pieces}-input.ms'
    copy_ms(source, infile)
    channels = np.arange(96)
    include = (channels < 24) | ((30 <= channels) & (channels < 63)) | (69 <= channels)
    flags = np.zeros((2, 96), dtype=bool)
    flags[0, 6] = True
    flags[1, 47] = True
    knots = []
    line = np.where(include, 0., 3.5)
    spectra = []
    for pol in range(2):
        valid = np.flatnonzero(include & ~flags[pol])
        bounds = [0, *(valid[(len(valid) * i + pieces - 1) // pieces]
                        for i in range(1, pieces)), len(channels)]
        knots.append(bounds)
        x = channels / (len(channels) - 1)
        spectrum = 7. + pol + .2 * x + .7 * x**2 - .05 * x**3
        for i, boundary in enumerate(bounds[1:-1], start=1):
            spectrum += (-1.)**i * (i + 2.) * np.maximum(0., x - x[boundary])**3
        spectra.append(spectrum + line)
    with opened(infile, True) as main:
        for row in range(main.nrows()):
            main.putcell('DATA', row, np.array(spectra, dtype=np.complex64))
            main.putcell('FLAG', row, flags)
    return infile, knots, line, flags


def check_spline(product, table, pieces, knots, line, flags):
    with opened(product) as main, opened(table) as models:
        assert main.nrows() == models.nrows() == 240
        for row in range(main.nrows()):
            np.testing.assert_allclose(main.getcell('DATA', row), np.tile(line, (2, 1)), atol=1e-5)
            np.testing.assert_array_equal(main.getcell('FLAG', row), flags)
            np.testing.assert_array_equal(models.getcell('FUNC_TYPE', row), 2)
            np.testing.assert_array_equal(models.getcell('FUNC_PARAM', row), pieces)
            np.testing.assert_array_equal(models.getcell('FUNC_FPARAM', row), knots)
            assert models.getcell('RESULT', row).shape == (2, 4 * pieces)


def direct_spline(factory, source, prefix, mask, pieces, **options):
    tool = factory()
    try:
        assert tool.open(str(source)) is True
        assert tool.set_selection(spw='*') is True
        assert tool.subtract_baseline_cspline(
            outfile=str(prefix.with_suffix('.ms')),
            bloutput=f',,{prefix.with_suffix(".bltable")}', spw=mask, npiece=pieces, **options,
        ) is True
    finally:
        assert tool.close() is True


def paired_splines(root, source, tasks, tools):
    mask = '*:0~23;30~62;69~95'
    for pieces in (1, 2, 3):
        infile, knots, line, flags = spline_input(root, source, pieces)
        products = {}
        for name, task in tasks.items():
            prefix = root / f'spline-{pieces}-{name}'
            output, table = prefix.with_suffix('.ms'), prefix.with_suffix('.bltable')
            start = time.perf_counter()
            assert task(str(infile), spw=mask, blfunc='CSPLINE', npiece=pieces, outfile=str(output),
                        blformat=['csv', 'text', 'table'],
                        bloutput=[str(prefix.with_suffix(ext)) for ext in ('.csv', '.txt', '.bltable')]) is None
            if pieces == 2:
                print(f'{name} cubic-spline sdbaseline 240 rows wall {time.perf_counter() - start:.3f}s')
            check_spline(output, table, pieces, knots, line, flags)
            products[name] = prefix
        if 'native' in products:
            expected, actual = products['casa'], products['native']
            compare_outputs(expected.with_suffix('.ms'), actual.with_suffix('.ms'))
            compare_baselines(expected.with_suffix('.bltable'), actual.with_suffix('.bltable'))
            compare_reports(expected, actual)
        if pieces == 2:
            for owner, prefix in products.items():
                for reader, factory in tools.items():
                    applied = root / f'spline-{owner}-{reader}-apply.ms'
                    tool_apply(factory, infile, prefix.with_suffix('.bltable'), applied)
                    check_application(prefix.with_suffix('.ms'), applied)
            for name, factory in tools.items():
                direct = root / f'direct-spline-{name}'
                direct_spline(factory, infile, direct, mask, pieces)
                check_spline(direct.with_suffix('.ms'), direct.with_suffix('.bltable'),
                             pieces, knots, line, flags)
        if pieces == 3:
            check_clipped_spline(root, infile, mask, knots, line, flags, tools)

    automatically_masked = {}
    for name, factory in tools.items():
        prefix = root / f'auto-spline-{name}'
        direct_spline(factory, source, prefix, '*', 2, linefinding=True, threshold=5.,
                      avg_limit=4, minwidth=4, edge=[3, 5])
        with opened(prefix.with_suffix('.bltable')) as table:
            np.testing.assert_array_equal(table.getcell('MASKLIST', 0), CASES[0][-1])
        automatically_masked[name] = prefix
    if 'native' in automatically_masked:
        expected, actual = automatically_masked['casa'], automatically_masked['native']
        compare_outputs(expected.with_suffix('.ms'), actual.with_suffix('.ms'))
        compare_baselines(expected.with_suffix('.bltable'), actual.with_suffix('.bltable'))


def check_clipped_spline(root, source, mask, knots, line, flags, tools):
    infile = root / 'spline-clipping-input.ms'
    copy_ms(source, infile)
    with opened(infile, True) as main:
        for row in range(main.nrows()):
            data = main.getcell('DATA', row)
            data[:, 10] += 12
            main.putcell('DATA', row, data)
    expected = line.copy()
    expected[10] += 12
    tables = {}
    for name, factory in tools.items():
        prefix = root / f'spline-clipped-{name}'
        direct_spline(factory, infile, prefix, mask, 3,
                      clip_threshold_sigma=3., num_fitting_max=2)
        table = prefix.with_suffix('.bltable')
        check_spline(prefix.with_suffix('.ms'), table, 3, knots, expected, flags)
        with opened(table) as models:
            ranges = models.getcell('MASKLIST', 0)
            assert all(not any(start <= 10 <= end for start, end in receptor.reshape(-1, 2))
                       for receptor in ranges)
        tables[name] = table
        try:
            direct_spline(factory, infile, root / f'spline-bad-pieces-{name}', mask, 0)
        except RuntimeError as error:
            assert 'npiece must be positive.' in str(error)
        else:
            raise AssertionError('created a cubic spline with zero pieces')
    if 'native' in tables:
        compare_baselines(tables['casa'], tables['native'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--auto-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    tasks = {'casa': casa.sdbaseline}
    tools = {'casa': singledishms}
    if not args.reference_only:
        from casars.tasks import sdbaseline
        from casars.tools import singledishms as native_tool
        tasks['native'] = sdbaseline
        tools['native'] = native_tool
    source, spectra, flags = spectral_lines(root)
    paired_masks(root, source, spectra, flags, tasks, tools)
    print('Automatic line masks and fitted spectra agree with CASA.', flush=True)
    if not args.auto_only:
        paired_splines(root, source, tasks, tools)
        print('Cubic-spline fits, coefficients and both table readers agree.', flush=True)


if __name__ == '__main__':
    main()
