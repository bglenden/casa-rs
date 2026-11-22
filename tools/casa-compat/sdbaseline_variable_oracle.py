#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare per-spectrum sdbaseline files, reports and tables with CASA 6.7.6.14."""

import argparse
import os
from pathlib import Path
import subprocess
import time

import casatasks as casa
from casatools import singledishms
import numpy as np

from calibration_smoothcal_oracle import opened
from sdbaseline_mask_spline_oracle import one_spectrum
from sdbaseline_oracle import check_application, compare_baselines, compare_reports, tool_apply
from sdbaseline_sinusoid_oracle import expected_error
from sdsmooth_oracle import compare_outputs, spectral_map


def write_parameters(root, name, *rows):
    path = root / f'{name}.params'
    path.write_text('# Row, polarization, fit controls and basis\n' + '\n'.join(rows) + '\n')
    return path


def set_spectra(path):
    channel = np.arange(96)
    phase = channel * (2 * np.pi / 95)
    noise = np.resize(np.array([-.021, .018, .008, -.005, .002, -.011, .015, 0.]), 96)
    spectra = np.array([
        4. -.03 * channel + .0002 * channel**2 + noise,
        6. + 1.8 * np.sin(3 * phase) + .3 * np.cos(3 * phase)
        + .7 * np.sin(7 * phase) - .6 * np.cos(7 * phase) + noise,
    ], dtype=np.complex64)
    spectra[0, 40:45] += 7.
    spectra[0, 25] += 10.
    flags = np.zeros(spectra.shape, dtype=bool)
    flags[0, 2] = flags[1, 82] = True
    with opened(path, True) as main:
        for row in range(main.nrows()):
            main.putcell('DATA', row, spectra)
            main.putcell('FLAG', row, flags)
    return spectra.real, flags


def fit_task(task, source, params, prefix, *, reports=False, **options):
    output, table = prefix.with_suffix('.ms'), prefix.with_suffix('.bltable')
    formats = ['csv', 'text', 'table'] if reports else ['table']
    extensions = ['.csv', '.txt', '.bltable'] if reports else ['.bltable']
    files = [str(prefix.with_suffix(suffix)) for suffix in extensions]
    try:
        assert task(str(source), outfile=str(output), blfunc='variable', blparam=str(params),
                    blformat=formats, bloutput=files, **options) is None
    except RuntimeError as error:
        if (task is not casa.sdbaseline or not options.get('updateweight')
                or 'column WEIGHT_SPECTRUM SIGMA_SPECTRUM does not exist' not in str(error)):
            raise
        # CASA passes both column names as one after writing the science result.
        with opened(output, True) as main:
            assert not main.iscelldefined('WEIGHT_SPECTRUM', 0)
            assert not main.iscelldefined('SIGMA_SPECTRUM', 0)
            main.removecols(['WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'])
    return output, table


def fit_tool(factory, source, params, prefix, **options):
    output, table = prefix.with_suffix('.ms'), prefix.with_suffix('.bltable')
    tool = factory()
    try:
        assert tool.open(str(source)) is True
        assert tool.set_selection(spw='*') is True
        assert tool.subtract_baseline_variable(outfile=str(output), blparam=str(params),
                                               bloutput=f',,{table}', spw='*', **options) is True
    finally:
        assert tool.close() is True
    return output, table


def check_mixed(product, table, spectra, flags):
    x = np.arange(96)
    phase = x * (2 * np.pi / 95)
    bases = [np.column_stack((np.ones_like(x), x, x**2)),
             np.column_stack((np.ones_like(x), np.sin(3 * phase), np.cos(3 * phase),
                              np.sin(7 * phase), np.cos(7 * phase)))]
    ranges = [[5, 24, 26, 39, 45, 90], [3, 80, 0, 0, 0, 0]]
    p0 = np.r_[5:25, 26:40, 45:91]
    p1 = np.r_[3:81]
    terms = [np.linalg.lstsq(matrix[mask], spectrum[mask], rcond=None)[0]
             for matrix, mask, spectrum in zip(bases, [p0, p1], spectra)]
    with opened(table) as models:
        assert models.nrows() == 1
        np.testing.assert_array_equal(models.getcell('APPLY', 0), True)
        np.testing.assert_array_equal(models.getcell('FUNC_TYPE', 0), [[0], [3]])
        np.testing.assert_array_equal(models.getcell('FUNC_PARAM', 0), [[2, 0, 0], [0, 3, 7]])
        np.testing.assert_array_equal(models.getcell('MASKLIST', 0), ranges)
        np.testing.assert_array_equal(models.getcell('CLIP_ITERATION', 0), [[1], [0]])
        np.testing.assert_array_equal(models.getcell('LF_THRESHOLD', 0), 5.)
        stored = models.getcell('RESULT', 0)
        np.testing.assert_allclose(stored[0], np.r_[terms[0], 0., 0.], atol=2e-6)
        np.testing.assert_allclose(stored[1], terms[1], atol=2e-6)
    with opened(product) as main:
        expected = np.array([values - basis @ coefficients for values, basis, coefficients
                             in zip(spectra, bases, terms)])
        np.testing.assert_allclose(main.getcell('DATA', 0), expected, atol=2e-5)
        np.testing.assert_array_equal(main.getcell('FLAG', 0), flags)
        weight_masks = [np.r_[5:40, 45:91], p1]
        weight = [1 / np.mean(residual[mask] ** 2)
                  for residual, mask in zip(expected, weight_masks)]
        np.testing.assert_allclose(main.getcell('WEIGHT', 0), weight, rtol=5e-6)


def check_native_verbose(root, source, params):
    binary = os.environ.get('CASARS_CALIBRATE_BIN', 'calibrate')
    arguments = [binary, 'sdbaseline', '--infile', str(source), '--blfunc', 'variable',
                 '--blparam', str(params), '--blformat=', '--no-save-last']
    env = os.environ | {'CASA_RS_LOG_STDERR_PRIORITY': 'INFO'}
    quiet = subprocess.run([*arguments, '--outfile', str(root / 'quiet.ms')],
                           env=env, capture_output=True, text=True, check=True)
    verbose = subprocess.run([*arguments, '--outfile', str(root / 'verbose.ms'), '--verbose'],
                             env=env, capture_output=True, text=True, check=True)
    assert 'Fitting Parameter' not in quiet.stderr
    assert 'poly order=2' in verbose.stderr
    assert 'sinusoid nwave=[0, 3, 7]' in verbose.stderr


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    full = spectral_map(root, (96, 96))
    source = one_spectrum(full, root / 'one.ms')
    spectra, flags = set_spectra(source)
    tasks = {'casa': casa.sdbaseline}
    tools = {'casa': singledishms}
    if not args.reference_only:
        from casars.tasks import sdbaseline as native_task
        from casars.tools import singledishms as native_tool
        tasks['native'], tools['native'] = native_task, native_tool

    mixed = write_parameters(
        root, 'mixed', '0,0,5~39;45~90,1,3,false,,,,,poly,2,,',
        '0,1,3~80,0,,false,,,,,sinusoid,,,[7,0,3,3]',
    )
    products = {}
    for name, task in tasks.items():
        prefix = root / f'mixed-{name}'
        fit, table = fit_task(task, source, mixed, prefix, reports=True, verbose=True,
                              updateweight=True, sigmavalue='rms')
        check_mixed(fit, table, spectra, flags)
        products[name] = fit, table
    if 'native' in products:
        compare_outputs(products['casa'][0], products['native'][0])
        compare_baselines(products['casa'][1], products['native'][1])
        compare_reports(root / 'mixed-casa', root / 'mixed-native')
        check_native_verbose(root, source, mixed)

    for producer, (fit, table) in products.items():
        for reader, factory in tools.items():
            applied = root / f'apply-{producer}-{reader}.ms'
            tool_apply(factory, source, table, applied, updateweight=True, sigmavalue='rms')
            check_application(fit, applied)
        if 'native' in tools:
            compare_outputs(root / f'apply-{producer}-casa.ms', root / f'apply-{producer}-native.ms')
    print('mixed polynomial and sinusoid, clipping, reports and both baseline-table readers', flush=True)

    spline = write_parameters(root, 'spline', '0,0,2~80,0,,false,,,,,cspline,,2,',
                              '0,1,,0,,true,9,2,3,1,chebyshev,1,,')
    direct = {}
    for name, factory in tools.items():
        output, table = fit_tool(factory, source, spline, root / f'direct-{name}', verbose=True)
        with opened(table) as models:
            assert models.nrows() == 1
            np.testing.assert_array_equal(models.getcell('FUNC_TYPE', 0), [[2], [1]])
            np.testing.assert_array_equal(models.getcell('FUNC_FPARAM', 0), [[0, 42, 96], [0, 0, 0]])
            np.testing.assert_array_equal(models.getcell('USE_LF', 0), [[False], [True]])
            np.testing.assert_array_equal(models.getcell('LF_EDGE', 0), [[0, 0], [2, 3]])
            np.testing.assert_array_equal(models.getcell('LF_THRESHOLD', 0), [[5.], [9.]])
            np.testing.assert_array_equal(models.getcell('LF_AVERAGE', 0), [[0], [1]])
        direct[name] = output, table
    if 'native' in direct:
        compare_outputs(direct['casa'][0], direct['native'][0])
        compare_baselines(direct['casa'][1], direct['native'][1])
    print('mixed spline and Chebyshev with per-spectrum stored line controls', flush=True)

    one_pol = write_parameters(root, 'one-pol', '0,1,,0,,false,,,,,poly,0,,')
    wrong = write_parameters(root, 'no-match', '1,1,,0,,false,,,,,poly,0,,')
    yy = write_parameters(root, 'yy', '0,0,,0,,false,,,,,poly,0,,',
                          '0,1,,0,,false,,,,,poly,1,,')
    for name, task in tasks.items():
        for case, params, expected_models, options in (
                ('one-pol', one_pol, 1, {}),
                ('no-match', wrong, 0, {}),
                ('yy', yy, 1, {'pol': 'YY'}),
                ('yy-missing', one_pol, 0, {'pol': 'YY'}),
        ):
            output, table = fit_task(task, source, params, root / f'{case}-{name}', **options)
            with opened(output) as main, opened(table) as models:
                assert main.nrows() == 1 and models.nrows() == expected_models
                output_flags = main.getcell('FLAG', 0)
                if case == 'one-pol':
                    np.testing.assert_array_equal(output_flags[0], True)
                    np.testing.assert_array_equal(output_flags[1], flags[1])
                    np.testing.assert_array_equal(models.getcell('APPLY', 0), [[False], [True]])
                elif expected_models == 0:
                    np.testing.assert_array_equal(output_flags, True)
                else:
                    np.testing.assert_array_equal(output_flags, flags[1:])
                    np.testing.assert_array_equal(models.getcell('FUNC_PARAM', 0), [[0]])
    print('skipped spectra, unmatched rows and selected YY polarization', flush=True)

    set_spectra(full)
    with opened(full, True) as main:
        eligible = np.flatnonzero(main.getcol('ANTENNA1') == main.getcol('ANTENNA2'))
        descriptions = main.getcol('DATA_DESC_ID')
        first = int(next(row for row in eligible[20:] if descriptions[row] == 1))
        second = int(next(row for row in eligible if row > first and descriptions[row] == 0))
        timestamps = [main.getcell('TIME', row) for row in (first, second)]
        assert len(set(timestamps)) == 2
        for row in (first, second):
            main.putcell('SCAN_NUMBER', row, 1100)
    selected = write_parameters(root, 'selected', '0,0,,0,,false,,,,,poly,0,,',
                                '1,0,,0,,false,,,,,poly,1,,')
    physical = write_parameters(root, 'physical', f'{first},0,,0,,false,,,,,poly,0,,',
                                f'{second},0,,0,,false,,,,,poly,1,,')
    for name, task in tasks.items():
        output, table = fit_task(task, full, selected, root / f'selected-{name}',
                                 scan='1100', pol='XX')
        with opened(output) as main, opened(table) as models:
            assert main.nrows() == models.nrows() == 2
            expected = dict(zip(timestamps, [0, 1]))
            for row in range(models.nrows()):
                np.testing.assert_array_equal(models.getcell('APPLY', row), True)
                model = expected[models.getcell('TIME', row)]
                np.testing.assert_array_equal(models.getcell('FUNC_PARAM', row), [[model]])
                assert int(models.getcell('SCANNO', row)) == 1100
        output, table = fit_task(task, full, physical, root / f'physical-{name}',
                                 scan='1100', pol='XX')
        with opened(output) as main, opened(table) as models:
            assert main.nrows() == 2 and models.nrows() == 0
            for row in range(2):
                np.testing.assert_array_equal(main.getcell('FLAG', row), True)
    print('variable file row numbers follow the selected MAIN view before SPW iteration', flush=True)

    invalid = [
        ('0,0,,,,false,,,,,poly,0,,', 'Number of maximum clip iteration is mandatory'),
        ('0,0,,0,,false,,,,,cspline,,,', "requires num_piece value"),
        ('0,0,,0,,false,,,,,sinusoid,,,[]', 'Incorrect format or empty nwave list'),
    ]
    for name, task in tasks.items():
        for index, (row, message) in enumerate(invalid):
            poorly_formed = write_parameters(root, f'invalid-{index}-{name}', row)
            expected_error(lambda: fit_task(task, source, poorly_formed,
                                            root / f'invalid-fit-{index}-{name}'),
                           RuntimeError, message)
    print('variable parameter file errors', flush=True)

    nominal = write_parameters(root, 'nominal',
                               *(f'{row},{pol},,0,,false,,,,,poly,2,,'
                                 for row in range(len(eligible)) for pol in range(2)))
    for name, task in tasks.items():
        started = time.perf_counter()
        output, table = fit_task(task, full, nominal, root / f'variable-timing-{name}')
        with opened(output) as main, opened(table) as models:
            assert main.nrows() == models.nrows() == len(eligible)
        print(f'{name} matched variable baseline MS and table wall {time.perf_counter()-started:.3f}s',
              flush=True)
    print('variable baseline files passed', flush=True)


if __name__ == '__main__':
    main()
