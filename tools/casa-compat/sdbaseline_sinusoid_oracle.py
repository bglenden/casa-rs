#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair explicit and FFT-assisted sinusoidal baselines with CASA 6.7.6.14."""

import argparse
from pathlib import Path
import time

import casatasks as casa
from casatools import singledishms
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from sdbaseline_mask_spline_oracle import one_spectrum
from sdbaseline_oracle import check_application, compare_baselines, compare_reports, tool_apply
from sdsmooth_oracle import compare_outputs, spectral_map


def write_spectra(path, varied=False):
    phase = np.arange(96) * (2 * np.pi / 95)
    ripple = 1.8 * np.sin(3 * phase) + .3 * np.cos(3 * phase)
    ripple += .7 * np.sin(7 * phase) - .6 * np.cos(7 * phase)
    second = 15 + .15 * np.sin(3 * phase) if varied else 6 + ripple
    spectra = np.array([5 + ripple, second], dtype=np.complex64)
    with opened(path, True) as main:
        for row in range(main.nrows()):
            main.putcell('DATA', row, spectra)
            main.putcell('FLAG', row, np.zeros_like(spectra, dtype=bool))
    return spectra.real


def check_coefficients(table, spectra, by_pol):
    phase = np.arange(96) * (2 * np.pi / 95)
    width = max(map(len, by_pol))
    parameters = [list(waves) + [0] * (width - len(waves)) for waves in by_pol]
    with opened(table) as models:
        assert models.nrows() > 0
        for row in range(models.nrows()):
            np.testing.assert_array_equal(models.getcell('FUNC_TYPE', row), 3)
            np.testing.assert_array_equal(models.getcell('FUNC_PARAM', row), parameters)
            assert models.getcell('FUNC_FPARAM', row).shape == (2, 0)
            stored = models.getcell('RESULT', row)
            for pol, waves in enumerate(by_pol):
                basis = np.column_stack([
                    column
                    for wave in waves
                    for column in (
                        [np.ones_like(phase)] if wave == 0
                        else [np.sin(wave * phase), np.cos(wave * phase)]
                    )
                ])
                expected, *_ = np.linalg.lstsq(basis, spectra[pol], rcond=None)
                np.testing.assert_allclose(stored[pol, :len(expected)], expected, atol=2e-6)
                np.testing.assert_array_equal(stored[pol, len(expected):], 0)


def fit_tool(factory, source, prefix, **options):
    table = prefix.with_suffix('.bltable')
    output = prefix.with_suffix('.ms')
    tool = factory()
    try:
        assert tool.open(str(source)) is True
        assert tool.set_selection(spw='*') is True
        assert tool.subtract_baseline_sinusoid(
            outfile=str(output), bloutput=f',,{table}', spw='*', **options,
        ) is True
    finally:
        assert tool.close() is True
    return output, table


def expected_error(action, error_type, message):
    try:
        action()
    except error_type as error:
        assert message in str(error), str(error)
    else:
        raise AssertionError(f'expected {error_type.__name__} containing {message!r}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    full = spectral_map(root, (96, 96))
    spectra = write_spectra(full)
    source = one_spectrum(full, root / 'one.ms')
    varied = one_spectrum(full, root / 'varied.ms')
    variable_spectra = write_spectra(varied, varied=True)
    tasks = {'casa': casa.sdbaseline}
    tools = {'casa': singledishms}
    if not args.reference_only:
        from casars.tasks import sdbaseline as native_task
        from casars.tools import singledishms as native_tool
        tasks['native'], tools['native'] = native_task, native_tool

    cases = [
        (dict(applyfft=False, addwn=[0, 3, 7]), [0, 3, 7]),
        (dict(applyfft=False, addwn='0,3,7', rejwn=7), [0, 3]),
        (dict(applyfft=False, addwn='>=3', rejwn='>=8'), [3, 4, 5, 6, 7]),
        (dict(applyfft=False, addwn='3-0', rejwn=[1, 2]), [0, 3]),
        (dict(applyfft=False, addwn='<=3', rejwn='2~1'), [0, 3]),
        (dict(applyfft=False, addwn='3', rejwn=[]), [3]),
        (dict(applyfft=False, addwn=[True, 3.0]), [0, 3]),
        (dict(applyfft=True, fftmethod='FFT', fftthresh='TOP3'), [0, 3, 7]),
        (dict(applyfft=True, fftthresh='2sigma'), [0]),
        (dict(applyfft=True, fftthresh=2.), [0]),
        (dict(applyfft=True, fftthresh=np.float32(2.)), [0]),
        (dict(applyfft=True, fftthresh=np.int64(2)), [0]),
        (dict(applyfft=True, fftthresh='1e-50sigma'), [0, 3, 7]),
        (dict(applyfft=False, addwn='3~0~1', rejwn=[1, 2]), [0, 3]),
        (dict(applyfft=True, fftthresh='top3', rejwn=[7]), [0, 3]),
    ]
    for index, (options, waves) in enumerate(cases):
        outputs = {}
        for name, task in tasks.items():
            prefix = root / f'case-{index}-{name}'
            table, output = prefix.with_suffix('.bltable'), prefix.with_suffix('.ms')
            assert task(str(source), outfile=str(output), blfunc='sinusoid',
                        dosubtract=False, blformat='table', bloutput=str(table),
                        **options) is None
            assert not output.exists()
            check_coefficients(table, spectra, [waves, waves])
            outputs[name] = table
        if 'native' in outputs:
            compare_baselines(outputs['casa'], outputs['native'])
        print(f'paired wavenumber case {index}: {waves}', flush=True)

    reports = {}
    padded_products = {}
    for name, task in tasks.items():
        prefix = reports[name] = root / f'fft-sigma-{name}'
        output, table = prefix.with_suffix('.ms'), prefix.with_suffix('.bltable')
        assert task(
            str(varied), outfile=str(output), blfunc='sinusoid', fftthresh='1SIGMA',
            blformat=['csv', 'text', 'table'],
            bloutput=[str(prefix.with_suffix(suffix)) for suffix in ('.csv', '.txt', '.bltable')],
        ) is None
        check_coefficients(table, variable_spectra, [[0, 3], [0]])
        padded_products[name] = (output, table)

    if 'native' in reports:
        compare_reports(reports['casa'], reports['native'])
        compare_baselines(padded_products['casa'][1], padded_products['native'][1])
        compare_outputs(padded_products['casa'][0], padded_products['native'][0])
    for producer, (fit, table) in padded_products.items():
        for consumer, factory in tools.items():
            output = root / f'padded-{producer}-to-{consumer}.ms'
            tool_apply(factory, varied, table, output)
            check_application(fit, output)
    print('paired padded polarizations, reports and cross-reader table application', flush=True)

    direct = {}
    for name, factory in tools.items():
        output, table = fit_tool(factory, source, root / f'direct-{name}',
                                 addwn='0,3,7', rejwn='8,-999',
                                 applyfft=True, fftthresh='top3')
        check_coefficients(table, spectra, [[0, 3, 7]] * 2)
        with opened(output) as vis:
            np.testing.assert_allclose(vis.getcell('DATA', 0), 0., atol=2e-6)
        direct[name] = (output, table)
    if 'native' in direct:
        compare_outputs(direct['casa'][0], direct['native'][0])
        compare_baselines(direct['casa'][1], direct['native'][1])

    errors = [
        (dict(addwn=True), ValueError, 'wrong value given for addwn/rejwn'),
        (dict(addwn=True, rejwn=''), ValueError, 'wrong value given for addwn/rejwn'),
        (dict(addwn=''), IndexError, 'string index out of range'),
        (dict(addwn=['3', -1]), ValueError, 'wrong value given for addwn/rejwn'),
        (dict(addwn=['x', 1]), ValueError, "invalid literal for int() with base 10: 'x'"),
        (dict(addwn='x'), ValueError, "invalid literal for int() with base 10: 'x'"),
        (dict(addwn=None, fftthresh=None, rejwn=None), AssertionError, str({
            'addwn': ['null value not allowed'],
            'fftthresh': ['null value not allowed'],
            'rejwn': ['null value not allowed'],
        })),
        (dict(addwn=[]), RuntimeError, 'addwn must contain at least one element.'),
        (dict(addwn='47', rejwn=[47]), RuntimeError, 'No effective wave number'),
        (dict(fftthresh=True), ValueError, 'fftthresh must be float or integer or string.'),
        (dict(fftthresh=0), ValueError, 'threshold given to fftthresh must be positive.'),
        (dict(fftthresh='top-1'), ValueError, 'threshold given to fftthresh must be positive.'),
        (dict(fftthresh='top-notanumber'), ValueError, 'fftthresh has a wrong format.'),
        (dict(applyfft=False, fftthresh='wrong'), ValueError, 'fftthresh has a wrong format.'),
        (dict(fftthresh='top3', addwn=0, rejwn='>=7'), RuntimeError, 'InvalidArgument'),
        (dict(fftthresh='1e0'), RuntimeError, 'fftthresh has a wrong value'),
        (dict(applyfft=False, addwn=[3.1, 3.3]), RuntimeError, 'InvalidArgument'),
        (dict(applyfft=False, addwn=['3', '12']), RuntimeError, 'InvalidArgument'),
        (dict(addwn=['3', 0]), TypeError, 'str'),
        (dict(addwn=(0, 3)), AssertionError, 'must be of cVariant type'),
        (dict(addwn=np.array([0, 3])), ValueError, 'wrong value given for addwn/rejwn'),
        (dict(applyfft=False, addwn='47~48', rejwn='>=55'), RuntimeError, 'CreateLSQFitContext'),
        (dict(applyfft=False, addwn=[47, 48], rejwn='>=55'), RuntimeError, 'CreateLSQFitContext'),
    ]
    for name, task in tasks.items():
        for index, (options, error_type, message) in enumerate(errors):
            output = root / f'invalid-{name}-{index}.ms'
            expected_error(lambda: task(str(source), outfile=str(output), blfunc='sinusoid',
                                        blformat='', **options), error_type, message)
            if name == 'native':
                assert not output.exists()
        occupied = root / f'occupied-{name}.ms'
        occupied.mkdir()
        sentinel = occupied / 'independent-file'
        sentinel.write_text('preserve this output')
        expected_error(lambda: task(str(source), outfile=str(occupied), blfunc='sinusoid',
                                    addwn='', blformat=''),
                       ValueError, 'exists, and cannot overwrite it.')
        assert sentinel.read_text() == 'preserve this output'
    for name, factory in tools.items():
        expected_error(lambda: fit_tool(factory, source, root / f'bad-method-{name}',
                                        applyfft=True, fftthresh='top3', fftmethod='bogus'),
                       RuntimeError, "fftmethod must be 'fft' for now.")
        expected_error(lambda: fit_tool(factory, source, root / f'bad-order-{name}',
                                        addwn='3,0'), RuntimeError, 'InvalidArgument')
        expected_error(lambda: fit_tool(factory, source, root / f'empty-fft-{name}',
                                        applyfft=True), RuntimeError, 'fftthresh has a wrong value')
        output, table = fit_tool(factory, source, root / f'numeric-array-{name}',
                                 applyfft=True, fftthresh=[2.0], addwn='nonnumeric')
        check_coefficients(table, spectra, [[0]] * 2)
        for index, (text, waves) in enumerate((('top2xyz', [0, 3]), ('2s', [0]))):
            _, table = fit_tool(factory, source, root / f'threshold-prefix-{name}-{index}',
                                applyfft=True, fftthresh=text)
            check_coefficients(table, spectra, [waves] * 2)
    print(f'paired task and direct-tool errors ({len(errors) + 3})', flush=True)

    clipped_source = one_spectrum(full, root / 'clipped.ms')
    with opened(clipped_source, True) as main:
        data = main.getcell('DATA', 0)
        data += np.random.default_rng(1532).normal(0, .01, size=data.shape)
        data[:, 25] += 10
        main.putcell('DATA', 0, data)
    clipped_products = {}
    for name, task in tasks.items():
        prefix = root / f'clipped-{name}'
        output, table = prefix.with_suffix('.ms'), prefix.with_suffix('.bltable')
        assert task(str(clipped_source), outfile=str(output), spw='*:5~86',
                    blfunc='sinusoid', addwn=[0, 3, 7], applyfft=False, clipthresh=3.,
                    clipniter=1, blformat='table', bloutput=str(table)) is None
        with opened(table) as models:
            np.testing.assert_array_equal(models.getcell('MASKLIST', 0), [[5, 24, 26, 86]] * 2)
            np.testing.assert_array_equal(models.getcell('CLIP_ITERATION', 0), 1)
        with opened(output) as result:
            residuals = result.getcell('DATA', 0).real
            np.testing.assert_allclose(residuals[:, 25], 10, atol=.05)
            np.testing.assert_allclose(residuals[:, [24, 26]], 0, atol=.05)
        clipped_products[name] = (output, table)
    if 'native' in clipped_products:
        compare_outputs(clipped_products['casa'][0], clipped_products['native'][0])
        compare_baselines(clipped_products['casa'][1], clipped_products['native'][1])
    print('paired clipping and selected-channel mask', flush=True)

    check_context_boundaries(root, full, source, spectra, direct, tasks, tools)

    for name, task in tasks.items():
        prefix = root / f'timing-{name}'
        start = time.perf_counter()
        assert task(str(full), outfile=str(prefix.with_suffix('.ms')), blfunc='sinusoid',
                    fftthresh='top3', blformat='table', bloutput=str(prefix.with_suffix('.bltable'))
                    ) is None
        print(f'{name} matched spectrum, MS and table elapsed {time.perf_counter()-start:.6f} s',
              flush=True)
    print('sinusoidal baselines passed', flush=True)


def check_context_boundaries(root, full, source, spectra, direct, tasks, tools):
    odd_root = root / 'odd'
    odd_root.mkdir()
    odd = one_spectrum(spectral_map(odd_root, (95, 95)), odd_root / 'one.ms')
    with opened(odd, True) as table:
        table.putcell('FLAG', 0, np.zeros((2, 95), dtype=bool))
    for name, task in tasks.items():
        def fit(wave):
            prefix = odd_root / f'wave-{wave}-{name}'
            return task(str(odd), outfile=str(prefix.with_suffix('.ms')), blfunc='sinusoid',
                        addwn=wave, rejwn='>=55', applyfft=False, dosubtract=False,
                        blformat='table', bloutput=str(prefix.with_suffix('.bltable')))

        assert fit(46) is None
        expected_error(lambda: fit(47), RuntimeError, 'CreateLSQFitContext')

    invalid = root / 'invalid-context.bltable'
    copy_ms(direct['casa'][1], invalid)
    with opened(invalid, True) as table:
        table.putcell('FUNC_PARAM', 0, np.tile([0, 3, 49], (2, 1)).astype('int32'))
    for name, factory in tools.items():
        expected_error(lambda: tool_apply(factory, source, invalid, root / f'bad-table-{name}.ms'),
                       RuntimeError, 'CreateLSQFitContext')

    sparse = root / 'sparse-first.ms'
    with opened(full) as table:
        rows = np.flatnonzero(table.getcol('ANTENNA1') == table.getcol('ANTENNA2'))[:2]
        selected = table.selectrows(rows.tolist())
        try:
            written = selected.copy(str(sparse), deep=True, valuecopy=True)
            written.close()
        finally:
            selected.close()
    with opened(sparse, True) as table:
        flags = np.ones((2, 96), dtype=bool)
        flags[:, :8] = False
        table.putcell('FLAG', 0, flags)
        table.putcell('DATA', 0, np.zeros((2, 96), dtype=np.complex64))
        table.putcell('DATA', 1, spectra.astype(np.complex64))

    products = {}
    for name, task in tasks.items():
        prefix = root / f'top6-sparse-{name}'
        output, table = prefix.with_suffix('.ms'), prefix.with_suffix('.bltable')
        assert task(str(sparse), outfile=str(output), blfunc='sinusoid', addwn=3, fftthresh='top6',
                    blformat='table', bloutput=str(table)) is None
        with opened(output) as main, opened(table) as models:
            assert main.nrows() == 2 and models.nrows() == 1
            np.testing.assert_array_equal(main.getcell('FLAG', 0), True)
            np.testing.assert_array_equal(main.getcell('FLAG', 1), False)
        products[name] = (output, table)
    if 'native' in products:
        compare_outputs(products['casa'][0], products['native'][0])
        compare_baselines(products['casa'][1], products['native'][1])
    print('paired 95/96-channel context bounds, invalid table and sparse FFT skip', flush=True)


if __name__ == '__main__':
    main()
