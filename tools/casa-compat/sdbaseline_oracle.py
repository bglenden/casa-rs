#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired polynomial baselines and cross-reader table application."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import time

import casatasks as casa
from casatools import singledishms
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from sdsmooth_oracle import compare_outputs, floating_data, spectral_map


def without_flags(root, source):
    result = root / 'unflagged.ms'
    copy_ms(source, result)
    with opened(result, True) as table:
        for row in range(table.nrows()):
            table.putcell('FLAG', row, np.zeros_like(table.getcell('FLAG', row)))
    return result


def tool_fit(factory, source, ms_output, baseline, family, *, subtract=True,
             mask='*', order=1, **options):
    tool = factory()
    try:
        assert tool.open(str(source)) is True
        assert tool.set_selection(spw='*') is True
        assert tool.subtract_baseline(
            outfile=str(ms_output), bloutput=f',,{baseline}', dosubtract=subtract, spw=mask,
            order=order, blfunc=family, **options,
        ) is True
    finally:
        assert tool.close() is True


def tool_apply(factory, source, baseline, output, **weights):
    tool = factory()
    try:
        assert tool.open(str(source)) is True
        assert tool.set_selection(spw='*') is True
        assert tool.apply_baseline_table(str(baseline), spw='*', outfile=str(output),
                                         **weights) is True
    finally:
        assert tool.close() is True


def check_tool_report_replacement(root, source, tools):
    previous = 'existing report contents\n'
    for name, factory in tools.items():
        prefix = root / f'tool-reports-{name}'
        csv_path, text_path = prefix.with_suffix('.csv'), prefix.with_suffix('.txt')
        csv_path.write_text(previous)
        text_path.write_text(previous)
        tool = factory()
        try:
            assert tool.open(str(source)) is True
            assert tool.set_selection(spw='*') is True
            assert tool.subtract_baseline(
                outfile=str(prefix.with_suffix('.ms')), spw='*', order=1,
                bloutput=f'{csv_path},{text_path},',
            ) is True
        finally:
            assert tool.close() is True
        assert len(csv_path.read_text().splitlines()) == 480
        assert not csv_path.read_text().startswith(previous)
        assert text_path.read_text().startswith(previous + 'Scan[')


def check_coefficients(source, calibration, family):
    with opened(source) as main, opened(calibration) as models:
        times = main.getcol('TIME')
        by_time = {time: index for index, time in enumerate(times)}
        assert models.nrows() == 240
        model_type = 0 if family == 'poly' else 1
        assert models.getkeyword('ApplyType') == 'BASELINE'
        assert models.info()['type'] == 'ApplyTable'
        assert models.getcolkeywords('TIME')['MEASINFO']['Ref'] == 'UTC'
        for row in range(models.nrows()):
            values = main.getcell('DATA', by_time[models.getcell('TIME', row)]).real
            npol, nchan = values.shape
            assert models.getcell('NCHAN', row) == nchan
            expected_type = np.full((npol, 1), model_type)
            np.testing.assert_array_equal(models.getcell('FUNC_TYPE', row), expected_type)
            if family == 'poly':
                x = np.arange(nchan)
                basis = np.polynomial.polynomial
            else:
                x = np.linspace(-1., 1., nchan)
                basis = np.polynomial.chebyshev
            expected = np.array([basis.polyfit(x, values[pol], 1)
                                 if family == 'poly' else basis.chebfit(x, values[pol], 1)
                                 for pol in range(npol)])
            np.testing.assert_allclose(models.getcell('RESULT', row), expected, rtol=2e-6, atol=1e-5)
            np.testing.assert_array_equal(models.getcell('MASKLIST', row),
                                          np.tile([0, nchan - 1], (npol, 1)))


def compare_baselines(reference, native):
    with opened(reference) as expected, opened(native) as actual:
        assert actual.nrows() == expected.nrows()
        for path, opened_table in ((reference, expected), (native, actual)):
            assert opened_table.getkeyword('FREQUENCIES') == f'Table: {path / "SPECTRAL_WINDOW"}'
        for name in ('SCANNO', 'BEAMNO', 'ANTNO', 'IFNO', 'FREQ_ID', 'TIME', 'NCHAN'):
            np.testing.assert_array_equal(actual.getcol(name), expected.getcol(name), err_msg=name)
        for row in range(actual.nrows()):
            for name in ('APPLY', 'FUNC_TYPE', 'FUNC_PARAM', 'FUNC_FPARAM', 'MASKLIST',
                         'RESULT', 'RMS', 'CLIP_THRESHOLD', 'CLIP_ITERATION', 'USE_LF',
                         'LF_THRESHOLD', 'LF_AVERAGE', 'LF_EDGE'):
                np.testing.assert_allclose(actual.getcell(name, row), expected.getcell(name, row),
                                           rtol=2e-6, atol=2e-5, err_msg=f'{name} row {row}')
    with opened(reference / 'SPECTRAL_WINDOW') as expected, opened(native / 'SPECTRAL_WINDOW') as actual:
        assert actual.nrows() == expected.nrows()
        for row in range(actual.nrows()):
            for name in ('CHAN_FREQ', 'CHAN_WIDTH', 'NUM_CHAN'):
                np.testing.assert_array_equal(actual.getcell(name, row), expected.getcell(name, row))


def check_application(fit, applied):
    with opened(fit) as reference, opened(applied) as result:
        np.testing.assert_array_equal(result.getcol('TIME'), reference.getcol('TIME'))
        for row in range(reference.nrows()):
            np.testing.assert_allclose(result.getcell('DATA', row), reference.getcell('DATA', row),
                                       atol=2e-5, rtol=2e-6)
            np.testing.assert_array_equal(result.getcell('FLAG', row), reference.getcell('FLAG', row))


def compare_reports(reference, native):
    with reference.with_suffix('.csv').open() as left, native.with_suffix('.csv').open() as right:
        expected, actual = list(csv.reader(left)), list(csv.reader(right))
    assert len(actual) == len(expected)
    for left, right in zip(expected, actual):
        assert left[:8] == right[:8], (left, right)
        np.testing.assert_allclose([float(a) for a in left[8:-1]],
                                   [float(a) for a in right[8:-1]], atol=1e-5, rtol=2e-6)
        assert left[-1] == right[-1]
    numerical = re.compile(r'(?<== )-?\d+(?:\.\d+)?(?:e[-+]\d+)?')
    left = reference.with_suffix('.txt').read_text().replace(str(reference), '/report')
    right = native.with_suffix('.txt').read_text().replace(str(native), '/report')
    assert numerical.sub('<number>', left) == numerical.sub('<number>', right)


def clipping_fixture(root, source):
    vis = root / 'clip-input.ms'
    copy_ms(source, vis)
    with opened(vis, True) as main:
        for row in range(main.nrows()):
            data = main.getcell('DATA', row)
            data[:] = np.array([[3 + 2j], [8 + 2j]])
            data[:, 0] += 80
            main.putcell('DATA', row, data)
    return vis


def check_clipping_endpoint(root, source, tools):
    vis = root / 'endpoint-input.ms'
    copy_ms(source, vis)
    with opened(vis, True) as main:
        for row in range(main.nrows()):
            data = main.getcell('DATA', row)
            data[:, ::2] = -1
            data[:, 1::2] = 1
            main.putcell('DATA', row, data)
    for name, factory in tools.items():
        output = root / f'endpoint-{name}.ms'
        models = root / f'endpoint-{name}.bltable'
        tool_fit(factory, vis, output, models, 'poly', order=0, num_fitting_max=2,
                 clip_threshold_sigma=0.5, updateweight=True)
        with opened(output) as ms, opened(models) as table:
            assert ms.nrows() == table.nrows() == 240
            for row in range(ms.nrows()):
                data = ms.getcell('DATA', row)
                nchan = data.shape[1]
                np.testing.assert_array_equal(data[:, ::2], -2)
                np.testing.assert_array_equal(data[:, 1::2], 0)
                np.testing.assert_array_equal(ms.getcell('FLAG', row), False)
                np.testing.assert_array_equal(ms.getcell('WEIGHT', row), 0)
                np.testing.assert_array_equal(table.getcell('RESULT', row), 1)
                np.testing.assert_array_equal(table.getcell('MASKLIST', row),
                                              np.tile([nchan - 1] * 2, (2, 1)))
        try:
            tool_fit(factory, vis, root / f'bad-refit-{name}.ms',
                     root / f'bad-refit-{name}.bltable', 'poly', order=1,
                     num_fitting_max=2, clip_threshold_sigma=0.1)
        except RuntimeError as error:
            assert 'sakura_LSQFitPolynomialFloat() failed -- NG' in str(error)
        else:
            raise AssertionError('accepted an underdetermined clipped refit')


def check_direct_mask_fallback(root, different_widths, tools):
    equal_root = root / 'equal-windows'
    equal_root.mkdir()
    source = without_flags(equal_root, spectral_map(equal_root, (6, 6)))
    for name, factory in tools.items():
        model = root / f'fallback-{name}.bltable'
        tool_fit(factory, source, root / f'fallback-{name}.ms', model, 'poly',
                 order=0, mask='0:1~2')
        with opened(model) as table:
            assert 1 in table.getcol('IFNO')
            for row in range(table.nrows()):
                np.testing.assert_array_equal(table.getcell('MASKLIST', row), [[1, 2], [1, 2]])
        try:
            tool_fit(factory, different_widths, root / f'bad-fallback-{name}.ms',
                     root / f'bad-fallback-{name}.bltable', 'poly', order=0, mask='0:1~2')
        except RuntimeError as error:
            assert 'sakura_LSQFitPolynomialFloat() failed -- InvalidArgument' in str(error)
        else:
            raise AssertionError('used an incompatible fallback fit mask')
    if 'native' in tools:
        compare_baselines(root / 'fallback-casa.bltable', root / 'fallback-native.bltable')


def check_clipping(product, table):
    with opened(product) as main, opened(table) as models:
        assert main.nrows() == models.nrows() == 240
        for row in range(main.nrows()):
            nchan = models.getcell('NCHAN', row)
            expected = np.zeros((2, nchan))
            expected[:, 0] = 80
            np.testing.assert_allclose(main.getcell('DATA', row), expected, atol=1e-5)
            np.testing.assert_array_equal(models.getcell('MASKLIST', row),
                                          np.tile([1, nchan - 1], (2, 1)))
            np.testing.assert_allclose(models.getcell('RESULT', row), [[3], [8]], atol=1e-5)
            np.testing.assert_array_equal(models.getcell('CLIP_ITERATION', row), [[1], [1]])


def check_task_errors(task, source, prefix):
    output = prefix.with_suffix('.error.ms')
    cases = [
        (RuntimeError, 'order must be positive or zero.', {'order': -1}),
        (RuntimeError, 'sakura_CreateLSQFitContextFloat() failed -- InvalidArgument', {'order': 12}),
        (RuntimeError, 'sakura_LSQFitPolynomialFloat() failed -- InvalidArgument',
         {'order': 1, 'clipthresh': 0}),
        (ValueError, 'file specified in bltable', {'blmode': 'apply', 'bltable': str(output) + '.missing'}),
        (ValueError, 'blformat and bloutput must have the same length.', {'bloutput': ['a', 'b']}),
        (ValueError, 'blformat must be specified when dosubtract is False',
         {'blformat': '', 'dosubtract': False}),
    ]
    for index, (kind, message, options) in enumerate(cases):
        try:
            task(str(source), outfile=str(output) + str(index), **({'blformat': ''} | options))
        except kind as error:
            assert message in str(error), str(error)
        else:
            raise AssertionError(f'accepted invalid baseline parameters: {options}')


def check_apply_weights(root, source, tools):
    shifted = root / 'offset-input.ms'
    copy_ms(source, shifted)
    with opened(shifted, True) as main:
        for row in range(main.nrows()):
            data = main.getcell('DATA', row)
            main.putcell('DATA', row, data + 4)
    for sigma in ('stddev', 'rms'):
        products = {}
        for name, factory in tools.items():
            output = root / f'weights-{name}-{sigma}.ms'
            tool_apply(factory, shifted, root / 'poly-casa.bltable', output,
                       updateweight=True, sigmavalue=sigma)
            products[name] = output
            with opened(output) as main:
                values = main.getcell('DATA', 0).real
                expected = 1. / (values[0].var() if sigma == 'stddev' else np.mean(values[0] ** 2))
                np.testing.assert_allclose(main.getcell('WEIGHT', 0)[0], expected, rtol=2e-6)
                assert not main.iscelldefined('WEIGHT_SPECTRUM', 0)
                assert not main.iscelldefined('SIGMA_SPECTRUM', 0)
        if 'native' in products:
            compare_outputs(products['casa'], products['native'])


def check_apply_table_edges(root, source, tools):
    original = root / 'poly-casa.bltable'
    padded = root / 'padded-mask.bltable'
    copy_ms(original, padded)
    with opened(padded, True) as table:
        table.putcell('FUNC_PARAM', 0, [[0], [0]])
        table.putcell('RESULT', 0, [[0.], [0.]])
        table.putcell('MASKLIST', 0, [[2, 5, 0, 0], [1, 2, 4, 5]])
    for name, factory in tools.items():
        output = root / f'padded-apply-{name}.ms'
        tool_apply(factory, source, padded, output, updateweight=True)
        with opened(output) as main:
            np.testing.assert_allclose(main.getcell('WEIGHT', 0), [.8, .4], rtol=2e-6)

    overlapping = root / 'overlapping-times.bltable'
    copy_ms(original, overlapping)
    with opened(overlapping, True) as table:
        table.removerows(list(range(2, table.nrows())))
        instant = table.getcell('TIME', 0)
        for column in table.colnames():
            table.putcell(column, 1, table.getcell(column, 0))
        for row, (offset, coefficient) in enumerate([(-.25, 1.), (0., 9.)]):
            table.putcell('TIME', row, instant + offset)
            table.putcell('FUNC_PARAM', row, [[0], [0]])
            table.putcell('RESULT', row, [[coefficient], [coefficient]])
    with opened(source) as main:
        row = np.flatnonzero(main.getcol('TIME') == instant)[0]
        expected = main.getcell('DATA', int(row)).real - 1
    for name, factory in tools.items():
        output = root / f'overlap-time-apply-{name}.ms'
        tool_apply(factory, source, overlapping, output)
        with opened(output) as main:
            assert main.getcell('TIME', 0) == instant
            np.testing.assert_array_equal(main.getcell('DATA', 0), expected)
        try:
            tool_apply(factory, source, root / 'model-only.ms',
                       root / f'wrong-kind-{name}.ms')
        except RuntimeError as error:
            assert 'Table column SCANNO is unknown' in str(error)
        else:
            raise AssertionError('accepted an empty MeasurementSet as a baseline table')

    if 'native' in tools:
        existing = root / 'protected-output.ms'
        copy_ms(source, existing)
        nested = existing / 'keep.bltable'
        copy_ms(original, nested)
        for output in (existing, nested / 'inside.ms'):
            try:
                tool_apply(tools['native'], source, nested, output)
            except RuntimeError as error:
                assert 'overlaps the input baseline table' in str(error)
            else:
                raise AssertionError('allowed output to overlap the baseline input')
            with opened(nested) as table:
                assert table.nrows() == 240


def check_default_products(task, source, prefix):
    infile = prefix.with_suffix('.default.ms')
    copy_ms(source, infile)
    output = Path(str(infile) + '_bs')
    diagnostic = Path(str(infile) + '_blparam.txt')
    assert task(str(infile), order=1) is None
    assert output.is_dir()
    assert f'Output File: {output}' in diagnostic.read_text()
    try:
        task(str(infile), order=1)
    except ValueError as error:
        assert f"outfile='{output}' exists" in str(error)
    else:
        raise AssertionError('replaced an existing default output without overwrite')
    assert task(str(infile), order=0, overwrite=True) is None
    assert 'Function = poly  order = 0' in diagnostic.read_text()
    assert 'Function = poly  order = 1' not in diagnostic.read_text()


def check_float_selection(root, source, tasks):
    floating = floating_data(root, source)
    with opened(floating) as main:
        times = main.getcol('TIME')
        lower = int(np.median(times))
        rows = (times > lower) & (main.getcol('DATA_DESC_ID') == 1)
        with opened(floating / 'STATE') as states:
            intent = 'CALIBRATE_PHASE#ON_SOURCE'
            state = list(states.getcol('OBS_MODE')).index(intent)
        expected = times[rows & (main.getcol('STATE_ID') == state)]
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    timestamp = (epoch + timedelta(seconds=lower)).strftime('%Y/%m/%d/%H:%M:%S')
    products = {}
    for name, task in tasks.items():
        output = root / f'float-selected-{name}.ms'
        assert task(str(floating), datacolumn='FLOAT_DATA', outfile=str(output),
                    blformat='', pol='YY', intent=intent, timerange=f'>{timestamp}',
                    spw='1', order=1) is None
        with opened(output) as main:
            assert main.nrows() > 0
            np.testing.assert_array_equal(main.getcol('TIME'), expected)
            assert main.getcell('FLOAT_DATA', 0).shape == (1, 4)
        products[name] = output
    if 'native' in products:
        compare_outputs(products['casa'], products['native'])


def task_checks(root, flagged, source, reference_only):
    tasks = {'casa': casa.sdbaseline}
    if not reference_only:
        from casars.tasks import sdbaseline
        tasks['native'] = sdbaseline
    check_float_selection(root, flagged, tasks)
    clipped = clipping_fixture(root, source)
    for name, task in tasks.items():
        prefix = root / f'public-{name}'
        output = prefix.with_suffix('.ms')
        table = prefix.with_suffix('.bltable')
        started = time.perf_counter()
        assert task(str(source), order=1, outfile=str(output),
                    blformat=['text', 'csv', 'table'],
                    bloutput=[str(prefix.with_suffix(suffix))
                              for suffix in ('.txt', '.csv', '.bltable')]) is None
        print(f'{name} sdbaseline 240 rows wall {time.perf_counter() - started:.3f}s', flush=True)
        assert task(str(source), order=1, outfile=str(prefix.with_suffix('.unused.ms')),
                    blformat='table', bloutput=str(prefix.with_suffix('.only.bltable')),
                    dosubtract=False) is None
        assert not prefix.with_suffix('.unused.ms').exists()
        check_coefficients(source, prefix.with_suffix('.only.bltable'), 'poly')
        with opened(output / 'HISTORY') as history:
            messages = [message for origin, message in zip(history.getcol('ORIGIN'),
                                                           history.getcol('MESSAGE'))
                        if origin == 'sdbaseline']
            assert len(messages) == 2
            assert messages[0] == 'taskname=sdbaseline'
            assert messages[1].startswith('version: ')
        assert task(str(source), blmode='APPLY', bltable=str(table),
                    outfile=str(prefix.with_suffix('.applied.ms'))) is None
        check_application(output, prefix.with_suffix('.applied.ms'))
        check_task_errors(task, source, prefix)
        check_default_products(task, source, prefix)
        clip_csv = prefix.with_suffix('.clip.csv')
        assert task(str(clipped), order=0, clipthresh=1.7, clipniter=1,
                    outfile=str(prefix.with_suffix('.clip.ms')), blformat=['table', 'csv'],
                    bloutput=[str(prefix.with_suffix('.clip.bltable')), str(clip_csv)]) is None
        check_clipping(prefix.with_suffix('.clip.ms'), prefix.with_suffix('.clip.bltable'))
        with clip_csv.open() as report:
            counts = [int(row[-1]) for row in csv.reader(report)]
            assert counts[:4] == ([1, 1, 2, 2] if name == 'casa' else [1, 1, 1, 1])
        for reindex in (False, True):
            result = root / f'selected-{name}-{reindex}.ms'
            try:
                assert task(str(flagged), datacolumn='CORRECTED', blfunc='CHEBYSHEV', order=0,
                            antenna='0', field='SelectedField', spw='1:0~2', scan='1',
                            reindex=reindex, outfile=str(result),
                            blformat='', updateweight=True, sigmavalue='rms') is None
            except RuntimeError as error:
                assert name == 'casa' and 'column WEIGHT_SPECTRUM SIGMA_SPECTRUM does not exist' in str(error)
                # The pinned wrapper passes two names as one to table.removecols.
                # Its earlier calculation succeeded. Remove the undefined columns
                # to check the resulting science product against the native task.
                with opened(result, True) as main:
                    assert not main.iscelldefined('WEIGHT_SPECTRUM', 0)
                    assert not main.iscelldefined('SIGMA_SPECTRUM', 0)
                    main.removecols(['WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'])
            with opened(result) as main:
                assert main.nrows() > 0
                assert main.getcell('DATA', 0).shape == (2, 4)
                assert 'SIGMA_SPECTRUM' not in main.colnames()
                assert 'WEIGHT_SPECTRUM' not in main.colnames()
    if not reference_only:
        compare_outputs(root / 'public-casa.ms', root / 'public-native.ms')
        compare_baselines(root / 'public-casa.bltable', root / 'public-native.bltable')
        compare_reports(root / 'public-casa', root / 'public-native')
        compare_baselines(root / 'public-casa.clip.bltable', root / 'public-native.clip.bltable')
        for reindex in (False, True):
            compare_outputs(root / f'selected-casa-{reindex}.ms', root / f'selected-native-{reindex}.ms')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--tool-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    flagged = spectral_map(root)
    source = without_flags(root, flagged)
    tools = {'casa': singledishms}
    if not args.reference_only:
        from casars.tools import singledishms as native_tool
        tools['native'] = native_tool
    for family in ('poly', 'chebyshev'):
        paths = {}
        for implementation, factory in tools.items():
            output = root / f'{family}-{implementation}.ms'
            baseline = root / f'{family}-{implementation}.bltable'
            tool_fit(factory, source, output, baseline, family)
            check_coefficients(source, baseline, family)
            paths[implementation] = output, baseline
        if 'native' in paths:
            compare_outputs(paths['casa'][0], paths['native'][0])
            compare_baselines(paths['casa'][1], paths['native'][1])
        for producer, (fit, baseline) in paths.items():
            for consumer, factory in tools.items():
                applied = root / f'{family}-{producer}-{consumer}-apply.ms'
                tool_apply(factory, source, baseline, applied)
                check_application(fit, applied)
    if 'native' in tools:
        baseline = root / 'singleton-native.bltable'
        output = root / 'singleton-native.ms'
        tool_fit(tools['native'], flagged, output, baseline, 'poly')
        applied = root / 'singleton-casa.ms'
        tool_apply(tools['casa'], flagged, baseline, applied)
        check_application(output, applied)
    no_ms = root / 'model-only.ms'
    table = root / 'model-only.bltable'
    tool_fit(tools['casa'], source, no_ms, table, 'poly', subtract=False)
    assert table.is_dir()
    with opened(no_ms) as skeleton:
        assert skeleton.nrows() == 0
    if 'native' in tools:
        native_skeleton = root / 'native-skeleton.ms'
        native_models = root / 'native-skeleton.bltable'
        tool_fit(tools['native'], source, native_skeleton, native_models, 'poly', subtract=False)
        compare_baselines(table, native_models)
        compare_outputs(no_ms, native_skeleton)
    if not args.tool_only:
        check_tool_report_replacement(root, source, tools)
        check_clipping_endpoint(root, source, tools)
        check_direct_mask_fallback(root, source, tools)
        check_apply_weights(root, source, tools)
        check_apply_table_edges(root, source, tools)
        task_checks(root, flagged, source, args.reference_only)
    print('Polynomial and Chebyshev baselines match and interchange.', flush=True)


if __name__ == '__main__':
    main()
