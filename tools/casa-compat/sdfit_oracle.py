#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair single-dish line fits, uncertainties, and tool products with CASA 6.7.6.14."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time

import casatasks as casa
from casatools import singledishms
import numpy as np
from scipy.optimize import curve_fit

from calibration_smoothcal_oracle import opened
from sdsmooth_oracle import floating_data, spectral_map


def gaussian(x, amplitude, center, width):
    return amplitude * np.exp(-4.0 * np.log(2.0) * ((x - center) / width) ** 2)


def lorentzian(x, amplitude, center, width):
    return amplitude / (1 + 4 * ((x - center) / width) ** 2)


def line_map(root):
    path = spectral_map(root, (96, 96))
    xx = np.arange(96)
    noise = np.random.default_rng(4817).normal(0, .013, size=(2, xx.size))
    first = np.stack([gaussian(xx, 6, 27.3, 7.8), gaussian(xx, -4, 26.1, 9.5)])
    second = np.stack([gaussian(xx, -3.5, 66.2, 9.6), gaussian(xx, 5, 66.7, 7.2)])
    data = (first + second + noise).astype(np.float32)
    corrected = np.stack([lorentzian(xx, -2.4, 49.7, 8.8),
                          lorentzian(xx, 4.5, 52.2, 10.1)]) + noise
    flags = np.zeros(data.shape, bool)
    flags[0, [10, 12, 26, 88]] = True
    flags[1, [13, 39, 82, 88]] = True
    with opened(path, True) as table:
        for row in range(table.nrows()):
            table.putcell('DATA', row, data.astype(np.complex64) + 4j)
            table.putcell('CORRECTED_DATA', row, corrected.astype(np.complex64) - 3j)
            table.putcell('FLAG', row, flags if row != 1 else np.ones_like(flags))
    return path, data, flags


def compare_fits(reference, result, *, rtol=2e-4, atol=4e-5):
    assert list(result) == ['cent', 'peak', 'fwhm', 'nfit']
    assert result['nfit'] == reference['nfit']
    for name in ('cent', 'peak', 'fwhm'):
        for expect, actual in zip(reference[name], result[name], strict=True):
            np.testing.assert_allclose(actual, expect, rtol=rtol, atol=atol, err_msg=name)


def compare_reports(reference, result):
    left, right = reference.read_text().splitlines(), result.read_text().splitlines()
    assert left[0] == right[0]
    for expect, actual in zip(left[1:], right[1:], strict=True):
        a, b = expect.split('\t'), actual.split('\t')
        assert a[:8] == b[:8], (a[:8], b[:8])
        np.testing.assert_allclose(np.array(a[8:11], float), np.array(b[8:11], float),
                                   rtol=2e-4, atol=4e-5)


def task_pair(root, label, source, tasks, *, report=False, **kw):
    results, products = {}, {}
    for name, task in tasks.items():
        path = root / f'{label}-{name}.txt'
        if report:
            kw['outfile'] = str(path)
        start = time.perf_counter()
        results[name] = task(str(source), **kw)
        elapsed = time.perf_counter() - start
        print(f'{label}: {name} {elapsed:.4f}s, {len(results[name]["nfit"])} spectra', flush=True)
        if report:
            products[name] = path
    if 'native' in results:
        compare_fits(results['casa'], results['native'])
        if report:
            compare_reports(products['casa'], products['native'])
    return results, products


def check_profiles(root, source, data, flags, tasks):
    zero, _ = task_pair(root, 'defaults', source, tasks)
    assert zero['casa']['nfit'] and set(zero['casa']['nfit']) == {0}
    ranges = '1:8~44;45~90'
    pair, _ = task_pair(root, 'separate-ranges', source, tasks, spw=ranges, nfit=[1, 1], report=True)
    assert set(pair['casa']['nfit']) == {2}
    comp, _ = task_pair(root, 'joint-components', source, tasks, spw='0:8~90', nfit=2)
    assert set(comp['casa']['nfit']) == {2}
    model = lambda x, *p: gaussian(x, *p[:3]) + gaussian(x, *p[3:])
    samples = np.arange(96)
    for pol, estimates in enumerate(([6, 27, 8, -3.5, 66, 10], [-4, 26, 9, 5, 67, 7])):
        available = (~flags[pol]) & (8 <= samples) & (samples <= 90)
        fit, covariance = curve_fit(model, samples[available], data[pol, available], p0=estimates)
        expected = np.column_stack((fit, np.sqrt(np.diag(covariance)))).reshape(2, 3, 2)
        for result in comp.values():
            actual = np.swapaxes([result['peak'][pol], result['cent'][pol], result['fwhm'][pol]], 0, 1)
            np.testing.assert_allclose(actual, expected, rtol=2e-4, atol=4e-5)
    corrected, _ = task_pair(root, 'lorentzian', source, tasks, datacolumn='CORRECTED',
                             spw='0:14~81^2', pol='YY', fitfunc='lorentzian', nfit=[1], report=True)
    np.testing.assert_allclose([corrected['casa'][k][0][0][0] for k in ('peak','cent','fwhm')],
                               [4.5, 52.2, 10.1], atol=.08)
    floating = floating_data(root, source)
    task_pair(root, 'floating', floating, tasks, datacolumn='float_data', spw=ranges, nfit=[1, 1])
    task_pair(root, 'empty-detection', source, tasks, fitmode='auto', spw='1', thresh=1e7)
    with opened(source) as table:
        lower = int(np.median(table.getcol('TIME')))
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    timestamp = (epoch + timedelta(seconds=lower)).strftime('%Y/%m/%d/%H:%M:%S')
    auto, _ = task_pair(root, 'selected-auto', source, tasks, fitmode='auto', nfit=[88],
                        antenna='0&1', field='SelectedField', spw='1:8~90',
                        timerange=f'>{timestamp}', scan='2', pol='YY',
                        intent='CALIBRATE_PHASE#ON_SOURCE', thresh=5, avg_limit=4,
                        minwidth=4, edge=[3, 5], report=True)
    assert auto['casa']['nfit'] and set(auto['casa']['nfit']) == {2}


def timed_spectra(root, source):
    selected = root / 'time-polarization.ms'
    with opened(source) as table:
        rows = table.selectrows([1, 3, 5, 7])
        try:
            copied = rows.copy(str(selected), deep=True, valuecopy=True)
            copied.close()
        finally:
            rows.close()
    with opened(selected, True) as table:
        start = np.floor(table.getcell('TIME', 0))
        for row, offset in enumerate((0., 2., 4., 6.)):
            table.putcell('TIME', row, start + offset)
            table.putcell('TIME_CENTROID', row, start + offset)
            table.putcell('SCAN_NUMBER', row, 1 + row // 2)
            table.putcell('FIELD_ID', row, 0)
            table.putcell('STATE_ID', row, 0)
            table.putcell('INTERVAL', row, 2.)
            table.putcell('EXPOSURE', row, 2.)
            spectral_weight = np.repeat(np.array([[1.], [4.]], dtype=np.float32), 96, axis=1)
            table.putcell('WEIGHT_SPECTRUM', row, spectral_weight)
            table.putcell('SIGMA_SPECTRUM', row, 1. / np.sqrt(spectral_weight))
            table.putcell('WEIGHT', row, np.array([2., 2.], dtype=np.float32))
            table.putcell('SIGMA', row, np.array([1., 1.], dtype=np.float32))
            data = gaussian(np.arange(96), 1., 43., 8.)
            profiles = np.stack([(2. + row) * data, (8. + 2. * row) * data])
            table.putcell('DATA', row, profiles.astype(np.complex64))
            flags = np.zeros((2, 96), bool)
            flags[0, 34] = True
            table.putcell('FLAG', row, flags)
    return selected


def copy_rows(source, destination, indices):
    with opened(source) as table:
        selected = table.selectrows(np.asarray(indices, dtype=int).tolist())
        try:
            copied = selected.copy(str(destination), deep=True, valuecopy=True)
            copied.close()
        finally:
            selected.close()
    return destination


def check_time_bins(root, spectra, source, tasks):
    for interval, count in (('1s', 8), ('3s', 4), ('8s', 2)):
        linear, reports = task_pair(root, f'bins-{interval}', spectra, tasks,
                                    timebin=interval, timespan='scan', nfit=[1], report=True)
        assert len(linear['casa']['nfit']) == count
        if interval == '1s':
            one_second, ascending_report = linear['casa'], reports['casa']
    reverse = copy_rows(spectra, root / 'reverse-time.ms', [3, 2, 1, 0])
    kwargs = dict(timebin='1s', timespan='scan', nfit=[1])
    # The installed iterator combines descending physical rows across the requested bin.
    assert len(tasks['casa'](str(reverse), **kwargs)['nfit']) == 2
    if 'native' in tasks:
        output = root / 'reverse-time-native.txt'
        result = tasks['native'](str(reverse), outfile=str(output), **kwargs)
        compare_fits(one_second, result)
        compare_reports(ascending_report, output)

    with opened(source) as table:
        indices = np.argsort(table.getcol('TIME'))[:40]
    grouped = copy_rows(source, root / 'chronological-two-window.ms', indices)
    for span in ('', 'scan,state,field'):
        task_pair(root, f'grouped-time-{span or "default"}', grouped, tasks,
                  timebin='40s', timespan=span, nfit=2, report=True)


def check_degenerate_ranges(root, spectra, tasks):
    task_pair(root, 'single-channel', spectra, tasks, spw='1:43', nfit=1)
    task_pair(root, 'three-channels', spectra, tasks, spw='1:43~45', nfit=1)
    task_pair(root, 'masked-range', spectra, tasks, spw='1:34', pol='XX', nfit=1)
    task_pair(root, 'overlapping-ranges', spectra, tasks, spw='1:11~82;16~90', nfit=[1, 1])
    task_pair(root, 'overspecified', spectra, tasks, spw='1:43~45', nfit=5)
    for task in tasks.values():
        fitted = task(str(spectra), spw='1:43~44', nfit=1)
        for index, (amplitude, center, width) in enumerate(zip(fitted['peak'], fitted['cent'], fitted['fwhm'])):
            assert amplitude[0][1] == center[0][1] == width[0][1] == 0.
            observed = (2. + index // 2) if index % 2 == 0 else (8. + 2. * (index // 2))
            x = np.array([43, 44])
            np.testing.assert_allclose(gaussian(x, amplitude[0][0], center[0][0], width[0][0]),
                                       gaussian(x, observed, 43., 8.), rtol=1e-3)


def check_averaging(root, source, tasks):
    spectra = timed_spectra(root, source)
    for mode in ('stokes', 'geometric'):
        result, _ = task_pair(root, f'polarization-{mode}', spectra, tasks, polaverage=mode, nfit=[1])
        expected = 5. if mode == 'stokes' else 6.8
        np.testing.assert_allclose(result['casa']['peak'][0][0][0], expected, atol=.02)
    for span in ('', 'scan'):
        result, _ = task_pair(root, f'time-{span or "within-scan"}', spectra, tasks,
                              timebin='8s', timespan=span, nfit=[1], report=True)
        assert len(result['casa']['nfit']) == (2 if span else 4)
    task_pair(root, 'combined-average', spectra, tasks, timebin='8s', timespan='scan',
              polaverage='geometric', nfit=[1], report=True)
    check_time_bins(root, spectra, source, tasks)
    check_degenerate_ranges(root, spectra, tasks)
    if 'native' in tasks:
        options = dict(nfit=1, pol='YY')
        reference = tasks['casa'](str(spectra), **options)
        compare_fits(reference, tasks['native'](str(spectra), polaverage='geometric', **options))
        failure(lambda: tasks['native'](str(spectra), nfit=-1), RuntimeError, 'number of line components')
    return spectra


def failure(call, kind, contains):
    try:
        call()
    except kind as error:
        assert contains in str(error), str(error)
    else:
        raise AssertionError(f'Expected {kind.__name__}: {contains}')


def check_reports(root, source, tasks):
    for name, task in tasks.items():
        output = root / f'overwrite-{name}.txt'
        output.write_text('previous\n')
        failure(lambda: task(str(source), outfile=str(output)), ValueError, ' exists.')
        assert output.read_text() == 'previous\n'
        failure(lambda: task(str(source), outfile=str(output), overwrite=True,
                             spw='1:5~90', nfit=[1, 2]), RuntimeError,
                'number of elements of nfit and fitranges')
        if name == 'casa':
            assert not output.exists()
        else:
            assert output.read_text() == 'previous\n'
        assert task(str(source), outfile=str(output), overwrite=True)['nfit']
        assert output.read_text().startswith('#SCAN\tTIME')
        failure(lambda: task(str(source), spw='1:5~90', nfit=[1, 2]), RuntimeError,
                'number of elements of nfit and fitranges')
        failure(lambda: task(str(source), timebin='0s'), RuntimeError, 'time bin should be positive')
        failure(lambda: task(str(source), fitmode='wrong'), AssertionError, 'unallowed value')
        failure(lambda: task(str(source), datacolumn='corrected_data'), RuntimeError, 'Invalid data column name')
        failure(lambda: task(str(source), spw='1:0~15;17~90', nfit=[0, 1]), AssertionError, '')
        failure(lambda: task(str(source), fitmode='auto', minwidth=0), RuntimeError, 'minwidth > 0')


def fit_tool(factory, prefix, source):
    tool = factory()
    failure(tool.fit_line, RuntimeError, 'please run open')
    assert tool.open(str(source))
    assert tool.set_selection(spw=[1])
    texts, tables = [], []
    for index, extra in enumerate(({'polaverage':'stokes'}, {}, {'timebin':'8s','timespan':'scan'}, {}, {})):
        if index == 4:
            assert tool.open(str(source))
        text, table = prefix.with_suffix(f'.{index}.csv'), prefix.with_suffix(f'.{index}.ms')
        assert tool.fit_line('data', '1', 'YY', nfit='1', tempfile=str(text), tempoutfile=str(table), **extra)
        assert text.exists()
        with opened(table) as ms:
            assert ms.nrows() == 0
            assert ms.getkeyword('SPECTRAL_WINDOW')
        texts.append(text)
        tables.append(table)
    failure(lambda: tool.fit_line(spw='1', tempfile=str(texts[-1]), tempoutfile=str(tables[-1])),
            RuntimeError, 'temporary ms file unexpectedly exists.')
    assert tool.close()
    return texts, tables


def check_tools(root, source, tools):
    products = {name: fit_tool(factory, root / f'direct-{name}', source)
                for name, factory in tools.items()}
    referee = products['casa'][0]
    assert [len(text.read_text().splitlines()) for text in referee] == [4, 4, 2, 2, 8]
    assert referee[0].read_text() == referee[1].read_text()
    assert referee[2].read_text() == referee[3].read_text()
    if 'native' in products:
        from casars.tools import table as native_table

        for expect, result in zip(referee, products['native'][0], strict=True):
            for left, right in zip(expect.read_text().splitlines(), result.read_text().splitlines(), strict=True):
                a, b = left.split(','), right.split(',')
                assert a[:6] == b[:6], (a[:6], b[:6])
                np.testing.assert_allclose(np.array(a[6:], float), np.array(b[6:], float), atol=4e-5, rtol=2e-4)
        for source_table, native_table_path in zip(products['casa'][1], products['native'][1], strict=True):
            for subtable, columns in (
                ('POLARIZATION', ('NUM_CORR', 'CORR_TYPE', 'CORR_PRODUCT', 'FLAG_ROW')),
                ('DATA_DESCRIPTION', ('POLARIZATION_ID', 'SPECTRAL_WINDOW_ID')),
            ):
                with opened(source_table / subtable) as expect, opened(native_table_path / subtable) as actual:
                    assert expect.nrows() == actual.nrows()
                    for row in range(expect.nrows()):
                        for column in columns:
                            np.testing.assert_array_equal(expect.getcell(column, row), actual.getcell(column, row))
        for tables in (products['casa'][1], products['native'][1]):
            reader = native_table()
            try:
                assert reader.open(str(tables[0]))
                assert reader.nrows() == 0
                assert reader.getkeyword('SPECTRAL_WINDOW')
            finally:
                assert reader.close()


def check_direct_channel_weights(root, source, tools):
    values = {}
    for name, factory in tools.items():
        tool = factory()
        raw = root / f'stride-weights-{name}.csv'
        product = root / f'stride-weights-{name}.ms'
        try:
            assert tool.open(str(source))
            assert tool.set_selection(spw='1:15~85', scan='1')
            assert tool.fit_line(spw='1:1~26', nfit=',,1,,', polaverage='geometric',
                                 tempfile=str(raw), tempoutfile=str(product))
        finally:
            assert tool.close()
        rows = [row.split(',') for row in raw.read_text().splitlines()]
        assert rows
        values[name] = np.asarray([row[6:] for row in rows], dtype=float)
    if 'native' in values:
        np.testing.assert_allclose(values['native'], values['casa'], rtol=4e-4, atol=5e-5)



def check_direct_publication(root, source, tools):
    def write(factory, report, output):
        tool = factory()
        try:
            assert tool.open(str(source))
            return tool.fit_line(spw='1', nfit='1', tempfile=str(report), tempoutfile=str(output))
        finally:
            assert tool.close()

    for name, factory in tools.items():
        target = root / f'fit-report-target-{name}.csv'
        report = root / f'fit-report-link-{name}.csv'
        output = root / f'fit-report-link-{name}.ms'
        report.symlink_to(target.name)
        assert write(factory, report, output)
        assert report.is_symlink() and target.read_text()
        with opened(output) as table:
            assert table.nrows() == 0

    if 'native' in tools:
        report = root / 'contested-fit.csv'
        products = [root / f'contested-fit-{index}.ms' for index in range(2)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            attempts = [pool.submit(write, tools['native'], report, output) for output in products]
        assert sum(attempt.exception() is None for attempt in attempts) == 1
        assert sum(output.exists() for output in products) == 1
        assert report.read_text()
        retry = next(output for output in products if not output.exists())
        assert write(tools['native'], root / 'contested-fit-retry.csv', retry)



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    tasks = {'casa': casa.sdfit}
    tools = {'casa': singledishms}
    if not args.reference_only:
        from casars.tasks import sdfit
        from casars.tools import singledishms as native_tool

        tasks['native'] = sdfit
        tools['native'] = native_tool
    source, data, flags = line_map(root)
    check_profiles(root, source, data, flags, tasks)
    spectra = check_averaging(root, source, tasks)
    check_reports(root, source, tasks)
    check_tools(root, spectra, tools)
    check_direct_channel_weights(root, source, tools)
    check_direct_publication(root, spectra, tools)
    print('Single-dish fit values, errors, averaging, selection and output products passed.', flush=True)


if __name__ == '__main__':
    main()
