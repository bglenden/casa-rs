#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input single-dish smoothing and tool lifecycle against CASA 6.7.6.14."""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import casatasks as casa
from casatools import singledishms
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from sdcal_edges_oracle import regular_map
from sdgaincal_oracle import duplicate_table_row


def spectral_map(root, channel_counts=(6, 4)):
    original = regular_map(root)
    vis = root / 'smooth-input.ms'
    casa.split(str(original), str(vis), spw='0', datacolumn='data')
    with opened(vis / 'ANTENNA', True) as table:
        second = duplicate_table_row(table, 0)
        table.putcell('NAME', second, 'SECOND')
    with opened(vis / 'FIELD', True) as table:
        alternate_field = duplicate_table_row(table, 0)
        table.putcell('NAME', alternate_field, 'SelectedField')
    with opened(vis / 'STATE', True) as table:
        alternate = duplicate_table_row(table, 0)
        table.putcell('OBS_MODE', alternate, 'CALIBRATE_PHASE#ON_SOURCE')
    with opened(vis / 'SPECTRAL_WINDOW', True) as table:
        assert duplicate_table_row(table, 0) == 1
        for window, count in enumerate(channel_counts):
            frequency = 225e9 + window * 1e9
            step = 4e6
            table.putcell('REF_FREQUENCY', window, frequency)
            table.putcell('NUM_CHAN', window, count)
            table.putcell('CHAN_FREQ', window, frequency + step * np.arange(count))
            for name in ('CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
                table.putcell(name, window, np.full(count, step))
            table.putcell('TOTAL_BANDWIDTH', window, count * step)
    with opened(vis / 'DATA_DESCRIPTION', True) as table:
        assert duplicate_table_row(table, 0) == 1
        table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(vis, True) as table:
        count = table.nrows()
        for name, source in (
            ('DATA', 'DATA'), ('CORRECTED_DATA', 'DATA'), ('FLAG', 'FLAG'),
            ('WEIGHT_SPECTRUM', 'WEIGHT'), ('SIGMA_SPECTRUM', 'WEIGHT'),
        ):
            descriptor = table.getcoldesc(source)
            descriptor.update(name=name, ndim=2, option=0, dataManagerType='StandardStMan',
                              dataManagerGroup=f'Oracle{name}')
            descriptor.pop('shape', None)
            if name in table.colnames():
                table.removecols(name)
            table.addcols({name: descriptor})
        times = table.getcol('TIME')[::-1]
        table.putcol('TIME', times)
        table.putcol('TIME_CENTROID', times)
        antenna2 = np.zeros(count, dtype=np.int32)
        antenna2[[0, 2]] = second
        table.putcol('ANTENNA2', antenna2)
        indices = np.arange(count, dtype=np.int32)
        table.putcol('FIELD_ID', np.where(indices // 2 % 2 == 1, alternate_field, 0).astype(np.int32))
        table.putcol('SCAN_NUMBER', 1 + indices // 4 % 2)
        table.putcol('STATE_ID', np.where(indices // 8 % 2 == 1, alternate,
                                        table.getcol('STATE_ID')).astype(np.int32))
        for row in range(count):
            window = row % 2
            channels = channel_counts[window]
            table.putcell('DATA_DESC_ID', row, window)
            shape = (2, channels)
            spectrum = (np.arange(2 * channels).reshape(shape) + row * .25 + 1 + 8j)
            spectrum[0, 1] = 15. + 3.j
            flags = np.zeros(shape, dtype=bool)
            flags[0, 1] = True
            weights = 2. + np.arange(2 * channels, dtype=np.float32).reshape(shape)
            table.putcell('DATA', row, spectrum.astype(np.complex64))
            table.putcell('CORRECTED_DATA', row, (spectrum * 3.).astype(np.complex64))
            table.putcell('FLAG', row, flags)
            table.putcell('WEIGHT_SPECTRUM', row, weights * 2.)
            table.putcell('SIGMA_SPECTRUM', row, 1. / np.sqrt(weights))
        table.putcol('FLAG_ROW', np.zeros(count, dtype=bool))
    return vis


def kernel(kind, length, width):
    if kind == 'gaussian':
        sigma = width / np.sqrt(8. * np.log(2.))
        distance = (np.arange(length) - length / 2.) / sigma
        return np.exp(-.5 * distance**2) / (np.sqrt(2. * np.pi) * sigma)
    span = int(width + .5)
    start = (length - span) // 2 + 1
    indices = np.arange(length)
    return ((start <= indices) & (indices < start + span)) / span


def check_pixels(source, product, kind, width, column='DATA', partial=False):
    with opened(source) as raw, opened(product) as output:
        times = raw.getcol('TIME')
        windows = raw.getcol('DATA_DESC_ID')
        valid = raw.getcol('ANTENNA1') == raw.getcol('ANTENNA2')
        rows = sorted(np.flatnonzero(valid), key=lambda row: (windows[row], times[row]))
        assert output.nrows() == len(rows)
        np.testing.assert_array_equal(output.getcol('TIME'), times[rows])
        for out_row, input_row in enumerate(rows):
            data = raw.getcell(column, input_row)
            flags = raw.getcell('FLAG', input_row)
            if partial and windows[input_row] == 0:
                data, flags = data[:, 1:5], flags[:, 1:5]
            length = data.shape[1]
            psf = kernel(kind, length, width)
            full = np.array([np.convolve(np.where(mask, 0., values.real), psf)
                             for values, mask in zip(data, flags)])
            expected = full[:, length // 2:length // 2 + length]
            target = 'FLOAT_DATA' if column == 'FLOAT_DATA' else 'DATA'
            np.testing.assert_allclose(output.getcell(target, out_row), expected,
                                       atol=1e-5, rtol=2e-6)
            np.testing.assert_array_equal(output.getcell('FLAG', out_row), flags)


def compare_outputs(expected, actual):
    with opened(expected) as reference, opened(actual) as result:
        assert reference.nrows() == result.nrows()
        for name in ('TIME', 'ANTENNA1', 'ANTENNA2', 'DATA_DESC_ID', 'FLAG_ROW',
                     'FIELD_ID', 'SCAN_NUMBER', 'OBSERVATION_ID', 'ARRAY_ID',
                     'FEED1', 'FEED2', 'STATE_ID'):
            np.testing.assert_array_equal(result.getcol(name), reference.getcol(name), err_msg=name)
        for row in range(reference.nrows()):
            for name in ('FLAG', 'DATA', 'FLOAT_DATA', 'WEIGHT', 'SIGMA',
                         'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'):
                if name not in reference.colnames():
                    assert name not in result.colnames(), name
                    continue
                assert name in result.colnames(), name
                assert reference.iscelldefined(name, row) == result.iscelldefined(name, row)
                if reference.iscelldefined(name, row):
                    np.testing.assert_allclose(result.getcell(name, row),
                                               reference.getcell(name, row), atol=1e-5,
                                               rtol=2e-6, err_msg=f'{name} row {row}')
    for subtable, columns in [
        ('DATA_DESCRIPTION', ('SPECTRAL_WINDOW_ID', 'POLARIZATION_ID')),
        ('SPECTRAL_WINDOW', ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'REF_FREQUENCY')),
        ('POLARIZATION', ('NUM_CORR', 'CORR_TYPE', 'CORR_PRODUCT')),
    ]:
        with opened(expected / subtable) as reference, opened(actual / subtable) as result:
            assert reference.nrows() == result.nrows(), subtable
            for row in range(reference.nrows()):
                for name in columns:
                    np.testing.assert_array_equal(result.getcell(name, row),
                                                  reference.getcell(name, row), err_msg=name)


def floating_data(root, source):
    vis = root / 'smooth-float.ms'
    copy_ms(source, vis)
    with opened(vis, True) as table:
        descriptor = table.getcoldesc('DATA')
        descriptor.update(name='FLOAT_DATA', valueType='float', dataManagerType='StandardStMan',
                          dataManagerGroup='FloatSpectrum')
        table.addcols({'FLOAT_DATA': descriptor})
        for row in range(table.nrows()):
            table.putcell('FLOAT_DATA', row, table.getcell('DATA', row).real.copy())
        table.removecols(['DATA', 'CORRECTED_DATA'])
    return vis


def task_pair(root, tasks, label, source, **options):
    products = {}
    for name, task in tasks.items():
        output = root / f'{label}-{name}.ms'
        assert task(str(source), outfile=str(output), **options) is None
        products[name] = output
    if 'native' in products:
        compare_outputs(products['casa'], products['native'])
        from casars.tools import table as native_table
        with opened(products['casa']) as reference:
            reader = native_table()
            try:
                assert reader.open(str(products['casa']))
                np.testing.assert_array_equal(reader.getcell('FLAG', 0), reference.getcell('FLAG', 0))
            finally:
                assert reader.close()
    return products


def test_reindex(root, tasks, source):
    for reindex in (True, False):
        products = task_pair(root, tasks, f'reindex-{reindex}', source,
                             spw='1', pol='YY', reindex=reindex, kwidth=2, kernel='boxcar')
        with opened(products['casa']) as table:
            np.testing.assert_array_equal(table.getcol('DATA_DESC_ID'), 0 if reindex else 1)
            assert table.getcell('DATA', 0).shape == (1, 4)


def test_selection(root, tasks, source):
    with opened(source) as table:
        times = table.getcol('TIME')
        scans = table.getcol('SCAN_NUMBER')
        fields = table.getcol('FIELD_ID')
        windows = table.getcol('DATA_DESC_ID')
        states = table.getcol('STATE_ID')
    with opened(source / 'STATE') as table:
        intent = 'CALIBRATE_PHASE#ON_SOURCE'
        state = list(table.getcol('OBS_MODE')).index(intent)
    with opened(source / 'FIELD') as table:
        field = list(table.getcol('NAME')).index('SelectedField')
    lower = int(np.median(times))
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    timestamp = (epoch + timedelta(seconds=lower)).strftime('%Y/%m/%d/%H:%M:%S')
    products = task_pair(root, tasks, 'selected', source, antenna='0&1',
                         field='SelectedField', scan='2', intent=intent,
                         timerange=f'>{timestamp}', spw='1', pol='YY', kwidth=2)
    expected = np.sort(times[(times > lower) & (scans == 2) & (fields == field)
                             & (windows == 1) & (states == state)])
    with opened(products['casa']) as output:
        assert 0 < output.nrows() < len(times) // 4
        np.testing.assert_array_equal(output.getcol('TIME'), expected)


def test_lifecycle(root, tools, source):
    products = {}
    for name, tool in tools.items():
        session = tool()
        assert session.close() is True
        try:
            session.name()
        except RuntimeError as error:
            assert str(error) == 'No MeasurementSet has been assigned, please run open.'
        else:
            raise AssertionError('An unopened tool accepted name()')
        definitely_missing = root / 'missing-input.ms'
        assert session.open(str(definitely_missing)) is True
        assert session.name() == str(definitely_missing)
        assert session.set_selection(spw='does-not-exist') is True
        assert session.open(str(source)) is True
        assert session.name() == str(source)
        assert session.set_selection(spw='7') is True
        assert session.set_selection(spw='0:1~4,1', reindex=False, beam=[0]) is True
        output = root / f'tool-{name}.ms'
        assert session.smooth('boxcar', 2, 'data', str(output)) is True
        assert session.done() is True
        assert session.close() is True
        products[name] = output
    check_pixels(source, products['casa'], 'boxcar', 2, partial=True)
    if 'native' in products:
        compare_outputs(products['casa'], products['native'])


def test_tool_filters(root, tools, source):
    vis = root / 'tool-filters.ms'
    copy_ms(source, vis)
    with opened(vis / 'OBSERVATION', True) as table:
        other_obs = duplicate_table_row(table, 0)
    with opened(vis / 'FEED', True) as table:
        other_feed = duplicate_table_row(table, 0)
        table.putcell('FEED_ID', other_feed, 1)
    with opened(vis, True) as table:
        rows = np.arange(table.nrows(), dtype=np.int32)
        feeds = rows // 2 % 2
        observations = np.where(rows // 8 % 2 == 1, other_obs, 0).astype(np.int32)
        table.putcol('FEED1', feeds)
        table.putcol('FEED2', feeds)
        table.putcol('OBSERVATION_ID', observations)
        # Keep both these filters in the intersection and let the TAQL
        # predicate admit one of the two otherwise identical scan groups.
        mask = (feeds == 1) & (observations == other_obs) & (table.getcol('SCAN_NUMBER') == 2)
        times = table.getcol('TIME')
        ddids = table.getcol('DATA_DESC_ID')
        expected = times[sorted(np.flatnonzero(mask), key=lambda row: (ddids[row], times[row]))]
    products = {}
    for name, tool in tools.items():
        session = tool()
        assert session.open(str(vis)) is True
        assert session.set_selection(observation=[other_obs], feed='1&&&',
                                     taql='SCAN_NUMBER == 2', reindex=False) is True
        output = root / f'tool-filtered-{name}.ms'
        assert session.smooth('gaussian', 2, 'data', str(output)) is True
        assert session.close() is True
        products[name] = output
    with opened(products['casa']) as table:
        assert 0 < table.nrows() < len(times) // 4
        np.testing.assert_array_equal(table.getcol('TIME'), expected)
    if 'native' in products:
        compare_outputs(products['casa'], products['native'])


def assert_error(invoke, error_type, expected):
    try:
        invoke()
    except error_type as error:
        assert str(error) == expected, repr(error)
    else:
        raise AssertionError(f'Expected {error_type.__name__}: {expected}')


def test_publication(root, tasks, source):
    for name, task in tasks.items():
        assert_error(task, AssertionError,
                     "the infile parameter must be a path that exists ('' does not exist)")
        assert_error(lambda: task(str(source)), ValueError, 'outfile is empty.')
        output = root / f'replace-{name}.ms'
        assert task(str(source), outfile=str(output), kernel='boxcar', kwidth=2) is None
        assert_error(lambda: task(str(source), outfile=str(output)), ValueError,
                     f'{output} exists.')
        assert_error(lambda: task(str(source), outfile=str(output), overwrite=True,
                                  spw='0:1~3', kernel='boxcar', kwidth=2), RuntimeError,
                     'Failed to get convolver. Smoothing is not properly configured.')
        if name == 'native':
            check_pixels(source, output, 'boxcar', 2)
            assert not list(root.glob(f'.{output.name}.casa-rs-spectral-*'))
            try:
                task(str(source), outfile=str(source), overwrite=True)
            except RuntimeError as error:
                assert 'paths must differ' in str(error), repr(error)
            else:
                raise AssertionError('An output alias overwrote the input')
        assert task(str(source), outfile=str(output), overwrite=True, kwidth=2) is None
        check_pixels(source, output, 'gaussian', 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    arguments = parser.parse_args()
    tasks = {'casa': casa.sdsmooth}
    tools = {'casa': singledishms}
    if not arguments.reference_only:
        from casars.tasks import sdsmooth
        from casars.tools import singledishms as native_singledishms
        tasks['native'] = sdsmooth
        tools['native'] = native_singledishms
    assert casa.version_string() == '6.7.6.14'
    root = arguments.work.resolve()
    root.mkdir()
    source = spectral_map(root)
    for kind, width, column in [('boxcar', 2, 'data'), ('gaussian', 3, 'corrected')]:
        products = task_pair(root, tasks, kind, source, datacolumn=column,
                             kernel=kind, kwidth=width)
        check_pixels(source, products['casa'], kind, width,
                     'CORRECTED_DATA' if column == 'corrected' else 'DATA')
    floating = floating_data(root, source)
    products = task_pair(root, tasks, 'float', floating, datacolumn='FLOAT_DATA',
                         kernel='gaussian', kwidth=2)
    check_pixels(floating, products['casa'], 'gaussian', 2, 'FLOAT_DATA')
    test_reindex(root, tasks, source)
    test_selection(root, tasks, source)
    test_lifecycle(root, tools, source)
    test_tool_filters(root, tools, source)
    test_publication(root, tasks, source)
    wide = root / 'wide'
    wide.mkdir()
    wide_source = spectral_map(wide, (130, 66))
    products = task_pair(wide, tasks, 'default', wide_source)
    check_pixels(wide_source, products['casa'], 'gaussian', 5)
    print('Smoothing spectra, original flags and convolution laws agree.', flush=True)


if __name__ == '__main__':
    main()
