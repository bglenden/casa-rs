#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare nondefault ALMA WVR task and direct-tool behavior with CASA 6.7.6.14."""

import argparse
import inspect
import os
import re
import shutil
from pathlib import Path
import time

import casatasks
from casatools import wvr
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from wvrgcal_oracle import fixture


def extra_inputs(ms, offset_table):
    """Add a second source, a valid POINTING series, and impaired antenna DA05."""
    from casacore.tables import makearrcoldesc, makescacoldesc, maketabdesc, table

    for subtable, name in (('FIELD', 'SCIENCE'), ('SOURCE', 'SCIENCE')):
        with opened(ms / subtable, True) as tab:
            values = {col: tab.getcell(col, 0) for col in tab.colnames()
                      if tab.iscelldefined(col, 0)}
            row = tab.nrows()
            tab.addrows(1)
            for column, value in values.items():
                tab.putcell(column, row, value)
            tab.putcell('NAME', row, name)
            tab.putcell('SOURCE_ID', row, 1)
    with opened(ms / 'HISTORY', True) as tab:
        row = tab.nrows()
        tab.addrows(1)
        tab.putcell('APPLICATION', row, 'input-only sentinel')
    with opened(ms / 'POINTING', True) as tab:
        for row in range(tab.nrows()):
            direction = tab.getcell('DIRECTION', row)
            direction[1, :] = 1.4833566897263437
            tab.putcell('DIRECTION', row, direction)
    with opened(ms, True) as tab:
        times, antenna = tab.getcol('TIME'), tab.getcol('ANTENNA1')
        first_time = min(times)
        tab.putcol('FIELD_ID', (times >= first_time + 12).astype('int32'))
        autos = np.flatnonzero((tab.getcol('DATA_DESC_ID') == 1) &
                               (antenna == tab.getcol('ANTENNA2')))
        for row in autos[(antenna[autos] == 4) & (times[autos] < first_time + 8)]:
            tab.putcell('FLAG', int(row), np.ones((1, 4), bool))
    description = maketabdesc([makescacoldesc('TIME', 0.), makescacoldesc('ANTENNA', 0),
                               makearrcoldesc('OFFSETS', 0., ndim=1, shape=[4])])
    with table(str(offset_table), description, nrow=len(autos), ack=False) as offsets:
        offsets.putcol('TIME', times[autos])
        offsets.putcol('ANTENNA', antenna[autos])
        offsets.putcol('OFFSETS', np.tile([.02, .01, -.02, .01], (len(autos), 1)))


def compare_tables(reference, candidate):
    columns = ('TIME', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'ANTENNA1', 'ANTENNA2',
               'INTERVAL', 'SCAN_NUMBER', 'OBSERVATION_ID', 'FLAG', 'PARAMERR', 'SNR')
    with opened(reference) as want, opened(candidate) as got:
        assert got.info()['subType'] == want.info()['subType'] == 'T Jones'
        for col in columns:
            np.testing.assert_array_equal(got.getcol(col), want.getcol(col), err_msg=col)
        actual, expected = got.getcol('CPARAM'), want.getcol('CPARAM')
    assert actual.shape == expected.shape and expected.size
    error = np.linalg.norm(actual - expected) / np.linalg.norm(expected)
    assert error <= 1e-3, (reference.name, error)
    with opened(candidate / 'HISTORY') as history:
        assert history.nrows() == 1 and 'WVR' in history.getcell('APPLICATION', 0)
    return float(error)


def compare_reports(reference, candidate):
    assert reference.keys() == candidate.keys()
    for key, expected in reference.items():
        if key in ('RMS_um', 'Disc_um'):
            np.testing.assert_allclose(candidate[key], expected, rtol=1e-3, atol=0.001, err_msg=key)
        else:
            np.testing.assert_array_equal(candidate[key], expected, err_msg=key)


def run_cases(root, vis, native, native_wvr):
    cases = (
        ('segmented', {}, {}),
        ('pointing', {'usefieldtab': False}, {}),
        ('two-fits', {'segsource': False, 'nsol': 2}, {}),
        ('time-offset', {'toffset': -1.}, {}),
        ('source-flag', {'sourceflag': ['FLUX']}, {}),
        ('tied-sources', {'tie': '0,1'}, {}),
        ('unsegmented-source-flag', {'segsource': False, 'sourceflag': ['FLUX']}, {}),
        ('stat-field', {'statfield': '0'}, {}),
        ('stat-source', {'statsource': 'FLUX'}, {}),
        ('stat-final-source', {'statsource': 'SCIENCE'}, {}),
        ('temperature-offsets', {'offsetstable': str(root / 'offsets')}, {}),
        ('dispersed-window', {'disperse': True, 'spw': 0, 'reversespw': '0'}, {}),
        ('smooth-scale', {'smooth': '7s', 'scale': .8}, {}),
        ('antenna-distance', {'wvrflag': ['0', 'DA02'], 'refant': ['DA02', '2'],
                              'maxdistm': 80., 'minnumants': 2, 'mingoodfrac': .5}, {}),
        ('good-fraction', {'mingoodfrac': .5, 'refant': '4'}, {}),
        ('fraction-above-one', {'mingoodfrac': 1.1}, {}),
        ('continuum', {'segsource': False, 'cont': True, 'rseed': 79}, {}),
        ('tool-smooth-reverse', {}, {'reverse': True, 'smooth': 4, 'wvrspw': 1}),
        ('tool-reverse-window', {}, {'reversespw': ['0'], 'spw': 0, 'segsource': True}),
    )
    for name, task_args, tool_args in cases:
        default = dict(wvrspw=[1], usefieldtab=True)
        expected, actual = (root / f'{name}-{engine}.cal' for engine in ('casa', 'native'))
        if tool_args:
            log_casa, log_native = (root / f'{name}-{engine}.log' for engine in ('casa', 'native'))
            assert wvr().gcal(str(vis), str(expected), logfile=str(log_casa), **(default | tool_args)) == 0
            assert native_wvr().gcal(str(vis), str(actual), logfile=str(log_native), **(default | tool_args)) == 0
            for logfile in (log_casa, log_native):
                text = logfile.read_text()
                assert 'Disc (um)' in text and 'Expected performance' in text
            if name == 'tool-reverse-window':
                def estimate(path):
                    return float(re.search(r'Rough estimate.*coefficient error.*: (\S+)', path.read_text())[1])
                expected_error = estimate(log_casa)
                assert expected_error > 0.0
                np.testing.assert_allclose(estimate(log_native), expected_error, rtol=0.002)
        else:
            reference = casatasks.wvrgcal(str(vis), str(expected), **(default | task_args))
            candidate = native.wvrgcal(str(vis), str(actual), **(default | task_args))
            assert reference['rval'] == (-2 if name == 'continuum' else 0)
            compare_reports(reference, candidate)
            if name == 'smooth-scale':
                compare_tables(Path(f'{expected}_unsmoothed'), Path(f'{actual}_unsmoothed'))
        error = compare_tables(expected, actual)
        print(f'{name} gain normalized RMS {error:.6g}', flush=True)
    return len(cases)


def publication_controls(root, vis, native, native_wvr):
    defaults = dict(usefieldtab=True, wvrspw=[1], wvrflag=['0', '1'],
                    refant='0', maxdistm=0.)
    denied = root / 'unusable-ref.cal'
    expected = casatasks.wvrgcal(str(vis), str(denied), **defaults)
    actual = native.wvrgcal(str(vis), str(denied), **defaults)
    compare_reports(expected, actual)
    assert actual['rval'] == -1 and not denied.exists()

    for invalid in (dict(nsol=2), dict(minnumants=4)):
        malformed = root / 'refused-argument.cal'
        for api in (casatasks, native):
            try:
                api.wvrgcal(str(vis), str(malformed), usefieldtab=True, **invalid)
            except Exception:
                assert not malformed.exists()
            else:
                raise AssertionError(f'invalid arguments were accepted: {invalid}')

    defaults = dict(usefieldtab=True, wvrspw=[1], segsource=False)
    reference = root / 'replace-casa.cal'
    candidate = root / 'replace-native.cal'
    for tool, output in ((wvr(), reference), (native_wvr(), candidate)):
        log = output.with_suffix('.log')
        assert tool.gcal(str(vis), str(output), logfile=str(log), **defaults) == 0
        assert tool.gcal(str(vis), str(output), logfile=str(log), scale=.9, **defaults) == 0
    compare_tables(reference, candidate)

    protected = vis / 'table.dat'
    before = protected.read_bytes()
    assert native_wvr().gcal(str(vis), str(denied), logfile=str(protected), **defaults) == -1
    assert protected.read_bytes() == before and not denied.exists()
    try:
        native.wvrgcal(str(vis), str(candidate), **defaults)
    except ValueError:
        pass
    else:
        raise AssertionError('the task replaced an existing table')
    from casacore.tables import table
    offsets = root / 'offsets'
    offset_view = root / 'offset-view'
    with table(str(offsets), ack=False) as backing, backing.selectrows(range(backing.nrows())) as view:
        view.copy(str(offset_view), deep=False).close()
    for scientific_input, options in (
        (root / 'libair-ddefault.csv', dict(disperse=True)),
        (offsets / 'table.dat', dict(offsets=str(offset_view))),
    ):
        original = scientific_input.read_bytes()
        assert native_wvr().gcal(str(vis), str(candidate), logfile=str(scientific_input),
                                 **(defaults | options)) == -1
        assert scientific_input.read_bytes() == original
    compare_tables(reference, candidate)


def reference_retry(root, original, native_wvr):
    vis = root / 'retry.ms'
    copy_ms(original, vis)
    with opened(vis, True) as ms:
        desc, left, right = (ms.getcol(col) for col in ('DATA_DESC_ID', 'ANTENNA1', 'ANTENNA2'))
        for row in np.flatnonzero((desc == 1) & (left == 0) & (right == 0)):
            ms.putcell('DATA', int(row), np.array([[297, 282, 285, 271]], np.complex64))
    paths = [root / 'retry-casa.cal', root / 'retry-native.cal']
    tool_args = dict(usefieldtab=True, wvrspw=[1], refant='0')
    for tool, path in zip((wvr(), native_wvr()), paths):
        assert tool.gcal(str(vis), str(path), logfile=str(path.with_suffix('.log')), **tool_args) == 0
        for selection, code in (({'wvrspw': [0]}, -10), ({'spw': [99]}, -11), ({'reversespw': [99]}, 1)):
            refused = root / f'{path.stem}-{code}.cal'
            assert tool.gcal(str(vis), str(refused), **(tool_args | selection)) == code
            assert not refused.exists()
    error = compare_tables(*paths)
    assert 'Bayesian evidence was zero' in paths[0].with_suffix('.log').read_text()
    print(f'zero-evidence antenna replacement gain normalized RMS {error:.6g}', flush=True)


def metadata_controls(root, vis, native, native_wvr):
    mutated = root / 'metadata.ms'
    copy_ms(vis, mutated)
    with opened(mutated / 'DATA_DESCRIPTION', True) as dd:
        row = dd.nrows()
        dd.addrows(1)
        for column in dd.colnames():
            dd.putcell(column, row, dd.getcell(column, 1))
    with opened(mutated / 'SOURCE', True) as source:
        row = source.nrows() - 1
        assert source.getcell('SOURCE_ID', row) == 1
        source.putcell('NAME', row, 'FLUX')
    reference = root / 'metadata-casa.cal'
    candidate = root / 'metadata-native.cal'
    options = dict(usefieldtab=True, statsource='FLUX')
    expected = casatasks.wvrgcal(str(mutated), str(reference), **options)
    actual = native.wvrgcal(str(mutated), str(candidate), **options)
    assert expected['success']
    compare_reports(expected, actual)
    compare_tables(reference, candidate)

    logfile = str(root / 'sparse-spw.log')
    sparse = root / 'sparse-reverse-native.cal'
    ordinary = root / 'sparse-ordinary-native.cal'
    sparse_casa = root / 'sparse-reverse-casa.cal'
    options = dict(usefieldtab=True, spw=[1], logfile=logfile)
    assert native_wvr().gcal(str(mutated), str(ordinary), **options) == 0
    assert native_wvr().gcal(str(mutated), str(sparse), reverse=True, **options) == 0
    assert wvr().gcal(str(mutated), str(sparse_casa), reverse=True, **options) == 0
    with opened(ordinary) as direct, opened(sparse) as reverse, opened(sparse_casa) as upstream:
        np.testing.assert_allclose(reverse.getcol('CPARAM'), np.conj(direct.getcol('CPARAM')), atol=1e-6)
        np.testing.assert_allclose(upstream.getcol('CPARAM'), direct.getcol('CPARAM'), atol=1e-6)
        assert np.max(np.abs(direct.getcol('CPARAM').imag)) > 0.1

    # Output history is independent when the source lacks its HISTORY subtable.
    no_history = root / 'no-history.ms'
    copy_ms(mutated, no_history)
    with opened(no_history, True) as ms:
        ms.removekeyword('HISTORY')
    shutil.rmtree(no_history / 'HISTORY')
    independent_history = root / 'independent-history.cal'
    assert native_wvr().gcal(str(no_history), str(independent_history), **options) == 0
    with opened(independent_history / 'HISTORY') as history:
        assert history.nrows() == 1

    with opened(mutated / 'SPECTRAL_WINDOW', True) as spw:
        channels = spw.getcell('CHAN_FREQ', 0)
        original_channels = channels.copy()
        channels[len(channels) // 2] = np.nan
        spw.putcell('CHAN_FREQ', 0, channels)
    assert native_wvr().gcal(str(mutated), str(candidate), spw=[0], usefieldtab=True, logfile=logfile) == -1
    compare_tables(reference, candidate)
    with opened(mutated / 'SPECTRAL_WINDOW', True) as spw:
        spw.putcell('CHAN_FREQ', 0, original_channels)

    unavailable = root / 'all-antenna-flags.cal'
    with opened(mutated / 'ANTENNA', True) as ant:
        ant.putcol('FLAG_ROW', np.ones(ant.nrows(), bool))
    for tool in (wvr(), native_wvr()):
        assert tool.gcal(str(mutated), str(unavailable), **options) == -1
        assert not unavailable.exists()
    with opened(mutated / 'ANTENNA', True) as ant:
        ant.putcol('FLAG_ROW', np.zeros(ant.nrows(), bool))
    with opened(mutated, True) as ms:
        for row in np.flatnonzero(ms.getcol('DATA_DESC_ID') == 1):
            ms.putcell('FLAG', int(row), np.ones((1, 4), bool))
    for api in (casatasks, native):
        failed = api.wvrgcal(str(mutated), str(unavailable), usefieldtab=True)
        assert failed['rval'] == 1 and not failed['success'] and not unavailable.exists()
    print('WVR metadata, all-flagged inputs and sparse reversal controls passed.', flush=True)


def recorded_cases(root, vis, native):
    """Compare recorded retrievals and apply the default tables to science data."""
    for name, args in (
        ('default', {}),
        ('time-offset', dict(segsource=False, toffset=-1.)),
        ('fixed-fits', dict(segsource=False, nsol=5, rseed=27)),
    ):
        results = []
        for name_api, api in (('casa', casatasks), ('native', native)):
            path = root / f'{name}-{name_api}.cal'
            start = time.monotonic()
            report = api.wvrgcal(str(vis), str(path), **args)
            duration = time.monotonic() - start
            assert report['success'], (name, name_api, report)
            results.append((path, report, duration))
        (path_casa, report_casa, casa_seconds), (path_native, report_native, native_seconds) = results
        compare_reports(report_casa, report_native)
        error = compare_tables(path_casa, path_native)
        print(f'{name} recorded gain normalized RMS {error:.6g}, CASA {casa_seconds:.3f}s, '
              f'casa-rs {native_seconds:.3f}s', flush=True)


def corrected_recorded_sample(root, vis, native):
    with opened(vis / 'SPECTRAL_WINDOW') as table:
        channels = table.getcol('NUM_CHAN')
    with opened(vis / 'DATA_DESCRIPTION') as table:
        windows = table.getcol('SPECTRAL_WINDOW_ID')
    with opened(vis) as table:
        present = np.unique(table.getcol('DATA_DESC_ID'))
    science = next(int(windows[dd]) for dd in present if channels[windows[dd]] != 4)
    descriptions = np.flatnonzero(windows == science).tolist()
    samples = []
    for engine, api in (('casa', casatasks), ('native', native)):
        product = root / f'corrected-{engine}.ms'
        copy_ms(vis, product)
        api.applycal(str(product), spw=str(science), gaintable=[str(root / f'default-{engine}.cal')],
                     flagbackup=False, calwt=False)
        with opened(product) as table:
            selected = table.query(f'DATA_DESC_ID IN {descriptions}')
            try:
                rows = np.linspace(0, selected.nrows() - 1, min(64, selected.nrows()), dtype=int)
                data = [selected.getcell('CORRECTED_DATA', int(row)) for row in rows]
                flags = [selected.getcell('FLAG', int(row)) for row in rows]
                samples.append((np.stack(data), np.stack(flags)))
            finally:
                selected.close()
    (reference, flags), (actual, actual_flags) = samples
    np.testing.assert_array_equal(flags, actual_flags)
    usable = ~flags
    denominator = np.linalg.norm(reference[usable])
    assert denominator > 0
    error = np.linalg.norm(actual[usable] - reference[usable]) / denominator
    assert error <= 1e-3, error
    print(f'recorded science SPW {science} corrected visibility normalized RMS {error:.6g}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--recorded-ms', type=Path, help='recorded ALMA MeasurementSet with WVR data')
    args = parser.parse_args()
    from casars import tasks
    from casars.tools import wvr as native_wvr

    assert casatasks.version_string() == '6.7.6.14'
    for oracle, candidate in ((casatasks.wvrgcal, tasks.wvrgcal), (wvr().gcal, native_wvr().gcal)):
        expected = inspect.signature(oracle).parameters
        actual = inspect.signature(candidate).parameters
        assert list(actual)[:len(expected)] == list(expected)
        for key, parameter in expected.items():
            assert (actual[key].kind, actual[key].default) == (parameter.kind, parameter.default), key
    root = args.work.resolve()
    root.mkdir()
    if args.recorded_ms is not None:
        recorded_ms = args.recorded_ms.resolve(strict=True)
        recorded_cases(root, recorded_ms, tasks)
        corrected_recorded_sample(root, recorded_ms, tasks)
        return
    os.chdir(root)
    vis = root / 'radiometer.ms'
    fixture(vis)
    extra_inputs(vis, root / 'offsets')
    (root / 'libair-ddefault.csv').write_text(
        '50000000000 , 0.02\n100000000000 , 0.07\n150000000000 , 0.05\n200000000000 , 0.15\n')
    os.environ['WVRGCAL_DISPDIR'] = str(root)
    count = run_cases(root, vis, tasks, native_wvr)
    publication_controls(root, vis, tasks, native_wvr)
    reference_retry(root, vis, native_wvr)
    metadata_controls(root, vis, tasks, native_wvr)
    print(f'{count} nondefault WVR scenarios and publication controls passed.')


if __name__ == '__main__':
    main()
