#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare nondefault ALMA WVR task and direct-tool behavior with CASA 6.7.6.14."""

import argparse
import inspect
import os
from pathlib import Path
import time

import casatasks
from casacore.tables import makearrcoldesc, makescacoldesc, maketabdesc, table
from casatools import wvr
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from wvrgcal_oracle import fixture


def extra_inputs(ms, offset_table):
    """Add a second source, a valid POINTING series, and impaired antenna DA05."""
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
        assert history.nrows() and any('WVR' in item for item in history.getcol('APPLICATION'))
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
        ('stat-field', {'statfield': '0'}, {}),
        ('stat-source', {'statsource': 'FLUX'}, {}),
        ('temperature-offsets', {'offsetstable': str(root / 'offsets')}, {}),
        ('dispersed-window', {'disperse': True, 'spw': 0, 'reversespw': '0'}, {}),
        ('smooth-scale', {'smooth': '7s', 'scale': .8}, {}),
        ('antenna-distance', {'wvrflag': ['0', 'DA02'], 'refant': ['DA02', '2'],
                              'maxdistm': 80., 'minnumants': 2, 'mingoodfrac': .5}, {}),
        ('good-fraction', {'mingoodfrac': .5, 'refant': '4'}, {}),
        ('continuum', {'segsource': False, 'cont': True, 'rseed': 79}, {}),
        ('tool-smooth-reverse', {}, {'reverse': True, 'smooth': 4, 'wvrspw': 1}),
        ('tool-reverse-window', {}, {'reversespw': ['0'], 'spw': 0}),
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
        for selection, code in (({'wvrspw': [0]}, -10), ({'spw': [99]}, -11)):
            refused = root / f'{path.stem}-{code}.cal'
            assert tool.gcal(str(vis), str(refused), **(tool_args | selection)) == code
            assert not refused.exists()
    error = compare_tables(*paths)
    assert 'Bayesian evidence was zero' in paths[0].with_suffix('.log').read_text()
    print(f'zero-evidence antenna replacement gain normalized RMS {error:.6g}', flush=True)


def recorded_cases(root, vis, native):
    """Read the recorded MS directly so validation only creates solution products."""
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
        (path_casa, casa, casa_seconds), (path_native, native, native_seconds) = results
        compare_reports(casa, native)
        error = compare_tables(path_casa, path_native)
        print(f'{name} recorded gain normalized RMS {error:.6g}, CASA {casa_seconds:.3f}s, '
              f'casa-rs {native_seconds:.3f}s', flush=True)


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
        recorded_cases(root, args.recorded_ms.resolve(strict=True), tasks)
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
    print(f'{count} nondefault WVR scenarios and publication controls passed.')


if __name__ == '__main__':
    main()
