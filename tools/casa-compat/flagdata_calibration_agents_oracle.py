#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA's quack and flag extension on persisted calibration tables."""

import argparse
from contextlib import chdir
import os
from pathlib import Path
import shutil

import casatasks as casa
import numpy as np

from calibration_smoothcal_oracle import opened
from flagdata_calibration_oracle import fixture, flags, outcome
from flagmanager_calibration_oracle import assert_flags, original_columns, preserved


def cases():
    for edge in ('beg', 'endb', 'end', 'tail'):
        yield f'quack-{edge}', dict(mode='quack', quackmode=edge, quackinterval=5.0)
    yield 'quack-increment', dict(mode='quack', quackinterval=5.0, quackincrement=True)
    yield 'quack-selected', dict(mode='quack', quackinterval=25.0, antenna='A1', spw='0:2~4')
    yield 'quack-calculate', dict(mode='quack', quackinterval=25.0, action='calculate')
    yield 'quack-invalid', dict(mode='quack', quackmode='unsupported', quackinterval=25.0)
    yield 'extend-pol', dict(mode='extend', extendpols=True, growtime=0.0, growfreq=0.0)
    yield 'extend-freq', dict(mode='extend', extendpols=False, growtime=0.0, growfreq=0.0, flagnearfreq=True)
    yield 'extend-time', dict(mode='extend', extendpols=False, growtime=0.0, growfreq=0.0, flagneartime=True)
    yield 'extend-counts', dict(mode='extend', extendpols=False, growtime=10.0, growfreq=20.0)
    yield 'extend-combined-scans', dict(mode='extend', extendpols=False, growtime=0.0, growfreq=0.0, flagneartime=True, combinescans=True)
    yield 'extend-interval', dict(mode='extend', extendpols=False, growtime=0.0, growfreq=0.0, flagneartime=True, ntime='1s')
    yield 'extend-selected', dict(mode='extend', extendpols=True, growtime=10.0, growfreq=20.0, antenna='A1', spw='0:5~7')
    yield 'extend-selected-window', dict(mode='extend', extendpols=True, growtime=0.0, growfreq=0.0, spw='0:5~7')
    yield 'extend-calculate', dict(mode='extend', extendpols=True, growtime=50.0, growfreq=50.0, action='calculate')
    yield 'extend-invalid', dict(mode='extend', growtime=200.0)
    for mode in ('elevation', 'shadow'):
        yield f'{mode}-unavailable', dict(mode=mode)


def exercise(root, native, source, column):
    comparisons, changes = 0, 0
    for name, settings in cases():
        want, have = [root / f'{engine}-{column}-{name}.cal' for engine in ('casa', 'native')]
        for path in (want, have):
            shutil.copytree(source, path)
            if name == 'extend-selected-window':
                second_window(path)
            if name == 'extend-combined-scans':
                same_field_scans(path)
            if name == 'quack-increment':
                flag_first_scan_time(path)
        initial, metadata = flags(have), original_columns(have)
        casa_result = outcome(casa.flagdata, want, flagbackup=False, **settings)
        our_result = outcome(native, have, flagbackup=False, **settings)
        assert casa_result == our_result, (column, name, casa_result, our_result)
        expected = flags(want)
        assert_flags(have, expected)
        preserved(have, metadata)
        count = int(np.count_nonzero(expected != initial))
        if name in ('quack-calculate', 'extend-calculate', 'quack-invalid', 'extend-invalid'):
            assert count == 0, (column, name, count)
        if name in ('quack-beg', 'extend-pol', 'extend-freq', 'extend-selected', 'extend-selected-window'):
            assert count > 0, (column, name, count)
        if name == 'extend-interval' and column == 'FPARAM':
            assert count > 0
        if name == 'extend-combined-scans':
            with opened(have) as table:
                other_scan = table.getcol('SCAN_NUMBER') != table.getcell('SCAN_NUMBER', 0)
                assert np.any(other_scan)
                np.testing.assert_array_equal(expected[:, :, other_scan], initial[:, :, other_scan])
        if name == 'extend-selected-window':
            with opened(have) as table:
                outside = table.getcol('SPECTRAL_WINDOW_ID') == 1
                assert np.any(outside)
                np.testing.assert_array_equal(expected[:, :, outside], initial[:, :, outside])
        print(f'{column} {name}: {count} changed cells', flush=True)
        comparisons += 1
        changes += count
    return comparisons, changes


def second_window(path):
    with opened(path / 'SPECTRAL_WINDOW', True) as table:
        assert table.nrows() == 1
        columns = {name: table.getcell(name, 0) for name in table.colnames()
                   if table.iscelldefined(name, 0)}
        table.addrows(1)
        for name, value in columns.items():
            table.putcell(name, 1, 'SPW1' if name == 'NAME' else value)
    with opened(path, True) as table:
        windows = table.getcol('SPECTRAL_WINDOW_ID')
        windows[2::2] = 1
        table.putcol('SPECTRAL_WINDOW_ID', windows)


def same_field_scans(path):
    with opened(path, True) as table:
        for column in ('FIELD_ID', 'OBSERVATION_ID'):
            values = table.getcol(column)
            values[:] = values[0]
            table.putcol(column, values)
        original = table.getcol('FLAG')
        original[:] = False
        original[0, 4, 0] = True
        table.putcol('FLAG', original)


def flag_first_scan_time(path):
    with opened(path, True) as table:
        times = table.getcol('TIME')
        scans = table.getcol('SCAN_NUMBER')
        affected = (times == times[0]) & (scans == scans[0])
        original = table.getcol('FLAG')
        original[:, :, affected] = True
        table.putcol('FLAG', original)


def quack_out_of_order(root, native, source):
    for mode in ('beg', 'endb'):
        for label, engine in (('casa', casa.flagdata), ('native', native)):
            path = root / f'{label}-out-of-order-{mode}.cal'
            shutil.copytree(source, path)
            with opened(path, True) as table:
                scans = table.getcol('SCAN_NUMBER')
                selected = scans == scans[0]
                times = table.getcol('TIME')
                start, stop = times[selected].min(), times[selected].max()
                assert stop - start > 1.0
                times[selected] = stop - (times[selected] - start)
                table.putcol('TIME', times)
                expected = np.zeros_like(table.getcol('FLAG'))
                table.putcol('FLAG', expected)
            edge = start if mode == 'beg' else stop
            affected = selected if label == 'casa' else selected & (times == edge)
            expected[:, :, affected] = True
            assert engine(str(path), mode='quack', quackmode=mode, quackinterval=1.0,
                          scan=str(scans[0]), flagbackup=False) == {}
            assert_flags(path, expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_FLAGDATA_BIN'] = str(args.native_bin.resolve())
    from casars.tasks import flagdata as native

    assert casa.version_string() == '6.7.6.14'
    comparisons, changes = 0, 0
    with chdir(root):
        products = fixture(root)
        for source, column in products:
            count, changed = exercise(root, native, source, column)
            comparisons += count
            changes += changed
        quack_out_of_order(root, native, {column: source for source, column in products}['FPARAM'])
    print(f'{comparisons} paired calibration quack, extension and unsupported-mode cases, '
          f'{changes} changed cells, and two out-of-order scan checks.')


if __name__ == '__main__':
    main()
