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
    yield 'extend-selected', dict(mode='extend', extendpols=True, growtime=10.0, growfreq=20.0, antenna='A1', spw='0:2~4')
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
        if name in ('quack-beg', 'extend-pol', 'extend-freq'):
            assert count > 0, (column, name, count)
        print(f'{column} {name}: {count} changed cells', flush=True)
        comparisons += 1
        changes += count
    return comparisons, changes


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
        for source, column in fixture(root):
            count, changed = exercise(root, native, source, column)
            comparisons += count
            changes += changed
    print(f'{comparisons} paired calibration quack, extension and unsupported-mode cases; {changes} changed cells.')


if __name__ == '__main__':
    main()
