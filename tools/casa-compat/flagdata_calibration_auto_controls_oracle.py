#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare automatic-agent averaging and chunk arguments on calibration tables."""
import argparse
from contextlib import chdir
import os
from pathlib import Path
import shutil

import casatasks as casa
import numpy as np

from flagdata_calibration_oracle import fixture, flags, outcome
from flagdata_calibration_rfi_oracle import compare_report, scan_fixture, time_fixture
from flagmanager_calibration_oracle import assert_flags, original_columns, preserved


def controls():
    yield 'default', {}, True
    yield 'channel-average', {'channelavg': True, 'chanbin': 2}, True
    yield 'time-average', {'timeavg': True, 'timebin': '5s'}, True
    yield 'time-chunk', {'ntime': '4s', 'combinescans': True}, True
    yield 'time-only-chunk', {'ntime': 4.0}, True
    yield 'time-zero-bin', {'timeavg': True, 'timebin': '0s'}, True
    yield 'time-invalid-bin', {'timeavg': True, 'timebin': 'bad'}, False
    yield 'time-unused-bad-bin', {'timeavg': False, 'timebin': 'bad'}, True
    yield 'time-negative-bin', {'timeavg': True, 'timebin': '-5s'}, True
    yield 'time-dimensionless', {'timeavg': True, 'timebin': '5'}, True
    yield 'time-frequency-unit', {'timeavg': True, 'timebin': '5Hz'}, True
    yield 'channel-zero-bin', {'channelavg': True, 'chanbin': 0}, True
    yield 'channel-negative-bin', {'channelavg': True, 'chanbin': -1}, True
    yield 'channel-array-bin', {'channelavg': True, 'chanbin': [2]}, True
    yield 'channel-average-vector', {'channelavg': [True], 'chanbin': 2}, False
    yield 'time-average-vector', {'timeavg': [True], 'timebin': '5s'}, False
    yield 'calculate-time-average', {'action': 'calculate', 'timeavg': True, 'timebin': '5s'}, False
    yield 'calculate-invalid-bin', {'action': 'calculate', 'timeavg': True, 'timebin': 'bad'}, False


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
    checked = 0
    with chdir(root):
        for source, column in fixture(root):
            for mode in ('tfcrop', 'rflag'):
                phase = column == 'CPARAM' and mode == 'tfcrop'
                timed = time_fixture(source, column, root / f'timed-{column}-{mode}.cal', phase=phase)
                original = flags(timed)
                reference = None
                base = dict(mode=mode, datacolumn=column, flagbackup=False, extendflags=False)
                if mode == 'tfcrop':
                    base.update(flagdimension='time', timecutoff=1.2, correlation='REAL_SOL1')
                else:
                    base.update(winsize=3, timedev=0.4, timedevscale=1.0, freqdev=1000.0)
                for name, extra, active in controls():
                    expected, actual = [root / f'{engine}-{column}-{mode}-{name}.cal'
                                        for engine in ('casa', 'native')]
                    for path in (expected, actual):
                        shutil.copytree(timed, path)
                    metadata = original_columns(actual)
                    want = outcome(casa.flagdata, expected, **base, **extra)
                    have = outcome(native, actual, **base, **extra)
                    compare_report(want, have)
                    changed = flags(expected)
                    if reference is None:
                        reference = changed
                        assert np.count_nonzero(changed != original) == (5 if mode == 'rflag' else (11 if phase else 0))
                    np.testing.assert_array_equal(changed, reference if active else original)
                    assert_flags(actual, changed)
                    preserved(actual, metadata)
                    checked += 1
                if mode == 'rflag':
                    scoped = scan_fixture(timed, column, root / f'scoped-{column}.cal')
                    for chunk in ({'combinescans': True}, {'ntime': '2s', 'combinescans': True}):
                        expected, actual = [root / f'{engine}-{column}-scans-{checked}.cal'
                                            for engine in ('casa', 'native')]
                        for path in (expected, actual):
                            shutil.copytree(scoped, path)
                        metadata = original_columns(actual)
                        options = dict(mode=mode, datacolumn=column, flagbackup=False, extendflags=False,
                                       timedev=100.0, **chunk)
                        assert outcome(casa.flagdata, expected, **options) == {}
                        assert outcome(native, actual, **options) == {}
                        changed = flags(expected)
                        assert np.count_nonzero(changed != flags(scoped)) == 1
                        assert_flags(actual, changed)
                        preserved(actual, metadata)
                        checked += 1
        ms = root / 'source.ms'
        original = flags(ms)
        for mode, settings, issue in [
            ('tfcrop', {'channelavg': True}, 978),
            ('rflag', {'timeavg': True}, 978),
            ('rflag', {'ntime': '2s'}, 1616),
            ('rflag', {'combinescans': True}, 1616),
        ]:
            result = outcome(native, ms, mode=mode, flagbackup=False, **settings)
            assert result[0] == 'TaskParameterError' and f'#{issue}' in result[1], result
            np.testing.assert_array_equal(flags(ms), original)
    print(f'{checked} paired automatic calibration control cases and four MeasurementSet refusals.')


if __name__ == '__main__':
    main()
