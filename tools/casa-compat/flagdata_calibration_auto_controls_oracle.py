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
    yield 'time-C-string-prefix', {'timeavg': True, 'timebin': '5s\0ignored'}, True
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
    yield 'time-numpy-vector', {'timeavg': np.array(True), 'timebin': '5s'}, False
    yield 'missing-column-before-vector', {'datacolumn': 'DATA', 'timeavg': [True]}, False
    yield 'selection-before-vector', {'antenna': 'unavailable', 'channelavg': [True]}, False
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
    from casars.tasks import flagdata as native, run as run_native
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
                    settings = base | extra
                    want = outcome(casa.flagdata, expected, **settings)
                    have = outcome(native, actual, **settings)
                    compare_report(want, have)
                    changed = flags(expected)
                    if reference is None:
                        reference = changed
                        assert np.count_nonzero(changed != original) == (5 if mode == 'rflag' else (11 if phase else 0))
                    np.testing.assert_array_equal(changed, reference if active else original)
                    assert_flags(actual, changed)
                    preserved(actual, metadata)
                    checked += 1
                if mode == 'rflag' and column == 'CPARAM':
                    def provider(vis, **settings):
                        return run_native('flagdata', vis=vis, **settings)

                    for invalid in ({'channelavg': 'oops'}, {'timeavg': 17}, {'channelavg': [1]}):
                        settings = base | {'flagbackup': True} | invalid
                        compare_report(outcome(casa.flagdata, timed, **settings),
                                       outcome(native, timed, **settings))
                        result = outcome(provider, timed, **settings)
                        assert isinstance(result, tuple) and result[0] == 'TaskParameterError', result
                        assert_flags(timed, original)
                        assert not timed.with_name(f'{timed.name}.flagversions').exists()
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
        ms_source = root / 'source.ms'
        original = flags(ms_source)
        for index, (mode, settings) in enumerate([
            ('rflag', {'channelavg': True}),
            ('rflag', {'timeavg': True}),
            ('rflag', {'ntime': '2s'}),
            ('rflag', {'combinescans': True}),
            ('rflag', {'ntime': -1, 'flagbackup': True}),
            ('rflag', {'ntime': 'bogus', 'flagbackup': True}),
            ('rflag', {'ntime': '2s', 'action': 'none', 'savepars': True,
                       'outfile': 'saved-ms-rflag.txt'}),
        ]):
            expected, actual = [root / f'{engine}-ms-control-{index}.ms'
                                for engine in ('casa', 'native')]
            for path in (expected, actual):
                shutil.copytree(ms_source, path)
            run_settings = {'flagbackup': False, **settings}
            if 'outfile' in run_settings:
                casa_out = root / 'casa-saved-ms-rflag.txt'
                native_out = root / 'native-saved-ms-rflag.txt'
                want = outcome(casa.flagdata, expected, mode=mode, **{**run_settings, 'outfile': str(casa_out)})
                have = outcome(native, actual, mode=mode, **{**run_settings, 'outfile': str(native_out)})
                assert casa_out.read_text() == native_out.read_text()
            else:
                want = outcome(casa.flagdata, expected, mode=mode, **run_settings)
                have = outcome(native, actual, mode=mode, **run_settings)
            compare_report(want, have)
            np.testing.assert_array_equal(flags(actual), flags(expected))
            assert (
                actual.with_name(f'{actual.name}.flagversions').exists()
                == expected.with_name(f'{expected.name}.flagversions').exists()
            )
            if isinstance(want, tuple):
                np.testing.assert_array_equal(flags(actual), original)
            checked += 1
    print(f'{checked} paired automatic calibration and MeasurementSet control cases '
          'and three typed-provider refusals.')


if __name__ == '__main__':
    main()
