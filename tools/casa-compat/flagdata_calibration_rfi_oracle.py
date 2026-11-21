#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare automatic calibration-table flagging and RFlag reports with CASA."""
import argparse
from contextlib import chdir
import os
from pathlib import Path
import shutil

import casatasks as casa
import numpy as np

from flagdata_calibration_oracle import fixture, flags, opened, outcome
from flagmanager_calibration_oracle import assert_flags, original_columns, preserved


def cases(column):
    for mode in ('tfcrop', 'rflag'):
        prefix = dict(mode=mode, datacolumn=column)
        yield f'{mode}-default', prefix
        yield f'{mode}-calculate', dict(prefix, action='calculate')
        yield f'{mode}-plane', dict(prefix, correlation='Sol1')
        yield f'{mode}-snr', dict(prefix, datacolumn='SNR')
        yield f'{mode}-missing', dict(prefix, datacolumn='DATA')
    yield 'tfcrop-frequency', dict(mode='tfcrop', datacolumn=column, timefit='line',
                                    freqfit='line', flagdimension='freq', extendflags=False)
    yield 'rflag-explicit', dict(mode='rflag', datacolumn=column, timedev=1.0,
                                 freqdev=1.2, extendflags=False)
    yield 'rflag-apply-channels', dict(mode='rflag', datacolumn=column, spw='0:3~7')
    rflag = dict(mode='rflag', datacolumn=column, action='calculate')
    yield 'rflag-calculate-unscaled', dict(rflag, timedevscale=2.0, freqdevscale=3.0)
    yield 'rflag-calculate-one-threshold', dict(rflag, freqdev=1.2)
    yield 'rflag-calculate-two-thresholds', dict(rflag, timedev=1.0, freqdev=1.2)
    yield 'rflag-calculate-channels', dict(rflag, spw='0:3~7')
    yield 'rflag-calculate-plane', dict(rflag, correlation='SOL2')
    yield 'rflag-calculate-operator', dict(rflag, correlation='REAL_SOL1')
    yield 'rflag-calculate-imaginary', dict(rflag, correlation='IMAG_SOL1')
    yield 'rflag-calculate-unavailable-plane', dict(rflag, correlation='SOL3')
    yield 'rflag-calculate-undecorated-all', dict(rflag, correlation='ALL')
    yield 'rflag-calculate-bool-window', dict(rflag, winsize=True)
    yield 'rflag-calculate-nonpositive-thresholds', dict(rflag, timedev=0.0, freqdev=-1.0)


def time_fixture(source, column, path, *, phase=False):
    """Make one ordered antenna group with one time impulse in a stored plane."""
    shutil.copytree(source, path)
    with opened(path, True) as table:
        for name in ('OBSERVATION_ID', 'SCAN_NUMBER', 'FIELD_ID', 'SPECTRAL_WINDOW_ID',
                     'ANTENNA1', 'ANTENNA2'):
            values = table.getcol(name)
            values[:] = values[0]
            table.putcol(name, values)
        times = table.getcol('TIME')
        times[:] = times[0] + np.arange(len(times))
        table.putcol('TIME', times)
        data = table.getcol(column)
        data[:] = 1.0 if column == 'CPARAM' else 100.0
        data[0, 4, 4] = -1.0 if phase else data[0, 4, 4] + 30.0
        table.putcol(column, data)
        table.putcol('FLAG', np.zeros_like(data, dtype=bool))
    return path


def time_cases(column):
    settings = dict(mode='rflag', datacolumn=column, extendflags=False, timedev=0.4,
                    timedevscale=1.0, freqdev=1000.0)
    for width, changed in [(1, 0), (2, 0), (3, 5), (5, 9)]:
        yield f'rflag-time-window-{width}', dict(settings, winsize=width), changed
    yield 'rflag-time-window-wrapped', dict(settings, winsize=2**32 + 3), 5
    yield 'rflag-time-window-numpy', dict(settings, winsize=np.int32(5)), 5
    yield 'rflag-time-other-plane', dict(settings, correlation='SOL2'), 0
    yield 'rflag-zero-fitted-threshold', dict(settings, timedev=[]), 0
    yield 'rflag-spectralmin', dict(settings, timedev=1000.0, spectralmin=0.1), None


def scan_fixture(source, column, path):
    """Give two scans on the same field and window different noise levels."""
    shutil.copytree(source, path)
    with opened(path, True) as table:
        scans = table.getcol('SCAN_NUMBER')
        scans[5:] += 1
        table.putcol('SCAN_NUMBER', scans)
        data = table.getcol(column)
        level = 10.0 if column == 'CPARAM' else 100.0
        offsets = np.array([0.0, 2.0, 3.0, -1.0, -2.0, 1.0, 1.5, -1.5])
        data[:, :, :5] = level + offsets[None, :, None] * 8.0
        data[:, :, 5:] = level + offsets[None, :, None] * 0.02
        data[0, 4, 8] += 4.0
        table.putcol(column, data)
    return path


def compare_report(expected, actual):
    if isinstance(expected, dict) and 'nreport' in expected:
        assert actual.keys() == expected.keys() and actual['type'] == expected['type'], (actual, expected)
        assert actual['nreport'] == expected['nreport']
        if expected['nreport'] == 0:
            return
        a, b = actual['report0'], expected['report0']
        assert a.keys() == b.keys() and a['type'] == b['type'] and a['name'] == b['name']
        for kind in ('timedev', 'freqdev'):
            expected_values, actual_values = b[kind], a[kind]
            assert isinstance(actual_values, np.ndarray) and actual_values.dtype == expected_values.dtype
            assert actual_values.shape == expected_values.shape
            np.testing.assert_array_equal(actual_values[:, :2], expected_values[:, :2])
            np.testing.assert_allclose(actual_values[:, 2], expected_values[:, 2], rtol=1e-3, atol=1e-9)
    else:
        assert actual == expected, (actual, expected)


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
    comparisons, edits, safe_refusals = 0, 0, 0
    with chdir(root):
        for source, column in fixture(root):
            timed = time_fixture(source, column, root / f'timed-{column}.cal')
            matrix = [(source, name, settings, None) for name, settings in cases(column)]
            matrix.extend((timed, name, settings, changed)
                          for name, settings, changed in time_cases(column))
            scoped = scan_fixture(timed, column, root / f'scans-{column}.cal')
            rflag = dict(mode='rflag', datacolumn=column, timedev=100.0, extendflags=False)
            matrix.extend([
                (scoped, 'rflag-scan-thresholds', rflag, 1),
                (scoped, 'rflag-field-report', dict(rflag, action='calculate'), 0),
            ])
            if column == 'CPARAM':
                phase = time_fixture(source, column, root / 'phase.cal', phase=True)
                tfcrop = dict(mode='tfcrop', datacolumn=column, flagdimension='time',
                              timecutoff=1.2, extendflags=False)
                matrix.extend([
                    (phase, 'tfcrop-phase-amplitude', tfcrop, 0),
                    (phase, 'tfcrop-phase-real', dict(tfcrop, correlation='REAL_SOL1'), 11),
                ])
            for source_table, name, settings, changed_count in matrix:
                expected, actual = [root / f'{engine}-{column}-{name}.cal' for engine in ('casa', 'native')]
                for path in (expected, actual):
                    shutil.copytree(source_table, path)
                original, metadata = flags(actual), original_columns(actual)
                want = outcome(casa.flagdata, expected, flagbackup=False, **settings)
                have = outcome(native, actual, flagbackup=False, **settings)
                compare_report(want, have)
                changed = flags(expected)
                if name == 'rflag-apply-channels':
                    # CASA's automatic extension can write outside this slice.
                    outside = np.count_nonzero(changed[:, :3] != original[:, :3])
                    assert outside == (1 if column == 'CPARAM' else 3)
                    changed[:, :3] = original[:, :3]
                assert_flags(actual, changed)
                preserved(actual, metadata)
                delta = int(np.count_nonzero(changed != original))
                if settings.get('action') == 'calculate':
                    assert delta == 0
                if changed_count is not None:
                    assert delta == changed_count, (name, delta, changed_count)
                print(f'{column} {name}: {delta} changed cells', flush=True)
                comparisons += 1
                edits += delta
            invalid_controls = (
                ({'winsize': 0}, 'winsize', 'winsize must be positive'),
                ({'winsize': -1}, 'winsize', 'winsize must be positive'),
                ({'timedev': 0.4, 'timedevscale': 0.0}, 'timedevscale',
                 'timedevscale must be positive when its threshold is positive'),
                ({'freqdev': 1.0, 'freqdevscale': -1.0}, 'freqdevscale',
                 'freqdevscale must be positive when its threshold is positive'),
            )
            for invalid, parameter, message in invalid_controls:
                prior, metadata = flags(timed), original_columns(timed)
                result = outcome(native, timed, mode='rflag', datacolumn=column,
                                 flagbackup=False, **invalid)
                assert result == ('TaskParameterError',
                                  f"flagdata refused its parameters (parameter '{parameter}'): {message}"), result
                assert_flags(timed, prior)
                preserved(timed, metadata)
                safe_refusals += 1
    print(f'{comparisons} paired automatic calibration cases and {edits} changed cells. '
          f'{safe_refusals} safe refusals verified.')


if __name__ == '__main__':
    main()
