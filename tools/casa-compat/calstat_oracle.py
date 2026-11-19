#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public calibration statistics and table views with CASA 6.7.6.14."""

import argparse
import inspect
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from record_casa_returns import make_cal_ms


def compare(actual, expected):
    assert list(actual) == list(expected), (actual.keys(), expected.keys())
    for column, reference in expected.items():
        result = actual[column]
        assert list(result) == list(reference)
        for field, want in reference.items():
            assert type(result[field]) is float
            if field == 'npts':
                assert result[field] == want
            else:
                np.testing.assert_allclose(result[field], want, rtol=3e-7, atol=2e-8,
                                           err_msg=f'{column}.{field}')


def refuse(call, message):
    try:
        call()
    except RuntimeError as error:
        assert message in str(error), error
    else:
        raise AssertionError(f'Expected refusal for {message}')


def legacy_table(path):
    tool = table()
    describe = {
        'GAIN': {'valueType': 'complex', 'ndim': 2, 'shape': [2, 2]},
        'CORRECTED_DATA': {'valueType': 'complex', 'ndim': 2, 'shape': [2, 2]},
        'MODEL_DATA': {'valueType': 'complex', 'ndim': 2, 'shape': [2, 2]},
        'VECTOR': {'valueType': 'double', 'ndim': 1, 'shape': [2]},
        'NAN': {'valueType': 'float'},
        'RAGGED': {'valueType': 'float', 'ndim': 2},
        'NO_DATA': {'valueType': 'int64'},
    }
    assert tool.create(str(path), describe, nrow=7)
    shape = (2, 2, 7)
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)/13.0
    gains = values + 1j * (values - 0.9)
    for name, scale in [('GAIN', 1), ('CORRECTED_DATA', 2), ('MODEL_DATA', 3)]:
        tool.putcol(name, gains*scale)
    exact = np.array([5_084_546_400.0 + np.arange(7)*80.0, np.arange(7)*0.7])
    tool.putcol('VECTOR', exact)
    tool.putcol('NAN', np.array([0., 3., 1., np.nan, 4., 8., 9.], dtype=np.float32))
    for row in range(7):
        tool.putcell('RAGGED', row, np.ones((2, 2+(row % 2)), dtype=np.float32))
    tool.close()


def run(root, native, native_table):
    source = root/'source.ms'
    gain, delay, legacy = (root/f'{name}.cal' for name in ('gain', 'delay', 'legacy'))
    make_cal_ms(str(source))
    casa.gaincal(str(source), str(gain), refant='A0', solint='int', minsnr=0.)
    casa.gencal(str(source), str(delay), 'sbd', parameter=[2.5])
    tool = table()
    tool.open(str(gain), nomodify=False)
    flags = tool.getcol('FLAG')
    flags[..., ::3] = True
    tool.putcol('FLAG', flags)
    sample = np.arange(flags.size, dtype=np.float32).reshape(flags.shape)
    gain_values = 0.5 + 0.069*sample + 1j*(0.7 - 0.021*sample)
    tool.putcol('CPARAM', gain_values)
    tool.close()
    legacy_table(legacy)
    spaced = root/'legacy with trailing space.cal '
    spaced.symlink_to(legacy, target_is_directory=True)

    successes = [
        (gain, axis, 'CPARAM')
        for axis in ('amp', 'amplitude', 'phase', 'real', 'imag', 'imaginary')
    ] + [
        (gain, 'flag', 'unused'),
        (gain, 'TIME', 'unused'),
        (gain, 'antenna1', 'unused'),
        (gain, 'paramerr', 'unused'),
        (gain, 'snr', 'unused'),
        (gain, 'amplitude', 'flag'),
        (delay, 'FPARAM', 'unused'),
        (delay, 'amplitude', 'fparam'),
        (legacy, 'amplitude', 'gain'),
        (legacy, 'real', 'corrected'),
        (legacy, 'phase', 'model'),
        (legacy, 'imaginary', 'corrected_data'),
        (legacy, 'VECTOR', 'unused'),
    ]
    for path, axis, datacolumn in successes:
        expected = casa.calstat(str(path), axis, datacolumn)
        if path == gain and datacolumn == 'CPARAM':
            assert expected['CPARAM']['npts'] == flags.size > np.count_nonzero(~flags)
        compare(native.calstat(caltable=str(path), axis=axis, datacolumn=datacolumn), expected)
    compare(native.calstat(str(legacy)), casa.calstat(str(legacy)))
    compare(native.calstat(str(spaced)), casa.calstat(str(spaced)))
    home_relative = f'~/{os.path.relpath(legacy, Path.home())}'
    compare(native.calstat(home_relative), casa.calstat(home_relative))
    compare(native.calstat(str(gain), 'phase', 'cparam'), casa.calstat(str(gain), 'phase', 'cparam'))

    expected_nan = casa.calstat(str(legacy), 'NAN')['NAN']
    actual_nan = native.calstat(str(legacy), 'NAN')['NAN']
    assert expected_nan['quartile'] == 7.0
    assert (actual_nan['min'], actual_nan['max'], actual_nan['npts']) == (0., 9., 7.)
    assert all(np.isnan(value) for key, value in actual_nan.items()
               if key not in ('min', 'max', 'npts'))

    errors = [
        ((str(gain),), 'Table column GAIN is unknown'),
        ((str(gain), 'FPARAM'), 'Table column FPARAM is unknown'),
        ((str(gain), 'cparam'), 'complex_value must be amp'),
        ((str(gain), 'AMP', 'CPARAM'), 'Table column AMP is unknown'),
        ((str(gain), ''), 'Table column  is unknown'),
        ((str(gain), 'amp', ' CPARAM '), 'Table column  CPARAM  is unknown'),
        ((str(legacy), 'NO_DATA'), 'Sorry, no support for Int64 scalar columns'),
        ((str(legacy), 'RAGGED'), 'shapes not conforming'),
    ]
    for parameters, prefix in errors:
        for task in (casa.calstat, native.calstat):
            refuse(lambda: task(*parameters), prefix)

    for task in (casa.calstat, native.calstat):
        try:
            task()
        except AssertionError as error:
            assert 'the caltable parameter must be a path that exists' in str(error)
        else:
            raise AssertionError('Accepted a missing caltable')

    invalid_types = [
        ([str(gain)], 'amplitude', 'CPARAM'),
        (str(gain), 1, 'CPARAM'),
        (str(gain), 'amplitude', 1),
    ]
    for parameters in invalid_types:
        for task in (casa.calstat, native.calstat):
            try:
                task(*parameters)
            except (AssertionError, TypeError, ValueError):
                pass
            else:
                raise AssertionError(f'Incorrectly accepted {parameters}')

    expected_tool, result_tool = table(), native_table()
    for instance in (expected_tool, result_tool):
        assert instance.open(str(legacy))
    compare(result_tool.statistics('VECTOR'), expected_tool.statistics('VECTOR'))
    compare(result_tool.statistics('GAIN', 'phase'), expected_tool.statistics('GAIN', 'phase'))
    selected = [1, 5, 3]
    a, b = expected_tool.selectrows(selected), result_tool.selectrows(selected)
    try:
        compare(b.statistics('GAIN', 'real'), a.statistics('GAIN', 'real'))
        compare(b.statistics('VECTOR'), a.statistics('VECTOR'))
    finally:
        for instance in (a, b, expected_tool, result_tool):
            instance.close()

    expected = casa.calstat(str(legacy), 'VECTOR')['VECTOR_0']
    cast = (5_084_546_400.0 + np.arange(7)*80.0).astype(np.float32).astype(np.float64)
    np.testing.assert_allclose(expected['var'], cast.var(ddof=1), rtol=2e-7)
    samples = cast.astype(np.float32)
    assert expected['sumsq'] == np.square(samples).sum(dtype=np.float64)
    print(f'{len(successes) + 4} public statistics comparisons, {len(errors) + len(invalid_types) + 1} errors, '
          'four table-tool comparisons, NaN behavior and independent moment checks passed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    from casars.tools import table as native_table

    assert casa.version_string() == '6.7.6.14'
    expected = inspect.signature(casa.calstat)
    actual = inspect.signature(native.calstat)
    assert [parameter.replace(annotation=inspect.Parameter.empty)
            for parameter in actual.parameters.values()] == list(expected.parameters.values())
    run(root, native, native_table)


if __name__ == '__main__':
    main()
