#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the Python uvsub task and ms tool with pinned CASA."""

import argparse
import inspect
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import ms as casa_ms, table
import numpy as np

from applycal_mms_oracle import referenced_tables
from bandpass_oracle import source_data
from calibration_scratch_oracle import history_count, prepare, state


def compare_data(actual, expected):
    left, left_virtual = actual
    right, right_virtual = expected
    assert left.keys() == right.keys()
    for column in left:
        if column == 'CORRECTED_DATA':
            np.testing.assert_allclose(left[column], right[column], rtol=2e-6, atol=2e-7)
        else:
            np.testing.assert_array_equal(left[column], right[column], err_msg=column)
    np.testing.assert_equal(left_virtual, right_virtual)
    return float(np.max(np.abs(left['CORRECTED_DATA']-right['CORRECTED_DATA'])))


def task_cases(root, source, native):
    results = {}
    cases = [
        ('observed-unit', {}, False, False),
        ('observed-reverse', {}, True, False),
        ('float-observed', {'float_data': 'only'}, False, False),
        ('float-precedence', {'float_data': 'also'}, True, False),
        ('corrected-unit', {'corrected': True}, False, False),
        ('observed-and-scratch', {'model': True}, False, False),
        ('corrected-and-scratch', {'corrected': True, 'model': True}, True, False),
        ('virtual-only', {'virtual': True}, False, False),
        ('virtual-precedence', {'corrected': True, 'model': True, 'virtual': True}, True, False),
        ('one-virtual-field', {'model': True, 'virtual': True}, False, True),
    ]
    for label, fixture, reverse, remove_one_field in cases:
        states = []
        for engine, task in [('native', native.uvsub), ('casa', casa.uvsub)]:
            path = root/f'{label}-{engine}.ms'
            prepare(source, path, **fixture)
            if remove_one_field:
                casa.delmod(str(path), field='1')
            assert task(str(path), reverse) is None
            assert history_count(path, 'uvsub') == 1
            states.append(state(path))
        results[label] = compare_data(*states)
    return results


def tool_cases(root, source, native_ms):
    states = []
    for engine, factory in [('native', native_ms), ('casa', casa_ms)]:
        path = root/f'tool-{engine}.ms'
        prepare(source, path, model=True, corrected=True, virtual=True)
        tool = factory()
        assert inspect.signature(tool.uvsub) == inspect.signature(casa_ms().uvsub)
        assert inspect.signature(tool.open) == inspect.signature(casa_ms().open)
        assert tool.uvsub() is False
        assert tool.open(str(path), True, True, True) is True
        assert tool.uvsub() is False
        assert tool.done() is True
        assert tool.done() is False
        initial, _ = state(path)
        assert tool.open(str(path), nomodify=False, lock=True, check=True) is True
        assert tool.uvsub() is True
        assert tool.close() is True
        assert tool.open(str(path), nomodify=False) is True
        assert tool.uvsub(reverse=True) is True
        assert tool.uvsub() is True
        assert tool.close() is True
        first = state(path)
        assert tool.open(str(path), nomodify=False) is True
        assert tool.uvsub(reverse=True) is True
        assert tool.close() is True
        restored, _ = state(path)
        np.testing.assert_allclose(restored['CORRECTED_DATA'], initial['CORRECTED_DATA'],
                                   rtol=1e-7, atol=1e-7)
        assert tool.close() is False
        assert history_count(path, 'uvsub') == 0
        states.append(first)

        fix = table()
        fix.open(str(path), nomodify=False)
        try:
            fix.putcell('FIELD_ID', 0, 12345)
        finally:
            fix.close()
        try:
            tool.open(str(path), check=True)
        except RuntimeError as error:
            assert 'FIELD_ID' in str(error)
        else:
            raise AssertionError(f'{engine} accepted an invalid FIELD_ID')
        assert tool.close() is False
        assert tool.open(str(path), check=False) is True
        assert tool.close() is True
    return {'tool-forward-reverse-and-access': compare_data(*states)}


def mms_case(root, source, native):
    outputs = []
    for engine, task in [('native', native.uvsub), ('casa', casa.uvsub)]:
        path = root/f'partitioned-{engine}.mms'
        casa.partition(str(source), str(path), separationaxis='scan', numsubms=2,
                       disableparallel=True, flagbackup=False, datacolumn='data')
        parts = referenced_tables(path)
        assert len(parts) == 2
        assert task(vis=str(path)) is None
        outputs.append([state(part) for part in parts])
    return {'mms-default': [compare_data(actual, expected)
                            for actual, expected in zip(*outputs, strict=True)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    from casars.tools import ms as native_ms

    expected = inspect.signature(casa.uvsub)
    actual = inspect.signature(native.uvsub)
    assert actual.parameters.keys() == expected.parameters.keys()
    for name in expected.parameters:
        assert actual.parameters[name].default == expected.parameters[name].default
        assert actual.parameters[name].kind == expected.parameters[name].kind
    root = args.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source, stokes='XX XY YX YY')
    got = task_cases(root, source, native)
    got.update(tool_cases(root, source, native_ms))
    got.update(mms_case(root, source, native))
    (root/'results.json').write_text(json.dumps(got, indent=2)+'\n')


if __name__ == '__main__':
    main()
