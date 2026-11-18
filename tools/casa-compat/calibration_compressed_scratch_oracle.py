#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Read and reset CASA-written compressed scratch visibility columns."""

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import calibrater, table
import numpy as np

from bandpass_oracle import source_data
from calibration_scratch_oracle import prepare, state, storage_layout


COLS = ('MODEL_DATA', 'CORRECTED_DATA')
SUFFIXES = ('_COMPRESSED', '_OFFSET', '_SCALE')
AUXILIARY = 'UNCHANGED_ARRAY'


def compress(path, variant, drop, *, single_dish=False, mixed=False):
    cb = calibrater()
    assert cb.open(str(path), compress=True, addcorr=True, addmodel=True)
    assert cb.close()
    tool = table()
    tool.open(str(path), nomodify=False)
    try:
        shape = tool.getcol('DATA').shape
        shape_index = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
        scale = (shape_index % 11) / 10
        start = -10 if variant == 'broad' else 20
        width = 20 if variant == 'broad' else 2
        data = (start + width*scale + 1j*(start + width*(1-scale))).astype(np.complex64)
        if single_dish or mixed:
            for name in COLS:
                rebind(tool, name, single_dish, mixed and name == 'CORRECTED_DATA')
        if single_dish:
            data[:, ::2, :] = data[:, ::2, :].real
        for name in COLS:
            tool.putcol(name, data)
        if drop:
            for suffix in ('', *SUFFIXES):
                tool.removecols([drop+suffix])
    finally:
        tool.close()


def manager(tool, name):
    return next(group for group in tool.getdminfo().values()
                if name in group['COLUMNS'])


def rebind(tool, name, single_dish, mixed):
    description = tool.getcoldesc(name)
    virtual = manager(tool, name)
    tool.removecols([name])
    if mixed:
        for suffix in ('_OFFSET', '_SCALE'):
            column = name+suffix
            scalar = tool.getcoldesc(column)
            scalar['dataManagerGroup'] = 'SSM'
            tool.removecols([column])
            assert tool.addcols({column: scalar})
        unrelated = tool.getcoldesc('UVW')
        unrelated.update(dataManagerType='StandardStMan', dataManagerGroup='SSM', keywords={})
        assert tool.addcols({AUXILIARY: unrelated})
        tool.putcol(AUXILIARY, np.arange(3*tool.nrows(), dtype=np.float64).reshape(3, -1))
        assert AUXILIARY in manager(tool, name+'_SCALE')['COLUMNS']
    if single_dish:
        virtual['TYPE'] = description['dataManagerType'] = 'CompressComplexSD'
    description['keywords'] = {}
    virtual.pop('SEQNR')
    assert tool.addcols({name: description}, virtual)
    assert manager(tool, name)['TYPE'] == virtual['TYPE']


def mixed_storage(path):
    tool = table()
    tool.open(str(path))
    try:
        group = manager(tool, AUXILIARY)
        assert 'CORRECTED_DATA_SCALE' in group['COLUMNS']
        assert group['TYPE'] == 'StandardStMan'
        arrays = tool.getcol(AUXILIARY)
    finally:
        tool.close()
    file = (path / f"table.f{group['SEQNR']}").stat()
    return (file.st_ino, file.st_size), arrays


def physical(path):
    tool = table()
    tool.open(str(path))
    try:
        return {name: tool.getcol(name) for name in tool.colnames()
                if name.startswith(COLS) and name.endswith(SUFFIXES)}
    finally:
        tool.close()


def compare(root, source, native, label, task_name, layout, drop, arguments,
            *, single_dish=False, mixed=False):
    results = []
    for engine, api in [('native', native), ('casa', casa)]:
        path = root/f'{label}-{engine}.ms'
        prepare(source, path)
        compress(path, layout, drop, single_dish=single_dish, mixed=mixed)
        mixed_before = mixed_storage(path) if mixed else None
        before = physical(path)
        managers = {name: value['TYPE'] for name, value in storage_layout(path).items()}
        assert getattr(api, task_name)(str(path), **arguments) is None
        after = physical(path)
        assert before.keys() == after.keys()
        for name, manager in managers.items():
            assert storage_layout(path)[name]['TYPE'] == manager
        if drop == 'CORRECTED_DATA':
            np.testing.assert_equal(after, before)
        if mixed:
            identity, arrays = mixed_storage(path)
            np.testing.assert_array_equal(arrays, mixed_before[1])
            if engine == 'native':
                assert identity == mixed_before[0], 'selected scalar save rebuilt the whole group'
        results.append((state(path), after))
    np.testing.assert_equal(*results)
    return {'packed_scratch_columns': sorted(results[0][1]), 'casa_values_exact': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks
    root = args.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source, stokes='XX XY YX YY')
    cases = [
        ('full', 'clearcal', 'narrow', None, {}),
        ('partial-within-scale', 'clearcal', 'broad', None, dict(field='0', spw='0:2~4')),
        ('partial-rescale', 'clearcal', 'narrow', None, dict(field='0', spw='0:2~4')),
        ('add-corrected', 'clearcal', 'narrow', 'CORRECTED_DATA', dict(field='0')),
        ('add-model', 'clearcal', 'narrow', 'MODEL_DATA', dict(field='1', spw='0:2~3', addmodel=True)),
        ('subtract-compressed', 'uvsub', 'broad', None, {}),
        ('add-compressed', 'uvsub', 'broad', None, dict(reverse=True)),
        ('subtract-model-from-data', 'uvsub', 'broad', 'CORRECTED_DATA', {}),
    ]
    outcomes = {label: compare(root, source, tasks, label, task, layout, drop, arguments)
                for label, task, layout, drop, arguments in cases}
    for label, task, arguments, mixed in [
        ('single-dish-full', 'clearcal', {}, False),
        ('single-dish-partial', 'clearcal', dict(field='0', spw='0:2~4'), False),
        ('single-dish-subtract-mixed', 'uvsub', {}, True),
        ('mixed-selected', 'clearcal', dict(field='0', spw='0:2~4'), True),
    ]:
        outcomes[label] = compare(root, source, tasks, label, task, 'broad', None, arguments,
                                  single_dish=label.startswith('single-dish'), mixed=mixed)
    (root/'results.json').write_text(json.dumps(outcomes, indent=2)+'\n')


if __name__ == '__main__':
    main()
