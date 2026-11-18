#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check clearcal and delmod against CASA's public Python tasks."""

import argparse
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import casatasks as casa
from casatools import calibrater, table
import numpy as np

from applycal_oracle import copy_ms
from applycal_mms_oracle import referenced_tables
from bandpass_oracle import source_data


SCRATCH = ('CORRECTED_DATA', 'MODEL_DATA')
MAIN = ('DATA', 'FLOAT_DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA')


def prepare(source, target, corrected=False, model=False, virtual=False, float_data=None,
            direct_scratch=False):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        for present, column in zip((corrected, model), SCRATCH):
            if column in tool.colnames():
                tool.removecols([column])
            if present:
                description = tool.getcoldesc('DATA')
                if direct_scratch:
                    manager = next(dm for dm in tool.getdminfo().values()
                                   if dm['TYPE'] == 'StandardStMan')
                    description.update(option=5, shape=list(tool.getcell('DATA', 0).shape),
                                       dataManagerType=manager['TYPE'], dataManagerGroup=manager['NAME'])
                tool.addcols({column: description})
                tool.putcol(column, np.full_like(tool.getcol('DATA'), 13+7j))
        weights = tool.getcol('WEIGHT')
        tool.putcol('WEIGHT', np.full_like(weights, 13.))
        tool.putcol('SIGMA', np.full_like(weights, .5))
        flags = tool.getcol('FLAG')
        flags[1, 3, ::4] = True
        tool.putcol('FLAG', flags)
        tool.putcol('STATE_ID', tool.getcol('FIELD_ID'))
        if float_data is not None:
            description = tool.getcoldesc('DATA')
            description.update(valueType='float', dataManagerType='StandardStMan',
                               dataManagerGroup='FloatData')
            tool.addcols({'FLOAT_DATA': description})
            tool.putcol('FLOAT_DATA', np.ascontiguousarray(tool.getcol('DATA').real + 7.,
                                                        dtype=np.float32))
            if float_data == 'only':
                tool.removecols('DATA')
    finally:
        tool.close()
    tool.open(str(target/'STATE'), nomodify=False)
    try:
        if tool.nrows() < 2:
            tool.addrows(2-tool.nrows())
        tool.putcell('OBS_MODE', 0, 'CALIBRATE_FLUX#ON_SOURCE')
        tool.putcell('OBS_MODE', 1, 'CALIBRATE_PHASE#ON_SOURCE')
    finally:
        tool.close()
    if virtual:
        for field in (0, 1):
            casa.setjy(str(target), field=str(field), standard='manual',
                       fluxdensity=[2.+field, 0., 0., 0.], usescratch=False)


def state(path):
    tool = table()
    tool.open(str(path))
    try:
        arrays = {name: tool.getcol(name) for name in (*MAIN, *SCRATCH)
                  if name in tool.colnames()}
        pointers = {key: tool.getkeyword(key) for key in tool.keywordnames()
                    if key.startswith(('definedmodel_field_', 'model_'))}
    finally:
        tool.close()
    tool.open(str(path/'SOURCE'))
    try:
        source = {key: tool.getkeyword(key) for key in tool.keywordnames()
                  if key.startswith(('definedmodel_field_', 'model_'))}
        records = [tool.getcell('SOURCE_MODEL', i) if tool.iscelldefined('SOURCE_MODEL', i) else None
                   for i in range(tool.nrows())] if 'SOURCE_MODEL' in tool.colnames() else None
    finally:
        tool.close()
    return arrays, (pointers, source, records)


def history_count(path, task):
    tool = table()
    tool.open(str(path/'HISTORY'))
    try:
        return np.count_nonzero(tool.getcol('MESSAGE') == f'taskname={task}')
    finally:
        tool.close()


def compare(root, source, native, name, task, fixture, options, refused=False):
    expected, actual = [], []
    for engine, reports in [('casa', expected), ('native', actual)]:
        api = casa if engine == 'casa' else native
        ms = root/f'{name}-{engine}.ms'
        prepare(source, ms, **fixture)
        before, _ = state(ms)
        positional = options if isinstance(options, tuple) else ()
        keywords = options if isinstance(options, dict) else {}
        try:
            assert getattr(api, task)(str(ms), *positional, **keywords) is None
        except RuntimeError:
            if not refused:
                raise
        else:
            assert not refused, (name, engine)
        after, virtual = state(ms)
        for column in MAIN:
            if column in before:
                np.testing.assert_array_equal(after[column], before[column], err_msg=column)
        reports.extend([after, virtual])
        assert history_count(ms, task) == int(not refused)
    np.testing.assert_equal(actual, expected)
    return {'returned_none': not refused, 'matched_columns': sorted(actual[0])}


def compare_mms(root, source, native, name, task, fixture, options):
    results = []
    for engine, api in [('casa', casa), ('native', native)]:
        ms = root/f'{name}-{engine}.ms'
        mms = root/f'{name}-{engine}.mms'
        prepare(source, ms, **fixture)
        casa.partition(str(ms), str(mms), separationaxis='scan', numsubms=2,
                       disableparallel=True, flagbackup=False, datacolumn='all')
        parts = referenced_tables(mms)
        assert len(parts) == 2
        assert getattr(api, task)(str(mms), **options) is None
        results.append([(state(part), history_count(part, task)) for part in parts])
    np.testing.assert_equal(*results)
    return {'partition_history_entries': [int(count) for _, count in results[1]]}


def storage_layout(path):
    tool = table()
    tool.open(str(path))
    try:
        return {str(manager['NAME']): manager for manager in tool.getdminfo().values()}
    finally:
        tool.close()


def deletion_layouts(root, source, native):
    outputs = {}
    for layout in ('shared', 'compressed'):
        states = []
        for engine, api in [('casa', casa), ('native', native)]:
            path = root/f'delete-{layout}-{engine}.ms'
            prepare(source, path)
            if layout == 'compressed':
                cb = calibrater()
                assert cb.open(str(path), compress=True, addcorr=True, addmodel=True)
                assert cb.close()
            else:
                tool = table()
                tool.open(str(path), nomodify=False)
                try:
                    manager = next(dm for dm in tool.getdminfo().values()
                                   if dm['TYPE'] == 'StandardStMan')
                    description = tool.getcoldesc('DATA')
                    description.update(dataManagerType=manager['TYPE'], dataManagerGroup=manager['NAME'])
                    tool.addcols({'MODEL_DATA': description})
                    tool.putcol('MODEL_DATA', np.full_like(tool.getcol('DATA'), 13+7j))
                finally:
                    tool.close()
            before = storage_layout(path)
            unrelated = {name: manager for name, manager in before.items() if
                         not any(column.startswith('MODEL_DATA') for column in manager['COLUMNS'])}
            prefixes = tuple(f'table.f{manager["SEQNR"]}' for manager in unrelated.values())
            untouched_files = {file: file.read_bytes() for file in path.iterdir() if file.is_file()
                               and any(file.name == prefix or file.name == prefix+'i'
                                       or file.name.startswith(prefix+'_') for prefix in prefixes)}
            assert api.delmod(str(path), otf=False, scr=True) is None
            after = storage_layout(path)
            assert all(not column.startswith('MODEL_DATA') for manager in after.values()
                       for column in manager['COLUMNS'])
            for name, manager in unrelated.items():
                np.testing.assert_equal(after[name], manager)
            for file, initial in untouched_files.items():
                assert file.read_bytes() == initial, file
            states.append(state(path))
        np.testing.assert_equal(*states)
        outputs[f'delete-{layout}-layout'] = {'untouched_managers_and_values': True}
    return outputs


def retained_lock_and_concurrent_cleanup(root, source):
    lock_code = """
import sys
from casatools import table
tool = table()
tool.open(sys.argv[1], lockoptions={'option': 'user'}, nomodify=True)
tool.lock(write=False)
assert tool.haslock(write=False)
print('held', flush=True)
sys.stdin.readline()
tool.close()
"""
    task_code = """
import sys
from casars import tasks
field = sys.argv[2]
print('started', flush=True)
tasks.delmod(sys.argv[1], field=field, otf=bool(field), scr=not field)
"""
    results = {}
    for label, required_fields in [('retained-reader', ['']), ('concurrent-removal', ['0', '1'])]:
        path = root/f'{label}.ms'
        prepare(source, path, model=True, virtual=bool(required_fields[0]))
        lock = subprocess.Popen([sys.executable, '-c', lock_code, str(path)], text=True,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        writers = []
        try:
            assert lock.stdout.readline().strip() == 'held', lock.stderr.read()
            for field in required_fields:
                writer = subprocess.Popen([sys.executable, '-c', task_code, str(path), field],
                                          text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                writers.append(writer)
                assert writer.stdout.readline().strip() == 'started', writer.stderr.read()
            time.sleep(.25)
            assert all(writer.poll() is None for writer in writers)
        finally:
            if lock.poll() is None:
                lock.communicate('\n', timeout=15)
            for writer in writers:
                _, error = writer.communicate(timeout=25)
                assert writer.returncode == 0, error
        arrays, virtual = state(path)
        if required_fields[0]:
            assert virtual[0] == {} and virtual[1] == {}
            assert 'MODEL_DATA' in arrays
        else:
            assert 'MODEL_DATA' not in arrays
        results[label] = {'waited_for_reader_and_completed': True}
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    options = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(options.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    for name in ('clearcal', 'delmod'):
        expected = inspect.signature(getattr(casa, name))
        actual = inspect.signature(getattr(native, name))
        assert list(actual.parameters) == list(expected.parameters)
        for parameter in expected.parameters:
            assert actual.parameters[parameter].kind == expected.parameters[parameter].kind
            assert actual.parameters[parameter].default == expected.parameters[parameter].default
        for api in (casa, native):
            for path_arguments in ((), (str(options.work/'missing.ms'),)):
                try:
                    getattr(api, name)(*path_arguments)
                except AssertionError as error:
                    assert 'the vis parameter must be a path that exists' in str(error)
                else:
                    raise AssertionError((name, api, path_arguments))
    root = options.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source, stokes='XX XY YX YY')
    scratch = {'corrected': True, 'model': True}
    virtual = {**scratch, 'virtual': True}
    results = {}
    for name, fixture, arguments, refused in [
        ('create-corrected', dict(virtual=True), dict(field='0', spw='0:2~4'), False),
        ('create-both', {}, dict(field='MISSING', addmodel=True), False),
        ('keep-existing-model', dict(model=True), dict(addmodel=False), False),
        ('selected-reset', virtual, dict(field='0', spw='0:2~4'), False),
        ('corrected-only-reset-virtual', dict(corrected=True, virtual=True),
         dict(field='0', spw='0:2~4'), False),
        ('selected-direct-standard-manager', {**scratch, 'direct_scratch': True},
         dict(field='0', spw='0:2~4'), False),
        ('channel-envelope', scratch, dict(field='0', spw='0:0~2;7~9'), False),
        ('strided-channels', scratch, dict(field='0', spw='0:0~9^2'), False),
        ('selected-intent', scratch, dict(intent='CALIBRATE_PHASE*'), False),
        ('bracketed-selectors', scratch, ('[0]', '[0:1~3]', '[CALIBRATE_FLUX*]', False), False),
        ('empty-bracket-selectors', scratch, ('[]', '[]', '[]', False), False),
        ('create-from-float-data', dict(float_data='only'), dict(addmodel=True), False),
        ('reset-with-float-and-complex', {**scratch, 'float_data': 'also'},
         dict(field='0', spw='0:1~3'), False),
        ('invalid-spectral-window', scratch, dict(spw='50'), True),
        ('invalid-intent', scratch, dict(intent='MISSING*'), True),
        ('create-model-reset-corrected', dict(corrected=True, virtual=True),
         dict(field='1', spw='0:2~4', addmodel=True), False),
        ('invalid-selection-after-model-creation', dict(corrected=True),
         dict(field='MISSING', addmodel=True), True),
    ]:
        results[name] = compare(root, source, native, name, 'clearcal', fixture, arguments, refused)
    for name, arguments, refused in [
        ('delete-scratch', dict(field='0', otf=False, scr=True), False),
        ('delete-one-virtual-field', dict(field='0'), False),
        ('delete-bracketed-field', (True, '[0]', False), False),
        ('delete-empty-bracket', (True, '[]', False), False),
        ('delete-all-virtual-fields', {}, False),
        ('delete-both', dict(field='1', otf=True, scr=True), False),
        ('delete-neither', dict(field='MISSING', otf=False, scr=False), False),
        ('delete-scratch-ignores-field', dict(field='MISSING', otf=False, scr=True), False),
        ('invalid-virtual-field-before-scratch-removal',
         dict(field='MISSING', otf=True, scr=True), True),
        ('invalid-numeric-field-before-scratch-removal',
         dict(field='999', otf=True, scr=True), True),
    ]:
        results[name] = compare(root, source, native, name, 'delmod', virtual, arguments, refused)
    for name, task, fixture, arguments in [
        ('mms-create', 'clearcal', {}, dict(field='MISSING', addmodel=True)),
        ('mms-select', 'clearcal', scratch, dict(field='0')),
        ('mms-no-selection-match', 'clearcal', scratch, dict(field='MISSING')),
        ('mms-delete-scratch', 'delmod', scratch, dict(field='0', otf=False, scr=True)),
        ('mms-invalid-field', 'delmod', scratch, dict(field='MISSING', scr=True)),
    ]:
        results[name] = compare_mms(root, source, native, name, task, fixture, arguments)
    results.update(deletion_layouts(root, source, native))
    results.update(retained_lock_and_concurrent_cleanup(root, source))
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
