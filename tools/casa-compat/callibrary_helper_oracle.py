#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the CASA and native Python calibration-library helpers."""

import argparse
from contextlib import redirect_stdout
from copy import deepcopy
import inspect
from io import StringIO
from pathlib import Path, PurePath

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np
from casatasks.private.callibrary import applycaltocallib as casa_apply
from casatasks.private.callibrary import callibrary as casa_callibrary
from casars.tasks.private.callibrary import applycaltocallib as native_apply
from casars.tasks.private.callibrary import callibrary as native_callibrary

from blcal_oracle import copy_ms, opened
from calibrater_apply_tool_oracle import same_record
from record_casa_returns import make_cal_ms


CASES = ('cldefinstance', 'clear', 'len', 'addold', 'add', 'parsetorec', 'addrec',
         'list', 'write', 'read', 'compare')


def observe(operation, *args, **kwargs):
    output = StringIO()
    with redirect_stdout(output):
        try:
            result = ('ok', operation(*args, **kwargs))
        except Exception as error:
            result = ('error', type(error).__name__, str(error))
    return result, output.getvalue()


def compare_calls(tools, method, *args, **kwargs):
    left, right = [observe(getattr(tool, method), *args, **kwargs) for tool in tools]
    assert left == right, (method, args, left, right)
    assert tools[0].cld == tools[1].cld, (method, tools[0].cld, tools[1].cld)
    return left


def signatures():
    assert str(inspect.signature(casa_callibrary)) == str(inspect.signature(native_callibrary))
    for method in CASES:
        assert inspect.signature(getattr(casa_callibrary, method)) == inspect.signature(
            getattr(native_callibrary, method)), method
    assert inspect.signature(casa_apply) == inspect.signature(native_apply)


def records(tools):
    assert tuple(map(repr, tools)) == ('<callibrary>', '<callibrary>')
    compare_calls(tools, 'cldefinstance')
    compare_calls(tools, 'len')
    compare_calls(tools, 'addold')
    for tool in tools:
        defaults = tool.cldefinstance()
        assert list(defaults) == ['field', 'intent', 'spw', 'obs', 'fldmap', 'obsmap',
                                  'spwmap', 'antmap', 'tinterp', 'finterp', 'reach']
        defaults['obsmap'].append(5)
        assert not tool.cldefinstance()['obsmap']
    compare_calls(tools, 'addold', '0', '0:2~6', '*CAL*', ['Z.cal', 'A.cal', 'C.cal'],
                  ['nearest', '2'], ['nearest,cubic', ',linear'], [[2, 1], [0]], [False, True])
    compare_calls(tools, 'addold', gaintable='scalar.cal', gainfield='2', interp='linear,nearest',
                  spwmap=[0, 1], calwt=True)
    compare_calls(tools, 'add', ['A.cal', 'D.cal', 'E.cal'], ['1', '2'], '*TARGET*',
                  ['2', '4'], ['0'], 'nearest', ['spline', 'cubic'], ['30s'], False,
                  [[1], [2]], 'nearest', [-1, 1], [[0], [1], [2]])
    compare_calls(tools, 'parsetorec', 'A.cal', '3', 'CAL', '4', '0', 'nearest', 'linear',
                  '60s', True, [0], [1], [2], [3])
    compare_calls(tools, 'add', [])
    compare_calls(tools, 'add', 'bare.cal')
    compare_calls(tools, 'addrec', {}, False)
    for i in range(12):
        compare_calls(tools, 'add', 'ordered.cal', field=str(i))
    for tool in tools:
        entry = tool.cldefinstance()
        mapping = [3, 1]
        entry['spwmap'] = mapping
        original = tool.cld
        assert tool.addrec({'live.cal': entry, 'ignored.cal': {}}, False) is None
        assert 'ignored.cal' not in tool.cld and tool.cld is original
        assert tool.cld['live.cal']['0'] is entry
        assert tool.cld['live.cal']['0']['spwmap'] is mapping
        mapping.append(7)
        original['live.cal']['0']['field'] = '2'
        assert tool.cld['live.cal']['0']['field'] == '2'
        assert tool.cld['live.cal']['0']['spwmap'] == [3, 1, 7]
        other = type(tool)()
        other.cld = deepcopy(tool.cld)
        assert tool.compare(other)
        other.cld['live.cal']['calwt'] = True
        assert not tool.compare(other)
    compare_calls(tools, 'list')
    assert tools[0].len() == tools[1].len() > 6
    print('method signatures, scalar/list forms, repeated records and live aliases passed', flush=True)


def files(work, tools):
    paths = [work/f'{engine}.callib' for engine in ('casa', 'native')]
    for tool, path in zip(tools, paths, strict=True):
        path.write_bytes(b'# prior product\n')
        assert tool.write(str(path), True) is None
    assert paths[0].read_bytes() == paths[1].read_bytes()
    assert paths[0].read_bytes().startswith(b'# prior product\n')
    for tool, path in zip(tools, paths, strict=True):
        assert tool.write(str(path), False) is None
    data = paths[0].read_bytes()
    assert data == paths[1].read_bytes()
    rows = [line for line in data.splitlines() if b"caltable='ordered.cal'" in line]
    assert b"field='10'" in rows[2] and b"field='2'" in rows[4]
    assert all(line.endswith(b' ') for line in data.splitlines())
    reread = [type(tool)() for tool in tools]
    for source in paths:
        compare_calls(reread, 'read', source)
        assert reread[0].len() == tools[0].len()
        assert reread[0].cld == reread[1].cld
        compare_calls(reread, 'clear')
    lines = [" # retained comment", "caltable = 'spaced name.cal' calwt=T field='0'\n",
             "caltable='map.cal' calwt=F obsmap=[2, 1] fldmap='nearest'\n",
             "caltable='map.cal' spwmap=[1,2] calwt=True\n"]
    compare_calls(reread, 'read', lines)
    assert 'spaced,name.cal' in reread[0].cld
    assert reread[0].cld['map.cal']['0']['obsmap'] == [2, 1]
    assert reread[0].cld['map.cal']['calwt'] is False
    old = [helper.cld for helper in reread]
    error, _ = compare_calls(reread, 'read', ["caltable='ok.cal'", "caltable='bad.cal' typo='x'"])
    assert error[0] == 'error'
    assert all(helper.cld == {} and previous != {} for helper, previous in zip(reread, old, strict=True))
    compare_calls(reread, 'write', '')
    compare_calls(reread, 'write', str(work/'empty.callib'))
    compare_calls(reread, 'read', ["caltable='bad.cal' calwt='x' unexpected=2"])
    print('lexical slice order, exact overwrite and append, read variants and failure state passed', flush=True)


def adapt_task_args(work):
    paths = [work/f'{engine}-wrapper.callib' for engine in ('casa', 'native')]
    for called, path in zip((casa_apply, native_apply), paths, strict=True):
        assert called(str(path), False, '2', '4', '*CAL*', ['G.cal', 'T.cal'],
                      ['nearest'], ['nearest,linear'], [[1, 0]], [True, False]) is None
        initial = path.read_bytes()
        assert called(str(path), append=True, gaintable='K.cal') is None
        assert path.read_bytes().startswith(initial)
    assert paths[0].read_bytes() == paths[1].read_bytes()
    for options in ({}, {'gaintable': 'G.cal'}):
        assert observe(casa_apply, '', **options) == observe(native_apply, '', **options)
    assert observe(casa_apply, str(paths[0])) == observe(native_apply, str(paths[0]))
    print('applycaltocallib argument and file semantics passed', flush=True)


def literal_boundary():
    library = native_callibrary()
    library.add('seed.cal')
    expression = "caltable='literal-label'.upper()"
    result, _ = observe(library.read, [expression])
    assert result[0] == 'error' and library.cld == {}
    print('reader accepts record literals without evaluating expressions', flush=True)


def mutated_records(work):
    tools = [casa_callibrary(), native_callibrary()]
    error, _ = compare_calls(tools, 'addrec', ['orphan.cal'], True)
    assert error[1] == 'AttributeError' and all(not tool.cld for tool in tools)
    compare_calls(tools, 'add', 'mutated.cal', field='0')
    for tool in tools:
        del tool.cld['mutated.cal']['0']['intent']
    error, printed = compare_calls(tools, 'list')
    assert error[1] == 'KeyError' and printed.endswith(" field='0' ")
    for tool in tools:
        del tool.cld['mutated.cal']['calwt']
    files = [work/f'mutated-{index}.callib' for index in range(2)]
    outcomes = [observe(tool.write, str(path)) for tool, path in zip(tools, files, strict=True)]
    assert outcomes[0] == outcomes[1] and outcomes[0][0][1] == 'KeyError'
    assert all(path.read_bytes() == b"caltable='mutated.cal' " for path in files)
    compare_calls(tools, 'clear')
    compare_calls(tools, 'add', PurePath('non-string.cal'))
    outcomes = [observe(tool.write, str(path)) for tool, path in zip(tools, files, strict=True)]
    assert outcomes[0] == outcomes[1] and outcomes[0][0][1] == 'TypeError'
    assert all(path.read_bytes() == b'' for path in files)
    print('malformed direct records and partial output match CASA', flush=True)


def interoperability(work):
    from casars.tools import calibrater as native_calibrater

    vis, gains = work/'original.ms', work/'solutions.cal'
    make_cal_ms(str(vis))
    casatasks.gaincal(str(vis), str(gains), refant='A0', solint='inf')
    signatures = []
    applied = []
    for writer, helper in [('casa', casa_callibrary), ('native', native_callibrary)]:
        file = work/f'{writer}-interoperability.callib'
        library = helper()
        library.add(str(gains), field='FLUX', fldmap='FLUX', tinterp='nearest',
                    spwmap=[0], calwt=False)
        library.write(str(file))
        for reader, calibrater in [('casa', casa_calibrater), ('native', native_calibrater)]:
            target = work/f'{writer}-writer-{reader}-reader.ms'
            copy_ms(vis, target)
            tool = calibrater()
            records = tool.parsecallibfile(str(file))
            signatures.append(records)
            assert tool.open(str(target), addcorr=True, addmodel=False)
            assert tool.selectvis(field='FLUX') and tool.validatecallib(records)
            assert tool.setcallib(records) and tool.correct() and tool.close()
            with opened(target) as table:
                fields = table.getcol('FIELD_ID')
                select = fields == 0
                assert select.any() and (~select).any()
                columns = {name: table.getcol(name) for name in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
                np.testing.assert_array_equal(columns['CORRECTED_DATA'][..., ~select],
                                              table.getcol('DATA')[..., ~select])
                applied.append(columns)
    for signature in signatures[1:]:
        same_record(signature, signatures[0])
    for columns in applied[1:]:
        np.testing.assert_array_equal(columns['FLAG'], applied[0]['FLAG'])
        np.testing.assert_array_equal(columns['WEIGHT'], applied[0]['WEIGHT'])
        np.testing.assert_allclose(columns['CORRECTED_DATA'], applied[0]['CORRECTED_DATA'], rtol=1e-3, atol=1e-7)
    assert not np.all(applied[0]['FLAG'])
    print('four-way helper file parsing and selected apply passed', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    work = parser.parse_args().work.resolve()
    work.mkdir()
    assert casatasks.version_string() == '6.7.6.14'
    signatures()
    tools = [casa_callibrary(), native_callibrary()]
    records(tools)
    files(work, tools)
    adapt_task_args(work)
    literal_boundary()
    mutated_records(work)
    interoperability(work)


if __name__ == '__main__':
    main()
