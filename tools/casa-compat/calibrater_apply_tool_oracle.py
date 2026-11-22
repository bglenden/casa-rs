#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pinned CASA parity for the calibrater's library and apply workflow."""

import argparse
from contextlib import chdir
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater
import numpy as np


def same_record(actual, expected):
    if isinstance(expected, dict):
        assert isinstance(actual, dict)
        assert tuple(actual) == tuple(expected), (tuple(actual), tuple(expected))
        for name, value in expected.items():
            same_record(actual[name], value)
    elif isinstance(expected, np.ndarray):
        assert isinstance(actual, np.ndarray)
        assert actual.dtype == expected.dtype
        np.testing.assert_array_equal(actual, expected)
    else:
        assert type(actual) is type(expected)
        assert actual == expected


def result(operation, *args, **options):
    try:
        return True, operation(*args, **options)
    except Exception as error:
        return False, (type(error).__name__, str(error))


def compare_calls(tools, method, *args, **options):
    results = {name: result(getattr(tool, method), *args, **options)
               for name, tool in tools.items()}
    if 'native' in results:
        actual, expected = results['native'], results['casa']
        assert actual[0] == expected[0], (method, actual, expected)
        if actual[0]:
            same_record(actual[1], expected[1])
        else:
            assert actual[1] == expected[1], (method, actual, expected)
    return results['casa']


def parse_libraries(work, tools):
    path = work/'calibrations.callib'
    cases = {
        'empty': '',
        'comments': '# comment only\n',
        'default': "caltable='A.cal'\n",
        'assignment-without-table': "field='0'\n",
        'empty-table-name': "caltable=''\n",
        'mixed-maps': ("caltable='B.cal' field='0' spw='0:3~7' intent='*TARGET*' "
                       "obs='0' scan='2' tinterp='nearest' finterp='cubic' reach='30s' "
                       "fldmap='closest' obsmap=[1,0] scanmap='self' spwmap=[0,2] "
                       "antmap=[2,1] calwt=F\n"),
        'multiple-tables-and-slices': (
            "caltable='Z.cal' calwt=False\ncaltable='A.cal' scan='3'\n" +
            ''.join(f"caltable='Z.cal' field='{index}' calwt=True\n" for index in range(11))
        ),
        'raw-fields': "caltable='A.cal' spwmap='self' tinterp='bad' unknown='ignored'\n",
        'missing-assignment': "caltable 'A.cal'\n",
        'array-prefix': "caltable='A.cal' spwmap=[1,two]\n",
    }
    for name, text in cases.items():
        path.write_text(text)
        success, value = compare_calls(tools, 'parsecallibfile', str(path))
        print('library', name, 'record' if success else value[0], flush=True)
        if name == 'multiple-tables-and-slices':
            assert success
            assert tuple(value) == ('A.cal', 'Z.cal')
            assert tuple(value['Z.cal'])[:4] == ('0', '1', '10', '11')
            assert value['Z.cal']['calwt'] is False
    for path_arg in (bytes(path), str(path) + '\0ignored'):
        assert compare_calls(tools, 'parsecallibfile', path_arg)[0]
    for path_arg in ('', str(work/'does-not-exist.callib'), None, 123, path):
        assert not compare_calls(tools, 'parsecallibfile', path_arg)[0]


def validate_libraries(tools):
    assert compare_calls(tools, 'validatecallib') == (True, True)
    for value in (None, [], True, {'bad': 4}, {'ignored': None}, {'missing.cal': {}},
                  {'table.cal': {'calwt': True, '0': {'field': '0'}}}):
        outcome = compare_calls(tools, 'validatecallib', value)
        if value is None or value == [] or value is True or value == {'bad': 4}:
            assert not outcome[0]
        else:
            assert outcome == (True, True)


def register_libraries(work, tools):
    from blcal_oracle import copy_ms, source

    vis = source(work)
    prior_current, prior_new, prior_bad = [
        work/name for name in ('prior-current.cal', 'prior-added.cal', 'prior-malformed.cal')
    ]
    for prior in (prior_current, prior_new, prior_bad):
        casatasks.gencal(str(vis), str(prior), 'amp', parameter=[1.2])
    path = work/'chain.callib'
    path.write_text(
        f"caltable='{prior_current}' tinterp='linear' calwt=F\n"
        f"caltable='{prior_new}' tinterp='nearest' calwt=F\n"
    )
    ok, library = compare_calls(tools, 'parsecallibfile', str(path))
    assert ok
    assert compare_calls(tools, 'setcallib', library) == (True, False)
    assert compare_calls(tools, 'returndict') == (True, {})
    for engine, tool in tools.items():
        attached = work/f'attachment-{engine}.ms'
        copy_ms(vis, attached)
        assert tool.open(str(attached)) is True
    assert compare_calls(tools, 'selectvis', field='FLUX', spw='0:2~8') == (True, True)
    assert compare_calls(tools, 'setcallib', {str(prior_current): library[str(prior_current)]}) == (
        True, True
    )
    was_open, before = compare_calls(tools, 'returndict')
    assert was_open
    np.testing.assert_array_equal(before['apply_tables'], [str(prior_current)])
    record = {str(prior_new): library[str(prior_new)], str(work/'z-absent.cal'): {}}
    assert not compare_calls(tools, 'setcallib', record)[0]
    same_record(compare_calls(tools, 'returndict')[1], before)
    record = {str(prior_new): library[str(prior_new)], str(prior_bad): {'0': {}}}
    assert not compare_calls(tools, 'setcallib', record)[0]
    ready, current = compare_calls(tools, 'returndict')
    assert ready
    np.testing.assert_array_equal(current['apply_tables'], sorted([str(prior_current), str(prior_new)]))
    assert compare_calls(tools, 'setcallib', library) == (True, True)
    same_record(compare_calls(tools, 'returndict')[1], current)
    check_registration_errors(work, vis, prior_current, tools)
    assert compare_calls(tools, 'reset', True, False) == (True, True)
    assert compare_calls(tools, 'returndict')[1]['apply_tables'].size == 0
    if 'native' in tools:
        from casars.tasks import gencal

        native_prior = work/'native-prior.cal'
        gencal(vis=str(vis), caltable=str(native_prior), caltype='amp', parameter=[1.2])
        path.write_text(f"caltable='{native_prior}'\n")
        ok, native_library = compare_calls(tools, 'parsecallibfile', str(path))
        assert ok
        assert compare_calls(tools, 'setcallib', native_library) == (True, True)
        np.testing.assert_array_equal(
            compare_calls(tools, 'returndict')[1]['apply_tables'], [str(native_prior)]
        )
    assert compare_calls(tools, 'close') == (True, True)
    assert compare_calls(tools, 'returndict') == (True, {})
    print('Library registration retained exactly the terms listed by CASA', flush=True)


def check_registration_errors(work, vis, prior, tools):
    from blcal_oracle import opened

    before = compare_calls(tools, 'returndict')[1]
    for subtype in ('BPOLY', 'GSPLINE', 'unknown calibration'):
        invalid = work/f'{subtype}.cal'
        copytree(prior, invalid)
        with opened(invalid, True) as table:
            info = table.info()
            info['subType'] = subtype
            table.putinfo(info)
        assert not compare_calls(tools, 'setcallib', {str(invalid): {}})[0]
    for record in ({str(vis): {}}, {str(prior): 4},
                   {str(prior): {'0': {'tinterp': 'linear'}, '1': {'tinterp': 'linear'}}},
                   {str(prior): {'0': {'tinterp': 'linear', 'fldmap': 'unknown-field'}}},
                   {str(prior): {'0': {'tinterp': 'linear', 'field': 'unknown-field'}}}):
        assert not compare_calls(tools, 'setcallib', record)[0]
        same_record(compare_calls(tools, 'returndict')[1], before)
    for slice_ in ({'tinterp': 'linear', 'fldmap': 'FLUX'},
                   {'field': 'PHASE'}, {'tinterp': 'bad', 'field': 'PHASE'},
                   {'tinterp': 'linear', 'spwmap': [0.5]},
                   {'tinterp': 'linear', 'spwmap': np.zeros((2, 1), dtype=int)}):
        assert compare_calls(tools, 'setcallib', {str(prior): {'calwt': 1, '0': slice_}}) == (
            True, True
        )


def selected_antenna_record(work, tools):
    from blcal_oracle import copy_ms, opened

    vis = work/'disjoint-antennas.ms'
    copy_ms(work/'baseline-2.ms', vis)
    with opened(vis, True) as table:
        scans = table.getcol('SCAN_NUMBER')
        first = scans == scans.min()
        table.putcol('ANTENNA1', np.where(first, 0, 2).astype(np.int32))
        table.putcol('ANTENNA2', np.where(first, 1, 3).astype(np.int32))
    for engine, tool in tools.items():
        assert tool.open(str(vis))
        selected = tool.returndict()['selectvis']
        np.testing.assert_array_equal(selected['scan'], [1, 2])
        expected = [2, 3] if engine == 'casa' else [0, 1, 2, 3]
        np.testing.assert_array_equal(selected['antennas'], expected)
        assert tool.close()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('work', type=Path)
    cli.add_argument('--reference-only', action='store_true')
    args = cli.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    tools = {'casa': calibrater()}
    if not args.reference_only:
        from casars.tools import calibrater as native_calibrater

        tools['native'] = native_calibrater()
    work = args.work.resolve()
    work.mkdir()
    with chdir(work):
        parse_libraries(work, tools)
        validate_libraries(tools)
        register_libraries(work, tools)
        selected_antenna_record(work, tools)


if __name__ == '__main__':
    main()
