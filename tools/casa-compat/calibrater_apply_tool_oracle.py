#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pinned CASA parity for the calibrater's library and apply workflow."""

import argparse
from contextlib import chdir
from pathlib import Path
from select import select
from shutil import copytree
import subprocess
import sys

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
        'quoted-empty-map': "caltable='A.cal' antmap='[]' spwmap='[]'\n",
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
    for mapping in ('antmap=[]', 'fldmap=[]', 'obsmap=[]', 'scanmap=[]',
                    'spwmap=[]', 'spwmap=[ ]'):
        path.write_text(f"caltable='A.cal' {mapping}\n")
        assert compare_calls(tools, 'parsecallibfile', str(path)) == (
            False, ('RuntimeError', 'Cal Library file: Parse error on line 1: syntax error')
        )
        print('library', mapping, 'rejected', flush=True)
    path.write_text(cases['array-prefix'])
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


def correct_selected_data(work, tools):
    from blcal_oracle import copy_ms, opened

    source = work/'baseline-2.ms'
    prior = work/'correction.cal'
    copytree(work/'prior-current.cal', prior)
    with opened(prior, True) as table:
        flags = table.getcol('FLAG')
        flags[..., table.getcol('ANTENNA1') == 0] = True
        table.putcol('FLAG', flags)

    plan = {str(prior): {'calwt': True, '0': {'tinterp': 'linear'}}}
    modes = ('', 'calflag', 'calonly', 'flagonly', 'trial', 'unknown',
             'calflagtrial', 'calonlytrial', 'flagonlycal')
    cases = [(mode or 'default', mode, plan, 1) for mode in modes]
    cases.extend([('same-table-twice', '', plan, 2), ('empty-library', '', {str(prior): {}}, 1)])
    for legacy in (False, True):
        for label, mode, record, registrations in cases:
            paths = {}
            for engine, tool in tools.items():
                name = f'correct-{engine}-{legacy}-{label}.ms'
                paths[engine] = work/name
                copy_ms(source, paths[engine])
                with opened(paths[engine], True) as table:
                    seed = np.zeros_like(table.getcol('CORRECTED_DATA'))
                    table.putcol('CORRECTED_DATA', seed)
                    table.putcol('WEIGHT', 12*np.ones_like(table.getcol('WEIGHT')))
                assert tool.setvi(legacy, True)
                assert tool.open(str(paths[engine]))
                assert tool.selectvis(field='FLUX', spw='0:2~8')
            assert compare_calls(tools, 'correct', None)[0] is False
            assert compare_calls(tools, 'correct')[0] is False
            for _ in range(registrations):
                assert compare_calls(tools, 'setcallib', record) == (True, True)
            assert compare_calls(tools, 'correct', mode) == (True, True)
            assert compare_calls(tools, 'activityrec')[0]
            assert compare_calls(tools, 'close') == (True, True)
            compare_correction_products(paths, label)
            if not legacy and label == 'calflag' and 'native' in paths:
                compare_correction_readers(paths)
            print('correction', 'VI1' if legacy else 'VI2', label, 'matched', flush=True)


def compare_correction_products(paths, label):
    from blcal_oracle import opened

    if 'native' not in paths:
        return
    with opened(paths['casa']) as reference, opened(paths['native']) as actual:
        for column in ('CORRECTED_DATA', 'WEIGHT', 'WEIGHT_SPECTRUM'):
            assert (column in reference.colnames()) == (column in actual.colnames())
            if column in reference.colnames() and reference.iscelldefined(column, 0):
                np.testing.assert_allclose(actual.getcol(column), reference.getcol(column),
                                           rtol=2e-6, atol=2e-7, err_msg=f'{label} {column}')
        for column in ('DATA', 'FLAG', 'FLAG_ROW', 'SIGMA', 'MODEL_DATA'):
            np.testing.assert_array_equal(actual.getcol(column), reference.getcol(column),
                                          err_msg=f'{label} {column}')


def compare_correction_readers(paths):
    from blcal_oracle import opened
    from casars.tools import table

    for writer, path in paths.items():
        reader = table()
        assert reader.open(str(path))
        try:
            with opened(path) as reference:
                for column in ('CORRECTED_DATA', 'FLAG', 'WEIGHT', 'SIGMA', 'MODEL_DATA'):
                    np.testing.assert_array_equal(reader.getcol(column), reference.getcol(column),
                                                  err_msg=f'{writer} {column}')
        finally:
            reader.close()


def retained_main_lock(work):
    from blcal_oracle import copy_ms, opened
    from casatools import table

    path = work/'concurrent-correction.ms'
    copy_ms(work/'baseline-2.ms', path)
    prior = work/'prior-current.cal'
    script = """
import sys
import threading
import time
from casars.tools import calibrater
tool = calibrater()
assert tool.open(sys.argv[1], addcorr=False)
assert tool.selectvis(field=sys.argv[3], spw='0:2~8')
assert tool.setapply(table=sys.argv[2])
print('ready', flush=True)
assert sys.stdin.readline().strip() == 'go'
def show_python_progress():
    time.sleep(0.1)
    print('GIL released', flush=True)
observer = threading.Thread(target=show_python_progress)
print('entering correction', flush=True)
observer.start()
assert tool.correct()
observer.join()
assert tool.close()
"""
    writers = []
    locker = table()
    try:
        for field in ('FLUX', 'PHASE'):
            writer = subprocess.Popen([sys.executable, '-c', script, str(path), str(prior), field],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True)
            writers.append(writer)
            assert select([writer.stdout], [], [], 15)[0]
            assert writer.stdout.readline().strip() == 'ready', writer.poll()
        assert locker.open(str(path), lockoptions={'option': 'user'}, nomodify=False)
        assert locker.lock(write=True)
        original = locker.getcol('CORRECTED_DATA')
        observed = 2*locker.getcol('DATA')
        for writer in writers:
            writer.stdin.write('go\n')
            writer.stdin.flush()
            assert select([writer.stdout], [], [], 15)[0]
            assert writer.stdout.readline().strip() == 'entering correction'
        for writer in writers:
            assert select([writer.stdout], [], [], 15)[0]
            assert writer.stdout.readline().strip() == 'GIL released'
        # Neither tool can publish while the independent casacore writer holds MAIN.
        assert not select([writer.stdout for writer in writers], [], [], 0.25)[0]
        locker.putcol('DATA', observed)
        assert locker.close()
        for writer in writers:
            _, stderr = writer.communicate(timeout=25)
            assert writer.returncode == 0, stderr
    finally:
        locker.close()
        for writer in writers:
            if writer.poll() is None:
                writer.kill()
                writer.communicate(timeout=5)
    with opened(path) as result:
        expected = original.copy()
        expected[:, 2:9, :] = observed[:, 2:9, :] / np.float32(1.2)**2
        np.testing.assert_allclose(result.getcol('CORRECTED_DATA'), expected, rtol=2e-6)
        np.testing.assert_array_equal(result.getcol('DATA'), observed)
    print('Both tools reopened and published under the retained casacore MAIN lock', flush=True)


def uncovered_spectral_window(work, tools):
    from blcal_oracle import copy_ms, opened
    from casatools import table as casa_table
    from gaincal_delay_oracle import duplicate_rows

    source = work/'two-windows.ms'
    copy_ms(work/'baseline-2.ms', source)
    for name in ('SPECTRAL_WINDOW', 'DATA_DESCRIPTION'):
        assert duplicate_rows(casa_table(), source/name) == 1
    with opened(source/'DATA_DESCRIPTION', True) as table:
        table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(source, True) as table:
        table.putcol('DATA_DESC_ID', table.getcol('FIELD_ID'))
        table.putcol('CORRECTED_DATA', np.zeros_like(table.getcol('DATA')))

    for mode in ('calflag', 'calflagstrict', 'flagonlystrict', 'calonlystrict', 'trialstrict'):
        paths = {}
        for engine, tool in tools.items():
            paths[engine] = work/f'uncovered-{engine}-{mode}.ms'
            copy_ms(source, paths[engine])
            assert tool.setvi(False, True)
            assert tool.open(str(paths[engine]))
            assert tool.selectvis(spw='0:2~8,1:2~8')
        assert compare_calls(tools, 'setapply', table=str(work/'prior-current.cal')) == (True, True)
        assert compare_calls(tools, 'correct', mode) == (True, False)
        assert compare_calls(tools, 'close') == (True, True)
        compare_correction_products(paths, mode)
        print('uncovered spectral window', mode, 'matched', flush=True)
    if 'native' in tools:
        strict_prefix_before_error(work, source, tools['native'])


def strict_prefix_before_error(work, source, tool):
    from blcal_oracle import copy_ms, opened

    path = work/'missing-corrected.ms'
    copy_ms(source, path)
    with opened(path, True) as table:
        fields = table.getcol('FIELD_ID')
        table.putcol('DATA_DESC_ID', 1-fields)
        table.removecols(['CORRECTED_DATA'])
        before = table.getcol('FLAG')
    assert tool.open(str(path), addcorr=False)
    assert tool.selectvis(spw='0:2~8,1:2~8')
    assert tool.setapply(table=str(work/'prior-current.cal'))
    for _ in range(2):
        ok, error = result(tool.correct, 'calflagstrict')
        assert not ok and error == ('RuntimeError', 'Error in Calibrater::correct.')
        assert tool.returndict()['apply_tables'].size == 0
    assert tool.close()
    with opened(path) as table:
        expected = before.copy()
        expected[:, 2:9, fields == 0] = True
        np.testing.assert_array_equal(table.getcol('FLAG'), expected)
        assert 'CORRECTED_DATA' not in table.colnames()
    print('Strict correction preserved earlier flags across a later missing-output error', flush=True)


def traditional_apply(work, tools):
    from blcal_oracle import copy_ms, opened

    prior = str(work/'prior-current.cal')
    for legacy in (False, True):
        paths = {}
        for engine, tool in tools.items():
            paths[engine] = work/f'traditional-{engine}-{legacy}.ms'
            copy_ms(work/'baseline-2.ms', paths[engine])
            assert tool.setvi(legacy, True)
            assert tool.open(str(paths[engine]))
            assert tool.selectvis(field='FLUX', spw='0:2~8')
        for supplied in ({'type': None}, {'t': None}, {'interp': None}, {'select': None},
                         {'table': Path(prior)}, {'calwt': np.bool_(True)}, {'spwmap': None},
                         {'opacity': (0.1,)}, {'type': 'T'}, {'type': 'invalid'},
                         {'table': str(work/'missing.cal')}, {'interp': 'aipslin'},
                         {'field': ['FLUX']}, {'spwmap': [0, 1]}, {'spwmap': [99]}):
            assert compare_calls(tools, 'setapply', **({'table': prior} | supplied))[0] is False
        for supplied in ({'table': prior}, {'type': b'G', 'field': None, 'interp': ''},
                         {'t': np.float32(25), 'select': '99', 'calwt': True,
                          'spwmap': [0.3], 'opacity': np.array([0.2])}):
            assert compare_calls(tools, 'setapply', **({'table': prior} | supplied)) == (True, True)
        assert compare_calls(tools, 'correct', 'calflag') == (True, True)
        assert compare_calls(tools, 'close') == (True, True)
        if 'native' in paths:
            with opened(paths['casa']) as reference, opened(paths['native']) as actual:
                np.testing.assert_allclose(actual.getcol('CORRECTED_DATA'),
                                           reference.getcol('CORRECTED_DATA'), rtol=2e-6)
                np.testing.assert_allclose(actual.getcol('WEIGHT'), reference.getcol('WEIGHT'),
                                           rtol=2e-6)
                np.testing.assert_array_equal(actual.getcol('FLAG'), reference.getcol('FLAG'))
        print('traditional setapply', 'VI1' if legacy else 'VI2', 'matched', flush=True)


def registration_retains_values(work, tools, *, library=False):
    from blcal_oracle import copy_ms, opened

    label = 'library' if library else 'traditional'
    first, second = work/f'{label}-register-first', work/f'{label}-register-second'
    first.mkdir()
    second.mkdir()
    relative = 'gain.cal'
    first_table, second_table = first/relative, second/relative
    for path in (first_table, second_table):
        copytree(work/'prior-current.cal', path)
    with opened(second_table, True) as table:
        table.putcol('CPARAM', 2*table.getcol('CPARAM'))

    paths = {}
    for engine, tool in tools.items():
        paths[engine] = work/f'snapshot-{label}-{engine}.ms'
        copy_ms(work/'baseline-2.ms', paths[engine])
        assert tool.setvi(False, True)
        assert tool.open(str(paths[engine]))
    with chdir(first):
        if library:
            assert compare_calls(tools, 'setcallib', {relative: {'0': {'tinterp': 'linear'}}}) == (
                True, True
            )
        else:
            assert compare_calls(tools, 'setapply', table=relative) == (True, True)
    with opened(first_table, True) as table:
        table.putcol('CPARAM', 3*table.getcol('CPARAM'))
    with chdir(second):
        configured = compare_calls(tools, 'returndict')[1]['apply_tables']
        np.testing.assert_array_equal(configured, [relative])
        assert compare_calls(tools, 'correct') == (True, True)
        assert compare_calls(tools, 'close') == (True, True)
    compare_correction_products(paths, 'registered relative table')


def repeated_parallactic_angle(work, tools):
    from blcal_oracle import copy_ms, opened, source

    vis = source(work, 'RR RL LR LL', 'circular')
    reference_outputs = []
    for registrations in (1, 2):
        paths = {}
        for engine, tool in tools.items():
            paths[engine] = work/f'parallactic-{engine}-{registrations}.ms'
            copy_ms(vis, paths[engine])
            with opened(paths[engine], True) as table:
                table.putcol('WEIGHT', 12*np.ones_like(table.getcol('WEIGHT')))
            assert tool.setvi(False, True)
            assert tool.open(str(paths[engine]))
        for _ in range(registrations):
            assert compare_calls(tools, 'setapply', type='P', calwt=True) == (True, True)
        assert compare_calls(tools, 'correct') == (True, True)
        assert compare_calls(tools, 'close') == (True, True)
        compare_correction_products(paths, f'{registrations} P Jones terms')
        with opened(paths['casa']) as reference:
            reference_outputs.append(reference.getcol('CORRECTED_DATA')[1])
            np.testing.assert_allclose(reference.getcol('WEIGHT'), 1.0, rtol=1e-6)
    assert np.max(np.abs(reference_outputs[0] - reference_outputs[1])) > 1e-2


def noncommuting_tool_terms(work, tools):
    from blcal_oracle import copy_ms, opened
    from record_casa_returns import make_cal_ms

    source, curve = work/'linear-vla.ms', work/'linear-curve.cal'
    make_cal_ms(str(source), channels=8, stokes='XX XY YX YY', telescope='VLA',
                frequency='1.5GHz', antenna_names=[f'ea0{n}' for n in range(1, 6)],
                autocorrelation_weight=1.)
    casatasks.gencal(vis=str(source), caltable=str(curve), caltype='gc')
    with opened(curve, True) as table:
        coefficients = np.zeros_like(table.getcol('FPARAM'))
        assert coefficients.shape[0] == 8
        coefficients[0] = 1.4
        coefficients[4] = 0.7
        table.putcol('FPARAM', coefficients)
    outputs = []
    terms = ({'table': str(curve)}, {'type': 'P'})
    for index, sequence in enumerate((terms, terms[::-1])):
        paths = {}
        for engine, tool in tools.items():
            paths[engine] = work/f'ordered-{engine}-{index}.ms'
            copy_ms(source, paths[engine])
            assert tool.open(str(paths[engine]))
        for arguments in sequence:
            assert compare_calls(tools, 'setapply', **arguments) == (True, True)
        assert compare_calls(tools, 'correct') == (True, True)
        assert compare_calls(tools, 'close') == (True, True)
        compare_correction_products(paths, 'P and EGainCurve order')
        with opened(paths['casa']) as table:
            outputs.append(table.getcol('CORRECTED_DATA'))
    np.testing.assert_array_equal(*outputs)


def activity_cycle(work, tools):
    from blcal_oracle import copy_ms

    prior = str(work/'prior-current.cal')
    for legacy in (False, True):
        paths = {}
        assert compare_calls(tools, 'activityrec') == (True, {})
        for engine, tool in tools.items():
            paths[engine] = work/f'activity-{engine}-{legacy}.ms'
            copy_ms(work/'baseline-2.ms', paths[engine])
            assert tool.setvi(legacy, True)
            assert tool.open(str(paths[engine]))
        assert compare_calls(tools, 'selectvis', field='FLUX', spw='0:2~8') == (True, True)
        assert compare_calls(tools, 'setapply', table=prior) == (True, True)
        assert compare_calls(tools, 'activityrec') == (True, {})
        assert compare_calls(tools, 'correct') == (True, True)
        first = compare_calls(tools, 'activityrec')[1]
        assert first['VisEquation']['nVisTotal'] == 4200
        assert first['VisEquation']['*1']['nflagIn'] == 320
        assert compare_calls(tools, 'correct', 'trial') == (True, True)
        second = compare_calls(tools, 'activityrec')[1]
        assert second['VisEquation']['*1']['ndata'] == 8400
        assert compare_calls(tools, 'setapply', table=prior) == (True, True)
        assert compare_calls(tools, 'correct', 'flagonly') == (True, True)
        multiple = compare_calls(tools, 'activityrec')[1]
        assert tuple(multiple['VisEquation']) == ('*1', '*2', 'nVisTotal')
        assert multiple['VisEquation']['*2']['ndata'] == 4200
        assert compare_calls(tools, 'reset', True, False) == (True, True)
        assert compare_calls(tools, 'activityrec') == (True, multiple)
        assert not compare_calls(tools, 'correct')[0]
        assert compare_calls(tools, 'activityrec') == (True, multiple)
        for name, tool in tools.items():
            assert tool.open(str(paths[name]))
        assert compare_calls(tools, 'activityrec') == (True, multiple)
        assert compare_calls(tools, 'close') == (True, True)
        assert compare_calls(tools, 'activityrec') == (True, {})
        for name, tool in tools.items():
            assert tool.open(str(paths[name]))
        assert compare_calls(tools, 'activityrec') == (True, {})
        assert compare_calls(tools, 'selectvis', spw='0:2~8') == (True, True)
        library = {prior: {'0': {'field': 'FLUX', 'tinterp': 'linear'},
                           '1': {'field': 'PHASE', 'tinterp': 'nearest'}}}
        assert compare_calls(tools, 'setcallib', library) == (True, True)
        assert compare_calls(tools, 'correct', 'trial') == (True, True)
        grouped = compare_calls(tools, 'activityrec')[1]['VisEquation']
        assert tuple(grouped) == ('*1', 'nVisTotal')
        assert grouped['*1']['ndata'] == grouped['nVisTotal'] == 8400
        assert compare_calls(tools, 'done') == (True, True)
        compare_correction_products(paths, 'activity and reset cycle')
        print('activity and reset', 'VI1' if legacy else 'VI2', 'matched', flush=True)


def corrupt_models(work, tools):
    from blcal_oracle import copy_ms, opened

    assert compare_calls(tools, 'corrupt') == (True, False)
    prior = str(work/'prior-current.cal')
    scenarios = (
        ('traditional', work/'baseline-2.ms', [('setapply', {'table': prior})], False),
        ('corrected-then-corrupt', work/'baseline-2.ms', [('setapply', {'table': prior})], True),
        ('noncommuting', work/'linear-vla.ms', [('setapply', {'table': str(work/'linear-curve.cal')}),
                                             ('setapply', {'type': 'P'})], False),
        ('library-partial', work/'two-windows.ms', [('setcallib', {'callib': {
            prior: {'0': {'tinterp': 'linear', 'spw': '0'}}
        }})], False),
    )
    for legacy in (False, True):
        for label, source, terms, correct_first in scenarios:
            paths = {}
            for name, tool in tools.items():
                paths[name] = work/f'corrupt-{name}-{label}-{legacy}.ms'
                copy_ms(source, paths[name])
                assert tool.setvi(legacy, True)
                assert tool.open(str(paths[name]))
            selection = '0:2~5,1:2~5' if label == 'library-partial' else '0:2~5'
            scalar_gain = label in ('traditional', 'corrected-then-corrupt')
            assert compare_calls(tools, 'selectvis', field='FLUX' if scalar_gain else '',
                                 spw=selection) == (True, True)
            assert not compare_calls(tools, 'corrupt')[0]
            for method, arguments in terms:
                assert compare_calls(tools, method, **arguments) == (True, True)
            assert compare_calls(tools, 'setptmodel', [9.0, 0.0, 0.0, 0.0]) == (True, True)
            if correct_first:
                assert compare_calls(tools, 'correct', 'trial') == (True, True)
            before = compare_calls(tools, 'activityrec')[1]
            incomplete_legacy_library = legacy and label == 'library-partial'
            for _ in range(1 if incomplete_legacy_library else 2):
                if incomplete_legacy_library:
                    assert compare_calls(tools, 'corrupt') == (
                        False, ('RuntimeError', 'Error in Calibrater::corrupt.')
                    )
                    assert compare_calls(tools, 'returndict')[1]['apply_tables'].size == 0
                    break
                assert compare_calls(tools, 'corrupt') == (True, True)
                activity = compare_calls(tools, 'activityrec')[1]
                if legacy:
                    same_record(activity, before)
                    continue
                assert activity['origin'] == 'Calibrater::corrupt2'
                assert activity['VisEquation']['nVisTotal'] == 0
                if correct_first:
                    same_record(activity['VisEquation']['*1'], before['VisEquation']['*1'])
                else:
                    assert activity['VisEquation']['*1']['ndata'] == 0
            assert compare_calls(tools, 'close') == (True, True)
            with opened(source) as original, opened(paths['casa']) as reference:
                model = reference.getcol('MODEL_DATA')
                if scalar_gain:
                    original_model = original.getcol('MODEL_DATA')
                    mask = original.getcol('FIELD_ID') == 0
                    expected = original_model.copy()
                    expected[:, 2:6, mask] *= np.float32(1.2)**(-4 if correct_first else 4)
                    np.testing.assert_allclose(model, expected, rtol=2e-6, atol=2e-7)
                    expected[:, 2:6, mask] = original_model[:, 2:6, mask] * np.float32(1.2)**4
                else:
                    expected = model
                if 'native' in paths:
                    with opened(paths['native']) as actual:
                        np.testing.assert_allclose(actual.getcol('MODEL_DATA'), expected,
                                                   rtol=2e-6, atol=2e-7, err_msg=label)
                        for column in ('DATA', 'CORRECTED_DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA'):
                            np.testing.assert_array_equal(actual.getcol(column), reference.getcol(column),
                                                          err_msg=f'{label} {column}')
            if not legacy and label == 'traditional' and 'native' in paths:
                compare_correction_readers(paths)
            print('corrupt', 'VI1' if legacy else 'VI2', label, 'matched', flush=True)


def corrupt_requires_physical_model(work, tools):
    from blcal_oracle import copy_ms, opened

    prior = str(work/'prior-current.cal')
    for legacy in (False, True):
        paths = {}
        for name, tool in tools.items():
            paths[name] = work/f'corrupt-missing-model-{name}-{legacy}.ms'
            copy_ms(work/'baseline-2.ms', paths[name])
            with opened(paths[name], True) as table:
                table.removecols(['MODEL_DATA'])
            assert tool.setvi(legacy, True)
            assert tool.open(str(paths[name]), addcorr=True, addmodel=False)
        assert compare_calls(tools, 'setapply', table=prior) == (True, True)
        suffix = '' if legacy else '2'
        assert compare_calls(tools, 'corrupt') == (
            False, ('RuntimeError', f'Error in Calibrater::corrupt{suffix}.')
        )
        assert compare_calls(tools, 'returndict')[1]['apply_tables'].size == 0
        assert compare_calls(tools, 'close') == (True, True)
        for path in paths.values():
            with opened(path) as table:
                assert 'MODEL_DATA' not in table.colnames()


def corrupt_prefix_before_error(work, tools):
    from blcal_oracle import copy_ms, opened

    prior = str(work/'prior-current.cal')
    for legacy in (False, True):
        paths = {}
        for name, tool in tools.items():
            path = paths[name] = work/f'corrupt-failed-buffer-{name}-{legacy}.ms'
            copy_ms(work/'baseline-2.ms', path)
            with opened(path, True) as table:
                original = table.getcol('MODEL_DATA')
                scan = np.arange(table.nrows(), dtype=np.int32) % 2 + 1
                table.putcol('SCAN_NUMBER', scan)
                descriptor = table.getcoldesc('MODEL_DATA')
                descriptor.update(option=0, dataManagerType='StandardStMan',
                                  dataManagerGroup='VariableModel')
                descriptor.pop('shape', None)
                assert table.removecols(['MODEL_DATA'])
                assert table.addcols({'MODEL_DATA': descriptor})
                for row in np.flatnonzero(scan == 1):
                    assert table.putcell('MODEL_DATA', int(row), original[:, :, row])
            assert tool.setvi(legacy, True)
            assert tool.open(str(path), addcorr=False, addmodel=False)
        assert compare_calls(tools, 'setapply', table=prior) == (True, True)
        suffix = '' if legacy else '2'
        assert compare_calls(tools, 'corrupt') == (
            False, ('RuntimeError', f'Error in Calibrater::corrupt{suffix}.')
        )
        assert compare_calls(tools, 'returndict')[1]['apply_tables'].size == 0
        assert compare_calls(tools, 'close') == (True, True)
        for path in paths.values():
            with opened(path) as table:
                for row in np.flatnonzero(scan == 1):
                    np.testing.assert_allclose(table.getcell('MODEL_DATA', int(row)),
                                               original[:, :, row] * np.float32(1.2)**2,
                                               rtol=2e-6, atol=2e-7)
                assert not table.iscelldefined('MODEL_DATA', int(np.flatnonzero(scan == 2)[0]))
        print('corrupt preserved the completed scan before the undefined model', legacy, flush=True)


def corrupt_library_scan_and_intent(work, tools):
    from blcal_oracle import copy_ms, opened

    prior = str(work/'prior-current.cal')
    for legacy in (False, True):
        for selector in ({'scan': '1'}, {'intent': 'CALIBRATE_FLUX*'}):
            paths = {}
            for name, tool in tools.items():
                path = paths[name] = work/f'corrupt-context-{name}-{legacy}-{next(iter(selector))}.ms'
                copy_ms(work/'baseline-2.ms', path)
                with opened(path, True) as table:
                    scan = np.arange(table.nrows(), dtype=np.int32) % 2 + 1
                    table.putcol('SCAN_NUMBER', scan)
                    table.putcol('STATE_ID', scan - 1)
                    original = table.getcol('MODEL_DATA')
                with opened(path/'STATE', True) as table:
                    table.addrows(max(0, 2 - table.nrows()))
                    table.putcell('OBS_MODE', 0, 'CALIBRATE_FLUX#ON_SOURCE')
                    table.putcell('OBS_MODE', 1, 'CALIBRATE_PHASE#ON_SOURCE')
                assert tool.setvi(legacy, True)
                assert tool.open(str(path))
            assert compare_calls(tools, 'setcallib', {
                prior: {'0': {'tinterp': 'linear', **selector}}
            }) == (True, True)
            assert compare_calls(tools, 'corrupt') == (True, True)
            assert compare_calls(tools, 'close') == (True, True)
            expected = original.copy()
            expected[:, :, (scan == 1) | legacy] *= np.float32(1.2)**2
            for path in paths.values():
                with opened(path) as table:
                    np.testing.assert_allclose(table.getcol('MODEL_DATA'), expected,
                                               rtol=2e-6, atol=2e-7)
            print('corrupt library slice context matched', legacy, selector, flush=True)


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
        correct_selected_data(work, tools)
        traditional_apply(work, tools)
        registration_retains_values(work, tools)
        registration_retains_values(work, tools, library=True)
        repeated_parallactic_angle(work, tools)
        noncommuting_tool_terms(work, tools)
        uncovered_spectral_window(work, tools)
        activity_cycle(work, tools)
        corrupt_models(work, tools)
        corrupt_requires_physical_model(work, tools)
        corrupt_prefix_before_error(work, tools)
        corrupt_library_scan_and_intent(work, tools)
        if 'native' in tools:
            retained_main_lock(work)


if __name__ == '__main__':
    main()
