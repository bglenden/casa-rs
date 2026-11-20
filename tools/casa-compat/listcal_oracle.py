#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA's file and terminal calibration listings with the native task."""

import argparse
from contextlib import chdir
import difflib
import inspect
import os
from pathlib import Path
import subprocess
import sys

import casatasks as casa
import numpy as np

from bpoly_oracle import multiwindow_data
from calibration_rerefant_oracle import reference_fixture
from calibration_smoothcal_oracle import copy_ms, fixture, opened


def same_text(actual, expected):
    if actual != expected:
        difference = ''.join(difflib.unified_diff(expected.splitlines(keepends=True),
                                                 actual.splitlines(keepends=True),
                                                 fromfile='CASA', tofile='native'))
        raise AssertionError(difference[:18000] or (repr(actual), repr(expected)))


def files(root, vis, multi_vis, products, native):
    cases = [
        ('gain-all', 'gain', {}),
        ('gain-selected', 'gain', dict(field='FLUX', antenna='A3,A0', spw='0:0')),
        ('gain-field-id', 'gain', dict(field='0', antenna='0~2', pagerows=0)),
        ('gain-first-baseline-antenna', 'gain', dict(antenna='A1&A3')),
        ('gain-file-pages', 'gain', dict(field='FLUX', pagerows=1, antenna='A0,A3')),
        ('gain-boolean-pages', 'gain', dict(field='FLUX', pagerows=True, antenna='A0')),
        ('bandpass-all', 'bandpass', dict(field='FLUX', pagerows=0)),
        ('bandpass-stride', 'bandpass', dict(field='FLUX', antenna='A0,A3', spw='0:2~6^2')),
        ('bandpass-first-range', 'bandpass', dict(field='FLUX', spw='0:1~2;5~6')),
        ('delay', 'delay', dict(pagerows=-1)),
        ('scalar', 'scalar', dict(pagerows=0)),
        ('fringe', 'fringe', dict(antenna='A0,A4', pagerows=0)),
        ('phase-field', 'solved', dict(field='PHASE', antenna='A0', pagerows=0)),
        ('multiple-windows', 'multi', dict(spw='1:0,0:0', antenna='A0,A3', pagerows=0)),
        ('repeat-time', 'repeat', dict(antenna='A0', pagerows=0)),
        ('physical-order', 'unordered', dict(antenna='A0', pagerows=0)),
        ('flagged-nan', 'nonfinite', dict(antenna='A0', pagerows=0)),
        ('narrowed-page-size', 'gain', dict(antenna='A0', pagerows=2**32+1)),
        ('numpy-page-size', 'gain', dict(antenna='A0', pagerows=np.int32(2))),
    ]
    for name, kind, options in cases:
        expected, actual = (root/f'{name}.{engine}.txt' for engine in ('casa', 'native'))
        inp = str(products[kind])
        observed = str(multi_vis if kind == 'multi' else vis)
        assert casa.listcal(observed, inp, listfile=str(expected), **options) is None
        assert native.listcal(vis=observed, caltable=inp, listfile=str(actual), **options) is None
        same_text(actual.read_text(), expected.read_text())

    with chdir(root):
        (root/'linked.ms').symlink_to(vis, target_is_directory=True)
        assert casa.listcal('linked.ms', 'gain.cal', listfile='relative.casa.txt') is None
        assert native.listcal('linked.ms', 'gain.cal', listfile='relative.native.txt') is None
        same_text(Path('relative.native.txt').read_text(), Path('relative.casa.txt').read_text())
    return len(cases) + 1


def terminals(vis, caltable, multi_vis, multi_cal):
    code = """
import sys
if sys.argv[1]=='casa':
    from casatasks import listcal
else:
    from casars.tasks import listcal
assert listcal(sys.argv[2],sys.argv[3],field='FLUX',antenna='A0',
               spw=sys.argv[5],pagerows=int(sys.argv[4])) is None
"""
    def execute(engine, source, table, size, spw, response):
        return subprocess.run([sys.executable, '-c', code, engine, str(source), str(table),
                               str(size), spw], input=response, text=True,
                              capture_output=True, timeout=40)

    cases = [(0, ''), (-2, ''), (5, 'a\n'), (5, '\nq\n'), (5, 'q\n'), (5, '')]
    for size, response in cases:
        casa_run = execute('casa', vis, caltable, size, '0:0~2', response)
        native = execute('native', vis, caltable, size, '0:0~2', response)
        assert casa_run.returncode == native.returncode == 0, (casa_run.stderr, native.stderr)
        same_text(native.stdout, casa_run.stdout)

    options = (multi_vis, multi_cal, 1, '0:0,1:1')
    casa_run = execute('casa', *options, 'q\n')
    native = execute('native', *options, 'q\n')
    assert casa_run.returncode == native.returncode == 0, (casa_run.stderr, native.stderr)
    assert 'Listed 4 antenna solutions.' in native.stdout
    same_text(native.stdout, casa_run.stdout)
    for engine in ('casa', 'native'):
        result = execute(engine, *options, '')
        assert result.returncode != 0
        assert 'SpwID = 0' in result.stdout and 'RETURN to continue' in result.stdout
        assert 'Listed ' not in result.stdout
    return len(cases) + 2


def errors(root, vis, caltable, native, bad_tables):
    existing = root/'existing.txt'
    existing.write_bytes(b'keep this listing\n')
    for task in (casa.listcal, native.listcal):
        for options in (dict(listfile=str(existing)), dict(field='no_such_field'),
                        dict(antenna='no_such_antenna'), dict(spw='0:6~7')):
            try:
                task(str(vis), str(caltable), **options)
            except RuntimeError:
                pass
            else:
                raise AssertionError(f'Invalid listing accepted {options}')
        for table in bad_tables:
            try:
                task(str(vis), str(table), listfile=str(root/'invalid-table.txt'))
            except RuntimeError:
                pass
            else:
                raise AssertionError(f'Invalid calibration type accepted at {table}')
        for args in ((), (str(vis),), (str(vis), str(caltable), 1),
                     (str(vis), str(caltable), '', '', '', '', 1.5),
                     (vis, str(caltable)), (str(vis), caltable),
                     (str(vis), str(caltable), '', '', '', '', np.int16(2)),
                     (str(vis), str(caltable), '', '', '', '', np.uint32(2))):
            try:
                task(*args)
            except AssertionError:
                pass
            else:
                raise AssertionError(f'Invalid argument accepted {args}')
    assert existing.read_bytes() == b'keep this listing\n'
    assert not (root/'invalid-table.txt').exists()
    return 12 + len(bad_tables)


def layout_cases(root, vis, products, native):
    for kind in ('repeat', 'unordered', 'nonfinite'):
        products[kind] = root/f'{kind}.cal'
        copy_ms(products['gain'], products[kind])
    for kind, duplicate in (('repeat', True), ('unordered', False)):
        with opened(products[kind], True) as t:
            times = t.getcol('TIME')
            antennas = len(np.unique(t.getcol('ANTENNA1')))
            a, b = times[:antennas].copy(), times[antennas:2*antennas].copy()
            times[:antennas] = a if duplicate else b
            times[antennas:2*antennas] = a
            t.putcol('TIME', times)
    with opened(products['nonfinite'], True) as t:
        gain, flag = t.getcol('CPARAM'), t.getcol('FLAG')
        gain[0, 0, 0] = np.complex64(complex(np.nan, 1.0))
        flag[0, 0, 0] = True
        t.putcol('CPARAM', gain)
        t.putcol('FLAG', flag)

    partial = root/'partial.cal'
    copy_ms(products['gain'], partial)
    with opened(partial, True) as t:
        t.removerows([t.nrows()-1])
    code = """
import resource, sys
resource.setrlimit(resource.RLIMIT_CORE, (0,0))
from casatasks import listcal
listcal(sys.argv[1], sys.argv[2], listfile=sys.argv[3])
"""
    child = subprocess.run([sys.executable, '-c', code, str(vis), str(partial),
                            str(root/'partial.casa.txt')], capture_output=True, timeout=30)
    assert child.returncode != 0
    try:
        native.listcal(str(vis), str(partial), listfile=str(root/'partial.native.txt'))
    except RuntimeError as error:
        assert 'incomplete antenna integration' in str(error), error
    else:
        raise AssertionError('Accepted an incomplete calibration integration')
    assert not (root/'partial.native.txt').exists()


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

    assert casa.version_string() == '6.7.6.14'
    actual = inspect.signature(native.listcal)
    expected = inspect.signature(casa.listcal)
    assert [p.replace(annotation=inspect.Parameter.empty) for p in actual.parameters.values()] == list(expected.parameters.values())
    vis = root/'source.ms'
    fixture(vis, root/'solved.cal')
    products = {name: root/f'{name}.cal' for name in (
        'gain', 'bandpass', 'delay', 'scalar', 'fringe', 'solved', 'multi',
    )}
    reference_fixture(vis, products['gain'], 'G Jones')
    casa.bandpass(str(vis), str(products['bandpass']), refant='A0', solint='inf', minsnr=0.)
    casa.gencal(str(vis), str(products['delay']), 'sbd', parameter=[2.5])
    reference_fixture(vis, products['scalar'], 'T Jones', width=1)
    reference_fixture(vis, products['fringe'], 'Fringe Jones', width=8, real=True)
    multi_vis = root/'multi.ms'
    multiwindow_data(vis, multi_vis)
    casa.gaincal(str(multi_vis), str(products['multi']), refant='A0', solint='int', minsnr=0.)
    layout_cases(root, vis, products, native)
    baseline, unknown = root/'baseline.cal', root/'unknown.cal'
    casa.blcal(str(vis), str(baseline), solint='inf')
    copy_ms(products['gain'], unknown)
    with opened(unknown, True) as t:
        info = t.info()
        info['subType'] = 'Mystery Jones'
        t.putinfo(info)
    file_cases = files(root, vis, multi_vis, products, native)
    terminal_cases = terminals(vis, products['bandpass'], multi_vis, products['multi'])
    failures = errors(root, vis, products['gain'], native, [baseline, unknown]) + 1
    print(f'{file_cases} file listings, {terminal_cases} terminal cases and {failures} error cases passed')


if __name__ == '__main__':
    main()
