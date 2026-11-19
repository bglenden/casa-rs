#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA's file and terminal calibration listings with the native task."""

import argparse
import difflib
import inspect
import os
from pathlib import Path
import subprocess
import sys

import casatasks as casa

from calibration_rerefant_oracle import reference_fixture
from calibration_smoothcal_oracle import fixture


def same_text(actual, expected):
    if actual != expected:
        difference = ''.join(difflib.unified_diff(expected.splitlines(keepends=True),
                                                 actual.splitlines(keepends=True),
                                                 fromfile='CASA', tofile='native'))
        raise AssertionError(difference[:18000] or (repr(actual), repr(expected)))


def files(root, vis, products, native):
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
    ]
    for name, kind, options in cases:
        expected, actual = (root/f'{name}.{engine}.txt' for engine in ('casa', 'native'))
        inp = str(products[kind])
        assert casa.listcal(str(vis), inp, listfile=str(expected), **options) is None
        assert native.listcal(vis=str(vis), caltable=inp, listfile=str(actual), **options) is None
        same_text(actual.read_text(), expected.read_text())
    return len(cases)


def terminals(vis, caltable):
    code = """
import sys
if sys.argv[1]=='casa':
    from casatasks import listcal
else:
    from casars.tasks import listcal
assert listcal(sys.argv[2],sys.argv[3],field='FLUX',antenna='A0,A3',
               spw='0:0~2',pagerows=int(sys.argv[4])) is None
"""
    cases = [(0, ''), (-2, ''), (5, 'a\n'), (5, '\nq\n'), (5, 'q\n'), (5, '')]
    for rows, response in cases:
        outputs = []
        for engine in ('casa', 'native'):
            process = subprocess.run([sys.executable, '-c', code, engine, str(vis),
                                      str(caltable), str(rows)], input=response, text=True,
                                     capture_output=True, timeout=40)
            assert process.returncode == 0, (engine, process.stderr, process.stdout)
            outputs.append(process.stdout)
        same_text(outputs[1], outputs[0])
    return len(cases)


def errors(root, vis, caltable, native):
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
        for args in ((), (str(vis),), (str(vis), str(caltable), 1),
                     (str(vis), str(caltable), '', '', '', '', 1.5)):
            try:
                task(*args)
            except AssertionError:
                pass
            else:
                raise AssertionError(f'Invalid argument accepted {args}')
    assert existing.read_bytes() == b'keep this listing\n'
    return 8


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
    products = {name: root/f'{name}.cal' for name in ('gain', 'bandpass', 'delay')}
    reference_fixture(vis, products['gain'], 'G Jones')
    casa.bandpass(str(vis), str(products['bandpass']), refant='A0', solint='inf', minsnr=0.)
    casa.gencal(str(vis), str(products['delay']), 'sbd', parameter=[2.5])
    file_cases = files(root, vis, products, native)
    terminal_cases = terminals(vis, products['bandpass'])
    failures = errors(root, vis, products['gain'], native)
    print(f'{file_cases} file listings, {terminal_cases} terminal transcripts and {failures} error cases passed')


if __name__ == '__main__':
    main()
