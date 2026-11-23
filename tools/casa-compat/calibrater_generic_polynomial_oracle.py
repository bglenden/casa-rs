#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare generic GSPLINE and isolate the pinned generic BPOLY setup crash."""

import argparse
from contextlib import chdir
import json
from pathlib import Path
import signal
from subprocess import run
import sys

import casatasks
from casatools import calibrater as casa_calibrater

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_polynomial_tool_oracle import verify_readers
from calibrater_state_oracle import casa_log, native_log
from gspline_oracle import compare_tables, source


def casa_spline_fit(vis, product, legacy, options, outcome, phase_control):
    child = """
from casatools import calibrater
import json, sys
legacy, vis, product, arguments, controlled = sys.argv[1:]
t = calibrater()
assert t.setvi(legacy == 'True', True) and t.open(vis)
assert t.selectvis(field='FLUX', spw='0:1~10') and t.setptmodel([1, 0, 0, 0])
args = json.loads(arguments)
if controlled == 'True':
    mode = 'P' if args.get('phaseonly', False) else args.get('apmode', 'AP')
    assert t.setsolvegainspline(product, mode=mode, splinetime=7200., npointaver=2,
                              phasewrap=0., preavg=args.get('preavg', -1.),
                              refant=args.get('refant', []))
else:
    assert t.setsolve(type='GSPLINE', table=product, **args)
try:
    result = t.solve(), None
except RuntimeError as failure:
    result = False, str(failure)
print('SOLVE RESULT', json.dumps([result, t.returndict()['solve_tables'].tolist()]), flush=True)
assert not t.activityrec() and t.close()
"""
    process = run([sys.executable, '-c', child, str(legacy), str(vis), str(product),
                   json.dumps(options), str(phase_control)],
                  capture_output=True, text=True, check=False)
    assert process.returncode == 0, (product.name, process.returncode, process.stderr[-4000:])
    marker = 'SOLVE RESULT '
    line = next(line for line in process.stdout.splitlines() if line.startswith(marker))
    solved, paths = json.loads(line[len(marker):])
    assert solved == ([False, 'Error in Calibrater::solve.'] if outcome == 'error' else [True, None])
    assert paths == ([] if outcome == 'error' else [str(product)])


def spline_case(work, vis, factories, name, legacy, options, poly_mode, outcome='product'):
    products, records = {}, {}
    for label, factory in factories.items():
        product = products[label] = work/f'{name}-{label}.cal'
        tool = factory()
        assert tool.setvi(legacy, True) and tool.open(str(vis))
        assert tool.selectvis(field='FLUX', spw='0:1~10') and tool.setptmodel([1, 0, 0, 0])
        assert tool.setsolve(type='GSPLINE', table=str(product), **options)
        log = casa_log(work/f'{name}.log') if label == 'casa' else native_log()
        with log as capture:
            state = capture(tool.state)
        state = state[0], [line.replace(str(product), '<table>') for line in state[1]]
        if label == 'casa':
            assert tool.close()
            # CASA's implicit wrapping controls produced different phase
            # coefficients for repeated generic input. Pin them for science.
            casa_spline_fit(vis, product, legacy, options, outcome, 'PHAS' in poly_mode)
            records[label] = state, {}
        else:
            solved = result(tool.solve)
            assert solved == ((False, ('RuntimeError', 'Error in Calibrater::solve.'))
                              if outcome == 'error' else (True, True)), (label, name, solved)
            returned = tool.returndict()
            assert list(returned['solve_tables']) == ([] if outcome == 'error' else [str(product)])
            records[label] = state, tool.activityrec()
            assert tool.close()
        assert product.exists() == (outcome == 'product')
        if outcome == 'product':
            with opened(product) as table:
                assert table.info()['subType'] == 'GSPLINE'
                assert list(table.getcol('POLY_MODE')) == [poly_mode] * 10
    if 'native' in records:
        assert records['native'][0] == records['casa'][0], records
        same_record(records['native'][1], records['casa'][1])
        if outcome == 'product':
            rows = list(range(10))
            compare_tables(products['casa'], products['native'], (rows, rows), logical_splines=True)
            for product in products.values():
                verify_readers(product, ('POLY_MODE', 'SPLINE_KNOTS_PHASE', 'POLY_COEFF_PHASE'))
    print(name, 'matched' if 'native' in records else 'reference', flush=True)


def append_spline(work, vis, factories):
    products = {}
    for name, factory in factories.items():
        out = products[name] = work/f'append-{name}.cal'
        tool = factory()
        assert tool.setvi(True, True) and tool.open(str(vis))
        for field, append in (('FLUX', False), ('PHASE', True)):
            assert tool.selectvis(field=field, spw='0:1~10') and tool.setptmodel([1, 0, 0, 0])
            assert tool.setsolve('GSPLINE', 'inf', str(out), apmode='A', append=append)
            assert tool.solve()
            with opened(out) as table:
                assert list(table.getcol('FIELD_ID')) == [0] * 5 + [1] * 5
                epoch = table.getcell('TIME', 0)
                if append:
                    assert epoch > initial_epoch
                else:
                    initial_epoch = epoch
        assert tool.close()
    if 'native' in products:
        rows = list(range(10))
        compare_tables(products['casa'], products['native'], (rows, rows), logical_splines=True)
    print('generic append refreshes the published fields', flush=True)


def bpoly_setup(work, vis, factories, legacy):
    output = work/f'generic-bpoly-{legacy}.cal'
    child = """
from casatools import calibrater
import sys
t = calibrater()
assert t.setvi(sys.argv[1] == 'True', True) and t.open(sys.argv[2])
assert t.selectvis(field='FLUX')
assert t.setptmodel([1, 0, 0, 0])
print('configured input', flush=True)
t.setsolve('BPOLY', 'inf', sys.argv[3])
"""
    process = run([sys.executable, '-c', child, str(legacy), str(vis), str(output)],
                  capture_output=True, text=True, check=False)
    assert process.returncode == -signal.SIGSEGV and 'configured input' in process.stdout, process
    assert not output.exists()
    if 'native' in factories:
        tool = factories['native']()
        assert tool.setvi(legacy, True) and tool.open(str(vis))
        assert tool.selectvis(field='FLUX')
        assert result(tool.setsolve, 'BPOLY', 'inf', str(output)) == (
            False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
        assert list(tool.returndict()['solve_tables']) == []
        assert tool.close() and not output.exists()
    print('generic BPOLY', legacy, 'isolated SIGSEGV reference', flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    factories = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    with chdir(work):
        vis, _ = source(work, 'ap', channels=12, scan_seconds=160, integration_seconds=4)
        for name, legacy, options, mode, outcome in (
            ('default', True, {}, 'PHASAMP', 'product'),
            ('amplitude', True, dict(apmode='A', refant='A3', preavg=0., t='int'), 'AMP', 'product'),
            ('phaseonly', True, dict(phaseonly=True, apmode='A', refant='A0'), 'PHAS', 'product'),
            ('preaveraged', True, dict(preavg=8., refant='A0', t='20s'), 'PHASAMP', 'product'),
            ('literal', True, dict(apmode='FOO'), 'FOO', 'product'),
            ('empty-mode', True, dict(apmode=''), '', 'product'),
            ('too-few-times', True, dict(preavg=200.), '', 'empty'),
            ('modern', False, dict(apmode='AP'), '', 'error'),
        ):
            spline_case(work, vis, factories, name, legacy, options, mode, outcome)
        append_spline(work, vis, factories)
        for legacy in (True, False):
            bpoly_setup(work, vis, factories, legacy)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
