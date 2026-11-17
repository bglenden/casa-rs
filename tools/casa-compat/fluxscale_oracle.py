#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input public-Python fluxscale equivalence, including all 15 parameters.

Run with CASA 6.7.6.14 Python and an installed casars package. The work
directory must be empty; all generated data and receipts stay there.
CASA is an independent oracle, never a native runtime dependency.
"""
from __future__ import annotations

import argparse
import ast
import inspect
import json
import os
from pathlib import Path
import re
import shutil

import numpy as np
import casatasks as casa
from casatools import table, quanta

from calibration_lifecycle_oracle import contents, equal, metadata, snapshot
from record_casa_returns import make_cal_ms


def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value.tolist() if hasattr(value, 'tolist') else value


def history(vis, first=0):
    tool = table()
    tool.open(str(vis/'HISTORY'))
    rows = [{name: plain(tool.getcell(name, row)) for name in tool.colnames() if name != 'TIME'}
            for row in range(first, tool.nrows())]
    count = tool.nrows()
    tool.close()
    return count, rows


def compare_history(left, right, paths, name):
    assert len(left) == len(right), (name, 'HISTORY count', len(left), len(right))
    for a, b in zip(left, right):
        for column in a:
            if column == 'OBJECT_ID':
                # MSHistoryHandler never initializes this required column.
                assert b[column] == 0
            elif column != 'MESSAGE':
                compare(a[column], b[column], f'{name}/HISTORY/{column}')
        message_a = a['MESSAGE'].replace(str(paths[0]), '<output>')
        message_b = b['MESSAGE'].replace(str(paths[1]), '<output>')
        if message_a.startswith('version:'):
            assert message_b.startswith('version: casa-rs ')
        elif message_a.split('=')[0].strip() in ['fitorder', 'refspwmap']:
            # The native provider records effective integers; CASA's Python
            # history can retain equivalent bool subclass spellings.
            key_a, value_a = message_a.split('=', 1)
            key_b, value_b = message_b.split('=', 1)
            assert key_a == key_b
            assert ast.literal_eval(value_a.strip()) == ast.literal_eval(value_b.strip())
        elif name == 'fit-4' and ('Spectral index fitting' in message_a or 'Fitted flux density' in message_a):
            # The documented zero-DOF roundoff correction also appears in HISTORY.
            assert message_a.split('+/-')[0] == message_b.split('+/-')[0]
        else:
            assert message_a == message_b, (name, message_a, message_b)


def compare(a, b, context='return'):
    if isinstance(a, dict):
        assert a.keys() == b.keys(), (context, a.keys(), b.keys())
        exact_fit_errors = set()
        if 'spidx' in a and len(a['spidx']) > 2:
            valid = sum(isinstance(value, dict) and 'fluxd' in value and value['fluxd'][0] > 0
                        for value in a.values())
            if valid == len(a['spidx']):
                # With no residual degrees of freedom, CASA's moment subtraction
                # can create tiny positive errors from roundoff. Native keeps
                # zero; bound the artifact relative to the fitted quantity
                # using the project's 1e-3 scientific accuracy policy.
                np.testing.assert_array_equal(b['spidxerr'], np.zeros(valid))
                assert b['fitFluxdErr'] == 0
                assert np.all(np.abs(a['spidxerr']) <= 1e-3*np.maximum(1., np.abs(a['spidx']))), a
                assert abs(a['fitFluxdErr']) <= 1e-3*max(1., abs(a['fitFluxd'])), a
                exact_fit_errors = {'spidxerr', 'fitFluxdErr'}
        for key in a:
            if key not in exact_fit_errors:
                compare(a[key], b[key], f'{context}/{key}')
    elif np.asarray(a).dtype.kind in 'biufc':
        # Preserve useful absolute accuracy for zero/tiny fitted uncertainties.
        np.testing.assert_allclose(a, b, rtol=1e-6, atol=1e-7, equal_nan=True, err_msg=context)
    else:
        np.testing.assert_array_equal(a, b, err_msg=context)


def compare_listing(left, right):
    number = re.compile(r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|[-+]?(?:inf|nan)', re.I)
    lines = lambda text: [line.lstrip('# ').strip() for line in text.splitlines() if line.strip()]
    a, b = lines(left), lines(right)
    assert len(a) == len(b), (a, b)
    for x, y in zip(a, b):
        assert re.sub(r'\s+', '', number.sub('<n>', x)) == re.sub(r'\s+', '', number.sub('<n>', y)), (x, y)
        # Both text writers print six significant digits, separately from the
        # stricter floating return/caltable comparisons.
        np.testing.assert_allclose([float(v) for v in number.findall(x)],
                                   [float(v) for v in number.findall(y)], rtol=2e-5, atol=1e-7)


def invoke(module, args, positional=False):
    preload = os.environ.pop('LD_PRELOAD', None) if module.__name__.startswith('casars') else None
    try:
        if positional:
            names = list(inspect.signature(casa.fluxscale).parameters)
            defaults = inspect.signature(casa.fluxscale).parameters
            return module.fluxscale(*(args.get(name, defaults[name].default) for name in names))
        return module.fluxscale(**args)
    finally:
        if preload is not None:
            os.environ['LD_PRELOAD'] = preload


def set_scans(gain):
    tool = table()
    tool.open(str(gain), nomodify=False)
    times, fields = tool.getcol('TIME'), tool.getcol('FIELD_ID')
    scan = np.empty(len(times), dtype=np.int32)
    for field in set(fields):
        for index, time in enumerate(sorted(set(times[fields == field]))):
            scan[(fields == field) & (times == time)] = 1 + index % 3
    tool.putcol('SCAN_NUMBER', scan)
    tool.close()
    return times


def multi_window(vis, gain, root, windows=3, name='multi'):
    multi_vis, multi_gain = root/f'{name}.ms', root/f'{name}.gcal'
    offsets = [10e9, 25e9, 40e9, 60e9][:windows-1]
    # CASA simulator MAIN row counts can reside in table.lock; deep copy with
    # casacore rather than omitting that file from an unmaterialized MS copy.
    tool = table()
    tool.open(str(vis))
    copied = tool.copy(str(multi_vis), deep=True, valuecopy=True)
    copied.close()
    tool.close()
    shutil.copytree(gain, multi_gain)
    for parent in [multi_vis, multi_gain]:
        tool.open(str(parent/'SPECTRAL_WINDOW'), nomodify=False)
        row = {col: tool.getcell(col, 0) for col in tool.colnames() if tool.iscelldefined(col, 0)}
        tool.addrows(len(offsets))
        for index, offset in enumerate(offsets, 1):
            for col, value in row.items():
                if col in ['CHAN_FREQ', 'REF_FREQUENCY']:
                    value = value + offset
                if col == 'NAME':
                    value = f'SPW{index}'
                tool.putcell(col, index, value)
        tool.close()
    tool.open(str(multi_gain), nomodify=False)
    count = tool.nrows()
    rows = [{col: tool.getcell(col, row) for col in tool.colnames() if tool.iscelldefined(col, row)}
            for row in range(count)]
    tool.addrows(count*len(offsets))
    for spw, factor in enumerate([1.2, 1.7, 1.4, 2.3][:len(offsets)], 1):
        for index, row in enumerate(rows):
            for col, value in row.items():
                if col == 'SPECTRAL_WINDOW_ID':
                    value = spw
                if col == 'CPARAM' and row['FIELD_ID'] == 1:
                    value = value*factor*(1 + .02*spw*row['ANTENNA1'])
                if col == 'CPARAM' and row['FIELD_ID'] == 0:
                    value = value*[1.4, .8, 1.2, 1.6][spw-1]
                tool.putcell(col, spw*count+index, value)
    tool.close()
    return multi_vis, multi_gain


def run(root, native_bin):
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    reference_signature = inspect.signature(casa.fluxscale)
    native_signature = inspect.signature(native.fluxscale)
    public = {name: parameter for name, parameter in native_signature.parameters.items()
              if parameter.kind != inspect.Parameter.KEYWORD_ONLY}
    assert list(public) == list(reference_signature.parameters)
    for name, parameter in reference_signature.parameters.items():
        assert public[name].kind == parameter.kind, name
        assert public[name].default == parameter.default, name
    os.environ['CASARS_CALIBRATE_BIN'] = str(native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    os.chdir(root)
    vis, gain = root/'original.ms', root/'original.gcal'
    make_cal_ms(str(vis))
    casa.gaincal(vis=str(vis), caltable=str(gain), refant='A0', solint='int')
    results = dict(casa_version=casa.version_string(), signature=str(inspect.signature(casa.fluxscale)), cases={}, comparison_failures=[])

    def case(name, extra=None, source=gain, dataset=vis, reject=False, positional=False):
        args = dict(vis=str(dataset), caltable=str(source), reference=['FLUX'], transfer=['PHASE'])
        args.update(extra or {})
        before = snapshot(source)
        records, paths = [], []
        for label, module in [('casa', casa), ('native', native)]:
            path = root/f'{name}.{label}.fluxcal'
            paths.append(path)
            effective = dict(args, fluxtable=str(path))
            history_start, _ = history(dataset)
            try:
                value = invoke(module, effective, positional)
            except (AssertionError, ValueError, RuntimeError, TypeError, OSError) as error:
                records.append(dict(error=type(error).__name__, message=str(error)))
            else:
                records.append(dict(value=plain(value)))
                if effective.get('listfile'):
                    records[-1]['listing'] = Path(effective['listfile']).read_text()
            _, records[-1]['history'] = history(dataset, history_start)
            if ('error' in records[-1]) != reject:
                results['comparison_failures'].append(dict(case=name, message=f'{label}: expected rejection={reject}, got {records[-1].get("message", "success")}'))
        assert before == snapshot(source), f'{name}: input gain table changed'
        if not reject and all('value' in record for record in records):
            try:
                compare(records[0]['value'], records[1]['value'], name)
                a, b = contents(paths[0]), contents(paths[1])
                assert a.keys() == b.keys(), (name, a.keys(), b.keys())
                equal(*paths)
                for column in ['TIME', 'INTERVAL']:
                    np.testing.assert_allclose(a[column], b[column], rtol=0, atol=1e-4,
                                               err_msg=f'{name}/{column}')
                metadata(*paths)
                if records[0].get('listing') is not None:
                    compare_listing(records[0]['listing'], records[1]['listing'])
            except AssertionError as error:
                results['comparison_failures'].append(dict(case=name, message=str(error)))
        try:
            compare_history(records[0]['history'], records[1]['history'], paths, name)
        except AssertionError as error:
            results['comparison_failures'].append(dict(case=name, message=str(error)))
        results['cases'][name] = dict(arguments=plain(extra or {}), casa=records[0], native=records[1])
        (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')
        failed = any(item['case'] == name for item in results['comparison_failures'])
        print(name, 'FAILED' if failed else 'passed', flush=True)
        return paths

    case('original')
    t_gain = root/'t.gcal'
    casa.gaincal(vis=str(vis), caltable=str(t_gain), refant='A0', solint='int', gaintype='T')
    case('t-jones', source=t_gain)
    case('t-incremental', dict(incremental=True), source=t_gain)
    selected = root/'selected.gcal'
    shutil.copytree(gain, selected)
    times = set_scans(selected)
    qa = quanta()
    stamp = lambda time: qa.time(qa.quantity(float(time), 's'), form='ymd', prec=10)[0]
    unique = sorted(set(times))
    timerange = stamp(unique[1]) + '~' + stamp(unique[-2])
    # Partial antenna restrictions suppress amplitude samples after CTIter has
    # assigned its reference time/frequency and checked timestamp uniqueness.
    sparse = root/'sparse.gcal'
    shutil.copytree(selected, sparse)
    tool = table()
    tool.open(str(sparse), nomodify=False)
    ids, ants, stamps = (tool.getcol(name) for name in ['FIELD_ID', 'ANTENNA1', 'TIME'])
    tool.putcol('ANTENNA2', np.full(tool.nrows(), -1, dtype=np.int32))
    tool.putcol('FLAG', np.zeros_like(tool.getcol('FLAG')))
    tool.removerows(np.flatnonzero((ids == 1) & (stamps == min(stamps[ids == 1])) & (ants != 0)).tolist())
    tool.close()
    case('partial-sparse-time', dict(antenna='!0', scan='2', incremental=True), source=sparse)
    suppressed_duplicate = root/'suppressed-duplicate.gcal'
    shutil.copytree(sparse, suppressed_duplicate)
    tool.open(str(suppressed_duplicate), nomodify=False)
    ids = np.flatnonzero(tool.getcol('FIELD_ID') == 1)
    row = {name: tool.getcell(name, int(ids[0])) for name in tool.colnames() if tool.iscelldefined(name, int(ids[0]))}
    last = tool.nrows()
    tool.addrows(1)
    for name, value in row.items():
        tool.putcell(name, last, value)
    tool.close()
    case('partial-suppressed-duplicate', dict(antenna='!0', scan='2'), source=suppressed_duplicate, reject=True)
    for name, extra in [
        ('antenna-id', dict(antenna='0')), ('antenna-name', dict(antenna='A0')),
        ('antenna-range', dict(antenna='0~2')), ('antenna-negated', dict(antenna='!0')),
        ('antenna-auto', dict(antenna='0&&')), ('baseline', dict(antenna='0&1')),
        ('time', dict(timerange=timerange)), ('scan-list', dict(scan='1,3')),
        ('scan-comparison', dict(scan='>1')), ('partial-time', dict(antenna='!0', timerange=timerange)),
        ('partial-scan', dict(antenna='!0', scan='1')), ('reference-regex', dict(reference=['/FL.*/'])),
        ('reference-part-missing', dict(reference='0,999')), ('transfer-default', dict(transfer=[])),
        ('threshold', dict(gainthreshold=.1)), ('threshold-large', dict(gainthreshold=2)),
        ('threshold-negative', dict(gainthreshold=-2)), ('numeric-bool', dict(refspwmap=False, gainthreshold=True, fitorder=True)),
        ('fit-zero-single', dict(fitorder=0)), ('display', dict(display=True)),
        ('incremental', dict(incremental=True)), ('positional', {})]:
        case(name, extra, source=selected, positional=name == 'positional')
    case('listfile', dict(listfile=str(root/'flux.txt')), source=selected)
    assert 'Flux density for PHASE' in (root/'flux.txt').read_text()
    unequal = root/'unequal.gcal'
    shutil.copytree(gain, unequal)
    tool = table()
    tool.open(str(unequal), nomodify=False)
    tool.putcol('INTERVAL', np.resize([1., 5., 10.], tool.nrows()))
    flags, fields, antennas = tool.getcol('FLAG'), tool.getcol('FIELD_ID'), tool.getcol('ANTENNA1')
    flags[1, :, (fields == 0) & (antennas == 4)] = True
    index = np.flatnonzero((fields == 1) & (antennas == 0) & ~flags[0, 0])[0]
    flags[0, 0, index] = True
    tool.putcol('FLAG', flags)
    tool.close()
    case('unequal-flagged', source=unequal)
    quoted_vis, quoted_gain = root/'quoted.ms', root/'quoted.gcal'
    tool.open(str(vis))
    copied = tool.copy(str(quoted_vis), deep=True, valuecopy=True)
    copied.close()
    tool.close()
    shutil.copytree(selected, quoted_gain)
    for parent in [quoted_vis, quoted_gain]:
        tool.open(str(parent/'FIELD'), nomodify=False)
        tool.putcell('NAME', 0, 'FLUX, field')
        tool.putcell('NAME', 1, 'PHASE$dollar')
        tool.close()
    case('quoted-fields', dict(reference='"FLUX, field"', transfer='"PHASE$dollar"'),
         source=quoted_gain, dataset=quoted_vis)
    reference_vis, reference_gain = root/'references.ms', root/'references.gcal'
    tool.open(str(vis))
    copied = tool.copy(str(reference_vis), deep=True, valuecopy=True)
    copied.close()
    tool.close()
    shutil.copytree(selected, reference_gain)
    for parent in [reference_vis, reference_gain]:
        tool.open(str(parent/'FIELD'), nomodify=False)
        row = {name: tool.getcell(name, 0) for name in tool.colnames() if tool.iscelldefined(name, 0)}
        tool.addrows(1)
        for name, value in row.items():
            tool.putcell(name, 2, 'REF2' if name == 'NAME' else value)
        tool.close()
    case('unavailable-reference', dict(reference=['FLUX', 'REF2']), source=reference_gain, dataset=reference_vis)
    tool.open(str(reference_gain), nomodify=False)
    ids = np.flatnonzero(tool.getcol('FIELD_ID') == 0)
    rows = [{col: tool.getcell(col, int(row)) for col in tool.colnames() if tool.iscelldefined(col, int(row))} for row in ids]
    first = tool.nrows()
    tool.addrows(len(rows))
    for index, row in enumerate(rows):
        for col, value in row.items():
            if col == 'FIELD_ID': value = 2
            if col == 'TIME': value += 1000.
            if col == 'CPARAM': value *= 3.
            tool.putcell(col, first+index, value)
    tool.close()
    case('multiple-references', dict(reference=['FLUX', 'REF2']), source=reference_gain, dataset=reference_vis)
    case('output-field-subset', source=reference_gain, dataset=reference_vis)
    unmatched = root/'unmatched.gcal'
    shutil.copytree(reference_gain, unmatched)
    tool.open(str(unmatched), nomodify=False)
    ids, ants = tool.getcol('FIELD_ID'), tool.getcol('ANTENNA1')
    tool.putcol('FLAG', np.zeros_like(tool.getcol('FLAG')))
    tool.removerows(np.flatnonzero(((ids != 2) & (ants != 0)) | ((ids == 2) & (ants != 1))).tolist())
    tool.close()
    case('unmatched-transfer', dict(transfer=['PHASE', 'REF2']), source=unmatched, dataset=reference_vis)
    for parent in [reference_vis, reference_gain]:
        tool.open(str(parent/'FIELD'), nomodify=False)
        tool.putcell('NAME', 2, '')
        tool.close()
    case('blank-reference-token', dict(reference=['', 'FLUX']), source=reference_gain, dataset=reference_vis, reject=True)
    multi_vis, multi_gain = multi_window(vis, selected, root)
    case('listfile-mapped', dict(listfile=str(root/'mapped.txt'), refspwmap=[0]), source=multi_gain, dataset=multi_vis)
    sparse_vis, sparse_gain = multi_window(vis, sparse, root, name='sparse-windows')
    tool.open(str(sparse_gain), nomodify=False)
    spws, scans, ants = (tool.getcol(name) for name in ['SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'ANTENNA1'])
    tool.removerows(np.flatnonzero((spws == 2) & ((scans != 1) | (ants != 0))).tolist())
    tool.close()
    case('partial-frequency', dict(antenna='!0', scan='2'), source=sparse_gain, dataset=sparse_vis)
    for name, extra in [
        ('fit-zero', dict(fitorder=0)), ('fit-one', dict(fitorder=1)), ('fit-two', dict(fitorder=2)),
        ('map-broadcast', dict(refspwmap=[0])), ('map-vector', dict(refspwmap=[0, 0, 1])),
        ('map-disabled', dict(refspwmap=[-1, 0, 0])), ('map-bool-list', dict(refspwmap=[False])),
        ('multi-incremental', dict(incremental=True))]:
        case(name, extra, source=multi_gain, dataset=multi_vis)
    higher_vis, higher_gain = multi_window(vis, selected, root, windows=5, name='higher')
    for order in [3, 4]:
        case(f'fit-{order}', dict(fitorder=order), source=higher_gain, dataset=higher_vis)
    two_gain = root/'two.gcal'
    shutil.copytree(multi_gain, two_gain)
    tool.open(str(two_gain), nomodify=False)
    tool.removerows(np.flatnonzero(tool.getcol('SPECTRAL_WINDOW_ID') == 2).tolist())
    tool.close()
    case('missing-window', source=two_gain, dataset=multi_vis)
    case('listfile-missing', dict(listfile=str(root/'missing.txt')), source=two_gain, dataset=multi_vis)
    for name, extra in [
        ('vis-list', dict(vis=[str(vis)])),
        ('bad-reference', dict(reference='999')), ('empty-reference', dict(reference=[])),
        ('no-transfer', dict(reference='0~1')), ('bad-transfer', dict(transfer='999')),
        ('bad-antenna', dict(antenna='999')), ('bad-time', dict(timerange='garbage')),
        ('bad-scan', dict(scan='garbage')), ('empty-scan', dict(scan='999')),
        ('zero-threshold', dict(gainthreshold=0)), ('string-threshold', dict(gainthreshold='.1')),
        ('float-map', dict(refspwmap=[0.])), ('float-order', dict(fitorder=1.)),
        ('integer-bool', dict(incremental=1)), ('numeric-reference', dict(reference=0))]:
        case(name, extra, source=selected, reject=True)
    # Append products from either implementation, with duplicate timestamps
    # deliberately refused when used as a new fluxscale estimation input.
    paths = case('append-seed', source=selected)
    for module, path in zip([casa, native], paths):
        invoke(module, dict(vis=str(vis), caltable=str(selected), fluxtable=str(path),
                            reference='FLUX', transfer='PHASE', append=True))
    equal(*paths)
    metadata(*paths)
    for module, path in zip([casa, native], paths[::-1]):
        invoke(module, dict(vis=str(vis), caltable=str(selected), fluxtable=str(path),
                            reference='FLUX', transfer='PHASE', append=True))
    equal(*paths)
    metadata(*paths)
    results['cases']['append-exchange'] = dict(status='passed', rows=180)
    case('duplicate-timestamp', source=paths[0], reject=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), f'work directory must be empty: {root}'
    results = run(root, args.native_bin.resolve())
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')
    assert not results['comparison_failures'], [(item['case'], item['message'][:300]) for item in results['comparison_failures']]


if __name__ == '__main__':
    main()
