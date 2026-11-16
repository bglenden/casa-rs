#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input lifecycle and bidirectional interchange with CASA 6.7.6.14.

Run with CASA's Python after installing casars and building calibrate:
    "$CASA_RS_CASA_PYTHON" tools/casa-compat/calibration_lifecycle_oracle.py \
        --native-bin target/release/calibrate --out target/lifecycle.json
CASA is used only as the independent test oracle. Failures never skip.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import tempfile
import shutil

import numpy as np
import casatasks as casa
from casatools import table
from record_casa_returns import make_cal_ms


def contents(path: Path) -> dict:
    tool = table()
    tool.open(str(path))
    try:
        return {column: tool.getcol(column) for column in tool.colnames()
                if tool.nrows() == 0 or all(tool.iscelldefined(column, row) for row in range(tool.nrows()))}
    finally:
        tool.close()


def snapshot(path: Path) -> dict:
    return {str(p.relative_to(path)): p.read_bytes() for p in path.rglob('*')
            if p.is_file() and p.name != 'table.lock'}


def equal(left: Path, right: Path, columns: list[str] | None = None) -> None:
    a, b = contents(left), contents(right)
    for name in columns or sorted(a.keys() & b.keys()):
        assert a[name].shape == b[name].shape, (left, right, name, a[name].shape, b[name].shape)
        if a[name].dtype.kind in 'fc':
            error = np.linalg.norm(a[name] - b[name]) / max(np.linalg.norm(b[name]), 1e-30)
            assert error < 1e-3, (left, right, name, error)
        else:
            np.testing.assert_array_equal(a[name], b[name], err_msg=str((left, right, name)))


def metadata(left: Path, right: Path) -> None:
    for name in ['ANTENNA', 'FIELD', 'SPECTRAL_WINDOW', 'OBSERVATION']:
        equal(left / name, right / name)
    tools = [table(), table()]
    for tool, path in zip(tools, [left, right]):
        tool.open(str(path))
    try:
        assert tools[0].info()['type'] == tools[1].info()['type'] == 'Calibration'
        for key in ['VisCal', 'ParType', 'MSName', 'PolBasis']:
            if key in tools[0].keywordnames():
                assert tools[0].getkeyword(key) == tools[1].getkeyword(key), key
        for tool, path in zip(tools, [left, right]):
            for name in ['ANTENNA','FIELD','SPECTRAL_WINDOW','OBSERVATION']:
                reference = str(tool.getkeyword(name)).removeprefix('Table: ')
                assert Path(reference).resolve() == (path/name).resolve(), (path,name,reference)
        for column in ['TIME', 'INTERVAL']:
            assert tools[0].getcolkeywords(column) == tools[1].getcolkeywords(column), column
    finally:
        for tool in tools:
            tool.close()


def rejected(function, **kwargs) -> None:
    try:
        function(**kwargs)
    except (ValueError, RuntimeError, OSError) as error:
        return error
    raise AssertionError(f'{function} accepted {kwargs}')


def run(root: Path, native_bin: Path) -> dict:
    os.environ['CASARS_CALIBRATE_BIN'] = str(native_bin.resolve())
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    vis = root / 'cal.ms'
    make_cal_ms(str(vis))
    evidence = {'casa_version': casa.version_string(), 'cases': []}

    for name, extra in [('gaincal', {'calmode': 'ap', 'smodel': [1.0]}),
                        ('bandpass', {'field': 'FLUX', 'smodel': [1.0]})]:
        args = dict(vis=str(vis), refant='A0', minsnr=0.0, minblperant=2, **extra)
        paths = [root / f'{name}.{engine}.cal' for engine in ['casa', 'native']]
        functions = [getattr(casa, name), getattr(native, name)]
        columns = ['TIME','INTERVAL','FIELD_ID','SPECTRAL_WINDOW_ID','ANTENNA1','ANTENNA2','SCAN_NUMBER','OBSERVATION_ID','CPARAM','FLAG']
        baseline = {}
        for index, append in enumerate([False, False, True]):
            for function, path in zip(functions, paths):
                value = function(caltable=str(path), append=append, **args)
                assert value['solve_tables'] == [str(path)]
                current = contents(path)
                if index == 0:
                    baseline[str(path)] = current
                else:
                    prior = baseline[str(path)]
                    # Formal errors and SNR keep each solver's baseline too;
                    # CAL-02 owns their existing numerical discrepancy.
                    for column in ['PARAMERR','SNR']:
                        if column in prior:
                            expected = np.repeat(prior[column], 2, axis=-1) if append else prior[column]
                            np.testing.assert_allclose(current[column], expected, rtol=1e-6, atol=1e-12)
            equal(*paths, columns)
            metadata(*paths)
            evidence['cases'].append(f'{name} append={append}: rows/parameters/metadata')
        for function, path in zip(functions, paths):
            before = snapshot(path)
            value = function(caltable=str(path), **{**args, 'minblperant': 100})
            assert value['solve_tables'] == [str(path)]
            assert snapshot(path) == before
            absent = path.with_name(path.name + '.empty')
            value = function(caltable=str(absent), **{**args, 'minblperant': 100})
            assert not absent.exists() and np.asarray(value['solvestats']['above_minblperant']).sum() == 0
            if name == 'bandpass' and function is casa.bandpass:
                # Its finally:return suppresses the C++ selection exception.
                swallowed = function(caltable=str(path), **{**args,'field':'missing-field'})
                assert np.asarray(swallowed['solve_tables']).size == 0
            else:
                rejected(function, caltable=str(path), **{**args, 'field': 'missing-field'})
            assert snapshot(path) == before
        # Each implementation appends to the other implementation's product.
        for function, path in zip(functions, paths[::-1]):
            function(caltable=str(path), append=True, **args)
        equal(*paths, columns)
        metadata(*paths)
        evidence['cases'].append(f'{name} no solutions, rejected selection, both interchange directions')
        missing = [root/f'{name}.append-absent.{engine}.cal' for engine in ['casa','native']]
        for function,path in zip(functions,missing):
            if function is casa.gaincal:
                rejected(function,caltable=str(path),append=True,**args)
            else:
                function(caltable=str(path),append=True,**args)
            assert path.exists() == (function is getattr(native,name))
        evidence['cases'].append(f'{name} append-to-absent native creation (documented CASA defect)')


    # MAIN may reference user-added local subtables. Both implementations
    # must keep them readable after appending and replacing the old directory.
    for function, engine in [(casa.gaincal, 'casa'), (native.gaincal, 'native')]:
        path = root / f'custom-subtable.{engine}.cal'
        args = dict(vis=str(vis), caltable=str(path), refant='A0', minsnr=0.0, minblperant=2)
        function(**args)
        extra = path / 'PROVENANCE'
        tool = table()
        tool.create(str(extra), {'ORIGIN': {'valueType': 'double'}}, nrow=1)
        tool.putcell('ORIGIN', 0, 42.0)
        tool.close()
        tool.open(str(path), nomodify=False)
        tool.putkeyword('PROVENANCE', 'Table: ' + str(extra))
        tool.close()
        function(append=True, **args)
        tool.open(str(path))
        reference = str(tool.getkeyword('PROVENANCE')).removeprefix('Table: ')
        tool.close()
        assert Path(reference).resolve() == extra.resolve()
        tool.open(reference)
        assert tool.getcell('ORIGIN', 0) == 42.0
        tool.close()
    evidence['cases'].append('append retains custom keyword subtable, independently read by CASA')

    # Removing the parameter column makes a genuinely incompatible schema.
    paths = [root/f'bad-schema.{engine}.cal' for engine in ['casa','native']]
    for function,path in zip([casa.gaincal,native.gaincal],paths):
        function(vis=str(vis),caltable=str(path),refant='A0',minsnr=0.0,minblperant=2)
        tool = table();tool.open(str(path),nomodify=False);tool.removecols('CPARAM');tool.close()
        before = snapshot(path)
        if function is casa.gaincal:
            # TableCopy copies only common columns, silently losing the gains.
            function(vis=str(vis),caltable=str(path),append=True,refant='A0',minsnr=0.0,minblperant=2)
            tool=table();tool.open(str(path));assert tool.nrows()==20 and 'CPARAM' not in tool.colnames();tool.close()
        else:
            rejected(function,vis=str(vis),caltable=str(path),append=True,refant='A0',minsnr=0.0,minblperant=2)
            assert snapshot(path) == before
    evidence['cases'].append('native refuses incompatible schema; CASA silently drops incoming gains (documented defect)')

    # Channel selection on a descending sideband exercises the append
    # frequency checks, including VI2's positive channel widths.
    descending = root / 'descending.ms'
    shutil.copytree(vis, descending)
    tool = table(); tool.open(str(descending/'SPECTRAL_WINDOW'),nomodify=False)
    tool.putcell('CHAN_FREQ',0,tool.getcell('CHAN_FREQ',0)[::-1])
    tool.putcell('CHAN_WIDTH',0,-np.abs(tool.getcell('CHAN_WIDTH',0)))
    tool.close()
    paths = [root/f'descending.{engine}.cal' for engine in ['casa','native']]
    args = dict(vis=str(descending),field='FLUX',spw='0:1~6',refant='A0',smodel=[1.0],minsnr=0.0,minblperant=2)
    for function,path in zip([casa.bandpass,native.bandpass],paths):
        function(caltable=str(path),**args)
    for function,path in zip([casa.bandpass,native.bandpass],paths[::-1]):
        function(caltable=str(path),append=True,**args)
    equal(*paths,columns);metadata(*paths)
    evidence['cases'].append('B channel selection and descending sideband interchange')

    for caltype, first, second in [('opac', [0.05], [0.12]),
                                    ('antpos', [0.01, 0.02, 0.03], [0.04, 0.05, 0.06])]:
        paths = [root / f'{caltype}.{engine}.cal' for engine in ['casa', 'native']]
        for function, path in zip([casa.gencal, native.gencal], paths):
            # Exercise CASA's positional call as well as keyword calls.
            function(str(vis), str(path), caltype, antenna='A0', parameter=first)
            function(vis=str(vis), caltable=str(path), caltype=caltype, antenna='A1', parameter=second)
            function(vis=str(vis), caltable=str(path), caltype=caltype, antenna='A0', parameter=first)
        equal(*paths)
        metadata(*paths)
        for function, path in zip([casa.gencal, native.gencal], paths[::-1]):
            function(vis=str(vis), caltable=str(path), caltype=caltype, antenna='A1', parameter=second)
        equal(*paths)
        for function, path in zip([casa.gencal, native.gencal], paths):
            before = snapshot(path)
            rejected(function, vis=str(vis), caltable=str(path), caltype='opac' if caltype == 'antpos' else 'antpos', antenna='A0', parameter=[0.1] if caltype == 'antpos' else [0.1, 0.2, 0.3])
            assert snapshot(path) == before
        evidence['cases'].append(f'gencal {caltype} selected cumulative updates, incompatible type, both interchange directions')

    # Independent input to fluxscale; duplicate-time gain solutions are not a
    # valid estimator input, so do not use the appended gaincal products.
    gain = root / 'flux-input.cal'
    casa.gaincal(vis=str(vis), caltable=str(gain), refant='A0', calmode='ap', smodel=[1.0], minsnr=0.0, minblperant=2)
    tool = table(); tool.open(str(gain), nomodify=False)
    gains = tool.getcol('CPARAM'); fields = tool.getcol('FIELD_ID')
    gains[:] = np.where(fields == 0, 1.0, 2.0)
    tool.putcol('CPARAM', gains); tool.close()
    args = dict(vis=str(vis), caltable=str(gain), reference=['FLUX'], transfer=['PHASE'])
    paths = [root / f'flux.{engine}.cal' for engine in ['casa', 'native']]
    for function, path in zip([casa.fluxscale, native.fluxscale], paths):
        function(fluxtable=str(path), **args)
        before = snapshot(path)
        assert isinstance(rejected(function, fluxtable=str(path), **args), ValueError)
        assert snapshot(path) == before
        function(fluxtable=str(path), append=True, **args)
    equal(*paths)
    metadata(*paths)
    for function, path in zip([casa.fluxscale, native.fluxscale], paths[::-1]):
        function(fluxtable=str(path), append=True, **args)
    equal(*paths)
    evidence['cases'].append('fluxscale refuses existing, append, both interchange directions')

    casa.applycal(vis=str(vis), gaintable=[str(gain)], flagbackup=False)
    output = root / 'native-export.ms'
    expected = root / 'casa-export.ms'
    casa.split(vis=str(vis), outputvis=str(expected), datacolumn='corrected')
    native.calibrate(mode='export_corrected_data', vis=str(vis), outputvis=str(output))
    equal(output, expected, ['DATA', 'FLAG', 'TIME', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID'])
    before = snapshot(output)
    rejected(native.calibrate, mode='export_corrected_data', vis=str(vis), outputvis=str(output))
    assert snapshot(output) == before
    native.calibrate(mode='export_corrected_data', vis=str(vis), outputvis=str(output), overwrite=True)
    equal(output, expected, ['DATA', 'FLAG', 'TIME', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID'])
    evidence['cases'].append('corrected export CASA-readable, refusal and explicit replacement')
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix='calibration-lifecycle-', dir=args.out.parent))
    result = run(directory, args.native_bin)
    shutil.rmtree(directory)
    args.out.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
