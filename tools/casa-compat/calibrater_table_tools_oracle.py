#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public calibrater table operations against pinned CASA 6.7.6.14."""

import argparse
import inspect
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import calibrater
import numpy as np

from calibration_rerefant_oracle import reference_fixture
from calibration_smoothcal_oracle import columns, compare_mapping, copy_ms, fixture, opened, synthetic_table
from calibration_table_tools_oracle import compare_tables, ordered_view


def outcome(tool, method, *args, **kwargs):
    try:
        return getattr(tool, method)(*args, **kwargs)
    except (RuntimeError, TypeError, SystemError) as error:
        return type(error), str(error)


def attach(factory, vis, old=False, scratch=False):
    tool = factory()
    assert tool.setvi(old, True) and tool.open(str(vis), False, scratch, scratch)
    return tool


def api_controls(work, vis, engines):
    cases = (
        ('accumulate', (), {}, False),
        ('accumulate', (), {'spwmap': '0'}, (TypeError, 'argument _spwmap must not be a string')),
        ('createcaltable', ('', 'Complex', 'G Jones', False), {}, False),
        ('createcaltable', ('', 'Complex', 'G Jones', 1), {}, (TypeError, "in method 'calibrater_createcaltable', argument 5 of type 'bool'")),
        ('smooth', ('', ''), {}, False),
        ('rerefant', ('', ''), {}, False),
        ('updatecaltable', (str(vis),), {}, True),
        ('updatecaltable', (str(work / 'absent.cal'),), {}, (RuntimeError, f'Table {work / "absent.cal"} does not exist')),
    )
    for factory in engines:
        tool = factory()
        for method, args, options, expected in cases:
            actual = outcome(tool, method, *args, **options)
            assert actual == expected, (factory, method, options, actual, expected)
        assert tool.open(str(vis), False, False, False)
        assert outcome(tool, 'accumulate') == (RuntimeError, 'Calibrater::accumulate not implemented')
        assert outcome(tool, 'createcaltable', str(vis), 'Complex', 'G Jones', True) == (
            RuntimeError, f'Invalid Table operation: Cannot copy/rename; target table {vis} is still open (is in the table cache)')
        assert tool.done() and tool.accumulate() is False
    # Pinned CASA can terminate with a pending exception on this invalid value.
    assert outcome(engines[1](), 'accumulate', t=None) == (
        TypeError, "in method 'calibrater_accumulate', argument 8 of type 'double'")
    return len(cases) + 4


def creation_and_repair(work, vis, source, engines):
    for index, (partype, caltype, single) in enumerate((
        ('Complex', 'G Jones', False), ('Float', 'K Jones', True), ('complex', 'arbitrary', True),
    )):
        outputs = []
        for name, factory in zip(('casa', 'native'), engines, strict=True):
            destination = work / f'{name}-new-{index}.cal'
            copy_ms(source, destination)
            tool = attach(factory, vis)
            if index == 1:
                assert tool.createcaltable(caltable=str(destination), partype=partype,
                                           caltype=caltype, singlechan=single)
            else:
                assert tool.createcaltable(str(destination), partype, caltype, single)
            assert tool.done()
            outputs.append(destination)
        compare_tables(outputs[1], outputs[0], empty=True)
    outputs = []
    for name, factory in zip(('casa', 'native'), engines, strict=True):
        dataset = work / f'{name}-container.ms'
        copy_ms(vis, dataset)
        before = columns(dataset)
        tool = attach(factory, dataset)
        destination = dataset / 'nested.cal'
        assert tool.createcaltable(str(destination), 'Complex', 'G Jones', False) and tool.done()
        compare_mapping(columns(dataset), before, False)
        outputs.append(destination)
    # Each product legitimately names its own attached MeasurementSet.
    for output in outputs:
        with opened(output) as table:
            assert table.info()['type'] == 'Calibration' and table.nrows() == 0
            assert table.getkeyword('MSName') == output.parent.name
    for condition in ('complete', 'column', 'subtable', 'both'):
        outputs = []
        for name, factory in zip(('casa', 'native'), engines, strict=True):
            destination = work / f'{name}-{condition}.cal'
            copy_ms(source, destination)
            if condition != 'complete':
                with opened(destination, True) as table:
                    if condition in ('column', 'both'):
                        assert table.removecols('OBSERVATION_ID')
                    if condition in ('subtable', 'both'):
                        assert table.removekeyword('OBSERVATION')
                if condition in ('subtable', 'both'):
                    shutil.rmtree(destination / 'OBSERVATION')
            inode = (destination / 'table.dat').stat().st_ino
            storage = {file: file.read_bytes() for file in destination.glob('table.f*')
                       if file.is_file()} if name == 'native' else {}
            tool = factory()
            assert tool.updatecaltable(caltable=str(destination)) and tool.done()
            if condition == 'complete':
                assert (destination / 'table.dat').stat().st_ino == inode
            for file, contents in storage.items():
                assert file.read_bytes() == contents, file.name
            outputs.append(destination)
        compare_tables(outputs[1], outputs[0])
    return 8


def accumulation(work, vis, gains, solved, engines):
    ordered = work / 'ordered.cal'
    ordered_view(solved, ordered)
    cases = (
        ('plain', gains, gains, {}, False, True),
        ('in-place', gains, gains, {'interp': b'nearest'}, True, True),
        ('field-map', solved, ordered, {'field': ['FLUX'], 'calfield': ['FLUX'],
                                        'interp': 'nearest', 'spwmap': np.array([0])}, False, False),
        ('grid', None, ordered, {'t': 15., 'interp': 'nearest,linear'}, False, False),
    )
    for label, parent, incremental, options, alias, payloads in cases:
        outputs = []
        for name, factory in zip(('casa', 'native'), engines, strict=True):
            output = work / f'{name}-accumulate-{label}.cal'
            if alias:
                copy_ms(parent, output)
            tool = attach(factory, vis, old=True)
            assert outcome(tool, 'accumulate') == (RuntimeError, 'Error in Calibrater::accumulate.')
            if label == 'plain':
                assert tool.accumulate(str(parent), str(incremental), str(output))
            else:
                assert tool.accumulate(tablein=str(output if alias else parent or ''),
                                       incrtable=str(incremental), tableout='' if alias else str(output),
                                       **options)
            assert tool.done()
            outputs.append(output)
        compare_tables(outputs[1], outputs[0], flagged_payloads=payloads)
    return len(cases)


def accumulation_selection(work, vis, engines):
    cases = ((0., True, False), (15., True, True), (15., False, False))
    for index, (cadence, valid, resets) in enumerate(cases):
        for name, factory in zip(('casa', 'native'), engines, strict=True):
            with TemporaryDirectory(dir=work) as attempt:
                root = Path(attempt)
                dataset = root / f'{name}-selected.ms'
                copy_ms(vis, dataset)
                tool = attach(factory, dataset, old=True, scratch=True)
                with opened(dataset, True) as table:
                    model = table.getcol('MODEL_DATA')
                    table.putcol('MODEL_DATA', np.full_like(model, 53.))
                    selected = table.getcol('FIELD_ID') == 0
                assert selected.any() and not selected.all() and tool.selectvis(field='0')
                options = dict(incrtable=str(work / ('ordered.cal' if valid else 'missing.cal')),
                               tableout=str(root / f'grid-{index}.cal'), t=cadence)
                expected = True if valid else (RuntimeError, 'Error in Calibrater::accumulate.')
                assert outcome(tool, 'accumulate', **options) == expected
                assert tool.initcalset(0) and tool.done()
                with opened(dataset) as table:
                    model = table.getcol('MODEL_DATA')
                np.testing.assert_array_equal(model[..., selected], 1.)
                np.testing.assert_array_equal(model[..., ~selected], 1. if resets else 53.)
    return len(cases)


def smoothing_and_reference(work, vis, solved, gains, engines):
    fringe = work / 'fringe.cal'
    synthetic_table(vis, fringe, 'Fringe Jones', True, 8)
    cases = (
        ('smooth', 'median', solved, (), {}, False),
        ('smooth', 'global', solved, (), dict(smoothtime=float('inf')), False),
        ('smooth', 'alias', solved, (['FLUX'], 'mean', 30., False), {}, True),
        ('smooth', 'rates', fringe, (), dict(smoothtype='mean', smoothtime=1536., ratesmooth=True), False),
        ('rerefant', 'strict', gains, ('strict', [0, 1, 3]), {}, False),
        ('rerefant', 'alias', gains, (), dict(refantmode='flex', refant='A0,A3'), True),
    )
    for method, label, source, args, options, alias in cases:
        outputs = []
        for name, factory in zip(('casa', 'native'), engines, strict=True):
            output = work / f'{name}-{method}-{label}.cal'
            if alias:
                copy_ms(source, output)
            tool = attach(factory, vis)
            input_name, output_name = str(output if alias else source), '' if alias else str(output)
            if method == 'rerefant':
                assert outcome(tool, method, input_name, output_name) == (RuntimeError, 'Error in Calibrater::reRefant.')
            else:
                assert outcome(tool, method, input_name, output_name, smoothtime=0) == (RuntimeError, 'Error in Calibrater::smooth.')
            assert getattr(tool, method)(input_name, output_name, *args, **options)
            assert tool.done()
            outputs.append(columns(output))
        compare_mapping(outputs[1], outputs[0], False)
    from casars.tasks import smoothcal as native_smoothcal

    for name, task in (('casa', casatasks.smoothcal), ('native', native_smoothcal)):
        output = work / f'{name}-smoothcal-global.cal'
        assert task(str(vis), str(solved), str(output), smoothtime=float('inf')) is None
        compare_mapping(columns(output), columns(work / 'casa-smooth-global.cal'), False)
    return len(cases) + 1


def run(root):
    from casars.tools import calibrater as native

    assert casatasks.version_string() == '6.7.6.14'
    engines = (calibrater, native)
    for method in ('accumulate', 'createcaltable', 'updatecaltable', 'smooth', 'rerefant'):
        assert inspect.signature(getattr(calibrater(), method)) == inspect.signature(getattr(native(), method))
    root.mkdir(parents=True)
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        vis, solved, gains = (work / name for name in ('source.ms', 'solved.cal', 'gains.cal'))
        fixture(vis, solved)
        reference_fixture(vis, gains, 'G Jones')
        results = {
            'api_controls': api_controls(work, vis, engines),
            'creation_and_repair': creation_and_repair(work, vis, gains, engines),
            'accumulation': accumulation(work, vis, gains, solved, engines),
            'accumulation_selection': accumulation_selection(work, vis, engines),
            'smoothing_and_reference': smoothing_and_reference(work, vis, solved, gains, engines),
        }
    print(results, flush=True)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    run(parser.parse_args().work.resolve())
