#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the stateful initweights tool with CASA 6.7.6.14."""

import argparse
import inspect
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casatools import calibrater
import numpy as np

from applycal_oracle import copy_ms
from calibration_initweights_oracle import UNCHANGED, add_spectra, compare, fixture, main_columns, opened
from calibration_initweights_tsys_oracle import make_table


def outcome(tool, *args, **options):
    try:
        return tool.initweights(*args, **options)
    except (TypeError, SystemError, RuntimeError) as error:
        return type(error), str(error)


def compare_arguments(engines):
    pairs = [({}, False), ({'wtmode': b'ones'}, False)]
    pairs += [({'spwmap': value}, False) for value in (
        0, 1., False, [0, 1], [1.2, '0'], [[0, 1]], np.array([[1., 0.]]), np.array(0),
    )]
    pairs += [({'spwmap': value}, SystemError) for value in (
        None, (0, 1), {}, np.int64(0), np.float32(0), [2**80],
    )]
    pairs += [({'spwmap': value}, TypeError) for value in ('0', b'0')]
    pairs += [({'dowtsp': value}, TypeError) for value in (1, None, np.bool_(True))]
    pairs += [({parameter: None}, TypeError) for parameter in (
        'wtmode', 'tsystable', 'gainfield', 'interp',
    )]
    for options, expected in pairs:
        reference, actual = [outcome(factory(), **options) for factory in engines]
        assert reference == actual, (options, reference, actual)
        assert reference is False if expected is False else reference[0] is expected
    return len(pairs)


def compare_weights(root, seed, engines, old, options, spectra, failure=False):
    outputs = []
    for name, factory in zip(('casa', 'native'), engines, strict=True):
        with TemporaryDirectory(dir=root) as attempt:
            vis = Path(attempt) / f'{name}.ms'
            copy_ms(seed, vis)
            add_spectra(vis, spectra)
            tool = factory()
            assert tool.setvi(old, True) and tool.open(str(vis), False, False, False)
            assert tool.selectvis(spw='0')
            before = main_columns(vis)
            result = outcome(tool, *options) if isinstance(options, tuple) else outcome(tool, **options)
            if failure:
                assert result == (RuntimeError, 'Error in Calibrater::initWeights.'), result
                compare(before, main_columns(vis))
                assert tool.initweights(wtmode='ones') is True
            else:
                assert result is True, result
            assert tool.close() and tool.initweights() is False and tool.done()
            after = main_columns(vis)
            for column in (*UNCHANGED, 'HISTORY'):
                np.testing.assert_array_equal(after[1][column], before[1][column])
            outputs.append(after)
    compare(*outputs, rtol=5e-7)


def compare_selection(root, seed, engines, options, resets):
    for name, factory in zip(('casa', 'native'), engines, strict=True):
        with TemporaryDirectory(dir=root) as attempt:
            vis = Path(attempt) / f'{name}.ms'
            copy_ms(seed, vis)
            tool = factory()
            assert tool.setvi(True, True) and tool.open(str(vis))
            with opened(vis, write=True) as main:
                model = main.getcol('MODEL_DATA')
                main.putcol('MODEL_DATA', np.full_like(model, 53.))
                selected = main.getcol('DATA_DESC_ID') == 0
            assert selected.any() and not selected.all() and tool.selectvis(spw='0')
            assert outcome(tool, **options) in (True, (RuntimeError, 'Error in Calibrater::initWeights.'))
            assert tool.initcalset(0) and tool.done()
            with opened(vis) as main:
                model = main.getcol('MODEL_DATA')
            np.testing.assert_array_equal(model[..., selected], 1.)
            np.testing.assert_array_equal(model[..., ~selected], 1. if resets else 53.)


def run(root):
    from casars.tools import calibrater as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    engines = (calibrater, native)
    assert inspect.signature(calibrater.initweights) == inspect.signature(native.initweights)
    controls = compare_arguments(engines)
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        seed = work / 'fixture.ms'
        fixture(seed)
        tsys = str(make_table(work, seed))
        cases = [
            ({}, '', False),
            (('nyq', True), 'partial', False),
            ({'wtmode': 'sigma'}, 'partial', False),
            ({'wtmode': b'ones'}, '', False),
            (('weight', True, 'unused', 'FLUX', 'invalid', [1, 0]), '', False),
            ({'wtmode': 'delwtsp'}, 'full', False),
            ({'wtmode': 'delsigsp', 'dowtsp': True}, 'full', False),
            ({'wtmode': 'tsys', 'tsystable': tsys}, '', False),
            (('tinttsys', True, tsys, '1', 'nearest,linear', np.array([[1., 0.]])), 'partial', False),
            ({'wtmode': 'tsys', 'tsystable': tsys, 'dowtsp': True, 'spwmap': [[1.2, '0']]}, '', False),
            ({'wtmode': 'tsys', 'tsystable': tsys, 'gainfield': 'nearest', 'spwmap': -1}, '', False),
            ({'wtmode': 'ONES'}, '', True),
            ({'wtmode': 'weight'}, 'full', True),
            ({'wtmode': 'tsys', 'tsystable': str(work / 'missing.cal')}, '', True),
            ({'wtmode': 'tsys', 'tsystable': tsys, 'interp': 'bad'}, '', True),
        ]
        for old in (False, True):
            for options, spectra, failure in cases:
                compare_weights(work, seed, engines, old, options, spectra, failure)
            print('iterator', 'legacy' if old else 'modern', len(cases), 'persisted comparisons', flush=True)
        selection = [
            ({'wtmode': 'ones'}, False),
            ({'wtmode': 'bad'}, False),
            ({'wtmode': 'tsys', 'tsystable': str(seed)}, False),
            ({'wtmode': 'tsys', 'tsystable': tsys}, True),
            ({'wtmode': 'tinttsys', 'tsystable': tsys, 'interp': 'bad'}, True),
            ({'wtmode': 'badtsys', 'tsystable': tsys}, True),
        ]
        for options, resets in selection:
            compare_selection(work, seed, engines, options, resets)
    results = {'persisted_cases': 2 * len(cases), 'selection_cases': len(selection), 'argument_cases': controls}
    print(results, flush=True)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    run(parser.parse_args().work.resolve())
