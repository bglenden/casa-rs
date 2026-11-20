#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare term switches and refusal timing on pinned legacy A/W prediction."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import image, imager, table
import numpy as np

from calibration_ft_oracle import component, coordinates
from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import cache_contents, configured_imager, data, ft, make_input, recipe


ACCEPTED = (
    {},
    {'psterm': False},
    {'aterm': False},
    {'mterm': False},
    {'wbawp': True},
    {'psterm': False, 'mterm': False, 'wbawp': True},
    {'aterm': False, 'mterm': False, 'wbawp': True},
)
DISABLED = {'psterm': False, 'aterm': False}


def set_options(tool, cache, machine, correction, switches):
    assert tool.setoptions(ftmachine=machine, wprojplanes=1, numthreads=1,
                           cfcachedirname=str(cache), dopbgriddingcorrections=correction,
                           **switches)


def configure(factory, dataset, cache, machine, correction, scratch, switches):
    tool = configured_imager(factory, dataset, cache, scratch=scratch,
                             spw='0:0', machine='ft')
    set_options(tool, cache, machine, correction, switches)
    return tool


def compare_switches(root, dataset, model, cache, machine, correction, engines):
    previous = cache_contents(cache)
    baseline = {}
    errors = []
    for switches in ACCEPTED:
        for name, factory in engines:
            with TemporaryDirectory(dir=root) as attempt:
                target = Path(attempt) / f'{name}.ms'
                make_input(dataset, target, scratch=True)
                tool = configure(factory, target, cache, machine, correction, True, switches)
                assert tool.ft(str(model)) and tool.done()
                actual = data(target, 'MODEL_DATA')
                if switches:
                    assert actual.tobytes() == baseline[name].tobytes()
                else:
                    baseline[name] = actual
                assert cache_contents(cache) == previous
        error = relative_rms(baseline['native'], baseline['casa'])
        assert error < 1.e-3
        errors.append(error)
    return errors, baseline


def refuse(root, dataset, model, cache, machine, correction, engines):
    point = root / 'point.cl'
    component(point, coordinates(dataset), 2.)
    for name, factory in engines:
        for label in ('empty-request', 'component', 'missing-image', 'existing-image', 'virtual'):
            with TemporaryDirectory(dir=root) as attempt:
                case = Path(attempt)
                target = case / f'{name}-{label}.ms'
                missing = case / 'empty.im'
                scratch = label != 'virtual'
                make_input(dataset, target, scratch=scratch)
                ft(factory, target, model, cache, scratch=scratch, machine='ft', spw='0:0')
                before = data(target, 'MODEL_DATA') if scratch else recipe(target)
                kwargs = {
                    'empty-request': {},
                    'component': {'complist': str(point)},
                    'missing-image': {'model': str(missing)},
                    'existing-image': {'model': str(model)},
                    'virtual': {'model': str(model)},
                }[label]
                before_cache = cache_contents(cache)
                tool = configure(factory, target, cache, machine, correction, scratch, DISABLED)
                assert not tool.ft(**kwargs) and tool.done()
                after = data(target, 'MODEL_DATA') if scratch else recipe(target)
                if scratch:
                    assert before.tobytes() == after.tobytes()
                else:
                    np.testing.assert_equal(before, after)
                assert cache_contents(cache) == before_cache
                assert missing.exists() == (label == 'missing-image')
                if missing.exists():
                    reader = image()
                    assert reader.open(str(missing))
                    pixels = reader.getchunk()
                    assert pixels.shape == (256, 256, 4, 1)
                    assert not np.any(pixels) and reader.done()


def recover(root, dataset, model, cache, machine, correction, engines, baseline):
    empty_selections = ({'taql': 'TIME < 0.0'}, {'time': '1900/01/01~1900/01/02'})
    before_cache = cache_contents(cache)
    point_baseline = {}
    for name, factory in engines:
        for selection in empty_selections:
            with TemporaryDirectory(dir=root) as attempt:
                target = Path(attempt) / f'{name}.ms'
                make_input(dataset, target, scratch=True)
                tool = configure(factory, target, cache, machine, correction, True, DISABLED)
                before = data(target, 'MODEL_DATA').tobytes()
                assert not tool.selectvis(field=0, spw='0:0', **selection)
                assert not tool.ft(str(model))
                tool.done()
                assert data(target, 'MODEL_DATA').tobytes() == before

        point = {'complist': str(root / 'point.cl')}
        for label in ('empty-first', 'image-replay', 'image-duplicate',
                      'component-direct', 'component-replay', 'component-duplicate'):
            with TemporaryDirectory(dir=root) as attempt:
                target = Path(attempt) / f'{name}.ms'
                make_input(dataset, target, scratch=True)
                component_only = label.startswith('component-')
                model_arg = point if component_only else {'model': str(model)}
                enabled = {'psterm': False}
                direct = label == 'component-direct'
                tool = configure(factory, target, cache, machine, correction, True,
                                 enabled if direct else DISABLED)
                before = data(target, 'MODEL_DATA').tobytes()
                if not direct:
                    first = {} if label == 'empty-first' else model_arg
                    assert not tool.ft(**first)
                    assert data(target, 'MODEL_DATA').tobytes() == before
                    set_options(tool, cache, machine, correction, enabled)
                second = {} if label.endswith('-replay') else model_arg
                success = tool.ft(**second)
                assert success == (not label.endswith('-duplicate')) and tool.done()
                after = data(target, 'MODEL_DATA')
                if direct:
                    assert after.tobytes() != before
                    point_baseline[name] = after
                else:
                    expected = point_baseline[name] if component_only else baseline[name]
                    assert after.tobytes() == (expected.tobytes() if success else before)
    assert cache_contents(cache) == before_cache
    error = relative_rms(point_baseline['native'], point_baseline['casa'])
    assert error < 1.e-3
    return error


def ordinary_grid(root, dataset, model, engines):
    with TemporaryDirectory(dir=root) as attempt:
        case = Path(attempt)
        result = {}
        for name, factory in engines:
            target = case / f'{name}.ms'
            make_input(dataset, target, scratch=True)
            tool = configured_imager(factory, target, case / 'unused', scratch=True, machine='ft')
            assert tool.setoptions(ftmachine='ft', numthreads=1, **DISABLED)
            assert tool.ft(str(model)) and tool.done()
            result[name] = data(target, 'MODEL_DATA')
        error = relative_rms(result['native'], result['casa'])
        assert error < 1.e-3
        return error


def run(root, dataset, model, corrected, uncorrected):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    engines = (('casa', imager), ('native', native))
    for machine in ('awproject', 'mawproject'):
        for correction, source_cache in ((True, corrected), (False, uncorrected)):
            with TemporaryDirectory(dir=root) as attempt:
                case = Path(attempt)
                cache = case / 'cache'
                shutil.copytree(source_cache, cache)
                label = f'{machine}-{correction}'
                results[label], baseline = compare_switches(case, dataset, model, cache, machine,
                                                           correction, engines)
                refuse(case, dataset, model, cache, machine, correction, engines)
                results[label].append(recover(case, dataset, model, cache, machine, correction,
                                              engines, baseline))
                print(label, max(results[label]), 'all switches, refusals and recovery', flush=True)
    results['ordinary-grid'] = ordinary_grid(root, dataset, model, engines)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model', 'corrected-cache', 'uncorrected-cache'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(*(path.resolve() for path in (args.work, args.dataset, args.model, args.corrected_cache,
                                      args.uncorrected_cache)))
