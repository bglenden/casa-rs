#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare retained image and component predictions on one public imager."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import image, imager, ms as casa_ms, table
import numpy as np

from calibration_ft_oracle import component, coordinates
from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import configured_imager, corrected_or_data, data, make_input


def compare(actual, expected, name, results):
    error = relative_rms(actual, expected)
    assert error < 1.e-3, (name, error)
    results[name] = error
    print(name, 'NRMS', error, flush=True)


def publish(factory, target, options, *, scratch, incremental=False):
    tool = configured_imager(factory, target, target.parent / 'unused-cache',
                             scratch=scratch, machine='ft')
    try:
        for number, arguments in enumerate(options):
            assert tool.ft(**arguments, incremental=incremental and number > 0)
        if scratch:
            return data(target, 'MODEL_DATA')
    finally:
        assert tool.done()


def virtual(factory, path, target):
    shutil.copytree(path, target)
    before = corrected_or_data(target)
    tool = factory()
    assert tool.open(str(target), nomodify=False)
    assert tool.uvsub() and tool.done()
    return before - data(target, 'CORRECTED_DATA')


def scratch_controls(root, dataset, factory, options, expected):
    target = root / 'scratch-controls.ms'
    make_input(dataset, target, scratch=True)
    tool = configured_imager(factory, target, root / 'unused-cache', scratch=True, machine='ft')
    assert tool.ft(**options['image'])
    assert tool.ft(**options['components'])
    before = data(target, 'MODEL_DATA')
    np.testing.assert_array_equal(before, expected)
    writer = table()
    assert writer.open(str(target), nomodify=False)
    assert writer.putcol('MODEL_DATA', np.full_like(before, 17. + 2.j)) and writer.done()
    assert tool.ft()
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
    assert tool.selectvis(field=0)
    assert tool.ft(**options['image'])
    reset = data(target, 'MODEL_DATA')
    assert relative_rms(reset, before) > .01
    assert not tool.ft(**options['image'])
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), reset)
    assert tool.done()

    target = root / 'component-repeat.ms'
    make_input(dataset, target, scratch=True)
    tool = configured_imager(factory, target, root / 'unused-cache', scratch=True, machine='ft')
    assert tool.ft(**options['components'])
    first = data(target, 'MODEL_DATA')
    assert not tool.ft(**options['components'])
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), first)
    assert tool.done()


def moved_source(root, dataset, factory, first, second, model, expected):
    root.mkdir()
    local_model = root / 'model.im'
    component_path = root / 'point.cl'
    shutil.copytree(model, local_model)
    component(component_path, coordinates(dataset), 2.)
    paths = {'image': local_model, 'components': component_path}
    keywords = {'image': 'model', 'components': 'complist'}
    target = root / 'source-moved.ms'
    make_input(dataset, target, scratch=True)
    tool = configured_imager(factory, target, root / 'unused-cache', scratch=True, machine='ft')
    assert tool.ft(**{keywords[first]: str(paths[first])})
    paths[first].rename(root / 'moved-source')
    assert tool.ft(**{keywords[second]: str(paths[second])})
    assert not paths[first].exists()
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), expected)
    assert tool.done()


def retry_image(root, dataset, factory, options, model, expected):
    root.mkdir()
    target = root / 'retry.ms'
    rejected = root / 'candidate.im'
    make_input(dataset, target, scratch=True)
    maker = image()
    assert maker.fromshape(str(rejected), [32, 32]) and maker.done()
    tool = configured_imager(factory, target, root / 'unused-cache', scratch=True, machine='ft')
    assert tool.ft(**options['components'])
    before = data(target, 'MODEL_DATA')
    assert not tool.ft(str(rejected))
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
    rejected.rename(root / 'rejected.im')
    shutil.copytree(model, rejected)
    assert tool.ft(str(rejected)) and tool.done()
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), expected)


def run(root, dataset, model):
    from casars.tools import imager as native, ms as native_ms

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    options = {'image': {'model': str(model)}}
    point = root / 'point.cl'
    component(point, coordinates(dataset), 2.)
    options['components'] = {'complist': str(point)}
    results = {}
    engines = (('casa', imager), ('native', native))
    for first, second in (('components', 'image'), ('image', 'components')):
        for incremental in (False, True):
            with TemporaryDirectory(dir=root) as attempt:
                case = Path(attempt)
                scratch = {}
                for engine, factory in engines:
                    path = case / f'{engine}-scratch.ms'
                    make_input(dataset, path, scratch=True)
                    scratch[engine] = publish(factory, path, [options[first], options[second]],
                                              scratch=True, incremental=incremental)
                label = f'{first}-{second}' + ('-incremental' if incremental else '')
                compare(scratch['native'], scratch['casa'], f'{label}-scratch', results)
                if incremental:
                    continue
                for engine, factory in engines:
                    moved_source(case / f'{engine}-moved', dataset, factory, first, second,
                                 model, scratch[engine])
                virtual_products = {}
                for writer, factory in engines:
                    path = case / f'{writer}-saved.ms'
                    make_input(dataset, path, scratch=False)
                    seed = imager()
                    assert seed.open(str(path), usescratch=True) and seed.done()
                    editor = table()
                    assert editor.open(str(path), nomodify=False)
                    assert editor.removecols('MODEL_DATA') and editor.done()
                    publish(factory, path, [options[first], options[second]], scratch=False)
                    for reader, source in (('casa', casa_ms), ('native', native_ms)):
                        virtual_products[writer, reader] = virtual(
                            source, path, case / f'{writer}-{reader}-subtracted.ms')
                for (writer, reader), prediction in virtual_products.items():
                    compare(prediction, virtual_products['casa', 'casa'],
                            f'{label}-{writer}-{reader}-saved', results)
                if first == 'image':
                    for engine, factory in engines:
                        controls = case / engine
                        controls.mkdir()
                        scratch_controls(controls, dataset, factory, options, scratch[engine])
                else:
                    retry_image(case / 'retry', dataset, native, options, model, scratch['native'])
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(*(path.resolve() for path in (args.work, args.dataset, args.model)))
