#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy imager moving-source scratch predictions and Fourier recipes."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import image, imager, measures, quanta, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_facets_oracle import copy_model, model_grids, model_recipe
from imager_mosaic_oracle import compare, consume, field_samples, fixture


def change_epochs(path, baseline_scale):
    stored = table()
    assert stored.open(str(path), nomodify=False)
    fields = stored.getcol('FIELD_ID')
    assert stored.putcol('UVW', baseline_scale * stored.getcol('UVW'))
    for column in ('TIME', 'TIME_CENTROID'):
        epochs = stored.getcol(column)
        # The legacy iterator visits field zero first even though it is later.
        epochs[fields == 0] += 6. * 3600.
        assert stored.putcol(column, epochs)
    assert stored.close()


def setup_ms(source, path, scratch):
    shutil.copytree(source, path)
    if not scratch:
        stored = table()
        assert stored.open(str(path), nomodify=False) and stored.removecols('MODEL_DATA')
        assert stored.close()
    return path


def define(tool, moving):
    assert tool.defineimage(nx=320, ny=320, cellx='0.3arcsec', celly='0.3arcsec',
                            phasecenter=0, outframe='TOPO', movingsource=moving)


def predict(path, factory, model, moving, machine, location, scratch, *, components=''):
    tool = factory()
    assert tool.open(str(path), usescratch=scratch) and tool.selectvis(field='0,1')
    try:
        define(tool, moving)
        assert tool.setoptions(ftmachine=machine, numthreads=1, wprojplanes=16, location=location)
        images = [] if model is None else str(model)
        return tool.ft(images, complist=str(components))
    finally:
        assert tool.done()


def observed_source(path):
    grid, = model_grids(model_recipe(path))
    assert grid['fixmovingsource']
    source = grid['movingdir_rec']
    anchor = grid['firstmovingdir_rec']
    assert anchor['refer'] == grid['mimage_rec']['refer']
    return source['refer'], [anchor[axis]['value'] for axis in ('m0', 'm1')]


def paired(root, source, model, label, moving, machine, location, factories, virtual_case):
    scratch = []
    for writer, factory in factories:
        path = setup_ms(source, root / f'{label}-scratch-{writer}.ms', True)
        assert predict(path, factory, model, moving, machine, location, True)
        scratch.append(field_samples(path))
    assert all(np.max(np.abs(field)) > 1. for field in scratch[0])
    evidence = {'scratch': compare(scratch[1], scratch[0])}
    if not virtual_case:
        return evidence, scratch

    reference = None
    anchors = []
    for writer, factory in factories:
        path = setup_ms(source, root / f'{label}-virtual-{writer}.ms', False)
        assert predict(path, factory, model, moving, machine, location, False)
        anchors.append(observed_source(path))
        for reader, _ in factories:
            values = consume(path, root / f'{label}-virtual-{writer}-{reader}.ms', reader)
            if reference is None:
                reference = values
            evidence[f'virtual-{writer}-{reader}'] = compare(values, reference)
    assert anchors[1][0] == anchors[0][0]
    np.testing.assert_allclose(anchors[1][1], anchors[0][1], atol=5e-11, rtol=0.)
    return evidence, scratch


def failed_replacement(root, source, model, factories):
    for engine, factory in factories:
        path = setup_ms(source, root / f'bad-moving-source-{engine}.ms', True)
        before = field_samples(path)
        try:
            predict(path, factory, model, 'THIS_SOURCE_DOES_NOT_EXIST', 'ft', [], True)
        except RuntimeError as error:
            assert 'Could not interprete phasecenter parameter' in str(error)
        else:
            raise AssertionError('unknown direction did not raise')
        assert predict(path, factory, model, 'COMET', 'ft', [], True) is False
        np.testing.assert_array_equal(field_samples(path), before)

        path = root / f'mars-virtual-{engine}.ms'
        before = model_recipe(path)
        assert predict(path, factory, model, 'COMET', 'ft', [], False) is False
        np.testing.assert_equal(model_recipe(path), before)


def multi_ms_tracking(root, source, model, factories):
    evidence = {}
    for scratch in (True, False):
        predictions, anchors = {}, {}
        for writer, factory in factories:
            paths = [setup_ms(source, root / f'joint-{scratch}-{writer}-{index}.ms', scratch)
                     for index in range(2)]
            stored = table()
            assert stored.open(str(paths[1]), nomodify=False)
            for name in ('TIME', 'TIME_CENTROID'):
                assert stored.putcol(name, stored.getcol(name) + 10. * 3600.)
            assert stored.close()
            tool = factory()
            for path in paths:
                assert tool.selectvis(str(path), field='0,1', usescratch=scratch)
            define(tool, 'MOON')
            assert tool.setoptions(ftmachine='ft', numthreads=1)
            assert tool.ft(str(model)) and tool.done()
            if scratch:
                predictions[writer] = [field_samples(path) for path in paths]
            else:
                anchors[writer] = [observed_source(path) for path in paths]
                np.testing.assert_array_equal(anchors[writer][0][1], anchors[writer][1][1])
                for reader, _ in factories:
                    predictions[writer, reader] = consume(
                        paths[1], root / f'joint-reopened-{writer}-{reader}.ms', reader)
        if scratch:
            for index in range(2):
                evidence[f'scratch-{index}'] = compare(
                    predictions['native'][index], predictions['casa'][index])
        else:
            np.testing.assert_allclose(anchors['native'][0][1], anchors['casa'][0][1],
                                       atol=5e-11, rtol=0.)
            reference = predictions['casa', 'casa']
            for (writer, reader), values in predictions.items():
                evidence[f'virtual-{writer}-{reader}'] = compare(values, reference)
    return evidence


def reset_location(root, source, model, factories):
    predictions, anchors = [], []
    for writer, factory in factories:
        path = setup_ms(source, root / f'location-reset-{writer}.ms', False)
        tool = factory()
        assert tool.open(str(path), usescratch=False) and tool.selectvis(field='0,1')
        define(tool, [])
        assert tool.setoptions(ftmachine='ft', numthreads=1)
        assert tool.ft(str(root / f'created-{writer}.im'))
        define(tool, 'MOON')
        assert tool.setoptions(ftmachine='ft', numthreads=1)
        assert tool.ft(str(model)) and tool.done()
        grid, = model_grids(model_recipe(path))
        assert grid['mlocation_rec']['m2']['value'] == 0.
        anchors.append(observed_source(path)[1])
        predictions.append(consume(path, root / f'location-reset-{writer}-read.ms', writer))
    np.testing.assert_allclose(anchors[1], anchors[0], atol=5e-11, rtol=0.)
    return {'virtual': compare(predictions[1], predictions[0])}


def empty_models(root, source, model, factories):
    blank = copy_model(model, root / 'untracked-blank.im', [])
    components = root / 'source.ms.cl'
    evidence = {}
    for writer, factory in factories:
        for scratch in (True, False):
            predictions = []
            for label, moving in (('stationary', []), ('comet', 'COMET')):
                path = setup_ms(source, root / f'components-{writer}-{scratch}-{label}.ms', scratch)
                assert predict(path, factory, None, moving, 'ft', [], scratch, components=components)
                values = (field_samples(path) if scratch else
                          consume(path, root / f'components-{writer}-{label}-read.ms', writer))
                predictions.append(values)
            assert all(np.max(np.abs(field)) > 1. for field in predictions[0])
            np.testing.assert_array_equal(predictions[1], predictions[0])
        path = setup_ms(source, root / f'blank-{writer}.ms', True)
        assert predict(path, factory, blank, 'COMET', 'ft', [], True)
        samples = field_samples(path)
        assert all(np.count_nonzero(field) == 0 for field in samples)
        evidence[writer] = sum(field.size for field in samples)
    return evidence

def local_coordinates(root, source, model, factories):
    stored = table()
    assert stored.open(str(source))
    epoch = stored.getcell('TIME', 0)
    assert stored.close()
    measure, quantity = measures(), quanta()
    assert measure.doframe(measure.observatory('VLA'))
    assert measure.doframe(measure.epoch('UTC', quantity.quantity(epoch, 's')))
    local = measure.measure(measure.direction('J2000', '0rad', '-30deg'), 'HADEC')
    lon, lat = (local[axis]['value'] for axis in ('m0', 'm1'))
    target = root / 'hadec.im'
    shutil.copytree(model, target)
    tool = image()
    assert tool.open(str(target))
    coords = tool.coordsys()
    assert coords.setreferencecode('HADEC', type='direction', adjust=False)
    assert coords.setreferencevalue(f'{lon}rad {lat}rad', type='direction')
    assert tool.setcoordsys(coords.torecord()) and coords.done() and tool.done()
    result, _ = paired(root, source, target, 'hadec', 'MARS', 'ft', 'VLA', factories, True)
    return result


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, antenna_pointings, model = fixture(root)
    nominal = root / 'nominal-baselines.ms'
    shutil.copytree(source, nominal)
    change_epochs(source, 100.)
    change_epochs(nominal, 1.)
    tool = measures()
    cases = [
        ('stationary', [], 'ft', [], source, False),
        ('mars', 'MARS', 'ft', [], source, True),
        ('lowercase', 'mars', 'ft', [], source, False),
        ('vector', ['MARS'], 'ft', [], source, False),
        ('record', tool.direction('MARS'), 'ft', [], source, False),
        ('moon', 'MOON', 'ft', [], source, True),
        ('literal', 'J2000 0rad -0.5rad', 'ft', [], source, True),
        ('apparent', tool.direction('APP', '0rad', '-0.5rad'), 'ft', [], source, True),
        ('planet-offset', tool.direction('MARS', '0.001deg', '0.002deg'), 'ft', [], source, True),
        ('moon-at-alma', 'MOON', 'ft', 'ALMA', nominal, True),
        ('wproject', 'MARS', 'wproject', [], source, True),
        ('mosaic', 'MARS', 'mosaic', [], nominal, True),
        ('mosaic-moon', 'MOON', 'mosaic', [], nominal, True),
    ]
    factories = [('casa', imager), ('native', native)]
    evidence, scratch = {}, {}
    for label, moving, machine, location, dataset, virtual_case in cases:
        evidence[label], scratch[label] = paired(
            root, dataset, model, label, moving, machine, location, factories, virtual_case)

    for index in range(len(factories)):
        baseline = scratch['stationary'][index]
        mars = scratch['mars'][index]
        moon = scratch['moon'][index]
        assert relative_rms(mars[1], baseline[1]) > 0.1
        assert relative_rms(moon[1], mars[1]) > 0.1
        for label in ('lowercase', 'vector', 'record'):
            np.testing.assert_array_equal(scratch[label][index], mars)
    failed_replacement(root, source, model, factories)
    evidence['joint-tracking'] = multi_ms_tracking(root, nominal, model, factories)
    evidence['location-reset'] = reset_location(root, nominal, model, factories)
    evidence['empty-models'] = empty_models(root, nominal, model, factories)
    evidence['hadec'] = local_coordinates(root, antenna_pointings, model, factories)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    root = parser.parse_args().work.resolve()
    evidence = run(root)
    (root / 'results.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps(evidence, indent=2), flush=True)


if __name__ == '__main__':
    main()
