#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy imager moving-source scratch predictions and Fourier recipes."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import imager, measures, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_facets_oracle import model_grids, model_recipe
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


def predict(path, factory, model, moving, machine, location, scratch):
    tool = factory()
    assert tool.open(str(path), usescratch=scratch) and tool.selectvis(field='0,1')
    try:
        assert tool.defineimage(nx=320, ny=320, cellx='0.3arcsec', celly='0.3arcsec',
                                phasecenter=0, outframe='TOPO', movingsource=moving)
        assert tool.setoptions(ftmachine=machine, numthreads=1, wprojplanes=16, location=location)
        return tool.ft(str(model))
    finally:
        assert tool.done()


def observed_source(path):
    grid, = model_grids(model_recipe(path))
    assert grid['fixmovingsource']
    source = grid['movingdir_rec']
    anchor = grid['firstmovingdir_rec']
    assert anchor['refer'] == 'J2000'
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


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, _, model = fixture(root)
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
