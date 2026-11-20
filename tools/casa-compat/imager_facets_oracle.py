#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public legacy faceted scratch and virtual model prediction."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import image, imager, table
import numpy as np

from imager_mosaic_oracle import compare, consume, field_samples, fixture


def configured_ft(path, factory, model, facets, machine, padding, scratch, *,
                  incremental=False, taylor=False, taql=''):
    tool = factory()
    assert tool.open(str(path), usescratch=scratch) and tool.selectvis(field='0,1', taql=taql)
    assert tool.defineimage(nx=320, ny=320, cellx='0.3arcsec', celly='0.3arcsec',
                            phasecenter=1, outframe='TOPO', facets=facets)
    if taylor:
        assert tool.settaylorterms(2, 95e9)
    assert tool.setoptions(ftmachine=machine, padding=padding, wprojplanes=16, numthreads=1)
    models = str(model) if isinstance(model, Path) else [str(path) for path in model]
    assert tool.ft(models, incremental=incremental), (path, facets, machine, scratch)
    if scratch and facets == 2 and machine == 'ft' and not incremental and not taylor:
        before = field_samples(path)
        assert tool.ft(models, incremental=True) is False
        np.testing.assert_array_equal(field_samples(path), before)
    assert tool.done()


def model_recipe(path):
    stored = table()
    assert stored.open(str(path / 'SOURCE'))
    key = stored.getkeyword('definedmodel_field_0')
    assert key == stored.getkeyword('definedmodel_field_1')
    model = stored.getcell('SOURCE_MODEL', stored.getkeyword(key))
    assert stored.close()
    return model


def model_grids(recipe):
    return [entry['container'] for key, entry in recipe.items() if key.startswith('ft_')]


def model_records(path, facets, machine):
    grids = model_grids(model_recipe(path))
    assert len(grids) == facets * facets
    machine_name = 'WProjectFT' if machine == 'wproject' else 'GridFT'
    shapes, tangent, centres = [], [], []
    for grid in grids:
        assert grid['name'] == machine_name and grid['tangentspecified']
        if machine == 'ft':
            assert grid['padding'] == 1.0
        shapes.append((grid['nx'], grid['ny']))
        tangent.append([grid['mtangent_rec'][axis]['value'] for axis in ('m0', 'm1')])
        centres.append([grid['mimage_rec'][axis]['value'] for axis in ('m0', 'm1')])
    return shapes, np.asarray(tangent), np.asarray(centres)


def alter_fixture(source, model):
    stored = table()
    assert stored.open(str(source), nomodify=False)
    stored.putcol('UVW', 100. * stored.getcol('UVW'))
    assert stored.close()
    tool = image()
    assert tool.open(str(model))
    pixels = tool.getchunk()
    pixels[319, 319, 0, 0] = .75
    assert tool.putchunk(pixels) and tool.done()


def incremental_case(root, model, native):
    predictions = []
    for engine, factory in (('casa', imager), ('native', native)):
        path = root / f'2-ft-incremental-{engine}.ms'
        shutil.copytree(root / f'2-ft-scratch-{engine}.ms', path)
        before = field_samples(path)
        configured_ft(path, factory, model, 2, 'ft', 1.2, True, incremental=True)
        values = field_samples(path)
        compare(values, [2. * field for field in before])
        predictions.append(values)
    expected, actual = predictions
    return compare(actual, expected)


def copy_model(reference, destination, points):
    shutil.copytree(reference, destination)
    tool = image()
    assert tool.open(str(destination))
    pixels = np.zeros(tool.shape(), np.float32)
    for x, y, flux in points:
        pixels[x, y, 0, 0] = flux
    assert tool.putchunk(pixels) and tool.done()
    return destination


def blank_replacement(root, blank, native):
    evidence = {}
    for engine, factory in (('casa', imager), ('native', native)):
        virtual = root / f'2-ft-virtual-{engine}.ms'
        original = model_recipe(virtual)
        before = consume(virtual, root / f'blank-before-{engine}.ms', engine)
        configured_ft(virtual, factory, blank, 2, 'ft', 1.2, False)
        np.testing.assert_equal(model_recipe(virtual), original)
        after = consume(virtual, root / f'blank-after-{engine}.ms', engine)
        np.testing.assert_array_equal(after, before)
        scratch = root / f'2-ft-scratch-{engine}.ms'
        before = field_samples(scratch)
        configured_ft(scratch, factory, blank, 2, 'ft', 1.2, True, incremental=True)
        np.testing.assert_array_equal(field_samples(scratch), before)
        configured_ft(scratch, factory, blank, 2, 'ft', 1.2, True)
        cleared = field_samples(scratch)
        assert all(np.count_nonzero(field) == 0 for field in cleared)
        evidence[engine] = sum(field.size for field in cleared)
    return evidence


def additional_case(root, source, label, models, native, machine='ft', *, taylor=False):
    if machine == 'mosaic':
        evidence = mosaic_scratch(root, source, models[0], native)
    else:
        scratch = {}
        for engine, factory in (('casa', imager), ('native', native)):
            path = root / f'{label}-scratch-{engine}.ms'
            shutil.copytree(source, path)
            configured_ft(path, factory, models, 2, machine, 1.2, True, taylor=taylor)
            scratch[engine] = field_samples(path)
        reference = scratch['casa']
        assert all(np.max(np.abs(field)) > .8 for field in reference), label
        evidence = {f'{label}-scratch': compare(scratch['native'], reference)}
    for writer, factory in (('casa', imager), ('native', native)):
        path = root / f'{label}-virtual-{writer}.ms'
        shutil.copytree(source, path)
        stored = table()
        assert stored.open(str(path), nomodify=False) and stored.removecols('MODEL_DATA')
        assert stored.close()
        configured_ft(path, factory, models, 2, machine, 1.2, False, taylor=taylor)
        grids = model_grids(model_recipe(path))
        if taylor:
            assert len(grids) == 4 + len(models) - 1
            assert all(grid['name'] == 'MultiTermFT' and grid['nterms'] == 2
                       and grid['thisterm'] == 0 and grid['reffreq'] == 95e9 for grid in grids)
        else:
            assert [grid['name'] for grid in grids] == ['MosaicFT'] + ['GridFT'] * 3
            assert all(not grid['tangentspecified'] and (grid['nx'], grid['ny']) == (160, 160)
                       for grid in grids)
            assert all(grid['padding'] == 1. and not grid['usedoublegrid']
                       and grid['cachesize'] == 1_000_000 for grid in grids[1:])
        for reader in ('casa', 'native'):
            actual = consume(path, root / f'{label}-virtual-{writer}-{reader}.ms', reader)
            if machine == 'mosaic' and writer == reader == 'casa':
                reference = actual
                assert all(np.max(np.abs(field)) > 1. for field in reference)
            evidence[f'{label}-virtual-{writer}-{reader}'] = compare(actual, reference)
    return evidence


def mosaic_scratch(root, source, model, native):
    native_ms = root / 'mixed-mosaic-scratch-native.ms'
    shutil.copytree(source, native_ms)
    configured_ft(native_ms, native, model, 2, 'mosaic', 1.2, True)
    stored = table()
    assert stored.open(str(native_ms))
    times = stored.getcol('TIME')
    fields = stored.getcol('FIELD_ID')
    native_model = stored.getcol('MODEL_DATA')
    assert stored.close()
    expected, actual = [], []
    # The legacy bulk CASA call drops the first facet after the first
    # integration. Start each pinned reference with a fresh VisBuffer.
    for index, time in enumerate(np.unique(times)):
        target = root / f'mixed-mosaic-casa-buffer-{index}.ms'
        shutil.copytree(source, target)
        keep = times == time
        assert set(fields[keep]) in ({0}, {1})
        configured_ft(target, imager, model, 2, 'mosaic', 1.2, True,
                      taql=f'abs(TIME - {time:.9f}) < 0.001')
        assert stored.open(str(target))
        expected.append(stored.getcol('MODEL_DATA')[..., keep])
        assert stored.close()
        actual.append(native_model[..., keep])
    assert all(np.max(np.abs(field)) > 1. for field in expected)
    return {'mixed-mosaic-scratch': compare(actual, expected),
            'mixed-mosaic-outside-pointing': mosaic_outside_pointing(
                root, source, model, native, float(times.min()))}


def mosaic_outside_pointing(root, source, model, native, time):
    predictions = []
    for engine, factory in (('casa', imager), ('native', native)):
        target = root / f'mixed-mosaic-outside-{engine}.ms'
        shutil.copytree(source, target)
        stored = table()
        assert stored.open(str(target / 'POINTING'), nomodify=False)
        directions = stored.getcol('DIRECTION')
        directions[1, ...] -= 1e-3
        assert stored.putcol('DIRECTION', directions) and stored.close()
        configured_ft(target, factory, model, 2, 'mosaic', 1.2, True,
                      taql=f'abs(TIME - {time:.9f}) < 0.001')
        assert stored.open(str(target))
        keep = stored.getcol('TIME') == time
        values = stored.getcol('MODEL_DATA')[..., keep]
        assert stored.close()
        # The first facet is outside the pointing window. The second
        # facet's ordinary GridFT still predicts the 0.9 Jy source.
        assert .89 < np.max(np.abs(values)) < .91
        predictions.append(values)
    return compare([predictions[1]], [predictions[0]])


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, mosaic_source, model = fixture(root)
    alter_fixture(source, model)
    evidence = {}
    for facets, machine, padding in ((2, 'ft', 1.2), (2, 'wproject', 1.2), (3, 'ft', 1.0)):
        label = f'{facets}-{machine}'
        predictions = {}
        records = {}
        for (engine, factory) in (('casa', imager), ('native', native)):
            path = root / f'{label}-scratch-{engine}.ms'
            shutil.copytree(source, path)
            configured_ft(path, factory, model, facets, machine, padding, True)
            predictions[engine] = field_samples(path)
            assert all(np.max(np.abs(field)) > 1. for field in predictions[engine])
        reference = predictions['casa']
        evidence[f'{label}-scratch'] = compare(predictions['native'], reference)
        for engine, factory in (('casa', imager), ('native', native)):
            path = root / f'{label}-virtual-{engine}.ms'
            shutil.copytree(source, path)
            stored = table()
            assert stored.open(str(path), nomodify=False)
            stored.removecols('MODEL_DATA')
            assert stored.close()
            configured_ft(path, factory, model, facets, machine, padding, False)
            records[engine] = model_records(path, facets, machine)
            for reader in ('casa', 'native'):
                actual = consume(path, root / f'{label}-virtual-{engine}-{reader}.ms', reader)
                evidence[f'{label}-virtual-{engine}-{reader}'] = compare(actual, reference)
        expected, actual = records['casa'], records['native']
        assert actual[0] == expected[0], (label, actual[0], expected[0])
        np.testing.assert_allclose(actual[1], expected[1], rtol=0., atol=2e-12)
        delta = (actual[2] - expected[2] + np.pi) % (2. * np.pi) - np.pi
        np.testing.assert_allclose(delta, 0., rtol=0., atol=2e-12)
    evidence['2-ft-fresh-incremental'] = incremental_case(root, model, native)
    blank = copy_model(model, root / 'blank.im', [])
    outlier = copy_model(model, root / 'outlier.im', [(191, 144, .9)])
    mosaic = copy_model(model, root / 'faceted-mosaic.im', [(80, 80, 1.7), (240, 80, .9)])
    evidence['blank-replacement'] = blank_replacement(root, blank, native)
    evidence.update(additional_case(root, source, 'single-image-taylor', [model], native, taylor=True))
    evidence.update(additional_case(root, source, 'outlier-taylor', [blank, outlier], native, taylor=True))
    evidence.update(additional_case(root, mosaic_source, 'mixed-mosaic', [mosaic], native, 'mosaic'))
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
