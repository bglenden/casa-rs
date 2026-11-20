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


def configured_ft(path, factory, model, facets, machine, padding, scratch, *, incremental=False):
    tool = factory()
    assert tool.open(str(path), usescratch=scratch) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=320, ny=320, cellx='0.3arcsec', celly='0.3arcsec',
                            phasecenter=1, outframe='TOPO', facets=facets)
    assert tool.setoptions(ftmachine=machine, padding=padding, wprojplanes=16, numthreads=1)
    assert tool.ft(str(model), incremental=incremental), (path, facets, machine, scratch)
    if scratch and facets == 2 and machine == 'ft' and not incremental:
        before = field_samples(path)
        assert tool.ft(str(model), incremental=True) is False
        np.testing.assert_array_equal(field_samples(path), before)
    assert tool.done()


def model_records(path, facets, machine):
    stored = table()
    assert stored.open(str(path / 'SOURCE'))
    key = stored.getkeyword('definedmodel_field_0')
    assert key == stored.getkeyword('definedmodel_field_1')
    model = stored.getcell('SOURCE_MODEL', stored.getkeyword(key))
    assert stored.close()
    grids = [entry['container'] for key, entry in model.items() if key.startswith('ft_')]
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


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, _, model = fixture(root)
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
