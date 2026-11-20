#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy MosaicFT with a VPManager image table."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import image, imager, table, vpmanager
import numpy as np

from gaincal_oracle import relative_rms
from imager_facets_oracle import model_grids, model_recipe
from imager_mosaic_oracle import compare, consume, field_samples, fixture, predict


def voltage_table(root, model, *, split=False):
    ia = image()
    assert ia.open(str(model))
    coords = ia.coordsys()
    assert coords.setstokes(['XX', 'XY', 'YX', 'YY'])
    record = coords.torecord()
    assert coords.done() and ia.done()
    x, y = np.meshgrid(np.arange(320) - 160, np.arange(320) - 160, indexing='ij')
    amplitude = np.exp(-((x - 8) / 71)**2 - ((y + 3) / 58)**2)
    manager = vpmanager()
    assert manager.reset()
    for index, names in enumerate((['A0', 'A1'], ['A2', 'A3', 'A4'])):
        voltage = np.zeros((320, 320, 4, 1), dtype=np.complex64)
        voltage[:, :, 0, 0] = amplitude
        voltage[:, :, 3, 0] = amplitude * (.96 - .07 * index) * np.exp(.001j * index * x)
        beam = root / f'beam-{index}.im'
        pixels = voltage.real if split else voltage
        assert ia.fromarray(str(beam), pixels, csys=record) and ia.done()
        if split:
            imaginary = root / f'imaginary-{index}.im'
            assert ia.fromarray(str(imaginary), voltage.imag, csys=record) and ia.done()
            paths = {'realimage': str(beam), 'imagimage': str(imaginary)}
        else:
            paths = {'compleximage': str(beam)}
        assert manager.setpbimage(telescope='ALMA', antnames=names, **paths)
    path = root / 'beam.tab'
    assert manager.setuserdefault(vplistnum=0, telescope='ALMA')
    assert manager.saveastable(str(path)) and manager.reset()
    return path


def custom_predict(factory, source, target, model, pattern, scratch, dovp, *, succeeds=True):
    shutil.copytree(source, target)
    if not scratch:
        tables = table()
        assert tables.open(str(target), nomodify=False)
        assert tables.removecols('MODEL_DATA') and tables.done()
    tool = factory()
    assert tool.open(str(target), usescratch=scratch) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=320, ny=320, cellx='0.3arcsec', celly='0.3arcsec',
                            phasecenter=0, outframe='TOPO')
    assert tool.setoptions(ftmachine='mosaic', numthreads=1)
    assert tool.setvp(dovp=dovp, usedefaultvp=False, vptable=str(pattern))
    assert tool.ft(str(model)) is succeeds
    assert tool.done()
    if succeeds and not scratch:
        grid, = model_grids(model_recipe(target))
        beam = grid['pbconvfunc']
        assert grid['name'] == 'MosaicFT' and beam['name'] == 'HetArrayConvFunc'
        assert beam['pbclass'] == 8 and beam['vptable'] == str(pattern)


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, _, model = fixture(root)
    pattern = voltage_table(root, model)
    results = {}
    predict(source, root / 'default-scratch.ms', imager, model, True, False)
    defaults = field_samples(root / 'default-scratch.ms')
    for enabled in (True, False):
        paths = {}
        for name, factory in [('casa', imager), ('native', native)]:
            for scratch in (True, False):
                label = name, scratch
                paths[label] = root / f'{name}-{enabled}-{scratch}.ms'
                custom_predict(factory, source, paths[label], model, pattern, scratch, enabled)
        reference = field_samples(paths['casa', True])
        if enabled:
            assert all(relative_rms(a, b) > .1 for a, b in zip(defaults, reference, strict=True))
            enabled_reference = reference
        else:
            assert all(np.array_equal(a, b) for a, b in zip(enabled_reference, reference, strict=True))
        results[f'{enabled}-scratch'] = compare(field_samples(paths['native', True]), reference)
        reopened = consume(paths['casa', False], root / f'{enabled}-casa-casa.ms', 'casa')
        for writer, reader in [('casa', 'native'), ('native', 'casa'), ('native', 'native')]:
            label = f'{enabled}-{writer}-{reader}'
            actual = consume(paths[writer, False], root / f'{label}.ms', reader)
            results[label] = compare(actual, reopened)
    split = root / 'split-images'
    split.mkdir()
    # Legacy C++ checks for imagimage and reads the misspelled imagim field.
    split_table = voltage_table(split, model, split=True)
    for name, factory in [('casa', imager), ('native', native)]:
        custom_predict(factory, source, split / f'{name}.ms', model, split_table, True, True,
                       succeeds=False)
    tables = table()
    assert tables.open(str(split_table), nomodify=False)
    for row in range(tables.nrows()):
        record = tables.getcell('pbdescription', row)
        record['imagim'] = record['imagimage']
        assert tables.putcell('pbdescription', row, record)
    assert tables.done()
    for name, factory in [('casa', imager), ('native', native)]:
        path = split / f'{name}-with-imagim.ms'
        custom_predict(factory, source, path, model, split_table, True, True)
        results[f'split-images-{name}'] = compare(field_samples(path), enabled_reference)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    root = parser.parse_args().work.resolve()
    results = run(root)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)


if __name__ == '__main__':
    main()
