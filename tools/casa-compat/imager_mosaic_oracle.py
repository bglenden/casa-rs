#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare live and persisted legacy MosaicFT models through public Python."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import image, imager, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_prediction_options_oracle import configure_data, coordinates, samples, write_model


def fixture(root):
    path = root / 'source.ms'
    configure_data(path)
    stored = table()
    assert stored.open(str(path / 'SPECTRAL_WINDOW'))
    frequency = stored.getcell('CHAN_FREQ', 0)[0]
    assert stored.close()
    ra, dec = coordinates(path)
    assert stored.open(str(path / 'FIELD'), nomodify=False)
    for column in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
        direction = stored.getcell(column, 0)
        direction[:, 0] += [4e-5, -3e-5]
        stored.putcell(column, 1, direction)
    assert stored.close()
    assert stored.open(str(path / 'ANTENNA'), nomodify=False)
    diameters = stored.getcol('DISH_DIAMETER')
    diameters[1::2] = 7.
    stored.putcol('DISH_DIAMETER', diameters)
    assert stored.close()
    assert stored.open(str(path), nomodify=False)
    fields = stored.getcol('FIELD_ID')
    first = stored.getcol('ANTENNA1')
    second = stored.getcol('ANTENNA2')
    for field in (0, 1):
        row = int(np.flatnonzero((fields == field) & (first != second))[2])
        stored.putcell('ANTENNA2', row, int(first[row]))
        stored.putcell('UVW', row, np.zeros(3))
    assert stored.close()
    offset_path = root / 'offset-pointings.ms'
    shutil.copytree(path, offset_path)
    offset_pointings(offset_path)
    assert stored.open(str(path / 'POINTING'), nomodify=False)
    if stored.nrows():
        stored.removerows(list(range(stored.nrows())))
    assert stored.close()

    model = root / 'mosaic.im'
    pixels = np.zeros((320, 320, 1, 1), np.float32)
    pixels[160, 160, 0, 0] = 1.7
    pixels[191, 144, 0, 0] = .9
    tool = image()
    assert tool.fromarray(outfile=str(model), pixels=pixels)
    coords = tool.coordsys()
    assert coords.setunits(['rad', 'rad', '', 'Hz'])
    assert coords.setreferencevalue(value=f'{ra}rad {dec}rad', type='direction')
    assert coords.setincrement(value='-0.3arcsec 0.3arcsec', type='direction')
    assert coords.setreferencepixel(value=[160., 160.], type='direction')
    assert coords.setreferencecode('TOPO', type='spectral', adjust=False)
    assert coords.setreferencevalue(value=f'{frequency}Hz', type='spectral')
    assert coords.setincrement(value='30MHz', type='spectral')
    assert tool.setcoordsys(coords.torecord()) and coords.done()
    assert tool.setbrightnessunit('Jy/pixel') and tool.done()
    return path, offset_path, model


def offset_pointings(path):
    stored = table()
    assert stored.open(str(path))
    times = stored.getcol('TIME')
    fields = stored.getcol('FIELD_ID')
    field_times = [np.unique(times[fields == field]) for field in (0, 1)]
    assert stored.close()
    assert stored.open(str(path / 'FIELD'))
    directions = [stored.getcell('PHASE_DIR', field) for field in (0, 1)]
    assert stored.close()
    assert stored.open(str(path / 'POINTING'), nomodify=False)
    assert stored.nrows() > 0
    for row, epoch in enumerate(stored.getcol('TIME')):
        field = min((0, 1), key=lambda field: np.min(np.abs(field_times[field] - epoch)))
        direction = directions[field].copy()
        direction[:, 0] += [2.25e-5, -3.1e-5]
        stored.putcell('DIRECTION', row, direction)
    assert stored.close()


def compare(actual, expected):
    assert len(actual) == len(expected)
    errors = [relative_rms(value, reference) for value, reference in zip(actual, expected, strict=True)]
    assert max(errors) < 1e-3, errors
    return {'complex_samples': sum(value.size for value in actual), 'normalized_rms': max(errors)}


def predict(source, target, factory, model, scratch, beam, *, size=320, cell='0.3arcsec'):
    shutil.copytree(source, target)
    if not scratch:
        stored = table()
        assert stored.open(str(target), nomodify=False)
        if 'MODEL_DATA' in stored.colnames():
            stored.removecols(['MODEL_DATA'])
        assert stored.close()
    tool = factory()
    assert tool.open(str(target), usescratch=scratch)
    assert tool.selectvis(field='0,1')
    assert tool.defineimage(nx=size, ny=size, cellx=cell, celly=cell,
                            phasecenter=0, outframe='TOPO')
    assert tool.setoptions(ftmachine='mosaic', numthreads=1)
    if beam:
        assert tool.setvp(dovp=True)
    assert tool.ft(str(model)) and tool.done(), (target, scratch, beam)
    if not scratch:
        stored = table()
        assert stored.open(str(target / 'SOURCE'))
        row = stored.getkeyword(stored.getkeyword('definedmodel_field_0'))
        record = stored.getcell('SOURCE_MODEL', row)['ft_0']['container']
        assert record['name'] == 'MosaicFT' and record['pbconvfunc']['name'] == 'HetArrayConvFunc'
        assert stored.close()


def consume(source, destination, reader):
    from casars.tools import ms

    shutil.copytree(source, destination)
    before = [samples(destination, 'CORRECTED_DATA', field) for field in (0, 1)]
    if reader == 'casa':
        assert casatasks.uvsub(str(destination)) is None
    else:
        tool = ms()
        assert tool.open(str(destination), nomodify=False) and tool.uvsub() and tool.done()
    return [value - samples(destination, 'CORRECTED_DATA', field) for field, value in enumerate(before)]


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, offset_path, model = fixture(root)
    baseline, control = predictions(root, source, model, (False, True), native)
    offset = root / 'offset'
    offset.mkdir()
    shifted, observed = predictions(offset, offset_path, model, (False,), native)
    changes = [relative_rms(after, before) for after, before in zip(observed, control, strict=True)]
    assert min(changes) > 0.01, changes
    return {'standard': baseline, 'tracked_pointing': shifted,
            'pointing_offset_relative_rms': changes,
            'small_crop': small_crop(root, source, native)}


def small_crop(root, source, native):
    stored = table()
    assert stored.open(str(source / 'SPECTRAL_WINDOW'))
    frequency = stored.getcell('CHAN_FREQ', 0)[0]
    assert stored.close()
    model = root / 'small-crop.im'
    write_model(model, coordinates(source), frequency, 1.7, cell=2.)
    evidence = {}
    for engine, factory in (('casa', imager), ('native', native)):
        scratch = root / f'small-crop-{engine}.ms'
        predict(source, scratch, factory, model, True, False, size=32, cell='2arcsec')
        values = [samples(scratch, field=field) for field in (0, 1)]
        assert all(np.count_nonzero(value) == 0 for value in values), engine
        evidence[engine] = sum(value.size for value in values)
    return evidence


def predictions(root, source, model, beams, native):
    results = {}
    reference = None
    for beam in beams:
        for engine, factory in (('casa', imager), ('native', native)):
            path = root / f'scratch-{engine}-{beam}.ms'
            predict(source, path, factory, model, True, beam)
            values = [samples(path, field=field) for field in (0, 1)]
            stored = table()
            assert stored.open(str(path))
            autos = np.flatnonzero(stored.getcol('ANTENNA1') == stored.getcol('ANTENNA2'))
            assert autos.size >= 2
            assert all(np.max(np.abs(stored.getcell('MODEL_DATA', int(row)))) > 1. for row in autos)
            assert stored.close()
            if reference is None:
                assert all(np.max(np.abs(field)) > 1. for field in values)
                reference = values
            results[f'scratch-{engine}-{beam}'] = compare(values, reference)
    for writer, factory in (('casa', imager), ('native', native)):
        path = root / f'virtual-{writer}.ms'
        predict(source, path, factory, model, False, False)
        for reader in ('casa', 'native'):
            values = consume(path, root / f'virtual-{writer}-{reader}.ms', reader)
            results[f'virtual-{writer}-{reader}'] = compare(values, reference)
    return results, reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    root = parser.parse_args().work.resolve()
    results = run(root)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)


if __name__ == '__main__':
    main()
