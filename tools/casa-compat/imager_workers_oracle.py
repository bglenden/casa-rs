#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public imager worker settings on a nonzero spectral model."""

import argparse
import json
from pathlib import Path
import threading

import casatasks
from casatools import image, imager, table
import numpy as np

from imager_custom_mosaic_oracle import copy_input
from imager_facets_oracle import model_grids, model_recipe
from imager_mosaic_oracle import compare, consume, field_samples
from imager_prediction_options_oracle import configure_data, coordinates


def create_cube(root, source):
    tables = table()
    assert tables.open(str(source / 'SPECTRAL_WINDOW'))
    frequencies = tables.getcell('CHAN_FREQ', 0)
    assert tables.done()
    pixels = np.zeros((256, 256, 1, len(frequencies)), dtype=np.float32)
    pixels[128, 128, 0, :] = np.linspace(1., 2., len(frequencies))
    pixels[151, 113, 0, :] = np.linspace(.6, .2, len(frequencies))
    ra, dec = coordinates(source)
    model = root / 'cube.im'
    tool = image()
    assert tool.fromarray(str(model), pixels)
    axis = tool.coordsys()
    assert axis.setunits(['rad', 'rad', '', 'Hz'])
    assert axis.setreferencevalue(value=f'{ra}rad {dec}rad', type='direction')
    assert axis.setreferencepixel(value=[128., 128.], type='direction')
    assert axis.setincrement(value='-6arcsec 6arcsec', type='direction')
    assert axis.setreferencecode('TOPO', type='spectral', adjust=False)
    assert axis.setreferencepixel(value=[0.], type='spectral')
    assert axis.setreferencevalue(value=f'{frequencies[0]}Hz', type='spectral')
    assert axis.setincrement(value=f'{frequencies[1] - frequencies[0]}Hz', type='spectral')
    assert tool.setcoordsys(axis.torecord()) and axis.done()
    assert tool.setbrightnessunit('Jy/pixel') and tool.done()
    return model


def observed_workers(call):
    quit_watch = threading.Event()
    maximum = 0

    def watch():
        nonlocal maximum
        while not quit_watch.wait(.001):
            workers = 0
            for name in Path('/proc/self/task').glob('*/comm'):
                try:
                    workers += name.read_text().startswith('imaging-worker-')
                except (FileNotFoundError, ProcessLookupError):
                    continue
            maximum = max(maximum, workers)

    watcher = threading.Thread(target=watch)
    watcher.start()
    try:
        assert call()
    finally:
        quit_watch.set()
        watcher.join()
    return maximum


def predict(factory, source, target, model, threads, *, scratch, observe):
    copy_input(source, target, scratch)
    tool = factory()
    assert tool.open(str(target), usescratch=scratch) and tool.selectvis(field='0,1', spw='0')
    assert tool.defineimage(nx=256, ny=256, cellx='6arcsec', celly='6arcsec',
                            phasecenter=0, mode='channel', nchan=12, outframe='TOPO')
    assert tool.setoptions(ftmachine='ft', numthreads=threads)
    if observe:
        workers = observed_workers(lambda: tool.ft(str(model)))
    else:
        assert tool.ft(str(model))
        workers = None
    assert tool.done()
    if not scratch and observe:
        grid, = model_grids(model_recipe(target))
        assert grid['numthreads'] == threads
    return workers


def faceted_mosaic_limit(root, source, model, factory):
    tool = image()
    assert tool.open(str(model))
    coords = tool.coordsys()
    assert coords.setincrement(value='-0.3arcsec 0.3arcsec', type='direction')
    assert tool.setcoordsys(coords.torecord()) and coords.done()
    pixels = tool.getchunk()
    pixels[127, 127, 0, :] = .7
    assert tool.putchunk(pixels) and tool.done()

    target = root / 'faceted-mosaic.ms'
    copy_input(source, target, False)
    im = factory()
    assert im.open(str(target), usescratch=False) and im.selectvis(field='0,1', spw='0')
    assert im.defineimage(nx=256, ny=256, cellx='.3arcsec', celly='.3arcsec',
                          phasecenter=0, outframe='TOPO', facets=2)
    assert im.setoptions(ftmachine='mosaic', numthreads=1)
    count = observed_workers(lambda: im.ft(str(model)))
    assert count == 0 and im.done()
    grids = model_grids(model_recipe(target))
    assert [grid['name'] for grid in grids] == ['MosaicFT', 'GridFT', 'GridFT', 'GridFT']
    assert [grid['numthreads'] for grid in grids] == [1, -1, -1, -1]
    return {'native_background_workers': count, 'facets': len(grids)}


def run(root, check_workers):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source = root / 'source.ms'
    configure_data(source)
    model = create_cube(root, source)
    results = {}
    for scratch in (True, False):
        for threads in (1, 2, -1, 0):
            actual = root / f'{scratch}-{threads}-native.ms'
            reference = root / f'{scratch}-{threads}-casa.ms'
            predict(imager, source, reference, model, threads, scratch=scratch, observe=False)
            count = predict(native, source, actual, model, threads, scratch=scratch,
                            observe=check_workers)
            if check_workers:
                if threads == 1:
                    assert count == 0
                elif threads == 2:
                    assert count == 2
                else:
                    assert 2 <= count <= 12
            if scratch:
                expected = field_samples(reference)
                observed = field_samples(actual)
            else:
                expected = consume(reference, root / f'{threads}-casa-reopened.ms', 'casa')
                observed = consume(actual, root / f'{threads}-native-reopened.ms', 'casa')
            assert all(abs(values).max() > 1. for values in expected)
            if threads == 1:
                serial = observed
            else:
                assert all(np.array_equal(a, b) for a, b in zip(observed, serial, strict=True))
            results[f'{scratch}-{threads}'] = {**compare(observed, expected),
                                               'native_background_workers': count}
    if check_workers:
        results['faceted_mosaic'] = faceted_mosaic_limit(root, source, model, native)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--check-workers', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    results = run(root, args.check_workers)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)


if __name__ == '__main__':
    main()
