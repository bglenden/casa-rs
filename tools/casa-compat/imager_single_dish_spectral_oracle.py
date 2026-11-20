#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair legacy SDGrid channel interpolation with pinned CASA."""

import argparse
from pathlib import Path
import subprocess
import sys

import casatasks
from casatools import image, imager
import numpy as np

from gaincal_oracle import relative_rms
from imager_mosaic_oracle import fixture
from imager_single_dish_oracle import correlations, prepare_ms, read_virtual


def write_cube(template, cube, values):
    tool = image()
    assert tool.open(str(template))
    coords = tool.coordsys()
    assert tool.done()
    assert coords.setreferencevalue(value='99999500000Hz', type='spectral')
    assert coords.setreferencepixel(value=[0.], type='spectral')
    assert coords.setincrement(value='2.7MHz', type='spectral')
    pixels = np.broadcast_to(np.asarray(values, dtype=np.float32), (320, 320, 1, len(values))).copy()
    assert tool.fromarray(str(cube), pixels, csys=coords.torecord()) and coords.done() and tool.done()


def predict(factory, path, model, mode, method, *, scratch=True, spw=''):
    tool = factory()
    assert tool.open(str(path), usescratch=scratch) and tool.selectvis(field='0,1', spw=spw)
    assert tool.defineimage(nx=320, ny=320, cellx='.3arcsec', celly='.3arcsec',
                            phasecenter=0, mode='channel', nchan=5, outframe='TOPO')
    assert tool.setsdoptions(convertfirst='NEVER')
    assert tool.setoptions(ftmachine=mode, gridfunction='BOX', freqinterp=method, numthreads=1)
    result = tool.ft(str(model))
    if result:
        (path.parent / f'{path.name}.prediction-complete').touch()
    assert tool.done()
    return result


def pair(root, source, model, mode, method, *, virtual=True, spw=''):
    from casars.tools import imager as native

    label = f'{mode}-{method}' + ('-channel' if spw else '')
    pinned = root / f'{label}-pinned.ms'
    scratch = root / f'{label}-scratch.ms'
    stored = root / f'{label}-virtual.ms'
    for path in (pinned, scratch):
        prepare_ms(source, path, mode)
    proc = subprocess.run([sys.executable, __file__, '--casa', str(pinned), str(model),
                           mode, method, '--spw', spw], check=False)
    assert proc.returncode in (0, -11) and (root / f'{pinned.name}.prediction-complete').exists()
    auto, expected, _ = correlations(pinned)
    assert predict(native, scratch, model, mode, method, spw=spw)
    _, actual, _ = correlations(scratch)
    products = [actual]
    if virtual:
        prepare_ms(source, stored, mode, scratch=False)
        assert predict(native, stored, model, mode, method, scratch=False)
        products.append(read_virtual(stored, mode, 'BOX'))
    for name, rows in [('autocorrelations', auto), ('cross-correlations', ~auto)]:
        if rows.any():
            values = expected[..., rows]
            errors = [relative_rms(product[..., rows], values) for product in products]
            print(label, name, errors, flush=True)
            assert np.abs(values).max() > .1 and max(errors) < 1.e-3
    return expected


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    _, source, template = fixture(root)
    model = root / 'cube.im'
    write_cube(template, model, [1., 2., .3, 4., 2.5])
    for mode in ('sd', 'both'):
        nearest = pair(root, source, model, mode, 'nearest')
        for method in ('linear', 'cubic', 'spline'):
            result = pair(root, source, model, mode, method)
            if mode == 'both':
                assert np.array_equal(result, nearest)
            else:
                assert relative_rms(result, nearest) > .05
    nearest = pair(root, source, model, 'sd', 'nearest', virtual=False, spw='0:5')
    linear = pair(root, source, model, 'sd', 'linear', virtual=False, spw='0:5')
    assert np.array_equal(nearest, linear)
    short = root / 'short.im'
    write_cube(template, short, [1., 2., .3])
    for scratch in (True, False):
        path = root / f'short-{scratch}.ms'
        prepare_ms(source, path, 'sd', scratch=scratch)
        assert not predict(native, path, short, 'sd', 'cubic', scratch=scratch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--casa', nargs=4)
    parser.add_argument('--spw', default='')
    args = parser.parse_args()
    if args.casa:
        path, model, mode, method = args.casa
        assert predict(imager, Path(path), Path(model), mode, method, spw=args.spw)
    else:
        run(args.work.resolve())


if __name__ == '__main__':
    main()
