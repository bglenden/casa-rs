#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair single-dish gridding and applied voltage patterns with pinned CASA."""

import argparse
from pathlib import Path
import subprocess
import sys

import casatasks
from casatools import imager, vpmanager
import numpy as np

from gaincal_oracle import relative_rms
from imager_mosaic_oracle import fixture
from imager_single_dish_oracle import correlations, model_image, prepare_ms, read_virtual


def predict(factory, path, model, mode, pattern, applied, *, scratch=True):
    tool = factory()
    assert tool.open(str(path), usescratch=scratch) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=320, ny=320, cellx='1.2arcsec', celly='1.2arcsec',
                            phasecenter=0, outframe='TOPO')
    assert tool.setsdoptions(convertfirst='NEVER')
    assert tool.setvp(dovp=applied, usedefaultvp=pattern is None, vptable=str(pattern or ''))
    assert tool.setoptions(ftmachine=mode, gridfunction='PB', numthreads=1)
    assert tool.ft(str(model))
    (path.parent / f'{path.name}.prediction-complete').touch()
    assert tool.done()


def pair(root, source, model, mode, pattern, applied):
    from casars.tools import imager as native

    label = f'{mode}-{int(pattern is not None)}-{int(applied)}'
    pinned, scratch, stored = (root / f'{label}-{suffix}.ms'
                               for suffix in ('pinned', 'scratch', 'stored'))
    prepare_ms(source, pinned, mode)
    prepare_ms(source, scratch, mode)
    prepare_ms(source, stored, mode, scratch=False)
    proc = subprocess.run([sys.executable, __file__, '--casa', str(pinned), str(model),
                           mode, str(pattern or ''), str(int(applied))], check=False)
    assert proc.returncode in (0, -11) and (root / f'{pinned.name}.prediction-complete').exists()
    auto, expected, _ = correlations(pinned)
    predict(native, scratch, model, mode, pattern, applied)
    _, actual, _ = correlations(scratch)
    predict(native, stored, model, mode, pattern, applied, scratch=False)
    if pattern is not None:
        unavailable = pattern.with_suffix('.outside')
        pattern.rename(unavailable)
    try:
        reopened = read_virtual(stored, mode, 'PB')
    finally:
        if pattern is not None:
            unavailable.rename(pattern)
    for name, rows in [('autocorrelations', auto), ('cross-correlations', ~auto)]:
        if rows.any():
            reference = expected[..., rows]
            errors = [relative_rms(samples[..., rows], reference) for samples in (actual, reopened)]
            print(label, name, errors, float(np.abs(reference).max()), flush=True)
            assert np.abs(reference).max() > .01 and max(errors) < 1.e-3
    return expected


def run(root):
    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, _, model = fixture(root)
    model_image(model, wide=True)
    pattern = root / 'gaussian.vp'
    manager = vpmanager()
    assert manager.reset()
    assert manager.setpbgauss(telescope='ALMA', halfwidth='23arcsec', maxrad='90arcsec',
                              reffreq='100GHz', dopb=True)
    assert manager.setuserdefault(vplistnum=0, telescope='ALMA')
    assert manager.saveastable(str(pattern)) and manager.reset()
    for mode in ('sd', 'both'):
        default = pair(root, source, model, mode, None, False)
        custom = pair(root, source, model, mode, pattern, False)
        applied = pair(root, source, model, mode, None, True)
        applied_custom = pair(root, source, model, mode, pattern, True)
        if mode == 'both':
            assert np.array_equal(custom, default)
        else:
            assert relative_rms(custom, default) > .2
        assert relative_rms(applied, default) > .2
        assert relative_rms(applied_custom, applied) > .2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--casa', nargs=5)
    args = parser.parse_args()
    if args.casa:
        path, model, mode, pattern, applied = args.casa
        predict(imager, Path(path), Path(model), mode, Path(pattern) if pattern else None,
                applied == '1')
    else:
        run(args.work.resolve())


if __name__ == '__main__':
    main()
