#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair single-dish gridding and applied voltage patterns with pinned CASA."""

import argparse
from pathlib import Path
import shutil
import subprocess
import sys

import casatasks
from casatools import image, imager, table, vpmanager
import numpy as np

from gaincal_oracle import relative_rms
from imager_facets_oracle import model_grids, model_recipe
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


def pinned_predict(path, model, mode, pattern, applied, *, scratch=True):
    command = [sys.executable, __file__, '--casa', str(path), str(model),
               mode, str(pattern or ''), str(int(applied))]
    if not scratch:
        command.append('--saved')
    proc = subprocess.run(command, check=False)
    assert proc.returncode in (0, -11) and (path.parent / f'{path.name}.prediction-complete').exists()


def apparent_sky(path):
    entry, = (entry for entry in model_grids(model_recipe(path)) if entry['name'] == 'SDGrid')
    tool = image()
    assert tool.open(entry['diskimage'])
    values = tool.getchunk()
    assert tool.done()
    return values


def pinned_saved_reference(root, source, model, mode, pattern, native_stored, label):
    saved = root / f'{label}-pinned-saved.ms'
    prepare_ms(source, saved, mode, scratch=False)
    pinned_predict(saved, model, mode, pattern, True, scratch=False)
    pixels = apparent_sky(saved)
    assert np.iscomplexobj(pixels) and not np.any(pixels.imag)
    assert relative_rms(apparent_sky(native_stored), pixels) < 1.e-5

    # CASA's reader cannot consume its SDGrid recipe. Run pinned scratch
    # prediction with the first apparent sky that CASA actually saved.
    apparent_model = root / f'{label}-apparent.im'
    tool = image()
    assert tool.open(str(model))
    coords = tool.coordsys()
    assert tool.done()
    assert tool.fromarray(str(apparent_model), pixels.real.astype(np.float32), csys=coords.torecord())
    assert coords.done() and tool.done()
    reference = root / f'{label}-pinned-apparent.ms'
    prepare_ms(source, reference, mode)
    pinned_predict(reference, apparent_model, mode, pattern, False)
    return correlations(reference)[1]


def pair(root, source, model, mode, pattern, applied):
    from casars.tools import imager as native

    label = f'{mode}-{pattern.stem if pattern else "default"}-{int(applied)}'
    pinned, scratch, stored = (root / f'{label}-{suffix}.ms'
                               for suffix in ('pinned', 'scratch', 'stored'))
    prepare_ms(source, pinned, mode)
    prepare_ms(source, scratch, mode)
    prepare_ms(source, stored, mode, scratch=False)
    pinned_predict(pinned, model, mode, pattern, applied)
    auto, expected, _ = correlations(pinned)
    predict(native, scratch, model, mode, pattern, applied)
    _, actual, _ = correlations(scratch)
    predict(native, stored, model, mode, pattern, applied, scratch=False)
    saved_reference = (
        pinned_saved_reference(root, source, model, mode, pattern, stored, label)
        if applied else expected
    )
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
            errors = [relative_rms(actual[..., rows], reference),
                      relative_rms(reopened[..., rows], saved_reference[..., rows])]
            print(label, name, errors, float(np.abs(reference).max()), flush=True)
            assert np.abs(reference).max() > .01 and max(errors) < 1.e-3
    return expected


def airy_control(root, source, model):
    pattern = root / 'airy.vp'
    manager = vpmanager()
    assert manager.reset()
    assert manager.setpbairy(telescope='ALMA', dishdiam='9m', blockagediam='.6m',
                             maxrad='75arcsec', reffreq='100GHz')
    assert manager.setuserdefault(vplistnum=0, telescope='ALMA')
    assert manager.saveastable(str(pattern)) and manager.reset()
    pair(root, source, model, 'sd', pattern, False)


def gaussian_table(root):
    pattern = root / 'gaussian.vp'
    manager = vpmanager()
    assert manager.reset()
    for halfwidth in ('23arcsec', '52arcsec'):
        assert manager.setpbgauss(telescope='ALMA', halfwidth=halfwidth, maxrad='90arcsec',
                                  reffreq='100GHz', dopb=True)
    assert manager.setuserdefault(vplistnum=1, telescope='ALMA')
    assert manager.saveastable(str(pattern)) and manager.reset()
    reader = table()
    assert reader.open(str(pattern / 'VPLIST_DEFAULTS'))
    defaults = dict(zip(reader.getcol('tel_and_anttype'), reader.getcol('vplistnum'), strict=True))
    assert defaults['ALMA'] == 1 and reader.done()
    return pattern


def reported_frame_control(root, source, model):
    reported = root / 'reported.im'
    shutil.copytree(model, reported)
    tool = image()
    assert tool.open(str(reported))
    coords = tool.coordsys()
    assert coords.setconversiontype(spectral='CMB')
    assert tool.setcoordsys(coords.torecord()) and coords.done() and tool.done()
    pair(root, source, reported, 'sd', None, True)


def selected_array_control(root, source, model, pattern):
    selected = root / 'array-selection.ms'
    prepare_ms(source, selected, 'sd')
    writer = table()
    assert writer.open(str(selected / 'OBSERVATION'), nomodify=False)
    assert writer.nrows() == 1 and writer.addrows(1)
    for column in writer.colnames():
        if writer.iscelldefined(column, 0):
            assert writer.putcell(column, 1, writer.getcell(column, 0))
    assert writer.putcell('TELESCOPE_NAME', 0, 'VLA') and writer.done()
    assert writer.open(str(selected), nomodify=False)
    for column in ('ARRAY_ID', 'OBSERVATION_ID'):
        assert writer.putcol(column, np.ones_like(writer.getcol(column)))
    assert writer.done()
    pair(root, selected, model, 'sd', pattern, False)


def run(root):
    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, _, model = fixture(root)
    model_image(model, wide=True)
    pattern = gaussian_table(root)
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
    airy_control(root, source, model)
    reporting = root / 'reported-frame'
    reporting.mkdir()
    reported_frame_control(reporting, source, model)
    array = root / 'selected-array'
    array.mkdir()
    selected_array_control(array, source, model, pattern)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--casa', nargs=5)
    parser.add_argument('--saved', action='store_true')
    args = parser.parse_args()
    if args.casa:
        path, model, mode, pattern, applied = args.casa
        predict(imager, Path(path), Path(model), mode, Path(pattern) if pattern else None,
                applied == '1', scratch=not args.saved)
    else:
        run(args.work.resolve())


if __name__ == '__main__':
    main()
