#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair legacy imager.setsdoptions prediction with CASA and analytic kernels."""

import argparse
import inspect
import json
from pathlib import Path
import shutil
import subprocess
import sys

import casatasks
from casatools import image, imager, measures, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_mosaic_oracle import fixture
from imager_single_dish_oracle import (
    analytic_gaussian, correlations, model_image, prepare_ms, read_virtual, setup,
)


def predict(factory, path, model, mode, kernel, settings, *, scratch=True):
    tool = setup(factory, path, mode, kernel, pinned=False, scratch=scratch)
    if isinstance(settings, list):
        assert tool.setsdoptions(*settings)
    else:
        assert tool.setsdoptions(**settings)
    assert tool.ft(str(model))
    (path.parent / f'{path.name}.prediction-complete').touch()
    assert tool.done()


def optional_columns(path):
    pointing = table()
    assert pointing.open(str(path / 'POINTING'), nomodify=False)
    target = pointing.getcol('TARGET')
    template = pointing.getcoldesc('TARGET')
    for name, offset in [('ENCODER', 1.4e-5), ('POINTING_OFFSET', -1.8e-5), ('SOURCE_OFFSET', 3.e-5)]:
        cells = target.copy()
        cells[0, 0, :] += offset
        if name == 'ENCODER':
            cells = cells[:, 0, :]
        shape = np.asarray(cells.shape[:-1], dtype=np.int32)
        assert pointing.addcols({name: template | {'ndim': len(shape), 'shape': shape, 'option': 4}})
        assert pointing.putcol(name, cells)
    assert pointing.done()


def comparison(root, source, model, label, kernel, settings, *, mode='sd', expected=None,
               reference_source=None):
    from casars.tools import imager as native

    pinned_path = root / f'{label}-pinned.ms'
    scratch_path = root / f'{label}-native.ms'
    if expected is None:
        prepare_ms(reference_source or source, pinned_path, mode)
        reference_settings = {} if reference_source else settings
        reference = subprocess.run([sys.executable, __file__, '--casa', str(pinned_path), str(model),
                                    mode, kernel, json.dumps(reference_settings)], check=False)
        assert reference.returncode in (0, -11)
        assert (root / f'{pinned_path.name}.prediction-complete').is_file()
        _, expected, _ = correlations(pinned_path)
    prepare_ms(source, scratch_path, mode)
    predict(native, scratch_path, model, mode, kernel, settings)
    _, actual, _ = correlations(scratch_path)
    virtual_path = root / f'{label}-virtual.ms'
    prepare_ms(source, virtual_path, mode, scratch=False)
    predict(native, virtual_path, model, mode, kernel, settings, scratch=False)
    virtual = read_virtual(virtual_path, mode, kernel)
    errors = [relative_rms(output, expected) for output in (actual, virtual)]
    print(label, errors, 'peak', float(np.abs(expected).max()), flush=True)
    assert max(errors) < 1.e-3
    return expected


def gaussian_parameters(root, source, model, shape):
    for kernel, settings, widths in (
        ('GAUSS', {'truncate': '0.96arcsec', 'gwidth': {'value': .66, 'unit': 'arcsec'}},
         (2.2, 3.2, 1.55)),
        ('GJINC', [1., 1., -1, 'DIRECTION', '2.3pixel', 1.7, '0.375arcsec'],
         (1.7, 2.3, 1.25)),
    ):
        expected = analytic_gaussian(source, model, kernel, shape, widths=widths)
        comparison(root, source, model, f'{kernel}-widths', kernel, settings, expected=expected)


def validation(root, source, model):
    from casars.tools import imager as native

    target = root / 'reconfigure.ms'
    prepare_ms(source, target, 'sd')
    tool = setup(native, target, 'sd', 'SF', pinned=False)
    assert tool.setsdoptions(convsupport=1) and tool.ft(str(model))
    try:
        tool.setsdoptions(pointingcolumntouse='PHASE_DIR')
    except RuntimeError as exception:
        assert 'PHASE_DIR' in str(exception)
    else:
        raise AssertionError('PHASE_DIR is absent from POINTING')
    assert tool.setsdoptions(convertfirst='unknown') and not tool.ft(str(model))
    assert tool.setsdoptions(convsupport=4) and tool.ft(str(model)) and tool.done()
    _, actual, _ = correlations(target)
    _, expected, _ = correlations(root / 'support4-native.ms')
    assert np.array_equal(actual, expected)


def convert_pointing_modes(root, source, model):
    from casars.tools import imager as native

    equatorial = root / 'equatorial-tracks.ms'
    horizontal = root / 'horizontal-tracks.ms'
    shutil.copytree(source, equatorial)
    tb = table()
    assert tb.open(str(equatorial), nomodify=False)
    times = tb.getcol('TIME')
    origin = times.min()
    main_time = origin + 100. * (times - origin) + 220.
    assert tb.putcol('TIME', main_time) and tb.putcol('TIME_CENTROID', main_time)
    assert tb.putcol('INTERVAL', np.ones_like(main_time)) and tb.done()
    assert tb.open(str(equatorial / 'POINTING'), nomodify=False)
    dates = origin + 100. * (tb.getcol('TIME') - origin)
    assert tb.putcol('TIME', dates) and tb.putcol('INTERVAL', np.full_like(dates, 1000.))
    assert tb.done()
    shutil.copytree(equatorial, horizontal)
    assert tb.open(str(horizontal / 'ANTENNA'))
    positions = tb.getcol('POSITION')
    assert tb.done()
    assert tb.open(str(horizontal / 'POINTING'), nomodify=False)
    axes = tb.getcol('DIRECTION')
    antennas = tb.getcol('ANTENNA_ID')
    m = measures()
    frames = [m.position('ITRF', *(f'{position:.14g}m' for position in xyz)) for xyz in positions.T]
    for index, (antenna, date) in enumerate(zip(antennas, dates, strict=True)):
        assert m.doframe(frames[antenna]) and m.doframe(m.epoch('UTC', f'{date:.9f}s'))
        longitude, latitude = axes[:, 0, index]
        sky = m.direction('J2000', f'{longitude:.17g}rad', f'{latitude:.17g}rad')
        converted = m.measure(sky, 'AZEL')
        axes[:, 0, index] = [converted[key]['value'] for key in ('m0', 'm1')]
    assert m.done()
    assert tb.putcol('DIRECTION', axes)
    metadata = tb.getcolkeyword('DIRECTION', 'MEASINFO')
    assert tb.putcolkeyword('DIRECTION', 'MEASINFO', metadata | {'Ref': 'AZEL'}) and tb.done()
    gradient = root / 'gradient.im'
    shutil.copytree(model, gradient)
    ia = image()
    assert ia.open(str(gradient))
    x, y = np.mgrid[:320, :320]
    brightness = (1.e-3 + x * 1.e-4 + y * 3.e-5).astype(np.float32)
    assert ia.putchunk(brightness[:, :, None, None]) and ia.done()
    expected = comparison(root, horizontal, gradient, 'convert-always', 'BOX',
                          {'convertfirst': 'ALWAYS'}, reference_source=equatorial)
    comparison(root, horizontal, gradient, 'convert-auto', 'BOX',
               {'convertfirst': 'AUTO', 'enablecache': True}, expected=expected)
    never_path = root / 'convert-never.ms'
    prepare_ms(horizontal, never_path, 'sd')
    predict(native, never_path, gradient, 'sd', 'BOX', {'convertfirst': 'NEVER'})
    _, never, _ = correlations(never_path)
    difference = relative_rms(never, expected)
    print('NEVER against convert-first control', difference, flush=True)
    assert difference > 1.e-3


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    assert inspect.signature(native().setsdoptions) == inspect.signature(imager().setsdoptions)
    root.mkdir(parents=True)
    _, offset, model = fixture(root)
    model_image(model)
    with_columns = root / 'all-pointing-columns.ms'
    shutil.copytree(offset, with_columns)
    optional_columns(with_columns)
    baseline = comparison(root, offset, model, 'default', 'SF', {})
    assert np.abs(baseline).max() > 1.e-3
    for name, support in [('support1', 1), ('support4', 4)]:
        result = comparison(root, offset, model, name, 'SF', {'convsupport': support})
        assert relative_rms(result, baseline) > .25
    zero = comparison(root, offset, model, 'support0', 'SF', {'convsupport': 0})
    assert not zero.any()
    for column in ('TARGET', 'ENCODER', 'POINTING_OFFSET', 'SOURCE_OFFSET'):
        result = comparison(root, with_columns, model, column, 'SF', {'pointingcolumntouse': column.lower()})
        assert relative_rms(result, baseline) > 1.e-3
    fallback = comparison(root, offset, model, 'missing-column', 'SF', {'pointingcolumntouse': 'SOURCE_OFFSET'})
    assert np.array_equal(fallback, baseline)
    cached = comparison(root, offset, model, 'cache-always', 'SF', {'enablecache': True, 'convertfirst': 'always'})
    assert np.array_equal(cached, baseline)
    mixed = comparison(root, offset, model, 'mixed-default', 'SF', {}, mode='both')
    configured = comparison(root, offset, model, 'mixed-configured', 'SF',
                            {'scale': 2., 'weight': .25, 'minweight': .3, 'clipminmax': True,
                             'convsupport': 1, 'pointingcolumntouse': 'TARGET'}, mode='both')
    assert np.array_equal(mixed, configured)
    gaussian_parameters(root, offset, model, baseline.shape)
    validation(root, offset, model)
    convert_pointing_modes(root, offset, model)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--casa', nargs=5)
    args = parser.parse_args()
    if args.casa:
        path, model, mode, kernel, settings = args.casa
        predict(imager, Path(path), Path(model), mode, kernel, json.loads(settings))
    else:
        run(args.work.resolve())


if __name__ == '__main__':
    main()
