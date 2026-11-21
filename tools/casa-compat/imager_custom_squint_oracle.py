#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair scratch and saved imager models using a custom radial beam's squint."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casatools import imager as casa_imager, measures, ms as casa_ms, table, vpmanager
import numpy as np

from gaincal_oracle import relative_rms
from imager_component_voltage_oracle import compare, predict, scratch_pair, source
from imager_legacy_pbw_oracle import corrected_or_data, data, make_input


def radial_table(path, frequency, *, airy=False, wrap_longitude=False):
    me = measures()
    direction = me.direction('J2000', '-180arcsec' if airy else '180arcsec', '72arcsec')
    manager = vpmanager()
    manager.reset()
    options = dict(telescope='VLA', maxrad='1500arcsec', reffreq='1.4GHz',
                   squintdir=direction, squintreffreq=frequency, dosquint=True)
    if airy:
        definition = manager.setpbairy(dishdiam='25m', blockagediam='2.36m', **options)
    else:
        definition = manager.setpbgauss(halfwidth='400arcsec', **options)
    reported = definition['squintdir']
    assert reported['refer'] == direction['refer']
    for axis in ('m0', 'm1'):
        assert reported[axis]['unit'] == direction[axis]['unit'] == 'rad'
        np.testing.assert_allclose(reported[axis]['value'], direction[axis]['value'], atol=1.e-15)
    assert manager.saveastable(str(path))
    reader = table()
    assert reader.open(str(path), nomodify=not wrap_longitude)
    stored = reader.getcell('pbdescription', 0)
    assert stored['squintdir'] == reported and stored['squintreffreq'] == definition['squintreffreq']
    if wrap_longitude:
        # CASA writes signed longitude. The native direction serializer uses [0, 2π).
        stored['squintdir']['m0']['value'] += np.float64(2. * np.pi)
        assert reader.putcell('pbdescription', 0, stored)
    assert reader.done()


def check_other_gridder(root, seed, model, beam):
    from casars.tools import imager as native_imager

    target = root / 'unsupported-wproject.ms'
    make_input(seed, target, scratch=True)
    tool = native_imager()
    assert tool.open(str(target), usescratch=True) and tool.selectvis(field=0, spw='0')
    assert tool.defineimage(nx=64, ny=64, cellx='64arcsec', celly='64arcsec')
    assert tool.setoptions(ftmachine='wproject', padding=1.4, wprojplanes=8)
    assert tool.setvp(dovp=True, usedefaultvp=False, vptable=str(beam), dosquint=True)
    before = data(target, 'MODEL_DATA')
    assert not tool.ft(model=str(model))
    np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
    assert tool.setoptions(ftmachine='ft')
    assert tool.ft(model=str(model)) and tool.done()
    assert np.max(np.abs(data(target, 'MODEL_DATA') - before)) > .1


def saved_exchange(root, seed, cases, beam, results):
    from casars.tools import imager as native_imager, ms as native_ms

    saved = []
    for name, component, model, expected in cases:
        for writer, factory in [('casa', casa_imager), ('native', native_imager)]:
            path = root / f'{name}-{writer}-saved.ms'
            make_input(seed, path, scratch=False)
            predict(factory, path, component, model=model, scratch=False, squint=True, vptable=beam)
            saved.append((name, writer, path, expected))
    beam.rename(root / 'unavailable-vp.tbl')
    for name, writer, path, expected in saved:
        for reader, factory in [('casa', casa_ms), ('native', native_ms)]:
            target = root / f'{name}-{writer}-to-{reader}.ms'
            make_input(path, target, scratch=False)
            before = corrected_or_data(target)
            tool = factory()
            assert tool.open(str(target), nomodify=False)
            assert tool.uvsub() and tool.done()
            compare(results, f'{name}-{writer}-to-{reader}', before - data(target, 'CORRECTED_DATA'),
                    expected)


def run(root, seed, model):
    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        beam, half = work / 'squint.tbl', work / 'half.tbl'
        radial_table(beam, '1.4GHz')
        radial_table(half, '0.7GHz')
        component = work / 'point.cl'
        source(component, seed, [3.7, .4, -.6, -.8])
        unattenuated = scratch_pair(work, seed, component, results, 'no-beam', beam=False)

        image_squint = None
        component_squint = None
        for name, point, sky in [('component', component, None), ('image', None, model)]:
            radial = scratch_pair(work, seed, point, results, f'{name}-radial', model=sky, vptable=beam)
            shifted = scratch_pair(work, seed, point, results, f'{name}-squint', model=sky,
                                   squint=True, vptable=beam)
            assert relative_rms(shifted, radial) > .1
            if name == 'image':
                image_squint = shifted
            else:
                component_squint = shifted
        scaled = scratch_pair(work, seed, component, results, 'reference-frequency',
                              squint=True, vptable=half)
        assert relative_rms(scaled, component_squint) > .1
        airy, wrapped = work / 'airy.tbl', work / 'airy-wrapped.tbl'
        radial_table(airy, '0.7GHz', airy=True)
        radial_table(wrapped, '0.7GHz', airy=True, wrap_longitude=True)
        signed = scratch_pair(work, seed, component, results, 'airy-component-signed', squint=True,
                              vptable=airy)
        alternate = scratch_pair(work, seed, component, results, 'airy-component-wrapped',
                                 squint=True, vptable=wrapped)
        np.testing.assert_allclose(signed, alternate, rtol=1.e-6, atol=1.e-6)
        scratch_pair(work, seed, None, results, 'airy-image-wrapped', model=model, squint=True,
                     vptable=wrapped)
        check_other_gridder(work, seed, model, beam)
        saved_exchange(work, seed, [
            ('component', component, None, unattenuated),
            ('image', None, model, image_squint),
        ], beam, results)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(args.work.resolve(), args.dataset.resolve(), args.model.resolve())
