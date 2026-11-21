#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare resolved setjy models, saved recipes and CASA interoperability."""

import argparse
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from time import perf_counter

import casatasks as casa
from casatools import ctsys, image, table
from casars import tasks as rust
import numpy as np

from imager_legacy_pbw_oracle import recipe
from calibration_ft_oracle import coordinates
from setjy_manual_oracle import (
    add_second_window, casa_ms, compare, compare_returns, consume, data, make_input, rust_ms,
)


def case(seed, root, name, *, reference=None, casaset=True, cube=False, downstream=False,
         spw='0:0~2', **options):
    readers = (('casa', casa_ms), ('rust', rust_ms))
    writers = (('casa', casa), ('rust', rust)) if casaset else (('rust', rust),)
    for scratch in (True, False):
        with TemporaryDirectory(dir=root, prefix=f'{name}-{scratch}-') as tmp:
            directory = Path(tmp)
            values = []
            tables = {}
            for writer, tasks in writers:
                target = directory / f'{writer}.ms'
                make_input(seed, target, scratch=scratch)
                started = perf_counter()
                result = tasks.setjy(str(target), field='0', spw=spw, usescratch=scratch,
                                     **options)
                values.append(result)
                print(name, writer, 'scratch', scratch, 'seconds', perf_counter() - started,
                      flush=True)
                if downstream:
                    for reader, solver in (('casa', casa), ('rust', rust)):
                        tables[writer, reader] = solve(solver, target, directory, writer, reader)
                if scratch:
                    predicted = data(target, 'MODEL_DATA')
                    if reference is None:
                        reference = predicted
                    compare(f'{name} {writer} scratch', predicted, reference)
                    if writer == 'rust':
                        native = predicted
                    continue
                record = recipe(target)
                expected_machine = ('SetJyGridFT' if options.get('scalebychan', True) and not cube
                                    else 'GridFT')
                assert record['name'] == expected_machine
                if expected_machine == 'SetJyGridFT':
                    assert record['scaleamp'].shape == record['freqscale'].shape
                    assert np.all(np.diff(record['freqscale']) > 0)
                for reader, factory in readers:
                    read = directory / f'{writer}-{reader}-read.ms'
                    shutil.copytree(target, read)
                    compare(f'{name} {writer} writer {reader} reader',
                            consume(factory, read), reference)
                    shutil.rmtree(read)
            if casaset:
                compare_returns(values[1], values[0], rtol=5e-8)
            else:
                np.testing.assert_array_equal(values[0]['0']['0']['fluxd'], [1., 0., 0., 0.])
            for (writer, reader), rows in tables.items():
                if writer == 'rust':
                    for task, (actual, flags) in rows.items():
                        expected, pinned_flags = tables['casa', reader][task]
                        np.testing.assert_array_equal(flags, pinned_flags)
                        assert np.any(~flags), (task, reader)
                        compare(f'{task} {reader} consuming either model writer', actual[~flags],
                                expected[~flags])
    return native


def corrected_flux(seed, root, model, unscaled):
    ws = table()
    assert ws.open(str(seed / 'SPECTRAL_WINDOW'))
    hz = ws.getcell('CHAN_FREQ', 0)
    assert ws.getcell('MEAS_FREQ_REF', 0) == 1 and ws.done()

    ia = image()
    assert ia.open(str(model))
    cs = ia.coordsys()
    x, y = cs.referencepixel()['numeric'][:2]
    rad_per_pixel = abs(cs.increment()['numeric'][0])
    pixels = ia.getchunk().squeeze()
    xx, yy = np.indices(pixels.shape)
    radius = 3. * np.pi / (180. * 3600.) / rad_per_pixel
    sum_i = np.sum(pixels[(xx - x)**2 + (yy - y)**2 <= radius**2], dtype=np.float64)
    assert cs.done() and ia.done()

    for per_channel in (True, False):
        options = {'scalebychan': per_channel, 'model': str(model), 'standard': 'manual'}
        for label, coefficients in (('flat', [0.]), ('curved', [-.74, .13])):
            sample_hz = hz if per_channel else np.median(hz)
            # Independent polynomial evaluation in dimensionless log frequency.
            x = np.log10(sample_hz / 1.41e9)
            intensity = 2.4 * 10.**sum(a * x**(i + 1) for i, a in enumerate(coefficients))
            reference = unscaled * (intensity / sum_i if not per_channel
                                    else (intensity / sum_i)[None, :, None])
            assert np.max(np.abs(reference)) > 2.
            case(seed, root, f'corrected-{label}-{per_channel}', casaset=False,
                 reference=reference, fluxdensity=2.4, reffreq='1.41GHz',
                 spix=coefficients, **options)


def solve(solver, target, directory, writer, reader):
    refant = data(target / 'ANTENNA', 'NAME')[data(target, 'ANTENNA1')[0]]
    answers = {}
    for name in ('gaincal', 'bandpass'):
        path = directory / f'{writer}-{reader}-{name}.cal'
        getattr(solver, name)(vis=str(target), field='0', spw='0', caltable=str(path),
                              solint='inf', refant=refant, minsnr=0.)
        answers[name] = data(path, 'CPARAM'), data(path, 'FLAG')
    return answers


def write_cube(path, seed):
    reader = table()
    assert reader.open(str(seed / 'SPECTRAL_WINDOW'))
    hz = reader.getcell('CHAN_FREQ', 0)
    assert reader.done()
    pixels = np.zeros((32, 32, 1, 5), dtype=np.float32)
    pixels[12, 15, 0, :] = [1., .9, .7, 1.4, 2.2]
    pixels[16, 19, 0, :] = [.7, .3, .6, .4, .3]
    ra, dec = coordinates(seed)
    ia = image()
    assert ia.fromarray(outfile=str(path), pixels=pixels)
    cs = ia.coordsys()
    assert cs.setunits(['rad', 'rad', '', 'Hz'])
    assert cs.setreferencevalue(f'{ra}rad {dec}rad', type='direction')
    assert cs.setincrement('-0.5arcsec 0.5arcsec', type='direction')
    assert cs.setreferencepixel([16., 16.], type='direction')
    assert cs.setreferencecode('LSRK', type='spectral', adjust=False)
    assert cs.setreferencepixel([0.], type='spectral')
    assert cs.setreferencevalue(f'{hz[0] - 1e5}Hz', type='spectral')
    assert cs.setincrement(f'{(hz[-1] - hz[0] + 2e5) / 4}Hz', type='spectral')
    assert ia.setcoordsys(cs.torecord()) and ia.setbrightnessunit('Jy/pixel')
    assert cs.done() and ia.done()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('vla_ms', type=Path)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    model_name = '3C286_L.im'
    model = Path(ctsys.resolve('nrao/VLA/CalModels/' + model_name))
    by_frequency = {}
    for by_channel in (True, False):
        by_frequency[by_channel] = case(
            args.vla_ms, args.root, f'unscaled-{by_channel}', model=str(model),
            standard='manual', fluxdensity=[0., .1, -.2, .3], scalebychan=by_channel)
        case(args.vla_ms, args.root, f'catalog-{by_channel}', model=model_name,
             scalebychan=by_channel, downstream=by_channel)
    case(args.vla_ms, args.root, 'manual-constant', model=str(model), standard='manual',
         fluxdensity=2.4, scalebychan=False)
    corrected_flux(args.vla_ms, args.root, model, by_frequency[True])
    cube = args.root / 'model-cube.im'
    write_cube(cube, args.vla_ms)
    for by_channel in (True, False):
        predicted = case(args.vla_ms, args.root, f'cube-{by_channel}', model=str(cube),
                         standard='manual', fluxdensity=23., scalebychan=by_channel, cube=True)
        assert np.max(np.abs(predicted)) < 3. and np.max(np.abs(predicted)) > 1.
    with TemporaryDirectory(dir=args.root) as tmp:
        two_windows = Path(tmp) / 'two-windows.ms'
        make_input(args.vla_ms, two_windows, scratch=False)
        add_second_window(two_windows)
        case(two_windows, args.root, 'two-windows', model=model_name, spw='0,1')
    dictionary = {'0': {'0': {'fluxd': np.array([2.4, 0., 0., 0.])}, 'fitFluxd': 2.4,
                        'fitRefFreq': 1.41e9, 'spidx': np.array([np.log10(2.4), 0.])},
                  'spwID': np.array([0]), 'freq': np.array([1.413e9])}
    case(args.vla_ms, args.root, 'fluxscale-image', model=model_name, standard='fluxscale',
         fluxdict=dictionary, scalebychan=False, spw='')


if __name__ == '__main__':
    main()
