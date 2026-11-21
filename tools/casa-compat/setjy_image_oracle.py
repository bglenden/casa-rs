#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare resolved setjy models, saved recipes and CASA interoperability."""

import argparse
from pathlib import Path
import os
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
from setjy_perley_butler_oracle import rename


def case(seed, root, name, *, reference=None, casaset=True, cube=False, downstream=False,
         spw='0:0~2', mixed_basis=False, match_scale=False, **options):
    readers = (('casa', casa_ms), ('rust', rust_ms))
    writers = (('casa', casa), ('rust', rust)) if casaset else (('rust', rust),)
    for scratch in (True, False):
        with TemporaryDirectory(dir=root, prefix=f'{name}-{scratch}-') as tmp:
            directory = Path(tmp)
            values = []
            tables = {}
            records = []
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
                records.append(record)
                expected_machine = ('SetJyGridFT' if options.get('scalebychan', True) and not cube
                                    else 'GridFT')
                assert record['name'] == expected_machine
                if expected_machine == 'SetJyGridFT':
                    assert record['scaleamp'].shape == record['freqscale'].shape
                    assert np.all(np.diff(record['freqscale']) > 0)
                for reader, factory in readers:
                    read = directory / f'{writer}-{reader}-read.ms'
                    shutil.copytree(target, read)
                    predicted = consume(factory, read)
                    label = f'{name} {writer} writer {reader} reader'
                    if mixed_basis and reader == 'casa':
                        circular = data(read, 'DATA_DESC_ID') == 0
                        compare(label + ' circular window', predicted[..., circular],
                                reference[..., circular])
                        # Pinned CASA reports no model on the linear window for either writer.
                        assert np.max(np.abs(reference[..., ~circular])) > 3.
                        np.testing.assert_array_equal(predicted[..., ~circular], 0.)
                    else:
                        compare(label, predicted, reference)
                    shutil.rmtree(read)
            if casaset:
                compare_returns(values[1], values[0], rtol=5e-8)
            else:
                np.testing.assert_array_equal(values[0]['0']['0']['fluxd'], [1., 0., 0., 0.])
            if match_scale and not scratch:
                np.testing.assert_allclose(records[1]['freqscale'], records[0]['freqscale'],
                                           rtol=0., atol=.2)
                np.testing.assert_allclose(records[1]['scaleamp'], records[0]['scaleamp'],
                                           rtol=5e-7)
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


def write_image(path, seed, *, cube=True, frame='LSRK', conversion=None, inner_band=False):
    reader = table()
    assert reader.open(str(seed / 'SPECTRAL_WINDOW'))
    hz = reader.getcell('CHAN_FREQ', 0)
    assert reader.done()
    pixels = np.zeros((32, 32, 1, 5) if cube else (32, 32, 4, 1), dtype=np.float32)
    if cube:
        pixels[12, 15, 0, :] = [1., .9, .7, 1.4, 2.2]
        pixels[16, 19, 0, :] = [.7, .3, .6, .4, .3]
    else:
        pixels[12, 15, :, 0] = [2., .4, .2, .1]
        pixels[16, 19, :, 0] = [1., .1, -.3, .2]
    ra, dec = coordinates(seed)
    ia = image()
    assert ia.fromarray(outfile=str(path), pixels=pixels)
    cs = ia.coordsys()
    assert cs.setunits(['rad', 'rad', '', 'Hz'])
    assert cs.setreferencevalue(f'{ra}rad {dec}rad', type='direction')
    assert cs.setincrement('-0.5arcsec 0.5arcsec', type='direction')
    assert cs.setreferencepixel([16., 16.], type='direction')
    assert cs.setreferencecode(frame, type='spectral', adjust=False)
    assert cs.setreferencepixel([0.], type='spectral')
    lo, hi = (hz[20], hz[40]) if inner_band else (hz[0] - 1e5, hz[-1] + 1e5)
    assert cs.setreferencevalue(f'{lo}Hz', type='spectral')
    assert cs.setincrement(f'{(hi - lo) / 4}Hz', type='spectral')
    if conversion is not None:
        assert cs.setconversiontype(spectral=conversion)
    assert ia.setcoordsys(cs.torecord()) and ia.setbrightnessunit('Jy/pixel')
    assert cs.done() and ia.done()


def mixed_feeds(path):
    writer = table()
    assert writer.open(str(path / 'POLARIZATION'), nomodify=False)
    original = {name: writer.getcell(name, 0) for name in writer.colnames()}
    writer.addrows(1)
    for name, value in (original | {'CORR_TYPE': np.array([9, 12], dtype=np.int32)}).items():
        assert writer.putcell(name, 1, value)
    assert writer.done()
    assert writer.open(str(path / 'DATA_DESCRIPTION'), nomodify=False)
    assert writer.putcell('POLARIZATION_ID', 1, 1) and writer.done()
    assert writer.open(str(path / 'FEED'), nomodify=False)
    count = writer.nrows()
    writer.addrows(count)
    for row in range(count):
        for name in writer.colnames():
            if writer.iscelldefined(name, row):
                assert writer.putcell(name, row + count, writer.getcell(name, row))
        assert writer.putcell('SPECTRAL_WINDOW_ID', row, 0)
        assert writer.putcell('SPECTRAL_WINDOW_ID', row + count, 1)
        assert writer.putcell('POLARIZATION_TYPE', row + count, ['X', 'Y'])
    assert writer.done()


def ambiguity(seed, root, model):
    with TemporaryDirectory(dir=root) as tmp:
        workspace = Path(tmp)
        path = workspace / 'test.ms'
        make_input(seed, path, scratch=False)
        shutil.copytree(model, workspace / model.name)
        cwd = Path.cwd()
        try:
            os.chdir(workspace)
            for engine in (casa, rust):
                try:
                    engine.setjy(str(path), field='0', model=model.name, usescratch=False)
                except Exception as error:
                    assert 'absolute path' in str(error), error
                else:
                    raise AssertionError('ambiguous image name was accepted')
                assert engine.setjy(str(path), field='0', model=str(model), usescratch=False)
        finally:
            os.chdir(cwd)


def varied_epoch(path):
    writer = table()
    assert writer.open(str(path), nomodify=False)
    times = writer.getcol('TIME')
    later = times > np.median(times)
    assert writer.putcol('SCAN_NUMBER', np.where(later, 2, 1).astype(np.int32))
    for name in ('TIME', 'TIME_CENTROID'):
        values = writer.getcol(name)
        values[later] += 400 * 86400.
        assert writer.putcol(name, values)
    assert writer.done()


def topo(path):
    writer = table()
    assert writer.open(str(path / 'SPECTRAL_WINDOW'), nomodify=False)
    assert writer.putcell('MEAS_FREQ_REF', 0, 5) and writer.done()


def review_cases(seed, root, model):
    ambiguity(seed, root, model)
    with TemporaryDirectory(dir=root) as tmp:
        staged = Path(tmp)
        unknown = staged / 'unknown.ms'
        rename(seed, unknown, 'CASA_TEST')
        writer = table()
        assert writer.open(str(unknown / 'FIELD'), nomodify=False)
        for column in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
            direction = writer.getcell(column, 0)
            direction[0] += np.deg2rad(5. / 3600.)
            assert writer.putcell(column, 0, direction)
        assert writer.done()
        for by_channel in (True, False):
            predicted = case(unknown, root, f'unknown-catalog-{by_channel}', model=str(model),
                             scalebychan=by_channel)
            assert .99 < np.max(np.abs(predicted)) < 1.01

        cube = staged / 'short-band.im'
        write_image(cube, seed, inner_band=True)
        predicted = case(seed, root, 'cube-end-planes', model=str(cube), standard='manual',
                         fluxdensity=23., cube=True)
        assert np.max(np.abs(predicted[:, (0, -1), :])) > 2.

        topocentric = staged / 'topo.ms'
        make_input(seed, topocentric, scratch=False)
        topo(topocentric)
        for native_frame, reporting_frame in (('LSRK', None), ('TOPO', 'LSRK')):
            cube = staged / f'{native_frame}.im'
            write_image(cube, topocentric, frame=native_frame, conversion=reporting_frame)
            case(topocentric, root, f'topo-cube-{native_frame}', model=str(cube),
                 standard='manual', fluxdensity=23., cube=True)

        varying = staged / 'variable-calibrator.ms'
        rename(topocentric, varying, '3C48')
        varied_epoch(varying)
        case(varying, root, 'catalog-first-epoch', model=str(model), scan='2', match_scale=True)

        split_feed = staged / 'mixed-feed.ms'
        make_input(seed, split_feed, scratch=False)
        add_second_window(split_feed)
        mixed_feeds(split_feed)
        polarized = staged / 'polarized.im'
        write_image(polarized, split_feed, cube=False)
        case(split_feed, root, 'mixed-circular-and-linear', model=str(polarized), spw='0,1',
             standard='manual', fluxdensity=0., mixed_basis=True)


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
    write_image(cube, args.vla_ms)
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
    review_cases(args.vla_ms, args.root, model)


if __name__ == '__main__':
    main()
