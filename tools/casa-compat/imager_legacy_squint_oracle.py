#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare fixed legacy imager voltage-pattern squint with pinned CASA."""

import argparse
from pathlib import Path
import shutil

from casars.tools import imager, ms
from casatools import image, imager as casa_imager, ms as casa_ms, table
import numpy as np

from imager_legacy_pbw_oracle import corrected_or_data, data, make_input, recipe, relative_rms


def predict(factory, dataset, model, squint, *, stokes='IQUV', scratch=True, angle='360deg',
            accepts=True):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=0, spw='0:0')
    assert tool.defineimage(nx=256, ny=256, cellx='15arcsec', celly='15arcsec',
                            phasecenter=0, stokes=stokes, outframe='TOPO')
    assert tool.setoptions(ftmachine='ft', numthreads=1)
    assert tool.setvp(dovp=True, dosquint=squint, parangleinc=angle, skyposthreshold='180deg')
    assert tool.ft(str(model)) is accepts
    assert tool.done()


def compare(label, actual, expected, *, limit=1.e-5):
    error = relative_rms(actual, expected)
    print(label, 'NRMS', error, flush=True)
    assert error < limit, (label, error)


def fourier_grid(dataset):
    record = recipe(dataset)
    assert record['name'] == 'GridFT'
    reader = image()
    assert reader.open(record['diskimage'])
    values = reader.getchunk()
    assert np.iscomplexobj(values) and reader.done()
    return values


def consume(factory, dataset):
    prior = corrected_or_data(dataset)
    tool = factory()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.uvsub() and tool.done()
    return prior - data(dataset, 'CORRECTED_DATA')


def receptor_control(root, source, model, baseline):
    results = []
    for engine, factory in (('pinned', casa_imager), ('native', imager)):
        dataset = root / f'{engine}-rotated-receptors.ms'
        make_input(source, dataset, scratch=True)
        feed = table()
        assert feed.open(str(dataset / 'FEED'), nomodify=False)
        angles = feed.getcol('RECEPTOR_ANGLE')
        angles[0, :] += .4
        assert feed.putcol('RECEPTOR_ANGLE', angles) and feed.done()
        predict(factory, dataset, model, True)
        results.append(data(dataset, 'MODEL_DATA')[:, 0, :])
    expected, actual = results
    assert relative_rms(expected, baseline) > 1.e-4
    compare('rotated feed receptors', actual, expected)


def projected_radius_control(root, source, original):
    model = root / 'shifted-tangent.im'
    shutil.copytree(original, model)
    tool = image()
    assert tool.open(str(model))
    coords = tool.coordsys()
    centre = coords.toworld([128, 128, 0, 0])
    tangent = centre['numeric'][:2].copy()
    tangent[1] -= np.radians(30.)
    assert coords.setreferencevalue(tangent, type='direction')
    displaced = coords.topixel(centre)['numeric'][:2]
    reference_pixel = coords.referencepixel('direction')['numeric']
    assert coords.setreferencepixel(reference_pixel + 128. - displaced, type='direction')
    projected = coords.toworld([128, 188, 0, 0])['numeric']
    # The brightness sample is 15 arcmin away in the projected grid.
    # Its sky separation is over 17 arcmin because the tangent is 30 degrees away.
    assert np.degrees(projected[1] - centre['numeric'][1]) * 60. > 17.
    assert tool.setcoordsys(coords.torecord()) and coords.done()
    pixels = np.zeros(tool.shape(), np.float32)
    pixels[128, 188, :, 0] = [1.3, .2, .15, -.1]
    assert tool.putchunk(pixels) and tool.done()
    grids = []
    for engine, factory in (('pinned', casa_imager), ('native', imager)):
        dataset = root / f'{engine}-shifted-tangent.ms'
        make_input(source, dataset, scratch=False)
        predict(factory, dataset, model, True, scratch=False)
        grids.append(fourier_grid(dataset))
    expected, actual = grids
    assert np.max(np.abs(expected)) > .1
    compare('off-tangent projected beam', actual, expected)


def refusal_controls(root, source, model):
    for label in ('linear-feed', 'multiple-arrays'):
        dataset = root / f'{label}.ms'
        make_input(source, dataset, scratch=True)
        writer = table()
        if label == 'linear-feed':
            assert writer.open(str(dataset), nomodify=False)
            for column in ('FLAG', 'WEIGHT', 'SIGMA', 'DATA', 'WEIGHT_SPECTRUM', 'MODEL_DATA'):
                values = writer.getcol(column)[[0, 0, 1, 1], ...]
                descriptor = writer.getcoldesc(column)
                descriptor.update(dataManagerType='StandardStMan', dataManagerGroup='LinearControl')
                assert writer.removecols(column) and writer.addcols({column: descriptor})
                assert writer.putcol(column, values)
            assert writer.done()
            assert writer.open(str(dataset / 'POLARIZATION'), nomodify=False)
            assert writer.putcell('NUM_CORR', 0, 4)
            assert writer.putcell('CORR_TYPE', 0, np.array([9, 10, 11, 12], dtype=np.int32))
            assert writer.putcell('CORR_PRODUCT', 0, np.array([[0, 0, 1, 1], [0, 1, 0, 1]], np.int32))
        else:
            assert writer.open(str(dataset), nomodify=False)
            arrays, times = writer.getcol('ARRAY_ID'), writer.getcol('TIME')
            arrays[times == times.max()] = 1
            assert writer.putcol('ARRAY_ID', arrays)
        assert writer.done()
        predict(imager, dataset, model, False)
        prior = data(dataset, 'MODEL_DATA')
        predict(imager, dataset, model, True, accepts=False)
        np.testing.assert_array_equal(data(dataset, 'MODEL_DATA'), prior)
        print(label, 'refusal preserves scratch visibilities', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--stokes-i-model', type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    result = {}
    saved = {}
    for engine, factory in (('pinned', casa_imager), ('native', imager)):
        for squint in (False, True):
            for stokes, model in (('IQUV', args.model), ('I', args.stokes_i_model)):
                target = args.work / f'{engine}-{stokes}-{squint}.ms'
                make_input(args.dataset, target, scratch=True)
                predict(factory, target, model, squint, stokes=stokes)
                result[engine, stokes, squint] = data(target, 'MODEL_DATA')
            target = args.work / f'{engine}-saved-{squint}.ms'
            make_input(args.dataset, target, scratch=False)
            predict(factory, target, args.model, squint, scratch=False)
            saved[engine, squint] = target
    for stokes in ('IQUV', 'I'):
        for squint in (False, True):
            expected, actual = (result[engine, stokes, squint] for engine in ('pinned', 'native'))
            assert np.max(np.abs(expected[:, 0, :])) > 1.
            np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
            compare(f'{stokes}, squint={squint}', actual[:, 0, :], expected[:, 0, :])
    for engine in ('pinned', 'native'):
        radial, shifted = (result[engine, 'IQUV', squint][:, 0, :] for squint in (False, True))
        assert relative_rms(shifted, radial) > 5.e-4
        np.testing.assert_array_equal(result[engine, 'I', True], result[engine, 'I', False])
        radial, shifted = (fourier_grid(saved[engine, squint]) for squint in (False, True))
        assert relative_rms(shifted[:, :, [0, 3]], radial[:, :, [0, 3]]) > 5.e-4
        np.testing.assert_allclose(shifted[:, :, [1, 2]], radial[:, :, [1, 2]],
                                   atol=2.e-7, rtol=2.e-6)
    for squint in (False, True):
        compare(f'saved Fourier grid, squint={squint}', fourier_grid(saved['native', squint]),
                fourier_grid(saved['pinned', squint]))
    expected = result['pinned', 'IQUV', True]
    for (writer, squint), serialized in saved.items():
        if not squint:
            continue
        for reader, factory in (('pinned', casa_ms), ('native', ms)):
            target = args.work / f'{writer}-to-{reader}.ms'
            make_input(serialized, target, scratch=False)
            actual = consume(factory, target)
            assert not np.any(actual[:, 1:, :])
            compare(f'saved {writer} to {reader}', actual[:, 0, :], expected[:, 0, :])
    receptor_control(args.work, args.dataset, args.model, expected[:, 0, :])
    projected_radius_control(args.work, args.dataset, args.model)
    refusal_controls(args.work, args.dataset, args.model)


if __name__ == '__main__':
    main()
