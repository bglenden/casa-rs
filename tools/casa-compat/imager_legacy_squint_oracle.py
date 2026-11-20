#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare fixed legacy imager voltage-pattern squint with pinned CASA."""

import argparse
from pathlib import Path

from casars.tools import imager, ms
from casatools import image, imager as casa_imager, ms as casa_ms
import numpy as np

from imager_legacy_pbw_oracle import corrected_or_data, data, make_input, recipe, relative_rms


def predict(factory, dataset, model, squint, *, stokes='IQUV', scratch=True, angle='360deg'):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=0, spw='0:0')
    assert tool.defineimage(nx=256, ny=256, cellx='15arcsec', celly='15arcsec',
                            phasecenter=0, stokes=stokes, outframe='TOPO')
    assert tool.setoptions(ftmachine='ft', numthreads=1)
    assert tool.setvp(dovp=True, dosquint=squint, parangleinc=angle, skyposthreshold='180deg')
    assert tool.ft(str(model)) and tool.done()


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
    assert tool.msselect({'field': '0', 'spw': '0:0'})
    assert tool.uvsub() and tool.done()
    return prior - data(dataset, 'CORRECTED_DATA')


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
        target = args.work / f'{engine}-saved.ms'
        make_input(args.dataset, target, scratch=False)
        predict(factory, target, args.model, True, scratch=False)
        saved[engine] = target
    for stokes in ('IQUV', 'I'):
        for squint in (False, True):
            expected, actual = (result[engine, stokes, squint] for engine in ('pinned', 'native'))
            assert np.max(np.abs(expected[:, 0, :])) > 1.
            np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
            compare(f'{stokes}, squint={squint}', actual[:, 0, :], expected[:, 0, :])
    for engine in ('pinned', 'native'):
        radial, shifted = (result[engine, 'IQUV', squint][:, 0, :] for squint in (False, True))
        assert relative_rms(shifted[[0, 3]], radial[[0, 3]]) > 5.e-4
        np.testing.assert_allclose(shifted[[1, 2]], radial[[1, 2]], atol=2.e-7, rtol=2.e-6)
        np.testing.assert_array_equal(result[engine, 'I', True], result[engine, 'I', False])
    compare('saved Fourier grid', fourier_grid(saved['native']), fourier_grid(saved['pinned']))
    expected = result['pinned', 'IQUV', True]
    for writer, serialized in saved.items():
        for reader, factory in (('pinned', casa_ms), ('native', ms)):
            target = args.work / f'{writer}-to-{reader}.ms'
            make_input(serialized, target, scratch=False)
            actual = consume(factory, target)
            assert not np.any(actual[:, 1:, :])
            compare(f'saved {writer} to {reader}', actual[:, 0, :], expected[:, 0, :])


if __name__ == '__main__':
    main()
