#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare resolved Butler–JPL-Horizons 2010 disk predictions with CASA."""

import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks as casa
from casatools import table
from casars import tasks as rust
import numpy as np

from bandpass_oracle import source_data
from setjy_image_oracle import write_image
from setjy_manual_oracle import compare, compare_returns, data, make_input


SCALE = 'Butler-JPL-Horizons 2010'


def ephemeris_observation(path, body, epoch, frequency):
    writer = table()
    assert writer.open(str(path / 'FIELD'), nomodify=False)
    assert writer.putcell('NAME', 0, body) and writer.done()

    assert writer.open(str(path), nomodify=False)
    delta = epoch * 86400. - writer.getcell('TIME', 0)
    for column in ('TIME', 'TIME_CENTROID'):
        assert writer.putcol(column, writer.getcol(column) + delta)
    assert writer.done()

    if frequency is not None:
        assert writer.open(str(path / 'SPECTRAL_WINDOW'), nomodify=False)
        channels = writer.getcell('CHAN_FREQ', 0)
        offset = frequency - np.mean(channels)
        assert writer.putcell('CHAN_FREQ', 0, channels + offset)
        assert writer.putcell('REF_FREQUENCY', 0, writer.getcell('REF_FREQUENCY', 0) + offset)
        assert writer.done()


def paired(seed, root, body, epoch, *, frequency=None, by_channel=True, missing=False, **options):
    with TemporaryDirectory(dir=root, prefix=f'{body}-') as tmp:
        work = Path(tmp)
        answers = []
        for label, module in (('casa', casa), ('rust', rust)):
            path = work / f'{label}.ms'
            make_input(seed, path, scratch=True)
            ephemeris_observation(path, body, epoch, frequency)
            if missing:
                module.setjy(str(path), field='0', standard='manual', fluxdensity=7.)
                saved = data(path, 'MODEL_DATA')
            result = module.setjy(str(path), field='0', standard=SCALE, scalebychan=by_channel,
                                  **options)
            model = data(path, 'MODEL_DATA')
            if missing:
                assert result == {}
                np.testing.assert_array_equal(model, saved)
            if options.get('fluxdensity') == 0.:
                np.testing.assert_array_equal(model[..., data(path, 'FIELD_ID') == 0], 0.)
            if body == 'Mars' and epoch == 55000. and by_channel and not options:
                read_scratch_in_both(path, model)
            answers.append((result, model))
        reference, native = answers
        if missing:
            return
        compare_returns(native[0], reference[0], rtol=2e-7)
        compare(f'{body} {epoch} {frequency} by-channel={by_channel}', native[1], reference[1])
        flux = native[0]['0']['0']['fluxd']
    assert np.all(np.isfinite(flux)) and flux[0] >= 0. and not np.any(flux[1:])
    if options.get('fluxdensity') == 0.:
        assert flux[0] == 0.
    elif options.get('model'):
        assert flux[0] == 1.
    else:
        assert flux[0] > 0. and not np.array_equal(native[1], np.ones_like(native[1]))


def read_scratch_in_both(path, model):
    for module in (casa, rust):
        writer = table()
        assert writer.open(str(path), nomodify=False)
        assert writer.putcol('CORRECTED_DATA', np.zeros_like(model)) and writer.done()
        module.uvsub(str(path), reverse=True)
        np.testing.assert_array_equal(data(path, 'CORRECTED_DATA'), model)


def refuses_virtual(seed, root):
    with TemporaryDirectory(dir=root) as tmp:
        for label, module in (('casa', casa), ('rust', rust)):
            path = Path(tmp) / f'{label}.ms'
            make_input(seed, path, scratch=False)
            ephemeris_observation(path, 'Mars', 55000., None)
            try:
                module.setjy(str(path), field='0', standard=SCALE, usescratch=False)
            except RuntimeError as error:
                assert 'Virtual model can not be used for an ephemeris object calibrator' in str(error)
            else:
                raise AssertionError('a 2010 Solar System virtual model was accepted')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    seed = args.root / 'base.ms'
    source_data(seed, stokes='XX XY YX YY')
    for body, epoch in (
        ('Mars', 55000.), ('Jupiter', 55000.), ('Ceres', 55000.), ('Titan', 55000.),
        ('Vesta', 55000.), ('Venus', 55900.), ('Uranus', 55000.), ('Neptune', 55000.),
    ):
        for by_channel in (True, False):
            paired(seed, args.root, body, epoch, by_channel=by_channel)
    for body, epoch, hz in (
        ('Jupiter', 55000.25, 1.3e9), ('Jupiter', 55000.6, 45.5e9),
        ('Uranus', 55000.4, 355e9), ('Neptune', 55000.9, 16.2e9),
        ('Venus', 55900.3, 33.1e9), ('Venus', 55900.6, 385e9),
    ):
        paired(seed, args.root, body, epoch, frequency=hz)
    paired(seed, args.root, 'Mars', 60000., missing=True)
    paired(seed, args.root, 'unrecognized', 55000., missing=True)
    paired(seed, args.root, 'Mars', 55000., fluxdensity=0.)
    paired(seed, args.root, 'Mars', 55000., fluxdensity=40.)
    model = args.root / 'resolved.im'
    write_image(model, seed, cube=False)
    paired(seed, args.root, 'Mars', 55000., model=str(model))
    refuses_virtual(seed, args.root)


if __name__ == '__main__':
    main()
