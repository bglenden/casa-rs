#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare resolved Butler–JPL-Horizons 2010 disk predictions with CASA."""

import argparse
from contextlib import chdir
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

from casaconfig import config
import casatasks as casa
from casatools import imager as casa_imager, ms, table
from casars import tasks as rust
from casars.tools import imager as rust_imager
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


def paired(seed, root, body, epoch, *, frequency=None, by_channel=True, missing=False,
           direction=None, attached_ephemeris=None, time_frame='UTC', check_readers=False,
           check_solvers=False, direct_tool=False, **options):
    with TemporaryDirectory(dir=root, prefix=f'{body}-') as tmp:
        work = Path(tmp)
        answers = []
        solutions = {}
        for label, module in (('casa', casa), ('rust', rust)):
            path = work / f'{label}.ms'
            make_input(seed, path, scratch=True)
            ephemeris_observation(path, body, epoch, frequency)
            if time_frame != 'UTC':
                writer = table()
                assert writer.open(str(path), nomodify=False)
                for name in ('TIME', 'TIME_CENTROID'):
                    info = dict(writer.getcolkeyword(name, 'MEASINFO'), Ref=time_frame)
                    assert writer.putcolkeyword(name, 'MEASINFO', info)
                assert writer.done()
            if direction is not None:
                writer = table()
                assert writer.open(str(path / 'FIELD'), nomodify=False)
                for name in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
                    angles = writer.getcell(name, 0)
                    angles[:, 0] = direction
                    assert writer.putcell(name, 0, angles)
                assert writer.done()
            if attached_ephemeris is not None:
                writer = ms()
                assert writer.open(str(path), nomodify=False)
                assert writer.addephemeris(0, str(attached_ephemeris), body, [0])
                assert writer.done()
            if missing:
                module.setjy(str(path), field='0', standard='manual', fluxdensity=7.)
                saved = data(path, 'MODEL_DATA')
            if direct_tool:
                tool = (casa_imager if label == 'casa' else rust_imager)()
                assert tool.open(str(path), usescratch=True)
                result = tool.setjy(field='0', standard=SCALE, scalebychan=by_channel, **options)
                assert tool.close()
            else:
                result = module.setjy(str(path), field='0', standard=SCALE, scalebychan=by_channel,
                                      **options)
            model = data(path, 'MODEL_DATA')
            if missing:
                assert result == {}
                np.testing.assert_array_equal(model, saved)
            if options.get('fluxdensity') == 0.:
                np.testing.assert_array_equal(model[..., data(path, 'FIELD_ID') == 0], 0.)
            if check_solvers:
                writer = table()
                assert writer.open(str(path / 'FIELD'), nomodify=False)
                assert writer.putcell('TIME', 0, epoch * 86400.) and writer.done()
                compare_solves(path, work, label, solutions)
            if check_readers or (body == 'Mars' and epoch == 55000. and by_channel and not options
                                 and direction is None and time_frame == 'UTC'):
                read_scratch_in_both(path, model)
            answers.append((result, model))
        reference, native = answers
        if missing:
            return
    compare_returns(native[0], reference[0], rtol=2e-7)
    compare(f'{body} {epoch} {frequency} by-channel={by_channel} direction={direction}',
            native[1], reference[1])
    flux = native[0]['0']['0']['fluxd']
    assert np.all(np.isfinite(flux)) and flux[0] >= 0. and not np.any(flux[1:])
    if options.get('fluxdensity') == 0.:
        assert flux[0] == 0.
    elif options.get('model'):
        assert flux[0] == 1.
    else:
        assert flux[0] > 0. and not np.array_equal(native[1], np.ones_like(native[1]))
    return flux[0]


def compare_solves(path, work, writer, references):
    for label, reader in (('casa', casa), ('rust', rust)):
        for task in ('gaincal', 'bandpass'):
            destination = work / f'{writer}-{label}-{task}.cal'
            options = {'calmode': 'ap'} if task == 'gaincal' else {}
            getattr(reader, task)(str(path), str(destination), field='0', spw='0',
                                  refant='A0', minsnr=0., solint='inf', **options)
            flags = data(destination, 'FLAG')
            values = data(destination, 'CPARAM')
            assert np.any(~flags)
            expected_flags, expected = references.setdefault(task, (flags, values))
            np.testing.assert_array_equal(flags, expected_flags)
            compare(f'legacy APP {writer} writer {label} {task}', values[~flags], expected[~flags])


def read_scratch_in_both(path, model):
    for module in (casa, rust):
        writer = table()
        assert writer.open(str(path), nomodify=False)
        assert writer.putcol('CORRECTED_DATA', np.zeros_like(model)) and writer.done()
        module.uvsub(str(path), reverse=True)
        np.testing.assert_array_equal(data(path, 'CORRECTED_DATA'), model)


def refuses_invalid_legacy_grid(seed, root, apparent):
    with TemporaryDirectory(dir=root, prefix='invalid-legacy-grid-') as tmp:
        path = Path(tmp) / 'invalid.ms'
        make_input(seed, path, scratch=True)
        ephemeris_observation(path, 'Uranus', 55000.12345, 100.e9)
        attachment = ms()
        assert attachment.open(str(path), nomodify=False)
        assert attachment.addephemeris(0, str(apparent), 'Uranus', [0])
        assert attachment.done()
        ephemeris = next((path / 'FIELD').glob('EPHEM0_*.tab'))
        writer = table()
        assert writer.open(str(ephemeris), nomodify=False)
        assert writer.putkeyword('dMJD', 0.) and writer.done()
        original = data(path, 'MODEL_DATA')
        try:
            rust.setjy(str(path), field='0', standard=SCALE)
        except RuntimeError as error:
            assert 'invalid MJD sampling grid' in str(error), error
        else:
            raise AssertionError('a zero ephemeris step was accepted')
        np.testing.assert_array_equal(data(path, 'MODEL_DATA'), original)


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
    paired(seed, args.root, 'Mars', 55000., direction=(0., 0.))
    paired(seed, args.root, 'Mars', 55000., direction=(0., np.pi / 2.))
    paired(seed, args.root, 'Mars', 55000.1, time_frame='TAI')
    model = args.root / 'resolved.im'
    write_image(model, seed, cube=False)
    paired(seed, args.root, 'Mars', 55000., model=str(model))
    ephemerides = Path(config.measurespath) / 'ephemerides' / 'JPL-Horizons'
    ephemeris = args.root / 'Uranus_54708-55437dUTC.tab'
    copytree(ephemerides / ephemeris.name, ephemeris)
    writer = table()
    assert writer.open(str(ephemeris), nomodify=False)
    assert writer.putkeyword('posrefsys', 'ICRF/J2000.0') and writer.done()
    paired(seed, args.root, 'Uranus', 55000., direction=(1e-6, -2e-6),
           attached_ephemeris=ephemeris)
    # Pre-ICRS Horizons tables use apparent directions when posrefsys is
    # absent. Attach the unmodified table and retain a nonzero FIELD offset.
    apparent = ephemerides / ephemeris.name
    assert writer.open(str(apparent))
    assert 'posrefsys' not in writer.keywordnames() and writer.done()
    paired(seed, args.root, 'Uranus', 55000.12345, frequency=100.e9,
           direction=(1.e-6, -2.e-6), attached_ephemeris=apparent, check_readers=True,
           check_solvers=True)
    paired(seed, args.root, 'Uranus', 55000.37, frequency=355.e9,
           direction=(-1.3e-6, 2.5e-6), attached_ephemeris=apparent, time_frame='TAI')
    paired(seed, args.root, 'Uranus', 55000.12345, frequency=100.e9,
           direction=(1.e-6, -2.e-6), attached_ephemeris=apparent, direct_tool=True,
           reffreq='100GHz', fluxdensity=-1.)
    refuses_invalid_legacy_grid(seed, args.root, apparent)
    custom = args.root.resolve() / 'custom-ephemeris'
    custom.mkdir()
    override = custom / 'Mars_54708-55437dUTC.tab'
    copytree(ephemerides / override.name, override)
    assert writer.open(str(override), nomodify=False)
    assert writer.removecols(['RA', 'DEC']) and writer.done()
    with chdir(custom):
        paired(seed.resolve(), args.root.resolve(), 'Mars', 55000.)
    overlapping = custom / 'Mars_54600-55460dUTC.tab'
    copytree(ephemerides / override.name, overlapping)
    for path, temperature in ((override, 30.), (overlapping, 90.)):
        assert writer.open(str(path), nomodify=False)
        assert writer.putkeyword('T_mean', dict(value=temperature, unit='K')) and writer.done()
    with chdir(custom):
        assert paired(seed.resolve(), args.root.resolve(), 'Mars', 55000.) < 15.
    refuses_virtual(seed, args.root)


if __name__ == '__main__':
    main()
