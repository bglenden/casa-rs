#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare 2012 Solar System setjy bands, models, and ephemeris directions."""

import argparse
from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory

from casaconfig import config
from casatools import ms, table
import numpy as np

from bandpass_oracle import source_data
from record_casa_returns import make_cal_ms
from setjy_manual_oracle import add_second_window, compare, data, make_input
from setjy_solar2010_oracle import casa, ephemeris_observation, read_scratch_in_both, rust


SCALE = 'Butler-JPL-Horizons 2012'


def dataset(seed, path, body, mjd, frequency, telescope):
    make_input(seed, path, scratch=True)
    ephemeris_observation(path, body, mjd, frequency)
    writer = table()
    assert writer.open(str(path / 'OBSERVATION'), nomodify=False)
    assert writer.putcell('TELESCOPE_NAME', 0, telescope) and writer.done()


def paired(seed, root, body, mjd, frequency, *, telescope='ALMA', cross_read=False):
    with TemporaryDirectory(dir=root, prefix=f'{body}-') as tmp:
        models = {}
        for by_channel, ephemdir in ((True, False), (False, False), (True, True)):
            observed = []
            for writer, implementation in (('casa', casa), ('rust', rust)):
                path = Path(tmp) / f'{writer}-{by_channel}-{ephemdir}.ms'
                dataset(seed, path, body, mjd, frequency, telescope)
                result = implementation.setjy(str(path), field='0', standard=SCALE,
                                              scalebychan=by_channel, useephemdir=ephemdir)
                model = data(path, 'MODEL_DATA')
                if cross_read and not ephemdir:
                    read_scratch_in_both(path, model)
                observed.append((result, model))
            reference, actual = observed
            assert list(actual[0]) == list(reference[0]) == ['0', 'format']
            assert actual[0]['format'] == reference[0]['format']
            assert actual[0]['0']['fieldName'] == reference[0]['0']['fieldName'] == body
            flux = actual[0]['0']['0']['fluxd']
            assert isinstance(flux, list) and len(flux) == 4
            assert flux[0] > 0. and all(value == 0 for value in flux[1:])
            np.testing.assert_allclose(flux, reference[0]['0']['0']['fluxd'],
                                       rtol=2.e-6, atol=1.e-10)
            compare(f'{body} MJD={mjd} frequency={frequency} by-channel={by_channel} '
                    f'useephemdir={ephemdir}', actual[1], reference[1], limit=1.e-4)
            models[by_channel, ephemdir] = actual[1]
        assert not np.allclose(models[True, True], models[True, False])


def rejects(seed, root, body, mjd, frequency, telescope='ALMA'):
    with TemporaryDirectory(dir=root, prefix='unsupported-') as tmp:
        for writer, implementation in (('casa', casa), ('rust', rust)):
            path = Path(tmp) / f'{writer}.ms'
            dataset(seed, path, body, mjd, frequency, telescope)
            implementation.setjy(str(path), field='0', standard='manual', fluxdensity=7.)
            previous = data(path, 'MODEL_DATA')
            try:
                implementation.setjy(str(path), field='0', standard=SCALE)
            except Exception as error:
                assert 'No componentlist is generated' in str(error)
            else:
                raise AssertionError(f'{writer} accepted this unsupported Solar System case')
            np.testing.assert_array_equal(data(path, 'MODEL_DATA'), previous)


def spectral_windows(seed, root):
    with TemporaryDirectory(dir=root, prefix='spectral-windows-') as tmp:
        for by_channel in (True, False):
            outputs = []
            sparse = []
            for writer, implementation in (('casa', casa), ('rust', rust)):
                path = Path(tmp) / f'{writer}-{by_channel}.ms'
                dataset(seed, path, 'Mars', 58000., 100e9, 'ALMA')
                fields, windows = add_second_window(path)
                tb = table()
                assert tb.open(str(path / 'SPECTRAL_WINDOW'), nomodify=False)
                frequencies = tb.getcell('CHAN_FREQ', 1) + 130e9
                assert tb.putcell('CHAN_FREQ', 1, frequencies) and tb.done()
                result = implementation.setjy(str(path), field='0', spw='0:0~2,1:1~3',
                                              standard=SCALE, scalebychan=by_channel)
                assert list(result['0']) == ['0', '1', 'fieldName']
                flux_history_matches(path, [result['0'][str(spw)]['fluxd'][0] for spw in (0, 1)])
                model = data(path, 'MODEL_DATA')
                np.testing.assert_array_equal(model[..., fields == 1], 1 + 0j,
                                              err_msg='unselected field')
                assert np.any(abs(model[0, -1, (fields == 0) & (windows == 1)]) > 1.)
                outputs.append((result, model))
                if by_channel:
                    assert tb.open(str(path / 'FIELD'), nomodify=False)
                    assert tb.putcell('NAME', 1, 'Mars') and tb.done()
                    result = implementation.setjy(str(path), field='1', spw='0,1', standard=SCALE)
                    assert list(result['1']) == ['0', '1', 'fieldName']
                    assert all(isinstance(result['1'][str(spw)]['fluxd'], np.ndarray)
                               for spw in (0, 1))
                    after = data(path, 'MODEL_DATA')
                    np.testing.assert_array_equal(after[..., fields == 0], model[..., fields == 0])
                    sparse.append((result, after))
            reference, actual = outputs
            for window in ('0', '1'):
                flux = actual[0]['0'][window]['fluxd']
                assert isinstance(flux, np.ndarray) and flux.shape == (4,)
                np.testing.assert_allclose(flux, reference[0]['0'][window]['fluxd'], rtol=2.e-6)
            compare(f'multiple SPWs by-channel={by_channel}', actual[1], reference[1])
            if by_channel:
                for window in ('0', '1'):
                    np.testing.assert_allclose(sparse[1][0]['1'][window]['fluxd'],
                                               sparse[0][0]['1'][window]['fluxd'], rtol=2.e-6)
                compare('explicit windows with sparse field coverage', sparse[1][1], sparse[0][1])


def incomplete_spectrum(seed, root):
    with TemporaryDirectory(dir=root, prefix='incomplete-spectrum-') as tmp:
        found = []
        for writer, implementation in (('casa', casa), ('rust', rust)):
            path = Path(tmp) / f'{writer}.ms'
            dataset(seed, path, 'Mars', 58000., 100e9, 'ALMA')
            tb = table()
            assert tb.open(str(path / 'SPECTRAL_WINDOW'), nomodify=False)
            frequencies = tb.getcell('CHAN_FREQ', 0)
            frequencies[-1] = 1600e9
            assert tb.putcell('CHAN_FREQ', 0, frequencies) and tb.done()
            result = implementation.setjy(str(path), field='0', standard=SCALE)
            assert result['0']['0']['fluxd'][0] > 1.
            model = data(path, 'MODEL_DATA')
            row_mask = data(path, 'FIELD_ID') == 0
            assert np.any(abs(model[0, 0, row_mask]) > 1.)
            np.testing.assert_array_equal(model[..., -1, row_mask], 0.)
            found.append(model)
        compare('unsupported last channel retains other channels', found[1], found[0])


def signed_width(root):
    with TemporaryDirectory(dir=root, prefix='signed-width-') as tmp:
        work = Path(tmp)
        seed = work / 'one-channel.ms'
        make_cal_ms(str(seed), channels=1, stokes='XX XY YX YY')
        results = {}
        for label, implementation in (('casa', casa), ('rust', rust)):
            for width in (1e9, -1e9):
                path = work / f'{label}-{width}.ms'
                dataset(seed, path, 'Titan', 60000., 345.9e9, 'ALMA')
                tb = table()
                assert tb.open(str(path / 'SPECTRAL_WINDOW'), nomodify=False)
                assert tb.putcell('CHAN_WIDTH', 0, [width]) and tb.done()
                result = implementation.setjy(str(path), field='0', standard=SCALE)
                results[label, width] = (result['0']['0']['fluxd'][0], data(path, 'MODEL_DATA'))
        expected, model = results['casa', 1e9]
        assert abs(results['casa', -1e9][0] / expected - 1.) > .1
        for width in (1e9, -1e9):
            actual, predicted = results['rust', width]
            np.testing.assert_allclose(actual, expected, rtol=2.e-6)
            compare(f'positive bandwidth with signed channel width {width}', predicted, model)


def attached_field(seed, root):
    ephemeris = (Path(config.measurespath) / 'ephemerides' / 'JPL-Horizons'
                 / 'Mars_55197-59214dUTC_J2000.tab')
    with TemporaryDirectory(dir=root, prefix='attached-field-') as tmp:
        outputs = []
        mjd = 58000.15
        for writer, implementation in (('casa', casa), ('rust', rust)):
            path = Path(tmp) / f'{writer}.ms'
            dataset(seed, path, 'Mars', mjd, 100e9, 'ALMA')
            times = data(path, 'TIME')
            tb = table()
            assert tb.open(str(path / 'OBSERVATION'), nomodify=False)
            assert tb.putcell('TIME_RANGE', 0, [times.min() - 10., times.max() + 10.])
            assert tb.done()
            assert tb.open(str(path / 'FIELD'), nomodify=False)
            assert tb.putcell('TIME', 0, times.min() + 5.)
            for name in ('DELAY_DIR', 'PHASE_DIR', 'REFERENCE_DIR'):
                assert tb.putcell(name, 0, np.array([[1e-6], [-2e-6]]))
            assert tb.done()
            tool = ms()
            assert tool.open(str(path), nomodify=False)
            assert tool.addephemeris(0, str(ephemeris), 'Mars', [0]) and tool.done()
            result = implementation.setjy(str(path), field='0', standard=SCALE)
            outputs.append((result['0']['0']['fluxd'], data(path, 'MODEL_DATA')))
        np.testing.assert_allclose(outputs[1][0], outputs[0][0], rtol=2.e-6)
        compare('attached Mars ephemeris and FIELD reference time', outputs[1][1], outputs[0][1])


def scan_and_time_selection(seed, root):
    with TemporaryDirectory(dir=root, prefix='selection-') as tmp:
        work = Path(tmp)
        native, reference = work / 'native.ms', work / 'reference.ms'
        for path in (native, reference):
            dataset(seed, path, 'Mars', 58000., 100e9, 'ALMA')
            tb = table()
            assert tb.open(str(path), nomodify=False)
            times = tb.getcol('TIME')
            fields = tb.getcol('FIELD_ID')
            relative = times - times.min()
            scans = tb.getcol('SCAN_NUMBER')
            scans[(fields == 0) & (relative > 20.)] = 2
            assert tb.putcol('SCAN_NUMBER', scans) and tb.done()
        selected = (fields == 0) & (scans == 2)
        for module, path in ((casa, reference), (rust, native)):
            assert module.setjy(str(path), field='0', scan='999', standard=SCALE) == {}
        initial = data(native, 'MODEL_DATA')
        casa.setjy(str(reference), field='0', scan='2', selectdata=True, standard=SCALE)
        rust.setjy(str(native), field='0', scan='2', selectdata=True, standard=SCALE)
        actual, expected = data(native, 'MODEL_DATA'), data(reference, 'MODEL_DATA')
        compare('selected scan', actual[..., selected], expected[..., selected])
        np.testing.assert_array_equal(actual[..., ~selected], initial[..., ~selected])
        assert not np.array_equal(expected[..., ~selected], initial[..., ~selected])

        native, reference = work / 'native-time.ms', work / 'reference-time.ms'
        for path in (native, reference):
            dataset(seed, path, 'Mars', 58000., 100e9, 'ALMA')
        selected = (fields == 0) & (relative >= 15.) & (relative <= 45.)
        tb = table()
        assert tb.open(str(reference))
        view = tb.selectrows(np.flatnonzero(selected))
        filtered = work / 'reference-selected.ms'
        copy = view.copy(str(filtered), deep=True, valuecopy=True)
        assert copy.done() and view.done() and tb.done()
        expected = casa.setjy(str(filtered), field='0', standard=SCALE)
        initial = data(native, 'MODEL_DATA')
        actual = rust.setjy(str(native), field='0', selectdata=True, standard=SCALE,
                            timerange='2017/09/04/00:00:15~2017/09/04/00:00:45')
        np.testing.assert_allclose(actual['0']['0']['fluxd'], expected['0']['0']['fluxd'],
                                   rtol=2.e-7)
        model = data(native, 'MODEL_DATA')
        compare('selected model epoch', model[..., selected], data(filtered, 'MODEL_DATA'))
        np.testing.assert_array_equal(model[..., ~selected], initial[..., ~selected])


def observatory_selection(seed, root):
    with TemporaryDirectory(dir=root, prefix='observatory-') as tmp:
        models = []
        for writer, implementation in (('casa', casa), ('rust', rust)):
            path = Path(tmp) / f'{writer}.ms'
            dataset(seed, path, 'Mars', 58000., 100e9, 'Unknown observatory')
            tb = table()
            assert tb.open(str(path / 'OBSERVATION'), nomodify=False)
            record = {name: tb.getcell(name, 0) for name in tb.colnames()
                      if tb.iscelldefined(name, 0)}
            assert tb.addrows(1)
            for name, value in (record | {'TELESCOPE_NAME': 'ALMA'}).items():
                assert tb.putcell(name, 1, value)
            assert tb.done()
            assert tb.open(str(path), nomodify=False)
            field = tb.getcol('FIELD_ID')
            assert tb.putcol('OBSERVATION_ID', (field == 0).astype(np.int32)) and tb.done()
            original = data(path, 'MODEL_DATA')
            try:
                implementation.setjy(str(path), field='0', standard=SCALE)
            except Exception as error:
                assert 'No componentlist is generated' in str(error)
            else:
                raise AssertionError('the default must read OBSERVATION row zero')
            np.testing.assert_array_equal(data(path, 'MODEL_DATA'), original)
            result = implementation.setjy(str(path), field='0', standard=SCALE,
                                          observation=1 if writer == 'casa' else '1')
            assert result['0']['0']['fluxd'][0] > 1.
            models.append(data(path, 'MODEL_DATA'))
        compare('explicit observatory selection', models[1], models[0])


def flux_history_matches(path, expected):
    history = path / 'HISTORY'
    messages = data(history, 'MESSAGE')
    is_flux = ((data(history, 'APPLICATION') == 'setjy') & (data(history, 'ORIGIN') == 'setjy')
               & (np.char.find(messages, 'Flux:[I=') >= 0))
    fluxes = [float(text.split('Flux:[I=', 1)[1].split(',', 1)[0]) for text in messages[is_flux]]
    np.testing.assert_allclose(fluxes, expected, rtol=2.e-6)
    assert np.any(np.char.find(messages, 'taskname') >= 0)


def partial_history(seed, root):
    with TemporaryDirectory(dir=root, prefix='partial-history-') as tmp:
        work = Path(tmp).resolve()
        models, logged_flux = [], []
        with chdir(work):
            for writer, implementation in (('casa', casa), ('rust', rust)):
                path = work / f'{writer}.ms'
                dataset(seed.resolve(), path, 'Mars', 58000., 100e9, 'ALMA')
                try:
                    implementation.setjy(str(path), field='0,1', standard=SCALE)
                except Exception as error:
                    assert 'No componentlist is generated' in str(error)
                else:
                    raise AssertionError('the second field should fail to calibrate')
                model = data(path, 'MODEL_DATA')
                field = data(path, 'FIELD_ID')
                assert np.any(abs(model[0, :, field == 0]) > 1.)
                np.testing.assert_array_equal(model[..., field == 1], 1 + 0j)
                history = path / 'HISTORY'
                messages = data(history, 'MESSAGE')
                flux = [float(text.split('Flux:[I=', 1)[1].split(',', 1)[0])
                        for text in messages if 'Mars:' in text and 'Flux:[I=' in text]
                assert len(flux) == 1 and flux[0] > 1.
                flux_history_matches(path, flux)
                logged_flux.append(flux)
                models.append(model)
        np.testing.assert_allclose(logged_flux[1], logged_flux[0], rtol=2.e-6)
        compare('first field persists after later field failure', models[1], models[0])


def solve_with_both_writers(seed, root, standard, epoch):
    with TemporaryDirectory(dir=root, prefix='solve-') as tmp:
        expected = {}
        for writer, producer in (('casa', casa), ('rust', rust)):
            path = Path(tmp) / f'{writer}.ms'
            dataset(seed, path, 'Mars', epoch, 100e9, 'ALMA')
            producer.setjy(str(path), field='0', standard=standard)
            for reader, solver in (('casa', casa), ('rust', rust)):
                for task in ('gaincal', 'bandpass'):
                    destination = Path(tmp) / f'{writer}-{reader}-{task}.cal'
                    options = {'calmode': 'ap'} if task == 'gaincal' else {}
                    getattr(solver, task)(str(path), str(destination), field='0', spw='0',
                                          refant='A0', minsnr=0., solint='inf', **options)
                    flags = data(destination, 'FLAG')
                    values = data(destination, 'CPARAM')
                    assert np.any(~flags)
                    reference_flags, reference_values = expected.setdefault(task, (flags, values))
                    np.testing.assert_array_equal(flags, reference_flags)
                    compare(f'{standard} {writer} writer {reader} {task}',
                            values[~flags], reference_values[~flags])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    seed = args.root / 'base.ms'
    source_data(seed, stokes='XX XY YX YY')
    paired(seed, args.root, 'Mars', 58000., 100e9, cross_read=True)
    paired(seed, args.root, 'Jupiter', 60000., 100e9)
    paired(seed, args.root, 'Ceres', 56800., 225e9)
    paired(seed, args.root, 'Ceres', 56000., 100e9)
    paired(seed, args.root, 'Venus', 60000.125, 100e9, telescope='VLA')
    paired(seed, args.root, 'Titan', 60000., 100e9)
    spectral_windows(seed, args.root)
    incomplete_spectrum(seed, args.root)
    signed_width(args.root)
    attached_field(seed, args.root)
    scan_and_time_selection(seed, args.root)
    observatory_selection(seed, args.root)
    partial_history(seed, args.root)
    rejects(seed, args.root, 'Aldebaran', 58000., 100e9)
    rejects(seed, args.root, 'Mars', 58000., 1.4e9)
    rejects(seed, args.root, 'Mars', 58000., 100e9, telescope='Unknown observatory')
    solve_with_both_writers(seed, args.root, 'Butler-JPL-Horizons 2010', 55000.)
    solve_with_both_writers(seed, args.root, SCALE, 58000.)


if __name__ == '__main__':
    main()
