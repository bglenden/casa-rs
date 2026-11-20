#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare live and persisted legacy MosaicFT models through public Python."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import image, imager, measures, quanta, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_prediction_options_oracle import configure_data, coordinates, samples, write_model


def fixture(root):
    path = root / 'source.ms'
    configure_data(path)
    stored = table()
    assert stored.open(str(path / 'SPECTRAL_WINDOW'))
    frequency = stored.getcell('CHAN_FREQ', 0)[0]
    assert stored.close()
    ra, dec = coordinates(path)
    assert stored.open(str(path / 'FIELD'), nomodify=False)
    for column in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
        direction = stored.getcell(column, 0)
        direction[:, 0] += [4e-5, -3e-5]
        stored.putcell(column, 1, direction)
    assert stored.close()
    assert stored.open(str(path / 'ANTENNA'), nomodify=False)
    diameters = stored.getcol('DISH_DIAMETER')
    diameters[1::2] = 7.
    stored.putcol('DISH_DIAMETER', diameters)
    assert stored.close()
    assert stored.open(str(path), nomodify=False)
    fields = stored.getcol('FIELD_ID')
    first = stored.getcol('ANTENNA1')
    second = stored.getcol('ANTENNA2')
    for field in (0, 1):
        row = int(np.flatnonzero((fields == field) & (first != second))[2])
        stored.putcell('ANTENNA2', row, int(first[row]))
        stored.putcell('UVW', row, np.zeros(3))
    assert stored.close()
    offset_path = root / 'offset-pointings.ms'
    shutil.copytree(path, offset_path)
    offset_pointings(offset_path)
    assert stored.open(str(path / 'POINTING'), nomodify=False)
    if stored.nrows():
        stored.removerows(list(range(stored.nrows())))
    assert stored.close()

    model = root / 'mosaic.im'
    pixels = np.zeros((320, 320, 1, 1), np.float32)
    pixels[160, 160, 0, 0] = 1.7
    pixels[191, 144, 0, 0] = .9
    tool = image()
    assert tool.fromarray(outfile=str(model), pixels=pixels)
    coords = tool.coordsys()
    assert coords.setunits(['rad', 'rad', '', 'Hz'])
    assert coords.setreferencevalue(value=f'{ra}rad {dec}rad', type='direction')
    assert coords.setincrement(value='-0.3arcsec 0.3arcsec', type='direction')
    assert coords.setreferencepixel(value=[160., 160.], type='direction')
    assert coords.setreferencecode('TOPO', type='spectral', adjust=False)
    assert coords.setreferencevalue(value=f'{frequency}Hz', type='spectral')
    assert coords.setincrement(value='30MHz', type='spectral')
    assert tool.setcoordsys(coords.torecord()) and coords.done()
    assert tool.setbrightnessunit('Jy/pixel') and tool.done()
    return path, offset_path, model


def offset_pointings(path):
    stored = table()
    assert stored.open(str(path))
    times = stored.getcol('TIME')
    fields = stored.getcol('FIELD_ID')
    field_times = [np.unique(times[fields == field]) for field in (0, 1)]
    assert stored.close()
    assert stored.open(str(path / 'FIELD'))
    directions = [stored.getcell('PHASE_DIR', field) for field in (0, 1)]
    assert stored.close()
    assert stored.open(str(path / 'POINTING'), nomodify=False)
    assert stored.nrows() > 0
    for row, epoch in enumerate(stored.getcol('TIME')):
        field = min((0, 1), key=lambda field: np.min(np.abs(field_times[field] - epoch)))
        direction = directions[field].copy()
        direction[:, 0] += [2.25e-5, -3.1e-5]
        stored.putcell('DIRECTION', row, direction)
    assert stored.close()


def compare(actual, expected):
    assert len(actual) == len(expected)
    errors = [relative_rms(value, reference) for value, reference in zip(actual, expected, strict=True)]
    assert max(errors) < 1e-3, errors
    return {'complex_samples': sum(value.size for value in actual), 'normalized_rms': max(errors)}


def predict(source, target, factory, model, scratch, beam, *, size=320, cell='0.3arcsec'):
    shutil.copytree(source, target)
    if not scratch:
        stored = table()
        assert stored.open(str(target), nomodify=False)
        if 'MODEL_DATA' in stored.colnames():
            stored.removecols(['MODEL_DATA'])
        assert stored.close()
    tool = factory()
    assert tool.open(str(target), usescratch=scratch)
    assert tool.selectvis(field='0,1')
    assert tool.defineimage(nx=size, ny=size, cellx=cell, celly=cell,
                            phasecenter=0, outframe='TOPO')
    assert tool.setoptions(ftmachine='mosaic', numthreads=1)
    if beam:
        assert tool.setvp(dovp=True)
    assert tool.ft(str(model)) and tool.done(), (target, scratch, beam)
    if not scratch:
        stored = table()
        assert stored.open(str(target / 'SOURCE'))
        row = stored.getkeyword(stored.getkeyword('definedmodel_field_0'))
        record = stored.getcell('SOURCE_MODEL', row)['ft_0']['container']
        assert record['name'] == 'MosaicFT' and record['pbconvfunc']['name'] == 'HetArrayConvFunc'
        assert stored.close()


def consume(source, destination, reader):
    from casars.tools import ms

    shutil.copytree(source, destination)
    before = [samples(destination, 'CORRECTED_DATA', field) for field in (0, 1)]
    if reader == 'casa':
        assert casatasks.uvsub(str(destination)) is None
    else:
        tool = ms()
        assert tool.open(str(destination), nomodify=False) and tool.uvsub() and tool.done()
    return [value - samples(destination, 'CORRECTED_DATA', field) for field, value in enumerate(before)]


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, offset_path, model = fixture(root)
    baseline, control = predictions(root, source, model, (False, True), native)
    offset = root / 'offset'
    offset.mkdir()
    shifted, observed = predictions(offset, offset_path, model, (False,), native)
    changes = [relative_rms(after, before) for after, before in zip(observed, control, strict=True)]
    assert min(changes) > 0.01, changes
    return {'standard': baseline, 'tracked_pointing': shifted,
            'pointing_offset_relative_rms': changes,
            'small_crop': small_crop(root, source, native),
            'metadata': metadata_cases(root / 'metadata', source, offset_path, model, native, control),
            'rejected_replacement_preserved_samples': rejected_replacement(root, model, native),
            'wide_cube': wide_cube(root, source, model, native)}


def small_crop(root, source, native):
    stored = table()
    assert stored.open(str(source / 'SPECTRAL_WINDOW'))
    frequency = stored.getcell('CHAN_FREQ', 0)[0]
    assert stored.close()
    model = root / 'small-crop.im'
    write_model(model, coordinates(source), frequency, 1.7, cell=2.)
    evidence = {}
    for engine, factory in (('casa', imager), ('native', native)):
        scratch = root / f'small-crop-{engine}.ms'
        predict(source, scratch, factory, model, True, False, size=32, cell='2arcsec')
        values = [samples(scratch, field=field) for field in (0, 1)]
        assert all(np.count_nonzero(value) == 0 for value in values), engine
        evidence[engine] = sum(value.size for value in values)
    return evidence


def predictions(root, source, model, beams, native):
    results = {}
    reference = None
    for beam in beams:
        for engine, factory in (('casa', imager), ('native', native)):
            path = root / f'scratch-{engine}-{beam}.ms'
            predict(source, path, factory, model, True, beam)
            values = [samples(path, field=field) for field in (0, 1)]
            stored = table()
            assert stored.open(str(path))
            autos = np.flatnonzero(stored.getcol('ANTENNA1') == stored.getcol('ANTENNA2'))
            assert autos.size >= 2
            assert all(np.max(np.abs(stored.getcell('MODEL_DATA', int(row)))) > 1. for row in autos)
            assert stored.close()
            if reference is None:
                assert all(np.max(np.abs(field)) > 1. for field in values)
                reference = values
            results[f'scratch-{engine}-{beam}'] = compare(values, reference)
    for writer, factory in (('casa', imager), ('native', native)):
        path = root / f'virtual-{writer}.ms'
        predict(source, path, factory, model, False, False)
        for reader in ('casa', 'native'):
            values = consume(path, root / f'virtual-{writer}-{reader}.ms', reader)
            results[f'virtual-{writer}-{reader}'] = compare(values, reference)
    return results, reference


def copy_ms(source, path):
    shutil.copytree(source, path)
    return path


def paired_case(root, source, model, native):
    root.mkdir()
    output = {}
    original = None
    for scratch in (True, False):
        predictions = []
        for name, factory in (('casa', imager), ('native', native)):
            path = root / f'{name}-{scratch}.ms'
            predict(source, path, factory, model, scratch, False)
            values = (field_samples(path) if scratch else
                      consume(path, root / f'{name}-subtracted.ms', name))
            predictions.append(values)
        expected, actual = predictions
        if scratch:
            original = expected
        output['scratch' if scratch else 'virtual'] = compare(actual, expected)
    return output, original


def field_samples(path):
    return [samples(path, field=field) for field in (0, 1)]


def with_telescope(path, name):
    stored = table()
    assert stored.open(str(path / 'OBSERVATION'), nomodify=False)
    stored.putcell('TELESCOPE_NAME', 0, name)
    assert stored.close()


def local_azel_pointings(path):
    stored = table()
    assert stored.open(str(path / 'ANTENNA'), nomodify=False)
    positions = stored.getcol('POSITION')
    positions[:, 0] += [140_000., -120_000., 100_000.]
    stored.putcol('POSITION', positions)
    assert stored.close()
    measure, quantity = measures(), quanta()
    assert stored.open(str(path / 'POINTING'), nomodify=False)
    for row, (antenna, epoch) in enumerate(zip(stored.getcol('ANTENNA_ID'), stored.getcol('TIME'), strict=True)):
        set_frame(measure, positions[:, antenna], epoch)
        lon, lat = stored.getcell('DIRECTION', row)[:, 0]
        azel = measure.measure(casa_direction(measure, lon, lat), 'AZEL')
        stored.putcell('DIRECTION', row, np.array([[quantity.convert(azel[key], 'rad')['value']]
                                                   for key in ('m0', 'm1')]))
    info = stored.getcolkeyword('DIRECTION', 'MEASINFO')
    info['Ref'] = 'AZEL'
    stored.putcolkeyword('DIRECTION', 'MEASINFO', info)
    assert stored.close()


def set_frame(measure, position, epoch):
    assert measure.doframe(measure.position('ITRF', *[f'{coordinate}m' for coordinate in position]))
    assert measure.doframe(measure.epoch('UTC', f'{epoch}s'))


def casa_direction(measure, lon, lat):
    return measure.direction('J2000', f'{lon}rad', f'{lat}rad')


def feed_offset_fixture(root, pointed):
    observed = copy_ms(pointed, root / 'feed-offset.ms')
    control = copy_ms(pointed, root / 'shifted-pointing-control.ms')
    stored = table()
    assert stored.open(str(observed / 'FEED'), nomodify=False)
    offsets = stored.getcol('BEAM_OFFSET')
    x, y = 1.8e-5, -1.6e-5
    offsets[0, 0, :] = x
    offsets[1, 0, :] = y
    stored.putcol('BEAM_OFFSET', offsets)
    assert stored.close()
    assert stored.open(str(control / 'ANTENNA'))
    positions = stored.getcol('POSITION')
    assert stored.close()
    assert stored.open(str(control))
    times, fields = stored.getcol('TIME'), stored.getcol('FIELD_ID')
    field_times = [np.unique(times[fields == field]) for field in (0, 1)]
    assert stored.close()
    assert stored.open(str(control / 'FIELD'))
    centres = [stored.getcell('PHASE_DIR', field)[:, 0] for field in (0, 1)]
    assert stored.close()
    measure, quantity = measures(), quanta()
    assert stored.open(str(control / 'POINTING'), nomodify=False)
    for row, (antenna, epoch) in enumerate(zip(stored.getcol('ANTENNA_ID'), stored.getcol('TIME'), strict=True)):
        set_frame(measure, positions[:, antenna], epoch)
        field = min((0, 1), key=lambda field: np.min(np.abs(field_times[field] - epoch)))
        field_dir = casa_direction(measure, *centres[field])
        zenith = measure.measure(measure.direction('AZEL', '0deg', '90deg'), 'J2000')
        pa = quantity.convert(measure.posangle(field_dir, zenith), 'rad')['value']
        dx, dy = -np.cos(pa) * x + np.sin(pa) * y, np.sin(pa) * x + np.cos(pa) * y
        lon, lat = stored.getcell('DIRECTION', row)[:, 0]
        shifted = [lon + np.arctan2(np.sin(dx), np.cos(dx) * np.cos(lat + dy)),
                   np.arcsin(np.cos(dx) * np.sin(lat + dy))]
        stored.putcell('DIRECTION', row, np.array(shifted).reshape(2, 1))
    assert stored.close()
    return observed, control


def feed_offset_cases(root, pointed, model, native):
    # The pinned imager crashes on the direct FEED case. An independent
    # Measures calculation folds the rotated offset into CASA's POINTING.
    observed, control = feed_offset_fixture(root, pointed)
    evidence = {}
    for scratch in (True, False):
        native_path = root / f'feed-native-{scratch}.ms'
        casa_path = root / f'feed-control-{scratch}.ms'
        predict(observed, native_path, native, model, scratch, False)
        predict(control, casa_path, imager, model, scratch, False)
        actual = (field_samples(native_path) if scratch else
                  consume(native_path, root / 'feed-native-subtracted.ms', 'native'))
        expected = (field_samples(casa_path) if scratch else
                    consume(casa_path, root / 'feed-control-subtracted.ms', 'casa'))
        evidence['scratch' if scratch else 'virtual'] = compare(actual, expected)
    return evidence


def metadata_cases(root, source, pointed, model, native, control):
    root.mkdir()
    stale = copy_ms(pointed, root / 'stale.ms')
    stored = table()
    assert stored.open(str(stale / 'POINTING'), nomodify=False)
    stored.putcol('TIME', stored.getcol('TIME') - 86400.)
    stored.putcol('INTERVAL', np.zeros(stored.nrows()))
    assert stored.close()
    stale_evidence, stale_values = paired_case(root / 'stale', stale, model, native)
    np.testing.assert_array_equal(stale_values, control)

    azel = copy_ms(pointed, root / 'azel.ms')
    local_azel_pointings(azel)
    azel_evidence, _ = paired_case(root / 'azel', azel, model, native)

    published = root.parent / 'virtual-casa.ms'
    changed = copy_ms(published, root / 'changed-dishes.ms')
    assert stored.open(str(changed / 'ANTENNA'), nomodify=False)
    stored.putcol('DISH_DIAMETER', np.full(stored.nrows(), 12.))
    assert stored.close()
    expected = consume(changed, root / 'changed-dishes-casa.ms', 'casa')
    actual = consume(changed, root / 'changed-dishes-native.ms', 'native')
    assert min(relative_rms(a, b) for a, b in zip(expected, control, strict=True)) > 0.005

    atf = copy_ms(source, root / 'ATF.ms')
    with_telescope(atf, 'ATF')
    atf_evidence, _ = paired_case(root / 'ATF', atf, model, native)
    common = copy_ms(source, root / 'NRAO140FT.ms')
    with_telescope(common, 'NRAO140FT')
    tool = native()
    assert tool.open(str(common), usescratch=True) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=320, cellx='0.3arcsec', outframe='TOPO')
    assert tool.setoptions(ftmachine='mosaic') and tool.ft(str(model)) is False and tool.done()

    return {'stale_pointing': stale_evidence, 'antenna_azel': azel_evidence,
            'feed_offset': feed_offset_cases(root, pointed, model, native),
            'regenerated_dishes': compare(actual, expected), 'ATF': atf_evidence}


def rejected_replacement(root, model, native):
    target = copy_ms(root / 'virtual-native.ms', root / 'model-replacement.ms')
    expected = consume(target, root / 'before-replacement.ms', 'native')
    tool = image()
    assert tool.open(str(model))
    coordinates_tool = tool.coordsys()
    assert tool.done() and coordinates_tool.setreferencepixel(value=[4., 4.], type='direction')
    small = root / 'invalid-screen.im'
    pixels = np.zeros((8, 8, 1, 1), np.float32)
    pixels[4, 4, 0, 0] = 2.
    assert tool.fromarray(outfile=str(small), pixels=pixels, csys=coordinates_tool.torecord())
    assert tool.done() and coordinates_tool.done()
    tool = native()
    assert tool.open(str(target), usescratch=False) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=8, cellx='0.3arcsec', outframe='TOPO')
    assert tool.setoptions(ftmachine='mosaic') and tool.ft(str(small)) is False and tool.done()
    actual = consume(target, root / 'after-replacement.ms', 'native')
    np.testing.assert_array_equal(actual, expected)
    return sum(field.size for field in actual)


def wide_cube(root, source, model, native):
    path = copy_ms(source, root / 'wide-source.ms')
    stored = table()
    assert stored.open(str(path / 'SPECTRAL_WINDOW'), nomodify=False)
    frequencies = (93. + np.arange(12)) * 1e9
    stored.putcell('CHAN_FREQ', 0, frequencies)
    for column in ('CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
        stored.putcell(column, 0, np.full(12, 1e9))
    stored.putcell('REF_FREQUENCY', 0, frequencies[0])
    stored.putcell('TOTAL_BANDWIDTH', 0, 12e9)
    assert stored.close()
    tool = image()
    assert tool.open(str(model))
    coords = tool.coordsys()
    assert tool.done() and coords.setreferencepixel(value=[0.], type='spectral')
    assert coords.setreferencevalue(value='92GHz', type='spectral')
    assert coords.setincrement(value='4GHz', type='spectral')
    wide = root / 'wide.im'
    pixels = np.zeros((320, 320, 1, 5), dtype=np.float32)
    pixels[191, 144, 0, :] = [1.2, 2., .4, 1.9, .8]
    assert tool.fromarray(outfile=str(wide), pixels=pixels, csys=coords.torecord())
    assert tool.setbrightnessunit('Jy/pixel') and tool.done() and coords.done()
    unselected = np.setdiff1d(np.arange(12), np.arange(2, 9, 2))
    before = field_samples(path)
    evidence = {}
    for method in ('linear', 'cubic', 'spline'):
        predictions = []
        for name, factory in (('casa', imager), ('native', native)):
            destination = copy_ms(path, root / f'wide-{method}-{name}.ms')
            tool = factory()
            assert tool.open(str(destination), usescratch=True)
            assert tool.selectvis(field='0,1', spw='0:2~8^2')
            assert tool.defineimage(nx=320, cellx='0.3arcsec', celly='0.3arcsec', outframe='TOPO')
            assert tool.setoptions(ftmachine='mosaic', freqinterp=method, numthreads=1)
            assert tool.ft(str(wide)) and tool.done()
            fields = field_samples(destination)
            for actual, previous in zip(fields, before, strict=True):
                np.testing.assert_array_equal(actual[:, unselected], previous[:, unselected])
            predictions.append(fields)
        expected, actual = predictions
        evidence[method] = compare(actual, expected)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    root = parser.parse_args().work.resolve()
    results = run(root)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)


if __name__ == '__main__':
    main()
