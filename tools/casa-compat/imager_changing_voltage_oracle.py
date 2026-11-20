#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy voltage-pattern refresh thresholds against pinned CASA."""

import argparse
from pathlib import Path

import numpy as np
from casars.tools import imager, ms
from casatools import imager as casa_imager, measures, ms as casa_ms, table

from imager_legacy_pbw_oracle import data, make_input, relative_rms
from imager_legacy_squint_oracle import compare, consume, fourier_grid


def pointings(source, output):
    make_input(source, output, scratch=True)
    epochs = np.unique(data(output, 'TIME'))
    assert len(epochs) == 3
    centre = data(output / 'FIELD', 'PHASE_DIR')[:, 0, 0]
    antennas = data(output / 'FEED', 'ANTENNA_ID')
    count = len(antennas)
    rows = count * len(epochs)
    positions = np.repeat(centre[:, None, None], rows, axis=2)
    positions[0, 0, count:] += np.radians(10. / 60.)
    positions[1, 0, 2 * count:] -= np.radians(12. / 60.)
    writer = table()
    assert writer.open(str(output / 'POINTING'), nomodify=False)
    assert writer.nrows() == 0 and writer.addrows(rows)
    for column, values in {
        'ANTENNA_ID': np.tile(antennas, len(epochs)),
        'TIME': np.repeat(epochs, count),
        'TIME_ORIGIN': np.repeat(epochs, count),
        'INTERVAL': np.full(rows, 30.),
        'NAME': ['voltage-control'] * rows,
        'NUM_POLY': np.zeros(rows, dtype=np.int32),
        'TRACKING': np.ones(rows, dtype=bool),
        'DIRECTION': positions,
        'TARGET': positions,
    }.items():
        assert writer.putcol(column, values)
    assert writer.done()
    return epochs


def predict(factory, dataset, model, angle, sky, squint, *, scratch=True):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=0, spw='0:0')
    assert tool.defineimage(nx=256, ny=256, cellx='15arcsec', celly='15arcsec',
                            phasecenter=0, stokes='IQUV', outframe='TOPO')
    assert tool.setoptions(ftmachine='ft', numthreads=1)
    assert tool.setvp(dovp=True, dosquint=squint, parangleinc=angle, skyposthreshold=sky)
    assert tool.ft(str(model)) and tool.done()


def azel_pointings(source, target, epochs):
    make_input(source, target, scratch=True)
    writer = table()
    assert writer.open(str(source))
    epoch_ref = writer.getcolkeyword('TIME', 'MEASINFO')['Ref']
    assert writer.done()
    assert writer.open(str(target / 'POINTING'), nomodify=False)
    values, times = writer.getcol('DIRECTION'), writer.getcol('TIME')
    tool = measures()
    assert tool.doframe(tool.observatory('VLA'))
    for epoch in epochs:
        selected = times == epoch
        lon, lat = values[:, 0, np.flatnonzero(selected)[0]]
        assert tool.doframe(tool.epoch(epoch_ref, {'unit': 's', 'value': epoch}))
        direction = tool.direction('J2000', {'unit': 'rad', 'value': lon},
                                   {'unit': 'rad', 'value': lat})
        local = tool.measure(direction, 'AZEL')
        values[:, 0, selected] = np.array([local['m0']['value'], local['m1']['value']])[:, None]
    for column in ('DIRECTION', 'TARGET'):
        info = writer.getcolkeyword(column, 'MEASINFO')
        info['Ref'] = 'AZEL'
        assert writer.putcolkeyword(column, 'MEASINFO', info) and writer.putcol(column, values)
    assert writer.done()


def within_buffer_control(root, source, model):
    clean = root / 'zero-uvw.ms'
    make_input(source, clean, scratch=True)
    writer = table()
    assert writer.open(str(clean), nomodify=False)
    assert writer.putcol('UVW', np.zeros_like(writer.getcol('UVW'))) and writer.done()
    results = []
    for label, factory in (('unmodified', casa_imager), ('pinned', casa_imager), ('native', imager)):
        target = root / f'{label}-buffer.ms'
        make_input(clean, target, scratch=True)
        if label != 'unmodified':
            assert writer.open(str(target / 'FEED'), nomodify=False)
            angles = writer.getcol('RECEPTOR_ANGLE')
            angles[0, [10, 11]] = [.8, -1.1]
            assert writer.putcol('RECEPTOR_ANGLE', angles) and writer.done()
        predict(factory, target, model, '0deg', '180deg', True)
        results.append(data(target, 'MODEL_DATA')[:, 0, :])
    baseline, expected, actual = results
    np.testing.assert_array_equal(expected, baseline)
    compare('within-buffer feed edits leave the held result', actual, expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    source = args.work / 'pointings.ms'
    epochs = pointings(args.dataset, source)
    times = data(source, 'TIME')
    results, saved = {}, {}
    for name, angle, sky, squint in (
        ('held', '360deg', '180deg', True),
        ('immediate', '0deg', '180deg', True),
        ('immediate-zero-sky', '0deg', '0deg', True),
        ('one-degree', '1deg', '0deg', True),
        ('immediate-radial', '0deg', '180deg', False),
    ):
        for engine, factory in (('pinned', casa_imager), ('native', imager)):
            dataset = args.work / f'{engine}-{name}.ms'
            make_input(source, dataset, scratch=True)
            predict(factory, dataset, args.model, angle, sky, squint)
            results[engine, name] = data(dataset, 'MODEL_DATA')
            if name in ('held', 'immediate'):
                dataset = args.work / f'{engine}-{name}-saved.ms'
                make_input(source, dataset, scratch=False)
                predict(factory, dataset, args.model, angle, sky, squint, scratch=False)
                saved[engine, name] = fourier_grid(dataset)
        expected, actual = (results[engine, name] for engine in ('pinned', 'native'))
        np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
        compare(name, actual[:, 0, :], expected[:, 0, :])
    held = results['pinned', 'held'][:, 0, :]
    immediate = results['pinned', 'immediate'][:, 0, :]
    middle, last = (times == epoch for epoch in epochs[1:])
    assert relative_rms(immediate[:, middle], held[:, middle]) > .1
    assert relative_rms(immediate[:, last], held[:, last]) > .2
    np.testing.assert_array_equal(results['pinned', 'immediate'],
                                  results['pinned', 'immediate-zero-sky'])
    one = results['pinned', 'one-degree'][:, 0, :]
    np.testing.assert_array_equal(one[:, middle], held[:, middle])
    np.testing.assert_array_equal(one[:, last], immediate[:, last])
    for engine in ('pinned', 'native'):
        np.testing.assert_array_equal(saved[engine, 'held'], saved[engine, 'immediate'])
    compare('saved first applied beam', saved['native', 'immediate'], saved['pinned', 'immediate'])
    for writer, reader, factory in (('pinned', 'native', ms), ('native', 'pinned', casa_ms)):
        target = args.work / f'{writer}-to-{reader}.ms'
        make_input(args.work / f'{writer}-immediate-saved.ms', target, scratch=False)
        replay = consume(factory, target)
        assert not np.any(replay[:, 1:, :])
        compare(f'saved {writer} to {reader}', replay[:, 0, :], held)
    local = args.work / 'pointings-azel.ms'
    azel_pointings(source, local, epochs)
    for engine, factory in (('pinned', casa_imager), ('native', imager)):
        target = args.work / f'{engine}-local-held.ms'
        make_input(local, target, scratch=True)
        predict(factory, target, args.model, '360deg', '180deg', True)
        actual = data(target, 'MODEL_DATA')[:, 0, :]
        compare(f'{engine} local-frame refresh with the default increment', actual, immediate)
    within_buffer_control(args.work, source, args.model)


if __name__ == '__main__':
    main()
