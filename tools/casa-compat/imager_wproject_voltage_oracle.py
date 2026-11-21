#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare voltage-pattern image and component prediction in legacy WProjectFT."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casatools import imager as casa_imager, ms as casa_ms, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_changing_voltage_oracle import pointings
from imager_component_voltage_oracle import compare, source
from imager_custom_squint_oracle import radial_table
from imager_legacy_ep_multislot_oracle import copy_row
from imager_legacy_pbw_oracle import corrected_or_data, data, make_input, recipe
from imager_saved_radial_curves_oracle import SHAPES, voltage_table


def configure(tool, beam, squint, planes, angle):
    assert tool.defineimage(nx=64, ny=64, cellx='64arcsec', celly='64arcsec')
    assert tool.setoptions(ftmachine='wproject', padding=1.4, wprojplanes=planes, numthreads=1)
    assert tool.setvp(dovp=True, usedefaultvp=False, vptable=str(beam), dosquint=squint,
                      parangleinc=angle, skyposthreshold='180deg')


def predict(factory, target, model, beam, *, scratch=True, squint=True, planes=8, angle='360deg',
            spw='0'):
    tool = factory()
    assert tool.open(str(target), usescratch=scratch)
    assert tool.selectvis(field=0, spw=spw)
    configure(tool, beam, squint, planes, angle)
    assert tool.ft(**model) and tool.done()


def scratch_pair(work, seed, label, model, beam, results, **opts):
    from casars.tools import imager as native_imager

    writes = []
    for engine, factory in [('casa', casa_imager), ('native', native_imager)]:
        target = work / f'{label}-{engine}.ms'
        make_input(seed, target, scratch=True)
        predict(factory, target, model, beam, **opts)
        writes.append(data(target, 'MODEL_DATA'))
        for column in ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT'):
            np.testing.assert_array_equal(data(target, column), data(seed, column))
    compare(results, label, writes[1], writes[0])
    return writes[0]


def saved_exchange(work, seed, cases, beam, results):
    from casars.tools import imager as native_imager, ms as native_ms

    writers = []
    for label, model in cases:
        for engine, factory in [('casa', casa_imager), ('native', native_imager)]:
            path = work / f'{label}-{engine}-saved.ms'
            make_input(seed, path, scratch=False)
            predict(factory, path, model, beam, scratch=False)
            writers.append((label, engine, path))
    beam.rename(work / 'unavailable.tbl')
    expected = {}
    for label, writer, path in writers:
        for engine, factory in [('casa', casa_ms), ('native', native_ms)]:
            target = work / f'{label}-{writer}-to-{engine}.ms'
            make_input(path, target, scratch=False)
            previous = corrected_or_data(target)
            tool = factory()
            assert tool.open(str(target), nomodify=False)
            assert tool.uvsub() and tool.done()
            values = previous - data(target, 'CORRECTED_DATA')
            assert np.max(np.abs(values)) > .1
            pinned = expected.setdefault(label, values)
            compare(results, f'{label}-{writer}-to-{engine}', values, pinned)
    return expected


def changing_beam(work, seed, image, beam, results):
    active = work / 'changing-pointings.ms'
    epochs = pointings(seed, active)
    times = data(active, 'TIME')
    writer = table()
    assert writer.open(str(active), nomodify=False)
    uvw = writer.getcol('UVW')
    uvw[2, times == epochs[1]] *= 4.
    assert writer.putcol('UVW', uvw) and writer.done()
    for planes in (8, -1):
        held = scratch_pair(work, active, f'{planes}-held', image, beam, results, planes=planes)
        label = f'{planes}-refreshed'
        refreshed = scratch_pair(work, active, label, image, beam, results,
                                 planes=planes, angle='0deg')
        assert relative_rms(refreshed, held) > .5
        actual = data(work / f'{label}-native.ms', 'MODEL_DATA')
        # Each group must keep the full selection's automatic W-plane spacing.
        for epoch in epochs:
            rows = times == epoch
            assert relative_rms(actual[..., rows], refreshed[..., rows]) < 1.e-6


def w_geometry(path):
    info = recipe(path)
    return {key: info[key] for key in ('minw', 'maxw', 'rmsw', 'nwplanes', 'savedwscale')}


def compare_geometry(expected, actual):
    assert expected['nwplanes'] == actual['nwplanes'], (expected, actual)
    for key in expected:
        np.testing.assert_allclose(actual[key], expected[key], rtol=1.e-13, atol=1.e-14,
                                   err_msg=key)


def two_windows(seed, active, high_window):
    make_input(seed, active, scratch=True)
    copy_row(active / 'SPECTRAL_WINDOW')
    copy_row(active / 'DATA_DESCRIPTION')
    writer = table()
    assert writer.open(str(active / 'SPECTRAL_WINDOW'), nomodify=False)
    for column in ('CHAN_FREQ', 'REF_FREQUENCY'):
        assert writer.putcell(column, high_window, writer.getcell(column, high_window) * 1.25)
    assert writer.done() and writer.open(str(active / 'DATA_DESCRIPTION'), nomodify=False)
    assert writer.putcell('SPECTRAL_WINDOW_ID', 1, 1) and writer.done()
    assert writer.open(str(active), nomodify=False)
    times, uvw = writer.getcol('TIME'), writer.getcol('UVW')
    windows = (times != times.min()).astype('int32')
    uvw[2, windows != high_window] *= 4.
    assert writer.putcol('DATA_DESC_ID', windows) and writer.putcol('UVW', uvw) and writer.done()


def multiple_windows(work, seed, image, beam, results):
    from casars.tools import imager as native_imager

    for high_window, selection in [(1, '0,1'), (0, '0:0~6^2,1:0~9^3')]:
        label = f'high-window-{high_window}'
        active = work / f'{label}.ms'
        two_windows(seed, active, high_window)
        summary = []
        for engine, factory in [('casa', casa_imager), ('native', native_imager)]:
            path = work / f'{label}-{engine}-saved.ms'
            make_input(active, path, scratch=False)
            predict(factory, path, image, beam, scratch=False, planes=-1, spw=selection)
            summary.append(w_geometry(path))
        compare_geometry(*summary)
        assert summary[0]['nwplanes'] > 32
        print(label, 'geometry', summary[0], flush=True)
        model = scratch_pair(work, active, label, image, beam, results, planes=-1, spw=selection,
                             angle='0deg')
        assert np.max(np.abs(model)) > .5


def multiple_measurement_sets(work, seed, image, beam, results):
    from casars.tools import imager as native_imager

    values = {}
    for scratch in (False, True):
        for engine, factory in [('casa', casa_imager), ('native', native_imager)]:
            paths = [work / f'multi-{engine}-{scratch}-{index}.ms' for index in range(2)]
            for index, path in enumerate(paths):
                make_input(seed, path, scratch=scratch)
                if index == 1:
                    writer = table()
                    assert writer.open(str(path), nomodify=False)
                    uvw = writer.getcol('UVW')
                    uvw[2] *= 4.
                    assert writer.putcol('UVW', uvw) and writer.done()
            tool = factory()
            for path in paths:
                assert tool.selectvis(vis=str(path), field=0, spw='0', usescratch=scratch)
            configure(tool, beam, True, -1, '360deg')
            assert tool.ft(**image) and tool.done()
            for index, path in enumerate(paths):
                if scratch:
                    model = data(path, 'MODEL_DATA')
                    assert np.max(np.abs(model)) > .5
                    if engine == 'casa':
                        values[index] = model
                    else:
                        name = f'multi-model-{index}'
                        compare(results, name, model, values[index])
                        assert results[name] < 1.e-6
                else:
                    stats = w_geometry(path)
                    if engine == 'casa' and index == 0:
                        values['stats'] = stats
                        assert stats['nwplanes'] > 32
                        print('enrolled measurement sets geometry', stats, flush=True)
                    else:
                        compare_geometry(values['stats'], stats)


def run(root, seed, model):
    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        beam, point = work / 'beam.tbl', work / 'point.cl'
        radial_table(beam, '1.4GHz')
        source(point, seed, [3.7, .4, -.6, -.8])
        image = {'model': str(model)}
        component = {'complist': str(point)}
        cases = [('image', image), ('point', component), ('combined', image | component)]
        for name, models in cases:
            radial = scratch_pair(work, seed, f'{name}-radial', models, beam, results, squint=False)
            squint = scratch_pair(work, seed, f'{name}-squint', models, beam, results)
            assert relative_rms(squint, radial) > .1 and np.max(np.abs(squint)) > .001
        scratch_pair(work, seed, 'auto-w-image', image, beam, results, planes=-1)
        changing_beam(work, seed, image, beam, results)
        multiple_windows(work, seed, image, beam, results)
        multiple_measurement_sets(work, seed, image, beam, results)
        for shape in SHAPES:
            custom = work / f'{shape}.tbl'
            voltage_table(custom, shape)
            for name, models in (('image', image), ('point', component)):
                values = scratch_pair(work, seed, f'{shape}-{name}', models, custom, results,
                                      squint=False)
                assert np.max(np.abs(values)) > .001
        saved = saved_exchange(work, seed, cases, beam, results)
        assert relative_rms(saved['point'], saved['image']) > .1
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(args.work.resolve(), args.dataset.resolve(), args.model.resolve())
