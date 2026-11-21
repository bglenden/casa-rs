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
from imager_legacy_pbw_oracle import corrected_or_data, data, make_input
from imager_saved_radial_curves_oracle import SHAPES, voltage_table


def predict(factory, target, model, beam, *, scratch=True, squint=True, planes=8, angle='360deg'):
    tool = factory()
    assert tool.open(str(target), usescratch=scratch)
    assert tool.selectvis(field=0, spw='0')
    assert tool.defineimage(nx=64, ny=64, cellx='64arcsec', celly='64arcsec')
    assert tool.setoptions(ftmachine='wproject', padding=1.4, wprojplanes=planes, numthreads=1)
    assert tool.setvp(dovp=True, usedefaultvp=False, vptable=str(beam), dosquint=squint,
                      parangleinc=angle, skyposthreshold='180deg')
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
