#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair legacy voltage-pattern squint on partial image polarization axes."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casatools import image, imager as casa_imager, ms as casa_ms, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_component_voltage_oracle import compare, predict, scratch_pair
from imager_legacy_pbw_oracle import make_input
from imager_legacy_squint_oracle import consume


def image_planes(original, work):
    tool = image()
    assert tool.open(str(original))
    coords = tool.coordsys()
    record, values = coords.torecord(), tool.getchunk()
    assert coords.done() and tool.done()
    stokes = next(key for key in record if key.startswith('stokes'))
    i, q, _, v = (values[:, :, n, :] for n in range(4))
    cases = {
        'iv': (['I', 'V'], [i, v]),
        'iq': (['I', 'Q'], [i, q]),
        'receptors': (['RR', 'LL'], [i + v, i - v]),
        'v': (['V'], [v]),
        'q': (['Q'], [q]),
    }
    for name, (axis, planes) in cases.items():
        assert np.max(np.abs(planes)) > 0., name
        target = work / f'{name}.im'
        record[stokes]['stokes'] = axis
        tool = image()
        assert tool.fromarray(str(target), np.stack(planes, axis=2), record)
        assert tool.done()
        yield name, target


def linear_input(seed, target):
    make_input(seed, target, scratch=True)
    writer = table()
    assert writer.open(str(target / 'POLARIZATION'), nomodify=False)
    assert writer.putcell('CORR_TYPE', 0, np.array([9, 12], np.int32))
    assert writer.putcell('CORR_PRODUCT', 0, np.array([[0, 1], [0, 1]], np.int32))
    assert writer.done() and writer.open(str(target / 'FEED'), nomodify=False)
    receptors = np.repeat(np.array([['X'], ['Y']]), writer.nrows(), axis=1)
    assert writer.putcol('POLARIZATION_TYPE', receptors) and writer.done()


def saved_exchange(work, seed, name, model, expected, results):
    from casars.tools import imager as native_imager, ms as native_ms

    for writer, factory in [('casa', casa_imager), ('native', native_imager)]:
        path = work / f'{name}-{writer}-saved.ms'
        make_input(seed, path, scratch=False)
        predict(factory, path, None, model=model, scratch=False, squint=True)
        for reader, ms_factory in [('casa', casa_ms), ('native', native_ms)]:
            target = work / f'{name}-{writer}-to-{reader}.ms'
            make_input(path, target, scratch=False)
            actual = consume(ms_factory, target)
            label = f'{name}-{writer}-to-{reader}'
            if name == 'q':
                assert np.max(np.abs(actual)) < 1.e-6
            else:
                compare(results, label, actual, expected)
                assert results[label] < 1.e-5


def run(root, seed, model):
    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        for name, target in image_planes(model, work):
            radial = scratch_pair(work, seed, None, results, f'{name}-radial', model=target)
            shifted = scratch_pair(work, seed, None, results, f'{name}-squint', model=target,
                                   squint=True)
            assert results[f'{name}-squint'] < 1.e-5
            if name == 'q':
                assert not np.any(shifted)
            else:
                assert relative_rms(shifted, radial) > 1.e-4
            saved_exchange(work, seed, name, target, shifted, results)
        linear = work / 'linear.ms'
        linear_input(seed, linear)
        radial = scratch_pair(work, linear, None, results, 'linear-radial', model=model)
        requested = scratch_pair(work, linear, None, results, 'linear-squint', model=model,
                                 squint=True)
        np.testing.assert_array_equal(radial, requested)
        saved_exchange(work, linear, 'linear', model, requested, results)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    args = parser.parse_args()
    run(args.work.resolve(), args.dataset.resolve(), args.model.resolve())
