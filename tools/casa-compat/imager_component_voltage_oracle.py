#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair legacy component-list predictions with CASA's configured voltage pattern."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import componentlist, imager as casa_imager, ms as casa_ms, table, vpmanager
import numpy as np

from calibration_ft_oracle import coordinates
from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import corrected_or_data, data, make_input


def source(path, dataset, flux, spectral_index=None):
    ra, dec = coordinates(dataset)
    settings = {} if spectral_index is None else {
        'spectrumtype': 'spectral index', 'index': spectral_index,
    }
    tool = componentlist()
    assert tool.addcomponent(dir=f'J2000 {ra + .003}rad {dec - .002}rad',
                             flux=flux, freq='1.55GHz', **settings)
    assert tool.rename(str(path)) and tool.done()


def predict(factory, dataset, component, *, model=None, scratch=True,
            beam=True, squint=False, spw='0', vptable=None):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch) and tool.selectvis(field=0, spw=spw)
    pattern = {} if vptable is None else {'usedefaultvp': False, 'vptable': str(vptable)}
    assert tool.setvp(dovp=beam, dosquint=squint, **pattern)
    options = {} if model is None else {'model': str(model)}
    assert tool.ft(complist=str(component), **options) and tool.done()


def compare(results, name, actual, expected):
    assert np.isfinite(actual).all() and np.isfinite(expected).all(), name
    error = relative_rms(actual, expected)
    assert error < 1.e-3, (name, error)
    results[name] = error
    print(name, 'NRMS', error, flush=True)


def scratch_pair(root, seed, component, results, name, *, model=None,
                 beam=True, squint=False, spw='0', vptable=None):
    from casars.tools import imager as native_imager

    targets = []
    for label, factory in [('casa', casa_imager), ('native', native_imager)]:
        path = root / f'{name}-{label}.ms'
        make_input(seed, path, scratch=True)
        predict(factory, path, component, model=model, beam=beam, squint=squint, spw=spw,
                vptable=vptable)
        targets.append(path)
    expected, actual = [data(path, 'MODEL_DATA') for path in targets]
    compare(results, name, actual, expected)
    for path in targets:
        for column in ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT'):
            np.testing.assert_array_equal(data(path, column), data(seed, column))
    return expected


def virtual_exchange(root, seed, component, expected, results):
    from casars.tools import imager as native_imager, ms as native_ms

    saved = []
    for writer, factory in [('casa', casa_imager), ('native', native_imager)]:
        path = root / f'{writer}-beam-virtual.ms'
        make_input(seed, path, scratch=False)
        predict(factory, path, component, scratch=False)
        reader = table()
        assert reader.open(str(path))
        assert 'MODEL_DATA' not in reader.colnames() and reader.done()
        saved.append((writer, path))
    component.rename(root / 'unavailable.cl')
    for writer, path in saved:
        for reader, factory in [('casa', casa_ms), ('native', native_ms)]:
            target = root / f'{writer}-to-{reader}-virtual.ms'
            shutil.copytree(path, target)
            before = corrected_or_data(target)
            tool = factory()
            assert tool.open(str(target), nomodify=False)
            assert tool.uvsub() and tool.done()
            compare(results, f'{writer}-to-{reader}-virtual-beam', before - data(target, 'CORRECTED_DATA'),
                    expected)


def run(root, seed, model):
    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    with TemporaryDirectory(dir=root) as temporary:
        work = Path(temporary)
        positive, signed, polarized = [work / f'{name}.cl' for name in ('positive', 'signed', 'polarized')]
        source(positive, seed, 3.7, spectral_index=-.81)
        source(signed, seed, -3.7)
        source(polarized, seed, [3.7, .4, -.6, -.8])
        plain = scratch_pair(work, seed, positive, results, 'beam-disabled', beam=False)
        tapered = scratch_pair(work, seed, positive, results, 'spectral-component')
        assert relative_rms(tapered, plain) > .1
        # The PB varies across the window, even on the same baseline.
        ratios = np.abs(tapered[0, [0, -1], 3] / plain[0, [0, -1], 3])
        assert abs(ratios[0] - ratios[1]) > 1.e-4
        scratch_pair(work, seed, signed, results, 'signed-component')
        radial = scratch_pair(work, seed, polarized, results, 'polarized-component')
        squinted = scratch_pair(work, seed, polarized, results, 'squinted-component', squint=True)
        assert relative_rms(squinted, radial) > 1.e-3
        beam_table = work / 'custom.tbl'
        manager = vpmanager()
        manager.setpbgauss(telescope='VLA', halfwidth='400arcsec', maxrad='1500arcsec',
                           reffreq='1.4GHz')
        assert manager.saveastable(str(beam_table))
        custom = scratch_pair(work, seed, positive, results, 'custom-component', vptable=beam_table)
        assert relative_rms(custom, tapered) > .1
        scratch_pair(work, seed, positive, results, 'image-and-component', model=model)
        active = scratch_pair(work, seed, positive, results, 'selected-channel', spw='0:23')
        np.testing.assert_allclose(active[:, 23, :], tapered[:, 23, :], rtol=1e-4, atol=1e-6)
        one = work / 'one-physical-channel.ms'
        casatasks.mstransform(str(seed), str(one), spw='0:23', datacolumn='data')
        assert data(one / 'SPECTRAL_WINDOW', 'CHAN_FREQ').size == 1
        scratch_pair(work, one, signed, results, 'signed-single-channel')
        virtual_exchange(work, seed, positive, plain, results)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{argument}', type=Path, required=True)
    args = parser.parse_args()
    run(args.work.resolve(), args.dataset.resolve(), args.model.resolve())
