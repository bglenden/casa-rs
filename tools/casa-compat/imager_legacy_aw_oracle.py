#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare warm legacy AW prediction against the pinned CASA imager."""

import argparse
from pathlib import Path
import shutil

from casars.tools import imager, ms
from casatools import image, ms as casa_ms
import numpy as np

from imager_legacy_pbw_oracle import (
    cache_contents, corrected_or_data, data, make_input, recipe, reject_recipe, relative_rms,
)


def predict(dataset, model, cache, machine, correction, *, scratch=True, limit=.05):
    tool = imager()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=0, spw='0:0')
    assert tool.defineimage(nx=256, ny=256, cellx='15arcsec', celly='15arcsec',
                            phasecenter=0, stokes='IQUV', outframe='TOPO')
    assert tool.setoptions(ftmachine=machine, wprojplanes=1, numthreads=1,
                           cfcachedirname=str(cache), applypointingoffsets=False,
                           dopbgriddingcorrections=correction, pblimit=limit)
    assert tool.ft(str(model)) and tool.done()


def complex_image(path):
    tool = image()
    assert tool.open(str(path))
    pixels = tool.getchunk()
    assert np.iscomplexobj(pixels) and tool.done()
    return pixels


def saved_control(root, dataset, model, cache, reference, expected, machine):
    target = root / f'{machine}-saved.ms'
    make_input(dataset, target, scratch=False)
    owned_cache = root / f'{machine}-source-cache'
    shutil.copytree(cache, owned_cache)
    predict(target, model, owned_cache, machine, True, scratch=False)
    record = recipe(target)
    assert record['name'] == 'AWProjectWBFT'
    saved_image = Path(record['diskimage'])
    held = saved_image / 'CASARS_LEGACY_AW_CACHE'
    kernel_names = sorted(path.name for path in owned_cache.glob('CFS_*'))
    assert kernel_names and all(complex_image(held / name).size for name in kernel_names)
    pinned = reference / f'{machine}.ms'
    pinned_image = Path(recipe(pinned)['diskimage'])
    error = relative_rms(complex_image(saved_image), complex_image(pinned_image))
    print(machine, 'saved Fourier image NRMS', error, flush=True)
    assert error < 1.e-3
    owned_cache.rename(root / f'{machine}-unavailable-cache')
    prior = corrected_or_data(target)
    tool = ms()
    assert tool.open(str(target), nomodify=False)
    assert tool.msselect({'field': '0'})
    assert tool.uvsub() and tool.done()
    actual = prior - data(target, 'CORRECTED_DATA')
    assert not np.any(actual[:, 1:, :])
    error = relative_rms(actual[:, 0, :], expected[:, 0, :])
    print(machine, 'reopened selected channel NRMS', error, flush=True)
    assert error < 1.e-3
    reject_recipe(casa_ms, target, 'Unsupported FTMachine')
    copied_reference = root / f'{machine}-pinned-saved.ms'
    make_input(pinned, copied_reference, scratch=False)
    reject_recipe(ms, copied_reference, 'CASARS_LEGACY_AW_CACHE')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--uncorrected-reference', type=Path, required=True)
    parser.add_argument('--corrected-reference', type=Path, required=True)
    parser.add_argument('--saved-reference', type=Path)
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    errors = []
    for correction, root in ((False, args.uncorrected_reference),
                             (True, args.corrected_reference)):
        cache = args.work / f'cache-{correction}'
        shutil.copytree(root / 'cache', cache)
        prior = cache_contents(cache)
        for machine in ('awproject', 'mawproject'):
            dataset = args.work / f'{machine}-{correction}.ms'
            make_input(args.dataset, dataset, scratch=True)
            pinned = root / f'control-{machine}-IQUV-{correction}.ms'
            expected = data(pinned, 'MODEL_DATA')
            assert np.abs(expected).max() > 1. and np.abs(expected.imag).max() > .1
            predict(dataset, args.model, cache, machine, correction)
            actual = data(dataset, 'MODEL_DATA')
            np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
            error = relative_rms(actual[:, 0, :], expected[:, 0, :])
            errors.append(error)
            print(machine, correction, expected.size, 'complex samples, selected channel NRMS',
                  error, flush=True)
            if correction and args.saved_reference and error < 1.e-3:
                saved_control(args.work, args.dataset, args.model, cache, args.saved_reference,
                              expected, machine)
        assert cache_contents(cache) == prior
    assert max(errors) < 1.e-3
    if args.saved_reference:
        target = args.work / 'high-pblimit.ms'
        make_input(args.dataset, target, scratch=True)
        predict(target, args.model, args.work / 'cache-True', 'awproject', True, limit=.8)
        expected = data(args.saved_reference / 'high-pblimit.ms', 'MODEL_DATA')
        error = relative_rms(data(target, 'MODEL_DATA')[:, 0, :], expected[:, 0, :])
        print('high pblimit selected channel NRMS', error, flush=True)
        assert error < 1.e-3


if __name__ == '__main__':
    main()
