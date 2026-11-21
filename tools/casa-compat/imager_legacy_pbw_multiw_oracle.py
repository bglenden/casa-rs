#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the selection and readback of legacy PB/W convolution planes."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import image, imager, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_legacy_pbw_oracle import cache_contents, data, ft, make_input, read_virtual, recipe


SUPPORT = (3, 3, 2, 3)
W_PLANES = len(SUPPORT)


def warm_cache(seed, cache):
    shutil.copytree(seed, cache)
    manifest = cache / 'aux.dat'
    count, planes, angle, _, _, sampling = manifest.read_text().split()
    assert (count, planes) == ('1', '1')
    widths = ' '.join(f'{support} {support}' for support in SUPPORT)
    manifest.write_text(f'1 {W_PLANES}\n{angle} {widths} {sampling}\n')
    for plane in range(1, W_PLANES):
        for prefix in ('CF', 'CFWT'):
            shutil.copytree(cache / f'{prefix}0_0', cache / f'{prefix}{plane}_0')
        editor = image()
        assert editor.open(str(cache / f'CF{plane}_0'))
        cells = editor.getchunk()
        # Give every plane a distinct asymmetric response, so a mislabeled
        # or duplicated plane cannot pass the numerical comparison.
        factor = 1. + (np.arange(cells.shape[0]) - cells.shape[0] // 2) * (.005 * plane)
        cells *= factor[:, None, None, None]
        assert editor.putchunk(cells) and editor.done()


def probe_rows(seed, target, *, cold=False):
    make_input(seed, target, scratch=True)
    times = data(target, 'TIME')
    eligible = ((times == times.min()) & (data(target, 'ANTENNA1') != data(target, 'ANTENNA2'))
                & ~data(target, 'FLAG_ROW') & ~np.any(data(target, 'FLAG')[:, 0, :], axis=0))
    selected = np.flatnonzero(eligible)
    frequency = data(target / 'SPECTRAL_WINDOW', 'CHAN_FREQ')[0, 0]
    cell = np.radians(15. / 3600.)
    scale = (W_PLANES - 1)**2 / (.25 / cell) if cold else W_PLANES * cell
    boundaries = (np.arange(W_PLANES) + .5)**2 * 299792458. / (frequency * scale)
    positive = np.r_[0., (boundaries[:, None] * np.asarray([.9999, 1.0001])).ravel()]
    w_values = np.r_[positive, -positive[1:]]
    rows = selected[:len(w_values)]
    assert len(rows) == len(w_values)
    editor = table()
    assert editor.open(str(target), nomodify=False)
    for row, w in zip(rows, w_values, strict=True):
        assert editor.putcell('UVW', int(row), [0., 0., w])
    assert editor.done()
    return rows, len(positive)


def compare(root, dataset, model, original_cache):
    from casars.tools import imager as native

    cache = root / 'four-plane-cache'
    warm_cache(original_cache, cache)
    cached = cache_contents(cache)
    source = root / 'seed.ms'
    rows, positives = probe_rows(dataset, source)
    selected = data(source, 'FIELD_ID') == 0
    model_array = image()
    original_planes = []
    for plane in range(W_PLANES):
        assert model_array.open(str(cache / f'CF{plane}_0'))
        original_planes.append(model_array.getchunk())
        assert model_array.done()
    errors = {}
    for machine, record in (('pbwproject', 'nPBWProjectFT'), ('pbmosaic', 'PBMosaicFT')):
        reference, scratch, saved = (root / f'{machine}-{name}.ms' for name in ('casa', 'native', 'saved'))
        for factory, product, is_scratch in ((imager, reference, True), (native, scratch, True), (native, saved, False)):
            make_input(source, product, scratch=is_scratch)
            ft(factory, product, model, cache, scratch=is_scratch, machine=machine, spw='0:0', w_planes=W_PLANES)
        assert cache_contents(cache) == cached
        reference_channel = data(reference, 'MODEL_DATA')[:, 0, :]
        expected = reference_channel[:, rows]
        np.testing.assert_array_equal(expected[:, 1:positives], expected[:, positives:])
        np.testing.assert_array_equal(expected[:, positives - 1], 0.)
        assert np.min(np.abs(expected[:, :positives - 1])) > .05
        assert len(np.unique(expected[0, [0, 2, 4, 6]].round(4))) == W_PLANES
        stored = recipe(saved)
        np.testing.assert_array_equal(stored['casars_legacy_pbw_support'], SUPPORT)
        assert model_array.open(str(Path(stored['diskimage']) / 'CASARS_LEGACY_PBW_KERNEL'))
        np.testing.assert_array_equal(model_array.getchunk(), np.concatenate(original_planes, axis=3))
        assert model_array.done()
        actual = data(scratch, 'MODEL_DATA')[:, 0, :]
        predicted = read_virtual(saved, name=record)[:, 0, :]
        measured = {
            'boundaries': [relative_rms(values[:, rows], expected) for values in (actual, predicted)],
            'selected': [relative_rms(values[:, selected], reference_channel[:, selected])
                         for values in (actual, predicted)],
        }
        print(machine, 'scratch and saved NRMS', measured, flush=True)
        assert max(max(group) for group in measured.values()) < 1.e-3
        errors[machine] = measured
    errors['disabled_and_missing'] = validate_cache(root, source, model, cache, rows)
    return errors


def validate_cache(root, source, model, cache, rows):
    from casars.tools import imager as native

    manifest = cache / 'aux.dat'
    initial = manifest.read_text()
    fields = initial.split()
    fields[7:9] = ['0', '0']
    manifest.write_text(' '.join(fields) + '\n')
    outcomes = []
    options = {'scratch': True, 'spw': '0:0', 'w_planes': W_PLANES}
    for name, factory in (('casa', imager), ('native', native)):
        target = root / f'disabled-{name}.ms'
        make_input(source, target, scratch=True)
        ft(factory, target, model, cache, **options)
        predicted = data(target, 'MODEL_DATA')[:, 0, rows]
        np.testing.assert_array_equal(predicted[:, [4, 5, 12, 13]], 0.)
        outcomes.append(predicted)
    error = relative_rms(outcomes[1], outcomes[0])
    assert error < 1.e-3
    manifest.write_text(initial)
    missing = cache / 'CF2_0'
    missing.rename(cache / 'moved-CF2_0')
    for name, factory in (('casa', imager), ('native', native)):
        target = root / f'disabled-{name}.ms'
        before = data(target, 'MODEL_DATA')
        ft(factory, target, model, cache, accepts=False, **options)
        np.testing.assert_array_equal(data(target, 'MODEL_DATA'), before)
    return {'disabled_plane_nrms': error, 'missing_plane_preserves_model': True}


def compare_cold(root, dataset, model):
    from casars.tools import imager as native

    source = root / 'seed.ms'
    rows, positives = probe_rows(dataset, source, cold=True)
    selected = data(source, 'FIELD_ID') == 0
    outputs = {}
    for machine, record in (('pbwproject', 'nPBWProjectFT'), ('pbmosaic', 'PBMosaicFT')):
        work = root / machine
        work.mkdir()
        options = {'spw': '0:0', 'w_planes': W_PLANES, 'machine': machine}
        pinned_cache = work / 'pinned-cache'
        native_cache = work / 'native-cache'
        pinned, actual, saved, reopen, imported = (
            work / f'{name}.ms' for name in ('pinned', 'native', 'saved', 'reopen', 'imported')
        )
        for target in (pinned, actual, saved, reopen, imported):
            make_input(source, target, scratch=target != saved)
        ft(imager, pinned, model, pinned_cache, scratch=True, **options)
        expected = data(pinned, 'MODEL_DATA')[:, 0, :]
        boundaries = expected[:, rows]
        assert len(np.unique(boundaries[0, [0, 2, 4, 6]].round(4))) == W_PLANES
        np.testing.assert_array_equal(boundaries[:, 1:positives], boundaries[:, positives:])
        np.testing.assert_array_equal(boundaries[:, positives - 1], 0.)

        # Simulate a writer interrupted after promoting a high W plane.
        abandoned = native_cache / '.ep-cache-interrupted'
        abandoned.mkdir(parents=True)
        (abandoned / 'pending-slot').write_text(f'0 {W_PLANES}')
        (native_cache / f'CF{W_PLANES - 1}_0').mkdir()
        (abandoned / f'CFWT{W_PLANES - 1}_0').mkdir()
        preparing = native_cache / '.ep-cache-preparing'
        preparing.mkdir()
        (preparing / 'preparing-slot').touch()
        (preparing / 'pending-slot.new').touch()
        (preparing / f'CF{W_PLANES - 1}_0').mkdir()
        old_journal = native_cache / '.ep-cache-old-journal'
        old_journal.mkdir()
        (old_journal / 'pending-slot').touch()
        unrelated = native_cache / '.ep-cache-unrelated'
        unrelated.mkdir()
        (unrelated / 'marker').write_text('keep')
        ft(native, actual, model, native_cache, scratch=True, **options)
        assert not any(path.exists() for path in (abandoned, preparing, old_journal))
        assert (unrelated / 'marker').read_text() == 'keep'
        np.testing.assert_allclose(
            np.loadtxt(pinned_cache / 'aux.dat', skiprows=1),
            np.loadtxt(native_cache / 'aux.dat', skiprows=1), atol=5e-5,
        )
        cache_errors = compare_cache_images(pinned_cache, native_cache)
        unchanged = cache_contents(native_cache)

        replay_cache = work / 'saved-cache'
        ft(native, saved, model, replay_cache, scratch=False, **options)
        assert 'casars_legacy_pbw_w_scale' in recipe(saved)
        replay_cache.rename(work / 'moved-cache')
        cold = data(actual, 'MODEL_DATA')[:, 0, :]
        replayed = read_virtual(saved, name=record)[:, 0, :]
        np.testing.assert_array_equal(cold[:, rows[positives - 1]], 0.)

        ft(native, reopen, model, native_cache, scratch=True, **options)
        ft(imager, imported, model, native_cache, scratch=True, **options)
        assert cache_contents(native_cache) == unchanged
        warm = data(reopen, 'MODEL_DATA')[:, 0, :]
        warm_pinned = data(imported, 'MODEL_DATA')[:, 0, :]
        assert relative_rms(expected[:, rows], warm_pinned[:, rows]) > .05
        errors = {
            'cold_scratch': relative_rms(cold[:, selected], expected[:, selected]),
            'cold_saved': relative_rms(replayed[:, selected], expected[:, selected]),
            'warm_reopen': relative_rms(warm[:, selected], warm_pinned[:, selected]),
            **cache_errors,
        }
        print(machine, 'cold and warm NRMS', errors, flush=True)
        assert max(errors.values()) < 1.e-3
        outputs[machine] = errors
    return outputs


def compare_cache_images(reference, actual):
    def pixels(path):
        tool = image()
        assert tool.open(str(path))
        cells = tool.getchunk()
        assert tool.done()
        return cells

    names = ['avgPB']
    names.extend(f'{prefix}{plane}_0' for plane in range(W_PLANES) for prefix in ('CF', 'CFWT'))
    errors = {}
    for name in names:
        expected, measured = pixels(reference / name), pixels(actual / name)
        if name.startswith('CF'):
            # Tap normalization can conceal a phase error common to an
            # entire plane. Compare the centre directly before degridding.
            centre = (expected.shape[0] // 2, expected.shape[1] // 2, 0, 0)
            assert abs(measured[centre] / expected[centre] - 1.) < 1.e-5, name
        errors[name] = relative_rms(measured, expected)
    return errors


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--cold', action='store_true')
    args = parser.parse_args()
    if not args.cold and args.cache is None:
        parser.error('--cache is required for the warm-cache comparison')
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=args.work) as scratch:
        if args.cold:
            evidence = compare_cold(Path(scratch), args.dataset, args.model)
        else:
            evidence = compare(Path(scratch), args.dataset, args.model, args.cache)
    (args.work / 'results.json').write_text(json.dumps(evidence, indent=2) + '\n')
