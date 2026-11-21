#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy VLA PB/W model prediction and cache exchange with pinned CASA."""

import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess
import sys

import casatasks
from casatools import image, imager, table
import numpy as np

from gaincal_oracle import relative_rms


def configured_imager(factory, dataset, cache, *, scratch, spw='', correction=True, limit=.05,
                      interp='nearest', machine='pbwproject', field=0, baseline='',
                      pointing=False, ep_table='', w_planes=1):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=field, spw=spw, baseline=baseline)
    assert tool.defineimage(nx=256, ny=256, cellx='15arcsec', celly='15arcsec',
                            phasecenter=0, stokes='IQUV', outframe='TOPO')
    assert tool.setoptions(ftmachine=machine, wprojplanes=w_planes, numthreads=1,
                           cfcachedirname=str(cache), applypointingoffsets=pointing,
                           epjtablename=str(ep_table),
                           dopbgriddingcorrections=correction, pblimit=limit, freqinterp=interp)
    return tool


def ft(factory, dataset, model, cache, *, accepts=True, **options):
    tool = configured_imager(factory, dataset, cache, **options)
    assert tool.ft(str(model)) is accepts
    if accepts:
        (dataset.parent / f'{dataset.name}.complete').touch()
    assert tool.done()


def data(dataset, column):
    reader = table()
    assert reader.open(str(dataset))
    result = reader.getcol(column)
    assert reader.done()
    return result


def make_input(dataset, target, *, scratch):
    shutil.copytree(dataset, target)
    if not scratch:
        writer = table()
        assert writer.open(str(target), nomodify=False)
        if 'MODEL_DATA' in writer.colnames():
            assert writer.removecols('MODEL_DATA')
        assert writer.done()


def cache_contents(cache):
    return {
        path.relative_to(cache): hashlib.sha256(path.read_bytes()).digest()
        for path in cache.rglob('*')
        if path.is_file() and path.name not in ('table.lock', 'CasaRsNativeCache.lock')
    }


def recipe(dataset):
    reader = table()
    assert reader.open(str(dataset / 'SOURCE'))
    key = reader.getkeyword('definedmodel_field_0')
    record = reader.getcell('SOURCE_MODEL', reader.getkeyword(key))
    assert reader.done()
    return record['ft_0']['container']


def corrected_or_data(dataset):
    reader = table()
    assert reader.open(str(dataset))
    prior = reader.getcol('CORRECTED_DATA' if 'CORRECTED_DATA' in reader.colnames() else 'DATA')
    assert reader.done()
    return prior


def read_virtual(dataset, *, name='nPBWProjectFT'):
    from casars.tools import ms

    entry = recipe(dataset)
    assert entry['name'] == name
    grid = Path(entry['diskimage'])
    tool = image()
    for path in (grid, grid / 'CASARS_LEGACY_PBW_KERNEL'):
        assert tool.open(str(path))
        assert np.iscomplexobj(tool.getchunk()) and tool.done()
    reader = table()
    assert reader.open(str(dataset))
    assert 'MODEL_DATA' not in reader.colnames()
    assert reader.done()
    prior = corrected_or_data(dataset)
    tool = ms()
    assert tool.open(str(dataset), nomodify=False)
    assert tool.msselect({'field': '0'})
    assert tool.uvsub() and tool.done()
    return prior - data(dataset, 'CORRECTED_DATA')


def reject_recipe(factory, dataset, reason):
    prior = corrected_or_data(dataset)
    subtract_refused(factory, dataset, reason)
    np.testing.assert_array_equal(corrected_or_data(dataset), prior)


def subtract_refused(factory, dataset, reason):
    reader = factory()
    assert reader.open(str(dataset), nomodify=False)
    try:
        reader.uvsub()
    except RuntimeError as error:
        assert reason in str(error), str(error)
    else:
        raise AssertionError('the reader accepted the unsupported virtual recipe')
    assert reader.done()


def setup_controls(root, dataset, model, cache, expected):
    from casars.tools import imager as native

    for label, keywords, arguments, changes in (
        ('uncorrected', {'correction': False}, ['--without-correction'], True),
        ('pb-limit', {'limit': .8}, ['--limit', '.8'], True),
        ('linear', {'interp': 'linear'}, ['--interp', 'linear'], False),
    ):
        pinned = root / f'{label}-pinned.ms'
        actual = root / f'{label}-native.ms'
        make_input(dataset, pinned, scratch=True)
        make_input(dataset, actual, scratch=True)
        subprocess.run([sys.executable, __file__, '--casa', str(pinned), str(model), str(cache),
                        *arguments], check=True)
        reference = data(pinned, 'MODEL_DATA')
        change = relative_rms(reference, expected)
        assert change > .02 if changes else change == 0.
        ft(native, actual, model, cache, scratch=True, **keywords)
        error = relative_rms(data(actual, 'MODEL_DATA'), reference)
        print(label, 'setup NRMS', error, 'change from default', change, flush=True)
        assert error < 1.e-3


def geometry_control(root, dataset, model, cache, *, pinned_only=False, machine='pbwproject'):
    root.mkdir(parents=True, exist_ok=True)
    pinned = root / 'geometry-pinned.ms'
    ready = root / 'geometry-pinned.ms.complete'
    times = data(dataset, 'TIME')
    first, second, last = np.unique(times)
    flags = data(dataset, 'FLAG')
    usable = (~data(dataset, 'FLAG_ROW') & ~np.any(flags[:, 0, :], axis=0)
              & (data(dataset, 'ANTENNA1') != data(dataset, 'ANTENNA2')))
    boundary = np.flatnonzero(usable & (times == first))[:5]
    rotation = np.flatnonzero(usable & (times == last))[0]
    assert len(boundary) == 5
    frequency = data(dataset / 'SPECTRAL_WINDOW', 'CHAN_FREQ')[0, 0]
    cutoff_m = .25 * 299792458. / (frequency * np.radians(15. / 3600.))
    if not ready.exists():
        make_input(dataset, pinned, scratch=True)
        writer = table()
        assert writer.open(str(pinned), nomodify=False)
        for column in ('TIME', 'TIME_CENTROID'):
            epochs = writer.getcol(column)
            epochs[times == second] += 1800.
            epochs[times == last] += 3600.
            assert writer.putcol(column, epochs)
        for row, w in zip(boundary, (cutoff_m - 2., cutoff_m + 2., 100., -1000., 0.),
                          strict=True):
            assert writer.putcell('UVW', int(row), [0., 0., w])
        assert writer.putcell('UVW', int(rotation), [0., 0., 0.])
        assert writer.done()
        subprocess.run([sys.executable, __file__, '--casa', str(pinned), str(model),
                        str(cache), '--machine', machine], check=True)
    expected = data(pinned, 'MODEL_DATA')
    assert np.all(np.abs(expected[:, 0, boundary[[0, 2, 4]]]) > 1.)
    assert not np.any(expected[:, 0, boundary[[1, 3]]])
    rotation_change = relative_rms(expected[:, 0, rotation], expected[:, 0, boundary[4]])
    print('legacy PB/W W boundary', np.abs(expected[0, 0, boundary]),
          'late-epoch rotation change', rotation_change, flush=True)
    assert rotation_change > .001
    if pinned_only:
        return
    from casars.tools import imager as native

    for label, scratch in (('scratch', True), ('saved', False)):
        target = root / f'geometry-{label}.ms'
        make_input(pinned, target, scratch=scratch)
        ft(native, target, model, cache, scratch=scratch, machine=machine)
        name = 'PBMosaicFT' if machine == 'pbmosaic' else 'nPBWProjectFT'
        actual = data(target, 'MODEL_DATA') if scratch else read_virtual(target, name=name)
        assert not np.any(actual[:, 0, boundary[[1, 3]]])
        errors = [relative_rms(actual, expected),
                  relative_rms(actual[:, 0, rotation], expected[:, 0, rotation])]
        print(label, 'legacy PB/W W and PA NRMS', errors, flush=True)
        assert max(errors) < 1.e-3


def invalid_cache_control(root, dataset, model, cache):
    from casars.tools import imager as native

    bad_cache = root / 'short-kernel-cache'
    shutil.copytree(cache, bad_cache)
    kernel = bad_cache / 'CF0_0'
    reader = image()
    assert reader.open(str(kernel))
    cells = reader.getchunk()[:121, :121, :, :]
    coordinates = reader.coordsys()
    record = coordinates.torecord()
    assert coordinates.done() and reader.done()
    kernel.rename(bad_cache / 'original-kernel')
    assert reader.fromarray(str(kernel), pixels=cells, csys=record) and reader.done()
    scratch = root / 'short-kernel-scratch.ms'
    make_input(dataset, scratch, scratch=True)
    seed = imager()
    assert seed.open(str(scratch), usescratch=True) and seed.done()
    before = data(scratch, 'MODEL_DATA')
    ft(native, scratch, model, bad_cache, scratch=True, accepts=False)
    np.testing.assert_array_equal(data(scratch, 'MODEL_DATA'), before)
    print('legacy PB/W rejected a short imported kernel without changing MODEL_DATA', flush=True)


def mosaic_control(root, dataset, model, cache):
    from casars.tools import imager as native, ms as native_ms

    root.mkdir(parents=True)
    pinned, scratch, saved, cold, received, pinned_saved = (
        root / f'{label}.ms' for label in ('pinned', 'scratch', 'saved', 'cold', 'received', 'pinned-saved')
    )
    for path in (pinned, scratch, saved, cold, received, pinned_saved):
        make_input(dataset, path, scratch=path not in (saved, pinned_saved))
    warm = root / 'warm'
    shutil.copytree(cache, warm)
    before = cache_contents(warm)
    parameters = ['--machine', 'pbmosaic']
    subprocess.run([sys.executable, __file__, '--casa', str(pinned), str(model), str(warm),
                    *parameters], check=True)
    expected = data(pinned, 'MODEL_DATA')
    assert np.abs(expected).max() > 1. and np.abs(expected.imag).max() > .1
    ft(native, scratch, model, warm, scratch=True, machine='pbmosaic')
    ft(native, saved, model, warm, scratch=False, machine='pbmosaic')
    assert cache_contents(warm) == before
    subprocess.run([sys.executable, __file__, '--casa', str(pinned_saved), str(model), str(warm),
                    '--saved', *parameters], check=True)
    ft(native, cold, model, root / 'built', scratch=True, machine='pbmosaic')
    subprocess.run([sys.executable, __file__, '--casa', str(received), str(model),
                    str(root / 'built'), *parameters], check=True)
    warm.rename(root / 'unavailable-warm')
    products = (data(scratch, 'MODEL_DATA'), read_virtual(saved, name='PBMosaicFT'),
                data(cold, 'MODEL_DATA'), data(received, 'MODEL_DATA'))
    errors = [relative_rms(product, expected) for product in products]
    print('legacy pbmosaic', expected.size, 'complex samples per product', errors, flush=True)
    assert max(errors) < 1.e-3
    assert 'name' not in recipe(pinned_saved)
    pinned_refuses = root / 'pinned-refuses.ms'
    make_input(pinned_saved, pinned_refuses, scratch=False)
    for refused in (pinned_saved, pinned_refuses):
        reader = table()
        assert reader.open(str(refused))
        assert 'CORRECTED_DATA' not in reader.colnames()
        assert reader.done()
    reject_recipe(native_ms, pinned_saved, 'virtual model has no name string')
    subprocess.run([sys.executable, __file__, '--casa-reject', str(pinned_refuses),
                    'field name is unknown'], check=True)
    np.testing.assert_array_equal(data(pinned_saved, 'CORRECTED_DATA'),
                                  data(pinned_refuses, 'CORRECTED_DATA'))
    subprocess.run([sys.executable, __file__, '--casa-reject', str(saved), 'Unsupported FTMachine'],
                   check=True)
    mixed_refusal_control(root, dataset, model, root / 'unavailable-warm')


def mixed_refusal_control(root, dataset, model, cache):
    from casatools import ms as casa_ms
    from casars.tools import ms as native_ms

    initial, pinned, native = (root / f'mixed-{label}.ms'
                              for label in ('initial', 'pinned', 'native'))
    make_input(dataset, initial, scratch=False)
    writer = table()
    assert writer.open(str(initial), nomodify=False)
    times = writer.getcol('TIME')
    later = times != times.min()
    assert later.any() and not later.all()
    assert writer.putcol('FIELD_ID', later.astype('int32')) and writer.done()
    for field, machine in ((0, 'ft'), (1, 'pbmosaic')):
        subprocess.run([sys.executable, __file__, '--casa', str(initial), str(model),
                        str(cache), '--saved', '--machine', machine, '--field', str(field)],
                       check=True)
    for target in (pinned, native):
        make_input(initial, target, scratch=False)
        reader = table()
        assert reader.open(str(target))
        assert 'CORRECTED_DATA' not in reader.colnames()
        assert reader.done()
    observed = data(initial, 'DATA')
    subtract_refused(casa_ms, pinned, 'field name is unknown')
    subtract_refused(native_ms, native, 'virtual model has no name string')
    expected = data(pinned, 'CORRECTED_DATA')
    actual = data(native, 'CORRECTED_DATA')
    earlier = (..., ~later)
    np.testing.assert_array_equal(expected[..., later], observed[..., later])
    np.testing.assert_array_equal(actual[..., later], observed[..., later])
    assert np.abs(expected[earlier] - observed[earlier]).max() > 1.
    error = relative_rms(actual[earlier] - observed[earlier], expected[earlier] - observed[earlier])
    print('uvsub earlier successful subtraction, later refused model', 'NRMS', error, flush=True)
    assert error < 1.e-3


def run(root, dataset, model, cache):
    from casars.tools import imager as native, ms as native_ms

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    pinned, scratch, stored = (root / f'{label}.ms' for label in ('pinned', 'scratch', 'saved'))
    for path, use_scratch in ((pinned, True), (scratch, True), (stored, False)):
        make_input(dataset, path, scratch=use_scratch)
    pinned_cache = root / 'pinned-cache'
    native_cache = root / 'native-cache'
    shutil.copytree(cache, pinned_cache)
    shutil.copytree(cache, native_cache)
    child = subprocess.run([sys.executable, __file__, '--casa', str(pinned), str(model),
                            str(pinned_cache)], check=False)
    assert child.returncode == 0 and (root / 'pinned.ms.complete').exists()
    expected = data(pinned, 'MODEL_DATA')
    assert np.abs(expected).max() > 1. and np.abs(expected.imag).max() > .1
    assert np.any(np.max(np.abs(expected), axis=(0, 2)) == 0.)
    auto = data(pinned, 'ANTENNA1') == data(pinned, 'ANTENNA2')
    assert np.abs(expected[..., auto]).max() > 1.
    assert np.abs(expected[..., ~auto]).max() > 1.
    before = cache_contents(native_cache)
    ft(native, scratch, model, native_cache, scratch=True)
    actual = data(scratch, 'MODEL_DATA')
    ft(native, stored, model, native_cache, scratch=False)
    assert cache_contents(native_cache) == before
    selected = root / 'saved-unmapped-channel.ms'
    make_input(dataset, selected, scratch=False)
    ft(native, selected, model, native_cache, scratch=False, spw='0:1')
    native_cache.rename(root / 'unavailable-native-cache')
    reopened = read_virtual(stored)
    errors = [relative_rms(result, expected) for result in (actual, reopened)]
    print('legacy pbwproject', expected.size, 'complex samples per product', errors, flush=True)
    assert max(errors) < 1.e-3
    assert not np.any(read_virtual(selected))
    built_cache = root / 'built-cache'
    cold = root / 'native-cold.ms'
    make_input(dataset, cold, scratch=True)
    ft(native, cold, model, built_cache, scratch=True)
    assert (built_cache / 'aux.dat').is_file()
    cold_error = relative_rms(data(cold, 'MODEL_DATA'), expected)
    casa_warm = root / 'casa-reads-native-cache.ms'
    make_input(dataset, casa_warm, scratch=True)
    subprocess.run([sys.executable, __file__, '--casa', str(casa_warm), str(model),
                    str(built_cache)], check=True)
    cache_error = relative_rms(data(casa_warm, 'MODEL_DATA'), expected)
    print('legacy pbwproject generated cache', expected.size, 'complex samples per product',
          [cold_error, cache_error], flush=True)
    assert max(cold_error, cache_error) < 1.e-3
    setup_controls(root, dataset, model, pinned_cache, expected)
    casa_saved = root / 'casa-saved.ms'
    make_input(dataset, casa_saved, scratch=False)
    subprocess.run([sys.executable, __file__, '--casa', str(casa_saved), str(model),
                    str(pinned_cache), '--saved'], check=True)
    assert (root / 'casa-saved.ms.complete').exists() and 'name' not in recipe(casa_saved)
    for path, message in ((casa_saved, 'field name is unknown'),
                          (stored, 'Unsupported FTMachine')):
        subprocess.run([sys.executable, __file__, '--casa-reject', str(path), message],
                       check=True)
    reject_recipe(native_ms, casa_saved, 'virtual model has no name string')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--dataset', type=Path)
    parser.add_argument('--model', type=Path)
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--casa', nargs=3)
    parser.add_argument('--saved', action='store_true')
    parser.add_argument('--without-correction', action='store_true')
    parser.add_argument('--limit', type=float, default=.05)
    parser.add_argument('--interp', default='nearest')
    parser.add_argument('--casa-reject', nargs=2)
    parser.add_argument('--geometry', choices=('both', 'pinned'))
    parser.add_argument('--invalid-cache', action='store_true')
    parser.add_argument('--mosaic', action='store_true')
    parser.add_argument('--machine', choices=('ft', 'pbwproject', 'pbmosaic'), default='pbwproject')
    parser.add_argument('--field', type=int, default=0)
    args = parser.parse_args()
    if args.casa:
        ft(imager, *(Path(path) for path in args.casa), scratch=not args.saved,
           correction=not args.without_correction, limit=args.limit, interp=args.interp,
           machine=args.machine, field=args.field)
    elif args.casa_reject:
        from casatools import ms

        path, reason = args.casa_reject
        reject_recipe(ms, Path(path), reason)
    elif args.geometry:
        geometry_control(*(path.resolve() for path in (args.work, args.dataset, args.model,
                                                      args.cache)),
                         pinned_only=args.geometry == 'pinned', machine=args.machine)
    elif args.invalid_cache:
        args.work.mkdir(parents=True)
        invalid_cache_control(*(path.resolve() for path in (args.work, args.dataset, args.model,
                                                           args.cache)))
    elif args.mosaic:
        mosaic_control(*(path.resolve() for path in (args.work, args.dataset, args.model, args.cache)))
    else:
        run(*(path.resolve() for path in (args.work, args.dataset, args.model, args.cache)))


if __name__ == '__main__':
    main()
