#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy AW prediction and cache exchange against the pinned CASA imager."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casars.tools import imager, ms
from casatools import image, imager as casa_imager, ms as casa_ms, table
import numpy as np

from imager_legacy_pbw_oracle import (
    cache_contents, configured_imager, corrected_or_data, data, make_input, recipe, reject_recipe,
    relative_rms,
)


def predict(dataset, model, cache, machine, correction, *, scratch=True, limit=.05,
            accepts=True, factory=imager, spw='0:0', wprojplanes=1, empty_model=None, **terms):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=0, spw=spw)
    definition = dict(nx=256, ny=256, cellx='15arcsec', celly='15arcsec',
                      phasecenter=0, stokes='IQUV', outframe='TOPO')
    assert tool.defineimage(**definition)
    assert tool.setoptions(ftmachine=machine, wprojplanes=wprojplanes, numthreads=1,
                           cfcachedirname=str(cache), applypointingoffsets=False,
                           dopbgriddingcorrections=correction, pblimit=limit, **terms)
    if empty_model is not None:
        assert tool.ft(str(empty_model)) and tool.defineimage(**definition)
    assert tool.ft(str(model)) is accepts
    assert tool.done()


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
    tile = next(owned_cache.glob('CFS_*/table.f*_TSM*'))
    original_tile = owned_cache / 'external-tile'
    tile.rename(original_tile)
    tile.symlink_to(original_tile.resolve())
    predict(target, model, owned_cache, machine, True, scratch=False)
    record = recipe(target)
    assert record['name'] == 'AWProjectWBFT'
    saved_image = Path(record['diskimage'])
    held = saved_image / 'CASARS_LEGACY_AW_CACHE'
    held_tile = held / tile.relative_to(owned_cache)
    assert held_tile.is_file() and not held_tile.is_symlink()
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


def autocorrelation_control(root, dataset, model, cache):
    pinned, actual = (root / f'autos-{mode}.ms' for mode in ('pinned', 'native'))
    reader = table()
    assert reader.open(str(dataset))
    selected = reader.query('ANTENNA1 == ANTENNA2')
    copied = selected.copy(str(pinned), deep=True, valuecopy=True)
    assert copied.done() and selected.done() and reader.done()
    make_input(pinned, actual, scratch=True)
    predict(pinned, model, cache, 'awproject', False, factory=casa_imager)
    predict(actual, model, cache, 'awproject', False)
    expected = data(pinned, 'MODEL_DATA')
    assert np.abs(expected[:, 0, :]).max() > 1.
    error = relative_rms(data(actual, 'MODEL_DATA')[:, 0, :], expected[:, 0, :])
    print('autocorrelation-only selected channel NRMS', error, flush=True)
    assert error < 1.e-3


def corrupt_kernel(cache):
    tool = image()
    assert tool.open(str(next(cache.glob('CFS_*'))))
    pixels = tool.getchunk()
    pixels[0, 0, 0, 0] = complex('nan')
    assert tool.putchunk(pixels) and tool.done()


def malformed_control(root, dataset, model, cache):
    bad = root / 'bad-cache'
    shutil.copytree(cache, bad)
    corrupt_kernel(bad)
    for scratch in (False, True):
        target = root / f'bad-input-{scratch}.ms'
        make_input(dataset, target, scratch=scratch)
        if scratch:
            seed = casa_imager()
            assert seed.open(str(target), usescratch=True) and seed.done()
            prior = data(target, 'MODEL_DATA')
        predict(target, model, bad, 'awproject', False, scratch=scratch, accepts=False)
        if scratch:
            np.testing.assert_array_equal(data(target, 'MODEL_DATA'), prior)
        reader = table()
        assert reader.open(str(target / 'SOURCE'))
        assert 'definedmodel_field_0' not in reader.keywordnames() and reader.done()
    saved = root / 'bad-owned-kernel.ms'
    make_input(dataset, saved, scratch=False)
    predict(saved, model, cache, 'awproject', False, scratch=False)
    corrupt_kernel(Path(recipe(saved)['diskimage']) / 'CASARS_LEGACY_AW_CACHE')
    reject_recipe(ms, saved, 'cell pixels must be finite')
    print('invalid imported and saved kernel pixels refused without changing visibilities', flush=True)


def compare_cold(root, dataset, model):
    assert casatasks.version_string() == '6.7.6.14'
    results = {}
    for machine in ('awproject', 'mawproject'):
        for label, terms in (
            ('wideband', {'wbawp': True}),
            ('mterm-off', {'wbawp': True, 'mterm': False}),
            ('ps-off', {'wbawp': True, 'mterm': False, 'psterm': False}),
        ):
            case = f'{machine}-{label}'
            with TemporaryDirectory(dir=root) as attempt:
                results[case] = cold_case(Path(attempt), dataset, model, machine, terms)
            print(case, results[case], flush=True)
            assert max(results[case].values()) < 1.e-3
    with TemporaryDirectory(dir=root) as control:
        results['stepped'] = stepped_range(Path(control), dataset, model)
    for selector, stops in (('0:0;3', (0, 3)), ('0:3;0', (3, 0))):
        with TemporaryDirectory(dir=root) as control:
            results[selector] = disjoint_ranges(Path(control), dataset, model, selector, stops)
    with TemporaryDirectory(dir=root) as control:
        refuses_unsupported_cells(Path(control), dataset, model)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


def compare_cold_evla(root, dataset, model):
    from imager_evla_squint_oracle import telescope_input

    assert casatasks.version_string() == '6.7.6.14'
    results = {}
    with TemporaryDirectory(dir=root) as temporary:
        work = Path(temporary)
        evla = work / 'evla.ms'
        telescope_input(dataset, evla, 'EVLA')
        editor = table()
        assert editor.open(str(evla / 'OBSERVATION'), nomodify=False)
        assert editor.nrows() == 1
        assert editor.addrows(1)
        for column in editor.colnames():
            if editor.iscelldefined(column, 0):
                assert editor.putcell(column, 1, editor.getcell(column, 0))
        assert editor.done()
        assert editor.open(str(evla), nomodify=False)
        ids = editor.getcol('OBSERVATION_ID')
        ids[len(ids) // 2:] = 1
        assert editor.putcol('OBSERVATION_ID', ids) and editor.done()
        setup = casa_imager()
        assert setup.open(str(evla), usescratch=True) and setup.done()
        factories = (('casa', casa_imager), ('native', imager))
        for name, factory in factories:
            mixed, cache = work / f'{name}-mixed.ms', work / f'{name}-mixed-cache'
            make_input(evla, mixed, scratch=True)
            assert editor.open(str(mixed / 'OBSERVATION'), nomodify=False)
            assert editor.putcell('TELESCOPE_NAME', 1, 'VLA') and editor.done()
            before = data(mixed, 'MODEL_DATA')
            predict(mixed, model, cache, 'awproject', False, accepts=False,
                    factory=factory, wbawp=True)
            assert not list(cache.glob('*CFS_*.im'))
            np.testing.assert_array_equal(data(mixed, 'MODEL_DATA'), before)
        print('Mixed EVLA and VLA observations refused before writing cells or model data', flush=True)
        for machine in ('awproject', 'mawproject'):
            case = work / machine
            case.mkdir()
            results[machine] = cold_case(case, evla, model, machine, {'wbawp': True})
            print('EVLA', machine, results[machine], flush=True)
            assert all(np.isfinite(error) and error < 1.e-3 for error in results[machine].values())
        warm_cache = work / 'awproject' / 'casa-cache'
        before = cache_contents(warm_cache)
        outputs = {}
        for name, factory in factories:
            mixed = work / f'{name}-mixed.ms'
            predict(mixed, model, warm_cache, 'awproject', False, factory=factory, wbawp=True)
            outputs[name] = data(mixed, 'MODEL_DATA')
        assert cache_contents(warm_cache) == before
        np.testing.assert_array_equal(outputs['native'][:, 1:, :], outputs['casa'][:, 1:, :])
        error = relative_rms(outputs['native'][:, 0, :], outputs['casa'][:, 0, :])
        assert np.isfinite(error) and error < 1.e-3
        results['mixed-warm'] = error
        print('Mixed EVLA and VLA observations reused CASA cells', error, flush=True)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


def compare_cold_multi_w(root, dataset, model, *, reference=None):
    # Put rows on both sides of the legacy truncation boundaries at both W signs.
    source = root / 'four-w-planes.ms'
    make_input(dataset, source, scratch=True)
    frequency = data(source / 'SPECTRAL_WINDOW', 'CHAN_FREQ')[0, 0]
    scale = 9.0 / (.25 / np.deg2rad(15. / 3600.))
    boundaries = np.array([.1, .55, 1.1, 1.55, 2.1, 2.55, 3.1, 3.55, 4.1])
    editor = table()
    assert editor.open(str(source), nomodify=False)
    uvw = editor.getcol('UVW')
    rows = np.arange(uvw.shape[1])
    uvw[2] = boundaries[rows % len(boundaries)] ** 2 / scale * 299792458. / frequency
    uvw[2, rows % 2 == 0] *= -1.
    assert editor.putcol('UVW', uvw) and editor.done()

    if reference is None:
        pinned, casa_cache = root / 'pinned.ms', root / 'casa-cache'
        make_input(source, pinned, scratch=True)
        predict(pinned, model, casa_cache, 'awproject', False,
                wprojplanes=4, wbawp=True, factory=casa_imager)
    else:
        pinned, casa_cache = reference
        np.testing.assert_array_equal(data(source, 'UVW'), data(pinned, 'UVW'))
    native, native_cache = root / 'native.ms', root / 'native-cache'
    make_input(source, native, scratch=True)
    predict(native, model, native_cache, 'awproject', False, wprojplanes=4, wbawp=True)
    expected = data(pinned, 'MODEL_DATA')
    errors = {'cells': compare_cells(native_cache, casa_cache, count=16)}
    results = {'native': native}
    for name, cache, factory in (('casa-reads-native', native_cache, casa_imager),
                                 ('native-reads-casa', casa_cache, imager)):
        target = root / f'{name}.ms'
        make_input(source, target, scratch=True)
        before = cache_contents(cache)
        predict(target, model, cache, 'mawproject', False, wprojplanes=4, wbawp=True, factory=factory)
        assert cache_contents(cache) == before
        results[name] = target
    for name, target in results.items():
        actual = data(target, 'MODEL_DATA')
        np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
        errors[name] = relative_rms(actual[:, 0, :], expected[:, 0, :])

    saved = root / 'saved.ms'
    make_input(source, saved, scratch=False)
    predict(saved, model, native_cache, 'awproject', False, scratch=False, wprojplanes=4, wbawp=True)
    record = recipe(saved)
    assert record['name'] == 'AWProjectWBFT'
    held = Path(record['diskimage']) / 'CASARS_LEGACY_AW_CACHE'
    errors['saved-kernels'] = compare_cells(held, casa_cache, count=16)
    native_cache.rename(root / 'moved-native-cache')
    prior = corrected_or_data(saved)
    tool = ms()
    assert tool.open(str(saved), nomodify=False)
    assert tool.msselect({'field': '0'}) and tool.uvsub() and tool.done()
    actual = prior - data(saved, 'CORRECTED_DATA')
    assert not np.any(actual[:, 1:, :])
    errors['saved-replay'] = relative_rms(actual[:, 0, :], expected[:, 0, :])
    errors.update(frame_converted_w_boundary(root, source, model, casa_cache, scale))
    print('multiple W planes', errors, flush=True)
    assert all(np.isfinite(error) and error < 1.e-3 for error in errors.values()), errors


def frame_converted_w_boundary(root, dataset, model, cache, scale):
    source, wide_model, tagged = (root / name for name in
                                  ('topo-source.ms', 'wide-model.im', 'tagged-cache'))
    make_input(dataset, source, scratch=True)
    shutil.copytree(model, wide_model)
    shutil.copytree(cache, tagged)
    editor = table()
    assert editor.open(str(source / 'SPECTRAL_WINDOW'), nomodify=False)
    assert editor.putcell('MEAS_FREQ_REF', 0, 5) and editor.done()  # TOPO
    frequency = data(source / 'SPECTRAL_WINDOW', 'CHAN_FREQ')[0, 0]
    assert editor.open(str(source), nomodify=False)
    uvw = editor.getcol('UVW')
    offsets = np.array([-3.e-4, -3.e-6, -3.e-7, 3.e-7, 1.e-6, 3.e-6, 1.e-5, 3.e-4])
    uvw[2] = (2. + np.resize(offsets, uvw.shape[1])) ** 2 / scale * 299792458. / frequency
    assert editor.putcol('UVW', uvw) and editor.done()
    tool = image()
    assert tool.open(str(wide_model))
    coordinates = tool.coordsys()
    increment = coordinates.increment()['numeric']
    increment[-1] = 1.e7
    assert coordinates.setincrement(increment) and tool.setcoordsys(coordinates.torecord())
    assert coordinates.done() and tool.done()
    # A phase tag in plane 1 makes choosing the wrong side of the
    # boundary observable, independently of the small change in W phase.
    cells = sorted(tagged.glob('CFS_0_0_CF_0_1_*.im'))
    assert len(cells) == 2
    for path in cells:
        assert tool.open(str(path))
        pixels = tool.getchunk()
        phase = np.exp(.45j * np.arange(pixels.shape[0], dtype=np.float32)).reshape((-1, 1, 1, 1))
        assert tool.putchunk(pixels * phase) and tool.done()

    outputs = {}
    for name, factory in (('casa', casa_imager), ('native', imager)):
        target = root / f'{name}-topo.ms'
        make_input(source, target, scratch=True)
        predict(target, wide_model, tagged, 'awproject', False, factory=factory,
                wprojplanes=4, wbawp=True, empty_model=root / f'{name}-blank.im')
        outputs[name] = data(target, 'MODEL_DATA')
    reference = outputs['casa'][:, 0, :]
    above = np.resize(offsets, reference.shape[1]) > 0.
    assert np.max(np.abs(reference[:, above])) > 10.
    assert np.max(np.abs(reference[:, ~above])) < 5.
    np.testing.assert_array_equal(outputs['native'][:, 1:, :], outputs['casa'][:, 1:, :])
    errors = {'native-frequency-scratch': relative_rms(outputs['native'][:, 0, :], reference)}

    saved = root / 'saved-topo.ms'
    make_input(source, saved, scratch=False)
    predict(saved, wide_model, tagged, 'awproject', False, scratch=False, wprojplanes=4, wbawp=True)
    prior = corrected_or_data(saved)
    reader = ms()
    assert reader.open(str(saved), nomodify=False)
    assert reader.msselect({'field': '0'}) and reader.uvsub() and reader.done()
    replay = prior - data(saved, 'CORRECTED_DATA')
    assert not np.any(replay[:, 1:, :])
    errors['native-frequency-saved'] = relative_rms(replay[:, 0, :], reference)
    return errors


def cold_case(root, dataset, model, machine, terms, *, reference=None):
    native, reopened, imported = (root / f'{name}.ms' for name in (
        'native', 'casa-reopened', 'native-imported',
    ))
    for target in (native, reopened, imported):
        make_input(dataset, target, scratch=True)
    native_cache = root / 'native-cache'
    if reference is None:
        pinned, casa_cache = root / 'pinned.ms', root / 'casa-cache'
        make_input(dataset, pinned, scratch=True)
        predict(pinned, model, casa_cache, machine, False, factory=casa_imager, **terms)
    else:
        pinned, casa_cache = reference / 'fixture.ms', reference / 'cache'
    expected = data(pinned, 'MODEL_DATA')
    assert np.abs(expected[:, 0, :]).max() > 1.

    interrupted = native_cache / '.casars-legacy-aw-interrupted'
    interrupted.mkdir(parents=True)
    (interrupted / 'preparing').touch()
    cell = 'CFS_0_0_CF_0_0_1.im'
    (interrupted / 'promoting').write_text(cell)
    (native_cache / cell).mkdir()
    (native_cache / cell / '.casars-legacy-aw-owner').write_text(interrupted.name)
    preparing = native_cache / '.casars-legacy-aw-preparing'
    preparing.mkdir()
    (preparing / 'preparing').touch()
    (preparing / cell).mkdir()
    (preparing / 'journal.new').touch()
    unrelated = native_cache / '.casars-legacy-aw-unowned'
    unrelated.mkdir()
    (unrelated / 'note').write_text('preserve')
    predict(native, model, native_cache, machine, False, **terms)
    assert not interrupted.exists() and not preparing.exists()
    assert (unrelated / 'note').read_text() == 'preserve'

    cache_error = compare_cells(native_cache, casa_cache)
    unchanged = cache_contents(native_cache)
    predict(reopened, model, native_cache, machine, False, factory=casa_imager, **terms)
    predict(imported, model, casa_cache, machine, False, **terms)
    assert cache_contents(native_cache) == unchanged
    errors = {'kernel_nrms': cache_error}
    for name, target in (('scratch', native), ('casa_reopened', reopened), ('native_imported', imported)):
        actual = data(target, 'MODEL_DATA')
        np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
        errors[name] = relative_rms(actual[:, 0, :], expected[:, 0, :])

    # One cold saved run per machine checks replay and C++ readback of the
    # copied kernel products after the independently generated cache moves.
    if len(terms) == 1:
        errors['replacement'] = preserve_replaced_cell(root, dataset, model, casa_cache, machine, terms, expected)
        saved, saved_cache = root / 'saved.ms', root / 'saved-cache'
        make_input(dataset, saved, scratch=False)
        predict(saved, model, saved_cache, machine, False, scratch=False, **terms)
        record = recipe(saved)
        assert record['name'] == 'AWProjectWBFT'
        grid = Path(record['diskimage'])
        assert complex_image(grid).shape == (256, 256, 4, 1)
        errors['saved_kernels'] = compare_cells(grid / 'CASARS_LEGACY_AW_CACHE', casa_cache)
        saved_cache.rename(root / 'moved-cache')
        prior = corrected_or_data(saved)
        tool = ms()
        assert tool.open(str(saved), nomodify=False)
        assert tool.msselect({'field': '0'}) and tool.uvsub() and tool.done()
        replayed = prior - data(saved, 'CORRECTED_DATA')
        assert not np.any(replayed[:, 1:, :])
        errors['saved_replay'] = relative_rms(replayed[:, 0, :], expected[:, 0, :])
    return errors


def preserve_replaced_cell(root, dataset, model, source_cache, machine, terms, expected):
    cache, target = root / 'replaced-cache', root / 'replaced.ms'
    shutil.copytree(source_cache, cache)
    cell = 'CFS_0_0_CF_0_0_1.im'
    (cache / cell / 'new-producer').write_text('preserve')
    before = cache_contents(cache)
    stage = cache / '.casars-legacy-aw-obsolete'
    stage.mkdir()
    (stage / 'preparing').touch()
    (stage / 'promoting').write_text(cell)
    make_input(dataset, target, scratch=True)
    predict(target, model, cache, machine, False, **terms)
    assert cache_contents(cache) == before
    actual = data(target, 'MODEL_DATA')
    np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
    return relative_rms(actual[:, 0, :], expected[:, 0, :])


def compare_cells(actual, expected, *, count=4):
    names = sorted(path.name for path in expected.glob('*CFS_*.im'))
    assert len(names) == count
    assert sorted(path.name for path in actual.glob('*CFS_*.im')) == names
    errors = []
    for name in names:
        def read(path):
            tool = image()
            assert tool.open(str(path))
            pixels, metadata = tool.getchunk(), tool.miscinfo()
            assert tool.done()
            reader = table()
            assert reader.open(str(path))
            coordinates = reader.getkeyword('coords')
            assert reader.done()
            return pixels, metadata, coordinates

        got, got_meta, got_coords = read(actual / name)
        wanted, wanted_meta, wanted_coords = read(expected / name)
        assert got.shape == wanted.shape, name
        for key in ('Xsupport', 'Ysupport', 'Sampling', 'MuellerElement', 'WValue', 'WIncr',
                    'ConjFreq', 'ConjPoln', 'TelescopeName', 'BandName'):
            assert got_meta[key] == wanted_meta[key], (name, key)
        compare_cell_coordinates(got_coords, wanted_coords)
        error = relative_rms(got, wanted)
        assert np.isfinite(error), name
        errors.append(error)
    return max(errors)


def compare_cell_coordinates(actual, expected):
    for key in ('axes', 'units'):
        np.testing.assert_array_equal(actual['linear0'][key], expected['linear0'][key])
    for key in ('crpix', 'crval', 'cdelt', 'pc'):
        np.testing.assert_allclose(actual['linear0'][key], expected['linear0'][key], rtol=1.e-8)
    np.testing.assert_array_equal(actual['stokes1']['stokes'], expected['stokes1']['stokes'])
    lhs, rhs = actual['spectral2'], expected['spectral2']
    for key in ('system', 'unit'):
        assert lhs[key] == rhs[key], key
    np.testing.assert_allclose(lhs['restfreq'], rhs['restfreq'], rtol=1.e-13)
    assert lhs['wcs']['ctype'] == rhs['wcs']['ctype']
    for key in ('crpix', 'crval', 'cdelt', 'pc'):
        np.testing.assert_allclose(lhs['wcs'][key], rhs['wcs'][key], rtol=1.e-13)


def stepped_range(root, dataset, model, *, reference=None):
    selector = '0:0~5^2'
    if reference is None:
        pinned, casa_cache = root / 'pinned.ms', root / 'casa-cache'
        make_input(dataset, pinned, scratch=True)
        predict(pinned, model, casa_cache, 'awproject', False, factory=casa_imager,
                spw=selector, wbawp=True)
    else:
        pinned, casa_cache = reference / 'fixture.ms', reference / 'cache'
    native, native_cache = root / 'native.ms', root / 'native-cache'
    make_input(dataset, native, scratch=True)
    predict(native, model, native_cache, 'awproject', False, spw=selector, wbawp=True)
    wanted, got = data(pinned, 'MODEL_DATA'), data(native, 'MODEL_DATA')
    indices = [0, 2, 4]
    untouched = np.ones(wanted.shape[1], dtype=bool)
    untouched[indices] = False
    np.testing.assert_array_equal(got[:, untouched, :], wanted[:, untouched, :])
    reference_hz = data(dataset / 'SPECTRAL_WINDOW', 'CHAN_FREQ')[5, 0]
    reader = table()
    assert reader.open(str(native_cache / 'CFS_0_0_CF_0_0_0.im'))
    actual_hz = reader.getkeyword('coords')['spectral2']['wcs']['crval']
    assert reader.done() and actual_hz == reference_hz
    errors = {
        'cells': compare_cells(native_cache, casa_cache),
        'prediction': relative_rms(got[:, indices, :], wanted[:, indices, :]),
    }
    print('stepped 0:0~5^2 uses stop channel 5', errors, flush=True)
    assert max(errors.values()) < 1.e-3
    return errors


def disjoint_ranges(root, dataset, model, selector, stops, *, reference=None):
    if reference is None:
        pinned, casa_cache = root / 'pinned.ms', root / 'casa-cache'
        make_input(dataset, pinned, scratch=True)
        predict(pinned, model, casa_cache, 'awproject', False, factory=casa_imager,
                spw=selector, wbawp=True)
    else:
        pinned, casa_cache = reference / 'fixture.ms', reference / 'cache'
    expected = data(pinned, 'MODEL_DATA')
    native, native_cache = root / 'native.ms', root / 'native-cache'
    make_input(dataset, native, scratch=True)
    predict(native, model, native_cache, 'awproject', False, spw=selector, wbawp=True)
    errors = {'cache': compare_cells(native_cache, casa_cache, count=8)}
    frequencies = data(dataset / 'SPECTRAL_WINDOW', 'CHAN_FREQ')[:, 0]
    for group, stop in enumerate(stops):
        reader = table()
        assert reader.open(str(native_cache / f'CFS_0_0_CF_{group}_0_0.im'))
        assert reader.getkeyword('coords')['spectral2']['wcs']['crval'] == frequencies[stop]
        assert reader.done()
    reopened = root / 'casa-reopened.ms'
    make_input(dataset, reopened, scratch=True)
    cached = cache_contents(native_cache)
    predict(reopened, model, native_cache, 'awproject', False, factory=casa_imager,
            spw=selector, wbawp=True)
    assert cache_contents(native_cache) == cached
    for name, target in (('native', native), ('casa_reopened', reopened)):
        actual = data(target, 'MODEL_DATA')
        np.testing.assert_array_equal(actual[:, 4:, :], expected[:, 4:, :])
        errors[name] = relative_rms(actual[:, :4, :], expected[:, :4, :])

    errors.update(disjoint_kernel_routing(root, dataset, model, native_cache, selector,
                                          data(reopened, 'MODEL_DATA')))

    if stops == (0, 3):
        saved, saved_cache = root / 'saved.ms', root / 'saved-cache'
        make_input(dataset, saved, scratch=False)
        predict(saved, model, saved_cache, 'awproject', False, scratch=False, spw=selector, wbawp=True)
        entry = recipe(saved)
        assert entry['name'] == 'AWProjectWBFT'
        assert tuple(entry['casars_legacy_aw_frequency_map'][:3]) == (0, 1, 1)
        assert compare_cells(Path(entry['diskimage']) / 'CASARS_LEGACY_AW_CACHE', casa_cache, count=8) < 1.e-3
        saved_cache.rename(root / 'unavailable-cache')
        before = corrected_or_data(saved)
        tool = ms()
        assert tool.open(str(saved), nomodify=False)
        assert tool.msselect({'field': '0'}) and tool.uvsub() and tool.done()
        replayed = before - data(saved, 'CORRECTED_DATA')
        assert not np.any(replayed[:, 4:, :])
        errors['saved'] = relative_rms(replayed[:, :4, :], expected[:, :4, :])
    print(selector, 'independent cache and prediction NRMS', errors, flush=True)
    assert all(np.isfinite(error) and error < 1.e-3 for error in errors.values()), errors
    return errors


def disjoint_kernel_routing(root, dataset, model, cache, selector, unchanged):
    errors = {}
    for index in (0, 1):
        changed = root / f'changed-cf-{index}'
        shutil.copytree(cache, changed)
        cells = sorted(changed.glob(f'CFS_*_CF_{index}_0_*.im'))
        assert len(cells) == 2
        for path in cells:
            tool = image()
            assert tool.open(str(path))
            pixels = tool.getchunk()
            u = np.arange(pixels.shape[0], dtype=np.float32)
            phase = np.exp(.6j * (u - u.mean())).reshape((-1, 1, 1, 1))
            # A scalar amplitude would cancel in the normalized degrid.
            assert tool.putchunk(pixels * phase) and tool.done()
        predictions = []
        for name, factory in ('casa', casa_imager), ('native', imager):
            target = root / f'changed-cf-{index}-{name}.ms'
            make_input(dataset, target, scratch=True)
            predict(target, model, changed, 'awproject', False, factory=factory,
                    spw=selector, wbawp=True)
            predictions.append(data(target, 'MODEL_DATA'))
        pinned, native = predictions
        if index == 0:
            np.testing.assert_array_equal(pinned, unchanged)
        else:
            assert relative_rms(pinned[:, 0, :], unchanged[:, 0, :]) > .01
            np.testing.assert_array_equal(pinned[:, 1:, :], unchanged[:, 1:, :])
        errors[f'changed_group_{index}'] = relative_rms(native[:, 0, :], pinned[:, 0, :])
        np.testing.assert_array_equal(native[:, 1:, :], pinned[:, 1:, :])
    return errors


def refuses_unsupported_cells(root, dataset, model):
    for label in ('cross-hand', 's-band'):
        target, cache = root / f'{label}.ms', root / f'{label}-cache'
        make_input(dataset, target, scratch=True)
        if label == 's-band':
            editor = table()
            assert editor.open(str(target / 'SPECTRAL_WINDOW'), nomodify=False)
            for column in ('CHAN_FREQ', 'REF_FREQUENCY'):
                assert editor.putcell(column, 0, editor.getcell(column, 0) + 1.6e9)
            assert editor.done()
        if label == 'cross-hand':
            editor = table()
            assert editor.open(str(target / 'POLARIZATION'), nomodify=False)
            assert editor.putcell('CORR_TYPE', 0, np.array([5, 6], dtype=np.int32))
            assert editor.putcell('CORR_PRODUCT', 0, np.array([[0, 0], [0, 1]], dtype=np.int32))
            assert editor.done()
        predict(target, model, cache, 'awproject', False, accepts=False, wbawp=True)
        assert not list(cache.glob('*CFS_*.im'))
        print(label, 'refused before publishing any cells', flush=True)
def cold_correction_retry(root, dataset, model, machine, *, scratch, factory):
    target, cache = root / 'target.ms', root / 'cache'
    make_input(dataset, target, scratch=scratch)
    tool = configured_imager(factory, target, cache, scratch=scratch, spw='0:0', machine='ft')

    def correction(enabled):
        assert tool.setoptions(ftmachine=machine, wprojplanes=1, numthreads=1,
                               cfcachedirname=str(cache), dopbgriddingcorrections=enabled,
                               wbawp=True)

    prior = data(target, 'MODEL_DATA') if scratch else None
    correction(True)
    assert not tool.ft(str(model))
    issued = cache_contents(cache)
    assert issued and not (cache / 'avgPB').exists()
    if scratch:
        np.testing.assert_array_equal(data(target, 'MODEL_DATA'), prior)
    else:
        reader = table()
        assert reader.open(str(target / 'SOURCE'))
        assert 'definedmodel_field_0' not in reader.keywordnames() and reader.done()
    correction(False)
    assert tool.ft() and tool.done()
    assert cache_contents(cache) == issued
    return target, cache


def compare_cold_correction(root, dataset, model, *, reference=None,
                            machines=('awproject', 'mawproject')):
    assert casatasks.version_string() == '6.7.6.14'
    measured = {}
    for machine in machines:
        with TemporaryDirectory(dir=root) as attempt:
            case = Path(attempt)
            if reference is None:
                pinned, casa_cache = cold_correction_retry(
                    case / 'pinned', dataset, model, machine, scratch=True, factory=casa_imager,
                )
            else:
                pinned, casa_cache = reference / f'{machine}.ms', reference / f'{machine}-cache'
            native, native_cache = cold_correction_retry(
                case / 'native', dataset, model, machine, scratch=True, factory=imager,
            )
            expected = data(pinned, 'MODEL_DATA')
            actual = data(native, 'MODEL_DATA')
            np.testing.assert_array_equal(actual[:, 1:, :], expected[:, 1:, :])
            errors = {
                'recovered_scratch': relative_rms(actual[:, 0, :], expected[:, 0, :]),
                'recovered_cache': compare_cells(native_cache, casa_cache),
            }
            for candidate, factory, cache in (
                (case / 'pinned-warm.ms', casa_imager, casa_cache),
                (case / 'native-warm.ms', imager, native_cache),
            ):
                for wideband in (True, False):
                    target = candidate.with_name(f'{candidate.stem}-{wideband}.ms')
                    make_input(dataset, target, scratch=True)
                    tool = configured_imager(factory, target, cache, scratch=True, spw='0:0', machine='ft')
                    before = data(target, 'MODEL_DATA').tobytes()
                    assert tool.setoptions(ftmachine=machine, wprojplanes=1, numthreads=1,
                                           cfcachedirname=str(cache), dopbgriddingcorrections=True,
                                           wbawp=wideband)
                    unchanged = cache_contents(cache)
                    assert not tool.ft(str(model)) and tool.done()
                    assert data(target, 'MODEL_DATA').tobytes() == before
                    assert cache_contents(cache) == unchanged

            saved, saved_cache = cold_correction_retry(
                case / 'virtual', dataset, model, machine, scratch=False, factory=imager,
            )
            entry = recipe(saved)
            assert entry['name'] == 'AWProjectWBFT'
            assert compare_cells(Path(entry['diskimage']) / 'CASARS_LEGACY_AW_CACHE', casa_cache) < 1.e-3
            saved_cache.rename(case / 'moved-cache')
            before = corrected_or_data(saved)
            tool = ms()
            assert tool.open(str(saved), nomodify=False)
            assert tool.msselect({'field': '0'}) and tool.uvsub() and tool.done()
            predicted = before - data(saved, 'CORRECTED_DATA')
            assert not np.any(predicted[:, 1:, :])
            errors['recovered_virtual'] = relative_rms(predicted[:, 0, :], expected[:, 0, :])
            measured[machine] = errors
            print(machine, errors, flush=True)
            assert all(error < 1.e-3 for error in errors.values())
    (root / 'results.json').write_text(json.dumps(measured, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--uncorrected-reference', type=Path)
    parser.add_argument('--corrected-reference', type=Path)
    parser.add_argument('--saved-reference', type=Path)
    parser.add_argument('--cold', action='store_true')
    parser.add_argument('--cold-evla', action='store_true')
    parser.add_argument('--cold-correction', action='store_true')
    parser.add_argument('--cold-multi-w', action='store_true')
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    if args.cold:
        compare_cold(args.work, args.dataset, args.model)
        return
    if args.cold_evla:
        compare_cold_evla(args.work, args.dataset, args.model)
        return
    if args.cold_multi_w:
        compare_cold_multi_w(args.work, args.dataset, args.model)
        return
    if args.cold_correction:
        compare_cold_correction(args.work, args.dataset, args.model)
        return
    if args.uncorrected_reference is None or args.corrected_reference is None:
        parser.error('both the corrected and uncorrected references are required for warm comparisons')
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
    autocorrelation_control(args.work, args.dataset, args.model, args.work / 'cache-False')
    malformed_control(args.work, args.dataset, args.model, args.work / 'cache-False')
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
