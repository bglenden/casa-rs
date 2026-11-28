#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired smoothcal products from the installed CASA task and casa-rs."""

import argparse
from contextlib import contextmanager
import inspect
import os
from pathlib import Path
import subprocess
import sys

import casatasks as casa
from casatools import calibrater, table
import numpy as np

from applycal_oracle import copy_ms
from record_casa_returns import make_cal_ms


@contextmanager
def opened(path, write=False):
    tool = table()
    assert tool.open(str(path), nomodify=not write)
    try:
        yield tool
    finally:
        tool.close()


def columns(path):
    with opened(path) as t:
        result = {}
        for name in t.colnames():
            present = tuple(t.iscelldefined(name, row) for row in range(t.nrows()))
            if all(present):
                result[name] = t.getcol(name)
            else:
                result[name] = tuple(t.getcell(name, row) if defined else None
                                     for row, defined in enumerate(present))
        return result


def fixture(vis, gain):
    make_cal_ms(str(vis))
    casa.gaincal(str(vis), str(gain), refant='A0', solint='int', minsnr=0.0)
    with opened(gain, True) as t:
        fields = t.getcol('FIELD_ID')
        antennas = t.getcol('ANTENNA1')
        times = t.getcol('TIME')
        slots = np.searchsorted(np.unique(times), times) % 6
        shape = t.getcol('CPARAM').shape
        phases = np.array([2.9, 3.05, -3.09, -2.95, .1, -.4])[slots]
        amplitude = np.array([1., 2., 3., 4., 5., 8.])[slots] + .1 * antennas + 10 * fields
        gains = np.broadcast_to(amplitude * np.exp(1j * phases), shape).astype(np.complex64).copy()
        flags = np.zeros(shape, dtype=bool)
        flags[0, 0, (antennas == 1) & (slots == 1)] = True
        flags[1, 0, (antennas == 1) & (slots != 3)] = True
        t.putcol('CPARAM', gains)
        t.putcol('FLAG', flags)
        t.putcol('TIME', times + np.where(slots >= 4, 11., 0.))
        t.putcol('PARAMERR', np.broadcast_to((1.+slots)/100., shape).astype(np.float32).copy())
        t.putcol('SNR', np.broadcast_to(100.+slots, shape).astype(np.float32).copy())


def synthetic_table(vis, path, subtype, real, width):
    cb = calibrater()
    assert cb.open(str(vis), addcorr=False, addmodel=False)
    try:
        assert cb.createcaltable(str(path), 'Float' if real else 'Complex', subtype, True)
    finally:
        cb.close()
    with opened(vis) as ms:
        epoch = ms.getcol('TIME').min()
    with opened(path, True) as t:
        nrows = 6
        shape = (width, 1, nrows)
        t.addrows(nrows)
        t.putcol('TIME', epoch + np.arange(nrows) * 512.)
        t.putcol('INTERVAL', np.full(nrows, 512.))
        for name in ('FIELD_ID', 'SPECTRAL_WINDOW_ID', 'ANTENNA1', 'SCAN_NUMBER', 'OBSERVATION_ID'):
            t.putcol(name, np.zeros(nrows, dtype=np.int32))
        t.putcol('ANTENNA2', np.ones(nrows, dtype=np.int32))
        flags = np.zeros(shape, dtype=bool)
        flags[..., 1] = True
        t.putcol('FLAG', flags)
        t.putcol('SNR', np.full(shape, 2., dtype=np.float32))
        t.putcol('PARAMERR', np.full(shape, .2, dtype=np.float32))
        if subtype == 'Fringe Jones':
            values = np.array([
                [2.9, -2.7, 1.1, -.3, 2.1, -.2], [8, 11, 14, 20, 23, 27],
                [1e-14, 1.2e-14, 2.3e-14, 0, -1e-14, 6e-15], [.1, .2, .3, .4, .5, .6],
                [-3, 2.4, -.9, 0, 1.5, -2], [-8, -12, -17, -20, -25, -31],
                [-1e-14, .8e-14, -1.5e-14, -1e-14, -1e-14, 2e-14], [.2, .4, .7, .9, 1.3, 1.8],
            ], dtype=np.float32).reshape(shape)
        else:
            dtype = np.float32 if real else np.complex64
            values = np.broadcast_to(np.arange(1, nrows+1, dtype=dtype), shape).copy()
        t.putcol('FPARAM' if real else 'CPARAM', values)


def compare_mapping(actual, expected, fringe_median):
    assert actual.keys() == expected.keys()
    for name, want in expected.items():
        got = actual[name]
        if name in ('CPARAM', 'FPARAM'):
            if fringe_median:
                got, want = got[[0,4]], want[[0,4]]
            tolerance = 2e-6 if name == 'FPARAM' else 2e-7
            np.testing.assert_allclose(got, want, rtol=5e-7, atol=tolerance, err_msg=name)
        elif isinstance(want, tuple):
            assert len(want) == len(got)
            for before, after in zip(want, got):
                np.testing.assert_array_equal(before, after, err_msg=name)
        else:
            np.testing.assert_array_equal(got, want, err_msg=name)

def apply_round_trip(root, native, vis, case):
    """Apply each engine's calibration table with both engines."""
    expected = None
    for reader, api in (('casa', casa), ('native', native)):
        for writer in ('casa', 'native'):
            applied = root/f'apply-{reader}-{writer}-{case}.ms'
            copy_ms(vis, applied)
            cal = root/f'{writer}-{case}.cal'
            assert api.applycal(str(applied), gaintable=[str(cal)], calwt=False,
                                flagbackup=False) is None
            with opened(applied) as result:
                flags = result.getcol('FLAG')
                data = result.getcol('CORRECTED_DATA')
            if expected is None:
                expected = flags, data
            else:
                np.testing.assert_array_equal(flags, expected[0])
                assert np.any(~flags)
                np.testing.assert_allclose(data[~flags], expected[1][~flags], rtol=2e-6)




def link_control_files(root, source):
    linked = root/'file-linked.cal'
    copy_ms(source, linked)
    originals = []
    for i, relative in enumerate(('table.dat', 'SPECTRAL_WINDOW/table.dat')):
        table_file = linked/relative
        storage_file = root/f'outside-{i}.dat'
        table_file.rename(storage_file)
        table_file.symlink_to(storage_file)
        originals.append((relative, storage_file, storage_file.read_bytes()))
    return linked, originals


def assert_detached_tables(linked, output, originals):
    for relative, storage_file, original in originals:
        assert storage_file.read_bytes() == original
        assert (linked/relative).is_symlink()
        assert not (output/relative).is_symlink()


def compare(root, native, vis, source, name, options, *, in_place=False, fringe_median=False):
    products = []
    for engine, api in (('casa', casa), ('native', native)):
        target = root/f'{engine}-{name}.cal'
        if in_place:
            copy_ms(source, target)
            args = (str(vis), str(target))
        else:
            args = (str(vis), str(source), str(target))
        assert api.smoothcal(*args, **options) is None
        products.append(columns(target))
    compare_mapping(products[1], products[0], fringe_median)
    if fringe_median:
        old = columns(source)
        observed, flags, times = old['FPARAM'], old['FLAG'], old['TIME']
        expected = observed.copy()
        width = options['smoothtime'] / 2
        for parameter in [1,2,3,5,6,7]:
            values = observed[parameter,0].copy()
            enabled = ~flags[parameter,0]
            selected = [enabled & (times > t-width) & (times <= t+width) for t in times]
            if parameter in [2,6]:
                values = np.array([np.median(values[mask]) if np.any(mask) else value
                                   for mask, value in zip(selected, values)])
            expected[parameter,0] = [np.median(values[mask]) if np.any(mask) else value
                                    for mask, value in zip(selected, values)]
        np.testing.assert_allclose(products[1]['FPARAM'][[1,2,3,5,6,7]],
                                   expected[[1,2,3,5,6,7]], rtol=5e-7)
        assert products[0]['FPARAM'][1,0,0] < 2.0
        assert products[1]['FPARAM'][1,0,0] == 8.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--native-bin', required=True, type=Path)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    assert list(inspect.signature(casa.smoothcal).parameters.values()) == [
        p.replace(annotation=inspect.Parameter.empty) for p in inspect.signature(native.smoothcal).parameters.values()
    ]
    vis, gains = root/'source.ms', root/'source.cal'
    fixture(vis, gains)
    cases = [
        ('defaults', {}),
        ('mean-edges', {'smoothtype': 'mean', 'smoothtime': 20}),
        ('median-edges', {'smoothtype': 'median', 'smoothtime': 20}),
        ('one-field', {'smoothtype': 'mean', 'field': 'FLUX'}),
        ('blank-field', {'field': ''}),
        ('blank-field-list', {'field': ['']}),
        ('space-field', {'field': ' '}),
        ('boolean-width', {'smoothtime': True}),
        ('field-list', {'field': ['FLUX', 'PHASE'], 'smoothtime': 20.}),
        ('flagged-singletons', {'smoothtime': 1.}),
        ('rate-switch-on-gains', {'ratesmooth': True}),
    ]
    for name, options in cases:
        compare(root, native, vis, gains, name, options)
    compare(root, native, vis, gains, 'in-place', {'smoothtype': 'mean'}, in_place=True)
    for subtype, real, width in [('K Jones', True, 2), ('B Jones', False, 2), ('T Jones', False, 1)]:
        source = root/f'{subtype}.cal'
        synthetic_table(vis, source, subtype, real, width)
        compare(root, native, vis, source, subtype, {'smoothtype': 'mean', 'smoothtime': 1536.})
    fringe = root/'fringe.cal'
    synthetic_table(vis, fringe, 'Fringe Jones', True, 8)
    for method in ('mean', 'median'):
        for ratesmooth in (False, True):
            options = {'smoothtype': method, 'smoothtime': 1536., 'ratesmooth': ratesmooth}
            compare(root, native, vis, fringe, f'fringe-{method}-{ratesmooth}', options,
                    fringe_median=(method == 'median'))
    for name, options in [('field', {'field': 'missing-field'}), ('duration', {'smoothtime': 0.}),
                          ('partly-blank', {'field': ['', 'FLUX']})]:
        for api, label in ((casa, 'casa'), (native, 'native')):
            output = root/f'{label}-invalid-{name}.cal'
            try:
                api.smoothcal(str(vis), str(gains), str(output), **options)
            except RuntimeError:
                assert not output.exists()
            else:
                raise AssertionError(f'{label} accepted invalid smoothing {name}')
    for engine, api in (('casa', casa), ('native', native)):
        area = root/engine
        area.mkdir()
        separate = area/' '
        assert api.smoothcal(str(vis), str(gains), str(separate)) is None
        compare_mapping(columns(separate), columns(root/f'{engine}-defaults.cal'), False)
        for position in (0, 1):
            args = [str(vis), str(gains), str(area/'invalid-input.cal')]
            args[position] = [args[position]]
            try:
                api.smoothcal(*args)
            except AssertionError:
                assert not (area/'invalid-input.cal').exists()
            else:
                raise AssertionError(f'{engine} accepted a list in required input slot {position}')

    reference = root/'saved-reference.cal'
    with opened(gains) as t:
        view = t.query('ANTENNA1 < 3', name=str(reference))
        view.close()
    compare(root, native, vis, reference, 'saved-reference', {})

    spw = root/'spw-flags.cal'
    copy_ms(gains, spw)
    with opened(spw/'SPECTRAL_WINDOW', True) as t:
        t.addrows(1)
        for name in t.colnames():
            if t.iscelldefined(name, 0):
                t.putcell(name, 1, t.getcell(name, 0))
        t.putcol('FLAG_ROW', np.array([True, False]))
    physical_spw = root/'shared-spectral-window.tab'
    (spw/'SPECTRAL_WINDOW').rename(physical_spw)
    (spw/'SPECTRAL_WINDOW').symlink_to(physical_spw, target_is_directory=True)
    compare(root, native, vis, spw, 'spw-flag-reconciled', {})
    for engine in ('casa', 'native'):
        with opened(root/f'{engine}-spw-flag-reconciled.cal'/'SPECTRAL_WINDOW') as t:
            np.testing.assert_array_equal(t.getcol('FLAG_ROW'), [False, True])
    with opened(spw/'SPECTRAL_WINDOW') as t:
        np.testing.assert_array_equal(t.getcol('FLAG_ROW'), [True, False])

    locked = root/'locked-destination.cal'
    copy_ms(gains, locked)
    original = columns(locked)
    handle = table()
    assert handle.open(str(locked), nomodify=False, lockoptions={'option': 'user'})
    try:
        handle.lock(write=True, nattempts=1)
        try:
            casa.smoothcal(str(vis), str(gains), str(locked))
        except RuntimeError:
            pass
        else:
            raise AssertionError('CASA smoothcal replaced an open destination')

        # POSIX record locks belong to the process. The Python task now runs
        # in-process, so a separate caller must contend for the CASA lock.
        native_probe = """
import sys
from casars import tasks

try:
    tasks.smoothcal(*sys.argv[1:])
except RuntimeError as error:
    if 'another process holds a read or write lock' not in str(error):
        raise
else:
    raise AssertionError('casa-rs smoothcal replaced an actively write-locked destination')
"""
        subprocess.run(
            [sys.executable, '-c', native_probe, str(vis), str(gains), str(locked)],
            check=True,
            timeout=30,
        )
    finally:
        handle.unlock()
        handle.close()
    compare_mapping(columns(locked), original, False)
    linked, originals = link_control_files(root, gains)
    compare(root, native, vis, linked, 'file-linked-input', {})
    assert_detached_tables(linked, root/'native-file-linked-input.cal', originals)
    apply_round_trip(root, native, vis, 'mean-edges')
    print('29 paired CASA smoothcal case groups and four downstream applications')


if __name__ == '__main__':
    main()
