#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native table-maintenance entry points with CASA's calibrater tool."""

import argparse
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys

import casatasks as casa
from casatools import calibrater
import numpy as np

from bpoly_oracle import multiwindow_data
from calibration_rerefant_oracle import reference_fixture
from calibration_smoothcal_oracle import (
    apply_round_trip, columns, compare_mapping, copy_ms, fixture, opened,
)
from gaincal_oracle import ephemeris_observation

STANDARD_SUBTABLES = ('ANTENNA', 'FIELD', 'OBSERVATION', 'SPECTRAL_WINDOW', 'HISTORY')


def tool(vis=None, *, old=False):
    cb = calibrater()
    if old:
        assert cb.setvi(old=True, quiet=True)
    if vis:
        assert cb.open(str(vis), addcorr=False, addmodel=False)
    return cb


def compare_tables(native, expected, *, empty=False, moving=False, flagged_payloads=True):
    with opened(expected) as a, opened(native) as b:
        assert a.info() == b.info()
        assert set(a.colnames()) == set(b.colnames())
        assert a.nrows() == b.nrows()
        for name in ('VisCal', 'MSName', 'ParType', 'PolBasis'):
            assert a.getkeyword(name) == b.getkeyword(name)
        for name in STANDARD_SUBTABLES:
            assert b.getkeyword(name) == f'Table: {native/name}'
    if not empty:
        actual, want = columns(native), columns(expected)
        if not flagged_payloads:
            np.testing.assert_array_equal(actual['FLAG'], want['FLAG'])
            actual['CPARAM'] = actual['CPARAM'][~want['FLAG']]
            want['CPARAM'] = want['CPARAM'][~want['FLAG']]
        compare_mapping(actual, want, False)
    for name in STANDARD_SUBTABLES:
        with opened(expected/name) as a, opened(native/name) as b:
            assert set(a.colnames()) == set(b.colnames())
            assert a.nrows() == b.nrows()
            for column in a.colnames():
                for row in range(a.nrows()):
                    defined = a.iscelldefined(column, row)
                    assert defined == b.iscelldefined(column, row), (name, column, row)
                    if defined:
                        want, result = a.getcell(column, row), b.getcell(column, row)
                        if moving and name == 'FIELD' and column.endswith('_DIR'):
                            np.testing.assert_allclose(result, want, rtol=1e-12, atol=1e-12)
                        else:
                            np.testing.assert_array_equal(result, want, err_msg=f'{name}.{column}[{row}]')


def creation_and_repair(root, vis, source, driver):
    cb = tool(vis)
    for i, parameters in enumerate([
        ('Complex', 'G Jones', False), ('Float', 'K Jones', True),
        ('complex', 'G', True), ('real', 'K', True), ('Float', 'unregistered', False),
        ('Float', '', False),
    ]):
        native, want = root/f'native-create-{i}.cal', root/f'casa-create-{i}.cal'
        for output in (native, want):
            copy_ms(source, output)
        partype, caltype, single = parameters
        assert cb.createcaltable(str(want), *parameters)
        subprocess.run([driver, 'create', vis, native, partype, caltype, str(single).lower()], check=True)
        compare_tables(native, want, empty=True)
    cb.close()

    cb = tool()
    for mode in ('complete', 'column', 'subtable', 'both'):
        native, want = root/f'native-update-{mode}.cal', root/f'casa-update-{mode}.cal'
        for output in (native, want):
            copy_ms(source, output)
            with opened(output, True) as t:
                if mode in ('column', 'both'):
                    t.removecols('OBSERVATION_ID')
                if mode in ('subtable', 'both'):
                    t.removekeyword('OBSERVATION')
            if mode in ('subtable', 'both'):
                shutil.rmtree(output/'OBSERVATION')
        inode = (native/'table.dat').stat().st_ino
        assert cb.updatecaltable(str(want))
        subprocess.run([driver, 'update', native], check=True)
        compare_tables(native, want)
        if mode == 'complete':
            assert (native/'table.dat').stat().st_ino == inode
    cb.close()

    for engine in ('casa', 'native'):
        alias = root/f'{engine}-alias.ms'
        copy_ms(vis, alias)
        with opened(alias) as t:
            rows = t.nrows()
        if engine == 'casa':
            cb = tool(alias)
            try:
                try:
                    result = cb.createcaltable(str(alias), 'Complex', 'G Jones', True)
                except RuntimeError:
                    result = False
                assert not result
            finally:
                cb.close()
        else:
            command = [driver, 'create', alias, alias, 'Complex', 'G Jones', 'true']
            assert subprocess.run(command, capture_output=True).returncode != 0
        with opened(alias) as t:
            assert t.nrows() == rows and t.info()['type'] == 'Measurement Set'

        unlinked = root/f'{engine}-unlinked.cal'
        copy_ms(source, unlinked)
        with opened(unlinked, True) as t:
            t.removekeyword('OBSERVATION')
        prior = columns(unlinked)
        if engine == 'casa':
            cb = tool()
            try:
                result = cb.updatecaltable(str(unlinked))
            except RuntimeError:
                result = False
            assert not result
        else:
            assert subprocess.run([driver, 'update', unlinked], capture_output=True).returncode != 0
        compare_mapping(columns(unlinked), prior, False)
        assert (unlinked/'OBSERVATION').is_dir()
    print('12 paired creation, legacy repair, alias and failure groups', flush=True)


def run_native(driver, vis, lhs, rhs, output, field='', calfield='',
               interp='linear', cadence=-1., spwmap=(-1,)):
    subprocess.run([driver, 'accumulate', vis, lhs, rhs, output, field, calfield,
                    interp, str(cadence), ','.join(map(str, spwmap))], check=True)


def paired_accumulation(root, driver, vis, lhs, rhs, name, *, field='', calfield='',
                        interp='linear', cadence=-1., spwmap=(-1,), alias=False,
                        flagged_payloads=True):
    want, actual = root/f'casa-{name}.cal', root/f'native-{name}.cal'
    if alias:
        for output in (actual, want):
            copy_ms(lhs, output)
        casa_input, native_input = str(want), str(actual)
        casa_output, native_output = '', ''
    else:
        casa_input = native_input = str(lhs)
        casa_output, native_output = str(want), str(actual)
    cb = tool(vis, old=True)
    try:
        assert cb.accumulate(casa_input, str(rhs), casa_output, field, calfield,
                             interp, cadence, list(spwmap))
    finally:
        cb.close()
    run_native(driver, vis, native_input, rhs, native_output, field, calfield, interp,
               cadence, spwmap)
    compare_tables(actual, want, flagged_payloads=flagged_payloads)
    return want, actual


def ordered_view(original, target):
    with opened(original) as t:
        query = t.query('', name=str(target), sortlist='SPECTRAL_WINDOW_ID,TIME,ANTENNA1,ANTENNA2')
        query.close()


def bandpass_oracle(root, driver, vis):
    bandpass = root/'bandpass.cal'
    casa.bandpass(str(vis), str(bandpass), refant='A0', solint='inf', minsnr=0.)
    want, actual = root/'casa-bandpass.cal', root/'native-bandpass.cal'
    code = """
import resource,sys
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
from casatools import calibrater
cb=calibrater()
assert cb.setvi(old=True,quiet=True)
assert cb.open(sys.argv[1],addcorr=False,addmodel=False)
assert cb.accumulate(sys.argv[2],sys.argv[2],sys.argv[3])
cb.close()
"""
    completed = subprocess.run([sys.executable, '-c', code, str(vis), str(bandpass), str(want)],
                               capture_output=True, timeout=60)
    assert completed.returncode == 0, completed.stderr.decode()
    run_native(driver, vis, bandpass, bandpass, actual)
    source, bad, native = columns(bandpass), columns(want), columns(actual)
    assert np.count_nonzero(abs(bad['CPARAM'] - source['CPARAM']) > 1e-6) == 20
    flags = source['FLAG']
    np.testing.assert_array_equal(native['FLAG'], flags)
    np.testing.assert_allclose(native['CPARAM'][~flags], source['CPARAM'][~flags]**2,
                              rtol=2e-7, atol=2e-7)
    print('Channelized bandpass matches the per-cell Jones product', flush=True)


def accumulate(root, driver, vis, gains, solved):
    cb = tool(vis)
    refused = root/'vi2-refused.cal'
    try:
        cb.accumulate(str(gains), str(gains), str(refused))
    except RuntimeError as error:
        assert 'not implemented' in str(error)
    else:
        raise AssertionError('CASA default VI2 unexpectedly accumulated')
    cb.close()
    assert not refused.exists()

    scalar = root/'scalar.cal'
    reference_fixture(vis, scalar, 'T Jones', width=1)
    for name, table in [('gain', gains), ('scalar', scalar)]:
        paired_accumulation(root, driver, vis, table, table, name)
    paired_accumulation(root, driver, vis, gains, gains, 'in-place', interp='nearest', alias=True)
    control = root/'ordered.cal'
    ordered_view(solved, control)
    for name, extra in [
        ('field-selection', dict(field='FLUX', calfield='FLUX', interp='nearest', spwmap=(0,))),
        ('field-transfer', dict(calfield='FLUX')),
        ('field-combination', dict(calfield='FLUX,PHASE')),
    ]:
        paired_accumulation(root, driver, vis, solved, control, name, **extra, flagged_payloads=False)
    for name, cadence, extra in [
        ('zero-cadence', 0., {}),
        ('cadence-15', 15., {}),
        ('cadence-30-nearest', 30., dict(interp='nearest')),
        ('cadence-50-select', 50., dict(field='PHASE', calfield='FLUX')),
    ]:
        paired_accumulation(root, driver, vis, '', control, name, **extra,
                            cadence=cadence, flagged_payloads=False)
    paired_accumulation(root, driver, vis, '', scalar, 'scalar-cadence', cadence=30.)
    for mode in ('cubic', 'nearest,linear', 'linearperobs', 'linearPD'):
        paired_accumulation(root, driver, vis, '', control, f'interpolation-{mode}',
                            interp=mode, cadence=15., flagged_payloads=False)

    native_unsorted = root/'native-unsorted-time.cal'
    run_native(driver, vis, solved, solved, native_unsorted, calfield='FLUX,PHASE')
    compare_tables(native_unsorted, root/'native-field-combination.cal')
    casa_unsorted = root/'casa-unsorted-time.cal'
    cb = tool(vis, old=True)
    assert cb.accumulate(str(solved), str(solved), str(casa_unsorted), [],
                         'FLUX,PHASE', 'linear', -1., [-1])
    cb.close()
    ordered, unordered = columns(native_unsorted), columns(casa_unsorted)
    valid = ~(ordered['FLAG'] | unordered['FLAG'])
    assert np.max(np.abs(ordered['CPARAM'][valid] - unordered['CPARAM'][valid])) > 1.0

    cb = tool(vis, old=True)
    for mode in ('aipslin', 'bogus'):
        output = root/f'invalid-{mode}.cal'
        try:
            cb.accumulate(str(gains), str(gains), str(output), [], [], mode, -1., [-1])
        except RuntimeError:
            assert not output.exists()
        else:
            raise AssertionError(f'CASA accepted invalid interpolation {mode}')
        command = [driver, 'accumulate', vis, gains, gains, output, '', '', mode, '-1', '-1']
        assert subprocess.run(command, capture_output=True).returncode != 0
        assert not output.exists()
    cb.close()


    multi_vis = root/'multi.ms'
    multiwindow_data(vis, multi_vis)
    multi = root/'multi.cal'
    casa.gaincal(str(multi_vis), str(multi), refant='A0', solint='int', minsnr=0.)
    for mapping in [(0,), (0,0), (1,0), (-999,)]:
        suffix = '-'.join(map(str,mapping))
        paired_accumulation(root, driver, multi_vis, multi, multi, f'mapping-{suffix}',
                            spwmap=mapping, flagged_payloads=False)

    from casars import tasks as native
    apply_round_trip(root, native, vis, 'gain')
    bandpass_oracle(root, driver, vis)
    print('22 paired accumulation case groups and four downstream applications', flush=True)


def main():
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-driver', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    assert casa.version_string() == '6.7.6.14'
    vis, solved, gains = root/'source.ms', root/'solved.cal', root/'gains.cal'
    fixture(vis, solved)
    reference_fixture(vis, gains, 'G Jones')
    creation_and_repair(root, vis, gains, args.native_driver.resolve())
    ephem_root = root/'ephemeris'
    ephem_root.mkdir()
    moving, _ = ephemeris_observation(ephem_root, vis)
    with opened(moving/'FIELD', True) as t:
        t.putcell('REFERENCE_DIR', 0, np.array([[1.4e-6],[-2.6e-6]]))
    actual, want = ephem_root/'native.cal', ephem_root/'casa.cal'
    cb = tool(moving)
    assert cb.createcaltable(str(want), 'Complex', 'G Jones', True)
    cb.close()
    driver = args.native_driver.resolve()
    subprocess.run([driver, 'create', moving, actual, 'Complex', 'G Jones', 'true'], check=True)
    compare_tables(actual, want, empty=True, moving=True)
    print('An ephemeris field uses its nominal reference direction', flush=True)
    accumulate(root, driver, vis, gains, solved)


if __name__ == '__main__':
    main()
