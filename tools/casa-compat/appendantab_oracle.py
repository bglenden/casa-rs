#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair ANTAB imports and their calibration consumers with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import shutil
import time

from casatools import table as casa_table
import casatasks as casa
import numpy as np

from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms

EPOCH = datetime(1858, 11, 17, tzinfo=timezone.utc)
ANTAB_SHA256 = {
    'evn': 'a5c2a4e4ab14559c21b2f6771eb943b7407c346959a60d0c9cb83c80d3d23a95',
    'vlba': 'd79ec866253d00151f9993eeaf61fea5cbf63bee7fdf754bb026a25f41d70beb',
}


def source(root, label, stations, frequency='7GHz', *, two_windows=False, start=None, channels=16):
    vis = root / f'{label}.ms'
    make_cal_ms(str(vis), channels=channels, scan_seconds=120, integration_seconds=4, stokes='RR LL',
                frequency=frequency)
    with opened(vis / 'ANTENNA', True) as table:
        if len(stations) > table.nrows():
            old_rows = table.nrows()
            table.addrows(len(stations) - old_rows)
            for row in range(old_rows, table.nrows()):
                for column in table.colnames():
                    if table.iscelldefined(column, 0):
                        table.putcell(column, row, table.getcell(column, 0))
                table.putcell('NAME', row, stations[row])
        table.putcol('STATION', np.array(stations))
    if start is not None:
        with opened(vis, True) as table:
            values = table.getcol('TIME')
            offset = (start - EPOCH).total_seconds() - values[0]
            for key in ('TIME', 'TIME_CENTROID'):
                table.putcol(key, table.getcol(key) + offset)
        with opened(vis / 'OBSERVATION', True) as table:
            table.putcol('TIME_RANGE', table.getcol('TIME_RANGE') + offset)
    if two_windows:
        for path in (vis / 'SPECTRAL_WINDOW', vis / 'DATA_DESCRIPTION', vis):
            original = duplicate_rows(casa_table(), path)
        with opened(vis / 'SPECTRAL_WINDOW', True) as table:
            for key in ('CHAN_FREQ', 'REF_FREQUENCY'):
                table.putcell(key, 1, table.getcell(key, 0) + 48e6)
        with opened(vis / 'DATA_DESCRIPTION', True) as table:
            table.putcell('SPECTRAL_WINDOW_ID', 1, 1)
        with opened(vis, True) as table:
            table.putcol('DATA_DESC_ID', np.ones(original, np.int32), original, original)
    return vis


def text_fixture(root, vis, revised=False):
    with opened(vis) as table:
        start = int(table.getcol('TIME')[0])
    dates = [EPOCH + timedelta(seconds=start + offset) for offset in (10, 24, 1000)]
    filename = root / ('revised.antab' if revised else 'analytic.antab')
    first_tsys = '105 115 125' if revised else '100 110 120'
    second_tsys = '120 140 170' if revised else '130 999.9 160'
    sensitivity = '.3,.4' if revised else '.1,.2'
    filename.write_text(
        f"! Synthetic two-receptor, two-window calibration\n"
        f"GAIN BR ALTAZ DPFU={sensitivity} FREQ=6900,7200 POLY=1.0,0.02 /\n"
        f"GAIN FD ELEV DPFU=.16 POLY=1.0 /\n"
        f"TSYS BR INDEX='R1:2','L1','L2' TIMEOFF=2 /\n"
        f"{dates[0]:%j %H:%M:%S} {first_tsys}\n"
        f"{dates[1]:%j %H:%M:%S} {second_tsys}\n"
        f"{dates[2]:%j %H:%M:%S} 999 999 999\n"
        f"/\n"
        f"GAIN UNKNOWN ELEV DPFU=.1 POLY=1.0 /\n"
        f"/\n")
    return filename, np.array([start + 12., start + 26.])


def imports(root, label, tasks, sources, antab, **options):
    outputs = {}
    for engine, task in tasks.items():
        destination = root / f'{label}-{engine}.ms'
        beginning = time.perf_counter()
        answer = task(str(sources[engine]), str(destination), str(antab), **options)
        assert answer is None
        assert destination.is_dir()
        print(label, engine, f'{time.perf_counter() - beginning:.3f}s', flush=True)
        outputs[engine] = destination
    return outputs


def compare_product(reference, actual, *, prefix=''):
    with opened(reference) as left, opened(actual) as right:
        assert left.nrows() == right.nrows(), prefix
        assert left.colnames() == right.colnames(), prefix
        for key in left.colnames():
            for row in range(left.nrows()):
                assert left.iscelldefined(key, row) == right.iscelldefined(key, row), (prefix, key, row)
                if not left.iscelldefined(key, row):
                    continue
                lhs, rhs = left.getcell(key, row), right.getcell(key, row)
                if np.asarray(lhs).dtype.kind in 'fc':
                    np.testing.assert_allclose(rhs, lhs, atol=1e-6 if key == 'TIME' else 0.,
                                               rtol=0., err_msg=f'{prefix} {key} {row}')
                else:
                    np.testing.assert_array_equal(rhs, lhs, err_msg=f'{prefix} {key} {row}')
        for column in ('TIME', 'INTERVAL'):
            if column in left.colnames():
                for keyword, values in left.getcolkeywords(column).items():
                    np.testing.assert_equal(right.getcolkeyword(column, keyword), values,
                                            err_msg=f'{prefix} {column} {keyword}')


def compare_subtables(products, present=('SYSCAL', 'GAIN_CURVE')):
    for key in ('SYSCAL', 'GAIN_CURVE'):
        for path in products.values():
            assert (path / key).exists() == (key in present), (path, key)
        if key in present and 'native' in products:
            compare_product(products['casa'] / key, products['native'] / key, prefix=key)


def check_inputs(vis, products):
    with opened(vis) as original:
        data = original.getcol('DATA')
        time_values = original.getcol('TIME')
    for path in products.values():
        with opened(path) as result:
            np.testing.assert_array_equal(result.getcol('DATA'), data)
            np.testing.assert_array_equal(result.getcol('TIME'), time_values)
    assert not (vis / 'GAIN_CURVE').exists()
    assert not (vis / 'SYSCAL').exists()


def check_laws(product, times, updated=False):
    with opened(product / 'SYSCAL') as table:
        np.testing.assert_array_equal(table.getcol('ANTENNA_ID'), np.zeros(4, dtype=int))
        np.testing.assert_array_equal(table.getcol('SPECTRAL_WINDOW_ID'), [0, 1, 0, 1])
        np.testing.assert_allclose(table.getcol('TIME'), times.repeat(2), atol=1e-6, rtol=0.)
        expected = [[105, 105, 120, 120], [115, 125, 140, 170]] if updated else [
            [100, 100, 130, 130], [110, 120, np.float32(-999.9), 160]]
        np.testing.assert_array_equal(table.getcol('TSYS'), expected)
    with opened(product / 'GAIN_CURVE') as table:
        np.testing.assert_array_equal(table.getcol('SPECTRAL_WINDOW_ID'), [0, 1, 0, 1])
        np.testing.assert_array_equal(table.getcol('TYPE'), ['POWER(ZA)'] * 2 + ['POWER(EL)'] * 2)
        np.testing.assert_allclose(table.getcell('GAIN', 0), [[1., .02], [1., .02]], atol=1e-7)
        np.testing.assert_allclose(table.getcol('SENSITIVITY')[:, 0],
                                   [.3, .4] if updated else [.1, .2], atol=1e-7)


def check_consumers(root, products, consumers):
    if 'native' in products:
        with opened(products['native']) as table:
            for key in ('SYSCAL', 'GAIN_CURVE'):
                assert table.getkeyword(key).endswith('/' + key)
        for key in ('SYSCAL', 'GAIN_CURVE'):
            with opened(f"{products['native']}::{key}") as table:
                assert table.nrows() > 0
    casa_input = root / 'reference-for-consumers.ms'
    shutil.copytree(products['casa'], casa_input)
    # The CASA importer writes the table links on the child tables. Supply
    # the links on MAIN for its independent gencal consumer.
    with opened(casa_input, True) as table:
        for key in ('SYSCAL', 'GAIN_CURVE'):
            table.putkeyword(key, f'Table: {casa_input / key}')
    for caltype in ('tsys', 'gc'):
        reference = root / f'{caltype}-reference.cal'
        assert consumers['casa'](str(casa_input), str(reference), caltype=caltype) is None
        for producer, vis in products.items():
            for consumer, task in consumers.items():
                if (producer, consumer) == ('casa', 'casa'):
                    continue
                if producer == 'casa':
                    vis = casa_input
                destination = root / f'{caltype}-{producer}-to-{consumer}.cal'
                assert task(str(vis), str(destination), caltype=caltype) is None
                with opened(reference) as left, opened(destination) as right:
                    assert left.info()['subType'] == right.info()['subType']
                    assert left.nrows() == right.nrows()
                    np.testing.assert_array_equal(right.getcol('FLAG'), left.getcol('FLAG'))
                    np.testing.assert_allclose(right.getcol('FPARAM'), left.getcol('FPARAM'), rtol=1e-7)


def check_analytic(root, tasks, consumers):
    vis = source(root, 'analytic-input', ['BR', 'FD', 'HN', 'KP', 'LA'], two_windows=True)
    inputs = dict.fromkeys(tasks, vis)
    antab, times = text_fixture(root, vis)
    products = imports(root, 'both', tasks, inputs, antab)
    compare_subtables(products)
    check_inputs(vis, products)
    for product in products.values():
        check_laws(product, times)
    for label, kwargs, present in [
        ('gain-only', dict(append_tsys=False), ('GAIN_CURVE',)),
        ('tsys-only', dict(append_gc=False), ('SYSCAL',)),
        ('copy-only', dict(append_gc=False, append_tsys=False), ()),
    ]:
        compare_subtables(imports(root, label, tasks, inputs, antab, **kwargs), present)
    repeated = imports(root, 'repeated', tasks, products, antab)
    compare_subtables(repeated)
    for path in repeated.values():
        with opened(path / 'SYSCAL') as table:
            assert table.nrows() == 8
        with opened(path / 'GAIN_CURVE') as table:
            assert table.nrows() == 4
    updated_antab, _ = text_fixture(root, vis, revised=True)
    updated = imports(root, 'overwrite', tasks, products, updated_antab, overwrite=True)
    compare_subtables(updated)
    for product in updated.values():
        check_laws(product, times, updated=True)
    for engine, task in tasks.items():
        # The original copy and a pre-existing destination both remain usable.
        assert task(str(vis), str(vis), str(antab), overwrite=True) is None
        assert task(str(vis), '', str(antab)) is None
        assert task(str(vis), str(products[engine]), str(updated_antab), overwrite=True) is None
        check_laws(products[engine], times)
        fresh = root / f'fresh-overwrite-{engine}.ms'
        if engine == 'casa':
            try:
                task(str(vis), str(fresh), str(antab), overwrite=True)
            except IndexError as error:
                assert 'out of bounds' in str(error)
            else:
                raise AssertionError('CASA unexpectedly accepted overwrite on an empty gain table')
        else:
            assert task(str(vis), str(fresh), str(antab), overwrite=True) is None
            for key in ('SYSCAL', 'GAIN_CURVE'):
                compare_product(products['casa'] / key, fresh / key)
    check_errors(root, vis, antab, tasks)
    check_consumers(root, products, consumers)
    check_bulk_time_edits(root, vis, antab, tasks)
    check_review_cases(root, vis, antab, tasks, products)


def check_bulk_time_edits(root, vis, antab, tasks):
    products = imports(root, 'before-time-edits', tasks, dict.fromkeys(tasks, vis), antab)
    edited_gains = {}
    for engine, product in products.items():
        for subtable in ('GAIN_CURVE', 'SYSCAL'):
            with opened(product / subtable, True) as table:
                times = table.getcol('TIME')
                times[-2:] = -1
                table.putcol('TIME', times)
            with opened(product / subtable) as table:
                np.testing.assert_array_equal(table.getcol('TIME'), times,
                                              err_msg=f'{engine} {subtable} bulk TIME edit')
            if subtable == 'GAIN_CURVE':
                edited_gains[engine] = times

    repeated = imports(root, 'after-time-edits', tasks, products, antab, append_tsys=False)
    compare_subtables(repeated)
    for engine, product in repeated.items():
        with opened(product / 'GAIN_CURVE') as table:
            assert table.nrows() == 6
            np.testing.assert_array_equal(table.getcol('TIME')[:4], edited_gains[engine])


def check_errors(root, vis, antab, tasks):
    malformed = root / 'malformed.antab'
    malformed.write_text('GAIN BR ALTAZ DPFU= /\n')
    for engine, task in tasks.items():
        bad = root / f'bad-{engine}.ms'
        for params, message in [
            (dict(vis=str(root / 'missing.ms')), 'the vis parameter must be a path'),
            (dict(antab=str(root / 'missing.antab')), 'the antab parameter must be a path'),
            (dict(antab=antab), 'the antab parameter must be a path'),
            (dict(outvis=bad), 'must be of cStr type'),
            (dict(overwrite=None), 'null value not allowed'),
            (dict(append_tsys=1), 'must be of cBool type'),
        ]:
            args = dict(vis=str(vis), outvis=str(bad), antab=str(antab)) | params
            try:
                task(**args)
            except AssertionError as error:
                assert message in str(error), str(error)
                assert not bad.exists()
            else:
                raise AssertionError(f'{engine} accepted {params}')
        try:
            task(str(vis), str(bad), str(malformed))
        except RuntimeError as error:
            assert 'error parsing ANTAB file' in str(error)
            if engine == 'native':
                assert not bad.exists()
        else:
            raise AssertionError(f'{engine} accepted a malformed ANTAB file')
        nested = root / f'nested-{engine}' / 'child' / 'copy.ms'
        assert task(str(vis), str(nested), str(antab), append_tsys=False, append_gc=False) is None
        assert nested.is_dir()


def check_review_cases(root, vis, antab, tasks, products):
    for engine, task in tasks.items():
        optional = root / f'optional-source-{engine}.ms'
        shutil.copytree(products[engine], optional)
        with opened(optional / 'SYSCAL', True) as table:
            table.addcols({'TSYS_FLAG': {'valueType': 'boolean'}})
            table.putcol('TSYS_FLAG', np.ones(4, dtype=bool))
        optional_out = root / f'optional-output-{engine}.ms'
        assert task(str(optional), str(optional_out), str(antab), append_gc=False) is None
        with opened(optional_out / 'SYSCAL') as table:
            np.testing.assert_array_equal(table.getcol('TSYS_FLAG'), [True] * 4 + [False] * 4)
        try:
            task(str(vis), str(root / f'mixed-error-{engine}.ms'), None, overwrite=1)
        except AssertionError as error:
            assert str(error) == "{'antab': ['null value not allowed'], 'overwrite': ['must be of cBool type']}"
        else:
            raise AssertionError('ANTAB accepted null file and integer overwrite')
    missing = root / 'missing-dpfu.antab'
    missing.write_text('GAIN BR ALTAZ POLY=1 /\n')
    for engine, task in tasks.items():
        try:
            task(str(vis), str(root / f'missing-dpfu-{engine}.ms'), str(missing))
        except KeyError as error:
            assert error.args == ('DPFU',)
        else:
            raise AssertionError('ANTAB accepted a known gain without DPFU')

    borderline = root / 'temperature-rounding.antab'
    borderline.write_text(antab.read_text().replace('100 110 120', '999.90001 110 120'))
    outputs = imports(root, 'temperature-rounding', tasks, dict.fromkeys(tasks, vis), borderline)
    compare_subtables(outputs)
    for output in outputs.values():
        with opened(output / 'SYSCAL') as table:
            assert table.getcell('TSYS', 0)[0] == np.float32(999.90001)

    reversed_dd = root / 'reversed-data-description.ms'
    shutil.copytree(vis, reversed_dd)
    with opened(reversed_dd / 'DATA_DESCRIPTION', True) as table:
        table.putcol('SPECTRAL_WINDOW_ID', np.array([1, 0], np.int32))
    outputs = imports(root, 'reordered-spw', tasks, dict.fromkeys(tasks, reversed_dd), antab)
    for engine, output in outputs.items():
        with opened(output / 'SYSCAL') as table:
            np.testing.assert_array_equal(table.getcol('SPECTRAL_WINDOW_ID'), [1, 0, 1, 0])
            expected = [110, 120] if engine == 'casa' else [120, 110]
            np.testing.assert_array_equal(table.getcol('TSYS')[1, :2], expected)
    unknown = root / 'consecutive-gains.antab'
    unknown.write_text('GAIN ZZ ALTAZ DPFU=.1 POLY=1 /\n'
                       'GAIN BR ALTAZ DPFU=.2 POLY=1 /\n'
                       'GAIN FD ALTAZ DPFU=.3 POLY=1 /\n')
    outputs = imports(root, 'unknown-station', tasks, dict.fromkeys(tasks, vis), unknown,
                      append_tsys=False)
    for engine, output in outputs.items():
        with opened(output / 'GAIN_CURVE') as table:
            np.testing.assert_array_equal(table.getcol('ANTENNA_ID'),
                                          [1, 1] if engine == 'casa' else [0, 0, 1, 1])
    check_single_channel_overwrite(root, tasks)
    check_source_aliases(root, vis, antab, tasks, products)


def check_single_channel_overwrite(root, tasks):
    vis = source(root, 'one-channel', ['BR', 'FD', 'HN', 'KP', 'LA'], channels=1)
    def write(name, first, second):
        path = root / name
        path.write_text(f'GAIN BR ALTAZ DPFU={first} FREQ=6000,8000 POLY=1 /\n'
                        f'GAIN FD ALTAZ DPFU={second} FREQ=6000,8000 POLY=1 /\n')
        return path
    first = write('gain-before.antab', '.1,.2', '.16,.17')
    changed = write('gain-change.antab', '.51,.52', '.92,.93')
    originals = imports(root, 'one-channel-first', tasks, dict.fromkeys(tasks, vis), first,
                        append_tsys=False)
    for engine, task in tasks.items():
        output = root / f'single-channel-overwrite-{engine}.ms'
        params = (str(originals[engine]), str(output), str(changed))
        if engine == 'casa':
            try:
                task(*params, append_tsys=False, overwrite=True)
            except IndexError:
                continue
            raise AssertionError('CASA unexpectedly accepted the one-channel overwrite')
        assert task(*params, append_tsys=False, overwrite=True) is None
        with opened(output / 'GAIN_CURVE') as table:
            np.testing.assert_allclose(table.getcol('SENSITIVITY'), [[.51, .92], [.52, .93]],
                                       rtol=1e-7)


def check_source_aliases(root, vis, antab, tasks, products):
    if 'native' not in tasks:
        return
    task = tasks['native']
    linked = root / 'linked-storage.ms'
    shutil.copytree(vis, linked)
    backing = root / 'linked-table.dat'
    (linked / 'table.dat').rename(backing)
    (linked / 'table.dat').symlink_to(backing)
    original = backing.read_bytes()
    independent = root / 'independent.ms'
    assert task(str(linked), str(independent), str(antab)) is None
    assert backing.read_bytes() == original
    assert not (independent / 'table.dat').is_symlink()
    check_inputs(linked, {'native': independent})

    external_ms = root / 'external-subtable-input.ms'
    shutil.copytree(products['native'], external_ms)
    external = root / 'external-tsys.table'
    (external_ms / 'SYSCAL').rename(external)
    with opened(external, True) as table:
        table.putcell('TSYS', 0, np.array([1234, 5678], dtype=np.float32))
    with opened(external_ms, True) as table:
        table.putkeyword('SYSCAL', f'Table: {external}')
    destination = root / 'copied-external-subtable.ms'
    assert task(str(external_ms), str(destination), str(antab), append_gc=False) is None
    with opened(f'{destination}::SYSCAL') as table, opened(external) as source_table:
        assert table.nrows() == 8
        assert source_table.nrows() == 4
        np.testing.assert_array_equal(table.getcell('TSYS', 0), [1234, 5678])
        np.testing.assert_array_equal(source_table.getcell('TSYS', 0), [1234, 5678])


def check_recorded(root, tasks, files):
    for experiment, path in files.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == ANTAB_SHA256[experiment]
        if experiment == 'evn':
            start = datetime(2019, 1, 1, 12, 8, tzinfo=timezone.utc) + timedelta(days=294)
            vis = source(root, 'evn-input',
                         ['BD', 'EF', 'HH', 'JB', 'NT', 'ON', 'SH', 'SV', 'TR', 'WB', 'YS', 'ZC'],
                         '4.8GHz', start=start)
        else:
            vis = source(root, 'vlba-input', ['BR', 'FD', 'HN', 'KP', 'LA', 'MK', 'NL', 'OV', 'PT', 'SC'])
        products = imports(root, experiment, tasks, dict.fromkeys(tasks, vis), path,
                           append_tsys=experiment == 'evn')
        compare_subtables(products, ('SYSCAL', 'GAIN_CURVE') if experiment == 'evn' else ('GAIN_CURVE',))
        with opened(products['casa'] / 'GAIN_CURVE') as table:
            assert table.nrows() >= 5
        if experiment == 'evn':
            with opened(products['casa'] / 'SYSCAL') as table:
                assert table.nrows() > 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--evn', type=Path)
    parser.add_argument('--vlba', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    arguments = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = arguments.work.resolve()
    root.mkdir()
    tasks, consumers = {'casa': casa.appendantab}, {'casa': casa.gencal}
    if not arguments.reference_only:
        from casars.tasks import appendantab, gencal
        tasks['native'], consumers['native'] = appendantab, gencal
    files = {key: path.resolve() for key in ('evn', 'vlba') if (path := getattr(arguments, key))}
    with chdir(root):
        check_analytic(root, tasks, consumers)
        check_recorded(root, tasks, files)
    print('ANTAB temperatures, gain curves, and consumers passed.', flush=True)


if __name__ == '__main__':
    main()
