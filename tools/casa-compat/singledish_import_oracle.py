#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA 6.7.6.14's detached single-dish imports and MSv2 outputs."""

import argparse
from pathlib import Path
import shutil
import time

import casatasks
from casatools import ms, singledishms
import numpy as np

from calibration_smoothcal_oracle import opened


def compare(a, b, label):
    if np.asarray(a).dtype.kind in 'fc':
        np.testing.assert_allclose(a, b, rtol=2e-7, atol=1e-7, equal_nan=True, err_msg=label)
    else:
        np.testing.assert_array_equal(a, b, err_msg=label)


def compare_tables(reference, output, fixes=None):
    with opened(reference) as left, opened(output) as right:
        assert left.nrows() == right.nrows(), str(reference)
        assert sorted(left.colnames()) == sorted(right.colnames()), str(reference)
        for name in left.colnames():
            label = f'{reference.name}:{name}'
            for keyword in ('UNIT', 'QuantumUnits', 'MEASINFO'):
                a = left.getcolkeywords(name).get(keyword)
                b = right.getcolkeywords(name).get(keyword)
                if isinstance(a, dict):
                    for item in a.keys():
                        if item in ('type', 'Ref', 'VarRefCol'):
                            compare(a.get(item), b.get(item), label + ':' + item)
                elif a is not None or b is not None:
                    compare(a, b, label + ':' + keyword)
            if not left.nrows():
                continue
            if fixes == 'asap' and reference.name == 'OBSERVATION' and name == 'TIME_RANGE':
                np.testing.assert_array_equal(right.getcol(name), left.getcol(name) * 86400.)
                continue
            if fixes and reference.name == 'FEED' and name == 'NUM_RECEPTORS':
                compare([len(left.getcell('POLARIZATION_TYPE', i)) for i in range(left.nrows())],
                        right.getcol(name), label)
                continue
            if fixes and reference.name == 'POLARIZATION' and name == 'CORR_PRODUCT':
                pairs = {6:(0, 1), 7:(1, 0), 8:(1, 1), 10:(0, 1), 11:(1, 0), 12:(1, 1)}
                for i in range(left.nrows()):
                    expected = np.array([pairs.get(code, (0, 0)) for code in left.getcell('CORR_TYPE', i)]).T
                    compare(expected, right.getcell(name, i), label)
                continue
            if not left.getcoldesc(name).get('ndim', 0):
                compare(left.getcol(name), right.getcol(name), label)
                continue
            for row in range(left.nrows()):
                defined = left.iscelldefined(name, row)
                assert right.iscelldefined(name, row) == defined, (label, row)
                if defined:
                    compare(left.getcell(name, row), right.getcell(name, row), f'{label}[{row}]')


def compare_products(reference, output, fixes=None):
    compare_tables(reference, output, fixes)
    subtables = sorted(entry.name for entry in reference.iterdir()
                       if entry.is_dir() and (entry / 'table.dat').is_file())
    assert subtables == sorted(entry.name for entry in output.iterdir()
                              if entry.is_dir() and (entry / 'table.dat').is_file())
    for name in subtables:
        compare_tables(reference / name, output / name, fixes)


def check_readers(paths):
    from casars.tools import table as native_table

    for path in paths:
        with opened(path) as reference:
            native = native_table()
            try:
                assert native.open(str(path))
                assert native.nrows() == reference.nrows()
                for row in (0, reference.nrows() - 1):
                    for name in ('FLOAT_DATA', 'FLAG', 'WEIGHT', 'SIGMA', 'DATA_DESC_ID'):
                        compare(reference.getcell(name, row), native.getcell(name, row), name)
            finally:
                assert native.close()
        dataset = ms()
        try:
            assert dataset.open(str(path))
        finally:
            assert dataset.close()


def check_asap_rows(source, product):
    with opened(source) as raw, opened(product) as result, opened(product / 'DATA_DESCRIPTION') as dd, \
            opened(product / 'POLARIZATION') as pol:
        codes = {'linear': (9, 12), 'circular': (5, 8)}[raw.getkeyword('POLTYPE')]
        lookup = {}
        for row, (time, beam, window, hand) in enumerate(zip(
                *(raw.getcol(name) for name in ('TIME', 'BEAMNO', 'IFNO', 'POLNO')))):
            lookup[time * 86400., beam, window, codes[hand]] = row
        for row, (time, feed, desc) in enumerate(zip(
                *(result.getcol(name) for name in ('TIME', 'FEED1', 'DATA_DESC_ID')))):
            window = dd.getcell('SPECTRAL_WINDOW_ID', desc)
            products = pol.getcell('CORR_TYPE', dd.getcell('POLARIZATION_ID', desc))
            source_rows = [lookup[time, feed, window, corr] for corr in products]
            expected_data = np.stack([raw.getcell('SPECTRA', index) for index in source_rows])
            expected_flag = np.stack([raw.getcell('FLAGTRA', index) != 0 for index in source_rows])
            np.testing.assert_array_equal(result.getcell('FLOAT_DATA', row), expected_data)
            np.testing.assert_array_equal(result.getcell('FLAG', row), expected_flag)


def copy_asap_rows(raw, scan, rows):
    selection = raw.selectrows(rows)
    try:
        table = selection.copy(str(scan), deep=True, valuecopy=True)
        table.close()
    finally:
        selection.close()


def append_hand(table, index):
    row = {name: table.getcell(name, index) for name in table.colnames()
           if table.iscelldefined(name, index)}
    end = table.nrows()
    table.addrows(1)
    for name, value in row.items():
        table.putcell(name, end, value)


def dual_hand_rows(raw):
    keys = zip(*(raw.getcol(name) for name in ('TIME', 'BEAMNO', 'IFNO', 'FIELDNAME', 'SRCTYPE')))
    groups = {}
    for row, (key, hand) in enumerate(zip(keys, raw.getcol('POLNO'))):
        groups.setdefault(key, {})[hand] = row
    hands = next(hands for hands in groups.values() if set(hands) == {0, 1})
    return [hands[0], hands[1]]


def check_polarization_modes(root, source, factories):
    with opened(source) as raw:
        hands = dual_hand_rows(raw)
        for basis, codes in (('linear', [9, 10, 11, 12]), ('circular', [5, 6, 7, 8]),
                             ('stokes', [1, 2, 3, 4]), ('linpol', [29, 32])):
            scan = root / f'{basis}.asap'
            copy_asap_rows(raw, scan, hands)
            with opened(scan, True) as table:
                table.putkeyword('POLTYPE', basis)
                channels = len(table.getcell('SPECTRA', 0))
                inputs = np.arange(len(codes) * channels, dtype=np.float32).reshape(len(codes), channels)
                channel_flags = np.zeros_like(inputs, dtype=np.uint8)
                channel_flags[-1, -1] = 1
                if len(codes) == 4:
                    template = {name: table.getcell(name, 0) for name in table.colnames()
                                if table.iscelldefined(name, 0)}
                    table.addrows(2)
                    for row in (2, 3):
                        for name, value in template.items():
                            table.putcell(name, row, value)
                for row in range(len(codes)):
                    table.putcell('POLNO', row, row)
                    table.putcell('SPECTRA', row, inputs[row])
                    table.putcell('FLAGTRA', row, channel_flags[row])
            compare_polarization_mode(root, scan, basis, codes, inputs, channel_flags, factories)


def compare_polarization_mode(root, scan, basis, codes, inputs, channel_flags, factories):
    if basis in ('linear', 'circular'):
        column = 'DATA'
        cross = inputs[2] + 1j * inputs[3]
        values = np.stack((inputs[0], cross, cross.conj(), inputs[1]))
        flag = channel_flags[2] | channel_flags[3]
        flags = np.stack((channel_flags[0], flag, flag, channel_flags[1]))
    else:
        column = 'FLOAT_DATA'
        values, flags = inputs, channel_flags
    outputs = []
    for name, factory in factories.items():
        output = root / f'{basis}-{name}.ms'
        assert factory().importasap(str(scan), str(output), True)
        with opened(output) as table, opened(output / 'POLARIZATION') as pol:
            assert table.nrows() == 1
            np.testing.assert_array_equal(pol.getcell('CORR_TYPE', 0), codes)
            np.testing.assert_array_equal(table.getcell(column, 0), values)
            np.testing.assert_array_equal(table.getcell('FLAG', 0), flags.astype(bool))
        outputs.append(output)
    if len(outputs) == 2:
        compare_products(*outputs, fixes='asap')


def check_temperature_axes(root, source, factories):
    for channels in (1, 3):
        scan = root / f'temperature-{channels}.asap'
        with opened(source) as raw:
            copy_asap_rows(raw, scan, dual_hand_rows(raw))
        expected = np.array([list(range(80, 80 + channels)), [90] + [-1] * (channels - 1)])
        with opened(scan / 'TCAL', True) as tcal:
            ids = tcal.getcol('ID')[:2]
            tcal.putcell('TCAL', 0, expected[0] + 100)
            tcal.putcell('TCAL', 1, np.array([190.]))
        with opened(scan, True) as table:
            for row in (0, 1):
                table.putcell('FLAGTRA', row, np.zeros(channels, dtype=np.uint8))
                table.putcell('SPECTRA', row, np.ones(channels, dtype=np.float32))
                table.putcell('WEATHER_ID', row, 99999)
                table.putcell('TCAL_ID', row, int(ids[row]))
            table.putcell('TSYS', 0, expected[0])
            table.putcell('TSYS', 1, np.array([90.]))
        results = []
        for name, factory in factories.items():
            path = root / f'temperature-{channels}-{name}.ms'
            assert factory().importasap(str(scan), str(path))
            with opened(path / 'SYSCAL') as syscal:
                assert not syscal.iscelldefined('TSYS', 0)
                np.testing.assert_array_equal(syscal.getcell('TSYS_SPECTRUM', 0), expected)
                tcal_expected = np.where(expected == -1, -1, expected + 100)
                np.testing.assert_array_equal(syscal.getcell('TCAL_SPECTRUM', 0), tcal_expected)
            with opened(path / 'WEATHER') as weather:
                assert weather.nrows() == 1
                for column in ('TEMPERATURE', 'PRESSURE', 'REL_HUMIDITY', 'WIND_SPEED', 'WIND_DIRECTION'):
                    assert weather.getcell(column, 0) == 0.
            results.append(path)
        if len(results) == 2:
            compare_products(*results, fixes='asap')


def check_duplicate_plane(root, source, factories):
    scan = root / 'duplicate-plane.asap'
    with opened(source) as raw:
        copy_asap_rows(raw, scan, dual_hand_rows(raw))
    for name, factory in factories.items():
        path = root / f'duplicate-protected-{name}.ms'
        assert factory().importasap(str(scan), str(path))
    with opened(scan, True) as table:
        append_hand(table, 0)
        table.putcell('SPECTRA', 2, table.getcell('SPECTRA', 0) + 1.)
    for name, factory in factories.items():
        path = root / f'duplicate-protected-{name}.ms'
        with opened(path) as table:
            before = table.getcell('FLOAT_DATA', 0)
        for parallel in (False,) if name == 'casa' else (False, True):
            assert factory().importasap(str(scan), str(path), parallel) is False
            with opened(path) as table:
                np.testing.assert_array_equal(table.getcell('FLOAT_DATA', 0), before)


def check_linked_input(root, source, factory):
    scan = root / 'linked-weather.asap'
    shutil.copytree(source, scan)
    weather = root / 'shared-weather'
    (scan / 'WEATHER').rename(weather)
    (scan / 'WEATHER').symlink_to(weather, target_is_directory=True)
    with opened(weather) as table:
        temperatures = table.getcol('TEMPERATURE')
    assert factory().importasap(str(scan), str(weather)) is False
    with opened(weather) as table:
        np.testing.assert_array_equal(table.getcol('TEMPERATURE'), temperatures)


def run_imports(factory, name, kind, source, work):
    tool = factory()
    method = getattr(tool, 'import' + kind)
    outputs = []
    for parallel in (False, True):
        output = work / f'{name}-{parallel}.ms'
        start = time.perf_counter()
        if parallel:
            assert method(infile=str(source), outfile=str(output), parallel=True)
        else:
            assert method(str(source), str(output))
        assert output.is_dir()
        print(f'{name} parallel={parallel} took {time.perf_counter() - start:.3f}s', flush=True)
        outputs.append(output)
    existing = work / f'{name}-existing'
    existing.write_text('existing artifact')
    missing = work / 'absent'
    assert method() is False
    assert method(str(source)) is False
    assert method(str(missing), str(outputs[0])) is False
    assert method(str(source), str(existing)) is False
    assert existing.read_text() == 'existing artifact'
    assert method(str(source), str(missing / 'output.ms')) is False
    for args in ((None, str(existing)), (str(source), None), (str(source), str(existing), 1)):
        try:
            method(*args)
        except TypeError:
            pass
        else:
            raise AssertionError(f'Expected argument type error for {args!r}')
    assert tool.open(str(outputs[1]))
    try:
        assert method(str(source), str(outputs[0])) is True
        assert tool.name() == str(outputs[1])
    finally:
        assert tool.close()
    if kind == 'asap':
        different_table = work / f'{name}-existing-scan.asap'
        shutil.copytree(source, different_table)
        assert method(str(different_table), str(different_table)) is False
        assert (different_table / 'FREQUENCIES').is_dir()
        assert method(str(source), str(different_table)) is True
        compare_products(outputs[0], different_table)
        assert (source / 'FREQUENCIES').is_dir()
    return outputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('asap', 'nro'))
    parser.add_argument('source', type=Path)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    source = args.source.resolve()
    casa = run_imports(singledishms, 'casa', args.kind, source, root)
    compare_products(*casa)
    factories = {'casa': singledishms}
    if args.kind == 'asap':
        check_asap_rows(source, casa[0])
    if not args.reference_only:
        from casars.tools import singledishms as native_singledishms

        factories['native'] = native_singledishms
        native = run_imports(native_singledishms, 'native', args.kind, source, root)
        for a, b in zip(casa, native):
            compare_products(a, b, fixes=args.kind)
        if args.kind == 'asap':
            check_asap_rows(source, native[0])
        check_readers(casa + native)
    if args.kind == 'asap':
        check_polarization_modes(root, source, factories)
        check_temperature_axes(root, source, factories)
        check_duplicate_plane(root, source, factories)
        if 'native' in factories:
            check_linked_input(root, source, factories['native'])
    print(f'{args.kind.upper()} import, serial/parallel equivalence and product checks passed.', flush=True)


if __name__ == '__main__':
    main()
