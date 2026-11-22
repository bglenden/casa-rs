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


def compare_tables(reference, output):
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
            if not left.getcoldesc(name).get('ndim', 0):
                compare(left.getcol(name), right.getcol(name), label)
                continue
            for row in range(left.nrows()):
                defined = left.iscelldefined(name, row)
                assert right.iscelldefined(name, row) == defined, (label, row)
                if defined:
                    compare(left.getcell(name, row), right.getcell(name, row), f'{label}[{row}]')


def compare_products(reference, output):
    compare_tables(reference, output)
    subtables = sorted(entry.name for entry in reference.iterdir()
                       if entry.is_dir() and (entry / 'table.dat').is_file())
    assert subtables == sorted(entry.name for entry in output.iterdir()
                              if entry.is_dir() and (entry / 'table.dat').is_file())
    for name in subtables:
        compare_tables(reference / name, output / name)


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


def check_polarization_modes(root, source, factories):
    with opened(source) as raw:
        keys = zip(*(raw.getcol(name) for name in ('TIME', 'BEAMNO', 'IFNO', 'FIELDNAME', 'SRCTYPE')))
        groups = {}
        for row, (key, hand) in enumerate(zip(keys, raw.getcol('POLNO'))):
            groups.setdefault(key, {})[hand] = row
        hands = next(hands for hands in groups.values() if set(hands) == {0, 1})
        for basis, codes in (('linear', [9, 10, 11, 12]), ('circular', [5, 6, 7, 8]),
                             ('stokes', [1, 2, 3, 4]), ('linpol', [29, 32])):
            scan = root / f'{basis}.asap'
            selection = raw.selectrows([hands[0], hands[1]])
            try:
                table = selection.copy(str(scan), deep=True, valuecopy=True)
                table.close()
            finally:
                selection.close()
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
        compare_products(*outputs)


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
            compare_products(a, b)
        if args.kind == 'asap':
            check_asap_rows(source, native[0])
        check_readers(casa + native)
    if args.kind == 'asap':
        check_polarization_modes(root, source, factories)
    print(f'{args.kind.upper()} import, serial/parallel equivalence and product checks passed.', flush=True)


if __name__ == '__main__':
    main()
