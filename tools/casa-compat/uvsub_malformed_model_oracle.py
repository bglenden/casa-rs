#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the persisted uvsub prefix when a later field has a malformed model."""

import argparse
from pathlib import Path

import numpy as np
from casars.tools import ms
from casatools import calibrater, ms as casa_ms, table

from imager_legacy_pbw_oracle import data, make_input, relative_rms


def remove_first_entry(dataset, field):
    writer = table()
    assert writer.open(str(dataset / 'SOURCE'), nomodify=False)
    keyword = writer.getkeyword(f'definedmodel_field_{field}')
    row = writer.getkeyword(keyword)
    record = writer.getcell('SOURCE_MODEL', row)
    assert record['numft'] >= 1 and record.pop('ft_0') is not None
    assert writer.putcell('SOURCE_MODEL', row, record) and writer.done()


def seed_corrected(dataset, input_values):
    writer = table()
    assert writer.open(str(dataset), nomodify=False)
    descriptor = writer.getcoldesc('DATA')
    descriptor.update(dataManagerType='StandardStMan', dataManagerGroup='ExistingCorrectedControl')
    assert writer.addcols({'CORRECTED_DATA': descriptor})
    assert writer.putcol('CORRECTED_DATA', input_values) and writer.done()
    return data(dataset, 'CORRECTED_DATA')


def refused_prediction(factory, dataset, reverse):
    tool = factory()
    assert tool.open(str(dataset), nomodify=False)
    try:
        tool.uvsub(reverse=reverse)
    except RuntimeError as error:
        assert 'ft_0' in str(error), str(error)
    else:
        raise AssertionError('uvsub accepted a missing Fourier entry')
    finally:
        assert tool.done()


def interleave_fields(source, target):
    reader = table()
    assert reader.open(str(source))
    sorted_rows = reader.query(sortlist='ANTENNA1, ANTENNA2, TIME DESC')
    assert sorted_rows.copy(str(target), deep=True, valuecopy=True).done()
    assert sorted_rows.done() and reader.done()
    assert reader.open(str(target), nomodify=False)
    for keyword in ('SORTED_TABLE', 'SORT_COLUMNS'):
        if keyword in reader.keywordnames():
            assert reader.removekeyword(keyword)
    assert reader.done()
    fields = data(target, 'FIELD_ID')
    assert fields[0] == 1 and np.any(np.diff(fields) > 0) and np.any(np.diff(fields) < 0)


def seed_compressed(dataset, later):
    writer = table()
    assert writer.open(str(dataset / 'SOURCE'))
    original = []
    for field in (0, 1):
        key = writer.getkeyword(f'definedmodel_field_{field}')
        row = writer.getkeyword(key)
        original.append((field, key, row, writer.getcell('SOURCE_MODEL', row)))
    assert writer.done()
    tool = calibrater()
    assert tool.open(str(dataset), compress=True, addcorr=True, addmodel=False) and tool.done()
    assert writer.open(str(dataset / 'SOURCE'), nomodify=False)
    for field, key, row, recipe in original:
        assert writer.putkeyword(f'definedmodel_field_{field}', key)
        assert writer.putkeyword(key, row) and writer.putcell('SOURCE_MODEL', row, recipe)
    assert writer.done()

    names = ('CORRECTED_DATA_COMPRESSED', 'CORRECTED_DATA_SCALE', 'CORRECTED_DATA_OFFSET')
    assert writer.open(str(dataset), nomodify=False)
    packed, scales, offsets = (writer.getcol(name) for name in names)
    packed[..., later] = (1 << 16) + 4
    packed[0, 0, later] = 0
    scales[later] = 1.
    offsets[later] = 0.
    for name, values in zip(names, (packed, scales, offsets), strict=True):
        assert writer.putcol(name, values)
    assert writer.done()
    return {name: data(dataset, name)[..., later] for name in names}


def compare_refusal(root, source, *, existing, reverse, compressed=False):
    fields = data(source, 'FIELD_ID')
    observed = data(source, 'DATA')
    products = []
    for engine, factory in (('pinned', casa_ms), ('native', ms)):
        target = root / f'{engine}-{existing}-{reverse}-{compressed}.ms'
        make_input(source, target, scratch=False)
        later = fields == 1
        backing = seed_compressed(target, later) if compressed else {}
        remove_first_entry(target, 1)
        prior = (data(target, 'CORRECTED_DATA') if compressed else
                 seed_corrected(target, observed + np.complex64(2. + 1.5j))
                 if existing else observed)
        refused_prediction(factory, target, reverse)
        after = data(target, 'CORRECTED_DATA')
        np.testing.assert_array_equal(after[..., later], prior[..., later])
        for name, raw in backing.items():
            np.testing.assert_array_equal(data(target, name)[..., later], raw)
        products.append((after - prior)[..., ~later])
    expected, actual = products
    assert np.max(np.abs(expected)) > 1.
    error = relative_rms(actual, expected)
    print(f'{root.name}, existing={existing}, reverse={reverse}, compressed={compressed}',
          'earlier field NRMS', error, flush=True)
    assert error < (1.e-4 if compressed else 1.e-5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True,
                        help='mixed-initial.ms from imager_legacy_pbw_oracle.py --mosaic')
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    fields = data(args.dataset, 'FIELD_ID')
    assert set(fields) == {0, 1} and np.all(np.diff(fields) >= 0)
    for existing in (False, True):
        for reverse in (False, True):
            compare_refusal(args.work, args.dataset, existing=existing, reverse=reverse)
    interleaved = args.work / 'interleaved.ms'
    interleave_fields(args.dataset, interleaved)
    varied = args.work / 'interleaved'
    varied.mkdir()
    compare_refusal(varied, interleaved, existing=False, reverse=False)
    compare_refusal(varied, interleaved, existing=True, reverse=False, compressed=True)


if __name__ == '__main__':
    main()
