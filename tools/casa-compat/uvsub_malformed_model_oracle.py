#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the persisted uvsub prefix when a later field has a malformed model."""

import argparse
from pathlib import Path

import numpy as np
from casars.tools import ms
from casatools import ms as casa_ms, table

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True,
                        help='mixed-initial.ms from imager_legacy_pbw_oracle.py --mosaic')
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    fields = data(args.dataset, 'FIELD_ID')
    assert set(fields) == {0, 1} and np.all(np.diff(fields) >= 0)
    observed = data(args.dataset, 'DATA')
    for existing in (False, True):
        for reverse in (False, True):
            products = []
            for engine, factory in (('pinned', casa_ms), ('native', ms)):
                target = args.work / f'{engine}-{existing}-{reverse}.ms'
                make_input(args.dataset, target, scratch=False)
                remove_first_entry(target, 1)
                prior = (seed_corrected(target, observed + np.complex64(2. + 1.5j))
                         if existing else observed)
                refused_prediction(factory, target, reverse)
                after = data(target, 'CORRECTED_DATA')
                np.testing.assert_array_equal(after[..., fields == 1], prior[..., fields == 1])
                products.append((after - prior)[..., fields == 0])
            expected, actual = products
            assert np.max(np.abs(expected)) > 1.
            error = relative_rms(actual, expected)
            print(f'existing={existing}, reverse={reverse}, earlier field NRMS', error, flush=True)
            assert error < 1.e-5


if __name__ == '__main__':
    main()
