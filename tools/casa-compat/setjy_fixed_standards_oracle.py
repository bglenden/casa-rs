#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare each fixed setjy standard and its saved visibilities with CASA."""

import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

from casatools import table
import numpy as np

from setjy_manual_oracle import casa_setjy, compare_returns, rust_setjy, verify_case
from setjy_perley_butler_oracle import rename


SCALES = ('Baars', 'Perley 90', 'Perley-Taylor 95', 'Perley-Taylor 99',
          'Perley-Butler 2010', 'Stevens-Reynolds 2016')
SOURCES = ('3C286', '3C48', '3C147', '3C138', 'J1939-6342', '3C295', '3C196')


def named_matrix(seed, root):
    for name in SOURCES:
        with TemporaryDirectory(prefix='fixed-standards-', dir=root) as directory:
            paths = [Path(directory) / f'{engine}.ms' for engine in ('casa', 'rust')]
            for path in paths:
                rename(seed, path, name, shift=True)
            for scale in (*SCALES, 'manual'):
                args = dict(field='0', spw='0', usescratch=False, standard=scale, fluxdensity=-1.)
                expected = casa_setjy(str(paths[0]), **args)
                actual = rust_setjy(str(paths[1]), **args)
                compare_returns(actual, expected)
                print(name, scale, actual['0']['0']['fluxd'][0], flush=True)


def break_window(dataset, break_hz):
    writer = table()
    assert writer.open(str(dataset / 'SPECTRAL_WINDOW'), nomodify=False)
    original = writer.getcell('CHAN_FREQ', 0)
    center = len(original) // 2
    shifted = break_hz + (np.arange(len(original)) - center) * abs(original[1] - original[0])
    assert shifted[center] == break_hz and shifted[0] < break_hz < shifted[-1]
    assert writer.putcell('CHAN_FREQ', 0, shifted)
    assert writer.putcell('REF_FREQUENCY', 0, break_hz) and writer.done()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('vla_ms', type=Path, help='VLA fixture with field 0 and one populated SPW')
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    named_matrix(args.vla_ms, args.root)

    target = args.root / 'steady.ms'
    rename(args.vla_ms, target, '3C286')
    for case, scale in enumerate(SCALES[:3]):
        verify_case(target, args.root, f'fixed-{case}', {'interpolation': ''}, standard=scale)
    for case, (scale, break_hz) in enumerate((
        ('Perley-Taylor 99', 10e9), ('Perley-Butler 2010', 10e9),
        ('Stevens-Reynolds 2016', 11.1496e9),
    )):
        target = args.root / f'boundary-{case}.ms'
        rename(args.vla_ms, target, '1934-638', shift=True)
        break_window(target, break_hz)
        verify_case(target, args.root, f'fixed-break-{case}', {}, standard=scale)
        verify_case(target, args.root, f'fixed-break-median-{case}', {'scalebychan': False}, standard=scale)
    verify_case(args.vla_ms, args.root, 'negative-manual-fallback', {'fluxdensity': -2.})


if __name__ == '__main__':
    main()
