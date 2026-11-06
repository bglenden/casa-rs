#!/usr/bin/env python3
"""Capture CASA array records with ignored NumPy scalar and 0-D elements."""
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def cases():
    return {
        'integers': [[1, np.array(2)], [np.array(3), 4]],
        'floats': [[1.5, np.array(2.)], [np.array(3.), 4.5]],
        'strings_before': [['a', np.array('b')], ['c', 'd']],
        'strings_last': [['a', 'b'], ['c', np.array('d')]],
        'strings_trailing': [['a', np.array('b')], [np.array('c'), np.array('d')]],
        'all_ignored': [[np.array(1), np.array(2)], [np.array(3), np.array(4)]],
        'bool_scalars': [[np.bool_(True), False], [True, np.bool_(False)]],
        'included_signed': [[np.int32(1), np.int32(2)], [np.int32(3), np.int32(4)]],
        'integer_cube': [[[1, np.array(2), 3], [4, 5, 6]], [[7, 8, 9], [10, np.array(11), 12]]],
        'string_cube': [[['a', np.array('b')], ['c', 'd']], [['e', 'f'], ['g', np.array('h')]]],
        'numeric_before_string': [[2, np.array(9)], ['a', np.array(9)]],
        'float_bool_string': [[np.bool_(True), 1.25], [True, 'a']],
        'single_row': [['a', np.array('b'), 'c', np.array('d')]],
        'single_column': [['a'], [np.array('b')], ['c'], [np.array('d')]],
        'ordinary_promotion': [[True, 1.25], ['a', 2]],
        'flat_float_then_string': [True, 1.5, 'a'],
        'flat_string_first': ['a', True, 1.5],
        'flat_integer_then_string': [True, 2, 'a'],
    }


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = {'casa_version': version_string(), 'cases': {}}
    with tempfile.TemporaryDirectory(prefix='imhead-ignored-matrix-') as temp:
        for name, value in cases().items():
            path = Path(temp) / f'{name}.im'; shutil.copytree(root / 'normal.im', path)
            imhead(str(path), mode='put', hdkey='custom', hdvalue={'value': value})
            got = imhead(str(path), mode='get', hdkey='custom')['value']
            expected['cases'][name] = {'dtype': str(got.dtype), 'shape': list(got.shape), 'values': got.tolist()}
    (root / 'ignored_matrix_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__': main()
