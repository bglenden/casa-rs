#!/usr/bin/env python3
"""Compare CASA and native nested array promotion across shapes and orderings."""
import itertools
import json
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string
from imhead_ignored_matrix_interop import typed


def cases():
    for sequence in itertools.product('sxif', repeat=4):
        if 's' not in sequence or 'x' not in sequence: continue
        values = [{'s': 'a', 'x': np.array(9), 'i': 2, 'f': 1.25}[key] for key in sequence]
        yield ''.join(sequence), [values[:2], values[2:]]
    for sequence in itertools.permutations('sifb', 4):
        values = [{'s': 'a', 'i': 2, 'f': 1.25, 'b': True}[key] for key in sequence]
        yield ''.join(sequence), [values[:2], values[2:]]
    yield 'large-integer-float', [[2**31, 1.5], [1, 2]]
    yield 'large-integer-string', [[True, 2**63 - 1], ['a', 2]]
    for shape in [(1, 4), (4, 1), (2, 1, 2), (1, 2, 1, 2), (1, 1, 1, 1, 4), (2, 2, 2), (2, 3, 2), (2, 3, 4), (3, 2, 1, 2)]:
        for phase in range(5):
            choices = ['a', np.array(9), 1.25, True, np.complex128(1 + 2j), np.uint32(6)]
            values = np.empty(math.prod(shape), dtype=object)
            values[:] = [choices[(index + phase) % len(choices)] for index in range(values.size)]
            yield f'{shape}:{phase}', values.reshape(shape).tolist()


def main():
    binary = Path(sys.argv[1]).resolve()
    source = Path('crates/casars-imexplore/tests/data/imhead_crpix/normal.im')
    count = 0
    with tempfile.TemporaryDirectory(prefix='imhead-array-exhaustive-') as temp:
        casa_path = Path(temp) / 'casa.im'; native_path = Path(temp) / 'native.im'
        shutil.copytree(source, casa_path); shutil.copytree(source, native_path)
        for name, value in cases():
            imhead(str(casa_path), mode='put', hdkey='custom', hdvalue={'value': value})
            expected = imhead(str(casa_path), mode='get', hdkey='custom')['value']
            request = {'surface': 'imhead', 'values': {'imagename': str(native_path), 'mode': 'put', 'hdkey': 'custom'},
                       'typed_values': {'hdvalue': {'kind': 'table', 'value': {'value': typed(value)}}}}
            result = subprocess.run([str(binary), 'imhead', '--json-run', '-'], input=json.dumps(request),
                                    text=True, capture_output=True, check=True)
            assert json.loads(result.stdout)['status'] == 'succeeded', (name, result.stdout)
            actual = imhead(str(native_path), mode='get', hdkey='custom')['value']
            assert actual.dtype == expected.dtype and actual.shape == expected.shape, (name, expected, actual)
            assert np.array_equal(actual, expected, equal_nan=actual.dtype.kind in 'fc'), (name, expected, actual)
            count += 1
    print(f"CASA {version_string()} matched {count} nested-array promotion cases")


if __name__ == '__main__': main()
