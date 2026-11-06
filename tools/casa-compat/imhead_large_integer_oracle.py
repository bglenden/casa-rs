#!/usr/bin/env python3
"""Capture CASA's integer conversion for imhead header writes."""
import json
import shutil
import tempfile
from pathlib import Path

from casatasks import imhead
from casatools import version_string


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if hasattr(value, 'tolist'):
        return value.tolist()
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    values = [2147483648, -2147483649, 4294967295, 4294967296,
              1099511627776, 9223372036854775807, -9223372036854775808]
    cases = [('custom', value) for value in values]
    cases += [('crpix1', value) for value in values]
    cases += [('custom', {'v': value}) for value in values]
    cases += [('custom', {'v': [2147483648, 2]}),
              ('custom', {'v': [2147483648, 1.5]}),
              ('custom', {'v': [2147483648, 'abc']}),
              ('custom', {'v': [[2147483648, 4294967295], [1, -2147483649]]}),
              ('cdelt1', {'unit': 'arcsec', 'value': 2147483648})]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-large-int-') as temp:
        for index, (key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'
            shutil.copytree(root / 'normal.im', path)
            mode = 'add' if key == 'custom' else 'put'
            result = imhead(str(path), mode=mode, hdkey=key, hdvalue=value)
            expected['cases'].append({'mode': mode, 'key': key, 'value': value,
                                      'return': result,
                                      'get': plain(imhead(str(path), mode='get', hdkey=key))})
    (root / 'large_integer_expected.json').write_text(
        json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
