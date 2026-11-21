#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct imager.predictcomp tables with installed CASA and both readers."""

from contextlib import chdir
import inspect
from pathlib import Path
from tempfile import TemporaryDirectory

from casars.tools import componentlist as native_components, imager as native_imager
from casatools import componentlist as casa_components, imager as casa_imager, measures
import numpy as np


CATALOGUE = 'Perley-Butler 2017'
SOLAR = 'Butler-JPL-Horizons 2010'
READERS = (casa_components, native_components)
IMAGERS = (casa_imager, native_imager)


def read_component(factory, path):
    reader = factory()
    assert reader.open(path)
    assert reader.length() == 1
    record = reader.getcomponent(0)
    reader.done()
    return record


def compare(actual, expected, label):
    if isinstance(expected, dict):
        assert set(actual) == set(expected), (label, set(actual), set(expected))
        for key, value in expected.items():
            compare(actual[key], value, f'{label}.{key}')
    elif isinstance(expected, str):
        assert actual == expected, (label, actual, expected)
    else:
        np.testing.assert_allclose(actual, expected, rtol=3e-7, atol=2e-11, err_msg=label)


def files_and_records(root):
    epoch = measures()
    cases = [
        ('fixed defaults', ('3C286', 'Baars'), {}),
        ('catalogue defaults', ('3C295', CATALOGUE), {}),
        ('aliases', ('J0137+3309', 'Perley & Butler 17', '2017/09/04/00:00:00', 1.7e9), {}),
        ('variable 2013', ('3C138', 'Perley-Butler 2013', '2012/06/13/03:10:00'),
         {'freqs': [1.4e9, 2.3e9, 3.4e9]}),
        ('catalogue extended alias', ('3C405', CATALOGUE, '58000.25d'), {'freqs': [1e9, 4e9]}),
        ('descending', ('3C286', CATALOGUE, epoch.epoch('UTC', '58000.0d'), [3e9, 2e9, 1e9]), {}),
        ('moving record epoch', ('Mars', SOLAR, epoch.epoch('UTC', '55000.25d')),
         {'freqs': [90e9, 95e9, 110e9]}),
        ('moving legacy alias', ('Mars', 'Butler-JPL-Horizons 2012', '55000.25d', [90e9]), {}),
        ('moving TAI', ('Uranus', 'JPL', epoch.epoch('TAI', '55000.25d'), [40e9, 100e9]), {}),
        ('fixed positional', ('3C286', 'Perley-Taylor 99', '58000.25d', [1e9, 2e9]), {}),
        ('fixed SH12', ('3C196', 'Scaife-Heald 2012', '58000.25d', [110e6, 145e6]), {}),
    ]
    for index, (case, args, kwargs) in enumerate(cases):
        expected = None
        expected_name = None
        for writer in IMAGERS:
            directory = root / writer.__module__.split('.')[0]
            directory.mkdir(exist_ok=True)
            tool = writer()
            prefix = str(directory / f'{index}_')
            path = tool.predictcomp(*args, **kwargs, pfx=prefix)
            name = path.removeprefix(prefix)
            if expected_name is None:
                expected_name = name
            assert path and name == expected_name and Path(path).is_dir(), (case, path, expected_name)
            for reader in READERS:
                record = read_component(reader, path)
                if expected is None:
                    expected = record
                compare(record, expected, f'{case} {writer.__module__} to {reader.__module__}')
                print(f'{case}: {writer.__module__} -> {reader.__module__}', flush=True)
            tool.done()


def lifecycle_and_errors(root):
    for i, writer in enumerate(IMAGERS):
        tool = writer()
        folder = root / f'default-{i}'
        folder.mkdir()
        with chdir(folder):
            default = tool.predictcomp('3C286', 'Perley 90')
            assert default == 'predictcompspw0_3C286_100GHz0d.cl'
            read_component(casa_components, default)

        prefix = str(root / f'collision-{i}_')
        original = tool.predictcomp('3C286', CATALOGUE, 58000., [1e9, 2e9], prefix)
        original_record = read_component(casa_components, original)
        tool.done()
        again = tool.predictcomp('3C286', 'Baars', 58000., [1e9, 4e9], prefix)
        assert again == original
        compare(read_component(casa_components, again), original_record, 'existing table')
        assert tool.predictcomp('unrecognized', CATALOGUE, 58000., [1e9], prefix) == ''
        assert tool.predictcomp('3C123', 'Perley-Taylor 99', 58000., [1e9], prefix) == ''
        for args, message in (
            ((), 'is not a recognized flux density scale'),
            (('3C286', 'undefined'), 'is not a recognized flux density scale'),
            (('Mars', SOLAR, 58000.), 'Unknown is not a supported component type'),
        ):
            try:
                tool.predictcomp(*args, pfx=prefix)
            except RuntimeError as error:
                assert message in str(error), error
            else:
                raise AssertionError((writer, args, 'expected RuntimeError'))
        tool.done()


def main():
    assert inspect.signature(casa_imager().predictcomp) == inspect.signature(native_imager().predictcomp)
    with TemporaryDirectory(prefix='imager-predictcomp-') as tmp:
        root = Path(tmp)
        files_and_records(root)
        lifecycle_and_errors(root)


if __name__ == '__main__':
    main()
