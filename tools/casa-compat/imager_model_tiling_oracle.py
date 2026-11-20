#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the tiled images created by public imager.ft against pinned CASA."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casatools import imager, table
import numpy as np

from imager_legacy_pbw_oracle import data, make_input
from imager_prediction_options_oracle import empty_image


def configure(factory, dataset, volume):
    tool = factory()
    assert tool.open(str(dataset), usescratch=True)
    assert tool.selectvis(field=0, spw='0:0~9')
    assert tool.defineimage(nx=96, ny=72, cellx='15arcsec', celly='15arcsec',
                            phasecenter=0, stokes='IQUV', mode='channel', nchan=10,
                            start=0, step=1, outframe='TOPO')
    params = {} if volume is None else dict(imagetilevol=volume)
    assert tool.setoptions(numthreads=1, **params)
    return tool


def product(factory, image):
    reader = factory()
    assert reader.open(str(image))
    managers = reader.getdminfo()
    tiled = next(manager['SPEC'] for manager in managers.values()
                 if 'map' in manager.get('COLUMNS', []))
    pixels = reader.getcell('map', 0)
    assert reader.done()
    assert pixels.shape == (96, 72, 4, 10)
    return list(tiled['DEFAULTTILESHAPE']), pixels


def integer_controls(root, dataset, factories):
    class Index:
        def __index__(self):
            return 4096

    with TemporaryDirectory(dir=root) as case:
        case = Path(case)
        for engine, factory in factories:
            target = case / f'{engine}-arguments.ms'
            make_input(dataset, target, scratch=True)
            for label, value in (('numpy', np.int64(4096)), ('index', Index()), ('bool', True)):
                model = case / f'{engine}-{label}.im'
                tool = configure(factory, target, value)
                assert tool.ft(str(model)) and tool.done()
                side = 10 if label == 'bool' else 12
                assert product(table, model)[0] == [side, side, 4, 10]
            tool = configure(factory, target, 0)
            for value, expected in ((None, SystemError), (2**63, SystemError),
                                    (np.bool_(True), SystemError), (4., TypeError),
                                    ('4096', TypeError)):
                try:
                    tool.setoptions(imagetilevol=value)
                except expected:
                    pass
                else:
                    raise AssertionError((engine, value, expected))
            assert tool.done()


def run(root, dataset):
    from casars.tools import imager as native, table as native_table

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    cases = ((None, 32), (0, 32), (1, 10), (4096, 12), (-4096, 12),
             (40_000, 32), (2**31 - 1, 72), (-2**31, 32), (2**31, 32),
             (2**31 + 4096, 72), (2**32 + 4096, 12), (-2**31 - 1, 72), (2**63 - 1, 10))
    report = {}
    factories = (('casa', imager), ('native', native))
    for volume, side in cases:
        with TemporaryDirectory(dir=root) as case:
            case = Path(case)
            images = {}
            visibilities = {}
            for engine, factory in factories:
                target = case / f'{engine}.ms'
                model = case / f'{engine}.im'
                make_input(dataset, target, scratch=True)
                tool = configure(factory, target, volume)
                assert tool.ft(str(model)) and tool.done()
                visibilities[engine] = data(target, 'MODEL_DATA')
                images[engine] = model

                # The existing image keeps its tile shape after setoptions changes.
                tool = configure(factory, target, 1 if side != 10 else 4096)
                assert tool.ft(str(model)) and tool.done()
                np.testing.assert_equal(data(target, 'MODEL_DATA'), visibilities[engine])

            wanted = [side, side, 4, 10]
            for writer, model in images.items():
                for reader, factory in (('casa', table), ('native', native_table)):
                    tile, pixels = product(factory, model)
                    assert tile == wanted, (volume, writer, reader, tile)
                    np.testing.assert_array_equal(pixels, np.zeros_like(pixels))
            np.testing.assert_equal(visibilities['native'], visibilities['casa'])
            np.testing.assert_equal(empty_image(images['native']), empty_image(images['casa']))
            report[str(volume)] = wanted
            print(volume, 'tile', wanted, 'both writers and readers', flush=True)
    integer_controls(root, dataset, factories)
    (root / 'results.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    args = parser.parse_args()
    run(args.work.resolve(), args.dataset.resolve())
