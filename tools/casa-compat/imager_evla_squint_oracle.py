#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare default radial beams with requested squint on and off."""

import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks
from casatools import componentlist, imager as casa_imager, ms as casa_ms, table
import numpy as np

from gaincal_oracle import relative_rms
from imager_component_voltage_oracle import compare, source
from imager_legacy_pbw_oracle import corrected_or_data, data, make_input


def telescope_input(source_ms, target, telescope):
    make_input(source_ms, target, scratch=True)
    tool = table()
    assert tool.open(str(target / 'OBSERVATION'), nomodify=False)
    for row in range(tool.nrows()):
        assert tool.putcell('TELESCOPE_NAME', row, telescope)
    assert tool.done()


def predict(factory, dataset, arguments, *, beam, squint, scratch):
    tool = factory()
    assert tool.open(str(dataset), usescratch=scratch)
    assert tool.selectvis(field=0, spw='0')
    assert tool.setvp(dovp=beam, dosquint=squint)
    assert tool.ft(**arguments) and tool.done()


def saved_images(root, seed, arguments, expected, results, label):
    from casars.tools import imager as native_imager, ms as native_ms

    for writer, factory in (('casa', casa_imager), ('native', native_imager)):
        saved = root / f'{writer}-saved.ms'
        make_input(seed, saved, scratch=False)
        predict(factory, saved, arguments, beam=True, squint=True, scratch=False)
        for reader, ms_factory in (('casa', casa_ms), ('native', native_ms)):
            target = root / f'{writer}-to-{reader}.ms'
            make_input(saved, target, scratch=False)
            before = corrected_or_data(target)
            tool = ms_factory()
            assert tool.open(str(target), nomodify=False)
            assert tool.uvsub() and tool.done()
            actual = before - data(target, 'CORRECTED_DATA')
            compare(results, f'{label}-{writer}-to-{reader}', actual, expected)


def run(root, dataset, model, telescope='EVLA'):
    from casars.tools import imager as native_imager

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    results = {}
    with TemporaryDirectory(dir=root) as attempt:
        work = Path(attempt)
        label = telescope.lower()
        seed = work / 'source.ms'
        telescope_input(dataset, seed, telescope)
        component = work / 'source.cl'
        source(component, seed, [3.7, .4, -.6, -.8])
        linear = work / 'linear.cl'
        shutil.copytree(component, linear)
        tool = componentlist()
        assert tool.open(str(linear)) and tool.convertfluxpol(0, 'Linear') and tool.done()
        cases = [('component', {'complist': str(component)}), ('linear', {'complist': str(linear)}),
                 ('image', {'model': str(model)})]
        for name, arguments in cases:
            baseline, expected = None, None
            for choice, beam, squint in [('no-beam', False, False), ('radial', True, False), ('squint', True, True)]:
                for engine, factory in [('casa', casa_imager), ('native', native_imager)]:
                    target = work / f'{name}-{choice}-{engine}.ms'
                    make_input(seed, target, scratch=True)
                    predict(factory, target, arguments, beam=beam, squint=squint, scratch=True)
                    values = data(target, 'MODEL_DATA')
                    if engine == 'casa':
                        pinned = values
                        if choice == 'no-beam':
                            baseline = values
                        elif choice == 'radial':
                            expected = values
                            assert relative_rms(expected, baseline) > 1.e-2
                        elif name == 'linear':
                            # GOFIGURE still converts a stored Linear component
                            # into circular while applying the radial beam.
                            assert relative_rms(values, expected) > .1
                        else:
                            np.testing.assert_array_equal(values, expected)
                    elif beam:
                        compare(results, f'{label}-{name}-{choice}', values, pinned)
                    for column in ('DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT'):
                        np.testing.assert_array_equal(data(target, column), data(seed, column))
            if name == 'image':
                saved_images(work, seed, arguments, expected, results, label)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    parser.add_argument('--telescope', default='EVLA')
    args = parser.parse_args()
    run(args.work.resolve(), args.dataset.resolve(), args.model.resolve(), args.telescope)
