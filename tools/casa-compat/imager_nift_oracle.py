#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the legacy nift mode and read its replacement virtual recipe."""

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys

import casatasks
from casatools import imager, table
import numpy as np

from imager_common_mosaic_oracle import prepare
from imager_custom_mosaic_oracle import copy_input
from imager_facets_oracle import model_grids, model_recipe
from imager_mosaic_oracle import compare, consume, field_samples
from imager_prediction_options_oracle import coordinates, write_model


def predict(factory, root, destination, machine, settings, *, scratch):
    convolution, facets, interpolation = settings
    copy_input(root / 'source.ms', destination, scratch)
    tool = factory()
    assert tool.open(str(destination), usescratch=scratch) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=32, ny=32, cellx='50arcsec', celly='50arcsec',
                            phasecenter=0, facets=facets, outframe='TOPO')
    assert tool.setoptions(ftmachine=machine, gridfunction=convolution,
                           freqinterp=interpolation, numthreads=1)
    print('Configured imager for ft', flush=True)
    assert tool.ft(str(root / 'nift.im')) and tool.done()


def reference_nift(root, destination, settings, *, scratch):
    convolution, facets, interpolation = settings
    command = [sys.executable, str(Path(__file__).resolve()), '--work', str(root),
               '--casa-reference', destination.name, '--convolution', convolution,
               '--facets', str(facets), '--interpolation', interpolation]
    if scratch:
        command.append('--scratch')
    child = subprocess.run(command, capture_output=True, text=True, check=False)
    assert 'Configured imager for ft' in child.stdout
    if child.returncode:
        assert not scratch or facets > 1, child.stderr
        assert child.returncode in (-signal.SIGABRT, -signal.SIGSEGV), child.stderr
        if child.returncode == -signal.SIGABRT:
            assert 'bad_alloc' in child.stderr, child.stderr
    return child.returncode


def run(root):
    from casars.tools import imager as native

    root.mkdir(parents=True)
    source, _ = prepare(root, False)
    tables = table()
    assert tables.open(str(source / 'SPECTRAL_WINDOW'))
    frequency = tables.getcell('CHAN_FREQ', 0)[0]
    assert tables.done()
    assert tables.open(str(source), nomodify=False)
    assert tables.putcol('UVW', 70. * tables.getcol('UVW')) and tables.done()
    write_model(root / 'nift.im', coordinates(source), frequency, 1.7, cell=50)

    cases = [('SF', 1, 'nearest'), ('BOX', 1, 'nearest'), ('SF', 2, 'linear')]
    results = {'casa-nift-scratch-exits': {}}
    for index, settings in enumerate(cases):
        outputs = {}
        for label, factory, machine in [('casa-ft', imager, 'ft'),
                                         ('native-nift', native, 'nift')]:
            path = root / f'{index}-{label}.ms'
            predict(factory, root, path, machine, settings, scratch=True)
            outputs[label] = field_samples(path)
        expected = outputs['casa-ft']
        assert all(abs(values).max() > .2 for values in expected)
        target = root / f'{index}-casa-nift.ms'
        code = reference_nift(root, target, settings, scratch=True)
        results['casa-nift-scratch-exits'][index] = code
        if code == 0:
            actual = field_samples(target)
            assert all(np.array_equal(a, b) for a, b in zip(actual, expected, strict=True))
        results[f'{index}-scratch'] = compare(outputs['native-nift'], expected)

    casa_virtual = root / 'casa-nift-virtual.ms'
    virtual_code = reference_nift(root, casa_virtual, cases[0], scratch=False)
    results['casa-nift-virtual-exit'] = virtual_code

    for label, factory, machine in [('casa-ft', imager, 'ft'), ('native-nift', native, 'nift')]:
        path = root / f'{label}-virtual.ms'
        predict(factory, root, path, machine, cases[0], scratch=False)
        grid, = model_grids(model_recipe(path))
        assert grid['name'] == 'GridFT'
    expected = consume(root / 'casa-ft-virtual.ms', root / 'casa-ft-reopened.ms', 'casa')
    for writer, reader in [('casa-ft', 'native'), ('native-nift', 'casa'), ('native-nift', 'native')]:
        label = f'{writer}-{reader}'
        observed = consume(root / f'{writer}-virtual.ms', root / f'{label}.ms', reader)
        results[label] = compare(observed, expected)
    if virtual_code == 0:
        for reader in ('casa', 'native'):
            label = f'casa-nift-{reader}'
            observed = consume(casa_virtual, root / f'{label}.ms', reader)
            results[label] = compare(observed, expected)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--casa-reference')
    parser.add_argument('--convolution')
    parser.add_argument('--facets', type=int)
    parser.add_argument('--interpolation')
    parser.add_argument('--scratch', action='store_true')
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    root = args.work.resolve()
    if args.casa_reference:
        settings = args.convolution, args.facets, args.interpolation
        predict(imager, root, root / args.casa_reference, 'nift', settings, scratch=args.scratch)
        return
    results = run(root)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)


if __name__ == '__main__':
    main()
