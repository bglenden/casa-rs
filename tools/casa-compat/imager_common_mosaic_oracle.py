#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy VLA common-beam scratch and virtual predictions."""

import argparse
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys

import casatasks
from casatools import image, imager, table

from gaincal_oracle import relative_rms
from imager_facets_oracle import model_grids, model_recipe
from imager_mosaic_oracle import compare, consume, field_samples, fixture


def prepare(root):
    source, _, model = fixture(root)
    tables = table()
    assert tables.open(str(source / 'OBSERVATION'), nomodify=False)
    assert tables.putcell('TELESCOPE_NAME', 0, 'VLA') and tables.close()
    assert tables.open(str(source / 'SPECTRAL_WINDOW'), nomodify=False)
    for column in ('CHAN_FREQ', 'REF_FREQUENCY', 'CHAN_WIDTH',
                   'EFFECTIVE_BW', 'RESOLUTION', 'TOTAL_BANDWIDTH'):
        assert tables.putcol(column, .015 * tables.getcol(column))
    assert tables.close()

    ia = image()
    assert ia.open(str(model))
    pixels = ia.getchunk()
    pixels[91, 145, 0, 0] = 1.4
    coords = ia.coordsys()
    assert coords.setincrement('-6arcsec 6arcsec', type='direction')
    assert coords.setreferencevalue('1.5GHz', type='spectral')
    assert coords.setincrement('450kHz', type='spectral')
    assert ia.putchunk(pixels) and ia.setcoordsys(coords.torecord())
    assert coords.done() and ia.done()
    return source, model


def predict(factory, source, output, model, scratch, machine='mosaic', *, child=False):
    shutil.copytree(source, output)
    if not scratch:
        tables = table()
        assert tables.open(str(output), nomodify=False)
        assert tables.removecols('MODEL_DATA') and tables.close()
    tool = factory()
    assert tool.open(str(output), usescratch=scratch) and tool.selectvis(field='0,1')
    assert tool.defineimage(nx=320, ny=320, cellx='6arcsec', celly='6arcsec',
                            phasecenter=0, outframe='TOPO')
    assert tool.setoptions(ftmachine=machine, numthreads=1)
    assert tool.ft(str(model))
    if child:
        print('CASA finished ft and entered imager.done()', flush=True)
    assert tool.done()


def casa_reference(root, scratch):
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), '--casa-reference',
         'scratch' if scratch else 'virtual', '--work', str(root)],
        capture_output=True, text=True, check=False)
    if result.returncode != 0:
        assert result.returncode == -signal.SIGABRT, result.stderr
        assert 'CASA finished ft and entered imager.done()' in result.stdout
        assert 'pure virtual method called' in result.stderr, result.stderr
    return result.returncode


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, model = prepare(root)
    reference_exit = {name: casa_reference(root, scratch)
                      for name, scratch in [('scratch', True), ('virtual', False)]}
    predict(imager, source, root / 'unweighted.ms', model, True, 'ft')
    expected = field_samples(root / 'casa-scratch.ms')
    unweighted = field_samples(root / 'unweighted.ms')
    assert all(relative_rms(a, b) > .01 for a, b in zip(expected, unweighted, strict=True))
    predict(native, source, root / 'native-scratch.ms', model, True)
    evidence = {'scratch': compare(field_samples(root / 'native-scratch.ms'), expected),
                'casa_reference_exit': reference_exit}

    predict(native, source, root / 'native-virtual.ms', model, False)
    for writer in ('casa', 'native'):
        grid, = model_grids(model_recipe(root / f'{writer}-virtual.ms'))
        assert grid['name'] == 'MosaicFT' and grid['telescope'] == 'VLA'
        assert grid['convsampling'] == 16 and grid['pbconvfunc']['name'] == 'SimplePBConvFunc'
    reference = consume(root / 'casa-virtual.ms', root / 'virtual-casa-casa.ms', 'casa')
    assert all(relative_rms(a, b) > .001 for a, b in zip(reference, expected, strict=True))
    for writer, reader in [('casa', 'native'), ('native', 'casa'), ('native', 'native')]:
        label = f'virtual-{writer}-{reader}'
        actual = consume(root / f'{writer}-virtual.ms', root / f'{label}.ms', reader)
        evidence[label] = compare(actual, reference)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--casa-reference', choices=['scratch', 'virtual'])
    args = parser.parse_args()
    root = args.work.resolve()
    if args.casa_reference:
        assert casatasks.version_string() == '6.7.6.14'
        predict(imager, root / 'source.ms', root / f'casa-{args.casa_reference}.ms',
                root / 'mosaic.im', args.casa_reference == 'scratch', child=True)
        return
    evidence = run(root)
    (root / 'results.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps(evidence, indent=2), flush=True)


if __name__ == '__main__':
    main()
