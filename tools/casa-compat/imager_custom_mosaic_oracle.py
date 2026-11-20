#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy MosaicFT with a VPManager image table."""

import argparse
import json
from pathlib import Path
import shutil

import casatasks
from casatools import image, imager, table, vpmanager
import numpy as np

from gaincal_oracle import relative_rms
from imager_facets_oracle import model_grids, model_recipe
from imager_mosaic_oracle import compare, consume, field_samples, fixture, predict


def voltage_table(root, model, *, split=False, with_imaginary=True):
    ia = image()
    assert ia.open(str(model))
    coords = ia.coordsys()
    assert coords.setstokes(['XX', 'XY', 'YX', 'YY'])
    record = coords.torecord()
    assert coords.done() and ia.done()
    x, y = np.meshgrid(np.arange(320) - 160, np.arange(320) - 160, indexing='ij')
    amplitude = np.exp(-((x - 8) / 71)**2 - ((y + 3) / 58)**2)
    manager = vpmanager()
    assert manager.reset()
    for index, names in enumerate((['A0', 'A1'], ['A2', 'A3', 'A4'])):
        voltage = np.zeros((320, 320, 4, 1), dtype=np.complex64)
        voltage[:, :, 0, 0] = amplitude
        voltage[:, :, 3, 0] = amplitude * (.96 - .07 * index) * np.exp(.001j * index * x)
        beam = root / f'beam-{index}.im'
        pixels = voltage.real if split else voltage
        assert ia.fromarray(str(beam), pixels, csys=record) and ia.done()
        if split:
            imaginary = root / f'imaginary-{index}.im'
            paths = {'realimage': str(beam)}
            if with_imaginary:
                assert ia.fromarray(str(imaginary), voltage.imag, csys=record) and ia.done()
                paths['imagimage'] = str(imaginary)
        else:
            paths = {'compleximage': str(beam)}
        assert manager.setpbimage(telescope='ALMA', antnames=names, **paths)
    path = root / 'beam.tab'
    assert manager.setuserdefault(vplistnum=0, telescope='ALMA')
    assert manager.saveastable(str(path)) and manager.reset()
    return path


def copy_input(source, target, scratch):
    shutil.copytree(source, target)
    if not scratch:
        tables = table()
        assert tables.open(str(target), nomodify=False)
        assert tables.removecols('MODEL_DATA') and tables.done()


def define_mosaic(tool):
    assert tool.defineimage(nx=320, ny=320, cellx='0.3arcsec', celly='0.3arcsec',
                            phasecenter=0, outframe='TOPO')
    assert tool.setoptions(ftmachine='mosaic', numthreads=1)


def open_mosaic(factory, target, scratch):
    tool = factory()
    assert tool.open(str(target), usescratch=scratch) and tool.selectvis(field='0,1')
    define_mosaic(tool)
    return tool


def custom_predict(factory, source, target, model, pattern, scratch, dovp, *, succeeds=True):
    copy_input(source, target, scratch)
    tool = open_mosaic(factory, target, scratch)
    assert tool.setvp(dovp=dovp, usedefaultvp=False, vptable=str(pattern))
    assert tool.ft(str(model)) is succeeds
    assert tool.done()
    if succeeds and not scratch:
        grid, = model_grids(model_recipe(target))
        beam = grid['pbconvfunc']
        assert grid['name'] == 'MosaicFT' and beam['name'] == 'HetArrayConvFunc'
        assert beam['pbclass'] == 8 and beam['vptable'] == str(pattern)


def lifecycle(root, source, model, pattern, native):
    evidence = {}
    for scratch in (True, False):
        for initial in (True, False):
            predictions = {}
            for name, factory in [('casa', imager), ('native', native)]:
                path = root / f'lifecycle-{scratch}-{initial}-{name}.ms'
                copy_input(source, path, scratch)
                tool = open_mosaic(factory, path, scratch)
                assert tool.setvp(dovp=False, usedefaultvp=not initial, vptable=str(pattern))
                observed = []
                for phase in ('initial', 'setvp', 'reset'):
                    assert tool.ft(str(model)), (name, scratch, initial, phase)
                    output = root / f'{path.stem}-{phase}.ms'
                    observed.append(field_samples(path) if scratch else consume(path, output, 'casa'))
                    if phase == 'initial':
                        assert tool.setvp(dovp=False, usedefaultvp=initial, vptable=str(pattern))
                    elif phase == 'setvp':
                        if scratch:
                            assert tool.setoptions(ftmachine='mosaic', numthreads=1)
                        else:
                            assert tool.selectvis(field='0,1')
                        assert tool.setvp(dovp=False, usedefaultvp=initial, vptable=str(pattern))
                assert tool.done()
                assert all(np.array_equal(a, b) for a, b in zip(observed[0], observed[1], strict=True))
                assert all(relative_rms(a, b) > .1 for a, b in zip(observed[0], observed[2], strict=True))
                predictions[name] = observed
            for index, phase in enumerate(('initial', 'setvp', 'reset')):
                evidence[f'{scratch}-{initial}-{phase}'] = compare(
                    predictions['native'][index], predictions['casa'][index])
    return evidence


def malformed_tables(root, source, model, pattern, published, native):
    evidence = {}
    tables = table()
    for label in ('missing-defaults', 'invalid-index'):
        malformed = root / f'{label}.tab'
        shutil.copytree(pattern, malformed)
        if label == 'missing-defaults':
            assert tables.open(str(malformed), nomodify=False)
            assert tables.removekeyword('VPLIST_DEFAULTS') and tables.done()
        else:
            assert tables.open(str(malformed / 'VPLIST_DEFAULTS'), nomodify=False)
            assert tables.putcell('vplistnum', 0, 99) and tables.done()
        assert vpmanager().reset()
        custom_predict(imager, source, root / f'{label}-casa.ms', model, malformed, False, False,
                       succeeds=False)
        target = root / f'{label}-native.ms'
        shutil.copytree(published, target)
        (before,) = model_grids(model_recipe(target))
        tool = open_mosaic(native, target, False)
        assert tool.setvp(dovp=False, usedefaultvp=False, vptable=str(malformed))
        assert not tool.ft(str(model)) and tool.done()
        (after,) = model_grids(model_recipe(target))
        assert before['diskimage'] == after['diskimage']
        assert before['pbconvfunc']['vptable'] == after['pbconvfunc']['vptable']
        evidence[label] = True
    return evidence


def split_predictions(root, source, model, reference, native):
    results = {}
    for with_imaginary in (True, False):
        split = root / f'split-images-{with_imaginary}'
        split.mkdir()
        pattern = voltage_table(split, model, split=True, with_imaginary=with_imaginary)
        custom_predict(imager, source, split / 'casa-unmodified.ms', model, pattern, True, True,
                       succeeds=False)
        scratch = split / 'native-scratch.ms'
        custom_predict(native, source, scratch, model, pattern, True, True)
        observed = field_samples(scratch)
        if with_imaginary:
            results['split-scratch'] = compare(observed, reference)
        targets = [split / f'virtual-{index}.ms' for index in range(2)]
        tool = native()
        for target in targets:
            copy_input(source, target, False)
            assert tool.selectvis(str(target), field='0,1', usescratch=False)
        define_mosaic(tool)
        assert tool.setvp(dovp=False, usedefaultvp=False, vptable=str(pattern))
        assert tool.ft(str(model)) and tool.done()
        for index, target in enumerate(targets):
            grid, = model_grids(model_recipe(target))
            saved = Path(grid['pbconvfunc']['vptable'])
            assert saved.parent == Path(grid['diskimage']) and saved != pattern
            tables = table()
            assert tables.open(str(saved))
            for row in range(tables.nrows()):
                record = tables.getcell('pbdescription', row)
                assert Path(record['imagim']).is_dir()
                if not with_imaginary:
                    assert Path(record['imagim']).parent == saved.parent
            assert tables.done()
            results[f'{with_imaginary}-reopened-{index}'] = compare(
                consume(target, split / f'reopened-{index}.ms', 'casa'), observed)
        assert tables.open(str(pattern))
        assert all('imagim' not in tables.getcell('pbdescription', row) for row in range(tables.nrows()))
        assert tables.done()
    return results


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    source, _, model = fixture(root)
    pattern = voltage_table(root, model)
    results = {}
    predict(source, root / 'default-scratch.ms', imager, model, True, False)
    defaults = field_samples(root / 'default-scratch.ms')
    for enabled in (True, False):
        paths = {}
        for name, factory in [('casa', imager), ('native', native)]:
            for scratch in (True, False):
                label = name, scratch
                paths[label] = root / f'{name}-{enabled}-{scratch}.ms'
                custom_predict(factory, source, paths[label], model, pattern, scratch, enabled)
        reference = field_samples(paths['casa', True])
        if enabled:
            assert all(relative_rms(a, b) > .1 for a, b in zip(defaults, reference, strict=True))
            enabled_reference = reference
        else:
            assert all(np.array_equal(a, b) for a, b in zip(enabled_reference, reference, strict=True))
        results[f'{enabled}-scratch'] = compare(field_samples(paths['native', True]), reference)
        reopened = consume(paths['casa', False], root / f'{enabled}-casa-casa.ms', 'casa')
        for writer, reader in [('casa', 'native'), ('native', 'casa'), ('native', 'native')]:
            label = f'{enabled}-{writer}-{reader}'
            actual = consume(paths[writer, False], root / f'{label}.ms', reader)
            results[label] = compare(actual, reopened)
    results['split'] = split_predictions(root, source, model, enabled_reference, native)
    results['lifecycle'] = lifecycle(root, source, model, pattern, native)
    results['malformed'] = malformed_tables(root, source, model, pattern, paths['native', False], native)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    root = parser.parse_args().work.resolve()
    results = run(root)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)


if __name__ == '__main__':
    main()
