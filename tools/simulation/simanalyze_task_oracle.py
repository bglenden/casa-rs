#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Small independent CASA 6.7.6.14 task reference, including its corrected floor.

Run with CASA's Python and --out target/simanalyze-oracle. The task's sole
controlled change is the already documented fidelity floor precision.
"""
import argparse
import gzip
import importlib
import inspect
import json
import os
from pathlib import Path

os.environ.setdefault('MPLBACKEND', 'Agg')
from simobserve_graphics_oracle import inputs, image_record


def corrected_simanalyze():
    from casatasks import simanalyze
    module = importlib.import_module('casatasks.private.task_simanalyze')
    source = inspect.getsource(module.simanalyze)
    assert source.count("max(abs('%s'), %f)") == 1
    source = source.replace("max(abs('%s'), %f)", "max(abs('%s'), %.17g)")
    namespace = module.__dict__.copy()
    exec(compile(source, 'CASA-documented-fidelity-floor-control', 'exec'), namespace)
    defaults = {key: value.default for key, value in inspect.signature(simanalyze).parameters.items()}
    return namespace['simanalyze'], defaults


def main():
    from casatasks import simobserve, version_string
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=True)
    os.chdir(root)
    assert version_string() == '6.7.6.14'
    model, config = inputs(root)
    observe = dict(project='analyze', skymodel=str(model), antennalist=str(config),
                   mapsize='1arcsec', integration='10s', totaltime='40s',
                   thermalnoise='', graphics='none', overwrite=True)
    simobserve(**observe)
    task, parameters = corrected_simanalyze()
    parameters.update(project='analyze', imsize=[128, 128], niter=20,
                      threshold='1e-7Jy', analyze=True, graphics='none', overwrite=True)
    task(**parameters)
    stem = root / 'analyze' / 'analyze.small'
    suffixes = ['psf', 'model', 'residual', 'image', 'pb', 'image.pbcor',
                'sumwt', 'mask', 'image.flat', 'skymodel.flat.regrid',
                'skymodel.flat.regrid.conv', 'diff', 'fidelity']
    result = dict(casa_version=version_string(), fidelity_control='full-precision floor',
                  observe=observe, analyze=parameters,
                  images={suffix: image_record(str(stem)+'.'+suffix) for suffix in suffixes})
    with gzip.open(root / 'simanalyze_task_casa.json.gz', 'wt') as output:
        json.dump(result, output, separators=(',', ':'))
    print('Recorded', len(result['images']), 'actual CASA task products')


if __name__ == '__main__':
    main()
