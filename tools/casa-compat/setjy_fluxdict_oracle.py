#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare setjy on matched fluxscale dictionaries and their saved MS models."""

import argparse
from copy import deepcopy
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import casatasks as casa
from casars import tasks as rust
import numpy as np

from record_casa_returns import make_cal_ms
from setjy_manual_oracle import (
    add_second_window, casa_ms, compare, compare_returns, consume, data, make_input, rust_ms,
)


def compare_case(seed, root, name, fluxdict, **options):
    with TemporaryDirectory(prefix=name + '-', dir=root) as directory:
        case = Path(directory)
        for scratch in (True, False):
            paths, returned = [], []
            for label, tasks in (('casa', casa), ('rust', rust)):
                path = case / f'{label}-{scratch}.ms'
                make_input(seed, path, scratch=scratch)
                result = tasks.setjy(vis=str(path), standard='fluxscale', fluxdict=fluxdict,
                                     usescratch=scratch, **options)
                paths.append(path)
                returned.append(result)
            compare_returns(returned[1], returned[0])
            if scratch:
                scratch_model = data(paths[0], 'MODEL_DATA')
                compare(name + ' scratch', data(paths[1], 'MODEL_DATA'), scratch_model)
                continue
            reference = None
            for writer, path in zip(('casa', 'rust'), paths):
                for reader, tool in (('casa', casa_ms), ('rust', rust_ms)):
                    read = case / f'{writer}-{reader}-read.ms'
                    shutil.copytree(path, read)
                    measured = consume(tool, read)
                    shutil.rmtree(read)
                    if reference is None:
                        reference = measured
                    compare(f'{name} {writer} writer {reader} reader', measured, reference)


def real_fluxscale(seed, root):
    for name, tasks in (('casa', casa), ('rust', rust)):
        gain = root / f'{name}-gain.cal'
        scale = root / f'{name}-scale.cal'
        tasks.gaincal(vis=str(seed), caltable=str(gain), refant='A0', solint='int', minsnr=0.)
        result = tasks.fluxscale(vis=str(seed), caltable=str(gain), fluxtable=str(scale),
                                 reference=['FLUX'], transfer=['PHASE'])
        assert result['1']['fitFluxd'] > 0 and result['1']['spidx'].shape == (2,)
        compare_case(seed, root, name + '-fluxscale-output', result, field='1')


def payload():
    window = lambda flux: {'fluxd': np.array(flux, dtype=np.float64)}
    return {
        '0': {'0': window([3., .1, .2, 0.]), '1': window([8., .4, .5, .6]),
              'fitFluxd': np.float64(2.4), 'fitRefFreq': 1.4e9, 'spidx': np.array([.4, -.5, .02])},
        '1': {'0': window([5., 0., 0., 0.]), '1': window([4., 0., 0., 0.]),
              'fitFluxd': 3.4, 'fitRefFreq': 1.4e9, 'spidx': np.array([.4, .5, .02])},
        'spwID': np.array([0, 1]), 'freq': np.array([1.4e9, 1.5e9]),
    }


def synthetic_fits(seed, root):
    source = root / 'two-windows.ms'
    make_input(seed, source, scratch=True)
    add_second_window(source)
    reference = payload()
    compare_case(source, root, 'two-field-fit', reference, fluxdensity=0., spix=10.5, reffreq='bad')

    window = reference['0']['0']
    window['fluxd'][0] = -1.
    compare_case(source, root, 'invalid-first-window', reference, field='0')
    reference['0']['fitFluxd'] = 0.
    compare_case(source, root, 'first-valid-window-flux', reference, field='0',
                 polindex=[.1, .02], polangle=[.3, -.04], rotmeas=9.)

    reference['0']['1']['fluxd'][0] = -1.
    reference['0']['fitFluxd'] = 2.5
    compare_case(source, root, 'fit-with-all-windows-flagged', reference, field='0', scalebychan=False)

    reference['0']['fitFluxd'] = -1.
    compare_case(source, root, 'negative-fit-catalog-fallback', reference, field='0')


def refused_domains(seed, root):
    base = payload()
    missing_window_list = {'freq': [1.4e9]}
    high_index = deepcopy(base)
    high_index['0']['spidx'] = np.array([0., 10.5])
    options = (
        ({}, '', 'fluxdict is empty'),
        (missing_window_list, '', 'Input fluxdict is missing keywords:spwID'),
        (high_index, '', 'Field 0: the spectral index is greater than 10!'),
        (base, '0', "'str' object has no attribute '_swigobj'"),
    )
    for index, (dictionary, spw, expected) in enumerate(options):
        for name, tasks in (('casa', casa), ('rust', rust)):
            target = root / f'{name}-invalid-{index}.ms'
            make_input(seed, target, scratch=False)
            try:
                tasks.setjy(str(target), field='0', spw=spw, standard='fluxscale',
                            fluxdict=dictionary, usescratch=False)
            except Exception as error:
                assert expected in str(error), (name, str(error))
            else:
                raise AssertionError(f'{name} accepted malformed fluxscale inputs')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    seed = args.root / 'transfer.ms'
    make_cal_ms(str(seed), frequency='1.4GHz')
    real_fluxscale(seed, args.root)
    synthetic_fits(seed, args.root)
    refused_domains(seed, args.root)


if __name__ == '__main__':
    main()
