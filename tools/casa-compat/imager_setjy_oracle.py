#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct imager.setjy returns, models and retained selections."""

import argparse
import inspect
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

from casatools import componentlist, imager as casa_imager
from casars.tools import imager as rust_imager
import numpy as np

from bandpass_oracle import source_data
from setjy_image_oracle import write_image
from setjy_manual_oracle import (casa_ms, compare, compare_returns, consume, data, make_input,
                                 rust_ms)
from setjy_solar2010_oracle import ephemeris_observation, read_scratch_in_both


ENGINES = [('casa', casa_imager), ('rust', rust_imager)]
READERS = [('casa', casa_ms), ('rust', rust_ms)]
SCALE = 'Butler-JPL-Horizons 2010'


def direct_case(seed, root, label, *, body='Mars', epoch=55000., frequency=100e9, options=None):
    options = options or {}
    with TemporaryDirectory(dir=root, prefix=f'{label}-') as tmp:
        work = Path(tmp)
        expected = None
        for scratch in (True, False):
            replies, results = [], []
            paths = []
            for writer, create in ENGINES:
                target = work / f'{writer}-{scratch}.ms'
                paths.append(target)
                make_input(seed, target, scratch=scratch)
                ephemeris_observation(target, body, epoch, frequency)
                tool = create()
                assert tool.open(str(target), usescratch=scratch)
                prior = tool.setjy(field='1', standard='Perley-Butler 2017',
                                   fluxdensity=1., reffreq='100GHz')
                assert prior['1']['0']['fluxd'][0] == 1.
                replies.append(tool.setjy(**options))
                assert tool.close()
                if scratch:
                    model = data(target, 'MODEL_DATA')
                    read_scratch_in_both(target, model)
                    results.append(model)
            compare_returns(replies[1], replies[0], rtol=2.e-7)
            if scratch:
                expected = results[0]
                compare(f'{label} scratch', results[1], expected)
                continue
            for (writer, _), target in zip(ENGINES, paths):
                for reader, create in READERS:
                    clone = work / f'{writer}-{reader}-virtual.ms'
                    copytree(target, clone)
                    compare(f'{label} {writer} virtual writer {reader} reader',
                            consume(create, clone), expected)


def positional_args(seed, root):
    args = ([0], [0], '', [4., .6, .2, .1], 'Perley-Butler 2017', True, [0.4, -.03],
            '90GHz', [.12, .01], [.2, .02], 7.5, '', '1', '', '0', 'nearest')
    models, replies = [], []
    with TemporaryDirectory(dir=root, prefix='positional-') as tmp:
        for writer, create in ENGINES:
            target = Path(tmp) / f'{writer}.ms'
            make_input(seed, target, scratch=True)
            ephemeris_observation(target, 'Mars', 55000., 100e9)
            tool = create()
            assert tool.open(str(target), usescratch=True)
            replies.append(tool.setjy(*args))
            assert tool.done()
            models.append(data(target, 'MODEL_DATA'))
    compare_returns(replies[1], replies[0], rtol=2.e-7)
    compare('all positional arguments', models[1], models[0])


def omitted_reference_frequency(seed, root):
    with TemporaryDirectory(dir=root, prefix='default-frequency-') as tmp:
        outputs = []
        for writer, create in ENGINES:
            target = Path(tmp) / f'{writer}.ms'
            make_input(seed, target, scratch=True)
            tool = create()
            assert tool.open(str(target), usescratch=True)
            options = {'reffreq': '1GHz'} if writer == 'casa' else {}
            reply = tool.setjy(field='0', fluxdensity=2.1, standard='Perley-Butler 2017',
                               **options)
            assert tool.done()
            outputs.append((reply, data(target, 'MODEL_DATA')))
        compare_returns(outputs[1][0], outputs[0][0])
        compare('omitted manual reference frequency', outputs[1][1], outputs[0][1])


def lifecycle(seed, root):
    with TemporaryDirectory(dir=root, prefix='lifecycle-') as tmp:
        work = Path(tmp)
        model = work / 'follow-on.cl'
        components = componentlist()
        assert components.addcomponent(flux=3.7, dir='J2000 0rad 0rad', freq='100GHz')
        assert components.rename(str(model)) and components.done()
        for invoke in (False, True):
            after = []
            for writer, create in ENGINES:
                target = work / f'{writer}-{invoke}.ms'
                make_input(seed, target, scratch=True)
                tool = create()
                assert tool.ssoflux() is False
                assert tool.setjy() == {}
                assert tool.open(str(target), usescratch=True)
                assert tool.selectvis(field='1')
                assert tool.ssoflux() is False
                if invoke:
                    result = tool.setjy(field=0, spw='0:0~2', fluxdensity=2.1,
                                        reffreq='100GHz', standard='Perley-Butler 2017')
                    assert result['0']['0']['fluxd'][0] == 2.1
                else:
                    assert tool.setjy() == {}
                previous = data(target, 'MODEL_DATA')
                assert tool.ft(complist=str(model))
                final = data(target, 'MODEL_DATA')
                assert tool.done() and tool.setjy() == {} and tool.ssoflux() is False
                selected = data(target, 'FIELD_ID') == (0 if invoke else 1)
                assert not np.array_equal(final[..., selected], previous[..., selected])
                np.testing.assert_array_equal(final[..., ~selected], previous[..., ~selected])
                after.append(final)
            compare(f'follow-on ft after setjy update={invoke}', after[1], after[0])


def multi_input(seed, root):
    with TemporaryDirectory(dir=root, prefix='inputs-') as tmp:
        work = Path(tmp)
        options = dict(field=0, spw=0, fluxdensity=4., reffreq='100GHz',
                       standard='Perley-Butler 2017')
        for writer, create in ENGINES:
            first = work / f'{writer}-first.ms'
            last = work / f'{writer}-last.ms'
            for path in (first, last):
                make_input(seed, path, scratch=True)
            baseline = data(first, 'MODEL_DATA')
            tool = create()
            assert tool.selectvis(vis=str(first), field='1', usescratch=True)
            assert tool.selectvis(vis=str(last), field='1', usescratch=True)
            assert tool.setjy(**options)['0']['0']['fluxd'][0] == 4.
            assert tool.done()
            np.testing.assert_array_equal(data(first, 'MODEL_DATA'), baseline)
            cells = data(last, 'MODEL_DATA')
            selected = data(last, 'FIELD_ID') == 0
            assert np.allclose(cells[0, :, selected], 4.)
            np.testing.assert_array_equal(cells[..., ~selected], baseline[..., ~selected])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    for name in ('setjy', 'ssoflux'):
        reference = inspect.signature(getattr(casa_imager, name))
        native = inspect.signature(getattr(rust_imager, name))
        assert list(native.parameters) == list(reference.parameters)
        for parameter in reference.parameters:
            assert native.parameters[parameter].default == reference.parameters[parameter].default
    seed = args.root / 'base.ms'
    source_data(seed, stokes='XX XY YX YY')
    direct_case(seed, args.root, 'manual', options=dict(field=[0], spw=0, fluxdensity=[5., .6, .2, .1],
                                                       reffreq='100GHz', standard='Perley-Butler 2017'))
    for scale in (SCALE, 'Butler-JPL-Horizons 2012'):
        direct_case(seed, args.root, scale, options=dict(field='0', spw=[0], fluxdensity=-1.,
                                                       scalebychan=True, standard=scale))
    direct_case(seed, args.root, 'catalogue', body='3C286', epoch=58000., frequency=1.4e9,
                options=dict(field='0', spw='0', fluxdensity=-1., standard='Perley-Butler 2017',
                             interpolation='cubic'))
    image = args.root / 'resolved.im'
    write_image(image, seed, cube=False)
    direct_case(seed, args.root, 'resolved', options=dict(field='0', modimage=str(image),
                fluxdensity=5., reffreq='100GHz', standard='Perley-Butler 2017'))
    positional_args(seed, args.root)
    omitted_reference_frequency(seed, args.root)
    lifecycle(seed, args.root)
    multi_input(seed, args.root)


if __name__ == '__main__':
    main()
