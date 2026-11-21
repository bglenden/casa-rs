#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct imager.setjy returns, models and retained selections."""

import argparse
import inspect
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

from casatools import componentlist, imager as casa_imager, measures, table
from casars.tools import imager as rust_imager
import numpy as np

from bandpass_oracle import source_data
from record_casa_returns import make_cal_ms
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


def tool_spellings(seed, root):
    frequency = measures().frequency('TOPO', '90GHz')
    cases = (
        ('catalogue alias', '3C48', 58123.25, 'Perley & Butler 17',
         {'fluxdensity': -1., 'interpolation': 'cubic-spline'}),
        ('BAARS alias', '3C286', 58000., 'BAARS 77', {'fluxdensity': -1.}),
        ('Solar alias', 'Mars', 55000., 'JPL', {'fluxdensity': -1.}),
        ('TOPO measure', 'Mars', 55000., 'Perley-Butler 2017',
         {'fluxdensity': 3., 'spix': 1., 'reffreq': frequency}),
    )
    with TemporaryDirectory(dir=root, prefix='spellings-') as tmp:
        for index, (label, body, epoch, standard, options) in enumerate(cases):
            products = []
            for writer, create in ENGINES:
                path = Path(tmp) / f'{index}-{writer}.ms'
                make_input(seed, path, scratch=True)
                ephemeris_observation(path, body, epoch, 1.4e9 if index <= 1 else 100e9)
                tool = create()
                assert tool.open(str(path), usescratch=True)
                result = tool.setjy(field='0', standard=standard, **options)
                assert tool.done()
                products.append((result, data(path, 'MODEL_DATA')))
            # The independent topocentric conversion differs at sub-ppm
            # precision in the bundled Earth-orientation runtime.
            tolerance = 1.e-6 if label == 'TOPO measure' else 2.e-7
            compare_returns(products[1][0], products[0][0], rtol=tolerance)
            compare(label, products[1][1], products[0][1])


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
                original = data(target, 'MODEL_DATA')
                assert tool.setjy(field='0', fluxdensity=2.1, reffreq='100GHz',
                                  standard='Perley-Butler 2017', interpolation='BAD') == {}
                np.testing.assert_array_equal(data(target, 'MODEL_DATA'), original)
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


def selection_failures(seed, root, follow_on):
    with TemporaryDirectory(dir=root, prefix='selection-errors-') as tmp:
        work = Path(tmp)
        for name, selection, next_field in (
            ('unknown field', {'field': 'ZZZZ'}, 1),
            ('missing image', {'field': '0', 'modimage': str(work / 'missing.im')}, 0),
            ('empty scan', {'field': '0', 'scan': '999'}, 1),
        ):
            for writer, create in ENGINES:
                target = work / f'{name}-{writer}.ms'
                make_input(seed, target, scratch=True)
                tool = create()
                assert tool.open(str(target), usescratch=True) and tool.selectvis(field='1')
                prior = data(target, 'MODEL_DATA')
                reply = tool.setjy(fluxdensity=2.1, reffreq='1GHz',
                                  standard='Perley-Butler 2017', **selection)
                if name == 'empty scan':
                    assert list(reply) == ['format']
                else:
                    assert reply == {}
                np.testing.assert_array_equal(data(target, 'MODEL_DATA'), prior)
                assert tool.ft(complist=str(follow_on)) and tool.done()
                after = data(target, 'MODEL_DATA')
                field = data(target, 'FIELD_ID')
                changed = [index for index in (0, 1)
                           if not np.array_equal(after[..., field == index], prior[..., field == index])]
                expected = [0, 1] if writer == 'casa' and name == 'empty scan' else [next_field]
                assert changed == expected, (writer, name, changed)


def partial_catalogue(root, follow_on):
    with TemporaryDirectory(dir=root, prefix='partial-catalogue-') as tmp:
        work = Path(tmp)
        seed = work / 'partial.ms'
        make_cal_ms(str(seed), frequency='300MHz')
        tb = table()
        assert tb.open(str(seed / 'FIELD'), nomodify=False)
        assert tb.putcol('NAME', ['3C138', '3C286']) and tb.done()
        outputs = []
        for writer, create in ENGINES:
            target = work / f'{writer}.ms'
            make_input(seed, target, scratch=True)
            tool = create()
            assert tool.open(str(target), usescratch=True)
            assert tool.setjy(field='0,1', spw='0', fluxdensity=2., reffreq='300MHz',
                              standard='Perley-Butler 2017')
            before = data(target, 'MODEL_DATA')
            try:
                tool.setjy(field='0,1', spw='0', standard='Scaife-Heald 2012', fluxdensity=-1.)
            except RuntimeError:
                pass
            else:
                raise AssertionError('the unavailable second field should report a partial write')
            field = data(target, 'FIELD_ID')
            after = data(target, 'MODEL_DATA')
            np.testing.assert_array_equal(after[..., field == 0], before[..., field == 0])
            assert not np.array_equal(after[..., field == 1], before[..., field == 1])
            assert tool.ft(complist=str(follow_on)) and tool.done()
            following = data(target, 'MODEL_DATA')
            np.testing.assert_array_equal(following[..., field == 1], after[..., field == 1])
            assert not np.array_equal(following[..., field == 0], after[..., field == 0])
            outputs.append((after, following))
        compare('partial catalogue publication', outputs[1][0], outputs[0][0])
        compare('follow-on ft after partial catalogue publication', outputs[1][1], outputs[0][1])


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
    tool_spellings(seed, args.root)
    lifecycle(seed, args.root)
    multi_input(seed, args.root)
    follow_on = args.root / 'selection-errors.cl'
    components = componentlist()
    assert components.addcomponent(flux=3.7, dir='J2000 0rad 0rad', freq='100GHz')
    assert components.rename(str(follow_on)) and components.done()
    selection_failures(seed, args.root, follow_on)
    partial_catalogue(args.root, follow_on)


if __name__ == '__main__':
    main()
