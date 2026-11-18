#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the ft model records, visibility storage and consumers with CASA."""

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import componentlist, image, table
import numpy as np

from calibration_scratch_oracle import MAIN, prepare, state
from bandpass_oracle import source_data


def coordinates(ms):
    tb = table()
    assert tb.open(str(ms/'FIELD'))
    try:
        return tb.getcell('PHASE_DIR', 0)[:, 0]
    finally:
        tb.close()


def component(path, direction, flux, *, resolved=False):
    cl = componentlist()
    ra, dec = direction
    shape = (dict(shape='Gaussian', majoraxis='23arcsec', minoraxis='9arcsec',
                  positionangle='28deg', freq='100GHz', spectrumtype='spectral index', index=-0.5)
             if resolved else {})
    assert cl.addcomponent(dir=f'J2000 {ra}rad {dec}rad', flux=flux, fluxunit='Jy', **shape)
    assert cl.rename(str(path))
    cl.done()


def compare_outputs(actual, expected):
    a, native_model = actual
    b, casa_model = expected
    assert a.keys() == b.keys()
    for col in a:
        if col in MAIN:
            np.testing.assert_array_equal(a[col], b[col], err_msg=col)
        else:
            scale = max(1., float(np.max(np.abs(b[col]))))
            np.testing.assert_allclose(a[col], b[col], atol=3e-5*scale, rtol=0, err_msg=col)
    np.testing.assert_equal(native_model, casa_model)


def make_images(root, source):
    phase = coordinates(source)
    tb = table()
    assert tb.open(str(source/'SPECTRAL_WINDOW'))
    try:
        frequencies = tb.getcell('CHAN_FREQ', 0)
    finally:
        tb.close()
    outputs = {}
    for name, count, flux in [('point', 1, 2.), ('term1', 1, .5),
                               ('cube', len(frequencies), 1.), ('empty', 1, 0.)]:
        path = root/f'{name}.im'
        tool = image()
        data = np.zeros((32, 32, 1, count), np.float32)
        data[16, 16, 0, :] = flux
        if count > 1:
            data[16, 16, 0, :] += np.arange(count)*.2
            data[17, 18, 0, :] = .1
        assert tool.fromarray(outfile=str(path), pixels=data)
        cs = tool.coordsys()
        cs.setunits(['rad', 'rad', '', 'Hz'])
        cs.setreferencevalue(value=f'{phase[0]}rad {phase[1]}rad', type='direction')
        cs.setincrement(value='-1arcsec 1arcsec', type='direction')
        cs.setreferencepixel(value=[16., 16.], type='direction')
        cs.setreferencecode('TOPO', type='spectral', adjust=False)
        cs.setreferencepixel(value=[0.], type='spectral')
        cs.setreferencevalue(value=f'{frequencies[0]}Hz', type='spectral')
        cs.setincrement(value='1000000Hz' if count > 1 else '30000000Hz', type='spectral')
        assert tool.setcoordsys(cs.torecord())
        assert tool.setbrightnessunit('Jy/pixel')
        cs.done()
        tool.done()
        outputs[name] = str(path)
    return outputs


def record_layout(path):
    pointers, source, records = state(path)[1]
    layout = []
    for record in records or []:
        if not record:
            continue
        entries = {}
        for key, entry in record.items():
            if key.startswith(('ft_', 'cl_')):
                container = entry['container']
                entries[key] = (entry['type'], entry['indexcombination'],
                                entry['channelselection'], container.get('name'),
                                container.get('thisterm'), container.get('reffreq'))
            else:
                entries[key] = entry
        layout.append(entries)
    return pointers, source, layout


def image_workflows(root, source, native, point, case=None):
    images = make_images(root, source)
    cases = [
        ('point-scratch', 'point', True, {}),
        ('point-virtual', 'point', False, {}),
        ('cube-scratch', 'cube', True, {}),
        ('cube-virtual', 'cube', False, {}),
        ('cube-selected', 'cube', True, {'spw': '0:2~4'}),
        ('cube-virtual-selected', 'cube', False, {'spw': '0:2~4'}),
        ('taylor-scratch', ('point', 'term1'), True, {'nterms': 2, 'reffreq': '99GHz'}),
        ('taylor-virtual', ('point', 'term1'), False, {'nterms': 2, 'reffreq': '99GHz'}),
        ('taylor-default-frequency', ('point', 'term1'), False, {'nterms': 2}),
        ('empty-scratch', 'empty', True, {}),
        ('empty-virtual', 'empty', False, {}),
        ('add-to-scratch', 'point', True, {'incremental': True}),
        ('with-component', 'point', True, {'complist': str(point)}),
        ('with-virtual-component', 'point', False, {'complist': str(point)}),
    ]
    outcomes = {}
    for label, model, scratch, kwargs in cases:
        if case and case not in label:
            continue
        model = images[model] if isinstance(model, str) else [images[name] for name in model]
        results = []
        for writer, reader in ((casa, casa), (native, casa), (casa, native), (native, native)):
            ms = root/f'image-{label}-{writer.__name__.split(".")[-1]}-{reader.__name__.split(".")[-1]}.ms'
            prepare(source, ms, model=scratch)
            assert writer.ft(str(ms), field='0', model=model, usescratch=scratch, **kwargs) is None
            record = record_layout(ms)
            if not scratch:
                assert reader.uvsub(str(ms)) is None
            actual, _ = state(ms)
            column = 'MODEL_DATA' if scratch else 'CORRECTED_DATA'
            if results:
                expectation, reference, ref_record = results[0]
                np.testing.assert_equal(record, ref_record)
                for name in MAIN:
                    if name in expectation:
                        np.testing.assert_array_equal(actual[name], expectation[name])
                scale = max(1., float(np.max(np.abs(reference))))
                np.testing.assert_allclose(actual[column], reference, atol=3e-5*scale, rtol=0,
                                           err_msg=label)
            results.append((actual, actual[column], record))
        outcomes[label] = {'paired_writes_and_cross_reads': True}
        print(label, flush=True)
    return outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--images', action='store_true')
    parser.add_argument('--case', help='Run cases containing this text')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks
    root = args.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source, stokes='XX XY YX YY')
    ra, dec = coordinates(source)
    point = root/'point.cl'
    second = root/'point-second.cl'
    gaussian = root/'gaussian.cl'
    component(point, (ra, dec), 2.)
    component(second, (ra, dec), 3.)
    component(gaussian, (ra+1e-5, dec-2e-5), [3., 0.2, -0.3, 0.1], resolved=True)
    cases = [
        ('virtual-default', point, {}, {}, False),
        ('virtual-field-channel', point, dict(field='0', spw='0:2~4'), {}, False),
        ('virtual-repeat', second, dict(field='0', spw='0:1~8^2'), {}, True),
        ('virtual-incremental', second, dict(field='0', incremental=True), {}, True),
        ('virtual-offaxis', gaussian, dict(field='0'), {}, False),
        ('scratch-default', point, dict(field='0', usescratch=True), {}, False),
        ('scratch-existing', point, dict(field='0', usescratch=True), {'model':True}, True),
        ('scratch-incremental', point, dict(field='0', usescratch=True, incremental=True), {'model':True}, False),
        ('scratch-selected', point, dict(field='0', usescratch=True, spw='0:2~4'), {'model':True}, False),
        ('scratch-strided', point, dict(field='0', usescratch=True, spw='0:1~8^2'), {'model':True}, False),
        ('scratch-multiple-ranges', point, dict(field='0', usescratch=True, spw='0:1~2;5~8^2'), {}, False),
        ('scratch-offaxis', gaussian, dict(field='0', usescratch=True), {}, False),
        ('scratch-selection-intent', point, dict(intent='CALIBRATE_FLUX*', usescratch=True), {}, False),
        ('scratch-selection-scan', point, dict(scan='1', usescratch=True), {}, False),
        ('scratch-selection-antenna', point, dict(antenna='0&1', usescratch=True), {}, False),
        ('scratch-selection-distance', point, dict(uvrange='>1m', usescratch=True), {}, False),
        ('scratch-selection-observation', point, dict(observation='0', usescratch=True), {}, False),
        ('scratch-empty-selection', point, dict(scan='999', usescratch=True), {}, True),
    ]
    evidence = {}
    for label, cl, options, fixture, initial in cases:
        if args.case and args.case not in label:
            continue
        states = []
        for engine, api in [('casa', casa), ('native', tasks)]:
            ms = root/f'{label}-{engine}.ms'
            prepare(source, ms, **fixture)
            if initial:
                casa.ft(str(ms), field='0', complist=str(point))
            assert api.ft(str(ms), complist=str(cl), **options) is None
            states.append(state(ms))
        compare_outputs(states[1], states[0])
        evidence[label] = {'paired': True}
        print(label, flush=True)
    if args.images:
        evidence.update(image_workflows(root, source, tasks, point, args.case))
    (root/'results.json').write_text(json.dumps(evidence, indent=2)+'\n')


if __name__ == '__main__':
    main()
