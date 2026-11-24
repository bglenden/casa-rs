#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check imager-origin SETJY HISTORY on a 3C147 MeasurementSet and continuum model."""

import argparse
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory

from casatasks import setjy as casa_setjy
from casatools import imager as casa_imager, table as casa_table
import numpy as np

from casars.tasks import setjy as native_setjy
from casars.tools import imager as native_imager, table as native_table
from setjy_image_oracle import write_image


COLUMNS = ('ORIGIN', 'APPLICATION', 'MESSAGE', 'OBJECT_ID', 'OBSERVATION_ID', 'PRIORITY')
SPECTRUM = 'Flux density as a function of frequency (channel 0 of each spw):'
MANUAL = dict(standard='Perley-Butler 2010', fluxdensity=1234., spix=-.7, reffreq='35GHz')


def history(factory, ms):
    reader = factory()
    assert reader.open(str(ms / 'HISTORY'))
    try:
        return list(zip(*(reader.getcol(name) for name in COLUMNS), strict=True))
    finally:
        assert reader.close()


def attached(factory, target, invalid=False, **parameters):
    tool = factory()
    assert tool.open(str(target), usescratch=True)
    try:
        result = tool.setjy(field='0', spw='0', **parameters)
    except (RuntimeError, ValueError):
        if invalid and factory == native_imager:
            return
        raise
    finally:
        assert tool.close()
    if invalid:
        assert result == {}, result
    else:
        assert result['0']['0']['fluxd'][0] > 0., result


def imager_rows(target, call):
    before = history(casa_table, target)
    call()
    after = history(casa_table, target)
    assert after[:len(before)] == before
    assert history(native_table, target) == after
    return [row for row in after[len(before):] if row[0] == 'imager::setjy()']


def spectrum(rows):
    for index, row in enumerate(rows):
        if row[2].startswith(SPECTRUM):
            return rows[index:]
    return []


def paired(seed, work, label, calls, relevant):
    expected, actual = [work / f'{label}-{engine}.ms' for engine in ('casa', 'native')]
    rows = []
    for target, call in zip((expected, actual), calls, strict=True):
        shutil.copytree(seed, target)
        rows.append(relevant(imager_rows(target, lambda: call(target))))
    assert rows[0] and rows[1] == rows[0], (label, rows)
    print(label, [row[2] for row in rows[1]], flush=True)


def scalar_manual(seed, work, model):
    target = work / 'manual-scalar.ms'
    shutil.copytree(seed, target)
    writer = casa_table()
    assert writer.open(str(target / 'SPECTRAL_WINDOW'), nomodify=False)
    frequencies = writer.getcell('CHAN_FREQ', 0)
    widths = writer.getcell('CHAN_WIDTH', 0)
    assert writer.putcell('MEAS_FREQ_REF', 0, 1) and writer.close()  # LSRK
    rows = imager_rows(target, lambda: attached(
        native_imager, target, modimage=str(model), scalebychan=False, **MANUAL))
    messages = [row[2] for row in rows if row[2].startswith('Scaling spw(s) ')]
    message, = messages
    match = re.search(r'to I = (\S+) Jy @ (\S+)Hz \(LSRK\)', message)
    assert match, message
    intensity, hz = map(float, match.groups())
    # The manual scalar correction normalizes at the band center. Require
    # agreement within half a channel and check the reported Jy independently.
    assert abs(hz - np.median(frequencies)) < .5 * abs(widths).max(), message
    np.testing.assert_allclose(intensity, 1234. * (hz / 35e9)**-.7, rtol=5e-6)
    print('manual scalar', message, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('ms', type=lambda path: Path(path).resolve())
    parser.add_argument('continuum', type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    with TemporaryDirectory(prefix='setjy-history-') as scratch:
        work = Path(scratch)
        cube = work / 'cube.im'
        write_image(cube, args.ms)
        for label, options, relevant in (
            ('catalog', dict(standard='Perley-Taylor 99', fluxdensity=-1., scalebychan=False),
             lambda rows: [row for row in rows if '(Perley-Taylor 99)' in row[2]]),
            ('scalar image', dict(standard='Perley-Butler 2010', fluxdensity=-1.,
                                  scalebychan=False, modimage=str(args.continuum)),
             lambda rows: [row for row in rows if row[2].startswith(
                 ('Using model image ', 'Scaling spw(s) '))]),
            ('manual cube', dict(MANUAL, modimage=str(cube)), spectrum),
        ):
            paired(args.ms, work, label, [
                lambda target, factory=factory: attached(factory, target, **options)
                for factory in (casa_imager, native_imager)
            ], relevant)
        bad_model = args.ms / 'FIELD'
        paired(args.ms, work, 'failed image', [
            lambda target, factory=factory: attached(
                factory, target, invalid=True, standard='Perley-Taylor 99',
                fluxdensity=-1., scalebychan=False, modimage=str(bad_model))
            for factory in (casa_imager, native_imager)
        ], lambda rows: [row for row in rows if '(Perley-Taylor 99)' in row[2]
                         or row[2].startswith(('Using model image ', 'Scaling spw(s) '))])
        dictionary = {
            '0': {'0': {'fluxd': np.array([1234., 0., 0., 0.])}, 'fitFluxd': 1234.,
                  'fitRefFreq': 35e9, 'spidx': np.array([np.log10(1234.), -.7])},
            'spwID': np.array([0]), 'freq': np.array([32.514e9]),
        }
        paired(args.ms, work, 'fluxscale dictionary', [
            lambda target, task=task: task(str(target), standard='fluxscale', field='0',
                                           fluxdict=dictionary, usescratch=True)
            for task in (casa_setjy, native_setjy)
        ], spectrum)
        scalar_manual(args.ms, work, args.continuum)


if __name__ == '__main__':
    main()
