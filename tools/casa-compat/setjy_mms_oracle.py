#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise setjy's scratch and virtual prediction on real CASA Multi-MS tables."""

import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

from casatools import table
import casatasks as casa
from casars import tasks as rust
import numpy as np

from applycal_mms_oracle import referenced_tables
from bandpass_oracle import source_data
from setjy_image_oracle import write_image
from setjy_manual_oracle import casa_ms, compare, compare_returns, consume, data, make_input, rust_ms


def mms(source, path, *, missing_source=False):
    casa.partition(str(source), str(path), separationaxis='scan', numsubms=2,
                   disableparallel=True, flagbackup=False, datacolumn='data')
    members = referenced_tables(path)
    assert len(members) == 2
    if missing_source:
        writer = table()
        assert writer.open(str(path / 'FIELD'), nomodify=False)
        assert writer.putcol('SOURCE_ID', np.full(writer.nrows(), -1, dtype=np.int32))
        assert writer.done()
    return members


def history(member):
    history = member / 'HISTORY'
    messages = data(history, 'MESSAGE')[data(history, 'ORIGIN') == 'setjy']
    assert any(line.split('=', 1)[0].strip() == 'vis' and str(member) in line
               for line in messages), messages
    assert any(line.split('=', 1)[0].strip() == 'ismms' and line.endswith('True')
               for line in messages), messages


def verify(source, root, name, *, usescratch, missing_source=False, **options):
    results = []
    with TemporaryDirectory(prefix=f'{name}-', dir=root) as directory:
        work = Path(directory)
        for label, tasks in (('casa', casa), ('rust', rust)):
            path = work / f'{label}.mms'
            members = mms(source, path, missing_source=missing_source)
            got = tasks.setjy(str(path), usescratch=usescratch, **options)
            cells = [data(member, 'MODEL_DATA') for member in members]
            if usescratch:
                for member, cell in zip(members, cells):
                    if np.max(np.abs(cell)) > 2.:
                        history(member)
            reopened = []
            if not usescratch:
                for index, member in enumerate(members):
                    for reader, factory in (('casa', casa_ms), ('rust', rust_ms)):
                        copy = work / f'{label}-{index}-{reader}.ms'
                        make_input(member, copy, scratch=False)
                        reopened.append(consume(factory, copy))
                assert max(np.max(np.abs(model)) for model in reopened) > 2., label
            results.append((got, cells, reopened))
        expected, actual = results
        compare_returns(actual[0], expected[0])
        for index, (want, got) in enumerate(zip(expected[1], actual[1])):
            compare(f'{name} partition {index} MODEL_DATA', got, want)
        for index, (want, got) in enumerate(zip(expected[2], actual[2])):
            compare(f'{name} virtual reader {index}', got, want)
        return actual[0]


def empty(source, root):
    with TemporaryDirectory(dir=root) as directory:
        work = Path(directory)
        for scratch in (True, False):
            for label, tasks in (('casa', casa), ('rust', rust)):
                path = work / f'{label}-{scratch}.mms'
                members = mms(source, path)
                options = dict(field='0', selectdata=True, scan='999', standard='manual',
                               fluxdensity=2.4, usescratch=scratch)
                if scratch:
                    try:
                        tasks.setjy(str(path), **options)
                    except RuntimeError as error:
                        assert 'Error in parallel processing of MMS' in str(error), error
                    else:
                        raise AssertionError(f'{label} accepts an empty scratch Multi-MS selection')
                else:
                    assert list(tasks.setjy(str(path), **options)) == ['format']
                for member in members:
                    assert np.max(np.abs(data(member, 'MODEL_DATA'))) == 1.


def all_predictions_fail(source, root):
    with TemporaryDirectory(dir=root) as directory:
        work = Path(directory)
        for label, tasks in (('casa', casa), ('rust', rust)):
            path = work / f'{label}.mms'
            members = mms(source, path)
            try:
                tasks.setjy(str(path), usescratch=True, standard='manual', fluxdensity=2.4,
                            model=str(work / 'missing.im'))
            except RuntimeError as error:
                assert 'Error in parallel processing of MMS' in str(error), error
            else:
                raise AssertionError(f'{label} accepted an invalid model in every partition')
            for member in members:
                assert np.max(np.abs(data(member, 'MODEL_DATA'))) == 1.


def preserve_other_field(source, root, *, missing_source=False):
    results = []
    with TemporaryDirectory(dir=root) as directory:
        work = Path(directory)
        for label, tasks in (('casa', casa), ('rust', rust)):
            path = work / f'{label}.mms'
            members = mms(source, path, missing_source=missing_source)
            for field, flux in (('0', 2.4), ('1', 3.7)):
                tasks.setjy(str(path), field=field, standard='manual', fluxdensity=flux,
                            usescratch=False)
            predictions = []
            for index, (member, flux) in enumerate(zip(members, (2.4, 3.7))):
                for reader, factory in (('casa', casa_ms), ('rust', rust_ms)):
                    copy = work / f'{label}-{index}-{reader}.ms'
                    make_input(member, copy, scratch=False)
                    model = consume(factory, copy)
                    np.testing.assert_allclose(np.max(np.abs(model)), flux, rtol=1e-6)
                    predictions.append(model)
            results.append(predictions)
        for index, (want, got) in enumerate(zip(*results)):
            compare(f'preserve other virtual field {missing_source} reader {index}', got, want)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    args.root.mkdir()
    source = args.root / 'base.ms'
    source_data(source, stokes='XX XY YX YY')
    options = {'standard': 'manual', 'fluxdensity': [2.4, .2, -.15, .1]}
    for scratch in (True, False):
        returned = verify(source, args.root, f'full-{scratch}', usescratch=scratch, **options)
        assert list(returned.keys()) == (['0', 'format'] if scratch else ['0', '1', 'format'])
        verify(source, args.root, f'second-field-{scratch}', usescratch=scratch, field='1', **options)
        verify(source, args.root, f'scan-{scratch}', usescratch=scratch, field='0',
               selectdata=True, scan='1', **options)
    empty(source, args.root)
    all_predictions_fail(source, args.root)
    preserve_other_field(source, args.root)
    preserve_other_field(source, args.root, missing_source=True)
    verify(source, args.root, 'physical-main-models', usescratch=False, field='0',
           missing_source=True, **options)
    image = args.root / 'calibrator.im'
    write_image(image, source, cube=False)
    for scratch in (True, False):
        verify(source, args.root, f'image-{scratch}', usescratch=scratch, field='0',
               model=str(image), standard='manual', fluxdensity=0.)


if __name__ == '__main__':
    main()
