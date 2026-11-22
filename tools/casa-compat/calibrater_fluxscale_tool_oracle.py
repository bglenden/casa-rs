#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct calibrater.fluxscale products, records and HISTORY with CASA."""

import argparse
from contextlib import chdir
import inspect
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater, quanta
import numpy as np

from blcal_oracle import copy_ms, opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from calibration_lifecycle_oracle import contents, equal, metadata, snapshot
from fluxscale_oracle import compare, compare_history, compare_listing, history, multi_window, plain, set_scans
from record_casa_returns import make_cal_ms


def check_types(expected, actual):
    assert type(expected) is type(actual), (type(expected), type(actual))
    if isinstance(expected, dict):
        assert expected.keys() == actual.keys()
        for key in expected:
            check_types(expected[key], actual[key])
    elif isinstance(expected, np.ndarray):
        assert expected.shape == actual.shape, (expected.shape, actual.shape)
        assert expected.dtype == actual.dtype, (expected.dtype, actual.dtype)


def transfer(work, engines, vis, gain, name, *, options=None, attempts=None, legacy=False,
             positional=False, listing=False, selection=None, product=True, native_options=None,
             output_parent=None, history_root=None, expected_history=None):
    paths, histories, returns, listings = {}, {}, {}, {}
    before = snapshot(gain)
    signature = inspect.signature(casa_calibrater.fluxscale).parameters
    for engine, make_tool in engines.items():
        tool = make_tool()
        path = (output_parent or work)/f'{name}-{engine}.cal'
        paths[engine] = path
        assert tool.setvi(legacy, True)
        assert result(tool.fluxscale, str(gain)) == (False, ('RuntimeError', 'Must first open a MeasurementSet.'))
        assert tool.open(str(vis), addcorr=False, addmodel=selection is not None)
        if selection:
            with opened(vis, True) as table:
                sentinel = np.full_like(table.getcol('MODEL_DATA'), 53.)
                assert table.putcol('MODEL_DATA', sentinel)
                selected = table.getcol('FIELD_ID') == 1
            assert selected.any() and not selected.all()
            assert tool.selectvis(**selection)
        prior, activity = tool.returndict(), tool.activityrec()
        history_dir = history_root or vis
        start, _ = history(history_dir)
        common = dict(tablein=str(gain), tableout=str(path), reference='FLUX', transfer='PHASE')
        common.update(options or {})
        if engine == 'native':
            common.update(native_options or {})
        if listing:
            listing_path = work/f'{name}-{engine}.txt\t'
            common['listfile'] = f'  {listing_path}\n'
        observed = []
        for extra, ok in attempts or [({}, product)]:
            effective = common | extra
            if positional:
                args = [effective.get(key, field.default) for key, field in signature.items() if key != 'self']
                response = result(tool.fluxscale, *args)
            else:
                response = result(tool.fluxscale, **effective)
            assert response[0] == ok, (name, engine, response)
            observed.append(response)
        returns[engine] = observed
        histories[engine] = history(history_dir, start)[1]
        if expected_history is not None:
            assert len(histories[engine]) == expected_history
        same_record(tool.returndict(), prior)
        same_record(tool.activityrec(), activity)
        if selection:
            assert tool.initcalset(0)
        assert tool.close()
        if selection:
            with opened(vis) as table:
                model = table.getcol('MODEL_DATA')
            np.testing.assert_array_equal(model[..., selected], 1.)
            np.testing.assert_array_equal(model[..., ~selected], 53.)
        assert path.is_dir() == product, (name, engine)
        if listing:
            listings[engine] = listing_path.read_text()
    assert snapshot(gain) == before
    for expected, actual in zip(returns['casa'], returns['native']):
        if not expected[0]:
            assert expected[1][0] == actual[1][0], (name, expected, actual)
            assert expected[1][1].replace(str(paths['casa']), '<output>') == (
                actual[1][1].replace(str(paths['native']), '<output>')), (name, expected, actual)
        else:
            check_types(expected[1], actual[1])
            compare(plain(expected[1]), plain(actual[1]), name)
    compare_history(histories['casa'], histories['native'], [paths['casa'], paths['native']], name)
    if product:
        assert contents(paths['casa']).keys() == contents(paths['native']).keys()
        equal(paths['casa'], paths['native'])
        metadata(paths['casa'], paths['native'])
        compare_readers(paths)
    if listing:
        compare_listing(listings['casa'], listings['native'])
    print(name, 'HISTORY rows', len(histories['casa']), flush=True)
    return returns


def check_retries(work, engines, vis, gain):
    listing = work/'listing-is-a-directory'
    listing.mkdir()
    for name, factory in engines.items():
        tool = factory()
        assert tool.open(str(vis), addcorr=False, addmodel=False)
        options = dict(tablein=str(gain), reference='FLUX', transfer='PHASE')
        output = work/f'{name}-retry.cal'
        start, _ = history(vis)
        if name == 'casa':
            # The native tool reports this I/O error before publishing.
            assert result(tool.fluxscale, **options, tableout=str(output), listfile=str(listing))[0]
            assert output.exists() and len(history(vis, start)[1]) == 2
        else:
            for malformed in (None, (0, 1), [2**100]):
                failure = result(tool.fluxscale, **options, tableout=str(output), refspwmap=malformed)
                assert not failure[0] and not output.exists(), failure
            assert not history(vis, start)[1]
            failure = result(tool.fluxscale, **options, tableout=str(output), listfile=str(listing))
            assert failure == (False, ('RuntimeError', 'Error in Calibrater::fluxscale.'))
            assert not output.exists() and len(history(vis, start)[1]) == 1
            assert result(tool.fluxscale, **options, tableout=str(output))[0]
        with opened(output) as table:
            rows = table.nrows()
        if name == 'native':
            before = snapshot(output)
            start, _ = history(vis)
            assert not result(tool.fluxscale, **options, tableout=str(output), append=True,
                              refspwmap=(0, 1))[0]
            assert not history(vis, start)[1]
            assert not result(tool.fluxscale, **options, tableout=str(output), append=True,
                              listfile=str(listing))[0]
            assert len(history(vis, start)[1]) == 1 and snapshot(output) == before
        assert result(tool.fluxscale, **options, tableout=str(output), append=True)[0]
        assert tool.close()
        with opened(output) as table:
            assert table.nrows() == 2 * rows
    equal(work/'casa-retry.cal', work/'native-retry.cal')
    print('unsafe-listing-and-invalid-map-retry passed', flush=True)


def check_linked_subtables(work, engines, vis, gain):
    dataset = work/'linked.ms'
    external = work/'external-subtables'
    copy_ms(vis, dataset)
    external.mkdir()
    for sub in ('FIELD', 'SPECTRAL_WINDOW', 'HISTORY'):
        copytree(dataset/sub, external/sub)
        with opened(dataset, True) as table:
            assert table.putkeyword(sub, f'Table: {external/sub}')
    with opened(external/'FIELD', True) as table:
        assert table.putcell('NAME', 0, 'RFLUX') and table.putcell('NAME', 1, 'RPHASE')
    with opened(external/'SPECTRAL_WINDOW', True) as table:
        assert table.putcell('NAME', 0, 'Linked window')
    start, _ = history(dataset)
    options = {'reference': 'RFLUX', 'transfer': 'RPHASE'}
    for incremental in (False, True):
        values = transfer(work, engines, dataset, gain, f'linked-{incremental}',
                          options=options | {'incremental': incremental}, history_root=external,
                          expected_history=2)
        for value in values.values():
            record = value[0][1]
            assert record['1']['fieldName'] == 'RPHASE'
            np.testing.assert_array_equal(record['spwName'], ['Linked window'])
    assert not history(dataset, start)[1]


def check_history_write_failure(work, make_tool, vis, gain):
    dataset = work/'broken-history.ms'
    copy_ms(vis, dataset)
    tool = make_tool()
    assert tool.open(str(dataset), addcorr=False, addmodel=False)
    with opened(dataset/'HISTORY', True) as table:
        assert table.removecols(['MESSAGE'])
        count = table.nrows()
    output = work/'blocked-by-history.cal'
    response = result(tool.fluxscale, str(gain), 'FLUX', str(output), 'PHASE')
    assert response == (False, ('RuntimeError', 'Error in Calibrater::fluxscale.'))
    assert tool.close() and not output.exists()
    with opened(dataset/'HISTORY') as table:
        assert table.nrows() == count
    print('history-write-failure passed', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    work = parser.parse_args().work.resolve()
    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    vis, gain = work/'input.ms', work/'prior.gcal'
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    with chdir(work):
        make_cal_ms(str(vis))
        casatasks.gaincal(str(vis), str(gain), refant='A0', solint='int')
        times = set_scans(gain)
        converter = quanta()
        bounds = [converter.time(converter.quantity(float(epoch), 's'), form='ymd')[0]
                  for epoch in np.percentile(times, [15., 85.])]
        converter.done()
        for name, options in [
            ('basic', {}), ('automatic-transfer', {'transfer': []}),
            ('by-id', {'reference': 0, 'transfer': 1}),
            ('numpy-reference', {'reference': np.array(['FLUX'])}),
            ('incremental-display', {'incremental': True, 'display': True}),
            ('gain-cut', {'gainthreshold': 15., 'antenna': '0,1,2,3'}),
            ('calibration-scans', {'scan': '1,3'}),
            ('calibration-times', {'timerange': '~'.join(bounds)}),
        ]:
            transfer(work, engines, vis, gain, name, options=options)
        scalar_gain = work/'prior-t.cal'
        casatasks.gaincal(str(vis), str(scalar_gain), gaintype='T', refant='A0', solint='int')
        transfer(work, engines, vis, scalar_gain, 'scalar-t')
        transfer(work, engines, vis, gain, 'retains-selection', legacy=True, selection={'field': 'PHASE'})
        transfer(work, engines, vis, gain, 'positional', positional=True, listing=True)
        transfer(work, engines, vis, gain, 'legacy', legacy=True)
        transfer(work, engines, vis, gain, 'no-window-map', native_options={'refspwmap': []})
        transfer(work, engines, vis, gain, 'append', attempts=[({}, True), ({'append': True}, True)])
        transfer(work, engines, vis, gain, 'occupied', attempts=[({}, True), ({}, False)])
        for name, options in [
            ('empty-output', {'tableout': ''}), ('empty-reference', {'reference': []}),
            ('unknown-reference', {'reference': 'MISSING'}),
            ('unknown-transfer', {'transfer': 'MISSING'}),
            ('missing-input', {'tablein': str(work/'missing.gcal')}),
            ('wrong-input', {'tablein': str(vis)}),
        ]:
            transfer(work, engines, vis, gain, name, options=options, product=False)
        unsupported = work/'unsupported.cal'
        copytree(gain, unsupported)
        with opened(unsupported, True) as table:
            info = table.info()
            info['subType'] = 'B Jones'
            assert table.putinfo(info)
        transfer(work, engines, vis, unsupported, 'unsupported-input', product=False, expected_history=1)
        parent = work/'file-parent'
        parent.write_text('preserve this file')
        transfer(work, engines, vis, gain, 'output-file-parent', product=False,
                 output_parent=parent, expected_history=2)
        assert parent.read_text() == 'preserve this file'
        parent = work/'missing-parent'
        transfer(work, engines, vis, gain, 'output-missing-parent', product=False,
                 output_parent=parent, expected_history=2)
        assert not parent.exists()
        check_linked_subtables(work, engines, vis, gain)
        check_retries(work, engines, vis, gain)
        check_history_write_failure(work, native_calibrater, vis, gain)
        windows, window_gains = multi_window(vis, gain, work)
        with opened(windows/'SPECTRAL_WINDOW', True) as table:
            for row in range(table.nrows()):
                assert table.putcell('NAME', row, f'Attached SPW {row}')
        for degree in (0, 1, 2):
            values = transfer(work, engines, windows, window_gains, f'fit-{degree}',
                              options={'fitorder': degree, 'refspwmap': [0, 0, 2]})
            for value in values.values():
                np.testing.assert_array_equal(value[0][1]['spwName'], [f'Attached SPW {i}' for i in range(3)])


if __name__ == '__main__':
    main()
