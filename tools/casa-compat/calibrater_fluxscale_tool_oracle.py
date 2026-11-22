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

from blcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from calibration_lifecycle_oracle import contents, equal, metadata, snapshot
from fluxscale_oracle import compare, compare_history, compare_listing, history, multi_window, plain, set_scans
from record_casa_returns import make_cal_ms


def transfer(work, engines, vis, gain, name, *, options=None, attempts=None, legacy=False,
             positional=False, listing=False, selection=None, product=True, native_options=None):
    paths, histories, returns, listings = {}, {}, {}, {}
    before = snapshot(gain)
    signature = inspect.signature(casa_calibrater.fluxscale).parameters
    for engine, make_tool in engines.items():
        tool = make_tool()
        path = work/f'{name}-{engine}.cal'
        paths[engine] = path
        assert tool.setvi(legacy, True)
        assert result(tool.fluxscale, str(gain)) == (False, ('RuntimeError', 'Must first open a MeasurementSet.'))
        assert tool.open(str(vis), addcorr=False, addmodel=False)
        if selection:
            assert tool.selectvis(**selection)
        prior, activity = tool.returndict(), tool.activityrec()
        start, _ = history(vis)
        common = dict(tablein=str(gain), tableout=str(path), reference='FLUX', transfer='PHASE')
        common.update(options or {})
        if engine == 'native':
            common.update(native_options or {})
        if listing:
            listing_path = work/f'{name}-{engine}.txt'
            common['listfile'] = f'  {listing_path}\t\n'
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
        histories[engine] = history(vis, start)[1]
        same_record(tool.returndict(), prior)
        same_record(tool.activityrec(), activity)
        assert tool.close()
        assert path.is_dir() == product, (name, engine)
        if listing:
            listings[engine] = listing_path.read_text()
    assert snapshot(gain) == before
    for expected, actual in zip(returns['casa'], returns['native']):
        if not expected[0]:
            assert expected == actual, (name, expected, actual)
        else:
            for key in ('freq', 'spwID', 'spwName'):
                assert type(actual[1][key]) is type(expected[1][key]), (name, key)
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
            ('gain-cut', {'gainthreshold': 15., 'antenna': 'A0~A3'}),
            ('calibration-scans', {'scan': '1,3'}),
            ('calibration-times', {'timerange': '~'.join(bounds)}),
        ]:
            transfer(work, engines, vis, gain, name, options=options)
        transfer(work, engines, vis, gain, 'retains-selection', selection={'field': 'PHASE'})
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
            assert table.putinfo(info) and table.putkeyword('VisCal', 'B Jones')
        transfer(work, engines, vis, unsupported, 'unsupported-input', product=False)
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
