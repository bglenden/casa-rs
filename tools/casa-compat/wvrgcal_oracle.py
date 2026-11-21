#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the pinned WVR task and tool on the same compact ALMA observations."""

import argparse
import os
from pathlib import Path

import casatasks as casa
from casatools import measures, simulator, wvr
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened


def fixture(ms):
    """A five-antenna science window and 4-channel radiometer autos."""
    me = measures()
    sm = simulator()
    assert sm.open(str(ms))
    assert sm.setconfig(telescopename='ALMA',
                        x=[0., 65., 0., 65., 130.], y=[0., 0., 65., 65., 0.],
                        z=[0.]*5, dishdiameter=[12.]*5, mount=['alt-az']*5,
                        antname=[f'DA{x:02d}' for x in range(1,6)],
                        padname=[f'P{x}' for x in range(5)], coordsystem='local',
                        referencelocation=me.observatory('ALMA'))
    assert sm.setfeed(mode='perfect X Y')
    assert sm.setspwindow(spwname='science', freq='95GHz', deltafreq='2MHz',
                          freqresolution='2MHz', nchannels=8, stokes='XX YY')
    assert sm.setspwindow(spwname='WVR#NOMINAL', freq='183GHz', deltafreq='1GHz',
                          freqresolution='1GHz', nchannels=4, stokes='XX')
    assert sm.setfield(sourcename='FLUX', sourcedirection=me.direction('J2000','0h0m0s','-28d0m0s'))
    assert sm.settimes(integrationtime='2s', usehourangle=True, referencetime=me.epoch('UTC','2020/01/01'))
    assert sm.setauto(autocorrwt=0.)
    assert sm.observe('FLUX','science', starttime='0s', stoptime='24s')
    assert sm.setauto(autocorrwt=1.)
    assert sm.observe('FLUX','WVR#NOMINAL',starttime='0s',stoptime='24s')
    sm.close()
    with opened(ms/'STATE', True) as t:
        if t.nrows() == 0:
            t.addrows(1)
        t.putcell('OBS_MODE',0,'OBSERVE_TARGET#ON_SOURCE')
        t.putcell('SIG',0,True)
        t.putcell('REF',0,False)
    with opened(ms, True) as t:
        times, windows = t.getcol('TIME'),t.getcol('DATA_DESC_ID')
        a1,a2 = t.getcol('ANTENNA1'),t.getcol('ANTENNA2')
        epochs = {timestamp: i for i,timestamp in enumerate(sorted(set(times[windows==1])))}
        t.putcol('STATE_ID', np.zeros(t.nrows(), np.int32))
        t.putcol('FLAG_ROW', np.zeros(t.nrows(), bool))
        for row, (timestamp,window,ant) in enumerate(zip(times,windows,a1)):
            if window == 1:
                amp = np.array([218.,142.,88.,43.]) + (0.2*ant+.15*epochs[timestamp])*np.array([1.0,.9,.7,.3])
                cell = amp[None,:].astype(np.complex64)
            else:
                cell = np.ones((2,8),np.complex64)
            existing = t.getcell('DATA', row)
            assert existing.shape == cell.shape, (row, existing.shape, cell.shape)
            t.putcell('DATA',row,cell)
            t.putcell('FLAG', row, np.full(cell.shape, window == 1 and a1[row] != a2[row], bool))

def read_solution(path):
    columns = ('TIME', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'ANTENNA1', 'ANTENNA2',
               'INTERVAL', 'SCAN_NUMBER', 'OBSERVATION_ID', 'CPARAM', 'FLAG', 'PARAMERR', 'SNR')
    with opened(path) as table:
        assert table.info()['subType'] == 'T Jones'
        assert table.getkeyword('VisCal') == 'T Jones'
        assert table.getkeyword('ParType') == 'Complex'
        assert table.nrows() == 120
        result = {name: table.getcol(name) for name in columns}
    np.testing.assert_array_equal(np.unique(result['SPECTRAL_WINDOW_ID']), [0, 1])
    np.testing.assert_array_equal(result['SNR'], 3.)
    np.testing.assert_array_equal(result['PARAMERR'], 0.)
    assert not np.any(result['FLAG'])
    assert np.max(np.abs(np.angle(result['CPARAM']))) > 0.5
    return result


def compare_solution(actual, expected):
    for name, values in expected.items():
        if name == 'CPARAM':
            relative_rms = np.linalg.norm(actual[name] - values) / np.linalg.norm(values)
            assert relative_rms <= 1e-3, (name, relative_rms)
        else:
            np.testing.assert_array_equal(actual[name], values, err_msg=name)


def report_contents(root, api, task_tool, vis, stem):
    reports = {}
    for seed in (0, 27):
        path = root/f'{stem}-seed-{seed}.cal'
        report = api.wvrgcal(str(vis), str(path), usefieldtab=True, wvrspw=[1], rseed=seed)
        assert report['success'] and report['rval'] == 0
        assert report['Name'] == [f'DA{number:02d}' for number in range(1, 6)]
        assert report['WVR'] == [True] * 5 and report['Flag'] == [False] * 5
        np.testing.assert_array_equal(report['Frac_unflagged'], 1.0)
        reports[seed] = read_solution(path), report
    assert not np.array_equal(reports[0][0]['CPARAM'], reports[27][0]['CPARAM'])
    for continuum in (False, True):
        path = root/f'{stem}-tool-{continuum}.cal'
        logfile = root/f'{stem}-tool-{continuum}.log'
        status = task_tool.gcal(vis=str(vis), output=str(path), logfile=str(logfile),
                                usefieldtab=True, wvrspw=[1], cont=continuum)
        assert status == (-2 if continuum else 0)
        assert logfile.is_file() and logfile.stat().st_size > 500
        result = read_solution(path)
        if not continuum:
            compare_solution(result, reports[0][0])
        reports[continuum, 'tool'] = result
    return reports


def compare_applied(root, vis, readers):
    expected = None
    for reader, api in readers.items():
        for writer in readers:
            output = root/f'{reader}-apply-{writer}.ms'
            copy_ms(vis, output)
            gaintable = root/f'{writer}-seed-0.cal'
            assert api.applycal(str(output), spw='0', gaintable=[str(gaintable)],
                                calwt=False, flagbackup=False) is None
            with opened(output) as table:
                science = np.flatnonzero(table.getcol('DATA_DESC_ID') == 0)
                data = np.stack([table.getcell('CORRECTED_DATA', int(row)) for row in science])
                flags = np.stack([table.getcell('FLAG', int(row)) for row in science])
            if expected is None:
                expected = data, flags
                assert np.max(np.abs(data - 1.0)) > 1e-2
            else:
                np.testing.assert_array_equal(flags, expected[1])
                assert np.linalg.norm(data - expected[0]) / np.linalg.norm(expected[0]) <= 1e-3


def compare_solver_preapply(root, vis, engines):
    """Recover injected complex gains after preapplying each engine's WVR table."""
    prior = read_solution(root / 'casa-seed-0.cal')
    atmospheric = {(time, antenna): value for time, antenna, window, value in
                   zip(prior['TIME'], prior['ANTENNA1'], prior['SPECTRAL_WINDOW_ID'],
                       prior['CPARAM'].reshape(-1)) if window == 0}
    injected = np.array([.95, 1.10, 1.12, .91, 1.08]) * np.exp(1j * np.array([0., .06, -.04, .02, -.09]))
    source = root / 'preapply.ms'
    copy_ms(vis, source)
    with opened(source, True) as ms:
        science = np.flatnonzero(ms.getcol('DATA_DESC_ID') == 0)
        for row in science:
            row = int(row)
            time, left, right = (ms.getcell(column, row) for column in
                                 ('TIME', 'ANTENNA1', 'ANTENNA2'))
            transfer = atmospheric[time, left] * np.conj(atmospheric[time, right])
            baseline = injected[left] * np.conj(injected[right])
            perturbation = 1 + 7e-4 * np.cos(left + 2 * right) + 1e-4j * np.sin(left * right)
            ms.putcell('DATA', row, np.full((2, 8), transfer * baseline * perturbation, np.complex64))
    for reader, api in engines.items():
        for writer in engines:
            output = root / f'{reader}-preapply-{writer}.cal'
            prior = root / f'{writer}-seed-0.cal'
            result = api.gaincal(str(source), str(output), spw='0', solint='int',
                                 refant='DA01', minsnr=0., minblperant=2,
                                 gaintable=[str(prior)], interp=['nearest'],
                                 gaintype='T', calmode='ap', smodel=[1.])
            assert isinstance(result, dict)
            with opened(output) as solution:
                assert not solution.getcol('FLAG').any()
                gains = solution.getcol('CPARAM').reshape(-1)
                expected = injected[solution.getcol('ANTENNA1')]
            assert gains.size == 60
            np.testing.assert_allclose(gains, expected, atol=9e-4)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    os.chdir(root)
    vis = root/'radiometer.ms'
    fixture(vis)
    reference = report_contents(root, casa, wvr(), vis, 'casa')
    readers = {'casa': casa}
    if not args.reference_only:
        from casars import tasks
        from casars.tools import wvr as native_wvr

        native = report_contents(root, tasks, native_wvr(), vis, 'native')
        readers['native'] = tasks
        for seed in (0, 27):
            compare_solution(native[seed][0], reference[seed][0])
            for name in ('RMS_um', 'Disc_um'):
                np.testing.assert_allclose(native[seed][1][name], reference[seed][1][name], atol=0.1)
        for continuum in (False, True):
            compare_solution(native[continuum, 'tool'], reference[continuum, 'tool'])
    compare_applied(root, vis, readers)
    compare_solver_preapply(root, vis, readers)
    print('Two seeds, both WVR entry points, continuum status/log, science apply and preapply passed.')


if __name__ == '__main__':
    main()
