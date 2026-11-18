#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare moving-field calibration and diagnostics with CASA 6.7.6.14."""

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import measures, msmetadata, table
import numpy as np

from applycal_oracle import copy_ms
from gaincal_oracle import ephemeris_observation, gain_and_apply, relative_rms
from record_casa_returns import make_cal_ms


def selected_epochs(dataset):
    tb = table()
    assert tb.open(str(dataset))
    try:
        a1, a2, field, time, flag = (tb.getcol(column) for column in (
            'ANTENNA1', 'ANTENNA2', 'FIELD_ID', 'TIME', 'FLAG_ROW'))
        return np.unique(time[(a1 == 0) & (a2 == 1) & (field == 0) & ~flag])
    finally:
        tb.close()


def snapshot_angles(root, dataset, epochs):
    """Use CASA's attached-field phase centre and evaluate its geometry at each epoch."""
    snapshot = root/'fixed-direction-snapshot.ms'
    copy_ms(dataset, snapshot)
    metadata = msmetadata()
    assert metadata.open(str(dataset))
    measure = measures()
    tb = table()
    output = []
    try:
        for stamp in epochs:
            epoch = measure.epoch('UTC', {'value': float(stamp), 'unit': 's'})
            reference = metadata.phasecenter(0, epoch)
            if reference['refer'] == 'ICRS':
                reference = measure.measure(reference, 'J2000')
            assert reference['refer'] == 'J2000'
            centre = np.array([[reference['m0']['value']], [reference['m1']['value']]])
            assert tb.open(str(snapshot/'FIELD'), nomodify=False)
            try:
                tb.putcell('EPHEMERIS_ID', 0, -1)
                for column in ('PHASE_DIR', 'REFERENCE_DIR', 'DELAY_DIR'):
                    tb.putcell(column, 0, centre)
            finally:
                tb.close()
            assert tb.open(str(snapshot))
            by_baseline = []
            try:
                for first in (0, 1):
                    rows = tb.query(f'FIELD_ID==0 AND ANTENNA1=={first} AND ANTENNA2=={first+1} '
                                    f'AND abs(TIME-{stamp:.17g})<0.001',
                                    columns='mscal.azel1() AS AZEL, mscal.hadec1() AS HADEC, '
                                            'mscal.pa1() AS PA, mscal.azel() AS ARRAY_AZEL, '
                                            'mscal.ha() AS ARRAY_HA, mscal.uvwJ2000() AS UVW')
                    try:
                        az, el = rows.getcell('AZEL', 0)
                        ha, dec = rows.getcell('HADEC', 0)
                        pa = rows.getcell('PA', 0)
                        array_az, array_el = rows.getcell('ARRAY_AZEL', 0)
                        array_ha = rows.getcell('ARRAY_HA', 0)
                        uvw = rows.getcell('UVW', 0)
                    finally:
                        rows.close()
                    by_baseline.append([az, el, ha, dec, pa, array_az, array_el, array_ha, *uvw])
            finally:
                tb.close()
            output.append(by_baseline)
    finally:
        metadata.done()
        measure.done()
    return np.asarray(output)


def diagnostic_series(root, native, dataset, epochs, expected):
    result = {}
    axes = [
        ('azimuth_vs_time', np.rad2deg(expected[:, 0]), 360.),
        ('elevation_vs_time', np.rad2deg(expected[:, 1]), None),
        ('hour_angle_vs_time', expected[:, 2]*12/np.pi, 24.),
        ('parallactic_angle_vs_time', np.rad2deg(expected[:, 4]), 360.),
    ]
    for preset, values, period in axes:
        for label, options in [('raw', {}), ('averaged', {'avgtime': 1.})]:
            plot = root/f'{preset}-{label}.txt'
            native.msexplore(vis=str(dataset), field='0', spw='0', selectdata=True,
                            msselect='ANTENNA1==0 && ANTENNA2==1',
                            preset=preset, plot_output=str(plot), plot_format='txt', **options)
            lines = [line.split('\t') for line in plot.read_text().splitlines()
                     if line and not line.startswith(('#', 'series_key', 'panel_key'))]
            actual = np.asarray(sorted((float(line[-2]), float(line[-1])) for line in lines))
            assert actual.shape == (len(epochs), 2), (preset, label, actual.shape, len(epochs))
            difference = actual[:, 1] - values
            if period is not None:
                difference = (difference + period/2) % period - period/2
            np.testing.assert_allclose(difference, 0., atol=2e-3, rtol=0, err_msg=preset)
            result[f'{preset}-{label}'] = float(np.max(np.abs(difference)))
    return result


def opacity(root, dataset, native, name):
    calibration = root/f'{name}-opacity.cal'
    casa.gencal(str(dataset), caltable=str(calibration), caltype='opac', parameter=[.1])
    measured = []
    tb = table()
    for engine, task in [('casa', casa), ('native', native)]:
        sample = root/f'{name}-{engine}.ms'
        copy_ms(dataset, sample)
        task.applycal(str(sample), field='0', spw='0', gaintable=[str(calibration)],
                      calwt=False, flagbackup=False)
        assert tb.open(str(sample))
        try:
            measured.append({name: tb.getcol(name) for name in (
                'DATA', 'CORRECTED_DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA')})
        finally:
            tb.close()
    expected, actual = measured
    for column in expected.keys() - {'CORRECTED_DATA'}:
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    corrected = relative_rms(actual['CORRECTED_DATA'], expected['CORRECTED_DATA'])
    assert corrected < 1e-3, (name, corrected)
    return {'corrected_nrms': float(corrected)}


def with_last_epoch(source, destination, last_epoch):
    copy_ms(source, destination)
    offset = last_epoch - selected_epochs(destination)[-1]
    tb = table()
    assert tb.open(str(destination), nomodify=False)
    try:
        for column in ('TIME', 'TIME_CENTROID'):
            tb.putcol(column, tb.getcol(column) + offset)
    finally:
        tb.close()


def check_upper_boundary(root, moving, native, last_epoch):
    outside = root/'outside.ms'
    with_last_epoch(moving, outside, last_epoch)
    metadata = msmetadata()
    measure = measures()
    assert metadata.open(str(outside))
    try:
        epoch = measure.epoch('UTC', {'value': last_epoch, 'unit': 's'})
        try:
            metadata.phasecenter(0, epoch)
        except RuntimeError as error:
            assert 'extractDirMeas' in str(error)
        else:
            raise AssertionError('CASA accepted the final ephemeris sample')
    finally:
        metadata.done()
        measure.done()
    try:
        native.msexplore(vis=str(outside), field='0', spw='0', selectdata=True,
                         preset='elevation_vs_time', plot_output=str(root/'outside.txt'),
                         plot_format='txt')
    except RuntimeError as error:
        assert 'outside the ephemeris interpolation range' in str(error)
    else:
        raise AssertionError('casa-rs accepted the final ephemeris sample')
    assert not (root/'outside.txt').exists()
    return {'refused': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--calibrate-bin', required=True, type=Path)
    parser.add_argument('--msexplore-bin', required=True, type=Path)
    options = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(options.calibrate_bin.resolve())
    os.environ['CASARS_MSEXPLORE_BIN'] = str(options.msexplore_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks

    root = options.work.resolve()
    root.mkdir(parents=True)
    fixed = root/'fixed.ms'
    make_cal_ms(str(fixed), stokes='XX XY YX YY')
    moving, _ = ephemeris_observation(root, fixed, offset=(0.002, -0.004))
    equatorial = root/'equatorial.ms'
    copy_ms(moving, equatorial)
    tb = table()
    assert tb.open(str(equatorial/'ANTENNA'), nomodify=False)
    try:
        tb.putcell('MOUNT', 0, 'EQUATORIAL')
    finally:
        tb.close()
    ephemeris = next((equatorial/'FIELD').glob('EPHEM*.tab'))
    assert tb.open(str(ephemeris), nomodify=False)
    try:
        tb.putkeyword('posrefsys', 'ICRF/ICRS')
    finally:
        tb.close()
    assert tb.open(str(ephemeris))
    try:
        last_epoch = float(tb.getcol('MJD')[-1]) * 86400.
    finally:
        tb.close()
    edge = root/'near-end.ms'
    with_last_epoch(moving, edge, last_epoch - 30.)
    results, derived = {}, {}
    for mount, dataset in [('alt-az', moving), ('equatorial', equatorial), ('near-end', edge)]:
        case = root/mount
        case.mkdir()
        epochs = selected_epochs(dataset)
        assert len(epochs) > 2
        angles = snapshot_angles(case, dataset, epochs)
        rows = [(stamp, first, numbers)
                for stamp, expected in zip(epochs.tolist(), angles.tolist())
                for first, numbers in enumerate(expected)]
        derived[mount] = {'ms': dataset.name, 'epochs': rows}
        results[mount] = diagnostic_series(case, tasks, dataset, epochs, angles[:, 0])
    (root/'derived.json').write_text(json.dumps(derived, indent=2)+'\n')
    results['upper-boundary'] = check_upper_boundary(root, moving, tasks, last_epoch)
    results['moving-opacity'] = opacity(root, moving, tasks, 'moving-opacity')
    results['icrs-opacity'] = opacity(root, equatorial, tasks, 'icrs-opacity')
    results['fixed-opacity'] = opacity(root, fixed, tasks, 'fixed-opacity')
    solve = root/'moving-parallactic-gain'
    solve.mkdir()
    results['moving-parallactic-gain'] = gain_and_apply(
        solve, moving, tasks, 'G', 'int', calmode='ap', field='0', spw='0',
        minblperant=2, smodel=[1., .15, .1, 0.], parang=True, apply_options={'parang': True})
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
