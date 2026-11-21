#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare F Jones generation, application and solver preapply with CASA."""

import argparse
from pathlib import Path

import casatasks as casa
from casatools import coordsys, image
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import columns, compare_mapping, opened
from record_casa_returns import make_cal_ms


def make_image(path, origin, *, daily=False, transformed=False):
    coords = coordsys()
    assert coords.newcoordsys(linear=3)
    assert coords.setreferencevalue([-180., -90., 0. if daily else origin - 100.])
    assert coords.setreferencepixel([0., 0., 0.])
    assert coords.setincrement([4., 4., 3600. if daily else 100.])
    if transformed:
        assert coords.setlineartransform(type='linear', value=[[2., .125, 0.], [0., 1., 0.], [0., 0., 1.]])
    x, y, t = np.indices((91, 46, 24 if daily else 6), dtype=np.float32)
    pixels = 3. + .03 * x + .11 * y + .12 * t
    record = coords.torecord()
    record['linear0']['units'] = ['deg', 'deg', 's']
    tool = image()
    assert tool.fromarray(str(path), pixels, csys=record)
    tool.done()
    coords.done()


def rms(actual, expected):
    return float(np.linalg.norm(actual - expected) / max(np.linalg.norm(expected), 1e-20))


def compare_tables(actual, expected):
    actual_columns = columns(actual)
    expected_columns = columns(expected)
    actual_tec = actual_columns.pop('FPARAM')
    expected_tec = expected_columns.pop('FPARAM')
    residual = rms(actual_tec, expected_tec)
    assert residual < 2e-7, residual
    assert actual_tec.shape == (1, 1, 60)
    assert np.ptp(expected_tec) > 1e15
    assert not expected_columns['FLAG'].any()
    np.testing.assert_array_equal(expected_columns['SNR'], 100.)
    np.testing.assert_array_equal(expected_columns['SPECTRAL_WINDOW_ID'], 0)
    compare_mapping(actual_columns, expected_columns, False)
    for path in (actual, expected):
        with opened(path) as cal:
            assert cal.info()['subType'] == cal.getkeyword('VisCal') == 'F Jones'
            assert cal.getkeyword('ParType') == 'Float'
    for key in ('CHAN_FREQ', 'REF_FREQUENCY', 'NUM_CHAN', 'TOTAL_BANDWIDTH', 'CHAN_WIDTH'):
        with opened(actual/'SPECTRAL_WINDOW') as native, opened(expected/'SPECTRAL_WINDOW') as cpp:
            np.testing.assert_array_equal(native.getcol(key), cpp.getcol(key))
    return residual, expected_tec


def generate(root, vis, engines):
    with opened(vis) as main:
        start = main.getcol('TIME').min()
    absolute, daily, outside, transformed = (
        root/f'{kind}.tec' for kind in ('absolute', 'daily', 'outside', 'transformed')
    )
    make_image(absolute, start)
    make_image(daily, start, daily=True)
    make_image(outside, start + 7200.)
    make_image(transformed, start, transformed=True)
    outputs, measured = {}, {}
    for case, source, parameter, height in (
        ('default', absolute, [], 450.), ('changed', absolute, [600.], 600.),
        ('vector', absolute, [300., 600.], 450.), ('daily', daily, [], 450.),
        ('negative', absolute, [-450.], -450.),
        ('transformed', transformed, [], 450.),
    ):
        results = {}
        for label, api in engines.items():
            output = root/f'{label}-{case}.cal'
            assert api.gencal(str(vis), str(output), 'tecim', str(source), spw='0', antenna='A0',
                              pol='ignored', parameter=parameter, uniform=False) is None
            with opened(output) as cal:
                assert cal.getkeyword('IonosphereHeight(km)') == height
            results[label] = output
        error, observed = compare_tables(results['native'], results['casa'])
        measured[case] = observed
        print(f'{case} line-of-sight TEC normalized RMS {error:.3g}', flush=True)
        outputs[case] = results
    np.testing.assert_array_equal(measured['default'], measured['vector'])
    assert rms(measured['changed'], measured['default']) > 5e-4
    assert rms(measured['daily'], measured['default']) > 1e-3
    assert rms(measured['negative'], measured['default']) > 1e-3
    assert rms(measured['transformed'], measured['default']) > 1e-3
    for label, api in engines.items():
        output = root/f'{label}-overwrite.cal'
        api.gencal(str(vis), str(output), 'tecim', str(absolute))
        api.gencal(str(vis), str(output), 'tecim', str(absolute), parameter=[600.])
        assert rms(columns(output)['FPARAM'], measured['changed']) < 2e-7
        if label == 'native':
            try:
                api.gencal(str(vis), str(output), 'tecim', str(outside))
            except RuntimeError as error:
                assert 'TEC image does not cover' in str(error)
            else:
                raise AssertionError('An out-of-range TEC image replaced an existing product')
            assert rms(columns(output)['FPARAM'], measured['changed']) < 2e-7
    return outputs['default']


def repolarize(vis):
    with opened(vis, True) as main:
        original = main.getcol('DATA')
        original[1] = np.complex64(.21 + .30j)
        original[2] = np.complex64(-.27 + .15j)
        original[3] *= np.complex64(.81 - .20j)
        main.putcol('DATA', original)
        flags = np.zeros_like(main.getcol('FLAG'))
        flags[1, 2, 7] = True
        main.putcol('FLAG', flags)
    return original


def apply_and_solve(root, vis, engines, tables):
    observed = root/'polarized.ms'
    copy_ms(vis, observed)
    samples = repolarize(observed)
    reference = {}
    solve_reference = None
    for writer, cal in tables.items():
        for reader, api in engines.items():
            solve = root/f'{writer}-{reader}-preapply.ms'
            copy_ms(vis, solve)
            output = root/f'{writer}-{reader}-preapply.cal'
            result = api.gaincal(str(solve), str(output), field='PHASE', gaintable=[str(cal)],
                                 solint='int', refant='A0', minsnr=0., minblperant=2)
            assert isinstance(result, dict)
            with opened(output) as solutions:
                values = solutions.getcol('CPARAM')
                flags = solutions.getcol('FLAG')
            assert values.size and not flags.any()
            if solve_reference is None:
                solve_reference = values
            else:
                error = rms(values, solve_reference)
                assert error < 5e-4, (writer, reader, error)
                print(f'{writer} to {reader} gain preapply normalized RMS {error:.3g}', flush=True)

            for selection, mode in (('', 'calflag'), ('0:1~2', 'calflag'), ('', 'calonly')):
                basename = f'{writer}-{reader}-{mode}-spw-{bool(selection)}'
                work = root/f'{basename}.ms'
                copy_ms(observed, work)
                assert api.applycal(str(work), spw=selection, applymode=mode, gaintable=[str(cal)], spwmap=[99],
                                    calwt=[True], flagbackup=False) is None
                with opened(work) as main, opened(observed) as raw:
                    data = main.getcol('CORRECTED_DATA')
                    flags = main.getcol('FLAG')
                    np.testing.assert_array_equal(main.getcol('WEIGHT'), raw.getcol('WEIGHT'))
                    np.testing.assert_array_equal(main.getcol('SIGMA'), raw.getcol('SIGMA'))
                    if mode == 'calonly':
                        np.testing.assert_array_equal(flags, raw.getcol('FLAG'))
                assert rms(data[~flags], samples[~flags]) > .01
                if (selection, mode) not in reference:
                    reference[selection, mode] = (data, flags)
                else:
                    want, want_flags = reference[selection, mode]
                    np.testing.assert_array_equal(flags, want_flags, err_msg=basename)
                    error = rms(data[~flags], want[~flags])
                    assert error < 5e-4, (basename, error)
                    print(f'{basename} corrected data normalized RMS {error:.3g}', flush=True)


def apply_at_field_reference(root, vis, engines, cal):
    source = root/'apparent-polynomial.ms'
    copy_ms(vis, source)
    repolarize(source)
    with opened(source, True) as main:
        main.putcolkeyword('TIME', 'MEASINFO', {'type': 'epoch', 'Ref': 'TAI'})
        epoch = float(main.getcol('TIME').min())
    with opened(source/'FIELD', True) as fields:
        for row in range(fields.nrows()):
            fields.putcell('NUM_POLY', row, 1)
            fields.putcell('TIME', row, epoch)
            for column in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
                initial = fields.getcell(column, row).reshape(2, 1)
                fields.putcell(column, row, np.column_stack((initial, [.002, -.001])))
    outputs = {}
    for label, api in engines.items():
        target = root/f'{label}-ref-epoch.ms'
        copy_ms(source, target)
        api.applycal(str(target), gaintable=[str(cal)], flagbackup=False)
        with opened(target) as result:
            outputs[label] = result.getcol('CORRECTED_DATA')
    error = rms(outputs['native'], outputs['casa'])
    assert error < 1e-4, error
    print(f'Polynomial field on a TAI-labelled MS normalized RMS {error:.3g}', flush=True)


def flagged_solution(root, vis, engines, source):
    with opened(source) as cal:
        copied = cal.copy(str(root/'flagged-last-antenna.cal'), deep=True, valuecopy=True)
        copied.close()
    cal = root/'flagged-last-antenna.cal'
    with opened(cal, True) as table:
        antenna = table.getcol('ANTENNA1')
        flags = table.getcol('FLAG')
        flags[..., antenna == antenna.max()] = True
        table.putcol('FLAG', flags)
    for label, api in engines.items():
        target = root/f'{label}-flagged-solution.ms'
        copy_ms(vis, target)
        api.applycal(str(target), gaintable=[str(cal)], flagbackup=False)
        with opened(target) as result:
            flags = result.getcol('FLAG')
            baseline = (result.getcol('ANTENNA1') == antenna.max()) | (result.getcol('ANTENNA2') == antenna.max())
        if label == 'native':
            assert flags[..., baseline].all() and not flags[..., ~baseline].any()
        else:
            assert not flags.any()


def apply_library(root, vis, engines, cal):
    source = root/'library-input.ms'
    copy_ms(vis, source)
    repolarize(source)
    library = root/'tec.callib'
    library.write_text(f"caltable='{cal}' calwt=False tinterp='nearest' finterp='linear' spwmap=[0]\n")
    results = {}
    for label, api in engines.items():
        via_library = root/f'{label}-library.ms'
        copy_ms(source, via_library)
        api.applycal(str(via_library), docallib=True, callib=str(library), flagbackup=False)
        via_table = root/f'{label}-table.ms'
        copy_ms(source, via_table)
        api.applycal(str(via_table), gaintable=[str(cal)], interp=['nearest'], flagbackup=False)
        with opened(via_library) as applied, opened(via_table) as ordinary:
            data = applied.getcol('CORRECTED_DATA')
            flags = applied.getcol('FLAG')
            assert rms(data, ordinary.getcol('CORRECTED_DATA')) > 1e-3
            results[label] = data, flags
    got, flags = results['native']
    want, want_flags = results['casa']
    np.testing.assert_array_equal(flags, want_flags)
    error = rms(got[~flags], want[~flags])
    assert error < 1e-4, error
    print(f'Calibration library on a 600 km table normalized RMS {error:.3g}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    work = args.work.resolve()
    work.mkdir()
    engines = {'casa': casa, 'native': native}
    for name, basis in (('linear', 'XX XY YX YY'), ('circular', 'RR RL LR LL')):
        root = work/name
        root.mkdir()
        vis = root/'seed.ms'
        make_cal_ms(str(vis), channels=4, frequency='332MHz', stokes=basis, telescope='VLA')
        with opened(vis, True) as main:
            main.putcol('FLAG', np.zeros_like(main.getcol('FLAG')))
            main.putcol('FLAG_ROW', np.zeros_like(main.getcol('FLAG_ROW')))
        tables = generate(root, vis, engines)
        apply_and_solve(root, vis, engines, tables)
        if name == 'circular':
            apply_at_field_reference(root, vis, engines, tables['casa'])
            flagged_solution(root, vis, engines, tables['casa'])
            apply_library(root, vis, engines, root/'casa-changed.cal')


if __name__ == '__main__':
    main()
