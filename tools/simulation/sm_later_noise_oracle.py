#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Isolated actual CASA6.7.6.14 later-observation noise controls (#1292)."""
import argparse
import concurrent.futures
import json
import resource
from pathlib import Path
import subprocess
import sys
import numpy as np


def read_main(path):
    from casatools import table
    tb = table(); tb.open(str(path))
    out = {name: np.asarray([tb.getcell(column, r) for r in range(tb.nrows())])
           for name, column in [('data','DATA'), ('time','TIME'), ('p','ANTENNA1'),
                                ('q','ANTENNA2'), ('window','DATA_DESC_ID'), ('flag','FLAG_ROW')]}
    tb.close()
    return out


def save_cal(path, label, output):
    from casatools import table
    if not path.exists(): return None
    tb = table(); tb.open(str(path))
    for name, column in [('time','TIME'), ('p','ANTENNA1'), ('q','ANTENNA2'),
                          ('window','SPECTRAL_WINDOW_ID'), ('values','CPARAM')]:
        output[f'{label}_{name}'] = np.asarray([tb.getcell(column, r) for r in range(tb.nrows())])
    subtype = tb.info()['subType']; tb.close(); return subtype


def worker(case, mode, otf, new_window):
    import casatasks
    from sm_multiple_models_oracle import make_session
    assert casatasks.version_string() == '6.7.6.14'
    path = case / 'data.ms'
    result = dict(mode=mode, otf=otf, new_window=new_window, stage='creating')
    def record(): (case/'result.json').write_text(json.dumps(result, indent=2))
    record()
    sm = make_session(path, 'XX XY YX YY')
    try:
        original = read_main(path)
        sm.setseed(19)
        result['setnoise'] = bool(sm.setnoise(mode=mode, OTF=otf, simplenoise='1Jy',
                                            pwv='1mm', tau=0.05, table=str(case/'noise.cal')))
        assert result['setnoise']
        window = 'band6'
        if new_window:
            assert sm.setspwindow(spwname='later', freq='100GHz', deltafreq='15.625MHz',
                freqresolution='15.625MHz', nchannels=4, stokes='XX XY YX YY', refcode='TOPO')
            window = 'later'
        result['observe'] = bool(sm.observe(sourcename='target', spwname=window,
                                            starttime='3563s', stoptime='3583s'))
        result['stage'] = 'before_corrupt'; record()
        result['corrupt'] = bool(sm.corrupt())
        result['stage'] = 'after_corrupt'; record()
    except Exception as error:
        result['exception'] = str(error)
    finally:
        sm.close()
    output = read_main(path)
    later = output['time'] > original['time'].max()
    result.update(rows=len(output['time']), later_rows=int(later.sum()),
                  later_flags=int(output['flag'][later].sum()),
                  old_power=float(np.mean(np.abs(output['data'][~later])**2)),
                  later_power=float(np.mean(np.abs(output['data'][later])**2)))
    result['calibrations'] = {label: save_cal(case/f'noise.{label}.cal', label, output)
                              for label in ['A','T']}
    output['later'] = later
    np.savez_compressed(case/'data.npz', **output)
    result['stage'] = 'complete'; record()
    return result



def nearest_values(case, prefix, antenna2=False, reverse=False):
    values, covered = [], []
    antennas = case['q'] if reverse else case['p']
    for time, p, q, window in zip(case['time'], antennas, case['q'], case['window']):
        matches = np.flatnonzero((case[prefix + '_window'] == window)
                                 & (case[prefix + '_p'] == p))
        if antenna2:
            matches = matches[case[prefix + '_q'][matches] == q]
        if len(matches):
            index = matches[np.argmin(np.abs(case[prefix + '_time'][matches] - time))]
            values.append(case[prefix + '_values'][index])
            covered.append(True)
        else:
            values.append(None)
            covered.append(False)
    return values, np.asarray(covered)


def nrms(actual, expected):
    return float(np.linalg.norm(actual - expected) / max(np.linalg.norm(expected), 1e-30))


def expected_data(case, unit=None):
    if unit is None:
        a, covered = nearest_values(case, 'A', antenna2=True)
        expected = np.asarray([-v if v is not None else np.zeros_like(case['data'][0])
                               for v in a])
    else:
        expected = unit.copy()
        covered = case['window'] == 0
        expected[~covered] = 0
    if 'T_values' in case:
        first, t_covered = nearest_values(case, 'T')
        second, _ = nearest_values(case, 'T', reverse=True)
        covered &= t_covered
        for row in range(len(expected)):
            if covered[row]:
                expected[row] /= first[row] * second[row].conj()
            else:
                expected[row] = 0
    return expected


def metadata_rows(path):
    from casatools import table
    tb = table(); tb.open(str(path)); rows = tb.nrows(); tb.close()
    return rows


def verify_controls(root):
    results = json.loads((root / 'summary.json').read_text())['cases']
    assert len(results) == 12, len(results)
    unit = np.load(root / 'simplenoise-otf1-window0/data.npz')['data']
    for result in results:
        mode, otf, window = result['mode'], result['otf'], result['new_window']
        crashed = mode == 'simplenoise' and otf and window
        if crashed:
            assert result['process_exit'] == -11, result
            print('CASA confirmed signal 11:', mode, 'OTF=True new_window=True', flush=True)
            continue
        assert result['process_exit'] == 0 and result['corrupt'], result
        case_dir = root / f'{mode}-otf{int(otf)}-window{int(window)}'
        case = dict(np.load(case_dir / 'data.npz'))
        residual = nrms(case['data'], expected_data(case, unit if otf else None))
        assert residual < 1e-3, (result, residual)
        for label in ['A', 'T']:
            calibration = case_dir / f'noise.{label}.cal'
            if calibration.exists():
                assert metadata_rows(calibration / 'SPECTRAL_WINDOW') == 1
        print('CASA original A/T solutions:', mode, otf, window, 'DATA NRMS', residual, flush=True)


def verify_native(root):
    from casatools import simulator
    cases = sorted(root.glob('*/case.json'))
    assert len(cases) == 12, len(cases)
    for specification in cases:
        result = json.loads(specification.read_text())
        if result['otf']:
            continue  # OTF RNGs differ; the native test checks the unit-noise scaling.
        directory = specification.parent
        path = directory / 'data.ms'
        case = read_main(path)
        for label in ['A', 'T']:
            save_cal(directory / f'noise.{label}.cal', label, case)
        before = case['data']
        residual = nrms(before, expected_data(case))
        assert residual < 1e-3, (result, residual)
        assert metadata_rows(path / 'SPECTRAL_WINDOW') == 1 + int(result['new_window'])
        assert metadata_rows(directory / 'noise.A.cal/SPECTRAL_WINDOW') == 1
        print('C++ read native original A/T:', result, 'DATA NRMS', residual, flush=True)
        a, covered = nearest_values(case, 'A', antenna2=True)
        expected = before.copy()
        expected[covered] -= np.asarray([a[i] for i in np.flatnonzero(covered)])
        calibrations = [(directory / 'noise.A.cal', expected.copy())]
        if 'T_values' in case:
            assert metadata_rows(directory / 'noise.T.cal/SPECTRAL_WINDOW') == 1
            first, t_covered = nearest_values(case, 'T')
            second, _ = nearest_values(case, 'T', reverse=True)
            for row in np.flatnonzero(t_covered):
                expected[row] *= first[row] * second[row].conj()
            calibrations.append((directory / 'noise.T.cal', expected.copy()))
        for calibration, expected in calibrations:
            sm = simulator(); assert sm.openfromms(str(path))
            assert sm.setapply(table=str(calibration), interp='nearest')
            assert sm.corrupt(); sm.close()
            residual = nrms(read_main(path)['data'], expected)
            assert residual < 1e-3, (result, calibration, residual)
            print('CASA consumed native', calibration.name, result, 'NRMS', residual, flush=True)



def gating_control(root):
    """Noise coverage gates D before its incompatible two-hand apply."""
    from casatools import table
    from sm_multiple_models_oracle import make_session
    root.mkdir()
    for noisy in [False, True]:
        path = root / str(noisy)
        sm = make_session(path, 'XX XY YX YY')
        if noisy:
            assert sm.setnoise(OTF=False, mode='simplenoise')
        assert sm.setspwindow(spwname='later', freq='100GHz', deltafreq='15.625MHz',
                              freqresolution='15.625MHz', nchannels=4, stokes='XX YY', refcode='TOPO')
        assert sm.observe(sourcename='target', spwname='later', starttime='3563s', stoptime='3583s')
        assert sm.setleakage(amplitude=[0.01])
        try:
            assert sm.corrupt() and noisy
        except RuntimeError as error:
            assert not noisy and 'incompatible with VisVector' in str(error)
        sm.close()
        tb = table(); tb.open(str(path))
        assert all(np.all(tb.getcell('DATA', row) == 0) for row in range(144, tb.nrows()))
        tb.close()
        print('CASA whole noise/D step gate:', noisy, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--out', type=Path)
    group.add_argument('--verify-controls', type=Path)
    group.add_argument('--verify-native', type=Path)
    parser.add_argument('--mode', choices=['simplenoise', 'tsys-manual', 'tsys-atm'])
    parser.add_argument('--otf', type=int, choices=[0, 1])
    parser.add_argument('--new-window', type=int, choices=[0, 1])
    args = parser.parse_args()
    if args.verify_controls:
        verify_controls(args.verify_controls); return
    if args.verify_native:
        import casatasks
        assert casatasks.version_string() == '6.7.6.14'
        verify_native(args.verify_native); return
    if args.mode:
        worker(args.out, args.mode, bool(args.otf), bool(args.new_window)); return
    # The known reference failure must not create a multi-GB core dump.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    root = args.out.resolve(); root.mkdir(parents=True, exist_ok=True)
    specifications = [(m, o, w) for m in ['simplenoise', 'tsys-manual', 'tsys-atm']
                       for o in [0, 1] for w in [0, 1]]
    def run(spec):
        mode, otf, window = spec
        case = root / f'{mode}-otf{otf}-window{window}'; case.mkdir()
        with (case / 'casa.log').open('w') as log:
            process = subprocess.run([sys.executable, __file__, '--out', str(case), '--mode', mode,
                '--otf', str(otf), '--new-window', str(window)], cwd=case,
                stdout=log, stderr=subprocess.STDOUT)
        result = json.loads((case / 'result.json').read_text())
        result['process_exit'] = process.returncode
        return result
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, specifications))
    (root / 'summary.json').write_text(json.dumps(dict(casa_version='6.7.6.14', cases=results), indent=2))
    verify_controls(root)
    gating_control(root / "noise-d-gate")


if __name__ == '__main__':
    main()
