#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check direct calibrater selectors and a complete two-object lifecycle."""

import argparse
from contextlib import chdir
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from shutil import copytree
import signal
from subprocess import run
import sys
from threading import Event, Thread
from time import monotonic, sleep

import casatasks
from casatasks.private.callibrary import callibrary
from casatools import calibrater as casa_calibrater, table
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from gaincal_delay_oracle import duplicate_rows
from record_casa_returns import make_cal_ms


def fill(work):
    source = work/'source.ms'
    make_cal_ms(str(source), channels=8)
    prep = casa_calibrater()
    assert prep.open(str(source), addcorr=True, addmodel=True) and prep.close()
    observation = duplicate_rows(table(), source/'OBSERVATION')
    state = duplicate_rows(table(), source/'STATE')
    with opened(source/'STATE', True) as states:
        assert states.putcell('OBS_MODE', state, 'CALIBRATE_PHASE#ON_SOURCE')
    with opened(source, True) as ms:
        data = ms.getcol('DATA')
        assert ms.putcol('DATA', np.full_like(data, 1.+.25j))
        assert ms.putcol('CORRECTED_DATA', np.full_like(data, -3.-2.j))
        assert ms.putcol('MODEL_DATA', np.ones_like(data))
        assert ms.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        cols = ('FIELD_ID', 'SCAN_NUMBER', 'ANTENNA1', 'ANTENNA2', 'TIME', 'UVW')
        field, scan, first, second, time, uvw = (ms.getcol(col) for col in cols)
        assert ms.putcol('FLAG_ROW', np.zeros(len(first), dtype=bool))
        assert ms.putcol('OBSERVATION_ID', np.where(field == 0, 0, observation).astype(np.int32))
        assert ms.putcol('STATE_ID', np.where(field == 0, 0, state).astype(np.int32))
    gain = work/'known.cal'
    casatasks.gencal(str(source), str(gain), 'amp', parameter=[.5])
    scan_id = int(np.unique(scan)[1])
    midpoint = float(np.mean(np.unique(time)[1:3]))
    limit = datetime(1858, 11, 17) + timedelta(seconds=midpoint)
    uvd = np.hypot(uvw[0], uvw[1])
    uvmax = round(float((min(uvd)+max(uvd))*.5), 5)
    baseline = ((first == 1) & (second == 3)) | ((first == 3) & (second == 1))
    cases = [
        ('baseline', 'A1&A3', baseline),
        ('scan', [scan_id], scan == scan_id),
        ('observation', [observation], field != 0),
        ('intent', '*CALIBRATE_PHASE*', field != 0),
        ('time', '<'+limit.strftime('%Y/%m/%d/%H:%M:%S.%f'), time < midpoint),
        ('uvrange', f'0~{uvmax}m', uvd < uvmax),
    ]
    return source, gain, cases


def check_selection(work, source, prior, engines, cases):
    for (key, value, rows), old in product(cases, (False, True)):
        assert 0 < rows.sum() < rows.size
        products = {}
        for label, factory in engines.items():
            path = work/f'{key}-{old}-{label}.ms'
            copytree(source, path)
            tool = factory()
            assert tool.setvi(old, True) and tool.open(str(path), addcorr=True, addmodel=True)
            assert tool.selectvis(**{key: value})
            selected = tool.returndict()['selectvis']
            assert tool.setapply(table=str(prior), calwt=False) and tool.correct('calonly')
            activity = tool.activityrec()
            assert tool.close()
            with opened(path) as ms:
                corrected = ms.getcol('CORRECTED_DATA')
            expected = np.where(rows[None, None, :], 4.+1.j, -3.-2.j)
            np.testing.assert_allclose(corrected, np.broadcast_to(expected, corrected.shape),
                                       atol=1e-6, rtol=0.)
            products[label] = dict(selected=selected, activity=activity, corrected=corrected)
        if 'native' in products:
            same_record(products['native'], products['casa'])
        print(key, old, 'rows', rows.sum(), flush=True)


def session(work, source, prior, factory, old, pinned):
    outputs = []
    for index, flux in enumerate((1., 2.)):
        path = work/f'object-{index}.ms'
        copytree(source, path)
        with opened(path, True) as ms:
            data = ms.getcol('DATA')
            jitter = .001*np.sin(np.arange(data.size).reshape(data.shape))
            assert ms.putcol('DATA', (4.+jitter).astype(data.dtype))
        sol = work/f'object-{index}.cal'
        tool = factory()
        assert tool.setvi(old, True) and tool.open(str(path), addcorr=True, addmodel=True)
        assert tool.selectvis(field='FLUX') and tool.setptmodel([flux, 0., 0., 0.])
        assert tool.setsolve('G', 'inf', str(sol), refant='A0', apmode='A', minblperant=2, minsnr=0.)
        outputs.append((tool, path, sol, flux))
    if old and pinned:
        print('both legacy objects configured', flush=True)
    products = []
    for tool, path, sol, flux in reversed(outputs):
        assert tool.solve()
        report = tool.returndict()
        assert list(report.pop('solve_tables')) == [str(sol)]
        with opened(sol) as solution:
            np.testing.assert_allclose(solution.getcol('CPARAM'), np.sqrt(4./flux), atol=1e-3, rtol=0.)
        assert tool.reset(False, True)
        assert tool.setapply(table=str(sol), calwt=False)
        assert tool.correct('calonly')
        corrected_activity = tool.activityrec()
        assert tool.corrupt()
        assert tool.close()
        with opened(path) as ms:
            field = ms.getcol('FIELD_ID') == 0
            corrected = ms.getcol('CORRECTED_DATA')[:, :, field]
            model = ms.getcol('MODEL_DATA')[:, :, field]
        np.testing.assert_allclose(corrected, flux, atol=2e-3, rtol=0.)
        # CASA can retain a correction matrix for corruption when the input
        # resolves to the same calibration slot. Native uses the forward gain.
        np.testing.assert_allclose(model, flux/4. if pinned else 4./flux, atol=2e-3, rtol=0.)
        assert tool.open(str(path), addcorr=False, addmodel=False)
        assert tool.selectvis(field='FLUX')
        assert result(tool.correct) == (False, ('RuntimeError', 'Error in Calibrater::correct.'))
        library = callibrary()
        library.add(caltable=str(prior), field='0', calwt=False)
        assert tool.setcallib(library.cld) and tool.correct('calonly') and tool.done()
        with opened(path) as ms:
            np.testing.assert_allclose(ms.getcol('CORRECTED_DATA')[:, :, field], 16., atol=5e-3)
        corrected_activity['VisEquation']['*1']['table'] = 'object.cal'
        products.append((report, corrected_activity))
    return products


def check_gil(work, factory):
    source = work/'gil.ms'
    make_cal_ms(str(source), scan_seconds=3600, integration_seconds=5)
    prior = work/'gil.cal'
    casatasks.gencal(str(source), str(prior), 'amp', parameter=[.5])
    tool = factory()
    assert tool.open(str(source), addcorr=True, addmodel=False)
    assert tool.selectvis() and tool.setapply(table=str(prior), calwt=False)
    started, finished = Event(), Event()
    samples = []

    def record_times():
        started.set()
        while not finished.is_set():
            samples.append(monotonic())
            sleep(.002)

    ticker = Thread(target=record_times)
    ticker.start()
    started.wait()
    original = sys.getswitchinterval()
    try:
        sys.setswitchinterval(1.)
        begin = monotonic()
        assert tool.correct('calonly')
        end = monotonic()
    finally:
        sys.setswitchinterval(original)
        finished.set()
        ticker.join()
    assert tool.close()
    inside = [stamp for stamp in samples if begin + .002 < stamp < end - .002]
    assert len(inside) >= 3, (len(inside), end-begin)
    print('Python thread ran during native correction', len(inside), 'observations', flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    engines = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater

        engines['native'] = calibrater
    work.mkdir()
    with chdir(work):
        source, gain, cases = fill(work)
        check_selection(work, source, gain, engines, cases)
        records = {}
        for label, factory in engines.items():
            directory = work/f'session-False-{label}'
            directory.mkdir()
            records[label] = session(directory, source, gain, factory, False, label == 'casa')
        if 'native' in records:
            for native, casa in zip(records['native'], records['casa'], strict=True):
                for actual, expected in zip(native, casa, strict=True):
                    same_record(actual, expected)
        print('two-object lifecycle VI2', flush=True)

        pinned = run([sys.executable, str(Path(__file__).resolve()), str(work/'session-True-casa'),
                      '--legacy-worker'], capture_output=True, text=True, timeout=35, check=False)
        assert pinned.returncode == -signal.SIGSEGV, pinned
        assert 'both legacy objects configured' in pinned.stdout, pinned
        assert not list((work/'session-True-casa').glob('*.cal'))
        print('isolated legacy reference stopped during G fit', flush=True)
        if not reference_only:
            directory = work/'session-True-native'
            directory.mkdir()
            session(directory, source, gain, engines['native'], True, False)
            print('two-object lifecycle VI1', flush=True)
            check_gil(work, engines['native'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--legacy-worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.legacy_worker:
        args.work.mkdir()
        session(args.work, args.work.parent/'source.ms', args.work.parent/'known.cal',
                casa_calibrater, True, True)
    else:
        compare(args.work, args.reference_only)
