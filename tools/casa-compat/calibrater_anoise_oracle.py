#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check calibrater application of saved simulator A Noise tables in CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater, simulator as casa_simulator
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import same_record
from sm_multiple_models_oracle import make_session
from sm_offline_noise_oracle import parameters_for_rows


LAYOUTS = {'one': ('XX', [0]), 'two': ('XX YY', [0, 3]),
           'four': ('XX XY YX YY', [0, 1, 2, 3])}


def setup(work, stokes):
    vis = work/'source.ms'
    sim = make_session(vis, stokes)
    assert sim.close()
    with opened(vis, True) as ms:
        shape = ms.getcol('DATA').shape
        assert ms.putcol('DATA', np.full(shape, 4.+4.j, dtype=np.complex64))
        assert ms.putcol('MODEL_DATA', np.full(shape, 7.-2.j, dtype=np.complex64))
        flags = np.zeros(shape, dtype=bool)
        flags[0, 1, 0] = True
        assert ms.putcol('FLAG', flags)
        assert ms.putcol('FLAG_ROW', np.zeros(shape[-1], dtype=bool))
    return vis


def write_noise(work, vis, factory, label):
    sim = factory()
    assert sim.openfromms(str(vis)) and sim.setseed(19)
    assert sim.setnoise(mode='simplenoise', simplenoise='0.25Jy', OTF=False,
                        table=str(work/f'{label}.cal'))
    assert sim.close()
    noise = work/f'{label}.A.cal'
    with opened(noise) as cal:
        assert cal.getkeyword('VisCal') == 'A Noise'
        assert cal.getcell('CPARAM', 0).shape == (4, 4)
    return noise


def modified_tables(work, noise):
    result = {}
    for case in ('missing', 'all-flagged', 'channel-flagged'):
        path = work/f'{case}.cal'
        copytree(noise, path)
        with opened(path, True) as cal:
            mask = (cal.getcol('ANTENNA1') == 0) & (cal.getcol('ANTENNA2') == 0)
            if case == 'missing':
                assert cal.removerows(np.flatnonzero(mask).tolist())
            else:
                flags = cal.getcol('FLAG')
                if case == 'all-flagged':
                    flags[:, :, mask] = True
                else:
                    flags[2, 0, 0] = True
                assert cal.putcol('FLAG', flags)
        result[case] = path
    return result


def run_application(dataset, source, noise, factory, legacy, action, case):
    copytree(source, dataset)
    if case == 'off-grid':
        with opened(dataset/'SPECTRAL_WINDOW', True) as window:
            frequency = window.getcell('CHAN_FREQ', 0)
            assert window.putcell('CHAN_FREQ', 0, frequency + .5*(frequency[1]-frequency[0]))
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(dataset), addcorr=True, addmodel=True)
    assert tool.selectvis()
    explicit = case == 'nearest'
    assert tool.setapply(type='N' if explicit else '', table=str(noise), calwt=True,
                         interp='nearest,nearest' if explicit else '')
    state = tool.state()
    assert getattr(tool, action)()
    activity = tool.activityrec()
    assert tool.close()
    with opened(dataset) as ms:
        column = 'CORRECTED_DATA' if action == 'correct' else 'MODEL_DATA'
        arrays = {key: ms.getcol(key) for key in (column, 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA')}
    return state, activity, arrays


def check_cases(work, source, noise, engines, slots, cases):
    with opened(source) as tab:
        inputs = {key: tab.getcol(key) for key in ('DATA', 'MODEL_DATA', 'FLAG', 'WEIGHT', 'SIGMA')}
        missing_rows = (tab.getcol('ANTENNA1') == 0) & (tab.getcol('ANTENNA2') == 0)
    for (case, prior), legacy, action in product(cases.items(), (True, False), ('correct', 'corrupt')):
        observed = {}
        for label, factory in engines.items():
            path = work/f'{noise.stem}-{case}-{legacy}-{action}-{label}.ms'
            state, activity, arrays = observed[label] = run_application(
                path, source, prior, factory, legacy, action, case)
            for name in ('WEIGHT', 'SIGMA'):
                np.testing.assert_array_equal(arrays[name], inputs[name])
            if case in ('normal', 'nearest'):
                offset = parameters_for_rows(source, noise)[:, slots, :].transpose(1, 2, 0)
                key, raw, sign = ('CORRECTED_DATA', 'DATA', -1) if action == 'correct' else (
                    'MODEL_DATA', 'MODEL_DATA', 1)
                np.testing.assert_allclose(arrays[key], inputs[raw] + sign*offset, atol=1e-6, rtol=0.)
            flags = inputs['FLAG'].copy()
            if action == 'correct' and case in ('missing', 'all-flagged'):
                flags[:, :, missing_rows] = True
            np.testing.assert_array_equal(arrays['FLAG'], flags)
        if 'native' in observed:
            native_state, native_activity, native = observed['native']
            casa_state, casa_activity, casa = observed['casa']
            same_record(native_state, casa_state)
            same_record(native_activity, casa_activity)
            for key, values in native.items():
                np.testing.assert_allclose(values, casa[key], atol=1e-6, rtol=0., err_msg=key)
        print(work.name, noise.name, case, legacy, action, flush=True)


def compare(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    engines, writers = {'casa': casa_calibrater}, {'casa': casa_simulator}
    if not reference_only:
        from casars.tools import calibrater, simulator

        engines['native'], writers['native'] = calibrater, simulator
    work.mkdir()
    with chdir(work):
        for layout, (stokes, slots) in LAYOUTS.items():
            directory = work/layout
            directory.mkdir()
            source = setup(directory, stokes)
            for label, writer in writers.items():
                if label == 'native' and layout != 'four':
                    continue
                noise = write_noise(directory, source, writer, label)
                cases = {'normal': noise}
                if label == 'casa' and layout == 'four':
                    cases.update({'nearest': noise, 'off-grid': noise}, **modified_tables(directory, noise))
                check_cases(directory, source, noise, engines, slots, cases)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    compare(args.work, args.reference_only)
