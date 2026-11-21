#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the direct calibration tools and a public polarization solve/apply chain."""

import argparse
from pathlib import Path
import signal
import subprocess
import sys

import casatasks as casa
import numpy as np

from applycal_oracle import copy_ms
from calibration_leakage_apply_oracle import LEAKAGE, make_table, result
from calibration_smoothcal_oracle import opened
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import plant, put_observations


def compare_workflow(root, native):
    root.mkdir()
    vis, _, gains, stokes, sky = plant(root, 'XX XY YX YY')
    with opened(vis) as table:
        times, fields = table.getcol('TIME'), table.getcol('FIELD_ID')
    leakage = root/'planted-full-leakage.cal'
    phase = root/'planted-phase.cal'
    make_table(vis, leakage, 'Dgen Jones', LEAKAGE, times.min())
    plant_cross_hand(vis, phase, 5, times.min())
    put_observations(vis, sky, [gains, leakage, phase])
    casa.delmod(str(vis), otf=True, scr=True)
    casa.setjy(str(vis), field='0', standard='manual', fluxdensity=stokes, usescratch=False)
    with opened(vis) as table:
        assert 'MODEL_DATA' not in table.colnames()
    outcomes = {}
    for writer, tasks in [('casa', casa), ('native', native)]:
        cross = root/f'{writer}-phase.cal'
        leak = root/f'{writer}-leakage.cal'
        common = dict(field='0')
        fitted = tasks.polcal(str(vis), str(cross), poltype='Xfparang+QU',
                              gaintable=[str(gains), str(leakage)], **common)
        np.testing.assert_allclose(fitted['FLUX']['SpwAve'][1:3],
                                   np.asarray(stokes[1:3])/stokes[0], atol=8e-5, rtol=0.)
        tasks.polcal(str(vis), str(leak), poltype='Dlls', refant='', minsnr=0.,
                     gaintable=[str(gains), str(cross)], **common)
        for reader, apply in [('casa', casa.applycal), ('native', native.applycal)]:
            target = root/f'{writer}-writer-{reader}-reader.ms'
            copy_ms(vis, target)
            apply(str(target), field='0', gaintable=[str(p) for p in (cross, leak, gains)],
                  parang=True, calwt=True, flagbackup=False)
            outcomes[writer, reader] = result(target)
    reference, reference_flags, reference_weights, reference_spectrum = outcomes['casa', 'casa']
    expected = sky[:, :, fields == 0]
    for label, (corrected, flags, weights, spectrum) in outcomes.items():
        np.testing.assert_array_equal(flags, reference_flags, err_msg=str(label))
        np.testing.assert_allclose(weights, reference_weights, atol=1e-7, rtol=2e-6)
        if reference_spectrum is None:
            assert spectrum is None
        else:
            np.testing.assert_allclose(spectrum, reference_spectrum, atol=1e-7, rtol=2e-6)
        wanted, actual = reference[:, :, fields == 0], corrected[:, :, fields == 0]
        assert np.linalg.norm(actual-wanted)/np.linalg.norm(wanted) < 1e-3
        error = np.linalg.norm(actual-expected)/np.linalg.norm(expected)
        print(label, 'corrected-data normalized RMS versus planted sky', error)
        assert error < 1e-3


def compare_tools(root):
    from casars.tools import calibrater
    root.mkdir()
    vis, leakage, gains, _, _ = plant(root, 'RR RL LR LL')
    from casatools import calibrater as reference
    vis = str(vis)
    for tool in (reference(), calibrater()):
        assert tool.posangcal([12.], str(gains)) is False
        assert tool.linpolcor() is False

    for legacy in (False, True):
        for table in (gains, leakage):
            output = root/f'posang-{legacy}-{table.stem}.cal'
            script = '''
from casatools import calibrater
import resource
import sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
tool = calibrater()
if sys.argv[4] == 'True':
    assert tool.setvi(old=True, quiet=True)
assert tool.open(sys.argv[1], addcorr=False, addmodel=False)
tool.posangcal([12.], sys.argv[2], sys.argv[3])
'''
            child = subprocess.run([sys.executable, '-c', script, vis, str(table), str(output),
                                    str(legacy)], text=True, capture_output=True)
            assert child.returncode == -signal.SIGABRT, child.stderr
            tool = calibrater()
            if legacy:
                assert tool.setvi(old=True, quiet=True)
            assert tool.open(vis, addcorr=False, addmodel=False)
            try:
                tool.posangcal(posangcor=[12.], tablein=str(table), tableout=str(output))
            except RuntimeError as error:
                assert 'posangcal' in str(error)
            else:
                raise AssertionError('native returned without a catchable position angle error')
            assert not output.exists()
            with opened(table) as calibration:
                before = calibration.getcol('CPARAM').copy()
            try:
                tool.posangcal([12.], str(table))
            except RuntimeError:
                pass
            else:
                raise AssertionError('native accepted the in-place position angle correction')
            with opened(table) as calibration:
                np.testing.assert_array_equal(calibration.getcol('CPARAM'), before)
            assert tool.close()
            assert tool.posangcal([12.], str(table)) is False

        for constructor in (reference, calibrater):
            output = root/'linpol-output.cal'
            tool = constructor()
            if legacy:
                assert tool.setvi(old=True, quiet=True)
            assert tool.open(vis, addcorr=False, addmodel=False)
            assert tool.linpolcor(str(gains), str(output), fields=['FLUX']) is False
            assert not output.exists()
            assert tool.close()
            assert tool.linpolcor() is False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    root = args.work.resolve()
    root.mkdir()
    compare_workflow(root/'workflow', tasks)
    compare_tools(root/'tools')


if __name__ == '__main__':
    main()
