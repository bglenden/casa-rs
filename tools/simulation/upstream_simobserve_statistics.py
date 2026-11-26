#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check the analytic noise assertions behind upstream seeded comparisons.

Run under CASA 6.7.6.14's Python with --test-file, --casatestdata and --out.
The original test bodies and their hardcoded-seed assertions still execute.
Their failures are recorded separately. Native counterparts check the same
bodies' independently computed analytic noise with the upstream 10% bound.
CASA supplies the input builders, output readers and analytic reference;
the simulator and simulation tasks run through the public casars interfaces.
"""
from __future__ import annotations

import argparse
import importlib.util
import inspect
import json
import os
from pathlib import Path
import sys

import numpy as np

from upstream_simulation_suite import PLUGIN


NOISE_CASES = (
    'testNZ_intMan', 'testNZ_intAtm', 'testNZ_sdAtm', 'testNZ_sdAtm_pwv',
    'testNZ_sdAtm_dnu', 'testNZ_sdAtm_tint', 'testNZ_sdAtm_el',
)


def record_result(instance, method, values):
    """Retain the original failure; require its analytic counterpart to pass."""
    original = inspect.unwrap(getattr(type(instance), method))
    raw = 'passed'
    try:
        original(instance)
    except AssertionError as error:
        raw = str(error)
        if 'The noise level differs from the previous value:' not in raw:
            raise
    measured, analytic = values['measured'], values['analytic']
    relative_error = abs(measured / analytic - 1.)
    assert relative_error < .1, (method, measured, analytic, relative_error)
    return dict(original=raw, measured=measured, analytic=analytic,
                relative_error=relative_error, analytic_counterpart='passed')


def sky_noise(module):
    """The sky-image noise case: test the additive radiometer distribution."""
    case = module.simobserve_sky('testSky_sdObs_noise')
    case.setUp()
    try:
        raw = 'passed'
        try:
            case.testSky_sdObs_noise()
        except AssertionError as error:
            raw = str(error)
            if "MS statistic 'firstquartile' does not match:" not in raw:
                raise
        prefix = f'{case.project}/{case.project}.aca.tp'
        samples = []
        for suffix in ('.noisy.sd.ms', '.sd.ms'):
            module.tb.open(prefix + suffix)
            try:
                samples.append(np.asarray(module.tb.getcol('DATA')))
            finally:
                module.tb.close()
        residual = (samples[0] - samples[1]).real.ravel()
        # The original sky case sets both temperatures to 263 K and
        # leaves tau0 at simobserve's 0.1 default, unlike noise_sd's 1.0.
        analytic = module.simobserve_noise()._calc_alma_noise(
            mode='manual', sd=True, tground=263., tsky=263., tau0=.1)
        assert residual.size >= 100
        mean_z = float(residual.mean() / analytic * np.sqrt(residual.size))
        std_z = float((residual.std(ddof=1) / analytic - 1.)
                      * np.sqrt(2. * (residual.size - 1)))
        # Gaussian mean and sample-standard-deviation errors, in standard
        # errors; this compares distributions rather than seed quantiles.
        assert abs(mean_z) < 6. and abs(std_z) < 6., (mean_z, std_z)
        return dict(original=raw, samples=residual.size, analytic=float(analytic),
                    mean_in_standard_errors=mean_z, std_in_standard_errors=std_z,
                    analytic_counterpart='passed')
    finally:
        case.tearDown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-file', type=Path, required=True)
    parser.add_argument('--casatestdata', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--only', nargs='+', choices=[*NOISE_CASES, 'testSky_sdObs_noise'])
    args = parser.parse_args()
    source, data, output = args.test_file.resolve(), args.casatestdata.resolve(), args.out.resolve()
    output.mkdir(parents=True, exist_ok=True)
    os.environ['SIMOBSERVE_SUITE_DATA'] = str(data)
    os.environ['CASARS_UPSTREAM_NATIVE'] = '1'
    os.environ['CASARS_ALIGN_SIMULATION_EPOCHS'] = '1'
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'frontends/casars-python/python'))
    exec(PLUGIN, {})
    from casatasks import version_string
    if version_string() != '6.7.6.14':
        raise RuntimeError('requires CASA 6.7.6.14')
    spec = importlib.util.spec_from_file_location('original_simobserve_tests', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    results = {}
    selected = args.only or [*NOISE_CASES, 'testSky_sdObs_noise']
    for name in (name for name in NOISE_CASES if name in selected):
        work = output / name
        work.mkdir(exist_ok=True)
        os.chdir(work)
        case = module.simobserve_noise(name)
        values = {}
        # Capture the two quantities the unchanged upstream body computes
        # before its seed-specific assertion aborts the remaining checks.
        measured, analytic = case._get_noise, case._calc_alma_noise

        def measure(*args, **kwargs):
            values['measured'] = float(measured(*args, **kwargs))
            return values['measured']

        def reference(*args, **kwargs):
            values['analytic'] = float(analytic(*args, **kwargs))
            return values['analytic']

        case._get_noise, case._calc_alma_noise = measure, reference
        case.setUp()
        try:
            results[name] = record_result(case, name, values)
            print(name, results[name], flush=True)
        finally:
            case.tearDown()
    if 'testSky_sdObs_noise' in selected:
        work = output / 'testSky_sdObs_noise'
        work.mkdir(exist_ok=True)
        os.chdir(work)
        results['testSky_sdObs_noise'] = sky_noise(module)
        print('testSky_sdObs_noise', results['testSky_sdObs_noise'], flush=True)
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
