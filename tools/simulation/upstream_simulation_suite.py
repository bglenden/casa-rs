#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run original CASA 6.7.6.14 simulation Python cases on both implementations.

CASA supplies input builders and independent output readers. The native run
replaces the simulator tool and simulation tasks with their public casars
Python surfaces before importing the unchanged upstream test module.
Each case runs in its own process, preserving the original assertions.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import tempfile

from simobserve_casa_suite import run_suite


PLUGIN = r'''
import os
from casatools import ctsys
ctsys.setpath([os.environ['SIMOBSERVE_SUITE_DATA']] + ctsys.getpath())

if os.environ.get('CASARS_UPSTREAM_NATIVE'):
    import casatools
    import casatasks
    from casars.tools import simulator
    from casars import tasks
    casatools.simulator = simulator
    for name in ('simobserve', 'simanalyze', 'simalma'):
        setattr(casatasks, name, getattr(tasks, name))
    if os.environ.get('CASARS_ALIGN_SIMULATION_EPOCHS'):
        me, qa = casatools.measures(), casatools.quanta()
        def aligned(task):
            def run(**kwargs):
                # Existing simulation control: CASA stores TAI under a UTC
                # label. Request the same physical sky from the native task.
                values = dict(kwargs)
                try:
                    hour = values.get('hourangle', 'transit')
                    if hour in ('', 'transit'):
                        seconds = 0.
                    elif isinstance(hour, str) and qa.isquantity(hour) and qa.compare(hour, '1s'):
                        seconds = qa.convert(hour, 's')['value']
                    else:
                        return task(**values)
                    day = '/'.join(values.get('refdate', '2014/01/01').split('/')[:3])
                    utc = me.epoch('utc', day+'/00:00:00')
                    tai = me.measure(utc, 'tai')
                    offset = (tai['m0']['value']-utc['m0']['value'])*86400.
                except (RuntimeError, TypeError, ValueError, KeyError, AttributeError):
                    # Invalid inputs must reach the native parameter checker.
                    return task(**values)
                values['hourangle'] = f'{seconds+offset!r}s'
                return task(**values)
            return run
        casatasks.simobserve = aligned(tasks.simobserve)
        casatasks.simalma = aligned(tasks.simalma)

def pytest_collection_modifyitems(items):
    if os.environ.get('CASARS_EXERCISE_UPSTREAM_SKIPS'):
        import inspect
        for item in items:
            bound = item.obj
            original = getattr(bound, '__func__', bound)
            if getattr(original, '__unittest_skip__', False):
                method = inspect.unwrap(original)
                original.__unittest_skip__ = False
                method.__unittest_skip__ = False
                setattr(item.cls, item.name, method)
                item.obj = method.__get__(bound.__self__, item.cls)
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-file', type=Path, required=True)
    parser.add_argument('--casatestdata', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--only', nargs='*')
    parser.add_argument('--implementation', choices=['both', 'casa', 'casa-rs'], default='both')
    parser.add_argument('--align-epochs', action='store_true',
                        help="use the documented TAI/UTC physical-epoch control for native task calls")
    parser.add_argument('--exercise-skipped', action='store_true',
                        help='also execute upstream-disabled bodies, keeping their original assertions')
    args = parser.parse_args()
    from casatasks import version_string
    if version_string() != '6.7.6.14':
        raise RuntimeError('the upstream simulation cases require CASA 6.7.6.14')
    root = Path(__file__).resolve().parents[2]
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=True)
    runs = {}
    with tempfile.TemporaryDirectory() as directory:
        scratch = Path(directory)
        (scratch / 'simobserve_suite_plugin.py').write_text(PLUGIN)
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join([
            str(scratch), str(root / 'frontends/casars-python/python'),
            env.get('PYTHONPATH', ''),
        ])
        env['SIMOBSERVE_SUITE_DATA'] = str(args.casatestdata.resolve())
        env.setdefault('MPLCONFIGDIR', str(output / 'matplotlib'))
        env.setdefault('XDG_CACHE_HOME', str(output / 'cache'))
        env.setdefault('MPLBACKEND', 'Agg')
        env.pop('SIMOBSERVE_SUITE_BINARY', None)
        if args.align_epochs:
            env['CASARS_ALIGN_SIMULATION_EPOCHS'] = '1'
        else:
            env.pop('CASARS_ALIGN_SIMULATION_EPOCHS', None)
        if args.exercise_skipped:
            env['CASARS_EXERCISE_UPSTREAM_SKIPS'] = '1'
        else:
            env.pop('CASARS_EXERCISE_UPSTREAM_SKIPS', None)
        implementations = ('casa', 'casa-rs') if args.implementation == 'both' else (args.implementation,)
        for implementation in implementations:
            env.pop('CASARS_UPSTREAM_NATIVE', None)
            if implementation == 'casa-rs':
                env['CASARS_UPSTREAM_NATIVE'] = '1'
            work = output / implementation
            work.mkdir(exist_ok=True)
            runs[implementation] = run_suite(args.test_file.resolve(), env, work, args.only)
    summary = {
        implementation: dict(Counter(outcome.split(':', 1)[0] for outcome in results.values()))
        for implementation, results in runs.items()
    }
    report = dict(casa_version=version_string(), source=args.test_file.name,
                  exercise_skipped=args.exercise_skipped,
                  align_epochs=args.align_epochs,
                  summary=summary, cases=runs)
    (output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(summary), flush=True)
    accepted = ('passed',) if args.exercise_skipped else ('passed', 'skipped')
    return int(any(outcome.split(':', 1)[0] not in accepted
                   for cases in runs.values() for outcome in cases.values()))


if __name__ == '__main__':
    raise SystemExit(main())
