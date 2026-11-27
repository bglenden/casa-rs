#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Matched public-Python fluxscale timing on the same CASA-written gains.

Use CASA's Python with casars installed. Input repetition adds distinct times
without changing gains/flags. Validation, preparation and eviction are outside
the timing. Fresh Python memory/import costs are measured separately.
"""
from __future__ import annotations
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

from lifecycle import usage, block_io, evict_files


@contextlib.contextmanager
def fixed_history(vis, scratch):
    """Hold the owned benchmark MS's HISTORY input fixed between calls."""
    history = vis/'HISTORY'
    shutil.copytree(history, scratch)

    def reset():
        shutil.rmtree(history)
        shutil.copytree(scratch, history)

    try:
        yield reset
    finally:
        reset()


def repeated_gain(source, destination, repetitions):
    import numpy as np
    from casatools import table
    tool = table()
    tool.open(str(source))
    copied = tool.copy(str(destination), deep=True, valuecopy=True)
    copied.close()
    tool.close()
    tool.open(str(destination), nomodify=False)
    count = tool.nrows()
    columns = {name: tool.getcol(name) for name in tool.colnames()
               if all(tool.iscelldefined(name, row) for row in range(count))}
    tool.addrows(count*(repetitions-1))
    span = max(1., np.ptp(columns['TIME']) + 10.)
    for name, values in columns.items():
        expanded = np.concatenate([values]*repetitions, axis=-1)
        if name == 'TIME':
            expanded += np.repeat(np.arange(repetitions)*span, count)
        tool.putcol(name, expanded)
    tool.close()
    return count*repetitions


def fresh(module, arguments, root):
    code = '''import importlib,json,resource,sys,time
from pathlib import Path
start=time.perf_counter();module=importlib.import_module(sys.argv[1]);imports=time.perf_counter()-start
start=time.perf_counter();module.fluxscale(**json.loads(sys.argv[2]));wall=time.perf_counter()-start
peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if sys.platform.startswith('linux'):
 peak=next(int(s.split()[1]) for s in Path('/proc/self/status').read_text().splitlines() if s.startswith('VmHWM:'))
elif sys.platform=='darwin':peak/=1024
print(json.dumps(dict(import_seconds=imports,wall_seconds=wall,python_peak_kib=peak)))
'''
    environment = os.environ.copy()
    if module.startswith('casars'):
        environment.pop('LD_PRELOAD', None)
    process = subprocess.run([sys.executable, '-c', code, module, json.dumps(arguments)],
                             env=environment, cwd=root, text=True, capture_output=True, check=True)
    result = json.loads(process.stdout.splitlines()[-1])
    result['native_peak_rss_kib'] = max(
        (int(line.split()[1]) for line in process.stderr.splitlines()
         if line.startswith('calibration_peak_rss_kib ')), default=None)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vis', type=Path, required=True)
    parser.add_argument('--gain', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--repeat-input', type=int, default=1)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--cache', choices=['warm', 'cold'], default='warm')
    args = parser.parse_args()
    assert args.repeat_input > 0 and args.repeats > 0
    vis, original = args.vis.resolve(), args.gain.resolve()
    args.out = args.out.resolve()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    os.environ['CASA_RS_CALIBRATION_TIMING'] = '1'
    import casatasks as casa
    from casars import tasks as native
    from casars.tools import table as native_table
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "casa-compat"))
    from calibration_lifecycle_oracle import equal, metadata
    from fluxscale_oracle import compare, plain
    evidence = dict(host=platform.platform(), cache=args.cache, casa_version=casa.version_string(),
                    worker_environment={name: os.getenv(name) for name in ['RAYON_NUM_THREADS', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS']},
                    cases={}, fresh={})
    evidence['history_policy'] = 'Restore identical input HISTORY before every call, outside timing; restore original HISTORY on exit.'
    with tempfile.TemporaryDirectory(prefix='fluxscale-perf-', dir=args.out.parent) as scratch, \
            fixed_history(vis, Path(scratch)/'input-HISTORY') as reset_history:
        root = Path(scratch)
        gain = root/'input.gcal'
        rows = repeated_gain(original, gain, args.repeat_input)
        evidence['input_rows'] = rows
        evidence['input_table_bytes'] = sum(path.stat().st_size for path in gain.rglob('*') if path.is_file())
        base = dict(vis=str(vis), caltable=str(gain), reference=['FLUX'], transfer=['PHASE'])
        for name, module in [('casa', 'casatasks'), ('native', 'casars.tasks')]:
            reset_history()
            evidence['fresh'][name] = fresh(module, dict(base, fluxtable=str(root/f'fresh.{name}')), root)
        for label, extra in [('full', {}), ('incremental', dict(incremental=True))]:
            paths = {name: root/f'{label}.{name}' for name in ['casa', 'native']}
            records = {name: [] for name in paths}
            values = {}
            def call(name):
                path = paths[name]
                shutil.rmtree(path, ignore_errors=True)
                reset_history()
                if args.cache == 'cold':
                    evict_files(vis, gain)
                captured = io.StringIO()
                before_io, before_cpu = block_io(), usage()
                preload = os.environ.pop('LD_PRELOAD', None) if name == 'native' else None
                start = time.perf_counter()
                try:
                    with contextlib.redirect_stderr(captured):
                        value = (casa if name == 'casa' else native).fluxscale(**base, **extra, fluxtable=str(path))
                finally:
                    if preload is not None:
                        os.environ['LD_PRELOAD'] = preload
                wall = time.perf_counter()-start
                after_io, after_cpu = block_io(), usage()
                phases, peaks = {}, []
                for line in captured.getvalue().splitlines():
                    if line.startswith('calibration_phase '):
                        _, phase, duration = line.split()
                        phases[phase] = phases.get(phase, 0.) + float(duration)
                    elif line.startswith('calibration_peak_rss_kib '):
                        peaks.append(int(line.split()[1]))
                if name == 'native' and sys.platform.startswith('linux'):
                    assert peaks, 'native memory/phase instrumentation missing'
                values[name] = plain(value)
                return dict(wall_seconds=wall, cpu_seconds=sum(after_cpu.values())-sum(before_cpu.values()),
                            cpu_breakdown_seconds={key: after_cpu[key]-before_cpu[key] for key in before_cpu},
                            block_io={key: after_io[key]-before_io[key] for key in before_io},
                            phases_seconds=phases, native_peak_rss_kib=max(peaks, default=None), workers=1)
            # Independent numerical/table validation, outside measured repeats.
            for name in paths:
                call(name)
            compare(values['casa'], values['native'])
            equal(paths['casa'], paths['native'])
            metadata(paths['casa'], paths['native'])
            for index in range(args.repeats):
                for name in (['casa', 'native'] if index % 2 == 0 else ['native', 'casa']):
                    records[name].append(call(name))
            evidence['cases'][label] = dict(runs=records, output_table_bytes={
                name: sum(path.stat().st_size for path in product.rglob('*') if path.is_file())
                for name, product in paths.items()}, median_seconds={
                name: statistics.median(run['wall_seconds'] for run in runs) for name, runs in records.items()})
            args.out.write_text(json.dumps(evidence, indent=2)+'\n')
        # Attainable native table-reader cost for exactly the estimator columns.
        readers = []
        for _ in range(args.repeats):
            if args.cache == 'cold':
                evict_files(gain)
            before_io, before_cpu = block_io(), usage()
            start = time.perf_counter()
            tool = native_table()
            tool.open(str(gain))
            for column in ['FIELD_ID', 'SPECTRAL_WINDOW_ID', 'ANTENNA1', 'ANTENNA2', 'TIME', 'INTERVAL', 'SCAN_NUMBER', 'CPARAM', 'FLAG']:
                tool.getcol(column)
            tool.close()
            wall = time.perf_counter()-start
            after_io, after_cpu = block_io(), usage()
            readers.append(dict(wall_seconds=wall, cpu_seconds=sum(after_cpu.values())-sum(before_cpu.values()),
                                block_io={key: after_io[key]-before_io[key] for key in before_io}))
        evidence['reader_runs'] = readers
        args.out.write_text(json.dumps(evidence, indent=2)+'\n')
        # Measure MAIN decode/encode without calibration. Subtable value-copy
        # belongs to the separate table-tool backlog; remove only references
        # from this disposable input after all task measurements are complete.
        from casatools import table as casa_table
        tool = casa_table()
        tool.open(str(gain), nomodify=False)
        for name, value in tool.getkeywords().items():
            if isinstance(value, str) and value.startswith('Table: '):
                tool.removekeyword(name)
        tool.close()
        evidence['rewrite_scope'] = 'Native public table.copy valuecopy MAIN only; owning subtable references removed after all task timings'
        rewrites = []
        for index in range(args.repeats):
            destination = root/f'rewrite-{index}'
            if args.cache == 'cold':
                evict_files(gain)
            before_io, before_cpu = block_io(), usage()
            start = time.perf_counter()
            tool = native_table()
            tool.open(str(gain))
            copied = tool.copy(str(destination), deep=True, valuecopy=True)
            copied.close()
            tool.close()
            wall = time.perf_counter()-start
            after_io, after_cpu = block_io(), usage()
            rewrites.append(dict(wall_seconds=wall, cpu_seconds=sum(after_cpu.values())-sum(before_cpu.values()),
                                 block_io={key: after_io[key]-before_io[key] for key in before_io}))
            shutil.rmtree(destination)
        evidence['rewrite_runs'] = rewrites
    args.out.write_text(json.dumps(evidence, indent=2)+'\n')
    print(json.dumps({name: record['median_seconds'] for name, record in evidence['cases'].items()}))


if __name__ == '__main__':
    main()
