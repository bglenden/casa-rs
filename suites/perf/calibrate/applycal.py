#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Matched public-Python applycal timing; stage inputs before running.

Every call starts from an identical copy prepared outside timing. Scientific
MAIN products are validated against CASA; API/HISTORY acceptance is separate.
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

from lifecycle import usage, block_io, evict_files, cold_imports


def fresh(module, arguments, root):
    code = '''import importlib,json,resource,sys,time
from pathlib import Path
start=time.perf_counter();module=importlib.import_module(sys.argv[1]);imports=time.perf_counter()-start
start=time.perf_counter();module.applycal(**json.loads(sys.argv[2]));wall=time.perf_counter()-start
scale=1024 if sys.platform=='darwin' else 1
peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/scale
if sys.platform.startswith('linux'):
 peak=next(int(s.split()[1]) for s in Path('/proc/self/status').read_text().splitlines() if s.startswith('VmHWM:'))
print(json.dumps(dict(import_seconds=imports,wall_seconds=wall,python_peak_kib=peak,child_peak_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss/scale)))
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
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--cache', choices=['warm', 'cold'], default='warm')
    args = parser.parse_args()
    if args.cache == 'cold' and not hasattr(os, 'posix_fadvise'):
        parser.error('cold file-cache measurements require posix_fadvise')
    vis, gain, args.out = args.vis.resolve(), args.gain.resolve(), args.out.resolve()
    assert args.repeats > 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    os.environ['CASA_RS_CALIBRATION_TIMING'] = '1'
    os.environ['CASA_RS_CALIBRATION_PERF'] = '1'
    import casatasks as casa
    from casatools import table
    from casars import tasks as native
    from casars.tools import table as native_table
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "casa-compat"))
    from calibration_lifecycle_oracle import contents, equal
    assert casa.version_string() == '6.7.6.14'
    tool = table()
    tool.open(str(vis))
    rows, samples = tool.nrows(), tool.getcell('DATA', 0).size*tool.nrows()
    tool.close()
    result = dict(casa_version=casa.version_string(), host=platform.platform(), cache=args.cache,
                  input_rows=rows, input_complex_samples=samples, input_data_bytes=samples*8,
                  input_ms_bytes=sum(path.stat().st_size for path in vis.rglob('*') if path.is_file()),
                  worker_environment={name: os.getenv(name) for name in ['RAYON_NUM_THREADS', 'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS']},
                  validation_scope='Every defined MAIN column and absolute times; HISTORY/API state is a separate oracle gate',
                  runs={}, fresh={})
    with tempfile.TemporaryDirectory(prefix='applycal-perf-', dir=args.out.parent) as scratch:
        root = Path(scratch)
        profiles = root/'profiles'
        profiles.mkdir()
        os.environ['CASA_RS_CALIBRATION_PERF_DIR'] = str(profiles)
        base = dict(gaintable=[str(gain)], flagbackup=False)
        def prepare(path):
            shutil.rmtree(path, ignore_errors=True)
            tool.open(str(vis))
            copied = tool.copy(str(path), deep=True, valuecopy=True)
            copied.close()
            tool.close()
        for name, module in [('casa', 'casatasks'), ('native', 'casars.tasks')]:
            path = root/f'fresh.{name}.ms'
            prepare(path)
            result['fresh'][name] = fresh(module, dict(base, vis=str(path)), root)
            shutil.rmtree(path)
        paths = {name: root/f'{name}.ms' for name in ['casa', 'native']}
        def call(name):
            path = paths[name]
            prepare(path)
            if args.cache == 'cold':
                evict_files(path, gain)
            before_profiles = set(profiles.glob('*.jsonl'))
            before_io, before_cpu = block_io(), usage()
            preload = os.environ.pop('LD_PRELOAD', None) if name == 'native' else None
            captured = io.StringIO()
            start = time.perf_counter()
            try:
                with contextlib.redirect_stderr(captured):
                    value = (casa if name == 'casa' else native).applycal(**base, vis=str(path))
            finally:
                if preload is not None:
                    os.environ['LD_PRELOAD'] = preload
            wall = time.perf_counter()-start
            after_io, after_cpu = block_io(), usage()
            assert value is None
            record = dict(wall_seconds=wall, cpu_seconds=sum(after_cpu.values())-sum(before_cpu.values()),
                          user_seconds=after_cpu['user']-before_cpu['user'],
                          system_seconds=after_cpu['system']-before_cpu['system'],
                          block_io={key: after_io[key]-before_io[key] for key in before_io},
                          native_peak_rss_kib=max((int(line.split()[1]) for line in captured.getvalue().splitlines()
                                                  if line.startswith('calibration_peak_rss_kib ')), default=None))
            if name == 'native':
                if sys.platform.startswith('linux'):
                    assert record['native_peak_rss_kib'], 'native memory instrumentation missing'
                traces = [json.loads(line) for file in set(profiles.glob('*.jsonl'))-before_profiles
                          for line in file.read_text().splitlines()]
                completed = [event for event in traces if event['kind'] == 'apply_completed']
                assert len(completed) == 1, traces
                record['native_trace'] = completed[0]
            return record
        for name in paths:
            call(name)
        a, b = contents(paths['casa']), contents(paths['native'])
        assert a.keys() == b.keys(), (a.keys(), b.keys())
        equal(paths['casa'], paths['native'])
        for column in ['TIME', 'INTERVAL', 'TIME_CENTROID']:
            import numpy as np
            np.testing.assert_allclose(a[column], b[column], rtol=0, atol=1e-4)
        records = {name: [] for name in paths}
        for index in range(args.repeats):
            for name in (['casa', 'native'] if index % 2 == 0 else ['native', 'casa']):
                records[name].append(call(name))
        result['runs'] = records
        result['median_seconds'] = {name: statistics.median(row['wall_seconds'] for row in runs)
                                    for name, runs in records.items()}
        args.out.write_text(json.dumps(result, indent=2)+'\n')
        readers = []
        for _ in range(args.repeats):
            if args.cache == 'cold':
                evict_files(vis)
            before_io, before_cpu = block_io(), usage()
            start = time.perf_counter()
            reader = native_table()
            reader.open(str(vis))
            for column in ['DATA', 'FLAG', 'SIGMA']:
                reader.getcol(column)
            reader.close()
            wall = time.perf_counter()-start
            after_io, after_cpu = block_io(), usage()
            readers.append(dict(wall_seconds=wall, cpu_seconds=sum(after_cpu.values())-sum(before_cpu.values()),
                                block_io={key: after_io[key]-before_io[key] for key in before_io}))
        result['reader_runs'] = readers
        result['cold_imports'] = cold_imports()
    args.out.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result['median_seconds']))


if __name__ == '__main__':
    main()
