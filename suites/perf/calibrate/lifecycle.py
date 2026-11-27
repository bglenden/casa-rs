#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Alternate public Python lifecycle workflows on identical, validated inputs.

Use CASA's Python with casars installed, after building release calibrate.
Import/startup is measured separately; repeated calls retain each package's
actual public task-launch cost. The host must be idle during timing.
"""
from __future__ import annotations
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time


def usage():
    own, child = (resource.getrusage(kind) for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])
    return dict(user=own.ru_utime+child.ru_utime,system=own.ru_stime+child.ru_stime)


def block_io():
    own, child = (resource.getrusage(kind) for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])
    return dict(read_bytes=(own.ru_inblock+child.ru_inblock)*512,
                write_bytes=(own.ru_oublock+child.ru_oublock)*512)


def evict_files(*roots):
    """Evict only the owned benchmark inputs/products, never global caches."""
    for root in roots:
        for path in Path(root).rglob('*'):
            if path.is_file():
                descriptor = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                    os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
                finally:
                    os.close(descriptor)


def cold_imports():
    """Fresh interpreter imports, separately from warm workflow timing."""
    result = {}
    for engine,module in [('casa','casatasks'),('native','casars.tasks')]:
        environment = os.environ.copy()
        if engine == 'native': environment.pop('LD_PRELOAD',None)
        code = "import time,importlib; t=time.perf_counter(); importlib.import_module("+repr(module)+"); print(time.perf_counter()-t)"
        start = time.perf_counter()
        process = subprocess.run([sys.executable,'-c',code],env=environment,text=True,capture_output=True,check=True)
        result[engine] = dict(process_seconds=time.perf_counter()-start,import_seconds=float(process.stdout.splitlines()[-1]))
    return result


def fresh_gaincal(vis, native_bin, root):
    """First public call and memory in separate fresh Python processes."""
    root.mkdir(parents=True,exist_ok=True)
    probe = '''import importlib,json,resource,sys,time
from pathlib import Path
start=time.perf_counter()
tasks=importlib.import_module(sys.argv[1])
imports=time.perf_counter()-start
def cpu():
    return sum(r.ru_utime+r.ru_stime for r in [resource.getrusage(resource.RUSAGE_SELF),resource.getrusage(resource.RUSAGE_CHILDREN)])
before=cpu();start=time.perf_counter()
tasks.gaincal(vis=sys.argv[2],caltable=sys.argv[3],refant='A0',calmode='ap',smodel=[1.0],minsnr=0.0,minblperant=2)
wall=time.perf_counter()-start
peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024 if sys.platform=='darwin' else 1)
if sys.platform.startswith('linux'):
    peak=next(int(line.split()[1]) for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmHWM:'))
print(json.dumps(dict(import_seconds=imports,task_wall_seconds=wall,task_cpu_seconds=cpu()-before,
    python_peak_kib=peak)))
'''
    result = {}
    for engine,module in [('casa','casatasks'),('native','casars.tasks')]:
        environment = os.environ.copy()
        environment['CASARS_CALIBRATE_BIN'] = str(native_bin.resolve())
        if engine == 'native': environment.pop('LD_PRELOAD',None)
        started = time.perf_counter()
        process = subprocess.run([sys.executable,'-c',probe,module,vis,str(root/engine)],
                                 env=environment,text=True,capture_output=True,check=True)
        result[engine] = json.loads(process.stdout.splitlines()[-1])
        result[engine]['process_seconds'] = time.perf_counter()-started
        peaks = [int(line.split()[1]) for line in process.stderr.splitlines() if line.startswith('calibration_peak_rss_kib ')]
        if engine == 'native' and sys.platform.startswith('linux'):
            assert peaks, 'native first-call VmHWM measurement missing'
        result[engine]['native_task_peak_kib'] = max(peaks,default=None)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vis', type=Path, required=True, help='staged five-antenna FLUX/PHASE fixture')
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--case', action='append', help='measure only these workflow labels (repeatable)')
    parser.add_argument('--cache', choices=['warm','cold'], default='warm',
                        help='cold evicts input/product file pages before every measured call (Linux)')
    args = parser.parse_args()
    if args.cache == 'cold' and not hasattr(os, 'posix_fadvise'):
        parser.error('cold file-cache measurements require posix_fadvise')
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASA_RS_CALIBRATION_TIMING'] = '1'
    imports = {}
    started = time.perf_counter()
    import casatasks as casa
    from casatools import table
    imports['casa'] = time.perf_counter() - started
    started = time.perf_counter()
    from casars import tasks as native
    imports['native'] = time.perf_counter() - started
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "casa-compat"))
    from calibration_lifecycle_oracle import equal
    vis = str(args.vis.resolve())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='lifecycle-perf-', dir=args.out.parent) as scratch:
        root = Path(scratch)
        gain = root / 'input.gcal'
        casa.gaincal(vis=vis, caltable=str(gain), refant='A0', calmode='ap', smodel=[1.0], minsnr=0.0, minblperant=2)
        import numpy as np
        tool = table();tool.open(str(gain),nomodify=False)
        gains = tool.getcol('CPARAM');gains[:] = np.where(tool.getcol('FIELD_ID') == 0,1.0,2.0)
        tool.putcol('CPARAM',gains);tool.close()
        casa.applycal(vis=vis, gaintable=[str(gain)], flagbackup=False)
        tool = table(); tool.open(vis)
        rows = tool.nrows(); samples = tool.getcell('DATA', 0).size * rows
        tool.close()
        evidence = dict(host=platform.platform(), workers={key: os.getenv(key) for key in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','RAYON_NUM_THREADS']},
                        cache=f'{args.cache} file pages; warm imports; alternating sequential calls; preparation/eviction outside timing',
                        controller_imports_seconds=imports, input_rows=rows, input_complex_samples=samples,
                        input_data_bytes=samples*8, runs={})
        common = dict(vis=vis, refant='A0', smodel=[1.0], minsnr=0.0, minblperant=2)
        cases = [
            ('gaincal_replace','gaincal',dict(common,calmode='ap')),
            ('gaincal_append','gaincal',dict(common,calmode='ap',append=True)),
            ('gaincal_model_replace','gaincal',dict(common,calmode='ap',smodel=[])),
            ('bandpass_replace','bandpass',dict(common,field='FLUX')),
            ('bandpass_append','bandpass',dict(common,field='FLUX',append=True)),
            ('gencal_update','gencal',dict(vis=vis,caltype='opac',parameter=[0.001])),
            ('fluxscale_append','fluxscale',dict(vis=vis,caltable=str(gain),reference=['FLUX'],transfer=['PHASE'],append=True)),
            ('corrected_export','split',dict(vis=vis,datacolumn='corrected')),
        ]
        if args.case:
            unknown = set(args.case) - {case[0] for case in cases}
            if unknown:
                parser.error(f'unknown workflow labels: {sorted(unknown)}')
            cases = [case for case in cases if case[0] in args.case]
        # An attainable warm-cache read/write baseline on this same filesystem.
        block = bytearray(64 * 1024 * 1024)
        disk = root / 'throughput.bin'
        start = time.perf_counter()
        with disk.open('wb') as stream:
            stream.write(block);stream.flush();os.fsync(stream.fileno())
        write_seconds = time.perf_counter() - start
        start = time.perf_counter()
        with disk.open('rb') as stream: stream.readinto(block)
        read_seconds = time.perf_counter() - start
        evidence['attainable_warm_mib_per_second'] = dict(read=64/read_seconds,write_fsync=64/write_seconds)
        if args.cache == 'cold':
            evict_files(root)
            before = block_io(); start = time.perf_counter()
            with disk.open('rb') as stream: stream.readinto(block)
            evidence['attainable_cold_read'] = dict(mib_per_second=64/(time.perf_counter()-start),
                                                    block_bytes=block_io()['read_bytes']-before['read_bytes'])
        disk.unlink()
        for label, task, kwargs in cases:
            paths = {engine: root / (label+'.'+engine) for engine in ['casa','native']}
            records = {engine: [] for engine in paths}
            def call(engine):
                output = paths[engine]
                if task == 'split':
                    if output.exists(): shutil.rmtree(output)
                    function = (lambda: casa.split(outputvis=str(output),**kwargs)) if engine == 'casa' else (lambda: native.calibrate(mode='export_corrected_data',vis=vis,outputvis=str(output)))
                else:
                    key = 'fluxtable' if task == 'fluxscale' else 'caltable'
                    function = lambda: getattr(casa if engine=='casa' else native,task)(**{**kwargs,key:str(output)})
                if args.cache == 'cold':
                    evict_files(vis, gain, output)
                captured = io.StringIO()
                before_io = block_io()
                cpu = usage(); start = time.perf_counter()
                preload = os.environ.pop('LD_PRELOAD',None) if engine == 'native' else None
                try:
                    with contextlib.redirect_stderr(captured): function()
                finally:
                    if preload is not None: os.environ['LD_PRELOAD'] = preload
                elapsed = time.perf_counter()-start
                after_cpu = usage()
                cpu_elapsed = sum(after_cpu.values())-sum(cpu.values())
                after_io = block_io()
                phases = {}
                peaks = []
                worker_counts = []
                for line in captured.getvalue().splitlines():
                    if line.startswith('calibration_phase '):
                        _,name,value=line.split();phases[name]=phases.get(name,0.0)+float(value)
                    elif line.startswith('calibration_peak_rss_kib '):
                        peaks.append(int(line.split()[1]))
                    elif line.startswith('calibration_workers '):
                        worker_counts.append(int(line.split()[1]))
                if engine == 'native' and sys.platform.startswith('linux'):
                    assert peaks, 'native VmHWM measurement missing'
                return dict(wall_seconds=elapsed,cpu_seconds=cpu_elapsed,native_phases_seconds=phases,
                            cpu_breakdown_seconds={key:after_cpu[key]-cpu[key] for key in cpu},
                            native_peak_rss_kib=max(peaks,default=None),
                            native_workers=max(worker_counts,default=None),
                            block_io={key:after_io[key]-before_io[key] for key in before_io})
            # CASA's append-to-absent path does not initialize its SPW table;
            # prepare a normal first product outside the append timing.
            if kwargs.get('append'):
                for engine in paths:
                    key = 'fluxtable' if task == 'fluxscale' else 'caltable'
                    getattr(casa if engine == 'casa' else native,task)(**{**kwargs,'append':False,key:str(paths[engine])})
            for engine in paths: call(engine)  # warm-up and initial products
            for repeat in range(args.repeats):
                for engine in (['casa','native'] if repeat%2==0 else ['native','casa']):
                    records[engine].append(call(engine))
            columns = ['DATA','FLAG','TIME','ANTENNA1','ANTENNA2','FIELD_ID'] if task=='split' else ['CPARAM','FLAG','TIME','ANTENNA1','ANTENNA2','FIELD_ID'] if task in ['gaincal','bandpass'] else None
            equal(paths['casa'],paths['native'],columns)
            selected_rows = rows//2 if task == 'bandpass' else rows if task in ['gaincal','split'] else 0
            output_bytes = {engine:sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) for engine,path in paths.items()}
            evidence['runs'][label] = dict(selected_rows=selected_rows,selected_data_bytes=selected_rows*(samples//rows)*8,output_bytes=output_bytes,records=records,medians={engine:statistics.median(r['wall_seconds'] for r in values) for engine,values in records.items()})
            args.out.write_text(json.dumps(evidence,indent=2)+'\n')
            print(label,evidence['runs'][label]['medians'],flush=True)
        evidence['memory_peak_kib'] = dict(controller_with_both_imports=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                                          native_task_children=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
        evidence['memory_note'] = 'diagnostic getrusage peaks can inherit pre-exec parent RSS; use per-native-call and fresh-process VmHWM for comparisons on Linux'
        evidence['cold_imports_seconds'] = cold_imports()
        evidence['cold_import_note'] = 'fresh interpreters; filesystem cache warm; normal native environment without the CASA wheel library preloads'
        evidence['fresh_gaincal'] = fresh_gaincal(vis,args.native_bin,root/'fresh')
        equal(root/'fresh/casa',root/'fresh/native',['CPARAM','FLAG','TIME','ANTENNA1','ANTENNA2','FIELD_ID'])
        evidence['fresh_gaincal_note'] = 'fresh interpreters and first task, warm file pages; Linux VmHWM reports each current process; Python/native task peaks are separate, not simultaneous totals'
        evidence['export_product_note'] = 'native export retains CORRECTED_DATA and MODEL_DATA; equivalent DATA/flags/row identities validated against CASA split; output bytes recorded'
        args.out.write_text(json.dumps(evidence,indent=2)+'\n')


if __name__ == '__main__': main()
