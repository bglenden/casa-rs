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
    return own.ru_utime + own.ru_stime + child.ru_utime + child.ru_stime


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vis', type=Path, required=True, help='staged five-antenna FLUX/PHASE fixture')
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--case', help='measure only this workflow label')
    args = parser.parse_args()
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
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'casa-compat'))
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
                        cache='warm imports and inputs; alternating sequential calls; outputs prepared outside timing',
                        controller_imports_seconds=imports, input_rows=rows, input_complex_samples=samples,
                        input_data_bytes=samples*8, runs={})
        common = dict(vis=vis, refant='A0', smodel=[1.0], minsnr=0.0, minblperant=2)
        cases = [
            ('gaincal_replace','gaincal',dict(common,calmode='ap')),
            ('gaincal_append','gaincal',dict(common,calmode='ap',append=True)),
            ('bandpass_replace','bandpass',dict(common,field='FLUX')),
            ('bandpass_append','bandpass',dict(common,field='FLUX',append=True)),
            ('gencal_update','gencal',dict(vis=vis,caltype='opac',parameter=[0.001])),
            ('fluxscale_append','fluxscale',dict(vis=vis,caltable=str(gain),reference=['FLUX'],transfer=['PHASE'],append=True)),
            ('corrected_export','split',dict(vis=vis,datacolumn='corrected')),
        ]
        if args.case:
            cases = [case for case in cases if case[0] == args.case]
            if not cases:
                parser.error(f'unknown workflow label: {args.case}')
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
                captured = io.StringIO()
                cpu = usage(); start = time.perf_counter()
                preload = os.environ.pop('LD_PRELOAD',None) if engine == 'native' else None
                try:
                    with contextlib.redirect_stderr(captured): function()
                finally:
                    if preload is not None: os.environ['LD_PRELOAD'] = preload
                elapsed = time.perf_counter()-start
                phases = {}
                for line in captured.getvalue().splitlines():
                    if line.startswith('calibration_phase '):
                        _,name,value=line.split();phases[name]=float(value)
                return dict(wall_seconds=elapsed,cpu_seconds=usage()-cpu,native_phases_seconds=phases)
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
        evidence['memory_note'] = 'Linux ru_maxrss; controller includes CASA and casars; child peak is cumulative across workflows'
        evidence['cold_imports_seconds'] = cold_imports()
        evidence['cold_import_note'] = 'fresh interpreters; filesystem cache warm; normal native environment without the CASA wheel library preloads'
        evidence['export_product_note'] = 'native export retains CORRECTED_DATA and MODEL_DATA; equivalent DATA/flags/row identities validated against CASA split; output bytes recorded'
        args.out.write_text(json.dumps(evidence,indent=2)+'\n')


if __name__ == '__main__': main()
