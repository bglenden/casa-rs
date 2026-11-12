#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulator phase-delay and scan/observation interpolation (#1291)."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import numpy as np
import sm_apply_time_oracle as curves
from sm_apply_parameters_oracle import initialize
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session

CASES = [dict(name=f'{kind}_{mode}PD', kind=kind, mode=mode+'PD', probe='curve')
         for kind in ('G','D','T') for mode in ('nearest','linear','cubic')]
CASES += [dict(name=f'{kind}_linearPD_flag', kind=kind, mode='linearPD', probe='flag') for kind in ('G','T')]
CASES += [dict(name=f'{kind}_linearPD', kind=kind, mode='linearPD', probe='curve') for kind in ('B','Tf')]
CASES += [dict(name='G_'+mode+'_'+segment, kind='G', mode=mode, probe='curve', segment=segment)
          for mode,segment in [('linearperobs','single'),('linearperscan','unset'),
                               ('linearperscan','split'),('nearestperscan','split'),
                               ('linearperscan','missing'),('linearperobs','split'),
                               ('linearperobs','mismatch'),('linearperobsperscan','split')]]
CASES += [dict(name='G_cubicperscan_short',kind='G',mode='cubicperscan',probe='curve',segment='split',refusal=True)]


def observation_rows(path, count):
    from casatools import table
    tb=table();tb.open(str(path),nomodify=False)
    while tb.nrows()<count:
        old={name:tb.getcell(name,0) for name in tb.colnames() if tb.iscelldefined(name,0)}
        row=tb.nrows();tb.addrows(1)
        for name,value in old.items():tb.putcell(name,row,value)
    tb.close()


def prepare_context(path, terms, spec):
    from casatools import table
    segment=spec.get('segment')
    split=segment=='split' and 'perobs' in spec['mode']
    if split:
        observation_rows(path/'OBSERVATION',2)
        observation_rows(terms/'OBSERVATION',2)
        tb=table();tb.open(str(path),nomodify=False)
        tb.putcol('OBSERVATION_ID',tb.getcol('FIELD_ID'));tb.close()
    tb=table();tb.open(str(terms),nomodify=False)
    times=tb.getcol('TIME');later=times>np.median(times)
    if 'perscan' in spec['mode'] and segment not in ('unset',None):
        tb.putcol('SCAN_NUMBER',np.where(later,3 if segment=='missing' else 2,1).astype(np.int32))
    if 'perobs' in spec['mode'] and segment in ('split','mismatch'):
        ids=later.astype(np.int32)
        if 'perscan' in spec['mode']:ids=1-ids
        tb.putcol('OBSERVATION_ID',ids)
    tb.close()
    if 'PD' in spec['mode'] and spec['kind'] in ('G','T','D'):
        tb.open(str(terms/'SPECTRAL_WINDOW'),nomodify=False)
        tb.putcell('CHAN_FREQ',0,np.array([100e9]));tb.putcell('REF_FREQUENCY',0,100e9);tb.close()


def apply_partitioned(sm, path, terms, spec, out):
    """CASA control using explicit table partitions and MAIN scan selection."""
    from casatools import table
    import shutil
    tb=table();tb.open(str(path));scans=np.unique(tb.getcol('SCAN_NUMBER'));tb.close()
    for scan in scans:
        subset=out/f'scan-{scan}.cal';shutil.copytree(terms,subset)
        tb.open(str(subset),nomodify=False);numbers=tb.getcol('SCAN_NUMBER')
        selected=np.flatnonzero(numbers==scan)
        if len(selected):tb.removerows(np.flatnonzero(numbers!=scan).tolist())
        else:
            # Missing per-scan gains are neutral, while corruption still
            # copies the selected DATA into CORRECTED_DATA.
            values=tb.getcol('CPARAM');tb.putcol('CPARAM',np.ones_like(values))
            tb.putcol('FLAG',np.zeros_like(tb.getcol('FLAG')))
        tb.putcol('SCAN_NUMBER',np.full(tb.nrows(),-1,dtype=np.int32));tb.close()
        assert sm.reset();assert sm.setdata(msselect=f'SCAN_NUMBER=={scan}')
        mode=spec['mode'].replace('perscan','').replace('perobs','')
        assert sm.setapply(table=str(subset),interp=mode+',nearest',calwt=False)
        assert sm.corrupt()


def worker(spec,out,fixture,partition_control=False):
    from casatools import simulator,table
    path=out/'data.ms';terms=out/'terms.cal'
    sm=make_session(path,stokes='XX XY YX YY');sm.close();initialize(path)
    sm=simulator();assert sm.openfromms(str(path));calibration=curves.build(sm,terms,spec);sm.close()
    prepare_context(path,terms,spec)
    sm=simulator();assert sm.openfromms(str(path))
    interp=spec['mode']+(',nearestflag' if spec['probe']=='flag' else ',nearest')
    stage='setapply';record={**spec,'interp':interp,'calibration':calibration}
    try:
        print('before setapply',interp,flush=True)
        if partition_control:
            stage='corrupt';apply_partitioned(sm,path,terms,spec,out)
        else:
            assert sm.setapply(table=str(terms),interp=interp,calwt=False);stage='corrupt'
            print('before corrupt',flush=True);assert sm.corrupt()
        record['outcome']='returned'
    except RuntimeError as error:
        record.update(outcome='raises',stage=stage,error=str(error),exception_class=type(error).__name__)
    sm.close();rows=read_rows(path)
    tb=table();tb.open(str(path));times=tb.getcol('TIME');obs=tb.getcol('OBSERVATION_ID');scans=tb.getcol('SCAN_NUMBER');tb.close()
    for row,time,observation,scan in zip(rows,times,obs,scans):
        row.update(time_s=float(time),observation=int(observation),scan=int(scan))
    record.update(rows=rows,data_unchanged=all(value==[3.,4.] for row in rows for value in row['data']))
    curves.write(fixture,record)


def main():
    import casatasks
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True);parser.add_argument('--fixture',type=Path,required=True)
    parser.add_argument('--only');parser.add_argument('--worker',action='store_true');parser.add_argument('--resume',action='store_true')
    parser.add_argument('--partition-control',action='store_true')
    args=parser.parse_args();assert casatasks.version_string()=='6.7.6.14'
    args.out.mkdir(parents=True,exist_ok=True)
    selected=[s for s in CASES if not args.only or s['name']==args.only];assert selected
    if args.worker:
        assert len(selected)==1;worker(selected[0],args.out,args.fixture,args.partition_control);return
    records=[]
    for spec in selected:
        import gzip
        def run(suffix='',flags=(),allow_failure=False):
            out=args.out/(spec['name']+suffix);out.mkdir(exist_ok=args.resume);result=out/'result.json.gz'
            if args.resume and result.exists():return 0,json.load(gzip.open(result,'rt'))
            if args.resume:
                import shutil
                for child in out.iterdir():
                    if child.is_dir():shutil.rmtree(child)
            with (out/'worker.log').open('w') as log:
                status=subprocess.run([sys.executable,__file__,'--worker','--only',spec['name'],'--out',str(out),'--fixture',str(result),*flags],stdout=log,stderr=subprocess.STDOUT).returncode
            if status and not allow_failure:raise RuntimeError(f'{spec["name"]} exited{status}; see {out}/worker.log')
            return status,json.load(gzip.open(result,'rt')) if result.exists() else None
        if 'perscan' in spec['mode'] and spec.get('segment') not in ('unset',None):
            status,raw=run('_raw',allow_failure=True)
            _,record=run(flags=('--partition-control',))
            record.update(casa_original_returncode=status,exception='Simulator VisBuffer synchronization omits SCAN_NUMBER; explicit scan/table partition controls supply the intended per-scan behavior.')
        else:_,record=run()
        assert record['outcome']==('raises' if spec.get('refusal') else 'returned'),record
        records.append(record);curves.write(args.fixture,dict(casa_version=casatasks.version_string(),cases=records))
        print(spec['name'],'passed',flush=True)


if __name__=='__main__':main()
