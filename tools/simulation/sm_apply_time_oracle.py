#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent CASA time interpolation and sampled natural-spline controls (#1053)."""
import argparse
import gzip
import json
from pathlib import Path
import subprocess
import sys
import numpy as np
import sm_apply_parameters_oracle as parameters
from sm_multiple_models_oracle import make_session
from sm_flagged_samples_oracle import read_rows

FACTORS=(1+0j,1.1+.7j,-.6+1.5j,-1.4-.3j)
OFFSETS=(0j,.06+.04j,-.16+.10j,.24-.08j)
KINDS=('G','D','T','B','Tf','Df')
CASES=[{'name':f'{kind}_{mode}','kind':kind,'mode':mode,'probe':'curve'} for kind in KINDS for mode in ('nearest','linear','cubic')]
CASES += [{'name':f'{kind}_cubic_flag','kind':kind,'mode':'cubic','probe':'flag'} for kind in ('G','T','B','Tf')]
CASES += [{'name':f'{kind}_cubic_single','kind':kind,'mode':'cubic','probe':'single'} for kind in ('G','T')]
CASES += [{'name':f'{kind}_{mode}_analytic','kind':kind,'mode':mode,'probe':'analytic'} for kind in ('G','T') for mode in ('cubic','spline')]
CASES += [{'name':f'{kind}_spline','kind':kind,'mode':'spline','probe':'curve'} for kind in KINDS]
REFUSALS=[{'name':f'{kind}_cubic_{probe}','kind':kind,'mode':'cubic','probe':probe,'refusal':True} for kind in ('G','T') for probe in ('short2','short3','short2_exact')]
REFUSALS += [{'name':'G_unknown_time','kind':'G','mode':'unknown-time-mode','probe':'curve','refusal':True}]


def write(path,record):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('wb') as raw,gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as stream:
        stream.write((json.dumps(record,separators=(',',':'),allow_nan=False)+'\n').encode())


def build(sm,path,spec):
    from casatools import table
    kind=spec['kind'];receptors=1 if kind in ('T','Tf') else 2;channels=4 if kind in ('B','Tf','Df') else 1
    assert sm.setgain(mode='fbm',table=str(path),amplitude=[0.,0.]);sm.reset()
    tb=table();tb.open(str(path),nomodify=False)
    original=np.unique(tb.getcol('TIME'));assert len(original)==4
    wanted=1 if spec['probe']=='single' else 2 if spec['probe'].startswith('short2') else 3 if spec['probe']=='short3' else 4
    keep=(original[[0,-1]] if wanted==2 else original[:wanted])
    tb.removerows(np.flatnonzero(~np.isin(tb.getcol('TIME'),keep)).tolist())
    times=tb.getcol('TIME');antennas=tb.getcol('ANTENNA1');shift=0 if spec['probe']=='short2_exact' else 5
    values=np.empty((receptors,channels,len(times)),dtype=np.complex64)
    for row,(time,antenna) in enumerate(zip(times,antennas)):
        index=int(np.flatnonzero(original==time)[0]);base=parameters.receptor_values('D' if kind in ('D','Df') else 'G',int(antenna),0)
        for receptor in range(receptors):
            for channel in range(channels):
                if spec['probe']=='analytic':value=(1.,2.,.5,1.25)[index]
                elif kind in ('D','Df'):value=base[receptor]+OFFSETS[index]+.01*channel
                else:value=base[receptor]*FACTORS[index]*(1+.05*channel)*np.exp(.03j*channel)
                values[receptor,channel,row]=value
    flags=np.zeros(values.shape,dtype=bool)
    if spec['probe']=='flag':flags[0,1 if channels>1 else 0,(antennas==0)&(times==original[0])]=True
    for column,data in [('CPARAM',values),('FLAG',flags),('PARAMERR',np.zeros(values.shape,dtype=np.float32)),('SNR',np.ones(values.shape,dtype=np.float32))]:tb.putcol(column,data)
    tb.putcol('TIME',times-shift);tb.putcol('FIELD_ID',np.zeros_like(tb.getcol('FIELD_ID')))
    tb.putcol('INTERVAL',np.zeros(tb.nrows()));tb.putkeyword('VisCal',kind+' Jones');info=tb.info();info['subType']=kind+' Jones';tb.putinfo(info);tb.close()
    if channels==4:
        source=table();source.open(str(path.parent/'data.ms'/'SPECTRAL_WINDOW'));tb.open(str(path/'SPECTRAL_WINDOW'),nomodify=False)
        for column in tb.colnames():
            if column in source.colnames() and source.iscelldefined(column,0):tb.putcell(column,0,source.getcell(column,0))
        tb.close();source.close()
    return {'channels':channels,'parameters':receptors,'times_s':(keep-shift).tolist(),'original_times_s':original.tolist(),'rows':len(times)}


def sample_spline(path):
    """Dense linear-system control, independent of the native tridiagonal solver."""
    from casatools import table
    tb=table();tb.open(str(path),nomodify=False);times=tb.getcol('TIME');antennas=tb.getcol('ANTENNA1');values=tb.getcol('CPARAM');flags=tb.getcol('FLAG')
    data_times=np.unique(times)+5;assert len(data_times)==4
    for antenna in np.unique(antennas):
        rows=np.flatnonzero(antennas==antenna);rows=rows[np.argsort(times[rows])];assert len(rows)==4
        z=values[:,:,rows].astype(np.complex128);axes=np.stack((np.abs(z),np.unwrap(np.angle(z),axis=-1)),axis=-1)
        curvature=np.zeros_like(axes);rhs=6*(axes[:,:,2:,:]-2*axes[:,:,1:3,:]+axes[:,:,:2,:])
        curvature[:,:,1:3,:]=np.linalg.solve(np.array([[4.,1.],[1.,4.]]),rhs)
        output=[]
        for position in (.5,1.5,2.5,3.):
            if position==3.:ap=axes[:,:,3,:]
            else:
                low=int(position);a=1-(position-low);b=1-a
                ap=a*axes[:,:,low,:]+b*axes[:,:,low+1,:]+((a**3-a)*curvature[:,:,low,:]+(b**3-b)*curvature[:,:,low+1,:])/6
            output.append(ap[:,:,0]*np.exp(1j*ap[:,:,1]))
        values[:,:,rows]=np.stack(output,axis=-1).astype(np.complex64)
    tb.putcol('CPARAM',values);tb.putcol('TIME',times+5);tb.putcol('FLAG',flags);tb.close()


def worker(spec,out,fixture,sampled=False,nearest_control=False,raw_spline=False):
    from casatools import simulator,table
    path=out/'data.ms';terms=out/'terms.cal';sm=make_session(path,stokes='XX XY YX YY');sm.close();parameters.initialize(path)
    sm=simulator();assert sm.openfromms(str(path));calibration=build(sm,terms,spec)
    if sampled:sample_spline(terms)
    mode='nearest' if sampled or nearest_control else spec['mode']
    interp=mode+(',nearestflag' if spec['probe']=='flag' else ',nearest')
    stage='setapply';record={**spec,'interp':spec['mode']+(',nearestflag' if spec['probe']=='flag' else ',nearest'),'calibration':calibration}
    try:
        print('before setapply',interp,flush=True);assert sm.setapply(table=str(terms),interp=interp,calwt=False)
        print('after setapply',flush=True);stage='corrupt';assert sm.corrupt();record['outcome']='returned'
    except RuntimeError as error:record.update(outcome='raises',stage=stage,error=str(error),exception_class=type(error).__name__)
    sm.close();rows=read_rows(path);tb=table();tb.open(str(path));times=tb.getcol('TIME');tb.close()
    for row,time in zip(rows,times):row['time_s']=float(time)
    record['rows']=rows;record['data_unchanged']=all(value==[3.,4.] for row in rows for value in row['data'])
    if sampled and spec['probe']=='analytic':
        for row in rows:
            channel=(row['time_s']-min(times))/10;amplitude=(289/160,203/160,47/80,5/4)[int(channel)]
            assert abs(complex(*row['data'][0])-(3+4j)*np.float32(amplitude)**2)<3e-6*abs(complex(*row['data'][0]))
    write(fixture,record)


def main():
    import casatasks
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--out',type=Path,required=True);parser.add_argument('--fixture',type=Path,required=True);parser.add_argument('--only');parser.add_argument('--worker',action='store_true');parser.add_argument('--sampled',action='store_true');parser.add_argument('--nearest-control',action='store_true');parser.add_argument('--raw-spline',action='store_true');parser.add_argument('--resume',action='store_true');args=parser.parse_args();assert casatasks.version_string()=='6.7.6.14'
    args.out.mkdir(parents=True,exist_ok=True);selected=[s for s in CASES+REFUSALS if not args.only or s['name']==args.only];assert selected
    if args.worker:
        assert len(selected)==1;worker(selected[0],args.out,args.fixture,args.sampled,args.nearest_control,args.raw_spline);return
    records=[]
    for spec in selected:
        def run(suffix='',flags=(),allow_failure=False):
            directory=args.out/(spec['name']+suffix);directory.mkdir(exist_ok=args.resume);result=directory/'result.json.gz'
            if args.resume and result.exists():return 0,json.load(gzip.open(result,'rt'))
            with (directory/'worker.log').open('w') as log:
                status=subprocess.run([sys.executable,__file__,'--worker','--only',spec['name'],'--out',str(directory),'--fixture',str(result),*flags],stdout=log,stderr=subprocess.STDOUT).returncode
            if status and not allow_failure:raise RuntimeError(f'{spec["name"]}{suffix} exited{status}; see {directory}/worker.log')
            return status,json.load(gzip.open(result,'rt')) if result.exists() else None
        if spec['mode']=='spline':
            status,raw=run('_raw',('--raw-spline',),True);assert status!=0 or raw['outcome']!='returned','CASA spline unexpectedly returned'
            _,record=run('',('--sampled',));_,control=run('_nearest',('--nearest-control',));record.update(casa_original_returncode=status,casa_nearest_rows=control['rows'],exception='CASA selects spline on an uninitialized time interpolator; use a natural spline with complete interior back-substitution.')
        else:_,record=run()
        if spec.get('refusal'):assert record['outcome']=='raises'
        else:assert record['outcome']=='returned'
        records.append(record);write(args.fixture,{'casa_version':'6.7.6.14','work_issue':1053,'recipe':'tools/simulation/sm_apply_time_oracle.py','cases':records});print(spec['name'],'passed',flush=True)


if __name__=='__main__':main()
