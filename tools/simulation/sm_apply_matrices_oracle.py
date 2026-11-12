#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Supply full polarized-error tables to simulated DATA (SIM-16)."""
import argparse
from pathlib import Path
import numpy as np
from casatools import simulator, table
from casatasks import version_string
from sm_multiple_models_oracle import make_session
from sm_apply_parameters_oracle import initialize
from sm_flagged_samples_oracle import read_rows
from sm_apply_time_oracle import write

CASES=[dict(name='general_jones',kind='J',channels=1),
       dict(name='flagged_general_jones',kind='J',channels=1,flag=True),
       dict(name='spectral_general_jones',kind='Jf',channels=4),
       dict(name='full_leakage',kind='Dgen',channels=1),
       dict(name='spectral_full_leakage',kind='Dfgen',channels=4)]


def parameters(kind, antenna, channel):
    a=float(antenna)
    if kind in ('J','Jf'):
        values=[1.1+.03*a+.2j,.02*a+.03j,-.01*a+.01j,.8+.02*a-.1j]
    else:
        values=[.02+.01*a+.03j,-.01*a+.01j]
    return np.array(values,dtype=np.complex128)*(1.+.05*channel)


def initialize_matrix_data(path):
    tb=table();tb.open(str(path),nomodify=False)
    data=np.array([3.+4j,-2.+.5j,1.5-.2j,4.-1j],dtype=np.complex64)[:,None]*np.array([1.,1.1,1.2,1.3],dtype=np.float32)[None,:]
    for row in range(tb.nrows()):tb.putcell('DATA',row,data)
    times=tb.getcol('TIME');tb.close();return times


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True);parser.add_argument('--fixture',type=Path,required=True)
    args=parser.parse_args();assert version_string()=='6.7.6.14'
    args.out.mkdir(parents=True,exist_ok=True);records=[]
    for spec in CASES:
        directory=args.out/spec['name'];directory.mkdir();path=directory/'data.ms';terms=directory/'terms.cal'
        sm=make_session(path,stokes='XX XY YX YY');assert sm.setgain(mode='random',table=str(terms),amplitude=[0.,0.]);sm.reset();sm.close();initialize(path)
        times=initialize_matrix_data(path);tb=table();tb.open(str(terms),nomodify=False)
        kind=spec['kind'];info=tb.info();info['subType']=kind+' Jones';tb.putinfo(info);tb.putkeyword('VisCal',kind+' Jones')
        for row,antenna in enumerate(tb.getcol('ANTENNA1')):
            values=np.stack([parameters(kind,antenna,c) for c in range(spec['channels'])],axis=1).astype(np.complex64)
            flags=np.zeros_like(values,dtype=bool)
            if spec.get('flag') and antenna==2:flags[1,0]=True
            for column,value in [('CPARAM',values),('FLAG',flags),('PARAMERR',np.zeros_like(values.real)),('SNR',np.ones_like(values.real))]:tb.putcell(column,row,value)
        count=tb.nrows();tb.close()
        if spec['channels']>1:
            tb.open(str(terms/'SPECTRAL_WINDOW'),nomodify=False);tb.putcell('NUM_CHAN',0,4);tb.putcell('CHAN_FREQ',0,230e9+np.arange(4)*15.625e6)
            for column in ('CHAN_WIDTH','RESOLUTION','EFFECTIVE_BW'):tb.putcell(column,0,np.full(4,15.625e6))
            tb.close()
        sm=simulator();assert sm.openfromms(str(path));record={**spec,'calibration_rows':count};stage='setapply'
        try:
            assert sm.setapply(table=str(terms),interp='linear,nearest',calwt=False);stage='corrupt';assert sm.corrupt();record['outcome']='returned'
        except RuntimeError as error:record.update(outcome='raises',stage=stage,error=str(error))
        sm.close();rows=read_rows(path)
        for row,time in zip(rows,times):row['time_s']=float(time)
        record['rows']=rows;records.append(record);write(args.fixture,dict(casa_version=version_string(),cases=records));print(spec['name'],record['outcome'],flush=True)


if __name__=='__main__':main()
