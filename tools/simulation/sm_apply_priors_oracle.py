#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Impose supplied delay/position/opacity tables on CASA simulated DATA (#1291)."""
import argparse
from pathlib import Path
import numpy as np
from casatools import simulator, table
from casatasks import gencal, version_string
from sm_multiple_models_oracle import make_session
from sm_apply_parameters_oracle import initialize, calibration as leakage_table
from sm_flagged_samples_oracle import read_rows
from sm_apply_time_oracle import write

CASES=[
    dict(name='delay',caltype='sbd',parameter=[1.2,-.8],antenna='0',pol='X,Y'),
    dict(name='signed_delay_interpolation',caltype='sbd',parameter=[1.2,-.8],antenna='0',pol='X,Y',time_values=[[-1.2,.8],[1.2,-.8]]),
    dict(name='antenna_position',caltype='antpos',parameter=[.01,-.02,.03],antenna='0'),
    dict(name='opacity',caltype='opac',parameter=[.05],antenna=''),
    dict(name='opacity_argument',caltype='opac',parameter=[.05],antenna='',opacity=.2),
    dict(name='gain_curve',caltype='opac',parameter=[.05],antenna='',subtype='EGainCurve',values=[1.,.001,-.00001,0.,1.2,-.001,.00002,0.]),
    dict(name='gain_curve_with_leakage',caltype='opac',parameter=[.05],antenna='',subtype='EGainCurve',values=[1.,.001,-.00001,0.,1.2,-.001,.00002,0.],leakage=True),
    dict(name='system_temperature',caltype='opac',parameter=[.05],antenna='',subtype='B TSYS',values=[4.,9.]),
    dict(name='negative_system_temperature',caltype='opac',parameter=[.05],antenna='',subtype='B TSYS',values=[-4.,9.]),
    dict(name='spectral_system_temperature',caltype='opac',parameter=[.05],antenna='',subtype='B TSYS',channels=3,values=[-4.,9.,16.,25.,4.,36.]),
    dict(name='cross_hand_delay',caltype='sbd',parameter=[1.2,-.8],antenna='0',pol='X,Y',subtype='Kcross Jones',values=[1.2,-.8]),
    dict(name='multi_band_delay',caltype='sbd',parameter=[.0012,-.0008],antenna='0',pol='X,Y',subtype='KMBD Jones',values=[.0012,-.0008]),
    dict(name='spectral_opacity',caltype='opac',parameter=[.05],antenna='',subtype='TfOpac',channels=4,values=[.03,.05,.08,.12]),
    dict(name='power_curve',caltype='opac',parameter=[.05],antenna='',subtype='EPowerCurve',values=[1.,.001,-.00001,0.,1e-8,0.,0.,1e-13,1.2,-.001,.00002,0.,0.,-1e-10,0.,0.]),
]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True);parser.add_argument('--fixture',type=Path,required=True)
    args=parser.parse_args();assert version_string()=='6.7.6.14'
    args.out.mkdir(parents=True,exist_ok=True);records=[]
    for spec in CASES:
        directory=args.out/spec['name'];directory.mkdir()
        path=directory/'data.ms';terms=directory/'terms.cal'
        sm=make_session(path,stokes='XX XY YX YY');sm.close();initialize(path)
        tb=table();tb.open(str(path));times=tb.getcol('TIME');tb.close();bounds=(float(min(times)),float(max(times)))
        gencal(vis=str(path),caltable=str(terms),caltype=spec['caltype'],parameter=spec['parameter'],antenna=spec['antenna'],pol=spec.get('pol',''))
        if 'subtype' in spec:
            tb=table();tb.open(str(terms),nomodify=False);info=tb.info();info['subType']=spec['subtype'];tb.putinfo(info);tb.putkeyword('VisCal',spec['subtype'])
            values=np.array(spec['values'],dtype=np.float32).reshape(-1,spec.get('channels',1))
            for row in range(tb.nrows()):
                row_values=np.zeros_like(values) if spec['caltype']=='sbd' and tb.getcell('ANTENNA1',row)!=0 else values
                for column,data in [('FPARAM',row_values),('FLAG',np.zeros_like(values,dtype=bool)),('PARAMERR',np.zeros_like(values)),('SNR',np.ones_like(values))]:tb.putcell(column,row,data)
            tb.close()
            if spec.get('channels',1)>1:
                tb.open(str(terms/'SPECTRAL_WINDOW'),nomodify=False);channels=spec['channels']
                tb.putcell('NUM_CHAN',0,channels);tb.putcell('CHAN_FREQ',0,np.linspace(230e9,230e9+3*15.625e6,channels))
                for column in ('CHAN_WIDTH','RESOLUTION','EFFECTIVE_BW'):tb.putcell(column,0,np.full(channels,3*15.625e6/(channels-1)))
                tb.close()
        if 'time_values' in spec:
            tb=table();tb.open(str(terms),nomodify=False)
            count=tb.nrows();templates=[{key:tb.getcell(key,row) for key in tb.colnames() if tb.iscelldefined(key,row)} for row in range(count)]
            tb.addrows(count)
            for endpoint in range(2):
                for row,template in enumerate(templates):
                    target=endpoint*count+row
                    for key,value in template.items():tb.putcell(key,target,value)
                    tb.putcell('TIME',target,bounds[endpoint]);tb.putcell('FIELD_ID',target,endpoint)
                    values=spec['time_values'][endpoint] if int(template['ANTENNA1'])==0 else [0.,0.]
                    tb.putcell('FPARAM',target,np.array(values,dtype=np.float32).reshape(2,1))
            tb.close()
        tb=table();tb.open(str(terms));info=tb.info();column='FPARAM' if 'FPARAM' in tb.colnames() else 'CPARAM'
        calibration=dict(kind=tb.getkeyword('VisCal'),rows=tb.nrows(),shape=list(tb.getcell(column,0).shape))
        tb.close();sm=simulator();assert sm.openfromms(str(path))
        if spec.get('leakage'):
            leakage=directory/'leakage.cal';leakage_table(sm,leakage,'D',bounds)
            assert sm.setapply(table=str(leakage),interp='linear',calwt=False)
        record={**spec,'calibration':calibration};stage='setapply'
        try:
            assert sm.setapply(table=str(terms),interp='linear',calwt=False,opacity=spec.get('opacity',0.));stage='corrupt'
            assert sm.corrupt();record['outcome']='returned'
        except RuntimeError as error:record.update(outcome='raises',stage=stage,error=str(error))
        sm.close();rows=read_rows(path);tb.open(str(path));times=tb.getcol('TIME');tb.close()
        for row,time in zip(rows,times):row['time_s']=float(time)
        record.update(rows=rows);records.append(record)
        write(args.fixture,dict(casa_version=version_string(),cases=records))
        print(spec['name'],calibration,record['outcome'],flush=True)


if __name__=='__main__':main()
