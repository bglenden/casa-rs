#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Impose supplied delay/position/opacity tables on CASA simulated DATA (#1291)."""
import argparse
from pathlib import Path
import numpy as np
from casatools import simulator, table
from casatasks import gencal, version_string
from sm_multiple_models_oracle import make_session
from sm_apply_parameters_oracle import initialize
from sm_flagged_samples_oracle import read_rows
from sm_apply_time_oracle import write

CASES=[
    dict(name='delay',caltype='sbd',parameter=[1.2,-.8],antenna='0',pol='X,Y'),
    dict(name='antenna_position',caltype='antpos',parameter=[.01,-.02,.03],antenna='0'),
    dict(name='opacity',caltype='opac',parameter=[.05],antenna=''),
    dict(name='opacity_argument',caltype='opac',parameter=[.05],antenna='',opacity=.2),
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
        gencal(vis=str(path),caltable=str(terms),caltype=spec['caltype'],parameter=spec['parameter'],antenna=spec['antenna'],pol=spec.get('pol',''))
        tb=table();tb.open(str(terms));info=tb.info();column='FPARAM' if 'FPARAM' in tb.colnames() else 'CPARAM'
        calibration=dict(kind=tb.getkeyword('VisCal'),rows=tb.nrows(),shape=list(tb.getcell(column,0).shape))
        tb.close();sm=simulator();assert sm.openfromms(str(path))
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
