#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 supplied T/Tf noise-scale outputs for simulation #1292."""
import argparse
import gzip
import json
from pathlib import Path
import tempfile
import numpy as np
from casatools import simulator, table
from sm_multiple_models_oracle import make_session

CASES = [dict(name='manual_clear', mode='tsys-manual', tau=0.),
         dict(name='manual_opaque', mode='tsys-manual', tau=0.05),
         dict(name='atmosphere', mode='tsys-atm', pwv='1mm')]


def verify_native(root):
    count=0
    for case in sorted(Path(root).glob("*/case.json")):
        directory=case.parent
        tb=table();tb.open(str(directory/'noise.T.cal'))
        gains={(float(tb.getcell('TIME',r)),int(tb.getcell('ANTENNA1',r))):tb.getcell('CPARAM',r)[0]
               for r in range(tb.nrows())}
        tb.close();tb.open(str(directory/'data.ms'))
        expected=[]
        for r in range(tb.nrows()):
            time=float(tb.getcell('TIME',r));p=int(tb.getcell('ANTENNA1',r));q=int(tb.getcell('ANTENNA2',r))
            expected.append(tb.getcell('DATA',r)*gains[time,p]*gains[time,q].conj())
        tb.close()
        sm=simulator();assert sm.openfromms(str(directory/'data.ms'))
        assert sm.setapply(table=str(directory/'noise.T.cal'),interp='nearest')
        assert sm.corrupt();sm.close()
        tb.open(str(directory/'data.ms'))
        actual=[tb.getcell('DATA',r) for r in range(tb.nrows())];tb.close()
        expected=np.asarray(expected);actual=np.asarray(actual)
        nrms=float(np.linalg.norm(actual-expected)/np.linalg.norm(expected))
        assert nrms<1e-3,(directory,nrms)
        print('CASA consumed native',json.loads(case.read_text())['options']['name'],'DATA NRMS',nrms)
        count+=1
    assert count==3,count


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--out')
    group.add_argument('--verify-native')
    args=parser.parse_args()
    if args.verify_native:
        verify_native(args.verify_native);return
    cases=[]
    with tempfile.TemporaryDirectory() as directory:
        for options in CASES:
            case=Path(directory)/options['name']; case.mkdir()
            path=case/'data.ms';sm=make_session(path,'XX XY YX YY');sm.close()
            sm=simulator();sm.openfromms(str(path))
            parameters={key:value for key,value in options.items() if key!='name'}
            assert sm.setnoise(table=str(case/'noise.cal'),OTF=True,**parameters)
            sm.close()
            tb=table();tb.open(str(case/'noise.T.cal'))
            result=dict(options=options,info=tb.info(),rows=[])
            for row in range(tb.nrows()):
                gain=tb.getcell('CPARAM',row)
                result['rows'].append(dict(time_s=float(tb.getcell('TIME',row)),
                    antenna=int(tb.getcell('ANTENNA1',row)),field=int(tb.getcell('FIELD_ID',row)),
                    window=int(tb.getcell('SPECTRAL_WINDOW_ID',row)),shape=list(gain.shape),
                    gains=[[float(x.real),float(x.imag)] for x in gain.ravel(order='F')],
                    flags=tb.getcell('FLAG',row).ravel(order='F').tolist()))
            tb.close();cases.append(result)
    with gzip.open(args.out,'wt') as output:
        json.dump(dict(casa_version='6.7.6.14',cases=cases),output)


if __name__=='__main__':main()
