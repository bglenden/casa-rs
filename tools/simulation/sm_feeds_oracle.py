#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Actual CASA multi-feed rows and independent single-integration offset controls."""
from pathlib import Path
import argparse,json
import numpy as np
from casatools import simulator,measures,table
import casatasks
assert casatasks.version_string()=='6.7.6.14'
CENTRE=[2.6179938779914944,0.593411945678072]

def run(path,count,mount,split=False):
    sm=simulator();me=measures();assert sm.open(str(path))
    centre=[-1601185.,-5041977.,3554875.]
    xyz=[[centre[k]+step*i for i in range(8)] for k,step in enumerate([20.,-6.,12.])]
    assert sm.setconfig(telescopename='ALMA',x=xyz[0],y=xyz[1],z=xyz[2],dishdiameter=[12.],offset=[0.],mount=[mount],antname=['DA'],padname=['A'],coordsystem='global',referencelocation=me.observatory('ALMA'))
    assert sm.setspwindow(spwname='band6',freq='230GHz',deltafreq='15.625MHz',freqresolution='15.625MHz',refcode='TOPO',nchannels=4,stokes='XX XY YX YY')
    assert sm.setfeed(mode='list',x=[0.,0.0002,-0.0001][:count],y=[0.,-0.0001,0.0003][:count],pol=['X','R','other'][:count])
    assert sm.setfield(sourcename='target',sourcedirection=me.direction('J2000',f'{CENTRE[0]}rad',f'{CENTRE[1]}rad'))
    assert sm.settimes(integrationtime='10s',usehourangle=False,referencetime=me.epoch('UTC','2024/06/01/00:00:00'))
    if split:
        for t in [0,10,20,30]:assert sm.observe(sourcename='target',spwname='band6',starttime=f'{t-37}s',stoptime=f'{t-27}s')
    else:assert sm.observe(sourcename='target',spwname='band6',starttime='-37s',stoptime='3s')
    sm.close();me.done()
    result={'count':count,'mount':mount,'split':split}
    for label,cols in [('MAIN',['ANTENNA1','ANTENNA2','FEED1','FEED2','FIELD_ID','SCAN_NUMBER','TIME','UVW','FLAG_ROW','WEIGHT','SIGMA']),('FEED',['ANTENNA_ID','FEED_ID','BEAM_OFFSET','POLARIZATION_TYPE']),('POINTING',['ANTENNA_ID','TIME','DIRECTION','TARGET'])]:
        tb=table();tb.open(str(path if label=='MAIN' else path/label));result[label]={c:[np.asarray(tb.getcell(c,r)).tolist() for r in range(tb.nrows())] for c in cols};tb.close()
    return result
def record(root):
    root=Path(root);root.mkdir()
    results=[]
    for count,mount,split in [(2,'alt-az',False),(3,'alt-az',False),(2,'equatorial',False),(2,'alt-az',True),(3,'alt-az',True)]:
        result=run(root/f'{count}-{mount}-{split}.ms',count,mount,split);results.append(result)
        print('CASE',count,mount,split,'MAIN',len(result['MAIN']['TIME']),'FEED',len(result['FEED']['FEED_ID']),'POINTING',len(result['POINTING']['TIME']),flush=True)
    for case in results:
        if case['split'] or case['mount'] != 'alt-az':
            continue
        combined = case['MAIN']
        control = next(c['MAIN'] for c in results if c['split'] and c['count'] == case['count'])
        lookup = {(t,p,q,f):v for t,p,q,f,v in zip(control['TIME'],control['ANTENNA1'],control['ANTENNA2'],control['FEED1'],control['UVW'])}
        case['corrected_uvw'] = [lookup[t,p,q,f] for t,p,q,f in zip(combined['TIME'],combined['ANTENNA1'],combined['ANTENNA2'],combined['FEED1'])]
        residual = float(np.max(np.linalg.norm(np.asarray(combined['UVW'])-case['corrected_uvw'],axis=1)))
        print('CASA accumulated-offset defect:',case['count'],'feeds, max UVW difference',residual,flush=True)
    (root/'reference.json').write_text(json.dumps({'casa_version':casatasks.version_string(),'cases':results}))


def verify_native(root):
    cases = sorted(Path(root).glob('*/case.json'))
    assert len(cases) == 3, len(cases)
    for specification in cases:
        case = json.loads(specification.read_text())
        path = specification.parent / 'data.ms'
        for label in ['MAIN','FEED','POINTING']:
            tb = table();tb.open(str(path if label == 'MAIN' else path / label))
            for column, expected in case[label].items():
                if column == 'UVW':
                    expected = case.get('corrected_uvw',expected)
                actual = np.asarray([tb.getcell(column,row) for row in range(tb.nrows())])
                expected = np.asarray(expected)
                if column == 'UVW':
                    residual=np.linalg.norm(actual-expected)/np.linalg.norm(expected)
                    assert residual<1e-3,residual
                    block=len(actual)//case['count']
                    for feed in range(1,case['count']):
                        a=actual[feed*block:(feed+1)*block]-actual[:block]
                        b=expected[feed*block:(feed+1)*block]-expected[:block]
                        assert np.linalg.norm(a-b)/np.linalg.norm(b)<1e-3
                elif expected.dtype.kind in 'OUSb':
                    assert np.array_equal(actual,expected), (specification,label,column)
                else:
                    np.testing.assert_allclose(actual,expected,rtol=0,atol=1e-5)
            tb.close()
        sm = simulator();assert sm.openfromms(str(path))
        assert sm.setnoise(mode='simplenoise',simplenoise='1Jy')
        assert sm.corrupt();sm.close()
        tb=table();tb.open(str(path))
        data=np.asarray([tb.getcell('DATA',r) for r in range(tb.nrows())])
        feeds=np.asarray([tb.getcell('FEED1',r) for r in range(tb.nrows())]);tb.close()
        assert np.all(np.isfinite(data))
        for feed in range(case['count']):
            assert np.any(data[feeds==feed] != 0)
        print('CASA read and corrupted native feeds:',case['count'],case['mount'],flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--out',type=Path)
    group.add_argument('--verify-native',type=Path)
    args=parser.parse_args()
    if args.out:record(args.out)
    else:verify_native(args.verify_native)


if __name__=='__main__':main()
