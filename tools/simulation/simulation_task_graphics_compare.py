#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native-rendered simulation figures with actual CASA artists."""
import argparse
from decimal import Decimal
import gzip
import json
import os
import re
from pathlib import Path
import tarfile
import numpy as np
from simobserve_graphics_oracle import artists


def nrms(actual,expected):
    a,b=np.asarray(actual,dtype=float),np.asarray(expected,dtype=float)
    if a.shape!=b.shape:return float('inf')
    if not np.all(np.isfinite(a)) or not np.all(np.isfinite(b)):return float('inf')
    return float(np.linalg.norm(a-b)/max(np.linalg.norm(b),np.finfo(float).tiny))


def panels(records):
    # CASA creates colorbar axes between its image axes; the shared renderer
    # creates the subplot axes first. Compare the ordered actual data panels.
    return [p for p in records if p['images'] or p['lines'] or p['circles'] or p['texts']]


def annotation_equal(actual,expected,panel):
    if actual==expected:return True
    if not actual.startswith(('min=','bmaj=')):return False
    number=r'[-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?'
    if re.sub(number,'#',actual)!=re.sub(number,'#',expected):return False
    a,b=re.findall(number,actual),re.findall(number,expected)
    scale=max((float(np.max(np.abs(image['pixels']))) for image in panel['images']),default=0.)
    # CASA prints one decimal scientific notation. Account for rounding at
    # either endpoint, plus the image comparison's 1e-3 numerical tolerance.
    return all(abs(float(x)-float(y)) <= .5*(10.**Decimal(x).as_tuple().exponent+10.**Decimal(y).as_tuple().exponent)+1e-3*max(scale,abs(float(y))) for x,y in zip(a,b))


def compare(actual,expected):
    failures=[]
    if set(actual)!=set(expected):failures.append(dict(files=dict(actual=sorted(actual),expected=sorted(expected))))
    for name in set(actual)&set(expected):
        a,b=panels(actual[name]),panels(expected[name])
        if len(a)!=len(b):failures.append(dict(figure=name,panels=[len(a),len(b)]));continue
        for i,(left,right) in enumerate(zip(a,b)):
            for field in ['xlim','ylim']:
                if not np.allclose(left[field],right[field],rtol=1e-3,atol=1e-9):failures.append(dict(figure=name,panel=i,field=field,actual=left[field],expected=right[field]))
            for field in ['aspect','frame_on']:
                if left[field]!=right[field]:failures.append(dict(figure=name,panel=i,field=field,actual=left[field],expected=right[field]))
            for kind in ['circles','anchored_circles','texts']:
                if len(left[kind])!=len(right[kind]):failures.append(dict(figure=name,panel=i,kind=kind,count=[len(left[kind]),len(right[kind])]))
                for n,(x,y) in enumerate(zip(left[kind],right[kind])):
                    for field in x.keys()|y.keys():
                        if field=='font_size':continue  # Existing native document typography.
                        a,b=x[field],y[field]
                        if field=='face_color' and a[3]==b[3]==0:continue  # Fully transparent fill.
                        if field=='text':same=annotation_equal(a,b,right)
                        else:same=np.allclose(a,b,rtol=1e-3,atol=1e-8) if isinstance(a,(list,float,int)) else a==b
                        if not same:failures.append(dict(figure=name,panel=i,kind=kind,layer=n,field=field,actual=a,expected=b))
            if Path(left['title']).name!=Path(right['title']).name:failures.append(dict(figure=name,panel=i,field='title',actual=left['title'],expected=right['title']))
            for kind in ['images','lines']:
                if len(left[kind])!=len(right[kind]):failures.append(dict(figure=name,panel=i,kind=kind,count=[len(left[kind]),len(right[kind])]))
                for n,(x,y) in enumerate(zip(left[kind],right[kind])):
                    fields=['pixels'] if kind=='images' else ['x','y']
                    for field in fields:
                        error=nrms(x[field],y[field])
                        if error>1e-3:failures.append(dict(figure=name,panel=i,layer=n,field=field,nrms=error))
                    if kind=='images':
                        for field in ['extent','clim']:
                            if not np.allclose(x[field],y[field],rtol=1e-3,atol=1e-12):failures.append(dict(figure=name,panel=i,layer=n,field=field,actual=x[field],expected=y[field]))
                        for field in ['cmap','origin','interpolation']:
                            if x[field]!=y[field]:failures.append(dict(figure=name,panel=i,field=field,actual=x[field],expected=y[field]))
            if (left['xlabel'],left['ylabel'])!=(right['xlabel'],right['ylabel']):failures.append(dict(figure=name,panel=i,labels=[left['xlabel'],left['ylabel'],right['xlabel'],right['ylabel']]))
    return failures


def main():
    from casars import tasks
    from matplotlib.figure import Figure
    from matplotlib import pyplot as plt
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference',required=True,type=Path)
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--case',choices=['all','simalma','simanalyze','multi','tp','seed','feather'],default='all')
    args=parser.parse_args()
    with gzip.open(args.reference,'rt') as source:reference=json.load(source)
    root=args.out.resolve();root.mkdir(parents=True,exist_ok=True)
    repo=Path(__file__).resolve().parents[2]
    with tarfile.open(repo/'libs/casa/casa-simulation/tests/data/simalma_inputs.tar.gz') as archive:archive.extractall(root,filter='data')
    with tarfile.open(repo/'libs/casa/casa-simulation/tests/data/simanalyze_multi_inputs.tar.gz') as archive:archive.extractall(root,filter='data')
    # The same default TP array is bundled with the independent inputs.
    os.environ['CASA_RS_MEASURESPATH']=os.environ.get('CASA_RS_MEASURESPATH','')
    captured={};original=Figure.savefig;all_failures=[]
    def save(figure,filename,*args,**kwargs):
        name=Path(filename).name
        match=re.fullmatch(r"\.(.+\.png)\.[^.]+\.png",name)
        captured[match.group(1) if match else name]=artists(figure)
        return original(figure,filename,*args,**kwargs)
    Figure.savefig=save
    try:
        cases=[('simalma',reference['simalma'])]+list(reference['simanalyze'].items())
        for case,data in cases:
            if args.case not in ['all',case] and not (args.case=='simanalyze' and case!='simalma'):continue
            p=dict(data['parameters'])
            if case=='simalma':
                p['hourangle']='35s'
                if p['project']=='combo':p.update(skymodel=str(root/'inputs/model.image'),antennalist=[str(root/'inputs/alma.test.cfg'),str(root/'inputs/aca.test.cfg')])
            completion=tasks.run('simalma' if case=='simalma' else 'simanalyze',workspace=root,**p)
            assert completion.successful
            with gzip.open(root/(case+'-artists.json.gz'),'wt') as output:json.dump(captured,output,separators=(',',':'))
            failures=compare(captured,data['figures'])
            all_failures.extend([dict(case=case,**failure) for failure in failures])
            print(case,'figures',len(captured),'failures',len(failures),flush=True)
            captured.clear();plt.close('all')
    finally:Figure.savefig=original
    (root/'graphics-comparison.json').write_text(json.dumps(all_failures,indent=2))
    print(json.dumps(all_failures,indent=2),flush=True)
    assert not all_failures


if __name__=='__main__':main()
