#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA AW, MVC and multiscale references on native T1 persisted input DATA.

MVC uses the three-channel T1 weighted_observation track; multiscale uses
the four-channel point/Gaussian track. Both use the current Simulator.
CASA reads that common input and computes every
reference image itself. No native image is a reference.
"""
import argparse
import json
import shutil
from pathlib import Path
import numpy as np


def main():
    from casatasks import tclean, version_string
    from casatools import table, image
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vis',required=True,type=Path)
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--case',choices=['aw','mvc','multiscale'],default='mvc')
    args=parser.parse_args()
    assert version_string()=='6.7.6.14'
    root=args.out.resolve();root.mkdir(parents=True,exist_ok=True)
    if args.case=='aw':
        # Point CASAPATH/data and the site configuration's datapath at this
        # casa-root/data directory so BeamCalc uses the same explicit surface.
        # The current Simulator's input MS uses the saved ideal 25 m VP table.
        import os
        surface_root=root/'casa-root/data/nrao/VLA'
        surface_root.mkdir(parents=True,exist_ok=True)
        surface=''.join(f'{r/100:.2f} {0.028*(r/100)**2:.8f} {0.056*r/100:.8f}\n' for r in range(1251))
        for telescope in ['EVLA','VLA']:
            (surface_root/(telescope+'.surface')).write_text(surface)
        os.environ['CASAPATH']=str(root/'casa-root')+' linux local host'
        prefix=root/'casa-aw'
        for product in root.glob('casa-aw.*'):
            if product.is_dir():shutil.rmtree(product)
            else:product.unlink()
        if (root/'casa-cf').exists():shutil.rmtree(root/'casa-cf')
        tclean(vis=str(args.vis.resolve()),imagename=str(prefix),imsize=1024,cell='10arcsec',specmode='mfs',weighting='natural',deconvolver='hogbom',niter=3000,threshold='.01Jy',gridder='awproject',wprojplanes=32,cfcache=str(root/'casa-cf'),normtype='flatnoise',pbcor=True,interactive=False)
        values={}
        for suffix in ['image.pbcor','model','residual','pb']:
            ia=image();ia.open(str(prefix)+'.'+suffix)
            pixels=ia.getchunk()[:,:,0,0];ia.done()
            values[suffix]=dict(centre=float(pixels[512,512]),off_axis=float(pixels[562,482]),box13=float(pixels[506:519,506:519].sum(dtype=np.float64)),box25=float(pixels[500:525,500:525].sum(dtype=np.float64)))
        (root/'reference.json').write_text(json.dumps(values,indent=2));print(json.dumps(values),flush=True)
        return
    if args.case=='multiscale':
        prefix=root/'multiscale'
        for product in root.glob('multiscale.*'):
            if product.is_dir():shutil.rmtree(product)
            else:product.unlink()
        tclean(vis=str(args.vis.resolve()),imagename=str(prefix),imsize=256,cell='.012arcsec',specmode='mfs',deconvolver='multiscale',scales=[0,6,12],weighting='natural',niter=500,threshold='.005Jy',cycleniter=1000,interactive=False)
        values={};beam_area=None
        from casatools import quanta
        qa=quanta()
        for suffix in ['model','image','residual']:
            ia=image();ia.open(str(prefix)+'.'+suffix)
            total=float(np.sum(ia.getchunk()[78:119,82:123,0,0],dtype=np.float64))
            if suffix=='image':
                beam=ia.restoringbeam()
                major=qa.convert(beam['major'],'rad')['value'];minor=qa.convert(beam['minor'],'rad')['value']
                beam_area=np.pi*major*minor/(4*np.log(2)*np.deg2rad(.012/3600)**2)
            if suffix!='model':total/=beam_area
            values[suffix]=total;ia.done()
        (root/'reference.json').write_text(json.dumps(values,indent=2));print(json.dumps(values),flush=True)
        return
    tb=table();tb.open(str(args.vis.resolve()),nomodify=False)
    if 'WEIGHT_SPECTRUM' not in tb.colnames():
        desc=tb.getcoldesc('DATA');desc.update(name='WEIGHT_SPECTRUM',valueType='float',keywords={},dataManagerType='StandardStMan',dataManagerGroup='WEIGHT_SPECTRUM')
        tb.addcols({'WEIGHT_SPECTRUM':desc})
    for row in range(tb.nrows()):
        weight=tb.getcell('WEIGHT',row)
        tb.putcell('WEIGHT_SPECTRUM',row,np.asarray(weight[:,None]*np.array([1.,.75,.4],dtype=np.float32),dtype=np.float32))
    tb.close()
    result=dict(casa_version=version_string(),vis=str(args.vis.resolve()),cases={})
    for pblimit in [-.1,.8]:
        for iterations in [1,3]:
            name=f'mvc-{pblimit}-{iterations}'
            prefix=root/name
            for product in root.glob(name+'.*'):
                if product.is_dir():shutil.rmtree(product)
                else:product.unlink()
            tclean(vis=str(args.vis.resolve()),imagename=str(prefix),imsize=256,cell='.3arcsec',specmode='mvc',deconvolver='mtmfs',nterms=2,nchan=3,reffreq='45GHz',weighting='natural',niter=iterations,cycleniter=1,pblimit=pblimit,interactive=False)
            values={}
            for suffix in ['model.tt0','model.tt1','residual.tt0','residual.tt1','model']:
                ia=image();ia.open(str(prefix)+'.'+suffix)
                values[suffix]=ia.getchunk()[152,144,0,:].tolist()
                ia.done()
            result['cases'][name]=values
    (root/'reference.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2),flush=True)


if __name__=='__main__':main()
