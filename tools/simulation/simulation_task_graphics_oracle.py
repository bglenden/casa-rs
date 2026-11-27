#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record actual CASA artists for simanalyze multi/TP and simalma figures."""
import argparse
import gzip
import importlib
import inspect
import json
import os
from pathlib import Path
import tarfile
from simalma_oracle import corrected_simalma,small_inputs
from simanalyze_multi_oracle import physical_sf_beam
from simanalyze_task_oracle import corrected_simanalyze
from simobserve_graphics_oracle import artists


def analysis_graphics_task():
    task,defaults=corrected_simanalyze()
    module=importlib.import_module('casatasks.private.task_simanalyze')
    source=inspect.getsource(module.simanalyze).replace("max(abs('%s'), %f)","max(abs('%s'), %.17g)")
    deleted='del temp_out, temp_cell, temp_imsize, sfcell_asec, cell_asec'
    assert source.count(deleted)==1
    # CASA deletes the scalar later needed by the seed+TP PSF plot. This
    # restores only its display scale, from the selected imaging cell.
    source=source.replace('psfim = imagename + ".psf"','psfim = imagename.removesuffix(".feather") + ".psf"')
    source=source.replace(deleted,deleted+"\n                        cell_asec = qa.convert(cell[0], 'arcsec')['value']")
    namespace=dict(task.__globals__)
    exec(compile(source,'CASA-TP-PSF-display-cell-control','exec'),namespace)
    return namespace['simanalyze'],defaults


def save_result(root,result):
    with gzip.open(root/'simulation_task_graphics_casa.json.gz','wt') as output:json.dump(result,output,separators=(',',':'))


def main():
    from casatasks import feather,simalma,version_string
    from casatools import table,msmetadata
    from matplotlib.figure import Figure
    from matplotlib import pyplot as plt
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--parameters',type=Path,help='upstream full-recipe parameters from simalma_oracle.py')
    parser.add_argument('--native-noise',type=Path,help='explicit common stochastic input, as in the calculation comparison')
    parser.add_argument('--case',choices=['all','simalma','multi','tp','seed','feather'],default='all')
    args=parser.parse_args()
    if args.parameters:args.parameters=args.parameters.resolve()
    if args.native_noise:args.native_noise=args.native_noise.resolve()
    root=args.out.resolve();root.mkdir(parents=True,exist_ok=True)
    os.chdir(root)
    assert version_string()=='6.7.6.14'
    physical_sf_beam()
    captured={};original=Figure.savefig
    def save(figure,filename,*args,**kwargs):
        captured[Path(filename).name]=artists(figure)
        return original(figure,filename,*args,**kwargs)
    Figure.savefig=save
    module=importlib.import_module('casatasks.private.task_simanalyze')
    module.gentools=lambda names:[{'tb':table,'msmd':msmetadata}[name]() for name in names]
    module.feather=feather
    existing=root/'simulation_task_graphics_casa.json.gz'
    if args.case!='all' and existing.exists():
        with gzip.open(existing,'rt') as source:result=json.load(source)
    else:result=dict(casa_version=version_string(),simalma={},simanalyze={})
    if args.case in ['all','simalma']:
        defaults={k:v.default for k,v in inspect.signature(simalma).parameters.items()}
        if args.parameters:
            p=json.loads(args.parameters.resolve().read_text())
            p['graphics']='file'
            for name in ['mapsize','totaltime']:
                if isinstance(p[name],str):p[name]=[p[name]]
        else:
            model=small_inputs(root)
            p=dict(project='combo',skymodel=str(model),antennalist=[str(root/'inputs/alma.test.cfg'),str(root/'inputs/aca.test.cfg')],totaltime=['60s'],tpnant=2,tptime='60s',pwv=0.,imsize=[128,128],mapsize=['12arcsec'],dryrun=False,overwrite=True,graphics='file')
        import simalma_oracle
        simalma_oracle.corrected_simanalyze=analysis_graphics_task
        noise_control=simalma_oracle.shared_noise(args.native_noise.resolve(),root) if args.native_noise else None
        corrected_simalma(noise_control)(**dict(defaults,**p))
        result['simalma']=dict(parameters=p,figures=dict(captured))
        save_result(root,result)
        captured.clear();plt.close('all')
    repo=Path(__file__).resolve().parents[2]
    if args.case=='all':
        with tarfile.open(repo/'libs/casa/casa-simulation/tests/data/simanalyze_multi_inputs.tar.gz') as archive:archive.extractall(root,filter='data')
    with gzip.open(repo/'libs/casa/casa-simulation/tests/data/simanalyze_multi_casa.json.gz','rt') as source:cases=json.load(source)['cases']
    task,defaults=analysis_graphics_task()
    import glob
    from types import SimpleNamespace
    task.__globals__['glob']=SimpleNamespace(glob=lambda pattern:sorted(glob.glob(pattern)))
    for case in ['multi','tp','seed','feather']:
        if args.case not in ['all',case]:continue
        if args.case!='all':
            import shutil
            for path in (root/case).iterdir():
                if path.is_dir() and not path.name.endswith(('.ms','.skymodel')):shutil.rmtree(path)
        p=dict(cases[case]['parameters'],graphics='file')
        if case=='tp':p['showresidual']=True
        task(**dict(defaults,**p))
        result['simanalyze'][case]=dict(parameters=p,figures=dict(captured))
        save_result(root,result)
        captured.clear();plt.close('all')
    Figure.savefig=original
    with gzip.open(root/'simulation_task_graphics_casa.json.gz','wt') as output:json.dump(result,output,separators=(',',':'))
    print('Recorded',len(result['simalma']['figures']),'simalma figures and',sum(len(v['figures']) for v in result['simanalyze'].values()),'simanalyze figures',flush=True)


if __name__=='__main__':main()
