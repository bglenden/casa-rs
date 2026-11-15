#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent CASA 6.7.6.14 simalma calculation oracle.

Restores legacy Python tool factories and callables, the accepted SF beam,
full-precision fidelity floor, and Python-3 cycle-name comparison. The task's
calculation parameters and formulas otherwise remain CASA's.
"""
import argparse
import importlib
import inspect
import json
import os
from pathlib import Path
import time
from simanalyze_multi_oracle import physical_sf_beam
from simanalyze_task_oracle import corrected_simanalyze


def corrected_simalma(noise_control=None):
    from casatasks import feather
    module = importlib.import_module('casatasks.private.task_simalma')
    source = inspect.getsource(module.simalma)
    source = source.replace("if cycles[i]>'-1':", "if str(cycles[i])>'-1':")
    task, defaults = corrected_simanalyze()
    # Accepted Phase 6 discovery rule: sort candidates, so auxiliary model
    # products are attached to the same sky model in both implementations.
    import glob
    from types import SimpleNamespace
    task.__globals__['glob']=SimpleNamespace(glob=lambda pattern:sorted(glob.glob(pattern)))
    namespace = dict(vars(module))
    namespace.update(feather=feather, simanalyze=lambda **values: task(**dict(defaults, **values)))
    if noise_control is not None:namespace['simobserve']=noise_control
    exec(compile(source, 'CASA-documented-simalma-Python3-control', 'exec'), namespace)
    return namespace['simalma']


def shared_noise(native_root,output):
    """CASA computes reference images from identical stochastic DATA.

    Clean prediction, weights and thermal noise scales remain independent
    comparisons. CASA and casa-rs have different random streams; their noisy
    pixels cannot be compared sample by sample without a common input.
    """
    import numpy as np
    from casatasks import simobserve
    from casatools import table
    records=[]
    def observe(**parameters):
        simobserve(**parameters)
        project=parameters['project']
        tp=parameters['obsmode']=='sd'
        config=parameters['sdantlist'] if tp else parameters['antennalist']
        stem=project+'.'+Path(config).name.replace('.cfg','').replace(';','_')
        suffix='.noisy.sd.ms' if tp else '.noisy.ms'
        casa=Path(project)/(stem+suffix)
        source=native_root/(stem+suffix+(('.Ant%d'%parameters['sdant']) if tp else ''))
        clean=Path(project)/(stem+('.sd.ms' if tp else '.ms'))
        values=[]
        for path in [clean,casa,source]:
            tb=table();tb.open(str(path));values.append(dict(data=tb.getcol('DATA'),flags=tb.getcol('FLAG'),sigma=tb.getcol('SIGMA')));tb.close()
        baseline,cpp,rust=values
        assert cpp['data'].shape==rust['data'].shape
        valid=~cpp['flags']
        cpp_noise=(cpp['data']-baseline['data'])[valid]
        rust_noise=(rust['data']-baseline['data'])[valid]
        cpp_rms=float(np.sqrt(np.mean(np.abs(cpp_noise)**2)))
        rust_rms=float(np.sqrt(np.mean(np.abs(rust_noise)**2)))
        ratio=rust_rms/cpp_rms
        np.testing.assert_allclose(rust['sigma'],cpp['sigma'],rtol=1e-6,atol=0.)
        assert abs(ratio-1)<0.05,(source,ratio)
        records.append(dict(product=source.name,casa_noise_rms=cpp_rms,native_noise_rms=rust_rms,ratio=ratio))
        (output/'noise-comparison.json').write_text(json.dumps(records,indent=2))
        tb=table();tb.open(str(casa),nomodify=False)
        for name in ['DATA','CORRECTED_DATA']:
            if name in tb.colnames():tb.putcol(name,rust['data'])
        tb.close()
    return observe


# Avoid "pa" in the project prefix: CASA's documented image-expression
# bug mislabels those results as position angle and changes the Stokes axis.
def small_inputs(root):
    import numpy as np
    from casatools import image
    from simobserve_graphics_oracle import inputs
    directory=root/'inputs';directory.mkdir(exist_ok=True)
    model,config=inputs(directory)
    ia=image();ia.open(str(model));cs=ia.coordsys();ia.done()
    cs.setreferencepixel([96,96],type='direction')
    pixels=np.zeros((192,192,1,1),dtype=np.float32)
    pixels[96,96,0,0]=1.25;pixels[105,92,0,0]=0.75
    ia.fromarray(outfile=str(model),pixels=pixels,csys=cs.torecord(),overwrite=True)
    ia.setbrightnessunit('Jy/pixel');ia.done();cs.done()
    (directory/'alma.test.cfg').write_text(config.read_text())
    (directory/'aca.test.cfg').write_text(config.read_text().replace('observatory=ALMA','observatory=ACA').replace('12.0','7.0'))
    from casatools import ctsys
    import shutil
    repository=directory/'alma/simmos';repository.mkdir(parents=True,exist_ok=True)
    shutil.copy(ctsys.resolve('alma/simmos/aca.tp.cfg'),repository/'aca.tp.cfg')
    return model


def record_small(root):
    import gzip,tarfile
    from simanalyze_multi_oracle import record_image
    from casatools import table
    result=dict(casa_version='6.7.6.14',parameters=json.loads((root/'parameters.json').read_text()),images={},measurementsets={})
    for path in sorted((root/'combo').iterdir()):
        if not path.is_dir() or not (path/'table.dat').exists():continue
        if (path/'ANTENNA').is_dir():
            tb=table();tb.open(str(path))
            result['measurementsets'][path.name]={}
            for name in ['DATA','UVW','WEIGHT','SIGMA','TIME','FLAG','FLAG_ROW']:
                data=tb.getcol(name)
                result['measurementsets'][path.name][name]=dict(shape=list(data.shape),real=data.real.tolist(),imag=data.imag.tolist() if data.dtype.kind=='c' else None)
            tb.close()
        else:
            import numpy as np
            record=record_image(path)
            pixels=np.asarray(record['pixels']);mask=np.asarray(record['mask'])
            nonfinite=~np.isfinite(pixels)
            assert not np.any(nonfinite & mask), (path,'non-finite defined sample')
            # JSON has no NaN. Undefined masked samples are explicit nulls;
            # the regression still requires exact masks and every valid pixel.
            serial=pixels.astype(object);serial[nonfinite]=None
            record['pixels']=serial.tolist()
            result['images'][path.name]=record
    with gzip.open(root/'simalma_casa.json.gz','wt') as output:json.dump(result,output,separators=(',',':'))
    with tarfile.open(root/'simalma_inputs.tar.gz','w:gz') as archive:archive.add(root/'inputs',arcname='inputs')


def main():
    from casatasks import simalma, version_string
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--model',type=Path)
    parser.add_argument('--pwv',type=float,default=0.)
    parser.add_argument('--small',action='store_true',help='record the compact integration fixture')
    parser.add_argument('--native-noise',type=Path,help='use native noisy DATA as common stochastic input to CASA imaging, after checking independent noise statistics')
    args=parser.parse_args()
    root=args.out.resolve();root.mkdir(parents=True,exist_ok=True)
    model=args.model.resolve() if args.model else None
    if args.small:model=small_inputs(root)
    if model is None:raise ValueError('--model is required for the upstream regression')
    parameters=dict(project='m51',overwrite=True,skymodel=str(model),
        indirection='J2000 23h59m59.96s -34d59m59.50s',incell='0.1arcsec',inbright='0.004',incenter='330.076GHz',inwidth='50MHz',
        antennalist=['alma.cycle6.3.cfg','aca.cycle6.cfg'],totaltime='1800s',tpnant=2,tptime='7200s',pwv=args.pwv,imsize=[128,128],mapsize='1arcmin',dryrun=False,graphics='none')
    if args.small:
        parameters.update(project='combo',indirection='',incell='',inbright='',incenter='',inwidth='',antennalist=[str(root/'inputs/alma.test.cfg'),str(root/'inputs/aca.test.cfg')],totaltime='60s',tptime='60s',mapsize='12arcsec')
    (root/'parameters.json').write_text(json.dumps(parameters,indent=2))
    defaults={name:parameter.default for name,parameter in inspect.signature(simalma).parameters.items()}
    # CASA's public validator converts vector parameters before calling private.
    for name in ['mapsize','totaltime']:
        if isinstance(parameters[name],str):parameters[name]=[parameters[name]]
    noise_control=None
    if args.native_noise is not None:noise_control=shared_noise(args.native_noise.resolve(),root)
    os.chdir(root)
    physical_sf_beam()
    started=time.perf_counter()
    corrected_simalma(noise_control)(**dict(defaults,**parameters))
    (root/'timing.json').write_text(json.dumps(dict(casa_version=version_string(),elapsed_s=time.perf_counter()-started)))
    if args.small:record_small(root)


if __name__=='__main__':main()
