#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare installed-native/CASA configuration discovery using a 1 Jy source.

Run under CASA 6.7.6.14's Python, with the native installed wheel and task:

    CASAPY tools/simulation/simobserve_configurations_oracle.py --out OUT \\
        --native-python VENV/bin/python --native-bin TARGET/release/simobserve \\
        --measures-root DATA_ROOT

DATA_ROOT is a valid measures runtime with public configurations in alma/simmos.
Omit it to check standard ~/.casa/data discovery for the named case. The three
cases check a CASA configuration name, an ALMA resolution string selecting a
different configuration, and a configuration found inside the project. CASA
reads every generated native MS and compares metadata, geometry and DATA.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import numpy as np


def native_worker(params):
    import casars, casars._core
    from casars import tasks
    prefix=Path(sys.prefix).resolve()
    assert prefix in Path(casars._core.__file__).resolve().parents
    print('installed native extension',casars._core.__file__,flush=True)
    tasks.simobserve(**json.loads(Path(params).read_text()))


def columns(path,names):
    from casatools import table
    tb=table();tb.open(str(path))
    result={c:np.asarray([tb.getcell(c,r) for r in range(tb.nrows())]) for c in names}
    tb.close();return result


def compare(casa,native):
    details={}
    for label,names in [('ANTENNA',['NAME','STATION','MOUNT','DISH_DIAMETER','POSITION']),('MAIN',['ANTENNA1','ANTENNA2','DATA_DESC_ID','FIELD_ID','SCAN_NUMBER','TIME','UVW','DATA']),('SPECTRAL_WINDOW',['MEAS_FREQ_REF','CHAN_FREQ'])]:
        a=columns(casa if label=='MAIN' else casa/label,names)
        b=columns(native if label=='MAIN' else native/label,names)
        for name in names:
            assert a[name].shape==b[name].shape,(label,name,a[name].shape,b[name].shape)
            if name=='POSITION':
                residual=float(np.max(np.abs(a[name]-b[name]),initial=0))
                assert residual<=1e-6,(label,name,residual)
                details['max_position_m']=residual
            elif name=='UVW':
                residual=float(np.max(np.abs(a[name]-b[name]),initial=0))
                assert residual<=1e-3,(label,name,residual)
                details['max_uvw_m']=residual
            elif name=='DATA':
                residual=float(np.linalg.norm(a[name]-b[name])/max(np.linalg.norm(a[name]),1e-30))
                assert residual<1e-3,(label,name,residual)
                details['data_nrms']=residual
            elif name=='CHAN_FREQ':
                np.testing.assert_allclose(a[name],b[name],rtol=0,atol=1e-3)
            else:assert np.array_equal(a[name],b[name]),(label,name)
    details['rows']=len(columns(casa,['TIME'])['TIME'])
    return details


def record(args):
    import casatasks
    from casatasks import simobserve
    from casatools import componentlist,ctsys
    assert casatasks.version_string()=='6.7.6.14'
    root=args.out.resolve();root.mkdir()
    repository=Path(ctsys.resolve('alma/simmos'))
    cl=componentlist();cl.addcomponent(dir='J2000 19h00m00 -23d00m00',flux=1.,freq='230GHz');cl.rename(str(root/'point.cl'));cl.done()
    records=[]
    for name,configuration in [('named','alma.out01.cfg'),('resolution','alma;0.078arcsec'),('project','custom.cfg')]:
        common=dict(complist=str(root/'point.cl'),compwidth='1MHz',setpointings=True,maptype='square',mapsize='1arcsec',
                    integration='10s',direction='J2000 19h00m00 -23d00m00',obsmode='int',antennalist=configuration,
                    hourangle='transit',totaltime='10s',refdate='2024/06/01',thermalnoise='',graphics='none',overwrite=True)
        outputs={}
        for side in ['casa','native']:
            work=root/side;work.mkdir(exist_ok=True)
            params=dict(common,project=name)
            if name=='project':
                (work/name).mkdir();shutil.copyfile(repository/'alma.out01.cfg',work/name/'custom.cfg')
            if side=='casa':
                previous=Path.cwd();os.chdir(work)
                try:simobserve(**params)
                finally:os.chdir(previous)
            else:
                params['hourangle']='37.0s'  # approved TAI-labelled-UTC correction
                source=work/f'{name}.json';source.write_text(json.dumps(params))
                environment=dict(os.environ)
                environment.pop('PYTHONPATH',None)
                if args.measures_root:
                    environment['CASA_RS_MEASURESPATH']=str(args.measures_root)
                elif name=='named':
                    environment.pop('CASA_RS_MEASURESPATH',None)
                else:
                    environment['CASA_RS_MEASURESPATH']=str(repository.parent.parent)
                environment['CASARS_SIMOBSERVE_BIN']=str(args.native_bin)
                with (work/f'{name}.log').open('w') as log:
                    process=subprocess.run([str(args.native_python),str(Path(__file__).resolve()),'--native-run',str(source)],cwd=work,env=environment,stdout=log,stderr=subprocess.STDOUT)
                assert process.returncode==0,(name,process.returncode,(work/f'{name}.log').read_text()[-4000:])
            measurement_sets=list((work/name).glob('*.ms'))
            assert len(measurement_sets)==1,(side,name,measurement_sets)
            outputs[side]=measurement_sets[0]
        details=compare(outputs['casa'],outputs['native']);details.update(name=name,configuration=configuration)
        records.append(details);print('RESULT',json.dumps(details),flush=True)
    (root/'report.json').write_text(json.dumps(dict(casa_version='6.7.6.14',repository=str(repository),native_measures_root=str(args.measures_root) if args.measures_root else None,cases=records),indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--out',type=Path)
    group.add_argument('--native-run')
    parser.add_argument('--native-python',type=Path)
    parser.add_argument('--native-bin',type=Path)
    parser.add_argument('--measures-root',type=Path)
    args=parser.parse_args()
    if args.native_run:native_worker(args.native_run)
    else:record(args)


if __name__=='__main__':main()
