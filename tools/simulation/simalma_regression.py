#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare every calculation product of the upstream simalma m51 recipe.

Generate CASA with simalma_oracle.py. Run under CASA Python to execute/read
the native task. The 2014 observation's 35 s physical-epoch control is the
same documented UTC correction used by the existing simulation comparisons.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import numpy as np
from simanalyze_regression import image_record,nrms


def compare(casa,native):
    from casatools import table
    from compare_sm_ms import TOLERANCES,RELATIVE_TOLERANCE
    report=dict(images={},measurementsets={},failures=[])
    for source in sorted(casa.iterdir()):
        if not source.is_dir() or not (source/'table.dat').exists():continue
        target=native/source.name
        assert target.exists(),('missing product',target)
        if (source/'ANTENNA').is_dir():
            data=[]
            for path in [source,target]:
                tb=table();tb.open(str(path))
                data.append({name:tb.getcol(name) for name in ['DATA','UVW','TIME','FLAG','FLAG_ROW','WEIGHT','SIGMA']});tb.close()
            expected,actual=data
            assert np.array_equal(expected['FLAG'],actual['FLAG'])
            assert np.array_equal(expected['FLAG_ROW'],actual['FLAG_ROW'])
            valid=~expected['FLAG'] & ~expected['FLAG_ROW'][None,None,:]
            errors={name:nrms(actual[name][valid] if name=='DATA' else actual[name],expected[name][valid] if name=='DATA' else expected[name]) for name in ['DATA','UVW','WEIGHT','SIGMA']}
            for name,error in errors.items():
                if error>1e-6:report['failures'].append(dict(product=source.name,column=name,nrms=error))
            np.testing.assert_allclose(actual['TIME'],expected['TIME'],rtol=RELATIVE_TOLERANCE,atol=TOLERANCES['TIME'])
            errors['rows']=int(len(actual['TIME']))
            report['measurementsets'][source.name]=errors
        else:
            expected,actual=image_record(source),image_record(target)
            mask=expected['mask'];equal=np.array_equal(actual['mask'],mask)
            error=nrms(actual['pixels'][mask],expected['pixels'][mask])
            if not equal or error>1e-3:report['failures'].append(dict(product=source.name,nrms=error,masks_equal=equal))
            for name in ['reference','increment']:
                np.testing.assert_allclose(actual[name],expected[name],rtol=1e-12,atol=1e-12)
            if source.name.endswith('.fidelity'):assert actual['unit']==''
            else:assert actual['unit']==expected['unit'],(source.name,actual['unit'],expected['unit'])
            report['images'][source.name]=dict(nrms=error,masks_equal=equal)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--casa',required=True,type=Path)
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--native-bin',required=True,type=Path)
    parser.add_argument('--compare-only',action='store_true')
    args=parser.parse_args()
    root=args.out.resolve();root.mkdir(parents=True,exist_ok=True)
    casa=args.casa.resolve()
    parameters=json.loads((casa/'parameters.json').read_text())
    project=Path(parameters['project']).name
    parameters.update(project=str(root/project),hourangle='35s')
    request=root/'request.json';request.write_text(json.dumps(dict(surface='simalma',values=parameters)))
    if not args.compare_only:
        started=time.perf_counter()
        result=subprocess.run([str(args.native_bin.resolve()),'--json-run',str(request)],text=True,capture_output=True)
        (root/'native-stderr.log').write_text(result.stderr);(root/'native-result.json').write_text(result.stdout)
        if result.returncode:raise RuntimeError(result.stdout,result.stderr[-4000:])
        (root/'timing.json').write_text(json.dumps(dict(elapsed_s=time.perf_counter()-started)))
    report=compare(casa/project,root/project)
    (root/'comparison.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2),flush=True)
    assert not report['failures'],report['failures']


if __name__=='__main__':main()
