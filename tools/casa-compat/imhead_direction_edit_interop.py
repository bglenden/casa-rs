#!/usr/bin/env python3
"""Have CASA reopen native imhead direction-frame edits and FITS exports."""
import argparse
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead, exportfits
from casatools import image, quanta, table, version_string
from imhead_direction_edit_oracle import fits_fields


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k):plain(v) for k,v in value.items()}
    if isinstance(value, (tuple,list)): return [plain(v) for v in value]
    return value


def invoke(binary,path,key,value):
    request = {'surface':'imhead','values':{'imagename':str(path),'mode':'put','hdkey':key,'hdvalue':value}}
    return subprocess.run([str(binary),'imhead','--json-run','-'],input=json.dumps(request),text=True,capture_output=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('binary',type=lambda path:Path(path).resolve()); args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix'); expected = json.loads((root/'direction_edit_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-direction-interop-') as temp:
        for index,case in enumerate(expected['cases']):
            path=Path(temp)/f'{index}.im'; shutil.copytree(root/f"{case['image']}.im",path)
            if 'initial' in case:
                initial=invoke(args.binary,path,'equinox',case['initial']); assert initial.returncode==0,(index,initial.stderr)
            tb=table();tb.open(str(path)); before=plain(tb.getkeyword('coords'));tb.close()
            ia=image();ia.open(str(path));count=len(ia.history(list=False));ia.done()
            result=invoke(args.binary,path,case['key'],case['value'])
            assert (result.returncode!=0)==('error' in case),(index,result.stderr)
            assert imhead(str(path),mode='get',hdkey='equinox')==case['get'],index
            assert imhead(str(path),mode='get',hdkey='minpos')==case['minpos'],index
            tb.open(str(path));after=plain(tb.getkeyword('coords'));tb.close()
            before['direction0']['system']=case['get']; assert before==after,index
            ia=image();ia.open(str(path));history=ia.history(list=False)[count:]
            assert len(history)==len(case['history']),(index,history)
            if len(history)==3: assert history[0]==case['history'][0],(index,history)
            cs=ia.coordsys();rec=cs.torecord();cs.done();qa=quanta()
            for point,target in zip(([0.0,0.0],[2.5,1.0]),case['world']):
                pixel=[0.0]*len(ia.shape())
                for axis in range(2): pixel[int(rec['pixelmap0'][axis])]=point[axis]
                values=ia.toworld(pixel,format='q')['quantity']
                actual=[qa.convert(values[f'*{int(rec["worldmap0"][axis])+1}'],'rad')['value'] for axis in range(2)]
                assert all(math.isclose(a,b,abs_tol=5e-11) for a,b in zip(actual,target)),(index,actual,target)
            ia.done()
            if 'fits' in case:
                fits=Path(temp)/f'{index}.fits';exportfits(str(path),fitsimage=str(fits),overwrite=True)
                assert fits_fields(fits)==case['fits'],(index,fits_fields(fits),case['fits'])
    print(f"CASA {version_string()} reopened {len(expected['cases'])} direction-frame edit results")


if __name__ == '__main__':main()
