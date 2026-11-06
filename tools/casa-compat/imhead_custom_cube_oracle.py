#!/usr/bin/env python3
"""Capture CASA higher-dimensional custom-array operations."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, table, version_string


def plain(value):
    if hasattr(value,'tolist'): return value.tolist()
    if isinstance(value,dict): return {str(k):plain(v) for k,v in value.items()}
    if isinstance(value,(tuple,list)): return [plain(v) for v in value]
    return value


def main():
    root=Path('crates/casars-imexplore/tests/data/imhead_crpix')
    arrays=[[[[1,2,3,4],[5,6,7,8],[9,10,11,12]],[[13,14,15,16],[17,18,19,20],[21,22,23,24]]],
            [[[True,False],[False,True]],[[False,True],[True,False]]],
            [[['a','b','c','d'],['e','f','g','h'],['i','j','k','l']],[['m','n','o','p'],['q','r','s','t'],['u','v','w','x']]],
            [[[1,'b'],[3.5,'d']],[[5,'f'],[7.0,'h']]],
            [[[[1,2],[3,4]]],[[[5,6],[7,8]]]]]
    for name,value in [('int',arrays[0]),('string',arrays[2])]:
        path=root/f'custom_cube_{name}.im'
        if path.exists(): shutil.rmtree(path)
        shutil.copytree(root/'normal.im',path); imhead(str(path),mode='put',hdkey='custom',hdvalue=value)
        for lock in path.rglob('table.lock'): lock.unlink()
        for info in path.rglob('table.info'): info.write_text('\n'.join(line.rstrip() for line in info.read_text().splitlines()).rstrip()+'\n')
    cases=arrays+[{'cube':value} for value in arrays]
    expected={'casa_version':version_string(),'cases':[]}
    with tempfile.TemporaryDirectory(prefix='imhead-custom-cube-') as temp:
        for index,value in enumerate(cases):
            path=Path(temp)/f'{index}.im';shutil.copytree(root/'normal.im',path)
            ia=image();ia.open(str(path));count=len(ia.history(list=False));ia.done()
            imhead(str(path),mode='put',hdkey='custom',hdvalue=value)
            ia=image();ia.open(str(path));history=[line.replace(str(path),'<IMAGE>') for line in ia.history(list=False)[count:]];ia.done()
            tb=table();tb.open(str(path));stored=tb.getkeyword('miscinfo')['custom'];tb.close()
            case={'value':value,'history':history,'stored':plain(stored)}
            if isinstance(stored,np.ndarray):case['shape']=list(stored.shape);case['kind']=stored.dtype.kind
            else:case['get']=plain(imhead(str(path),mode='get',hdkey='custom'))
            expected['cases'].append(case)
    (root/'custom_cube_expected.json').write_text(json.dumps(expected,indent=2,sort_keys=True)+'\n')


if __name__ == '__main__':main()
