#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 controls for the tsdimaging observation adapter (#938).

--input is the map produced by tsdimaging_oracle.py. All mutations are on
private copies under --work; the source measurement set is preserved.
"""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import numpy as np
from casatasks import tsdimaging, version_string
from casatools import measures, table
from tsdimaging_oracle import record_image, CENTRE, IMSIZE, CELL_ARCSEC


def change(path, case):
    tool = table()
    if case == 'float_data':
        tool.open(str(path), nomodify=False)
        description = tool.getcoldesc('DATA')
        description.update(name='FLOAT_DATA', valueType='float')
        tool.addcols({'FLOAT_DATA': description})
        tool.putcol('FLOAT_DATA', tool.getcol('DATA').real.astype(np.float32))
        tool.removecols([name for name in ('DATA', 'CORRECTED_DATA', 'MODEL_DATA') if name in tool.colnames()])
        tool.close()
        return
    if case == 'flagged_state':
        tool.open(str(path/'STATE'), nomodify=False)
        tool.putcol('FLAG_ROW', np.ones(tool.nrows(), dtype=bool))
        tool.close()
        return
    if case == 'outside_coverage':
        tool.open(str(path), nomodify=False)
        for column in ['TIME', 'TIME_CENTROID']:
            values = tool.getcol(column); values[:10] += 10000.
            tool.putcol(column, values)
        tool.close()
        return
    tool.open(str(path/'POINTING'), nomodify=False)
    if case == 'empty_pointing':
        tool.removerows(list(range(tool.nrows())))
    elif case == 'between_pointings':
        keep = set(range(0, tool.nrows(), 4)) | {tool.nrows()-1}
        tool.removerows([row for row in range(tool.nrows()) if row not in keep])
        tool.putcol('INTERVAL', np.full(tool.nrows(), 4.))
    elif case == 'target_column':
        direction = tool.getcol('DIRECTION')
        direction[0] += 0.01
        tool.putcol('TARGET', direction)
    elif case == 'azel':
        direction = tool.getcol('DIRECTION'); times = tool.getcol('TIME')
        antenna = tool.getcol('ANTENNA_ID')
        ant = table(); ant.open(str(path/'ANTENNA'))
        positions = ant.getcol('POSITION'); ant.close()
        frame = measures()
        for row in range(tool.nrows()):
            x,y,z = positions[:,antenna[row]]
            frame.doframe(frame.position('ITRF', f'{x}m', f'{y}m', f'{z}m'))
            frame.doframe(frame.epoch('UTC', f'{times[row]/86400.}d'))
            source = frame.direction('J2000', f'{direction[0,0,row]}rad', f'{direction[1,0,row]}rad')
            converted = frame.measure(source, 'AZEL')
            direction[:,0,row] = [converted['m0']['value'], converted['m1']['value']]
        tool.putcol('DIRECTION', direction)
        info = tool.getcolkeyword('DIRECTION', 'MEASINFO'); info['Ref']='AZEL'
        tool.putcolkeyword('DIRECTION', 'MEASINFO', info)
    tool.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    assert version_string()=='6.7.6.14'
    source=args.input.resolve();work=args.work.resolve();work.mkdir(parents=True,exist_ok=True)
    cases=[]
    for name in ['azel','empty_pointing','outside_coverage','between_pointings','target_column','flagged_state','float_data']:
        path=work/(name+'.ms');shutil.rmtree(path,ignore_errors=True);shutil.copytree(source,path)
        change(path,name)
        outfile=work/name
        parameters=dict(infiles=[str(path)],outfile=str(outfile),overwrite=True,
                        imsize=IMSIZE,cell=f'{CELL_ARCSEC}arcsec',phasecenter=CENTRE,
                        gridfunction='SF',convsupport=6)
        if name=='target_column': parameters['pointingcolumn']='target'
        record=dict(name=name,parameters=parameters)
        try:
            record['return']=tsdimaging(**parameters)
            record['image']=record_image(str(outfile)+'.image')
            record['weight']=record_image(str(outfile)+'.weight')
        except Exception as error:
            record['error']=dict(type=type(error).__name__,message=str(error))
        if name == 'empty_pointing' and 'error' in record:
            from casatasks.private.task_tsdimaging import do_weight_mask
            image_path, weight_path = str(outfile)+'.image', str(outfile)+'.weight'
            if Path(image_path).exists() and Path(weight_path).exists():
                record['raw_image'] = record_image(image_path)
                do_weight_mask(image_path, weight_path, 0.1)
                record['control'] = 'Complete the normal weight mask after CASA beam sampling crashes on empty POINTING; no beam is invented.'
                record['image'] = record_image(image_path)
                record['weight'] = record_image(weight_path)
        cases.append(record)
        print(name,record.get('error','products written'),flush=True)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    with gzip.open(args.out,'wt') as output:
        json.dump(dict(casa_version=version_string(),cases=cases),output,default=lambda value:value.item())


if __name__=='__main__':main()
