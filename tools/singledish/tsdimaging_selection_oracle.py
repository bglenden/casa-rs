#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA controls for selected native channels and declared brightness units.

--input is the small map from tsdimaging_oracle.py. All mutations use private
copies. References retain complete image/weight pixels and spectral metadata.
"""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import tarfile

import numpy as np
from casatasks import tsdimaging, version_string
from casatools import image, table
from tsdimaging_oracle import record_image


def duplicate_first(path):
    tool = table()
    tool.open(str(path), nomodify=False)
    tool.addrows(1)
    for column in tool.colnames():
        if tool.iscelldefined(column, 0):
            tool.putcell(column, 1, tool.getcell(column, 0))
    return tool


def intent_order(path):
    tool = duplicate_first(path/'SPECTRAL_WINDOW')
    for column in ('CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
        tool.putcell(column, 0, tool.getcell(column, 0)[:1])
    tool.putcell('NUM_CHAN', 0, 1)
    tool.close()
    tool = duplicate_first(path/'DATA_DESCRIPTION')
    tool.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    tool.close()
    tool = duplicate_first(path/'STATE')
    tool.putcell('OBS_MODE', 1, 'CALIBRATE_ATMOSPHERE#OFF_SOURCE')
    tool.close()
    tool.open(str(path), nomodify=False)
    tool.putcol('DATA_DESC_ID', np.ones(tool.nrows(), dtype=np.int32))
    tool.putcell('DATA_DESC_ID', 0, 0)
    tool.putcell('STATE_ID', 0, 1)
    for column in ('DATA', 'CORRECTED_DATA', 'MODEL_DATA', 'FLAG'):
        description = tool.getcoldesc(column)
        values = [tool.getcell(column, row) for row in range(tool.nrows())]
        tool.removecols(column)
        description.update(dataManagerGroup='selection_'+column, dataManagerType='StandardStMan')
        tool.addcols({column: description})
        for row, value in enumerate(values):
            tool.putcell(column, row, value[:, :1] if row == 0 else value)
    tool.close()


def units(path, case):
    tool = table()
    tool.open(str(path), nomodify=False)
    tool.putcolkeyword('DATA', 'QuantumUnits', ['Jy'])
    tool.putcolkeyword('CORRECTED_DATA', 'QuantumUnits', ['K'])
    if case == 'corrected_unit':
        tool.putcolkeyword('CORRECTED_DATA', 'QuantumUnits', ['Jy'])
        tool.putcolkeyword('CORRECTED_DATA', 'UNIT', ' K ')
    elif case in ('float_precedence', 'float_fallback'):
        description = tool.getcoldesc('DATA')
        description.update(name='FLOAT_DATA', valueType='float')
        tool.addcols({'FLOAT_DATA': description})
        tool.putcol('FLOAT_DATA', tool.getcol('DATA').real.astype(np.float32))
        tool.putcolkeyword('FLOAT_DATA', 'QuantumUnits', ['K'])
        if case == 'float_precedence':
            tool.removecols('CORRECTED_DATA')
        else:
            tool.removecolkeyword('CORRECTED_DATA', 'QuantumUnits')
    tool.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == '6.7.6.14'
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    records = []
    names = ('intent_order', 'corrected_quantum', 'corrected_unit', 'float_precedence', 'float_fallback', 'explicit_lowercase_k')
    for name in names:
        path = work/(name+'.ms')
        shutil.rmtree(path, ignore_errors=True)
        shutil.copytree(args.input, path)
        if name == 'intent_order':
            intent_order(path)
        else:
            units(path, name)
        output = work/name
        parameters = dict(infiles=str(path), outfile=str(output), overwrite=True,
                          imsize=[40, 40], cell='3arcsec', phasecenter='J2000 270deg -23deg', gridfunction='BOX')
        if name == 'explicit_lowercase_k':
            parameters['brightnessunit'] = 'k'
        try:
            tsdimaging(**parameters)
        except Exception as error:
            if name != 'explicit_lowercase_k':
                raise
            records.append(dict(name=name, parameters=parameters,
                                error=dict(type=type(error).__name__, message=str(error))))
            continue
        tool = image()
        tool.open(str(output)+'.image')
        coordinates = tool.coordsys()
        wcs = {key: getattr(coordinates, key)()['numeric'].tolist()
               for key in ('referencevalue', 'increment', 'referencepixel')}
        wcs['frame'] = list(coordinates.referencecode())
        wcs['rest_frequency_hz'] = float(coordinates.restfrequency()['value'][0])
        coordinates.done()
        tool.close()
        records.append(dict(name=name, parameters=parameters, wcs=wcs,
                            image=record_image(str(output)+'.image'), weight=record_image(str(output)+'.weight')))
    with gzip.open(args.reference, 'wt') as output:
        json.dump(dict(casa_version=version_string(), cases=records), output, default=lambda value: value.item())
    with tarfile.open(args.archive, 'w:gz') as archive:
        for name in names:
            archive.add(work/(name+'.ms'), arcname=name+'.ms', filter=lambda info: None if info.name.endswith('/table.lock') else info)


if __name__ == '__main__':
    main()
