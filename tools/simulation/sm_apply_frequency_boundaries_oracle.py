#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Independent CASA exact-channel flag and short-axis controls (#1053)."""
from pathlib import Path
import sm_apply_frequency_oracle as oracle

extra=[{'name':f'{kind}_{mode}_{probe}','kind':kind,'interp':'nearest,'+mode,'options':{},'probe':probe} for kind in ('B','Tf') for mode,probes in [('linearflag',('flag_channel','flag_channel_descending')),('splineflag',('flag_channel','flag_channel_descending')),('cubicflag',('one_valid','one_valid_descending'))] for probe in probes]
extra += [{'name':f'{kind}_{mode}_short{channels}','kind':kind,'interp':'nearest,'+mode,'options':{},'probe':f'short{channels}_shifted'} for kind in ('B','Tf','Df') for channels in (2,3) for mode in ('linear','cubic','spline')]
original=oracle.calibration

def calibration(sm,path,kind,bounds,probe):
    from casatools import table
    short=probe.startswith('short');descending=probe.endswith('_descending')
    base='shifted' if short else probe.removesuffix('_descending')
    result=original(sm,path,kind,bounds,base)
    channels=int(probe[5]) if short else 4
    tb=table()
    if short:
        tb.open(str(path),nomodify=False)
        for column in ('CPARAM','FLAG','PARAMERR','SNR','WEIGHT'):
            if column in tb.colnames() and tb.iscelldefined(column,0):
                tb.putcol(column,tb.getcol(column)[:,:channels,:])
        tb.close()
    if short or descending:
        tb.open(str(path/'SPECTRAL_WINDOW'),nomodify=False)
        for row in range(tb.nrows()):
            for column in ('CHAN_FREQ','CHAN_WIDTH','EFFECTIVE_BW','RESOLUTION'):
                values=tb.getcell(column,row)[:channels]
                if descending and column=='CHAN_FREQ':values=values[::-1]
                tb.putcell(column,row,values)
            tb.putcell('NUM_CHAN',row,channels)
        result['frequencies_hz']=tb.getcell('CHAN_FREQ',0).tolist();tb.close()
    return {**result,'channels':channels}

oracle.CASES=extra;oracle.REFUSALS=[];oracle.calibration=calibration;oracle.__file__=__file__
if __name__=='__main__':oracle.main()
