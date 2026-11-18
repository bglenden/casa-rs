#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 componentlist sorting and diagnostics."""
from casaconfig import config
config.data_auto_update=False;config.measures_auto_update=False
from casatools import componentlist,version_string
import json
from pathlib import Path
import argparse
parser=argparse.ArgumentParser(description="Record CASA component orderings")
parser.add_argument('--out',type=Path,required=True)
args=parser.parse_args()
entries=[([1,0.5,0,0],'J2000 20deg 0deg','first','4arcsec'),([-3,0,0,0],'J2000 0deg 0deg','negative','3arcmin'),([2,1.5,0,0],'J2000 10deg 0deg','polarized','2deg'),([0,1,0,0],'J2000 100deg 0deg','zero','1arcsec')]
cases=[]
for criteria in ['Flux','Position','Polarization','fLuX','flux\0ignored','bad','f','polarisation','']:
 c=componentlist()
 for flux,direction,label,major in entries:c.addcomponent(flux=flux,dir=direction,label=label,shape='Gaussian',majoraxis=major,minoraxis='0.5arcsec',positionangle='10deg')
 c.select([0,2])
 try:result={'result':c.sort(criteria)}
 except Exception as e:result={'error':{'type':type(e).__name__,'message':str(e)}}
 cases.append({'criteria':criteria,**result,'labels':[c.getlabel(i) for i in range(c.length())],'selected':c.selected().tolist()});c.done()
args.out.write_text(json.dumps({'casatools':version_string(),'entries':entries,'cases':cases},indent=2)+'\n')
