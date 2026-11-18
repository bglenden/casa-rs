#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record componentlist direction labels and conversions from CASA 6.7.6.14."""
from casaconfig import config
config.data_auto_update=False;config.measures_auto_update=False
from casatools import componentlist,version_string
import json
import tarfile, tempfile
from pathlib import Path
import argparse
parser=argparse.ArgumentParser(description="Probe CASA direction label conversion and persistence")
parser.add_argument('--out',type=Path,required=True)
args=parser.parse_args()
frames=['J2000','B1950_VLA','BMEAN','BTRUE','MERCURY','VENUS','MARS','JUPITER','SATURN','URANUS','NEPTUNE','PLUTO','SUN','MOON','COMET']
def snapshot(c):
 r=c.getrefdir(0);return {k:{'value':v['value'],'unit':v['unit']} if isinstance(v,dict) else v for k,v in r.items()}
cases=[]
for frame in frames:
 for method in ['setrefdirframe','convertrefdir']:
  c=componentlist();c.addcomponent(dir='J2000 10deg 20deg')
  try:out={'result':getattr(c,method)(0,frame)}
  except Exception as e:out={'error':str(e)}
  cases.append({'frame':frame,'method':method,**out,'after':snapshot(c)});c.done()
for source in ['SUN','MARS','COMET']:
 for target in ['J2000',source,'SUN','MARS']:
  c=componentlist();c.addcomponent(dir='J2000 10deg 20deg');c.setrefdirframe(0,source)
  try:out={'result':c.convertrefdir(0,target)}
  except Exception as e:out={'error':str(e)}
  cases.append({'source':source,'frame':target,'method':'convertrefdir',**out,'after':snapshot(c)});c.done()
with tempfile.TemporaryDirectory(dir=args.out.resolve().parent) as directory:
 c=componentlist()
 for frame in frames:
  c.addcomponent(dir='J2000 10deg 20deg',label=frame)
  c.setrefdirframe(c.length()-1,frame)
 c.rename(str(Path(directory)/'frames.cl'));c.done()
 with tarfile.open(args.out.with_suffix('.cl.tgz'),'w:gz') as archive:archive.add(Path(directory)/'frames.cl',arcname='frames.cl')
Path(args.out).write_text(json.dumps({'casatools':version_string(),'cases':cases},indent=2)+'\n')

