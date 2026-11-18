#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA retained read locks using an independent external native writer.

Run with CASA 6.7.6.14 Python; --native-python names a Python with casars
installed. The worker is initialized before cl.open to exclude startup delays.
"""
from casaconfig import config
config.data_auto_update=False;config.measures_auto_update=False
from casatools import componentlist, table, version_string
assert version_string() == "6.7.6-14"
import tempfile,subprocess,json
from pathlib import Path
code='''
import json,sys
from casars.tools import table
print('READY',flush=True)
for line in sys.stdin:
 request=json.loads(line);t=table();t.open(request['path'],nomodify=False,lockoptions={'option':'usernoread'});locked=t.lock(write=True,nattempts=1)
 if locked:
  t.putkeyword('counter',43);t.putkeyword('added',99)
 t.close();print(json.dumps({'locked':locked}),flush=True)
'''
import argparse
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--native-python', required=True)
parser.add_argument('--out', type=Path, required=True)
args=parser.parse_args()
worker=subprocess.Popen([args.native_python,'-u','-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
assert worker.stdout.readline().strip()=='READY';out={'casatools':version_string()}
try:
 with tempfile.TemporaryDirectory() as d:
  p=str((Path(d)/'read.cl').resolve());cl=componentlist();cl.addcomponent();cl.rename(p);cl.putkeyword('counter',42);cl.done()
  def probe():
   worker.stdin.write(json.dumps({'path':p})+'\n');worker.stdin.flush();return json.loads(worker.stdout.readline())
  cl.open(p,True);out['after_open']=probe();out['has_added']=cl.haskeyword('added');out['after_has']=probe();out['get']=cl.getkeyword('counter');out['after_get']=probe();out['clearlocks']=table().clearlocks();out['after_clearlocks']=probe();out['get_after_publication']=cl.getkeyword('counter');out['has_after_publication']=cl.haskeyword('added');cl.done();out['after_close']=probe()
finally:
 worker.stdin.close();worker.wait(timeout=5)
args.out.write_text(json.dumps(out,indent=2)+'\n');print(out)
