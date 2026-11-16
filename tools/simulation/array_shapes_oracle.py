#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Generate the independent synthetic IncrementalStMan shape fixture."""
import argparse
import json
from pathlib import Path
import numpy as np
from casatools import table
import casatasks

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--template",type=Path,required=True,help="Synthetic CASA MS containing DATA")
parser.add_argument("--out",type=Path,required=True)
args=parser.parse_args()
assert casatasks.version_string()=="6.7.6.14"
source=table()
source.open(str(args.template))
descriptor=source.getcoldesc("DATA")
source.close()
descriptor.pop("shape",None)
descriptor.update(ndim=2,option=0,dataManagerType="IncrementalStMan",dataManagerGroup="Shapes")
tool=table()
tool.create(str(args.out),{"DATA":descriptor},nrow=5)
for row,shape in ((1,(2,1)),(3,(1,5)),(4,(2,4))):
    tool.putcell("DATA",row,np.full(shape,7,dtype=np.complex64))
tool.close()
tool.open(str(args.out))
shapes=[list(tool.getcell("DATA",row).shape) if tool.iscelldefined("DATA",row) else None for row in range(5)]
tool.close()
assert shapes==[None,[2,1],[2,1],[1,5],[2,4]]
(args.out/"reference.json").write_text(json.dumps({"casa_version":casatasks.version_string(),"shapes":shapes},indent=1)+"\n")
