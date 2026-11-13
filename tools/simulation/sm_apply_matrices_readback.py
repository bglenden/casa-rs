#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA reads native full-polarized corruptions and consumes their input tables."""
import argparse
import gzip
import json
from pathlib import Path
import numpy as np
from casatools import table, simulator
from casatasks import version_string
from sm_apply_parameters_oracle import initialize
from sm_apply_parameters_readback import compare
from sm_multiple_models_oracle import make_session
from sm_apply_matrices_oracle import parameters, initialize_matrix_data


def check_table(path, case):
    tb=table();tb.open(str(path))
    try:
        assert tb.nrows()==case['calibration_rows']
        assert tb.info()['subType']==tb.getkeyword('VisCal')==case['kind']+' Jones'
        values,flags=tb.getcol('CPARAM'),tb.getcol('FLAG')
        expected_shape=(len(parameters(case['kind'],0,0)),case['channels'],case['calibration_rows'])
        assert values.shape==flags.shape==expected_shape
        for row,antenna in enumerate(tb.getcol('ANTENNA1')):
            for channel in range(case['channels']):
                np.testing.assert_array_equal(values[:,channel,row],parameters(case['kind'],antenna,channel).astype(np.complex64))
            expected=np.zeros(expected_shape[:2],dtype=bool)
            if case.get('flag') and antenna==2:expected[1,0]=True
            np.testing.assert_array_equal(flags[:,:,row],expected)
    finally:tb.close()


def consume(directory, case, output):
    sm=make_session(output,stokes='XX XY YX YY');sm.close();initialize(output);initialize_matrix_data(output)
    sm=simulator();assert sm.openfromms(str(output))
    if case.get('leakage'):assert sm.setapply(table=str((directory/'leakage.cal').resolve()),interp='linear',calwt=False)
    assert sm.setapply(table=str((directory/'terms.cal').resolve()),interp='linear,nearest',calwt=False)
    assert sm.corrupt();sm.close();return compare(output,case)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('root','out','fixture'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();assert version_string()=='6.7.6.14'
    cases={case['name']:case for case in json.load(gzip.open(args.fixture,'rt'))['cases']}
    expected={(name,workers) for name in cases for workers in (1,3)};seen=set();reports=[]
    args.out.mkdir(parents=True,exist_ok=True)
    for manifest in sorted(args.root.glob('*/case.json')):
        run=json.loads(manifest.read_text());key=run['name'],run['workers'];assert key in expected and key not in seen;seen.add(key)
        case=cases[run['name']];directory=manifest.parent;check_table(directory/'terms.cal',case)
        report={**run,'native_ms_cpp_nrms':compare(directory/'data.ms',case)}
        if run['workers']==1:report['native_table_consumer_nrms']=consume(directory,case,args.out/(run['name']+'.ms'))
        reports.append(report)
    assert seen==expected,('missing',sorted(expected-seen))
    (args.out/'report.json').write_text(json.dumps(dict(casa_version=version_string(),runs=reports),indent=1,allow_nan=False)+'\n')
    print('CASA full-polarized table interoperability passed',len(reports),flush=True)


if __name__=='__main__':main()
