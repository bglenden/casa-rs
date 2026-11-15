#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native tsdimaging with CASA 6.7.6.14 on its public datasets.

Run under CASA's Python, with --data pointing at casatestdata (or a public
subset with the same paths). CASA reads both native products, including their
masks, units and coordinates. Full arrays stay under --work, not in git.
"""
import argparse
import json
from pathlib import Path
import os
import subprocess
import time
import statistics

import numpy as np
from casatasks import tsdimaging, version_string
from casatools import image


def compare(actual, reference):
    tool = image()
    records = []
    for path in (actual, reference):
        tool.open(str(path))
        cs = tool.coordsys()
        records.append(dict(
            shape=tool.shape(), data=tool.getchunk(), mask=tool.getchunk(getmask=True),
            unit=tool.brightnessunit(), beam=tool.restoringbeam(),
            value=cs.referencevalue()['numeric'], increment=cs.increment()['numeric'],
            pixel=cs.referencepixel()['numeric'], frame=cs.referencecode(),
        ))
        cs.done()
        tool.close()
    native, casa = records
    assert np.array_equal(native['shape'], casa['shape'])
    assert native['unit'] == casa['unit'], (native['unit'], casa['unit'])
    for key in ('value', 'increment', 'pixel'):
        np.testing.assert_allclose(native[key], casa[key], rtol=1e-10, atol=1e-15, err_msg=key)
    assert list(native['frame']) == list(casa['frame'])
    error = np.linalg.norm((native['data'].astype(float)-casa['data']).ravel())
    power = np.linalg.norm(casa['data'].astype(float).ravel())
    nrms = error/power if power else error
    assert nrms <= 1e-3, (actual, nrms)
    mismatch = int(np.count_nonzero(native['mask'] != casa['mask']))
    assert mismatch == 0, (actual, 'mask', mismatch)
    return dict(nrms=float(nrms), mask_mismatches=mismatch,
                native_beam=native['beam'], casa_beam=casa['beam'])


def cases(data):
    multi = data/'regression/sim_multi_arrays_and_TP/m51c_reference'
    tool = image()
    tool.open(str(multi/'m51c.sd.image'))
    cs = tool.coordsys()
    centre = cs.referencevalue()['numeric'][:2]
    frame = cs.referencecode()[0]
    cs.done()
    tool.close()
    return [
        ('sim_multi_arrays_and_TP', dict(
            infiles=[str(multi/'m51c.aca.tp.sd.ms')],
            intent='',
            imsize=[128, 128], cell='3arcsec',
            phasecenter=f'{frame} {centre[0]:.17g}rad {centre[1]:.17g}rad',
            gridfunction='SF', convsupport=6,
        )),
        # Exact native-channel test102 task parameters; default PB kernel.
        # Its 1024 input channels are retained, without the width=10 binning
        # used by the suite's other channel tests (outside the core subset).
        ('unittest_test102', dict(
            infiles=[str(data/'measurementset/other/sdimaging.ms')],
            intent='', spw='0', imsize=[75, 75], cell=['3.0arcmin', '3.0arcmin'],
            phasecenter='J2000 17:18:29 +59.31.23', gridfunction='PB',
            nchan=1024, start=0, width=1, minweight=0.,
        )),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path)
    parser.add_argument('--native-measures', type=Path)
    parser.add_argument('--only', nargs='*')
    parser.add_argument('--reuse-casa', action='store_true', help='Reuse existing references for unchanged inputs and parameters.')
    parser.add_argument('--repeats', type=int, default=1, help='Serial complete task runs for median elapsed time.')
    args = parser.parse_args()
    assert args.repeats > 0
    assert version_string() == '6.7.6.14'
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    report = dict(casa_version=version_string(), cases=[])
    for name, parameters in cases(args.data.resolve()):
        if args.only and name not in args.only:
            continue
        record = dict(name=name, parameters=parameters)
        reference = work/(name+'-casa')
        started = time.perf_counter()
        if args.reuse_casa and Path(str(reference)+'.image').exists():
            record['casa_reference_reused'] = True
        else:
            durations = []
            for _ in range(args.repeats):
                started = time.perf_counter()
                tsdimaging(**parameters, outfile=str(reference), overwrite=True)
                durations.append(time.perf_counter()-started)
            record['casa_run_seconds'] = durations
            record['casa_seconds'] = statistics.median(durations)
        if args.native_bin:
            destination = work/(name+'-native')
            values = dict(parameters, outfile=str(destination), overwrite=True)
            request = work/(name+'-request.json')
            request.write_text(json.dumps(dict(surface='tsdimaging', values=values)))
            environment = os.environ.copy()
            if args.native_measures:
                environment['CASA_RS_MEASURESPATH'] = str(args.native_measures.resolve())
            durations = []
            for _ in range(args.repeats):
                started = time.perf_counter()
                run = subprocess.run([str(args.native_bin.resolve()), '--json-run', str(request)],
                                     env=environment, capture_output=True, text=True)
                durations.append(time.perf_counter()-started)
                assert run.returncode == 0, run.stderr + run.stdout
            record['native_run_seconds'] = durations
            record['native_seconds'] = statistics.median(durations)
            (work/(name+'-native-stdout.json')).write_text(run.stdout)
            (work/(name+'-native-stderr.log')).write_text(run.stderr)
            assert run.returncode == 0, run.stderr + run.stdout
            record['products'] = {suffix: compare(str(destination)+suffix, str(reference)+suffix)
                                  for suffix in ('.image', '.weight')}
        report['cases'].append(record)
        (work/'results.json').write_text(json.dumps(report, indent=2))
        print(name, record.get('products', 'CASA reference complete'), flush=True)


if __name__ == '__main__':
    main()
