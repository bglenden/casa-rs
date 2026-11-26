#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Build the compact existing-MS input for the upstream settrop counterpart.

Run under CASA 6.7.6.14 Python. CASA only constructs the independent input;
the regular Python regression executes entirely in casa-rs.
"""
import argparse
import json
from pathlib import Path
import tarfile
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    from casatasks import version_string
    from casatools import measures, simulator, table
    assert version_string() == '6.7.6.14'
    root = Path(__file__).resolve().parents[2]
    antenna = json.loads((root / 'frontends/casars-python/python/tests/data/upstream_simulator_antennas.json').read_text())
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / 'screen.ms'
        sm = simulator()
        sm.open(str(path))
        sm.setconfig(telescopename='VLA', x=antenna['x'][:4], y=antenna['y'][:4],
                     z=antenna['z'][:4], dishdiameter=[25.], antname=['A', 'B', 'C', 'D'])
        sm.setfeed(mode='perfect R L')
        sm.setspwindow(spwname='L', freq='1.4GHz', deltafreq='1MHz',
                       freqresolution='1MHz', nchannels=1, stokes='RR LL')
        sm.setfield(sourcename='sky', sourcedirection=measures().direction('J2000', '19h53m50', '40d06m00'))
        sm.settimes(integrationtime='0.1s', usehourangle=True,
                    referencetime=measures().epoch('UTC', '2019/10/04'))
        sm.setlimits(shadowlimit=1., elevationlimit='-90deg')
        sm.setauto(autocorrwt=0.)
        sm.observe('sky', 'L', starttime='0s', stoptime='20s')
        sm.close()
        tb = table()
        tb.open(str(path), nomodify=False)
        if 'CORRECTED_DATA' not in tb.colnames():
            desc = tb.getcoldesc('DATA')
            desc.update(name='CORRECTED_DATA', dataManagerType='StandardStMan',
                        dataManagerGroup='CORRECTED_DATA')
            tb.addcols({'CORRECTED_DATA': desc})
            tb.putcol('CORRECTED_DATA', tb.getcol('DATA'))
        tb.close()
        for lock in path.rglob('table.lock'):
            lock.unlink()
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(args.out, 'w:gz') as archive:
            archive.add(path, arcname='screen.ms')


if __name__ == '__main__':
    main()
