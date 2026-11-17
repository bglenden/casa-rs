# SPDX-License-Identifier: LGPL-3.0-or-later
"""Native equivalents of CASA 6.7.6.14's simulator-tool regression cases.

The PLP scenario follows test_tool_simulator.sm_predict_test.test_plp
(CAS-13439). Expected visibilities come from the analytic spectral law.
"""
import json
import os
from pathlib import Path
import shutil
import sys
import tarfile

import numpy as np
import pytest

from _table_readback import open_table, read_column
from casars.tools import componentlist, measures, simulator, table


@pytest.mark.skipif(not os.environ.get('CASA_RS_MEASURESPATH'), reason='needs measures runtime')
def test_upstream_plp_predicts_spectral_curvature(tmp_path):
    antenna = json.loads((Path(__file__).parent / 'data/upstream_simulator_antennas.json').read_text())
    path = tmp_path / 'plp.ms'
    sm = simulator()
    me = measures()
    sm.open(str(path))
    assert sm.setconfig(
        telescopename=antenna['telname'], x=antenna['x'], y=antenna['y'], z=antenna['z'],
        dishdiameter=antenna['diameter'], mount=['alt-az'], antname=antenna['ant_names'],
        coordsystem='global', referencelocation=me.observatory(antenna['telname']),
    )
    assert sm.setfeed(mode='perfect R L', pol=[''])
    assert sm.setspwindow(spwname='LBand', freq='1.0GHz', deltafreq='0.1GHz',
                          freqresolution='0.1GHz', nchannels=5, stokes='RR LL')
    direction = me.direction(rf='J2000', v0='19h53m50', v1='40d06m00')
    assert sm.setfield(sourcename='fake', sourcedirection=direction)
    assert sm.setlimits(shadowlimit=0.01, elevationlimit='1deg')
    assert sm.setauto(autocorrwt=0.0)
    assert sm.settimes(integrationtime='2000s', usehourangle=True,
                       referencetime=me.epoch('UTC', '2019/10/4/00:00:00'))
    assert sm.observe(sourcename='fake', spwname='LBand', starttime='-5h', stoptime='+5h')
    assert sm.close()
    flags = table()
    assert flags.open(str(path), nomodify=False)
    for name in ('FLAG', 'FLAG_ROW'):
        assert flags.putcol(name, np.zeros_like(flags.getcol(name)))
    assert flags.close()
    components = componentlist()
    assert components.addcomponent(dir='J2000 19h53m50 40d06m00', flux=5., fluxunit='Jy',
                                    freq='1.5GHz', shape='point', spectrumtype='plp', index=[3, 2])
    model = tmp_path / 'sky.cl'
    assert components.rename(str(model))
    assert components.done()
    assert sm.openfromms(str(path))
    assert sm.predict(complist=str(model), incremental=False)
    assert sm.close()
    data = np.asarray(read_column(open_table(path), 'DATA'))
    ratio = np.array([1., 1.1, 1.2, 1.3, 1.4]) / 1.5
    expected = 5. * ratio ** (3. + 2. * np.log(ratio))
    # Retain the upstream rtol and numpy.allclose's original 1e-8 atol.
    assert np.allclose(data[50, 0].real, expected, rtol=5e-8, atol=1e-8)


def screen_table(tmp_path, interval):
    """The upstream settrop parameters on a compact 0.1 s native observation."""
    with tarfile.open(Path(__file__).parent / 'data/upstream_simulator_screen.ms.tgz') as archive:
        options = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **options)
    path = tmp_path / f'screen-{interval}.ms'
    shutil.move(tmp_path / 'screen.ms', path)
    sm = simulator()
    assert sm.openfromms(str(path))
    output = tmp_path / f'screen-{interval}.cal'
    assert sm.settrop(mode='screen', table=str(output), pwv=3., deltapwv=.15,
                      beta=1.1, windspeed=7., simint=interval)
    assert sm.corrupt()
    assert sm.close()
    assert 'CORRECTED_DATA' in open_table(path).colnames()
    return open_table(output)


def assert_screen_sampling(solutions):
    times = np.asarray(read_column(solutions, 'TIME'))
    antenna = np.asarray(read_column(solutions, 'ANTENNA1'))
    gains = np.asarray(read_column(solutions, 'CPARAM'))[:, 0, 0]
    assert np.all(np.isfinite(gains))
    np.testing.assert_allclose(np.abs(gains), 1., rtol=1e-6, atol=1e-6)
    # AtmosCorruptor.cphase translates a frozen screen by floor(slot*dt*v/pixel).
    # At 7 m/s, a 25 m dish gives 12.5 m pixels. All samples in one pixel
    # must share its phase, including samples around 9 s and 11 s. This
    # keeps the upstream simint regression independent of random phase draws.
    slots = np.rint((times - times.min()) / .1).astype(int)
    pixels = np.floor(slots * .1 * 7. / 12.5).astype(int)
    for ant in np.unique(antenna):
        selected = antenna == ant
        for pixel in np.unique(pixels[selected]):
            values = gains[selected & (pixels == pixel)]
            np.testing.assert_array_equal(values, np.full(values.shape, values[0]))
        assert len(np.unique(gains[selected])) > 1


@pytest.mark.skipif(not os.environ.get('CASA_RS_MEASURESPATH'), reason='needs measures runtime')
def test_upstream_settrop_uses_the_requested_screen_sampling(tmp_path):
    """Native equivalent of the upstream-disabled sm_settrop_test.test_smsettrop.

    Its hardcoded phase ratio depends on CASA's random draw and its documented
    defective screen/wet-delay model. Check the shared frozen-screen sampling
    law instead, and prove that reverting to the 10 s default fails it.
    """
    assert_screen_sampling(screen_table(tmp_path, .1))
    with pytest.raises(AssertionError):
        assert_screen_sampling(screen_table(tmp_path, 10.))
