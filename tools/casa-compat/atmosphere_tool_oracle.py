#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise the installed atmosphere API and the corrected ATM layer oracle."""

import argparse
import inspect
import json
from pathlib import Path
import signal
import subprocess
import sys

from casatools import atmosphere as casa_atmosphere
import casatools
import numpy as np


def q(value, unit):
    return {'value': value, 'unit': unit}


def quantity(result, unit, count=1):
    assert type(result) is dict and list(result) == ['unit', 'value'], result
    value = result['value']
    assert result['unit'] == unit and value.dtype == np.float64 and value.shape == (count,), result
    return value


def close(actual, expected):
    np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-14)


def fails(function, category, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except category:
        return
    raise AssertionError(f'{function.__name__} expected {category.__name__}')


def shape_state(factory):
    tool = factory()
    other = factory()
    assert tool.getAtmVersion() == 'ATM-1_0_0'
    assert tool.listAtmosphereTypes() == ['1 - TROPICAL', '2 - MIDLATSUMMER', '3 - MIDLATWINTER',
                                          '4 - SUBARTSUMMER', '5 - SUBARTWINTER']
    assert tool.getNumLayers() == tool.getNumSpectralWindows() == tool.getNumChan() == -1
    quantity(tool.getGroundWH2O(), '', 0)
    assert tool.getAirMass() == -1.
    assert not tool.setAirMass(2.) and not tool.setUserWH2O() and not tool.setSkyBackgroundTemperature()
    fails(tool.getDryOpacity, RuntimeError)
    fails(tool.initAtmProfile, TypeError, altitude=5000.)
    fails(tool.initAtmProfile, RuntimeError, atmType=0)
    fails(tool.initAtmProfile, RuntimeError, atmType=-1)
    fails(tool.initAtmProfile, RuntimeError, layerBoundaries=[5100.], layerTemperature=[])
    assert tool.done() and tool.close()
    result = tool.initAtmProfile(altitude='5km', temperature=q([270.,280.], 'K'), pressure=q(56000., 'Pa'),
                                maxAltitude=q(48000.,'m'), humidity=20., dTem_dh=q(-.0056,'K/m'),
                                dP=q(1000., 'Pa'), dPm=1.5, h0=q(2000.,'m'), atmType=2,
                                layerBoundaries=[5100.,5500.], layerTemperature=[280.,290.])
    assert type(result) is tuple and len(result) == 8 and 'User-defined temperature profile: ON' in result[0]
    assert 'Pressure step factor:         1.5 ' in result[0]
    close(quantity(result[1], 'km'), [5.])
    close(quantity(result[2], 'K', 2), [270.,280.])
    basic = tool.getBasicAtmParms()
    assert len(basic) == 11 and basic[8] == 1.075 and basic[-1] == 'MIDLATSUMMER'
    count = tool.getNumLayers()
    layers = tool.getProfile()
    assert len(layers) == 9
    close(quantity(layers[2], 'K', count)[[0,3]],[280.,290.])
    for field, unit in zip(layers[1:], ('m','K','kg m-3','m-3','mb','m-3','m-3','m-3')):
        quantity(field, unit, count)
    assert other.getNumLayers() == -1
    assert tool.initSpectralWindow() == 1 and tool.getChanNum(q(120.,'GHz')) == 1.
    assert tool.addSpectralWindow(fRes=q(0.,'GHz')) == -1
    assert tool.addSpectralWindow() == 4
    assert tool.addSpectralWindow(q([90.,80.],'GHz'), q([-2.,2.],'GHz'), q([-.5,.5],'GHz')) == 4
    close(quantity(tool.getSpectralWindow(2),'Hz',4),[90.75e9,90.25e9,89.75e9,89.25e9])
    assert tool.initSpectralWindow(1,q(90.,'GHz'),q(-2.,'GHz'),q(-.5,'GHz')) == 4
    assert tool.getNumChan(np.int64(0)) == 4 and tool.getRefChan() == 1
    for output, unit, expected in (
        (tool.getChanFreq(chanNum=2), 'GHz', 89.75), (tool.getChanSep(), 'MHz', -500.),
        (tool.getRefFreq(), 'GHz', 90.25), (tool.getBandwidth(), 'GHz', 1.5),
        (tool.getMinFreq(), 'GHz', 89.25), (tool.getMaxFreq(), 'GHz', 90.75),
    ):
        close(quantity(output, unit), expected)
    close(tool.getChanNum(q(90., 'GHz')), .5)
    fails(tool.getChanFreq, RuntimeError, -1, 0)
    fails(tool.getAbsTotalDry, RuntimeError, 0, -1, 0)
    assert tool.getDryOpacity(4) == -999.
    assert tool.getDryOpacity(-2) == tool.getDryOpacity(1)
    fails(tool.getDryOpacity, TypeError, 0.)
    assert tool.setUserWH2O(q(.5,'mm')) and tool.setAirMass(1.8)
    assert tool.setSkyBackgroundTemperature(q(6.,'K'))
    assert tool.initSpectralWindow(2,q(90.,'GHz'),q(2.,'GHz'),q(.5,'GHz')) == -1
    assert tool.getNumSpectralWindows() == 1 and tool.getAirMass() == 1.8
    close(quantity(tool.getUserWH2O(),'mm'), .5)
    close(quantity(tool.getSkyBackgroundTemperature(),'K'), 6.)
    ground = quantity(tool.getGroundWH2O(),'mm')[0]
    for kind in ('Dispersive','NonDispersive'):
        for metric, unit in [('PhaseDelay','deg'),('PathLength','m')]:
            user = getattr(tool,'get'+kind+metric)(0,0)
            wet = getattr(tool,'get'+kind+'Wet'+metric)(0,0)
            close(quantity(user,unit), quantity(wet,unit)*(.5/ground))
    bound = quantity(tool.getAbsTotalWet(0,0,0),'m-1').copy()
    assert tool.setUserWH2O('0.6mm')
    close(quantity(tool.getAbsTotalWet(0,0,0),'m-1'), bound)
    for kind in ('Tebb','Trj'):
        for method in ('get'+kind+'Sky','getAverage'+kind+'Sky','get'+kind+'SkySpec'):
            function = getattr(tool,method)
            normal, sentinel = function(), function(wh2o=q(-1.,'m'))
            if type(normal) is tuple:
                assert normal[0] == sentinel[0] == 4
                normal, sentinel = normal[1], sentinel[1]
                values = 4
            else:
                values = 1
            close(quantity(normal,'K',values), quantity(sentinel,'K',values))
            function(wh2o=q(1.,'mm'))
            close(quantity(tool.getUserWH2O(),'mm'), .6)
    assert type(tool.getH2OLinesOpacity()) is type(tool.getH2OContOpacity()) is float
    close(quantity(tool.getWetOpacity(),'neper'), tool.getH2OLinesOpacity()+tool.getH2OContOpacity())
    n, dry = tool.getDryOpacitySpec()
    assert n == 4 and dry.shape == (4,) and dry.dtype == np.float64
    close(dry, [tool.getDryOpacity(i) for i in range(n)])
    n, wet = tool.getWetOpacitySpec()
    close(quantity(wet,'neper',n), [quantity(tool.getWetOpacity(i),'neper')[0] for i in range(n)])
    for temperature in [270.1,270.2,270.2]:
        assert tool.updateAtmProfile(temperature=q(temperature,'K'))[0]
        close(quantity(tool.getBasicAtmParms()[2],'K'),270.)
        close(quantity(tool.getUserWH2O(),'mm'),ground)
        close(quantity(tool.getProfile()[2],'K',count)[0],280.)
    assert tool.updateAtmProfile(temperature=q(271.,'K'))[0]
    close(quantity(tool.getBasicAtmParms()[2],'K'),271.)
    assert not np.isclose(quantity(tool.getProfile()[2],'K',tool.getNumLayers())[0],280.)
    assert tool.getNumChan() == 4 and tool.getAirMass() == 1.8
    assert tool.addSpectralWindow() == 4 and tool.getAirMass() == 1.
    close(quantity(tool.getSkyBackgroundTemperature(),'K'),2.73)
    assert other.initAtmProfile(atmType=5)[0] and other.initSpectralWindow() == 1
    assert tool.close() and tool.getNumSpectralWindows() == -1 and other.getNumSpectralWindows() == 1
    assert tool.initAtmProfile(atmType=4)[0] and tool.getNumSpectralWindows() == -1
    print(factory.__module__, 'profile/window/sky state and quantity forms passed',flush=True)


def constructor(factory):
    assert str(inspect.signature(factory)) == '(*args, **kwargs)'
    tool = factory('unused', altitude='7km', swig_object=None)
    independent = factory()
    assert tool.getNumLayers() == independent.getNumLayers() == -1
    assert tool.initAtmProfile()[0] and tool.getNumLayers() == 27
    alias = factory(swig_object=tool)
    assert alias.getNumLayers() == 27 and independent.getNumLayers() == -1
    assert alias.close() and tool.getNumLayers() == -1


def update_thresholds(factory):
    # Each tuple gives the argument, reported record slot, canonical unit,
    # initial value, and changes below and above the native ATM threshold.
    cases = [('altitude',1,'m',5000.,.5,2.), ('pressure',3,'mbar',560.,.5,2.),
             ('temperature',2,'K',270.,.2,1.), ('dTem_dh',6,'K/km',-5.6,.005,.02),
             ('h0',9,'km',2.,.01,.05)]
    for name,index,unit,initial,below,above in cases:
        tool=factory()
        assert tool.initAtmProfile()[0]
        for delta in (below,below,above):
            assert tool.updateAtmProfile(**{name:q(initial+delta,unit)})[0]
            expected = initial if delta == below else initial+above
            close(quantity(tool.getBasicAtmParms()[index],unit), expected)
        assert tool.close()
    tool=factory()
    assert tool.initAtmProfile()[0]
    assert tool.updateAtmProfile(humidity=55.)[0]
    assert tool.getBasicAtmParms()[5] == 20.
    assert tool.updateAtmProfile(humidity=55.,temperature=q(271.,'K'))[0]
    close(tool.getBasicAtmParms()[5], 55.)
    assert tool.close()
    print(factory.__module__, 'all six profile-update controls passed',flush=True)


def window_units(factory):
    tool = factory()
    assert tool.initAtmProfile()[0]
    assert tool.initSpectralWindow(1, q(90., 'GHz'), q(4., 'GHz'), q(1., 'GHz')) == 4
    assert tool.setAirMass(1.8)
    invalid = q(1., 'bogusunit')
    fails(tool.initSpectralWindow, RuntimeError, fWidth=invalid)
    fails(tool.addSpectralWindow, RuntimeError, fCenter=invalid)
    fails(tool.addSpectralWindow, RuntimeError, fRes=q(0., 'bogusunit'))
    assert tool.getNumSpectralWindows() == 1 and tool.getNumChan() == 4
    assert tool.getAirMass() == 1.8
    assert tool.initSpectralWindow(1, q(90., 'GHz'), q(3., 'GHz'), q(1., 'GHz')) == 3
    assert tool.getChanNum(q(9e13, 'mHz')) == tool.getChanNum(q(90., 'GHz')) == 0.
    print(factory.__module__, 'window unit errors preserve the profile and mHz converts to Hz', flush=True)


def exact_weather_thresholds(factory):
    for name, update, slot, effective, unit in (
        ('pressure', q(560.99, 'mbar'), 3, 560., 'mbar'),
        ('h0', q(2.02, 'km'), 9, 2., 'km'),
        ('humidity', -100., 5, 20., None),
    ):
        tool = factory()
        assert tool.initAtmProfile(layerBoundaries=[5100., 5500.], layerTemperature=[280., 290.])[0]
        report = tool.updateAtmProfile(**{name: update})[0]
        found = tool.getBasicAtmParms()[slot]
        close(found if unit is None else quantity(found, unit), effective)
        close(quantity(tool.getProfile()[2], 'K', tool.getNumLayers())[0], 280.)
        if name == 'humidity':
            assert '0.1 %' in report
    tool = factory()
    reference = tool.initAtmProfile(humidity=0.1)[0]
    water = quantity(tool.getGroundWH2O(), 'mm').copy()
    for humidity in (0., -100.):
        assert tool.initAtmProfile(humidity=humidity)[0] == reference
        close(tool.getBasicAtmParms()[5], 0.1)
        close(quantity(tool.getGroundWH2O(), 'mm'), water)
    print(factory.__module__, 'exact weather thresholds and humidity floor passed', flush=True)


def invalid_layer_reference():
    # ATM's checked vector access throws std::out_of_range, which the pinned
    # wrapper does not catch. Isolate the failing reference call.
    program = """import resource
from casatools import atmosphere

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
tool = atmosphere()
assert tool.initAtmProfile()[0] and tool.initSpectralWindow() == 1
tool.getAbsTotalDry(tool.getNumLayers(), 0)
"""
    child = subprocess.run([sys.executable, '-c', program], capture_output=True, text=True, timeout=20)
    assert child.returncode == -signal.SIGABRT and 'std::out_of_range' in child.stderr, child.stderr
    print('CASA out-of-range layer abort reproduced', flush=True)


def invalid_layer(factory):
    tool = factory()
    assert tool.initAtmProfile()[0] and tool.initSpectralWindow() == 1
    count = tool.getNumLayers()
    for name in ('TotalDry', 'TotalWet', 'H2OLines', 'H2OCont', 'O2Lines', 'DryCont',
                 'O3Lines', 'COLines', 'N2OLines'):
        getter = getattr(tool, 'getAbs' + name)
        fails(getter, RuntimeError, count, 0)
        quantity(getter(count - 1, 0), 'm-1')
    print('All nine layer getters reject out-of-range indices and remain usable', flush=True)


def corrected_layers(factory):
    path = Path(__file__).resolve().parents[2]/'libs/casa/casa-atmosphere/tests/data/absorption.json'
    fixture = json.loads(path.read_text())
    windows = fixture['windows']
    separations = [w['sep_hz'] for w in windows]
    centers = [w['ref_hz']+((w['channels']-1)/2-w['ref_chan'])*w['sep_hz'] for w in windows]
    widths = [w['channels']*w['sep_hz'] for w in windows]
    flat = [(spw,channel) for spw,w in enumerate(windows) for channel in range(w['channels'])]
    species = {'total_dry':'TotalDry','total_wet':'TotalWet','o2':'O2Lines','dry_continuum':'DryCont',
               'o3':'O3Lines','co':'COLines','n2o':'N2OLines','h2o':'H2OLines','h2o_continuum':'H2OCont'}
    for reference in fixture['profiles']:
        tool = factory()
        assert tool.initAtmProfile(
            altitude=q(reference['altitude_m'],'m'), temperature=q(reference['ground_temperature_k'],'K'),
            pressure=q(reference['ground_pressure_hpa'],'mbar'), maxAltitude=q(reference['top_altitude_km'],'km'),
            humidity=reference['relative_humidity_percent'], dTem_dh=q(reference['lapse_rate_k_per_km'],'K/km'),
            dP=q(reference['pressure_step_hpa'],'mbar'), h0=q(reference['water_vapour_scale_height_km'],'km'),
            atmType=reference['atmosphere'],
        )[0]
        assert tool.initSpectralWindow(len(windows),q(centers,'Hz'),q(widths,'Hz'),q(separations,'Hz')) == windows[0]['channels']
        assert [tool.getNumChan(i) for i in range(len(windows))] == [w['channels'] for w in windows]
        assert tool.setUserWH2O('0.5mm')
        layers = reference['layers']
        assert tool.getNumLayers() == layers
        for key, name in species.items():
            channels = reference['layer_channels_totals' if key.startswith('total') else 'layer_channels_species']
            function = getattr(tool,'getAbs'+name)
            values = [quantity(function(layer, flat[channel][1], flat[channel][0]), 'm-1')[0]
                      for channel in channels for layer in range(layers)]
            close(values, reference['layer_'+key])
        print('corrected C++ layers',reference['atmosphere'],layers,len(flat),flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-only',action='store_true')
    args = parser.parse_args()
    assert casatools.version_string() == '6.7.6-14'
    constructor(casa_atmosphere)
    shape_state(casa_atmosphere)
    update_thresholds(casa_atmosphere)
    window_units(casa_atmosphere)
    exact_weather_thresholds(casa_atmosphere)
    invalid_layer_reference()
    if not args.reference_only:
        from casars.tools import atmosphere
        constructor(atmosphere)
        shape_state(atmosphere)
        update_thresholds(atmosphere)
        window_units(atmosphere)
        exact_weather_thresholds(atmosphere)
        invalid_layer(atmosphere)
        corrected_layers(atmosphere)
