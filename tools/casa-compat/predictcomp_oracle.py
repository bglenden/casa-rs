#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public predictcomp products and plot data with pinned CASA."""

from contextlib import chdir
import inspect
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from casars import tasks
from casatasks import predictcomp as casa_task
from casatasks.private.simutil import simutil
from casatools import ctsys, measures, quanta, simulator, table
import matplotlib
import numpy as np
from scipy.special import j1

from imager_predictcomp_oracle import READERS, compare, read_component

matplotlib.use('Agg')
from matplotlib import pyplot as plt


PT = 'Perley-Butler 2017'
JPL10 = 'Butler-JPL-Horizons 2010'
JPL12 = 'Butler-JPL-Horizons 2012'


def check_modern_angle(objname, actual, reference):
    observed = actual['positionangle']['value']
    baseline = reference['positionangle']['value']
    assert actual['positionangle']['unit'] == reference['positionangle']['unit'] == 'deg'
    # The pinned task passes degrees from the ephemeris as arcseconds.
    np.testing.assert_allclose(np.sin(np.deg2rad(2. * (observed - baseline * 3600.))),
                               0., rtol=0., atol=2e-7)
    if objname == 'Jupiter':
        ephemerides = Path(ctsys.resolve('ephemerides/JPL-Horizons')).glob('Jupiter_*.tab')
        for path in ephemerides:
            data = table()
            assert data.open(str(path))
            mjd = data.getcol('MJD')
            row = int(np.argmin(np.abs(mjd - 60000.)))
            if abs(mjd[row] - 60000.) < 1e-8:
                expected = data.getcell('NP_ang', row)
                data.close()
                np.testing.assert_allclose(
                    np.sin(np.deg2rad(2. * (observed - expected))), 0., atol=2e-7)
                break
            data.close()
        else:
            raise AssertionError("Jupiter ephemeris has no MJD 60000 row")
    reference['positionangle'] = actual['positionangle']


def compare_products(reference, actual, modern):
    assert Path(actual['clist']).is_dir()
    assert Path(reference['clist']).is_dir()
    raw_reference = reference['clist']
    raw_actual = actual['clist']
    reference['clist'] = Path(raw_reference).name.removeprefix('casa_')
    actual['clist'] = Path(raw_actual).name.removeprefix('native_')
    assert actual['freqs (GHz)'].dtype == np.float64
    rtol = 2e-6 if modern else 3e-7
    for reader in READERS:
        for path in (raw_reference, raw_actual):
            observed = read_component(reader, path)
            expected = read_component(READERS[0], raw_reference)
            if modern and path == raw_actual:
                check_modern_angle(actual['objname'], observed['shape'], expected['shape'])
            compare(observed, expected, f'{Path(path).name} {reader.__module__}', rtol=rtol)
    if modern:
        check_modern_angle(actual['objname'], actual['shape'], reference['shape'])
    if reference['antennalist']:
        assert isinstance(actual['spectrum']['bl0flux']['value'], np.float64)
    compare(actual, reference, actual['objname'], rtol=rtol)


def task_outputs(root):
    cases = [
        ('point defaults', dict(objname='3C286', minfreq='1.45GHz', standard=PT)),
        ('point band', dict(objname='3C48', minfreq='1400MHz', maxfreq='2000MHz', nfreqs=np.int64(5),
                            epoch='2017-09-04T05:00:00', standard='Perley-Butler 2013')),
        ('point descending', dict(objname='3C286', minfreq='3GHz', maxfreq='1000MHz', nfreqs=0,
                                  epoch='2017/09/04/05:00:00', standard='Baars')),
        ('default legacy', dict(objname='Mars', minfreq='90GHz', maxfreq='100GHz',
                                epoch='2010/06/18/06:00:00')),
        ('legacy scalar', dict(objname='Uranus', minfreq='40GHz', maxfreq='-1GHz',
                               epoch='55000.25d', standard=JPL10)),
        ('modern vector', dict(objname='Jupiter', minfreq='90GHz', maxfreq='110GHz', nfreqs=4,
                               epoch='2023/02/25/00:00:00', standard=JPL12, antennalist='array.cfg')),
        ('modern scalar', dict(objname='Jupiter', minfreq='100GHz',
                               epoch='2023/02/25/00:00:00', standard=JPL12)),
        ('modern descending', dict(objname='Jupiter', minfreq='110GHz', maxfreq='90GHz',
                                   epoch='2023/02/25/00:00:00', standard=JPL12)),
    ]
    for index, (case, params) in enumerate(cases):
        directory = root / f'catalogue-{index}'
        directory.mkdir()
        reference = casa_task(**params, prefix=str(directory / 'casa_'))
        actual = tasks.predictcomp(**params, prefix=str(directory / 'native_'))
        compare_products(reference, actual, params.get('standard') == JPL12)
        print(f'paired task and component tables {case}', flush=True)


def tool_reference(path, antenna_path, prediction, *, predict=True):
    x, y, z, sizes, pads, antennas, name, position = simutil().readantenna(str(antenna_path))
    tool = simulator()
    assert tool.open(str(path))
    assert tool.setconfig(telescopename=name, x=x, y=y, z=z, dishdiameter=sizes,
                          mount=['alt-az'] * len(x), antname=antennas, padname=pads,
                          coordsystem='global', referencelocation=position)
    freqs = prediction['freqs (GHz)']
    increment = (freqs[-1] - freqs[0]) / (len(freqs) - 1) if len(freqs) > 1 else 1.
    refcode = prediction['spectrum']['frequency']['refer']
    assert tool.setspwindow(spwname='SPW', freq=f'{freqs[0]}GHz', deltafreq=f'{increment}GHz',
                            freqresolution='1Hz', nchannels=len(freqs), refcode=refcode, stokes='RR')
    assert tool.setfield(sourcename=prediction['objname'], sourcedirection=prediction['shape']['direction'])
    assert tool.setfeed(mode='perfect R L')
    assert tool.setlimits(shadowlimit=.01, elevationlimit='0deg')
    assert tool.setauto(0.)
    # CASA stores TAI-valued seconds as the UVW integration epoch. Feed
    # the reference simulator an epoch whose stored seconds represent the
    # requested UTC instant, to compare the same physical projection.
    me = measures()
    epoch = prediction['epoch']
    utc = epoch['m0']['value']
    tai = me.measure(epoch, 'TAI')['m0']['value']
    aligned = me.epoch('UTC', {'unit':'d', 'value': 2.*utc - tai})
    assert tool.settimes(integrationtime='1s', usehourangle=False, referencetime=aligned)
    assert tool.observe(prediction['objname'], 'SPW', starttime='-0.5s', stoptime='0.5s')
    try:
        if predict:
            assert tool.setvp()
            assert tool.predict(complist=prediction['clist'])
    finally:
        assert tool.close()

    columns = table()
    assert columns.open(str(path))
    order = np.lexsort((columns.getcol('ANTENNA2'), columns.getcol('ANTENNA1')))
    uvw = columns.getcol('UVW')[:, order]
    data = columns.getcol('DATA')[0, :, order].T
    columns.close()
    return uvw, np.abs(data)


def plot_products(root):
    antennas = root / 'array.cfg'
    antennas.write_text(
        '# observatory=VLA\n# coordsys=LOC\n'
        '0 0 0 25 P0 A0\n100 20 1 25 P1 A1\n-45 165 3 25 P2 A2\n170 110 5 25 P3 A3\n'
    )
    png = root / 'amplitudes.png'
    svg = root / 'metres.svg'
    base = dict(objname='Jupiter', standard=JPL12, epoch='2023/02/25/00:00:00',
                minfreq='90GHz', maxfreq='110GHz', nfreqs=3, antennalist=str(antennas))
    fallback = root / 'casa-no-output.png'
    reference = casa_task(**base, prefix=str(root / 'casa_'), savefig=str(fallback), showplot=True,
                          symb='x', include0amp=True, include0bl=True, blunit='klambda', showbl0flux=True)
    assert reference['savedfig'] is None and not fallback.exists()

    with patch.object(plt, 'show') as show:
        native = tasks.predictcomp(**base, prefix=str(root / 'native_'), savefig=str(png),
                                   showplot=True, symb='x', include0amp=True, include0bl=True,
                                   blunit='klambda', showbl0flux=True)
        show.assert_called_once_with(block=False)
        figure = plt.gcf()
        axis, = figure.axes
        assert png.read_bytes().startswith(b'\x89PNG\r\n\x1a\n')
        assert native['savedfig'] == str(png)
        assert axis.get_xlim()[0] == axis.get_ylim()[0] == 0.
        assert axis.get_xlabel() == r'Baseline length (k$\lambda$)'
        assert 'Zero-baseline flux' in axis.get_legend().get_title().get_text()
        assert len(axis.lines) == 3
        for index, line in enumerate(axis.lines):
            assert line.get_marker() == 'x'
            np.testing.assert_allclose(line.get_xdata(), native['baselines'] * 90.e6 / 299792458.)
            np.testing.assert_allclose(line.get_ydata(), native['amps'][index])
        plt.close(figure)

    # CASA's simulator cannot convert the task's TOPO spectrum. Its UVW
    # columns still provide a separate projection for the analytic ellipse.
    uvw, _ = tool_reference(root / 'jupiter.ms', antennas, native, predict=False)
    qa = quanta()
    shape = native['shape']
    major, minor, angle = [qa.convert(shape[key], 'rad')['value'] for key in
                           ('majoraxis', 'minoraxis', 'positionangle')]
    cos, sin = np.cos(angle), np.sin(angle)
    u, v = uvw[:2]
    spatial = np.hypot(minor * (u * cos - v * sin), major * (u * sin + v * cos))
    radius = np.pi * native['freqs (GHz)'][:, None] * 1e9 / 299792458. * spatial
    expected_amp = np.abs(2. * j1(radius) / radius * native['spectrum']['ival'][:, None])
    compare_numeric('Jupiter projected baselines', native['baselines'], np.hypot(u, v))
    compare_numeric('Jupiter analytic ellipse amplitudes', native['amps'], expected_amp)

    legacy = base | {'objname':'Mars', 'standard':JPL10, 'epoch':'2010/06/18/00:00:00'}
    native_mars = tasks.predictcomp(**legacy, prefix=str(root / 'native-mars_'),
                                   savefig=str(root / 'mars.png'))
    # The legacy simulator reuses a tabular component's first flux across
    # channels. A fresh scalar component at each frequency independently
    # exercises the same disk transform and the requested flux.
    amplitudes = []
    for index, freq in enumerate(native_mars['freqs (GHz)']):
        band = legacy | {'minfreq':f'{freq}GHz', 'maxfreq':''}
        casa = casa_task(**band, prefix=str(root / f'casa-mars-{index}_'))
        uvw, amps = tool_reference(root / f'mars-{index}.ms', antennas, casa)
        amplitudes.append(amps[0])
    compare_numeric('Mars projected baselines', native_mars['baselines'], np.hypot(*uvw[:2]))
    compare_numeric('Mars CASA simulator amplitudes', native_mars['amps'], np.stack(amplitudes))

    native = tasks.predictcomp(**base, prefix=str(root / 'saveonly_'), savefig=str(svg))
    assert '<svg' in svg.read_text()
    assert native['savedfig'] == str(svg) and native['blunit'] == 'm'
    assert not plt.get_fignums()

    without_antenna = base | {'antennalist': '', 'savefig': str(root / 'ignored.png'), 'showplot': True}
    native = tasks.predictcomp(**without_antenna, prefix=str(root / 'noplot_'))
    assert native['savedfig'] is None and not Path(without_antenna['savefig']).exists()


def compare_numeric(label, actual, expected):
    assert actual.shape == expected.shape, (label, actual.shape, expected.shape)
    nrms = np.linalg.norm(actual - expected) / np.linalg.norm(expected)
    assert nrms <= 1e-3, (label, nrms, actual, expected)
    print(f'{label} NRMS {nrms}', flush=True)


def errors_and_collision(root):
    base = dict(objname='3C286', standard=PT, minfreq='1GHz', epoch='58000.25d')
    for run in (casa_task, tasks.predictcomp):
        for args, error in (({}, ValueError), ({'objname':'3C286'}, ValueError),
                            (base | {'epoch':'invalid'}, KeyError),
                            (base | {'minfreq':1e9}, AssertionError),
                            (base | {'standard':'undefined'}, AssertionError),
                            (base | {'blunit':'m'}, AssertionError)):
            try:
                run(**args, prefix=str(root / 'errors_'))
            except error:
                pass
            else:
                raise AssertionError((run, args, error))
        assert run(**(base | {'objname':'unrecognized'}), prefix=str(root / 'unknown_')) is None
    output = str(root / 'collision_')
    first = tasks.predictcomp(**base, maxfreq='2GHz', nfreqs=2, prefix=output)
    second = tasks.predictcomp(**base, maxfreq='4GHz', nfreqs=True, prefix=output)
    assert first['clist'] == second['clist']
    np.testing.assert_equal(read_component(READERS[0], second['clist'])['spectrum']['tabFreqVal'],
                            [1e9, 4e9])


def main():
    def signature(func):
        return [(p.name, p.kind, p.default) for p in inspect.signature(func).parameters.values()]

    assert signature(casa_task) == signature(tasks.predictcomp)
    with TemporaryDirectory(prefix='task-predictcomp-') as tmp, chdir(tmp):
        root = Path(tmp)
        task_outputs(root)
        plot_products(root)
        errors_and_collision(root)


if __name__ == '__main__':
    main()
