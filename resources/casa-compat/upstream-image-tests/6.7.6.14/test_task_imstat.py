########################################################################
# test_task_imstat.py
#
# Copyright (C) 2018
# Associated Universities, Inc. Washington DC, USA
#
# This script is free software; you can redistribute it and/or modify it
# under the terms of the GNU Library General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Library General Public
# License for more details.
#
# CAS-12997
#
# Based on the requirements listed in plone found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.analysis.imstat.html
#
#
##########################################################################
import os
import shutil
import unittest
import math
import numpy as np
import numbers

from casatools import ctsys, image, table, quanta, regionmanager
from casatasks import imstat
_tb = table()
_qa = quanta()
_rg = regionmanager()

datapath = 'unittest/imstat/'

'''
Unit tests for task imstat.
'''
class imstat_test(unittest.TestCase):

    # Input and output names
    moment = 'moment_map.im'
    s150 = '150arcsec_pix.im'
    s15 = '15arcsec_pix.im'
    s0_015 = '0.015arcsec_pix.im'
    s0_0015 = '0.0015arcsec_pix.im'
    s0_00015 = '0.00015arcsec_pix.im'
    linear_coords = 'linearCoords.fits'
    fourdim = '4dim.im'
    kimage = "ktest.im"
    res = None
    inpfiles = [moment, s150, s15, s0_015, s0_0015, s0_00015, linear_coords, fourdim, kimage]

    def _compare(self, resold, resnew, helpstr):
        mytype = type(resold)
        self.assertTrue(mytype == type(resnew), helpstr + ": types differ")
        if mytype == dict:
            for k in resold.keys():
                self._compare(resold[k], resnew[k], helpstr)
        elif mytype == np.ndarray:
            oldarray = resold.ravel()
            newarray = resnew.ravel()
            self.assertTrue(
                len(oldarray) == len(newarray),
                helpstr + ": array lengths not equal"
            )
            for i in range(len(oldarray)):
                self._compare(oldarray[i], newarray[i], helpstr)
        elif mytype == str:
            self.assertTrue(
                resold == resnew,
                helpstr + ": string inequality, old = " + resold + ", new = " + resnew
            )
        elif isinstance(resold, numbers.Integral) or mytype == np.int32:
            self.assertTrue(
                resold == resnew,
                helpstr + ": integral inequality, old = " + str(resold) + ", new = " + str(resnew)
            )
        elif isinstance(resold, numbers.Real):
            self.assertTrue(
                resold == resnew
                or abs(resnew/resold - 1) < 1e-6,
                helpstr + "float inequality: old = " + str(resold)
                + ", new = " + str(resnew)
            )
        else:
            self.assertTrue(False, "Unhandled type " + str(mytype))

    def setUp(self):
        self.res = None
        self._myia = image()
        self.datapath = ctsys.resolve(datapath)

    def tearDown(self):
        self._myia.done()
        self.assertTrue(len(_tb.showcache()) == 0)
        # Only delete the files created by the tests (avoid cleaning the full local directory!!)
        for f in self.inpfiles:
            if os.path.isdir(f):
                shutil.rmtree(f)
            if os.path.isfile(f):
                os.remove(f)

        # Remove created files
        os.system('rm -rf chauvtest.im fhtest.im hftest.im internally_excluded_region.im maskim test011.im tmp.im')

    def test_moment_map_flux(self):
        """Test 1: verify moment maps can have flux densities computed in statistics"""
        shutil.copytree(os.path.join(self.datapath, self.moment), self.moment)
        stats = imstat(imagename=self.moment)
        mean = stats['mean']
        npts = stats['npts']
        _myia = image()
        _myia.open(self.moment)
        summary = _myia.summary()
        _myia.close()
        rainc = _qa.abs(_qa.quantity(summary['incr'][0],'rad'))
        rainc = _qa.convert(rainc,'arcsec')
        decinc = _qa.abs(_qa.quantity(summary['incr'][1],'rad'))
        decinc = _qa.convert(decinc,'arcsec')
        beam = summary['restoringbeam']
        major = beam['major']
        minor = beam['minor']
        pixperbeam = _qa.div(_qa.mul(major,minor),(_qa.mul(rainc,decinc)))['value']*(math.pi/(4*math.log(2)))
        got = stats['flux'][0]
        expected = (mean*npts/pixperbeam)[0]
        self.assertTrue(abs(got - expected) < 1e-11)

    def test_box_param_can_have_spaces(self):
        """ test that box parameter can have spaces, CAS-2050 """
        shutil.copytree(os.path.join(self.datapath,self.s0_00015), self.s0_00015)
        box = '0, 0,  1 ,   1'
        stats = imstat(imagename=self.s0_00015, box=box)
        self.assertTrue(stats['npts'] == 4)

    def test_CAS_2195_image_can_have_linear_rather_than_direction_coordinate(self):
        """ verify fix for CAS-2195, image has linear, not direction, coordinate"""
        myim = self.linear_coords
        shutil.copy(os.path.join(self.datapath,myim), myim)
        expected_max = [3, 10]
        expected_min = [4, 0]
        stats = imstat(myim)
        self.assertTrue((stats['maxpos'] == expected_max).all())
        self.assertTrue((stats['minpos'] == expected_min).all())

    def test_specifying_axes_param(self):
        """choose axes works"""
        myim = self.fourdim
        shutil.copytree(os.path.join(self.datapath,myim), myim)
        axes = [-1, [0, 1, 2], [0, 1], 3]
        expected_mean = [
                [59.5], [ 57.5,  58.5,  59.5,  60.5,  61.5],
                [
                    [50., 51., 52., 53., 54.],
                    [55., 56., 57., 58., 59.],
                    [60., 61., 62., 63., 64.],
                    [65., 66., 67., 68., 69.]
                ],
                [
                    [
                        [2., 7., 12., 17.],
                        [22., 27., 32., 37.],
                        [42., 47., 52., 57.]
                    ],
                    [
                        [62., 67., 72., 77.],
                        [ 82.,  87.,  92., 97.],
                        [ 102., 107., 112., 117.]
                    ]
                ]
            ]
        expected_sumsq = [
                [568820], [ 108100.,  110884.,  113716.,  116596.,  119524.],
                [
                    [ 22000., 22606., 23224., 23854., 24496.],
                    [ 25150., 25816., 26494., 27184., 27886.],
                    [ 28600., 29326., 30064., 30814., 31576.],
                    [ 32350., 33136., 33934., 34744., 35566.]
                ],
                [
                    [
                        [ 3.00000000e+01, 2.55000000e+02, 7.30000000e+02, 1.45500000e+03],
                        [ 2.43000000e+03, 3.65500000e+03, 5.13000000e+03, 6.85500000e+03],
                        [  8.83000000e+03,   1.10550000e+04,   1.35300000e+04, 1.62550000e+04]
                    ],
                    [
                        [  1.92300000e+04,   2.24550000e+04,   2.59300000e+04, 2.96550000e+04],
                        [  3.36300000e+04,   3.78550000e+04,   4.23300000e+04, 4.70550000e+04],
                        [  5.20300000e+04,   5.72550000e+04,   6.27300000e+04, 6.84550000e+04]
                    ]
                ]
            ]
        for i in range(len(axes)):
            stats = imstat(myim, axes=axes[i])
            self.assertTrue((stats['mean'] == expected_mean[i]).all())
            self.assertTrue((stats['sumsq'] == expected_sumsq[i]).all())

    def test_stretch(self):
        """ imstat: Test stretch parameter"""
        yy = image()
        mymask = "maskim"
        yy.fromshape(mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200,200,1,20]
        imagename = "tmp.im"
        yy.fromshape(imagename, shape)
        yy.addnoise()
        yy.done()
        try:
            OK = False
            zz = imstat(
                imagename=imagename, mask=mymask + ">0", stretch=False
            )
        except:
            OK = True
        # CASA 5 returns False (depending on __rethrow_casa_exceptions),
        # CASA 6 returns exception
        self.assertTrue(OK)
        zz = imstat(
            imagename=imagename, mask=mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type({}) and (not zz == {}))
        yy.done()

    def test_logfile_param(self):
        """test logfile """
        logfile = "imstat.log"
        myim = self.fourdim
        shutil.copytree(os.path.join(self.datapath,myim), myim)
        i = 1
        for append in [False, True]:
            stats = imstat(myim, axes=[0], logfile=logfile, append=append)
            size = os.path.getsize(logfile)
            # appending, second time through size should double
            self.assertTrue(size > 1.2e4*i and size < 1.3e4*i )
            i = i+1

    def test_multiple_region_support(self):
        """ test multiple region support"""
        shape = [10, 10, 10]
        myia = self._myia
        myia.fromshape("test011.im", shape)
        box = "0, 0, 2, 2, 4, 4, 6, 6"
        chans = "0~4, 6, >8"
        bb = imstat(imagename=myia.name(), chans=chans, box=box)
        myia.done()
        self.assertTrue(bb["npts"][0] == 126)

    def test_hingesfences(self):
        """Test hinges-fences algorithm"""
        data = list(range(100))
        myia = self._myia
        imagename = "hftest.im"
        myia.fromarray(imagename, data)
        myia.done()
        classic = imstat(imagename, algorithm="cl")
        hfall = imstat(imagename=imagename, algorithm="h")
        hf0 = imstat(imagename=imagename, algorithm="h", fence=0)
        for k in classic.keys():
            if type(classic[k]) == np.ndarray:
                if k == 'sigma':
                    self.assertTrue((abs(hfall[k]/classic[k] - 1) < 1e-15).all())
                else:
                    self.assertTrue((hfall[k] == classic[k]).all())
            else:
                self.assertTrue(hfall[k] == classic[k])
        self.assertTrue(hf0['npts'][0] == 51)
        self.assertTrue(hf0['mean'][0] == 49)
        self.assertTrue(hf0['q1'][0] == 36)

    def test_fithalf(self):
        """Test fit to half algorithm"""
        data = np.array(range(100))
        data = data*data
        myia = self._myia
        imagename = "fhtest.im"
        myia.fromarray(imagename, data)
        myia.done()
        for center in ["mean", "median", "zero"]:
            for lside in [True, False]:
                res = imstat(
                    imagename=imagename, algorithm="f",
                    center=center, lside=lside
                )
                if (lside):
                    if (center == "mean"):
                        self.assertTrue(res['npts'][0] == 116)
                        self.assertTrue(res['mean'][0] == 3283.5)
                        self.assertTrue(res['median'][0] == 3283.5)
                        self.assertTrue(res['q1'][0] == 784.0)
                    elif (center == "median"):
                        self.assertTrue(res['npts'][0] == 100)
                        self.assertTrue(res['mean'][0] == 2450.5)
                        self.assertTrue(res['median'][0] == 2450.5)
                        self.assertTrue(res['q1'][0] == 576.0)
                    elif (center == "zero"):
                        self.assertTrue(res['npts'][0] == 2)
                        self.assertTrue(res['mean'][0] == 0)
                        self.assertTrue(res['median'][0] == 0)
                        self.assertTrue(res['q1'][0] == 0)
                else:
                    if (center == "mean"):
                        self.assertTrue(res['npts'][0] == 84)
                        self.assertTrue(res['mean'][0] == 3283.5)
                        self.assertTrue(res['median'][0] == 3283.5)
                        self.assertTrue(res['q1'][0] == 326.0)
                    elif (center == "median"):
                        self.assertTrue(res['npts'][0] == 100)
                        self.assertTrue(res['mean'][0] == 2450.5)
                        self.assertTrue(res['median'][0] == 2450.5)
                        self.assertTrue(res['q1'][0] == -724.0)
                    elif (center == "zero"):
                        self.assertTrue(res['npts'][0] == 200)
                        self.assertTrue(res['mean'][0] == 0)
                        self.assertTrue(res['median'][0] == 0)
                        self.assertTrue(res['q1'][0] == -2500.0)

    def test_chauvenet(self):
        """Test Chauvenet's criterion algorithm"""
        data = [
                -2.61279178e+00,  -2.59342551e+00,  -2.16943479e+00,
                -2.13970494e+00,  -1.91509378e+00,  -1.91133809e+00,
                -1.84780550e+00,  -1.67959487e+00,  -1.55754685e+00,
                -1.49124575e+00,  -1.47779667e+00,  -1.38040781e+00,
                -1.37083769e+00,  -1.34913635e+00,  -1.29416192e+00,
                -1.10022914e+00,  -1.07126451e+00,  -1.05194223e+00,
                -1.03733921e+00,  -1.02524054e+00,  -9.84085381e-01,
                -9.46198046e-01,  -9.23078358e-01,  -9.21401978e-01,
                -8.76483500e-01,  -8.60657215e-01,  -8.26754928e-01,
                -7.59524405e-01,  -7.36167967e-01,  -6.76235080e-01,
                -6.72010839e-01,  -6.33015037e-01,  -5.91541886e-01,
                -5.87743282e-01,  -5.28600693e-01,  -5.03111005e-01,
                -4.84272331e-01,  -3.87220532e-01,  -3.62094551e-01,
                -3.12986404e-01,  -3.01742464e-01,  -2.86407530e-01,
                -2.77583510e-01,  -2.37437248e-01,  -2.37364024e-01,
                -2.35247806e-01,  -2.11185545e-01,  -1.92734912e-01,
                -1.87121660e-01,  -1.77792773e-01,  -1.69995695e-01,
                -1.45033970e-01,  -1.16942599e-01,  -6.27262741e-02,
                -3.45510058e-02,  -3.06752156e-02,  -1.79617219e-02,
                -1.14524942e-02,  -3.16955987e-03,   7.29589257e-04,
                1.24999344e-01,   2.12515876e-01,   2.50957519e-01,
                2.79240131e-01,   2.81288683e-01,   3.05763662e-01,
                3.11809599e-01,   3.40768367e-01,   3.51874888e-01,
                3.91162097e-01,   4.58450705e-01,   4.82642174e-01,
                4.96854514e-01,   7.20111370e-01,   7.22756803e-01,
                7.25001752e-01,   8.35289240e-01,   8.46509099e-01,
                8.93022776e-01,   9.00427580e-01,   9.17734325e-01,
                9.18030262e-01,   1.04210591e+00,   1.05506992e+00,
                1.09472048e+00,   1.15250385e+00,   1.16275501e+00,
                1.21244884e+00,   1.22725236e+00,   1.31463480e+00,
                1.33273876e+00,   1.57637489e+00,   1.58221984e+00,
                1.65665936e+00,   1.80032420e+00,   1.91410339e+00,
                2.02669597e+00,   2.08605909e+00,   2.09777880e+00,
                2.21240473e+00,
                3.5, 4, 5, 6, 7, 8, 1000000
            ]
        myia = self._myia
        imagename = "chauvtest.im"
        myia.fromarray(imagename, data)
        myia.done()
        for zscore in [3.5, -1]:
            for maxiter in [0, 1, -1]:
                stats = imstat(imagename=imagename, algorithm="ch", zscore=zscore, maxiter=maxiter)
                if zscore == 3.5:
                    if maxiter == 0:
                        enpts = 106
                        emax = 8
                    elif maxiter == 1:
                        enpts = 104
                        emax = 6
                    elif maxiter == -1:
                        enpts = 102
                        emax = 4
                elif zscore == -1:
                    if maxiter == 0:
                        enpts = 106
                        emax = 8
                    elif maxiter == 1:
                        enpts = 103
                        emax = 5
                    elif maxiter == -1:
                        enpts = 100
                        emax = data[99]
                self.assertTrue(stats['npts'][0] == enpts)
                self.assertTrue(abs(stats['max'][0] - emax) < 1e-6)

    def test_internal_region_exclusion(self):
        """Verify data not returned for internally excluded regions"""
        myia = image()
        imagename = "internally_excluded_region.im"
        myia.fromshape(imagename, [100, 200, 110, 4])
        myia.addnoise()
        myia.done()
        zz = imstat(imagename, axes=[0, 1], chans="10~20;60~90", stokes="IV")
        self.assertTrue((zz['npts'].shape == (42, 2)))
        self.assertTrue(np.min(zz['npts']) > 0)

    def test_biweight(self):
        """Test biweight algorithm CAS-11100"""
        myia = image()
        imagename = os.path.join(self.datapath,"biweight_test.im")
        for niter in (20, 2, -1):
            res = imstat(imagename=imagename, algorithm='b', niter=niter)
            self.assertAlmostEqual(res['min'][0], -5.48938513)
            self.assertAlmostEqual(res['max'][0], 104.80391693)
            self.assertEqual(res['npts'][0], 1.25000000e+08)
            if niter == 20:
                self.assertAlmostEqual(res['sigma'][0], 1.02012422703)
                self.assertAlmostEqual(res['mean'][0], -5.43024227e-06)
            elif niter == 2:
                self.assertAlmostEqual(res['sigma'][0], 1.02012435686)
                self.assertAlmostEqual(res['mean'][0], 0.00026525095639)
            elif niter == -1:
                self.assertAlmostEqual(res['sigma'][0], 1.02031194)
                self.assertAlmostEqual(res['mean'][0], 0.00284497)

if __name__ == '__main__':
    unittest.main()
