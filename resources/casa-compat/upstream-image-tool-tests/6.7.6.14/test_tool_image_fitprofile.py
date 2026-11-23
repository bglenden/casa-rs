##########################################################################
# test_tool_image_fitprofile.py
#
# Copyright (C) 2018
# Associated Universities, Inc. Washington DC, USA.
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
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.fitprofile
#
##########################################################################
import shutil
import unittest
import math
import numpy
import os
from numpy import isnan

from casatools import functional
from casatools import regionmanager
from casatools import image as iatool
from casatools import table
from casatools import ctsys
ctsys_resolve = ctsys.resolve

twogauss = "specfit_multipix_2gauss.fits"
polyim = "specfit_multipix_poly_2gauss.fits"
gauss_triplet = "gauss_triplet.fits"
two_lorentzians = "two_lorentzians.fits"
invalid_fits = "invalid_fits.im"
solims = [
    "amp", "ampErr", "center", "centerErr",
    "fwhm", "fwhmErr", "integral", "integralErr"
]
birdie = "birdie.im"

nanvalue = 4.53345345

datapath = ctsys_resolve('unittest/ia_fitprofile/')

def run_fitprofile (
    imagename, box, region, chans, stokes,
    axis, mask, ngauss, poly, multifit, model="",
    residual="", amp="", amperr="", center="", centererr="",
    fwhm="", fwhmerr="", integral="", integralerr="",
    estimates="", logresults=True, pampest=[ ], pcenterest=[ ], pfwhmest=[ ],
    pfix="", gmncomps=0, gmampcon=[ ], gmcentercon=[ ],
    gmfwhmcon=[ ], gmampest=[0], gmcenterest=[0],
    gmfwhmest=[0], gmfix="", logfile="", pfunc="",
    goodamprange=[0.0], goodcenterrange=[0.0], goodfwhmrange=[0.0],
    sigma=[ ], outsigma=""
):
    myia = iatool()
    myia.open(imagename)
    if (not myia.isopen()):
        myia.done()
        raise Exception
    res = myia.fitprofile(
        box=box, region=region, chans=chans,
        stokes=stokes, axis=axis, mask=mask,
        ngauss=ngauss, poly=poly, estimates=estimates,
        multifit=multifit,
        model=model, residual=residual, amp=amp,
        amperr=amperr, center=center, centererr=centererr,
        fwhm=fwhm, fwhmerr=fwhmerr, integral=integral,
        integralerr=integralerr, logresults=logresults, pampest=pampest,
        pcenterest=pcenterest, pfwhmest=pfwhmest, pfix=pfix,
        gmncomps=gmncomps, gmampcon=gmampcon,
        gmcentercon=gmcentercon, gmfwhmcon=gmfwhmcon,
        gmampest=gmampest, gmcenterest=gmcenterest,
        gmfwhmest=gmfwhmest, gmfix=gmfix, logfile=logfile,
        pfunc=pfunc, goodamprange=goodamprange,
        goodcenterrange=goodcenterrange,
        goodfwhmrange=goodfwhmrange, sigma=sigma, outsigma=outsigma    )
    myia.close()
    myia.done()
    return res

class ia_fitprofile_test(unittest.TestCase):

    def setUp(self):
        shutil.copy(datapath + twogauss, twogauss)
        shutil.copy(datapath + polyim, polyim)

    def tearDown(self):
        os.remove(twogauss)
        os.remove(polyim)
        data = [
            "amp_0","amp_1","ampErr_0","ampErr_1",
            "ampErr_gm_0","ampErr_gm_1","ampErr_gm_2",
            "ampErr_ls_0","ampErr_ls_1",
            "amp_gm_0","amp_gm_1","amp_gm_2",
            "amp_ls_0","amp_ls_1",
            "bad.im","CAS6134_in.im",
            "center_0","center_1",
            "centerErr_0","centerErr_1",
            "centerErr_gm_0","centerErr_gm_1","centerErr_gm_2",
            "centerErr_ls_0","centerErr_ls_1",
            "center_gm_0","center_gm_1","center_gm_2",
            "center_ls_0","center_ls_1",
            "fwhm_0","fwhm_1",
            "fwhmErr_0","fwhmErr_1",
            "fwhmErr_gm_0","fwhmErr_gm_1","fwhmErr_gm_2",
            "fwhmErr_ls_0","fwhmErr_ls_1",
            "fwhm_gm_0","fwhm_gm_1","fwhm_gm_2",
            "fwhm_ls_0","fwhm_ls_1",
            "good1.im","good2.im","good3.im",
            "ia.fromshape.fit","ia.fromshape.resid",
            "integral_0","integral_1",
            "integralErr_0","integralErr_1",
            "integralErr_gm_0","integralErr_gm_1","integralErr_gm_2",
            "integralErr_ls_0","integralErr_ls_1",
            "integral_gm_0","integral_gm_1","integral_gm_2",
            "integral_ls_0","integral_ls_1",
            "ltpfit.im","maskim","mylog.txt","sigma.im",
            "spxfit.im","tg_poly.im","two_lorentzian_fit.log"
]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        #tb = table( )
        #self.assertTrue(len(tb.showcache()) == 0)
        #tb.done( )

    def checkImage(self, gotImage, expectedName):
        expected = iatool()
        expected.open(expectedName)
        got = iatool()
        if type(gotImage) == str:
            got.open(gotImage)
        else:
            got = gotImage
        self.assertTrue((got.shape() == expected.shape()).all())
        gotchunk = got.getchunk()
        expchunk = expected.getchunk()
        if (numpy.isnan(gotchunk).any()):
            gotchunk = gotchunk.ravel()
            for i in range(len(gotchunk)):
                if isnan(gotchunk[i]):
                    gotchunk[i] = nanvalue
        if (numpy.isnan(expchunk).any()):
            expchunk = expchunk.ravel()
            for i in range(len(expchunk)):
                if isnan(expchunk[i]):
                    expchunk[i] = nanvalue
        diffData = gotchunk - expchunk
        self.assertTrue(abs(diffData).max() < 2e-11)
        self.assertTrue(
            (
                got.getchunk(getmask=True) == expected.getchunk(getmask=True)
            ).all()
        )
        gotCsys = got.coordsys()
        expectedCsys = expected.coordsys()
        diffPixels = gotCsys.referencepixel()['numeric'] - expectedCsys.referencepixel()['numeric']
        self.assertTrue(abs(diffPixels).max() == 0)

        diffRef = gotCsys.referencevalue()['numeric'] - expectedCsys.referencevalue()['numeric']
        # fracDiffRef = (diffRef)/expectedCsys.referencevalue()['numeric'];
        self.assertTrue(abs(diffRef).max() == 0)
        got.close()
        got.done()
        expected.close()
        expected.done()

    def test_exceptions(self):
        """ia_fitprofile(): Test various exception cases"""

        def testit(
            imagename, box, region, chans, stokes,
            axis, mask, ngauss, poly, multifit, model,
            residual
        ):
            self.assertRaises(
                Exception, run_fitprofile, imagename,
                box, region, chans, stokes, axis, mask,
                ngauss, poly, multifit, model, residual
            )
        # Exception if no image name given",
        testit(
            "", "", "", "", "", 2, "", False, 1, -1, "", ""
        )
        # Exception if bogus image name given
        testit(
            "my bad", "", "", "", "", 2, "", 1, -1, False, "", ""
        )
        # Exception if given axis is out of range
        testit(
            twogauss, "", "", "", "", 5, "", 1, -1, False, "", ""
        )
        # Exception if bogus box string given #1
        testit(
            twogauss, "abc", "", "", "", 2, "", 1, -1, False, "", ""
        )
        # Exception if bogus box string given #2
        testit(
            twogauss, "0,0,1000,1000", "", "", "", 2, "", 1, -1, False, "", ""
        )
        # Exception if bogus chans string given #1
        testit(
            twogauss, "", "", "abc", "", 2, "", 1, -1, False, "", ""
        )
        # Exception if bogus chans string given #2
        testit(
            twogauss, "", "", "0-200", "", 2, "", 1, -1, False, "", ""
        )
        # Exception if bogus stokes string given #1
        testit(
            twogauss, "", "", "", "abc", 2, "", 1, -1, False, "", ""
        )
        # Exception if bogus stokes string given #2
        testit(
            twogauss, "", "", "", "v", 2, "", 1, -1, False, "", ""
        )
        # Exception if no gaussians and no polynomial specified
        testit(
            twogauss, "", "", "", "", 2, "", 0, -1, False, "", ""
        )

    def test_1(self):
        """Tests of averaging over a region and then fitting"""
        imagename = twogauss
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        ngauss = 2
        poly = -1
        multifit = False
        model = ""
        residual = ""
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual
            )
            self.assertTrue(len(res["converged"]) == 1)
            self.assertTrue(res["converged"][0,0,0,0])
            # even though two components given, only one is fit
            self.assertTrue(res["ncomps"][0,0,0,0] == 1)
            # the fit component is a gaussian
            self.assertTrue(res["type"][0,0,0,0,0] == "GAUSSIAN")
            gs = res["gs"]
            self.assertAlmostEqual(gs["amp"][0,0,0,0,0], 49.7, 1, "amplitude determination failure")
            self.assertAlmostEqual(gs["ampErr"][0,0,0,0,0], 4.0, 1, "amplitude error determination failure")
            self.assertAlmostEqual(gs["center"][0,0,0,0,0], -237.7, 1, "center determination failure")
            self.assertAlmostEqual(gs["centerErr"][0,0,0,0,0], 1.7, 1, "center error determination failure")
            self.assertAlmostEqual(gs["fwhm"][0,0,0,0,0], 42.4, 1, "fwhm determination failure")
            self.assertAlmostEqual(gs["fwhmErr"][0,0,0,0,0], 4.0, 1, "fwhm error determination failure")

            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")

    def test_2(self):
        """ multipixel, two gaussian fit"""
        imagename = twogauss
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        ngauss = 2
        poly = -1
        multifit = True
        model = ""
        residual = ""
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual
            )
            self.assertTrue(len(res["converged"].ravel()) == 81)
            self.assertTrue(res["converged"].all())
            self.assertTrue(res["ncomps"][0, 0, 0, 0] == 1)
            self.assertTrue((res["ncomps"][:, 1:, 0, 0] == 2).all())
            self.assertTrue((res["ncomps"][1:, 0, 0, 0] == 2).all())
            self.assertTrue((res["type"][:,:,:,:,0] == "GAUSSIAN").all())
            self.assertTrue(res["type"][0, 0, 0, 0, 1] == "UNDEF")
            self.assertTrue((res["type"][:, 1:, 0, 0, 1] == "GAUSSIAN").all())
            self.assertTrue((res["type"][1:, 0, 0, 0, 1] == "GAUSSIAN").all())

            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")

    def test_3(self):
        """ Test two gaussian + one polynomial image"""
        imagename = polyim
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        ngauss = 2
        poly = 3
        multifit = True
        model = ""
        residual = ""
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual
            )
            self.assertTrue(len(res["converged"].ravel()) == 81)
            # fit #72 did not converge
            self.assertTrue(res["converged"][:, :7, 0, 0].all())
            self.assertTrue(res["converged"][1:,8,0,0].all())
            self.assertFalse(res["converged"][0, 8, 0, 0])

            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")

    def test_4(self):
        """writing solution images for multipixel, two gaussian fit"""
        imagename = twogauss
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        ngauss = 2
        poly = -1
        multifit = True
        model = ""
        residual = ""
        kk = [s + "-2g" for s in solims]
        [
            amp, amperr, center, centererr,
            fwhm, fwhmerr, integral, integralerr
        ] = kk
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual, amp,
                amperr, center, centererr, fwhm, fwhmerr,
                integral, integralerr
            )
            for im in kk:
                for j in ["_0", "_1"]:
                    name = im + j
                    self.checkImage(name, datapath + name)
                    shutil.rmtree(name)

    def test_4_5(self):
        """writing solution images for multipixel, two gaussian fit with mask - CAS-6134"""
        imagename = twogauss
        outfile = "CAS6134_in.im"
        myia = iatool()
        myia.open(imagename)
        subim = myia.subimage(outfile=outfile)
        myia.done()
        cc = subim.getchunk()
        cc[5,5,:,:] = 1e9
        subim.putchunk(cc)
        # so we have a mask in the output
        subim.calcmask(outfile + "<1e8")
        subim.done()
        imagename = outfile
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        ngauss = 2
        poly = -1
        multifit = True
        model = ""
        residual = ""
        [
            amp, amperr, center, centererr,
            fwhm, fwhmerr, integral, integralerr
        ] = solims
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual, amp,
                amperr, center, centererr, fwhm, fwhmerr,
                integral, integralerr
            )
            # running successfully validates that the fix worked

    def test_5(self):
        """test results of multi-pixel one gaussian fit with estimates file"""
        imagename = twogauss
        ngauss=10
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        poly = -1
        estimates = datapath + "goodProfileEstimatesFormat_2.txt"
        multifit = True
        model = ""
        residual = ""
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual,
                estimates=estimates
            )
        # no tests yet, just confirm it runs to completion

    def test_6(self):
        """test results of non-multi-pixel one gaussian fit with estimates file"""
        imagename = twogauss
        ngauss=10
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        poly = -1
        estimates = datapath + "goodProfileEstimatesFormat_2.txt"
        multifit = False
        model = ""
        residual = ""
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual,
                estimates=estimates
            )
            self.assertTrue(res['converged'] == 1)
            self.assertTrue(res['converged'][0][0][0][0])
            self.assertTrue(res['ncomps'][0][0][0][0] == 1)
            self.assertTrue(res["type"][0,0,0,0,0] == "GAUSSIAN")
            gs = res["gs"]
            self.assertAlmostEqual(gs["amp"][0,0,0,0,0], 49.7, 1, "amplitude determination failure")
            self.assertAlmostEqual(gs["ampErr"][0,0,0,0,0], 4.0, 1, "amplitude error determination failure")
            self.assertAlmostEqual(gs["center"][0,0,0,0,0], -237.7, 1, "center determination failure")
            self.assertAlmostEqual(gs["centerErr"][0,0,0,0,0], 1.7, 1, "center error determination failure")
            self.assertAlmostEqual(gs["fwhm"][0,0,0,0,0], 42.4, 1, "fwhm determination failure")
            self.assertAlmostEqual(gs["fwhmErr"][0,0,0,0,0], 4.0, 1, "fwhm error determination failure")
            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")

    def test_7(self):
        """test results of non-multi-pixel one gaussian fit with estimates file keeping peak fixed"""
        imagename = twogauss
        ngauss=10
        box = ""
        region = ""
        chans = ""
        stokes = ""
        axis = 2
        mask = ""
        poly = -1
        estimates = datapath + "goodProfileEstimatesFormat_3.txt"
        multifit = False
        model = ""
        residual = ""
        for code in [run_fitprofile]:
            res = code(
                imagename, box, region, chans,
                stokes, axis, mask, ngauss, poly,
                multifit, model, residual,
                estimates=estimates
            )
            self.assertTrue(res['converged'] == 1)
            self.assertTrue(res['converged'][0][0][0][0])
            self.assertTrue(res['ncomps'][0][0][0][0] == 1)
            self.assertTrue(res["type"][0,0,0,0,0] == "GAUSSIAN")
            self.assertAlmostEqual(res["gs"]["amp"][0,0,0,0,0], 45, 1, "amplitude determination failure")
            self.assertAlmostEqual(res["gs"]["ampErr"][0,0,0,0,0], 0, 1, "amplitude error determination failure")
            self.assertAlmostEqual(res["gs"]["center"][0,0,0,0,0], -237.7, 1, "center determination failure")
            self.assertAlmostEqual(res["gs"]["centerErr"][0,0,0,0,0], 1.9, 1, "center error determination failure")
            self.assertAlmostEqual(res["gs"]["fwhm"][0,0,0,0,0], 45.6, 1, "fwhm determination failure")
            self.assertAlmostEqual(res["gs"]["fwhmErr"][0,0,0,0,0], 3.8, 1, "fwhm error determination failure")
            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")


    def test_stretch(self):
        """ia.fitprofile(): test mask stretch"""
        imagename = twogauss
        yy = iatool()
        yy.open(imagename)
        mycsys = yy.coordsys().torecord()
        yy.done()
        mymask = "maskim"
        yy.fromshape(mymask, [9, 9, 1, 1])
        yy.setcoordsys(mycsys)
        yy.addnoise()
        yy.done()
        yy.open(imagename)
        self.assertRaises(
            Exception, yy.fitprofile,
            ngauss=2, mask=mymask + ">-100",
            stretch=False
        )
        zz = yy.fitprofile(
            ngauss=2, mask=mymask + ">-100",
            stretch=True
        )
        self.assertTrue(len(zz.keys()) > 0)
        yy.done()

    def test_8(self):
        """ Test two gaussian + one polynomial image with estimates"""

        estimates = datapath + "poly+2gauss_estimates.txt"

        for code in [run_fitprofile]:
            res = code(
                imagename=polyim, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=3,
                multifit=True, model="", residual="", estimates=estimates
            )
            self.assertTrue(len(res["converged"].ravel()) == 81)
            # fit #0 did not converge
            self.assertTrue(res["converged"][1:, :, 0, 0].all())
            self.assertTrue(res["converged"][0,1:,0,0].all())
            self.assertFalse(res["converged"][0, 0, 0, 0])

            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")
        gs = res["gs"]
        center = gs["center"]
        center[0,0,0,0,0] = nanvalue
        center[0,0,0,0,1] = nanvalue

        amp = gs["amp"]
        amp[0,0,0,0,0] = nanvalue
        amp[0,0,0,0,1] = nanvalue

        fwhm = gs["fwhm"]
        fwhm[0,0,0,0,0] = nanvalue
        fwhm[0,0,0,0,1] = nanvalue

        for code in [run_fitprofile]:
            res = code(
                imagename=polyim, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=3,
                multifit=True, model="", residual="", estimates="",
                pampest=[50, 10], pcenterest=[90, 30], pfwhmest=[10, 7]
            )
            self.assertTrue(len(res["converged"].ravel()) == 81)
            # fit #0 did not converge
            self.assertTrue(res["converged"][1:, :, 0, 0].all())
            self.assertTrue(res["converged"][0,1:,0,0].all())
            self.assertFalse(res["converged"][0, 0, 0, 0])

            self.assertTrue(res["xUnit"] == "km/s")
            self.assertTrue(res["yUnit"] == "Jy")
            self.assertTrue(len(res["direction"].ravel()) == 81)
            gs = res["gs"]
            gs["center"][0,0,0,0,0] = nanvalue
            gs["center"][0,0,0,0,1] = nanvalue
            self.assertTrue((gs["center"] == center).all())

            gs["amp"][0,0,0,0,0] = nanvalue
            gs["amp"][0,0,0,0,1] = nanvalue
            self.assertTrue((abs(gs["amp"] - amp) < 1e-13).all())

            gs["fwhm"][0,0,0,0,0] = nanvalue
            gs["fwhm"][0,0,0,0,1] = nanvalue
            self.assertTrue((abs(gs["fwhm"] - fwhm) < 1e-13).all())

    def test_8a(self):
        """ Test two gaussian + one polynomial image with estimates"""
        imagename = "tg_poly.im"
        estimates = datapath + "poly+2gauss_estimates.txt"
        myia = iatool()
        nx = 5
        ny = 5
        nz = 120
        myia.fromshape(imagename, [nx, ny, nz])
        myia.setbrightnessunit("Jy/pixel")
        bb = myia.getchunk()
        c = numpy.zeros([3, nx, ny])
        center = numpy.zeros([2, nx, ny])
        amp = numpy.zeros([2, nx, ny])
        width = numpy.zeros([2, nx, ny])
        fn = functional( )
        for i in range(nx):
            for j in range(ny):
                c[0, i, j] = 1+i+j
                c[1, i, j] = 5 + 0.1*(0.5*i*i + 0.2*j)
                c[2, i, j] = 0.1*(2*i - 0.2*j*j)
                #c[3, i, j] = (0.001)*(0.1*i + 0.2*j)
                cubic = fn.polynomial(c[:, i, j])
                amp[0, i, j] = 50 + 0.02*i - 0.03*j
                center[0, i, j] = 90 + 0.01*i - 0.03*j
                width[0, i, j] = 10 + 0.01*i - 0.025*j
                amp[1, i, j] = 10 + 0.02*i - 0.03*j
                center[1, i, j] = 30 + 0.01*i - 0.03*j
                width[1, i, j] = 7 + 0.01*i - 0.025*j
                g0 = fn.gaussian1d(amp[0, i, j], center[0, i, j], width[0, i, j])
                g1 = fn.gaussian1d(amp[1, i, j], center[1, i, j], width[1, i, j])
                for k in range(nz):
                    bb[i, j, k] = cubic.f(k)  + g0.f(k) + g1.f(k)
        myia.putchunk(bb)
        myia.done()
        for code in [run_fitprofile]:
            res = code(
                imagename=imagename, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=2,
                multifit=True, model="", residual="",
                estimates=estimates
            )
            self.assertTrue((res['ncomps'] == 3).all())
            res = code(
                imagename=imagename, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=2,
                multifit=True, model="", residual="",
                pampest=[50, 10], pcenterest=[90, 30], pfwhmest=[10, 7]
            )
            self.assertTrue((res['ncomps'] == 3).all())
        fn.done( )

    def test_9(self):
        """Polynomial fitting, moved from imagetest_regression.py"""
        shape = [16,16,128]
        imname = 'ia.fromshape.image'
        myia = iatool()
        myim = myia.newimagefromshape(shape=shape)
        myim.set(pixels='1.0')        #
        residname = 'ia.fromshape.resid'
        fitname = 'ia.fromshape.fit'
        res = myim.fitprofile (multifit=True, residual=residname, model=fitname, poly=0, ngauss=0, axis=2)
        myim.done()
        myim.open(residname)
        pixels = myim.getchunk()
        self.assertTrue(pixels.shape == tuple(shape))
        self.assertTrue((pixels == 0).all())
        myim.done()
        myia.done()

    def test_10(self):
        """test results of non-multi-fit gaussian triplet"""
        imagename=datapath+gauss_triplet
        gmampcon = [0.7, 0.55]
        gmcentercon = [52, 0]
        myia = iatool()
        for code in [run_fitprofile]:
            res = code(
                imagename=imagename, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=-1,
                multifit=False, model="", residual="", estimates="",
                gmncomps=3, gmampest=[1.2, 0.8, 0.6],
                gmcenterest=[20, 72, 100], gmfwhmest=[4, 4, 4],
                gmampcon=gmampcon, gmcentercon=gmcentercon
            )
            self.assertTrue(res["type"].ravel() == "GAUSSIAN MULTIPLET")
            gm0 = res["gm0"]
            exp = [4.15849, 2.91095, 2.28717]
            got = gm0["amp"].ravel()
            for i in [0, 1, 2]:
                self.assertAlmostEqual(got[i], exp[i], 5)
            exp = [1149.73, 1138.76, 1133.66]
            got = gm0["center"].ravel()
            for i in [0, 1, 2]:
                self.assertAlmostEqual(got[i], exp[i], 2)
            exp = [5.75308, 4.09405, 3.93497]
            got = gm0["fwhm"].ravel()
            for i in [0, 1, 2]:
                self.assertAlmostEqual(got[i], exp[i], 5)
            exp = [0.0301945, 0.0211362, 0.016607]
            got = gm0["ampErr"].ravel()
            for i in [0, 1, 2]:
                self.assertAlmostEqual(got[i], exp[i], 7)
            exp = [0.0221435, 0.0221435, 0.0475916]
            got = gm0["centerErr"].ravel()
            for i in [0, 1, 2]:
                self.assertAlmostEqual(got[i], exp[i], 7)
            exp = [0.0556099, 0.085414, 0.0987483]
            got = gm0["fwhmErr"].ravel()
            for i in [0, 1, 2]:
                self.assertAlmostEqual(got[i], exp[i], 7)
            self.assertAlmostEqual(
                gm0["amp"].ravel()[0]*gmampcon[0],
                gm0["amp"].ravel()[1], 7
            )
            self.assertAlmostEqual(
                gm0["ampErr"].ravel()[0]*gmampcon[0],
                gm0["ampErr"].ravel()[1], 7
            )
            self.assertAlmostEqual(
                gm0["amp"].ravel()[0]*gmampcon[1],
                gm0["amp"].ravel()[2], 7
            )
            self.assertAlmostEqual(
                gm0["ampErr"].ravel()[0]*gmampcon[1],
                gm0["ampErr"].ravel()[2], 7
            )
            myia.open(imagename)
            mc = myia.coordsys()
            myia.done()
            restfreq = mc.restfrequency()["value"][0]
            dv = mc.increment()["numeric"][2]
            increment = -dv/restfreq*299797
            self.assertAlmostEqual(
                gm0["center"].ravel()[0] + gmcentercon[0]*increment,
                gm0["center"].ravel()[1], 3
            )
            self.assertAlmostEqual(
                gm0["centerErr"].ravel()[0],
                gm0["centerErr"].ravel()[1], 7
            )
        myia.done()

    def test_11(self):
        """test results of multi-fit gaussian triplet"""
        imagename=datapath+gauss_triplet
        gmampcon = [0.7, 0.55]
        gmcentercon = [52, 0]
        logfile = "mylog.txt"
        i = 1
        for code in [run_fitprofile]:
            res = code(
                imagename=imagename, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=-1,
                multifit=True, center="center",
                centererr="centerErr", fwhm="fwhm",
                fwhmerr="fwhmErr", amp="amp", amperr="ampErr",
                integral="integral", integralerr="integralErr",
                gmncomps=3, gmampest=[1.2, 0.1, 0.1],
                gmcenterest=[20, 0, 100], gmfwhmest=[4, 4, 4],
                gmampcon=gmampcon, gmcentercon=gmcentercon,
                logfile=logfile
            )
            for image in (
                "center", "centerErr", "fwhm", "fwhmErr", "amp",
                "ampErr", "integral", "integralErr"
            ):
                for j in ["0", "1", "2"]:
                    self.checkImage(
                        image + "_gm_" + j,
                        datapath + image + "_gm_" + j
                    )
            # appending, second time through size should double
            self.assertTrue(os.path.getsize(logfile) > 3e4*i)
            i = i+1

    def test_12(self):
        """test results of lorentzian fitting"""
        imagename=datapath+two_lorentzians
        pamp = [1, 7]
        pcen = [30, 111]
        pfwhm = [4, 4]
        pfunc = ["l", "l"]

        logfile = "two_lorentzian_fit.log"
        i = 1
        for code in [run_fitprofile]:
            res = code(
                imagename=imagename, box="", region="", chans="",
                stokes="", axis=2, mask="", ngauss=0, poly=-1,
                multifit=True, center="center",
                centererr="centerErr", fwhm="fwhm",
                fwhmerr="fwhmErr", amp="amp", amperr="ampErr",
                integral="integral", integralerr="integralErr",
                pampest=pamp, pcenterest=pcen, pfwhmest=pfwhm,
                logfile=logfile, pfunc=pfunc
            )
            for image in (
                "center", "centerErr", "fwhm", "fwhmErr", "amp",
                "ampErr", "integral", "integralErr"
            ):
                for j in ["0", "1"]:
                    myim = image + "_ls_" + str(j)
                    self.checkImage(
                        myim, datapath + myim
                    )
            # appending, second time through size should double
            self.assertTrue(os.path.getsize(logfile) > 2e4*i)
            i = i+1

    def test_13(self):
        """test setting solution parameter validities """
        imagename = datapath + invalid_fits
        i = 0
        goodfwhmrange= [[0], [2,20]]
        for gfr in goodfwhmrange:
            for code in [run_fitprofile]:
                res = code(
                    imagename=imagename, box="", region="", chans="",
                    stokes="", axis=3, mask="", ngauss=2, poly=-1,
                    multifit=True, logresults=False, goodfwhmrange=gfr
                )
                if i == 0:
                    exp = 16
                else:
                    exp = 6
                self.assertTrue(res["valid"].sum() == exp)
            i = i+1

    def test_14(self):
        imagename = datapath + birdie
        sigmaimage = "sigma.im"
        myia = iatool()
        myia.open(imagename)
        bb = myia.getchunk()
        myia.done()
        fullsigma = bb
        fullsigma[:] = 1
        s = []
        s.append(fullsigma[:])
        s.append(fullsigma[0, 0, :, 0].reshape([1, 1, 100, 1]))
        s.append(fullsigma[0, 0, :, 0])
        outsigma = "outsigma.im"
        expfwhmtrue = {
            "0": [ 61.77550675, 41.74020775],
            "0.1" : [82.55515563, 36.4484157],
            "1": [ 61.89796905, 41.69283371],
            "100": [ 61.77551883, 41.74020306]
        }
        expfwhmerrtrue = {
            "0": [  2.88886469e-07, 4.18990637e-07],
            "0.1": [ 1.96169836, 2.99212108],
            "1": [ 0.30788521, 0.44558165],
            "100": [ 0.00307781, 0.00446394]
        }
        expfwhmfalse = {
            "0": 42.43941294,
            "0.1": 42.43941294,
            "1": 42.43941294,
            "100":  42.43941294
        }
        expfwhmerrfalse = {
            "0": 4.0707913,
            "0.1": 6.09143172,
            "1": 4.07523664,
            "100": 4.04975604
        }
        for sigma in (s):
            for birdiesigma in [0, 0.1, 1, 100]:
                fullsigma[:, :, 50, :] = birdiesigma
                mymax = max(1, birdiesigma)
                for i in range(2):
                    sig = sigma
                    if (sig.ndim == 1):
                        sig[50] = birdiesigma
                    else:
                        sig[:,:,50,:] = birdiesigma
                    if i == 1:
                        myia.fromarray(sigmaimage, sig, overwrite=True)
                        myia.done()
                        sig = sigmaimage
                    for code in [run_fitprofile]:
                        for multifit in [True, False]:
                            res = code(
                                imagename=imagename, box="", region="", chans="",
                                stokes="", axis=2, mask="'" + ctsys.resolve(imagename) + "'<1000", ngauss=2, poly=-1,
                                multifit=multifit, logresults=False, sigma=sig,
                                outsigma=outsigma
                            )
                            if multifit:
                                self.assertTrue((abs(res["gs"]["fwhm"][8,8,0,0,:] - expfwhmtrue[str(birdiesigma)]) < 1e-7).all())
                                self.assertTrue((abs(res["gs"]["fwhmErr"][8,8,0,0,:] - expfwhmerrtrue[str(birdiesigma)]) < 1e-7).all())
                            else:
                                self.assertTrue(abs(res["gs"]["fwhm"][0,0,0,0,0] - expfwhmfalse[str(birdiesigma)]) < 1e-7)
                                self.assertTrue(abs(res["gs"]["fwhmErr"][0,0,0,0,0] - expfwhmerrfalse[str(birdiesigma)]) < 1e-5)
                            myia.open(outsigma)
                            if (birdiesigma == 0 or birdiesigma == 1):
                                self.assertTrue((mymax*myia.getchunk() == fullsigma).all())
                            else:
                                self.assertTrue(((mymax*myia.getchunk() - fullsigma)/fullsigma < 1e-7).all())
                            myia.remove()

    def test_planes(self):
        """Test setting planes to use for fit"""
        myia = iatool()
        myrg = regionmanager( )
        myia.fromshape("", [1, 1, 20])
        myia.setbrightnessunit("Jy/pixel")
        bb = myia.getchunk()
        for i in range(20):
            bb[:,:,i] = 0.2 - 0.3*i + 0.04*i*i
        bb[:,:,5] = 1000
        bb[:,:,10] = 1000
        bb[:,:,15] = 1000
        myia.putchunk(bb)
        residual = "bad.im"
        res = myia.fitprofile(ngauss=0, poly=2, residual=residual)
        zz = list(range(20))
        del zz[15]
        del zz[10]
        del zz[5]
        mask = "indexin(2," + str(zz) +")"
        tt = iatool()
        tt.open(residual)
        stats = tt.statistics(mask=mask)
        tt.done()
        self.assertTrue(stats['rms'][0] > 100)
        residual = "good1.im"
        res2 = myia.fitprofile(ngauss=0, poly=2, planes=zz, residual=residual)
        tt.open(residual)
        stats = tt.statistics(mask=mask)
        tt.done()
        self.assertTrue(stats['rms'][0] < 1e-6)
        residual = "good2.im"
        res3 = myia.fitprofile(
            ngauss=0, poly=2, planes=zz, residual=residual,
            region=myrg.box([0,0,2], [0,0,18])
        )
        tt.open(residual)
        mask = mask = "indexin(2,[0, 1, 2, 4, 5, 6, 7, 9, 10, 11, 12, 14, 15])"
        stats = tt.statistics(mask=mask)
        tt.done()
        self.assertTrue(stats['rms'][0] < 1e-6)
        residual = "good3.im"
        res4 = myia.fitprofile(
            ngauss=0, poly=2, planes=zz, residual=residual,
            region=myrg.box([0,0,6], [0,0,18])
        )
        tt.open(residual)
        mask = mask = "indexin(2,[0, 1, 2, 3, 5, 6, 7, 8, 10, 11])"
        stats = tt.statistics(mask=mask)
        tt.done()
        self.assertTrue(stats['rms'][0] < 1e-6)
        myrg.done
        myia.done()

    def test_CAS_7620(self):
        """Test fix of segfault that occurred for small channel ranges"""
        imagename = twogauss
        box = ""
        region = ""
        chans = "3~4"
        stokes = ""
        axis = 2
        mask = ""
        ngauss = 1
        poly = -1
        multifit = False
        model = ""
        residual = ""
        self.assertRaises(
            Exception, run_fitprofile,
            imagename, box, region, chans,
            stokes, axis, mask, ngauss, poly,
            multifit, model, residual
        )

    ### begin tests for spectral index fitting
    def test_exceptions(self):
        """spxfit: Test various exception cases"""
        myia = iatool()
        myia.fromshape("", [1,1,10])
        self.assertRaises(Exception, myia.fitprofile, poly=2, plpest=[1,2])

    def test_plpfit(self):
        """ Test fitting a power logarithmic polynomial"""
        imagename = "spxfit.im"
        myia = iatool()
        myia.fromshape(imagename,[2, 2, 100])
        csys = myia.coordsys()
        inc = csys.increment()['numeric']
        inc[2] = 1e7
        csys.setincrement(inc)
        myia.setcoordsys(csys.torecord())
        zz = myia.getchunk()
        plpest = [0.5, 2]
        fn = functional()
        myfn = fn.powerlogpoly(plpest)
        for i in range(zz.shape[2]):
            world = myia.toworld([0,0,i])['numeric'][2]
            zz[:,:,i] = myfn.f(world/1e9)
        myia.putchunk(zz)

        rec = myia.fitprofile(ngauss=0, spxtype="plp", spxest=plpest)
        sols = rec['plp']['solution'].ravel()
        self.assertTrue((abs(1 - sols/plpest) < 0.1e-7).all())
        if i == 1:
            rec = myia.fitprofile(ngauss=0, spxtype="plp", spxest=[0.4, 3])
        sols = rec['plp']['solution'].ravel()
        self.assertTrue((abs(1 - sols/plpest) < 0.1e-7).all())

        myia.addnoise(pars=[0, 0.001])
        plpestoff = [0.4, 3]
        rec = myia.fitprofile(ngauss=0, spxtype="plp", spxest=plpestoff)
        sols = rec['plp']['solution'].ravel()
        print("*** i %d" % i)
        print("** max %s" % (sols/plpest))
        self.assertTrue((abs(1 - sols/plpest) < 4e-2).all())
        plpsol = "plpsol.im"
        plperr = "plperr.im"
        plpestoff = [0.4, 2.2]
        plpfix = [False, True]
        rec = myia.fitprofile(
            ngauss=0, spxtype="plp", spxest=plpestoff, spxfix=plpfix,
            multifit=True, spxsol=plpsol, spxerr=plperr
        )
        sols = rec['plp']['solution']
        self.assertTrue((sols[:,:,:,1] == 2.2).all())
        for j in [0, 1]:
            myia.open(plpsol + "_" + str(j))
            self.assertTrue(
                (abs(myia.getchunk()/sols[:,:,:,j] - 1) < 1e-7).all()
            )
            myia.done(remove=True)

            myia.open(plperr + "_" + str(j))
            self.assertTrue(
                (abs(myia.getchunk() - rec['plp']['error'][:, :, :, j]) < 1e-8).all()
            )
            myia.done(remove=True)

    def test_ltpfit(self):
        """ Test fitting a logarithmic transformed polynomial"""
        imagename = "ltpfit.im"
        myia = iatool()
        myia.fromshape(imagename,[2, 2, 100])
        csys = myia.coordsys()
        inc = csys.increment()['numeric']
        inc[2] = 1e7
        csys.setincrement(inc)
        myia.setcoordsys(csys.torecord())
        zz = myia.getchunk()
        plpest = [0.5, 2]
        fn = functional()
        myfn = fn.powerlogpoly(plpest)
        for i in range(zz.shape[2]):
            world = myia.toworld([0,0,i])['numeric'][2]
            zz[:,:,i] = myfn.f(world/1e9)
        myia.putchunk(zz)
        ltpest = plpest
        ltpest[:] = plpest
        ltpest[0] = math.log(plpest[0])
        rec = myia.fitprofile(ngauss=0, spxtype="ltp", spxest=ltpest)
        print(str(rec))
        sols = rec['ltp']['solution'].ravel()
        self.assertTrue((abs(1 - sols/ltpest) < 0.1e-7).all())
        rec = myia.fitprofile(ngauss=0, spxtype="ltp", spxest=[0.4, 3])
        sols = rec['ltp']['solution'].ravel()
        self.assertTrue((abs(1 - sols/ltpest) < 0.1e-7).all())
        print('*** xUnit %s' % rec['xUnit'])
        self.assertTrue(rec['xUnit'] == "Hz")

        myia.addnoise(pars=[0, 0.001])
        ltpestoff = [0.4, 3]
        rec = myia.fitprofile(ngauss=0, spxtype="ltp", spxest=ltpestoff)
        sols = rec['ltp']['solution'].ravel()
        self.assertTrue(
            (abs(1 - sols/ltpest) < 3e-2).all(),
            "sols " + str(sols) + " ltpest " + str(ltpest)
        )
        spxsol = "ltpsol.im"
        spxerr = "ltperr.im"
        ltpestoff = [0.4, 2.2]
        ltpfix = [False, True]
        rec = myia.fitprofile(
            ngauss=0,  spxtype="ltp", spxest=ltpestoff, spxfix=ltpfix,
            multifit=True, spxsol=spxsol, spxerr=spxerr
        )
        sols = rec['ltp']['solution']
        self.assertTrue((sols[:,:,:,1] == 2.2).all())
        for j in [0, 1]:
            self.assertTrue(myia.open(spxsol + "_" + str(j)))
            self.assertTrue(
                (abs(myia.getchunk()/sols[:,:,:,j] - 1) < 1e-7).all()
            )
            myia.done(remove=True)

            self.assertTrue(myia.open(spxerr + "_" + str(j)))
            self.assertTrue(
                (abs(myia.getchunk() - rec['ltp']['error'][:,:,:,j]) < 1e-8).all()
            )
            myia.done(remove=True)

    def test_ltpfit_with_negative_values(self):
        """Test fitting of ltp when y values are negative returns something reasonable because of auto masking"""
        myia = iatool()
        myia.fromshape("", [1, 1, 10])
        f = []
        for i in range(10):
            f.append(myia.toworld([0,0,i])['numeric'][2])
        f = numpy.array(f)
        vals = (f/f[0])**(3.0)
        # add a negative value to the array to test that it's not used in the fit
        vals[0] = -1
        bb = myia.getchunk()
        bb[0, 0, :] = vals
        myia.putchunk(bb)
        res = myia.fitprofile(spxtype="ltp",spxest=[0, 3],div=f[0])
        myia.done()
        self.assertTrue(abs(res['ltp']['solution'][0][0][0][1] - 3) < 0.01 )

if __name__ == '__main__':
    unittest.main()
