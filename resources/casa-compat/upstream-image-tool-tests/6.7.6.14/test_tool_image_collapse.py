##########################################################################
# test_tool_image_collapse.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.collapse
#
##########################################################################
import shutil
import unittest
import numpy
import os
from math import sqrt

from casatools import image as iatool
from casatools import regionmanager
from casatools import table
from casatools import quanta
from casatools import ctsys
ctsys_resolve = ctsys.resolve


good_image = "collapse_in.fits"
masked_image = "im_w_mask.im"
datapath = ctsys_resolve('unittest/ia_collapse/')

def run_collapse(
    imagename, function, axes, outfile, region, box, chans,
    stokes, mask, overwrite, stretch=False
):
    myia = iatool()
    myia.open(imagename)
    res = myia.collapse(
        function=function, axes=axes, outfile=outfile,
        region=region, box=box, chans=chans, stokes=stokes,
        mask=mask, overwrite=overwrite, stretch=stretch
    )
    myia.done()
    return res

class ia_collapse_test(unittest.TestCase):

    def setUp(self):
        self.rg = regionmanager( )
        self.qa = quanta( )
#        shutil.copy(ctsys.resolve(datapath + good_image), good_image)
        shutil.copy(datapath + good_image, good_image)
        self.tabular_spectral_image = datapath + "longZax"

    def tearDown(self):
        self.qa.done( )
        self.rg.done( )
        os.remove(good_image)
        data = ["collapse_avg_0_1.fits","collapse_avg_0.fits",
                "collapse_avg_2.fits","collapse_in.im",
                "collapse_sum_1.fits","flux_test.im","median.im",
                "test_1_0.im","test_1_right.im","test_1_r.im",
                "test_2_2.im","test_2_f.im","test_2_freq.im",
                "test_3.im","test_8_0mean","test_8_0median","test_8_1mean",
                "test_8_1median","test_8_2mean","test_8_2median","ymask"
            ]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)


        tb = table( )
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def checkImage(self, gotImage, expectedName):
        expected = iatool()
        expected.open(expectedName)
        got = iatool()
        if type(gotImage) == str:
            got.open(gotImage)
        else:
            got = gotImage
        self.assertTrue(all(got.shape() == expected.shape()))
        diffData = got.getchunk() - expected.getchunk()
        self.assertTrue(abs(diffData).max() == 0)
        gotCsys = got.coordsys()
        expectedCsys = expected.coordsys()
        diffPixels = gotCsys.referencepixel()['numeric'] - expectedCsys.referencepixel()['numeric']
        self.assertTrue(abs(diffPixels).max() == 0)
        fracDiffRef = (
            gotCsys.referencevalue()['numeric'] - expectedCsys.referencevalue()['numeric']
        )/expectedCsys.referencevalue()['numeric'];
        self.assertTrue(abs(fracDiffRef).max() <= 1.5e-6)
        beam = got.restoringbeam()
        self.assertTrue(len(beam) == 3)
        self.assertTrue(abs(beam["major"]["value"] - 1) < 1.5e-6)
        self.assertTrue(abs(beam["minor"]["value"] - 1) < 1.5e-6)
        self.assertTrue(abs(beam["positionangle"]["value"] - 40) < 1.5e-6)
        got.close()
        got.done()
        expected.close()
        expected.done()

    def test_exceptions(self):
        """ia.collapse: Test various exception cases"""

        bogus = "mybogus.im"
        def testit(
            imagename, function, axes, outfile, region,
            box, chans, stokes, mask, overwrite, wantreturn
        ):
            self.assertRaises(
                Exception, run_collapse, imagename,
                function, axes, outfile, region, box,
                chans, stokes, mask, overwrite
            )

        # bad image name given
        testit(bogus, "mean", 0, "", "", "", "", "", "", False, True)
        # no function given
        testit(good_image, "", 0, "", "", "", "", "", "", False, True)
        # bogus function given
        testit(good_image, "bogus function", 0, "", "", "", "", "", "", False, True)
        # bogus region given
        testit(good_image, "mean", 0, "", "bogus_region", "", "", "", "", False, True)
        #bogus box
        testit(good_image, "mean", 0, "", "", "abc", "", "", "", False, True)
        # another bogus box
        testit(good_image, "mean", 0, "", "", "0,0,1000,1000", "", "", "", False, True)
        # no axes
        testit(good_image, "mean", "", "", "", "", "", "", "", False, True)
        # bogus axes
        testit(good_image, "mean", 10, "", "", "", "", "", "", False, True)

    def test_1(self):
        """ia.collapse(): average full image collapse along axis 0"""
        expected = "collapse_avg_0.fits"
        shutil.copy(datapath + expected, expected)
        for axis in (0 ,"r", "right"):
            outname = "test_1_" + str(axis) + ".im"
            mytool = run_collapse(
                good_image, "mean", axis, outname, "", "",
                "", "", "", False
            )
            self.assertTrue(isinstance(mytool,iatool))
            self.checkImage(mytool, expected)
            self.checkImage(outname, expected)

    def test_2(self):
        """ia.collapse(): average full image collapse along axis 2"""
        expected = "collapse_avg_2.fits"
        shutil.copy(datapath + expected, expected)
        for axis in (2, "f", "freq"):
            outname = "test_2_" + str(axis) + ".im"
            mytool = run_collapse(
                good_image, "mean", axis, outname, "", "",
                "", "", "", False
            )
            self.assertTrue(isinstance(mytool,iatool))
            self.checkImage(mytool, expected)
            self.checkImage(outname, expected)

    def test_3(self):
        """ia.collapse(): average full image collapse along axis 2 and check output overwritability"""
        expected = "collapse_sum_1.fits"
        shutil.copy(datapath + expected, expected)
        box = "1,1,2,2"
        chans = "1~2"
        stokes = "qu"
        outname = "test_3.im"
        mytool = run_collapse(
            good_image, "sum", 1, outname, "", box,
            chans, stokes, "", False
        )
        # this should throw an exception because we are trying to overwrite a file
        # that is open in the table cache
        self.assertRaises(
            Exception, run_collapse, good_image, "sum", 1, outname, "", box,
            chans, stokes, "", True
        )
        mytool.done()
        # now the image is closed, so check that can overwrite previous output. Then check output image
        mytool = run_collapse(
            good_image, "sum", 1, outname, "", box,
            chans, stokes, "", True
        )
        self.assertTrue(isinstance(mytool,iatool))
        self.checkImage(mytool, expected)
        self.checkImage(outname, expected)
        mytool.done()

    def test_4(self):
        """ia.collapse(): not specifying an output image is ok"""
        expected = "collapse_avg_2.fits"
        shutil.copy(datapath + expected, expected)
        mytool = run_collapse(
            good_image, "mean", 2, "", "", "",
            "", "", "", False
        )
        self.assertTrue(isinstance(mytool,iatool))
        self.checkImage(mytool, expected)

    def test_6(self):
        """ia.collapse(): memory only images can be collapsed"""
        mytool = run_collapse(
            good_image, "mean", 2, "", "", "",
            "", "", "", False
        )
        mytool2 = mytool.collapse("mean", 3)
        expected = [3, 3, 1, 1]
        self.assertTrue(all(mytool2.shape() == expected))

    def test_7(self):
        """ia.collapse(): verify collapsing along multiple axes works"""
        expected = "collapse_avg_0_1.fits"
        shutil.copy(datapath + expected, expected)
        for axes in ([0, 1], ["r", "d"], ["right", "dec"]):
            mytool = run_collapse(
                good_image, "mean", axes, "", "", "",
                "", "", "", False
            )
            self.assertTrue(isinstance(mytool,iatool))
            self.checkImage(mytool, expected)

    def test_8(self):
        """ia.collapse(): test both OTF and permanent masking works"""
        xx = iatool()
        good_image_im = "collapse_in.im"
        xx.fromfits(good_image_im, good_image)
        xx.calcmask(good_image_im + "<78")
        xx.close()
        xx.done()
        mytool = False
        axes = 3
        for j in [0, 1, 2]:
            mask = good_image_im + ">7"
            if j == 0:
                xx.open(good_image_im)
                xx.maskhandler("set", "")
                xx.close()
                xx.done()
            if j == 1:
                mask = ""
                xx.open(good_image_im)
                xx.maskhandler("set", "mask0")
                xx.close()
                xx.done()
            for func in ["mean", "median"]:
                for outfile in ["", "test_8_" + str(j) + func]:
                    mytool = run_collapse(
                        good_image_im, func, axes, outfile, "", "",
                        "", "", mask, False
                    )
                    self.assertTrue(isinstance(mytool,iatool))
                    npts = mytool.statistics()["npts"]
                    mytool.close()
                    mytool.done()
                    if (j == 0):
                        self.assertTrue(npts == 25)
                    elif (j == 1):
                        self.assertTrue(npts == 26)
                    else:
                        self.assertTrue(npts == 24)

    def test_median(self):
        """Test median when collapsing along multiple axes"""
        myia = iatool()
        imagename = "median.im"
        myia.fromshape(imagename, [3, 3, 3])
        bb = myia.getchunk()
        count = 0
        for i in range(3):
            for j in range(3):
                for k in range(3):
                    bb[i, j, k] = count
                    count += 1
        myia.putchunk(bb)
        collapsed = myia.collapse(axes=[0, 1], function="median")
        bb = collapsed.getchunk()
        self.assertTrue(bb[0, 0, 0] == 12)
        self.assertTrue(bb[0, 0, 1] == 13)
        self.assertTrue(bb[0, 0, 2] == 14)
        collapsed.done()

        collapsed = myia.collapse(
            axes=[0, 1], function="median",
            mask=imagename + "<14 || " + imagename + ">16"
        )
        bb = collapsed.getchunk()
        self.assertTrue(bb[0, 0, 0] == 10.5)
        self.assertTrue(bb[0, 0, 1] == 11.5)
        self.assertTrue(bb[0, 0, 2] == 14)
        collapsed.done()

        myia.fromshape("", [20, 20, 5])
        reg = self.rg.fromtext(
            "circle [[10pix, 10pix], 5pix]", csys=myia.coordsys().torecord(),
            shape=myia.shape()
        )
        collapsed = myia.collapse(axes=[0, 1], function="median", region=reg)
        myia.done()
        collapsed.done()

    def test_CAS_3418(self):
        """ia.collapse(): Test separate code for median due to performance issues"""
        for i in range(0,4):
            xx = iatool()
            xx.open(good_image)
            exp = xx.statistics(robust=True, axes=i)["median"]
            xx.done()
            mytool = run_collapse(
                good_image, "median", i, "", "", "",
                "", "", "", False
            )
            zz = mytool.subimage("", dropdeg=True)
            got = zz.getchunk()
            self.assertTrue((got == exp).all())
            mytool.done()
            zz.done()

    def test_region(self):
        """ ia.collapse(): Test region"""
        myia = iatool()
        myia.fromshape("", [10, 10, 10])
        bb = myia.getchunk()
        for i in range(10):
            bb[i,5,:] = i
            bb[i,0:5,:] = i+1
            bb[i,6:10,:] = i+2
        myia.putchunk(bb)
        res = myia.collapse("mean", 1, box="0,4,9,6")
        expec = myia.makearray(0, [10, 1, 10])
        for i in range(10):
            expec[i, 0, :] = i+1
        got = res.getchunk()
        self.assertTrue((expec == got).all())

    def test_stretch(self):
        """ ia.collapse(): Test stretch parameter"""
        yy = iatool()
        yy.open(good_image)
        mycs = yy.coordsys().torecord()
        yy.done()
        maskim = "ymask"
        yy.fromshape(maskim,[3,3,1,1])
        bb = yy.getchunk()
        bb = bb + 1
        bb[1,1] = -1
        yy.putchunk(bb)
        yy.setcoordsys(mycs)
        yy.done()
        yy = run_collapse(
            good_image, "mean", 0, "", "", "", "",
            "", maskim + ">0", False, stretch=True
        )
        self.assertTrue(isinstance(yy,iatool))
        yy.done()

    def test_CAS3737(self):
        """ ia.collapse(): test tabular spectral axis has correct collapsed reference value """
        image = self.tabular_spectral_image
        for chans in ["2445~2555", "range=[2445pix,2555pix]"]:
            mytool = run_collapse(
                image, "mean", 2, "", "", "",
                chans, "", "", False
            )
            expected = 98318505973583.641
            got = mytool.toworld([0,0,0])["numeric"][2]
            mytool.done()
            frac = got/expected - 1
            self.assertTrue(frac < 1e-6 and frac > -1e-6)

    def test_beams(self):
        """test per plane beams"""
        myia = iatool()
        myia.fromshape("", [10, 10, 10, 4])
        myia.setrestoringbeam(
            major="4arcsec", minor="3arcsec",
            pa="20deg", channel=1, polarization=1
        )
        for i in range (myia.shape()[2]):
            for j in range(myia.shape()[3]):
                major = self.qa.quantity(4 + i + j, "arcsec")
                minor = self.qa.quantity(2 + i + 0.5*j, "arcsec")
                pa = self.qa.quantity(10*i + j, "deg")
                myia.setrestoringbeam(
                    major=major, minor=minor, pa=pa,
                    channel=i, polarization=j
                )
        reg = self.rg.box(blc=[1,1,1,1], trc=[2,2,2,2])
        collapsed = myia.collapse(function="mean", axes=2, outfile="", region=reg)
        beam = collapsed.restoringbeam()
        self.assertTrue(len(beam) == 3)
        self.assertTrue(beam["major"] == self.qa.quantity(6, "arcsec"))
        self.assertTrue(beam["minor"] == self.qa.quantity(3.5, "arcsec"))
        self.assertTrue(beam["positionangle"] == self.qa.quantity(11, "deg"))
        myia.done()
        collapsed.done()

    def test_complex(self):
        """Test support for complex valued images"""
        myia = iatool()

        myia.fromshape("", [2, 2, 2], type='c')
        bb = myia.getchunk()
        counter = 0
        for i in [0, 1]:
            for j in [0, 1]:
                for k in [0, 1]:
                    bb[i, j, k] = counter*(1-1j)
                    counter += 1
        myia.putchunk(bb)
        col = myia.collapse("min", [2])
        got = col.subimage(dropdeg=True).getchunk()
        exp = numpy.min(bb, 2)
        self.assertTrue((got == exp).all())

        col = myia.collapse("mean", [2])
        got = col.subimage(dropdeg=True).getchunk()
        exp = numpy.average(bb, 2)
        self.assertTrue((got == exp).all())

        myia.done()
        col.done()

    def test_flux(self):
        """Test flux function"""
        myia = iatool()
        imagename = "flux_test.im"
        myia.fromshape(imagename, [10, 10, 10])
        bb = myia.getchunk()
        bb[:] = 1
        bb[0,0,0] = 0
        myia.putchunk(bb)
        self.assertRaises(Exception, myia.collapse, axes=[0,1], function="flux")
        myia.setrestoringbeam(major="3arcmin", minor="3arcmin", pa="0deg")
        myia.setbrightnessunit("Jy/beam")
        col = myia.collapse(axes=[0,1], function="flux", mask=imagename + "> 0")
        self.assertTrue((col.shape() == [1, 1, 10]).all())
        bb = col.getchunk()
        for i in range(10):
            if i == 0:
                self.assertTrue(abs(bb[0,0,i] - 9.707966) < 1e-5)
            else:
                self.assertTrue(abs(bb[0,0,i] - 9.806027) < 1e-5)
        col.done()
        myia.done()

    def test_sqrtsum(self):
        """Test sqrtsum function"""
        myia = iatool()
        myia.fromshape("",[2,2,2])
        bb = myia.getchunk()
        bb[:, :, 0] = 1
        bb[:, :, 1] = 2
        myia.putchunk(bb)
        zz = myia.collapse(axes=[0,1], function="sqrtsum")
        bb = zz.getchunk()
        self.assertTrue(bb[0, 0, 0] == 2)
        self.assertTrue(abs(bb[0, 0, 1] - 2*sqrt(2)) < 1e-6)
        bb = myia.getchunk()
        bb[:, :, 0] = -1
        myia.putchunk(bb)
        zz = myia.collapse(axes=[0,1], function="sqrtsum")
        bb = zz.getchunk()
        self.assertTrue(bb[0, 0, 0] == 0)
        self.assertTrue(abs(bb[0, 0, 1] - 2*sqrt(2)) < 1e-6)

    def test_sqrtsum_npix(self):
        """Test sqrtsum function"""
        myia = iatool()
        myia.fromshape("",[2,2,2])
        bb = myia.getchunk()
        bb[:, :, 0] = 1
        bb[:, :, 1] = 2
        myia.putchunk(bb)
        zz = myia.collapse(axes=[0,1], function="sqrtsum_npix")
        bb = zz.getchunk()
        self.assertTrue(bb[0, 0, 0] == 0.5)
        self.assertTrue(abs(bb[0, 0, 1] - 0.5*sqrt(2)) < 1e-6)
        bb = myia.getchunk()
        bb[:, :, 0] = -1
        myia.putchunk(bb)
        zz = myia.collapse(axes=[0,1], function="sqrtsum_npix")
        bb = zz.getchunk()
        self.assertTrue(bb[0, 0, 0] == 0)
        self.assertTrue(abs(bb[0, 0, 1] - 0.5*sqrt(2)) < 1e-6)

    def test_sqrtsum_npix_beam(self):
        """Test sqrtsum function"""
        myia = iatool()
        myia.fromshape("",[2,2,2])
        myia.setrestoringbeam(major="3arcmin", minor="3arcmin", pa="0deg")
        bb = myia.getchunk()
        bb[:, :, 0] = 1
        bb[:, :, 1] = 2
        myia.putchunk(bb)
        zz = myia.collapse(axes=[0,1], function="sqrtsum_npix_beam")
        bb = zz.getchunk()
        self.assertTrue(abs(bb[0, 0, 0] - 0.19612053) < 1e-6)
        self.assertTrue(abs(bb[0, 0, 1] - 0.27735632) < 1e-6)
        bb = myia.getchunk()
        bb[:, :, 0] = -1
        myia.putchunk(bb)
        zz = myia.collapse(axes=[0,1], function="sqrtsum_npix_beam")
        bb = zz.getchunk()
        self.assertTrue(bb[0, 0, 0] == 0)
        self.assertTrue(abs(bb[0, 0, 1] - 0.27735632) < 1e-6)

    def test_history(self):
        """Test history record is written"""
        myia = iatool()
        myia.fromshape("",[20,20,20])
        bb = myia.collapse(function="mean", axes=2)
        myia.done()
        msgs = bb.history()
        bb.done()
        self.assertTrue("ia.collapse" in msgs[-1])

if __name__ == '__main__':
    unittest.main()
