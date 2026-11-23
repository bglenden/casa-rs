##########################################################################
# test_tool_image_pbcor.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.pbcor
#
##########################################################################

import os
import shutil
import unittest

from casatools import ctsys, image, table
_ia = image()
_tb = table()
datapath = ctsys.resolve('unittest/ia_pbcor/')

def run_pbcor(
    imagename, pbimage, outfile, overwrite, region, box, chans,
    stokes, mask, mode, cutoff
):
    myia = image()
    myia.open(imagename)
    res = myia.pbcor(
        pbimage=pbimage, outfile=outfile, overwrite=overwrite,
        region=region, box=box, chans=chans, stokes=stokes,
        mask=mask, mode=mode, cutoff=cutoff
    )
    myia.close()
    myia.done()
    return res

class ia_pbcor_test(unittest.TestCase):

    def setUp(self):
        self.im1 = "pbtest1_im.fits"
        self.pb1 = "pbtest1_pb.fits"
        self.co1_1 = "pbtest1_co1.fits"
        self.co1_2 = "pbtest1_co2.im"

        self.im2 = "pb2_im.fits"
        self.pb2 = "pb2_pb.fits"
        self.co2 = "pb2_co.im"

        self.pb4 = "CAS_5096template.im"
        self.outfile = ''
        self.mymask = ''
        self.imagename = ''
        self.pbimage = ''
        self.newpb = ''
        data = [self.im1, self.pb1, self.co1_1, self.co1_2, self.im2, self.pb2, self.co2, self.pb4]
        for f in data:
            resolved = os.path.join(datapath, f)
            if os.path.isdir(resolved):
                shutil.copytree(resolved, f)
            if os.path.isfile(resolved):
                shutil.copy(resolved,f)

    def tearDown(self):
        data = [self.im1, self.pb1, self.co1_1, self.co1_2, self.im2, self.pb2, self.co2, self.pb4, self.outfile, self.mymask, self.imagename, self.pbimage, self.newpb]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        self.assertTrue(len(_tb.showcache()) == 0)


    def checkImage(self, gotImage, expectedName, epsilon):
        expected = image()
        expected.open(expectedName)
        got = image()
        if type(gotImage) == str:
            got.open(gotImage)
        else:
            got = gotImage
        self.assertTrue((got.shape() == expected.shape()).all(), 'image shape failure')
        diffData = got.getchunk()/expected.getchunk() - 1
        self.assertTrue(abs(diffData).max() <= epsilon, 'pixel value failure')
        gotCsys = got.coordsys()
        expectedCsys = expected.coordsys()
        diffPixels = gotCsys.referencepixel()['numeric'] - expectedCsys.referencepixel()['numeric']
        self.assertTrue(abs(diffPixels).max() == 0)
        denom = expectedCsys.referencevalue()['numeric']
        for i in range(len(denom)):
            if (denom[i] == 0):
                denom[i] = 1
        fracDiffRef = (
            gotCsys.referencevalue()['numeric'] - expectedCsys.referencevalue()['numeric']
        )/denom;
        self.assertTrue(abs(fracDiffRef).max() <= 1.5e-6, 'refval failure')
        gotnpts = got.statistics()['npts']
        expnpts = expected.statistics()['npts']
        self.assertTrue(gotnpts == expnpts, 'npts failure')
        got.done()
        expected.done()

    def test_exceptions(self):
        """ia.pbcor: Test various exception cases"""
        def testit(
            imagename, pbimage, outfile, overwrite, region,
            box, chans, stokes, mask, mode, cutoff, wantreturn
        ):
            self.assertRaises(
                Exception, run_pbcor, imagename=imagename,
                pbimage=pbimage, outfile=outfile, overwrite=overwrite,
                region=region, box=box, chans=chans,
                stokes=stokes, mask=mask, mode=mode,
                cutoff=cutoff
            )

        # no image name given
        testit(
            imagename="", pbimage=self.pb1, outfile="",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )
        # bad image name given
        testit(
            imagename="totally_bogus", pbimage=self.pb1, outfile="jj.im",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )
        # no pbimage name given
        testit(
            imagename=self.im1, pbimage="", outfile="mm.im",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )
        # bad pbimage name given
        testit(
            imagename=self.im1, pbimage="totally_bogus2", outfile="pp.im",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )
        # unwritable self.outfile
        testit(
            imagename=self.im1, pbimage=self.pb1, outfile="/bogusplace/bogusimage",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )
        # bogus region
        testit(
            imagename=self.im1, pbimage=self.pb1, outfile="qq.im",
            overwrite=False, region="bogus_region", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )
        # bad mode
        testit(
            imagename=self.im1, pbimage=self.pb1, outfile="rr.im",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="zz", cutoff=-1.0,
            wantreturn=True
        )
        # incompatible image and pb
        testit(
            imagename=self.im1, pbimage=self.pb2, outfile="ss.im",
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d", cutoff=-1.0,
            wantreturn=True
        )

    def _testit(
        self, expected, imagename, pbimage, overwrite, region, box,
        chans, stokes, mask, mode, cutoff, epsilon=0.0
    ):
        myia = image()
        myia.open(pbimage)
        pbpix = myia.getchunk()
        myia.done()
        del myia
        for j in [0, 1]:
            self.outfile = "mypb.im"
            if j == 1:
                pbimage = pbpix
            self.outfile = self.outfile
            res = run_pbcor(
                imagename=imagename, pbimage=pbimage,
                outfile=self.outfile, overwrite=overwrite,
                region=region, box=box, chans=chans,
                stokes=stokes, mask=mask, mode=mode,
                cutoff=cutoff
            )
            self.assertTrue(res)
            res.done()
            self.checkImage(self.outfile, expected, epsilon)
            shutil.rmtree(self.outfile)

    def test_full_image_divide(self):
        """ia.pbcor: Test full image divide"""
        self._testit(
            expected=self.co1_1, imagename=self.im1, pbimage=self.pb1,
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d",
            cutoff=-1.0
        )

    def test_full_image_using_cutoff(self):
        """ia.pbcor: Test full image divide with cutoff"""
        self._testit(
            expected=self.co1_2, imagename=self.im1, pbimage=self.pb1,
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d",
            cutoff=0.001
        )

    def test_4d_image_with_2d_pb(self):
        """ia.pbcor: Test full image divide with cutoff. Primary beam is 2 D, image is 4 D"""
        self._testit(
            expected=self.co2, imagename=self.im2, pbimage=self.pb2,
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="d",
            cutoff=0.001
        )

    def test_multiply(self):
        """ia.pbcor: Test full image multiply with cutoff. Primary beam is 2 D, image is 4 D"""
        myia = image()
        myia.open(self.pb2)
        pixels = myia.getchunk()
        csys = myia.coordsys().torecord()
        myia.done()
        pixels = 1/pixels
        self.newpb = 'mult_pb.im'
        myia.fromarray(self.newpb, pixels, csys=csys)
        myia.done()
        self._testit(
            expected=self.co2, imagename=self.im2, pbimage=self.newpb,
            overwrite=False, region="", box="",
            chans="", stokes="", mask="", mode="m",
            cutoff=1000, epsilon=2e-7
        )

    def test_stretch(self):
        """ ia.pbcor(): Test stretch parameter"""
        yy = image()
        self.mymask = "maskim"
        yy.fromshape("", [113, 76, 1, 1])
        yy.addnoise()
        xx = yy.transpose(self.mymask, "0132")
        yy.done()
        xx.done()
        yy.open(self.im2)
        self.assertRaises(
            RuntimeError, yy.pbcor, pbimage=self.pb2,
            mask=self.mymask + ">0", stretch=False, outfile="garbage"
        )
        yy.open(self.im2)
        self.outfile = "blahblah"
        zz = yy.pbcor(
            pbimage=self.pb2, outfile=self.outfile, mask=self.mymask + ">0", stretch=True
        )
        yy.done()
        self.assertTrue(zz)
        zz.done()

    def test_diff_spectral_coordinate(self):
        """Verify fix that a different spectral coordinates in target and template don't matter, CAS-5096"""
        imagename = os.path.join(datapath, "CAS_5096target.im")
        template = self.pb4
        self.outfile = "mypb.im"
        myia = image()
        myia.open(imagename)
        yy = myia.pbcor(
            pbimage=template,
            outfile=self.outfile, mask='"' + template + '">0.21'
        )
        myia.done()
        yy.done()
        self.assertTrue(os.path.exists(self.outfile))

    def test_history(self):
        """Test history records are written"""
        myia = image()
        self.imagename = "zz.im"
        myia.fromshape(self.imagename, [20,20])
        gg = myia.getchunk()
        gg[:] = 1
        myia.done()
        self.outfile = "pb_out.im"
        myia.open(self.imagename)
        yy = myia.pbcor(pbimage=gg, outfile=self.outfile)
        myia.done()
        msgs = yy.history()
        yy.done()
        teststr = "ia.pbcor"
        self.assertTrue(teststr in msgs[-2], "'" + teststr + "' not found")
        self.assertTrue(teststr in msgs[-1], "'" + teststr + "' not found")

    def test_empty_region_and_stokes(self):
        """Test specifying stokes with empty region works (CAS-11708)"""
        myia = image()
        self.imagename = "t_in.im"
        self.pbimage = "t_pb_in.im"
        shape = [20, 20, 4, 20]
        for im in [self.imagename, self.pbimage]:
            myia.fromshape(im, shape)
            myia.addnoise()
            myia.done()
        self.outfile = "t_out.im"
        myia.open(self.imagename)
        yy = myia.pbcor(self.pbimage, outfile=self.outfile, region="", stokes="I")
        myia.done()
        self.assertTrue((yy.shape() == [20,20,1,20]).all(), "Incorrect shape")
        yy.done()

if __name__ == '__main__':
    unittest.main()
