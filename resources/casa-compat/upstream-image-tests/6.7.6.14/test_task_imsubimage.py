#########################################################################
# test_task_imsubimage.py
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
#
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.analysis.imsubimage.html
#
##########################################################################
import os
import shutil
import unittest
import numpy

from casatasks import imsubimage
from casatools import image, ctsys
from casatools import quanta
from casatools import regionmanager
from casatools import table

_tb = table()

datapath = ctsys.resolve('unittest/imsubimage/')

class imsubimage_test(unittest.TestCase):

    def setUp(self):
        self.myia = image()

    def tearDown(self):
        self.myia.done()
        # FIXME need to figure out why this table is left open when test_stretch throws
        # reasonable exception (CAS-4890)
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_stretch(self):
        """Test the stretch parameter"""
        myia = self.myia
        myia.fromshape("mask1.im", [20, 30, 4, 10])
        myia.fromshape("mask2.im", [20, 30, 4, 1])
        myia.fromshape("mask3.im", [20, 30, 4, 2])
        myia.done()
        imname = "xx.im"
        myia.fromshape(imname, [20,30,4,10])
        mask1 = "mask1.im > 10"
        outfile = "stretch1"
        imsubimage(imagename=imname, outfile=outfile, mask=mask1)
        self.assertTrue(os.path.exists(outfile))
        myia.done()
        self.assertTrue(len(_tb.showcache()) == 0)
        mask2 = "mask2.im > 10"
        self.assertRaises(
            Exception, imsubimage, imname, "stretch4", mask=mask2, stretch=False
        )
        self.assertTrue(len(_tb.showcache()) == 0)
        outfile = "stretch2"
        imsubimage(imname, outfile=outfile, mask=mask2, stretch=True)
        self.assertTrue(os.path.exists(outfile))
        mask3 = "mask3.im > 10"
        self.assertRaises(
            Exception, imsubimage, imname, "junk", mask=mask3, stretch=True
        )

    def test_beams(self):
        """ Test per plane beams """
        # CAS-5282
        imagename = datapath + "50beams.im"
        outfile = "test_beams1.im"
        imsubimage(
            imagename=imagename, outfile=outfile, box="",
            region="", chans="18~29",stokes="I",mask="",
            dropdeg=False, verbose=True, stretch=False
        )
        myia = self.myia
        myia.open(outfile)
        beams = myia.restoringbeam()
        self.assertTrue(len(beams['beams']) == 12)

    def test_CAS7704(self):
        """Test CAS-7704, chans can be specified with region file"""
        myia = self.myia
        imagename = "CAS-7704.im"
        myia.fromshape(imagename,[20,20,20, 4])
        outfile = 'myout.im'
        region = "box[[1pix,1pix],[19pix,19pix]])"
        imsubimage(
            imagename=imagename, outfile=outfile, overwrite=True, region=region,
            chans=""
        )
        myia.open(outfile)
        self.assertTrue((myia.shape() == numpy.array([19, 19, 20, 4])).all())
        myia.done()
        self.assertRaises(
            Exception, imsubimage, imagename=imagename, outfile=outfile,
            overwrite=True, region=region, chans="5~6,9~10"
        )
        imsubimage(
            imagename=imagename, outfile=outfile, overwrite=True, region=region,
            chans="5~10"
        )
        myia.open(outfile)
        self.assertTrue((myia.shape() == numpy.array([19, 19, 6, 4])).all())
        myia.done()
        imsubimage(
            imagename=imagename, outfile=outfile, overwrite=True, region=region,
            stokes="IU"
        )
        myia.open(outfile)
        # includes Q although that plane should be fully masked
        self.assertTrue((myia.shape() == numpy.array([19, 19, 20, 3])).all())
        self.assertTrue(myia.getchunk(getmask=True)[:,:,:,0].all())
        self.assertTrue(myia.getchunk(getmask=True)[:,:,:,2].all())
        self.assertFalse(myia.getchunk(getmask=True)[:,:,:,1].any())
        myia.done()

        region = "box[[2pix,2pix],[6pix,6pix]])"
        box = "10,10,12,12"
        imsubimage(
            imagename=imagename, box=box, outfile=outfile, overwrite=True, region=region,
            chans=""
        )
        myia.open(outfile)
        self.assertTrue((myia.shape() == numpy.array([11, 11, 20, 4])).all())
        myia.done()

        imsubimage(
            imagename=imagename, box=box, outfile=outfile, overwrite=True, region=region,
            chans="5~10"
        )
        myia.open(outfile)
        self.assertTrue((myia.shape() == numpy.array([11, 11, 6, 4])).all())
        myia.done()

    def test_keepaxes(self):
        """Test the keepaxes parameter"""
        imagename = "keep.im"
        myia = self.myia
        myia.fromshape(imagename, [10, 20, 1, 1])
        myia.done()

        outfile = "keep_out.im"
        imsubimage(imagename, outfile=outfile, dropdeg=False, overwrite=True)
        zz = image()
        zz.open(outfile)
        self.assertTrue((zz.shape() == [10, 20, 1, 1]).all())
        zz.done()
        imsubimage(imagename, outfile=outfile, dropdeg=True, overwrite=True)
        zz.open(outfile)
        self.assertTrue((zz.shape() == [10, 20]).all())
        zz.done()
        imsubimage(imagename, outfile=outfile, dropdeg=False, keepaxes=[0], overwrite=True)
        zz.open(outfile)
        self.assertTrue((zz.shape() == [10, 20, 1, 1]).all())
        zz.done()
        imsubimage(imagename, outfile=outfile, dropdeg=True, keepaxes=[0], overwrite=True)
        zz.open(outfile)
        self.assertTrue((zz.shape() == [10, 20]).all())
        zz.done()
        imsubimage(imagename, outfile=outfile, dropdeg=False, keepaxes=[0], overwrite=True)
        zz.open(outfile)
        self.assertTrue((zz.shape() == [10, 20, 1, 1]).all())
        zz.done()
        imsubimage(imagename, outfile=outfile, dropdeg=True, keepaxes=[3], overwrite=True)
        zz.open(outfile)
        self.assertTrue((zz.shape() == [10, 20, 1]).all())
        zz.done()

    def test_history(self):
        """verify history writing"""
        myia = self.myia
        imagename = "zz.im"
        myia.fromshape(imagename, [20, 20])
        myia.done()

        outfile = "zz_out.im"
        imsubimage(imagename=imagename, outfile=outfile)
        myia.open(outfile)
        msgs = myia.history()
        myia.done()
        teststr = "version"
        self.assertTrue(teststr in msgs[-2], "'" + teststr + "' not found")
        teststr = "imsubimage"
        self.assertTrue(teststr in msgs[-1], "'" + teststr + "' not found")

if __name__ == '__main__':
    unittest.main()
