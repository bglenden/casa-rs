########################################################################
# test_tool_image_transpose.py
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
# CAS-12700
#
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.transpose
#
##########################################################################

import os
import shutil
import unittest

from casatools import ctsys, image, table
_tb = table( )
ctsys_resolve = ctsys.resolve


datapath = 'unittest/ia_transpose/'


def run_transpose(imagename, outfile, order):
    myia = image()
    myia.open(imagename)
    res = myia.transpose(outfile=outfile, order=order)
    myia.done()
    return res

def run_imtrans(imagename, outfile, order):
    return imtrans(imagename=imagename, outfile=outfile, order=order)

class ia_transpose_test(unittest.TestCase):

    def setUp(self):
        self.out1 = ''
        self.good_image = "reorder_in.fits"
        self.cas_2364im = "CAS-2364.im"
        shutil.copy(ctsys_resolve(os.path.join(datapath, self.good_image)), self.good_image)

    def tearDown(self):
        data = [self.out1, self.cas_2364im, self.good_image] + ["straight_copy_{}".format(x) for x in range(4)]+ ["transpose_{}".format(x) for x in range(4)]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        self.assertTrue(len(_tb.showcache()) == 0)


    def test_exceptions(self):
        """imtrans: Test various exception cases"""
        #def testit(imagename, outfile, order):
        #    self.assertRaises(Exception, run_transpose, imagename, outfile, order)

        # blank imagename
        self.assertRaises(Exception, run_transpose, "", "blah", "012")

        # not enough specified axes
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", "01")
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", 10)
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", ["r", "d"])

        # too many specified axes
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", "0123")
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", 1230)
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", ["r", "d", "f", "s"])

        # Bogus axes specification
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", "123")
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", ["r", "d", "s"])
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", ["r", "d", "r"])
        self.assertRaises(Exception, run_transpose, self.good_image, "blah", 103)

    def test_straight_copy(self):
        """No actual transposing"""
        imagename = self.good_image
        myia = image()
        myia.open(imagename)
        expecteddata = myia.getchunk()
        expectednames = myia.coordsys().names()
        myia.close()
        count = 0
        for order in ["012", 12, ['r', 'd', 'f'], ["righ", "declin", "freq"]]:
            outfile = "straight_copy_" + str(count)
            newim = run_transpose(imagename, outfile, order)
            gotdata = newim.getchunk()
            gotnames = newim.coordsys().names()
            newim.done()
            self.assertTrue((expecteddata == gotdata).all())
            self.assertTrue(expectednames == gotnames)
            count += 1

    def test_transpose(self):
        """Test transposing"""
        imagename = self.good_image
        myia = image()
        myia.open(imagename)
        expecteddata = myia.getchunk()
        expectednames = myia.coordsys().names()
        myia.done()
        count = 0
        for order in ["120", 120, ['d', 'f', 'r'], ["declin", "freq", "righ"]]:
            for outname in ["transpose_" + str(count), ""]:
                newim = run_transpose(imagename, outname, order)
                gotdata = newim.getchunk()
                inshape = expecteddata.shape
                for i in range(inshape[0]):
                    for j in range(inshape[1]):
                        for k in range(inshape[2]):
                            self.assertTrue(expecteddata[i][j][k] == gotdata[j][k][i])
                gotnames = newim.coordsys().names()
                newim.done()
                self.assertTrue(expectednames[0] == gotnames[2])
                self.assertTrue(expectednames[1] == gotnames[0])
                self.assertTrue(expectednames[2] == gotnames[1])
            count += 1

    def test_cas_2364(self):
        "test CAS-2364 fix"
        shutil.copytree(ctsys_resolve(os.path.join(datapath, self.cas_2364im)), self.cas_2364im)
        order = "0132"
        self.out1 = "blahxx.im"
        myia = image()
        myia.open(self.cas_2364im)
        trans = myia.transpose(self.out1, order)
        myia.done()
        trans.done()
        self.assertTrue(len(_tb.showcache()) == 0)
        # to verify fix, just open the image. bug was that exception was thrown when opening output from reorder
        myia.open(self.out1)
        self.assertTrue(myia)
        myia.done()

    def test_history(self):
        """Test history records are written"""
        myia = image()
        myia.fromshape("", [10,10,4,10])
        order = "3210"
        kk = myia.transpose("",order=order)
        myia.done()
        msgs = kk.history()
        kk.done()
        teststr = "ia.transpose"
        self.assertTrue(teststr in msgs[-2], "'" + teststr + "' not found")
        self.assertTrue(teststr in msgs[-1], "'" + teststr + "' not found")

    def test_imageinfo(self):
        """Verify image info is copied"""
        myia = image()
        myia.fromshape("",[10,20,30])
        myia.setbrightnessunit("Jy/beam")
        myia.setrestoringbeam("4arcmin", "3arcmin", "0deg")
        kk = myia.transpose("", "201")
        self.assertEqual(
            kk.brightnessunit(), "Jy/beam",
            "brightness unit not copied"
        )
        self.assertTrue(
            kk.restoringbeam(), "restoring beam not copied"
        )
        kk.done()

if __name__ == '__main__':
    unittest.main()
