##########################################################################
# test_tool_image_subimage.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.subimage
#
##########################################################################
import shutil
import unittest
import numpy
import os

from casatools import image
from casatools import quanta
from casatools import regionmanager
from casatools import table

_tb = table()

#datapath = 'regression/unittest/imsubimage/'

class ia_subimage_test(unittest.TestCase):

    def setUp(self):
        self.myia = image()
        self.mask1 = ''
        self.mask2 = ''
        self.mask3 = ''
        self.imname = ''
        self.imagename = ''

    def tearDown(self):
        self.myia.done()
        # FIXME need to figure out why this table is left open when test_stretch throws
        # reasonable exception (CAS-4890)
        data = ["mask1.im", "mask2.im", "mask3.im", self.imagename, self.imname]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_stretch(self):
        """Test the stretch parameter"""
        myia = self.myia
        myia.fromshape("mask1.im", [20, 30, 4, 10])
        myia.fromshape("mask2.im", [20, 30, 4, 1])
        myia.fromshape("mask3.im", [20, 30, 4, 2])
        myia.done()

        self.imname = "xx.im"
        myia.fromshape(self.imname, [20,30,4,10])
        self.mask1 = "mask1.im > 10"
        mm = myia.subimage("", mask=self.mask1)
        self.assertTrue(mm)
        mm.done()
        myia.done()
        self.assertTrue(len(_tb.showcache()) == 0)
        self.mask2 = "mask2.im > 10"
        self.assertRaises(Exception, myia.subimage, "", mask=self.mask2, stretch=False)
        myia.open(self.imname)
        mm = myia.subimage("", mask=self.mask2, stretch=True)
        myia.done()
        mm.done()
        self.assertTrue(len(_tb.showcache()) == 0)
        self.mask3 = "mask3.im > 10"
        zz = None
        myia.open(self.imname)
        self.assertRaises(Exception, myia.subimage, "", mask=self.mask3, stretch=True)
        myia.done()

    def test_beams(self):
        """ Test per plane beams """
        myia = self.myia
        # simple copy
        myia.fromshape("", [10, 10, 10, 4])
        myia.setrestoringbeam(
            "4arcsec", "2arcsec", "5deg", channel=0, polarization=0
        )
        qa = quanta()
        for i in range(10):
            for j in range(4):
                myia.setrestoringbeam(
                    qa.quantity(i + j + 2, "arcsec"),
                    qa.quantity(i + j + 1, "arcsec"),
                    qa.quantity("5deg"),
                    channel=i, polarization=j
                )
        rg = regionmanager()
        box = rg.box([2, 2, 2, 2], [5, 5, 5, 3])
        subim = myia.subimage("", region=box)
        for i in range(subim.shape()[2]):
            for j in range(subim.shape()[3]):
                self.assertTrue(
                    subim.restoringbeam(channel=i, polarization=j)
                    == myia.restoringbeam(channel=i+2, polarization=j+2)
                )
        box = rg.box([2, 2, 2, 2], [5, 5, 5, 2])
        subim = myia.subimage("", region=box, dropdeg=True)
        for i in range(subim.shape()[2]):
            self.assertTrue(
                subim.restoringbeam(channel=i, polarization=-1)
                == myia.restoringbeam(channel=i+2, polarization=2)
            )
        box = rg.box([2, 2, 6, 1], [5, 5, 6, 3])
        subim = myia.subimage("", region=box, dropdeg=True)
        for i in range(subim.shape()[2]):
            self.assertTrue(
                subim.restoringbeam(channel=-1, polarization=i)
                == myia.restoringbeam(channel=6, polarization=i+1)
            )
        subim.done()
        myia.done()

    def test_precision(self):
        """Test various precision valued image support"""
        myia = self.myia
        j = 1.2345678901234567890123456789
        k = j*(1+1j)
        for mytype in ['f', 'c', 'd', 'cd']:
            myia.fromshape("",[2,2], type=mytype)
            zz = myia.getchunk()
            expectype = type(zz[0, 0])
            if mytype == 'f' or mytype =='d':
                zz[:] = j
            else:
                zz[:] = k
            myia.putchunk(zz)
            subim = myia.subimage()
            myia.done()
            yy = subim.getchunk()
            subim.done()
            self.assertTrue(type(yy[0,0]) == expectype)
            if mytype == 'f' or mytype == 'c':
                self.assertTrue(numpy.isclose(yy, zz, 1e-8, 1e-8).all())
            else:
                self.assertTrue((yy == zz).all())

    def test_keepaxes(self):
        """Test the keepaxes parameter"""
        myia = self.myia
        myia.fromshape("", [10, 20, 30])
        zz = myia.subimage("", dropdeg=False)
        self.assertTrue((zz.shape() == [10, 20, 30]).all())
        zz = myia.subimage("", dropdeg=True)
        self.assertTrue((zz.shape() == [10, 20, 30]).all())
        zz = myia.subimage("", dropdeg=False, keepaxes=[0])
        self.assertTrue((zz.shape() == [10, 20, 30]).all())
        zz = myia.subimage("", dropdeg=True, keepaxes=[0])
        self.assertTrue((zz.shape() == [10, 20, 30]).all())

        self.imagename = "keep.im"
        myia.fromshape(self.imagename, [10, 20, 1, 1])
        zz = myia.subimage("", dropdeg=False)
        self.assertTrue((zz.shape() == [10, 20, 1, 1]).all())
        zz = myia.subimage("", dropdeg=True)
        self.assertTrue((zz.shape() == [10, 20]).all())
        zz = myia.subimage("", dropdeg=False, keepaxes=[0])
        self.assertTrue((zz.shape() == [10, 20, 1, 1]).all())
        zz = myia.subimage("", dropdeg=True, keepaxes=[0])
        self.assertTrue((zz.shape() == [10, 20]).all())
        zz = myia.subimage("", dropdeg=False, keepaxes=[0])
        self.assertTrue((zz.shape() == [10, 20, 1, 1]).all())
        zz = myia.subimage("", dropdeg=True, keepaxes=[3])
        self.assertTrue((zz.shape() == [10, 20, 1]).all())
        zz.done()
        myia.done()

    def test_history(self):
        """verify history writing"""
        myia = self.myia
        self.imagename = "zz.im"
        myia.fromshape(self.imagename, [20, 20])
        myia = myia.subimage()
        msgs = myia.history()
        myia.done()
        teststr = "ia.subimage"
        self.assertTrue(teststr in msgs[-2], "'" + teststr + "' not found")
        self.assertTrue(teststr in msgs[-1], "'" + teststr + "' not found")
        # verify no history written if dohistory set to False
        ia2 = image()
        ia2.dohistory(False)
        self.imname = "gg"
        ia2.fromshape(self.imname,[20, 20])
        msgs = ia2.history()
        ia2 = ia2.subimage()
        ia2.done()
        for m in msgs:
            self.assertFalse(teststr in msgs, "History unexpectedly written")

if __name__ == '__main__':
    unittest.main()
