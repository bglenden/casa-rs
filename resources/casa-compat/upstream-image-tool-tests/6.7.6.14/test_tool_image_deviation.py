##########################################################################
# test_tool_image_deviation.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.deviation
#
##########################################################################
import os
import sys
import shutil
import unittest
import math
import numpy
import numbers

from casatools import image as iatool
from casatools import regionmanager as rgtool
from casatools import table, ctsys
ctsys_resolve = ctsys.resolve

_rg = rgtool( )

datapath = ctsys_resolve('unittest/ia_deviation/')

input0 = datapath + "100x100x2.im"
ref0 = datapath + "ref0.im"
ref1 = datapath + "ref1.im"
ref2 = datapath + "ref2.im"
ref3 = datapath + "ref3.im"
ref4 = datapath + "ref4.im"
ref5 = datapath + "ref5.im"
ref6 = datapath + "ref6.im"
ref7 = datapath + "ref7.im"

class ia_deviation_test(unittest.TestCase):

    def _compare(self, resold, resnew, helpstr):
        mytype = type(resold)
        self.assertTrue(mytype == type(resnew), helpstr + ": types differ")
        if mytype == dict:
            for k in resold.keys():
                self._compare(resold[k], resnew[k], helpstr)
        elif mytype == numpy.ndarray:
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
        elif isinstance(resold, numbers.Integral) or mytype == numpy.int32:
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
        self._myia = iatool()
        self.imagename = ''

    def tearDown(self):
        self._myia.done()
        if self.imagename:
            if os.path.isfile(self.imagename):
                os.unlink(self.imagename)
            else:
                shutil.rmtree(self.imagename)
        tb = table( )
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done( )

    def test001(self):
        """Every pixel is a grid point"""
        self._myia.open(input0)
        zz = self._myia.deviation(
            "", grid=[1,1], xlength="4pix", ylength="4pix", stattype="npts",
            interp="lin",anchor=[0,0], statalg="cl"
        )
        self._myia.open(ref0)
        self._compare(self._myia.getchunk(), zz.getchunk(), "test001 compare")
        zz = self._myia.deviation(
            "", grid=[1,1], xlength="4pix", ylength="4pix", stattype="sigma",
            interp="lin",anchor=[0,0], statalg="cl"
        )
        self._myia.open(ref1)
        self._myia.done()
        zz.done()

    def test002(self):
        """Every pixel is a grid point with an offset, so should be the same result
        as test001"""
        anchor = [1,1]
        self._myia.open(input0)
        zz = self._myia.deviation(
            "", grid=[1,1], xlength="4pix", ylength="4pix", stattype="npts",
            interp="cub",anchor=anchor, statalg="cl"
        )
        self._myia.open(ref0)
        self._compare(self._myia.getchunk(), zz.getchunk(), "test001 compare")
        zz = self._myia.deviation(
            "", grid=[1,1], xlength="4pix", ylength="4pix", stattype="sigma",
            interp="cub",anchor=anchor, statalg="cl"
        )
        self._myia.open(ref1)
        self._myia.done()
        zz.done()

    def test003(self):
        """Every third pixel is a grid point"""
        grid = [3,3]
        for anchor in [[0,0], [12, 60]]:
            self._myia.open(input0)
            zz = self._myia.deviation(
                "", grid=grid, xlength="4pix", ylength="4pix", stattype="npts",
                interp="lin",anchor=anchor, statalg="cl"
            )
            self._myia.open(ref2)
            self._compare(self._myia.getchunk(), zz.getchunk(), "test001 compare")
            zz = self._myia.deviation(
                "", grid=grid, xlength="4pix", ylength="4pix", stattype="sigma",
                interp="lin",anchor=anchor, statalg="cl"
            )
            self._myia.open(ref3)
        self._myia.done()
        zz.done()

    def test005(self):
        """Every third pixel is a grid point, using anchor [2,2]"""
        anchor = [2,2]
        grid = [3,3]
        self._myia.open(input0)
        for anchor in [[2,2], [17,11]]:
            zz = self._myia.deviation(
                "", grid=grid, xlength="4pix", ylength="4pix", stattype="npts",
                interp="lin", anchor=anchor, statalg="cl"
            )
            self._myia.open(ref4)
            self._compare(self._myia.getchunk(), zz.getchunk(), "test005 compare")
            zz = self._myia.deviation(
                "", grid=grid, xlength="4pix", ylength="4pix", stattype="sigma",
                interp="lin", anchor=anchor, statalg="cl"
            )
            self._myia.open(ref5)
        self._myia.done()
        zz.done()

    def test006(self):
        """Test that regions work as expected"""
        self._myia.open(input0)
        myrg = rgtool()
        reg = myrg.box([5,5,0],[85,85,1])
        anchor = [1,1]
        grid = [13,13]
        length = "10.001pix"
        zz = self._myia.deviation(
            grid=grid, xlength=length, ylength=length, stattype="sigma",
            interp="lin", anchor=anchor, statalg="cl"
        )
        yy = self._myia.deviation(
            grid=grid, xlength=length, ylength=length, stattype="sigma",
            interp="lin", anchor=anchor, statalg="cl", region=reg
        )
        self._myia.done()
        sub0 = zz.subimage(region=myrg.box([27,27,0],[66,66,0]))
        zz.done()
        sub1 = yy.subimage(region=myrg.box([22,22,0],[61,61,0]))
        yy.done()
        self._compare(sub0.getchunk(), sub1.getchunk(), "test006 compare")
        sub0.done()
        sub1.done()

    def test_refpix(self):
        """Test using reference pixel"""
        self._myia.fromshape("", [20,20])
        self._myia.addnoise()
        xlen = "4pix"
        ylen = "4pix"
        grid = [4, 4]
        res = self._myia.deviation("", grid=grid, xlength=xlen, ylength=ylen, anchor=[10,10])
        expec = res.getchunk()
        res.done()
        res = self._myia.deviation("", grid=grid, xlength=xlen, ylength=ylen, anchor="ref")
        got = res.getchunk()
        res.done()
        self._myia.done()
        self.assertTrue(numpy.all(numpy.isclose(got, expec)), "ref val as anchor compare")

    def test_mask(self):
        aa = numpy.array(range(100), dtype=numpy.double)
        aa = aa.reshape([10,10])
        self._myia.fromshape("", [10, 10])
        self._myia.putchunk(aa*aa)
        mask = self._myia.getchunk(getmask=True)
        mask[2,2] = False
        mask[6,6] = False
        self.assertFalse(mask.all())
        self._myia.putregion(pixelmask=mask)
        self.assertTrue((self._myia.getchunk(getmask=True) == mask).all())
        mm = self._myia.deviation("", grid=[3,3], anchor=[2,2], xlength="4pix", ylength="4pix", interp="linear")
        self._myia.done()
        expec = mask[:]
        expec[0:5, 0:5] = False
        self.assertTrue((mm.getchunk(getmask=True) == expec).all())
        mm.done()

        self._myia.fromshape("", [10, 10, 2])
        bb = self._myia.getchunk()
        bb[:,:,0] = aa
        bb[:,:,1] = aa
        self._myia.putchunk(bb)
        mask2 = self._myia.getchunk(getmask=True)
        mask2[:,:,0] = mask
        mask2[2,5,1] = False
        mask2[6,6,1] = False
        self.assertFalse(mask2.all())
        self._myia.putregion(pixelmask=mask2)
        self.assertTrue((self._myia.getchunk(getmask=True) == mask2).all())
        mm = self._myia.deviation(
            "", grid=[3,3], anchor=[2,2], xlength="4pix",
            ylength="4pix", interp="linear"
        )
        self._myia.done()
        expec = mask2[:]
        expec[0:5, 0:5, 0] = False
        expec[0:5, :, 1] = False
        expec[2, 2, 1] = True
        expec[2, 8, 1] = True
        self.assertTrue((mm.getchunk(getmask=True) == expec).all())
        mm.done()

    def test_circle(self):
        """test circles work correctly CAS-10296"""
        myia = self._myia
        self.imagename = "mycirc.im"
        myia.fromshape(self.imagename, [100, 100])
        bb = myia.getchunk()
        bb[:] = 1
        myia.putchunk(bb)
        zz = myia.deviation(
            "", xlength="40pix", ylength="", stattype="sum", grid=[20,20]
        )
        myia.done()
        self.assertTrue(
            numpy.isclose(zz.getchunk()[50,50], 1257.0, 1e-7),
            "incorrect grid pixel value"
        )
        zz.done()

if __name__ == '__main__':
    unittest.main()
