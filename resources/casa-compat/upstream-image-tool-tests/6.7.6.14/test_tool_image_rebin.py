##########################################################################
# test_tool_image_rebin.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.rebin
#
##########################################################################
import shutil
import numpy
import unittest
import os

from casatools import image
from casatools import regionmanager
from casatools import table

_rg = regionmanager()
_tb = table()

def alleqnum(x,num,tolerance=0):
    if len(x.shape)==1:
        for i in range(x.shape[0]):
            if not (abs(x[i]-num) < tolerance):
                print ("x[",i,"]=", x[i])
                return False
    if len(x.shape)==2:
        for i in range(x.shape[0]):
            for j in range(x.shape[1]):
                if not (abs(x[i][j]-num) < tolerance):
                    print("x[",i,"][",j,"]=", x[i][j])
                    return False
    if len(x.shape)==3:
        for i in range(x.shape[0]):
            for j in range(x.shape[1]):
                for k in range(x.shape[2]):
                    if not (abs(x[i][j][k]-num) < tolerance):
                        print("x[",i,"][",j,"][",k,"]=", x[i][j][k])
                        return False
    if len(x.shape)==4:
        for i in range(x.shape[0]):
            for j in range(x.shape[1]):
                for k in range(x.shape[2]):
                    for l in range(x.shape[3]):
                        if not (abs(x[i][j][k][l]-num) < tolerance):
                            print("x[",i,"][",j,"][",k,"][",l,"]=", x[i][j][k])
                            return False
    if len(x.shape)>4:
        stop('unhandled array shape in alleq')
    return True

class ia_rebin_test(unittest.TestCase):

    def setUp(self):
        self._myia = image()
        self.imagename = ''
        self.mymask = ''

    def tearDown(self):
        self._myia.done()
        data = [
            self.imagename, self.mymask]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        self.assertTrue(len(_tb.showcache()) == 0)


    def test_stretch(self):
        """ ia.rebin(): Test stretch parameter"""
        yy = image()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200,200,1,10]
        self.imagename = "aa.im"
        yy.fromshape(self.imagename, shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.rebin, outfile="", bin=[2,2,1,1],
            mask=self.mymask + ">0", stretch=False
        )

        zz = yy.rebin(
            outfile="", bin=[2,2,1,1],
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_general(self):
        """ ia.rebin(): General tests"""
        # tests moved from imagetest_regression.py and modified

        myia = self._myia
        shp2 = [20,40]
        d2 = myia.makearray(1.0, [shp2[0], shp2[1]])
        self.imagename = "st.im"
        myim2 = myia.newimagefromarray(outfile=self.imagename, pixels=d2)
        self.assertTrue(myim2)

        outfile = "gk.im"
        self.assertRaises(Exception, myim2.rebin, "", bin=[-100,2], overwrite=True)

        myim2b = myim2.rebin("", bin=[2,2])
        self.assertTrue(myim2b)
        p = myim2b.getchunk()
        self.assertTrue(alleqnum(p,1.0,tolerance=0.0001))

        self.assertTrue(myim2.done() and myim2b.done())

    def test_multibeam(self):
        """Test multiple beams"""
        myia = self._myia
        self.imagename = "gd.im"
        myia.fromshape(self.imagename, [10, 10, 10])
        myia.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="0deg",
            channel=0, polarization=0
        )
        rebin = myia.rebin("", [2, 2, 1])
        self.assertTrue(rebin)
        rebin.done()
        self.assertRaises(
            Exception, myia.rebin, "", [2,2,2]
        )
        myia.done()

    def test_crop(self):
        """Test crop parameter"""
        myia = self._myia
        self.imagename = "xxyy.im"
        myia.fromshape(self.imagename, [20, 20, 20])
        factor = [3,3,3]
        zz = myia.rebin("", bin=factor, crop=True)
        self.assertTrue((zz.shape() == [6,6,6]).all())
        zz = myia.rebin("", bin=factor, crop=False)
        self.assertTrue((zz.shape() == [7,7,7]).all())
        myia.done()
        zz.done()

    def test_dropdeg(self):
        """Test dropdeg parameter"""
        myia = self._myia
        self.imagename = "kjfasd.im"
        myia.fromshape(self.imagename, [20, 20, 1])
        factor = [5,5]
        zz = myia.rebin("", bin=factor, dropdeg=True)
        myia.done()
        self.assertTrue((zz.shape() == [4,4]).all())
        zz.done()

    def test_box(self):
        """Test use of box"""
        myia = self._myia
        self.imagename = "erzvd.im"
        myia.fromshape(self.imagename, [30, 30, 1])
        factor = [5,5]
        zz = myia.rebin("", bin=factor, region=_rg.box([5,5,0],[25,25,0]),crop=True)
        myia.done()
        self.assertTrue((zz.shape() == [4,4,1]).all())
        zz.done()

    def test_dropdeg2(self):
        """ axes that become degenerate when regridded are dropped if dropdeg=True: CAS-5836"""
        myia = self._myia
        self.imagename = "kbesd.im"
        myia.fromshape(self.imagename, [20, 20, 20])
        factor = [1, 1, 20]
        zz = myia.rebin("", bin=factor, dropdeg=True)
        myia.done()
        self.assertTrue((zz.shape() == [20,20]).all())
        zz.done()

    def test_history(self):
        """Test history writing"""
        myia = self._myia
        self.imagename = "zz.im"
        factor = [1, 1, 20]
        myia.fromshape(self.imagename,[20,20,20])
        bb = myia.rebin("", bin=factor)
        myia.done()
        msgs = bb.history()
        bb.done()
        teststr = "ia.rebin"
        self.assertTrue(teststr in msgs[-2], "'" + teststr + "' not found")
        self.assertTrue(teststr in msgs[-1], "'" + teststr + "' not found")

if __name__ == '__main__':
    unittest.main()
