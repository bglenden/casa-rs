##########################################################################
# test_tool_image_regrid.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.image.html#casatools.image.image.regrid
#
##########################################################################
import os
import shutil
import numpy as np
import unittest

from casatools import ctsys, image, regionmanager, table, coordsys
_ia = image()
_rg = regionmanager()
_cs = coordsys()
_tb = table()
ctsys_resolve = ctsys.resolve

sep = os.sep
datapath = ctsys_resolve(os.path.join('unittest','ia_regrid'))

IMAGE = 'image.im'
gim = "gaussian_source.im"

total = 0
fail  = 0
current_test =""
stars = "*************"

def alleqnum(x,num,tolerance=0):
    if len(x.shape)==1:
        for i in range(x.shape[0]):
            if not (abs(x[i]-num) < tolerance):
                print("x[",i,"]=", x[i])
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

out1 = 'regridded'
out2 = 'bigger_image'
out3 = 'shifted_image'
out4 = 'back_to_image'
out5 = 'template'
out6 = 'gal_coords.im'
first = 'first'
fourth = 'fourth'
ngc5921 = "ngc5921.clean.image"
myout = 'gal_regrid_out.image'
myref = "gal_regrid.image"
imname = 'ia.fromshape.image1'
real1 = "real1.im"
imag1 = "imag1.im"
lin_template = 'lin_template.im'
lin_source = 'lin_source.im'
mymask = "maskim"
mou = 'moulou1'
sixth = 'sixth'
third = 'third'

class ia_regrid_test(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        if len(_tb.showcache()) != 0:
            raise Exception("Tables remain open in the table cache after execution")

    def setUp(self):
        pass

    def tearDown(self):
        _ia.done()

        for i in (
            IMAGE, out1, out2, out3, out4, out5, out6, first, fourth,
            ngc5921, myout, myref, imname, real1, imag1, lin_source,
            lin_template, mymask, mou, sixth, third
        ):
            if (os.path.exists(i)):
                shutil.rmtree(i)

    def test_asvelocity(self):
        """ Test regrid by velocity """
        image = "byvel.im"
        expected = "expected.im"
        shutil.copytree(ctsys_resolve(os.path.join(datapath, image)), image)
        shutil.copytree(ctsys_resolve(os.path.join(datapath, expected)), expected)
        myia = _ia
        myia.open(expected)
        csys = myia.coordsys().torecord()
        myia.done()
        myia.open(image)
        ff = myia.regrid("",csys=csys,asvelocity=True)
        myia.done()
        myia.open(expected)
        res = (ff.getchunk() == myia.getchunk()).all()
        self.assertTrue(res)
        res = (ff.getchunk(getmask=True) == myia.getchunk(getmask=True)).all()
        self.assertTrue(res)
        ff.done()
        outfile = "junk"
        outim = myia.regrid(outfile=outfile, csys=csys, asvelocity=True)
        outim.done()
        ff.open(outfile)
        res = (ff.getchunk() == myia.getchunk()).all()
        self.assertTrue(res)
        res = (ff.getchunk(getmask=True) == myia.getchunk(getmask=True)).all()
        ff.done()
        myia.done()
        self.assertTrue(res)
        shutil.rmtree(outfile)
        shutil.rmtree(image)
        shutil.rmtree(expected)
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_stretch(self):
        """ ia.regrid(): Test stretch parameter"""
        yy = _ia
        yy.fromshape(mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200,200,1,20]
        yy.fromshape("", shape)
        yy.addnoise()
        mycsys = yy.coordsys()
        mycsys.setreferencepixel([2.5], "spectral")
        for i in [0,1]:
            byvel = i == 0
            self.assertRaises(
                Exception,
                yy.regrid, outfile="", asvelocity=byvel,
                csys=mycsys.torecord(),
                mask=mymask + ">0", stretch=False
            )
            zz = yy.regrid(
                outfile="", asvelocity=byvel,
                csys=mycsys.torecord(),
                mask=mymask + ">0", stretch=True
            )
            self.assertTrue(type(zz) == type(yy))
            zz.done()

        yy.done()
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_general(self):
        """ ia.regrid general tests """
        # moved from iamgetest_regression

        # Make RA/DEC/Spectral image
        imshape = [32,32,32]
        myia = _ia
        myim = myia.newimagefromshape(imname, imshape)
        self.assertTrue(myim)
        self.assertTrue(myim.set(1.0))
        # Forced failures
        self.assertRaises(Exception, myim.regrid, axes=[20])
        self.assertRaises(Exception, myim.regrid, shape=[10,20,30,40])
        self.assertRaises(Exception, myim.regrid, csys='fish')
        self.assertRaises(Exception, myim.regrid, method='doggies')
        # Regrid it to itself (all axes        #
        iDone = 1
        #      for method in ["near","linear","cubic"]:
        for method in ["cubic"]:
            myim2 = myim.regrid(method=method)
            self.assertTrue(myim2)
            p = myim2.getchunk([3,3],[imshape[0]-3,imshape[1]-3,imshape[2]-3])
            self.assertTrue(alleqnum(p,1,tolerance=1e-3))
            self.assertTrue(myim2.done())
            iDone = iDone + 1

        #      for method in ["cubic","linear","near"]:
        for method in ["cubic"]:
            myim2 = myim.regrid(method=method, axes=[0,1])
            self.assertTrue(myim2)
            p = myim2.getchunk([3,3],[imshape[0]-3,imshape[1]-3,imshape[2]-3])
            self.assertTrue(alleqnum(p,1,tolerance=1e-3))
            self.assertTrue(myim2.done())
            iDone = iDone + 1

        #      for method in ["near","linear","cubic"]:
        for method in ["cubic"]:
            myim2 = myim.regrid(method=method, axes=[2])
            self.assertTrue(myim2)
            p = myim2.getchunk([3,3],[imshape[0]-3,imshape[1]-3,imshape[2]-3])
            self.assertTrue(alleqnum(p,1,tolerance=1e-3))
            self.assertTrue(myim2.done())
            iDone = iDone + 1
        #
        self.assertTrue(myim.done())
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_multibeam(self):
        """ia.regrid, test multibeam image"""
        myia = _ia
        myia.fromshape("", [10, 10, 10])
        csys = myia.coordsys()
        refpix = csys.increment()["numeric"][2]
        refpix = refpix * 0.9
        csys.setincrement(refpix, "spectral")

        myia.setrestoringbeam(major="4arcsec", minor="2arcsec", pa="0deg", channel=0, polarization=-1)
        regridded = myia.regrid(axes=[0, 1], csys=csys.torecord(), decimate=3)
        regridded.done()
        self.assertRaises(Exception, myia.regrid, axes=[0,1,2], csys=csys.torecord())
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_CAS_4315(self):
        """ test ia.regrid does not leave image open after tool is closed"""
        myia = _ia
        myia.fromshape("",[100,100,1,1])
        myib = myia.regrid(
            outfile=mou, csys=myia.coordsys().torecord(), axes=[0,1],
            overwrite=True, shape=[100, 100, 1, 1]
        )
        myia.done()
        myib.done()
        self.assertTrue(len(_tb.showcache()) == 0)

    def test_CAS_4262(self):
        """ Test degenerate axes are not relabeled to template"""
        # test degenerate spectral axis is not regridded nor relabeled in output
        myia = _ia
        myia.fromshape("", [10, 10, 1, 1])
        csys = myia.coordsys()
        refvals = csys.referencevalue()["numeric"]
        refvals[3] *= 10
        csys.setreferencevalue(refvals)
        regridded = myia.regrid("", myia.shape(), csys.torecord(), asvelocity=False)
        self.assertTrue(regridded.getchunk(getmask=True).all())
        self.assertTrue(
            (
             regridded.coordsys().referencevalue()["numeric"]
             == myia.coordsys().referencevalue()["numeric"]
            ).all()
        )

    def test_overlap(self):
        """Test for notification if no overlap between input and output images"""
        myia = _ia
        myia.fromshape("", [20, 20, 20, 4])
        csys = myia.coordsys()
        csys.setreferencevalue([1800, 0], 'direction')
        myia.setcoordsys(csys.torecord())

        ccopy = csys.copy()
        xx = myia.regrid(outfile=first,csys=ccopy.torecord())
        self.assertTrue(xx)
        xx.done()

        ccopy.setreferencevalue([1890, 0], 'direction')
        self.assertRaises(Exception, myia.regrid, "second",csys=ccopy.torecord())
        xx = myia.regrid(fourth, csys=ccopy.torecord(), axes=2)
        self.assertTrue(xx)
        xx.done()
        myia.fromshape("", [200, 200, 20, 4], csys=csys.torecord())
        xx = myia.regrid(outfile=third, csys=ccopy.torecord())
        self.assertTrue(xx)
        xx.done()
        ccopy.setreferencevalue(1.416e9, 'spectral')
        self.assertRaises(Exception, myia.regrid, "fifth",csys=ccopy.torecord())
        myia.fromshape("", [20, 20, 1001, 4], csys=csys.torecord())
        xx = myia.regrid(outfile=sixth, csys=ccopy.torecord(), axes=2)
        self.assertTrue(xx)
        xx.done()
        self.assertRaises(
            Exception,myia.regrid, outfile="seventh",csys=ccopy.torecord(),
            axes=2, region=_rg.box([0,0,0,0],[19,19,998,3])
        )

    def test_regrid_galactic(self):
        """Verify fix for CAS-5534"""
        shutil.copytree(ctsys_resolve(os.path.join(datapath, ngc5921)), ngc5921)
        shutil.copytree(ctsys_resolve(os.path.join(datapath, myref)), myref)
        myia = _ia
        myia.open(ngc5921)
        csys = myia.coordsys()
        csys.setreferencecode('GALACTIC', type='direction', adjust=True)
        zz = myia.regrid(
            outfile=myout, shape=[300, 300, 1, 46], csys=csys.torecord(), overwrite=True
        )
        myia.open(myref)
        self.assertTrue(np.max(np.abs(zz.getchunk() - myia.getchunk())) < 1e-8)
        self.assertTrue((zz.getchunk(getmask=True) == myia.getchunk(getmask=True)).all())
        myia.done()
        zz.done()

    def test_linear_overlap(self):
        """Test that overlapping linear coordinates works, CAS-5767"""
        shutil.copytree(ctsys_resolve(os.path.join(datapath, lin_template)), lin_template)
        shutil.copytree(ctsys_resolve(os.path.join(datapath, lin_source)), lin_source)
        myia = _ia
        myia.open(lin_template)
        csys = myia.coordsys().torecord()
        shape = myia.shape()
        myia.done()
        myia.open(lin_source)
        xx = myia.regrid(axes=[0,1], csys=csys, shape=shape)
        myia.done()
        self.assertTrue((xx.shape() == shape).all())
        xx.done()

    def test_decimate(self):
        """ia.regrid, test too high a value for decimate throws exception - CAS-5313"""
        myia = _ia
        myia.fromshape("", [10, 10, 10])
        self.assertRaises(
            Exception, myia.regrid, axes=[0, 1], csys=myia.coordsys().torecord()
        )
        regridded = myia.regrid(
            axes=[0, 1], csys=myia.coordsys().torecord(), decimate=3
        )
        self.assertTrue(regridded)
        regridded.done()
        # decimate doen't matter for non-direction axis regridding
        regridded = myia.regrid(
            axes=[2], csys=myia.coordsys().torecord()
        )
        self.assertTrue(regridded)
        regridded.done()

    def test_complex(self):
        """Test regridding a complex image, CAS-1390"""
        shutil.copytree(ctsys_resolve(os.path.join(datapath, real1)), real1)
        shutil.copytree(ctsys_resolve(os.path.join(datapath, imag1)), imag1)
        myia = _ia
        for i in (0, 1):
            myia.open(real1)
            if i == 1:
                gg = myia.getchunk()
                myia.fromarray("", gg, myia.coordsys().torecord(), type='d')
            realpart = myia.getchunk()
            csys = myia.coordsys()
            csys.setincrement([-0.9, 0.9])
            rrg = myia.regrid(csys=csys.torecord())
            rrgpart = rrg.getchunk()
            rrg.done()
            myia.open(imag1)
            if i == 1:
                gg = myia.getchunk()
                myia.fromarray("", gg, myia.coordsys().torecord(), type='d')
            imagpart = myia.getchunk()
            irg = myia.regrid(csys=csys.torecord())
            irgpart = irg.getchunk()
            irg.done()
            compltype = 'c'
            if i == 1:
                compltype = 'cd'
            myia.fromshape("", myia.shape(), type=compltype)
            comp = myia.getchunk()
            comp = realpart + imagpart*1j
            myia.putchunk(comp)
            crg = myia.regrid(csys=csys.torecord())
            crgpart = crg.getchunk()
            crg.done()
            myia.done()
            self.assertTrue((crgpart == rrgpart + irgpart*1j).all())

    def test_multibeam(self):
        """test multibeams cannot be regridded"""
        myia = _ia
        myia.fromshape("",[100,100,20])
        myia.setrestoringbeam("20arcsec", "20arcsec", "0deg", channel=0)
        myia.setrestoringbeam("30arcsec", "30arcsec", "0deg", channel=1)
        self.assertRaises(
            Exception, myia.regrid, "", shape=[10, 10, 10],
            csys=myia.coordsys().torecord()
        )
        self.assertRaises(
            Exception, myia.regrid, "", shape=[10, 10, 10],
            csys=myia.coordsys().torecord(), axes=2
        )
        bb = myia.regrid(
            "", shape=[50, 50, 20],
            csys=myia.coordsys().torecord(), axes=[0, 1]
        )
        bb.done()
        myia.done()

if __name__ == '__main__':
     unittest.main()
