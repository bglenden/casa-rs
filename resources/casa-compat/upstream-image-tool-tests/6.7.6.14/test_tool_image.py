##########################################################################
# test_tool_image.py
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
# https://casadocs.readthedocs.io/en/latest/api/tt/casatools.image.html
#
# Methods tested in this script
# adddegaxes, addnoise, boxcar, calcmask, commonbeam, newimagefromarray, newimagefromshape,continuumsub,
# convertflux, convolve, convolve, convolve2d, coordmeasures, crop, decimate, decompose, deconvolvecomponentlist,
# deconvolvefrombeam, fft, findsources, fromarray, fromcomplist, fromfits, fromimage, fromrecord, fromshape,
# getregion, hanning, histograms, imageconcat, insert, isconform, makecomplex, maskhandler, modify,
# newfromimage, pad, putchunk, getchunk, putregion, replacemaskedpixels, restoringbeam, rotate, sepconvolve, set,
# setbrightnessunit, setcoordsys, setmiscinfo, summary, tofits, twopointcorrelation, fitsheader
##########################################################################
import shutil
import pytest
import unittest
import os
import numpy as np
import math

from casatools import image as iatool
from casatools import coordsys, ctsys, functional, quanta
from casatools import measures, regionmanager, table
from casatools import componentlist as cltool
qa = quanta()
cs = coordsys()

class ImageBase(unittest.TestCase):
    # Path to input test data
    datapath = ctsys.resolve('unittest/image/')

    def setUp(self):
        self._myia = iatool()
        self.tb = table()
        self.qa = quanta()
        self.rg = regionmanager()
        self.cl = cltool()
        self.mymask = ''
        self.imagename = ''
        self.im1 = ''
        self.im2 = ''
        self.outfile = ''
        self.kernel = ''
        self.insert = ''
        self.outcont = ''
        self.outline = ''


    def tearDown(self):
        self._myia.done()
        self.tb.done()
        self.qa.done()
        self.cl.done()


# Tests for image.adddegaxes
class ia_adddegaxes_test(ImageBase):


    def tearDown(self):
        self._myia.done()
        data = ["ia.fromshape2.image_c" , "ia.fromshape.image_c",
                "ia.fromshape2.image_f" , "ia.fromshape.image_f" ]
        for f in data:
            if os.path.exists(f) and os.path.isdir(f):
                shutil.rmtree(f)


    def test_general(self):
        """general tests"""
        myim = self._myia

        for t in ('f', 'c'):
            # Make RA/DEC image
            imname = 'ia.fromshape.image_' + t
            imshape = [10,10]
            myim.fromshape(imname, imshape, type=t)
            self.assertTrue(myim)
            self.assertRaises(Exception, myim.adddegaxes, direction=True)
            myim2 = myim.adddegaxes(spectral=True)
            self.assertTrue(myim2)
            s = myim2.shape()
            s2 = [imshape[0],imshape[1],1]
            self.assertTrue((s == s2).all())
            mycs = myim2.coordsys()
            types = mycs.axiscoordinatetypes()
            self.assertTrue(types[2] == 'Spectral')
            self.assertTrue(mycs.done())
            self.assertTrue(myim2.done())
            myim2 = myim.adddegaxes(stokes='i')
            self.assertTrue(myim2)
            s = myim2.shape()
            s2 = [imshape[0],imshape[1],1]
            self.assertTrue((s == s2).all())
            mycs = myim2.coordsys()
            types = mycs.axiscoordinatetypes()
            self.assertTrue(types[2] == 'Stokes')
            self.assertTrue(mycs.done())
            self.assertTrue(myim2.done())
            #
            myim2 = myim.adddegaxes(linear=True)
            self.assertTrue(myim2)
            s = myim2.shape()
            s2 = [imshape[0],imshape[1],1]
            self.assertTrue((s == s2).all())
            mycs = myim2.coordsys()
            types = mycs.axiscoordinatetypes()
            self.assertTrue(types[2] == 'Linear')
            self.assertTrue(mycs.done())
            self.assertTrue(myim2.done())

            myim2 = myim.adddegaxes(tabular=True)
            self.assertTrue(myim2)
            s = myim2.shape()
            s2 = [imshape[0],imshape[1],1]
            self.assertTrue((s == s2).all())
            mycs = myim2.coordsys()
            types = mycs.axiscoordinatetypes()
            self.assertTrue(types[2] == 'Tabular')
            self.assertTrue(mycs.done())
            self.assertTrue(myim2.done())
            self.assertTrue(myim.done())
            #
            # Make Spectral image
            #
            mycs = cs.newcoordsys(spectral=True)
            self.assertTrue(mycs)
            imname = 'ia.fromshape2.image_' + t
            imshape = [10]
            myim.fromshape(imname, imshape, csys=mycs.torecord(), type=t)
            self.assertTrue(myim)
            myim2 = myim.adddegaxes(direction=True)
            self.assertTrue(myim2)
            s = myim2.shape()
            s2 = [imshape[0],1,1]
            self.assertTrue((s == s2).all())
            mycs2 = myim2.coordsys()
            types = mycs2.axiscoordinatetypes()
            self.assertTrue(types[1] == 'Direction' and types[2] == 'Direction')
            self.assertTrue(mycs2.done())
            self.assertTrue(myim2.done())
            self.assertTrue(mycs.done())
            self.assertTrue(myim.done())
        cs.done( )


    def test_beams(self):
        """test hyperbeams get accounted for correctly"""
#        qa = quanta( )
#        cs = coordsys( )
        myia = self._myia
        myia.fromshape(shape=[10, 10, 10])
        major = "4arcsec"
        minor = "3arcsec"
        pa = "4deg"
        nminor = "2arcsec"
        myia.setrestoringbeam(major=major, minor=minor, pa=pa, channel=1)
        myia.setrestoringbeam(major=major, minor=nminor, pa=pa, channel=3)
        deg = myia.adddegaxes(stokes="I")
        self.assertTrue((deg.shape() == [10, 10, 10, 1]).all())
        beam = deg.restoringbeam(channel=1)
        self.assertTrue(beam["major"] == qa.quantity(major))
        self.assertTrue(beam["minor"] == qa.quantity(minor))
        self.assertTrue(beam["positionangle"] == qa.quantity(pa))
        beam = deg.restoringbeam(channel=3)
        self.assertTrue(beam["major"] == qa.quantity(major))
        self.assertTrue(beam["minor"] == qa.quantity(nminor))
        self.assertTrue(beam["positionangle"] == qa.quantity(pa))
        qa.done( )
        deg.done()


    def test_history(self):
        """Test history writing"""
        myia = self._myia
        myia.fromshape("", [10,10])
        deg = myia.adddegaxes(spectral=True)
        myia.done()
        msgs = deg.history()
        deg.done()
        self.assertTrue("ia.adddegaxes" in msgs[-2])
        self.assertTrue("ia.adddegaxes" in msgs[-1])


# Tests for image.addnoise
class ia_addnoise_test(ImageBase):


    def tearDown(self):
        self._myia.done()
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()


    def test_history(self):
        """Test history is added to image"""
        myia = self._myia
        myia.fromshape("", [10, 10])
        myia.addnoise()
        msgs = myia.history();
        self.assertTrue("addnoise" in msgs[-1])
        myia.done()


class ia_beamarea_test(ImageBase):


    def test_exceptions(self):
        """Test various exception cases"""
        def __test_exception(method_parms, expected_msg):
            res = None
            with self.assertRaises(RuntimeError) as cm:
                res = ia.beamarea(**method_parms)
            print('res', res)
            got_exception = cm.exception
            pos = str(got_exception).find(expected_msg)
            self.assertNotEqual(
                pos, -1, msg=f'Unexpected exception was thrown: {got_exception}'
            )


        ia = self._myia
        # no beam
        ia.fromshape('', [200,200,4,10])
        __test_exception({},
                'This image has no beam(s). The setrestoringbeam() method may be '
                'used to set its beam(s).'
            )
        # channel too high
        ia.setrestoringbeam(
                major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
                polarization=0
            )
        parms = {}
        parms['channel'] = 15
        parms['polarization'] = 0
        __test_exception(
                parms,
                'This image only has 10 channels, so a channel value less than '
                '10 must be specified.'
            )
        parms['channel'] = 8
        parms['polarization'] = 5
        __test_exception(
                parms,
                'This image only has 4 polarizations, so a polarization value '
                'less than 4 must be specified.'
            )
        parms['channel'] = -1
        parms['polarization'] = 0
        __test_exception(
                parms,
                'In the case of a multibeam image, either both channel and '
                'polarization must be non-negative, or both must be negative.'
            )
        parms['channel'] = 0
        parms['polarization'] = -1
        __test_exception(
                parms,
                'In the case of a multibeam image, either both channel and '
                'polarization must be non-negative, or both must be negative.'
            )
        ia.done()


    def test_single_beam(self):
        """Test getting area for a single beam"""
        ia = self._myia
        ia.fromshape('', [200,200,4,10])
        ia.setrestoringbeam(major='3arcmin', minor='1arcmin', pa='40deg')
        expec = ia.beamarea()
        for chan in (-1, 3, 13):
            for stokes in (-1, 3, 13):
                got = ia.beamarea(channel=chan, polarization=stokes)
                for k in ('arcsec2', 'pixels'):
                    self.assertEqual(got[k], expec[k])
        ia.done()


    def test_multi_beams(self):
        """Test getting the area for multiple beams"""
        ia = self._myia
        # polarization axis before spectral axis
        ia.fromshape('', [200,200,4,10])
        ia.setrestoringbeam(
                major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
                polarization=0
            )
        ia.setrestoringbeam(
                major='5arcmin', minor='4arcmin', pa='40deg', channel=3,
                polarization=1
            )
        got = ia.beamarea()
        f = np.pi/(4*np.log(2))
        expec_pix = np.full([4, 10], f*3*1)
        expec_pix[1, 3] = f*5*4
        expec_as2 = 3600*expec_pix
        self.assertTrue(np.allclose(got['pixels'], expec_pix))
        self.assertTrue(np.allclose(got['arcsec2'], expec_as2))
        for i in range(4):
            for j in range(10):
                expec_pix = f*5*4 if i == 1 and j == 3 else f*3*1
                expec_as2 = 3600*expec_pix
                got = ia.beamarea(channel=j, polarization=i)
                self.assertTrue(np.isclose(got['pixels'], expec_pix))
                self.assertTrue(np.isclose(got['arcsec2'], expec_as2))
        ia.done()

        # spectral axis before polarization axis
        ia.fromshape('', [200,200,10,4])
        ia.setrestoringbeam(
                major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
                polarization=0
            )
        ia.setrestoringbeam(
                major='5arcmin', minor='4arcmin', pa='40deg', channel=3,
                polarization=1
            )
        got = ia.beamarea()
        expec_pix = np.full([10, 4], f*3*1)
        expec_pix[3, 1] = f*5*4
        expec_as2 = 3600*expec_pix
        self.assertTrue(np.allclose(got['pixels'], expec_pix))
        self.assertTrue(np.allclose(got['arcsec2'], expec_as2))
        for i in range(10):
            for j in range(4):
                expec_pix = f*5*4 if i == 3 and j == 1 else f*3*1
                expec_as2 = 3600*expec_pix
                got = ia.beamarea(channel=i, polarization=j)
                self.assertTrue(np.isclose(got['pixels'], expec_pix))
                self.assertTrue(np.isclose(got['arcsec2'], expec_as2))
        ia.done()


# Tests for image.boxcar
class ia_boxcar_test(ImageBase):

    def tearDown(self):
        self.assertTrue(len(self.tb.showcache()) == 0)
        self.tb.done()
        data = [self.mymask, self.imagename]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def test_stretch(self):
        """ ia.boxcar(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.boxcar, mask=self.mymask + ">0", stretch=False
        )
        zz = yy.boxcar(
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_general(self):
        """Test general behavior"""
        myia = self._myia
        length = 13
        self.imagename = "test_gen.im"
        myia.fromshape(self.imagename, [1, 1, length])
        bb = myia.getchunk()
        for i in range(length):
            bb[0, 0, i] = i * i + 1
        gg = bb.ravel()
        myia.putchunk(bb)
        rg = regionmanager()
        for i in range(length):
            reg = rg.box([0, 0, 0], [0, 0, i])
            for width in [3, 4]:
                outfile = "out" + str(i) + str(width) + ".im"
                if (i < width - 1):
                    self.assertRaises(
                        Exception, myia.boxcar, region=reg, axis=2, width=width
                    )
                else:
                    undec = []
                    if width == 3:
                        for j in range(i - 1):
                            undec.append((gg[j] + gg[j + 1] + gg[j + 2]) / 3.0)
                    elif width == 4:
                        for j in range(i - 2):
                            undec.append((gg[j] + gg[j + 1] + gg[j + 2] + gg[j + 3]) / 4.0)
                    for drop in [False, True]:
                        if drop:
                            for dmethod in ("c", "m"):
                                outfile = "out" + str(i) + str(width) + str(drop) + dmethod + ".im"
                                runit = dmethod != "m" or i >= 2 * width - 1
                                expec = []
                                kk = 0
                                if dmethod == "c":
                                    while kk < len(undec):
                                        expec.append(undec[kk])
                                        kk += width
                                elif dmethod == "m":
                                    if not runit:
                                        self.assertRaises(
                                            Exception, myia.boxcar, region=reg, axis=2,
                                            width=width, dmethod=dmethod
                                        )
                                    while kk < int(len(undec) / width) * width:
                                        sum = 0
                                        npoints = 0
                                        for jj in range(width):
                                            if kk + jj == len(undec):
                                                break
                                            else:
                                                sum += undec[kk + jj]
                                                npoints += 1
                                        expec.append(sum / float(npoints))
                                        kk += width
                                if runit:
                                    boxcar = myia.boxcar(
                                        region=reg, axis=2, drop=drop, dmethod=dmethod,
                                        width=width
                                    )
                                    got = boxcar.getchunk().ravel()
                                    self.assertTrue((abs(got / expec - 1) < 1e-6).all())
                                    boxcar.done()
                        else:
                            dmethod = "c"
                            expec = undec
                            boxcar = myia.boxcar(
                                region=reg, axis=2, drop=drop, dmethod=dmethod,
                                width=width
                            )
                            got = boxcar.getchunk().ravel()
                            self.assertTrue((abs(got / expec - 1) < 1e-6).all())
                            boxcar.done()
        rg.done()
        myia.done()

    def test_history(self):
        """test writing of history"""
        myia = iatool()
        myia.fromshape("", [20, 20, 20])
        zz = myia.boxcar()
        orig = len(myia.history())
        end = len(zz.history())
        myia.done()
        zz.done()
        self.assertTrue(end > orig, "Wrong number of history records found")

    def test_iicopy(self):
        """Test image info copy"""
        myia = iatool()
        myia.fromshape("", [20, 20, 20])
        qa = quanta()
        major = qa.quantity("10arcmin")
        minor = qa.quantity("5arcmin")
        pa = qa.quantity("20deg")
        qa.done()
        unit = "Jy/beam"
        myia.setrestoringbeam(major=major, minor=minor, pa=pa)
        myia.setbrightnessunit(unit)
        zz = myia.boxcar()
        myia.done()
        beam = zz.restoringbeam()
        self.assertTrue(len(beam) == 3, "Incorrect beam")
        self.assertTrue(beam['major'] == major, "Wrong major axis")
        self.assertTrue(beam['minor'] == minor, "Wrong minor axis")
        self.assertTrue(beam['positionangle'] == pa, "Wrong pa")
        self.assertTrue(zz.brightnessunit() == unit, "Wrong unit")
        zz.done()

    def test_ref_value(self):
        """Verify smoothed axis has correct coordinate values"""
        myia = iatool()
        myia.fromshape("", [20, 20, 20])
        orig = []
        for i in range(20):
            orig.append(myia.toworld([0, 0, i])['numeric'][2])
        zz = myia.boxcar(width=3, drop=False)
        myia.done()
        self.assertTrue((zz.shape() == [20, 20, 18]).all())
        got = []
        for i in range(18):
            got.append(zz.toworld([0, 0, 0, i])['numeric'][2])
        zz.done()

# Tests for image.calcmask
class ia_calcmask_test(ImageBase):

    def tearDown(self):
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

        data = ["hist.im", "mycomplexmask.im", "myfloatmask.im",
                "mycomplex.im", "myfloat.im"]
        for f in data:
            if os.path.exists(f) and os.path.isdir(f):
                shutil.rmtree(f)

    def test_basic(self):
        """Test basic functionality of ia.calcmask()"""
        myia = iatool()
        self.im1 = "myfloatmask.im"
        myia.fromshape(self.im1, [2, 2], type='f')
        bb = myia.getchunk()
        bb[0, 0] = 1
        myia.putchunk(bb)
        myia.done()
        self.im2 = "myfloat.im"
        myia.fromshape(self.im2, [2, 2], type='f')
        myia.calcmask(self.im1 + "<= 0")
        mask = myia.getchunk(getmask=True)
        myia.done()
        for i in [0, 1]:
            for j in [0, 1]:
                if i == 0 and j == 0:
                    self.assertFalse(mask[i, j])
                else:
                    self.assertTrue(mask[i, j])

        self.im1 = "mycomplexmask.im"
        myia.fromshape(self.im1, [2, 2], type='c')
        bb = myia.getchunk()
        bb[0, 0] = 1 + 1j
        myia.putchunk(bb)
        myia.done()
        self.im2 = "mycomplex.im"
        myia.fromshape(self.im2, [2, 2], type='c')
        myia.calcmask("real(" + self.im1 + ") <= 0")
        mask = myia.getchunk(getmask=True)
        myia.done()
        for i in [0, 1]:
            for j in [0, 1]:
                if i == 0 and j == 0:
                    self.assertFalse(mask[i, j])
                else:
                    self.assertTrue(mask[i, j])

    def test_history(self):
        """Test history is written"""
        myia = iatool()
        self.im1 = "hist.im"
        myia.fromshape(self.im1, [20, 20])
        myia.calcmask(self.im1 + "== 0")
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.calcmask" in msgs[-2])
        self.assertTrue("ia.calcmask" in msgs[-1])

# Tests for image.commonbeam
class ia_commonbeam_test(ImageBase):

    def test_nobeam(self):
        """ test having no beam throws an exception"""
        self._myia.fromshape("", [2, 2, 2])
        self.assertRaises(Exception, self._myia.commonbeam)
        self._myia.done()

    def test_enclosingbeam(self):
        """ test case where one beam in the set encloses the others"""
        self._myia.fromshape("", [2, 2, 2])
        major = {'value': 4.0, 'unit': 'arcsec'}
        minor = {'value': 2.0, 'unit': 'arcsec'}
        pa = {'value': 0.0, 'unit': 'deg'}
        self._myia.setrestoringbeam(major=major, minor=minor, pa=pa, polarization=0)
        self._myia.setrestoringbeam(major="1arcsec", minor="1arcsec", pa="0deg", polarization=1)
        x = self._myia.commonbeam()
        print(str(x))
        self.assertTrue(x['major'] == major)
        self.assertTrue(x['minor'] == minor)
        self.assertTrue(x['pa'] == pa)
        self._myia.done()

    def test_onebeam(self):
        """ test global beam case"""
        self._myia.fromshape("", [2, 2, 2])
        major = {'value': 4.0, 'unit': 'arcsec'}
        minor = {'value': 2.0, 'unit': 'arcsec'}
        pa = {'value': 0.0, 'unit': 'deg'}
        self._myia.setrestoringbeam(major=major, minor=minor, pa=pa)
        x = self._myia.commonbeam()
        self.assertTrue(x['major'] == major)
        self.assertTrue(x['minor'] == minor)
        self.assertTrue(x['pa'] == pa)
        self._myia.done()

    def test_overlappingbeams(self):
        """ test case where one beam does not enclose the other"""
        self._myia.fromshape("", [2, 2, 2])
        major = {'value': 4.0, 'unit': 'arcsec'}
        minor = {'value': 2.0, 'unit': 'arcsec'}
        self._myia.setrestoringbeam(major=major, minor=minor, pa="0deg", polarization=0)
        self._myia.setrestoringbeam(major=major, minor=minor, pa="60deg", polarization=1)
        x = self._myia.commonbeam()
        print(str(x))
        self.assertTrue(x['major']['value'] < 4.486)
        self.assertTrue(x['minor']['value'] < 3.292)
        self.assertTrue(abs(x['pa']['value'] - 30) < 1e-7)
        self._myia.done()

# Tests for image.newimagefromarray and image.newimagefromshape
class ia_constructors_test(ImageBase):

    def tearDown(self):
        self.tb.done()
        if self.outfile:
            if os.path.isfile(self.outfile):
                os.unlink(self.outfile)
            else:
                shutil.rmtree(self.outfile)

    def test_newimagefromarray(self):
        """ test repeated call of newimagefromarray doesn't segfault, CAS-5646"""
        my_image = np.zeros([128, 128, 16])
        myia = iatool()
        self.outfile = "mynewimage.image"
        zz = myia.newimagefromarray(
            outfile=self.outfile, pixels=my_image,
            overwrite=True
        )
        myia.open(self.outfile)
        self.assertRaises(
            Exception, myia.newimagefromarray,
            outfile=self.outfile,
            pixels=my_image,
            overwrite=True
        )
        myia.done()
        zz.done()
        self.assertTrue(len(self.tb.showcache()) == 0)

    def test_history(self):
        """verify history writing"""
        myia = iatool()
        self.outfile = "zz"
        myia.fromshape(self.outfile, [20, 20])
        csys = myia.coordsys()
        ary = myia.getchunk()
        myia = myia.newimagefromarray(pixels=ary, csys=csys.torecord())
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.newimagefromarray" in msgs[-2])
        self.assertTrue("ia.newimagefromarray" in msgs[-1])

    def test_history1(self):
        """verify history writing"""
        myia = iatool()
        myia = myia.newimagefromshape(shape=[20, 20])
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.newimagefromshape" in msgs[-2])
        self.assertTrue("ia.newimagefromshape" in msgs[-1])

# Tests for image.continuumsub
class ia_continuumsub_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        data = [self.outline, self.outcont]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def test_beams(self):
        """test per plane beams get accounted for correctly"""
        qa = quanta()
        myia = self._myia
        myia.fromshape("", [4, 4, 20, 4])
        chunk = myia.getchunk()
        for i in range(20):
            for j in range(4):
                chunk[2, 2, i, j] = i
        myia.putchunk(chunk)
        myia.setrestoringbeam("3arcsec", "2arcsec", "4deg", channel=10)
        for i in range(20):
            for j in range(4):
                major = qa.quantity(i + 4 * j + 4, "arcsec")
                myia.setrestoringbeam(major, "2arcsec", "4deg", channel=i, polarization=j)
        rg = regionmanager()
        reg = rg.box(blc=[2, 2, 3, 2], trc=[2, 2, 17, 2])
        rg.done()
        self.outcont = "line.im"
        resid = myia.continuumsub(outcont=self.outcont, fitorder=1, region=reg)
        for i in range(resid.shape()[2]):
            exp = qa.quantity(i + 15, "arcsec")
            got = resid.restoringbeam(channel=i)["major"]
            self.assertTrue(got == exp)
        myia.open(self.outcont)
        self.assertTrue(myia.restoringbeam() == resid.restoringbeam())
        qa.done()
        resid.done()

    def test_history(self):
        "verify history is written to output"""
        fn = functional()
        g1d = fn.gaussian1d(40, 30, 10)
        fn.done()
        myia = self._myia
        myia.fromshape("", [1, 1, 50])
        bb = myia.getchunk()
        for i in range(50):
            bb[0, 0, i] = g1d.f(i)
        myia.putchunk(bb)
        self.outline = "outline"
        self.outcont = "outcont"
        xx = myia.continuumsub(outline=self.outline, outcont=self.outcont)
        myia.done()
        xx.done()
        for x in [self.outline, self.outcont]:
            myia.open(x)
            msgs = myia.history()
            myia.done()
            self.assertTrue("ia.continuumsub" in msgs[-6])
            self.assertTrue("ia.continuumsub" in msgs[-7])

# Tests for image.convertflux
class ia_convertflux_test(ImageBase):

    def tearDown(self):
        self._myia.done()

    def test_beams(self):
        """test per plane beams get accounted for correctly"""
        myia = self._myia
        qa = quanta()
        myia.fromshape("", [2, 2, 2])
        myia.setbrightnessunit("Jy/beam")
        myia.setrestoringbeam(
            "4arcsec", "3arcsec", "40deg", channel=-1,
            polarization=0
        )
        myia.setrestoringbeam(
            "5arcsec", "4arcsec", "40deg", channel=-1,
            polarization=1
        )
        got = myia.convertflux("1Jy", "1arcsec", "1arcsec", topeak=True, polarization=0)
        exp = qa.quantity("12Jy/beam")
        self.assertTrue(got["unit"] == exp["unit"])
        self.assertTrue(abs(got["value"] - exp["value"]) / exp["value"] < 1e-7)

        got = myia.convertflux("3Jy/beam", "4arcsec", "2arcsec", topeak=False, polarization=0)
        exp = qa.quantity("2Jy")
        self.assertTrue(got["unit"] == exp["unit"])
        self.assertTrue(abs(got["value"] - exp["value"]) / exp["value"] < 1e-7)

        got = myia.convertflux("1Jy", "1arcsec", "1arcsec", topeak=True, polarization=1)
        exp = qa.quantity("20Jy/beam")
        self.assertTrue(got["unit"] == exp["unit"])
        self.assertTrue(abs(got["value"] - exp["value"]) / exp["value"] < 1e-7)

        got = myia.convertflux("3Jy/beam", "4arcsec", "2arcsec", topeak=False, polarization=1)
        exp = qa.quantity("1.2Jy")
        self.assertTrue(got["unit"] == exp["unit"])
        self.assertTrue(abs(got["value"] - exp["value"]) / exp["value"] < 1e-7)
        qa.done()

# Tests for image.convolve
class ia_convolve_test(ImageBase):

    def tearDown(self):
        data = [self.kernel, self.mymask]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def test_stretch(self):
        """ ia.convolve(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        self.kernel = "kernel"
        shape = [200, 200, 1, 20]
        yy.fromshape(self.kernel, shape)
        yy.fromshape("", shape)
        yy.addnoise()
        yy.done()
        yy.fromshape("", shape)
        self.assertRaises(
            Exception,
            yy.convolve, "", self.mymask, mask=self.mymask + ">0", stretch=False
        )
        zz = yy.convolve("", self.mymask, mask=self.mymask + ">0", stretch=True)
        self.assertTrue(type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_history(self):
        """Test history writing"""
        yy = iatool()
        self.kernel = "khistory"
        shape = [200, 200, 1, 20]
        yy.fromshape(self.kernel, shape)
        yy.fromshape("", shape)
        yy.addnoise()
        yy.done()
        yy.fromshape("", shape)
        zz = yy.convolve("", self.kernel)
        yy.done()
        msgs = zz.history()
        zz.done()
        self.assertTrue("convolve" in msgs[-2])
        self.assertTrue("convolve" in msgs[-1])

# Tests for image.convolve2d
#class ia_convolve2d_test(unittest.TestCase):
class ia_convolve2d_test(ImageBase):

    def tearDown(self):
        if self.imagename:
            if os.path.isfile(self.imagename):
                os.unlink(self.imagename)
            else:
                shutil.rmtree(self.imagename)
        f = "jk.im"
        if os.path.isfile(f):
            os.unlink(f)
        elif os.path.isdir(f):
            shutil.rmtree(f)

        self.assertTrue(len(self.tb.showcache()) == 0, 'table cache is not empty')

    def _near(self, got, expected, tol):
        return self.qa.le(
            self.qa.div(self.qa.abs(self.qa.sub(got, expected)), expected), tol
        )

    def make_gauss2d(self, shape, xfwhm, yfwhm):
        fac = 4 * math.log(2)
        values = np.empty(shape, dtype=float)
        for i in range(shape[0]):
            x = shape[0] / 2 - i
            for j in range(shape[1]):
                y = shape[1] / 2 - j
                xfac = x * x * fac / (xfwhm * xfwhm)
                yfac = y * y * fac / (yfwhm * yfwhm)
                values[i, j] = math.exp(-(xfac + yfac));
        return values

    def run_convolve2d(self,
            imagename, major, minor, pa, targetres,
            outfile, kernel="gauss", beam={}, overwrite=False
    ):
        myia = iatool()
        myia.open(imagename)
        res = myia.convolve2d(
            type=kernel,
            major=major, minor=minor, pa=pa,
            targetres=targetres, outfile=outfile,
            beam=beam, overwrite=overwrite
        )
        myia.done()
        res.done()

    def _compare_beams(self, beam1, beam2):
        self.assertTrue(self._near(beam1["major"], beam2["major"], 2e-5))
        self.assertTrue(self._near(beam1["minor"], beam2["minor"], 2e-5))
        pa = []
        for b in [beam1, beam2]:
            if "positionangle" in b:
                pa.append(b["positionangle"])
            else:
                pa.append(b["pa"])

        diff = abs(
            self.qa.sub(
                self.qa.quantity(pa[0]),
                self.qa.quantity(pa[1])
            )["value"]
        )
        self.assertTrue(diff < 1e-5)

    def test_multibeam(self):
        """Test per plane beams"""
        myia = iatool()
        self.imagename = "test_image2dconvolver_multibeam.im"
        shutil.copytree(ctsys.resolve(os.path.join(self.datapath, self.imagename)), self.imagename)
        myia.open(self.imagename)
        major = "10arcmin"
        minor = "8arcmin"
        pa = "80deg"
        got = myia.convolve2d(axes=[0, 1], major=major, minor=minor, pa=pa)
        shape = myia.shape()
        for i in range(5):
            blc = [0, 0, i]
            trc = [shape[0] - 1, shape[1] - 1, i]
            reg = self.rg.box(blc=blc, trc=trc)
            xx = myia.subimage(region=reg)
            exp = xx.convolve2d(axes=[0, 1], major=major, minor=minor, pa=pa)
            expbeam = exp.restoringbeam()
            gotbeam = got.restoringbeam(channel=i)
            for j in ["major", "minor", "positionangle"]:
                self.assertTrue(self._near(gotbeam[j], expbeam[j], 2e-7))
            self.assertTrue(abs(got.getchunk(blc=blc, trc=trc) - exp.getchunk()).max() < 3e-5)
            exp.done()
            xx.done()
        myia.done()
        got.done()

    def test_targetres(self):
        """Test targetres parameter"""
        myia = iatool()
        self.imagename = "tres1.im"
        myia.fromshape(self.imagename, [100, 100])
        csys = myia.coordsys()
        csys.setunits(["arcsec", "arcsec"])
        csys.setincrement([-1, 1])
        myia.setcoordsys(csys.torecord())
        myia.setrestoringbeam(major="6arcsec", minor="3arcsec", pa="0deg")
        shape = myia.shape()
        values = self.make_gauss2d(shape, 3.0, 6.0)
        expected = self.make_gauss2d(shape, 5.0, 10.0)
        myia.putchunk(values)
        myia.done()
        emaj = self.qa.quantity("10arcsec")
        emin = self.qa.quantity("5arcsec")
        epa = self.qa.quantity("0deg")
        for unit in ("Jy/beam", "K"):
            myia.open(self.imagename)
            myia.setbrightnessunit(unit)
            myia.done()
            expected = self.make_gauss2d(shape, 5.0, 10.0)
            if (unit == "K"):
                expected *= 3.0 * 6.0 / 5.0 / 10.0
            # for code in (run_convolve2d, run_imsmooth):
            for targetres in [False, True]:
                if not targetres:
                    major = "8arcsec"
                    minor = "4arcsec"
                    pa = "0deg"
                    outfile = "tres1" + unit[0]
                else:
                    major = "10arcsec"
                    minor = "5arcsec"
                    pa = "0deg"
                    outfile = "tres2" + unit[0]
                self.run_convolve2d(
                    imagename=self.imagename, kernel="gaussian",
                    major=major, minor=minor, pa=pa, targetres=targetres,
                    outfile=outfile
                )
                myia.open(outfile)
                gotbeam = myia.restoringbeam()
                gotvals = myia.getchunk()
                myia.done()
                shutil.rmtree(outfile)
                self._compare_beams(
                    gotbeam, {"major": emaj, "minor": emin, "pa": epa}
                )

    def test_beam(self):
        """Test the beam parameter"""
        myia = iatool()
        self.imagename = "tbeam1.im"
        myia.fromshape(self.imagename, [100, 100])
        csys = myia.coordsys()
        csys.setunits(["arcsec", "arcsec"])
        csys.setincrement([1, 1])
        myia.setcoordsys(csys.torecord())
        myia.setbrightnessunit("Jy/beam")
        myia.setrestoringbeam(major="6arcsec", minor="3arcsec", pa="0deg")
        shape = myia.shape()
        myia.putchunk(self.make_gauss2d(shape, 3.0, 6.0))
        expected = self.make_gauss2d(shape, 5.0, 10.0)
        for beam in [
            {"major": "8arcsec", "minor": "4arcsec", "pa": "0deg"},
            {
                "major": {"unit": "arcsec", "value": 8},
                "minor": {"unit": "arcsec", "value": 4},
                "pa": {"unit": "deg", "value": 0},
            }
        ]:
            outfile = 'convolve2d'
            x = self.run_convolve2d(
                imagename=self.imagename, major="", minor="", pa="",
                beam=beam, outfile=outfile, targetres=False,
                overwrite=True
            )
            if type(x) == type(myia):
                x.done()
            myia.open(outfile)
            maxdiff = (abs(myia.getchunk() - expected)).max()
            self.assertTrue(maxdiff < 1e-6)
            myia.done()
            shutil.rmtree(outfile)

    def test_history(self):
        """Test that history is written"""
        myia = iatool()
        self.imagename = "zz.im"
        myia.fromshape(self.imagename, [20, 20])
        major = "2arcmin"
        minor = "2arcmin"
        pa = "0deg"
        bb = myia.convolve2d("", major=major, minor=minor, pa=pa)
        myia.done()
        msgs = bb.history()
        bb.done()
        teststr = "ia.convolve2d"
        self.assertTrue(teststr in msgs[-4], "'" + teststr + "' not found")
        self.assertTrue(teststr in msgs[-3], "'" + teststr + "' not found")

    def test_stretch(self):
        """ ia.convolve2d(): Test stretch parameter"""
        yy = iatool()
        self.imagename = "maskim"
        yy.fromshape(self.imagename, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.convolve2d, "", [0, 1], "gaussian", "4arcmin",
            "4arcmin", "0deg", mask=self.imagename + ">0", stretch=False
        )
        zz = yy.convolve2d(
            "", [0, 1], "gaussian", "4arcmin", "4arcmin", "0deg",
            mask=self.imagename + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_precision(self):
        """Test images of various precisions"""
        yy = iatool()
        for mytype in ['d', 'c', 'f', 'cd']:
            yy.fromshape("", [20, 20, 1], type=mytype)
            yy.addnoise()
            if mytype == 'f' or mytype == 'd':
                zz = yy.convolve2d(
                    "", [0, 1], "gaussian", "4arcmin", "4arcmin", "0deg"
                )
                self.assertTrue(zz)
                zz.done()
            else:
                self.assertRaises(
                    Exception, yy.convolve2d, "", [0, 1], "gaussian",
                    "4arcmin", "4arcmin", "0deg"
                )
            yy.done()

    def test_copying_of_input_mask(self):
        """CAS-12904: copy input mask to output image"""
        self.imagename = 'orig.im'
        yy = iatool()
        yy.fromshape(self.imagename, [100, 100, 3])
        pix = yy.getchunk()
        for i in range(3):
            pix[:, :, i] = i
        yy.putchunk(pix)
        subi = yy.subimage("", mask=self.imagename + '>0')
        yy.done()
        for i in range(3):
            reg = self.rg.box([0, 0, i], [99, 99, i])
            npts = subi.statistics(region=reg)['npts']
            expec = 0 if i == 0 else 1
            # shows mask was created correctly
            self.assertEqual(npts.size, expec, 'wrong length npts array')
            if i > 0:
                self.assertEqual(npts[0], 10000, 'wrong number of pts')
        conv = subi.convolve2d(
            major='4arcmin', minor='4arcmin', pa='0deg', mask=self.imagename + '<2'
        )
        subi.done()
        for i in range(3):
            reg = self.rg.box([0, 0, i], [99, 99, i])
            npts = conv.statistics(region=reg)['npts']
            expec = 1 if i == 1 else 0
            # shows mask was copied correctly
            self.assertEqual(npts.size, expec, 'wrong length npts array')
            if i == 1:
                self.assertEqual(npts[0], 10000, 'wrong number of pts')
        conv.done()

    def test_multi_channel_multi_pol(self):
        """
        Verify fix for CAS-14301, images with multiple channels and multiple
        polarizations are convolved correctly
        """
        yy = iatool()
        yy.fromshape("jk.im", [100,100,4,5])
        yy.addnoise()
        yy.setbrightnessunit("Jy/beam")
        for chan in range(5):
            for stokes in range(4):
                print("chan", chan, "stokes", stokes)
                maj = 4 + chan/10 + stokes/100
                yy.setrestoringbeam(
                    major=f"{maj}arcmin", minor="3arcmin", pa="20deg",
                    channel=chan, polarization=stokes
                )
        yy.done()
        yy.open("jk.im")
        z = yy.convolve2d("", major="5arcmin", minor="4arcmin", pa="20deg")
        stats = z.statistics(axes=[0, 1])
        z.done()
        for i in range(4):
            for j in range(5):
                print(stats["sumsq"][i, j])
                self.assertTrue(stats["sumsq"][i, j] > 0, f"Plane {i},{j} was not convolved")


# Tests for image.coordmeasures
class ia_coordmeasures_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_frame(self):
        """CAS-7927: Test returned frame is correct"""
        myia = self._myia
        myia.fromshape("", [20, 20, 20])
        cm = myia.coordmeasures()
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(cm['measure']['spectral']['frequency']['m0']['value'] == 1.415e9, "wrong frequency")
        self.assertTrue(cm['measure']['spectral']['frequency']['refer'] == "LSRK", "wrong frequency reference frame")
        self.assertTrue(cm["measure"]["direction"]['m0']['value'] == 0, "wrong RA")
        self.assertTrue(cm["measure"]["direction"]['m1']['value'] == 0, "wrong Dec")
        self.assertTrue(cm["measure"]["direction"]['refer'] == "J2000", "wrong direction reference frame")
        csys = myia.coordsys()
        csys.setconversiontype(direction='B1950', spectral='CMB')
        myia.setcoordsys(csys.torecord())

        for i in range(4):
            if i == 0:
                cm = myia.coordmeasures()
            elif i == 1:
                cm = myia.coordmeasures(dframe="cl", sframe="cl")
            elif i == 2:
                cm = myia.coordmeasures(dframe="cl")
            elif i == 3:
                cm = myia.coordmeasures(sframe="cl")
            self.assertTrue(cm, "Unable to get coordmeasures")
            self.assertTrue(abs(cm['measure']['spectral']['frequency']['m0']['value'] - 1416700650.52) < 0.1,
                            "wrong frequency")
            self.assertTrue(cm['measure']['spectral']['frequency']['refer'] == 'CMB', "wrong frequency reference frame")
            self.assertTrue(abs(cm["measure"]["direction"]['m0']['value'] - -0.0111827672206) < 1e-6, "wrong RA")
            self.assertTrue(abs(cm["measure"]["direction"]['m1']['value'] - -0.00485811473549) < 1e-6, "wrong Dec")
            self.assertTrue(cm["measure"]["direction"]['refer'] == "B1950", "wrong direction reference frame")

        cm = myia.coordmeasures(dframe="native", sframe="native")
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(cm['measure']['spectral']['frequency']['m0']['value'] == 1.415e9, "wrong frequency")
        self.assertTrue(cm['measure']['spectral']['frequency']['refer'] == "LSRK", "wrong frequency reference frame")
        self.assertTrue(cm["measure"]["direction"]['m0']['value'] == 0, "wrong RA")
        self.assertTrue(cm["measure"]["direction"]['m1']['value'] == 0, "wrong Dec")
        self.assertTrue(cm["measure"]["direction"]['refer'] == "J2000", "wrong direction reference frame")

        cm = myia.coordmeasures(dframe="cl", sframe="native")
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(cm['measure']['spectral']['frequency']['m0']['value'] == 1.415e9, "wrong frequency")
        self.assertTrue(
            abs(cm["measure"]["direction"]['m0']['value'] - -0.0111827672206) < 1e-6,
            "wrong RA"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m1']['value'] - -0.00485811473549) < 1e-6,
            "wrong Dec"
        )
        self.assertTrue(
            cm["measure"]["direction"]['refer'] == "B1950",
            "wrong direction reference frame"
        )

        cm = myia.coordmeasures(dframe="native", sframe="cl")
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(abs(cm['measure']['spectral']['frequency']['m0']['value'] - 1416700650.52) < 0.1,
                        "wrong frequency")
        self.assertTrue(cm['measure']['spectral']['frequency']['refer'] == 'CMB', "wrong frequency reference frame")
        self.assertTrue(cm["measure"]["direction"]['m0']['value'] == 0, "wrong RA")
        self.assertTrue(cm["measure"]["direction"]['m1']['value'] == 0, "wrong Dec")
        self.assertTrue(cm["measure"]["direction"]['refer'] == "J2000", "wrong direction reference frame")

        cm = myia.coordmeasures(dframe="GALACTIC", sframe="cl")
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(
            abs(cm['measure']['spectral']['frequency']['m0']['value'] - 1416700650.52) < 0.1,
            "wrong frequency"
        )
        self.assertTrue(
            cm['measure']['spectral']['frequency']['refer'] == 'CMB',
            "wrong frequency reference frame"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m0']['value'] - 1.68140724) < 1e-6,
            "wrong RA"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m1']['value'] - -1.05048941) < 1e-6,
            "wrong Dec"
        )
        self.assertTrue(
            cm["measure"]["direction"]['refer'] == "GALACTIC",
            "wrong direction reference frame"
        )

        cm = myia.coordmeasures(dframe="cl", sframe="LGROUP")
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(
            abs(cm['measure']['spectral']['frequency']['m0']['value'] - 1414142155.34) < 0.1,
            "wrong frequency"
        )
        self.assertTrue(
            cm['measure']['spectral']['frequency']['refer'] == 'LGROUP',
            "wrong frequency reference frame"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m0']['value'] - -0.0111827672206) < 1e-6,
            "wrong RA"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m1']['value'] - -0.00485811473549) < 1e-6,
            "wrong Dec"
        )
        self.assertTrue(
            cm["measure"]["direction"]['refer'] == "B1950",
            "wrong direction reference frame"
        )

        cm = myia.coordmeasures(dframe="GALACTIC", sframe="LGROUP")
        self.assertTrue(cm, "Unable to get coordmeasures")
        self.assertTrue(
            abs(cm['measure']['spectral']['frequency']['m0']['value'] - 1414142155.34) < 0.1,
            "wrong frequency"
        )
        self.assertTrue(
            cm['measure']['spectral']['frequency']['refer'] == 'LGROUP',
            "wrong frequency reference frame"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m0']['value'] - 1.68140724) < 1e-6,
            "wrong RA"
        )
        self.assertTrue(
            abs(cm["measure"]["direction"]['m1']['value'] - -1.05048941) < 1e-6,
            "wrong Dec"
        )
        self.assertTrue(
            cm["measure"]["direction"]['refer'] == "GALACTIC",
            "wrong direction reference frame"
        )

        self.assertRaises(Exception, myia.coordmeasures, dframe="CL", sframe="BOGUS")
        self.assertRaises(Exception, myia.coordmeasures, dframe="BOGUS", sframe="CL")

        myia.done()

# Tests for image.crop
class ia_crop_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_crop(self):
        """Test general cropping functionality"""
        myia = self._myia
        myia.fromshape("", [20, 20, 20])
        myrg = self.rg
        myrg.setcoordinates(myia.coordsys().torecord())
        # mask 4, 6, and 8 pixels at image edges for axes 0, 1, 2
        reg = myrg.complement(
            myrg.wbox(
                ["8.00002381e+00arcmin", "-7.00000484e+00arcmin", "1.41499400e+09Hz"],
                ["2.15930000e+04arcmin", "6.00000305e+00arcmin", "1.41500500e+09Hz"]
            )
        )
        myia.set(pixelmask=False, region=reg)
        crop = myia.crop()
        self.assertTrue((crop.shape() == [16, 14, 12]).all())
        crop = myia.crop(axes=[0])
        self.assertTrue((crop.shape() == [16, 20, 20]).all())
        crop = myia.crop(axes=[1])
        self.assertTrue((crop.shape() == [20, 14, 20]).all())
        crop = myia.crop(axes=[2])
        self.assertTrue((crop.shape() == [20, 20, 12]).all())
        crop = myia.crop(axes=[0, 1])
        self.assertTrue((crop.shape() == [16, 14, 20]).all())
        crop = myia.crop(axes=[0, 2])
        self.assertTrue((crop.shape() == [16, 20, 12]).all())
        crop = myia.crop(axes=[1, 2])
        self.assertTrue((crop.shape() == [20, 14, 12]).all())
        crop = myia.crop(axes=[0, 1, 2])
        self.assertTrue((crop.shape() == [16, 14, 12]).all())
        crop.done()

    def test_history(self):
        """Verify history writing"""
        myia = self._myia
        myia.fromshape("", [20, 20])
        bb = myia.crop()
        myia.done()
        msgs = bb.history()
        bb.done()
        self.assertTrue("ia.crop" in msgs[-4])
        self.assertTrue("ia.crop" in msgs[-3])

# Tests for image.decimate
class ia_decimate_test(ImageBase):

    def tearDown(self):
        self.rg.done()
        data = ["maskim", "mregions.im", "myim_0.im", "myim_1.im", "xx2.im"]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_stretch(self):
        """ ia.decimate(): Test stretch parameter"""
        yy = iatool()
        mymask = "maskim"
        yy.fromshape(mymask, [20, 20, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [20, 20, 1, 5]
        yy.fromshape("", shape)
        # yy.addnoise()
        self.assertRaises(
            Exception,
            yy.decimate, outfile="xx.im", mask=mymask + ">0",
            stretch=False
        )
        zz = yy.decimate(
            outfile="xx2.im", mask=mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_methods(self):
        """Test straight aggregation methods"""
        myia = iatool()
        for m in [0, 1]:
            imagename = "myim_" + str(m) + ".im"
            shape = np.array([10, 20, 41, 1])
            myia.fromshape(imagename, shape)
            myia.addnoise()
            myia.calcmask(imagename + " < 0")
            factor = 4
            if m == 0:
                method = "copy"
            if m == 1:
                method = "mean"
            zz = myia.decimate("", axis=2, factor=factor, method=method)
            expec = shape
            expec[2] /= factor
            if m == 0:
                expec[2] += 1
            print("method %s" % method)
            self.assertTrue((zz.shape() == expec).all())

            inc = myia.coordsys()
            outc = zz.coordsys()
            expec = inc.increment()['numeric']
            expec[2] *= factor
            got = outc.increment()['numeric']
            self.assertTrue((expec == got).all())

            expec = inc.referencepixel()['numeric']
            expec[2] /= factor
            got = outc.referencepixel()['numeric']
            self.assertTrue((expec == got).all())

            expec = inc.referencevalue()['numeric']
            got = outc.referencevalue()['numeric']
            self.assertTrue((expec == got).all())

            for i in range(10):
                blc = [0, 0, i * factor, 0]
                trc = shape - 1
                if m == 0:
                    trc[2] = i * factor
                    expdata = myia.getchunk(blc, trc)
                    expmask = myia.getchunk(blc, trc, getmask=True)
                elif m == 1:
                    trc[2] = (i + 1) * factor - 1
                    reg = self.rg.box(blc, trc)
                    cc = myia.collapse("mean", 2, region=reg)
                    expdata = cc.getchunk()
                    expmask = cc.getchunk(getmask=True)
                blc = [0, 0, i, 0]
                trc = shape - 1
                trc[2] = i
                got = zz.getchunk(blc, trc)
                self.assertTrue((expdata == got).all())
                got = zz.getchunk(blc, trc, getmask=True)
                self.assertTrue((expmask == got).all())
            myia.done()
            zz.done()

    def test_multiple_regions(self):
        """Test multiple region support"""
        myia = iatool()
        self.rg = regionmanager()
        myia.fromshape("", [20, 20, 20])
        r1 = self.rg.frombcs(
            box="0, 0, 9, 9", csys=myia.coordsys().torecord(),
            shape=myia.shape()
        )
        r2 = self.rg.frombcs(
            box="10, 10, 19, 19", csys=myia.coordsys().torecord(),
            shape=myia.shape()
        )
        regions = {'region1': r1, 'region2': r2}
        reg = self.rg.makeunion(regions)
        bb = myia.decimate("mregions.im", axis=2, factor=2, region=reg)
        self.assertTrue(bb.getchunk([0, 0, 0], [9, 9, 9], getmask=True).all())
        self.assertTrue(bb.getchunk([10, 10, 0], [19, 19, 9], getmask=True).all())
        self.assertTrue((bb.getchunk([0, 10, 0], [9, 19, 9], getmask=True) == False).all())
        self.assertTrue((bb.getchunk([10, 0, 0], [19, 9, 9], getmask=True) == False).all())

        bb.done()
        myia.done()

    def test_history(self):
        """Verify history writing"""
        myia = iatool()
        myia.fromshape("", [20, 20, 20])
        bb = myia.decimate("")
        myia.done()
        msgs = bb.history()
        bb.done()
        self.assertTrue("ia.decimate" in msgs[-2])
        self.assertTrue("ia.decimate" in msgs[-1])

# Tests for image.decompose
class ia_decompose_test(ImageBase):

    def tearDown(self):
        if self.mymask:
            if os.path.isfile(self.mymask):
                os.unlink(self.mymask)
            else:
                shutil.rmtree(self.mymask)

    def test_stretch(self):
        """ ia.decompose(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [20, 20, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [20, 20, 1, 5]
        yy.fromshape("", shape)
        # yy.addnoise()
        self.assertRaises(
            Exception,
            yy.decompose, threshold=0.001, mask=self.mymask + ">0",
            stretch=False
        )
        zz = yy.decompose(
            threshold=0.001, mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type({}))
        yy.done()

# Tests for image.deconvolvecomponentlist
class ia_deconvolvecomponentlist_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        self.cl.done()
        self.qa.done()

    def __near(self, got, exp, tol):
        qgot = self.qa.quantity(got)
        qexp = self.qa.quantity(exp)
        return self.qa.abs(self.qa.div(self.qa.sub(qgot, qexp), qexp))["value"] < tol

    def test_multibeams(self):
        """ ia.deconvolvecomponentlist(): Test multi beams"""
        myia = self._myia
        mycl = self.cl
        mycl.addcomponent(
            flux=1, dir=["J2000", "2h0m0s", "40d0m0s"], shape="gauss",
            majoraxis="4arcsec", minoraxis="3arcsec", positionangle="20deg"
        )

        myia.fromshape("", [2, 2, 2])
        mycs = myia.coordsys()
        mycs.setunits(["deg", "deg", ""])
        mycs.setdirection(
            refcode="J2000", refval=[30, 40],
            incr=[-1.0 / 36000, 1.0 / 36000]
        )
        myia.setcoordsys(mycs.torecord())
        myia.setrestoringbeam(
            major="2arcsec", minor="1arcsec", pa="20deg",
            polarization=0
        )
        myia.setrestoringbeam(
            major="3arcsec", minor="2arcsec", pa="50deg",
            polarization=1
        )
        bb = cltool()
        emaj = [
            self.qa.quantity({'unit': 'arcsec', 'value': 3.4641016151377548}),
            self.qa.quantity({'unit': 'arcsec', 'value': 3.0203474964295665})
        ]
        emin = [
            self.qa.quantity({'unit': 'arcsec', 'value': 2.8284271247461894}),
            self.qa.quantity({'unit': 'arcsec', 'value': 1.6963198403637358})
        ]
        epa = [
            self.qa.quantity({'unit': 'deg', 'value': 20}),
            self.qa.quantity({'unit': 'deg', 'value': -1.948943124031587 + 180})
        ]
        tol = 1e-10
        for i in [0, 1]:
            res = myia.deconvolvecomponentlist(mycl.torecord(), polarization=i)
            bb.fromrecord(res)
            shape = bb.getshape(0)
            bb.done()
            self.assertTrue(self.__near(shape["majoraxis"], emaj[i], tol))
            self.assertTrue(self.__near(shape["minoraxis"], emin[i], tol))
            print("*** pa %s" % shape["positionangle"])
            self.assertTrue(self.__near(shape["positionangle"], epa[i], tol))

        myia.done()
        mycl.done()

# Tests for image.deconvolvefrombeam
class ia_deconvolvefrombeam_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        self.qa.done()

    def __near(self, got, exp, tol):
        qgot = self.qa.quantity(got)
        qexp = self.qa.quantity(exp)
        return self.qa.abs(self.qa.div(self.qa.sub(qgot, qexp), qexp))["value"] < tol

    def test_multibeams(self):
        """ ia.deconvolvefrombeam(): Basic tests"""
        print("*** start ")
        myia = self._myia
        source = ["4arcsec", "3arcsec", "20deg"]
        beam = [
            ["2arcsec", "1arcsec", "20deg"],
            ["3arcsec", "2arcsec", "50deg"]
        ]
        emaj = [
            self.qa.quantity({'unit': 'arcsec', 'value': 3.4641016151377548}),
            self.qa.quantity({'unit': 'arcsec', 'value': 3.0203474964295665})
        ]
        emin = [
            self.qa.quantity({'unit': 'arcsec', 'value': 2.8284271247461894}),
            self.qa.quantity({'unit': 'arcsec', 'value': 1.6963198403637358})
        ]
        epa = [
            self.qa.quantity({'unit': 'deg', 'value': 20}),
            self.qa.quantity({'unit': 'deg', 'value': -1.9489431240069859})
        ]
        tol = 1e-10
        for i in [0, 1]:
            res = myia.deconvolvefrombeam(source, beam[i])
            fit = res["fit"]
            self.assertTrue(self.__near(fit["major"], emaj[i], tol))
            self.assertTrue(self.__near(fit["minor"], emin[i], tol))
            print("*** got %s" % fit["pa"])
            print("*** exp %s" % epa[i])
            self.assertTrue(self.__near(fit["pa"], epa[i], tol))

# Tests for image.fft
class ia_fft_test(ImageBase):

    def tearDown(self):
        data = [
            "amp.imc", "amp.imf", "amp_reg",
            "complex.imc", "complex.imf",
            "imag.imc", "imag.imf", "imag_reg",
            "maskim", "myamp.im", "mycomplex.im",
            "myimag.im", "myphase.im", "myreal.im",
            "phase.imc", "phase.imf", "phase_reg",
            "real2.im", "real.imc", "real.imf", "real_reg"]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

        self.assertTrue(len(self.tb.showcache()) == 0)
        self.tb.done()

    def test_stretch(self):
        """ ia.fft(): Test stretch parameter"""
        yy = self._myia
        mymask = "maskim"
        yy.fromshape(mymask, [20, 20, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [20, 20, 1, 5]
        yy.fromshape("", shape)
        # yy.addnoise()
        self.assertRaises(
            Exception,
            yy.fft, real="real1.im", mask=mymask + ">0",
            stretch=False
        )
        real = 'real2.im'
        zz = yy.fft(
            real=real, mask=mymask + ">0", stretch=True
        )
        shutil.rmtree(mymask)
        shutil.rmtree(real)
        self.assertTrue(type(zz) == type(False))
        yy.done()

    def test_delta(self):
        """Test fft of delta function"""
        myia = self._myia
        for t in ['f', 'c']:
            myia.fromshape("", [100, 100], type=t)
            bb = myia.getchunk()
            bb[50, 50] = 1
            myia.putchunk(bb)
            real = "real.im" + t
            imag = "imag.im" + t
            amp = "amp.im" + t
            phase = "phase.im" + t
            _complex = "complex.im" + t
            myia.fft(
                real=real, imag=imag, amp=amp,
                phase=phase, complex=_complex
            )
            for im in [real, imag, amp, phase, _complex]:
                expec = 1
                if im == imag or im == phase:
                    expec = 0
                elif im == _complex:
                    expec = 1 + 0j
                myia.open(im)
                got = myia.getchunk()
                myia.done(remove=True)
                self.assertTrue((got == expec).all())

    def test_regression(self):
        """Was regression test in imagetest"""

        # Open test image (has sky coordinates)
        #testname = 'unittest/ia_fft/test_image.im'
        testname = os.path.join(self.datapath, 'test_image.im')
        myia = self._myia
        testim = self._myia
        testim.open(ctsys.resolve(testname))
        self.assertTrue(testim)
        testshape = testim.shape()
        self.assertTrue(len(testshape) == 3)
        rname = 'real_reg'
        iname = 'imag_reg'
        aname = 'amp_reg'
        pname = 'phase_reg'
        self.assertTrue(
            testim.fft(
                real=rname, imag=iname, phase=pname, amp=aname
            )
        )
        im1 = myia.newimage(rname)
        self.assertTrue(im1)
        im2 = myia.newimage(iname)
        self.assertTrue(im2)
        im3 = myia.newimage(aname)
        self.assertTrue(im3)
        im4 = myia.newimage(pname)
        self.assertTrue(im4)
        trc = testim.shape()
        trc[2] = 0
        a1 = im1.getchunk(trc=trc)
        a2 = im2.getchunk(trc=trc)
        a3 = im3.getchunk(trc=trc)
        a4 = im4.getchunk(trc=trc)

        p = testim.getchunk(trc=trc)
        c = np.fft.fft2(p)
        b1 = c.real
        b2 = c.imag
        b3 = abs(c)  # sqrt( real(x)^2 + imag(x)^2 )

        ok = im1.remove(True) and im2.remove(True) and im3.remove(True) and im4.remove(True)
        self.assertTrue(ok)
        #
        # FFT whole image
        #
        ndim = len(testim.shape())
        axes = list(range(ndim))
        ok = testim.fft(real=rname, imag=iname, phase=pname, amp=aname, axes=axes)
        self.assertTrue(ok)
        im1 = myia.newimage(rname)
        self.assertTrue(im1)
        im2 = myia.newimage(iname)
        self.assertTrue(im2)
        im3 = myia.newimage(aname)
        self.assertTrue(im3)
        im4 = myia.newimage(pname)
        self.assertTrue(im4)
        a1 = im1.getchunk()
        a2 = im2.getchunk()
        a3 = im3.getchunk()
        a4 = im4.getchunk()

        p = testim.getchunk()
        c = np.fft.fft2(p)
        b1 = c.real
        b2 = c.imag
        b3 = abs(c)

        self.assertTrue(
            testim.done() and im1.done(remove=True)
            and im2.done(remove=True) and im3.done(remove=True)
            and im4.done(remove=True)
        )

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        (real, imag, amp, phase, complx) = ("myreal.im", "myimag.im", "myamp.im", "myphase.im", "mycomplex.im")
        myia.fromshape("", [20, 20])
        myia.fft(real=real, imag=imag, amp=amp, phase=phase, complex=complx)
        myia.done()
        for im in (real, imag, amp, phase, complx):
            myia.open(im)
            msgs = myia.history()
            myia.done(remove=True)
            self.assertTrue("ia.fft" in msgs[-2])
            self.assertTrue("ia.fft" in msgs[-1])

    def test_units(self):
        """
        CAS-13489: test output units

        The output phase image should have units of radians
        If the input image represents the image plane and has units of Jy/beam or Jy/pixel,
        then the ouptut (uv-plane) images should have units of Jy. If the input image has
        a beam, the beam should be copied to the output images.
        If the input image represents the uv-plane, then the brightness unit of the
        output (image plane) images should be Jy/beam or Jy/pixel, depending on if the
        input image has a beam or not.
        """
        qa = quanta()
        bmaj = qa.quantity('4arcmin')
        bmin = qa.quantity('3arcmin')
        bpa = qa.quantity('60deg')
        _ia = self._myia
        for bu in ('Jy/beam', 'Jy/pixel'):
            # create image-domain image
            _ia.fromshape("", [20, 20])
            self.assertTrue(_ia.setbrightnessunit('Jy/pixel'), 'Failed to set brightness unit')
            if bu == 'Jy/beam':
                self.assertTrue(
                    _ia.setrestoringbeam(major=bmaj, minor=bmin, pa=bpa),
                    'Failed to set restoring beam'
                )
            real = "real.im"
            imag = "imag.im"
            amp = "amp.im"
            phase = "phase.im"
            _complex = "complex.im"
            self.assertTrue(
                _ia.fft(
                    real=real, imag=imag, amp=amp,
                    phase=phase, complex=_complex
                ), 'ia.fft() failed'
            )
            _ia.done()
            for im in (real, imag, amp, phase, _complex):
                _ia.open(im)
                bunit = _ia.brightnessunit()
                beam = _ia.restoringbeam()
                _ia.done(remove=(im != real))
                expec = 'Jy'
                if im == phase:
                    expec = 'rad'
                self.assertTrue(
                    bunit == expec, 'image ' + im + ' has unit ' + bunit
                    + ' but should be ' + expec
                )
                if bu == 'Jy/pixel':
                    self.assertTrue(beam == {}, 'this image should have no restoring beam')
                elif bu == 'Jy/beam':
                    self.assertTrue(beam['major'] == bmaj, 'Incorrect restoring beam')
                    self.assertTrue(beam['minor'] == bmin, 'Incorrect restoring beam')
                    self.assertTrue(beam['positionangle'] == bpa, 'Incorrect restoring beam')
            # transform from uv to image plane
            real1 = 'real1.im'
            _ia.open(real)
            self.assertTrue(
                _ia.fft(
                    real=real1, imag=imag, amp=amp,
                    phase=phase, complex=_complex, axes=[0, 1],
                ), 'ia.fft() failed'
            )
            _ia.done(remove=True)
            for im in (real1, imag, amp, phase, _complex):
                _ia.open(im)
                bunit = _ia.brightnessunit()
                beam = _ia.restoringbeam()
                _ia.done(remove=True)
                expec = bu
                if im == phase:
                    expec = 'rad'
                self.assertTrue(
                    bunit == expec, 'image ' + im + ' has unit ' + bunit
                    + ' but should be ' + expec
                )
                if bu == 'Jy/pixel':
                    self.assertTrue(beam == {}, 'this image should have no restoring beam')
                elif bu == 'Jy/beam':
                    self.assertTrue(beam['major'] == bmaj, 'Incorrect restoring beam')
                    self.assertTrue(beam['minor'] == bmin, 'Incorrect restoring beam')
                    self.assertTrue(beam['positionangle'] == bpa, 'Incorrect restoring beam')

    def test_new_inc(self):
        """verify CAS-13629 ouput cellsize fix"""
        myia = self._myia
        npix = 200
        myia.fromshape("", [npix, npix])
        myia.fft(real='real.im')
        myia.open('real.im')
        csys = myia.coordsys()
        myia.done(remove=True)
        cdelt = csys.increment()['numeric']
        csys.done()
        mye = 1 / (npix / 60 * np.pi / 180)
        expec = np.array([-mye, mye])
        print((cdelt - expec) / expec)
        self.assertTrue(np.allclose(cdelt, expec))

# Tests for image.findsources
class ia_findsources_test(ImageBase):

    def tearDown(self):
        self._myia.done()

    def test_units(self):
        """test various units are allowed"""
        myia = self._myia
        myia.maketestimage()
        mycl = cltool()
        for unit in ("Jy", "Jy.km/s"):
            myia.setbrightnessunit(unit)
            mycl.fromrecord(myia.findsources(cutoff=1, point=False))
            shape = mycl.getshape(0)
            self.assertTrue(shape['majoraxis']['value'] > 80)
            mycl.done()
        myia.done()

# Tests for image.fromarray
class ia_fromarray_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_fromarray(self):
        """Test general functionality"""
        myia = self._myia
        ar1 = np.zeros([2, 3], np.float64)
        fval = 2.2
        ar1[:] = fval
        ar2 = np.zeros([4, 4], complex)
        cval = 2 - 6j
        ar2[:] = cval
        i = 0
        for a in [ar1, ar2]:
            myia.fromarray("", a)
            self.assertTrue((myia.shape() == a.shape).all())
            bb = myia.getchunk()
            if (i == 0):
                self.assertTrue(abs(bb[0, 0] - fval) < 1e-6)
            else:
                self.assertTrue(bb[0, 0] == cval)
            i += 1
        myia.done()

    def test_history(self):
        """test writing of history"""
        myia = self._myia
        ar1 = np.zeros([2, 3], np.float64)
        myia.fromarray("", ar1)
        msgs = myia.history()
        self.assertTrue("ia.fromarray" in msgs[-2])
        self.assertTrue("ia.fromarray" in msgs[-1])

# Tests for image.fromcomplist
class ia_fromcomplist_test(ImageBase):
    estimates_file = os.path.join(ImageBase.datapath, '2gauss_estimates.txt')
    climage = os.path.join(ImageBase.datapath, 'simple_cl.im')
    print(estimates_file)

    def tearDown(self):
        self._myia.done()
        self.cl.done()
        data = ["1ptsource.im", "jj.cl", "jk.im", "jm.im",
                "akd.im", "simple_cl.im"]
        for f in data:
            if os.path.exists(f) and os.path.isdir(f):
                shutil.rmtree(f)

    def test_ia_fromcomplist(self):
        """Test ia.fromcomplist() functionality"""
        mycl = self.cl
        myia = self._myia
        flux = [1, 2, 3, 4]
        direction = ['J2000', '00:00:00.00', '00.00.00.0']
        pt = "point"
        mycl.addcomponent(flux=flux, dir=direction, shape=pt)

        shape = [5, 5]
        self.assertTrue(myia.fromcomplist("", shape=shape, cl=mycl.torecord()))

        shape = [5, 5, 5]
        self.assertTrue(myia.fromcomplist("", shape=shape, cl=mycl.torecord()))

        shape = [5, 5, 4, 5]
        self.assertTrue(myia.fromcomplist("", shape=shape, cl=mycl.torecord()))

        imagename = "1ptsource.im"
        self.assertTrue(myia.fromcomplist(imagename, shape=shape, cl=mycl.torecord()))
        self.assertTrue(myia.open(imagename))

    def test_vals(self):
        """Test valid pixel values"""
        mycl = self.cl
        myia = self._myia
        flux = [1, 2, 3, 4]
        direction = ['J2000', '00:00:00.00', '00.00.00.0']
        pt = "point"
        mycl.addcomponent(flux=flux, dir=direction, shape=pt)

        shape = [5, 5, 4, 1]
        self.assertTrue(myia.fromcomplist("", shape=shape, cl=mycl.torecord()))
        vals = myia.getchunk()
        for x in range(5):
            for y in range(5):
                for s in range(4):
                    if x == 2 and y == 2:
                        expec = flux[s]
                    else:
                        expec = 0
                    self.assertEqual(vals[x, y, s, 0], expec)
        mycsys = myia.coordsys()
        myia.done()

        # shuffle the stokes
        mycsys.setstokes("U V Q I")
        stokestoflux = [2, 3, 1, 0]

        self.assertTrue(
            myia.fromcomplist(
                "", shape=shape, cl=mycl.torecord(), csys=mycsys.torecord()
            )
        )
        vals = myia.getchunk()
        for x in range(5):
            for y in range(5):
                for s in range(4):
                    if x == 2 and y == 2:
                        expec = flux[stokestoflux[s]]
                    else:
                        expec = 0
                    self.assertEqual(vals[x, y, s, 0], expec)
        myia.done()
        mycl.done()
        major = "5arcmin"
        minor = "4arcmin"
        pa = "0deg"
        gauss = "Gaussian"
        mycl.addcomponent(
            flux=flux, dir=direction, majoraxis=major,
            minoraxis=minor, positionangle=pa, shape=gauss
        )
        shape = [30, 30, 4, 1]
        mycsys.setreferencepixel([15, 15, 0, 0])
        self.assertTrue(
            myia.fromcomplist(
                "", shape=shape, cl=mycl.torecord(), csys=mycsys.torecord()
            )
        )
        stats = myia.statistics(axes=[0, 1, 3])
        myia.done()
        for i in range(4):
            got = stats['sum'][i]
            expec = flux[stokestoflux[i]]
            self.assertTrue(
                np.isclose(got, expec),
                'i=' + str(i) + ' got=' + str(got) + ' expec=' + str(expec)
            )
        mycl.done()

    def test_gaussian(self):
        """Test gaussian produces correct results"""
        mycl = self.cl
        myia = self._myia
        flux = [1, 2, 3, 4]
        gauss = "Gaussian"
        major = "5arcmin"
        minor = "4arcmin"
        pa = "0deg"
        stokes = ["I", "Q", "U", "V"]
        mycsys = cs.newcoordsys(
            direction=True, spectral=True, stokes=stokes
        )
        mycsys.setreferencepixel([15, 15, 0, 0])
        shape = [30, 30, 4, 1]
        dir0 = ['J2000', '00:00:00.00', '00.00.00.0']
        dir1 = ['J2000', '00:00:10.00', '-00.04.18']
        expecra = [0, 10]
        expecdec = [0, -4.3]
        j = 0
        tol = 1e-6
        for mydir in [dir0, dir1]:
            mycl.addcomponent(
                flux=flux, dir=mydir, majoraxis=major,
                minoraxis=minor, positionangle=pa, shape=gauss
            )
            self.assertTrue(
                myia.fromcomplist(
                    "", shape=shape, cl=mycl.torecord(), csys=mycsys.torecord()
                )
            )
            mycl.done()
            i = 0
            for s in stokes:
                res = myia.fitcomponents(stokes=s)
                mycl.fromrecord(res['results'])
                gotdir = mycl.getrefdir(0)
                rainsec = self.qa.convert(gotdir['m0'], 's')['value']
                decinamin = self.qa.convert(gotdir['m1'], 'arcmin')['value']
                self.assertTrue(np.isclose(rainsec, expecra[j], tol))
                self.assertTrue(np.isclose(decinamin, expecdec[j], tol))
                self.assertEqual(gotdir['refer'], "J2000")
                self.assertTrue(np.isclose(mycl.getfluxvalue(0)[i], flux[i]))
                mycl.done()
                i += 1
            myia.done()
            mycl.done()
            j += 1

        # try two gaussians simultaneously
        for mydir in [dir0, dir1]:
            mycl.addcomponent(
                flux=flux, dir=mydir, majoraxis=major,
                minoraxis=minor, positionangle=pa, shape=gauss
            )
        self.assertEqual(mycl.length(), 2)
        self.assertTrue(
            myia.fromcomplist(
                "", shape=shape, cl=mycl.torecord(), csys=mycsys.torecord()
            )
        )
        mycl.done()
        k = 0
        atol = 1e-5
        for s in stokes:
            res = myia.fitcomponents(stokes=s, estimates=self.estimates_file)
            mycl.fromrecord(res['results'])
            self.assertEqual(mycl.length(), 2)
            for i in [0, 1]:
                gotdir = mycl.getrefdir(i)
                rainsec = self.qa.convert(gotdir['m0'], 's')['value']
                decinamin = self.qa.convert(gotdir['m1'], 'arcmin')['value']
                self.assertTrue(
                    np.isclose(rainsec, expecra[i], rtol=0, atol=atol),
                    "got: " + str(rainsec) + " expec: " + str(expecra[i])
                )
                self.assertTrue(np.isclose(decinamin, expecdec[i], rtol=0, atol=atol))
                self.assertEqual(gotdir['refer'], "J2000")
                self.assertTrue(np.isclose(mycl.getfluxvalue(0)[k], flux[k]))
            mycl.done()
            k += 1
        myia.done()

    def test_history(self):
        """verify history writing"""
        mycl = self.cl
        myia = self._myia
        flux = [1, 0, 0, 0]
        dir0 = ['J2000', '00:00:00.00', '00.00.00.0']
        pt = "point"
        mycl.addcomponent(flux=flux, dir=dir0, shape=pt)
        shape = [20, 20]
        myia.fromcomplist("", shape=shape, cl=mycl.torecord())
        mycl.done()
        msgs = myia.history()
        myia.done()
        teststr = "ia.fromcomplist"
        self.assertTrue(teststr in msgs[-2])
        self.assertTrue(teststr in msgs[-1])

    def test_multi_points_same_pixel(self):
        """Test that multiple point sources at the same pixel produce the correct result"""
        mycl = self.cl
        myia = self._myia
        flux = [1, 0, 0, 0]
        dir0 = ['J2000', '00:00:00.00', '00.00.00.0']
        pt = "point"
        mycl.addcomponent(flux=flux, dir=dir0, shape=pt)
        mycl.addcomponent(flux=flux, dir=dir0, shape=pt)
        shape = [20, 20]
        clname = "jj.cl"
        mycl.rename(clname)
        myia.fromcomplist("", shape=shape, cl=mycl.torecord())
        mycl.done()
        stats = myia.statistics()
        myia.done()
        self.assertEqual(stats['max'], 2)
        # test opening by name
        myia.fromcomplist("", shape=shape, cl=clname)
        stats = myia.statistics()
        myia.done()
        self.assertEqual(stats['max'], 2)

    def test_mask(self):
        """Test support for masks"""
        mycl = self.cl
        myia = self._myia
        flux0 = [1, 0, 0, 0]
        dir0 = ['J2000', '00:00:00.00', '00.00.00.0']
        flux1 = [2, 0, 0, 0]
        dir1 = ['J2000', '00:00:00.00', '00.05.00.0']
        pt = "point"
        mycl.addcomponent(flux=flux0, dir=dir0, shape=pt)
        mycl.addcomponent(flux=flux1, dir=dir1, shape=pt)
        shape = [20, 20]
        imagename = "jk.im"
        myia.fromcomplist(outfile=imagename, shape=shape, cl=mycl.torecord())
        mycl.done()
        stats = myia.statistics()
        self.assertEqual(stats['max'], 2)
        self.assertEqual(stats['sum'], 3)
        myia.calcmask(imagename + " > 1")
        stats = myia.statistics()
        self.assertEqual(stats['max'], 2)
        self.assertEqual(stats['sum'], 2)
        self.assertEqual(stats['npts'], 1)
        myia.calcmask(imagename + " < 2")
        myia.maskhandler("set", "mask1")
        stats = myia.statistics()
        self.assertEqual(stats['max'], 1)
        self.assertEqual(stats['sum'], 1)
        self.assertEqual(stats['npts'], 399)
        myia.maskhandler("set", "")
        stats = myia.statistics()
        self.assertEqual(stats['max'], 2)
        self.assertEqual(stats['sum'], 3)
        self.assertEqual(stats['npts'], 400)
        stats = myia.statistics(mask=imagename + " > 0")
        self.assertEqual(stats['max'], 2)
        self.assertEqual(stats['sum'], 3)
        self.assertEqual(stats['npts'], 2)

    def test_fromimage(self):
        """Test fromimage() supports reading from a componentlist image"""
        myia = self._myia
        infile = 'simple_cl.im'
        outfile = "akd.im"
        shutil.copytree(self.climage, infile)
        self.assertTrue(myia.fromimage(outfile=outfile, infile=infile))
        bb = myia.getchunk()
        myia.done()
        myia.open(outfile)
        cc = myia.getchunk()
        myia.done()
        self.assertTrue((bb == cc).all())

    def test_plp(self):
        """Test plp component"""
        mycl = self.cl
        myia = self._myia
        flux0 = [1, 0, 0, 0]
        dir0 = ['J2000', '00:00:00.00', '00.00.00.0']
        pt = "point"
        index = [1, 2, 3, 4]
        mycl.addcomponent(
            flux=flux0, dir=dir0,shape=pt, spectrumtype='plp', index=index
        )
        freq0 = mycl.getspectrum(0)['frequency']['m0']['value']
        shape = [20, 20, 20]
        imagename = "jm.im"
        myia.fromcomplist(outfile=imagename, shape=shape, cl=mycl.torecord())
        mycl.done()
        csys = myia.coordsys()
        inc = csys.increment()['numeric']
        inc[2] *= 10000
        csys.setincrement(inc)
        myia.setcoordsys(csys.torecord())
        pix = myia.getchunk()
        for chan in range(shape[2]):
            freq = myia.toworld([10, 10, chan])['numeric'][2]/1e9
            y = freq/freq0
            x = math.log(y)
            exponent = index[0] + index[1]*x + index[2]*x*x + index[3]*x*x*x
            expec = pow(y, exponent)
            self.assertTrue(
                np.isclose(pix[10, 10, chan], expec, 1e-7),
                'Incorrect pixel value'
            )
        myia.done()

# Tests for image.fromfits
class ia_fromfits_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        if self.fits:
            if os.path.isfile(self.fits):
                os.unlink(self.fits)
            else:
                shutil.rmtree(self.fits)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_history(self):
        """test writing of history"""
        myia = self._myia
        self.fits = ''
        ar1 = np.zeros([2, 3], np.float64)
        myia.fromarray("", ar1)
        self.fits = "myim.fits"
        myia.tofits(self.fits)
        myia.done()
        myia.fromfits("", self.fits)
        msgs = myia.history()
        self.assertTrue("ia.fromfits" in msgs[-2])
        self.assertTrue("ia.fromfits" in msgs[-1])

# Tests for image.fromimage
class ia_fromimage_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        if self.name:
            if os.path.isfile(self.name):
                os.unlink(self.name)
            else:
                shutil.rmtree(self.name)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_history(self):
        """test writing of history"""
        myia = self._myia
        self.name = "myim.im"
        myia.fromshape(self.name, [20, 20])
        myia.done()
        myia.fromimage("", self.name)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.fromimage" in msgs[-2])
        self.assertTrue("ia.fromimage" in msgs[-1])

# Tests for image.fromrecord
class ia_fromrecord_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_fromrecord(self):
        """Test general functionality"""
        myia = self._myia
        shape = [2, 3, 4]
        myia.fromshape("", shape)
        myia.addnoise()
        chunk = myia.getchunk()
        rec = myia.torecord()
        myia.fromrecord(rec)
        self.assertTrue((myia.shape() == shape).all(), "Shape is incorrect")
        self.assertTrue((myia.getchunk() == chunk).all(), "Pixel values are incorrect")
        a = myia.history()
        self.assertTrue(len(a) == 2, "wrong history length")
        for aa in a:
            self.assertTrue("fromrecord" in aa, "Expected string not found in history")

        # Tests for image.

# Tests for image.fromshape
class ia_fromshape_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        data = ["ttc", "ttf"]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_fromshape(self):
        """Test general functionality"""
        myia = self._myia
        shape = [2, 3, 4]
        myia.fromshape("", shape)
        self.assertTrue((myia.shape() == shape).all())
        for t in ['c', 'f']:
            myia.fromshape("", shape, type=t)
            self.assertTrue((myia.shape() == shape).all())
            outfile = "tt" + t
            myia.fromshape(outfile, shape, type=t)
            myia.done()
            myia.open(outfile)
            self.assertTrue((myia.shape() == shape).all())

    def test_history(self):
        """Test history records are written"""
        myia = self._myia
        myia.fromshape("", [10, 10])
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.fromshape" in msgs[-2])
        self.assertTrue("ia.fromshape" in msgs[-1])

    # Tests for image.

# Tests for image.getregion
class ia_getregion_test(ImageBase):

    def tearDown(self):
        if self.mymask:
            if os.path.isfile(self.mymask):
                os.unlink(self.mymask)
            else:
                shutil.rmtree(self.mymask)

    def test_stretch(self):
        """ ia.getregion(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.getregion, mask=self.mymask + ">0", stretch=False
        )
        zz = yy.getregion(
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type(yy.getchunk()))
        yy.done()

# Tests for image.hanning
class ia_hanning_test(ImageBase):
    def setUp(self):
        self.hanname = ''
        self._myia = iatool()
        self.imagename = ''
        self.mymask = ''

    def tearDown(self):
        self._myia.done()
        data = [self.mymask, self.imagename, self.hanname]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_stretch(self):
        """ ia.hanning(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.hanning, mask=self.mymask + ">0", stretch=False
        )
        zz = yy.hanning(
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_regression(self):
        """Tests moved from imagetest regression"""
        # Make image
        self.imagename = 'ia.fromshape.image'
        myia = self._myia
        imshape = [10, 20]
        myim = myia.newimagefromshape(outfile=self.imagename, shape=imshape)
        self.assertTrue(myim)
        pixels = myim.getchunk()
        self.assertTrue(len(pixels) > 0)
        for i in range(pixels.shape[0]):
            for j in range(pixels.shape[1]):
                if pixels[i][j] > -10000:
                    pixels[i][j] = 1
        self.assertTrue(myim.putchunk(pixels))
        self.assertRaises(Exception, myim.hanning, axis=19)
        self.hanname = 'hanning.image'
        myim2 = myim.hanning(outfile=self.hanname, axis=0, drop=False)
        self.assertTrue(myim2)
        pixels2 = myim2.getchunk()
        self.assertFalse(len(pixels2) == 0)
        self.assertTrue((pixels2 == 1).all())
        self.assertTrue(myim2.remove(done=True))
        myim2 = myim.hanning(outfile=self.hanname, axis=0, drop=True)
        self.assertTrue(myim2)
        shape2 = [myim.shape()[0] / 2 - 1, myim.shape()[1]]
        self.assertTrue((myim2.shape() == shape2).all())
        pixels2 = myim2.getchunk()
        self.assertFalse(len(pixels2) == 0)
        self.assertTrue((pixels2 == 1).all())
        self.assertTrue(myim2.remove(done=True))
        pixels = myim.getregion()
        mask = myim.getregion(getmask=True)
        mask[0, 0] = False
        mask[1, 0] = False
        mask[2, 0] = False
        mask[3, 0] = False
        self.assertTrue(myim.putregion(pixelmask=mask))
        myim2 = myim.hanning(outfile=self.hanname, axis=0, drop=False)
        self.assertTrue(myim2)
        pixels2 = myim2.getregion()
        mask2 = myim2.getregion(getmask=True)
        self.assertTrue(mask2[0, 0] == False and mask2[1, 0] == False)
        self.assertFalse(mask2[2, 0])
        self.assertFalse(mask2[3, 0])
        self.assertTrue(pixels2[0, 0] == 0 and pixels2[1, 0] == 0)
        self.assertTrue(pixels2[2, 0] == 0)
        self.assertTrue(pixels2[3, 0] == 0.25)

        self.assertTrue(myim2.done())

        self.assertTrue(myim.done())

    def test_general(self):
        """Test general behavior"""
        myia = iatool()
        length = 6
        self.imagename = "test_gen.im"
        myia.fromshape(self.imagename, [1, 1, length])
        bb = myia.getchunk()
        for i in range(length):
            bb[0, 0, i] = i * i + 1
        myia.putchunk(bb)
        rg = regionmanager()
        for i in range(length):
            reg = rg.box([0, 0, 0], [0, 0, i])
            outfile = "out" + str(i) + ".im"
            if (i < 2):
                self.assertRaises(Exception, myia.hanning, region=reg, axis=2)
            else:
                for drop in (False, True):
                    outfile = "out" + str(i) + str(drop) + ".im"
                    if drop:
                        for dmethod in ("c", "m"):
                            outfile = "out" + str(i) + str(drop) + dmethod + ".im"
                            if i == 2 or i == 3:
                                if dmethod == "c":
                                    expec = [2.5]
                                else:
                                    if i == 2:
                                        expec = [3.0]
                                    elif i == 3:
                                        expec = [4.0]
                            elif i == 4 or i == 5:
                                if dmethod == "c":
                                    expec = [2.5, 10.5]
                                else:
                                    if i == 4:
                                        expec = [4.0, 12.0]
                                    if i == 5:
                                        expec = [4.0, 14.0]
                            han = myia.hanning(
                                region=reg, axis=2, drop=drop, dmethod=dmethod
                            )
                            got = han.getchunk().ravel()
                            self.assertTrue((got == expec).all())
                            han.done()
                    else:
                        dmethod = "c"
                        if i == 2:
                            expec = [1.5, 2.5, 3.5]
                        elif i == 3:
                            expec = [1.5, 2.5, 5.5, 7.5]
                        elif i == 4:
                            expec = [1.5, 2.5, 5.5, 10.5, 13.5]
                        elif i == 5:
                            expec = [1.5, 2.5, 5.5, 10.5, 17.5, 21.5]
                        han = myia.hanning(
                            region=reg, axis=2, drop=drop, dmethod=dmethod
                        )
                        got = han.getchunk().ravel()
                        self.assertTrue((got == expec).all())
                        han.done()
        rg.done()
        myia.done()

    def test_history(self):
        """Test history records are written"""
        myia = iatool()
        self.imagename = "zz.im"
        myia.fromshape(self.imagename, [20, 20, 20])
        bb = myia.hanning()
        myia.done()
        msgs = bb.history()
        bb.done()
        teststr = "ia.hanning"
        self.assertTrue(teststr in msgs[-4], "'" + teststr + "' not found")
        self.assertTrue(teststr in msgs[-3], "'" + teststr + "' not found")

# Tests for image.histograms
class ia_histograms_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        data = [self.mymask, self.imagename]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def alleqnum(self, x, num, tolerance=0):
        if len(x.shape) == 1:
            for i in range(x.shape[0]):
                if not (abs(x[i] - num) < tolerance):
                    print("x[%d]=%f" % (i, x[i]))
                    return False
        if len(x.shape) == 2:
            for i in range(x.shape[0]):
                for j in range(x.shape[1]):
                    if not (abs(x[i][j] - num) < tolerance):
                        print("x[%d][%d]=%f" % (i, j, x[i][j]))
                        return False
        if len(x.shape) == 3:
            for i in range(x.shape[0]):
                for j in range(x.shape[1]):
                    for k in range(x.shape[2]):
                        if not (abs(x[i][j][k] - num) < tolerance):
                            print("x[%d][%d][%d]=%f" % (i, j, k, x[i][j][k]))
                            return False
        if len(x.shape) == 4:
            for i in range(x.shape[0]):
                for j in range(x.shape[1]):
                    for k in range(x.shape[2]):
                        for l in range(x.shape[3]):
                            if not (abs(x[i][j][k][l] - num) < tolerance):
                                print("x[%d][%d][%d][%d]=%f" % (i, j, k, l, x[i][j][k]))
                                return False
        if len(x.shape) > 4:
            stop('unhandled array shape in alleq')

        return True

    def test_general(self):
        """general tests"""
        # moved from imagetest_regression.py
        myia = self._myia
        imshape = [5, 10]
        pixels = myia.makearray(0.0, imshape)
        pixels[0, 0] = -100
        pixels[imshape[0] - 1, imshape[1] - 1] = 100
        self.imagename = 'ia.fromarray.image'
        myim = myia.newimagefromarray(outfile=self.imagename, pixels=pixels)
        self.assertTrue(myim)
        try:
            ok = myim.histograms(axes=[9, 19])
        except Exception as e:
            print('Caught expected Exception %s' % str(e))
            ok = False
        self.assertFalse(ok, 'Histograms unexpectedly did not fail (1)')

        nbins = 25
        idx = nbins // 2 + 1
        out = myim.histograms(nbins=nbins)
        self.assertTrue(out, 'Histograms failed (1)')
        hists = out
        self.assertTrue(
            'values' in hists and 'counts' in hists,
            'Histograms record does not have the correct fields'
        )
        self.assertTrue(
            len(hists['values']) == nbins and len(hists['counts']) == nbins
            , 'Histograms value arrays have the wrong shape (1)'
        )
        ok = hists['counts'][0] == 1 and hists['counts'][nbins - 1] == 1
        ok = ok and (hists['counts'][idx - 1] == (imshape[0] * imshape[1] - 2))
        self.assertTrue(ok, 'histogram counts wrong')

        blc = [0, 0];
        trc = [4, 4]
        rg = regionmanager()
        r1 = rg.box(blc=blc, trc=trc)
        rg.done()
        hists = myim.histograms(nbins=nbins, region=r1)
        self.assertTrue(hists, 'Histograms failed (2)')
        ok = (hists['counts'][0] == 1) and (
                    hists['counts'][nbins - 1] == ((trc[0] - blc[0] + 1) * (trc[1] - blc[1] + 1) - 1))
        self.assertTrue(ok, 'Histograms values are wrong (2)')

        for j in range(imshape[1]):
            pixels[0, j] = -100 * (j + 1)
            pixels[imshape[0] - 1, j] = 100 * (j + 1)
        ok = myim.putchunk(pixels)
        self.assertTrue(ok, 'putchunk failed (1)')
        hists = myim.histograms(nbins=nbins, axes=[0])
        self.assertTrue(hists, 'Histograms failed (3)')
        ok = list(hists['values'].shape) == [nbins, imshape[1]]
        ok = ok and list(hists['counts'].shape) == [nbins, imshape[1]]
        self.assertTrue(ok, 'Histograms value arrays have the wrong shape (2)')
        for j in range(imshape[1]):
            ok = hists['counts'][0, j] == 1 and hists['counts'][nbins - 1, j] == 1
            ok = ok and self.alleqnum(hists['counts'][idx - 1], (imshape[0] - 2), tolerance=0.0001)
        self.assertTrue(ok, 'Histograms values are wrong (3)')

        hists = myim.histograms(includepix=[-5, 5], nbins=25)
        self.assertTrue(hists, 'Histograms failed (4)')
        ok = hists['counts'][idx - 1] == (imshape[0] * imshape[1] - (imshape[1] + imshape[1]))
        self.assertTrue(ok, 'Histograms values are wrong (4)')
        ok = ok and self.alleqnum(hists['counts'][0:(idx - 2)], 0, tolerance=0.0001)
        self.assertTrue(ok, 'Histograms values are wrong (4)')
        ok = ok and self.alleqnum(hists['counts'][idx:nbins], 0, tolerance=0.0001)
        self.assertTrue(ok, 'Histograms values are wrong (4)')

        hists = myim.histograms()
        self.assertTrue(hists, 'histograms failed (4)')
        hists = myim.histograms()
        self.assertTrue(hists, 'histograms failed (5)')
        hists = myim.histograms(cumu=True, log=True)
        self.assertTrue(hists, 'histograms failed (6)')

        ok = myim.done()
        self.assertTrue(ok, 'Done failed (1)')

    def test_stretch(self):
        """ ia.histogram(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.histograms, mask=self.mymask + ">0", stretch=False
        )
        zz = yy.histograms(
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(zz and type(zz) == type({}))
        yy.done()

    def test_stats(self):
        """ test returned stats"""
        myia = iatool()
        myia.open(self.datapath + "myim.im")
        res = myia.histograms()
        self.assertTrue(abs(res['mean'][0] - -0.01586026) < 1e-7)
        self.assertTrue(abs(res['sigma'][0] - 0.99244785) < 1e-7)
        res = myia.histograms(axes=[0, 1])
        bb = myia.getchunk()
        for i in range(20):
            self.assertTrue(abs(res['mean'][i] - np.mean(bb[:, :, i])) < 1e-7)
            self.assertTrue(abs(res['sigma'][i] - np.std(bb[:, :, i], ddof=1)) < 1e-7)
        myia.done()

# Tests for image.imageconcat
class ia_imageconcat_test(ImageBase):

    def make_images(self):
        myia = self._myia
        myia.fromshape("", [1, 1, 5])
        names = []
        rg = self.rg
        for i in range(5):
            name = "chan_" + str(i)
            names.append(name)
            subi = myia.subimage(
                name, region=rg.box([0, 0, i], [0, 0, i]), overwrite=True
            )
            got = subi.toworld([0, 0, 0])['numeric'][2]
            expec = myia.toworld([0, 0, i])['numeric'][2]
            self.assertTrue(got == expec)
            subi.done()
        return (names, myia)


    def tearDown(self):
        self._myia.done()
        data = [
            "c0_cd.im", "c0_c.im", "c0_d.im", "c0_f.im",
            "c1_cd.im", "c1_c.im", "c1_d.im", "c1_f.im",
            "chan_2", "hist1.im", "hist2.im", "image1.im", "image2.im",
            "loop1_c.im", "loop1_m.im", "loop1_n.im", "loop1_p.im",
            "loop2_c.im", "loop2_m.im", "loop2_n.im", "loop2_p.im",
            "loop3_c.im", "loop3_m.im", "loop3_n.im", "loop3_p.im",
            "reorder_0", "reorder_1", "reorder_2", "reorder_3", "reorder_4"
        ]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def test_multibeam(self):
        """Test concatenating images with different beams"""
        myia = self._myia
        shape = [4, 4, 20]
        myia.fromshape("", shape)
        blc1 = [0, 0, 0]
        trc1 = [shape[0] - 1, shape[1] - 1, shape[2] / 2 - 1]
        rg1 = self.rg.box(blc=blc1, trc=trc1)
        im1 = "image1.im"
        sub1 = myia.subimage(im1, region=rg1)
        im2 = "image2.im"
        blc2 = [0, 0, trc1[2] + 1]
        trc2 = [shape[0] - 1, shape[1] - 1, shape[2] - 1]
        rg2 = self.rg.box(blc=blc2, trc=trc2)
        sub2 = myia.subimage(im2, region=rg2)
        major = self.qa.quantity("3arcmin")
        minor = self.qa.quantity("2arcmin")
        pa = self.qa.quantity("0deg")
        major2 = self.qa.quantity("4arcmin")
        minor2 = self.qa.quantity("3arcmin")
        pa2 = self.qa.quantity("10deg")
        major3 = self.qa.quantity("5arcmin")
        minor3 = self.qa.quantity("4arcmin")
        pa3 = self.qa.quantity("20deg")

        # first image has no beam while second does
        sub1.setbrightnessunit("Jy/pixel")
        sub2.setrestoringbeam(major=major, minor=minor, pa=pa)
        sub2.setbrightnessunit("Jy/beam")
        self.assertRaises(Exception, myia.imageconcat, "", [im1, im2])
        concat = myia.imageconcat("", [im1, im2], relax=True)
        self.assertTrue((concat.shape() == shape).all())

        # first image has a single beam, second has per plane beams
        sub1.setbrightnessunit("Jy/beam")
        sub1.setrestoringbeam(major=major, minor=minor, pa=pa)
        sub2.setbrightnessunit("Jy/beam")
        sub2.setrestoringbeam(remove=True)
        sub2.setrestoringbeam(
            major=major2, minor=minor2, pa=pa2,
            channel=0, polarization=-1
        )

        sub2.setrestoringbeam(
            major=major3, minor=minor3, pa=pa3,
            channel=5, polarization=-1
        )
        concat = myia.imageconcat("", [im1, im2])
        for i in range(concat.shape()[2]):
            beam = concat.restoringbeam(channel=i)
            if i < sub1.shape()[2]:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa))
            elif i == sub1.shape()[2] + 5:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major3))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor3))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa3))
            else:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major2))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor2))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa2))

        # both images have a single beam which is the same
        sub1.setbrightnessunit("Jy/beam")
        sub1.setrestoringbeam(major=major, minor=minor, pa=pa)
        sub2.setrestoringbeam(remove=True)
        sub2.setbrightnessunit("Jy/beam")
        sub2.setrestoringbeam(major=major, minor=minor, pa=pa)
        concat = myia.imageconcat("", [im1, im2])
        beam = concat.restoringbeam()
        self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major))
        self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor))
        self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa))

        # both images have single, unequal beams
        sub1.setbrightnessunit("Jy/beam")
        sub1.setrestoringbeam(major=major, minor=minor, pa=pa)
        sub2.setrestoringbeam(remove=True)
        sub2.setbrightnessunit("Jy/beam")
        sub2.setrestoringbeam(major=major2, minor=minor2, pa=pa2)
        concat = myia.imageconcat("", [im1, im2])
        for i in range(concat.shape()[2]):
            beam = concat.restoringbeam(channel=i)
            if i < sub1.shape()[2]:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa))
            else:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major2))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor2))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa2))

        # first image has per plane beams, second has single beam
        sub2.setbrightnessunit("Jy/beam")
        sub2.setrestoringbeam(remove=True)
        sub2.setrestoringbeam(major=major, minor=minor, pa=pa)
        sub1.setbrightnessunit("Jy/beam")
        sub1.setrestoringbeam(remove=True)
        sub1.setrestoringbeam(
            major=major2, minor=minor2, pa=pa2,
            channel=0, polarization=-1
        )
        sub1.setrestoringbeam(
            major=major3, minor=minor3, pa=pa3,
            channel=5, polarization=-1
        )
        concat = myia.imageconcat("", [im1, im2])
        for i in range(concat.shape()[2]):
            beam = concat.restoringbeam(channel=i)
            if i == 5:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major3))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor3))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa3))
            elif i < sub1.shape()[2]:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major2))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor2))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa2))
            else:
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor))
                self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa))

        # both images have a single beam which is the same
        sub1.setbrightnessunit("Jy/beam")
        sub1.setrestoringbeam(remove=True)
        sub1.setrestoringbeam(major=major, minor=minor, pa=pa)
        sub2.setrestoringbeam(remove=True)
        sub2.setbrightnessunit("Jy/beam")
        sub2.setrestoringbeam(major=major, minor=minor, pa=pa)
        concat = myia.imageconcat("", [im1, im2])
        beam = concat.restoringbeam()
        self.assertTrue(self.qa.eq(self.qa.quantity(beam["major"]), major))
        self.assertTrue(self.qa.eq(self.qa.quantity(beam["minor"]), minor))
        self.assertTrue(self.qa.eq(self.qa.quantity(beam["positionangle"]), pa))

    def test_basic(self):
        """Test basic functionality"""
        myia = self._myia
        myia.fromshape("", [1, 1, 5])
        names = []
        for i in range(5):
            name = "chan_" + str(i)
            names.append(name)
            subi = myia.subimage(name, region=self.rg.box([0, 0, i], [0, 0, i]))
            got = subi.toworld([0, 0, 0])['numeric'][2]
            expec = myia.toworld([0, 0, i])['numeric'][2]
            self.assertTrue(got == expec)
            subi.done()
        concat = myia.imageconcat(infiles=[names[0], names[1], names[2]])
        for i in range(3):
            got = concat.toworld([0, 0, i])['numeric'][2]
            expec = myia.toworld([0, 0, i])['numeric'][2]
            self.assertTrue(got == expec)

        self.assertRaises(
            Exception, myia.imageconcat, outfile="blah.im", infiles=[names[0], names[1], names[3]]
        )
        concat = myia.imageconcat(
            infiles=[names[0], names[1], names[3]], relax=True
        )
        for i in range(3):
            k = i
            if i == 2: k = 3
            got = concat.toworld([0, 0, i])['numeric'][2]
            expec = myia.toworld([0, 0, k])['numeric'][2]
            self.assertTrue(got == expec)

        concat = myia.imageconcat(
            infiles=[names[0], names[1], names[3], names[4]],
            relax=True
        )
        for i in range(4):
            k = i
            if i >= 2: k = i + 1
            got = concat.toworld([0, 0, i])['numeric'][2]
            expec = myia.toworld([0, 0, k])['numeric'][2]
            self.assertTrue(got == expec)
        concat.done()
        myia.done()

    def test_reorder(self):
        """Test reorder param functionality"""
        myia = self._myia
        myia.fromshape("", [1, 1, 5])
        names = []
        for i in range(5):
            name = "reorder_" + str(i)
            names.append(name)
            subi = myia.subimage(name, region=self.rg.box([0, 0, i], [0, 0, i]))
            got = subi.toworld([0, 0, 0])['numeric'][2]
            expec = myia.toworld([0, 0, i])['numeric'][2]
            self.assertTrue(got == expec)
            bb = subi.getchunk()
            bb[:] = i
            subi.putchunk(bb)
            subi.done()

        concat = myia.imageconcat(
            infiles=[names[1], names[3], names[2]], reorder=True
        )
        for i in range(3):
            myia.open(names[i + 1])
            got = concat.toworld([0, 0, i])['numeric'][2]
            expec = myia.toworld([0, 0, 0])['numeric'][2]
            self.assertTrue(got == expec)
            self.assertTrue(
                concat.getchunk()[0, 0, i] == myia.getchunk()[0, 0, 0]
            )
        myia.done()

    def test_history(self):
        """Test history writing"""
        im1 = "hist1.im"
        im2 = "hist2.im"
        myia = self._myia
        myia.fromshape(im1, [10, 10, 5])
        myia.fromshape(im2, [10, 10, 5])
        myia.done()
        zz = myia.imageconcat("", [im1, im2], relax=True)
        msgs = zz.history()
        self.assertTrue("ia.imageconcat" in msgs[-1])
        self.assertTrue("ia.imageconcat" in msgs[-2])

    def test_precision(self):
        """Test different image precisions"""
        myia = self._myia
        shape = [4, 4, 5]
        expec = {}
        expec['f'] = 'float'
        expec['c'] = 'complex'
        expec['d'] = 'double'
        expec['cd'] = 'dcomplex'
        for mytype in ['f', 'c', 'd', 'cd']:
            out0 = "c0_" + mytype + ".im"
            myia.fromshape(out0, shape, type=mytype)
            out1 = "c1_" + mytype + ".im"
            myia.fromshape(out1, shape, type=mytype)
            myia.done()
            concat = myia.imageconcat("", [out0, out1], relax=True, axis=2)
            myia.done()
            self.assertTrue(
                concat.pixeltype() == expec[mytype], "wrong type for " + mytype
            )
            concat.done()

    def test_mode(self):
        """Test various output formats CAS-12600"""
        # 'm' has to be the last one...
        modes = ['c', 'n', 'p', 'm']
        (names, myia) = self.make_images()
        for mode in modes:
            outname = "loop1_" + mode + ".im"
            concat = myia.imageconcat(outname, infiles=names, mode=mode)
            for i in range(3):
                got = concat.toworld([0, 0, i])['numeric'][2]
                expec = myia.toworld([0, 0, i])['numeric'][2]
                self.assertTrue(got == expec)
        (names, myia) = self.make_images()
        for mode in modes:
            outname = "loop2_" + mode + ".im"
            concat = myia.imageconcat(
                outname, infiles=[names[0], names[1], names[3]], relax=True, mode=mode
            )
            for i in range(3):
                k = i
                if i == 2: k = 3
                got = concat.toworld([0, 0, i])['numeric'][2]
                expec = myia.toworld([0, 0, k])['numeric'][2]
                self.assertTrue(got == expec)
        (names, myia) = self.make_images()
        for mode in modes:
            outname = "loop3_" + mode + ".im"
            concat = myia.imageconcat(
                outname, infiles=[names[0], names[1], names[3],
                                  names[4]], relax=True, mode=mode
            )
            for i in range(4):
                k = i
                if i >= 2: k = i + 1
                got = concat.toworld([0, 0, i])['numeric'][2]
                expec = myia.toworld([0, 0, k])['numeric'][2]
                self.assertTrue(got == expec)
        concat.done()
        myia.done()

# Tests for image.insert
class ia_insert_test(ImageBase):

    def tearDown(self):
        if self.insert:
            if os.path.isfile(self.insert):
                os.unlink(self.insert)
            else:
                shutil.rmtree(self.insert)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_insert(self):
        """ ia.insert(): Test insert()"""
        myia = self._myia
        self.insert = "zxye.im"
        myia.fromshape(self.insert, [10, 10, 10])
        myia.set(10)
        myia.done()
        myia.fromshape("", [20, 20, 20])
        myia.set(20)
        stats = myia.statistics()
        self.assertTrue(stats["max"] == 20)
        self.assertTrue(stats["min"] == 20)
        self.assertTrue(myia.insert(infile=self.insert))
        bb = myia.getchunk()
        self.assertTrue(bb[0, 0, 0] == 20)
        self.assertTrue(bb[10, 10, 10] == 10)
        # ensure the stats were reset
        stats = myia.statistics()
        self.assertTrue(stats["max"] == 20)
        self.assertTrue(stats["min"] == 10)

    def test_history(self):
        """Verify ia.insert writes history to image"""
        myia = self._myia
        self.insert = "hist_zxye.im"
        myia.fromshape(self.insert, [10, 10, 10])
        myia.set(10)
        myia.done()
        myia.fromshape("", [20, 20, 20])
        myia.set(20)
        self.assertTrue(myia.insert(infile=self.insert))
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.insert" in msgs[-2])
        self.assertTrue("ia.insert" in msgs[-1])

# Tests for image.isconform
class ia_isconform_test(ImageBase):

    def setUp(self):
        self.fits = "jj.fits"
        shutil.copy(self.datapath + self.fits, self.fits)
        self._myia = iatool()
        self._myia.maketestimage()

    def tearDown(self):
        self._myia.done()
        del self._myia
        if self.fits:
            if os.path.isfile(self.fits):
                os.unlink(self.fits)
            else:
                shutil.rmtree(self.fits)

    def test_unattached(self):
        self._myia.done()
        self.assertRaises(Exception, self._myia.isconform("x"))

    def test_trueness(self):
        self.assertTrue(self._myia.isconform(self.fits))

    def test_diffaxes(self):
        _newia = self._myia.adddegaxes(spectral=True)
        self.assertFalse(_newia.isconform(self.fits))

    def test_diffaxes_cs(self):
        _cs = self._myia.coordsys()
        names = _cs.names()
        _cs.setnames([names[1], names[0]])
        self._myia.setcoordsys(_cs.torecord())
        self.assertFalse(self._myia.isconform(self.fits))

    def test_diffincrements(self):
        _cs = self._myia.coordsys()
        _cs.setincrement([0.1, 0.1])
        self._myia.setcoordsys(_cs.torecord())
        self.assertFalse(self._myia.isconform(self.fits))

# Tests for image.makecomplex
class ia_makecomplex_test(ImageBase):

    def tearDown(self):
        data = [self.im2, self.im1]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_history(self):
        """Verify ia.makecomplex() writes history to image"""
        myia = self._myia
        self.im2 = "hist_zxye.im"
        shape = [20, 20]
        myia.fromshape(self.im2, shape)
        myia.set(10)
        myia.done()
        myia.fromshape("", shape)
        myia.set(20)
        self.im1 = "myc.im"
        self.assertTrue(myia.makecomplex(self.im1, imag=self.im2))
        myia.done()
        myia.open(self.im1)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.makecomplex" in msgs[-2])
        self.assertTrue("ia.makecomplex" in msgs[-1])

# Tests for image.maskhandler
class ia_maskhandler_test(ImageBase):

    def tearDown(self):
        if self.im1:
            if os.path.isfile(self.im1):
                os.unlink(self.im1)
            else:
                shutil.rmtree(self.im1)
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_history(self):
        """Verify ia.insert writes history to image"""
        myia = self._myia
        self.im1 = "hist_zxye.im"
        shape = [20, 20]
        myia.fromshape(self.im1, shape)
        myia.calcmask(self.im1 + ">0")
        myia.maskhandler("rename", ["mask0", "blahmask"])
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.maskhandler" in msgs[-2])
        self.assertTrue("ia.maskhandler" in msgs[-1])

    # Tests for image.

# Tests for image.modify
class ia_modify_test(ImageBase):

    def tearDown(self):
        self.qa.done()
        data = ["CAS5688_1.im", "CAS5688_2.im", self.mymask]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)
        pass

    def test_stretch(self):
        """ ia.histogram(): Test stretch parameter"""
        mycl = cltool()
        mycl.addcomponent(flux=1, dir=['J2000', '00:00:00.00', '00.00.00.0'])
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.modify, model=mycl.torecord(),
            mask=self.mymask + ">0", stretch=False
        )
        zz = yy.modify(
            model=mycl.torecord(), mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(zz and type(zz) == type(True))
        yy.done()
        mycl.done()

    def test_CAS5688(self):
        """verify output is the same after this performance fix"""
        self.me = measures()
        myia = iatool()
        for i in [0, 1]:
            if i == 0:
                imagename = "CAS5688_1.im"
            elif i == 1:
                imagename = "CAS5688_2.im"
            myia.fromshape(imagename, [20, 20, 1, 10])
            if i == 0:
                world = myia.toworld([4.4, 4.4])['numeric']
            elif i == 1:
                world = myia.toworld([4.51, 4.51])['numeric']
            myia.setbrightnessunit("Jy/pixel")
            v0 = self.qa.quantity(world[0], "arcmin")
            v1 = self.qa.quantity(world[1], "arcmin")
            dir = self.me.direction("J2000", v0, v1)
            self.me.done()

            mycl = cltool()
            mycl.addcomponent(
                [1, 0, 0, 0], "Jy", dir=dir, shape="point",
                polarization="Stokes", spectrumtype="spectral index", index=2.5
            )
            myia.modify(model=mycl.torecord(), subtract=False)
            bb = myia.getchunk()
            myia.done()
            mycl.done()
            myia.open(self.datapath + os.sep + imagename)
            cc = myia.getchunk()
            myia.done()
            self.assertTrue((bb == cc).all())

    def test_history(self):
        """Test history is added"""
        mycl = cltool()
        mycl.addcomponent(flux=1, dir=['J2000', '00:00:00.00', '00.00.00.0'])
        myia = iatool()
        myia.fromshape("", [200, 200, 1, 1])
        self.assertTrue(
            myia.modify(model=mycl.torecord()), "Failed to run ia.modify"
        )
        msgs = myia.history()
        mycl.done()
        myia.done()
        self.assertTrue("ia.modify" in msgs[-2], "History not written")
        self.assertTrue("ia.modify" in msgs[-1], "History not written")

    def test_disk(self):
        """test disk gives the right flux, CAS-10887"""
        mycl = cltool()
        mycl.addcomponent(
            dir="J2000 0:00:00 0.00.00", flux=1.0, shape="disk", majoraxis="100arcmin",
            minoraxis="100arcmin", positionangle="0deg"
        )
        myia = iatool()
        myia.fromshape("", [101, 101])
        myia.modify(mycl.torecord(), subtract=False)
        mycl.done()
        self.assertTrue(np.isclose(myia.statistics()['sum'][0], 1, 1e-2))
        myia.done()

# Tests for image.newfromimage
class ia_newimagefromimage_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        if self.mymask:
            if os.path.isfile(self.mymask):
                os.unlink(self.mymask)
            else:
                shutil.rmtree(self.mymask)

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        self.mymask = "zz"
        myia.fromshape(self.mymask, [20, 20])
        myia = myia.newimagefromimage(self.mymask)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.newimagefromimage" in msgs[-2])
        self.assertTrue("ia.newimagefromimage" in msgs[-1])

# Tests for image.pad
class ia_pad_test(ImageBase):


    def tearDown(self):
        data = [self.mymask, self.imagename]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)


    def test_pad(self):
        """ ia.pad(): Test pad()"""
        myia = self._myia
        shape = [10, 10, 10]
        myia.fromshape(shape=shape)
        chunk = myia.getchunk()
        chunk[:, :, :] = 1
        myia.putchunk(chunk)
        for npix in [5, 7]:
            pad = myia.pad(npixels=npix)
            got = pad.shape()
            # make a copy, not a reference
            expshape = shape[:]
            expshape[0] += 2 * npix
            expshape[1] += 2 * npix
            # did we get the correct shape
            self.assertTrue((got == expshape).all())
            # did we actually copy the source pixel values
            exp = myia.statistics()['sum']
            got = pad.statistics()['sum']
            self.assertTrue((got == exp).all())
            # test that padding pixels are masked
            got = pad.statistics()['npts']
            exp = myia.statistics()['npts']
            self.assertTrue((got == exp).all())
            # coordinate system consistency checks
            exp = myia.coordsys().referencepixel()['numeric'] + [npix, npix, 0]
            got = pad.coordsys().referencepixel()['numeric']
            self.assertTrue((abs(got - exp) < 1e-8).all())
            exp = myia.toworld([0, 0, 0])['numeric']
            got = pad.toworld([npix, npix, 0])['numeric']

            self.assertTrue((abs(got - exp) < 1e-8).all())

            # checks for not masking pixels
            pad = myia.pad(npixels=npix, padmask=True)
            got = pad.shape()
            self.assertTrue((got == expshape).all())
            # test that padding pixels are not masked
            got = pad.statistics()['npts']
            exp = np.prod(pad.shape())
            self.assertTrue(got[0] == exp)
            # coordinate system consistency checks
            exp = myia.coordsys().referencepixel()['numeric'] + [npix, npix, 0]
            got = pad.coordsys().referencepixel()['numeric']
            self.assertTrue((abs(got - exp) < 1e-8).all())
            exp = myia.toworld([0, 0, 0])['numeric']
            got = pad.toworld([npix, npix, 0])['numeric']

            # checks for not masking pixels and setting to value
            pad = myia.pad(npixels=npix, padmask=True, value=1)
            got = pad.shape()
            self.assertTrue((got == expshape).all())
            # test that padding pixels are not masked
            got = pad.statistics()['npts']
            exp = np.prod(pad.shape())
            self.assertTrue(got[0] == exp)
            # test that padding pixels are set to value
            got = pad.statistics()['sum'][0]
            exp = myia.statistics()['sum'] + (np.prod(pad.shape()) - np.prod(shape))
            # coordinate system consistency checks
            exp = myia.coordsys().referencepixel()['numeric'] + [npix, npix, 0]
            got = pad.coordsys().referencepixel()['numeric']
            self.assertTrue((abs(got - exp) < 1e-8).all())
            exp = myia.toworld([0, 0, 0])['numeric']
            got = pad.toworld([npix, npix, 0])['numeric']

    def test_stretch(self):
        """ ia.pad(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.pad, npixels=1,
            mask=self.mymask + ">0", stretch=False
        )
        zz = yy.pad(
            npixels=1, mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(zz and type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_mask(self):
        """Test that mask is preserved"""

        def _check():
            data = pad.getchunk()
            self.assertTrue((data[0, :] == 0).all())
            self.assertTrue((data[:, 0] == 0).all())
            self.assertTrue((data[padsize - 1, :] == 0).all())
            self.assertTrue((data[:, padsize - 1] == 0).all())
            self.assertTrue((data[1:padsize - 1, 1:padsize - 1] == myia.getchunk()).all())
            mask = pad.getchunk(getmask=True)
            self.assertTrue((mask[0, :] == False).all())
            self.assertTrue((mask[:, 0] == False).all())
            self.assertTrue((mask[padsize - 1, :] == False).all())
            self.assertTrue((mask[:, padsize - 1] == False).all())
            self.assertTrue((mask[1:padsize - 1, 1:padsize - 1] == expec).all())

        myia = self._myia
        self.imagename = "xyz.im"
        n = 20
        myia.fromshape(self.imagename, [n, n])
        myia.addnoise()
        npix = 1
        padsize = n + 2 * npix
        pad = myia.pad(npixels=npix)
        expec = myia.getchunk(getmask=True)
        _check()

        pad = myia.pad(npixels=npix, mask=self.imagename + ">0")
        expec = myia.getchunk() > 0
        _check()

        # give the image a pixel mask
        myia.calcmask(self.imagename + "<0")
        pad = myia.pad(npixels=npix)
        expec = myia.getchunk(getmask=True)
        _check()

        # pixel mask + region defined by using an OTF mask
        pad = myia.pad(npixels=npix, mask=self.imagename + "<0.5")
        expec = np.logical_and(myia.getchunk(getmask=True), myia.getchunk() < 0.5)
        _check()

        pad.done()
        myia.done()

    def test_history(self):
        """Verify history writing"""
        myia = iatool()
        myia.fromshape("", [20, 20])
        bb = myia.pad()
        myia.done()
        msgs = bb.history()
        bb.done()
        self.assertTrue("ia.pad" in msgs[-4])
        self.assertTrue("ia.pad" in msgs[-3])

# Tests for image.putchunk and image.getchunk
class ia_putchunk_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        self.assertTrue(len(self.tb.showcache()) == 0)
        self.tb.done()

    def test_fromshape(self):
        """Test general functionality"""
        myia = self._myia
        shape = [2, 3, 4]
        fval = 2.7
        cval = 8.6 - 5.4j

        # complex valued image
        myia.fromshape("", shape, type='c')
        bb = myia.getchunk()
        bb[:] = cval
        myia.putchunk(bb)
        self.assertTrue((abs(abs(myia.getchunk()) - abs(cval)) < 1e-6).all())
        bb[:] = fval
        myia.putchunk(bb)
        self.assertTrue((abs(abs(myia.getchunk()) - abs(fval)) < 1e-6).all())

        # float valued image
        myia.fromshape("", shape, type='f')
        cc = myia.getchunk()
        cc[:] = fval
        myia.putchunk(cc)
        self.assertTrue((abs(myia.getchunk() - fval) < 1e-6).all())
        # can't put a complex valued array in a float valued image
        self.assertRaises(Exception, myia.putchunk, bb)

    def test_history(self):
        """Verify history is written"""
        myia = self._myia
        myia.fromshape("", [20, 20])
        bb = myia.getchunk()
        bb[:] = 5
        myia.putchunk(bb)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.putchunk" in msgs[-2])
        self.assertTrue("ia.putchunk" in msgs[-1])

# Tests for image.putregion
class ia_putregion_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        tb = table()
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done()

    def test_history(self):
        """Verify history is written"""
        myia = self._myia
        myia.fromshape("", [20, 20])
        bb = myia.getchunk()
        bb[:] = 5
        myia.putregion(bb)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.putregion" in msgs[-2])
        self.assertTrue("ia.putregion" in msgs[-1])

# Tests for image.rename
class ia_rename_test(ImageBase):

    def tearDown(self):
        if self.newname:
            if os.path.isfile(self.newname):
                os.unlink(self.newname)
            else:
                shutil.rmtree(self.newname)
        self.assertTrue(len(self.tb.showcache()) == 0)

    def test_rename(self):
        """verify history writing"""
        myia = self._myia
        myia.fromshape("zz", [20, 20])
        self.newname = "xx.im"
        self.assertTrue(myia.rename(self.newname), "rename unsuccessful")
        got = myia.name(strippath=True)
        self.assertTrue(
            got == self.newname,
            "wrong name " + got + " should be " + self.newname
        )
        myia.done()

    def test_overwrite(self):
        myia = self._myia
        self.newname = "kfe.im"
        myia.fromshape(self.newname, [20, 20])
        myia.done()
        name = "jfjd.im"
        myia.fromshape(name, [5, 5])
        try:
            myia.rename(self.newname, overwrite=False)
            thrown = False
        except:
            thrown = True
        self.assertTrue(thrown, "overwrite=False exception not thrown")
        myia.open(name)
        res = myia.rename(self.newname, overwrite=True)
        print("res", res)
        self.assertTrue(myia.rename(self.newname, overwrite=True), "overwrite=True unsuccessful")
        self.assertTrue(myia.name(strippath=True) == self.newname, "wrong name")
        myia.done()

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        myia.fromshape("zz", [20, 20])
        self.newname = "zy.im"
        myia.rename(self.newname)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.rename" in msgs[-3], "wrong history")
        self.assertTrue("ia.rename" in msgs[-2], "wrong history")

# Tests for image.replacemaskedpixels
class ia_replacemaskedpixels_test(ImageBase):

    def tearDown(self):
        if self.mymask:
            if os.path.isfile(self.mymask):
                os.unlink(self.mymask)
            else:
                shutil.rmtree(self.mymask)

    def test_stretch(self):
        """ ia.replacemaskedpixels(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.replacemaskedpixels, pixels=-255,
            mask=self.mymask + ">0", stretch=False
        )
        zz = yy.replacemaskedpixels(
            pixels=-255,
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(zz)
        yy.done()

    def test_history(self):
        """Verify history writing"""
        yy = iatool()
        self.mymask = "history.im"
        yy.fromshape(self.mymask, [20, 20])
        yy.addnoise()
        yy.replacemaskedpixels(pixels=-255, mask=self.mymask + ">0")
        msgs = yy.history()
        yy.done()
        self.assertTrue("ia.replacemaskedpixels" in msgs[-2])
        self.assertTrue("ia.replacemaskedpixels" in msgs[-1])

# Tests for image.restoringbeam
class ia_restoringbeam_test(ImageBase):


    def tearDown(self):
        if self.imagename:
            if os.path.isfile(self.imagename):
                os.unlink(self.imagename)
            else:
                shutil.rmtree(self.imagename)


    def test_global_beam(self):
        """Test adding, deleting, and setting beams"""
        myia = self._myia
        myia.fromshape(shape=[10, 10, 4, 10])
        self.assertFalse(bool(myia.restoringbeam()))
        major = "4arcsec"
        minor = "3arcsec"
        pa = "10deg"
        myia.setrestoringbeam(major=major, minor=minor, pa=pa)
        beam = myia.restoringbeam()
        self.assertTrue(beam["major"] == self.qa.quantity(major))
        self.assertTrue(beam["minor"] == self.qa.quantity(minor))
        self.assertTrue(beam["positionangle"] == self.qa.quantity(pa))
        for channel in [-1, 0, 1, 2]:
            for polarization in [-1, 0, 1, 2]:
                beam = myia.restoringbeam(channel=channel, polarization=polarization)
                self.assertTrue(beam["major"] == self.qa.quantity(major))
                self.assertTrue(beam["minor"] == self.qa.quantity(minor))
                self.assertTrue(beam["positionangle"] == self.qa.quantity(pa))
        myia.setrestoringbeam(remove=True)
        self.assertFalse(bool(myia.restoringbeam()))

    def test_per_plane_beams(self):
        myia = self._myia
        nchan = 10
        npol = 4
        for t in ['f', 'c']:
            myia.fromshape(shape=[10, 10, npol, nchan], type=t)
            self.assertFalse(bool(myia.restoringbeam()))
            major = "4arcsec"
            minor = "3arcsec"
            pa = "10deg"
            myia.setrestoringbeam(major=major, minor=minor, pa=pa, channel=20, polarization=2)
            nmajor = "10arcsec"
            nminor = "5arcsec"
            npa = "40deg"
            myia.setrestoringbeam(
                major=nmajor, minor=nminor, pa=npa,
                channel=2, polarization=1
            )
            beams = myia.restoringbeam()
            self.assertTrue(beams["nChannels"] == nchan)
            self.assertTrue(beams["nStokes"] == npol)
            self.assertTrue(len(beams["beams"]) == nchan)
            for chan in range(nchan):
                rec = beams["beams"]["*" + str(chan)]
                for pol in range(npol):
                    bmaj = major
                    bmin = minor
                    bpa = pa
                    if chan == 2 and pol == 1:
                        bmaj = nmajor
                        bmin = nminor
                        bpa = npa
                    beam = rec["*" + str(pol)]
                    self.assertTrue(beam["major"] == self.qa.quantity(bmaj))
                    self.assertTrue(beam["minor"] == self.qa.quantity(bmin))
                    self.assertTrue(beam["positionangle"] == self.qa.quantity(bpa))
                    beam = myia.restoringbeam(channel=chan, polarization=pol)
                    self.assertTrue(beam["major"] == self.qa.quantity(bmaj))
                    self.assertTrue(beam["minor"] == self.qa.quantity(bmin))
                    self.assertTrue(beam["positionangle"] == self.qa.quantity(bpa))
            for chan in [-1, 10]:
                for pol in [-1, 10]:
                    if chan != -1 or pol != -1:
                        self.assertRaises(
                            Exception, myia.restoringbeam,
                            channel=chan, polarization=pol
                        )

    def test_copy_beams(self):
        """Test copy beamset option - CAS-5435"""
        myia = self._myia
        self.imagename = "source.im"
        nchan = 10
        nstokes = 4
        myia.fromshape(self.imagename, shape=[5, 5, nchan, nstokes])
        myia.setrestoringbeam(major="4arcsec", minor="2arcsec", pa="0deg", channel=0, polarization=0)
        myia.setrestoringbeam(major="8arcsec", minor="4arcsec", pa="0deg", channel=2, polarization=2)
        myia.done()
        myia.fromshape("", shape=[10, 10, nchan, nstokes])
        """
        self.assertRaises(
            Exception, myia.setrestoringbeam,
            major="4arcsec", minor="2arcsec", pa="0deg",
            imagename=self.imagename
        )
        self.assertRaises(
            Exception, myia.setrestoringbeam,
            remove=True,
            imagename=self.imagename
        )
        """
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)
        # test overwriting
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)
        myia.done()
        # swap axes
        myia.fromshape("", shape=[10, 10, nstokes, nchan])
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)
        source = iatool()
        source.fromshape(
            self.imagename, shape=[5, 5, nchan - 1, nstokes - 1],
            overwrite=True
        )
        source.done()
        # source has no beam
        self.assertRaises(
            Exception, myia.setrestoringbeam, imagename=self.imagename
        )
        source.open(self.imagename)
        source.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.setrestoringbeam(
            major="8arcsec", minor="8arcsec", pa="0deg",
            channel=0, polarization=0
        )
        # incompatible beam matrices
        self.assertRaises(
            Exception, myia.setrestoringbeam, imagename=self.imagename
        )
        source.fromshape(
            self.imagename, shape=[5, 5, nstokes],
            overwrite=True
        )
        source.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.setrestoringbeam(
            major="8arcsec", minor="4arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.done()
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)

        source.fromshape(
            self.imagename, shape=[5, 5, nstokes, 1],
            overwrite=True
        )
        source.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.setrestoringbeam(
            major="8arcsec", minor="4arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.done()
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)

        source.fromshape(
            self.imagename, shape=[5, 5, nchan],
            overwrite=True
        )
        source.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.setrestoringbeam(
            major="8arcsec", minor="4arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.done()
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)

        source.fromshape(
            self.imagename, shape=[5, 5, nchan, 1],
            overwrite=True
        )
        source.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.setrestoringbeam(
            major="8arcsec", minor="4arcsec", pa="0deg",
            channel=0, polarization=0
        )
        source.done()
        myia.setrestoringbeam(imagename=self.imagename)
        self._compareBeams(myia, self.imagename)

        myia.done()

    def _compareBeams(self, target, imagename):
        source = iatool()
        source.open(imagename)
        tshape = target.shape()
        sshape = source.shape()
        tnchan = tshape[target.coordsys().findaxisbyname("spectral")]
        tnstokes = tshape[target.coordsys().findaxisbyname("stokes")]
        try:
            snchan = sshape[source.coordsys().findaxisbyname("spectral")]
        except:
            snchan = 0
        try:
            snstokes = sshape[source.coordsys().findaxisbyname("stokes")]
        except:
            snstokes = 0
        self.assertTrue(
            (tnchan == snchan or snchan <= 1)
            and (tnstokes == snstokes or snstokes <= 1)
        )
        for c in range(tnchan):
            for p in range(tnstokes):
                got = target.restoringbeam(channel=c, polarization=p)
                sc = c
                if (snchan == 0):
                    sn = -1;
                expec = source.restoringbeam(channel=sc, polarization=p)
                self.assertTrue(got == expec)
        source.done()

    def test_rotate(self):
        """Test rotating beam"""
        myia = self._myia
        nchan = 2
        nstokes = 1
        myia.fromshape("", shape=[5, 5, nstokes, nchan])
        myia.setrestoringbeam(
            major="4arcsec", minor="2arcsec", pa="20deg",
            channel=0, polarization=0
        )
        myia.setrestoringbeam(
            major="8arcsec", minor="4arcsec", pa="40deg",
            channel=1, polarization=0
        )
        myia.rotatebeam("60deg")
        self.assertTrue(
            self.qa.eq(
                myia.restoringbeam(channel=0, polarization=0)['positionangle'],
                "80deg"
            )
        )
        self.assertTrue(
            self.qa.eq(
                myia.restoringbeam(channel=1, polarization=0)['positionangle'],
                "-80deg"
            )
        )

    def test_history_rotatebeam(self):
        """verify history writing"""
        myia = self._myia
        myia.fromshape("", [20, 20])
        myia.setrestoringbeam(major="5arcmin", minor="3arcmin", pa="45deg")
        myia.rotatebeam("20deg")
        msgs = myia.history()
        self.assertTrue(
            "ia.rotatebeam" in msgs[-4],
            f'found unexpected entry -2 in list {msgs}'
        )
        self.assertTrue(
            "ia.rotatebeam" in msgs[-3],
            f'found unexpected entry -1 in list {msgs}'
        )


    def test_history_restoringbeam(self):
        """verify history writing"""
        myia = self._myia
        self.imagename = "zz"
        myia.fromshape(self.imagename, [20, 20])
        major = "4arcsec"
        minor = "3arcsec"
        pa = "10deg"
        myia.setrestoringbeam(major=major, minor=minor, pa=pa)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.setrestoringbeam" in msgs[-2])
        self.assertTrue("ia.setrestoringbeam" in msgs[-1])


    def test_replacing_largest_beam(self):
        """Verify fix for CAS-12627"""
        myia = self._myia
        myia.fromshape("", [20, 20, 1, 2])
        myia.setrestoringbeam(
            channel=0, polarization=0,
            beam={
                'major': {'unit': 'arcsec', 'value': 77.02751922607422},
                'minor': {'unit': 'arcsec', 'value': 50.90080261230469},
                'positionangle': {'unit': 'deg', 'value': -83.47551727294922}
            }
        )
        myia.setrestoringbeam(
            channel=1, polarization=0,
            beam={
                'major': {'unit': 'arcsec', 'value': 10},
                'minor': {'unit': 'arcsec', 'value': 10},
                'positionangle': {'unit': 'deg', 'value': 0}
            }
        )
        commonbeam = myia.commonbeam()
        myia.setrestoringbeam(
            channel=0, polarization=-1,
            beam={
                'major': {'unit': 'arcsec', 'value': 12},
                'minor': {'unit': 'arcsec', 'value': 12},
                'positionangle': {'unit': 'deg', 'value': 0}
            }
        )
        newcommonbeam = myia.commonbeam()
        self.assertEqual(
            newcommonbeam['major']['value'], 12,
            'replacement of largest beam failed'
        )

    def test_exceptions(self):
        """Test various exception cases"""
        def __test_exception(method_parms, expected_msg):
            res = None
            with self.assertRaises(RuntimeError) as cm:
                res = ia.restoringbeam(**method_parms)
            got_exception = cm.exception
            pos = str(got_exception).find(expected_msg)
            self.assertNotEqual(
                pos, -1, msg=f'Unexpected exception was thrown: {got_exception}'
            )


        ia = self._myia
        ia.fromshape('', [200,200,4,10])
        # mbret='matrix' tests
        # mbret='matrix' without per-plane beams
        parms = {}
        parms['mbret'] = 'm'
        __test_exception(
            parms,
            'mbret=\'matrix\' only makes sense if the image has per-plane '
            'beams. This image does not.'
        )
        ia.setrestoringbeam(major='3arcmin', minor='1arcmin', pa='40deg')
        __test_exception(
            parms,
            'mbret=\'matrix\' only makes sense if the image has per-plane '
            'beams. This image does not.'
        )
        ia.setrestoringbeam(remove=True)
        ia.setrestoringbeam(
            major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
            polarization=0
        )
        parms['channel'] = 0
        __test_exception(
            parms,
            'mbret=\'matrix\' only makes sense if both channel and '
            'polarization are negative'
        )
        parms = {}
        parms['polarization'] = 0
        parms['mbret'] = 'm'
        __test_exception(
            parms,
            'mbret=\'matrix\' only makes sense if both channel and '
            'polarization are negative'
        )
        # mbret='list' and image has per-plane beam tests tests
        # no channel axis, but channel specified
        ia.fromshape("", [20, 20, 4])
        ia.setrestoringbeam(
            major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
            polarization=0
        )
        parms = {}
        parms['channel'] = 0
        parms['polarization'] = 0
        ia.fromshape("", [20, 20, 10])
        ia.setrestoringbeam(
            major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
            polarization=0
        )
        parms = {}
        parms['channel'] = 0
        parms['polarization'] = 0
        # channel too high
        parms = {}
        parms['channel'] = 15
        parms['polarization'] = 0
        __test_exception(
            parms,
            'channel value specified as 15 must be less than the number of '
            'channels in the image, which is 10.'
        )
        ia.fromshape("", [20, 20, 4, 10])
        ia.setrestoringbeam(
            major='3arcmin', minor='1arcmin', pa='40deg', channel=0,
            polarization=0
        )
        parms = {}
        parms['channel'] = 8
        parms['polarization'] = 5
        __test_exception(
            parms,
            'polarization value must be less than the number of polarization '
            'planes in the image, which is 4'
        )
        parms['channel'] = -1
        parms['polarization'] = 0
        __test_exception(
            parms,
            'Exception: In the case of an image with both a nondegenerate '
            'spectral axis and a nondegenerate polarization axis either both '
            'channel and polarization must be positive, or both must be '
            'negative.'
        )
        parms['channel'] = 0
        parms['polarization'] = -1
        __test_exception(
            parms,
            'Exception: In the case of an image with both a nondegenerate '
            'spectral axis and a nondegenerate polarization axis either both '
            'channel and polarization must be positive, or both must be '
            'negative.'
        )
        parms = {}
        parms['mbret'] = 'bogus'
        __test_exception(
            parms,
            'Unsupported mbret value bogus'
        )
        ia.done()


    def test_empty_return(self):
        """
        Test that an empty dictionary is returned if mbret='list' and the image
        has no beam(s)
        """
        ia = self._myia
        ia.fromshape("", [20, 20, 4, 5])
        self.assertEqual(
            ia.restoringbeam(), {},
            'Empty dictionary was not returned but should have been'
        )
        ia.done()


    def test_single_beam(self):
        """
        Test that a single global beam is always returned if mbret='list', no
        matter what the values of channel and polarization are
        """
        ia = self._myia
        ia.fromshape("", [20, 20, 4, 5])
        bmaj = qa.quantity('5arcsec')
        bmin = qa.quantity('4arcsec')
        bpa = qa.quantity('40deg')
        ia.setrestoringbeam(major=bmaj, minor=bmin, pa=bpa)
        for c in range(-2, 10):
            for p in range(-2, 10):
                beam = ia.restoringbeam(channel=c, polarization=p)
                self.assertEqual(
                    beam['major'], bmaj, 'returned beam major axis incorrect'
                )
                self.assertEqual(
                    beam['minor'], bmin, 'returned beam minor axis incorrect'
                )
                self.assertEqual(
                    beam['positionangle'], bpa,
                    'returned beam position angle incorrect'
                )
        ia.done()


    def test_per_plane_beams_list_mode(self):
        """
        Test retrieving beams for an image with per-plane beams using mode='list'
        """
        # test specifying specific channel/polarization pair returns correct beam
        ia = self._myia
        ia.fromshape("", [20, 20, 4, 5])
        bmaj0 = qa.quantity('5arcsec')
        bmin0 = qa.quantity('4arcsec')
        bpa0 = qa.quantity('40deg')
        ia.setrestoringbeam(
            major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
        )
        bmaj1 = qa.quantity('7arcsec')
        bmin1 = qa.quantity('6arcsec')
        bpa1 = qa.quantity('50deg')
        ia.setrestoringbeam(
            major=bmaj1, minor=bmin1, pa=bpa1, channel=3, polarization=2
        )
        for c in range(5):
            for p in range(4):
                if c == 3 and p == 2:
                    bmaje = bmaj1
                    bmine = bmin1
                    bpae = bpa1
                else:
                    bmaje = bmaj0
                    bmine = bmin0
                    bpae = bpa0
                ret = ia.restoringbeam(channel=c, polarization=p)
                emsg = (
                    f'Incorrect beam returned for channel={c}, polarization={p}'
                )
                self.assertEqual(ret['major'], bmaje, emsg)
                self.assertEqual(ret['minor'], bmine, emsg)
                self.assertEqual(ret['positionangle'], bpae, emsg)
        ia.done()
        # Test polarization can be anything if channel is non-negative if stokes
        # axis doesn't exist or is degenerate
        for shape in ([20, 20, 5], [20, 20, 5, 1]):
            ia.fromshape("", shape)
            ia.setrestoringbeam(
                major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
            )
            ia.setrestoringbeam(
                major=bmaj1, minor=bmin1, pa=bpa1, channel=3, polarization=0
            )
            for c in range(5):
                for p in range(-2, 10):
                    if c == 3:
                        bmaje = bmaj1
                        bmine = bmin1
                        bpae = bpa1
                    else:
                        bmaje = bmaj0
                        bmine = bmin0
                        bpae = bpa0
                    ret = ia.restoringbeam(channel=c, polarization=p)
                    emsg = (
                        f'Incorrect beam returned for channel={c}, '
                        f'polarization={p}'
                    )
                    self.assertEqual(ret['major'], bmaje, emsg)
                    self.assertEqual(ret['minor'], bmine, emsg)
                    self.assertEqual(ret['positionangle'], bpae, emsg)
            ia.done()
        # Test channel can be anything if polarization is non-negative and
        # if spectral axis doesn't exist or is degenerate
        for shape in ([20, 20, 4], [20, 20, 4, 1]):
            ia.fromshape("", shape)
            ia.setrestoringbeam(
                major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
            )
            ia.setrestoringbeam(
                major=bmaj1, minor=bmin1, pa=bpa1, channel=0, polarization=2
            )
            for c in range(-2, 10):
                for p in range(4):
                    if p == 2:
                        bmaje = bmaj1
                        bmine = bmin1
                        bpae = bpa1
                    else:
                        bmaje = bmaj0
                        bmine = bmin0
                        bpae = bpa0
                    ret = ia.restoringbeam(channel=c, polarization=p)
                    emsg = (
                        f'Incorrect beam returned for channel={c}, '
                        f'polarization={p}'
                    )
                    self.assertEqual(ret['major'], bmaje, emsg)
                    self.assertEqual(ret['minor'], bmine, emsg)
                    self.assertEqual(ret['positionangle'], bpae, emsg)
            ia.done()
        # test both channel and polarization negative return the expected
        # dictionary
        # polarization axis non-extant or degenerate
        for shape in ([20, 20, 5], [20, 20, 5, 1]):
            ia.fromshape("", shape)
            ia.setrestoringbeam(
                major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
            )
            ia.setrestoringbeam(
                major=bmaj1, minor=bmin1, pa=bpa1, channel=3, polarization=0
            )
            ret = ia.restoringbeam(channel=-1, polarization=-1)
            emsg = (
                'Incorrect dictionary returned for both channel and '
                'polarization negative'
            )
            self.assertEqual(ret['nChannels'], 5, emsg)
            self.assertEqual(ret['nStokes'], 1, emsg)
            for c in range(5):
                if c == 3:
                    bmaje = bmaj1
                    bmine = bmin1
                    bpae = bpa1
                else:
                    bmaje = bmaj0
                    bmine = bmin0
                    bpae = bpa0
                subrec = ret['beams'][f'*{c}']['*0']
                self.assertEqual(subrec['major'], bmaje, emsg)
                self.assertEqual(subrec['minor'], bmine, emsg)
                self.assertEqual(subrec['positionangle'], bpae, emsg)
            ia.done()

        # spectral axis non-extant or degenerate
        for shape in ([20, 20, 4], [20, 20, 4, 1]):
            ia.fromshape("", shape)
            ia.setrestoringbeam(
                major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
            )
            ia.setrestoringbeam(
                major=bmaj1, minor=bmin1, pa=bpa1, channel=0, polarization=2
            )
            ret = ia.restoringbeam(channel=-1, polarization=-1)
            self.assertEqual(ret['nChannels'], 1, emsg)
            self.assertEqual(ret['nStokes'], 4, emsg)
            for p in range(4):
                if p == 2:
                    bmaje = bmaj1
                    bmine = bmin1
                    bpae = bpa1
                else:
                    bmaje = bmaj0
                    bmine = bmin0
                    bpae = bpa0
                subrec = ret['beams']['*0'][f'*{p}']
                self.assertEqual(subrec['major'], bmaje, emsg)
                self.assertEqual(subrec['minor'], bmine, emsg)
                self.assertEqual(subrec['positionangle'], bpae, emsg)
            ia.done()

        # nStokes and nChannels both > 1
        ia.fromshape("", [20, 20, 4, 5])
        ia.setrestoringbeam(
            major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
        )
        ia.setrestoringbeam(
            major=bmaj1, minor=bmin1, pa=bpa1, channel=3, polarization=2
        )
        ret = ia.restoringbeam(channel=-1, polarization=-1)
        self.assertEqual(ret['nChannels'], 5, emsg)
        self.assertEqual(ret['nStokes'], 4, emsg)
        for c in range(5):
            for p in range(4):
                if c == 3 and p == 2:
                    bmaje = bmaj1
                    bmine = bmin1
                    bpae = bpa1
                else:
                    bmaje = bmaj0
                    bmine = bmin0
                    bpae = bpa0
                subrec = ret['beams'][f'*{c}'][f'*{p}']
                self.assertEqual(subrec['major'], bmaje, emsg)
                self.assertEqual(subrec['minor'], bmine, emsg)
                self.assertEqual(subrec['positionangle'], bpae, emsg)
        ia.done()


    def test_per_plane_beams_matrix_mode(self):
        """
        Test retrieving beams for an image with per-plane beams using
        mode='matrix' and channel and polarization both negative
        """
        # alternate the axes ordering
        ia = self._myia
        bmaj0 = qa.quantity('5arcsec')
        bmin0 = qa.quantity('4arcsec')
        bpa0 = qa.quantity('40deg')
        bmaj1 = qa.quantity('7arcsec')
        bmin1 = qa.quantity('6arcsec')
        bpa1 = qa.quantity('50deg')
        ns = 4
        nc = 5
        for shape in ([20, 20, ns, nc], [20, 20, nc, ns]):
            ia.fromshape("", shape)
            ia.setrestoringbeam(
                major=bmaj0, minor=bmin0, pa=bpa0, channel=0, polarization=0
            )
            ia.setrestoringbeam(
                major=bmaj1, minor=bmin1, pa=bpa1, channel=3, polarization=2
            )
            ret = ia.restoringbeam(mbret='matrix')
            self.assertEqual(ret['nChannels'], nc, 'Wrong number of channels')
            self.assertEqual(
                ret['nStokes'], ns, 'Wrong number of polarizations'
            )
            bmaje = np.full(shape[2:], bmaj0['value'])
            bmine = np.full(shape[2:], bmin0['value'])
            bpae = np.full(shape[2:], bpa0['value'])
            if shape[2] == nc:
                bmaje[3, 2] = bmaj1['value']
                bmine[3, 2] = bmin1['value']
                bpae[3, 2] = bpa1['value']
            else:
                bmaje[2, 3] = bmaj1['value']
                bmine[2, 3] = bmin1['value']
                bpae[2, 3] = bpa1['value']
            bmaje = qa.quantity(bmaje, bmaj0['unit'], keepshape=True)
            bmine = qa.quantity(bmine, bmin0['unit'], keepshape=True)
            bpae = qa.quantity(bpae, bpa0['unit'], keepshape=True)
            self.assertTrue(
                (ret['major']['value'] == bmaje['value']).all(),
                f'Incorrect major axis, got {ret["major"]}, expected {bmaje}'
            )
            self.assertEqual(
                ret['major']['unit'], bmaje['unit'],
                f'Incorrect major axis, got {ret["major"]}, expected {bmaje}'
            )
            self.assertTrue(
                (ret['minor']['value'] == bmine['value']).all(),
                f'Incorrect minor axis, got {ret["minor"]}, expected {bmine}'
            )
            self.assertEqual(
                ret['minor']['unit'], bmine['unit'],
                f'Incorrect mainor axis, got {ret["major"]}, expected {bmine}'
            )
            self.assertTrue(
                (ret['pa']['value'] == bpae['value']).all(),
                f'Incorrect pa, got {ret["pa"]}, expected {bpae}'
            )
            self.assertEqual(
                ret['pa']['unit'], bpae['unit'],
                f'Incorrect pa, got {ret["pa"]}, expected {bpae}'
            )


    def test_setrestoringbeam_rules(self):
        """
        CAS-12599 rules for setrestoringbeam
        """
        # the beam parameter has higher priority than major, minor, pa
        ia = self._myia
        ia.fromshape("", [20, 20])
        ia.setrestoringbeam(
            major='40arcsec', minor='20arcsec', pa='0deg',
            beam={
                'major': qa.quantity("10arcsec"),
                'minor': qa.quantity('7arcsec'),
                'positionangle': qa.quantity('70deg')
            }
        )
        got_beam = ia.restoringbeam()
        self.assertEqual(got_beam['major']['value'], 10, 'Incorrect major axis')
        self.assertEqual(got_beam['minor']['value'], 7, 'Incorrect minor axis')
        self.assertEqual(
            got_beam['positionangle']['value'], 70, 'Incorrect position angle'
        )
        with pytest.raises(
            Exception, match=r'Beam record does not contain 3 fields'
        ):
            ia.setrestoringbeam(
                major='40arcsec', minor='20arcsec', pa='0deg',
                beam={
                    'major': qa.quantity("10arcsec"),
                    'minor': qa.quantity('7arcsec'),
                }
            )
        with pytest.raises(Exception, match=r'non-negative'):
            ia.setrestoringbeam(
                major='40arcsec', minor='20arcsec', pa='0deg',
                beam={
                    'major': qa.quantity("10arcsec"),
                    'minor': qa.quantity('0arcsec'),
                    'positionangle': qa.quantity('20deg')
                }
            )
        ia.setrestoringbeam(major='40arcsec', minor='20arcsec', pa='0deg')
        got_beam = ia.restoringbeam()
        self.assertEqual(
            qa.quantity(got_beam['major']), qa.quantity('40arcsec'),
            'Incorrect major axis'
        )
        self.assertEqual(
            qa.quantity(got_beam['minor']), qa.quantity('20arcsec'),
            'Incorrect minor axis'
        )
        self.assertEqual(
            qa.quantity(got_beam['positionangle']), qa.quantity('0deg'),
            'Incorrect position angle'
        )
        with pytest.raises(Exception, match=r'beam record is empty, minor'):
            ia.setrestoringbeam(major='40arcsec', pa='0deg')
        with pytest.raises(Exception,
            match=r'If beam record not specified, all of major, minor, '
            'and positionangle'
        ):
            ia.setrestoringbeam(major='40arcsec', minor='20arcsec')
        ia.setrestoringbeam(major='40arcmin', minor='20arcmin', pa='1rad')
        ia.setrestoringbeam(major=7, minor=6, pa=2)
        got_beam = ia.restoringbeam()
        self.assertEqual(
            {'value': 7, 'unit': 'arcmin'}, got_beam['major'],
            'Incoorect major axis in implied unit test'
        )
        self.assertEqual(
            {'value': 6, 'unit': 'arcmin'}, got_beam['minor'],
            'Incoorect minor axis in implied unit test'
        )
        self.assertEqual(
            {'value': 2, 'unit': 'rad'}, got_beam['positionangle'],
            'Incoorect pa in implied unit test'
        )
        ia.setrestoringbeam(remove=True)
        ia.setrestoringbeam(major=7, minor=6, pa=2)
        got_beam = ia.restoringbeam()
        self.assertEqual(
            {'value': 7, 'unit': 'arcsec'}, got_beam['major'],
            'Incoorect major axis in implied unit test'
        )
        self.assertEqual(
            {'value': 6, 'unit': 'arcsec'}, got_beam['minor'],
            'Incoorect minor axis in implied unit test'
        )
        self.assertEqual(
            {'value': 2, 'unit': 'deg'}, got_beam['positionangle'],
            'Incoorect pa in implied unit test'
        )




# Tests for image.rotate
class ia_rotate_test(ImageBase):

    def tearDown(self):
        if self.mymask:
            if os.path.isfile(self.mymask):
                os.unlink(self.mymask)
            else:
                shutil.rmtree(self.mymask)

    def test_stretch(self):
        """ ia.rotate(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.rotate,
            mask=self.mymask + ">0", stretch=False
        )
        zz = yy.rotate(
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(zz and type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_basic(self):
        """verify basic rotation works"""
        myia = iatool()
        myia.open(self.datapath + "prerot.im")
        rot = myia.rotate(pa="45deg")
        got = rot.getchunk();
        rot.done()
        myia.open(self.datapath + "postrot.im")
        expec = myia.getchunk()
        myia.done()
        self.assertTrue(np.abs(got - expec).max() < 10e-22)

    def test_history(self):
        """Verify history is written"""
        myia = iatool()
        myia.open(self.datapath + "prerot.im")
        rot = myia.rotate(pa="45deg")
        myia.done()
        msgs = rot.history()
        print("msgs %s" % msgs)
        rot.done()
        self.assertTrue("ia.rotate" in msgs[-2])
        self.assertTrue("ia.rotate" in msgs[-1])

# Tests for image.sepconvolve
class ia_sepconvolve_test(ImageBase):

    def tearDown(self):
        if self.mymask:
            if os.path.isfile(self.mymask):
                os.unlink(self.mymask)
            else:
                shutil.rmtree(self.mymask)

    def test_stretch(self):
        """ ia.sepconvolve(): Test stretch parameter"""
        yy = iatool()
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.sepconvolve, widths=[2],
            mask=self.mymask + ">0", stretch=False
        )
        zz = yy.sepconvolve(widths=[2],
                            mask=self.mymask + ">0", stretch=True
                            )
        self.assertTrue(zz and type(zz) == type(yy))
        yy.done()
        zz.done()

    def test_history(self):
        """Verify history writing"""
        yy = iatool()
        yy.fromshape("", [20, 20])
        zz = yy.sepconvolve(widths=[2])
        yy.done()
        msgs = zz.history()
        zz.done()
        self.assertTrue("ia.sepconvolve" in msgs[-2])
        self.assertTrue("ia.sepconvolve" in msgs[-1])

# Tests for image.set
class ia_set_test(ImageBase):

    def tearDown(self):
        self._myia.done()

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        myia.fromshape("", [20, 20])
        myia.set(5)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.set" in msgs[-2])
        self.assertTrue("ia.set" in msgs[-1])

# Tests for image.setbrightnessunit
class ia_setbrightnessunit_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        if self.imagename:
            if os.path.isfile(self.imagename):
                os.unlink(self.imagename)
            else:
                shutil.rmtree(self.imagename)

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        self.imagename = "zz"
        myia.fromshape(self.imagename, [20, 20])
        myia.setbrightnessunit("Jy/beam")
        msgs = myia.history()
        self.assertTrue("ia.setbrightnessunit" in msgs[-2])
        self.assertTrue("ia.setbrightnessunit" in msgs[-1])

# Tests for image.setcoordsys
class ia_setcoordsys_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        if self.imagename:
            if os.path.isfile(self.imagename):
                os.unlink(self.imagename)
            else:
                shutil.rmtree(self.imagename)

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        self.imagename = "zz"
        myia.fromshape(self.imagename, [20, 20])
        csys = myia.coordsys()
        myia.setcoordsys(csys.torecord())
        msgs = myia.history()
        self.assertTrue("ia.setcoordsys" in msgs[-2])
        self.assertTrue("ia.setcoordsys" in msgs[-1])

    def test_one_direction_axis(self):
        """Verify fix for CAS-10447"""
        myia = self._myia
        myia.fromshape('', [64, 64, 128])
        mycs = myia.coordsys(axes=[0, 2])
        myia.fromshape('', [64, 128], csys=mycs.torecord())
        self.assertTrue(myia.coordsys().naxes() == 2, "Wrong number of axes")

        myia.fromshape('', [64, 64, 128])
        mycs = myia.coordsys(axes=[0, 2])
        myia.fromshape('', [64, 128])
        myia.setcoordsys(mycs.torecord())
        self.assertTrue(myia.coordsys().naxes() == 2, "Wrong number of axes")
        myia.done()
        mycs.done()

# Tests for image.setmiscinfo
class ia_setmiscinfo_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        if self.imagename:
            if os.path.isfile(self.imagename):
                os.unlink(self.imagename)
            else:
                shutil.rmtree(self.imagename)

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        self.imagename = "zz"
        myia.fromshape(self.imagename, [20, 20])
        myia.setmiscinfo({"me": "you"})
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.setmiscinfo" in msgs[-2])
        self.assertTrue("ia.setmiscinfo" in msgs[-1])

# Tests for image.summary
class ia_summary_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        self.tb.done()
        self.qa.done()
        self.assertTrue(len(self.tb.showcache()) == 0)

    def test_beams(self):
        """test per plane beams get accounted for correctly"""
        #self.qa = quanta()
        myia = self._myia
        shape = [10, 10, 10, 4]
        for t in ['f', 'c']:
            myia.fromshape("", shape, type=t)
            bmaj = self.qa.quantity("4arcsec")
            bmin = self.qa.quantity("2arcsec")
            bpa = self.qa.quantity("40deg")
            myia.setrestoringbeam(major=bmaj, minor=bmin, pa=bpa, channel=0, polarization=0)
            cmaj = self.qa.quantity("7arcsec")
            cmin = self.qa.quantity("5arcsec")
            cpa = self.qa.quantity("80deg")
            myia.setrestoringbeam(major=cmaj, minor=cmin, pa=cpa, channel=6, polarization=3)
            summary = myia.summary()
            self.assertTrue("perplanebeams" in summary)
            beams = summary["perplanebeams"]["beams"]
            for c in range(shape[2]):
                for p in range(shape[3]):
                    beam = beams["*" + str(c)]["*" + str(p)]
                    majax = beam["major"]
                    minax = beam["minor"]
                    pa = beam["positionangle"]
                    if c == 6 and p == 3:
                        self.assertTrue(majax == cmaj)
                        self.assertTrue(minax == cmin)
                        self.assertTrue(pa == cpa)
                    else:
                        self.assertTrue(majax == bmaj)
                        self.assertTrue(minax == bmin)
                        self.assertTrue(pa == bpa)

# Tests for image.tofits
class ia_tofits_test(ImageBase):

    def tearDown(self):
        self._myia.done()
        self.qa.done()
        data = ["blah2.fits", "maskim", "myfits.fits", "my.im"]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def test_stretch(self):
        """ ia.tofits(): Test stretch parameter"""
        yy = self._myia
        mymask = "maskim"
        yy.fromshape(mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.tofits, outfile="blah1.fits",
            mask=mymask + ">0", stretch=False
        )
        zz = yy.tofits(
            outfile="blah2.fits",
            mask=mymask + ">0", stretch=True
        )
        self.assertTrue(zz and type(zz) == type(True))
        yy.done()

    def test_CAS3675(self):
        """ test fix for CAS 3675, outfile must be specified """
        name = "my.im"
        yy = self._myia
        yy.fromshape(name, [1, 1, 1, 1])
        self.assertRaises(Exception, yy.tofits, overwrite=True)
        yy.done()

    def test_multibeam(self):
        """Test exporting and importing an image with multiple beams"""
        myia = self._myia
        shape = [10, 10, 10, 4]
        myia.fromshape("", shape)
        bmaj = self.qa.quantity("10arcsec")
        bmin = self.qa.quantity("7arcsec")
        bpa = self.qa.quantity("45deg")
        myia.setrestoringbeam(
            major=bmaj, minor=bmin, pa=bpa,
            channel=0, polarization=0
        )
        cmaj = self.qa.quantity("12arcsec")
        cmin = self.qa.quantity("8arcsec")
        cpa = self.qa.quantity("50deg")
        myia.setrestoringbeam(
            major=cmaj, minor=cmin, pa=cpa,
            channel=6, polarization=3
        )
        myia.addnoise()
        exppix = myia.getchunk()
        fitsname = "myfits.fits"
        myia.tofits(outfile=fitsname)
        myia.done()
        for i in range(4):
            if i == 0:
                myia.fromfits("", fitsname)
            elif i == 1:
                myia.open(fitsname)
            elif i == 2:
                zz = iatool()
                myia = zz.newimagefromfits("", fitsname)
            else:
                zz = iatool()
                myia = zz.newimagefromfile(fitsname)
            ep = 1e-7
            for c in range(shape[2]):
                for p in range(shape[3]):
                    beam = myia.restoringbeam(c, p)
                    majax = self.qa.convert(beam["major"], "arcsec")["value"]
                    minax = self.qa.convert(beam["minor"], "arcsec")["value"]
                    pa = self.qa.convert(beam["positionangle"], "deg")["value"]
                    if c == 6 and p == 3:
                        self.assertTrue(abs(1 - majax / cmaj["value"]) < ep)
                        self.assertTrue(abs(1 - minax / cmin["value"]) < ep)
                        self.assertTrue(abs(1 - pa / cpa["value"]) < ep)
                    else:
                        self.assertTrue(abs(1 - majax / bmaj["value"]) < ep)
                        self.assertTrue(abs(1 - minax / bmin["value"]) < ep)
                        self.assertTrue(abs(1 - pa / bpa["value"]) < ep)
                    # ensure the pixel values were read correctly
                    gotpix = myia.getchunk()
                    self.assertTrue((gotpix == exppix).all())
            myia.done()


# Tests for image.twopointcorrelation
class ia_twopointcorrelation_test(ImageBase):

    def tearDown(self):
        data = [self.mymask, self.imagename, self.outfile]
        for f in data:
            if os.path.exists(f):
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f)
                else:
                    shutil.rmtree(f)

    def test_stretch(self):
        """ ia.twopointcorrelation(): Test stretch parameter"""
        yy = self._myia
        self.mymask = "maskim"
        yy.fromshape(self.mymask, [20, 20, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [20, 20, 1, 20]
        yy.fromshape("", shape)
        yy.addnoise()
        self.assertRaises(
            Exception,
            yy.twopointcorrelation,
            mask=self.mymask + ">0", stretch=False
        )
        zz = yy.twopointcorrelation(
            mask=self.mymask + ">0", stretch=True
        )
        self.assertTrue(zz and type(zz) == type(True))
        yy.done()

    def test_history(self):
        """verify history writing"""
        myia = self._myia
        self.imagename = "zz"
        myia.fromshape(self.imagename, [20, 20])
        self.outfile = "xyz.im"
        # does not return an ia tool, just a bool
        self.assertTrue(myia.twopointcorrelation(self.outfile))
        myia.open(self.outfile)
        msgs = myia.history()
        myia.done()
        self.assertTrue("ia.twopointcorrelation" in msgs[-2])
        self.assertTrue("ia.twopointcorrelation" in msgs[-1])

class ia_fitsheader_test(ImageBase):

    def compare_dicts( self, dict1, dict2, exclude_keys=[ ] ):
        def isnumarray( ary ):
            ### check if array is numeric
            return np.issubdtype(ary.dtype, np.number)

        diff = { }

        for k, v in dict1.items( ):
            ###
            ###  more conditions would be needed to compare nested dictionaries
            ###
            if k in exclude_keys:
                ### skip excluded keys
                continue
            if isinstance(dict1[k], np.ndarray) and isinstance(dict2[k], np.ndarray):
                ### compare arrays
                if isnumarray(dict1[k]) and isnumarray(dict2[k]):
                    # numeric array compare
                    if not np.allclose(dict1[k],dict2[k]):
                        diff[k] = ( dict1[k], dict2[k] )
                elif not isnumarray(dict1[k]) and not isnumarray(dict2[k]):
                    # string array compare
                    if not np.array_equal(dict1[k],dict2[k]):
                        diff[k] = ( dict1[k], dict2[k] )
                else:
                    # different sorts of arrays
                    diff[k] = ( dict1[k], dict2[k] )
                continue
            if isinstance(dict1[k], np.ndarray) or isinstance(dict2[k], np.ndarray):
                ### one array and one non-array is a difference
                diff[k] = ( dict1[k], dict2[k] )
                continue
            if dict1[k] != dict2[k]:
                ### compare singleton values
                diff[k] = ( dict1[k], dict2[k] )

        return diff

    def test_fitsheader(self):
        """test creation of FITS header"""
        myia = self._myia
        myia.maketestimage()
        expected_header = { 'BITPIX': -32, 'BMAJ': 0.014861112, 'BMIN': 0.0094999997, 'BPA': 6.0, 'BSCALE': 1.0,
                            'BTYPE': 'Intensity', 'BUNIT': 'Jy/beam ', 'BZERO': 0.0,
                            'CDELT': np.array([-0.00222222,  0.00333333]), 'CRPIX': np.array([56., 38.]),
                            'CRVAL': np.array([0., 0.]), 'CTYPE': np.array(['RA---SIN', 'DEC--SIN'], dtype='<U8'),
                            'CUNIT1': 'deg     ', 'CUNIT2': 'deg     ', 'DATE': '2023-07-31T17:55:28.349375',
                            'END': '', 'EQUINOX': 2000.0, 'EXTEND': True,
                            'HISTORY': np.array([ "  File modified by user 'dbarnes' with fv  on 1999-07-28T14:19:41",
                                                  "  File modified by user 'dbarnes' with fv  on 1999-07-28T14:21:43",
                                                  'CASA START LOGTABLE',
                                                  "2023-07-31T17:55:28 INFO SRCCODE='::image::maketestimage'",
                                                  'Ran ia.maketestimage',
                                                  "2023-07-31T17:55:28 INFO SRCCODE='::image::maketestimage'",
                                                  'ia.maketestimage(outfile="", overwrite=false)',
                                                  'CASA END LOGTABLE'], dtype='<U65'),
                            'IMAGENME': 'Temporary_Image', 'LATPOLE': 0.0, 'LONPOLE': 180.0,
                            'NAXIS': np.array([  2, 113,  76]), 'OBJECT': '        ',
                            'ORIGIN': 'casacore-@PROJECT_VERSION@', 'PC1_1': 1.0, 'PC1_2': 0.0, 'PC2_1': -0.0,
                            'PC2_2': 1.0, 'PV2_1': 0.0, 'PV2_2': 0.0, 'RADESYS': 'FK5     ',
                            'SIMPLE': True, 'TIMESYS': 'UTC     ' }

        difference = self.compare_dicts(expected_header, myia.fitsheader( ),['HISTORY','DATE','ORIGIN'])
        self.assertEqual( len(difference), 0, f'''expected no differences between reference and created fits headers, but found: {repr(difference)}''' )

if __name__ == '__main__':
    unittest.main()
