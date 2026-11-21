##########################################################################
# test_task_impv.py
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
# [Add the link to the JIRA ticket here once it exists]
#
# Based on the requirements listed in plone found here:
# # https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.analysis.impv.html
#
#
##########################################################################
import sys
import os
import unittest
import shutil
from filecmp import dircmp
import numpy

import casatools
from casatasks import impv, casalog

NATIVE_TASK = os.environ.get("CASARS_UPSTREAM_IMAGE_IMPLEMENTATION") == "casa-rs"
if NATIVE_TASK:
    from casars.tasks import TaskExecutionError

### DATA ###
datapath = casatools.ctsys.resolve('unittest/impv/ngc5921.clean.image')
dataroot = casatools.ctsys.resolve('unittest/impv/')
qa = casatools.quanta()
mytb = casatools.table()
myia = casatools.image()

testfile = 'testing.im'
testfile2 = 'testing2.im'
testfile3 = 'testing3.im'

logpath = casalog.logfile()
logname = 'testlog.log'

def run_impv(
             imagename, outfile, start, end, width,
             center=[], length=[], pa='', mode="coords"
             ):
    return impv(
                imagename=imagename, outfile=outfile, start=start,
                end=end, width=width, center=center, length=length,
                mode=mode, pa=pa
                )

def makeImage():

    imagename = "gen.im"
    myia.fromshape(imagename, [10, 10, 10])
    bb = myia.getchunk()
    # basic sanity test, no rotation involved
    for i in range(10):
        bb[i,5,:] = i
        bb[i,0:5,:] = i+1
        bb[i,6:10,:] = i+2
    myia.putchunk(bb)
    expeccoord = myia.toworld([1,5,0])['numeric'][2]
    mycsys = myia.coordsys()
    units = mycsys.units()
    expinc = mycsys.increment()["numeric"]
    expinc = [
        abs(
            qa.convert(
                qa.quantity(expinc[0], units[0]), "arcsec"
            )["value"]
        ),
        expinc[2]
    ]
    myia.done()

    return imagename

class impv_test(unittest.TestCase):

    def assertInvalidSelection(self, native_message):
        if NATIVE_TASK:
            return self.assertRaisesRegex(TaskExecutionError, native_message)
        return self.assertRaises(UnboundLocalError)

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        if os.path.exists(testfile):
            shutil.rmtree(testfile)

        if os.path.exists(testfile2):
            shutil.rmtree(testfile2)

        if os.path.exists(testfile3):
            shutil.rmtree(testfile3)

        if os.path.exists('gen.im'):
            shutil.rmtree('gen.im')

        casalog.setlogfile(logpath)

        if os.path.exists(logname):
            os.remove(logname)

    @classmethod
    def tearDownClass(cls):
        os.system('rm -rf test_pv_*')
        os.system('rm -rf *.im')
        shutil.rmtree('xyz')
        shutil.rmtree('kk')
        shutil.rmtree('maskim')

    def test_createsImage(self):
        '''
            test_createsImage
            -------------------

            Check that an output image name is generated if an outfile is given
        '''

        impv(imagename=datapath, outfile=testfile, start=[10,15], end=[110,120])
        self.assertTrue(os.path.exists(testfile))

        # Check that the outfile needs to be given
        refusal = (self.assertRaisesRegex(ValueError, "outfile.*(missing|empty)")
                   if NATIVE_TASK else self.assertRaises(UnboundLocalError))
        with refusal:
            impv(imagename=datapath, outfile='', start=[10,15], end=[110,120])


    def test_modelength(self):
        '''
            test_modelength
            -----------

            If mode='coords' use start and end values, if 'length' use center, length, and pa
            The use of parameters for the other mode is not allowed
        '''
        # Catch if using the wrong combo of length and mode are allowed
        with self.assertInvalidSelection("length mode requires center, length and pa"):
            impv(imagename=datapath, outfile=testfile, start=[10,15], end=[110,120], mode='length')

        self.assertFalse(os.path.exists(testfile))

    def test_modeStartEnd(self):
        ''' Check that start and end is required '''
        # Catch if there is no start or end given
        with self.assertInvalidSelection("coords mode requires start and end"):
            impv(imagename=datapath, outfile=testfile, mode='coords')

    def test_modeUnsupported(self):
        ''' Check unsupported mode values '''
        impv(imagename=datapath, outfile=testfile, start=[10,15], end=[110,120], mode='c')

        self.assertTrue(os.path.exists(testfile))


    def test_region(self):
        '''
            test_region
            -------------

            Check that the region parameter sets the spectral extent of the final image
            If this is left blank then the entire spectral range is used.
        '''

        impv(imagename=datapath, outfile=testfile, start=[10,15], end=[110,120], region='box[[0pix,0pix],[255pix,255pix]]')
        self.assertTrue(os.path.exists(testfile))


    def test_startEnd(self):
        '''
            test_startEnd
            ---------------

            Check that the start and end parameters can be represented multiple ways
            TODO come back to this for the alternate coord systems selections
        '''

        imagename = makeImage()

        impv(imagename=imagename, outfile=testfile, start=[2,5], end=[7,5])
        impv(imagename=imagename, outfile=testfile2, start=["3.00000038arcmin", "0'"], end=["2.15980000e+04'", "0arcmin"])
        impv(imagename=imagename, outfile=testfile3, start=["0h0m12s", "0d0m0s" ], end=["2.15980000e+04'", "0arcmin"])

        self.assertTrue(os.path.exists(testfile))
        self.assertTrue(os.path.exists(testfile2))
        self.assertTrue(os.path.exists(testfile3))


    def test_center(self):
        '''
            test_center
            -------------

            Check that the center parameter can be represented in multiple ways
            TODO come back to this for the alternate coord system selecions
        '''

        imagename = makeImage()

        impv(imagename=imagename, outfile=testfile, center=[4.5, 5], pa='90deg', length=5,mode='length')
        impv(imagename=imagename, outfile=testfile2, center=["0:0:02", "0.0.0"], pa='90deg', length=5,mode='length')
        self.assertTrue(os.path.exists(testfile))
        self.assertTrue(os.path.exists(testfile2))

    def test_length(self):
        '''
            test_length
            -------------

            Check that the length parameter can be specified by numeric value or valid quantity
        '''

        impv(imagename=datapath, outfile=testfile, center=[45,50], pa='45deg', length=5, mode='length')
        self.assertTrue(os.path.exists(testfile))

        impv(imagename=datapath, outfile=testfile2, center=[45,50], pa='45deg', length='5arcmin', mode='length')
        self.assertTrue(os.path.exists(testfile2))


    def test_pa(self):
        '''
            test_pa
            ---------

            Check that the pa parameter works when provided with a valid quanitity

            NOTE: Does the default case need to be execured again, even though it is run in other parts of the tests?
            This may just be adding time without any real benifit to the test
        '''

        impv(imagename=datapath, outfile=testfile, center=[45,50], pa='45deg', length=5, mode='length')
        self.assertTrue(os.path.exists(testfile))


    def test_width(self):
        '''
            test_width
            ------------

            Check that the width parameer works when provided with a valid string quantity or a quantity record
        '''

        impv(imagename=datapath, outfile=testfile, center=[45,50], pa='45deg', length=5, mode='length', width='4arcsec')
        self.assertTrue(os.path.exists(testfile))

        impv(imagename=datapath, outfile=testfile2, center=[45,50], pa='45deg', length=5, mode='length', width=qa.quantity('4arcsec'))
        self.assertTrue(os.path.exists(testfile2))


    def test_unit(self):
        '''
            test_unit
            -----------

            Check that this parameter gives the unit for the offset axis in the resulting image
        '''

        impv(imagename=datapath, outfile=testfile, center=[45,50], pa='45deg', length=5, mode='length', unit='deg')
        self.assertTrue(os.path.exists(testfile))


    def test_tableRecord(self):
        '''
            test_tableRecord
            ------------------

            Check that the result is written to the output image as a table record, meaning it can be retrieved with the table tool.
        '''

        impv(imagename=datapath, outfile=testfile, center=[45,50], pa='45deg', length=5, mode='length')
        mytb.open(testfile)
        mytb.close()

# MERGED TESTS CASES FROM ORIGINAL test_impv
    def test_pv(self):
        """ ia.pv(): Test pv()"""
        #myia = self.ia
        imagename = "zxye.im"
        myia.fromshape(imagename, [10, 10, 10])
        bb = myia.getchunk()
        # basic sanity test, no rotation involved
        for i in range(10):
            bb[i, 5, :] = i
            bb[i, 0:5, :] = i + 1
            bb[i, 6:10, :] = i + 2
        myia.putchunk(bb)
        expeccoord = myia.toworld([1, 5, 0])['numeric'][2]
        mycsys = myia.coordsys()
        units = mycsys.units()
        expinc = mycsys.increment()["numeric"]
        expinc = [
            abs(
                qa.convert(
                    qa.quantity(expinc[0], units[0]), "arcsec"
                )["value"]
            ),
            expinc[2]
        ]
        myia.done()
        self.assertTrue(len(mytb.showcache()) == 0)
        pv = myia
        # no width
        for i in range(7):
            if i == 0:
                start = [2, 5]
                end = [7, 5]
                mode = "coords"
            elif i == 1:
                start = ["3.00000038arcmin", "0'"]
                end = ["2.15980000e+04'", "0arcmin"]
                mode = "coords"
            if i == 2:
                start = ["0h0m12s", "0d0m0s"]
                end = ["23:59:52", "0.0.0"]
                mode = "coords"
            if i == 3:
                center = [4.5, 5]
                length = 5
                pa = "90deg"
                mode = "length"
            if i == 4:
                center = ["0:0:02", "0.0.0"]
                length = 5
                pa = "90deg"
                mode = "length"
            if i == 5:
                center = ["0:0:02", "0.0.0"]
                length = "5arcmin"
                pa = "90deg"
                mode = "length"
            if i == 6:
                center = [4.5, 5]
                length = "5arcmin"
                pa = "90deg"
                mode = "length"
            outfile = "test_pv_" + str(i)
            if i <= 2:
                xx = run_impv(
                    imagename=imagename, outfile=outfile, start=start,
                    end=end, width=1, mode=mode
                )
            else:
                xx = run_impv(
                    imagename=imagename, outfile=outfile, start=[],
                    end=[], width=1, center=center, length=length,
                    pa=pa, mode=mode
                )
            if (type(xx) == type(myia)):
                xx.done()
            self.assertTrue(len(mytb.showcache()) == 0)
            pv.open(outfile)
            expec = [6, 10]
            got = pv.shape()
            self.assertTrue((got == expec).all())
            expec = numpy.zeros(got)
            for i in range(10):
                expec[:, i] = range(2, 8)
            got = pv.getchunk()
            self.assertTrue((got == expec).all())
            self.assertTrue(pv.getchunk(getmask=True).all())
            got = pv.toworld([0, 0, 0])['numeric'][1]
            self.assertTrue(abs(got - expeccoord) < 1e-6)
            gotinc = pv.coordsys().increment()["numeric"]
            # the position offset axis always has units of arcsec, the units
            # in the input image were arcmin
            self.assertTrue((abs(gotinc - expinc) < 1e-5).all())
            pv.done()

        # width > 1
        for i in range(5):
            outfile = "test_pv_1_" + str(i)
            if i == 0:
                width = 3;
            elif i == 1:
                width = "3arcmin"
            elif i == 2:
                width = "1.1arcmin"
            elif i == 3:
                width = qa.quantity("1.2arcmin")
            elif i == 4:
                # width units different from axis units, CAS-5975
                width = qa.quantity("72000marcsec")
            xx = run_impv(
                imagename=imagename, outfile=outfile, start=[2, 5],
                end=[7, 5], width=width, mode="coords"
            )
            if (type(xx) == type(myia)):
                xx.done()
            pv.open(outfile)
            expec = [6, 10]
            got = pv.shape()
            self.assertTrue((got == expec).all())
            expec = numpy.zeros(got)
            for i in range(10):
                expec[:, i] = range(3, 9)
            got = pv.getchunk()
            self.assertTrue((got == expec).all())
            self.assertTrue(pv.getchunk(getmask=True).all())
            pv.done()

    def test_stretch(self):
        """ia.pv(): Test stretch parameter"""
        yy = myia
        mymask = "maskim"
        yy.fromshape(mymask, [200, 200, 1, 1])
        yy.addnoise()
        yy.done()
        shape = [200, 200, 1, 20]
        yy.fromshape("kk", shape)
        yy.addnoise()
        yy.done()
        self.assertRaises(
            Exception, impv, imagename="kk", outfile="x1.im", start=[2, 2],
            end=[20, 2], mask=mymask + ">0", stretch=False
        )

        outfile = "xyz"
        impv(
            imagename="kk", outfile=outfile, start=[2, 2], end=[20, 2],
            mask=mymask + ">0", stretch=True
        )
        self.assertTrue(os.path.exists(outfile))

    def test_CAS_2996(self):
        """ia.pv(): Test issues raised in CAS-2996"""
        # the only tests necessary here are to ensure ia.pv() runs
        # successfully for the provided inputs
        # calculate stats to make sure region determination code doesn't segfault (CAS-4881
        myia.open(dataroot + "pv1.im")
        xx = myia.pv(start=[30, 30], end=[250, 250])
        xx.statistics()
        xx = myia.pv(start=[30, 250], end=[250, 30])
        xx.statistics()
        xx = myia.pv(start=[250, 250], end=[30, 30])
        xx.statistics()
        xx = myia.pv(start=[250, 30], end=[30, 250])
        xx.statistics()

        myia.open(dataroot + "pv2.im")
        x1 = 264.865854
        x2 = 166.329268
        y1 = 142.914634
        y2 = 232.670732
        # test units from task level
        outfile = "unittest.im"
        unit = "arcmin"
        impv(
            imagename=dataroot + "pv1.im", unit=unit,
            outfile="unittest.im", start=[3, 3], end=[6, 6]
        )
        myia.open(outfile)
        self.assertTrue(myia.coordsys().units()[0] == unit)
        myia.done()

    def test_mask(self):
        """Verify fix of mask defect in CAS-5520"""
        outfile = "mask_test_got.im"
        impv(
            imagename=dataroot + "pv_mask_test.im", outfile=outfile,
            overwrite=True, start=[343, 42], end=[343, 660], width=425, unit='arcsec'
        )
        myia.open(dataroot + "pv_mask_exp.im")
        expec = myia.getchunk(getmask=True)
        myia.open(outfile)
        got = myia.getchunk(getmask=True)
        myia.done()
        self.assertTrue((got == expec).all())

    def test_machine_precision_fix(self):
        """Test fix for finite machine precision issue, CAS-6043"""
        outfile = "CAS-6043.out.im"
        impv(
            imagename=dataroot + 'CAS-6043.im', outfile=outfile,
            start=[187, 348], end=[228, 383]
        )
        self.assertTrue(os.path.exists(outfile))

    def test_pa(self):
        """Test that when pa is given, the start of the slice is at pa and end is at pa-180deg"""
        myia.open(dataroot + "pv_patest_exp.im")
        expec = myia.getchunk()
        myia.done()
        imagename = dataroot + "pv_patest.im"

        for length in [19, "19arcmin"]:
            for center in [
                [9, 9], ["00h00m4s", "-0d1m"], "00:00:04 -0d1m",
                "GALACTIC +096.21.17.792 -060.12.37.929"
            ]:
                pa = "45deg"
                if type(center) == str and center.startswith("G"):
                    # pa = "68.46450771415163deg"
                    pa = "68.464508deg"
                outfile = "pv_patest_got" + str(length) + str(center) + ".im"
                impv(
                    imagename=imagename, outfile=outfile,
                    center=center, length=length, pa=pa,
                    mode="length"
                )
                myia.open(outfile)
                got = myia.getchunk()
                myia.done()
                self.assertTrue(abs(got / expec - 1).max() < 1e-6)

    def test_CAS7765(self):
        """CAS-7765, successful completion is all that is necessary to indicate verification"""
        imagename = "CAS7765.im"
        myia.fromshape(imagename, [30, 30, 30])
        myia.done()
        length = "14arcmin"
        center = [15, 15]
        outfile = "90deg_" + str(length) + ".im"
        impv(
            imagename=imagename, outfile=outfile,
            center=center, length=length, pa="90deg",
            mode="length"
        )
        self.assertTrue(os.path.exists(outfile))

        outfile = "270deg_" + str(length) + ".im"
        impv(
            imagename=imagename, outfile=outfile,
            center=center, length=length, pa="270deg",
            mode="length"
        )
        self.assertTrue(os.path.exists(outfile))

    def test_history(self):
        """Verify history is written to created image"""
        imagename = "zz.im"
        myia.fromshape(imagename, [30, 30, 30])
        length = "14arcmin"
        center = [15, 15]
        myia.done()

        outfile = "zz_out.im"
        impv(
            imagename=imagename, mode="length", center=center,
            length=length, pa="45deg", outfile=outfile
        )
        myia.open(outfile)
        msgs = myia.history()
        myia.done()
        teststr = "version"
        self.assertTrue(teststr in msgs[-2], "'" + teststr + "' not found")
        teststr = "impv"
        self.assertTrue(teststr in msgs[-1], "'" + teststr + "' not found")

if __name__ == '__main__':
    unittest.main()
