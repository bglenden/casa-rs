##########################################################################
# test_task_feather.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.imaging.feather.html
#
#
##########################################################################
import os
import unittest
import shutil
import numpy as np

import casatools
from casatasks import feather, casalog
tb = casatools.table()
ia = casatools.image()

### DATA ###
datapath = casatools.ctsys.resolve('unittest/feather/')

#Input files
intimg = 'orion_tfeather.im'
sdimg = 'orion_tsdmem.image'
cube_int = 'papersky_standard.intimage'
cube_sd = 'papersky_standard.sdimage'

# Output files
output = 'feathered.im'
output2 = 'feathered2.im'

logpath = casalog.logfile()
logname = 'testlog.log'

def get_map(infile):

    tb.open(infile)
    res = tb.getcol('map')
    tb.close()

    return res

class feather_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        if not os.path.exists(intimg):
            os.symlink(os.path.join(datapath, intimg), intimg)
        if not os.path.exists(sdimg):
            os.symlink(os.path.join(datapath, sdimg), sdimg)
        if not os.path.exists(cube_int):
            os.symlink(os.path.join(datapath, cube_int), cube_int)
        if not os.path.exists(cube_sd):
            os.symlink(os.path.join(datapath, cube_sd), cube_sd)

    def tearDown(self):
        if os.path.exists(output):
            shutil.rmtree(output)

        if os.path.exists(output2):
            shutil.rmtree(output2)

        if os.path.exists(logname):
            os.remove(logname)

        casalog.setlogfile(logpath)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(intimg):
            os.unlink(intimg)
        if os.path.exists(sdimg):
            os.unlink(sdimg)
        if os.path.exists(cube_int):
            os.unlink(cube_int)
        if os.path.exists(cube_sd):
            os.unlink(cube_sd)

    def test_combine(self):
        '''
            test_combine
            --------------

            Check that interferometric and Single dish images can be combined
        '''

        feather(imagename=output, highres=intimg, lowres=sdimg)
        self.assertTrue(os.path.exists(output))

    def test_imagename(self):
        '''
            test_imagename
            ----------------

            Check that the imagename parameter gives the name of the output image file
        '''

        feather(imagename=output, highres=intimg, lowres=sdimg)
        feather(imagename=output2, highres=intimg, lowres=sdimg)

        self.assertTrue(os.path.exists(output))
        self.assertTrue(os.path.exists(output2))

    def test_highres(self):
        '''
            test_highres
            --------------

            Check that the interferometric image is provided with this parameter
            This parameter is nessisary to run the task
        '''

        with self.assertRaises(AssertionError):
            feather(imagename=output, lowres=sdimg)

#             casalog.setlogfile(logname)
#             feather(imagename=output, lowres=sdpath)
#             self.assertTrue(('SEVERE' in open(logname).read()))

    def test_lowres(self):
        '''
            test_lowres
            -------------

            Check that the single dish image is provided with this parameter
            This parameter is nessisary to run the task
        '''

        with self.assertRaises(AssertionError):
            feather(imagename=output, highres=intimg)

#             casalog.setlogfile(logname)
#             feather(imagename=output, highres=interpath)
#             self.assertTrue('SEVERE' in open(logname).read())


    def test_sdfactor(self):
        '''
            test_sdfactor
            ---------------

            Check that differing sdfactors results in differing image files
        '''

        feather(imagename=output, highres=intimg, lowres=sdimg)
        feather(imagename=output2, highres=intimg, lowres=sdimg, sdfactor=0.5)

        res1 = get_map(output)
        res2 = get_map(output2)

        self.assertFalse(np.all(np.isclose(res1, res2)))

    def test_effdishdiam(self):
        '''
            test_effdishdiam
            ------------------

            Check that chaging the effective dish diameter results in differing image files
        '''

        feather(imagename=output, highres=intimg, lowres=sdimg)
        feather(imagename=output2, highres=intimg, lowres=sdimg, effdishdiam=1)

        res1 = get_map(output)
        res2 = get_map(output2)

        self.assertFalse(np.all(np.isclose(res1, res2)))

        with self.assertRaises(RuntimeError):
            feather(imagename=output2, highres=intimg, lowres=sdimg, effdishdiam=1000)

    def test_lowpassfiltersd(self):
        '''
            test_lowpassfiltersd
            ----------------------

            Check that lowpassfiltersd = True results in a different image than the default
        '''

        feather(imagename=output, highres=intimg, lowres=sdimg)
        feather(imagename=output2, highres=intimg, lowres=sdimg, lowpassfiltersd=True)

        res1 = get_map(output)
        res2 = get_map(output2)

        self.assertFalse(np.all(np.isclose(res1, res2)))

    #@unittest.skip("Skip test until INT cube makes it to the test data repo")
    def test_perplanebeam(self):
        '''
            test_perplanebeam
            ----------------------
            Check that feather works with cubes with perplanebeams
        '''
        feather(imagename=output, highres=cube_int, lowres=cube_sd)
        self.assertTrue(os.path.exists(output))


    #@unittest.skip("Skip test until INT cube makes it to the test data repo")
    def test_common_and_mixed_beam(self):
        '''
            test_common_and_mixed_beams
            ----------------------
            Check that feather works with cubes with a common restoring beam for all channels
            Also, if either one image has per-plane beams, it should default to per-plane feathering.
        '''
        onebeam_sd = 'onebeam_sd.im'
        onebeam_int = 'onebeam_int.im'

        if os.path.exists(onebeam_sd):
            shutil.rmtree(onebeam_sd)
            #shutil.copytree(sdcube,onebeam_sd,symlinks=False)
        if os.path.exists(onebeam_int):
            shutil.rmtree(onebeam_int)
            #shutil.copytree(intcube,onebeam_int,symlinks=False)

        os.system('cp -Lr ' + cube_sd + ' ' + onebeam_sd)
        os.system('cp -Lr ' + cube_int + ' ' + onebeam_int)

        ## Set single restoring beam for INT cube
        ia.open(onebeam_int)
        rb = ia.restoringbeam()['beams']['*0']['*0']
        ia.setrestoringbeam(remove=True)
        ia.setrestoringbeam(beam=rb)
        ia.close()

        ## Set single restoring beam for SD cube
        ia.open(onebeam_sd)
        ia.setrestoringbeam(remove=True)
        ia.setrestoringbeam(beam=rb)
        ia.close()

        ## Test both with commonbeam
        feather(imagename=output, highres=onebeam_int, lowres=onebeam_sd)
        res1 = os.path.exists(output)

        ## Test with SD with perplane beams and INT with a common beam. ( Not testing the third combination )
        feather(imagename=output, highres=onebeam_int, lowres=cube_sd)
        res2 = os.path.exists(output)

        if os.path.exists(onebeam_sd):
            shutil.rmtree(onebeam_sd)
        if os.path.exists(onebeam_int):
            shutil.rmtree(onebeam_int)

        self.assertTrue(res1 and res2 )


if __name__ == '__main__':
    unittest.main()
