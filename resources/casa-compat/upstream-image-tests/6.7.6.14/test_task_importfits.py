#########################################################################
# test_task_importfits.py
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
# [Add the link to the JIRA ticket here once it exists]
#
# Based on the requirements listed in plone found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.data.importfits.html
#
#
##########################################################################
import os
import shutil
import unittest
import numpy as np

import casatools
from casatasks import importfits, exportfits, imhead
tb = casatools.table()
ia = casatools.image()

reg_unittest_datap = 'unittest/importfits/'
datapath = casatools.ctsys.resolve(reg_unittest_datap)
fitsdata = datapath + 'two_gaussian_model.fits'
multihdu = datapath + 'vogtstar_awt.fits'
axesdata = datapath + 'test_image2dconvolver.fits'
outpath = 'fitstestout.im'
outpath2 = 'fitstestout2.im'
genfitspath = 'genfits.fits'


class importfits_test(unittest.TestCase):

    def tearDown(self):
        if os.path.exists(outpath):
            shutil.rmtree(outpath)
        if os.path.exists(outpath2):
            shutil.rmtree(outpath2)
        if os.path.exists(genfitspath):
            os.remove(genfitspath)

    def test_createsCasaImage(self):
        ''' Check that the task creates a casa image from a fits image '''

        importfits(fitsimage=fitsdata, imagename=outpath)

        # Get stats to check from header
        header = imhead(outpath)
        imaxisnames = header['axisnames']
        imaxisunits = header['axisunits']
        imshape = header['shape']

        # Check a few stats from the header
        self.assertTrue(np.all(imaxisnames == ['Right Ascension', 'Declination', 'Frequency', 'Stokes']))
        self.assertTrue(np.all(imaxisunits == ['rad', 'rad', 'Hz', '']))
        self.assertTrue(np.all(imshape == [251, 251,   1,   1]))

    def test_whichrep(self):
        ''' Check that the whichrep parameter can change coordinate representation '''

        # Need a dataset with multiple reps
        importfits(fitsimage=fitsdata, imagename=outpath, whichrep=1)

        # get the shape and avg of the image data
        tb.open(outpath)
        mapavg = np.mean(tb.getcol('map'))
        mapshape = tb.getcol('map').shape
        tb.close()

        # Compare avg and shape to expected values
        self.assertTrue(np.isclose(mapavg, 2.7356038852669697))
        self.assertTrue(mapshape == (251, 251, 1, 1, 1))

    def test_whichHDU(self):
        ''' Check that specific header data can be selected if the fits contains multiple images '''

        importfits(fitsimage=multihdu, imagename=outpath, whichhdu=1)
        importfits(fitsimage=multihdu, imagename=outpath2, whichhdu=0)

        # Get the image compare
        tb.open(outpath)
        hdu1map = tb.getcol('map')
        tb.close()

        tb.open(outpath2)
        hdu0map = tb.getcol('map')
        tb.close()

        # check the shapes with imhead
        hdu1shape = imhead(outpath)['shape']
        hdu0shape = imhead(outpath2)['shape']

        # The two maps should not be the same
        self.assertFalse(np.array_equal(hdu1map, hdu0map))
        # check the shapes
        self.assertTrue(np.all(hdu1shape == [860]))
        self.assertTrue(np.all(hdu0shape == [512, 861]))

    def test_defaultAxes(self):
        """Check that default coordinate axes are added where they are missing"""

        importfits(fitsimage=axesdata, imagename=outpath, defaultaxes=True, defaultaxesvalues=['19h30m00', '-02d30m00', '88.5GHz', 'Q'])
        importfits(fitsimage=axesdata, imagename=outpath2, defaultaxes=False)

        # Get the two different images
        tb.open(outpath)
        withaxes = tb.getcol('map')
        tb.close()

        tb.open(outpath2)
        withoutaxes = tb.getcol('map')
        tb.close()

        self.assertFalse(np.array_equal(withaxes, withoutaxes))

    def test_overwrite(self):
        """Check that the task can only overwrite a table if overwrite=True"""

        importfits(fitsimage=fitsdata, imagename=outpath)
        # Run the task again and it should fail
        try:
            importfits(fitsimage=fitsdata, imagename=outpath)
        except RuntimeError:
            noOverwrite = False
        # This should return false
        self.assertFalse(noOverwrite)

        # Running again with overwrite should give no return value
        withOverwrite = importfits(fitsimage=fitsdata, imagename=outpath, overwrite=True)
        self.assertTrue(withOverwrite == None)

    def test_beam(self):
        """Provide values to be used with the synthesized beam"""

        importfits(fitsimage=fitsdata, imagename=outpath, beam=['0.35arcsec', '0.24arcsec', '25deg'])
        summary = imhead(outpath, mode='summary')
        beamresult = summary['restoringbeam']

        # Check that the image has a beam with the specified properties
        # Major Beam
        self.assertTrue(beamresult['major']['unit'] == 'arcsec')
        self.assertTrue(beamresult['major']['value'] == 0.35)
        # Minor Beam
        self.assertTrue(beamresult['minor']['unit'] == 'arcsec')
        self.assertTrue(beamresult['minor']['value'] == 0.24)
        # Position Angle
        self.assertTrue(beamresult['positionangle']['unit'] == 'deg')
        self.assertTrue(beamresult['positionangle']['value'] == 25.0)

    def test_zeroblanks(self):
        """Check that the zeroblanks parameter changes nan values to zeros"""

        # Generate an image full of nans
        genarray = ia.makearray(v=np.nan, shape=[64, 64, 4, 128])
        result = ia.fromarray(outfile=outpath, pixels=genarray)
        ia.done()
        # Convert to fits
        exportfits(imagename=outpath, fitsimage=genfitspath)
        # Convert back to casa image
        importfits(fitsimage=genfitspath, imagename=outpath2, zeroblanks=True)

        # Get the data
        tb.open(outpath2)
        imdata = tb.getcol('map')
        tb.close()

        # Check that all the nans are now 0s
        self.assertTrue(np.all(imdata == 0))

if __name__ == '__main__':
    unittest.main()
