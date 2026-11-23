#######################################################################
# test_task_listcal.py
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
# Based on the requirements listed in plone found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.information.listcal.html
#
#
##########################################################################
import os
import shutil
import unittest

import casatools
from casatasks import listcal #, casalog


reg_unittest_datap = 'unittest/listcal/'
datapath = casatools.ctsys.resolve(reg_unittest_datap)

# This is for tests that check what the parameter validator does when parameters are
# given wrong types - these don't exercise the task but the parameter validator!
validator_exc_type = AssertionError

def searchInFile(filename, searched):
    """
    Search for a substring in a file
    """
    with open(filename) as searched_file:
        for line in searched_file:
            if searched in line:
                return True
        return False


def getLine(filename, lineNum):

    counter = 0
    with open(filename) as fout:
        for line in fout:
            if counter == lineNum:
                return line
            else:
                counter += 1


class test_listcal_minimal(unittest.TestCase):
    """
    Most basic ways of calling listcal, with an existing small dataset from the
    'unittest' input data that is used in the bandpass and gaincal tests.
    This is just a start, to have some tests for the task listcal.
    """

    @classmethod
    def setUpClass(cls):
        vis_name = 'ngc5921.ms'
        caltable_name = 'ngc5921.ref1a.gcal'
        shutil.copytree(os.path.join(datapath,vis_name),vis_name)
        shutil.copytree(os.path.join(datapath,caltable_name),caltable_name)
        cls._vis = vis_name
        cls._caltable = caltable_name
        cls._listfile = 'listcal_listfile.txt'

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._vis)
        shutil.rmtree(cls._caltable)

    def tearDown(self):
        if os.path.isfile(self._listfile):
            os.remove(self._listfile)

    def test_small_gcal_nosel(self):
        """
        Test listcal with MS + gcal caltable, without any selection.
        """
        listcal(vis=self._vis, caltable=self._caltable, listfile=self._listfile)
        self.assertTrue(os.path.isfile(self._listfile))
        size = os.path.getsize(self._listfile)
        self.assertGreaterEqual(size, 10000)
        self.assertLessEqual(size, 12000)

    def test_small_gcal_sel_ant(self):
        """
        Test to have some additional tests with selections for the moment. Too basic.
        """
        listcal(vis=self._vis, caltable=self._caltable, antenna='VA02',
                listfile=self._listfile)
        self.assertTrue(os.path.isfile(self._listfile))
        size = os.path.getsize(self._listfile)
        self.assertGreaterEqual(size, 950)
        self.assertLessEqual(size, 1200)

    def test_small_gcal_forget_caltable(self):
        """
        Test proper error when listcal with MS + forget caltable.
        """
        with self.assertRaises(validator_exc_type):
            listcal(vis=self._vis, listfile=self._listfile)

        self.assertFalse(os.path.isfile(self._listfile))

    def test_small_gcal_forget_ms(self):
        """
        Test proper error when listcal caltable + forget MS.
        """
        with self.assertRaises(validator_exc_type):
            listcal(caltable=self._caltable, listfile=self._listfile)

        self.assertFalse(os.path.isfile(self._listfile))

    def test_small_gcal_wrong_antenna(self):
        """
        Test that gives a wrong selection (antenna).
        """
        with self.assertRaises(RuntimeError):
            listcal(vis=self._vis, caltable=self._caltable, antenna='inexistent-no-no')
        self.assertFalse(os.path.isfile(self._listfile))


class listcal_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        vis_name = 'ngc5921.ms'
        caltable_name = 'ngc5921.ref1a.gcal'
        reffile_name = 'reflistcal.txt'

        gainvis_name = 'gaincaltest2.ms'
        gaincaltable_name = 'gaincaltest2.ms.G0'
        fringefitvis_name = 'n08c1-single.ms'
        fringefitcaltable_name = 'n08c1.sbdcal'
        gaincurvevis_name = 'ba123a.ms'
        gaincurvecaltable_name = 'ba123a.gc'
        shutil.copytree(os.path.join(datapath,vis_name),vis_name)
        shutil.copytree(os.path.join(datapath,caltable_name),caltable_name)
        shutil.copytree(os.path.join(datapath, gainvis_name), gainvis_name)
        shutil.copytree(os.path.join(datapath, gaincaltable_name), gaincaltable_name)
        shutil.copyfile(os.path.join(datapath, reffile_name), reffile_name)

        shutil.copytree(os.path.join(datapath, fringefitvis_name), fringefitvis_name)
        shutil.copytree(os.path.join(datapath, fringefitcaltable_name), fringefitcaltable_name)
        shutil.copytree(os.path.join(datapath, gaincurvevis_name), gaincurvevis_name)
        shutil.copytree(os.path.join(datapath, gaincurvecaltable_name), gaincurvecaltable_name)

        cls._vis = vis_name
        cls._caltable = caltable_name
        cls._gainvis = gainvis_name
        cls._gaincaltable = gaincaltable_name
        cls._listfile = 'listcal_listfile.txt'
        cls._reffile = reffile_name
        cls._fringefitvis = fringefitvis_name
        cls._fringefitcaltable = fringefitcaltable_name
        cls._gaincurvevis = gaincurvevis_name
        cls._gaincurvecaltable = gaincurvecaltable_name

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._vis)
        shutil.rmtree(cls._caltable)
        shutil.rmtree(cls._gainvis)
        shutil.rmtree(cls._gaincaltable)
        shutil.rmtree(cls._fringefitvis)
        shutil.rmtree(cls._fringefitcaltable)
        shutil.rmtree(cls._gaincurvevis)
        shutil.rmtree(cls._gaincurvecaltable)
        os.remove(cls._reffile)

    def tearDown(self):
        if os.path.isfile(self._listfile):
            os.remove(self._listfile)

    def test_fieldSelection(self):
        """ Checks that specific fields can be selected """
        listcal(vis=self._vis, caltable=self._caltable, field='N5921_2', listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        self.assertFalse(searchInFile(self._listfile, '1331+30500002_0'), msg='Found fields that are not selected')
        self.assertTrue(searchInFile(self._listfile, 'N5921_2'), msg='did not find selected field')

    def test_antennaSelection(self):
        """ Checks that specific antenna can be selected """
        listcal(vis=self._vis, caltable=self._caltable, antenna='VA01', listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        self.assertFalse(searchInFile(self._listfile, 'VA02'), msg='Found anntennas that were not selected')
        self.assertTrue(searchInFile(self._listfile, 'VA01'), msg='The selected antenna was not found')

    def test_spwSelection(self):
        """ Check that specific spectral windows can be selected """
        listcal(vis=self._gainvis, caltable=self._gaincaltable, spw='1', listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        self.assertFalse(searchInFile(self._listfile, 'SpwID = 0'), msg='Found spw that was not selected')
        self.assertTrue(searchInFile(self._listfile, 'SpwID = 1'), msg='Did not find the selected spw')

    def test_headerLayout(self):
        """ Check that the header is layed out in the form described in the documentation """
        listcal(vis=self._gainvis, caltable=self._gaincaltable, listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        header = getLine(self._listfile, 1)

        self.assertTrue("SpwID" in header)
        self.assertTrue("Date" in header)
        self.assertTrue("CalTable" in header)
        self.assertTrue("MS name" in header)

    def test_spectralWindowHeader(self):
        """ Check that there is spectral window, date, caltable name, and ms name in the header """
        listcal(vis=self._gainvis, caltable=self._gaincaltable, listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        header = getLine(self._listfile, 16)

        self.assertTrue("SpwID" in header)
        self.assertTrue("Date" in header)
        self.assertTrue("CalTable" in header)
        self.assertTrue("MS name" in header)

    def test_gaincalHeaderColumnLabels(self):
        """ Check that the data columns are labeled as described """
        listcal(vis=self._gainvis, caltable=self._gaincaltable, listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        header = getLine(self._listfile, 4)
        self.assertTrue(header.startswith("Time       Field      Chn| Amp    Phs  F  Amp    Phs  F| Amp    Phs  F  Amp    Phs  F| Amp    Phs  F  Amp    Phs  F| Amp    Phs  F  Amp    Phs  F|"))

    def test_compareValues(self):
        """ Compare the values in the output to a reference output for the same table """
        listcal(vis=self._gainvis, caltable=self._gaincaltable, listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        with open(self._listfile) as file1:
            with open(self._reffile) as file2:
                diff = set(file1).difference(file2)

        # There are expected differences in the file paths recorded within the output files
        self.assertTrue(len(diff) == 4)


    def test_fringefitListing(self):
        """ Check that the data columns are labeled correctly for fringefit tables """
        listcal(vis=self._fringefitvis, caltable=self._fringefitcaltable, listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        header = getLine(self._listfile, 4)
        self.assertTrue(header.startswith("Time       Field      Chn|    Ph deg   F     Del ns   F   Rat ps/s   F   DDel uHz   F     Ph deg   F     Del ns   F   Rat ps/s   F   DDel uHz   F|"))

        line11=getLine(self._listfile, 11)
        self.assertTrue(line11.startswith("17:07:31.4    4C39.25   0| 131.76147       -8.66358       -0.05847        0.00000      -92.22026      -16.27947       -0.06037        0.00000"))


    def test_gaincurveListing(self):
        """ Check that the data columns are labeled correctly for gaincurve tables, and that first line of data is correct """
        listcal(vis=self._gaincurvevis, caltable=self._gaincurvecaltable, listfile=self._listfile)

        self.assertTrue(os.path.exists(self._listfile))

        header = getLine(self._listfile, 4)
        self.assertTrue(header.startswith("Time       Field Chn|      c[0]   F       c[1]   F       c[2]   F       c[3]   F       c[4]   F       c[5]   F       c[6]   F       c[7]   F       c[0]   F"))

        #  First line with actual interesting values
        line6=getLine(self._listfile, 6)
        self.assertTrue(line6.startswith("04:13:09.5         0|   0.09357        0.00226       -0.00003        0.00000        0.00000        0.00000        0.00000        0.00000  "))


if __name__ == '__main__':
    unittest.main()
