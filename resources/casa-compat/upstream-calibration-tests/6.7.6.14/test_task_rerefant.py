##########################################################################
# test_task_rerefant.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.rerefant.html
#
#
##########################################################################
import sys
import os
import unittest
import shutil
import numpy as np

import casatools
from casatasks import rerefant, casalog, fringefit, flagdata
tb = casatools.table()

#import pyfits

## DATA ##
casavis = casatools.ctsys.resolve('unittest/rerefant/ngc5921.ms/')
casacal = casatools.ctsys.resolve('unittest/rerefant/ngc5921.ref1a.gcal')
src = casatools.ctsys.resolve('unittest/rerefant/n08c1.ms')

copyvis = 'vis.ms'
copycal = 'copycal.gcal'

logpath = casalog.logfile()

def file_copy(filename, perm):
    os.chmod(filename, perm)
    for root, dirs, files in os.walk(filename):
        for d in dirs:
            os.chmod(os.path.join(root, d), perm)
        for f in files:
            os.chmod(os.path.join(root, f), perm)

class rerefant_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        shutil.copytree(casavis, copyvis)
        shutil.copytree(casacal, copycal)
        file_copy(copyvis, 493)
        file_copy(copycal, 493)

    def tearDown(self):
        shutil.rmtree(copyvis)
        shutil.rmtree(copycal)
        casalog.setlogfile(logpath)

        if os.path.exists('testlog.log'):
            os.remove('testlog.log')
        if os.path.exists('out.cal'):
            shutil.rmtree('out.cal')
        if os.path.exists('n08c1_reref.ms'):
            shutil.rmtree('n08c1_reref.ms')
        if os.path.exists("fringe.cal"):
            shutil.rmtree("fringe.cal")
        if os.path.exists("reref.cal"):
            shutil.rmtree("reref.cal")

    @classmethod
    def tearDownClass(cls):
        pass

    def test_listPrioritizedFlex(self):
        ''' Test that the first item in refants is used with no drop out '''

        rerefant(vis=copyvis, tablein=copycal, caltable='out.cal', refant='VA01', refantmode='flex')

        tb.open('out.cal')
        antennaCol = tb.getcol('ANTENNA2')
        tb.close()

        self.assertTrue(np.all(0 == antennaCol))

    def test_listPrioritizedDropsFlex(self):
        ''' Test that the next item in refants is used if the first antenna drops out '''

        flagdata(copycal, antenna='VA01')

        rerefant(vis=copyvis, tablein=copycal, caltable='out.cal', refant='VA01,VA02', refantmode='flex')

        tb.open('out.cal')
        antennaCol = tb.getcol('ANTENNA2')
        tb.close()

        self.assertTrue(np.all(1 == antennaCol))

    def test_listDropOutDropInFlex(self):
        ''' Test that when an antenna drops out and drops back in '''

        flagdata(copycal, scan='1~3', antenna='VA01')

        rerefant(vis=copyvis, tablein=copycal, caltable='out.cal', refant='VA01,VA02', refantmode='flex')

        tb.open('out.cal')
        antennaCol = tb.getcol('ANTENNA2')
        tb.close()

        self.assertFalse(np.all(1 == antennaCol))

    def test_refantPreferredStrict(self):
        ''' Test the strict application of a reference antenna '''

        rerefant(vis=copyvis, tablein=copycal, caltable='out.cal', refant='VA01', refantmode='strict')

        tb.open('out.cal')
        antennaCol = tb.getcol('ANTENNA2')
        tb.close()

        self.assertTrue(np.all(0 == antennaCol))

    def test_absentRefantFlagAllStrict(self):
        ''' Test that all antennas are flagged when the refant drops out in mode strict '''
        flagdata(copycal, antenna='VA01')

        rerefant(vis=copyvis, tablein=copycal, caltable='out.cal', refant='VA01', refantmode='strict')

        tb.open('out.cal')
        antennaCol = tb.getcol('ANTENNA2')
        tb.close()

        self.assertTrue(np.all(-1 == antennaCol))

    def test_listStrict(self):
        ''' Test that only the first refant is used when mode is strict '''
        flagdata(copycal, antenna='VA01')

        rerefant(vis=copyvis, tablein=copycal, caltable='out.cal', refant='VA01, VA02', refantmode='strict')

        tb.open('out.cal')
        antennaCol = tb.getcol('ANTENNA2')
        tb.close()

        self.assertTrue(np.all(-1 == antennaCol))

    # old test case

    def test_fringefit(self):
        dst = "n08c1_reref.ms"
        calfile = "fringe.cal"
        recalfile = "reref.cal"

        shutil.copytree(src, dst)

        # Perform fringe fit with EF as the reference station.
        fringefit(vis=dst, caltable=calfile, refant="EF")
        self.assertTrue(os.path.exists(calfile))

        # Rereference the results using ON as the new reference station.
        rerefant(vis=dst, tablein=calfile, caltable=recalfile, refant="ON")
        self.assertTrue(os.path.exists(recalfile))

        # Check original calibration table.
        tb.open(calfile)
        ant2=tb.getcol('ANTENNA2')
        taql="ANTENNA1 == 3 && ANTENNA2 == 0"
        tsel=tb.query(taql)
        fparam=tsel.getcol('FPARAM')
        tb.close()
        # Reference anttena is EF, aka antenna number 0.
        self.assertTrue(len(set(ant2)) == 1)
        self.assertTrue(0 in set(ant2))

        # Check rereferenced calibration table.
        tb.open(recalfile)
        ant2=tb.getcol('ANTENNA2')
        taql="ANTENNA1 == 0 && ANTENNA2 == 3"
        tsel=tb.query(taql)
        refparam=tsel.getcol('FPARAM')
        tb.close()
        # Reference anttena is ON, aka antenna number 3.
        self.assertTrue(len(set(ant2)) == 1)
        self.assertTrue(3 in set(ant2))

        # Parameters on EF-ON baseline should be opposite.
        self.assertTrue(np.isclose(refparam, -fparam, 1e-15).all())

        # Rereferenced parameters for ON should all be zero.
        tb.open(recalfile)
        taql="ANTENNA1 == 3 && ANTENNA2 == 3"
        tsel=tb.query(taql)
        refparam=tsel.getcol('FPARAM')
        tb.close()
        self.assertTrue(not refparam.any())

        # Rereferenced parameters for EF should not be zero.
        tb.open(recalfile)
        taql="ANTENNA1 == 0 && ANTENNA2 == 3"
        tsel=tb.query(taql)
        refparam=tsel.getcol('FPARAM')
        tb.close()
        self.assertTrue(refparam.any())

if __name__ == '__main__':
    unittest.main()

