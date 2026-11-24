##########################################################################
# test_task_blcal.py
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
#
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.blcal.html
#
#
##########################################################################
import sys
import os
import unittest
import shutil
import numpy as np

import casatools
from casatasks import blcal, mstransform, casalog
tb = casatools.table()

# Data Path using new data repo
rootpath = casatools.ctsys.resolve('unittest/blcal/')


datapath = rootpath + 'gaincaltest2.ms'
refpath = rootpath + 'gaincaltest2.ms.G0'
calout = 'blcal.cal'
datacopy = 'blcaltestcopy.ms'

def getparam(caltable, colname='CPARAM'):

    tb.open(caltable)
    outtable = tb.getcol(colname)
    tb.close()

    return outtable

class blcal_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        shutil.copytree(datapath, datacopy)

    def tearDown(self):
        if os.path.exists(calout):
            shutil.rmtree(calout)
        if os.path.exists(datacopy):
            shutil.rmtree(datacopy)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_caltableOut(self):
        ''' Test that a caltable with the proper name is generated '''
        blcal(vis=datacopy, caltable=calout)
        self.assertTrue(os.path.exists(calout))

    def test_basicCalResult(self):
        ''' Test the result of a basic blcal result with default parameters '''

        blcal(vis=datacopy, caltable=calout)

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.4252234967607154+0.07460366999516284j)))

    def test_fieldSelect(self):
        ''' Test the field selection when creating the output table '''
        blcal(vis=datacopy, caltable=calout, field='0')

        tb.open(calout)
        result_length = len(tb.getcol('FIELD_ID'))
        tb.close()

        self.assertTrue(result_length == 220)

    def test_spwSelect(self):
        ''' Test the spw selection when creating the output table '''
        blcal(vis=datacopy, caltable=calout, spw='0')

        tb.open(calout)
        result_length = len(tb.getcol('SPECTRAL_WINDOW_ID'))
        tb.close()

        self.assertTrue(result_length == 1155)

    def test_intentSelect(self):
        # The gaincal test data doesnt have intents to select on?
        pass

    def test_selectDataEnable(self):
        ''' Test that disabling data selection ignores selection parameters '''
        blcal(vis=datacopy, caltable=calout, selectdata=False, scan='0')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.4252234967607154+0.07460366999516284j)))

    def test_uvrangeSelect(self):
        ''' Test that the uvrange selection parameter selects a subset of the data '''
        blcal(vis=datacopy, caltable=calout, uvrange='0~500klambda')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.30717815756958605+0.06031985813547712j)))

    def test_antennaSelect(self):
        ''' Test that the antenna selection parameter selects a subset of the data '''
        blcal(vis=datacopy, caltable=calout, antenna='1')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.08396210874708286+0.01500331772838062j)))

    def test_scanSelect(self):
        ''' Test that the scan selection parameter selects a subset of the data '''
        blcal(vis=datacopy, caltable=calout, scan='8')

        result = len(getparam(calout))
        self.assertTrue(result == 2)

    def test_observationSelect(self):
        ''' Test that the observation selection parameter selects a subset of the data '''
        # This data only has one observation with ID 0
        blcal(vis=datacopy, caltable=calout, observation='0')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.4252234967607154+0.07460366999516284j)))

    def test_solint(self):
        ''' Test that to solint parameter works properly '''
        blcal(vis=datacopy, caltable=calout, solint='60s')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.4251396369380383+0.07459191235066753j)))

    def test_combine(self):
        ''' Test that the combine correctly combines spectral windows '''
        blcal(vis=datacopy, caltable=calout, combine='spw')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.42522349675872373+0.07460367036948093j)))

    def test_freqdep(self):
        ''' Test that the freqdep True matches reference values'''
        blcal(vis=datacopy, caltable=calout, freqdep=True)

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.42522349749032323+0.0746036708774733j)))

    def test_solnorm(self):
        ''' Test that solutions are normalized '''
        blcal(vis=datacopy, caltable=calout, solnorm=True)

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.4296210607291418+0.07537533312484945j)))

    def test_gaintable(self):
        ''' Test that gaintable selects a table to pre-apply to the ms '''
        blcal(vis=datacopy, caltable=calout, gaintable=refpath)

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.6698459748491555+0.017203496349874073j)))

    def test_gainfield(self):
        ''' Test that a subset of the table is pre-applied based on field selection '''
        # Only has one field to select from
        blcal(vis=datacopy, caltable=calout, gaintable=refpath, gainfield='0')

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.6698459748491555+0.017203496349874073j)))

    def test_spwmap(self):
        ''' Test that spw map sets which spw from the calibrator to use on the ms '''
        blcal(vis=datacopy, caltable=calout, gaintable=refpath, spwmap=[0, 0, 1, 1])

        result = np.mean(getparam(calout))
        self.assertTrue(np.isclose(result, (0.6418628206012734+0.05439148921378219j)))

if __name__ == '__main__':
    unittest.main()
