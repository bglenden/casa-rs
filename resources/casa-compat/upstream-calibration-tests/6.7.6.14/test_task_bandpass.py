#########################################################################
# test_task_bandpass.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.bandpass.html
#
##########################################################################
import os
import shutil
import numpy as np
import unittest

from casatools import ctsys
from casatasks import bandpass, flagdata
from casatestutils import testhelper as th

''' Python unit tests for the bandpass task

These tests will only verify if the bandpass calibration
tables created for an MS and an MMS agree. These are
not full unit tests for the bandpass task.
'''

datapath = ctsys.resolve('unittest/bandpass/')

# Pick up alternative data directory to run tests on MMSs
testmms = False
if 'TEST_DATADIR' in os.environ:
    DATADIR = str(os.environ.get('TEST_DATADIR'))+'/bandpass/'
    if os.path.isdir(DATADIR):
        testmms = True
        datapath = DATADIR
    else:
        print('WARN: directory '+DATADIR+' does not exist')

print('bandpass tests will use data from '+datapath)

# Base class which defines setUp functions
# for importing different data sets
class test_base(unittest.TestCase):

    def cleanUp(self):
        shutil.rmtree(self.msfile, ignore_errors=True)
        os.system('rm -rf '+self.msfile+'.bcal')

    def setUp_ngc5921(self):

        # Input names
        prefix = 'ngc5921'
        self.msfile = prefix + '.ms'
#        if testmms:
#            self.msfile = prefix + '.mms'

        self.reffile = os.path.join(datapath, prefix)
        self.cleanUp()

        fpath = os.path.join(datapath,self.msfile)
        if os.path.lexists(fpath):
            shutil.copytree(fpath, self.msfile)
        else:
            self.fail('Data does not exist -> '+fpath)

    def setUp_ngc4826(self):

        # Input names
        prefix = 'ngc4826'
        self.msfile = prefix + '.ms'
#        if testmms:
#            self.msfile = prefix + '.mms'

        self.reffile = os.path.join(datapath, prefix)
        self.cleanUp()

        fpath = os.path.join(datapath,self.msfile)
        if os.path.lexists(fpath):
            shutil.copytree(fpath, self.msfile)
        else:
            self.fail('Data does not exist -> '+fpath)

class bandpass1_test(test_base):

    def setUp(self):
        self.setUp_ngc5921()

    def tearDown(self):
        if os.path.lexists(self.msfile):
            shutil.rmtree(self.msfile)

        os.system('rm -rf ngc5921*.bcal')

    def test1a(self):
        '''Bandpass 1a: Create bandpass table using field=0'''
        msbcal = self.msfile + '.bcal'
        reference = self.reffile + '.ref1a.bcal'
        bandpass(vis=self.msfile, caltable=msbcal, field='0',uvrange='>0.0',
                 bandtype='B',solint='inf',combine='scan',refant='VA15')
        self.assertTrue(os.path.exists(msbcal))

        # Compare the calibration tables
        self.assertTrue(th.compTables(msbcal, reference, ['WEIGHT']))

    def test_returnDict(self):
        """ Test that the return value is a dictionary """
        msbcal = self.msfile + '.bcal'
        res = bandpass(vis=self.msfile, caltable=msbcal)
        self.assertTrue(type(res) == dict)

    def test_dictOutputFlagged(self):
        """ Test that when an spw is flagged the final data counts are zero """
        # Flag the spw
        flagdata(vis=self.msfile, spw='0')
        # Run bandpass
        msbcal = self.msfile + '.bcal'
        res = bandpass(vis=self.msfile, caltable=msbcal)
        toCheck = ['above_minblperant', 'above_minsnr', 'data_unflagged']

        for i in toCheck:
            self.assertTrue(np.all(res['solvestats']['spw0'][i] == 0))
        self.assertTrue(np.all(res['solvestats']['spw0']['expected'] > 0))

    def test_dictOutputAntennaFlag(self):
        """ Test that preflagging antennas shows in the output dict """
        # Flag the antenna
        flagdata(vis=self.msfile, antenna='0')
        # Run bandpass
        msbcal = self.msfile + '.bcal'
        res = bandpass(vis=self.msfile, caltable=msbcal)
        toCheck = ['above_minblperant', 'above_minsnr', 'data_unflagged', 'used_as_refant']

        for i in toCheck:
            self.assertTrue(np.all(res['solvestats']['spw0']['ant0'][i] == 0))
        self.assertTrue(np.all(res['solvestats']['spw0']['ant0']['expected'] > 0))

    def test_dictBelowMinBl(self):
        """ Test that results will reflect ants excluded due to missing baselines """
        flagdata(vis=self.msfile, antenna='0~24')
        msbcal = self.msfile + '.bcal'
        res = bandpass(vis=self.msfile, caltable=msbcal)

        for i in range(25, 28):
            ant = 'ant'+str(i)
            self.assertTrue(np.all(res['solvestats']['spw0'][ant]['above_minblperant'] == 0))


class bandpass2_test(test_base):

    def setUp(self):
        self.setUp_ngc4826()

    def tearDown(self):
        if os.path.lexists(self.msfile):
            shutil.rmtree(self.msfile)


        os.system('rm -rf ngc4826*.bcal')


    def test1b(self):
        '''Bandpass 1b: Create cal tables for the MS and MMS split by spws'''
        msbcal = self.msfile + '.bcal'
        reference = self.reffile + '.ref1b.bcal'
        bandpass(vis=self.msfile, caltable=msbcal, uvrange='>0.0',
                 field='0',spw='0',bandtype='B',
                 solint='inf',combine='scan',refant='ANT5')
        self.assertTrue(os.path.exists(msbcal))

        # Compare the calibration tables
        self.assertTrue(th.compTables(msbcal, reference, ['WEIGHT']))

if __name__ == '__main__':
    unittest.main()
