##########################################################################
# test_task_uvmodelfit.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.manipulation.uvmodelfit.html
#
#
##########################################################################
import sys
import os
import unittest
import shutil
import numpy as np

import casatools
from casatasks import uvmodelfit, casalog
tb = casatools.table()

### DATA ###
datapath = casatools.ctsys.resolve('unittest/uvmodelfit/Itziar.ms/')
spwpath = casatools.ctsys.resolve('unittest/uvmodelfit/ngc7538_ut.ms/')

datacopy = 'test.ms'
spwcopy = 'testspw.ms'
logpath = casalog.logfile()
logname= 'testlog.log'

def makecopy():

    shutil.copytree(datapath, datacopy)
    os.chmod(datacopy, 493)
    for root, dirs, files in os.walk(datacopy):
        for d in dirs:
            os.chmod(os.path.join(root, d), 493)
        for f in files:
            os.chmod(os.path.join(root, f), 493)

    shutil.copytree(spwpath, spwcopy)
    os.chmod(spwcopy, 493)
    for root, dirs, files in os.walk(spwcopy):
        for d in dirs:
            os.chmod(os.path.join(root, d), 493)
        for f in files:
            os.chmod(os.path.join(root, f), 493)

def get_col(data, col='Flux'):

    tb.open(data)
    res = tb.getcol(col)
    tb.close()

    return res

class uvmodelfit_test(unittest.TestCase):


    def setUp(self):
        makecopy()

    def tearDown(self):
        casalog.setlogfile(logpath)

        shutil.rmtree(datacopy)
        shutil.rmtree(spwcopy)

        if os.path.exists(logname):
            os.remove(logname)

        if os.path.exists('test1.cl'):
            shutil.rmtree('test1.cl')

        if os.path.exists('test2.cl'):
            shutil.rmtree('test2.cl')

    def test_takesvis(self):
        '''
            test_takesvis
            -----------------

            Check that the task takes a valid visibility file
        '''

        casalog.setlogfile(logname)
        uvmodelfit(vis=datacopy, field='0')

        self.assertFalse('SEVERE' in open(logname).read())

    def test_fieldSelect(self):
        '''
            test_fieldSelect
            -------------------

            Check that the field parameter properly selects fields by name or by id
            TODO find out what is wrong with selecting multiple fields
        '''

        uvmodelfit(vis=datacopy, field='0', outfile='test1.cl')
        uvmodelfit(vis=datacopy, field='1', outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))

    def test_spwSelect(self):
        '''
            test_spwSelect
            ----------------

            Check that the spw parameter properly selects spectral windows
        '''

        uvmodelfit(vis=spwcopy, spw='0', outfile='test1.cl')
        uvmodelfit(vis=spwcopy, spw='1', outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_selectData(self):
        '''
            test_selectData
            -----------------

            Allows for the use of the data selection parameters timerang, uvrange, antenna, scan, and msselect when True
        '''

        uvmodelfit(vis=datacopy, antenna='0~5&', field='1', selectdata=False, outfile='test1.cl')
        uvmodelfit(vis=datacopy, antenna='0~5&', field='1', selectdata=True, outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_timerangeSelect(self):
        '''
            test_timerangeSelect
            -----------------------

            Check that the timerange parameter properly selects the desired timerange
        '''

        uvmodelfit(vis=datacopy, timerange='10:40:46~10:41:36', outfile='test1.cl')
        uvmodelfit(vis=datacopy, timerange='10:45:46~10:46:36', outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_uvrangeSelect(self):
        '''
            test_uvrangeSelect
            --------------------

            Check that the uvrange parameter properly select the uvranges
        '''

        # Can't select a range? It seems this requires being able to select a number of multiple fields.

        uvmodelfit(vis=datacopy, uvrange='0~50', field='1', outfile='test1.cl')
        uvmodelfit(vis=datacopy, uvrange='50~100', field='1', outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_scanSelect(self):
        '''
            test_scanSelect
            -----------------

            Check that this parameter allows for seletion based on the scan id
        '''

        uvmodelfit(vis=datacopy, scan='1', outfile='test1.cl')
        uvmodelfit(vis=datacopy, scan='2', outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_antennaSelect(self):
        '''
            test_antennaSelect
            --------------------

            Check that this parameter allows for selection based on the antenna name or id
            NOTE: Using just the antenna selection will raise an error saying that multiple fields are being used
        '''

        uvmodelfit(vis=datacopy, antenna='1', field='1', outfile='test1.cl')
        uvmodelfit(vis=datacopy, antenna='2', field='1',outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    """
    def test_msSelect(self):
        '''
            test_msSelect
            ---------------

            Check that data can be selected by column and row in ms. Documentation parameter page says to ignore this for now.
        '''
    """

    def test_niter(self):
        '''
            test_niter
            -----------

            Check that the number of niter parameter selects the number of iterations uvmodelfit runs
        '''

        uvmodelfit(vis=datacopy, niter=1, field='1', outfile='test1.cl')
        uvmodelfit(vis=datacopy, niter=5, field='1', outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_comptype(self):
        '''
            test_comptype
            ---------------

            Check that the comptype parameter changes the component model type
        '''

        uvmodelfit(vis=datacopy, comptype='D', field='1', sourcepar=[1.0,1.0,1.0,1.0,1.0,1.0], outfile='test1.cl')
        uvmodelfit(vis=datacopy, comptype='G', field='1', sourcepar=[1.0,1.0,1.0,1.0,1.0,1.0], outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_sourcepar(self):
        '''
            test_sourcepar
            ----------------

            Check that sourcepar selects starting guess for component parameters
        '''

        uvmodelfit(vis=datacopy, field='1', sourcepar=[1.0,0.0,0.0], outfile='test1.cl')
        uvmodelfit(vis=datacopy, field='1', sourcepar=[1.0,1.0,1.0], outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_varypar(self):
        '''
            test_varypar
            --------------

            Check that varypar selects which parameters to let vary the fit
        '''

        uvmodelfit(vis=datacopy, field='1', sourcepar=[1.0,1.0,1.0], varypar=[True,True,True],outfile='test1.cl')
        uvmodelfit(vis=datacopy, field='1', sourcepar=[1.0,1.0,1.0], varypar=[True,False,False],outfile='test2.cl')

        res1 = get_col('test1.cl')
        res2 = get_col('test2.cl')

        self.assertFalse(np.all(np.isclose(res1, res2)))


    def test_outfile(self):
        '''
            test_outfile
            --------------

            Check that the task creates an output file when specified with the output parameter
        '''

        uvmodelfit(vis=datacopy, field='1', outfile='test1.cl')

        self.assertTrue(os.path.exists('test1.cl'))

if __name__ == '__main__':
    unittest.main()
