##########################################################################
# test_task_ft.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.imaging.ft.html
#
# Test_logreturn checks to make sure a logfile is generated and populated
# Test_dictreturn checks that the result is a python dict object containing keys specified in the documentation
# Test_takescal checks that a caltable is accepted and non-cal tables are rejected
# Test_axis checks that different axis vaules will provide different information
# Test_axisvals checks that the values for axis provided in the documentatin are accepted as valid values
# Test_datacolumn checks that different datacolumn values provide different information
#
##########################################################################
import os
import unittest
import shutil
import numpy as np

import casatools
from casatasks import ft
from casatools import table, componentlist
tb = table()
cl = componentlist()

# Gaincaltest and gaussian model with noise
# Need model w/o standard gridder
# Need new model for model/complist preference?

datapath = casatools.ctsys.resolve('unittest/ft/uid___X02_X3d737_X1_01_small.ms')
modelpath = casatools.ctsys.resolve('unittest/ft/uid___X02_X3d737_X1_01_small.model')
simdata = casatools.ctsys.resolve('unittest/ft/ft_test_simulated.ms')
simcomplist = casatools.ctsys.resolve('unittest/ft/ft_test_simulated_complist.cl')
simmodel = casatools.ctsys.resolve('unittest/ft/ft_test_simulated_image.im')
multiterm0 = casatools.ctsys.resolve('unittest/ft/ft_test_multiterm.model.tt0')
multiterm1 = casatools.ctsys.resolve('unittest/ft/ft_test_multiterm.model.tt1')

datacopy = 'ft_test_copy.ms'
modelcopy = 'ft_test_model_copy.model'
nonStandardGridCopy = 'ft_test_nonstandard_gridder.model'
simdatacopy = 'ft_test_simdata.ms'
simcomplistcopy = 'ft_test_simcomplist.cl'

ftcomponentlist = 'ft_test_comp_list.cl'

def getColList(table):
    tb.open(table)
    columns = tb.colnames()
    tb.close()

    return columns

class ft_test(unittest.TestCase):

    def setUp(self):
        if not os.path.exists(datacopy):
            shutil.copytree(datapath, datacopy)
        if not os.path.exists(modelcopy):
            shutil.copytree(modelpath, modelcopy)
        if not os.path.exists(simdatacopy):
            shutil.copytree(simdata, simdatacopy)
        if not os.path.exists(simcomplistcopy):
            shutil.copytree(simcomplist, simcomplistcopy)

    def tearDown(self):
        shutil.rmtree(datacopy)
        shutil.rmtree(modelcopy)
        shutil.rmtree(simdatacopy)
        shutil.rmtree(simcomplistcopy)

        if os.path.exists(ftcomponentlist):
            shutil.rmtree(ftcomponentlist)

    def test_takesModel(self):
        ''' Test that a MODEL_DATA column is added to the MS when a *.model is provided '''
        ft(vis=datacopy, model=modelcopy, usescratch=True)

        # Find the MODEL_DATA column
        columns = getColList(datacopy)
        # Make sure MODEL_DATA is in the MS ONLY IF USESCRATCH=TRUE
        self.assertTrue('MODEL_DATA' in columns, msg='No MODEL_DATA added to the MS')

    def test_noScratchCol(self):
        ''' Test that if usescratch is false then a SOURCE_MODEL column is generated in the SOURCE table '''
        # Create the complist to use in the test
        cl.addcomponent(shape='point', flux=1, fluxunit='Jy', spectrumtype='spectral index',
        index=-0.8, freq='1,23GHz', dir='J2000 12h33m45.3s -23d01m11.2s')
        cl.rename(ftcomponentlist)
        cl.close()

        ft(vis=datacopy, complist=ftcomponentlist, usescratch=False)

        columns = getColList(datacopy + '/SOURCE')

        # SOURcE_MODEL col should be generated
        self.assertTrue('SOURCE_MODEL' in columns)


    def test_useScratch(self):
        ''' Test that when usescratch=True the model visibilites are stored in the MODEL_DATA column '''
        # first test that no MODEL_DATA column is created when usescratch = False
        ft(datacopy, model=modelcopy, usescratch=False)

        # get the columns list
        columns = getColList(datacopy)
        # Make sure there is no MODEL_DATA
        self.assertFalse('MODEL_DATA' in columns)

        ft(vis=datacopy, model=modelcopy, usescratch=True)

        # Find the MODEL_DATA column
        columns = getColList(datacopy)
        # Check that the MODEL_DATA column exists
        self.assertTrue('MODEL_DATA' in columns)

    def test_takesComponentList(self):
        ''' Test that a MODEL_DATA column is added to the MS when a component list is provided '''
        # Create the complist to use in the test
        cl.addcomponent(shape='point', flux=1, fluxunit='Jy', spectrumtype='spectral index',
                        index=-0.8, freq='1,23GHz', dir='J2000 12h33m45.3s -23d01m11.2s')
        cl.rename(ftcomponentlist)
        cl.close()
        # Run the ft command
        ft(vis=datacopy, complist=ftcomponentlist, usescratch=True)

        # Find the MODEL_DATA column
        columns = getColList(datacopy)
        # Check that the MODEL_DATA column exists
        self.assertTrue('MODEL_DATA' in columns)

    def test_addModel(self):
        ''' Test that with incremental=True the new model will be added instead of replacing the old one '''
        ft(vis=simdatacopy, model=simmodel, usescratch=True)

        tb.open(simdatacopy)
        originalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        ft(vis=simdatacopy, model=simmodel, usescratch=True, incremental=True)

        tb.open(simdatacopy)
        incrementalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertFalse(np.isclose(originalMean, incrementalMean))

    def test_componentListModelPriority(self):
        ''' Test that when a model and comp list are provided only the model is used '''
        # Test first with just the model
        ft(vis=simdatacopy, model=simmodel, usescratch=True)

        tb.open(simdatacopy)
        justmodel = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        # Now give both model and complist
        ft(vis=simdatacopy, model=simmodel, complist=simcomplistcopy, usescratch=True)

        tb.open(simdatacopy)
        bothmodelcomp = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(bothmodelcomp, justmodel))

    def test_multiTerm(self):
        ''' Test that the ft task accepts multi-term data '''
        # run with multi-term models
        ft(vis=simdatacopy, model=[multiterm0, multiterm1], usescratch=True)
        # get list of columns in the ms
        columns = getColList(simdatacopy)
        # Check that the MODEL_DATA column has been generated
        self.assertTrue('MODEL_DATA' in columns)

    def test_modelReplace(self):
        ''' When incremental = False the existing model should be replaced in MODEL_DATA '''
        # Create the complist to use in the test
        cl.addcomponent(shape='point', flux=1, fluxunit='Jy', spectrumtype='spectral index',
                        index=-0.8, freq='1,23GHz', dir='J2000 12h33m45.3s -23d01m11.2s')
        cl.rename(ftcomponentlist)
        cl.close()

        # Run ft with the complist
        ft(vis=datacopy, complist=ftcomponentlist, usescratch=True)

        # Get the mean of the MODEL_DATA
        tb.open(datacopy)
        originalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        # Run ft with a new model
        ft(vis=datacopy, model=modelcopy, usescratch=True)

        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertFalse(np.isclose(finalMean, originalMean))

    def test_spwSelection(self):
        ''' Test spw selection parameter '''
        ft(vis=datacopy, model=modelcopy, usescratch=True, spw='0')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.0001953125+0j)))

    def test_fieldSelection(self):
        ''' Test the field selection parameter '''
        ft(vis=datacopy, model=modelcopy, usescratch=True, field='0')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.11119791666666667+0j)))

    def test_timeSelection(self):
        ''' Test the time selection parameter '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, timerange='03:01:35.76~03:05:40.90')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.09452112268518519+0j)))

    def test_timeSelectionNoscratch(self):
        ''' Test the time selection parameter without usescratch'''

        with self.assertRaises(ValueError):
            ft(vis=datacopy, model=modelcopy, timerange='03:01:35.76~03:05:40.90')

    def test_uvrangeSelection(self):
        ''' Test the uvrange selection parameter '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, uvrange='150~180m')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.24556206597222222+0j)))

    def test_uvrangeSelectionNoscratch(self):
        ''' Test the uvrange selection parameter without usescratch'''

        with self.assertRaises(ValueError):
            ft(vis=datacopy, model=modelcopy, uvrange='150~180m')

    def test_antennaSelection(self):
        ''' Test the antenna selection parameter '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, antenna='!2')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.0833984375+0j)))

    def test_antennaSelectionNoscratch(self):
        ''' Test the antenna selection parameter without usescratch'''

        with self.assertRaises(ValueError):
            ft(vis=datacopy, model=modelcopy, antenna='!2')
    def test_scanSelection(self):
        ''' Test the scan selection parameter '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, scan='1')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.11119791666666667+0j)))

    def test_scanSelectionNoscratch(self):
        ''' Test the scan selection parameter without usescratch'''

        with self.assertRaises(ValueError):
            ft(vis=datacopy, model=modelcopy, scan='1')

    def test_observationSelection(self):
        ''' Test the observation selection parameter '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, observation='0')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.+0j)))

    def test_observationSelectionNoscratch(self):
        ''' Test the observation selection parameter without usescratch'''

        with self.assertRaises(ValueError):
            ft(vis=datacopy, model=modelcopy, observation='1,2')

    def test_intentSelection(self):
        ''' Test the intent selection parameter '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, intent='1,2')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0.19459635416666668+0j)))

    def test_intentSelectionNoscratch(self):
        ''' Test the intent selection parameter without usescratch'''

        with self.assertRaises(ValueError):
            ft(vis=datacopy, model=modelcopy, intent='1,2')

    def test_severalConsecutiveSelections(self):
        ''' Test that after applying several selections
            the MODEL_DATA is build up completely, by covering all scans '''

        ft(vis=datacopy, model=modelcopy, usescratch=True, scan='1')
        ft(vis=datacopy, model=modelcopy, usescratch=True, scan='2')
        ft(vis=datacopy, model=modelcopy, usescratch=True, scan='3')

        # get the mean value of MODEL_DATA
        tb.open(datacopy)
        finalMean = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(finalMean, (0+0j)))

    # Test for nterms and reffreq, requires additional models

    def test_plp_support(self):
        ''' CAS-13439: Test that componentlist plp spectral model is supported '''
        # Create the complist to use in the test
        reffreq = 8.53057441e10
        rf = str(reffreq) + 'Hz'
        index = [-0.8, 99.9, 99.9]
        cl.addcomponent(
            shape='point', flux=1, fluxunit='Jy', spectrumtype='plp',
            index=index, freq=rf, dir='J2000 12h33m45.3s -23d01m11.2s'
        )
        cl.rename(ftcomponentlist)
        cl.close()
        # Run the ft command
        ft(vis=datacopy, complist=ftcomponentlist, usescratch=True)

        # Find the MODEL_DATA column
        columns = getColList(datacopy)
        # Check that the MODEL_DATA column exists
        self.assertTrue('MODEL_DATA' in columns)

        tb.open(datacopy)
        data = np.real(tb.getcol('MODEL_DATA')[0, :, 0])
        tb.done()
        tb.open(datacopy + '/SPECTRAL_WINDOW')
        freqs = tb.getcell('CHAN_FREQ', 0)
        tb.done()
        x = freqs/reffreq
        lx = np.log(x)
        expec = x**(index[0] + index[1]*lx + index[2]*lx*lx)
        self.assertTrue(np.allclose(data, expec), 'Incorrect intensities')

if __name__ == '__main__':
    unittest.main()

