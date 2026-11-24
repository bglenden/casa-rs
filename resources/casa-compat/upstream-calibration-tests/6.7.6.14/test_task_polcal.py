##########################################################################
# test_task_polcal.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.polcal.html
#
#
##########################################################################
import sys
import os
import unittest
import shutil
import numpy as np

import casatools
from casatasks import polcal, casalog
tb = casatools.table()
dataroot = casatools.ctsys.resolve('unittest/polcal/')

### DATA ###
datapath = os.path.join(dataroot, 'polcal_LINEAR_BASIS.ms')
calpathLin = os.path.join(dataroot, 'polcal_LINEAR_BASIS.ms.Dtrue')
# circular data
datapathCirc = os.path.join(dataroot, 'polcal_CIRCULAR_BASIS.ms')
calpathCirc = os.path.join(dataroot, 'polcal_CIRCULAR_BASIS.ms.Dtrue')


def getparam(caltable, colname='CPARAM'):
    ''' Open a caltable and get the provided column '''

    tb.open(caltable)
    outtable = tb.getcol(colname)
    tb.close()

    return outtable


def tableComp(table1, table2, cols=[], rtol=8e-7, atol=1e-8):
    ''' Compare two caltables '''

    tableVal1 = {}
    tableVal2 = {}

    tb.open(table1)
    colname1 = tb.colnames()

    for col in colname1:
        try:
            tableVal1[col] = tb.getcol(col)
        except RuntimeError:
            pass
    tb.close()

    tb.open(table2)
    colname2 = tb.colnames()

    for col in colname2:
        try:
            tableVal2[col] = tb.getcol(col)
        except RuntimeError:
            pass
    tb.close()

    truthDict = {}

    for col in tableVal1.keys():

        try:
            truthDict[col] = np.isclose(tableVal1[col], tableVal2[col], rtol=rtol, atol=atol)
        except TypeError:
            print(col, 'ERROR in finding truth value')
            casalog.post(message=col + ': ERROR in determining the truth value')

    if len(cols) == 0:

        truths = [[x, np.all(truthDict[x] == True)] for x in truthDict.keys()]

    else:

        truths = [[x, np.all(truthDict[x] == True)] for x in cols]

    return np.array(truths)


def getSelection(table, field='0', spw='0'):

    output = []

    tb.open(table)
    fields = tb.getcol('FIELD_ID')
    spws = tb.getcol('SPECTRAL_WINDOW_ID')
    results = tb.getcol('CPARAM')

    # give the cparam result for selected field and spw
    for i in range(tb.nrows()):
        if fields[i] == int(field) and spws[i] == int(spw):
            output.append(results[:,:,i])

    tb.close()

    return output


outcal = 'polcalTestOutput.ms.Df'
datacopyLin = 'polcalTestCopy.ms'
datacopyCirc = 'polcalTestCopyCirc.ms'


class polcal_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        # Use a copy of the MS
        shutil.copytree(datapath, datacopyLin)
        shutil.copytree(datapathCirc, datacopyCirc)

    def tearDown(self):
        # After each test remove the copy and the output cal table
        if os.path.exists(datacopyLin):
            shutil.rmtree(datacopyLin)
        if os.path.exists(datacopyCirc):
            shutil.rmtree(datacopyCirc)
        if os.path.exists(outcal):
            shutil.rmtree(outcal)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('polcalTestOutput.ms.Df.PA-rel/')
        shutil.rmtree('polcalTestOutput.ms.Df.XfpaQU-rel/')

    def test_noPolDfllsLin(self):
        ''' Test on field 0 and spw 0. Use channelized solutions '''

        polcal(vis=datacopyLin, caltable=outcal, field='0', spw='0', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.0, 0.0, 0.0], refant='5')

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal

        self.assertTrue(np.all(np.isclose(calresult, 0)))

    def test_noPolDllsLin(self):
        ''' Test on Field 0 and spw 0 there should be no source or instrumental pol. This is probably not a useful case '''

        polcal(vis=datacopyLin, caltable=outcal, field='0', spw='0', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Dlls',
               smodel=[1.0, 0.0, 0.0, 0.0], refant='5')

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, :10]
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, refresult)))

    def test_sourcePolDfllsLin(self):
        ''' Test one Field 1 and spw 0. Channelized solution '''

        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='0', solint='inf', combine='scan',
               preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # Need an unchannelized version of the cal to compare to?

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal

        self.assertTrue(np.all(np.isclose(calresult, 0)))

    def test_sourcePolDllsLin(self):
        ''' Test on Field 1 and spw 0. Should return the source pol '''

        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='0', solint='inf', combine='scan',
               preavg=101.0, minsnr=0.0, poltype='Dlls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # Need an unchannelized version of the cal to compare to?

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, :10]
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, refresult)))

    def test_onlyInstPolDfllsLin(self):
        ''' Test on Field 0 and spw 1. Use channelized solutions '''

        polcal(vis=datacopyLin, caltable=outcal, field='0', spw='1', solint='inf', combine='scan',
               preavg=101.0, minsnr=0.0, poltype='Dflls', refant='5',
               smodel=[1.0, 0.0, 0.0, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 10:20]
        tb.close()

        resDiff = calresult-refresult

        self.assertTrue(np.all(np.isclose(resDiff, 0)))

    def test_onlyInstPolDllsLin(self):
        ''' Test on Field 0 and spw 1. Should clear inst pol and see no source pol. needs refant '''

        polcal(vis=datacopyLin, caltable=outcal, field='0', spw='1', solint='inf', combine='scan',
               preavg=101.0, minsnr=0.0, poltype='Dlls', refant='5',
               smodel=[1.0, 0.0, 0.0, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 10:20]
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, refresult)))

    def test_sourceInstPolDfllsLin(self):
        ''' Test on Field 1 and spw 1. Use channelized solutions '''

        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf', combine='scan',
               preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 10:20]
        tb.close()

        np.all(np.isclose(calresult, refresult))

    def test_sourceInstPolDllsLin(self):
        ''' Test on Field 1 and spw 1. Should see source pol '''

        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf', combine='scan',
               preavg=101.0, minsnr=0.0, poltype='Dlls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 10:20]
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, refresult)))

    def test_sourceInstPolNoiseLin(self):
        ''' Test on Field 1 and spw 2 or 3. Should see source pol with noise '''
        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='2', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 20:30]
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, refresult, atol=9e-4)))

    def test_combineSpwLin(self):
        ''' Test that combine spws increases the SNR/ decreases the noise '''

        # Combine no channels
        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='2,3', solint='inf,1ch',
               combine='scan,spw', preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 20:40]
        noNoise = tb.getcol('CPARAM')[:, :, 10:20]
        print(refresult.shape)
        tb.close()

        combinedOneChan = np.mean(calresult[:, 0, :])

        # Clear old output
        shutil.rmtree(outcal)

        # Combine 4 channels
        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='2,3', solint='inf,2ch',
               combine='scan,spw', preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        calresult = getparam(outcal)
        combinedTwoChan = np.mean(calresult[:, 0, :])

        # Clear old output
        shutil.rmtree(outcal)

        # Combine all channels
        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='2,3', solint='inf,8ch',
               combine='scan,spw', preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        calresult = getparam(outcal)

        # Differences
        oneDiff = (combinedOneChan - np.mean(noNoise))
        twoDiff = (combinedTwoChan - np.mean(noNoise))

        meanDiff = np.mean(calresult) - np.mean(refresult)

        self.assertTrue(np.isclose((twoDiff-(oneDiff*np.sqrt(2))), 0, atol=4e-4))
        self.assertTrue(np.isclose(meanDiff, 0, atol=1e-4))

    def test_noSourceInstPolNoiseLin(self):
        ''' Test on Field 0 and spw 2 or 3. Should just see noise '''
        polcal(vis=datacopyLin, caltable=outcal, field='0', spw='3', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Dflls',
               smodel=[1.0, 0.08, 0.06, 0.0])

        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathLin)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 30:40]
        tb.close()

        meanDifference = np.mean(calresult - refresult)

        self.assertTrue(np.all(np.isclose(meanDifference, 0, atol=4e-3)))

    def test_poltypeXfLin(self):
        ''' Test poltype Xf and assume correct and negated QU '''

        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Xf',
               smodel=[1.0, 0.08, 0.06, 0.0], gaintable=[calpathLin])

        calresult = getparam(outcal)

        self.assertTrue(np.all(np.isclose(calresult, [1 + 0j])))

        shutil.rmtree(outcal)
        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Xf',
               smodel=[1.0, -0.08, -0.06, 0.0], gaintable=[calpathLin])

        calresult = getparam(outcal)

        self.assertTrue(np.all(np.isclose(calresult, [-1 + 0j])))


    def test_poltypeXfParangQULin(self):
        ''' Test poltypeXfParang+QU and assume the correct QU '''

        resQU = polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='Xfparang+QU',
               smodel=[1.0, 0.08, 0.06, 0.0], gaintable=[calpathLin])

        # Try flipping the signs here too for smodel
        calresult = getparam(outcal)

        self.assertTrue(np.all(np.isclose(calresult, [1+0j])))
        self.assertTrue(np.all(np.isclose([1.0, 0.08, 0.06, 0.0], resQU['J2354-3600']['SpwAve'], atol=1e-4)))

    def test_posAngLin(self):
        ''' Test poltype PosAng and assume the correct QU '''

        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='PosAng',
               smodel=[1.0, 0.08, 0.06, 0.0], gaintable=[calpathLin])

        tb.open(outcal)
        calresult = tb.getcol('FPARAM')
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, 0)))

        #with more interesting PosAng
        polcal(vis=datacopyLin, caltable=outcal, field='1', spw='1', solint='inf',
               combine='scan', preavg=101.0, minsnr=0.0, poltype='PosAng',
               smodel=[1.0, 0.06, 0.08, 0.0], gaintable=[calpathLin])

        tb.open(outcal)
        calresult = tb.getcol('FPARAM')
        tb.close()

        self.assertTrue(np.all(np.isclose(calresult, -0.1418972)))

    ### Circular Test cases ###
    # only use outcal prefix?
    def test_unpolarizedDfCirc(self):
        ''' Test unpolarized calibration Q=U=0 for Df mode'''
        polcal(vis=datacopyCirc, caltable=outcal, field='0', spw='1,2,3', preavg=101.0,
               minsnr=0.0, poltype='Df', smodel=[1.0, 0, 0, 0], refant='5')
        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathCirc)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 10:40]
        tb.close()

        self.assertTrue(np.all(np.isclose(np.mean(calresult), np.mean(refresult), atol=1e-4)))

    def test_unpolarizedDfQUCirc(self):
        ''' Test unpolarized calibraion Q=U=0 for Df+QU mode '''
        polcal(vis=datacopyCirc, caltable=outcal, field='0', spw='1,2,3', preavg=101.0,
               minsnr=0.0, poltype='Df+QU', smodel=[1.0, 0, 0, 0], refant='5')
        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathCirc)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, 10:40]
        tb.close()

        self.assertTrue(np.all(np.isclose(np.mean(calresult), np.mean(refresult), atol=1e-4)))

    def test_unknownPolDfQUCirc(self):
        ''' Test polarized calibration with unknown polarization for Df+QU mode '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', preavg=101.0,
               minsnr=0.0, poltype='Df+QU', smodel=[1.0, 0, 0, 0], refant='5')
        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathCirc)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, :]
        tb.close()

        self.assertTrue(np.all(np.isclose(np.mean(calresult), np.mean(refresult), atol=1e-4)))

    def test_knownPolDfllsCirc(self):
        ''' Test polarized calibration with known polarization for Dflls mode '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', preavg=101.0,
               minsnr=0.0, poltype='Dflls', smodel=[1.0, 0.08, 0.06, 0], refant='5')
        # get the CPARAM of the polcal run
        calresult = getparam(outcal)
        # get CPARAM values from ref cal
        tb.open(calpathCirc)
        # Each 10 rows is a spectral window
        refresult = tb.getcol('CPARAM')[:, :, :]
        tb.close()

        self.assertTrue(np.all(np.isclose(np.mean(calresult), np.mean(refresult), atol=1e-4)))

    ### Xf ###

    def test_XfCorrectQUCirc(self):
        ''' Test poltype Xf and assume the correct Q, U '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='Xf', smodel=[1.0, 0.08, 0.06, 0], refant='5', gaintable=[calpathCirc])

        calresult = getparam(outcal)

        self.assertTrue(np.all(np.isclose(np.mean(calresult), [1 + 0j], atol=1e-4)))

    def test_XfAlternateQUCirc(self):
        ''' Test poltype Xf and assume alternate Q, U values '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='Xf', smodel=[1.0, 0.1, 0.0, 0], refant='5', gaintable=[calpathCirc])

        calresult = getparam(outcal)

        self.assertTrue(np.all(np.isclose(np.mean(calresult), [0.8 + 0.6j], atol=1e-4)))

    def test_XfNegatedQUCirc(self):
        ''' Test poltype Xf and assume Q, U with flipped signs '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='Xf', smodel=[1.0, -0.08, -0.06, 0], refant='5', gaintable=[calpathCirc])

        calresult = getparam(outcal)

        self.assertTrue(np.all(np.isclose(np.mean(calresult), [-1 + 0j], atol=1e-4)))

    ### PosAng ###

    def test_PosAngCorrectQUCirc(self):
        ''' Test poltype PosAng and assume the correct Q, U '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='PosAng', smodel=[1.0, 0.08, 0.06, 0], gaintable=[calpathCirc])

        tb.open(outcal)
        calresult = tb.getcol('FPARAM')
        tb.close()

        self.assertTrue(np.all(np.isclose(np.mean(calresult), 0, rtol=1e-4, atol=1e-4)))

    def test_PosAngAlternateQUCirc(self):
        ''' Test poltype PosAng and assume alternate Q, U values '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='PosAng', smodel=[1.0, 0.1, 0.0, 0], gaintable=[calpathCirc])

        tb.open(outcal)
        calresult = tb.getcol('FPARAM')
        tb.close()

        # Answer is ~ 0.32193175 radians
        self.assertTrue((np.isclose(np.degrees(np.mean(calresult)), 18.43, atol=1e-4, rtol=1e-3)))

    def test_PosAngNegatedQUCirc(self):
        ''' Test poltype PosAng and assume Q, U with flipped signs '''
        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='PosAng', smodel=[1.0, -0.08, -0.06, 0], gaintable=[calpathCirc])

        tb.open(outcal)
        calresult = tb.getcol('FPARAM')
        tb.close()

        calresult = np.mean(abs(np.degrees(calresult)))
        # ans ~ -1.5706 radians
        self.assertTrue(np.isclose(calresult, 90, atol=1e-4, rtol=1e-4))
    def test_PosAngNegatedQUApplyCirc(self):
        ''' Test applying the negated table to a new polcal call '''
        polcal(vis=datacopyCirc, caltable=outcal+'.PA-rel', field='1', spw='', solint='inf',
               minsnr=0.0, poltype='PosAng', smodel=[1.0, -0.08, -0.06, 0], gaintable=[calpathCirc])

        polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
               minsnr=0.0, poltype='PosAng', smodel=[1.0, -0.08, -0.06, 0], gaintable=[calpathCirc,
                                                                                       outcal+'.PA-rel'])

        tb.open(outcal)
        calresult = tb.getcol('FPARAM')
        tb.close()

        self.assertTrue(np.all(np.isclose(np.mean(calresult), 0, atol=1e-4)))

    ### Xfparang+QU

    def test_XfParangQUCorrectQUCirc(self):
        ''' Test poltype XfParang+QU and assume the correct Q, U '''
        P = polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
                   minsnr=0.0, poltype='Xfparang+QU', smodel=[1.0, 0.08, 0.06, 0])

        calresult = getparam(outcal)
        Q = P['J2354-3600']['SpwAve'][1]
        U = P['J2354-3600']['SpwAve'][2]

        self.assertTrue(np.all(np.isclose(np.mean(calresult), [1 + 0j], atol=1e-4)))
        self.assertTrue(np.isclose(Q, 0.1, atol=1e-4))
        self.assertTrue(np.isclose(U, 0.0, atol=1e-4))

    def test_XfParangQUAlternateQUCirc(self):
        ''' Test poltype XfParang+QU and assume the correct Q, U '''
        P = polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
                   minsnr=0.0, poltype='Xfparang+QU', smodel=[1.0, 0.1, 0.0, 0])

        calresult = getparam(outcal)
        Q = P['J2354-3600']['SpwAve'][1]
        U = P['J2354-3600']['SpwAve'][2]

        self.assertTrue(np.all(np.isclose(np.mean(calresult), [0.8 + 0.6j], atol=1e-4)))
        self.assertTrue(np.isclose(Q, 0.1, atol=1e-4))
        self.assertTrue(np.isclose(U, 0.0, atol=1e-4))

    def test_XfParangQUNegatedQUCirc(self):
        ''' Test poltype XfParang+QU  and assume Q, U with flipped signs '''
        P = polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
                   minsnr=0.0, poltype='Xfparang+QU', smodel=[1.0, -0.08, -0.06, 0])

        calresult = getparam(outcal)
        Q = P['J2354-3600']['SpwAve'][1]
        U = P['J2354-3600']['SpwAve'][2]

        self.assertTrue(np.all(np.isclose(np.mean(calresult), [-1 + 0j], atol=1e-4)))
        self.assertTrue(np.isclose(Q, 0.1, atol=1e-4))
        self.assertTrue(np.isclose(U, 0.0, atol=1e-4))

    def test_XfParangQUNegatedQUApplyCirc(self):
        ''' Test applying the negated table to a new polcal call '''
        polcal(vis=datacopyCirc, caltable=outcal+'.XfpaQU-rel', field='1', spw='', solint='inf',
               minsnr=0.0, poltype='Xfparang+QU', smodel=[1.0, -0.08, -0.06, 0])

        P = polcal(vis=datacopyCirc, caltable=outcal, field='1', spw='', solint='inf',
                   minsnr=0.0, poltype='Xfparang+QU', smodel=[1.0, -0.08, -0.06, 0],
                   gaintable=[outcal + '.XfpaQU-rel', calpathCirc])

        calresult = getparam(outcal)
        Q = P['J2354-3600']['SpwAve'][1]
        U = P['J2354-3600']['SpwAve'][2]

        self.assertTrue(np.all(np.isclose(calresult, [1.0 + 0j], atol=1e-4)))
        self.assertTrue(np.isclose(Q, 0.1, atol=1e-4))
        self.assertTrue(np.isclose(U, 0.0, atol=1e-4))

if __name__ == '__main__':
    unittest.main()
