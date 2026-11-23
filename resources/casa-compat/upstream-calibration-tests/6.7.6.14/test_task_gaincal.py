##########################################################################
# test_task_gaincal.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.gaincal.html
#
#
##########################################################################
import sys
import os
import unittest
import shutil
import numpy as np
import pylab as pl

import casatools
from casatasks import gaincal, mstransform, casalog, flagdata, gencal, rmtables
tb = casatools.table()
from casatestutils import testhelper as th

from math import pi

rootpath = casatools.ctsys.resolve('unittest/gaincal/')

datapath = rootpath + 'gaincaltest2.ms'
compCal = rootpath + 'gaincaltest2.ms.G0'
tCal = rootpath + 'gaincaltest2.ms.T0'
# Reference Cals
combinedRef = rootpath + 'genDataCombine.G0'
preTRef = rootpath + 'genDataPreT.G0'
preGRef = rootpath + 'genDataPreG.T0'
calModeP = rootpath + 'calModeTest.G0'
calModeA = rootpath + 'calModeTest.G1'
typeCalK = rootpath + 'gaintypek.G0'
typeCalSpline = rootpath + 'gaintypeSpline.G0'
spwMapCal = rootpath + 'spwMap.G0'
# From merged test
merged_dataset1 = rootpath + 'ngc5921.ms'
merged_refcal1 = rootpath + 'ngc5921.ref1a.gcal'
merged_refcal2 = rootpath + 'ngc5921.ref2a.gcal'
merged_dataset2 = rootpath + 'ngc4826.ms'
merged_refcal3 = rootpath + 'ngc4826.ref1b.gcal'


fullRangeCal = 'testgaincal.cal'
maxScanCal = 'testScan.cal'
int70Cal = 'int70.cal'
int30Cal = 'int30.cal'

tempCal = 'temp.cal'
tempCal2 = 'temp2.cal'
selectCal = 'select.cal'

flagcopy = 'flagged.ms'
datacopy = 'gaincalTestCopy.ms'
merged_copy1 = 'merged_copy1.ms'
merged_copy2 = 'merged_copy2.ms'

msname0= rootpath + 'gaincaltestK.ms'
datacopyK = 'gaincaltestKcopy.ms'

# created within:
sysdel4='gaincaltestK_4spw.K'        # gencal delays for 4 spws
solvedel4a='gaincaltestK.Ksolve4a'   # solved delays

msname1='gaincaltestK_2spw.ms'
sysdel2='gaincaltestK_2spw.K'        # gencal delays for 2 spws
solvedel2a='gaincaltestK.Ksolve2a'   # solved delays

solvedel4b='gaincaltestK.Ksolve4b'   # solved delays (mixed 2->4)
solvedel2b='gaincaltestK.Ksolve2b'   # solved delays (mixed 4->2)
xyPhaCal='xyPhaCal.G'  # for X/Y phase alignment for T solutions

cleanupList = [sysdel4, solvedel4a, msname1, sysdel2, solvedel2a, solvedel4b, solvedel2b, xyPhaCal]



def getparam(caltable, colname='CPARAM'):
    ''' Open a caltable and get the provided column '''

    tb.open(caltable)
    outtable = tb.getcol(colname)
    tb.close()

    return outtable

def tableComp(table1, table2, cols=[], rtol=8e-5, atol=1e-6):
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
            casalog.post(message=col+': ERROR in determining the truth value')

    if len(cols) == 0:

        truths = [[x, np.all(truthDict[x] == True)] for x in truthDict.keys()]

    else:

        truths = [[x, np.all(truthDict[x] == True)] for x in cols]

    return np.array(truths)

def change_perms(path):
    os.chmod(path, 0o777)
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chmod(os.path.join(root,d), 0o777)
        for f in files:
            os.chmod(os.path.join(root,f), 0o777)

class gaincal_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.copytree(datapath, datacopy)
        shutil.copytree(msname0, datacopyK)
        shutil.copytree(merged_dataset1, merged_copy1)
        shutil.copytree(merged_dataset2, merged_copy2)
        #change permissions
        change_perms(datacopy)
        change_perms(merged_copy1)
        change_perms(merged_copy2)


        gaincal(vis=datacopy, caltable=fullRangeCal, combine='scan', solint='inf', field='0', refant='0',
                smodel=[1, 0, 0, 0], scan='0~9')

        gaincal(vis=datacopy, caltable=maxScanCal, solint='inf', field='0', refant='0',
                smodel=[1, 0, 0, 0], scan='0~9')

        gaincal(vis=datacopy, caltable=int70Cal, solint='70s', field='0', refant='0',
                smodel=[1, 0, 0, 0], scan='0~9')

        gaincal(vis=datacopy, caltable=int30Cal, solint='30s', field='0', refant='0',
                smodel=[1, 0, 0, 0], scan='0~9')

        gaincal(vis=datacopy, caltable=selectCal, solint='inf', field='0', refant='0',
                smodel=[1, 0, 0, 0], scan='2', spw='2')

    def setUp(self):
        shutil.copytree(datacopy, flagcopy)

    def tearDown(self):
        if os.path.exists(tempCal):
            shutil.rmtree(tempCal)

        if os.path.exists(flagcopy):
            shutil.rmtree(flagcopy)

        if os.path.exists(tempCal2):
            shutil.rmtree(tempCal2)
        if os.path.exists('testcorrdepflags.ms'):
            shutil.rmtree('testcorrdepflags.ms')
        if os.path.exists('testcorrdepflagsF.G'):
            shutil.rmtree('testcorrdepflagsF.G')
        if os.path.exists('testcorrdepflagsT.G'):
            shutil.rmtree('testcorrdepflagsT.G')

        if os.path.exists('testspwmap.ms'):
            shutil.rmtree('testspwmap.ms')

        if os.path.exists('testspwmap.G0'):
            shutil.rmtree('testspwmap.G0')

        if os.path.exists('testspwmap.G1'):
            shutil.rmtree('testspwmap.G1')

        if os.path.exists('testspwmap.G2'):
            shutil.rmtree('testspwmap.G2')

        if os.path.exists('testspwmap.G3'):
            shutil.rmtree('testspwmap.G3')

        for item in cleanupList:
            if os.path.exists(item):
                shutil.rmtree(item)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(datacopy)
        shutil.rmtree(datacopyK)
        shutil.rmtree(merged_copy1)
        shutil.rmtree(merged_copy2)

        if os.path.exists(fullRangeCal):
            shutil.rmtree(fullRangeCal)

        if os.path.exists(maxScanCal):
            shutil.rmtree(maxScanCal)

        if os.path.exists(int70Cal):
            shutil.rmtree(int70Cal)

        if os.path.exists(int30Cal):
            shutil.rmtree(int30Cal)

        if os.path.exists(selectCal):
            shutil.rmtree(selectCal)

        # Removing frequency metadata cal tables
        if os.path.exists('fmd1a.G'):
            shutil.rmtree('fmd1a.G')

        if os.path.exists('fmd1b.G'):
            shutil.rmtree('fmd1b.G')

        if os.path.exists('fmd2a.G'):
            shutil.rmtree('fmd2a.G')

        if os.path.exists('fmd2b.G'):
            shutil.rmtree('fmd2b.G')

    def test_correctGains(self):
        '''
            test_correctGains
            -------------------

            Check that the gaincal results match a reference gaincal table
        '''

        self.assertTrue(np.all(tableComp(fullRangeCal, combinedRef)[:,1] == 'True'))
        #self.assertTrue(ch.Compare.compare_CASA_tables(fullRangeCal, combinedRef))

    def test_intervalSNR(self):
        '''
            test_intervalSNR
            ------------------

            Check that shorter interval times result in a lower signal to noise
        '''

        snrCombine = np.mean(getparam(fullRangeCal, 'SNR'))
        snrScans = np.mean(getparam(maxScanCal, 'SNR'))
        int70Snr = np.mean(getparam(int70Cal, 'SNR'))
        int30Snr = np.mean(getparam(int30Cal, 'SNR'))

        self.assertTrue(int30Snr < int70Snr < snrScans < snrCombine)

    def test_minSNR(self):
        '''
            test_minSNR
            -------------

            Check that values below the provided SNR threshold are flagged
        '''

        gaincal(vis=datacopy, caltable=tempCal, solint='30s', field='0', refant='0',
                smodel=[1, 0, 0, 0], minsnr=1000)

        flagged = getparam(tempCal, 'FLAG')

        self.assertTrue(np.all(flagged == 1))

    def test_fieldSelect(self):
        '''
            test_fieldSelect
            ------------------

            Check that the field selection parameter functions properly
        '''

        fields = getparam(fullRangeCal, 'FIELD_ID')

        self.assertTrue(np.all(fields == 0))

    def test_refantSelect(self):
        '''
            test_refantSelect
            -------------------

            Check that the refant selection functions properly
        '''

        refants = getparam(fullRangeCal, 'ANTENNA2')

        self.assertTrue(np.all(refants == 0))

    def test_scanSelect(self):
        '''
            test_scanSelect
            -----------------

            Check that the scan selection functions properly
        '''

        scans = getparam(selectCal, 'SCAN_NUMBER')

        self.assertTrue(np.all(scans == 2))

    def test_spwSelect(self):
        '''
            test_spwSelect
            ----------------

            Check that the spw selection parameter functions properly
        '''

        spws = getparam(selectCal, 'SPECTRAL_WINDOW_ID')

        self.assertTrue(np.all(spws == 2))

    def test_uvrangeSelect(self):
        '''Check that using the uv range parameter you can cut off specific antennas'''
        gaincal(vis=datacopy, caltable=tempCal, spw='2', refant='0', uvrange='<1160', minblperant=1)

        tb.open(tempCal)
        antennas = tb.getcol('ANTENNA1')
        flags = tb.getcol('FLAG')
        tb.close()

        flagged_ants = set()
        expected = {5, 8}

        for i in range(len(antennas)):
            if np.all(flags[:, :, i] == True):
                flagged_ants.add(antennas[i])

        self.assertTrue(flagged_ants == expected)

    def test_refantDiff(self):
        '''
            test_refantDiff
            -----------------

            Check that selecting refant will cause that refant to be set at 0
        '''

        gaincal(vis=datacopy, caltable=tempCal, solint='inf', field='0', combine='scan', refant='1',
                smodel=[1, 0, 0, 0])

        gAmp = getparam(tempCal)
        refs = [[np.mean(gAmp.imag[j,0,i::10]) for i in range(10)] for j in range(2)]

        self.assertTrue(np.isclose(refs[0][1], 0) and np.isclose(refs[1][1], 0))

    def test_preapplyT0(self):
        '''
            test_preapplyT0
            -----------------

            Check that pre applying the T table results in the regular G table calibration
        '''

        gaincal(vis=datacopy, caltable=tempCal, refant='0', solint='inf', smodel=[1, 0, 0, 0], gaintype='G', field='0', combine='scan',
                  gaintable=[tCal])

        self.assertTrue(np.all(tableComp(preTRef, tempCal)[:,1] == 'True'))

    def test_preapplyG0(self):
        '''
            test_preapplyG0
            -----------------

            Check that pre applying the G table results in the regular T table calibration
        '''

        gaincal(vis=datacopy, caltable=tempCal, refant='0', solint='int', smodel=[1, 0, 0, 0],
                gaintype='T', gaintable=[compCal])

        self.assertTrue(np.all(tableComp(preGRef, tempCal)[:,1] == 'True'))

    def test_antennaSelect(self):
        '''
            test_antennaSelect
            --------------------

            Check that antennas that aren't selected are flagged
        '''

        gaincal(vis=datacopy, caltable=tempCal, refant='0', field='0', solint='inf', combine='scan', antenna='0~5&', smodel=[1,0,0,0], gaintype='G')

        flags = getparam(tempCal, colname='FLAG')
        flagnum = np.sum(flags)

        self.assertTrue(flagnum == 32)

    def test_minBl(self):
        '''
            test_minBl
            ------------

            Check that if the min baseline threshold isn't met those antennas aren't used. If no antennas have enough then a file is not written.
        '''

        gaincal(vis=datacopy, caltable=tempCal, refant='0', solint='int', smodel=[1,0,0,0], gaintype='G', combine='scan', antenna='0~5&', minblperant=6)

        self.assertFalse(os.path.exists(tempCal))

    def test_preboth(self):
        '''
            test_preboth
            --------------

            Check that when preapplying both then gaintype T increases the SNR

        '''

        gaincal(vis=datacopy, caltable=tempCal, refant='0', solint='inf', smodel=[1, 0, 0, 0], gaintype='G', field='0',
                  gaintable=[tCal, compCal], gainfield=['0','0'], interp=[''])

        gaincal(vis=datacopy, caltable=tempCal2, refant='0', solint='inf', smodel=[1, 0, 0, 0], gaintype='T', field='0',
                  gaintable=[tCal, compCal])

        SNR1 = np.mean(getparam(tempCal, colname='SNR'))
        SNR2 = np.mean(getparam(tempCal2, colname='SNR'))

        self.assertTrue(SNR1 < SNR2)

    def test_calModeP(self):
        '''
            test_calModeP
            ---------------

            Check that the output with calmode 'p' is equal to a reference calibration table
        '''

        gaincal(vis=datacopy, caltable=tempCal, field='0', smodel=[1,0,0,0], solint='inf', combine='scan', calmode='p')

        self.assertTrue(np.all(tableComp(tempCal, calModeP)[:,1] == 'True'))

    def test_calModeA(self):
        '''
            test_calModeA
            ---------------

            Check that the output with calmode 'a' is equal to a reference calibration table
        '''

        gaincal(vis=datacopy, caltable=tempCal, field='0', smodel=[1,0,0,0], solint='inf', combine='scan', calmode='a')

        self.assertTrue(np.all(tableComp(tempCal, calModeA)[:,1] == 'True'))

    def test_gainTypeK(self):
        '''
            test_gainTypeK
            ----------------

            Check that the output with gaintype k is equal to a reference calibration table
        '''

        gaincal(vis=datacopy, caltable=tempCal, field='0', smodel=[1,0,0,0], solint='inf', combine='scan', gaintype='KCROSS', refant='0')

        self.assertTrue(np.all(tableComp(tempCal, typeCalK)[:,1] == 'True'))

    def test_gainTypeKSpwCountMisMatch(self):
        '''
            test_gainTypeKSpwCountMisMatch
            -------------------------------

            Check that a caltable can be applied when the number of spws
            in the ms and caltable do not match
        '''

        # create systematic delay caltable, same in all 4 spws
        d=list([0,0]+list(pl.arange(1,19)/100.))*4
        gencal(vis=datacopyK,caltable=sysdel4,
               caltype='sbd',
               spw='0,1,2,3',antenna='0,1,2,3,4,5,6,7,8,9',pol='R,L',parameter=d)
        # extract truth for comparisons below:
        tb.open(sysdel4)
        sysK4=tb.getcol('FPARAM')
        tb.close()

        # solve for delays relative to the systematic delay caltable
        #  one solution for all, combining scans, fields
        gaincal(vis=datacopyK,caltable=solvedel4a,
                gaintype='K',smodel=[1,0,0,0],
                solint='inf',combine='scan,field',refant='0',
                gaintable=[sysdel4])
        # extract results and compare to k0
        tb.open(solvedel4a)
        K4a=tb.getcol('FPARAM')
        tb.close()

        dk=K4a+sysK4    # sum should be ~zero
        self.assertTrue(np.isclose(np.mean(dk), 2.674306e-5, atol=5e-5), msg=f"Sum should be ~zero, caltable with all 4 spws. {np.mean(dk)}")

        # extract spws 0,3 from orig MS to create MS with only 2 spws
        mstransform(vis=datacopyK,outputvis=msname1,
                    spw='0,3',datacolumn='data')

        # create systematic delay table for the 2-spw MS
        #  d[0:40] is half of original
        gencal(vis=msname1,caltable=sysdel2,
               caltype='sbd',
               spw='0,1',antenna='0,1,2,3,4,5,6,7,8,9',pol='R,L',parameter=d[0:40])
        # extract truth for comparisons below:
        tb.open(sysdel2)
        sysK2=tb.getcol('FPARAM')
        tb.close()

        # solve for delays relative to the systematic delay caltable
        #  one solution for all, combining scans, fields
        gaincal(vis=msname1,caltable=solvedel2a,
                gaintype='K',smodel=[1,0,0,0],
                solint='inf',combine='scan,field',refant='0',
                gaintable=[sysdel2])
        # extract results and compare to k0
        tb.open(solvedel2a)
        K2a=tb.getcol('FPARAM')
        tb.close()

        dk=K2a+sysK2    # sum should be ~zero (within noise)
        self.assertTrue(np.isclose(np.mean(dk), 3.147865e-05, atol=5e-5), msg=f"Sum should be close to 0 within noise, caltable and ms with 2 spws. Mean is {np.mean(dk)}")

        # Solve on orig dataset using sysdel2
        #  using spwmap
        #  solutions should match original (sysK4)
        gaincal(vis=datacopyK,caltable=solvedel4b,
                gaintype='K',smodel=[1,0,0,0],
                solint='inf',combine='scan,field',refant='0',
                gaintable=[sysdel2],spwmap=[0,0,1,1])
        # extract results and compare to sysK4
        tb.open(solvedel4b)
        K4b=tb.getcol('FPARAM')
        tb.close()

        dk=K4b+sysK4    # sum should be ~zero
        self.assertTrue(np.isclose(np.mean(dk), 2.674664e-05, atol=5e-5), msg=f"Sum should be close to 0, caltable with 2 spws ms with 4, Mean is {np.mean(dk)}")

        dk=K4b-K4a      # should be precisely zero (same effective sysdel)
        self.assertTrue(np.isclose(np.mean(dk), 0, atol=5e-5), msg=f"These two should be the same. Mean is {np.mean(dk)}")

        # Solve on 2-spw MS using sysdel4
        #  using spwmap
        #  solutions should match original (sysK2)
        gaincal(vis=msname1,caltable=solvedel2b,
                gaintype='K',smodel=[1,0,0,0],
                solint='inf',combine='scan,field',refant='0',
                gaintable=[sysdel4],spwmap=[[0,1]])
        # extract results and compare to sysK2
        tb.open(solvedel2b)
        K2b=tb.getcol('FPARAM')
        tb.close()

        dk=K2b+sysK2    # sum should be ~zero
        self.assertTrue(np.isclose(np.mean(dk), 3.148629e-05, atol=5e-5), msg=f"Sum should be close to 0, caltable with 4 spws ms with 2. Mean is {np.mean(dk)}")

        dk=K2b-K2a      # should be precisely zero (same effective sysdel)
        self.assertTrue(np.isclose(np.mean(dk), 0, atol=5e-5), msg=f"These two should be the same. Mean is {np.mean(dk)}")

    def test_gainTypeSpline(self):
        '''
            test_gainTypeSpline
            ----------------

            Check that the output with gaintype GSPLINE is equal to a reference calibration table
        '''

        gaincal(vis=datacopy, caltable=tempCal, field='0', smodel=[1,0,0,0], solint='inf', combine='scan', gaintype='GSPLINE', refant='0')

        self.assertTrue(np.all(tableComp(tempCal, typeCalSpline)[:,1] == 'True'))

    def test_gainTypeGL1OutlierRejection(self):
        '''
            test_gainTypeGL1OutlierRejection
            ------------------------------------

            Check that solmodes 'L1', 'R', and 'L1R' yield
            numerically larger mean SNR than solmode=''
        '''

        # Nominal G solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='G',solmode='')
        tb.open(tempCal)
        snr=tb.getcol('SNR')
        tb.close()

        # 'L1' G solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='G',solmode='L1')
        tb.open(tempCal)
        snrL1=tb.getcol('SNR')
        tb.close()

        # L1 SNR is numerically larger because L1>sqrt(L2)
        Rat=np.mean(snrL1/snr)
        #print('L1 snr mean ratio: ',Rat)
        self.assertTrue(Rat>1.0)


        # 'R' G solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='G',solmode='R')
        tb.open(tempCal)
        snrR=tb.getcol('SNR')
        tb.close()

        # R SNR is numerically larger because (a few!) outliers excluded
        Rat=np.mean(snrR/snr)
        #print('R snr mean ratio: ',Rat)
        self.assertTrue(Rat>1.0)


        # 'L1R' G solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='G',solmode='L1R')
        tb.open(tempCal)
        snrL1R=tb.getcol('SNR')
        tb.close()

        # L1R SNR is numerically larger because L1>sqrt(L2) and no outliers
        Rat=np.mean(snrL1R/snr)
        #print('L1R snr mean ratio: ',Rat)
        self.assertTrue(Rat>1.0)




    def test_gainTypeTL1OutlierRejection(self):
        '''
            test_gainTypeTL1OutlierRejection
            ------------------------------------

            Check that solmodes 'L1', 'R', and 'L1R' yield
            numerically larger mean SNR than solmode=''
        '''

        # we must align parallel-hands with a G solution
        a=gaincal(vis=datacopy,caltable=xyPhaCal,scan='2',spw='3',
                  solint='inf',gaintype='G',solmode='')

        # align Y w/ X on apply, phases only
        tb.open(xyPhaCal,nomodify=False)
        g=tb.getcol('CPARAM')
        g[1,0,:]=g[1,0,:]/g[0,0,:]
        g[1,0,:]/=np.absolute(g[1,0,:])
        g[0,0,:]=1
        tb.putcol('CPARAM',g)
        tb.close()


        # Nominal T solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='T',solmode='',
                  gaintable=[xyPhaCal],interp=['nearest'])
        tb.open(tempCal)
        snr=tb.getcol('SNR')
        tb.close()

        # 'L1' T solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='T',solmode='L1',
                  gaintable=[xyPhaCal],interp=['nearest'])
        tb.open(tempCal)
        snrL1=tb.getcol('SNR')
        tb.close()

        # L1 SNR is numerically larger because L1>sqrt(L2)
        Rat=np.mean(snrL1/snr)
        #print('L1 snr mean ratio: ',Rat)
        self.assertTrue(Rat>1.0)


        # 'R' T solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='T',solmode='R',
                  gaintable=[xyPhaCal],interp=['nearest'])
        tb.open(tempCal)
        snrR=tb.getcol('SNR')
        tb.close()

        # R SNR is numerically larger because (a few!) outliers excluded
        Rat=np.mean(snrR/snr)
        #print('R snr mean ratio: ',Rat)
        self.assertTrue(Rat>1.0)


        # 'L1R' T solution
        a=gaincal(vis=datacopy,caltable=tempCal,spw='3',
                  solint='inf',gaintype='T',solmode='L1R',
                  gaintable=[xyPhaCal],interp=['nearest'])
        tb.open(tempCal)
        snrL1R=tb.getcol('SNR')
        tb.close()

        # L1R SNR is numerically larger because L1>sqrt(L2) and no outliers
        Rat=np.mean(snrL1R/snr)
        #print('L1R snr mean ratio: ',Rat)
        self.assertTrue(Rat>1.0)

    def test_spwMap(self):
        '''
            test_spwMap
            -------------

            Check that the output with spwMap matches to a reference calibration table
        '''

        gaincal(vis=datacopy, caltable=tempCal, field='0', smodel=[1,0,0,0], solint='inf', combine='scan', refant='0',spwmap=[0,0,1,1])

        self.assertTrue(np.all(tableComp(tempCal, spwMapCal)[:,1] == 'True'))



        # Add more interesting test, including test of CAS-12591 fix

        tsmdata='testspwmap.ms'

        # slice out just scan 2
        mstransform(vis=datacopy,outputvis=tsmdata,scan='2',datacolumn='data')

        # Run gaincal w/ solint='inf' to get solutions for all spws
        tsmcal0='testspwmap.G0'
        gaincal(vis=tsmdata,caltable=tsmcal0,solint='inf',refant='0',smodel=[1,0,0,0])

        # change spws in tsmcal0 [0,1,2,3] to [2,3,0,1], so we can use spwmap non-trivially
        tb.open(tsmcal0,nomodify=False)
        spwid=tb.getcol('SPECTRAL_WINDOW_ID')
        spwid = [(i+2)%4 for i in spwid]
        tb.putcol('SPECTRAL_WINDOW_ID',spwid)
        tb.close()

        # Solve for gains using tsmcal0 with spwmap=[2,3,0,1], which should "undo"
        #  spwid change made above, expecting all solutions ~= (1,0)
        tsmcal1='testspwmap.G1'
        gaincal(vis=tsmdata,caltable=tsmcal1,solint='inf',refant='0',smodel=[1,0,0,0],
                gaintable=[tsmcal0],spwmap=[2,3,0,1])

        # test that output calibration is ~(1,0)
        #  gains-1.0 ~ zero (to within precision and solve convergence fuzz
        tb.open(tsmcal1)
        g1=tb.getcol('CPARAM')
        tb.close()
        self.assertTrue(np.absolute(np.mean(g1-1.0))<2e-6)


        # Run gaincal to get solutions for spw=0,1
        tsmcal2='testspwmap.G2'
        gaincal(vis=tsmdata,caltable=tsmcal2,spw='0,1',solint='inf',refant='0',smodel=[1,0,0,0])

        # Reset spwid  0,1->3,2 so we can exercise spwmap=[3,2,0,1]
        # also fix FLAG_ROW in SPECTRAL_WINDOW subtable
        tb.open(tsmcal2,nomodify=False)
        spwid=tb.getcol('SPECTRAL_WINDOW_ID')
        spwid[spwid==0]=3
        spwid[spwid==1]=2
        tb.putcol('SPECTRAL_WINDOW_ID',spwid)
        tb.close()
        tb.open(tsmcal2+'/SPECTRAL_WINDOW',nomodify=False)
        fr=tb.getcol('FLAG_ROW')
        fr=[1,1,0,0]
        tb.putcol('FLAG_ROW',fr)
        tb.close()

        # solve again with unselected spws all mapped to unavailable solutions
        #  this tests the fix for CAS-12591, wherein the solution-availability check
        #  was applying the spwmap twice, causing a mysterious exception and
        #  failure to calibrate
        #  (In this case, if spw 2,3 are mapped twice (to 0,1), the availability check
        #   would fail)
        #  (expecting g~=(1,0) if applied solutions mapped correctly)
        tsmcal3='testspwmap.G3'
        gaincal(vis=tsmdata,caltable=tsmcal3,spw='0,1',solint='inf',refant='0',smodel=[1,0,0,0],
                gaintable=[tsmcal2],spwmap=[3,2,0,1])

        # test that output calibration is ~(1,0)
        #  gains-1.0 ~ zero (to within precision and solve convergence fuzz
        tb.open(tsmcal3)
        g3=tb.getcol('CPARAM')
        tb.close()
        self.assertTrue(np.absolute(np.mean(g1-1.0))<2e-6)

    def test_mergedCreatesGainTable(self):
        ''' Gaincal 1a: Default values to create a gain table '''

        gaincal(vis=merged_copy1, caltable=tempCal, uvrange='>0.0')
        self.assertTrue(os.path.exists(tempCal))

        self.assertTrue(th.compTables(tempCal, merged_refcal1, ['WEIGHT']))

    def test_mergedFieldSelect(self):
        ''' Gaincal 2a: Create a gain table using field selection '''

        gaincal(vis=merged_copy1, caltable=tempCal, uvrange='>0.0', field='0', gaintype='G', solint='int', combine='', refant='VA02')
        self.assertTrue(os.path.exists(tempCal))

        self.assertTrue(th.compTables(tempCal, merged_refcal2, ['WEIGHT']))

    def test_mergedSpwSelect(self):
        ''' Gaincal 1b: Create a gain table for an MS with many spws '''


        gaincal(vis=merged_copy2, caltable=tempCal, uvrange='>0.0', field='0,1', spw='0', gaintype='G', minsnr=2.0, refant='ANT5', solint='inf', combine='')
        self.assertTrue(os.path.exists(tempCal))

        self.assertTrue(th.compTables(tempCal, merged_refcal3, ['WEIGHT']))

    def test_corrDepFlags(self):
        '''
            test_corrDepFlags
            -----------------
        '''

        # This test exercises the corrdepflags parameter
        #
        #  With corrdepflags=False (the default), one (or more) flagged correlations causes
        #  all correlations (per channel, per baseline) to behave as flagged, thereby
        #  causing both polarizations to be flagged in the output cal table
        #
        #  With corrdepflags=True, unflagged correlations will be used as normal, and
        #  only the implicated polarization will be flagged in the output cal table
        #
        #  NB: when some data are flagged, we expect solutions to change slightly,
        #      since available data is different.  For now, we are testing only the
        #      resulting flags.

        cdfdata='testcorrdepflags.ms'
        # slice out just scan 2
        mstransform(vis=datacopy,outputvis=cdfdata,scan='2',datacolumn='data')

        # modify flags in interesting corr-dep ways in scan 2 for subset of antennas
        tb.open(cdfdata,nomodify=False)

        # we modify the flags as follows:
        #  spw=0:  one antenna, one correlation (YY)
        #  spw=1:  one antenna, one correlation (XX)
        #  spw=2:  two antennas, opposite correlations
        #  spw=3:  one antenna, both cross-hands flagged

        # set flags for spw=0, antenna=3, corr=YY
        st=tb.query('SCAN_NUMBER==2 && DATA_DESC_ID==0 && (ANTENNA1==3 || ANTENNA2==3)')
        fl=st.getcol('FLAG')
        fl[3,:,:]=True
        st.putcol('FLAG',fl)
        st.close()

        # set flags for spw=1, antenna=6, corr=XX
        st=tb.query('SCAN_NUMBER==2 && DATA_DESC_ID==1 && (ANTENNA1==6 || ANTENNA2==6)')
        fl=st.getcol('FLAG')
        fl[0,:,:]=True
        st.putcol('FLAG',fl)
        st.close()

        # set flags for spw=2, antenna=2, corr=XX
        st=tb.query('SCAN_NUMBER==2 && DATA_DESC_ID==2 && (ANTENNA1==2 || ANTENNA2==2)')
        fl=st.getcol('FLAG')
        fl[0,:,:]=True
        st.putcol('FLAG',fl)
        st.close()
        # set flags for spw=2, antenna=7, corr=YY
        st=tb.query('SCAN_NUMBER==2 && DATA_DESC_ID==2 && (ANTENNA1==7 || ANTENNA2==7)')
        fl=st.getcol('FLAG')
        fl[3,:,:]=True
        st.putcol('FLAG',fl)
        st.close()

        # set flags for spw=3, antenna=8, corr=XY,YX
        st=tb.query('SCAN_NUMBER==2 && DATA_DESC_ID==3 && (ANTENNA1==8 || ANTENNA2==8)')
        fl=st.getcol('FLAG')
        fl[1:3,:,:]=True
        st.putcol('FLAG',fl)
        st.close()

        tb.close()

        # Run gaincal on scan 2, solint='inf' with corrdepflags=False
        #   expect both pols to be flagged for ants with one or more corr flagged
        cdfF='testcorrdepflagsF.G'
        gaincal(vis=cdfdata,caltable=cdfF,solint='inf',refant='0',smodel=[1,0,0,0],corrdepflags=False)

        tb.open(cdfF)
        flF=tb.getcol('FLAG')
        tb.close()

        # flag count per spw  (both pols in every case)
        self.assertTrue(np.sum(flF[:,0,0:10])==2)
        self.assertTrue(np.sum(flF[:,0,10:20])==2)
        self.assertTrue(np.sum(flF[:,0,20:30])==4)
        self.assertTrue(np.sum(flF[:,0,30:40])==2)

        # check flags set for specific antennas, each spw  (both pols each antenna)
        self.assertTrue(np.all(flF[:,0,0:10][:,3]))        # spw 0
        self.assertTrue(np.all(flF[:,0,10:20][:,6]))       # spw 1
        self.assertTrue(np.all(flF[:,0,20:30][:,[2,7]]))   # spw 2
        self.assertTrue(np.all(flF[:,0,30:40][:,8]))       # spw 3

        # Run gaincal on scan 2, solint='inf' with corrdepflags=True
        #   expect unflagged solutions for unflagged pol
        cdfT='testcorrdepflagsT.G'
        gaincal(vis=cdfdata,caltable=cdfT,solint='inf',refant='0',smodel=[1,0,0,0],corrdepflags=True)

        tb.open(cdfT)
        flT=tb.getcol('FLAG')
        tb.close()

        # flag count per spw (one pol per antenna, at most)
        self.assertTrue(np.sum(flT[:,0,0:10])==1)
        self.assertTrue(np.sum(flT[:,0,10:20])==1)
        self.assertTrue(np.sum(flT[:,0,20:30])==2)
        self.assertTrue(np.sum(flT[:,0,30:40])==0)

        # check flags set for specific antennas, each spw (one pol per antenna, at most)
        self.assertTrue(flT[1,0,0:10][3])        # spw 0, antenna 3, pol=Y
        self.assertTrue(flT[0,0,10:20][6])       # spw 1, antenna 6, pol=X
        self.assertTrue(flT[0,0,20:30][2])       # spw 2, antenna 2, pol=X
        self.assertTrue(flT[1,0,20:30][7])       # spw 2, antenna 7, pol=Y
        # (spw 3 tested above)

    def test_FreqMetaData1a(self):
        '''
            test_FreqMetaData1a: No explicit spw selection + append
            -------------------
        '''
        # 1a. No explicit spw selection

        # extract MS frequencies, upon which caltable frequency meta data are based
        tb.open(datacopy+'/SPECTRAL_WINDOW')
        msfreq=tb.getcol('CHAN_FREQ')
        tb.close()


        # the caltable
        ct='fmd1a.G'

        # create the table
        gaincal(vis=datacopy,caltable=ct,scan='2,4,6',spw='',solint='inf',smodel=[1,0,0,0])

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ')
        ctspwflag=tb.getcol('FLAG_ROW')  # shoule be [F,F,F,F]   # all spws unflagged
        tb.close()
        fdiff=(ctfreq[0,:] - np.mean(msfreq,0))/ctfreq[0,:]  # should be all ~zero  (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 120 = (nant=10)*(nspw=4)*(nscan=3)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==False))  # all spws unflagged
        self.assertTrue(ctnrows==120)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))

        #  + append=True
        gaincal(vis=datacopy,caltable=ct,scan='14,16',spw='',solint='inf',smodel=[1,0,0,0],append=True)

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ')
        ctspwflag=tb.getcol('FLAG_ROW')  # shoule be [F,F,F,F]   # all spws unflagged
        tb.close()
        fdiff=(ctfreq[0,:] - np.mean(msfreq,0))/ctfreq[0,:]  # should be all ~zero  (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 200 = (nant=10)*(nspw=4)*(nscan=3+2)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==False))  # all spws unflagged
        self.assertTrue(ctnrows==200)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))

    def test_FreqMetaData1b(self):
        '''
            test_FreqMetaData1b: Non-trivial spw/channel selection + append
            -------------------
        '''

        # 1b  Non-trivial spw selection, including some channel selection

        # extract MS frequencies, upon which caltable frequency meta data are based
        tb.open(datacopy+'/SPECTRAL_WINDOW')
        msfreq=tb.getcol('CHAN_FREQ')
        tb.close()

        # the caltable
        ct='fmd1b.G'

        # create the table
        gaincal(vis=datacopy,caltable=ct,scan='2,4,6',spw='1:1~4,2,3:4~7',solint='inf',smodel=[1,0,0,0])

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ',1,3)  # only 1,2,3
        ctspwflag=tb.getcol('FLAG_ROW')  # shoule be [T,F,F,F]   # spw 0 is flagged (not selected)
        tb.close()
        fdiff=ctfreq[0,:].copy()
        fdiff[0]-=np.mean(msfreq[1:5,1])  # chans 1-4
        fdiff[1]-=np.mean(msfreq[:,2])    # all chans
        fdiff[2]-=np.mean(msfreq[4:8,3])  # chans 4-7
        fdiff/=ctfreq[0,:]    # should be ~zero (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 90 = (nant=10)*(nspw=3)*(nscan=3)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==[True,False,False,False]))  # only spw 0 flagged
        self.assertTrue(ctnrows==90)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))


        #  different spw selection (MISMATCHED in spw 3!) + append=True   THIS SHOULD FAIL W/ EXCEPTION
        try:
            gaincal(vis=datacopy,caltable=ct,scan='21,23',spw='0,1:1~4,2,3:0~1',solint='inf',smodel=[1,0,0,0],append=True)
            print("In testFreqMetaData1b, a gaincal which should have thrown an exception did not!")
            self.assertTrue(False)
        except RuntimeError:
            self.assertTrue(True)


        #  different spw selection (overlaps correctly with above) + append=True
        gaincal(vis=datacopy,caltable=ct,scan='14,16',spw='0,1:1~4,2',solint='inf',smodel=[1,0,0,0],append=True)

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ')    # all spws now
        ctspwflag=tb.getcol('FLAG_ROW')  # shoule be [F,F,F,F]   # all spws unflagged
        tb.close()
        fdiff=ctfreq[0,:].copy()
        fdiff[0]-=np.mean(msfreq[:,0])  # all chans
        fdiff[1]-=np.mean(msfreq[1:5,1])  # chans 1-4
        fdiff[2]-=np.mean(msfreq[:,2])    # all chans
        fdiff[3]-=np.mean(msfreq[4:8,3])  # chans 4-7
        fdiff/=ctfreq[0,:]    # should be ~zero (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 150 = (nant=10)*(nspw=3)*(nscan=3+2)  (NB: different 3 spws)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==False))  # solutions for all spws now
        self.assertTrue(ctnrows==150)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))

    def test_FreqMetaData2a(self):
        '''
            test_FreqMetaData2a: No explicit spw selection w/ combine=spw + append
            -------------------
        '''
        # 2a. No explicit spw selection w/ combine='spw'

        # extract MS frequencies, upon which caltable frequency meta data are based
        tb.open(datacopy+'/SPECTRAL_WINDOW')
        msfreq=tb.getcol('CHAN_FREQ')
        tb.close()

        # the caltable
        ct='fmd2a.G'

        # create table
        gaincal(vis=datacopy,caltable=ct,scan='2,4,6',spw='',combine='spw',solint='inf',smodel=[1,0,0,0])

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ',0,1)  # only 0
        ctspwflag=tb.getcol('FLAG_ROW')    # should be [F,T,T,T]   # only for spw 0
        tb.close()
        fdiff=(ctfreq[0,0]-np.mean(msfreq,(0,1)))/ctfreq[0,0]     # should be ~zero (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 30 = (nant=10)*(nspw=1)*(nscan=3)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==[False,True,True,True]))  # only spw 0 unflagged
        self.assertTrue(ctnrows==30)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))

        #  + append=True
        gaincal(vis=datacopy,caltable=ct,scan='14,16',spw='',combine='spw',solint='inf',smodel=[1,0,0,0],append=True)

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ',0,1)  # only 0
        ctspwflag=tb.getcol('FLAG_ROW')    # should be [F,T,T,T]   # only for spw 0
        tb.close()
        fdiff=(ctfreq[0,0]-np.mean(msfreq,(0,1)))/ctfreq[0,0]   # should be ~zero (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 50 = (nant=10)*(nspw=1)*(nscan=3+2)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==[False,True,True,True]))  # only spw 0 unflagged
        self.assertTrue(ctnrows==50)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))

    def test_FreqMetaData2b(self):
        '''
            test_FreqMetaData2b: Non-trivial spw/channel selection  w/ combine=spw + append
            -------------------
        '''
        # 2b. Non-trivial spw selection, including some channel selection, w/ combine='spw'  fanin:  [1,2,3]->[1]

        # extract MS frequencies, upon which caltable frequency meta data are based
        tb.open(datacopy+'/SPECTRAL_WINDOW')
        msfreq=tb.getcol('CHAN_FREQ')
        tb.close()


        # the caltable
        ct='fmd2b.G'

        # create table
        gaincal(vis=datacopy,caltable=ct,scan='2,4,6',spw='1:1~4,2,3:4~7',combine='spw',solint='inf',smodel=[1,0,0,0])

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ',1,1)  # only 1
        ctspwflag=tb.getcol('FLAG_ROW')    # should be [T,F,T,T]   # only unflagged for spw 1
        tb.close()
        fdiff=(ctfreq[0,0]-np.mean(list(msfreq[1:5,1])+list(msfreq[:,2])+list(msfreq[4:8,3])))/ctfreq[0,0]   # should be ~zero (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 30 = (nant=10)*(nspw=1)*(nscan=3)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==[True,False,True,True]))  # only spw 0 unflagged
        self.assertTrue(ctnrows==30)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))


        # attemp to append incongruent channel selection (fanin is still [1,2,3]->[1])   SHOULD FAIL W/ EXCEPTION
        try:
            gaincal(vis=datacopy,caltable=ct,scan='21,23',spw='1:1~4,2,3:0~1',combine='spw',solint='inf',smodel=[1,0,0,0],append=True)
            print("In testFreqMetaData2b, a gaincal which should have thrown an exception did not!")
            self.assertTrue(False)
        except RuntimeError:
            self.assertTrue(True)

        #  + append=True
        gaincal(vis=datacopy,caltable=ct,scan='14,16',spw='1:1~4,2,3:4~7',combine='spw',solint='inf',smodel=[1,0,0,0],append=True)

        tb.open(ct+'/SPECTRAL_WINDOW')
        ctfreq=tb.getcol('CHAN_FREQ',1,1)  # only 1
        ctspwflag=tb.getcol('FLAG_ROW')    # should be [T,F,T,T]   # only unflagged for spw 1
        tb.close()
        fdiff=(ctfreq[0,0]-np.mean(list(msfreq[1:5,1])+list(msfreq[:,2])+list(msfreq[4:8,3])))/ctfreq[0,0]    # should be ~zero (<1e-15)

        tb.open(ct)
        ctnrows=tb.nrows()   # ctnrows should be 50 = (nant=10)*(nspw=1)*(nscan=3+2)
        tb.close()

        #print(ctnrows, ctspwflag, fdiff)
        self.assertTrue(np.all(ctspwflag==[True,False,True,True]))  # only spw 0 unflagged
        self.assertTrue(ctnrows==50)
        self.assertTrue(np.all(np.absolute(fdiff)<1e-15))

    def test_dictOutput(self):
        """ Test that a dictionary is output by the task """
        res = gaincal(vis=datacopy, caltable=tempCal)
        self.assertTrue(type(res) == dict)

    def test_dictOutputFlagged(self):
        """ Test that when an spw is flagged the final data counts are zero """
        # Flag the spw
        flagdata(vis=flagcopy, spw='0')
        # Run gaincal
        res = gaincal(vis=flagcopy, caltable=tempCal)
        toCheck = ['above_minblperant', 'above_minsnr', 'data_unflagged']

        for i in toCheck:
            self.assertTrue(np.all(res['solvestats']['spw0'][i] == 0))
        self.assertTrue(np.all(res['solvestats']['spw0']['expected'] > 0))

    def test_dictOutputAntennaFlag(self):
        """ Test that preflagging antennas shows in the output dict """
        # Flag the antenna
        flagdata(vis=flagcopy, antenna='0')
        # Run gaincal
        res = gaincal(vis=flagcopy, caltable=tempCal)
        toCheck = ['above_minblperant', 'above_minsnr', 'data_unflagged', 'used_as_refant']

        for i in toCheck:
            self.assertTrue(np.all(res['solvestats']['spw0']['ant0'][i] == 0))
        self.assertTrue(np.all(res['solvestats']['spw0']['ant0']['expected'] > 0))

    def test_dictBelowMinBl(self):
        """ Test that results will reflect ants excluded due to missing baselines """
        flagdata(vis=flagcopy, antenna='0~6')
        res = gaincal(vis=flagcopy, caltable=tempCal)

        for i in range(7, 10):
            ant = 'ant'+str(i)
            self.assertTrue(np.all(res['solvestats']['spw0'][ant]['above_minblperant'] == 0))


# local variables for gaincal_interpPD_tests
interpPDms = os.path.join(rootpath,'sim_interpPD.ms')
local_interpPDms = 'local_interpPD.ms'
local_interpPDG0 = 'local_interpPD.G0'
local_interpPDG1 = 'local_interpPD.G1'

class gaincal_interpPD_tests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.copytree(interpPDms, local_interpPDms)

    def setUp(self):
        pass

    def tearDown(self):
        rmtables(local_interpPDG0)
        rmtables(local_interpPDG1)

    @classmethod
    def tearDownClass(cls):
        rmtables(local_interpPDms)

    # We'll use this function to extract info for testing below
    def getAntSpwPha(self,ct):
        tb.open(ct)
        ant=tb.getcol('ANTENNA1')
        spw=tb.getcol('SPECTRAL_WINDOW_ID')
        pha=np.angle(tb.getcol('CPARAM'))*180/pi
        tb.close()
        return (ant,spw,pha)

    def test_nearestPD_unflagged_int(self):

        #print('test_nearestPD_unflagged_int')
        # get solution for spw='0,1,3'
        #  will try spwmap+linearPD spwmaping
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG0,spw='0,1,3',
                    solint='int',refant='9',smodel=[1,0,0,0])

        # NB: local_interpPDG1 is re-used for multiple tests below

        # incremental gaincal w/ spwmap=[0]*6
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['nearestPD'],
                    spwmap=[[0]*6])

        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # mean of all phases should be within 0.002deg, rms < 0.25deg
        self.assertAlmostEqual(np.mean(pha),0.0,delta=0.002)
        self.assertLess(np.std(pha),0.25)


        # incremental gaincal w/ spwmap=[1]*6
        # NB: ant=3 is not corrected to zeroth cycle by G0, so
        #     residual phase detected here is at non-trivial phase
        #     values (but stable)
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['nearestPD'],
                    spwmap=[[1]*6])

        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # mean of all Y phases should be within 0.01deg, rms < 0.25deg
        self.assertAlmostEqual(np.mean(pha[1,0,:]),0.0,delta=0.01)
        self.assertLess(np.std(pha[1,0,:]),0.25)

        # mean of all X phases EXCEPT ant=3 should be
        #    within 0.01deg, rms < 0.25deg
        self.assertAlmostEqual(np.mean(pha[0,0,ant!=3]),0.0,delta=0.01)
        self.assertLess(np.std(pha[0,0,ant!=3]),0.25)

        # mean of all X phases for ant=3 should be as follows (per spw):
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==0]),  -7.7396,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==1]),   0.0,   delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==2]),  54.1983,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==3]),  61.9098,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==4]),-150.9726,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==5]),-143.2347,delta=0.002)

        # incremental gaincal w/ spwmap=[3]*6
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['nearestPD'],
                    spwmap=[[3]*6])

        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # mean of all phases EXCEPT ant=3 should be
        #    within 0.05deg, rms < 0.25deg
        self.assertAlmostEqual(np.mean(pha[:,0,ant!=3]),0.0,delta=0.05)
        self.assertLess(np.std(pha[:,0,ant!=3]),0.25)

        # mean of all phases for ant=3 should be as follows (per pol,spw):
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==0]), -59.4258,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==0]),  59.4430,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==1]), -52.8221,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==1]),  52.8440,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==2]),  -6.5756,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==2]),   6.6126,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==3]),   0.0,   delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==3]),   0.0,   delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==4]),  72.7122,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==4]), -72.6589,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==5]),  79.3142,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==5]), -79.2761,delta=0.02)

    def test_linearPD_unflagged_timeINdep_combspw(self):

        #print('test_linearPD_unflagged_timeINdep_combspw')

        # get solution for combined spw='0,1', only in first timestamp
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG0,spw='0,1',
                    timerange='<2025/06/27/09:10:15',
                    combine='spw',
                    solint='inf',refant='9',smodel=[1,0,0,0])

        # incremental gaincal w/ spwmap=[0]*6 (i.e., combined 0,1 solution from above)
        # using 'linearPD', which will behave like 'nearestPD' for single solution from above
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['linearPD'],
                    spwmap=[[0]*6])

        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # Extract first phases (in time) for spw pairs (all non refant=9) for testing for zero-ish-ness
        #  (pre-applied phase was from first timestamp)
        pha01X=[pha[0,0,ant==iant][spw[ant==iant]==ispw][0] for iant in range(9) for ispw in range(2)]
        pha01Y=[pha[1,0,ant==iant][spw[ant==iant]==ispw][0] for iant in range(9) for ispw in range(2)]
        pha23X=[pha[0,0,ant==iant][spw[ant==iant]==ispw][0] for iant in range(9) for ispw in range(2,4)]
        pha23Y=[pha[1,0,ant==iant][spw[ant==iant]==ispw][0] for iant in range(9) for ispw in range(2,4)]
        pha45X=[pha[0,0,ant==iant][spw[ant==iant]==ispw][0] for iant in range(9) for ispw in range(4,6)]
        pha45Y=[pha[1,0,ant==iant][spw[ant==iant]==ispw][0] for iant in range(9) for ispw in range(4,6)]

        # We expect residual phase in spws 0,1 to be very nearest zero because applied phase originated
        #  in the combination of these spws (partially correlated noise)
        self.assertAlmostEqual(np.mean(pha01X),0.0,delta=0.001)
        self.assertAlmostEqual(np.mean(pha01Y),0.0,delta=0.001)
        self.assertLess(np.std(pha01X),0.05)
        self.assertLess(np.std(pha01Y),0.05)

        # We expect mean residual phase in spws 2,3 to be near zero, but not quite as tight,
        #  since noise from 0,1 combination is modestly magnified by freq ratio (1.17)
        self.assertAlmostEqual(np.mean(pha23X),0.0,delta=0.1)
        self.assertAlmostEqual(np.mean(pha23Y),0.0,delta=0.1)
        self.assertLess(np.std(pha23X),0.1)
        self.assertLess(np.std(pha23Y),0.1)

        # We expect mean residual phase in spws 4,5 to be near zero, but even less tight,
        #  since noise from 0,1 combination is even more magnified by freq ratio (2.62)
        self.assertAlmostEqual(np.mean(pha45X),0.0,delta=0.2)
        self.assertAlmostEqual(np.mean(pha45Y),0.0,delta=0.2)
        self.assertLess(np.std(pha45X),0.2)
        self.assertLess(np.std(pha45Y),0.2)


    def test_linearPD_unflagged_int_combspw(self):

        #print('test_nearestPD_unflagged_int_combspw')

        # get solution for combined spw='0,1'
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG0,spw='0,1',
                    combine='spw',
                    solint='int',refant='9',smodel=[1,0,0,0])


        # incremental gaincal w/ spwmap=[0]*6 (i.e., combined 0,1 solution from above)
        # using 'linearPD', which will behave like 'nearestPD' for int solution from above
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['linearPD'],
                    spwmap=[[0]*6])

        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # mean of all phases should be zero
        #    within 0.005deg, rms < 0.25deg
        self.assertAlmostEqual(np.mean(pha),0.0,delta=0.005)
        self.assertLess(np.std(pha[:,0,ant!=3]),0.25)


    def test_linearPD_unflagged_60s(self):

        #print('test_linearPD_unflagged_60s')

        # non-trivial linear interpolation (w/ PD)

        # get solution for spw='0'
        #  ~sparse 60s solution, for non-trivial linear interp below
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG0,spw='0',
                    solint='60s',refant='9',smodel=[1,0,0,0])


        # incremental gaincal w/ spwmap=[0]*6
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['linearPD'],
                    spwmap=[[0]*6])

        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # mean of all phases should be zero
        #    within 0.005deg, rms < 0.4deg
        self.assertAlmostEqual(np.mean(pha),0.0,delta=0.005)
        self.assertLess(np.std(pha),0.4)


    def test_nearestPD_flagged_int(self):

        #print('test_nearestPD_flagged_int')

        # get solution for spw='0,1,3'
        #  will try spwmap+linearPD spwmaping
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG0,spw='1',
                    solint='int',refant='9',smodel=[1,0,0,0])


        # manually set some flags in the caltable (for antid=0)
        # phase ~scrambled for the flagged solutions, to see if this disrupts
        #   phase connection in cycle counting for nearestPD
        tb.open(local_interpPDG0,nomodify=False)
        st=tb.query('ANTENNA1==0 && SPECTRAL_WINDOW_ID==1')
        fl=st.getcol('FLAG')
        g=st.getcol('CPARAM')
        fl[0,0,12:16]=True
        g[0,0,12:16:2]=np.conj(g[0,0,12:16:2])
        fl[0,0,78:90]=True
        g[0,0,78:90:2]=np.conj(g[0,0,78:90:2])
        fl[1,0,0:6]=True
        g[1,0,0:6:2]=np.conj(g[1,0,0:6:2])
        fl[1,0,14]=True
        g[1,0,14]=np.conj(g[1,0,14])
        fl[1,0,-6::]=True
        g[1,0,-6::2]=np.conj(g[1,0,-6::2])
        fl[1,0,60:78:2]=True
        g[1,0,60:78:2]=np.conj(g[1,0,60:78:2])
        fl[1,0,82:102]=True
        g[1,0,82:102:2]=np.conj(g[1,0,82:102:2])
        fl[1,0,114:132:2]=True
        g[1,0,114:132:2]=np.conj(g[1,0,114:132:2])
        g[1,0,114:132:2]*=(-1+0.1j)
        st.putcol('FLAG',fl)
        st.putcol('CPARAM',g)
        st.close()
        tb.close()


        # incremental gaincal w/ spwmap=[1]*6, some solutions flagged
        out=gaincal(vis=local_interpPDms,caltable=local_interpPDG1,
                    solint='int',refant='9',smodel=[1,0,0,0],
                    gaintable=[local_interpPDG0],
                    interp=['nearestPD'],
                    spwmap=[[1]*6])


        # Extract info from caltable for testing
        ant,spw,pha = self.getAntSpwPha(local_interpPDG1)

        # mean of all phases for ant=3 should be as follows (per pol,spw):
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==0]),  -7.7392,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==0]),   0.0,   delta=0.01)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==1]),   0.0,   delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==1]),   0.0,   delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==2]),  54.1985,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==2]),   0.0,   delta=0.01)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==3]),  61.9084,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==3]),   0.0,   delta=0.002)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==4]),-150.9723,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==4]),   0.0,   delta=0.01)
        self.assertAlmostEqual(np.mean(pha[0,0,ant==3][spw[ant==3]==5]),-143.2343,delta=0.002)
        self.assertAlmostEqual(np.mean(pha[1,0,ant==3][spw[ant==3]==5]),   0.0,   delta=0.02)



if __name__ == '__main__':
    unittest.main()
