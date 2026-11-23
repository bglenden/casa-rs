#########################################################################
# test_task_fringefit.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.fringefit.html
#
##########################################################################
import os
import sys
import shutil
import unittest
import itertools
import numpy as np

# For information about parameters that are unexpectedly zero, set
# VERBOSE to true.  Currently there are none, so this is for developers
# only
VERBOSE = False
from casatools import ms, ctsys, table
from casatasks import fringefit, flagmanager, flagdata

from casatestutils import testhelper as th
ctsys_resolve = ctsys.resolve

tblocal = table()

datapath = ctsys.resolve('unittest/fringefit/')

class Fringefit_tests(unittest.TestCase):
    prefix = 'n08c1'
    msfile = prefix + '.ms'
    uvfile = 'gaincaltest2copy.ms'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile)
        shutil.copytree(os.path.join(datapath, 'gaincaltest2.ms'), self.uvfile)

    def tearDown(self):
        shutil.rmtree(self.msfile)
        shutil.rmtree(self.prefix + '.sbdcal', True)
        shutil.rmtree(self.prefix + '-zerorates.sbdcal', True)
        shutil.rmtree(self.prefix + '.mbdcal', True)
        # shutil.rmtree(self.prefix + '.mbdcal2', True)
        shutil.rmtree(self.uvfile, True)
        shutil.rmtree('uvrange_with.cal', True)

    def test_sbd(self):
        sbdcal = self.prefix + '.sbdcal'
        fringefit(vis=self.msfile, caltable=sbdcal, refant='EF')
        reference = os.path.join(datapath, sbdcal)
        self.assertTrue(th.compTables(sbdcal, reference, ['WEIGHT', 'SNR']))

    def test_mbd(self):
        sbdcal = self.prefix + '-zerorates.sbdcal'
        mbdcal = self.prefix + '.mbdcal'
        fringefit(vis=self.msfile, caltable=sbdcal, field='4C39.25',
                  refant='EF', zerorates=True)
        fringefit(vis=self.msfile, caltable=mbdcal, field='J0916+3854',
                   combine='spw', gaintable=[sbdcal], refant='EF')
        reference = os.path.join(datapath, mbdcal)
        self.assertTrue(th.compTables(mbdcal, reference, ['WEIGHT', 'SNR']))
    # def test_mbd_combo(self):
    #     sbdcal = self.prefix + '-zerorates.sbdcal'
    #     mbdcal = self.prefix + '.mbdcal2'
    #     fringefit(vis=self.msfile, caltable=sbdcal, field='4C39.25',
    #               refant='EF', zerorates=True)
    #     fringefit(vis=self.msfile, caltable=mbdcal, field='J0916+3854',
    #                combine='spw', concatspws=False, gaintable=[sbdcal], refant='EF')
    #     reference = os.path.join(datapath, self.prefix + '.mbdcal')
    #     self.assertTrue(th.compTables(mbdcal, reference, ['WEIGHT', 'SNR']))


    def test_uvrange(self):
        ''' Check that the uvrnage parameter excludes antennas '''
        # create a caltable with uvrange selection
        fringefit(vis=self.uvfile, caltable='uvrange_with.cal', spw='2', refant='0', uvrange='<1160')

        # get the subset of antennas that are used vs all
        tblocal.open('uvrange_with.cal')
        output = tblocal.getcol('FLAG')
        antennas = tblocal.getcol('ANTENNA1')
        tblocal.close()

        flagged = set()
        intended_flagged = {5,8}

        for i in range(len(antennas)):
            if np.all(output[:, :, i] == True):
                flagged.add(antennas[i])

        self.assertTrue(flagged == intended_flagged)


class Fringefit_single_tests(unittest.TestCase):
    prefix = 'n08c1-single'
    msfile = prefix + '.ms'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile)

    def tearDown(self):
        shutil.rmtree(self.msfile)
        shutil.rmtree(self.prefix + '.sbdcal', True)
        shutil.rmtree(self.prefix + '-2.sbdcal', True)

    def test_single(self):
        sbdcal = self.prefix + '.sbdcal'
        fringefit(vis=self.msfile, caltable=sbdcal, refant='EF')
        tblocal.open(sbdcal)
        flag = tblocal.getcol('FLAG')
        tblocal.close()
        # CAS-12693: Check that the right parameters are flagged
        self.assertFalse(flag[0, 0, 0])
        self.assertFalse(flag[0, 0, 1])
        self.assertTrue(flag[0, 0, 2])
        self.assertFalse(flag[0, 0, 3])
        self.assertTrue(flag[0, 0, 4])
        self.assertTrue(flag[4, 0, 0])
        self.assertTrue(flag[4, 0, 1])
        self.assertTrue(flag[4, 0, 2])
        self.assertTrue(flag[4, 0, 3])
        self.assertTrue(flag[4, 0, 4])
    def test_param(self):
        sbdcal = self.prefix + '-2.sbdcal'
        # We make a triple cartesian product of booleans
        # to test all possible paramactive values
        eps = 1e-20
        refant = 0
        refant_s = str(refant)
        for pactive in itertools.product(*3*[[False, True]]):
            fringefit(vis=self.msfile, paramactive=list(pactive), caltable=sbdcal, refant=refant_s)
            tblocal.open(sbdcal)
            fparam = tblocal.getcol('FPARAM')
            flag = tblocal.getcol('FLAG')
            tblocal.close()
            param_names = ['delay', 'rate', 'dispersivity']
            if VERBOSE:
                print(pactive, file=sys.stderr)
            # Loop over parameters
            for i in range(2):
                # Loop over stations; it seems like station 2 is the one with non zero results
                for j in range(4):
                    if not pactive[i]:
                        self.assertTrue(abs(fparam[i+1, 0, j]) < eps )
                        self.assertTrue(abs(fparam[i+5, 0, j]) < eps)
                    else:
                        # Obviously the reference antenna show have zero values for all parameters!
                        if j==refant: continue
                        # We don't report dispersion being zero when it
                        # is included in the parameters to solve
                        # because this branch doesn't yet *solve* for
                        # dispersion
                        if i==3: continue
                        if (abs(fparam[i+1, 0, j]) < eps) and (not flag[i+1,0,j]):
                            name = param_names[i]
                            v1 = fparam[i+1, 0, j]
                            v1 = fparam[i+5, 0, j]
                            if VERBOSE:
                                print("   Parameter {} for antenna {} is {}, {}".format(name, j, v1, v1),
                                      "when it doesn't have to be",
                                      file=sys.stderr)


class Fringefit_dispersive_tests(unittest.TestCase):
    prefix = 'n14p1'
    msfile = prefix+'.ms'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile)
        flagdata(self.prefix + '.ms', mode='manual', spw='*:0~2;29~31')

    def tearDown(self):
        shutil.rmtree(self.msfile)
        shutil.rmtree(self.prefix + '.ms' + '.flagversions')
        shutil.rmtree(self.prefix + '.mpc', True)
        shutil.rmtree(self.prefix + '.disp', True)

    def test_manual_phase_cal(self):
        fringefit(vis="n14p1.ms", caltable="n14p1.mpc",
                  scan="1", solint="300", refant="WB",
                  minsnr=50, zerorates=True,
                  globalsolve=True, niter=100, gaintable=[],
                  parang=True)
        mpcal = self.prefix + '.mpc'
        reference = os.path.join(datapath, mpcal)
        self.assertTrue(th.compTables(mpcal, reference, ['WEIGHT', 'SNR']))
        fringefit(vis=self.prefix + '.ms',
                  caltable='n14p1.disp', refant="WB",
                  scan='1', solint='60', spw='0,1,2',
                  paramactive=[True, True, True],
                  minsnr=50,
                  niter=100,
                  gaintable=['n14p1.mpc'],
                  parang=True)
        dispcal = self.prefix + '.disp'
        reference = os.path.join(datapath, dispcal)
        self.assertTrue(th.compTables(dispcal, reference, ['WEIGHT', 'SNR']))


class Fringefit_refant_bookkeeping_tests(unittest.TestCase):
    prefix = 'n08c1-single'
    msfile = prefix + '.ms'
    sbdcal = prefix + '-book.sbdcal'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile)
        flagdata(self.prefix + '.ms', mode='manual', spw='*:0~2;29~31')
        flagdata(self.prefix + '.ms', mode='manual', antenna='EF')

    def tearDown(self):
        shutil.rmtree(self.msfile)
        shutil.rmtree(self.msfile + '.flagversions')
        shutil.rmtree(self.sbdcal, True)

    def test_bookkeeping(self):
        eps = 1e-2
        refant_ind = 1
        fringefit(vis=self.msfile, caltable=self.sbdcal, refant='WB')
        tblocal.open(self.sbdcal)
        fparam = tblocal.getcol('FPARAM')
        flag = tblocal.getcol('FLAG')
        tblocal.close()
        for i in range(8):
            self.assertTrue(abs(fparam[i, 0, refant_ind]) < eps )
            self.assertTrue(abs(fparam[i, 0, refant_ind]) < eps)

class FreqMetaTests(unittest.TestCase):
    prefix = 'n08c1'
    msfile = prefix + '.ms'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile)

    def tearDown(self):
        shutil.rmtree(self.msfile)
        shutil.rmtree(self.prefix + '.mbdcal', True)
        shutil.rmtree(self.prefix + '-zerorates.sbdcal', True)

    def test_metadata(self):
        sbdcal = self.prefix + '-zerorates.sbdcal'
        mbdcal = self.prefix + '.mbdcal'
        fringefit(vis=self.msfile, caltable=sbdcal, field='4C39.25',
                  refant='EF', zerorates=True)
        fringefit(vis=self.msfile, caltable=mbdcal, spw="0,1,2,3", field='J0916+3854', timerange="17:10:00~17:11:00",
                   combine='spw', gaintable=[sbdcal], refant='EF')
        tblocal.open(mbdcal + '/SPECTRAL_WINDOW')
        flagrow = tblocal.getcol('FLAG_ROW')
        # There are only 4 spws in n08c1!
        tblocal.close()
        self.assertTrue((flagrow==[False] + 3*[True]).all())
        # Only two scans in unit test ms
        # /scratch/small/UnitTest/n08c1.ms
        # 10-Mar-2008/17:06:00.0 - 17:09:00.0     1      3 4C39.25                   4320  [0,1,2,3]  [1, 1, 1, 1]
        #             17:09:00.0 - 17:11:00.0     2      2 J0916+3854                2880  [0,1,2,3]  [1, 1, 1, 1]
        try:
            fringefit(vis=self.msfile, caltable=mbdcal, spw="0:6~32,1,2,3", field='J0916+3854', timerange="17:10:00~17:11:00",
                      combine='spw', gaintable=[sbdcal], refant='EF', append=True)
            print("In test_metadata, a fringefit which should have thrown an exception did not!")
            self.assertTrue(False)
        except RuntimeError as e:
            print(e)
            self.assertTrue(True)


class Fringefit_corrcomb(unittest.TestCase):
    polcombtestms = 'gaincalcopy.ms'
    testout = 'polcombout.cal'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, 'gaincaltest2.ms'), self.polcombtestms)

    def tearDown(self):
        shutil.rmtree(self.polcombtestms)
        if os.path.exists(self.testout):
            shutil.rmtree(self.testout)

    def test_comb(self):
        fringefit(vis=self.polcombtestms, caltable=self.testout, refant='0', spw='2~3', corrcomb='none')

        tblocal.open(self.testout)
        none_result = np.nanmean(tblocal.getcol('SNR'))
        tblocal.close()

        fringefit(vis=self.polcombtestms, caltable=self.testout, refant='0', spw='2~3', corrcomb='stokes')   # formerly 'all'

        tblocal.open(self.testout)
        combine_result = np.nanmean(tblocal.getcol('SNR'))
        tblocal.close()

        self.assertTrue(combine_result > none_result)

class Fringefit_corrcomb2(unittest.TestCase):
    polcombtestms = 'TPOL0006b_scan5_copy.ms'
    testout = polcombtestms+'.ffcal'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, 'TPOL0006b_scan5.ms'),self.polcombtestms)
        # For local testing before data is in the DR
        #datapath0='/home/daibutsu/gmoellen/JIRA/CAS-14195/testdir/'
        #shutil.copytree(os.path.join(datapath0, 'TPOL0006b_scan5.ms'),self.polcombtestms)

    def tearDown(self):
        shutil.rmtree(self.polcombtestms)
        if os.path.exists(self.testout):
            shutil.rmtree(self.testout)

    def test_corrcomb2(self):

        # NB: No SNR improvement tests here (cf test_corrcomb)
        #   because these (raw) data are not coherent (not aligned)
        #   between polarizations
        #   TBD: add a solve for pol alignment (zerorates=True), and
        #        add it as a prior cal in each corrcomb!='none' test

        # corrdepflags=False, corrcomb='none'
        # ant id=6 completely flagged
        fringefit(vis=self.polcombtestms, caltable=self.testout,
                  spw='0,1',
                  refant='0',solint='inf',
                  corrdepflags=False,corrcomb='none',concatspws=False)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        tblocal.close()
        #print(np.sum(fl),np.sum(fl)==16)
        #print(fl[:,0,6::10]) # both pols
        #print(np.alltrue(fl[:,0,6::10]))

        self.assertTrue(np.sum(fl)==16)   # 4 params in 2 pols in 2 spws
        self.assertTrue(np.all(fl[:,0,6::10]))  # both pols flagged


        # corrdepflags=True, corrcomb='none'
        # ant id=6 pol id=1 only flagged
        fringefit(vis=self.polcombtestms, caltable=self.testout,
                  spw='0,1',
                  refant='0',solint='inf',
                  corrdepflags=True,corrcomb='none',concatspws=False)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        tblocal.close()
        #print(np.sum(fl),np.sum(fl)==8)
        #print(fl[4:,0,6::10])  # 2nd pol only
        #print(np.alltrue(fl[4:,0,6::10]))

        self.assertTrue(np.sum(fl)==8)   # 4 params in 1 pol in 2 spws
        self.assertTrue(np.all(fl[4:,0,6::10]))  # 2nd pol only flagged

        # corrdepflags=True, corrcomb='stokes'
        # ant id=6 completely flagged
        # solutions identical in both pols
        fringefit(vis=self.polcombtestms, caltable=self.testout,
                  spw='0,1',
                  refant='0',solint='inf',
                  corrdepflags=True,corrcomb='stokes',concatspws=False)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        sol=tblocal.getcol('FPARAM')
        tblocal.close()
        #print(np.sum(fl),np.sum(fl)==16)
        #print(fl[:,0,6::10]) # both pols
        #print(np.alltrue(fl[:,0,6::10]))
        #print(np.alltrue(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols

        self.assertTrue(np.sum(fl)==16)   # 4 params in 2 pols in 2 spws
        self.assertTrue(np.all(fl[:,0,6::10]))  # both pols flagged
        self.assertTrue(np.all(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols

        # corrdepflags=True, corrcomb='parallel'
        # nothing flagged (ant id=6 pol id=0 solutions used for both pols)
        # solutions identical in both pols
        fringefit(vis=self.polcombtestms, caltable=self.testout,
                  spw='0,1',
                  refant='0',solint='inf',
                  corrdepflags=True,corrcomb='parallel',concatspws=False)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        sol=tblocal.getcol('FPARAM')
        tblocal.close()
        #print(np.sum(fl),np.sum(fl)==0)  # no flagged solutions!
        #print(np.alltrue(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols

        self.assertTrue(np.sum(fl)==0)  # nothing flagged
        self.assertTrue(np.all(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols

        # corrdepflags=True, corrcomb='stokes', concatspws=True
        # ant id=6 fully flagged
        # solutions identical in both pols
        fringefit(vis=self.polcombtestms, caltable=self.testout,
                  spw='0,1,2,3',combine='spw',
                  refant='0',solint='inf',
                  corrdepflags=True,corrcomb='stokes',concatspws=True)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        sol=tblocal.getcol('FPARAM')
        tblocal.close()
#        print(np.sum(fl),np.sum(fl)==8)
#        print(fl[:,0,6::10]) # both pols
#        print(np.alltrue(fl[:,0,6::10]))
#        print(np.alltrue(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols
        self.assertTrue(np.sum(fl)==8)  # ant id 6 completely flagged
        self.assertTrue(np.all(fl[:,0,6::10]))  # both pols flagged
        self.assertTrue(np.all(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols


        # corrdepflags=True, corrcomb='parallel', concatspws=True
        # no solutions flagged
        # solutions identical in both pols
        fringefit(vis=self.polcombtestms, caltable=self.testout,
                  spw='0,1,2,3',combine='spw',
                  refant='0',solint='inf',
                  corrdepflags=True,corrcomb='parallel',concatspws=True)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        sol=tblocal.getcol('FPARAM')
        tblocal.close()
#        print(np.sum(fl),np.sum(fl)==0)  # no flagged solutions!
#        print(np.alltrue(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols
        self.assertTrue(np.sum(fl)==0)  # nothing flagged
        self.assertTrue(np.all(sol[0:4,0,:]==sol[4:,0,:]))  # same soln in both pols




class Fringefit_paramactive_caltable(unittest.TestCase):
    prefix = 'n08c1'
    msfile = prefix + '.ms'
    testcallib = 'testcaltable.txt'

    preapplytable = 'topreapply.cal'
    nocallib = 'nocallib.cal'
    withcallib = 'withcallib.cal'
    manualdefault = 'manualcallib.cal'
    singlechan = 'singlechan.cal'
    singletime= 'singletime.cal'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile)
        self.make_preapply_table()

    def tearDown(self):
        shutil.rmtree(self.msfile)

        if os.path.exists(self.preapplytable):
            shutil.rmtree(self.preapplytable)
        if os.path.exists(self.nocallib):
            shutil.rmtree(self.nocallib)
        if os.path.exists(self.withcallib):
            shutil.rmtree(self.withcallib)
        if os.path.exists(self.manualdefault):
            shutil.rmtree(self.manualdefault)
        if os.path.exists(self.singlechan):
            shutil.rmtree(self.singlechan)
        if os.path.exists(self.singletime):
            shutil.rmtree(self.singletime)
        if os.path.exists(self.testcallib):
            os.remove(self.testcallib)

    def make_preapply_table(self):
        # create the table to pre-apply
        fringefit(vis=self.msfile, caltable=self.preapplytable, refant='0')

        # create a callib file fot the pre-apply table
        with open(self.testcallib, 'w') as f:
            f.write(f"caltable=\'{self.preapplytable}\'")


    def test_paramactive_callib(self):
        """ Test that the default state for paramactive with callib matches [T,T,F]"""
        # run with gaintable preapply and with callib and running default paramactive
        fringefit(vis=self.msfile, caltable=self.nocallib, refant='0', docallib=False, gaintable=[self.preapplytable], paramactive=[])
        fringefit(vis=self.msfile, caltable=self.withcallib, refant='0', docallib=True, callib=self.testcallib, paramactive=[])
        fringefit(vis=self.msfile, caltable=self.manualdefault, refant='0', docallib=True, callib=self.testcallib, paramactive=[True,True,False])

        # get the FPARAM data for each table and compare
        tblocal.open(self.nocallib)
        res1 = tblocal.getcol('FPARAM')
        tblocal.close()

        tblocal.open(self.withcallib)
        res2 = tblocal.getcol('FPARAM')
        tblocal.close()

        tblocal.open(self.manualdefault)
        res3 = tblocal.getcol('FPARAM')
        tblocal.close()

        self.assertTrue(np.all(res1 == res2), msg='Results differ when preapplying with callib vs gaintable')
        self.assertTrue(np.all(res2 == res3), msg='Results differ between paramactive [] and [True,True,False]')

    def test_single_time(self):
        fringefit(vis=self.msfile, caltable=self.singletime, timerange='17:06:30~17:06:32', refant='0',
                  docallib=True, callib=self.testcallib, paramactive=[True, False,False])

    def test_single_chan(self):
        fringefit(vis=self.msfile, caltable=self.singlechan, spw='0:16', refant='0',
                  docallib=True, callib=self.testcallib, paramactive=[False,True,False])


class Fringefit_spw_surgery(unittest.TestCase):
    prefix = 'n08c1'
    msfile = prefix + '.ms'
    testout = f'{prefix}-spw.fj'

    def setUp(self):
        shutil.copytree(os.path.join(datapath, self.msfile),self.msfile)

    def tearDown(self):
        shutil.rmtree(self.msfile)
        if os.path.exists(self.testout):
            shutil.rmtree(self.testout)

    def test_spw(self):
        fringefit(vis=self.msfile, caltable=self.testout,
                  spw='0:16~31,1:0~11',
                  refant='0',solint='inf',
                  corrdepflags=False,corrcomb='none',concatspws=True)
        tblocal.open(self.testout)
        fl=tblocal.getcol('FLAG')
        tblocal.close()



if __name__ == '__main__':
    unittest.main()

