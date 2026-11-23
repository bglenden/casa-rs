#########################################################################
# test_task_flagdata.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.flagging.flagdata.html
#
##########################################################################
import os
import shutil
import unittest
import filecmp
import pprint
import numpy as np
from numpy import array
import ast
import sys

from casatasks import flagcmd, flagdata, mstransform, setjy, delmod, split
from casatools import ctsys, agentflagger, table, measures, quanta
from casatasks.private.parallel.parallel_task_helper import ParallelTaskHelper
from casatasks.private import flaghelper as fh
from casatestutils import testhelper as th

ctsys_resolve = ctsys.resolve

#
# Test of flagdata modes
#

def func_test_eq(result, total, flagged):

    print("%s of %s data was flagged, expected %s of %s" % \
    (result['flagged'], result['total'], flagged, total))
    assert result['total'] == total, \
               "%s data in total; %s expected" % (result['total'], total)
    assert result['flagged'] == flagged, \
           "%s flags set; %s expected" % (result['flagged'], flagged)

def create_input(str_text, filename):
    '''Save the string in a text file'''

    inp = filename
    cmd = str_text

    # remove file first
    if os.path.exists(inp):
        os.system('rm -f '+ inp)

    # save to a file
    with open(inp, 'w') as f:
        f.write(cmd)

    f.close()

    return

# Path for data
datapath = ctsys_resolve("unittest/flagdata/")

# Pick up alternative data directory to run tests on MMSs
testmms = False
if 'TEST_DATADIR' in os.environ:
    DATADIR = str(os.environ.get('TEST_DATADIR'))+'/flagdata/'
    if os.path.isdir(DATADIR):
        testmms = True
        datapath = DATADIR

print('flagdata tests will use data from '+datapath)

# jagonzal (CAS-4287): Add a cluster-less mode to by-pass parallel processing for MMSs as requested
if 'BYPASS_PARALLEL_PROCESSING' in os.environ:
    ParallelTaskHelper.bypassParallelProcessing(1)

# Local copy of the agentflagger tool
aflocal = agentflagger()


class test_dict_consolidation(unittest.TestCase):
    '''This could find a better place of its own, somewhere for unit tests of the parallel
    helper functions, as it can be tested independently from the tasks. For now it's
    a start as a bunch of checks specific to flagdata dictionaries and rflag in particular'''
    def test_flagdata_dict_consolidation(self):
        '''flagdata:: test return dictionary consolidation functions from parallel_task_helper'''

        def assert_dict_allclose(dicta, dictb):
            """
            Recursively assert: are dicta and dictb "allclose", in the sense that
            numpy array values will be approx-compared using np.assert_allclose()?
            Values of different types will be compared using unittest.assertEqual()

            np.assert_equal handles dictionaries but only supports exact comparisons.
            All other np. approx comparison functions don't seem to support arbitrary
            dictionaries
            """
            if not dicta:
                self.assertEqual(dicta, dictb)
                return

            for key, vala in dicta.items():
                valb = dictb[key]
                if type(vala) == dict:
                    assert_dict_allclose(vala, valb)
                elif type(vala) == np.ndarray:
                    np.testing.assert_allclose(vala, valb, rtol=1e-3)
                else:
                    self.assertEqual(vala, valb)

        # flagdata-returned dicts and their consolidated dicts
        ret_bogus = {'i_am_bogus': 3}
        cons_bogus = None

        # free version of rflag return dict for Four_ants_3C286_mms.ms
        ret_rflag_4ants_single = {'nreport': 1, 'report0':
                                  {'freqdev': array([[1, 0, 3.1-0o2], [1, 1, 2.8-0o2],
                                                     [1, 2, 2.3-0o2], [1, 3, 2.1-0o2],
                                                     [1, 4, 2.5-0o2], [1, 5, 1.6-0o2],
                                                     [2, 10, 3.6-0o2], [2, 11, 2.6-0o2],
                                                     [2, 12, 1.6-0o2], [2, 13, 1.7-0o2],
                                                     [3, 14, 1.2-0o2], [3, 15, 9.4-0o3]]),
                                   'name': 'Rflag',
                                   'timedev': array([[1, 0.0, 7.0-0o3], [1, 1, 5.9-0o3],
                                                   [1, 2, 5.7-0o3], [1, 3, 5.3-0o3],
                                                   [1, 4, 7.7-0o3], [1, 5, 5.2-0o3],
                                                   [2, 10, 2.7-0o2], [2, 11, 8.9-0o3],
                                                   [2, 12, 6.2-0o3], [2, 13, 4.9-0o3],
                                                   [3, 14, 3.6-0o3], [3, 15, 3.5-0o3]]),
                                   'type': 'rflag'}, 'type': 'list'}
        ret_rflag_4ants_1rep = { '/path/to/dummy.ms/SUBMSS/dummy.ms.0000.ms':
                            ret_rflag_4ants_single}
        cons_rflag_4ants_1rep = ret_rflag_4ants_single

        names_6rep = ['/path/to/dummy.ms/SUBMSS/dummy.ms.000{0}.ms'.format(idx) for idx in
                      range(6)]
        ret_rflag_4ants_6rep = dict(zip(names_6rep, 6*[ret_rflag_4ants_single]))
        cons_rflag_4ants_6rep = ret_rflag_4ants_single

        # free version of rflag return dict for ALMA uid___A002_X30a93d_X43e_small.ms
        ret_x43e = {'/path/to/x43e.ms/SUBMSS/x43e.ms.0000.ms':
                    {'type': 'list', 'report0':
                     {'freqdev': array([[0, 1, 0.00330866]]),
                      'type': 'rflag', 'name': 'Rflag',
                      'timedev': array([[0, 1, 1.96219644e-04]])}, 'nreport': 1},
                    '/path/to/x43e.ms/SUBMSS/x43e.ms.0003.ms':
                    {'type': 'list', 'report0':
                     {'freqdev': array([[2, 3, 0]]),
                      'type': 'rflag', 'name': 'Rflag',
                      'timedev': array([[2, 3, 0.01768084]])},
                     'nreport': 1},
                    '/path/to/x43e.ms/SUBMSS/x43e.ms.0001.ms':
                    {'type': 'list', 'report0':
                     {'freqdev': array([[2, 2, 0.0054054], [3, 3, 0]]),
                      'type': 'rflag', 'name': 'Rflag',
                      'timedev': array([[2, 2, 0.02742571], [3, 3, 0.01728651]])},
                     'nreport': 1},
                    '/path/to/x43e.ms/SUBMSS/x43e.ms.0002.ms':
                    {'type': 'list', 'report0':
                     {'freqdev': array([[3, 2, 0.00540063]]),
                      'type': 'rflag', 'name': 'Rflag',
                      'timedev': array([[3, 2,  0.02657252]])},
                     'nreport': 1}}
        cons_x43e = {'type': 'list', 'report0':
                     {'type': 'rflag', 'freqdev':
                      array([[0, 1, 0.00330866], [2, 2, 0.0054054],
                             [2, 3, 0], [3, 2, 0.00540063], [3, 3, 0]]),
                      'name': 'Rflag', 'timedev':
                      array([[0, 1, 1.96219644e-04], [2, 2, 2.74257102e-02],
                             [2, 3, 1.76808392e-02], [3, 2, 2.65725189e-02],
                             [3, 3, 1.72865119e-02]])},
                     'nreport': 1}

        # free version of rflag return dict for ngc5921.ms
        ret_ngc5921 = {'/path/to/ngc5921.ms/SUBMSS/ngc5921.ms.0002.ms':
                       {'type': 'list', 'report0': {
                           'freqdev': array([[0, 0, 0.15954576], [1, 0, 0.11957453]]),
                           'type': 'rflag', 'name': 'Rflag',
                           'timedev': array([[0, 0, 0.03786448], [1, 0, 0.03808762]])},
                        'nreport': 1},
                       '/path/to/ngc5921.ms/SUBMSS/ngc5921.ms.0000.ms':
                       {'type': 'list', 'report0': {'freqdev': array([[2, 0, 0.10978827]]),
                                                    'type': 'rflag', 'name': 'Rflag',
                                                    'timedev': array([[2, 0, 0.0282818]])},
                        'nreport': 1},
                       '/path/to/ngc5921.ms/SUBMSS/ngc5921.ms.0003.ms':
                       {'type': 'list', 'report0': {'freqdev': array([[1, 0, 0.11688346]]),
                                                    'type': 'rflag', 'name': 'Rflag',
                                                    'timedev': array([[1, 0, 0.03754368]])},
                        'nreport': 1},
                       '/path/to/ngc5921.ms/SUBMSS/ngc5921.ms.0001.ms':
                       {'type': 'list', 'report0': {'freqdev': array([[1, 0, 0.1189428],
                                                                      [2, 0, 0.10868936]]),
                                                    'type': 'rflag', 'name': 'Rflag',
                                                    'timedev': array([[1, 0, 4.12124127e-04],
                                                                      [2, 0, 2.73353433e-02]])},
                        'nreport': 1}}
        cons_ngc5921 =  {'type': 'list', 'report0':
                         {'type': 'rflag', 'name': 'Rflag','freqdev':
                          array([[0, 0, 0.15954576], [1, 0, 0.1189428], [2, 0, 0.10923881]]),
                           'timedev':
                          array([[0, 0, 0.03786448], [1, 0, 0.03754368], [2, 0, 0.02780857]])},
                         'nreport': 1}

        multi_dicts = [ret_bogus, ret_rflag_4ants_1rep, ret_rflag_4ants_6rep,
                      ret_x43e, ret_ngc5921]
        expected_cons = [cons_bogus, cons_rflag_4ants_1rep, cons_rflag_4ants_6rep,
                         cons_x43e, cons_ngc5921]

        for multi, exp_cons in zip(multi_dicts, expected_cons):
            cons = ParallelTaskHelper.consolidateResults(multi, 'flagdata')
            assert_dict_allclose(cons, exp_cons)


# Base class which defines setUp functions
# for importing different data sets
class test_base(unittest.TestCase):
    def setUp_flagdatatest(self):
        '''VLA data set, scan=2500~2600 spw=0 1 chan, RR,LL'''
        self.vis = "flagdatatest.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')

        self.unflag_ms()

    def setUp_ngc5921(self, force=False):
        '''VLA data set, scan=1~7, spw=0 63 chans, RR,LL'''
        self.vis = "ngc5921.ms"

        if force:
            # Need a fresh restart. Copy the MS
            shutil.rmtree(self.vis, True)
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        elif not os.path.exists(self.vis):
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')

        print("Unflag the MS")
        flagdata(vis=self.vis, mode='unflag', flagbackup=False)

    def setUp_alma_ms(self):
        '''ALMA MS, scan=1,8,10 spw=0~3 4,128,128,1 chans, I,XX,YY'''
        self.vis = "uid___A002_X30a93d_X43e_small.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
#        self.unflag_ms()
        flagdata(vis=self.vis, mode='unflag', flagbackup=False)

    def setUp_data4tfcrop(self):
        '''EVLA MS, 4 ants, scan=30,31 spw=0~15, 64 chans, RR,RL,LR,LL'''
        self.vis = "Four_ants_3C286.ms"
        self._check_path_move_remove_versions_unflag_etc()

    def setUp_data4preaveraging(self):
        # Four_ants_3C286_spw9_small_for_preaveraging.ms was generated with a command like:
        # mstransform(vis='Four_ants_3C286.ms',
        #             outputvis='Four_ants_3C286_spw9_small_for_preaveraging.ms',
        #             datacolumn='data',spw='9', antenna='1&2',
        #             timerange='2010/10/16/14:45:08.50~2010/10/16/14:45:11.50')
        self.vis = 'Four_ants_3C286_spw9_small_for_preaveraging.ms'
        self._check_path_move_remove_versions_unflag_etc()

    def setUp_data4timeavg(self):
        # Four_ants_3C286_spw9_small_for_preaveraging.ms was generated with a command like:
        # The different wrt data4preaveraging is that we include more rows / integrations so
        # that it is possible to run timeavg with a bigger timebin (up to 100s).
        # mstransform(vis='Four_ants_3C286.ms',
        #             outputvis='Four_ants_3C286_spw9_small_for_timeavg.ms',
        #             datacolumn='data',spw='9', antenna='1&2',
        #             timerange='2010/10/16/14:45:08.50~2010/10/16/14:46:48.50')
        self.vis = 'Four_ants_3C286_spw9_small_for_timeavg.ms'
        self._check_path_move_remove_versions_unflag_etc()

    def setUp_shadowdata1(self):
        '''ALMA ACA observation with one field in APP ref frame'''
        self.vis = "shadowAPP.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        flagdata(vis=self.vis, mode='unflag', flagbackup=False)

    def setUp_shadowdata2(self):
        '''CASA simulation data set. scan=0 spw=0, 2 chans, RR,LL'''
        self.vis = "shadowtest_part.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        flagdata(vis=self.vis, mode='unflag', flagbackup=False)

    def setUp_shadowdata_alma_small(self):
        '''ALMA simulation data set. 16 12m antennas, all baselines present. 2 time steps'''
        self.vis = "sim.alma.cycle0.compact.noisy.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH ' + os.path.join(datapath, self.vis) + ' ' + self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        flagdata(vis=self.vis, mode='unflag', flagbackup=False)

    def setUp_multi(self):
        self.vis = "multiobs.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_mwa(self):
        '''MWA data set, scan=1 spw=0, 11 chans, XX,XY,YX,YY'''
        self.vis = "testmwa.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_wtspec(self):
        # Four rows, 2 ants, 1 spw, 31 chans, 2 pols, WEIGHT_SPECTRUM col
        self.vis = "four_rows_weight_spectrum.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_floatcol(self, force=False):
        # 15 rows, 3 scans, 9 spw, mixed chans, XX,YY, FLOAT_DATA col
        self.vis = "SDFloatColumn.ms"

        if force:
            # Need a fresh restart. Copy the MS
            shutil.rmtree(self.vis, True)
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        elif not os.path.exists(self.vis):
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')

        print("Unflag the MS")

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_tsys_case(self):
        self.vis = "X7ef.tsys"

        if os.path.exists(self.vis):
            print("The CalTable is already around, just unflag")

        else:
            print("Moving data...")
            os.system('cp -RH ' + \
                      ctsys_resolve(os.path.join(datapath,self.vis)) + \
                      ' ' + self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_bpass_case(self):
        self.vis = "cal.fewscans.bpass"

        if os.path.exists(self.vis):
            print("The CalTable is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH ' + \
                      ctsys_resolve(os.path.join(datapath,self.vis)) + \
                      ' ' + self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_newcal(self):
        '''New cal table format from 4.1 onwards'''
        self.vis = "ap314.gcal"

        if os.path.exists(self.vis):
            print("The CalTable is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH ' + \
                      ctsys_resolve(os.path.join(datapath,self.vis)) + \
                      ' ' + self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_weightcol(self):
        '''Small MS with two rows and WEIGHT column'''

        inpvis = "combine-1-timestamp-2-SPW-no-WEIGHT_SPECTRUM-Same-Exposure.ms"
        self.vis = "msweight.ms"

        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,inpvis)+' ' + self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_tbuff(self):
        '''Small ALMA MS with low-amp points to be flagged with tbuff parameter'''

        self.vis = 'uid___A002_X72c4aa_X8f5_scan21_spw18_field2_corrXX.ms'
        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' ' + self.vis)

        # Copy the online flags file
        self.online = 'uid___A002_X72c4aa_X8f5_online.txt'
        self.user = 'uid___A002_X72c4aa_X8f5_user.txt'
        os.system('cp -RH '+os.path.join(datapath,self.online)+' ' + self.online)
        os.system('cp -RH '+os.path.join(datapath,self.user)+' ' + self.user)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def setUp_evla_15A_397(self):
        '''EVLA example MS wich has decreasing number of rows per chunk when traversed with VI/VB2'''

        self.vis = 'evla_15A-397_spw1_7_scan_4_6.ms'
        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' ' + self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')
        self.unflag_ms()

    def unflag_ms(self):
        aflocal.open(self.vis)
        aflocal.selectdata()
        agentUnflag={'apply':True,'mode':'unflag'}
        aflocal.parseagentparameters(agentUnflag)
        aflocal.init()
        aflocal.run(writeflags=True)
        aflocal.done()

    def extract_reports(self, report_list):
        summary_list = []

        # Extract only the type 'summary' reports
        nreps = report_list.keys()
        for rep in range(len(nreps)):
            repname = 'report'+str(rep);
            if(report_list[repname]['type']=='summary'):
                  summary_list.append(report_list[repname]);

        return summary_list

    def _check_path_move_remove_versions_unflag_etc(self):
        if os.path.exists(self.vis):
            print("The MS is already around, just unflag")
        else:
            print("Moving data...")
            os.system('cp -RH '+os.path.join(datapath,self.vis)+' '+ self.vis)

        os.system('rm -rf ' + self.vis + '.flagversions')

        self.unflag_ms()


class test_tfcrop(test_base):
    """flagdata:: Test of mode = 'tfcrop'"""

    def setUp(self):
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def test_tfcrop1(self):
        '''flagdata:: Test1 of mode = tfcrop'''
        flagdata(vis=self.vis, mode='tfcrop', correlation='ABS_RR',ntime=51.0,spw='9',
                 savepars=False, extendflags=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 4489)
        self.assertEqual(res['antenna']['ea19']['flagged'], 2294)
        self.assertEqual(res['spw']['7']['flagged'], 0)

    def test_tfcrop2(self):
        '''flagdata:: Test2 of mode = tfcrop ABS_ALL'''
        # Note : With ntime=51.0, 64-bit machines get 18696 flags, and 32-bit gets 18695 flags.
        #           As far as we can determine, this is a genuine precision-related difference.
        #           With ntime=53.0, there happens to be no difference.
        flagdata(vis=self.vis, mode='tfcrop',ntime=53.0,spw='9', savepars=False,
                 extendflags=False)
        res = flagdata(vis=self.vis, mode='summary', spw='9')
        self.assertEqual(res['flagged'], 18671)
        self.assertEqual(res['correlation']['LL']['flagged'], 4250)
        self.assertEqual(res['correlation']['RL']['flagged'], 5007)
        self.assertEqual(res['correlation']['LR']['flagged'], 4931)
        self.assertEqual(res['correlation']['RR']['flagged'], 4483)

    # Remove this test once Scott fixes Jenkins!!!
    def test2(self):
        '''flagdata:: Test2 of mode = tfcrop ABS_ALL'''
        # Note : With ntime=51.0, 64-bit machines get 18696 flags, and 32-bit gets 18695 flags.
        #           As far as we can determine, this is a genuine precision-related difference.
        #           With ntime=53.0, there happens to be no difference.
        flagdata(vis=self.vis, mode='tfcrop',ntime=53.0,spw='9', savepars=False,
                 extendflags=False)
        res = flagdata(vis=self.vis, mode='summary', spw='9')
        self.assertEqual(res['flagged'], 18671)
        self.assertEqual(res['correlation']['LL']['flagged'], 4250)
        self.assertEqual(res['correlation']['RL']['flagged'], 5007)
        self.assertEqual(res['correlation']['LR']['flagged'], 4931)
        self.assertEqual(res['correlation']['RR']['flagged'], 4483)

    def test_extendpols(self):
        '''flagdata:: Extend the flags created by clip'''
        flagdata(vis=self.vis, mode='clip', correlation='abs_rr', clipminmax=[0,2])
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['RR']['flagged'], 43)
        self.assertEqual(res['correlation']['LL']['flagged'], 0)
        flagdata(vis=self.vis, mode='extend', extendpols=True, savepars=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', correlation='Ll'), 1099776, 43)

    def test_extendtime(self):
        '''flagdata:: Extend the flags created by tfcrop'''
        flagdata(vis=self.vis, mode='tfcrop', extendflags=False)
        # The total per spw:channel/baseline/correlation/scan is 89.
        # Show only the ones that are 50% of the total, 44 flags, These should grow
        pre = flagdata(vis=self.vis, mode='summary', spwchan=True, basecnt=True, correlation='RR',spw='5',
                       antenna='ea11&&ea19')
        # these should grow later
        self.assertEqual(pre['spw:channel']['5:10']['flagged'], 118)
        self.assertEqual(pre['spw:channel']['5:10']['total'], 179)
        self.assertEqual(pre['spw:channel']['5:28']['flagged'], 128)
        self.assertEqual(pre['spw:channel']['5:29']['flagged'], 151)

        # these should not grow later. After the consolidation of MMS summaries
        # is fixed, revise this test
#        self.assertEqual(pre['spw:channel']['5:11']['flagged'], 32)
#        self.assertEqual(pre['spw:channel']['5:12']['flagged'], 29)
#        self.assertEqual(pre['spw:channel']['5:21']['flagged'], 34)

        # Extend in time only
        flagdata(vis=self.vis, mode='extend', extendpols=False, growtime=50.0, growfreq=0.0,
                 growaround=False,flagneartime=False,flagnearfreq=False,savepars=False)
        pos = flagdata(vis=self.vis, mode='summary', spwchan=True, basecnt=True, correlation='RR',spw='5',
                       antenna='ea11&&ea19')
        self.assertEqual(pos['spw:channel']['5:10']['flagged'], 179)
        self.assertEqual(pos['spw:channel']['5:10']['total'], 179)
        self.assertEqual(pos['spw:channel']['5:28']['flagged'], 179)
        self.assertEqual(pos['spw:channel']['5:29']['flagged'], 179)

        # These did not grow
#        self.assertEqual(pos['spw:channel']['5:11']['flagged'], 32)
#        self.assertEqual(pos['spw:channel']['5:12']['flagged'], 29)
#        self.assertEqual(pos['spw:channel']['5:21']['flagged'], 34)

    def test_extendfreq(self):
        '''flagdata:: Extend the flags created manually for one scan only'''
        flagdata(vis=self.vis, mode='manual',spw='*:0~35',timerange='2010/10/16/14:45:00~14:45:20')
        pre = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(pre['scan']['31']['flagged'], 0)
        self.assertEqual(pre['scan']['30']['flagged'], 165888)
        self.assertEqual(pre['flagged'], 165888)

        # Extend in frequency only
        flagdata(vis=self.vis, mode='extend', extendpols=False, growtime=0.0, growfreq=50.0, savepars=False)
        pos = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(pos['scan']['31']['flagged'], 0)
        self.assertEqual(pos['flagged'], 294912)

    def test_tfcrop_extendflags(self):
        '''flagdata: mode tfcrop with extendflags=True'''
        # First, extend the flags manually
        flagdata(vis=self.vis, mode='tfcrop', extendflags=False, flagbackup=False)
        flagdata(vis=self.vis, mode='extend', flagbackup=False,
                 extendpols=True, growtime=50.0, growfreq=80.0)
        pre = flagdata(vis=self.vis, mode='summary', spw='0')
        self.assertEqual(pre['spw']['0']['flagged'], 27768)
        self.assertEqual(pre['spw']['0']['total'], 274944)

#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()

        # Now, extend the flags automatically and compare
        flagdata(vis=self.vis, mode='tfcrop', spw='0', extendflags=True, flagbackup=False)
        pos = flagdata(vis=self.vis, mode='summary', spw='0')

        # Flags should be extended in time if > 50%, freq > 80% and
        # will extend to the other polarizations too.
        self.assertEqual(pos['spw']['0']['flagged'], pre['spw']['0']['flagged'])


class test_rflag(test_base):
    """flagdata:: Test of mode = 'rflag'"""

    def setUp(self):
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')
        if os.path.exists('outcmd.txt'):
            os.system('rm -rf outcmd.txt')
        if os.path.exists('rflag_output_thresholds_freqdev0.txt'):
            os.system('rm -rf rflag_output_thresholds_*')

    def cleanup_threshold_txt_files(self):
        os.remove('tdevfile.txt')
        os.remove('fdevfile.txt')

    def test_rflag_auto_thresholds(self):
        '''flagdata:: mode = rflag : automatic thresholds'''
        flagdata(vis=self.vis, mode='rflag', spw='9,10', timedev=[], freqdev=[], flagbackup=False,
                 extendflags=False)
        res = flagdata(vis=self.vis, mode='summary',spw='7,9,10')
        self.assertEqual(res['flagged'], 42728.0)
        self.assertEqual(res['antenna']['ea19']['flagged'], 18411.0)
        self.assertEqual(res['spw']['7']['flagged'], 0)

    def test_rflag_partial_thresholds(self):
        '''flagdata:: mode = rflag : partially-specified thresholds'''
        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev=[[1, 10, 0.1],[1, 11, 0.07]], freqdev=0.5,
                 flagbackup=False, extendflags=False)
        res = flagdata(vis=self.vis, mode='summary',spw='9,10,11')
        self.assertEqual(res['flagged'], 24494)
        self.assertEqual(res['antenna']['ea19']['flagged'], 11413)
        self.assertEqual(res['spw']['11']['flagged'], 0)

    def test_rflag_numpy_types(self):
        '''flagdata:: mode = rflag : partially-specified thresholds using numpy types'''
        # Results should be the same as in test_rflag_partial_thresholds above
        t1 = [np.int32(1), 10, np.float32(0.1)]
        t2 = [1, np.int16(11), np.float64(0.07)]

        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev=[t1, t2], freqdev=0.5,
                 flagbackup=False, extendflags=False)
        res = flagdata(vis=self.vis, mode='summary',spw='9,10,11')
        self.assertEqual(res['flagged'], 24494)
        self.assertEqual(res['antenna']['ea19']['flagged'], 11413)
        self.assertEqual(res['spw']['11']['flagged'], 0)

    def test_rflag_calculate_file_apply_scales(self):
        '''flagdata:: mode = rflag : use output/input time/freq threshold files via two methods, and with different scales'''

        def check_threshold_files_saved(timedev_filename, freqdev_filename):

            self.assertTrue(os.path.isfile(freqdev_filename))
            self.assertTrue(os.path.isfile(timedev_filename))

            freqdev_str = open(freqdev_filename, 'r').read()
            saved_freqdev = ast.literal_eval(freqdev_str)
            self.assertTrue('freqdev' in saved_freqdev and 'name' in saved_freqdev)
            self.assertEqual(saved_freqdev['name'], 'Rflag')
            self.assertEqual(len(saved_freqdev['freqdev']), 2)
            # wider tolerance for parallel/MMS CAS-10202
            if not testmms:
                rtol = 1e-5
            else:
                rtol = 5e-2
            np.testing.assert_allclose(
                saved_freqdev['freqdev'], [[1, 9, 0.01583025], [1, 10, 0.04113872]],
                rtol=rtol)

            timedev_str = open(timedev_filename, 'r').read()
            saved_timedev = ast.literal_eval(timedev_str)
            self.assertTrue('timedev' in saved_timedev and 'name' in saved_timedev)
            self.assertEqual(len(saved_timedev['timedev']), 2)
            if not testmms:
                rtol = 1e-5
            else:
                rtol = 2e-1
            np.testing.assert_allclose(
                saved_timedev['timedev'], [[1, 9, 0.00777182], [1, 10, 0.03256665]],
                rtol=rtol)

        # (1) Test input/output files, through the task, mode='rflag'
        # Files tdevfile.txt and fdevfile.txt are created in this step
        #  step 1: calculate thresholds and write them in text files
        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev='tdevfile.txt', freqdev='fdevfile.txt',
                 action='calculate', extendflags=False)

        self.assertTrue(os.path.exists('tdevfile.txt'))
        self.assertTrue(os.path.exists('fdevfile.txt'))
        check_threshold_files_saved('tdevfile.txt', 'fdevfile.txt')

        #  step 2: apply thresholds using text files as input
        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev='tdevfile.txt', freqdev='fdevfile.txt',
                 timedevscale=5.0, freqdevscale=5.0,
                 action='apply', flagbackup=False, extendflags=False)
        res1 = flagdata(vis=self.vis, mode='summary', spw='9,10')

        # unflag like flagdata(vis=self.vis,mode='unflag', flagbackup=False)
        self.unflag_ms()

        # (2) Test rflag output written to cmd file via mode='rflag' and 'savepars'
        #      and then read back in via list mode.
        #      Also test the 'savepars' when timedev and freqdev are specified differently...
        flagdata(vis=self.vis, mode='rflag', spw='9,10', timedev='',
                 freqdev=[], action='calculate',
                 extendflags=False, savepars=True, outfile='outcmd.txt');
        self.assertTrue(os.path.exists('outcmd.txt'))

        # Note: after CAS-5808, when mode='rflag' and action='calculate' the
        # time/freqdevscale parameters are not considered for the calculation of the
        # thresholds. The scale factors will be used when action='calculate'.
        flagdata(vis=self.vis, mode='list', inpfile='outcmd.txt', flagbackup=False)
        res2 = flagdata(vis=self.vis, mode='summary', spw='9,10')

        # A normal 'apply' (res1) and a mode='list' run apply (res2) should match:

        # 'not testmms' for CAS-10202 differences
        if not testmms:
            flagged_cnt = 39504
        else:
            flagged_cnt = 42740
        self.assertEqual(res1['flagged'], flagged_cnt)
        self.assertLessEqual(res1['flagged'] - res2['flagged'], 20)

        # (3) Now try different scales with the same input time/freqdevscale files
        self.unflag_ms()
        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev='tdevfile.txt', freqdev='fdevfile.txt',
                 timedevscale=5.0, freqdevscale=5.0,
                 action='apply', extendflags=False);
        res_scale5 = flagdata(vis=self.vis, mode='summary', spw='9,10')
        self.assertEqual(res_scale5['flagged'], flagged_cnt)

        self.unflag_ms()
        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                              timedev='tdevfile.txt', freqdev='fdevfile.txt',
                              timedevscale=4.1, freqdevscale=4.1,
                              action='apply', extendflags=False);
        res_scale4 = flagdata(vis=self.vis, mode='summary', spw='9,10')
        if not testmms:
            flagged_cnt = 51057
        else:
            flagged_cnt = 55159
        self.assertEqual(res_scale4['flagged'], flagged_cnt)

        self.cleanup_threshold_txt_files()

    def test_rflag_calculate_dict_then_apply(self):
        '''flagdata:: mode = rflag : output/input via returned dictionary and cmd'''
        # (1) Test input/output files, through the task, mode='rflag'
        # Files tdevfile.txt and fdevfile.txt are created in this step

        rdict = flagdata(vis=self.vis, mode='rflag', spw='9,10', timedev='',
                          freqdev='', action='calculate', extendflags=False)

        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev=rdict['report0']['timedev'],
                 freqdev=rdict['report0']['freqdev'],
                 timedevscale=2.5, freqdevscale=2.5,
                 action='apply', flagbackup=False, extendflags=False)
        res1 = flagdata(vis=self.vis, mode='summary', spw='9,10')

        # unflag like flagdata(vis=self.vis,mode='unflag', flagbackup=False)
        self.unflag_ms()

        # (2) Test rflag output written to cmd file via mode='rflag' and 'savepars'
        #      and then read back in via list mode.
        #      Also test the 'savepars' when timedev and freqdev are specified differently...
        flagdata(vis=self.vis, mode='rflag', spw='9,10',
                 timedev='', freqdev=[], action='calculate', extendflags=False,
                 timedevscale=2.5, freqdevscale=2.5,
                 savepars=True, outfile='outcmd.txt')
        flagdata(vis=self.vis, mode='list', inpfile='outcmd.txt', flagbackup=False)
        res2 = flagdata(vis=self.vis, mode='summary', spw='9,10')

        # Differences with parallel/MMS because of CAS-10202
        if not testmms:
            self.assertEqual(res1['flagged'], res2['flagged'])
            self.assertEqual(res1['flagged'], 98403)
            rtol_time = 1e-4
            rtol_freq = 1e-4
        else:
            self.assertEqual(res1['flagged'], 104710)
            self.assertEqual(res2['flagged'], 105560)
            rtol_freq = 5e-2
            rtol_time = 1.2e-1

        np.testing.assert_allclose(rdict['report0']['freqdev'], [[1, 9, 0.01583],
                                                                 [1, 10, 0.041139]],
                                   rtol=rtol_freq)
        np.testing.assert_allclose(rdict['report0']['timedev'], [[1, 9, 7.771820e-03],
                                                                 [1, 10, 3.256665e-02]],
                                   rtol=rtol_time)

    def test_rflag_correlation_selection(self):
        '''flagdata:: mode = rflag : correlation selection'''
        flagdata(vis=self.vis, mode='rflag', spw='9,10', correlation='rr,ll', flagbackup=False,
                 extendflags=False)
        res = flagdata(vis=self.vis, mode='summary',spw='9,10')
        self.assertEqual(res['correlation']['RR']['flagged'], 9781.0)
        self.assertEqual(res['correlation']['LL']['flagged'], 10355.0)
        self.assertEqual(res['correlation']['LR']['flagged'], 0,)
        self.assertEqual(res['correlation']['RL']['flagged'], 0,)

    def test_rflag_CAS_5037(self):
        '''flagdata:: Use provided value for time stats, but automatically computed value for freq. stats'''
        flagdata(vis=self.vis, mode='rflag', field = '1', spw='10', timedev=0.1, \
                 timedevscale=5.0, freqdevscale=5.0, action='calculate', flagbackup=False)

    def test_rflag_return_dict1(self):
        '''flagdata:: Use provided value for time stats, but automatically computed value for freq. stats - returning dictionary'''

        rflag_dict = flagdata(vis=self.vis, mode='rflag', field = '1', spw='10', timedev=0.1, \
                 timedevscale=5.0, freqdevscale=5.0, action='calculate', flagbackup=False)

        fdev = rflag_dict['report0']['freqdev']
        tdev = rflag_dict['report0']['timedev']

        self.assertTrue(isinstance(fdev, np.ndarray))
        self.assertEqual(fdev.ndim, 2)
        self.assertEqual(fdev.shape, (1,3))
        self.assertEqual(fdev[0, 0], 1)
        self.assertEqual(fdev[0, 1], 10.0)
        # TODO: The tolerance used to be 1e-5 when this test was disabled for MMS. It might
        # be possible to use a finer tolerance again if a sound solution for CAS-10202 is
        # found (and this small test dataset is well behaved).
        np.testing.assert_allclose(fdev[0,2], 0.0410, rtol=5e-3)

        self.assertTrue(isinstance(tdev, np.ndarray))
        self.assertEqual(tdev.ndim, 2)
        self.assertEqual(tdev.shape, (0,3))

    def test_rflag_extendflags(self):
        '''flagdata: automatically extend the flags after rflag'''
        # Manually extend the flags
        flagdata(vis=self.vis, mode='rflag', spw='9,10', flagbackup=False,
                 extendflags=False)
        flagdata(vis=self.vis, mode='extend', growtime=50.0, growfreq=80.0,
                 extendpols=True, flagbackup=False)
        pre = flagdata(vis=self.vis, mode='summary', spw='9,10')

#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()

        # Automatically extend the flags by default
        flagdata(vis=self.vis, mode='rflag', spw='9,10', flagbackup=False)
        pos = flagdata(vis=self.vis, mode='summary', spw='9,10')
        self.assertEqual(pos['spw']['9']['flagged'], pre['spw']['9']['flagged'])
        self.assertEqual(pos['spw']['10']['flagged'], pre['spw']['10']['flagged'])

    def test_rflag_extendflags_list_mode(self):
        '''flagdata: in list mode extend the flags automatically after rflag'''
        def getcounts():
            ### Channel 51 should extend flags, but channel 52 should not.
            counts = flagdata(vis=self.vis, mode='summary',spw='4',antenna='ea01&&ea11',
                              scan='30',correlation='LL',spwchan=True)
            chan51 = counts['spw:channel']['4:51']
            chan52 = counts['spw:channel']['4:52']

            counts = flagdata(vis=self.vis, mode='summary',spw='4',antenna='ea01&&ea11',
                              scan='30',correlation='RL',spwchan=True)

            chan51rl=counts['spw:channel']['4:51']

            return chan51, chan52, chan51rl

        # do not extend the flags
        cmdlist = ["mode='rflag' spw='4' freqdevscale=4.0 extendflags=False"]
        flagdata(vis=self.vis, mode='list', inpfile=cmdlist, flagbackup=False)
        chan51, chan52, chan51rl = getcounts()

        # Unflag
        flagdata(vis=self.vis, mode='unflag', spw='4', flagbackup=False)

        # automatically extend the flags
        cmdlist = ["mode='rflag' spw='4' freqdevscale=4.0"]
        flagdata(vis=self.vis, mode='list', inpfile=cmdlist, flagbackup=False)
        achan51, achan52, achan51rl = getcounts()

        if chan51['flagged']/chan51['total']>0.5 and achan51['flagged']/achan51['total']==1.0 :
            print('Channel 51 had more than 50% and got extended. PASS')
        else:
            self.fail('Channel 51 failed')

        if chan52['flagged']/chan52['total']<50.0 and achan52['flagged']/achan52['total']==chan52['flagged']/chan52['total']:
            print('Channel 52 had less than 50% and did not get extended. PASS')
        else:
            self.fail('Channel 52 failed')

        if chan51rl['flagged']/chan51rl['total']<0.5 and achan51rl['flagged']/achan51rl['total']==1.0:
            print('Channel 51 in RL had less than 50% but got completely flagged because Channel 51 in LL got extended. PASS')
        else:
            self.fail('Channel 51 extendpols failed')

    def test_rflag_summary_list(self):
        '''flagdata: rflag and summaries in list mode'''
        fcmd = ["mode='summary' spw='7,9,10' name='InitFlags'",
                "mode='rflag' spw='9,10' timedev=[] freqdev=[] extendflags=False",
                "mode='summary' spw='7,9,10' name='RflagFlags'"]

        res = flagdata(vis=self.vis, mode='list', inpfile=fcmd, flagbackup=False)
        self.assertEqual(res['report0']['flagged'],0)
        self.assertEqual(res['report1']['flagged'], 42728)
        self.assertEqual(res['report1']['antenna']['ea19']['flagged'], 18411)
        self.assertEqual(res['report1']['spw']['7']['flagged'], 0,)

    def test_rflag_residual_data(self):
        '''flagdata: rflag using MODEL and virtual MODEL columns'''

        # Delete model columns, if any
        delmod(vis=self.vis,otf=True,scr=True)

        # Create MODEL_COLUMN
        setjy(vis=self.vis, field='3C286_A',usescratch=True)

        # rflag
        flagdata(vis=self.vis, mode='rflag', spw='9,10',datacolumn='RESIDUAL_DATA',flagbackup=False)
        # 448772.0 flags on MODEL col
        # '1': {'flagged': 8224.0
        flags_mod = flagdata(vis=self.vis, mode='summary',spw='9,10')

        # Now use a virtual MODEL column
        # Unflag
        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        delmod(vis=self.vis,otf=True,scr=True)

        # Create virtual MODEL_COLUMN
        setjy(vis=self.vis, field='3C286_A',usescratch=False)

        # rflag
        flagdata(vis=self.vis, mode='rflag', spw='9,10',datacolumn='RESIDUAL_DATA',flagbackup=False)
        # 444576.0 flags on virtual MODEL col
        flags_vmod = flagdata(vis=self.vis, mode='summary',spw='9,10')

        # Flags should be the same
        self.assertTrue(flags_mod['flagged'],flags_vmod['flagged'])

        # This test is mischievous, manipulates the model column. Don't leave a messed up MS.
        os.system('rm -rf {0}'.format(self.vis))

class test_rflag_evla(test_base):
    """flagdata:: Test of mode = 'rflag'"""

    def setUp(self):
        self.setUp_evla_15A_397()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf evla_15A-397_spw1_7_scan_4_6.ms*')

    def test_rflag_CAS_13360(self):
        '''flagdata:: rflag in a MS which has decreasing number of rows in subsequent chunks'''

        flagdata(vis='evla_15A-397_spw1_7_scan_4_6.ms', mode='rflag', \
                 datacolumn='data', ntime='scan', combinescans=False, \
                 extendflags=False, winsize=3, timedev='', freqdev='',\
                 timedevscale=5.0, freqdevscale=5.0, spectralmax=1000000.0, \
                 spectralmin=0.0, flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 3185281)


class test_shadow(test_base):
    def setUp(self):
        self.setUp_shadowdata2()

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists('shadowtest_part.ms'):
            os.system('rm -rf shadowtest_part.ms*')
        if os.path.exists('shadowAPP.ms'):
            os.system('rm -rf shadowAPP.ms*')
        if os.path.exists('sim.alma.cycle0.compact.noisy.ms'):
            os.system('rm -rf sim.alma.cycle0.compact.noisy.ms*')
        if os.path.exists('cas2399.txt'):
            os.system('rm -rf cas2399.txt*')
        if os.path.exists('myants.txt'):
            os.system('rm -rf myants.txt*')
        if os.path.exists('listfile.txt'):
            os.system('rm -rf listfile.txt*')
        if os.path.exists('withdict.txt'):
            os.system('rm -rf withdict.txt*')

    def _check_uvw_match_shadow(self, vis, ant_distance=12.0, tolerance=0.0):
        """Compare (and assert for equality) flags applied to this MS by
        flagdata/mode='shadow' against flags derived from calculations
        based on the UVW column of the MS. These calculations follow
        the spec in the docs: uvdist < (ant_idstance - tolerance),
        where the uv_dist is calculated from the MS UVW, and the sign
        of the W is used to decide which antenna from the baseline is
        flagged by the other one.

        This is a very rudimentary re-implemenation of shadow, meant
        to be used when shadow flagging is applied based only on
        column UVW (when all antennas are found in the baselines
        present in the data, without involving computeAntUVW()).

        :param: vis: an MS flagged with flagdata/mode='shadow'
        :param: vis: antenna distances ((diam1 + diam2/2) - all antennas same
                     size assumed
        :param: tolerance: shadow method tolerance

        """
        tbt = table()
        try:
            tbt.open(vis)
            uvw = tbt.getcol('UVW')
            flags = tbt.getcol('FLAG')
            ant1 = tbt.getcol('ANTENNA1')
            ant2 = tbt.getcol('ANTENNA2')
            col_time = tbt.getcol('TIME')
            self.assertEqual(uvw.shape[1], flags.shape[2])
            self.assertEqual(uvw.shape[1], flags.shape[2])

            # collapse dimensions 0 (pols) and 1 (channels) (should be the same as
            # tbt.getcol('FLAG_ROW') but not using it as it is ~deprecated, albeit updated
            # correctly by flagdata at least)
            row_flagged = np.all(flags, (0,1))

            uv_dist = np.sqrt(np.multiply(uvw[0],uvw[0]) + np.multiply(uvw[1],uvw[1]))
            flagged_by_uvw = (uv_dist > 0) & (uv_dist < ant_distance - tolerance)
            ant1_shadowed_uvw = flagged_by_uvw & (uvw[2] < 0)
            ant2_shadowed_uvw = flagged_by_uvw & (uvw[2] >= 0)

            # Go from shadowed antenna to all baselines that have that one (same time step)
            # This is inefficient code - should only be used with few timesteps
            baselines_flagged_from_ants = np.full(ant1_shadowed_uvw.shape, False)
            for idx in np.arange(0, len(ant1_shadowed_uvw)):
                ant_shadowed = None
                if ant1_shadowed_uvw[idx]:
                    ant_shadowed = ant1[idx]
                elif ant2_shadowed_uvw[idx]:
                    ant_shadowed = ant2[idx]
                if ant_shadowed:
                    for flag_idx in np.arange(0, len(ant1_shadowed_uvw)):
                        if col_time[idx] != col_time[flag_idx]:
                            continue
                        if ant1[flag_idx] == ant_shadowed or ant2[flag_idx] == ant_shadowed:
                            baselines_flagged_from_ants[flag_idx] = True

            np.testing.assert_array_equal(baselines_flagged_from_ants, row_flagged,
                                          'mismatch between flags set and flags expected '
                                          'from UVW distances found in the MS.')

        finally:
            tbt.close()

    def test_CAS2399(self):
        '''flagdata: shadow by antennas not present in MS'''

        if os.path.exists("cas2399.txt"):
            os.system('rm -rf cas2399.txt')

        myinput = 'name=VLA01\n'+\
                'diameter=25.0\n'+\
                'position=[-1601144.96146691, -5041998.01971858, 3554864.76811967]\n'+\
                'name=VLA02\n'+\
                'diameter=25.0\n'+\
                'position=[-1601105.7664601889, -5042022.3917835914, 3554847.245159178]\n'+\
                'name=VLA09\n'+\
                'diameter=25.0\n'+\
                'position=[-1601197.2182404203, -5041974.3604805721, 3554875.1995636248]\n'+\
                'name=VLA10\n'+\
                'diameter=25.0\n'+\
                'position=[-1601227.3367843349,-5041975.7011900628,3554859.1642644769]\n'

        filename = 'cas2399.txt'
        create_input(myinput, filename)

        flagdata(vis=self.vis, mode='shadow', tolerance=0.0, addantenna=filename,flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        ##print(res['antenna']['VLA3']['flagged'], res['antenna']['VLA4']['flagged'], res['antenna']['VLA5']['flagged'])
        self.assertEqual(res['antenna']['VLA3']['flagged'], 3752)
        self.assertEqual(res['antenna']['VLA4']['flagged'], 1320)
        self.assertEqual(res['antenna']['VLA5']['flagged'], 1104)

    def test_addantenna(self):
        '''flagdata: use antenna file in list mode'''
        if os.path.exists("myants.txt"):
            os.system('rm -rf myants.txt')

        # Create antennafile in disk
        myinput = 'name=VLA01\n'+\
                'diameter=25.0\n'+\
                'position=[-1601144.96146691, -5041998.01971858, 3554864.76811967]\n'+\
                'name=VLA02\n'+\
                'diameter=25.0\n'+\
                'position=[-1601105.7664601889, -5042022.3917835914, 3554847.245159178]\n'+\
                'name=VLA09\n'+\
                'diameter=25.0\n'+\
                'position=[-1601197.2182404203, -5041974.3604805721, 3554875.1995636248]\n'+\
                'name=VLA10\n'+\
                'diameter=25.0\n'+\
                'position=[-1601227.3367843349,-5041975.7011900628,3554859.1642644769]\n'

        antfile = 'myants.txt'
        create_input(myinput, antfile)

        # Create list file
        myinput = "mode='shadow' tolerance=0.0 addantenna='myants.txt'"
        filename = 'listfile.txt'
        create_input(myinput, filename)

        # Flag
        flagdata(vis=self.vis, mode='list', inpfile=filename, savepars=True, outfile='withdict.txt',
                 flagbackup=False)

        # Check flags
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['antenna']['VLA3']['flagged'], 3752)
        self.assertEqual(res['antenna']['VLA4']['flagged'], 1320)
        self.assertEqual(res['antenna']['VLA5']['flagged'], 1104)

    def test_vla_without_addantenna(self):
        '''flagdata: simple mode shadow test without adding antennas'''
        flagdata(vis=self.vis, mode='shadow', flagbackup=False)

        # Check flags
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 16116)
        self.assertEqual(res['antenna']['VLA0']['flagged'], 7920)

    def test_shadow_APP(self):
        '''flagdata: flag shadowed antennas with ref frame APP'''
        # After CAS-12555 this test no longer flags any data (computeAntUVW not used when
        # all antennas present in baselines found in data). See test_shadow_APP_with_sel
        self.setUp_shadowdata1()
        flagdata(vis=self.vis, flagbackup=False, mode='shadow')
        res = flagdata(self.vis, mode='summary')
        self.assertEqual(res['flagged'], 0)
        # This MS is not flagged at all, and has a mix of 7m and 12m antennas. Skip:
        # self._check_uvw_match_shadow(self.vis, 12.0)

    def test_shadow_alma_small(self):
        '''flagdata: flag shadowe antennas, compare with expected calculations based on the UVWs from the MS'''
        # For this test we need:
        # - an ALMA MS, without missing baselines (all antenna combinations present for all
        #   time steps).
        # - not too many time steps (check_uvw_match_shadow could be slow)
        # - antennas of same size, for simplicity,
        #
        # 'uid___A002_X30a93d_X43e_small.ms' is not appropriate, it has missing baselines ->
        #  triggers the 2nd block of calculations in shadow (computeAntUVW())

        # all baselines present, 12m antennas
        self.setUp_shadowdata_alma_small()

        ant_diameter = 12
        # This dataset does not really have direct shadowing.
        # tolerance<0 induces a bit of shadowing (forces antennas apart)
        tolerance = -6.5
        flagdata(vis=self.vis, flagbackup=False, mode='shadow', tolerance=tolerance)
        res = flagdata(self.vis, mode='summary')
        self.assertEqual(res['flagged'], 7680)
        self.assertEqual(res['antenna']['A01']['flagged'], 7680)
        self._check_uvw_match_shadow(self.vis, ant_diameter, tolerance)

    def test_shadow_APP_with_sel(self):
        '''flagdata: flag shadowed antennas with ref frame APP, selecting some antennas -> triggering computeAntUVW calculations'''
        self.setUp_shadowdata1()
        # Try to pick antennas such that the available UVW distances (shadow-code-block-1)
        # leave enough untouched baselines for the phase-center distances
        # (shadow-code-block-2 == computeAntUVW) to be able to flag them. CAS-12555
        ants = 'DA61,PM02,PM03,CM07,DV18,CM06,DA59,DV20,DV24,CM12'
        flagdata(vis=self.vis, antenna=ants, flagbackup=False, mode='shadow')
        res = flagdata(self.vis, mode='summary')
        self.assertEqual(res['flagged'], 720)
        self.assertEqual(res['total'], 6552)
        nflags_ants = {'CM10': 360, 'CM04': 360}
        for ant, nflags in nflags_ants.items():
            self.assertEqual(res['antenna'][ant]['flagged'], nflags)


class test_msselection(test_base):

    def setUp(self):
        self.setUp_ngc5921(True)

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ngc5921.ms*')
        if os.path.exists('cas9366.flags.txt'):
            os.system('rm -rf cas9366.flags.txt')
        if os.path.exists('listauto.txt'):
            os.system('rm -rf listauto.txt')

    def test_simple(self):
        '''flagdata: select only cross-correlations'''
        baselines = flagdata(vis = self.vis, mode="summary", antenna="VA09", basecnt=True)['baseline'].keys()
        assert "VA09&&VA09" not in baselines
        assert "VA09&&VA10" in baselines
        assert "VA09&&VA11" in baselines
        assert "VA10&&VA10" not in baselines
        assert "VA10&&VA11" not in baselines

        baselines = flagdata(vis = self.vis, mode="summary", antenna="VA09,VA10", basecnt=True)['baseline'].keys()
        assert "VA09&&VA09" not in baselines
        assert "VA09&&VA10" in baselines
        assert "VA09&&VA11" in baselines
        assert "VA10&&VA10" not in baselines
        assert "VA10&&VA11" in baselines

    def test_amp(self):
        '''flagdata: select only cross-correlations'''
        baselines = flagdata(vis = self.vis, mode="summary", antenna="VA09,VA10&",basecnt=True)['baseline'].keys()
        assert "VA09&&VA09" not in baselines
        assert "VA09&&VA10" in baselines
        assert "VA09&&VA11" not in baselines
        assert "VA10&&VA10" not in baselines
        assert "VA10&&VA11" not in baselines

        baselines = flagdata(vis = self.vis, mode="summary", antenna="VA09&VA10",basecnt=True)['baseline'].keys()
        assert "VA09&&VA09" not in baselines
        assert "VA09&&VA10" in baselines
        assert "VA09&&VA11" not in baselines
        assert "VA10&&VA10" not in baselines
        assert "VA10&&VA11" not in baselines

    def test_autocorr1(self):
        '''flagdata: flag only auto-correlations with antenna selection'''
        flagdata(vis=self.vis, mode='manual', antenna='VA05&&&', flagbackup=False)
        s = flagdata(vis = self.vis, mode="summary",basecnt=True)['baseline']
        assert s['VA05&&VA05']['flagged'] == 7560
        assert s['VA01&&VA05']['flagged'] == 0
        assert s['VA02&&VA05']['flagged'] == 0
        assert s['VA05&&VA10']['flagged'] == 0
        assert s['VA05&&VA11']['flagged'] == 0

        s = flagdata(vis = self.vis, mode="summary")
        self.assertEqual(s['flagged'], 7560)

    def test_autocorr2(self):
        '''flagdata: flag auto-corrs with parameter'''
        flagdata(vis=self.vis, autocorr=True, flagbackup=False)
        s = flagdata(vis = self.vis, mode="summary")
        self.assertEqual(s['flagged'], 203994)

    def test_autocorr3(self):
        '''flagdata: flag auto-corrs in list mode'''
        # creat input list
        myinput = "scan='1' mode='manual' autocorr=True reason='AUTO'\n"\
                "scan='3' autocorr=True reason='AUTO'\n"\
                "scan='4' reason='ALL'"
        filename = 'listauto.txt'
        create_input(myinput, filename)

        # select only the autocorr reasons to flag
        flagdata(vis=self.vis, mode='list', inpfile=filename, reason='AUTO', action='apply',
                 flagbackup=False)
        s = flagdata(vis = self.vis, mode="summary", basecnt=True)
        self.assertEqual(s['scan']['4']['flagged'], 0)
        self.assertEqual(s['baseline']['VA09&&VA28']['flagged'], 0)
        self.assertEqual(s['baseline']['VA09&&VA09']['flagged'], 3528)

        # select only the third line scan=4
        flagdata(vis=self.vis, mode='list', inpfile=filename, reason='ALL', action='apply')
        s = flagdata(vis = self.vis, mode="summary", basecnt=True)
        self.assertEqual(s['scan']['4']['flagged'], 95256)
        self.assertEqual(s['baseline']['VA09&&VA28']['flagged'], 252)
        self.assertEqual(s['baseline']['VA09&&VA09']['flagged'], 3780)
        self.assertEqual(s['flagged'], 190386)

    def test_spw_error_handler_name(self):
        '''flagdata: A non-existing spw name in a compound with a existing spw should not fail'''
        # CAS-9366: flagcmd fails when applying flags based on an spw selection by name, when
        # one of the spws do not exist
        # The spw names in Four_ants_3C286.ms are not unique. They are:
        #     Subband:0 Subband:1 Subband:2 Subband:3 Subband:4 Subband:5 Subband:6 Subband:7
        # spw=0,8       1,9       2,10 etc.
        self.setUp_data4tfcrop()

        # Copy the input flagcmd file with a non-existing spw name
        # flagsfile has spw='"Subband:1","Subband:2","Subband:8"
        flagsfile = 'cas9366.flags.txt'
        os.system('cp -RH '+os.path.join(datapath,flagsfile)+' '+ ' .')

        # Try to flag
        try:
            flagdata(self.vis, mode='list', inpfile=flagsfile, flagbackup=False)
        except Exception as instance:
            print('Expected RuntimeError error: %s'%instance)

        # should flag spws 1,2,9,10
        s = flagdata(self.vis, mode='summary')
        self.assertEqual(s['spw']['1']['flagged'],274944.0)
        self.assertEqual(s['spw']['9']['flagged'],274944.0)
        self.assertEqual(s['spw']['2']['flagged'],274944.0)
        self.assertEqual(s['spw']['10']['flagged'],274944.0)
        self.assertEqual(s['flagged'],274944.0*4)

    def test_spw_error_handler_id(self):
        '''flagdata: A non-existing spw ID in a compound with a existing spw should not fail'''

        # Try to flag. Only spw=0 exists
        try:
            flagdata(self.vis, spw='0,32,33', flagbackup=False)
        except Exception as instance:
            print('Expected RuntimeError error: %s'%instance)

        # Only spw 0 should be flagged
        s = flagdata(self.vis, mode='summary')
        self.assertEqual(s['flagged'],2854278.0)


class test_statistics_queries(test_base):

    def setUp(self):
        self.setUp_ngc5921(True)

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ngc5921.ms*')

    def test_CAS2021(self):
        '''flagdata: test antenna negation selection'''

        flagdata(vis=self.vis, antenna='!VA05', savepars=False)
        s = flagdata(vis = self.vis, mode="summary",basecnt=True)['baseline']
        assert s['VA01&&VA05']['flagged'] == 0
        assert s['VA02&&VA05']['flagged'] == 0
        assert s['VA03&&VA05']['flagged'] == 0
        assert s['VA04&&VA05']['flagged'] == 0
        assert s['VA05&&VA06']['flagged'] == 0
        assert s['VA05&&VA07']['flagged'] == 0
        assert s['VA05&&VA08']['flagged'] == 0
        assert s['VA05&&VA09']['flagged'] == 0
        assert s['VA05&&VA10']['flagged'] == 0
        assert s['VA05&&VA11']['flagged'] == 0
        assert s['VA05&&VA12']['flagged'] == 0
        assert s['VA05&&VA13']['flagged'] == 0
        assert s['VA05&&VA14']['flagged'] == 0
        assert s['VA05&&VA15']['flagged'] == 0
        assert s['VA05&&VA16']['flagged'] == 0
        assert s['VA05&&VA17']['flagged'] == 0
        assert s['VA05&&VA18']['flagged'] == 0
        assert s['VA05&&VA19']['flagged'] == 0
        assert s['VA05&&VA20']['flagged'] == 0
        assert s['VA05&&VA21']['flagged'] == 0
        assert s['VA05&&VA22']['flagged'] == 0
        assert s['VA05&&VA24']['flagged'] == 0
        assert s['VA05&&VA25']['flagged'] == 0
        assert s['VA05&&VA26']['flagged'] == 0
        assert s['VA05&&VA27']['flagged'] == 0
        assert s['VA05&&VA28']['flagged'] == 0
        assert s['VA05&&VA05']['flagged'] == 7560
        assert s['VA05&&VA05']['total'] == 7560


    def test_CAS2212(self):
        '''flagdata: Clipping scan selection, CAS-2212, CAS-3496'''
        # By default correlation='ABS_ALL'
        flagdata(vis=self.vis, mode='clip', scan="2", clipminmax = [0.2, 0.3], savepars=False)
        s = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(s['flagged'], 85404)
        self.assertEqual(s['total'], 2854278)

        s = flagdata(vis=self.vis, mode='summary')['scan']

        # Make sure no other scan is clipped
        self.assertEqual(s['1']['flagged'], 0)
        self.assertEqual(s['3']['flagged'], 0)
        self.assertEqual(s['4']['flagged'], 0)
        self.assertEqual(s['5']['flagged'], 0)
        self.assertEqual(s['6']['flagged'], 0)
        self.assertEqual(s['7']['flagged'], 0)
        self.assertEqual(s['2']['flagged'], 85404)

    def test021(self):
        '''flagdata: Test of flagging statistics and queries'''

        flagdata(vis=self.vis, correlation='LL', savepars=False, flagbackup=False)
        flagdata(vis=self.vis, spw='0:17~19', savepars=False, flagbackup=False)
        flagdata(vis=self.vis, antenna='VA05&&VA09', savepars=False, flagbackup=False)
        flagdata(vis=self.vis, antenna='VA14', savepars=False, flagbackup=False)
        flagdata(vis=self.vis, field='1', savepars=False, flagbackup=False)
        s = flagdata(vis=self.vis, mode='summary', minrel=0.9, spwchan=True, basecnt=True)
        assert list(s['antenna'].keys()) == ['VA14']
        assert 'VA05&&VA09' in s['baseline']
        assert set(s['spw:channel']) == set(['0:17', '0:18', '0:19'])
        assert list(s['correlation'].keys()) == ['LL']  # LL
        assert list(s['field'].keys()) == ['1445+09900002_0']
        assert set(s['scan']) == set(['2', '4', '5', '7']) # field 1
        s = flagdata(vis=self.vis, mode='summary', maxrel=0.8)
        assert set(s['field']) == set(['1331+30500002_0', 'N5921_2'])
        s = flagdata(vis=self.vis, mode='summary', minabs=400000)
        assert set(s['scan']) == set(['3', '6'])
        s = flagdata(vis=self.vis, mode='summary', minabs=400000, maxabs=450000)
        assert list(s['scan'].keys()) == ['3']

    def test_chanavg0(self):
        print("Test of channel average")
        flagdata(vis=self.vis, mode='clip',channelavg=False, clipminmax=[30., 60.], correlation='ABS_RR',
                 savepars=False, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 1414186)

    def test_chanavg1(self):
        flagdata(vis=self.vis, mode='clip',channelavg=True, clipminmax=[30., 60.], correlation='ABS_RR',
                 savepars=False, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 1347822)

    def test_chanavg2(self):
        flagdata(vis=self.vis, mode='clip',channelavg=False, clipminmax=[30., 60.], spw='0:0~10',
                 correlation='ABS_RR', savepars=False, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 242053)

    def test_chanavg3(self):
        flagdata(vis=self.vis, mode='clip',channelavg=True, clipminmax=[30., 60.], spw='0:0~10',
                 correlation='ABS_RR', savepars=False, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 231374)


#    def test8(self):
#        print("Test of mode = 'quack'")
#        print("parallel quack")
#        flagdata(vis=self.vis, mode='quack', quackinterval=[1.0, 5.0], antenna=['2', '3'], correlation='RR')
#        func_test_eq(flagdata(vis=self.vis, mode='summary'), 2854278, 22365)
#
    def test9(self):
        '''flagdata: quack mode'''
        flagdata(vis=self.vis, mode='quack', quackmode='beg', quackinterval=1, savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 329994)

    def test10(self):
        '''flagdata: quack mode'''
        flagdata(vis=self.vis, mode='quack', quackmode='endb', quackinterval=1, savepars=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 333396)

    def test11(self):
        '''flagdata: quack mode'''
        flagdata(vis=self.vis, mode='quack', quackmode='end', quackinterval=1, savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 2520882)

    def test12(self):
        '''flagdata: quack mode'''
        flagdata(vis=self.vis, mode='quack', quackmode='tail', quackinterval=1, savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 2524284)

    def test13(self):
        '''flagdata: quack mode, quackincrement'''
        flagdata(vis=self.vis, mode='quack', quackinterval=50, quackmode='endb', quackincrement=True,
                 savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary'), 2854278, 571536)

        flagdata(vis=self.vis, mode='quack', quackinterval=20, quackmode='endb', quackincrement=True,
                 savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary'), 2854278, 857304)

        flagdata(vis=self.vis, mode='quack', quackinterval=150, quackmode='endb', quackincrement=True,
                 savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary'), 2854278, 1571724)

        flagdata(vis=self.vis, mode='quack', quackinterval=50, quackmode='endb', quackincrement=True,
                 savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary'), 2854278, 1762236)
        # flagdata(vis=self.vis, mode='unflag', savepars=False, flagbackup=False)

    def test_quackincrement_list(self):

        # flag 2 minutes; flagged: 91854.0; scan': {'1': {'flagged': 91854.0
        flagdata(vis=self.vis,mode='manual',timerange='09:18:00~09:20:00',spw='0',scan='1', flagbackup=False)
        res0 = flagdata(vis=self.vis, spw='0', scan='1', mode='summary')

        # quack flag it by 120 seconds; 'flagged': 234738.0,
        flagdata(vis=self.vis,mode='quack',quackinterval=120.0,spw='0',scan='1',quackincrement=False, flagbackup=False)
        res1 = flagdata(vis=self.vis, spw='0', scan='1', mode='summary')

        # unflag
        flagdata(vis=self.vis,mode='unflag')

        # quackincrement=True in list mode should be ignored
        flagdata(vis=self.vis, mode='list', flagbackup=False, inpfile=["timerange='09:18:00~09:20:00' spw='0' scan='1'",
                                                     "mode='quack' quackinterval=120.0 spw='0' scan='1' quackincrement=True"])
        resT = flagdata(vis=self.vis, spw='0', scan='1', mode='summary')
        self.assertEqual(resT['flagged'],res0['flagged'])

        # unflag
        flagdata(vis=self.vis,mode='unflag')

        # quackincrement=False in list mode should work fine. It should reflag what was flagged by
        # the manual cmd above in res0. and more. It should flag the equivalent of 120s ; 'flagged': 234738.0
        flagdata(vis=self.vis, mode='list', flagbackup=False, inpfile=["timerange='09:18:00~09:20:00' spw='0' scan='1'",
                                                     "mode='quack' quackinterval=120.0 spw='0' scan='1' quackincrement=False"])

        resF = flagdata(vis=self.vis, spw='0', scan='1', mode='summary')
        self.assertEqual(resF['flagged'],res1['flagged'])

        # unflag
        flagdata(vis=self.vis,mode='unflag')

        # If quackincrement=True is the first command in list, it should run fine
        # flags: 234738.0 because the manual cmd will reflag the same portion already flagged by the quack cmd
        flagdata(vis=self.vis, mode='list', flagbackup=False, inpfile=[
                                                "mode='quack' quackinterval=120.0 spw='0' scan='1' quackincrement=True",
                                                "timerange='09:18:00~09:20:00' spw='0' scan='1'"])
        resT = flagdata(vis=self.vis, spw='0', scan='1', mode='summary')
        self.assertEqual(resT['flagged'],res1['flagged'])

    def test_summary_spwcorr(self):
        ''' flagdata: basic check of summary/spwcorr stats. CAS-14185'''
        flagdata(vis=self.vis, spw='0', correlation='LL', mode='manual', savepars=False, flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary', spwcorr=True)
        self.assertTrue("spw:correlation" in res)
        func_test_eq(res, 2854278, 1427139)
        self.assertEqual(res["spw:correlation"]["0:LL"]["total"], 1427139)
        self.assertEqual(res["spw:correlation"]["0:LL"]["flagged"], 1427139)
        self.assertEqual(res["spw:correlation"]["0:RR"]["total"], 1427139)
        self.assertEqual(res["spw:correlation"]["0:RR"]["flagged"], 0)


    def test_summary_basecnt(self):
        ''' flagdata: basic check of summary/basecnt stats. CAS-14185'''
        test_ant = "VA17"
        flagdata(vis=self.vis, antenna=test_ant, spw='0', scan='1', mode='manual',
                 savepars=False, flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary', basecnt=True)
        self.assertTrue("baseline" in res)
        self.assertTrue("antenna:scan" in res)
        func_test_eq(res, 2854278, 36036)

        # Check 'baseline' stats
        for bsn_key in res["baseline"]:
            ant_occur = bsn_key.count(test_ant)
            if ant_occur == 1:
                self.assertEqual(res["baseline"][bsn_key]["total"], 7434)
                self.assertEqual(res["baseline"][bsn_key]["flagged"], 1386)
            elif ant_occur == 2:
                self.assertEqual(res["baseline"][bsn_key]["total"], 7434)
                self.assertEqual(res["baseline"][bsn_key]["flagged"], 0)

        # Check 'antenna:scan' stats
        self.assertEqual(res["antenna:scan"]["VA17:1"]["total"], 37422)
        self.assertEqual(res["antenna:scan"]["VA17:1"]["flagged"], 36036)
        unflagged_scans = ["2", "3", "4", "5", "6", "7"]
        for scan in unflagged_scans:
            self.assertEqual(res["antenna:scan"]["VA17:1"]["total"], 37422)
            self.assertEqual(res["antenna:scan"]["VA17:1"]["flagged"], 36036)


class test_selections(test_base):
    """Test various selections"""

    def setUp(self):
        self.setUp_ngc5921(True)

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ngc5921.ms*')
        if os.path.exists('spwflags.txt'):
            os.system('rm -rf spwflags.txt')

    def test_scan(self):
        '''flagdata: scan selection and manualflag compatibility'''
        flagdata(vis=self.vis, scan='3', mode='manualflag', savepars=False)
        res = flagdata(vis=self.vis, mode='summary', antenna='VA02')
        self.assertEqual(res['flagged'],52416)

    def test_antenna(self):
        '''flagdata: antenna selection'''
        flagdata(vis=self.vis, antenna='VA02', savepars=False,flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='VA02'), 196434, 196434)

    def test_spw(self):
        '''flagdata: spw selection'''
        flagdata(vis=self.vis, spw='0', savepars=False,flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 196434)

    def test_spw_list(self):
        '''flagdata: spw selection in list mode'''
        spwfile = 'spwflags.txt'
        if os.path.exists(spwfile):
            os.system('rm -rf '+spwfile)

        flagdata(vis=self.vis, spw='0:1~10', savepars=True, outfile=spwfile, flagbackup=False)
        res0 = flagdata(vis=self.vis, mode='summary', spwchan=True)
        self.assertEqual(res0['flagged'], 453060, 'Only channels 1~10 should be flagged')

        # Unflag
#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()
        ures = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(ures['flagged'], 0)

        # Flag using the saved list
        flagdata(vis=self.vis, mode='list', inpfile=spwfile, action='apply',
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spwchan=True)
        self.assertEqual(res0['flagged'], res['flagged'])

        # Only channels 1~10 should be flagged
        self.assertEqual(res['spw:channel']['0:0']['flagged'], 0)
        self.assertEqual(res['spw:channel']['0:1']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:2']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:3']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:4']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:5']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:6']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:7']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:8']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:9']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:10']['flagged'], 45306)
        self.assertEqual(res['spw:channel']['0:11']['flagged'], 0)

    def test_correlation(self):
        flagdata(vis=self.vis, correlation='LL', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 98217)
        func_test_eq(flagdata(vis=self.vis, mode='summary', correlation='RR'), 1427139, 0)
#        flagdata(vis=self.vis, mode='unflag', savepars=False, flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, correlation='LL,RR', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 196434)

#        flagdata(vis=self.vis, mode='unflag', savepars=False, flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, mode='clip', correlation='NORM_RR,LL', clipminmax=[0.,3.],
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 204979)
#        flagdata(vis=self.vis, correlation='LL RR')
#        flagdata(vis=self.vis, correlation='LL ,, ,  ,RR')
#        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 196434)

    def test_field(self):
        '''flagdata: field selection'''
        flagdata(vis=self.vis, field='0', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 39186)

    def test_uvrange(self):
        '''flagdata: uvrange selection'''
        flagdata(vis=self.vis, uvrange='200~400m', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='VA02'), 196434, 55944)

    def test_timerange(self):
        '''flagdata: timerange selection'''
        flagdata(vis=self.vis, timerange='09:50:00~10:20:00', savepars=False,
                 flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 6552)

    def test_array(self):
        '''flagdata: array selection'''
        flagdata(vis=self.vis, array='0', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 196434)

    def test_action(self):
        '''flagdata: action = calculate'''
        flagdata(vis=self.vis, antenna='2,3,4', action='calculate')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 0, 'Nothing should be flagged when action=calculate')

    def test_missing_corr_product(self):
        '''CAS-4234: Keep going when one of the corr products is not available but others are present'''
        flagdata(vis=self.vis, correlation='LL,LR', savepars=False, flagbackup=False)
        self.assertEqual(flagdata(vis=self.vis, mode='summary', antenna='2')['flagged'],98217)
        self.assertEqual(flagdata(vis=self.vis, mode='summary', correlation='RR')['flagged'], 0)

#        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 98217)
#        func_test_eq(flagdata(vis=self.vis, mode='summary', correlation='RR'), 1427139, 0)
#        flagdata(vis=self.vis, mode='unflag', savepars=False, flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, correlation='LL,RR,RL', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', antenna='2'), 196434, 196434)

    def test_multi_timerange(self):
        '''flagdata: CAS-5300, in list mode, flag multiple timerange intervals'''
        inpcmd = ["timerange='09:26:00~09:30:00,09:42:00~09:43:00,10:33:00~10:50:00'"]
        flagdata(vis=self.vis, mode='list', inpfile=inpcmd, flagbackup=False)

        # Should flag scan=2, scan=3 and scan=6,7
        res = flagdata(vis=self.vis, mode='summary', scan='2,3,6,7')
        self.assertEqual(res['scan']['2']['flagged'], 238140)
        self.assertEqual(res['scan']['3']['flagged'], 47628)
        self.assertEqual(res['scan']['6']['flagged'], 476280)
        self.assertEqual(res['scan']['7']['flagged'], 190512)
        self.assertEqual(res['flagged'], 238140+47628+476280+190512)


class test_alma(test_base):
    # Test various flagging on alma data

    def setUp(self):
        self.setUp_alma_ms()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf uid___A002_X30a93d_X43e_small*')

    def test_scanitent(self):
        '''flagdata: scanintent selection'''
        # flag POINTING CALIBRATION scans
        # (CALIBRATE_POINTING_.. from STATE table's OBS_MODE)
        flagdata(vis=self.vis, intent='CAL*POINT*', savepars=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'], 192416.0)

    def test_wvr(self):
        '''flagdata: flag WVR correlation'''
        flagdata(vis=self.vis, correlation='I', savepars=False, flagbackup=False)
        func_test_eq(flagdata(vis=self.vis, mode='summary', spw='0'),608, 608)

    def test_abs_wvr(self):
        '''flagdata: clip ABS_WVR'''
        flagdata(vis=self.vis, mode='clip',clipminmax=[0,50], correlation='ABS_WVR', savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spw='0')
        self.assertEqual(res['spw']['0']['flagged'], 498)
        self.assertEqual(res['flagged'], 498)
        self.assertEqual(res['correlation']['I']['flagged'], 498)

    def test_abs_i(self):
        '''flagdata: clip ABS_I. Do not flag WVR'''
        flagdata(vis=self.vis, mode='clip', clipminmax=[0,50], correlation='ABS_I', savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spw='0')
        self.assertEqual(res['spw']['0']['flagged'], 0)
        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['correlation']['I']['flagged'], 0)

    def test_abs_all(self):
        '''flagdata: clip ABS ALL. Do not flag WVR'''
        flagdata(vis=self.vis, mode='clip', clipminmax=[0,1], correlation='ABS ALL', savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['spw']['0']['flagged'], 0)
        self.assertEqual(res['flagged'], 1851)
        self.assertEqual(res['correlation']['I']['flagged'], 0)
        self.assertEqual(res['correlation']['XX']['flagged'], 568)
        self.assertEqual(res['correlation']['YY']['flagged'], 1283)

    def test_alma_spw(self):
        '''flagdata: flag various spw'''
        # Test that a white space in the spw parameter is taken correctly
        flagdata(vis=self.vis, mode='manual', spw='1,2, 3', savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['spw']['0']['flagged'], 0, 'spw=0 should not be flagged')
        self.assertEqual(res['spw']['1']['flagged'], 192000, 'spw=1 should be fully flagged')
        self.assertEqual(res['spw']['3']['flagged'], 1200, 'spw=3 should be flagged')
        self.assertEqual(res['spw']['3']['total'], 1200, 'spw=3 should be flagged')

    def test_null_intent_selection1(self):
        '''flagdata: handle unknown scan intent in list mode'''

        myinput = ["intent='FOCUS'",   # non-existing intent
                 "intent='CALIBRATE_POINTING*'", # scan=1
                 "intent='*DELAY*'"] # non-existing

        flagdata(vis=self.vis, mode='list', inpfile=myinput, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary',scan='1')
        self.assertEqual(res['flagged'], 192416)
        self.assertEqual(res['total'], 192416)

    def test_unknown_intent(self):
        '''flagdata: CAS-3712, handle unknown value in intent expression'''
        flagdata(vis=self.vis,intent='*POINTING*,*FOCUS*',flagbackup=False)

        # Only POINTING scan exists. *FOCUS* should not raise a NULL MS selection
        res = flagdata(vis=self.vis, mode='summary', scan='1')
        self.assertEqual(res['flagged'], 192416)
        self.assertEqual(res['total'], 192416)

    def test_autocorr_wvr(self):
        '''flagdata: CAS-5286, do not flag auto-correlations in WVR data'''
        flagdata(vis=self.vis,autocorr=True,flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spw='0,1')

        # spw='0' contains the WVR data
        self.assertEqual(res['spw']['1']['flagged'], 15360)
        self.assertEqual(res['spw']['0']['flagged'], 0)
        self.assertEqual(res['flagged'], 15360)

    def test_autocorr_wvr_list(self):
        '''flagdata: CAS-5485 flag autocorrs in list mode'''
        mycmd = ["mode='manual' antenna='DA41'",
                 "mode='manual' autocorr=True"]

        # The first cmd only flags cross-correlations of DV41
        # The second cmd only flags auto-corrs of all antennas
        # that have processor type=CORRELATOR. The radiometer
        # data should not be flagged, which is in spw=0
        res = flagdata(vis=self.vis, mode='list', inpfile=mycmd, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary',basecnt=True)

        # These are the auto-correlations not flagged in the list flagging.
        # Verify that the non-flagged points are those from the WVR data
        wvr1 = res['baseline']['DA41&&DA41']['total'] - res['baseline']['DA41&&DA41']['flagged']
        wvr2 = res['baseline']['DA42&&DA42']['total'] - res['baseline']['DA42&&DA42']['flagged']
        wvr3 = res['baseline']['DV02&&DV02']['total'] - res['baseline']['DV02&&DV02']['flagged']
        wvr4 = res['baseline']['PM03&&PM03']['total'] - res['baseline']['PM03&&PM03']['flagged']
        wvrspw= res['spw']['0']['total']

        self.assertEqual(wvrspw, wvr1+wvr2+wvr3+wvr4, 'Auto-corr of WVR data should not be flagged')
        self.assertEqual(res['antenna']['DA41']['flagged'],75600)
        self.assertEqual(res['antenna']['DA41']['total'],75752)
        self.assertEqual(res['spw']['0']['flagged'], 0)

class test_selections2(test_base):
    '''Test other selections'''

    def setUp(self):
        self.setUp_multi()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf multiobs*')
        if os.path.exists('obs2.txt'):
            os.system('rm -rf obs2.txt')

    def test_observation1(self):
        '''flagdata: observation ID selections'''
        # string
        flagdata(vis=self.vis, observation='1', savepars=False, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary',observation='1')
        self.assertEqual(res['flagged'], 28500)
        self.assertEqual(res['total'], 28500)

        # integer
#        flagdata(vis=self.vis, mode='unflag', savepars=False)
        self.unflag_ms()
        flagdata(vis=self.vis, observation=1, savepars=False)
        res = flagdata(vis=self.vis, mode='summary',observation='1')
        self.assertEqual(res['flagged'], 28500)
        self.assertEqual(res['total'], 28500)

    def test_observation2(self):
        '''flagdata: observation ID selections in list mode'''
        # creat input list
        myinput = "observation='0' mode='manual'"
        filename = 'obs2.txt'
        create_input(myinput, filename)

        flagdata(vis=self.vis, mode='list', inpfile=filename, savepars=False,
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['observation']['0']['flagged'], 2854278.0)
        self.assertEqual(res['observation']['1']['flagged'], 0, 'Only observation 0 should be flagged')

    def test_field_breakdown(self):
        '''flagdata: Produce a separated dictionary per field'''

        # First pre-clip the data to have interesting flag counts
        flagdata(vis=self.vis, mode='clip',clipminmax=[0,2],savepars=False,flagbackup=False)

        # Obtain list of summaries per field
        summary = flagdata(vis=self.vis, mode='summary', fieldcnt=True,savepars=False,flagbackup=False)

        field_list = ['1331+30500002_0','1331+30500002_0','N5921_2','NGC7538C']

        # Obtain list of summaries for each field separatelly
        summary_list={}
        for field in field_list:
            summary_list[field] = flagdata(vis=self.vis, mode='summary', field=field,
                                           fieldcnt=False,savepars=False,flagbackup=False)

        # Compare results
        for field in field_list:
            self.assertEqual(summary[field]['total'], summary_list[field]['total'],
                             'Total number of counts different for field' + field)
            self.assertEqual(summary[field]['flagged'], summary_list[field]['flagged'],
                             'Total number of flags different for field' + field)


class test_elevation(test_base):
    """Test of mode = 'elevation'"""
    def setUp(self):
        self.setUp_ngc5921()
        self.x55 = 666792    # data below 55 degrees, etc.
        self.x60 = 1428840
        self.x65 = 2854278
        self.all = 2854278

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ngc5921*')

    def test_lower(self):
        flagdata(vis = self.vis, mode = 'elevation', savepars=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, 0)

        flagdata(vis = self.vis, mode = 'elevation', lowerlimit = 50, savepars=False,
                 flagbackup=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, 0)

        flagdata(vis = self.vis, mode = 'elevation', lowerlimit = 55, savepars=False,
                 flagbackup=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, self.x55)

        flagdata(vis = self.vis, mode = 'elevation', lowerlimit = 60, savepars=False,
                 flagbackup=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, self.x60)

        flagdata(vis = self.vis, mode = 'elevation', lowerlimit = 65, savepars=False,
                 flagbackup=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, self.x65)

    def test_upper(self):
        flagdata(vis = self.vis, mode = 'elevation', upperlimit = 60, savepars=False,
                 flagbackup=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, self.all - self.x60)


    def test_interval(self):
        flagdata(vis = self.vis,mode = 'elevation',lowerlimit = 55,upperlimit = 60,
                  savepars=False,flagbackup=False)

        func_test_eq(flagdata(vis=self.vis, mode='summary'), self.all, self.all - (self.x60 - self.x55))


class test_list_file(test_base):
    """Test of mode = 'list' using input file"""

    def setUp(self):
        self.setUp_ngc5921(True)

    def tearDown(self):
        os.system('rm -rf list*.txt list*.tmp *myflags*')

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ngc5921*')

    def test_file1(self):
        '''flagdata: apply flags from a list and do not save'''
        # creat input list
        myinput = "scan='1~3' mode='manual'\n"+"scan='5' mode='manualflag'\n"\
                "#scan='4'"
        filename = 'list1.txt'
        create_input(myinput, filename)

        # apply and don't save to MS. Ignore comment line
        flagdata(vis=self.vis, mode='list', inpfile=filename, savepars=False, action='apply')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['4']['flagged'], 0)
        self.assertEqual(res['flagged'], 1711206, 'Total flagged does not match')

    def test_file2(self):
        '''flagdata: only save parameters without running the tool'''
        # creat input list
        myinput = "scan='1~3' mode='manual'\n"+"scan='5' mode='manual'\n"
        filename = 'list2.txt'
        create_input(myinput, filename)

        # save to another file
        if os.path.exists("myflags.txt"):
            os.system('rm -rf myflags.txt')

        flagdata(vis=self.vis, mode='list', inpfile=filename, savepars=True, action='', outfile='myflags.txt')
        self.assertTrue(filecmp.cmp(filename, 'myflags.txt', 1), 'Files should be equal')

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 0, 'No flags should have been applied')

    def test_file3(self):
        '''flagdata: flag and save list to FLAG_CMD'''
        # creat input list
        myinput = "scan='1~3' mode='manual'\n"+"scan='5' mode='manual'\n"
        filename = 'list3.txt'
        create_input(myinput, filename)

        # Delete any rows from FLAG_CMD
        flagcmd(vis=self.vis, action='clear', clearall=True)

        # Flag from list and save to FLAG_CMD
        flagdata(vis=self.vis, mode='list', inpfile=filename, savepars=True,
                 flagbackup=False)

        # Verify
        if os.path.exists("myflags.txt"):
            os.system('rm -rf myflags.txt')
        flagcmd(vis=self.vis, action='list', savepars=True, outfile='myflags.txt', useapplied=True)
        self.assertTrue(filecmp.cmp(filename, 'myflags.txt', 1), 'Files should be equal')


    def test_file4(self):
        '''flagdata: save without running and apply in flagcmd'''
        # Delete any rows from FLAG_CMD
        flagcmd(vis=self.vis, action='clear', clearall=True)

        # Test that action='none' is also accepted
        flagdata(vis=self.vis, mode='quack', quackmode='tail', quackinterval=1, action='none',
                 savepars=True, flagbackup=False)

        flagcmd(vis=self.vis, action='apply')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 2524284)

    def test_file6(self):
        '''flagdata: select by reason in list mode from a file'''
        # creat input list
        myinput = "mode='manual' scan='1' reason='SCAN_1'\n"\
                "mode='manual' scan='2'\n"\
                "scan='3' reason='SCAN_3'\n"\
                "scan='4' reason=''"
        filename = 'list6.txt'
        create_input(myinput, filename)

        # Select one reason
        flagdata(vis=self.vis, mode='list', inpfile=filename, reason='SCAN_3',
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', scan='3')
        self.assertEqual(res['scan']['3']['flagged'], 762048, 'Should flag only reason=SCAN_3')
        self.assertEqual(res['flagged'], 762048, 'Should flag only reason=SCAN_3')

        # Select list of reasons
        flagdata(vis=self.vis, mode='list', inpfile=filename, reason=['','SCAN_1'],
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary',scan='1,4')
        self.assertEqual(res['scan']['4']['flagged'], 95256, 'Should flag reason=\'\'')
        self.assertEqual(res['scan']['1']['flagged'], 568134, 'Should flag reason=SCAN_1')

        # No reason selection
#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, mode='list', inpfile=filename, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', scan='1~4')
        self.assertEqual(res['scan']['1']['flagged'], 568134)
        self.assertEqual(res['scan']['2']['flagged'], 238140)
        self.assertEqual(res['scan']['3']['flagged'], 762048)
        self.assertEqual(res['scan']['4']['flagged'], 95256)
        self.assertEqual(res['flagged'],568134+238140+762048+95256, 'Total flagged')

    def test_file_CAS4819(self):
        '''flagdata: CAS-4819, Flag commands from three files'''
        # creat first input file
        myinput = "scan='1'\n"\
                "scan='2'\n"\
                "# a comment line\n"\
                "scan='3'"
        filename1 = 'list7a.txt'
        create_input(myinput, filename1)

        # Create second input file
        myinput = "scan='5'\n"\
                  " \n"\
                "scan='6'\n"\
                "scan='7'"
        filename2 = 'list7b.txt'
        create_input(myinput, filename2)

         # Create third input file
        myinput = "scan='4' mode='clip' clipminmax=[0,4]"
        filename3 = 'list7c.txt'
        create_input(myinput, filename3)

        flagdata(vis=self.vis, mode='list', inpfile=[filename1,filename2,filename3],
                 flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'], 568134)
        self.assertEqual(res['scan']['2']['flagged'], 238140)
        self.assertEqual(res['scan']['3']['flagged'], 762048)
        self.assertEqual(res['scan']['4']['flagged'], 6696)
        self.assertEqual(res['scan']['5']['flagged'], 142884)
        self.assertEqual(res['scan']['6']['flagged'], 857304)
        self.assertEqual(res['scan']['7']['flagged'], 190512)
        self.assertEqual(res['total'],2854278)
        self.assertEqual(res['flagged'],2765718)

    def test_reason1(self):
        '''flagdata: add_reason to FLAG_CMD'''
        flagcmd(vis=self.vis, action='clear', clearall=True)
        flagdata(vis=self.vis, mode='manual', scan='1,3', savepars=True, cmdreason='SCAN_1_3',
                  action='')

        # Apply flag cmd
        flagcmd(vis=self.vis, action='apply', reason='SCAN_1_3')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 1330182, 'Only scans 1 and 3 should be flagged')

    def test_reason2(self):
        '''flagdata: add_reason to text file'''
        flagdata(vis=self.vis, mode='clip', scan='4', clipminmax=[0, 5], savepars=True,
                  cmdreason='CLIPSCAN4', outfile='listreason2.txt', action='')

        flagdata(vis=self.vis, mode='clip', scan='2~3', clipminmax=[ 0, 5], savepars=True,
                  cmdreason='CLIPSCAN2_3', outfile='listreason2.txt', action='')

        # Apply flag cmd
        flagdata(vis=self.vis, mode='list', inpfile='listreason2.txt',reason='CLIPSCAN2_3',
                 flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 69568)

    def test_reason3(self):
        '''flagdata: replace input reason from file with cmdreason'''
        # creat input list
        myinput = "mode='manual' scan='1' reason='SCAN_1'\n"\
                "mode='manual' scan='2'\n"\
                "scan='3' reason='SCAN_3'\n"\
                "scan='4' reason=''"
        filename = 'listinput3.txt'
        create_input(myinput, filename)

        flagdata(vis=self.vis, mode='list', inpfile=filename, savepars=True, outfile='listreason3a.txt',
                  cmdreason='MANUALFLAG', action='')

        # Apply the flag cmds
        flagdata(vis=self.vis, mode='list', inpfile='listreason3a.txt', reason='MANUALFLAG',
                 flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 1663578)

    def test_reason4(self):
        '''flagdata: select by reason from two files'''
        # creat first input file
        myinput = "scan='1' spw='0:10~20' reason='NONE'\n"\
                "scan='2' reason='EVEN'\n"\
                "scan='3' reason='ODD'"
        filename1 = 'listreasonfile1.txt'
        create_input(myinput, filename1)

        # Create second input file
        myinput = "scan='5' reason='ODD'\n"\
                "scan='6' reason='EVEN'\n"\
                "scan='7' reason='ODD'"
        filename2 = 'listreasonfile2.txt'
        create_input(myinput, filename2)

        # Apply flag cmds on ODD reason
        flagdata(vis=self.vis, mode='list', inpfile=[filename1,filename2], reason='ODD',
                 flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['3']['flagged'], 762048)
        self.assertEqual(res['scan']['5']['flagged'], 142884)
        self.assertEqual(res['scan']['6']['flagged'], 0)
        self.assertEqual(res['scan']['7']['flagged'], 190512)
        self.assertEqual(res['flagged'], 762048+142884+190512)

        # Apply flag cmds on NONE reason
#        flagdata(vis=self.vis, mode='unflag')
        self.unflag_ms()
        flagdata(vis=self.vis, mode='list', inpfile=[filename1,filename2], reason='NONE',
                 flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary', scan='1')
        self.assertEqual(res['scan']['1']['flagged'], 99198)
        self.assertEqual(res['flagged'], 99198)

    def test_anychar(self):
        '''flagdata: Do not continue if parameter doesn't exist'''
        myinput = "mode=manualflag field='Any $3[=character (}'"
        filename = 'listanychar.txt'
        create_input(myinput, filename)

        outname = 'listwrongpar.txt'

        # CAS-6704: should raise an exception because parameter $3[ doesn't exist in flagdata
        try:
            flagdata(vis=self.vis, mode='list', inpfile=filename, action='', savepars=True,
                  outfile=outname)
        except Exception as instance:
            print('Expected IOError error: %s'%instance)

        # It should fail above and not create an output file
        self.assertFalse(os.path.exists(outname))

    def test_file_summary1(self):
        '''flagdata: summary commands in list mode'''
        myinput = "mode='manual' scan='2'\n"\
                  "mode='summary' name='Scan2'\n"\
                  "mode='clip' clipzeros=True\n"\
                  "mode='summary' name='Zeros'"
        filename = 'listsumm1.txt'
        create_input(myinput, filename)
        summary_stats_list = flagdata(vis=self.vis, mode='list', inpfile=filename, flagbackup=False)

        # Extract only the type 'summary' reports into a list
        summary_reps = self.extract_reports(summary_stats_list)

        for ind in range(len(summary_reps)):
            flagcount = summary_reps[ind]['flagged'];
            totalcount = summary_reps[ind]['total'];
            # From the second summary onwards, subtract counts from the previous one :)
            if ( ind > 0 ):
                 flagcount = flagcount - summary_reps[ind-1]['flagged'];

            print("Summary ", ind , "(" , summary_reps[ind]['name']  , ") :  Flagged : " , flagcount , " out of " , totalcount)

        self.assertEqual(summary_reps[0]['flagged'],238140, 'Should show only scan=2 flagged')
        self.assertEqual(summary_reps[1]['flagged']-summary_reps[0]['flagged'],0, 'Should not flag any zeros')

    def test_file_summary2(self):
        '''flagdata: compare summaries from a list with individual reports'''
        myinput = ["scan='1~3' mode='manual'",
                   "mode='summary' name='SCANS123'",
                   "scan='5' mode='manualflag'",
                   "#scan='4'",
                   "mode='summary' name='SCAN5'"]

        summary_stats_list = flagdata(vis=self.vis, mode='list', inpfile=myinput, flagbackup=False)
        summary_reps = self.extract_reports(summary_stats_list)

        # Unflag and flag scan=1~3
        self.unflag_ms()
        flagdata(vis=self.vis, scan='1~3', flagbackup=False)
        rscan123 = flagdata(vis=self.vis, mode='summary')
        # Unflag and flag scan=5
        self.unflag_ms()
        flagdata(vis=self.vis, scan='5', flagbackup=False)
        rscan5 = flagdata(vis=self.vis, mode='summary')

        # Compare
        self.assertEqual(summary_reps[0]['flagged'],rscan123['flagged'], 'scan=1~3 should be flagged')
        self.assertEqual(summary_reps[1]['flagged'],rscan5['flagged']+rscan123['flagged'],\
                         'scan=1~3,5 should be flagged')
        self.assertEqual(summary_reps[1]['flagged']-summary_reps[0]['flagged'],rscan5['flagged'],\
                         'scan=5 should be flagged')

    def test_file_scan_int(self):
        '''flagdata: select a scan by giving an int value'''
        # The new fh.parseFlagParameters should allow this
        myinput = "mode='manual' scan=1\n"\
                  "scan='2'\n"\
                  "mode='summary'"
        filename = 'listintscan.txt'
        create_input(myinput, filename)

        with self.assertRaises(ValueError, msg='Expected error '):
            res = flagdata(vis=self.vis, mode='list', inpfile=filename, flagbackup=False)

    def test_file_scan_list(self):
        '''flagdata: select a scan by giving a list value. Expect error.'''
        # The new fh.parseFlagParameters should NOT allow this
        myinput = "scan='1' mode='manual'\n"\
                   "scan=[2]\n"\
                   "mode='summary'"

        filename = 'listscan.txt'
        create_input(myinput, filename)
        with self.assertRaises(Exception, msg='Expected error when reading input list'):
            res = flagdata(vis=self.vis, mode='list', inpfile=filename, flagbackup=False)

    def test_file_overwrite_true(self):
        '''flagdata: Use savepars and overwrite=True'''

        # Create flag commands in file called flagcmd.txt
        filename = 'listscan4clip.txt'
        myinput = "scan='4' mode='clip' correlation='ABS_RR' clipminmax=[0, 4]\n"
        create_input(myinput, filename)
        # Copy it to a new file
        newfile = 'listnewfile.txt'
        os.system('rm -rf '+newfile)
        os.system('cp '+filename+' '+newfile)

        # Create different flag command
        myinput = "scan='1'\n"
        filename = 'listscan1.txt'
        create_input(myinput, filename)

        # Apply flags from filename and try to save in newfile
        # Overwrite parameter should allow this
        flagdata(vis=self.vis, action='calculate', mode='list', inpfile=filename, savepars=True, outfile=newfile,
                flagbackup=False)

        # newfile should contain what was in filename
        self.assertTrue(filecmp.cmp(filename, newfile, 1), 'Files should be equal')

    def test_file_overwrite_false(self):
        '''flagdata: Use savepars and overwrite=False with an existing file'''

        # Create flag commands in file called flagcmd.txt
        myinput = "scan='4' mode='clip' correlation='ABS_RR' clipminmax=[0, 4]\n"
        filename = 'listscan4clip.txt'
        create_input(myinput, filename)
        # Copy it to a new file
        newfile = 'listnewfile.txt'
        os.system('rm -rf '+newfile)
        os.system('cp '+filename+' '+newfile)

        # Create different flag command
        myinput = "scan='1'\n"
        filename = 'listscan1.txt'
        create_input(myinput, filename)

        # Apply flags from file and try to save in newfile
        # Overwrite parameter should give an error and not save
        with self.assertRaises(RuntimeError, msg='expected issue with overwrite'):
            flagdata(vis=self.vis, action='calculate', mode='list', inpfile=filename,
                     savepars=True, outfile=newfile, flagbackup=False, overwrite=False)

        # newfile should not be overwritten
        self.assertFalse(filecmp.cmp(filename, newfile, 1), 'Files should be different')

    def test_file_overwrite_false1(self):
        '''flagdata: Use savepars and overwrite=False'''

        # Create flag commands in file called flagcmd.txt
        myinput = "scan='4' mode='clip' correlation='ABS_RR' clipminmax=[0, 4]\n"
        filename = 'listscan4clip.txt'
        create_input(myinput, filename)

        newfile='listmyflags.txt'
        # Apply flags from file and try to save in newfile
        # Overwrite=False should allow it since newfile doesn't exist
        flagdata(vis=self.vis, action='calculate', mode='list',inpfile=filename, savepars=True, outfile=newfile,
                flagbackup=False, overwrite=False)

        # newfile should not be overwritten
        self.assertTrue(filecmp.cmp(filename, newfile, 1), 'Files should be the same')


class test_list_list(test_base):
    """Test of mode = 'list' using input list"""

    def setUp(self):
        self.setUp_ngc5921(True)
        self.outfile = ''
        self.outfile2 = ''

    def tearDown(self) -> None:
        if os.path.exists(self.outfile):
            os.system('rm -rf '+self.outfile)
        if os.path.exists(self.outfile2):
            os.system('rm -rf '+self.outfile2)

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ngc5921*')

    def test_list1(self):
        '''flagdata: apply flags from a Python list and do not save'''
        # creat input list
        myinput = ["scan='1~3' mode='manual'",
                 "scan='5' mode='manualflag'",
                 "#scan='4'"]

        # apply and don't save to MS. Ignore comment line
        flagdata(vis=self.vis, mode='list', inpfile=myinput, savepars=False, action='apply')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['4']['flagged'], 0)
        self.assertEqual(res['flagged'], 1711206, 'Total flagged does not match')

    def test_list2(self):
        '''flagdata: only save parameters without running the tool'''
        # creat input list
        myinput = ["scan='1~3' mode='manual'",
                 "scan='5' mode='manual'"]

        self.outfile = 'myflags.txt'
        # save to another file
        if os.path.exists(self.outfile):
            os.system('rm -rf '+self.outfile)

        flagdata(vis=self.vis, mode='list', inpfile=myinput, savepars=True, action='', outfile=self.outfile)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 0, 'No flags should have been applied')

    def test_list3(self):
        '''flagdata: Compare flags from flagdata and flagcmd'''
        # creat input list
        myinput = ["scan='1~3' mode='manual'",
                 "scan='5' mode='manual'"]

        # Delete any rows from FLAG_CMD
        flagcmd(vis=self.vis, action='clear', clearall=True)

        # Flag from list and save to FLAG_CMD
        flagdata(vis=self.vis, mode='list', inpfile=myinput,flagbackup=False)
        res1 = flagdata(vis=self.vis, mode='summary')

        # Unflag and save in flagcmd using the cmd mode
#        flagdata(vis=self.vis, mode='unflag',flagbackup=False)
        self.unflag_ms()
        flagcmd(vis=self.vis, inpmode='list', inpfile=myinput)
        res2 = flagdata(vis=self.vis, mode='summary')

        # Verify
        self.assertEqual(res1['flagged'], res2['flagged'])
        self.assertEqual(res1['total'], res2['total'])

    def test_list5(self):
        '''flagdata: select by reason in list mode from a list'''
        # creat input list
        myinput = ["mode='manual' scan='1' reason='SCAN_1'",
                "mode='manual' scan='2'",
                "scan='3' reason='SCAN_3'",
                "scan='4' reason=''"]

        # Select one reason
        flagdata(vis=self.vis, mode='list', inpfile=myinput, reason='SCAN_3',
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['3']['flagged'], 762048, 'Should flag only reason=SCAN_3')
        self.assertEqual(res['flagged'], 762048, 'Should flag only reason=SCAN_3')

        # Select list of reasons
        flagdata(vis=self.vis, mode='list', inpfile=myinput, reason=['','SCAN_1'],
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['4']['flagged'], 95256, 'Should flag reason=\'\'')
        self.assertEqual(res['scan']['1']['flagged'], 568134, 'Should flag reason=SCAN_1')

        # No reason selection
#        flagdata(vis=self.vis, mode='unflag',flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, mode='list', inpfile=myinput, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'], 568134)
        self.assertEqual(res['scan']['2']['flagged'], 238140)
        self.assertEqual(res['scan']['3']['flagged'], 762048)
        self.assertEqual(res['scan']['4']['flagged'], 95256)
        self.assertEqual(res['flagged'],568134+238140+762048+95256, 'Total flagged')

    def test_reason_list(self):
        '''flagdata: replace input reason from list with cmdreason'''
        # creat input list
        myinput = ["mode='manual' scan='1' reason='SCAN_1'",
                "mode='manual' scan='2'",
                "scan='3' reason='SCAN_3'",
                "scan='4' reason=''"]

        self.outfile = 'reason3b.txt'
        flagdata(vis=self.vis, mode='list', inpfile=myinput, savepars=True, outfile=self.outfile,
                  cmdreason='MANUALFLAG', action='')

        # Apply the flag cmds
        flagdata(vis=self.vis, mode='list', inpfile=self.outfile, reason='MANUALFLAG',
                 flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 1663578)

    # The new parser allows whitespaces in reason values. Change the test
    def test_cmdreason1(self):
        '''flagdata: allow whitespace in cmdreason'''
        self.outfile = 'spacereason1.txt'
        flagdata(vis=self.vis, scan='1,3', action='calculate', savepars=True, outfile=self.outfile,
                 cmdreason='ODD SCANS')
        self.outfile2 = 'spacereason2.txt'
        flagdata(vis=self.vis, scan='2,4', action='calculate', savepars=True, outfile=self.outfile2,
                 cmdreason='EVEN SCANS')
        os.system("cat "+self.outfile+" >> "+self.outfile2)

        # Apply the cmd with blanks in reason.
        flagdata(vis=self.vis, mode='list', inpfile=self.outfile2, reason='ODD SCANS')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'], 568134)
        self.assertEqual(res['scan']['3']['flagged'], 762048)
        self.assertEqual(res['flagged'], 568134+762048)

    def test_cmdreason2(self):
        '''flagdata: Blanks in reason are also allowed in FLAG_CMD table'''
        self.outfile = 'goodreason.txt'
        flagdata(vis=self.vis, scan='1,3', action='calculate', savepars=True,
                 cmdreason='ODD SCANS')
        flagdata(vis=self.vis, scan='2,4', action='calculate', savepars=True,
                 cmdreason='EVEN SCANS')

        flagcmd(vis=self.vis, reason='ODD SCANS')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'], 568134)
        self.assertEqual(res['scan']['3']['flagged'], 762048)
        self.assertEqual(res['flagged'], 568134+762048)

    def test_list_summary1(self):
        '''flagdata: check names of multiple summaries in a list'''
        myinput = ["scan='3' mode='manual'",
                   "mode='summary' name='SCAN_3'",
                   "scan='15' mode='manualflag'",  # provoke an error
                   "#scan='4'",
                   "mode='summary' name='SCAN15'"]

        summary_stats_list = flagdata(vis=self.vis, mode='list', inpfile=myinput, flagbackup=False)
        summary_reps = self.extract_reports(summary_stats_list)

        self.assertEqual(summary_reps[0]['scan']['3']['flagged'],
                         summary_reps[0]['scan']['3']['total'])
        self.assertEqual(summary_reps[0]['name'],'SCAN_3')
        self.assertEqual(summary_reps[1]['name'],'SCAN15')


class test_clip(test_base):
    """flagdata:: Test of mode = 'clip'"""

    def setUp(self):
        self.setUp_data4tfcrop()
        self.timeavgms = ''

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')
        if os.path.exists('list5.txt'):
            os.system('rm -rf list5.txt*')
        if os.path.exists('test_residual_step1_timeavg.ms'):
            os.system('rm -rf test_residual_step1_timeavg.ms*')
        if os.path.exists('test_residual_step2_timeavg.ms'):
            os.system('rm -rf test_residual_step2_timeavg.ms*')
        if os.path.exists('timeavg.ms'):
            os.system('rm -rf timeavg.ms*')

    def test_clipzeros(self):
        '''flagdata: clip only zero-value data'''

        flagdata(vis=self.vis, mode='clip', clipzeros=True, flagbackup=False)
        spw = '8'
        res = flagdata(vis=self.vis, mode='summary', spw='8')
        exp_flagged = 274944
        self.assertEqual(res['flagged'], exp_flagged, 'Should clip only spw=8')
        self.assertEqual(res['spw'][spw]['flagged'], 274944, 'All flags should be seen '
                         'in spw {}'.format(spw))

    def test_clip_list1(self):
        '''flagdata: clip zeros in mode=list and save reason to FLAG_CMD'''

        # creat input list
        myinput = ["mode='clip' clipzeros=True reason='CLIP_ZERO'"]

        # Save to FLAG_CMD
        flagdata(vis=self.vis, mode='list', inpfile=myinput, action='', savepars=True)

        # Run in flagcmd and select by reason
        flagcmd(vis=self.vis, action='apply', reason='CLIP_ZERO')

        res = flagdata(vis=self.vis, mode='summary', spw='8')
        self.assertEqual(res['flagged'], 274944, 'Should clip only spw=8')

    def test_clip_file1(self):
        '''flagdata: clip zeros in mode=list and save reason to FLAG_CMD'''

        # creat input list
        myinput = "mode='clip' clipzeros=True reason='CLIP_ZERO'"
        filename = 'list5.txt'
        create_input(myinput, filename)

        # Save to FLAG_CMD
        flagdata(vis=self.vis, mode='list', inpfile=filename, action='', savepars=True)

        # Run in flagcmd and select by reason
        flagcmd(vis=self.vis, action='apply', reason='CLIP_ZERO')

        res = flagdata(vis=self.vis, mode='summary', spw='8')
        self.assertEqual(res['flagged'], 274944, 'Should clip only spw=8')
        self.assertEqual(res['total'], 274944)

    def test_datacol_corrected(self):
        ''''flagdata: clip CORRECTED data column'''
        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='CORRECTED',
                 clipminmax=[0.,10.])
        # only corrected column has amplitude above 10.0
        res = flagdata(vis=self.vis, mode='summary', spw='5,9,10,11')
        self.assertEqual(res['flagged'], 1142)

        # Make sure the corrected data was used, not the default data column
        self.unflag_ms()
        flagdata(vis=self.vis, flagbackup=False, mode='clip',
                 clipminmax=[0.,10.])

        # should not flag anything
        res = flagdata(vis=self.vis, mode='summary', spw='5,9,10,11')
        self.assertEqual(res['flagged'], 0)

    def test_residual_col(self):
        '''flagdata: clip RESIDUAL column'''
        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='RESIDUAL', clipzeros=True)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 137472)

    def test_clip_timeavg_cmp_mstransform(self):
        '''flagdata: clip with time average and compare with mstransform'''

        def check_expected_flag_positions(self, msname):
            """
            Implements a very hand-made check on the exact positions of flags for the test
            dataset used in these tests.

            This has been crossed-checked manually, looking at the visibility values and the
            thresholds set in clip (clipminmax). The pattern used here must be produced by
            the (back)propagation of flags with time average if it is working correctly.
            """
            try:
                tbt = table()
                tbt.open(msname)
                flags = tbt.getcol('FLAG')

                # Indices are: correlation, channel, time:
                exp_flags = [[ 0, 51, 0], [ 0, 51, 1], [ 0, 60, 0], [ 0, 60, 1],
                             [ 1, 51, 0], [ 1, 51, 1], [ 1, 60, 1], [ 2, 51, 0],
                             [ 2, 51, 1], [ 2, 60, 0], [ 2, 60, 1], [ 3 ,21, 1],
                             [ 3, 51, 0], [ 3, 51, 1], [ 3, 60, 0], [ 3, 60, 1]]
                expected = np.full((4, 64, 2), False)
                for pos in exp_flags:
                    expected[pos[0], pos[1], pos[2]] = True

                np.testing.assert_equal(flags, expected,
                                        "Flags do not match the expected, manually verified "
                                        "pattern.")
            finally:
                tbt.close()

        # Create an output with 4 rows
        timeavgms = 'timeavg.ms'
        split(vis=self.vis,outputvis=timeavgms,datacolumn='data',spw='9',scan='30',antenna='1&2',
               timerange='2010/10/16/14:45:08.50~2010/10/16/14:45:11.50')
        flagdata(timeavgms, flagbackup=False, mode='unflag')

        # STEP 1
        # Create time averaged output in mstransform
        ms_step1 = 'test_residual_step1_timeavg.ms'
        mstransform(timeavgms,outputvis=ms_step1,
                    datacolumn='data',timeaverage=True,timebin='2s')

        # clip it
        flagdata(ms_step1, flagbackup=False, mode='clip',
                 clipminmax=[0.0,0.08])
        res1 = flagdata(vis=ms_step1, mode='summary', spwchan=True)

        # STEP 2
        # Clip with time averaging.
        ms_step2 = 'test_residual_step2_timeavg.ms'
        flagdata(vis=timeavgms, flagbackup=False, mode='clip', datacolumn='DATA',
                 timeavg=True, timebin='2s', clipminmax=[0.0,0.08])

        # Do another time average in mstransform to have the corrected averaged visibilities
        mstransform(timeavgms, outputvis=ms_step2,
                    datacolumn='data',timeaverage=True,timebin='2s')

        res2 = flagdata(vis=ms_step2, mode='summary', spwchan=True)

        # Compare step1 vs step2
        self.assertEqual(res1['flagged'], res2['flagged'])
        # Check specific channels
        self.assertEqual(res2['spw:channel']['0:21']['flagged'], 1)
        self.assertEqual(res2['spw:channel']['0:51']['flagged'], 8)
        self.assertEqual(res2['spw:channel']['0:60']['flagged'], 7)

        # Additional checks on the exact positions of flags, to better cover issues found
        # in CAS-12737, CAS-12910.
        check_expected_flag_positions(self, ms_step1)
        check_expected_flag_positions(self, ms_step2)

    def test_timeavg_spw9_2scans(self):
        '''flagdata: clip with time averaging in spw 9'''

        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='DATA', spw='9',
                 timeavg=True, timebin='2s', clipminmax=[0.0,0.08])

        res = flagdata(vis=self.vis, mode='summary', spw='9')
        self.assertEqual(res['spw']['9']['flagged'], 42106)
        self.assertEqual(res['flagged'], 42106)


    def test_clip_no_model_col(self):
        "flagdata: Should fail when MODEL or virtual MODEL columns do not exist"
        # Use an MS without MODEL_DATA column
        self.setUp_ngc5921(True)

        # RESIDUAL = CORRECTED - MODEL.
        # It should fail and not flag anything
        datacols = ["RESIDUAL","RESIDUAL_DATA"]
        for col in datacols:
            with self.assertRaises(ValueError):
                flagdata(vis=self.vis, mode='clip', datacolumn=col, clipminmax=[2.3,3.1],
                         clipoutside=False, action='apply')
            print('flagadta is expected to fail with datacolumn='+col)
            self.assertEqual(flagdata(vis=self.vis, mode='summary')['flagged'],0.0)

    def test_clip_with_model_col(self):
        "flagdata: Should flag DATA-MODEL when RESIDUAL-DATA is asked"
        self.setUp_ngc5921(True)
        os.system('cp -r '+self.vis + ' ngc5921_virtual_model_col.ms')

        # Create MODEL column
        setjy(vis=self.vis, field='1331+305*',model='',standard='Perley-Taylor 99',scalebychan=False, usescratch=True)

        # Create virtual MODEL column
        setjy(vis='ngc5921_virtual_model_col.ms', field='1331+305*',model='',standard='Perley-Taylor 99',scalebychan=False, usescratch=False)

        # Flag RESIDUAL_DATA = DATA - MODEL
        flagdata(vis=self.vis, mode='clip',datacolumn='RESIDUAL_DATA',clipminmax=[2.3,3.1],clipoutside=False, action='apply')
        self.assertEqual(flagdata(vis=self.vis, mode='summary')['flagged'],412.0)

        # Flag RESIDUAL_DATA = DATA - virtual MODEL
        flagdata(vis='ngc5921_virtual_model_col.ms', mode='clip',datacolumn='RESIDUAL_DATA',clipminmax=[2.3,3.1],
                 clipoutside=False, action='apply')
        self.assertEqual(flagdata(vis='ngc5921_virtual_model_col.ms', mode='summary')['flagged'],412.0)


    def test_clip_virtual_model_col_use_delmod(self):
        "flagdata: Should flag DATA-MODEL when RESIDUAL-DATA is asked"
        self.setUp_ngc5921(True)
        os.system('cp -r '+self.vis + ' ngc5921_virtual_model_col.ms')

        # Create virtual MODEL column
        setjy(vis='ngc5921_virtual_model_col.ms', field='1331+305*',model='',standard='Perley-Taylor 99',scalebychan=False,
              usescratch=False)

        # Flag RESIDUAL_DATA = DATA - virtual MODEL
        flagdata(vis='ngc5921_virtual_model_col.ms', mode='clip',datacolumn='RESIDUAL_DATA',clipminmax=[2.3,3.1],
                 clipoutside=False, action='apply')
        self.assertEqual(flagdata(vis='ngc5921_virtual_model_col.ms', mode='summary')['flagged'],412.0)

        # Unflag and delete model columns
        flagdata(vis='ngc5921_virtual_model_col.ms', mode='unflag',flagbackup=False)
        delmod(vis='ngc5921_virtual_model_col.ms',otf=True,scr=True)

        # Flag RESIDUAL_DATA should fail because virtual MODEL column doesn't exist
        with self.assertRaises(ValueError):
            flagdata(vis='ngc5921_virtual_model_col.ms', mode='clip',
                     datacolumn='RESIDUAL_DATA', clipminmax=[2.3,3.1], clipoutside=False,
                     action='apply')
        self.assertEqual(flagdata(vis='ngc5921_virtual_model_col.ms', mode='summary')['flagged'],0)


class test_antint(test_base):
    """flagdata:: Test of mode = 'antint'"""

    def setUp(self):
        # TODO: we need a more appropriate input MS for this.
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def test_antint_spw3_high_threshold(self):
        '''flagdata: mode = antint, spw = 3, minchanfrac = 0.6'''

        flagdata(vis=self.vis, mode='antint', spw='3', antint_ref_antenna='ea01', minchanfrac=0.6)
        res = flagdata(vis=self.vis, mode='summary', spw='3')

        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['antenna']['ea01']['flagged'], 0)
        self.assertEqual(res['spw']['3']['total'], 274944)
        self.assertEqual(res['spw']['3']['flagged'], 0)

    def test_antint_spw3_low_threshold(self):
        '''flagdata: mode = antint, spw = 3, minchanfrac = -.1'''

        flagdata(vis=self.vis, mode='antint', spw='3', antint_ref_antenna='ea01', minchanfrac=-.1)
        res = flagdata(vis=self.vis, mode='summary', spw='3')

        self.assertEqual(res['flagged'], 274944)
        self.assertEqual(res['antenna']['ea01']['flagged'], 137472)
        self.assertEqual(res['spw']['3']['total'], 274944)
        self.assertEqual(res['spw']['3']['flagged'], 274944)

    def test_antint_spw0_high_threshold(self):
        '''flagdata: mode = antint, spw = 0, minchanfrac = 0.45'''

        flagdata(vis=self.vis, mode='antint', spw='0', antint_ref_antenna='ea01', minchanfrac=0.45)
        res = flagdata(vis=self.vis, mode='summary', spw='0')

        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['antenna']['ea01']['flagged'], 0)
        self.assertEqual(res['spw']['0']['total'], 274944)
        self.assertEqual(res['spw']['0']['flagged'], 0)

    def test_antint_spw0_low_threshold(self):
        '''flagdata: mode = antint, spw = 0, minchanfrac = 0.05'''

        flagdata(vis=self.vis, mode='antint', spw='0', antint_ref_antenna='ea01', minchanfrac=0.05)
        res = flagdata(vis=self.vis, mode='summary', spw='0')

        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['antenna']['ea01']['flagged'], 0)
        self.assertEqual(res['spw']['0']['total'], 274944)
        self.assertEqual(res['spw']['0']['flagged'], 0)

    def test_antint_list_mode1_with_clip(self):
        '''flagdata in list mode: mode = antint + clip, spw = 2, minchanfrac=0.3'''

        in_list = ["mode='clip' spw='2' clipminmax=[0.1, 0.7] clipzeros=True",
                   "mode='antint' antint_ref_antenna='ea01' spw='2' minchanfrac=0.3 verbose=True",
                   "mode='summary' spw='2'"]

        res = flagdata(vis=self.vis, mode='list', inpfile=in_list)

        self.assertEqual(res['total'], 274944)
        self.assertEqual(res['flagged'], 149258)
        self.assertEqual(len(res['antenna']), 4)
        self.assertEqual(res['antenna']['ea01']['total'], 137472)
        self.assertEqual(res['antenna']['ea01']['flagged'], 74300)
        self.assertEqual(len(res['spw']), 1)
        self.assertEqual(res['spw']['2']['total'], 274944)
        self.assertEqual(res['spw']['2']['flagged'], 149258)

    def test_antint_list_mode2_compare_against_flagcmd(self):
        '''flagdata and flagcmd in list mode: mode = antint, spw = 2, minchanfrac=0.3'''

        # Clear all flags with flagcmd
        flagcmd(vis=self.vis, action='clear', clearall=True)

        in_list = ["mode='clip' spw='2' clipminmax=[0.1, 0.7] clipzeros=True",
                   "mode='antint' antint_ref_antenna='ea01' spw='2' minchanfrac=0.3 verbose=True",
                   "mode='summary' spw='2'"]

        # Run antint mode with flagdata in list mode
        res_flagdata = flagdata(vis=self.vis, mode='list', inpfile=in_list)

        # Re-run antint mode with flagcmd
        self.unflag_ms()
        flagcmd(vis=self.vis, inpmode='list', inpfile=in_list)
        res_flagcmd = flagdata(vis=self.vis, mode='summary', spw='2')

        # Check result from flagdata against flagcmd
        self.assertEqual(res_flagdata['total'], res_flagcmd['total'])
        self.assertEqual(res_flagdata['flagged'], res_flagcmd['flagged'])
        self.assertEqual(len(res_flagdata['antenna']), len(res_flagcmd['antenna']))
        self.assertEqual(res_flagdata['antenna']['ea01']['total'],
                         res_flagcmd['antenna']['ea01']['total'])
        self.assertEqual(res_flagdata['antenna']['ea01']['flagged'],
                         res_flagcmd['antenna']['ea01']['flagged'])
        self.assertEqual(len(res_flagdata['spw']), len(res_flagcmd['spw']))
        self.assertEqual(res_flagdata['spw']['2']['total'],
                         res_flagcmd['spw']['2']['total'])
        self.assertEqual(res_flagdata['spw']['2']['flagged'],
                         res_flagcmd['spw']['2']['flagged'])


class test_CASA_4_0_bug_fix(test_base):
    """flagdata:: Regression test for the fixes introduced during the CASA 4.0 bug fix season"""

    def setUp(self):
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def test_CAS_4270(self):
        """flagdata: Test uvrange given in lambda units"""

        flagdata(vis=self.vis,mode='manual',uvrange='<2klambda')
        flagdata(vis=self.vis,mode='clip', flagbackup=False)
        summary_ref = flagdata(vis=self.vis,mode='summary', spw='2,3')

#        flagdata(vis=self.vis,mode='unflag', flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis,mode='list',inpfile=["uvrange='<2Klambda'","mode='clip'"],
                 flagbackup=False)
        summary_out = flagdata(vis=self.vis,mode='summary', spw='2,3')

        self.assertEqual(summary_out['flagged'],summary_ref['flagged'],'uvrange given in lambda is not properly translated into meters')

    def test_CAS_4312(self):
        """flagdata: Test channel selection with Rflag agent"""

        flagdata(vis=self.vis,mode='rflag',spw='9:10~20', extendflags=False)
        summary = flagdata(vis=self.vis,mode='summary', spw='8,9,10')
        self.assertEqual(summary['spw']['8']['flagged'],0,'Error in channel selection with Rflag agent')
        self.assertEqual(summary['spw']['9']['flagged'],1861,'Error in channel selection with Rflag agent')
        self.assertEqual(summary['spw']['10']['flagged'],0,'Error in channel selection with Rflag agent')


    def test_CAS_4200(self):
        """flagdata: Test quack mode with quackinterval 0"""

        res = flagdata(vis=self.vis,mode='quack',quackinterval=0, flagbackup=False)
        self.assertTrue(res == {})
#        summary_zero = flagdata(vis=self.vis,mode='summary', spw='8')
#        self.assertEqual(summary_zero['flagged'],0,'Error in quack mode with quack interval 0')

        flagdata(vis=self.vis,mode='quack',quackinterval=1, flagbackup=False)
        summary_one = flagdata(vis=self.vis,mode='summary', spw='8')

#        flagdata(vis=self.vis,mode='unflag', flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis,mode='quack', flagbackup=False)
        summary_default = flagdata(vis=self.vis,mode='summary', spw='8')

        self.assertEqual(summary_one['flagged'],summary_default['flagged'],'Error in quack mode with quack interval 1')

    def test_alias(self):
        '''flagdata: Test flagdata alias'''
        res = flagdata(vis=self.vis, mode='summary', spw='1')['flagged']
        self.assertEqual(res, 0)

    def test_spw_freq1(self):
        '''flagdata: CAS-3562, flag all spw channels greater than a frequency'''
        flagdata(vis=self.vis, spw='>2000MHz', flagbackup=False)

        # Flag only spw=6,7
        res = flagdata(vis=self.vis, mode='summary', spw='0,6,7,10')
        self.assertEqual(res['spw']['0']['flagged'], 0)
        self.assertEqual(res['spw']['10']['flagged'], 0)
        self.assertEqual(res['spw']['7']['flagged'], 274944)
        self.assertEqual(res['spw']['7']['total'], 274944)
        self.assertEqual(res['spw']['6']['flagged'], 274944)
        self.assertEqual(res['spw']['6']['total'], 274944)
        self.assertEqual(res['flagged'], 549888)

    def test_spw_freq2(self):
        '''flagdata: CAS-3562, flag the channel with a frequency'''
        flagdata(vis=self.vis, spw='*:1956MHz,*:945MHz', flagbackup=False)

         # Flag only spw=5,8, first channel (0)
        res = flagdata(vis=self.vis, mode='summary', spw='1,5,8,15',spwchan=True)
        self.assertEqual(res['spw:channel']['1:0']['flagged'], 0)
        self.assertEqual(res['spw:channel']['15:0']['flagged'], 0)
        self.assertEqual(res['spw:channel']['5:0']['flagged'], 4296)
        self.assertEqual(res['spw:channel']['5:0']['total'], 4296)
        self.assertEqual(res['spw:channel']['8:0']['flagged'], 4296)
        self.assertEqual(res['spw:channel']['8:0']['total'], 4296)
        self.assertEqual(res['flagged'], 8592)

    def test_spw_freq3(self):
        '''flagdata: CAS-3562, flag a range of frequencies'''
        flagdata(vis=self.vis, spw='1500 ~ 2000MHz', flagbackup=False)

        # Flag only spw=0~5
        res = flagdata(vis=self.vis, mode='summary', spw='0~6', spwchan=True)
        self.assertEqual(res['spw']['0']['flagged'], 274944)
        self.assertEqual(res['spw']['1']['flagged'], 274944)
        self.assertEqual(res['spw']['2']['flagged'], 274944)
        self.assertEqual(res['spw']['3']['flagged'], 274944)
        self.assertEqual(res['spw']['4']['flagged'], 274944)
        self.assertEqual(res['spw']['5']['flagged'], 274944)
        self.assertEqual(res['spw']['6']['flagged'], 0)
        self.assertEqual(res['flagged'], 1649664)

    def test_invalid_antenna(self):
        '''flagdata: CAS-3712, handle good and bad antenna names in MS selection'''

        flagdata(vis=self.vis, antenna='ea01,ea93', mode='manual', flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', antenna='ea01', basecnt=True)
        self.assertEqual(res['flagged'], 2199552)
        self.assertEqual(res['total'], 2199552)


class test_correlations(test_base):
    '''Test combinations of correlation products'''
    def setUp(self):
        self.setUp_mwa()

    def tearDown(self):
        shutil.rmtree(self.vis, ignore_errors=True)

    def test_xx_xy(self):
        '''flagdata: flag XX,XY'''
        flagdata(vis=self.vis, mode='manual', flagbackup=False, correlation='XX,XY')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['XX']['flagged'], 429792)
        self.assertEqual(res['correlation']['XY']['flagged'], 429792)
        self.assertEqual(res['correlation']['YY']['flagged'], 0)
        self.assertEqual(res['correlation']['YX']['flagged'], 0)

    def test_xx_yx(self):
        '''flagdata: flag XX,YX'''
        flagdata(vis=self.vis, mode='manual', flagbackup=False, correlation='XX,YX')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['XX']['flagged'], 429792)
        self.assertEqual(res['correlation']['XY']['flagged'], 0)
        self.assertEqual(res['correlation']['YY']['flagged'], 0)
        self.assertEqual(res['correlation']['YX']['flagged'], 429792)

    def test_xx_yx_xy(self):
        '''flagdata: flag XX,YX, XY with space'''
        flagdata(vis=self.vis, mode='manual', flagbackup=False, correlation='XX,YX, XY')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['XX']['flagged'], 429792)
        self.assertEqual(res['correlation']['XY']['flagged'], 429792)
        self.assertEqual(res['correlation']['YY']['flagged'], 0)
        self.assertEqual(res['correlation']['YX']['flagged'], 429792)

    def test_yy_yx(self):
        '''flagdata: flag YY,YX'''
        flagdata(vis=self.vis, mode='manual', flagbackup=False, correlation=' YY,YX')
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['XX']['flagged'], 0)
        self.assertEqual(res['correlation']['XY']['flagged'], 0)
        self.assertEqual(res['correlation']['YY']['flagged'], 429792)
        self.assertEqual(res['correlation']['YX']['flagged'], 429792)


class test_tsys(test_base):
    """Flagdata:: Flagging of Tsys-based CalTable """

    def setUp(self):
         self.setUp_tsys_case()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf X7ef.tsys*')
        if os.path.exists('callist.txt'):
            os.system('rm -rf callist.txt')

    def test_unsupported_elevation(self):
        '''Flagdata: Unsupported elevation mode'''
        with self.assertRaises(ValueError):
            res = flagdata(vis=self.vis, mode='elevation')

    def test_unsupported_shadow(self):
        '''Flagdata: Unsupported shadow mode'''
        with self.assertRaises(ValueError):
            res = flagdata(vis=self.vis, mode='shadow', flagbackup=False)

    def test_mixed_list(self):
        '''Flagdata: mixed supported and unsupported modes in a list'''
        cmds = ["spw='1'",
                "spw='3' mode='elevation'",
                "mode='shadow'",
                "spw='5'"]

        flagdata(vis=self.vis, mode='list', inpfile=cmds, flagbackup=False)
        res = flagdata(vis=self.vis,mode='summary',spw='1,3,5')
        self.assertEqual(res['spw']['1']['flagged'], 32256)
        self.assertEqual(res['spw']['3']['flagged'], 0)
        self.assertEqual(res['spw']['5']['flagged'], 32256)
        self.assertEqual(res['flagged'], 32256*2)

    def test_invalid_scan(self):
        '''Flagdata: selection of invalid scans only => error'''
        try:
            flagdata(vis=self.vis, scan='2', flagbackup=False)
        except RuntimeError as instance:
            print('Expected error: %s'%instance)

    def test_default_fparam(self):
        '''Flagdata: default data column FPARAM'''
        flagdata(vis=self.vis, mode='clip', clipminmax=[0,500], flagbackup=False,
                 datacolumn='FPARAM')
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 5325)

    def test_manual_field_selection(self):
        """Flagdata:: Manually flag a Tsys-based CalTable using field selection"""

        flagdata(vis=self.vis, field='0', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['field']['3c279']['flagged'], 9216)
        self.assertEqual(res['field']['Titan']['flagged'], 0)
        self.assertEqual(res['field']['TW Hya']['flagged'], 0)
        self.assertEqual(res['field']['J1037-295=QSO']['flagged'], 0)

    def test_manual_antenna_selection(self):
        """Flagdata:: Manually flag a Tsys-based CalTable using antenna selection"""

        flagdata(vis=self.vis, antenna='DV09', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['antenna']['DV09']['flagged'], 14336)
        self.assertEqual(res['antenna']['DV10']['flagged'], 0)

    def test_clip_fparam_sol1(self):
        """Flagdata:: Test clipping first calibration solution product of FPARAM
        column using a minmax range """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='REAL_Sol1',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_list_fparam_sol1_extension(self):
        """Flagdata:: Test list mode to clip first calibration solution product of FPARAM
        column using a minmax range, and then extend to the other solution """

        cmds = ["mode='clip' datacolumn='FPARAM' correlation='Sol1' "
                "clipzeros=True clipminmax=[0.,600.]",
                "mode='extend' extendpols=True growfreq=0.0 growtime=0.0"]
        flagdata(vis=self.vis, mode='list', inpfile=cmds)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['total'], 129024)
        self.assertEqual(res['flagged'], 1500)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 750)

        # Get the same results when flagging using a file
#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()
        myinput = "mode='clip' datacolumn='FPARAM' correlation='Sol1' clipzeros=True clipminmax=[0.,600.]\n"\
                "mode='extend' extendpols=True growfreq=0.0 growtime=0.0"
        filename = 'callist.txt'
        create_input(myinput, filename)
        flagdata(vis=self.vis, mode='list', inpfile=filename, flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['total'], 129024)
        self.assertEqual(res['flagged'], 1500)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 750)

    def test_clip_fparam_sol2(self):
        """Flagdata:: Test cliping second calibration solution product of FPARAM
        column using a minmax range """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='Sol2',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 442.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 442.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_clip_fparam_sol1sol2(self):
        """Flagdata:: Test cliping first and second calibration solution products of
        FPARAM column using a minmax range """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='Sol1,Sol2',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 1192.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 442.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_invalid_corr(self):
        '''Flagdata: default correlation should be REAL_ALL in this case'''
        flagdata(vis=self.vis, mode='clip', correlation='ABS_ALL',clipminmax=[0.,600.],
                 flagbackup=False, datacolumn='FPARAM')
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 1192.0)

    def test_invalid_datacol_cal(self):
        '''Flagdata: invalid data column should not fall back to default'''
        with self.assertRaises(ValueError):
            flagdata(vis=self.vis, mode='clip', clipminmax=[0.,600.],datacolumn='PARAMERR',
                     flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
#        self.assertEqual(res['flagged'], 1192.0)
        self.assertFalse(res['flagged']==1192.0)

    def test_clip_fparam_all(self):
        """Flagdata:: Test cliping all calibration solution products of FPARAM
        column using a minmax range """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 1192.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 442.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_clip_fparam_all(self):
        """Flagdata:: Test cliping only zeros in all calibration solution
        products of FPARAM column"""

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='',
                 clipzeros=True, flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 126.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 56.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 70.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_clip_nans_fparam_all(self):
        """Flagdata:: Test cliping only NaNs/Infs in all calibration solution products of FPARAM column"""

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='',
                 flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_clip_fparam_error_absall(self):
        """Flagdata:: Error case test when a complex operator is used with CalTables """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='ABS_ALL',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 1192.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 442.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_clip_fparam_error_abs1(self):
        """Flagdata:: Error case test when a complex operator is used with CalTables """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='ABS_Sol1',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024)
        self.assertEqual(res['flagged'], 750)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 0)

    def test_clip_fparam_error_abs12(self):
        """Flagdata:: Fall back to default REAL operator """

        flagdata(vis=self.vis, mode='clip', datacolumn='FPARAM', correlation='ABS Sol1,Sol2',
                 clipzeros=True, clipminmax=[0.,600.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024)
        self.assertEqual(res['flagged'], 1192)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 750.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 442)

    def test_clip_snr_all(self):
        """Flagdata:: Test cliping all calibration solution products of SNR
        column using a minmax range for Tsys CalTable"""

        flagdata(vis=self.vis, mode='clip', datacolumn='SNR', correlation='',
                 clipzeros=True, clipminmax=[0.,2.], flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['total'], 129024.0)
        self.assertEqual(res['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol1']['total'], 64512.0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 0.0)
        self.assertEqual(res['correlation']['Sol2']['total'], 64512.0)

    def test_spw_selection(self):
        '''Flagdata: Select spw in cal tables'''
        flagdata(vis=self.vis, mode='manual', spw='1,3', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['spw']['1']['flagged'],32256)
        self.assertEqual(res['spw']['3']['flagged'],32256)
        self.assertEqual(res['flagged'],32256+32256)

    def test_channel_selection(self):
        '''Flagdata: Select spw:channel in cal tables'''
        flagdata(vis=self.vis, mode='manual', spw='*:0~8,*:120~127', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary', spwchan=True)
        self.assertEqual(res['spw']['1']['flagged'],4284)
        self.assertEqual(res['spw']['3']['flagged'],4284)
        self.assertEqual(res['spw:channel']['5:0']['flagged'],252)
        self.assertEqual(res['spw:channel']['5:0']['total'],252)
        self.assertEqual(res['spw:channel']['5:9']['flagged'],0)
        self.assertEqual(res['spw:channel']['5:119']['flagged'],0)
        self.assertEqual(res['spw:channel']['5:120']['flagged'],252)
        self.assertEqual(res['spw:channel']['5:127']['flagged'],252)
        self.assertEqual(res['flagged'],4284*4)

    def test_tsys_scan1(self):
        '''Flagdata: select valid scans'''
        flagdata(vis=self.vis, mode='manual', scan='1,10,14,30', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'],9216)
        self.assertEqual(res['scan']['3']['flagged'],0)
        self.assertEqual(res['scan']['10']['flagged'],9216)
        self.assertEqual(res['scan']['14']['flagged'],9216)
        self.assertEqual(res['scan']['30']['flagged'],9216)
        self.assertEqual(res['flagged'],9216*4)

    def test_tsys_scan2(self):
        '''Flagdata: select valid and invalid scans'''
        # scan=2 does not exist. It should not raise an error
        flagdata(vis=self.vis, mode='manual', scan='1~3', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'],9216)
        self.assertEqual(res['scan']['3']['flagged'],9216)
        self.assertEqual(res['flagged'],9216*2)

    def test_tsys_time1(self):
        '''Flagdata: select a timerange'''
        flagdata(vis=self.vis, mode='clip', clipminmax=[-2000.,2000.], timerange="<03:50:00",
                 datacolumn='FPARAM', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')['flagged']
        self.assertEqual(res, 5)

    def test_tsys_time2(self):
        '''Flagdata: select a timerange for one spw'''
        # timerange=03:50:00~04:10:00 covers scans 14 17 only
        flagdata(vis=self.vis, mode='manual', timerange="03:50:00~04:10:00",
                 flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['14']['flagged'],9216)
        self.assertEqual(res['scan']['17']['flagged'],9216)
        self.assertEqual(res['spw']['1']['flagged'],4608)
        self.assertEqual(res['spw']['3']['flagged'],4608)
        self.assertEqual(res['flagged'],18432)

        # Run for one spw only
#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, mode='manual', timerange="03:50:00~04:10:00", spw='1',
                 flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['14']['flagged'],2304)
        self.assertEqual(res['scan']['17']['flagged'],2304)
        self.assertEqual(res['spw']['1']['flagged'],4608)
        self.assertEqual(res['spw']['3']['flagged'],0)
        self.assertEqual(res['flagged'],4608)

        # Now check that the same is flagged using scan selection
#        flagdata(vis=self.vis, mode='unflag', flagbackup=False)
        self.unflag_ms()
        flagdata(vis=self.vis, mode='manual', scan='14,17', spw='1',
                 flagbackup=False)
        res1=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res1['scan']['14']['flagged'],2304)
        self.assertEqual(res1['scan']['17']['flagged'],2304)
        self.assertEqual(res1['spw']['1']['flagged'],4608)
        self.assertEqual(res1['spw']['3']['flagged'],0)
        self.assertEqual(res1['flagged'],4608)
        self.assertEqual(res1['flagged'], res['flagged'])

class test_bandpass(test_base):
    """Flagdata:: Test flagging task with Bpass-based CalTable """

    def setUp(self):
        self.setUp_bpass_case()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf cal.fewscans.bpass*')

    def test_unsupported_modes(self):
        '''Flagdata: elevation and shadow are not supported in cal tables'''
        with self.assertRaises(ValueError):
            res = flagdata(vis=self.vis, mode='elevation', flagbackup=False)

        with self.assertRaises(ValueError):
            res = flagdata(vis=self.vis, mode='shadow', flagbackup=False)

    def test_nullselections(self):
        '''Flagdata: unkonwn scan selection in cal tables'''
        try:
            flagdata(vis=self.vis, scan='1', flagbackup=False)
        except RuntimeError as instance:
            print('Expected error: %s'%instance)

    def test_default_cparam(self):
        '''Flagdata: flag CPARAM as the default column'''
        flagdata(vis=self.vis, mode='clip', clipzeros=True, datacolumn='CPARAM', flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 11078.0, 'Should use CPARAM as the default column')

    def test_invalid_datacol(self):
        '''Flagdata: invalid data column should not fall back to default'''
        with self.assertRaises(ValueError):
            flagdata(vis=self.vis, mode='clip', clipzeros=True, datacolumn='PARAMERR',
                     flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
#        self.assertEqual(res['flagged'], 11078.0)
        self.assertNotEqual(res['flagged'], 11078.0)


    def test_manual_field_selection_for_bpass(self):
        """Flagdata:: Manually flag a bpass-based CalTable using field selection"""

        flagdata(vis=self.vis, field='3C286_A', flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')

        self.assertEqual(summary['field']['3C286_A']['flagged'], 499200.0)
        self.assertEqual(summary['field']['3C286_B']['flagged'], 0)
        self.assertEqual(summary['field']['3C286_C']['flagged'], 0)
        self.assertEqual(summary['field']['3C286_D']['flagged'], 0)

    def test_list_field_Selection_for_bpass(self):
        """Flagdata:: Manually flag a bpass-based CalTable using list mode """

        flagdata(vis=self.vis, mode='list', inpfile=["field='3C286_A'"],
                 flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['field']['3C286_A']['flagged'], 499200.0)
        self.assertEqual(summary['field']['3C286_B']['flagged'], 0)
        self.assertEqual(summary['field']['3C286_C']['flagged'], 0)
        self.assertEqual(summary['field']['3C286_D']['flagged'], 0)

    def test_manual_antenna_selection_for_bpass(self):
        """Flagdata:: Manually flag a bpass-based CalTable using antenna selection"""
        flagdata(vis=self.vis, antenna='ea09', flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['antenna']['ea09']['flagged'], 48000.0)
        self.assertEqual(summary['antenna']['ea10']['flagged'], 0.0)

    def test_list_antenna_Selection_for_bpass(self):
        """Flagdata:: Manually flag a bpass-based CalTable using list mode"""

        flagdata(vis=self.vis, mode='list', inpfile=["antenna='ea09'"],
                 flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['antenna']['ea09']['flagged'], 48000.0)
        self.assertEqual(summary['antenna']['ea10']['flagged'], 0.0)

    def test_clip_nan_and_inf_cparam_all_for_bpass(self):
        """Flagdata:: Clip only NaNs and Infs in all calibration solutions of CPARAM column"""

        flagdata(vis=self.vis, mode='clip',datacolumn='CPARAM', correlation='',
                 flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['total'], 1248000.0)
        self.assertEqual(summary['flagged'], 0.0)
        self.assertEqual(summary['correlation']['Sol1']['flagged'], 0.0)
        self.assertEqual(summary['correlation']['Sol1']['total'], 624000.0)
        self.assertEqual(summary['correlation']['Sol2']['flagged'], 0.0)
        self.assertEqual(summary['correlation']['Sol2']['total'], 624000.0)

    def test_clip_minmax_cparam_all_for_bpass(self):
        """Flagdata:: Clip all calibration solutions of CPARAM column using a minmax range"""

        flagdata(vis=self.vis, mode='clip',clipzeros=True, clipminmax=[0,0.3], datacolumn='CPARAM',
                 flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['flagged'], 11175.0)
        self.assertEqual(summary['total'], 1248000)
        self.assertEqual(summary['correlation']['Sol1']['flagged'], 11136.0)
        self.assertEqual(summary['correlation']['Sol2']['flagged'], 39)

    def test_clip_minmax_snr_all_for_bpass(self):
        """Flagdata:: Test cliping all calibration solution products of SNR column using a
        minmax range for bpass CalTable"""

        flagdata(vis=self.vis, mode='clip', clipzeros=True,clipminmax=[0.,550.],datacolumn='snr',
                 correlation='', flagbackup=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['total'], 1248000.0)
        self.assertEqual(summary['flagged'], 74371.0)
        self.assertEqual(summary['correlation']['Sol1']['flagged'], 36327.0)
        self.assertEqual(summary['correlation']['Sol1']['total'], 624000.0)
        self.assertEqual(summary['correlation']['Sol2']['flagged'], 38044.0)
        self.assertEqual(summary['correlation']['Sol2']['total'], 624000.0)

    def test_clip_one_list(self):
        '''Flagdata: Flag one solution using one command in a list'''
        flagdata(vis=self.vis, mode='list', inpfile=["mode='clip' clipminmax=[0,3] "\
        "correlation='REAL_Sol1' datacolumn='CPARAM'"],
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'], 309388)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 0)

    def test_rflag_cparam_sol2_for_bpass(self):
        """Flagdata:: Test rflag solution 2 of CPARAM column for bpass"""

        flagdata(vis=self.vis, mode='rflag', correlation='Sol2', flagbackup=False,
                 datacolumn='CPARAM', extendflags=False)
        summary=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(summary['flagged'], 13197)
        self.assertEqual(summary['correlation']['Sol1']['flagged'], 0)
        self.assertEqual(summary['correlation']['Sol2']['flagged'], 13197)

    def test_tfcrop_cparam_all_for_bpass(self):
        """Flagdata:: Test tfcrop in ABS_ALL calibration solutions of CPARAM column"""

        flagdata(vis=self.vis, mode='clip', datacolumn='CPARAM',correlation='ABS_ALL',clipzeros=True,
                 flagbackup=False)
        flagdata(vis=self.vis, mode='tfcrop', datacolumn='CPARAM',correlation='ABS_ALL',
                 flagbackup=False, extendflags=False)
        summary=flagdata(vis=self.vis, mode='summary')
#        self.assertTrue(abs(summary['flagged'] - 63861.0) <= 5)
#        self.assertEqual(abs(summary['flagged'] - 69369) <= 5)
        assert abs(summary['flagged'] - 49524) <= 5
        assert abs(summary['correlation']['Sol1']['flagged'] - 30427) <= 5
        assert abs(summary['correlation']['Sol2']['flagged'] - 19097) <= 5

    def test_tfcrop_cparam_sol1_extension_for_bpass(self):
        """Flagdata:: Test tfcrop first calibration solution product of CPARAM column,
        and then extend to the other solution for bpass CalTable"""

        flagdata(vis=self.vis, mode='clip', datacolumn='CPARAM',correlation='Sol1',clipzeros=True,
                 flagbackup=False)
        flagdata(vis=self.vis, mode='tfcrop', datacolumn='CPARAM',correlation='Sol1',
                 flagbackup=False, extendflags=False)
        pre=flagdata(vis=self.vis, mode='summary')
        assert abs(pre['flagged'] - 30426) <= 5
        assert abs(pre['correlation']['Sol1']['flagged'] - 30426) <= 5

        # Extend to other solution
        flagdata(vis=self.vis, mode='extend', extendpols=True, growfreq=0.0, growtime=0.0,
                 flagbackup=False)
        pos=flagdata(vis=self.vis, mode='summary')
        assert abs(pos['flagged'] - 2*30426) <= 10
        assert abs(pos['correlation']['Sol2']['flagged'] - 30426) <= 5

    def test_cal_time1(self):
        '''Flagdata: clip a timerange from one field'''
        # this timerange corresponds to field 3C286_A
        flagdata(vis=self.vis, mode='clip', timerange='<14:12:52',clipzeros=True,
                 clipminmax=[0.,0.35], datacolumn='CPARAM',flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['field']['3C286_A']['flagged'],2230)
        self.assertEqual(res['field']['3C286_B']['flagged'],0)
        self.assertEqual(res['field']['3C286_C']['flagged'],0)
        self.assertEqual(res['field']['3C286_D']['flagged'],0)
        self.assertEqual(res['flagged'],2230)

    def test_cal_time_field(self):
        '''Flagdata: clip a timerange from another field'''
        # this timerange corresponds to field 3C286_D
        flagdata(vis=self.vis, mode='clip', timerange='>14:58:33.6',clipzeros=True,
                 clipminmax=[0.,0.4], datacolumn='CPARAM',flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['field']['3C286_A']['flagged'],0)
        self.assertEqual(res['field']['3C286_B']['flagged'],0)
        self.assertEqual(res['field']['3C286_C']['flagged'],0)
        self.assertEqual(res['field']['3C286_D']['flagged'],2221)
        self.assertEqual(res['flagged'],2221)

    def test_cal_time_corr(self):
        '''Flagdata: select a timerange for one solution'''
        flagdata(vis=self.vis, mode='clip', clipminmax=[0.,0.4], timerange='14:23:50~14:48:40.8',
                 correlation='Sol2',datacolumn='CPARAM',flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['Sol1']['flagged'], 0)
        self.assertEqual(res['correlation']['Sol2']['flagged'], 17)
        self.assertEqual(res['flagged'],17)

        # Check that the timerange selection was taken. Flag only the solution
        flagdata(vis=self.vis, mode='unflag', flagbackup=True)
        flagdata(vis=self.vis, mode='clip', clipminmax=[0.,0.4], correlation='Sol2',
                 datacolumn='CPARAM',flagbackup=False)
        res1=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res1['correlation']['Sol1']['flagged'], 0)
        self.assertEqual(res1['correlation']['Sol2']['flagged'], 22)
        self.assertEqual(res1['flagged'],22)
        self.assertEqual(res1['flagged']-res['flagged'], 5)

    def test_observation(self):
        '''flagdata: flag an observation from an old cal table format'''
        # Note: this cal table does not have an observation column.
        # The column and sub-table should be added and the flagging
        # should happen after this.
        flagdata(vis=self.vis, observation='0', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'],1248000)
        self.assertEqual(res['total'],1248000)


class test_newcal(test_base):
    """Flagdata:: Test flagging task with new CalTable format"""

    def setUp(self):
        self.setUp_newcal()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf ap314.gcal*')

    def test_newcal_selection1(self):
        '''Flagdata: select one solution for one scan and spw'''
        flagdata(vis=self.vis, mode='clip', clipminmax=[0,0.1], correlation='Sol1', spw='0',
                 scan='46', datacolumn='CPARAM', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['Sol2']['flagged'], 0)
        self.assertEqual(res['correlation']['Sol1']['flagged'], 27)
        self.assertEqual(res['spw']['0']['flagged'], 27)
        self.assertEqual(res['scan']['46']['flagged'], 27)
        self.assertEqual(res['flagged'],27)

    def test_newcal_time1(self):
        '''Flagdata: select a timerange in a new cal table'''
        flagdata(vis=self.vis, mode='manual', timerange="09:36:00~16:48:00", flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['field']['1328+307']['flagged'],0)
        self.assertEqual(res['field']['2229+695']['flagged'],2052)
        self.assertEqual(res['flagged'],2052)

    def test_newcal_time2(self):
        '''Flagdata: select a timerange for half the scans'''
        flagdata(vis=self.vis, mode='manual', timerange="09:20:00~14:12:00", flagbackup=False)

        # It should flag scans 1~25
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['scan']['1']['flagged'],108)
        self.assertEqual(res['scan']['2']['flagged'],108)
        self.assertEqual(res['scan']['25']['flagged'],108)
        self.assertEqual(res['scan']['27']['flagged'],0)
        # NOTE: data DOES not have all scans
        self.assertEqual(res['flagged'],108*14)

    def test_manual_wrong_corr_rr(self):
        """ flagdata: manual mode, wrong selection in correlation"""
        flagdata(vis=self.vis, mode='manual', correlation="RR", flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        print(f"{res=}")
        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['total'], 2916)

    def test_manual_wrong_antenna_corr_rr(self):
        """ flagdata: manual mode, antenna selection + wrong selection in correlation"""
        flagdata(vis=self.vis, mode='manual', antenna="VA01", correlation="bla", flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        print(f"{res=}")
        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['total'], 2916)

    def test_manual_wrong_corr_rr_ll(self):
        """ flagdata: manual mode, wrong selection in correlation"""
        flagdata(vis=self.vis, mode='manual', correlation="RR, LL", flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        print(f"{res=}")
        self.assertEqual(res['flagged'], 0)
        self.assertEqual(res['total'], 2916)

    def test_list_with_manual_wrong_corr_sel(self):
        """ flagdata: list with a manual mode with wrong selection in corr """
        fagents = ["mode='clip' clipminmax=[0,3] " "correlation='REAL_Sol1'"
                   " datacolumn='CPARAM'"]
        flagdata(vis=self.vis, mode='list', inpfile=fagents, flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        flagged_global = res['flagged']
        self.assertEqual(flagged_global, 649)
        flagged_sol1 = res['correlation']['Sol1']['flagged']
        self.assertEqual(flagged_sol1, flagged_global)
        flagged_sol2 = res['correlation']['Sol2']['flagged']
        self.assertEqual(flagged_sol2, 0)

        # Use same clip in list mode, together with a bogus manual with
        # bogus corr selection. The bogus manual agent should have no effaect
        fagents.append("mode='manual' correlation='RR' datacolumn='CPARAM'")
        flagdata(vis=self.vis, mode='list', inpfile=fagents, flagbackup=False)
        res2 = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(flagged_global, res2['flagged'])
        self.assertEqual(flagged_sol1, res2['correlation']['Sol1']['flagged'])
        self.assertEqual(flagged_sol2, res2['correlation']['Sol2']['flagged'])

    def test_newcal_clip(self):
        '''Flagdata: clip zeros in one solution'''
        flagdata(vis=self.vis, mode='clip', clipzeros=True, correlation='Sol2',
                 datacolumn='CPARAM',flagbackup=False)

        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['correlation']['Sol1']['flagged'],0)
        self.assertEqual(res['correlation']['Sol2']['flagged'],1398)
        self.assertEqual(res['flagged'],1398)

    def test_newcal_obs1(self):
        '''flagdata: flag an observation from a new cal table format'''
        flagdata(vis=self.vis, observation='1', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['observation']['0']['flagged'],0)
        self.assertEqual(res['observation']['1']['flagged'],2052)
        self.assertEqual(res['flagged'],2052)
        self.assertEqual(res['total'],2916)

    def test_newcal_obs2(self):
        '''flagdata: flag an observation and a scan from a new cal table format'''
        # observation=0 has only scan=1
        flagdata(vis=self.vis, observation='0', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['observation']['0']['flagged'],108)
        self.assertEqual(res['scan']['1']['flagged'],108)
        self.assertEqual(res['flagged'],108)

        # Check that obs=0 is scan=1
#        flagdata(vis=self.vis, mode='unflag')
        self.unflag_ms()
        flagdata(vis=self.vis, scan='1', flagbackup=False)
        res=flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['observation']['0']['flagged'],108)
        self.assertEqual(res['scan']['1']['flagged'],108)
        self.assertEqual(res['flagged'],108)

# CAS-5044
class test_weight_spectrum(test_base):
    """flagdata:: Test flagging WEIGHT_SPECTRUM column"""
    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf four_rows_weight_spectrum.ms*')
        os.system('rm -rf msweight.ms*')

    def test_clipzeros_weight(self):
        '''flagdata: datacolumn=WEIGHT_SPECTRUM, clip zeros'''
        self.setUp_wtspec()
        flagdata(vis=self.vis, mode='clip', datacolumn='weight_SPECTRUM',
                 clipzeros=True, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        # First and last channels have WEIGHT_SPECTRUM zero.
        # 2chans * 2pols * 4rows = 16 flagged points
        self.assertEqual(res['flagged'],16)

    def test_clip_range(self):
        '''flagdata: datacolumn=WEIGHT_SPECTRUM, flag a range'''
        self.setUp_wtspec()
        flagdata(vis=self.vis, mode='clip', datacolumn='WEIGHT_SPECTRUM',
                 clipminmax=[0,2.1], spw='0:1~29', flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        # Should clip only correlation LL. Excluding zero channels (0 and 30)
        self.assertEqual(res['flagged'],116)
        self.assertEqual(res['correlation']['RR']['flagged'],0)
        self.assertEqual(res['correlation']['LL']['flagged'],116)

    def test_clip_chanavg(self):
        '''flagdata: datacolumn=WEIGHT_SPECTRUM, channel average'''
        self.setUp_wtspec()

        # jagonzal (CAS-7782 - Generalized pre-averaging for visibility flagging)
        # When doing channel average the resulting WEIGHT_SPECTRUM
        # is the sum (not average) of the input WEIGHT_SPECTRUM
        # Therefore I have to multiply the clip threshold
        # by the number of input channels
        flagdata(vis=self.vis, mode='clip', spw='0:1~29',datacolumn='WEIGHT_SPECTRUM',
                 clipminmax=[0,2.1*29], channelavg=True, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        # Same result as previous test. The values of the weight_spectrum
        # for each channel are the same, excluding the 2 channels that are
        # zero in each polarization
        self.assertEqual(res['flagged'],116)
        self.assertEqual(res['correlation']['RR']['flagged'],0)
        self.assertEqual(res['correlation']['LL']['flagged'],116)

    def test_clip_onepol(self):
        '''flagdata: datacolumn=WEIGHT_SPECTRUM, one polarization'''
        self.setUp_wtspec()
        flagdata(vis=self.vis, mode='clip', datacolumn='WEIGHT_SPECTRUM',
                 clipminmax=[0,2.04], correlation='RR', clipzeros=True, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')

        self.assertEqual(res['flagged'],95)
        self.assertEqual(res['correlation']['RR']['flagged'],95)
        self.assertEqual(res['correlation']['LL']['flagged'],0)

    def test_tfcrop_weight(self):
        '''flagdata: datacolumn=WEIGHT_SPECTRUM, run tfcrop'''
        self.setUp_wtspec()
        flagdata(vis=self.vis, mode='tfcrop', datacolumn='WEIGHT_SPECTRUM',
                 flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'],16)

    def test_weight1(self):
        '''flagdata: use datacolumn='WEIGHT' and clip spw=0'''
        self.setUp_weightcol()
        flagdata(vis=self.vis, mode='clip', datacolumn='WEIGHT',
                 clipminmax=[0,50.0], flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'],31)
        self.assertEqual(res['spw']['0']['flagged'],31)

    def test_weight2(self):
        '''flagdata: use datacolumn='WEIGHT' and clip inside'''
        self.setUp_weightcol()
        flagdata(vis=self.vis, mode='clip', datacolumn='WEIGHT',
                 clipminmax=[0,50.0], clipoutside=False, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'],31)
        self.assertEqual(res['spw']['1']['flagged'],31)

    def test_weight3(self):
        '''flagdata: clip using datacolumn='WEIGHT' and channelavg=True'''
        self.setUp_weightcol()

        # jagonzal (CAS-7782 - Generalized pre-averaging for visibility flagging)
        # When doing channel average the resulting WEIGHT_SPECTRUM
        # is the sum (not average) of the input WEIGHT_SPECTRUM
        # Therefore I have to multiply the clip threshold
        # by the number of input channels
        flagdata(vis=self.vis, mode='clip', datacolumn='WEIGHT',
                 clipminmax=[0,50.0*31], clipoutside=True, channelavg=True, flagbackup=False)

        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['flagged'],31)
        self.assertEqual(res['spw']['0']['flagged'],31)

    def test_weight4(self):
        '''flagdata: clip using datacolumn='WEIGHT' and select some channels'''
        self.setUp_weightcol()
        flagdata(vis=self.vis, mode='clip', datacolumn='WEIGHT', spw='0,1:1~10',
                 clipminmax=[0,31.0], clipoutside=True, flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spwchan=True)
        self.assertEqual(res['flagged'],41)
        self.assertEqual(res['spw']['0']['flagged'],31)
        self.assertEqual(res['spw:channel']['1:0']['flagged'],0)
        self.assertEqual(res['spw:channel']['1:1']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:2']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:3']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:4']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:5']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:6']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:7']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:8']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:9']['flagged'],1)
        self.assertEqual(res['spw:channel']['1:10']['flagged'],1)

    def test_weight5(self):
        '''flagdata: clip using WEIGHT, then using WEIGHT_SPECTRUM'''
        self.setUp_weightcol()
        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='WEIGHT',
                 clipminmax=[0.0, 50.0])

        res = flagdata(vis=self.vis, mode='summary', spwchan=True)
        self.assertEqual(res['flagged'],31)
        self.assertEqual(res['spw']['0']['flagged'],31)

        # Unflag, run mstransform to create a WEIGHT_SPECTRUM and flag again
        self.unflag_ms()
        mstransform(vis=self.vis, outputvis='weight_spectrum.ms',datacolumn='all',
                    usewtspectrum=True)

        # divide WEIGHT clipmax by the number of channels
        newmax = 50.
        flagdata(vis='weight_spectrum.ms', flagbackup=False, mode='clip',
                 datacolumn='WEIGHT_SPECTRUM', clipminmax=[0.0, newmax])
        res = flagdata(vis='weight_spectrum.ms', mode='summary', spwchan=True)
        self.assertEqual(res['flagged'],31)
        self.assertEqual(res['spw']['0']['flagged'],31)

        self.addCleanup(shutil.rmtree, 'weight_spectrum.ms',True)


class test_float_column(test_base):
    """flagdata:: Test flagging FLOAT_DATA column"""

    def setUp(self):
        self.setUp_floatcol()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf SDFloatColumn.ms*')
        if os.path.exists('outfakefield.txt'):
            os.system('rm -rf outfakefield.txt')

    def test_manual_channels(self):
        '''flagdata: flag meta-data from a single-dish MS'''
        flagdata(vis=self.vis, spw='1;3;5;7:0~4,1;3:507~511,5:1019~1023,7:2043~2047')
        res = flagdata(vis=self.vis, mode='summary', spw='1,3,5,7', spwchan=False)
        self.assertEqual(res['spw']['1']['flagged'],20)
        self.assertEqual(res['spw']['3']['flagged'],20)
        self.assertEqual(res['spw']['5']['flagged'],40)
        self.assertEqual(res['spw']['7']['flagged'],40)
        self.assertEqual(res['flagged'],120)

    def test_field_name(self):
        '''flagdata: Field name with whitespaces'''
        flagdata(vis=self.vis, flagbackup=False, field='r aqr')
        res = flagdata(vis=self.vis, mode='summary', field='r aqr')
        self.assertEqual(res['field']['r aqr']['flagged'],14360)

    def test_clip_frange(self):
        '''flagdata: datacolumn=FLOAT_DATA, flag a range'''
        flagdata(vis=self.vis, spw='0',mode='clip', datacolumn='FLOAT_DATA',
                 clipminmax=[0,230.5], flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spw='0')
        self.assertEqual(res['flagged'],3)

    def test_clip_fchanavg(self):
        '''flagdata: datacolumn=FLOAT_DATA, channel average'''
        flagdata(vis=self.vis, mode='clip', spw='2', clipminmax=[0,3.9],
                 channelavg=True, datacolumn='FLOAT_DATA',flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary',spw='2')
        # There is only one channel in each polarization
        self.assertEqual(res['flagged'],2)
        self.assertEqual(res['total'],2)

    def test_clip_fchanavg_onepol(self):
        '''flagdata: datacolumn=FLOAT_DATA, one pol, channel average'''
        flagdata(vis=self.vis, mode='clip', spw='2', clipminmax=[0,3.9],
                 channelavg=True, correlation='YY', flagbackup=False, datacolumn='float_data')
        res = flagdata(vis=self.vis, mode='summary',spw='2')
        # There is only one channel in each polarization
        self.assertEqual(res['flagged'],1)
        self.assertEqual(res['total'],2)

    def test_tfcrop_float(self):
        '''flagdata: datacolumn=FLOAT_DATA, run tfcrop'''
        flagdata(vis=self.vis, mode='tfcrop', datacolumn='FLOAT_DATA',
                 flagbackup=True)
        res = flagdata(vis=self.vis, mode='summary')
        # It only shows that it runs without problems
        self.assertEqual(res['flagged'],264)

    def test_float_autocorr(self):
        '''flagdata: CAS-5286, autocorr=True should not flag single-dish data'''
        flagdata(vis=self.vis, mode='manual', autocorr=True,
                 flagbackup=False)

        # The PROCESSOR TYPE of this dataset is unset, therefore it should not be
        # flagged
        res = flagdata(vis=self.vis, mode='summary', basecnt=True)
        self.assertEqual(res['flagged'],0)
        self.assertEqual(res['baseline']['PM04&&PM04']['flagged'],0)

    def test_field_strange_chars(self):
        '''flagdata: CAS-5481, field name with = sign'''
        # Create a fake list, as this MS does not have such field
        cmdlist = "mode='manual' field='FAKE=FIELD' autocorr=False\n"+\
                   "mode='clip' clipzeros=True field='Is= TO FAKE'\n"

        filename = 'listfakefield.txt'
        create_input(cmdlist, filename)

        flagdata(vis=self.vis, mode='list', inpfile=filename, flagbackup=False,
                  action='', savepars=True, outfile='outfakefield.txt')

        self.assertTrue(filecmp.cmp(filename, 'outfakefield.txt', 1), 'Files should be equal')


class test_tbuff(test_base):
    '''Test flagdata in list mode and time buffer padding'''
    def setUp(self):
        self.setUp_tbuff()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf uid___A002_X72c4aa_X8f5_scan21_spw18_field2_corrXX.ms*')

    def tearDown(self):
        os.system('rm -rf '+self.online)
        os.system('rm -rf '+self.user)

    def test_double_tbuff(self):
        '''flagdata: Apply a tbuff in the online flags'''

        # Apply the sub-set of online flags
        # uid___A002_X72c4aa_X8f5_online.txt contains the DV04&&* flag
        flagdata(self.vis, flagbackup=False,mode='list',inpfile=self.online, tbuff=0.0)
        flags_before = flagdata(self.vis, mode='summary', basecnt=True)

        # Unflag and apply a tbuff=0.504s
        flagdata(self.vis, flagbackup=False,mode='unflag')
        flagdata(self.vis, flagbackup=False,mode='list',inpfile=self.online, tbuff=0.504)
        flags_after = flagdata(self.vis, mode='summary', basecnt=True)

        self.assertEqual(flags_before['flagged'], flags_after['flagged']/2)

    def test_list_tbuff(self):
        '''flagdata: Apply a tbuff list in two files'''

        # Apply the sub-set of online flags and user flags
        # uid___A002_X72c4aa_X8f5_online.txt contains the DV04&&* flag
        # uid___A002_X72c4aa_X8f5_user.txt contains the DV10 flag
        flagdata(self.vis, flagbackup=False,mode='list',inpfile=[self.user,self.online])

        # Only the DV04 baselines are flagged, not the DV10 (except for DV04&DV10)
        flags1 = flagdata(self.vis, mode='summary', basecnt=True)
        self.assertEqual(flags1['antenna']['DV04']['flagged'],29) # DV04&&*
        self.assertEqual(flags1['antenna']['DV10']['flagged'],1) # DV04&DV10

        # Unflag and apply tbuff=[0.504]. It should increase the DV04 and DV10 flags
        flagdata(self.vis, flagbackup=False,mode='unflag')
        flagdata(self.vis, flagbackup=False,mode='list',inpfile=[self.user,self.online], tbuff=[0.504])
        flags2 = flagdata(self.vis, mode='summary', basecnt=True)
        self.assertEqual(flags2['antenna']['DV04']['flagged'],58)
        self.assertEqual(flags2['antenna']['DV10']['flagged'],30)

        # Unflag and apply tbuff=[0.504,0.504]. The same as above
        flagdata(self.vis, flagbackup=False,mode='unflag')
        flagdata(self.vis, flagbackup=False,mode='list',inpfile=[self.online,self.user], tbuff=[0.504,0.504])
        flags3 = flagdata(self.vis, mode='summary', basecnt=True)
        self.assertEqual(flags3['antenna']['DV04']['flagged'],58)
        self.assertEqual(flags3['antenna']['DV10']['flagged'],30)

    def test_list_tbuff_timestamp(self):
        '''flagdata: test a cmd list with tbuff when the timestamps are in HH:MM:SS format'''
        # Before CAS-13664, timestamps without date were not handled correctly with tbuf,
        # and no flagging was applied. All the flag counts after the commands used below
        # were 0.
        # The timestamps in this inpfile omit the date: '2013/11/15/'
        inpfile = [
            "antenna='DA42&&*' timerange='10:35:05.011~10:36:05.162' reason='testing CAS-13664 with an ALMA MS.'",
            "antenna='DA43&&*' timerange='10:35:05.011~10:36:05.162' reason='testing CAS-13664 with an ALMA MS.'"
            ]
        # A) apply an inpfile with time-only formatted timestamps in timerange
        flagdata(self.vis, mode='list', inpfile=inpfile, tbuff=1.1, flagbackup=False)

        flags_tbuff1 = flagdata(self.vis, mode='summary')
        # Without date in timestamp these flags should still be applied
        self.assertEqual(flags_tbuff1['antenna']['DA42']['flagged'], 1682)
        self.assertEqual(flags_tbuff1['antenna']['DA43']['flagged'], 1682)
        self.assertEqual(flags_tbuff1['antenna']['DA44']['flagged'], 116)
        self.assertEqual(flags_tbuff1['antenna']['PM04']['flagged'], 116)

        def asserts_with_tbuff_4_5(obj, summary):
            '''Asserts the same results for the next variations of the same commands'''
            cnt_da42_43 = 1914
            cnt_others = 132
            self.assertEqual(summary['antenna']['DA42']['flagged'], cnt_da42_43)
            self.assertEqual(summary['antenna']['DA43']['flagged'], cnt_da42_43)
            self.assertEqual(summary['antenna']['DA44']['flagged'], cnt_others)
            self.assertEqual(summary['antenna']['PM04']['flagged'], cnt_others)

        # B) Unflag and apply larger tbuff => increase flag counts
        flagdata(self.vis, flagbackup=False,mode='unflag')
        flagdata(self.vis, mode='list', inpfile=inpfile, tbuff=4.5, flagbackup=False)

        flags_tbuff4 = flagdata(self.vis, mode='summary')
        asserts_with_tbuff_4_5(self, flags_tbuff4)

        # C) Unflag, apply the same inpfile but now including the full date/time timestamps
        # => should flag the same as above
        inpfile_date = [
            "antenna='DA42&&*' timerange='2013/11/15/10:35:05.011~2013/11/15/10:36:05.162' reason='testing CAS-13664 with an ALMA MS.'",
            "antenna='DA43&&*' timerange='2013/11/15/10:35:05.011~2013/11/15/10:36:05.162' reason='testing CAS-13664 with an ALMA MS.'"
            ]
        flagdata(self.vis, flagbackup=False,mode='unflag')
        flagdata(self.vis, mode='list', inpfile=inpfile_date, tbuff=4.5, flagbackup=False)

        flags_date = flagdata(self.vis, mode='summary')
        asserts_with_tbuff_4_5(self, flags_date)

        # D) Unflag and apply the same inpfile but now with a mix of timestamp formats in t0
        # and t1 ==> should flag the same as above
        inpfile_mix = [
            "antenna='DA42&&*' timerange='2013/11/15/10:35:05.011~10:36:05.162' reason='testing CAS-13664 with an ALMA MS.'",
            "antenna='DA43&&*' timerange='2013/11/15/10:35:05.011~10:36:05.162' reason='testing CAS-13664 with an ALMA MS.'"
            ]
        flagdata(self.vis, flagbackup=False,mode='unflag')
        flagdata(self.vis, mode='list', inpfile=inpfile_mix, tbuff=4.5, flagbackup=False)
        flags_mix = flagdata(self.vis, mode='summary')
        asserts_with_tbuff_4_5(self, flags_mix)


class TestMergeManualTimerange(unittest.TestCase):
    def setUp(self):
        self.cmds = [
            {'mode': 'summary1'},
            {'mode': 'manual',
             'timerange': '00:00~00:01'},
            {'mode': 'manual',
             'timerange': '00:02~00:03'},
            {'mode': 'summary2'},
            {'mode': 'manual',
             'timerange': '00:04~00:05'},
            {'mode': 'manual',
             'timerange': '00:06~00:07'},
            {'mode': 'summary3'}
            ]

    def test_empty(self):
        self.assertEqual(fh._merge_timerange([]), [])

    def test_merge(self):
        res = fh._merge_timerange(self.cmds)
        self.assertEqual(len(res), 5)
        self.assertEqual(res[0]['mode'], 'summary1')
        self.assertEqual(res[1]['mode'], 'manual')
        self.assertEqual(res[1]['timerange'], '00:00~00:01,00:02~00:03')
        self.assertEqual(res[2]['mode'], 'summary2')
        self.assertEqual(res[3]['mode'], 'manual')
        self.assertEqual(res[3]['timerange'], '00:04~00:05,00:06~00:07')
        self.assertEqual(res[4]['mode'], 'summary3')

        res = fh._merge_timerange(self.cmds[1:])
        self.assertEqual(len(res), 4)
        self.assertEqual(res[0]['mode'], 'manual')
        self.assertEqual(res[0]['timerange'], '00:00~00:01,00:02~00:03')
        self.assertEqual(res[1]['mode'], 'summary2')
        self.assertEqual(res[2]['mode'], 'manual')
        self.assertEqual(res[2]['timerange'], '00:04~00:05,00:06~00:07')
        self.assertEqual(res[3]['mode'], 'summary3')

        res = fh._merge_timerange(self.cmds[:-2])
        self.assertEqual(len(res), 4)
        self.assertEqual(res[0]['mode'], 'summary1')
        self.assertEqual(res[1]['mode'], 'manual')
        self.assertEqual(res[1]['timerange'], '00:00~00:01,00:02~00:03')
        self.assertEqual(res[2]['mode'], 'summary2')
        self.assertEqual(res[3]['mode'], 'manual')
        self.assertEqual(res[3]['timerange'], '00:04~00:05')

    def test_nohash_nomerge(self):
        self.cmds[3]['nohash'] = dict()
        res = fh._merge_timerange(self.cmds)
        self.assertEqual(len(res), 5)
        self.assertEqual(res[0]['mode'], 'summary1')
        self.assertEqual(res[1]['mode'], 'manual')
        self.assertEqual(res[1]['timerange'], '00:00~00:01,00:02~00:03')
        self.assertEqual(res[2]['mode'], 'summary2')
        self.assertEqual(res[3]['mode'], 'manual')
        self.assertEqual(res[3]['timerange'], '00:04~00:05,00:06~00:07')
        self.assertEqual(res[4]['mode'], 'summary3')

    def test_nohash_merge(self):
        self.cmds[2]['nohash'] = dict()
        res = fh._merge_timerange(self.cmds)
        self.assertEqual(len(res), 6)
        self.assertEqual(res[0]['mode'], 'summary1')
        self.assertEqual(res[1]['mode'], 'manual')
        self.assertEqual(res[1]['timerange'], '00:00~00:01')
        self.assertEqual(res[2]['timerange'], '00:02~00:03')
        self.assertEqual(res[3]['mode'], 'summary2')
        self.assertEqual(res[4]['mode'], 'manual')
        self.assertEqual(res[4]['timerange'], '00:04~00:05,00:06~00:07')
        self.assertEqual(res[5]['mode'], 'summary3')

    def test_invalid_range(self):
        cmds = [
            {'mode': 'summary1'},
            {'mode': 'manual',
             'timerange': '00:00~00:01'},
            {'mode': 'manual',
             'timerange': '00:02~00:03'},
            {'mode': 'manual',
             'timerange': '00:03~00:02'},
            {'mode': 'manual',
             'timerange': '00:04~00:05'},
            {'mode': 'summary2'},
            {'mode': 'manual',
             'timerange': '00:04~00:05'},
            {'mode': 'manual',
             'timerange': '00:06~00:07'},
            {'mode': 'summary3'}
            ]
        res = fh._merge_timerange(cmds)
        self.assertEqual(len(res), 7)
        self.assertEqual(res[0]['mode'], 'summary1')
        self.assertEqual(res[1]['mode'], 'manual')
        self.assertEqual(res[1]['timerange'], '00:00~00:01,00:02~00:03')
        self.assertEqual(res[2]['mode'], 'manual')
        self.assertEqual(res[2]['timerange'], '00:03~00:02')
        self.assertEqual(res[3]['mode'], 'manual')
        self.assertEqual(res[3]['timerange'], '00:04~00:05')
        self.assertEqual(res[4]['mode'], 'summary2')
        self.assertEqual(res[5]['mode'], 'manual')
        self.assertEqual(res[5]['timerange'], '00:04~00:05,00:06~00:07')
        self.assertEqual(res[6]['mode'], 'summary3')


class test_preaveraging(test_base):
    """Test channel/time pre-averaging for visibility-based flagging"""

    def setUp(self):
        self.setUp_data4preaveraging()
        self.corrs = ['RL', 'LL', 'LR', 'RR']

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286_spw9_small_for_preaveraging.ms*')

    def tearDown(self):
        os.system('rm -rf test_preaveraging.ms')
        os.system('rm -rf test_clip_timeavg*')
        os.system('rm -rf test_clip_chanavg*')
        os.system('rm -rf test_clip_time_chanavg*')
        os.system('rm -rf test_rflag_timeavg*')
        os.system('rm -rf test_rflag_chanavg*')
        os.system('rm -rf test_rflag_time_chanavg*')
        os.system('rm -rf test_tfcrop_timeavg*')
        os.system('rm -rf test_tfcrop_chanavg*')
        os.system('rm -rf test_tfcrop_time_chanavg*')

    def test_clip_timeavg(self):
        '''flagdata: clip with time average and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Time average with mstransform, then flagging with normal clip
        mstransform(vis=self.vis,outputvis='test_clip_timeavg_step1.ms',datacolumn='data',
                    timeaverage=True,timebin='2s')
        flagdata(vis='test_clip_timeavg_step1.ms',flagbackup=False, mode='clip',clipminmax=[0.0,0.08])
        res1 = flagdata(vis='test_clip_timeavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with clip using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='DATA',
                 timeavg=True, timebin='2s', clipminmax=[0.0,0.08])
        mstransform(vis=self.vis,outputvis='test_clip_timeavg_step2.ms',datacolumn='data',
                    timeaverage=True,timebin='2s')
        res2 = flagdata(vis='test_clip_timeavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res1['flagged'], res2['flagged'])

    def test_clip_chanavg(self):
        '''flagdata: clip with chan average and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Chan average with mstransform, then flagging with normal clip
        mstransform(vis=self.vis,outputvis='test_clip_chanavg_step1.ms',datacolumn='data',
                    chanaverage=True,chanbin=2)
        flagdata(vis='test_clip_chanavg_step1.ms',flagbackup=False, mode='clip',clipminmax=[0.0,0.08])
        res1 = flagdata(vis='test_clip_chanavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with clip using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='DATA',
                 channelavg=True, chanbin=2, clipminmax=[0.0,0.08])
        mstransform(vis=self.vis, outputvis='test_clip_chanavg_step2.ms',datacolumn='data',
                    chanaverage=True,chanbin=2)
        res2 = flagdata(vis='test_clip_chanavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res1['flagged'], res2['flagged'])

    def test_clip_time_chanavg(self):
        '''flagdata: clip with time/chan average and compare vs mstransform'''

        # Unflag the original input data  - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Chan average with mstransform, then flagging with normal clip
        mstransform(vis=self.vis,outputvis='test_clip_time_chanavg_step1.ms',datacolumn='data',
                    timeaverage=True,timebin='2s',chanaverage=True,chanbin=2)
        flagdata(vis='test_clip_time_chanavg_step1.ms',flagbackup=False, mode='clip',clipminmax=[0.0,0.08])
        res1 = flagdata(vis='test_clip_time_chanavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with clip using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='clip', datacolumn='DATA',
                 timeavg=True, timebin='2s', channelavg=True, chanbin=2, clipminmax=[0.0,0.08])
        mstransform(vis=self.vis, outputvis='test_clip_time_chanavg_step2.ms',datacolumn='data',
                    timeaverage=True,timebin='2s',chanaverage=True,chanbin=2)
        res2 = flagdata(vis='test_clip_time_chanavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res1['flagged'], res2['flagged'])

    def test_rflag_timeavg(self):
        '''flagdata: rflag with time average and compare vs mstransform'''

        # # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Time average with mstransform, then flagging with normal rflag
        mstransform(vis=self.vis,outputvis='test_rflag_timeavg_step1.ms',datacolumn='data',
                    timeaverage=True,timebin='2s')
        flagdata(vis='test_rflag_timeavg_step1.ms',flagbackup=False, mode='rflag',extendflags=False)
        res1 = flagdata(vis='test_rflag_timeavg_step1.ms', mode='summary', spwchan=True)

        # # Unflag the original input data - not needed
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with rflag using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='rflag', datacolumn='DATA',
                 timeavg=True, timebin='2s', extendflags=False)
        mstransform(vis=self.vis,outputvis='test_rflag_timeavg_step2.ms',datacolumn='data',
                    timeaverage=True,timebin='2s')
        res2 = flagdata(vis='test_rflag_timeavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res1['flagged'], res2['flagged'])
        self.assertEqual(res1['flagged'], 27)
        for cor in self.corrs:
            self.assertEqual(res1['correlation'][cor]['flagged'],
                             res2['correlation'][cor]['flagged'])

    def test_rflag_timeavg_extendflags(self):
        '''flagdata: rflag with time average + extendflags, and compare vs mstransform'''
        # Unflag the original input data
        flagdata(self.vis, flagbackup=False, mode='unflag')

        timebin = '2s'

        # STEP 1: Time average with mstransform, then flagging with normal rflag+extendflags
        mstransform(vis=self.vis, outputvis='test_rflag_timeavg_extendflags_step1.ms',
                    datacolumn='data', timeaverage=True, timebin=timebin)
        flagdata(vis='test_rflag_timeavg_extendflags_step1.ms', flagbackup=False,
                 mode='rflag', extendflags=True)
        res1 = flagdata(vis='test_rflag_timeavg_extendflags_step1.ms', mode='summary')

        # Unflag again the original input data
        flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with rflag using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='rflag', datacolumn='DATA',
                 timeavg=True, timebin='2s', extendflags=True)
        mstransform(vis=self.vis, outputvis='test_rflag_timeavg_extendflags_step2.ms',
                    datacolumn='data', timeaverage=True, timebin=timebin)
        res2 = flagdata(vis='test_rflag_timeavg_extendflags_step2.ms', mode='summary')

        # Compare results
        self.assertEqual(res1['total'], res2['total'])
        self.assertEqual(res1['flagged'], res2['flagged'])
        self.assertEqual(res1['flagged'], 40)
        for cor in self.corrs:
            self.assertEqual(res1['correlation'][cor]['flagged'],10)
            self.assertEqual(res1['correlation'][cor]['flagged'],
                             res2['correlation'][cor]['flagged'])

    def test_rflag_chanavg(self):
        '''flagdata: rflag with chan average and compare vs mstransform'''

        # Unflag the original input data  - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Chan average with mstransform, then flagging with normal rflag
        mstransform(vis=self.vis,outputvis='test_rflag_chanavg_step1.ms',datacolumn='data',
                    chanaverage=True,chanbin=2)
        flagdata(vis='test_rflag_chanavg_step1.ms',flagbackup=False, mode='rflag',extendflags=False)
        res1 = flagdata(vis='test_rflag_chanavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with rflag using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='rflag', datacolumn='DATA',
                 channelavg=True, chanbin=2,extendflags=False)
        mstransform(vis=self.vis, outputvis='test_rflag_chanavg_step2.ms',datacolumn='data',
                    chanaverage=True,chanbin=2)
        res2 = flagdata(vis='test_rflag_chanavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res1['flagged'], res2['flagged'])
        for cor in self.corrs:
            self.assertEqual(res2['correlation'][cor]['flagged'],
                             res1['correlation'][cor]['flagged'])

    def test_rflag_chanavg_extendflags(self):
        '''flagdata: rflag with chan average + extendflags, and compare vs mstransform'''

        # Unflag the original input data  - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        chanbin = 8

        # STEP 1: Chan average with mstransform, then flagging with normal rflag
        mstransform(vis=self.vis, outputvis='test_rflag_chanavg_extendflags_step1.ms',
                    datacolumn='data', chanaverage=True, chanbin=chanbin)
        flagdata(vis='test_rflag_chanavg_extendflags_step1.ms', flagbackup=False,
                 mode='rflag', extendflags=True)
        res1 = flagdata(vis='test_rflag_chanavg_extendflags_step1.ms', mode='summary')

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with rflag using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='rflag', datacolumn='DATA',
                 channelavg=True, chanbin=chanbin, extendflags=True)
        mstransform(vis=self.vis, outputvis='test_rflag_chanavg_extendflags_step2.ms',
                    datacolumn='data', chanaverage=True, chanbin=chanbin)
        res2 = flagdata(vis='test_rflag_chanavg_extendflags_step2.ms', mode='summary')

        # Compare results
        self.assertEqual(res1['total'], res2['total'])
        self.assertEqual(res1['flagged'], res2['flagged'])
        self.assertEqual(res1['flagged'], 20)
        for cor in self.corrs:
            self.assertEqual(res1['correlation'][cor]['flagged'], 5)
            self.assertEqual(res2['correlation'][cor]['flagged'],
                             res1['correlation'][cor]['flagged'])

    def test_rflag_time_chanavg(self):
        '''flagdata: rflag with time/chan average and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: chan+time average with mstransform, then flagging with normal rflag
        mstransform(vis=self.vis,outputvis='test_rflag_time_chanavg_step1.ms',
                    datacolumn='data', timeaverage=True, timebin='2s',
                    chanaverage=True, chanbin=2)
        res1 = flagdata(vis='test_rflag_time_chanavg_step1.ms', action='calculate',
                        mode='rflag', extendflags=False)

        # STEP 2: rflag using chan+time average, then mstransform using chan+time avg
        flagdata(vis=self.vis, flagbackup=False, mode='rflag', datacolumn='DATA',
                 timeavg=True, timebin='2s', channelavg=True, chanbin=2, extendflags=False)
        mstransform(vis=self.vis, outputvis='test_rflag_time_chanavg_step2.ms',
                    datacolumn='data', timeaverage=True, timebin='2s',
                    chanaverage=True, chanbin=2)
        res2 = flagdata(vis='test_rflag_time_chanavg_step2.ms', action='calculate',
                        mode='rflag', extendflags=False)

        # Check results. Note when doing chan+time avg we cannot assume the thresholds and #
        # of flagged channels will be the same
        self.assertEqual(res1['type'], 'list')
        self.assertEqual(res1['type'], res2['type'])

        # The tolerance for timedev needs to be absurdly big because of osx 10.12
        # See CAS-11572, the "data4preaveraging" dataset should have more than 4 rows.
        tolerances = [1.1, 7.5e-1]
        for threshold_type, tol in zip(['freqdev', 'timedev'], tolerances):
            self.assertTrue(np.less_equal(res2['report0'][threshold_type],
                                          res1['report0'][threshold_type]).all())
            self.assertTrue(np.allclose(res1['report0'][threshold_type],
                                        res2['report0'][threshold_type], rtol=tol))

    def test_tfcrop_timeavg(self):
        '''flagdata: tfcrop with time average and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Time average with mstransform, then flagging with normal tfcrop
        mstransform(vis=self.vis,outputvis='test_tfcrop_timeavg_step1.ms',datacolumn='data',
                    timeaverage=True,timebin='2s')
        flagdata(vis='test_tfcrop_timeavg_step1.ms',flagbackup=False, mode='tfcrop',
                 extendflags=False)
        res1 = flagdata(vis='test_tfcrop_timeavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with tfcrop using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='tfcrop', datacolumn='DATA',
                 timeavg=True, timebin='2s', extendflags=False)
        mstransform(vis=self.vis,outputvis='test_tfcrop_timeavg_step2.ms',datacolumn='data',
                    timeaverage=True,timebin='2s')
        res2 = flagdata(vis='test_tfcrop_timeavg_step2.ms', mode='summary', spwchan=True)

        # Check results
        self.assertEqual(res2['flagged'], res2['flagged'])
        for cor in self.corrs:
            self.assertEqual(res2['correlation'][cor]['flagged'],
                             res1['correlation'][cor]['flagged'])

    def test_tfcrop_timeavg_extendflags(self):
        '''flagdata: tfcrop with time average + extendflags, and compare vs mstransform'''

        # Unflag the original input data  - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        timebin = '2s'

        # STEP 1: Time average with mstransform, then flagging with normal tfcrop
        mstransform(vis=self.vis, outputvis='test_tfcrop_timeavg_extendflags_step1.ms',
                    datacolumn='data', timeaverage=True, timebin=timebin)
        flagdata(vis='test_tfcrop_timeavg_extendflags_step1.ms', flagbackup=False,
                 mode='tfcrop', extendflags=True)
        res1 = flagdata(vis='test_tfcrop_timeavg_extendflags_step1.ms', mode='summary')

        # Unflag the original input data
        flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with tfcrop using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='tfcrop', datacolumn='DATA',
                 timeavg=True, timebin=timebin, extendflags=True)
        mstransform(vis=self.vis,outputvis='test_tfcrop_timeavg_extendflags_step2.ms',
                    datacolumn='data', timeaverage=True, timebin='2s')
        res2 = flagdata(vis='test_tfcrop_timeavg_extendflags_step2.ms', mode='summary')

        # Check results
        self.assertEqual(res1['total'], res2['total'])
        self.assertEqual(res1['flagged'], res2['flagged'])
        self.assertEqual(res1['flagged'], 96)
        for cor in self.corrs:
            self.assertEqual(res1['correlation'][cor]['flagged'], 24)
            self.assertEqual(res1['correlation'][cor]['flagged'],
                             res2['correlation'][cor]['flagged'])

    def test_tfcrop_chanavg(self):
        '''flagdata: tfcrop with chan average and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        chanbin = 2

        # STEP 1: Chan average with mstransform, then flagging with normal tfcrop
        mstransform(vis=self.vis,outputvis='test_tfcrop_chanavg_step1.ms',datacolumn='data',
                    chanaverage=True,chanbin=2)
        flagdata(vis='test_tfcrop_chanavg_step1.ms',flagbackup=False, mode='tfcrop',
                 extendflags=False)
        res1 = flagdata(vis='test_tfcrop_chanavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with tfcrop using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='tfcrop', datacolumn='DATA',
                 channelavg=True, chanbin=chanbin, extendflags=False)
        mstransform(vis=self.vis, outputvis='test_tfcrop_chanavg_step2.ms',datacolumn='data',
                    chanaverage=True, chanbin=chanbin)
        res2 = flagdata(vis='test_tfcrop_chanavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res1['flagged'], res2['flagged'])
        for cor in self.corrs:
            self.assertEqual(res2['correlation'][cor]['flagged'],
                             res1['correlation'][cor]['flagged'])

    def test_tfcrop_chanavg_extendflags(self):
        '''flagdata: tfcrop with chan average + extendflags, and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        chanbin = 4

        # STEP 1: Chan average with mstransform, then flagging with normal tfcrop
        mstransform(vis=self.vis, outputvis='test_tfcrop_chanavg_extendflags_step1.ms',
                    datacolumn='data', chanaverage=True, chanbin=chanbin)
        flagdata(vis='test_tfcrop_chanavg_extendflags_step1.ms', flagbackup=False,
                 mode='tfcrop', extendflags=True)
        res1 = flagdata(vis='test_tfcrop_chanavg_extendflags_step1.ms', mode='summary')

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with tfcrop using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='tfcrop', datacolumn='DATA',
                 channelavg=True, chanbin=chanbin, extendflags=True)
        mstransform(vis=self.vis, outputvis='test_tfcrop_chanavg_extendflags_step2.ms',
                    datacolumn='data', chanaverage=True, chanbin=chanbin)
        res2 = flagdata(vis='test_tfcrop_chanavg_extendflags_step2.ms', mode='summary')

        # Compare results
        self.assertEqual(res1['total'], res2['total'])
        self.assertEqual(res1['flagged'], res2['flagged'])
        for cor in self.corrs:
            self.assertEqual(res1['correlation'][cor]['flagged'], 8)
            self.assertEqual(res2['correlation'][cor]['flagged'],
                             res1['correlation'][cor]['flagged'])

    def test_tfcrop_time_chanavg(self):
        '''flagdata: tfcrop with time/chan average and compare vs mstransform'''

        # Unflag the original input data - alread done by setUp
        # flagdata(self.vis, flagbackup=False, mode='unflag')

        # STEP 1: Chan average with mstransform, then flagging with normal tfcrop
        mstransform(vis=self.vis,outputvis='test_tfcrop_time_chanavg_step1.ms',
                    datacolumn='data', timeaverage=True, timebin='2s', chanaverage=True,
                    chanbin=2)
        flagdata(vis='test_tfcrop_time_chanavg_step1.ms',flagbackup=False, mode='tfcrop',
                 extendflags=False)
        res1 = flagdata(vis='test_tfcrop_time_chanavg_step1.ms', mode='summary', spwchan=True)

        # Unflag the original input data
        flagdata(vis=self.vis, flagbackup=False, mode='unflag')

        # STEP 2: Flagging with tfcrop using time average, then time average with mstransform
        flagdata(vis=self.vis, flagbackup=False, mode='tfcrop', datacolumn='DATA',
                 timeavg=True, timebin='2s', channelavg=True, chanbin=2, extendflags=False)
        mstransform(vis=self.vis, outputvis='test_tfcrop_time_chanavg_step2.ms',datacolumn='data',
                    timeaverage=True,timebin='2s',chanaverage=True,chanbin=2)
        res2 = flagdata(vis='test_tfcrop_time_chanavg_step2.ms', mode='summary', spwchan=True)

        # Compare results
        self.assertEqual(res2['flagged'], res2['flagged'])
        for cor in self.corrs:
            self.assertEqual(res2['correlation'][cor]['flagged'],
                             res1['correlation'][cor]['flagged'])


# Motivated by CAS-11397. test_preaveraging is about datacolumn='data', and checks what
# flags are written to the output
# test_preaveraging_rflag_residual is about datacolumn='residual' and doesn't write flags. It
# checks the threshold calculations from RFlag
class test_preaveraging_rflag_residual(test_base):
    """Test pre-averaging (channel / time) with RFlag and datacolumn='residual'"""

    def setUp(self):
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def tearDown(self):
        os.system('rm -rf test_rflag_timeavg_residual*step2*ms')
        os.system('rm -rf test_rflag_channelavg_residual*step2*ms')

    def test_rflag_timeavg_on_residual(self):
        '''flagdata: rflag with timeavg on residual (corrected - model), and compare
        vs mstransform + rflag without timeavg'''

        # Initial integration time of 'Four_ants_3C286.ms' is 1s
        timebin = '8s'

        # using action calculate, which is faster (reduced I/O) and enough to test thresholds
        # using only these spws for speed
        spws = '0,1,2'

        # STEP 1: rflag-calculate with original time
        res1 = flagdata(vis=self.vis, spw=spws, action='calculate', mode='rflag',
                        datacolumn='residual', extendflags=False)

        # STEP 2: timeavg with mstransform, then rflag-calculate on residual
        flagged2 = 'test_rflag_timeavg_residual_step2.ms'
        mstransform(vis=self.vis, spw=spws, outputvis=flagged2,
                    datacolumn='data,model,corrected', timeaverage=True, timebin=timebin)
        res2 = flagdata(vis=flagged2, spw=spws, action='calculate', mode='rflag',
                        datacolumn='residual', extendflags=False)

        # STEP 3: rflag-calculate with timeavg on residual
        res3 = flagdata(vis=self.vis, spw=spws, action='calculate', mode='rflag',
                        datacolumn='residual', timeavg=True, timebin=timebin,
                        extendflags=False)

        def check_reports_timeavg(report1, report2, report3):
            self.assertEqual(report2['type'], 'rflag')
            self.assertEqual(report3['type'], report2['type'])
            freq_tol = 1e-1
            self.assertTrue(np.allclose(report1['freqdev'], report3['freqdev'],
                                        rtol=freq_tol))
            self.assertTrue(np.allclose(report2['freqdev'], report3['freqdev'],
                                        rtol=freq_tol))
            # divide 3rd column (thresholds). Matrices have rows like: field, spw, threshold.
            report1['timedev'][:,2] = report1['timedev'][:,2] / np.sqrt(8)
            time_div_tol = 3.3e-1
            self.assertTrue(np.allclose(report1['timedev'], report3['timedev'],
                                        rtol=time_div_tol))
            time_tol = 1.5e-1
            self.assertTrue(np.allclose(report2['timedev'],
                                        report3['timedev'], rtol=time_tol))

        self.assertEqual(res1['type'], 'list')
        self.assertEqual(res1['type'], res2['type'])
        self.assertEqual(res1['type'], res3['type'])
        check_reports_timeavg(res1['report0'], res2['report0'], res3['report0'])

    def test_rflag_channelavg_on_residual(self):
        '''flagdata: rflag with channelavg on residual (corrected - model), and compare
        vs mstransform + rflag without average'''

        # Initial integration time of 'Four_ants_3C286.ms' is 1s
        timebin = '8s'

        # using action calculate, which is faster (reduced I/O) and enough to test thresholds
        # using only these spws for speed
        spws = '0,1,2'

        # STEP 1: rflag-calculate with original MS
        res1 = flagdata(vis=self.vis, spw=spws, action='calculate', mode='rflag',
                        datacolumn='residual', extendflags=False)

        # STEP 2: chanavg with mstransform, then rflag-calculate on residual
        flagged2 = 'test_rflag_channelavg_residual_step2.ms'
        mstransform(vis=self.vis, spw=spws, outputvis=flagged2,
                    datacolumn='data,model,corrected', chanaverage=True, chanbin=32)
        res2 = flagdata(vis=flagged2, spw=spws, action='calculate', mode='rflag',
                        datacolumn='residual', extendflags=False)

        # STEP 3: rflag-calculate with channelavg on residual
        res3 = flagdata(vis=self.vis, spw=spws, action='calculate', mode='rflag',
                        datacolumn='residual', channelavg=True, chanbin=32,
                        extendflags=False)

        def check_reports_channelavg(report1, report2, report3):
            self.assertEqual(report2['type'], 'rflag')
            self.assertEqual(report3['type'], report2['type'])
            # divide 3rd column (thresholds). Matrices have rows like: field, spw, threshold.
            report1['freqdev'][:,2] = report1['freqdev'][:,2] / 2
            freq_div_tol = 1e-1
            self.assertTrue(np.allclose(report1['freqdev'], report3['freqdev'],
                                        rtol=freq_div_tol))
            freq_tol = 5e-2
            self.assertTrue(np.allclose(report2['freqdev'], report3['freqdev'],
                                        rtol=freq_tol))

            report1['timedev'][:,2] = report1['timedev'][:,2] / 4
            time_div_tol = 6.6e-1
            self.assertTrue(np.allclose(report1['timedev'], report3['timedev'],
                                        rtol=time_div_tol))
            time_tol = 5e-2
            self.assertTrue(np.allclose(report2['timedev'],
                                        report3['timedev'], rtol=time_tol))

        self.assertEqual(res1['type'], 'list')
        self.assertEqual(res1['type'], res2['type'])
        self.assertEqual(res1['type'], res3['type'])
        check_reports_channelavg(res1['report0'], res2['report0'], res3['report0'])


class test_virtual_col(test_base):
    def setUp(self):
        self.setUp_ngc5921(force=True)

    def tearDown(self):
        os.system('rm -rf ngc5921*')

    def test_no_model_col(self):
        '''flagdata: catch failure when MODEL or virtual MODEL do not exist'''
        # Verify that a MODEL or virtual MODEL column do not exist in MS
        tblocal = table()
        tblocal.open(self.vis)
        cols = tblocal.colnames()
        tblocal.close()

        tblocal.open(self.vis+'/SOURCE')
        cols_v = tblocal.colnames()
        tblocal.close()

        self.assertFalse('MODEL_DATA' in cols, 'Test cannot have a MODEL_DATA column')
        self.assertFalse('SOURCE_MODEL' in cols_v, 'Test cannot have a virtual MODEL column')

        # Run flagdata on it. RESIDUAL_DATA = DATA - MODEL
        with self.assertRaises(ValueError):
            flagdata(self.vis, mode='clip', datacolumn='RESIDUAL_DATA',
                     clipminmax=[2.3,3.1],clipoutside=False)

    def test_virtual_model_col(self):
        '''flagdata: Tests using a virtual MODEL column'''

        # Copy MS to new MS
        os.system('cp -RH ngc5921.ms ngc5921_virtual.ms')
        self.MSvirtual = 'ngc5921_virtual.ms'

        # First, run setjy to create a virtual MODEl column (SOURCE_MODEL)
        setjy(vis=self.MSvirtual, field='1331+305*',model='',standard='Perley-Taylor 99',
                scalebychan=False, usescratch=False)

        # Verify that the virtual column exist
        mcol = th.getColDesc(self.MSvirtual+'/SOURCE', 'SOURCE_MODEL')
        mkeys = mcol.keys()
        self.assertTrue(mkeys.__len__() > 0, 'Should have a SOURCE_MODEL column')

        # Run flagdata on it. RESIDUAL_DATA = DATA - MODEL
        flagdata(vis=self.MSvirtual,mode='clip',datacolumn='RESIDUAL_DATA',clipminmax=[2.3,3.1],clipoutside=False)
        res_virtual = flagdata(vis=self.MSvirtual, mode='summary')['flagged']

        # Compare with a normal MODEL column flagging
        # Run setjy to create a normal MODEl column (SOURCE_MODEL)
        setjy(vis=self.vis, field='1331+305*',model='',standard='Perley-Taylor 99',
                scalebychan=False, usescratch=True)

        flagdata(vis=self.vis,mode='clip',datacolumn='RESIDUAL_DATA',clipminmax=[2.3,3.1],clipoutside=False)
        res = flagdata(vis=self.vis, mode='summary')['flagged']

        self.assertEqual(res_virtual, res, 'Flagging using virtual MODEL column differs from normal MODEL column')


class test_flags_propagation_base(test_base):
    """
    Common methods and infrastructure used in test_flags_propagation_channelavg and
    test_flags_propagation_timeavg.
    """

    def tearDown(self):
        shutil.rmtree(self.vis)

    def get_flags(self, mss):
        """
        Returns the flags column of an MS. Use only on tiny MSs as the one used in this
        test

        :param mss: An MS
        :return: The FLAG column of the MS
        """
        try:
            tbt = table()
            tbt.open(mss)
            return tbt.getcol('FLAG')
        finally:
            tbt.close()

    def check_flags_preserved(self, flags_before, flags_after):
        """
        Check 'flags before' against 'flags after' and ensures that all the flags set
        'before' are also set 'after'.
        The flags are expected in the same format as returned by tbtool.getcol('FLAG').
        This is to ensure the desired behavior from CAS-12737 (never lose flags).

        :param before_flags: flags before manipulating/flagging an MS
        :param after_flags: flags after manipulating/flagging an MS
        :return: true if all flags set in flags_before are also set in flags_after
        """
        flag_cnt_before = np.count_nonzero(flags_before)
        and_flags = np.logical_and(flags_before, flags_after)
        flag_cnt_and = np.count_nonzero(and_flags)

        if flag_cnt_and != flag_cnt_before:
            print(' * Not all the flags set before ({}) are set after ({}). Flags before: '
                  '{}\n Flags after: {}'.format(flag_cnt_before, flag_cnt_and,
                                                flags_before, flags_after))
        return flag_cnt_and == flag_cnt_before


class test_flags_propagation_channelavg(test_flags_propagation_base):
    """
    Tests on the number and positions of flags when using
       flagdata + channelavg + autoflag_methods AND the dataset is already flagged
    ... where channelavg is implemented via the ChannelAverageTVI
    This is to make sure that flags set before the flagdata command are preserved
    (CAS-12737). The tests check the expected number of flags from several methods (clip,
    tfcrop, rflag) and that all the data points originally flagged are still flagged after
    applying, in the exact same positions.

    Uses the small VLA dataset from "data4preaveraging" which is convenient for visual
    and/or manual inspection via the browser, table tool, etc.

    To illustrate the potential "loss" of flags before the fix from CAS-12737, the tests
    use a range of chanbin values (~2...5) with intentionally sparse "a priori" flags like
    X 0 X 0 X 0 X 0     (with chanbin=2 could produce a total loss of "a priori" flags)
    or
    X 0 0 X 0 0 X 0 0   (with chanbin=3 could produce a total loss of "a priori" flags)
    ...
    There are notes in the comments that give the final number of flags that would be seen
    before the fix from CAS-12737 (much lower, lower than the original "a priori" flags).
    """

    def setUp(self):
        self.setUp_data4preaveraging()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286_spw9_small_for_preaveraging.ms*')

    def run_auto_flag_preavg_propagation(self, chanbin=2, mode='clip', ims='', **kwargs):
        """
        Enables channel average and prepares a priori flags in a sparse pattern across
        channels such that we can test (back)propagation of flags after channel-averaging.
        One channel is flagged every 'chanbin'
        With chanbin=2, 50% of channels will be a priori flagged (X 0 X 0 X 0...)
        With chanbin-3, 33% of channels will be a priori flagged, and so on (X 0 0 X 0 0...)
        This would maximize the "loss" of flags as seen in CAS-12737

        :param chanbin: chanbin as used in flagdata
        :param mode: auto-flag mode
        :param kwargs: Use kwargs to pass mode specific parameters, such as clipminmax for
        clip, etc.
        :return: res+apriori_flags+final_flags. res is the flagdata summary dict from the
        MS after applying flagging with channelavg. apriori_flags is the FLAG column before
        applying channelavg+autoflag_method. final_flags is the FLAG column after applying
        channelavg+autoflag_method.
        """
        def get_nchan(ims):
            """
            This function assumes single-SPW (as is the case in this test) or all SPWs
            have the same number of channels.

            :param ims: an MS name
            :return: number of channels in SPW(s)
            """
            try:
                tbt = table()
                tbt.open(os.path.join(ims, 'SPECTRAL_WINDOW'))
                chans = tbt.getcol('NUM_CHAN')
                if len(chans) < 1:
                    raise RuntimeError('Inconsistency found, NUM_CHAN: {}'.format(chans))
                if not np.all(chans[0] == chans):
                    raise RuntimeError('This supports only MSs with all SPWs with the same '
                                       'number of channels. Got NUM_CHAN: {}'.format(chans))
                nchan = chans[0]
            except RuntimeError as exc:
                raise RuntimeError('Error while trying to figure out the #channels: {}'.
                                   format(exc))
            finally:
                tbt.close()

            return nchan

        flagdata(vis=ims, mode='unflag')

        # Pre-flag channels, for example '*:0,1,2,4,...62'
        nchan = get_nchan(ims)
        flag_chans = np.arange(0, nchan, chanbin)
        flag_spw_str = '*:{}'.format(';'.join(['{}'.format(chan) for chan in flag_chans]))
        flagdata(vis=ims, mode='manual', spw=flag_spw_str)

        apriori_flags = self.get_flags(self.vis)

        res_avg = flagdata(vis=ims, mode=mode, channelavg=True, chanbin=chanbin, **kwargs)

        res = flagdata(vis=ims, mode='summary')

        final_flags = self.get_flags(self.vis)

        return res, apriori_flags, final_flags

    def test_propagation_clip_chanbin_2(self):
        """ clip, chanavg, chanbin=2, propagate flags forth and back """

        # Make clip flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(chanbin=2, ims=self.vis,
                                                  clipminmax=[0.0, 0.1])

        self.assertEqual(res['total'], 1024)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 44
        # Instead of >= 512 (a priori)
        self.assertEqual(res['flagged'], 534)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_clip_chanbin_3(self):
        """ clip, chanavg, chanbin=3, propagate flags forth and back """

        # Make clip flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(chanbin=3, ims=self.vis,
                                                  clipminmax=[0.001, 0.1])

        self.assertEqual(res['total'], 1024)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 40
        # Instead of >= 352 (a priori)
        self.assertEqual(res['flagged'], 368)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_tfcrop_chanbin_4(self):
        """ tfcrop, chanavg, chanbin=4, propagate flags forth and back """

        # Make tfcrop flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(chanbin=4, ims=self.vis,
                                                  mode='tfcrop', extendflags=False)

        self.assertEqual(res['total'], 1024)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 68
        # Instead of >= 256
        self.assertEqual(res['flagged'], 307)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_rflag_chanbin_5(self):
        """ rflag, chanavg, chanbin=2, propagate flags forth and back """

        # Make rflag flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(chanbin=5, ims=self.vis,
                                                  mode='rflag', extendflags=False)

        self.assertEqual(res['total'], 1024)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 35
        # Instead of >= 208
        self.assertEqual(res['flagged'], 236)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_clip_chanbin_64(self):
        """ clip, chanavg, chanbin=64 (all), propagate flags forth and back """

        # Make clip flag something (if no flags are added, the flag cube is not written)
        # Use min=0.0025 to flag very little but still something
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(chanbin=64, ims=self.vis,
                                                  clipminmax=[0.004, 0.1])

        self.assertEqual(res['total'], 1024)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 196
        # (the total is >= 16 a priori, but losing some of the initial flags which would
        # be overwritten as False).
        self.assertEqual(res['flagged'], 205)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')


class test_flags_propagation_timeavg(test_flags_propagation_base):
    """
    Tests on the number and positions of flags when using
       flagdata + timeavg + autoflag_methods AND the dataset is already flagged
    ... where timeavg is implemented via the AveragingTVI
    This is to make sure that flags set before the flagdata command are preserved
    (CAS-12737). Similarly as in the tests test_flags_propagation_timeavg, the tests of this
    class check the expected number of flags from several methods (clip, tfcrop, rflag) and
    that all the data points originally flagged are still flagged after applying, in the
    exact same positions.

    Uses the small VLA dataset from "data4timeavg" which has enough integrations to test
    a range of timebins

    To illustrate the potential "loss" of flags before the fix from CAS-12737, the tests
    use a range of timebin values (2s...100s) with intentionally sparse "a priori" flags
    like
    X 0 X 0 X 0 X 0     (with timebin=2 could produce a total loss of "a priori" flags)
    or
    X 0 0 X 0 0 X 0 0   (with timebin=3 could produce a total loss of "a priori" flags)
    ...

    There are notes in the comments that give the final number of flags that would be seen
    before the fix from CAS-12737 (much lower, lower than the initial "a priori" flags).
    """

    def setUp(self):
        self.setUp_data4timeavg()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286_spw9_small_for_timeavg.ms*')

    def run_auto_flag_preavg_propagation(self, timebin=2, mode='clip', ims='', **kwargs):
        """
        Enables time average and prepares a priori flags in a sparse pattern through rows/
        time. The pattern is then used to test the (back)propagation of flags after
        time-averaging.
        One row or timestamp is flagged every 'timebin' where timebin is an integer flagging
        step.
        With timebin=2, 50% of timestamps will be flagged (X 0 X 0 X 0...)
        With timebin-3, 33% of timestamps will be flagged (X 0 0 X 0 0...) and so on.
        This pattern maximizes the "loss" of flags as seen in CAS-12737.
        Uses the column 'TIME_CENTROID' (and the time reference from there) to find the list
        of timestamps to flag.

        :param timebin: number of timestamps to average (in time) - does not check actual
                        integrations times
        :param mode: one auto-flag mode
        :param ims: input ms name
        :param kwargs: to pass mode specific parameters, such as rflag thresholds, etc.
        :return: res+apriori_flags+final_flags. res is the flagdata summary dict from the
        MS after applying flagging with timeavg. apriori_flags is the FLAG column before
        applying timeavg+autoflag_method. final_flags is the FLAG column after applying
        timeavg+autoflag_method.
        """

        def get_unique_ms_times(ims):
            """
            Get the list of unique time stamps of the MS (from TIME_CENTROID).

            :param: ims: an MS name
            :return: list of unique times in the MS. Times as produced by the quanta tool.
            """
            try:
                tbt = table()
                tbt.open(ims)
                times = tbt.getcol('TIME_CENTROID')

                ref = tbt.getcolkeyword('TIME_CENTROID', 'MEASINFO')['Ref']
            except RuntimeError as exc:
                pass
            finally:
                tbt.close()

            centroids = np.unique(times)
            # Produce time records ready for flagdata.
            # This could be done without measures tool:
            # times = [qat.time({'unit': 's', 'value': cent, 'refer': ref, # 'UTC'
            #                    'type': 'epoch'},
            #                   form=['ymd'], prec=9)[0]
            #          for cent in centroids]
            # Or even with plain string formatting:
            # times = [time.strftime('%Y/%m/%d/%H:%M:%S.%f',
            #          time.gmtime(cent)) for cent in  centroids]
            # But better to produce times with the measures tool and using MEASINFO:
            qat = quanta()
            met = measures()
            times = [qat.time(met.epoch('ref', '{}s'.format(cent))['m0'], form=['ymd'],
                              prec=9)[0]
                     for cent in centroids]
            return times

        flagdata(vis=ims, mode='unflag')

        # Pre-flag some timestamps, at 'timebin' steps
        times = get_unique_ms_times(ims)
        flag_times = ['timerange={}'.format(one) for one in times[0::timebin]]

        flagdata(vis=ims, mode='list', inpfile=flag_times)

        apriori_flags = self.get_flags(self.vis)

        res_avg = flagdata(vis=ims, mode=mode, timeavg=True, timebin='{}s'.
                           format(timebin), **kwargs)

        res = flagdata(vis=ims, mode='summary')

        final_flags = self.get_flags(self.vis)

        return res, apriori_flags, final_flags

    def test_propagation_clip_timebin_2s(self):
        """ clip, timeavg, timebin=2, propagate flags forth and back """

        # Make clip flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(timebin=2, ims=self.vis,
                                                  clipminmax=[0.0, 0.1])

        self.assertEqual(res['total'], 25600)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 1490
        self.assertEqual(res['flagged'], 10311)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_clip_timebin_5s(self):
        """ clip, timeavg, timebin=5, propagate flags forth and back """

        # Make clip flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(timebin=5, ims=self.vis,
                                                  clipminmax=[0.001, 0.1])

        self.assertEqual(res['total'], 25600)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 1339
        self.assertEqual(res['flagged'], 5140)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_tfcrop_timebin_20(self):
        """ tfcrop, timeavg, timebin=20, propagate flags forth and back """

        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(timebin=20, ims=self.vis,
                                                  mode='tfcrop', extendflags=False)

        self.assertEqual(res['total'], 25600)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 68
        # Instead of >= 1280  (= 5 rows x 4 pol x 64 chan)
        self.assertEqual(res['flagged'], 2905)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_rflag_timebin_20(self):
        """ rflag, timeavg, timebin=20, propagate flags forth and back """

        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(timebin=20, ims=self.vis,
                                                  mode='rflag', extendflags=False)

        self.assertEqual(res['total'], 25600)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 756
        # Instead of >= 1280
        self.assertEqual(res['flagged'], 3586)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')

    def test_propagation_clip_timebin_100s(self):
        """ clip, timeavg, timebin=100, propagate flags forth and back """

        # Make clip flag something (if no flags are added, the flag cube is not written)
        res, apriori_flags, final_flags =\
            self.run_auto_flag_preavg_propagation(timebin=100, ims=self.vis,
                                                  clipminmax=[0.001, 0.1])

        self.assertEqual(res['total'], 25600)
        # Before CAS-12727, there is some 'loss' of flags. This would be: 1339
        self.assertEqual(res['flagged'], 1609)
        self.assertTrue(self.check_flags_preserved(apriori_flags, final_flags),
                        'Not all the flags set "before" are set "after"')


class test_forbid_avg_in_non_autoflagging_list(test_base):
    """
    CAS-12294: forbid the use of timeavg or chanavg in methods other than the
    auto-flagging methods (clip, tfcrop, rflag) when given inside the list of
    commands in list mode.
    """

    def setUp(self):
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def _run_method_with_avg(self, method, more_params=''):
        inplist = ["mode='{}' timeavg=True timebin='1s' {}".format(method, more_params)]

        # Sshould raise exception when trying to use chanavg in non-autoflagging mode
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        inplist = ["mode='{}' channelavg=True chanbin=4 {}".format(method, more_params)]

        # Should again raise exception when trying to use timeavg in non-autoflagging mode
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        # Nothing should have been flagged
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['total'], 4399104)
        self.assertEqual(res['flagged'], 0)

    def test_forbid_avg_list_manual(self):
        '''flagdata: timeavg=True should not be accepted in manual mode inside list'''

        inplist = ["mode='manual' spw='7' timeavg=True timebin='2s'"]

        # Sshould raise exception when trying to use chanavg in non-autoflagging mode
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        inplist = ["mode='manual' spw='9' channelavg=True chanbin=4"]

        # Should again raise exception when trying to use timeavg in non-autoflagging mode
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        # Nothing should have been flagged or unflagged
        res = flagdata(vis=self.vis, mode='summary', spw='7,9')
        self.assertEqual(res['total'], 549888)
        self.assertEqual(res['flagged'], 0)
        # This is what it would flag if clip+timeavg+manual was accepted:
        # self.assertEqual(res['spw']['7']['flagged'], 274944)
        # self.assertEqual(res['spw']['9']['flagged'], 0)
        # self.assertEqual(res['flagged'], 274944)

    def test_forbid_avg_list_quack(self):
        '''flagdata: timeavg=True and channelavg=True should not be accepted in quack
        mode inside list'''

        self._run_method_with_avg('quack')

    def test_forbid_avg_list_shadow(self):
        '''flagdata: timeavg=True and channelavg=True should not be accepted in shadow
        mode inside list'''

        self._run_method_with_avg('shadow')

    def test_forbid_avg_list_elevation(self):
        '''flagdata: timeavg=True and channelavg=True should not be accepted in elevation
        mode inside list'''

        self._run_method_with_avg('elevation')

    def test_forbid_avg_list_antint(self):
        '''flagdata: timeavg=True and channelavg=True should not be accepted in antint
        mode inside list'''

        self._run_method_with_avg('antint', ' antint_ref_antenna=ea01')

    def test_forbid_avg_list_unflag(self):
        '''flagdata: timeavg=True and channelavg=True should not be accepted in unflag
        mode inside list'''

        self._run_method_with_avg('unflag')

    def test_forbid_avg_list_summary(self):
        '''flagdata: timeavg=True and channelavg=True should not be accepted in summary
        mode inside list'''

        self._run_method_with_avg('summary')


class test_list_modes_forbidden_with_avg(test_base):
    """
    CAS-12294: forbid the use of timeavg or chanavg in auto-flagging methods when they
    are used in list mode together with other methods that are not auto-flagging methods
    (clip, tfcrop, rflag).

    For now we still allow lists with auto-methods (any or all) + timeavg + chanavg +
    + extendflags + antint.
    """

    def setUp(self):
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def test_test_forbid_timeavg_list(self):
        '''flagdata: timeavg=True should not be accepted in list mode, with +manual'''

        inplist = ["mode='manual' spw='7'",
                   "mode='clip' spw='9' timeavg=True timebin='2s' clipminmax=[0.0, 0.8]"]

        # CAS-12294: should raise exception when trying to use timeavg and forbidden modes
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        # Nothing should have been flagged or unflagged
        res = flagdata(vis=self.vis, mode='summary', spw='7,9')
        self.assertEqual(res['total'], 549888)
        self.assertEqual(res['flagged'], 0)
        # This is what it would flag if clip+timeavg+manual was accepted:
        # self.assertEqual(res['spw']['7']['flagged'], 274944)
        # self.assertEqual(res['spw']['9']['flagged'], 0)
        # self.assertEqual(res['flagged'], 274944)

    def test_forbid_timeavg_list_longer(self):
        '''flagdata: timeavg=True should not be accepted in list mode, with +manual'''

        inplist = ["mode='manual' spw='7'",
                   "mode='clip' spw='9' timeavg=True timebin='2s' clipminmax=[0.0, 0.8]",
                   "mode='clip' spw='8' clipzeros=True"]

        # CAS-12294: as above, should not be accepted
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        res = flagdata(vis=self.vis, mode='summary', spw='7,8,9')
        self.assertEqual(res['total'], 824832)
        self.assertEqual(res['flagged'], 0)
        # This is what it would flag if clip+timeavg+manual was accepted?
        # self.assertEqual(res['spw']['7']['flagged'], 274944)
        # self.assertEqual(res['spw']['8']['flagged'], 274944)
        # self.assertEqual(res['spw']['9']['flagged'], 0)
        # self.assertEqual(res['flagged'], 274944*2)

    def test_forbid_chanavg_list(self):
        '''flagdata: chanavg=True should not be accepted in list mode, with +manual'''

        inplist = ["mode='manual' spw='8'",
                   "mode='clip' spw='9' channelavg=True chanbin=2 clipminmax=[0.0, 0.8]"]

        # CAS-12294: as above, should not be accepted
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        res = flagdata(vis=self.vis, mode='summary', spw='7,8,9')
        self.assertEqual(res['total'], 824832)
        self.assertEqual(res['flagged'], 0)

    def test_forbid_chanavg_list_other_modes(self):
        '''flagdata: chanavg=True should not be accepted in list mode, with shadow,
        unflag, elevation, quack'''

        # CAS-12294: as above, should not be accepted. Try to apply several forbidden
        # methods in a row, and check at the end, to avoid running too many summaries
        inplist = ["mode='unflag'",
                   "mode='clip' spw='9' channelavg=True chanbin=2 clipminmax=[0.0, 0.1]"]
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        inplist = ["mode='shadow'",
                   "mode='clip' spw='9' channelavg=True chanbin=2 clipminmax=[0.0, 0.1]"]
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        inplist = ["mode='elevation' lowerlimit=89.0 upperlimit=89.5",
                   "mode='clip' spw='9' channelavg=True chanbin=2 clipminmax=[0.0, 0.1]"]
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        inplist = ["mode='clip' spw='9' channelavg=True chanbin=2 clipminmax=[0.0, 0.1]",
                   "mode='quack' quackmode='tail' quackinterval=1.0"]
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        inplist = ["mode='clip' spw='9' channelavg=True chanbin=2 clipminmax=[0.0, 0.1]",
                   "mode='quack' quackmode='end' quackinterval=1.0"]
        with self.assertRaises(RuntimeError):
            res = flagdata(vis=self.vis, mode='list', inpfile=inplist)

        # Nothing should have been flagged
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['total'], 4399104)
        self.assertEqual(res['flagged'], 0)

    def test_allow_all_auto_methods_timeavg_chanavg_extendflags_antint(self):
        """ Full list: all auto-methods, avg (time and chan), extendflags and antint """

        # Note that the only way to enable antint together with one auto-method is to use
        # the list mode
        inplist = ["mode='clip' clipminmax=[0.01, 1.] spw='10' channelavg=True chanbin=8",
                   "mode='tfcrop' timeavg=True timebin='2s'",
                   "mode='rflag' extendflags=True",
                   "mode='antint' antint_ref_antenna='ea01' minchanfrac=0.01"
        ]

        res = flagdata(vis=self.vis, flagbackup=False, mode='list', inpfile=inplist)
        self.assertEqual(res, {})
        # The return from listmode is not enough to know. Let's see if there are flags
        res = flagdata(vis=self.vis, mode='summary')
        self.assertEqual(res['total'], 4399104)
        # With bamboo setup (20211126), RHEL7: 254912, OSX: 266432
        self.assertGreaterEqual(res['flagged'], 254000)


@unittest.skipIf(True,
                 'These tests will open the flagging display GUI -> they are not meant to '
                 'run together with the usual automated verification tests of test_flagdata')
class test_auto_methods_display(test_base):
    """ Test display together with auto-flagging methods and additional methods that can
    be used together (extendflags and even antint). """

    def setUp(self):
        """ This MS has 1 field, 2 scans, 16 spws, with 64 channels each. 4 corr"""
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def test_display_clip_timeavg_chanavg(self):
        """ Display data with clip, enabling avg (time and chan)"""

        # Note flagdata with display='data' doesn't return anything (an empty dict)
        flagdata(vis=self.vis, flagbackup=False, mode='clip', clipminmax=[0.05,10.],
                 datacolumn='DATA', spw='10,11',
                 channelavg=True, chanbin=8, timeavg=True, timebin='4s',
                 display='data')

    def test_display_tfcrop_timeavg_chanavg_extendflags(self):
        """ Display data with tfcrop, enabling avg (time and chan), extendflags"""

        # SPWs picked to get not too uninteresting outputs (avoid all or almost all
        # flagged/unflagged)
        flagdata(vis=self.vis, flagbackup=False, mode='tfcrop',
                 datacolumn='DATA', spw='5,6',
                 channelavg=True, chanbin=4, timeavg=True, timebin='4s',
                 extendflags=True, display='data')

    def test_display_rflag_timeavg_chanavg_extendflags(self):
        """ Display data with tfcrop, enabling avg (time and chan), extendflags"""

        flagdata(vis=self.vis, flagbackup=False, mode='rflag',
                 datacolumn='DATA', spw='5,6',
                 channelavg=True, chanbin=4, timeavg=True, timebin='2s',
                 extendflags=True, display='data', action='calculate')

    def test_display_all_auto_timeavg_chanavg_extendflags_list_antint(self):
        """ Display with auto-methods (all), avg (time and chan), extendflags + antint """

        # Note that the only way to enable antint together with one auto-method is to use
        # the list mode
        inplist = ["mode='clip' clipminmax=[0.01, 1.] spw='10' timeavg=True timebin='2s' "
                   "channelavg=True chanbin=4",
                   "mode='tfcrop'",
                   "mode='rflag'",
                   "mode='antint' antint_ref_antenna='ea01' minchanfrac=0.01"
        ]

        flagdata(vis=self.vis, flagbackup=False, mode='list', inpfile=inplist,
                 display='data')

class test_summary(test_base):
    """ Test summary mode parameters """

    def setUp(self):
        """ This MS has 1 field, 2 scans, 16 spws, with 64 channels each and 4 corr"""
        self.setUp_data4tfcrop()

    @classmethod
    def tearDownClass(cls) -> None:
        os.system('rm -rf Four_ants_3C286.ms*')

    def test_summary_spwcorr(self):
        """ Flag manually spw and correlation and check spwcorr parameter of summary mode"""
        flagdata(vis=self.vis, mode='manual', spw='3', correlation='LL', flagbackup=False)
        res = flagdata(vis=self.vis, mode='summary', spw='3', spwcorr=True)
        self.assertEqual(res['spw:correlation']['3:LL']['flagged'],res['flagged'])

if __name__ == '__main__':
    unittest.main()
