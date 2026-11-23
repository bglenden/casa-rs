##########################################################################
# test_task_mstransform.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.manipulation.mstransform.html
#
##########################################################################
import shutil
import unittest
import os
import numpy
import math
import sys
import filecmp
import glob

from casatools import ctsys, ms, table, msmetadata, agentflagger
from casatasks import applycal, cvel, cvel2, flagcmd, flagdata, importasdm, listpartition, listobs, mstransform, setjy, split
#from casatasks import importasdm     ### tar files have been created to avoid the importasdm dependency

# Define the root for the data files
datapath = ctsys.resolve('unittest/mstransform/')

af_local = agentflagger()
msmd_local = msmetadata()
ms_local = ms()
tb_local = table()

from casatestutils import testhelper as th

def weighToSigma(weight):
    if weight > sys.float_info.min:
        return 1.0/math.sqrt(weight)
    else:
        return -1.0

def sigmaToWeight(sigma):
    if sigma > sys.float_info.min:
        return 1.0/math.pow(sigma,2)
    else:
        return 0.0


def check_eq(val, expval, tol=None):
    """Checks that val matches expval within tol."""
#    print(val)
    if type(val) == dict:
        for k in val:
            check_eq(val[k], expval[k], tol)
    else:
        try:
            if tol and hasattr(val, '__rsub__'):
                are_eq = abs(val - expval) < tol
            else:
                are_eq = val == expval
            if hasattr(are_eq, 'all'):
                are_eq = are_eq.all()
            if not are_eq:
                raise ValueError('!=')
        except ValueError:
            errmsg = "%r != %r" % (val, expval)
            if (len(errmsg) > 66): # 66 = 78 - len('ValueError: ')
                errmsg = "\n%r\n!=\n%r" % (val, expval)
            raise ValueError(errmsg)
        except Exception:
            print("Error comparing", val, "to", expval)
            raise

# Base class which defines setUp functions
# for importing different data sets
class test_base(unittest.TestCase):

    vis = None

    @classmethod
    def setUp_ngc5921(cls):
        # data set with spw=0, 63 channels in LSRK
        test_base.vis = "ngc5921.ms"
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_4ants(cls):
        # data set with spw=0~15, 64 channels each in TOPO
        test_base.vis = "Four_ants_3C286.ms"
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_jupiter(cls):
        # data col, spw=0,1 1 channel each, TOPO, field=0~12, 93 scans
        test_base.vis = 'jupiter6cm.demo-thinned.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_g19(cls):
        # data with spw=0~23 128 channel each in LSRK, field=0,1
        test_base.vis = 'g19_d2usb_targets_line-shortened-thinned.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_5076(cls):
        test_base.vis = 'CAS-5076.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_almasim(cls):
        test_base.vis = 'sim.alma.cycle0.compact.noisy.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_floatcol(cls):
        # 15 rows, 3 scans, 9 spw, mixed chans, XX,YY, FLOAT_DATA col
        test_base.vis = 'SDFloatColumn.ms'
        cls._setup_std_reusing_input_vis(cls.vis, datapath_sp=datapath)

    @classmethod
    def setUp_3c84(cls):
        # MS is as follows (scan=1):
        #  SpwID   #Chans   Corrs
        #   0      256      RR
        #   0      256      LL
        #   1      128      RR  LL
        #   2      64       RR  RL  LR  LL

        test_base.vis = '3c84scan1.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_5013(cls):

        test_base.vis = 'ALMA-data-mst-science-testing-CAS-5013-one-baseline-one-timestamp.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_4850(cls):

        test_base.vis = 'CAS-4850-30s-limit-ALMA.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_4983(cls):
        test_base.vis = 'CAS-4983.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_5172(cls):
        test_base.vis = 'CAS-5172-phase-center.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_sub_tables_evla(cls):

        test_base.vis = 'test-subtables-evla.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_sub_tables_alma(cls):

        test_base.vis = 'test-subtables-alma.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_titan(cls):
        test_base.vis = 'titan-one-baseline-one-timestamp.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_6733(cls):
        test_base.vis = 'CAS-6733.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_6941(cls):
        test_base.vis = 'CAS-6941.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_6951(cls):
        test_base.vis = 'CAS-6951.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_7841(cls):
        test_base.vis = 'CAS-7841.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_CAS_7259(cls):
        test_base.vis = 'n0337d03-CAS-7259.ms'
        cls._setup_std_reusing_input_vis(cls.vis)

    @classmethod
    def setUp_flags(cls):
        asdmname = 'test_uid___A002_X997a62_X8c-short' # Flag.xml is modified
        test_base.vis = asdmname+'.ms'
        cls.flagfile = asdmname+'_cmd.txt'

        os.system('ln -sf {0}'.format(os.path.join(datapath, asdmname)))
        importasdm(asdmname, convert_ephem2geo=False, flagbackup=False, process_syspower=False, lazy=True,
                   scans='1', savecmds=True, overwrite=True)

    def createMMS(self, msfile, axis='auto',scans='',spws=''):
        '''Create MMSs for tests with input MMS'''
        prefix = msfile.rstrip('.ms')
        if not os.path.exists(msfile):
            os.system('cp -RL '+ os.path.join(datapath, msfile) +' '+ msfile)

        # Create an MMS for the tests
        self.testmms = prefix + ".test.mms"

        if os.path.exists(self.testmms):
            os.system("rm -rf " + self.testmms)

        print("................. Creating test MMS ..................")
        mstransform(vis=msfile, outputvis=self.testmms, datacolumn='data',
                    createmms=True,separationaxis=axis, scan=scans, spw=spws)

    @staticmethod
    def copyfile(filename):

        if os.path.exists(filename):
           os.system('rm -rf '+ filename)

        os.system('cp -RL {0} {1}'.format(os.path.join(datapath, filename), filename))

    @staticmethod
    def removeInputMS(self):
        os.system('rm -rf '+ self.vis)

    @staticmethod
    def _setup_std_reusing_input_vis(vis, datapath_sp=None):
        test_base._copy_input_ms_if_needed(vis, datapath_sp=datapath_sp)

    @staticmethod
    def _copy_input_ms_if_needed(vis, reuse_input=True, datapath_sp=None):
        # special datapath
        if datapath_sp:
            copy_path = datapath_sp
        else:
            copy_path = datapath  # global

        found = os.path.exists(vis)
        if found and not reuse_input:
           test_base.system('rm -rf {0}'.format(vis))
        if not found or not reuse_input:
            os.system('cp -RL ' + os.path.join(copy_path, vis) + ' ' + vis)


class test_base_compare(test_base):

    def setUp(self):

        self.outvis = ''
        self.refvis = ''
        self.outvis_sorted = ''
        self.refvis_sorted = ''

        self.subtables=['/ANTENNA','/DATA_DESCRIPTION','/FEED','/FIELD','/FLAG_CMD',
                        '/POINTING','/POLARIZATION','/PROCESSOR','/STATE']
        self.sortorder=['OBSERVATION_ID','ARRAY_ID','SCAN_NUMBER','FIELD_ID','DATA_DESC_ID','ANTENNA1','ANTENNA2','TIME']
        self.excludecols=['WEIGHT_SPECTRUM','SIGMA_SPECTRUM','SIGMA','FLAG_CATEGORY']

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)
        os.system('rm -rf '+ self.outvis_sorted)
        os.system('rm -rf '+ self.refvis_sorted)

    def sort(self):
        ms_local.open(self.outvis)
        ms_local.sort(self.outvis_sorted,self.sortorder)
        ms_local.done()

        ms_local.open(self.refvis)
        ms_local.sort(self.refvis_sorted,self.sortorder)
        ms_local.done()

    def generate_tolerance_map(self):

        # Get column names
        tb_local.open(self.refvis)
        self.columns = tb_local.colnames()
        tb_local.close()

        # Define default tolerance
        self.mode={}
        self.tolerance={}
        for col in self.columns:
            self.mode[col] = "absolute"
            self.tolerance[col] = 1E-6

    def check_history_growth(self):
        """
        Check that the HISTORY subtable of the output MS has been updated (to prevent issues
        like CAS-14575).

        This is for now a very simple check (checks the output MS/HISTORY has >= rows than the number
        of rows in the input MS + the lines added by an mstransform run)
        """
        try:
            tb_local.open(self.outvis+'/HISTORY')
            nrows_after = tb_local.nrows()
        finally:
            tb_local.done()

        try:
            tb_local.open(self.vis+'/HISTORY')
            nrows_before = tb_local.nrows()
        finally:
            tb_local.done()

        # Too simple assumption/check (just enough to catch issues like CAS-14575)
        ROWS_PER_MSTRANSFORM_HISTORY_ENTRY = 57
        self.assertGreaterEqual(nrows_after, nrows_before + ROWS_PER_MSTRANSFORM_HISTORY_ENTRY)

    def compare_subtables(self):
        for subtable in self.subtables:
            self.assertTrue(th.compTables(self.outvis_sorted+subtable,self.refvis_sorted+subtable, [],0.000001,"absolute"))

        # Special case for SOURCE which contains many un-defined columns
        # CAS-5172 (jagonzal): Commenting this out because cvel and mstransform produce different SOURCE subtable
        # For some reason cvel removes sources which are not present in any row of the main table even if the
        # user does not specify field selection
        #self.assertTrue(th.compTables(self.outvis_sorted+'/SOURCE',self.refvis_sorted+'/SOURCE',
        #                              ['POSITION','TRANSITION','REST_FREQUENCY','SYSVEL','SOURCE_MODEL'],0.000001,"absolute"))

        # Special case for OBSERVATION which contains many un-defined columns
        self.assertTrue(th.compTables(self.outvis_sorted+'/OBSERVATION',self.refvis_sorted+'/OBSERVATION',
                                      ['LOG','SCHEDULE'],0.000001,"absolute"))

    def compare_main_table_columns(self,startrow = 0, nrow = -1, rowincr = 1):
        for col in self.columns:
            if col not in self.excludecols:
                    tmpcolumn = self.columns[:]
                    tmpcolumn.remove(col)
                    self.assertTrue(th.compTables(self.refvis_sorted,self.outvis_sorted,tmpcolumn,self.tolerance[col],self.mode[col],startrow,nrow,rowincr))

    def post_process(self,startrow = 0, nrow = -1, rowincr = 1, check_history=True):

        # Sort the output MSs so that they can be compared
        self.sort()

        # Compare results for subtables
        self.compare_subtables()

        # Compare columns from main table
        self.compare_main_table_columns(startrow,nrow,rowincr)

        if check_history:
            self.check_history_growth()


class test_Combspw1(test_base):
    ''' Tests for combinespws parameter'''

    def setUp(self):
        self.setUp_4ants()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf inpmms*.*ms combcvel*ms list.obs')

    def test_combspw1_1(self):
        '''mstransform: Combine four spws into one'''

        self.outputms = "combspw11.ms"
        mstransform(self.vis, self.outputms, combinespws=True, spw='0~3')
        self.assertTrue(os.path.exists(self.outputms))

        ret = th.verifyMS(self.outputms, 1, 256, 0)
        self.assertTrue(ret[0],ret[1])

        listobs(self.outputms, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

    def test_combspw1_2(self):
        '''mstransform: Combine some channels of two spws'''

        self.outputms = "combspw12.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True, spw='0:60~63,1:60~63')
        self.assertTrue(os.path.exists(self.outputms))

        # The spws contain gaps, therefore the number of channels is bigger
        ret = th.verifyMS(self.outputms, 1, 68, 0)
        self.assertTrue(ret[0],ret[1])

        # Compare with cvel results
        cvel(vis=self.vis, outputvis='combcvel12.ms', spw='0:60~63,1:60~63')
        ret = th.verifyMS('combcvel12.ms', 1, 68, 0)
        self.assertTrue(ret[0],ret[1])

    def test_combspw1_5(self):
        '''mstransform: Combine four spws into one'''

        self.outputms = "combspw15.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True, spw='2,5,8')
        self.assertTrue(os.path.exists(self.outputms))

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 1, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')

        # DDI subtable should have 4 rows with the proper indices
        tb_local.open(self.outputms + '/DATA_DESCRIPTION')
        spwCol = tb_local.getcol('SPECTRAL_WINDOW_ID')
        tb_local.close()
        nspw = spwCol.size
        check_eq(nspw, 1)
        check_eq(spwCol[0], 0)

    def test_combspw1_6(self):
        '''mstransform: Combine some channels of two spws'''

        self.outputms = "combspw16.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True, spw='0~1:60~63')
        self.assertTrue(os.path.exists(self.outputms))

        # The spws contain gaps, therefore the number of channels is bigger
        ret = th.verifyMS(self.outputms, 1, 68, 0)
        self.assertTrue(ret[0],ret[1])

        # Compare with cvel results
        cvel(vis=self.vis, outputvis='combcvel12.ms', spw='0~1:60~63')
        ret = th.verifyMS('combcvel12.ms', 1, 68, 0)
        self.assertTrue(ret[0],ret[1])

    def test_combspw_overlap(self):
        '''mstransform: Combine some channels of two overlapping spws'''

        self.outputms = "combspw12.ms"
        self.setUp_CAS_7259()
        cvel2(vis=self.vis,
              outputvis='cvel2overlap.ms',field="NGC0337",spw="",
              antenna="",timerange="",scan="",array="",datacolumn='corrected',
              mode="velocity",nchan=107,start="1360.00km/s",width="5.2km/s",interpolation="linear",
              phasecenter="",restfreq="1420405752.0Hz",outframe="BARY",veltype="radio",hanning=False)

        self.assertTrue(os.path.exists('cvel2overlap.ms'))
        # The spws contain gaps, therefore the number of channels is bigger
        ret = th.verifyMS('cvel2overlap.ms', 1, 107, 0)
        self.assertTrue(ret[0],ret[1])
        split(vis=self.vis,outputvis="n0337d03.src.ms.tmp",datacolumn="corrected",field="",spw="0,1",
              width=1,antenna="",timebin="0s",timerange="",scan="",
              intent="",array="",uvrange="",correlation="",observation="",
              combine="",keepflags=False,keepmms=False)
        cvel(vis="n0337d03.src.ms.tmp",outputvis='cveloverlap.ms',passall=False,field="NGC0337",spw="",
             selectdata=True,antenna="",timerange="",scan="",array="",
             mode="velocity",nchan=107,start="1360.00km/s",width="5.2km/s",interpolation="linear",
             phasecenter="",restfreq="1420405752.0Hz",outframe="BARY",veltype="radio",hanning=False)

        ms_local.open('cveloverlap.ms')
        ms_local.sort('cveloverlap-sorted.ms',['OBSERVATION_ID','ARRAY_ID','SCAN_NUMBER','FIELD_ID','DATA_DESC_ID','ANTENNA1','ANTENNA2','TIME'])
        ms_local.done()

        ms_local.open('cvel2overlap.ms')
        ms_local.sort('cvel2overlap-sorted.ms',['OBSERVATION_ID','ARRAY_ID','SCAN_NUMBER','FIELD_ID','DATA_DESC_ID','ANTENNA1','ANTENNA2','TIME'])
        ms_local.done()

        # TODO: there are differences in values of masked entries
        tb_local.open('cvel2overlap-sorted.ms', nomodify=False)
        nd = tb_local.getcol('DATA')
        nf = tb_local.getcol('FLAG')
        nd[nf] = 0.
        tb_local.putcol('DATA', nd)
        tb_local.done()
        tb_local.open('cveloverlap-sorted.ms', nomodify=False)
        nd = tb_local.getcol('DATA')
        nf = tb_local.getcol('FLAG')
        nd[nf] = 0.
        tb_local.putcol('DATA', nd)
        tb_local.done()

        # TODO: there are differences in weights and sigma column of masked channels
        self.assertTrue(th.compTables('cveloverlap-sorted.ms','cvel2overlap-sorted.ms',
                                      ['WEIGHT_SPECTRUM', 'FLAG_CATEGORY', 'WEIGHT', 'SIGMA'],
                                      tolerance=0.0,mode="absolute"))
        os.system('rm -rf cveloverlap-sorted.ms cveloverlap.ms n0337d03.src.ms.tmp')
        os.system('rm -rf cvel2overlap-sorted.ms cvel2overlap.ms')


class test_combinespws_diff_channels(test_base):
    '''Tests for combinespws option when the spw's have different numbers of channels'''

    def setUp(self):
        self.setUp_CAS_4983()

    def test_combinespws_not_supported_all(self):
        '''mstransform: combinespws does not currently work when the spw's have
        different numbers of channels. An error should be produced.'''
        self.outputms = "combinespws_fail_all_spws_test.ms"
        with self.assertRaises(RuntimeError):
            mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True)
        self.assertFalse(os.path.exists(self.outputms))

    def test_combinespws_not_supported_n23(self):
        '''mstransform: combinespws does not currently work when the spw's have
        different numbers of channels. An error should be produced. spw 2 has 128
        channels but the other spw's have 3840 channels.'''
        self.outputms = "combinespws_fail_bad_spws_test.ms"
        with self.assertRaises(RuntimeError):
            mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True, spw='2,3')
        self.assertFalse(os.path.exists(self.outputms))

    def test_combinespws_not_supported_n321(self):
        '''mstransform: combinespws does not currently work when the spw's have
        different numbers of channels. An error should be produced.'''
        self.outputms = "combinespws_fail_bad_spws_test.ms"
        with self.assertRaises(RuntimeError):
            mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True,
                        spw='3,2,1')
        self.assertFalse(os.path.exists(self.outputms))

    def test_combinespws_not_supported_n0123(self):
        '''mstransform: combinespws does not currently work when the spw's have
        different numbers of channels. An error should be produced. All spw's are
        selected here.'''
        self.outputms = "combinespws_fail_bad_spws_test.ms"
        with self.assertRaises(RuntimeError):
            mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True,
                        spw='0,1,2,3')
        self.assertFalse(os.path.exists(self.outputms))

    def test_combinespws_ok(self):
        '''No error should be produced because the spw's selected have the
        same number of channels, even though other spw's have different
        numbers of channels.'''
        self.outputms = "combinespws_fail_spws_ok_test.ms"
        res = mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data',
                          combinespws=True, spw='1,0')
        self.assertTrue(os.path.exists(self.outputms))


class test_regridms_four_ants(test_base):
    '''Tests for regridms parameter using Four_ants_3C286.ms'''

    def setUp(self):
        self.setUp_4ants()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputvis)
        os.system('rm -rf testmms*ms list.obs')

    def test_regrid1_defaults(self):
        '''mstransform: Default of regridms parameters'''

        self.outputvis = "reg11.ms"
        mstransform(vis=self.vis, outputvis=self.outputvis, regridms=True)
        self.assertTrue(os.path.exists(self.outputvis))

        # The regriding should be the same as the input
        for i in range(16):
            ret = th.verifyMS(self.outputvis, 16, 64, i)
            self.assertTrue(ret[0],ret[1])

        listobs(self.outputvis)
        listobs(self.outputvis, listfile='list.obs', overwrite=True)
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

        # Check the shape of WEIGHT and SIGMA
        inp_ws = th.getColShape(self.vis,'WEIGHT')
        inp_ss = th.getColShape(self.vis,'SIGMA')
        out_ws = th.getColShape(self.outputvis,'WEIGHT')
        out_ss = th.getColShape(self.outputvis,'SIGMA')
        self.assertListEqual(inp_ws, out_ws, 'WEIGHT shape differ in input and output')
        self.assertListEqual(inp_ss, out_ss, 'SIGMA shape differ in input and output')

    def test_regrid1_defaults_spw_sel(self):
        '''mstransform: Default regridms with spw selection'''

        self.outputvis = "reg12.ms"
        mstransform(vis=self.vis, outputvis=self.outputvis, regridms=True, spw='1,3,5,7')
        self.assertTrue(os.path.exists(self.outputvis))

        # The output should be the same as the input
        for i in range(4):
            ret = th.verifyMS(self.outputvis, 4, 64, i)
            self.assertTrue(ret[0],ret[1])

        listobs(self.outputvis)

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputvis+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 4, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r3'][0], 2,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r4'][0], 3,'Error re-indexing DATA_DESCRIPTION table')


class test_regridms_jupiter(test_base):
    '''Tests for regridms parameter using Jupiter MS'''

    def setUp(self):
        self.setUp_jupiter()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputvis)
        os.system('rm -rf cvel31*.*ms')

    def test_regrid3_1(self):
        '''mstransform 12: Check that output columns are the same when using mstransform'''
        self.outputvis = 'reg31.ms'

        mstransform(vis=self.vis, outputvis=self.outputvis, field='6',
                    combinespws=True, regridms=True, datacolumn='data',
                    mode='frequency', nchan=2, start='4.8101 GHz', width='50 MHz',
                    outframe='')

        ret = th.verifyMS(self.outputvis, 1, 2, 0)
        self.assertTrue(ret[0],ret[1])

        # Now run with cvel to compare the columns, CAS-4866
        outputvis = 'cvel31.ms'
        cvel(vis=self.vis, outputvis=outputvis, field='6',
            passall=False,mode='frequency',nchan=2,start='4.8101 GHz',
            width='50 MHz',outframe='')

        # Sort the output MSs so that they can be compared

        ms_local.open('cvel31.ms')
        ms_local.sort('cvel31-sorted.ms',['OBSERVATION_ID','ARRAY_ID','SCAN_NUMBER','FIELD_ID','DATA_DESC_ID','ANTENNA1','ANTENNA2','TIME'])
        ms_local.done()

        ms_local.open('reg31.ms')
        ms_local.sort('reg31-sorted.ms',['OBSERVATION_ID','ARRAY_ID','SCAN_NUMBER','FIELD_ID','DATA_DESC_ID','ANTENNA1','ANTENNA2','TIME'])
        ms_local.done()

        self.assertTrue(th.compTables('cvel31-sorted.ms','reg31-sorted.ms', 'FLAG_CATEGORY',0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/ANTENNA','reg31-sorted.ms/ANTENNA', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/DATA_DESCRIPTION','reg31-sorted.ms/DATA_DESCRIPTION', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/FEED','reg31-sorted.ms/FEED', ['SPECTRAL_WINDOW_ID'],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/FIELD','reg31-sorted.ms/FIELD', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/FLAG_CMD','reg31-sorted.ms/FLAG_CMD', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/OBSERVATION','reg31-sorted.ms/OBSERVATION', ['LOG','SCHEDULE'],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/POINTING','reg31-sorted.ms/POINTING', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/POLARIZATION','reg31-sorted.ms/POLARIZATION', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/PROCESSOR','reg31-sorted.ms/PROCESSOR', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/SOURCE','reg31-sorted.ms/SOURCE', [],0.000001,"absolute"))
        self.assertTrue(th.compTables('cvel31-sorted.ms/STATE','reg31-sorted.ms/STATE', [],0.000001,"absolute"))

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputvis+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 1, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')

    def test_regrid3_2(self):
        '''mstransform: Combine spw and regrid MS with two spws, select one field and 2 spws'''
        # cvel: test8
        self.outputvis = "reg32a.ms"
        mstransform(vis=self.vis, outputvis=self.outputvis, combinespws=True, regridms=True,
                    spw='0,1',field = '11',nchan=1, width=2, datacolumn='DATA')
        self.assertTrue(os.path.exists(self.outputvis))

        ret = th.verifyMS(self.outputvis, 1, 1, 0)
        self.assertTrue(ret[0],ret[1])

        # Now, do only the regridding and do not combine spws
        outputms = "reg32b.ms"
        mstransform(vis=self.vis, outputvis=outputms, combinespws=False, regridms=True,
                    spw='0,1',field = '11',nchan=1, width=2, datacolumn='DATA')
        self.assertTrue(os.path.exists(outputms))

        ret = th.verifyMS(outputms, 2, 1, 0)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(outputms, 2, 1, 1)
        self.assertTrue(ret[0],ret[1])


class test_regridms_negative_width(test_base):
    """
    Test regridding when the parameter width takes a negative value, in different
    regridding modes.

    I was tempted to put these test cases inside test_regridms_jupiter
    and/or test_regridms_four_ants, but these two operate on MSs that are unnecessarily
    big and have columns (model, corrected) that are not needed for this type of test.
    The dataset used here has only one data row, which I hope removes the need for ordering
    before comparing. Still, I think we should find or make a suitable but smaller MS.
    """

    @classmethod
    def setUpClass(cls):
        # 1 spw, 1 data row, 3840 channels
        cls.setUp_titan()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        pass

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputvis
        os.system('rm -rf ' + self.outputvis)

    def _check_chan_freqs_widths(self, freqs, widths, exp_nchan, exp_first_freq,
                                 exp_last_freq, exp_width):

        self.assertEqual(len(freqs), exp_nchan)
        self.assertEqual(len(widths), exp_nchan)
        self.assertTrue(numpy.isclose(freqs[0], exp_first_freq, rtol=1e-5, atol=1e-8))
        self.assertTrue(numpy.isclose(freqs[-1], exp_last_freq,  rtol=1e-5, atol=1e-8))
        self.assertTrue(numpy.allclose(numpy.ediff1d(freqs), exp_width, rtol=1e-2))
        self.assertTrue(numpy.allclose(widths, exp_width, rtol=1e-3))

    def test_regrid_channel_neg_width(self):
        '''mstransform: regridding in channel mode with negative width'''

        self.outputvis = 'regrid_chan_neg_width.ms'
        nchan = 10
        mstransform(vis=self.vis, outputvis=self.outputvis, datacolumn='data',
                    regridms=True, outframe='BARY',
                    mode='channel', nchan=nchan, start=100, width=-10)

        chan_freqs, chan_widths = th.get_channel_freqs_widths(self.outputvis, 0)
        self._check_chan_freqs_widths(chan_freqs, chan_widths, nchan,
                                      3.543540545236911e+11, 3.5435954715143951e+11,
                                      610291.972)

    def test_regrid_channel_b_neg_width(self):
        '''mstransform: regridding in channel_b mode with negative width'''

        self.outputvis = 'regrid_chan_b_neg_width.ms'
        nchan = 8
        mstransform(vis=self.vis, outputvis=self.outputvis, datacolumn='data',
                    regridms=True, outframe='BARY',
                    mode='channel_b', nchan=nchan, start=100, width=-10)

        chan_freqs, chan_widths = th.get_channel_freqs_widths(self.outputvis, 0)
        self._check_chan_freqs_widths(chan_freqs, chan_widths, nchan,
                                      3.5435521407843884e+11, 3.5435948612224243e+11,
                                      610291.9720)

    def test_regrid_velocity_neg_width(self):
        '''mstransform: regridding in velocity mode with negative width'''

        self.outputvis = 'regrid_vel_neg_width.ms'

        vis_freqs, _vis_widths = th.get_channel_freqs_widths(self.vis, 0)
        restf = vis_freqs[0]

        nchan = 10
        mstransform(vis=self.vis, outputvis=self.outputvis, datacolumn='data',
                    regridms=True, outframe='BARY',
                    mode='velocity', veltype='RADIO', restfreq='{0:.0f}Hz'.format(restf),
                    nchan=nchan, start='25km/s', width='-1km/s')

        chan_freqs, chan_widths = th.get_channel_freqs_widths(self.outputvis, 0)
        self._check_chan_freqs_widths(chan_freqs, chan_widths, nchan,
                                      3.5435876611326282e+11, 3.543694051229682e+11,
                                      1.182112189e+06)

    def test_regrid_frequency_neg_width(self):
        '''mstransform: regridding in frequency mode with negative width'''

        self.outputvis = 'regrid_freq_neg_width.ms'

        nchan = 12
        mstransform(vis=self.vis, outputvis=self.outputvis, datacolumn='data',
                    regridms=True, outframe='LSRK',
                    mode='frequency', nchan=nchan, start='354.42GHz', width='-0.3GHz')

        chan_freqs, chan_widths = th.get_channel_freqs_widths(self.outputvis, 0)
        self._check_chan_freqs_widths(chan_freqs, chan_widths, nchan,
                                      3.5112e+11, 3.5442e+11, 3e+8)


class test_regridms_interpolation_only(test_base):
    '''Look into the DATA and WEIGHT produced by regridding, using the different
    interpolation methods available, when not combining them with channel average
    or any other transformations.'''

    @classmethod
    def setUpClass(cls):
        # Small MS with two data rows (two SPWs, one row per SPW).
        cls.vis = 'combine-1-timestamp-2-SPW-with-WEIGHT_SPECTRUM-Same-Exposure.ms'
        cls.out_nchan = 10
        cls.copyfile(cls.vis)

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outvis
        os.system('rm -rf '+ self.outvis)

    def check_output_values(self, vis, eq_pattern, nchan=10, eq_epsilon=1e-5):
        ''' Checks DATA, WEIGHT, and WEIGHT_SPECTRUM '''
        tb_local.open(vis)
        weight_spectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        data = tb_local.getcol('DATA')
        weights = tb_local.getcol('WEIGHT')
        tb_local.close()

        # for 'combine-1-timestamp-2-SPW-with-WEIGHT_SPECTRUM-Same-Exposure.ms'
        # shape is (1, 10, 2) - 1 pol x 10 channels x 2 rows
        out_nchan = data.shape[1]
        check_eq(out_nchan, nchan)
        eq_epsilon = 1e-5
        check_eq(weights[:,0], 59.026817, eq_epsilon)
        check_eq(weights[:,1], 31.586397, eq_epsilon)
        check_eq(weight_spectrum[:,:,0], weights[0,0], eq_epsilon)
        check_eq(weight_spectrum[:,:,1], weights[0,1], eq_epsilon)
        for pat in eq_pattern:
            row = pat[0]
            chan = pat[1]
            real = pat[2]
            imag = pat[3]
            check_eq(data[0][chan][row].real, real, eq_epsilon)
            check_eq(data[0][chan][row].imag, imag, eq_epsilon)

    def run_mstransform_simply_regrid(self, vis, outvis, interpolation, nchan=10):
        mstransform(vis=vis, outputvis=outvis, datacolumn="DATA",
                    regridms=True, mode="frequency", nchan=nchan,
                    start="2.20804+10Hz", width="2.5e+05Hz",
                    phasecenter="J2000 12h01m53.13s -18d53m09.8s",
                    interpolation=interpolation)

    def test_simple_regrid_nearest(self):
        ''' mstransform: regrid, nothing else. interpolation nearest, check output values'''
        self.outvis = 'test_simple_regrid_nearest.ms'
        self.run_mstransform_simply_regrid(self.vis, self.outvis, 'nearest')

        eq_pattern = [[0, 0, .0, .0],
                      [0, 1, 0.18098925, 0.35214117],
                      [1, 5, 0.42104605, 0.26617193],
                      [1, self.out_nchan-1, 0.02630020, 0.19990821]]
        self.check_output_values(self.outvis, eq_pattern, self.out_nchan)

    def test_simple_regrid_linear(self):
        ''' mstransform: regrid, nothing else. interpolation linear, check output values'''
        self.outvis = 'test_simple_regrid_linear.ms'
        self.run_mstransform_simply_regrid(self.vis, self.outvis, 'linear')

        eq_pattern = [[0, 0, 0.02533850, 0.04929977],
                      [0, 1, 0.14533380, 0.47466096],
                      [1, 5, 0.36502194, 0.45022112],
                      [1, self.out_nchan-1, 0.12325509, 0.05476397]]
        self.check_output_values(self.outvis, eq_pattern, self.out_nchan)

    def test_simple_regrid_cubic(self):
        ''' mstransform: regrid, nothing else. interpolation cubic, check output values'''
        self.outvis = 'test_simple_regrid_cubic.ms'
        self.run_mstransform_simply_regrid(self.vis, self.outvis, interpolation='cubic')

        eq_pattern = [[0, 0, 0.05564772, 0.03979797],
                      [0, 1, 0.15562536, 0.50231683],
                      [1, 5, 0.39988747, 0.53319526 ],
                      [1, self.out_nchan-1, 0.15120457, 0.07539418]]
        self.check_output_values(self.outvis, eq_pattern, self.out_nchan)

    def test_simple_regrid_spline(self):
        ''' mstransform: regrid, nothing else. interpolation cubic, check output values'''
        self.outvis = 'test_simple_regrid_spline.ms'
        self.run_mstransform_simply_regrid(self.vis, self.outvis, interpolation='spline')

        eq_pattern = [[0, 0, 0.03446201, 0.05137327],
                      [0, 1, 0.15356585, 0.52084935],
                      [1, 5, 0.40553212, 0.55733341 ],
                      [1, self.out_nchan-1, 0.17665164, 0.05236539]]
        self.check_output_values(self.outvis, eq_pattern, self.out_nchan)

    def test_simple_regrid_fftshift(self):
        ''' mstransform: regrid, nothing else. interpolation fftshift, check output values'''
        self.outvis = 'test_simple_regrid_fftshift.ms'
        self.run_mstransform_simply_regrid(self.vis, self.outvis, interpolation='fftshift')

        eq_pattern = [[0, 0, 0.02533850, 0.04929977],
                      [0, 1, 0.14533380, 0.47466096],
                      [1, 5, 0.36502194, 0.45022112],
                      [1, self.out_nchan-1, 0.12325509, 0.05476397]]
        self.check_output_values(self.outvis, eq_pattern, self.out_nchan)


class test_regridms_single_spw(test_base_compare):
    '''Tests for regridms w/o combining SPWS'''

    def setUp(self):
        super(test_regridms_single_spw,self).setUp()
        self.setUp_CAS_5013()
        self.outvis = 'test_regridms_single_spw_mst.ms'
        self.refvis = 'test_regridms_single_spw_cvel.ms'
        self.outvis_sorted = 'test_regridms_single_spw_mst_sorted.ms'
        self.refvis_sorted = 'test_regridms_single_spw_cvel_sorted.ms'
        os.system('rm -rf test_regridms_single_sp*')

    def tearDown(self):
        super(test_regridms_single_spw,self).tearDown()

    def test_regrid_only_LSRK(self):
        '''mstransform: Change ref. frame to LSRK'''

        mstransform(vis=self.vis,outputvis=self.outvis,regridms=True,datacolumn='ALL',
                    field='Vy_CMa',spw='3',mode='frequency',nchan=3830,start='310427.353MHz',width='-244.149kHz',outframe='lsrk')
        cvel(vis=self.vis,outputvis=self.refvis,
             field='Vy_CMa',spw='3',mode='frequency',nchan=3830,start='310427.353MHz',width='-244.149kHz',outframe='lsrk')

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-3

        self.post_process()

class test_regridms_multiple_spws(test_base_compare):
    '''Tests for regridms combining SPWS'''

    def setUp(self):
        super(test_regridms_multiple_spws,self).setUp()
        self.setUp_CAS_5172()
        self.outvis = 'test_regridms_multiple_spw_mst.ms'
        self.refvis = 'test_regridms_multiple_spw_cvel.ms'
        self.outvis_sorted = 'test_regridms_multiple_spw_mst_sorted.ms'
        self.refvis_sorted = 'test_regridms_multiple_spw_cvel_sorted.ms'
        os.system('rm -rf test_regridms_multiple_spw*')

    def tearDown(self):
        super(test_regridms_multiple_spws,self).tearDown()

    @unittest.skip('Skip, cvel produces an exception since release 4.7.2 as per CAS-9798')
    def test_combine_regrid_fftshift(self):
        '''mstransform: Combine 2 SPWs and change ref. frame to LSRK using fftshift'''

        cvel(vis = self.vis, outputvis = self.refvis ,mode = 'velocity',nchan = 10,start = '-50km/s',width = '5km/s',
             interpolation = 'fftshift',restfreq = '36.39232GHz',outframe = 'LSRK',veltype = 'radio', phasecenter="2")

        mstransform(vis = self.vis, outputvis = self.outvis, datacolumn='all',combinespws = True, regridms = True,
                    mode = 'velocity', nchan = 10, start = '-50km/s', width = '5km/s', interpolation = 'fftshift',
                    restfreq = '36.39232GHz', outframe = 'LSRK', veltype = 'radio')

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 50

        # Exlude FEED from the list of sub-tables to compare because cvel does not remove duplicates
        self.subtables=['/ANTENNA','/DATA_DESCRIPTION','/FIELD','/FLAG_CMD',
                        '/POINTING','/POLARIZATION','/PROCESSOR','/STATE']

        self.post_process()

    def test_combine_noregrid_fftshift(self):
        '''mstransform: Combine 2 SPWs and change ref. frame to LSRK using fftshift'''

        mstransform(vis=self.vis, outputvis=self.outvis, datacolumn='all',
                    combinespws=True, regridms=True, mode='velocity', nchan=10,
                    start='-50km/s', width='5km/s', interpolation='fftshift',
                    restfreq='36.39232GHz', outframe='LSRK', veltype='radio')

        self.assertTrue(os.path.isdir(self.outvis))


class test_regridms_spw_with_different_number_of_channels(test_base):
    '''Tests for regridms w/o combining SPWS'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_CAS_4983()
        cls.outvis = 'test_regridms_spw_with_different_number_of_channels.ms'

    @classmethod
    def tearDownClass(self):
        os.system('rm -rf '+ self.vis)

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)

    def test_regridms_spw_with_different_number_of_channels_separately(self):
        '''mstransform: Regrid SPWs separately, applying pre-channel averaging to only some of them'''

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='data',field='J0102-7546',regridms=True,
                    mode='frequency',width='29297.28kHz',outframe='lsrk',veltype='radio')

        # DDI subtable should have 4 rows with the proper indices
        tb_local.open(self.outvis + '/SPECTRAL_WINDOW')
        numChan = tb_local.getcol('NUM_CHAN')
        tb_local.close()
        check_eq(numChan[0], 32)
        check_eq(numChan[1], 2)
        check_eq(numChan[2], 68)
        check_eq(numChan[3], 2)


class test_Hanning_with_g19(test_base):
    '''Test for hanning transformation - tests that use dataset g19'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_g19()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outputms = ''

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputms
        os.system('rm -rf '+ self.outputms)

    def test_hanning1(self):
        '''mstransform: Apply Hanning smoothing in MS with 24 spws. Do not combine spws.'''

        self.outputms = "hann1.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, combinespws=False, hanning=True,
                    datacolumn='data')

        self.assertTrue(os.path.exists(self.outputms))
        ret = th.verifyMS(self.outputms, 24, 128, 0)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 2)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 15)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 18)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 23)
        self.assertTrue(ret[0],ret[1])

    def test_hanning1_datacolumn_uppercase(self):
        '''mstransform: Apply Hanning smoothing in MS with 24 spws. Do not combine spws.'''

        self.outputms = "hann1.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, combinespws=False, hanning=True,
                    datacolumn='DATA')

        self.assertTrue(os.path.exists(self.outputms))
        ret = th.verifyMS(self.outputms, 24, 128, 0)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 2)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 15)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 18)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 24, 128, 23)
        self.assertTrue(ret[0],ret[1])

    def test_hanning2(self):
        '''mstransform: Apply Hanning smoothing and combine spw=1,2,3.'''

        self.outputms = "hann2.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, combinespws=True, hanning=True,
                    spw='1,2,3', datacolumn='data')

        self.assertTrue(os.path.exists(self.outputms))
        ret = th.verifyMS(self.outputms, 1, 1448, 0)
        self.assertTrue(ret[0],ret[1])

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 1, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')


class test_Hanning_with_ngc5921(test_base):
    '''Test for hanning transformation - tests that use dataset ngc5921'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_ngc5921()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outputms = ''

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputms
        os.system('rm -rf '+ self.outputms)

    def test_hanning3(self):
        '''mstransform: Hanning theoretical and calculated values should be the same'''
        # hanning: test4
        self.outputms = "hann3.ms"

        # The hanningsmooth task flags the first and last channels. Check it!
        # Before running the task
        flag_col = th.getVarCol(self.vis, 'FLAG')
        self.assertTrue(flag_col['r1'][0][0] == [False])
        self.assertTrue(flag_col['r1'][0][1] == [False])
        self.assertTrue(flag_col['r1'][0][61] == [False])
        self.assertTrue(flag_col['r1'][0][62] == [False])

        # Get the DATA column before the transformation
        data_col = th.getVarCol(self.vis, 'DATA')

        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', hanning=True)

        # After running the task
        flag_col = th.getVarCol(self.outputms, 'FLAG')
        self.assertTrue(flag_col['r1'][0][0] == [True])
        self.assertTrue(flag_col['r1'][0][1] == [False])
        self.assertTrue(flag_col['r1'][0][61] == [False])
        self.assertTrue(flag_col['r1'][0][62] == [True])

        corr_col = th.getVarCol(self.outputms, 'DATA')
        nrows = len(corr_col)

      # Loop over every 2nd row,pol and get the data for each channel
        max = 1e-05
        for i in range(1,nrows,2) :
            row = 'r%s'%i
            # polarization is 0-1
            for pol in range(0,2) :
                # array's channels is 0-63
                for chan in range(1,62) :
                    # channels must start from second and end before the last
                    data = data_col[row][pol][chan]
                    dataB = data_col[row][pol][chan-1]
                    dataA = data_col[row][pol][chan+1]

                    Smoothed = th.calculateHanning(dataB,data,dataA)
                    CorData = corr_col[row][pol][chan]

                    # Check the difference
                    self.assertTrue(abs(CorData-Smoothed) < max )

    def test_hanning4(self):
        '''mstransform: Flagging should be correct after hanning smoothing and frame transformation.'''
        # hanning: test8
#        clearcal(vis=self.vis)
        self.outputms = "hann4.ms"

      # check correct flagging (just for one row as a sample)
        flag_col = th.getVarCol(self.vis, 'FLAG')
        self.assertTrue(flag_col['r1'][0][0] == [False])
        self.assertTrue(flag_col['r1'][0][1] == [False])
        self.assertTrue(flag_col['r1'][0][61] == [False])
        self.assertTrue(flag_col['r1'][0][62] == [False])

        mstransform(vis=self.vis, outputvis=self.outputms, hanning=True, regridms=True,
                        outframe='cmb',datacolumn='data')

      # check correct flagging (just for one row as a sample)
        flag_col = th.getVarCol(self.outputms, 'FLAG')
        self.assertTrue(flag_col['r1'][0][0] == [True])
        self.assertTrue(flag_col['r1'][0][1] == [False])
        self.assertTrue(flag_col['r1'][0][2] == [False])
        self.assertTrue(flag_col['r1'][0][60] == [False])
        self.assertTrue(flag_col['r1'][0][61] == [True])
        self.assertTrue(flag_col['r1'][0][62] == [True])


class test_FreqAvg(test_base):
    '''Tests for frequency averaging'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_g19()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        pass

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputvis
        os.system('rm -rf '+ self.outputms)

    def test_freqavg1(self):
        '''mstranform: Average 20 channels of one spw'''
        self.outputms = "favg1.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, spw='2', chanaverage=True, chanbin=20)

        self.assertTrue(os.path.exists(self.outputms))
        ret = th.verifyMS(self.outputms, 1, 6, 0)
        self.assertTrue(ret[0],ret[1])

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 1, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')

    def test_freqavg2(self):
        '''mstranform: Select a few channels to average from one spw'''
        self.outputms = "favg2.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, spw='2:10~20', chanaverage=True, chanbin=2)

        self.assertTrue(os.path.exists(self.outputms))
        ret = th.verifyMS(self.outputms, 1, 5, 0)
        self.assertTrue(ret[0],ret[1])

    def test_freqavg3(self):
        '''mstranform: Average all channels of one spw'''
        self.outputms = "favg3.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, spw='23', chanaverage=True, chanbin=128)

        self.assertTrue(os.path.exists(self.outputms))
        ret = th.verifyMS(self.outputms, 1, 1, 0)
        self.assertTrue(ret[0],ret[1])

    def test_freqavg4(self):
        '''mstranform: Average using different bins for several spws'''
        self.outputms = "favg4.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, spw='10,12,20', chanaverage=True,
                    chanbin=[128,4,10])

        self.assertTrue(os.path.exists(self.outputms))

        # Output should be:
        # spw=0 1 channel
        # spw=1 32 channels
        # spw=3 13 channels
        ret = th.verifyMS(self.outputms, 3, 1, 0, ignoreflags=True)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 3, 32, 1, ignoreflags=True)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 3, 12, 2, ignoreflags=True)
        self.assertTrue(ret[0],ret[1])

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 3, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r3'][0], 2,'Error re-indexing DATA_DESCRIPTION table')

    def test_freqavg5(self):
        '''mstranform: Different number of spws and chanbin. Expected error'''
        self.outputms = "favg5.ms"
        with self.assertRaises(ValueError):
            mstransform(vis=self.vis, outputvis=self.outputms, spw='2,10', chanaverage=True,
                        chanbin=[10,20,4])
        self.assertFalse(os.path.exists(self.outputms))

    def test_freqavg11(self):
        '''mstransform: Automatically convert numpy type to Python type'''
        self.outputms = "freqavg_numpytype.ms"
        bin1 = numpy.int32(128)
        mstransform(vis=self.vis, outputvis=self.outputms, spw='10', chanaverage=True,
                    chanbin=bin1)

        self.assertTrue(os.path.exists(self.outputms))

        # Output should be:
        # spw=0 1 channel
        ret = th.verifyMS(self.outputms, 1, 1, 0, ignoreflags=True)
        self.assertTrue(ret[0],ret[1])


class test_Shape(test_base):
    '''Test the tileshape parameter'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        pass

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputvis
        os.system('rm -rf '+ self.outputms)

    def test_shape1(self):
        '''mstransform: default tileshape'''
        self.outputms = "shape1.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, createmms=False, tileshape=[0])

        self.assertTrue(os.path.exists(self.outputms))

        # Get the tile shape in input
        tb_local.open(self.vis)
        inpdm = tb_local.getdminfo()
        tb_local.close()
        inptsh = th.getTileShape(inpdm)

        # Get the tile shape for the output
        tb_local.open(self.outputms)
        outdm = tb_local.getdminfo()
        tb_local.close()
        outtsh = th.getTileShape(outdm)

        # Compare both
        self.assertTrue((inptsh==outtsh).all(), 'Tile shapes are different')

    @unittest.skip('As reported in CAS-7377 now there is a custom tile shape for hypercube and data type')
    def test_shape2(self):
        '''mstransform: custom tileshape'''
        self.outputms = "shape2.ms"
        inptsh = [4,20,1024]
        mstransform(vis=self.vis, outputvis=self.outputms, createmms=False, tileshape=inptsh)

        self.assertTrue(os.path.exists(self.outputms))

        # Check the tile shape for the output
        tb_local.open(self.outputms)
        outdm = tb_local.getdminfo()
        tb_local.close()
        outtsh = th.getTileShape(outdm)

        self.assertTrue((inptsh==outtsh).all(), 'Tile shapes are different')


class test_Columns(test_base):
    '''Test different datacolumns'''

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf ngc5921Jy.ms allcols.ms')

    def test_col1(self):
          """mstransform: try to make real a non-existing virtual MODEL column"""
          self.setUp_ngc5921()
          self.outputms = "col1.ms"
          mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='all', realmodelcol=True)

          self.assertTrue(os.path.exists(self.outputms))
          mcol = th.getColDesc(self.outputms, 'MODEL_DATA')
          mkeys = mcol.keys()
          self.assertTrue(mkeys.__len__()==0, 'Should not add MODEL_DATA column')

    def test_col2(self):
          """mstransform: make real a virtual MODEL column """
          self.setUp_ngc5921()
          self.outputms = "col2.ms"
          inpms = 'ngc5921Jy.ms'
          shutil.copytree(self.vis, inpms)

          # First, run setjy to create a virtual MODEl column (SOURCE_MODEL)
          setjy(vis=inpms,field='1331+305*',model='',standard='Perley-Taylor 99',
                scalebychan=False, usescratch=False)

          # Verify that the virtual column exists
          mcol = th.getColDesc(inpms+'/SOURCE', 'SOURCE_MODEL')
          mkeys = mcol.keys()
          self.assertTrue(mkeys.__len__() > 0, 'Should have a SOURCE_MODEL column')

          # Make the virtual column a real MODEL_DATA column
          mstransform(vis=inpms, outputvis=self.outputms, datacolumn='all', realmodelcol=True)

          self.assertTrue(os.path.exists(self.outputms))
          mcol = th.getColDesc(self.outputms, 'MODEL_DATA')
          mkeys = mcol.keys()
          self.assertTrue(mkeys.__len__() > 0, 'Should have a MODEL_DATA column')

    def test_col3(self):
        '''mstransform: split out the MODEL column'''
        self.setUp_4ants()
        self.outputms = 'col3.ms'
        mstransform(vis=self.vis, outputvis=self.outputms,field='1',spw='0:0~61',
                    datacolumn='model')

        # Compare with split. CAS-4940
        outputms = 'split3.ms'
        split(vis=self.vis, outputvis=outputms,field='1',spw='0:0~61',
                    datacolumn='model')

        th.compVarColTables('split3.ms','col3.ms','DATA')

    def test_col4(self):
        '''mstransform: split out the DATA,MODEL,CORRECTED columns'''
        self.setUp_4ants()
        self.outputms = 'col4.ms'
        mstransform(vis=self.vis, outputvis=self.outputms,spw='0',
                    datacolumn='data,model,corrected')
        mstransform(vis=self.vis, outputvis='allcols.ms',spw='0',
                    datacolumn='all')

        self.assertTrue(th.compTables('allcols.ms', self.outputms,['FLAG_CATEGORY','WEIGHT_SPECTRUM','SIGMA_SPECTRUM'],0.000001,"absolute"))

    def test_weight_corr_sel(self):
        '''mstransform: check WEIGHT shape after correlation selection'''
        self.setUp_4ants()
        self.outputms = 'colweight1.ms'
        mstransform(vis=self.vis, outputvis=self.outputms,correlation='RR,LL',spw='1',
                    datacolumn='data')

        # Check the dimensions of the WEIGHT and SIGMA columns. CAS-6946
        out_ws = th.getColShape(self.outputms,'WEIGHT')
        out_ss = th.getColShape(self.outputms,'SIGMA')
        self.assertEqual(out_ws[0],'[2]','WEIGHT shape is not correct')
        self.assertEqual(out_ss[0],'[2]','SIGMA shape is not correct')

    def test_weight_ws_corr_sel(self):
        '''mstransform: check WEIGHT shape after correlation selection. WEIGHT_SPECTRUM exists'''
        self.setUp_4ants()
        self.outputms = 'colweight2.ms'
        mstransform(vis=self.vis, outputvis=self.outputms,correlation='RL,LR',spw='2:1~10',
                    datacolumn='corrected',usewtspectrum=True)

        # Check the dimensions of the WEIGHT and SIGMA columns. CAS-6946
        out_ws = th.getColShape(self.outputms,'WEIGHT')
        out_ss = th.getColShape(self.outputms,'SIGMA')
        self.assertEqual(out_ws[0],'[2]','WEIGHT shape is not correct')
        self.assertEqual(out_ss[0],'[2]','SIGMA shape is not correct')


class test_SeparateSPWs(test_base):
    '''Test the nspw parameter to separate spws'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.tmpms = ""

    def tearDown(self):
        os.system('rm -rf '+ self.tmpms)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf list.obs')

    def test_sep1(self):
        '''mstransform: separate one spw into 4, using default regrid parameters'''
        self.outputms = "separate1.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, spw='2', regridms=True, nspw=4)
        self.assertTrue(os.path.exists(self.outputms))

        # Should create 4 spws with 16 channels each
        ret = th.verifyMS(self.outputms, 4, 16, 0)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 4, 16, 1)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 4, 16, 2)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 4, 16, 3)
        self.assertTrue(ret[0],ret[1])

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 4, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r3'][0], 2,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r4'][0], 3,'Error re-indexing DATA_DESCRIPTION table')

    def test_sep2(self):
        '''mstransform: separate three spws into 2, using default regrid parameters'''
        self.outputms = "separate2.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, spw='2,3,4', regridms=True, nspw=2)
        self.assertTrue(os.path.exists(self.outputms))

        # Should create 2 spws with ?96 channels each
        ret = th.verifyMS(self.outputms, 2, 96, 0)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 2, 96, 1)
        self.assertTrue(ret[0],ret[1])

        listobs(self.outputms, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

    def test_sep3(self):
        '''mstransform: separate 16 spws into 4 with 10 channels each'''
        self.outputms = "separate3.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, regridms=True, nspw=4, nchan=10)
        self.assertTrue(os.path.exists(self.outputms))

        # Should create 4 spws with 10 channels each
        ret = th.verifyMS(self.outputms, 4, 10, 0)
        self.assertTrue(ret[0],ret[1])
        ret = th.verifyMS(self.outputms, 4, 10, 3)
        self.assertTrue(ret[0],ret[1])

    def test_CAS_5403_1(self):
        '''mstransform: separate spw 0 into 4 spws and check that DDI subtable is reindexed properly'''
        self.outputms = "test_5403_1.ms"
        mstransform(vis=self.vis, outputvis=self.outputms,regridms=True,spw='0',nspw=4)
        self.assertTrue(os.path.exists(self.outputms))

        # DDI subtable should have 4 rows with the proper index
        tb_local.open(self.outputms + '/DATA_DESCRIPTION')
        spwCol = tb_local.getcol('SPECTRAL_WINDOW_ID')
        tb_local.close()
        nspw = spwCol.size
        check_eq(nspw, 4)
        check_eq(spwCol[0], 0)
        check_eq(spwCol[1], 1)
        check_eq(spwCol[2], 2)
        check_eq(spwCol[3], 3)

        # Check some values from main table
        tb_local.open(self.outputms)
        data = tb_local.getcol('DATA')
        check_eq(data.shape,(4,16,4296))
        tb_local.close()

    def test_CAS_5403_2(self):
        '''mstransform: combine spw 0,1,2 into one spw and then break it down in 4 spws.
                        and then check that DDI subtable is reindexed properly'''
        self.outputms = "test_5403_2.ms"
        mstransform(vis=self.vis, outputvis=self.outputms,regridms=True,spw='0,1,2',nspw=4)
        self.assertTrue(os.path.exists(self.outputms))

        # DDI subtable should have 4 rows with the proper index
        tb_local.open(self.outputms + '/DATA_DESCRIPTION')
        spwCol = tb_local.getcol('SPECTRAL_WINDOW_ID')
        tb_local.close()
        nspw = spwCol.size
        check_eq(nspw, 4)
        check_eq(spwCol[0], 0)
        check_eq(spwCol[1], 1)
        check_eq(spwCol[2], 2)
        check_eq(spwCol[3], 3)

        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 4, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r3'][0], 2,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r4'][0], 3,'Error re-indexing DATA_DESCRIPTION table')

    def test_slicing_problem(self):
        '''mstransform: Separate SPWs after re-gridding one single SPW'''
        self.outputms = "test_slicing_problem.ms"
        mstransform(vis=self.vis, outputvis=self.outputms,regridms=True,nspw=3,nchan=10,spw='0:0~49')
        self.assertTrue(os.path.exists(self.outputms))

        tb_local.open(self.outputms + '/SPECTRAL_WINDOW')
        numChan = tb_local.getcol('NUM_CHAN')
        tb_local.close()
        check_eq(numChan[0], 10)
        check_eq(numChan[1], 10)
        check_eq(numChan[2], 10)

    def test_CAS_6529(self):
        '''mstransform: Combine all SPWs and then separated them to obtain the original grid'''

        self.tmpms = "combined.ms"
        self.outputms = "separated.ms"

        mstransform(vis=self.vis, outputvis=self.tmpms,combinespws=True,datacolumn = 'data')
        mstransform(vis=self.tmpms,outputvis=self.outputms,regridms = True ,
                    mode = 'channel' , nchan = 64 , nspw = 16, datacolumn='data')

        listobs_dict = listpartition(self.outputms,createdict=True)
        spws_scan_30 = listobs_dict[0]['scanId'][30]['spwIds']
        self.assertEqual(len(spws_scan_30), 16,'Wrong number of SPWs found in scan 30')
        self.assertEqual(spws_scan_30[0],0,'SPW Id 0 not found in scan 30')
        self.assertEqual(spws_scan_30[15],15,'SPW Id 15 not found in scan 30')


class test_state(test_base):
    '''Test operation with state id'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_CAS_5076()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        pass

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputms
        os.system('rm -rf '+ self.outputms)

    def test_select_by_scan_intent_and_reindex_state_accordingly(self):
        '''mstransform: select a scan intent and re-index state sub-table'''
        self.outputms = "test_select_by_scan_intent_and_reindex_state_accordingly.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, intent='OBSERVE_TARGET*', datacolumn='DATA')
        self.assertTrue(os.path.exists(self.outputms))

        # Check that state sub-table has been re-indexed
        tb_local.open(self.outputms + '/STATE')
        scan_intent = tb_local.getcol('OBS_MODE')
        tb_local.close()
        n_subscans = scan_intent.size
        check_eq(n_subscans, 12)

        # listobs checks that re-indexing is consistent
        listobs(self.outputms)

    def test_select_by_scan_but_not_implicit_state_reindex(self):
        '''mstransform: select 2 scans and do not automatically re-index state sub-table'''
        self.outputms = "test_select_by_scan_but_not_implicit_state_reindex.ms"
        mstransform(vis=self.vis, outputvis=self.outputms, scan='2,3', datacolumn='DATA')
        self.assertTrue(os.path.exists(self.outputms))

        # Check that state sub-table has not been re-indexed
        tb_local.open(self.outputms + '/STATE')
        scan_intent = tb_local.getcol('OBS_MODE')
        tb_local.close()
        n_subscans = scan_intent.size
        check_eq(n_subscans, 30)

        # listobs checks that re-indexing is consistent
        listobs(self.outputms)

class test_WeightSpectrum(test_base):
    '''Test usage of WEIGHT_SPECTRUM to channel average and combine SPWs with different exposure'''

    def setUp(self):
        pass

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outvis)

    def test_combineSPWDiffExpWithWeightSpectrum(self):
        '''mstransform: Combine SPWs with different exposure using WEIGHT_SPECTRUM'''

        self.vis = 'combine-1-timestamp-2-SPW-with-WEIGHT_SPECTRUM-Different-Exposure.ms'
        self.outvis = 'combineSPWDiffExpWithWeightSpectrum.ms'
        self.copyfile(self.vis)

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn="DATA",combinespws=True,regridms=True,
                    mode="frequency",nchan=50,start="2.20804e+10Hz",width="1.950702e+05Hz",nspw=1,
                    interpolation="fftshift",phasecenter="J2000 12h01m53.13s -18d53m09.8s",
                    outframe="CMB",veltype="radio")

        tb_local.open(self.outvis)
        data = tb_local.getcol('DATA')
        exposure = tb_local.getcol('EXPOSURE')
        tb_local.close()
        nchan = data.size
        check_eq(nchan, 50)
        check_eq(data[0][0][0].real, 0.1030, 0.0001)
        check_eq(data[0][nchan-1][0].imag, 0.0465, 0.0001)
        check_eq(exposure[0], 7.5, 0.0001)

    def test_combineSPWDiffExpWithWeightSpectrumFilledFromWeight(self):
        '''mstransform: Combine SPWs with different exposure using WEIGHT_SPECTRUM filled from WEIGHT'''

        self.vis = 'combine-1-timestamp-2-SPW-no-WEIGHT_SPECTRUM-Different-Exposure.ms'
        self.outvis = 'combineSPWDiffExpWithWeightSpectrumFilledFromWeight.ms'
        self.copyfile(self.vis)

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn="DATA",combinespws=True,regridms=True,
                    mode="frequency",nchan=50,start="2.20804e+10Hz",width="1.950702e+05Hz",nspw=1,
                    interpolation="fftshift",phasecenter="J2000 12h01m53.13s -18d53m09.8s",
                    outframe="CMB",veltype="radio",usewtspectrum=True)

        tb_local.open(self.outvis)
        data = tb_local.getcol('DATA')
        exposure = tb_local.getcol('EXPOSURE')
        tb_local.close()
        nchan = data.size
        check_eq(nchan, 50)
        check_eq(data[0][0][0].real, 0.1030, 0.0001)
        check_eq(data[0][nchan-1][0].imag, 0.0465, 0.0001)
        check_eq(exposure[0], 7.5, 0.0001)

    @unittest.skip('Skip until propagation of WEIGHT_SPECTRUM by cvel is defined')
    def test_fillWeightSpectrumFromWeight(self):
        '''mstransform: Fill output WEIGHT_SPECTRUM using WEIGHTS'''

        self.vis = 'combine-1-timestamp-2-SPW-no-WEIGHT_SPECTRUM-Same-Exposure.ms'
        self.outvis = 'fillWeightSpectrumFromWeight.ms'
        self.copyfile(self.vis)

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn="DATA",combinespws=True,regridms=True,
                    mode="frequency",nchan=50,start="2.20804e+10Hz",width="1.950702e+05Hz",nspw=1,
                    interpolation="fftshift",phasecenter="J2000 12h01m53.13s -18d53m09.8s",
                    outframe="CMB",veltype="radio",usewtspectrum=True)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        nchan = weightSpectrum.size
        check_eq(nchan, 50)
        check_eq(weightSpectrum[0][0][0], 1.9007, 0.0001)
        check_eq(weightSpectrum[0][nchan-1][0], 1.0156, 0.0001)

    def test_combineSPWAndChanAvgWithWeightSpectrum(self):
        '''mstransform: Combine SPWs and channel average using WEIGHT_SPECTRUM'''

        self.vis = 'combine-1-timestamp-2-SPW-with-WEIGHT_SPECTRUM-Same-Exposure.ms'
        self.outvis = 'test_combineSPWAndChanAvgWithWeightSpectrum.ms'
        self.copyfile(self.vis)

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn="DATA",combinespws=True,regridms=True,
                    mode="frequency",nchan=12,start="2.20804e+10Hz",width="7.802808e+05Hz",nspw=1,
                    interpolation="fftshift",phasecenter="J2000 12h01m53.13s -18d53m09.8s",
                    outframe="CMB",veltype="radio",chanaverage=True,chanbin=4)

        tb_local.open(self.outvis)
        data = tb_local.getcol('DATA')
        tb_local.close()
        nchan = data.size
        check_eq(nchan, 12)
        check_eq(data[0][0][0].real, 0.0901, 0.0001)
        check_eq(data[0][nchan-1][0].imag, -0.2374, 0.0001)

    def test_combineSPWDiffExpAndChanAvgWithWeightSpectrum(self):
        '''mstransform: Combine SPWs with different exposure and channel average using WEIGHT_SPECTRUM'''

        self.vis = 'combine-1-timestamp-2-SPW-with-WEIGHT_SPECTRUM-Different-Exposure.ms'
        self.outvis = 'test_combineSPWDiffExpAndChanAvgWithWeightSpectrum.ms'
        self.copyfile(self.vis)

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn="DATA",combinespws=True,regridms=True,
                    mode="frequency",nchan=12,start="2.20804e+10Hz",width="7.802808e+05Hz",nspw=1,
                    interpolation="fftshift",phasecenter="J2000 12h01m53.13s -18d53m09.8s",
                    outframe="CMB",veltype="radio",chanaverage=True,chanbin=4)

        tb_local.open(self.outvis)
        data = tb_local.getcol('DATA')
        tb_local.close()
        nchan = data.size
        check_eq(nchan, 12)
        check_eq(data[0][0][0].real, 0.0901, 0.0001)
        check_eq(data[0][nchan-1][0].imag, -0.2374, 0.0001)


class test_channelAverageByDefault(test_base_compare):

    def setUp(self):
        super(test_channelAverageByDefault,self).setUp()
        self.setUp_almasim()
        self.outvis = 'test_channelAverageByDefaultInVelocityMode-mst.ms'
        self.refvis = 'test_channelAverageByDefaultInVelocityMode-cvel.ms'
        self.outvis_sorted = 'test_channelAverageByDefaultInVelocityMode-mst-sorted.ms'
        self.refvis_sorted = 'test_channelAverageByDefaultInVelocityMode-cvel-sorted.ms'
        os.system('rm -rf test_channelAverageByDefaultInVelocityMode*')

    def tearDown(self):
        super(test_channelAverageByDefault,self).tearDown()

    @unittest.skip('Skip, cvel produces an exception since release 4.7.2 as per CAS-9798'
                   ' (regridding with pre-averaging)')
    def test_channelAverageByDefaultInVelocityMode(self):
        self.outvis = 'test_channelAverageByDefaultInVelocityMode.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,regridms=True,combinespws=True,interpolation="linear",
                    mode="velocity",veltype="optical",width='30km/s',restfreq='230GHz',datacolumn='ALL')
        cvel(vis=self.vis,outputvis=self.refvis,interpolation="linear",mode="velocity",veltype="optical",width='30km/s',restfreq='230GHz')

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1

        self.post_process()

    def test_noregrid_channelAverageByDefaultInVelocityMode(self):
        self.outvis = 'test_channelAverageByDefaultInVelocityMode.ms'

        mstransform(vis=self.vis, outputvis=self.outvis, regridms=True,
                    combinespws=True,interpolation="linear", mode="velocity",
                    veltype="optical", width='30km/s', restfreq='230GHz', datacolumn='ALL')

        self.assertTrue(os.path.isdir(self.outvis))


class test_float_column(test_base):
    def setUp(self):
        self.setUp_floatcol()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_regrid_float(self):
        '''mstransform: change outframe of FLOAT_DATA MS'''
        self.outputms = 'floatcol1.mms'
        # jagonzal: Let's use spw 0, because the rest have negative chan widths
        mstransform(vis=self.vis,outputvis=self.outputms,datacolumn='FLOAT_DATA',
                    regridms=True,outframe='LSRK',spw='0')

        print("Check column and keywords")
        tb_local.open(self.outputms+'/SPECTRAL_WINDOW')
        refnum = tb_local.getcell('MEAS_FREQ_REF',0)
        tb_local.close()
        self.assertEqual(refnum, 1)

        # CAS-5900. Check the keywords in FLOAT_DATA are the same
        tb_local.open(self.outputms)
        okeys = tb_local.getcolkeywords('FLOAT_DATA')
        tb_local.close()
        tb_local.open(self.vis)
        ikeys = tb_local.getcolkeywords('FLOAT_DATA')
        tb_local.close()
        self.assertDictEqual(ikeys, okeys, 'Keywords from FLOAT_DATA are different')


class test_timeaverage(test_base_compare):

    def setUp(self):
        super(test_timeaverage,self).setUp()
        self.setUp_4ants()
        self.outvis = 'test_timeaverage-mst.ms'
        self.refvis = 'test_timeaverage-split.ms'
        self.outvis_sorted = 'test_timeaverage-mst-sorted.ms'
        self.refvis_sorted = 'test_timeaverage-split-sorted.ms'
        self.timerange = '14:45:08.00~14:45:10.00'
        self.spw = '0'
        self.antenna = '0&&1'
        self.timerange = ''
        self.spw = ''
        self.antenna = ''
        os.system('rm -rf test_timeaverage*')

    def tearDown(self):
        super(test_timeaverage,self).tearDown()

    def unflag_ms(self):
        af_local.open(self.vis)
        af_local.selectdata()
        agentUnflag={'apply':True,'mode':'unflag'}
        af_local.parseagentparameters(agentUnflag)
        af_local.init()
        af_local.run(writeflags=True)
        af_local.done()

    def flag_ms(self):
        af_local.open(self.vis)
        af_local.selectdata()
        agentUnflag={'apply':True,'mode':'rflag','extendflags':False}
        af_local.parseagentparameters(agentUnflag)
        af_local.init()
        af_local.run(writeflags=True)
        af_local.done()

    def test_timeaverage_data_unflagged(self):

        self.unflag_ms()

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='DATA',timeaverage=True,timebin='30s',
                    timerange=self.timerange,spw=self.spw,antenna=self.antenna)
        split(vis=self.vis,outputvis=self.refvis,datacolumn='DATA',timebin='30s',
                    timerange=self.timerange,spw=self.spw,antenna=self.antenna)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5

        self.post_process()

        # Check the dimenstions of the WEIGHT and SIGMA columns. CAS-6946
        inp_ws = th.getColShape(self.vis,'WEIGHT')
        inp_ss = th.getColShape(self.vis,'SIGMA')
        out_ws = th.getColShape(self.outvis,'WEIGHT')
        out_ss = th.getColShape(self.outvis,'SIGMA')
        self.assertListEqual(inp_ws, out_ws, 'WEIGHT shape differ in input and output')
        self.assertListEqual(inp_ss, out_ss, 'SIGMA shape differ in input and output')

    def test_timeaverage_model_unflagged(self):

        self.unflag_ms()

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='MODEL',timeaverage=True,timebin='30s',
                   timerange=self.timerange,spw=self.spw,antenna=self.antenna)
        split(vis=self.vis,outputvis=self.refvis,datacolumn='MODEL',timebin='30s',
              timerange=self.timerange,spw=self.spw,antenna=self.antenna)


        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5

        self.post_process()

    def test_timeaverage_corrected_unflagged(self):

        self.unflag_ms()

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='CORRECTED',timeaverage=True,timebin='30s',
                    timerange=self.timerange,spw=self.spw,antenna=self.antenna)
        split(vis=self.vis,outputvis=self.refvis,datacolumn='CORRECTED',timebin='30s',
              timerange=self.timerange,spw=self.spw,antenna=self.antenna)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-3

        self.mode['EXPOSURE'] = "absolute"
        self.tolerance['EXPOSURE'] = 1E-4

        self.mode['TIME_CENTROID'] = "absolute"
        self.tolerance['TIME_CENTROID'] = 1E-5


        self.post_process()

    def test_timeaverage_baseline_dependent_unflagged(self):

        self.unflag_ms()

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='DATA',timeaverage=True,timebin='10s',maxuvwdistance=1E5)
        mstransform(vis=self.vis,outputvis=self.refvis,datacolumn='DATA',timeaverage=True,timebin='10s')

        self.generate_tolerance_map()

        self.post_process()

class test_timeaverage_limits(test_base):

    @classmethod
    def setUpClass(cls):
        cls.setUp_CAS_4850()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = 'test_timeaverage_limits.ms'

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outvis
        os.system('rm -rf '+ self.outvis)

    def test_CAS_4850(self):

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='DATA',timeaverage=True,timebin='40s')

        tb_local.open(self.outvis)
        interval = tb_local.getcol('INTERVAL')
        print(interval[0])
        tb_local.close()
        check_eq(interval[0] >= 40.0,True)

class test_multiple_transformations(test_base_compare):

    def setUp(self):
        super(test_multiple_transformations,self).setUp()
        self.setUp_4ants()
        self.outvis = 'test_timeaverage_and_combine_spws_single_run.ms'
        self.tmpvis = 'test_timeaverage_and_combine_spws_1st_step.ms'
        self.refvis = 'test_timeaverage_and_combine_spws_2nd_step.ms'
        self.outvis_sorted = 'test_timeaverage_and_combine_spws_single_run_sorted.ms'
        self.refvis_sorted = 'test_timeaverage_and_combine_spws_2nd_step_sorted.ms'
        os.system('rm -rf test_timeaverage_and_combine_spws*')

    def tearDown(self):
        super(test_multiple_transformations,self).tearDown()

    def test_timeaverage_2x_and_combine_two_spws_one_baseline_one_timestep(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',antenna="0&&1", timerange='14:45:08.50~14:45:9.50',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',antenna="0&&1", timerange='14:45:08.50~14:45:9.50',
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_2x_and_combine_two_spws_one_baseline_two_timesteps(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',antenna="0&&1", timerange='14:45:08.50~14:45:11.50',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',antenna="0&&1", timerange='14:45:08.50~14:45:11.50',
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_2x_and_combine_two_spws_two_baselines_one_timestep(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',antenna="0&&1~2", timerange='14:45:08.50~14:45:9.50',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',antenna="0&&1~2", timerange='14:45:08.50~14:45:9.50',
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_2x_and_combine_two_spws_two_baselines_two_timesteps(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',antenna="0&&1~2", timerange='14:45:08.50~14:45:11.50',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',antenna="0&&1~2", timerange='14:45:08.50~14:45:11.50',
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_2x_and_combine_two_spws_two_baselines(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',antenna="0&&1~2",
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',antenna="0&&1~2",
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_30x_and_combine_two_spws_two_baselines(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',antenna="0&&1~2",
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='30s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',antenna="0&&1~2",
                    datacolumn='DATA',timeaverage=True,timebin='30s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_2x_and_combine_two_spws_four_baselines(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_30x_and_combine_two_spws_four_baselines(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='9,10',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='30s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='9,10',
                    datacolumn='DATA',timeaverage=True,timebin='30s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_2x_and_combine_seven_spws_four_baselines(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='8~15',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='2s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='8~15',
                    datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5
        self.post_process()

    def test_timeaverage_30x_and_combine_seven_spws_four_baselines(self):

        mstransform(vis=self.vis,outputvis=self.outvis,spw='8~15',
                    datacolumn='DATA',combinespws=True,timeaverage=True,timebin='30s')
        mstransform(vis=self.vis,outputvis=self.tmpvis,spw='8~15',
                    datacolumn='DATA',timeaverage=True,timebin='30s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',combinespws=True)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

    def test_timeaverage_and_channel_average(self):

        mstransform(vis=self.vis,outputvis=self.outvis,datacolumn='DATA',timeaverage=True,timebin='2s',chanaverage=True,chanbin=2)
        mstransform(vis=self.vis,outputvis=self.tmpvis,datacolumn='DATA',timeaverage=True,timebin='2s',usewtspectrum=True)
        mstransform(vis=self.tmpvis,outputvis=self.refvis,datacolumn='DATA',chanaverage=True,chanbin=2)

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1E-5
        self.mode['SIGMA'] = "absolute"
        self.tolerance['SIGMA'] = 1E-5

        self.post_process()

        th.compTables(self.vis+'/FEED', self.outvis+'/FEED', ['FOCUS_LENGTH'])


class test_spw_poln(test_base):
    '''tests for spw with different correlation shapes'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_3c84()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        pass

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputms
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf list.obs')

    def test_corr_selection(self):
        '''mstransform: verify correct re-indexing of sub-tables'''
        self.outputms = '3cLL.ms'

        # It will select spws 1,2,3, polids=1,2,3 but each with 1 NUM_CORR only
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', correlation='LL')

        # Verify the input versus the output
        ms_local.open(self.vis)
        ms_local.msselect({'polarization':'LL'})
        inp_nrow = ms_local.nrow(True)
        ms_local.close()

        ms_local.open(self.outputms)
        out_nrow = ms_local.nrow()
        ms_local.close()

        self.assertEqual(inp_nrow, out_nrow)

        # Verify that DATA_DESCRIPTION table is properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 3, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r3'][0], 2,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')

        pol_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'POLARIZATION_ID')
        self.assertEqual(pol_col['r1'][0], 1,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')
        self.assertEqual(pol_col['r2'][0], 2,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')
        self.assertEqual(pol_col['r3'][0], 3,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')

        # Verify that POLARIZATION table is not re-sized.
        corr_col = th.getVarCol(self.outputms+'/POLARIZATION', 'NUM_CORR')
        self.assertEqual(corr_col.keys().__len__(), 4, 'Wrong number of rows in POLARIZATION table')

        listobs(self.outputms, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

    def test_repeated_spwid(self):
        '''mstransform: split one spw ID mapping to two DDI'''
        # MS looks like this:
        # SpwID  #Chans  Corrs        DDID
        # 0       256    RR           0
        # 0       256    LL           1
        # 1       128    RR,LL        2
        # 2       64     RR,RL,LR,LL  3

        self.outputms = '3cspw0.ms'
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='0')

        # Verify the input versus the output
        ms_local.open(self.vis)
        ms_local.msselect({'spw':'0'})
        inp_nrow = ms_local.nrow(True)
        ms_local.close()

        ms_local.open(self.outputms)
        out_nrow = ms_local.nrow()
        ms_local.close()

        self.assertEqual(inp_nrow, out_nrow)

        # Verify that DATA_DESCRIPTION table is properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 2, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 0,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')

        pol_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'POLARIZATION_ID')
        self.assertEqual(pol_col['r1'][0], 0,'Error re-indexing POLARIZATION_ID of DATA_DESCRIPTION table')
        self.assertEqual(pol_col['r2'][0], 1,'Error re-indexing POLARIZATION_ID of DATA_DESCRIPTION table')

    def test_spwid_poln_LL(self):
        '''mstransform: split one spw ID, polarization LL from RR,LL'''
        # MS looks like this:
        # SpwID  #Chans  Corrs        DDID
        # 0       256    RR           0
        # 0       256    LL           1
        # 1       128    RR,LL        2
        # 2       64     RR,RL,LR,LL  3

        self.outputms = '3cspw0LL.ms'
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='0',
                    correlation='LL')

        # Verify the input versus the output
        ms_local.open(self.vis)
        ms_local.msselect({'spw':'0','polarization':'LL'})
        inp_nrow = ms_local.nrow(True)
        ms_local.close()

        ms_local.open(self.outputms)
        out_nrow = ms_local.nrow()
        ms_local.close()

        self.assertEqual(inp_nrow, out_nrow)

    def test_spwids_poln_RR(self):
        '''mstransform: split two spw IDs, polarization RR from RR,LL'''
        # MS looks like this:
        # SpwID  #Chans  Corrs        DDID
        # 0       256    RR           0
        # 0       256    LL           1
        # 1       128    RR,LL        2
        # 2       64     RR,RL,LR,LL  3

        self.outputms = '3cspw01RR.ms'
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='0,1',
                    correlation='RR')

        # Verify the input versus the output
        ms_local.open(self.vis)
        ms_local.msselect({'spw':'0,1','polarization':'RR'})
        inp_nrow = ms_local.nrow(True)
        ms_local.close()

        ms_local.open(self.outputms)
        out_nrow = ms_local.nrow()
        ms_local.close()

        self.assertEqual(inp_nrow, out_nrow)

    def test_spw_selection(self):
        '''mstransform: split two spws with different polarization shapes'''
        self.outputms = '3cspw12.ms'
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='1,2')

        # Verify the input versus the output
        ms_local.open(self.vis)
        ms_local.msselect({'spw':'1,2'})
        inp_nrow = ms_local.nrow(True)
        ms_local.close()

        ms_local.open(self.outputms)
        out_nrow = ms_local.nrow()
        ms_local.close()
        self.assertEqual(inp_nrow, out_nrow)

        # Verify that DATA_DESCRIPTION table is properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 2, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing SPECTRAL_WINDOW_ID of DATA_DESCRIPTION table')

        pol_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'POLARIZATION_ID')
        self.assertEqual(pol_col['r1'][0], 2,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')
        self.assertEqual(pol_col['r2'][0], 3,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')

        # Verify that POLARIZATION table is not re-sized.
        corr_col = th.getVarCol(self.outputms+'/POLARIZATION', 'NUM_CORR')
        self.assertEqual(corr_col.keys().__len__(), 4, 'Wrong number of rows in POLARIZATION table')

    def test_regrid_spw_with_diff_pol_shape(self):
        '''mstransform: regrid spw 0 that has repeated SPW ID'''
        self.outputms = '3cLSRKspw0.ms'
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='0',
                    regridms=True,outframe='LSRK')

        ret = th.verifyMS(self.outputms, 1, 256, 0)
        self.assertTrue(ret[0],ret[1])

        tb_local.open(self.outputms+'/SPECTRAL_WINDOW')
        refnum = tb_local.getcell('MEAS_FREQ_REF',0)
        tb_local.close()
        self.assertEqual(refnum, 1)

        listobs(self.outputms, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

        # Verify the metadata information
        msmd_local.open(self.outputms)
        dds = msmd_local.datadescids()
        msmd_local.done()
        self.assertEqual(dds.__len__(),2,'Wrong number of rows in DD table')

    def test_chanavg_spw_with_diff_pol_shape(self):
        '''mstransform: channel average spw 0 that has repeated SPW ID'''
        self.outputms = '3cChAvespw0.ms'
        # Create only one output channel
        mstransform(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='0',
                    chanaverage=True,chanbin=256)

        # verify the metadata of the output
        msmd_local.open(self.outputms)
        nchan = msmd_local.nchan(0) # 1
        nrow = msmd_local.nrows() # 2600
        dds = msmd_local.datadescids() # 2
        meanfreq = msmd_local.meanfreq(0) # 4968996093.75
        chanfreq = msmd_local.chanfreqs(0) # [4.96899609e+09]
        chanwidth = msmd_local.chanwidths(spw=0, unit='kHz') # 2000
        msmd_local.done()

        self.assertEqual(dds.__len__(),2,'Wrong number of rows in DD table')
        self.assertEqual(nchan, 1)
        self.assertEqual(nrow, 2600,'Wrong number of rows in DD table')
        self.assertTrue(numpy.isclose(meanfreq, 4968996093.75, rtol=1e-10, atol=1e-1))
        self.assertTrue(numpy.isclose(chanwidth, 2000, rtol=1e-10, atol=1e-4))
        self.assertAlmostEqual(meanfreq, chanfreq, 1)

        listobs(self.outputms, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

        # Check the FEED table
        out_feed_spw = th.getVarCol(self.outputms+'/FEED', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(len(out_feed_spw.keys()), 26)


class testFlags(test_base):
    '''Test the keepflags parameter'''
    def setUp(self):
        self.setUp_4ants()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_split_keepflags_false(self):
        '''mstransform: split them and do not keep flags in output MS'''
        self.outputms = 'donotkeepflags.ms'

        # Unflag and flag spw=4
        flagdata(self.vis, flagbackup=False, mode='list', inpfile=["mode='unflag'","spw='4'"])

        # Split scan=31 out
        mstransform(self.vis, outputvis=self.outputms, datacolumn='corrected', scan='31', keepflags=False)

        msmd_local.open(self.outputms)
        spws = msmd_local.spwsforscan(31)
        msmd_local.close()
        self.assertEqual(spws.size, 15)

    def test_select_dropped_spw(self):
        '''mstransform: keepflags=False and select flagged spw. Expect error.'''
        self.outputms = 'donotkeepflags_spw15.ms'

        # Unflag and flag spw=15
        flagdata(self.vis, flagbackup=False, mode='list', inpfile=["mode='unflag'","spw='15'"])

        try:
            mstransform(self.vis, outputvis=self.outputms, datacolumn='data', spw='>14', keepflags=False)
        except RuntimeError as instance:
            print('Expected Error: {0}'.format(instance))

        print('Expected Error!')

class test_subtables_evla(test_base):
    '''Test effect of SPW combination/separation on EVLA sub-tables'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_sub_tables_evla()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.tmpms = ''
        self.outputms = ''

    def tearDown(self):
        # all test cases expected to generate the output MSs in tmpsms / outputms
        os.system('rm -rf '+ self.tmpms)
        os.system('rm -rf '+ self.outputms)

    def test_remove_duplicates_after_combine_evla(self):
        '''mstransform: Check that sub-tables have no duplicates after SPW combination'''

        self.outputms = 'combined.ms'

        mstransform(self.vis, outputvis=self.outputms, datacolumn='data', combinespws=True)

        tb_local.open(self.outputms + '/SOURCE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 1)

        tb_local.open(self.outputms + '/FEED')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 27)

        tb_local.open(self.outputms + '/CALDEVICE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 27)

        tb_local.open(self.outputms + '/SYSPOWER')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 193590)

        tb_local.open(self.outputms + '/SYSPOWER')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 193590)

        tb_local.open(self.outputms + '/SPECTRAL_WINDOW')
        nrows = tb_local.nrows()
        bbcNo = tb_local.getcol('BBC_NO')
        tb_local.close()
        self.assertEqual(nrows, 1)
        self.assertEqual(bbcNo[0], 12)

    def test_multiplex_after_separation_evla(self):
        '''mstransform: Check that sub-tables are multiplexed after separating SPWs'''

        self.tmpms = 'combined.ms'
        self.outputms = 'separated.ms'

        mstransform(self.vis, outputvis=self.tmpms, datacolumn='data', combinespws=True)
        mstransform(self.tmpms, outputvis=self.outputms, datacolumn='data',regridms=True,mode='channel',nchan=64,nspw=2)

        tb_local.open(self.outputms + '/SOURCE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 1*2)

        tb_local.open(self.outputms + '/FEED')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 27*2)

        tb_local.open(self.outputms + '/CALDEVICE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 27*2)

        tb_local.open(self.outputms + '/SYSPOWER')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 193590*2)

        tb_local.open(self.outputms + '/SYSPOWER')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 193590*2)

        tb_local.open(self.outputms + '/SPECTRAL_WINDOW')
        nrows = tb_local.nrows()
        bbcNo = tb_local.getcol('BBC_NO')
        tb_local.close()
        self.assertEqual(nrows, 1*2)
        self.assertEqual(bbcNo[0], 12)

class test_weight_spectrum_creation(test_base):
    '''Test when WEIGHT/SIGMA_SPECTRUM columns are created or not (with usewtspectrum)'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        pass

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outputms
        os.system('rm -rf '+ self.outputms)

    def test_usewtspectrum_on(self):
        '''mstransform: when input MS does not have WEIGHT/SIGMA_SPECTRUM columns,
        usewtspectrum=True should be respected'''
        self.outputms = 'with_weight_spectrum.ms'

        mstransform(self.vis, outputvis=self.outputms, spw='0', scan='31', antenna='0',
                    usewtspectrum=True)

        tb_local.open(self.outputms)
        colnames = tb_local.colnames()
        sub_kw_names = tb_local.keywordnames()
        weight_sp_shape = tb_local.getcolshapestring('WEIGHT_SPECTRUM')
        sigma_sp_shape = tb_local.getcolshapestring('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertEqual(24, len(colnames))
        self.assertEqual(16, len(sub_kw_names))
        self.assertTrue('WEIGHT_SPECTRUM' in colnames)
        self.assertEqual(270, len(weight_sp_shape))
        self.assertEqual('[4, 64]', weight_sp_shape[0])
        self.assertTrue('SIGMA_SPECTRUM' in colnames)
        self.assertEqual(270, len(sigma_sp_shape))
        self.assertEqual('[4, 64]', sigma_sp_shape[0])

    def test_usewtspectrum_off(self):
        '''mstransform: when input MS does not have WEIGHT/SIGMA_SPECTRUM columns,
        if usewtspectrum is not set=True WEIGHT/SIGMA_SPECTRUM should not be created in
        the output MS'''
        self.outputms = 'without_weight_spectrum.ms'

        mstransform(self.vis, outputvis=self.outputms, scan='31', spw='0', antenna='0',
                    usewtspectrum=False)

        tb_local.open(self.outputms)
        colnames = tb_local.colnames()
        sub_kw_names = tb_local.keywordnames()
        tb_local.close()

        self.assertEqual(22, len(colnames))
        self.assertEqual(16, len(sub_kw_names))
        self.assertTrue('WEIGHT_SPECTRUM' not in colnames)
        self.assertTrue('SIGMA_SPECTRUM' not in colnames)

class test_subtables_alma(test_base):
    '''Test effect of SPW combination/separation on ALMA sub-tables'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_sub_tables_alma()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.tmpms = ''
        self.outputms = ''

    def tearDown(self):
        # all test cases are expected to generate the output MSs in tmpms / outputms
        os.system('rm -rf '+ self.tmpms)
        os.system('rm -rf '+ self.outputms)

    def test_remove_duplicates_after_combine_alma(self):
        '''mstransform: Check that sub-tables have no duplicates after SPW combination'''

        self.outputms = 'combined.ms'

        mstransform(self.vis, outputvis=self.outputms, datacolumn='data', combinespws=True)

        tb_local.open(self.outputms + '/SOURCE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 7)

        tb_local.open(self.outputms + '/FEED')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 32)

        tb_local.open(self.outputms + '/CALDEVICE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 32)

        tb_local.open(self.outputms + '/SYSCAL')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 288)

        tb_local.open(self.outputms + '/SPECTRAL_WINDOW')
        nrows = tb_local.nrows()
        bbcNo = tb_local.getcol('BBC_NO')
        tb_local.close()
        self.assertEqual(nrows, 1)
        self.assertEqual(bbcNo[0], 1)

    def test_multiplex_after_separation_alma(self):
        '''mstransform: Check that sub-tables are multiplexed after separating SPWs'''

        self.tmpms = 'combined.ms'
        self.outputms = 'separated.ms'

        mstransform(self.vis, outputvis=self.tmpms, datacolumn='data', combinespws=True)
        mstransform(self.tmpms, outputvis=self.outputms, datacolumn='data',regridms=True,nspw=4)

        tb_local.open(self.outputms + '/SOURCE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 7*4)

        tb_local.open(self.outputms + '/FEED')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 32*4)

        tb_local.open(self.outputms + '/CALDEVICE')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 32*4)

        tb_local.open(self.outputms + '/SYSCAL')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 288*4)

        tb_local.open(self.outputms + '/SPECTRAL_WINDOW')
        nrows = tb_local.nrows()
        bbcNo = tb_local.getcol('BBC_NO')
        tb_local.close()
        self.assertEqual(nrows, 1*4)
        self.assertEqual(bbcNo[0], 1)

    def test_chanwidth_sign(self):
        # mstransform: Check that chanwidth sign is correct after regridding without combine
        self.outputms = 'combined.ms'
        mstransform(self.vis, outputvis=self.outputms, datacolumn='data', regridms=True)

        tb_local.open(self.outputms + '/SPECTRAL_WINDOW')
        cw = tb_local.getcol('CHAN_WIDTH').T
        cf = tb_local.getcol('CHAN_FREQ').T
        for w, f in zip(cw, cf):
            if numpy.diff(f)[0] < 0:
                self.assertTrue((w < 0).all())
            else:
                self.assertTrue((w > 0).all())
        self.assertTrue((tb_local.getcol('TOTAL_BANDWIDTH') > 0).all())
        tb_local.close()


class test_radial_velocity_correction(test_base_compare):

    def setUp(self):
        super(test_radial_velocity_correction,self).setUp()
        self.setUp_titan()
        self.outvis = "test_radial_velocity_correction-mst.ms"
        self.refvis = "test_radial_velocity_correction-cvel.ms"
        self.outvis_sorted = "test_radial_velocity_correction-mst-sorted.ms"
        self.refvis_sorted = "test_radial_velocity_correction-cvel-sorted.ms"
        os.system("rm -rf test_radial_velocity_correction*")

    def tearDown(self):
        super(test_radial_velocity_correction,self).tearDown()

    def test_radial_velocity_correction_without_channel_average(self):

        cvel(vis=self.vis,outputvis=self.refvis,outframe='SOURCE',mode = 'velocity', width = '0.1km/s',restfreq = '354.50547GHz')
        mstransform(vis=self.vis,outputvis=self.outvis,regridms=True,datacolumn='DATA',outframe='SOURCE',
                    mode = 'velocity', width = '0.1km/s',restfreq = '354.50547GHz')

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1

        self.post_process()

    def test_too_low_width(self):
        # test for no crash
        with self.assertRaises(RuntimeError):
            mstransform(vis=self.vis,outputvis=self.outvis, regridms=True,
                        datacolumn='DATA',outframe='SOURCE', mode='velocity',
                        width='0.001km/s', restfreq = '354.50547GHz')

class test_radial_velocity_correction_largetimerange(test_base_compare):
    # CAS-7382, test large timerange spawns that require an additional
    # correction and exposed wrong reference timestamp usage in mstransform
    def setUp(self):
        super(test_radial_velocity_correction_largetimerange, self).setUp()
        self.vis = 'CAS-7382.ms'
        if os.path.exists(self.vis):
           self.removeInputMS()

        os.system('cp -RL {0} {1}'.format(os.path.join(datapath, self.vis),
                                          self.vis))

        self.outvis = "test-CAS-7382-mst.ms"
        self.refvis = "test-CAS-7382-cvel.ms"
        self.outvis_sorted = "test-CAS-7382-mst-sorted.ms"
        self.refvis_sorted = "test-CAS-7382-cvel-sorted.ms"
        os.system("rm -rf test-CAS-7382*")

    @unittest.skip('Skip, cvel produces an exception since release 4.7.2 as per CAS-9798')
    def test_ascending_freq(self):
        cvel(vis=self.vis, outputvis=self.refvis, spw='1', field='Titan',
             mode='velocity', width='0.5km/s', interpolation='linear',
             restfreq='349.45370GHz', outframe='SOURCE')
        cvel2(vis=self.vis, outputvis=self.outvis, spw='1', field='Titan',
             mode='velocity', width='0.5km/s', interpolation='linear',
             restfreq='349.45370GHz', outframe='SOURCE')

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1
        self.tolerance['DATA'] = 1e-5

        self.post_process()

        # Also check that the ephemerides table is copied to the right place
        self.assertTrue(os.path.isdir(self.outvis + '/FIELD/EPHEM0_Titan.tab'), 'Ephemerides table not copied to FIELD')
        self.assertFalse(os.path.isdir(self.outvis + '/EPHEM0_Titan.tab'), 'Ephemerides table copied to MAIN')

    def test_ascending_freq_noregrid(self):
        cvel2(vis=self.vis, outputvis=self.outvis, spw='1', field='Titan',
             mode='velocity', width='0.5km/s', interpolation='linear',
             restfreq='349.45370GHz', outframe='SOURCE')

        # Also check that the ephemerides table is copied to the right place
        self.assertTrue(os.path.isdir(self.outvis + '/FIELD/EPHEM0_Titan.tab'),
                        'Ephemerides table not copied to FIELD')
        self.assertFalse(os.path.isdir(self.outvis + '/EPHEM0_Titan.tab'),
                         'Ephemerides table copied to MAIN')

    @unittest.skip('Skip, cvel produces an exception since release 4.7.2 as per CAS-9798')
    def test_descending_freq(self):
        cvel(vis=self.vis, outputvis=self.refvis, spw='0', field='Titan',
             mode='velocity', width='0.5km/s', interpolation='linear',
             restfreq='349.45370GHz', outframe='SOURCE')
        cvel2(vis=self.vis, outputvis=self.outvis, spw='0', field='Titan',
             mode='velocity', width='0.5km/s', interpolation='linear',
             restfreq='349.45370GHz', outframe='SOURCE')

        self.generate_tolerance_map()

        self.mode['WEIGHT'] = "absolute"
        self.tolerance['WEIGHT'] = 1
        self.tolerance['DATA'] = 1e-5

        self.post_process()

        # Also check that the ephemerides table is copied to the right place
        self.assertTrue(os.path.isdir(self.outvis + '/FIELD/EPHEM0_Titan.tab'), 'Ephemerides table not copied to FIELD')
        self.assertFalse(os.path.isdir(self.outvis + '/EPHEM0_Titan.tab'), 'Ephemerides table copied to MAIN')

    def test_descending_freq_noregrid(self):
        cvel2(vis=self.vis, outputvis=self.outvis, spw='0', field='Titan',
             mode='velocity', width='0.5km/s', interpolation='linear',
             restfreq='349.45370GHz', outframe='SOURCE')

        # Also check that the ephemerides table is copied to the right place
        self.assertTrue(os.path.isdir(self.outvis + '/FIELD/EPHEM0_Titan.tab'),
                        'Ephemerides table not copied to FIELD')
        self.assertFalse(os.path.isdir(self.outvis + '/EPHEM0_Titan.tab'),
                         'Ephemerides table copied to MAIN')


class test_vla_mixed_polarizations(test_base):
    '''Test behaviour of mstransform in split mode when the input MS contains mixed VLA correlations XY/LR'''

    def setUp(self):

        self.setUp_CAS_6733()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_vla_mixed_polarizations_1(self):

        self.outputms = 'test_vla_mixed_polarizations_1.ms'

        mstransform(vis=self.vis,outputvis=self.outputms,scan='16',datacolumn='DATA')

        # Check that DDI sub-table is consistent with POLARIZATION sub-table
        tb_local.open(self.outputms + '/POLARIZATION')
        npols = tb_local.nrows()
        tb_local.close()

        tb_local.open(self.outputms + '/DATA_DESCRIPTION')
        polIds = tb_local.getcol('POLARIZATION_ID')
        tb_local.close()

        self.assertTrue(all(polIds < npols) and all(polIds > -1), 'PolarizationId in DATA_DESCRIPTION not consistent with POLARIZATION table')

        # Check that flagdata can run properly with output MS
        summary = flagdata(vis=self.outputms,mode='summary')
        self.assertTrue('correlation' in summary, 'Flagdata failure due to missformated MS')


class test_polarization_reindex(test_base):
    '''Test behaviour of mstransform in split mode when the input MS contains polarizations not reference by any DDI'''

    def setUp(self):

        self.setUp_CAS_7841()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_polarization_reindex_1(self):

        self.outputms = 'test_polarization_reindex_1.ms'

        mstransform(vis=self.vis,outputvis=self.outputms,field='J1256-0547',datacolumn='DATA')

        # Check that DDI sub-table is consistent with POLARIZATION sub-table
        tb_local.open(self.outputms + '/POLARIZATION')
        npols = tb_local.nrows()
        tb_local.close()

        tb_local.open(self.outputms + '/DATA_DESCRIPTION')
        polIds = tb_local.getcol('POLARIZATION_ID')
        tb_local.close()

        self.assertTrue(all(polIds < npols) and all(polIds > -1), 'PolarizationId in DATA_DESCRIPTION not consistent with POLARIZATION table')

        # Check that flagdata can run properly with output MS
        summary = flagdata(vis=self.outputms,mode='summary')
        self.assertTrue('correlation' in summary, 'Flagdata failure due to missformated MS')

class test_antenna_reindexing(test_base):
    '''Test to check proper reindex of subtables'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_CAS_7259()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = 'test_reindex_antenna_subtable.ms'

    def tearDown(self):
        # all test cases are expected to generate the output MS from mstransform in outvis
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf list.obs')

    def test_antenna_reindexing_va0(self):
        '''mstransform: check reindexing when selecting a subset of the antennas'''

        mstransform(vis=self.vis,outputvis=self.outvis,antenna='VA0*&&VA0*', datacolumn='all', reindex=True)

        listobs(self.outvis, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')
        tb_local.open(self.outvis+'/ANTENNA')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 9, 'ANTENNA subtable should be resized to 9 VA0* antennas')

    def test_antenna_reindexing_all_va(self):
        '''mstransform: check that reindexing keeps all antennas that are involved in the selected baselines'''

        mstransform(vis=self.vis,outputvis=self.outvis,antenna='VA01&&VA*', datacolumn='all', reindex=True)

        listobs(self.outvis, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')
        tb_local.open(self.outvis+'/ANTENNA')
        nrows = tb_local.nrows()
        tb_local.close()
        self.assertEqual(nrows, 27, 'ANTENNA subtable should contain all 27 VA* antennas')


class test_alma_wvr_correlation_products(test_base):
    '''Test behaviour of mstransform in split mode when the input MS contains ALMA WVR correlation products'''

    def setUp(self):

        self.setUp_CAS_6941()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_alma_wvr_correlation_products_1(self):

        self.outputms = 'test_alma_wvr_correlation_products_1.ms'

        mstransform(vis=self.vis,outputvis=self.outputms,spw='0,1,2',datacolumn='DATA')

        # Check that POLARIZATION sub-table is properly sorted
        tb_local.open(self.outputms + '/POLARIZATION')
        numCorr = tb_local.getcol('NUM_CORR')
        tb_local.close()

        self.assertEqual(numCorr[0],2,'POLARIZATION table miss-sorted')
        self.assertEqual(numCorr[1],1, 'POLARIZATION table miss-sorted')

        # Check that flagdata can run properly with output MS
        summary = flagdata(vis=self.outputms,mode='summary')
        self.assertTrue('correlation' in summary, 'Flagdata failure due to missformated MS')


class test_alma_autocorr_selection_with_wvr(test_base):
    '''Test behaviour of mstransform selecting correlation products that include WVR XX'''

    def setUp(self):

        self.setUp_CAS_6951()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_alma_autocorr_selection_with_wvr_1(self):

        self.outputms = 'test_alma_autocorr_selection_with_wvr_1.ms'

        mstransform(vis=self.vis,outputvis=self.outputms,correlation='XX;YY',datacolumn='DATA')

        # Check that POLARIZATION sub-table is properly sorted
        tb_local.open(self.outputms + '/POLARIZATION')
        numCorr = tb_local.getcol('NUM_CORR')
        tb_local.close()

        self.assertEqual(numCorr[0],2,'Incorrect number of correlations')
        self.assertEqual(numCorr[1],1, 'Incorrect number of correlations')


class test_spectrum_transformations_mean(test_base):
    '''Check that WEIGHT/SIGMA are equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)

    def test_chanavg_mean_corrected(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_chanavg_mean_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')


    def test_timeavg_mean_corrected(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_mean_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_mean_corrected(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_chanavg_mean_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_chanavg_mean_data(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_chanavg_mean_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_mean_data(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_mean_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_mean_data(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_chanavg_mean_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_chanavg_mean_model(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_chanavg_mean_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_mean_model(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_mean_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_mean_model(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_chanavg_mean_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_chanavg_mean_all(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_chanavg_mean_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_mean_all(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_mean_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_mean_all(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_timeavg_chanavg_mean_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_spw_separation_mean_corrected(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_spw_separation_mean_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    regridms=True, spw='0,1,2,3,4,5,6,7', nspw=4)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_spw_separation_mean_data(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_spw_separation_mean_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    regridms=True, spw='0,1,2,3,4,5,6,7', nspw=4)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_spw_separation_mean_model(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_spw_separation_mean_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    regridms=True, spw='0,1,2,3,4,5,6,7', nspw=4)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')

    def test_spw_separation_mean_all(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the mean of the unflagged WEIGHT_SPECTRUM samples'''

        self.outvis = 'test_spw_separation_mean_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    regridms=True, spw='0,1,2,3,4,5,6,7', nspw=4)

        tb_local.open(self.outvis)
        flag = tb_local.getcol("FLAG")
        weight  = tb_local.getcol("WEIGHT")
        weightSpectrum  = tb_local.getcol("WEIGHT_SPECTRUM")

        allSamplesFlagged = numpy.all(flag == True,1)
        weightSpectrumSamples = numpy.sum((flag == False),1) + allSamplesFlagged* numpy.sum(flag == True,1)
        weightSpectrumCumSum = numpy.sum(weightSpectrum*(flag==False),1) + allSamplesFlagged* numpy.sum(weightSpectrum*(flag == True),1)
        weightSpectrumMean = weightSpectrumCumSum / weightSpectrumSamples
        self.assertTrue((numpy.abs(weightSpectrumMean - weight ) < 1E-4).all(), 'WEIGHT is not mean of WEIGHT_SPECTRUM')


@unittest.skip('Median replaced with mean to capture overall behaviour')
class test_spectrum_transformations_median(test_base):
    '''Check that WEIGHT/SIGMA are equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM

    This test should not be run any longer:
    # jagonzal: Replace median with mean to capture overall behaviour
    # test_spectrum_transformations_median,
    '''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)

    def test_chanavg_median_corrected(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_chanavg_median_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_median_corrected(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_median_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        # jagonzal: SIGMA is not derived from the median of SIGMA_SPECTRUM but from WEIGHT turned into SIGMA by using 1/pow(weight
        # self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_chanavg_median_corrected(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_chanavg_median_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_chanavg_median_data(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_chanavg_median_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_median_data(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_median_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        # jagonzal: SIGMA is not derived from the median of SIGMA_SPECTRUM but from WEIGHT turned into SIGMA by using 1/pow(weight,2)
        #self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_chanavg_median_data(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_chanavg_median_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_chanavg_median_model(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_chanavg_median_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_median_model(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_median_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        # jagonzal: SIGMA is not derived from the median of SIGMA_SPECTRUM but from WEIGHT turned into SIGMA by using 1/pow(weight
        #self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_chanavg_median_model(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_chanavg_median_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_chanavg_median_all(self):
        '''mstransform: Check that after chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_chanavg_median_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_median_all(self):
        '''mstransform: Check that after time avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_median_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        # jagonzal: SIGMA is not derived from the median of SIGMA_SPECTRUM but from the median of the
        #           WEIGHT format of SIGMA_SPECTRUM turned into SIGMA by using 1/pow(weight,2)
        #self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_timeavg_chanavg_median_all(self):
        '''mstransform: Check that after time/chan avg WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_timeavg_chanavg_median_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_spw_separation_median_corrected(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_spw_separation_median_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    regridms=True, nspw=4)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_spw_separation_median_data(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_spw_separation_median_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    regridms=True, nspw=4)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_spw_separation_median_model(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_spw_separation_median_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    regridms=True, nspw=4)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')

    def test_spw_separation_median_all(self):
        '''mstransform: Check that after spw separation WEIGHT/SIGMA is equivalent to the median of WEIGHT_SPECTRUM/SIGMA_SPECTRUM'''

        self.outvis = 'test_spw_separation_median_all.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    regridms=True, nspw=4)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()
        weightSpectrumMedian = numpy.median(weightSpectrum,1)

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()
        sigmaSpectrumMedian = numpy.median(sigmaSpectrum,1)

        tb_local.open(self.outvis)
        weight = tb_local.getcol('WEIGHT')
        tb_local.close()

        tb_local.open(self.outvis)
        sigma = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumMedian - weight) < 1E-5).all(), 'WEIGHT is not median of WEIGHT_SPECTRUM')
        self.assertTrue((numpy.abs(sigmaSpectrumMedian - sigma) < 1E-5).all(), 'SIGMA is not median of SIGMA_SPECTRUM')


class test_spectrum_transformations_sigma_from_weight(test_base):
    '''Check that WEIGHT/SIGMA and WEIGHT_SPECTRUM/SIGMA_SPECTRUM follow the relation sigma = 1 sqrt(weight) '''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)

    def test_chanavg_sigma_from_weight_corrected(self):
        '''mstransform: Check that after chan avg CORRECTED SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_chanavg_sigma_from_weight_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_timeavg_sigma_from_weight_corrected(self):
        '''mstransform: Check that after time avg CORRECTED SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_timeavg_sigma_from_weight_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_sigma_from_weight_corrected(self):
        '''mstransform: Check that after time + chan avg CORRECTED SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_timeavg_chanavg_sigma_from_weight_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_chanavg_sigma_from_weight_data(self):
        '''mstransform: Check that after chan avg DATA SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_chanavg_sigma_from_weight_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_timeavg_sigma_from_weight_data(self):
        '''mstransform: Check that after time avg DATA SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_timeavg_sigma_from_weight_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_sigma_from_weight_data(self):
        '''mstransform: Check that after time + chan avg DATA SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_timeavg_chanavg_sigma_from_weight_data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_chanavg_sigma_from_weight_model(self):
        '''mstransform: Check that after chan avg MODEL SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_chanavg_sigma_from_weight_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_timeavg_sigma_from_weight_model(self):
        '''mstransform: Check that after time avg MODEL SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_timeavg_sigma_from_weight_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')

    def test_timeavg_chanavg_sigma_from_weight_model(self):
        '''mstransform: Check that after time + chan avg MODEL SIGMA=1/sqrt(WEIGHT)'''

        self.outvis = 'test_timeavg_chanavg_sigma_from_weight_model.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        (ncorr,nchan,nrow) = weightSpectrum.shape
        for row in range(0,nrow):
            for chan in range(0,nchan):
                for corr in range(0,ncorr):
                    weightSpectrum[corr,chan,row] = weighToSigma(weightSpectrum[corr,chan,row])

        tb_local.open(self.outvis)
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum - sigmaSpectrum) < 1E-4).all(), 'SIGMA_SPECTRUM not derived from WEIGHT_SPECTRUM')


class test_spectrum_transformations_2_steps_vs_1_step(test_base):
    '''Check that the result of chan/time average applied in 1 step is the same as applied in 2 steps'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.auxvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.auxvis)
        os.system('rm -rf '+ self.refvis)

    def test_timeavg_chanavg_2_steps_vs_1_step_corrected(self):
        '''mstransform: Check that the result of time/chan average CORRECTED applied in 1 step is the same as applied in 2 steps '''

        self.auxvis = 'test_timeavg_chanavg_2_steps_vs_1_step_corrected-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.outvis = 'test_timeavg_chanavg_2_steps_vs_1_step_corrected-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    # Also CORRECTED was already renamed to DATA so there is no need to use CORRECTED
                    datacolumn='DATA',
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_timeavg_chanavg_2_steps_vs_1_step_corrected-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_chanavg_timeavg_2_steps_vs_1_step_corrected(self):
        '''mstransform: Check that the result of chan/time average CORRECTED applied in 1 step is the same as applied in 2 steps '''

        self.auxvis = 'test_chanavg_timeavg_2_steps_vs_1_step_corrected-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.outvis = 'test_chanavg_timeavg_2_steps_vs_1_step_corrected-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    # Also CORRECTED was already renamed to DATA so there is no need to use CORRECTED
                    datacolumn='DATA',
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_2_steps_vs_1_step_corrected-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_timeavg_chanavg_2_steps_vs_1_step_data(self):
        '''mstransform: Check that the result of time/chan average DATA applied in 1 step is the same as applied in 2 steps '''

        self.auxvis = 'test_timeavg_chanavg_2_steps_vs_1_step_data-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.outvis = 'test_timeavg_chanavg_2_steps_vs_1_step_data-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    datacolumn='DATA',
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_timeavg_chanavg_2_steps_vs_1_step_data-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_chanavg_timeavg_2_steps_vs_1_step_data(self):
        '''mstransform: Check that the result of chan/time average DATA applied in 1 step is the same as applied in 2 steps '''

        self.auxvis = 'test_chanavg_timeavg_2_steps_vs_1_step_data-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.outvis = 'test_chanavg_timeavg_2_steps_vs_1_step_data-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    datacolumn='DATA',
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_2_steps_vs_1_step_data-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_timeavg_chanavg_2_steps_vs_1_step_model(self):
        '''mstransform: Check that the result of time/chan average MODEL applied in 1 step is the same as applied in 2 steps '''

        self.auxvis = 'test_timeavg_chanavg_2_steps_vs_1_step_model-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.outvis = 'test_timeavg_chanavg_2_steps_vs_1_step_model-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    # Also MODEL was already renamed to DATA so there is no need to use CORRECTED
                    datacolumn='DATA',
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_timeavg_chanavg_2_steps_vs_1_step_model-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_chanavg_timeavg_2_steps_vs_1_step_model(self):
        '''mstransform: Check that the result of chan/time average MODEL applied in 1 step is the same as applied in 2 steps '''

        self.auxvis = 'test_chanavg_timeavg_2_steps_vs_1_step_model-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.outvis = 'test_chanavg_timeavg_2_steps_vs_1_step_model-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    # Also MODEL was already renamed to DATA so there is no need to use CORRECTED
                    datacolumn='DATA',
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_2_steps_vs_1_step_model-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_chanavg_timeavg_2_steps_vs_1_step_all(self):
        '''mstransform: Check that the result of chan/time average ALL applied in 1 step is the same as applied in 2 steps'''

        self.auxvis = 'test_chanavg_timeavg_2_steps_vs_1_step_all-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.outvis = 'test_chanavg_timeavg_2_steps_vs_1_step_all-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    datacolumn='ALL',
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_2_steps_vs_1_step_all-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_timeavg_chanavg_2_steps_vs_1_step_all(self):
        '''mstransform: Check that the result of time/chan average ALL applied in 1 step is the same as applied in 2 steps'''

        self.auxvis = 'test_timeavg_chanavg_2_steps_vs_1_step_all-1st_step.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.outvis = 'test_timeavg_chanavg_2_steps_vs_1_step_all-2nd_step.ms'

        mstransform(vis=self.auxvis,outputvis=self.outvis,
                    # No need to set usewtspectrum=True because it already exists
                    datacolumn='ALL',
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_timeavg_chanavg_2_steps_vs_1_step_all-together.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s',
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

class test_spectrum_transformations_chanavg_useWeightSpectrum_false_vs_true(test_base):
    '''Check that WEIGHT/SIGMA and WEIGHT_SPECTRUM/SIGMA_SPECTRUM follow the relation sigma = 1 sqrt(weight)

    # jagonzal: mstransform has been optimized to not use weight spectrum for chan. avg.
    # DATA when there are no input SPECTRUM cols because VI/VB generates constant SPECTRUM
    '''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)

    def test_chanavg_useWeightSpectrum_false_vs_true_corrected(self):
        '''mstransform: When there are no input SPECTRUM cols chan avg result is the same regardless of useWeightSpectrum because VI/VB fills a cte. weightSpectrum/sigmaSpectrum across channels'''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_corrected-true.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_corrected-false.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        dataTrue = tb_local.getcol('DATA')
        tb_local.close()

        tb_local.open(self.refvis)
        dataFalse = tb_local.getcol('DATA')
        tb_local.close()

        self.assertTrue((numpy.abs(dataTrue - dataFalse) < 1E-6).all(), 'CORRECTED improperly averaged')

    def test_chanavg_useWeightSpectrum_false_vs_true_data(self):
        '''mstransform: When there are no input SPECTRUM cols chan avg result is the same regardless of useWeightSpectrum because VI/VB fills a cte. weightSpectrum/sigmaSpectrum across channels'''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_data-true.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_data-false.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        dataTrue = tb_local.getcol('DATA')
        tb_local.close()

        tb_local.open(self.refvis)
        dataFalse = tb_local.getcol('DATA')
        tb_local.close()

        self.assertTrue((numpy.abs(dataTrue - dataFalse) < 1E-6).all(), 'DATA improperly averaged')

    def test_chanavg_useWeightSpectrum_false_vs_true_model(self):
        '''mstransform: When there are no input SPECTRUM cols chan avg result is the same regardless of useWeightSpectrum because VI/VB fills a cte. weightSpectrum/sigmaSpectrum across channels'''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_model-true.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_model-false.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        dataTrue = tb_local.getcol('DATA')
        tb_local.close()

        tb_local.open(self.refvis)
        dataFalse = tb_local.getcol('DATA')
        tb_local.close()

        self.assertTrue((numpy.abs(dataTrue - dataFalse) < 1E-6).all(), 'MODEL improperly averaged')

    def test_chanavg_useWeightSpectrum_false_vs_true_all(self):
        '''mstransform: When there are no input SPECTRUM cols chan avg result is the same regardless of useWeightSpectrum because VI/VB fills a cte. weightSpectrum/sigmaSpectrum across channels'''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_all-true.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_all-false.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        cols = ['DATA','CORRECTED_DATA','MODEL_DATA']
        for col in cols:
            tb_local.open(self.outvis)
            dataTrue = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            dataFalse = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(dataTrue - dataFalse) < 1E-6).all(), col + ' improperly generated')


class test_spectrum_transformations_multiple_col(test_base):
    '''Check the result of multiple column operation vs single column operation'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.auxvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.auxvis)
        os.system('rm -rf '+ self.refvis)

    def test_chanavg_all_vs_data_and_corrected(self):
        '''mstransform: Check that the result of chan avg ALL is equivalent to the results of DATA/CORRECTED stand-alone'''

        self.outvis = 'test_chanavg_all_vs_data_and_corrected-data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.auxvis = 'test_chanavg_all_vs_data_and_corrected-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_all_vs_data_and_corrected-all.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrumData = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumData = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.auxvis)
        weightSpectrumCorrected = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumCorrected = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumAll = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumAll = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumAll-weightSpectrumCorrected) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrumAll-sigmaSpectrumData) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_timeavg_all_vs_data_and_corrected(self):
        '''mstransform: Check that the result of time avg ALL is equivalent to the results of DATA/CORRECTED stand-alone'''

        self.outvis = 'test_timeavg_all_vs_data_and_corrected-data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.auxvis = 'test_timeavg_all_vs_data_and_corrected-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_all_vs_data_and_corrected-all.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrumData = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumData = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.auxvis)
        weightSpectrumCorrected = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumCorrected = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumAll = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumAll = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumAll-weightSpectrumCorrected) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrumAll-sigmaSpectrumData) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_timeavg_chanavg_all_vs_data_and_corrected(self):
        '''mstransform: Check that the result of time+chan avg ALL is equivalent to the results of DATA/CORRECTED stand-alone'''

        self.outvis = 'test_timeavg_chanavg_all_vs_data_and_corrected-data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        self.auxvis = 'test_timeavg_chanavg_all_vs_data_and_corrected-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.auxvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_chanavg_all_vs_data_and_corrected-all.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrumData = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumData = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.auxvis)
        weightSpectrumCorrected = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumCorrected = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumAll = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumAll = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrumAll-weightSpectrumCorrected) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrumAll-sigmaSpectrumData) < 1E-4).all(), 'Sigma Spectrum improperly generated')


@unittest.skip('Skipping - chan average should generate the avg of the flagged data if all'
               'are flagged')
class test_spectrum_transformations_weight_zero_flag_set(test_base):
    '''mstransform: Check that flags are set when the resulting weight is zero'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)

    def test_timeavg_weight_zero_flag_set_corrected(self):
        '''mstransform: Check that flags are set when the weight resulting from time avg is zero'''

        self.outvis = 'test_timeavg_weight_zero_flag_set_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.outvis)
        flag = tb_local.getcol('FLAG')
        tb_local.close()

        indexZeroSpectrum = weightSpectrum < 1E-6
        indexFlagSet = flag == True
        self.assertTrue((indexZeroSpectrum == indexFlagSet).all(), 'WEIGHT_SPECTRUM not consistent with FLAG cube')

    def test_chanavg_weight_zero_flag_set_corrected(self):
        '''mstransform: Check that flags are set when the weight resulting from time avg is zero'''

        self.outvis = 'test_chanavg_weight_zero_flag_set_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.outvis)
        flag = tb_local.getcol('FLAG')
        tb_local.close()

        indexZeroSpectrum = weightSpectrum < 1E-6
        indexFlagSet = flag == True
        self.assertTrue((indexZeroSpectrum == indexFlagSet).all(), 'WEIGHT_SPECTRUM not consistent with FLAG cube')

    @unittest.skip('Skipping - chan average should generate the avg of the flagged data if '
                   'all are flagged')
    def test_timeavg_chanavg_weight_zero_flag_set_corrected(self):
        '''mstransform: Check that flags are set when the weight resulting from time avg is zero'''

        self.outvis = 'test_timeavg_chanavg_weight_zero_flag_set_corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.outvis)
        flag = tb_local.getcol('FLAG')
        tb_local.close()

        indexZeroSpectrum = weightSpectrum < 1E-6
        indexFlagSet = flag == True
        self.assertTrue((indexZeroSpectrum == indexFlagSet).all(), 'WEIGHT_SPECTRUM not consistent with FLAG cube')


class test_spectrum_transformations_weight_constant(test_base):
    '''mstransform: Check that the result of avg CORRECTED with constant WEIGHT'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)

    def test_timeavg_corrected_vs_model_with_weight_constant(self):
        '''mstransform: Check that the result of time/chan avg CORRECTED is the same as time avg MODEL multiplied by input WEIGHT'''

        self.outvis = 'test_timeavg_corrected_vs_model_with_weight_constant-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_corrected_vs_model_with_weight_constant-model.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        # Get input WEIGHT
        tb_local.open(self.vis)
        inputWeight = tb_local.getcol('WEIGHT')
        tb_local.close()
        # Transform WEIGHT_SPECTRUM from MODEL multipling by input WEIGHT
        weightSpectrumRef[0,:,:] = inputWeight[0,0]*weightSpectrumRef[0,:,:]
        weightSpectrumRef[1,:,:] = inputWeight[1,0]*weightSpectrumRef[1,:,:]
        weightSpectrumRef[2,:,:] = inputWeight[2,0]*weightSpectrumRef[2,:,:]
        weightSpectrumRef[3,:,:] = inputWeight[3,0]*weightSpectrumRef[3,:,:]

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')

    def test_chanavg_corrected_vs_model_with_weight_constant(self):
        '''mstransform: Check that the result of chan avg CORRECTED is the same as time avg MODEL multiplied by input WEIGHT'''

        self.outvis = 'test_chanavg_corrected_vs_model_with_weight_constant-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_corrected_vs_model_with_weight_constant-model.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        # Get input WEIGHT
        tb_local.open(self.vis)
        inputWeight = tb_local.getcol('WEIGHT')
        tb_local.close()
        # Transform WEIGHT_SPECTRUM from MODEL multipling by input WEIGHT
        weightSpectrumRef[0,:,:] = inputWeight[0,0]*weightSpectrumRef[0,:,:]
        weightSpectrumRef[1,:,:] = inputWeight[1,0]*weightSpectrumRef[1,:,:]
        weightSpectrumRef[2,:,:] = inputWeight[2,0]*weightSpectrumRef[2,:,:]
        weightSpectrumRef[3,:,:] = inputWeight[3,0]*weightSpectrumRef[3,:,:]

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')


class test_spectrum_transformations_sigma_unit(test_base):
    '''stransform: Check that the result of avg with sigma 1'''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)

    def test_timeavg_data_vs_model_with_sigma_unit(self):
        '''mstransform: Check that the result of time avg DATA with sigma 1 is the same as averaging MODEL'''

        self.outvis = 'test_timeavg_data_vs_model_with_sigma_unit-data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_data_vs_model_with_sigma_unit-model.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_chanavg_data_vs_model_with_sigma_unit(self):
        '''mstransform: Check that the result of chan avg DATA with sigma 1 is the same as averaging MODEL'''

        self.outvis = 'test_chanavg_data_vs_model_with_sigma_unit-data.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_data_vs_model_with_sigma_unit-model.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrum = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        sigmaSpectrumRef = tb_local.getcol('SIGMA_SPECTRUM')
        tb_local.close()

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')
        self.assertTrue((numpy.abs(sigmaSpectrum-sigmaSpectrumRef) < 1E-4).all(), 'Sigma Spectrum improperly generated')

    def test_timeavg_corrected_vs_data_with_sigma_unit(self):
        '''mstransform: Check that the result of time avg CORRECTED is the same as time avg DATA with sigma 1 multiplied by input WEIGHT'''

        self.outvis = 'test_timeavg_corrected_vs_data_with_sigma_unit-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_corrected_vs_data_with_sigma_unit-data.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        # Get input WEIGHT
        tb_local.open(self.vis)
        inputWeight = tb_local.getcol('WEIGHT')
        tb_local.close()
        # Transform WEIGHT_SPECTRUM from MODEL multipling by input WEIGHT
        weightSpectrumRef[0,:,:] = inputWeight[0,0]*weightSpectrumRef[0,:,:]
        weightSpectrumRef[1,:,:] = inputWeight[1,0]*weightSpectrumRef[1,:,:]
        weightSpectrumRef[2,:,:] = inputWeight[2,0]*weightSpectrumRef[2,:,:]
        weightSpectrumRef[3,:,:] = inputWeight[3,0]*weightSpectrumRef[3,:,:]

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')

    def test_chanavg_corrected_vs_data_with_sigma_unit(self):
        '''mstransform: Check that the result of chan avg CORRECTED is the same as time avg DATA with sigma 1 multiplied by input WEIGHT'''

        self.outvis = 'test_chanavg_corrected_vs_data_with_sigma_unit-corrected.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_corrected_vs_data_with_sigma_unit-data.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    antenna='0&&1',spw='0',timerange='<14:45:52.50', # Limit data selection to gurantee constant WEIGHT
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightSpectrum = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        tb_local.open(self.refvis)
        weightSpectrumRef = tb_local.getcol('WEIGHT_SPECTRUM')
        tb_local.close()

        # Get input WEIGHT
        tb_local.open(self.vis)
        inputWeight = tb_local.getcol('WEIGHT')
        tb_local.close()
        # Transform WEIGHT_SPECTRUM from MODEL multipling by input WEIGHT
        weightSpectrumRef[0,:,:] = inputWeight[0,0]*weightSpectrumRef[0,:,:]
        weightSpectrumRef[1,:,:] = inputWeight[1,0]*weightSpectrumRef[1,:,:]
        weightSpectrumRef[2,:,:] = inputWeight[2,0]*weightSpectrumRef[2,:,:]
        weightSpectrumRef[3,:,:] = inputWeight[3,0]*weightSpectrumRef[3,:,:]

        self.assertTrue((numpy.abs(weightSpectrum-weightSpectrumRef) < 1E-4).all(), 'Weight Spectrum improperly generated')


class test_spectrum_transformations_useWeightSpectrum_false_vs_true(test_base):
    '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.outvis = ''
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)

    def test_chanavg_useWeightSpectrum_false_vs_true_corrected (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_corrected-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_corrected-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_useWeightSpectrum_false_vs_true_data (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_data-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_data-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_useWeightSpectrum_false_vs_true_model (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_model-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_model-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_useWeightSpectrum_false_vs_true_all (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_useWeightSpectrum_false_vs_true_all-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=False,
                    chanaverage=True,chanbin=2)

        self.refvis = 'test_chanavg_useWeightSpectrum_false_vs_true_all-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_timeavg_useWeightSpectrum_false_vs_true_corrected (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_timeavg_useWeightSpectrum_false_vs_true_corrected-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=False,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_useWeightSpectrum_false_vs_true_corrected-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_timeavg_useWeightSpectrum_false_vs_true_data (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_timeavg_useWeightSpectrum_false_vs_true_data-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=False,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_useWeightSpectrum_false_vs_true_data-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_timeavg_useWeightSpectrum_false_vs_true_model (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_timeavg_useWeightSpectrum_false_vs_true_model-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=False,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_useWeightSpectrum_false_vs_true_model-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_timeavg_useWeightSpectrum_false_vs_true_all (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_timeavg_useWeightSpectrum_false_vs_true_all-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=False,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_timeavg_useWeightSpectrum_false_vs_true_all-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_timeavg_useWeightSpectrum_false_vs_true_corrected (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_corrected-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=False,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_corrected-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_timeavg_useWeightSpectrum_false_vs_true_data (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_data-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=False,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_data-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_timeavg_useWeightSpectrum_false_vs_true_model (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_model-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=False,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_model-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')

    def test_chanavg_timeavg_useWeightSpectrum_false_vs_true_all (self):
        '''Check that WEIGHT/SIGMA are generated in the same way regardless of the useWeightSpectrum parameter '''

        self.outvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_all-false.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=False,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        self.refvis = 'test_chanavg_timeavg_useWeightSpectrum_false_vs_true_all-true.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        tb_local.open(self.outvis)
        weightFalse = tb_local.getcol('WEIGHT')
        sigmaFalse = tb_local.getcol('SIGMA')
        tb_local.close()

        tb_local.open(self.refvis)
        weightTrue = tb_local.getcol('WEIGHT')
        sigmaTrue = tb_local.getcol('SIGMA')
        tb_local.close()

        self.assertTrue((numpy.abs(weightTrue - weightFalse) < 1E-6).all(), 'Error calculating WEIGHT')
        self.assertTrue((numpy.abs(sigmaTrue - sigmaFalse) < 1E-6).all(), 'Error calculating SIGMA')



class test_spectrum_transformations_flagged_average(test_base):
    '''Check that when all the samples are flagged the avg. and spectrum correspond to the avg./spectrum of all the flagged samples '''

    @classmethod
    def setUpClass(cls):
        cls.setUp_4ants()

    @classmethod
    def tearDownClass(cls):
        # Reuse self.vis for all tests in this class. It's input only
        os.system('rm -rf ' + cls.vis)

    def setUp(self):
        self.refvis = ''

    def tearDown(self):
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.refvis)

    def test_chanavg_spectrum_transformations_flagged_average_corrected (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_spectrum_transformations_flagged_average_corrected-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_spectrum_transformations_flagged_average_corrected-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2)


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_spectrum_transformations_flagged_average_data (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_spectrum_transformations_flagged_average_data-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_spectrum_transformations_flagged_average_data-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2)


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_spectrum_transformations_flagged_average_model (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_spectrum_transformations_flagged_average_model-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_spectrum_transformations_flagged_average_model-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_spectrum_transformations_flagged_average_all (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_spectrum_transformations_flagged_average_all-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_spectrum_transformations_flagged_average_all-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2)


        cols = ['CORRECTED_DATA','DATA','MODEL_DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_timeavg_spectrum_transformations_flagged_average_corrected (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_timeavg_spectrum_transformations_flagged_average_corrected-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_timeavg_spectrum_transformations_flagged_average_corrected-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    timeaverage=True,timebin='10s')


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_timeavg_spectrum_transformations_flagged_average_data (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_timeavg_spectrum_transformations_flagged_average_data-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_timeavg_spectrum_transformations_flagged_average_data-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    timeaverage=True,timebin='10s')


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_timeavg_spectrum_transformations_flagged_average_model (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_timeavg_spectrum_transformations_flagged_average_model-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_timeavg_spectrum_transformations_flagged_average_model-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_timeavg_spectrum_transformations_flagged_average_all (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_timeavg_spectrum_transformations_flagged_average_all-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_timeavg_spectrum_transformations_flagged_average_all-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    timeaverage=True,timebin='10s')


        cols = ['CORRECTED_DATA','DATA','MODEL_DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_timeavg_spectrum_transformations_flagged_average_corrected (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_corrected-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_corrected-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='CORRECTED',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_timeavg_spectrum_transformations_flagged_average_data (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_data-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_data-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='DATA',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_timeavg_spectrum_transformations_flagged_average_model (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_model-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_model-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='MODEL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')


        cols = ['DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')

    def test_chanavg_timeavg_spectrum_transformations_flagged_average_all (self):

        # Flag the entire MS
        flagdata(self.vis, flagbackup=False, mode='manual')

        self.outvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_all-flagged.ms'

        mstransform(vis=self.vis,outputvis=self.outvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')

        # Unflag the entire MS
        flagdata(self.vis, flagbackup=False, mode='unflag')

        self.refvis = 'test_chanavg_timeavg_spectrum_transformations_flagged_average_all-unflagged.ms'

        mstransform(vis=self.vis,outputvis=self.refvis,
                    datacolumn='ALL',usewtspectrum=True,
                    chanaverage=True,chanbin=2,
                    timeaverage=True,timebin='10s')


        cols = ['CORRECTED_DATA','DATA','MODEL_DATA','WEIGHT','SIGMA','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        for col in cols:
            tb_local.open(self.outvis)
            testCol = tb_local.getcol(col)
            tb_local.close()
            tb_local.open(self.refvis)
            refCol = tb_local.getcol(col)
            tb_local.close()
            self.assertTrue((numpy.abs(testCol - refCol) < 1E-6).all(), col + ' improperly generated')


class test_otf_calibration(test_base_compare):
    '''Check that corrected data produce otf by mstransform is the same as produced otf by applycal'''

    def tearDown(self):
        super(test_channelAverageByDefault,self).tearDown()

    def setUp(self):

        super(test_otf_calibration,self).setUp()

        if os.path.exists('mstransform_reference'): os.system('rm -rf ' + 'mstransform_reference')
        os.system('cp -RL {0} .'.format(os.path.join(datapath, 'mstransform_reference')))

        self.vis = 'mstransform_reference/ngc5921.ms'
        self.outvis = 'mst_otf_calibration.ms'
        self.refvis = 'applycal_split_otf_calibration.ms'
        self.outvis_sorted = 'mst_otf_calibration-sorted.ms'
        self.refvis_sorted = 'applycal_split_otf_sorted.ms'
        self.auxfile = 'mstransform_reference/ngc5921_callib.txt'

    def tearDown(self):

        super(test_otf_calibration,self).tearDown()

        os.system('rm -rf '+ 'mstransform_reference')

    def test_otf_calibration_mst_vs_applycal_split(self):

        mstransform(vis=self.vis,outputvis=self.outvis,docallib=True,callib=self.auxfile)

        applycal(vis=self.vis,docallib=True,callib=self.auxfile)
        split(vis=self.vis,outputvis=self.refvis,datacolumn='CORRECTED')

        self.generate_tolerance_map()

        # This test case runs other tasks (applycal/callib) on the input MS, modifying its history
        self.post_process(check_history=False)

class test_no_reindexing(test_base_compare):
    '''Test using no-reindexing feature'''

    def setUp(self):
        super(test_no_reindexing,self).setUp()
        self.setUp_sub_tables_alma()
        self.outvis = 'test_no_reindexing_noreidnex.ms'
        self.refvis = 'test_no_reindexing_reindex.ms'
        self.outvis_sorted = self.outvis
        self.refvis_sorted = self.refvis
        os.system('rm -rf test_no_reindexing_*')

    def tearDown(self):
        super(test_no_reindexing,self).tearDown()

    def test_regrid_SPWs_separately_with_no_reindexing(self):
        '''mstransform: Change ref. frame to LSRK for each SPW separately w/o reindexing'''

        mstransform(vis=self.vis,outputvis=self.outvis,regridms=True,datacolumn='ALL',correlation='XX',
                    field='SXDF-NB1006-4',spw='1:10~20,2:30~40',outframe='lsrk',reindex=False)
        mstransform(vis=self.vis,outputvis=self.refvis,regridms=True,datacolumn='ALL',correlation='XX',
                    field='SXDF-NB1006-4',spw='1:10~20,2:30~40',outframe='lsrk')

        listobs(self.outvis, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

        self.excludecols = ['DATA_DESC_ID','FIELD_ID','FLAG_CATEGORY','WEIGHT_SPECTRUM','SIGMA_SPECTRUM']
        self.generate_tolerance_map()
        self.compare_main_table_columns()


class test_no_reindexing_ephemeris_copy(test_base):
    #Test copying ephemeris table using no-reindexing feature CAS-8618
    def setUp(self):
        self.outvis = 'split_ephemeris_no_reindex.ms'
        self.splitvis = 'split_ephemeris_no_reindex.split.ms'
        self.asdm = 'uid___A002_X997a62_X8c-short'
        os.system('cp -RL {0} {1}'.format(os.path.join(datapath, self.asdm), self.asdm))

        importasdm(self.asdm, vis=self.outvis, convert_ephem2geo=True, process_pointing=False, flagbackup=False)
        if os.path.isfile(os.path.join(datapath,self.asdm+".tar.xz")):
            os.system('tar -Jxf {0}'.format(os.path.join(datapath,
                                                         self.asdm+".tar.xz")))
        else:
            os.system('cp -RL {0} {1}'.format(os.path.join(datapath, self.asdm),
                                              self.asdm))

    def tearDown(self):
        os.system('rm -rf '+ self.asdm)
        os.system('rm -rf '+ self.outvis)
        os.system('rm -rf '+ self.splitvis)

    def test_ephemeris_copy(self):
        mstransform(self.outvis, outputvis=self.splitvis, datacolumn='DATA', reindex=False)
        eph = glob.glob(os.path.join(self.splitvis, 'FIELD', 'EPHEM*.tab'))
        self.assertEqual(len(eph), 2)
        for e in eph:
            tb_local.open(e)
            tb_local.close()

        os.system('rm -rf '+ self.splitvis)

        mstransform(self.outvis, outputvis=self.splitvis, datacolumn='DATA', reindex=False, field='0')
        eph = glob.glob(os.path.join(self.splitvis, 'FIELD', 'EPHEM*.tab'))
        self.assertEqual(len(eph), 2)
        for e in eph:
            tb_local.open(e)
            tb_local.close()


class test_splitUpdateFlagCmd(test_base):

    def setUp(self):
        self.setUp_flags()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf list.obs')

    def test_split_update_flagcmd(self):
        '''split: Do not update FLAG_CMD table when spw selection in FLAG_CMD is by name'''
        self.outputms = 'mst_split_spwName.ms'
        mstransform(vis=self.vis, outputvis=self.outputms, spw='1,2', datacolumn='data')
        flagcmd(self.outputms, action='list', savepars=True, outfile='mstspwnames.txt', useapplied=True)
        self.assertTrue(filecmp.cmp(self.flagfile, 'mstspwnames.txt',1))

class test_selectiononly_notransformation(test_base):
    def setUp(self):
        self.outputms = "selectiononly_notransformation.ms"
        self.setUp_CAS_6951()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)

    def test_select_several_channels_different_spw(self):
        '''mstransform: apply a selection of different channels over several spw'''
        '''See CAS-10596, CAS-11087 for cases in which this went wrong'''
        mstransform(vis=self.vis, outputvis=self.outputms, spw='1:5;10;15,3:5;10;15,5:5;10;15', scan='1', datacolumn='data', reindex=True)
        # Verify that some sub-tables are properly re-indexed.
        spw_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'SPECTRAL_WINDOW_ID')
        self.assertEqual(spw_col.keys().__len__(), 3, 'Wrong number of rows in DD table')
        self.assertEqual(spw_col['r1'][0], 0,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r2'][0], 1,'Error re-indexing DATA_DESCRIPTION table')
        self.assertEqual(spw_col['r3'][0], 2,'Error re-indexing DATA_DESCRIPTION table')


# Cleanup class
class Cleanup(test_base):

    def tearDown(self):
        os.system('rm -rf ngc5921.*ms* jupiter6cm.demo*')
        os.system('rm -rf Four_ants_3C286.*ms* g19_d2usb_targets*')
        os.system('rm -rf comb*.*ms* reg*.*ms hann*.*ms favg*.*ms')
        os.system('rm -rf split*.*ms')
        os.system('rm -rf 3c84scan1*ms* test.mms')
        os.system('rm -rf donotkeepflags*')

    def test_runTest(self):
        '''mstransform: Cleanup'''
        pass

if __name__ == '__main__':
    unittest.main()
