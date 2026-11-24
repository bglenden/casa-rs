#########################################################################
# test_task_gencal.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.gencal.html
#
##########################################################################
import contextlib
import csv
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch
import uuid

import numpy as np

from casatestutils import testhelper as th

from casatasks import gencal, rmtables
from casatasks.private import tec_maps
from casatools import ctsys, table

_tb = table()

datapath = ctsys.resolve('/unittest/gencal/')

# input data
evndata = 'n08c1.ms'
vlbadata = 'ba123a.ms'
swpowdata = '3C286_syspower_CAS-11860.ms'

vlbacaltabRef = os.path.join(datapath, 'ba123a.gc')
evncalRef = os.path.join(datapath, 'n08c1.tsys')

caltab = 'cal.A'
evncopy = 'evn_copy.ms'
vlbacopy = 'vlba_copy.ms'
swpowcopy = 'swpow_copy.ms'

# these are for test_gainCurveVLA
vladata = 'tdem0003gencal.ms'
vlacopy = 'vla_copy.ms'
vlacaltabRef = os.path.join(datapath, 'gencalGaincurveRef.gc')



'''
Unit tests for gencal
'''
#
# ToDo:
# add more tests
# once more independent tests (e.g. comparison
# the AIPS REWAY results) add reference mses
# and do tests against them
#

# Pick up alternative data directory to run tests on MMSs
testmms = False
if 'TEST_DATADIR' in os.environ:
    DATADIR = str(os.environ.get('TEST_DATADIR'))+'/gencal/'
    if os.path.isdir(DATADIR):
        testmms = True
        datapath = DATADIR
    else:
        print('WARN: directory '+DATADIR+' does not exist')

print('gencal tests will use data from ' + datapath)


class gencal_antpostest(unittest.TestCase):

    # Input and output names
    msfile = 'tdem0003gencal.ms'
    # used for test_antpos_auto_evla_CAS13057
    msfile2 = 'auto_antposcorr_evla_gencal.ms'
#    if testmms:
#        msfile = 'tdem0003gencal.mms'
    caltable = 'anpos.cal'
    reffile1 = os.path.join(datapath+'evla_reference/', 'anpos.manual.cal')
    reffile2 = os.path.join(datapath+'evla_reference/', 'anpos.auto.cal')
    reffile3 = os.path.join(datapath+'evla_reference/', 'anpos.autoCAS13057.cal')
    res = False

    def setUp(self):
        if (os.path.exists(self.msfile)):
            shutil.rmtree(self.msfile)
        if (os.path.exists(self.msfile2)):
            shutil.rmtree(self.msfile2)

        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile, symlinks=True)
        shutil.copytree(os.path.join(datapath, self.msfile2), self.msfile2, symlinks=True)

    def tearDown(self):
        if (os.path.exists(self.msfile)):
            shutil.rmtree(self.msfile)
        if (os.path.exists(self.msfile2)):
            shutil.rmtree(self.msfile2)

        shutil.rmtree(self.caltable, ignore_errors=True)

    def test_antpos_manual(self):
        """Test manual antenna position correction."""
        gencal(vis=self.msfile,
               caltable=self.caltable,
               caltype='antpos',
               antenna='ea12,ea22',
               parameter=[-0.0072, 0.0045, -0.0017, -0.0220, 0.0040, -0.0190])

        self.assertTrue(os.path.exists(self.caltable))

        # ToDo:check generated caltable. Wait for new caltable

        # Compare with reference file from the repository
        reference = self.reffile1
        self.assertTrue(th.compTables(self.caltable, reference, ['WEIGHT', 'OBSERVATION_ID']))

    def test_antpos_auto_evla(self):
        """Test automated antenna position correction."""
        # check if the URL is reachable
        from urllib.request import urlopen
        from urllib.error import URLError

        # current EVLA baseline correction URL
        evlabslncorrURL = "http://www.vla.nrao.edu/cgi-bin/evlais_blines.cgi?Year="
        try:
            urlaccess = urlopen(evlabslncorrURL+"2010", timeout=60.0)
            gencal(vis=self.msfile,
                   caltable=self.caltable,
                   caltype='antpos',
                   antenna='')

            self.assertTrue(os.path.exists(self.caltable))

            # ToDo: check for generated caltable

            # Compare with reference file from the repository
            reference = self.reffile2
            self.assertTrue(th.compTables(self.caltable, reference, ['WEIGHT', 'OBSERVATION_ID']))

        except URLError as err:
            print("Cannot access %s , skip this test" % evlabslncorrURL)
            self.res = True

    def test_antpos_auto_evla_CAS13057(self):
        """
        gencal: test a bugfix of CAS-13057 for automated antenna position correction
        """
        # check if the URL is reachable
        from urllib.request import urlopen
        from urllib.error import URLError

        # current EVLA baseline correction URL
        evlabslncorrURL = "http://www.vla.nrao.edu/cgi-bin/evlais_blines.cgi?Year="
        try:
            urlaccess = urlopen(evlabslncorrURL+"2019", timeout=60.0)
            gencal(vis=self.msfile2,
                   caltable=self.caltable,
                   caltype='antpos',
                   antenna='')

            self.assertTrue(os.path.exists(self.caltable))
            # Compare with reference file from the repository
            # CAS-13940 - as the correction values are accumulated, running the test at later time
            # may cause the values to deviate from the time of reference caltable generation.
            # For this specific data set, with antenna 28 on the same pad for a long timespan such
            # situation can occur. So here do the comparison of the caltables skipping the correction
            reference = self.reffile3
            self.assertTrue(th.compTables(self.caltable, reference, ['WEIGHT', 'OBSERVATION_ID', 'FPARAM']))

            # now just compare antennas 23 and 25 entries for FPARAM...
            # row 21 (=ant 23) and 23 (= ant 25)
            _tb.open(self.caltable)
            curfparam=_tb.getcol('FPARAM').transpose()
            _tb.close()
            _tb.open(reference)
            reffparam=_tb.getcol('FPARAM').transpose()
            _tb.close()
            self.assertTrue((curfparam[21]==reffparam[21]).all())
            self.assertTrue((curfparam[23]==reffparam[23]).all())

        except URLError as err:
            print("Cannot access %s , skip this test" % evlabslncorrURL)
            self.res = True

    def test_antpos_manual_time_limit_evla(self):
        """
        gencal: test if time limit sets cutoff date for antpos corrections
        """
        # Mechanical test if time limit functions as expected, very short limit
        gencal(vis=self.msfile2,
               caltable=self.caltable,
               caltype='antpos',
               ant_pos_time_limit=400)

        _tb.open(self.caltable)
        res = np.mean(_tb.getcol('FPARAM'))
        _tb.close()

        self.assertTrue(np.isclose(res, -1.2345658040341035e-06, atol=1e-5))

        shutil.rmtree(self.caltable)

        # Test again with no time limit/ ant_pos_time_limit = 0
        gencal(vis=self.msfile2,
               caltable=self.caltable,
               caltype='antpos',
               ant_pos_time_limit=0)

        _tb.open(self.caltable)
        res = np.mean(_tb.getcol('FPARAM'))
        _tb.close()

        self.assertTrue(np.isclose(res, -5.308641580703818e-05, atol=1e-5))


class test_gencal_antpos_alma(unittest.TestCase):
    """Tests the automatic generation of antenna position corrections for ALMA.
       This test exercises the creation of an antenna calibration table from
       a JSON file presumably obtained with task getantposalma"""

    ALMA_MODIFIED_POINT_SOURCE_MS = 'uid___A002_Xdbc154_X50bd_modified_point_source.ms'
    ALMA_ANTENNA_FAKE_POSITIONS = 'antenna_fake_positions.json'
    ALMA_ANTENNA_NON_EXISTING = 'antenna_non_existing.json'
    ALMA_ANTENNA_EMPTY = 'antenna_empty.json'
    ALMA_ANTENNA_NOTALMA = 'antenna_notalma.json'
    CAL_TYPE = 'antpos'
    OUT_CALTABLE = 'uid___A002_Xdbc154_X50bd_modified_point_source.ms.cal'
    REF_CALTABLE = os.path.join(datapath, 'alma_reference/uid___A002_Xdbc154_X50bd_antpos_reference.cal')
    IGNORE_COLS = ['WEIGHT']
    TOLERANCE_PHASE = 1e-5

    def setUp(self):
        if (os.path.exists(self.ALMA_MODIFIED_POINT_SOURCE_MS)):
            shutil.rmtree(self.ALMA_MODIFIED_POINT_SOURCE_MS)

        shutil.copytree(os.path.join(datapath, self.ALMA_MODIFIED_POINT_SOURCE_MS),
                        self.ALMA_MODIFIED_POINT_SOURCE_MS, symlinks=True)
        if (os.path.exists(self.OUT_CALTABLE)):
            shutil.rmtree(self.OUT_CALTABLE)

        self.create_fake_antenna_json()

    def tearDown(self):
        if (os.path.exists(self.ALMA_MODIFIED_POINT_SOURCE_MS)):
            shutil.rmtree(self.ALMA_MODIFIED_POINT_SOURCE_MS)
        if (os.path.exists(self.OUT_CALTABLE)):
            shutil.rmtree(self.OUT_CALTABLE)
        if (os.path.exists(self.ALMA_ANTENNA_FAKE_POSITIONS)):
            os.remove(self.ALMA_ANTENNA_FAKE_POSITIONS)
        if (os.path.exists(self.ALMA_ANTENNA_NON_EXISTING)):
            os.remove(self.ALMA_ANTENNA_NON_EXISTING)
        if (os.path.exists(self.ALMA_ANTENNA_EMPTY)):
            os.remove(self.ALMA_ANTENNA_EMPTY)
        if (os.path.exists(self.ALMA_ANTENNA_NOTALMA)):
            os.remove(self.ALMA_ANTENNA_NOTALMA)

    def create_fake_antenna_json(self):
        """ Creates JSON files with some modified positions"""
        antenna_json_map = {'data': {'DA57': [2225188.291082358, -5440190.333712143, -2481301.9766639145], 'DA56': [2224943.8476697714, -5439974.666278855, -2482014.485205257], 'DA55': [2225052.614710198, -5440046.804298845, -2481737.049418094], 'DA54': [2225287.7654479267, -5439952.665663545, -2481718.7996364096], 'DA51': [2225085.7614180557, -5440062.100071216, -2481674.2369584036], 'DA50': [2225029.5940700597, -5440081.848538211, -2481682.2747704606], 'DV11': [2225093.791947928, -5440090.106591557, -2481604.305180936], 'DV12': [2225196.572533947, -5439865.589890478, -2482003.1711576893], 'DV13': [2225090.6956029134, -5440083.263062006, -2481622.447347566], 'DV14': [2224759.171832559, -5440069.4735431, -2481944.5729209166], 'DV15': [2225070.98396082, -5440031.731295895, -2481752.2387204156], 'DV16': [2224942.993180323, -5440088.424202303, -2481748.384453158], 'DV17': [2225074.4229963943, -5440002.263817599, -2481815.3772309585], 'DV19': [2225269.6690117344, -5439908.284361401, -2481832.203728896], 'DA48': [2225270.7369890315, -5440073.089656675, -2481471.418744986], 'DA46': [2225024.5299670226, -5440089.535013906, -2481670.03996153], 'DA45': [2225082.2254641117, -5440048.017236954, -2481708.046694106], 'DA44': [2225070.07414655, -5440067.186890746, -2481677.1332974513], 'DA43': [2225075.354086505, -5440059.362042112, -2481689.4740548218], 'DA65': [2224981.097100461, -5440131.251717349, -2481621.066842173], 'DA42': [2225053.230759497, -5440093.368435207, -2481635.630547281], 'DA64': [2225119.129949429, -5440069.216680659, -2481628.004886443], 'DA63': [2224948.593711863, -5440040.069551125, -2481852.6256770953], 'DA62': [2225193.4484411315, -5439993.761547387, -2481722.5395953925], 'DA61': [2224774.742715025, -5440235.548074935, -2481577.8152244966], 'DA60': [2225109.1404872905, -5440027.983339809, -2481726.421739189], 'DV20': [2225199.2537456006, -5440058.162041458, -2481571.8029956906], 'DV22': [2224946.2488416233, -5440207.495062985, -2481489.4745832346], 'DV01': [2225031.876371435, -5440052.000290567, -2481745.463977669], 'DV23': [2225069.766534102, -5440092.184458731, -2481621.6963312705], 'DV02': [2225088.4062740593, -5440026.489261174, -2481746.862259024], 'DV24': [2225078.2504491988, -5440185.64380042, -2481414.9495449723], 'DV25': [2225376.499595644, -5439991.419010953, -2481543.23362462], 'DV04': [2225064.8108568713, -5440109.239943672, -2481588.4482270624], 'DV05': [2225117.8101813397, -5440052.283765855, -2481665.80127241], 'DV06': [2225010.2945872336, -5440077.490336823, -2481707.649867947], 'DV07': [2225113.7092840783, -5440059.309074434, -2481653.1234815717], 'DV08': [2225095.82453017, -5440034.295320967, -2481723.2508713044], 'DV09': [2225176.481477592, -5439963.820396381, -2481800.5291207368], 'DA59': [2224910.667195755, -5440129.817686593, -2481689.08632993], 'DA58': [2224799.014454588, -5440161.72949903, -2481726.2117059752]}, 'metadata': {'caltype': 'ALMA antenna positions', 'description': 'ALMA ITRF antenna positions in meters', 'product_code': 'antposalma', 'outfile': 'test.json', 'hosts': ['https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa/'], 'asdm': 'uid://A002/Xdbc154/X50bd', 'search': 'both_latest', 'successful_url': 'manual test data', 'timestamp': '2024-05-07 15:01:28.954651'}}
        import json
        with open(self.ALMA_ANTENNA_FAKE_POSITIONS, 'w') as f:
            json.dump(antenna_json_map, f)

        antenna_json_map = {'data': {'NON_EXISTENT': [2225188.291082358, -5440190.333712143, -2481301.9766639145], 'DA56': [2224943.8476697714, -5439974.666278855, -2482014.485205257]}, 'metadata': {'caltype': 'ALMA antenna positions', 'description': 'ALMA ITRF antenna positions in meters', 'product_code': 'antposalma', 'outfile': 'test.json', 'hosts': ['https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa/'], 'asdm': 'uid://A002/Xdbc154/X50bd', 'search': 'both_latest', 'successful_url': 'manual test data', 'timestamp': '2024-05-07 15:01:28.954651'}}
        import json
        with open(self.ALMA_ANTENNA_NON_EXISTING, 'w') as f:
            json.dump(antenna_json_map, f)

        antenna_json_map = {'data': {}, 'metadata': {'caltype': 'ALMA antenna positions', 'description': 'ALMA ITRF antenna positions in meters', 'product_code': 'antposalma', 'outfile': 'test.json', 'hosts': ['https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa/'], 'asdm': 'uid://A002/Xdbc154/X50bd', 'search': 'both_latest', 'successful_url': 'manual test data', 'timestamp': '2024-05-07 15:01:28.954651'}}
        import json
        with open(self.ALMA_ANTENNA_EMPTY, 'w') as f:
            json.dump(antenna_json_map, f)

        antenna_json_map = {'data': {}, 'metadata': {'caltype': 'ALMA antenna positions', 'description': 'ALMA ITRF antenna positions in meters', 'product_code': 'otherproduct', 'outfile': 'test.json', 'hosts': ['https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa/'], 'asdm': 'uid://A002/Xdbc154/X50bd', 'search': 'both_latest', 'successful_url': 'manual test data', 'timestamp': '2024-05-07 15:01:28.954651'}}
        import json
        with open(self.ALMA_ANTENNA_NOTALMA, 'w') as f:
            json.dump(antenna_json_map, f)

    def test_antpos_alma_fake_positions(self) :
        """This test uses a MS that has been hand-crafted to be a
           point source model distorted with the known phases
           that a displacement in the antenna positions would cause.
           Then, the right positions are used in gencal to generate
           a calibration table. Those positions are input in a JSON
           file created by create_fake_antenna_json()."""

        # Form antpos caltable from faked json file
        gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
            caltable=self.OUT_CALTABLE,
            caltype=self.CAL_TYPE,
            infile=self.ALMA_ANTENNA_FAKE_POSITIONS)

        # Test values in caltable to be the same as a reference calibration
        # table used by the ALMA pipeline (self.REF_CALTABLE)
        from casatestutils import testhelper as th
        self.assertTrue(th.compTables(self.REF_CALTABLE,
            self.OUT_CALTABLE,
            self.IGNORE_COLS))

        # Apply the antpos caltable
        from casatasks import applycal
        applycal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
            gaintable=[self.OUT_CALTABLE],
            flagbackup=False)

        # Test values to ensure that all CORRECTED_DATA phases ~zero
        tb=table()
        tb.open(self.ALMA_MODIFIED_POINT_SOURCE_MS)
        cdph=np.absolute(np.angle(tb.getcol('CORRECTED_DATA')))
        tb.close()
        self.assertTrue(np.all(cdph<1e-5))

    def test_antpos_alma_non_existing_antenna(self) :
        """This test checks that an exception is thrown
           if the name of any of the antennas in the JSON file
           is not found in the MS"""

        # Call gencal with a JSON that has a non-existing antenna
        with self.assertRaises(ValueError) :
            gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
                caltable=self.OUT_CALTABLE,
                caltype=self.CAL_TYPE,
                infile=self.ALMA_ANTENNA_NON_EXISTING)

    def test_antpos_alma_empty(self) :
        """This test checks that an exception is thrown
           if the name of any of the antennas in the JSON file
           is not found in the MS"""

        # Call gencal with a JSON that has no antennas
        with self.assertRaises(ValueError) :
            gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
                caltable=self.OUT_CALTABLE,
                caltype=self.CAL_TYPE,
                infile=self.ALMA_ANTENNA_EMPTY)

    def test_antpos_alma_overspecify(self) :
        """This test checks when the JSON file is used there are
           no other parameters being set like the antenna parameter"""

        # Call gencal with infile and antenna should fail
        with self.assertRaises(ValueError) :
            gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
                caltable=self.OUT_CALTABLE,
                caltype=self.CAL_TYPE,
                infile=self.ALMA_ANTENNA_FAKE_POSITIONS,
                antenna='DA57,DA56')

        # Call gencal with infile and pol should fail
        with self.assertRaises(ValueError) :
            gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
                caltable=self.OUT_CALTABLE,
                caltype=self.CAL_TYPE,
                infile=self.ALMA_ANTENNA_FAKE_POSITIONS,
                pol='R,L')

        # Call gencal with infile and parameter should fail
        with self.assertRaises(ValueError) :
            gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
                caltable=self.OUT_CALTABLE,
                caltype=self.CAL_TYPE,
                infile=self.ALMA_ANTENNA_FAKE_POSITIONS,
                parameter=[0.01,0.02,0.03, -0.03,-0.01,-0.02])

    def test_antpos_alma_notalma(self) :
        """This test checks when the JSON file is used there are
           no other parameters being set like the antenna parameter"""

        # Call gencal with a JSON that does not have product_code = "antposalma"
        with self.assertRaises(ValueError) :
            gencal(vis=self.ALMA_MODIFIED_POINT_SOURCE_MS,
                caltable=self.OUT_CALTABLE,
                caltype=self.CAL_TYPE,
                infile=self.ALMA_ANTENNA_NOTALMA)


class gencal_test_tec_vla(unittest.TestCase):

    # Input and output names
    msfile = 'tdem0003gencal.ms'
    igsfile = 'igsg1160.10i'
    tecfile = msfile+'.IGS_TEC.im'
    rmstecfile = msfile+'.IGS_RMS_TEC.im'
    caltable = msfile+'_tec.cal'
    newigsfile='IGS0OPSFIN_20233350000_01D_02H_GIM.INX'

    # NEAL: Please check that these setUp and tearDown functions are ok

    def setUp(self):
        self.tearDown()
        shutil.copytree(os.path.join(datapath, self.msfile), self.msfile, symlinks=True)

    def tearDown(self):
        if os.path.exists(self.msfile):
            shutil.rmtree(self.msfile)

        if os.path.exists(self.igsfile):
            os.remove(self.igsfile)

        shutil.rmtree(self.tecfile, ignore_errors=True)
        shutil.rmtree(self.rmstecfile, ignore_errors=True)
        shutil.rmtree(self.caltable, ignore_errors=True)

        # this file is created by a successful test
        if os.path.exists(self.newigsfile):
            os.remove(self.newigsfile)

    def test_tec_maps(self):
        """
        gencal: very basic test of tec_maps and gencal(caltype='tecim')
        """

        try:
            tec_maps.create0(self.msfile)
            gencal(vis=self.msfile, caltable=self.caltable, caltype='tecim', infile=self.msfile+'.IGS_TEC.im')

            self.assertTrue(os.path.exists(self.caltable))

            _tb.open(self.caltable)
            nrows = _tb.nrows()
            dtecu = abs(13.752-np.mean(_tb.getcol('FPARAM'))/1e16)
            _tb.close()

            # print(str(nrows)+' '+str(dtecu))

            self.assertTrue(nrows == 1577)
            self.assertTrue(dtecu < 1e-3)

            # Test new CDDIS filename convention
            #  (file with correct name is retrieved and uncompressed)
            #  (CAS-14219, CAS-14192)
            #  (tec_maps.create0 above tests the old filename convention)
            a=tec_maps.get_IGS_TEC('2023/12/01')
            self.assertTrue(os.path.exists(self.newigsfile))
            self.assertTrue(a[9]=='IGS_Final_Product')

        except:
            # should catch case of internet access failure?
            raise


class gencal_gaincurve_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        shutil.copytree(os.path.join(datapath, vladata), vlacopy)
        shutil.copytree(os.path.join(datapath, evndata), evncopy)
        shutil.copytree(os.path.join(datapath, vlbadata), vlbacopy)

    def setUp(self):
        pass

    def tearDown(self):
        rmtables(caltab)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(vlacopy)
        shutil.rmtree(evncopy)
        shutil.rmtree(vlbacopy)

    def test_gainCurveVLA(self):
        ''' Test calibration table produced when gencal is run on a *VLA* MS and relying on data/nrao/VLA/GainCurves '''

        gencal(vis=vlacopy, caltable=caltab, caltype='gc')

        self.assertTrue(os.path.exists(caltab))    # file was created
        self.assertTrue(th.compTables(caltab,vlacaltabRef, ['WEIGHT']))

    def test_gainCurveVLBA(self):
        ''' Test calibration table produced when gencal is run on a VLBA MS with an internal GAIN_CURVE table '''

        gencal(vis=vlbacopy, caltable=caltab, caltype='gc')

        self.assertTrue(os.path.exists(caltab))    # file was created
        self.assertTrue(th.compTables(caltab, vlbacaltabRef, ['WEIGHT']))

    def test_noGainCurveEVN(self):
        ''' Test that when gencal is run on an EVN MS with no GAIN_CURVE table it creates no calibration table '''

        try:
            gencal(vis=evncopy, caltable=caltab, caltype='gc')
        except:
            pass

        self.assertFalse(os.path.exists(caltab))


class gencal_tsys_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.copytree(os.path.join(datapath, evndata), evncopy)
        shutil.copytree(os.path.join(datapath, vlbadata), vlbacopy)

    def setUp(self):
        pass

    def tearDown(self):
        rmtables(caltab)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(evncopy)
        shutil.rmtree(vlbacopy)

    def test_tsys(self):
        ''' Test calibration table produced when gencal is run on an MS with a SYSCAL table'''

        gencal(vis=evncopy, caltable=caltab, caltype='tsys', uniform=False)

        self.assertTrue(os.path.exists(caltab))
        self.assertTrue(th.compTables(caltab, evncalRef, ['WEIGHT']))

    def test_tsys_nan(self):
        ''' Test calibration table produced when gencal is run on an MS with a SYSCAL table that contains NaNs'''

        # Change negative values in SYSCAL to NaNs.
        # This should result in the same calibration table entries
        # being flagged.
        _tb.open(evncopy + '/SYSCAL', nomodify=False)
        tsys = _tb.getcol('TSYS')
        tsys = np.where(tsys < 0, float('nan'), tsys)
        _tb.putcol('TSYS', tsys)
        _tb.close()

        gencal(vis=evncopy, caltable=caltab, caltype='tsys', uniform=False)

        self.assertTrue(os.path.exists(caltab))
        self.assertTrue(th.compTables(caltab, evncalRef, ['FPARAM', 'WEIGHT']))


class TestJyPerK(unittest.TestCase):
    """Tests specifying antenna-based calibration values with external resource.

    The caltype jyperk is a type of amplitude correction or 'amp'. In the process
    of specifycal() executed within gencal(), the values loaded from a csv file
    with factors or obtained from the Jy/K Web API are given as the 'parameter'
    argument.

    Details are as follows.
    https://open-jira.nrao.edu/browse/CAS-12236
    """

    vis = 'uid___A002_X85c183_X36f.ms'
    jyperk_factor_csv = os.path.join(datapath, 'jyperk_factor.csv')

    @classmethod
    def setUpClass(cls):
        cls.casa_cwd_path = os.getcwd()

        cls.working_directory = TestJyPerK._generate_uniq_fuse_name_in_cwd(
                                    prefix='working_directory_for_jyperk_')
        os.mkdir(cls.working_directory)
        os.chdir(cls.working_directory)

        original_vis = os.path.join(datapath, f'{cls.vis}.sel')
        shutil.copytree(original_vis, cls.vis, symlinks=False)

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.casa_cwd_path)
        shutil.rmtree(cls.working_directory)

    def setUp(self):
        # The caltable is generated by each gencal task.
        self.caltable = TestJyPerK._generate_uniq_fuse_name_in_cwd(
                                prefix='generated_caltable_', suffix='.cal')

    def tearDown(self):
        if os.path.isdir(self.caltable):
            shutil.rmtree(self.caltable)

    @staticmethod
    def _generate_uniq_fuse_name_in_cwd(prefix='', suffix=''):
        while True:
            fuse_name = f'{prefix}{str(uuid.uuid4())}{suffix}'
            if not os.path.isdir(fuse_name):
                return fuse_name

    @classmethod
    @contextlib.contextmanager
    def _generate_jyperk_file_xxyy(cls, infile):
        with open(infile, 'r') as f:
            lines = map(lambda x: x.rstrip('\n'), f)
            header = next(lines)
            factors = list(filter(
                lambda x: x.startswith(cls.vis),
                lines
            ))

        with tempfile.NamedTemporaryFile() as f:
            # editing file here
            f.write(f'{header}\n'.encode())
            for line in factors:
                items = line.split(',')
                factor_org = float(items[-1])
                # factor for XX is original value while
                # factor for YY is 4 times original value
                for factor, pol in zip([factor_org, factor_org * 4], ['XX', 'YY']):
                    _items = items[:-2] + [pol, str(factor)]
                    f.write(f'{",".join(_items)}\n'.encode())
            f.flush()
            yield f.name

    def _read_cparam_as_real(self, name):
        tb = table()
        tb.open(name)
        try:
            paramlist = tb.getcol('CPARAM').real
        finally:
            tb.close()
        return paramlist[0, 0], paramlist[1, 0]

    def _load_jyperkdb_responses(self, test_data):
        responses = {}
        with open(test_data) as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                responses[row[0]] = row[1]
        return responses

    @patch('casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response')
    def test_jyperk_gencal_for_web_api_error(self, mock_retrieve):
        """Test to check that the factors from the web API are applied to the caltable.

        The following arguments are required for this test.
        * caltype='jyperk'
        * endpoint='asdm'
        """
        error_message = "expected error"

        def get_response(url):
            # return failed response
            response = '{"success": false, "error": "%s"}' % (error_message)
            return response

        mock_retrieve.side_effect = get_response

        with self.assertRaisesRegex(RuntimeError, f'Failed to get Jy/K factors from DB: {error_message}'):
            gencal(vis=self.vis,
                   caltable=self.caltable,
                   caltype='jyperk',
                   endpoint='asdm',
                   uniform=False)

        self.assertTrue(mock_retrieve.called)

    @patch('casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response')
    def test_jyperk_gencal_for_asdm_web_api(self, mock_retrieve):
        """Test to check that the factors from the web API are applied to the caltable.

        The following arguments are required for this test.
        * caltype='jyperk'
        * endpoint='asdm'
        """
        def get_response(url):
            return responses[url]

        responses = self._load_jyperkdb_responses(
                os.path.join(datapath, 'jyperk_web_api_response/asdm.csv'))
        mock_retrieve.side_effect = get_response

        gencal(vis=self.vis,
               caltable=self.caltable,
               caltype='jyperk',
               endpoint='asdm',
               uniform=False)

        self.assertTrue(os.path.exists(self.caltable))

        reference_caltable = os.path.join(
                datapath, 'jyperk_reference/web_api_with_asdm.cal')
        self.assertTrue(th.compTables(self.caltable, reference_caltable, ['WEIGHT']))
        self.assertTrue(mock_retrieve.called)

    @patch('casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response')
    def test_jyperk_gencal_for_model_fit_web_api(self, mock_retrieve):
        """Test to check that the factors from the web API are applied to the caltable.

        The following arguments are required for this test.
        * caltype='jyperk'
        * endpoint='model-fit'
        """
        def get_response(url):
            return responses[url]

        responses = self._load_jyperkdb_responses(
                os.path.join(datapath, 'jyperk_web_api_response/model-fit.csv'))
        mock_retrieve.side_effect = get_response

        gencal(vis=self.vis,
               caltable=self.caltable,
               caltype='jyperk',
               endpoint='model-fit',
               uniform=False)

        self.assertTrue(os.path.exists(self.caltable))

        reference_caltable = os.path.join(
                datapath, 'jyperk_reference/web_api_with_model_fit.cal')
        self.assertTrue(th.compTables(self.caltable, reference_caltable, ['WEIGHT']))
        self.assertTrue(mock_retrieve.called)

    @patch('casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response')
    def test_jyperk_gencal_for_interpolation_web_api(self, mock_retrieve):
        """Test to check that the factors from the web API are applied to the caltable.

        The following arguments are required for this test.
        * caltype='jyperk'
        * endpoint='interpolation'
        """
        def get_response(url):
            return responses[url]

        responses = self._load_jyperkdb_responses(
                os.path.join(datapath, 'jyperk_web_api_response/interpolation.csv'))
        mock_retrieve.side_effect = get_response

        gencal(vis=self.vis,
               caltable=self.caltable,
               caltype='jyperk',
               endpoint='interpolation',
               uniform=False)

        self.assertTrue(os.path.exists(self.caltable))

        reference_caltable = os.path.join(
                datapath, 'jyperk_reference/web_api_with_interpolation.cal')
        self.assertTrue(th.compTables(self.caltable, reference_caltable, ['WEIGHT']))
        self.assertTrue(mock_retrieve.called)

    def test_jyperk_gencal_for_factor_file(self):
        """Test to check that the factors in the csv file are applied to the caltable.

        The following arguments are required for this test.
        * caltype='jyperk'
        * infile
        """
        gencal(vis=self.vis,
               caltable=self.caltable,
               caltype='jyperk',
               infile=self.jyperk_factor_csv,
               uniform=False)

        self.assertTrue(os.path.exists(self.caltable))

        reference_caltable = os.path.join(
                datapath, 'jyperk_reference/factor_file.cal')
        self.assertTrue(th.compTables(self.caltable, reference_caltable, ['WEIGHT']))

        reference = \
            np.array([1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,
                     1.,1.,1.,1.,1.,1., 1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,
                     0.13882191479206085,0.13882191479206085,0.13882191479206085,
                     1.,1.,1.,0.13728643953800201,0.13728643953800201,0.13728643953800201,
                     1.,1.,1.,0.13593915104866028,0.13593915104866028,0.13593915104866028,
                     1.,1.,1.,0.13782501220703125,0.13782501220703125,0.13782501220703125,
                     1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.])

        p1, p2 = self._read_cparam_as_real(self.caltable)
        self.assertTrue(np.allclose(reference, p1))
        self.assertTrue(np.allclose(reference, p2))

    def test_jyperk_gencal_for_factor_file_xxyy(self):
        """Test to check that the factors in the csv file are applied to the caltable.

        The following arguments are required for this test.
        * caltype='jyperk'
        * infile
        """
        with self._generate_jyperk_file_xxyy(self.jyperk_factor_csv) as temp_csv:
            # temp_csv should contain pol-dependent Jy/K factors
            # factors for XX is same as original factors for I while
            # factors for YY is 4 times original factors so that
            # CPARAM value becomes half of reference value
            gencal(vis=self.vis,
                   caltable=self.caltable,
                   caltype='jyperk',
                   infile=temp_csv,
                   uniform=False)

        self.assertTrue(os.path.exists(self.caltable))

        reference_caltable = os.path.join(
                datapath, 'jyperk_reference/factor_file.cal')
        self.assertTrue(th.compTables(self.caltable, reference_caltable, ['WEIGHT', 'CPARAM']))

        # reference_xx is same as "reference" in test_jyperk_gencal_for_factor_file
        reference_xx = \
            np.array([1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,
                     1.,1.,1.,1.,1.,1., 1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,
                     0.13882191479206085,0.13882191479206085,0.13882191479206085,
                     1.,1.,1.,0.13728643953800201,0.13728643953800201,0.13728643953800201,
                     1.,1.,1.,0.13593915104866028,0.13593915104866028,0.13593915104866028,
                     1.,1.,1.,0.13782501220703125,0.13782501220703125,0.13782501220703125,
                     1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.,1.])
        # reference_yy is half of original reference value (except 1.0)
        reference_yy = np.where(
            reference_xx < 1.0, reference_xx / 2, 1.0
        )

        p1, p2 = self._read_cparam_as_real(self.caltable)
        self.assertTrue(np.allclose(reference_xx, p1))
        self.assertTrue(np.allclose(reference_yy, p2))

    def test_not_vis_name_in_factor_csv(self):
        """Test to check a caltable does not been generated when there are not vis name in the factor csv file.
        """
        vis = 'non-existent-observation.ms'
        if not os.path.isfile(vis):
            os.symlink(self.vis, vis)

        with self.assertRaises(Exception) as cm:
            gencal(vis=vis,
                   caltable=self.caltable,
                   caltype='jyperk',
                   infile=self.jyperk_factor_csv,
                   uniform=False)

        self.assertEqual(cm.exception.args[0], 'There is no factor.')

    def test_infile_is_incorrect_type(self):
        """Test to check for ejecting raise when infile is incorrect type."""
        from casatasks.private.task_gencal import gencal as private_gencal

        with self.assertRaises(Exception) as cm:
            private_gencal(vis=self.vis,
                           caltable=self.caltable,
                           caltype='jyperk',
                           infile=[self.jyperk_factor_csv],
                           uniform=False)

        self.assertEqual(cm.exception.args[0], 'The infile argument should be str or None.')

class TestSwPow(unittest.TestCase):

    testcal = 'swpow.cal'
    def setUp(self):
        shutil.copytree(os.path.join(datapath,swpowdata), swpowcopy)

    def tearDown(self):
        if os.path.exists(swpowcopy):
            shutil.rmtree(swpowcopy)
        if os.path.exists(self.testcal):
            shutil.rmtree(self.testcal)

    def test_switched_power_weights_caltype(self):
        """Check that resulting caltable has all 1's for gains and non-trivial values for weight adjustment

        The following arguments are required for this test.
        * caltype='swpwts'
        """
        gencal(vis=swpowcopy, caltable=self.testcal, caltype='swpwts')

        _tb.open(self.testcal)
        res = _tb.getcol('FPARAM')
        _tb.close()

        #self.assertTrue(np.all(res[0:1,:,:] == 1))
        self.assertTrue(np.mean(res[1,:,:]) != 1)

class gencal_eoptest(unittest.TestCase):

    usno_finals_erp = os.path.join(datapath, 'usno_finals.erp')
    eopc04_IAU2000 = os.path.join(datapath, 'eopc04_IAU2000.62-now')

    @classmethod
    def setUpClass(cls):
        shutil.copytree(os.path.join(datapath, evndata), evncopy)
        shutil.copytree(os.path.join(datapath, vlbadata), vlbacopy)

    def setUp(self):
        pass

    def tearDown(self):
        rmtables(caltab)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(evncopy)
        shutil.rmtree(vlbacopy)

    def test_eop(self):
        """Test calibration table produced when gencal is run on an MS
           with an EARTH_ORIENTATION table."""

        gencal(vis=vlbacopy, caltable=caltab, caltype='eop')

        self.assertTrue(os.path.exists(caltab))

        # Compare with reference file from the repository
        reference = os.path.join(datapath, 'ba123a_casa.eop')
        self.assertTrue(th.compTables(caltab, reference, ['WEIGHT'], [0.001, 1e-14], mode="both"))

    def test_eop_usno(self):
        """Test calibration table produced when gencal is run using an
           external file."""

        gencal(vis=vlbacopy, caltable=caltab, caltype='eop',
               infile=self.usno_finals_erp)

        self.assertTrue(os.path.exists(caltab))

        # Compare with reference file from the repository
        reference = os.path.join(datapath, 'ba123a_usno.eop')
        self.assertTrue(th.compTables(caltab, reference, ['WEIGHT'], [0.001, 1e-14], mode="both"))

    def test_eop_iers(self):
        """Test calibration table produced when gencal is run using an
           external file."""

        gencal(vis=vlbacopy, caltable=caltab, caltype='eop',
               infile=self.eopc04_IAU2000)

        self.assertTrue(os.path.exists(caltab))

        # Compare with reference file from the repository
        reference = os.path.join(datapath, 'ba123a_iers.eop')
        self.assertTrue(th.compTables(caltab, reference, ['WEIGHT'], [0.001, 1e-14], mode="both"))

    def test_noeop(self):
        """Test that no calibration table is produced when gencal is run on an
           MS without an EARTH_ORIENTATION table.

        """

        try:
            gencal(vis=evncopy, caltable=caltab, caltype='eop')
        except:
            pass

        self.assertFalse(os.path.exists(caltab))


if __name__ == '__main__':
    unittest.main()
