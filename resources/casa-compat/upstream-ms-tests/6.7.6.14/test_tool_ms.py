##########################################################################
# test_tool_ms.py
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
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.ms.html
#
#
##########################################################################

import shutil
import os
import time
import unittest
from numpy import abs, array, max, ndarray, testing, unique, where
from math import ceil

from casatools import calibrater as cbtool
from casatools import ms as mstool
from casatools import ctsys
from casatools import quanta
from casatools import table as tbtool
from casatools import msmetadata as msmdtool

datadir = 'unittest/mstool/'
datafile = os.path.join(datadir, "ngc5921_ut.ms")
print('ms tool tests will use data from '+ datafile)

class mstool_test_base(unittest.TestCase):

    testms = "ngc5921.ms"
    testms2 = "ngc7538.ms"
    testfits = "ngc5921.fits"

    def setUpTest(self, nomodify=True):
        # Despite setUp/tearDown, some tests with writable MS left
        # MS in a changed state and caused later tests to fail.
        # So will copy RO testms to a RW one in those tests
        self.ms = mstool()
        if not os.path.exists(self.testms):
            shutil.copytree(ctsys.resolve(datafile), self.testms)
        self.ms.open(self.testms, nomodify)

    def setUpMs2(self):
        ms2 = os.path.join(datadir, 'ngc7538_ut.ms')
        if not os.path.exists(self.testms2):
            shutil.copytree(ctsys.resolve(ms2), self.testms2, symlinks=True)

    def setUpFits(self):
        self.qa = quanta()
        self.msname = ''
        self.fitsname = ''
        fitsfile = os.path.join(datadir, 'ngc5921.fits')
        if not os.path.exists(self.testfits):
            shutil.copyfile(ctsys.resolve(fitsfile), self.testfits)

    def tearDownTest(self):
        self.ms.done()
        self.removeMS(self.testms)
        self.ms = None

    def checkMS(self, msName):
        # MS is a directory
        self.assertTrue(os.path.isdir(msName), "MS check failed")

    def removeMS(self, msName):
        # really any directory, usually an MS in these tests
        try:
            shutil.rmtree(msName)
        except OSError:
            pass

    def checkFile(self, fileName):
        # MS is a directory
        self.assertTrue(os.path.isfile(fileName), "File check failed")

    def removeFile(self, fileName):
        try:
            os.remove(fileName)
        except OSError:
            pass

# ------------------------------------------------------------------------------

class mstool_test_ms(mstool_test_base):
    """ Test basic MS info """

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def test_msinfo(self):
        """test ms.iswritable, name, nrow, getreferencedtables"""
        self.assertFalse(self.ms.iswritable())
        self.assertEqual(self.ms.name(), os.path.join(os.getcwd(), self.testms))
        self.assertEqual(self.ms.nrow(), 22653)
        self.assertEqual(self.ms.getreferencedtables(), [self.ms.name()])
        print

    def test_getfielddirmeas(self):
        """test ms.getfielddirmeas"""
        # Reads row 0 of DELAY_DIR column in FIELD table
        delayDir = self.ms.getfielddirmeas("DELAY_DIR", 0)
        self.assertAlmostEqual(delayDir['m0']['value'], -2.7439276)
        self.assertAlmostEqual(delayDir['m1']['value'], 0.53248521)
        print

    def test_asdmref(self):
        """test ms.asdmref"""
        # empty string if no ASDM reference
        self.assertEqual(self.ms.asdmref(), "")
        print

# ------------------------------------------------------------------------------

class mstool_test_concat(mstool_test_base):

    def setUp(self):
        self.setUpTest(False) # need write access
        self.setUpMs2()
        self.outputMS = os.path.join(os.getcwd(), "outputTest.ms")

    def tearDown(self):
        self.tearDownTest()
        self.removeMS(self.outputMS)
        self.removeMS(self.testms2)

    def test_createmultims(self):
        """test ms.createmultims, ms.getreferencedtables"""
        self.ms.createmultims(self.outputMS, self.testms2,
            ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
        self.checkMS(self.outputMS)
        self.assertTrue(self.ms.ismultims())
        # For multims returns path to SUBMSS as a list
        refTable = os.path.join(self.outputMS, "SUBMSS/ngc7538.ms")
        self.assertEqual(self.ms.getreferencedtables(), [refTable])
        print

    def test_concatenate(self):
        """test ms.concatenate"""
        # concatenate testms2 to testms
        self.ms.concatenate(self.testms2, '1GHz', '1arcsec')
        self.ms.close() # to flush changes
        self.ms.open(self.testms)
        # test MS originally had 22653 rows
        self.assertEqual(self.ms.nrow(), 49329)
        print

    def test_testconcatenate(self):
        """test ms.testconcatenate"""
        # concat without MAIN and POINTING tables
        self.assertEqual(len(self.ms.range(["fields"])['fields']), 3)
        self.ms.testconcatenate(self.testms2, '1GHz', '1arcsec')
        self.assertEqual(len(self.ms.range(["fields"])['fields']), 6)
        print

    def test_virtconcatenate(self):
        """test ms.virtconcatenate"""
        self.assertEqual(len(self.ms.range(["fields"])['fields']), 3)
        self.ms.virtconcatenate(self.testms2, 'ngcaux.dat', '1GHz', '1arcsec')
        self.assertEqual(len(self.ms.range(["fields"])['fields']), 6)
        self.removeFile('ngcaux.dat')
        print

# ------------------------------------------------------------------------------

class mstool_test_summary(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def test_summary(self):
        """test ms.summary"""
        # Returns record (python dict)
        summary = self.ms.summary()
        # just check simple keyword values (also has scan and field dicts)
        self.assertEqual(summary['numrecords'], 22653)
        self.assertEqual(summary['timeref'], 'TAI')
        self.assertEqual(summary['nfields'], 3)
        self.assertAlmostEqual(summary['IntegrationTime'], 5310.000972747803)
        self.assertAlmostEqual(summary['BeginTime'], 49820.38802083069)
        self.assertAlmostEqual(summary['EndTime'], 49820.44947917527)
        print

    def test_getscansummary(self):
        """test ms.getscansummary"""
        # Returns record (python dict)
        scanInfo = self.ms.getscansummary()
        self.assertEqual(len(scanInfo), 7) # 7 scans in this ms
        scan7 = scanInfo['7']['0']  # we'll just check the details of one scan
        self.assertEqual(scan7['nRow'], 1512)
        self.assertEqual(scan7['StateId'], -1)
        self.assertEqual(scan7['FieldId'], 1)
        self.assertEqual(scan7['IntegrationTime'], 30.0)
        self.assertAlmostEqual(scan7['BeginTime'], 49820.44826388359)
        self.assertAlmostEqual(scan7['EndTime'], 49820.449305564165)
        print

    def test_getspectralwindowinfo(self):
        """test ms.getspectralwindowinfo"""
        # Returns record (python dict)
        spwInfo = self.ms.getspectralwindowinfo()
        self.assertEqual(len(spwInfo), 1) # 1 spw
        spw0 = spwInfo['0']
        self.assertEqual(spw0['NumChan'], 63)
        self.assertEqual(spw0['NumCorr'], 2)
        self.assertEqual(spw0['Frame'], 'LSRK')
        self.assertEqual(spw0['PolId'], 0)
        self.assertEqual(spw0['SpectralWindowId'], 0)
        self.assertAlmostEqual(spw0['ChanWidth'], 24414.0625)
        self.assertAlmostEqual(spw0['TotalWidth'], 1550196.875)
        self.assertAlmostEqual(spw0['Chan1Freq'], 1412665073.7687755)
        self.assertAlmostEqual(spw0['RefFreq'], 1413428013.2219005)
        print

    def test_listhistory(self):
        """test ms.listhistory"""
        # Lists history table in logger, returns bool
        self.assertTrue(self.ms.listhistory())
        print

    def test_writehistory(self):
        """test ms.writehistory"""
        self.assertTrue(self.ms.writehistory('an arbitrary history message'))
        self.assertTrue(self.ms.listhistory())
        print

    def xtest_statisticsold(self):
        """test ms.statisticsold"""
        # Returns record (python dict)
        stats = self.ms.statisticsold(column="DATA", complex_value='amp',
                field="2")["DATA"]
        self.assertEqual(stats['npts'], 1619352.0)
        self.assertAlmostEqual(stats['medabsdevmed'], 0.0122800)
        self.assertAlmostEqual(stats['min'], 2.2130522e-05)
        self.assertAlmostEqual(stats['max'], 73.6559143)
        self.assertAlmostEqual(stats['sum'], 6899544.742657, 6)
        self.assertAlmostEqual(stats['sumsq'], 439740418.53064, 5)
        self.assertAlmostEqual(stats['mean'], 4.2606825)
        self.assertAlmostEqual(stats['median'], 0.0273157)
        self.assertAlmostEqual(stats['rms'], 16.47888, 5)
        self.assertAlmostEqual(stats['stddev'], 15.9185447)
        self.assertAlmostEqual(stats['var'], 253.4000652)
        self.assertAlmostEqual(stats['quartile'], 0.0254916)
        print

    def test_statistics(self):
        """test ms.statistics"""
        # Returns record (python dict)
        stats = self.ms.statistics(column="DATA", complex_value='amp', field="2")
        stats = stats['']
        self.assertTrue(stats['isMasked'])
        self.assertFalse(stats['isWeighted'])
        self.assertEqual(stats['minIndex'], 8692)
        self.assertEqual(stats['maxIndex'], 2408)
        self.assertEqual(stats['maxDatasetIndex'], 13)
        self.assertEqual(stats['minDatasetIndex'], 32)
        self.assertEqual(stats['npts'], 1619352.0)
        self.assertAlmostEqual(stats['medabsdevmed'], 0.0122800)
        self.assertAlmostEqual(stats['min'], 2.2130521e-05)
        self.assertAlmostEqual(stats['max'], 73.6559143)
        self.assertAlmostEqual(stats['sum'], 6899544.7427, 4)
        self.assertAlmostEqual(stats['sumsq'], 439740418.5306, 4)
        self.assertAlmostEqual(stats['mean'], 4.2606825)
        self.assertAlmostEqual(stats['median'], 0.0273157)
        self.assertAlmostEqual(stats['rms'], 16.47888, 5)
        self.assertAlmostEqual(stats['stddev'], 15.9185447)
        self.assertAlmostEqual(stats['variance'], 253.4000652)
        self.assertAlmostEqual(stats['firstquartile'], 0.0163202)
        self.assertAlmostEqual(stats['thirdquartile'], 0.0418118)
        print

    def test_range(self):
        """test ms.range"""
        # Returns record (python dict)
        ranges = self.ms.range(["time","uvdist","amplitude","antenna1"])
        self.assertEqual(len(ranges['antenna1']), 27)
        self.assertAlmostEqual(ranges['time'][0], 4.30448154e+09, 1)
        self.assertAlmostEqual(ranges['time'][1], 4.30448682e+09, 1)
        self.assertAlmostEqual(ranges['uvdist'][0], 0.)
        self.assertAlmostEqual(ranges['uvdist'][1], 1020.32003137)
        self.assertAlmostEqual(ranges['amplitude'][0], 2.21305218e-05)
        self.assertAlmostEqual(ranges['amplitude'][1], 7.37500000e+01)
        print

    def test_lister(self):
        """test ms.lister"""
        fname = 'lister.txt'
        self.assertTrue(self.ms.lister(field='2', antenna='1', correlation='RR',
            scan='3', listfile=fname))
        self.checkFile(fname)
        f = open(fname)
        # we're not going to do any parsing...
        self.assertEqual(len(f.readlines()), 30784)
        f.close()
        self.removeFile('lister.txt')
        print

    def test_metadata(self):
        """test ms.metadata"""
        # returns msmetadata
        msmd = self.ms.metadata()
        # just check ms.metadata example;
        # other msmetadata functions tested in test_msmd
        self.assertEqual(msmd.nspw(), 1)
        msmd.done()
        print

# ------------------------------------------------------------------------------

class mstool_test_select(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def xtest_selectinitold(self):
        """test ms.selectinitold, ms.nrowold"""
        self.assertTrue(self.ms.selectinitold())
        # this MS only has one DDID so no selection done
        self.assertEqual(self.ms.nrowold(), 22653)
        print

    def test_selectinit(self):
        """test ms.selectinit, ms.nrow"""
        self.assertTrue(self.ms.selectinit())
        # this MS only has one DDID so no selection done
        self.assertEqual(self.ms.nrow(False), 22653)
        self.assertEqual(self.ms.nrow(True), 22653)
        print

    def test_msselect(self):
        """test ms.msselect, ms.msselectedindices, ms.reset"""
        staql={'field':'1445+0990*', 'scan':'3~5'}
        # msselect changes "base" MS
        self.ms.msselect(staql)
        # fewer rows with selection
        self.assertEqual(self.ms.nrow(), 22653)
        self.assertEqual(self.ms.nrow(True), 1890)
        # selected field 1
        self.assertEqual(self.ms.msselectedindices()['field'][0], 1)
        testing.assert_array_equal(self.ms.msselectedindices()['scan'], [3,4,5])
        # reset to orig MS - no selected indices
        self.ms.reset()
        self.assertEqual(self.ms.nrow(), 22653)
        self.assertEqual(self.ms.msselectedindices()['field'].size, 0)
        print

    def test_msselect_timerange(self):
        """test ms.msselect with timerange selection"""
        # specific test for the bug reported in CAS-13534
        # which interpreted
        #
        #   "1995/04/13/09:18:58.5+0:0:1.7"
        #
        # as
        #
        #   "1995/04/13/09:18:58+0:0:1"
        #
        # making the following selection null
        qa = quanta()
        t0 = '1995/04/13/09:18:58.5'
        dt_val = qa.quantity(1.7, 's')
        dt = qa.formxxx(dt_val, 'hms', prec=1)
        timerange = f'{t0}+{dt}'
        staql = {'time': timerange}
        expected_nrow = 351

        # msselect changes "base" MS
        self.ms.msselect(staql)
        # fewer rows with selection
        self.assertEqual(self.ms.nrow(), 22653)
        self.assertEqual(self.ms.nrow(True), expected_nrow)

        # make sure selected time is within the range
        data = self.ms.getdata(['time'])
        time_data = data['time']
        self.assertEqual(len(time_data), expected_nrow)
        tmin = qa.convert(qa.quantity(t0), 's')
        tmax = qa.add(tmin, dt_val)
        time_le_tmax = time_data <= tmax['value']
        time_ge_tmin = time_data >= tmin['value']
        self.assertTrue(time_le_tmax.all())
        self.assertTrue(time_ge_tmin.all())
        print

    def xtest_selectold(self):
        """test ms.selectold"""
        self.assertEqual(len(self.ms.rangeold(["antenna1"])["antenna1"]), 27)
        # select 3 antenna1
        antsel = [1,3,5]
        self.ms.selectold({'antenna1':antsel})
        # range for antenna is list of sel antennas, not min/max
        testing.assert_array_equal(self.ms.rangeold(['antenna1'])['antenna1'], antsel)
        print

    def test_select(self):
        """test ms.select"""
        self.assertEqual(len(self.ms.range(["antenna1"])["antenna1"]), 27)
        # select 3 antenna1
        antsel = [1,3,5]
        self.ms.select({'antenna1':antsel})
        # range for antenna is list of sel antennas, not min/max
        testing.assert_array_equal(self.ms.range(['antenna1'])['antenna1'], antsel)
        print

    def xtest_selecttaqlold(self):
        """test ms.selecttaqlold"""
        self.ms.selectinitold()
        # select one antenna1
        self.ms.selecttaqlold('ANTENNA1==3')
        testing.assert_array_equal(self.ms.rangeold(["antenna1"])['antenna1'], [3])
        print

    def test_selecttaql(self):
        """test ms.selecttaql"""
        self.ms.selectinit()
        self.ms.selecttaql('ANTENNA1==3')
        # range returns array of size 1, antennaId 3
        testing.assert_array_equal(self.ms.range(["antenna1"])['antenna1'], [3])
        print

    def xtest_selectchannelold(self):
        """test ms.selectchannelold"""
        # original ms has 63 channels
        self.ms.selectinitold()
        rec = self.ms.getdataold(["axis_info"])
        self.assertEqual(len(rec["axis_info"]["freq_axis"]["chan_freq"]), 63)

        # average into 3 channels
        self.assertTrue(self.ms.selectchannelold(3,2,5,8))
        rec = self.ms.getdataold(["data", "axis_info"])
        # check chan_freq
        chan_freqs = rec["axis_info"]["freq_axis"]["chan_freq"]
        self.assertEqual(len(chan_freqs), 3)
        exp_freqs = array([[1.41276273e+09], [1.41295804e+09], [1.41315336e+09]])
        testing.assert_array_almost_equal(chan_freqs, exp_freqs, -1)
        # check channel-averaged data
        self.assertEqual(rec['data'].shape[1], 3)
        self.assertAlmostEqual(rec['data'][0][0][0], (62.093727111816406+0j))

        # test invalid selection
        print("\nTest invalid channel selection:")
        self.ms.selectinitold(reset=True)
        self.assertFalse(self.ms.selectchannelold(128,2,5,3))
        print

    def test_selectchannel(self):
        """test ms.selectchannel"""
        # original ms has 63 channels
        self.ms.selectinit()
        rec = self.ms.getdata(["axis_info"])
        self.assertEqual(len(rec["axis_info"]["freq_axis"]["chan_freq"]), 63)

        # average into 3 channels
        self.assertTrue(self.ms.selectchannel(3,2,5,8))
        rec = self.ms.getdata(["data", "axis_info"])
        # check chan_freq
        chan_freqs = rec["axis_info"]["freq_axis"]["chan_freq"]
        self.assertEqual(len(chan_freqs), 3)
        exp_freqs = array([[1.41276273e+09], [1.41295804e+09], [1.41315336e+09]])
        testing.assert_array_almost_equal(chan_freqs, exp_freqs, -1)
        # check channel-averaged data
        self.assertEqual(rec['data'].shape[1], 3)
        self.assertAlmostEqual(rec['data'][0][2][0], (70.219131469726562+0j))

        # test invalid selection
        print("\nTest invalid channel selection:")
        self.ms.selectinit(reset=True)
        self.assertFalse(self.ms.selectchannel(2,128,5,3))
        print

    def xtest_selectpolarizationold(self):
        """test ms.selectpolarizationold"""
        # no selection: RR,LL
        self.ms.selectinitold()
        rec = self.ms.getdataold(["axis_info", "data"])
        testing.assert_array_equal(rec["axis_info"]["corr_axis"], ["RR", "LL"])
        self.assertEqual(rec['data'].shape[0], 2)
        # select one polarization RR
        self.ms.selectpolarizationold(["RR"])
        rec = self.ms.getdataold(["axis_info", "data"])
        testing.assert_array_equal(rec["axis_info"]["corr_axis"], ["RR"])
        self.assertEqual(rec['data'].shape[0], 1)
        self.assertAlmostEqual(rec['data'][0][0][0], (3.1573812961578369+0j))
        # test polarization conversion
        self.ms.reset()
        self.ms.selectpolarizationold(["XX"])
        rec = self.ms.getdataold(["axis_info", "data"])
        testing.assert_array_equal(rec["axis_info"]["corr_axis"], ["XX"])
        self.assertEqual(rec['data'].shape[0], 1)
        self.assertAlmostEqual(rec['data'][0][0][0], (2.7459716796875+0j))
        print

    def test_selectpolarization(self):
        """test ms.selectpolarization"""
        # no selection: RR,LL
        self.ms.selectinit()
        rec = self.ms.getdata(["axis_info", "data"])
        testing.assert_array_equal(rec["axis_info"]["corr_axis"], ["RR", "LL"])
        self.assertEqual(rec['data'].shape[0], 2)
        # select one polarization RR
        self.ms.selectpolarization(["RR"])
        rec = self.ms.getdata(["axis_info", "data"])
        testing.assert_array_equal(rec["axis_info"]["corr_axis"], ["RR"])
        self.assertEqual(rec['data'].shape[0], 1)
        self.assertAlmostEqual(rec['data'][0][0][0], (3.1573812961578369+0j))
        # test polarization conversion
        self.ms.reset()
        self.ms.selectpolarization(["XX"])
        rec = self.ms.getdata(["axis_info", "data"])
        testing.assert_array_equal(rec["axis_info"]["corr_axis"], ["XX"])
        self.assertEqual(rec['data'].shape[0], 1)
        self.assertAlmostEqual(rec['data'][0][0][0], (2.7459716796875+0j))
        print

    def test_select_getdata_empty(self):
        """test ms.select() and ms.getdata() with empty selection"""
        self.assertTrue(self.ms.selectinit())
        self.assertEqual(self.ms.nrow(True), 22653)

        # This ms has scans 1-7 only. casacore will throw a MSSelectionNullSelection
        # ms.msselect should return false and the selection set to empty=0 rows
        field = 'N5921_2'
        scan = '9999'
        sel={'field': field, 'scan': scan}
        total_rows = 22653
        self.assertEqual(False, self.ms.msselect(sel))
        self.assertEqual(self.ms.nrow(True), 0)
        self.assertEqual(self.ms.nrow(), total_rows)
        self.assertEqual(self.ms.msselectedindices()['field'].size, 1)
        self.assertEqual(self.ms.msselectedindices()['field'][0], 2)
        self.assertEqual(self.ms.msselectedindices()['scan'].size, 1)
        self.assertEqual(self.ms.msselectedindices()['scan'][0], int(scan))
        # ms.getdata should handle the empty selection gracefully and return an empty dict
        self.assertEqual({}, self.ms.getdata(["axis_info", "data"]))

        self.ms.reset()
        self.assertEqual(self.ms.nrow(True), total_rows)
        self.assertEqual(self.ms.nrow(False), total_rows)
        self.assertEqual(self.ms.msselectedindices()['field'].size, 0)
        self.assertEqual(self.ms.msselectedindices()['scan'].size, 0)
        get_res = self.ms.getdata(["axis_info", "data"])
        self.assertTrue('data' in get_res)
        self.assertEqual(type(get_res['data']), ndarray)
        self.assertEqual(get_res['data'].shape, (2, 63, 22653))

    def test_msseltoindex(self):
        """test ms.msseltoindex"""
        # select field id 2
        rec = self.ms.msseltoindex(self.testms, field="N*")
        self.assertEqual(rec['field'], [2])
        print

# ------------------------------------------------------------------------------

class mstool_test_getdata(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def test_getweights(self):
        """test ms.getdata, weight"""
        self.assertEqual(len(self.ms.getdata(['weight'])['weight'][0]), self.ms.nrow())
        print

    def test_getsigma(self):
        """test ms.getdata, sigma"""
        self.assertEqual(len(self.ms.getdata(['sigma'])['sigma'][0]), self.ms.nrow())
        print

    def test_getweights_channavg(self):
        """test ms.getdata, weight, channavg"""
        # Average all channels together
        self.ms.selectchannel(1, 0, 63, 1)
        self.assertEqual(len(self.ms.getdata(['weight'])['weight'][0]), self.ms.nrow())
        print

    def test_getsigma_channavg(self):
        """test ms.getdata, sigma channavg"""
        # Average all channels together
        self.ms.selectchannel(1, 0, 63, 1)
        self.assertEqual(len(self.ms.getdata(['sigma'])['sigma'][0]), self.ms.nrow())
        print

# ------------------------------------------------------------------------------

class mstool_test_transform(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def test_regridspw(self):
        """test ms.regridspw"""
        # Basic check, tested more fully in regridspw_test.py
        # See pre-regridspw values in test_getspectralwindowinfo
        self.ms.close()
        regridms = 'ngc5921_regrid.ms'
        shutil.copytree(self.testms, regridms)
        self.ms.open(regridms, False)
        self.assertTrue(self.ms.regridspw(mode="chan", center=24, chanwidth=2, bandwidth=32))
        spwinfo = self.ms.getspectralwindowinfo()['0']
        self.ms.close()
        self.removeMS(regridms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # check values
        self.assertEqual(spwinfo['NumChan'], 16)  # was 63
        self.assertAlmostEqual(spwinfo['ChanWidth'], 48828.125) # was 24414.0625
        self.assertAlmostEqual(spwinfo['TotalWidth'], 781250.0) # was 1550196.875
        print

    def test_cvel(self):
        """test ms.cvel"""
        self.ms.close()
        cvelms = 'ngc5921_cvel.ms'
        shutil.copytree(self.testms, cvelms)
        self.ms.open(cvelms, False)
        self.assertTrue(self.ms.cvel(mode="channel", width=2))
        spwinfo = self.ms.getspectralwindowinfo()['0']
        self.ms.close()
        self.removeMS(cvelms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # check values
        self.assertEqual(spwinfo['NumChan'], 31)  # was 63
        self.assertAlmostEqual(spwinfo['ChanWidth'], 48828.125) # was 24414.0625
        self.assertAlmostEqual(spwinfo['TotalWidth'], 1513671.875) # was 1550196.875
        self.assertAlmostEqual(spwinfo['RefFreq'], 1412677280.8000255) # was 1413428013.2219005
        self.assertAlmostEqual(spwinfo['Chan1Freq'], 1412677280.8000255) # was 1412665073.7687755
        print

    def test_cvelfreqs(self):
        """test ms.cvelfreqs"""
        self.ms.close()
        cvelms = 'ngc5921_cvelfreq.ms'
        shutil.copytree(self.testms, cvelms)
        self.ms.open(cvelms, False)
        centers = self.ms.cvelfreqs(mode="channel", width=2)
        self.ms.close()
        self.removeMS(cvelms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # check len, first and last values
        self.assertEqual(len(centers), 31)
        self.assertAlmostEqual(centers[0], 1412677280.8000255)
        self.assertAlmostEqual(centers[30], 1414142124.5500255)
        print

    def test_statwt(self):
        """test ms.statwt"""
        self.ms.close()
        statwtms = 'ngc5921_statwt.ms'
        shutil.copytree(self.testms, statwtms)
        self.ms.open(statwtms, False)
        self.assertTrue(self.ms.oldstatwt(fitspw='0:0~32', field=[0], datacolumn='data'))
        rec = self.ms.getdata(['weight','sigma'])
        self.ms.close()
        self.removeMS(statwtms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # orig weight=378.0, sigma=0.0514344
        self.assertAlmostEqual(rec['weight'][0][0], .0101575451)
        self.assertAlmostEqual(rec['sigma'][0][0], 9.9221458)
        print

    def test_continuum_uv_cont_sub(self):
        """test ms.continuumsub, uvsub, contsub"""
        # Make writable ms
        self.ms.close()
        subms = 'ngc5921_sub.ms'
        shutil.copytree(self.testms, subms)
        self.ms.open(subms, False)
        # no model or corrected data in orig MS
        exp_model = array([-0.05976862+0.21077624j,-0.13658243+0.1545141j,-0.21587822-0.11069717j,-0.13609098-0.17520858j,0.18628541-0.08726287j])
        exp_corr = array([ 0.05145176-0.20602664j,0.12195288-0.1444806j,0.21040531+0.09249699j,0.11938272+0.16995102j,-0.19297704+0.09558354j])
        self.assertTrue(self.ms.continuumsub(field=1, mode='subtract'))
        rec = self.ms.getdata(['model_data', 'corrected_data'])
        self.assertEqual(rec['model_data'].shape, (2,63,22653))
        # check some values
        model0 = rec['model_data'][0][0][-5:]
        corr0 = rec['corrected_data'][0][0][-5:]
        testing.assert_array_almost_equal(corr0, exp_corr, 7)
        testing.assert_array_almost_equal(model0, exp_model, 7)
        # Now that we have model data, call uvsub (corrected-model)
        self.assertTrue(self.ms.uvsub())
        rec = self.ms.getdata(['corrected_data'])
        corr1 = rec['corrected_data'][0][0][-5:]
        testing.assert_array_almost_equal(corr1, corr0-model0, 7)
        # call contsub
        contsubms = "contsub.ms"
        self.assertTrue(self.ms.contsub(contsubms, fitspw='0',
            fitorder=0, field=[0], combine='spw'))
        self.ms.close()
        self.removeMS(subms)
        self.checkMS(contsubms)
        self.ms.open(contsubms)
        rec = self.ms.getdata(['data'])
        self.assertAlmostEqual(rec['data'][0][0][0], (-59.606513977050781+0j))
        self.ms.close()
        self.removeMS(contsubms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        print

    def test_hanningsmooth(self):
        """test ms.hanningsmooth"""
        self.ms.close()
        smoothms = 'ngc5921_statwt.ms'
        shutil.copytree(self.testms, smoothms)
        self.ms.open(smoothms, False)
        self.assertTrue(self.ms.hanningsmooth('data'))
        rec = self.ms.getdata(['data'])
        self.ms.close()
        self.removeMS(smoothms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # orig data was 3.15738129616+0j
        self.assertAlmostEqual(rec['data'][0][0][0], (14.5159635544+0j))
        print

    # No tests for these functions:
    def xtest_moments(self):
        """test ms.moments"""
        # No test: this function does not exist in ms_cmpt.cc!
        print

    def xtest_addephemeris(self):
        """test ms.addephemeris"""
        # No test: no ephemeris sources in test MS
        print

# ------------------------------------------------------------------------------

class mstool_test_dataIO(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def xtest_getdataold_putdataold(self):
        """test ms.getdataold, putdataold"""
        self.ms.close()
        # need writable MS for putdata:
        putms = 'ngc5921_put.ms'
        self.removeMS(putms)
        shutil.copytree(self.testms, putms)
        # add corrected_data
        cb = cbtool()
        cb.open(putms)
        cb.close()
        self.ms.open(putms, False)
        # items list
        rec = self.ms.getdataold(['data', 'antenna1', 'scan_number'])
        self.assertEqual(rec['antenna1'][1], 27)
        self.assertEqual(rec['scan_number'][1], 1)
        data0 = rec['data']
        # double the data and write it into the MS
        doubleData = data0 * 2
        doubleRec = {'corrected_data': doubleData}
        self.ms.putdataold(doubleRec)
        # make sure it is 2x
        rec = self.ms.getdataold(['corrected_data'])
        testing.assert_array_almost_equal(rec['corrected_data'],
                data0*2, 7)
        self.ms.close()
        self.removeMS(putms)
        self.ms.open(self.testms) # reopen for tearDown
        print

    def test_getdata_putdata(self):
        """test ms.getdata, putdata"""
        self.ms.close()
        # need writable MS for putdata:
        putms = 'ngc5921_put.ms'
        self.removeMS(putms)
        shutil.copytree(self.testms, putms)
        # add corrected_data
        cb = cbtool()
        cb.open(putms)
        cb.close()
        self.ms.open(putms, False)
        # items list
        rec = self.ms.getdata(['data', 'antenna1', 'scan_number'])
        self.assertEqual(rec['antenna1'][1], 27)
        self.assertEqual(rec['scan_number'][1], 1)
        data0 = rec['data']
        # double the data and write it into the MS
        doubleData = data0 * 2
        doubleRec = {'corrected_data': doubleData}
        self.ms.putdata(doubleRec)
        # make sure it is 2x
        rec = self.ms.getdata(['corrected_data'])
        self.assertEqual(rec['corrected_data'].shape, (2,63,22653))
        testing.assert_array_almost_equal(rec['corrected_data'], data0*2)
        # divide by 2 (=data again) and put it back, use ifraxis
        rec = self.ms.getdata(['corrected_data'], ifraxis=True)
        halfData = rec['corrected_data']*0.5
        self.assertEqual(halfData.shape, (2,63,378,60))
        halfRec = {'corrected_data': halfData}
        self.ms.putdata(halfRec)
        rec = self.ms.getdata(['data','corrected_data'])
        testing.assert_array_almost_equal(rec['corrected_data'],
                rec['data'])
        self.ms.close()
        self.removeMS(putms)
        self.ms.open(self.testms) # reopen for tearDown
        print

    def xtest_getdataold_args(self):
        """test ms.getdataold ifraxis, increment, and average"""
        # Expected data shape
        ncorr = 2
        nchan = 63
        nrow = 22653
        # Expected data shape with ifraxis
        nIfr = 378
        nAnt = 60
        # Expected data after averaging
        exp_data1 = [0.18245400-0.00021324j, 1.50447035-0.0020918j, 2.84147143-0.00377467j, 3.58339596-0.00475751j, 4.02027655-0.00478132j]
        exp_data2 = [2.49434376e+00+0.j, -1.52505504e-03-0.00459018j, 1.09913200e-03+0.00773078j, -4.95528942e-03-0.00457142j, 7.29188230e-03+0.00369013j]
        # defaults
        self.ms.selectinitold()
        rec = self.ms.getdataold(['data'])
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nrow))
        # ifraxis True
        rec = self.ms.getdataold(['data', 'ifr_number'], ifraxis=True)
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nIfr, nAnt))
        # increment 2
        rec = self.ms.getdataold(['data'], increment=2)
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nrow/2))
        # average True, ifraxis False - avg over row axis
        rec = self.ms.getdataold(['data'], average=True)
        self.assertEqual(rec['data'].shape, (ncorr, nchan))
        testing.assert_array_almost_equal(rec['data'][0][:5], exp_data1, 8)
        # average True, ifraxis True - avg over time axis
        rec = self.ms.getdataold(['data'], ifraxis=True, average=True)
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nIfr))
        testing.assert_array_almost_equal(rec['data'][0][0][:5], exp_data2, 8)
        print

    def test_getdata_args(self):
        """test ms.getdata ifraxis, increment, and average"""
        # Expected data shape
        ncorr = 2
        nchan = 63
        nrow = 22653
        # Expected data shape with ifraxis
        nIfr = 378
        nAnt = 60
        # Expected data after averaging
        exp_data1 = [0.18245400-0.00021324j, 1.50447035-0.0020918j, 2.84147143-0.00377467j, 3.58339596-0.00475751j, 4.02027655-0.00478132j]
        exp_data2 = [2.49434376e+00+0.j, -1.52505504e-03-0.00459018j, 1.09913200e-03+0.00773078j, -4.95528942e-03-0.00457142j, 7.29188230e-03+0.00369013j]
        # defaults
        rec = self.ms.getdata(['data'])
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nrow))
        # caps and mixed case
        rec = self.ms.getdata(['DATA', 'Flag'])
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nrow))
        self.assertEqual(rec['flag'].shape, (ncorr, nchan, nrow))
        # ifraxis True
        rec = self.ms.getdata(['data', 'ifr_number'], ifraxis=True)
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nIfr, nAnt))
        # increment 2
        rec = self.ms.getdata(['data'], increment=2)
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nrow//2))
        # average True, ifraxis False - avg over row axis
        rec = self.ms.getdata(['data'], average=True)
        self.assertEqual(rec['data'].shape, (ncorr, nchan))
        testing.assert_array_almost_equal(rec['data'][0][:5], exp_data1, 8)
        # average True, ifraxis True - avg over time axis
        rec = self.ms.getdata(['data'], ifraxis=True, average=True)
        self.assertEqual(rec['data'].shape, (ncorr, nchan, nIfr))
        testing.assert_array_almost_equal(rec['data'][0][0][:5], exp_data2, 8)
        print

    def test_getdata_average(self):
        # test consistent results for averaged amplitude, phase, weight, flag
        # amp only
        rec0amp = self.ms.getdata(['amplitude'], ifraxis=False, average=True)
        amp0 = rec0amp['amplitude'][1]

        # phase only
        rec0phase = self.ms.getdata(['phase'], ifraxis=False, average=True)
        phase0 = rec0phase['phase'][1]

        # amp, phase same as 0
        rec1 = self.ms.getdata(['amplitude', 'phase', 'weight', 'flag'], ifraxis=False, average=True)
        amp1 = rec1['amplitude'][1]
        phase1 = rec1['phase'][1]
        weight1 = rec1['weight'][1]
        flag1 = rec1['flag'][1]
        testing.assert_array_equal(amp1, amp0)
        testing.assert_array_equal(phase1, phase0)

        # Change order: phase, amp same as 1
        rec2 = self.ms.getdata(['phase', 'amplitude', 'weight', 'flag'], ifraxis=False, average=True)
        amp2 = rec2['amplitude'][1]
        phase2 = rec2['phase'][1]
        weight2 = rec2['weight'][1]
        flag1 = rec1['flag'][1]
        testing.assert_array_equal(amp1, amp2)
        testing.assert_array_equal(phase1, phase2)
        testing.assert_array_equal(weight1, weight2)

    def xtest_ngetdata(self):
        """test ms.ngetdata"""
        # 'items' list is only argument implemented (partially)
        rec = self.ms.ngetdata(['data', 'flag'])
        self.assertAlmostEqual(rec['data'][0][0][0], (3.15738129616+0j))
        self.assertEqual(rec['flag'][0][0][0], 0)
        print

# ------------------------------------------------------------------------------

class mstool_test_sort(mstool_test_base):

    def setUp(self):
        # For these tests, no newmsname caused exception:
        # Exception Reported: Invalid Table operation: Cannot copy/rename;
        # target table <path>/nosedir/ngc5921.ms is still
        # open (is in the table cache)
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def test_timesort(self):
        """test ms.timesort"""
        rec0 = self.ms.getdata(['time'])
        timesortms = 'ngc5921_timesort.ms'
        self.assertTrue(self.ms.timesort(timesortms))
        self.ms.close()
        self.ms.open(timesortms)
        rec1 = self.ms.getdata(['time'])
        self.ms.close()
        self.removeMS(timesortms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # ms is already time-sorted!
        testing.assert_array_equal(rec0['time'], rec1['time'])
        print

    def test_sort(self):
        """test ms.sort"""
        rec = self.ms.getdata(['antenna1', 'antenna2'])
        self.assertEqual(rec['antenna1'][1], 27)
        self.assertEqual(rec['antenna2'][2], 7)
        sortms = 'ngc5921_sort.ms'
        self.assertTrue(self.ms.sort(sortms, ['ANTENNA1', 'ANTENNA2']))
        self.ms.close()
        self.ms.open(sortms)
        rec = self.ms.getdata(['antenna1', 'antenna2'])
        self.ms.close()
        self.removeMS(sortms)
        self.ms.open(self.testms) # prevent SEVERE ms::detached errors in cleanup
        # check that antennas were sorted
        self.assertEqual(rec['antenna1'][1], 0)
        self.assertEqual(rec['antenna2'][2], 0)
        print

# ------------------------------------------------------------------------------

class mstool_test_subms(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def test_split(self):
        """test ms.split"""
        self.assertEqual(self.ms.nrow(), 22653)
        self.assertTrue(self.ms.split('subms.ms', field=[0], step=[5]))
        self.checkMS('subms.ms')
        self.ms.close()
        self.ms.open('subms.ms')
        # fewer rows with field id 0 only
        self.assertEqual(self.ms.nrow(), 4509)
        rec = self.ms.getdata(['field_id'])
        self.ms.close()
        self.removeMS('subms.ms')
        self.ms.open(self.testms) # for teardown
        # check values
        field0 = rec['field_id']
        self.assertTrue((field0 == 0).all())
        print

    def test_split_averaging(self):
        """test ms.split time averaging"""
        self.assertTrue(self.ms.split('subms.ms', timebin='120s'))
        self.checkMS('subms.ms')
        self.ms.close()
        self.ms.open('subms.ms')
        self.assertEqual(self.ms.nrow(), 9450)
        self.ms.close()
        self.removeMS('subms.ms')
        self.ms.open(self.testms) # for teardown
        print

    def test_partition(self):
        """test ms.partition"""
        self.assertTrue(self.ms.partition('partition.ms', field=[0]))
        self.checkMS('partition.ms')
        self.ms.close()
        self.ms.open('partition.ms')
        # fewer rows with field id 0 only
        self.assertEqual(self.ms.nrow(), 4509)
        self.ms.close()
        self.removeMS('partition.ms')
        self.ms.open(self.testms) # for teardown
        print

    def xtest_partition_averaging(self):
        """test ms.partition time averaging"""
        # TBD: test removed, timebin causes seg fault!
        self.assertTrue(self.ms.partition('partition.ms', timebin='120s'))
        self.checkMS('partition.ms')
        self.ms.close()
        self.ms.open('partition.ms')
        self.assertEqual(self.ms.nrow(), 4509)
        self.ms.close()
        self.removeMS('partition.ms')
        self.ms.open(self.testms) # for teardown
        print

# ------------------------------------------------------------------------------

class mstool_test_iter(mstool_test_base):

    def setUp(self):
        self.setUpTest()

    def tearDown(self):
        self.tearDownTest()

    def xtest_iterold_columns(self):
        """test ms.iterold functions with sort columns"""
        scannum = 1
        # rows per chunk
        rows = [4509, 1890, 6048, 756, 1134, 6804, 1512]
        self.ms.iterinitold(["SCAN_NUMBER", "ARRAY_ID", "FIELD_ID",
            "DATA_DESC_ID", "TIME"], adddefaultsortcolumns=False)
        self.ms.iteroriginold()
        rec = self.ms.getdataold(["scan_number", "data"])
        self.assertEqual(rec['scan_number'][0], scannum)
        self.assertEqual(rec["data"].shape[2], rows[scannum-1])
        while self.ms.iternextold():
            scannum += 1;
            rec = self.ms.getdataold(["scan_number", "data"])
            self.assertEqual(rec['scan_number'][0], scannum)
            self.assertEqual(rec["data"].shape[2], rows[scannum-1])
        self.ms.iterendold()
        print

    def test_iteration_columns(self):
        """test ms.iter functions with sort columns"""
        scannum = 1
        # rows per chunk
        rows = [4509, 1890, 6048, 756, 1134, 6804, 1512]
        self.ms.iterinit(["SCAN_NUMBER", "ARRAY_ID", "FIELD_ID",
            "DATA_DESC_ID", "TIME"], adddefaultsortcolumns=False)
        self.ms.iterorigin()
        rec = self.ms.getdata(["scan_number", "data"])
        self.assertEqual(rec['scan_number'][0], scannum)
        self.assertEqual(rec["data"].shape[2], rows[scannum-1])
        while self.ms.iternext():
            scannum += 1;
            rec = self.ms.getdata(["scan_number", "data"])
            self.assertEqual(rec['scan_number'][0], scannum)
            self.assertEqual(rec["data"].shape[2], rows[scannum-1])
        self.ms.iterend()
        print

    def xtest_iterinitold_interval(self):
        """test ms.iterinitold interval parameter"""
        lasttime = 0.0
        interval = 120.0
        nchunk = 1
        # Select a scan to avoid time jumps between scans
        self.ms.msselect({'scan': '3'})
        times = self.ms.rangeold(['time'])['time']
        scantime = times[1]-times[0]
        totalchunks = ceil(scantime/interval)
        self.ms.iterinitold(interval=interval)
        self.ms.iteroriginold() # chunk1
        while self.ms.iternextold(): # next chunk
            nchunk += 1
            rec = self.ms.getdataold(["time"])
            thistime = rec["time"][0]
            if lasttime != 0.0:
                self.assertAlmostEqual(thistime-lasttime, interval, 2)
            lasttime = thistime
        self.ms.iterendold()
        self.assertEqual(nchunk, totalchunks)
        print

    def test_iterinit_interval(self):
        """test ms.iterinit interval parameter"""
        lasttime = 0.0
        interval = 120.0
        nchunk = 1
        # Select a scan to avoid time jumps between scans
        self.ms.msselect({'scan': '3'})
        times = self.ms.range(['time'])['time']
        scantime = times[1]-times[0]
        totalchunks = ceil(scantime/interval)
        self.ms.iterinit(interval=interval)
        self.ms.iterorigin() # chunk1
        while self.ms.iternext(): # next chunk
            nchunk += 1
            rec = self.ms.getdata(["time"])
            thistime = rec["time"][0]
            if lasttime != 0.0:
                self.assertAlmostEqual(thistime-lasttime, interval, 2)
            lasttime = thistime
        self.ms.iterend()
        self.assertEqual(nchunk, totalchunks)
        print

    def Xtest_iterinitold_maxrows(self):
        """test ms.iterinitold maxrows parameter"""
        maxrows = 256
        nchunk = 1
        self.ms.msselect({'scan': '2'})
        rec = self.ms.getdataold(['data']) # get all rows at once
        totalrows = rec['data'].shape[2]
        totalchunks = ceil(float(totalrows)/maxrows)
        self.ms.iterinitold(maxrows=maxrows)
        self.ms.iteroriginold() # chunk1
        rec = self.ms.getdataold(["data"])
        self.assertEqual(rec["data"].shape[2], maxrows)
        while self.ms.iternextold(): # next chunk
            nchunk += 1
            # last chunk will be partial
            if nchunk < totalchunks:
                rec = self.ms.getdataold(["data"])
                self.assertEqual(rec["data"].shape[2], maxrows)
        self.ms.iterendold()
        self.assertEqual(nchunk, totalchunks)
        print

    def test_iterinit_maxrows(self):
        """test ms.iterinit maxrows parameter"""
        maxrows = 256
        nchunk = 1
        self.ms.msselect({'scan': '2'})
        rec = self.ms.getdata(['data']) # get all rows at once
        totalrows = rec['data'].shape[2]
        totalchunks = ceil(float(totalrows)/maxrows)
        self.ms.iterinit(maxrows=maxrows)
        self.ms.iterorigin() # chunk1
        rec = self.ms.getdata(["data"])
        self.assertEqual(rec["data"].shape[2], maxrows)
        while self.ms.iternext(): # next chunk
            nchunk += 1
            # last chunk will be partial
            if nchunk < totalchunks:
                rec = self.ms.getdata(["data"])
                self.assertEqual(rec["data"].shape[2], maxrows)
        self.ms.iterend()
        self.assertEqual(nchunk, totalchunks)
        print

    def xtest_niteration(self):
        """test ms.niter"""
        exp_rows = [0, 4509, 6399]
        got_rows = []
        self.ms.niterinit()
        self.ms.niterorigin()
        while not self.ms.niterend():
            rec = self.ms.ngetdata(['rows'])
            got_rows.append(rec['rows'][0])
            self.ms.niternext()
        self.assertEqual(got_rows, exp_rows)
        print

    def xtest_niteration_interval(self):
        """test ms.niter interval parameter"""
        # DISABLED THIS TEST - intervals are "mostly" correct at 120s;
        # Not going to debug this as it is almost deprecated (old VIVB)
        # Test interval
        self.ms.msselect({'scan': '3'})
        lasttime = 0.0
        interval = 120.0
        self.ms.niterinit(interval=interval)
        self.ms.niterorigin()
        while not self.ms.niterend():
            rec = self.ms.ngetdata(["time"])
            thistime = rec["time"][0]
            if lasttime != 0.0:
                self.assertAlmostEqual(thistime - lasttime, interval, 2)
            lasttime = thistime
            self.ms.niternext()
        print

    def xtest_niteration_rows(self):
        """test ms.niter rows parameter"""
        # Note this does not work quite like old iter,
        # niternext gets next VB not next set of rows
        # (i.e., there is only one VB so one iteration)
        # Also setting maxrows returns maxrows+1!
        maxrows = 256
        self.ms.msselect({'scan': '2'})
        self.ms.niterinit(maxrows=maxrows)
        self.ms.niterorigin()
        while not self.ms.niterend():
            rec = self.ms.ngetdata(['rows'])
            self.assertEqual(len(rec['rows']), maxrows+1)
            self.ms.niternext()
        print

# ------------------------------------------------------------------------------
# Tests for ms.fromfits and ms.tofits
class mstool_test_fits(mstool_test_base):
    # Some of the test cases come from old test_uvfits.py

    def setUp(self):
        self.setUpTest()
        self.setUpFits()

    def tearDown(self):
        self.tearDownTest()
        self.removeFile(self.testfits)
        shutil.rmtree(self.msname, ignore_errors=True)
        if os.path.exists(self.fitsname):
            os.system('rm -rf ' + self.fitsname)

    def test_list_from_tofits(self):
        """test ms.listfits, fromfits, tofits"""
        self.assertTrue(self.ms.listfits(self.testfits))
        # from fits to ms
        fitsms = 'fromfits.ms'
        testfits = "testfile.fits"
        self.assertTrue(self.ms.fromfits(fitsms, self.testfits))
        self.checkMS(fitsms)
        # from ms to fits
        self.ms.close() # self.testms
        self.ms.open(fitsms)
        self.assertTrue(self.ms.tofits(testfits, column="DATA", field=[0]))
        self.checkFile(testfits)
        # clean up
        self.ms.close() # fitsms
        self.removeMS(fitsms)
        self.removeFile(testfits)
        self.ms.open(self.testms) # for teardown
        print

    def test_stokes(self):
        """Verify fix to CAS_4283, uvfits files containing actual Stokes parameters will not be imported"""
        myms = mstool()
        self.msname = "my.ms"
        inpfits = ctsys.resolve(datadir + "1331+305_I.UVFITS")
        self.assertRaises(Exception, myms.fromfits, self.msname, inpfits)

    def test_receptor_angle(self):
        """CAS-7081: Test receptor angle is preserved"""
        myms = mstool()
        inpms = ctsys.resolve(datadir + "uvfits_test.ms")
        self.assertTrue(myms.open(inpms), "Input dataset not found")
        self.fitsname = "xyz.uvfits"
        self.assertTrue(myms.tofits(self.fitsname), "Failed to write uvfits")
        myms.done()
        feed = "/FEED"
        mytb = tbtool()
        mytb.open(inpms + feed)
        rec_ang = "RECEPTOR_ANGLE"
        expec = mytb.getcol(rec_ang)
        mytb.done()
        self.msname = "ke.ms"
        self.assertTrue(myms.fromfits(self.msname, self.fitsname), "Failed uvfits import")
        myms.done()
        mytb.open(self.msname + feed)
        got = mytb.getcol(rec_ang)
        mytb.done()
        self.assertTrue(max(abs(got-expec)) < 1e-7, "Receptor angles not preserved")

    def test_diameters(self):
        """CAS-5818: Verify bogus dish diameters in AN table are not used but normal algorithm is used instead"""
        myms = mstool()
        inpfits = ctsys.resolve(datadir + "CTR_CHI_TR2.RWYCP-10rows-ANT-DIAMTER-0")
        self.msname = "CAS-5818.ms"
        self.assertTrue(myms.fromfits(self.msname, inpfits), "Failed to import uvfits file")
        myms.done()
        mymd = msmdtool()
        mymd.open(self.msname)
        diam = mymd.antennadiameter(-1)
        mymd.done()
        expec = "25m"
        for i in diam.keys():
            self.assertTrue(self.qa.eq(diam[i], expec), "Unexpected diameter for antenna " + i)

    def test_filename_extensions(self):
        """CAS-7696: Verify we turn off fits filename extension support when necessary"""
        myms = mstool()
        inpfits = ctsys.resolve(datadir + "name10rows+000")
        self.msname = "CAS-7696.ms"
        self.assertTrue(myms.fromfits(self.msname, inpfits), "Failed to import uvfits file")
        myms.done()

    def test_export_overwrite(self):
        """CAS-5492: test the overwrite parameter when exporting MSes to uvfits"""
        myms = mstool()
        inpms = ctsys.resolve(datadir + "uvfits_test.ms")
        myms.open(inpms)
        self.fitsname = "CAS-5492.uvfits"
        self.assertTrue(myms.tofits(self.fitsname))
        # fail because overwrite=False
        self.assertRaises(RuntimeError, myms.tofits, fitsfile=self.fitsname, overwrite=False)
        # succeed because overwrite=True
        self.assertTrue(myms.tofits(self.fitsname, overwrite=True))
        myms.done()

    def test_badscan(self):
        """CAS-10054: Tests intermittent incorrect scan number in last row of single-scan dataset"""
        myms = mstool()
        inpfits = ctsys.resolve(datadir + "3c273.fits7")
        self.msname = "ngc4826.tutorial.3c273.7.ms"
        self.assertTrue(myms.fromfits(self.msname, inpfits), "Failed to import uvfits file")
        myms.done()

        mytb = tbtool()
        mytb.open(self.msname)
        scans = mytb.getcol('SCAN_NUMBER')
        mytb.close()

        nrows = len(scans)

        print('Last row has scan=' + str(scans[nrows - 1]) + ' ; (should be 1).')
        self.assertFalse(scans[nrows - 1] == 2, "Last row has wrong scan number: " + str(scans[nrows - 1]))
        # the following verifies that _all_ scan numbers are correct (and lists unique values)
        self.assertTrue(sum(scans == 1) == nrows, "Unexpected scan number found: " + str(unique(scans)))


if __name__ == '__main__':
    unittest.main()
