##########################################################################
# test_tool_calibrater.py
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
# [CAS-13589]
#
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/latest/api/tt/casatools.calibrater.html
#
#
##########################################################################


import casatools
cb = casatools.calibrater()
tb = casatools.table()

import os
import shutil
import unittest
import numpy as np

reg_unittest_datap = 'unittest/calibrater/'
datapath = casatools.ctsys.resolve(reg_unittest_datap)

# This is for tests that check what the parameter validator does when parameters are
# given wrong types - these don't exercise the task but the parameter validator!
validator_exc_type = AssertionError


class calibrater_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._vis = 'gaincaltest2.ms'
        cls._visObs = 'Itziar.ms'
        cls._cal = 'gaincaltest2.ms.G0'
        cls._lib = os.path.join(datapath, 'refcalgainG.txt')
        cls._bandvis = 'ngc5921.ms'
        cls._bandcal = 'ngc5921.gcal'

    @classmethod
    def tearDownClass(cls):

        if os.path.exists('gainspline.AMP.pol0.log'):
            os.remove('gainspline.AMP.pol0.log')
        if os.path.exists('gainspline.AMP.pol1.log'):
            os.remove('gainspline.AMP.pol1.log')
        if os.path.exists('gainspline.PHASE.pol0.log'):
            os.remove('gainspline.PHASE.pol0.log')
        if os.path.exists('gainspline.PHASE.pol1.log'):
            os.remove('gainspline.PHASE.pol1.log')


    def setUp(self):
        shutil.copytree(os.path.join(datapath, self._vis), self._vis)
        shutil.copytree(os.path.join(datapath, self._visObs), self._visObs)
        shutil.copytree(os.path.join(datapath, self._cal), self._cal)
        shutil.copytree(os.path.join(datapath, self._bandvis), self._bandvis)
        shutil.copytree(os.path.join(datapath, self._bandcal), self._bandcal)


    def tearDown(self):
        if os.path.exists('testlog.log'):
            os.remove('testlog.log')
        if os.path.exists('testcalout.cal'):
            shutil.rmtree('testcalout.cal')
        if os.path.exists('bpoly'):
            shutil.rmtree('bpoly')
        if os.path.exists('gainspline'):
            shutil.rmtree('gainspline')
        if os.path.exists('output.ms'):
            shutil.rmtree('output.ms')

        cb.close()
        cb.setvi(old=False)
        shutil.rmtree(self._vis)
        shutil.rmtree(self._cal)
        shutil.rmtree(self._visObs)
        shutil.rmtree(self._bandvis)
        shutil.rmtree(self._bandcal)


    def test_takesMs(self):
        """ Check that the calibrater tool can open and close an MS """

        cb.open(self._vis)
        # Check the cache to see that the table is opened
        cache = tb.showcache()
        self.assertTrue(len(cache) > 0)
        # Check that the cache is empty on closing
        cb.close()
        cache = tb.showcache()
        self.assertTrue(len(cache) == 0)

    def test_activityRecord(self):
        """ Check that using the calibrater to modify the vis shows in the activity record """

        # Open the ms and corrupt with a cal table
        cb.open(self._vis)
        cb.setapply(table=self._cal)
        cb.corrupt()
        # Check that the activity has been marked in the record
        actRecord = cb.activityrec()
        self.assertTrue(len(actRecord) > 0)

    def test_standardPath(self):
        """ open setapply setsolve state solve close """

        cb.setvi(old=True)
        cb.open(self._vis)
        cb.setapply(table=self._cal)
        cb.setsolve(table='output.ms')
        cb.state()
        cb.solve()
        cb.close()

        self.assertTrue(os.path.exists('output.ms'))

    def test_createEmpty(self):
        """ Check that an empty cal table can be created"""

        cb.open(self._vis)
        cb.createcaltable(caltable='testcalout.cal', partype='', caltype='GAIN', singlechan=True)

        self.assertTrue(os.path.exists('testcalout.cal'))

    def test_writeToCorrected(self):
        """ Check that the tool writes to the CORRECTED_DATA column """

        # Check that before using the calibrater there is not table col
        tb.open(self._vis)
        columns = tb.colnames()
        tb.close()

        self.assertFalse('CORRECTED_DATA' in columns)

        cb.open(self._vis)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        # Check that CORRECTED_DATA exists after calibration
        tb.open(self._vis)
        columns = tb.colnames()
        tb.close()

        self.assertTrue('CORRECTED_DATA' in columns)

    def test_done(self):
        """ Check that done closes the active calibrator tool """

        # Check that the cache is empty to begin with
        self.assertTrue(len(tb.showcache()) == 0, msg="The cache is not empty to begin with")
        cb.open(self._vis)
        self.assertTrue(len(tb.showcache()) > 0)
        cb.close()

    def test_reinitModel(self):
        """ Check that initcalset will reset the CORRECTED_DATA to unity """

        # Need to be using the old vis
        cb.setvi(old=True)


        # do a calibration so that there is a MODEL_DATA col
        cb.open(self._vis)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        # Get the corrected data
        tb.open(self._vis)
        beforecol = tb.getcol('CORRECTED_DATA')
        tb.close()

        # Now to reinitialize
        cb.open(self._vis)
        cb.initcalset()
        cb.close()

        # Get the corrected data after
        tb.open(self._vis)
        aftercol = tb.getcol('CORRECTED_DATA')
        observed = tb.getcol('DATA')
        tb.close()

        self.assertFalse(np.array_equal(beforecol, aftercol))
        self.assertTrue(np.array_equal(aftercol, observed))


    def test_resetSolveApply(self):
        """ Check that the reset function can clear set apply and solves """

        # Use setapply and setsolve that will be cleared later
        cb.open(self._vis)
        cb.setapply(table=self._cal)
        cb.setsolve(table=self._cal)
        cb.reset(solve=True, apply=True)

        # Both solve and correct should now fail due to the set apply and solve being reset
        try:
            cb.solve()
            self.fail()
        except RuntimeError:
            self.assertTrue(True)
        try:
            cb.correct()
            self.fail()
        except:
            self.assertTrue(True)


    # =============== SELECTION PARAMETERS =================

    def test_selectVisSpw(self):
        """Check that spw is properly selected by selectvis"""
        # First find all of the spw indexes where spw = 0
        rowswithspw = []
        rowswithoutspw = []
        tb.open(self._vis)
        datacol = tb.getcol('DATA_DESC_ID')

        for i in range(len(datacol)):
            if datacol[i] == 0:
                rowswithspw.append(i)
            else:
                rowswithoutspw.append(i)

        # Now save the DATA column to compare later
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Correct with selecting spw = 0
        cb.open(self._vis)
        cb.selectvis(spw=0)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        selectedrows = []
        unselectedrows = []

        # Compare the selected and unselected rows
        tb.open(self._vis)
        datacol = tb.getcol('CORRECTED_DATA')[0][0]

        for i in rowswithspw:
            if datacol[i] != beforedata[i]:
                selectedrows.append(True)

        for i in rowswithoutspw:
            if datacol[i] == beforedata[i]:
                unselectedrows.append(True)
        tb.close()

        # The selected rows should be all different and the unselected should be untouched
        self.assertTrue(len(rowswithspw) == len(selectedrows))
        self.assertTrue(len(rowswithoutspw) == len(unselectedrows))



    def test_selectVisTime(self):
        """Check that time is properly selected by selectvis"""
        # Get the data before cal
        tb.open(self._vis)
        before = tb.getcol('DATA')
        tb.close()

        # Correct with selecting times past 04:38:23
        cb.open(self._vis)
        cb.selectvis(time='>04:38:23')
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        tb.open(self._vis)
        after = tb.getcol('CORRECTED_DATA')
        tb.close()

        # Find if the resulting data is different
        self.assertFalse(np.array_equal(before, after))


    def test_selectVisScan(self):
        """Check tht the scan is properly selected by selectvis"""
        rowswithscan = []
        rowswithoutscan = []
        tb.open(self._vis)
        datacol = tb.getcol('SCAN_NUMBER')

        for i in range(len(datacol)):
            if datacol[i] == 2:
                rowswithscan.append(i)
            else:
                rowswithoutscan.append(i)

        # Now save the DATA column to compare later
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Correct with selecting spw = 0
        cb.open(self._vis)
        cb.selectvis(scan=2)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        selectedrows = []
        unselectedrows = []

        # Compare the selected and unselected rows
        tb.open(self._vis)
        datacol = tb.getcol('CORRECTED_DATA')[0][0]

        for i in rowswithscan:
            if datacol[i] != beforedata[i]:
                selectedrows.append(True)

        for i in rowswithoutscan:
            if datacol[i] == beforedata[i]:
                unselectedrows.append(True)
        tb.close()

        # The selected rows should be all different and the unselected should be untouched
        self.assertTrue(len(rowswithscan) == len(selectedrows))
        self.assertTrue(len(rowswithoutscan) == len(unselectedrows))

    def test_selectVisField(self):
        """Check that the field is properly selected by selectvis"""
        rowswithfield = []
        rowswithoutfield = []
        tb.open(self._vis)
        datacol = tb.getcol('FIELD_ID')

        for i in range(len(datacol)):
            if datacol[i] == 0:
                rowswithfield.append(i)
            else:
                rowswithoutfield.append(i)

        # Now save the DATA column to compare later
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Correct with selecting spw = 0
        cb.open(self._vis)
        cb.selectvis(field=0)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        selectedrows = []
        unselectedrows = []

        # Compare the selected and unselected rows
        tb.open(self._vis)
        datacol = tb.getcol('CORRECTED_DATA')[0][0]

        for i in rowswithfield:
            if datacol[i] != beforedata[i]:
                selectedrows.append(True)

        for i in rowswithoutfield:
            if datacol[i] == beforedata[i]:
                unselectedrows.append(True)
        tb.close()

        # The selected rows should be all different and the unselected should be untouched
        self.assertTrue(len(rowswithfield) == len(selectedrows))
        self.assertTrue(len(rowswithoutfield) == len(unselectedrows))

    def test_selectVisIntent(self):
        """Check that the intent is properly selected by selectvis"""
        # Modify the ms source table to have new intents
        tb.open(self._vis + '/STATE', nomodify=False)
        tb.addrows(1)
        data = tb.getcol('OBS_MODE')
        data[1] = 'CALIBRATE_DELAY#ON_SOURCE,CALIBRATE_PHASE#ON_SOURCE,CALIBRATE_WVR#ON_SOURCE'
        tb.putcol('OBS_MODE', data)
        tb.close()

        # Modify the main table to look at the new intents in some areas
        tb.open(self._vis, nomodify=False)
        data = tb.getcol('STATE_ID')
        data[10:5000]
        tb.putcol('STATE_ID', data)
        tb.close()

        tb.open(self._vis)
        # Now save the DATA column to compare later
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Correct with selecting intent on AMPLI
        cb.open(self._vis)
        cb.selectvis(intent='*AMPLI*')
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        # Compare the selected and unselected rows
        tb.open(self._visObs)
        datacol = tb.getcol('CORRECTED_DATA')[0][0]

        self.assertFalse(np.array_equal(datacol, beforedata))


    def test_selectVisObs(self):
        # Edit table to have multiple obs IDs
        tb.open(self._vis, nomodify=False)
        obsids = tb.getcol('OBSERVATION_ID')
        obsids[10:10000] = 1
        tb.putcol('OBSERVATION_ID', obsids)
        tb.close()
        # Add a new row to the observation subtable
        tb.open(self._vis+'/OBSERVATION', nomodify=False)
        tb.copyrows(self._vis+'/OBSERVATION')
        tb.close()

        tb.open(self._cal, nomodify=False)
        obsids = tb.getcol('OBSERVATION_ID')
        obsids[20:30] = 1
        tb.putcol('OBSERVATION_ID', obsids)
        tb.close()

        tb.open(self._vis)
        # Now save the DATA column to compare later
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Correct with selecting observation = 0
        cb.open(self._vis)
        cb.selectvis(observation=0)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        # Compare the selected and unselected rows
        tb.open(self._visObs)
        datacol = tb.getcol('CORRECTED_DATA')[0][0]

        self.assertFalse(np.array_equal(datacol, beforedata))

    def test_selectVisUVrange(self):
        """Check that selectvis properly selects for uvrange"""

        # Get the old results
        tb.open(self._visObs)
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Run calibration with uvrange selection
        cb.open(self._vis)
        cb.selectvis(uvrange='> 500000lambda')
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        # Get the new results
        tb.open(self._vis)
        afterdata = tb.getcol('DATA')[0][0]
        tb.close()

        # Make sure the data has been modified
        issame = True

        for i in range(len(beforedata)):
            if beforedata[i] != afterdata[i]:
                issame = False
                break

        self.assertFalse(issame)

    def test_selectVisBaseline(self):
        """Check that selectvis properly selects baseline/antenna"""
        rowswithant = []
        rowswithoutant = []
        tb.open(self._vis)
        datacol = tb.getcol('ANTENNA1')
        datacol2 = tb.getcol('ANTENNA2')

        for i in range(len(datacol)):
            if datacol[i] == 0 or datacol2[i] == 0:
                rowswithant.append(i)
            else:
                rowswithoutant.append(i)

        # Now save the DATA column to compare later
        beforedata = tb.getcol('DATA')[0][0]
        tb.close()

        # Correct with selecting spw = 0
        cb.open(self._vis)
        cb.selectvis(baseline=0)
        cb.setapply(table=self._cal)
        cb.correct()
        cb.close()

        selectedrows = []
        unselectedrows = []

        # Compare the selected and unselected rows
        tb.open(self._vis)
        datacol = tb.getcol('CORRECTED_DATA')[0][0]

        for i in rowswithant:
            if datacol[i] != beforedata[i]:
                selectedrows.append(True)

        for i in rowswithoutant:
            if datacol[i] == beforedata[i]:
                unselectedrows.append(True)
        tb.close()

        print(len(rowswithant), len(selectedrows))
        print(len(rowswithoutant), len(unselectedrows))

        # The selected rows should be all different and the unselected should be untouched
        self.assertTrue(len(rowswithant) == len(selectedrows))
        self.assertTrue(len(rowswithoutant) == len(unselectedrows))

    # =====================================================

    def test_setCalLib(self):
        """ Check that a provided cal table can be used to corrupt the MODEL_DATA """

        cb.open(self._vis)
        thiscallib = cb.parsecallibfile(self._lib)
        cb.setcallib(thiscallib)
        cb.corrupt()
        cb.close()

        tb.open(self._vis)
        ref = np.mean(tb.getcol('MODEL_DATA'))
        tb.close()

        self.assertTrue(np.isclose(ref, (0.3162506820017762+0.0490544367995527j)))

    def test_setCorrDepFlags(self):
        """ Check that corrdepflags will be checked """

        # Should I check the return code or log?
        cb.open(self._vis)
        result = cb.setcorrdepflags(corrdepflags=True)
        # This is simply checking the return code of the function
        self.assertTrue(result, msg="Setting of corrdepflags has failed")

    def test_solveBandpass(self):
        """ Check that solve band poly creates the output table"""

        # Use the old visibility
        cb.setvi(old=True, quiet=False)
        # Run the solve
        cb.open(self._bandvis)
        cb.setsolvebandpoly(table='bpoly', degamp=5, degphase=7)
        cb.solve()
        cb.close()
        # The bpoly table should have been created
        self.assertTrue(os.path.exists('bpoly'))

    def test_solveGainspline(self):
        """ Check that solve gain spline creates the output table """

        cb.setvi(old=True, quiet=False)
        cb.open(self._bandvis)
        cb.setsolvegainspline(table='gainspline', mode='AMP', splinetime=10800.0)
        cb.solve()
        cb.close()

        tb.open('gainspline')
        nPolyAmp = tb.getcol('N_POLY_AMP')
        nPolyPhase = tb.getcol('N_POLY_PHASE')
        tb.close()

        # Check that the table was created
        self.assertTrue(os.path.exists('gainspline'))
        self.assertTrue(np.all(nPolyAmp == 8))
        self.assertTrue(np.all(nPolyPhase == 0))

    def test_smoothedCalTables(self):
        """ Check that the smooth command creates a smoothed cal table """

        # Open the caltable and run smooth
        cb.open(self._bandvis)
        cb.smooth(tablein=self._bandcal, tableout='testcalout.cal', smoothtype='mean', smoothtime=5000.0, field=1)
        cb.close()

        tb.open(self._bandcal)
        olddata = tb.getcol('CPARAM')
        tb.close()

        tb.open('testcalout.cal')
        data = tb.getcol('CPARAM')
        tb.close()

        # Check that the smoothed data is different than the original cal
        self.assertFalse(np.array_equal(olddata, data))
        # Compare the smoothed average to reference
        self.assertTrue(np.isclose(np.mean(data), (1.4439346407141005+0.017319496272897555j)))

    def test_specifyCal(self):
        """ Check that specifycal can set values for specific spws and antennas"""

        cb.open(self._vis)
        cb.specifycal(caltable='testcalout.cal', spw='1', caltype='G', parameter=[3.0])
        cb.close()

        tb.open('testcalout.cal')
        data = tb.getcol('CPARAM')
        tb.close()

        self.assertTrue(np.all(data[0][0][10:20] == (3.+0.j)))

    def test_corruptCal(self):
        """ Check that the MS is corrupted using the cal table """

        cb.open(self._vis)
        cb.setapply(table=self._cal)
        cb.corrupt()
        cb.close()

        tb.open(self._vis)
        columns = tb.colnames()
        modelData = tb.getcol('MODEL_DATA')
        corData = tb.getcol('CORRECTED_DATA')
        data = tb.getcol('DATA')
        tb.close()

        # Check that both the CORRECTED_DATA and MODEL_DATA columns are created
        self.assertTrue('CORRECTED_DATA' in columns)
        self.assertTrue('MODEL_DATA' in columns)

        # Check that the MODEL_DATA has been modified by the caltable
        np.isclose(np.mean(modelData), (0.3162506820017762+0.0490544367995527j))
        # Check that the CORRECTED_DATA is unchanged
        np.array_equal(corData, data)

    def test_returnDict(self):
        """ Check that the returndict function gives a dictonary with the expected keys """
        cb.open(self._vis)
        expectedKeys =  ['apply_tables', 'selectvis', 'solve_tables']
        res = cb.returndict()
        beforeSpw = res['selectvis']['spw']
        resKeys = res.keys()

        cb.selectvis(spw='1')
        res = cb.returndict()
        afterSpw = res['selectvis']['spw']

        for key in resKeys:
            self.assertTrue(key in expectedKeys, msg=key + " not in expected keys")
        self.assertTrue(len(resKeys) == len(expectedKeys))
        self.assertFalse(np.array_equal(beforeSpw, afterSpw))
        self.assertTrue(np.array_equal(afterSpw, [1]))

if __name__ == '__main__':
    unittest.main()
