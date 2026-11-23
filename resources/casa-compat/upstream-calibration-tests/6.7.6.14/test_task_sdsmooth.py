########################################################################
# test_task_sdsmooth.py
#
# Copyright (C) 2018
# Associated Universities, Inc. Washington DC, USA
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.single.sdsmooth.html
#
#
##########################################################################
import math
import os
import re
import shutil
import sys
import unittest

import numpy
from scipy import signal

from casatasks import sdsmooth
from casatasks.private.sdutil import calibrater_manager, table_manager
from casatools import ctsys, ms, table

tb = table()


def gaussian_kernel(nchan, kwidth):
    sigma = kwidth / (2.0 * math.sqrt(2.0 * math.log(2.0)))
    g = signal.windows.gaussian(nchan, sigma, False)
    g /= g.sum()
    g0 = g[0]
    g[:-1] = g[1:]
    g[-1] = g0
    return g


class sdsmooth_test_base(unittest.TestCase):
    """Base class for sdsmooth unit test.

    The following attributes/functions are defined here.

        datapath
        decorators (invalid_argument_case, exception_case)
    """

    # Data path of input
    datapath = ctsys.resolve('unittest/sdsmooth/')

    # Input
    infile_data = 'tsdsmooth_test.ms'
    infile_float = 'tsdsmooth_test_float.ms'

    # task execution result
    result = None

    @property
    def outfile(self):
        return self.infile.rstrip('/') + '_out'

    # decorators
    @staticmethod
    def invalid_argument_case(func):
        """Decorator for the test case that is intended to fail due to invalid argument."""
        import functools

        @functools.wraps(func)
        def wrapper(self):
            func(self)
            self.assertFalse(self.result, msg='The task must return False')
        return wrapper

    @staticmethod
    def exception_case(exception_type, exception_pattern):
        """Decorator for the test case that is intended to throw exception.

        Args:
            exception_type: type of exception
            exception_pattern: regex for inspecting exception message
                               using re.search
        """
        def wrapper(func):
            import functools

            @functools.wraps(func)
            def _wrapper(self):
                self.assertTrue(len(exception_pattern) > 0, msg='Internal Error')
                with self.assertRaises(exception_type) as ctx:
                    func(self)
                    self.fail(msg='The task must throw exception')
                the_exception = ctx.exception
                message = str(the_exception)
                self.assertIsNotNone(re.search(exception_pattern, message),
                                     msg='error message \'%s\' is not expected.' % (message))
            return _wrapper
        return wrapper

    @staticmethod
    def weight_case(func):
        import functools

        @functools.wraps(func)
        def wrapper(self):
            with table_manager(self.infile) as tb:
                for irow in range(tb.nrows()):
                    self.assertTrue(tb.iscelldefined('WEIGHT_SPECTRUM', irow))

            # weight mode flag
            self.weight_propagation = True

            func(self)

        return wrapper

    def run_test(self, *args, **kwargs):
        datacol_name = self.datacolumn.upper()
        weight_mode = hasattr(self, 'weight_propagation') and getattr(
            self, 'weight_propagation') is True

        if 'kwidth' in kwargs:
            kwidth = kwargs['kwidth']
        else:
            kwidth = 5

        self.result = sdsmooth(infile=self.infile, outfile=self.outfile,
                               kernel='gaussian', datacolumn=self.datacolumn, **kwargs)

        # sanity check
        self.assertIsNone(self.result, msg='The task must complete without error')
        self.assertTrue(os.path.exists(self.outfile), msg='Output file is not properly created.')

        if 'spw' in kwargs:
            spw = kwargs['spw']
        else:
            spw = ''
        dd_selection = None
        if len(spw) == 0:
            expected_nrow = 2
            with table_manager(self.infile) as tb:
                data_in = tb.getvarcol(datacol_name)
                flag_in = tb.getvarcol('FLAG')
                if weight_mode:
                    weight_in = tb.getvarcol('WEIGHT_SPECTRUM')
        else:
            myms = ms()
            a = myms.msseltoindex(self.infile, spw=spw)
            spw_selection = a['spw']
            dd_selection = a['dd']
            expected_nrow = len(spw_selection)
            with table_manager(self.infile) as tb:
                try:
                    tsel = tb.query('DATA_DESC_ID IN %s' % (dd_selection.tolist()))
                    data_in = tsel.getvarcol(datacol_name)
                    flag_in = tsel.getvarcol('FLAG')
                    if weight_mode:
                        weight_in = tsel.getvarcol('WEIGHT_SPECTRUM')
                finally:
                    tsel.close()

        with table_manager(self.outfile) as tb:
            nrow = tb.nrows()
            data_out = tb.getvarcol(datacol_name)
            # flag_out = tb.getvarcol('FLAG')
            if weight_mode:
                weight_out = tb.getvarcol('WEIGHT_SPECTRUM')

        # verify nrow
        self.assertEqual(
            nrow, expected_nrow,
            msg=f'Number of rows mismatch (expected {expected_nrow} actual {nrow})')

        # verify data
        eps = 1.0e-6
        for key in data_out.keys():
            row_in = data_in[key]
            flg_in = flag_in[key]
            row_in[numpy.where(flg_in == True)] = 0.0
            row_out = data_out[key]
            self.assertEqual(row_in.shape, row_out.shape, msg='Shape mismatch in row %s' % (key))

            npol, nchan, _ = row_out.shape
            kernel_array = gaussian_kernel(nchan, kwidth)
            expected = numpy.convolve(row_in[0, :, 0], kernel_array, mode='same')
            output = row_out[0, :, 0]
            zero_index = numpy.where(numpy.abs(expected) <= eps)
            self.assertTrue(all(numpy.abs(output[zero_index]) < eps),
                            msg='Failed to verify zero values: row %s' % (key))
            nonzero_index = numpy.where(numpy.abs(expected) > eps)
            diff = numpy.abs((output[nonzero_index] - expected[nonzero_index]
                              ) / expected[nonzero_index].max())
            # print diff
            # print output[nonzero_index]
            # print expected[nonzero_index]
            self.assertTrue(all(diff < eps), msg='Failed to verify nonzero values: row %s' % (key))
            # print 'row_in', row_in[0,:,0].tolist()
            # print 'gaussian', kernel_array.tolist()
            # print 'expected', expected.tolist()
            # print 'result', row_out[0,:,0].tolist()

            # weight check if this is weight test
            if weight_mode:
                # print 'Weight propagation test'
                wgt_in = weight_in[key]
                wgt_out = weight_out[key]
                wkwidth = int(kwidth + 0.5)
                wkwidth += (1 if wkwidth % 2 == 0 else 0)
                half_width = wkwidth // 2
                peak_chan = kernel_array.argmax()
                start_chan = peak_chan - half_width
                wkernel = kernel_array[start_chan:start_chan + wkwidth].copy()
                wkernel /= sum(wkernel)
                weight_expected = wgt_in.copy()
                for ichan in range(half_width, nchan - half_width):
                    s = numpy.zeros(npol, dtype=float)
                    for jchan in range(wkwidth):
                        s += wkernel[jchan] * wkernel[jchan] / \
                            wgt_in[:, ichan - half_width + jchan, 0]
                    weight_expected[:, ichan, 0] = 1.0 / s
                # print weight_expected[:,:10]
                diff = numpy.abs((wgt_out - weight_expected) / weight_expected)
                self.assertTrue(all(diff.flatten() < eps),
                                msg='Failed to verify spectral weight: row %s' % (key))

    def _setUp(self, files, task):
        for f in files:
            if os.path.exists(f):
                shutil.rmtree(f)
            shutil.copytree(os.path.join(self.datapath, f), f)

    def _tearDown(self, files):
        for f in files:
            if os.path.exists(f):
                shutil.rmtree(f)

    def setUp(self):
        self._setUp([self.infile], sdsmooth)

    def tearDown(self):
        self._tearDown([self.infile, self.outfile])


class sdsmooth_test_fail(sdsmooth_test_base):
    """
    Unit test for task sdsmooth.

    The list of tests:
    test_sdsmooth_fail01 --- default parameters (raises an error)
    test_sdsmooth_fail02 --- invalid kernel type
    test_sdsmooth_fail03 --- invalid selection (empty selection result)
    test_sdsmooth_fail04 --- outfile exists (overwrite=False)
    test_sdsmooth_fail05 --- empty outfile
    test_sdsmooth_fail06 --- invalid data column name
    """

    invalid_argument_case = sdsmooth_test_base.invalid_argument_case
    exception_case = sdsmooth_test_base.exception_case

    infile = sdsmooth_test_base.infile_data

    @invalid_argument_case
    def test_sdsmooth_fail01(self):
        """test_sdsmooth_fail01 --- default parameters (raises an error)."""
        # casatasks throw exceptions, CASA5 tasks return False
        self.assertRaises(Exception, sdsmooth)

    @invalid_argument_case
    def test_sdsmooth_fail02(self):
        """test_sdsmooth_fail02 --- invalid kernel type."""
        # casatasks throw exceptions, CASA5 tasks return False
        self.assertRaises(Exception, sdsmooth, infile=self.infile,
                          kernel='normal', outfile=self.outfile)

    @exception_case(RuntimeError, 'Spw Expression: No match found for 3')
    def test_sdsmooth_fail03(self):
        """test_sdsmooth_fail03 --- invalid selection (empty selection result)."""
        self.result = sdsmooth(infile=self.infile, kernel='gaussian', outfile=self.outfile, spw='3')

    @exception_case(Exception, r'sdsmooth_test\.ms_out exists\.')
    def test_sdsmooth_fail04(self):
        """test_sdsmooth_fail04 --- outfile exists (overwrite=False)."""
        shutil.copytree(self.infile, self.outfile)
        self.result = sdsmooth(infile=self.infile, kernel='gaussian',
                               outfile=self.outfile, overwrite=False)

    @exception_case(Exception, r'outfile is empty\.')
    def test_sdsmooth_fail05(self):
        """test_sdsmooth_fail05 --- empty outfile."""
        self.result = sdsmooth(infile=self.infile, kernel='gaussian', outfile='')

    @invalid_argument_case
    def test_sdsmooth_fail06(self):
        """test_sdsmooth_fail06 --- invalid data column name."""
        # casatasks throw exceptions, CASA5 tasks return False
        self.assertRaises(Exception, sdsmooth, infile=self.infile,
                          outfile=self.outfile, kernel='gaussian', datacolumn='spectra')


class sdsmooth_test_complex(sdsmooth_test_base):
    """
    Unit test for task sdsmooth. Process MS having DATA column.

    The list of tests:
    test_sdsmooth_complex_fail01 --- non-existing data column (FLOAT_DATA)
    test_sdsmooth_complex_gauss01 --- gaussian smoothing (kwidth 5)
    test_sdsmooth_complex_gauss02 --- gaussian smoothing (kwidth 3)
    test_sdsmooth_complex_select --- data selection (spw)
    test_sdsmooth_complex_overwrite --- overwrite existing outfile (overwrite=True)
    """

    exception_case = sdsmooth_test_base.exception_case
    infile = sdsmooth_test_base.infile_data
    datacolumn = 'data'

    @exception_case(RuntimeError, r'Desired column \(FLOAT_DATA\) not found in the input MS')
    def test_sdsmooth_complex_fail01(self):
        """test_sdsmooth_complex_fail01 --- non-existing data column (FLOAT_DATA)."""
        self.result = sdsmooth(infile=self.infile, outfile=self.outfile,
                               kernel='gaussian', datacolumn='float_data')

    def test_sdsmooth_complex_gauss01(self):
        """test_sdsmooth_complex_gauss01 --- gaussian smoothing (kwidth 5)."""
        self.run_test(kwidth=5)

    def test_sdsmooth_complex_gauss02(self):
        """test_sdsmooth_complex_gauss02 --- gaussian smoothing (kwidth 3)."""
        self.run_test(kwidth=3)

    def test_sdsmooth_complex_select(self):
        """test_sdsmooth_complex_select --- data selection (spw)."""
        self.run_test(kwidth=5, spw='1')

    def test_sdsmooth_complex_overwrite(self):
        """test_sdsmooth_complex_overwrite --- overwrite existing outfile (overwrite=True)."""
        shutil.copytree(self.infile, self.outfile)
        self.run_test(kwidth=5, overwrite=True)


class sdsmooth_test_float(sdsmooth_test_base):
    """
    Unit test for task sdsmooth. Process MS having FLOAT_DATA column.

    The list of tests:
    test_sdsmooth_float_fail01 --- non-existing data column (DATA)
    test_sdsmooth_float_gauss01 --- gaussian smoothing (kwidth 5)
    test_sdsmooth_float_gauss02 --- gaussian smoothing (kwidth 3)
    test_sdsmooth_float_select --- data selection (spw)
    test_sdsmooth_float_overwrite --- overwrite existing outfile (overwrite=True)
    """

    exception_case = sdsmooth_test_base.exception_case
    infile = sdsmooth_test_base.infile_float
    datacolumn = 'float_data'

    @exception_case(RuntimeError, r'Desired column \(DATA\) not found in the input MS')
    def test_sdsmooth_float_fail01(self):
        """test_sdsmooth_complex_fail01 --- non-existing data column (DATA)."""
        self.result = sdsmooth(infile=self.infile, outfile=self.outfile,
                               kernel='gaussian', datacolumn='data')

    def test_sdsmooth_float_gauss01(self):
        """test_sdsmooth_float_gauss01 --- gaussian smoothing (kwidth 5)."""
        self.run_test(kwidth=5)

    def test_sdsmooth_float_gauss02(self):
        """test_sdsmooth_float_gauss02 --- gaussian smoothing (kwidth 3)."""
        self.run_test(kwidth=3)

    def test_sdsmooth_float_select(self):
        """test_sdsmooth_float_select --- data selection (spw)."""
        self.run_test(kwidth=5, spw='1')

    def test_sdsmooth_float_overwrite(self):
        """test_sdsmooth_float_overwrite --- overwrite existing outfile (overwrite=True)."""
        shutil.copytree(self.infile, self.outfile)
        self.run_test(kwidth=5, overwrite=True)


class sdsmooth_test_weight(sdsmooth_test_base):
    """
    Unit test for task sdsmooth. Verify weight propagation.

    The list of tests:
    test_sdsmooth_weight_gauss01 --- gaussian smoothing (kwidth 5)
    test_sdsmooth_weight_gauss02 --- gaussian smoothing (kwidth 3)
    """

    weight_case = sdsmooth_test_base.weight_case
    infile = sdsmooth_test_base.infile_data
    datacolumn = 'data'

    def setUp(self):
        super(sdsmooth_test_weight, self).setUp()

        # initialize WEIGHT_SPECTRUM
        with calibrater_manager(self.infile) as cb:
            cb.initweights()

    @weight_case
    def test_sdsmooth_weight_gauss01(self):
        """test_sdsmooth_weight_gauss01 --- gaussian smoothing (kwidth 5)."""
        self.run_test(kwidth=5)

    @weight_case
    def test_sdsmooth_weight_gauss02(self):
        """test_sdsmooth_weight_gauss02 --- gaussian smoothing (kwidth 3)."""
        self.run_test(kwidth=3)


class sdsmooth_test_boxcar(sdsmooth_test_base):
    """
    Unit test for checking boxcar smoothing.

    The input data (sdsmooth_delta.ms) has data with the following features:
      in row0, pol0: 1 at index 100, 0 elsewhere,
      in row0, pol1: 1 at index 0 and 2047(i.e., at both ends), 0 elsewhere,
      in row1, pol0: 1 at index 10 and 11, 0 elsewhere,
      in row1, pol1: 0 throughout.
    If input spectrum has delta-function-like feature, the
    expected output spectrum will be smoothing kernel itself.
    As for the data at [row0, pol0], the output data will be:
      kwidth==1 -> spec[100] = 1
      kwidth==2 -> spec[100,101] = 1/2 (=0.5)
      kwidth==3 -> spec[99,100,101] = 1/3 (=0.333...)
      kwidth==4 -> spec[99,100,101,102] = 1/4 (=0.25)
      kwidth==5 -> spec[98,99,100,101,102] = 1/5 (=0.2)
      and so on.
    """

    infile = 'tsdsmooth_delta.ms'
    datacolumn = 'float_data'
    centers = {'00': [100], '01': [0, 2047], '10': [10, 11], '11': []}

    def _getLeftWidth(self, kwidth):
        assert(0 < kwidth)
        return (2 - kwidth) // 2

    def _getRightWidth(self, kwidth):
        assert(0 < kwidth)
        return kwidth // 2

    def _checkResult(self, spec, kwidth, centers, tol=5.0e-06):
        sys.stdout.write('testing kernel_width = ' + str(kwidth) + '...')
        for i in range(len(spec)):
            count = 0
            for j in range(len(centers)):
                lidx = centers[j] + self._getLeftWidth(kwidth)
                ridx = centers[j] + self._getRightWidth(kwidth)
                if (lidx <= i) and (i <= ridx):
                    count += 1
            value = count / float(kwidth)
            self.assertTrue(((spec[i] - value) < tol), msg='Failed.')
        sys.stdout.write('OK.\n')

    def setUp(self):
        super(sdsmooth_test_boxcar, self).setUp()

    def test000(self):
        # testing kwidth from 1 to 5.
        for kwidth in range(1, 6):
            sdsmooth(infile=self.infile, outfile=self.outfile, datacolumn=self.datacolumn,
                     overwrite=True, kernel='boxcar', kwidth=kwidth)
            with table_manager(self.outfile) as tb:
                for irow in range(tb.nrows()):
                    spec = tb.getcell(self.datacolumn.upper(), irow)
                    for ipol in range(len(spec)):
                        center = self.centers[str(irow) + str(ipol)]
                        self._checkResult(spec[ipol], kwidth, center)

    def test000_datacolumn_uppercase(self):
        # testing kwidth from 1 to 5.
        datacolumn = "FLOAT_DATA"
        for kwidth in range(1, 6):
            sdsmooth(infile=self.infile, outfile=self.outfile, datacolumn=datacolumn,
                     overwrite=True, kernel='boxcar', kwidth=kwidth)
            with table_manager(self.outfile) as tb:
                for irow in range(tb.nrows()):
                    spec = tb.getcell(datacolumn.upper(), irow)
                    for ipol in range(len(spec)):
                        center = self.centers[str(irow) + str(ipol)]
                        self._checkResult(spec[ipol], kwidth, center)


class sdsmooth_selection(sdsmooth_test_base, unittest.TestCase):
    infile = "analytic_type1.sm.ms"
    outfile = "smoothed.ms"
    common_param = dict(infile=infile, outfile=outfile,
                        kernel='boxcar', kwidth=5)
    selections = dict(intent=("CALIBRATE_ATMOSPHERE#OFF*", [1]),
                      antenna=("DA99", [1]),
                      field=("M1*", [0]),
                      spw=(">6", [1]),
                      timerange=("2013/4/28/4:13:21", [1]),
                      scan=("0~8", [0]),
                      pol=("YY", [1]))
    verbose = False

    def _get_selection_string(self, key):
        if key not in self.selections.keys():
            raise ValueError("Invalid selection parameter %s" % key)
        return {key: self.selections[key][0]}

    def _get_selected_row_and_pol(self, key):
        if key not in self.selections.keys():
            raise ValueError("Invalid selection parameter %s" % key)
        pols = [0, 1]
        rows = [0, 1]
        if key == 'pol':  # self.selection stores pol ids
            pols = self.selections[key][1]
        else:  # self.selection stores row ids
            rows = self.selections[key][1]
        return (rows, pols)

    def _get_reference(self, nchan, row_offset, pol_offset, datacol):
        if datacol.startswith("float"):
            col_offset = 10
        elif datacol.startswith("corr"):
            col_offset = 50
        else:
            raise ValueError("Got unexpected datacolumn.")
        spike_chan = col_offset + 20 * row_offset + 10 * pol_offset
        reference = numpy.zeros(nchan)
        reference[spike_chan - 2:spike_chan + 3] = 0.2
        if self.verbose:
            print("reference=%s" % str(reference))
        return reference

    def run_test(self, sel_param, datacolumn, reindex=True):
        inparams = self._get_selection_string(sel_param)
        inparams.update(self.common_param)
        sdsmooth(datacolumn=datacolumn, reindex=reindex, **inparams)
        self._test_result(inparams["outfile"], sel_param, datacolumn)

    def _test_result(self, msname, sel_param, dcol, atol=1.e-5, rtol=1.e-5):
        # Make sure output MS exists
        self.assertTrue(os.path.exists(msname), "Could not find output MS")
        # Compare output MS with reference (nrow, npol, and spectral values)
        (rowids, polids) = self._get_selected_row_and_pol(sel_param)
        if dcol.startswith("float"):
            testcolumn = "FLOAT_DATA"
        else:  # output is in DATA column
            testcolumn = "DATA"
        tb.open(msname)
        try:
            self.assertEqual(tb.nrows(), len(rowids),
                             "Row number is wrong %d (expected: %d)" % (tb.nrows(), len(rowids)))
            for out_row in range(len(rowids)):
                in_row = rowids[out_row]
                sp = tb.getcell(testcolumn, out_row)
                self.assertEqual(
                    sp.shape[0], len(polids),
                    f"Number of pol is wrong in row={out_row}: {len(polids)} " +
                    f"(expected: {sp.shape[0]})")
                nchan = sp.shape[1]
                for out_pol in range(len(polids)):
                    in_pol = polids[out_pol]
                    reference = self._get_reference(nchan, in_row, in_pol, dcol)
                    if self.verbose:
                        print("data=%s" % str(sp[out_pol]))
                    self.assertTrue(
                        numpy.allclose(sp[out_pol], reference, atol=atol, rtol=rtol),
                        f"Smoothed spectrum differs in row={out_row}, pol={out_pol}")
        finally:
            tb.close()

    def testIntentF(self):
        """Test selection by intent (float_data)."""
        self.run_test("intent", "float_data")

    def testIntentC(self):
        """Test selection by intent (corrected)."""
        self.run_test("intent", "corrected")

    def testAntennaF(self):
        """Test selection by antenna (float_data)."""
        self.run_test("antenna", "float_data")

    def testAntennaC(self):
        """Test selection by antenna (corrected)."""
        self.run_test("antenna", "corrected")

    def testFieldF(self):
        """Test selection by field (float_data)."""
        self.run_test("field", "float_data")

    def testFieldC(self):
        """Test selection by field (corrected)."""
        self.run_test("field", "corrected")

    def testSpwF(self):
        """Test selection by spw (float_data)."""
        self.run_test("spw", "float_data")

    def testSpwC(self):
        """Test selection by spw (corrected)."""
        self.run_test("spw", "corrected")

    def testTimerangeF(self):
        """Test selection by timerange (float_data)."""
        self.run_test("timerange", "float_data")

    def testTimerangeC(self):
        """Test selection by timerange (corrected)."""
        self.run_test("timerange", "corrected")

    def testScanF(self):
        """Test selection by scan (float_data)."""
        self.run_test("scan", "float_data")

    def testScanC(self):
        """Test selection by scan (corrected)."""
        self.run_test("scan", "corrected")

    def testPolF(self):
        """Test selection by pol (float_data)."""
        self.run_test("pol", "float_data")

    def testPolC(self):
        """Test selection by pol (corrected)."""
        self.run_test("pol", "corrected")

    def testReindexSpw(self):
        """Test reindex =T/F in spw selection."""
        outfile = self.common_param['outfile']
        for datacol in ['float_data', 'corrected']:
            print("Test: %s" % datacol.upper())
            for (reindex, ddid, spid) in zip([True, False], [0, 1], [0, 7]):
                print("- reindex=%s" % str(reindex))
                self.run_test("spw", datacol, reindex=reindex)
                tb.open(outfile)
                try:
                    self.assertEqual(ddid, tb.getcell('DATA_DESC_ID', 0),
                                     "comparison of DATA_DESCRIPTION_ID failed.")
                finally:
                    tb.close()
                tb.open(outfile + '/DATA_DESCRIPTION')
                try:
                    self.assertEqual(spid, tb.getcell('SPECTRAL_WINDOW_ID', ddid),
                                     "comparison of SPW_ID failed.")
                finally:
                    tb.close()
                shutil.rmtree(outfile)

    def testReindexIntent(self):
        """Test reindex =T/F in intent selection."""
        outfile = self.common_param['outfile']
        for datacol in ['float_data', 'corrected']:
            print("Test: %s" % datacol.upper())
            for (reindex, idx) in zip([True, False], [0, 4]):
                print("- reindex=%s" % str(reindex))
                self.run_test("intent", datacol, reindex=reindex)
                tb.open(outfile)
                try:
                    self.assertEqual(idx, tb.getcell('STATE_ID', 0),
                                     "comparison of state_id failed.")
                finally:
                    tb.close()
                shutil.rmtree(outfile)


if __name__ == '__main__':
    unittest.main()
