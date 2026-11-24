########################################################################
# test_task_sdgaincal.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.single.sdgaincal.html
#
#
##########################################################################
import os
import shutil
import unittest

import numpy

from casatasks import mstransform, sdcal, sdgaincal
from casatasks.private.sdutil import table_manager
from casatools import ctsys
from casatools import ms as mstool
from casatools import table as tbtool

# this distinction doesn't exist in casatasks
mstransform_cli = mstransform
sdcal_cli = sdcal


class sdgaincal_test_base(unittest.TestCase):
    """
    Base class for sdgainal unit tests.

    This class defines attributes and methods common to test cases
    """

    datapath = ctsys.resolve('unittest/sdgaincal/')

    def __copy_from_datapath(self, filename):
        if os.path.exists(filename):
            shutil.rmtree(filename)
        shutil.copytree(os.path.join(self.datapath, filename), filename)

    def setUp(self):
        self.__copy_from_datapath(self.infile)

        if hasattr(self, 'reffile'):
            self.__copy_from_datapath(self.reffile)

    def tearDown(self):
        to_be_removed = [self.infile, self.outfile]
        if hasattr(self, 'reffile'):
            to_be_removed.append(self.reffile)
        for f in to_be_removed:
            if os.path.exists(f):
                shutil.rmtree(f)

    def generate_params(self, **params):
        default_params = {'infile': self.infile,
                          'outfile': self.outfile,
                          'overwrite': False,
                          'calmode': 'doublecircle',
                          'radius': '',
                          'smooth': True,
                          'antenna': '',
                          'spw': '',
                          'field': '',
                          'spw': '',
                          'scan': '',
                          'applytable': '',
                          'interp': '',
                          'spwmap': []}
        retval = {}
        for (k, v) in default_params.items():
            if k in params:
                retval[k] = params[k]
            else:
                retval[k] = v
        return retval

    def run_task(self, **params):
        result = sdgaincal(**params)
        return result

    def _verify_caltable(self, custom_check_func, **params):
        caltable = params['outfile']

        # basic check
        self.inspect_caltable(caltable)

        custom_check_func(**params)

    def inspect_caltable(self, caltable):
        # caltable must exist
        self.assertTrue(os.path.exists(caltable))

        # caltable must be a directory
        self.assertTrue(os.path.isdir(caltable))

        # caltable must be opened by table tool
        tb = tbtool()
        is_open_successful = tb.open(caltable)
        self.assertTrue(is_open_successful)

        try:
            # caltype must be "G Jones"
            caltype = tb.getkeyword('VisCal')
            self.assertEqual(caltype, "SDGAIN_OTFD")
            # paramter must be Complex
            partype = tb.getkeyword('ParType')
            self.assertEqual(partype, 'Complex')
            self.assertIn('CPARAM', tb.colnames())
        finally:
            tb.close()

    def _generic_verify(self, **params):
        tb = tbtool()
        ms = mstool()

        nrow_per_spw = 102

        spwsel = params['spw']
        if spwsel == '':
            nspw = 2
        else:
            infile = params['infile']
            ms.open(infile)
            try:
                ms.msselect({'spw': spwsel})
                mssel = ms.msselectedindices()
                nspw = len(mssel['spw'])
            finally:
                ms.close()

        caltable = params['outfile']
        tb.open(caltable)
        try:
            nrow = tb.nrows()
            self.assertEqual(nrow, nspw * nrow_per_spw)

            spwids = tb.getcol('SPECTRAL_WINDOW_ID')
            spwid_list = set(spwids)
            for spwid in spwid_list:
                self.assertEqual(len(spwids[spwids == spwid]), nrow_per_spw)

            # by definition, mean of gain factor becomes almost 1.0
            for spwid in spwid_list:
                t = tb.query('SPECTRAL_WINDOW_ID == %s' % (spwid))
                try:
                    fparam = t.getcol('CPARAM').real
                    flag = t.getcol('FLAG')
                finally:
                    t.close()
                ma = numpy.ma.masked_array(fparam, flag)
                mean_gain = ma.mean(axis=2)
                print(mean_gain)
                npol = fparam.shape[0]
                for ipol in range(npol):
                    if numpy.any(flag[ipol] == False):
                        self.assertTrue(abs(mean_gain[ipol] - 1.0) < 0.01)

            self._verify_param_and_flag(tb)

        finally:
            tb.close()

    def _verify_param_and_flag(self, table):
        self.fail('_verify_param_and_flag not implemented')


class sdgaincal_fail_test(sdgaincal_test_base):
    """
    Unit tests for task sdgaincal.

    The list of tests:
    Test Name       | Reason
    ==========================================================================
    test_fail01     | infile not exist
    test_fail02     | not overwrite existing outfile
    test_fail03     | wrong calibration mode
    test_fail04     | negative radius
    """

    infile = 'doublecircletest_const.ms'
    outfile = 'sdgaincal_fail_test.sdgain.caltable'

    def _test_fail(self, **params):
        result = self.run_task(**params)
        self.assertEqual(result, False)

    def _test_except_regex(self, exception_type, pattern, **params):
        with self.assertRaisesRegex(exception_type, pattern):
            self.run_task(**params)

    def test_fail01(self):
        """test_fail01: infile not exist."""
        params = self.generate_params(infile=self.infile + '.notexist')
        # casatasks throw exceptions to indicate failure
        self.assertRaises(Exception, self.run_task, **params)

    def test_fail02(self):
        """test_fail02: not overwrite existing outfile."""
        params = self.generate_params()

        # outfile exists
        shutil.copytree(params['infile'], params['outfile'])
        # these may be equivalent
        self.assertRaises(Exception, self.run_task, **params)

    def test_fail03(self):
        """test_fail03: wrong calibration mode."""
        params = self.generate_params(calmode='otf')
        # casatasks throw exceptions to indicate failure
        self.assertRaises(Exception, self.run_task, **params)

    def test_fail04(self):
        """test_fail04: negative radius."""
        params = self.generate_params(radius='-30arcsec')
        # these may be equivalent
        self.assertRaises(Exception, self.run_task, **params)


class sdgaincal_const_test(sdgaincal_test_base):
    """Unit tests for task sdgaincal.

    Test data contains the data constant over time and direction, which
    means that gain factor is always 1.0.

    The list of tests:
    Test Name        | Radius      | Expectation
    ==========================================================================
    test_const01     | ''          | too narrow central region, empty caltable is created
    test_const02     | '65arcsec'  | valid caltable is created. gain factor is all 1.0.
    test_const03     | ''          | overwrite existing file
    test_const04     | ''          | test if data selection works
    """

    infile = 'doublecircletest_const.ms'
    outfile = 'sdgaincal_const_test.sdgain.caltable'

    def _is_empty_caltable(self, **params):
        tb = tbtool()
        tb.open(params['outfile'])
        try:
            nrow = tb.nrows()
        finally:
            tb.close()
        self.assertEqual(nrow, 0)

    def _verify_param_and_flag(self, table):
        for irow in range(table.nrows()):
            fparam = table.getcell('CPARAM', irow).real
            self.assertTrue(numpy.all(fparam == 1.0))

            flag = table.getcell('FLAG', irow)
            self.assertTrue(numpy.all(flag == False))

    def test_const01(self):
        """test_const01: too narrow central region, empty caltable is created."""
        params = self.generate_params()
        self.run_task(**params)

        self._verify_caltable(self._is_empty_caltable, **params)

    def test_const02(self):
        """test_const02: valid caltable is created. gain factor is all 1.0."""
        params = self.generate_params(radius='65arcsec')
        self.run_task(**params)

        self._verify_caltable(self._generic_verify, **params)

    def test_const03(self):
        """test_const03: overwrite existing file."""
        params = self.generate_params(overwrite=True, radius='65arcsec')

        # outfile exists
        shutil.copytree(params['infile'], params['outfile'])

        self.run_task(**params)

        self._verify_caltable(self._generic_verify, **params)


class sdgaincal_variable_test(sdgaincal_test_base):
    """Unit tests for task sdgaincal.

    Gain calibration for variable data.

    The list of tests:
    Test Name           | Radius      | Expectation
    ==========================================================================
    test_variable01     | '65arcsec'  | valid caltable is created
    """

    infile = 'doublecircletest_autoscale.ms'
    outfile = 'sdgaincal_variable_test.sdgain.caltable'
    reffile = 'doublecircletest_autoscale.sdgain.caltable'

    def _verify_param_and_flag(self, table):
        reftable = tbtool()
        reftable.open(self.reffile)

        try:
            nrow = table.nrows()
            ref_nrow = reftable.nrows()
            self.assertEqual(nrow, ref_nrow)

            for irow in range(nrow):
                ref_fparam = reftable.getcell('CPARAM', irow).real
                fparam = table.getcell('CPARAM', irow).real
                self.assertTrue(numpy.all(ref_fparam == fparam))

                ref_flag = reftable.getcell('FLAG', irow)
                flag = table.getcell('FLAG', irow)
                self.assertTrue(numpy.all(ref_flag == flag))
        finally:
            reftable.close()

    def test_variable01(self):
        """test_variable01: valid caltable is created."""
        params = self.generate_params(radius='65arcsec')
        self.run_task(**params)

        self._verify_caltable(self._generic_verify, **params)


class sdgaincal_preapply_test(sdgaincal_test_base):
    """Unit tests for task sdgaincal.

    This class is intended to verify preapplication capability (CAS-8879).
    Test data contains the data constant over time and direction, which
    means that gain factor is always 1.0.

    The list of tests:
    Test Name        | Radius      | Expectation
    ==========================================================================
    test_preapply01  | '65arcsec'  | only sky caltable is applied (resulting const factor)
    test_preapply02  | '65arcsec'  | only tsys caltable is applied (resulting variable factor)
    test_preapply03  | '65arcsec'  | tsys and sky caltables are applied (resulting variable factor)
    test_preapply04  | '65arcsec'  | transfer Tsys from [2,3] to [0,1]
    """

    infile = 'doublecircletest_const.ms'
    outfile = 'sdgaincal_const_test.sdgain.caltable'
    tsystable = infile + '.tsys'
    skytable = infile + '.sky'

    def setUp(self):
        super(sdgaincal_preapply_test, self).setUp()

        # generate tsys and sky table
        self.generate()

    def tearDown(self):
        super(sdgaincal_preapply_test, self).tearDown()

        # remove tsys and sky table
        if os.path.exists(self.tsystable):
            shutil.rmtree(self.tsystable)

        if os.path.exists(self.skytable):
            shutil.rmtree(self.skytable)

    def _verify_param_and_flag_const(self, table):
        for irow in range(table.nrows()):
            param = table.getcell('CPARAM', irow).real
            self.assertTrue(numpy.all(param == 1.0))

            flag = table.getcell('FLAG', irow)
            self.assertTrue(numpy.all(flag == False))

    def _verify_param_and_flag_variable(self, table):
        nrow = table.nrows()
        nrow_per_spw = nrow // 2
        # ref_min = 0.90240508
        # ref_max = 1.08644176
        # delta = (ref_max - ref_min) / nrow_per_spw
        ref = numpy.array(
            [0.90240508, 0.90413946, 0.90609813, 0.90798497, 0.90980464,
             0.91156137, 0.91352099, 0.91541398, 0.91724426, 0.91901565,
             0.92073143, 0.9226175, 0.92444533, 0.92621815, 0.92793888,
             0.92961025, 0.93143708, 0.93321222, 0.93493825, 0.93661767,
             0.93825269, 0.94003046, 0.94176179, 0.94344884, 0.94509363,
             0.94669813, 0.94843435, 0.95012838, 0.95178211, 0.95339727,
             0.95497549, 0.95667583, 0.95833755, 0.95996231, 0.96155155,
             0.96310669, 0.96477556, 0.96640879, 0.96800792, 0.96957415,
             0.97110873, 0.97275752, 0.9743731, 0.9759568, 0.97924149,
             0.98238301, 0.98564625, 0.98889875, 0.99214059, 0.99537188,
             0.99845505, 1.00166595, 1.0048666, 1.00805712, 1.0111016,
             1.01427245, 1.0174334, 1.02058458, 1.02372611, 1.0252763,
             1.02685285, 1.02845693, 1.03008926, 1.03160965, 1.03315723,
             1.0347333, 1.03633857, 1.03782654, 1.03934276, 1.04088831,
             1.04246414, 1.04391682, 1.04539847, 1.04691041, 1.04845381,
             1.04986715, 1.05131042, 1.0527848, 1.05429184, 1.05566108,
             1.05706096, 1.05849302, 1.05995858, 1.06145918, 1.06282246,
             1.06421888, 1.06565022, 1.06711829, 1.06862497, 1.06997252,
             1.07135618, 1.07277775, 1.07423937, 1.07552946, 1.07685661,
             1.07822275, 1.07963037, 1.08108199, 1.08235824, 1.08367515,
             1.08503532, 1.08644176], dtype=numpy.float64)
        for irow in range(nrow):
            ref_param = ref[irow % nrow_per_spw]
            param = table.getcell('CPARAM', irow).real
            diff = numpy.abs((param - ref_param) / ref_param)
            self.assertTrue(numpy.all(diff < 1e-8),
                            msg=f'row {irow} actual {param[0, 0]} expected {ref_param}')
            # self.assertTrue(numpy.all(ref_param == param),
            #                msg=f'row {irow} actual {param[0, 0]} expected {ref_param}')

            ref_flag = False
            flag = table.getcell('FLAG', irow)
            self.assertTrue(numpy.all(flag == ref_flag))

            # print irow, param, flag

    def generate(self):
        # generate Tsys table
        # in casatasks, sdcal_cli IS sdcal
        sdcal_cli(infile=self.infile, outfile=self.tsystable,
                  calmode='tsys', overwrite=True)

        self.assertTrue(os.path.exists(self.tsystable))

        # get information from MS
        tb = tbtool()
        tb.open(self.infile)
        s = tb.getcol('FLOAT_DATA', 0, 2)
        w = tb.getcol('WEIGHT', 0, 2).reshape((2, 1, 2))
        t = tb.getcol('TIME')
        tmax = t.max()
        tmin = t.min()
        tb.close()

        # generate sky table based on Tsys table
        tb.open(self.tsystable)
        t = tb.copy(self.skytable, deep=True)
        tb.close()
        t.close()
        tb.open(self.skytable, nomodify=False)
        tb.putkeyword('VisCal', 'SDSKY_PS')
        s[:] = w
        tb.putcol('WEIGHT', s, 0, 2)
        s[:] = 1.0
        tb.putcol('FPARAM', s, 0, 2)
        tb.close()
        with open(self.skytable + '/table.info', 'r') as f:
            lines = f.read()
        # print lines
        lines = lines.replace('B TSYS', 'SDSKY_PS')
        # print lines
        with open(self.skytable + '/table.info', 'w') as f:
            f.write(lines)

        self.assertTrue(os.path.exists(self.skytable))

        # edit Tsys table
        tb.open(self.tsystable, nomodify=False)
        spw_id = tb.getcol('SPECTRAL_WINDOW_ID')
        spw_id[2:] = spw_id[:2]
        tb.putcol('SPECTRAL_WINDOW_ID', spw_id)
        time = tb.getcol('TIME')
        time[:2] = tmin
        time[2:] = tmax
        tb.putcol('TIME', time)
        param = tb.getcol('FPARAM')
        param[:, :, :2] = 100.0
        param[:, :, 2:] = 200.0
        tb.putcol('FPARAM', param)
        param[:] = 1.0
        tb.putcol('WEIGHT', param)
        tb.close()

    def _edit_tsys_spw(self, spwmap):
        tb = tbtool()
        tb.open(self.tsystable, nomodify=False)
        spw_id = tb.getcol('SPECTRAL_WINDOW_ID')
        print('before ', spw_id)
        for i in range(len(spw_id)):
            spw_id[i] = spwmap[spw_id[i]]
        print('after', spw_id)
        tb.putcol('SPECTRAL_WINDOW_ID', spw_id)
        tb.close()

    def test_preapply01(self):
        """test_preapply01: only sky caltable is applied (resulting const factor)."""
        params = self.generate_params(radius='65arcsec', applytable=self.skytable)
        self.run_task(**params)

        setattr(self, '_verify_param_and_flag', self._verify_param_and_flag_const)
        self._verify_caltable(self._generic_verify, **params)

    def test_preapply02(self):
        """test_preapply02: only tsys caltable is applied (resulting variable factor)."""
        params = self.generate_params(radius='65arcsec', applytable=self.tsystable)
        self.run_task(**params)

        setattr(self, '_verify_param_and_flag', self._verify_param_and_flag_variable)
        self._verify_caltable(self._generic_verify, **params)

    def test_preapply03(self):
        """test_preapply03: both tsys and sky caltables are applied (resulting variable factor)."""
        params = self.generate_params(radius='65arcsec',
                                      applytable=[self.tsystable, self.skytable])
        self.run_task(**params)

        setattr(self, '_verify_param_and_flag', self._verify_param_and_flag_variable)
        self._verify_caltable(self._generic_verify, **params)

    def test_preapply04(self):
        """test_preapply04: transfer Tsys from [2,3] to [0,1]."""
        # edit spwid [0,1] to [2,3]
        spwmap = [2, 3, 2, 3]
        self._edit_tsys_spw(spwmap=spwmap)
        params = self.generate_params(radius='65arcsec',
                                      applytable=[self.tsystable, self.skytable],
                                      interp='', spwmap=[spwmap, [-1]])
        self.run_task(**params)

        setattr(self, '_verify_param_and_flag', self._verify_param_and_flag_variable)
        self._verify_caltable(self._generic_verify, **params)


class sdgaincal_single_polarization_test(sdgaincal_test_base):
    """
    Unit tests for task sdgaincal.

    The list of tests:
    Test Name        | Radius      | Expectation
    ==========================================================================
    test_single_pol  | '65arcsec'  | test single-polarization calibration (YY)
    """

    infile = 'doublecircletest_const.ms'
    outfile = 'sdgaincal_const_test.sdgain.caltable'

    # for single-polarization test
    infile_YY = 'doublecircletest_const.YY.ms'

    def tearDown(self):
        super(sdgaincal_single_polarization_test, self).tearDown()

        if os.path.exists(self.infile_YY):
            shutil.rmtree(self.infile_YY)

    def _verify_param_and_flag(self, table):
        """Verify calibration factors and flags.

        Only first polarization is effective.
        Second polarization should be all flagged.
        """
        print('sdgaincal_single_polarization_test._verify_param_and_flag')
        for irow in range(table.nrows()):
            fparam = table.getcell('CPARAM', irow).real
            self.assertTrue(numpy.all(fparam[0] == 1.0))
            self.assertTrue(numpy.all(fparam[1] == 0.0))

            flag = table.getcell('FLAG', irow)
            self.assertTrue(numpy.all(flag[0] == False))
            self.assertTrue(numpy.all(flag[1] == True))

    def test_single_pol(self):
        """test_single_pol: test single-polarization calibration (YY)."""
        # generate single-polarization MS
        # for casatasks, mstransform_cli IS mstransform
        mstransform_cli(vis=self.infile, outputvis=self.infile_YY, correlation='YY',
                        datacolumn='float_data')

        self.assertTrue(os.path.exists(self.infile_YY))
        with table_manager(self.infile_YY) as tb:
            try:
                for irow in range(tb.nrows()):
                    flag = tb.getcell('FLAG', irow)
                    self.assertEqual(flag.shape[0], 1)
            finally:
                tb.close()

        params = self.generate_params(radius='65arcsec')
        params['infile'] = self.infile_YY
        self.run_task(**params)

        self._verify_caltable(self._generic_verify, **params)


if __name__ == '__main__':
    unittest.main()
