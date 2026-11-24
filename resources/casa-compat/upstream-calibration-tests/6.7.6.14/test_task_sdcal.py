########################################################################
# test_task_sdcal.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.single.sdcal.html
#
#
##########################################################################
import contextlib
import os
import re
import shutil
import unittest

import numpy

from casatasks import casalog, initweights, partition, sdcal
from casatasks.private.sdutil import table_manager, table_selector
from casatestutils import listing
from casatools import ctsys, measures, ms, table

tb = table()

ctsys_resolve = ctsys.resolve


@contextlib.contextmanager
def mmshelper(vis, separationaxis='auto'):
    outputvis = vis.rstrip('/') + '.mms'
    os.system('rm -rf {0}*'.format(outputvis))
    try:
        partition(vis=vis, outputvis=outputvis, separationaxis=separationaxis)
        if os.path.exists(outputvis):
            yield outputvis
        else:
            yield None
    finally:
        os.system('rm -rf {0}*'.format(outputvis))


class sdcal_test(unittest.TestCase):
    """
    Unit test for task sdcal.

    The list of tests:
    test00	--- default parameters (raises an error)
    test01	--- spwmap comprising list
    test02	--- spwmap comprising dictionary
    test03	--- spwmap comprising others
    test04	--- there is no infile
    test05
    """

    # Data path of input
    datapath = ctsys_resolve('unittest/sdcal/')

    # Input
    infile1 = 'uid___A002_X6218fb_X264.ms.sel'
    infiles = [infile1]
    tsystable = 'out.cal'

    def setUp(self):
        for infile in self.infiles:
            if os.path.exists(infile):
                shutil.rmtree(infile)
            shutil.copytree(os.path.join(self.datapath, infile), infile)

    def tearDown(self):
        for infile in self.infiles:
            if (os.path.exists(infile)):
                shutil.rmtree(infile)

        if os.path.exists(self.tsystable):
            shutil.rmtree(self.tsystable)

    def _compareOutFile(self, out, reference):
        self.assertTrue(os.path.exists(out))
        self.assertTrue(os.path.exists(reference), msg="Reference file doesn't exist: " + reference)
        self.assertTrue(listing.compare(out, reference),
                        f'New and reference files are different. {out} != {reference}. ')

    def test00(self):
        """Test00:Check the identification of TSYS_SPECTRuM and FPARAM."""
        # tid = "00"
        infile = self.infile1
        sdcal(infile=infile, calmode='tsys', outfile=self.tsystable)
        compfile1 = infile + '/SYSCAL'
        compfile2 = self.tsystable

        tb.open(compfile1)
        subt1 = tb.query('', sortlist='ANTENNA_ID, TIME, SPECTRAL_WINDOW_ID',
                         columns='TSYS_SPECTRUM')
        tsys1 = subt1.getcol('TSYS_SPECTRUM')
        tb.close()
        subt1.close()

        tb.open(compfile2)
        subt2 = tb.query('', sortlist='ANTENNA1, TIME, SPECTRAL_WINDOW_ID', columns='FPARAM, FLAG')
        tsys2 = subt2.getcol('FPARAM')
        flag = subt2.getcol('FLAG')

        tb.close()
        subt2.close()

        if (tsys1 == tsys2).all():
            print('')
            print('The shape of the MS/SYSCAL/TSYS_SPECTRUM', tsys1.shape)
            print('The shape of the FPARAM extracted with sdcal', tsys2.shape)
            print('Both tables are identical.')
        else:
            print('')
            print('The shape of the MS/SYSCAL/TSYS_SPECTRUM', tsys1.shape)
            print('The shape of the FPARAM of the extraction with sdcal', tsys2.shape)
            print('Both tables are not identical.')

        if flag.all() == 0:
            print('ALL FLAGs are set to zero.')

    def test00M(self):
        """Test00M:Check the identification of TSYS_SPECTRuM and FPARAM (MMS)."""
        # tid = "00M"
        infile = self.infile1
        with mmshelper(infile) as mvis:
            self.assertTrue(mvis is not None)
            sdcal(infile=mvis, calmode='tsys', outfile=self.tsystable)
        compfile1 = infile + '/SYSCAL'
        compfile2 = self.tsystable

        tb.open(compfile1)
        subt1 = tb.query('', sortlist='ANTENNA_ID, TIME, SPECTRAL_WINDOW_ID',
                         columns='TSYS_SPECTRUM')
        tsys1 = subt1.getcol('TSYS_SPECTRUM')
        tb.close()
        subt1.close()

        tb.open(compfile2)
        subt2 = tb.query('', sortlist='ANTENNA1, TIME, SPECTRAL_WINDOW_ID', columns='FPARAM, FLAG')
        tsys2 = subt2.getcol('FPARAM')
        flag = subt2.getcol('FLAG')

        tb.close()
        subt2.close()

        if (tsys1 == tsys2).all():
            print('')
            print('The shape of the MS/SYSCAL/TSYS_SPECTRUM', tsys1.shape)
            print('The shape of the FPARAM extracted with sdcal', tsys2.shape)
            print('Both tables are identical.')
        else:
            print('')
            print('The shape of the MS/SYSCAL/TSYS_SPECTRUM', tsys1.shape)
            print('The shape of the FPARAM of the extraction with sdcal', tsys2.shape)
            print('Both tables are not identical.')

        if flag.all() == 0:
            print('ALL FLAGs are set to zero.')

    def test01(self):
        """Test01: weight = 1/(SIGMA**2) X 1/(FPARAM_ave**2) dictionary version."""
        # focus on antenna1=0, data_disk_id=1
        # spwmap_dict={1:[1],3:[3],5:[5],7:[7]}

        # tid = "01"
        infile = self.infile1
        sdcal(infile=infile, calmode='tsys', outfile=self.tsystable)
        initweights(vis=infile, wtmode='nyq', dowtsp=True)
        # spwmap_list=[0,1,2,3,4,5,6,7,8,1,10,3,12,5,14,7,16]
        # spwmap_dict={1:[9],3:[11],5:[13],7:[15]}

        spwmap_dict = {1: [1], 3: [3], 5: [5], 7: [7]}
        sdcal(infile=infile, calmode='apply', spwmap=spwmap_dict,
              applytable=self.tsystable, outfile='')

        tb.open(infile)
        sigma00 = tb.getcol('SIGMA')[0][0]
        sigma10 = tb.getcol('SIGMA')[1][0]
        weight00 = tb.getcol('WEIGHT')[0][0]
        weight10 = tb.getcol('WEIGHT')[1][0]
        tb.close()

        tb.open(self.tsystable)
        sum_fparam0 = 0
        sum_fparam1 = 0
        for i in range(128):
            sum_fparam0 += tb.getvarcol('FPARAM')['r1'][0][i][0]
            sum_fparam1 += tb.getvarcol('FPARAM')['r1'][1][i][0]
        fparam0_ave = sum_fparam0 / 128.0
        fparam1_ave = sum_fparam1 / 128.0
        print('fparam_average_r1_0', fparam0_ave)
        print('fparam_average_r1_1', fparam1_ave)
        print('SIGMA00 ', sigma00)
        print('SIGMA10 ', sigma10)
        print('WEIGHT00 ', weight00)
        print('WEIGHT10 ', weight10)
        answer0 = 1 / (sigma00**2) * 1 / (fparam0_ave**2)
        answer1 = 1 / (sigma10**2) * 1 / (fparam1_ave**2)
        print('pol0: 1/SIGMA**2 X 1/(FPARAM_ave)**2', answer0)
        print('pol1: 1/SIGMA**2 X 1/(FPARAM_ave)**2', answer1)
        diff0_percent = (weight00 - answer0) / weight00 * 100
        diff1_percent = (weight10 - answer1) / weight10 * 100
        print('difference between fparam_r1_0 and weight00', diff0_percent, '%')
        print('difference between fparam_r1_1 and weight10', diff1_percent, '%')
        tb.close()

    def test02(self):
        """Test02: weight = 1/(SIGMA**2) X 1/(FPARAM_ave**2) list version."""
        # focus on antenna1=0, data_disk_id=1
        # spwmap_list=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]

        # tid = "02"
        infile = self.infile1
        sdcal(infile=infile, calmode='tsys', outfile=self.tsystable)
        initweights(vis=infile, wtmode='nyq', dowtsp=True)
        spwmap_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        # spwmap_dict={1:[9],3:[11],5:[13],7:[15]}

        # spwmap_dict={1:[1],3:[3],5:[5],7:[7]}
        sdcal(infile=infile, calmode='apply', spwmap=spwmap_list,
              applytable=self.tsystable, outfile='')

        tb.open(infile)
        sigma00 = tb.getcol('SIGMA')[0][0]
        sigma10 = tb.getcol('SIGMA')[1][0]
        weight00 = tb.getcol('WEIGHT')[0][0]
        weight10 = tb.getcol('WEIGHT')[1][0]
        tb.close()

        tb.open(self.tsystable)
        sum_fparam0 = 0
        sum_fparam1 = 0
        for i in range(128):
            sum_fparam0 += tb.getvarcol('FPARAM')['r1'][0][i][0]
            sum_fparam1 += tb.getvarcol('FPARAM')['r1'][1][i][0]
        fparam0_ave = sum_fparam0 / 128.0
        fparam1_ave = sum_fparam1 / 128.0
        print('fparam_average_r1_0', fparam0_ave)
        print('fparam_average_r1_1', fparam1_ave)
        print('SIGMA00 ', sigma00)
        print('SIGMA10 ', sigma10)
        print('WEIGHT00 ', weight00)
        print('WEIGHT10 ', weight10)
        answer0 = 1 / (sigma00**2) * 1 / (fparam0_ave**2)
        answer1 = 1 / (sigma10**2) * 1 / (fparam1_ave**2)
        print('pol0: 1/SIGMA**2 X 1/(FPARAM_ave)**2', answer0)
        print('pol1: 1/SIGMA**2 X 1/(FPARAM_ave)**2', answer1)
        diff0_percent = (weight00 - answer0) / weight00 * 100
        diff1_percent = (weight10 - answer1) / weight10 * 100
        print('difference between fparam_r1_0 and weight00', diff0_percent, '%')
        print('difference between fparam_r1_1 and weight10', diff1_percent, '%')
        tb.close()

        # print(type(fparam_dict))
        # print('shape of fparam')
        # print('shape of fparam_dict['r29']', fparam_dict['r29'].shape)
        # print(fparam_dict['r29'][0])
        # print(fparam_dict['r29'][1])
        # tb.close()

        # tb.open(infile)

        # data_dict=tb.getvarcol('DATA')

        # subt=tb.query('', sortlist='ANTENNA1, TIME, SPECTRAL_WINDOW_ID', columns='FPARAM, DATA')
        # data=subt2.getcol('DATA')
        # fparam=subt2.getcol('FPARAM')
        # print(data[0])
        # print(data[1])
        # print(fparam[0])
        # print(fparam[1])

        # subt_dict=tb.query('', sortlist='ANTENNA1, TIME', columns='WEIGHT, CORRECTED_DATA')
        # weight_dict = subt_dict.getcol('WEIGHT')
        # weight_dict=tb.getvarcol('WEIGHT')
        # print(type(weight_dict))
        # print(weight_dict['r69'])
        # print(weight_dict['r69'][0])
        # print(weight_dict['r69'][1])
        # print(weight_dict)

        # corrected_data_dict = subt_dict.getcol('CORRECTED_DATA')
        # tb.close()
        # subt_dict.close()

        # sdcal(infile=infile, calmode='apply', spwmap=spwmap_dict,
        #       applytable='tsys.cal', outfile='')
        # tb.open(infile)
        # subt_list=tb.query('', sortlist='ANTENNA1, TIME, SPECTRAL_WINDOW_ID',
        #                    columns='WEIGHT, CORRECTED_DATA')
        # weight_list = subt_list.getcol('WEIGHT')
        # corrected_data_list = subt_list.getcol('CORRECTED_DATA')
        # tb.close()
        # subt_list.close()

        # sdcal(infile=infile, calmode='apply', spwmap=spwmap_list,
        #       applytable='tsys.cal', outfile='')

        # print('dict:', spwmap)
        # print('list:', spwmap)
        # if spwmap.all()==spwmap_dict.all():
        #    Spwmap is able to cope with dictionary and list.
        # print(spwmap.all()==spwmap_dict.all())

    def test03(self):
        """Test03: Validation of CORRECTED_DATA = DATA X FPARAM (spwmap={1:[1], 3:[3], 5:[5], 7:[7]})."""
        # tid = "03"
        infile = self.infile1
        tsysfile = self.tsystable

        # tsys table is produced
        sdcal(infile=infile, calmode='tsys', outfile=tsysfile)
        # spwmap=[0,1,2,3,4,5,6,7,8,1,10,3,12,5,14,7,16]
        spwmap = {1: [1], 3: [3], 5: [5], 7: [7]}
        initweights(vis=infile, wtmode='nyq', dowtsp=True)

        sdcal(infile=infile, calmode='apply', spwmap=spwmap, applytable=tsysfile)

        tb.open(infile)
        corrected_data = tb.getvarcol('CORRECTED_DATA')['r1'][0][0][0]
        data = tb.getvarcol('DATA')['r1'][0][0][0]
        tb.close()

        tb.open(tsysfile)
        fparam = tb.getvarcol('FPARAM')['r1'][0][0][0]
        tb.close()

        print("CORRECTED_DATA", corrected_data)
        print("DATA", data)
        print("FPARAM", fparam)
        diff = corrected_data.real - (data.real * fparam)
        diff_per = (diff / corrected_data.real) * 100
        print("difference between CORRECTED_DATA and DATA X FPARAM", diff_per, "%")

    def test04(self):
        """Test04: Validation of CORRECTED_DATA = DATA X FPARAM.

        (spwmap={1:[9], 3:[11], 5:[13], 7:[15]})
        antanna1=0, DATA_DISC_ID=9, FPARAM_average
        """
        # tid = "04"
        infile = self.infile1
        tsysfile = self.tsystable

        # tsys table is produced
        sdcal(infile=infile, calmode='tsys', outfile=tsysfile)
        # spwmap=[0,1,2,3,4,5,6,7,8,1,10,3,12,5,14,7,16]
        spwmap = {1: [9], 3: [11], 5: [13], 7: [15]}
        initweights(vis=infile, wtmode='nyq', dowtsp=True)
        sdcal(infile=infile, calmode='apply', spwmap=spwmap, applytable=tsysfile)

        tb.open(infile)
        corrected_data = tb.getvarcol('CORRECTED_DATA')['r2'][0][0][0]
        data = tb.getvarcol('DATA')['r2'][0][0][0]
        tb.close()

        tb.open(tsysfile)
        sum_fparam = 0
        for i in range(128):
            fparam = tb.getvarcol('FPARAM')['r1'][0][i][0]
            sum_fparam += fparam
        fparam_ave = sum_fparam / 128.0
        tb.close()

        print("CORRECTED_DATA", corrected_data)
        print("DATA", data)
        print("FPARAM average(128ch)", fparam_ave)
        diff = corrected_data.real - (data.real * fparam_ave)
        diff_per = (diff / corrected_data.real) * 100
        print("difference between CORRECTED_DATA and DATA X FPARAM_average(128)", diff_per, "%")

    def test05(self):
        """Test05: Validation of CORRECTED_DATA = DATA X FPARAM.

        (spwmap={1:[9], 3:[11], 5:[13], 7:[15]})
        antanna1=0, DATA_DISC_ID=9, FPARAM_average
        """
        print('')

        # tid = "05"
        infile = self.infile1
        tsysfile = self.tsystable

        # tsys table is produced
        sdcal(infile=infile, calmode='tsys', outfile=tsysfile)
        # spwmap=[0,1,2,3,4,5,6,7,8,1,10,3,12,5,14,7,16]
        spwmap = {1: [9], 3: [11], 5: [13], 7: [15]}
        initweights(vis=infile, wtmode='nyq', dowtsp=True)
        sdcal(infile=infile, calmode='apply', spwmap=spwmap, applytable=tsysfile)

        tb.open(infile)
        corrected_data = tb.getvarcol('CORRECTED_DATA')['r2'][0][0][0]
        data = tb.getvarcol('DATA')['r2'][0][0][0]
        tb.close()

        tb.open(tsysfile)
        fparam = tb.getvarcol('FPARAM')['r1'][0][0][0]
        tb.close()

        print("CORRECTED_DATA", corrected_data)
        print("DATA", data)
        print("FPARAM", fparam)
        diff = corrected_data.real - (data.real * fparam)
        diff_per = (diff / corrected_data.real) * 100
        print("difference between CORRECTED_DATA and DATA X FPARAM", diff_per, "%")

    def test06(self):
        """Test06: weight_spectrum = 1/(SIGMA**2) X 1/(FPARAMx**2) dictionary version."""
        # focus on antenna1=0, data_disk_id=1
        # spwmap_dict={1:[1],3:[3],5:[5],7:[7]}

        # tid = "06"
        infile = self.infile1
        sdcal(infile=infile, calmode='tsys', outfile=self.tsystable)
        initweights(vis=infile, wtmode='nyq', dowtsp=True)
        # spwmap_list=[0,1,2,3,4,5,6,7,8,1,10,3,12,5,14,7,16]
        # spwmap_dict={1:[9],3:[11],5:[13],7:[15]}

        spwmap_dict = {1: [1], 3: [3], 5: [5], 7: [7]}
        sdcal(infile=infile, calmode='apply', spwmap=spwmap_dict,
              applytable=self.tsystable, interp='nearest', outfile='')

        row = 0
        # eps = 1.0e-1

        tb.open(infile)
        sigma = tb.getcell('SIGMA', row)
        weight_spectrum = tb.getcell('WEIGHT_SPECTRUM', row)
        total_ch = tb.getcell('WEIGHT_SPECTRUM', row).shape[1]
        tb.close()

        tb.open(self.tsystable)
        fparam = tb.getcell('FPARAM', row)
        for ch in range(total_ch):
            # print('SIGMA00 ', sigma[0])
            # print('SIGMA10 ', sigma[1])
            # print('WEIGHT_SPECTRUM00 ', weight_spectrum[0][ch])
            # print('WEIGHT_SPECTRUM10 ', weight_spectrum[1][ch])
            answer0 = 1 / (sigma[0]**2) * 1 / (fparam[0][ch]**2)
            answer1 = 1 / (sigma[1]**2) * 1 / (fparam[1][ch]**2)
            # print('pol0: 1/SIGMA**2 X 1/(FPARAM)**2', answer0)
            # print('pol1: 1/SIGMA**2 X 1/(FPARAM)**2', answer1i)
            diff0 = weight_spectrum[0][ch] - answer0
            diff1 = weight_spectrum[1][ch] - answer1
            diff0_percent = diff0 / weight_spectrum[0][ch] * 100
            diff1_percent = diff1 / weight_spectrum[1][ch] * 100

            # diff0_percent=(weight_spectrum[0][ch]-answer0)/weight_spectrum[0][ch]*100
            # diff1_percent=(weight_spectrum[1][ch]-answer1)/weight_spectrum[1][ch]*100
            print('')
            print('pol0 & pol1 ch ' + str(ch) +
                  ': diff between 1/SIGMA**2 X 1/(FPARAM[' +
                  str(ch) + '])**2 and WEIGHT_SPECTRUM[' + str(ch) + ']', diff0, diff1)
            print(diff0_percent, '%', diff1_percent, '%')
            # self.assertTrue(diff0 < eps, msg='The error is small enough')
        tb.close()


class sdcal_test_base(unittest.TestCase):
    """Base class for sdcal unit test.

    The following attributes/functions are defined here.

        datapath
        decorators (invalid_argument_case, exception_case)
    """

    # Data path of input
    datapath = ctsys_resolve('unittest/sdcal/')

    # Input
    infile = 'uid___A002_X6218fb_X264.ms.sel'
    applytable = infile + '.sky'

    # task execution result
    result = None

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

    def _setUp(self, files, task):
        for f in files:
            if os.path.exists(f):
                shutil.rmtree(f)
            shutil.copytree(os.path.join(self.datapath, f), f)

    def _tearDown(self, files):
        for f in files:
            if os.path.exists(f):
                shutil.rmtree(f)


class sdcal_test_ps(sdcal_test_base):
    """
    Unit test for task sdcal (position switchsky calibration).

    The list of tests:
    test_ps00 --- default parameters (raises an error)
    test_ps01 --- invalid calibration type
    test_ps02 --- invalid selection (empty selection result)
    test_ps03 --- outfile exists (overwrite=False)
    test_ps04 --- empty outfile
    test_ps05 --- position switch calibration ('ps')
    test_ps06 --- position switch calibration ('ps') with data selection
    test_ps07 --- outfile exists (overwrite=True)
    test_ps08 --- inappropriate calmode ('otfraster')
    """

    invalid_argument_case = sdcal_test_base.invalid_argument_case
    exception_case = sdcal_test_base.exception_case

    @property
    def outfile(self):
        return self.applytable

    def setUp(self):
        self._setUp([self.infile], sdcal)

    def tearDown(self):
        self._tearDown([self.infile, self.outfile])

    def normal_case(**kwargs):
        """Decorator for the test case that is intended to verify normal execution result.

        selection --- data selection parameter as dictionary

        Here, expected result is as follows:
            - total number of rows is 12
            - number of antennas is 2
            - number of spectral windows is 2
            - each (antenna,spw) pair has 3 rows
            - expected sky data is a certain fixed value except completely
              flagged channels
              ANT, SPW, SKY
              0     9   [1.0, 2.0, 3.0]
              1     9   [7.0, 8.0, 9.0]
              0    11   [4.0, 5.0, 6.0]
              1    11   [10.0, 11.0, 12.0]
            - channels 0~10 are flagged, each integration has sprious
              ANT, SPW, SKY
              0     9   [(511,512), (127,128), (383,384)]
              1     9   [(511,512), (127,128), (383,384)]
              0    11   [(511,512), (127,128), (383,384)]
              1    11   [(511,512), (127,128), (383,384)]
        """
        def wrapper(func):
            import functools

            @functools.wraps(func)
            def _wrapper(self):
                func(self)

                # sanity check
                self.assertIsNone(self.result, msg='The task must complete without error')
                self.assertTrue(os.path.exists(self.outfile),
                                msg='Output file is not properly created.')

                # verifying nrow
                if len(kwargs) == 0:
                    expected_nrow = 12
                    antenna1_selection = None
                    spw_selection = None
                else:
                    myms = ms()
                    myargs = kwargs.copy()
                    if 'baseline' not in myargs:
                        with table_manager(self.infile) as tb:
                            antenna1 = numpy.unique(tb.getcol('ANTENNA1'))
                            myargs['baseline'] = '%s&&&' % (','.join(map(str, antenna1)))
                    a = myms.msseltoindex(self.infile, **myargs)
                    antenna1_selection = a['antenna1']
                    spw_selection = a['spw']
                    expected_nrow = 3 * len(spw_selection) * len(antenna1_selection)
                with table_manager(self.outfile) as tb:
                    self.assertEqual(
                        tb.nrows(),
                        expected_nrow,
                        msg=f'Number of rows mismatch (expected {expected_nrow} ' +
                            f'actual {tb.nrows()})')

                # verifying resulting sky spectra
                expected_value = {0: {9: [1., 2., 3.],
                                      11: [4., 5., 6.]},
                                  1: {9: [7., 8., 9.],
                                      11: [10., 11., 12.]}}
                eps = 1.0e-6
                for (ant, d) in expected_value.items():
                    if antenna1_selection is not None and ant not in antenna1_selection:
                        continue
                    for (spw, val) in d.items():
                        if spw_selection is not None and spw not in spw_selection:
                            continue
                        # print(ant, spw, val)

                        def construct(x):
                            return '%s == %s' % (x)

                        taql = ' && '.join(
                            map(construct, [('ANTENNA1', ant), ('SPECTRAL_WINDOW_ID', spw)]))
                        with table_selector(self.outfile, taql) as tb:
                            nrow = tb.nrows()
                            self.assertEqual(nrow, 3, msg='Number of rows mismatch')
                            for irow in range(tb.nrows()):
                                expected = val[irow]
                                self.assertGreater(expected, 0.0, msg='Internal Error')
                                fparam = tb.getcell('FPARAM', irow)
                                flag = tb.getcell('FLAG', irow)

                                def message_template(x, y):
                                    return f'Unexpected {x} for antenna {ant} spw {spw} ' + \
                                           f'row {irow} (expected {y})'

                                self.assertTrue(all(flag[:, :10].flatten() == True),
                                                msg=message_template('flag status', True))
                                self.assertTrue(all(flag[:, 10:].flatten() == False),
                                                msg=message_template('flag status', False))
                                fparam_valid = fparam[flag == False]
                                error = abs((fparam_valid - expected) / expected)
                                self.assertTrue(all(error < eps),
                                                msg=message_template('sky data', expected))
            return _wrapper
        return wrapper

    @invalid_argument_case
    def test_ps00(self):
        """test_ps00 --- default parameters (raises an error)."""
        # CASA6 throws an exception
        self.assertRaises(Exception, sdcal)

    @invalid_argument_case
    def test_ps01(self):
        """test_ps01 --- invalid calibration type."""
        # CASA6 throws an exception
        self.assertRaises(Exception, sdcal, infile=self.infile,
                          calmode='invalid_type', outfile=self.outfile)

    @exception_case(RuntimeError, 'Spw Expression: No match found for 99,')
    def test_ps02(self):
        """test_ps02 --- invalid selection (invalid spw selection)."""
        self.result = sdcal(infile=self.infile, calmode='ps', spw='99', outfile=self.outfile)

    @exception_case(RuntimeError, '^overwrite is False and output file exists:')
    def test_ps03(self):
        """test_ps03 --- outfile exists (overwrite=False)."""
        # copy input to output
        shutil.copytree(self.infile, self.outfile)
        self.result = sdcal(infile=self.infile, calmode='ps', outfile=self.outfile, overwrite=False)

    @exception_case(RuntimeError, r'Output file name must be specified\.')
    def test_ps04(self):
        """test_ps04 --- empty outfile."""
        self.result = sdcal(infile=self.infile, calmode='ps', outfile='', overwrite=False)

    @normal_case()
    def test_ps05(self):
        """test_ps05 --- position switch calibration ('ps')."""
        self.result = sdcal(infile=self.infile, calmode='ps', outfile=self.outfile)

    @normal_case()
    def test_ps05M(self):
        """test_ps05M --- position switch calibration ('ps') for MMS."""
        with mmshelper(vis=self.infile) as mvis:
            self.assertTrue(mvis is not None)
            self.result = sdcal(infile=mvis, calmode='ps', outfile=self.outfile)

    @normal_case(spw='9')
    def test_ps06(self):
        """test_ps06 --- position switch calibration ('ps') with data selection."""
        self.result = sdcal(infile=self.infile, calmode='ps', spw='9', outfile=self.outfile)

    @normal_case()
    def test_ps07(self):
        """test_ps07 --- outfile exists (overwrite=True)."""
        # copy input to output
        shutil.copytree(self.infile, self.outfile)
        self.result = sdcal(infile=self.infile, calmode='ps', outfile=self.outfile, overwrite=True)

    @exception_case(RuntimeError, "Error in Calibrater::solve")
    def test_ps08(self):
        """test_ps08 --- inappropriate calmode ('otfraster')."""
        # the data doesn't an OTF raster scan so that unexpected behavior may happen
        # if calmode is 'otfraster'
        # in this case, gap detection detects the row having only one integration
        # due to irregular time stamp distribution and causes the "Too many edge
        # points" error
        self.result = sdcal(infile=self.infile, outfile=self.outfile, calmode='otfraster')


class DataManager:
    """Decorator to implement setUp/tearDown methods.

    Functor decorator to handle data setup/teardown of unit test class methods
    on a per-test basis
    """

    def __init__(self, io_files=None):
        self.io_files = io_files

    def setUp(self):
        input_ms_name = self.io_files['input']['ms_name']
        input_ms_path = os.path.join(self.io_files['datapath'], input_ms_name)

        if not os.path.exists(input_ms_path):
            err_msg = 'Input MS not found:\n{}'.format(input_ms_path)
            raise Exception(err_msg)

        input_ms_copy_name = input_ms_name
        if os.path.exists(input_ms_copy_name):
            shutil.rmtree(input_ms_copy_name)

        shutil.copytree(input_ms_path, input_ms_copy_name)

        output_cal_tbl_name = self.io_files['output']['cal_tbl_name']
        if os.path.exists(output_cal_tbl_name):
            shutil.rmtree(output_cal_tbl_name)

    def tearDown(self):
        input_ms_copy_name = self.io_files['input']['ms_name']
        if os.path.exists(input_ms_copy_name):
            shutil.rmtree(input_ms_copy_name)

        output_cal_tbl_name = self.io_files['output']['cal_tbl_name']
        if os.path.exists(output_cal_tbl_name):
            shutil.rmtree(output_cal_tbl_name)

    def __call__(self, func):
        def wrapped_func(inst_UnitTest):
            inst_UnitTest.io_files = self.io_files
            self.setUp()
            func(inst_UnitTest)
            self.tearDown()

        return wrapped_func


class sdcal_test_bug_fix_cas_12712(unittest.TestCase):
    """Test fix for sdcal bug reported in CAS-12712."""

    io_files = None

    @DataManager({'input': {'ms_name': 'uid___A002_X85c183_X36f.ms.sel'},
                  'output': {'cal_tbl_name': 'uid___A002_X85c183_X36f.ms.sel.cal.sky.tbl'},
                  # Path to search when looking for test input data,
                  # relative to some root data directory
                  'datapath': ctsys_resolve('unittest/sdcal/')
                  })
    def test_cas_12712_01(self):
        input_ms = self.io_files['input']['ms_name']
        output_cal_tbl = self.io_files['output']['cal_tbl_name']

        spw_sel = "23"
        sdcal(infile=input_ms,
              outfile=output_cal_tbl,
              spw=spw_sel,
              overwrite=True,
              calmode='ps')

        cal_tbl_path = os.path.join(os.getcwd(), output_cal_tbl)
        err_msg = 'Calibration table not found:\n{}'.format(cal_tbl_path)
        self.assertTrue(os.path.exists(cal_tbl_path), err_msg)

        # Expected number of calibration table rows for spw=23 and antenna=0:
        # 61 valid (unflagged) rows, 3 totally flagged rows, 64 rows in total
        expected_rows = 64
        expected_flagged_rows = 3
        computed_rows = None
        computed_flagged_rows = None
        ant_id = 0
        res_tbl = None
        try:
            tb.open(cal_tbl_path)
            qry = f'select all(FLAG) as FLAG_ROW from {output_cal_tbl} ' + \
                  f'where ( ANTENNA1 == {ant_id} and ANTENNA2 == {ant_id} )'
            res_tbl = tb.taql(qry)
            computed_rows = res_tbl.nrows()
            computed_flags = res_tbl.getcol('FLAG_ROW')
            computed_flagged_rows = computed_flags.sum()
        finally:
            if tb:
                tb.close()
            if res_tbl:
                res_tbl.close()

            err_msg = "\n".join([
                f"Calibration table: {cal_tbl_path}",
                f"Expected number of rows for ant={ant_id} and spw={spw_sel}: "
                f"{expected_rows}",
                f"Computed number of rows for ant={ant_id} and spw={spw_sel}: "
                f"{computed_rows}",
                f"Expected number of flagged rows: {expected_flagged_rows}",
                f"Computed number of flagged rows: {computed_flagged_rows}"
            ])
            self.assertEqual(
                expected_rows,
                computed_rows, err_msg
            )
            self.assertEqual(
                expected_flagged_rows,
                computed_flagged_rows,
                err_msg
            )


class sdcal_test_otfraster(sdcal_test_base):
    """Unit test for task sdcal (OTF raster sky calibration).

    Since basic test case is covered by sdcal_test_ps, only
    tests specific to otfraster calibration are defined here.

    The list of tests:
    test_otfraster00 --- invalid fraction (non numeric value)
    test_otfraster01 --- too many edge points (fraction 0.5)
    test_otfraster02 --- too many edge points (fraction '50%')
    test_otfraster03 --- too many edge points (noff 100000)
    ###test_otfraster04 --- negative edge points
    ###test_otfraster05 --- zero edge points
    test_otfraster06 --- inappropriate calibration mode ('ps')
    test_otfraster07 --- OTF raster calibration ('otfraster') with default setting
    test_otfraster08 --- OTF raster calibration ('otfraster') with string fraction (numeric value)
    test_otfraster09 --- OTF raster calibration ('otfraster') with string fraction (percentage)
    test_otfraster10 --- OTF raster calibration ('otfraster') with numeric fraction
    test_otfraster11 --- OTF raster calibration ('otfraster') with auto detection
    test_otfraster12 --- OTF raster calibration ('otfraster') with custom noff
    test_otfraster13 --- check if noff takes priority over fraction
    """

    invalid_argument_case = sdcal_test_base.invalid_argument_case
    exception_case = sdcal_test_base.exception_case
    infile = 'uid___A002_X6218fb_X264.ms.sel.otfraster'

    @staticmethod
    def calculate_expected_value(table, numedge=1):
        expected_value = {}
        with table_manager(table) as tb:
            antenna_list = numpy.unique(tb.getcol('ANTENNA1'))
            ddid_list = numpy.unique(tb.getcol('DATA_DESC_ID'))
        with table_manager(os.path.join(table, 'DATA_DESCRIPTION')) as tb:
            dd_spw_map = tb.getcol('SPECTRAL_WINDOW_ID')
        for antenna in antenna_list:
            expected_value[antenna] = {}
            for ddid in ddid_list:
                spw = dd_spw_map[ddid]
                taql = f'ANTENNA1 == {antenna} && ANTENNA2 == {antenna} ' +  \
                       f'&& DATA_DESC_ID == {ddid}'
                with table_manager(table) as tb:
                    try:
                        tsel = tb.query(taql, sortlist='TIME')
                        time_list = tsel.getcol('TIME')
                        data = tsel.getcol('DATA').real
                        flag = tsel.getcol('FLAG')
                    finally:
                        tsel.close()
                # print('time_list', time_list)
                if len(time_list) < 2:
                    continue
                data_list = []
                time_difference = time_list[1:] - time_list[:-1]
                # print('time_difference', time_difference)
                gap_threshold = numpy.median(time_difference) * 5
                # print('gap_threshold', gap_threshold)
                gap_list = numpy.concatenate(
                    ([0], numpy.where(time_difference > gap_threshold)[0] + 1))
                if gap_list[-1] != len(time_list):
                    gap_list = numpy.concatenate((gap_list, [len(time_list)]))
                # print('gap_list', gap_list)
                for i in range(len(gap_list) - 1):
                    start = gap_list[i]
                    end = gap_list[i + 1]
                    raster_data = data[:, :, start:end]
                    raster_flag = flag[:, :, start:end]
                    raster_row = numpy.ma.masked_array(raster_data, raster_flag)
                    left_edge = raster_row[:, :, :numedge].mean(axis=2)
                    right_edge = raster_row[:, :, -numedge:].mean(axis=2)
                    data_list.extend([left_edge, right_edge])
                expected_value[antenna][spw] = data_list
                # print('antenna', antenna, 'spw', spw, 'len(data_list)', len(data_list))

        return expected_value

    @property
    def outfile(self):
        return self.applytable

    def setUp(self):
        self._setUp([self.infile], sdcal)

    def tearDown(self):
        self._tearDown([self.infile, self.outfile])

    def normal_case(numedge=1, **kwargs):
        """Decorator for the test case that is intended to verify normal execution result.

        numedge --- expected number of edge points
        selection --- data selection parameter as dictionary

        Here, expected result is as follows:
            - total number of rows is 24
            - number of antennas is 2
            - number of spectral windows is 2
            - each (antenna,spw) pair has 6 rows
        """
        def wrapper(func):
            import functools

            @functools.wraps(func)
            def _wrapper(self):
                func(self)

                # sanity check
                self.assertIsNone(self.result, msg='The task must complete without error')
                self.assertTrue(os.path.exists(self.outfile),
                                msg='Output file is not properly created.')

                # verifying nrow
                if len(kwargs) == 0:
                    expected_nrow = 24
                    antenna1_selection = None
                    spw_selection = None
                else:
                    myms = ms()
                    myargs = kwargs.copy()
                    if 'baseline' not in myargs:
                        with table_manager(self.infile) as tb:
                            antenna1 = numpy.unique(tb.getcol('ANTENNA1'))
                            myargs['baseline'] = '%s&&&' % (','.join(map(str, antenna1)))
                    a = myms.msseltoindex(self.infile, **myargs)
                    antenna1_selection = a['antenna1']
                    spw_selection = a['spw']
                    expected_nrow = 6 * len(spw_selection) * len(antenna1_selection)
                with table_manager(self.outfile) as tb:
                    self.assertEqual(
                        tb.nrows(), expected_nrow,
                        msg='Number of rows mismatch '
                            f'(expected {expected_nrow} actual {tb.nrows()})')

                # verifying resulting sky spectra
                eps = 1.0e-6
                expected_value = sdcal_test_otfraster.calculate_expected_value(self.infile, numedge)
                for (ant, d) in expected_value.items():
                    if antenna1_selection is not None and ant not in antenna1_selection:
                        continue
                    for (spw, val) in d.items():
                        if spw_selection is not None and spw not in spw_selection:
                            continue
                        # print(ant, spw, val)

                        def construct(x):
                            return '%s == %s' % (x)

                        taql = ' && '.join(
                            map(construct, [('ANTENNA1', ant), ('SPECTRAL_WINDOW_ID', spw)]))
                        with table_selector(self.outfile, taql) as tb:
                            nrow = tb.nrows()
                            self.assertEqual(nrow, 6, msg='Number of rows mismatch')
                            for irow in range(tb.nrows()):
                                expected = val[irow]
                                fparam = tb.getcell('FPARAM', irow)
                                flag = tb.getcell('FLAG', irow)
                                self.assertEqual(
                                    expected.shape,
                                    fparam.shape,
                                    msg=f'Shape mismatch for antenna {ant} spw {spw} ' +
                                        f'row {irow} (expected {expected.shape} ' +
                                        f'actual {fparam.shape})')
                                npol, nchan = expected.shape
                                for ipol in range(npol):
                                    for ichan in range(nchan):

                                        def message_template(x, y, z):
                                            return f'Unexpected {x} for antenna {ant} ' + \
                                                   f'spw {spw} row {irow} pol {ipol} ' + \
                                                   f'channel {ichan} (expected {y} actual {z})'

                                        _flag = flag[ipol, ichan]
                                        _mask = expected.mask[ipol, ichan]
                                        _expected = expected.data[ipol, ichan]
                                        _fparam = fparam[ipol, ichan]
                                        self.assertEqual(
                                            _mask, _flag,
                                            msg=message_template('FLAG', _mask, _flag))
                                        if _mask:
                                            self.assertEqual(0.0, _fparam, msg=message_template(
                                                'FPARAM', 0.0, _fparam))
                                        elif abs(_expected) < eps:
                                            self.assertLess(abs(_fparam), eps, msg=message_template(
                                                'FPARAM', _expected, _fparam))
                                        else:
                                            diff = abs((_fparam - _expected) / _expected)
                                            self.assertLess(diff, eps, msg=message_template(
                                                'FPARAM', _expected, _fparam))
                                # self.assertTrue(
                                #     all(flag[:,:10].flatten() == True),
                                #     msg=message_template('flag status', True))
                                # self.assertTrue(
                                #     all(flag[:,10:].flatten() == False),
                                # msg=message_template('flag status', False))
                                # fparam_valid = fparam[flag == False]
                                # error = abs((fparam_valid - expected) / expected)
                                # self.assertTrue(
                                #     all(error < eps),
                                #     msg=message_template('sky data', expected))
            return _wrapper
        return wrapper

    @exception_case(RuntimeError, r'^Invalid fraction value \(.+\)$')
    def test_otfraster00(self):
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction='auto')

    @exception_case(ValueError, r'^Too many edge points\. fraction must be < 0.5\.$')
    def test_otfraster01(self):
        """test_otfraster01 --- too many edge points (fraction 0.5)."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction=0.5)

    @exception_case(ValueError, r'^Too many edge points\. fraction must be < 0.5\.$')
    def test_otfraster02(self):
        """test_otfraster02 --- too many edge points (fraction 50%)."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction='50%')

    @exception_case(RuntimeError, 'Error in Calibrater::solve')
    def test_otfraster03(self):
        """test_otfraster03 --- too many edge points (noff 100000)."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', noff=10000)

    # @exception_case(RuntimeError, 'Error in Calibrater::solve')
    # def test_otfraster04(self):
    #    """test_otfraster04 --- negative edge points."""
    #    self.result = sdcal(infile=self.infile, outfile=self.outfile,
    #                         calmode='otfraster', noff=-3)

    # @exception_case(RuntimeError, 'Error in Calibrater::solve')
    # def test_otfraster05(self):
    #    """test_otfraster05 --- zero edge points."""
    #    self.result = sdcal(infile=self.infile, outfile=self.outfile,
    #                         calmode='otfraster', noff=0)

    @exception_case(RuntimeError, 'Error in Calibrater::solve')
    def test_otfraster06(self):
        """test_otfraster06 --- inappropriate calibration mode ('ps')."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='ps')

    @normal_case(numedge=1)
    def test_otfraster07(self):
        """test_otfraster07 --- OTF raster calibration ('otfraster') with default setting."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster')

    @normal_case(numedge=1)
    def test_otfraster07M(self):
        """test_otfraster07M --- OTF raster calibration ('otfraster') with default setting (MMS)."""
        with mmshelper(vis=self.infile) as mvis:
            self.assertTrue(mvis is not None)
            self.result = sdcal(infile=mvis, outfile=self.outfile,
                                calmode='otfraster')

    @normal_case(numedge=2)
    def test_otfraster08(self):
        """test_otfraster08 --- OTF raster calibration ('otfraster') with string fraction (numeric value)."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction='0.3')

    @normal_case(numedge=2)
    def test_otfraster09(self):
        """test_otfraster09 --- OTF raster calibration ('otfraster') with string fraction (percentage)."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction='30%')

    @normal_case(numedge=2)
    def test_otfraster10(self):
        """test_otfraster10 --- OTF raster calibration ('otfraster') with numeric fraction."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction=0.3)

    @normal_case(numedge=2)
    def test_otfraster11(self):
        """test_otfraster11 --- OTF raster calibration ('otfraster') with auto detection."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction=0, noff=0)

    @normal_case(numedge=3)
    def test_otfraster12(self):
        """test_otfraster12 --- OTF raster calibration ('otfraster') with custom noff."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', noff=3)

    @normal_case(numedge=3)
    def test_otfraster13(self):
        """test_otfraster13 --- check if noff takes priority over fraction."""
        self.result = sdcal(infile=self.infile, outfile=self.outfile,
                            calmode='otfraster', fraction='90%', noff=3)


def assert_true(condition, err_msg):
    """Assertion not optimized away -contrary to Python assert- when Python interpreter is run in optimized mode (__debug__=False)."""
    if not condition:
        raise RuntimeError(err_msg)


class CasaTableChecker:
    """Base class for OTF mode checkers."""

    def __init__(self, tbl_path):
        self.tb = table()
        self.path = tbl_path
        self.tb.open(self.path)  # Raises RuntimeError on failure
        assert self.tb.ok()

    def __del__(self):
        self.tb.close()


class MsCalTableChecker(CasaTableChecker):
    """OTF mode checker.

    checking equality of 2 ms_caltable fparam columns within specified tolerance.
    """

    def __init__(self, tbl_path, tol=1e-6):
        CasaTableChecker.__init__(self, tbl_path)
        assert_true('FPARAM' in self.tb.colnames(),
                    str(self.path) + ': FPARAM column missing')
        self.fparam = self.tb.getcol('FPARAM')
        self.tol = tol

    def __eq__(self, other):
        assert_true(
            self.fparam.shape == other.fparam.shape,
            'FPARAM columns: shape mismatch: ' +
            f'expected {self.fparam.shape} result {other.fparam.shape}')
        assert_true(numpy.allclose(self.fparam, other.fparam, atol=self.tol, rtol=0.0),
                    'FPARAM columns: error exceeds tolerance=' + str(self.tol))
        return True

    def __del__(self):
        CasaTableChecker.__del__(self)


class MsCorrectedDataChecker(CasaTableChecker):
    """OTF mode checker.

    checking equality of 2 ms corrected_data columns within specified tolerance.
    """

    def __init__(self, tbl_path, convert_to_kelvin=False, tol_real=1e-6):
        CasaTableChecker.__init__(self, tbl_path)
        self.tol_real = tol_real
        assert_true('CORRECTED_DATA' in self.tb.colnames(),
                    str(self.path) + ': CORRECTED_DATA column missing')
        self.cdata = self.tb.getcol('CORRECTED_DATA')
        if convert_to_kelvin:
            tbl_syscal = table()
            tbl_syscal.open(os.path.join(tbl_path, 'SYSCAL'))
            tsys_spectrum = tbl_syscal.getcol('TSYS_SPECTRUM')
            self.cdata = tsys_spectrum * self.cdata
            tbl_syscal.close()

    def __eq__(self, other):
        assert_true(self.cdata.shape == other.cdata.shape,
                    'CORRECTED_DATA: shape: mismatch')
        assert_true((self.cdata.imag == other.cdata.imag).all(),
                    'CORRECTED_DATA: imaginary part: mismatch')
        assert_true(numpy.allclose(self.cdata.real, other.cdata.real, atol=self.tol_real, rtol=0.0),
                    'CORRECTED_DATA: real part: error exceeds tolerance=' + str(self.tol_real))
        return True

    def __del__(self):
        CasaTableChecker.__del__(self)


class sdcal_test_otf(unittest.TestCase):
    """Unit tests for task sdcal.

    sky calibration mode = 'otf' : On-The-Fly (OTF) *non-raster*

    The list of tests:
    Test       | Input            | Edges    | Calibration
    Name       | MS               | Fraction | Mode
    ==========================================================================
    test_otf01 | squares.dec60_cs | 10%      | otf
    test_otf02 | squares.dec60_cs | 20%      | otf
    test_otf03 | lissajous        | 10%      | otf
    test_otf04 | lissajous        | 20%      | otf
    test_otf05 | squares.dec60_cs | 10%      | otf,apply
    test_otf06 | lissajous        | 10%      | apply
    test_otf07 | lissajous        | 10%      | otf,tsys,apply
    """

    # Required checkers:
    # - compare 2 calibration tables
    # - compare 2 corrected data
    datapath = ctsys_resolve('unittest/sdcal/')
    ref_datapath = os.path.join(datapath, 'otf_reference_data')
    sdcal_params = {}
    current_test_params = {}

    def setup(self):
        # Copy input MS into current directory
        infile = self.sdcal_params['infile']
        if os.path.exists(infile):
            shutil.rmtree(infile)
        shutil.copytree(os.path.join(self.datapath, infile), infile)
        # Delete output calibration table if any
        if 'outfile' in self.sdcal_params:
            outfile = self.sdcal_params['outfile']
            if os.path.exists(outfile):
                shutil.rmtree(outfile)
        # Compute reference calibrated ms if required
        if 'compute_ref_ms' in self.current_test_params:
            # Create a second copy of input MS
            ref_ms_name = 'ref_' + infile
            if os.path.exists(ref_ms_name):
                shutil.rmtree(ref_ms_name)
            shutil.copytree(src=infile, dst=ref_ms_name)
            # Calibrate it using current test caltable
            sdcal(infile=ref_ms_name, calmode='apply', applytable=self.ref_caltable())
            # Update test params
            self.current_test_params['ref_calibrated_ms'] = ref_ms_name

    def tearDown(self):
        casalog.post("tearDown")
        infile = self.sdcal_params['infile']
        if os.path.exists(infile):
            shutil.rmtree(infile)
        if 'outfile' in self.sdcal_params:
            outfile = self.sdcal_params['outfile']
            if os.path.exists(outfile):
                shutil.rmtree(outfile)
        if 'compute_ref_ms' in self.current_test_params:
            ref_ms_name = self.ref_calibrated_ms()
            if os.path.exists(ref_ms_name):
                shutil.rmtree(ref_ms_name)

    def run_sdcal(self):
        self.setup()
        sdcal(**self.sdcal_params)

    def ref_caltable(self):
        assert_true('ref_caltable' in self.current_test_params, 'sdcal_test_otf internal error')
        return os.path.join(self.ref_datapath, self.current_test_params['ref_caltable'])

    def ref_calibrated_ms(self):
        assert_true('ref_calibrated_ms' in self.current_test_params,
                    'sdcal_test_otf internal error')
        ref_ms_name = self.current_test_params['ref_calibrated_ms']
        if 'compute_ref_ms' in self.current_test_params:
            return ref_ms_name
        else:
            return os.path.join(self.ref_datapath, ref_ms_name)

    def test_otf01(self):
        """test_otf01 --- Compute calibration table. calmode='otf' ms=squares.dec60_cs.ms."""
        self.sdcal_params = {
            'infile': 'squares.dec60_cs.ms',
            'calmode': 'otf',
            'outfile': 'test_otf01.ms_caltable'
        }
        self.current_test_params = {
            'ref_caltable': 'squares.dec60_cs.edges_fraction_0.1.ms_caltable'
        }
        expected_result = MsCalTableChecker(self.ref_caltable())
        self.run_sdcal()
        sdcal_result = MsCalTableChecker(self.sdcal_params['outfile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf02(self):
        """test_otf02 --- Compute calibration table. calmode='otf' ms=squares.dec60_cs.ms edges_fraction=20%."""
        self.sdcal_params = {
            'infile': 'squares.dec60_cs.ms',
            'calmode': 'otf',
            'outfile': 'test_otf02.ms_caltable',
            'fraction': 0.2
        }
        self.current_test_params = {
            'ref_caltable': 'squares.dec60_cs.edges_fraction_0.2.ms_caltable'
        }
        expected_result = MsCalTableChecker(self.ref_caltable())
        self.run_sdcal()
        sdcal_result = MsCalTableChecker(self.sdcal_params['outfile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf03(self):
        """test_otf03 --- Compute calibration table. calmode='otf' ms=lissajous.ms."""
        self.sdcal_params = {
            'infile': 'lissajous.ms',
            'calmode': 'otf',
            'outfile': 'test_otf03.ms_caltable'
        }
        self.current_test_params = {
            'ref_caltable': 'lissajous.edges_new_fraction_0.1.ms_caltable'
        }
        expected_result = MsCalTableChecker(self.ref_caltable())
        self.run_sdcal()
        sdcal_result = MsCalTableChecker(self.sdcal_params['outfile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf03M(self):
        """test_otf03 --- Compute calibration table. calmode='otf' ms=lissajous.ms (MMS)."""
        self.sdcal_params = {
            'infile': 'lissajous.ms',
            'calmode': 'otf',
            'outfile': 'test_otf03.ms_caltable'
        }
        self.current_test_params = {
            'ref_caltable': 'lissajous.edges_new_fraction_0.1.ms_caltable'
        }
        expected_result = MsCalTableChecker(self.ref_caltable())
        self.setup()
        infile = self.sdcal_params['infile']
        with mmshelper(vis=infile) as mvis:
            self.assertTrue(mvis is not None)
            self.sdcal_params['infile'] = mvis
            sdcal(**self.sdcal_params)
            self.sdcal_params['infile'] = infile
        sdcal_result = MsCalTableChecker(self.sdcal_params['outfile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf04(self):
        """test_otf04 --- Compute calibration table. calmode='otf' ms=lissajous.ms edges_fraction=20%."""
        self.sdcal_params = {
            'infile': 'lissajous.ms',
            'calmode': 'otf',
            'outfile': 'test_otf04.ms_caltable',
            'fraction': '20%'
        }
        self.current_test_params = {
            'ref_caltable': 'lissajous.edges_new_fraction_0.2.ms_caltable'
        }
        expected_result = MsCalTableChecker(self.ref_caltable())
        self.run_sdcal()
        sdcal_result = MsCalTableChecker(self.sdcal_params['outfile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf05(self):
        """test_otf05 --- Sky calibration. calmode='otf,apply' ms=squares.dec60_cs.ms."""
        self.sdcal_params = {
            'infile': 'squares.dec60_cs.ms',
            'calmode': 'otf,apply'
        }
        self.current_test_params = {
            'ref_caltable': 'squares.dec60_cs.edges_fraction_0.1.ms_caltable',
            'compute_ref_ms': True
        }
        self.run_sdcal()
        expected_result = MsCorrectedDataChecker(self.ref_calibrated_ms())
        sdcal_result = MsCorrectedDataChecker(self.sdcal_params['infile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf06(self):
        """test_otf06 --- Sky calibration reusing caltable pre-computed with calmode='otf'. calmode='apply' ms=lissajous.ms."""
        self.sdcal_params = {
            'infile': 'lissajous.ms',
            'calmode': 'apply',
            'applytable': os.path.join(
                self.ref_datapath,
                'lissajous.edges_new_fraction_0.1.ms_caltable')
        }
        self.current_test_params = {
            # new reference data after George's CTTimeInterp1 fix
            # 'ref_calibrated_ms':'lissajous.edges_new_fraction_0.1.sky.ms'
            'ref_calibrated_ms': 'lissajous.edges_after4.7_fraction_0.1.sky.ms'
        }
        expected_result = MsCorrectedDataChecker(self.ref_calibrated_ms())
        self.run_sdcal()
        sdcal_result = MsCorrectedDataChecker(self.sdcal_params['infile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics

    def test_otf07(self):
        """test_otf07 --- Sky calibration + Tsys conversion, composite calmode='otf,tsys,apply'. ms=lissajous.ms."""
        self.sdcal_params = {
            'infile': 'lissajous.ms',
            'calmode': 'otf,tsys,apply',
        }
        self.current_test_params = {
            # new reference data after George's CTTimeInterp1 fix
            # 'ref_calibrated_ms':'lissajous.edges_new_fraction_0.1.sky.ms'
            'ref_calibrated_ms': 'lissajous.edges_after4.7_fraction_0.1.sky.ms'
        }
        expected_result = MsCorrectedDataChecker(self.ref_calibrated_ms(), convert_to_kelvin=True)
        self.run_sdcal()
        sdcal_result = MsCorrectedDataChecker(self.sdcal_params['infile'])
        self.assertEqual(sdcal_result, expected_result)  # AlmostEqual semantics


class sdcal_test_otf_ephem(unittest.TestCase):
    """Unit tests for task sdcal.

    sky calibration mode = 'otf' : On-The-Fly (OTF) *non-raster*

    Test cases for ephemeris objects are defined in this class.

    The list of tests:
    Test            | Input            | Edges    | Calibration
    Name            | MS               | Fraction | Mode
    ==========================================================================
    test_otfephem01 | otf_ephem.ms     | 10%      | otf
    test_otfephem02 | otf_ephem.ms     | 10%      | otf,apply
    """

    datapath = ctsys_resolve('unittest/sdcal/')
    infile = 'otf_ephem.ms'
    outfile = infile + '.otfcal'

    def setUp(self):
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

    def tearDown(self):
        to_be_removed = [self.infile, self.outfile]
        for f in to_be_removed:
            if os.path.exists(f):
                shutil.rmtree(f)

    def check_ephem(self, vis):
        with table_manager(os.path.join(vis, 'SOURCE')) as tb:
            names = tb.getcol('NAME')
        self.assertEqual(len(names), 1)
        me = measures()
        direction_refcodes = me.listcodes(me.direction())
        ephemeris_codes = direction_refcodes['extra']
        self.assertIn(names[0].upper(), ephemeris_codes)

    def check_fresh_ms(self, vis):
        with table_manager(vis) as tb:
            colnames = tb.colnames()

        self.assertNotIn('CORRECTED_DATA', colnames)

    def check_caltable(self, caltable):
        with table_manager(caltable) as tb:
            data = tb.getcol('FPARAM')

        self.assertEqual(data.shape, (2, 1, 10))
        self.assertTrue(numpy.all(data == 1.0))

    def check_corrected(self, vis):
        with table_manager(vis) as tb:
            data = tb.getcol('CORRECTED_DATA')
            nrow = tb.nrows()

        self.assertEqual(nrow, 40)

        real = data.real
        expected = numpy.ones(real.shape, dtype=numpy.float64)
        for irow in range(nrow):
            if irow % 4 == 3:
                expected[:, :, irow] = 0.0
        self.assertTrue(numpy.all(real == expected))

        imag = data.imag
        self.assertTrue(numpy.all(imag == 0))

    def test_otfephem01(self):
        """test_otfephem01: Sky calibration of 'otf' mode for ephemeris object."""
        self.check_ephem(self.infile)
        self.assertFalse(os.path.exists(self.outfile))
        sdcal(infile=self.infile, outfile=self.outfile, calmode='otf')
        self.check_caltable(self.outfile)

    def test_otfephem02(self):
        """test_otfephem02: On-the-fly application of 'otf' calibration mode for ephemeris object."""
        self.check_ephem(self.infile)
        self.check_fresh_ms(self.infile)
        sdcal(infile=self.infile, calmode='otf,apply')
        self.check_corrected(self.infile)


# interpolator utility for testing
class Interpolator(object):
    @staticmethod
    def __interp_freq_linear(data, flag):
        outdata = data.copy()
        outflag = flag
        npol, nchan = outdata.shape
        for ipol in range(npol):
            valid_chans = numpy.where(outflag[ipol, :] == False)[0]
            if len(valid_chans) == 0:
                continue
            for ichan in range(nchan):
                if outflag[ipol, ichan] == True:
                    # print('###', ipol, ichan, 'before', data[ipol,ichan])
                    if ichan <= valid_chans[0]:
                        outdata[ipol, ichan] = data[ipol, valid_chans[0]]
                    elif ichan >= valid_chans[-1]:
                        outdata[ipol, ichan] = data[ipol, valid_chans[-1]]
                    else:
                        ii = abs(valid_chans - ichan).argmin()
                        if valid_chans[ii] - ichan > 0:
                            ii -= 1
                        i0 = valid_chans[ii]
                        i1 = valid_chans[ii + 1]
                        outdata[ipol, ichan] = ((i1 - ichan) * data[ipol, i0] +
                                                (ichan - i0) * data[ipol, i1]) / (i1 - i0)
                    # print('###', ipol, ichan, 'after', data[ipol,ichan])
        return outdata, outflag

    @staticmethod
    def interp_freq_linear(data, flag):
        outflag = flag.copy()
        outflag[:] = False
        outdata, outflag = Interpolator.__interp_freq_linear(data, outflag)
        return outdata, outflag

    @staticmethod
    def interp_freq_nearest(data, flag):
        outdata = data.copy()
        outflag = flag
        npol, nchan = outdata.shape
        for ipol in range(npol):
            valid_chans = numpy.where(outflag[ipol, :] == False)[0]
            if len(valid_chans) == 0:
                continue
            for ichan in range(nchan):
                if outflag[ipol, ichan] == True:
                    # print('###', ipol, ichan, 'before', data[ipol,ichan])
                    if ichan <= valid_chans[0]:
                        outdata[ipol, ichan] = data[ipol, valid_chans[0]]
                    elif ichan >= valid_chans[-1]:
                        outdata[ipol, ichan] = data[ipol, valid_chans[-1]]
                    else:
                        ii = abs(valid_chans - ichan).argmin()
                        outdata[ipol, ichan] = data[ipol, valid_chans[ii]]
                    # print('###', ipol, ichan, 'after', data[ipol,ichan])
        return outdata, outflag

    @staticmethod
    def interp_freq_linearflag(data, flag):
        # NOTE
        # interpolation/extrapolation of flag along frequency axis is
        # also needed for linear interpolation. Due to this, number of
        # flag channels will slightly increase and causes different
        # behavior from existing scantable based single dish task
        # (sdcal2).
        #
        # It appears that effective flag at a certain channel is set to
        # the flag at previous channels (except for channel 0).
        #
        # 2015/02/26 TN
        npol, nchan = flag.shape
        # print('###BEFORE', flag[:,:12])
        outflag = flag.copy()
        for ichan in range(nchan - 1):
            outflag[:, ichan] = numpy.logical_or(flag[:, ichan], flag[:, ichan + 1])
        outflag[:, 1:] = outflag[:, :-1]
        outflag[:, -1] = flag[:, -2]
        # print('###AFTER', outflag[:,:12])

        outdata, outflag = Interpolator.__interp_freq_linear(data, outflag)
        return outdata, outflag

    @staticmethod
    def interp_freq_nearestflag(data, flag):
        outdata, outflag = Interpolator.interp_freq_nearest(data, flag)
        return outdata, outflag

    def __init__(self, table, finterp='linear'):
        self.table = table
        self.taql = ''
        self.time = None
        self.data = None
        self.flag = None
        self.exposure = None
        self.finterp = getattr(Interpolator, 'interp_freq_%s' % (finterp.lower()))
        print('self.finterp:', self.finterp.__name__)

    def select(self, antenna, spw):
        self.taql = f'ANTENNA1 == {antenna} && ANTENNA2 == {antenna} ' + \
                    f'&& SPECTRAL_WINDOW_ID == {spw}'
        with table_selector(self.table, self.taql) as tb:
            self.time = tb.getcol('TIME')
            self.data = tb.getcol('FPARAM')
            self.flag = tb.getcol('FLAG')
            self.exposure = tb.getcol('INTERVAL')

    def interpolate(self, t):
        raise Exception('Not implemented')

    def weightscale_linear(self, dt_on, dt_off0, dt_off1=None, t_on=None, t_off0=None, t_off1=None):
        if dt_off1 is None:
            return self.weightscale_nearest(dt_on, dt_off0)
        else:
            delta = t_off1 - t_off0
            delta0 = t_on - t_off0
            delta1 = t_off1 - t_on
            sigmasqscale = 1.0 + dt_on / (delta * delta) \
                * (delta1 * delta1 / dt_off0 + delta0 * delta0 / dt_off1)
            return 1.0 / sigmasqscale

    def weightscale_nearest(self, dt_on, dt_off):
        return dt_off / (dt_on + dt_off)


class LinearInterpolator(Interpolator):
    def __init__(self, table, finterp='linear'):
        super(LinearInterpolator, self).__init__(table, finterp)

    def interpolate(self, t, tau):
        dt = self.time - t
        index = abs(dt).argmin()
        if dt[index] > 0.0:
            index -= 1
        if index < 0:
            ref = self.data[:, :, 0].copy()
            weightscale = self.weightscale_linear(tau, self.exposure[0])
        elif index >= len(self.time) - 1:
            ref = self.data[:, :, -1].copy()
            weightscale = self.weightscale_linear(tau, self.exposure[-1])
        else:
            t0 = self.time[index]
            t1 = self.time[index + 1]
            d0 = self.data[:, :, index]
            d1 = self.data[:, :, index + 1]
            dt0 = t - t0
            dt1 = t1 - t
            dt2 = t1 - t0
            ref = (dt1 * d0 + dt0 * d1) / dt2
            tau0 = self.exposure[index]
            tau1 = self.exposure[index + 1]
            weightscale = self.weightscale_linear(tau, tau0, tau1, t, t0, t1)
        flag = self.interpolate_flag(t)
        ref, refflag = self.finterp(ref, flag)

        return ref, refflag, weightscale

    def interpolate_flag(self, t):
        dt = self.time - t
        index = abs(dt).argmin()
        if dt[index] > 0.0:
            index -= 1
        if index < 0:
            flag = self.flag[:, :, 0].copy()
        elif index >= len(self.time) - 1:
            flag = self.flag[:, :, -1].copy()
        else:
            f0 = self.flag[:, :, index]
            f1 = self.flag[:, :, index + 1]
            flag = numpy.logical_or(f0, f1)

        return flag


class NearestInterpolator(Interpolator):
    def __init__(self, table, finterp='nearest'):
        super(NearestInterpolator, self).__init__(table, finterp)

    def interpolate(self, t, tau):
        dt = self.time - t
        index = abs(dt).argmin()
        weightscale = self.weightscale_nearest(tau, self.exposure[index])
        ref, refflag = self.finterp(self.data[:, :, index].copy(), self.flag[:, :, index].copy())
        return ref, refflag, weightscale


class sdcal_test_apply(sdcal_test_base):
    """
    Unit test for task sdcal (apply tables).

    The list of tests:
    test_apply_sky00 --- empty applytable
    test_apply_sky01 --- empty applytable (list ver.)
    test_apply_sky02 --- empty applytable list
    test_apply_sky03 --- unexisting applytable
    test_apply_sky04 --- unexisting applytable (list ver.)
    test_apply_sky05 --- invalid selection (empty selection result)
    test_apply_sky06 --- invalid interp value
    test_apply_sky07 --- invalid applytable (not caltable)
    test_apply_sky08 --- apply data (linear)
    test_apply_sky09 --- apply selected data
    test_apply_sky10 --- apply data (nearest)
    test_apply_sky11 --- apply data (linearflag for frequency interpolation)
    test_apply_sky12 --- apply data (nearestflag for frequency interpolation)
    test_apply_sky13 --- apply data (string applytable input)
    test_apply_sky14 --- apply data (interp='')
    test_apply_sky15 --- check if WEIGHT_SPECTRUM is updated properly when it exists
    test_apply_sky16 --- apply both sky table and Tsys table simultaneously
    test_apply_composite00 --- on-the-fly application of sky table ('ps,apply')
    test_apply_composite01 --- on-the-fly application of sky table with existing Tsys table
    test_apply_composite02 --- on-the-fly application of sky and tsys tables ('ps,tsys,apply')
    test_apply_composite03 --- on-the-fly application of sky table ('otfraster,apply')
    """

    invalid_argument_case = sdcal_test_base.invalid_argument_case
    exception_case = sdcal_test_base.exception_case

    @property
    def nrow_per_chunk(self):
        # number of rows per antenna per spw is 18
        return 18

    @property
    def eps(self):
        # required accuracy is 2.0e-4
        return 3.0e-4

    def setUp(self):
        self._setUp([self.infile, self.applytable], sdcal)

    def tearDown(self):
        self._tearDown([self.infile, self.applytable])

    def check_weight(self, inweight, outweight, scale):
        # print('inweight', inweight)
        # print('outweight', outweight)
        # print('scale', scale)
        # shape check
        self.assertEqual(inweight.shape, outweight.shape, msg='')

        # weight should not be zero
        self.assertFalse(any(inweight.flatten() == 0.0), msg='')
        self.assertFalse(any(outweight.flatten() == 0.0), msg='')

        # check difference
        expected_weight = inweight * scale
        diff = abs((outweight - expected_weight) / expected_weight)
        self.assertTrue(all(diff.flatten() < self.eps),
                        msg='')

    def normal_case(interp='linear', tsys=1.0, **kwargs):
        """Decorator for the test case that is intended to verify normal execution result.

        interp --- interpolation option ('linear', 'nearest', '*flag')
                   comma-separated list is allowed and it will be
                   interpreted as '<interp for time>,<intep for freq>'
        tsys --- tsys scaling factor
        selection --- data selection parameter as dictionary
        """
        def wrapper(func):
            import functools

            @functools.wraps(func)
            def _wrapper(self):
                # data selection
                myms = ms()
                myargs = kwargs.copy()
                if 'baseline' not in myargs:
                    with table_manager(self.infile) as tb:
                        antenna1 = numpy.unique(tb.getcol('ANTENNA1'))
                        myargs['baseline'] = '%s&&&' % (','.join(map(str, antenna1)))
                a = myms.msseltoindex(self.infile, **myargs)
                antennalist = a['antenna1']
                with table_manager(self.applytable) as tb:
                    spwlist = numpy.unique(tb.getcol('SPECTRAL_WINDOW_ID'))
                with table_manager(os.path.join(self.infile, 'DATA_DESCRIPTION')) as tb:
                    spwidcol = tb.getcol('SPECTRAL_WINDOW_ID').tolist()
                    spwddlist = map(spwidcol.index, spwlist)
                if len(a['spw']) > 0:
                    spwlist = list(set(spwlist) & set(a['spw']))
                    spwddlist = map(spwidcol.index, spwlist)

                # preserve original flag and weight
                flag_org = {}
                weight_org = {}
                weightsp_org = {}
                for antenna in antennalist:
                    flag_org[antenna] = {}
                    weight_org[antenna] = {}
                    weightsp_org[antenna] = {}
                    for (spw, spwdd) in zip(spwlist, spwddlist):
                        taql = f'ANTENNA1 == {antenna} && ANTENNA2 == {antenna} ' + \
                               f'&& DATA_DESC_ID == {spwdd}'
                        with table_selector(self.infile, taql) as tb:
                            flag_org[antenna][spw] = tb.getcol('FLAG')
                            weight_org[antenna][spw] = tb.getcol('WEIGHT')
                            if 'WEIGHT_SPECTRUM' in tb.colnames() \
                                    and tb.iscelldefined('WEIGHT_SPECTRUM', 0):
                                # print('WEIGHT_SPECTRUM is defined for '
                                #       f'antenna {antenna} spw {spw}')
                                weightsp_org[antenna][spw] = tb.getcol('WEIGHT_SPECTRUM')
                            # else:
                            #    print('WEIGHT_SPECTRUM is NOT defined for '
                            #          f'antenna {antenna} spw {spw}')

                # execute test
                func(self)

                # sanity check
                self.assertIsNone(self.result, msg='The task must complete without error')
                # verify if CORRECTED_DATA exists
                with table_manager(self.infile) as tb:
                    self.assertTrue(
                        'CORRECTED_DATA' in tb.colnames(),
                        msg='CORRECTED_DATA column must be created after task execution!')

                # parse interp
                pos = interp.find(',')
                if pos == -1:
                    tinterp = interp.lower()
                    finterp = 'linearflag'
                else:
                    tinterp = interp[:pos].lower()
                    finterp = interp[pos + 1:]
                if len(tinterp) == 0:
                    tinterp = 'linear'
                if len(finterp) == 0:
                    finterp = 'linearflag'

                # CAS-10772
                # Linear flag interpolation along spectral axis behaves like "nearest"
                # if science and calibrater data have same set of frequency channels.
                # This is always true for single dish sky calibration.
                # So, finterp option for flags should always be 'nearestflag'.
                if finterp == 'linearflag':
                    finterp = 'nearestflag'

                # result depends on interp
                print('Interpolation option:', tinterp, finterp)
                self.assertTrue(tinterp in ['linear', 'nearest'], msg='Internal Error')
                if tinterp == 'linear':
                    interpolator = LinearInterpolator(self.applytable, finterp)
                else:
                    interpolator = NearestInterpolator(self.applytable, finterp)
                for antenna in antennalist:
                    for (spw, spwdd) in zip(spwlist, spwddlist):
                        interpolator.select(antenna, spw)
                        taql = f'ANTENNA1 == {antenna} && ANTENNA2 == {antenna} ' + \
                               f'&& DATA_DESC_ID == {spwdd}'
                        with table_selector(self.infile, taql) as tb:
                            self.assertEqual(
                                tb.nrows(), self.nrow_per_chunk,
                                msg=f'Number of rows mismatch in antenna {antenna} spw {spw}')
                            if spw in weightsp_org[antenna]:
                                has_weightsp = True

                            else:
                                has_weightsp = False
                            for irow in range(tb.nrows()):
                                t = tb.getcell('TIME', irow)
                                dt = tb.getcell('INTERVAL', irow)
                                data = tb.getcell('DATA', irow)
                                outflag = tb.getcell('FLAG', irow)
                                corrected = tb.getcell('CORRECTED_DATA', irow)
                                ref, calflag, weightscale = interpolator.interpolate(t, dt)
                                inflag = flag_org[antenna][spw][:, :, irow]
                                expected = tsys * (data - ref) / ref
                                expected_flag = numpy.logical_or(inflag, calflag)

                                # weight test
                                self.assertEqual(tb.iscelldefined('WEIGHT_SPECTRUM', irow),
                                                 has_weightsp,
                                                 msg='')
                                inweight = weight_org[antenna][spw][:, irow]
                                outweight = tb.getcell('WEIGHT', irow)
                                tsyssq = tsys * tsys
                                if has_weightsp:
                                    # Need to check WEIGHT_SPECTRUM
                                    inweightsp = weightsp_org[antenna][spw][:, :, irow]
                                    outweightsp = tb.getcell('WEIGHT_SPECTRUM', irow)

                                    self.check_weight(inweight, outweight, weightscale / tsyssq)
                                    self.check_weight(inweightsp, outweightsp, weightscale / tsyssq)
                                else:
                                    self.check_weight(inweight, outweight, weightscale / tsyssq)

                                # print('antenna', antenna, 'spw', spw, 'row', irow)
                                # print('inflag', inflag[:,:12], 'calflag', calflag[:,:12],
                                #       'expflag', expected_flag[:,:12], 'outflag', outflag[:,:12])
                                # print('ref', ref[:,126:130], 'data', data[:,126:130],
                                #       'expected', expected[:,126:130],
                                #       'corrected', corrected[:,126:130])

                                self.assertEqual(
                                    corrected.shape,
                                    expected.shape,
                                    msg=f'Shape mismatch in antenna {antenna} spw {spw} row {irow} '
                                        f'(expeted {expected.shape} actual {corrected.shape})')
                                npol, nchan = corrected.shape

                                # verify data
                                diff = numpy.ones(expected.shape, dtype=float)
                                small_data = numpy.where(abs(expected) < 1.0e-7)
                                diff[small_data] = abs(corrected[small_data] - expected[small_data])
                                regular_data = numpy.where(abs(expected) >= 1.0e-7)
                                diff[regular_data] = abs(
                                    (corrected[regular_data] - expected[regular_data])
                                    / expected[regular_data])
                                self.assertTrue(
                                    all(diff.flatten() < self.eps),
                                    msg=f'Calibrated result differ in antenna {antenna} '
                                        f'spw {spw} row {irow} '
                                        f'(expected {expected} actual {corrected} diff {diff})')

                                # verify flag
                                self.assertTrue(
                                    all(outflag.flatten() == expected_flag.flatten()),
                                    msg=f'Resulting flag differ in antenna {antenna} '
                                        f'spw {spw} row {irow} '
                                        f'(expected {expected_flag} actual {outflag})')

            return _wrapper
        return wrapper

    @exception_case(Exception, 'Applytable name must be specified.')
    def test_apply_sky00(self):
        """test_apply_sky00 --- empty applytable."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable='')

    @exception_case(Exception, 'Applytable name must be specified.')
    def test_apply_sky01(self):
        """test_apply_sky01 --- empty applytable (list ver.)."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable=[''])

    @exception_case(Exception, 'Applytable name must be specified.')
    def test_apply_sky02(self):
        """test_apply_sky02 --- empty applytable list."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable=[])

    @exception_case(Exception, "^Table doesn't exist:")
    def test_apply_sky03(self):
        """test_apply_sky03 --- unexisting applytable."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable='notexist.sky')

    @exception_case(Exception, "^Table doesn't exist:")
    def test_apply_sky04(self):
        """test_apply_sky04 --- unexisting applytable (list ver.)."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable=['notexist.sky'])

    @exception_case(RuntimeError, 'Spw Expression: No match found for 99')
    def test_apply_sky05(self):
        """test_apply_sky05 --- invalid selection (empty selection result)."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            spw='99', applytable=[self.applytable])

    # @exception_case(RuntimeError, '^Unknown interptype: \'.+\'!! Check inputs and try again\.$')
    @exception_case(RuntimeError, 'Error in Calibrater::setapply.')
    def test_apply_sky06(self):
        """test_apply_sky06 --- invalid interp value."""
        # 'sinusoid' interpolation along time axis is not supported
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=[self.applytable], interp='sinusoid')

    @exception_case(RuntimeError, '^Applytable \'.+\' is not a caltable format$')
    def test_apply_sky07(self):
        """test_apply_sky07 --- invalid applytable (not caltable)."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=[self.infile], interp='linear')

    @normal_case()
    def test_apply_sky08(self):
        """test_apply_sky08 --- apply data (linear)."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=[self.applytable], interp='linear')

    @normal_case()
    def test_apply_sky08M(self):
        """test_apply_sky08M --- apply data (linear) for MMS."""
        self.skipTest(
            'Skip test_apply_sky08M until calibrator tool supports processing MMS on serial casa')
        # self.result = sdcal(infile=self.infile, calmode='apply',
        #                     applytable=[self.applytable], interp='linear')

    @normal_case(spw='9')
    def test_apply_sky09(self):
        """test_apply_sky09 --- apply selected data."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable=[
                            self.applytable], spw='9', interp='linear')

    @normal_case(interp='nearest')
    def test_apply_sky10(self):
        """test_apply_sky10 --- apply data (nearest)."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=[self.applytable], interp='nearest')

    @normal_case(interp='linear,linearflag')
    def test_apply_sky11(self):
        """test_apply_sky11 --- apply data (linearflag for frequency interpolation)."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable=[
                            self.applytable], interp='linear,linearflag')

    @normal_case(interp='linear,nearestflag')
    def test_apply_sky12(self):
        """test_apply_sky12 --- apply data (nearestflag for frequency interpolation)."""
        self.result = sdcal(infile=self.infile, calmode='apply', applytable=[
                            self.applytable], interp='linear,nearestflag')

    @normal_case(interp='linear')
    def test_apply_sky13(self):
        """test_apply_sky13 --- apply data (string applytable input)."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=self.applytable, interp='linear')

    @normal_case(interp='')
    def test_apply_sky14(self):
        """test_apply_sky14 --- apply data (interp='')."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=self.applytable, interp='')

    def fill_weightspectrum(func):
        import functools

        @functools.wraps(func)
        def wrapper(self):
            with table_manager(self.infile, nomodify=False) as tb:
                self.assertTrue('WEIGHT_SPECTRUM' in tb.colnames(), msg='Internal Error')
                nrow = tb.nrows()
                for irow in range(nrow):
                    w = tb.getcell('WEIGHT', irow)
                    wsp = numpy.ones(tb.getcell('DATA', irow).shape, dtype=float)
                    for ipol in range(len(w)):
                        wsp[ipol, :] = w[ipol]
                    tb.putcell('WEIGHT_SPECTRUM', irow, wsp)
                    self.assertTrue(tb.iscelldefined('WEIGHT_SPECTRUM', irow), msg='Internal Error')
            func(self)
        return wrapper

    @fill_weightspectrum
    @normal_case(interp='linear')
    def test_apply_sky15(self):
        """test_apply_sky15 --- check if WEIGHT_SPECTRUM is updated properly when it exists."""
        self.result = sdcal(infile=self.infile, calmode='apply',
                            applytable=self.applytable, interp='linear')

    def modify_tsys(func):
        import functools

        @functools.wraps(func)
        def wrapper(self):
            with table_manager(os.path.join(self.infile, 'SYSCAL'), nomodify=False) as tb:
                tsel = tb.query('SPECTRAL_WINDOW_ID IN [1,3]',
                                sortlist='ANTENNA_ID,SPECTRAL_WINDOW_ID,TIME')
                try:
                    nrow = tsel.nrows()
                    tsys = 100.0
                    for irow in range(nrow):
                        tsys_spectrum = tsel.getcell('TSYS_SPECTRUM', irow)
                        tsys_spectrum[:] = tsys
                        tsel.putcell('TSYS_SPECTRUM', irow, tsys_spectrum)
                        # tsys += 100.0
                finally:
                    tsel.close()
            func(self)
        return wrapper

    @normal_case()
    def test_apply_composite00(self):
        """test_apply_composite00 --- on-the-fly application of sky table ('ps,apply')."""
        sdcal(infile=self.infile, calmode='ps,apply')

    @modify_tsys
    @normal_case(tsys=100.0)
    def test_apply_composite01(self):
        """test_apply_composite01 --- on-the-fly application of sky table with existing Tsys table."""
        # generate Tsys table
        tsystable = self.infile.rstrip('/') + '.tsys'

        try:
            # generate Tsys table
            sdcal(infile=self.infile, calmode='tsys', outfile=tsystable)

            # apply
            sdcal(infile=self.infile, calmode='ps,apply', applytable=tsystable,
                  spwmap={1: [9], 3: [11]})
        finally:
            if os.path.exists(tsystable):
                shutil.rmtree(tsystable)

    @modify_tsys
    @normal_case(tsys=100.0)
    def test_apply_composite02(self):
        """test_apply_composite02 --- on-the-fly application of sky and tsys tables ('ps,tsys,apply')."""
        sdcal(infile=self.infile, calmode='ps,tsys,apply',
              spwmap={1: [9], 3: [11]})

    @modify_tsys
    @normal_case(tsys=100.0)
    def test_apply_composite03(self):
        """test_apply_composite03 --- on-the-fly application of sky table ('otfraster,apply')."""
        sdcal(infile=self.infile, calmode='tsys,apply', applytable=self.applytable,
              spwmap={1: [9], 3: [11]})


class sdcal_test_single_polarization(sdcal_test_base):
    """Unit test for task sdcal (calibration/application of single-polarization data).

    The list of tests:
    test_single_pol_ps --- generate caltable for single-polarization data
    test_single_pol_apply --- apply caltable to single-polarization data
    test_single_pol_apply_composite --- on-the-fly calibration/application
                                        on single-polarization data
    """

    datapath = ctsys_resolve('unittest/sdcal/')
    # Input
    infile = 'analytic_spectra.ms'

    # task execution result
    result = None

    @property
    def outfile(self):
        return self.applytable

    def setUp(self):
        self._setUp([self.infile], sdcal)

    def tearDown(self):
        self._tearDown([self.infile, self.outfile])

    def _verify_caltable(self):
        """Verify single polarization caltable.

        This method checks if

            - calibration solution is properly stored in pol 0
            - pol 1 is all flagged

        Generated caltable will have the following properties:

            - number of rows is 2
            - FPARAM (pol 0) has identical value to infile rows that
              corresponds to OFF_SOURCE intents (STATE_ID 1)
            - FPARAM (pol 1) is all 0 and is all flagged
        """
        # get reference data from infile
        with table_manager(self.infile, nomodify=False) as tb:
            tsel = tb.query('STATE_ID == 1', sortlist='TIME')
            try:
                reftime = tsel.getcol('TIME')
                refdata = tsel.getcol('FLOAT_DATA')
                refflag = tsel.getcol('FLAG')
            finally:
                tsel.close()

        # verify caltable
        with table_manager(self.outfile, nomodify=False) as tb:
            caltime = tb.getcol('TIME')
            fparam = tb.getcol('FPARAM')
            calflag = tb.getcol('FLAG')

        self.assertEqual(len(caltime), len(reftime))
        self.assertTrue(numpy.all(caltime == reftime))

        nrow = len(caltime)

        self.assertEqual(fparam.shape, calflag.shape)
        calshape = fparam.shape
        datashape = refdata.shape
        self.assertEqual(calshape[0], 2)
        self.assertEqual(datashape[0], 1)
        self.assertEqual(calshape[1], datashape[1])
        self.assertEqual(calshape[2], datashape[2])

        for irow in range(nrow):
            # FPARAM (pol 0)
            self.assertTrue(numpy.all(fparam[0, :, irow] == refdata[0, :, irow]))
            self.assertTrue(numpy.all(calflag[0, :, irow] == refflag[0, :, irow]))

            # FPARAM (pol 1)
            self.assertTrue(numpy.all(fparam[1, :, irow] == 0))
            self.assertTrue(numpy.all(calflag[1, :, irow] == True))

    def _verify_application(self):
        """Verify single polarization application result.

        This method checks if

            - calibration solution is properly applied

        CORRECTED_DATA column will have the following properties:

            - For calibration spectra (STATE_ID 0), CORRECTED_DATA is identical to FLOAT_DATA
            - For OFF_SOURCE spectra (STATE_ID 1), CORRECTED_DATA is all 0
            - For ON_SOURCE spectra (STATE_ID 2), CORRECTED_DATA is a calculated result of
              (ON - OFF) / OFF with interpolated OFF in time
        """
        with table_manager(self.infile, nomodify=False) as tb:
            # calibration spectra (STATE_ID 0)
            tsel = tb.query('STATE_ID == 0')
            try:
                float_data = tsel.getcol('FLOAT_DATA')
                corrected_data = tsel.getcol('CORRECTED_DATA')

                self.assertTrue(numpy.all(corrected_data.real == float_data))
                self.assertTrue(numpy.all(corrected_data.imag == 0.0))
            finally:
                tsel.close()

            # OFF_SOURCE spectra (STATE_ID 1)
            reftime = None
            refdata = None
            tsel = tb.query('STATE_ID == 1', sortlist='TIME')
            try:
                corrected_data = tsel.getcol('CORRECTED_DATA')

                self.assertTrue(numpy.all(corrected_data.real == 0.0))
                self.assertTrue(numpy.all(corrected_data.imag == 0.0))
            finally:
                reftime = tsel.getcol('TIME')
                refdata = tsel.getcol('FLOAT_DATA')

                tsel.close()

            # ON_SOURCE spectra (STATE_ID 2)
            self.assertFalse(reftime is None)
            self.assertFalse(refdata is None)
            tsel = tb.query('STATE_ID == 2')
            try:
                sptime = tsel.getcol('TIME')
                float_data = tsel.getcol('FLOAT_DATA')
                corrected_data = tsel.getcol('CORRECTED_DATA')

                self.assertEqual(len(reftime), 2)

                nrow = float_data.shape[2]

                off_data = numpy.zeros(corrected_data.shape, dtype=numpy.float64)
                for irow in range(nrow):
                    off_data[:, :, irow] = (refdata[:, :, 1] * (sptime[irow] - reftime[0])
                                            + refdata[:, :, 0] * (reftime[1] - sptime[irow])) \
                        / (reftime[1] - reftime[0])
                calibrated = (float_data - off_data) / off_data

                # exclude nan
                idx_not_nan = numpy.where(numpy.isfinite(float_data))
                diff = numpy.abs((corrected_data.real - calibrated) / calibrated)
                diff_not_nan = diff[idx_not_nan]
                eps = 1.0e-7
                # print('maxdiff = {}'.format(diff_not_nan.max()))
                self.assertTrue(numpy.all(diff_not_nan < eps))
                self.assertTrue(numpy.all(corrected_data[idx_not_nan].imag == 0.0))
            finally:
                tsel.close()

    def test_single_pol_ps(self):
        """test_single_pol_ps --- generate caltable for single-polarization data."""
        self.result = sdcal(infile=self.infile, calmode='ps', outfile=self.outfile)
        self._verify_caltable()

    def test_single_pol_apply(self):
        """test_single_pol_apply --- apply caltable to single-polarization data."""
        self.test_single_pol_ps()
        self.assertTrue(os.path.exists(self.outfile))

        self.result = sdcal(infile=self.infile, calmode='apply', applytable=self.outfile)
        self._verify_application()

    def test_single_pol_apply_composite(self):
        """test_single_pol_apply_composite --- on-the-fly calibration/application on single-polarization data."""
        self.result = sdcal(infile=self.infile, calmode='ps,apply')
        self._verify_application()


if __name__ == '__main__':
    unittest.main()
