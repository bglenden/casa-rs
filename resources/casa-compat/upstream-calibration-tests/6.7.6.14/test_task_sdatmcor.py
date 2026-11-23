########################################################################
# test_task_sdatmcor.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.single.sdatmcor.html
#
#
##########################################################################
import contextlib
import functools
import itertools
import os
import re
import shutil
import unittest

import numpy as np

from casatasks import applycal, casalog, gencal, sdatmcor
from casatasks.private.sdutil import (convert_antenna_spec_autocorr,
                                      get_antenna_selection_include_autocorr,
                                      table_manager, table_selector)
import casatasks.private.task_sdatmcor as sdatmcor_impl
from casatools import calibrater, ctsys
from casatools import ms as mstool
from casatools import quanta

ctsys_resolve = ctsys.resolve

# hold omp_get_num_threads value when this file is imported
OMP_NUM_THREADS_INITIAL = casalog.ompGetNumThreads()


def smart_remove(name):
    if os.path.exists(name):
        if os.path.isdir(name):
            shutil.rmtree(name)
        elif os.path.isfile(name):
            os.remove(name)
        else:
            # could be a symlink
            os.remove(name)


def std_with_clip(arr, num_iter=1, factor=3):
    assert num_iter > 0
    std = np.abs(arr).max()
    for i in range(num_iter):
        std = arr[np.abs(arr) < std * factor].std()
    return std


def read_table(name, spw, cols=['STATE_ID', 'DATA']):
    ms = mstool()
    idx = ms.msseltoindex(name, spw=[int(spw)])
    ddid = idx['dd']
    with table_manager(name) as tb:
        tsel = tb.query('DATA_DESC_ID IN [{}]'.format(','.join([str(i) for i in ddid])))
        try:
            result_dict = dict((k, tsel.getcol(k)) for k in cols if k in tb.colnames())
        finally:
            tsel.close()
    return result_dict


def apply_gainfactor(name, spw, factor):
    ms = mstool()
    idx = ms.msseltoindex(name, spw=[int(spw)])
    ddid = idx['dd'][0]
    with table_manager(name, nomodify=False) as tb:
        colnames = tb.colnames()
        tsel = tb.query('DATA_DESC_ID=={}'.format(ddid))
        try:
            for colname in ['DATA', 'FLOAT_DATA', 'CORRECTED_DATA']:
                if colname in colnames:
                    data = tsel.getcol(colname)
                    tsel.putcol(colname, data * factor)
        finally:
            tsel.close()


@contextlib.contextmanager
def environment_variable_manager(var_name):
    var_org = os.environ.get(var_name)
    try:
        yield var_org
    finally:
        if var_org is None:
            os.environ.pop(var_name, None)
        else:
            os.environ[var_name] = var_org


def get_end_pos_for(logfile: str) -> int:
    with open(logfile, 'r') as f:
        offset = 0
        from_end_of_file = os.SEEK_END
        f.seek(offset, from_end_of_file)
        return f.tell()


class test_sdatmcor(unittest.TestCase):
    datapath = ctsys_resolve('measurementset/almasd')
    infile = 'X320b_sel2.ms'
    outfile = infile + '.atmcor'
    caltable = infile + '.k2jycal'

    local_unit_test = False

    casalog_seek_pos = 0

    def setUp(self):
        # default Args
        self.args = {
            'infile': self.infile,
            'datacolumn': 'data',
            'outfile': self.outfile,
        }

        smart_remove(self.infile)
        smart_remove(self.outfile)
        smart_remove(self.caltable)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

        self.casalog_seek_pos = get_end_pos_for(casalog.logfile())

    def tearDown(self):
        smart_remove(self.infile)
        smart_remove(self.outfile)
        smart_remove(self.caltable)

    def skip_omp_test_if_darwin(self):
        sysname = os.uname()[0]
        if sysname == 'Darwin':
            self.skipTest('Skip OpenMP tests on macOS')
            return

    def _check_result_spw(self, spw, is_selected, is_processed, on_source_only):

        contents_after = read_table(self.outfile, spw, ['STATE_ID', 'DATA', 'CORRECTED_DATA'])
        stateids_after = contents_after['STATE_ID']
        if 'CORRECTED_DATA' in contents_after:
            data_after = contents_after['CORRECTED_DATA'].real
        else:
            data_after = contents_after['DATA'].real

        # if is_selected is False, selected data should be empty
        if not is_selected:
            self.assertEqual(len(stateids_after), 0)
            return

        contents_before = read_table(self.infile, spw, ['STATE_ID', 'DATA', 'CORRECTED_DATA'])
        stateids_before = contents_before['STATE_ID']
        if 'CORRECTED_DATA' in contents_before:
            data_before = contents_before['CORRECTED_DATA'].real
        else:
            data_before = contents_before['DATA'].real

        # check if spw exist in the data
        self.assertGreater(len(stateids_after), 0)

        if is_processed:
            if spw == 23:
                # examine averaged spectrum
                #   - exclude edge channels (4 channels)
                #   - take average along pol and time axes
                #   - subtract zero order baseline
                #   - compute std with clipping (3 iterations => 2 clipping)
                #   - set threshold to 7.5 times std
                #   - check if any data exceeds threshold for data before correction
                #   - check if no data exceeds threshold for data after correction
                edge = 4
                # data before correction
                mask0 = np.logical_or(stateids_before == 14, stateids_before == 84)
                data_on = data_before[:, edge:-edge, mask0]
                data_mean = data_on.mean(axis=(0, 2))
                data_sub = data_mean - np.median(data_mean)
                threshold = 7.5 * std_with_clip(data_sub, num_iter=3)
                self.assertFalse(np.all(np.abs(data_sub) < threshold))
                # data after correction
                mask1 = np.logical_or(stateids_after == 14, stateids_after == 84)
                data_on = data_after[:, edge:-edge, mask1]
                data_mean = data_on.mean(axis=(0, 2))
                data_sub = data_mean - np.median(data_mean)
                threshold = 7.5 * std_with_clip(data_sub, num_iter=3)
                self.assertTrue(np.all(np.abs(data_sub) < threshold))
            elif spw == 19:
                # examine averaged spectrum
                #   - take average along pol and time axes
                #   - take difference of data before and after correction
                #   - check if diff.mean() > 0.95 * data_before_correction.mean()
                #     which means that most of the continuum-like component is
                #     originated by residual of atmospheric emission
                #   - check if std of difference is less than 30% of
                #     (diff.max() - diff.min())
                mask0 = np.logical_or(stateids_before == 14, stateids_before == 84)
                data_on_before = data_before[:, :, mask0]
                data_mean_before = data_on_before.mean(axis=(0, 2))
                mask1 = np.logical_or(stateids_after == 14, stateids_after == 84)
                data_on_after = data_after[:, :, mask1]
                data_mean_after = data_on_after.mean(axis=(0, 2))
                diff = data_mean_before - data_mean_after
                diff_mean = diff.mean()
                diff_std = diff.std()
                diff_std_norm = diff_std / (diff.max() - diff.min())
                self.assertGreater(diff_mean, data_mean_after.mean() * 0.95)
                self.assertLess(diff_std_norm, 0.3)

            if not on_source_only:
                # OFF_SOURCE data should not be touched
                mask0 = np.logical_and(stateids_before != 14, stateids_before != 84)
                data_off_before = data_before[:, :, mask0]
                mask1 = np.logical_and(stateids_after != 14, stateids_after != 84)
                data_off_after = data_after[:, :, mask1]
                self.assertTrue(np.all(data_off_before == data_off_after))
        else:
            self.assertTrue(np.all(data_after == data_before))

    def __check_casalog_spw_corrected(self, striplog: list, spwprocess: dict):
        """Test if casalog contains proper log message for list of corrected spws.

        Expected log message is,

            "processspw (input) = [XX, YY, ...]",

        where [XX, YY, ...] is a list of corrected spw ids.

        Args:
            striplog: Log message
            spwprocess: Dictionary describing input parameter
        """
        log_corrected = [line for line in striplog if line.startswith('processspw (input) = ')]
        spw_corrected = sorted([k for k, v in spwprocess.items() if v])
        # corrected spws must be non-zero
        self.assertGreater(len(spw_corrected), 0)
        # this log message must present
        self.assertEqual(len(log_corrected), 1)
        # extract list of spws from the log, and compare with expected list
        try:
            # the string to be evaluated should be a list like '[17, 19, 21, 23]'
            spw_list_str = log_corrected[0].split('=')[1].strip()
            spw_corrected_from_log = eval(spw_list_str)
        except Exception as e:
            print(str(e))
            self.fail(f'Unexpected log format: {spw_list_str}')
        self.assertEqual(spw_corrected_from_log, spw_corrected)

    def __check_casalog_spw_not_corrected(self, striplog: list, spwprocess: dict):
        """Test if casalog contains proper log message for list of *not* corrected spws.

        Expected log message is either,

            "SPWs XX YY ... are output but not corrected", or
            "SPW XX is output but not corrected",

        where XX YY ... or XX are a list of *not* corrected spw ids.

        Args:
            striplog: Log message
            spwprocess: Dictionary to describe spw selection
        """
        log_not_corrected = [line for line in striplog if line.endswith(' output but not corrected')]
        spw_not_corrected = sorted([k for k, v in spwprocess.items() if not v])
        if len(spw_not_corrected) > 0:
            # not corrected spws present, so log message must present
            self.assertEqual(len(log_not_corrected), 1)
            # extract list of spws from the log, and compare with expected list
            spw_not_corrected_from_log = [int(v) for v in log_not_corrected[0].split()[1:-5]]
            self.assertEqual(spw_not_corrected_from_log, spw_not_corrected)
        else:
            # all the spws are corrected, so log message must not present
            self.assertEqual(len(log_not_corrected), 0)

    def _check_casalog(self, spwprocess: dict):
        """Test if casalog contains expected log messages.

        See CAS-14171 for the purpose of the test.

        Args:
            spwprocess: Dictionary to describe spw selection.
                        Keys are the list of spws selected (outputspw)
                        while values indicate whether each spw is
                        corrected (True) or not (False).
        """
        # WARNING
        # Current code may result in intermittent and non-deterministic test failures,
        # in case the expected log lines have not yet been flushed.
        # If such failures occur, please consider to make it possible to: casalog.flush()
        with open(casalog.logfile(), 'r') as f:
            f.seek(self.casalog_seek_pos, 0)
            log = f.read()
        lines = log.split('\n')
        # strip meta data, only keep body of the log
        striplog = [line.split('\t')[-1] for line in lines]
        self.assertGreater(len(log), 0)
        self.__check_casalog_spw_corrected(striplog, spwprocess)
        self.__check_casalog_spw_not_corrected(striplog, spwprocess)

    def check_result(self, spwprocess, on_source_only=False):
        """Check Result.

        Args:
            spwprocess (dict): key is spw id, value is whether or not the spw is processed
        """
        # outfile should exist
        self.assertTrue(os.path.exists(self.outfile))

        for spw in [19, 23]:
            is_selected = spw in spwprocess
            is_processed = spwprocess.get(spw, False)
            self._check_result_spw(spw, is_selected, is_processed, on_source_only)

        # test OpenMP related stuff
        self.assertEqual(casalog.ompGetNumThreads(), OMP_NUM_THREADS_INITIAL)

        # test casalog messages
        self._check_casalog(spwprocess)

    def test_sdatmcor_normal(self):
        """Test normal usage of sdatmcor."""
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data')
        self.check_result({19: True, 23: True})

    def test_sdatmcor_explicit_atmtype(self):
        """Test specifying atmtype explicitly."""
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data', atmtype=2)
        self.check_result({19: True, 23: True})

    def test_sdatmcor_overwrite(self):
        """Test overwriting existing outfile."""
        os.mkdir(self.outfile)
        self.assertTrue(os.path.exists(self.outfile))
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data', overwrite=True)
        self.check_result({19: True, 23: True})

    def test_sdatmcor_no_overwrite(self):
        """Test to avoid overwriting existing outfile."""
        os.mkdir(self.outfile)
        self.assertTrue(os.path.exists(self.outfile))
        with self.assertRaises(Exception):
            sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data', overwrite=False)

    def test_sdatmcor_wrong_datacolumn(self):
        """Test wrong datacolumn."""
        wrong_colnames = ['corrected', 'float_data']
        for colname in wrong_colnames:
            with self.assertRaises(Exception):
                sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn=colname)

    def test_sdatmcor_corrected(self):
        """Test if CORRECTED_DATA column is handled properly."""
        # add CORRECTED_DATA column
        cb = calibrater()
        cb.open(self.infile, addcorr=True, addmodel=False)
        cb.close()
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='corrected')
        self.check_result({19: True, 23: True})

    def test_sdatmcor_spw_select_23(self):
        """Test data selection: select spw 23."""
        sdatmcor(infile=self.infile, outputspw='23', outfile=self.outfile, datacolumn='data')
        self.check_result({23: True})

    def test_sdatmcor_spw_select_all(self):
        """Test data selection: select 19 and 23 explicitly."""
        sdatmcor(infile=self.infile, outputspw='19,23', outfile=self.outfile, datacolumn='data')
        self.check_result({19: True, 23: True})

    def test_sdatmcor_spw_process_19(self):
        """Test data selection: process only spw 19."""
        sdatmcor(infile=self.infile, spw='19', outfile=self.outfile, datacolumn='data')
        self.check_result({19: True, 23: False})

    def test_sdatmcor_spw_process_all(self):
        """Test data selection: declare to process 19 and 23 explicitly."""
        sdatmcor(infile=self.infile, spw='19,23', outfile=self.outfile, datacolumn='data')
        self.check_result({19: True, 23: True})

    def test_sdatmcor_spw_process_23_select_23(self):
        """Test data selection: select and process spw 23."""
        sdatmcor(infile=self.infile, spw='23', outputspw='23',
                 outfile=self.outfile, datacolumn='data')
        self.check_result({23: True})

    def test_sdatmcor_spw_process_all_select_19(self):
        """Test data selection: process spw 19 and 23 but output only spw 19."""
        sdatmcor(infile=self.infile, spw='19,23', outputspw='19',
                 outfile=self.outfile, datacolumn='data')
        self.check_result({19: True})

    def test_sdatmcor_spw_process_23_select_all(self):
        """Test data selection: process only spw 23 but output both 19 and 23."""
        sdatmcor(infile=self.infile, spw='23', outputspw='19,23',
                 outfile=self.outfile, datacolumn='data')
        self.check_result({19: False, 23: True})

    def test_sdatmcor_spw_process_99_select_all(self):
        """Test data selection: specify invalid spw to process."""
        with self.assertRaises(Exception):
            sdatmcor(infile=self.infile, spw='19,99', outputspw='19,23',
                     outfile=self.outfile, datacolumn='data')

    def test_sdatmcor_intent_selection(self):
        """Test intent selection: test if selection of ON_SOURCE data (i.e. excluding OFF_SOURCE data) still works."""
        sdatmcor(infile=self.infile, outfile=self.outfile,
                 intent='OBSERVE_TARGET#ON_SOURCE*', datacolumn='data')
        self.check_result({19: True, 23: True}, on_source_only=True)

    def test_sdatmcor_spw_process_less_than_20_select_all(self):
        """Test data selection: specify invalid spw to process."""
        sdatmcor(infile=self.infile, spw='<20', outputspw='',
                 outfile=self.outfile, datacolumn='data')
        self.check_result({19: True, 23: False})

    def test_sdatmcor_scan_selection(self):
        """Test data selection: select only one scan (scan 5)."""
        # just to confirm the task completes without error
        try:
            sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data', scan='5')
        except Exception:
            self.fail('sdatmcor should not raise any Exception')

    def test_sdatmcor_antenna_selection(self):
        """Test antenna selection."""
        sdatmcor(infile=self.infile, antenna='PM02', outfile=self.outfile)
        self.check_result({19: True, 23: True})

    def test_sdatmcor_msselect(self):
        """Test msselect."""
        sdatmcor(infile=self.infile, msselect='ANTENNA1 == 1', outfile=self.outfile)
        self.check_result({19: True, 23: True})

        with self.assertRaises(Exception):
            sdatmcor(infile=self.infile, msselect='ANTENNA1 == 2', outfile=self.outfile)

    def test_sdatmcor_gainfactor_float(self):
        """Test gainfactor: float input."""
        gainfactor = 10.0
        apply_gainfactor(self.infile, 19, gainfactor)
        apply_gainfactor(self.infile, 23, gainfactor)
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data', gainfactor=gainfactor)
        self.check_result({19: True, 23: True})

    def test_sdatmcor_gainfactor_dict(self):
        """Test gainfactor: dict input."""
        gainfactor = {'19': 10.0, '23': 45.0}
        apply_gainfactor(self.infile, 19, gainfactor['19'])
        apply_gainfactor(self.infile, 23, gainfactor['23'])
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data', gainfactor=gainfactor)
        self.check_result({19: True, 23: True})

    def test_sdatmcor_gainfactor_caltable(self):
        """Test gainfactor: caltable input."""
        gainfactor = {'19': 10.0, '23': 45.0}
        for k, v in gainfactor.items():
            p = 1 / np.sqrt(v)
            gencal(vis=self.infile, caltable=self.caltable, caltype='amp', spw=k, parameter=[p])
        applycal(vis=self.infile, gaintable=self.caltable, flagbackup=False)
        sdatmcor(infile=self.infile, outfile=self.outfile,
                 datacolumn='corrected', gainfactor=gainfactor)
        self.check_result({19: True, 23: True})

    def test_parse_gainfactor_exception(self):
        """Test exception raised in parse_gainfactor."""
        with self.assertRaises(RuntimeError):
            sdatmcor_impl.parse_gainfactor(self.infile)

    def test_parse_spw(self):
        """Test utility functio, parse_spw."""
        test_cases = [
            ('', [19, 23]),
            ('*', [19, 23]),
            ('19,23', [19, 23]),
            ('19', [19]),
            ('<20', [19]),
            ('17,19,21', [19]),
            ('>20', [23]),
            ('0', []),
            ('1000', RuntimeError),
            ([19], TypeError),
        ]
        for spw, expected in test_cases:
            if isinstance(expected, list):
                actual = sdatmcor_impl.parse_spw(self.infile, spw)
                self.assertEqual(
                    actual,
                    expected,
                    msg='failed: input {} result {} expected {}'.format(
                        spw, actual, expected
                    )
                )
            else:
                # error cases
                with self.assertRaises(expected):
                    actual = sdatmcor_impl.parse_spw(self.infile, spw)

    def test_tweak_antenna_selection(self):
        """Test tweak of antenna selection."""
        # common test cases
        test_cases0 = [
            ('', ''),
            ('PM02', 'PM02&&&'),
            ('PM02&', 'PM02&&&'),
            ('PM02&&', 'PM02&&'),
            ('PM02&&&', 'PM02&&&'),
            ('0', '0&&&'),
            ('0&', '0&&&'),
            ('0&&', '0&&'),
            ('0&&&', '0&&&'),
            ('0&1', '0&&&;1&&&'),
            ('0&&1', '0&&&;1&&&'),
            ('0;1', '0&&&;1&&&'),
        ]
        # specific to convert_antenna_spec_autocorr
        test_cases1 = [
            ('0&;1&&', '0&&&;1&&'),
            ('0&&&;1', '0&&&;1&&&')
        ]
        for antenna, expected in test_cases0 + test_cases1:
            actual = convert_antenna_spec_autocorr(antenna)
            self.assertEqual(actual, expected)
        # specific to get_antenna_selection_include_autocorr
        test_cases2 = [
            ('0&;1&&', '0&;1&&'),
            ('0&&&;1', '0&&&;1'),
        ]
        for antenna, expected in test_cases0 + test_cases2:
            actual = get_antenna_selection_include_autocorr(self.infile, antenna)
            self.assertEqual(actual, expected)

    def test_get_default_antenna(self):
        """Test get_default_antenna and relevant function."""
        duration_ref = {
            'PM01': 1024.320,
            'PM02': 1019.808,
            'PM03': 1027.728,
            'PM04': 1046.112,
        }
        count_ref = {'PM01': 474, 'PM02': 475, 'PM03': 474, 'PM04': 474}
        counts, durations = sdatmcor_impl.inspect_flag_cmd(self.infile)
        for k, v in count_ref.items():
            self.assertEqual(v, counts[k])
        for k, v in duration_ref.items():
            self.assertAlmostEqual(v, durations[k], places=4)

        # default antenna should be PM02 (ID 1)
        default_antenna = sdatmcor_impl.get_default_antenna(self.infile)
        self.assertEqual(default_antenna, 1)

    def test_default_antenna_with_selection(self):
        """Test default antenna determination with data selection excluding the best one."""
        # check if the "best" antenna is not in the MAIN table
        with table_manager(os.path.join(self.infile, 'ANTENNA'), nomodify=False) as tb:
            # replace antenna name to simulate the data selection excluding PM02
            # PM01 <-> PM02
            tb.putcell('NAME', 0, 'PM02')
            tb.putcell('NAME', 1, 'PM01')
        sdatmcor(infile=self.infile, outfile=self.outfile, spw='19', datacolumn='data')
        self.check_result({19: True, 23: False})

    def test_default_antenna_with_no_flag_commands(self):
        """Test default antenna determination for empty FLAG_CMD table."""
        # check if the task can handle empty FLAG_CMD table
        with table_manager(os.path.join(self.infile, 'FLAG_CMD'), nomodify=False) as tb:
            for i in range(tb.nrows()):
                tb.putcell('REASON', i, 'NO_REASON')
            tb.flush()
        sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data')
        self.check_result({19: True, 23: True})

    def test_custom_atm_params(self):
        """Test customized ATM parameters."""
        sdatmcor(
            infile=self.infile, outfile=self.outfile, datacolumn='data',
            dtem_dh='-5.7K/km', h0='2010m',
            atmdetail=True,
            altitude='5.1km', temperature='290K', pressure='700hPa',
            humidity=30, pwv='0.1cm', dp='10hPa', dpm=1.2,
            layerboundaries='800m,1.5km', layertemperature='250K,200K'
        )
        self.check_result({19: True, 23: True})

    def test_custom_atm_params_nounit(self):
        """Test customized ATM parameters (no unit)."""
        sdatmcor(
            infile=self.infile, outfile=self.outfile, datacolumn='data',
            dtem_dh=-5.7, h0=2.01,
            atmdetail=True,
            altitude=5100., temperature=290., pressure=700.,
            humidity=30, pwv=10., dp=10., dpm=1.2,
            layerboundaries=[800., 1500.], layertemperature=[250., 200.]
        )
        self.check_result({19: True, 23: True})

    def test_custom_atm_params_non_conform_list_input(self):
        """Test customized ATM parameters: non-conform layerboundaries and layertemperature."""
        with self.assertRaises(Exception):
            sdatmcor(
                infile=self.infile, outfile=self.outfile, datacolumn='data',
                atmdetail=True,
                layerboundaries='800m,1.5km', layertemperature='250K,200K,190K'
            )

    def __extract_num_threads_from_logfile(self, logfile):
        with open(logfile, 'r') as file:
            pattern = re.compile(r'.*Setting numThreads_ to ([0-9]+)')
            matches_iterator = map(lambda line: pattern.search(line), file)
            last_match = functools.reduce(
                lambda last_match_, current_match:
                    current_match if current_match else last_match_,
                matches_iterator
            )

        self.assertIsNotNone(
            last_match,
            msg=f'No match found for pattern "{pattern.pattern}" in "{casalog.logfile()}"'
        )
        num_threads_log = int(last_match.group(1))

        return num_threads_log

    def test_set_omp_num_threads(self):
        """Test if the task respects OMP_NUM_THREADS environment variable."""
        self.skip_omp_test_if_darwin()

        omp_num_threads_org_int = None

        with environment_variable_manager('OMP_NUM_THREADS') as omp_num_threads_org:
            # set num_threads for OpenMP to any value different from the current one
            if omp_num_threads_org is None:
                num_threads = 2
            else:
                self.assertTrue(
                    omp_num_threads_org.isdigit(),
                    msg="Value of OMP_NUM_THREADS environment variable must be integer"
                )
                omp_num_threads_org_int = int(omp_num_threads_org)
                num_threads = omp_num_threads_org_int + 1
            self.assertNotEqual(num_threads, omp_num_threads_org_int)
            os.environ['OMP_NUM_THREADS'] = f'{num_threads}'

            # run task
            sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data')

        # consistency check
        omp_num_threads_current = os.environ.get('OMP_NUM_THREADS')
        if omp_num_threads_current is None:
            self.assertIsNone(omp_num_threads_org)
        else:
            self.assertIsNotNone(omp_num_threads_org)
            self.assertEqual(omp_num_threads_current, omp_num_threads_org)

        # check log
        self.assertTrue(os.path.exists(casalog.logfile()), msg='casalog file is missing!')
        num_threads_log = self.__extract_num_threads_from_logfile(casalog.logfile())

        casalog.post(
            f'OMP_NUM_THREAD_VALUES: initial: {OMP_NUM_THREADS_INITIAL} (returned by get_omp_num_threads), '
            f'at test start time: {omp_num_threads_org}, current: {num_threads}, '
            f'last set in logfile: {num_threads_log}')
        self.assertEqual(num_threads, num_threads_log)

        # check output MS
        self.check_result({19: True, 23: True})

    def test_set_omp_num_threads_zero(self):
        """Test if the task works when OMP_NUM_THREADS is zero."""
        self.skip_omp_test_if_darwin()

        with environment_variable_manager('OMP_NUM_THREADS') as omp_num_threads_org:
            # set num_threads for OpenMP to any value different from the current one
            num_threads = 0
            os.environ['OMP_NUM_THREADS'] = f'{num_threads}'

            # run task
            sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data')

        # consistency check
        omp_num_threads_current = os.environ.get('OMP_NUM_THREADS')
        if omp_num_threads_current is None:
            self.assertIsNone(omp_num_threads_org)
        else:
            self.assertIsNotNone(omp_num_threads_org)
            self.assertEqual(omp_num_threads_current, omp_num_threads_org)

        # check log
        self.assertTrue(os.path.exists(casalog.logfile()), msg='casalog file is missing!')
        num_threads_log = self.__extract_num_threads_from_logfile(casalog.logfile())
        num_threads_expected = min(8, casalog.getNumCPUs())

        casalog.post(
            f'OMP_NUM_THREAD_VALUES: initial: {OMP_NUM_THREADS_INITIAL} (returned by get_omp_num_threads), '
            f'at test start time: {omp_num_threads_org}, current: {num_threads}, '
            f'last set in logfile: {num_threads_log}')
        self.assertEqual(num_threads_expected, num_threads_log)

        # check output MS
        self.check_result({19: True, 23: True})

    def test_unset_omp_num_threads(self):
        """Test if the task respects OMP_NUM_THREADS environment variable."""
        self.skip_omp_test_if_darwin()

        with environment_variable_manager('OMP_NUM_THREADS') as omp_num_threads_org:
            # unset OMP_NUM_THREADS if it is set
            if omp_num_threads_org is not None:
                os.environ.pop('OMP_NUM_THREADS')
            self.assertFalse('OMP_NUM_THREADS' in os.environ)

            # run task
            sdatmcor(infile=self.infile, outfile=self.outfile, datacolumn='data')

        # consistency check
        if omp_num_threads_org is None:
            self.assertIsNone(os.environ.get('OMP_NUM_THREADS'))
        else:
            self.assertEqual(os.environ.get('OMP_NUM_THREADS'), omp_num_threads_org)

        # check log
        self.assertTrue(os.path.exists(casalog.logfile()), msg='casalog file is missing!')
        num_threads_log = self.__extract_num_threads_from_logfile(casalog.logfile())
        num_threads_expected = min(8, casalog.getNumCPUs())

        casalog.post(
            f'OMP_NUM_THREAD_VALUES: initial: {OMP_NUM_THREADS_INITIAL} (returned by get_omp_num_threads), '
            f'at test start time: {omp_num_threads_org}, expected: {num_threads_expected}, '
            f'last set in logfile: {num_threads_log}')
        self.assertEqual(num_threads_expected, num_threads_log)

        # check output MS
        self.check_result({19: True, 23: True})


def set_data_to_zero(infile: str, spw: int) -> np.ndarray:
    """Set ON_SOURCE CORRECTED_DATA to zero for given spw.

    This is implemented by TaQL and is intended to do the
    same with the following code snippet.

        ms.open(infile)
        ms.msselect(
            {'spw': str(spw), 'scanintent': 'OBSERVE_TARGET#ON_SOURCE'},
            onlyparse=True
        )
        msidx = ms.msselectedindices()
        ms.close()
        ddid = msidx['spwdd'][0]
        stateid = list(msidx['stateid'])
        taql = f'DATA_DESC_ID == {ddid} && STATE_ID IN {stateid}'

        with table_selector(infile, taql=taql, nomodify=False) as tb:
            cdata = tb.getcol('CORRECTED_DATA')
            cdata[::] = 0
            tb.putcol(colname, cdata)
        return cdata

    Args:
        infile: Input MS name
        spw: Spectral window Id

    Returns:
        Data array manipulated by this function
    """
    with table_manager(infile) as tb:
        taql_string = f'''
        USING STYLE PYTHON
        UPDATE "{infile}" SET CORRECTED_DATA = 0
        WHERE
          DATA_DESC_ID IN
            [SELECT ROWID() FROM ::DATA_DESCRIPTION WHERE SPECTRAL_WINDOW_ID == {spw}]
          && STATE_ID IN
            [SELECT ROWID() FROM ::STATE WHERE OBS_MODE ~ m/^OBSERVE_TARGET#ON_SOURCE/]
        '''
        t = tb.taql(taql_string)
        cdata = t.getcol('CORRECTED_DATA')
        t.close()
    return cdata.real


class test_sdatmcor_smoothing(unittest.TestCase):
    datapath = 'measurementset/almasd'
    infile = 'X59ca_sel.ms'
    outfile = infile + '.atmcor'

    def setUp(self):
        smart_remove(self.infile)
        smart_remove(self.outfile)
        datapath_to_ms = ctsys_resolve(os.path.join(self.datapath, self.infile))
        shutil.copytree(datapath_to_ms, self.infile)

    def tearDown(self):
        smart_remove(self.infile)
        smart_remove(self.outfile)

    def _get_data(self, ms_name, spw):
        with table_selector(ms_name, f'DATA_DESC_ID=={spw}') as tb:
            data = tb.getcol('DATA').real

        return data

    def test_mitigation(self):
        """Test if mitigation for boundary effect of convolution works."""
        # Set data zero to get correction factor
        # In sdatmcor, correction factor is subtracted from the data
        # so we can get correction factor if we set data all zero
        # (output_data = 0 - correction_factor).
        zero_data = set_data_to_zero(self.infile, spw=17)
        self.assertTrue(np.all(zero_data == 0))

        # Apply correction with smoothing
        sdatmcor(
            infile=self.infile,
            outfile=self.outfile,
            spw='17,19',
            intent='OBSERVE_TARGET#ON_SOURCE',
            datacolumn='corrected',
            gainfactor={17: 41.49, 19: 41.48},
            dtem_dh=-5.6,
            h0=2.0,
            atmtype=1
        )

        # Resulting data should be -correction_factor. Only check spectral
        # data for spw 17 since it is severely suffered from the boundary
        # effect. If mitigation didn't work, there will be steep increase
        # or decrease at edge channels. In that case, the slope should be
        # order of magnitude larger.
        correction_factor_spw17 = self._get_data(self.outfile, spw=17)
        average_factor_per_pol = correction_factor_spw17.mean(axis=2)
        # average derivative excluding edge channels
        delta = average_factor_per_pol[:, 1:] - average_factor_per_pol[:, :-1]
        average_delta = delta[:, 10:-10].mean()
        threshold = abs(average_delta) * 10
        for ipol in range(correction_factor_spw17.shape[0]):
            print(f'Examining pol {ipol}')
            edge_delta = np.abs(delta[ipol, [0, -1]])
            print('edge_delta', edge_delta)
            print(f'threshold = {threshold}')
            self.assertTrue(
                np.all(edge_delta < threshold),
                msg=f'Mitigation did not work for pol {ipol}'
            )


class ATMParamTest(unittest.TestCase):
    def _param_test_template(self, valid_test_cases,
                             invalid_user_input, user_default, task_default, unit=''):
        # internal error
        wrong_task_default = 'NG'
        with self.assertRaises(RuntimeError):
            param, is_customized = sdatmcor_impl.parse_atm_params(
                '',
                user_default,
                wrong_task_default
            )

        # invalid inputs
        with self.assertRaises(ValueError):
            param, is_customized = sdatmcor_impl.parse_atm_params(
                invalid_user_input,
                user_default,
                task_default
            )

        # valid inputs
        qa = quanta()
        for user_input, expected in itertools.chain(
                [(user_default, task_default)], valid_test_cases):
            casalog.post(f'user input: "{user_input}", expected: "{expected}"')
            param, is_customized = sdatmcor_impl.parse_atm_params(
                user_input,
                user_default,
                task_default,
                default_unit=unit
            )
            casalog.post(f'param "{param}", is_customized "{is_customized}"')
            self.assertEqual(is_customized, user_input != user_default)
            if qa.isquantity(expected):
                qparam = qa.quantity(param, unit)
                self.assertTrue(qa.compare(qparam, expected))
                self.assertTrue(qa.eq(qparam, expected))
            else:
                self.assertEqual(param, expected)

    def _list_param_test_template(self, valid_test_cases,
                                  invalid_user_input, user_default, task_default, unit):
        # internal error
        wrong_task_default = 'NG'
        with self.assertRaises(ValueError):
            param, is_customized = sdatmcor_impl.parse_atm_list_params(
                '0,0',
                user_default,
                task_default,
                wrong_task_default
            )

        # invalid inputs
        with self.assertRaises(ValueError):
            param, is_customized = sdatmcor_impl.parse_atm_list_params(
                invalid_user_input,
                user_default,
                task_default,
                unit
            )

        # valid inputs
        qa = quanta()
        for user_input, expected in itertools.chain(
                [(user_default, task_default)], valid_test_cases):
            casalog.post(f'user input: "{user_input}", expected: "{expected}"')
            param, is_customized = sdatmcor_impl.parse_atm_list_params(
                user_input,
                user_default,
                task_default,
                unit
            )
            self.assertEqual(is_customized, user_input != user_default)
            self.assertEqual(len(param), len(expected))
            for p, e in zip(param, expected):
                expected_in_unit = qa.convert(e, unit)['value']
                self.assertEqual(p, expected_in_unit)

    def test_h0(self):
        qa = quanta()
        task_default_cases = ['2km', qa.quantity(2, 'km')]
        user_default = ''
        test_cases = [
            (5.0, 5.0),
            ('5', 5.0),
            ('5km', 5.0),
            ('5000m', 5.0),
            ('500000cm', 5.0),
            ('5000000mm', 5.0),
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273K',
                user_default=user_default,
                task_default=task_default,
                unit='km'
            )

    def test_dtem_dh(self):
        qa = quanta()
        task_default_cases = ['-5.6K/km', qa.quantity(-5.6, 'K/km')]
        user_default = ''
        test_cases = [
            (-5, -5),
            ('-5', -5),
            ('-5K/km', -5),
            ('-0.005K/m', -5),
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='2km',
                user_default=user_default,
                task_default=task_default,
                unit='K/km'
            )

    def test_altitude(self):
        qa = quanta()
        task_default_cases = ['5000m', qa.quantity(5000, 'm')]
        user_default = ''
        test_cases = [
            (4800, 4800),
            ('4800', 4800),
            ('4800m', 4800),
            ('4.8km', 4800)
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273K',
                user_default=user_default,
                task_default=task_default,
                unit='m'
            )

    def test_temperature(self):
        qa = quanta()
        task_default_cases = ['273K', qa.quantity(273, 'K')]
        user_default = ''
        test_cases = [
            (300, 300),
            ('300', 300),
            ('300K', 300)
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273m',
                user_default=user_default,
                task_default=task_default,
                unit='K'
            )

    def test_pressure(self):
        qa = quanta()
        task_default_cases = ['1000mbar', qa.quantity(1000, 'mbar')]
        user_default = ''
        test_cases = [
            (1000, 1000),
            ('1000', 1000),
            ('1000mbar', 1000),
            ('1bar', 1000),
            ('1000hPa', 1000)
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273m',
                user_default=user_default,
                task_default=task_default,
                unit='mbar'
            )

    def test_humidity(self):
        qa = quanta()
        task_default_cases = ['20%', qa.quantity(20, '%')]
        user_default = -1
        test_cases = [
            (50, 50),
            ('50', 50),
            ('50%', 50)
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273K',
                user_default=user_default,
                task_default=task_default,
                unit='%'
            )

    def test_pwv(self):
        qa = quanta()
        task_default_cases = ['1mm', qa.quantity(1, 'mm')]
        user_default = ''
        test_cases = [
            (5, 5),
            ('5', 5),
            ('5mm', 5),
            ('0.5cm', 5),
            ('5e-3m', 5)
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273K',
                user_default=user_default,
                task_default=task_default,
                unit='mm'
            )

    def test_dp(self):
        qa = quanta()
        task_default_cases = ['10mbar', qa.quantity(10, 'mbar')]
        user_default = ''
        test_cases = [
            (10, 10),
            ('10', 10),
            ('10mbar', 10),
            ('0.01bar', 10),
            ('10hPa', 10)
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273m',
                user_default=user_default,
                task_default=task_default,
                unit='mbar'
            )

    def test_dpm(self):
        qa = quanta()
        task_default_cases = ['1.5', 1.5, qa.quantity(1.5, '')]
        user_default = -1
        test_cases = [
            (1.2, 1.2),
            ('1.2', 1.2),
        ]
        for task_default in task_default_cases:
            self._param_test_template(
                valid_test_cases=test_cases,
                invalid_user_input='273K',
                user_default=user_default,
                task_default=task_default,
                unit=''
            )

    def test_layerboundaries(self):
        task_default = []
        user_default = ''
        test_cases = [
            ([], []),
            ('', []),
            ([1500, 2000], [1500, 2000]),
            (['1500m', '2000m'], [1500, 2000]),
            ('1500,2000', [1500, 2000]),
            ('1500m,2000m', [1500, 2000]),
            ('1500m, 2000m', [1500, 2000]),
        ]
        self._list_param_test_template(
            valid_test_cases=test_cases,
            invalid_user_input='273K',
            user_default=user_default,
            task_default=task_default,
            unit='m'
        )

    def test_layertemperature(self):
        task_default = []
        user_default = ''
        test_cases = [
            ([], []),
            ('', []),
            ([270, 250], [270, 250]),
            (['270K', '250K'], [270, 250]),
            ('270,250', [270, 250]),
            ('270K,250K', [270, 250]),
            ('270K, 250K', [270, 250]),
        ]
        self._list_param_test_template(
            valid_test_cases=test_cases,
            invalid_user_input='273m',
            user_default=user_default,
            task_default=task_default,
            unit='K'
        )


if __name__ == '__main__':
    unittest.main()
