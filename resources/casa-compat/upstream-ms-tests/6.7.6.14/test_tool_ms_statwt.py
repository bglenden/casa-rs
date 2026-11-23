##########################################################################
# test_tool_ms_statwt.py
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
# https://casadocs.readthedocs.io/en/latest/api/tt/casatools.ms.html#casatools.ms.ms.statwt
#
#
##########################################################################

import os
import shutil
import unittest
import numpy as np
import numpy.ma as ma

subdir = 'unittest/statwt/'
from casatools import ctsys, table, ms
datadir = ctsys.resolve(subdir)
myms = ms()
mytb = table()

src = datadir + 'ngc5921_small.statwt.ms'
vlass = os.path.join(datadir, 'test_vlass_subset.ms')
if not os.path.exists(src):
    raise Exception('Cannot find ' + src)

# Place for reference data used in the tests
refdir = datadir + '/statwt_reference/'

# rows and target_row are the row numbers from the subtable formed
# by the baseline query
# In the chan_flags, a value of False means the channel is good (not flagged)
# so should be used. It follows the convention of the FLAGS column in the MS.

# EVEN IF THIS IS NO LONGER USED BY THE TESTS, IT SHOULDN'T BE DELETED BECAUSE
# IT IS USEFUL IN SANTIFY CHECKING NEW TESTS
def get_weights(
    data, flags, chan_flags, exposures, combine_corr, target_exposure, chanbins,
    target_flags, wtrange
):
    shape = data.shape
    ncorr_groups = 1 if combine_corr else shape[0]
    nchanbins = 1 if chanbins is None else len(chanbins)
    tchanbins = chanbins
    if nchanbins == 1:
        tchanbins = [[0, shape[1]]]
    ncorr = shape[0]
    weights = np.zeros([shape[0], shape[1]])
    wt = np.zeros(shape[0])
    nrows = data.shape[2]
    median_axis = 1 if ncorr_groups > 1 else None
    mod_flags = target_flags[:]
    if type(chan_flags) == type(None):
        myflags = flags[:]
    else:
        t_flags = np.expand_dims(np.expand_dims(chan_flags, 0), 2)
        myflags = np.logical_or(flags, t_flags)
    for corr in range(ncorr_groups):
        end_corr = corr + 1 if ncorr_groups > 1 else ncorr + 1
        for cb in tchanbins:
            var = variance(
                data[corr:end_corr, cb[0]:cb[1], :],
                myflags[corr:end_corr, cb[0]:cb[1], :], exposures
            )
            if flags[corr:end_corr, cb[0]:cb[1]].all():
                weights[corr:end_corr, cb[0]:cb[1]] = 0
                mod_flags[corr:end_corr, cb[0]:cb[1]] = True
            if var == 0:
                weights[corr:end_corr, cb[0]:cb[1]] = 0
                mod_flags[corr:end_corr, cb[0]:cb[1]] = True
            else:
                weights[corr:end_corr, cb[0]:cb[1]] = target_exposure/var
            if type(wtrange) != type(None):
                condition = np.logical_or(
                    np.less(
                        weights[corr:end_corr, cb[0]:cb[1]], wtrange[0]
                    ),
                    np.greater(
                        weights[corr:end_corr, cb[0]:cb[1]], wtrange[1]
                    )
                )
                exp_condition = np.expand_dims(condition, 2)
                weights[corr:end_corr, cb[0]:cb[1]] = np.where(
                    condition, 0, weights[corr:end_corr, cb[0]:cb[1]]
                )
                mod_flags[corr:end_corr, cb[0]:cb[1]] = np.where(
                    exp_condition, True, mod_flags[corr:end_corr, cb[0]:cb[1]]
                )
            mweights = ma.array(
                weights[corr:end_corr, :],
                mask=mod_flags[corr:end_corr, :]
            )
            wt[corr:end_corr] = np.median(mweights, median_axis)
    mod_flags = np.where(np.expand_dims(weights, 2) == 0, True, mod_flags)
    return (weights, wt, mod_flags)

def variance(data, flags, exposures):
    if flags.all():
        return 0
    expo = ma.masked_array(np.resize(exposures, data.shape), mask=flags)
    d = ma.array(data, mask=flags)
    myreal = np.real(d)
    myimag = np.imag(d)
    mean_r = np.sum(expo*myreal)/np.sum(expo)
    mean_i = np.sum(expo*myimag)/np.sum(expo)
    var_r = np.sum(expo * (myreal - mean_r)*(myreal - mean_r))/d.count()
    var_i = np.sum(expo * (myimag - mean_i)*(myimag - mean_i))/d.count()
    return (var_r + var_i)/2


def _get_dst_cols(dst, other="", dodata=True):
    mytb.open(dst)
    wt = mytb.getcol("WEIGHT")
    wtsp = mytb.getcol("WEIGHT_SPECTRUM")
    flag = mytb.getcol("FLAG")
    frow = mytb.getcol("FLAG_ROW")
    if dodata:
        data = mytb.getcol("CORRECTED_DATA")
    if len(other) > 0:
        othercol = mytb.getcol(other)
    mytb.close()
    cols = [wt, wtsp, flag, frow]
    if dodata:
        cols.append(data)
    if len(other) > 0:
        cols.append(othercol)
    return cols

def _get_table_cols(mytb):
    times = mytb.getcol("TIME")
    wt = mytb.getcol("WEIGHT")
    wtsp = None if mytb.colnames().count('WEIGHT_SPECTRUM') == 0 \
        else mytb.getcol("WEIGHT_SPECTRUM")
    flag = mytb.getcol("FLAG")
    frow = mytb.getcol("FLAG_ROW")
    data_col_name = 'CORRECTED_DATA' \
        if mytb.colnames().count('CORRECTED_DATA') > 0 else 'DATA'
    data = mytb.getcol(data_col_name)
    sigma = mytb.getcol("SIGMA")
    sisp = None if mytb.colnames().count('SIGMA_SPECTRUM') == 0 \
        else mytb.getcol("SIGMA_SPECTRUM")
    return [times, wt, wtsp, flag, frow, data, sigma, sisp]

class statwt_test(unittest.TestCase):

    def tearDown(self):
        myms.done()
        mytb.done()

    def _check_weights(
        self, msname, row_to_rows, data_column, chan_flags, combine_corr,
        chanbins, wtrange
    ):
        if data_column.startswith('c'):
            col_data = 'CORRECTED_DATA'
            check_sigma = False
        elif data_column.startswith('d'):
            col_data = 'DATA'
            check_sigma = True
        else:
            raise Exception("Unhandled column spec " + data_column)
        for ant1 in range(10):
            for ant2 in range((ant1 + 1), 10):
                query_str = 'ANTENNA1=' + str(ant1) + ' AND ANTENNA2=' \
                     + str(ant2)
                tb.open(msname)
                subt = tb.query(query_str)
                data = subt.getcol(col_data)
                flags = subt.getcol('FLAG')
                exposures = subt.getcol('EXPOSURE')
                wt = subt.getcol('WEIGHT')
                wtsp = subt.getcol('WEIGHT_SPECTRUM')
                flag_row = subt.getcol('FLAG_ROW')
                if check_sigma:
                    sigma = subt.getcol('SIGMA')
                    sisp = subt.getcol('SIGMA_SPECTRUM')
                subt.done()
                tb.done()
                nrows = data.shape[2]
                for row in range(nrows):
                    start = row_to_rows[row][0]
                    end = row_to_rows[row][1]
                    (weights, ewt, mod_flags) = get_weights(
                        data[:,:,start:end], flags[:, :, start:end], chan_flags,
                        exposures[start: end], combine_corr, exposures[row],
                        chanbins, flags[:, :, row:row+1], wtrange
                    )
                    self.assertTrue(
                        np.allclose(weights, wtsp[:, :, row]),
                        'Failed wtsp, got ' + str(wtsp[:, :, row])
                        + '\nexpected ' + str(weights) + '\nbaseline '
                        + str([ant1, ant2]) + '\nrow ' + str(row)
                    )
                    self.assertTrue(
                        np.allclose(ewt, wt[:, row]),
                        'Failed weight, got ' + str(wt[:, row])
                        + '\nexpected ' + str(np.median(weights, 1))
                        + '\nbaseline ' + str([ant1, ant2]) + '\nrow '
                        + str(row)
                    )
                    self.assertTrue(
                        (mod_flags == np.expand_dims(flags[:, :, row], 2)).all(),
                        'Failed flag, got ' + str(flags[:, :, row])
                        + '\nexpected ' + str(mod_flags) + '\nbaseline '
                        + str([ant1, ant2]) + '\nrow ' + str(row)
                    )
                    eflag_row = mod_flags.all()
                    self.assertTrue(
                        (eflag_row == flag_row[row]).all(),
                        'Failed flag_row, got ' + str(flag_row[row])
                        + '\nexpected ' + str(eflag_row) + '\nbaseline '
                        + str([ant1, ant2]) + '\nrow ' + str(row)
                    )
                    # all flags must be True where wtsp = 0
                    self.assertTrue(np.extract(weights == 0, mod_flags).all())
                    if check_sigma:
                        esigma = np.where(ewt == 0, -1, 1/np.sqrt(ewt))
                        self.assertTrue(
                            np.allclose(esigma, sigma[:, row]),
                            'Failed sigma, got ' + str(sigma[:, row])
                            + '\nexpected ' + str(esigma)
                            + '\nbaseline ' + str([ant1, ant2]) + '\nrow '
                            + str(row)
                        )
                        esisp = np.where(weights == 0, -1, 1/np.sqrt(weights))
                        self.assertTrue(
                            np.allclose(esisp, sisp[:, :, row]),
                            'Failed sigma_spectrum, got ' + str(sisp[:, :, row])
                            + '\nexpected ' + str(esisp)
                            + '\nbaseline ' + str([ant1, ant2]) + '\nrow '
                            + str(row)
                        )

    def compare(self, dst, ref):
        self.assertTrue(mytb.open(dst), "Table open failed for " + dst)
        [
            gtimes, gwt, gwtsp, gflag, gfrow, gdata, gsigma, gsisp
        ] = _get_table_cols(mytb)
        mytb.done()
        ref = os.path.join(refdir, ref)
        self.assertTrue(mytb.open(ref), "Table open failed for " + ref)
        [
            etimes, ewt, ewtsp, eflag, efrow, edata, esigma, esisp
        ] = _get_table_cols(mytb)
        mytb.done()
        self.assertTrue(
            np.allclose(gwt, ewt),
            f'WEIGHT comparison failed. max diff {np.max(np.abs(gwt - ewt))}'
        )
        if type(gwtsp) != type(None) and type(ewtsp) != type(None):
            self.assertTrue(
                np.allclose(gwtsp, ewtsp), 'WEIGHT_SPECTRUM comparison failed'
            )
        self.assertTrue((gflag == eflag).all(), 'FLAG comparison failed')
        self.assertTrue((gfrow == efrow).all(), 'FLAG_ROW comparison failed')
        # all flags must be True where wtsp = 0
        self.assertTrue(np.extract(gwtsp == 0, gflag).all())
        self.assertTrue(np.allclose(gsigma, esigma), 'SIGMA comparison failed')
        if type(gsisp) != type(None) and type(esisp) != type(None):
            self.assertTrue(np.allclose(
                gsisp, esisp), 'SIGMA_SPECTRUM comparison failed'
            )

    def test_algorithm(self):
        """ Test the algorithm, includes excludechans tests"""
        dst = "ngc5921.split.ms"
        cflags = np.array(63 * [False])
        cflags[10:21] = True
        """
        row_to_rows = []
        for row in range(60):
            row_to_rows.append((row, row+1))
        """
        for combine in ["", "corr"]:
            c = 0
            for fitspw in ["0:0~9;21~62", "", "0:10~20"]:
                shutil.copytree(src, dst)
                excludechans = c == 2
                myms.open(dst, nomodify=False)
                myms.statwt(
                    combine=combine, fitspw=fitspw,
                    excludechans=excludechans
                )
                myms.done()
                chan_flags = cflags if fitspw else None
                if combine == '':
                    if fitspw == '':
                        ref = 'ngc5921_statwt_ref_test_algorithm_sep_corr_no_fitspw.ms'
                    else:
                        ref = 'ngc5921_statwt_ref_test_algorithm_sep_corr_fitspw.ms'
                else:
                    if fitspw == '':
                        ref = 'ngc5921_statwt_ref_test_algorithm_combine_corr_no_fitspw.ms'
                    else:
                        ref = 'ngc5921_statwt_ref_test_algorithm_combine_corr_has_fitspw.ms'
                self.compare(dst, ref)
                shutil.rmtree(dst)
                c += 1

    def test_timebin(self):
        """Test time binning"""
        dst = "ngc5921.split.timebin.ms"
        combine = "corr"
        for timebin in ["300s", 10]:
            shutil.copytree(src, dst)
            myms.open(dst, nomodify=False)
            myms.statwt(timebin=timebin, combine=combine)
            myms.done()
            ref = 'ngc5921_statwt_ref_test_timebin_' + str(timebin) + '.ms'
            self.compare(dst, ref)
            shutil.rmtree(dst)

    def test_chanbin(self):
        """Test channel binning"""
        dst = "ngc5921.split.chanbin_0.ms"
        row_to_rows = []
        for i in range(60):
            row_to_rows.append([i, i+1])
        bins = [
            [0, 8], [8, 16], [16, 24], [24, 32], [32, 40], [40, 48],
            [48, 56], [56,63]
        ]
        for combine in ["", "corr"]:
            for i in [0, 2]:
                for chanbin in ["195.312kHz", 8]:
                    if i == 2 and combine != '' and chanbin != 8:
                        # only run the check for i == 2 once
                        continue
                    shutil.copytree(src, dst)
                    if i == 0:
                        myms.open(dst, nomodify=False)
                        myms.statwt(chanbin=chanbin, combine=combine)
                        myms.done()
                    elif i == 2:
                        # check WEIGHT_SPECTRUM is created, only check once,
                        # this test is long as it is
                        mytb.open(dst, nomodify=False)
                        x = mytb.ncols()
                        self.assertTrue(
                            mytb.removecols("WEIGHT_SPECTRUM"),
                            "column not removed"
                        )
                        y = mytb.ncols()
                        self.assertTrue(y == x-1, "wrong number of columns")
                        mytb.done()
                        myms.open(dst, nomodify=False)
                        myms.statwt(chanbin=chanbin, combine=combine)
                        myms.done()
                    if combine == '':
                        ref = refdir + 'ngc5921_statwt_ref_test_chanbin_sep_corr.ms'
                        ref = refdir + 'ngc5921_statwt_ref_test_chanbin_combine_corr.ms'
                    shutil.rmtree(dst)

    def test_minsamp(self):
        """Test minimum number of points"""
        dst = "ngc5921.split.minsamp.ms"
        combine = "corr"
        trow = 12
        for minsamp in [60, 80]:
            shutil.copytree(src, dst)
            myms.open(dst, nomodify=False)
            myms.statwt(minsamp=minsamp, combine=combine)
            myms.done()
            [wt, wtsp, flag, frow, data] = _get_dst_cols(dst)
            if minsamp == 60:
                self.assertTrue(
                    (wt[:, trow] > 0).all(), "Incorrect weight row " + str(trow)
                )
                self.assertTrue(
                    (wtsp[:, :, trow] > 0).all(),
                    "Incorrect weight spectrum row " + str(trow)
                )
                self.assertFalse(
                    flag[:,:,trow].all(), "Incorrect flag row " + str(trow)
                )
                self.assertFalse(
                    frow[trow], "Incorrect flagrow row " + str(trow)
                )
            else:
                self.assertTrue(
                    (wt[:, trow] == 0).all(),
                    "Incorrect weight row " + str(trow)
                )
                self.assertTrue(
                    (wtsp[:, :, trow] == 0).all(),
                    "Incorrect weight spectrum row " + str(trow)
                )
                self.assertTrue(
                    flag[:,:,trow].all(), "Incorrect flag row " + str(trow)
                )
                self.assertTrue(
                    frow[trow], "Incorrect flagrow row " + str(trow)
                )
            shutil.rmtree(dst)

    def test_default_boundaries(self):
        """Test default scan, field, etc boundaries"""
        dst = "ngc5921.split.normalbounds.ms"
        ref = 'ngc5921_statwt_ref_test_default_boundaries.ms'
        timebin = "6000s"
        # there are three field_ids, and there is a change in field_id when
        # there is a change in scan number, so specifying combine="field" in the
        # absence of "scan" will give the same result as combine=""
        row_to_rows = []
        for i in range(12):
            row_to_rows.append([0, 12])
        for i in range(12, 17):
            row_to_rows.append([12, 17])
        for i in range(17, 33):
            row_to_rows.append([17, 33])
        for i in range(33, 35):
            row_to_rows.append([33, 35])
        for i in range(35, 38):
            row_to_rows.append([35, 38])
        for i in range(38, 56):
            row_to_rows.append([38, 56])
        for i in range(56, 60):
            row_to_rows.append([56, 60])
        for combine in ["corr", "corr,field"]:
            shutil.copytree(src, dst)
            myms.open(dst, nomodify=False)
            myms.statwt(timebin=timebin, combine=combine)
            myms.done()
            self.compare(dst, ref)
            shutil.rmtree(dst)

    def test_no_scan_boundaries(self):
        """Test no scan boundaries"""
        dst = "ngc5921.no_scan_bounds.ms"
        timebin = "6000s"
        # ref = os.path.join(datadir, 'ngc5921_statwt_ref_test_no_scan_bounds.ms')
        ref = 'ngc5921_statwt_ref_test_no_scan_bounds.ms'
        combine = "corr, scan"
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        myms.statwt(timebin=timebin, combine=combine)
        myms.done()
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_no_scan_nor_field_boundaries(self):
        """Test no scan nor field boundaries"""
        dst = "ngc5921.no_scan_nor_field_bounds.ms"
        timebin = "6000s"
        # ref = os.path.join(datadir, 'ngc5921_statwt_ref_test_no_scan_nor_field_bounds.ms')
        ref = 'ngc5921_statwt_ref_test_no_scan_nor_field_bounds.ms'
        for combine in ["corr,scan,field", "corr,field,scan"]:
            shutil.copytree(src, dst)
            myms.open(dst, nomodify=False)
            myms.statwt(timebin=timebin, combine=combine)
            myms.done()
            self.compare(dst, ref)
            shutil.rmtree(dst)

    def test_statalg(self):
        """Test statalg"""
        # just testing inputs
        dst = "ngc5921.split.statalg.ms"
        for statalg in ["cl", "ch", "h", "f", "bogus"]:
            shutil.copytree(src, dst)
            myms.open(dst, nomodify=False)
            if statalg == "cl":
                self.assertTrue(myms.statwt(statalg=statalg))
            elif statalg == "ch":
                self.assertTrue(
                    myms.statwt(statalg=statalg, zscore=5, maxiter=3)
                )
            elif statalg == "h":
                self.assertTrue(myms.statwt(statalg=statalg, fence=0.2))
            elif statalg == "f":
                self.assertTrue(
                    myms.statwt(statalg=statalg, center="median", lside=False)
                )
            elif statalg == "bogus":
                self.assertRaises(Exception, myms.statwt, statalg=statalg)
            myms.done()
            shutil.rmtree(dst)

    def test_wtrange(self):
        """Test weight range"""
        dst = "ngc5921.split.timebin.ms"
        ref = "ngc5921_statwt_ref_test_wtrange_300s.ms"
        combine = "corr"
        timebin = "300s"
        wtrange = [1, 2]
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        myms.statwt(timebin=timebin, combine=combine, wtrange=wtrange)
        myms.done()
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_preview(self):
        """Test preview mode"""
        dst = "ngc5921.split.preview.ms"
        [refwt, refwtsp, refflag, reffrow, refdata] = _get_dst_cols(src)
        combine = "corr"
        timebin = "300s"
        wtrange = [1, 2]
        preview = True
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        myms.statwt(
            timebin=timebin, combine=combine, wtrange=wtrange, preview=preview
        )
        myms.done()
        [tstwt, tstwtsp, tstflag, tstfrow, tstdata] = _get_dst_cols(dst)
        self.assertTrue(np.all(tstflag == refflag), "FLAGs don't match")
        self.assertTrue(np.all(tstfrow == reffrow), "FLAG_ROWs don't match")
        self.assertTrue(
            np.all(np.isclose(tstwt, refwt)), "WEIGHTs don't match"
        )
        self.assertTrue(
            np.all(np.isclose(tstwtsp, refwtsp)), "WEIGHT_SPECTRUMs don't match"
        )
        shutil.rmtree(dst)

    def test_data_col(self):
        """Test using data column"""
        dst = "ngc5921.split.data.ms"
        ref = 'ngc5921_statwt_ref_test_data_col.ms'
        combine = "corr"
        timebin = 1
        data = "data"
        """
        row_to_rows = []
        for i in range(60):
            row_to_rows.append([i, i+1])
        """
        shutil.copytree(src, dst)
        self.assertTrue(mytb.open(dst, nomodify=False))
        self.assertTrue(mytb.removecols("DATA"))
        self.assertTrue(mytb.renamecol("CORRECTED_DATA", "DATA"))
        mytb.done()
        myms.open(dst, nomodify=False)
        myms.statwt(timebin=timebin, combine=combine, datacolumn=data)
        myms.done()
        # self._check_weights(dst, row_to_rows, 'd', None, True, None, None)
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_sliding_time_window(self):
        """Test sliding time window"""
        dst = "ngc5921.split.sliding_time_window.ms"
        ref = 'ngc5921_statwt_ref_test_sliding_time_window.ms'
        timebin = "300s"
        """
        row_to_rows = []
        row_to_rows.append([0, 6])
        row_to_rows.append([0, 7])
        row_to_rows.append([0, 8])
        row_to_rows.append([0, 9])
        row_to_rows.append([0, 9])
        row_to_rows.append([0, 10])
        row_to_rows.append([1, 12])
        row_to_rows.append([2, 12])
        row_to_rows.append([3, 12])
        row_to_rows.append([5, 12])
        row_to_rows.append([6, 12])
        row_to_rows.append([6, 12])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([17, 20])
        row_to_rows.append([17, 21])
        row_to_rows.append([17, 22])
        row_to_rows.append([18, 23])
        row_to_rows.append([19, 24])
        row_to_rows.append([20, 25])
        row_to_rows.append([21, 26])
        row_to_rows.append([22, 27])
        row_to_rows.append([23, 28])
        row_to_rows.append([24, 29])
        row_to_rows.append([25, 30])
        row_to_rows.append([26, 31])
        row_to_rows.append([27, 32])
        row_to_rows.append([28, 33])
        row_to_rows.append([29, 33])
        row_to_rows.append([30, 33])
        row_to_rows.append([33, 35])
        row_to_rows.append([33, 35])
        row_to_rows.append([35, 38])
        row_to_rows.append([35, 38])
        row_to_rows.append([35, 38])
        row_to_rows.append([38, 41])
        row_to_rows.append([38, 42])
        row_to_rows.append([38, 43])
        row_to_rows.append([39, 44])
        row_to_rows.append([40, 45])
        row_to_rows.append([41, 46])
        row_to_rows.append([42, 47])
        row_to_rows.append([43, 48])
        row_to_rows.append([44, 49])
        row_to_rows.append([45, 50])
        row_to_rows.append([46, 51])
        row_to_rows.append([47, 52])
        row_to_rows.append([48, 53])
        row_to_rows.append([49, 54])
        row_to_rows.append([50, 55])
        row_to_rows.append([51, 56])
        row_to_rows.append([52, 56])
        row_to_rows.append([53, 56])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        """
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        myms.statwt(timebin=timebin, slidetimebin=True)
        myms.done()
        # self._check_weights(
        #    dst, row_to_rows, 'c', None, False, None, None
        # )
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_sliding_window_timebin_int(self):
        """Test sliding window with timebin as int specified"""
        dst = "ngc5921.split.sliding_time_window.ms"
        # row_to_rows = []
        """
        # odd int, timebin = 5
        row_to_rows.append([0, 5])
        row_to_rows.append([0, 5])
        row_to_rows.append([0, 5])
        row_to_rows.append([1, 6])
        row_to_rows.append([2, 7])
        row_to_rows.append([3, 8])
        row_to_rows.append([4, 9])
        row_to_rows.append([5, 10])
        row_to_rows.append([6, 11])
        row_to_rows.append([7, 12])
        row_to_rows.append([7, 12])
        row_to_rows.append([7, 12])

        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])

        row_to_rows.append([17, 22])
        row_to_rows.append([17, 22])
        row_to_rows.append([17, 22])
        row_to_rows.append([18, 23])
        row_to_rows.append([19, 24])
        row_to_rows.append([20, 25])
        row_to_rows.append([21, 26])
        row_to_rows.append([22, 27])
        row_to_rows.append([23, 28])
        row_to_rows.append([24, 29])
        row_to_rows.append([25, 30])
        row_to_rows.append([26, 31])
        row_to_rows.append([27, 32])
        row_to_rows.append([28, 33])
        row_to_rows.append([28, 33])
        row_to_rows.append([28, 33])

        row_to_rows.append([33, 35])
        row_to_rows.append([33, 35])

        row_to_rows.append([35, 38])
        row_to_rows.append([35, 38])
        row_to_rows.append([35, 38])

        row_to_rows.append([38, 43])
        row_to_rows.append([38, 43])
        row_to_rows.append([38, 43])
        row_to_rows.append([39, 44])
        row_to_rows.append([40, 45])
        row_to_rows.append([41, 46])
        row_to_rows.append([42, 47])
        row_to_rows.append([43, 48])
        row_to_rows.append([44, 49])
        row_to_rows.append([45, 50])
        row_to_rows.append([46, 51])
        row_to_rows.append([47, 52])
        row_to_rows.append([48, 53])
        row_to_rows.append([49, 54])
        row_to_rows.append([50, 55])
        row_to_rows.append([51, 56])
        row_to_rows.append([51, 56])
        row_to_rows.append([51, 56])

        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        """
        """
        # even timebin = 6
        row_to_rows.append([0, 6])
        row_to_rows.append([0, 6])
        row_to_rows.append([0, 6])
        row_to_rows.append([1, 7])
        row_to_rows.append([2, 8])
        row_to_rows.append([3, 9])
        row_to_rows.append([4, 10])
        row_to_rows.append([5, 11])
        row_to_rows.append([6, 12])
        row_to_rows.append([6, 12])
        row_to_rows.append([6, 12])
        row_to_rows.append([6, 12])

        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])
        row_to_rows.append([12, 17])

        row_to_rows.append([17, 23])
        row_to_rows.append([17, 23])
        row_to_rows.append([17, 23])
        row_to_rows.append([18, 24])
        row_to_rows.append([19, 25])
        row_to_rows.append([20, 26])
        row_to_rows.append([21, 27])
        row_to_rows.append([22, 28])
        row_to_rows.append([23, 29])
        row_to_rows.append([24, 30])
        row_to_rows.append([25, 31])
        row_to_rows.append([26, 32])
        row_to_rows.append([27, 33])
        row_to_rows.append([27, 33])
        row_to_rows.append([27, 33])
        row_to_rows.append([27, 33])

        row_to_rows.append([33, 35])
        row_to_rows.append([33, 35])

        row_to_rows.append([35, 38])
        row_to_rows.append([35, 38])
        row_to_rows.append([35, 38])

        row_to_rows.append([38, 44])
        row_to_rows.append([38, 44])
        row_to_rows.append([38, 44])
        row_to_rows.append([39, 45])
        row_to_rows.append([40, 46])
        row_to_rows.append([41, 47])
        row_to_rows.append([42, 48])
        row_to_rows.append([43, 49])
        row_to_rows.append([44, 50])
        row_to_rows.append([45, 51])
        row_to_rows.append([46, 52])
        row_to_rows.append([47, 53])
        row_to_rows.append([48, 54])
        row_to_rows.append([49, 55])
        row_to_rows.append([50, 56])
        row_to_rows.append([50, 56])
        row_to_rows.append([50, 56])
        row_to_rows.append([50, 56])

        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        row_to_rows.append([56, 60])
        """

        for timebin in [5, 6]:
            ref = 'ngc5921_statwt_ref_test_sliding_time_window_' + str(timebin) + '.ms'
            shutil.copytree(src, dst)
            myms.open(dst, nomodify=False)
            myms.statwt(timebin=timebin, slidetimebin=True)
            myms.done()
            #self._check_weights(
            #    dst, row_to_rows, 'c', None, False, None, None
            #)
            self.compare(dst, ref)
            shutil.rmtree(dst)

    def test_residual(self):
        """Test using corrected_data - model_data column"""
        dst = "ngc5921.split.residualwmodel.ms"
        ref = 'ngc5921_statwt_ref_test_residual.ms'
        data = "residual"
        # row_to_rows = []
        # for i in range(60):
        #    row_to_rows.append([i, i+1])
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        myms.statwt(datacolumn=data)
        myms.done()
        # self._check_weights(
        #    dst, row_to_rows, data, None, False, None, None
        # )
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_residual_no_model(self):
        """Test datacolumn='residual' in the absence of a MODEL_DATA column"""
        dst = "ngc5921.split.residualwoutmodel.ms"
        ref = 'ngc5921_statwt_ref_test_residual_no_model.ms'
        data = "residual"
        shutil.copytree(src, dst)
        self.assertTrue(mytb.open(dst, nomodify=False))
        self.assertTrue(mytb.removecols("MODEL_DATA"))
        mytb.done()
        myms.open(dst, nomodify=False)
        myms.statwt(datacolumn=data)
        myms.done()
        # self._check_weights(
        #    dst, row_to_rows, data, None, False, None, None
        # )
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_residual_data(self):
        """Test using data - model_data column"""
        dst = "ngc5921.split.residualdatawmodel.ms"
        ref = 'ngc5921_statwt_ref_test_residual_data.ms'
        data = "residual_data"
        # row_to_rows = []
        # for i in range(60):
        #     row_to_rows.append([i, i+1])
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        myms.statwt(datacolumn=data)
        myms.done()
        # self._check_weights(
        #    dst, row_to_rows, data, None, False, None, None
        # )
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_residual_data_no_model(self):
        """Test using residual data in absence of MODEL_DATA"""
        dst = "ngc5921.split.residualdatawoutmodel.ms"
        ref = 'ngc5921_statwt_ref_test_residual_data_no_model.ms'
        data = "residual_data"
        # row_to_rows = []
        # for i in range(60):
        #     row_to_rows.append([i, i+1])
        shutil.copytree(src, dst)
        self.assertTrue(mytb.open(
            dst, nomodify=False), "unable to open table " + dst
        )
        self.assertTrue(
            mytb.removecols("MODEL_DATA"), "unable to remove MODEL_DATA column"
        )
        mytb.done()
        myms.open(dst, nomodify=False)
        myms.statwt(datacolumn=data)
        myms.done()
        # self._check_weights(
        #     dst, row_to_rows, data, None, False, None, None
        # )
        self.compare(dst, ref)
        shutil.rmtree(dst)

    def test_returned_stats(self):
        """Test returned stats, CAS-10881"""
        dst = "ngc5921.split.statstest.ms"
        shutil.copytree(src, dst)
        myms.open(dst, nomodify=False)
        res = myms.statwt()
        myms.done()
        self.assertTrue(
            np.isclose(res['mean'], 3.691224144843796),
            "mean is incorrect"
        )
        self.assertTrue(
            np.isclose(res['variance'], 6.860972180192186),
            "variance is incorrect"
        )
        shutil.rmtree(dst)

    def test_multi_spw_no_spectrum_columns(self):
        "Test multi spw with no sigma nor weight spectrum columns works"
        for tb in [1, "5s"]:
            dst = "statwt_test_vlass_timebin" + str(tb) + ".ms"
            shutil.copytree(vlass, dst)
            myms.open(dst, nomodify=False)
            res = myms.statwt(
                combine='scan,field,state', timebin=tb,
                datacolumn='residual_data'
            )
            myms.done()
            ref = 'test_vlass_timebin' + str(tb) + '.ms'
            self.compare(dst, ref)
            shutil.rmtree(dst)

if __name__ == '__main__':
    unittest.main()
