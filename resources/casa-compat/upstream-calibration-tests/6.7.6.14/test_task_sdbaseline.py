########################################################################
# test_task_sdbaseline.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.single.sdbaseline.html
#
#
##########################################################################
import contextlib
import copy
import csv
import filecmp
import glob
import os
import re
import shutil
import unittest
from io import StringIO

import numpy as np
from casatasks import casalog, sdbaseline
from casatasks.private.sdutil import table_manager
from casatasks.private.task_sdbaseline import (check_fftthresh, is_empty,
                                               parse_wavenumber_param)
from casatools import ctsys, table

tb = table()
ctsys_resolve = ctsys.resolve


# Utilities for reading blparam file
class FileReader(object):
    def __init__(self, filename):
        self.__filename = filename
        self.__data = None
        self.__nline = None

    def read(self):
        if self.__data is None:
            f = open(self.__filename, 'r')
            self.__data = f.readlines()
            f.close()
            self.__nline = len(self.__data)
        return

    def nline(self):
        self.read()
        return self.__nline

    def index(self, txt, start):
        return self.__data[start:].index(txt) + 1 + start

    def getline(self, idx):
        return self.__data[idx]


class BlparamFileParser(FileReader):
    def __init__(self, blfile):
        FileReader.__init__(self, blfile)
        self.__nrow = None
        self.__coeff = None
        self.__rms = None
        self.__ctxt = 'Baseline parameters\n'
        self.__rtxt = 'Results of baseline fit\n'

    def nrow(self):
        self.read()
        if self.__nrow is None:
            return self._nrow()
        else:
            return self.__nrow

    def coeff(self):
        self.read()
        if self.__coeff is None:
            self.parseCoeff()
        return self.__coeff

    def rms(self):
        self.read()
        if self.__rms is None:
            self.parseRms()
        return self.__rms

    def _nrow(self):
        self.__nrow = 0
        for i in range(self.nline()):
            if self.getline(i) == self.__ctxt:
                self.__nrow += 1
        return self.__nrow

    def parse(self):
        self.read()
        self.parseCoeff()
        self.parseRms()
        return

    def parseCoeff(self):
        self.__coeff = []
        nrow = self.nrow()
        idx = 0
        while (len(self.__coeff) < nrow):
            idx = self.index(self.__ctxt, idx)
            coeffs = []
            while (self.getline(idx) != self.__rtxt):
                coeff = self.__parseCoeff(idx)
                coeffs += coeff
                idx += 1
            self.__coeff.append(coeffs)
        return

    def parseRms(self):
        self.__rms = []
        nrow = self.nrow()
        idx = 0
        while (len(self.__rms) < nrow):
            idx = self.index(self.__rtxt, idx)
            self.__rms.append(self.__parseRms(idx))
        return

    def __parseCoeff(self, idx):
        return parseCoeff(self.getline(idx))

    def __parseRms(self, idx):
        return parseRms(self.getline(idx))


def parseCoeff(txt):
    clist = txt.rstrip('\n').split(',')
    return [float(c.split('=')[1]) for c in clist]


def parseRms(txt):
    t = txt.lstrip().rstrip('\n')[6:]
    return float(t)


def remove_single_file_dir(filename):
    """
    Remove a single file or a single directory.
    For filename, '.' and those end with '..' (namely, '..', '../..' etc.)
    are not allowed.
    """
    if filename == '.' or filename[-2:] == '..':
        raise Exception("Caution! Attempting to remove '" + filename + "'!!")

    if os.path.exists(filename):
        if os.path.isdir(filename):
            shutil.rmtree(filename)
        else:  # file or symlink
            os.remove(filename)


def remove_files_dirs(filename):
    """
    Remove files/directories/symlinks 'filename*'.
    For filename, '', '.' and those end with '..' (namely, '..', '../..' etc.)
    are not allowed.
    """
    if filename == '.' or filename[-2:] == '..':
        raise Exception("Caution! Attempting to remove '" + filename + "*'!!")
    elif filename == '':
        raise Exception("The parameter 'filename' must not be a null string.")

    filenames = glob.glob('{}*'.format(filename.rstrip('/')))

    for filename in filenames:
        remove_single_file_dir(filename)


def compare_dir(dir1, dir2):
    # Write dircmp() result to a text file, then read the result as a list
    compare_result = 'compare_' + dir1 + '_' + dir2
    with open(compare_result, 'w') as o:
        with contextlib.redirect_stdout(o):
            filecmp.dircmp(dir1, dir2, ignore=['table.lock']).report_full_closure()
    with open(compare_result) as f:
        lines = f.readlines()
    remove_single_file_dir(compare_result)

    # Added/deleted files
    n_only = _get_num_files_by_keyword(lines, 'Only in ')
    # Modified files
    n_diff = _get_num_files_by_keyword(lines, 'Differing ')

    mesg = ''
    if n_only > 0:
        mesg += '{} added or deleted'.format(n_only)
    if n_diff > 0:
        if n_only > 0:
            mesg += ', '
        mesg += '{} modified'.format(n_diff)

    return mesg


def _get_num_files_by_keyword(cmpresult, keyword):
    return len(sum([eval(s[s.index(':') + 1:]) for s in cmpresult if s.startswith(keyword)], []))


class sdbaseline_unittest_base(unittest.TestCase):
    """
    Base class for sdbaseline unit test
    """
    # Data path of input/output
    datapath = ctsys_resolve('unittest/sdbaseline/')
    taskname = "sdbaseline"
    verboselog = False

    # complist = ['max','min','rms','median','stddev']

    blparam_order = ['row', 'pol', 'mask', 'nclip', 'cthre',
                     'uself', 'lthre', 'ledge', 'redge', 'chavg',
                     'btype', 'order', 'npiec', 'nwave']
    blparam_dic = {}
    blparam_dic['row'] = [0, 0, 1, 1, 2, 2, 3, 3]
    blparam_dic['pol'] = [0, 1, 0, 1, 0, 1, 0, 1]
    # blparam_dic['mask']  = ['0~4000;6000~8000']*3 + ['']*5
    blparam_dic['mask'] = ['500~2500;5000~7500'] * 8
    blparam_dic['nclip'] = [0] * 8
    blparam_dic['cthre'] = ['3.'] * 8
    blparam_dic['uself'] = ['false'] * 4 + ['true'] + ['false'] * 3
    blparam_dic['lthre'] = ['0.'] * 4 + ['3.', '', '', '0.']
    blparam_dic['ledge'] = [0] * 4 + [10, 50, '', 0]
    blparam_dic['redge'] = [0] * 4 + [10, 50, '', 0]
    blparam_dic['chavg'] = [0] * 4 + [4, '', '', 0]
    blparam_dic['btype'] = ['poly'] + ['chebyshev'] * 2\
        + ['poly', 'chebyshev', 'poly'] + ['cspline'] * 2
    blparam_dic['order'] = [0, 0, 1, 1, 2, 2, '', '']
    blparam_dic['npiec'] = [0] * 6 + [1] * 2
    blparam_dic['nwave'] = [[]] * 3 + [''] * 2 + [[]] * 3

    # helper functions for tests
    def _createBlparamFile(self, file, param_order, val, option=''):
        nspec = 8
        f = open(file, 'w')
        assert(len(param_order) == len(val.keys()))
        for key in val.keys():
            assert(len(val[key]) == nspec)
        for i in range(nspec):
            do_write = True
            s = ''
            for key in param_order:
                v = val[key][i]
                if key == 'nwave':
                    if v != '':
                        s += ','
                        s += str(v)
                else:
                    s += str(v)
                    if key != 'npiec':
                        s += ','
            s += '\n'
            if (option == 'r2p1less') and (val['row'][i] == 2) and (val['pol'][i] == 1):
                do_write = False
            if (option == 'r2p1cout') and (val['row'][i] == 2) and (val['pol'][i] == 1):
                s = '#' + s
            if do_write:
                f.write(s)
        f.close()

    def _checkfile(self, name, fail=True):
        """
        Check if the file exists.
        name : the path and file name to test
        fail : if True, Error if the file does not exists.
               if False, return if the file exists
        """
        isthere = os.path.exists(name)
        if fail:
            self.assertTrue(isthere, msg='Could not find, %s' % (name))
        else:
            return isthere

    def _remove(self, names):
        """
        Remove a list of files and directories from disk
        """
        for name in names:
            remove_single_file_dir(name)

    def _copy(self, names, from_dir=None, dest_dir=None):
        """
        Copy a list of files and directories from a directory (from_dir) to
        another (dest_dir) in the same name.

        names : a list of files and directories to copy
        from_dir : a path to directory from which search and copy files
                   and directories (the default is the current path)
        to_dir   : a path to directory to which copy files and directories
                   (the default is the current path)
        NOTE: it is not allowed to specify
        """
        # Check for paths
        if from_dir is None and dest_dir is None:
            raise ValueError("Can not copy files to exactly the same path.")
        from_path = os.path.abspath("." if from_dir is None else from_dir.rstrip("/"))
        to_path = os.path.abspath("." if dest_dir is None else dest_dir.rstrip("/"))
        if from_path == to_path:
            raise ValueError("Can not copy files to exactly the same path.")
        # Copy a list of files and directories
        for name in names:
            from_name = from_path + "/" + name
            to_name = to_path + "/" + name
            if os.path.exists(from_name):
                if os.path.isdir(from_name):
                    shutil.copytree(from_name, to_name)
                else:
                    shutil.copyfile(from_name, to_name)
                if self.verboselog:
                    casalog.post("Copying '%s' FROM %s TO %s" % (name, from_path, to_path))
            else:
                casalog.post("Could not find '%s'...skipping copy" % from_name, 'WARN')

    def _getUniqList(self, val):
        """Accepts a python list and returns a list of unique values"""
        if not isinstance(val, list):
            raise Exception('_getUniqList: input value must be a list.')
        return list(set(val))

    def _getListSelection(self, val):
        """
        Converts input to a list of unique integers
        Input: Either comma separated string of IDs, an integer, or a list of values.
        Output: a list of unique integers in input arguments for string and integer input.
                In case the input is a list of values, output will be a list of unique values.
        """
        if isinstance(val, str):
            val_split = val.split(',')
            val_sel = [int(val_split[j]) for j in range(len(val_split))]
        elif isinstance(val, int):
            val_sel = [val]
        elif isinstance(val, list) or isinstance(val, tuple):
            val_sel = val.copy()
        else:
            raise Exception('_getListSelection: wrong value ' + str(val) + ' for selection.')
        return self._getUniqList(val_sel)

    def _getListSelectedRowID(self, data_list, sel_list):
        """
        Returns IDs of data_list that contains values equal to one in
        sel_list.
        The function is used to get row IDs that corresponds to a
        selected IDs. In that use case, data_list is typically a list
        of values in a column of an MS (e.g., SCAN_NUMBER) and sel_list is
        a list of selected (scan) IDs.

        data_list : a list to test and get IDs from
        sel_list  : a list of values to look for existance in data_list
        """
        res = [i for i in range(len(data_list)) if data_list[i] in sel_list]
        return self._getUniqList(res)

    def _getEffective(self, spec, mask):
        """
        Returns an array made by selected elements in spec array.
        Only the elements in the ID range in mask are returned.

        spec : a data array
        mask : a mask list of the channel ranges to use. The format is
               [[start_idx0, end_idx0], [start_idx1, end_idx1], ...]
        """
        res = [spec[j] for i in range(len(mask)) for j in range(mask[i][0], mask[i][1])]
        return np.array(res)

    def _getStats(self, filename=None, spw=None, pol=None, colname=None, mask=None):
        """
        Returns a list of statistics dictionary of selected rows in an MS.

        filename : the name of MS
        spw      : spw ID selection (default: all spws in MS)
        pol      : pol ID selection (default: all pols in MS)
        colname  : the name of data column (default: 'FLOAT_DATA')
        mask     : a mask list of the channel ranges to use. The format is
                   [[start_idx0, end_idx0], [start_idx1, end_idx1], ...]

        The order of output list is in the ascending order of selected row IDs.
        The dictionary in output list has keys:
        'row' (row ID in MS), 'pol' (pol ID), 'rms', 'min', 'max', 'median',
        and 'stddev'
        """
        # Get selected row and pol IDs in MS. Also get spectrumn in the MS
        if not spw:
            spw = ''
        select_spw = (spw not in ['', '*'])
        if select_spw:
            spw_sel = self._getListSelection(spw)
        if not pol:
            pol = ''
        select_pol = (pol not in ['', '*'])
        if select_pol:
            pol_sel = self._getListSelection(pol)
        if not colname:
            colname = 'FLOAT_DATA'
        self._checkfile(filename)
        with table_manager(filename) as tb:
            data = tb.getcol(colname)
            ddid = tb.getcol('DATA_DESC_ID')
        with table_manager(filename + '/DATA_DESCRIPTION') as tb:
            spwid = tb.getcol('SPECTRAL_WINDOW_ID').tolist()
        if not select_spw:
            spw_sel = spwid
        # get the selected DD IDs from selected SPW IDs.
        dd_sel = self._getListSelectedRowID(spwid, spw_sel)
        # get the selected row IDs from selected DD IDs
        row_sel = self._getListSelectedRowID(ddid, dd_sel)
        if not select_spw:
            row_sel = range(len(ddid))
        if not select_pol:
            pol_sel = range(len(data))

        res = []
        for irow in row_sel:
            for ipol in pol_sel:
                spec = data[ipol, :, irow]
                res_elem = self._calc_stats_of_array(spec, mask=mask)
                res_elem['row'] = irow
                res_elem['pol'] = ipol

                res.append(res_elem)

        return res

    def _calc_stats_of_array(self, data, mask=None):
        """
        """
        if mask is not None:
            spec = self._getEffective(data, mask)
        else:
            spec = np.array(data)
        res_elem = {}
        res_elem['rms'] = np.sqrt(np.var(spec))
        res_elem['min'] = np.min(spec)
        res_elem['max'] = np.max(spec)
        # res_elem['mean'] = np.mean(spec)
        res_elem['median'] = np.median(spec)
        res_elem['stddev'] = np.std(spec)
        return res_elem

    def _convert_statslist_to_dict(self, stat_list):
        """
        Returns a disctionary of statistics of selected rows in an MS.

        stat_list: a list of stats dictionary (e.g., return value of _getStats)

        The output dictionary is in form:
        {'max': [max0, max1, max2, ...], 'min': [min0, min1,...], ...}
        The order of elements are in ascending order of row and pol IDs pair, i.e.,
        (row0, pol0), (row0, pol1), (row1, pol0), ....
        """
        # if len(stat_list) == 0:
        #     raise Exception, "No row selected in MS"
        stat_dict = {key: [stat[key] for stat in stat_list] for key in stat_list[0].keys()}
        return stat_dict

    def _compareStats(self, currstat, refstat, rtol=1.0e-2, atol=1.0e-5, complist=None):
        """
        Compare statistics results (dictionaries) and test if the values are within
        an allowed tolerance.

        currstat : the statistic values to test (either an MS name or
                   a dictionary)
        refstat  : the reference statistics values (a dictionary)
        rtol   : tolerance of relative difference
        atol   : tolerance of absolute difference
        complist : statistics to compare (default: keys in refstat)
        """
        # test if the statistics of baselined spectra are equal to
        # the reference values
        printstat = False  # True
        # In case currstat is filename
        if isinstance(currstat, str) and os.path.exists(currstat):
            # print "calculating statistics from '%s'" % currstat
            currstat = self._getStats(currstat)

        self.assertTrue(isinstance(currstat, dict) and isinstance(refstat, dict),
                        "Need to specify two dictionaries to compare")
        if complist:
            keylist = complist
        else:
            keylist = refstat.keys()
            # keylist = self.complist

        for key in keylist:
            self.assertTrue(key in currstat,
                            msg="%s is not defined in the current results." % key)
            self.assertTrue(key in refstat,
                            msg="%s is not defined in the reference data." % key)
            refval = refstat[key]
            currval = currstat[key]
            # Quantum values
            if isinstance(refval, dict):
                if 'unit' in refval and 'unit' in currval:
                    if printstat:
                        print("Comparing unit of '%s': %s (current run), %s (reference)" %
                              (key, currval['unit'], refval['unit']))
                    self.assertEqual(refval['unit'], currval['unit'],
                                     "The units of '%s' differs: %s (expected: %s)" %
                                     (key, currval['unit'], refval['unit']))
                    refval = refval['value']
                    currval = currval['value']
                else:
                    raise Exception("Invalid quantum values. %s (current run) %s (reference)" %
                                    (str(currval), str(refval)))
            currval = self._to_list(currval)
            refval = self._to_list(refval)
            if printstat:
                print("Comparing '%s': %s (current run), %s (reference)" %
                      (key, str(currval), str(refval)))
            self.assertTrue(len(currval) == len(refval),
                            "Number of elemnets in '%s' differs." % key)
            if isinstance(refval[0], str):
                for i in range(len(currval)):
                    if isinstance(refval[i], str):
                        self.assertTrue(currval[i] == refval[i],
                                        msg="%s[%d] differs: %s (expected: %s) " %
                                        (key, i, str(currval[i]), str(refval[i])))
            else:
                # np.allclose handles almost zero case more properly.
                self.assertTrue(np.allclose(currval, refval, rtol=rtol, atol=atol),
                                msg="%s differs: %s" % (key, str(currval)))
            del currval, refval

    def _to_list(self, input):
        """
        Convert input to a list
        If input is None, this method simply returns None.
        """
        listtypes = (list, tuple, np.ndarray)
        if input is None:
            return None
        elif type(input) in listtypes:
            return list(input)
        else:
            return [input]

    def _compareBLparam(self, out, reference):
        # test if baseline parameters are equal to the reference values
        # currently comparing every lines in the files
        # TO DO: compare only "Fitter range" and "Baseline parameters"
        self._checkfile(out)
        self._checkfile(reference)

        blparse_out = BlparamFileParser(out)
        blparse_out.parse()
        coeffs_out = blparse_out.coeff()
        rms_out = blparse_out.rms()
        blparse_ref = BlparamFileParser(reference)
        blparse_ref.parse()
        coeffs_ref = blparse_ref.coeff()
        rms_ref = blparse_ref.rms()
        allowdiff = 0.01
        print('Check baseline parameters:')
        for irow in range(len(rms_out)):
            print('Row %s:' % (irow))
            print('   Reference rms  = %s' % (rms_ref[irow]))
            print('   Calculated rms = %s' % (rms_out[irow]))
            print('   Reference coeffs  = %s' % (coeffs_ref[irow]))
            print('   Calculated coeffs = %s' % (coeffs_out[irow]))
            r0 = rms_ref[irow]
            r1 = rms_out[irow]
            rdiff = (r1 - r0) / r0
            self.assertTrue((abs(rdiff) < allowdiff),
                            msg='row %s: rms is different' % (irow))
            c0 = coeffs_ref[irow]
            c1 = coeffs_out[irow]
            for ic in range(len(c1)):
                rdiff = (c1[ic] - c0[ic]) / c0[ic]
                self.assertTrue((abs(rdiff) < allowdiff),
                                msg='row %s: coefficient for order %s is different' % (irow, ic))
        print('')


class sdbaseline_basicTest(sdbaseline_unittest_base):
    """
    Basic unit tests for task sdbaseline. No interactive testing.

    List of tests:
    test000 : default values for all parameters
    test001 : polynominal baselining with no mask (maskmode = 'list'). spw and pol specified.
    test002 : Chebyshev baselining with no mask (maskmode = 'list'). spw and pol specified.
    test003 : cubic spline baselining with no mask (maskmode = 'list'). spw and pol specified.
    test004 : sinusoidal baselining with no mask (maskmode = 'list'). spw and pol specified.
    test050 : existing file as outfile with overwrite=False (raises an exception)
    test051 : no data after selection (raises an exception)
    test060 : blparam files should be overwritten when overwrite=True in fit mode
    test061 : blparam files should not exist when overwrite=False in fit mode
    test062 : blparam files should not be removed in apply mode
    test070 : no output MS when dosubtract=False
    test071 : dosubtract=False and blformat is empty (raises an exception)
    test080 : existent outfile is not overwritten if dosubtract=False

    Note: The input data 'OrionS_rawACSmod_calave.ms' is generated
          from a single dish regression data 'OrionS_rawACSmod' as follows:

          sdcal(infile='OrionS_rawACSmod',scanlist=[20,21,22,23],
                calmode='ps',tau=0.09,outfile='temp.asap')
          sdcal(infile='temp.asap',timeaverage=True,
                tweight='tintsys',outfile='temp2.asap')
          sdsave(infile='temp2.asap',outformat='MS2',
                 outfile='OrionS_rawACSmod_calave.ms')
    """
    # Input and output names
    infile = 'OrionS_rawACSmod_calave.ms'
    outroot = sdbaseline_unittest_base.taskname + '_basictest'
    blrefroot = os.path.join(sdbaseline_unittest_base.datapath, 'refblparam')
    tid = None

    def setUp(self):
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

        if os.path.exists(self.infile + '_blparam.txt'):
            os.remove(self.infile + '_blparam.txt')
        if os.path.exists(self.infile + '_blparam.csv'):
            os.remove(self.infile + '_blparam.csv')
        if os.path.exists(self.infile + '_blparam.btable'):
            shutil.rmtree(self.infile + '_blparam.btable')

    def tearDown(self):
        remove_files_dirs(self.infile)
        remove_files_dirs(self.outroot)

    def test000(self):
        """Basic Test 000: default values for all parameters"""
        tid = '000'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        result = sdbaseline(infile=infile, datacolumn=datacolumn,
                            outfile=outfile)
        # sdbaseline returns None if it runs successfully
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        # uncomment the next line once blparam file can be output
        # self._compareBLparam(outfile+"_blparam.txt",self.blrefroot+tid)
        row = 3
        pol = 1
        results = self._getStats(outfile, '')
        theresult = None
        for i in range(len(results)):
            if ((results[i]['row'] == int(row)) and (results[i]['pol'] == int(pol))):
                theresult = results[i]
        reference = {'rms': 0.16677055621054496,
                     'min': -2.5817961692810059,
                     'max': 1.3842859268188477,
                     'median': -0.00086212158203125,
                     'stddev': 0.16677055621054496,
                     }
        self._compareStats(theresult, reference)

    def test001(self):
        """simple successful case: blfunc = 'poly', maskmode = 'list' and masklist=[] (no mask)"""
        tid = '001'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        maskmode = 'list'
        blfunc = 'poly'
        spw = '3'
        pol = 'LL'
        overwrite = True
        result = sdbaseline(infile=infile, datacolumn=datacolumn,
                            maskmode=maskmode, blfunc=blfunc,
                            spw=spw, pol=pol, outfile=outfile,
                            overwrite=overwrite)
        # sdbaseline returns None if it runs successfully
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        # uncomment the next line once blparam file can be output
        # self._compareBLparam(outfile + "_blparam.txt", self.blrefroot + tid)
        results = self._getStats(outfile)
        print(self._getStats(outfile))
        theresult = None
        for i in range(len(results)):
            theresult = results[i]

        reference = {'rms': 0.16677055621054496,
                     'min': -2.5817961692810059,
                     'max': 1.3842859268188477,
                     'median': -0.00086212158203125,
                     'stddev': 0.16677055621054496,
                     }

        self._compareStats(theresult, reference)

    def test001_uppercase_params(self):
        """simple successful case: blfunc = 'poly', maskmode = 'list' and masklist=[] (no mask)"""
        tid = '001'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'FLOAT_DATA'
        maskmode = 'LIST'
        blfunc = 'POLY'
        spw = '3'
        pol = 'LL'
        overwrite = True
        result = sdbaseline(infile=infile, datacolumn=datacolumn,
                            maskmode=maskmode, blfunc=blfunc,
                            spw=spw, pol=pol, outfile=outfile,
                            overwrite=overwrite)
        # sdbaseline returns None if it runs successfully
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        # uncomment the next line once blparam file can be output
        # self._compareBLparam(outfile + "_blparam.txt", self.blrefroot + tid)
        results = self._getStats(outfile)
        print(self._getStats(outfile))
        theresult = None
        for i in range(len(results)):
            theresult = results[i]

        reference = {'rms': 0.16677055621054496,
                     'min': -2.5817961692810059,
                     'max': 1.3842859268188477,
                     'median': -0.00086212158203125,
                     'stddev': 0.16677055621054496,
                     }

        self._compareStats(theresult, reference)

    def test002(self):
        """successful case: blfunc = 'chebyshev', maskmode = 'list' and masklist=[] (no mask)"""
        tid = '002'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        maskmode = 'list'
        blfunc = 'chebyshev'
        spw = '3'
        pol = 'LL'
        overwrite = True
        result = sdbaseline(infile=infile, datacolumn=datacolumn,
                            maskmode=maskmode, blfunc=blfunc,
                            spw=spw, pol=pol, outfile=outfile,
                            overwrite=overwrite)
        # sdbaseline returns None if it runs successfully
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        # uncomment the next line once blparam file can be output
        # self._compareBLparam(outfile + "_blparam.txt", self.blrefroot + tid)
        results = self._getStats(outfile)
        print(self._getStats(outfile))
        theresult = None
        for i in range(len(results)):
            theresult = results[i]

        reference = {'rms': 0.16677055621054496,
                     'min': -2.5817961692810059,
                     'max': 1.3842859268188477,
                     'median': -0.00086212158203125,
                     'stddev': 0.16677055621054496,
                     }

        self._compareStats(theresult, reference)

    def test003(self):
        """successful case: blfunc = 'cspline', maskmode = 'list' and masklist=[] (no mask)"""
        print("")

        tid = '003'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        maskmode = 'list'
        blfunc = 'cspline'
        overwrite = True
        npiece = 3
        spw = '3'
        pol = 'LL'
        result = sdbaseline(infile=infile, datacolumn=datacolumn,
                            maskmode=maskmode,
                            blfunc=blfunc, npiece=npiece,
                            spw=spw, pol=pol,
                            outfile=outfile, overwrite=overwrite)

        # sdbaseline returns None if it runs successfully
        self.assertIsNone(result, msg="The task returned '" + str(result) + "' instead of None")
        # self._compareBLparam(outfile+"_blparam.txt",self.blrefroot+tid)
        results = self._getStats(outfile)
        print(self._getStats(outfile))

        theresult = None
        for i in range(len(results)):
            theresult = results[i]

        reference = {'rms': 0.16685959517745799,
                     'min': -2.5928177833557129,
                     'max': 1.3953156471252441,
                     'median': -0.00089824199676513672,
                     'stddev': 0.16685959517745766,
                     }

        self._compareStats(theresult, reference)

        # check if baseline is subtracted

        # Output MS only has the selected pol, LL
        in_pol = 1
        out_pol = 0

        # open the original MS
        tb.open(infile)
        orig_pol1_value = np.array(tb.getcell('FLOAT_DATA', int(spw))[in_pol, :])
        tb.close()
        variance_orig_pol1 = np.var(orig_pol1_value)

        # open the MS after sdbaseline
        tb.open(outfile)
        pol1_value = np.array(tb.getcell('FLOAT_DATA', 0)[out_pol, :])
        tb.close()
        variance_pol1 = np.var(pol1_value)

        # assert pol1_value < orig_pol1_value
        self.assertTrue((pol1_value < orig_pol1_value).all())

        # assert variance of pol1_value < variance of orig_pol1_value
        self.assertLess(variance_pol1**0.5, variance_orig_pol1**0.5)

        # print '1sigma before cspline (pol1)', variance_orig_pol1**0.5
        # print '1sigma after cspline (pol1)',  variance_pol1**0.5

    def test050(self):
        """Basic Test 050: failure case: existing file as outfile with overwrite=False"""
        infile = self.infile
        outfile = 'Dummy_Empty.ms'
        mode = 'list'
        os.mkdir(outfile)
        try:
            sdbaseline(infile=infile, outfile=outfile, overwrite=False, maskmode=mode)
        except Exception as e:
            pos = str(e).find("outfile='" + outfile + "' exists, and cannot overwrite it.")
            self.assertNotEqual(pos, -1, msg='Unexpected exception was thrown: %s' % (str(e)))
        finally:
            shutil.rmtree(outfile)

    def test051(self):
        """Basic Test 051: failure case: no data after selection"""
        tid = '051'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        spw = '10'  # non-existent IF value
        mode = 'list'
        try:
            sdbaseline(infile=infile, outfile=outfile, spw=spw, maskmode=mode)
        except Exception as e:
            self.assertIn('Spw Expression: No match found for 10,', str(e))

    def test060(self):
        """Basic Test 060: blparam file(s) should be overwritten when overwrite=True in fit mode"""
        tid = '060'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        overwrite = False
        datacolumn = 'float_data'

        # First run
        sdbaseline(infile=infile, outfile=outfile, overwrite=overwrite, datacolumn=datacolumn)

        # Keep blparam.txt, and remove outfile only
        shutil.rmtree(outfile)
        self.assertFalse(os.path.exists(outfile), msg='{} should not exist'.format(outfile))
        blparamfile = infile + '_blparam.txt'
        self.assertTrue(os.path.exists(blparamfile), msg='{} should exist'.format(blparamfile))

        # Second run (in fit mode, overwrite=True), which must be successful
        overwrite = True
        sdbaseline(infile=infile, outfile=outfile, overwrite=overwrite, datacolumn=datacolumn)

    def test061(self):
        """Basic Test 061: blparam file(s) should not exist when overwrite=False in fit mode """
        tid = '061'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        overwrite = False
        datacolumn = 'float_data'

        # First run
        sdbaseline(infile=infile, outfile=outfile, overwrite=overwrite, datacolumn=datacolumn)

        # Keep blparam.txt, and remove outfile only
        shutil.rmtree(outfile)
        self.assertFalse(os.path.exists(outfile), msg='{} should not exist'.format(outfile))
        blparamfile = infile + '_blparam.txt'
        self.assertTrue(os.path.exists(blparamfile), msg='{} should exist'.format(blparamfile))

        # Second run (in fit mode, overwrite=False), which must emit ValueError
        with self.assertRaises(ValueError, msg='{}_blparam.txt exists.'.format(infile)):
            sdbaseline(infile=infile, outfile=outfile, overwrite=overwrite, datacolumn=datacolumn)

    def test062(self):
        """Basic Test 062: blparam file(s) should not be removed in apply mode"""
        tid = '062'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        overwrite = False
        datacolumn = 'float_data'
        blmode = 'fit'
        blformat = ['text', 'table']

        # First run
        sdbaseline(infile=infile, outfile=outfile, overwrite=overwrite, datacolumn=datacolumn,
                   blmode=blmode, blformat=blformat)

        # Keep blparam files, and remove outfile only
        shutil.rmtree(outfile)
        self.assertFalse(os.path.exists(outfile), msg='{} should not exist'.format(outfile))
        for ext in ['txt', 'bltable']:
            blparamfile = infile + '_blparam.' + ext
            self.assertTrue(os.path.exists(blparamfile), msg='{} should exist'.format(blparamfile))
            # Backup blparam file for comparison
            blparamfile_backup = blparamfile + '.backup'
            if ext == 'txt':
                shutil.copy(blparamfile, blparamfile_backup)
            elif ext == 'bltable':
                shutil.copytree(blparamfile, blparamfile_backup)

        # Second run (in apply mode), which must be successful
        blmode = 'apply'
        bltable = infile + '_blparam.bltable'
        overwrite = True
        sdbaseline(infile=infile, outfile=outfile, overwrite=overwrite, datacolumn=datacolumn,
                   blmode=blmode, bltable=bltable)

        # Param files created in the first run should be kept
        for ext in ['txt', 'bltable']:
            blparamfile = infile + '_blparam.' + ext
            self.assertTrue(os.path.exists(blparamfile), msg='{} should exist'.format(blparamfile))
            blparamfile_backup = blparamfile + '.backup'
            if ext == 'txt':
                res = filecmp.cmp(blparamfile, blparamfile_backup)
                self.assertTrue(res, msg='{} changed'.format(blparamfile))
            elif ext == 'bltable':
                res = compare_dir(blparamfile, blparamfile_backup)
                self.assertEqual(res, '', msg='{0} changed: {1}'.format(blparamfile, res))

    def test070(self):
        """Basic Test 070: no output MS when dosubtract=False"""
        tid = '070'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        dosubtract = False
        blformats = ['text', 'csv', 'table', ['text', 'table'], ['text', ''],
                     ['text', 'csv', 'table']]

        for blformat in blformats:
            print(f"Testing blformat='{blformat}'...")
            remove_files_dirs(infile + '_blparam.')
            remove_single_file_dir(outfile)
            self.assertFalse(os.path.exists(outfile), f"{outfile} must be deleted before testing.")

            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile, dosubtract=dosubtract,
                       blformat=blformat)
            self.assertFalse(os.path.exists(outfile), f"{outfile} is created.")

    def test071(self):
        """Basic Test 071: dosubtract=False and blformat is empty (raises an exception)"""
        tid = '071'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        dosubtract = False
        blformats = ['', [], ['', '', '']]

        for blformat in blformats:
            print(f"Testing blformat='{blformat}'...")
            with self.assertRaises(ValueError,
                                   msg="blformat must be specified when dosubtract is False"):
                sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                           dosubtract=dosubtract, blformat=blformat)

    def test080(self):
        """Basic Test 080: existent outfile is not overwritten if dosubtract=False"""
        tid = '080'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        dosubtract = False
        overwrite = False
        datacolumn = 'float_data'

        # Copy infile to outfile so that outfile does exist prior to sdbaseline, and also
        # outfile is identical to non-baselined data (infile)
        shutil.copytree(infile, outfile)

        # Run sdbaseline with dosubtract=False
        try:
            sdbaseline(infile=infile, outfile=outfile, dosubtract=dosubtract, overwrite=overwrite,
                       datacolumn=datacolumn)
        except Exception as e:
            print('unexpected failure!')
            raise e

        # Confirm outfile data unchanged, i.e., identical to infile
        for sta_in, sta_out in zip(self._getStats(infile, ''), self._getStats(outfile, '')):
            for k in sta_in.keys():
                if k in ['row', 'pol']:
                    self.assertEqual(sta_in[k], sta_out[k])
                else:
                    self.assertAlmostEqual(sta_in[k], sta_out[k])


class sdbaseline_maskTest(sdbaseline_unittest_base):
    """
    Tests for various mask selections. No interactive testing.

    List of tests:
    test100 : with masked ranges at the edges of spectrum. blfunc is cspline.
    test101 : with masked ranges not touching spectrum edge

    Note: input data is generated from a single dish regression data,
    'OrionS_rawACSmod', as follows:
      sdcal(infile='OrionS_rawACSmod',scanlist=[20,21,22,23],
                calmode='ps',tau=0.09,outfile='temp.asap')
      sdcal(infile='temp.asap',timeaverage=True,
                tweight='tintsys',outfile='temp2.asap')
      sdsave(infile='temp2.asap',outformat='MS2',
                outfile='OrionS_rawACSmod_calave.ms')
    """
    # Input and output names
    infile = 'OrionS_rawACSmod_calave.ms'
    outroot = sdbaseline_unittest_base.taskname + '_masktest'
    blrefroot = os.path.join(sdbaseline_unittest_base.datapath, 'refblparam_mask')
    tid = None

    # Channel range excluding bad edge
    search = [[200, 7599]]
    # Baseline channels. should be identical to one selected by 'auto' mode
    blchan0 = [[200, 3979], [4152, 7599]]
    blchan2 = [[200, 2959], [3120, 7599]]

    # reference values
    ref_pol0if0 = {'linemaxpos': 4102.0, 'linesum': 103.81604766845703,
                   'linemax': 1.6280698776245117,
                   'baserms': 0.15021507441997528,
                   'basestd': 0.15022546052932739}
    ref_pol0if2 = {'rms': 0.13134850561618805,
                   'stddev': 0.1313575953245163}
    """
    ref_pol0if2 = {'linemaxpos': 3045.0, 'linesum': 127.79755401611328,
                   'linemax': 2.0193681716918945,
                   'baserms': 0.13134850561618805,
                   'basestd': 0.1313575953245163}
    """

    def setUp(self):
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

        if os.path.exists(self.infile + '_blparam.txt'):
            os.remove(self.infile + '_blparam.txt')
        if os.path.exists(self.infile + '_blparam.csv'):
            os.remove(self.infile + '_blparam.csv')
        if os.path.exists(self.infile + '_blparam.btable'):
            shutil.rmtree(self.infile + '_blparam.btable')

    def tearDown(self):
        remove_files_dirs(self.infile)
        remove_files_dirs(self.outroot)

    def test100(self):
        """Mask Test 100: with masked ranges at the edges of spectrum. blfunc must be cspline."""
        self.tid = '100'
        infile = self.infile
        outfile = self.outroot + self.tid + '.ms'
        datacolumn = 'float_data'
        mode = 'list'
        spw = '2:%s' % (';'.join(map(self._get_range_in_string, self.search)))
        pol = 'RR'
        blfunc = 'cspline'
        npiece = 4

        result = sdbaseline(infile=infile, datacolumn=datacolumn, maskmode=mode,
                            spw=spw, pol=pol, blfunc=blfunc, npiece=npiece,
                            outfile=outfile)
        # sdbaseline returns None if it runs successfully
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        # Compare IF2
        testval = self._getStats(filename=outfile, spw='', pol=0, mask=self.search)
        ref100 = {'rms': 0.18957555661537034,
                  'min': -0.48668813705444336,
                  'max': 1.9516196250915527,
                  'median': -0.013428688049316406,
                  'stddev': 0.18957555661537034,
                  'row': 0,
                  'pol': 0}
        self._compareStats(testval[0], ref100)

    def test101(self):
        """Mask Test 101: with masked ranges not touching spectrum edge"""
        self.tid = '101'
        infile = self.infile
        outfile = self.outroot + self.tid + '.ms'
        datacolumn = 'float_data'
        mode = 'list'
        spw = '2:%s' % (';'.join(map(self._get_range_in_string, self.blchan2)))
        pol = 'RR'

        print('spw =', spw)

        result = sdbaseline(infile=infile, datacolumn=datacolumn, maskmode=mode,
                            outfile=outfile, spw=spw, pol=pol)
        # sdbaseline returns None if it runs successfully
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        # Compare IF2
        testval = self._getStats(filename=outfile, spw='', pol=0, mask=self.blchan2)
        self._compareStats(testval[0], self.ref_pol0if2)
        # self._compareBLparam(self.outroot+self.tid+'.asap_blparam.txt',\
        #                     self.blrefroot+self.tid)

    def _get_range_in_string(self, valrange):
        if isinstance(valrange, list) or isinstance(valrange, tuple):
            return str(valrange[0]) + '~' + str(valrange[1])
        else:
            return False


class sdbaseline_sinusoidTest(sdbaseline_unittest_base):
    """
    Tests for sinusoidal baseline fitting. No interactive testing.

    List of tests:
    test000 : addwn as integer
    test001 : addwn as list of an integer
    test002 : addwn as list of integers
    test003 : addwn as tuple of an integer
    test004 : addwn as tuple of integers
    test005 : addwn as string (single wave number)
    test006 : addwn as string (comma-separated wave numbers)
    test007 : addwn as string (wave number range specified with '-')
    test008 : addwn as string (wave number range specified with '~')
    test009 : addwn as string (less or equal pattern 1)
    test010 : addwn as string (less or equal pattern 2)
    test011 : addwn as string (less or equal pattern 3)
    test012 : addwn as string (less or equal pattern 4)
    test013 : addwn as string (less pattern 1)
    test014 : addwn as string (less pattern 2)
    test015 : addwn as string (greater or equal pattern 1)
    test016 : addwn as string (greater or equal pattern 2)
    test017 : addwn as string (greater or equal pattern 3)
    test018 : addwn as string (greater or equal pattern 4)
    test019 : addwn as string (greater pattern 1)
    test020 : addwn as string (greater pattern 2)
    test021 : specify fftthresh by 'sigma' + checking residual rms
    test022 : specify fftthresh by 'top' + checking residual rms
    test023 : sinusoid-related parameters with default values
    test024 : addwn has too large value but rejwn removes it

    test021_uppercase_params : specify fftthresh by 'SIGMA' + checking residual rms
    test022_uppercase_params : specify fftthresh by 'TOP' + checking residual rms
    test025_uppercase_params : specify fftmethod by 'FFT'

    test100 : no effective wave number set (addwn empty list, applyfft=False)
    test101 : no effective wave number set (addwn empty list, applyfft=True)
    test102 : no effective wave number set (addwn empty tuple, applyfft=False)
    test103 : no effective wave number set (addwn empty tuple, applyfft=True)
    test104 : no effective wave number set (addwn empty string, applyfft=False)
    test105 : no effective wave number set (addwn empty string, applyfft=True)
    test106 : no effective wave number set (addwn and rejwn identical, applyfft=False)
    test107 : no effective wave number set (addwn and rejwn identical, applyfft=True)
    test108 : no effective case (rejwn covers wider range than that of addwn, applyfft=False)
    test109 : no effective case (rejwn covers wider range than that of addwn, applyfft=True)
    test110 : wn range greater than upper limit
    test111 : explicitly specify wn (greater than upper limit)
    test112 : explicitly specify wn (negative)
    test113 : explicitly specify wn (addwn has negative and greater than upper limit)
    test114 : explicitly specify wn (both addwn/rejwn have negative and greater than upper limit)
    test115 : wrong fftthresh (as list)
    test116 : wrong fftthresh (as string 'asigma')
    test117 : wrong fftthresh (as string 'topa')
    test118 : wrong fftthresh (as string 'top3sigma')
    test119 : wrong fftthresh (as string 'a123')
    test120 : wrong fftthresh (as string '')
    test121 : wrong fftthresh (as string '-3.0')
    test122 : wrong fftthresh (as string '0.0')
    test123 : wrong fftthresh (as string '-3')
    test124 : wrong fftthresh (as string '0')
    test125 : wrong fftthresh (as string '-3.0sigma')
    test126 : wrong fftthresh (as string '0.0sigma')
    test127 : wrong fftthresh (as string '-3sigma')
    test128 : wrong fftthresh (as string '0sigma')
    test129 : wrong fftthresh (as string 'top-3')
    test130 : wrong fftthresh (as string 'top0')
    test131 : wrong fftthresh (as string 'top1.5')
    test132 : wrong fftthresh (as float -3.0)
    test133 : wrong fftthresh (as float 0.0)
    test134 : wrong fftthresh (as int -3)
    test135 : wrong fftthresh (as int 0)

    Note: The input data 'sinusoidal.ms' has just two spectral data,
          which are actually identical and described as
          spec[i] = sin(i*2*PI/8191) + 4 * sin(i*2*PI/8191*3)
                    + 8 * sin(i*2*PI/8191*5) + 2 * sin(i*2*PI/8191*12).
          addwn='1,3,5,12' will be enough to perfectly fit this spectrum, but
          applyfft=True and fftthresh='top4' will also do.
    """
    # Input and output names
    infile = 'sinusoidal.ms'
    outroot = sdbaseline_unittest_base.taskname + '_sinusoidtest'
    tid = None

    def setUp(self):
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

        if os.path.exists(self.infile + '_blparam.txt'):
            os.remove(self.infile + '_blparam.txt')
        if os.path.exists(self.infile + '_blparam.csv'):
            os.remove(self.infile + '_blparam.csv')
        if os.path.exists(self.infile + '_blparam.btable'):
            shutil.rmtree(self.infile + '_blparam.btable')

    def tearDown(self):
        remove_files_dirs(self.infile)
        remove_files_dirs(self.outroot)

    def test000(self):
        """Sinusoid Test 000: addwn as integer"""
        tid = '000'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = 0
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test001(self):
        """Sinusoid Test 001: addwn as list of an integer"""
        tid = '001'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [0]
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test002(self):
        """Sinusoid Test 002: addwn as list of integers"""
        tid = '002'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [0, 1]
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test003(self):
        """Sinusoid Test 003: addwn as tuple of an integer"""
        tid = '003'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [0]
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test004(self):
        """Sinusoid Test 004: addwn as tuple of integers"""
        tid = '004'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [0, 1]
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test005(self):
        """Sinusoid Test 005: addwn as string (single wave number)"""
        tid = '005'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '0'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test006(self):
        """Sinusoid Test 006: addwn as string (comma-separated wave numbers)"""
        tid = '006'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '0,1'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test007(self):
        """Sinusoid Test 007: addwn as string (wave number range specified with '-')"""
        tid = '007'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '0-2'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test008(self):
        """Sinusoid Test 008: addwn as string (wave number range specified with '~')"""
        tid = '008'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '0~2'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test009(self):
        """Sinusoid Test 009: addwn as string (less or equal pattern 1)"""
        tid = '009'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '<=2'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test010(self):
        """Sinusoid Test 010: addwn as string (less or equal pattern 2)"""
        tid = '010'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '=<2'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test011(self):
        """Sinusoid Test 011: addwn as string (less or equal pattern 3)"""
        tid = '011'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '2>='
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test012(self):
        """Sinusoid Test 012: addwn as string (less or equal pattern 4)"""
        tid = '012'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '2=>'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test013(self):
        """Sinusoid Test 013: addwn as string (less pattern 1)"""
        tid = '013'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '<2'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test014(self):
        """Sinusoid Test 014: addwn as string (less pattern 2)"""
        tid = '014'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '2>'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test015(self):
        """Sinusoid Test 015: addwn as string (greater or equal pattern 1)"""
        tid = '015'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '4090<='
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test016(self):
        """Sinusoid Test 016: addwn as string (greater or equal pattern 2)"""
        tid = '016'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '4090=<'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test017(self):
        """Sinusoid Test 017: addwn as string (greater or equal pattern 3)"""
        tid = '017'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '>=4090'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test018(self):
        """Sinusoid Test 018: addwn as string (greater or equal pattern 4)"""
        tid = '018'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '=>4090'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test019(self):
        """Sinusoid Test 019: addwn as string (greater pattern 1)"""
        tid = '019'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '4090<'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test020(self):
        """Sinusoid Test 020: addwn as string (greater pattern 2)"""
        tid = '020'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '>4090'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=False)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test021(self):
        """Sinusoid Test 021: specify fftthresh by 'sigma' + checking residual rms"""
        tid = '021'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '0'
        fftthresh = '3.0sigma'
        torr = 1.0e-6
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=True, fftthresh=fftthresh)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        stat = self._getStats(filename=outfile, pol='0')
        self.assertTrue(stat[0]['rms'] < torr)

    def test021_uppercase_params(self):
        """Sinusoid Test 021: specify fftthresh by 'SIGMA' + checking residual rms"""
        tid = '021'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'FLOAT_DATA'
        addwn = '0'
        fftthresh = '3.0SIGMA'
        torr = 1.0e-6
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=True, fftthresh=fftthresh)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        stat = self._getStats(filename=outfile, pol='0')
        self.assertTrue(stat[0]['rms'] < torr)

    def test022(self):
        """Sinusoid Test 022: specify fftthresh by 'top' + checking residual rms"""
        tid = '022'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '0'
        fftthresh = 'top4'
        torr = 1.0e-6
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=True, fftthresh=fftthresh)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        stat = self._getStats(filename=outfile, pol='0')
        self.assertTrue(stat[0]['rms'] < torr)

    def test022_uppercase_params(self):
        """Sinusoid Test 022: specify fftthresh by 'TOP' + checking residual rms"""
        tid = '022'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'FLOAT_DATA'
        addwn = '0'
        fftthresh = 'TOP4'
        torr = 1.0e-6
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=True, fftthresh=fftthresh)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        stat = self._getStats(filename=outfile, pol='0')
        self.assertTrue(stat[0]['rms'] < torr)

    def test023(self):
        """Sinusoid Test 023: sinusoid-related parameters with default values"""
        tid = '023'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid')
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test024(self):
        """Sinusoid Test 024: addwn has too large value but rejwn removes it"""
        tid = '024'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        applyfft = False
        addwn = [0, 10000]
        rejwn = '4000<'
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', applyfft=applyfft, addwn=addwn, rejwn=rejwn)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def test025_uppercase_params(self):
        """Sinusoid Test 025: specify fftmethod by 'FFT' + checking residual rms"""
        tid = '025'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'FLOAT_DATA'
        addwn = '0'
        fftmethod = 'FFT'
        fftthresh = '3.0SIGMA'
        torr = 1.0e-6
        result = sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                            blfunc='sinusoid', addwn=addwn, applyfft=True,
                            fftmethod=fftmethod, fftthresh=fftthresh)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        stat = self._getStats(filename=outfile, pol='0')
        self.assertTrue(stat[0]['rms'] < torr)

    def test100(self):
        """Sinusoid Test 100: no effective wave number set (addwn empty list, applyfft=False)"""
        tid = '100'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, applyfft=False)
        except Exception as e:
            self.assertEqual(str(e), 'addwn must contain at least one element.')

    def test101(self):
        """Sinusoid Test 101: no effective wave number set (addwn empty list, applyfft=True)"""
        tid = '101'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'addwn must contain at least one element.')

    def test102(self):
        """Sinusoid Test 102: no effective wave number set (addwn empty tuple, applyfft=False)"""
        tid = '102'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, applyfft=False)
        except Exception as e:
            self.assertEqual(str(e), 'addwn must contain at least one element.')

    def test103(self):
        """Sinusoid Test 103: no effective wave number set (addwn empty tuple, applyfft=True)"""
        tid = '103'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'addwn must contain at least one element.')

    def test104(self):
        """Sinusoid Test 104: no effective wave number set (addwn empty string, applyfft=False)"""
        tid = '104'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = ''
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, applyfft=False)
        except Exception as e:
            self.assertEqual(str(e), 'string index out of range')

    def test105(self):
        """Sinusoid Test 105: no effective wave number set (addwn empty string, applyfft=True)"""
        tid = '105'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = ''
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'string index out of range')

    def test106(self):
        """no effective wavenumber (addwn and rejwn identical, applyfft=False)"""
        tid = '106'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [0, 1, 2]
        rejwn = [0, 1, 2]
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=False)
        except Exception as e:
            self.assertEqual(str(e), 'No effective wave number given for sinusoidal fitting.')

    def test107(self):
        """no effective wavenumber (addwn and rejwn identical, applyfft=True)"""
        tid = '107'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [0, 1, 2]
        rejwn = [0, 1, 2]
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'No effective wave number given for sinusoidal fitting.')

    def test108(self):
        """no effective wavenumber (rejwn covers wider range than that of addwn, applyfft=False)"""
        tid = '108'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '<5'
        rejwn = '<10'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=False)
        except Exception as e:
            self.assertEqual(str(e), 'No effective wave number given for sinusoidal fitting.')

    def test109(self):
        """no effective wavenumber (rejwn covers wider range than that of addwn, applyfft=True)"""
        tid = '109'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '<5'
        rejwn = '<10'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'No effective wave number given for sinusoidal fitting.')

    def test110(self):
        """Sinusoid Test 110: wn range greater than upper limit"""
        tid = '110'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = '5000<'
        rejwn = '<5100'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'No effective wave number given for sinusoidal fitting.')

    def test111(self):
        """Sinusoid Test 111: explicitly specify wn value (greater than upper limit)"""
        tid = '111'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [5000, 5500]
        rejwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'No effective wave number given for sinusoidal fitting.')

    def test112(self):
        """Sinusoid Test 112: explicitly specify wn value (negative)"""
        tid = '112'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [-10, 5]
        rejwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'wrong value given for addwn/rejwn')

    def test113(self):
        """explicitly specify wn (addwn has negative and greater than upper limit)"""
        tid = '113'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [-10, 5000]
        rejwn = []
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'wrong value given for addwn/rejwn')

    def test114(self):
        """explicitly specify wn (both addwn/rejwn have negative and greater than upper limit)"""
        tid = '114'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        addwn = [-10, 5000]
        rejwn = [-10, 5500]
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', addwn=addwn, rejwn=rejwn, applyfft=True)
        except Exception as e:
            self.assertEqual(str(e), 'wrong value given for addwn/rejwn')

    def test115(self):
        """Sinusoid Test 115: wrong fftthresh (as list)"""
        tid = '115'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = [3.0]
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh must be float or integer or string.')

    def test116(self):
        """Sinusoid Test 116: wrong fftthresh (as string 'asigma')"""
        tid = '116'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'asigma'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh has a wrong format.')

    def test117(self):
        """Sinusoid Test 117: wrong fftthresh (as string 'topa')"""
        tid = '117'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'topa'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh has a wrong format.')

    def test118(self):
        """Sinusoid Test 118: wrong fftthresh (as string 'top3sigma')"""
        tid = '118'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'top3sigma'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh has a wrong format.')

    def test119(self):
        """Sinusoid Test 119: wrong fftthresh (as string 'a123')"""
        tid = '119'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'a123'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh has a wrong format.')

    def test120(self):
        """Sinusoid Test 120: wrong fftthresh (as string '')"""
        tid = '120'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = ''
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh has a wrong format.')

    def test121(self):
        """Sinusoid Test 121: wrong fftthresh (as string '-3.0')"""
        tid = '121'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '-3.0'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test122(self):
        """Sinusoid Test 122: wrong fftthresh (as string '0.0')"""
        tid = '122'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '0.0'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test123(self):
        """Sinusoid Test 123: wrong fftthresh (as string '-3')"""
        tid = '123'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '-3'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test124(self):
        """Sinusoid Test 124: wrong fftthresh (as string '0')"""
        tid = '124'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '0'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test125(self):
        """Sinusoid Test 125: wrong fftthresh (as string '-3.0sigma')"""
        tid = '125'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '-3.0sigma'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test126(self):
        """Sinusoid Test 126: wrong fftthresh (as string '0.0sigma')"""
        tid = '126'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '0.0sigma'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test127(self):
        """Sinusoid Test 127: wrong fftthresh (as string '-3sigma')"""
        tid = '127'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '-3sigma'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test128(self):
        """Sinusoid Test 128: wrong fftthresh (as string '0sigma')"""
        tid = '128'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = '0sigma'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test129(self):
        """Sinusoid Test 129: wrong fftthresh (as string 'top-3')"""
        tid = '129'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'top-3'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test130(self):
        """Sinusoid Test 130: wrong fftthresh (as string 'top0')"""
        tid = '130'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'top0'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test131(self):
        """Sinusoid Test 131: wrong fftthresh (as string 'top1.5')"""
        tid = '131'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 'top1.5'
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'fftthresh has a wrong format.')

    def test132(self):
        """Sinusoid Test 132: wrong fftthresh (as float -3.0)"""
        tid = '132'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = -3.0
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test133(self):
        """Sinusoid Test 133: wrong fftthresh (as float 0.0)"""
        tid = '133'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 0.0
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test134(self):
        """Sinusoid Test 134: wrong fftthresh (as int -3)"""
        tid = '134'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = -3
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')

    def test135(self):
        """Sinusoid Test 135: wrong fftthresh (as int 0)"""
        tid = '135'
        infile = self.infile
        outfile = self.outroot + tid + '.ms'
        datacolumn = 'float_data'
        fftthresh = 0
        try:
            sdbaseline(infile=infile, datacolumn=datacolumn, outfile=outfile,
                       blfunc='sinusoid', applyfft=True, fftthresh=fftthresh)
        except Exception as e:
            self.assertEqual(str(e), 'threshold given to fftthresh must be positive.')


# this class is not included in the suite, skip the tests (needed for CASA6)
class sdbaseline_multi_IF_test(sdbaseline_unittest_base):
    """
    Unit tests for task sdbaseline. No interactive testing.

    This test intends to check whether sdbaseline task works fine
    for data that has multiple IFs whose nchan differ each other.

    List of tests:
    test200 : test multi IF data input
    """
    # Input and output names
    infile = 'testMultiIF.asap'
    blparamfile_suffix = '_blparam.txt'
    outroot = os.path.join(sdbaseline_unittest_base.taskname, '_multi')
    refblparamfile = 'refblparam_multiIF'

    def setUp(self):
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

        if os.path.exists(self.infile + '_blparam.txt'):
            os.remove(self.infile + '_blparam.txt')
        if os.path.exists(self.infile + '_blparam.csv'):
            os.remove(self.infile + '_blparam.csv')
        if os.path.exists(self.infile + '_blparam.btable'):
            shutil.rmtree(self.infile + '_blparam.btable')

    def tearDown(self):
        remove_single_file_dir(self.infile)
        remove_files_dirs(self.outroot)

    @unittest.skip("Not currently part of the test suite")
    def test200(self):
        """test200: Test the task works with multi IF data"""
        infile = self.infile
        mode = "list"
        blfunc = "poly"
        order = 1
        outfile = os.path.join(self.outroot, ".asap")
        blparamfile = os.path.join(outfile, self.blparamfile_suffix)

        result = sdbaseline(infile=infile, maskmode=mode, outfile=outfile,
                            blfunc=blfunc, order=order)
        self.assertIsNone(result, msg="The task returned '" + str(result) + "' instead of None")
        self._compareBLparam(blparamfile, self.datapath + self.refblparamfile)
        reference = {5: {'rms': 1.4250789880752563,
                         'min': -4.2702846527099609,
                         'max': 5.5566844940185547,
                         'max_abscissa': {'value': 823.0,
                                          'unit': 'channel'},
                         'median': 0.017315864562988281,
                         'min_abscissa': {'value': 520.0,
                                          'unit': 'channel'},
                         'stddev': 1.425775408744812},
                     7: {'rms': 1.4971292018890381,
                         'min': -4.7103700637817383,
                         'max': 5.4820127487182617,
                         'max_abscissa': {'value': 1335.0,
                                          'unit': 'channel'},
                         'median': 0.027227401733398438,
                         'min_abscissa': {'value': 1490.0,
                                          'unit': 'channel'},
                         'stddev': 1.4974949359893799}}
        for ifno in [5, 7]:
            currstat = self._getStats(outfile, ifno)
            self._compareStats(currstat, reference[ifno])


class sdbaseline_outbltableTest(sdbaseline_unittest_base):
    """
    Tests for outputting baseline table

    List of tests
    test301 : blmode='fit', bloutput!='', dosubtract=True, blfunc='poly'/'chebyshev'/'cspline'
                (poly/chebyshev/cspline fit in MS, bltable is written)
    test302 : blmode='fit', bloutput!='', dosubtract=True, blfunc='variable'
                (variable fit in MS, bltable is written)
                testing 3 cases:
                    (1) blparam contains values for all spectra
                    (2) no values for a spectrum (row=2,pol=1), which is to be skipped
                    (3) values commented out for a spectrum (row=2,pol=1), which is to be skipped
    test303 : blmode='fit', bloutput!='', dosubtract=True, blfunc='poly','chebyshev','cspline'
                testing if bltable is shortened
                testing 3 cases:
                    (1) all spectra in row 2 are flagged entirely
                    (2) in row 2, entirely flagged for pol 0, also pol 1 is unselected
                    (3) in row 2, entirely flagged for pol 1, also pol 0 is unselected
    test304 : same as test303, but for blfunc='variable'
    test305 : blmode='fit', bloutput!='', dosubtract=True, blfunc='variable(sinusoid)'

    Note: input data is generated from a single dish regression data,
    'OrionS_rawACSmod', as follows:
      sdcal(infile='OrionS_rawACSmod',scanlist=[20,21,22,23],
                calmode='ps',tau=0.09,outfile='temp.asap')
      sdcal(infile='temp.asap',timeaverage=True,
                tweight='tintsys',outfile='temp2.asap')
      sdsave(infile='temp2.asap',outformat='MS2',
                outfile='OrionS_rawACSmod_calave.ms')
    """
    # Input and output names
    infile = 'OrionS_rawACSmod_calave.ms'
    sin_infile = 'sinusoidal.ms'
    sin_blparam = 'blparam_variable_sin.txt'
    outroot = sdbaseline_unittest_base.taskname + '_bltabletest'
    tid = None
    ftype = {'poly': 0, 'chebyshev': 1, 'cspline': 2, 'sinusoid': 3}

    def setUp(self):
        dirs_to_copy = [self.infile, self.sin_infile, self.sin_blparam]
        for directory in dirs_to_copy:
            shutil.rmtree(directory, ignore_errors=True)
            if directory == self.sin_blparam:
                shutil.copyfile(os.path.join(self.datapath, directory), directory)
            else:
                shutil.copytree(os.path.join(self.datapath, directory), directory,
                                 dirs_exist_ok=False)

        files_to_remove = [
            self.infile + '_blparam.txt',
            self.infile + '_blparam.csv',
            self.infile + '_blparam.btable',
            self.sin_infile + '_blparam.txt',
            self.sin_infile + '_blparam.csv',
            self.sin_infile + '_blparam.btable'
        ]
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path, ignore_errors=True)
                else:
                    os.remove(file_path)

    def tearDown(self):
        remove_single_file_dir(self.infile)
        remove_files_dirs(self.outroot)
        remove_single_file_dir(self.sin_infile)
        remove_single_file_dir(self.sin_blparam)

    def _checkBltableVar(self, outms, bltable, blparam, option):
        npol = 2
        results = [[4.280704], [3.912475],
                   [4.323003, 0.00196013], [3.839441, -8.761247e-06],
                   [4.280719, 0.00255683, 0.00619966], [4.140454, -7.516477e-05, 6.538814e-09],
                   [4.221929, -8.751897e-06, -6.81303991e-09, 3.36383428e-13],
                   [3.983634, -6.322114e-06, -1.11215614e-08, 7.00922610e-13]
                   ]
        rms = [0.162739, 0.182507, 0.140955, 0.159999, 0.132135, 0.381708, 0.128761, 0.146849]
        tb.open(bltable)
        try:
            for i in range(npol * tb.nrows()):
                irow = i // npol
                ipol = i % npol
                is_skipped = (option != '') and (irow == 2) and (ipol == 1)

                self.assertEqual(not is_skipped, tb.getcell('APPLY', irow)[ipol][0])
                if is_skipped:
                    continue

                self.assertEqual(self.ftype[blparam['btype'][i].lower()],
                                 tb.getcell('FUNC_TYPE', irow)[ipol][0])
                fparam_key = 'order' if (blparam['btype'][i] != 'cspline') else 'npiec'
                self.assertEqual(blparam[fparam_key][i], tb.getcell('FUNC_PARAM', irow)[ipol][0])

                if (blparam['btype'][i] == 'cspline'):
                    for j in range(blparam['npiec'][i]):
                        self.assertEqual(0.0, tb.getcell('FUNC_FPARAM', irow)[ipol][j])
                else:
                    self.assertEqual(0, len(tb.getcell('FUNC_FPARAM', irow)[ipol]))
                for j in range(len(results[i])):
                    self._checkValue(results[i][j], tb.getcell('RESULT', irow)[ipol][j], 1.0e-5)
                self._checkValue(rms[i], tb.getcell('RMS', irow)[ipol][0], 1.0e-1)
                self._checkValue(float(blparam['cthre'][i]),
                                 tb.getcell('CLIP_THRESHOLD', irow)[ipol][0], 1.0e-6)
                self.assertEqual(blparam['nclip'][i], tb.getcell('CLIP_ITERATION', irow)[ipol][0])
                uself = (blparam['uself'][i] == 'true')
                self.assertEqual(uself, tb.getcell('USE_LF', irow)[ipol][0])
                lthre = 5.0 if ((blparam['lthre'][i] == '') or not uself) \
                    else float(blparam['lthre'][i])
                self._checkValue(lthre, tb.getcell('LF_THRESHOLD', irow)[ipol][0], 1.0e-6)
                chavg = 0 if (blparam['chavg'][i] == '') else int(blparam['chavg'][i])
                self.assertEqual(chavg, tb.getcell('LF_AVERAGE', irow)[ipol][0])
                ledge = 0 if ((blparam['ledge'][i] == '') or not uself) \
                    else int(blparam['ledge'][i])
                self.assertEqual(ledge, tb.getcell('LF_EDGE', irow)[ipol][0])
                redge = 0 if ((blparam['redge'][i] == '') or not uself) \
                    else int(blparam['redge'][i])
                self.assertEqual(redge, tb.getcell('LF_EDGE', irow)[ipol][1])
        finally:
            tb.close()

    def _checkBltable(self, outms, bltable, blfunc, order, mask):
        tb.open(bltable)
        for irow in range(tb.nrows()):
            for ipol in range(len(tb.getcell('RMS', irow))):
                self.assertEqual(tb.getcell('FUNC_TYPE', irow)[ipol], self.ftype[blfunc.lower()])
                self.assertEqual(tb.getcell('FUNC_PARAM', irow)[ipol], order)
                ref = self._getStats(filename=outms, spw=str(irow), pol=str(ipol), mask=mask[irow])
                # tolerance value in the next line is temporarily set a bit large
                # since rms in bltable is smaller than expected because it is
                # calculated based on masklist currently stored in bltable, which
                # is after an extra clipping.
                # this bug is already fixed in trunk of Sakura, so once libsakura
                # is updated we can set smaller tolerance value. (2015/4/22 WK)
                self._checkValue(ref[0]['rms'], tb.getcell('RMS', irow)[ipol][0], 2.0e-2)
        tb.close()

    def _checkValue(self, ref, out, tol=1.0e-02):
        # print '###################################'
        # print 'ref = ' + str(ref) + ', out = ' + str(out)
        if (abs(ref) > tol) or (abs(out) > tol):
            if ref != 0.0:
                rel = abs((out - ref) / ref)
            elif out != 0.0:
                rel = abs((out - ref) / out)
            else:
                rel = abs(out - ref)
            self.assertTrue((rel < tol),
                            msg=f'the output ({out}) differs from reference ({ref})')

    def test301(self):
        """test301: poly/chebyshev/cspline baselining, output bltable"""
        self.tid = '301'
        infile = self.infile
        datacolumn = 'float_data'
        spw = '0:1000~3500;5000~7500,1:500~7500,2:500~2500;3500~7500'
        mask = [[[1000, 3500], [5000, 7500]],
                [[500, 7500]],
                [[500, 2500], [3500, 7500]]
                ]
        blmode = 'fit'
        blformat = 'table'
        dosubtract = True
        blfunc = ['poly', 'chebyshev', 'cspline']
        order = 5
        npiece = 4
        rms_s0p0_ms = [0.150905484071, 0.150905484071, 0.149185846787]

        for i in range(len(blfunc)):
            print('testing blfunc=' + blfunc[i] + '...')
            outfile = self.outroot + self.tid + blfunc[i] + '.ms'
            bloutput = self.outroot + self.tid + blfunc[i] + '.bltable'
            result = sdbaseline(infile=infile, datacolumn=datacolumn,
                                blmode=blmode, blformat=blformat, bloutput=bloutput,
                                spw=spw, blfunc=blfunc[i], order=order, npiece=npiece,
                                dosubtract=dosubtract, outfile=outfile)
            self.assertEqual(result, None,
                             msg="The task returned '" + str(result) + "' instead of None")
            msresult = self._getStats(filename=outfile, spw='0', pol='0', mask=mask[0])
            self._checkValue(rms_s0p0_ms[i], msresult[0]['stddev'], 1.0e-6)

            fparam = npiece if blfunc[i] == 'cspline' else order
            self._checkBltable(outfile, bloutput, blfunc[i], fparam, mask)

    def test301_uppercase_params(self):
        """test301: poly/chebyshev/cspline baselining, output bltable"""
        self.tid = '301'
        infile = self.infile
        datacolumn = 'FLOAT_DATA'
        spw = '0:1000~3500;5000~7500,1:500~7500,2:500~2500;3500~7500'
        mask = [[[1000, 3500], [5000, 7500]],
                [[500, 7500]],
                [[500, 2500], [3500, 7500]]
                ]
        blmode = 'FIT'
        blformat = 'TABLE'
        dosubtract = True
        blfunc = ['POLY', 'CHEBYSHEV', 'CSPLINE']
        order = 5
        npiece = 4
        rms_s0p0_ms = [0.150905484071, 0.150905484071, 0.149185846787]

        for i in range(len(blfunc)):
            print('testing blfunc=' + blfunc[i] + '...')
            outfile = self.outroot + self.tid + blfunc[i] + '.ms'
            bloutput = self.outroot + self.tid + blfunc[i] + '.bltable'
            result = sdbaseline(infile=infile, datacolumn=datacolumn,
                                blmode=blmode, blformat=blformat, bloutput=bloutput,
                                spw=spw, blfunc=blfunc[i], order=order, npiece=npiece,
                                dosubtract=dosubtract, outfile=outfile)
            self.assertEqual(result, None,
                             msg="The task returned '" + str(result) + "' instead of None")
            msresult = self._getStats(filename=outfile, spw='0', pol='0', mask=mask[0])
            self._checkValue(rms_s0p0_ms[i], msresult[0]['stddev'], 1.0e-6)

            fparam = npiece if blfunc[i] == 'CSPLINE' else order
            self._checkBltable(outfile, bloutput, blfunc[i], fparam, mask)

    def test302(self):
        """test302: per-spectrum baselining, output bltable"""
        self.tid = '302'
        infile = self.infile
        datacolumn = 'float_data'
        blmode = 'fit'
        blformat = 'table'
        blfunc = 'variable'
        dosubtract = True

        for option in ['', 'r2p1less', 'r2p1cout']:
            bloutput = self.outroot + self.tid + option + '.bltable'
            outfile = self.outroot + self.tid + option + '.ms'
            blparam = self.outroot + self.tid + option + '.blparam'
            self._createBlparamFile(blparam, self.blparam_order, self.blparam_dic, option)
            result = sdbaseline(infile=infile, datacolumn=datacolumn,
                                blmode=blmode, blformat=blformat, bloutput=bloutput,
                                blfunc=blfunc, blparam=blparam,
                                dosubtract=dosubtract, outfile=outfile)
            self.assertEqual(result, None,
                             msg="The task returned '" + str(result) + "' instead of None")
            self._checkBltableVar(outfile, bloutput, self.blparam_dic, option)

    def test303(self):
        """test303: testing shortening baseline table for poly,chebyshev,cspline"""
        self.tid = '303'
        infile = self.infile
        datacolumn = 'float_data'
        spw = ''
        blmode = 'fit'
        blformat = 'table'
        dosubtract = True
        blfunc = ['poly', 'chebyshev', 'cspline']
        order = 5
        npiece = 4
        with table_manager(infile) as tb:
            nrow_data = tb.nrows()
        testmode = ['masked_masked', 'masked_unselect', 'unselect_masked']
        prange = [[0, 1], [0], [1]]
        polval = ['', 'RR', 'LL']
        for i in range(len(blfunc)):
            for j in range(len(testmode)):
                print('testing blfunc=' + blfunc[i] + ', testmode=' + testmode[j] + '...')
                # prepare input data
                if os.path.exists(infile):
                    shutil.rmtree(infile)
                shutil.copytree(os.path.join(self.datapath, self.infile), infile)

                with table_manager(infile, nomodify=False) as tb:
                    r2msk = tb.getcell('FLAG', 2)
                    r2msk[prange[j], :] = True
                    tb.putcell('FLAG', 2, r2msk)

                pol = polval[j]
                outfile = self.outroot + self.tid + blfunc[i] + testmode[j] + '.ms'
                bloutput = self.outroot + self.tid + blfunc[i] + testmode[j] + '.bltable'
                result = sdbaseline(infile=infile, datacolumn=datacolumn,
                                    blmode=blmode, blformat=blformat, bloutput=bloutput,
                                    spw=spw, pol=pol, blfunc=blfunc[i], order=order, npiece=npiece,
                                    dosubtract=dosubtract, outfile=outfile)
                self.assertEqual(result, None,
                                 msg="The task returned '" + str(result) + "' instead of None")
                with table_manager(bloutput) as tb:
                    nrow_bltable = tb.nrows()
                self.assertTrue((nrow_bltable == nrow_data - 1),
                                msg="The baseline table is not shortened...")
                # delete used data
                if (os.path.exists(self.infile)):
                    shutil.rmtree(self.infile)
                os.system('rm -rf ' + self.outroot + '*')

    def test304(self):
        """test304: testing shortening baseline table for blfunc=variable"""
        self.tid = '304'
        infile = self.infile
        datacolumn = 'float_data'
        spw = ''
        blmode = 'fit'
        blformat = 'table'
        blfunc = 'variable'
        dosubtract = True
        with table_manager(infile) as tb:
            nrow_data = tb.nrows()
        testmode = ['masked_masked', 'masked_unselect', 'unselect_masked']
        prange = [[0, 1], [0], [1]]
        polval = ['', 'RR', 'LL']
        for j in range(len(testmode)):
            print('testing blfunc=' + blfunc + ', testmode=' + testmode[j] + '...')
            # prepare input data
            if os.path.exists(self.infile):
                shutil.rmtree(self.infile)
            shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

            blparam = self.outroot + '.blparam'
            self._createBlparamFile(blparam, self.blparam_order, self.blparam_dic, '')

            tb.open(tablename=infile, nomodify=False)
            r2msk = tb.getcell('FLAG', 2)
            r2msk[prange[j], :] = True
            tb.putcell('FLAG', 2, r2msk)
            tb.close()
            pol = polval[j]

            outfile = self.outroot + self.tid + blfunc + '.ms'
            bloutput = self.outroot + self.tid + blfunc + '.bltable'
            sdbaseline(infile=infile, datacolumn=datacolumn,
                       blmode=blmode, blformat=blformat, bloutput=bloutput,
                       spw=spw, pol=pol, blfunc=blfunc, blparam=blparam,
                       dosubtract=dosubtract, outfile=outfile)
            with table_manager(bloutput) as tb:
                nrow_bltable = tb.nrows()
            self.assertTrue((nrow_bltable == nrow_data - 1),
                            msg="The baseline table is not shortened...")
            # delete used data
            if (os.path.exists(self.infile)):
                shutil.rmtree(self.infile)
            os.system('rm -rf ' + self.outroot + '*')

    def test305(self):
        """test305: sinusoid, per-spectrum baselining, output bltable is created"""
        self.tid = '305'
        infile = self.sin_infile
        datacolumn = 'float_data'
        blmode = 'fit'
        blformat = 'table'
        blfunc = 'variable'
        dosubtract = True
        option = 'variable_sinusoid'
        blparam = 'blparam_variable_sin.txt'

        bloutput = self.outroot + self.tid + option + '.bltable'
        outfile = self.outroot + self.tid + option + '.ms'
        result = sdbaseline(infile=infile, datacolumn=datacolumn,
                            blmode=blmode, blformat=blformat, bloutput=bloutput,
                            blfunc=blfunc, blparam=blparam,
                            dosubtract=dosubtract, outfile=outfile)
        self.assertEqual(result, None,
                            msg=f"Expected return code: None, got: {result}")
        with table_manager(self.sin_infile) as tb:
            nrow_data = tb.nrows()
            with table_manager(bloutput) as tb:
                nrow_bltable = tb.nrows()
            self.assertEqual(nrow_bltable, nrow_data,
                            msg="The baseline table is not same size as the data...")



class sdbaseline_applybltableTest(sdbaseline_unittest_base):
    """
    Tests for applying baseline table
    (blmode='apply' mode)

    List of tests
    test400 : MS with no all-channel-flagged, bltable with apply=True for all spectra
    test401 : MS with one spectrum with all channels flagged, while apply=True throughout bltable
    test402 : MS with no all-channel-flagged, while apply=False for one spectrum in bltable
    test403 : MS with no all-channel-flagger, while bltable lacks one row (irow=2)

    Note: for tests401-403, the spectrum with all channels flagged, or the corresponding
    data in baseline table has apply=False or is inexist, should not be subtracted baseline.
    """
    # Input and output names
    infile = 'OrionS_rawACSmod_calave.ms'
    outroot = sdbaseline_unittest_base.taskname + '_bltabletest'
    reffile = outroot + '.ms'
    blmode = 'apply'
    bltable = outroot + '.bltable'
    tid = None

    def setUp(self):
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

        if os.path.exists(self.infile + '_blparam.txt'):
            os.remove(self.infile + '_blparam.txt')
        if os.path.exists(self.infile + '_blparam.csv'):
            os.remove(self.infile + '_blparam.csv')
        if os.path.exists(self.infile + '_blparam.btable'):
            shutil.rmtree(self.infile + '_blparam.btable')

        # create baseline table
        blparam = self.outroot + '.blparam'
        self._createBlparamFile(blparam, self.blparam_order, self.blparam_dic, '')
        result = sdbaseline(infile=self.infile, datacolumn='float_data',
                            blmode='fit', blformat='table', bloutput=self.bltable,
                            blfunc='variable', blparam=blparam,
                            dosubtract=True, outfile=self.reffile)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")

    def tearDown(self):
        remove_single_file_dir(self.infile)
        remove_files_dirs(self.outroot)

    def _checkResult(self, outfile, option):
        npol = 2
        with table_manager(outfile) as tb:
            out_spec = tb.getcol('FLOAT_DATA')
            out_flag = tb.getcol('FLAG')
        with table_manager(self.reffile) as tb:
            ref_spec = tb.getcol('FLOAT_DATA')
            ref_flag = tb.getcol('FLAG')
        with table_manager(self.infile) as tb:
            in_spec = tb.getcol('FLOAT_DATA')
            # in_flag = tb.getcol('FLAG')
            nrow = tb.nrows()
            nchan = len(in_spec[0][0])

        for ipol in range(npol):
            for ichan in range(nchan):
                for irow in range(nrow):
                    outspec = out_spec[ipol][ichan][irow]
                    outflag = out_flag[ipol][ichan][irow]
                    if ((option == 'r2p1msflagged') and (irow == 2) and (ipol == 1)) or \
                       ((option == 'r2p1bltnotapply') and (irow == 2) and (ipol == 1)) or \
                       ((option == 'r2p1bltinexist') and (irow == 2)):
                        ansspec = in_spec[ipol][ichan][irow]
                        ansflag = True
                    else:
                        ansspec = ref_spec[ipol][ichan][irow]
                        ansflag = ref_flag[ipol][ichan][irow]

                    self.assertTrue(abs(outspec - ansspec) < 1e-6, msg='spec: result != answer')
                    self.assertEqual(outflag, ansflag, msg='flag: result != answer')

    def test400(self):
        """test400: apply baseline table. all bltable entries applied to all MS data."""
        self.tid = '400'
        outfile = self.outroot + self.tid + '.ms'
        result = sdbaseline(infile=self.infile, datacolumn='float_data',
                            blmode=self.blmode, bltable=self.bltable,
                            outfile=outfile)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        self._checkResult(outfile, '')

    def test400_uppercase_params(self):
        """apply baseline table with blmode='APPLY'. all bltable entries applied to all MS data."""
        self.tid = '400'
        outfile = self.outroot + self.tid + '.ms'
        result = sdbaseline(infile=self.infile, datacolumn='float_data',
                            blmode=self.blmode.upper(), bltable=self.bltable,
                            outfile=outfile)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        self._checkResult(outfile, '')

    def test401(self):
        """test401: apply baseline table to MS with a spectrum totally flagged."""
        self.tid = '401'
        outfile = self.outroot + self.tid + '.ms'
        try:
            tb.open(tablename=self.infile, nomodify=False)
            tmpflag = tb.getcell('FLAG', 2)
            for ichan in range(len(tmpflag[0])):
                tmpflag[1][ichan] = True
            tb.putcell('FLAG', 2, tmpflag)
        finally:
            tb.close()

        result = sdbaseline(infile=self.infile, datacolumn='float_data',
                            blmode=self.blmode, bltable=self.bltable,
                            outfile=outfile)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        self._checkResult(outfile, 'r2p1msflagged')

    def test402(self):
        """test402: apply baseline table containing apply=False data."""
        self.tid = '402'
        outfile = self.outroot + self.tid + '.ms'

        try:
            tb.open(tablename=self.bltable, nomodify=False)
            tmpapply = tb.getcell('APPLY', 2)
            tmpapply[1] = False
            tb.putcell('APPLY', 2, tmpapply)
        finally:
            tb.close()

        result = sdbaseline(infile=self.infile, datacolumn='float_data',
                            blmode=self.blmode, bltable=self.bltable,
                            outfile=outfile)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        self._checkResult(outfile, 'r2p1bltnotapply')

    def test403(self):
        """test403: apply baseline table lacking data for a spectrum in MS."""
        self.tid = '403'
        outfile = self.outroot + self.tid + '.ms'

        try:
            tb.open(tablename=self.bltable, nomodify=False)
            tb.removerows([2])
            self.assertEqual(tb.nrows(), 3, msg='failed to remove a row in bltable.')
        finally:
            tb.close()

        result = sdbaseline(infile=self.infile, datacolumn='float_data',
                            blmode=self.blmode, bltable=self.bltable,
                            outfile=outfile)
        self.assertEqual(result, None,
                         msg="The task returned '" + str(result) + "' instead of None")
        self._checkResult(outfile, 'r2p1bltinexist')


    def extractResultFromFile(self, name):
        # Extract FLOAT_DATA from a single file
        with table_manager(name) as tb:
            data = tb.getcol('FLOAT_DATA')
        return data

    def extractResult(self, names):
        # Use a generator expression to apply extractResultFromFile for each name and return the results as a tuple
        return tuple(self.extractResultFromFile(name) for name in names)

    def modifyLines(self, line, inverse=False, replacePart=False):
        parts = line.split(',', 1)  # Split only at the first comma

        if len(parts) > 1:
            if parts[0].isdigit():  # Check if the first part is a number
                if inverse:
                    # Modify the second column (POL) to flip between 0 and 1
                    parts_list = line.split(',')
                    if len(parts_list) > 1:
                        if parts_list[1].strip() == '0':  # If POL is '0'
                            parts_list[1] = '1'
                        elif parts_list[1].strip() == '1':  # If POL is '1'
                            parts_list[1] = '0'
                    return ','.join(parts_list)
                else:
                    # Default behavior: Replace the first number with '2'
                    parts[0] = '2'

        # Replace [1,3] with [1,3,5,12] if needed
        if replacePart:
            line = line.replace('[1,3]', '[1,3,5,12]')

        return line

    def mergeBlparam(self, names, inverse=False, replacePart=False):
        # Open the new file in write mode
        with open(names[2], 'w') as mf:
            # Read and write contents of the first file
            with open(names[1], 'r') as f1:
                mf.write(f1.read())

            # Read, modify, and write contents of the second file
            with open(names[0], 'r') as f2:
                for line in f2:
                    modified_line = self.modifyLines(line, inverse=inverse,
                                                     replacePart=replacePart)
                    mf.write(modified_line)

    def prepareFiles(self, infiles):
        for file in infiles:
            shutil.rmtree(file, ignore_errors=True)
            path = os.path.join(self.datapath, file)
            if os.path.isfile(self.datapath+file):
                shutil.copyfile(path, file)
            else:
                shutil.copytree(path, file)


    def test404(self):
        """test404: apply baseline table for all fitting strategies.
        Irregular shape number of waves."""
        try:
            self.tid = '404'
            infiles = ['analytic_order3_withoffset.ms', 'sinusoidal.ms']
            ms_outfiles = [self.outroot + self.tid + '.ms',
                            self.outroot + self.tid + 'apply' + '.ms']
            bltable_outfile = self.outroot + self.tid + '.bl'
            blparam_files = ['blparam_variable_sin.txt',
                            'analytic_variable_blparam_mask.txt']
            merged_blparam = 'merged_blparam.txt'
            self.prepareFiles(infiles+blparam_files)
            # Combine the data
            with table_manager(infiles[1], nomodify=False) as tb:
                tb.copyrows(infiles[0])
            # Construct joined blparam.txt
            self.mergeBlparam(blparam_files + [merged_blparam])
            # Since the data was copied directly to analytic_order3_withoffset
            # use this file
            sdbaseline(infile=infiles[0], datacolumn='float_data',
                        blmode='fit', blfunc='variable', bloutput=bltable_outfile,
                        outfile=ms_outfiles[0], blformat='table',
                        blparam=merged_blparam)
            # Test apply mode for combined data and blparam
            result = sdbaseline(infile=infiles[0], datacolumn='float_data',
                                blmode=self.blmode,
                                bltable=bltable_outfile, outfile=ms_outfiles[1])
            self.assertIsNone(result,
                            msg="The task returned '" + str(result) +
                            "' instead of None")
            col_fit, col_apply = self.extractResult(ms_outfiles)

            self.assertTrue(
                np.allclose(col_fit, col_apply, atol=1e-6, rtol=0),
                msg=(
                    "The difference is over the tolerance limit\n"
                    f"Max abs diff: {np.max(np.abs(col_fit - col_apply))}\n"
                    f"Mean abs diff: {np.mean(np.abs(col_fit - col_apply))}\n"
                    "Tolerance: 1e-6"
                )
            )

        finally:
            # Clean up
            for ms in (infiles + blparam_files + [merged_blparam]):
                remove_single_file_dir(ms)

    def test405(self):
        """test405: apply baseline table for all fitting strategies.
        Irregular shape number of waves(inversed)."""
        try:
            self.tid = '405'
            infiles = ['analytic_order3_withoffset.ms', 'sinusoidal.ms']
            ms_outfiles = [self.outroot + self.tid + '.ms',
                            self.outroot + self.tid + 'apply' + '.ms']
            bltable_outfile = self.outroot + self.tid + '.bl'
            blparam_files = ['blparam_variable_sin.txt',
                            'analytic_variable_blparam_mask.txt']
            merged_blparam = 'merged_blparam.txt'
            self.prepareFiles(infiles+blparam_files)
            # Combine the data
            with table_manager(infiles[1], nomodify=False) as tb:
                tb.copyrows(infiles[0])
            # Construct joined blparam.txt
            self.mergeBlparam(blparam_files + [merged_blparam],
                                                inverse=True)
            # Since the data was copied directly to analytic_order3_withoffset
            # use this file
            sdbaseline(infile=infiles[0], datacolumn='float_data',
                        blmode='fit', blfunc='variable', bloutput=bltable_outfile,
                        outfile=ms_outfiles[0], blformat='table',
                        blparam=merged_blparam)
            # Test apply mode for combined data and blparam
            result = sdbaseline(infile=infiles[0], datacolumn='float_data',
                                blmode=self.blmode,
                                bltable=bltable_outfile, outfile=ms_outfiles[1])
            self.assertIsNone(result,
                            msg="The task returned '" + str(result) +
                            "' instead of None")
            col_fit, col_apply = self.extractResult(ms_outfiles)

            self.assertTrue(
                np.allclose(col_fit, col_apply, atol=1e-6, rtol=0),
                msg=(
                    "The difference is over the tolerance limit\n"
                    f"Max abs diff: {np.max(np.abs(col_fit - col_apply))}\n"
                    f"Mean abs diff: {np.mean(np.abs(col_fit - col_apply))}\n"
                    "Tolerance: 1e-6"
                )
            )

        finally:
            # Clean up
            for ms in (infiles + blparam_files + [merged_blparam]):
                remove_single_file_dir(ms)

    def test406(self):
        """test406: apply baseline table for all fitting strategies.
        Same shape."""
        try:
            self.tid = '405'
            infiles = ['analytic_order3_withoffset.ms', 'sinusoidal.ms']
            ms_outfiles = [self.outroot + self.tid + '.ms',
                            self.outroot + self.tid + 'apply' + '.ms']
            bltable_outfile = self.outroot + self.tid + '.bl'
            blparam_files = ['blparam_variable_sin.txt',
                            'analytic_variable_blparam_mask.txt']
            merged_blparam = 'merged_blparam.txt'
            self.prepareFiles(infiles+blparam_files)
            # Combine the data
            with table_manager(infiles[1], nomodify=False) as tb:
                tb.copyrows(infiles[0])
            # Construct joined blparam.txt
            self.mergeBlparam(blparam_files + [merged_blparam],
                                replacePart = True)
            # Since the data was copied directly to analytic_order3_withoffset
            # use this file
            sdbaseline(infile=infiles[0], datacolumn='float_data',
                        blmode='fit', blfunc='variable', bloutput=bltable_outfile,
                        outfile=ms_outfiles[0], blformat='table',
                        blparam=merged_blparam)
            # Test apply mode for combined data and blparam
            result = sdbaseline(infile=infiles[0], datacolumn='float_data',
                                blmode=self.blmode,
                                bltable=bltable_outfile, outfile=ms_outfiles[1])
            self.assertIsNone(result,
                            msg="The task returned '" + str(result) +
                            "' instead of None")
            col_fit, col_apply = self.extractResult(ms_outfiles)

            self.assertTrue(
                np.allclose(col_fit, col_apply, atol=1e-6, rtol=0),
                msg=(
                    "The difference is over the tolerance limit\n"
                    f"Max abs diff: {np.max(np.abs(col_fit - col_apply))}\n"
                    f"Mean abs diff: {np.mean(np.abs(col_fit - col_apply))}\n"
                    "Tolerance: 1e-6"
                )
            )

        finally:
            # Clean up
            for ms in (infiles + blparam_files + [merged_blparam]):
                remove_single_file_dir(ms)


class sdbaseline_variableTest(sdbaseline_unittest_base):
    """
    Tests for blfunc='variable'

    List of tests necessary
    00: test baseline subtraction with variable baseline functions and orders
    01: test skipping rows by comment, i.e., lines start with '#' (rows should be flagged)
    02: test skipping rows by non-existent lines in blparam file (rows should be flagged)
    03: test mask selection
    04: test data selection
    05: test clipping
    06: duplicated fitting parameter in blparam file (the last one is adopted)
    10: check if baseline function names are correctly output in text file
    11: check if the numbers of baseline coefficients are correctly output in text/csv
    12: check if the numbers of sinusoid baseline coefficients are correctly output in csv
        and length is same, some differences are acceptable due to ManyLinux nuance
    NOT IMPLEMENTED YET
    * line finder
    * edge flagging
    """
    outfile = 'variable_bl.ms'
    column = 'float_data'
    nspec = 4
    refstat0 = {'max': [0.0] * nspec, 'min': [0.0] * nspec,
                'rms': [0.0] * nspec, 'stddev': [0.0] * nspec}

    def setUp(self):
        if hasattr(self, 'infile'):
            self.__refetch_files(self.infile)

    def tearDown(self):
        remove_files_dirs(os.path.splitext(self.infile)[0])
        remove_single_file_dir(self.paramfile)
        remove_single_file_dir(self.outfile)

    def _refetch_files(self, files, from_dir=None):
        if type(files) == str:
            files = [files]
        self._remove(files)
        self._copy(files, from_dir)

    def __select_stats(self, stats, idx_list):
        """
        Returns a dictionary with selected elements of statistics
        stats    : a dictionary of statistics
        idx_list : a list of indices to select in stats
        """
        ret_dict = {}
        for key in stats.keys():
            ret_dict[key] = [stats[key][idx] for idx in idx_list]
        return ret_dict

    def _run_test(self, infile, reference, mask=None, rtol=1.e-5, atol=1.e-6,
                  flag_spec=(), **task_param):
        """
        Run sdbaseline with mode='variable' and test output MS.

        infile    : input ms name
        reference : reference statistic values in form {'key': [value0, value1, ...], ...}
        mask      : list of masklist to calculate statistics of output MS (None=use all)
        rtol, atol: relative and absolute tolerance of comparison.
        flag_spec : a list of rowid and polid pair whose spectrum should be flagged in output MS
        **task_param : additional parameters to invoke task. blfunc and outfile are predefined.
        """
        self.infile = infile
        sdbaseline(infile=self.infile, blfunc='variable', outfile=self.outfile, **task_param)
        colname = (task_param['datacolumn'] if 'datacolumn' in task_param else 'data').upper()

        # calculate statistics of valid spectrum. Test flagged spectrum.
        ivalid_spec = 0
        ispec = 0
        stats_list = []
        valid_idx = []
        with table_manager(self.outfile) as tb:
            for rowid in range(tb.nrows()):
                data = tb.getcell(colname, rowid)
                flag = tb.getcell('FLAG', rowid)
                npol = len(data)
                for polid in range(npol):
                    if (rowid, polid) in flag_spec:
                        # for flagged rows
                        self.assertTrue(flag[polid].all(),
                                        "row=%d, pol=%d should be flagged" % (rowid, polid))
                    else:
                        spec = data[polid, :]
                        masklist = mask[ivalid_spec] if mask is not None else None
                        stats_list.append(self._calc_stats_of_array(spec, masklist))
                        ivalid_spec += 1
                        valid_idx.append(ispec)
                    ispec += 1
        # shrink reference list if # of processed spectra is smaller than reference (selection)
        if len(stats_list) < len(list(reference.values())[0]):
            self.assertEqual(len(valid_idx), len(stats_list),
                             "Internal error: len(valid_idx)!=len(stats_list)")
            reference = self.__select_stats(reference, valid_idx)

        currstat = self._convert_statslist_to_dict(stats_list)
        # print("cruustat=%s" % str(currstat))
        self._compareStats(currstat, reference, rtol=1.0e-6, atol=1.0e-6)

    def testVariable00(self):
        """Test blfunc='variable' with variable baseline functions and orders"""
        infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        self._run_test(infile, self.refstat0, blparam=self.paramfile, datacolumn=self.column)

    def testVariable01(self):
        """Test blfunc='variable' with skipping rows by comment ('#') (rows should be flagged)"""
        infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam_comment.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        self._run_test(infile, self.refstat0, flag_spec=[(0, 0)],
                       blparam=self.paramfile, datacolumn=self.column)

    def testVariable02(self):
        """Test blfunc='variable' with non-existent lines in blparam file
        (rows should be flagged)
        """
        infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam_2lines.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        self._run_test(infile, self.refstat0, flag_spec=[(0, 0), (1, 1)],
                       blparam=self.paramfile, datacolumn=self.column)

    def testVariable03(self):
        """Test blfunc='variable' with mask selection"""
        infile = 'analytic_order3_withoffset.ms'
        self.paramfile = 'analytic_variable_blparam_mask.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        mask = [[[0, 4000], [6000, 8000]],
                [[0, 5000], [6000, 8000]],
                [[0, 3000], [5000, 8000]], None]
        self._run_test(infile, self.refstat0, mask=mask,
                       blparam=self.paramfile, datacolumn=self.column)

    def testVariable04(self):
        """Test blfunc='variable' with data selection (spw='1')"""
        infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam_spw1.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        self._run_test(infile, self.refstat0, spw='1',
                       blparam=self.paramfile, datacolumn=self.column)

    def testVariable05(self):
        """Test blfunc='variable' with clipping"""
        infile = 'analytic_order3_withoffset.ms'
        self.paramfile = 'analytic_variable_blparam_clip.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        mask = [[[0, 4000], [6000, 8000]],
                [[0, 5000], [6000, 8000]],
                [[0, 3000], [5000, 8000]], None]
        self._run_test(infile, self.refstat0, atol=1.e-5,
                       mask=mask, blparam=self.paramfile, datacolumn=self.column)

    def testVariable06(self):
        """Test blfunc='variable' with duplicated fitting parameters (the last one is adopted)"""
        infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam_duplicate.txt'
        self._refetch_files([infile, self.paramfile], self.datapath)
        self._run_test(infile, self.refstat0, blparam=self.paramfile, datacolumn=self.column)

    def _extract_blfunc_params(self, paramfile):
        blparams = {'func': [], 'pname': [], 'pvalue': []}
        isref = (paramfile == self.paramfile)

        if isref:  # Extract baseline parameters from reference file in csv format
            ref_data = np.genfromtxt(paramfile,
                                     delimiter=',',
                                     names=True,
                                     usecols=('ROW', 'POL', 'BL_TYPE', 'ORDER', 'N_PIECE'),
                                     dtype=None,
                                     encoding='utf-8')

            sorted_refdata = np.sort(ref_data, order=['ROW', 'POL'])

            bltype, order, npiece = [sorted_refdata[col] for col in ['BL_TYPE', 'ORDER', 'N_PIECE']]

            blparams['func'] = bltype.tolist()
            blparams['pname'] = ['npiece' if t == 'cspline' else 'order' for t in bltype]

            is_cspline = (bltype == 'cspline')
            not_cspline = np.logical_not(is_cspline)
            blparams['pvalue'] = np.select([is_cspline, not_cspline], [npiece, order]).tolist()

        else:  # Extract baseline parameters from ad-hoc 'free-format' text file
            blparams_pattern = '^Baseline parameters  Function = (?P<func>[a-z]+)  '\
                               '(?P<pname>[a-z]+) = (?P<pvalue>[0-9]+)'

            with open(paramfile, 'r') as f:
                blparams_rows = re.findall(blparams_pattern, f.read(), re.MULTILINE)

            blparams_csv = '\n'.join([','.join(row) for row in blparams_rows])

            blparams_data = np.genfromtxt(StringIO(blparams_csv),
                                          delimiter=',',
                                          dtype=[('func', '<U10'),
                                                 ('pname', '<U10'),
                                                 ('pvalue', 'i4')])

            for key in blparams.keys():
                blparams[key] = blparams_data[key].tolist()

        return blparams

    def testVariable10(self):
        """Check if baseline function names are correctly output in text file"""
        self.infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam.txt'
        self._refetch_files([self.infile, self.paramfile], self.datapath)
        blformat = 'text'
        bloutput = self.infile + '_blparam.txt'

        sdbaseline(infile=self.infile,
                   datacolumn='float_data',
                   blformat=blformat,
                   bloutput=bloutput,
                   dosubtract=False,
                   blfunc='variable',
                   blparam=self.paramfile)

        blparams_answer = self._extract_blfunc_params(self.paramfile)
        blparams_result = self._extract_blfunc_params(bloutput)
        self.assertDictEqual(blparams_answer, blparams_result,
                             msg='baseline parameter output in text file is wrong.')

    def _get_num_coeff(self, paramfile):
        ncoeffs = []
        isref = (paramfile == self.paramfile)

        if isref:
            blparams = self._extract_blfunc_params(paramfile)
            offsets = [1 if t == 'order' else 3 for t in blparams['pname']]
            ncoeffs = [v + o for v, o in zip(blparams['pvalue'], offsets)]
        else:
            iscsv = (os.path.splitext(paramfile)[1][1:] == 'csv')
            delimiter = ',' if iscsv else None
            with open(paramfile, 'r') as f:
                for line in f.readlines():
                    elems = line.rstrip('\n').split(delimiter)
                    if iscsv:
                        ncoeff = len(elems) - 10
                    else:  # txt
                        if len(elems) < 1:
                            continue
                        if elems[0] != 'p0':
                            continue

                        ncoeff = sum(e.startswith('p') for e in elems)

                    ncoeffs.append(ncoeff)

        return ncoeffs

    def testVariable11(self):
        """Check if the numbers of baseline coefficients are correctly output in text/csv"""
        self.infile = 'analytic_variable.ms'
        self.paramfile = 'analytic_variable_blparam.txt'
        self._refetch_files([self.infile, self.paramfile], self.datapath)

        blformat = ['text', 'csv']
        blformat_ext = ['txt', 'csv']
        bloutput = []
        for ext in blformat_ext:
            bloutput.append(self.infile + '_blparam.' + ext)

        sdbaseline(infile=self.infile,
                   datacolumn='float_data',
                   blformat=blformat,
                   bloutput=bloutput,
                   dosubtract=False,
                   blfunc='variable',
                   blparam=self.paramfile)

        ncoeff_ref = self._get_num_coeff(self.paramfile)
        for blfile in bloutput:
            ext = os.path.splitext(blfile)[1][1:]
            self.assertEqual(ncoeff_ref, self._get_num_coeff(blfile),
                             msg=f'number of baseline coefficients in {ext} file is wrong.')

    def testVariable12(self):
        """Check if the numbers of sinusoid baseline coefficients are correctly output in csv"""
        self.infile = 'sinusoidal.ms'
        self.paramfile = 'blparam_variable_sin.txt'
        output_reference = "variable_sin_bloutput.csv"
        self._refetch_files([self.infile, self.paramfile, output_reference], self.datapath)

        blformat = 'csv'
        bloutput = self.infile + '_blparam.' + 'csv'
        try:
            sdbaseline(infile=self.infile,
                    datacolumn='float_data',
                    blformat=blformat,
                    bloutput=bloutput,
                    dosubtract=False,
                    blfunc='variable',
                    blparam=self.paramfile)

            with open(bloutput, 'r') as file:
                    list_all = [row for row in csv.reader(file)]
                    with open(output_reference, 'r') as ref_file:
                        ref_all = [row for row in csv.reader(ref_file)]
                        # Compare lengths of the lists
                        self.assertEqual(len(list_all), len(ref_all),
                                        msg=f"Output lengths differ: ref={ref_all}, list={list_all}" )

                        for row_ref, row_list in zip(ref_all, list_all):
                            for idx, (val_ref, val_list) in enumerate(zip(row_ref, row_list)):
                                # Skip columns 5 and 6 (zero-based index)
                                if idx == 5 or idx == 6:
                                    continue

                                # Convert string values to floats for other columns
                                try:
                                    float_val_ref = float(val_ref)
                                    float_val_list = float(val_list)
                                except ValueError:
                                    print("Warning: Skipping.. Cannot convert to float - {} {}"
                                          .format(val_ref, val_list))
                                    continue

                        # Compare float values using assertAlmostEqual
                        self.assertAlmostEqual(float_val_ref, float_val_list,
                                                places=6,
                                                msg=f"Values differ: ref={val_ref}, list={val_list}")
        finally:
            remove_single_file_dir(output_reference)

class sdbaseline_bloutputTest(sdbaseline_unittest_base):
    """
    Basic unit tests for task sdbaseline. No interactive testing.

    List of tests:

    test000 : no bloutput cases

    test010 : single bloutput cases
    test011 : single bloutput cases (blformat with extra empty elements)

    test020 : double bloutput cases
    test021 : double bloutput cases (blformat with an extra empty element)

    test030 : triple bloutput cases

    test100 : sinusoid test for finite wavenumber patterns
    test101 : sinusoid test for infinite wavenumber pattern
    """

    infile = 'OrionS_rawACSmod_calave.ms'
    outfile = 'test.ms'
    blparam = 'analytic_variable_blparam.txt'

    blfuncs = ['poly', 'cspline', 'sinusoid', 'variable']
    blout_exts = {'text': '.txt', 'csv': '.csv', 'table': '.bltable'}
    blout_fmts_short = ['text', 'csv']
    wns = ['0', '02', '1']

    blout_default_root = infile + '_blparam'
    blout_defaults = {}
    blout_nondefault_root = infile + '_bloutput_nondefault'
    blout_nondefaults = {}
    ref_blout_root = 'bloutput_'
    ref_blout = {}
    ref_blout_s_root = 'bloutput_sinusoid_addwn012_rejwn'
    ref_blout_s = {}

    for fmt in blout_exts.keys():
        blout_defaults[fmt] = blout_default_root + blout_exts[fmt]
        blout_nondefaults[fmt] = blout_nondefault_root + blout_exts[fmt]

        ref_blout[fmt] = {}
        for fn in blfuncs:
            ref_blout[fmt][fn] = ref_blout_root + fn + blout_exts[fmt]

        ref_blout_s[fmt] = {}
        for wn in wns:
            ref_blout_s[fmt][wn] = ref_blout_s_root + wn + blout_exts[fmt]

    ref_blout101 = 'bloutput_sinusoid_addwnGt4000_rejwn4005.txt'

    base_param = dict(infile=infile,
                      datacolumn='float_data',
                      maskmode='list',
                      outfile=outfile,
                      blparam=blparam)

    def setUp(self):
        self._clear_working_directory()

        self._copy_data(self.infile)
        self._copy_data(self.blparam)

        for fmt in self.blout_fmts_short:
            for fn in self.blfuncs:
                self._copy_data(self.ref_blout[fmt][fn])
            for wn in self.wns:
                self._copy_data(self.ref_blout_s[fmt][wn])

        self._copy_data(self.ref_blout101)

    def tearDown(self):
        self._clear_working_directory()

    def _clear_working_directory(self):
        remove_single_file_dir(self.infile)
        remove_single_file_dir(self.outfile)
        remove_single_file_dir(self.blparam)

        for fmt in self.blout_fmts_short:
            for fn in self.blfuncs:
                remove_single_file_dir(self.ref_blout[fmt][fn])
            for wn in self.wns:
                remove_single_file_dir(self.ref_blout_s[fmt][wn])

        remove_single_file_dir(self.ref_blout101)

        for fmt in self.blout_exts.keys():
            remove_single_file_dir(self.blout_defaults[fmt])
            remove_single_file_dir(self.blout_nondefaults[fmt])

    def exec_sdbaseline(self, **kwargs):
        task_param = self.base_param.copy()
        for key, value in kwargs.items():
            task_param[key] = value
        sdbaseline(**task_param)

    def check_bloutput(self, bloutput):
        for fname in bloutput:
            if fname != '':
                result_exist = os.path.exists(fname)
                self.assertEqual(result_exist, True, msg=fname + 'does not exist!')

    def check_bloutputparam_csv(self, bloutputfile, ref_all):
        with open(bloutputfile, 'r') as file:
            list_all = [row for row in csv.reader(file)]
            self.assertEqual(ref_all, list_all,
                             msg='parameter values of the output csv file are \
                                  not equivalent to referece values!')

    def _copy_data(self, filename):
        remote = os.path.join(self.datapath, filename)
        if os.path.isdir(remote):
            shutil.copytree(remote, filename)
        else:
            shutil.copyfile(remote, filename)

    def _set_actual_bloutput(self, blfmt, blout):
        res = {}
        if blfmt == '':
            res['name'] = None
            res['default'] = None
            res['format'] = None
        else:
            res['name'] = self.blout_defaults[blfmt] if blout == '' else blout
            res['default'] = (blout == '')
            res['format'] = blfmt

        return res

    def _create_test_cases(self, blformat):
        # generate a full list of possible bloutput for given blformat.
        # for example, when blformat=['csv', 'text'], the returned value will be:
        # [ ['',''],['foo.csv',''],['','foo.txt'],['foo.csv','foo.txt'] ]
        # in case blformat has only one element (or a string) like ['csv'],
        # string values '' and 'foo.csv' are added to the returned value.

        bloutputs = []

        if isinstance(blformat, str):
            blformat = [blformat]

        if not isinstance(blformat, list):
            raise Exception('blformat must be list or string!')

        if len(blformat) == 0:
            raise Exception('blformat must have one element at least!')

        for i in range(2 ** len(blformat)):
            bloutput = []
            idxs = [int(v) for v in str(bin(i))[2:].zfill(len(blformat))]
            for j in range(len(idxs)):
                elem = '' if (idxs[j] == 0) or (blformat[j] == '') \
                       else self.blout_nondefaults[blformat[j]]
                bloutput.append(elem)
            if bloutput not in bloutputs:
                bloutputs.append(bloutput)

        if len(blformat) == 1:
            bloutputs.append('')
            bloutputs.append(self.blout_nondefaults[blformat[0]])

        return bloutputs

    def _do_run_test(self, blformat, bloutput):
        # print(f'testing blformat={blformat}, bloutput={bloutput}')

        for blfunc in self.blfuncs:
            result = self.exec_sdbaseline(blfunc=blfunc, blformat=blformat, bloutput=bloutput)
            self.assertIsNone(result, msg=f'invalid return value ({result})')

            actual_bloutput = []
            if isinstance(blformat, str) and isinstance(bloutput, str):
                actual_bloutput.append(self._set_actual_bloutput(blformat, bloutput))
            elif isinstance(blformat, list) and isinstance(bloutput, list):
                self.assertEqual(len(blformat), len(bloutput),
                                 msg=f'{blformat} and {bloutput} have different length!')
                for i in range(len(bloutput)):
                    actual_bloutput.append(self._set_actual_bloutput(blformat[i], bloutput[i]))

            for f in actual_bloutput:
                the_blout = f['name']
                blfmt = f['format']
                if the_blout is None:
                    continue
                self.assertTrue(os.path.exists(the_blout), msg=f'{the_blout} does not exist!')
                if not f['default']:
                    fdef = self.blout_defaults[blfmt]
                    self.assertFalse(os.path.exists(fdef), msg=f'{fdef} exists!')
                if blfmt != 'table':
                    res = filecmp.cmp(the_blout, self.ref_blout[blfmt][blfunc])
                    self.assertTrue(res, msg=f'{the_blout} is not identical with the reference!')

            remove_single_file_dir(self.outfile)
            remove_files_dirs(self.blout_default_root)
            remove_files_dirs(self.blout_nondefault_root)

    def _get_blformat_inclusive(self, blformats):
        for i in range(len(blformats)):
            if isinstance(blformats[i], str):
                blformats[i] = [blformats[i]]

        res = [''] * len(blformats[0])
        for blformat in blformats:
            for i in range(len(blformat)):
                if blformat[i] != '':
                    res[i] = blformat[i]

        if res == ['']:
            res = ['text']
        elif res == ['', '']:
            res = ['text', 'csv']
        elif res == ['', '', '']:
            res = ['text', 'csv', 'table']

        return res

    def _run_test(self, blformats):
        blfmt_incl = self._get_blformat_inclusive(blformats)
        bloutputs = self._create_test_cases(blfmt_incl)
        lst = [(blformat, bloutput) for blformat in blformats for bloutput in bloutputs]
        for blformat, bloutput in lst:
            self._do_run_test(blformat=blformat, bloutput=bloutput)

    def test000(self):
        """no bloutput cases"""
        self._run_test(['', ['']])
        self._run_test([['', '']])
        self._run_test([['', '', '']])

    def test010(self):
        """single bloutput cases"""
        # self._run_test(['text', ['text']])  # tentatively skipped for CAS-13673
        # self._run_test(['csv', ['csv']])  # tentatively skipped for CAS-13674
        self._run_test(['table', ['table']])

    def test011(self):
        """single bloutput cases (blformat with empty elements)"""
        # self._run_test([['', 'csv'], ['text', '']])  # tentatively skipped for CAS-13673
        self._run_test([['', '', 'table']])
        # self._run_test([['', 'text', '']])  # tentatively skipped for CAS-13673
        # self._run_test([['csv', '', '']])  # tentatively skipped for CAS-13674

    @unittest.skip("Not currently part of the test suite")
    def test020(self):
        """double bloutput cases"""
        self._run_test([['table', 'text']])
        self._run_test([['text', 'table']])
        self._run_test([['table', 'csv']])
        self._run_test([['csv', 'table']])
        self._run_test([['text', 'csv']])
        self._run_test([['csv', 'text']])

    @unittest.skip("Not currently part of the test suite")
    def test021(self):
        """double bloutput cases (blformat with an empty element)"""
        self._run_test([['table', 'text', ''], ['table', '', 'csv'], ['', 'text', 'csv']])

    @unittest.skip("Not currently part of the test suite")
    def test030(self):
        """triple bloutput cases"""
        self._run_test([['table', 'text', 'csv']])
        self._run_test([['text', 'table', 'csv']])
        self._run_test([['csv', 'text', 'table']])

    def _run_sinusoid_test(self, rejwn):
        # print(f'testing {rejwn}...')

        blformat = self.blout_fmts_short  # ['text', 'csv']
        result = self.exec_sdbaseline(blfunc='sinusoid', addwn=[0, 1, 2], rejwn=rejwn,
                                      blformat=blformat, bloutput=['', ''])
        self.assertIsNone(result, msg=f'invalid return value ({result})')

        for fmt in blformat:
            the_blout = self.blout_defaults[fmt]
            ref_blout = self.ref_blout_s[fmt][''.join([str(v) for v in rejwn])]
            res = filecmp.cmp(the_blout, ref_blout)
            self.assertTrue(res, msg=f'{the_blout} is not identical with the reference!')

        remove_single_file_dir(self.outfile)
        remove_files_dirs(self.blout_default_root)
        remove_files_dirs(self.blout_nondefault_root)

    def test100(self):
        """confirm if bloutput is correctly output for various wavenumber sets"""
        rejwns = [[0], [0, 2], [1]]
        for rejwn in rejwns:
            self._run_sinusoid_test(rejwn)

    def test101(self):
        """Basic Test 0127: addwn>4000, rejwn4005 test"""

        blfunc = 'sinusoid'
        blformat = ['text', 'csv']
        bloutput = ['', '']
        addwn = '>4000'
        rejwn = [4005]
        spw = '0'
        applyfft = False

        result = self.exec_sdbaseline(blfunc=blfunc, addwn=addwn, rejwn=rejwn, applyfft=applyfft,
                                      blformat=blformat, bloutput=bloutput, spw=spw)
        self.assertIsNone(result, msg=f'invalid return value ({result})')

        the_blout = self.blout_defaults['text']
        res = filecmp.cmp(the_blout, self.ref_blout101)
        self.assertTrue(res, msg=f'{the_blout} is not identical with the reference!')

        remove_single_file_dir(self.outfile)
        remove_files_dirs(self.blout_default_root)
        remove_files_dirs(self.blout_nondefault_root)


class sdbaseline_autoTest(sdbaseline_unittest_base):
    """
    A class that tests maskmode='auto'.

    testAutoPolyNoMask : polynomial fitting using all channels but edge=(500, 500)
    testAutoChebNoMask : Chebyshev polynomial fitting using all channels but edge=(500, 500)
    testAutoCsplNoMask : cspline fitting using all channels but edge=(500, 500)
    testAutoSinuNoMask : sinusoidal fitting using all channels but edge=(500, 500)
    testAutoPolyMaskChan : polynomial fitting using 500~7691 channels (no edge mask)
    testAutoChebMaskChan : Chebyshev polynomial fitting using 500~7691 channels (no edge mask)
    testAutoCsplMaskChan : cspline fitting using 500~7691 channels (no edge mask)
    testAutoSinuMaskChan : sinusoidal fitting using 500~7691 channels (no edge mask)
    testAutoPolyMaskFreq : polynomial fitting using 500~7691 (no edge mask)
    testAutoChebMaskFreq : Chebyshev polynomial fitting using 500~7691 (no edge mask)
    testAutoCsplMaskFreq : cspline fitting using 500~7691 (no edge mask)
    testAutoSinuMaskFreq : sinusoidal fitting using 500~7691 (no edge mask)
    testAutoPolyChanFlag : polynomial fitting of all channels with channel flag in both edge
    testAutoChebChanFlag : Chebyshev fitting of all channels with channel flag in both edge
    testAutoCsplChanFlag : cspline fitting of all channels with channel flag in both edge
    testAutoSinuChanFlag : sinusoidal fitting of all channels with channel flag in both edge
    """
    infile = 'OrionS_rawACSmod_calave.ms'
    outroot = sdbaseline_unittest_base.taskname + '_lftest'
    outfile = outroot + ".ms"
    bloutput = outroot + "_blout"
    base_param = dict(infile=infile,
                      datacolumn='float_data',
                      pol='RR',
                      maskmode='auto',
                      thresh=5.0,
                      avg_limit=16,
                      minwidth=16,
                      outfile=outfile,
                      blformat='csv',
                      bloutput=bloutput)
    edge = [500, 500]
    spw = '2'
    spwchan = '2:500~7691'
    spwfreq = '2:44052975469.940445~44096877113.524124Hz'  # 44052978522~44096874062Hz'
    # in either tests,
    statrange = [[1000, 7191]]
    polystat = {'rms': 0.20170082215673005, 'min': -0.42453908920288086,
                'max': 2.0263485908508301, 'median': 0.0034337043762207031,
                'stddev': 0.20170082215673005}
    chebstat = {'rms': 0.20170082215673005, 'min': -0.42453908920288086,
                'max': 2.0263485908508301, 'median': 0.0034337043762207031,
                'stddev': 0.20170082215673005}
    csplstat = {'rms': 0.20181625130943376, 'min': -0.42370939254760742,
                'max': 2.0274257659912109, 'median': 0.0038695335388183594,
                'stddev': 0.20181625130943376}
    # sinustat = {'max': , 'min': , 'median': , 'rms': , 'stddev': }

    def setUp(self):
        for prevout in glob.glob(self.outroot + '*'):
            if os.path.isdir(prevout):
                shutil.rmtree(prevout)
            else:
                os.remove(prevout)
        if os.path.exists(self.infile):
            shutil.rmtree(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

    def tearDown(self):
        remove_single_file_dir(self.infile)
        remove_files_dirs(self.outroot)

    def flag(self, infile, edge=None, rowidx=None):
        rowflag = True if edge is None else False
        if isinstance(rowidx, int):
            rowidx = [rowidx]

        with table_manager(infile, nomodify=False) as tb:
            if rowidx is None:
                rowidx = range(tb.nrows())
            for idx in rowidx:
                specs = tb.getcell("FLAG", idx)
                if rowflag:
                    specs = True
                else:
                    specs[:, :edge[0]] = True
                    if edge[1] > 0:
                        specs[:, -edge[1]:] = True
                tb.putcell('FLAG', idx, specs)

    def run_test(self, refstat, **kwargs):
        task_param = self.base_param.copy()
        for key, val in kwargs.items():
            task_param[key] = val
        sdbaseline(**task_param)
        outfile = task_param['outfile']
        polid = 0 if task_param['pol'] in ['RR', 'LL'] else None
        currstat = self._getStats(outfile, spw='0', pol=polid,
                                  colname=task_param['datacolumn'].upper(),
                                  mask=self.statrange)
        self._compareStats(currstat[0], refstat)

    def testAutoPolyNoMask(self):
        """polynomial fitting using all channels but edge=[500, 500]"""
        self.run_test(self.polystat, spw=self.spw, edge=self.edge, blfunc='poly')

    def testAutoChebNoMask(self):
        """Chebyshev polynomial fitting using all channels but edge=[500, 500]"""
        self.run_test(self.chebstat, spw=self.spw, edge=self.edge, blfunc='chebyshev')

    def testAutoCsplNoMask(self):
        """cspline fitting using all channels but edge=[500, 500]"""
        self.run_test(self.csplstat, spw=self.spw, edge=self.edge, blfunc='cspline')

#     def testAutoSinuNoMask(self):
#         """sinusoidal fitting using all channels but edge=[500, 500]"""
#         self.run_test(self.sinustat, spw=self.spw, edge=self.edge, blfunc='sinusoid')

    def testAutoPolyMaskChan(self):
        """polynomial fitting using 500~7691 channels (no edge mask)"""
        self.run_test(self.polystat, spw=self.spwchan, edge=[0, 0], blfunc='poly')

    def testAutoChebMaskChan(self):
        """Chebyshev polynomial fitting using 500~7691 channels (no edge mask)"""
        self.run_test(self.chebstat, spw=self.spwchan, edge=[0, 0], blfunc='chebyshev')

    def testAutoCsplMaskChan(self):
        """cspline fitting using 500~7691 channels (no edge mask)"""
        self.run_test(self.csplstat, spw=self.spwchan, edge=[0, 0], blfunc='cspline')

#     def testAutoSinuMaskChan(self):
#         """sinusoidal fitting using 500~7691 channels (no edge mask)"""
#         self.run_test(self.sinustat, spw=self.spwchan, edge=self.noedge, blfunc='sinusoid')

    def testAutoPolyMaskFreq(self):
        """polynomial fitting using 500~7691 channels (no edge mask)"""
        self.run_test(self.polystat, spw=self.spwfreq, edge=[0, 0], blfunc='poly')

    def testAutoChebMaskFreq(self):
        """Chebyshev polynomial fitting using 500~7691 channels (no edge mask)"""
        self.run_test(self.chebstat, spw=self.spwfreq, edge=[0, 0], blfunc='chebyshev')

    def testAutoCsplMaskFreq(self):
        """cspline fitting using 500~7691 channels (no edge mask)"""
        self.run_test(self.csplstat, spw=self.spwfreq, edge=[0, 0], blfunc='cspline')

#     def testAutoSinuMaskFreq(self):
#         """sinusoidal fitting using 500~7691 channels (no edge mask)"""
#         self.run_test(self.sinustat, spw=self.spwfreq, edge=self.noedge, blfunc='sinusoid')

    def testAutoPolyChanFlag(self):
        """polynomial fitting of all channels with channel flag in both edge"""
        self.flag(self.infile, edge=self.edge)
        self.run_test(self.polystat, spw=self.spw, edge=[0, 0], blfunc='poly')

    def testAutoChebChanFlag(self):
        """Chebyshev polynomial of all channels with channel flag in both edge"""
        self.flag(self.infile, edge=self.edge)
        self.run_test(self.chebstat, spw=self.spw, edge=[0, 0], blfunc='chebyshev')

    def testAutoCsplChanFlag(self):
        """cspline fitting of all channels with channel flag in both edge"""
        self.flag(self.infile, edge=self.edge)
        self.run_test(self.csplstat, spw=self.spw, edge=[0, 0], blfunc='cspline')

#     def testAutoSinuChanFlag(self):
#         """sinusoidal fitting of all channels with channel flag in both edge"""
#         self.flag(self.infile,edge=self.edge)
#         self.run_test(self.sinustat, spw=self.spw, edge=self.noedge, blfunc='sinusoid')


class sdbaseline_selectionTest(unittest.TestCase):
    datapath = ctsys_resolve('unittest/sdbaseline/')
    infile = "analytic_type1.bl.ms"
    outfile = "baselined.ms"
    bloutfile = infile + "_blparam.txt"
    common_param = dict(infile=infile, outfile=outfile,
                        maskmode='list', blmode='fit', dosubtract=True,
                        blfunc='poly', order=1)
    selections = dict(intent=("CALIBRATE_ATMOSPHERE#OFF*", [1]),
                      antenna=("DA99", [1]),
                      field=("M1*", [0]),
                      spw=(">6", [1]),
                      timerange=("2013/4/28/4:13:21", [1]),
                      scan=("0~8", [0]),
                      pol=("YY", [1]))
    # baseline mask for each row of MS
    chan_mask = {'float_data': ("0~19;21~127", "0~39;41~127"),
                 'corrected': ("0~59;61~127", "0~79;81~127")}
    # data of line (chan, amp) for each pol and row of MS
    line_data = {'float_data': {'r0': ((20, 50.0), (20, 100.0)),
                                'r1': ((40, 150.0), (40, 200.0))},
                 'corrected': {'r0': ((60, 75.0), (60, 125.0)),
                               'r1': ((80, 175.0), (80, 225.0))}
                 }
    templist = [infile, outfile, bloutfile]
    verbose = False

    def _clearup(self):
        for name in self.templist:
            remove_single_file_dir(name)

    def setUp(self):
        self._clearup()
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

    def tearDown(self):
        self._clearup()

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

    def _get_reference(self, nchan, irow, ipol, datacol):
        line_chan, line_amp = self.line_data[datacol][('r%d' % irow)][ipol]
        reference = np.zeros(nchan)
        reference[line_chan] = line_amp
        if self.verbose:
            print("reference=%s" % str(reference))
        return reference

    def _format_spw_mask(self, datacolumn, sel_param):
        (rowids, polids) = self._get_selected_row_and_pol(sel_param)
        spwstr = "*"
        if sel_param == "spw":
            spwstr = self._get_selection_string(sel_param)['spw']
        if len(rowids) == 1:
            return ("%s:%s" % (spwstr, self.chan_mask[datacolumn][rowids[0]]))
        else:
            spwids = ['6', '7']
            spwstr = ""
            for irow in rowids:
                if len(spwstr) > 0:
                    spwstr = spwstr + ","
                spwstr = spwstr + \
                    ("%s:%s" % (spwids[irow], self.chan_mask[datacolumn][irow]))
            return spwstr

    def run_test(self, sel_param, datacolumn, reindex=True):
        inparams = self._get_selection_string(sel_param)
        inparams['spw'] = self._format_spw_mask(datacolumn, sel_param)
        inparams.update(self.common_param)
        print("task param: %s" % str(inparams))
        sdbaseline(datacolumn=datacolumn, reindex=reindex, **inparams)
        self._test_result(inparams["outfile"], sel_param, datacolumn)

    def _test_result(self, msname, sel_param, dcol, atol=1.e-5, rtol=1.e-5, applymode=False):
        # Make sure output MS exists
        self.assertTrue(os.path.exists(msname), "Could not find output MS")
        # Compare output MS with reference (nrow, npol, and spectral values)
        (rowids, polids) = self._get_selected_row_and_pol(sel_param)
        poltest = (sel_param == "pol")
        if dcol.startswith("float"):
            testcolumn = "FLOAT_DATA"
        else:  # output is in DATA column
            testcolumn = "DATA"
        tb.open(msname)
        try:
            if not applymode:  # normal fit
                self.assertEqual(tb.nrows(), len(rowids),
                                 "Row number wrong %d (expected: %d)" % (tb.nrows(), len(rowids)))
            else:  # in case of apply, rownumber does not change from input MS
                self.assertGreaterEqual(tb.nrows(), np.array(rowids).max(),
                                        'Reference row number is larger than table size.')
            for out_row in range(len(rowids)):
                in_row = rowids[out_row]
                if applymode:
                    out_row = in_row
                sp = tb.getcell(testcolumn, out_row)
                if not poltest:
                    self.assertEqual(sp.shape[0], len(polids),
                                     "Number of pol is wrong in row=%d:  %d (expected: %d)" %
                                     (out_row, len(polids), sp.shape[0]))
                nchan = sp.shape[1]
                for out_pol in range(len(polids)):
                    in_pol = polids[out_pol]
                    reference = self._get_reference(nchan, in_row, in_pol, dcol)
                    if self.verbose:
                        print("data=%s" % str(sp[out_pol]))
                    self.assertTrue(np.allclose(sp[out_pol], reference,
                                                atol=atol, rtol=rtol),
                                    "Baselined data differs at row=%d, pol=%d" % (out_row, out_pol))
        finally:
            tb.close()

    def run_test_apply(self, sel_param, datacolumn, reindex=True):
        """BL table generation + application"""
        inparams = self._get_selection_string(sel_param)
        inparams['spw'] = self._format_spw_mask(datacolumn, sel_param)
        inparams.update(self.common_param)
        outms = inparams['outfile']
        bltable = outms + '.bl.cal'
        print('generate BL table')
        inparams.update(dict(dosubtract=False, blformat='table', bloutput=bltable, outfile=''))
        sdbaseline(datacolumn=datacolumn, reindex=reindex, **inparams)
        self.assertTrue(os.path.exists(bltable), 'Failed to generate BL caltable')
        self.assertFalse(os.path.exists(outms), 'Output MS should not be generated yet.')
        print('apply BL table')
        sdbaseline(datacolumn=datacolumn, reindex=reindex, infile=inparams['infile'],
                   outfile=outms, blmode='apply', bltable=bltable)
        self._test_result(outms, sel_param, datacolumn, applymode=True)

    def testIntentF(self):
        """Test selection by intent (float_data)"""
        self.run_test("intent", "float_data")

    def testIntentC(self):
        """Test selection by intent (corrected)"""
        self.run_test("intent", "corrected")

    def testAntennaF(self):
        """Test selection by antenna (float_data)"""
        self.run_test("antenna", "float_data")

    def testAntennaC(self):
        """Test selection by antenna (corrected)"""
        self.run_test("antenna", "corrected")

    def testFieldF(self):
        """Test selection by field (float_data)"""
        self.run_test("field", "float_data")

    def testFieldC(self):
        """Test selection by field (corrected)"""
        self.run_test("field", "corrected")

    def testSpwF(self):
        """Test selection by spw (float_data)"""
        self.run_test("spw", "float_data")

    def testSpwC(self):
        """Test selection by spw (corrected)"""
        self.run_test("spw", "corrected")

    def testTimerangeF(self):
        """Test selection by timerange (float_data)"""
        self.run_test("timerange", "float_data")

    def testTimerangeC(self):
        """Test selection by timerange (corrected)"""
        self.run_test("timerange", "corrected")

    def testScanF(self):
        """Test selection by scan (float_data)"""
        self.run_test("scan", "float_data")

    def testScanC(self):
        """Test selection by scan (corrected)"""
        self.run_test("scan", "corrected")

    def testPolF(self):
        """Test selection by pol (float_data)"""
        self.run_test("pol", "float_data")

    def testPolC(self):
        """Test selection by pol (corrected)"""
        self.run_test("pol", "corrected")

    def testReindexSpw(self):
        """Test reindex =T/F in spw selection"""
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
                os.remove('%s_blparam.txt' % self.common_param['infile'])

    def testReindexIntent(self):
        """Test reindex =T/F in intent selection"""
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
                os.remove('%s_blparam.txt' % self.common_param['infile'])


class sdbaseline_updateweightTest(sdbaseline_unittest_base):
    """
    Tests for updateweight=True
    to confirm if WEIGHT_SPECTRUM column is removed
    """

    datapath = ctsys_resolve('unittest/sdbaseline/')
    infile = 'uid___A002_X6218fb_X264.ms'
    outroot = sdbaseline_unittest_base.taskname + '_updateweighttest'
    outfile = outroot + '.ms'
    params = {'infile': infile, 'outfile': outfile,
              'intent': 'OBSERVE_TARGET#ON_SOURCE',
              'spw': '9', 'datacolumn': 'data',
              'updateweight': True}

    def setUp(self):
        remove_files_dirs(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)

    def tearDown(self):
        remove_files_dirs(self.infile)
        remove_files_dirs(self.outroot)

    def test000(self):
        with table_manager(self.infile) as tb:
            colnames_in = tb.colnames()
        infile_has_wspec = 'WEIGHT_SPECTRUM' in colnames_in
        self.assertTrue(infile_has_wspec,
                        msg='WEIGHT_SPECTRUM not found in the input data.')

        sdbaseline(**self.params)

        with table_manager(self.outfile) as tb:
            colnames_out = tb.colnames()
        outfile_no_wspec = 'WEIGHT_SPECTRUM' not in colnames_out
        self.assertTrue(outfile_no_wspec,
                        msg='WEIGHT_SPECTRUM is not removed.')


class sdbaseline_updateweightTest2(sdbaseline_unittest_base):
    """
    Tests for updateweight=True cases

    test000 : updateweight=False - WEIGHT column must not be updated
    test010 : updateweight=True, sigmavalue=default('stddev')
    test011 : updateweight=True, sigmavalue=default('stddev'), channels 4500~6500 flagged
    test012 : updateweight=True, sigmavalue=default('stddev'), spw to flag channels 4500-6499
    test020 : updateweight=True, sigmavalue='stddev'
    test021 : updateweight=True, sigmavalue='stddev', channels 4500~6500 flagged in input data
    test022 : updateweight=True, sigmavalue='stddev', spw to flag channels 4500-6499
    test030 : updateweight=True, sigmavalue='rms'
    test031 : updateweight=True, sigmavalue='rms', channels 4500~6500 flagged in input data
    test032 : updateweight=True, sigmavalue='rms', spw to flag channels 4500-6499
    test040 : blfunc='variable'
    test041 : blfunc='variable', channels 4500~6500 flagged in input data
    test042 : blfunc='variable', spw to flag channels 4500-6499
    test050 : blmode='apply'
    test051 : blmode='apply', channels 4500~6500 flagged in input data
    test052 : blmode='apply', spw to flag channels 4500-6499
    test060 : confirm if clipping result is used to compute weights in fit mode
    test061 : confirm if clipping result is used to compute weights in apply mode
    """

    datapath = ctsys_resolve('unittest/sdbaseline/')
    infile = 'analytic_order3_withoffset.ms'
    outroot = sdbaseline_unittest_base.taskname + '_updateweighttest'
    outfile = outroot + '.ms'
    spw = '*:0~4499;6500~8191'
    params_base = {'infile': infile,
                   'outfile': outfile,
                   'intent': 'OBSERVE_TARGET#ON_SOURCE',
                   'datacolumn': 'float_data',
                   'updateweight': True}

    OUTLIER_CHANNEL = 4000
    OUTLIER_VALUE = 100000000.0

    def setUp(self):
        remove_files_dirs(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)
        self.params = copy.deepcopy(self.params_base)

    def tearDown(self):
        remove_files_dirs(self.infile)
        remove_files_dirs(self.outroot)

    def _check_weight_identical(self):
        with table_manager(self.infile) as tb:
            wgt_in = tb.getcol('WEIGHT')
        with table_manager(self.outfile) as tb:
            wgt_out = tb.getcol('WEIGHT')
        self.assertTrue(np.array_equal(wgt_in, wgt_out),
                        msg='WEIGHT column is unexpectedly updated!')

    def _check_weight_values(self, sigmavalue='stddev'):
        """
        Check if the values in the WEIGHT column are identical
        to those calculated per polarisation and per row
        as 1/(sigma(pol, row)^2), where sigma is
        - the standard deviation if sigmavalue is 'stddev',
          in which case sigma^2 is the variance, or
        - the root mean square if sigmavalue is 'rms',
          in which case sigma^2 is the mean square
        calculated over all *valid* spectra
        along the frequency channels axis of (pol, row).
        Note that the values in the WEIGHT column should be
        zero in case all channels are flagged.
        """
        with table_manager(self.outfile) as tb:
            wgt = tb.getcol('WEIGHT')
            data = tb.getcol('FLOAT_DATA')
            flag = tb.getcol('FLAG')
            if 'spw' in self.params.keys():
                flag[:, 4500:6500, :] = True

        mdata = np.ma.masked_array(data, mask=flag)
        if sigmavalue == 'stddev':
            mwgt_ref = 1.0 / np.var(mdata, axis=1)
        elif sigmavalue == 'rms':
            mwgt_ref = 1.0 / np.mean(np.square(mdata), axis=1)
        else:
            raise ValueError("Illegal argument: sigmavalue={}: must be \
                             'stddev' or 'rms'".format(sigmavalue))
        wgt_ref = np.ma.filled(mwgt_ref, fill_value=0.0)

        self.assertTrue(np.allclose(wgt, wgt_ref, rtol=1.0e-2, atol=1.0e-5))

    def run_test(self):
        sdbaseline(**self.params)

        if self.params['updateweight']:
            sigmavalue = self.params['sigmavalue'] if 'sigmavalue' in self.params else 'stddev'
            self._check_weight_values(sigmavalue)
        else:
            self._check_weight_identical()

    def write_param_file(self, param_file):
        params = [[''] * 2 for i in range(2)]
        params[0][0] = '0,0,,0,3.,false,,,,,poly,3,0,[]\n'
        params[0][1] = '0,1,,0,3.,false,,,,,chebyshev,2,0,[]\n'
        params[1][0] = '1,0,,0,3.,false,,,,,cspline,,1,[]\n'
        params[1][1] = '1,1,,0,3.,false,,,,,cspline,,2,[]\n'

        with open(param_file, mode='w') as f:
            for irow in range(len(params)):
                for ipol in range(len(params[0])):
                    f.write(params[irow][ipol])

    def add_mask(self):
        # flag channels from 4500 to 6499 for each spectrum
        with table_manager(self.infile, nomodify=False) as tb:
            flag = tb.getcol('FLAG')
            flag[:, 4500:6500, :] = True
            tb.putcol('FLAG', flag)

    def test000(self):
        """updateweight=False - WEIGHT column must not be updated"""
        self.params.update(updateweight=False)
        self.run_test()

    def test010(self):
        """updateweight=True, sigmavalue=default('stddev')"""
        self.run_test()

    def test011(self):
        """updateweight=True, sigmavalue=default('stddev'), channels 4500~6500 flagged"""
        self.add_mask()
        self.run_test()

    def test012(self):
        """updateweight=True, sigmavalue=default('stddev'), spw to flag channels 4500-6499"""
        self.params.update(spw=self.spw)
        self.run_test()

    def test020(self):
        """updateweight=True, sigmavalue='stddev'"""
        self.params.update(sigmavalue='stddev')
        self.run_test()

    def test021(self):
        """updateweight=True, sigmavalue='stddev', channels 4500~6500 flagged in input data"""
        self.add_mask()
        self.params.update(sigmavalue='stddev')
        self.run_test()

    def test022(self):
        """updateweight=True, sigmavalue='stddev', spw to flag channels 4500-6499"""
        self.params.update(spw=self.spw, sigmavalue='stddev')
        self.run_test()

    def test030(self):
        """updateweight=True, sigmavalue='rms'"""
        self.params.update(sigmavalue='rms')
        self.run_test()

    def test031(self):
        """updateweight=True, sigmavalue='rms', channels 4500~6500 flagged in input data"""
        self.add_mask()
        self.params.update(sigmavalue='rms')
        self.run_test()

    def test032(self):
        """updateweight=True, sigmavalue='rms', spw to flag channels 4500-6499"""
        self.params.update(spw=self.spw, sigmavalue='rms')
        self.run_test()

    def test040(self):
        """blfunc='variable'"""
        self.params.update(blfunc='variable', blparam=self.outroot + '_param.txt')
        self.write_param_file(self.params['blparam'])
        self.run_test()

    def test041(self):
        """blfunc='variable', channels 4500~6500 flagged in input data"""
        self.add_mask()
        self.params.update(blfunc='variable', blparam=self.outroot + '_param.txt')
        self.write_param_file(self.params['blparam'])
        self.run_test()

    def test042(self):
        """blfunc='variable', spw to flag channels 4500-6499"""
        self.params.update(spw=self.spw, blfunc='variable', blparam=self.outroot + '_param.txt')
        self.write_param_file(self.params['blparam'])
        self.run_test()

    def run_apply_test(self):
        bltable = self.infile + '.bltable'
        self.params.update(blformat='table', bloutput=bltable, bltable=bltable)

        # make a baseline table
        self.params.update(blmode='fit', updateweight=False)
        sdbaseline(**self.params)
        self._checkfile(bltable)
        remove_single_file_dir(self.outfile)

        # apply
        self.params.update(blmode='apply', updateweight=True)
        self.run_test()

    def test050(self):
        """blmode='apply'"""
        self.run_apply_test()

    def test051(self):
        """blmode='apply', channels 4500~6500 flagged in input data"""
        self.add_mask()
        self.run_apply_test()

    def test052(self):
        """blmode='apply', spw to flag channels 4500-6499"""
        self.params.update(spw=self.spw)
        self.run_apply_test()

    def _set_data_for_clipping(self):
        # set artificial spectra (flat+outlier) in input MS
        with table_manager(self.infile, nomodify=False) as tb:
            spec = tb.getcell('FLOAT_DATA', 0)  # row 0, shape = (npol, nchan)
            # flat spectrum with (mean, sigma) = (0, 1)
            spec[:, 0::2] = 1.0
            spec[:, 1::2] = -1.0
            spec[:, self.OUTLIER_CHANNEL] = self.OUTLIER_VALUE
            tb.putcell('FLOAT_DATA', 0, spec)

            flag = tb.getcell('FLAG', 0)  # row 0
            flag.fill(False)
            tb.putcell('FLAG', 0, flag)

            return spec, flag

    def _get_reference_weight(self, spec, flag):
        # compute the reference weight value
        flag[:, self.OUTLIER_CHANNEL] = True
        mdata = np.ma.masked_array(spec, mask=flag)

        return 1.0 / np.var(mdata, axis=1)

    def run_clipping_test(self, doapply=False):
        bltable = self.infile + '_blparam.bltable'
        if doapply:
            self.params.update(blmode='fit',
                               blformat='table', bloutput=bltable,
                               dosubtract=False,
                               updateweight=False)

        spec, flag = self._set_data_for_clipping()
        weight_ref = self._get_reference_weight(spec, flag)

        sdbaseline(**self.params)

        if doapply:
            self.params.update(blmode='apply', bltable=bltable,
                               dosubtract=True,
                               updateweight=True)
            sdbaseline(**self.params)

        # value checking
        # the weight values in the output MS should be close to 1
        # if the outlier channel is correctly eliminated by iterative clipping.
        # if clipping result is not taken into account when computing weights,
        # the weight should be a very small value (~ 8 * 10^-13)
        with table_manager(self.outfile) as tb:
            self.assertTrue(np.allclose(tb.getcell('WEIGHT', 0), weight_ref),
                            msg='weight value is wrong.')

    def test060(self):
        """confirm if clipping result is used to compute weights in fit mode"""
        self.params.update(clipniter=5, clipthresh=3.0, updateweight=True,
                           blmode='fit', blfunc='poly', order=0)
        self.run_clipping_test()

    def test061(self):
        """confirm if clipping result is used to compute weights in apply mode"""
        self.params.update(clipniter=5, clipthresh=3.0,
                           blmode='fit', blfunc='poly', order=0)
        self.run_clipping_test(doapply=True)


class sdbaseline_clippingTest(sdbaseline_unittest_base):
    """
    Tests for iterative sigma clipping

    test000 : to confirm if clipping works regardless of blformat when blfunc='poly'
    test001 : to confirm if clipping works regardless of blformat blfunc='cspline'
    test002 : to confirm if clipping works regardless of blformat blfunc='sinusoid'
    test003 : to confirm if clipping works regardless of blformat blfunc='variable'

    test010 : clipping runs multiple times (positive spikes only, threshold=3sigma)
    test011 : clipping runs multiple times (positive spikes only, threshold=10sigma)
    test012 : clipping runs multiple times (negative spikes only)
    test013 : clipping runs multiple times (both positive/negative spikes)

    test020 : clipping does run but actually no data clipped (huge threshold)
    test021 : clipping does run but actually no data clipped (no spike)
    """

    datapath = ctsys_resolve('unittest/sdbaseline/')
    infile = 'analytic_order3_withoffset.ms'
    outroot = sdbaseline_unittest_base.taskname + '_clippingtest'
    outfile = outroot + '.ms'
    csvfile = infile + '_blparam.csv'
    blparamfile = outroot + '.blparam'
    params = {'infile': infile, 'outfile': outfile,
              'overwrite': True,
              'intent': 'OBSERVE_TARGET#ON_SOURCE',
              'datacolumn': 'float_data',
              'blmode': 'fit',
              'order': 0, 'npiece': 1, 'addwn': 0,
              'blparam': blparamfile}
    outdata = {}
    outmask = {}

    def setUp(self):
        remove_files_dirs(self.infile)
        shutil.copytree(os.path.join(self.datapath, self.infile), self.infile)
        self.outdata = {}
        self.outmask = {}

    def tearDown(self):
        remove_files_dirs(self.infile)
        remove_files_dirs(self.outroot)

    def _setup_input_data(self, spikes):
        with table_manager(self.infile, nomodify=False) as tb:
            data = tb.getcell('FLOAT_DATA', 0)
            # flat spectrum with (mean, sigma) = (5, 1)
            data[:, 0::2] = 6.0
            data[:, 1::2] = 4.0
            # outliers
            for chan, value in spikes:
                data[:, chan] = value
            tb.putcell('FLOAT_DATA', 0, data)

    def _set_params(self, blfunc, outbl, clipniter, thres):
        self.params['blfunc'] = blfunc
        self.params['blformat'] = 'csv' if outbl else ''
        self.params['clipniter'] = clipniter
        self.params['clipthresh'] = thres

    def _get_data_name(self, outbl, clipniter):
        name_bl = 'bl' if outbl else 'nobl'
        name_cl = str(clipniter)

        return name_bl + '-' + name_cl

    def _exec_sdbaseline(self, blfunc, outbl, clipniter, thres):
        self._set_params(blfunc, outbl, clipniter, thres)

        if blfunc == 'variable':
            self._create_blparam_file(clipniter, thres)

        sdbaseline(**self.params)

        with table_manager(self.outfile) as tb:
            data_name = self._get_data_name(outbl, clipniter)
            self.outdata[data_name] = tb.getcell('FLOAT_DATA', 0)[0]  # row(spw)=0, pol=0
        if outbl:
            with open(self.csvfile) as f:
                for line in csv.reader(f):
                    if (line[2] == '0') and (line[3] == '0'):  # row(spw)=0, pol=0
                        data_name = self._get_data_name(outbl, clipniter)
                        self.outmask[data_name] = line[5]

            remove_single_file_dir(self.csvfile)

        remove_files_dirs(self.outroot)

    def _result(self, outbl, clipniter):
        return self.outdata[self._get_data_name(outbl, clipniter)]

    def _resmask(self, clipniter):
        return self.outmask[self._get_data_name(True, clipniter)]

    def _create_blparam_file(self, clipniter, thres):
        with open(self.blparamfile, 'w') as f:
            f.write(f'0,0,,{clipniter},{thres},false,,,,,poly,0,,[]')
            f.write(f'0,1,,{clipniter},{thres},false,,,,,chebyshev,0,,[]')
            f.write(f'1,0,,{clipniter},{thres},false,,,,,cspline,,1,[]')
            f.write(f'1,1,,{clipniter},{thres},false,,,,,cspline,,1,[]')

    def _run_test(self, blfunc='poly', spikes=[(4000, 100000.0)], thres=3.0, ifclipped=True):
        self._setup_input_data(spikes=spikes)

        bools = [False, True]
        lst = [(outbl, clipniter) for outbl in bools for clipniter in [0, 1]]
        for outbl, clipniter in lst:
            self._exec_sdbaseline(blfunc, outbl, clipniter, thres)

        # when clipping is turned on, output of sdbaseline must be identical
        # regardless of whether blformat is empty or not
        self.assertTrue(np.allclose(self._result(False, 1), self._result(True, 1)),
                        msg='unexpected result; result differs with different blformat.')
        # when clipping is turned off, output of sdbaseline must be identical
        # regardless of whether blformat is empty or not
        self.assertTrue(np.allclose(self._result(False, 0), self._result(True, 0)),
                        msg='unexpected result; result differs with different blformat.')
        # when clipping is turned on, output of sdbaseline must be different from that
        # without clipping, regardless of whether blformat is empty or not
        if ifclipped:
            for blout in bools:
                self.assertFalse(np.allclose(self._result(blout, 1), self._result(blout, 0)),
                                 msg='unexpected result; clipping is not working.')
        else:
            for blout in bools:
                self.assertTrue(np.allclose(self._result(blout, 1), self._result(blout, 0)),
                                msg='unexpected result; clipping is done.')

    def _run_test_multiple_clipping(self, spikes, thres=3.0):
        # using a data with two spikes with different values.
        # this test is to confirm if the larger spike is clipped in the first turn
        # and if the smaller spike is clipped in the second turn
        # and if no more data is clipped afterwards.

        self._setup_input_data(spikes=[(2000, 1000.0), (4000, 100000.0)])
        maxiter = 5

        bools = [False, True]
        for niter in range(maxiter):
            for outbl in bools:
                self._exec_sdbaseline(blfunc='poly', outbl=outbl, clipniter=niter, thres=thres)

            # checking for every iteration if output spectrum is identical
            # regardless of whether bloutput is empty or not
            self.assertTrue(np.allclose(self._result(False, niter), self._result(True, niter)),
                            msg='unexpected result; result differs with different blformat.')

        # checking for every iteration if output mask is correct
        answer_mask_before = '[[0;8191]]'
        self.assertEqual(self._resmask(0), answer_mask_before)
        answer_mask_after_1clip = '[[0;3999];[4001;8191]]'  # channel 4000 is clipped
        self.assertEqual(self._resmask(1), answer_mask_after_1clip)
        answer_mask_after_2clip = '[[0;1999];[2001;3999];[4001;8191]]'  # channel 2000 is clipped
        for i in range(2, maxiter):
            self.assertEqual(self._resmask(i), answer_mask_after_2clip)

    def test000(self):
        """confirm if clipping works regardless of blformat when blfunc='poly'"""
        self._run_test(blfunc='poly')

    def test001(self):
        """confirm if clipping works regardless of blformat blfunc='cspline'"""
        self._run_test(blfunc='cspline')

    def test002(self):
        """confirm if clipping works regardless of blformat blfunc='sinusoid'"""
        self._run_test(blfunc='sinusoid')

    def test003(self):
        """confirm if clipping works regardless of blformat blfunc='variable'"""
        self._run_test(blfunc='variable')

    def test010(self):
        """clipping runs multiple times (positive spikes only, threshold=3sigma)"""
        self._run_test_multiple_clipping(spikes=[(2000, 1000.0), (4000, 100000.0)])

    def test011(self):
        """clipping runs multiple times (positive spikes only, threshold=10sigma)"""
        self._run_test_multiple_clipping(spikes=[(2000, 1000.0), (4000, 100000.0)], thres=10.0)

    def test012(self):
        """clipping runs multiple times (negative spikes only)"""
        self._run_test_multiple_clipping(spikes=[(2000, -1000.0), (4000, -100000.0)])

    def test013(self):
        """clipping runs multiple times (both positive/negative spikes)"""
        self._run_test_multiple_clipping(spikes=[(2000, -1000.0), (4000, 100000.0)])

    def test020(self):
        """clipping does run but actually no data clipped (huge threshold)"""
        self._run_test(thres=100.0, ifclipped=False)

    def test021(self):
        """clipping does run but actually no data clipped (no spike)"""
        self._run_test(thres=3.0, spikes=[], ifclipped=False)


class sdbaseline_helperTest(sdbaseline_unittest_base):
    """
    Tests for helper functions

    test000 : tests for is_empty()
    test010 : tests for parse_wavenumber_param()
    test020 : tests for check_fftthresh()
    """

    def test000(self):
        """tests for is_empty()"""
        print("Testing a helper function is_empty() with")

        # right cases
        blformats = [None, '', [], ['', '', '']]
        for blformat in blformats:
            print(f"    blformat='{blformat}'...")
            self.assertTrue(is_empty(blformat))

        # wrong cases
        blformats = ['text', 'csv', 'table',
                     ['text'], ['csv'], ['table'],
                     ['text', ''], ['', 'table'],
                     ['text', 'csv'], ['text', 'table'], ['csv', 'table'],
                     ['text', 'csv', ''], ['text', '', 'table'], ['', 'csv', 'table'],
                     ['text', 'csv', 'table'],
                     ['text', 'csv', 'table', ''], ['', 'text', 'csv', 'table']]
        for blformat in blformats:
            print(f"    blformat='{blformat}'...")
            self.assertFalse(is_empty(blformat))

    def test010(self):
        """tests for parse_wavenumber_param()"""
        test_cases = [([1, 2, 3], '1,2,3'),
                      ([1, 3, 2], '1,2,3'),
                      ([3, 2, 1], '1,2,3'),
                      ([3, 1, 3], '1,3'),
                      ([-5, 1, 2], 'ERROR'),
                      ((3, 2, 1), '1,2,3'),
                      ((4, 1, 4), '1,4'),
                      ((-5, 1, 2), 'ERROR'),
                      (5, '5'),
                      (0, '0'),
                      (-6, 'ERROR'),
                      (7.0, 'ERROR'),
                      (True, 'ERROR'),
                      ('5', '5'),
                      ('0', '0'),
                      ('-6', 'ERROR'),
                      ('7.0', 'ERROR'),
                      ('1, 2, 3', '1,2,3'),
                      ('3, 2, 1', '1,2,3'),
                      ('3, 1, 3', '1,3'),
                      ('-5, 1, 2', 'ERROR'),
                      ('2-5', '2,3,4,5'),
                      ('3~6', '3,4,5,6'),
                      ('<=3', '0,1,2,3'),
                      ('=<4', '0,1,2,3,4'),
                      ('5>=', '0,1,2,3,4,5'),
                      ('6=>', '0,1,2,3,4,5,6'),
                      ('<3', '0,1,2'),
                      ('4>', '0,1,2,3'),
                      ('>=3', '3,-999'),
                      ('=>4', '4,-999'),
                      ('5<=', '5,-999'),
                      ('6=<', '6,-999'),
                      ('>3', '4,-999'),
                      ('4<', '5,-999')]

        print("Testing a helper function parse_wavenumber_param() with")
        for (wn, answer) in test_cases:
            print(f"    wn='{wn}'...")
            if answer == 'ERROR':
                with self.assertRaises(ValueError, msg="wrong value given for addwn/rejwn"):
                    parse_wavenumber_param(wn)
            else:
                self.assertEqual(answer, parse_wavenumber_param(wn))

    def test020(self):
        """tests for check_fftthresh()"""
        print("Testing a helper function check_fftthresh() with")

        # right cases
        test_cases = [3, 3.0, 'top4', '5sigma', '5.0sigma']
        for fftthresh in test_cases:
            print(f"    fftthresh='{fftthresh}'...")
            try:
                check_fftthresh(fftthresh)
            except Exception as e:
                print("Unexpected error!")
                raise e

        # wrong cases
        test_cases = [{'fftthresh': [0, 0.0, -3, -3.0, 'top-4', '-5sigma', '-5.0sigma'],
                       'errmsg': "threshold given to fftthresh must be positive."},
                      {'fftthresh': ['fivesigma'],
                       'errmsg': "fftthresh has a wrong format."},
                      {'fftthresh': [None, True, ['3.0'], ('top4',)],
                       'errmsg': "fftthresh must be float or integer or string."}]
        for test_case in test_cases:
            for fftthresh in test_case['fftthresh']:
                print(f"    fftthresh='{fftthresh}'...")
                with self.assertRaises(ValueError, msg=test_case['errmsg']):
                    check_fftthresh(fftthresh)


if __name__ == '__main__':
    unittest.main()
