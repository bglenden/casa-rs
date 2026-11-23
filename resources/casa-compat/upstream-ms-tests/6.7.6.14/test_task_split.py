#########################################################################
# test_task_split.py
#
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.manipulation.split.html
#
# Features tested:
#   1. Are the POLARIZATION, DATA_DESCRIPTION, and (to some extent) the
#      SPECTRAL_WINDOW tables correct with and without correlation selection?
#   2. Are the data shapes and values correct with and without correlation
#      selection?
#   3. Are the WEIGHT and SIGMA shapes and values correct with and without
#      correlation selection?
#   4. Is a SOURCE table with bogus entries properly handled?
#   5. Is the STATE table properly handled?
#   6. Are generic subtables copied over?
#   7. Are CHAN_WIDTH and RESOLUTION properly handled in SPECTRAL_WINDOW when
#      channels are being selected and/or averaged?
#   8. The finer points of spw:chan selection.
#
##########################################################################
import os
import numpy
import re
import sys
import shutil
import filecmp
import unittest

from casatasks import cvel, flagcmd, flagdata, importasdm, listobs, partition, split
from casatools import ctsys, ms, msmetadata, table
from casatasks.private.parallel.parallel_task_helper import ParallelTaskHelper

from casatestutils import testhelper as th

datapath = ctsys.resolve('unittest/split/')

# To run test_split with MMS, create MMS from the MSs used in these tests and save them
# to a directory in your system that is called unittest/split/. The ctsys.resolve() used
# in these tests will find the MMS there. Check the --rcdir option of casa to point to
# a local config.py for this.

'''
    Start of old tests, which are the same as test_split.
    class SplitFlags on are new tests.
'''

# local copy of tool
tblocal = table()

def check_eq(val, expval, tol=None):
    """Checks that val matches expval within tol."""
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
                raise ValueError('value != expected')
        except ValueError:
            errmsg = "%r != %r" % (val, expval)
            if (len(errmsg) > 66): # 66 = 78 - len('ValueError: ')
                errmsg = "\n%r\n!=\n%r" % (val, expval)
            raise ValueError(errmsg)
        except Exception:
            print("Error comparing", val, "to", expval)
            raise

def slurp_table(tabname):
    """
    Returns a dictionary containing the CASA table tabname.  The dictionary
    is arranged like:

    {'keywords': tblocal.getkeywords(),
     'cols': {colname0, {'desc': tblocal.getcoldesc(colname0),
                         'keywords': tblocal.getcolkeywords(colname0),
                         'data': tblocal.getcol(colname0)},
              colname1, {'desc': tblocal.getcoldesc(colname1),
                             'keywords': tblocal.getcolkeywords(colname1),
                         'data': tblocal.getcol(colname1)},
              ...}}
    """
    tblocal.open(tabname)
    retval = {'keywords': tblocal.getkeywords(),
              'cols': {}}
    cols = tblocal.colnames()
    for col in cols:
        entry = {'desc': tblocal.getcoldesc(col),
                 'keywords': tblocal.getcolkeywords(col)}
        if tblocal.isvarcol(col):
            entry['data'] = tblocal.getvarcol(col)
        else:
            entry['data'] = tblocal.getcol(col)
        retval['cols'][col] = entry
    tblocal.close()
    return retval

def compare_tables(tabname, exptabname, tol=None):
    """
    Raises a ValueError if the contents of tabname are not the same as those
    of exptabname to within tol.
    """
    exptabdict = slurp_table(exptabname)
    tabdict = slurp_table(tabname)

    if set(tabdict['keywords']) != set(exptabdict['keywords']):
        raise ValueError(tabname + ' and ' + exptabname + ' have different keywords')
    if set(tabdict['cols'].keys()) != set(exptabdict['cols'].keys()):
        raise ValueError(tabname + ' and ' + exptabname + ' have different columns')
    for col, tabentry in tabdict['cols'].items():
        if set(tabentry['keywords']) != set(exptabdict['cols'][col]['keywords']):
            raise ValueError(tabname + ' and ' + exptabname + ' have different keywords for column ' + col)

        # Check everything in the description except the data manager.
        for thingy in tabentry['desc']:
            if (
                thingy not in ('dataManagerGroup', 'dataManagerType')
                and tabentry['desc'][thingy]
                != exptabdict['cols'][col]['desc'][thingy]
            ):
                raise ValueError(thingy + ' differs in the descriptions of ' + col + ' in ' + tabname + ' and ' + exptabname)

        check_eq(tabentry['data'], exptabdict['cols'][col]['data'])



class SplitChecker(unittest.TestCase):
    """
    Base class for unit test suites that do multiple tests per split run.
    """
    # Don't setup class variables here - the children would squabble over them.
    #
    # DON'T use numtests or tests_passed as (class) variables.  One of those is
    # a function elsewhere in the testing framework, and the name clash will
    # lead to a cryptic error.
    #
    # DO define a do_split(corrsel) method in each subclass to do the work and
    # record the results.  Any variables that it sets for use by the tests must
    # be class variables, i.e. prefixed by self.__class__..  The tests,
    # however, will refer to them as instance variables.  Example: usually
    # do_split() will set self.__class__.records, and the tests will use it as
    # self.records.  This quirk is a result of unittest.TestCase's preference
    # for starting from scratch, and tearing down afterwards, for each test.
    # That's exactly what SplitChecker is avoiding.

    def setUp(self):
        if self.need_to_initialize:
            self.initialize()

    def tearDown(self):
        """
        Will only clean things up if all the splits have run.
        """
        all_ran = all(self.records.get(corrsel) for corrsel in self.corrsels)

        if all_ran and self.inpms[0] != '/' and os.path.exists(self.inpms):
            shutil.rmtree(self.inpms, ignore_errors=True)

    def initialize(self):
        # The realization that need_to_initialize needs to be
        # a class variable more or less came from
        # http://www.gossamer-threads.com/lists/python/dev/776699
        self.__class__.need_to_initialize = False

        inpms = self.inpms

        if not os.path.exists(inpms):
            # Copying is technically unnecessary for split, but self.inpms is
            # shared by other tests, so making it readonly might break them.
            # Make inpms an already existing path (i.e. datapath + inpms) to
            # disable this copy.
            shutil.copytree(os.path.join(datapath,inpms), inpms)

        if not os.path.exists(inpms):
            raise EnvironmentError("Missing input MS: " + os.path.join(datapath,inpms))

        for corrsel in self.corrsels:
            self.res = self.do_split(corrsel)

    def checkMSes(self, holderdict, dir, files):
        """
        Updates holderdict['msdict'] with a list of (ncorr, nchan)s for
        each MS in dir that matches holderdict['mspat'].
        """
        # Yup, ignore files.  It's just a os.path.walk()ism.
        from glob import glob

        mses = glob(os.path.join(dir, holderdict['mspat']))

        # musthave = holderdict.get('musthave', set([]))
        # use_and = holderdict.get('use_and', False)
        # listall = holderdict.get('listall', False)

        if not holderdict.get('msdict'):  # Initialize it so retval
            holderdict['msdict'] = {}  # can be tied to it.
        retval = holderdict['msdict']

        # needed_items = holderdict.get('needed_items', {})

        mytb = holderdict['mytb']
        incl_ddid = holderdict['incl_ddid']

        def myopen(mytb, whichtab):
            """
            A wrapper around (my)tb.open(whichtab) which is smarter about error
            handling.  It will still throw an exception on an error, but it tries
            to make the message less misleading.
            """
            retval = False
            if not hasattr(mytb, 'open'):
                raise ValueError('mytb is not a tb tool')
            try:
                mytb.open(whichtab)
                retval = True
            except Exception as e:
                # Typically if we are here whichtab is too malformed for
                # mytb to handle, and e is usually "whichtab does not exist",
                # which is usually incorrect.
                if str(e)[-15:] == " does not exist":
                    print("tb could not open", whichtab)
                else:
                    print("Error", e, "from tb.open(", whichtab, ")")
                mytb.close()  # Just in case.
            return retval

        for currms in mses:
            if currms[:2] == './':  # strip off leading ./, if present.
                currms = currms[2:]  # cosmetic.

            if incl_ddid:
                retval[currms] = {}
            else:
                retval[currms] = set([])

            if not myopen(mytb, currms + '/POLARIZATION'):
                break
            num_corrs = mytb.getcol('NUM_CORR')
            mytb.close()

            if not myopen(mytb, currms + '/SPECTRAL_WINDOW'):
                break
            num_chans = mytb.getcol('NUM_CHAN')
            mytb.close()

            if not myopen(mytb, currms + '/DATA_DESCRIPTION'):
                break

            for row in range(mytb.nrows()):
                if not mytb.getcell('FLAG_ROW', row):
                    key = (num_corrs[mytb.getcell('POLARIZATION_ID', row)],
                           num_chans[mytb.getcell('SPECTRAL_WINDOW_ID', row)])
                    if incl_ddid:
                        if key in retval[currms]:
                            retval[currms][key].append(row)
                        else:
                            retval[currms][key] = [row]
                    else:
                        retval[currms].add(key)
            mytb.close()

    def listshapes(self, musthave=[], mspat="*[-_.][Mm][Ss]", combine='or',
                   sortfirst=False, incl_ddid=False):
        """
        Lists the data shapes of the MSes matched by mspat.
        """
        if type(musthave) == str:
            musthave = [s.replace(',', '') for s in musthave.split()]

        holderdict = {'mytb': table()}
        holderdict['incl_ddid'] = incl_ddid

        splitatdoubleglob = mspat.split('**/')
        if len(splitatdoubleglob) > 1:
            if splitatdoubleglob[0] == '':
                splitatdoubleglob[0] = '.'
            holderdict['mspat'] = splitatdoubleglob[1]
            os.path.walk(splitatdoubleglob[0], self.checkMSes, holderdict)
        else:
            holderdict['mspat'] = mspat
            self.checkMSes(holderdict, '', [])
        return holderdict['msdict']

    def check_subtables(self, corrsel, expected):
        """
        Compares the shapes of self.records[corrsel]['ms']'s subtables
        to the ones listed in expected.

        Removes self.records[corrsel]['ms'] afterwards since nothing else
        needs it, and this is the most reliable way to clean up.
        """
        oms = self.records[corrsel]['ms']
        assert self.listshapes(mspat=oms)[oms] == set(expected)
        shutil.rmtree(oms)

@unittest.skip("split_test_tav is skipped")
class split_test_tav(SplitChecker):
    need_to_initialize = True
    inpms = '0420+417.ms'
    if datapath.count('unittest_mms')==1:
        inpms = '0420+417.ms'

    corrsels = ['', 'rr, ll', 'rl, lr', 'rr', 'll']
    records = {}
    #n_tests = 20
    #n_tests_passed = 0

    def do_split(self, corrsel):
        outms = 'tav' + re.sub(',\s*', '', corrsel) + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nTime averaging", self.inpms, corrsel)
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw='', width=1, antenna='',
                             timebin='20s', timerange='',
                             scan='', array='', uvrange='',
                             correlation=corrsel)
            tblocal.open(outms)
            record['data']   = tblocal.getcell('DATA', 2)
            record['weight'] = tblocal.getcell('WEIGHT', 5)
            record['sigma']  = tblocal.getcell('SIGMA', 7)
            tblocal.close()
        except Exception:
            print("Error time averaging and reading", outms)
            raise
        self.__class__.records[corrsel] = record
        return splitran

    def test_sts(self):
        """Subtables, time avg. without correlation selection"""
        self.check_subtables('', [(4, 1)])
        #self.__class__.n_tests_passed += 1

    def test_sts_rrll(self):
        """Subtables, time avg. RR, LL"""
        self.check_subtables('rr, ll', [(2, 1)])
        #self.__class__.n_tests_passed += 1

    def test_sts_rllr(self):
        """Subtables, time avg. RL, LR"""
        self.check_subtables('rl, lr', [(2, 1)])
        #self.__class__.n_tests_passed += 1

    def test_sts_rr(self):
        """Subtables, time avg. RR"""
        self.check_subtables('rr', [(1, 1)])
        #self.__class__.n_tests_passed += 1

    def test_sts_ll(self):
        """Subtables, time avg. LL"""
        self.check_subtables('ll', [(1, 1)])
        #self.__class__.n_tests_passed += 1

    ## # split does not yet return a success value, and exceptions
    ## # are captured.
    ## # But at least on June 10 it correctly exited with an error
    ## # msg for correlation = 'rr, rl, ll'.
    ## def test_abort_on_rrrlll(self):
    ##     """
    ##     Cannot slice out RR, RL, LL
    ##     """
    ##     self.assertFalse(self.doSplit('rr, rl, ll'))

    def test_data(self):
        """DATA[2],   time avg. without correlation selection"""
        check_eq(self.records['']['data'],
                 numpy.array([[ 0.14428490-0.03145669j],
                              [-0.00379944+0.00710297j],
                              [-0.00381106-0.00066403j],
                              [ 0.14404297-0.04763794j]]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_data_rrll(self):
        """DATA[2],   time avg. RR, LL"""
        check_eq(self.records['rr, ll']['data'],
                 numpy.array([[ 0.14428490-0.03145669j],
                              [ 0.14404297-0.04763794j]]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_data_rllr(self):
        """DATA[2],   time avg. RL, LR"""
        check_eq(self.records['rl, lr']['data'],
                 numpy.array([[-0.00379944+0.00710297j],
                              [-0.00381106-0.00066403j]]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_data_rr(self):
        """DATA[2],   time avg. RR"""
        check_eq(self.records['rr']['data'],
                 numpy.array([[ 0.14428490-0.03145669j]]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_data_ll(self):
        """DATA[2],   time avg. LL"""
        check_eq(self.records['ll']['data'],
                 numpy.array([[ 0.14404297-0.04763794j]]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_wt(self):
        """WEIGHT[5], time avg. without correlation selection"""
        check_eq(self.records['']['weight'],
                 numpy.array([143596.34375, 410221.34375,
                              122627.1640625, 349320.625]),
                 1.0)
        #self.__class__.n_tests_passed += 1

    def test_wt_rrll(self):
        """WEIGHT[5], time avg. RR, LL"""
        check_eq(self.records['rr, ll']['weight'],
                 numpy.array([143596.34375, 349320.625]),
                 1.0)
        #self.__class__.n_tests_passed += 1

    def test_wt_rllr(self):
        """WEIGHT[5], time avg. RL, LR"""
        check_eq(self.records['rl, lr']['weight'],
                 numpy.array([410221.34375, 122627.1640625]),
                 1.0)

    def test_wt_rr(self):
        """WEIGHT[5], time avg. RR"""
        check_eq(self.records['rr']['weight'],
                 numpy.array([143596.34375]),
                 1.0)
        #self.__class__.n_tests_passed += 1

    def test_wt_ll(self):
        """WEIGHT[5], time avg. LL"""
        check_eq(self.records['ll']['weight'],
                 numpy.array([349320.625]),
                 1.0)
        #self.__class__.n_tests_passed += 1

    def test_sigma(self):
        """SIGMA[7], time avg. without correlation selection"""
        check_eq(self.records['']['sigma'],
                 numpy.array([0.00168478, 0.00179394,
                              0.00182574, 0.00194404]),
                 0.0001)

    def test_sigma_rrll(self):
        """SIGMA[7], time avg. RR, LL"""
        check_eq(self.records['rr, ll']['sigma'],
                 numpy.array([0.00168478, 0.00194404]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_sigma_rllr(self):
        """SIGMA[7], time avg. RL, LR"""
        check_eq(self.records['rl, lr']['sigma'],
                 numpy.array([0.00179394, 0.00182574]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_sigma_rr(self):
        """SIGMA[7], time avg. RR"""
        check_eq(self.records['rr']['sigma'],
                 numpy.array([0.00168478]),
                 0.0001)

    def test_sigma_ll(self):
        """SIGMA[7], time avg. LL"""
        check_eq(self.records['ll']['sigma'],
                 numpy.array([0.00194404]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

class split_test_cav(SplitChecker):
    need_to_initialize = True
    corrsels = ['', 'rr', 'll']
    inpms = 'ctb80-vsm.ms'
    if datapath.count('unittest_mms')==1:
        inpms = 'ctb80-vsm.ms'

    records = {}
    #n_tests = 12
    #n_tests_passed = 0

    def do_split(self, corrsel):
        outms = 'cav' + re.sub(',\s*', '', corrsel) + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nChannel averaging", corrsel)
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw='0:5~16', width=3,
                             antenna='',
                             timebin='', timerange='',
                             scan='', array='', uvrange='',
                             correlation=corrsel)
            tblocal.open(outms)
            record['data']   = tblocal.getcell('DATA', 2)
            record['weight'] = tblocal.getcell('WEIGHT', 5)
            record['sigma']  = tblocal.getcell('SIGMA', 7)
            tblocal.close()
        except Exception:
            print("Error channel averaging and reading", outms)
            raise
        self.records[corrsel] = record
        return splitran

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_sts(self):
        """Subtables, chan avg. without correlation selection"""
        self.check_subtables('', [(2, 4)])
        #self.__class__.n_tests_passed += 1

    def test_sts_rr(self):
        """Subtables, chan avg. RR"""
        self.check_subtables('rr', [(1, 4)])
        #self.__class__.n_tests_passed += 1

    def test_sts_ll(self):
        """Subtables, chan avg. LL"""
        self.check_subtables('ll', [(1, 4)])
        #self.__class__.n_tests_passed += 1

    def test_data(self):
        """DATA[2],   chan avg. without correlation selection"""
        check_eq(self.records['']['data'],
                 numpy.array([[16.795681-42.226387j, 20.5655-44.9874j,
                               26.801544-49.595020j, 21.4770-52.0462j],
                              [-2.919122-38.427235j, 13.3042-50.8492j,
                                4.483857-43.986446j, 10.1733-19.4007j]]),
                 0.0005)
        #self.__class__.n_tests_passed += 1

    def test_data_rr(self):
        """DATA[2],   chan avg. RR"""
        check_eq(self.records['rr']['data'],
                 numpy.array([[16.79568-42.226387j, 20.5655-44.9874j,
                               26.80154-49.595020j, 21.4770-52.0462j]]),
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_data_ll(self):
        """DATA[2],   chan avg. LL"""
        check_eq(self.records['ll']['data'],
                 numpy.array([[-2.919122-38.427235j, 13.3042-50.8492j,
                                4.483857-43.986446j, 10.1733-19.4007j]]),
                 0.0001)

    def test_wt(self):
        """WEIGHT[5], chan avg. without correlation selection"""
        check_eq(self.records['']['weight'],
                 numpy.array([ 2.75,  2.75]), 0.001)
        #self.__class__.n_tests_passed += 1

    def test_wt_rr(self):
        """WEIGHT[5], chan avg. RR"""
        check_eq(self.records['rr']['weight'],
                 numpy.array([2.75]), 0.001)

    def test_wt_ll(self):
        """WEIGHT[5], chan avg. LL"""
        check_eq(self.records['ll']['weight'],
                 numpy.array([ 2.75]), 0.001)
        #self.__class__.n_tests_passed += 1

    def test_sigma(self):
        """SIGMA[7], chan avg. without correlation selection"""
        check_eq(self.records['']['sigma'],
                 numpy.array([ 0.60978937,  0.60978937]), 0.0001)

    def test_sigma_rr(self):
        """SIGMA[7], chan avg. RR"""
        check_eq(self.records['rr']['sigma'],
                 numpy.array([0.60978937]), 0.0001)

    def test_sigma_ll(self):
        """SIGMA[7], chan avg. LL"""
        check_eq(self.records['ll']['sigma'],
                 numpy.array([ 0.60978937]), 0.0001)
        #self.__class__.n_tests_passed += 1

class split_test_cav5(SplitChecker):
    need_to_initialize = True
    corrsels = ['', 'll']
    inpms = 'ctb80-vsm.ms'
    if datapath.count('unittest_mms')==1:
        inpms = 'ctb80-vsm.ms'

    records = {}
    #n_tests = 12
    #n_tests_passed = 0

    def do_split(self, corrsel):
        outms = 'cav' + re.sub(',\s*', '', corrsel) + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nChannel averaging", corrsel)
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw='0:5~16', width=5,
                             antenna='',
                             timebin='', timerange='',
                             scan='', array='', uvrange='',
                             correlation=corrsel)
            tblocal.open(outms)
            record['data']   = tblocal.getcell('DATA', 2)
            record['weight'] = tblocal.getcell('WEIGHT', 5)
            record['sigma']  = tblocal.getcell('SIGMA', 7)
            tblocal.close()
        except Exception:
            print("Error channel averaging and reading", outms)
            raise
        self.records[corrsel] = record
        return splitran

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_sts(self):
        """Subtables, chan avg. without correlation selection"""
        self.check_subtables('', [(2, 2)])
        #self.__class__.n_tests_passed += 1

    def test_sts_ll(self):
        """Subtables, chan avg. LL"""
        self.check_subtables('ll', [(1, 2)])
        #self.__class__.n_tests_passed += 1

    def test_data(self):
        """DATA[2],   chan avg. without correlation selection"""
        check_eq(self.records['']['data'],
                 numpy.array([[17.13964462-42.20331192j, 26.04414749-49.97922897j],
                              [ 5.80819368-43.6548233j,   6.72127867-44.33802414j]]),0.0005)
        #self.__class__.n_tests_passed += 1

    def test_data_ll(self):
        """DATA[2],   chan avg. LL"""
        check_eq(self.records['ll']['data'],
                 numpy.array([[ 5.80819368-43.6548233j,  6.72127867-44.33802414j]]),0.0001)

    def test_wt(self):
        """WEIGHT[5], chan avg. without correlation selection"""
        # jagonzal: New WEIGHT calculation based on median
        # check_eq(self.records['']['weight'],numpy.array([5.0, 5.0]),0.001)
        check_eq(self.records['']['weight'],numpy.array([4.5, 4.5]),0.001)
        #self.__class__.n_tests_passed += 1

    def test_wt_ll(self):
        """WEIGHT[5], chan avg. LL"""
        # jagonzal: New WEIGHT calculation based on median
        # check_eq(self.records['ll']['weight'],numpy.array([5.]),0.001)
        check_eq(self.records['ll']['weight'],numpy.array([4.5]),0.001)
        #self.__class__.n_tests_passed += 1

    def test_sigma(self):
        """SIGMA[7], chan avg. without correlation selection"""
        # jagonzal: New SIGMA calculation based on median
        #check_eq(self.records['']['sigma'],numpy.array([0.44721359, 0.44721359]), 0.0001)
        check_eq(self.records['']['sigma'],numpy.array([0.4736068,  0.4736068]), 0.0001)

    def test_sigma_ll(self):
        """SIGMA[7], chan avg. LL"""
        # jagonzal: New SIGMA calculation based on median
        # check_eq(self.records['ll']['sigma'],numpy.array([0.44721359]), 0.0001)
        check_eq(self.records['ll']['sigma'],numpy.array([0.4736068]), 0.0001)
        #self.__class__.n_tests_passed += 1

class split_test_cdsp(SplitChecker):
    need_to_initialize = True
    corrsels = ['cas-3307.ms', 'bogusCDSP.ms']  # MSes, not corr selections.
    inpms = corrsels[0]                         # This variable is not used.
    records = {}

    def initialize(self):
        # The realization that need_to_initialize needs to be
        # a class variable more or less came from
        # http://www.gossamer-threads.com/lists/python/dev/776699
        self.__class__.need_to_initialize = False

        for inpms in self.corrsels:
            if not os.path.exists(os.path.join(datapath,inpms)):
                raise EnvironmentError("Missing input MS: " + os.path.join(datapath,inpms))
            self.res = self.do_split(inpms)

    def do_split(self, corrsel):     # corrsel is really an input MS in
        outms = 'reind_' + corrsel   # this class.
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nRemapping CALDEVICE and SYSPOWER of", corrsel)
            splitran = split(os.path.join(datapath,corrsel), outms, datacolumn='data',
                             field='', spw='0,2', width=1,
                             antenna='ea05,ea13&',
                             timebin='', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
            for st in ('CALDEVICE', 'SYSPOWER'):
                record[st] = {}
                tblocal.open(outms + '/' + st)
                for c in ('ANTENNA_ID', 'SPECTRAL_WINDOW_ID'):
                    record[st][c]   = tblocal.getcol(c)
                tblocal.close()
            shutil.rmtree(outms, ignore_errors=True)
        except Exception:
            print("Error channel averaging and reading", outms)
            raise
        self.records[corrsel] = record
        return splitran

    def test_bogus_cd_antid1(self):
        """ANTENNA_ID selection from a bad CALDEVICE"""
        # The resulting CALDEVICE is probably useless; the point is to ensure
        # that split ran to completion.
        check_eq(self.records['bogusCDSP.ms']['CALDEVICE']['ANTENNA_ID'],
                 numpy.array([0, 1, 0, 1]))

    def test_bogus_cd_spwid1(self):
        """SPECTRAL_WINDOW_ID selection from a bad CALDEVICE"""
        # The resulting CALDEVICE is probably useless; the point is to ensure
        # that split ran to completion.
        check_eq(self.records['bogusCDSP.ms']['CALDEVICE']['SPECTRAL_WINDOW_ID'],
                 numpy.array([0, 0, 1, 1]))

    def test_bogus_cd_antid2(self):
        """ANTENNA_ID selection from a bad SYSPOWER"""
        # The resulting SYSPOWER is probably useless; the point is to ensure
        # that split ran to completion.
        check_eq(self.records['bogusCDSP.ms']['SYSPOWER']['ANTENNA_ID'][89:97],
                 numpy.array([0, 0, 1, 0, 0, 1, 1, 1]))

    def test_bogus_cd_spwid2(self):
        """SPECTRAL_WINDOW_ID selection from a bad SYSPOWER"""
        # The resulting SYSPOWER is probably useless; the point is to ensure
        # that split ran to completion.
        check_eq(self.records['bogusCDSP.ms']['SYSPOWER']['SPECTRAL_WINDOW_ID'][189:197],
                 numpy.array([0, 1, 0, 0, 0, 1, 1, 1]))

    def test_cd_antid1(self):
        """ANTENNA_ID selection from CALDEVICE"""
        check_eq(self.records['cas-3307.ms']['CALDEVICE']['ANTENNA_ID'],
                 numpy.array([0, 1, 0, 1]))

    def test_cd_spwid1(self):
        """SPECTRAL_WINDOW_ID selection from CALDEVICE"""
        check_eq(self.records['cas-3307.ms']['CALDEVICE']['SPECTRAL_WINDOW_ID'],
                 numpy.array([0, 0, 1, 1]))

    def test_cd_antid2(self):
        """ANTENNA_ID selection from SYSPOWER"""
        # Purposely take a few from near the end.
        check_eq(self.records['cas-3307.ms']['SYSPOWER']['ANTENNA_ID'][-19:-6],
                 numpy.array([1, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 0, 1]))

    def test_cd_spwid2(self):
        """SPECTRAL_WINDOW_ID selection from SYSPOWER"""
        check_eq(self.records['cas-3307.ms']['SYSPOWER']['SPECTRAL_WINDOW_ID'][-18:-7],
                 numpy.array([0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1]))


class split_test_cst(SplitChecker):
    """
    The main thing here is to not segfault even when the SOURCE table
    contains nonsense.
    """
    need_to_initialize = True
    corrsels = ['']
    inpms = os.path.join(datapath,'crazySourceTable.ms') # read-only
    outms = 'filteredsrctab.ms'
    records = {}

    def initialize(self):
        # The realization that need_to_initialize needs to be
        # a class variable more or less came from
        # http://www.gossamer-threads.com/lists/python/dev/776699
        self.__class__.need_to_initialize = False

        if not os.path.isdir(self.inpms):
            raise EnvironmentError("Missing input MS: " + self.inpms)
        self.res = self.do_split(self.inpms)

    def do_split(self, inpms):
        shutil.rmtree(self.outms, ignore_errors=True)
        record = {}
        try:
            print("\nSplitting", inpms)
            splitran = split(inpms, self.outms, datacolumn='data',
                             field='', spw='', width=1,
                             antenna='',
                             timebin='', timerange='',
                             scan='', array='', uvrange='',
                             correlation='',
                             observation='1~3,5'
                             )
        except Exception:
            print("Error splitting to", self.outms)
            raise
        try:
            tblocal.open(self.outms + '/SOURCE')
            record['srcids'] = tblocal.getcol('SOURCE_ID')
            tblocal.close()
            tblocal.open(self.outms)
            #record['lastmainobsid'] = tblocal.getcell('OBSERVATION_ID', tblocal.nrows() - 1)
            tcol = tblocal.getcol('OBSERVATION_ID')
            tcol.sort()
            record['lastmainobsid'] = tcol[tblocal.nrows() - 1]
            tblocal.close()
            tblocal.open(self.outms + '/OBSERVATION')
            record['ebs'] = tblocal.getcol('SCHEDULE')[1]
            tblocal.close()
            shutil.rmtree(self.outms, ignore_errors=True)
        except Exception:
            print("Error getting results from", self.outms)
            raise
        self.records[inpms] = record
        return splitran


#    def tearDown(self):
#        shutil.rmtree(self.outms, ignore_errors=True)

    def test_cst(self):
        """
        Check that only the good part of a SOURCE subtable with some nonsense made it through
        """
        check_eq(self.records[self.inpms]['srcids'],
                 numpy.array([0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0,
                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))

    def test_obs(self):
        """
        Selected right observation IDs?
        """
        check_eq(self.records[self.inpms]['ebs'],
                 numpy.array(['ExecBlock uid://A002/Xb4fac/X1',
                              'ExecBlock uid://A002/Xb4f4c/X1',
                              'ExecBlock uid://A002/Xb4eec/X1',
                              'ExecBlock uid://A002/Xb506c/X1']))
        check_eq(self.records[self.inpms]['lastmainobsid'], 2)


class split_test_state(unittest.TestCase):
    """
    Checks the STATE subtable after selecting by intent.
    """
    inpms = os.path.join(datapath,'doppler01fine-01.ms')
    locms = inpms.split(os.path.sep)[-1]
    outms = 'obstar.ms'

    def setUp(self):
        try:
            shutil.rmtree(self.outms, ignore_errors=True)
            os.symlink(self.inpms, self.locms)  # Paranoia
            splitran = split(self.locms, self.outms, datacolumn='data',
                             intent='OBSERVE_TARGET.UNSPECIFIED'
                             )
        except Exception:
            print("Error splitting", self.locms, "to", self.outms)
            raise

    def tearDown(self):
        os.unlink(self.locms)
        shutil.rmtree(self.outms, ignore_errors=True)

    def test_state(self):
        """
        Is STATE correct after selecting by intent?
        """
        tblocal.open(self.outms + '/STATE')
        om = tblocal.getcol('OBS_MODE')
        tblocal.close()
        check_eq(om, numpy.array(['OBSERVE_TARGET.UNSPECIFIED']))
        tblocal.open(self.outms)
        mytime = tblocal.getcol('TIME')
        myrow = 0
        for i in range(len(mytime)):
            if mytime[i]==4785966752.5:
                myrow = i
                break
        rec = {}
        for c in ('ANTENNA1', 'ANTENNA2', 'DATA_DESC_ID', 'DATA',
                  'SCAN_NUMBER', 'STATE_ID', 'TIME'):
            rec[c] = tblocal.getcell(c, myrow)
        tblocal.close()
        # Row 1330 in inpms is the first one with STATE_ID 0.
        check_eq(rec, {'ANTENNA1': 0,
                       'ANTENNA2': 1,
                       'DATA': numpy.array([[287638.+0.j, 287638.+1.j,
                                             287638.+2.j, 287638.+3.j],
                                            [287638.+0.j, 287638.+1.j,
                                             287638.+2.j, 287638.+3.j]]),
                       'DATA_DESC_ID': 0,
                       'SCAN_NUMBER': 38,
                       'STATE_ID': 0,
                       'TIME': 4785966752.5})

class split_test_cavcd(unittest.TestCase):
    """
    Checks that the CORRECTED_DATA column can be channel averaged.
    """
    inpms = 'labelled_by_time+ichan.ms'
    if datapath.count('unittest_mms')==1:
        inpms = 'labelled_by_time+ichan.ms'

    outms = 'cavcd.ms'

    def setUp(self):
        try:
            shutil.rmtree(self.outms, ignore_errors=True)

            if not os.path.exists(self.inpms):
                # Copying is technically unnecessary for split,
                # but self.inpms is shared by other tests, so making
                # it readonly might break them.
                shutil.copytree(os.path.join(datapath,self.inpms), self.inpms)

            print("\n\tSplitting", self.inpms)
            splitran = split(self.inpms, self.outms, datacolumn='corrected',
                             field='', spw='', width=4,
                             antenna='',
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
        except Exception:
            print("Error splitting", self.inpms, "to", self.outms)
            raise

    def tearDown(self):
        shutil.rmtree(self.inpms, ignore_errors=True)
        shutil.rmtree(self.outms, ignore_errors=True)

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_cavcd(self):
        """
        Was the CORRECTED_DATA column channel averaged?
        """
        tblocal.open(self.outms)
        cod = tblocal.getcell('DATA', 0)
        tblocal.close()
        check_eq(cod.shape, (1, 2))

class split_test_genericsubtables(unittest.TestCase):
    """
    Check copying generic subtables
    """
#    inpms = os.path.join(datapath,'2554.ms')
    inpms = os.path.join(datapath,'alma_2010_8ant.ms')
    outms = 'musthavegenericsubtables.ms'

    def setUp(self):
        try:
            shutil.rmtree(self.outms, ignore_errors=True)

            #print "\n\tSplitting", self.inpms
            splitran = split(self.inpms, self.outms, datacolumn='data',
                             field='', spw='0', width=1,
                             antenna='',
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
        except Exception:
            print("Error splitting", self.inpms, "to", self.outms)
            raise

    def tearDown(self):
        shutil.rmtree(self.outms, ignore_errors=True)

    def test_genericsubtables(self):
        """
        Can we copy generic subtables?
        """
        tblocal.open(self.outms)
        kws = tblocal.keywordnames()
        tblocal.close()
        # Just check a few, and order does not matter.  Include both "generic"
        # and "standard" (mandatory and optional) subtables.
        for subtab in ('ASDM_CALWVR', 'ASDM_CALDELAY', 'DATA_DESCRIPTION',
                       'POINTING', 'SYSCAL'):
            assert subtab in kws

class split_test_singchan(unittest.TestCase):
    """
    Check selecting a single channel with the spw:chan syntax
    """
    # rename and make readonly when plotxy goes away.
    inpms = 'ctb80-vsm.ms'
    if datapath.count('unittest_mms')==1:
        inpms = 'ctb80-vsm.ms'

    outms = 'musthavesingchan.ms'

    def setUp(self):
        try:
            shutil.rmtree(self.outms, ignore_errors=True)

            if not os.path.exists(self.inpms):
                # Copying is technically unnecessary for split,
                # but self.inpms is shared by other tests, so making
                # it readonly might break them.
                shutil.copytree(os.path.join(datapath,self.inpms), self.inpms)

            print("\n\tSplitting", self.inpms)
            splitran = split(self.inpms, self.outms, datacolumn='data',
                             field='', spw='0:25', width=1,
                             antenna='',
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
        except Exception:
            print("Error splitting", self.inpms, "to", self.outms)
            raise

    def tearDown(self):
        # Leaves an empty viewertest dir in nosedir
        shutil.rmtree(self.inpms, ignore_errors=True)

        shutil.rmtree(self.outms, ignore_errors=True)

    def test_singchan(self):
        """
        Did we get the right channel?
        """
        tblocal.open(self.inpms)
        data_orig = tblocal.getcell('DATA', 3)
        tblocal.close()
        tblocal.open(self.outms)
        data_sp = tblocal.getcell('DATA', 3)
        tblocal.close()

        # For all correlations, compare output channel 0 to input channel 25.
        check_eq(data_sp[:,0], data_orig[:,25], 0.0001)

class split_test_blankov(unittest.TestCase):
    """
    Check that outputvis == '' causes a prompt exit.
    """
    # rename and make readonly when plotxy goes away.
    inpms = 'ctb80-vsm.ms'
    if datapath.count('unittest_mms')==1:
        inpms = 'ctb80-vsm.ms'

    outms = ''

    def setUp(self):
        try:
            shutil.rmtree(self.outms, ignore_errors=True)

            if not os.path.exists(self.inpms):
                # Copying is technically unnecessary for split,
                # but self.inpms is shared by other tests, so making
                # it readonly might break them.
                shutil.copytree(os.path.join(datapath,self.inpms), self.inpms)
        except Exception:
            print("Error in rm -rf %s or cp -r %s" % (self.outms, self.inpms))
            raise

    def tearDown(self):
        shutil.rmtree(self.inpms, ignore_errors=True)
        shutil.rmtree(self.outms, ignore_errors=True)

    def test_blankov(self):
        """
        Does outputvis == '' cause a prompt exit?
        """
        splitran = False
        try:
            splitran = split(self.inpms, self.outms, datacolumn='data',
                             field='', spw='0:25', width=1,
                             antenna='',
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
        except ValueError:
            splitran = False
        except Exception as e:
            print("Unexpected but probably benign exception:", e)

        assert not splitran

class split_test_almapol(SplitChecker):
    """
    Check that correlations can be selected when WVR data is in spw 0,
    and that nonstandard columns in WEATHER are being copied.
    """
    need_to_initialize = True
    corrsels = ['xx,yy']
    inpms = os.path.join(datapath,'ixxxyyxyy.ms')
    records = {}

    def do_split(self, corrsel):
        outms = 'xxyyspw1_3.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        splitran = False
        try:
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw='1~3', width=1,
                             antenna='',
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation=corrsel)
            tblocal.open(outms + '/WEATHER')
            record['nsid'] = {0: tblocal.getcell('NS_WX_STATION_ID', 0),
                              1: tblocal.getcell('NS_WX_STATION_ID', 1)}
            record['nspos'] = {0: tblocal.getcell('NS_WX_STATION_POSITION', 0),
                               1: tblocal.getcell('NS_WX_STATION_POSITION', 1)}
            tblocal.close()
        except Exception:
            print( "Error selecting %s from %s:" % (corrsel, outms))
            raise
        self.records[corrsel] = record
        return splitran

    def test_almapol(self):
        """Can we select corrs when WVR data is in spw 0?"""
        for corrsel in self.corrsels:
            assert os.path.isdir(self.records[corrsel]['ms'])
            shutil.rmtree(self.records[corrsel]['ms'], ignore_errors=True)

    def test_nsid(self):
        """Did NS_WX_STATION_ID get copied?"""
        for corrsel in self.corrsels:
            check_eq(self.records[corrsel]['nsid'][0], 8)
            check_eq(self.records[corrsel]['nsid'][1], 9)

    def test_nspos(self):
        """Did NS_WX_STATION_POS get copied?"""
        for corrsel in self.corrsels:
            check_eq(self.records[corrsel]['nspos'][0],
                     numpy.array([2225262.12, -5440307.30, -2480962.57]), 0.01)
            check_eq(self.records[corrsel]['nspos'][1],
                     numpy.array([2224782.10, -5440330.29, -2481339.08]), 0.01)


class split_test_unorderedpolspw(SplitChecker):
    """
    Check spw selection from a tricky MS.
    """
    need_to_initialize = True
    inpms = os.path.join(datapath,'unordered_polspw.ms')
    corrsels = ['']
    records = {}
    #n_tests = 2
    #n_tests_passed = 0

    def do_split(self, corrsel):
        outms = 'pss' + re.sub(',\s*', '', corrsel) + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nSelecting spws 1, 3, and 5.")
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw='1,3,5', width=1, antenna='',
                             timebin='0s', timerange='18:32:40~18:33:20',
                             scan='', array='', uvrange='',
                             correlation=corrsel)
            tblocal.open(outms)
            record['data'] = tblocal.getcell('DATA', 2)
            tblocal.close()
        except Exception:
            print("Error selecting spws 1, 3, and 5 from", self.inpms)
            raise
        self.__class__.records[corrsel] = record
        return splitran

    def test_datashape(self):
        """Data shape"""
        assert self.records['']['data'].shape == (2, 128)
        #self.__class__.n_tests_passed += 1

    def test_subtables(self):
        """DATA_DESCRIPTION, SPECTRAL_WINDOW, and POLARIZATION shapes"""
        self.check_subtables('', [(2, 128)])
        #self.__class__.n_tests_passed += 1

class split_test_sw_and_fc(SplitChecker):
    """
    Check SPECTRAL_WINDOW and FLAG_CMD with chan selection and averaging.
    """
    need_to_initialize = True
#    inpms = os.path.join(datapath,'2562.ms')
    inpms = os.path.join(datapath,'vla_12191+48299_2spw.ms')
    records = {}

    # records uses these as keys, so they MUST be tuples, not lists.
    # Each tuple is really (spw, width), but it's called corrsels for
    # compatibility with SplitChecker.
    corrsels = (('1:12~115', '1'), ('1', '3'))

    def do_split(self, spwwidth):
        outms = 'cw' + spwwidth[1] + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nChecking SPECTRAL_WINDOW and FLAG_CMD with width " + spwwidth[1] + '.')
            # Antenna selection added just so it's tested somewhere.
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw=spwwidth[0], width=spwwidth[1],
                             antenna='VA03,VA05&',               # Case sensitive
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
            tblocal.open(outms + '/SPECTRAL_WINDOW')
            cf = tblocal.getcell('CHAN_FREQ', 0)
            record['nchan'] = cf.shape[0]
            record['cf0']   = cf[0]
            record['cf']    = cf[33]
            record['cflc']  = cf[-1]
            record['res']   = tblocal.getcell('RESOLUTION', 0)
            record['cw']    = tblocal.getcell('CHAN_WIDTH', 0)
            record['eb']    = tblocal.getcell('EFFECTIVE_BW', 0)
            record['tb']    = tblocal.getcell('TOTAL_BANDWIDTH', 0)
            record['rf']    = tblocal.getcell('REF_FREQUENCY', 0)
            tblocal.close()
            tblocal.open(outms + '/FLAG_CMD')
            record['fc'] = []
            for i in (0, 1, 2, 3, 4, 515, 516):
                record['fc'].append(tblocal.getcell('COMMAND', i))
            tblocal.close()
            shutil.rmtree(outms, ignore_errors=True)
        except Exception :
            print("Error selecting spws 1, 3, and 5 from", self.inpms)
            raise
        self.__class__.records[spwwidth] = record
        return splitran

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_fc_noavg(self):
        """Updating of FLAG_CMD after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['fc'],
                 ['',
                  "antenna='ea18' spw='0:28~43' timerange='2010/04/08/20:03:52.502~2010/04/08/20:03:55.504'",
                  "antenna='ea20' timerange='2010/04/08/20:03:56.804~2010/04/08/20:03:59.936'",
                  "antenna='ea17' spw='0:1~21' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.259'",
                  "antenna='ea22' spw='0:0~11' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.829'",
                  " antenna='ea17' spw='0:1~21' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.259,2010/04/08/20:04:50.917~2010/04/08/20:04:58.403,2010/04/08/20:06:01.627~2010/04/08/20:06:05.527,2010/04/08/20:06:16.444~2010/04/08/20:06:20.656,2010/04/08/20:06:36.308~2010/04/08/20:06:40.113,2010/04/08/20:06:56.059~2010/04/08/20:06:59.095,2010/04/08/20:07:16.302~2010/04/08/20:07:19.909,2010/04/08/20:07:36.027~2010/04/08/20:07:40.325,2010/04/08/20:07:56.374~2010/04/08/20:08:00.534,2010/04/08/20:08:16.436~2010/04/08/20:08:20.406,2010/04/08/20:08:35.928~2010/04/08/20:08:39.026,2010/04/08/20:08:56.301~2010/04/08/20:08:59.788,2010/04/08/20:09:16.035~2010/04/08/20:09:20.368,2010/04/08/20:09:36.382~2010/04/08/20:09:40.741,2010/04/08/20:09:56.591~2010/04/08/20:10:00.388,2010/04/08/20:10:16.083~2010/04/08/20:10:19.120,2010/04/08/20:10:36.085~2010/04/08/20:10:39.700,2010/04/08/20:10:49.701~2010/04/08/20:11:07.582,2010/04/08/20:10:49.900~2010/04/08/20:10:57.482,2010/04/08/20:10:50.401~2010/04/08/20:10:54.665'",
                  " antenna='ea22' spw='0:0~11' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.829,2010/04/08/20:04:51.020~2010/04/08/20:04:55.716,2010/04/08/20:06:01.661~2010/04/08/20:06:05.692,2010/04/08/20:06:16.392~2010/04/08/20:06:20.699,2010/04/08/20:06:36.403~2010/04/08/20:06:40.312,2010/04/08/20:06:55.903~2010/04/08/20:06:59.121,2010/04/08/20:07:16.181~2010/04/08/20:07:19.702,2010/04/08/20:07:35.915~2010/04/08/20:07:40.438,2010/04/08/20:07:56.297~2010/04/08/20:08:00.638,2010/04/08/20:08:16.445~2010/04/08/20:08:20.458,2010/04/08/20:08:36.006~2010/04/08/20:08:39.129,2010/04/08/20:08:56.129~2010/04/08/20:08:59.736,2010/04/08/20:09:16.044~2010/04/08/20:09:20.549,2010/04/08/20:09:36.374~2010/04/08/20:09:40.793,2010/04/08/20:09:56.479~2010/04/08/20:10:00.579,2010/04/08/20:10:15.781~2010/04/08/20:10:19.085,2010/04/08/20:10:36.093~2010/04/08/20:10:39.597,2010/04/08/20:10:49.805~2010/04/08/20:11:06.294,2010/04/08/20:10:49.995~2010/04/08/20:10:54.000,2010/04/08/20:10:50.298~2010/04/08/20:10:55.417'"])

    def test_rf_noavg(self):
        """REF_FREQUENCY after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['rf'], 22141747338.809235)

    def test_nchan_noavg(self):
        """# of channels after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['nchan'], 104)

    def test_res_noavg(self):
        """RESOLUTION after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['res'], 14771.10564634, 1e-4)

    def test_cf0_noavg(self):
        """CHAN_FREQ[0] after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['cf0'], 22141747338.809235, 1e-4)

    def test_cf_noavg(self):
        """CHAN_FREQ[33] after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['cf'], 22142150187.145042, 1e-4)

    def test_cflc_noavg(self):
        """CHAN_FREQ[-1] after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['cflc'], 22143004713.917973, 1e-4)

    def test_cw_noavg(self):
        """CHAN_WIDTH after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['cw'], 12207.525327551524, 1e-4)

    def test_eb_noavg(self):
        """EFFECTIVE_BW after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['eb'], 14771.10564634, 1e-4)

    def test_tb_noavg(self):
        """TOTAL_BANDWIDTH after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['tb'], 1269582.6340653566, 1e-4)

    def test_nchan_wavg(self):
        """# of channels after averaging, but no selection."""
        # The last channel is dropped when width is narrower than the others,
        # in order to have an uniform grid
        check_eq(self.records[('1', '3')]['nchan'], 42)

    def test_rf_wavg(self):
        """REF_FREQUENCY after averaging, but no selection."""
        check_eq(self.records[('1', '3')]['rf'], 22141613056.030632)

    def test_res_wavg(self):
        """RESOLUTION after averaging and simple selection."""
        # The last one really is different (128 % 3 != 0), but the variation
        # of the rest is numerical jitter.
        # The last channel is dropped when width is narrower than the others,
        # in order to have an uniform grid
        check_eq(self.records[('1', '3')]['res'],
                 numpy.array([39186.15630552, 39186.1563017, 39186.15630552,
                              39186.1563017,  39186.1563017, 39186.1563017,
                              39186.1563017,  39186.1563055, 39186.1563017,
                              39186.15629789, 39186.1563055, 39186.15629789,
                              39186.1563017,  39186.1562941, 39186.15629789,
                              39186.1563017,  39186.1562979, 39186.15629789,
                              39186.15629789, 39186.1562979, 39186.15630552,
                              39186.1563017,  39186.1563055, 39186.1563017,
                              39186.1563017,  39186.1563055, 39186.15629789,
                              39186.15630552, 39186.1563017, 39186.1563017,
                              39186.1563017,  39186.1563017, 39186.15630552,
                              39186.1563017,  39186.1562979, 39186.15630552,
                              39186.1563017,  39186.1563055, 39186.15629789,
                              39186.1563017,  39186.1563055, 39186.15629789]), 1e-4)

    def test_cf0_wavg(self):
        """CHAN_FREQ[0] after averaging, but no selection."""
        check_eq(self.records[('1', '3')]['cf0'], 22141613056.030632, 1e-4)

    def test_cf_wavg(self):
        """CHAN_FREQ[33] after averaging, but no selection."""
        check_eq(self.records[('1', '3')]['cf'], 22142821601.038055, 1e-4)

    def test_cflc_wavg(self):
        """CHAN_FREQ[-1] after averaging, but no selection."""
        # The last channel is dropped when width is narrower than the others,
        # in order to have an uniform grid
        check_eq(self.records[('1', '3')]['cflc'], 22143114581.64592, 1e-4)

    def test_cw_wavg(self):
        """CHAN_WIDTH after averaging, but no selection."""
        # The last one really is different (128 % 3 != 0), but the variation
        # of the rest is numerical jitter.
        # The last channel is dropped when width is narrower than the others,
        # in order to have an uniform grid
        check_eq(self.records[('1', '3')]['cw'],
                 numpy.array([36622.57598673, 36622.57598292, 36622.57598673,
                              36622.57598292, 36622.57598292, 36622.57598292,
                              36622.57598292, 36622.57598673, 36622.57598292,
                              36622.5759791,  36622.57598673, 36622.5759791,
                              36622.57598292, 36622.57597529, 36622.5759791,
                              36622.57598292, 36622.5759791,  36622.5759791,
                              36622.5759791,  36622.5759791,  36622.57598673,
                              36622.57598292, 36622.57598673, 36622.57598292,
                              36622.57598292, 36622.57598673, 36622.5759791,
                              36622.57598673, 36622.57598292, 36622.57598292,
                              36622.57598292, 36622.57598292, 36622.57598673,
                              36622.57598292, 36622.5759791,  36622.57598673,
                              36622.57598292, 36622.57598673, 36622.5759791,
                              36622.57598292, 36622.57598673, 36622.5759791]), 1e-3)

    def test_eb_wavg(self):
        """EFFECTIVE_BW after averaging, but no selection."""
        # The last one really is different (128 % 3 != 0), but the variation
        # of the rest is numerical jitter.
        # The last channel is dropped when width is narrower than the others,
        # in order to have an uniform grid
        check_eq(self.records[('1', '3')]['eb'],
                 numpy.array([39186.15630552, 39186.1563017,  39186.15630552,
                              39186.1563017,  39186.1563017,  39186.1563017,
                              39186.1563017,  39186.15630552, 39186.1563017,
                              39186.15629789, 39186.15630552, 39186.15629789,
                              39186.1563017,  39186.15629407, 39186.15629789,
                              39186.1563017,  39186.15629789, 39186.15629789,
                              39186.15629789, 39186.15629789, 39186.15630552,
                              39186.1563017,  39186.15630552, 39186.1563017,
                              39186.1563017,  39186.15630552, 39186.15629789,
                              39186.15630552, 39186.1563017,  39186.1563017,
                              39186.1563017,  39186.1563017,  39186.15630552,
                              39186.1563017,  39186.15629789, 39186.15630552,
                              39186.1563017,  39186.15630552, 39186.15629789,
                              39186.1563017,  39186.15630552, 39186.15629789]), 1e-3)

    def test_tb_wavg(self):
        """Is TOTAL_BANDWIDTH conserved after averaging, but no selection?"""
        # The expected value comes from spw 1 of inpms.
        # The last channel is dropped when width is narrower than the others,
        # in order to have an uniform grid
        check_eq(self.records[('1', '3')]['tb'], 1538148.1912714909, 0.1)

    def test_fc_wavg(self):
        """Updating of FLAG_CMD after averaging, but simple selection."""
        check_eq(self.records[('1', '3')]['fc'],
                 ['',
                  "antenna='ea18' spw='0:13~18' timerange='2010/04/08/20:03:52.502~2010/04/08/20:03:55.504'",
                  "antenna='ea20' timerange='2010/04/08/20:03:56.804~2010/04/08/20:03:59.936'",
                  "antenna='ea17' spw='0:1~2;4~11' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.259'",
                  "antenna='ea22' spw='0:3~7' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.829'",
                  " antenna='ea17' spw='0:1~2;4~11' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.259,2010/04/08/20:04:50.917~2010/04/08/20:04:58.403,2010/04/08/20:06:01.627~2010/04/08/20:06:05.527,2010/04/08/20:06:16.444~2010/04/08/20:06:20.656,2010/04/08/20:06:36.308~2010/04/08/20:06:40.113,2010/04/08/20:06:56.059~2010/04/08/20:06:59.095,2010/04/08/20:07:16.302~2010/04/08/20:07:19.909,2010/04/08/20:07:36.027~2010/04/08/20:07:40.325,2010/04/08/20:07:56.374~2010/04/08/20:08:00.534,2010/04/08/20:08:16.436~2010/04/08/20:08:20.406,2010/04/08/20:08:35.928~2010/04/08/20:08:39.026,2010/04/08/20:08:56.301~2010/04/08/20:08:59.788,2010/04/08/20:09:16.035~2010/04/08/20:09:20.368,2010/04/08/20:09:36.382~2010/04/08/20:09:40.741,2010/04/08/20:09:56.591~2010/04/08/20:10:00.388,2010/04/08/20:10:16.083~2010/04/08/20:10:19.120,2010/04/08/20:10:36.085~2010/04/08/20:10:39.700,2010/04/08/20:10:49.701~2010/04/08/20:11:07.582,2010/04/08/20:10:49.900~2010/04/08/20:10:57.482,2010/04/08/20:10:50.401~2010/04/08/20:10:54.665'",
                  " antenna='ea22' spw='0:3~7' timerange='2010/04/08/20:04:50.614~2010/04/08/20:05:07.829,2010/04/08/20:04:51.020~2010/04/08/20:04:55.716,2010/04/08/20:06:01.661~2010/04/08/20:06:05.692,2010/04/08/20:06:16.392~2010/04/08/20:06:20.699,2010/04/08/20:06:36.403~2010/04/08/20:06:40.312,2010/04/08/20:06:55.903~2010/04/08/20:06:59.121,2010/04/08/20:07:16.181~2010/04/08/20:07:19.702,2010/04/08/20:07:35.915~2010/04/08/20:07:40.438,2010/04/08/20:07:56.297~2010/04/08/20:08:00.638,2010/04/08/20:08:16.445~2010/04/08/20:08:20.458,2010/04/08/20:08:36.006~2010/04/08/20:08:39.129,2010/04/08/20:08:56.129~2010/04/08/20:08:59.736,2010/04/08/20:09:16.044~2010/04/08/20:09:20.549,2010/04/08/20:09:36.374~2010/04/08/20:09:40.793,2010/04/08/20:09:56.479~2010/04/08/20:10:00.579,2010/04/08/20:10:15.781~2010/04/08/20:10:19.085,2010/04/08/20:10:36.093~2010/04/08/20:10:39.597,2010/04/08/20:10:49.805~2010/04/08/20:11:06.294,2010/04/08/20:10:49.995~2010/04/08/20:10:54.000,2010/04/08/20:10:50.298~2010/04/08/20:10:55.417'"])

class split_test_optswc(SplitChecker):
    """
    Check propagation of SPECTRAL_WINDOW's optional columns
    """
    need_to_initialize = True
    inpms = os.path.join(datapath,'optswc.ms')
    records = {}
    expcols = set(['MEAS_FREQ_REF', 'CHAN_FREQ',       'REF_FREQUENCY',
                   'CHAN_WIDTH',    'EFFECTIVE_BW',    'RESOLUTION',
                   'FLAG_ROW',      'FREQ_GROUP',      'FREQ_GROUP_NAME',
                   'IF_CONV_CHAIN', 'NAME',            'NET_SIDEBAND',
                   'NUM_CHAN',      'TOTAL_BANDWIDTH', 'BBC_NO',
                   'ASSOC_SPW_ID',  'ASSOC_NATURE'])

    # records uses these as keys, so they MUST be tuples, not lists.
    # Each tuple is really (spw, width), but it's called corrsels for
    # compatibility with SplitChecker.
    corrsels = (('1:12~115', '1'), ('', '3'))

    def do_split(self, spwwidth):
        outms = 'optswc_' + spwwidth[1] + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nChecking SPECTRAL_WINDOW's opt cols with width " + spwwidth[1] + '.')
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw=spwwidth[0], width=spwwidth[1], antenna='',
                             timebin='0s', timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
            tblocal.open(outms + '/SPECTRAL_WINDOW')
            record['colnames'] = set(tblocal.colnames())
            record['bbc_no']   = tblocal.getcell('BBC_NO', 0)
            tblocal.close()
            shutil.rmtree(outms, ignore_errors=True)
        except Exception:
            print("Error selecting spws 1, 3, and 5 from", self.inpms)
            raise
        self.__class__.records[spwwidth] = record
        return splitran

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_rightcols_noavg(self):
        """List of SW cols after selection, but no averaging."""
        check_eq(self.records[('1:12~115', '1')]['colnames'],
                 self.expcols)

    def test_rightcols_wavg(self):
        """List of SW cols after averaging, but no selection."""
        check_eq(self.records[('', '3')]['colnames'],
                 self.expcols)

    def test_bbcno_noavg(self):
        """Can we get BBC1?"""
        check_eq(self.records[('1:12~115', '1')]['bbc_no'], 1)

    def test_bbcno_wavg(self):
        """Can we get any BBC if we average?"""
        check_eq(self.records[('', '3')]['bbc_no'], 0)


@unittest.skip("split_test_tav_then_cvel is skipped")
class split_test_tav_then_cvel(SplitChecker):
    need_to_initialize = True
    # doppler01fine-01.ms was altered by
    # make_labelled_ms(vis, vis,
    #                  {'SCAN_NUMBER': 1.0,
    #                   'DATA_DESC_ID': 0.01,
    #                   'chan': complex(0, 1),
    #                   'STATE_ID': complex(0, 0.1),
    #                   'time': 100.0}, ow=True)
    inpms = os.path.join(datapath,'doppler01fine-01.ms')
    corrsels = ['']
    records = {}
    #n_tests = 6
    #n_tests_passed = 0

    def do_split(self, corrsel):
        tavms = 'doppler01fine-01-10s.ms'
        cvms  = 'doppler01fine-01-10s-cvel.ms'
        record = {'tavms': tavms, 'cvms': cvms,
                  'tav': {},      'cv': False}
        self.__class__._cvel_err = False

        shutil.rmtree(tavms, ignore_errors=True)
        shutil.rmtree(cvms, ignore_errors=True)
        try:
            print("\nTime averaging", corrsel)
            splitran = split(self.inpms, tavms, datacolumn='data',
                             field='', spw='', width=1, antenna='',
                             timebin='10s', timerange='',
                             scan='', array='', uvrange='',
                             correlation=corrsel)
            tblocal.open(tavms)
            for c in ['DATA', 'WEIGHT', 'INTERVAL', 'SCAN_NUMBER', 'STATE_ID', 'TIME']:
                record['tav'][c] = {}
                for r in [0, 4, 5, 6, 7, 90, 91]:
                    record['tav'][c][r] = tblocal.getcell(c, r)
            for c in ['SCAN_NUMBER', 'STATE_ID', 'TIME']:
                record['tav'][c][123] = tblocal.getcell(c, 123)
            tblocal.close()
        except Exception:
            print("Error time averaging and reading", tavms)
            raise
        try:
            print("Running cvel")
            cvelran = cvel(tavms, cvms, passall=False, field='', spw='0~8',
                           selectdata=True, timerange='', scan="", array="",
                           mode="velocity", nchan=-1, start="-4km/s",
                           width="-1.28km/s", interpolation="linear",
                           phasecenter="", restfreq="6035.092MHz",
                           outframe="lsrk", veltype="radio", hanning=False)
        except Exception as e:
            print("Error running cvel:", e)
            # Do NOT raise e: that would prevent the tav tests from running.
            # Use test_cv() to register a cvel error.
            self.__class__._cvel_err = True
        self.__class__.records = record
        shutil.rmtree(tavms, ignore_errors=True)
        # Don't remove cvms yet, its existence is tested.
        return splitran

    def test_tav_data(self):
        """Time averaged DATA"""
        check_eq(self.records['tav']['DATA'],
                 {0: numpy.array([[ 455.+0.10000001j,  455.+1.10000014j,
                                    455.+2.10000014j,  455.+3.10000014j],
                                  [ 455.+0.10000001j,  455.+1.10000014j,
                                    455.+2.10000014j,  455.+3.10000014j]]),
                  4: numpy.array([[4455.+0.10000001j, 4455.+1.10000014j,
                                   4455.+2.10000014j, 4455.+3.10000014j],
                                  [4455.+0.10000001j, 4455.+1.10000014j,
                                   4455.+2.10000014j, 4455.+3.10000014j]]),
                  5: numpy.array([[5405.+0.10000001j, 5405.+1.10000002j,
                                   5405.+2.10000014j, 5405.+3.10000014j],
                                  [5405.+0.10000001j, 5405.+1.10000002j,
                                   5405.+2.10000014j, 5405.+3.10000014j]]),
                  6: numpy.array([[6356.+0.10000002j, 6356.+1.10000014j,
                                   6356.+2.10000014j, 6356.+3.10000014j],
                                  [6356.+0.10000002j, 6356.+1.10000014j,
                                   6356.+2.10000014j, 6356.+3.10000014j]]),
                  7: numpy.array([[7356.+0.10000002j, 7356.+1.10000014j,
                                   7356.+2.10000014j, 7356.+3.10000014j],
                                  [7356.+0.10000002j, 7356.+1.10000014j,
                                   7356.+2.10000014j, 7356.+3.10000014j]]),
                 90: numpy.array([[162467.015625+0.j, 162467.015625+1.j,
                                   162467.015625+2.j, 162467.015625+3.j],
                                  [162467.015625+0.j, 162467.015625+1.j,
                                   162467.015625+2.j, 162467.015625+3.j]]),
                 91: numpy.array([[163467.015625+0.j, 163467.015625+1.j,
                                   163467.015625+2.j, 163467.015625+3.j],
                                  [163467.015625+0.j, 163467.015625+1.j,
                                   163467.015625+2.j, 163467.015625+3.j]])},
                 0.0001)
        #self.__class__.n_tests_passed += 1

    def test_tav_wt(self):
        """Time averaged WEIGHT"""
        check_eq(self.records['tav']['WEIGHT'],
                 {0: numpy.array([ 10.,  10.]),
                  4: numpy.array([ 10.,  10.]),
                  5: numpy.array([ 9.,  9.]),
                  6: numpy.array([ 10.,  10.]),
                  7: numpy.array([ 10.,  10.]),
                  90: numpy.array([ 10.,  10.]),
                  91: numpy.array([ 10.,  10.])}, 0.01)
        #self.__class__.n_tests_passed += 1

    def test_tav_int(self):
        """Time averaged INTERVAL"""
        check_eq(self.records['tav']['INTERVAL'],
                 {0: 10.0, 4: 10.0, 5: 9.0, 6: 10.0, 7: 10.0, 90: 10.0, 91: 10.0},
                 0.01)
        #self.__class__.n_tests_passed += 1

    def test_tav_state_id(self):
        """Time averaged STATE_ID"""
        check_eq(self.records['tav']['STATE_ID'],
                 {0: 1, 4: 1, 5: 1, 6: 1, 7: 1, 90: 0, 91: 0, 123: 0})

    def test_tav_scan(self):
        """Time averaged SCAN_NUMBER"""
        check_eq(self.records['tav']['SCAN_NUMBER'],
                 {0: 5, 4: 5, 5: 5, 6: 6, 7: 6, 90: 17, 91: 17, 123: 40})

    def test_tav_time(self):
        """Time averaged TIME"""
        check_eq(self.records['tav']['TIME'],
                 {0: 4785963881.0,
                  4: 4785963921.0,
                  5: 4785963930.5,
                  6: 4785963940.0,
                  7: 4785963950.0,
                  90: 4785965501.0,
                  91: 4785965511.0,
                  123: 4785966907.0})

    def test_cv(self):
        """cvel completed"""
        assert self._cvel_err == False and os.path.isdir(self.records['cvms'])
        shutil.rmtree(self.records['cvms'])
        #self.__class__.n_tests_passed += 1

class split_test_wttosig(SplitChecker):
    """
    Check WEIGHT and SIGMA after various datacolumn selections and averagings.
    """
    need_to_initialize = True
    inpms = os.path.join(datapath,'testwtsig.ms')
    records = {}

    # records uses these as keys, so they MUST be tuples, not lists.
    # Each tuple is really (datacolumn, width, timebin), but it's called corrsels for
    # compatibility with SplitChecker.
    corrsels = (('data',      '1', '0s'), # straight selection of DATA.
                ('corrected', '1', '0s'), # straight CORRECTED -> DATA.
                ('data', '2', '0s'),      # channel averaged DATA
                ('data', '1', '60s'),     # time averaged DATA
                ('data', '1', '30s'),     # time averaged DATA with interval the same as the data itself
                ('corrected', '2', '0s'), # channel averaged CORRECTED -> DATA
                ('corrected', '1', '30s'), # time averaged CORRECTED -> DATA with interval the same as the data itself
                ('corrected', '1', '60s')) # time averaged CORRECTED -> DATA


    def do_split(self, dcwtb):
        outms = 'wtsig_' + '_'.join(dcwtb) + '.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nChecking WEIGHT and SIGMA after %s." % (dcwtb,))
            splitran = split(self.inpms, outms, datacolumn=dcwtb[0],
                             field='', spw='', width=dcwtb[1], antenna='',
                             timebin=dcwtb[2], timerange='',
                             scan='', array='', uvrange='',
                             correlation='')
            tblocal.open(outms)
            record['sigma'] = tblocal.getcol('SIGMA')[:,0:5].transpose()
            record['wt']    = tblocal.getcol('WEIGHT')[:,0:5].transpose()
            tblocal.close()
            shutil.rmtree(outms, ignore_errors=True)
        except Exception:
            print("Error splitting %s from %s", (dcwtb, self.inpms))
            raise
        self.__class__.records[dcwtb] = record
        return splitran

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_wt_straightselection(self):
        """WEIGHT after straight selection of DATA."""
        check_eq(self.records[('data', '1', '0s')]['wt'],
                 numpy.array([[ 0.0625    ,  0.11111111,  0.25      ,  1.        ],
                              [ 0.0625    ,  0.11111111,  0.25      ,  1.        ],
                              [ 1.        ,  0.25      ,  0.11111111,  0.0625    ],
                              [ 0.04      ,  0.02777778,  0.02040816,  0.015625  ],
                              [ 1.        ,  1.        ,  1.        ,  1.        ]]),
                 0.001)

    def test_sig_straightselection(self):
        """SIGMA after straight selection of DATA."""
        check_eq(self.records[('data', '1', '0s')]['sigma'],
                 numpy.array([[4.,     3.,       2.,       1.],
                              [4.,     3.,       2.,       1.],
                              [1.,     2.,       3.,       4.],
                              [5.,     6.,       7.,       8.],
                              [1.,     1.,       1.,       1.]]), 0.001)

    def test_wt_corrtodata(self):
        """WEIGHT after straight CORRECTED -> DATA."""
        check_eq(self.records[('corrected', '1', '0s')]['wt'],
                 numpy.array([[1.,     4.,       9.,      16.],
                              [0.0625, 0.111111, 0.25,     1.],
                              [1.,     0.25,     0.111111, 0.0625],
                              [1.,     1.,       1.,       1.],
                              [1.,     1.,       1.,       1.]]), 0.001)

    def test_sig_corrtodata(self):
        """SIGMA after straight CORRECTED -> DATA."""
        check_eq(self.records[('corrected', '1', '0s')]['sigma'],
                 numpy.array([[1.,     0.5,      0.333333, 0.25],
                              [4.,     3.,       2.,       1.],
                              [1.,     2.,       3.,       4.],
                              [1.,     1.,       1.,       1.],
                              [1.,     1.,       1.,       1.]]), 0.001)

    def test_wt_cavdata(self):
        """WEIGHT after channel averaging DATA."""
        check_eq(self.records[('data', '2', '0s')]['wt'],
                 numpy.array([[ 0.125     ,  0.22222224,  0.5       ,  2.        ],
                        [ 0.125     ,  0.22222224,  0.5       ,  2.        ],
                        [ 2.        ,  0.5       ,  0.22222224,  0.125     ],
                        [ 0.08      ,  0.05555556,  0.04081633,  0.03125   ],
                        [ 2.        ,  2.        ,  2.        ,  2.        ]]),
                 0.001)

    def test_sig_cavdata(self):
        """SIGMA after channel averaging DATA."""
        check_eq(self.records[('data', '2', '0s')]['sigma'],
                 numpy.array([[ 2.82842708,  2.12132049,  1.41421354,  0.70710677],
                              [ 2.82842708,  2.12132049,  1.41421354,  0.70710677],
                              [ 0.70710677,  1.41421354,  2.12132049,  2.82842708],
                              [ 3.53553391,  4.24264097,  4.94974756,  5.65685415],
                              [ 0.70710677,  0.70710677,  0.70710677,  0.70710677]]),
                 0.001)

    def test_wt_tav30data(self):
        """WEIGHT after time averaging 30s DATA."""
        check_eq(self.records[('data', '1', '30s')]['wt'],
                 numpy.array([[ 0.0625    ,  0.11111111,  0.25      ,  1.        ],
                              [ 0.0625    ,  0.11111111,  0.25      ,  1.        ],
                              [ 1.        ,  0.25      ,  0.11111111,  0.0625    ],
                              [ 0.04      ,  0.02777778,  0.02040816,  0.015625  ],
                              [ 1.        ,  1.        ,  1.        ,  1.        ]]),
                 0.001)

    def test_sig_tav30sdata(self):
        """SIGMA after time averaging 30s DATA."""
        check_eq(self.records[('data', '1', '30s')]['sigma'],
                 numpy.array([[4.,     3.,       2.,       1.],
                              [4.,     3.,       2.,       1.],
                              [1.,     2.,       3.,       4.],
                              [5.,     6.,       7.,       8.],
                              [1.,     1.,       1.,       1.]]), 0.001)

    def test_wt_tavdata(self):
        """WEIGHT after time averaging DATA."""
        check_eq(self.records[('data', '1', '60s')]['wt'],
                 numpy.array([[ 0.125    ,  0.22222222,  0.5       ,  2.        ],
                              [ 0.125    ,  0.22222222,  0.5       ,  2.        ],
                              [ 2.       ,  0.5       ,  0.22222222,  0.125     ],
                              [ 0.08     ,  0.05555556,  0.04081633,  0.03125   ],
                              [ 2.       ,  2.        ,  2.        ,  2.        ]]),
                 0.001)

    def test_sig_tavdata(self):
        """SIGMA after time averaging DATA."""
        check_eq(self.records[('data', '1', '60s')]['sigma'],
                 numpy.array([[2.82842708, 2.12132025, 1.41421354, 0.70710677],
                              [2.82842708, 2.12132025, 1.41421354, 0.70710677],
                              [0.70710677, 1.41421354, 2.12132025, 2.82842708],
                              [3.53553414, 4.2426405 , 4.94974756, 5.65685415],
                              [0.70710677, 0.70710677, 0.70710677, 0.70710677]]),
                              0.001)

    def test_wt_cavcorr(self):
        """WEIGHT after channel averaging CORRECTED_DATA."""
        check_eq(self.records[('corrected', '2', '0s')]['wt'],
                 numpy.array([[  2.      ,   8.      ,  18.      ,  32.      ],
                        [  0.125   ,   0.222222,   0.5     ,   2.      ],
                        [  2.      ,   0.5     ,   0.222222,   0.125   ],
                        [  2.      ,   2.      ,   2.      ,   2.      ],
                        [  2.      ,   2.      ,   2.      ,   2.      ]]),
                 0.001)

    def test_sig_cavcorr(self):
        """SIGMA after channel averaging CORRECTED_DATA."""
        check_eq(self.records[('corrected', '2', '0s')]['sigma'],
                 numpy.array([[ 0.70710677,  0.35355338,  0.23570226,  0.17677669],
                        [ 2.82842708,  2.12132144,  1.41421354,  0.70710677],
                        [ 0.70710677,  1.41421354,  2.12132144,  2.82842708],
                        [ 0.70710677,  0.70710677,  0.70710677,  0.70710677],
                        [ 0.70710677,  0.70710677,  0.70710677,  0.70710677]]),
                 0.001)

#After reenabling split_test_wttosig in the context of CAS-11139,
#he tests with time averaging seem not to be working yet.
    def test_wt_tavcorr(self):
        """WEIGHT after time averaging CORRECTED_DATA."""
        check_eq(self.records[('corrected', '1', '60s')]['wt'],
                 numpy.array([[2.      ,8.      ,18.     ,32.     ],
                              [0.125   ,0.222211,0.5     ,2.      ],
                              [2.      ,0.5     ,0.222221,0.125   ],
                              [2.      ,2.      ,2.      ,2.      ],
                              [2.      ,2.      ,2.      ,2.      ]]), 0.001)

    def test_sig_tavcorr(self):
        """SIGMA after time averaging CORRECTED_DATA."""
        check_eq(self.records[('corrected', '1', '60s')]['sigma'],
                 numpy.array([[0.70710677, 0.35355338, 0.23570228, 0.17677669],
                              [2.82842708, 2.12137389, 1.41421354, 0.70710677],
                              [0.70710677, 1.41421354, 2.12132621, 2.82842708],
                              [0.70710677, 0.70710677, 0.70710677, 0.70710677],
                              [0.70710677, 0.70710677, 0.70710677, 0.70710677]]), 0.001)

class split_test_singlespw_severalchranges(unittest.TestCase):
    """
    Check that if the selection contains a single SPW but several channel
    ranges within the same SPW, you get as an output a single SPW in the
    data description table. See CAS-11087
    """
    inpms = os.path.join(datapath,'uid___A002_X30a93d_X43e_small.ms')
    outms = 'uid___A002_X30a93d_X43e_small_chanl4.ms'

    def setUp(self):
        try:
            shutil.rmtree(self.outms, ignore_errors=True)
            print("\nChecking DDI after channel selection ranges in single SPW")
            split(self.inpms, self.outms, keepmms=True, field='',
                   spw='1:1~2;5~6', scan='', antenna='',
                   correlation='', timerange='', intent='',
                   array='', uvrange='', observation='',
                   feed='', datacolumn='DATA', keepflags=True,
                   width=1, timebin='0s', combine='')
        except Exception:
            print("Error running split selecting different channel ranges in single SPW from", self.inpms)
            raise

    def tearDown(self):
        shutil.rmtree(self.outms, ignore_errors=True)

    def test_ddi_entries(self):
        """Check that there is a single row in the DDI table."""
        tblocal.open(os.path.join(self.outms,'DATA_DESCRIPTION'))
        nrows_ddi = tblocal.nrows()
        tblocal.close()
        check_eq(nrows_ddi, 1)

@unittest.skip("FLAG_CATEGORY not supported in mstransform (new split)")
class split_test_fc(SplitChecker):
    """
    Check FLAG_CATEGORY after various selections and averagings.
    """
    need_to_initialize = True
    inpms = os.path.join(datapath,'hasfc.ms')
    records = {}

    # records uses these as keys, so they MUST be tuples, not lists.
    # Each tuple is really (datacolumn, width, timebin), but it's called corrsels for
    # compatibility with SplitChecker.
    corrsels = (('21:37:30~21:39:00', 1, '0s'),  # straight selection
                ('',                  2, '0s'),  # channel averaged
                ('',                  1, '20s')) # time averaged

    def do_split(self, trwtb):
        outms = 'fc.ms'
        record = {'ms': outms}

        shutil.rmtree(outms, ignore_errors=True)
        try:
            print("\nChecking FLAG_CATEGORY after %s." % (trwtb,))
            splitran = split(self.inpms, outms, datacolumn='data',
                             field='', spw='', width=trwtb[1], antenna='',
                             timebin=trwtb[2], timerange=trwtb[0],
                             scan='', array='', uvrange='',
                             correlation='')
            tblocal.open(outms)
            record['fc'] = tblocal.getcell('FLAG_CATEGORY', 5)[2]
            categories = tblocal.getcolkeyword('FLAG_CATEGORY', 'CATEGORY')
            tblocal.close()
            shutil.rmtree(outms, ignore_errors=True)
        except Exception as exc:
            print("Error splitting {0} from {1}. Exception: {2}".
                  format(trwtb, self.inpms, exc))
            raise
        self.__class__.records[trwtb] = record
        self.__class__.records['categories'] = categories
        return splitran

    # NOTE: In MSTransform (split), if fewer channels than chanbin are left at
    # the end of the spw, these channels will be dropped.

    def test_fc_categories(self):
        """FLAG_CATEGORY's CATEGORY keyword"""
        check_eq(self.records['categories'],
                 numpy.array(['FLAG_CMD', 'ORIGINAL', 'USER']))

    def test_fc_straightselection(self):
        """FLAG_CATEGORY after straight selection"""
        check_eq(self.records[('21:37:30~21:39:00', 1, '0s')]['fc'],
                 numpy.array([[ True, False, False],
                              [ True, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [ True, False, False],
                              [ True, False, False]]))

    def test_fc_cav(self):
        """FLAG_CATEGORY after channel averaging"""
        check_eq(self.records[('', 2, '0s')]['fc'],
                 numpy.array([[ True, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [ True, False, False]]))

    def test_fc_tav(self):
        """FLAG_CATEGORY after time averaging"""
        check_eq(self.records[('', 1, '20s')]['fc'],
                 numpy.array([[ True, False, False],
                              [ True, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [False, False, False],
                              [ True, False, False],
                              [ True, False, False]]))


''' New tests for split'''
class test_base(unittest.TestCase):

    def setUp_4ants(self):
        # data set with spw=0~15, 64 channels each in TOPO
        self.vis = "Four_ants_3C286.ms"

        if os.path.exists(self.vis):
           os.system('rm -rf '+ self.vis + '*')

        os.system('cp -RH '+os.path.join(self.datapath,self.vis)+' '+ self.vis)

    def setUp_3c84(self):
        # MS is as follows (scan=1):
        #  SpwID   #Chans   Corrs
        #   0      256      RR
        #   0      256      LL
        #   1      128      RR  LL
        #   2      64       RR  RL  LR  LL

        self.vis = '3c84scan1.ms'
        if os.path.exists(self.vis):
           os.system('rm -rf '+ self.vis + '*')

        os.system('cp -RH '+os.path.join(self.datapath,self.vis)+' '+ self.vis)

    def setUp_mixedpol(self):
        # DD table is as follows:
        #  PolID SpwID
        #    0    0
        #    1    1
        #    1    2
        #    0    3

        self.vis = 'split_ddid_mixedpol_CAS-12283.ms'
        if os.path.exists(self.vis):
           os.system('rm -rf '+ self.vis + '*')

        os.system('cp -RH '+os.path.join(self.datapath,self.vis)+' '+ self.vis)

    def setUp_flags(self):
        asdmname = 'test_uid___A002_X997a62_X8c-short' # Flag.xml is modified
        self.vis = asdmname+'.ms'
        self.flagfile = asdmname+'_cmd.txt'

        asdmpath=ctsys.resolve('unittest/split/')
        os.system('ln -sf '+os.path.join(asdmpath,asdmname))
        importasdm(asdmname, convert_ephem2geo=False, flagbackup=False, process_syspower=False, lazy=True,
                   scans='1', savecmds=True)


    def createMMS(self, msfile, axis='auto',scans='',spws=''):
        '''Create MMSs for tests with input MMS'''
        prefix = msfile.rstrip('.ms')
        if not os.path.exists(msfile):
            os.system('cp -RH '+os.path.join(datapath,msfile)+' '+ msfile)

        # Create an MMS for the tests
        self.testmms = prefix + ".test.mms"

        if os.path.exists(self.testmms):
            os.system("rm -rf " + self.testmms)
            os.system("rm -rf " + self.testmms +'.flagversions')

        print("................. Creating test MMS ..................")
        partition(vis=msfile, outputvis=self.testmms, separationaxis=axis, scan=scans, spw=spws)


class splitTests(test_base):
    '''Test the keepflags parameter'''

    def setUp(self):
        self.datapath = ctsys.resolve('unittest/split/')
        self.setUp_4ants()

    def tearDown(self):
        os.system('rm -rf '+ self.vis + '*')
        os.system('rm -rf '+ self.outputms + '*')

    def test_keepflags(self):
        '''split: keepflags=False'''
        self.outputms = 'split_notkeep.ms'

        # Unflag and flag spw=0,15
        flagdata(self.vis, flagbackup=False, mode='list', inpfile=["mode='unflag'","spw='0,15'"])

        # Split scan=31 out
        split(vis=self.vis, outputvis=self.outputms, datacolumn='corrected', scan='31', keepflags=False)

        expected_spws = list(range(1,15))
        msmdt = msmetadata()
        msmdt.open(self.outputms)
        spws = msmdt.spwsforscan(31)
        msmdt.close()
        lspws = spws.tolist()
        self.assertListEqual(expected_spws, lspws)

    def test_split_combine_scan_axis(self):
        """split: raise error when combine=\'scan\' and axis=\'scan\'"""
        # create MMS first
        self.createMMS(self.vis, axis='scan', spws='0,2,3')
        self.outputms = "split_heur1.ms"
        try:
            split(vis=self.testmms, outputvis=self.outputms, timebin='20s', combine='scan', datacolumn='data')
            os.system('rm -rf ' + self.testmms + '*')
        except Exception as instance:
            print('Expected Error: %s'%instance)
            os.system('rm -rf ' + self.testmms + '*')

        print('Expected Error!')

    def test_flagversions(self):
        '''split: raise an error when .flagversions exist'''
        self.outputms = 'spw0.ms'

        os.system('cp -RH ' + self.vis + ' ' + self.outputms)

        # First, create a .flagversions file
        flagdata(vis=self.outputms, flagbackup=True, spw='0', mode='unflag')
        self.assertTrue(os.path.exists(self.outputms+'.flagversions'))

        # Now, delete only the MS and leave the .flagversions in disk
        os.system('rm -rf '+self.outputms)
        with self.assertRaises(RuntimeError):
            split(vis=self.vis, outputvis=self.outputms, spw='0')
        # The next code doesn't work with the __rethrow_casa_exceptions=False in prelude.py
#         with self.assertRaises(IOError):
#             split(vis=self.vis, outputvis=self.outputms, spw='0')
#         print 'Expected Error!'

    def test_numpy_width(self):
        '''split: Automatically convert numpy type to Python type'''
        self.outputms = "split_numpytype.ms"
        bin1 = numpy.int32(64)
        split(vis=self.vis, outputvis=self.outputms, spw='10', datacolumn='data',
                    width=bin1)

        self.assertTrue(os.path.exists(self.outputms))

        # Output should be:
        # spw=0 1 channel
        ret = th.verifyMS(self.outputms, 1, 1, 0, ignoreflags=True)
        self.assertTrue(ret[0],ret[1])

    def test_numpy_width_mms(self):
        '''split: Automatically convert numpy type to Python type in an MMS'''
        self.createMMS(self.vis, axis='auto', spws='0,10')
        # spws are renumbered to 0,1 in the above command

        self.outputms = "split_numpytype.mms"
        bin1 = numpy.int32(64)
        ParallelTaskHelper.bypassParallelProcessing(1)
        # This will cause MS NULL selections in some subMSs that have only spw=0
        split(vis=self.testmms, outputvis=self.outputms, spw='1', datacolumn='data',
              width=bin1)

        os.system('rm -rf ' + self.testmms + '*')
        ParallelTaskHelper.bypassParallelProcessing(0)
        self.assertTrue(ParallelTaskHelper.isParallelMS(self.outputms),'Output should be an MMS')

        # Output should be:
        # spw=0 1 channel
        ret = th.verifyMS(self.outputms, 1, 1, 0, ignoreflags=True)
        self.assertTrue(ret[0],ret[1])

    def test_combinescan_mms(self):
        '''split: combine=scan with axis=scan'''
        self.createMMS(self.vis, axis='scan', spws='0')

        self.outputms = "split_combscan_spw.mms"
        # This should not work because scan length is 89s
        try:
            split(vis=self.testmms, outputvis=self.outputms, datacolumn='data',combine='scan',
                    timebin='100s')
            self.assertTrue(ParallelTaskHelper.isParallelMS(self.outputms),'Output should be an MMS')
        except Exception:
            print('Expected error!')
            os.system('rm -rf ' + self.testmms + '*')

    def test_combinescan_ms(self):
        '''split: combine=scan with axis=scan, keepmms=false'''
        self.createMMS(self.vis, axis='scan', spws='0')

        self.outputms = "split_combscan_spw.ms"
        split(vis=self.testmms, outputvis=self.outputms, datacolumn='data',combine='scan',
                    timebin='100s', keepmms=False)
        os.system('rm -rf ' + self.testmms + '*')
        self.assertFalse(ParallelTaskHelper.isParallelMS(self.outputms),'Output should be an MS')

    def test_combinescan_spw_mms(self):
        '''split: combine=scan with axis=spw'''
        self.createMMS(self.vis, axis='spw', scans='31',spws='0,3,4')

        self.outputms = "split_combscan.mms"
        split(vis=self.testmms, outputvis=self.outputms, datacolumn='data',combine='scan',
                    timebin='100s')
        os.system('rm -rf ' + self.testmms + '*')
        self.assertTrue(ParallelTaskHelper.isParallelMS(self.outputms),'Output should be an MMS')


class splitSpwPoln(test_base):
    '''tests for spw with different polarization shapes
       CAS-3666
    '''

    def setUp(self):
        self.datapath = ctsys.resolve('unittest/split/')
        self.setUp_3c84()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf list.obs')

    def test_split_different_corrs(self):
        '''split: split spws with different shapes'''
        self.outputms = 'split_corrs.ms'
        split(self.vis, outputvis=self.outputms, spw='>0', correlation='RR,LL', datacolumn='DATA')

        # Verify the input versus the output
        myms = ms()
        myms.open(self.vis)
        myms.msselect({'spw':'1,2'})
        inp_nrow = myms.nrow(True)
        myms.close()

        mymd = msmetadata()
        mymd.open(self.outputms)
        out_nrow = mymd.nrows()
        dds = mymd.datadescids()
        mymd.done()

        self.assertEqual(inp_nrow, out_nrow)
        self.assertEqual(dds.size, 2)

        pol_col = th.getVarCol(self.outputms+'/DATA_DESCRIPTION', 'POLARIZATION_ID')
        self.assertEqual(pol_col['r1'][0], 2,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')
        self.assertEqual(pol_col['r2'][0], 3,'Error in POLARIZATION_ID of DATA_DESCRIPTION table')

        # Verify that POLARIZATION table is not re-sized.
        corr_col = th.getVarCol(self.outputms+'/POLARIZATION', 'NUM_CORR')
        self.assertEqual(corr_col.keys().__len__(), 4, 'Wrong number of rows in POLARIZATION table')

    def test_split_chanavg_spw_with_diff_pol_shape(self):
        '''split: channel average spw 0 that has repeated SPW ID'''
        self.outputms = 'split_3cChAvespw0.ms'
        # Create only one output channel
        split(vis=self.vis, outputvis=self.outputms, datacolumn='data', spw='0',
                width=256)

        # verify the metadata of the output
        msmd = msmetadata()
        msmd.open(self.outputms)
        nchan = msmd.nchan(0) # 1
        nrow = msmd.nrows() # 2600
        dds = msmd.datadescids() # 2
        meanfreq = msmd.meanfreq(0) # 4968996093.75
        chanfreq = msmd.chanfreqs(0) # [4.96899609e+09]
        chanwidth = msmd.chanwidths(spw=0, unit='kHz') # 2000
        msmd.done()

        self.assertEqual(dds.size,2,'Wrong number of rows in DD table')
        self.assertEqual(nchan, 1)
        self.assertEqual(nrow, 2600,'Wrong number of rows in DD table')
        self.assertEqual(meanfreq, 4968996093.75)
        self.assertEqual(chanwidth, 2000)
        self.assertAlmostEqual(meanfreq, chanfreq, 1)

        listobs(self.outputms, listfile='list.obs')
        self.assertTrue(os.path.exists('list.obs'), 'Probable error in sub-table re-indexing')

class splitUnsortedPoln(test_base):
    '''tests for DDs with polIDs in unsorted order
       CAS-12283
    '''

    def setUp(self):
        self.datapath = ctsys.resolve('unittest/split/')
        self.setUp_mixedpol()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf list.obs')

    def test_split_unsorted_polids(self):
        '''split: split MS with unsorted polIDs'''
        self.outputms = 'split_unsorted_polids.ms'
        split(self.vis, outputvis=self.outputms, spw='', scan='11', correlation='RR,LL', datacolumn='all')

        # Verify the input versus the output
        myms = ms()
        myms.open(self.vis)
        inp_nrow = myms.nrow(True)
        myms.close()

        mymd = msmetadata()
        mymd.open(self.outputms)
        out_nrow = mymd.nrows()
        dds = mymd.datadescids()
        mymd.done()

        self.assertEqual(inp_nrow, out_nrow)
        self.assertEqual(dds.size, 64)

        # Check that the data description column in the main table is unchanged.
        mytbtool = table()
        mytbtool.open(self.vis)
        ddcol_inp = mytbtool.getcol('DATA_DESC_ID')
        mytbtool.close()
        mytbtool.open(self.outputms)
        ddcol_out = mytbtool.getcol('DATA_DESC_ID')
        mytbtool.close()
        self.assertTrue(ddcol_inp.tolist() == ddcol_out.tolist())

class splitUpdateFlagCmd(test_base):

    def setUp(self):
        self.datapath = '.'
        self.setUp_flags()

    def tearDown(self):
        os.system('rm -rf '+ self.vis)
        os.system('rm -rf '+ self.outputms)
        os.system('rm -rf list.obs')
        os.system('rm -rf spwnames.txt')
        # the asdmname isn't available in the class - recover it from the vis name - everything before ".ms"
        asdmname = self.vis[:self.vis.index('.ms')]
        os.system('rm -rf '+ asdmname)
        os.system('rm -rf '+ asdmname+'_cmd.txt')

    def test_updateFlagcmd1(self):
        '''split: Do not update FLAG_CMD table when spw selection in FLAG_CMD is by name'''
        self.outputms = 'split_spwName.ms'
        split(vis=self.vis, outputvis=self.outputms, spw='1,2', datacolumn='data')
        flagcmd(self.outputms, action='list', savepars=True, outfile='spwnames.txt', useapplied=True)
        self.assertTrue(filecmp.cmp(self.flagfile, 'spwnames.txt',1))

if __name__ == '__main__':
    unittest.main()
