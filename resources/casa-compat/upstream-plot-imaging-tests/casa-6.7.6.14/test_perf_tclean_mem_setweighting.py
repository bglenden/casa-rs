##########################################################################
# test_perf_tclean_mem_setweighting.py
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
# [https://open-jira.nrao.edu/browse/CAS-12743]
#
#
##########################################################################

'''
    Background info for this performance test:

    As per CAS-12166, memory overuse would be proportional to image_size X number_fiels X number_MSs.
    The script runs a tclean command, in cube mode, as it would be run by the pipeilne in the hif_findcont stage.
    This is the first stage where the memory issue of CAS-12166 etc. tickets would manifest and make CASA crash even
    in large memory machines (256 GB).

    Dataset:

    The Pillars dataset is one very practical example to expose one type of memory overuse (and leak) issue of
    CASA 5.4 (and 5.5) that showed up while running the ALMA pipeline in Cycle 6. The major memory issue in
    cycle 6 affected the setWeighting step of tclean, and became a serious burden in operations, together with
    other memory issues. The Pillars dataset is small (~2.2 GB for the target MS) but is a mosaic with many fields.
    The number of fields is the important parameter to expose the setWeighting memory issue.

    Example of a casa.synthesis.imager.memprofile.... file:

    # PID, MemRSS_(VmRSS)_MB, VmWHM_MB, VirtMem_(VmSize)_MB, VmPeak_MB, VmSwap_MB, ProcTime_sec, FDSize, label_checkpoint
    86187,199,199,1158,1158,0,4.795180,256,[Start Run]
    86187,219,219,1171,1171,0,5.58189,256,[Set Weighting]
    86187,219,219,1171,1171,0,5.58762,256,[Start Run]
    86187,221,221,1172,1172,0,5.86129,256,[Start Major Cycle]
    86187,1256,2008,2714,3480,0,135.412207,512,[After initGrid for all mappers]
    86187,1451,2008,3533,3533,0,286.420693,512,[Before finalize for all mappers]
    86187,533,2008,5500,5965,0,315.825281,512,[End Major Cycle]
    86187,526,2008,5492,5965,0,319.698658,512,[Start Major Cycle]
    86187,878,2008,5844,5965,0,319.941591,512,[After initGrid for all mappers]
    86187,843,2008,5809,5965,0,402.641870,512,[Before finalize for all mappers]
    86187,538,2008,5499,5965,0,417.735762,512,[End Major Cycle]
    86187,538,2008,5499,5965,0,420.928097,512,[End Run]
    86187,531,2008,5492,5965,0,420.933379,512,[End SynthesisImager]
    86187,401,2008,5362,5965,0,421.46043,512,[End SynthesisNormalizer]

    Criteria for success and failure

    Compare the MemRSS_(VmRSS)_MB values taken by steps [Set Weighting] and [End SynthesisNormalizer].
    The tolerances used for comparison are:

    |  Maximum 300 MB of memory at Set Weighting step.
    |  Maximum 500 MB of memory at last step, End SynthesisNormalizer (more than this would indicate memory leaks)
    |  Maximum of 1.5 GB of memory used in whole tclean run.

'''
####    Imports     ####
import os
import shutil
import unittest
import re

from casatestutils.imagerhelpers import TestHelpers
th = TestHelpers()

CASA6 = False

try:
    from casatools import ctsys
    from casatasks import casalog, tclean
    from casatasks.private.parallel.parallel_task_helper import ParallelTaskHelper
    from casatasks.private.imagerhelpers.parallel_imager_helper import PyParallelImagerHelper

    CASA6 = True
except ImportError:
    from __main__ import default
    from taskinit import casalog
    from tasks import tclean


if CASA6:
    dataroot = ctsys.resolve('performance/tclean_mem_setweighting/')
else:
    dataroot = os.environ.get('CASAPATH').split()[0] + '/casatestdata/performance/tclean_mem_setweighting/'

input_ms = 'uid___A002_Xb9dfa4_X4724_target_spw16.ms'
datapath = dataroot + input_ms

# Name of original casalog file
logpath = casalog.logfile()

####    Tests     ####
class TestTcleanMemProf(unittest.TestCase):
    ### Set Up
    @classmethod
    def setUpClass(cls):
        '''A class method called before tests in an individual class run'''
        pass

    def setUp(self):
        '''Method called to prepare the test fixture.  This is called immediately before calling the test method'''
        # This test is currently disabled in parallel from the parameter in tclean
        self.parallel = False
        if ParallelTaskHelper.isMPIEnabled():
            self.parallel = True
        # Tclean opens this MS in read-only mode
        os.symlink(datapath, input_ms)

    ### Teardown
    def tearDown(self):
        '''Method called immediately after the test method has been called and the result recorded'''
        # Be careful not to use forward slash or it will delete the directory
        os.unlink(input_ms)
        casalog.setlogfile(str(logpath))
        os.system('rm -rf tclean_memprofile.log')
#        os.system('rm -rf casa.synthesis.imager.memprofile*')


    @classmethod
    def tearDownClass(cls):
        '''A class method called after tests in an individual class have run'''
        # remove all the generated data
        os.system('rm -rf memtest_*')

    def test_tclean_alma_mem_cubemode_mosaic_briggs(self):
        '''Test memory usage in stages of tclean and check against reference values'''

        # Set casalog to a different file
        templogfile = 'tclean_memprofile.log'
        casalog.setlogfile(templogfile)

        imsize = [1344, 1512]
        nchan = 10  # original is nchan=2046
        phasecenter='ICRS 10:43:50.2473 -059.56.48.583'
        imagename = ('memtest_{0}x{1}_uid___A001_X87a_X13d.s28_0.Pillar_3_sci.spw16.mfs.I.findcont'.
             format(imsize[0], imsize[1]))

        tclean(vis=input_ms, imagename=imagename, phasecenter=phasecenter, scan=['17,11,13'],
               restoration=False, datacolumn='data', pbcor=False, spw='0', weighting='briggs',
               intent='OBSERVE_TARGET#ON_SOURCE', threshold='0mJy', robust=0.5, savemodel='none',
               imsize=imsize, stokes='I', nchan=nchan, deconvolver='hogbom', field='Pillar_3',
               npixels=0, niter=0, pblimit=0.2, restoringbeam=[], cell=['0.94arcsec'],
               start='230.490186515GHz', outframe='LSRK', specmode='cube', chanchunks=-1,
               width='0.0610478663509MHz', gridder='mosaic', interactive=False, parallel=False)

        # Check that memory profile file is created
        # Usual name of profile file is composed of: casa.synthesis.imager.memprofile.PID.HOSTNAME.DATE_TIME_when_test_started.txt
        # e.g.: casa.synthesis.imager.memprofile.50431.casa-el7-ts3-dev01.20191022_100639.txt
        # Check if mem profile was created by checking the casa log
        with open(templogfile) as mylog:
            for line in mylog:
                match = re.search('casa.synthesis.imager.memprofile',line)
                if match:
                    str_match = match.string
                    break

        mylog.close()
        self.assertTrue(match, 'Memory profile filename is not found in casalog. Please, check if ~/.casa/rc has a setting synthesis.imager.memprofile.enable: 1')

        # Get name of memprofile created by tclean
        (start,middle,end)=str_match.partition('casa.synthesis.imager.memprofile')
        mem_profile = middle + end.rstrip()
        self.assertTrue(os.path.exists(mem_profile), 'Memory profile file {0} is not found'.format(mem_profile))

        # Get the memory values of the second column named MemRSS_(VmRSS)_MB, for each row
        from collections import OrderedDict
        with open(mem_profile,'r') as mfile:
            memdict = OrderedDict()
            maxFDSize = 0
            for line in mfile:
                linelist = []
                print(line.rstrip())
                if line.startswith('#'):
                    continue

                linelist = line.split(',')
                tclean_step = str(linelist[-1].rstrip())
                memdict[tclean_step.strip('[]')] = int(linelist[1])
                maxFDSize = max(maxFDSize, int(linelist[7]))
        # For information, print the memory profile

        # Compare memory at [Set Weighting] step
        mfile.close()
        max_ref_memory = 300
        (out, msg) = th.check_val_less_than(val=memdict['Set Weighting'], bound=max_ref_memory, valname='Memory at [Set Weigthing] step',
                               testname ="check_val_less_than")

        # print out memory profile for information
        self.assertTrue(out, msg)

        # Compare memory at [End SynthesisNormalizer] step
        # It will check for leaked memory in last step
        max_ref_memory = 500
        (out, msg) = th.check_val_less_than(val=memdict['End SynthesisNormalizer'], bound=max_ref_memory, valname='Memory at [End SynthesisNormalizer] step')
        self.assertTrue(out, msg)

        # Compare maximum memory in whole tclean run
        import operator
        step_max_mem = max(memdict.items(), key=operator.itemgetter(1))[1]
        step_name = max(memdict.items(), key=operator.itemgetter(1))[0]
        max_ref_memory = 1500
        (out, msg) = th.check_val_less_than(val=step_max_mem, bound=max_ref_memory, valname='Memory at ['+step_name+'] step')
        self.assertTrue(out, msg)

        # compare the maximum FDSize in the whole tclean run, expected to peak at 256 here
        (out, msg) = th.check_val_less_than(val=maxFDSize, bound=257, valname='FDSize')
        self.assertTrue(out, "number of available file descriptors exceeds the expected limit " + msg)

####    Suite: Required for CASA5     ####
def suite():
    return[TestTcleanMemProf]

####    Imports     ####
if __name__ == '__main__':
    unittest.main()
