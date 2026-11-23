########################################################################
# test_task_simobserve.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.simulation.simobserve.html
#
#
##########################################################################
import os
import sys
import shutil
import numpy
import glob
import unittest

from casatools import ctsys, image, ms, msmetadata, quanta, atmosphere, table
from casatasks import simobserve
from casatasks.private.simutil import *

# use the global versions of these tools
_ia = image()
_ms = ms()
_msmd = msmetadata()
_qa = quanta()
_at = atmosphere()

#
# Unit test of simobserve task.
#
class simobserve_unittest_base(unittest.TestCase):
    """
    Base class of simobserve unit test.
    The class defines common variables and test methods.
    """
    graphics = "file"
    # Variables
    datapath = ctsys.resolve('unittest/simobserve/')

    thistask = "simobserve"
    imkeys=['max','mean','min','npts','rms','blc','blcf','trc','trcf','sigma','sum','sumsq']
    # relative and absolute tolerance
    # (atol=0. means to ignore absolute tolerance)
    rtol = 5.0e-3
    atol = 0.
    showcomp = False
    teardown = True

    # Test methods
    def _check_file(self, name, msg=""):
        isthere = os.path.exists(name)
        if len(msg) == 0:
            msg = "output file %s was not created because of the task failure"%(name)
        self.assertEqual(isthere, True, msg=msg)

    def _get_imstats(self, name):
            self._check_file(name)
            _ia.open(name)
            stats = _ia.statistics(list=True, verbose=True)
            _ia.close()
            return stats

    def _get_msstats(self, name, column, compval):
        self._check_file(name)
        _ms.open(name)
        stats = _ms.statistics(column, compval)
        _ms.close()
        return stats[list(stats.keys())[0]]

    def _check_imstats(self, name, ref, rtol=None, atol=None):
        # ref: a dictionary of reference statistics or reference image name
        # name: the name of image to compare statistics
        refname = ""
        if type(ref) == str:
            refname = ("'%s'" % os.path.basename(ref))
            # get statistics of reference image
            ref = self._get_imstats(ref)
        # get statistics of  image
        stats = self._get_imstats(name)
        # tolerance
        if not rtol: rtol = self.rtol
        if not atol: atol = self.atol
        # define statistics to compare
        if hasattr(self,'imkeys'):
            compkeys = self.imkeys
        else:
            compkeys = ref.keys()
        print(("Comparing image statistics of '%s' with reference %s" % \
              (name, refname)))
        for key in compkeys:
            if self.showcomp:
                print(("Comparing '%s' (%s): %s (expected: %s)" % \
                      (key, type(stats[key]), str(stats[key]), str(ref[key]))))
            message="image statistic '%s' does not match: %s (expected: %s)" % \
                     (key, str(stats[key]), str(ref[key]))
            if type(stats[key])==str:
                # only maxposf, minposf, blcf, trcf return <type 'str'>
                # these are actually all lists
                ax_stats = [x.strip() for x in stats[key].split(',')]
                ax_ref = [x.strip() for x in ref[key].split(',')]
                # compare dimension of image axes
                self.assertEqual(len(ax_stats),len(ax_ref),msg=message)
                # extract, compare numerical data from axis world coordinates
                for kk in zip(ax_stats, ax_ref):
                    # only check the first element in tuple
                    if qa.isquantity(kk[0]):
                        # test and reference numbers
                        s_val = qa.quantity(kk[0])['value']
                        r_val = qa.quantity(kk[1])['value']
                        ret=numpy.allclose(s_val,r_val,
                                           rtol=rtol,atol=atol)
                        self.assertEqual(ret,True,msg=message)
                    else: # should only be Stokes axis
                        self.assertEqual(kk[0],kk[1],msg=message)
            else:
                # not a string so expect numpy arrays
                ret=numpy.allclose(stats[key],ref[key],
                                   rtol=rtol,atol=atol)
                self.assertEqual(ret,True,msg=message)

    def _check_msstats(self,name,ref, rtol=None, atol=None):
        # ref: a dictionary of reference statistics or reference MS name
        # name: the name of MS to compare statistics
        column = "DATA"
        compval = "real"
        refname = ""
        if type(ref) == str:
            refname = ("'%s'" % os.path.basename(ref))
            # get statistics of reference MS
            ref=self._get_msstats(ref,column,compval)
        stats=self._get_msstats(name,column,compval)
        # tolerance
        if not rtol: rtol = self.rtol
        if not atol: atol = self.atol
        # define statistics to compare
        if hasattr(self,'mskeys'):
            compkeys = self.mskeys
        else:
            compkeys = ref.keys()
        print(("Comparing MS statistics of '%s' with reference %s" % \
              (name, refname)))
        for key in compkeys:
            if self.showcomp:
                print(("Comparing '%s' : %s (expected: %s)" % \
                      (key, str(stats[key]), str(ref[key]))))
            message="MS statistic '%s' does not match: %s (expected: %s)" % \
                     (key, str(stats[key]), str(ref[key]))
            ret=numpy.allclose([stats[key]],[ref[key]],
                               rtol=rtol,atol=atol)
            self.assertEqual(ret,True,msg=message)

    def _check_ptgfile(self, name, refname):
        # name: the name of pointing file to test
        # refname: the name of reference pointing file to compare
        self._check_file(name, "Pointing file %s does not exist" % name)
        self._check_file(refname,\
                         "Reference pointing file %s does not exist" % refname)
        myutil = simutil()
        npts_ref, dirs_ref, times_ref = myutil.read_pointings(refname)
        npts, dirs, times = myutil.read_pointings(name)
        self.assertEqual(npts, npts_ref,\
                         msg="The nuber of pointings differs: %d (expected: %d)" % (npts, npts_ref))
        print(("Comparing %d pointings in '%s' with '%s'" % (npts, name, os.path.basename(refname))))
        timetol = 1.e-3
        dirtol = _qa.convert("0.1arcsec", "deg")['value']
        for ipts in range(npts):
            # time (string -> float)
            if times[ipts] != times_ref[ipts]:
                tc = float(times[ipts])
                tr = float(times_ref[ipts])
                reldiff = abs(tc - tr)/tr
                self.assertTrue(reldiff < timetol,\
                                msg="Integration time differs (%d-th point): %f (expected: %f)" % (ipts, tc, tr))
            # direction (string -> double)
            if dirs[ipts] == dirs_ref[ipts]:
                continue
            ec, xc, yc = myutil.direction_splitter(dirs[ipts])
            er, xr, yr = myutil.direction_splitter(dirs_ref[ipts])
            self.assertEqual(ec, er, msg="The epoch differs (%d-th point): %s (expected: %s)" % (ipts, ec, er))
            self.assertTrue(abs(xc['value']-xr['value']) < dirtol,\
                            msg="R.A. of %dth point differs" % ipts)
            self.assertTrue(abs(yc['value']-yr['value']) < dirtol,\
                            msg="Dec of %dth point differs" % ipts)


    # common helper methods
    def _copy_input(self,datanames=None):
        if not datanames:
            return
        if type(datanames) == str:
            datanames = [datanames]
        if len(datanames) > 0:
            for indata in datanames:
                if os.path.exists(indata): self._remove(indata)
                if os.path.exists(os.path.join(self.datapath,indata)):
                    #print "copying", indata
                    self._copy(os.path.join(self.datapath,indata), indata)

    def _remove(self, path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

    def _copy(self, src, dest):
        if os.path.isdir(src):
            shutil.copytree(src, dest)
        else:
            shutil.copy(src, dest)

    def _get_data_prefix(self,cfgname, project=""):
        if str.upper(cfgname[0:4]) == "ALMA":
            foo=cfgname.replace(';','_')
        else:
            foo = cfgname

        foo=foo.replace(".cfg","")
        sfoo=foo.split('/')
        if len(sfoo)>1: foo=sfoo[-1]

        return project+"."+foo

########################################################################
#
# Test skymodel only simulations
#
class simobserve_sky(simobserve_unittest_base):
    """
    Test skymodel simulations
    - Single step at a time
    - All steps for obsmode = 'int' and 'sd'
    """
    project = simobserve_unittest_base.thistask+"_sky"
    inmodel = "core5ps.skymodel4mod"
    sdantlist = "aca.tp.cfg"
    antlist = "alma.out01.cfg"

    refproj = "ref_sky"
    refpref = os.path.join(simobserve_unittest_base.datapath,refproj) + "/"

    prevmsg = "The noise level differs from the previous value: %f (previous: %f)"

    # Reserved methods
    def setUp(self):
        print("")
        if os.path.exists(self.project):
            shutil.rmtree(self.project)

        self.refpref_sd = self.refpref + \
                          self._get_data_prefix(self.sdantlist,self.refproj)
        self.refpref_int = self.refpref + \
                           self._get_data_prefix(self.antlist,self.refproj)
        self.refmodel = self.refpref_sd+".skymodel" # reference modified skymodel
        # reference simulated MS
        self.refms_sd = self.refpref_sd+".sd.ms"
        self.refms_sd_noisy = self.refpref_sd+".noisy.sd.ms"
        self.refms_int = self.refpref_int+".ms"
        self.refms_int_fp = self.refpref_int+".fullpol.ms"

    def tearDown(self):
        if self.teardown and os.path.exists(self.project):
            shutil.rmtree(self.project)
            for stray_file in [self.inmodel, self.sdantlist, self.antlist]:
                try:
                    self._remove(stray_file)
                except(FileNotFoundError, NotADirectoryError):
                    pass

    # Tests of skymodel simulations
    def testSky_skymodel(self):
        """Test skymodel simulation: only modify model"""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        #setpointings = False
        #ptgfile =   # necessary even if only modifymodel
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,obsmode=obsmode,
                       antennalist=antennalist,sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare skymodel
        currmodel = self.project + "/" + \
                    self._get_data_prefix(antennalist,self.project)+".skymodel"
        self._check_imstats(currmodel, self.refmodel)

    def testSky_almaptg(self):
        """Test skymodel simulation: only setpointing (maptype='ALMA')"""
        skymodel = self.refmodel
        setpointings = True
        maptype = "ALMA"
        obsmode = ""
        antennalist = "alma.out01.cfg"
        sdantlist = ""
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(antennalist,self.project)+".ptg.txt"
        refptg = self.refpref + "alma.alma.out01.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    def testSky_hexptg(self):
        """Test skymodel simulation: only setpointing (maptype='hexagonal')"""
        skymodel = self.refmodel
        setpointings = True
        maptype = "hexagonal"
        obsmode = ""
        antennalist = "aca.i.cfg"
        sdantlist = ""
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(antennalist,self.project)+".ptg.txt"
        refptg = self.refpref + "hex.aca.i.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    def testSky_sqptg(self):
        """Test skymodel simulation: only setpointing (maptype='square')"""
        skymodel = self.refmodel
        setpointings = True
        maptype = "square"
        obsmode = ""
        antennalist = ""
        sdantlist = self.sdantlist
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(sdantlist,self.project)+".ptg.txt"
        refptg = self.refpref + "square.aca.tp.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    def testSky_sdObs(self):
        """Test skymodel simulation: only observation (SD)"""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_sd + ".ptg.txt"
        integration = "4s"
        obsmode = "sd"
        sdantlist = self.sdantlist
        totaltime = "576s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       sdantlist=sdantlist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(sdantlist,self.project)+".sd.ms"
        self._check_msstats(currms,self.refms_sd)

    def testSky_sdObs_noise(self):
        """Test skymodel simulation: observation (SD) with added thermal noise"""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_sd + ".ptg.txt"
        integration = "4s"
        obsmode = "sd"
        sdantlist = self.sdantlist
        totaltime = "576s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       sdantlist=sdantlist,totaltime=totaltime,
                       thermalnoise="tsys-manual",
                       t_ground = 263.0, t_sky = 263.0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # compare output MSs
        currms = self.project + "/" + \
                 self._get_data_prefix(sdantlist,self.project)+".sd.ms"
        self._check_msstats(currms,self.refms_sd)
        currms = self.project + "/" + \
                 self._get_data_prefix(sdantlist,self.project)+".noisy.sd.ms"
        self._check_msstats(currms,self.refms_sd_noisy)


    def testSky_intObs(self):
        """Test skymodel simulation: only observation (INT)"""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)+".ms"
        self._check_msstats(currms,self.refms_int)

    def testSky_intObs_namedAntlist(self):
        """Test skymodel simulation: observation (INT) with ACA configuration file containing an extra column to indirectly exercise readantenna method of simutil"""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = "aca.cycle7.named.cfg"
        totaltime = "28s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # ensure named are filled in MS
        currms = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)+".ms"
        _msmd.open(currms)
        names = _msmd.antennanames()
        _msmd.done()
        self.assertEqual(names,['CM01', 'CM02', 'CM03', 'CM04', 'CM05',
                                'CM06', 'CM07', 'CM08', 'CM09', 'CM10'])


    def testSky_intLeak(self):
        """Test skymodel simulation: only observation (INT) with leakage>0"""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        leakage = 0.1
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics,
                       leakage=leakage,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)+".fullpol.ms"
        self._check_msstats(currms,self.refms_int_fp)

    def testSky_intLeak_noise(self):
        """Test INT thermal noise (tsys-manual) in full pol with leakage>0"""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        leakage = 0.1
        thermalnoise="tsys-manual"
        tau0=1.0
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise=thermalnoise, tau0=tau0,
                       graphics=self.graphics,
                       leakage=leakage,
                       refdate="2014/05/21")
        except Exception:
            self.fail()

        # check for output file
        noisyms = self.project+'/simobserve_sky.alma.out01.noisier.ms'
        origms = self.project+'/simobserve_sky.alma.out01.fullpol.ms'
        msnoise = self._get_noise(noisyms, origms)

        # Now compare the result
        refval = 3.477825 # old 9.78451847017  # testing only REAL part
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))




    def testSky_sdAll(self):
        """Test skymodel simulation: single dish"""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        integration = "4s"
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "576s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                 self._get_data_prefix(sdantlist,self.project)
        self._check_imstats(currpref+".skymodel", self.refmodel)
        self._check_ptgfile(currpref+".ptg.txt", self.refpref_sd+".ptg.txt")
        self._check_msstats(currpref+".sd.ms",self.refms_sd)

    def testSky_intAll(self):
        """Test skymodel simulation: interferometer"""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        integration = "4s"
        mapsize = ['20arcsec', '20arcsec']
        maptype = "ALMA"
        obsmode = 'int'
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist=antennalist,
                       thermalnoise="",graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)
        self._check_imstats(currpref+".skymodel", self.refmodel)
        self._check_ptgfile(currpref+".ptg.txt", self.refpref_int+".ptg.txt")
        self._check_msstats(currpref+".ms",self.refms_int)


    #-----------------------------------------------------------------#
    # Helper function
    def _get_noise(self, noisyms, origms):
        tb.open(noisyms)
        noisy_data = tb.getcol("DATA")
        tb.close()
        tb.open(origms)
        orig_data = tb.getcol("DATA")
        tb.close()
        diff_data = noisy_data - orig_data
        return diff_data.real.std()


########################################################################
#
# Test components list only simulations
#
class simobserve_comp(simobserve_unittest_base):
    """
    Test components list simulations
    """
    """
    Test components list simulations
    - Single step at a time
    - All steps for obsmode = 'int' and 'sd'
    """
    project = simobserve_unittest_base.thistask+"_comp"
    incomp = "core5ps.clist"
    point_comp = 'point.cl'
    compwidth = "10MHz"
    comp_nchan = 1
    direction = "J2000 19h00m00 -23d00m00"
    sdantlist = "aca.tp.cfg"
    antlist = "alma.out01.cfg"

    refproj = "ref_comp"
    refpref = os.path.join(simobserve_unittest_base.datapath,refproj) + "/"

    # Reserved methods
    def setUp(self):
        print("")
        if os.path.exists(self.project):
            shutil.rmtree(self.project)

        self.refpref_sd = self.refpref + \
                          self._get_data_prefix(self.sdantlist,self.refproj)
        self.refpref_int = self.refpref + \
                           self._get_data_prefix(self.antlist,self.refproj)
        self.refmodel = self.refpref_sd+".compskymodel" # reference skymodel
        self.refmodel_int = self.refpref_int+".compskymodel" # reference skymodel
        # reference simulated MS
        self.refms_sd = self.refpref_sd+".sd.ms"
        self.refms_int = self.refpref_int+".ms"

        # new data for comp_nchan > 1
        self.refmodel_int_8ch = self.refpref_int+".8ch.compskymodel"
        self.refms_int_8ch = self.refpref_int+".8ch.ms"

        # copy input components list
        self._copy_input(self.incomp)
        self._copy_input(self.point_comp)

    def tearDown(self):
        if self.teardown:
            if os.path.exists(self.project):
                shutil.rmtree(self.project)
            if os.path.exists(self.incomp):
                shutil.rmtree(self.incomp)
            if os.path.exists(self.point_comp):
                shutil.rmtree(self.point_comp)

    # Tests of complist simulations
    def testComp_complist(self):
        """Test complist simulation: only generating input model"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=True,obsmode=obsmode,
                       antennalist=antennalist,sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare compskymodel
        currmodel = self.project + "/" + \
                    self._get_data_prefix(antennalist,self.project)+".compskymodel"
        self._check_imstats(currmodel, self.refmodel_int)

    def testComp_almaptg(self):
        """Test complist simulation: only setpointing (maptype='ALMA')"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = True
        maptype = "ALMA"
        obsmode = ""
        antennalist = "alma.out01.cfg"
        sdantlist = ""
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(antennalist,self.project)+".ptg.txt"
        refptg = self.refpref + "alma.alma.out01.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    def testComp_hexptg(self):
        """Test complist simulation: only setpointing (maptype='hexagonal')"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = True
        maptype = "hexagonal"
        obsmode = ""
        antennalist = "aca.i.cfg"
        sdantlist = ""
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()

        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(antennalist,self.project)+".ptg.txt"
        refptg = self.refpref + "hex.aca.i.ptg.txt"
        self._check_ptgfile(currptg, refptg)


    def testComp_sqptg(self):
        """Test complist simulation: only setpointing (maptype='square')"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = True
        maptype = "square"
        obsmode = ""
        antennalist = ""
        sdantlist = self.sdantlist
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(sdantlist,self.project)+".ptg.txt"
        refptg = self.refpref + "square.aca.tp.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    @unittest.skip('Previously disabled with comment: "TEMPORARY discarding due to the bug in simulator. Pending CAS-5095."')
    def testComp_sdObs(self):
        """Test complist simulation: only observation (SD)"""
        complist = self.incomp
        compwidth = self.compwidth
        setpointings = False
        ptgfile = self.refpref_sd + ".ptg.txt"
        integration = "4s"
        obsmode = "sd"
        sdantlist = self.sdantlist
        totaltime = "144s"
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       sdantlist=sdantlist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(sdantlist,self.project)+".sd.ms"
        self._check_msstats(currms,self.refms_sd)

    def testComp_intObs(self):
        """Test complist simulation: only observation (INT)"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)+".ms"
        self._check_msstats(currms,self.refms_int)


    @unittest.skip('Previously disabled for unknown reason.')
    def testComp_intLeak(self):
        """Test complist simulation: only observation (INT)"""
        complist = self.incomp
        compwidth = self.compwidth
        setpointings = False
        obsmode = ""
        leakage = 0.5
        simobserve(project=self.project,complist=complist,
                   compwidth=compwidth,comp_nchan=comp_nchan,
                   setpointings=setpointings,ptgfile=ptgfile,
                   obsmode=obsmode,thermalnoise="",
                   leakage=leakage,graphics=self.graphics)

    @unittest.skip('Previously disabled with comment: "TEMPORARY discarding due to the bug in simulator. Pending CAS-5095."')
    def testComp_sdAll(self):
        """Test complist simulation: single dish"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        integration = "4s"
        direction = self.direction
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "144s"
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth =
                       compwidth,comp_nchan=comp_nchan,setpointings=True,
                       integration=integration,direction=direction,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                   self._get_data_prefix(sdantlist,self.project)
        self._check_imstats(currpref+".compskymodel", self.refmodel)
        self._check_ptgfile(currpref+".ptg.txt", self.refpref_sd+".ptg.txt")
        self._check_msstats(currpref+".sd.ms",self.refms_sd)

    def testComp_intAll(self):
        """Test complist simulation: interferometer"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        integration = "4s"
        direction = self.direction
        mapsize = ['20arcsec', '20arcsec']
        maptype = "ALMA"
        obsmode = 'int'
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=True,integration=integration,
                       direction=direction,mapsize=mapsize,maptype=maptype,
                       obsmode=obsmode,totaltime=totaltime,
                       antennalist=antennalist,thermalnoise="",
                       graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)
        self._check_imstats(currpref+".compskymodel", self.refmodel_int)
        self._check_ptgfile(currpref+".ptg.txt", self.refpref_int+".ptg.txt")
        self._check_msstats(currpref+".ms",self.refms_int)

    def testComp_intNchan(self):
        """Test complist simulation: interferometer, but with comp_nchan > 1"""
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = 8
        integration = "4s"
        direction = self.direction
        mapsize = ['20arcsec', '20arcsec']
        maptype = "ALMA"
        obsmode = 'int'
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,complist=complist,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       setpointings=True,
                       integration=integration,direction=direction,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist=antennalist,
                       thermalnoise="",graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)
        self._check_imstats(currpref+".compskymodel",
                            self.refmodel_int_8ch)
        self._check_ptgfile(currpref+".ptg.txt",
                            self.refpref_int+".8ch.ptg.txt")
        self._check_msstats(currpref+".ms",
                            self.refms_int_8ch)


    def testComp_plp(self):
        """Test plp (spectral dependency) is supported CAS-13776"""
        complist = self.point_comp
        antennalist = 'alma.cycle5.1.cfg'
        totaltime = '2000s'
        direction = 'J2000 10h00m00.08s -30d00m00.0s'
        try:
            simobserve(
                project=self.project, complist=complist, compwidth='10GHz',
                direction=direction, obsmode="int",
                antennalist=antennalist, totaltime=totaltime,
                mapsize="10arcsec", thermalnoise='', comp_nchan=10
            )
        except Exception:
            self.fail()
        ms = os.path.join(
            self.project, self.project + '.' + antennalist[:-4] + '.ms'
        )
        tb = table()
        tb.open(ms)
        x = tb.getcol('DATA')
        spw = os.path.join(ms, 'SPECTRAL_WINDOW')
        tb.open(spw)
        f = tb.getcol('CHAN_FREQ')
        tb.done()
        k = f/230e9

        expect = (7 * k**(2 + 3*numpy.log(k)))[:, 0]
        got = x[0, :, 0]
        self.assertTrue(numpy.isclose(numpy.real(got), expect).all(), f'Failed plp test got {numpy.real(got)} \n expect {expect} \n {numpy.real(got) - expect}')


########################################################################
#
# Test skymodel + components list simulations
#
class simobserve_skycomp(simobserve_unittest_base):
    """
    Test skymodel + components list simulations
    - Single step at a time
    - All steps for obsmode = 'int' and 'sd'
    """
    project = simobserve_unittest_base.thistask+"_sc"
    inmodel = "core1.skymodel"
    indirection = "J2000 19h00m00 -23d00m00"
    incomp = "ps5.clist"
    compwidth = "10MHz"
    comp_nchan = 1
    sdantlist = "aca.tp.cfg"
    antlist = "alma.out01.cfg"

    refproj = "ref_sky"
    refpref = os.path.join(simobserve_unittest_base.datapath,refproj) + "/"
    # relative tolerance (larger tolerance to compare with ref_sky)
    rtol = 2.0e-2        # Image
    rtol_sdms = 4.0e-2   # SD MS
    rtol_intms = 1.5e-1  # INT MS (sum and mean give ~13% difference)
    # types of MS statistics tested
    mskeys = ["rms", "min", "max", "stddev", "npts", "medabsdevmed", "firstquartile", "sumsq", "sum", "mean"]#, "median"

    # Reserved methods
    def setUp(self):
        print("")
        if os.path.exists(self.project):
            shutil.rmtree(self.project)

        self.refpref_sd = self.refpref + \
                          self._get_data_prefix(self.sdantlist,self.refproj)
        self.refpref_int = self.refpref + \
                           self._get_data_prefix(self.antlist,self.refproj)
        self.refmodel = self.refpref_sd+".skymodel.flat" # reference flat skymodel
        # reference simulated MS
        self.refms_sd = self.refpref_sd+".sd.ms"
        self.refms_int = self.refpref_int+".ms"
        # copy input data
        self._copy_input([self.incomp, self.inmodel])

    def tearDown(self):
        if self.teardown:
            if os.path.exists(self.project):
                shutil.rmtree(self.project)
            if os.path.exists(self.incomp):
                shutil.rmtree(self.incomp)
            if os.path.exists(self.inmodel):
                self._remove(self.inmodel)

    # Tests of skymodel + components list simulations
    def testSC_skymodel(self):
        """Test skymodel + complist simulation: only modify model"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        #setpointings = False
        #ptgfile =   # necessary even if only modifymodel
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=True,obsmode=obsmode,
                       antennalist=antennalist,sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare skymodel
        currmodel = self.project + "/" + \
                    self._get_data_prefix(antennalist,self.project)+".skymodel.flat"
                    #self._get_data_prefix(antennalist,self.project)+".skymodel"
        self._check_imstats(currmodel, self.refmodel)

    def testSC_almaptg(self):
        """Test skymodel + complist simulation: only setpointing (maptype='ALMA')"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = True
        maptype = "ALMA"
        obsmode = ""
        antennalist = "alma.out01.cfg"
        sdantlist = ""
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(antennalist,self.project)+".ptg.txt"
        refptg = self.refpref + "alma.alma.out01.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    def testSC_hexptg(self):
        """Test skymodel + complist simulation: only setpointing (maptype='hexagonal')"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = True
        maptype = "hexagonal"
        obsmode = ""
        antennalist = "aca.i.cfg"
        sdantlist = ""
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(antennalist,self.project)+".ptg.txt"
        refptg = self.refpref + "hex.aca.i.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    def testSC_sqptg(self):
        """Test skymodel + complist simulation: only setpointing (maptype='square')"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = True
        maptype = "square"
        obsmode = ""
        antennalist = ""
        sdantlist = self.sdantlist
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=setpointings,maptype=maptype,
                       obsmode=obsmode,antennalist=antennalist,
                       sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare pointing files
        currptg = self.project + "/" + \
                  self._get_data_prefix(sdantlist,self.project)+".ptg.txt"
        refptg = self.refpref + "square.aca.tp.ptg.txt"
        self._check_ptgfile(currptg, refptg)

    @unittest.skip('Previously disabled with comment: "TEMPORARY discarding due to the bug in simulator. Pending CAS-5095."')
    def testSC_sdObs(self):
        """Test skymodel + complist simulation: only observation (SD)"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = False
        ptgfile = self.refpref_sd + ".ptg.txt"
        integration = "4s"
        obsmode = "sd"
        sdantlist = self.sdantlist
        totaltime = "144s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       sdantlist=sdantlist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(sdantlist,self.project)+".sd.ms"
        self._check_msstats(currms,self.refms_sd,rtol=self.rtol_sdms)

    def testSC_intObs(self):
        """Test skymodel + complist simulation: only observation (INT)"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare output MS
        currms = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)+".ms"
        self._check_msstats(currms,self.refms_int,rtol=self.rtol_intms)

    @unittest.skip('Previously disabled for unknown reason.')
    def testSC_intLeak(self):
        """Test skymodel + complist simulation: only leakage (INT)"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        setpointings = False
        obsmode = ""
        leakage = 0.5
        simobserve(project=self.project,skymodel=skymodel,
                   complist=complist,compwidth=compwidth,
                   comp_nchan=comp_nchan,
                   setpointings=setpointings,ptgfile=ptgfile,
                   obsmode=obsmode,thermalnoise="",
                   leakage=leakage,graphics=self.graphics)

    @unittest.skip('Previously disabled with comment: "TEMPORARY discarding due to the bug in simulator. Pending CAS-5095."')
    def testSC_sdAll(self):
        """Test skymodel + complist simulation: single dish"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        integration = "4s"
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "144s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                   self._get_data_prefix(sdantlist,self.project)
        self._check_imstats(currpref+".skymodel.flat", self.refmodel)
        self._check_ptgfile(currpref+".ptg.txt", self.refpref_sd+".ptg.txt")
        self._check_msstats(currpref+".sd.ms",self.refms_sd,rtol=self.rtol_sdms)

    def testSC_intAll(self):
        """Test skymodel + complist simulation: interferometer"""
        skymodel = self.inmodel
        complist = self.incomp
        compwidth = self.compwidth
        comp_nchan = self.comp_nchan
        integration = "4s"
        mapsize = ['20arcsec', '20arcsec']
        maptype = "ALMA"
        obsmode = 'int'
        antennalist = 'alma.out01.cfg'
        totaltime = "28s"
        try:
            simobserve(project=self.project,skymodel=skymodel,
                       complist=complist,compwidth=compwidth,
                       comp_nchan=comp_nchan,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist=antennalist,
                       thermalnoise="",graphics=self.graphics)
        except Exception:
            self.fail()
        # compare outputs
        currpref = self.project + "/" + \
                 self._get_data_prefix(antennalist,self.project)
        self._check_imstats(currpref+".skymodel.flat", self.refmodel)
        self._check_ptgfile(currpref+".ptg.txt", self.refpref_int+".ptg.txt")
        self._check_msstats(currpref+".ms",self.refms_int,rtol=self.rtol_intms)


########################################################################
#
# Test noise calculations
#
class simobserve_noise(simobserve_unittest_base):
    """
    Test noise level of simulated MS
    """
    # global variables of the class
    inimage = "flatone.model"
    ptgfile = "flatone.single.ptg.txt"
    indata = [inimage, ptgfile]

    # standard parameter settings
    project = "noise_sd"
    project_int = "noise_int"
    tint = "4s"
    tottime = "1800s" # 30min
    mapsize = ["5arcsec","5arcsec"] # single pointing
    pointingspacing = "10arcsec"
    sdantlist = "aca.tp.cfg"
    antennalist = ""
    tau0 = 1.0
    pwv = 1.0
    graphics = 'file'

    skymodel = project + "/" + project + ".aca.tp.model"

    prevmsg = "The noise level differs from the previous value: %f (previous: %f)"
    anamsg = "The noise level differs more than 10%% from the analytic value: %f (analytic: %f)"

    # Reserved methods
    def setUp(self):
        # Add new line for better reading (these tests always print errors).
        print("")
        for simdir in [self.project, self.project_int]:
            if os.path.exists(simdir):
                shutil.rmtree(simdir)

        self._copy_input(self.indata)

    def tearDown(self):
        if self.teardown:
            if (os.path.exists(self.inimage)):
                shutil.rmtree(self.inimage)
            if os.path.exists(self.project):
                shutil.rmtree(self.project)
            if os.path.exists(self.ptgfile):
                os.remove(self.ptgfile)

    #-----------------------------------------------------------------#
    # thermalnoise = "tsys-manual"
    def testNZ_intMan(self):
        """Test INT thermal noise (tsys-manual)"""
        project = self.project_int
        self._copy_input(project)
        skymodel = project+"/noise_int.aca_cycle1.model"
        antlist = "aca_cycle1.cfg"
        thermalnoise="tsys-manual"
        try:
            simobserve(project=project,skymodel=skymodel,
                       setpointings=False,integration=self.tint,
                       obsmode='',sdantlist="",antennalist=antlist,
                       thermalnoise=thermalnoise,tau0=self.tau0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(project,antlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=False,aca7m=True)
        #print "MS noise:", msnoise
        #print "Analytic:", ananoise
        # Now compare the result
        refval = 9.78451847017  # testing only REAL part
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_intMan_Leak(self):
        """Test INT thermal noise (tsys-manual) in full pol with leakage>0"""
        project = self.project_int
        self._copy_input(project)
        skymodel = project+"/noise_int.aca_cycle1.model"
        antlist = "aca_cycle1.cfg"
        thermalnoise="tsys-manual"
        leakage = 0.1
        try:
            simobserve(project=project,skymodel=skymodel,
                       setpointings=False,integration=self.tint,
                       obsmode='int',sdantlist="",antennalist=antlist,
                       thermalnoise=thermalnoise,tau0=self.tau0,
                       leakage=leakage,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(project,antlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=False,aca7m=True)
        #print "MS noise:", msnoise
        #print "Analytic:", ananoise
        # Now compare the result
        refval = 9.517164  # testing only REAL part
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdMan(self):
        """Test SD thermal noise (tsys-manual): standard parameter set"""
        thermalnoise="tsys-manual"
        self._copy_input(self.project)
        try:
            simobserve(project=self.project,skymodel=self.skymodel,
                       setpointings = False,integration=self.tint,
                       obsmode="",sdantlist=self.sdantlist,antennalist="",
                       thermalnoise=thermalnoise,tau0=self.tau0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=True)
        # Now compare the result
        refval = 4.91379000092
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdMan_tau(self):
        """Test SD thermal noise (tsys-manual): tau0=1.5"""
        thermalnoise="tsys-manual"
        tau0 = 1.5
        self._copy_input(self.project)
        try:
            simobserve(project=self.project,skymodel=self.skymodel,
                       setpointings = False,integration=self.tint,
                       obsmode="",sdantlist=self.sdantlist,antennalist="",
                       thermalnoise=thermalnoise,tau0=tau0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=True,tau0=tau0)
        # Now compare the result
        refval = 9.27620818144
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdMan_dnu(self):
        """Test SD thermal noise (tsys-manual): inwidth='1MHz'"""
        thermalnoise="tsys-manual"
        inwidth = '1MHz'
        # need to recalculate skymodel and MS
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       inwidth=inwidth,setpointings=False,
                       ptgfile=self.ptgfile,integration=self.tint,
                       obsmode='sd',sdantlist=self.sdantlist,
                       antennalist=self.antennalist,totaltime=self.tottime,
                       thermalnoise=thermalnoise,tau0=self.tau0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=True,dnu=inwidth)
        # Now compare the result
        refval = 15.5387677134
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdMan_tint(self):
        """Test SD thermal noise (tsys-manual): integration='2s'"""
        thermalnoise="tsys-manual"
        integration = '2s'
        totaltime = '900s'
        # need to recalculate MS
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       setpointings=False,ptgfile=self.ptgfile,
                       integration=integration,
                       obsmode='sd',sdantlist=self.sdantlist,
                       antennalist=self.antennalist,totaltime=totaltime,
                       thermalnoise=thermalnoise,tau0=self.tau0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=True,integration=integration)
        # Now compare the result
        refval = 6.94461790663
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdMan_el(self):
        """Test SD thermal noise (tsys-manual): elevation = 60 deg"""
        thermalnoise="tsys-manual"
        indir = 'J2000 19h00m00 -53d00m00'
        # need to recalculate ptgs and MS
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       indirection=indir,setpointings=True,
                       integration=self.tint,mapsize=self.mapsize,
                       pointingspacing=self.pointingspacing,
                       obsmode='sd',sdantlist=self.sdantlist,
                       antennalist=self.antennalist,totaltime=self.tottime,
                       thermalnoise=thermalnoise,tau0=self.tau0,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="manual",sd=True,dir=-53.)
        #print "MS noise:", msnoise
        #print "Analytic:", ananoise
        # Now compare the result
        refval = 6.0450620991
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    #-----------------------------------------------------------------#
    # thermalnoise = "tsys-atm"
    @unittest.skip("disabled pending change in CAS-12496")
    def testNZ_intAtm(self):
        """Test INT thermal noise (tsys-atm): standard parameter set"""
        project = self.project_int
        self._copy_input(project)
        skymodel = project+"/noise_int.aca_cycle1.model"
        antlist = "aca_cycle1.cfg"
        thermalnoise="tsys-atm"
        try:
            simobserve(project=project,skymodel=skymodel,
                       setpointings=False,integration=self.tint,
                       obsmode='',sdantlist="",antennalist=antlist,
                       thermalnoise=thermalnoise,user_pwv=self.pwv,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(project,antlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="atm",sd=False,aca7m=True)
        # Now compare the result
        refval = 2.27105136133
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdAtm(self):
        """Test SD thermal noise (tsys-atm): standard parameter set"""
        thermalnoise="tsys-atm"
        self._copy_input(self.project)
        try:
            simobserve(project=self.project,skymodel=self.skymodel,
                       setpointings = False,integration=self.tint,
                       obsmode="",sdantlist=self.sdantlist,antennalist="",
                       thermalnoise=thermalnoise,user_pwv=self.pwv,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="atm",sd=True)
        # Now compare the result
        refval = 1.067681 # old 1.13985820952
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdAtm_pwv(self):
        """Test SD thermal noise (tsys-atm): pwv = 2.0"""
        thermalnoise="tsys-atm"
        pwv = 2.0
        self._copy_input(self.project)
        try:
            simobserve(project=self.project,skymodel=self.skymodel,
                       setpointings = False,integration=self.tint,
                       obsmode="",sdantlist=self.sdantlist,antennalist="",
                       thermalnoise=thermalnoise,user_pwv=pwv,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="atm",sd=True,pwv=pwv)
        # Now compare the result
        refval = 1.523367 # old 1.61886644931
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdAtm_dnu(self):
        """Test SD thermal noise (tsys-atm): inwidth='1MHz'"""
        thermalnoise="tsys-atm"
        inwidth = '1MHz'
        # need to recalculate skymodel and MS
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       inwidth=inwidth,setpointings=False,
                       ptgfile=self.ptgfile,integration=self.tint,
                       obsmode='sd',sdantlist=self.sdantlist,
                       antennalist=self.antennalist,totaltime=self.tottime,
                       thermalnoise=thermalnoise,user_pwv=self.pwv,
                       graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="atm",sd=True,dnu=inwidth)
        # Now compare the result
        refval = 3.382055 # old 3.60454794841
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdAtm_tint(self):
        """Test SD thermal noise (tsys-atm): integration = '2s'"""
        thermalnoise="tsys-atm"
        integration = '2s'
        totaltime = '900s'
        # need to recalculate MS
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       setpointings=False,ptgfile=self.ptgfile,
                       integration=integration,
                       obsmode='sd',sdantlist=self.sdantlist,
                       antennalist=self.antennalist,totaltime=totaltime,
                       thermalnoise=thermalnoise,user_pwv=self.pwv,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="atm",sd=True,integration=integration)
        # Now compare the result
        refval = 1.509632 # old 1.61165299786
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    def testNZ_sdAtm_el(self):
        """Test SD thermal noise (tsys-atm): elevation = 60 deg"""
        thermalnoise="tsys-atm"
        indir = 'J2000 19h00m00 -53d00m00'
        # need to recalculate ptgs and MSes
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       indirection=indir,setpointings=True,
                       integration=self.tint,mapsize=self.mapsize,
                       pointingspacing=self.pointingspacing,
                       obsmode='sd',sdantlist=self.sdantlist,
                       antennalist=self.antennalist,totaltime=self.tottime,
                       thermalnoise=thermalnoise,user_pwv=self.pwv,
                       graphics=self.graphics)
        except Exception:
            self.fail()
        # check for output file
        msdict = self._get_ms_names(self.project,self.sdantlist)
        if msdict is None:
            self.fail("Could not find output MSes")
        noisyms = msdict['noisy']
        origms = msdict['original']
        msnoise = self._get_noise(noisyms, origms)
        ananoise = self._calc_alma_noise(mode="atm",sd=True,dir=-53.)
        #print "MS noise:", msnoise
        #print "Analytic:", ananoise
        # Now compare the result
        refval = 1.145133 # old 1.22177450558
        self.assertTrue(abs((msnoise-refval)/refval) < 5.e-2,\
                        msg=self.prevmsg % (msnoise, refval))
        self.assertTrue(abs((msnoise-ananoise)/ananoise) < 1.e-1, \
                        msg=self.anamsg % (msnoise, ananoise))

    #-----------------------------------------------------------------#
    # Helper functions
    def _get_ms_names(self, project, antennalist):
        retDict = {"original": None, "noisy": None}
        prefix = project+"/"+self._get_data_prefix(antennalist, project)
        mslist = glob.glob(prefix+"*.noisier*.ms")
        if len(mslist) > 0:
            retDict['noisy'] = mslist[0]
        else:
            mslist = glob.glob(prefix+"*.noisy*.ms")
            if len(mslist) > 0:
                retDict['noisy'] = mslist[0]
            else:
                return None

        if os.path.exists(prefix+".ms"):
            retDict["original"] = prefix+".ms"
        elif os.path.exists(prefix+".sd.ms"):
            retDict["original"] = prefix+".sd.ms"
        else:
            return None

        return retDict

    def _get_noise(self, noisyms, origms):
        tb.open(noisyms)
        noisy_data = tb.getcol("DATA")
        tb.close()
        tb.open(origms)
        orig_data = tb.getcol("DATA")
        tb.close()
        diff_data = noisy_data - orig_data
        #if (diff_data.imag != 0).any():
        #    return [diff_data.real.std(),diff_data.imag.std()]
        #else:
        #    return [diff_data.real.std()]
        return diff_data.real.std()

    def _calc_alma_noise(self, mode="manual",sd=True,aca7m=False,\
                         freq="345GHz",dnu="10MHz",integration='4s',dir=-23.,\
                         pwv=1.0,tground=269.,tsky=263.,tau0=1.0):
        if sd:
            senscoeff = 1.0
        else:
            senscoeff = 1./numpy.sqrt(2)

        freq_ghz = _qa.convert(freq,"GHz")['value']
        dnu_hz = _qa.convert(dnu,"Hz")['value']
        tint_sec = _qa.convert(integration,"s")['value']

        if aca7m:
            telescope = "ACA"
            diam = 7.
        else: #12m
            telescope = "ALMA"
            diam = 12.

        myutil = simutil()
        eta_phase, espill, eta_block, eta_taper, ecorr, trx \
                   = myutil.noisetemp(telescope=telescope,freq=freq)
        eant = eta_phase * espill * eta_block * eta_taper

        tcmb = 2.725
        # ALMA latitude
        antpos = -23.022886 # deg
        el = numpy.pi/2.- (dir-antpos)/180.*numpy.pi     # rad
        airmass = 1./numpy.sin(el)

        hn_k = 0.04799274551*freq_ghz

        if mode.find("manual") > -1:
            tau = tau0*airmass
            Rtcmb = 1./(numpy.exp(hn_k/tcmb)-1.)
            Rtatmos = 1./(numpy.exp(hn_k/tsky)-1.)
            Rtground = 1./(numpy.exp(hn_k/tground)-1.)
            R = Rtcmb*espill + \
                numpy.exp(tau) *( espill * (1.-numpy.exp(-tau)) * Rtatmos \
                               + (1.-espill) * Rtground + trx/hn_k)
        else: # atm
            tsky, tau0 = self._get_atmsky(tground, tcmb, freq, dnu,
                                         espill, pwv, airmass)
            tau = tau0*airmass
            R = numpy.exp(tau) * (1./(numpy.exp(hn_k/tsky)-1.) + trx/hn_k)

        amp = 8 * 1.38062e-16 * 1e23 * 1e-4 / (eant * ecorr * numpy.pi)
        tsys=hn_k*R
        factor = numpy.sqrt(senscoeff * amp / numpy.sqrt(dnu_hz * tint_sec))
        par = diam / factor / numpy.sqrt(tsys)

        return 1./par**2

    def _get_atmsky(self, tground, tcmb, freq, dnu, espill, pwv, airmass):
        freq_ghz = _qa.convert(freq,"GHz")['value']
        hn_k = 0.04799274551*freq_ghz

        _at.initAtmProfile(temperature=_qa.quantity(tground))
        atmnchan = 10.
        fcntr = _qa.quantity(freq)
        bw = _qa.quantity(dnu)
        fres = _qa.div(_qa.quantity(dnu),atmnchan)
        _at.initSpectralWindow(nbands=1,fCenter=fcntr,fWidth=bw,fRes=fres)
        _at.setSkyBackgroundTemperature(_qa.quantity(tcmb,"K"))
        _at.setAirMass(airmass)
        _at.setUserWH2O(_qa.quantity(pwv,"mm"))
        rchan = _at.getRefChan(spwid=0)
        ratio = _at.getUserWH2O()["value"]/_at.getGroundWH2O()["value"]

        #tsky = _at.getTebbSky(nc=-1,spwid=0)
        dz = _at.getProfile()[1]["value"]
        tz = _at.getProfile()[2]["value"]
        radiance = 0.
        kv = 0.
        for iz in range(_at.getNumLayers()):
            dtau0 = dz[iz] * (_at.getAbsTotalDry(iz, rchan, 0)["value"] + \
                              _at.getAbsTotalWet(iz, rchan, 0)["value"] * ratio)
            dmass = dtau0[0]*airmass
            radiance += (1./(numpy.exp(hn_k/tz[iz])-1.)) * numpy.exp(-kv*airmass) \
                        * (1. - numpy.exp(-dmass))
            kv += dtau0[0]

        R = espill * (radiance + (1./(numpy.exp(hn_k/tcmb)-1.))*numpy.exp(-kv*airmass))\
            + (1./(numpy.exp(hn_k/_qa.quantity(tground)["value"]) - 1.)) * (1. - espill)
        tsky = hn_k / numpy.log(1. + (1. / R))
        tau0 = _at.getDryOpacity(spwid=0) + \
               _at.getWetOpacity(spwid=0)['value'][0]
        return tsky, tau0


########################################################################
#
# Tests on bad input parameter settings
#
class simobserve_badinputs(simobserve_unittest_base):
    """
    Tests on bad input parameter setting
    """
    # global variables of the class
    inimage = "core5ps.skymodel"
    incomp = "core5ps.clist"
    indata = [inimage,incomp]
    # Limit pointings to make elapse time shorter
    tottime = "1" #number of visit
    mapsize = ["5arcsec","5arcsec"] # single pointing
    sdmapsize = ["40arcsec","40arcsec"]
    sdantlist = "aca.tp.cfg"

    # bad parameter values
    badsize = "-60arcsec"
    badfreq = "-3GHz"
    baddir = "J3000 19h00m00 -23d00m00"
    badname = "badname"
    badtime = "-100s"
    badquant = "5bad"
    badnum = -1.
    project = simobserve_unittest_base.thistask+"_bad"

    failmsg = "The task must throw exception"
    errmsg = "Unexpected exception was thrown: %s"

    # NOTE: antennalist argument added to make this work in casataks where
    # the constraints in the xml arguments are not applied (that happens
    # in casalith). The value used is what that constraint value is for
    # these cases (obsmode="int", the default obsmode value)

    # Reserved methods of unit tests
    def setUp(self):
        if (os.path.exists(self.project)):
            shutil.rmtree(self.project)

        for data in self.indata:
            if os.path.exists(data):
                os.system("rm -rf %s" % data)
            os.system("cp -RH %s %s" % (os.path.join(self.datapath,data), data))

        # Add new line for better reading (these tests always print errors).
        print("")

    def tearDown(self):

        if self.teardown:
            if (os.path.exists(self.project)):
                shutil.rmtree(self.project)
            if os.path.exists(self.incomp):
                shutil.rmtree(self.incomp)
            if os.path.exists(self.project+".badptg.txt"):
                self._remove(self.project+".badptg.txt")

            for data in self.indata:
                if os.path.exists(data):
                    os.system("rm -rf %s" % data)


    # Tests on invalid parameter sets
    def test_default(self):
        """Test Default parameter set. Neigher skymodel nor complist"""
        try:
            simobserve(antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("At least one of skymodel or complist must be set.")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)


    def test_noProject(self):
        """Test no project name"""
        project = ''
        try:
            simobserve(project=project,antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("No such file or directory: ''")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)


    def testBad_skymodel(self):
        """Test bad skymodel name"""
        skymodel=self.badname
        try:
            simobserve(project=self.project,skymodel=skymodel,antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("No sky input found")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def test_notImage(self):
        """Test non-image skymodel"""
        skymodel=self.incomp
        try:
            simobserve(project=self.project,antennalist="alma.out10.cfg",
                       totaltime=self.tottime,mapsize=self.mapsize,
                       skymodel=skymodel)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Unable to open image %s." % skymodel)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_inbright(self):
        """Test bad inbright"""
        inbright=self.badquant
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       inbright=inbright,antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("could not convert string to float: '%s'" % inbright)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_indirection(self):
        """Test bad indirection ('J3000' is defaulted to 'J2000')"""
        indirection=self.baddir
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       indirection=indirection,graphics=self.graphics,
                       antennalist="alma.out10.cfg")
        except Exception:
            self.fail()
        # Need to compare MS with one generated with J2000

    def testBad_incell(self):
        """Test bad incell"""
        incell=self.badquant
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       incell=incell,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find('Error in QuantumHolder::fromString with input string "%s": Illegal input units or format' % incell)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_incenter(self):
        """Test bad incenter"""
        # Negaitve and non-frequency quantity are ignored
        incenter=self.badfreq

        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       incenter=incenter,graphics=self.graphics,
                       antennalist="alma.out10.cfg")
        except Exception:
            self.fail()
        # Need to compare MS with one generated with J2000

    def testBad_inwidth(self):
        """Test bad inwidth"""
        # Negaitve and non-frequency quantity are ignored
        inwidth=self.badfreq

        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       inwidth=inwidth,graphics=self.graphics,
                       antennalist="alma.out10.cfg")
        except Exception:
            self.fail()
        # Need to compare MS with one generated with J2000

    def testBad_complist(self):
        """Test bad complist name"""
        complist=self.badname
        try:
            simobserve(project=self.project,complist=complist,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("No sky input found")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def test_notComp(self):
        """Test non-components list complist"""
        complist=self.inimage
        try:
            simobserve(project=self.project,complist=complist,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("%s is non existant or is not a componentlist table" % complist)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_compwidth(self):
        """Test bad compwidth"""
        # not frequency
        compwidth="2arcsec"
        comp_nchan=1
        try:
            simobserve(project=self.project,complist=self.incomp,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Quantum::operator- unequal units 'GHz, 'arcsec'")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_comp_nchan(self):
        """Test bad comp_nchan"""
        compwidth="2arcsec"
        comp_nchan=self.badnum
        try:
            simobserve(project=self.project,complist=self.incomp,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       compwidth=compwidth,comp_nchan=comp_nchan,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("must be of cInt type")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_ptgfile(self):
        """Test bad ptgfile name"""
        setpointings=False
        ptgfile = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       setpointings=setpointings,ptgfile=ptgfile,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Can't find pointing file")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def test_notPtgfile(self):
        """Test nonconforming ptgfile"""
        # Generate bad file
        fname = self.project+".badptg.txt"
        f = open(fname,"w")
        f.write("#This is bad pointing file\nsome bad data written")
        f.close()
        del f

        setpointings=False
        ptgfile = fname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       setpointings=setpointings,ptgfile=ptgfile,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("No valid lines found in pointing file")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_integration(self):
        """Test bad integration"""
        integration = self.badtime
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       integration=integration,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find('Failed AlwaysAssert qIntTime.getValue("s")>=0')
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_direction(self):
        """Test bad direction ('J3000' is defaulted to 'J2000')"""
        direction = self.baddir

        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       direction=direction,graphics=self.graphics,
                       antennalist="alma.out10.cfg")
        except Exception:
            self.fail()
        # Need to compare MS with one generated with J2000

    def testBad_mapsize(self):
        """Test bad mapsize"""
        setpointings=True
        mapsize = [self.badquant, self.badquant]
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,
                       setpointings=setpointings,mapsize=mapsize,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("can't interpret '%s' as a CASA quantity" % self.badquant)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed maptype values are not checked in casatasks.")
    def testBad_maptype(self):
        """Test bad maptype"""
        maptype = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       maptype=maptype)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)


    def testBad_spacing(self):
        """Test bad pointingspacing"""
        pointingspacing = self.badquant
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       pointingspacing=pointingspacing,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("can't interpret '%s' as a CASA quantity" % pointingspacing)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)


    @unittest.skip("Allowed obsmode types are not checked in casatasks")
    def testBad_obsmode(self):
        """Test bad obsmode"""
        obsmode = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       obsmode=obsmode)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_antennalist(self):
        """Test bad antennalist name"""
        antennalist = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       antennalist=antennalist)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Couldn't find antennalist")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_caldirection(self):
        """Test bad caldirection ('J3000' is defaulted to 'J2000')"""
        caldirection = self.baddir

        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       caldirection=caldirection,graphics=self.graphics,
                       antennalist="alma.out10.cfg")
        except Exception:
            self.fail()
        # Need to compare MS with one generated with J2000


    def testBad_calflux(self):
        """Test bad calflux"""
        caldirection = "J2000 19h00m00 -23d00m50"
        calflux = self.badquant
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       caldirection=caldirection,calflux=calflux,
                       antennalist="alma.out10.cfg")
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("can't interpret '%s' as a CASA quantity" % calflux)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_sdantlist(self):
        """Test bad sdantlist name"""
        obsmode = "sd"
        mapsize = self.sdmapsize
        sdantlist = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=mapsize,
                       obsmode=obsmode,sdantlist=sdantlist)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Couldn't find antennalist")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    # simobserve automatically defaults a bad ID number to 0.
    # therefore testing non-numeric 'sdant' here
    def testBad_sdant(self):
        """Test bad sdant (non-numeric sdant)"""
        obsmode = "sd"
        mapsize = self.sdmapsize
        sdantlist = self.sdantlist
        sdant = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=mapsize,
                       obsmode=obsmode,sdantlist=sdantlist,
                       sdant=sdant)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("must be of cInt type")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_refdate(self):
        """Test bad refdate"""
        obsmode = "sd"
        mapsize = self.sdmapsize
        sdantlist = self.sdantlist
        refdate = "05/21"
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=mapsize,
                       obsmode=obsmode,sdantlist=sdantlist,
                       refdate=refdate)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Invalid reference date")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    def testBad_hourangle(self):
        """Test bad hourangle"""
        obsmode = "sd"
        mapsize = self.sdmapsize
        sdantlist = self.sdantlist
        hourangle = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=mapsize,
                       obsmode=obsmode,sdantlist=sdantlist,
                       hourangle=hourangle)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Cannot interpret your hourangle parameter %s as a time quantity" % hourangle)
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    # casapy crashes for totaltime < 0
    def testBad_totaltime(self):
        """Test bad totaltime"""
        obsmode = "sd"
        mapsize = self.sdmapsize
        sdantlist = self.sdantlist
        totaltime = self.badtime
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       mapsize=mapsize,
                       obsmode=obsmode,sdantlist=sdantlist,
                       totaltime=totaltime)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Negative totaltime is not allowed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed noisetype values are not checked in casatasks.")
    def testBad_noisetype(self):
        """Test bad thermalnoise type"""
        thermalnoise = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       thermalnoise=thermalnoise)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed user_pwv values are not checked in casatasks")
    def testBad_pwv(self):
        """Test bad user_pwv"""
        thermalnoise = 'tsys-atm'
        user_pwv = self.badnum
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       thermalnoise=thermalnoise,user_pwv=user_pwv)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed t_ground values are not checked in casatasks")
    def testBad_Tground(self):
        """Test bad t_ground"""
        thermalnoise = 'tsys-atm'
        t_ground = self.badnum
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       thermalnoise=thermalnoise,t_ground=t_ground)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed t_sky values are not checked in casatasks")
    def testBad_Tsky(self):
        """Test bad t_sky"""
        thermalnoise = 'tsys-manual'
        t_sky = self.badnum
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       thermalnoise=thermalnoise,t_sky=t_sky)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed tau0 values are not checked in casatasks")
    def testBad_tau0(self):
        """Test bad tau0"""
        thermalnoise = 'tsys-manual'
        tau0 = self.badnum
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       thermalnoise=thermalnoise,tau0=tau0)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed leakage values are not checked in casatasks")
    def testBad_leakage(self):
        """Test bad leakage"""
        leakage = self.badnum
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       leakage=leakage)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

    @unittest.skip("Allowed graphics values are not checked in casatasks")
    def testBad_graphics(self):
        """Test bad graphics selection"""
        graphics = self.badname
        try:
            simobserve(project=self.project,skymodel=self.inimage,
                       totaltime=self.tottime,mapsize=self.mapsize,
                       graphics=graphics)
            self.fail(self.failmsg)
        except Exception as e:
            pos=str(e).find("Parameter verification failed")
            msg =  self.errmsg % str(e)
            self.assertNotEqual(pos,-1,msg=msg)

class simobserve_correlator_ALMA(simobserve_unittest_base):
    """
    Test correlator with ALMA
    """
    project = simobserve_unittest_base.thistask+"_sky"
    inmodel = "core5ps.skymodel4mod"
    sdantlist = "aca.tp.cfg"
    antlist = "alma.out01.cfg"

    refproj = "ref_sky"
    refpref = os.path.join(simobserve_unittest_base.datapath,refproj) + "/"

    # Reserved methods
    def setUp(self):
        print("")
        if os.path.exists(self.project):
            shutil.rmtree(self.project)

        self.refpref_sd = self.refpref + \
                          self._get_data_prefix(self.sdantlist,self.refproj)
        self.refpref_int = self.refpref + \
                           self._get_data_prefix(self.antlist,self.refproj)
        self.refmodel = self.refpref_sd+".skymodel" # reference modified skymodel
        # reference simulated MS
        self.refms_sd = self.refpref_sd+".sd.ms"
        self.refms_int = self.refpref_int+".ms"

    def tearDown(self):
        if self.teardown and os.path.exists(self.project):
            shutil.rmtree(self.project)
            for stray_file in [self.inmodel, self.sdantlist, self.antlist]:
                try:
                    self._remove(stray_file)
                except(FileNotFoundError, NotADirectoryError):
                    pass

    def test_correlator_ACA_With_ALMA(self):
        """Test: correlator = ACA is not allowed for telescope = ALMA."""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        #setpointings = False
        #ptgfile =   # necessary even if only modifymodel
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""

        with self.assertRaises(RuntimeError):
            simobserve(project=self.project,skymodel=skymodel,
               inbright=inbright,indirection=indirection,
               incell=incell,incenter=incenter,inwidth=inwidth,
               setpointings=True,obsmode=obsmode,
               antennalist=antennalist,sdantlist=sdantlist,
               thermalnoise="",graphics=self.graphics, correlator='ACA')

    def test_correlator_ACASpec_With_ALMA(self):
        """Test: correlator = ACASpec is not allowed for telescope = ALMA."""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        #setpointings = False
        #ptgfile =   # necessary even if only modifymodel
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""

        with self.assertRaises(RuntimeError):
            simobserve(project=self.project,skymodel=skymodel,
               inbright=inbright,indirection=indirection,
               incell=incell,incenter=incenter,inwidth=inwidth,
               setpointings=True,obsmode=obsmode,
               antennalist=antennalist,sdantlist=sdantlist,
               thermalnoise="",graphics=self.graphics, correlator='ACASpec')


    def test_correlator_BLC_With_ALMA(self):
        """Test: correlator = BLC for telescope = ALMA."""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        #setpointings = False
        #ptgfile =   # necessary even if only modifymodel
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""


        simobserve(project=self.project,skymodel=skymodel,
               inbright=inbright,indirection=indirection,
               incell=incell,incenter=incenter,inwidth=inwidth,
               setpointings=True,obsmode=obsmode,
               antennalist=antennalist,sdantlist=sdantlist,
               thermalnoise="",graphics=self.graphics, correlator='BLC')

    def test_correlator_BLC_4x4bit_With_ALMA(self):
        """Test: correlator = BLC_4x4bitfor telescope = ALMA."""
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        #setpointings = False
        #ptgfile =   # necessary even if only modifymodel
        obsmode = ""
        antennalist="alma.out01.cfg" # necessary even if only modifymodel
        sdantlist = ""


        simobserve(project=self.project,skymodel=skymodel,
               inbright=inbright,indirection=indirection,
               incell=incell,incenter=incenter,inwidth=inwidth,
               setpointings=True,obsmode=obsmode,
               antennalist=antennalist,sdantlist=sdantlist,
               thermalnoise="",graphics=self.graphics, correlator='BLC_4x4bit')


class simobserve_correlator_ALMASD(simobserve_unittest_base):
    """
    Test correlator with ALMASD
    """
    project = simobserve_unittest_base.thistask+"_sky"
    inmodel = "core5ps.skymodel4mod"
    sdantlist = "aca.tp.cfg"
    antlist = "alma.out01.cfg"

    refproj = "ref_sky"
    refpref = os.path.join(simobserve_unittest_base.datapath,refproj) + "/"

    # Reserved methods
    def setUp(self):
        print("")
        if os.path.exists(self.project):
            shutil.rmtree(self.project)

        self.refpref_sd = self.refpref + \
                          self._get_data_prefix(self.sdantlist,self.refproj)
        self.refpref_int = self.refpref + \
                           self._get_data_prefix(self.antlist,self.refproj)
        self.refmodel = self.refpref_sd+".skymodel" # reference modified skymodel
        # reference simulated MS
        self.refms_sd = self.refpref_sd+".sd.ms"
        self.refms_int = self.refpref_int+".ms"

    def tearDown(self):
        if self.teardown and os.path.exists(self.project):
            shutil.rmtree(self.project)
            for stray_file in [self.inmodel, self.sdantlist, self.antlist]:
                try:
                    self._remove(stray_file)
                except(FileNotFoundError, NotADirectoryError):
                    pass

    def test_correlator_ACA_With_ALMASD(self):
        """Test: correlator = ACA obsmode = sd """
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        integration = "4s"
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "576s"


        simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics, correlator='ACA')

    def test_correlator_ACASpec_With_ALMASD(self):
        """Test: correlator = ACASpec obsmode = sd """
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        integration = "4s"
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "576s"


        simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics, correlator='ACASpec')

    def test_correlator_BLC_With_ALMASD(self):
        """Test: correlator = BLC is not allowed for obsmode = sd """
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        integration = "4s"
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "576s"

        with self.assertRaises(RuntimeError):
            simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics, correlator='BLC')

    def test_correlator_BLC_4x4bit_With_ALMASD(self):
        """Test: correlator = BLC_4x4bit is not allowed for obsmode = sd """
        skymodel = self.inmodel
        self._copy_input(skymodel)
        inbright = "5.95565834e-05Jy/pixel"
        indirection = "J2000 19h00m00 -23d00m00"
        incell = "0.043080964943481216arcsec"
        incenter = "345GHz"
        inwidth = "10MHz"
        integration = "4s"
        mapsize = ["60arcsec", "60arcsec"]
        maptype = "square"
        obsmode = "sd"
        sdantlist = "aca.tp.cfg"
        totaltime = "576s"

        with self.assertRaises(RuntimeError):
            simobserve(project=self.project,skymodel=skymodel,
                       inbright=inbright,indirection=indirection,
                       incell=incell,incenter=incenter,inwidth=inwidth,
                       setpointings=True,integration=integration,
                       mapsize=mapsize,maptype=maptype,obsmode=obsmode,
                       totaltime=totaltime,antennalist="",sdantlist=sdantlist,
                       thermalnoise="",graphics=self.graphics, correlator='BLC_4x4bit')

########### New Class 3
class simobserve_correlator_ACA(simobserve_unittest_base):
    """
    Test correlator with ACA
    """
    project = simobserve_unittest_base.thistask+"_sky"
    inmodel = "core5ps.skymodel4mod"
    sdantlist = "aca.tp.cfg"
    antlist = "alma.out01.cfg"

    refproj = "ref_sky"
    refpref = os.path.join(simobserve_unittest_base.datapath,refproj) + "/"

    # Reserved methods
    def setUp(self):
        print("")
        if os.path.exists(self.project):
            shutil.rmtree(self.project)

        self.refpref_sd = self.refpref + \
                          self._get_data_prefix(self.sdantlist,self.refproj)
        self.refpref_int = self.refpref + \
                           self._get_data_prefix(self.antlist,self.refproj)
        self.refmodel = self.refpref_sd+".skymodel" # reference modified skymodel
        # reference simulated MS
        self.refms_sd = self.refpref_sd+".sd.ms"
        self.refms_int = self.refpref_int+".ms"

    def tearDown(self):
        if self.teardown and os.path.exists(self.project):
            shutil.rmtree(self.project)
            for stray_file in [self.inmodel, self.sdantlist, self.antlist]:
                try:
                    self._remove(stray_file)
                except(FileNotFoundError, NotADirectoryError):
                    pass

    def test_correlator_ACA_With_ACA(self):
        """Test: correlator = ACA for telescope = ACA."""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = "aca.cycle7.named.cfg"
        totaltime = "28s"

        simobserve(project=self.project,skymodel=skymodel,
           setpointings=setpointings,ptgfile=ptgfile,
           integration=integration,obsmode=obsmode,
           antennalist=antennalist,totaltime=totaltime,
           thermalnoise="",graphics=self.graphics,
           refdate="2014/05/21",t_ground=269.,correlator='ACA')

    def test_correlator_ACASpec_With_ACA(self):
        """Test: correlator = ACASpec is not allowed for telescope = ACA."""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = "aca.cycle7.named.cfg"
        totaltime = "28s"
        with self.assertRaises(RuntimeError):
            simobserve(project=self.project,skymodel=skymodel,
                       setpointings=setpointings,ptgfile=ptgfile,
                       integration=integration,obsmode=obsmode,
                       antennalist=antennalist,totaltime=totaltime,
                       thermalnoise="",graphics=self.graphics,
                       refdate="2014/05/21",t_ground=269.,correlator='ACASpec')

    def test_correlator_BLC_With_ACA(self):
        """Test: correlator = BLC for telescope = ACA."""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = "aca.cycle7.named.cfg"
        totaltime = "28s"

        simobserve(project=self.project,skymodel=skymodel,
           setpointings=setpointings,ptgfile=ptgfile,
           integration=integration,obsmode=obsmode,
           antennalist=antennalist,totaltime=totaltime,
           thermalnoise="",graphics=self.graphics,
           refdate="2014/05/21",t_ground=269.,correlator='BLC')

    def test_correlator_BLC_4x4bit_With_ACA(self):
        """Test: correlator = BLC_4x4bit for telescope = ACA."""
        skymodel = self.refmodel
        setpointings = False
        ptgfile = self.refpref_int + ".ptg.txt"
        integration = "4s"
        obsmode = "int"
        antennalist = "aca.cycle7.named.cfg"
        totaltime = "28s"

        simobserve(project=self.project,skymodel=skymodel,
           setpointings=setpointings,ptgfile=ptgfile,
           integration=integration,obsmode=obsmode,
           antennalist=antennalist,totaltime=totaltime,
           thermalnoise="",graphics=self.graphics,
           refdate="2014/05/21",t_ground=269.,correlator='BLC_4x4bit')

if __name__ == '__main__':
    unittest.main()
