##########################################################################
# test_task_wvrgcal.py
#
# Copyright (C) 2018, 2024
# Associated Universities, Inc. Washington DC, USA.
# European Sourthern Observatory, Garching, Germany
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
#
##########################################################################
import os
import sys
import shutil
import numpy as np
import matplotlib.pyplot as plt
import math

from casatools import ctsys, table
from casatasks import flagdata, smoothcal, split
from casatasks import wvrgcal

import unittest

from casatestutils import testhelper as th

tb = table()

def comp_gainphase(oldctab, newctab, myant=0, spw=None, tol_deg=0.2, figfile='', symsize=12, x_is_time=True):
    """
    Test the phaseangle dfference of the two gaintables oldctab and newctab for antenna myant
    together with their difference and write plot to "figfile".png .

    If myant==-1, check all antennas.

    tol_deg is the max permitted difference in phase angle (degrees) for return value True.

    If figfile is not an empty string, dignostic plots of phase angle difference between
    the result and the reference are produced with the filenames starting with the value of figfile.

    symsize specifies the plot symbol size in pixels. The minimum size is limited to 5 pixels.

    x_is_time specifies whether the plot x-axis is in units of time or table rows.

    """

    rval = True

    mytb = table()

    mytb.open(oldctab)
    old = mytb.getcol('CPARAM')
    ants = mytb.getcol('ANTENNA1')
    fields = mytb.getcol('FIELD_ID')
    spws = mytb.getcol('SPECTRAL_WINDOW_ID')
    flags = mytb.getcol('FLAG')[0][0]
    times = mytb.getcol('TIME')
    times -= times[0]
    mytb.close()
    mytb.open(newctab)
    new = mytb.getcol('CPARAM')
    mytb.close()

    olda = np.angle(old[0][0],deg=True)
    newa = np.angle(new[0][0],deg=True)

    nant = np.max(ants)+1

    if myant<-1 or myant>nant:
        print('myant out of range. Numants is '+str(nant))
        return False

    the_ants = [myant]
    if myant==-1:
        the_ants = list(range(nant))


    if spw==None:
        the_spw = [spws[0]]
    elif type(spw) == int:
        the_spw = [spw]
    elif type(spw) == list:
        the_spw = spw

    for the_ant in the_ants:

        x = []
        y1 = []
        y2 = []
        y3 = []
        y4 = []
        prev_field = -1
        fieldavs = {}
        fieldns = {}
        field_occurrence = {}
        fo_rows = {}

        j = 0
        for i in range(the_ant, len(olda), nant):
            if flags[i]==0 and spws[i] in the_spw:
                if x_is_time:
                    x.append(times[i])
                else:
                    x.append(j)

                y1.append(olda[i])
                y2.append(newa[i])
                if olda[i]==math.nan:
                    if newa[i]==math.nan:
                        myy3 = 0.
                    else:
                        myy3 = math.nan
                elif newa[i]==math.nan:
                    myy3 = math.nan
                else:
                    myy3 = newa[i]-olda[i]
                    if myy3 < -180:
                        myy3 += 360.
                    elif myy3 > 180.:
                        myy3 -= 360.

                y3.append(myy3)

                if fields[i] not in fieldavs.keys():
                    print('Found field ', fields[i])
                    fieldavs[fields[i]] = 0.
                    fieldns[fields[i]] = 0.
                    field_occurrence[fields[i]] = 0
                    fo_rows[fields[i]] = []
                elif fields[i] != prev_field:
                    #print('Found field again ', fields[i])
                    # initialise new occurence
                    field_occurrence[fields[i]] += 1
                    fieldavs[fields[i]+field_occurrence[fields[i]]*1000] = 0.
                    fieldns[fields[i]+field_occurrence[fields[i]]*1000] = 0.
                    fo_rows[fields[i]+field_occurrence[fields[i]]*1000] = []

                if myy3 != math.nan:
                    fieldavs[fields[i]+field_occurrence[fields[i]]*1000] += myy3
                fieldns[fields[i]+field_occurrence[fields[i]]*1000] += 1.
                fo_rows[fields[i]+field_occurrence[fields[i]]*1000].append(j)

                #print(fieldns, fields[i], myy3)

                prev_field=fields[i]
                j+=1


        #print(fieldavs)
        #print(fieldns)

        y4 = list(np.zeros(len(y3)))
        within_tol = np.ones(len(y3), dtype='bool')
        for myfield in fieldavs.keys():
            if fieldns[myfield] != 0:
                fieldavs[myfield] /= fieldns[myfield]
                for j in fo_rows[myfield]:
                    if y3[j] != math.nan:
                        myy4 = (y3[j] - fieldavs[myfield])
                        if myy4 < -180:
                            myy4 += 360.
                        elif myy4 > 180.:
                            myy4 -= 360.
                    else:
                        myy4 = 999.

                    y4[j] = myy4
                    if abs( myy4 ) > tol_deg or myy4==math.nan:
                        within_tol[j] = False


        if figfile !='':
            fig, ax = plt.subplots()
            ax.set_ylim(-181.,181.)
            if symsize<5:
                symsize=5
            ax.scatter(np.array(x), np.array(y1), c='tab:blue', s=symsize, label='ant '+str(the_ant)+' old',
                       alpha=1.0, edgecolors='none')
            ax.scatter(np.array(x), np.array(y2), c='tab:orange', s=symsize-1, label='ant '+str(the_ant)+' new',
                       alpha=1.0, edgecolors='none')
            ax.scatter(np.array(x), np.array(y3), c='tab:green', s=symsize-2, label='new minus old',
                       alpha=1.0, edgecolors='none')
            ax.scatter(np.array(x), np.array(y4), c='tab:red', s=symsize-3, label='(new - old) - avDiffPerField',
                       alpha=1.0, edgecolors='none')

            if x_is_time:
                ax.set_xlabel('time since obs. start (sec)')
            else:
                ax.set_xlabel('row')

            ax.set_ylabel('degrees')

            ax.legend()
            ax.grid(True)

            #plt.show()

            the_file = figfile+'_ant'+str(the_ant)+'.png'
            os.system('rm -rf '+the_file)
            plt.savefig(the_file)

            plt.close()

        rval = rval and (False not in within_tol)
        print('FIELD averages ant '+str(the_ant)+': ', fieldavs)
        if len(y4) >0:
            print('Ant '+str(the_ant)+' Max diff (deg)', np.max(y4))
            print('Ant '+str(the_ant)+' Min diff (deg)', np.min(y4))
        else:
            print('Ant '+str(the_ant)+': no diff values.')

    # end for the_ant

    return rval


def plot_gainphase_diff(oldctab, newctab, myant=0, figfile='lastplot', symsize=10., spw=None):
    """
    Plot the phaseangle of the two gaintables oldctab and newctab for antenna myant
    together with their difference and write plot to "figfile".png .

    If myant==-1, produce plots for all antennas.

    Returns the number of plot produced.

    """

    mytb = tbtool()

    mytb.open(oldctab)
    old = mytb.getcol('CPARAM')
    ants = mytb.getcol('ANTENNA1')
    spws = mytb.getcol('SPECTRAL_WINDOW_ID')
    mytb.close()
    mytb.open(newctab)
    new = mytb.getcol('CPARAM')
    mytb.close()

    olda = np.angle(old[0][0],deg=True)
    newa = np.angle(new[0][0],deg=True)

    nant = np.max(ants)+1

    if myant<-1 or myant>nant:
        print('myant out of range. Numants is '+str(nant))
        return 0

    the_ants = [myant]
    if myant==-1:
        the_ants = list(range(nant))


    if spw==None:
        the_spw = [spws[0]]
    elif type(spw) == int:
        the_spw = [spw]
    elif type(spw) == list:
        the_spw = spw

    for the_ant in the_ants:

        x = []
        y1 = []
        y2 = []
        y3 = []
        j = 0
        for i in range(the_ant, len(olda), nant):
            if spws[i] in the_spw:
                x.append(j)
                j+=1
                y1.append(olda[i])
                y2.append(newa[i])
                y3.append(newa[i]-olda[i])

        fig, ax = plt.subplots()
        ax.set_ylim(-181.,181.)
        ax.scatter(np.array(x), np.array(y1), c='tab:blue', s=symsize, label='ant '+str(the_ant)+' old',
                   alpha=0.8, edgecolors='none')
        ax.scatter(np.array(x), np.array(y2), c='tab:orange', s=symsize, label='ant '+str(the_ant)+' new',
                   alpha=0.8, edgecolors='none')
        ax.scatter(np.array(x), np.array(y3), c='tab:green', s=symsize, label='new minus old',
                   alpha=0.8, edgecolors='none')

        ax.set_xlabel('row')
        ax.set_ylabel('degrees')

        ax.legend()
        ax.grid(True)

        #plt.show()

        if figfile !='':
            the_file = figfile+'_ant'+str(the_ant)+'.png'
            os.system('rm -rf '+the_file)
            plt.savefig(the_file)

        plt.close()

    return len(the_ants)

def comp_refs(dirold, dirnew):
    refs = ['multisource_unittest_reference-mod.wvr',
            'wvrgcalctest_disperse_v2.W',
            'wvrgcalctest_scale.W',
            'wvrgcalctest_tie1_v2.W',
            'multisource_unittest_reference-newformat.wvr',
            'wvrgcalctest_nsol_v2.W',
            'wvrgcalctest_sourceflag2_v2.W',
            'wvrgcalctest_tie2_v2.W',
            'wvrgcalctest-test19_v2.W',
            'wvrgcalctest_reversespw.W',
            'wvrgcalctest_statsource.W',
            'wvrgcalctest_toffset.W']

    rval = True

    for myref in refs:
        numants = plot_gainphase_diff(dirold+'/'+myref, dirnew+'/'+myref, myant=-1, figfile=myref)
        print(myref+': found '+str(numants)+' ants.')
        if numants <1:
            rval = False

    print('Done.')
    return rval


class wvrgcal_test(unittest.TestCase):

    vis_f = 'multisource_unittest.ms'
    vis_g = 'wvrgcal4quasar_10s.ms'
    vis_h = 'uid___A002_X8ca70c_X5_shortened.ms'
    inplist = [vis_f, vis_g, vis_h]
    ref = ['multisource_unittest_reference.wvr', # ref0
           'multisource_unittest_reference-newformat.wvr', # ref1: test2
           'wvrgcalctest.W', # ref2
           'wvrgcalctest_toffset.W', # ref3: test3
           'wvrgcalctest_segsource.W', # ref4
           'wvrgcalctest_wvrflag1.W', # ref5
           'wvrgcalctest_wvrflag2.W', # ref6
           'wvrgcalctest_reverse.W',  # ref7
           'wvrgcalctest_reversespw.W', # ref8: test4
           'wvrgcalctest_smooth.W', # ref9
           'wvrgcalctest_scale.W', # ref10: test6
           'wvrgcalctest_tie1_v2.W', # ref11: test7
           'wvrgcalctest_tie2_v2.W', # ref12: test8
           'wvrgcalctest_sourceflag1.W', # ref13
           'wvrgcalctest_sourceflag2_v2.W', # ref14: test9
           'wvrgcalctest_statsource.W', # ref15: test10
           'wvrgcalctest_nsol_v2.W', # ref16: test11
           'wvrgcalctest_disperse_v2.W', # ref17: test12
           'multisource_unittest_reference-mod.wvr', # ref18: test16
           'wvrgcalctest-test19_v2.W'] # ref19: test19

## 2   'wvrgcalctest.W': '',
## 3   'wvrgcalctest_toffset.W': '--toffset -1', ........................ test3
## 4   'wvrgcalctest_segsource.W': '--segsource',
## 5   'wvrgcalctest_wvrflag1.W': '--wvrflag DV03',
## 6   'wvrgcalctest_wvrflag2.W': '--wvrflag DV03 --wvrflag PM02',
## 7   'wvrgcalctest_reverse.W': '--reverse',
## 8   'wvrgcalctest_reversespw.W': '--reversespw 1', ................... test4
## 9   'wvrgcalctest_smooth.W':'smooth 3 seconds',........................ test5
## 10   'wvrgcalctest_scale.W':'--scale 0.8', ............................ test6
## 11   'wvrgcalctest_tie1.W':'--segsource --tie 0,1,2', ................. test7
## 12   'wvrgcalctest_tie2.W':'--segsource --tie 0,3 --tie 1,2', ......... test8
## 13   'wvrgcalctest_sourceflag1.W':'--sourceflag 0455-462 --segsource',
## 14   'wvrgcalctest_sourceflag2.W':'--sourceflag 0455-462 --sourceflag 0132-169 --segsource', ...test9
## 15   'wvrgcalctest_statsource.W':'--statsource 0455-462', ..............test10
## 16   'wvrgcalctest_nsol.W':'--nsol 5' ..................................test11
## 17   'wvrgcalctest_disperse.W':'--disperse', .......................... test12

    makeref = False # set this to true to generate new reference tables

    makeplots = False # set this to true to generate caltable comparison plots where applicable

    out = 'mycaltable.wvr'
    comptabtol = 0.001 # default is 0.001, i.e. 0.1%
    compangtol = 0.25 # max. permitted phase correction difference (degrees)
    rval = False

    def setUp(self):
        self.rval = False

        datapath = ctsys.resolve('unittest/wvrgcal/')
        refpath = ctsys.resolve('unittest/wvrgcal/wvrgcal_reference/')
        if(not os.path.exists(self.vis_f)):
            shutil.copytree(os.path.join(datapath,self.vis_f), self.vis_f)
        if(not os.path.exists(self.vis_g)):
            shutil.copytree(os.path.join(datapath,self.vis_g), self.vis_g)
        if(not os.path.exists(self.vis_h)):
            shutil.copytree(os.path.join(datapath,self.vis_h), self.vis_h)
        for i in range(0,len(self.ref)):
            if(not os.path.exists(self.ref[i])):
                shutil.copytree(os.path.join(refpath,self.ref[i]), self.ref[i])

        if self.makeref:
            print("Will create copies of generated caltables in directory \"newref\"")
            os.system('mkdir -p newref')

        if self.makeplots:
            print("Will create diagnostic plots of caltable comparisons where applicable.")

    def tearDown(self):
        os.system('rm -rf myinput.ms*')
        os.system('rm -rf ' + self.out +'*')
        for i in range(0,len(self.ref)):
            os.system('rm -rf ' + self.ref[i])

        for ii in self.inplist:
            if os.path.exists(ii):
                shutil.rmtree(ii)

        if os.path.exists('comp.W'): shutil.rmtree('comp.W')
        if os.path.exists('comp2.W'): shutil.rmtree('comp2.W')

# Test cases
    def test1(self):
        '''Test 1: Testing default'''
        passes = False
        try:
            self.rval = wvrgcal()
        except AssertionError:
            passes = True
            print("Expected error ...")
        self.assertTrue(passes)

    def test2(self):
        '''Test 2: Testing with a multi-source dataset'''
        myvis = self.vis_f
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, wvrflag=['0', '1'], toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[1])
            os.system('cp -R '+self.out+' newref/'+self.ref[1])

        print('test2')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(referencetab=self.ref[1], testtab=self.out, excludecols=['WEIGHT','CPARAM'],
                                      # ignore WEIGHT because it is empty, CPARAM because it is tested separately
##                                             ['TIME',
##                                              'FIELD_ID',
##                                              'SPECTRAL_WINDOW_ID',
##                                              'ANTENNA1',
##                                              'ANTENNA2',
##                                              'INTERVAL',
##                                              'SCAN_NUMBER',
##                                              'CPARAM',
##                                              'PARAMERR',
##                                              'FLAG',
##                                              'SNR',
##                                              'WEIGHT']
                                      tolerance=self.comptabtol)
        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test2'
            self.rval = comp_gainphase(oldctab=self.ref[1], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test3(self):
        '''Test 3:  wvrgcal4quasar_10s.ms, segsource False'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, segsource=False, toffset=-1.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[3])
            os.system('cp -R '+self.out+' newref/'+self.ref[3])

        print('test3')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[3], self.out, ['WEIGHT', 'CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test3'
            self.rval = comp_gainphase(oldctab=self.ref[3], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)


        self.assertTrue(self.rval)

    def test4(self):
        '''Test 4:  wvrgcal4quasar_10s.ms, reversespw, segsource False'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, reversespw='1', segsource=False, toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[8])
            os.system('cp -R '+self.out+' newref/'+self.ref[8])

        print('test4')
        print(rvaldict)

        self.rval = rvaldict['success']


        if(self.rval):
            self.rval = th.compTables(self.ref[8], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test4'
            self.rval = comp_gainphase(oldctab=self.ref[8], newctab=self.out, myant=-1, spw=[0,1], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)


    def test5(self):
        '''Test 5:  wvrgcal4quasar_10s.ms, smooth, segsource False'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, smooth='3s', segsource=False, toffset=0.)

        print('test5')
        print(rvaldict)

        self.rval = rvaldict['success']

        self.assertTrue(os.path.exists(self.out))
        self.assertTrue(os.path.exists(self.out+'_unsmoothed'))
        smoothcal(vis = "myinput.ms",
		  tablein = self.out+'_unsmoothed',
		  caltable = self.out+'_ref',
		  smoothtype = 'mean',
		  smoothtime = 3.)
        if(self.rval):
            self.rval = th.compTables(self.out+'_ref', self.out, ['WEIGHT','CPARAM'],
                                      tolerance=0.01) # tolerance 1 % to accomodate differences between Linux and Mac OSX
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test5'
            self.rval = comp_gainphase(oldctab=self.out+'_ref', newctab=self.out, myant=-1, spw=[0,1], tol_deg=self.compangtol, figfile=figfile)


        self.assertTrue(self.rval)

    def test6(self):
        '''Test 6:  wvrgcal4quasar_10s.ms, scale, segsource=False'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, scale=0.8, segsource=False, toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[10])
            os.system('cp -R '+self.out+' newref/'+self.ref[10])

        print('test6')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[10], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test6'
            self.rval = comp_gainphase(oldctab=self.ref[10], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test7(self):
        '''Test 7:  wvrgcal4quasar_10s.ms, tie three sources'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, tie=['0,1,2'], toffset=0.)

        if self.makeref: # remove
            os.system('rm -rf newref/'+self.ref[11])
            os.system('cp -R '+self.out+' newref/'+self.ref[11])

        print('test7')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[11], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test7'
            self.rval = comp_gainphase(oldctab=self.ref[11], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test8(self):
        '''Test 8:  wvrgcal4quasar_10s.ms, tie two times two sources'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, tie=['0,3', '1,2'], toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[12])
            os.system('cp -R '+self.out+' newref/'+self.ref[12])

        print('test8')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[12], self.out, ['WEIGHT','CPARAM'], 0.01)
            # increase tolerance to 1 % to temporarily
            # overcome difference between 32bit and 64bit output;
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test8'
            self.rval = comp_gainphase(oldctab=self.ref[12], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test9(self):
        '''Test 9:  wvrgcal4quasar_10s.ms, sourceflag two sources'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, sourceflag=['0455-462','0132-169'], toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[14])
            os.system('cp -R '+self.out+' newref/'+self.ref[14])

        print('test9')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[14], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test9'
            self.rval = comp_gainphase(oldctab=self.ref[14], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test10(self):
        '''Test 10:  wvrgcal4quasar_10s.ms, statsource, segsource=False'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, segsource=False, statsource='0455-462', toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[15])
            os.system('cp -R '+self.out+' newref/'+self.ref[15])

        print('test10')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[15], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test10'
            self.rval = comp_gainphase(oldctab=self.ref[15], newctab=self.out, myant=-1, spw=[0,1], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test11(self):
        '''Test 11:  wvrgcal4quasar_10s.ms, nsol, segsource=False'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, segsource=False, nsol=5, toffset=0.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[16])
            os.system('cp -R '+self.out+' newref/'+self.ref[16])

        print('test11')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[16], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test11'
            self.rval = comp_gainphase(oldctab=self.ref[16], newctab=self.out, myant=-1, spw=[0],
                                       tol_deg=20*self.compangtol, # the nsol>1 setting leads to a larger impact of the rnd gen dependencies
                                       figfile=figfile)

        self.assertTrue(self.rval)

    def test12(self):
        '''Test 12:  wvrgcal4quasar_10s.ms, disperse'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, disperse=True, toffset=-1.)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[17])
            os.system('cp -R '+self.out+' newref/'+self.ref[17])

        print('test12')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[17], self.out, ['WEIGHT','CPARAM'], tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test12'
            self.rval = comp_gainphase(oldctab=self.ref[17], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test13(self):
        '''Test 13:  wvrgcal4quasar_10s.ms,  totally flagged main table'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')

        flagdata(vis="myinput.ms", spw='0', mode='manual')

        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, disperse=True, toffset=-1.)

        print('test13')
        print(rvaldict)

        self.rval = rvaldict['success']

        print("Expected error ...")

        self.assertFalse(self.rval)


    def test14(self):
        '''Test 14:  wvrgcal4quasar_10s.ms, first seconds flagged for one antenna, mingoodfrac=0.99'''
        myvis = self.vis_g
        os.system('rm -rf myinput2.ms comp.W comp2.W')
        os.system('cp -R ' + myvis + ' myinput.ms')

        flagdata(vis="myinput.ms", timerange='09:10:11~09:10:15', antenna='DV14&&*', mode='manual')
        split(vis='myinput.ms', outputvis='myinput2.ms', datacolumn='data', keepflags=False)

        rvaldict = wvrgcal(vis="myinput.ms", caltable='comp.W', toffset=0., mingoodfrac=0.99)
        rvaldict2 = wvrgcal(vis="myinput2.ms", caltable='comp2.W', toffset=0., mingoodfrac=0.99)

        print('test14-1')
        print(rvaldict)
        print('test14-2')
        print(rvaldict2)

        self.rval = rvaldict['success'] and rvaldict2['success']

        if 'Frac_unflagged' in rvaldict.keys():
            self.rval = self.rval and (rvaldict['Frac_unflagged'][14]>0.9)

        if(self.rval):
            # antenna DV14 has antenna id 14 (accidentally)
            print(rvaldict['Disc_um'][14], rvaldict2['Disc_um'][14])
            rvaldict2['Disc_um'][14]= rvaldict['Disc_um'][14] # The value for antenna 14 is the only one expected to be different
            print(rvaldict['RMS_um'][14], rvaldict2['RMS_um'][14])
            rvaldict2['RMS_um'][14]=rvaldict['RMS_um'][14]  # The value for antenna 14 is the only one expected to be different
            print(rvaldict['Flag'][14], rvaldict2['Flag'][14])
            rvaldict2['Flag'][14]=rvaldict['Flag'][14]  # The value for antenna 14 is the only one expected to be different
            if 'Frac_unflagged' in rvaldict.keys():
                print(rvaldict['Frac_unflagged'][14], rvaldict2['Frac_unflagged'][14])
                rvaldict2['Frac_unflagged'][14]=rvaldict['Frac_unflagged'][14] # The value for antenna 14 is the only one expected to be different

            self.rval = (rvaldict==rvaldict2)

        self.assertTrue(self.rval)


    def test15(self):
        '''Test 15:  wvrgcal4quasar_10s.ms, one antenna flagged'''
        myvis = self.vis_g
        os.system('rm -rf myinput.ms comp.W')
        os.system('cp -R ' + myvis + ' myinput.ms')

        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, wvrflag='DA41', toffset=-1.)

        flagdata(vis="myinput.ms", antenna='DA41&&*', mode='manual', spw='0;9~26') # flag the WVR SPWs for antenna 2 = DA41

        rvaldict2 = wvrgcal(vis="myinput.ms", caltable='comp.W', toffset=-1.)

        print('test15-1')
        print(rvaldict)
        print('test15-2')
        print(rvaldict2)

        self.rval = rvaldict['success'] and rvaldict2['success']

        if(self.rval):
            if 'Frac_unflagged' in rvaldict.keys():
                print(rvaldict['Frac_unflagged'][2], rvaldict2['Frac_unflagged'][2])
                rvaldict2['Frac_unflagged'][2]=rvaldict['Frac_unflagged'][2] # The value for antenna 2 is the only one expected to be different
            self.rval = (rvaldict==rvaldict2) # otherwise, it shouldn't matter if we use wvrflag or flag the WVR data with flagdata

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test15'
            self.rval = comp_gainphase(oldctab='comp.W', newctab=self.out, myant=-1, spw=[1,3,5,7], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

    def test16(self):
        '''Test 16: Test the maxdistm and minnumants parameters'''
        myvis = self.vis_f
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, wvrflag=['0', '1'], toffset=0., maxdistm=40., minnumants=2)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[18])
            os.system('cp -R '+self.out+' newref/'+self.ref[18])

        print('test16')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[18], self.out, ['WEIGHT', 'CPARAM'],
                                      tolerance=self.comptabtol)
        if(self.rval):
            tb.open(self.out)
            a = tb.getcol('ANTENNA1')
            c = tb.getcol('CPARAM')[0][0]
            tb.close()
            for i in range(len(a)):
                if (a[i]==1 and not (c[i]==(1+0j))):
                    self.rval=False
                    print("CPARAM for antenna 1 has value ", c[i], " expected (1+0j).")
                    break

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test16'
            self.rval = comp_gainphase(oldctab=self.ref[18], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)


        self.assertTrue(self.rval)

    def test17(self):
        '''Test 17:  wvrgcal4quasar_10s.ms, two antennas flagged in main table, one only partially, use of mingoodfrac'''
        myvis = self.vis_g
        os.system('rm -rf myinput.ms comp.W')
        os.system('cp -R ' + myvis + ' myinput.ms')

        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, wvrflag='DA41', toffset=-1.)

        flagdata(vis='myinput.ms', mode='manual', antenna='DA41&&*')
        flagdata(vis='myinput.ms', mode='manual', antenna='CM01&&*', scan='1') # antenna 0, scan 1 only!

        rvaldict2 = wvrgcal(vis="myinput.ms", caltable='comp.W', toffset=-1., mingoodfrac=0.5)

        print('test17-1')
        print(rvaldict)
        print('test17-2')
        print(rvaldict2)

        self.rval = rvaldict['success'] and rvaldict2['success']

        if(self.rval):
            print(rvaldict['Disc_um'][2], rvaldict2['Disc_um'][2])
            rvaldict2['Disc_um'][2]= rvaldict['Disc_um'][2] # The value for antenna 2 is the only one expected to be different
                                                            # as it was flagged. Replace by value for the unflagged case
                                                            # to make following test pass if all else agrees.
            rvaldict2['Flag'][2]=True # by the same logic as above
            if 'Frac_unflagged' in rvaldict.keys():
                rvaldict2['Frac_unflagged'][2]=rvaldict['Frac_unflagged'][2] # by the same logic as above
            print(rvaldict['RMS_um'][2], rvaldict2['RMS_um'][2])
            rvaldict2['RMS_um'][2]=rvaldict['RMS_um'][2] # by the same logic as above
            for mykey in ['Name', 'WVR', 'RMS_um', 'Disc_um']:
                print(mykey+" "+str(rvaldict[mykey]==rvaldict2[mykey]))
            self.rval = (rvaldict==rvaldict2)

        self.assertTrue(self.rval)

    def test18(self):
        '''Test 18:  wvrgcal4quasar_10s.ms, two antennas flagged in main table, one only partially'''
        myvis = self.vis_g
        os.system('rm -rf myinput.ms comp.W')
        os.system('cp -R ' + myvis + ' myinput.ms')

        flagdata(vis='myinput.ms', mode='manual', antenna='DA41&&*', spw='0,9~26')
        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, toffset=-1.)

        flagdata(vis='myinput.ms', mode='manual', antenna='DV12&&*', timerange='9:10:12~9:10:13,9:12:31~9:12:32', spw='0,9~26') # a few non-contiguous scans!
        rvaldict2 = wvrgcal(vis="myinput.ms", caltable='comp.W', toffset=-1.)

        print('test18-1')
        print(rvaldict)
        print('test18-2')
        print(rvaldict2)

        self.rval = rvaldict['success'] and rvaldict2['success']

        # DA41 has ID 2, DV12 has ID 12

        if(self.rval):
            print(rvaldict['Disc_um'][12], rvaldict2['Disc_um'][12])
            rvaldict2['Disc_um'][12]=rvaldict['Disc_um'][12] # The value for antenna 12 is the only one expected to be different
                                                             # as it was flagged only in the second call. Replace by value for the unflagged case
                                                             # to make following test pass if all else agrees.
            print(rvaldict['RMS_um'][12], rvaldict2['RMS_um'][12])
            rvaldict2['RMS_um'][12]=rvaldict['RMS_um'][12] # by the same logic as above
            self.rval = (rvaldict==rvaldict2)

        self.assertTrue(self.rval)

    def test19(self):
        '''Test 19:  wvrgcal4quasar_10s.ms, PM02 partially flagged in main table, DV41 with wvrflag, PM02 necessary for interpol of DV41'''
        myvis = self.vis_g
        os.system('cp -R ' + myvis + ' myinput.ms')

        flagdata(vis='myinput.ms', mode='manual', antenna='PM02&&*', scan='3')

        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, wvrflag='DA41', toffset=-1., mingoodfrac=0.2)

        if self.makeref:
            os.system('rm -rf newref/'+self.ref[19])
            os.system('cp -R '+self.out+' newref/'+self.ref[19])

        print('test19')
        print(rvaldict)

        self.rval = rvaldict['success']

        if(self.rval):
            self.rval = th.compTables(self.ref[19], self.out, ['WEIGHT','CPARAM'],
                                      tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test19'
            self.rval = comp_gainphase(oldctab=self.ref[19], newctab=self.out, myant=-1, spw=[0], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)


    def test20(self):
        '''Test 20:  wvrgcal4quasar_10s.ms, spw=[1,3,5,7], wvrspw=[0]'''
        myvis = self.vis_g
        os.system('rm -rf myinput2.ms comp.W')
        os.system('cp -R ' + myvis + ' myinput.ms')

        rvaldict = wvrgcal(vis="myinput.ms", caltable=self.out, toffset=-1., spw=[1,3,5,7], wvrspw=[0])

        rvaldict2 = wvrgcal(vis="myinput.ms", caltable='comp.W', toffset=-1.)

        print('test20-1')
        print(rvaldict)
        print('test20-2')
        print(rvaldict2)

        self.rval = rvaldict['success'] and rvaldict2['success']

        if(self.rval):
            self.rval = th.compcaltabnumcol(self.out, 'comp.W', self.comptabtol, colname1='CPARAM', colname2="CPARAM", testspw=1)
        if(self.rval):
            self.rval = th.compcaltabnumcol(self.out, 'comp.W', self.comptabtol, colname1='CPARAM', colname2="CPARAM", testspw=3)
        if(self.rval):
            self.rval = th.compcaltabnumcol(self.out, 'comp.W', self.comptabtol, colname1='CPARAM', colname2="CPARAM", testspw=5)
        if(self.rval):
            self.rval = th.compcaltabnumcol(self.out, 'comp.W', self.comptabtol, colname1='CPARAM', colname2="CPARAM", testspw=7)

        self.assertTrue(self.rval)

    def test21(self):
        '''Test 21:  uid___A002_X8ca70c_X5_shortened.ms - refant handling'''
        myvis = self.vis_h
        os.system('cp -R ' + myvis + ' myinput.ms')
        rvaldict = wvrgcal(vis="myinput.ms",caltable=self.out, toffset=0, refant=['DV11','DV12','DV09'], wvrflag=['DA41','DV11'], spw=[1,3,5,7])
        rvaldict2 = wvrgcal(vis="myinput.ms",caltable=self.out+'.ref', toffset=0, refant=['DV12'], wvrflag=['DA41','DV11'], spw=[1,3,5,7])

        print('test21')
        print(rvaldict)
        print(rvaldict2)

        self.rval = rvaldict['success'] and rvaldict2['success']

        if(self.rval):
            self.rval = (rvaldict['Disc_um'][0]==0.) and (rvaldict['Disc_um'][21]==0.)

        if(self.rval):
            self.rval = th.compTables(self.out+'.ref', self.out, ['WEIGHT','CPARAM'],
                                      tolerance=self.comptabtol)
            # ignore WEIGHT because it is empty, CPARAM because it is tested separately

        if(self.rval):
            figfile = ''
            if self.makeplots:
                figfile='test21'
            self.rval = comp_gainphase(oldctab=self.out+'.ref', newctab=self.out, myant=-1, spw=[1,3,5,7], tol_deg=self.compangtol, figfile=figfile)

        self.assertTrue(self.rval)

if __name__ == '__main__':
    unittest.main()
