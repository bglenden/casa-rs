##########################################################################
# test_task_plotms.py
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
# [Add the link to the JIRA ticket here once it exists]
#
# Based on the requirements listed in plone found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casaplotms.plotms.html
#
#
##########################################################################
import csv
import fnmatch
import matplotlib.pyplot as plt
import numpy as np
import os
import shutil
import sys
import unittest

from casatools import ctsys
from casaplotms import plotms
from casaplotms import plotmstool as pm

# Paths for data
datapath = ctsys.resolve("unittest/plotms/")

# caltable: (plotsize, xaxis, yaxis)
# Include ms for BPOLY and GSPLINE.
caltable_tests = {'a_mueller.uvcont.tbl': (250000, 'time', 'gamp'),
    'anpos.autoCAS13057.cal': (40000, 'antenna1', 'antpos'),
    'bandtypeBPOLY.B0': (70000, 'freq', 'gamp'),
    'df_jones.cal': (120000, 'antenna1', 'gamp'),
    'egaincurve.cal': (40000, 'time', 'gamp'),
    'f_jones.cal': (50000, 'time', 'tec'),
    'gaincaltest2.ms': (200000, 'time', 'amp'),
    'gaincaltest2.ms.T0': (40000, 'time', 'gamp'),
    'gaintypek.G0': (40000, 'antenna1', 'delay'),
    'gaintypeSpline.G0': (90000, 'time', 'gamp'),
    'g_evlaswpow.cal': (40000,  'time', 'spgain'),
    'glinxphf_jones.cal': (30000, 'time', 'gamp'),
    'k_jones.cal': (40000, 'antenna1', 'delay'),
    'm_mueller.cal': (40000, 'time', 'gamp'),
    'n08c1.sbdcal': (40000, 'time', 'delay'),
    'ngc5921.ref1a.bcal': (100000, 'chan', 'gamp'),
    'ngc5921.ref1a.gcal': (40000, 'time', 'gamp'),
    'ngc5921.ref2a.gcal': (40000, 'time', 'gamp'),
    'topac.cal': (30000, 'time', 'opac'),
    'tsysweight_ave.tsys.cal': (100000, 'chan', 'tsys'),
    'uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl': (70000, 'time', 'greal'),
    'uid___A002_X85c183_X36f.ms.hsd_skycal.s5_2.M100.skycal.tbl': (140000, 'time', 'greal'),
    'xf_jones.cal': (80000, 'chan', 'gphase')}

# Pick up alternative data directory to run tests on MMSs
if 'TEST_DATADIR' in os.environ:
    DATADIR = str(os.environ.get('TEST_DATADIR'))+'/plotms/'
    if os.path.isdir(DATADIR):
        datapath = DATADIR

print('plotms tests will use data from '+ datapath)

class plotms_test_base(unittest.TestCase):

    outputDir="/tmp/" + str(os.getpid()) + "/"
    plotfile_jpg = "/tmp/myplot.jpg"
    display = os.environ.get("DISPLAY")

    testms  = "pm_ngc5921.ms"
    testms2 = "ngc5921.ms"
    testms3 = "sun.subset.pentagon.ms"
    testms4 = "split_ddid_mixedpol_CAS-12283.ms"
    testct = 'ngc5921.ref1a.gcal'
    testct2 = 'ngc5921.ref2a.gcal'
    testct3 = 'a_mueller.uvcont.tbl'
    testct4 = 'ngc5921.ref1a.bcal'
    testct5 = 'uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl'
    testct6 = 'uid___A002_X85c183_X36f.ms.hsd_skycal.s5_2.M100.skycal.tbl'
    testct7 = 'anpos.autoCAS13057.cal'

    ms = os.path.join(outputDir, testms)
    ms2 = os.path.join(outputDir, testms2)
    ms3 = os.path.join(outputDir, testms3)
    ms4 = os.path.join(outputDir, testms4)
    ct = os.path.join(outputDir, testct)
    ct2 = os.path.join(outputDir, testct2)
    ct3 = os.path.join(outputDir, testct3)
    ct4 = os.path.join(outputDir, testct4)
    ct5 = os.path.join(outputDir, testct5)
    ct6 = os.path.join(outputDir, testct6)
    ct7 = os.path.join(outputDir, testct7)

    def cleanUp(self):
        if os.path.exists(self.outputDir):
            shutil.rmtree(self.outputDir)

    def setUpData(self):
        if not os.path.exists(self.ms):
            shutil.copytree(os.path.join(datapath, self.testms),
                self.ms, symlinks=True)

    def tearDownData(self):
        self.cleanUp()
        pm.setPlotMSFilename("")

    def setUpAltData(self):
        if not os.path.exists(self.ms2):
            shutil.copytree(os.path.join(datapath, self.testms2),
                self.ms2, symlinks=True)

    def setUpCallibData(self):
        if not os.path.exists(self.ms2):
            shutil.copytree(os.path.join(datapath, self.testms2),
                self.ms2, symlinks=True)
        if not os.path.exists(self.ct):
            shutil.copytree(os.path.join(datapath, self.testct),
                self.ct, symlinks=True)

    def setUpPointingData(self):
        if not os.path.exists(self.ms3):
            shutil.copytree(os.path.join(datapath, self.testms3),
                self.ms3, symlinks=True)

    def setUpCalData(self):
        for caltable in caltable_tests.keys():
            testtable = os.path.join(self.outputDir, caltable)
            if not os.path.exists(testtable):
                shutil.copytree(os.path.join(datapath, caltable), testtable,
                    symlinks=True)

    def setUpOverlayData(self):
        if not os.path.exists(self.ms4):
            shutil.copytree(os.path.join(datapath, self.testms4),
                self.ms4, symlinks=True)

    def checkPlotfile(self, plotfileName, minSize, maxSize=None):
        self.assertTrue(os.path.isfile(plotfileName), "Plot was not created")
        plotSize = os.path.getsize(plotfileName)
        print(plotfileName, 'file size is', plotSize)
        self.assertGreater(plotSize, minSize)
        if maxSize:
            self.assertLess(plotSize, maxSize)

    def checkTextfileAnts(self, plotfile_txt, exp_ant1, exp_ant2, exp_nrow, nheaders):
        # Read in exported text file and check antenna values
        self.assertTrue(os.path.isfile(plotfile_txt), "Plot was not created")
        with open(plotfile_txt, newline='') as textfile:
            reader = csv.reader(textfile, delimiter=' ')

            if (exp_nrow == 0): # selection yields zero rows
                nrow = 0
                for row in reader:
                    nrow += 1
                assertEqual(nrow, 2) # headers only
            else:
                # skip header lines
                [next(reader, None) for line in range(nheaders)]

                # read rows and check ant1, ant2
                nrow = 0
                for row in reader:
                    if exp_ant2 != "-1":
                        if (exp_ant2 == "*"):
                            # cross correlations of ANT1: *&ANT1 or ANT1&*
                            self.assertTrue(row[5] == exp_ant1 or row[6] == exp_ant1)
                            self.assertTrue(row[5] != row[6])
                        else:
                            # cross correlations of ANT1 & ANT2: ANT1&ANT2 or ANT2&ANT1
                            self.assertTrue((row[5] == exp_ant1 and row[6] == exp_ant2)
                                or (row[5] == exp_ant2 and row[6] == exp_ant1))
                    else:
                        # ANT1 & ANT2 baselines
                        self.assertTrue(row[5] == exp_ant1 and row[6] == exp_ant2)
                    nrow += 1

                # check nrow (number of points)
                self.assertEqual(nrow, exp_nrow)

    def checkNoPlotfile(self, plotfileName):
        self.assertFalse(os.path.isfile(plotfileName), "Plot was created")

    def getFilecount(self, dirName, namePattern):
        nameTarget = namePattern + '*'
        count = 0
        for  file in os.listdir( dirName ):
            if fnmatch.fnmatch( file, nameTarget):
                count = count + 1
        return count

    def removeFiles(self, dirName, namePattern):
        nameTarget = namePattern + '*'
        for  file in os.listdir( dirName ):
            if fnmatch.fnmatch( file, nameTarget):
                os.remove(os.path.join(dirName,file))

    def removePlotfile(self, plotfile=None):
        try:
            if not plotfile:
                plotfile = self.plotfile_jpg
            os.remove(plotfile)
        except OSError:  # "No such file or directory"
            pass

    def checkDisplay(self):
        self.assertGreater(len(self.display), 0, 'DISPLAY not set, cannot run test')

# ------------------------------------------------------------------------------

class test_basic(plotms_test_base):
    ''' Test basic single plots '''

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_basic_plot(self):
        '''test_basic_plot: Basic plot with default xaxis, yaxis, or both'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testBasic01.jpg")
        self.removePlotfile()
        # default axes
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)
        self.removePlotfile()
        # default xaxis only
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            yaxis='freq', showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 240000)
        self.removePlotfile()
        # default yaxis only
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            xaxis='scan', showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 50000)

    def test_basic_blankplot(self):
        '''test_basic_blankplot: Blank plot running plotms with no arguments'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testBasic02.jpg")
        self.removePlotfile()
        with self.assertRaises(Exception):
            plotms(showgui=False, plotfile=self.plotfile_jpg, highres=True)

    def test_basic_overwrite(self):
        '''test_basic_overwrite: Check overwrite functionality'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testBasic03.jpg")
        self.removePlotfile()
        # Create plotfile
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)

        # Next, overwrite is False so the save should fail.
        with self.assertRaises(RuntimeError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg,
                showgui=False, highres=True)

        # Next, overwrite is True so the save should succeed.
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            overwrite=True, showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)

    def test_basic_badplotindex(self):
        '''test_basic_badplotindex: nonzero plotindex'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testBasic04.jpg")
        self.removePlotfile()
        # Fail with nonzero plotindex when clearplots=True (default)
        with self.assertRaises(ValueError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg,
                showgui=False, highres=True, plotindex=1)

    def test_basic_pngExport(self):
        '''test_basic_pngExport: Export plot in png format'''
        plotfile_png = os.path.join(self.outputDir, "testBasic05.png")
        self.removePlotfile(plotfile_png)
        plotms(vis=self.ms, plotfile=plotfile_png,
            showgui=False, highres=False)
        self.checkPlotfile(plotfile_png, 20000)

    def test_basic_overplotSkipPlotindex(self):
        '''test skipped plotindex in overplot should still plot'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testBasic06.jpg")
        self.removePlotfile()
        # Plot MS scan vs time, plotindex = 0 (default)
        plotms(vis=self.ms, showgui=False, yaxis='scan')
        # Plot MS field vs time and export it, plotindex = 2 (skip 1)
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, yaxis='field', plotindex=2, clearplots=False)
        self.checkPlotfile(self.plotfile_jpg, 45000)

    def xtest_basic_screenExport(self):
        '''test_basic_screenExport: Export plot in screen resolution'''
        # not working yet...
        self.plotfile_jpg = os.path.join(self.outputDir, "testBasic07.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=False)
        self.checkPlotfile(self.plotfile_jpg, 30000)

    def test_basic_pngExport(self):
        '''test_basic_pngExport: Export plot in png format'''
        plotfile_png = os.path.join(self.outputDir, "testBasic07.png")
        self.removePlotfile(plotfile_png)
        plotms(vis=self.ms, plotfile=plotfile_png,
               showgui=False, highres=False)
        self.checkPlotfile(plotfile_png, 20000)
        self.removePlotfile(plotfile_png)

    def xtest_basic_pdfExport(self):
        '''test_basic_pdfExport: Export plot in pdf format'''
        plotfile_pdf = os.path.join(self.outputDir, "testBasic08.pdf")
        self.removePlotfile(plotfile_pdf)
        plotms(vis=self.ms, plotfile=plotfile_pdf,
            showgui=False, highres=False)
        self.checkPlotfile(plotfile_pdf, 135000)
        self.removePlotfile(plotfile_pdf)

    def xtest_basic_psExport(self):
        '''test_basic_psExport: Export plot in ps format'''
        plotfile_ps = os.path.join(self.outputDir, "testBasic9.ps")
        self.removePlotfile(plotfile_ps)
        plotms(vis=self.ms, plotfile=plotfile_ps, expformat='ps',
               showgui=False, highres=False)
        self.checkPlotfile(plotfile_ps, 1500000)
        self.removePlotfile(plotfile_ps)

    def xtest_basic_txtExport(self):
        '''test_basic_txtExport: Export plot in txt format'''
        plotfile_txt = os.path.join(self.outputDir, "testBasic10.txt")
        self.removePlotfile(plotfile_txt)
        # Verbose text export (default) - use selection for small plot
        plotms(vis=self.ms, xaxis='chan',
               plotfile=plotfile_txt, expformat='txt', verbose=True,
               spw='0:20', antenna='0', scan='4', correlation='LL',
               showgui=False, highres=False)
        self.checkPlotfile(plotfile_txt, 4000)
        self.removePlotfile(plotfile_txt)
        # Exclude metadata in text export - use selection for small plot
        plotms(vis=self.ms, xaxis='chan',
               plotfile=plotfile_txt, expformat='txt', verbose=False,
               spw='0:20', antenna='0', scan='4', correlation='LL',
               showgui=False, highres=False)
        self.checkPlotfile(plotfile_txt, 700)
        self.removePlotfile(plotfile_txt)

# ------------------------------------------------------------------------------

class test_overplot(plotms_test_base):
    ''' Test overplot two MS '''

    def setUp(self):
        self.checkDisplay()
        self.setUpData()
        self.setUpAltData()

    def tearDown(self):
        self.tearDownData()

    def test_overplot_2MS_2color(self):
        '''test_overplot_2MS_2color: Overplot two data sets with different colors.'''
        if os.path.exists(os.path.join(self.outputDir, self.ms2)):
            self.plotfile_jpg = os.path.join(self.outputDir, "testOverplot01.jpg")
            self.removePlotfile()
            # Plot first MS scan vs time
            plotms(vis=self.ms, showgui=False, yaxis='scan', xaxis='time',
                customsymbol=[True], symbolshape=['diamond'], symbolsize=[3],
                symbolcolor=['ff0000'], symbolfill=['mesh3'], highres=True)
            # Plot second MS field vs time in different color and export it
            plotms(vis=self.ms2, plotfile=self.plotfile_jpg,
                showgui=False, yaxis='field', xaxis='time',
                plotindex=1, clearplots=False,
                customsymbol=[True], symbolshape=['circle'], symbolsize=[3],
                symbolcolor=['00FF00'], symbolfill=['mesh3'], highres=True)
            self.checkPlotfile(self.plotfile_jpg, 45000)
        else:
            print("Skipping test, no path to alternate MS")

    def test_overplot_2MS_freq(self):
        '''test_overplot_2MS_freq: CAS-6975 overplotting problem'''
        if os.path.exists(os.path.join(self.outputDir, self.ms2)):
            self.plotfile_jpg = os.path.join(self.outputDir, "testOverplot02.jpg")
            self.removePlotfile()
            # Create the first plot
            plotms(vis=self.ms, showgui=False, highres=True,
                xaxis="freq", yaxis="phase", avgchannel="63")
            # Do an overplot with a different file
            plotms(vis=self.ms2, showgui=False, plotfile=self.plotfile_jpg,
                xaxis="freq", yaxis="phase", avgchannel="63",
                plotindex=1, clearplots=False, highres=True)
            self.checkPlotfile(self.plotfile_jpg, 30000)
        else:
            print("Skipping test, no path to alternate MS")

    def test_overplot_scriptfile(self):
        '''test_overplot_scriptfile: test exec file with overplots'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testOverplot03.jpg")
        self.removePlotfile()

        # create the script file
        scriptfile = "/tmp/scriptfile.py"
        file1 = open(scriptfile, 'w')
        file1.write("plotms('" + self.ms + "', scan='1', avgtime='60', showgui=False)\n")
        file1.write("plotms('" + self.ms + "', scan='2', avgtime='60', customsymbol=True, symbolcolor='orange', clearplots=False, plotindex=1, showgui=False)\n")
        file1.write("plotms('" + self.ms + "', scan='3', avgtime='60', customsymbol=True, symbolcolor='green', clearplots=False, plotindex=2, showgui=False)\n")
        file1.write("plotms('" + self.ms + "', scan='4', avgtime='60', customsymbol=True, symbolcolor='red', clearplots=False, plotindex=3, showgui=False, plotfile='" + self.plotfile_jpg + "', highres=True)\n")
        file1.close()

        # exec the script file
        testscript = open(scriptfile, 'r')
        exec(testscript.read())
        testscript.close()
        self.removePlotfile(scriptfile)
        self.checkPlotfile(self.plotfile_jpg, 180000)

# ------------------------------------------------------------------------------

class test_averaging(plotms_test_base):
    ''' test plotms averaging options '''

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_averaging_time(self):
        '''test_averaging_time: Average time'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging01.jpg")
        self.removePlotfile()
        # interval = 30s
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, avgtime='60', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000, 350000)

    def test_averaging_timescan(self):
        '''test_averaging_timescan: Average time over scans'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging02.jpg")
        self.removePlotfile()
        # interval = 30s
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, avgtime='120', avgscan=True)
        self.checkPlotfile(self.plotfile_jpg, 140000, 240000)

    def test_averaging_timefield(self):
        '''test_averaging_timefield: Average time over fields'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging03.jpg")
        self.removePlotfile()
        # interval = 30s
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, avgtime='120', avgfield=True)
        self.checkPlotfile(self.plotfile_jpg, 140000, 240000)

    def test_averaging_chan(self):
        '''test_averaging_chan: Average channel'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging04.jpg")
        self.removePlotfile()
        # nchan = 63
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, avgchannel='7', xaxis='chan')
        self.checkPlotfile(self.plotfile_jpg, 40000, 80000)

    def test_averaging_baseline(self):
        '''test_averaging_baseline: Average over baseline'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging05.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, avgbaseline=True)
        self.checkPlotfile(self.plotfile_jpg, 140000, 220000)

    def test_averaging_antenna(self):
        '''test_averaging_antenna: Average per antenna'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging06.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, avgantenna=True)
        self.checkPlotfile(self.plotfile_jpg, 160000, 340000)

    def test_averaging_blnant(self):
        '''test_averaging_blnant: Average over baseline and per antenna (should fail)'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging07.jpg")
        self.removePlotfile()
        with self.assertRaises(ValueError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
                showgui=False, avgbaseline=True, avgantenna=True)

    def test_averaging_spw(self):
        '''test_averaging_spw: Average over spw'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAveraging08.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, avgspw=True)
        self.checkPlotfile(self.plotfile_jpg, 190000, 340000)

# ------------------------------------------------------------------------------

class test_axis(plotms_test_base):
    ''' test axis and datacolumn options '''

    def setUp(self):
        self.checkDisplay()
        self.setUpData()
        self.setUpAltData()
        self.setUpPointingData()

    def tearDown(self):
        self.tearDownData()

    def test_axis_all(self):
        '''test_axis_all: Test all axis names'''
        axes = ['scan', 'field', 'time', 'interval', 'spw', 'chan',
                'freq', 'vel', 'corr', 'ant1', 'ant2', 'baseline',
                'row', 'amp', 'phase', 'real', 'imag', 'wt', 'wtsp',
                'sigma', 'sigmasp', 'flag', 'flagrow', 'uvdist',
                'uvwave', 'u', 'v', 'w', 'uwave', 'vwave', 'wwave',
                'azimuth', 'elevation', 'hourang', 'parang', 'ant',
                'ant-azimuth', 'ant-elevation', 'ant-parang', 'ant-ra',
                'ant-dec', 'observation', 'intent']
        for axis in axes:
            filename = "testAxis01_" + axis + ".jpg"
            plotfile = os.path.join(self.outputDir, filename)
            self.removePlotfile(plotfile)
            axis_vis = self.ms
            if axis in ['ant-ra','ant-dec']:
                axis_vis = self.ms3
            plotms(vis=axis_vis, plotfile=plotfile, highres=True,
                showgui=False, yaxis=axis)
            self.checkPlotfile(plotfile, 40000)
            self.removePlotfile(plotfile)

    def test_axis_synonyms(self):
        '''test_axis_synonym: Test shortened form of axis names'''
        # Just test a commonly-used subset
        syns = ['chan', 'freq', 'vel', 'ant', 'ant1', 'ant2',
                'imag', 'wtsp']
        for syn in syns:
            filename = "testAxis02_" + syn + ".jpg"
            plotfile = os.path.join(self.outputDir, filename)
            self.removePlotfile(plotfile)
            plotms(vis=self.ms, plotfile=plotfile, highres=True,
                showgui=False, yaxis=syn)
            self.checkPlotfile(plotfile, 30000)
            self.removePlotfile(plotfile)

    def test_axis_syn_bad(self):
        '''test_axis_syn_bad: Test invalid axis synonym'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis03.jpg")
        self.removePlotfile()
        with self.assertRaises(ValueError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
                showgui=False, yaxis='veloc')

    def test_axis_wtamp(self):
        '''test_axis_wtamp: Test that wt*amp works for x-and y-axis choices.'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis04.jpg")
        self.removePlotfile()
        # plot wt*amp vs time
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, showgui=False,
            highres=True, yaxis='wt*amp', xaxis='time')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile()

        # plot time vs wt*amp
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, showgui=False,
            highres=True, xaxis='wt*amp', yaxis='time')
        self.checkPlotfile(self.plotfile_jpg, 40000)

    def test_axis_list(self):
        '''test_axis_list: plot yaxis list'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis05.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, xaxis='time', yaxis=['scan','field'],
            yaxislocation=['left','right'],
            customsymbol=[True,True], symbolshape=['diamond','circle'],
            symbolsize=[5,5], symbolcolor=['ff0000','00ff00'],
            symbolfill=['mesh3','mesh3'], highres=True)
        self.checkPlotfile(self.plotfile_jpg, 40000)

    def test_axis_list_duplicate(self):
        '''test_axis_list_duplicate: plot duplicate yaxis list'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis06.jpg")
        self.removePlotfile()
        # duplicate y-axes (same axis, same datacolumn) fails
        with self.assertRaises(RuntimeError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, overwrite=True,
                showgui=False, yaxis=['amp','amp'],
                yaxislocation=['left','right'], highres=True)

    def test_axis_list_ydatacolumn(self):
        '''test_axis_list_ydatacolumn: plot duplicate yaxis list with diff datacol'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis07.jpg")
        self.removePlotfile()
        plotms(vis=self.ms2, plotfile=self.plotfile_jpg,
            overwrite=True, showgui=False, xaxis='time', yaxis=['amp','amp'],
            ydatacolumn=['data','model'], yaxislocation=['left','right'],
            customsymbol=[True,True], symbolshape=['diamond','circle'],
            symbolsize=[5,5], symbolcolor=['ff0000','00ff00'],
            symbolfill=['mesh3','mesh3'], highres=True)
        self.checkPlotfile(self.plotfile_jpg, 70000)

    def test_axis_datacolumns(self):
        '''test_axis_datacolumns: Test datacolumn options'''
        datacols = ['data'
                   , 'corrected']
        '''
                   , 'model'
                   , 'residual'
                   , 'corrected-model'
                   , 'corrected-model_vector'
                   , 'corrected-model_scalar'
                   , 'data-model'
                   , 'data-model_vector'
                   , 'data-model_scalar'
                   , 'corrected/model'
                   , 'corrected/model_vector'
                   , 'corrected/model_scalar'
                   , 'data/model'
                   , 'data/model_vector'
                   , 'data/model_scalar']
        '''
        for datacol in datacols:
            filename = "testAxis08_datacolumn.jpg"
            plotfile = os.path.join(self.outputDir, filename)
            self.removePlotfile(plotfile)
            plotms(vis=self.ms2, plotfile=plotfile, highres=True,
                showgui=False, ydatacolumn=datacol)
            minSize = 60000
            if datacol == 'model':
                minSize = 30000
            self.checkPlotfile(plotfile, minSize)
            self.removePlotfile(plotfile)

    def test_axis_baddatacolumn(self):
        '''test_axis_baddatacolumn: Test invalid datacolumn'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis09.jpg")
        self.removePlotfile()
        with self.assertRaises(ValueError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
                showgui=False, ydatacolumn='corr/model')

    def test_axis_nodatacolumn(self):
        '''test_axis_nodatacolumn: Test non-visibility axes (no datacolumn)'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis10.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            showgui=False, xaxis='elevation', yaxis='azimuth')
        self.checkPlotfile(self.plotfile_jpg, 30000)

    def test_axis_datacolumnNoFloat(self):
        '''test_axis_datacolumnNoFloat: ms with no float datacolumn fails'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis11.jpg")
        self.removePlotfile()
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
                showgui=False, ydatacolumn='float')

    def test_axis_datacolumnNoCorrected(self):
        '''test_axis_datacolumnNoCorrected: ms with no corrected datacolumn plots data'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis12.jpg")
        self.removePlotfile()
        # test ms has no corrected data, should revert to data
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, yaxis='amp', ydatacolumn='corrected',
            xaxis='freq', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 40000)

    def test_axis_radec_params(self,debug=False):
        '''test_axis_radec_params: Test ant-ra/ant-dec parameters'''
        yx_axes = [('ant-ra','time'),
                   ('ant-dec','time'),
                   ('ant-dec','ant-ra')
                  ]
        interp_methods = ['nearest','cubic spline']
        # Sub-plots grid
        grid_cols = len(yx_axes)
        grid_rows = len(interp_methods)
        n_plots = grid_rows*grid_cols
        # Create 1 plot file per supported reference frame
        ref_frames = {'icrs':30000, 'j2000':30000, 'b1950':30000,
                      'galactic':30000,'azelgeo':30000}
        for ref_frame, plot_min_size in ref_frames.items( ):
            # Plot file
            plot_filename = "testAxis13_radec_" + ref_frame + ".png"
            plot_dir = self.outputDir if not debug else '/tmp'
            plot_path = os.path.join(plot_dir, plot_filename)
            self.removePlotfile(plot_path)
            # Create sub-plots, export plot when plotting last sub-plot
            plot_index = 0
            for row, interp_method in enumerate(interp_methods):
                for col, (y_axis,x_axis) in enumerate(yx_axes):
                    is_first_plot = (plot_index == 0)
                    is_last_plot = ( (plot_index + 1) == n_plots)
                    title_fmt = 'ref={ref_frame}, interp={interp}'
                    title = title_fmt.format(ref_frame=ref_frame,
                                             interp=interp_method)
                    plotms(
                        vis = self.ms3,
                        #
                        title = title,
                        titlefont = 10,
                        #
                        gridrows = grid_rows,
                        gridcols = grid_cols,
                        #
                        rowindex = row,
                        colindex = col,
                        plotindex = plot_index,
                        #
                        xaxis = x_axis,
                        xframe = ref_frame,
                        xinterp = interp_method,
                        #
                        yaxis = y_axis,
                        yframe = ref_frame,
                        yinterp = interp_method,
                        #
                        coloraxis = 'spw',
                        #
                        plotfile = plot_path if is_last_plot else '',
                        width = 1024,
                        height = 768,
                        highres = True,
                        overwrite = True,
                        #
                        showgui = False,
                        #
                        clearplots = is_first_plot
                        )
                    plot_index = plot_index + 1
                    # Note: last plotms call is blocking : plotms waits
                    #       until all plots are drawn before exporting
            self.checkPlotfile(plot_path, plot_min_size)
            if not debug:
                self.removePlotfile(plot_path)

    def test_axis_plotrange(self):
        '''test_axis_plotrange: test asymmetrical and reverse plotrange'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis15.jpg")
        self.removePlotfile()
        # autorange x and y; plotms makes ranges symmetrical about 0
        plotrange1 = [0, 0, 0, 0]
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            xaxis='u', yaxis='v', showgui=False, plotrange=plotrange1)
        self.checkPlotfile(self.plotfile_jpg, 180000, 310000)
        self.removePlotfile()
        # autorange x, limit y - larger plot
        plotrange2 = [0, 0, -500, 500]
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            xaxis='u', yaxis='v', showgui=False, plotrange=plotrange2)
        self.checkPlotfile(self.plotfile_jpg, 180000)
        self.removePlotfile()
        # autorange x, reverse limit y - larger plot
        plotrange3 = [0, 0, 500, -500]
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
            xaxis='u', yaxis='v', showgui=False, plotrange=plotrange3)
        self.checkPlotfile(self.plotfile_jpg, 180000)

    def test_axis_cal(self):
        '''test_axis_cal: Test invalid caltable axes for MS'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testAxis16.jpg")
        self.removePlotfile()
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
                showgui=False, xaxis='poln', yaxis='amp')
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, highres=True,
                showgui=False, xaxis='antpos', yaxis='amp')

# ------------------------------------------------------------------------------

class test_calibration(plotms_test_base):
    ''' test plotms callib parameter '''

    def setUp(self):
        self.checkDisplay()
        self.setUpCallibData()

    def tearDown(self):
        self.tearDownData()

    def test_calibration_callib(self):
        '''test_calibration_callib: callib string parameter for OTF calibration'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalibration01.jpg")
        self.removePlotfile()
        callibStr = "caltable='" + self.ct + "' calwt=True tinterp='nearest'"
        plotms(vis=self.ms2, plotfile = self.plotfile_jpg,
            ydatacolumn="corrected", xaxis="frequency",
            showgui=False, callib=callibStr, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 160000)

    def test_calibration_badcallib(self):
        '''test_calibration_badcallib: callib file does not exist'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalibration02.jpg")
        self.removePlotfile()
        with self.assertRaises(RuntimeError):
            plotms(vis=self.ms2, plotfile = self.plotfile_jpg,
                ydatacolumn="corrected", xaxis="frequency",
                showgui=False, callib='/tmp/nocallib.txt',
                highres=True)

# ------------------------------------------------------------------------------

class test_calplot(plotms_test_base):
    ''' Test plotting cal tables '''

    def setUp(self):
        self.checkDisplay()
        self.setUpCalData()

    def tearDown(self):
        self.tearDownData()

    def test_calplot_basic(self):
        '''test_calplot_basic: Basic plot of caltable with default axes'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot01.jpg")
        self.removePlotfile()
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 30000)

    def test_calplot_axes(self):
        '''test_calplot_axes: Basic plot of caltable with non-default axes'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot02.jpg")
        self.removePlotfile()
        # gamp vs scan
        plotms(caltable=self.ct, xaxis='scan', plotfile=self.plotfile_jpg,
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 20000)
        self.removePlotfile()
        # gphase vs baseline
        plotms(caltable=self.ct, yaxis='phase', xaxis='baseline',
            plotfile=self.plotfile_jpg, showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 60000)
        self.removePlotfile()

    def test_calplot_iteration(self):
        '''test_calplot_iteration: caltable with corr iteraxis'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot03.jpg")
        plotfile1 = os.path.join(self.outputDir, "testCalPlot03_Poln1_2.jpg")
        self.removeFiles(self.outputDir, "testCalPlot03_")
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, iteraxis='corr', exprange='all')
        fileCount = self.getFilecount(self.outputDir, "testCalPlot03_")
        self.assertEqual(fileCount, 2)
        self.checkPlotfile(plotfile1, 30000)
        self.removeFiles(self.outputDir, "testCalPlot03_")

    def test_calplot_polselection(self):
        '''test_calplot_polselection: caltable with polarization selection'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot04.jpg")
        self.removePlotfile()
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, correlation='R')
        self.checkPlotfile(self.plotfile_jpg, 30000)
        self.removePlotfile()
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, polarization='R')
        self.checkPlotfile(self.plotfile_jpg, 30000)
        self.removePlotfile()

    def test_calplot_ratioplot(self):
        '''test_calplot_ratioplot: caltable with ratio polarization selection'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot05.jpg")
        self.removePlotfile()
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, correlation='/')
        self.checkPlotfile(self.plotfile_jpg, 50000)

    def test_calplot_antselection(self):
        '''test_calplot_antselection: caltable with antenna selection'''
        # cal tables are small enough to export to text
        plotfile_txt = os.path.join(self.outputDir, "testCalPlot06.txt")

        # antenna selections
        ant1 = '1'
        cross_ant = "1&0"
        nHeaderRows = 5

        # (testct, ant2, nrow, crossant2, crossnrow):
        anttests = [(self.ct, '-1', 14, '-1', 14), # pure ant-based
            (self.ct2, '1', 24, '0', 24),          # refant-based, ref=1
            (self.ct3, '*', 41408, '0', 3104)]     # baseline-based

        for (testct, ant2, nrow, crossant2, crossnrow) in anttests:
            self.removePlotfile(plotfile_txt)

            # ANT1 test - select ant1
            plotms(caltable=testct, plotfile=plotfile_txt,
                showgui=False, highres=True, antenna=ant1)
            self.checkTextfileAnts(plotfile_txt, ant1, ant2, nrow, nHeaderRows)
            self.removePlotfile(plotfile_txt)

            # ANT1 & ANT2 test - select cross_ant
            if (crossnrow > 0):
                plotms(caltable=testct, plotfile=plotfile_txt,
                    showgui=False, highres=True, antenna=cross_ant)
                self.checkTextfileAnts(
                    plotfile_txt, ant1, crossant2, crossnrow, nHeaderRows)
            else:
                with self.assertRaises(Exception):
                    plotms(caltable=testct, plotfile=plotfile_txt,
                        showgui=False, highres=True, antenna=cross_ant)
        self.removePlotfile(plotfile_txt)

        # test SD cal table, treated as pure ant-based
        nHeaderRows = 6 # add spw selection for fewer points in text export
        plotms(caltable=self.ct5, plotfile=plotfile_txt,
            showgui=False, highres=True, spw='23', antenna=ant1)
        self.checkTextfileAnts(plotfile_txt, '1', '1', 6000, nHeaderRows)
        self.removePlotfile(plotfile_txt)
        # With cross_ant '1&0': selection ignores the '&0'
        plotms(caltable=self.ct5, plotfile=plotfile_txt,
            showgui=False, highres=True, spw='23', antenna=cross_ant)
        self.checkTextfileAnts(plotfile_txt, '1', '1', 6000, nHeaderRows)
        self.removePlotfile(plotfile_txt)

    def test_calplot_chanselection(self):
        '''test_calplot_chanselection: caltable with spw:chan selections'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot07.jpg")
        self.removePlotfile()
        # No selection (only one spw)
        plotms(caltable=self.ct4, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, spw='0')
        self.checkPlotfile(self.plotfile_jpg, 100000)
        self.removePlotfile()
        # Smaller plot with chan selection
        plotms(caltable=self.ct4, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, spw='0:0~10')
        self.checkPlotfile(self.plotfile_jpg, 45000, 80000)

    def test_calplot_default_axes(self):
        '''test_calplot_default_axes: supported caltable types with default axes'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot08.jpg")
        self.removePlotfile()

        for table in caltable_tests.keys():
            if os.path.splitext(table)[1] == '.ms':
                continue
            print('Test caltable default axes:', table)
            testtable = os.path.join(self.outputDir, table)
            (plotsize, xaxis, yaxis) = caltable_tests[table]

            # no xaxis or yaxis parameters: use default
            plotms(caltable=testtable, plotfile=self.plotfile_jpg,
               showgui=False, highres=True, overwrite=True)
            self.checkPlotfile(self.plotfile_jpg, plotsize)
            self.removePlotfile()

            # Set xaxis and yaxis parameters explicitly
            plotms(caltable=testtable, plotfile=self.plotfile_jpg, xaxis=xaxis,
               yaxis=yaxis, showgui=False, highres=True, overwrite=True)
            self.checkPlotfile(self.plotfile_jpg, plotsize)
            self.removePlotfile()

    def test_calplot_cal_axes(self):
        '''test_calplot_cal_axes: plot supported caltable types with cal yaxis'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot09.jpg")
        self.removePlotfile()

        # test non-default cal axes
        # (caltable, plotsize, xaxis, yaxis)
        axes_tests = [('ngc5921.ref1a.gcal', 90000, "time", "gphase"),
            ('ngc5921.ref1a.gcal', 60000, "time", "greal"),
            ('ngc5921.ref1a.gcal', 60000, "time", "gimag"),
            # test Fringe Jones axes
            ('n08c1.sbdcal', 40000, "time", "rate"),
            ('n08c1.sbdcal', 40000, "time", "disp"),
            # tests axes added in CAS-13450: az, el, uvdist
            ('a_mueller.uvcont.tbl', 60000, 'uvdist', 'gamp'),
            ('ngc5921.ref1a.gcal', 40000, 'azimuth', 'elevation'),
            ('tsysweight_ave.tsys.cal', 30000, 'elevation', 'tsys')]

        for (table, plotsize, xaxis, yaxis) in axes_tests:
            testtable = os.path.join(self.outputDir, table)
            print('Test caltable', table, ':', yaxis, 'vs', xaxis)

            plotms(caltable=testtable, plotfile=self.plotfile_jpg, xaxis=xaxis,
               yaxis=yaxis, showgui=False, highres=True, overwrite=True)
            self.checkPlotfile(self.plotfile_jpg, plotsize)
            self.removePlotfile()

    def test_calplot_averaging(self):
        '''test_calplot_averaging: caltable with time and channel averaging'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot10.jpg")
        self.removePlotfile()
        # Time averaging for G Jones table
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, avgtime='6000', avgscan=True)
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile()
        # Channel averaging for B Jones table
        plotms(caltable=self.ct4, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, avgchannel='6000')
        self.checkPlotfile(self.plotfile_jpg, 40000)

    def test_calplot_avg_bpoly(self):
        '''test_calplot_avg_bpoly: BPOLY averaging fails'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot11.jpg")
        self.removePlotfile()
        # BPOLY fails with channel averaging
        bpoly = os.path.join(self.outputDir, "bandtypeBPOLY.B0")
        with self.assertRaises(Exception):
            plotms(caltable=bpoly, plotfile=self.plotfile_jpg,
                showgui=False, highres=True, xaxis='chan', avgchannel='8')

    def test_calplot_avg_gspline(self):
        '''test_calplot_avg_gspline: GSPLINE averaging fails'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot12.jpg")
        self.removePlotfile()
        # GSPLINE fails with time averaging
        gspline = os.path.join(self.outputDir, "gaintypeSpline.G0")
        with self.assertRaises(Exception):
            plotms(caltable=gspline, plotfile=self.plotfile_jpg,
                showgui=False, highres=True, avgtime='600')

    def test_calplot_avg_sel(self):
        '''test_calplot_avg_sel: caltable with averaging and selection combined'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot13.jpg")
        self.removePlotfile()

        # Time averaging with field selection
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, avgtime='6000', field='1')
        self.checkPlotfile(self.plotfile_jpg, 70000)
        self.removePlotfile()

        # Time averaging with spw selection (only 1 spw in ct)
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, avgtime='6000', field='1', spw='0')
        self.checkPlotfile(self.plotfile_jpg, 70000)
        self.removePlotfile()

        # Channel averaging with spw:channel selection
        plotms(caltable=self.ct4, plotfile=self.plotfile_jpg,
                showgui=False, highres=True, avgchannel='10', spw='0:10~50')
        self.checkPlotfile(self.plotfile_jpg, 50000)

    def test_calplot_avg_chan_ant(self):
        '''test_calplot_avg_chan_ant: caltable with channel and antenna averaging combined'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot14.jpg")
        self.removePlotfile()

        # CAS-13456
        plotms(caltable=self.ct6, plotfile=self.plotfile_jpg, xaxis='time', yaxis='amp',
            field='1', spw='17', avgchannel='1e8', avgantenna=True,
            coloraxis='ant1', showlegend=True, showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 120000)

    def test_calplot_selection_iteration_avg(self):
        '''test_calplot_selection_iteration_avg: caltable with antenna selection, iteraxis, and averaging'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot15.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testCalPlot15_AntennaVA01@VLA:N7.jpg")
        plotfile2_jpg = os.path.join(self.outputDir, "testCalPlot15_AntennaDA61@A075.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        # Exclude antenna 1 with iteration (pure antenna-based cal table)
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, iteraxis='antenna', antenna='!1')
        self.checkPlotfile(plotfile1_jpg, 45000)
        self.removePlotfile(plotfile1_jpg)
        # Exclude antenna 1 with iteration and averaging (baseline-based cal table)
        plotms(caltable=self.ct6, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, iteraxis='antenna', antenna='!1', avgantenna=True)
        self.checkPlotfile(plotfile2_jpg, 180000)
        self.removePlotfile(plotfile2_jpg)

    def test_calplot_poln(self):
        '''test_calplot_poln: CAS-14311 caltable with poln axis, selection, iteraxis, coloraxis'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot16.jpg")
        self.removePlotfile(self.plotfile_jpg)
        # gcal table poln xaxis
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, xaxis='poln')
        self.checkPlotfile(self.plotfile_jpg, 35000)
        self.removePlotfile(self.plotfile_jpg)
        # gcal table poln selection
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, polarization='R')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile(self.plotfile_jpg)
        # gcal table poln iteraxis
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, iteraxis='poln', exprange='all')
        fileCount = self.getFilecount(self.outputDir, "testCalPlot16_")
        self.assertEqual(fileCount, 2)
        self.removeFiles(self.outputDir, "testCalPlot16_")
        # gcal table poln coloraxis
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, coloraxis='poln')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile(self.plotfile_jpg)

    def test_calplot_antpos(self):
        '''test_calplot_antpos: CAS-14311 caltable with antpos axis, selection, iteraxis, coloraxis'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testCalPlot17.jpg")
        self.removePlotfile(self.plotfile_jpg)
        # antpos table antpos xaxis
        plotms(caltable=self.ct7, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, xaxis='antpos')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile(self.plotfile_jpg)
        # antpos table antpos selection
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, antpos='X')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile(self.plotfile_jpg)
        # antpos table antpos iteraxis
        plotms(caltable=self.ct7, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, iteraxis='antpos', exprange='all')
        fileCount = self.getFilecount(self.outputDir, "testCalPlot17_")
        self.assertEqual(fileCount, 3) # X, Y, Z
        self.removeFiles(self.outputDir, "testCalPlot17_")
        # antpos table antpos coloraxis
        plotms(caltable=self.ct7, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, coloraxis='antpos')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile(self.plotfile_jpg)
        # gcal table invalid antpos xaxis exception
        with self.assertRaises(Exception):
            plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
                showgui=False, highres=True, xaxis='antpos')
        # gcal table invalid antpos iteraxis reset to None
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, iteraxis='antpos', exprange='all')
        fileCount = self.getFilecount(self.outputDir, "testCalPlot17")
        self.assertEqual(fileCount, 1)
        self.removePlotfile(self.plotfile_jpg)
        # gcal table invalid antpos coloraxis reset to None
        plotms(caltable=self.ct, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, coloraxis='antpos')
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile(self.plotfile_jpg)

# ------------------------------------------------------------------------------

class PlotmsPageHeader:
    ''' Analyze PlotMS page header from png image of PlotMS Plot
        Assumptions: graphical area has a black frame, header background color is light
    '''
    def __init__(self,png_path,debug=False):
        self.png_path = png_path
        self.debug = debug
        self.color_img = plt.imread(png_path) # shape = (height,width,4)
        self.img_height , self.img_width , depth = self.color_img.shape
        self.height = -1
        self.height_ratio = -1.0
        self.rows = -1
        self._analyze()

    def _analyze(self):
        gray_img = self.color_img.min(axis=2)
        # Binarize
        is_light = gray_img > 0.75
        bin_img = np.zeros(gray_img.shape,dtype=gray_img.dtype)
        bin_img[is_light] = 1.0
        # Project on X (vertical) axis
        gray_xproj_bin = bin_img.min(axis=1)
        # White to black transitions
        (steps_down,) = np.where(np.diff(gray_xproj_bin) == -1.0 )
        if steps_down.size > 0 :
            self.height = steps_down[-1]
            self.rows = steps_down.size - 1
        else:
            self.height = 0
            self.rows = 0
        if self.img_height > 0:
            self.height_ratio = float(self.height) / self.img_height
        if self.debug:
            gray_xmask = np.outer(gray_xproj_bin,np.ones(self.img_width,dtype=np.float))
            path_parts = os.path.splitext(self.png_path)
            gray_xmask_path = ''.join([path_parts[0],'.xmask',path_parts[1]])
            plt.imsave(gray_xmask_path,gray_xmask,cmap=plt.cm.gray)

    def empty(self):
        return self.height <= 0

    def hasCorrectHeightRatio(self):
        return 0.10 <= self.height_ratio <= 0.30

class test_pageheader(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        if not self.debug:
            self.tearDownData()

    def checkPageHeader(self,expected_rows):
        page_header = PlotmsPageHeader(self.plotfile_png)
        # Empty or not
        if expected_rows == 0:
            err_msg = 'Page header: is unexpected, has a height of: '
            err_msg += '{:d} pixels'.format(page_header.height)
            self.assertTrue(page_header.empty(),err_msg)
            return
        else:
            err_msg = 'Page header: is missing'
            self.assertTrue(not page_header.empty(),err_msg)
        # Height ratio
        err_msg = 'Page header: has incorrect height ratio: '
        err_msg += '{:2.0f}% not in [10%,30%]'.format(100*page_header.height_ratio)
        self.assertTrue(page_header.hasCorrectHeightRatio(), err_msg)
        # Number of rows
        err_msg = 'Page header: has wrong number of rows: '
        err_msg += '{:d} rows detected, {:d} rows expected'.format(page_header.rows,expected_rows)
        self.assertTrue(page_header.rows == expected_rows, err_msg)

    def test_pageheader_none(self):
        '''test_pageheader_none: no page header if no header items'''
        self.plotfile_png = os.path.join(self.outputDir, "testPageHeader01.png")
        self.removePlotfile(self.plotfile_png)
        plotms(vis=self.ms, plotfile=self.plotfile_png,
            antenna='0&2',
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_png, 15000)
        self.checkPageHeader(expected_rows=0)

    def test_pageheader_items01(self):
        '''test_pageheader_items01: filename,telescope,projid,observer'''
        self.plotfile_png = os.path.join(self.outputDir, "testPageHeader02.png")
        self.removePlotfile(self.plotfile_png)
        plotms(vis=self.ms, plotfile=self.plotfile_png,
            antenna='0&2',headeritems='filename,telescope,projid,observer',
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_png, 20000)
        self.checkPageHeader(expected_rows=2)

    def test_pageheader_items02(self):
        '''test_pageheader_items02: targdir,telescope,targname,observer,ycolumn'''
        self.plotfile_png = os.path.join(self.outputDir, "testPageHeader03.png")
        self.removePlotfile(self.plotfile_png)
        plotms(vis=self.ms, plotfile=self.plotfile_png,
            antenna='0&2',headeritems='targdir,telescope,targname,observer,ycolumn',
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_png, 20000)
        self.checkPageHeader(expected_rows=3)

# ------------------------------------------------------------------------------

class test_display(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_display_symbol(self):
        '''test_display_symbol: Set a custom plotting symbol'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay01.jpg")
        self.removePlotfile()
        # Test diamond fill
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True,
            customsymbol=True, symbolshape='diamond', symbolsize=5,
            symbolcolor='00ff00', symbolfill='fill')
        self.checkPlotfile(self.plotfile_jpg, 180000)
        self.removePlotfile()
        # Test pixel shape
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True,
            customsymbol=True, symbolshape='pixel',
            symbolcolor='00aa00')
        self.checkPlotfile(self.plotfile_jpg, 190000)

    def test_display_flaggedsymbol(self):
        '''test_display_flaggedsymbol: Set a custom flagged symbol'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay02.jpg")
        self.removePlotfile()
        # Set customflaggedsymbol=True
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            customflaggedsymbol=True, flaggedsymbolshape='diamond',
            flaggedsymbolsize=5, flaggedsymbolcolor='00ff00',
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile()
        # Set customflaggedsymbol=False CAS-7046
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, showgui=False,
            customflaggedsymbol=False, flaggedsymbolshape='diamond',
            flaggedsymbolsize=5, flaggedsymbolcolor='00ff00',
            highres=True)
        self.checkPlotfile(self.plotfile_jpg, 40000)

    def test_display_legend(self):
        '''test_display_legend: Place a legend on a plot.'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay03.jpg")
        self.removePlotfile()
        # Place a legend in the upper right corner of the plot
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, showlegend=True, legendposition='upperRight',
            highres=True)
        self.checkPlotfile(self.plotfile_jpg, 50000)

    def test_display_legend_overplot(self):
        '''test_display_legend_overplot: Test that legend works with overplots'''
        # Must manually check plot and make sure there is a legend there.
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay04.jpg")
        self.removePlotfile()
        # First plot: scan vs time
        plotms(vis=self.ms, showgui=False, yaxis='scan', highres=True,
            plotindex=0, showlegend=True, legendposition='lowerRight',
            customsymbol=[True], symbolshape=['diamond'], symbolsize=[3],
            symbolcolor=['ff0000'], symbolfill=['mesh3'])
        # Overplot: field vs time.
        plotms(vis=self.ms, showgui=False, yaxis='field',
            plotindex=1, clearplots=False,
            showlegend=True, legendposition='lowerRight',
            customsymbol=[True], symbolshape=['circle'], symbolsize=[3],
            symbolcolor=['00FF00'], symbolfill=['mesh3'],
            plotfile=self.plotfile_jpg, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 45000)

    def test_display_coloraxis(self):
        '''test_display_coloraxis: Colorize plot by time and chan'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay05.jpg")
        self.removePlotfile()
        # Colorize by time
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, coloraxis='time', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 180000)
        self.removePlotfile()
        # Colorize by synonym, CAS-6921.
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, coloraxis='chan', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 180000)
        self.removePlotfile()
        # Colorize by averaged time
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, coloraxis='time', avgtime='3600', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 60000)

    def test_display_labels(self):
        '''test_display_labels: test custom title and axis labels'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay06.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True,
            title='NGC5921', xlabel='x axis', ylabel='y axis')
        self.checkPlotfile(self.plotfile_jpg, 190000)

    def test_display_gridlines(self):
        '''test_display_gridlines: show major and minor grids'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay07.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showmajorgrid=True, showminorgrid=True,
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 280000)

    def test_display_color_bins(self):
        '''test_display_colors: Colorize plot and check that colors vary'''
        plotfile_one_color = os.path.join(self.outputDir, "testDisplay08_one_color.jpg")
        plotfile_coloraxis = os.path.join(self.outputDir, "testDisplay08_coloraxis.jpg")
        self.removePlotfile(plotfile_one_color)
        self.removePlotfile(plotfile_coloraxis)

        # Plot channel selection in color0
        color0 = "#202020" # colors list in PlotMSConstants.cc
        plotms(vis=self.ms, spw='0:0;10;20', plotfile=plotfile_one_color,
            customsymbol=True, symbolcolor=color0, showgui=False, highres=True)
        self.checkPlotfile(plotfile_one_color, 85000)
        plotsize_one_color = os.path.getsize(plotfile_one_color)

        # Plot channel selection with coloraxis, should not be single color
        plotms(vis=self.ms, spw='0:0;10;20', plotfile=plotfile_coloraxis,
            coloraxis='channel', showgui=False, highres=True)
        self.checkPlotfile(plotfile_coloraxis, 95000)
        plotsize_coloraxis = os.path.getsize(plotfile_coloraxis)

        # Identical plot with coloraxis larger than single color
        self.assertGreater(plotsize_coloraxis, plotsize_one_color)

        self.removePlotfile(plotfile_one_color)
        self.removePlotfile(plotfile_coloraxis)

    def test_display_coloraxis_symbolfill(self):
        '''test_display_coloraxis_symbolfill: Colorize plot and change fill'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay09.jpg")
        self.removePlotfile()
        # Test circle mesh1
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, antenna='0~2',
            showgui=False, highres=True, coloraxis='antenna1',
            customsymbol=True, symbolshape='circle', symbolsize=5,
            symbolfill='mesh1')
        self.checkPlotfile(self.plotfile_jpg, 50000)
        self.removePlotfile()
        # Test circle mesh2
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, antenna='0~2',
            showgui=False, highres=True, coloraxis='antenna1',
            customsymbol=True, symbolshape='circle', symbolsize=5,
            symbolfill='mesh2')
        self.checkPlotfile(self.plotfile_jpg, 55000)
        self.removePlotfile()
        # Test circle mesh3
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, antenna='0~2',
            showgui=False, highres=True, coloraxis='antenna1',
            customsymbol=True, symbolshape='circle', symbolsize=5,
            symbolfill='mesh3')
        self.checkPlotfile(self.plotfile_jpg, 60000)
        self.removePlotfile()
        # Test circle no fill
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, antenna='0~2',
            showgui=False, highres=True, coloraxis='antenna1',
            customsymbol=True, symbolshape='circle', symbolsize=5,
            symbolfill='nofill')
        self.checkPlotfile(self.plotfile_jpg, 60000)

    def test_display_legend_coloraxis(self):
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay10.jpg")
        self.removePlotfile()
        # No legend
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, coloraxis='antenna1', showlegend=False)
        self.checkPlotfile(self.plotfile_jpg, 240000)
        self.removePlotfile()
        # Coloraxis legend
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, coloraxis='antenna1', showlegend=True)
        self.checkPlotfile(self.plotfile_jpg, 260000)

    def test_display_cal_coloraxis(self):
        '''test_display_cal_coloraxis: Colorize plot by cal axes'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testDisplay11.jpg")
        self.removePlotfile()
        # Colorize by poln - ignored
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, coloraxis='poln', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)
        self.removePlotfile()
        # Colorize by antpos - ignored
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, coloraxis='antpos', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)
        self.removePlotfile()

# ------------------------------------------------------------------------------

class test_grid(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_grid_location(self):
        '''test_grid_location: rowindex & colindex for plot location'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testGrid01.jpg")
        self.removePlotfile()
        # Grid with 2 rows and 3 columns; plot in second row, second col
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, gridrows=2, gridcols=3,
            rowindex=1, colindex=1, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 50000)

    def test_grid_fill(self):
        '''test_grid_fill: Set grid and fill each location with a plot'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testGrid02.jpg")
        self.removePlotfile()
        # Grid with 2 rows and 2 columns. Fill all the plots in the grid
        plotms(vis=self.ms, plotindex=0, title='Plot A',
            showgui=False, gridrows=2, gridcols=2,
            rowindex=0, colindex=0)
        plotms(vis=self.ms, plotindex=1, title='Plot B',
            showgui=False, clearplots=False,
            gridrows=2, gridcols=2,
            rowindex=0, colindex=1)
        plotms(vis=self.ms, plotindex=2, title='Plot C',
            showgui=False, clearplots=False,
            gridrows=2, gridcols=2,
            rowindex=1, colindex=0)
        plotms(vis=self.ms, plotindex=3, title='Plot D',
            showgui=False, clearplots=False,
            gridrows=2, gridcols=2,
            rowindex=1, colindex=1,
            plotfile=self.plotfile_jpg, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 200000)

    def test_grid_badindex(self):
        '''test_grid_badindex: use row/col index > gridrow/gridcol'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testGrid03.jpg")
        self.removePlotfile()
        with self.assertRaises(ValueError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg,
                showgui=False, highres=True,
                gridrows=2, gridcols=2, rowindex=2, colindex=2)

# ------------------------------------------------------------------------------

class test_iteration(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_iteration_scan(self):
        '''test_iteration_scan: Iterate by scan and export all'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration01.jpg")
        plotFiles = [os.path.join(self.outputDir, "testIteration01_Scan1.jpg"),
                     os.path.join(self.outputDir, "testIteration01_Scan2_2.jpg"),
                     os.path.join(self.outputDir, "testIteration01_Scan3_3.jpg"),
                     os.path.join(self.outputDir, "testIteration01_Scan4_4.jpg"),
                     os.path.join(self.outputDir, "testIteration01_Scan5_5.jpg"),
                     os.path.join(self.outputDir, "testIteration01_Scan6_6.jpg"),
                     os.path.join(self.outputDir, "testIteration01_Scan7_7.jpg")]
        for i in range(0, len(plotFiles)):
            self.removePlotfile(plotFiles[i])
        plotms(vis=self.ms, plotfile=plotfile_jpg,
            showgui=False, iteraxis='scan', exprange='all',
            highres=True)
        # Check each page got saved
        for  i in range(0, len(plotFiles)):
            self.checkPlotfile(plotFiles[i], 40000)
            self.removePlotfile(plotFiles[i])

    def test_iteration_antenna(self):
        '''test_iteration_antenna: Iterate by antenna and export all'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration02.jpg")
        # Create the plot and check that there are 27 iterations
        plotms(vis=self.ms, plotfile = plotfile_jpg, exprange='all',
            showgui=False, iteraxis='antenna', highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration02_")
        # no Antenna23
        self.assertEqual(fileCount, 27)
        self.removeFiles(self.outputDir, "testIteration02_")

    def test_iteration_time(self):
        '''test_iteration_time: Iterate over time'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration03.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration03_Time09:18:59.9998.jpg")
        self.removePlotfile(plotfile1_jpg)
        plotms(vis=self.ms, plotfile=plotfile_jpg, showgui=False,
            xaxis='elevation', yaxis='amp', iteraxis='time',
            highres=True)
        self.checkPlotfile(plotfile1_jpg, 30000)
        self.removePlotfile(plotfile1_jpg)

    def test_iteration_timeavg(self):
        '''test_iteration_time: Iterate over averaged time'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration04.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration04_Time09:19:15.0000.jpg")
        self.removePlotfile(plotfile1_jpg)
        plotms(vis=self.ms, plotfile=plotfile_jpg,
            showgui=False, xaxis='elevation', yaxis='amp', avgtime='60',
            iteraxis='time', highres=True)
        self.checkPlotfile(plotfile1_jpg, 40000)
        self.removePlotfile(plotfile1_jpg)

    def test_iteration_grid(self):
        '''test_iteration_grid: Iterate by scan on square grid.'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration05.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration05_Scan1,2,3,4.jpg")
        plotfile2_jpg = os.path.join(self.outputDir, "testIteration05_Scan5,6,7_2.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        # Make 2 pages of 2x2 iteration plots over scan
        plotms(vis=self.ms, plotfile=plotfile_jpg,
            showgui=False, iteraxis='scan', exprange='all',
            gridrows=2, gridcols=2, highres=True)
        self.checkPlotfile(plotfile1_jpg, 80000)
        self.removePlotfile(plotfile1_jpg)
        self.checkPlotfile(plotfile2_jpg, 55000)
        self.removePlotfile(plotfile2_jpg)

    def test_iteration_selection(self):
        '''test_iteration_selection: CAS-7050:(Pipeline) Iteration with selection'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration06.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration06_Antenna1@VLA:N7.jpg")
        plotfile2_jpg = os.path.join(self.outputDir, "testIteration06_Antenna2@VLA:W1_2.jpg")
        plotfile3_jpg = os.path.join(self.outputDir, "testIteration06_Antenna3@VLA:W2_3.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        self.removePlotfile(plotfile3_jpg)
        # Select 3, check that there are 3 iteration plots
        plotms(vis=self.ms, plotfile=plotfile_jpg, exprange='all',
            antenna='1~3&&&', iteraxis='antenna',
            showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration06_")
        self.assertEqual(fileCount, 3)
        self.checkPlotfile(plotfile1_jpg, 180000)
        self.removePlotfile(plotfile1_jpg)
        self.checkPlotfile(plotfile2_jpg, 170000)
        self.removePlotfile(plotfile2_jpg)
        self.checkPlotfile(plotfile3_jpg, 160000)
        self.removePlotfile(plotfile3_jpg)

    def test_iteration_select1(self):
        '''test_iteration_select1: CAS-7050 (Pipeline) Iteration with selection of 1'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration07.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration07_Antenna28@VLA:W7.jpg")
        self.removePlotfile(plotfile1_jpg)
        # Select all cross-correlation baselines which include antenna 28, 29, 30, 31
        # (only 28 is valid); results in all antennas
        plotms(vis=self.ms, plotfile = plotfile_jpg, exprange='all',
            antenna='28~31', iteraxis='antenna',
            showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration07_")
        self.assertEqual(fileCount, 27)
        self.removeFiles(self.outputDir, "testIteration07_" )
        # Select all cross- and auto-correlation baselines between antenna 28, 29, 30, 31
        # (only 28 is valid); results in one plot [28&&28].
        plotms(vis=self.ms, plotfile = plotfile_jpg, exprange='all',
            antenna='28~31&&', iteraxis='antenna',
            showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration07_")
        self.assertEqual(fileCount, 1)
        self.checkPlotfile(plotfile1_jpg, 60000)
        self.removePlotfile(plotfile1_jpg)

    def test_iteration_select0(self):
        '''test_iteration_select0: CAS-7050 (Pipeline) Iteration with empty selection'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testIteration08.jpg")
        self.removePlotfile()
        # Create plot with empty antenna selection and check exception
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, exprange='all',
                antenna="100,101,102", iteraxis='antenna',
                showgui=False, highres=True)

    def test_iteration_skipselection(self):
        '''test_iteration_skipselection:  CAS-7050:  (Pipeline) Iteration with skipped selection'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration09.jpg")
        plotFiles = [os.path.join(self.outputDir, "testIteration09_Scan1.jpg"),
            os.path.join(self.outputDir, "testIteration09_Scan2_2.jpg"),
            os.path.join(self.outputDir, "testIteration09_Scan3_3.jpg"),
            os.path.join(self.outputDir, "testIteration09_Scan4_4.jpg")]
        for plotfile in plotFiles:
            self.removePlotfile(plotfile)
        # The expectation is the plot with the bad scan 100 will be skipped
        plotms(vis=self.ms, plotfile = plotfile_jpg, exprange='all',
            scan="1,2,100,3,4", iteraxis='scan',
            showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration09_")
        self.assertEqual(fileCount, 4)
        for plotfile in plotFiles:
            self.checkPlotfile(plotfile, 40000)
            self.removePlotfile(plotfile)

    def test_iteration_corr(self):
        '''test_iteration_corr: Iterate by correlation and export all'''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration10.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration10_CorrRR.jpg")
        plotfile2_jpg = os.path.join(self.outputDir, "testIteration10_CorrLL_2.jpg")
        # Create the plot and check that there are 27 iterations
        plotms(vis=self.ms, plotfile=plotfile_jpg, exprange='all',
            iteraxis='corr', showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration10_")
        self.assertEqual(fileCount, 2)
        self.checkPlotfile(plotfile1_jpg, 220000)
        self.removePlotfile(plotfile1_jpg)
        self.checkPlotfile(plotfile2_jpg, 220000)
        self.removePlotfile(plotfile2_jpg)

    def test_iteration_two_yaxes(self):
        '''test_iteration_two_yaxes: Iterate by antenna with two yaxes'''
        # input plotfile name
        plotfile_jpg = os.path.join(self.outputDir, "testIteration11.jpg")
        # output plotfile name with iteraxis
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration11_Antenna1@VLA:N7.jpg")
        self.removePlotfile(plotfile_jpg)
        self.removePlotfile(plotfile1_jpg)
        plotms(vis=self.ms, plotfile=plotfile_jpg, highres=True,
            showgui=False, xaxis='time', yaxis=['scan','field'],
            yaxislocation=['left','right'], iteraxis="antenna",
            customsymbol=[True,True], symbolshape=['diamond','circle'],
            symbolsize=[5,5], symbolcolor=['ff0000','00ff00'],
            symbolfill=['mesh3','mesh3'])
        self.checkPlotfile(plotfile1_jpg, 50000)
        self.removePlotfile(plotfile1_jpg)

    def test_iteration_sharedaxis(self):
        '''test_iteration_sharedaxis: CAS-7074 sharedaxis on 2x2 grid'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testIteration12.jpg")
        self.removeFiles(self.outputDir, "testIteration12_" )
        # Create the plot and check that there are 19 iterations
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, exprange='all',
            scan='3', antenna='1~3', iteraxis='baseline',
            showgui=False, highres=True,
            gridrows=2, gridcols=2,
            xselfscale=True, yselfscale=True,
            xsharedaxis=True, ysharedaxis=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration12_")
        self.assertEqual(fileCount,19)
        self.removeFiles(self.outputDir, "testIteration12_" )

    def test_iteration_noselfscale(self):
        '''test_iteration_noselfscale: CAS-7074 xsharedaxis must have selfscale=True'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testIteration13.jpg")
        self.removePlotfile()
        # xsharedaxis without xselfscale!
        with self.assertRaises(ValueError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg,
                gridrows=2, gridcols=2, iteraxis='antenna',
                showgui=False, xsharedaxis=True, highres=True)

    def test_iteration_selection_avg(self):
        '''test_iteration_selection_avg: Iteration with negation selection and averaging'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testIteration14.jpg")
        # antenna "1" (name) is antenna ID 0
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration14_Antenna1@VLA:N7.jpg")
        self.removePlotfile(plotfile1_jpg)
        # Create plot with negative antenna selection and iteration
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            antenna="!1", iteraxis='antenna', showgui=False, highres=True)
        self.checkPlotfile(plotfile1_jpg, 170000)
        self.removePlotfile(plotfile1_jpg)
        # Create plot with negative antenna selection, iteration, and averaging
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, avgantenna=True,
            antenna="!1", iteraxis='antenna', showgui=False, highres=True)
        self.checkPlotfile(plotfile1_jpg, 180000)
        self.removePlotfile(plotfile1_jpg)

    def test_iteration_corr_wt(self):
        '''test_iteration_corr_wt: Iterate weight axis by correlation '''
        plotfile_jpg = os.path.join(self.outputDir, "testIteration15.jpg")
        plotfile1_jpg = os.path.join(self.outputDir, "testIteration15_CorrRR.jpg")
        # Create the plot and check min/max file size
        # CAS-14305: with invalid weight values was >70000
        plotms(vis=self.ms, plotfile = plotfile_jpg, yaxis='wt',
            iteraxis='corr', showgui=False, highres=True)
        self.checkPlotfile(plotfile1_jpg, 45000, 70000)
        self.removePlotfile(plotfile1_jpg)

    def test_iteration_cal_axes(self):
        '''test_iteration_cal_axes: Iterate by poln and antpos cal axes (ignored)'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testIteration15.jpg")
        # Create the plot and check that there is 1 exported plotfile (no iteration)
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, exprange='all',
            iteraxis='poln', showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration15")
        self.assertEqual(fileCount, 1)
        self.removePlotfile(self.plotfile_jpg)
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, exprange='all',
            iteraxis='antpos', showgui=False, highres=True)
        fileCount = self.getFilecount(self.outputDir, "testIteration15")
        self.assertEqual(fileCount, 1)
        self.removePlotfile(self.plotfile_jpg)

# ------------------------------------------------------------------------------

class test_multi(plotms_test_base):
    ''' tests of multiple plotms arguments '''

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_multi_cookbook(self):
        '''test_cookbook: Juergen's cookbook Plotting Multiple Data Sets example'''
        plotfile1_jpg = os.path.join(self.outputDir, "testMulti01.jpg")
        plotfile2_jpg = os.path.join(self.outputDir, "testMulti01_2.jpg")
        plotfile3_jpg = os.path.join(self.outputDir, "testMulti01_3.jpg")
        plotfile4_jpg = os.path.join(self.outputDir, "testMulti01_4.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        self.removePlotfile(plotfile3_jpg)
        self.removePlotfile(plotfile4_jpg)

        # Plot in the second column, first row, plotindex=0
        print('Test plot 1')
        plotms(vis=self.ms, gridrows=2, gridcols=2,
            rowindex=0, colindex=1, highres=True,
            showgui=False, plotfile=plotfile1_jpg,
            customsymbol=True, symbolshape='diamond', symbolsize=5,
            symbolcolor='ff0000')
        self.checkPlotfile(plotfile1_jpg, 50000)
        self.removePlotfile(plotfile1_jpg)
        # Overplot in the same panel, plotindex=1
        print('Test plot 2')
        plotms(vis=self.ms, plotfile=plotfile2_jpg,
            showgui=False, clearplots=False,
            plotindex=1, rowindex=0, colindex=1,
            gridrows=2, gridcols=2, yaxislocation='right',
            customsymbol=True, symbolshape='circle', symbolsize=5,
            symbolcolor='00ff00', highres=True)
        self.checkPlotfile(plotfile2_jpg, 50000)
        self.removePlotfile(plotfile2_jpg)
        # Define a second plot plotindex=2, in the lower right corner
        print('Test plot 3')
        plotms(vis=self.ms, clearplots=False,
            plotindex=2, rowindex=1, colindex=1,
            gridrows=2, gridcols=2,
            showgui=False, plotfile=plotfile3_jpg,
            customsymbol=False, yaxislocation='', highres=True)
        self.checkPlotfile(plotfile3_jpg, 50000)
        self.removePlotfile(plotfile3_jpg)
        # Move the plot with the overplot one panel to the left.
        print('Test plot 4')
        plotms(vis=self.ms, showgui=False,
            gridrows=2, gridcols=2,
            rowindex=0, colindex=0,
            customsymbol=True, symbolshape='diamond', symbolsize=5,
            symbolcolor='ff0000')
        plotms(vis=self.ms, showgui=False, clearplots=False,
            gridrows=2, gridcols=2,
            plotindex=1, rowindex=0, colindex=0,
            yaxislocation='right',
            customsymbol=True, symbolshape='circle', symbolsize=5,
            symbolcolor='00ff00')
        plotms(vis=self.ms, showgui=False, clearplots=False,
            gridrows=2, gridcols=2,
            plotindex=2, rowindex=1, colindex=1,
            plotfile=plotfile4_jpg, highres=True,
            customsymbol=False, yaxislocation='')
        self.checkPlotfile(plotfile4_jpg, 40000)
        self.removePlotfile(plotfile4_jpg)

    def test_multi_args(self):
        '''test_multi_args: CAS-6662 Pipeline no plot scenario with multiple arguments.'''
        plotfile1_jpg = os.path.join(self.outputDir, "testMulti02.jpg")
        plotfile2_jpg = os.path.join(self.outputDir, "testMulti02_2.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)

        plotms(vis=self.ms, showgui=False, plotfile=plotfile1_jpg,
            xaxis='uvdist', yaxis='amp',ydatacolumn='model',
            spw='0', scan='2,4,6,8', coloraxis='spw',
            highres=True)
        self.checkPlotfile(plotfile1_jpg, 20000)
        self.removePlotfile(plotfile1_jpg)

        plotms(vis=self.ms, ydatacolumn="corrected", field="1",
            scan="2,3", correlation="LL,RR", coloraxis="antenna2",
            avgtime="1e8", avgscan=True, veldef="RADIO",
            customsymbol=True, symbolshape="autoscaling", symbolsize=2,
            symbolcolor="0000ff",symbolfill="fill",symboloutline=False,
            customflaggedsymbol=False, flaggedsymboloutline=False,
            flaggedsymbolshape="nosymbol", flaggedsymbolsize=2,
            flaggedsymbolcolor="ff0000", flaggedsymbolfill="fill",
            plotfile=plotfile2_jpg, showgui=False, highres=True)
        self.checkPlotfile(plotfile2_jpg, 30000)
        self.removePlotfile(plotfile2_jpg)

# ------------------------------------------------------------------------------

class test_overlays(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()
        self.setUpOverlayData()

    def tearDown(self):
        self.tearDownData()

    def test_atm_overlays(self):
        '''test_atm_overlays: showatm and showtsky overplots'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testOverlay01.jpg")
        self.removePlotfile()
        # basic plot with showatm, xaxis chan
        plotms(vis=self.ms, xaxis='chan', plotfile=self.plotfile_jpg,
            showgui=False, highres=True, showatm=True)
        self.checkPlotfile(self.plotfile_jpg, 80000)
        self.removePlotfile()
        # basic plot with showtsky, xaxis freq
        self.plotfile_jpg = os.path.join(self.outputDir, "testOverlay02.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, xaxis='freq', plotfile=self.plotfile_jpg,
            showgui=False, highres=True, showtsky=True)
        self.checkPlotfile(self.plotfile_jpg, 80000)
        self.removePlotfile()
        # plotfile without overlay: xaxis must be chan or freq
        # so ignores showatm/tsky
        self.plotfile_jpg = os.path.join(self.outputDir, "testOverlay03.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, highres=True, showatm=True)
        self.checkPlotfile(self.plotfile_jpg, 170000)

    def test_image_overlay(self):
        '''test_image_overlay: atm and image overplots'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testOverlay04.jpg")
        self.removePlotfile()

        # showimage fails but should still plot showatm
        plotms(vis=self.ms, xaxis='chan', plotfile=self.plotfile_jpg,
            showgui=False, highres=True, showatm=True, showimage=True)
        self.checkPlotfile(self.plotfile_jpg, 80000)
        self.removePlotfile()

        # showimage succeeds
        self.plotfile_jpg = os.path.join(self.outputDir, "testOverlay05.jpg")
        self.removePlotfile()
        plotms(vis=self.ms4, xaxis='freq', spw="58", plotfile=self.plotfile_jpg,
            showgui=False, highres=True, showatm=True, showimage=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)

# ------------------------------------------------------------------------------

class test_selection(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_selection_scan(self):
        '''test_selection_scan: Check scan invalid/valid selections'''
        # Will check max size to ensure selection was done
        plotfile1_jpg = os.path.join(self.outputDir, "testSelection01_scan1.jpg") #FAIL
        plotfile2_jpg = os.path.join(self.outputDir, "testSelection01_scan2.jpg")
        plotfile3_jpg = os.path.join(self.outputDir, "testSelection01_scan3.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        self.removePlotfile(plotfile3_jpg)
        # Fail with invalid scan
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=plotfile1_jpg,
                showgui=False, scan='8', highres=True)
        # Succeed with valid scans
        plotms(vis=self.ms, plotfile=plotfile2_jpg,
            overwrite=True, showgui=False, scan='2,4', highres=True)
        self.checkPlotfile(plotfile2_jpg, 60000, 130000)
        self.removePlotfile(plotfile2_jpg)
        # Succeed with different scan selection (CAS-6813)
        plotms(vis=self.ms, plotfile=plotfile3_jpg,
            overwrite=True, showgui=False, scan='5,7', highres=True)
        self.checkPlotfile(plotfile3_jpg, 60000, 130000)
        self.removePlotfile(plotfile3_jpg)

    def test_selection_spw(self):
        '''test_selection_spw: Check spw invalid/valid selections'''
        # Will check max size to ensure selection was done
        plotfile1_jpg = os.path.join(self.outputDir, "testSelection02_spw500.jpg") # EXPECT FAIL
        plotfile2_jpg = os.path.join(self.outputDir, "testSelection02_spw0.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        # Fail with invalid spw
        print("invalid spw test fails")
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=plotfile1_jpg,
                showgui=False, spw='500', highres=True)
        # Succeed with valid spw - no channel selection
        plotms(vis=self.ms, plotfile=plotfile2_jpg, xaxis='channel',
            showgui=False, spw='0', highres=True)
        self.checkPlotfile(plotfile2_jpg, 90000, 130000)
        self.removePlotfile(plotfile2_jpg)
        # Test channel selection
        plotms(vis=self.ms, plotfile=plotfile2_jpg, xaxis='channel',
            showgui=False, spw='0:10~20', highres=True)
        self.checkPlotfile(plotfile2_jpg, 45000, 60000)
        self.removePlotfile(plotfile2_jpg)
        # Test channel selection with step CAS-13641
        plotms(vis=self.ms, plotfile=plotfile2_jpg, xaxis='channel',
            showgui=False, spw='0:10~20^2', highres=True)
        self.checkPlotfile(plotfile2_jpg, 35000, 60000)
        self.removePlotfile(plotfile2_jpg)


    def test_selection_ant(self):
        '''test_selection_ant: Check antenna invalid/valid selections'''
        plotfile1_jpg = os.path.join(self.outputDir, "testSelection03_ant1.jpg") #FAIL
        plotfile2_jpg = os.path.join(self.outputDir, "testSelection03_ant2.jpg")
        plotfile3_jpg = os.path.join(self.outputDir, "testSelection03_ant3.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        self.removePlotfile(plotfile3_jpg)
        # Fail with invalid antenna
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=plotfile1_jpg,
                showgui=False, selectdata=True, antenna='ea22&&*',
                highres=True)
        # Succeed without antenna selection (make sure it cleared)
        plotms(vis=self.ms, plotfile=plotfile2_jpg,
            showgui=False, highres=True)
        self.checkPlotfile(plotfile2_jpg, 190000, 340000)
        self.removePlotfile(plotfile2_jpg)
        # Succeed with valid antenna
        plotms(vis=self.ms, plotfile=plotfile3_jpg,
            showgui=False, antenna='0~1', highres=True)
        self.checkPlotfile(plotfile3_jpg, 50000, 110000)
        self.removePlotfile(plotfile3_jpg)

    def test_selection_field(self):
        '''test_selection_field: Check field invalid/valid selections'''
        # Will check max size to ensure selection was done
        plotfile1_jpg = os.path.join(self.outputDir, "testSelection04_field1.jpg") #FAIL
        plotfile2_jpg = os.path.join(self.outputDir, "testSelection04_field2.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        # Fail with invalid field
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=plotfile1_jpg,
                showgui=False, field='3', highres=True)
        # Succeed with valid field
        plotms(vis=self.ms, plotfile=plotfile2_jpg,
            showgui=False, field='1', highres=True)
        self.checkPlotfile(plotfile2_jpg, 70000, 160000)
        self.removePlotfile(plotfile2_jpg)

    def test_selection_corr(self):
        '''test_selection_corr: Check corr invalid/valid selections'''
        # Will check max size to ensure selection was done
        plotfile1_jpg = os.path.join(self.outputDir, "testSelection05_corr1.jpg") #FAIL
        plotfile2_jpg = os.path.join(self.outputDir, "testSelection05_corr2.jpg")
        self.removePlotfile(plotfile1_jpg)
        self.removePlotfile(plotfile2_jpg)
        # Succeed with valid corr
        plotms(vis=self.ms, plotfile=plotfile1_jpg,
            showgui=False, correlation='RR', highres=True)
        self.checkPlotfile(plotfile1_jpg, 190000, 380000)
        self.removePlotfile(plotfile1_jpg)
        # Succeed with corr conversion
        plotms(vis=self.ms, plotfile=plotfile2_jpg,
            showgui=False, correlation='XX', highres=True)
        self.checkPlotfile(plotfile2_jpg, 190000, 380000)
        self.removePlotfile(plotfile2_jpg)

    def test_selection_stokes(self):
        '''test_selection_stokes: Check corr selection of computed stokes'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testSelection06.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, correlation='I,Q,U,V', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, correlation='I,Q,U,V', highres=True,
            iteraxis='corr', exprange='all')
        fileCount = self.getFilecount(self.outputDir, "testSelection06_")
        self.assertEqual(fileCount, 4)
        self.removeFiles(self.outputDir, "testSelection06_")

    # Test MS has no STATE table for intent selection test

    def test_selection_poln(self):
        '''test_selection_poln: Check polarization selection of poln and corr'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testSelection07.jpg")
        self.removePlotfile()
        # polarization selection fails
        with self.assertRaises(Exception):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg,
                showgui=False, polarization='R', highres=True)
        # polarization selection of correlation succeeds
        # (same selection as correlation='RR' in code)
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, polarization='RR', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)
        self.removePlotfile(self.plotfile_jpg)

    def test_selection_antpos(self):
        '''test_selection_antpos: Check antpos selection is ignored'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testSelection08.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg,
            showgui=False, antpos='X', highres=True)
        self.checkPlotfile(self.plotfile_jpg, 190000)
        self.removePlotfile(self.plotfile_jpg)

# ------------------------------------------------------------------------------

class test_transform(plotms_test_base):

    def setUp(self):
        self.checkDisplay()
        self.setUpData()

    def tearDown(self):
        self.tearDownData()

    def test_transform_freqframe(self):
        '''test_transform_freqframe: Test frequency frames'''
        frames = ['LSRK', 'LSRD', 'BARY', 'GEO', 'TOPO',
                  'GALACTO', 'LGROUP', 'CMB']
        for frame in frames:
            filename = "testTransform01_" + frame + ".jpg"
            plotfile = os.path.join(self.outputDir, filename)
            self.removePlotfile(plotfile)
            plotms(vis=self.ms, plotfile=plotfile, yaxis='freq',
                freqframe=frame, showgui=False, highres=True)
            self.checkPlotfile(plotfile, 180000)
            self.removePlotfile(plotfile)

    def test_transform_badframe(self):
        '''test_transform_badframe: Test that invalid freqframe fails.'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testTransform02.jpg")
        self.removePlotfile()
        with self.assertRaises(AssertionError):
            plotms(vis=self.ms, plotfile=self.plotfile_jpg, yaxis='freq',
                freqframe='J2000', showgui=False, highres=True)

    def test_transform_veldef(self):
        '''test_transform_veldef: Test velocity definitions'''
        vels = ['RADIO', 'OPTICAL', 'TRUE']
        for vel in vels:
            filename = "testTransform03_" + vel + ".jpg"
            plotfile = os.path.join(self.outputDir, filename)
            self.removePlotfile(plotfile)
            plotms(vis=self.ms, plotfile=plotfile, yaxis='freq',
                veldef=vel, showgui=False, highres=True)
            self.checkPlotfile(plotfile, 260000)
            self.removePlotfile(plotfile)

    def test_transform_restfreq(self):
        '''test_transform_restfreq: Test rest frequency'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testTransform04.jpg")
        self.removePlotfile()
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, yaxis='freq',
            restfreq='1420', showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 260000)

    def test_transform_phasecenter(self):
        '''test_transform_phasecenter: Test phase center'''
        self.plotfile_jpg = os.path.join(self.outputDir, "testTransform05.jpg")
        self.removePlotfile()
        # Plot vs uvdist
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, yaxis='phase', xaxis='uvdist',
            spw='0:10~50', antenna='1&2', field='1', correlation='RR',
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 80000)
        self.removePlotfile()
        # Phase center
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, yaxis='phase', xaxis='uvdist',
            spw='0:10~50', antenna='1&2', field='1', correlation='RR',
            phasecenter='J2000 14:45:16 10.30.00', showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 50000)
        self.removePlotfile()
        # Phase center with time average
        plotms(vis=self.ms, plotfile=self.plotfile_jpg, yaxis='phase', xaxis='time',
            spw='0:10~50', antenna='1&2', field='1', correlation='RR',
            phasecenter='J2000 14:45:16 10.30.00', avgtime='1800', avgscan=True,
            showgui=False, highres=True)
        self.checkPlotfile(self.plotfile_jpg, 40000)
        self.removePlotfile()


class test_export_text(plotms_test_base):
    """Test precision of text export functionality."""

    plotfile_txt = 'test_export_text_precision.txt'

    def setUp(self) -> None:
        self.checkDisplay()
        self.setUpData()
        self.setUpPointingData()
        self.removePlotfile(self.plotfile_txt)

    def tearDown(self) -> None:
        self.tearDownData()
        self.removePlotfile(self.plotfile_txt)

    def __count_digits_fixed(self, number: str) -> int:
        """Count number of digits after decimal point.

        This method counts number of digits after decimal point.
        For example, this returns 3 when `number` is '21.875'.
        If `number` is an integer, this returns 0.

        Scientific notation is supported. In that case, only
        significand part of the `number` is counted.

        Parameters
        ----------
        number
            Floating point number or integer as a string

        Returns
        -------
        Number of digits after decimal point

        Examples
        --------
        This method returns 3 when `number` is '21.875'.

        >>> from test_task_plotms import test_export_text
        >>> tester = test_export_text()
        >>> tester._test_export_text__count_digits_fixed('21.875')
        3

        If `number` is an integer, this returns 0.

        >>> tester._test_export_text__count_digits_fixed('21875')
        0

        Scientific notation is supported. In that case, only
        significand part of the `number` is counted.

        >>> tester._test_export_text__count_digits_fixed('3.4e-5')
        1
        """
        epos = number.lower().find('e')
        significand = number[:epos] if epos > 0 else number
        abs_number = significand.lstrip('-')
        pos = abs_number.find('.')
        if pos < 0:
            return 0
        else:
            return len(abs_number) - 1 - pos

    def __count_digits(self, number: str) -> int:
        """Count number of digits.

        This method simply counts number of digits.
        For example, this returns 5 when `number` is '21.875'.
        If `number` is '21875', this also returns 5.

        Scientific notation is supported. In that case, only
        significand part of the `number` is counted.

        Parameters
        ----------
        number
            Floating point number or integer as a string

        Returns
        -------
        Number of digits

        Examples
        --------
        This method returns 5 when `number` is '21.875'.

        >>> from test_task_plotms import test_export_text
        >>> tester = test_export_text()
        >>> tester._test_export_text__count_digits('21.875')
        5

        If `number` is an integer, this returns number of digits.

        >>> tester._test_export_text__count_digits('21875')
        5

        Scientific notation is supported. In that case, only
        significand part of the `number` is counted.

        >>> tester._test_export_text__count_digits('3.4e-5')
        2
        """
        epos = number.lower().find('e')
        significand = number[:epos] if epos > 0 else number
        abs_number = significand.lstrip('-')
        return len(abs_number.replace('.', ''))

    def check_precision(self, column: int, precision: int, fixed: bool) -> None:
        """Check precision of the exported text file.

        This method counts number of digits in the text data loaded
        from the text file exported by plotms. The data is supposed
        to be tabulated. Only data in the column specified by
        `column` is examined. The method compares maximum number of
        digits in the data with user-supplied `precision` and returns
        whether or not they are equal. Boolean parameter `fixed`
        controls the range of count. If `fixed` is `True`, the method
        counts number of digits after decimal point. Otherwise,
        the method counts whole digits. For scientific notation,
        only significand part is examined.

        Parameters
        ----------
        column
            Column identifier (0-based)
        precision
            Required precision
        fixed
            Set True to check precisition after decimal point.
        """
        data = np.loadtxt(self.plotfile_txt, dtype=str)
        if fixed:
            digits_list = map(self.__count_digits_fixed, data[:, column])
        else:
            digits_list = map(self.__count_digits, data[:, column])
        max_digits = max(digits_list)
        print(f'max digits: {max_digits} (required precision {precision})')
        self.assertEqual(max_digits, precision)

    def test_precision_velocity(self):
        """Test precision for velocity."""
        plotms(vis=self.ms, plotfile=self.plotfile_txt,
            xaxis='velocity', yaxis='real',
            antenna='0&&&', field='1', showgui=False, expformat='txt', verbose=False)
        self.check_precision(column=0, precision=9, fixed=False)

    def test_precision_az_el(self):
        """Test precision for azimuth and elevation."""
        plotms(vis=self.ms, plotfile=self.plotfile_txt,
            xaxis='elevation', yaxis='azimuth',
            antenna='0&&&', field='1', showgui=False, expformat='txt', verbose=False)
        self.check_precision(column=0, precision=5, fixed=True)
        self.check_precision(column=1, precision=5, fixed=True)

    def test_precision_antaz_antel(self):
        """Test precision for ant-azimuth and ant-elevation."""
        plotms(vis=self.ms, plotfile=self.plotfile_txt,
            xaxis='ant-azimuth', yaxis='ant-elevation',
            antenna='0&&&', field='1', showgui=False, expformat='txt', verbose=False)
        self.check_precision(column=0, precision=5, fixed=True)
        self.check_precision(column=1, precision=5, fixed=True)

    def test_precision_antra_antdec(self):
        """Test precision for ant-ra and ant-dec."""
        plotms(vis=self.ms3, plotfile=self.plotfile_txt,
            xaxis='ant-ra', yaxis='ant-dec',
            antenna='0&&&', field='0', showgui=False, expformat='txt', verbose=False)
        self.check_precision(column=0, precision=5, fixed=True)
        self.check_precision(column=1, precision=5, fixed=True)

    def test_precision_time_antel(self):
        """Test precision for time and ant-elevation."""
        plotms(vis=self.ms, plotfile=self.plotfile_txt,
            xaxis='time', yaxis='ant-elevation',
            antenna='0&&&', field='1', showgui=False, expformat='txt', verbose=False)
        self.check_precision(column=1, precision=5, fixed=True)


if __name__ == '__main__':
    #print(pm.stub( ))
    unittest.main()
