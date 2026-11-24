###########################################################################
# test_task_plotweather.py
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
#
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.visualization.plotweather.html
#
##########################################################################

"""
Unit tests for task plotweather.

This module contains comprehensive unit tests for the plotweather task,
validating its core functionality and parameter handling.

Test Coverage:
    vis: Validates correct and incorrect file paths
    seasonal_weight: Tests default value (0.5) and custom values
    doPlot: Tests default behavior (True) and disabled plotting (False)
    plotName: Verifies output file creation and various format support
    return value: Validates opacity list return type

Test Cases:
    test0: Verifies AssertionError raised when required parameters missing
    test1: Verifies AssertionError raised for invalid input file
    test2: Tests MS file without WEATHER table and no plot output
    test3: Tests valid MS file with plot output generation
    test4: Tests valid MS file with plot generation disabled
    test5: Tests seasonal_weight parameter with custom value
    test6: Tests PDF output format generation
    test7: Tests PostScript output format generation
    test8: Tests Encapsulated PostScript output format generation
    test9: Tests SVG output format generation

References:
    https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.visualization.plotweather.html
"""

import os
import shutil
import unittest

from casatasks import plotweather
from casatools import ctsys

class plotweather_test(unittest.TestCase):

    # Input MS, must have WEATHER table
    msfile = 'nep2-shrunk.ms'
    msNoWeatherfile = 'ngc5921_ut.ms'
    # output plots
    fig = '/tmp/plotweathertest.png'
    defaultFig = msfile + ".plotweather.png"

    def setUp(self):
        if (os.path.exists(self.msfile)):
            shutil.rmtree(self.msfile)
        shutil.copytree(ctsys.resolve(os.path.join("unittest/plotweather",self.msfile)), self.msfile)

    def tearDown(self):
        if (os.path.exists(self.msfile)):
            shutil.rmtree(self.msfile)
        if (os.path.exists(self.fig)):
            os.remove(self.fig)
        if (os.path.exists(self.defaultFig)):
            os.remove(self.defaultFig)

    def test0(self):
        '''Test 0: Default parameters'''
        with self.assertRaises(AssertionError):
            plotweather()

    def test1(self):
        '''Test 1: Bad input file'''
        badmsfile = 'badfile.ms'
        with self.assertRaises(AssertionError):
            plotweather(vis=badmsfile)

#    @unittest.skipIf("failure, 0.005426051322080905 != 0.0054234724819465846 within 7 places")
    def test2(self):
        '''Test 2: ms with no weather, no plot '''
        if (os.path.exists(self.msNoWeatherfile)):
            shutil.rmtree(self.msNoWeatherfile)
        shutil.copytree(ctsys.resolve(os.path.join("unittest/plotweather",self.msNoWeatherfile)), self.msNoWeatherfile)

        opac = plotweather(vis=self.msNoWeatherfile, plotName=self.fig)
        self.assertIsNotNone(opac)
        self.assertAlmostEqual(opac[0], 0.005400654844564904, places=5)
        self.assertFalse(os.path.exists(self.fig))
        if (os.path.exists(self.msNoWeatherfile)):
            shutil.rmtree(self.msNoWeatherfile)

#    @unittest.skipIf("failure, 1.3931958371884026 != 1.3867727940788754 within 7 places")
    def test3(self):
        '''Test 3: Good input file and output exists'''
        res = plotweather(vis=self.msfile, plotName=self.fig)
        self.assertIsNotNone(res)
        opac = res[0]/1e55
        self.assertAlmostEqual(opac, 1.3747135081808317, places=5)
        self.assertTrue(os.path.exists(self.fig))

#    @unittest.skipIf("failure, 1.3931958371884026 != 1.3867727940788754 within 7 places")
    def test4(self):
        '''Test 4: Good input file and no output plot exists'''
        res = plotweather(vis=self.msfile, doPlot=False)
        self.assertIsNotNone(res)
        opac = res[0]/1e55
        self.assertAlmostEqual(opac, 1.3747135081808317, places=5)
        defaultFig = self.msfile + ".plotweather.png"
        self.assertFalse(os.path.exists(defaultFig))

#    @unittest.skipIf("failure, 6.965979185942013 != 6.933863970394376 within 7 places")
    def test5(self):
        '''Test 5: seasonal_weight'''
        res = plotweather(vis=self.msfile, seasonal_weight=0.75, plotName=self.fig)
        self.assertIsNotNone(res)
        opac = res[0]/1e54
        self.assertAlmostEqual(opac, 6.873567540904158, places=5)
        self.assertTrue(os.path.exists(self.fig))

    def test6(self):
        '''Test 6: pdf output format'''
        plot = '/tmp/plotweathertest.pdf'
        opac = plotweather(vis=self.msfile, plotName=plot)
        self.assertTrue(os.path.exists(plot))
        os.remove(plot)

    def test7(self):
        '''Test 7: ps output format'''
        plot = '/tmp/plotweathertest.ps'
        opac = plotweather(vis=self.msfile, plotName=plot)
        self.assertTrue(os.path.exists(plot))
        os.remove(plot)

    def test8(self):
        '''Test 8: eps output format'''
        plot = '/tmp/plotweathertest.eps'
        opac = plotweather(vis=self.msfile, plotName=plot)
        self.assertTrue(os.path.exists(plot))
        os.remove(plot)

    def test9(self):
        '''Test 9: svg output format'''
        plot = '/tmp/plotweathertest.svg'
        opac = plotweather(vis=self.msfile, plotName=plot)
        self.assertTrue(os.path.exists(plot))
        os.remove(plot)

if __name__ == '__main__':
    unittest.main()
