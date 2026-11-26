# SPDX-License-Identifier: LGPL-3.0-or-later
# Ported from CASA 6.7.6.14 casatools/tests/tools/componentlist/test_tool_componentlist.py.
##########################################################################
# test_tool_componentlist.py
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
# https://casadocs.readthedocs.io/en/latest/api/tt/casatools.componentlist.html
#
#
##########################################################################

import os, shutil, unittest

from casars.tools import componentlist as cltool

import pytest


@pytest.fixture(autouse=True)
def isolated_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


class componentlist_test(unittest.TestCase):

    tablename = 'my.cl'

    def exception_check(self, func, method_parms, expected_msg, exc=RuntimeError ):
        with self.assertRaises(exc) as cm: 
            res = func(**method_parms)
        got_exception = cm.exception
        if type(expected_msg) == list:
            pos = max( [ str(got_exception).find(msg) for msg in expected_msg ] )
        else:
            pos = str(got_exception).find(expected_msg)
        self.assertNotEqual(
            pos, -1, msg=f'Unexpected exception was thrown: {got_exception} ({pos} != -1)'
        )


    def setUp(self):
        self.cleanup()

    def tearDown(self):
        self.cleanup()

    def cleanup(self):
        if os.path.exists(self.tablename):
            shutil.rmtree(self.tablename)
 
    def test_summarize(self):
        """Test the cl.summarize() method"""
        # make me a list
        mycl = cltool()
        mycl.addcomponent(
            [1,0,0,0],'Jy','Stokes',['J2000', '10:30:00.00', '-20.00.00.0'],
            'gaussian','4arcsec','2arcsec','30deg'
        )
        self.assertTrue(mycl.summarize(0))
        self.assertRaises(Exception, mycl.summarize, which=1)

    def test_getfluxerror(self):
        """Test cl.getfluxerror()"""
        mycl = cltool()
        mycl.addcomponent(
            [1,0,0,0],'Jy','Stokes',['J2000', '10:30:00.00', '-20.00.00.0'],
            'gaussian','4arcsec','2arcsec','30deg'
        )
        ferror = [0.2, 0.3, 0, 0]
        mycl.setflux(0, value=mycl.getfluxvalue(0), error=ferror)
        got = mycl.getfluxerror(0)
        self.assertTrue((got == ferror).all())

    def test_plp(self):
        """Test adding/updating plp spectral component"""
        coeffs = [1, 2, 3]
        mycl = cltool()
        mycl.addcomponent(
            [1,0,0,0],'Jy','Stokes', ['J2000', '10:30:00.00', '-20.00.00.0'],
            'gaussian','4arcsec','2arcsec','30deg', spectrumtype='plp',
            index=coeffs
        )
        x = mycl.getspectrum(0)
        mycl.close()
        self.assertTrue(
            (x['coeffs'] == coeffs).all(), "Incorrect coefficients found"
        )
        self.assertTrue(
            x['type'] == 'Power Logarithmic Polynomial',
            'Incorrect spectral type'
        )
        mycl.addcomponent(
            [1,0,0,0],'Jy','Stokes', ['J2000', '10:30:00.00', '-20.00.00.0'],
           'gaussian','4arcsec','2arcsec','30deg'
        )
        self.assertTrue(
            mycl.getspectrum(0)['type'] == 'Constant', 'Incorrect spectral type'
        )
        mycl.setspectrum(0, type='plp', index=coeffs)
        mytype = mycl.spectrumtype(0)
        x = mycl.getspectrum(0)
        mycl.close()
        self.assertTrue(
            (x['coeffs'] == coeffs).all(), "Incorrect coefficients found"
        )
        self.assertTrue(
            mytype == 'Power Logarithmic Polynomial',
            'Incorrect spectral type'
        )
        self.assertTrue(
            x['type'] == 'Power Logarithmic Polynomial',
            'Incorrect spectral type'
        )


    def test_table_keyword_interface(self):
        """Test putting, getting, querying table keywords"""
        cl = cltool()
        msg = 'A table is not attached to this ComponentList'
        self.exception_check(
            cl.putkeyword, {'keyword': 'metadata', 'value': 'x'}, msg, exc=RuntimeError
        )
        self.exception_check(
            cl.getkeyword, {'keyword': 'metadata'}, msg, exc=RuntimeError
        )
        self.exception_check(
            cl.haskeyword, {'keyword': 'metadata'}, msg, exc=RuntimeError
        )
        cl.addcomponent(
            [1,0,0,0],'Jy','Stokes',['J2000', '10:30:00.00', '-20.00.00.0'],
            'gaussian','4arcsec','2arcsec','30deg'
        )
        self.exception_check(
            cl.putkeyword, {'keyword': 'metadata', 'value': 'x'}, msg, exc=RuntimeError
        )
        self.exception_check(
            cl.getkeyword, {'keyword': 'metadata'}, msg, exc=RuntimeError
        )
        self.exception_check(
            cl.haskeyword, {'keyword': 'metadata'}, msg, exc=RuntimeError
        )
        cl.rename(self.tablename)
        self.assertFalse(cl.haskeyword('metadata'), 'false not returned')
        # This causes a core dump in the test only, running it interactively
        # does not produce a core dump and it behaves as expected. Do not
        # test here.
        # self.exception_check(
        #    cl.getkeyword, {'keyword': 'metadata'}, msg, exc=RuntimeError
        # )
        cl.putkeyword('metadata', 'myval')
        self.assertTrue(cl.haskeyword('metadata'), 'cl does not have expected keyword')
        self.assertEqual(cl.getkeyword('metadata'), 'myval', 'cl keyword value unexpected')
        cl.done()


if __name__ == '__main__':
    unittest.main()
