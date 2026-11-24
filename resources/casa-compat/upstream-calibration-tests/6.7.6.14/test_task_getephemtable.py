#########################################################################
# test_task_getephemtable.py
# Copyright (C) 2023
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.data.getephemtable.html
#
##########################################################################
import contextlib
import csv
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock
from unittest.mock import patch
from urllib.error import URLError, HTTPError
import certifi
import pytest
import numpy as np

from casatestutils import testhelper as th

from casatasks import getephemtable
from casatasks.private import jplhorizons_query
from casatools import ctsys, table, measures

_me = measures()
_tb = table()

datapath = ctsys.resolve("unittest/getephemtable/")
# datapath = '/export/home/murasame/casamodular/cas13705-2/test/utest/'
hostname = "https://ssd.jpl.nasa.gov/api/horizons.api"


def isDatabaseURLunreachable(hostname):
    from urllib.request import urlopen
    from urllib.error import URLError
    import certifi
    import ssl

    context = ssl.create_default_context(cafile=certifi.where())
    try:
        urlaccess = urlopen(hostname, context=context, timeout=60.0)
        print("urlaccess=", urlaccess.getcode())
        if urlaccess.getcode() == 200:
            print("code 200")
            return False
        else:
            print("not 200")
            return True
    except Exception:
        return True


class getephemtable_test(unittest.TestCase):
    def setUp(self):
        # self.hostname = 'https://ssd.jpl.nasa.gov/api/horizons.api'
        self.outfile = "testephem.tab"
        self.caltimerange = "2023/09/01/20:00~2023/09/04/20:00"
        self.jdtimerange = "JD2460189.33333~2460189.88542"
        self.mjdtimerange = "MJD60188.83333~60189.38542"
        self.reftable = datapath + "titan_jplhorizons_eph_ref.tab"
        # tmppath = '/Users/ttsutsum/SWDevel/casa/imaging/ephemimaging/getephemtable-test/'
        self.inALMAtextfile = datapath + "titan_jplhorizons_eph_alma.txt"
        self.inVLAtextfile = datapath + "titan_jplhorizons_eph_vla.txt"
        self.inGBTtextfile = datapath + "titan_jplhorizons_eph_gbt.txt"
        self.otheroutputs = [
            "saved_rawqueryresult.txt",
            "titan_eph_from_ALMAtextdata.tab",
            "titan_eph_from_VLAtextdata.tab",
            "titan_eph_from_GBTtextdata.tab",
            "titan_eph_from_textdata.tab",
        ]

    def tearDown(self):
        if os.path.exists(self.outfile):
            shutil.rmtree(self.outfile)
        for output in self.otheroutputs:
            if os.path.exists(output):
                if os.path.isfile(output):
                    os.remove(output)
                else:
                    shutil.rmtree(output)

    def checkEphemTableContent(self, intab, reftab):
        retval = True
        try:
            _tb.open(intab)
            intabcols = _tb.colnames()
            intabnrows = _tb.nrows()
            intabkeywds = _tb.getkeywords()
            _tb.close()
            _tb.open(reftab)
            reftabcols = _tb.colnames()
            reftabnrows = _tb.nrows()
            reftabkeywds = _tb.getkeywords()
            _tb.close()
            if intabnrows != reftabnrows:
                print(
                    f"Nrows of {intab} differs from that of {reftab}: {intabnrows} != {reftabnrows}"
                )
                retval = False
            missingcols = []
            for refc in reftabcols:
                if refc not in intabcols:
                    missingcols.append(refc)
            if missingcols != []:
                print(f"Missing column(s) in {intab}: {missingcols}")
                retval = False
            missingkeys = []
            for refkey in reftabkeywds.keys():
                if refkey not in intabkeywds:
                    missingkeys.append(refkey)
                elif refkey == "posrefsys":
                    if intabkeywds[refkey] != "ICRS":
                        print(
                            f"Wrong posrefsys label {intabkeywds[refky]} is detected."
                        )
                        retval = False
            if missingkeys:
                print(f"Missing keyword(s) in {intab}: {missingkeys}")
                retval = False
            for inkey in intabkeywds.keys():
                if inkey not in reftabkeywds:
                    print(f"{inkey} is not in reference table keywords")
        except Exception:
            print(f"Error occurred in checking content of {intab}")
            retval = False

        return retval

    def test_invalid_inputs(self):
        """Test task inputs"""
        with self.assertRaisesRegex(ValueError, r"objectname must be specified"):
            getephemtable()

        with self.assertRaisesRegex(ValueError, r"timerange must be specified"):
            getephemtable(objectname="Titan")

        with self.assertRaisesRegex(ValueError, r"outfile must be specified"):
            getephemtable(objectname="Titan", timerange=self.caltimerange)

        with self.assertRaisesRegex(RuntimeError, r"objectname is given as an ID"):
            getephemtable(
                objectname="606",
                timerange=self.caltimerange,
                outfile=self.outfile,
                overwrite=True,
            )

        with self.assertRaisesRegex(
            ValueError, r"timerange needs to be specified with"
        ):
            getephemtable(
                objectname="Titan",
                timerange="2023/09/01/20:00  2023/09/04/20:00",
                outfile=self.outfile,
                overwrite=True,
            )

        with self.assertRaisesRegex(
            ValueError, r"Error translating stop time of timerange specified in JD"
        ):
            getephemtable(
                objectname="Titan",
                timerange="JD 2460189.8~ 2023/09/04/00",
                outfile=self.outfile,
                overwrite=True,
            )

        with self.assertRaisesRegex(
            ValueError, r"Error translating stop time of timerange specified in MJD"
        ):
            getephemtable(
                objectname="Titan",
                timerange="MJD 60189.3~ 2023/09/04/00",
                outfile=self.outfile,
                overwrite=True,
            )

        with self.assertRaisesRegex(ValueError, r"Error in timerange format"):
            getephemtable(
                objectname="Titan",
                timerange="09-01-2023 20:00~ 09-02-2023 09:20",
                outfile=self.outfile,
                overwrite=True,
            )

        with self.assertRaisesRegex(ValueError, r"interval value must be integer"):
            getephemtable(
                objectname="Titan",
                timerange=self.mjdtimerange,
                interval="15.0",
                outfile=self.outfile,
                overwrite=True,
            )

        # check for overwrite parameter (2nd task call should trigger an exception)
        with self.assertRaisesRegex(Exception, r"exists and overwrite=False"):
            getephemtable(
                objectname="Titan",
                timerange=self.mjdtimerange,
                outfile=self.outfile,
                overwrite=True,
            )
            getephemtable(
                objectname="Titan",
                timerange=self.mjdtimerange,
                outfile=self.outfile,
                overwrite=False,
            )

    @patch("casatasks.private.jplhorizons_query.queryhorizons")
    def test_webservice_errors(self, mock_query):
        with self.assertRaisesRegex(HTTPError, r"Not Found"):
            mock_query.side_effect = HTTPError(
                url="127.0.0.1", code=404, hdrs={}, fp=None, msg="Not Found"
            )
            getephemtable(
                objectname="Titan",
                timerange=self.caltimerange,
                outfile=self.outfile,
                overwrite=True,
            )

        with self.assertRaisesRegex(URLError, r"Unknown host"):
            mock_query.side_effect = URLError("Unknown host")
            getephemtable(
                objectname="Titan",
                timerange=self.caltimerange,
                outfile=self.outfile,
                overwrite=True,
            )

    @unittest.skipIf(
        isDatabaseURLunreachable(hostname), "JPL-Horizons data server is not reachable"
    )
    def test_table_generation(self):
        """Test ephem table generation"""
        getephemtable(
            objectname="Titan",
            timerange=self.caltimerange,
            interval="1d",
            outfile=self.outfile,
            overwrite=True,
        )

        self.assertTrue(os.path.exists(self.outfile))
        # make sure the table is readable by measures
        self.assertTrue(_me.framecomet(self.outfile))
        # further check if all the required columns exist
        self.assertTrue(self.checkEphemTableContent(self.outfile, self.reftable))

    @unittest.skipIf(
        isDatabaseURLunreachable(hostname), "JPL-Horizons data server is not reachable"
    )
    def test_save_rawdata(self):
        """Test raw query result saving"""
        getephemtable(
            objectname="Titan",
            timerange=self.caltimerange,
            interval="1d",
            outfile=self.outfile,
            rawdatafile="saved_rawqueryresult.txt",
            overwrite=True,
        )
        self.assertTrue(os.path.exists(self.outfile))
        # make sure the table is readable by measures
        self.assertTrue(_me.framecomet(self.outfile))
        # further check if all the required columns exist
        self.assertTrue(self.checkEphemTableContent(self.outfile, self.reftable))
        self.assertTrue(os.path.exists("saved_rawqueryresult.txt"))
        with open("saved_rawqueryresult.txt", "r") as f:
            rawdata = f.read()
            if "Target radii" in rawdata:
                # extract the radii values
                # expected format of the line" Target radii    : 2575.15, 2574.78, 2574.47 km    {Equator_a, b, pole_c}
                import re

                m = re.search(
                    r"Target radii\s*:\s*([\d\.]+)\s*,\s*([\d\.]+)\s*,\s*([\d\.]+)\s*km",
                    rawdata,
                )
                if m:
                    radius_a = float(m.group(1))
                    radius_b = float(m.group(2))
                    radius_c = float(m.group(3))
                    # make sure the same values exist in the ephem table
                    _tb.open(self.outfile)
                    kys = _tb.getkeywords()
                    print("kys=", kys)
                    if "radii" in kys:
                        radii_dict = _tb.getkeyword("radii")
                        self.assertTrue(radii_dict["unit"] == "km")
                        self.assertTrue(radii_dict["value"][0] == radius_a)
                        self.assertTrue(radii_dict["value"][1] == radius_b)
                        self.assertTrue(radii_dict["value"][2] == radius_c)
                    else:
                        self.fail("radii keyword is missing in the ephem table")

    @unittest.skipIf(
        isDatabaseURLunreachable(hostname), "JPL-Horizons data server is not reachable"
    )
    def test_nonalphanumeric_name(self):
        """Test object name extraction for the ojbect name contains nonalphanumeric characters"""
        getephemtable(
            objectname="90000322",
            asis=True,
            timerange="2018/09/16/10:15:54~2018/09/22/13:16:21",
            outfile=self.outfile,
            overwrite=False,
        )
        self.assertTrue(os.path.exists(self.outfile))
        _tb.open(self.outfile)
        objname = _tb.getkeyword("NAME")
        _tb.done()
        self.assertTrue(objname == "21P/Giacobini-Zinner")

    @unittest.skipIf(
        isDatabaseURLunreachable(hostname), "JPL-Horizons data server is not reachable"
    )
    def test_secondaryname_in_parentheses(self):
        """Test object name extraction from the secondary name in the prentheses (e.g. certain comets, etc)"""
        getephemtable(
            objectname="C/2025 N1",
            asis=True,
            timerange="2025/09/20/14:00:00~2025/09/20/16:00:00",
            outfile=self.outfile,
            overwrite=False,
        )
        self.assertTrue(os.path.exists(self.outfile))
        _tb.open(self.outfile)
        keys = _tb.getkeywords()
        self.assertTrue("NAME" in keys)
        self.assertTrue(_tb.getkeyword("NAME") == "C/2025 N1")
        _tb.done()

    def test_tocasatb_textfile_geo(self):
        """Test tocasatb function independently,  geocentric location"""
        getephemtable(
            objectname="Titan",
            timerange=self.caltimerange,
            interval="1d",
            outfile=self.outfile,
            rawdatafile="saved_rawqueryresult.txt",
            overwrite=True,
        )
        from casatasks.private import jplhorizons_query as jplq

        jplq.tocasatb("saved_rawqueryresult.txt", "titan_eph_from_textdata.tab")
        self.assertTrue(
            self.checkEphemTableContent("titan_eph_from_textdata.tab", self.outfile)
        )

    def test_tocasatb_textfile_topo(self):
        """Test tocasatb function independently, topocentric (ALMA, VLA, and GBT) locations"""

        from casatasks.private import jplhorizons_query as jplq

        jplq.tocasatb(self.inALMAtextfile, "titan_eph_from_ALMAtextdata.tab")
        _tb.open("titan_eph_from_ALMAtextdata.tab")
        obsloc = _tb.getkeyword("obsloc")
        _tb.done()
        self.assertTrue(obsloc, "ALMA")
        jplq.tocasatb(self.inVLAtextfile, "titan_eph_from_VLAtextdata.tab")
        _tb.open("titan_eph_from_VLAtextdata.tab")
        obsloc = _tb.getkeyword("obsloc")
        _tb.done()
        self.assertTrue(obsloc, "VLA")
        jplq.tocasatb(self.inGBTtextfile, "titan_eph_from_GBTtextdata.tab")
        _tb.open("titan_eph_from_GBTtextdata.tab")
        obsloc = _tb.getkeyword("obsloc")
        _tb.done()
        self.assertTrue(obsloc, "GBT")


if __name__ == "__main__":
    unittest.main()
