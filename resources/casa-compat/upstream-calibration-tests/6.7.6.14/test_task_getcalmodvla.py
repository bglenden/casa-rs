##########################################################################
# test_task_getcalmodvla.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.manipulation.phaseshift.html
#
#
##########################################################################
import glob
import http.server
import numpy as np
import os
from pathlib import Path
import re
import shutil
import sys
import threading
import unittest
from urllib import request
from urllib.error import URLError
from urllib.parse import urlparse, parse_qs

from casatasks import casalog

from casatools import componentlist, measures

from casatasks import getcalmodvla

import casatestutils


# NOTE be certain to specify the top-level casatestutils directory
# in your PYTHONPATH so you load the casatestutils directory which
# is a subdir of that


class MockHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTPServer mock request handler"""

    def do_GET(self):
        casalog.post("server path " + self.path, "WARN")
        parms = parse_qs(urlparse(self.path).query)
        casalog.post(f"server parms {parms}", "INFO")
        good_sources = ("3C48", "3C286", "3C138", "3C147")
        if "source" in parms and parms["source"][0].upper() not in good_sources:
            explain = f"source must be one of {good_sources}"
            self.send_error(400, message="Invalid input", explain=explain)
            self.end_headers()
            return
        """Handle GET requests"""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        myfile = os.sep.join([
            casatestutils.__path__[0], "getcalmodvla_helpers", "query1.json"
        ])
        with open(myfile, "r") as f:
            file_contents = f.read()
        self.wfile.write(str.encode(file_contents))

    def log_request(self, code=None, size=None):
        """Don"t log anything"""

class getcalmodvla_test(unittest.TestCase):


    hostname = "http://127.0.0.1:8080"


    def setUp(self):
        self.cl = componentlist()
        self.clname = "my.cl"


    def tearDown(self):
        self.cl.done()
        del self.cl
        if os.path.exists(self.clname):
            shutil.rmtree(self.clname)


    def exception_verification(self, cm, expected_msg):
        exc = cm.exception
        pos = str(exc).find(expected_msg)
        self.assertNotEqual(
            pos, -1, msg=f"Unexpected exception was thrown: {exc}"
        )


    def query_server(self, method):
        server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 8080), MockHTTPRequestHandler
        )
        with server:
            server_thread = threading.Thread(target=server.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            try:
                method()
            finally:
                server.shutdown()


    def test_inputs(self):
        """Test inputs meet various constraints"""
        with self.assertRaises(ValueError) as cm:
            getcalmodvla()
        self.exception_verification(cm, "outfile must be specified")
        outfile = "my.cl"
        Path(outfile).touch()
        with self.assertRaises(RuntimeError) as cm:
            getcalmodvla(outfile, False)
        self.exception_verification(
            cm,
            "The overwrite parameter is False and a file or directory named "
            f"{outfile} already exists. Either remove or rename it, or change "
            "overwrite to True, or both."
        )
        os.remove(outfile)
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl")
        self.exception_verification(cm, "Exactly one of source or direction must be specified")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "mysource", "mydirection")
        self.exception_verification(cm, "Both source and direction may not be simultaneously specified")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, direction="mydirection")
        self.exception_verification(cm, "Illegal direction specification mydirection")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, direction="1 2 3")
        self.exception_verification(cm, "Illegal direction specification 1 2 3")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48")
        self.exception_verification(cm, "band must be specified")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="m")
        self.exception_verification(cm, "band m not supported")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", obsdate=[])
        self.exception_verification(
            cm,
            "obsdate must either be a number or a string of the form YYYY-MM-DD"
        )
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", obsdate="hi")
        self.exception_verification(
            cm, "If specified as a string, obsdate must be of the form YYYY-MM-DD"
        )
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", obsdate=50000, refdate="123")
        self.exception_verification(
            cm, "If specified as a string, refdate must be of the form "
            + "YYYY-MM-DD"
        )
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", obsdate=50000, refdate=0, hosts=[])
        self.exception_verification(cm, "hosts must be specified")
        hosts = ["zz"]
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", obsdate=50000, refdate=0, hosts=hosts)
        self.exception_verification(cm, "zz is not a valid host expressed as a URL")
        hosts = ["http://my.bogus.com:8080"]
        with self.assertRaises(RuntimeError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", obsdate=50000, refdate=0, hosts=hosts)
        self.exception_verification(cm, "All URLs failed to return a component list")
        with self.assertRaises(ValueError) as cm:
            getcalmodvla("my.cl", True, "3c48", band="q", refdate=[])
        self.exception_verification(
            cm,
            "refdate must either be a number or a string of the form YYYY-MM-DD"
        )


    def test_component_list_writing(self):
        """Test successful writing of a component list"""
        hosts = [self.hostname]
        self.query_server(
            lambda: getcalmodvla(
                self.clname, True, "3C48", band="Q",obsdate=50000, hosts=hosts
            )
        )
        self.cl.open(self.clname)
        self.assertEqual(self.cl.length(), 385, "Incorrect number of components")
        ws = self.cl.getkeyword("web_service")
        self.assertEqual(ws["band"], "Q", "Incorrect band in web_service metadata")
        self.assertEqual(ws["source"], "3C48", "Incorrect source in web_service metadata")


    def test_obsdate_as_string(self):
        hosts = [self.hostname]
        self.query_server(
            lambda: getcalmodvla(
                self.clname, True, "3C48", band="Q",obsdate="2002-04-20", hosts=hosts
            )
        )
        self.cl.open(self.clname)
        self.assertEqual(self.cl.length(), 385, "Incorrect number of components")
        ws = self.cl.getkeyword("web_service")
        self.assertEqual(ws["band"], "Q", "Incorrect band in web_service metadata")
        self.assertEqual(ws["source"], "3C48", "Incorrect source in web_service metadata")


    def test_direction(self):
        """Test direction input"""
        hosts = [self.hostname]
        direction = "J2000 01:37:41.1 33.09.32"
        self.query_server(
            lambda: getcalmodvla(
                self.clname, True, direction=direction, band="Q",obsdate=50000, hosts=hosts
            )
        )
        self.cl.open(self.clname)
        self.assertEqual(self.cl.length(), 385, "Incorrect number of components")
        ws = self.cl.getkeyword("web_service")
        self.assertEqual(ws["band"], "Q", "Incorrect band in web_service metadata")


if __name__ == "__main__":
     unittest.main()
