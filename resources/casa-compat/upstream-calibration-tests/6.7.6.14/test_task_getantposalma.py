##########################################################################
# test_task_getantposalma.py
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


import ast
import casatestutils
import http.server
import json
import os
import threading
import unittest
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from casatasks import getantposalma, casalog


# NOTE be certain to specify the top-level casatestutils directory
# in your PYTHONPATH so you load the casatestutils directory which
# is a subdir of that


class MockHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTPServer mock request handler"""

    def do_GET(self):
        casalog.post('server path ' + self.path, 'WARN')
        parms = parse_qs(urlparse(self.path).query)
        casalog.post(f'server parms {parms}', 'INFO')
        """Handle GET requests"""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        myfile = os.sep.join([
            casatestutils.__path__[0], 'getantposalma_helpers', 'query1.json'
        ])
        with open(myfile, 'r') as f:
            file_contents = f.read()
        self.wfile.write(str.encode(file_contents))


    def log_request(self, code=None, size=None):
        """Don't log anything"""
        pass


class getantposalma_test(unittest.TestCase):

    hostname = "http://127.0.0.1:8080"

    outfile = "getantposalma.json"

    def setUp(self):
        if os.path.exists(self.outfile):
            os.remove(self.outfile)


    def tearDown(self):
        if os.path.exists(self.outfile):
            os.remove(self.outfile)


    def exception_verification(self, cm, expected_msg):
        exc = cm.exception
        pos = str(exc).find(expected_msg)
        self.assertNotEqual(
            pos, -1, msg=f'Unexpected exception was thrown: {exc}. Expected to find: {expected_msg}'
        )


    def _query_server(self, method, server_class=MockHTTPRequestHandler):
        server = http.server.ThreadingHTTPServer(
            ('127.0.0.1', 8080), server_class
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
        """
        Test inputs meet various constraints. All the exceptions are thrown
        without having queried the server
        """
        with self.assertRaises(ValueError) as cm:
            getantposalma()
        self.exception_verification(cm, "Parameter outfile must be specified")
        with self.assertRaises(ValueError) as cm:
            getantposalma(hosts=["good.example.com"])
        self.exception_verification(cm, "Parameter outfile must be specified")
        outfile = "kyfjak.blah"
        Path(outfile).touch()
        with self.assertRaises(RuntimeError) as cm:
            getantposalma(outfile=outfile, overwrite=False, hosts=["http://good.example.com"])
        self.exception_verification(
            cm,
            f"A file or directory named {outfile} already exists and overwrite "
            "is False, so exiting. Either rename the existing file or directory, "
            "change the value of overwrite to True, or both."
        )
        os.remove(outfile)
        with self.assertRaises(ValueError) as cm:
            getantposalma(outfile="myants.json", hosts=[])
        self.exception_verification(cm, "Parameter hosts must be specified")
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                tw="1,2,3", hosts=["http://good.example.com"]
            )
        self.exception_verification(
            cm, "Parameter tw should contain exactly one comma that separates two times"
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d", tw="1,3",
                hosts=["http://good.example.com"]
            )
        self.exception_verification(
            cm,
            "Begin time 1 does not appear to have a valid format. The correct format "
            "is of the form YYYY-MM-DDThh:mm:ss."
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                tw="2023-04-15T17:15:22,3", hosts=["http://good.example.com"]
            )
        self.exception_verification(
            cm,
            "End time 3 does not appear to have a valid format. The correct format "
            "is of the form YYYY-MM-DDThh:mm:ss."
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                tw="2023-04-15T17:15:22,2023-03-02T14:41:00",
                hosts=["http://good.example.com"]
            )
        self.exception_verification(
            cm,
            "Parameter tw, start time (2023-04-15T17:15:22) must be less than end "
            "time (2023-03-02T14:41:00)."
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                tw="2023-04-15T17:15:22,2023-04-15T17:15:22",
                hosts=["good.example.com"]
            )
        self.exception_verification(
            cm,
            "Parameter tw, start time (2023-04-15T17:15:22) must be less than end "
            "time (2023-04-15T17:15:22)."
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                snr=-1, hosts=["http://good.example.com"]
            )
        self.exception_verification(
            cm, "If a number, parameter snr (-1) must be non-negative."
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                hosts=["bogus./12?.example.com"]
            )
        self.exception_verification(
            cm,
            "Parameter hosts: bogus./12?.example.com is not a valid host expressed as a URL."
        )
        with self.assertRaises(RuntimeError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                hosts=["http://www.bogus.edu"]
            )
        self.exception_verification(
            cm,
            "All URLs failed to return an antenna position list."
        )
        with self.assertRaises(AssertionError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                hosts=["http://www.bogus.edu"], firstintegration=7
            )
        self.exception_verification(
            cm,
            "{'firstintegration': ['must be of cBool type']}"
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                hosts=["http://www.bogus.edu"], nretry=-1, rdelay=1
            )
        self.exception_verification(
            cm,"Parameter nretry (=-1) must be non-negative."
        )
        with self.assertRaises(ValueError) as cm:
            getantposalma(
                outfile="myants.json", asdm="uid://A002/X10ac6bc/X896d",
                hosts=["http://www.bogus.edu"], nretry=1, rdelay=-1
            )
        self.exception_verification(
            cm, "If nretry > 0, rdelay (=-1.0) must be positive."
        )


    def test_json_file_writing(self):
        """Test successful writing of json file of antenna positions"""
        hosts = [self.hostname]
        firstintegration = False
        for snr in ("default", 5):
            self._query_server(
                lambda: getantposalma(
                    self.outfile, asdm="uid://A002/X10ac6bc/X896d",
                    hosts=hosts, snr=snr, firstintegration=firstintegration
                )
            )
            self.assertTrue(os.path.exists(self.outfile))
            with open(self.outfile, "r") as f:
                res_dict = json.load(f)
            self.assertTrue(
                "data" in res_dict and "metadata" in res_dict,
                "Incorrect data structure"
            )
            antpos = res_dict["data"]
            md = res_dict["metadata"]
            self.assertEqual(type(antpos), dict, "Wrong data type")
            self.assertEqual(len(antpos), 3, "Wrong number of antennas")
            self.assertTrue(
                "description" in md, "metadata lacks required description key"
            )
            self.assertTrue(
                "product_code" in md and md["product_code"] == "antposalma",
                "metadata either does not contains required product_code key "
                "or the value associated with that key is incorrect. It must "
                "be 'antposalma'"
            )
            self.assertEqual(
                md["outfile"], self.outfile, "Wrong outfile name in metadata"
            )
            self.assertEqual(
                md["asdm"], "uid://A002/X10ac6bc/X896d", "Wrong asdm name in metadata"
            )
            if snr == "default":
                self.assertTrue(
                    "snr" not in md,
                    "snr incorrectly in meetadata when default value used"
                )
            else:
                self.assertEqual(md["snr"], 5, "Wrong value for snr")
            self.assertEqual(
                md["firstIntegration"], firstintegration, "Incorrect metadata firstintegraton"
            )
            self.assertEqual(
                md["caltype"], "ALMA antenna positions", "Incorrect metadata caltype"
            )
            os.remove(self.outfile)


    def test_retries(self):
        """Test that retries occur when server is not initially available"""
        nretry = 2
        class FlakyMockHTTPRequestHandler(MockHTTPRequestHandler):
            """MockHTTPRequestHandler that fails the first two requests"""

            request_count = 0

            def do_GET(self):
                FlakyMockHTTPRequestHandler.request_count += 1
                if FlakyMockHTTPRequestHandler.request_count < nretry + 1:
                    casalog.post(
                        f'FlakyMockHTTPRequestHandler failing request '
                        f'number {FlakyMockHTTPRequestHandler.request_count}',
                        'WARN'
                    )
                    self.send_response(500)
                    self.end_headers()
                else:
                    super().do_GET()
        hosts = [self.hostname]
        self._query_server(
            lambda: getantposalma(
                self.outfile, asdm="uid://A002/X10ac6bc/X896d",
                hosts=hosts, snr=10, nretry=nretry, rdelay=3.0
            ),
            server_class=FlakyMockHTTPRequestHandler
        )
        self.assertTrue(os.path.exists(self.outfile))
        with open(self.outfile, "r") as f:
            res_dict = json.load(f)
        self.assertTrue(
            "data" in res_dict and "metadata" in res_dict,
            "Incorrect data structure"
        )

if __name__ == '__main__':
     unittest.main()
