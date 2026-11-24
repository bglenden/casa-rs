# SPDX-License-Identifier: LGPL-3.0-or-later
"""Expose the stock gencal test's Jy/K transport double over loopback HTTP."""

from contextlib import contextmanager
import csv
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from math import isclose
import os
from threading import Thread
from unittest.mock import Mock, patch
from urllib.parse import parse_qsl, urlsplit

JYPERK_ORIGIN = "https://asa.alma.cl/science/jy-kelvins"


def recorded_query_key(url):
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    elevation = query.pop("elevation")
    frequency = query.pop("frequency")
    return (parsed.path, tuple(sorted(query.items()))), elevation, frequency


def recorded_queries(endpoint):
    from casatools import ctsys

    fixture = ctsys.resolve(f"unittest/gencal/jyperk_web_api_response/{endpoint}.csv")
    with open(fixture, newline="", encoding="utf-8") as stream:
        queries = []
        for url, raw in csv.reader(stream, delimiter="\t"):
            response = json.loads(raw)
            response.pop("timestamp", None)
            response.pop("elapsed", None)
            response["query"].pop("elevation", None)
            response["query"].pop("frequency", None)
            queries.append((recorded_query_key(url), url, response))
        return queries


def recorded_url(url, queries):
    key, elevation, frequency = recorded_query_key(url)
    # Measures conversion and mean channel frequency can round differently.
    # Nearby windows may share a response. Require agreement on their payloads.
    matches = [
        (candidate, response)
        for (other_key, other_elevation, other_frequency), candidate, response in queries
        if key == other_key and (
            elevation == other_elevation
            or isclose(float(elevation), float(other_elevation), abs_tol=1e-10, rel_tol=0)
        ) and isclose(float(frequency), float(other_frequency), abs_tol=0.05, rel_tol=0)
    ]
    if not matches or any(payload != matches[0][1] for _, payload in matches[1:]):
        raise ValueError(f"missing or inconsistent Jy/K fixture response for {url}")
    return matches[0][0]


@contextmanager
def jyperk_transport(arguments):
    if arguments.get("caltype") != "jyperk" or arguments.get("infile"):
        yield
        return

    from casatasks.private.jyperk import JyPerKDatabaseClient

    response = JyPerKDatabaseClient._try_to_get_response
    if not isinstance(response, Mock):
        yield
        return

    endpoint = arguments.get("endpoint")
    queries = recorded_queries(endpoint) if endpoint in {"interpolation", "model-fit"} else []
    failures = []

    class Request(BaseHTTPRequestHandler):
        def do_GET(self):
            url = f"{JYPERK_ORIGIN}{self.path}"
            try:
                if queries:
                    url = recorded_url(url, queries)
                body = response(url).encode("utf-8")
            except Exception as error:
                failures.append(RuntimeError(f"upstream Jy/K mock rejected {url}: {error}"))
                body = json.dumps({"success": False, "error": str(failures[-1])}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    with HTTPServer(("127.0.0.1", 0), Request) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}"
            with patch.dict(os.environ, {"JYPERKDB_URL": url}):
                yield
        finally:
            server.shutdown()
            thread.join()
            if failures:
                raise failures[0]
