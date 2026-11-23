#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare both public getephemtable tasks against the same recorded Horizons reply."""

import argparse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading
from unittest.mock import patch
from urllib.parse import parse_qs
from urllib.request import Request, urlopen

import casatasks
import casatools
from casars import tasks, tools
import numpy as np


@contextmanager
def horizons(text):
    requests = []
    payload = json.dumps({"result": text}).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            form = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append(parse_qs(form.decode(), strict_parsing=True))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/api/horizons.api", requests
        finally:
            server.shutdown()
            worker.join()


def read(api, path):
    table = api.table()
    assert table.open(str(path))
    try:
        cols = {name: (table.getcolkeywords(name), table.getcol(name)) for name in table.colnames()}
        keywords = table.getkeywords()
        keywords.pop("VS_DATE")
        return table.info(), cols, keywords
    finally:
        assert table.done()


def equal(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), (actual.keys(), expected.keys())
        for key, value in expected.items():
            equal(actual[key], value)
    elif isinstance(expected, tuple):
        for left, right in zip(actual, expected, strict=True):
            equal(left, right)
    elif isinstance(expected, (float, np.floating)) or (
        isinstance(expected, np.ndarray) and expected.dtype.kind in "fi"
    ):
        np.testing.assert_allclose(actual, expected, rtol=2e-13, atol=1e-14)
    else:
        np.testing.assert_equal(actual, expected)


def compare_case(work, raw, objectname, asis, timerange, interval, expected):
    with horizons(raw) as (url, requests):
        def redirect(request, *, context, timeout):
            del context
            assert request.full_url == "https://ssd.jpl.nasa.gov/api/horizons.api"
            return urlopen(Request(url, data=request.data), timeout=timeout)

        outputs = {}
        for label, api in (("casa", casatasks), ("native", tasks)):
            output = work / f"{label}.tab"
            rawfile = work / f"{label}.txt"
            with patch("urllib.request.urlopen", redirect), patch.dict(os.environ, {"CASARS_JPL_HORIZONS_URL": url}):
                result = api.getephemtable(objectname, asis, timerange, interval, str(output), str(rawfile), False)
                assert result is None
                assert rawfile.read_bytes() == raw.encode()
                assert output.is_dir()
                try:
                    api.getephemtable(objectname, asis, timerange, interval, str(output), str(rawfile), False)
                except ValueError as error:
                    assert "exists and overwrite=False" in str(error)
                else:
                    raise AssertionError("the task replaced an existing output")
                assert api.getephemtable(objectname, asis, timerange, interval, str(output), str(rawfile), True) is None
            outputs[label] = output
        assert requests == [expected] * 4, requests
    oracle = read(casatools, outputs["casa"])
    for written in outputs.values():
        for reader in (casatools, tools):
            equal(read(reader, written), oracle)
        measure = casatools.measures()
        assert measure.framecomet(str(written))
        measure.done()
    return len(oracle[1]["MJD"][1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    parser.add_argument("raw_geocentric", type=Path)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    raw = args.raw_geocentric.read_text()
    root = args.work.resolve()
    root.mkdir()
    os.chdir(root)
    common = dict(format="json", EPHEM_TYPE="OBSERVER", OBJ_DATA="YES", CENTER="500@399",
                  QUANTITIES="'1,14,15,17,19,20,24'", ANG_FORMAT="DEG", STEP_SIZE="1d", COMMAND="'606'")
    for name, objectname, asis, timerange, start, stop in (
        ("calendar", " TiTaN ", False, "2023/09/01/20:00~2023/09/04/20:00", "2023/09/01/20:00:00", "2023/09/04/20:00:00"),
        ("jd", "606", True, "JD2460189.333333~2460192.333333", "JD2460189.333333", "JD2460192.333333"),
        ("mjd", "Titan", False, "MJD 60188.833333~MJD 60191.833333", "JD2460189.333333", "JD2460192.333333"),
    ):
        case = root / name
        case.mkdir()
        expected = {key: [value] for key, value in (common | dict(START_TIME=start, STOP_TIME=stop)).items()}
        rows = compare_case(case, raw, objectname, asis, timerange, "1 d", expected)
        print(f"{name}: {rows} ephemeris rows matched across both writers and both readers")


if __name__ == "__main__":
    main()
