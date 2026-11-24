#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare both public getephemtable tasks against the same recorded Horizons reply."""

import argparse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import socket
import threading
from unittest.mock import patch
from urllib.parse import parse_qs
from urllib.request import Request, urlopen

import casatasks
import casatools
from casars import tasks, tools
import numpy as np
from record_casa_returns import make_cal_ms


@contextmanager
def horizons(text, status=200):
    requests = []
    payload = json.dumps({"result": text}).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            form = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append(parse_qs(form.decode(), strict_parsing=True))
            self.send_response(status)
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
        assert re.fullmatch(r"\d{4}/\d{2}/\d{2}/\d{2}:\d{2}", keywords.pop("VS_DATE"))
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


@contextmanager
def route_horizons_requests(url):
    def redirect(request, *, context, timeout):
        del context
        assert request.full_url == "https://ssd.jpl.nasa.gov/api/horizons.api"
        return urlopen(Request(url, data=request.data), timeout=timeout)

    with patch("urllib.request.urlopen", redirect), patch.dict(os.environ, {"CASARS_JPL_HORIZONS_URL": url}):
        yield


def compare_case(work, raw, objectname, asis, timerange, interval, expected, *, usable_by_measures=True):
    with horizons(raw) as (url, requests):
        outputs = {}
        for label, api in (("casa", casatasks), ("native", tasks)):
            output = work / f"{label}.tab"
            rawfile = work / f"{label}.txt"
            with route_horizons_requests(url):
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
        # Casa measures rejects MJD0 below 10000 even in its own tables.
        assert measure.framecomet(str(written)) == usable_by_measures
        measure.done()
    return len(oracle[1]["MJD"][1])


def check_query_failure(work, apis, url):
    with route_horizons_requests(url):
        for label, api in apis:
            output = work / f"{label}-failed.tab"
            rawfile = work / f"{label}-failed.txt"
            assert api.getephemtable(
                "Titan", timerange="2023/09/01/20:00~2023/09/04/20:00",
                outfile=str(output), rawdatafile=str(rawfile),
            ) is None
            assert not output.exists() and not rawfile.exists()


def check_transport_errors(work, apis):
    with horizons("No result", status=404) as (url, requests):
        check_query_failure(work, apis, url)
        assert len(requests) == len(apis)
    # Keep the port reserved, without accepting connections.
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        check_query_failure(work, apis, f"http://127.0.0.1:{port}/api/horizons.api")


def check_destination_protection(work, raw):
    output = work / "calendar/native.tab"
    leap_seconds = work / "leap-seconds.tab"
    shutil.copytree(casatools.ctsys.resolve("geodetic/TAI_UTC"), leap_seconds)
    snapshots = {table: (table / "table.dat").read_bytes() for table in (output, leap_seconds)}
    with horizons(raw) as (url, requests), route_horizons_requests(url):
        for destination, rawfile, expected in (
            (output, str(output / "table.dat"), "rawdatafile and outfile overlap"),
            (leap_seconds, "", "is not a IERS Comet table"),
        ):
            try:
                tasks.getephemtable(
                    "Titan", timerange="2023/09/01/20:00~2023/09/04/20:00",
                    outfile=str(destination), rawdatafile=rawfile, overwrite=True,
                )
            except (ValueError, RuntimeError) as error:
                assert expected in str(error)
            else:
                raise AssertionError(f"the task replaced {destination}")
        assert not requests
    assert all((table / "table.dat").read_bytes() == data for table, data in snapshots.items())


def compare_moving_field(work):
    source = work / "source.ms"
    make_cal_ms(str(source))
    measure = casatools.measures()
    snapshots = []
    for label in ("casa", "native"):
        vis = work / f"{label}.ms"
        shutil.copytree(source, vis)
        attachment = casatools.ms()
        assert attachment.open(str(vis), nomodify=False)
        try:
            assert attachment.addephemeris(0, str(work / f"{label}.tab"), "Titan", [0])
        finally:
            assert attachment.done()
        for reader in (casatools, tools):
            metadata = reader.msmetadata()
            assert metadata.open(str(vis))
            try:
                centres = []
                for day in (60189.083333333336, 60190.083333333336, 60191.083333333336):
                    epoch = measure.epoch("UTC", {"value": day, "unit": "d"})
                    centre = measure.measure(metadata.phasecenter(0, epoch), "J2000")
                    centres.append([centre[key]["value"] for key in ("m0", "m1")])
                snapshots.append(centres)
            finally:
                assert metadata.done()
    measure.done()
    assert np.max(np.ptp(snapshots[0], axis=0)) > 1e-3
    for result in snapshots[1:]:
        np.testing.assert_allclose(result, snapshots[0], atol=2e-8, rtol=0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    parser.add_argument("raw_geocentric", type=Path)
    parser.add_argument("--jupiter", type=Path, help="recorded Jupiter reply with a sidereal rotation period")
    parser.add_argument("--historical-moon", type=Path, help="recorded Moon reply from the Julian calendar")
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    raw = args.raw_geocentric.read_text()
    extra = [(name, file.read_text(), target, timerange, query_range)
             for name, file, target, timerange, query_range in (
                 ("jupiter", args.jupiter, "Jupiter", "2023/09/01~2023/09/04", "2023/09/01~2023/09/04"),
                 # CASA's historical date formatting sends the previous day.
                 ("historical-moon", args.historical_moon, "Moon", "1500/09/02~1500/09/05", "1500/09/01~1500/09/04"),
             ) if file is not None]
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
        if name == "calendar":
            compare_moving_field(case)
            print("Three moving J2000 phase centres agree through both MeasurementSet readers")
    check_transport_errors(root, (("casa", casatasks), ("native", tasks)))
    print("Both public tasks handle HTTP 404 and refused connections without publishing products")
    check_destination_protection(root, raw)
    print("An overlapping raw path and an unrelated IERS table retain their original storage")
    for name, data, target, timerange, query_range in extra:
        case = root / name
        case.mkdir()
        start, stop = (f"{date}/00:00:00" for date in query_range.split("~"))
        expected = {key: [value] for key, value in (common | dict(
            START_TIME=start, STOP_TIME=stop, COMMAND="'599'" if target == "Jupiter" else "'301'",
        )).items()}
        compare_case(case, data, target, False, timerange, "1d", expected,
                     usable_by_measures=name != "historical-moon")
        print(f"{name} metadata matched in both writers and both readers")


if __name__ == "__main__":
    main()
