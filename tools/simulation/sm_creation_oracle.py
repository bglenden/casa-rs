#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Create the same unequal-window mosaic with installed CASA and casa-rs.

Use CASA 6.7.6.14 for `create casa` and `compare msv2`, the installed
casa-rs wheel for `create default|msv2|msv4`, and the pinned XRADIO
environment for `compare msv4`. CASA creates the shared component list.
"""

from argparse import ArgumentParser, Namespace
from pathlib import Path

import numpy as np

CENTRE = (2.6179938779914944, 0.593411945678072)
OFFSET = (CENTRE[0] + 6e-6 / 0.8338858220678796, CENTRE[1] - 4e-6)


def create(args):
    is_casa = args.runtime == "casa"
    if is_casa:
        from casatasks import version_string
        from casatools import componentlist, measures, simulator

        assert version_string() == "6.7.6.14"
        sm = simulator()
    else:
        from casars.tools import measures, simulator

        sm = simulator(backend="cpu", workers=args.workers)
    me = measures()
    if args.runtime in ("msv2", "msv4"):
        assert sm.open(str(args.output), outformat=args.runtime)
    else:
        assert sm.open(str(args.output))
    centre = [-1601185.0, -5041977.0, 3554875.0]
    xyz = [[origin + step * index for index in range(8)]
           for origin, step in zip(centre, (20.0, -6.0, 12.0), strict=True)]
    assert sm.setconfig(telescopename="ALMA", x=xyz[0], y=xyz[1], z=xyz[2], dishdiameter=[12.0],
                        offset=[0.0], mount=["alt-az"],
                        antname=[f"DA{index}" for index in range(8)],
                        padname=[f"A{index}" for index in range(8)],
                        coordsystem="global", referencelocation=me.observatory("ALMA"))
    high_channels = 3 if args.same_shape else 8
    for name, frequency, width, channels in (("low", 100e9, 1e7, 3),
                                               ("high", 101e9, 2e7, high_channels)):
        assert sm.setspwindow(spwname=name, freq=f"{frequency}Hz", deltafreq=f"{width}Hz",
                              freqresolution=f"{width}Hz", refcode="TOPO", nchannels=channels,
                              stokes="XX YY")
    assert sm.setfeed(mode="perfect X Y")
    reference = me.epoch("UTC", "2024/06/01/00:00:00")
    assert sm.settimes(integrationtime="10s", usehourangle=False, referencetime=reference)
    for name, (ra, dec) in (("target", CENTRE), ("offset", OFFSET)):
        assert sm.setfield(sourcename=name, sourcedirection=me.direction("J2000", f"{ra}rad", f"{dec}rad"))
    # CASA stores the numeric TAI time in a column tagged UTC. Offset CASA
    # to give both tools the same persisted physical instants.
    correction = 37 if is_casa else 0
    scans = (("target", "low", 0, args.duration),
             ("offset", "high", args.duration, 2 * args.duration),
             ("offset", "low", 2 * args.duration, 2 * args.duration + 10))
    for index, (field, window, start, end) in enumerate(scans):
        first, last = f"{start - correction}s", f"{end - correction}s"
        if index == 2:
            # This rejected scan also has a beam pointing distinct from
            # its field phase centre, as in the fresh-pointings oracle.
            assert sm.setlimits(elevationlimit="90deg")
            direction = f"J2000 {OFFSET[0] + 2e-5}rad {OFFSET[1] - 1e-5}rad"
            assert sm.observemany(sourcenames=[field], spwname=window, starttimes=[first],
                                  stoptimes=[last], directions=[direction],
                                  state_obs_mode="OBSERVE_TARGET.ON_SOURCE")
        else:
            assert sm.observe(sourcename=field, spwname=window, starttime=first, stoptime=last)
    if is_casa and not args.component.exists():
        cl = componentlist()
        assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad",
                               flux=1.0, freq="100GHz")
        cl.rename(str(args.component))
        cl.done()
    assert sm.predict(complist=str(args.component))
    assert sm.close()
    me.done()


def compare_msv2(casa, result):
    from compare_sm_ms import compare_scenario

    # Source time records the reference epoch used at setfield. The
    # comparator accounts for the existing native column-profile decisions.
    scenario = {"name": "mixed_window_mosaic", "data": "model"}
    comparison = compare_scenario(scenario, str(casa), str(result),
                                  Namespace(uvw_tolerance_m=1e-4, data_nrms=1e-3))
    assert not comparison["differences"], comparison
    assert comparison["data"]["rows_flagged_in_both"] == 36, comparison
    print(comparison["data"])


def compare_msv4(casa, result, *, observer="TOPO", datasets=3, flagged_rows=36,
                 offset_reference=False):
    from arcae.lib.arrow_tables import Table
    from astropy.time import Time
    from xradio.measurement_set import open_processing_set
    from xradio.schema.check import check_datatree

    def open_table(name=""):
        return Table.from_filename(f"{casa}::{name}" if name else str(casa))

    def casa_time(seconds):
        days, parts = np.divmod(seconds, 86400.0)
        return Time(days, parts / 86400.0, format="mjd", scale="utc")

    def xradio_time(coordinate):
        return Time(coordinate.values, format=coordinate.attrs["format"],
                    scale=coordinate.attrs["scale"])

    def check_directions(actual, expected):
        # Both [-pi, pi) and [0, 2*pi) describe the same longitude.
        difference = np.asarray(actual) - expected
        difference[..., 0] = (difference[..., 0] + np.pi) % (2 * np.pi) - np.pi
        np.testing.assert_allclose(difference, 0, atol=1e-12, rtol=0)

    root = open_processing_set(str(result))
    issues = check_datatree(root)
    assert not issues, str(issues)
    main = open_table()
    ant = open_table("ANTENNA")
    field = open_table("FIELD")
    dd = open_table("DATA_DESCRIPTION")
    spw = open_table("SPECTRAL_WINDOW")
    observation = open_table("OBSERVATION")
    pointing = open_table("POINTING")
    state = open_table("STATE")
    # XRADIO names ALMA antennas as NAME_STATION; validate both parts
    # against the independent ANTENNA table.
    stations = ant.getcol("STATION")
    ant_names = np.asarray([f"{name}_{station}" for name, station in
                            zip(ant.getcol("NAME"), stations, strict=True)])
    field_names = [f"{name}_{idx}" for idx, name in enumerate(field.getcol("NAME"))]
    windows = [f"{name}_{idx}" for idx, name in enumerate(spw.getcol("NAME"))]
    times = main.getcol("TIME")
    scans = main.getcol("SCAN_NUMBER")
    sky = main.getcol("FIELD_ID")
    data_desc = main.getcol("DATA_DESC_ID")
    observation_ids = main.getcol("OBSERVATION_ID")
    projects = observation.getcol("PROJECT")
    state_ids = main.getcol("STATE_ID")
    modes = state.getcol("OBS_MODE")
    first, second = main.getcol("ANTENNA1"), main.getcol("ANTENNA2")
    point_time = pointing.getcol("TIME")
    point_ant = pointing.getcol("ANTENNA_ID")
    point_direction = pointing.getcol("DIRECTION").reshape(-1, 2)
    point_rows = {(time, ant): index for index, (time, ant) in
                  enumerate(zip(point_time, point_ant, strict=True))}
    if offset_reference:
        rest_frequencies = open_table("SOURCE").getcol("REST_FREQUENCY")
    rows_seen = np.zeros(len(times), dtype=bool)
    num_flagged = 0
    for node in root.children.values():
        ds = node.ds.load()
        scan, = np.unique(ds.scan_name.values.astype(int))
        rows = np.flatnonzero(scans == scan)
        assert rows.size and not rows_seen[rows].any()
        rows_seen[rows] = True
        project, = np.unique(projects[observation_ids[rows]])
        assert node.attrs["observation_info"]["project_UID"] == project
        assert set(ds.scan_name.attrs["scan_intents"]) == set(modes[state_ids[rows]])
        time_index = np.searchsorted(np.unique(times[rows]), times[rows])
        baselines = {(a, b): index for index, (a, b) in
                     enumerate(zip(ds.baseline_antenna1_name.values, ds.baseline_antenna2_name.values,
                                   strict=True))}
        baseline_index = [baselines[ant_names[first[row]], ant_names[second[row]]] for row in rows]
        at = (time_index, baseline_index)
        assert ds.sizes["time"] * ds.sizes["baseline_id"] == rows.size
        window, = np.unique(dd.getcol("SPECTRAL_WINDOW_ID")[data_desc[rows]])
        assert ds.frequency.attrs["spectral_window_name"] == windows[window]
        np.testing.assert_array_equal(ds.polarization.values, ["XX", "YY"])
        channels = spw.getcol("CHAN_FREQ", index=([window],))[0]
        np.testing.assert_allclose(ds.frequency.values, channels, atol=0.01, rtol=0)
        assert ds.frequency.attrs["observer"].upper() == observer
        reference = spw.getcol("REF_FREQUENCY")[window]
        np.testing.assert_allclose(ds.frequency.attrs["reference_frequency"]["data"], reference,
                                   atol=0.01, rtol=0)
        if offset_reference:
            assert reference != channels[0]
            np.testing.assert_array_equal(rest_frequencies, reference)
        np.testing.assert_allclose(ds.frequency.attrs["channel_width"]["data"],
                                   spw.getcol("CHAN_WIDTH", index=([window],))[0], atol=0.01, rtol=0)
        data = main.getcol("DATA", index=(rows,))
        row_flags = main.getcol("FLAG_ROW", index=(rows,))
        num_flagged += np.count_nonzero(row_flags)
        flags = ds.FLAG.values[at]
        np.testing.assert_array_equal(flags, main.getcol("FLAG", index=(rows,)) | row_flags[:, None, None])
        np.testing.assert_allclose(ds.VISIBILITY.values[at][~flags], data[~flags], atol=1e-6, rtol=1e-6)
        weights = main.getcol("WEIGHT", index=(rows,))
        np.testing.assert_array_equal(ds.WEIGHT.values[at], np.broadcast_to(weights[:, None, :], data.shape))
        np.testing.assert_allclose(ds.UVW.values[at], main.getcol("UVW", index=(rows,)), atol=1e-4, rtol=0)
        np.testing.assert_allclose((xradio_time(ds.time)[time_index] - casa_time(times[rows])).sec,
                                   0.0, atol=2e-6, rtol=0)
        centroids = main.getcol("TIME_CENTROID", index=(rows,))
        np.testing.assert_allclose((xradio_time(ds.TIME_CENTROID)[at] - casa_time(centroids)).sec,
                                   0.0, atol=2e-6, rtol=0)
        np.testing.assert_array_equal(ds.EFFECTIVE_INTEGRATION_TIME.values[at],
                                      main.getcol("EXPOSURE", index=(rows,)))
        np.testing.assert_allclose(ds.time.attrs["integration_time"]["data"],
                                   main.getcol("INTERVAL", index=(rows,)), atol=1e-6, rtol=0)
        np.testing.assert_array_equal(ds.field_name.values[time_index], np.asarray(field_names)[sky[rows]])
        sky_reference = node["field_and_source_base_xds"].ds
        expected = field.getcol("PHASE_DIR").reshape(-1, 2)
        wanted = [expected[field_names.index(name)] for name in sky_reference.field_name.values]
        check_directions(sky_reference.FIELD_PHASE_CENTER_DIRECTION, wanted)
        ant_reference = node["antenna_xds"].ds
        ids = [list(ant_names).index(name) for name in ant_reference.antenna_name.values]
        np.testing.assert_array_equal(ant_reference.station_name, stations[ids])
        np.testing.assert_allclose(ant_reference.ANTENNA_POSITION, ant.getcol("POSITION")[ids],
                                   atol=1e-6, rtol=0)
        point_reference = node["pointing_xds"].ds
        point_ids = [list(ant_names).index(name) for name in point_reference.antenna_name.values]
        expected_point_times = np.unique(times[rows])
        np.testing.assert_allclose((xradio_time(point_reference.time_pointing) - casa_time(expected_point_times)).sec,
                                   0.0, atol=2e-6, rtol=0)
        expected_points = [[point_direction[point_rows[time, ant_id]] for ant_id in point_ids]
                           for time in expected_point_times]
        check_directions(point_reference.POINTING_BEAM, expected_points)
        if row_flags.all():
            # The fully flagged scan deliberately moves the beam while
            # its phase centre remains the same field direction.
            assert not np.allclose(expected_points[0][0], wanted[0], atol=1e-7, rtol=0)
    assert rows_seen.all()
    assert num_flagged == flagged_rows
    assert len(root.children) == datasets
    print(f"XRADIO schema and {rows_seen.size} CASA acquisitions passed across {datasets} datasets")


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    creating = commands.add_parser("create")
    creating.add_argument("runtime", choices=("casa", "default", "msv2", "msv4"))
    creating.add_argument("output", type=Path)
    creating.add_argument("component", type=Path)
    creating.add_argument("--workers", type=int, default=1)
    creating.add_argument("--duration", type=int, default=20)
    creating.add_argument("--same-shape", action="store_true")
    comparing = commands.add_parser("compare")
    comparing.add_argument("format", choices=("msv2", "msv4"))
    comparing.add_argument("casa", type=Path)
    comparing.add_argument("result", type=Path)
    args = parser.parse_args()
    if args.command == "create":
        create(args)
    elif args.format == "msv2":
        compare_msv2(args.casa, args.result)
    else:
        compare_msv4(args.casa, args.result)
