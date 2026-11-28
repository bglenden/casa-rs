# SPDX-License-Identifier: LGPL-3.0-or-later
"""Read a direct VLA import with XRADIO and compare it to CASA's archive import."""

import argparse
from pathlib import Path

from arcae.lib.arrow_tables import Table
from astropy.time import Time
import numpy as np
from xradio.measurement_set import open_processing_set
from xradio.schema.check import check_datatree

from check_interop import xradio_time


def archive_time(seconds):
    days, within_day = np.divmod(seconds, 86400.0)
    return Time(days, within_day / 86400.0, format="mjd", scale="tai")


def millis(seconds):
    return np.rint(seconds * 1000.0).astype(np.int64)


def check(casa, zarr):
    root = open_processing_set(str(zarr))
    issues = check_datatree(root)
    assert not issues, str(issues)
    assert root.children

    def columns(table, *names):
        path = str(casa) if not table else f"{casa}::{table}"
        reader = Table.from_filename(path)
        return tuple(np.asarray(reader.getcol(name)) for name in names)

    antenna_names, stations, positions, diameters = columns(
        "ANTENNA", "NAME", "STATION", "POSITION", "DISH_DIAMETER"
    )
    labels = np.array([f"{antenna}_{station}" for antenna, station in zip(antenna_names, stations)])
    antenna_rows = dict(zip(labels, range(len(labels)), strict=True))
    field_names, directions = columns("FIELD", "NAME", "PHASE_DIR")
    fields = np.array([f"{name}_{index}" for index, name in enumerate(field_names)])
    field_rows = dict(zip(fields, range(len(fields)), strict=True))
    windows, pol_ids = columns("DATA_DESCRIPTION", "SPECTRAL_WINDOW_ID", "POLARIZATION_ID")
    window_names, frequencies, widths, frames = columns(
        "SPECTRAL_WINDOW", "NAME", "CHAN_FREQ", "CHAN_WIDTH", "MEAS_FREQ_REF"
    )
    spectral = [f"{name}_{index}" if name not in ("", "none") else f"spw_{index}"
                for index, name in enumerate(window_names)]
    (polarizations,) = columns("POLARIZATION", "CORR_TYPE")
    # CASA Stokes enum's four circular and four linear products.
    names = ("", "I", "Q", "U", "V", "RR", "RL", "LR", "LL", "XX", "XY", "YX", "YY")
    (project,) = columns("OBSERVATION", "PROJECT")
    scan, observation, description, time, centroid, a1, a2, field, interval, exposure = columns(
        "", "SCAN_NUMBER", "OBSERVATION_ID", "DATA_DESC_ID", "TIME", "TIME_CENTROID",
        "ANTENNA1", "ANTENNA2", "FIELD_ID", "INTERVAL", "EXPOSURE"
    )
    times = archive_time(time)
    times_ms = millis(times.unix)
    centers = archive_time(centroid)
    data, corrected, model, raw_flags, row_flags, weights, uvw = columns(
        "", "DATA", "CORRECTED_DATA", "MODEL_DATA", "FLAG", "FLAG_ROW", "WEIGHT", "UVW"
    )
    assert np.count_nonzero(row_flags), "the fixture must exercise folded row flags"
    actual_count = 0
    seen = np.zeros(len(time), dtype=bool)
    for node in root.children.values():
        ds = node.ds.load()
        obs_project = node.attrs["observation_info"]["project_UID"]
        window = spectral.index(ds.frequency.attrs["spectral_window_name"])
        by_scan = (np.isin(scan, ds.scan_name.values.astype(int))
                   & (project[observation] == obs_project)
                   & (windows[description] == window))
        rows = np.flatnonzero(by_scan & np.isin(times_ms, millis(ds.time.values)))
        assert rows.size and not seen[rows].any()
        seen[rows] = True
        time_index = dict(zip(millis(ds.time.values), range(ds.sizes["time"]), strict=True))
        baseline_index = {(first, second): index for index, (first, second) in enumerate(
            zip(ds.baseline_antenna1_name.values, ds.baseline_antenna2_name.values, strict=True)
        )}
        t = np.array([time_index[times_ms[row]] for row in rows])
        b = np.array([baseline_index[labels[a1[row]], labels[a2[row]]] for row in rows])
        grid_rows = np.zeros((ds.sizes["time"], ds.sizes["baseline_id"]), dtype=bool)
        grid_rows[t, b] = True
        np.testing.assert_equal(np.count_nonzero(grid_rows), rows.size)
        np.testing.assert_array_equal(np.isfinite(ds.WEIGHT.values).any(axis=(2, 3)), grid_rows)
        assert ds.FLAG.values[~grid_rows].all()
        assert np.isnan(ds.VISIBILITY.values[~grid_rows]).all()
        for variable, expected in [("VISIBILITY", data), ("VISIBILITY_CORRECTED", corrected),
                                   ("VISIBILITY_MODEL", model)]:
            np.testing.assert_allclose(ds[variable].values[t, b], expected[rows], atol=1e-5, rtol=1e-6)
        np.testing.assert_array_equal(ds.FLAG.values[t, b], raw_flags[rows] | row_flags[rows, None, None])
        np.testing.assert_allclose(ds.WEIGHT.values[t, b],
                                   np.broadcast_to(weights[rows, None, :], data[rows].shape),
                                   atol=1e-5, rtol=1e-6)
        np.testing.assert_allclose(ds.UVW.values[t, b], uvw[rows], atol=1e-5, rtol=1e-6)
        np.testing.assert_allclose((xradio_time(ds.time)[t] - times[rows]).sec, 0.0, atol=2e-6, rtol=0)
        np.testing.assert_allclose((xradio_time(ds.TIME_CENTROID)[t, b] - centers[rows]).sec,
                                   0.0, atol=2e-6, rtol=0)
        np.testing.assert_allclose(ds.EFFECTIVE_INTEGRATION_TIME.values[t, b], exposure[rows],
                                   atol=1e-5, rtol=1e-6)
        np.testing.assert_allclose(ds.time.attrs["integration_time"]["data"], interval[rows],
                                   atol=1e-5, rtol=1e-6)
        np.testing.assert_array_equal(ds.field_name.values[t], fields[field[rows]])
        np.testing.assert_array_equal(ds.scan_name.values[t].astype(int), scan[rows])
        assert len(set(description[rows])) == 1
        pol = polarizations[pol_ids[description[rows[0]]]]
        np.testing.assert_array_equal(ds.polarization.values, [names[code] for code in pol])
        np.testing.assert_allclose(ds.frequency.values, frequencies[window], atol=0.01, rtol=0)
        assert ds.frequency.attrs["observer"].upper() == ("LSRK" if frames[window] == 1 else "TOPO")
        np.testing.assert_allclose(ds.frequency.attrs["channel_width"]["data"], widths[window],
                                   atol=0.01, rtol=0)
        assert ds.UVW.attrs["frame"] == "fk5"
        ants = node["antenna_xds"].ds.load()
        ids = [antenna_rows[name] for name in ants.antenna_name.values]
        np.testing.assert_allclose(ants.ANTENNA_POSITION, positions[ids], atol=1e-6, rtol=0)
        np.testing.assert_allclose(ants.ANTENNA_DISH_DIAMETER, diameters[ids], atol=1e-6, rtol=0)
        np.testing.assert_array_equal(ants.station_name, stations[ids])
        sky = node["field_and_source_base_xds"].ds.load()
        expected = np.asarray([directions[field_rows[name]].reshape(-1)[:2] for name in sky.field_name.values])
        np.testing.assert_allclose(sky.FIELD_PHASE_CENTER_DIRECTION, expected, atol=1e-10, rtol=0)
        actual_count += rows.size
    assert seen.all() and actual_count == len(time)
    print(f"XRADIO schema and {actual_count} CASA acquisition rows passed across {len(root.children)} datasets")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("casa", type=Path)
    parser.add_argument("zarr", type=Path)
    args = parser.parse_args()
    check(args.casa, args.zarr)
