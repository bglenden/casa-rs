# SPDX-License-Identifier: LGPL-3.0-or-later
"""XRADIO side of the MSv2/MSv4 trial's interoperability check
(docs/measurement-set/trial/README.md). Python here is only the oracle.

    check_interop.py rust-read --store XRADIO.ps.zarr --dump DUMP_DIR
        Rust read XRADIO's store (`ms_trial_interop dump`); compare every array
        and coordinate it delivered with XRADIO's own load of the store.

    check_interop.py rust-written --store RUST.ps.zarr --reference XRADIO.ps.zarr --ms MS
        Rust wrote a fresh store (`ms_trial_interop write`); open it with XRADIO,
        run XRADIO's schema check, force-load every array, and compare it with
        XRADIO's conversion of the same MSv2 and with the MSv2 itself (read
        through casacore with arcae).

Each prints a JSON report and exits non-zero on any mismatch.
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
from arcae.lib.arrow_tables import Table
from xradio.measurement_set import open_processing_set
from xradio.schema.check import check_datatree

RESULTS = []


def record(check, ok, detail=None):
    RESULTS.append({"check": check, "ok": bool(ok), **({"detail": detail} if detail else {})})


def exact(check, expected, actual):
    expected, actual = np.asarray(expected), np.asarray(actual)
    same_shape = expected.shape == actual.shape
    equal = same_shape and np.array_equal(
        expected, actual, equal_nan=np.issubdtype(expected.dtype, np.inexact)
    )
    record(
        check,
        equal,
        None
        if equal
        else {"expected_shape": expected.shape, "actual_shape": actual.shape},
    )


def dataset(ps):
    name = sorted(ps.children)[0]
    return name, ps[name]


def rust_read(store, dump):
    manifest = json.loads((dump / "manifest.json").read_text())
    _, node = dataset(open_processing_set(str(store)))
    ds = node.ds.load()
    for name, spec in manifest["arrays"].items():
        values = np.fromfile(dump / f"{name}.bin", dtype=spec["dtype"]).reshape(spec["shape"])
        reference = ds[name].values
        exact(f"rust read {name} equals XRADIO load", reference, values)
    exact("polarization", ds.polarization.values, manifest["polarization"])
    exact("baseline_antenna1_name", ds.baseline_antenna1_name.values, manifest["baseline_antenna1_name"])
    exact("baseline_antenna2_name", ds.baseline_antenna2_name.values, manifest["baseline_antenna2_name"])
    exact("field_name per time", ds.field_name.values, manifest["field_name"])
    record("time origin is the Unix epoch, MJD 40587", manifest["time_origin_mjd"] == 40587.0)
    record(
        "time scale",
        manifest["time_scale"] == ds.time.attrs["scale"].upper(),
        {"xradio": ds.time.attrs["scale"], "rust": manifest["time_scale"]},
    )
    record(
        "frequency frame",
        manifest["frequency_frame"] == ds.frequency.attrs["observer"].upper(),
        {"xradio": ds.frequency.attrs["observer"], "rust": manifest["frequency_frame"]},
    )
    frames = {"fk5": "J2000", "icrs": "ICRS"}
    record(
        "UVW frame",
        manifest["uvw_frame"] == frames[ds.UVW.attrs["frame"]],
        {"xradio": ds.UVW.attrs["frame"], "rust": manifest["uvw_frame"]},
    )
    record(
        "visibility units kept as declared, not taken for Jy",
        manifest["units"] == f'Declared("{ds.VISIBILITY.attrs["units"]}")',
        {"xradio": ds.VISIBILITY.attrs["units"], "rust": manifest["units"]},
    )
    record(
        "data group",
        manifest["data_group"] in node.attrs["data_groups"],
        {"groups": list(node.attrs["data_groups"])},
    )
    exact(
        "channel width",
        ds.frequency.attrs["channel_width"]["data"],
        manifest["channel_width_hz"][0],
    )
    fields = node["field_and_source_base_xds"].ds.load()
    for entry in manifest["field_phase_centre"]:
        exact(
            f"phase centre of {entry['name']}",
            fields.FIELD_PHASE_CENTER_DIRECTION.sel(field_name=entry["name"]).values,
            entry["rad"],
        )
    antennas = node["antenna_xds"].ds.load()
    exact("antenna names", antennas.antenna_name.values, [a["name"] for a in manifest["antennas"]])
    exact("antenna positions", antennas.ANTENNA_POSITION.values, [a["position_m"] for a in manifest["antennas"]])


def rust_written(store, reference, ms):
    ps = open_processing_set(str(store))
    issues = check_datatree(ps)
    record("XRADIO schema check of the Rust store", len(issues) == 0, {"issues": str(issues)} if issues else None)
    loaded = ps.load()
    name, node = dataset(loaded)
    _, expected_node = dataset(open_processing_set(str(reference)).load())
    ds, expected = node.ds, expected_node.ds
    record("dataset type and schema version", (node.attrs["type"], node.attrs["schema_version"]) == ("visibility", "4.0.1"))
    for var in ["VISIBILITY", "FLAG", "WEIGHT", "UVW"]:
        exact(f"{var} equals XRADIO's conversion", expected[var].values, ds[var].values)
        record(
            f"{var} dims and dtype",
            ds[var].dims == expected[var].dims and ds[var].dtype == expected[var].dtype,
            {"rust": [list(ds[var].dims), str(ds[var].dtype)], "xradio": [list(expected[var].dims), str(expected[var].dtype)]},
        )
    for coord in ["time", "baseline_id", "frequency", "polarization", "baseline_antenna1_name", "baseline_antenna2_name", "field_name", "scan_name", "uvw_label"]:
        exact(f"coordinate {coord}", expected[coord].values, ds[coord].values)
    for attr in ["type", "units", "scale", "format", "integration_time"]:
        record(f"time.{attr}", ds.time.attrs.get(attr) == expected.time.attrs.get(attr), {"rust": ds.time.attrs.get(attr), "xradio": expected.time.attrs.get(attr)})
    for attr in ["type", "units", "observer", "spectral_window_name", "channel_width", "reference_frequency"]:
        record(f"frequency.{attr}", ds.frequency.attrs.get(attr) == expected.frequency.attrs.get(attr), {"rust": ds.frequency.attrs.get(attr), "xradio": expected.frequency.attrs.get(attr)})
    for attr in ["type", "units", "frame"]:
        record(f"UVW.{attr}", ds.UVW.attrs.get(attr) == expected.UVW.attrs.get(attr), {"rust": ds.UVW.attrs.get(attr), "xradio": expected.UVW.attrs.get(attr)})
    record("VISIBILITY.units", ds.VISIBILITY.attrs.get("units") == expected.VISIBILITY.attrs.get("units"), {"rust": ds.VISIBILITY.attrs.get("units")})
    record("scan_name.scan_intents", ds.scan_name.attrs.get("scan_intents") == expected.scan_name.attrs.get("scan_intents"))
    roles = ["correlated_data", "flag", "weight", "uvw", "field_and_source"]
    record(
        "data group base roles",
        {r: node.attrs["data_groups"]["base"][r] for r in roles} == {r: expected_node.attrs["data_groups"]["base"][r] for r in roles},
    )
    for sub, variables in [
        ("antenna_xds", ["antenna_name", "station_name", "mount", "telescope_name", "receptor_label", "polarization_type", "ANTENNA_POSITION", "ANTENNA_DISH_DIAMETER", "ANTENNA_RECEPTOR_ANGLE"]),
        ("field_and_source_base_xds", ["field_name", "source_name", "sky_dir_label", "FIELD_PHASE_CENTER_DIRECTION"]),
    ]:
        mine, theirs = node[sub].ds, expected_node[sub].ds
        for var in variables:
            exact(f"{sub}/{var}", theirs[var].values, mine[var].values)
        for var in ["ANTENNA_POSITION", "FIELD_PHASE_CENTER_DIRECTION"]:
            if var in theirs:
                keys = ["type", "units", "frame", "coordinate_system", "origin_object_name"]
                record(f"{sub}/{var} measure attributes", {k: mine[var].attrs.get(k) for k in keys} == {k: theirs[var].attrs.get(k) for k in keys})
    missing = sorted(set(expected.data_vars) - set(ds.data_vars))
    record("variables XRADIO wrote that the trial writer does not", True, {"not written": missing})

    # Against the MSv2 itself, independently of either Rust reader or XRADIO.
    main = Table.from_filename(str(ms))
    data = np.asarray(main.getcol("DATA"))
    flag = np.asarray(main.getcol("FLAG"))
    weight = np.asarray(main.getcol("WEIGHT"))
    uvw = np.asarray(main.getcol("UVW"))
    time = np.asarray(main.getcol("TIME")) - 40587.0 * 86400.0
    names = np.asarray(Table.from_filename(str(ms) + "::ANTENNA").getcol("NAME"))
    stations = np.asarray(Table.from_filename(str(ms) + "::ANTENNA").getcol("STATION"))
    label = [f"{n}_{s}" for n, s in zip(names, stations)]
    a1 = [label[i] for i in np.asarray(main.getcol("ANTENNA1"))]
    a2 = [label[i] for i in np.asarray(main.getcol("ANTENNA2"))]
    t_index = {t: i for i, t in enumerate(ds.time.values)}
    b_index = {(p, q): i for i, (p, q) in enumerate(zip(ds.baseline_antenna1_name.values, ds.baseline_antenna2_name.values))}
    rows = [(t_index[t], b_index[(p, q)]) for t, p, q in zip(time, a1, a2)]
    t, b = np.array(rows).T
    exact("every MSv2 row has its own slot", len(set(rows)), len(rows))
    exact("VISIBILITY equals MSv2 DATA", data, ds.VISIBILITY.values[t, b])
    exact("FLAG equals MSv2 FLAG", flag, ds.FLAG.values[t, b])
    exact("WEIGHT equals MSv2 WEIGHT broadcast", np.broadcast_to(weight[:, None, :], data.shape), ds.WEIGHT.values[t, b])
    exact("UVW equals MSv2 UVW", uvw, ds.UVW.values[t, b])
    record("every slot is an MSv2 row (no padding)", len(rows) == ds.time.size * ds.baseline_id.size)
    meta = json.loads((Path(store) / name / "VISIBILITY" / "zarr.json").read_text())
    record("VISIBILITY layout", True, {"chunks": meta["chunk_grid"]["configuration"]["chunk_shape"], "codecs": [c["name"] for c in meta["codecs"]]})


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    read = sub.add_parser("rust-read")
    read.add_argument("--store", type=Path, required=True)
    read.add_argument("--dump", type=Path, required=True)
    written = sub.add_parser("rust-written")
    written.add_argument("--store", type=Path, required=True)
    written.add_argument("--reference", type=Path, required=True)
    written.add_argument("--ms", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "rust-read":
        rust_read(args.store, args.dump)
    else:
        rust_written(args.store, args.reference, args.ms)
    failed = [r for r in RESULTS if not r["ok"]]
    print(json.dumps({"mode": args.mode, "checks": len(RESULTS), "failed": len(failed), "results": RESULTS}, indent=1, default=str))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
