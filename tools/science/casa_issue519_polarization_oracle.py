#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Regenerate T33's CASA polarized point-source oracle."""

import json
import math
import os
import shutil
import sys

from casatools import componentlist, ctsys, measures, ms, simulator, table
from casatasks import applycal
from casatasks.private.simutil import simutil


def casa_form_stokes(ms_path, selected_time, selected_antenna1, selected_antenna2):
    """Ask CASA to form I,Q,U,V and match the selected physical row."""
    mstool = ms()
    try:
        mstool.open(ms_path)
        mstool.selectinit(datadescid=0)
        mstool.selectpolarization(["I", "Q", "U", "V"])
        formed = mstool.getdata(["corrected_data", "antenna1", "antenna2", "time"])
    finally:
        mstool.close()
    rows = [
        index
        for index, (time, antenna1, antenna2) in enumerate(
            zip(formed["time"], formed["antenna1"], formed["antenna2"])
        )
        if time == selected_time
        and int(antenna1) == selected_antenna1
        and int(antenna2) == selected_antenna2
    ]
    if len(rows) != 1:
        raise RuntimeError(f"CASA Stokes conversion matched {len(rows)} rows")
    values = formed["corrected_data"][:, 0, rows[0]]
    return [[float(value.real), float(value.imag)] for value in values]


def casa_weight_flag_rules(flags, weights):
    """Apply source-pinned VisBuffer::formStokesWeightandFlag pairings."""
    paired_flags = [
        flags[0] or flags[3],
        flags[0] or flags[3],
        flags[1] or flags[2],
        flags[1] or flags[2],
    ]
    paired_weights = [
        weights[0] + weights[3],
        weights[0] + weights[3],
        weights[1] + weights[2],
        weights[1] + weights[2],
    ]
    return {
        "flags": [bool(value) for value in paired_flags],
        "weights": [float(value) for value in paired_weights],
        "weight_flag_origin": "CASA VisBuffer.cc::formStokesWeightandFlag",
    }


def main(output_root: str) -> None:
    os.makedirs(output_root, exist_ok=True)
    me = measures()
    config = ctsys.resolve("alma/simmos/vla.a.cfg")
    x, y, z, diameter, names, _, telescope, _ = simutil().readantenna(config)
    direction = me.direction("J2000", "12h00m00s", "+00d00m00s")
    results = {}
    for label, feed, correlations in [
        ("linear", "perfect X Y", "XX XY YX YY"),
        ("circular", "perfect R L", "RR RL LR LL"),
    ]:
        ms_path = os.path.join(output_root, f"{label}.ms")
        component_path = os.path.join(output_root, f"{label}.cl")
        for path in [ms_path, component_path]:
            if os.path.exists(path):
                shutil.rmtree(path)

        sm = simulator()
        sm.open(ms_path)
        sm.setconfig(
            telescopename=telescope,
            x=x,
            y=y,
            z=z,
            dishdiameter=diameter,
            mount=["alt-az"],
            antname=names,
            coordsystem="global",
            referencelocation=me.observatory(telescope),
        )
        sm.setfeed(mode=feed, pol=[""])
        sm.setspwindow(
            spwname="LBand",
            freq="1.4GHz",
            deltafreq="1MHz",
            freqresolution="1MHz",
            nchannels=1,
            stokes=correlations,
        )
        sm.setfield(sourcename="polarized", sourcedirection=direction)
        sm.setlimits(shadowlimit=0.0, elevationlimit="-89deg")
        sm.setauto(autocorrwt=0.0)
        sm.settimes(
            integrationtime="10s",
            usehourangle=True,
            referencetime=me.epoch("UTC", "2026/08/25/00:00:00"),
        )
        sm.observe(
            sourcename="polarized",
            spwname="LBand",
            starttime="1795s",
            stoptime="1805s",
        )
        sm.close()

        components = componentlist()
        components.addcomponent(
            dir="J2000 12h00m00s +00d00m00s",
            flux=[2.0, 0.4, -0.2, 0.1],
            fluxunit="Jy",
            polarization="Stokes",
            freq="1.4GHz",
            shape="point",
        )
        components.rename(component_path)
        components.done()
        sm = simulator()
        sm.openfromms(ms_path)
        sm.predict(complist=component_path, incremental=False)
        sm.close()
        applycal(vis=ms_path, gaintable=[], parang=True, applymode="calonly")

        tb = table()
        tb.open(ms_path)
        data = tb.getcol("CORRECTED_DATA")
        times = tb.getcol("TIME")
        antenna1 = tb.getcol("ANTENNA1")
        antenna2 = tb.getcol("ANTENNA2")
        tb.close()
        row = data.shape[2] // 2
        selected_data = data[:, 0, row]
        selected_flags = [False, True, False, False]
        selected_weights = [2.0, 0.5, 3.0, 1.25]

        def parallactic_angle(antenna):
            position = me.position(
                "ITRF", f"{x[antenna]}m", f"{y[antenna]}m", f"{z[antenna]}m"
            )
            latitude = me.measure(position, "WGS84")["m1"]["value"]
            me.doframe(position)
            me.doframe(me.epoch("UTC", f"{times[row]}s"))
            hadec = me.measure(direction, "HADEC")
            hour_angle = hadec["m0"]["value"]
            declination = hadec["m1"]["value"]
            return math.atan2(
                math.cos(latitude) * math.sin(hour_angle),
                math.sin(latitude) * math.cos(declination)
                - math.cos(latitude)
                * math.sin(declination)
                * math.cos(hour_angle),
            )

        angles = [
            parallactic_angle(int(antenna1[row])),
            parallactic_angle(int(antenna2[row])),
        ]
        selected_antenna1 = int(antenna1[row])
        selected_antenna2 = int(antenna2[row])
        results[label] = {
            "antenna_pair": [selected_antenna1, selected_antenna2],
            "correlations": correlations.split(),
            "data": [[float(value.real), float(value.imag)] for value in selected_data],
            "parallactic_angles_rad": angles,
            "operator_angles_rad": [-angle for angle in angles],
            "row_count": int(data.shape[2]),
            "weighted_flagged_correlations": {
                "flags": selected_flags,
                "weights": selected_weights,
            },
            "casa_visbuffer_form_stokes": {
                "coordinates": ["I", "Q", "U", "V"],
                "data_origin": "CASA ms.selectpolarization I,Q,U,V",
                "data": casa_form_stokes(
                    ms_path,
                    times[row],
                    selected_antenna1,
                    selected_antenna2,
                ),
                **casa_weight_flag_rules(selected_flags, selected_weights),
            },
        }

    oracle = {
        "schema": "casa-rs.issue519-polarization-oracle.v2",
        "casa_version": "6.7.6.14",
        "generator": "repo://tools/science/casa_issue519_polarization_oracle.py",
        "case": {
            "array": "VLA A, 27 antennas, 351 baselines",
            "source": "phase-centred point",
            "stokes_jy": [2.0, 0.4, -0.2, 0.1],
            "frequency_hz": 1.4e9,
            "hour_angle_seconds": 1800.0,
            "casa_operation": "applycal parang P correction",
            "comparison": "complex normalized RMS <= 0.001",
        },
        **results,
    }
    print(json.dumps(oracle, indent=2, sort_keys=True))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: casa_issue519_polarization_oracle.py OUTPUT_ROOT")
    main(sys.argv[1])
