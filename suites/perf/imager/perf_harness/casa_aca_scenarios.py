#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Checked-in CASA ACA and simalma scenario protocol."""

import json
import pathlib
import shutil
import sys
import time

from casatasks import feather as casa_feather
from casatasks import simalma, simanalyze, simobserve
from casatasks.private import task_simanalyze

task_simanalyze.feather = casa_feather

request_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
request = json.loads(request_path.read_text(encoding="utf-8"))
scenario = request["scenario"]
staged = request["staged"]

if scenario == "simalma":
    work = pathlib.Path.cwd()
    model = pathlib.Path(staged["inputs"]["m51ha_fits"]["path"])
    local_model = work / "M51ha.fits"
    if not local_model.exists():
        shutil.copy2(model, local_model)

    started = time.perf_counter()
    simalma(
        project="m51",
        overwrite=True,
        skymodel=str(local_model),
        indirection="J2000 23h59m59.96s -34d59m59.50s",
        incell="0.1arcsec",
        inbright="0.004",
        incenter="330.076GHz",
        inwidth="50MHz",
        antennalist=[
            staged["configs"]["alma.cycle6.3.cfg"]["path"],
            staged["configs"]["aca.cycle6.cfg"]["path"],
        ],
        totaltime="1800s",
        tpnant=2,
        tptime="7200s",
        pwv=0.6,
        imsize=[128, 128],
        mapsize="1arcmin",
        dryrun=False,
    )
    elapsed = time.perf_counter() - started
    outputs = sorted(str(path.relative_to(work)) for path in work.glob("m51*"))
    result = {"scenario": "simalma", "elapsed_seconds": elapsed, "outputs": outputs}
elif scenario == "aca":
    work = pathlib.Path.cwd()
    model = staged["inputs"]["m51ha_model"]["path"]
    project = "m51c"

    started = time.perf_counter()
    simobserve(
        project=project,
        skymodel=model,
        inbright="0.004",
        indirection="B1950 23h59m59.96 -34d59m59.50",
        incell="0.1arcsec",
        incenter="330.076GHz",
        inwidth="50MHz",
        setpointings=True,
        integration="10s",
        mapsize="1arcmin",
        maptype="hex",
        pointingspacing="9arcsec",
        obsmode="int",
        refdate="2012/11/21/20:00:00",
        totaltime="3600s",
        antennalist="alma;0.5arcsec",
        thermalnoise="",
        graphics="file",
        verbose=True,
        overwrite=True,
    )
    simobserve(
        project=project,
        skymodel=model,
        inbright="0.004",
        indirection="B1950 23h59m59.96 -34d59m59.50",
        incell="0.1arcsec",
        incenter="330.076GHz",
        inwidth="50MHz",
        setpointings=True,
        integration="10s",
        mapsize="1arcmin",
        maptype="square",
        pointingspacing="9arcsec",
        obsmode="sd",
        refdate="2012/11/21/20:00:00",
        totaltime="2h",
        sdantlist=staged["configs"]["aca.tp.cfg"]["path"],
        sdant=0,
        thermalnoise="",
        graphics="file",
        verbose=True,
        overwrite=True,
    )
    simobserve(
        project=project,
        skymodel=model,
        inbright="0.004",
        indirection="B1950 23h59m59.96 -34d59m59.50",
        incell="0.1arcsec",
        incenter="330.076GHz",
        inwidth="50MHz",
        setpointings=True,
        integration="10s",
        mapsize="1arcmin",
        maptype="hex",
        pointingspacing="15arcsec",
        obsmode="int",
        refdate="2012/11/21/20:00:00",
        totaltime="3",
        antennalist=staged["configs"]["aca.i.cfg"]["path"],
        thermalnoise="",
        graphics="file",
        verbose=True,
        overwrite=True,
    )
    simanalyze(
        project=project,
        vis="$project.aca.i.ms,$project.aca.tp.sd.ms",
        image=True,
        imsize=[512, 512],
        cell="0.2arcsec",
        modelimage="$project.sd.image",
        analyze=True,
        showpsf=False,
        showresidual=False,
        showconvolved=True,
        graphics="file",
    )
    simanalyze(
        project=project,
        vis="$project.alma_0.5arcsec.ms",
        image=True,
        imsize=[512, 512],
        cell="0.2arcsec",
        modelimage="$project.aca.i.image",
        analyze=True,
        showpsf=False,
        showresidual=False,
        showconvolved=True,
        graphics="file",
    )
    elapsed = time.perf_counter() - started
    outputs = sorted(str(path.relative_to(work)) for path in work.rglob("m51c*"))
    result = {"scenario": "aca", "elapsed_seconds": elapsed, "outputs": outputs}
else:
    raise ValueError(f"unsupported ACA scenario: {scenario!r}")

output_path.write_text(
    json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
