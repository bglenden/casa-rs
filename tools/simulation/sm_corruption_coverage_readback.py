#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA reads native window-coverage products and actually consumes their tables."""
import argparse
import gzip
import json
from pathlib import Path
import numpy as np
import casatasks
from casatools import ms, simulator, table
from sm_apply_parameters_oracle import initialize
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session

FIXTURES={
    "plain":"sm_apply_existing_window_casa.json.gz",
    "noise":"sm_apply_later_window_noise_casa.json.gz",
    "selected":"sm_apply_selected_window_casa.json.gz",
    "repeat":"sm_apply_repeated_window_casa.json.gz",
    "unobserved":"sm_apply_unobserved_window_casa.json.gz",
    "overlap":"sm_apply_overlapping_terms_casa.json.gz",
}


def compare(path,reference,noise):
    measurement=ms();assert measurement.open(str(path));measurement.close()
    rows=read_rows(path);expected=reference["rows"];assert len(rows)==len(expected) and len(rows) in (144,180)
    tool=table();tool.open(str(path))
    times,windows=tool.getcol("TIME"),tool.getcol("DATA_DESC_ID");tool.close()
    error=power=native_noise=casa_noise=0.0
    for actual,wanted,time,window in zip(rows,expected,times,windows):
        assert float(time)==wanted["time_s"] and int(window)==wanted["window"]
        for key in ("antennas","field","flag_row","flags","weight","sigma","model_data"):
            assert actual[key]==wanted[key],(key,window)
        a=np.asarray(actual["data"]);b=np.asarray(wanted["data"])
        if noise and window==0:
            native_noise+=float(np.sum((a-[12,16])**2));casa_noise+=float(np.sum((b-[12,16])**2))
        else:
            error+=float(np.sum((a-b)**2));power+=float(np.sum(b*b))
        corrected=actual["data"] if wanted["corrected_data"]==wanted["data"] else wanted["corrected_data"]
        assert actual["corrected_data"]==corrected
    nrms=(error/power)**0.5;assert nrms<=1e-3
    result={"rows":len(rows),"data_nrms":nrms,"stored_time":"exact","quality_and_scratch":"preserved"}
    if noise:
        ratio=native_noise/casa_noise;assert 0.85<=ratio<=1.15,ratio
        result["noise_variance_ratio"]=ratio
    return result,np.asarray([row["data"] for row in rows])


def consume(directory,variant,out):
    out.mkdir(parents=True)
    path=out/"casa.ms";sm=make_session(path,stokes="XX XY YX YY")
    assert sm.setspwindow(spwname="later",freq="230GHz",deltafreq="15.625MHz",freqresolution="15.625MHz",refcode="TOPO",nchannels=4,stokes="XX XY YX YY")
    if variant!="unobserved":
        assert sm.observe(sourcename="target",spwname="later",starttime="3s",stoptime="13s")
    sm.close();initialize(path);sm=simulator();assert sm.openfromms(str(path))
    assert sm.setapply(table=str((directory/"g.cal").resolve()),interp="nearest",spwmap=[-1],calwt=False)
    if variant=="overlap":
        assert sm.setapply(table=str((directory/"g.cal").resolve()),interp="nearest",spwmap=[0],calwt=False)
    if variant=="noise":
        assert sm.setseed(5);assert sm.setnoise(simplenoise="0.1Jy")
    if variant=="selected":assert sm.setdata(fieldid=[0])
    assert sm.corrupt()
    if variant=="repeat":
        sm.reset();assert sm.setapply(table=str((directory/"second.cal").resolve()),interp="nearest",spwmap=[-1],calwt=False)
        assert sm.corrupt()
    sm.close();return path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--fixtures",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args();assert casatasks.version_string()=="6.7.6.14"
    references={}
    for name,filename in FIXTURES.items():
        with gzip.open(args.fixtures/filename,"rt") as handle:references[name]=json.load(handle)
    expected={(variant,workers) for variant in FIXTURES for workers in (1,3)};seen=set();reports=[];worker_data={}
    args.out.mkdir(parents=True)
    for manifest in sorted(args.root.glob("*/case.json")):
        case=json.loads(manifest.read_text());variant=case["variant"];key=(variant,case["workers"])
        assert key in expected and key not in seen;seen.add(key)
        reference=references[variant];assert reference["casa_version"]=="6.7.6.14"
        native,values=compare(manifest.parent/"applied.ms",reference,variant=="noise")
        worker_data[key]=values
        report={**case,"native_ms":native}
        if case["workers"]==1:
            output=consume(manifest.parent,variant,args.out/variant)
            report["casa_consumes_native_tables"]=compare(output,reference,variant=="noise")[0]
        reports.append(report)
    assert seen==expected,("missing",expected-seen)
    workers={}
    for variant in FIXTURES:
        a,b=(worker_data[(variant,w)] for w in (1,3))
        difference=float(np.max(np.linalg.norm(a-b,axis=-1))/np.max(np.linalg.norm(a,axis=-1)))
        assert difference<=1e-4,(variant,difference)
        workers[variant]={"max_relative":difference,"limit":1e-4}
    (args.out/"report.json").write_text(json.dumps({"casa_version":"6.7.6.14","runs":reports,"worker_comparison":workers,"native_ms_reads":len(reports),"table_applications":sum(2 if variant in ("repeat","overlap") else 1 for variant in FIXTURES)},indent=1)+"\n")
    print(f"C++ reads{len(reports)} native MSs and consumes8 native calibration tables across{len(FIXTURES)} scenarios",flush=True)


if __name__=="__main__":main()
