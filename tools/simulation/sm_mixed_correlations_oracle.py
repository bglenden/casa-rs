# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulation prediction across different window correlation setups."""
import argparse
from pathlib import Path
import shutil
import numpy as np

from casatasks import version_string
from casatools import componentlist, simulator, table
from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session

CASES = {
    "reversed": {"stokes": "YY XX"},
    "circular": {"stokes": "RR LL"},
    "full_linear": {"stokes": "XX XY YX YY"},
    "full_circular": {"stokes": "RR RL LR LL"},
    "selected_full": {"stokes": "XX XY YX YY", "selected": True},
    "components": {"stokes": "XX XY YX YY", "components": True},
    "alias_reversed": {"stokes": "YY XX", "components": True, "aliases": True},
    "alias_full": {"stokes": "XX XY YX YY", "components": True, "aliases": True},
    "alias_selected": {"stokes": "XX XY YX YY", "components": True, "aliases": True, "selected": True},
    "fresh_linear": {"stokes": "XX XY YX YY", "components": True, "fresh": True},
    "fresh_circular": {"stokes": "RR RL LR LL", "channels": 2, "fresh": True},
    "fresh_selected": {"stokes": "XX XY YX YY", "channels": 2, "selected": True, "fresh": True},
}

def single_window_control(root, spec, kind="joint"):
    """Independent CASA prediction with a fresh polarization map per window."""
    path=root/(kind+"-polarization-control.ms")
    shutil.copytree(root/"inputs/base.ms",path,ignore=shutil.ignore_patterns("table.lock"))
    arguments={} if kind=="components" else dict(imagename=str(root/"model.image"))
    if spec.get("components") and kind!="image":arguments["complist"]=str(root/"point.cl")
    for window in ([1] if spec.get("selected") else [0,1]):
        sm=simulator();assert sm.openfromms(str(path))
        if spec.get("aliases"):
            assert sm.setdata(spwid=[],fieldid=[],msselect=f"DATA_DESC_ID == {window}")
        else:
            assert sm.setdata(spwid=[window],fieldid=[])
        assert sm.predict(**arguments)
        sm.close()
    return snapshot(path)["rows"]

def add_science(images, components, selected=None):
    """The already-documented flagged-component defect is corrected by I+C."""
    import copy
    rows=copy.deepcopy(images)
    for row,image,component in zip(rows,images,components):
        if selected is not None and row["data_desc_id"] != selected:
            continue
        for name in ("data","corrected_data"):
            def complex32(values):
                a=np.asarray(values,dtype=np.float32)
                return a[:,0]+np.complex64(1j)*a[:,1]
            row[name]=[[float(v.real),float(v.imag)]
                       for v in complex32(image[name])+complex32(component[name])]
    return rows

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--fixture",type=Path,required=True)
    parser.add_argument("--case",choices=CASES,required=True)
    args=parser.parse_args()
    assert version_string()=="6.7.6.14"
    root=args.out.resolve();root.mkdir(parents=True)
    spec=CASES[args.case]
    path=root/"predicted.ms"
    sm=make_session(path)
    assert sm.setspwindow(spwname="mixed",freq="230GHz",deltafreq="15.625MHz",
        freqresolution="15.625MHz",refcode="TOPO",nchannels=spec.get("channels",4),stokes=spec["stokes"])
    assert sm.observe(sourcename="target",spwname="mixed",starttime="3s",stoptime="23s")
    assert sm.observe(sourcename="offset",spwname="mixed",starttime="23s",stoptime="43s")
    sm.close()
    if spec.get("aliases"):
        tb=table();tb.open(str(path/"DATA_DESCRIPTION"),nomodify=False)
        tb.putcell("SPECTRAL_WINDOW_ID",1,0)
        tb.close()
    tb=table();tb.open(str(path),nomodify=False)
    for row in range(tb.nrows()):
        shape=tb.getcell("DATA",row).shape
        for column,value in (("DATA",3+4j),("MODEL_DATA",7-2j),("CORRECTED_DATA",-5+6j)):
            # Define CASA's scratch cells before copying/reopening controls.
            # Fresh native simulations retain their approved DATA-only form.
            tb.putcell(column,row,np.full(shape,0 if spec.get("fresh") else value,dtype=np.complex64))
        if spec.get("fresh"):
            continue
        flags=np.zeros(shape,dtype=bool)
        if row%11==2:flags[0,0]=True
        tb.putcell("FLAG",row,flags)
        tb.putcell("FLAG_ROW",row,row%19==4)
    tb.close()
    before=snapshot(path)
    inputs=root/"inputs";inputs.mkdir()
    shutil.copytree(path,inputs/"base.ms",ignore=shutil.ignore_patterns("table.lock"))
    sm=simulator();assert sm.openfromms(str(path))
    model=root/"model.image"
    source=Path(__file__).resolve().parents[2]/"crates/casa-simulation/tests/data/sm_image_layouts/iquv.image"
    shutil.copytree(source,model,ignore=shutil.ignore_patterns("table.lock"))
    if spec.get("selected"):
        if spec.get("aliases"):
            assert sm.setdata(spwid=[],fieldid=[],msselect="DATA_DESC_ID == 1")
        else:
            assert sm.setdata(spwid=[1],fieldid=[])
    arguments=dict(imagename=str(model))
    if spec.get("components"):
        cl=componentlist()
        assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad",flux=[.25,.05,-.02,.01],freq="230GHz",polarization="Stokes")
        component=root/"point.cl";cl.rename(str(component));cl.done()
        arguments["complist"]=str(component)
    returned=bool(sm.predict(**arguments))
    case=dict(name=args.case,returned=returned,spec=spec,all_fields_selected=True,
        before=before,after=snapshot(path))
    if spec.get("fresh"):case["native_created"]=True
    sm.close()
    case["polarization_map_corrected_rows"]=single_window_control(root,spec)
    if spec.get("components"):
        case["single_window_joint_rows"]=case["polarization_map_corrected_rows"]
        case["image_only_rows"]=single_window_control(root,spec,"image")
        case["component_only_rows"]=single_window_control(root,spec,"components")
        case["polarization_map_corrected_rows"]=add_science(case["image_only_rows"],case["component_only_rows"],
            1 if spec.get("selected") else None)
    write(args.fixture,dict(casa_version=version_string(),cases=[case]))
    print(args.case,"returned",returned,flush=True)

if __name__=="__main__":main()
