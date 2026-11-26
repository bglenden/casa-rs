"""CASA tool signatures and implementation status for `casars.tools` (ADR-0017).

Each class has every method of CASA 6.7.6.14's installed binding, with its
signature (`data/casa_<tool>_signatures.json`, written by
`tools/simulation/dump_casa_tool_signatures.py`, and a live CASA when one is
importable), and the object-surface bundle describes the same methods. A
method casa-rs does not implement yet raises `NotImplementedError` naming the
issue that records it. `IMPLEMENTED` grows as #1025's parts land; each
tool's own test file checks what an implemented method does.
"""

from __future__ import annotations

import inspect

import pytest

import casars
from casars import tools

from _casa_tools import (
    assert_bundle_describes,
    assert_casa_signatures,
    assert_live_signatures,
    casa_signatures,
)

#: Each tool, and the issue its refused methods name.
ISSUES = {"image": 1027, "coordsys": 1027, "table": 1028, "msmetadata": 1029, "ms": 1030,
          "calibrater": 1528, "calanalysis": 1529, "agentflagger": 1527, "wvr": 1531, "singledishms": 1532, "atmosphere": 1533}

#: The methods each tool implements.
IMPLEMENTED: dict[str, set[str]] = {tool: set() for tool in ISSUES}
IMPLEMENTED["calanalysis"] = {
    "antenna", "antenna1", "antenna2", "calname", "close", "feed", "field", "fit", "freq", "get",
    "msname", "numantenna", "numantenna1", "numantenna2", "numchannel", "numfeed",
    "numfield", "numspw", "numtime", "open", "partype", "polbasis", "spw", "time", "viscal",
}
IMPLEMENTED["msmetadata"] = {
    "open", "close", "done", "name", "nantennas", "nfields", "nobservations",
    "nstates", "fieldnames", "observatorynames", "observers", "projects",
    "nchan", "chanfreqs", "chanwidths", "meanfreq", "sideband", "reffreq",
    "datadescids", "spwfordatadesc", "polidfordatadesc", "corrtypesforpol",
    "corrprodsforpol", "ncorrforpol", "wvrspws", "nspw",
    "antennanames", "namesforfields", "namesforspws", "bandwidths", "sourceidforfield",
    "sourceidsfromsourcetable", "sourcenames", "nsources",
    "almaspws", "baseband", "chanavgspws", "chaneffbws", "chanres", "corrbit", "fdmspws",
    "rxbands", "spwsforbaseband", "subwindows", "tdmspws",
    "narrays", "nscans", "scannumbers", "fieldsforname", "fieldsforscan", "fieldsforscans",
    "scansforfield", "scansforfields", "antennasforscan", "nbaselines", "baselines",
    "fieldsforspw", "spwsforfield", "spwsforfields", "spwsfornames", "spwsforscan",
    "spwsforscans", "scansforspw", "scansforspws", "fieldsfortimes", "scansfortimes",
    "timesforfield", "timesforscan", "timesforscans", "timesforspws", "scansforstate",
    "statesforscan", "statesforscans",
    "intents", "intentsforfield", "intentsforscan", "intentsforspw",
    "fieldsforintent", "scansforintent", "spwsforintent", "timesforintent",
    "fieldsforsource", "fieldsforsources", "sourcedirs", "sourcetimes",
    "propermotions", "restfreqs", "transitions",
    "antennadiameter", "antennaids", "antennaoffset", "antennaposition",
    "antennastations", "observatoryposition", "schedule", "timerangeforobs",
    "phasecenter", "refdir", "pointingdirection",
    "nrows", "effexposuretime", "exposuretime", "summary",
}
IMPLEMENTED["atmosphere"] = set(casa_signatures("atmosphere")["methods"])
IMPLEMENTED["wvr"] = {"gcal"}
IMPLEMENTED["singledishms"] = {
    "importasap", "importnro", "atmcor", "fit_line",
    "open", "close", "done", "name", "set_selection", "smooth",
    "subtract_baseline", "subtract_baseline_cspline", "subtract_baseline_sinusoid",
    "subtract_baseline_variable", "apply_baseline_table",
}
IMPLEMENTED["ms"] = {
    "addephemeris", "close", "concatenate", "continuumsub", "continuumsubold", "contsub", "createmultims", "cvel", "cvelfreqs", "done", "getdata", "getdataold", "getfielddirmeas", "getreferencedtables", "getspectralwindowinfo",
    "getscansummary", "hanningsmooth", "ismultims", "iswritable",
    "iterend", "iterendold", "iterinit", "iterinitold", "iternext", "iternextold",
    "iterorigin", "iteroriginold", "listfits", "listhistory", "lister",
    "metadata", "msselect", "msselectedindices", "msseltoindex", "name",
    "ngetdata", "niterend", "niterinit", "niternext", "niterorigin", "nrow", "nrowold",
    "open", "partition", "putdata", "putdataold", "range", "rangeold", "regridspw", "reset", "select", "selectinit", "selectinitold", "selectold",
    "selectchannel", "selectchannelold", "selectpolarization", "selectpolarizationold", "selecttaql", "sort", "split",
    "statistics", "statisticsold", "statwt", "summary", "testconcatenate", "timesort", "uvsub", "virtconcatenate",
    "writehistory", "writehistory_batch",
}
IMPLEMENTED["agentflagger"] = {
    "open", "done", "selectdata", "init", "run", "printflagselection", "parseagentparameters",
    "parsemanualparameters", "parseclipparameters", "parsequackparameters", "parseelevationparameters",
    "parsetfcropparameters", "parseantintparameters", "parseextendparameters", "parsesummaryparameters",
    "getflagversionlist", "saveflagversion", "restoreflagversion", "deleteflagversion",
}
IMPLEMENTED["calibrater"] = {
    "setsolvebandpoly", "setsolvegainspline",
    "open", "close", "done", "setvi", "selectvis", "reset",
    "initcalset", "delmod", "setptmodel", "modelfit", "initweights",
    "accumulate", "createcaltable", "updatecaltable", "smooth", "rerefant", "listcal", "plotcal",
    "linpolcor", "parsecallibfile", "validatecallib", "setcorrdepflags", "fluxscale", "specifycal", "state",
}
PARTIAL = {
    "ms": {"selecttaqlold"},
    "calibrater": {"setmodel", "setsolve", "posangcal", "setapply", "setcallib", "returndict",
                   "correct", "corrupt", "activityrec", "solve"},
}

IMPLEMENTED["coordsys"] = {
    "convert", "convertdirection", "convertmany", "reorder", "replace",
    "setconversiontype", "setdirection", "setprojection",
    "setreferencecode", "setspectral", "settabular", "subimage", "transpose",
    "setlineartransform",
    "frequencytofrequency", "frequencytovelocity", "velocitytofrequency",
    "increment", "referencevalue",
    "addcoordinate", "axesmap", "axiscoordinatetypes", "conversiontype", "coordinatetype", "copy", "done", "epoch",
    "findaxis", "findaxisbyname", "findcoordinate", "fromrecord", "getconversiontype", "lineartransform",
    "names", "naxes", "ncoordinates", "newcoordsys",
    "observer", "parentname", "projection", "referencecode", "referencepixel", "restfrequency",
    "setepoch", "setincrement", "setreferencevalue",
    "setnames", "setobserver", "setparentname", "setstokes", "setunits",
    "setreferencelocation", "setreferencepixel", "setrestfrequency", "settelescope",
    "stokes", "summary", "telescope", "toabs", "toabsmany", "topixel", "topixelmany", "torecord", "torel",
    "torelmany", "toworld", "toworldmany", "type", "units",
}
IMPLEMENTED["image"] = {
    "fromimage", "imageconcat", "newimage", "newimagefromfile", "newimagefromimage", "open",
    "boxcar", "collapse", "continuumsub", "convolve", "convolve2d", "decompose", "deviation", "fft", "findsources", "fitcomponents", "fitprofile", "hanning", "histograms",
    "maxfit",
    "moments",
    "pad", "pbcor", "pv", "regrid", "rotate", "sepconvolve", "transpose", "twopointcorrelation",
    "getprofile", "getslice",
    "crop",
    "decimate",
    "rebin",
    "boundingbox",
    "insert",
    "addnoise",
    "coordmeasures", "topixel", "toworld",
    "adddegaxes",
    "statistics",
    "beamarea", "beamforconvolvedsize", "brightnessunit", "calc", "calcmask", "close", "commonbeam", "convertflux", "coordsys", "deconvolvefrombeam", "dohistory", "done", "fitsheader", "fromarray", "fromcomplist", "fromfits", "fromshape",
    "getchunk", "getregion", "haslock", "history", "imagecalc", "isconform", "isopen", "ispersistent", "lock", "name", "pixeltype", "putchunk", "remove", "removefile", "rename", "restoringbeam", "unlock",
    "newimagefromarray", "newimagefromfits", "newimagefromshape",
    "deconvolvecomponentlist", "fromrecord", "makecomplex", "modify", "replacemaskedpixels", "torecord",
    "makearray", "maketestimage", "maskhandler", "miscinfo", "pixelvalue", "rotatebeam", "setbrightnessunit", "setcoordsys", "sethistory", "setmiscinfo", "setrestoringbeam", "shape", "subimage", "type",
    "summary", "set", "tofits", "putregion",
}
IMPLEMENTED["table"] = {
    "fromASDM", "fromascii", "fromfits", "toasciifmt",
    "open", "close", "done", "ok", "name", "nrows", "ncols", "colnames", "iswritable", "info",
    "lockoptions", "haslock", "lock", "unlock", "isscalarcol", "isvarcol", "coldatatype",
    "colarraytype", "getcell", "getcol", "getvarcol", "getcolshapestring", "iscelldefined",
    "keywordnames", "getkeywords", "getkeyword", "fieldnames", "colkeywordnames",
    "getcolkeywords", "getcolkeyword", "colfieldnames",
    "putkeyword", "putkeywords", "removekeyword", "putcolkeyword", "putcolkeywords",
    "removecolkeyword", "putinfo", "addreadmeline", "putcell", "putcol", "putvarcol",
    "getcellslice", "getcolslice", "putcellslice", "putcolslice", "getcoldesc",
    "addrows", "removerows", "renamecol", "removecols", "addcols", "getdminfo",
    "endianformat", "flush", "resync", "datachanged", "ismultiused", "setmaxcachesize",
    "clearlocks", "copyrows", "copy", "create", "createmultitable", "rownumbers", "getdesc",
    "isopened", "showcache", "listlocks", "testincrstman", "statistics", "selectrows",
    "summary", "browse", "getcoliter", "row", "query", "taql", "calc",
}  # fmt: skip


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_the_class_is_casas(tool: str) -> None:
    cls = getattr(tools, tool)
    assert cls is getattr(casars._core, tool)
    assert cls.__name__ == tool
    assert cls.__module__ == "casars.tools"
    assert tool in tools.__all__


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_every_casa_method_exists_with_casas_signature(tool: str) -> None:
    assert_casa_signatures(getattr(tools, tool), casa_signatures(tool))


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_signatures_match_a_live_casa(tool: str) -> None:
    casatools = pytest.importorskip("casatools")
    assert_live_signatures(getattr(tools, tool), getattr(casatools, tool))


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_the_bundle_describes_each_method_and_what_casa_rs_does_with_it(tool: str) -> None:
    methods = set(casa_signatures(tool)["methods"])
    partial = PARTIAL.get(tool, set())
    assert_bundle_describes(
        getattr(tools, tool),
        tool,
        {
            "implemented": IMPLEMENTED[tool],
            "partial": partial,
            "not_yet_implemented": methods - IMPLEMENTED[tool] - partial,
        },
    )


@pytest.mark.parametrize("tool", sorted(ISSUES))
def test_a_method_casa_rs_lacks_names_its_issue(tool: str) -> None:
    cls = getattr(tools, tool)
    instance = cls()
    missing = set(casa_signatures(tool)["methods"]) - IMPLEMENTED[tool] - PARTIAL.get(tool, set())
    for name in sorted(missing):
        required = [
            parameter.name
            for parameter in inspect.signature(getattr(cls, name)).parameters.values()
            if parameter.name != "self" and parameter.default is inspect.Parameter.empty
        ]
        issue = ISSUES[tool]
        with pytest.raises(NotImplementedError, match=rf"^{tool}\.{name} .*\(#{issue}\)$"):
            getattr(instance, name)(**{parameter: None for parameter in required})


def test_calibrater_method_returns_before_open() -> None:
    instance = tools.calibrater()
    assert instance.close() is True
    assert instance.done() is True
    assert instance.setvi(old=True, quiet=True) is True
    assert instance.selectvis() is False
    assert instance.reset() is False
    assert instance.state() is False
    assert instance.setcorrdepflags(True) is True
    assert instance.solve() is False
    assert instance.setsolvebandpoly() is False
    assert instance.setsolvegainspline() is False
    assert instance.initcalset() is False
    assert instance.delmod() is False
    assert instance.linpolcor() is False
    assert instance.posangcal([12.0], "unused.cal") is False
    with pytest.raises(RuntimeError, match=r"Error in Calibrater::setModel: no VisEquation\."):
        instance.setptmodel()


IMAGER_CALIBRATION = {
    "open", "close", "done", "selectvis", "uvrange", "settaylorterms", "ft", "setjy", "ssoflux", "predictcomp",
}
IMAGER_SETTINGS = {"defineimage", "setoptions", "setsdoptions", "setvp"}


def test_imager_calibration_signatures() -> None:
    recorded = casa_signatures("imager")
    assert_casa_signatures(tools.imager, {
        **recorded,
        "methods": {name: recorded["methods"][name] for name in IMAGER_CALIBRATION | IMAGER_SETTINGS},
    })
    assert_bundle_describes(tools.imager, "imager", {
        "implemented": IMAGER_CALIBRATION, "partial": IMAGER_SETTINGS,
    })


def test_imager_calibration_signatures_match_live_casa() -> None:
    casatools = pytest.importorskip("casatools")
    assert_live_signatures(tools.imager, casatools.imager, methods=IMAGER_CALIBRATION | IMAGER_SETTINGS)
