// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `msmetadata` tool's surface: every method of CASA's installed `msmetadata`
//! binding (103) with its parameters and effective defaults (CASA
//! 6.7.6.14), in the shared [`crate::tools::surface`] form.
//!
//! `python/tests/test_tools_casa_inventory.py` keeps this table, the pyo3
//! declarations in [`super`] and the installed CASA binding
//! (`python/tests/data/casa_msmetadata_signatures.json`) in step.

#[allow(unused_imports)]
use crate::tools::surface::{
    CasaDefault, Kind, Method, Returns, Status, ToolSurface, method, param,
};

#[allow(unused_imports)]
use CasaDefault::{
    Bool, Bools, Empty, EmptyRecord, Float, Floats, Int, Ints, Required, Text, Texts,
};
use Kind as K;
use Returns as R;
#[allow(unused_imports)]
use Status::{Implemented as Yes, NotYetImplemented as No};

/// Every public method of CASA's installed `msmetadata` binding.
const METHODS: &[Method] = &[
    method(
        "almaspws",
        &[
            param("chavg", K::Bool, Bool(false)),
            param("fdm", K::Bool, Bool(false)),
            param("sqld", K::Bool, Bool(false)),
            param("tdm", K::Bool, Bool(false)),
            param("wvr", K::Bool, Bool(false)),
            param("complement", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "antennadiameter",
        &[param("antenna", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "antennaids",
        &[
            param("name", K::Text, Text("")),
            param("mindiameter", K::Text, Text("0m")),
            param("maxdiameter", K::Text, Text("1pc")),
            param("obsid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "antennanames",
        &[param("antennaids", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "antennaoffset",
        &[param("which", K::Int, Int(0))],
        R::Any,
        No,
    ),
    method(
        "antennaposition",
        &[param("which", K::Int, Int(0))],
        R::Any,
        No,
    ),
    method(
        "antennasforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "antennastations",
        &[
            param("which", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method("bandwidths", &[param("spw", K::Int, Int(-1))], R::Any, No),
    method("baseband", &[param("spw", K::Any, Required)], R::Any, No),
    method("baselines", &[], R::Any, No),
    method("chanavgspws", &[], R::Any, No),
    method(
        "chaneffbws",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("")),
            param("asvel", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "chanfreqs",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("Hz")),
        ],
        R::Any,
        No,
    ),
    method(
        "chanres",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("")),
            param("asvel", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "chanwidths",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("Hz")),
        ],
        R::Any,
        No,
    ),
    method("close", &[], R::Any, No),
    method("corrbit", &[param("spw", K::Int, Int(-1))], R::Any, No),
    method(
        "corrprodsforpol",
        &[param("pol", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "corrtypesforpol",
        &[param("pol", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "datadescids",
        &[param("spw", K::Int, Int(-1)), param("pol", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method("done", &[], R::Any, No),
    method("effexposuretime", &[], R::Any, No),
    method(
        "exposuretime",
        &[
            param("scan", K::Int, Int(0)),
            param("spwid", K::Int, Int(0)),
            param("polid", K::Int, Int(-1)),
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method("fdmspws", &[], R::Any, No),
    method("fieldnames", &[], R::Any, No),
    method(
        "fieldsforintent",
        &[
            param("intent", K::Text, Text("")),
            param("asnames", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "fieldsforname",
        &[param("name", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "fieldsforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("asnames", K::Bool, Bool(false)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "fieldsforscans",
        &[
            param("scans", K::Any, Empty),
            param("asnames", K::Bool, Bool(false)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
            param("asmap", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "fieldsforsource",
        &[
            param("source", K::Int, Int(-1)),
            param("asnames", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "fieldsforsources",
        &[param("asnames", K::Bool, Bool(false))],
        R::Any,
        No,
    ),
    method(
        "fieldsforspw",
        &[
            param("spw", K::Int, Int(-1)),
            param("asnames", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "fieldsfortimes",
        &[
            param("time", K::Float, Float(-1.0)),
            param("tol", K::Float, Float(0.0)),
        ],
        R::Any,
        No,
    ),
    method("intents", &[], R::Any, No),
    method(
        "intentsforfield",
        &[param("field", K::Int, Int(0))],
        R::Any,
        No,
    ),
    method(
        "intentsforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "intentsforspw",
        &[param("spw", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "meanfreq",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("Hz")),
        ],
        R::Any,
        No,
    ),
    method("name", &[], R::Any, No),
    method(
        "namesforfields",
        &[param("fieldids", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "namesforspws",
        &[param("spwids", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method("nantennas", &[], R::Any, No),
    method("narrays", &[], R::Any, No),
    method(
        "nbaselines",
        &[param("ac", K::Bool, Bool(false))],
        R::Any,
        No,
    ),
    method("nchan", &[param("spw", K::Any, Required)], R::Any, No),
    method(
        "ncorrforpol",
        &[param("polid", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method("nfields", &[], R::Any, No),
    method("nobservations", &[], R::Any, No),
    method(
        "nrows",
        &[
            param("autoc", K::Bool, Bool(true)),
            param("flagged", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method("nscans", &[], R::Any, No),
    method("nsources", &[], R::Any, No),
    method(
        "nspw",
        &[param("includewvr", K::Bool, Bool(true))],
        R::Any,
        No,
    ),
    method("nstates", &[], R::Any, No),
    method("observatorynames", &[], R::Any, No),
    method(
        "observatoryposition",
        &[param("which", K::Int, Int(0))],
        R::Any,
        No,
    ),
    method("observers", &[], R::Any, No),
    method(
        "open",
        &[
            param("msfile", K::Text, Text("")),
            param("maxcache", K::Float, Float(50.0)),
        ],
        R::Any,
        No,
    ),
    method(
        "phasecenter",
        &[
            param("fieldid", K::Int, Int(0)),
            param("epoch", K::Record, EmptyRecord),
        ],
        R::Any,
        No,
    ),
    method(
        "pointingdirection",
        &[
            param("rownum", K::Int, Int(0)),
            param("interpolate", K::Bool, Bool(false)),
            param("initialrow", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "polidfordatadesc",
        &[param("ddid", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method("projects", &[], R::Any, No),
    method("propermotions", &[], R::Any, No),
    method(
        "refdir",
        &[
            param("field", K::Int, Int(0)),
            param("epoch", K::Record, EmptyRecord),
        ],
        R::Any,
        No,
    ),
    method("reffreq", &[param("spw", K::Int, Int(-1))], R::Any, No),
    method(
        "restfreqs",
        &[
            param("sourceid", K::Int, Int(0)),
            param("spw", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method("rxbands", &[param("spwids", K::Text, Text(""))], R::Any, No),
    method(
        "scannumbers",
        &[
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansforfield",
        &[
            param("field", K::Text, Text("")),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansforfields",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansforintent",
        &[
            param("intent", K::Text, Text("")),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansforspw",
        &[
            param("spw", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansforspws",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansforstate",
        &[
            param("state", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "scansfortimes",
        &[
            param("time", K::Float, Float(-1.0)),
            param("tol", K::Float, Float(0.0)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method("schedule", &[param("obsid", K::Int, Int(-1))], R::Any, No),
    method("sideband", &[param("spw", K::Any, Required)], R::Any, No),
    method("sourcedirs", &[], R::Any, No),
    method(
        "sourceidforfield",
        &[param("field", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method("sourceidsfromsourcetable", &[], R::Any, No),
    method("sourcenames", &[], R::Any, No),
    method("sourcetimes", &[], R::Any, No),
    method(
        "spwfordatadesc",
        &[param("ddid", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "spwsforbaseband",
        &[
            param("baseband", K::Int, Int(-1)),
            param("sqldmode", K::Text, Text("include")),
        ],
        R::Any,
        No,
    ),
    method(
        "spwsforfield",
        &[param("field", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method("spwsforfields", &[], R::Any, No),
    method(
        "spwsforintent",
        &[param("intent", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "spwsfornames",
        &[param("spwids", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "spwsforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "spwsforscans",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "statesforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "statesforscans",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "subwindows",
        &[param("spwids", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method("summary", &[], R::Any, No),
    method("tdmspws", &[], R::Any, No),
    method(
        "timerangeforobs",
        &[param("obsid", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "timesforfield",
        &[param("field", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method(
        "timesforintent",
        &[param("intent", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "timesforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
            param("perspw", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "timesforscans",
        &[
            param("scans", K::Int, Int(0)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method("timesforspws", &[param("spw", K::Int, Int(-1))], R::Any, No),
    method(
        "transitions",
        &[
            param("sourceid", K::Int, Int(0)),
            param("spw", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "wvrspws",
        &[param("complement", K::Bool, Bool(false))],
        R::Any,
        No,
    ),
];

/// The `msmetadata` tool's surface.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "msmetadata",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "close", "done"],
};
