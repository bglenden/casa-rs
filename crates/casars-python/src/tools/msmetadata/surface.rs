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
        Yes,
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
        Yes,
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
        Yes,
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
    method("bandwidths", &[param("spw", K::Int, Int(-1))], R::Any, Yes),
    method("baseband", &[param("spw", K::Any, Required)], R::Any, Yes),
    method("baselines", &[], R::Any, Yes),
    method("chanavgspws", &[], R::Any, Yes),
    method(
        "chaneffbws",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("")),
            param("asvel", K::Bool, Bool(false)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "chanfreqs",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("Hz")),
        ],
        R::Any,
        Yes,
    ),
    method(
        "chanres",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("")),
            param("asvel", K::Bool, Bool(false)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "chanwidths",
        &[
            param("spw", K::Any, Required),
            param("unit", K::Text, Text("Hz")),
        ],
        R::Any,
        Yes,
    ),
    method("close", &[], R::Any, Yes),
    method("corrbit", &[param("spw", K::Int, Int(-1))], R::Any, Yes),
    method(
        "corrprodsforpol",
        &[param("pol", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
    method(
        "corrtypesforpol",
        &[param("pol", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
    method(
        "datadescids",
        &[param("spw", K::Int, Int(-1)), param("pol", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
    method("done", &[], R::Any, Yes),
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
    method("fdmspws", &[], R::Any, Yes),
    method("fieldnames", &[], R::Any, Yes),
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
        Yes,
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
        Yes,
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
        Yes,
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
        Yes,
    ),
    method(
        "fieldsfortimes",
        &[
            param("time", K::Float, Float(-1.0)),
            param("tol", K::Float, Float(0.0)),
        ],
        R::Any,
        Yes,
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
        Yes,
    ),
    method("name", &[], R::Any, Yes),
    method(
        "namesforfields",
        &[param("fieldids", K::Text, Text(""))],
        R::Any,
        Yes,
    ),
    method(
        "namesforspws",
        &[param("spwids", K::Text, Text(""))],
        R::Any,
        Yes,
    ),
    method("nantennas", &[], R::Any, Yes),
    method("narrays", &[], R::Any, Yes),
    method(
        "nbaselines",
        &[param("ac", K::Bool, Bool(false))],
        R::Any,
        Yes,
    ),
    method("nchan", &[param("spw", K::Any, Required)], R::Any, Yes),
    method(
        "ncorrforpol",
        &[param("polid", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
    method("nfields", &[], R::Any, Yes),
    method("nobservations", &[], R::Any, Yes),
    method(
        "nrows",
        &[
            param("autoc", K::Bool, Bool(true)),
            param("flagged", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method("nscans", &[], R::Any, Yes),
    method("nsources", &[], R::Any, Yes),
    method(
        "nspw",
        &[param("includewvr", K::Bool, Bool(true))],
        R::Any,
        Yes,
    ),
    method("nstates", &[], R::Any, Yes),
    method("observatorynames", &[], R::Any, Yes),
    method(
        "observatoryposition",
        &[param("which", K::Int, Int(0))],
        R::Any,
        No,
    ),
    method("observers", &[], R::Any, Yes),
    method(
        "open",
        &[
            param("msfile", K::Text, Text("")),
            param("maxcache", K::Float, Float(50.0)),
        ],
        R::Any,
        Yes,
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
        Yes,
    ),
    method("projects", &[], R::Any, Yes),
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
    method("reffreq", &[param("spw", K::Int, Int(-1))], R::Any, Yes),
    method(
        "restfreqs",
        &[
            param("sourceid", K::Int, Int(0)),
            param("spw", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "rxbands",
        &[param("spwids", K::Text, Text(""))],
        R::Any,
        Yes,
    ),
    method(
        "scannumbers",
        &[
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "scansforfield",
        &[
            param("field", K::Text, Text("")),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "scansforfields",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
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
        Yes,
    ),
    method(
        "scansforspws",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "scansforstate",
        &[
            param("state", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
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
        Yes,
    ),
    method("schedule", &[param("obsid", K::Int, Int(-1))], R::Any, No),
    method("sideband", &[param("spw", K::Any, Required)], R::Any, Yes),
    method("sourcedirs", &[], R::Any, No),
    method(
        "sourceidforfield",
        &[param("field", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
    method("sourceidsfromsourcetable", &[], R::Any, Yes),
    method("sourcenames", &[], R::Any, Yes),
    method("sourcetimes", &[], R::Any, No),
    method(
        "spwfordatadesc",
        &[param("ddid", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
    method(
        "spwsforbaseband",
        &[
            param("baseband", K::Int, Int(-1)),
            param("sqldmode", K::Text, Text("include")),
        ],
        R::Any,
        Yes,
    ),
    method(
        "spwsforfield",
        &[param("field", K::Text, Text(""))],
        R::Any,
        Yes,
    ),
    method("spwsforfields", &[], R::Any, Yes),
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
        Yes,
    ),
    method(
        "spwsforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "spwsforscans",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "statesforscan",
        &[
            param("scan", K::Int, Int(-1)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "statesforscans",
        &[
            param("obsid", K::Int, Int(0)),
            param("arrayid", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "subwindows",
        &[param("spwids", K::Text, Text(""))],
        R::Any,
        Yes,
    ),
    method("summary", &[], R::Any, No),
    method("tdmspws", &[], R::Any, Yes),
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
        Yes,
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
        Yes,
    ),
    method(
        "timesforscans",
        &[
            param("scans", K::Int, Int(0)),
            param("obsid", K::Int, Int(-1)),
            param("arrayid", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "timesforspws",
        &[param("spw", K::Int, Int(-1))],
        R::Any,
        Yes,
    ),
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
        Yes,
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
