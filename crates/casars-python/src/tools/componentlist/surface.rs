// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `componentlist` tool's surface: every method of CASA's installed
//! `componentlist` binding (61) with its parameters and effective defaults
//! (CASA 6.7.6), in the shared [`crate::tools::surface`] form.
//!
//! `python/tests/test_tools_componentlist.py` keeps this table, the pyo3
//! declarations in [`super`] and the installed CASA binding
//! (`python/tests/data/casa_componentlist_signatures.json`) in step.

use crate::tools::surface::{
    CasaDefault, Kind, Method, Parameter, Returns, Status, ToolSurface, method, param,
};

use CasaDefault::{Bool, Empty, EmptyRecord, Floats, Int, Ints, Required, Text};
use Kind as K;
use Returns as R;
use Status::{Implemented as Yes, NotYetImplemented as No};

const WHICH: Parameter = param("which", K::Int, Required);
const WHICH_ALL: Parameter = param("which", K::Ints, Ints(&[-1]));
const LOG: Parameter = param("log", K::Bool, Bool(true));
const KEYWORD: Parameter = param("keyword", K::Text, Text("metadata"));
const TABULAR_FREQS: Parameter = param("tabularfreqs", K::Floats, Floats(&[100_000_000_000.0]));

/// A method of component `which` alone.
const fn of_one(name: &'static str, returns: Returns, status: Status) -> Method {
    method(name, &[WHICH], returns, status)
}

/// Every public method of CASA's installed `componentlist` binding.
const METHODS: &[Method] = &[
    method(
        "add",
        &[
            param("thecomponent", K::Record, Required),
            param("iknow", K::Bool, Bool(true)),
        ],
        R::Bool,
        No,
    ),
    method(
        "addcomponent",
        &[
            param("flux", K::Any, Empty),
            param("fluxunit", K::Text, Text("Jy")),
            param("polarization", K::Text, Text("Stokes")),
            param("dir", K::Any, Empty),
            param("shape", K::Text, Text("point")),
            param("majoraxis", K::Any, Empty),
            param("minoraxis", K::Any, Empty),
            param("positionangle", K::Any, Empty),
            param("freq", K::Any, Empty),
            param("spectrumtype", K::Text, Text("constant")),
            param("index", K::Any, Empty),
            param("optionalparms", K::Floats, Floats(&[0.0])),
            param("label", K::Text, Text("")),
        ],
        R::Bool,
        Yes,
    ),
    method(
        "asciitocomponentlist",
        &[
            param("filename", K::Text, Required),
            param("asciifile", K::Text, Required),
            param("refer", K::Text, Text("J2000")),
            param("format", K::Text, Text("ST")),
            param("direction", K::Record, EmptyRecord),
            param("spectrum", K::Record, EmptyRecord),
            param("flux", K::Record, EmptyRecord),
            LOG,
        ],
        R::Int,
        No,
    ),
    method("close", &[LOG], R::Bool, Yes),
    method(
        "concatenate",
        &[param("list", K::Any, Empty), WHICH_ALL, LOG],
        R::Bool,
        No,
    ),
    method(
        "convertfluxpol",
        &[WHICH, param("polarization", K::Text, Text("Stokes"))],
        R::Bool,
        No,
    ),
    method(
        "convertfluxunit",
        &[WHICH, param("unit", K::Text, Text("Jy"))],
        R::Bool,
        No,
    ),
    method(
        "convertfrequnit",
        &[WHICH, param("unit", K::Text, Text("GHz"))],
        R::Bool,
        No,
    ),
    method(
        "convertrefdir",
        &[WHICH, param("frame", K::Text, Required)],
        R::Bool,
        No,
    ),
    method(
        "convertshape",
        &[
            WHICH,
            param("majoraxis", K::Text, Text("arcmin")),
            param("minoraxis", K::Text, Text("arcmin")),
            param("positionangle", K::Text, Text("deg")),
        ],
        R::Bool,
        No,
    ),
    method(
        "deselect",
        &[param("which", K::Ints, Required)],
        R::Bool,
        No,
    ),
    method("done", &[], R::Bool, Yes),
    method("edit", &[WHICH, LOG], R::Bool, No),
    method(
        "fromrecord",
        &[param("record", K::Record, EmptyRecord)],
        R::Bool,
        No,
    ),
    method(
        "getcomponent",
        &[WHICH, param("iknow", K::Bool, Bool(false))],
        R::Record,
        Yes,
    ),
    of_one("getfluxerror", R::Floats, No),
    of_one("getfluxpol", R::Text, No),
    of_one("getfluxunit", R::Text, Yes),
    of_one("getfluxvalue", R::Floats, Yes),
    of_one("getfreq", R::Record, No),
    of_one("getfreqframe", R::Text, No),
    of_one("getfrequnit", R::Text, No),
    of_one("getfreqvalue", R::Float, No),
    method("getkeyword", &[KEYWORD], R::Record, No),
    of_one("getlabel", R::Text, Yes),
    of_one("getrefdir", R::Record, Yes),
    method(
        "getrefdirdec",
        &[
            WHICH,
            param("unit", K::Text, Text("deg")),
            param("precision", K::Int, Int(6)),
        ],
        R::Text,
        No,
    ),
    of_one("getrefdirframe", R::Text, No),
    method(
        "getrefdirra",
        &[
            WHICH,
            param("unit", K::Text, Text("deg")),
            param("precision", K::Int, Int(6)),
        ],
        R::Text,
        No,
    ),
    of_one("getshape", R::Record, No),
    of_one("getspectrum", R::Record, Yes),
    method("haskeyword", &[KEYWORD], R::Bool, No),
    method("indices", &[], R::Ints, No),
    method(
        "iscomponentlist",
        &[param("tool", K::Any, Empty)],
        R::Bool,
        No,
    ),
    method("isphysical", &[WHICH_ALL], R::Bool, No),
    method("length", &[], R::Int, Yes),
    method(
        "open",
        &[
            param("filename", K::Text, Text("")),
            param("nomodify", K::Bool, Bool(false)),
            LOG,
        ],
        R::Bool,
        Yes,
    ),
    method("purge", &[], R::Bool, No),
    method(
        "putkeyword",
        &[KEYWORD, param("value", K::Any, Empty)],
        R::Bool,
        No,
    ),
    method("recover", &[LOG], R::Bool, No),
    method("remove", &[WHICH_ALL, LOG], R::Bool, No),
    method(
        "rename",
        &[param("filename", K::Text, Required), LOG],
        R::Bool,
        Yes,
    ),
    method(
        "replace",
        &[WHICH, param("list", K::Record, Required), WHICH_ALL_ONES],
        R::Bool,
        No,
    ),
    method(
        "sample",
        &[
            param("direction", K::Any, Empty),
            param("pixellatsize", K::Any, Empty),
            param("pixellongsize", K::Any, Empty),
            param("frequency", K::Any, Empty),
        ],
        R::Floats,
        No,
    ),
    method("select", &[param("which", K::Ints, Required)], R::Bool, No),
    method("selected", &[], R::Ints, No),
    method(
        "setflux",
        &[
            WHICH,
            param("value", K::Any, Empty),
            param("unit", K::Text, Text("Jy")),
            param("polarization", K::Text, Text("Stokes")),
            param("error", K::Any, Empty),
            LOG,
        ],
        R::Bool,
        No,
    ),
    method(
        "setfreq",
        &[
            WHICH,
            param("value", K::Float, Required),
            param("unit", K::Text, Text("GHz")),
            LOG,
        ],
        R::Bool,
        No,
    ),
    method(
        "setfreqframe",
        &[WHICH, param("frame", K::Text, Text("LSRK")), LOG],
        R::Bool,
        No,
    ),
    method(
        "setlabel",
        &[WHICH, param("value", K::Text, Required), LOG],
        R::Bool,
        No,
    ),
    method(
        "setrefdir",
        &[
            param("which", K::Int, Int(1)),
            param("ra", K::Any, Empty),
            param("dec", K::Any, Empty),
            LOG,
        ],
        R::Bool,
        No,
    ),
    method(
        "setrefdirframe",
        &[WHICH, param("frame", K::Text, Required), LOG],
        R::Bool,
        No,
    ),
    method(
        "setshape",
        &[
            WHICH,
            param("type", K::Text, Text("Point")),
            param("majoraxis", K::Any, Empty),
            param("minoraxis", K::Any, Empty),
            param("positionangle", K::Any, Empty),
            param("majoraxiserror", K::Any, Empty),
            param("minoraxiserror", K::Any, Empty),
            param("positionangleerror", K::Any, Empty),
            param("optionalparms", K::Floats, Floats(&[0.0])),
            LOG,
        ],
        R::Bool,
        No,
    ),
    method(
        "setspectrum",
        &[
            WHICH,
            param("type", K::Text, Text("spectral index")),
            param("index", K::Any, Empty),
            TABULAR_FREQS,
            param("tabularflux", K::Floats, Floats(&[1.0])),
            param("tabularframe", K::Text, Text("LSRK")),
        ],
        R::Bool,
        No,
    ),
    method(
        "setstokesspectrum",
        &[
            WHICH,
            param("type", K::Text, Text("spectral index")),
            param("index", K::Floats, Floats(&[0.0])),
            TABULAR_FREQS,
            param("tabulari", K::Floats, Floats(&[1.0])),
            param("tabularq", K::Floats, Floats(&[0.0])),
            param("tabularu", K::Floats, Floats(&[0.0])),
            param("tabularv", K::Floats, Floats(&[0.0])),
            param("reffreq", K::Any, Empty),
            param("frame", K::Text, Text("LSRK")),
        ],
        R::Bool,
        No,
    ),
    of_one("shapetype", R::Text, Yes),
    method(
        "simulate",
        &[param("howmany", K::Int, Int(1)), LOG],
        R::Bool,
        No,
    ),
    method(
        "sort",
        &[param("criteria", K::Text, Text("Flux")), LOG],
        R::Bool,
        No,
    ),
    of_one("spectrumtype", R::Text, Yes),
    method("summarize", &[param("which", K::Int, Int(-1))], R::Bool, No),
    method("torecord", &[], R::Record, Yes),
];

const WHICH_ALL_ONES: Parameter = param("whichones", K::Ints, Ints(&[-1]));

/// The `componentlist` tool's surface.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "componentlist",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "rename", "close", "done"],
};
