// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `coordsys` tool's surface: every method of CASA's installed `coordsys`
//! binding (69) with its parameters and effective defaults (CASA
//! 6.7.6.14), in the shared [`crate::tools::surface`] form.
//!
//! `python/tests/test_tools_casa_inventory.py` keeps this table, the pyo3
//! declarations in [`super`] and the installed CASA binding
//! (`python/tests/data/casa_coordsys_signatures.json`) in step.

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

/// Every public method of CASA's installed `coordsys` binding.
const METHODS: &[Method] = &[
    method(
        "addcoordinate",
        &[
            param("direction", K::Bool, Bool(false)),
            param("spectral", K::Bool, Bool(false)),
            param("stokes", K::Any, Empty),
            param("linear", K::Int, Int(0)),
            param("tabular", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "axesmap",
        &[param("toworld", K::Bool, Bool(true))],
        R::Any,
        No,
    ),
    method(
        "axiscoordinatetypes",
        &[param("world", K::Bool, Bool(true))],
        R::Any,
        No,
    ),
    method(
        "conversiontype",
        &[param("type", K::Text, Text("direction"))],
        R::Any,
        No,
    ),
    method(
        "convert",
        &[
            param("coordin", K::Any, Required),
            param("absin", K::Bools, Bools(&[true])),
            param("dopplerin", K::Text, Text("radio")),
            param("unitsin", K::Texts, Texts(&["Native"])),
            param("absout", K::Bools, Bools(&[true])),
            param("dopplerout", K::Text, Text("radio")),
            param("unitsout", K::Texts, Texts(&["Native"])),
            param("shape", K::Ints, Ints(&[-1])),
        ],
        R::Any,
        No,
    ),
    method(
        "convertdirection",
        &[param("frame", K::Any, Required)],
        R::Any,
        No,
    ),
    method(
        "convertmany",
        &[
            param("coordin", K::Any, Empty),
            param("absin", K::Bools, Bools(&[true])),
            param("dopplerin", K::Text, Text("radio")),
            param("unitsin", K::Texts, Texts(&["Native"])),
            param("absout", K::Bools, Bools(&[true])),
            param("dopplerout", K::Text, Text("radio")),
            param("unitsout", K::Texts, Texts(&["Native"])),
            param("shape", K::Ints, Ints(&[-1])),
        ],
        R::Any,
        No,
    ),
    method(
        "coordinatetype",
        &[param("which", K::Int, Int(-1))],
        R::Any,
        No,
    ),
    method("copy", &[], R::Any, No),
    method("done", &[], R::Any, No),
    method("epoch", &[], R::Any, No),
    method(
        "findaxis",
        &[
            param("world", K::Bool, Bool(true)),
            param("axis", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "findaxisbyname",
        &[
            param("axisname", K::Text, Text("")),
            param("allowfriendlyname", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method(
        "findcoordinate",
        &[
            param("type", K::Text, Text("direction")),
            param("which", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "frequencytofrequency",
        &[
            param("value", K::Any, Required),
            param("frequnit", K::Text, Text("")),
            param("velocity", K::Any, Empty),
        ],
        R::Any,
        No,
    ),
    method(
        "frequencytovelocity",
        &[
            param("value", K::Any, Required),
            param("frequnit", K::Text, Text("")),
            param("doppler", K::Text, Text("radio")),
            param("velunit", K::Text, Text("km/s")),
        ],
        R::Any,
        No,
    ),
    method(
        "fromrecord",
        &[param("record", K::Any, Required)],
        R::Any,
        No,
    ),
    method(
        "getconversiontype",
        &[
            param("type", K::Text, Text("")),
            param("showconversion", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method(
        "increment",
        &[
            param("format", K::Text, Text("n")),
            param("type", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "lineartransform",
        &[param("type", K::Any, Required)],
        R::Any,
        No,
    ),
    method("names", &[param("type", K::Text, Text(""))], R::Any, No),
    method("naxes", &[param("world", K::Bool, Bool(true))], R::Any, No),
    method("ncoordinates", &[], R::Any, No),
    method(
        "newcoordsys",
        &[
            param("direction", K::Bool, Bool(false)),
            param("spectral", K::Bool, Bool(false)),
            param("stokes", K::Any, Empty),
            param("linear", K::Int, Int(0)),
            param("tabular", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method("observer", &[], R::Any, No),
    method("parentname", &[], R::Any, No),
    method(
        "projection",
        &[param("type", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "referencecode",
        &[
            param("type", K::Text, Text("")),
            param("list", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "referencepixel",
        &[param("type", K::Text, Text(""))],
        R::Any,
        No,
    ),
    method(
        "referencevalue",
        &[
            param("format", K::Text, Text("n")),
            param("type", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method("reorder", &[param("order", K::Any, Required)], R::Any, No),
    method(
        "replace",
        &[
            param("csys", K::Any, Required),
            param("whichin", K::Any, Required),
            param("whichout", K::Any, Required),
        ],
        R::Any,
        No,
    ),
    method("restfrequency", &[], R::Any, No),
    method(
        "setconversiontype",
        &[
            param("direction", K::Text, Text("")),
            param("spectral", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "setdirection",
        &[
            param("refcode", K::Text, Text("")),
            param("proj", K::Text, Text("")),
            param("projpar", K::Floats, Floats(&[-1.0])),
            param("refpix", K::Floats, Floats(&[-1.0])),
            param("refval", K::Any, Empty),
            param("incr", K::Any, Empty),
            param("xform", K::Any, Empty),
            param("poles", K::Any, Empty),
        ],
        R::Any,
        No,
    ),
    method("setepoch", &[param("value", K::Any, Required)], R::Any, No),
    method(
        "setincrement",
        &[
            param("value", K::Any, Empty),
            param("type", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "setlineartransform",
        &[
            param("type", K::Text, Text("")),
            param("value", K::Any, Empty),
        ],
        R::Any,
        No,
    ),
    method(
        "setnames",
        &[
            param("value", K::Any, Required),
            param("type", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "setobserver",
        &[param("value", K::Any, Required)],
        R::Any,
        No,
    ),
    method(
        "setparentname",
        &[param("imagename", K::Any, Required)],
        R::Any,
        No,
    ),
    method(
        "setprojection",
        &[
            param("type", K::Any, Required),
            param("parameters", K::Floats, Floats(&[-1.0])),
        ],
        R::Any,
        No,
    ),
    method(
        "setreferencecode",
        &[
            param("value", K::Any, Required),
            param("type", K::Text, Text("direction")),
            param("adjust", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method(
        "setreferencelocation",
        &[
            param("pixel", K::Ints, Ints(&[-1])),
            param("world", K::Any, Empty),
            param("mask", K::Bools, Bools(&[false])),
        ],
        R::Any,
        No,
    ),
    method(
        "setreferencepixel",
        &[
            param("value", K::Any, Required),
            param("type", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "setreferencevalue",
        &[
            param("value", K::Any, Empty),
            param("type", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "setrestfrequency",
        &[
            param("value", K::Any, Empty),
            param("which", K::Int, Int(0)),
            param("append", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "setspectral",
        &[
            param("refcode", K::Text, Text("")),
            param("restfreq", K::Any, Empty),
            param("frequencies", K::Any, Empty),
            param("doppler", K::Text, Text("")),
            param("velocities", K::Any, Empty),
        ],
        R::Any,
        No,
    ),
    method(
        "setstokes",
        &[param("stokes", K::Any, Required)],
        R::Any,
        No,
    ),
    method(
        "settabular",
        &[
            param("pixel", K::Floats, Floats(&[-1.0])),
            param("world", K::Floats, Floats(&[-1.0])),
            param("which", K::Int, Int(0)),
        ],
        R::Any,
        No,
    ),
    method(
        "settelescope",
        &[param("value", K::Any, Required)],
        R::Any,
        No,
    ),
    method(
        "setunits",
        &[
            param("value", K::Any, Required),
            param("type", K::Text, Text("")),
            param("overwrite", K::Bool, Bool(false)),
            param("which", K::Int, Int(-10)),
        ],
        R::Any,
        No,
    ),
    method("stokes", &[], R::Any, No),
    method(
        "subimage",
        &[
            param("originshft", K::Any, Empty),
            param("newshape", K::Any, Empty),
        ],
        R::Any,
        No,
    ),
    method(
        "summary",
        &[
            param("doppler", K::Text, Text("RADIO")),
            param("list", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method("telescope", &[], R::Any, No),
    method(
        "toabs",
        &[
            param("value", K::Any, Empty),
            param("isworld", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "toabsmany",
        &[
            param("value", K::Any, Empty),
            param("isworld", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method("topixel", &[param("value", K::Any, Empty)], R::Any, No),
    method("topixelmany", &[param("value", K::Any, Empty)], R::Any, No),
    method("torecord", &[], R::Any, No),
    method(
        "torel",
        &[
            param("value", K::Any, Empty),
            param("isworld", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "torelmany",
        &[
            param("value", K::Any, Empty),
            param("isworld", K::Int, Int(-1)),
        ],
        R::Any,
        No,
    ),
    method(
        "toworld",
        &[
            param("value", K::Any, Empty),
            param("format", K::Text, Text("n")),
        ],
        R::Any,
        No,
    ),
    method("toworldmany", &[param("value", K::Any, Empty)], R::Any, No),
    method("transpose", &[param("order", K::Any, Required)], R::Any, No),
    method("type", &[], R::Any, No),
    method("units", &[param("type", K::Text, Text(""))], R::Any, No),
    method(
        "velocitytofrequency",
        &[
            param("value", K::Any, Required),
            param("frequnit", K::Text, Text("")),
            param("doppler", K::Text, Text("radio")),
            param("velunit", K::Text, Text("km/s")),
        ],
        R::Any,
        No,
    ),
];

/// The `coordsys` tool's surface.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "coordsys",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["done"],
};
