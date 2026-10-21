// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `measures` tool's surface: every method of CASA's installed
//! `measures` binding (48) with its parameters and effective defaults
//! (CASA 6.7.6), in the shared [`crate::tools::surface`] form.
//!
//! `python/tests/test_tools_measures.py` keeps this table, the pyo3
//! declarations in [`super`] and the installed CASA binding
//! (`python/tests/data/casa_measures_signatures.json`) in step.

use crate::tools::surface::{
    CasaDefault, Kind, Method, Parameter, Returns, Status, ToolSurface, method, param,
};

use CasaDefault::{Bool, Empty, EmptyRecord, Required, Text};
use Kind as K;
use Returns as R;
use Status::{Implemented as Yes, NotYetImplemented as No};

const OFF: Parameter = param("off", K::Record, EmptyRecord);
const V: [Parameter; 1] = [param("v", K::Record, Required)];
const RF_V0: [Parameter; 2] = [
    param("rf", K::Text, Required),
    param("v0", K::Record, Required),
];
const PAIR: [Parameter; 2] = [
    param("m1", K::Record, Required),
    param("m2", K::Record, Required),
];
const RISE: [Parameter; 2] = [param("crd", K::Any, Empty), param("ev", K::Any, Empty)];

/// A measure of three values: `baseline`, `earthmagnetic`, `position`,
/// `uvw`.
const fn three(rf: &'static str) -> [Parameter; 5] {
    [
        param("rf", K::Text, Text(rf)),
        param("v0", K::Any, Empty),
        param("v1", K::Any, Empty),
        param("v2", K::Any, Empty),
        OFF,
    ]
}

/// A measure of one value: `doppler`, `epoch`, `frequency`,
/// `radialvelocity`.
const fn one(rf: &'static str) -> [Parameter; 3] {
    [
        param("rf", K::Text, Text(rf)),
        param("v0", K::Any, Empty),
        OFF,
    ]
}

const BASELINE: [Parameter; 5] = three("ITRF");
const EARTHMAGNETIC: [Parameter; 5] = three("IGRF");
const POSITION: [Parameter; 5] = three("WGS84");
const UVW: [Parameter; 5] = three("ITRF");
const DOPPLER: [Parameter; 3] = one("RADIO");
const EPOCH: [Parameter; 3] = one("UTC");
const FREQUENCY: [Parameter; 3] = one("LSRK");
const RADIALVELOCITY: [Parameter; 3] = one("LSRK");

/// Every public method of CASA's installed `measures` binding.
const METHODS: &[Method] = &[
    method(
        "addxvalue",
        &[param("a", K::Record, Required)],
        R::Record,
        No,
    ),
    method(
        "asbaseline",
        &[param("pos", K::Record, Required)],
        R::Record,
        No,
    ),
    method("baseline", &BASELINE, R::Record, No),
    method("cometangdiam", &[], R::Record, No),
    method("cometdist", &[], R::Record, No),
    method("cometname", &[], R::Text, No),
    method("comettopo", &[], R::Record, No),
    method("comettype", &[], R::Text, No),
    method(
        "direction",
        &[
            param("rf", K::Text, Text("J2000")),
            param("v0", K::Any, Empty),
            param("v1", K::Any, Empty),
            OFF,
        ],
        R::Record,
        Yes,
    ),
    method("dirshow", &V, R::Text, Yes),
    method("doframe", &V, R::Bool, Yes),
    method("done", &[], R::Bool, Yes),
    method("doppler", &DOPPLER, R::Record, No),
    method("earthmagnetic", &EARTHMAGNETIC, R::Record, No),
    method("epoch", &EPOCH, R::Record, Yes),
    method(
        "expand",
        &[param("v", K::Record, EmptyRecord)],
        R::Record,
        No,
    ),
    method("framecomet", &[param("v", K::Text, Text(""))], R::Bool, No),
    method("framenow", &[], R::Bool, Yes),
    method("frequency", &FREQUENCY, R::Record, Yes),
    method("getoffset", &V, R::Record, No),
    method("getref", &V, R::Text, Yes),
    method("gettype", &V, R::Text, Yes),
    method("getvalue", &V, R::Record, Yes),
    method("ismeasure", &V, R::Bool, Yes),
    method("linelist", &[], R::Text, No),
    method(
        "listcodes",
        &[param("ms", K::Record, Required)],
        R::Record,
        Yes,
    ),
    method(
        "measure",
        &[
            param("v", K::Record, Required),
            param("rf", K::Text, Required),
            OFF,
        ],
        R::Record,
        Yes,
    ),
    method(
        "observatory",
        &[param("name", K::Text, Text("ALMA"))],
        R::Record,
        Yes,
    ),
    method("obslist", &[], R::Texts, No),
    method("posangle", &PAIR, R::Record, Yes),
    method("position", &POSITION, R::Record, Yes),
    method("radialvelocity", &RADIALVELOCITY, R::Record, No),
    method("rise", &RISE, R::Record, No),
    method("riseset", &RISE, R::Record, No),
    method("separation", &PAIR, R::Record, Yes),
    method(
        "shift",
        &[
            param("v", K::Record, EmptyRecord),
            param("offset", K::Quantity, Empty),
            param("pa", K::Quantity, Empty),
        ],
        R::Record,
        No,
    ),
    method(
        "show",
        &[
            param("v", K::Record, Required),
            param("refcode", K::Bool, Bool(true)),
        ],
        R::Text,
        Yes,
    ),
    method("showframe", &[], R::Text, No),
    method(
        "source",
        &[param("name", K::Measure, Empty)],
        R::Record,
        Yes,
    ),
    method("sourcelist", &[], R::Text, No),
    method(
        "spectralline",
        &[param("name", K::Text, Text("HI"))],
        R::Record,
        No,
    ),
    method(
        "todoppler",
        &[
            param("rf", K::Text, Required),
            param("v0", K::Record, Required),
            param("rfq", K::Any, Empty),
        ],
        R::Record,
        No,
    ),
    method(
        "tofrequency",
        &[
            param("rf", K::Text, Required),
            param("v0", K::Record, Required),
            param("rfq", K::Record, Required),
        ],
        R::Record,
        No,
    ),
    method("toradialvelocity", &RF_V0, R::Record, No),
    method(
        "torestfrequency",
        &[
            param("v0", K::Record, Required),
            param("d0", K::Record, Required),
        ],
        R::Record,
        No,
    ),
    method(
        "touvw",
        &[param("v", K::Record, EmptyRecord)],
        R::Record,
        No,
    ),
    method("type", &[], R::Text, Yes),
    method("uvw", &UVW, R::Record, No),
];

/// `casars.tools.measures`.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "measures",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["done"],
};
