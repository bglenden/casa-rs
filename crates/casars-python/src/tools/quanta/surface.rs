// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `quanta` tool's surface: every method of CASA's installed `quanta`
//! binding (57) with its parameters and effective defaults (CASA 6.7.6), in
//! the shared [`crate::tools::surface`] form.
//!
//! `python/tests/test_tools_quanta.py` keeps this table, the pyo3
//! declarations in [`super`] and the installed CASA binding
//! (`python/tests/data/casa_quanta_signatures.json`) in step.

use crate::tools::surface::{
    CasaDefault, Kind, Method, Parameter, Returns, Status, ToolSurface, method, param,
};

use CasaDefault::{Bool, Empty, Float, Int, Required, Text, Texts};
use Kind as K;
use Returns as R;
use Status::{Implemented as Yes, NotYetImplemented as No};

const V: [Parameter; 1] = [param("v", K::Quantity, Empty)];
const V_A: [Parameter; 2] = [
    param("v", K::Quantity, Empty),
    param("a", K::Quantity, Empty),
];
const QUANTITY: [Parameter; 3] = [
    param("v", K::Any, Empty),
    param("unitname", K::Text, Text("")),
    param("keepshape", K::Bool, Bool(false)),
];
const FORMAT: [Parameter; 4] = [
    param("v", K::Quantity, Empty),
    param("prec", K::Int, Int(0)),
    param("form", K::Texts, Texts(&[])),
    param("showform", K::Bool, Bool(false)),
];

/// A one-argument quantity method.
const fn unary(name: &'static str, returns: Returns, status: Status) -> Method {
    method(name, &V, returns, status)
}

/// A two-argument quantity method.
const fn binary(name: &'static str, returns: Returns, status: Status) -> Method {
    method(name, &V_A, returns, status)
}

/// Every public method of CASA's installed `quanta` binding.
const METHODS: &[Method] = &[
    unary("abs", R::Record, Yes),
    unary("acos", R::Record, No),
    binary("add", R::Record, Yes),
    method("angle", &FORMAT, R::Texts, Yes),
    unary("asin", R::Record, No),
    unary("atan", R::Record, No),
    binary("atan2", R::Record, No),
    unary("canon", R::Record, Yes),
    unary("canonical", R::Record, Yes),
    unary("ceil", R::Record, No),
    method("check", &[param("v", K::Text, Required)], R::Bool, Yes),
    method("checkfreq", &[param("cm", K::Quantity, Empty)], R::Bool, No),
    binary("compare", R::Bool, Yes),
    method(
        "constants",
        &[param("v", K::Text, Text("pi"))],
        R::Record,
        No,
    ),
    method(
        "convert",
        &[
            param("v", K::Quantity, Empty),
            param("outunit", K::Quantity, Empty),
        ],
        R::Record,
        Yes,
    ),
    method(
        "convertdop",
        &[
            param("v", K::Quantity, Empty),
            param("outunit", K::Text, Text("km/s")),
        ],
        R::Record,
        No,
    ),
    method(
        "convertfreq",
        &[
            param("v", K::Quantity, Empty),
            param("outunit", K::Text, Text("Hz")),
        ],
        R::Record,
        No,
    ),
    unary("cos", R::Record, No),
    method(
        "define",
        &[
            param("name", K::Text, Required),
            param("v", K::Quantity, Empty),
        ],
        R::Bool,
        No,
    ),
    binary("div", R::Record, Yes),
    method("done", &[param("kill", K::Bool, Bool(false))], R::Bool, Yes),
    binary("eq", R::Bool, No),
    unary("exp", R::Record, No),
    method("fits", &[], R::Bool, No),
    unary("floor", R::Record, No),
    method(
        "formxxx",
        &[
            param("v", K::Quantity, Empty),
            param("format", K::Text, Text("dms")),
            param("prec", K::Int, Int(2)),
        ],
        R::Text,
        Yes,
    ),
    binary("ge", R::Bool, No),
    method("getformat", &[param("t", K::Text, Text(""))], R::Text, No),
    method("getunit", &[param("v", K::Any, Empty)], R::Text, Yes),
    method("getvalue", &[param("v", K::Any, Empty)], R::Floats, Yes),
    binary("gt", R::Bool, No),
    unary("isangle", R::Bool, No),
    method("isquantity", &[param("v", K::Any, Empty)], R::Bool, Yes),
    binary("le", R::Bool, No),
    unary("log", R::Record, No),
    unary("log10", R::Record, No),
    binary("lt", R::Bool, No),
    method("map", &[param("v", K::Text, Text("all"))], R::Text, No),
    method("maprec", &[param("v", K::Text, Text("all"))], R::Record, No),
    binary("mul", R::Record, Yes),
    binary("ne", R::Bool, No),
    unary("neg", R::Record, Yes),
    method(
        "norm",
        &[
            param("v", K::Quantity, Empty),
            param("a", K::Float, Float(-0.5)),
        ],
        R::Record,
        No,
    ),
    method(
        "pow",
        &[param("v", K::Quantity, Empty), param("a", K::Int, Int(1))],
        R::Record,
        No,
    ),
    method("quantity", &QUANTITY, R::Record, Yes),
    method(
        "setformat",
        &[
            param("t", K::Text, Text("")),
            param("v", K::Text, Text("F")),
        ],
        R::Bool,
        No,
    ),
    unary("sin", R::Record, No),
    unary("splitdate", R::Record, Yes),
    unary("sqrt", R::Record, No),
    binary("sub", R::Record, Yes),
    unary("tan", R::Record, No),
    method("time", &FORMAT, R::Texts, Yes),
    unary("toangle", R::Record, Yes),
    method(
        "tos",
        &[
            param("v", K::Quantity, Empty),
            param("prec", K::Int, Int(9)),
        ],
        R::Text,
        Yes,
    ),
    unary("totime", R::Record, Yes),
    method("type", &[], R::Text, Yes),
    method("unit", &QUANTITY, R::Record, Yes),
];

/// `casars.tools.quanta`.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "quanta",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["done"],
};
