// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed CASA 6.7.6.14 calanalysis signatures.

use crate::tools::surface::{
    CasaDefault as D, Kind as K, Method, Parameter, Returns as R, Status as S, ToolSurface, method,
    param,
};

const NAMES: &[Parameter] = &[param("name", K::Bool, D::Bool(true))];
const GET: [Parameter; 10] = [
    param("field", K::Any, D::Empty),
    param("antenna", K::Any, D::Empty),
    param("timerange", K::Any, D::Empty),
    param("spw", K::Any, D::Empty),
    param("feed", K::Any, D::Empty),
    param("axis", K::Text, D::Text("TIME")),
    param("ap", K::Text, D::Text("AMPLITUDE")),
    param("norm", K::Bool, D::Bool(false)),
    param("unwrap", K::Bool, D::Bool(false)),
    param("jumpmax", K::Float, D::Float(0.)),
];
const FIT: &[Parameter] = &[
    GET[0],
    GET[1],
    GET[2],
    GET[3],
    GET[4],
    GET[5],
    GET[6],
    GET[7],
    GET[8],
    GET[9],
    param("order", K::Text, D::Text("AVERAGE")),
    param("type", K::Text, D::Text("LSQ")),
    param("weight", K::Bool, D::Bool(false)),
];

const METHODS: &[Method] = &[
    method("antenna", NAMES, R::Texts, S::Implemented),
    method("antenna1", NAMES, R::Texts, S::Implemented),
    method("antenna2", NAMES, R::Texts, S::Implemented),
    method("calname", &[], R::Text, S::Implemented),
    method("close", &[], R::Bool, S::Implemented),
    method("feed", &[], R::Texts, S::Implemented),
    method("field", NAMES, R::Texts, S::Implemented),
    method("fit", FIT, R::Record, S::NotYetImplemented),
    method("freq", &[], R::Record, S::Implemented),
    method("get", &GET, R::Record, S::Implemented),
    method("msname", &[], R::Text, S::Implemented),
    method("numantenna", &[], R::Int, S::Implemented),
    method("numantenna1", &[], R::Int, S::Implemented),
    method("numantenna2", &[], R::Int, S::Implemented),
    method("numchannel", &[], R::Ints, S::Implemented),
    method("numfeed", &[], R::Int, S::Implemented),
    method("numfield", &[], R::Int, S::Implemented),
    method("numspw", &[], R::Int, S::Implemented),
    method("numtime", &[], R::Int, S::Implemented),
    method(
        "open",
        &[param("caltable", K::Text, D::Text(""))],
        R::Bool,
        S::Implemented,
    ),
    method("partype", &[], R::Text, S::Implemented),
    method("polbasis", &[], R::Text, S::Implemented),
    method("spw", NAMES, R::Texts, S::Implemented),
    method("time", &[], R::Floats, S::Implemented),
    method("viscal", &[], R::Text, S::Implemented),
];

pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "calanalysis",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "close"],
};
