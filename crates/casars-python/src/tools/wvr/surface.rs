// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed CASA 6.7.6.14 WVR signature.

use crate::tools::surface::{
    CasaDefault as D, Kind as K, Method, Returns as R, Status as S, ToolSurface, method, param,
};

const METHODS: &[Method] = &[method(
    "gcal",
    &[
        param("vis", K::Text, D::Text("")),
        param("output", K::Text, D::Text("")),
        param("toffset", K::Float, D::Float(0.0)),
        param("nsol", K::Int, D::Int(1)),
        param("segsource", K::Bool, D::Bool(false)),
        param("reverse", K::Bool, D::Bool(false)),
        param("reversespw", K::Ints, D::Ints(&[])),
        param("disperse", K::Bool, D::Bool(false)),
        param("cont", K::Bool, D::Bool(false)),
        param("wvrflag", K::Text, D::Text("")),
        param("sourceflag", K::Texts, D::Texts(&[])),
        param("statfield", K::Texts, D::Texts(&[])),
        param("statsource", K::Texts, D::Texts(&[])),
        param("tie", K::Texts, D::Texts(&[])),
        param("smooth", K::Int, D::Int(1)),
        param("scale", K::Float, D::Float(1.0)),
        param("maxdistm", K::Float, D::Float(500.0)),
        param("minnumants", K::Int, D::Int(2)),
        param("mingoodfrac", K::Float, D::Float(0.8)),
        param("usefieldtab", K::Bool, D::Bool(false)),
        param("spw", K::Ints, D::Ints(&[])),
        param("wvrspw", K::Ints, D::Ints(&[])),
        param("refant", K::Text, D::Text("")),
        param("offsets", K::Text, D::Text("")),
        param("rseed", K::Int, D::Int(0)),
        param("logfile", K::Text, D::Text("wvrgcal.log")),
    ],
    R::Int,
    S::Implemented,
)];

pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "wvr",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &[],
};
