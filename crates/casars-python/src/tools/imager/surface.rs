// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-facing methods exposed by the legacy imager.

use crate::tools::surface::{
    CasaDefault as D, Kind as K, Method, Returns as R, Status as S, ToolSurface, method, param,
};

const METHODS: &[Method] = &[
    method("close", &[], R::Bool, S::Implemented),
    method("done", &[], R::Bool, S::Implemented),
    Method {
        mutating: true,
        ..method(
            "ft",
            &[
                param("model", K::Texts, D::Texts(&[])),
                param("complist", K::Text, D::Text("")),
                param("incremental", K::Bool, D::Bool(false)),
                param("phasecentertime", K::Float, D::Float(-1.0)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "open",
            &[
                param("thems", K::Text, D::Text("")),
                param("compress", K::Bool, D::Bool(false)),
                param("usescratch", K::Bool, D::Bool(false)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "selectvis",
            &[
                param("vis", K::Text, D::Text("")),
                param("nchan", K::Ints, D::Ints(&[-1])),
                param("start", K::Ints, D::Ints(&[0])),
                param("step", K::Ints, D::Ints(&[1])),
                param("spw", K::Any, D::Empty),
                param("field", K::Any, D::Empty),
                param("baseline", K::Any, D::Empty),
                param("time", K::Any, D::Empty),
                param("scan", K::Any, D::Empty),
                param("intent", K::Text, D::Text("")),
                param("observation", K::Any, D::Empty),
                param("uvrange", K::Any, D::Empty),
                param("taql", K::Text, D::Text("")),
                param("usescratch", K::Bool, D::Bool(false)),
                param("datainmemory", K::Bool, D::Bool(false)),
                param("writeaccess", K::Bool, D::Bool(true)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method(
        "settaylorterms",
        &[
            param("ntaylorterms", K::Int, D::Int(2)),
            param("reffreq", K::Float, D::Float(0.0)),
        ],
        R::Bool,
        S::Implemented,
    ),
    method(
        "uvrange",
        &[
            param("uvmin", K::Float, D::Float(0.0)),
            param("uvmax", K::Float, D::Float(0.0)),
        ],
        R::Bool,
        S::Implemented,
    ),
];

pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "imager",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "close", "done"],
};
