// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed CASA 6.7.6.14 single-dish tool signature.

use crate::tools::surface::{
    CasaDefault as D, Kind as K, Method, Returns as R, Status as S, ToolSurface, method, param,
};

const METHODS: &[Method] = &[
    Method {
        mutating: true,
        ..method(
            "apply_baseline_table",
            &[
                param("bltable", K::Text, D::Text("")),
                param("datacolumn", K::Text, D::Text("data")),
                param("spw", K::Any, D::Empty),
                param("updateweight", K::Bool, D::Bool(false)),
                param("sigmavalue", K::Text, D::Text("stddev")),
                param("outfile", K::Text, D::Text("")),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "atmcor",
            &[
                param("config", K::Record, D::EmptyRecord),
                param("datacolumn", K::Text, D::Text("data")),
                param("outfile", K::Text, D::Text("")),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method("close", &[], R::Bool, S::Implemented),
    method("done", &[], R::Bool, S::Implemented),
    Method {
        mutating: true,
        ..method(
            "fit_line",
            &[
                param("datacolumn", K::Text, D::Text("data")),
                param("spw", K::Any, D::Empty),
                param("pol", K::Any, D::Empty),
                param("timebin", K::Text, D::Text("")),
                param("timespan", K::Text, D::Text("")),
                param("polaverage", K::Text, D::Text("")),
                param("fitfunc", K::Text, D::Text("gaussian")),
                param("nfit", K::Text, D::Text("0")),
                param("linefinding", K::Bool, D::Bool(false)),
                param("threshold", K::Float, D::Float(5.0)),
                param("avg_limit", K::Int, D::Int(4)),
                param("minwidth", K::Int, D::Int(4)),
                param("edge", K::Ints, D::Ints(&[0, 0])),
                param("tempfile", K::Text, D::Text("")),
                param("tempoutfile", K::Text, D::Text("")),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "importasap",
            &[
                param("infile", K::Text, D::Text("")),
                param("outfile", K::Text, D::Text("")),
                param("parallel", K::Bool, D::Bool(false)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "importnro",
            &[
                param("infile", K::Text, D::Text("")),
                param("outfile", K::Text, D::Text("")),
                param("parallel", K::Bool, D::Bool(false)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method("name", &[], R::Text, S::Implemented),
    method(
        "open",
        &[param("ms_name", K::Text, D::Text(""))],
        R::Bool,
        S::Implemented,
    ),
    method(
        "set_selection",
        &[
            param("spw", K::Any, D::Empty),
            param("field", K::Any, D::Empty),
            param("antenna", K::Any, D::Empty),
            param("timerange", K::Any, D::Empty),
            param("scan", K::Any, D::Empty),
            param("observation", K::Any, D::Empty),
            param("polarization", K::Any, D::Empty),
            param("beam", K::Any, D::Empty),
            param("intent", K::Any, D::Empty),
            param("feed", K::Any, D::Empty),
            param("taql", K::Text, D::Text("")),
            param("reindex", K::Bool, D::Bool(true)),
        ],
        R::Bool,
        S::Implemented,
    ),
    Method {
        mutating: true,
        ..method(
            "smooth",
            &[
                param("type", K::Text, D::Text("gaussian")),
                param("width", K::Float, D::Float(0.0)),
                param("datacolumn", K::Text, D::Text("data")),
                param("outfile", K::Text, D::Text("")),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "subtract_baseline",
            &[
                param("datacolumn", K::Text, D::Text("data")),
                param("outfile", K::Text, D::Text("")),
                param("bloutput", K::Text, D::Text("")),
                param("dosubtract", K::Bool, D::Bool(true)),
                param("spw", K::Any, D::Empty),
                param("updateweight", K::Bool, D::Bool(false)),
                param("sigmavalue", K::Text, D::Text("stddev")),
                param("blfunc", K::Text, D::Text("poly")),
                param("order", K::Int, D::Int(5)),
                param("clip_threshold_sigma", K::Float, D::Float(3.0)),
                param("num_fitting_max", K::Int, D::Int(1)),
                param("linefinding", K::Bool, D::Bool(false)),
                param("threshold", K::Float, D::Float(5.0)),
                param("avg_limit", K::Int, D::Int(4)),
                param("minwidth", K::Int, D::Int(4)),
                param("edge", K::Ints, D::Ints(&[0, 0])),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "subtract_baseline_cspline",
            &[
                param("datacolumn", K::Text, D::Text("data")),
                param("outfile", K::Text, D::Text("")),
                param("bloutput", K::Text, D::Text("")),
                param("dosubtract", K::Bool, D::Bool(true)),
                param("spw", K::Any, D::Empty),
                param("updateweight", K::Bool, D::Bool(false)),
                param("sigmavalue", K::Text, D::Text("stddev")),
                param("npiece", K::Int, D::Int(5)),
                param("clip_threshold_sigma", K::Float, D::Float(3.0)),
                param("num_fitting_max", K::Int, D::Int(1)),
                param("linefinding", K::Bool, D::Bool(false)),
                param("threshold", K::Float, D::Float(5.0)),
                param("avg_limit", K::Int, D::Int(4)),
                param("minwidth", K::Int, D::Int(4)),
                param("edge", K::Ints, D::Ints(&[0, 0])),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "subtract_baseline_sinusoid",
            &[
                param("datacolumn", K::Text, D::Text("data")),
                param("outfile", K::Text, D::Text("")),
                param("bloutput", K::Text, D::Text("")),
                param("dosubtract", K::Bool, D::Bool(true)),
                param("spw", K::Any, D::Empty),
                param("updateweight", K::Bool, D::Bool(false)),
                param("sigmavalue", K::Text, D::Text("stddev")),
                param("addwn", K::Text, D::Text("0")),
                param("rejwn", K::Text, D::Text("")),
                param("applyfft", K::Bool, D::Bool(false)),
                param("fftmethod", K::Text, D::Text("fft")),
                param("fftthresh", K::Any, D::Empty),
                param("clip_threshold_sigma", K::Float, D::Float(3.0)),
                param("num_fitting_max", K::Int, D::Int(1)),
                param("linefinding", K::Bool, D::Bool(false)),
                param("threshold", K::Float, D::Float(5.0)),
                param("avg_limit", K::Int, D::Int(4)),
                param("minwidth", K::Int, D::Int(4)),
                param("edge", K::Ints, D::Ints(&[0, 0])),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "subtract_baseline_variable",
            &[
                param("datacolumn", K::Text, D::Text("data")),
                param("outfile", K::Text, D::Text("")),
                param("bloutput", K::Text, D::Text("")),
                param("dosubtract", K::Bool, D::Bool(true)),
                param("spw", K::Any, D::Empty),
                param("updateweight", K::Bool, D::Bool(false)),
                param("sigmavalue", K::Text, D::Text("stddev")),
                param("blparam", K::Text, D::Text("")),
                param("verbose", K::Bool, D::Bool(false)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
];

pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "singledishms",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "close", "done"],
};
