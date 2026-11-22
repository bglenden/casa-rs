// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed CASA 6.7.6.14 signatures for the calibrater object contract.

use crate::tools::surface::{
    CasaDefault as D, Kind as K, Method, Returns as R, Status as S, ToolSurface, method, param,
};

const METHODS: &[Method] = &[
    Method {
        mutating: true,
        ..method(
            "accumulate",
            &[
                param("tablein", K::Text, D::Text("")),
                param("incrtable", K::Text, D::Text("")),
                param("tableout", K::Text, D::Text("")),
                param("field", K::Any, D::Empty),
                param("calfield", K::Any, D::Empty),
                param("interp", K::Text, D::Text("linear")),
                param("t", K::Float, D::Float(-1.0)),
                param("spwmap", K::Ints, D::Ints(&[-1])),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method("activityrec", &[], R::Any, S::Partial),
    method("close", &[], R::Bool, S::Implemented),
    Method {
        mutating: true,
        ..method(
            "correct",
            &[param("applymode", K::Text, D::Text(""))],
            R::Bool,
            S::Partial,
        )
    },
    Method {
        mutating: true,
        ..method("corrupt", &[], R::Bool, S::Partial)
    },
    Method {
        mutating: true,
        ..method(
            "createcaltable",
            &[
                param("caltable", K::Text, D::Required),
                param("partype", K::Text, D::Required),
                param("caltype", K::Text, D::Required),
                param("singlechan", K::Bool, D::Required),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "delmod",
            &[
                param("otf", K::Bool, D::Bool(false)),
                param("field", K::Any, D::Empty),
                param("spw", K::Any, D::Empty),
                param("scr", K::Bool, D::Bool(false)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method("done", &[], R::Bool, S::Implemented),
    Method {
        mutating: true,
        ..method(
            "fluxscale",
            &[
                param("tablein", K::Any, D::Required),
                param("reference", K::Any, D::Empty),
                param("tableout", K::Text, D::Text("")),
                param("transfer", K::Any, D::Empty),
                param("listfile", K::Text, D::Text("")),
                param("append", K::Bool, D::Bool(false)),
                param("refspwmap", K::Ints, D::Ints(&[-1])),
                param("gainthreshold", K::Float, D::Float(-1.0)),
                param("antenna", K::Text, D::Text("")),
                param("timerange", K::Text, D::Text("")),
                param("scan", K::Text, D::Text("")),
                param("incremental", K::Bool, D::Bool(false)),
                param("fitorder", K::Int, D::Int(1)),
                param("display", K::Bool, D::Bool(false)),
            ],
            R::Record,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "initcalset",
            &[param("calset", K::Int, D::Int(0))],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "initweights",
            &[
                param("wtmode", K::Text, D::Text("nyq")),
                param("dowtsp", K::Bool, D::Bool(false)),
                param("tsystable", K::Text, D::Text("")),
                param("gainfield", K::Text, D::Text("")),
                param("interp", K::Text, D::Text("")),
                param("spwmap", K::Ints, D::Empty),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method(
        "linpolcor",
        &[
            param("tablein", K::Text, D::Text("")),
            param("tableout", K::Text, D::Text("")),
            param("fields", K::Any, D::Empty),
        ],
        R::Bool,
        S::Implemented,
    ),
    method(
        "listcal",
        &[
            param("caltable", K::Text, D::Required),
            param("field", K::Any, D::Empty),
            param("antenna", K::Any, D::Empty),
            param("spw", K::Any, D::Empty),
            param("listfile", K::Text, D::Text("")),
            param("pagerows", K::Int, D::Int(50)),
        ],
        R::Bool,
        S::Implemented,
    ),
    Method {
        mutating: true,
        ..method(
            "modelfit",
            &[
                param("vary", K::Bools, D::Required),
                param("niter", K::Int, D::Int(0)),
                param("compshape", K::Text, D::Text("P")),
                param("par", K::Floats, D::Floats(&[1.0, 0.0, 0.0])),
                param("file", K::Text, D::Text("")),
            ],
            R::Floats,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method(
            "open",
            &[
                param("filename", K::Text, D::Required),
                param("compress", K::Bool, D::Bool(false)),
                param("addcorr", K::Bool, D::Bool(true)),
                param("addmodel", K::Bool, D::Bool(true)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method(
        "parsecallibfile",
        &[param("filein", K::Text, D::Required)],
        R::Any,
        S::Implemented,
    ),
    method(
        "plotcal",
        &[
            param("antennas", K::Any, D::Required),
            param("fields", K::Any, D::Required),
            param("spwids", K::Any, D::Required),
            param("plottype", K::Text, D::Text("AMP")),
            param("tablename", K::Text, D::Text("")),
            param("polarization", K::Int, D::Int(1)),
            param("multiplot", K::Bool, D::Bool(false)),
            param("nx", K::Int, D::Int(1)),
            param("ny", K::Int, D::Int(1)),
            param("psfile", K::Text, D::Text("")),
        ],
        R::Bool,
        S::Implemented,
    ),
    method(
        "posangcal",
        &[
            param("posangcor", K::Any, D::Required),
            param("tablein", K::Any, D::Required),
            param("tableout", K::Text, D::Text("")),
        ],
        R::Bool,
        S::Partial,
    ),
    Method {
        mutating: true,
        ..method(
            "rerefant",
            &[
                param("tablein", K::Text, D::Required),
                param("tableout", K::Text, D::Required),
                param("refantmode", K::Text, D::Text("flexible")),
                param("refant", K::Any, D::Empty),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method(
        "reset",
        &[
            param("apply", K::Bool, D::Bool(true)),
            param("solve", K::Bool, D::Bool(true)),
        ],
        R::Bool,
        S::Implemented,
    ),
    method("returndict", &[], R::Any, S::Partial),
    method(
        "selectvis",
        &[
            param("time", K::Any, D::Empty),
            param("spw", K::Any, D::Empty),
            param("scan", K::Any, D::Empty),
            param("field", K::Any, D::Empty),
            param("intent", K::Any, D::Empty),
            param("observation", K::Any, D::Empty),
            param("baseline", K::Any, D::Empty),
            param("uvrange", K::Any, D::Empty),
            param("chanmode", K::Text, D::Text("none")),
            param("nchan", K::Int, D::Int(1)),
            param("start", K::Int, D::Int(0)),
            param("step", K::Int, D::Int(1)),
            param("mstart", K::Quantity, D::Quantity(0.0, "km/s")),
            param("mstep", K::Quantity, D::Quantity(0.0, "km/s")),
            param("msselect", K::Text, D::Text("")),
        ],
        R::Bool,
        S::Implemented,
    ),
    method(
        "setapply",
        &[
            param("type", K::Text, D::Text("")),
            param("t", K::Float, D::Float(0.0)),
            param("table", K::Text, D::Text("")),
            param("field", K::Any, D::Empty),
            param("interp", K::Text, D::Text("linear")),
            param("select", K::Text, D::Text("")),
            param("calwt", K::Bool, D::Bool(false)),
            param("spwmap", K::Ints, D::Ints(&[-1])),
            param("opacity", K::Floats, D::Floats(&[0.0])),
        ],
        R::Bool,
        S::Partial,
    ),
    method(
        "setcallib",
        &[param("callib", K::Record, D::EmptyRecord)],
        R::Bool,
        S::Partial,
    ),
    method(
        "setcorrdepflags",
        &[param("corrdepflags", K::Bool, D::Required)],
        R::Bool,
        S::Implemented,
    ),
    method(
        "setmodel",
        &[param("modelimage", K::Text, D::Required)],
        R::Bool,
        S::Partial,
    ),
    method(
        "setptmodel",
        &[param("stokes", K::Floats, D::Floats(&[0.0, 0.0, 0.0, 0.0]))],
        R::Bool,
        S::Implemented,
    ),
    method(
        "setsolve",
        &[
            param("type", K::Text, D::Text("MF")),
            param("t", K::Any, D::Empty),
            param("table", K::Text, D::Text("")),
            param("append", K::Bool, D::Bool(false)),
            param("preavg", K::Float, D::Float(-1.0)),
            param("phaseonly", K::Bool, D::Bool(false)),
            param("apmode", K::Text, D::Text("AP")),
            param("refant", K::Any, D::Empty),
            param("refantmode", K::Text, D::Text("flex")),
            param("minblperant", K::Int, D::Int(4)),
            param("solnorm", K::Bool, D::Bool(false)),
            param("normtype", K::Text, D::Text("mean")),
            param("minsnr", K::Float, D::Float(0.0)),
            param("combine", K::Text, D::Text("")),
            param("fillgaps", K::Int, D::Int(0)),
            param("cfcache", K::Text, D::Text("")),
            param("painc", K::Float, D::Float(360.0)),
            param("fitorder", K::Int, D::Int(0)),
            param("fraction", K::Float, D::Float(0.1)),
            param("numedge", K::Int, D::Int(-1)),
            param("radius", K::Text, D::Text("")),
            param("smooth", K::Bool, D::Bool(true)),
            param("zerorates", K::Bool, D::Bool(false)),
            param("globalsolve", K::Bool, D::Bool(true)),
            param("niter", K::Int, D::Int(100)),
            param("corrcomb", K::Text, D::Text("none")),
            param("delaywindow", K::Any, D::Empty),
            param("ratewindow", K::Any, D::Empty),
            param("paramactive", K::Any, D::Empty),
            param("concatspws", K::Bool, D::Bool(true)),
            param("solmode", K::Text, D::Text("")),
            param("rmsthresh", K::Any, D::Empty),
        ],
        R::Bool,
        S::Partial,
    ),
    method(
        "setsolvebandpoly",
        &[
            param("table", K::Text, D::Text("")),
            param("append", K::Bool, D::Bool(false)),
            param("t", K::Any, D::Empty),
            param("combine", K::Text, D::Text("")),
            param("degamp", K::Int, D::Int(3)),
            param("degphase", K::Int, D::Int(3)),
            param("visnorm", K::Bool, D::Bool(false)),
            param("solnorm", K::Bool, D::Bool(true)),
            param("maskcenter", K::Int, D::Int(0)),
            param("maskedge", K::Float, D::Float(5.0)),
            param("refant", K::Any, D::Empty),
        ],
        R::Bool,
        S::Implemented,
    ),
    method(
        "setsolvegainspline",
        &[
            param("table", K::Text, D::Text("")),
            param("append", K::Bool, D::Bool(false)),
            param("mode", K::Text, D::Text("PHAS")),
            param("splinetime", K::Float, D::Float(10800.0)),
            param("preavg", K::Float, D::Float(0.0)),
            param("npointaver", K::Int, D::Int(10)),
            param("phasewrap", K::Float, D::Float(250.0)),
            param("refant", K::Any, D::Empty),
        ],
        R::Bool,
        S::Implemented,
    ),
    method(
        "setvi",
        &[
            param("old", K::Bool, D::Bool(false)),
            param("quiet", K::Bool, D::Bool(false)),
        ],
        R::Bool,
        S::Implemented,
    ),
    Method {
        mutating: true,
        ..method(
            "smooth",
            &[
                param("tablein", K::Text, D::Required),
                param("tableout", K::Text, D::Required),
                param("field", K::Any, D::Empty),
                param("smoothtype", K::Text, D::Text("median")),
                param("smoothtime", K::Float, D::Float(60.0)),
                param("ratesmooth", K::Bool, D::Bool(false)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    Method {
        mutating: true,
        ..method("solve", &[], R::Bool, S::Partial)
    },
    Method {
        mutating: true,
        ..method(
            "specifycal",
            &[
                param("caltable", K::Text, D::Text("")),
                param("time", K::Text, D::Text("")),
                param("spw", K::Text, D::Text("")),
                param("antenna", K::Text, D::Text("")),
                param("pol", K::Text, D::Text("")),
                param("caltype", K::Text, D::Text("")),
                param("parameter", K::Floats, D::Floats(&[1.0])),
                param("infile", K::Text, D::Text("")),
                param("uniform", K::Bool, D::Bool(true)),
            ],
            R::Bool,
            S::Implemented,
        )
    },
    method("state", &[], R::Bool, S::Implemented),
    Method {
        mutating: true,
        ..method(
            "updatecaltable",
            &[param("caltable", K::Text, D::Required)],
            R::Bool,
            S::Implemented,
        )
    },
    method(
        "validatecallib",
        &[param("callib", K::Record, D::EmptyRecord)],
        R::Bool,
        S::Implemented,
    ),
];

pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "calibrater",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "close", "done"],
};
