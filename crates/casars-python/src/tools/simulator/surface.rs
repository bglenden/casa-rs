// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `simulator` tool's surface: every method of CASA's installed
//! `simulator` binding with its parameters, value kinds and effective
//! defaults (CASA 6.7.6), in the shared [`crate::tools::surface`] form.
//!
//! The pyo3 declarations in [`super`] carry the same names and defaults.
//! `python/tests/test_tools_simulator.py` keeps the three in step: it checks
//! that `inspect.signature` of every method equals this table as the bundle
//! publishes it, and equals the installed CASA binding
//! (`python/tests/data/casa_simulator_signatures.json`).

use crate::tools::surface::{
    CasaDefault, Kind, Method, Parameter, Returns, Status, ToolSurface, param,
};

/// A simulator method: they all return a `bool`, except `name` and `type`.
const fn method(name: &'static str, parameters: &'static [Parameter], status: Status) -> Method {
    crate::tools::surface::method(name, parameters, Returns::Bool, status)
}

use CasaDefault::{Bool, Empty, Float, Floats, Int, Ints, Text, Texts};
use Kind as K;
use Status::{DeprecatedInCasa, DisabledInCasa, Implemented, NotYetImplemented};

/// The keyword-only constructor controls CASA lacks (ADR-0019 item 3), with
/// casa-rs's defaults: CASA's constructor takes none.
const CONSTRUCTOR: &[Parameter] = &[
    param("memory_budget", K::Int, CasaDefault::None),
    param("backend", K::Text, Text("auto")),
    param("workers", K::Int, CasaDefault::None),
];

const STATE: [Parameter; 9] = [
    param("add_observation", K::Bool, Bool(false)),
    param("state_sig", K::Bool, Bool(true)),
    param("state_ref", K::Bool, Bool(false)),
    param("state_cal", K::Float, Float(0.0)),
    param("state_load", K::Float, Float(0.0)),
    param("state_sub_scan", K::Int, Int(0)),
    param("state_obs_mode", K::Text, Text("OBSERVE_TARGET.ON_SOURCE")),
    param("observer", K::Text, Text("CASA simulator")),
    param("project", K::Text, Text("CASA simulation")),
];

const OBSERVE: [Parameter; 13] = [
    param("sourcename", K::Text, Text("")),
    param("spwname", K::Text, Text("")),
    param("starttime", K::Quantity, Empty),
    param("stoptime", K::Quantity, Empty),
    STATE[0],
    STATE[1],
    STATE[2],
    STATE[3],
    STATE[4],
    STATE[5],
    STATE[6],
    STATE[7],
    STATE[8],
];

const OBSERVEMANY: [Parameter; 14] = [
    param("sourcenames", K::Texts, Texts(&[])),
    param("spwname", K::Text, Text("")),
    param("starttimes", K::Texts, Texts(&["0s"])),
    param("stoptimes", K::Texts, Texts(&["3600s"])),
    param("directions", K::Texts, Texts(&[""])),
    STATE[0],
    STATE[1],
    STATE[2],
    STATE[3],
    STATE[4],
    STATE[5],
    // CASA's observemany default differs from observe's.
    param("state_obs_mode", K::Text, Text("OBSERVE_TARGET#ON_SOURCE")),
    STATE[7],
    STATE[8],
];

const SETCONFIG: [Parameter; 11] = [
    param("telescopename", K::Text, Text("VLA")),
    param("x", K::Floats, Floats(&[0.0])),
    param("y", K::Floats, Floats(&[0.0])),
    param("z", K::Floats, Floats(&[0.0])),
    param("dishdiameter", K::Floats, Floats(&[0.0])),
    param("offset", K::Floats, Floats(&[0.0])),
    param("mount", K::Texts, Texts(&["ALT-AZ"])),
    param("antname", K::Texts, Texts(&["A"])),
    param("padname", K::Texts, Texts(&["P"])),
    param("coordsystem", K::Text, Text("global")),
    param("referencelocation", K::Measure, Empty),
];

const SETNOISE: [Parameter; 19] = [
    param("mode", K::Text, Text("simplenoise")),
    param("table", K::Text, Text("")),
    param("simplenoise", K::Quantity, Empty),
    param("pground", K::Quantity, Empty),
    param("relhum", K::Float, Float(20.0)),
    param("altitude", K::Quantity, Empty),
    param("waterheight", K::Quantity, Empty),
    param("pwv", K::Quantity, Empty),
    param("tatmos", K::Float, Float(250.0)),
    param("tau", K::Float, Float(0.1)),
    param("antefficiency", K::Float, Float(0.8)),
    param("spillefficiency", K::Float, Float(0.85)),
    param("correfficiency", K::Float, Float(0.88)),
    param("trx", K::Float, Float(50.0)),
    param("tground", K::Float, Float(270.0)),
    param("tcmb", K::Float, Float(2.73)),
    param("OTF", K::Bool, Bool(true)),
    param("senscoeff", K::Float, Float(0.0)),
    param("rxtype", K::Int, Int(0)),
];

const OLDSETNOISE: [Parameter; 10] = [
    param("mode", K::Text, Text("calculate")),
    param("table", K::Text, Text("")),
    param("simplenoise", K::Quantity, Empty),
    param("antefficiency", K::Float, Float(0.8)),
    param("correfficiency", K::Float, Float(0.85)),
    param("spillefficiency", K::Float, Float(0.85)),
    param("tau", K::Float, Float(0.1)),
    param("trx", K::Float, Float(50.0)),
    param("tatmos", K::Float, Float(230.0)),
    param("tcmb", K::Float, Float(2.7)),
];

const MS: [Parameter; 1] = [param("ms", K::Text, Text(""))];

/// Every public method of CASA's installed `simulator` binding, in its
/// order.
const METHODS: &[Method] = &[
    Method {
        mutating: true,
        ..method("open", &MS, Implemented)
    },
    method("openfromms", &MS, NotYetImplemented),
    Method {
        mutating: true,
        ..method("close", &[], Implemented)
    },
    Method {
        mutating: true,
        ..method("done", &[], Implemented)
    },
    Method {
        returns: Returns::Text,
        ..method("name", &[], Implemented)
    },
    method("summary", &[], Implemented),
    Method {
        returns: Returns::Text,
        ..method("type", &[], Implemented)
    },
    method(
        "settimes",
        &[
            param("integrationtime", K::Quantity, Empty),
            param("usehourangle", K::Bool, Bool(true)),
            param("referencetime", K::Measure, Empty),
        ],
        Implemented,
    ),
    method("observe", &OBSERVE, Implemented),
    method("observemany", &OBSERVEMANY, NotYetImplemented),
    method(
        "setlimits",
        &[
            param("shadowlimit", K::Float, Float(1e-6)),
            param("elevationlimit", K::Quantity, Empty),
        ],
        Implemented,
    ),
    method(
        "setauto",
        &[param("autocorrwt", K::Float, Float(0.0))],
        Implemented,
    ),
    method("setconfig", &SETCONFIG, Implemented),
    method(
        "setfeed",
        &[
            param("mode", K::Text, Text("")),
            param("x", K::Floats, Floats(&[0.0])),
            param("y", K::Floats, Floats(&[0.0])),
            param("pol", K::Texts, Texts(&["R"])),
        ],
        Implemented,
    ),
    method(
        "setfield",
        &[
            param("sourcename", K::Text, Text("SOURCE")),
            param("sourcedirection", K::Measure, Empty),
            param("calcode", K::Text, Text("")),
            param("distance", K::Quantity, Empty),
        ],
        Implemented,
    ),
    method(
        "setmosaicfield",
        &[
            param("sourcename", K::Text, Text("SOURCE")),
            param("calcode", K::Text, Text("")),
            param("fieldcenter", K::Measure, Empty),
            param("xmosp", K::Int, Int(1)),
            param("ymosp", K::Int, Int(1)),
            param("mosspacing", K::Quantity, Empty),
            param("distance", K::Quantity, Empty),
        ],
        NotYetImplemented,
    ),
    method(
        "setspwindow",
        &[
            param("spwname", K::Text, Text("XBAND")),
            param("freq", K::Quantity, Empty),
            param("deltafreq", K::Quantity, Empty),
            param("freqresolution", K::Quantity, Empty),
            param("refcode", K::Text, Text("TOPO")),
            param("nchannels", K::Int, Int(1)),
            param("stokes", K::Text, Text("RR LL")),
        ],
        Implemented,
    ),
    method(
        "setdata",
        &[
            param("spwid", K::Ints, Ints(&[0])),
            param("fieldid", K::Ints, Ints(&[0])),
            param("msselect", K::Text, Text("")),
        ],
        NotYetImplemented,
    ),
    method(
        "predict",
        &[
            param("imagename", K::Texts, Texts(&[])),
            param("complist", K::Text, Text("")),
            param("incremental", K::Bool, Bool(false)),
        ],
        NotYetImplemented,
    ),
    method(
        "setoptions",
        &[
            param("ftmachine", K::Text, Text("ft")),
            param("cache", K::Int, Int(0)),
            param("tile", K::Int, Int(16)),
            param("gridfunction", K::Text, Text("SF")),
            param("location", K::Measure, Empty),
            param("padding", K::Float, Float(1.3)),
            param("facets", K::Int, Int(1)),
            param("maxdata", K::Float, Float(2000.0)),
            param("wprojplanes", K::Int, Int(1)),
        ],
        NotYetImplemented,
    ),
    method(
        "setvp",
        &[
            param("dovp", K::Bool, Bool(true)),
            param("usedefaultvp", K::Bool, Bool(true)),
            param("vptable", K::Text, Text("")),
            param("dosquint", K::Bool, Bool(true)),
            param("parangleinc", K::Quantity, Empty),
            param("skyposthreshold", K::Quantity, Empty),
            param("pblimit", K::Float, Float(0.01)),
        ],
        NotYetImplemented,
    ),
    method("corrupt", &[], Implemented),
    method("reset", &[], Implemented),
    method(
        "setbandpass",
        &[
            param("mode", K::Text, Text("calculate")),
            param("table", K::Text, Text("")),
            param("interval", K::Quantity, Empty),
            param("amplitude", K::Floats, Floats(&[0.0])),
        ],
        DisabledInCasa,
    ),
    method(
        "setapply",
        &[
            param("table", K::Text, Text("")),
            param("type", K::Text, Text("")),
            param("t", K::Float, Float(0.0)),
            param("field", K::Any, Empty),
            param("interp", K::Text, Text("linear")),
            param("calwt", K::Bool, Bool(false)),
            param("spwmap", K::Ints, Ints(&[-1])),
            param("opacity", K::Float, Float(0.0)),
        ],
        NotYetImplemented,
    ),
    method(
        "setgain",
        &[
            param("mode", K::Text, Text("fbm")),
            param("table", K::Text, Text("")),
            param("interval", K::Quantity, Empty),
            param("amplitude", K::Floats, Floats(&[0.01])),
        ],
        NotYetImplemented,
    ),
    method(
        "settrop",
        &[
            param("mode", K::Text, Text("screen")),
            param("table", K::Text, Text("")),
            param("pwv", K::Float, Float(3.0)),
            param("deltapwv", K::Float, Float(0.15)),
            param("beta", K::Float, Float(1.1)),
            param("windspeed", K::Float, Float(7.0)),
            param("simint", K::Float, Float(-1.0)),
        ],
        NotYetImplemented,
    ),
    method(
        "setpointingerror",
        &[
            param("epjtablename", K::Text, Text("")),
            param("applypointingoffsets", K::Bool, Bool(false)),
            param("dopbcorrection", K::Bool, Bool(false)),
        ],
        DisabledInCasa,
    ),
    method(
        "setleakage",
        &[
            param("mode", K::Text, Text("constant")),
            param("table", K::Text, Text("")),
            param("amplitude", K::Floats, Floats(&[0.01])),
            param("offset", K::Floats, Floats(&[0.0])),
        ],
        NotYetImplemented,
    ),
    method("oldsetnoise", &OLDSETNOISE, DeprecatedInCasa),
    method("setnoise", &SETNOISE, Implemented),
    method(
        "setpa",
        &[
            param("mode", K::Text, Text("calculate")),
            param("table", K::Text, Text("")),
            param("interval", K::Quantity, Empty),
        ],
        DisabledInCasa,
    ),
    method(
        "setseed",
        &[param("seed", K::Int, Int(185_349_251))],
        Implemented,
    ),
];

/// `casars.tools.simulator`.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "simulator",
    module: "casars.tools",
    constructor: CONSTRUCTOR,
    methods: METHODS,
    lifecycle: &["open", "close", "done"],
};
