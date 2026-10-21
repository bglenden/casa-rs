// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `simulator` tool's surface: every method of CASA's installed
//! `simulator` binding with its parameters, value kinds and effective
//! defaults (CASA 6.7.6), and the object-surface contract built from them.
//!
//! The pyo3 declarations in [`super`] carry the same names and defaults
//! (`signature` and `text_signature` must be literals, so they cannot be
//! generated from this table). `python/tests/test_tools_simulator.py` keeps
//! the three in step: it checks that `inspect.signature` of every method
//! equals this table as the bundle publishes it, and equals the installed
//! CASA binding (`python/tests/data/casa_simulator_signatures.json`).

use casa_provider_contracts::{
    ObjectConstructorDescriptor, ObjectMethodDescriptor, ObjectTypeContract,
};
use schemars::schema::RootSchema;
use serde_json::{Map, Value as JsonValue, json};

/// How a parameter's Python value is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A `str`.
    Text,
    /// A `bool`.
    Bool,
    /// A `float` (an `int` is accepted).
    Float,
    /// An `int`.
    Int,
    /// A sequence of numbers, or one number.
    Floats,
    /// A sequence of integers, or one integer.
    Ints,
    /// A sequence of `str`, or one `str`.
    Texts,
    /// CASA's `any` read as a quantity.
    Quantity,
    /// CASA's `any` read as an epoch, direction or position measure.
    Measure,
    /// CASA's `any` with no fixed reading.
    Any,
}

/// A default as the installed CASA binding declares it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CasaDefault {
    /// A `str`.
    Text(&'static str),
    /// A `bool`.
    Bool(bool),
    /// A `float`.
    Float(f64),
    /// An `int`.
    Int(i64),
    /// A list of floats.
    Floats(&'static [f64]),
    /// A list of ints.
    Ints(&'static [i64]),
    /// A list of `str`.
    Texts(&'static [&'static str]),
    /// `[]`, the default of every `any` parameter.
    Empty,
    /// `None` (casa-rs constructor controls only).
    None,
}

impl CasaDefault {
    /// The default as JSON, as Python's `json` reads it back.
    pub(crate) fn to_json(self) -> JsonValue {
        match self {
            Self::Text(text) => json!(text),
            Self::Bool(flag) => json!(flag),
            Self::Float(value) => json!(value),
            Self::Int(value) => json!(value),
            Self::Floats(values) => json!(values),
            Self::Ints(values) => json!(values),
            Self::Texts(values) => json!(values),
            Self::Empty => json!([]),
            Self::None => JsonValue::Null,
        }
    }
}

/// One parameter.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Parameter {
    pub(crate) name: &'static str,
    pub(crate) kind: Kind,
    pub(crate) default: CasaDefault,
}

/// What casa-rs does with a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    /// Implemented.
    Implemented,
    /// Raises `NotImplementedError` until a later simulation phase.
    NotYetImplemented,
    /// Disabled in CASA itself: logs CASA's error and returns `False`.
    DisabledInCasa,
    /// Deprecated in CASA: raises, naming the replacement.
    DeprecatedInCasa,
}

/// One method.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Method {
    pub(crate) name: &'static str,
    pub(crate) parameters: &'static [Parameter],
    /// Whether the method returns a `str` (else a `bool`).
    pub(crate) returns_text: bool,
    pub(crate) status: Status,
    /// Whether the method writes the MeasurementSet.
    pub(crate) writes: bool,
}

const fn param(name: &'static str, kind: Kind, default: CasaDefault) -> Parameter {
    Parameter {
        name,
        kind,
        default,
    }
}

const fn method(name: &'static str, parameters: &'static [Parameter], status: Status) -> Method {
    Method {
        name,
        parameters,
        returns_text: false,
        status,
        writes: false,
    }
}

use CasaDefault::{Bool, Empty, Float, Floats, Int, Ints, Text, Texts};
use Kind as K;
use Status::{DeprecatedInCasa, DisabledInCasa, Implemented, NotYetImplemented};

/// The keyword-only constructor controls CASA lacks (ADR-0019 item 3).
pub(crate) const CONSTRUCTOR: &[Parameter] = &[param("memory_budget", K::Int, CasaDefault::None)];

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
pub(crate) const METHODS: &[Method] = &[
    Method {
        writes: true,
        ..method("open", &MS, Implemented)
    },
    method("openfromms", &MS, NotYetImplemented),
    Method {
        writes: true,
        ..method("close", &[], Implemented)
    },
    Method {
        writes: true,
        ..method("done", &[], Implemented)
    },
    Method {
        returns_text: true,
        ..method("name", &[], Implemented)
    },
    method("summary", &[], Implemented),
    Method {
        returns_text: true,
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
    method("corrupt", &[], NotYetImplemented),
    method("reset", &[], NotYetImplemented),
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
    method("setnoise", &SETNOISE, NotYetImplemented),
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
        NotYetImplemented,
    ),
];

/// The object contract of `casars.tools.simulator`.
pub(crate) fn object_contract() -> ObjectTypeContract {
    ObjectTypeContract {
        name: "simulator".to_string(),
        constructors: vec![ObjectConstructorDescriptor {
            name: "simulator".to_string(),
            parameters_schema: parameters_schema(CONSTRUCTOR),
        }],
        properties: Vec::new(),
        methods: METHODS
            .iter()
            .map(|method| ObjectMethodDescriptor {
                name: method.name.to_string(),
                parameters_schema: parameters_schema(method.parameters),
                result_schema: Some(root_schema(if method.returns_text {
                    json!({"type": "string"})
                } else {
                    json!({"type": "boolean"})
                })),
                mutating: method.writes,
            })
            .collect(),
        lifecycle_operations: vec!["open".to_string(), "close".to_string(), "done".to_string()],
    }
}

/// The Python projection of the tool: its module and class, the parameter
/// order of the constructor (keyword-only) and of each method (positional or
/// keyword), which JSON Schema's unordered properties do not carry.
pub(crate) fn python_projection() -> JsonValue {
    let names = |parameters: &[Parameter]| {
        parameters
            .iter()
            .map(|parameter| parameter.name)
            .collect::<Vec<_>>()
    };
    let methods = METHODS
        .iter()
        .map(|method| (method.name.to_string(), json!(names(method.parameters))))
        .collect::<Map<_, _>>();
    json!({
        "name": "simulator",
        "class_name": "simulator",
        "module": "casars.tools",
        "constructor_keyword_only": names(CONSTRUCTOR),
        "method_parameters": methods,
    })
}

/// Python annotations: the class and what casa-rs does with each method.
pub(crate) fn python_annotation() -> JsonValue {
    let status = METHODS
        .iter()
        .map(|method| {
            let status = match method.status {
                Status::Implemented => "implemented",
                Status::NotYetImplemented => "not_yet_implemented",
                Status::DisabledInCasa => "disabled_in_casa",
                Status::DeprecatedInCasa => "deprecated_in_casa",
            };
            (method.name.to_string(), json!(status))
        })
        .collect::<Map<_, _>>();
    json!({
        "python_class": "casars.tools.simulator",
        "casa_tool": "simulator",
        "casa_version": "6.7.6",
        "method_status": status,
    })
}

/// A JSON Schema object of `parameters` with their defaults.
fn parameters_schema(parameters: &[Parameter]) -> RootSchema {
    let properties = parameters
        .iter()
        .map(|parameter| {
            let mut schema = kind_schema(parameter.kind);
            let default = parameter.default.to_json();
            if schema.get("$ref").is_some() {
                schema = json!({"allOf": [schema]});
            }
            if parameter.default == CasaDefault::None {
                schema = json!({"anyOf": [schema, {"type": "null"}]});
            }
            schema["default"] = default;
            (parameter.name.to_string(), schema)
        })
        .collect::<Map<_, _>>();
    let mut root = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
        "additionalProperties": false,
    });
    let definitions = argument_definitions(parameters);
    if !definitions.is_empty() {
        root["definitions"] = JsonValue::Object(definitions);
    }
    root_schema(root)
}

/// The schema of one value kind.
fn kind_schema(kind: Kind) -> JsonValue {
    let list_of =
        |item: JsonValue| json!({"anyOf": [item.clone(), {"type": "array", "items": item}]});
    match kind {
        Kind::Text => json!({"type": "string"}),
        Kind::Bool => json!({"type": "boolean"}),
        Kind::Float => json!({"type": "number"}),
        Kind::Int => json!({"type": "integer"}),
        Kind::Floats => list_of(json!({"type": "number"})),
        Kind::Ints => list_of(json!({"type": "integer"})),
        Kind::Texts => list_of(json!({"type": "string"})),
        Kind::Quantity => json!({"$ref": "#/definitions/CasaQuantityArgument"}),
        Kind::Measure => json!({"$ref": "#/definitions/CasaMeasureArgument"}),
        Kind::Any => json!({}),
    }
}

/// The shared definitions `parameters` refer to.
fn argument_definitions(parameters: &[Parameter]) -> Map<String, JsonValue> {
    let uses = |kind| parameters.iter().any(|parameter| parameter.kind == kind);
    let mut definitions = Map::new();
    let empty = json!({"type": "array", "maxItems": 0});
    if uses(Kind::Quantity) || uses(Kind::Measure) {
        definitions.insert(
            "CasaQuantityRecord".to_string(),
            json!({
                "description": "A quantity record, as qa.quantity returns it.",
                "type": "object",
                "properties": {"value": {"type": "number"}, "unit": {"type": "string"}},
                "required": ["value", "unit"],
            }),
        );
    }
    if uses(Kind::Quantity) {
        definitions.insert(
            "CasaQuantityArgument".to_string(),
            json!({
                "description": "A CASA quantity: a string such as '10s' or '12h30m', a quantity record, or a number in the parameter's SI unit; [] or None is 0.",
                "anyOf": [
                    {"type": "string"},
                    {"type": "number"},
                    {"$ref": "#/definitions/CasaQuantityRecord"},
                    empty,
                    {"type": "null"},
                ],
            }),
        );
    }
    if uses(Kind::Measure) {
        definitions.insert(
            "CasaMeasureArgument".to_string(),
            json!({
                "description": "A CASA measure: a string such as 'J2000 12h00m00 34d00m00', a list of such tokens, or a measure record; an epoch also takes a number of MJD days; [] or None is the parameter's default.",
                "anyOf": [
                    {"type": "string"},
                    {"type": "array", "items": {"type": "string"}},
                    {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "refer": {"type": "string"},
                            "m0": {"$ref": "#/definitions/CasaQuantityRecord"},
                            "m1": {"$ref": "#/definitions/CasaQuantityRecord"},
                            "m2": {"$ref": "#/definitions/CasaQuantityRecord"},
                        },
                        "required": ["type", "refer"],
                    },
                    {"type": "number"},
                    empty,
                    {"type": "null"},
                ],
            }),
        );
    }
    definitions
}

fn root_schema(value: JsonValue) -> RootSchema {
    serde_json::from_value(value).expect("the simulator surface schemas are valid JSON Schema")
}
