// SPDX-License-Identifier: LGPL-3.0-or-later
//! The tool surfaces: each CASA tool's methods with their parameters, value
//! kinds and the installed CASA binding's effective defaults, and the
//! object-surface contract, Python projection and annotations built from
//! them (ADR-0019).
//!
//! Each tool keeps one [`ToolSurface`] table. The pyo3 declarations carry the
//! same names and defaults (`signature` and `text_signature` must be
//! literals, so they cannot be generated from the table); every tool's
//! Python tests keep the three in step: `inspect.signature` of each method
//! must equal the table as the bundle publishes it, and the installed CASA
//! binding (`python/tests/data/casa_<tool>_signatures.json`).

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
    /// A sequence of `bool`, or one `bool`.
    Bools,
    /// CASA's `any` read as a quantity.
    Quantity,
    /// CASA's `any` read as an epoch, direction or position measure.
    Measure,
    /// A `dict` (CASA's `record`), such as a measure record.
    Record,
    /// CASA's `any` with no fixed reading.
    Any,
}

/// A default as the installed CASA binding declares it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CasaDefault {
    /// No default: the argument is required.
    Required,
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
    /// A list of `bool`.
    Bools(&'static [bool]),
    /// `[]`, the default of every `any` parameter.
    Empty,
    /// `{}`, the default of record parameters.
    EmptyRecord,
    /// `None` (casa-rs constructor controls).
    None,
}

impl CasaDefault {
    /// The default as JSON, as Python's `json` reads it back; `None` for a
    /// required parameter.
    pub(crate) fn to_json(self) -> Option<JsonValue> {
        Some(match self {
            Self::Required => return None,
            Self::Text(text) => json!(text),
            Self::Bool(flag) => json!(flag),
            Self::Float(value) => json!(value),
            Self::Int(value) => json!(value),
            Self::Floats(values) => json!(values),
            Self::Ints(values) => json!(values),
            Self::Texts(values) => json!(values),
            Self::Bools(values) => json!(values),
            Self::Empty => json!([]),
            Self::EmptyRecord => json!({}),
            Self::None => JsonValue::Null,
        })
    }
}

/// One parameter.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Parameter {
    pub(crate) name: &'static str,
    pub(crate) kind: Kind,
    pub(crate) default: CasaDefault,
}

/// What a method returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Returns {
    /// A `bool`.
    Bool,
    /// An `int`.
    Int,
    /// A `float`.
    Float,
    /// A `str`.
    Text,
    /// A `dict`: a quantity, measure or other CASA record.
    Record,
    /// A `list` of `str`.
    Texts,
    /// A NumPy array of floats.
    Floats,
    /// A NumPy array of integers.
    Ints,
    /// Whatever CASA's binding returns, not described further: another
    /// tool, an array of any type, or a value whose type depends on the
    /// arguments.
    Any,
}

impl Returns {
    fn schema(self) -> JsonValue {
        match self {
            Self::Any => json!({}),
            Self::Bool => json!({"type": "boolean"}),
            Self::Int => json!({"type": "integer"}),
            Self::Float => json!({"type": "number"}),
            Self::Text => json!({"type": "string"}),
            Self::Record => json!({"type": "object"}),
            Self::Texts => json!({"type": "array", "items": {"type": "string"}}),
            Self::Floats => json!({"type": "array", "items": {"type": "number"}}),
            Self::Ints => json!({"type": "array", "items": {"type": "integer"}}),
        }
    }
}

/// What casa-rs does with a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    /// Implemented.
    Implemented,
    /// Raises `NotImplementedError` until a later phase.
    NotYetImplemented,
    /// Disabled in CASA itself: logs CASA's error and returns `False`.
    DisabledInCasa,
    /// Deprecated in CASA: raises, naming the replacement.
    DeprecatedInCasa,
}

impl Status {
    fn name(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::NotYetImplemented => "not_yet_implemented",
            Self::DisabledInCasa => "disabled_in_casa",
            Self::DeprecatedInCasa => "deprecated_in_casa",
        }
    }
}

/// One method.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Method {
    pub(crate) name: &'static str,
    pub(crate) parameters: &'static [Parameter],
    pub(crate) returns: Returns,
    pub(crate) status: Status,
    /// Whether the method writes persistent data.
    pub(crate) mutating: bool,
}

/// A parameter.
pub(crate) const fn param(name: &'static str, kind: Kind, default: CasaDefault) -> Parameter {
    Parameter {
        name,
        kind,
        default,
    }
}

/// A method that does not write.
pub(crate) const fn method(
    name: &'static str,
    parameters: &'static [Parameter],
    returns: Returns,
    status: Status,
) -> Method {
    Method {
        name,
        parameters,
        returns,
        status,
        mutating: false,
    }
}

/// One tool class: its names, constructor and methods.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ToolSurface {
    /// The class name, CASA's.
    pub(crate) name: &'static str,
    /// The Python module that exposes the class.
    pub(crate) module: &'static str,
    /// The constructor's parameters: keyword-only casa-rs controls
    /// (ADR-0019 item 3); CASA's tool constructors take none.
    pub(crate) constructor: &'static [Parameter],
    /// Every public method of CASA's class.
    pub(crate) methods: &'static [Method],
    /// The methods that open, close or end the object's state.
    pub(crate) lifecycle: &'static [&'static str],
}

impl ToolSurface {
    /// The object contract of the class.
    pub(crate) fn object_contract(&self) -> ObjectTypeContract {
        ObjectTypeContract {
            name: self.name.to_string(),
            constructors: vec![ObjectConstructorDescriptor {
                name: self.name.to_string(),
                parameters_schema: parameters_schema(self.constructor),
            }],
            properties: Vec::new(),
            methods: self
                .methods
                .iter()
                .map(|method| ObjectMethodDescriptor {
                    name: method.name.to_string(),
                    parameters_schema: parameters_schema(method.parameters),
                    result_schema: Some(root_schema(method.returns.schema())),
                    mutating: method.mutating,
                })
                .collect(),
            lifecycle_operations: self
                .lifecycle
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        }
    }

    /// The Python projection of the class: its module and class, the
    /// parameter order of the constructor (keyword-only) and of each method
    /// (positional or keyword), which JSON Schema's unordered properties do
    /// not carry.
    pub(crate) fn python_projection(&self) -> JsonValue {
        let names = |parameters: &[Parameter]| {
            parameters
                .iter()
                .map(|parameter| parameter.name)
                .collect::<Vec<_>>()
        };
        let methods = self
            .methods
            .iter()
            .map(|method| (method.name.to_string(), json!(names(method.parameters))))
            .collect::<Map<_, _>>();
        json!({
            "name": self.name,
            "class_name": self.name,
            "module": self.module,
            "constructor_keyword_only": names(self.constructor),
            "method_parameters": methods,
        })
    }

    /// Python annotations: the class and what casa-rs does with each method.
    pub(crate) fn python_annotation(&self) -> JsonValue {
        let status = self
            .methods
            .iter()
            .map(|method| (method.name.to_string(), json!(method.status.name())))
            .collect::<Map<_, _>>();
        json!({
            "python_class": format!("{}.{}", self.module, self.name),
            "casa_tool": self.name,
            "casa_version": "6.7.6",
            "method_status": status,
        })
    }
}

/// A JSON Schema object of `parameters` with their defaults.
fn parameters_schema(parameters: &[Parameter]) -> RootSchema {
    let mut required = Vec::new();
    let properties = parameters
        .iter()
        .map(|parameter| {
            let mut schema = kind_schema(parameter.kind);
            if schema.get("$ref").is_some() {
                schema = json!({"allOf": [schema]});
            }
            if parameter.default == CasaDefault::None {
                schema = json!({"anyOf": [schema, {"type": "null"}]});
            }
            match parameter.default.to_json() {
                Some(default) => schema["default"] = default,
                None => required.push(parameter.name),
            }
            (parameter.name.to_string(), schema)
        })
        .collect::<Map<_, _>>();
    let mut root = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
        "additionalProperties": false,
    });
    if !required.is_empty() {
        root["required"] = json!(required);
    }
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
        Kind::Bools => list_of(json!({"type": "boolean"})),
        Kind::Quantity => json!({"$ref": "#/definitions/CasaQuantityArgument"}),
        Kind::Measure => json!({"$ref": "#/definitions/CasaMeasureArgument"}),
        Kind::Record => json!({"type": "object"}),
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
    serde_json::from_value(value).expect("the tool surface schemas are valid JSON Schema")
}

#[cfg(test)]
mod tests {
    use super::{CasaDefault, Kind, Returns, Status, ToolSurface, method, param};

    const PARAMETERS: [super::Parameter; 2] = [
        param("v", Kind::Record, CasaDefault::Required),
        param("off", Kind::Record, CasaDefault::EmptyRecord),
    ];
    const SURFACE: ToolSurface = ToolSurface {
        name: "probe",
        module: "casars.tools",
        constructor: &[],
        methods: &[method(
            "m",
            &PARAMETERS,
            Returns::Record,
            Status::Implemented,
        )],
        lifecycle: &[],
    };

    #[test]
    fn required_parameters_have_no_default_and_are_listed() {
        let contract = SURFACE.object_contract();
        let schema = serde_json::to_value(&contract.methods[0].parameters_schema).unwrap();
        assert_eq!(schema["required"], serde_json::json!(["v"]));
        assert!(schema["properties"]["v"].get("default").is_none());
        assert_eq!(
            schema["properties"]["off"]["default"],
            serde_json::json!({})
        );
        assert_eq!(
            schema["properties"]["off"]["type"],
            serde_json::json!("object")
        );
        let annotation = SURFACE.python_annotation();
        assert_eq!(annotation["python_class"], "casars.tools.probe");
        assert_eq!(annotation["method_status"]["m"], "implemented");
    }
}
