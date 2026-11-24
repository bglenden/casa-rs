// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical object-surface schema bundle for `casars.data` and the CASA
//! tools of `casars.tools`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use casa_provider_contracts::{
    ObjectSemanticContract, ObjectTypeContract, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::tools::TOOLS;

/// Stable protocol name advertised by the `casars.data` object bundle.
pub const DATA_OBJECT_PROTOCOL_NAME: &str = "casars_data_objects";
/// Stable protocol version advertised by the `casars.data` object bundle.
pub const DATA_OBJECT_PROTOCOL_VERSION: u32 = 1;

/// Version/compatibility information for the object-surface schema bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DataObjectProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Python binding version implementing the contract.
    pub binding_version: String,
}

impl DataObjectProtocolInfo {
    /// Build the current `casars.data` protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: DATA_OBJECT_PROTOCOL_NAME.to_string(),
            protocol_version: DATA_OBJECT_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Object,
            binding_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct LogicalComplex64 {
    /// Real component.
    real: f32,
    /// Imaginary component.
    imag: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct LogicalComplex128 {
    /// Real component.
    real: f64,
    /// Imaginary component.
    imag: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LogicalInputScalarValue {
    /// Python/Rust bool.
    Bool(bool),
    /// Python int mapped to CASA Int64.
    Int64(i64),
    /// Python float mapped to CASA Double.
    Float64(f64),
    /// Python complex mapped to CASA DComplex.
    Complex128(LogicalComplex128),
    /// Python str mapped to CASA String.
    String(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LogicalOutputScalarValue {
    /// CASA Bool.
    Bool(bool),
    /// CASA uChar.
    UInt8(u8),
    /// CASA uShort.
    UInt16(u16),
    /// CASA uInt.
    UInt32(u32),
    /// CASA Short.
    Int16(i16),
    /// CASA Int.
    Int32(i32),
    /// CASA Int64.
    Int64(i64),
    /// CASA Float.
    Float32(f32),
    /// CASA Double.
    Float64(f64),
    /// CASA Complex.
    Complex64(LogicalComplex64),
    /// CASA DComplex.
    Complex128(LogicalComplex128),
    /// CASA String.
    String(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct LogicalNdArray {
    /// Logical dtype name such as `float32`, `complex128`, or `bool`.
    dtype: String,
    /// Shape in CASA/Rust axis order.
    shape: Vec<usize>,
    /// Semantic axis-order label carried across projections.
    axis_order: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LogicalInputValue {
    /// Scalar CASA/Python value.
    Scalar(LogicalInputScalarValue),
    /// N-dimensional typed array.
    NdArray(LogicalNdArray),
    /// Nested record value.
    Record(BTreeMap<String, LogicalInputValue>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LogicalOutputValue {
    /// Missing value represented as Python `None`.
    Null,
    /// Scalar CASA/Python value.
    Scalar(LogicalOutputScalarValue),
    /// N-dimensional typed array.
    NdArray(LogicalNdArray),
    /// Nested record value.
    Record(BTreeMap<String, LogicalOutputValue>),
    /// Table-reference path returned as a Python string.
    TableRef(PathBuf),
}

/// Canonical schema bundle for the `casars.data` object surface.
#[derive(Debug, Clone, Serialize)]
pub struct DataObjectSchemaBundle {
    /// Compatibility descriptor for the object-surface bundle.
    pub protocol: DataObjectProtocolInfo,
    /// Canonical object semantic contract.
    pub semantic: ObjectSemanticContract,
    /// Shared component schemas reusable across objects.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Projection metadata for direct Python consumers.
    pub projections: ProviderProjectionMetadata,
}

impl DataObjectSchemaBundle {
    /// Build the current object-surface schema bundle.
    pub fn current() -> Self {
        let input_scalar_schema = schema_for!(LogicalInputScalarValue);
        let output_scalar_schema = schema_for!(LogicalOutputScalarValue);
        let input_value_schema = schema_for!(LogicalInputValue);
        let output_value_schema = schema_for!(LogicalOutputValue);
        let input_record_schema = schema_for!(BTreeMap<String, LogicalInputValue>);
        let output_record_schema = schema_for!(BTreeMap<String, LogicalOutputValue>);
        let tools = Self::tool_contracts();

        let mut components = ProviderComponentSchemas::new();
        for tool in &tools {
            components.extend(Self::object_components(tool));
        }
        components.extend(merged_components([
            &input_scalar_schema,
            &output_scalar_schema,
            &input_value_schema,
            &output_value_schema,
            &input_record_schema,
            &output_record_schema,
        ]));

        Self {
            protocol: DataObjectProtocolInfo::current(),
            semantic: ObjectSemanticContract { objects: tools },
            components,
            annotations: Self::python_annotations(),
            projections: Self::python_projections(),
        }
    }

    /// The object contracts of the CASA tools of `casars.tools`, in
    /// [`TOOLS`] order. Every tool entry of the bundle (contract,
    /// annotations, projection) comes from that one list.
    fn tool_contracts() -> Vec<ObjectTypeContract> {
        TOOLS.iter().map(|tool| tool.object_contract()).collect()
    }

    /// The component schemas an object's constructors and methods define.
    fn object_components(object: &ObjectTypeContract) -> ProviderComponentSchemas {
        let constructors = object
            .constructors
            .iter()
            .map(|constructor| &constructor.parameters_schema);
        let methods = object
            .methods
            .iter()
            .map(|method| &method.parameters_schema);
        merged_components(constructors.chain(methods))
    }

    /// Python presentation annotations: the value-model notes and the Python
    /// class and return types of each object.
    fn python_annotations() -> JsonValue {
        let mut annotations = json!({
            "python": {
                "module": "casars.data",
                "notes": {
                    "logical_arrays": "LogicalNdArray values project to numpy.ndarray instances in the direct Python binding; the schema captures dtype, shape, and CASA/Rust axis-order semantics rather than a transport encoding.",
                    "records": "Record and keyword schemas map to nested dict[str, Any] values in Python.",
                    "logical_values": "LogicalInputValue and LogicalOutputValue capture the recursive CASA scalar/array/record value model used by the Python binding, including complex numbers and non-JSON array payloads."
                },
                "objects": {}
            }
        });
        for tool in TOOLS {
            annotations["python"]["objects"][tool.name] = tool.python_annotation();
        }
        annotations
    }

    /// Projection metadata mapping each object to its Python class: the
    /// CASA tools name their module and carry their parameter order.
    fn python_projections() -> ProviderProjectionMetadata {
        let objects = TOOLS
            .iter()
            .map(|tool| tool.python_projection())
            .collect::<Vec<_>>();
        ProviderProjectionMetadata {
            cli: None,
            python: Some(json!({
                "module": "casars.data",
                "objects": objects
            })),
        }
    }

    /// Serialize the current bundle as pretty-printed JSON.
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::ProviderSurfaceKind;

    use super::{
        DATA_OBJECT_PROTOCOL_NAME, DATA_OBJECT_PROTOCOL_VERSION, DataObjectProtocolInfo,
        DataObjectSchemaBundle,
    };

    #[test]
    fn bundle_uses_current_protocol_and_objects() {
        let bundle = DataObjectSchemaBundle::current();
        assert_eq!(bundle.protocol.protocol_name, DATA_OBJECT_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            DATA_OBJECT_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Object);
        let names = bundle
            .semantic
            .objects
            .iter()
            .map(|object| object.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "simulator",
                "measures",
                "quanta",
                "componentlist",
                "image",
                "coordsys",
                "table",
                "msmetadata",
                "ms",
                "calibrater",
                "imager",
                "agentflagger",
                "wvr",
                "singledishms",
                "calanalysis",
                "atmosphere"
            ]
        );
        let annotated = bundle.annotations["python"]["objects"]
            .as_object()
            .expect("annotated objects");
        let projected = bundle
            .projections
            .python
            .as_ref()
            .expect("python projection")["objects"]
            .as_array()
            .expect("projected objects")
            .len();
        assert_eq!((annotated.len(), projected), (names.len(), names.len()));
        assert!(bundle.components.contains_key("CasaQuantityArgument"));
        assert!(bundle.components.contains_key("CasaMeasureArgument"));
        assert_eq!(
            bundle
                .projections
                .python
                .as_ref()
                .and_then(|value| value["module"].as_str()),
            Some("casars.data")
        );
        assert!(bundle.components.contains_key("LogicalNdArray"));
        assert!(bundle.components.contains_key("LogicalInputValue"));
        assert!(bundle.components.contains_key("LogicalOutputValue"));
        assert!(bundle.components.contains_key("LogicalComplex128"));
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = DataObjectProtocolInfo::current();
        assert_eq!(info.protocol_name, DATA_OBJECT_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, DATA_OBJECT_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Object);
    }
}
