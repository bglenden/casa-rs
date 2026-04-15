// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical object-surface schema bundle for `casars.data`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use casa_provider_contracts::{
    ObjectConstructorDescriptor, ObjectMethodDescriptor, ObjectPropertyDescriptor,
    ObjectSemanticContract, ObjectTypeContract, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ImagePixelTypeDescriptor {
    Float32,
    Float64,
    Complex64,
    Complex128,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct OpenObjectRequest {
    /// Existing persistent image/table path.
    path: PathBuf,
    /// Whether the opened object permits writes.
    #[serde(default)]
    writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct ArrayRegionRequest {
    /// Inclusive starting coordinate in CASA/Rust axis order.
    start: Vec<usize>,
    /// Requested region shape in CASA/Rust axis order.
    shape: Vec<usize>,
    /// Optional positive per-axis stride.
    stride: Option<Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct ImagePlaneRequest {
    /// Axis orthogonal to the returned plane.
    axis: usize,
    /// Plane index along `axis`.
    index: usize,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LogicalColumnInputValue {
    /// Dense column payload that projects to a NumPy array.
    NdArray(LogicalNdArray),
    /// Row-wise payload list for strings, records, or variable-shape arrays.
    ValueList(Vec<LogicalInputValue>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LogicalColumnOutputValue {
    /// Dense column payload that projects to a NumPy array.
    NdArray(LogicalNdArray),
    /// Row-wise payload list for strings, records, missing values, or variable-shape arrays.
    ValueList(Vec<LogicalOutputValue>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct ImagePutSliceRequest {
    /// Logical array payload written into the image.
    data: LogicalNdArray,
    /// Inclusive starting coordinate in CASA/Rust axis order.
    start: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct TableColumnKeywordsRequest {
    /// Target column name.
    column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct TableCellRequest {
    /// Target row index.
    row: usize,
    /// Target column name.
    column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct TableSetCellRequest {
    /// Target row index.
    row: usize,
    /// Target column name.
    column: String,
    /// Replacement value encoded with the logical CASA/Python input value model.
    value: LogicalInputValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct TableColumnRequest {
    /// Target column name.
    column: String,
    /// Starting row index.
    #[serde(default)]
    start: usize,
    /// Number of rows to read; `null` means to the end.
    count: Option<usize>,
    /// Positive row stride.
    #[serde(default = "one")]
    step: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct TablePutColumnRequest {
    /// Target column name.
    column: String,
    /// Replacement values encoded with the logical CASA/Python input value model.
    values: LogicalColumnInputValue,
    /// Starting row index.
    #[serde(default)]
    start: usize,
    /// Positive row stride.
    #[serde(default = "one")]
    step: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct TableSetColumnKeywordsRequest {
    /// Target column name.
    column: String,
    /// Replacement keyword record.
    keywords: BTreeMap<String, LogicalInputValue>,
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
        let open_schema = schema_for!(OpenObjectRequest);
        let array_region_schema = schema_for!(ArrayRegionRequest);
        let plane_request_schema = schema_for!(ImagePlaneRequest);
        let logical_array_schema = schema_for!(LogicalNdArray);
        let optional_logical_array_schema = schema_for!(Option<LogicalNdArray>);
        let image_put_slice_schema = schema_for!(ImagePutSliceRequest);
        let pixel_type_schema = schema_for!(ImagePixelTypeDescriptor);
        let input_scalar_schema = schema_for!(LogicalInputScalarValue);
        let output_scalar_schema = schema_for!(LogicalOutputScalarValue);
        let input_value_schema = schema_for!(LogicalInputValue);
        let output_value_schema = schema_for!(LogicalOutputValue);
        let input_record_schema = schema_for!(BTreeMap<String, LogicalInputValue>);
        let output_record_schema = schema_for!(BTreeMap<String, LogicalOutputValue>);
        let optional_output_record_schema =
            schema_for!(Option<BTreeMap<String, LogicalOutputValue>>);
        let string_list_schema = schema_for!(Vec<String>);
        let optional_string_schema = schema_for!(Option<String>);
        let shape_schema = schema_for!(Vec<usize>);
        let units_schema = schema_for!(String);
        let row_count_schema = schema_for!(usize);
        let table_cell_request_schema = schema_for!(TableCellRequest);
        let table_set_cell_schema = schema_for!(TableSetCellRequest);
        let table_column_request_schema = schema_for!(TableColumnRequest);
        let table_put_column_schema = schema_for!(TablePutColumnRequest);
        let table_column_keywords_request_schema = schema_for!(TableColumnKeywordsRequest);
        let table_set_column_keywords_schema = schema_for!(TableSetColumnKeywordsRequest);
        let column_input_schema = schema_for!(LogicalColumnInputValue);
        let column_output_schema = schema_for!(LogicalColumnOutputValue);
        let written_count_schema = schema_for!(usize);

        let components = merged_components([
            &open_schema,
            &array_region_schema,
            &plane_request_schema,
            &logical_array_schema,
            &optional_logical_array_schema,
            &image_put_slice_schema,
            &pixel_type_schema,
            &input_scalar_schema,
            &output_scalar_schema,
            &input_value_schema,
            &output_value_schema,
            &input_record_schema,
            &output_record_schema,
            &optional_output_record_schema,
            &string_list_schema,
            &optional_string_schema,
            &shape_schema,
            &units_schema,
            &row_count_schema,
            &table_cell_request_schema,
            &table_set_cell_schema,
            &table_column_request_schema,
            &table_put_column_schema,
            &table_column_keywords_request_schema,
            &table_set_column_keywords_schema,
            &column_input_schema,
            &column_output_schema,
            &written_count_schema,
        ]);

        Self {
            protocol: DataObjectProtocolInfo::current(),
            semantic: ObjectSemanticContract {
                objects: vec![
                    ObjectTypeContract {
                        name: "Image".to_string(),
                        constructors: vec![ObjectConstructorDescriptor {
                            name: "open".to_string(),
                            parameters_schema: open_schema.clone(),
                        }],
                        properties: vec![
                            ObjectPropertyDescriptor {
                                name: "shape".to_string(),
                                value_schema: shape_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "pixel_type".to_string(),
                                value_schema: pixel_type_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "units".to_string(),
                                value_schema: units_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "image_info".to_string(),
                                value_schema: output_record_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "misc_info".to_string(),
                                value_schema: output_record_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "mask_names".to_string(),
                                value_schema: string_list_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "default_mask_name".to_string(),
                                value_schema: optional_string_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                        ],
                        methods: vec![
                            ObjectMethodDescriptor {
                                name: "get_slice".to_string(),
                                parameters_schema: array_region_schema.clone(),
                                result_schema: Some(logical_array_schema.clone()),
                                mutating: false,
                            },
                            ObjectMethodDescriptor {
                                name: "put_slice".to_string(),
                                parameters_schema: image_put_slice_schema.clone(),
                                result_schema: None,
                                mutating: true,
                            },
                            ObjectMethodDescriptor {
                                name: "get_plane".to_string(),
                                parameters_schema: plane_request_schema.clone(),
                                result_schema: Some(logical_array_schema.clone()),
                                mutating: false,
                            },
                            ObjectMethodDescriptor {
                                name: "get_mask_slice".to_string(),
                                parameters_schema: array_region_schema.clone(),
                                result_schema: Some(optional_logical_array_schema),
                                mutating: false,
                            },
                        ],
                        lifecycle_operations: Vec::new(),
                    },
                    ObjectTypeContract {
                        name: "Table".to_string(),
                        constructors: vec![ObjectConstructorDescriptor {
                            name: "open".to_string(),
                            parameters_schema: open_schema,
                        }],
                        properties: vec![
                            ObjectPropertyDescriptor {
                                name: "row_count".to_string(),
                                value_schema: row_count_schema,
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "column_names".to_string(),
                                value_schema: string_list_schema,
                                readable: true,
                                writable: false,
                            },
                            ObjectPropertyDescriptor {
                                name: "keywords".to_string(),
                                value_schema: output_record_schema.clone(),
                                readable: true,
                                writable: false,
                            },
                        ],
                        methods: vec![
                            ObjectMethodDescriptor {
                                name: "column_keywords".to_string(),
                                parameters_schema: table_column_keywords_request_schema,
                                result_schema: Some(optional_output_record_schema),
                                mutating: false,
                            },
                            ObjectMethodDescriptor {
                                name: "get_cell".to_string(),
                                parameters_schema: table_cell_request_schema,
                                result_schema: Some(output_value_schema),
                                mutating: false,
                            },
                            ObjectMethodDescriptor {
                                name: "set_cell".to_string(),
                                parameters_schema: table_set_cell_schema,
                                result_schema: None,
                                mutating: true,
                            },
                            ObjectMethodDescriptor {
                                name: "get_column".to_string(),
                                parameters_schema: table_column_request_schema,
                                result_schema: Some(column_output_schema),
                                mutating: false,
                            },
                            ObjectMethodDescriptor {
                                name: "put_column".to_string(),
                                parameters_schema: table_put_column_schema,
                                result_schema: Some(written_count_schema),
                                mutating: true,
                            },
                            ObjectMethodDescriptor {
                                name: "set_column_keywords".to_string(),
                                parameters_schema: table_set_column_keywords_schema,
                                result_schema: None,
                                mutating: true,
                            },
                        ],
                        lifecycle_operations: Vec::new(),
                    },
                ],
            },
            components,
            annotations: json!({
                "python": {
                    "module": "casars.data",
                    "notes": {
                        "logical_arrays": "LogicalNdArray values project to numpy.ndarray instances in the direct Python binding; the schema captures dtype, shape, and CASA/Rust axis-order semantics rather than a transport encoding.",
                        "records": "Record and keyword schemas map to nested dict[str, Any] values in Python.",
                        "logical_values": "LogicalInputValue and LogicalOutputValue capture the recursive CASA scalar/array/record value model used by the Python binding, including complex numbers and non-JSON array payloads."
                    },
                    "objects": {
                        "Image": {
                            "python_class": "casars.data.Image",
                            "return_types": {
                                "get_slice": "numpy.ndarray",
                                "get_plane": "numpy.ndarray",
                                "get_mask_slice": "numpy.ndarray | None"
                            }
                        },
                        "Table": {
                            "python_class": "casars.data.Table",
                            "return_types": {
                                "get_cell": "Any",
                                "get_column": "numpy.ndarray | list[numpy.ndarray] | list[Any]",
                                "column_keywords": "dict[str, Any] | None"
                            }
                        }
                    }
                }
            }),
            projections: ProviderProjectionMetadata {
                cli: None,
                ui_schema: None,
                python: Some(json!({
                    "module": "casars.data",
                    "objects": [
                        { "name": "Image", "class_name": "Image" },
                        { "name": "Table", "class_name": "Table" }
                    ]
                })),
            },
        }
    }

    /// Serialize the current bundle as pretty-printed JSON.
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

const fn one() -> usize {
    1
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
        assert_eq!(bundle.semantic.objects.len(), 2);
        assert_eq!(bundle.semantic.objects[0].name, "Image");
        assert_eq!(bundle.semantic.objects[1].name, "Table");
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

        let image = bundle
            .semantic
            .objects
            .iter()
            .find(|object| object.name == "Image")
            .expect("image object");
        let get_mask_slice = image
            .methods
            .iter()
            .find(|method| method.name == "get_mask_slice")
            .expect("get_mask_slice method");
        let result_schema = serde_json::to_value(
            get_mask_slice
                .result_schema
                .as_ref()
                .expect("result schema"),
        )
        .expect("serialize get_mask_slice schema");
        assert!(result_schema.to_string().contains("\"null\""));
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = DataObjectProtocolInfo::current();
        assert_eq!(info.protocol_name, DATA_OBJECT_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, DATA_OBJECT_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Object);
    }
}
