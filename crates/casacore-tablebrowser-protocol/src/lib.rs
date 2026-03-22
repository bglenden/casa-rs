// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wire protocol for the `tablebrowser` subprocess session.
//!
//! The protocol is versioned JSON Lines over stdio. Every client request and
//! server response is a single JSON object terminated by `\n`.
//!
//! Version `1` is intentionally read-only, but it already carries stable value
//! addresses, typed inspector payloads, and capability flags so later edit-mode
//! extensions can target the same contract without a wire redesign.

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

/// Current JSON protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Render viewport requested by the consumer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserViewport {
    /// Available content width in terminal cells.
    pub width: u16,
    /// Available content height in terminal cells.
    pub height: u16,
}

impl BrowserViewport {
    /// Construct a viewport from terminal-cell dimensions.
    pub const fn new(width: u16, height: u16) -> Self {
        Self { width, height }
    }
}

impl Default for BrowserViewport {
    fn default() -> Self {
        Self {
            width: 80,
            height: 24,
        }
    }
}

/// JSON Lines request envelope sent from `casars` to the browser backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserRequestEnvelope {
    /// Protocol version expected by the client.
    pub version: u32,
    /// Requested command.
    pub command: BrowserCommand,
}

impl BrowserRequestEnvelope {
    /// Wrap a command using the current protocol version.
    pub fn new(command: BrowserCommand) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            command,
        }
    }
}

/// Command sent to the browser backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum BrowserCommand {
    /// Open the root table for a new or existing session.
    OpenRoot {
        /// Filesystem path to the root table.
        path: String,
        /// Current render viewport.
        viewport: BrowserViewport,
    },
    /// Update the render viewport without changing selection state.
    Resize {
        /// Current render viewport.
        viewport: BrowserViewport,
    },
    /// Rotate between top-level browser views.
    CycleView {
        /// `true` for next view, `false` for previous view.
        forward: bool,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Move the active selection one or more steps upward.
    MoveUp {
        /// Number of steps to move.
        steps: usize,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Move the active selection one or more steps downward.
    MoveDown {
        /// Number of steps to move.
        steps: usize,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Move the active selection or page left.
    MoveLeft {
        /// Number of steps to move.
        steps: usize,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Move the active selection or page right.
    MoveRight {
        /// Number of steps to move.
        steps: usize,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Move by one or more pages upward.
    PageUp {
        /// Number of pages to move.
        pages: usize,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Move by one or more pages downward.
    PageDown {
        /// Number of pages to move.
        pages: usize,
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Activate the selected item or expand the selected inspector node.
    Activate {
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Pop back to the parent table, if any.
    Back {
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Collapse one inspector level or return focus to the main pane.
    Escape {
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
    /// Request the current snapshot without changing state.
    GetSnapshot {
        /// Optional viewport update applied before rendering.
        viewport: Option<BrowserViewport>,
    },
}

/// JSON Lines response envelope sent from the backend to `casars`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BrowserResponseEnvelope {
    /// Protocol version returned by the backend.
    pub version: u32,
    /// Response payload.
    pub response: BrowserResponse,
}

impl BrowserResponseEnvelope {
    /// Wrap a snapshot response using the current protocol version.
    pub fn snapshot(snapshot: BrowserSnapshot) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            response: BrowserResponse::Snapshot(Box::new(snapshot)),
        }
    }

    /// Wrap an error response using the current protocol version.
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            response: BrowserResponse::Error(BrowserErrorPayload {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

/// Backend response payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "response", rename_all = "snake_case")]
pub enum BrowserResponse {
    /// A full render snapshot after applying the requested command.
    Snapshot(Box<BrowserSnapshot>),
    /// A structured protocol or backend error.
    Error(BrowserErrorPayload),
}

/// Structured error payload returned by the browser backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserErrorPayload {
    /// Stable machine-readable error code.
    pub code: String,
    /// Human-readable explanation.
    pub message: String,
}

/// Top-level browser view.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BrowserView {
    /// Table summary and storage metadata.
    Overview,
    /// Column inventory and per-column metadata.
    Columns,
    /// Table and column keyword tree.
    Keywords,
    /// Row/column cell grid.
    Cells,
    /// Linked persistent subtables.
    Subtables,
}

impl BrowserView {
    /// Human-friendly tab label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Columns => "Columns",
            Self::Keywords => "Keywords",
            Self::Cells => "Cells",
            Self::Subtables => "Subtables",
        }
    }
}

/// Active focus target inside the browser.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BrowserFocus {
    /// Navigation is acting on the main view selection.
    Main,
    /// Navigation is acting on the inspector selection.
    Inspector,
}

/// Capabilities advertised by the backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserCapabilities {
    /// Whether edit operations are available for the current session.
    pub editable: bool,
}

/// Breadcrumb entry for the current table stack.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserBreadcrumbEntry {
    /// Display label used in breadcrumbs.
    pub label: String,
    /// Filesystem path for the table.
    pub path: String,
}

/// Stable address of the currently selected object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrowserAddress {
    /// A selected column definition.
    Column {
        /// Filesystem path of the current table.
        table_path: String,
        /// Column name.
        column: String,
    },
    /// A selected cell or nested cell value.
    Cell {
        /// Filesystem path of the current table.
        table_path: String,
        /// Selected row index.
        row: usize,
        /// Selected column name.
        column: String,
        /// Nested path within the cell value.
        value_path: Vec<ValuePathSegment>,
    },
    /// A selected table keyword or nested keyword value.
    TableKeyword {
        /// Filesystem path of the current table.
        table_path: String,
        /// Keyword path from the table keyword root.
        keyword_path: Vec<String>,
        /// Nested path within the keyword value.
        value_path: Vec<ValuePathSegment>,
    },
    /// A selected column keyword or nested column-keyword value.
    ColumnKeyword {
        /// Filesystem path of the current table.
        table_path: String,
        /// Owning column name.
        column: String,
        /// Keyword path from the column keyword root.
        keyword_path: Vec<String>,
        /// Nested path within the keyword value.
        value_path: Vec<ValuePathSegment>,
    },
    /// A selected linked subtable entry.
    Subtable {
        /// Filesystem path of the current table.
        table_path: String,
        /// Human-readable description of where the link was discovered.
        source: String,
        /// Resolved target table path.
        target_path: String,
    },
}

/// Nested path segment inside a selected value.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "segment", rename_all = "snake_case")]
pub enum ValuePathSegment {
    /// Drill into a record field.
    RecordField {
        /// Record field name.
        name: String,
    },
    /// Drill into a flattened array element.
    ArrayIndex {
        /// Zero-based flat element index in row-major order.
        flat_index: usize,
    },
}

/// Primitive element type carried by scalar and array values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BrowserPrimitiveType {
    /// Boolean scalar or array element.
    Bool,
    /// Unsigned 8-bit integer.
    UInt8,
    /// Unsigned 16-bit integer.
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// 32-bit floating-point value.
    Float32,
    /// 64-bit floating-point value.
    Float64,
    /// 32-bit complex value.
    Complex32,
    /// 64-bit complex value.
    Complex64,
    /// UTF-8 string value.
    String,
}

/// Broad kind of a browser value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BrowserValueKind {
    /// Missing / undefined value.
    Undefined,
    /// Scalar value.
    Scalar,
    /// Array value.
    Array,
    /// Record value.
    Record,
    /// Linked-table reference.
    TableRef,
}

/// Typed complex32 payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BrowserComplex32Value {
    /// Real component.
    pub re: f32,
    /// Imaginary component.
    pub im: f32,
}

/// Typed complex64 payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BrowserComplex64Value {
    /// Real component.
    pub re: f64,
    /// Imaginary component.
    pub im: f64,
}

/// Typed scalar payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum BrowserScalarValue {
    /// Boolean scalar.
    Bool(bool),
    /// Unsigned 8-bit integer scalar.
    UInt8(u8),
    /// Unsigned 16-bit integer scalar.
    UInt16(u16),
    /// Unsigned 32-bit integer scalar.
    UInt32(u32),
    /// Signed 16-bit integer scalar.
    Int16(i16),
    /// Signed 32-bit integer scalar.
    Int32(i32),
    /// Signed 64-bit integer scalar.
    Int64(i64),
    /// 32-bit floating-point scalar.
    Float32(f32),
    /// 64-bit floating-point scalar.
    Float64(f64),
    /// 32-bit complex scalar.
    Complex32(BrowserComplex32Value),
    /// 64-bit complex scalar.
    Complex64(BrowserComplex64Value),
    /// UTF-8 string scalar.
    String(String),
}

/// Array element exposed through the inspector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BrowserArrayElement {
    /// Flat row-major element index.
    pub flat_index: usize,
    /// Multidimensional index corresponding to `flat_index`.
    pub index: Vec<usize>,
    /// Typed scalar element payload.
    pub value: BrowserScalarValue,
    /// Whether this element is currently selected in the inspector.
    pub selected: bool,
}

/// Record-field summary exposed through the inspector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserRecordFieldSummary {
    /// Field name.
    pub name: String,
    /// Broad value kind.
    pub kind: BrowserValueKind,
    /// Compact summary shown in the inspector.
    pub summary: String,
    /// Whether `Activate` can drill into this field.
    pub expandable: bool,
    /// Whether `Activate` can open this field as a linked table.
    pub openable: bool,
    /// Whether this field is currently selected in the inspector.
    pub selected: bool,
}

/// Current trail from the base selected value to the inspector node.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BrowserInspectorTrailEntry {
    /// Path label for this trail segment.
    pub label: String,
    /// Compact value summary for this segment.
    pub summary: String,
}

/// Typed inspector node for the selected value.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrowserValueNode {
    /// Undefined / absent cell or keyword.
    Undefined,
    /// Scalar leaf value.
    Scalar {
        /// Typed scalar payload.
        value: BrowserScalarValue,
    },
    /// Array value, returned as a paged scalar-element view.
    Array {
        /// Primitive element type.
        primitive: BrowserPrimitiveType,
        /// Full array shape.
        shape: Vec<usize>,
        /// Total element count.
        total_elements: usize,
        /// First flat element index in this page.
        page_start: usize,
        /// Maximum elements requested for this page.
        page_size: usize,
        /// Page payload.
        elements: Vec<BrowserArrayElement>,
    },
    /// Record value, returned as a paged field summary view.
    Record {
        /// Total field count.
        total_fields: usize,
        /// First field index in this page.
        page_start: usize,
        /// Maximum fields requested for this page.
        page_size: usize,
        /// Page payload.
        fields: Vec<BrowserRecordFieldSummary>,
    },
    /// Linked table reference.
    TableRef {
        /// Original relative or absolute stored path.
        path: String,
        /// Resolved filesystem path.
        resolved_path: String,
        /// Whether the current backend can open this reference.
        openable: bool,
    },
}

/// Typed inspector payload for the selected value.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BrowserInspectorSnapshot {
    /// Inspector title shown in the UI.
    pub title: String,
    /// Trail from the base selection to the current node.
    pub trail: Vec<BrowserInspectorTrailEntry>,
    /// Current typed node.
    pub node: BrowserValueNode,
    /// Render-ready inspector lines for the current viewport.
    pub rendered_lines: Vec<String>,
}

/// Rendered browser snapshot returned after every command.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BrowserSnapshot {
    /// Backend capabilities for the current session.
    pub capabilities: BrowserCapabilities,
    /// Active top-level view.
    pub view: BrowserView,
    /// Active focus target.
    pub focus: BrowserFocus,
    /// Currently opened table path.
    pub table_path: String,
    /// Current breadcrumb stack.
    pub breadcrumb: Vec<BrowserBreadcrumbEntry>,
    /// Current viewport applied by the backend.
    pub viewport: BrowserViewport,
    /// User-facing status line.
    pub status_line: String,
    /// Render-ready content lines for the viewport.
    pub content_lines: Vec<String>,
    /// Stable address of the current selection.
    pub selected_address: Option<BrowserAddress>,
    /// Typed inspector payload for the current selection.
    pub inspector: Option<BrowserInspectorSnapshot>,
}

/// Render the request-envelope schema as pretty JSON.
pub fn request_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schema_for!(BrowserRequestEnvelope))
}

/// Render the response-envelope schema as pretty JSON.
pub fn response_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schema_for!(BrowserResponseEnvelope))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    #[test]
    fn request_schema_matches_committed_artifact() {
        assert_schema_matches(
            request_schema_json().expect("request schema"),
            include_str!("../schemas/request.schema.json"),
        );
    }

    #[test]
    fn response_schema_matches_committed_artifact() {
        assert_schema_matches(
            response_schema_json().expect("response schema"),
            include_str!("../schemas/response.schema.json"),
        );
    }

    #[test]
    fn request_envelopes_round_trip_all_command_variants() {
        let viewport = BrowserViewport::new(120, 32);
        let cases = vec![
            BrowserRequestEnvelope::new(BrowserCommand::OpenRoot {
                path: "/tmp/root.ms".to_string(),
                viewport,
            }),
            BrowserRequestEnvelope::new(BrowserCommand::Resize { viewport }),
            BrowserRequestEnvelope::new(BrowserCommand::CycleView {
                forward: true,
                viewport: Some(viewport),
            }),
            BrowserRequestEnvelope::new(BrowserCommand::MoveUp {
                steps: 2,
                viewport: None,
            }),
            BrowserRequestEnvelope::new(BrowserCommand::MoveDown {
                steps: 3,
                viewport: Some(viewport),
            }),
            BrowserRequestEnvelope::new(BrowserCommand::MoveLeft {
                steps: 1,
                viewport: None,
            }),
            BrowserRequestEnvelope::new(BrowserCommand::MoveRight {
                steps: 4,
                viewport: Some(viewport),
            }),
            BrowserRequestEnvelope::new(BrowserCommand::PageUp {
                pages: 2,
                viewport: None,
            }),
            BrowserRequestEnvelope::new(BrowserCommand::PageDown {
                pages: 5,
                viewport: Some(viewport),
            }),
            BrowserRequestEnvelope::new(BrowserCommand::Activate { viewport: None }),
            BrowserRequestEnvelope::new(BrowserCommand::Back {
                viewport: Some(viewport),
            }),
            BrowserRequestEnvelope::new(BrowserCommand::Escape { viewport: None }),
            BrowserRequestEnvelope::new(BrowserCommand::GetSnapshot {
                viewport: Some(viewport),
            }),
        ];

        for case in cases {
            assert_round_trip(case);
        }
    }

    #[test]
    fn response_envelopes_round_trip_snapshot_and_error_variants() {
        let viewport = BrowserViewport::new(120, 32);
        let breadcrumb = vec![
            BrowserBreadcrumbEntry {
                label: "root.ms".to_string(),
                path: "/tmp/root.ms".to_string(),
            },
            BrowserBreadcrumbEntry {
                label: "FIELD".to_string(),
                path: "/tmp/root.ms/FIELD".to_string(),
            },
        ];

        let cases = vec![
            BrowserResponseEnvelope::snapshot(BrowserSnapshot {
                capabilities: BrowserCapabilities { editable: false },
                view: BrowserView::Columns,
                focus: BrowserFocus::Main,
                table_path: "/tmp/root.ms".to_string(),
                breadcrumb: breadcrumb.clone(),
                viewport,
                status_line: "Columns ready".to_string(),
                content_lines: vec!["Columns".to_string(), "NAME scalar string".to_string()],
                selected_address: Some(BrowserAddress::Column {
                    table_path: "/tmp/root.ms".to_string(),
                    column: "NAME".to_string(),
                }),
                inspector: Some(BrowserInspectorSnapshot {
                    title: "Complex scalar".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "complex32".to_string(),
                    }],
                    node: BrowserValueNode::Scalar {
                        value: BrowserScalarValue::Complex32(BrowserComplex32Value {
                            re: 1.25,
                            im: -0.5,
                        }),
                    },
                    rendered_lines: vec![
                        "Inspector".to_string(),
                        "Type: complex32".to_string(),
                        "Value: 1.2500-0.5000i".to_string(),
                    ],
                }),
            }),
            BrowserResponseEnvelope::snapshot(BrowserSnapshot {
                capabilities: BrowserCapabilities { editable: false },
                view: BrowserView::Cells,
                focus: BrowserFocus::Inspector,
                table_path: "/tmp/root.ms".to_string(),
                breadcrumb: breadcrumb.clone(),
                viewport,
                status_line: "Array inspector".to_string(),
                content_lines: vec!["Cells".to_string()],
                selected_address: Some(BrowserAddress::Cell {
                    table_path: "/tmp/root.ms".to_string(),
                    row: 7,
                    column: "DATA".to_string(),
                    value_path: vec![ValuePathSegment::ArrayIndex { flat_index: 1 }],
                }),
                inspector: Some(BrowserInspectorSnapshot {
                    title: "Complex array".to_string(),
                    trail: vec![
                        BrowserInspectorTrailEntry {
                            label: "root".to_string(),
                            summary: "array[2x2]".to_string(),
                        },
                        BrowserInspectorTrailEntry {
                            label: "[0,1]".to_string(),
                            summary: "complex64".to_string(),
                        },
                    ],
                    node: BrowserValueNode::Array {
                        primitive: BrowserPrimitiveType::Complex64,
                        shape: vec![2, 2],
                        total_elements: 4,
                        page_start: 0,
                        page_size: 4,
                        elements: vec![
                            BrowserArrayElement {
                                flat_index: 0,
                                index: vec![0, 0],
                                value: BrowserScalarValue::Complex64(BrowserComplex64Value {
                                    re: 1.0,
                                    im: -1.0,
                                }),
                                selected: false,
                            },
                            BrowserArrayElement {
                                flat_index: 1,
                                index: vec![0, 1],
                                value: BrowserScalarValue::Complex64(BrowserComplex64Value {
                                    re: 2.0,
                                    im: -2.0,
                                }),
                                selected: true,
                            },
                        ],
                    },
                    rendered_lines: vec![
                        "Inspector".to_string(),
                        "Type: array".to_string(),
                        "[0,1] = 2.0000-2.0000i".to_string(),
                    ],
                }),
            }),
            BrowserResponseEnvelope::snapshot(BrowserSnapshot {
                capabilities: BrowserCapabilities { editable: false },
                view: BrowserView::Keywords,
                focus: BrowserFocus::Inspector,
                table_path: "/tmp/root.ms".to_string(),
                breadcrumb: breadcrumb.clone(),
                viewport,
                status_line: "Keyword record".to_string(),
                content_lines: vec!["Keywords".to_string()],
                selected_address: Some(BrowserAddress::ColumnKeyword {
                    table_path: "/tmp/root.ms".to_string(),
                    column: "UVW".to_string(),
                    keyword_path: vec!["MEASINFO".to_string()],
                    value_path: vec![ValuePathSegment::RecordField {
                        name: "Ref".to_string(),
                    }],
                }),
                inspector: Some(BrowserInspectorSnapshot {
                    title: "Record".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "record(2)".to_string(),
                    }],
                    node: BrowserValueNode::Record {
                        total_fields: 2,
                        page_start: 0,
                        page_size: 32,
                        fields: vec![
                            BrowserRecordFieldSummary {
                                name: "type".to_string(),
                                kind: BrowserValueKind::Scalar,
                                summary: "\"direction\"".to_string(),
                                expandable: false,
                                openable: false,
                                selected: false,
                            },
                            BrowserRecordFieldSummary {
                                name: "Ref".to_string(),
                                kind: BrowserValueKind::Scalar,
                                summary: "\"J2000\"".to_string(),
                                expandable: false,
                                openable: false,
                                selected: true,
                            },
                        ],
                    },
                    rendered_lines: vec![
                        "Inspector".to_string(),
                        "Type: record".to_string(),
                        "Ref = \"J2000\"".to_string(),
                    ],
                }),
            }),
            BrowserResponseEnvelope::snapshot(BrowserSnapshot {
                capabilities: BrowserCapabilities { editable: false },
                view: BrowserView::Keywords,
                focus: BrowserFocus::Inspector,
                table_path: "/tmp/root.ms".to_string(),
                breadcrumb: breadcrumb.clone(),
                viewport,
                status_line: "Keyword table ref".to_string(),
                content_lines: vec!["Keywords".to_string()],
                selected_address: Some(BrowserAddress::TableKeyword {
                    table_path: "/tmp/root.ms".to_string(),
                    keyword_path: vec!["FIELD".to_string(), "CHILD".to_string()],
                    value_path: vec![ValuePathSegment::RecordField {
                        name: "child".to_string(),
                    }],
                }),
                inspector: Some(BrowserInspectorSnapshot {
                    title: "Table ref".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "table_ref".to_string(),
                    }],
                    node: BrowserValueNode::TableRef {
                        path: "FIELD".to_string(),
                        resolved_path: "/tmp/root.ms/FIELD".to_string(),
                        openable: true,
                    },
                    rendered_lines: vec![
                        "Inspector".to_string(),
                        "Type: table_ref".to_string(),
                        "FIELD".to_string(),
                    ],
                }),
            }),
            BrowserResponseEnvelope::snapshot(BrowserSnapshot {
                capabilities: BrowserCapabilities { editable: false },
                view: BrowserView::Subtables,
                focus: BrowserFocus::Main,
                table_path: "/tmp/root.ms".to_string(),
                breadcrumb,
                viewport,
                status_line: "Undefined selection".to_string(),
                content_lines: vec!["Subtables".to_string()],
                selected_address: Some(BrowserAddress::Subtable {
                    table_path: "/tmp/root.ms".to_string(),
                    source: "keyword:FIELD".to_string(),
                    target_path: "/tmp/root.ms/FIELD".to_string(),
                }),
                inspector: Some(BrowserInspectorSnapshot {
                    title: "Undefined".to_string(),
                    trail: Vec::new(),
                    node: BrowserValueNode::Undefined,
                    rendered_lines: vec!["Type: undefined".to_string()],
                }),
            }),
            BrowserResponseEnvelope::error("unsupported_version", "expected v1"),
        ];

        for case in cases {
            assert_round_trip(case);
        }
    }

    fn assert_schema_matches(actual_json: String, expected_json: &str) {
        let actual = serde_json::from_str::<JsonValue>(&actual_json).expect("parse actual schema");
        let expected =
            serde_json::from_str::<JsonValue>(expected_json).expect("parse expected schema");
        assert_eq!(actual, expected);
    }

    fn assert_round_trip<T>(value: T)
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_string(&value).expect("serialize round-trip value");
        let parsed = serde_json::from_str::<T>(&json).expect("deserialize round-trip value");
        assert_eq!(parsed, value);
    }
}
