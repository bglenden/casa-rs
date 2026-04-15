// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wire protocol for the `imagebrowser` subprocess session.

use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, SessionSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Stable protocol name advertised by `imexplore --protocol-info`.
pub const IMAGEBROWSER_SESSION_PROTOCOL_NAME: &str = "casa_imagebrowser_session";

/// Current JSON protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Version/compatibility information for the imagebrowser session protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImageBrowserProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl ImageBrowserProtocolInfo {
    /// Build the current imagebrowser session protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: IMAGEBROWSER_SESSION_PROTOCOL_NAME.to_string(),
            protocol_version: PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Session,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Canonical JSON-schema bundle for the public imagebrowser session protocol.
#[derive(Debug, Clone, Serialize)]
pub struct ImageBrowserSessionSchemaBundle {
    /// Compatibility descriptor for the session protocol.
    pub protocol: ImageBrowserProtocolInfo,
    /// Canonical semantic session contract.
    pub semantic: SessionSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI and CLI consumers.
    pub projections: ProviderProjectionMetadata,
    /// JSON schema for [`ImageBrowserRequestEnvelope`].
    pub request_schema: schemars::schema::RootSchema,
    /// JSON schema for [`ImageBrowserResponseEnvelope`].
    pub response_schema: schemars::schema::RootSchema,
}

impl ImageBrowserSessionSchemaBundle {
    /// Build the current imagebrowser schema bundle.
    pub fn current(ui_schema: JsonValue) -> Self {
        let request_schema = schema_for!(ImageBrowserRequestEnvelope);
        let response_schema = schema_for!(ImageBrowserResponseEnvelope);
        Self {
            protocol: ImageBrowserProtocolInfo::current(),
            semantic: SessionSemanticContract {
                transport: "jsonl_stdio".to_string(),
                request_schema: request_schema.clone(),
                response_schema: response_schema.clone(),
            },
            components: merged_components([&request_schema, &response_schema]),
            annotations: derived_ui_schema_annotations(),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        ui_schema: Some("--ui-schema".to_string()),
                        json_schema: Some("--json-schema".to_string()),
                        protocol_info: Some("--protocol-info".to_string()),
                        json_run: None,
                        session: Some("--session".to_string()),
                    },
                }),
                ui_schema: Some(ui_schema),
                python: None,
            },
            request_schema,
            response_schema,
        }
    }

    /// Return the launcher/TUI compatibility view projected from the bundle.
    pub fn ui_schema_projection(&self) -> Result<JsonValue, String> {
        self.projections
            .ui_schema
            .clone()
            .ok_or_else(|| "missing ui_schema projection".to_string())
    }
}

/// Render viewport requested by the consumer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageBrowserViewport {
    pub width: u16,
    pub height: u16,
    #[serde(default)]
    pub inspector_height: u16,
    #[serde(default)]
    pub plane_pixel_width: u16,
    #[serde(default)]
    pub plane_pixel_height: u16,
}

impl ImageBrowserViewport {
    pub const fn new(width: u16, height: u16) -> Self {
        Self {
            width,
            height,
            inspector_height: 0,
            plane_pixel_width: 0,
            plane_pixel_height: 0,
        }
    }

    pub const fn with_inspector_height(width: u16, height: u16, inspector_height: u16) -> Self {
        Self {
            width,
            height,
            inspector_height,
            plane_pixel_width: 0,
            plane_pixel_height: 0,
        }
    }

    pub const fn with_plane_pixels(
        width: u16,
        height: u16,
        inspector_height: u16,
        plane_pixel_width: u16,
        plane_pixel_height: u16,
    ) -> Self {
        Self {
            width,
            height,
            inspector_height,
            plane_pixel_width,
            plane_pixel_height,
        }
    }
}

impl Default for ImageBrowserViewport {
    fn default() -> Self {
        Self::new(80, 24)
    }
}

/// Structured navigation metrics for the active right-hand pane.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageNavigationMetrics {
    pub selected_index: usize,
    pub total_items: usize,
    pub viewport_items: usize,
}

/// JSON Lines request envelope sent from `casars` to the image browser backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageBrowserRequestEnvelope {
    pub version: u32,
    pub command: ImageBrowserCommand,
}

impl ImageBrowserRequestEnvelope {
    pub fn new(command: ImageBrowserCommand) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            command,
        }
    }
}

/// Command sent to the image browser backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ImageBrowserCommand {
    OpenRoot {
        path: String,
        viewport: ImageBrowserViewport,
        #[serde(default)]
        parameters: Option<ImageBrowserParameters>,
    },
    Resize {
        viewport: ImageBrowserViewport,
    },
    CycleView {
        forward: bool,
    },
    SetFocus {
        focus: ImageBrowserFocus,
    },
    MoveCursor {
        dx: i32,
        dy: i32,
    },
    SetCursor {
        x: usize,
        y: usize,
    },
    StepNonDisplayAxis {
        axis: usize,
        delta: i32,
    },
    SetSelectedNonDisplayAxis {
        axis: usize,
    },
    SetViewWindow {
        parameters: ImageBrowserParameters,
    },
    SetPlaneContentMode {
        mode: ImagePlaneContentMode,
    },
    StartRegionShape,
    AppendRegionVertex {
        x: usize,
        y: usize,
    },
    CloseRegionShape,
    UndoRegionVertex,
    CancelRegionShape,
    ClearRegion,
    SaveRegionDefinition,
    LoadNextRegionDefinition,
    LoadRegionDefinition {
        name: String,
    },
    RenameRegionDefinition {
        name: String,
        new_name: String,
    },
    DeleteRegionDefinition {
        name: String,
    },
    SetDefaultMask {
        name: String,
    },
    UnsetDefaultMask,
    DeleteMask {
        name: String,
    },
    WriteRegionMask {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        set_default: bool,
    },
    PreviewOccurrence {
        request: ImageBrowserPreviewRequest,
    },
    GetSnapshot,
}

/// Content representation requested for the Plane view.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ImagePlaneContentMode {
    Raster,
    Spreadsheet,
}

/// JSON Lines response envelope sent from the backend to `casars`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageBrowserResponseEnvelope {
    pub version: u32,
    pub response: ImageBrowserResponse,
}

impl ImageBrowserResponseEnvelope {
    pub fn snapshot(snapshot: ImageBrowserSnapshot) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            response: ImageBrowserResponse::Snapshot(Box::new(snapshot)),
        }
    }

    pub fn preview(payload: ImageBrowserPreviewPayload) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            response: ImageBrowserResponse::Preview(Box::new(payload)),
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            response: ImageBrowserResponse::Error(ImageBrowserErrorPayload {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

/// Backend response payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "response", rename_all = "snake_case")]
pub enum ImageBrowserResponse {
    Snapshot(Box<ImageBrowserSnapshot>),
    Preview(Box<ImageBrowserPreviewPayload>),
    Error(ImageBrowserErrorPayload),
}

/// Stateless preview request for a specific movie/frame occurrence.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageBrowserPreviewRequest {
    pub viewport: ImageBrowserViewport,
    pub parameters: ImageBrowserParameters,
    pub plane_content_mode: ImagePlaneContentMode,
    #[serde(default)]
    pub non_display_indices: Vec<usize>,
    #[serde(default)]
    pub include_profile: bool,
}

/// Structured error payload returned by the browser backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageBrowserErrorPayload {
    pub code: String,
    pub message: String,
}

/// Top-level browser view.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ImageBrowserView {
    Plane,
    Spectrum,
    Metadata,
    Coordinates,
}

impl ImageBrowserView {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Plane => "Plane",
            Self::Spectrum => "Spectrum",
            Self::Metadata => "Metadata",
            Self::Coordinates => "Coordinates",
        }
    }
}

/// Active focus target within the image browser workspace.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ImageBrowserFocus {
    Inspector,
    Content,
}

/// Capability flags exposed by the image browser session.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
pub struct ImageBrowserCapabilities {
    pub renderable_plane: bool,
    pub world_coords_available: bool,
    pub pixel_only_mode: bool,
    pub non_display_axis_selectors: bool,
    pub mask_present: bool,
    pub complex_unsupported: bool,
}

/// Normalized image view parameters reflected in the left-side live form.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
pub struct ImageBrowserParameters {
    pub blc: String,
    pub trc: String,
    pub inc: String,
    #[serde(default)]
    pub stretch: String,
    #[serde(default)]
    pub autoscale: String,
    #[serde(default)]
    pub clip_low: String,
    #[serde(default)]
    pub clip_high: String,
}

/// Quantized grayscale plane raster returned by the backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImagePlaneRaster {
    pub width: usize,
    pub height: usize,
    pub pixels_u8: Vec<u8>,
    pub clip_min: f64,
    pub clip_max: f64,
    #[serde(default)]
    pub data_min: f64,
    #[serde(default)]
    pub data_max: f64,
    #[serde(default)]
    pub value_unit: String,
    #[serde(default)]
    pub histogram_bins: Vec<u32>,
    pub masked_or_non_finite_count: usize,
    pub no_finite_values: bool,
}

/// Named numeric axis value.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageBrowserAxisValue {
    pub name: String,
    pub unit: String,
    pub value: f64,
}

/// Display-axis metadata for the current rendered plane window.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageDisplayAxisState {
    pub axis: usize,
    pub name: String,
    pub unit: String,
    pub blc: usize,
    pub trc: usize,
    pub inc: usize,
    pub sampled_len: usize,
    /// World-coordinate increment per source pixel, in the native axis units.
    pub world_increment: Option<f64>,
}

/// Cursor state for the active rendered plane.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImagePlaneCursorState {
    pub sampled_x: usize,
    pub sampled_y: usize,
    pub pixel_x: usize,
    pub pixel_y: usize,
}

/// Cursor probe payload for the active image plane.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageBrowserProbe {
    pub pixel_indices: Vec<usize>,
    pub pixel_axes: Vec<ImageBrowserAxisValue>,
    pub value: f64,
    pub masked: bool,
    pub finite: bool,
    pub world_axes: Vec<ImageBrowserAxisValue>,
}

/// A single sample in a 1D spectrum/profile extraction.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageProfileSampleState {
    pub sample_index: usize,
    pub pixel_index: usize,
    pub value: f64,
    pub masked: bool,
    pub finite: bool,
    pub world_axis: Option<ImageBrowserAxisValue>,
}

/// Structured 1D spectrum/profile payload for linked client-side rendering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageProfilePayload {
    pub axis: usize,
    pub axis_name: String,
    pub axis_unit: String,
    pub value_unit: String,
    pub coord_type: String,
    pub selected_sample_index: usize,
    pub samples: Vec<ImageProfileSampleState>,
}

/// A sampled 2D point used for region overlays in the active plane view.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageRegionOverlayVertex {
    pub sampled_x: f64,
    pub sampled_y: f64,
}

/// A polygonal region shape projected into the active plane view.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageRegionOverlayShapeState {
    pub vertices: Vec<ImageRegionOverlayVertex>,
    pub closed: bool,
}

/// Statistics for the active region in the current plane/image units.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageRegionStatsState {
    pub pixel_count: usize,
    pub median: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub sigma: f64,
    pub rms: f64,
    pub sum: f64,
    pub value_unit: String,
}

/// Backend plane cache classification for the current snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageBackendPlaneCacheResult {
    Hit,
    PrefetchHit,
    Miss,
}

/// Backend timing payload attached to a snapshot when performance tracing is enabled.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageBackendTimingState {
    pub plane_cache_result: ImageBackendPlaneCacheResult,
    pub cached_plane_lookup_ns: u64,
    pub plane_extract_ns: u64,
    pub stat_collection_ns: u64,
    pub histogram_ns: u64,
    pub rasterize_ns: u64,
    pub total_plane_ns: u64,
    #[serde(default)]
    pub profile_cache_hits: u64,
    #[serde(default)]
    pub profile_cache_misses: u64,
    #[serde(default)]
    pub profile_extract_total_ns: u64,
}

/// Active region state for the current image browser session.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageRegionState {
    pub label: String,
    pub shape_count: usize,
    pub closed_shape_count: usize,
    pub editing: bool,
    pub active_shape_vertices: usize,
    #[serde(default)]
    pub overlay_shapes: Vec<ImageRegionOverlayShapeState>,
    #[serde(default)]
    pub stats: Option<ImageRegionStatsState>,
}

/// Non-display axis selector state for the current plane.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageNonDisplayAxisState {
    pub axis: usize,
    pub label: String,
    pub index: usize,
    pub length: usize,
    pub pixel: usize,
}

/// Full image browser render snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageBrowserSnapshot {
    pub status_line: String,
    pub active_view: ImageBrowserView,
    pub focus: ImageBrowserFocus,
    #[serde(default)]
    pub shape: Vec<usize>,
    #[serde(default)]
    pub parameters: ImageBrowserParameters,
    pub inspector_lines: Vec<String>,
    pub content_lines: Vec<String>,
    pub navigation: ImageNavigationMetrics,
    pub plane: Option<ImagePlaneRaster>,
    pub probe: Option<ImageBrowserProbe>,
    #[serde(default)]
    pub profile: Option<ImageProfilePayload>,
    #[serde(default)]
    pub display_axes: Vec<ImageDisplayAxisState>,
    pub plane_cursor: Option<ImagePlaneCursorState>,
    #[serde(default)]
    pub non_display_axes: Vec<ImageNonDisplayAxisState>,
    #[serde(default)]
    pub region: Option<ImageRegionState>,
    #[serde(default)]
    pub saved_region_names: Vec<String>,
    #[serde(default)]
    pub active_region_definition_name: Option<String>,
    #[serde(default)]
    pub mask_names: Vec<String>,
    #[serde(default)]
    pub default_mask_name: Option<String>,
    #[serde(default)]
    pub backend_timing: Option<ImageBackendTimingState>,
    pub capabilities: ImageBrowserCapabilities,
}

/// Stateless preview payload for a specific movie/frame occurrence.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageBrowserPreviewPayload {
    #[serde(default)]
    pub non_display_indices: Vec<usize>,
    pub snapshot: Box<ImageBrowserSnapshot>,
}

/// Returns the JSON schema for the request envelope.
pub fn request_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schema_for!(ImageBrowserRequestEnvelope))
}

/// Returns the JSON schema for the response envelope.
pub fn response_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schema_for!(ImageBrowserResponseEnvelope))
}

/// Returns the canonical schema bundle for the public imagebrowser session protocol.
pub fn schema_bundle_json(ui_schema: JsonValue) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&ImageBrowserSessionSchemaBundle::current(ui_schema))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let envelope =
            ImageBrowserRequestEnvelope::new(ImageBrowserCommand::MoveCursor { dx: 1, dy: -2 });
        let encoded = serde_json::to_string(&envelope).unwrap();
        let decoded: ImageBrowserRequestEnvelope = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn snapshot_roundtrip() {
        let snapshot = ImageBrowserSnapshot {
            status_line: "ready".into(),
            active_view: ImageBrowserView::Plane,
            focus: ImageBrowserFocus::Content,
            shape: vec![4, 4],
            parameters: ImageBrowserParameters {
                blc: "0,0".into(),
                trc: "3,3".into(),
                inc: "1,1".into(),
                stretch: "percentile99".into(),
                autoscale: "per_plane".into(),
                clip_low: String::new(),
                clip_high: String::new(),
            },
            inspector_lines: vec!["shape: [4, 4]".into()],
            content_lines: Vec::new(),
            navigation: ImageNavigationMetrics {
                selected_index: 0,
                total_items: 4,
                viewport_items: 4,
            },
            plane: Some(ImagePlaneRaster {
                width: 2,
                height: 2,
                pixels_u8: vec![0, 64, 128, 255],
                clip_min: 0.0,
                clip_max: 1.0,
                data_min: 0.0,
                data_max: 1.0,
                value_unit: "Jy/beam".into(),
                histogram_bins: vec![1, 1, 1, 1],
                masked_or_non_finite_count: 0,
                no_finite_values: false,
            }),
            probe: Some(ImageBrowserProbe {
                pixel_indices: vec![1, 1],
                pixel_axes: vec![ImageBrowserAxisValue {
                    name: "Axis0".into(),
                    unit: "px".into(),
                    value: 1.0,
                }],
                value: 42.0,
                masked: false,
                finite: true,
                world_axes: Vec::new(),
            }),
            profile: Some(ImageProfilePayload {
                axis: 2,
                axis_name: "Frequency".into(),
                axis_unit: "Hz".into(),
                value_unit: "Jy/beam".into(),
                coord_type: "Spectral".into(),
                selected_sample_index: 1,
                samples: vec![
                    ImageProfileSampleState {
                        sample_index: 0,
                        pixel_index: 0,
                        value: 1.0,
                        masked: false,
                        finite: true,
                        world_axis: Some(ImageBrowserAxisValue {
                            name: "Frequency".into(),
                            unit: "Hz".into(),
                            value: 1.42e9,
                        }),
                    },
                    ImageProfileSampleState {
                        sample_index: 1,
                        pixel_index: 1,
                        value: 2.0,
                        masked: false,
                        finite: true,
                        world_axis: Some(ImageBrowserAxisValue {
                            name: "Frequency".into(),
                            unit: "Hz".into(),
                            value: 1.421e9,
                        }),
                    },
                ],
            }),
            display_axes: vec![
                ImageDisplayAxisState {
                    axis: 0,
                    name: "Axis0".into(),
                    unit: "px".into(),
                    blc: 0,
                    trc: 3,
                    inc: 1,
                    sampled_len: 4,
                    world_increment: None,
                },
                ImageDisplayAxisState {
                    axis: 1,
                    name: "Axis1".into(),
                    unit: "px".into(),
                    blc: 0,
                    trc: 3,
                    inc: 1,
                    sampled_len: 4,
                    world_increment: None,
                },
            ],
            plane_cursor: Some(ImagePlaneCursorState {
                sampled_x: 1,
                sampled_y: 1,
                pixel_x: 1,
                pixel_y: 1,
            }),
            non_display_axes: Vec::new(),
            region: None,
            saved_region_names: vec!["Region 1".into()],
            active_region_definition_name: Some("Region 1".into()),
            mask_names: vec!["roi".into()],
            default_mask_name: Some("roi".into()),
            backend_timing: Some(ImageBackendTimingState {
                plane_cache_result: ImageBackendPlaneCacheResult::Miss,
                cached_plane_lookup_ns: 100,
                plane_extract_ns: 200,
                stat_collection_ns: 300,
                histogram_ns: 400,
                rasterize_ns: 500,
                total_plane_ns: 1_500,
                profile_cache_hits: 2,
                profile_cache_misses: 1,
                profile_extract_total_ns: 600,
            }),
            capabilities: ImageBrowserCapabilities {
                renderable_plane: true,
                world_coords_available: false,
                pixel_only_mode: true,
                non_display_axis_selectors: false,
                mask_present: false,
                complex_unsupported: false,
            },
        };
        let encoded =
            serde_json::to_string(&ImageBrowserResponseEnvelope::snapshot(snapshot.clone()))
                .unwrap();
        let decoded: ImageBrowserResponseEnvelope = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, ImageBrowserResponseEnvelope::snapshot(snapshot));
    }

    #[test]
    fn schemas_generate() {
        assert!(
            request_schema_json()
                .unwrap()
                .contains("ImageBrowserRequestEnvelope")
        );
        assert!(
            response_schema_json()
                .unwrap()
                .contains("ImageBrowserResponseEnvelope")
        );
    }

    #[test]
    fn schema_bundle_uses_current_protocol_and_transport() {
        let bundle = ImageBrowserSessionSchemaBundle::current(serde_json::json!({
            "schema_version": 1,
            "command_id": "imexplore",
        }));
        assert_eq!(
            bundle.protocol.protocol_name,
            IMAGEBROWSER_SESSION_PROTOCOL_NAME
        );
        assert_eq!(bundle.protocol.protocol_version, PROTOCOL_VERSION);
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Session);
        assert_eq!(bundle.semantic.transport, "jsonl_stdio");
        assert!(bundle.components.contains_key("ImageBrowserCommand"));
        assert!(bundle.components.contains_key("ImageBrowserResponse"));
    }
}
