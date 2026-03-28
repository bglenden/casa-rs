// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wire protocol for the `imagebrowser` subprocess session.

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

/// Current JSON protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Render viewport requested by the consumer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageBrowserViewport {
    pub width: u16,
    pub height: u16,
    #[serde(default)]
    pub inspector_height: u16,
}

impl ImageBrowserViewport {
    pub const fn new(width: u16, height: u16) -> Self {
        Self {
            width,
            height,
            inspector_height: 0,
        }
    }

    pub const fn with_inspector_height(width: u16, height: u16, inspector_height: u16) -> Self {
        Self {
            width,
            height,
            inspector_height,
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
    StepHiddenAxis {
        delta: i32,
    },
    GetSnapshot,
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
    Error(ImageBrowserErrorPayload),
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
    Metadata,
    Coordinates,
}

impl ImageBrowserView {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Plane => "Plane",
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
    pub single_hidden_axis_stepper: bool,
    pub mask_present: bool,
    pub complex_unsupported: bool,
}

/// Quantized grayscale plane raster returned by the backend.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImagePlaneRaster {
    pub width: usize,
    pub height: usize,
    pub pixels_u8: Vec<u8>,
    pub clip_min: f64,
    pub clip_max: f64,
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

/// Hidden-axis selector state for 3D cubes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImageHiddenAxisState {
    pub label: String,
    pub index: usize,
    pub length: usize,
}

/// Full image browser render snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageBrowserSnapshot {
    pub status_line: String,
    pub active_view: ImageBrowserView,
    pub focus: ImageBrowserFocus,
    pub inspector_lines: Vec<String>,
    pub content_lines: Vec<String>,
    pub navigation: ImageNavigationMetrics,
    pub plane: Option<ImagePlaneRaster>,
    pub probe: Option<ImageBrowserProbe>,
    pub hidden_axis: Option<ImageHiddenAxisState>,
    pub capabilities: ImageBrowserCapabilities,
}

/// Returns the JSON schema for the request envelope.
pub fn request_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schema_for!(ImageBrowserRequestEnvelope))
}

/// Returns the JSON schema for the response envelope.
pub fn response_schema_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schema_for!(ImageBrowserResponseEnvelope))
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
            hidden_axis: None,
            capabilities: ImageBrowserCapabilities {
                renderable_plane: true,
                world_coords_available: false,
                pixel_only_mode: true,
                single_hidden_axis_stepper: false,
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
}
