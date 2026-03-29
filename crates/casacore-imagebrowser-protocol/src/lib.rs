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
}
