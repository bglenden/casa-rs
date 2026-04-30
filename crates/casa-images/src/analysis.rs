// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped image-analysis operations matching CASA image task semantics.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem, CoordinateType, FitsHeader, FitsValue,
    fits::{from_fits_header, to_fits_header},
};
use casa_lattices::{LatticeStatistics, Statistic, StatsElement, TiledShape};
use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use casa_types::{ArrayD, ScalarValue, Value};
use fitsio::{
    FitsFile,
    hdu::HduInfo,
    images::{ImageDescription, ImageType as FitsImageType},
};
use ndarray::{Axis, IxDyn, ShapeBuilder, Zip};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageInterface, ImagePixel,
    ImagePixelType, ImageType, PagedImage, TempImage,
};

/// Stable protocol name advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_NAME: &str = "casa_image_analysis_task";
/// Stable protocol version advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION: u32 = 1;

const SPEED_OF_LIGHT_KM_S: f64 = 299_792.458;

/// Version/compatibility information for the JSON image-analysis task protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImageAnalysisProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl ImageAnalysisProtocolInfo {
    /// Build the current image-analysis protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: IMAGE_ANALYSIS_TASK_PROTOCOL_NAME.to_string(),
            protocol_version: IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Task,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// JSON-schema bundle for the public image-analysis task protocol.
#[derive(Debug, Clone, Serialize)]
pub struct ImageAnalysisTaskSchemaBundle {
    /// Compatibility descriptor for the request/result schemas.
    pub protocol: ImageAnalysisProtocolInfo,
    /// Canonical semantic task contract.
    pub semantic: TaskSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI, CLI, and Python consumers.
    pub projections: ProviderProjectionMetadata,
    /// JSON schema for [`ImageAnalysisTaskRequest`].
    pub request_schema: RootSchema,
    /// JSON schema for [`ImageAnalysisTaskResult`].
    pub result_schema: RootSchema,
}

impl ImageAnalysisTaskSchemaBundle {
    /// Build the current request/result schema bundle.
    pub fn current(_binary: &str) -> Self {
        let request_schema = schema_for!(ImageAnalysisTaskRequest);
        let result_schema = schema_for!(ImageAnalysisTaskResult);
        Self {
            protocol: ImageAnalysisProtocolInfo::current(),
            semantic: TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: vec![
                    TaskOperationDescriptor {
                        name: "imhead".to_string(),
                        request_kind: "imhead".to_string(),
                        result_kind: Some("imhead".to_string()),
                    },
                    TaskOperationDescriptor {
                        name: "imstat".to_string(),
                        request_kind: "imstat".to_string(),
                        result_kind: Some("imstat".to_string()),
                    },
                    TaskOperationDescriptor {
                        name: "immoments".to_string(),
                        request_kind: "immoments".to_string(),
                        result_kind: Some("immoments".to_string()),
                    },
                    TaskOperationDescriptor {
                        name: "impv".to_string(),
                        request_kind: "impv".to_string(),
                        result_kind: Some("impv".to_string()),
                    },
                    TaskOperationDescriptor {
                        name: "exportfits".to_string(),
                        request_kind: "exportfits".to_string(),
                        result_kind: Some("exportfits".to_string()),
                    },
                    TaskOperationDescriptor {
                        name: "importfits".to_string(),
                        request_kind: "importfits".to_string(),
                        result_kind: Some("importfits".to_string()),
                    },
                ],
            },
            components: merged_components([&request_schema, &result_schema]),
            annotations: derived_ui_schema_annotations(),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        ui_schema: None,
                        json_schema: Some("--json-schema".to_string()),
                        protocol_info: Some("--protocol-info".to_string()),
                        json_run: Some("--json-run <SOURCE>".to_string()),
                        session: None,
                    },
                }),
                ui_schema: None,
                python: Some(serde_json::json!({
                    "module": "casars.tasks.image_analysis",
                    "functions": ["imhead", "imstat", "immoments", "impv", "exportfits", "importfits"],
                })),
            },
            request_schema,
            result_schema,
        }
    }
}

/// Return a launcher-compatible UI schema JSON document for image-analysis binaries.
pub fn image_analysis_ui_schema_json(binary: &str) -> Result<String, ImageError> {
    let (command_id, display_name, summary, usage, arguments) = match binary {
        "immoments" => (
            "immoments",
            "Image Moments",
            "Create CASA-style image moment maps",
            "immoments <imagename> --outfile <path> [--moments 0|1] [--chans 4~12] [--includepix min,max] [--overwrite]",
            serde_json::json!([
                arg(UiArgument {
                    id: "imagename",
                    label: "Image",
                    order: 0,
                    parser: positional("imagename"),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Input CASA image path",
                    group: "Input",
                }),
                arg(UiArgument {
                    id: "outfile",
                    label: "Output",
                    order: 1,
                    parser: option(["--outfile"], "path", []),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Output CASA image path",
                    group: "Output",
                }),
                arg(UiArgument {
                    id: "moments",
                    label: "Moment",
                    order: 2,
                    parser: option(["--moments"], "0|1", ["0", "1"]),
                    value_kind: "choice",
                    required: false,
                    default: serde_json::json!("0"),
                    help: "Moment number",
                    group: "Moment",
                }),
                arg(UiArgument {
                    id: "chans",
                    label: "Channels",
                    order: 3,
                    parser: option(["--chans"], "range", []),
                    value_kind: "string",
                    required: false,
                    default: JsonValue::Null,
                    help: "CASA channel range, for example 4~12",
                    group: "Selection",
                }),
                arg(UiArgument {
                    id: "includepix",
                    label: "Include Pixels",
                    order: 4,
                    parser: option(["--includepix"], "min,max", []),
                    value_kind: "string",
                    required: false,
                    default: JsonValue::Null,
                    help: "Inclusive pixel range",
                    group: "Selection",
                }),
                arg(UiArgument {
                    id: "overwrite",
                    label: "Overwrite",
                    order: 5,
                    parser: toggle(["--overwrite"], []),
                    value_kind: "bool",
                    required: false,
                    default: serde_json::json!("false"),
                    help: "Replace existing output image",
                    group: "Output",
                })
            ]),
        ),
        "impv" => (
            "impv",
            "Position-Velocity Slice",
            "Extract a CASA-style position-velocity image",
            "impv <imagename> --outfile <path> --start x,y --end x,y [--width pixels] [--chans 4~12] [--overwrite]",
            serde_json::json!([
                arg(UiArgument {
                    id: "imagename",
                    label: "Image",
                    order: 0,
                    parser: positional("imagename"),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Input CASA image path",
                    group: "Input",
                }),
                arg(UiArgument {
                    id: "outfile",
                    label: "Output",
                    order: 1,
                    parser: option(["--outfile"], "path", []),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Output CASA image path",
                    group: "Output",
                }),
                arg(UiArgument {
                    id: "start",
                    label: "Start",
                    order: 2,
                    parser: option(["--start"], "x,y", []),
                    value_kind: "string",
                    required: true,
                    default: JsonValue::Null,
                    help: "Start pixel coordinate",
                    group: "Slice",
                }),
                arg(UiArgument {
                    id: "end",
                    label: "End",
                    order: 3,
                    parser: option(["--end"], "x,y", []),
                    value_kind: "string",
                    required: true,
                    default: JsonValue::Null,
                    help: "End pixel coordinate",
                    group: "Slice",
                }),
                arg(UiArgument {
                    id: "width",
                    label: "Width",
                    order: 4,
                    parser: option(["--width"], "pixels", []),
                    value_kind: "number",
                    required: false,
                    default: serde_json::json!("1"),
                    help: "Averaging width in pixels",
                    group: "Slice",
                }),
                arg(UiArgument {
                    id: "chans",
                    label: "Channels",
                    order: 5,
                    parser: option(["--chans"], "range", []),
                    value_kind: "string",
                    required: false,
                    default: JsonValue::Null,
                    help: "CASA channel range, for example 4~12",
                    group: "Selection",
                }),
                arg(UiArgument {
                    id: "overwrite",
                    label: "Overwrite",
                    order: 6,
                    parser: toggle(["--overwrite"], []),
                    value_kind: "bool",
                    required: false,
                    default: serde_json::json!("false"),
                    help: "Replace existing output image",
                    group: "Output",
                })
            ]),
        ),
        "exportfits" => (
            "exportfits",
            "Export FITS",
            "Export CASA images to FITS",
            "exportfits <imagename> <fitsimage> [--velocity] [--overwrite]",
            serde_json::json!([
                arg(UiArgument {
                    id: "imagename",
                    label: "Image",
                    order: 0,
                    parser: positional("imagename"),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Input CASA image path",
                    group: "Input",
                }),
                arg(UiArgument {
                    id: "fitsimage",
                    label: "FITS",
                    order: 1,
                    parser: positional("fitsimage"),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Output FITS path",
                    group: "Output",
                }),
                arg(UiArgument {
                    id: "velocity",
                    label: "Velocity Axis",
                    order: 2,
                    parser: toggle(["--velocity"], []),
                    value_kind: "bool",
                    required: false,
                    default: serde_json::json!("false"),
                    help: "Write spectral axis as velocity where possible",
                    group: "FITS",
                }),
                arg(UiArgument {
                    id: "overwrite",
                    label: "Overwrite",
                    order: 3,
                    parser: toggle(["--overwrite"], []),
                    value_kind: "bool",
                    required: false,
                    default: serde_json::json!("false"),
                    help: "Replace existing FITS output",
                    group: "Output",
                })
            ]),
        ),
        "importfits" => (
            "importfits",
            "Import FITS",
            "Import a FITS primary image into a CASA image",
            "importfits <fitsimage> <imagename> [--overwrite]",
            serde_json::json!([
                arg(UiArgument {
                    id: "fitsimage",
                    label: "FITS",
                    order: 0,
                    parser: positional("fitsimage"),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Input FITS image path",
                    group: "Input",
                }),
                arg(UiArgument {
                    id: "imagename",
                    label: "Image",
                    order: 1,
                    parser: positional("imagename"),
                    value_kind: "path",
                    required: true,
                    default: JsonValue::Null,
                    help: "Output CASA image path",
                    group: "Output",
                }),
                arg(UiArgument {
                    id: "overwrite",
                    label: "Overwrite",
                    order: 2,
                    parser: toggle(["--overwrite"], []),
                    value_kind: "bool",
                    required: false,
                    default: serde_json::json!("false"),
                    help: "Replace existing output image",
                    group: "Output",
                })
            ]),
        ),
        _ => (
            "image-analysis",
            "Image Analysis",
            "CASA-style image analysis",
            "image-analysis",
            serde_json::json!([]),
        ),
    };
    let value = serde_json::json!({
        "schema_version": 1,
        "command_id": command_id,
        "invocation_name": binary,
        "display_name": display_name,
        "category": "Images",
        "summary": summary,
        "usage": usage,
        "arguments": arguments,
        "managed_output": null
    });
    serde_json::to_string_pretty(&value)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
}

struct UiArgument<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    parser: JsonValue,
    value_kind: &'a str,
    required: bool,
    default: JsonValue,
    help: &'a str,
    group: &'a str,
}

fn arg(argument: UiArgument<'_>) -> JsonValue {
    serde_json::json!({
        "id": argument.id,
        "label": argument.label,
        "order": argument.order,
        "parser": argument.parser,
        "value_kind": argument.value_kind,
        "required": argument.required,
        "default": argument.default,
        "help": argument.help,
        "group": argument.group,
        "advanced": false,
        "hidden_in_tui": false
    })
}

fn positional(metavar: &str) -> JsonValue {
    serde_json::json!({"kind": "positional", "metavar": metavar})
}

fn option(
    flags: impl IntoIterator<Item = &'static str>,
    metavar: &str,
    choices: impl IntoIterator<Item = &'static str>,
) -> JsonValue {
    serde_json::json!({
        "kind": "option",
        "flags": flags.into_iter().collect::<Vec<_>>(),
        "metavar": metavar,
        "choices": choices.into_iter().collect::<Vec<_>>()
    })
}

fn toggle(
    true_flags: impl IntoIterator<Item = &'static str>,
    false_flags: impl IntoIterator<Item = &'static str>,
) -> JsonValue {
    serde_json::json!({
        "kind": "toggle",
        "true_flags": true_flags.into_iter().collect::<Vec<_>>(),
        "false_flags": false_flags.into_iter().collect::<Vec<_>>()
    })
}

/// Top-level image-analysis JSON task request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ImageAnalysisTaskRequest {
    /// CASA `imhead(..., mode="summary")` style metadata summary.
    Imhead(ImheadRequest),
    /// CASA `imstat` style image statistics.
    Imstat(ImstatRequest),
    /// CASA `immoments` style moment map generation.
    Immoments(ImmomentsRequest),
    /// CASA `impv` style position-velocity extraction.
    Impv(ImpvRequest),
    /// CASA `exportfits` style FITS image export.
    Exportfits(ExportFitsRequest),
    /// CASA `importfits` style FITS image import.
    Importfits(ImportFitsRequest),
}

/// Top-level image-analysis JSON task result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ImageAnalysisTaskResult {
    /// Result for [`ImageAnalysisTaskRequest::Imhead`].
    Imhead(ImageHeaderSummary),
    /// Result for [`ImageAnalysisTaskRequest::Imstat`].
    Imstat(ImageStatisticsSummary),
    /// Result for [`ImageAnalysisTaskRequest::Immoments`].
    Immoments(MomentMapSummary),
    /// Result for [`ImageAnalysisTaskRequest::Impv`].
    Impv(PvImageSummary),
    /// Result for [`ImageAnalysisTaskRequest::Exportfits`].
    Exportfits(FitsExportSummary),
    /// Result for [`ImageAnalysisTaskRequest::Importfits`].
    Importfits(FitsImportSummary),
}

/// CASA `imhead` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImheadRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
}

/// CASA `imstat` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImstatRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// CASA inclusive pixel box, formatted as `x0,y0,x1,y1`.
    #[serde(default)]
    pub box_pixels: Option<String>,
    /// CASA channel expression, supporting tutorial forms like `0~4`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Inclusive pixel-value range to include.
    #[serde(default)]
    pub includepix: Option<[f64; 2]>,
}

/// CASA `immoments` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImmomentsRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// CASA moment number. Tutorial-supported values are `0` and `1`.
    pub moments: i32,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Inclusive pixel-value range to include.
    #[serde(default)]
    pub includepix: Option<[f64; 2]>,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// CASA `impv` request for a tutorial-scoped position-velocity image.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImpvRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// CASA mode. This implementation supports `coords`.
    #[serde(default = "default_impv_mode")]
    pub mode: String,
    /// Start pixel coordinate as `x,y`.
    pub start: String,
    /// End pixel coordinate as `x,y`.
    pub end: String,
    /// Slice width in pixels, averaged perpendicular to the path.
    #[serde(default = "default_impv_width")]
    pub width: usize,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

fn default_impv_mode() -> String {
    "coords".to_string()
}

fn default_impv_width() -> usize {
    1
}

/// Summary returned after writing a PV image.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PvImageSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Output CASA image path.
    pub outfile: String,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Number of samples along the PV path.
    pub path_pixels: usize,
    /// Width in pixels averaged perpendicular to the path.
    pub width: usize,
    /// Pixel units copied from the input image.
    pub units: String,
}

/// CASA `exportfits` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExportFitsRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output FITS image path.
    pub fitsimage: PathBuf,
    /// Emit the spectral axis as velocity where possible.
    #[serde(default)]
    pub velocity: bool,
    /// Replace an existing FITS file.
    #[serde(default)]
    pub overwrite: bool,
}

/// CASA `importfits` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImportFitsRequest {
    /// Input FITS image path.
    pub fitsimage: PathBuf,
    /// Output CASA image path.
    pub imagename: PathBuf,
    /// Replace an existing CASA image.
    #[serde(default)]
    pub overwrite: bool,
}

/// Basic axis metadata used by header summaries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageAxisSummary {
    /// Zero-based image pixel axis.
    pub axis: usize,
    /// Coordinate type for this axis.
    pub coordinate_type: String,
    /// Human-readable axis name.
    pub name: String,
    /// Axis length in pixels.
    pub shape: usize,
    /// World-coordinate value at the reference pixel.
    pub reference_value: f64,
    /// Reference pixel in CASA zero-based coordinates.
    pub reference_pixel: f64,
    /// Axis increment in world units per pixel.
    pub increment: f64,
    /// Axis units.
    pub unit: String,
}

/// CASA `imhead`-style image header summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageHeaderSummary {
    /// Input image path.
    pub imagename: String,
    /// Runtime pixel type detected from the persistent image.
    pub pixel_type: String,
    /// Image shape.
    pub shape: Vec<usize>,
    /// Brightness units.
    pub units: String,
    /// Astronomical object name.
    pub object_name: String,
    /// Image quantity classification.
    pub image_type: String,
    /// Stored default pixel mask name.
    pub default_mask: Option<String>,
    /// Stored pixel masks.
    pub masks: Vec<String>,
    /// Stored regions.
    pub regions: Vec<String>,
    /// Restoring beam if a single global beam is present.
    pub restoring_beam: Option<RestoringBeamSummary>,
    /// Axis metadata.
    pub axes: Vec<ImageAxisSummary>,
}

/// Restoring beam summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RestoringBeamSummary {
    /// Major axis in arcseconds.
    pub major_arcsec: f64,
    /// Minor axis in arcseconds.
    pub minor_arcsec: f64,
    /// Position angle in degrees.
    pub position_angle_deg: f64,
}

/// CASA `imstat`-style global image statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageStatisticsSummary {
    /// Input image path.
    pub imagename: String,
    /// Applied zero-based origin.
    pub blc: Vec<usize>,
    /// Applied zero-based inclusive top-right corner.
    pub trc: Vec<usize>,
    /// Number of selected pixels.
    pub npts: f64,
    /// Minimum selected value.
    pub min: f64,
    /// Maximum selected value.
    pub max: f64,
    /// Sum of selected values.
    pub sum: f64,
    /// Sum of selected value squares.
    pub sumsq: f64,
    /// Mean selected value.
    pub mean: f64,
    /// RMS selected value.
    pub rms: f64,
    /// Population sigma selected value.
    pub sigma: f64,
    /// Median selected value.
    pub median: f64,
    /// Minimum position relative to the original image.
    pub minpos: Option<Vec<usize>>,
    /// Maximum position relative to the original image.
    pub maxpos: Option<Vec<usize>>,
    /// CASA-style flux density when beam and pixel area are available.
    pub flux: Option<f64>,
    /// Image units.
    pub units: String,
}

/// CASA `immoments` output summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MomentMapSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Output CASA image path.
    pub outfile: String,
    /// Moment number generated.
    pub moment: i32,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Output image units.
    pub units: String,
    /// Number of valid collapsed profiles.
    pub valid_profiles: usize,
}

/// CASA `exportfits` output summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FitsExportSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Output FITS image path.
    pub fitsimage: String,
    /// Exported FITS image shape.
    pub shape: Vec<usize>,
    /// Whether velocity-axis export was requested.
    pub velocity: bool,
}

/// CASA `importfits` output summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FitsImportSummary {
    /// Input FITS image path.
    pub fitsimage: String,
    /// Output CASA image path.
    pub imagename: String,
    /// Imported CASA image shape.
    pub shape: Vec<usize>,
    /// Imported pixel type.
    pub pixel_type: String,
    /// Imported brightness unit.
    pub units: String,
    /// Imported object name, if any.
    pub object_name: String,
    /// Whether a single restoring beam was imported.
    pub has_restoring_beam: bool,
}

/// Run an image-analysis JSON task request.
pub fn run_image_analysis_task(
    request: ImageAnalysisTaskRequest,
) -> Result<ImageAnalysisTaskResult, ImageError> {
    match request {
        ImageAnalysisTaskRequest::Imhead(request) => {
            Ok(ImageAnalysisTaskResult::Imhead(imhead(&request.imagename)?))
        }
        ImageAnalysisTaskRequest::Imstat(request) => Ok(ImageAnalysisTaskResult::Imstat(imstat(
            &request.imagename,
            request.box_pixels.as_deref(),
            request.chans.as_deref(),
            request.includepix,
        )?)),
        ImageAnalysisTaskRequest::Immoments(request) => {
            Ok(ImageAnalysisTaskResult::Immoments(immoments(&request)?))
        }
        ImageAnalysisTaskRequest::Impv(request) => {
            Ok(ImageAnalysisTaskResult::Impv(impv(&request)?))
        }
        ImageAnalysisTaskRequest::Exportfits(request) => {
            Ok(ImageAnalysisTaskResult::Exportfits(export_fits(
                &request.imagename,
                &request.fitsimage,
                request.velocity,
                request.overwrite,
            )?))
        }
        ImageAnalysisTaskRequest::Importfits(request) => Ok(ImageAnalysisTaskResult::Importfits(
            import_fits(&request.fitsimage, &request.imagename, request.overwrite)?,
        )),
    }
}

/// Read a JSON task request from a file path or `-` for stdin.
pub fn read_image_analysis_request_source(
    source: impl AsRef<Path>,
) -> Result<ImageAnalysisTaskRequest, ImageError> {
    let source = source.as_ref();
    let text = if source == Path::new("-") {
        let mut text = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        text
    } else {
        fs::read_to_string(source).map_err(|error| ImageError::Io(error.to_string()))?
    };
    serde_json::from_str(&text).map_err(|error| ImageError::InvalidMetadata(error.to_string()))
}

/// Return a CASA `imhead(..., mode="summary")`-style metadata summary.
pub fn imhead(path: impl AsRef<Path>) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    let image = AnyPagedImage::open(path)?;
    match &image {
        AnyPagedImage::Float32(image) => imhead_typed(path, image, image.pixel_type_label()),
        AnyPagedImage::Float64(image) => imhead_typed(path, image, image.pixel_type_label()),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            let mut summary = ImageHeaderSummary {
                imagename: path.display().to_string(),
                pixel_type: format!("{:?}", image.pixel_type()),
                shape: image.shape().to_vec(),
                units: String::new(),
                object_name: String::new(),
                image_type: ImageType::Undefined.to_string(),
                default_mask: image.default_mask_name(),
                masks: image.mask_names(),
                regions: image.region_names(),
                restoring_beam: None,
                axes: Vec::new(),
            };
            match &image {
                AnyPagedImage::Complex32(image) => {
                    summary.units = image.units().to_string();
                    summary.axes = axis_summaries(image);
                }
                AnyPagedImage::Complex64(image) => {
                    summary.units = image.units().to_string();
                    summary.axes = axis_summaries(image);
                }
                _ => {}
            }
            Ok(summary)
        }
    }
}

/// Return CASA `imstat`-style statistics for tutorial selections.
pub fn imstat(
    path: impl AsRef<Path>,
    box_pixels: Option<&str>,
    chans: Option<&str>,
    includepix: Option<[f64; 2]>,
) -> Result<ImageStatisticsSummary, ImageError> {
    let path = path.as_ref();
    let image = AnyPagedImage::open(path)?;
    match &image {
        AnyPagedImage::Float32(image) => imstat_typed(path, image, box_pixels, chans, includepix),
        AnyPagedImage::Float64(image) => imstat_typed(path, image, box_pixels, chans, includepix),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("imstat currently supports real-valued images".to_string()),
        ),
    }
}

/// Generate a CASA `immoments`-style moment map for tutorial moment 0/1 use.
pub fn immoments(request: &ImmomentsRequest) -> Result<MomentMapSummary, ImageError> {
    let image = AnyPagedImage::open(&request.imagename)?;
    match &image {
        AnyPagedImage::Float32(image) => immoments_typed(image, request),
        AnyPagedImage::Float64(image) => immoments_typed(image, request),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            Err(ImageError::InvalidMetadata(
                "immoments currently supports real-valued images".to_string(),
            ))
        }
    }
}

/// Extract a CASA `impv`-style position-velocity image for pixel-coordinate slices.
pub fn impv(request: &ImpvRequest) -> Result<PvImageSummary, ImageError> {
    let image = AnyPagedImage::open(&request.imagename)?;
    match &image {
        AnyPagedImage::Float32(image) => impv_typed(image, request),
        AnyPagedImage::Float64(image) => impv_typed(image, request),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("impv currently supports real-valued images".to_string()),
        ),
    }
}

/// Export a CASA image as a primary-HDU FITS image.
pub fn export_fits(
    imagename: impl AsRef<Path>,
    fitsimage: impl AsRef<Path>,
    velocity: bool,
    overwrite: bool,
) -> Result<FitsExportSummary, ImageError> {
    let imagename = imagename.as_ref();
    let fitsimage = fitsimage.as_ref();
    let image = AnyPagedImage::open(imagename)?;
    if fitsimage.exists() {
        if overwrite {
            fs::remove_file(fitsimage).map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "FITS output already exists: {}",
                fitsimage.display()
            )));
        }
    }
    match &image {
        AnyPagedImage::Float32(image) => export_fits_typed(imagename, fitsimage, image, velocity),
        AnyPagedImage::Float64(image) => export_fits_typed(imagename, fitsimage, image, velocity),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            Err(ImageError::InvalidMetadata(
                "exportfits currently supports real-valued images".to_string(),
            ))
        }
    }
}

/// Import a primary-HDU FITS image as a CASA image.
pub fn import_fits(
    fitsimage: impl AsRef<Path>,
    imagename: impl AsRef<Path>,
    overwrite: bool,
) -> Result<FitsImportSummary, ImageError> {
    let fitsimage = fitsimage.as_ref();
    let imagename = imagename.as_ref();
    if imagename.exists() {
        if overwrite {
            fs::remove_dir_all(imagename).map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "CASA image output already exists: {}",
                imagename.display()
            )));
        }
    }

    let mut fits = FitsFile::open(fitsimage).map_err(|error| ImageError::Io(error.to_string()))?;
    let hdu = fits
        .primary_hdu()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let image_type = match &hdu.info {
        HduInfo::ImageInfo { image_type, .. } => *image_type,
        HduInfo::TableInfo { .. } | HduInfo::AnyInfo => {
            return Err(ImageError::InvalidMetadata(
                "primary HDU is not a FITS image".to_string(),
            ));
        }
    };
    let shape = read_fits_axis_shape(&mut fits, &hdu)?;
    let header = read_primary_fits_header(&mut fits, &hdu, &shape)?;
    let coordinates = from_fits_header(&header, &shape)?;
    match image_type {
        FitsImageType::Double => {
            let data = hdu
                .read_image::<Vec<f64>>(&mut fits)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            import_fits_typed::<f64>(fitsimage, imagename, shape, coordinates, header, data)
        }
        FitsImageType::Float
        | FitsImageType::UnsignedByte
        | FitsImageType::Byte
        | FitsImageType::Short
        | FitsImageType::UnsignedShort
        | FitsImageType::Long
        | FitsImageType::UnsignedLong
        | FitsImageType::LongLong => {
            let data = hdu
                .read_image::<Vec<f32>>(&mut fits)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            import_fits_typed::<f32>(fitsimage, imagename, shape, coordinates, header, data)
        }
    }
}

fn import_fits_typed<T>(
    fitsimage: &Path,
    imagename: &Path,
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    header: FitsHeader,
    data: Vec<T>,
) -> Result<FitsImportSummary, ImageError>
where
    T: ImagePixel,
{
    let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), data).map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid FITS image shape: {error}"))
    })?;
    let tile_shape = TiledShape::new(shape.clone()).tile_shape();
    let mut image =
        PagedImage::<T>::create_with_tile_shape(shape.clone(), tile_shape, coordinates, imagename)?;
    image.put_slice(&array, &vec![0; shape.len()])?;
    if let Some(units) = header.get_string("BUNIT") {
        image.set_units(units)?;
    }
    let image_info = image_info_from_fits_header(&header);
    image.set_image_info(&image_info)?;
    image.add_history(format!("Imported from FITS {}", fitsimage.display()))?;
    image.save()?;

    Ok(FitsImportSummary {
        fitsimage: fitsimage.display().to_string(),
        imagename: imagename.display().to_string(),
        shape,
        pixel_type: image_pixel_type::<T>().pixel_type_label(),
        units: header.get_string("BUNIT").unwrap_or("").to_string(),
        object_name: image_info.object_name,
        has_restoring_beam: image_info
            .beam_set
            .single_beam()
            .is_some_and(|beam| !beam.is_null()),
    })
}

fn image_info_from_fits_header(header: &FitsHeader) -> ImageInfo {
    let object_name = header.get_string("OBJECT").unwrap_or("").to_string();
    let beam_set = match (
        header.get_float("BMAJ"),
        header.get_float("BMIN"),
        header.get_float("BPA"),
    ) {
        (Some(major_deg), Some(minor_deg), Some(position_angle_deg)) => {
            ImageBeamSet::new(GaussianBeam::new(
                major_deg.to_radians(),
                minor_deg.to_radians(),
                position_angle_deg.to_radians(),
            ))
        }
        _ => ImageBeamSet::default(),
    };
    ImageInfo {
        beam_set,
        image_type: header
            .get_int("IMTYPE")
            .map(|value| ImageType::from_fits_value(value as i32))
            .unwrap_or(ImageType::Undefined),
        object_name,
    }
}

fn read_primary_fits_header(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
    shape: &[usize],
) -> Result<FitsHeader, ImageError> {
    let mut header = FitsHeader::new();
    header.set("NAXIS", FitsValue::Integer(shape.len() as i64));
    for (axis, axis_len) in shape.iter().enumerate() {
        header.set(
            format!("NAXIS{}", axis + 1),
            FitsValue::Integer(*axis_len as i64),
        );
    }
    for key in [
        "RADESYS", "CUNIT1", "CUNIT2", "CUNIT3", "CUNIT4", "CTYPE1", "CTYPE2", "CTYPE3", "CTYPE4",
        "SPECSYS", "BUNIT", "OBJECT", "TELESCOP", "OBSERVER",
    ] {
        if let Some(value) = read_optional_key::<String>(fits, hdu, key)? {
            header.set(key, FitsValue::String(value.trim_end().to_string()));
        }
    }
    for key in [
        "EQUINOX", "CRVAL1", "CRVAL2", "CRVAL3", "CRVAL4", "CRPIX1", "CRPIX2", "CRPIX3", "CRPIX4",
        "CDELT1", "CDELT2", "CDELT3", "CDELT4", "RESTFRQ", "BMAJ", "BMIN", "BPA", "IMTYPE",
        "LONPOLE", "LATPOLE", "CROTA2",
    ] {
        if let Some(value) = read_optional_key::<f64>(fits, hdu, key)? {
            header.set(key, FitsValue::Float(value));
        }
    }
    for i in 1..=shape.len() {
        for j in 1..=shape.len() {
            let pc_key = format!("PC{i}_{j}");
            if let Some(value) = read_optional_key::<f64>(fits, hdu, &pc_key)? {
                header.set(pc_key, FitsValue::Float(value));
            }
            let cd_key = format!("CD{i}_{j}");
            if let Some(value) = read_optional_key::<f64>(fits, hdu, &cd_key)? {
                header.set(cd_key, FitsValue::Float(value));
            }
        }
    }
    Ok(header)
}

fn read_fits_axis_shape(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
) -> Result<Vec<usize>, ImageError> {
    let naxis = hdu
        .read_key::<i64>(fits, "NAXIS")
        .map_err(|error| ImageError::Io(error.to_string()))? as usize;
    let mut shape = Vec::with_capacity(naxis);
    for axis in 1..=naxis {
        let axis_len = hdu
            .read_key::<i64>(fits, &format!("NAXIS{axis}"))
            .map_err(|error| ImageError::Io(error.to_string()))?;
        shape.push(axis_len as usize);
    }
    Ok(shape)
}

fn read_optional_key<T>(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
    key: &str,
) -> Result<Option<T>, ImageError>
where
    T: fitsio::headers::ReadsKey,
{
    match hdu.read_key::<T>(fits, key) {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            let text = error.to_string();
            if text.contains("keyword not found") || text.contains("not found in header") {
                Ok(None)
            } else {
                Err(ImageError::Io(text))
            }
        }
    }
}

trait PixelTypeLabel {
    fn pixel_type_label(&self) -> String;
}

impl PixelTypeLabel for ImagePixelType {
    fn pixel_type_label(&self) -> String {
        match self {
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Complex32 => "Complex32",
            Self::Complex64 => "Complex64",
        }
        .to_string()
    }
}

impl PixelTypeLabel for PagedImage<f32> {
    fn pixel_type_label(&self) -> String {
        "Float32".to_string()
    }
}

impl PixelTypeLabel for PagedImage<f64> {
    fn pixel_type_label(&self) -> String {
        "Float64".to_string()
    }
}

fn imhead_typed<T>(
    path: &Path,
    image: &PagedImage<T>,
    pixel_type: String,
) -> Result<ImageHeaderSummary, ImageError>
where
    T: ImagePixel,
{
    let info = image.image_info()?;
    Ok(ImageHeaderSummary {
        imagename: path.display().to_string(),
        pixel_type,
        shape: image.shape().to_vec(),
        units: image.units().to_string(),
        object_name: info.object_name.clone(),
        image_type: info.image_type.to_string(),
        default_mask: image.default_mask_name(),
        masks: image.mask_names(),
        regions: image.region_names(),
        restoring_beam: info.beam_set.single_beam().and_then(format_beam),
        axes: axis_summaries(image),
    })
}

fn format_beam(beam: GaussianBeam) -> Option<RestoringBeamSummary> {
    if beam.is_null() {
        return None;
    }
    Some(RestoringBeamSummary {
        major_arcsec: beam.major_in("arcsec").ok()?,
        minor_arcsec: beam.minor_in("arcsec").ok()?,
        position_angle_deg: beam.position_angle_in("deg").ok()?,
    })
}

fn axis_summaries<T: ImagePixel>(image: &impl ImageInterface<T>) -> Vec<ImageAxisSummary> {
    let mut summaries = Vec::new();
    let shape = image.shape();
    let mut axis = 0usize;
    for coord_idx in 0..image.coordinates().n_coordinates() {
        let coord = image.coordinates().coordinate(coord_idx);
        let refs = coord.reference_value();
        let refpix = coord.reference_pixel();
        let increments = coord.increment();
        let names = coord.axis_names();
        let units = coord.axis_units();
        for axis_in_coord in 0..coord.n_pixel_axes() {
            summaries.push(ImageAxisSummary {
                axis,
                coordinate_type: format!("{:?}", coord.coordinate_type()),
                name: names
                    .get(axis_in_coord)
                    .cloned()
                    .unwrap_or_else(|| format!("Axis{axis}")),
                shape: *shape.get(axis).unwrap_or(&0),
                reference_value: *refs.get(axis_in_coord).unwrap_or(&0.0),
                reference_pixel: *refpix.get(axis_in_coord).unwrap_or(&0.0),
                increment: *increments.get(axis_in_coord).unwrap_or(&0.0),
                unit: units.get(axis_in_coord).cloned().unwrap_or_default(),
            });
            axis += 1;
        }
    }
    while axis < shape.len() {
        summaries.push(ImageAxisSummary {
            axis,
            coordinate_type: "Linear".to_string(),
            name: format!("Axis{axis}"),
            shape: shape[axis],
            reference_value: 0.0,
            reference_pixel: 0.0,
            increment: 1.0,
            unit: String::new(),
        });
        axis += 1;
    }
    summaries
}

fn imstat_typed<T>(
    path: &Path,
    image: &PagedImage<T>,
    box_pixels: Option<&str>,
    chans: Option<&str>,
    includepix: Option<[f64; 2]>,
) -> Result<ImageStatisticsSummary, ImageError>
where
    T: ImagePixel + StatsElement,
{
    let selection = Selection::new(image, box_pixels, chans)?;
    let sub = image.sub_image(selection.start.clone(), selection.shape.clone())?;
    let mut stats = LatticeStatistics::new(&sub);
    if let Some([min, max]) = includepix {
        stats.set_include_range(min, max);
    }
    if let Some(mask) =
        image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?
    {
        stats.set_pixel_mask(mask);
    }
    let npts = scalar_stat(&stats, Statistic::Npts)?;
    let sum = scalar_stat(&stats, Statistic::Sum)?;
    let min = scalar_stat(&stats, Statistic::Min)?;
    let max = scalar_stat(&stats, Statistic::Max)?;
    let (minpos, maxpos) = stats.get_min_max_pos()?;
    let flux = image_flux(image, sum);
    Ok(ImageStatisticsSummary {
        imagename: path.display().to_string(),
        blc: selection.start.clone(),
        trc: selection
            .start
            .iter()
            .zip(&selection.shape)
            .map(|(start, shape)| start + shape - 1)
            .collect(),
        npts,
        min,
        max,
        sum,
        sumsq: scalar_stat(&stats, Statistic::SumSq)?,
        mean: scalar_stat(&stats, Statistic::Mean)?,
        rms: scalar_stat(&stats, Statistic::Rms)?,
        sigma: scalar_stat(&stats, Statistic::Sigma)?,
        median: scalar_stat(&stats, Statistic::Median)?,
        minpos: minpos.map(|pos| offset_pos(&selection.start, pos)),
        maxpos: maxpos.map(|pos| offset_pos(&selection.start, pos)),
        flux,
        units: image.units().to_string(),
    })
}

fn scalar_stat<T: StatsElement>(
    stats: &LatticeStatistics<'_, T>,
    stat: Statistic,
) -> Result<f64, ImageError> {
    let values = stats.get_statistic(stat)?;
    values
        .iter()
        .next()
        .copied()
        .ok_or_else(|| ImageError::InvalidMetadata("empty statistic result".to_string()))
}

fn offset_pos(start: &[usize], pos: Vec<usize>) -> Vec<usize> {
    start
        .iter()
        .zip(pos)
        .map(|(base, offset)| base + offset)
        .collect()
}

#[derive(Debug, Clone)]
struct Selection {
    start: Vec<usize>,
    shape: Vec<usize>,
    channel_indices: Option<Vec<usize>>,
}

impl Selection {
    fn new<T: ImagePixel>(
        image: &impl ImageInterface<T>,
        box_pixels: Option<&str>,
        chans: Option<&str>,
    ) -> Result<Self, ImageError> {
        let mut start = vec![0; image.ndim()];
        let mut shape = image.shape().to_vec();
        if let Some(box_text) = box_pixels.filter(|text| !text.trim().is_empty()) {
            let [x0, y0, x1, y1] = parse_box(box_text)?;
            if image.ndim() < 2 || x1 >= shape[0] || y1 >= shape[1] {
                return Err(ImageError::ShapeMismatch {
                    expected: image.shape().to_vec(),
                    got: vec![x0, y0, x1, y1],
                });
            }
            start[0] = x0;
            start[1] = y0;
            shape[0] = x1 - x0 + 1;
            shape[1] = y1 - y0 + 1;
        }
        let channel_indices = if let Some(chans) = chans.filter(|text| !text.trim().is_empty()) {
            let spectral_axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
                ImageError::InvalidMetadata(
                    "channel selection requires a spectral axis".to_string(),
                )
            })?;
            let indices = parse_indices(chans, image.shape()[spectral_axis])?;
            start[spectral_axis] = indices[0];
            shape[spectral_axis] = indices[indices.len() - 1] - indices[0] + 1;
            Some(indices)
        } else {
            None
        };
        Ok(Self {
            start,
            shape,
            channel_indices,
        })
    }
}

fn parse_box(text: &str) -> Result<[usize; 4], ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid box {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != 4 || values[2] < values[0] || values[3] < values[1] {
        return Err(ImageError::InvalidMetadata(format!(
            "box must be x0,y0,x1,y1 with inclusive upper bounds: {text:?}"
        )));
    }
    Ok([values[0], values[1], values[2], values[3]])
}

fn parse_indices(text: &str, axis_len: usize) -> Result<Vec<usize>, ImageError> {
    let mut indices = Vec::new();
    for segment in text.split(',').filter(|segment| !segment.trim().is_empty()) {
        let segment = segment.trim();
        if let Some((lhs, rhs)) = segment.split_once('~') {
            let start = lhs.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid channel range {segment:?}: {error}"))
            })?;
            let end = rhs.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid channel range {segment:?}: {error}"))
            })?;
            if end < start {
                return Err(ImageError::InvalidMetadata(format!(
                    "channel range end precedes start: {segment:?}"
                )));
            }
            indices.extend(start..=end);
        } else {
            indices.push(segment.parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid channel {segment:?}: {error}"))
            })?);
        }
    }
    if indices.is_empty() || indices.iter().any(|index| *index >= axis_len) {
        return Err(ImageError::ShapeMismatch {
            expected: vec![axis_len],
            got: indices,
        });
    }
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

fn image_flux<T: ImagePixel>(image: &PagedImage<T>, sum: f64) -> Option<f64> {
    if !image.units().eq_ignore_ascii_case("Jy/beam") {
        return None;
    }
    let pixel_area = direction_pixel_area(image.coordinates())?;
    let beam_area = image.image_info().ok()?.beam_set.single_beam()?.area();
    if beam_area <= 0.0 {
        return None;
    }
    Some(sum * pixel_area / beam_area)
}

fn direction_pixel_area(coords: &CoordinateSystem) -> Option<f64> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        if coord.coordinate_type() == CoordinateType::Direction && coord.n_pixel_axes() >= 2 {
            let increment = coord.increment();
            return Some(increment[0].abs() * increment[1].abs());
        }
    }
    None
}

fn collapsed_image_info<T: ImagePixel>(image: &PagedImage<T>) -> Result<ImageInfo, ImageError> {
    let mut info = image.image_info()?;
    if info.beam_set.is_multi() {
        info.beam_set = ImageBeamSet::new(info.beam_set.common_beam()?);
    }
    Ok(info)
}

fn immoments_typed<T>(
    image: &PagedImage<T>,
    request: &ImmomentsRequest,
) -> Result<MomentMapSummary, ImageError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    if request.moments != 0 && request.moments != 1 {
        return Err(ImageError::InvalidMetadata(format!(
            "tutorial immoments support is limited to moments 0 and 1, got {}",
            request.moments
        )));
    }
    if request.outfile.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.outfile)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "moment output already exists: {}",
                request.outfile.display()
            )));
        }
    }
    let spectral_axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
        ImageError::InvalidMetadata("immoments requires a spectral axis".to_string())
    })?;
    let selection = Selection::new(image, None, request.chans.as_deref())?;
    let input = image.get_slice(&selection.start, &selection.shape)?;
    let mask = image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    let local_axis = spectral_axis;
    let coords = spectral_velocity_values(image, local_axis, &selection)?;
    let scale = integrated_scale_factor(image, local_axis)?;
    let (output_data, output_mask) = collapse_moment(
        &input,
        mask.as_ref(),
        local_axis,
        &coords,
        scale,
        request.includepix,
        request.moments,
    )?;
    let out_shape = output_data.shape().to_vec();
    let mut output = TempImage::<f32>::new(out_shape.clone(), image.coordinates().clone())?;
    output.set_units(moment_units(image.units(), request.moments))?;
    output.set_image_info(&collapsed_image_info(image)?)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&output_data, &vec![0; output.ndim()])?;
    if output_mask.iter().any(|valid| !*valid) {
        output.put_mask("mask0", &output_mask)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.outfile)?;
    let valid_profiles = output_mask.iter().filter(|value| **value).count();
    Ok(MomentMapSummary {
        imagename: request.imagename.display().to_string(),
        outfile: request.outfile.display().to_string(),
        moment: request.moments,
        shape: out_shape,
        units: output.units().to_string(),
        valid_profiles,
    })
}

fn impv_typed<T>(image: &PagedImage<T>, request: &ImpvRequest) -> Result<PvImageSummary, ImageError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    if !request.mode.eq_ignore_ascii_case("coords") {
        return Err(ImageError::InvalidMetadata(format!(
            "impv mode {:?} is not supported by the tutorial path; use mode='coords'",
            request.mode
        )));
    }
    if image.ndim() < 2 {
        return Err(ImageError::InvalidMetadata(
            "impv requires at least two image axes".to_string(),
        ));
    }
    if request.outfile.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.outfile)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "PV output already exists: {}",
                request.outfile.display()
            )));
        }
    }
    let [x0, y0] = parse_pixel_pair(&request.start)?;
    let [x1, y1] = parse_pixel_pair(&request.end)?;
    let path = bresenham_line(x0, y0, x1, y1);
    if path.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "impv path contains no pixels".to_string(),
        ));
    }
    let mut start = vec![0; image.ndim()];
    let mut shape = image.shape().to_vec();
    let spectral_axis = image.find_axis(CoordinateType::Spectral);
    let channel_indices = if let (Some(axis), Some(chans)) = (
        spectral_axis,
        request
            .chans
            .as_deref()
            .filter(|text| !text.trim().is_empty()),
    ) {
        let indices = parse_indices(chans, image.shape()[axis])?;
        start[axis] = indices[0];
        shape[axis] = indices[indices.len() - 1] - indices[0] + 1;
        Some((axis, indices))
    } else {
        None
    };
    let mut output_shape = shape.clone();
    output_shape[0] = path.len();
    output_shape[1] = 1;
    let mut output_data = ArrayD::<f32>::zeros(IxDyn(&output_shape).f());
    let other_shape = output_shape[2..].to_vec();
    let offsets = perpendicular_offsets(x0, y0, x1, y1, request.width.max(1));
    for sample_index in all_indices_for_shape(&other_shape) {
        for (path_index, &(x, y)) in path.iter().enumerate() {
            let mut sum = 0.0;
            let mut count = 0usize;
            for &(dx, dy) in &offsets {
                let sx = x + dx;
                let sy = y + dy;
                if sx < 0
                    || sy < 0
                    || sx as usize >= image.shape()[0]
                    || sy as usize >= image.shape()[1]
                {
                    continue;
                }
                let mut input_index = start.clone();
                input_index[0] = sx as usize;
                input_index[1] = sy as usize;
                for (offset_axis, value) in sample_index.iter().copied().enumerate() {
                    input_index[offset_axis + 2] += value;
                }
                if let Some((axis, indices)) = &channel_indices {
                    let local = sample_index[*axis - 2];
                    input_index[*axis] = indices[local];
                }
                sum += image.get_at(&input_index)?.into();
                count += 1;
            }
            if count > 0 {
                let mut output_index = vec![0; output_shape.len()];
                output_index[0] = path_index;
                output_index[1] = 0;
                for (offset_axis, value) in sample_index.iter().copied().enumerate() {
                    output_index[offset_axis + 2] = value;
                }
                output_data[IxDyn(&output_index)] = (sum / count as f64) as f32;
            }
        }
    }
    let mut output = TempImage::<f32>::new(output_shape.clone(), image.coordinates().clone())?;
    output.set_units(image.units())?;
    output.set_image_info(&collapsed_image_info(image)?)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&output_data, &vec![0; output.ndim()])?;
    output.save_as(&request.outfile)?;
    Ok(PvImageSummary {
        imagename: request.imagename.display().to_string(),
        outfile: request.outfile.display().to_string(),
        shape: output_shape,
        path_pixels: path.len(),
        width: request.width.max(1),
        units: output.units().to_string(),
    })
}

fn parse_pixel_pair(text: &str) -> Result<[isize; 2], ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<isize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid pixel coordinate {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != 2 {
        return Err(ImageError::InvalidMetadata(format!(
            "pixel coordinate must be x,y, got {text:?}"
        )));
    }
    Ok([values[0], values[1]])
}

fn bresenham_line(x0: isize, y0: isize, x1: isize, y1: isize) -> Vec<(isize, isize)> {
    let mut points = Vec::new();
    let mut x = x0;
    let mut y = y0;
    let dx = (x1 - x0).abs();
    let sx = if x0 < x1 { 1 } else { -1 };
    let dy = -(y1 - y0).abs();
    let sy = if y0 < y1 { 1 } else { -1 };
    let mut err = dx + dy;
    loop {
        points.push((x, y));
        if x == x1 && y == y1 {
            break;
        }
        let e2 = 2 * err;
        if e2 >= dy {
            err += dy;
            x += sx;
        }
        if e2 <= dx {
            err += dx;
            y += sy;
        }
    }
    points
}

fn perpendicular_offsets(
    x0: isize,
    y0: isize,
    x1: isize,
    y1: isize,
    width: usize,
) -> Vec<(isize, isize)> {
    let dx = (x1 - x0) as f64;
    let dy = (y1 - y0) as f64;
    let length = (dx * dx + dy * dy).sqrt();
    let (px, py) = if length > 0.0 {
        (-dy / length, dx / length)
    } else {
        (0.0, 1.0)
    };
    let center = (width as isize - 1) as f64 / 2.0;
    (0..width)
        .map(|index| {
            let offset = index as f64 - center;
            (
                (px * offset).round() as isize,
                (py * offset).round() as isize,
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn all_indices_for_shape(shape: &[usize]) -> Vec<Vec<usize>> {
    fn push(shape: &[usize], axis: usize, current: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if axis == shape.len() {
            out.push(current.clone());
            return;
        }
        for value in 0..shape[axis] {
            current.push(value);
            push(shape, axis + 1, current, out);
            current.pop();
        }
    }
    let mut out = Vec::new();
    push(shape, 0, &mut Vec::new(), &mut out);
    out
}

fn collapse_moment<T>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axis: usize,
    coords: &[f64],
    integrated_scale: f64,
    includepix: Option<[f64; 2]>,
    moment: i32,
) -> Result<(ArrayD<f32>, ArrayD<bool>), ImageError>
where
    T: Into<f64> + Copy,
{
    let mut out_shape = input.shape().to_vec();
    out_shape.remove(axis);
    let mut out = ArrayD::<f32>::zeros(IxDyn(&out_shape).f());
    let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(&out_shape).f(), false);
    match mask {
        Some(mask) => Zip::from(&mut out)
            .and(&mut out_mask)
            .and(input.lanes(Axis(axis)))
            .and(mask.lanes(Axis(axis)))
            .for_each(|out_value, out_valid, input_lane, mask_lane| {
                let (result, valid) = collapse_moment_lane(
                    input_lane.iter().copied(),
                    Some(mask_lane.iter().copied()),
                    coords,
                    integrated_scale,
                    includepix,
                    moment,
                );
                *out_value = result;
                *out_valid = valid;
            }),
        None => Zip::from(&mut out)
            .and(&mut out_mask)
            .and(input.lanes(Axis(axis)))
            .for_each(|out_value, out_valid, input_lane| {
                let (result, valid) = collapse_moment_lane(
                    input_lane.iter().copied(),
                    None::<std::iter::Empty<bool>>,
                    coords,
                    integrated_scale,
                    includepix,
                    moment,
                );
                *out_value = result;
                *out_valid = valid;
            }),
    }
    Ok((
        out.insert_axis(Axis(axis)).to_owned(),
        out_mask.insert_axis(Axis(axis)).to_owned(),
    ))
}

fn collapse_moment_lane<T, I, M>(
    values: I,
    mask: Option<M>,
    coords: &[f64],
    integrated_scale: f64,
    includepix: Option<[f64; 2]>,
    moment: i32,
) -> (f32, bool)
where
    T: Into<f64> + Copy,
    I: IntoIterator<Item = T>,
    M: IntoIterator<Item = bool>,
{
    let mut s0 = 0.0;
    let mut s1 = 0.0;
    let mut npts = 0usize;
    match mask {
        Some(mask) => {
            for ((value, coord), valid) in values.into_iter().zip(coords.iter()).zip(mask) {
                if !valid {
                    continue;
                }
                let value = value.into();
                if let Some([min, max]) = includepix {
                    if value < min || value > max {
                        continue;
                    }
                }
                s0 += value;
                s1 += value * *coord;
                npts += 1;
            }
        }
        None => {
            for (value, coord) in values.into_iter().zip(coords.iter()) {
                let value = value.into();
                if let Some([min, max]) = includepix {
                    if value < min || value > max {
                        continue;
                    }
                }
                s0 += value;
                s1 += value * *coord;
                npts += 1;
            }
        }
    }
    let result = if npts == 0 {
        return (0.0, false);
    } else if moment == 0 {
        s0 * integrated_scale
    } else if s0 != 0.0 {
        s1 / s0
    } else {
        return (0.0, false);
    };
    (result as f32, true)
}

#[cfg(test)]
fn all_indices(shape: &[usize]) -> Vec<Vec<usize>> {
    fn push_indices(
        shape: &[usize],
        axis: usize,
        current: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if axis == shape.len() {
            out.push(current.clone());
            return;
        }
        for value in 0..shape[axis] {
            current.push(value);
            push_indices(shape, axis + 1, current, out);
            current.pop();
        }
    }
    let mut out = Vec::new();
    push_indices(shape, 0, &mut Vec::new(), &mut out);
    out
}

#[cfg(test)]
fn collapse_moment_reference<T>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axis: usize,
    coords: &[f64],
    integrated_scale: f64,
    includepix: Option<[f64; 2]>,
    moment: i32,
) -> Result<(ArrayD<f32>, ArrayD<bool>), ImageError>
where
    T: Into<f64> + Copy,
{
    let mut out_shape = input.shape().to_vec();
    out_shape.remove(axis);
    let mut out = ArrayD::<f32>::zeros(IxDyn(&out_shape));
    let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(&out_shape), false);
    for out_index in all_indices(&out_shape) {
        let mut s0 = 0.0;
        let mut s1 = 0.0;
        let mut npts = 0usize;
        for (chan, coord) in coords.iter().enumerate().take(input.shape()[axis]) {
            let mut full_index = out_index.clone();
            full_index.insert(axis, chan);
            if let Some(mask) = mask {
                if !mask[IxDyn(&full_index)] {
                    continue;
                }
            }
            let value = (*input.get(IxDyn(&full_index)).ok_or_else(|| {
                ImageError::InvalidMetadata("moment index out of bounds".to_string())
            })?)
            .into();
            if let Some([min, max]) = includepix {
                if value < min || value > max {
                    continue;
                }
            }
            s0 += value;
            s1 += value * *coord;
            npts += 1;
        }
        let (result, valid) = if npts == 0 {
            (0.0, false)
        } else if moment == 0 {
            (s0 * integrated_scale, true)
        } else if s0 != 0.0 {
            (s1 / s0, true)
        } else {
            (0.0, false)
        };
        out[IxDyn(&out_index)] = result as f32;
        out_mask[IxDyn(&out_index)] = valid;
    }
    Ok((
        out.insert_axis(Axis(axis)).to_owned(),
        out_mask.insert_axis(Axis(axis)).to_owned(),
    ))
}

fn spectral_velocity_values<T: ImagePixel>(
    image: &PagedImage<T>,
    spectral_axis: usize,
    selection: &Selection,
) -> Result<Vec<f64>, ImageError> {
    let full_indices = selection
        .channel_indices
        .clone()
        .unwrap_or_else(|| (0..image.shape()[spectral_axis]).collect());
    let mut values = Vec::with_capacity(full_indices.len());
    let rest = spectral_rest_frequency(image.coordinates()).unwrap_or(0.0);
    for chan in full_indices {
        let mut pixel = vec![0.0; image.ndim()];
        pixel[spectral_axis] = chan as f64;
        let world = image.coordinates().to_world(&pixel)?;
        let freq = *world.get(spectral_axis).ok_or_else(|| {
            ImageError::InvalidMetadata("spectral world coordinate missing".to_string())
        })?;
        values.push(if rest > 0.0 {
            SPEED_OF_LIGHT_KM_S * (1.0 - freq / rest)
        } else {
            freq
        });
    }
    Ok(values)
}

fn integrated_scale_factor<T: ImagePixel>(
    image: &PagedImage<T>,
    spectral_axis: usize,
) -> Result<f64, ImageError> {
    let rest = spectral_rest_frequency(image.coordinates()).unwrap_or(0.0);
    let coord = coordinate_for_axis(image.coordinates(), spectral_axis)
        .ok_or_else(|| ImageError::InvalidMetadata("spectral coordinate missing".to_string()))?;
    let reference_pixel = coord.reference_pixel()[0];
    if rest > 0.0 {
        let f0 = coord.to_world(&[reference_pixel - 0.5])?[0];
        let f1 = coord.to_world(&[reference_pixel + 0.5])?[0];
        let v0 = SPEED_OF_LIGHT_KM_S * (1.0 - f0 / rest);
        let v1 = SPEED_OF_LIGHT_KM_S * (1.0 - f1 / rest);
        Ok((v1 - v0).abs())
    } else {
        Ok(coord.increment()[0].abs())
    }
}

fn coordinate_for_axis(
    coords: &CoordinateSystem,
    axis: usize,
) -> Option<&dyn casa_coordinates::Coordinate> {
    let mut offset = 0;
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        let next = offset + coord.n_pixel_axes();
        if axis >= offset && axis < next {
            return Some(coord);
        }
        offset = next;
    }
    None
}

fn spectral_rest_frequency(coords: &CoordinateSystem) -> Option<f64> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        if coord.coordinate_type() == CoordinateType::Spectral {
            let record = coord.to_record();
            match record.get("restfreq") {
                Some(Value::Scalar(ScalarValue::Float64(value))) => return Some(*value),
                Some(Value::Scalar(ScalarValue::Float32(value))) => return Some(f64::from(*value)),
                _ => {}
            }
        }
    }
    None
}

fn moment_units(input_units: &str, moment: i32) -> String {
    if moment == 0 {
        if input_units.is_empty() {
            "km/s".to_string()
        } else {
            format!("{input_units}.km/s")
        }
    } else {
        "km/s".to_string()
    }
}

fn export_fits_typed<T>(
    imagename: &Path,
    fitsimage: &Path,
    image: &PagedImage<T>,
    velocity: bool,
) -> Result<FitsExportSummary, ImageError>
where
    T: ImagePixel + fitsio::images::WriteImage + Copy,
{
    let layout = casa_fits_export_layout(image.coordinates(), image.shape());
    let fits_dimensions = layout.shape.iter().rev().copied().collect::<Vec<_>>();
    let description = ImageDescription {
        data_type: match image_pixel_type::<T>() {
            ImagePixelType::Float32 => FitsImageType::Float,
            ImagePixelType::Float64 => FitsImageType::Double,
            _ => FitsImageType::Float,
        },
        dimensions: &fits_dimensions,
    };
    let mut fits = FitsFile::create(fitsimage)
        .with_custom_primary(&description)
        .open()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let hdu = fits
        .primary_hdu()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let mut data = image.get()?;
    if layout.axis_order != (0..image.shape().len()).collect::<Vec<_>>() {
        data = data.permuted_axes(layout.axis_order.clone()).to_owned();
    }
    let flat = data
        .as_slice_memory_order()
        .ok_or_else(|| ImageError::InvalidMetadata("image data is not contiguous".to_string()))?;
    hdu.write_image(&mut fits, flat)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let header = to_fits_header(&layout.coordinates, &layout.shape);
    for keyword in header.iter() {
        if keyword.name == "NAXIS"
            || keyword
                .name
                .strip_prefix("NAXIS")
                .is_some_and(|suffix| suffix.chars().all(|ch| ch.is_ascii_digit()))
        {
            continue;
        }
        if velocity
            && keyword.name.starts_with("CTYPE")
            && matches!(keyword.value, FitsValue::String(ref s) if s == "FREQ")
        {
            hdu.write_key(&mut fits, &keyword.name, "VRAD")
                .map_err(|error| ImageError::Io(error.to_string()))?;
            continue;
        }
        write_fits_key(&hdu, &mut fits, &keyword.name, &keyword.value)?;
    }
    if !image.units().is_empty() {
        hdu.write_key(&mut fits, "BUNIT", image.units())
            .map_err(|error| ImageError::Io(error.to_string()))?;
    }
    let info = image.image_info()?;
    if !info.object_name.is_empty() {
        hdu.write_key(&mut fits, "OBJECT", info.object_name.as_str())
            .map_err(|error| ImageError::Io(error.to_string()))?;
    }
    if let Some(beam) = info.beam_set.single_beam() {
        if !beam.is_null() {
            hdu.write_key(&mut fits, "BMAJ", beam.major_in("deg")?)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            hdu.write_key(&mut fits, "BMIN", beam.minor_in("deg")?)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            hdu.write_key(&mut fits, "BPA", beam.position_angle_in("deg")?)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        }
    }
    Ok(FitsExportSummary {
        imagename: imagename.display().to_string(),
        fitsimage: fitsimage.display().to_string(),
        shape: layout.shape,
        velocity,
    })
}

struct FitsExportLayout {
    coordinates: CoordinateSystem,
    axis_order: Vec<usize>,
    shape: Vec<usize>,
}

fn casa_fits_export_layout(
    coordinates: &CoordinateSystem,
    image_shape: &[usize],
) -> FitsExportLayout {
    let mut blocks = Vec::new();
    let mut axis_start = 0usize;
    for coordinate_index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(coordinate_index);
        let axis_count = coordinate.n_pixel_axes();
        let axes = (axis_start..axis_start + axis_count).collect::<Vec<_>>();
        blocks.push((
            casa_fits_coordinate_priority(coordinate.coordinate_type()),
            coordinate_index,
            axes,
        ));
        axis_start += axis_count;
    }
    blocks.sort_by_key(|(priority, coordinate_index, _)| (*priority, *coordinate_index));

    let mut reordered = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
    let mut axis_order = Vec::new();
    for (_, coordinate_index, axes) in &blocks {
        reordered.add_coordinate(coordinates.coordinate(*coordinate_index).clone_box());
        axis_order.extend(axes.iter().copied());
    }
    let shape = axis_order
        .iter()
        .map(|axis| image_shape[*axis])
        .collect::<Vec<_>>();

    FitsExportLayout {
        coordinates: reordered,
        axis_order,
        shape,
    }
}

fn casa_fits_coordinate_priority(coordinate_type: CoordinateType) -> usize {
    match coordinate_type {
        CoordinateType::Direction => 0,
        CoordinateType::Spectral => 1,
        CoordinateType::Stokes => 2,
        CoordinateType::Linear | CoordinateType::Tabular => 3,
    }
}

fn write_fits_key(
    hdu: &fitsio::hdu::FitsHdu,
    fits: &mut FitsFile,
    name: &str,
    value: &FitsValue,
) -> Result<(), ImageError> {
    match value {
        FitsValue::String(value) => hdu.write_key(fits, name, value.as_str()),
        FitsValue::Integer(value) => hdu.write_key(fits, name, *value),
        FitsValue::Float(value) => hdu.write_key(fits, name, *value),
        FitsValue::Logical(value) => hdu.write_key(fits, name, if *value { 1i16 } else { 0i16 }),
    }
    .map_err(|error| ImageError::Io(error.to_string()))
}

fn image_pixel_type<T: ImagePixel>() -> ImagePixelType {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f32>() {
        ImagePixelType::Float32
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>() {
        ImagePixelType::Float64
    } else {
        ImagePixelType::Complex32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::{
        DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate, StokesCoordinate,
        StokesType,
    };
    use casa_types::Complex32;
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;

    #[test]
    fn parse_box_is_inclusive() {
        assert_eq!(parse_box("100,100,150,150").unwrap(), [100, 100, 150, 150]);
    }

    #[test]
    fn parse_indices_expands_casa_range() {
        assert_eq!(parse_indices("4~6,8", 10).unwrap(), vec![4, 5, 6, 8]);
    }

    #[test]
    fn casa_fits_export_layout_places_spectral_before_stokes() {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.5],
            [-1.0e-6, 1.0e-6],
            [4.0, 5.0],
        )));
        coordinates.add_coordinate(Box::new(StokesCoordinate::new(vec![StokesType::I])));
        coordinates.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            372.0e9,
            1.0e6,
            0.0,
            372.0e9,
        )));

        let layout = casa_fits_export_layout(&coordinates, &[10, 11, 1, 7]);
        assert_eq!(layout.axis_order, vec![0, 1, 3, 2]);
        assert_eq!(layout.shape, vec![10, 11, 7, 1]);

        let header = to_fits_header(&layout.coordinates, &layout.shape);
        assert_eq!(
            header.get("CTYPE3"),
            Some(&FitsValue::String("FREQ".to_string()))
        );
        assert_eq!(
            header.get("CTYPE4"),
            Some(&FitsValue::String("STOKES".to_string()))
        );
        assert_eq!(header.get("NAXIS3"), Some(&FitsValue::Integer(7)));
        assert_eq!(header.get("NAXIS4"), Some(&FitsValue::Integer(1)));
    }

    #[test]
    fn image_analysis_schema_and_ui_surfaces_advertise_task_contracts() {
        let bundle = ImageAnalysisTaskSchemaBundle::current("image-analysis");
        assert_eq!(
            bundle.protocol.protocol_name,
            IMAGE_ANALYSIS_TASK_PROTOCOL_NAME
        );
        assert_eq!(bundle.semantic.operations.len(), 6);
        assert_eq!(
            bundle
                .projections
                .cli
                .as_ref()
                .unwrap()
                .machine_actions
                .json_run
                .as_deref(),
            Some("--json-run <SOURCE>")
        );
        assert_eq!(
            bundle.projections.python.as_ref().unwrap()["module"],
            "casars.tasks.image_analysis"
        );

        for (binary, expected_args) in [
            ("immoments", ["imagename", "outfile", "moments"].as_slice()),
            ("impv", ["imagename", "outfile", "start", "end"].as_slice()),
            (
                "exportfits",
                ["imagename", "fitsimage", "velocity"].as_slice(),
            ),
            (
                "importfits",
                ["fitsimage", "imagename", "overwrite"].as_slice(),
            ),
            ("unknown-tool", [].as_slice()),
        ] {
            let schema: serde_json::Value =
                serde_json::from_str(&image_analysis_ui_schema_json(binary).unwrap()).unwrap();
            assert_eq!(schema["schema_version"], 1);
            assert_eq!(schema["invocation_name"], binary);
            assert_eq!(schema["category"], "Images");
            let argument_ids = schema["arguments"]
                .as_array()
                .unwrap()
                .iter()
                .map(|argument| argument["id"].as_str().unwrap())
                .collect::<Vec<_>>();
            for expected in expected_args {
                assert!(
                    argument_ids.contains(expected),
                    "{binary} schema missing {expected}: {argument_ids:?}"
                );
            }
        }
    }

    #[test]
    fn image_analysis_task_dispatch_roundtrips_real_image_products() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.image");
        let moment_path = temp.path().join("moment.image");
        let fits_path = temp.path().join("source.fits");
        let imported_path = temp.path().join("imported.image");
        let request_path = temp.path().join("request.json");
        create_spectral_test_image(&source_path);

        let imhead_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Imhead(ImheadRequest {
                imagename: source_path.clone(),
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Imhead(header) = imhead_result else {
            panic!("expected imhead result");
        };
        assert_eq!(header.shape, vec![2, 2, 3]);
        assert_eq!(header.units, "Jy/beam");
        assert_eq!(header.object_name, "NGC Test");
        assert_eq!(header.axes.len(), 3);
        assert!(header.restoring_beam.is_some());

        let imstat_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Imstat(ImstatRequest {
                imagename: source_path.clone(),
                box_pixels: Some("0,0,1,1".to_string()),
                chans: Some("1~2".to_string()),
                includepix: Some([5.0, 12.0]),
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Imstat(stats) = imstat_result else {
            panic!("expected imstat result");
        };
        assert_eq!(stats.blc, vec![0, 0, 1]);
        assert_eq!(stats.trc, vec![1, 1, 2]);
        assert_eq!(stats.npts, 8.0);
        assert_eq!(stats.units, "Jy/beam");
        assert!(stats.flux.is_some());

        let moment_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Immoments(ImmomentsRequest {
                imagename: source_path.clone(),
                outfile: moment_path.clone(),
                moments: 0,
                chans: Some("0~2".to_string()),
                includepix: Some([2.0, 12.0]),
                overwrite: false,
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Immoments(moment) = moment_result else {
            panic!("expected immoments result");
        };
        assert_eq!(moment.shape, vec![2, 2, 1]);
        assert_eq!(moment.units, "Jy/beam.km/s");
        assert_eq!(moment.valid_profiles, 4);
        let moment_image = PagedImage::<f32>::open(&moment_path).unwrap();
        assert_eq!(moment_image.default_mask_name(), None);

        let pv_path = temp.path().join("pv.image");
        let pv_result = run_image_analysis_task(ImageAnalysisTaskRequest::Impv(ImpvRequest {
            imagename: source_path.clone(),
            outfile: pv_path.clone(),
            mode: "coords".to_string(),
            start: "0,0".to_string(),
            end: "1,1".to_string(),
            width: 1,
            chans: Some("1~2".to_string()),
            overwrite: false,
        }))
        .unwrap();
        let ImageAnalysisTaskResult::Impv(pv) = pv_result else {
            panic!("expected impv result");
        };
        assert_eq!(pv.shape, vec![2, 1, 2]);
        assert_eq!(pv.path_pixels, 2);
        let pv_image = PagedImage::<f32>::open(&pv_path).unwrap();
        assert_eq!(pv_image.shape(), &[2, 1, 2]);

        run_image_analysis_task(ImageAnalysisTaskRequest::Exportfits(ExportFitsRequest {
            imagename: source_path.clone(),
            fitsimage: fits_path.clone(),
            velocity: true,
            overwrite: false,
        }))
        .unwrap();
        assert!(fits_path.exists());

        run_image_analysis_task(ImageAnalysisTaskRequest::Importfits(ImportFitsRequest {
            fitsimage: fits_path.clone(),
            imagename: imported_path.clone(),
            overwrite: false,
        }))
        .unwrap();
        let imported = PagedImage::<f32>::open(&imported_path).unwrap();
        assert_eq!(imported.shape(), &[2, 2, 3]);
        assert_eq!(imported.units(), "Jy/beam");

        fs::write(
            &request_path,
            serde_json::to_string(&ImageAnalysisTaskRequest::Imhead(ImheadRequest {
                imagename: imported_path,
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            read_image_analysis_request_source(&request_path).unwrap(),
            ImageAnalysisTaskRequest::Imhead(_)
        ));
    }

    #[test]
    fn image_analysis_rejects_unsupported_pixels_and_invalid_selections() {
        let temp = tempfile::tempdir().unwrap();
        let real_path = temp.path().join("real.image");
        let complex_path = temp.path().join("complex.image");
        let output_path = temp.path().join("moment.image");
        create_direction_only_test_image(&real_path);
        create_complex_test_image(&complex_path);

        let complex_header = imhead(&complex_path).unwrap();
        assert_eq!(complex_header.pixel_type, "Complex32");
        assert_eq!(complex_header.units, "Jy");
        assert_eq!(complex_header.axes.len(), 2);

        assert!(matches!(
            imstat(&complex_path, None, None, None),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("real-valued images")
        ));
        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: complex_path.clone(),
                outfile: output_path.clone(),
                moments: 0,
                chans: None,
                includepix: None,
                overwrite: false,
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("real-valued images")
        ));
        assert!(matches!(
            export_fits(&complex_path, temp.path().join("complex.fits"), false, false),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("real-valued images")
        ));

        assert!(matches!(
            imstat(&real_path, Some("0,0,4,4"), None, None),
            Err(ImageError::ShapeMismatch { .. })
        ));
        assert!(matches!(
            imstat(&real_path, None, Some("0"), None),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("spectral axis")
        ));
        assert!(matches!(
            parse_box("2,0,1,1"),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("inclusive upper bounds")
        ));
        assert!(matches!(
            parse_indices("3~1", 4),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("end precedes start")
        ));
        assert!(matches!(
            parse_indices("0,9", 4),
            Err(ImageError::ShapeMismatch { .. })
        ));
        assert_eq!(parse_indices("2,1,2", 4).unwrap(), vec![1, 2]);

        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: real_path.clone(),
                outfile: output_path.clone(),
                moments: 2,
                chans: None,
                includepix: None,
                overwrite: false,
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("moments 0 and 1")
        ));
        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: real_path,
                outfile: output_path,
                moments: 0,
                chans: None,
                includepix: None,
                overwrite: false,
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("spectral axis")
        ));
    }

    #[test]
    fn fits_import_export_roundtrip_preserves_pixels_wcs_units_beam_and_object() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.image");
        let first_fits = temp.path().join("first.fits");
        let imported_path = temp.path().join("imported.image");
        let second_fits = temp.path().join("second.fits");

        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [1.0, 0.0],
        )));
        coordinates.add_coordinate(Box::new(StokesCoordinate::new(vec![
            StokesType::I,
            StokesType::Q,
        ])));
        coordinates.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            372.0e9,
            2.0e6,
            2.0,
            372.0e9,
        )));

        let shape = vec![3, 2, 2, 5];
        let mut source =
            PagedImage::<f32>::create(shape.clone(), coordinates, &source_path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f32 + 0.25)
                .collect(),
        )
        .unwrap();
        source.put_slice(&data, &[0, 0, 0, 0]).unwrap();
        source.set_units("Jy/beam").unwrap();
        source
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(GaussianBeam::new(
                    1.7e-4_f64.to_radians(),
                    1.2e-4_f64.to_radians(),
                    (-34.5_f64).to_radians(),
                )),
                image_type: ImageType::Intensity,
                object_name: "TW Hya".to_string(),
            })
            .unwrap();
        source.save().unwrap();

        export_fits(&source_path, &first_fits, false, false).unwrap();
        import_fits(&first_fits, &imported_path, false).unwrap();
        export_fits(&imported_path, &second_fits, false, false).unwrap();

        let imported = PagedImage::<f32>::open(&imported_path).unwrap();
        assert_eq!(imported.shape(), &[3, 2, 5, 2]);
        assert_eq!(imported.units(), "Jy/beam");
        let info = imported.image_info().unwrap();
        assert_eq!(info.object_name, "TW Hya");
        let beam = info.beam_set.single_beam().unwrap();
        assert!((beam.major_in("deg").unwrap() - 1.7e-4).abs() < 1.0e-12);
        assert!((beam.minor_in("deg").unwrap() - 1.2e-4).abs() < 1.0e-12);
        assert!((beam.position_angle_in("deg").unwrap() + 34.5).abs() < 1.0e-12);

        assert_eq!(
            read_fits_pixels(&first_fits),
            read_fits_pixels(&second_fits)
        );
        for key in [
            "NAXIS", "NAXIS1", "NAXIS2", "NAXIS3", "NAXIS4", "BUNIT", "BMAJ", "BMIN", "BPA",
            "OBJECT", "RADESYS", "CTYPE1", "CTYPE2", "CTYPE3", "CTYPE4", "CRVAL1", "CRVAL2",
            "CRVAL3", "CRVAL4", "CRPIX1", "CRPIX2", "CRPIX3", "CRPIX4", "CDELT1", "CDELT2",
            "CDELT3", "CDELT4", "CUNIT1", "CUNIT2", "CUNIT3", "CUNIT4", "RESTFRQ", "SPECSYS",
        ] {
            assert_fits_key_matches(&first_fits, &second_fits, key);
        }
    }

    #[test]
    fn fits_import_export_overwrite_policy_preserves_double_images() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.image");
        let fits_path = temp.path().join("source.fits");
        let imported_path = temp.path().join("imported.image");
        create_double_spectral_test_image(&source_path);

        export_fits(&source_path, &fits_path, false, false).unwrap();
        assert!(matches!(
            export_fits(&source_path, &fits_path, false, false),
            Err(ImageError::Io(message)) if message.contains("already exists")
        ));
        let export_summary = export_fits(&source_path, &fits_path, false, true).unwrap();
        assert_eq!(export_summary.shape, vec![2, 2, 2]);

        import_fits(&fits_path, &imported_path, false).unwrap();
        assert!(matches!(
            import_fits(&fits_path, &imported_path, false),
            Err(ImageError::Io(message)) if message.contains("already exists")
        ));
        let import_summary = import_fits(&fits_path, &imported_path, true).unwrap();
        assert_eq!(import_summary.pixel_type, "Float64");
        assert_eq!(import_summary.units, "K");
        assert_eq!(import_summary.object_name, "Double Test");

        let imported = PagedImage::<f64>::open(&imported_path).unwrap();
        assert_eq!(imported.shape(), &[2, 2, 2]);
        assert_eq!(imported.units(), "K");

        let header = imhead(&imported_path).unwrap();
        assert_eq!(header.pixel_type, "Float64");
        assert_eq!(header.object_name, "Double Test");
        let stats = imstat(
            &imported_path,
            Some("0,0,1,1"),
            Some("0~1"),
            Some([1.0, 8.0]),
        )
        .unwrap();
        assert_eq!(stats.npts, 7.0);
        assert_eq!(stats.units, "K");
    }

    #[test]
    fn optimized_moment_collapse_matches_reference_for_masked_axis() {
        let shape = vec![3, 2, 4];
        let input = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f32 / 10.0)
                .collect(),
        )
        .unwrap();
        let mask = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value % 3 != 0)
                .collect(),
        )
        .unwrap();
        let coords = vec![-2.0, -1.0, 0.0, 1.0];
        for moment in [0, 1] {
            let actual = collapse_moment(
                &input,
                Some(&mask),
                2,
                &coords,
                0.25,
                Some([0.4, 1.8]),
                moment,
            )
            .unwrap();
            let expected = collapse_moment_reference(
                &input,
                Some(&mask),
                2,
                &coords,
                0.25,
                Some([0.4, 1.8]),
                moment,
            )
            .unwrap();
            assert_eq!(actual, expected);
        }
    }

    fn create_spectral_test_image(path: &Path) {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [0.0, 0.0],
        )));
        coordinates.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            372.0e9,
            2.0e6,
            0.0,
            372.0e9,
        )));
        let shape = vec![2, 2, 3];
        let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (1..=shape.iter().product::<usize>())
                .map(|value| value as f32)
                .collect(),
        )
        .unwrap();
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.set_units("Jy/beam").unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(GaussianBeam::new(
                    1.7e-4_f64.to_radians(),
                    1.2e-4_f64.to_radians(),
                    (-34.5_f64).to_radians(),
                )),
                image_type: ImageType::Intensity,
                object_name: "NGC Test".to_string(),
            })
            .unwrap();
        image.save().unwrap();
    }

    fn create_double_spectral_test_image(path: &Path) {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-1.0e-6, 1.0e-6],
            [0.0, 0.0],
        )));
        coordinates.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            115.0e9,
            1.0e6,
            0.0,
            115.0e9,
        )));
        let shape = vec![2, 2, 2];
        let mut image = PagedImage::<f64>::create(shape.clone(), coordinates, path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f64 + 0.125)
                .collect(),
        )
        .unwrap();
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.set_units("K").unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::default(),
                image_type: ImageType::Intensity,
                object_name: "Double Test".to_string(),
            })
            .unwrap();
        image.save().unwrap();
    }

    fn create_direction_only_test_image(path: &Path) {
        let shape = vec![2, 2];
        let mut image =
            PagedImage::<f32>::create(shape.clone(), direction_coordinates(), path).unwrap();
        let data = ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        image.put_slice(&data, &[0, 0]).unwrap();
        image.save().unwrap();
    }

    fn create_complex_test_image(path: &Path) {
        let shape = vec![2, 2];
        let mut image =
            PagedImage::<Complex32>::create(shape.clone(), direction_coordinates(), path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            vec![
                Complex32::new(1.0, 0.5),
                Complex32::new(2.0, -0.5),
                Complex32::new(3.0, 1.5),
                Complex32::new(4.0, -1.5),
            ],
        )
        .unwrap();
        image.put_slice(&data, &[0, 0]).unwrap();
        image.set_units("Jy").unwrap();
        image.save().unwrap();
    }

    fn direction_coordinates() -> CoordinateSystem {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [0.0, 0.0],
        )));
        coordinates
    }

    fn read_fits_pixels(path: &Path) -> Vec<f32> {
        let mut fits = FitsFile::open(path).unwrap();
        let hdu = fits.primary_hdu().unwrap();
        hdu.read_image::<Vec<f32>>(&mut fits).unwrap()
    }

    fn assert_fits_key_matches(lhs: &Path, rhs: &Path, key: &str) {
        let lhs_value = read_fits_key_as_string(lhs, key);
        let rhs_value = read_fits_key_as_string(rhs, key);
        assert_eq!(lhs_value, rhs_value, "FITS key {key}");
    }

    fn read_fits_key_as_string(path: &Path, key: &str) -> String {
        let mut fits = FitsFile::open(path).unwrap();
        let hdu = fits.primary_hdu().unwrap();
        if let Ok(value) = hdu.read_key::<String>(&mut fits, key) {
            return value.trim_end().to_string();
        }
        if let Ok(value) = hdu.read_key::<i64>(&mut fits, key) {
            return value.to_string();
        }
        hdu.read_key::<f64>(&mut fits, key).unwrap().to_string()
    }
}
