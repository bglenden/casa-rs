// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped image-analysis operations matching CASA image task semantics.

use std::fs;
use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType, FitsValue, fits::to_fits_header};
use casa_lattices::{LatticeStatistics, Statistic, StatsElement};
use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use casa_types::{ArrayD, ScalarValue, Value};
use fitsio::{
    FitsFile,
    images::{ImageDescription, ImageType as FitsImageType},
};
use ndarray::{Axis, IxDyn};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    AnyPagedImage, GaussianBeam, ImageError, ImageInterface, ImagePixel, ImagePixelType, ImageType,
    PagedImage, TempImage,
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
                        name: "exportfits".to_string(),
                        request_kind: "exportfits".to_string(),
                        result_kind: Some("exportfits".to_string()),
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
                    "functions": ["imhead", "imstat", "immoments", "exportfits"],
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
    /// CASA `exportfits` style FITS image export.
    Exportfits(ExportFitsRequest),
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
    /// Result for [`ImageAnalysisTaskRequest::Exportfits`].
    Exportfits(FitsExportSummary),
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
        ImageAnalysisTaskRequest::Exportfits(request) => {
            Ok(ImageAnalysisTaskResult::Exportfits(export_fits(
                &request.imagename,
                &request.fitsimage,
                request.velocity,
                request.overwrite,
            )?))
        }
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

trait PixelTypeLabel {
    fn pixel_type_label(&self) -> String;
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
    output.set_image_info(&image.image_info()?)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&output_data, &vec![0; output.ndim()])?;
    output.put_mask("mask0", &output_mask)?;
    output.set_default_mask("mask0")?;
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
    let description = ImageDescription {
        data_type: match image_pixel_type::<T>() {
            ImagePixelType::Float32 => FitsImageType::Float,
            ImagePixelType::Float64 => FitsImageType::Double,
            _ => FitsImageType::Float,
        },
        dimensions: &layout.shape,
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
}
