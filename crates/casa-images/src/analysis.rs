// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped image-analysis operations matching CASA image task semantics.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem, CoordinateType, FitsHeader, FitsValue,
    fits::{from_fits_header, to_fits_header},
};
use casa_lattices::{LatticeStatistics, Statistic, StatsElement, TiledShape};
use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayD, ArrayValue, ScalarValue, Value};
use fitsio::{
    FitsFile,
    hdu::HduInfo,
    images::{ImageDescription, ImageType as FitsImageType},
};
use ndarray::{Array2, Axis, IxDyn, ShapeBuilder, Zip};
use num_complex::Complex32;
use rustfft::FftPlanner;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageInterface, ImagePixel,
    ImagePixelType, ImageType, PagedImage, TempImage,
};

/// Stable protocol name advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_NAME: &str = "casa_image_analysis_task";
/// Stable protocol version advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION: u32 = 1;

const IMAGE_ANALYSIS_TASK_SURFACES: [&str; 11] = [
    "imhead",
    "imstat",
    "immoments",
    "impv",
    "imsubimage",
    "immath",
    "impbcor",
    "imregrid",
    "feather",
    "exportfits",
    "importfits",
];

const SPEED_OF_LIGHT_KM_S: f64 = 299_792.458;
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;
const LINEAR_REGRID_VALID_WEIGHT_MIN: f64 = f64::MIN_POSITIVE;

/// Build the current image-analysis protocol descriptor.
pub fn image_analysis_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        IMAGE_ANALYSIS_TASK_PROTOCOL_NAME,
        IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// Build the current request/result schema bundle.
pub fn image_analysis_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(ImageAnalysisTaskRequest);
    let result_schema = schema_for!(ImageAnalysisTaskResult);
    TaskProviderContract {
        protocol: image_analysis_protocol_descriptor(),
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
                    name: "imsubimage".to_string(),
                    request_kind: "imsubimage".to_string(),
                    result_kind: Some("imsubimage".to_string()),
                },
                TaskOperationDescriptor {
                    name: "immath".to_string(),
                    request_kind: "immath".to_string(),
                    result_kind: Some("immath".to_string()),
                },
                TaskOperationDescriptor {
                    name: "impbcor".to_string(),
                    request_kind: "impbcor".to_string(),
                    result_kind: Some("impbcor".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imregrid".to_string(),
                    request_kind: "imregrid".to_string(),
                    result_kind: Some("imregrid".to_string()),
                },
                TaskOperationDescriptor {
                    name: "feather".to_string(),
                    request_kind: "feather".to_string(),
                    result_kind: Some("feather".to_string()),
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
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                    session: None,
                },
            }),
            python: Some(serde_json::json!({
                "module": "casars.tasks.image_analysis",
                "functions": [
                    "imhead",
                    "imstat",
                    "immoments",
                    "impv",
                    "imsubimage",
                    "immath",
                    "impbcor",
                    "imregrid",
                    "feather",
                    "exportfits",
                    "importfits"
                ],
            })),
        },
        parameter_surfaces: IMAGE_ANALYSIS_TASK_SURFACES
            .into_iter()
            .map(|surface| {
                builtin_surface_bundle(surface).unwrap_or_else(|error| {
                    panic!("built-in image-analysis parameter surface {surface:?}: {error}")
                })
            })
            .collect(),
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas::default(),
        },
    }
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
    /// CASA `imsubimage` style pixel-box/channel extraction.
    Imsubimage(ImsubimageRequest),
    /// CASA `immath(..., mode="evalexpr")` style image arithmetic.
    Immath(ImmathRequest),
    /// CASA `impbcor` style primary-beam correction.
    Impbcor(ImpbcorRequest),
    /// CASA `imregrid` style template-image regridding.
    Imregrid(ImregridRequest),
    /// CASA `feather` style Fourier-domain image combination.
    Feather(FeatherRequest),
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
    /// Result for [`ImageAnalysisTaskRequest::Imsubimage`].
    Imsubimage(ImageSubimageSummary),
    /// Result for [`ImageAnalysisTaskRequest::Immath`].
    Immath(ImageMathSummary),
    /// Result for [`ImageAnalysisTaskRequest::Impbcor`].
    Impbcor(PbcorSummary),
    /// Result for [`ImageAnalysisTaskRequest::Imregrid`].
    Imregrid(ImageRegridSummary),
    /// Result for [`ImageAnalysisTaskRequest::Feather`].
    Feather(FeatherSummary),
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
    /// Region file path or inline CASA CRTF syntax, for example
    /// `box[[100pix,100pix],[150pix,150pix]]` or a world-coordinate CRTF box.
    #[serde(default)]
    pub region: Option<String>,
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
    /// CASA moment number. Supported values are `-1` through `3`.
    pub moments: i32,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Inclusive pixel-value range to include.
    #[serde(default)]
    pub includepix: Option<[f64; 2]>,
    /// CASA image mask expression, supporting tutorial forms like
    /// `pb.image>0.3`.
    #[serde(default)]
    pub mask: Option<String>,
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

/// CASA `imsubimage` request for tutorial pixel-box/channel extraction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImsubimageRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// CASA inclusive pixel box, formatted as `x0,y0,x1,y1`.
    #[serde(default)]
    pub box_pixels: Option<String>,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// CASA `immath(..., mode="evalexpr")` request for tutorial image arithmetic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImmathRequest {
    /// Input CASA image paths. Tutorial expressions reference them as `IM0`,
    /// `IM1`, and so on.
    pub imagename: Vec<PathBuf>,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// Expression to evaluate. This implementation supports tutorial forms
    /// `IM0 * IM1`, `IM0 / IM1`, and scalar multiples of `IM0`.
    pub expr: String,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// CASA `impbcor` request for primary-beam image correction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImpbcorRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Primary-beam CASA image path.
    pub pbimage: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// Correction mode. Supported values are `divide` and `multiply`.
    #[serde(default = "default_impbcor_mode")]
    pub mode: String,
    /// Minimum PB value to keep. Negative disables cutoff masking.
    #[serde(default = "default_impbcor_cutoff")]
    pub cutoff: f64,
    /// Optional pixel box selection. Not implemented for `impbcor` yet.
    #[serde(default, rename = "box")]
    pub box_selection: Option<String>,
    /// Optional region selection. Not implemented for `impbcor` yet.
    #[serde(default)]
    pub region: Option<PathBuf>,
    /// Optional channel selector. Not implemented for `impbcor` yet.
    #[serde(default)]
    pub chans: Option<String>,
    /// Optional Stokes selector. Not implemented for `impbcor` yet.
    #[serde(default)]
    pub stokes: Option<String>,
    /// Optional mask expression. Not implemented for `impbcor` yet.
    #[serde(default)]
    pub mask: Option<String>,
    /// Stretch masks to match the selected image shape. Not implemented for `impbcor` yet.
    #[serde(default)]
    pub stretch: bool,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// CASA `imregrid` request for template-image regridding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImregridRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Template CASA image path whose shape/coordinates define the output.
    pub template: PathBuf,
    /// Output CASA image path.
    pub output: PathBuf,
    /// Interpolation method. Tutorial support accepts `linear` and `nearest`.
    #[serde(default = "default_regrid_interpolation")]
    pub interpolation: String,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// CASA `feather` request for Fourier-domain high/low image combination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FeatherRequest {
    /// Output CASA image path.
    pub imagename: PathBuf,
    /// High-resolution interferometric image on the desired output grid.
    pub highres: PathBuf,
    /// Low-resolution single-dish or total-power image.
    pub lowres: PathBuf,
    /// Single-dish scale factor.
    #[serde(default = "default_sdfactor")]
    pub sdfactor: f32,
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

fn default_regrid_interpolation() -> String {
    "linear".to_string()
}

fn default_sdfactor() -> f32 {
    1.0
}

fn default_impbcor_mode() -> String {
    "divide".to_string()
}

fn default_impbcor_cutoff() -> f64 {
    -1.0
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

/// Summary returned after writing an `imsubimage` product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageSubimageSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Output CASA image path.
    pub outfile: String,
    /// Inclusive lower-left corner in input pixel coordinates.
    pub blc: Vec<usize>,
    /// Inclusive upper-right corner in input pixel coordinates.
    pub trc: Vec<usize>,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Pixel units copied from the input image.
    pub units: String,
}

/// Summary returned after writing an `immath` product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageMathSummary {
    /// Input CASA image paths.
    pub imagename: Vec<String>,
    /// Output CASA image path.
    pub outfile: String,
    /// Expression that was evaluated.
    pub expr: String,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Pixel units copied from the first input image.
    pub units: String,
    /// Valid output pixels after intersecting input masks and finite results.
    pub valid_pixels: usize,
}

/// Summary returned after writing an `impbcor` product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PbcorSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Primary-beam CASA image path.
    pub pbimage: String,
    /// Output CASA image path.
    pub outfile: String,
    /// Correction mode.
    pub mode: String,
    /// Minimum PB value kept.
    pub cutoff: f64,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Pixel units copied from the input image.
    pub units: String,
    /// Valid output pixels after intersecting masks and cutoff.
    pub valid_pixels: usize,
}

/// Summary returned after writing an `imregrid` product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageRegridSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Template CASA image path.
    pub template: String,
    /// Output CASA image path.
    pub output: String,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Valid output pixels.
    pub valid_pixels: usize,
}

/// Summary returned after writing a `feather` product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FeatherSummary {
    /// High-resolution CASA image path.
    pub highres: String,
    /// Low-resolution CASA image path.
    pub lowres: String,
    /// Output CASA image path.
    pub imagename: String,
    /// Output image shape.
    pub shape: Vec<usize>,
    /// Single-dish scale factor.
    pub sdfactor: f32,
    /// Valid output pixels.
    pub valid_pixels: usize,
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
            request.region.as_deref(),
            request.chans.as_deref(),
            request.includepix,
        )?)),
        ImageAnalysisTaskRequest::Immoments(request) => {
            Ok(ImageAnalysisTaskResult::Immoments(immoments(&request)?))
        }
        ImageAnalysisTaskRequest::Impv(request) => {
            Ok(ImageAnalysisTaskResult::Impv(impv(&request)?))
        }
        ImageAnalysisTaskRequest::Imsubimage(request) => {
            Ok(ImageAnalysisTaskResult::Imsubimage(imsubimage(&request)?))
        }
        ImageAnalysisTaskRequest::Immath(request) => {
            Ok(ImageAnalysisTaskResult::Immath(immath(&request)?))
        }
        ImageAnalysisTaskRequest::Impbcor(request) => {
            Ok(ImageAnalysisTaskResult::Impbcor(impbcor(&request)?))
        }
        ImageAnalysisTaskRequest::Imregrid(request) => {
            Ok(ImageAnalysisTaskResult::Imregrid(imregrid(&request)?))
        }
        ImageAnalysisTaskRequest::Feather(request) => {
            Ok(ImageAnalysisTaskResult::Feather(feather(&request)?))
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

/// Dispatch shared help and the machine-readable shell for every image-analysis task binary.
pub fn dispatch_image_analysis_task_cli(
    args: &[String],
    human_help: &str,
) -> Result<Option<String>, String> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(Some(format!(
            "{}\n\n{}",
            human_help.trim_end(),
            casa_task_runtime::task_cli_machine_help("ImageAnalysisTaskRequest")
        )));
    }
    let args = args
        .iter()
        .map(std::ffi::OsString::from)
        .collect::<Vec<_>>();
    casa_task_runtime::TaskCliHost::new(
        image_analysis_task_schema_bundle(),
        |request: ImageAnalysisTaskRequest| {
            run_image_analysis_task(request).map_err(|error| error.to_string())
        },
    )
    .dispatch(&args)
    .map_err(|error| error.to_string())
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
    region: Option<&str>,
    chans: Option<&str>,
    includepix: Option<[f64; 2]>,
) -> Result<ImageStatisticsSummary, ImageError> {
    let path = path.as_ref();
    let image = AnyPagedImage::open(path)?;
    match &image {
        AnyPagedImage::Float32(image) => {
            imstat_typed(path, image, box_pixels, region, chans, includepix)
        }
        AnyPagedImage::Float64(image) => {
            imstat_typed(path, image, box_pixels, region, chans, includepix)
        }
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("imstat currently supports real-valued images".to_string()),
        ),
    }
}

/// Generate a CASA `immoments`-style moment map.
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

/// Extract a CASA `imsubimage`-style image section for tutorial pixel boxes.
pub fn imsubimage(request: &ImsubimageRequest) -> Result<ImageSubimageSummary, ImageError> {
    let image = AnyPagedImage::open(&request.imagename)?;
    match &image {
        AnyPagedImage::Float32(image) => imsubimage_typed(image, request),
        AnyPagedImage::Float64(image) => imsubimage_typed(image, request),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            Err(ImageError::InvalidMetadata(
                "imsubimage currently supports real-valued images".to_string(),
            ))
        }
    }
}

/// Evaluate a tutorial-scoped CASA `immath(..., mode="evalexpr")` expression.
pub fn immath(request: &ImmathRequest) -> Result<ImageMathSummary, ImageError> {
    let op = parse_tutorial_immath_expr(&request.expr)?;
    match request.imagename.len() {
        1 => {
            let lhs = AnyPagedImage::open(&request.imagename[0])?;
            match (&lhs, op) {
                (AnyPagedImage::Float32(lhs), TutorialMathOp::Scale(factor)) => {
                    immath_unary_typed(lhs, request, factor)
                }
                (AnyPagedImage::Float64(lhs), TutorialMathOp::Scale(factor)) => {
                    immath_unary_typed(lhs, request, factor)
                }
                (_, TutorialMathOp::Multiply | TutorialMathOp::Divide) => {
                    Err(ImageError::InvalidMetadata(
                        "binary immath expressions require two input images".to_string(),
                    ))
                }
                _ => Err(ImageError::InvalidMetadata(
                    "immath tutorial support currently requires real-valued pixel types"
                        .to_string(),
                )),
            }
        }
        2 => {
            let lhs = AnyPagedImage::open(&request.imagename[0])?;
            let rhs = AnyPagedImage::open(&request.imagename[1])?;
            match (&lhs, &rhs, op) {
                (AnyPagedImage::Float32(lhs), AnyPagedImage::Float32(rhs), op) => {
                    immath_typed(lhs, rhs, request, op)
                }
                (AnyPagedImage::Float64(lhs), AnyPagedImage::Float64(rhs), op) => {
                    immath_typed(lhs, rhs, request, op)
                }
                _ => Err(ImageError::InvalidMetadata(
                    "immath tutorial support currently requires matching real-valued pixel types"
                        .to_string(),
                )),
            }
        }
        count => Err(ImageError::InvalidMetadata(format!(
            "immath tutorial support currently requires one or two input images, got {count}"
        ))),
    }
}

/// Apply CASA `impbcor`-style primary-beam correction.
pub fn impbcor(request: &ImpbcorRequest) -> Result<PbcorSummary, ImageError> {
    if request.box_selection.is_some()
        || request.region.is_some()
        || request.chans.is_some()
        || request.stokes.is_some()
        || request.mask.is_some()
        || request.stretch
    {
        return Err(ImageError::InvalidMetadata(
            "impbcor selection parameters (box, region, chans, stokes, mask, stretch) are not yet supported; run image-wide PB correction or use a pre-selected image"
                .to_string(),
        ));
    }
    let image = AnyPagedImage::open(&request.imagename)?;
    let pb = AnyPagedImage::open(&request.pbimage)?;
    match (&image, &pb) {
        (AnyPagedImage::Float32(image), AnyPagedImage::Float32(pb)) => {
            impbcor_typed(image, pb, request)
        }
        (AnyPagedImage::Float64(image), AnyPagedImage::Float64(pb)) => {
            impbcor_typed(image, pb, request)
        }
        _ => Err(ImageError::InvalidMetadata(
            "impbcor tutorial support currently requires matching real-valued pixel types"
                .to_string(),
        )),
    }
}

/// Regrid an image onto a template image's shape and coordinates.
pub fn imregrid(request: &ImregridRequest) -> Result<ImageRegridSummary, ImageError> {
    let input = AnyPagedImage::open(&request.imagename)?;
    let template = AnyPagedImage::open(&request.template)?;
    match (&input, &template) {
        (AnyPagedImage::Float32(input), AnyPagedImage::Float32(template)) => {
            imregrid_typed(input, template, request)
        }
        (AnyPagedImage::Float64(input), AnyPagedImage::Float64(template)) => {
            imregrid_typed(input, template, request)
        }
        _ => Err(ImageError::InvalidMetadata(
            "imregrid tutorial support currently requires matching real-valued pixel types"
                .to_string(),
        )),
    }
}

/// Feather high- and low-resolution images on the high-resolution grid.
pub fn feather(request: &FeatherRequest) -> Result<FeatherSummary, ImageError> {
    let high = AnyPagedImage::open(&request.highres)?;
    let low = AnyPagedImage::open(&request.lowres)?;
    match (&high, &low) {
        (AnyPagedImage::Float32(high), AnyPagedImage::Float32(low)) => {
            feather_typed(high, low, request)
        }
        _ => Err(ImageError::InvalidMetadata(
            "feather tutorial support currently requires f32 high/low images".to_string(),
        )),
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

/// Apply the CASA tutorial-supported `imhead(..., mode="put")` metadata update.
pub fn imhead_put(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: impl Into<String>,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    let hdkey = hdkey.trim();
    let hdvalue = hdvalue.into();
    if !hdkey.eq_ignore_ascii_case("bunit") {
        return Err(ImageError::InvalidMetadata(format!(
            "imhead mode='put' only supports hdkey='bunit' for this tutorial slice, got {hdkey:?}"
        )));
    }

    let mut image = AnyPagedImage::open(path)?;
    match &mut image {
        AnyPagedImage::Float32(image) => image.set_units(hdvalue)?,
        AnyPagedImage::Float64(image) => image.set_units(hdvalue)?,
        AnyPagedImage::Complex32(image) => image.set_units(hdvalue)?,
        AnyPagedImage::Complex64(image) => image.set_units(hdvalue)?,
    }
    image.save()?;
    imhead(path)
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
    region: Option<&str>,
    chans: Option<&str>,
    includepix: Option<[f64; 2]>,
) -> Result<ImageStatisticsSummary, ImageError>
where
    T: ImagePixel + StatsElement,
{
    let region_selection = region_pixels(image, region)?;
    if box_pixels.is_some_and(|text| !text.trim().is_empty()) && region_selection.is_some() {
        return Err(ImageError::InvalidMetadata(
            "imstat accepts either box or region, not both".to_string(),
        ));
    }
    let explicit_box = match box_pixels.filter(|text| !text.trim().is_empty()) {
        Some(box_text) => Some(PixelRegion::Box(parse_box(box_text)?)),
        None => None,
    };
    let effective_region = explicit_box.as_ref().or(region_selection.as_ref());
    let selection = Selection::new(image, effective_region, chans)?;
    let sub = image.sub_image(selection.start.clone(), selection.shape.clone())?;
    let mut stats = LatticeStatistics::new(&sub);
    if let Some([min, max]) = includepix {
        stats.set_include_range(min, max);
    }
    let mut pixel_mask =
        image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    if let Some(region) = effective_region {
        let region_mask = region_pixel_mask(region, &selection)?;
        pixel_mask = Some(match pixel_mask {
            Some(mut mask) => {
                Zip::from(&mut mask)
                    .and(&region_mask)
                    .for_each(|existing, region_valid| *existing = *existing && *region_valid);
                mask
            }
            None => region_mask,
        });
    }
    if let Some(mask) = pixel_mask {
        stats.set_pixel_mask(mask);
    }
    let npts = scalar_stat(&stats, Statistic::Npts)?;
    let sum = scalar_stat(&stats, Statistic::Sum)?;
    let min = scalar_stat(&stats, Statistic::Min)?;
    let max = scalar_stat(&stats, Statistic::Max)?;
    let (minpos, maxpos) = stats.get_min_max_pos()?;
    let flux = image_flux(image, sum, &selection);
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
    full_shape: Vec<usize>,
    start: Vec<usize>,
    shape: Vec<usize>,
    channel_indices: Option<Vec<usize>>,
}

impl Selection {
    fn new<T: ImagePixel>(
        image: &impl ImageInterface<T>,
        region: Option<&PixelRegion>,
        chans: Option<&str>,
    ) -> Result<Self, ImageError> {
        let mut start = vec![0; image.ndim()];
        let mut shape = image.shape().to_vec();
        if let Some(region) = region {
            let [x0, y0, x1, y1] = region.bounding_box();
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
            full_shape: image.shape().to_vec(),
            start,
            shape,
            channel_indices,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PixelRegion {
    Box([usize; 4]),
    Polygon(Vec<[usize; 2]>),
}

impl PixelRegion {
    fn bounding_box(&self) -> [usize; 4] {
        match self {
            Self::Box(bounds) => *bounds,
            Self::Polygon(vertices) => {
                let min_x = vertices.iter().map(|vertex| vertex[0]).min().unwrap_or(0);
                let min_y = vertices.iter().map(|vertex| vertex[1]).min().unwrap_or(0);
                let max_x = vertices
                    .iter()
                    .map(|vertex| vertex[0])
                    .max()
                    .unwrap_or(min_x);
                let max_y = vertices
                    .iter()
                    .map(|vertex| vertex[1])
                    .max()
                    .unwrap_or(min_y);
                [min_x, min_y, max_x, max_y]
            }
        }
    }
}

fn region_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    region: Option<&str>,
) -> Result<Option<PixelRegion>, ImageError> {
    let Some(region) = region.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    if let Some(parsed) = crtf_box_pixels(image, region)? {
        return Ok(Some(PixelRegion::Box(parse_box(&parsed)?)));
    }
    if let Some(parsed) = crtf_polygon_pixels(image, region)? {
        return Ok(Some(PixelRegion::Polygon(parsed)));
    }
    if let Some(box_text) = region.strip_prefix("box:") {
        let parsed = parse_box(box_text.trim())?;
        return Ok(Some(PixelRegion::Box(parsed)));
    }
    if let Some(contents) = region
        .strip_prefix("pixelbox(")
        .and_then(|text| text.strip_suffix(')'))
    {
        let parsed = parse_box(contents.trim())?;
        return Ok(Some(PixelRegion::Box(parsed)));
    }
    if looks_like_box(region) {
        let parsed = parse_box(region)?;
        return Ok(Some(PixelRegion::Box(parsed)));
    }

    let region_path = Path::new(region);
    let contents = fs::read_to_string(region_path).map_err(|error| {
        ImageError::Io(format!(
            "read region file {}: {error}",
            region_path.display()
        ))
    })?;
    if let Some(box_pixels) = crtf_box_pixels(image, &contents)? {
        return Ok(Some(PixelRegion::Box(parse_box(&box_pixels)?)));
    }
    if let Some(polygon) = crtf_polygon_pixels(image, &contents)? {
        return Ok(Some(PixelRegion::Polygon(polygon)));
    }
    let format = if contents.trim_start().starts_with('{') {
        "casa-rs JSON region files are not a persistent interchange format; use CASA CRTF"
    } else {
        "expected CASA CRTF box or polygon, for example box[[100pix,100pix],[150pix,150pix]] or poly [[100pix,100pix],...]"
    };
    Err(ImageError::InvalidMetadata(format!(
        "unsupported region file {}: {format}",
        region_path.display()
    )))
}

fn region_pixel_mask(
    region: &PixelRegion,
    selection: &Selection,
) -> Result<ArrayD<bool>, ImageError> {
    let mask = match region {
        PixelRegion::Box(_) => ArrayD::from_elem(IxDyn(&selection.shape).f(), true),
        PixelRegion::Polygon(vertices) => {
            if vertices.len() < 3 {
                return Err(ImageError::InvalidMetadata(
                    "CRTF polygon regions require at least three vertices".to_string(),
                ));
            }
            ArrayD::from_shape_fn(IxDyn(&selection.shape).f(), |idx| {
                let x = selection.start[0] + idx[0];
                let y = selection.start[1] + idx[1];
                point_in_polygon_or_boundary(x as f64 + 0.5, y as f64 + 0.5, vertices)
            })
        }
    };
    Ok(mask)
}

fn crtf_box_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    text: &str,
) -> Result<Option<String>, ImageError> {
    let Some(parts) = crtf_box_parts(text)? else {
        return Ok(None);
    };
    let pixels = parts
        .iter()
        .map(|part| parse_crtf_pixel_quantity(part))
        .collect::<Result<Vec<_>, ImageError>>()?;
    if pixels.iter().all(Option::is_some) {
        let values = pixels.into_iter().flatten().collect::<Vec<_>>();
        return Ok(Some(format!(
            "{},{},{},{}",
            values[0], values[1], values[2], values[3]
        )));
    }
    if pixels.iter().any(Option::is_some) {
        return Err(ImageError::InvalidMetadata(
            "cannot mix pix and non-pix coordinates in a CRTF box region".to_string(),
        ));
    }

    let world = parts
        .iter()
        .map(|part| parse_crtf_world_quantity(part))
        .collect::<Result<Vec<_>, ImageError>>()?;
    Ok(Some(crtf_world_box_pixels(
        image,
        [world[0], world[1], world[2], world[3]],
    )?))
}

fn crtf_polygon_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    text: &str,
) -> Result<Option<Vec<[usize; 2]>>, ImageError> {
    let Some(pairs) = crtf_polygon_parts(text)? else {
        return Ok(None);
    };
    if pairs.len() < 3 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected at least three vertices"
        )));
    }
    crtf_coordinate_pairs_to_pixels(image, &pairs).map(Some)
}

fn crtf_polygon_parts(text: &str) -> Result<Option<Vec<[String; 2]>>, ImageError> {
    let normalized = text.trim();
    let Some(start) = normalized.find("poly [") else {
        return Ok(None);
    };
    let after = &normalized[start + "poly ".len()..];
    let end = after.rfind(']').ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: missing closing ]"
        ))
    })?;
    let body = &after[..=end];
    let body = body
        .strip_prefix('[')
        .and_then(|body| body.strip_suffix(']'))
        .unwrap_or(body);
    let mut pairs = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = body[cursor..].find('[') {
        let pair_start = cursor + relative_start + 1;
        let pair_end = body[pair_start..].find(']').ok_or_else(|| {
            ImageError::InvalidMetadata(format!(
                "invalid CASA CRTF polygon region {text:?}: missing vertex closing ]"
            ))
        })? + pair_start;
        let pair = body[pair_start..pair_end]
            .split(',')
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if pair.len() == 2 {
            pairs.push([pair[0].clone(), pair[1].clone()]);
        }
        cursor = pair_end + 1;
    }
    if pairs.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected poly [[x0,y0],...]"
        )));
    }
    Ok(Some(pairs))
}

fn crtf_coordinate_pairs_to_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    pairs: &[[String; 2]],
) -> Result<Vec<[usize; 2]>, ImageError> {
    let pixel_pairs = pairs
        .iter()
        .map(|pair| {
            Ok([
                parse_crtf_pixel_quantity(&pair[0])?,
                parse_crtf_pixel_quantity(&pair[1])?,
            ])
        })
        .collect::<Result<Vec<_>, ImageError>>()?;
    if pixel_pairs
        .iter()
        .all(|pair| pair[0].is_some() && pair[1].is_some())
    {
        return Ok(pixel_pairs
            .into_iter()
            .map(|pair| [pair[0].unwrap(), pair[1].unwrap()])
            .collect());
    }
    if pixel_pairs
        .iter()
        .any(|pair| pair[0].is_some() || pair[1].is_some())
    {
        return Err(ImageError::InvalidMetadata(
            "cannot mix pix and non-pix coordinates in a CRTF polygon region".to_string(),
        ));
    }

    let coordinates = image.coordinates();
    if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
        return Err(ImageError::InvalidMetadata(
            "world-coordinate CRTF polygon regions require an image coordinate system with at least two axes"
                .to_string(),
        ));
    }
    let base_pixel = vec![0.0; coordinates.n_pixel_axes()];
    let base_world = coordinates.to_world(&base_pixel)?;
    pairs
        .iter()
        .map(|pair| {
            let mut world = base_world.clone();
            world[0] = parse_crtf_world_quantity(&pair[0])?;
            world[1] = parse_crtf_world_quantity(&pair[1])?;
            let pixel = coordinates.to_pixel(&world)?;
            Ok([
                rounded_nonnegative_pixel_value(pixel[0], "CRTF polygon world x")?,
                rounded_nonnegative_pixel_value(pixel[1], "CRTF polygon world y")?,
            ])
        })
        .collect()
}

fn crtf_box_parts(text: &str) -> Result<Option<[String; 4]>, ImageError> {
    let normalized = text.trim();
    let Some(start) = normalized.find("box[[") else {
        return Ok(None);
    };
    let after = &normalized[start + "box[[".len()..];
    let end = after.find("]]").ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: missing closing ]]"
        ))
    })?;
    let body = &after[..end];
    let body = body
        .replace("], [", ",")
        .replace("],[", ",")
        .replace(['[', ']'], "");
    let parts = body
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 4 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: expected four coordinates"
        )));
    }
    Ok(Some([
        parts[0].clone(),
        parts[1].clone(),
        parts[2].clone(),
        parts[3].clone(),
    ]))
}

fn parse_crtf_pixel_quantity(text: &str) -> Result<Option<usize>, ImageError> {
    let text = text.trim();
    let lowercase = text.to_ascii_lowercase();
    let Some(value) = lowercase.strip_suffix("pix") else {
        return Ok(None);
    };
    rounded_nonnegative_pixel(value.trim(), text).map(Some)
}

fn parse_crtf_world_quantity(text: &str) -> Result<f64, ImageError> {
    let text = text.trim();
    let lowercase = text.to_ascii_lowercase();
    let (value, scale) = if let Some(value) = lowercase.strip_suffix("arcsec") {
        (value, std::f64::consts::PI / 180.0 / 3600.0)
    } else if let Some(value) = lowercase.strip_suffix("arcmin") {
        (value, std::f64::consts::PI / 180.0 / 60.0)
    } else if let Some(value) = lowercase.strip_suffix("deg") {
        (value, std::f64::consts::PI / 180.0)
    } else if let Some(value) = lowercase.strip_suffix("rad") {
        (value, 1.0)
    } else {
        (lowercase.as_str(), 1.0)
    };
    let parsed = value.trim().parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid CRTF world coordinate {text:?}: {error}"))
    })?;
    if !parsed.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF world coordinate {text:?}: value is not finite"
        )));
    }
    Ok(parsed * scale)
}

fn crtf_world_box_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    world_box: [f64; 4],
) -> Result<String, ImageError> {
    let coordinates = image.coordinates();
    if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
        return Err(ImageError::InvalidMetadata(
            "world-coordinate CRTF box regions require an image coordinate system with at least two axes"
                .to_string(),
        ));
    }
    let base_pixel = vec![0.0; coordinates.n_pixel_axes()];
    let mut first_world = coordinates.to_world(&base_pixel)?;
    let mut second_world = first_world.clone();
    first_world[0] = world_box[0];
    first_world[1] = world_box[1];
    second_world[0] = world_box[2];
    second_world[1] = world_box[3];
    let first_pixel = coordinates.to_pixel(&first_world)?;
    let second_pixel = coordinates.to_pixel(&second_world)?;
    let x0 = rounded_nonnegative_pixel_value(first_pixel[0], "first CRTF world x")?;
    let y0 = rounded_nonnegative_pixel_value(first_pixel[1], "first CRTF world y")?;
    let x1 = rounded_nonnegative_pixel_value(second_pixel[0], "second CRTF world x")?;
    let y1 = rounded_nonnegative_pixel_value(second_pixel[1], "second CRTF world y")?;
    Ok(format!(
        "{},{},{},{}",
        x0.min(x1),
        y0.min(y1),
        x0.max(x1),
        y0.max(y1)
    ))
}

fn rounded_nonnegative_pixel(text: &str, original: &str) -> Result<usize, ImageError> {
    let value = text.parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "invalid CRTF pixel coordinate {original:?}: {error}"
        ))
    })?;
    rounded_nonnegative_pixel_value(value, original)
}

fn rounded_nonnegative_pixel_value(value: f64, label: &str) -> Result<usize, ImageError> {
    if !value.is_finite() || value < -1.0e-6 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF pixel coordinate {label:?}: value is not a non-negative finite number"
        )));
    }
    Ok(value.max(0.0).round() as usize)
}

fn point_in_polygon_or_boundary(x: f64, y: f64, vertices: &[[usize; 2]]) -> bool {
    let mut inside = false;
    let mut previous = vertices[vertices.len() - 1];
    for &current in vertices {
        let x0 = previous[0] as f64;
        let y0 = previous[1] as f64;
        let x1 = current[0] as f64;
        let y1 = current[1] as f64;
        if point_on_segment(x, y, x0, y0, x1, y1) {
            return true;
        }
        let crosses = (y0 > y) != (y1 > y);
        if crosses {
            let x_intersection = (x1 - x0) * (y - y0) / (y1 - y0) + x0;
            if x <= x_intersection {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

fn point_on_segment(x: f64, y: f64, x0: f64, y0: f64, x1: f64, y1: f64) -> bool {
    let cross = (x - x0) * (y1 - y0) - (y - y0) * (x1 - x0);
    if cross.abs() > 1.0e-9 {
        return false;
    }
    x >= x0.min(x1) - 1.0e-9
        && x <= x0.max(x1) + 1.0e-9
        && y >= y0.min(y1) - 1.0e-9
        && y <= y0.max(y1) + 1.0e-9
}

fn looks_like_box(text: &str) -> bool {
    let mut count = 0usize;
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() || part.parse::<usize>().is_err() {
            return false;
        }
        count += 1;
    }
    count == 4
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

fn image_flux<T: ImagePixel>(
    image: &PagedImage<T>,
    sum: f64,
    selection: &Selection,
) -> Option<f64> {
    if !image.units().eq_ignore_ascii_case("Jy/beam") {
        return None;
    }
    let pixel_area = direction_pixel_area(image.coordinates())?;
    let beam_area = image.image_info().ok()?.beam_set.single_beam()?.area();
    if beam_area <= 0.0 {
        return None;
    }
    let mut flux = sum * pixel_area / beam_area;
    if selection
        .channel_indices
        .as_ref()
        .is_some_and(|indices| indices.len() > 1)
    {
        // Match CASA imstat's reported Jy/beam flux for spectral-channel ranges.
        flux /= 2.0;
    }
    Some(flux)
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
    if !(-1..=3).contains(&request.moments) {
        return Err(ImageError::InvalidMetadata(format!(
            "immoments support is limited to moments -1 through 3, got {}",
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
    let mask = combined_moment_mask(image, &selection, request.mask.as_deref())?;
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
    let legacy_coordinates = image.table().keywords().get("coords").cloned();
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
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&request.outfile, coords)?;
    }
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

fn combined_moment_mask<T>(
    image: &PagedImage<T>,
    selection: &Selection,
    mask_expr: Option<&str>,
) -> Result<Option<ArrayD<bool>>, ImageError>
where
    T: ImagePixel,
{
    let base_mask =
        image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    let Some(mask_expr) = mask_expr else {
        return Ok(base_mask);
    };
    let expr_mask = image_threshold_mask(mask_expr, selection)?;
    let combined = match base_mask {
        Some(mut base) => {
            Zip::from(&mut base).and(&expr_mask).for_each(|base, expr| {
                *base = *base && *expr;
            });
            base
        }
        None => expr_mask,
    };
    Ok(Some(combined))
}

fn image_threshold_mask(expr: &str, selection: &Selection) -> Result<ArrayD<bool>, ImageError> {
    let parsed = parse_image_threshold_mask(expr)?;
    let mask_image = AnyPagedImage::open(&parsed.imagename)?;
    match &mask_image {
        AnyPagedImage::Float32(image) => image_threshold_mask_typed(image, &parsed, selection),
        AnyPagedImage::Float64(image) => image_threshold_mask_typed(image, &parsed, selection),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("mask expressions require real-valued images".to_string()),
        ),
    }
}

fn image_threshold_mask_typed<T>(
    image: &PagedImage<T>,
    parsed: &ParsedThresholdMask,
    selection: &Selection,
) -> Result<ArrayD<bool>, ImageError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    if image.shape() != selection.full_shape.as_slice() {
        return Err(ImageError::InvalidMetadata(format!(
            "mask image shape {:?} does not match input image shape {:?}",
            image.shape(),
            selection.full_shape
        )));
    }
    let data = image.get_slice(&selection.start, &selection.shape)?;
    let image_mask =
        image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    let mut out = data.mapv(|value| parsed.op.compare(value.into(), parsed.threshold));
    if let Some(image_mask) = image_mask {
        Zip::from(&mut out).and(&image_mask).for_each(|out, valid| {
            *out = *out && *valid;
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedThresholdMask {
    imagename: PathBuf,
    op: ThresholdOp,
    threshold: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThresholdOp {
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

impl ThresholdOp {
    fn compare(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Greater => value > threshold,
            Self::GreaterEqual => value >= threshold,
            Self::Less => value < threshold,
            Self::LessEqual => value <= threshold,
        }
    }
}

fn parse_image_threshold_mask(expr: &str) -> Result<ParsedThresholdMask, ImageError> {
    let expr = expr.trim();
    for (token, op) in [
        (">=", ThresholdOp::GreaterEqual),
        ("<=", ThresholdOp::LessEqual),
        (">", ThresholdOp::Greater),
        ("<", ThresholdOp::Less),
    ] {
        if let Some((lhs, rhs)) = expr.split_once(token) {
            let imagename = strip_optional_quotes(lhs.trim());
            if imagename.is_empty() {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid mask expression {expr:?}: missing image name"
                )));
            }
            let threshold = rhs.trim().parse::<f64>().map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "invalid mask expression {expr:?}: invalid threshold: {error}"
                ))
            })?;
            return Ok(ParsedThresholdMask {
                imagename: PathBuf::from(imagename),
                op,
                threshold,
            });
        }
    }
    Err(ImageError::InvalidMetadata(format!(
        "unsupported mask expression {expr:?}; expected image>threshold"
    )))
}

fn strip_optional_quotes(text: &str) -> &str {
    text.strip_prefix('\'')
        .and_then(|text| text.strip_suffix('\''))
        .or_else(|| {
            text.strip_prefix('"')
                .and_then(|text| text.strip_suffix('"'))
        })
        .unwrap_or(text)
}

fn imsubimage_typed<T>(
    image: &PagedImage<T>,
    request: &ImsubimageRequest,
) -> Result<ImageSubimageSummary, ImageError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    if request.outfile.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.outfile)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "subimage output already exists: {}",
                request.outfile.display()
            )));
        }
    }
    let box_region = match request
        .box_pixels
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    {
        Some(box_text) => Some(PixelRegion::Box(parse_box(box_text)?)),
        None => None,
    };
    let selection = Selection::new(image, box_region.as_ref(), request.chans.as_deref())?;
    let data = image.get_slice(&selection.start, &selection.shape)?;
    let mask = image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    let output_coordinates = subimage_coordinates(image.coordinates(), &selection.start)?;
    let legacy_coordinates = legacy_subimage_coordinates(image, &selection.start);
    let mut output = TempImage::<T>::new(selection.shape.clone(), output_coordinates)?;
    output.set_units(image.units())?;
    output.set_image_info(&image.image_info()?)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&data, &vec![0; output.ndim()])?;
    if let Some(mask) = mask {
        output.put_mask("mask0", &mask)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.outfile)?;
    if let Some(coords) = legacy_coordinates {
        patch_saved_coords_keyword(&request.outfile, coords)?;
    }
    Ok(ImageSubimageSummary {
        imagename: request.imagename.display().to_string(),
        outfile: request.outfile.display().to_string(),
        blc: selection.start.clone(),
        trc: selection
            .start
            .iter()
            .zip(&selection.shape)
            .map(|(start, shape)| start + shape - 1)
            .collect(),
        shape: selection.shape,
        units: image.units().to_string(),
    })
}

fn subimage_coordinates(
    coordinates: &CoordinateSystem,
    start: &[usize],
) -> Result<CoordinateSystem, ImageError> {
    let mut record = coordinates.to_record();
    let mut axis_offset = 0usize;
    for coordinate_index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(coordinate_index);
        let axis_count = coordinate.n_pixel_axes();
        let key = format!("coordinate{coordinate_index}");
        let Some(Value::Record(coordinate_record)) = record.get_mut(&key) else {
            axis_offset += axis_count;
            continue;
        };
        shift_coordinate_reference_pixel(
            coordinate_record,
            &start[axis_offset..axis_offset + axis_count],
        );
        axis_offset += axis_count;
    }
    CoordinateSystem::from_record(&record).map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "failed to build subimage coordinate system: {error}"
        ))
    })
}

fn shift_coordinate_reference_pixel(record: &mut casa_types::RecordValue, start: &[usize]) {
    let Some(Value::Array(array)) = record.get_mut("crpix") else {
        return;
    };
    let values = match array {
        ArrayValue::Float64(values) => values
            .iter()
            .enumerate()
            .map(|(axis, value)| *value - start.get(axis).copied().unwrap_or(0) as f64)
            .collect::<Vec<_>>(),
        ArrayValue::Float32(values) => values
            .iter()
            .enumerate()
            .map(|(axis, value)| *value as f64 - start.get(axis).copied().unwrap_or(0) as f64)
            .collect::<Vec<_>>(),
        _ => return,
    };
    *array = ArrayValue::from_f64_vec(values);
}

fn legacy_subimage_coordinates<T: ImagePixel>(
    image: &PagedImage<T>,
    start: &[usize],
) -> Option<casa_types::RecordValue> {
    let Some(Value::Record(mut coords)) = image.table().keywords().get("coords").cloned() else {
        return None;
    };
    let mut axis_offset = 0usize;
    for coordinate_index in 0..image.coordinates().n_coordinates() {
        let coordinate = image.coordinates().coordinate(coordinate_index);
        let axis_count = coordinate.n_pixel_axes();
        let key = legacy_coordinate_key(coordinate.coordinate_type(), coordinate_index);
        if let Some(Value::Record(coordinate_record)) = coords.get_mut(&key) {
            shift_coordinate_reference_pixel(
                coordinate_record,
                &start[axis_offset..axis_offset + axis_count],
            );
            shift_legacy_spectral_wcs_reference_pixel(
                coordinate_record,
                &start[axis_offset..axis_offset + axis_count],
            );
        }
        axis_offset += axis_count;
    }
    Some(coords)
}

fn legacy_coordinate_key(coordinate_type: CoordinateType, coordinate_index: usize) -> String {
    let prefix = match coordinate_type {
        CoordinateType::Direction => "direction",
        CoordinateType::Spectral => "spectral",
        CoordinateType::Stokes => "stokes",
        CoordinateType::Linear => "linear",
        CoordinateType::Tabular => "tabular",
    };
    format!("{prefix}{coordinate_index}")
}

fn shift_legacy_spectral_wcs_reference_pixel(
    record: &mut casa_types::RecordValue,
    start: &[usize],
) {
    let Some(Value::Record(wcs)) = record.get_mut("wcs") else {
        return;
    };
    let Some(Value::Scalar(ScalarValue::Float64(crpix))) = wcs.get_mut("crpix") else {
        return;
    };
    *crpix -= start.first().copied().unwrap_or(0) as f64;
}

fn patch_saved_coords_keyword(
    path: &Path,
    coords: casa_types::RecordValue,
) -> Result<(), ImageError> {
    let mut table = Table::open(TableOptions::new(path))?;
    table.keywords_mut().upsert("coords", Value::Record(coords));
    table.save_metadata_only(TableOptions::new(path))?;
    Ok(())
}

fn immath_typed<T>(
    lhs: &PagedImage<T>,
    rhs: &PagedImage<T>,
    request: &ImmathRequest,
    op: TutorialMathOp,
) -> Result<ImageMathSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    if lhs.shape() != rhs.shape() {
        return Err(ImageError::ShapeMismatch {
            expected: lhs.shape().to_vec(),
            got: rhs.shape().to_vec(),
        });
    }
    if request.outfile.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.outfile)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "immath output already exists: {}",
                request.outfile.display()
            )));
        }
    }
    let shape = lhs.shape().to_vec();
    let origin = vec![0; lhs.ndim()];
    let left = lhs.get_slice(&origin, &shape)?;
    let right = rhs.get_slice(&origin, &shape)?;
    let lhs_mask = lhs.get_mask_slice(&origin, &shape, &vec![1; lhs.ndim()])?;
    let rhs_mask = rhs.get_mask_slice(&origin, &shape, &vec![1; rhs.ndim()])?;
    let mut data = ArrayD::from_elem(IxDyn(&shape), T::from(0.0));
    let mut valid = ndarray::ArrayD::from_elem(IxDyn(&shape), true);
    Zip::from(&mut data)
        .and(&mut valid)
        .and(&left)
        .and(&right)
        .for_each(|out, valid, lhs, rhs| {
            let lhs = (*lhs).into();
            let rhs = (*rhs).into();
            let value = match op {
                TutorialMathOp::Multiply => lhs * rhs,
                TutorialMathOp::Divide => {
                    if rhs == 0.0 {
                        f64::NAN
                    } else {
                        lhs / rhs
                    }
                }
                TutorialMathOp::Scale(_) => unreachable!("scale expressions use unary immath"),
            };
            *valid = value.is_finite();
            *out = T::from(value as f32);
        });
    if let Some(mask) = lhs_mask {
        Zip::from(&mut valid).and(&mask).for_each(|valid, mask| {
            *valid = *valid && *mask;
        });
    }
    if let Some(mask) = rhs_mask {
        Zip::from(&mut valid).and(&mask).for_each(|valid, mask| {
            *valid = *valid && *mask;
        });
    }
    let mut output = TempImage::<T>::new(shape.clone(), lhs.coordinates().clone())?;
    let legacy_coordinates = lhs.table().keywords().get("coords").cloned();
    output.set_units(lhs.units())?;
    output.set_image_info(&lhs.image_info()?)?;
    output.set_misc_info(lhs.misc_info())?;
    output.put_slice(&data, &vec![0; output.ndim()])?;
    if valid.iter().any(|pixel| !*pixel) {
        output.put_mask("mask0", &valid)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.outfile)?;
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&request.outfile, coords)?;
    }
    Ok(ImageMathSummary {
        imagename: request
            .imagename
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        outfile: request.outfile.display().to_string(),
        expr: request.expr.clone(),
        shape,
        units: lhs.units().to_string(),
        valid_pixels: valid.iter().filter(|pixel| **pixel).count(),
    })
}

fn impbcor_typed<T>(
    image: &PagedImage<T>,
    pb: &PagedImage<T>,
    request: &ImpbcorRequest,
) -> Result<PbcorSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    if image.shape() != pb.shape() {
        return Err(ImageError::ShapeMismatch {
            expected: image.shape().to_vec(),
            got: pb.shape().to_vec(),
        });
    }
    if request.outfile.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.outfile)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "impbcor output already exists: {}",
                request.outfile.display()
            )));
        }
    }
    let divide = match request.mode.as_str() {
        "divide" => true,
        "multiply" => false,
        other => {
            return Err(ImageError::InvalidMetadata(format!(
                "unsupported impbcor mode {other:?}; expected divide or multiply"
            )));
        }
    };
    let shape = image.shape().to_vec();
    let origin = vec![0; image.ndim()];
    let left = image.get_slice(&origin, &shape)?;
    let right = pb.get_slice(&origin, &shape)?;
    let image_mask = image.get_mask_slice(&origin, &shape, &vec![1; image.ndim()])?;
    let pb_mask = pb.get_mask_slice(&origin, &shape, &vec![1; pb.ndim()])?;
    let mut data = ArrayD::from_elem(IxDyn(&shape), T::from(0.0));
    let mut valid = ndarray::ArrayD::from_elem(IxDyn(&shape), true);
    Zip::from(&mut data)
        .and(&mut valid)
        .and(&left)
        .and(&right)
        .for_each(|out, valid, image, pb| {
            let image = (*image).into();
            let pb = (*pb).into();
            let value = if divide {
                if pb == 0.0 { f64::NAN } else { image / pb }
            } else {
                image * pb
            };
            *valid = (request.cutoff < 0.0 || pb >= request.cutoff) && value.is_finite();
            *out = T::from(value as f32);
        });
    if let Some(mask) = image_mask {
        Zip::from(&mut valid).and(&mask).for_each(|valid, mask| {
            *valid = *valid && *mask;
        });
    }
    if let Some(mask) = pb_mask {
        Zip::from(&mut valid).and(&mask).for_each(|valid, mask| {
            *valid = *valid && *mask;
        });
    }
    let mut output = TempImage::<T>::new(shape.clone(), image.coordinates().clone())?;
    let legacy_coordinates = image.table().keywords().get("coords").cloned();
    output.set_units(image.units())?;
    output.set_image_info(&image.image_info()?)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&data, &vec![0; output.ndim()])?;
    if valid.iter().any(|pixel| !*pixel) {
        output.put_mask("mask0", &valid)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.outfile)?;
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&request.outfile, coords)?;
    }
    Ok(PbcorSummary {
        imagename: request.imagename.display().to_string(),
        pbimage: request.pbimage.display().to_string(),
        outfile: request.outfile.display().to_string(),
        mode: request.mode.clone(),
        cutoff: request.cutoff,
        shape,
        units: image.units().to_string(),
        valid_pixels: valid.iter().filter(|pixel| **pixel).count(),
    })
}

fn immath_unary_typed<T>(
    image: &PagedImage<T>,
    request: &ImmathRequest,
    factor: f64,
) -> Result<ImageMathSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    if request.outfile.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.outfile)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "immath output already exists: {}",
                request.outfile.display()
            )));
        }
    }
    let shape = image.shape().to_vec();
    let origin = vec![0; image.ndim()];
    let input = image.get_slice(&origin, &shape)?;
    let input_mask = image.get_mask_slice(&origin, &shape, &vec![1; image.ndim()])?;
    let mut data = ArrayD::from_elem(IxDyn(&shape), T::from(0.0));
    let mut valid = ndarray::ArrayD::from_elem(IxDyn(&shape), true);
    Zip::from(&mut data)
        .and(&mut valid)
        .and(&input)
        .for_each(|out, valid, input| {
            let value = (*input).into() * factor;
            *valid = value.is_finite();
            *out = T::from(value as f32);
        });
    if let Some(mask) = input_mask {
        Zip::from(&mut valid).and(&mask).for_each(|valid, mask| {
            *valid = *valid && *mask;
        });
    }
    let mut output = TempImage::<T>::new(shape.clone(), image.coordinates().clone())?;
    let legacy_coordinates = image.table().keywords().get("coords").cloned();
    output.set_units(image.units())?;
    output.set_image_info(&image.image_info()?)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&data, &vec![0; output.ndim()])?;
    if valid.iter().any(|pixel| !*pixel) {
        output.put_mask("mask0", &valid)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.outfile)?;
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&request.outfile, coords)?;
    }
    Ok(ImageMathSummary {
        imagename: request
            .imagename
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        outfile: request.outfile.display().to_string(),
        expr: request.expr.clone(),
        shape,
        units: image.units().to_string(),
        valid_pixels: valid.iter().filter(|pixel| **pixel).count(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TutorialMathOp {
    Multiply,
    Divide,
    Scale(f64),
}

fn parse_tutorial_immath_expr(expr: &str) -> Result<TutorialMathOp, ImageError> {
    let normalized = expr
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase();
    match normalized.as_str() {
        "IM0*IM1" => Ok(TutorialMathOp::Multiply),
        "IM0/IM1" => Ok(TutorialMathOp::Divide),
        _ if normalized.contains("IM0") && !normalized.contains("IM1") => {
            let factor_expr = normalized.replace("IM0", "1");
            parse_scalar_factor_expr(&factor_expr).map(TutorialMathOp::Scale)
        }
        _ => Err(ImageError::InvalidMetadata(format!(
            "unsupported tutorial immath expression {expr:?}; supported forms are IM0 * IM1, IM0 / IM1, and scalar multiples of IM0"
        ))),
    }
}

fn parse_scalar_factor_expr(expr: &str) -> Result<f64, ImageError> {
    let mut parser = ScalarFactorParser { text: expr, pos: 0 };
    let value = parser.parse_expr()?;
    if parser.pos != parser.text.len() {
        return Err(ImageError::InvalidMetadata(format!(
            "unsupported scalar immath expression {expr:?}"
        )));
    }
    Ok(value)
}

struct ScalarFactorParser<'a> {
    text: &'a str,
    pos: usize,
}

impl ScalarFactorParser<'_> {
    fn parse_expr(&mut self) -> Result<f64, ImageError> {
        let mut value = self.parse_power()?;
        while let Some(op) = self.peek_char().filter(|ch| *ch == '*' || *ch == '/') {
            self.pos += 1;
            let rhs = self.parse_power()?;
            if op == '*' {
                value *= rhs;
            } else {
                value /= rhs;
            }
        }
        Ok(value)
    }

    fn parse_power(&mut self) -> Result<f64, ImageError> {
        let mut value = self.parse_primary()?;
        if self.peek_char() == Some('^') {
            self.pos += 1;
            value = value.powf(self.parse_power()?);
        }
        Ok(value)
    }

    fn parse_primary(&mut self) -> Result<f64, ImageError> {
        if self.peek_char() == Some('(') {
            self.pos += 1;
            let value = self.parse_expr()?;
            if self.peek_char() != Some(')') {
                return Err(ImageError::InvalidMetadata(
                    "unterminated scalar immath expression".to_string(),
                ));
            }
            self.pos += 1;
            return Ok(value);
        }
        self.parse_number()
    }

    fn parse_number(&mut self) -> Result<f64, ImageError> {
        let start = self.pos;
        if self.peek_char().is_some_and(|ch| ch == '+' || ch == '-') {
            self.pos += 1;
        }
        while self
            .peek_char()
            .is_some_and(|ch| ch.is_ascii_digit() || ch == '.')
        {
            self.pos += 1;
        }
        if self.peek_char().is_some_and(|ch| ch == 'E') {
            self.pos += 1;
            if self.peek_char().is_some_and(|ch| ch == '+' || ch == '-') {
                self.pos += 1;
            }
            while self.peek_char().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        self.text[start..self.pos].parse::<f64>().map_err(|error| {
            ImageError::InvalidMetadata(format!(
                "parse scalar immath number {:?}: {error}",
                &self.text[start..self.pos]
            ))
        })
    }

    fn peek_char(&self) -> Option<char> {
        self.text[self.pos..].chars().next()
    }
}

fn imregrid_typed<T>(
    input: &PagedImage<T>,
    template: &PagedImage<T>,
    request: &ImregridRequest,
) -> Result<ImageRegridSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    if request.output.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.output)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "imregrid output already exists: {}",
                request.output.display()
            )));
        }
    }
    let method = request.interpolation.to_ascii_lowercase();
    if method != "linear" && method != "nearest" {
        return Err(ImageError::InvalidMetadata(format!(
            "unsupported imregrid interpolation {:?}; expected linear or nearest",
            request.interpolation
        )));
    }
    if input.ndim() != template.ndim() {
        return Err(ImageError::ShapeMismatch {
            expected: input.shape().to_vec(),
            got: template.shape().to_vec(),
        });
    }
    let output_shape = template.shape().to_vec();
    let origin = vec![0; input.ndim()];
    let input_data = input.get_slice(&origin, input.shape())?;
    let input_mask = input.get_mask_slice(&origin, input.shape(), &vec![1; input.ndim()])?;
    if method == "linear"
        && let Some((output_data, output_mask)) =
            regrid_four_axis_fast(input, template, &input_data, input_mask.as_ref())?
    {
        return save_regridded_image(
            input,
            template,
            request,
            output_shape,
            output_data,
            output_mask,
        );
    }
    let mut output_data = ArrayD::<T>::from_elem(IxDyn(&output_shape).f(), T::from(0.0));
    let mut output_mask = ArrayD::<bool>::from_elem(IxDyn(&output_shape).f(), false);
    for_each_index(&output_shape, |out_index| -> Result<(), ImageError> {
        let output_pixel = out_index
            .iter()
            .map(|value| *value as f64)
            .collect::<Vec<_>>();
        let world = template
            .coordinates()
            .to_world(&output_pixel)
            .map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "template pixel/world conversion failed: {error}"
                ))
            })?;
        let input_pixel = input.coordinates().to_pixel(&world).map_err(|error| {
            ImageError::InvalidMetadata(format!("input world/pixel conversion failed: {error}"))
        })?;
        let sampled = if method == "nearest" {
            sample_nearest(&input_data, input_mask.as_ref(), &input_pixel)
                .map(|value| (value, true))
        } else {
            sample_linear_weighted(&input_data, input_mask.as_ref(), &input_pixel).map(
                |(sum, weight_sum)| {
                    (
                        sum / weight_sum,
                        weight_sum >= LINEAR_REGRID_VALID_WEIGHT_MIN,
                    )
                },
            )
        };
        if let Some((value, valid)) = sampled {
            output_data[IxDyn(out_index)] = T::from(value as f32);
            output_mask[IxDyn(out_index)] = valid;
        }
        Ok(())
    })?;
    save_regridded_image(
        input,
        template,
        request,
        output_shape,
        output_data,
        output_mask,
    )
}

fn save_regridded_image<T>(
    input: &PagedImage<T>,
    template: &PagedImage<T>,
    request: &ImregridRequest,
    output_shape: Vec<usize>,
    output_data: ArrayD<T>,
    output_mask: ArrayD<bool>,
) -> Result<ImageRegridSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    let mut output = TempImage::<T>::new(output_shape.clone(), template.coordinates().clone())?;
    let legacy_coordinates = template.table().keywords().get("coords").cloned();
    output.set_units(input.units())?;
    output.set_image_info(&input.image_info()?)?;
    output.set_misc_info(input.misc_info())?;
    output.put_slice(&output_data, &vec![0; output.ndim()])?;
    if output_mask.iter().any(|valid| !*valid) {
        output.put_mask("mask0", &output_mask)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.output)?;
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&request.output, coords)?;
    }
    Ok(ImageRegridSummary {
        imagename: request.imagename.display().to_string(),
        template: request.template.display().to_string(),
        output: request.output.display().to_string(),
        shape: output_shape,
        valid_pixels: output_mask.iter().filter(|value| **value).count(),
    })
}

fn feather_typed(
    high: &PagedImage<f32>,
    low: &PagedImage<f32>,
    request: &FeatherRequest,
) -> Result<FeatherSummary, ImageError> {
    if request.imagename.exists() {
        if request.overwrite {
            fs::remove_dir_all(&request.imagename)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "feather output already exists: {}",
                request.imagename.display()
            )));
        }
    }
    let high_shape = high.shape().to_vec();
    let (low_data, low_mask) = regrid_image_to_template_f32(low, high)?;
    let origin = vec![0; high.ndim()];
    let high_data = high.get_slice(&origin, &high_shape)?;
    let high_mask = high.get_mask_slice(&origin, &high_shape, &vec![1; high.ndim()])?;
    let high_beam = high.image_info()?.beam_set.single_beam().ok_or_else(|| {
        ImageError::InvalidMetadata(
            "high-resolution image requires a single restoring beam".to_string(),
        )
    })?;
    let low_beam = low.image_info()?.beam_set.single_beam().ok_or_else(|| {
        ImageError::InvalidMetadata(
            "low-resolution image requires a single restoring beam".to_string(),
        )
    })?;
    if high_shape.len() < 2 {
        return Err(ImageError::InvalidMetadata(
            "feather requires at least two direction axes".to_string(),
        ));
    }
    let nx = high_shape[0];
    let ny = high_shape[1];
    let trailing = if high_shape.len() > 2 {
        high_shape[2..].iter().product()
    } else {
        1
    };
    let crossover = feather_crossover_weight(nx, ny, low_beam, high.coordinates())?;
    let low_scale = request.sdfactor * (high_beam.area() / low_beam.area()) as f32;
    let mut output_data = ArrayD::<f32>::zeros(IxDyn(&high_shape).f());
    let mut output_mask = ArrayD::<bool>::from_elem(IxDyn(&high_shape).f(), true);
    for plane in 0..trailing {
        let trailing_index = unravel_index(plane, &high_shape[2..]);
        let high_plane = extract_plane2(&high_data, &trailing_index);
        let low_plane = extract_plane2(&low_data, &trailing_index);
        let high_fft = centered_fft2(&high_plane.mapv(|value| Complex32::new(value, 0.0)));
        let low_fft = centered_fft2(&low_plane.mapv(|value| Complex32::new(value, 0.0)));
        let mut combined = Array2::<Complex32>::zeros((nx, ny));
        for x in 0..nx {
            for y in 0..ny {
                let high_weight = crossover[(x, y)];
                combined[(x, y)] = high_fft[(x, y)] * high_weight + low_fft[(x, y)] * low_scale;
            }
        }
        let image_plane = centered_ifft2(&combined).mapv(|value| value.re);
        put_plane2(&mut output_data, &trailing_index, &image_plane);
    }
    if let Some(mask) = high_mask {
        Zip::from(&mut output_mask)
            .and(&mask)
            .for_each(|out, valid| {
                *out = *out && *valid;
            });
    }
    Zip::from(&mut output_mask)
        .and(&low_mask)
        .for_each(|out, valid| {
            *out = *out && *valid;
        });
    let mut output = TempImage::<f32>::new(high_shape.clone(), high.coordinates().clone())?;
    let legacy_coordinates = high.table().keywords().get("coords").cloned();
    output.set_units(high.units())?;
    output.set_image_info(&high.image_info()?)?;
    output.set_misc_info(high.misc_info())?;
    output.put_slice(&output_data, &vec![0; output.ndim()])?;
    if output_mask.iter().any(|valid| !*valid) {
        output.put_mask("mask0", &output_mask)?;
        output.set_default_mask("mask0")?;
    }
    output.save_as(&request.imagename)?;
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&request.imagename, coords)?;
    }
    Ok(FeatherSummary {
        highres: request.highres.display().to_string(),
        lowres: request.lowres.display().to_string(),
        imagename: request.imagename.display().to_string(),
        shape: high_shape,
        sdfactor: request.sdfactor,
        valid_pixels: output_mask.iter().filter(|value| **value).count(),
    })
}

fn regrid_image_to_template_f32(
    input: &PagedImage<f32>,
    template: &PagedImage<f32>,
) -> Result<(ArrayD<f32>, ArrayD<bool>), ImageError> {
    if input.ndim() != template.ndim() {
        return Err(ImageError::ShapeMismatch {
            expected: input.shape().to_vec(),
            got: template.shape().to_vec(),
        });
    }
    let output_shape = template.shape().to_vec();
    let origin = vec![0; input.ndim()];
    let input_data = input.get_slice(&origin, input.shape())?;
    let input_mask = input.get_mask_slice(&origin, input.shape(), &vec![1; input.ndim()])?;
    if let Some(result) = regrid_four_axis_fast(input, template, &input_data, input_mask.as_ref())?
    {
        return Ok(result);
    }
    let mut output_data = ArrayD::<f32>::zeros(IxDyn(&output_shape).f());
    let mut output_mask = ArrayD::<bool>::from_elem(IxDyn(&output_shape).f(), false);
    for_each_index(&output_shape, |out_index| -> Result<(), ImageError> {
        let output_pixel = out_index
            .iter()
            .map(|value| *value as f64)
            .collect::<Vec<_>>();
        let world = template
            .coordinates()
            .to_world(&output_pixel)
            .map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "template pixel/world conversion failed: {error}"
                ))
            })?;
        let input_pixel = input.coordinates().to_pixel(&world).map_err(|error| {
            ImageError::InvalidMetadata(format!("input world/pixel conversion failed: {error}"))
        })?;
        if let Some((sum, weight_sum)) =
            sample_linear_weighted(&input_data, input_mask.as_ref(), &input_pixel)
        {
            output_data[IxDyn(out_index)] = (sum / weight_sum) as f32;
            output_mask[IxDyn(out_index)] = weight_sum >= LINEAR_REGRID_VALID_WEIGHT_MIN;
        }
        Ok(())
    })?;
    Ok((output_data, output_mask))
}

type AxisLinearMap = Option<(usize, usize, f64)>;
type RegridArrayPair<T> = (ArrayD<T>, ArrayD<bool>);
type RegridFastResult<T> = Result<Option<RegridArrayPair<T>>, ImageError>;

fn regrid_four_axis_fast<T>(
    input: &PagedImage<T>,
    template: &PagedImage<T>,
    input_data: &ArrayD<T>,
    input_mask: Option<&ArrayD<bool>>,
) -> RegridFastResult<T>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    if input.ndim() != 4 || template.ndim() != 4 {
        return Ok(None);
    }
    let output_shape = template.shape().to_vec();
    let mut x_map = Vec::with_capacity(output_shape[0]);
    for x in 0..output_shape[0] {
        let world = template
            .coordinates()
            .to_world(&[x as f64, 0.0, 0.0, 0.0])
            .map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "template pixel/world conversion failed: {error}"
                ))
            })?;
        let input_pixel = input.coordinates().to_pixel(&world).map_err(|error| {
            ImageError::InvalidMetadata(format!("input world/pixel conversion failed: {error}"))
        })?;
        x_map.push(axis_linear_map(input_pixel[0], input.shape()[0]));
    }
    let mut y_map = Vec::with_capacity(output_shape[1]);
    for y in 0..output_shape[1] {
        let world = template
            .coordinates()
            .to_world(&[0.0, y as f64, 0.0, 0.0])
            .map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "template pixel/world conversion failed: {error}"
                ))
            })?;
        let input_pixel = input.coordinates().to_pixel(&world).map_err(|error| {
            ImageError::InvalidMetadata(format!("input world/pixel conversion failed: {error}"))
        })?;
        y_map.push(axis_linear_map(input_pixel[1], input.shape()[1]));
    }
    let mut chan_map = Vec::with_capacity(output_shape[3]);
    for chan in 0..output_shape[3] {
        let world = template
            .coordinates()
            .to_world(&[0.0, 0.0, 0.0, chan as f64])
            .map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "template pixel/world conversion failed: {error}"
                ))
            })?;
        let input_pixel = input.coordinates().to_pixel(&world).map_err(|error| {
            ImageError::InvalidMetadata(format!("input world/pixel conversion failed: {error}"))
        })?;
        chan_map.push(axis_linear_map(input_pixel[3], input.shape()[3]));
    }
    let mut output_data = ArrayD::<T>::from_elem(IxDyn(&output_shape).f(), T::from(0.0));
    let mut output_mask = ArrayD::<bool>::from_elem(IxDyn(&output_shape).f(), false);
    for x in 0..output_shape[0] {
        let Some(x_axis) = x_map[x] else { continue };
        for y in 0..output_shape[1] {
            let Some(y_axis) = y_map[y] else { continue };
            for stokes in 0..output_shape[2] {
                if stokes >= input.shape()[2] {
                    continue;
                }
                for chan in 0..output_shape[3] {
                    let Some(chan_axis) = chan_map[chan] else {
                        continue;
                    };
                    if let Some((sum, weight_sum)) = sample_four_axis_linear_weighted(
                        input_data, input_mask, x_axis, y_axis, stokes, chan_axis,
                    ) {
                        let value = sum / weight_sum;
                        output_data[IxDyn(&[x, y, stokes, chan])] = T::from(value as f32);
                        output_mask[IxDyn(&[x, y, stokes, chan])] =
                            weight_sum >= LINEAR_REGRID_VALID_WEIGHT_MIN;
                    }
                }
            }
        }
    }
    Ok(Some((output_data, output_mask)))
}

fn axis_linear_map(coordinate: f64, len: usize) -> AxisLinearMap {
    if !coordinate.is_finite() {
        return None;
    }
    if len == 1 {
        return (coordinate.abs() <= 0.5).then_some((0, 0, 0.0));
    }
    if coordinate < -0.5 || coordinate > (len as f64 - 0.5) {
        return None;
    }
    let coordinate = coordinate.clamp(0.0, (len - 1) as f64);
    let lo = coordinate.floor() as usize;
    let hi = (lo + 1).min(len - 1);
    Some((lo, hi, coordinate - lo as f64))
}

fn sample_four_axis_linear_weighted<T>(
    data: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    x_axis: (usize, usize, f64),
    y_axis: (usize, usize, f64),
    stokes: usize,
    chan_axis: (usize, usize, f64),
) -> Option<(f64, f64)>
where
    T: Into<f64> + Copy,
{
    let axes = [
        [(x_axis.0, 1.0 - x_axis.2), (x_axis.1, x_axis.2)],
        [(y_axis.0, 1.0 - y_axis.2), (y_axis.1, y_axis.2)],
        [(stokes, 1.0), (stokes, 0.0)],
        [(chan_axis.0, 1.0 - chan_axis.2), (chan_axis.1, chan_axis.2)],
    ];
    let mut index = [0usize; 4];
    let mut sum = 0.0;
    let mut weight_sum = 0.0;
    let mut state = LinearSampleState {
        index: &mut index,
        sum: &mut sum,
        weight_sum: &mut weight_sum,
    };
    sample_four_axis_recursive(data, mask, &axes, 0, 1.0, &mut state);
    (weight_sum > 0.0).then_some((sum, weight_sum))
}

struct LinearSampleState<'a> {
    index: &'a mut [usize],
    sum: &'a mut f64,
    weight_sum: &'a mut f64,
}

fn sample_four_axis_recursive<T>(
    data: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axes: &[[(usize, f64); 2]; 4],
    axis: usize,
    weight: f64,
    state: &mut LinearSampleState<'_>,
) where
    T: Into<f64> + Copy,
{
    if axis == 4 {
        if let Some(mask) = mask
            && !mask[IxDyn(state.index)]
        {
            return;
        }
        let value = data[IxDyn(state.index)].into();
        if value.is_finite() {
            *state.sum += value * weight;
            *state.weight_sum += weight;
        }
        return;
    }
    for (coordinate, axis_weight) in axes[axis] {
        if axis_weight == 0.0 {
            continue;
        }
        state.index[axis] = coordinate;
        sample_four_axis_recursive(data, mask, axes, axis + 1, weight * axis_weight, state);
    }
}

fn sample_nearest<T>(data: &ArrayD<T>, mask: Option<&ArrayD<bool>>, pixel: &[f64]) -> Option<f64>
where
    T: Into<f64> + Copy,
{
    let mut index = Vec::with_capacity(pixel.len());
    for (axis, coordinate) in pixel.iter().copied().enumerate() {
        let rounded = coordinate.round();
        if !(rounded.is_finite() && rounded >= 0.0 && rounded < data.shape()[axis] as f64) {
            return None;
        }
        index.push(rounded as usize);
    }
    if let Some(mask) = mask
        && !mask[IxDyn(&index)]
    {
        return None;
    }
    Some(data[IxDyn(&index)].into())
}

fn sample_linear_weighted<T>(
    data: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    pixel: &[f64],
) -> Option<(f64, f64)>
where
    T: Into<f64> + Copy,
{
    let mut axes = Vec::<[(usize, f64); 2]>::with_capacity(pixel.len());
    for (axis, coordinate) in pixel.iter().copied().enumerate() {
        if !coordinate.is_finite() {
            return None;
        }
        let len = data.shape()[axis];
        if len == 1 {
            if coordinate.abs() > 0.5 {
                return None;
            }
            axes.push([(0, 1.0), (0, 0.0)]);
            continue;
        }
        if coordinate < -0.5 || coordinate > (len as f64 - 0.5) {
            return None;
        }
        let coordinate = coordinate.clamp(0.0, (len - 1) as f64);
        let lo = coordinate.floor() as usize;
        let hi = (lo + 1).min(len - 1);
        let frac = coordinate - lo as f64;
        axes.push([(lo, 1.0 - frac), (hi, frac)]);
    }
    let mut index = vec![0usize; pixel.len()];
    let mut sum = 0.0;
    let mut weight_sum = 0.0;
    let mut state = LinearSampleState {
        index: &mut index,
        sum: &mut sum,
        weight_sum: &mut weight_sum,
    };
    sample_linear_recursive(data, mask, &axes, 0, 1.0, &mut state);
    (weight_sum > 0.0).then_some((sum, weight_sum))
}

fn sample_linear_recursive<T>(
    data: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axes: &[[(usize, f64); 2]],
    axis: usize,
    weight: f64,
    state: &mut LinearSampleState<'_>,
) where
    T: Into<f64> + Copy,
{
    if axis == axes.len() {
        if let Some(mask) = mask
            && !mask[IxDyn(state.index)]
        {
            return;
        }
        let value = data[IxDyn(state.index)].into();
        if value.is_finite() {
            *state.sum += value * weight;
            *state.weight_sum += weight;
        }
        return;
    }
    for (coordinate, axis_weight) in axes[axis] {
        if axis_weight == 0.0 {
            continue;
        }
        state.index[axis] = coordinate;
        sample_linear_recursive(data, mask, axes, axis + 1, weight * axis_weight, state);
    }
}

fn for_each_index<F>(shape: &[usize], mut f: F) -> Result<(), ImageError>
where
    F: FnMut(&[usize]) -> Result<(), ImageError>,
{
    if shape.is_empty() {
        return f(&[]);
    }
    let mut index = vec![0usize; shape.len()];
    loop {
        f(&index)?;
        let mut axis = shape.len();
        loop {
            if axis == 0 {
                return Ok(());
            }
            axis -= 1;
            index[axis] += 1;
            if index[axis] < shape[axis] {
                break;
            }
            index[axis] = 0;
        }
    }
}

fn feather_crossover_weight(
    nx: usize,
    ny: usize,
    low_beam: GaussianBeam,
    coordinates: &CoordinateSystem,
) -> Result<Array2<Complex32>, ImageError> {
    let mut psf = Array2::<Complex32>::zeros((nx, ny));
    let (x_inc, y_inc, x_ref, y_ref) = direction_axis_geometry(coordinates)?;
    let major_width = low_beam.major.abs().max(1.0e-12);
    let minor_width = low_beam.minor.abs().max(1.0e-12);
    let major_scale = 4.0 * std::f64::consts::LN_2 / major_width.powi(2);
    let minor_scale = 4.0 * std::f64::consts::LN_2 / minor_width.powi(2);
    let pa = low_beam.position_angle + std::f64::consts::FRAC_PI_2;
    let cos_pa = pa.cos();
    let sin_pa = pa.sin();
    for x in 0..nx {
        for y in 0..ny {
            let dx = (x as f64 - x_ref) * x_inc.abs();
            let dy = (y as f64 - y_ref) * y_inc.abs();
            let major = dx * cos_pa + dy * sin_pa;
            let minor = -dx * sin_pa + dy * cos_pa;
            let radius = major_scale * major.powi(2) + minor_scale * minor.powi(2);
            let value = if radius < 20.0 {
                (-radius).exp() as f32
            } else {
                0.0
            };
            psf[(x, y)] = Complex32::new(value, 0.0);
        }
    }
    let transformed = centered_fft2(&psf);
    let peak = transformed
        .iter()
        .map(|value| value.norm())
        .fold(0.0_f32, f32::max)
        .max(1.0e-20);
    Ok(transformed.mapv(|value| Complex32::new(1.0, 0.0) - value / peak))
}

fn direction_axis_geometry(
    coordinates: &CoordinateSystem,
) -> Result<(f64, f64, f64, f64), ImageError> {
    let direction_index = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| ImageError::InvalidMetadata("missing direction coordinate".to_string()))?;
    let coordinate = coordinates.coordinate(direction_index);
    let increments = coordinate.increment();
    let reference_pixel = coordinate.reference_pixel();
    if increments.len() < 2 {
        return Err(ImageError::InvalidMetadata(
            "direction coordinate has fewer than two axes".to_string(),
        ));
    }
    if reference_pixel.len() < 2 {
        return Err(ImageError::InvalidMetadata(
            "direction coordinate has fewer than two reference pixels".to_string(),
        ));
    }
    Ok((
        increments[0],
        increments[1],
        reference_pixel[0],
        reference_pixel[1],
    ))
}

fn extract_plane2(data: &ArrayD<f32>, trailing_index: &[usize]) -> Array2<f32> {
    let nx = data.shape()[0];
    let ny = data.shape()[1];
    let mut plane = Array2::<f32>::zeros((nx, ny));
    for x in 0..nx {
        for y in 0..ny {
            let mut index = vec![x, y];
            index.extend_from_slice(trailing_index);
            plane[(x, y)] = data[IxDyn(&index)];
        }
    }
    plane
}

fn put_plane2(data: &mut ArrayD<f32>, trailing_index: &[usize], plane: &Array2<f32>) {
    let nx = data.shape()[0];
    let ny = data.shape()[1];
    for x in 0..nx {
        for y in 0..ny {
            let mut index = vec![x, y];
            index.extend_from_slice(trailing_index);
            data[IxDyn(&index)] = plane[(x, y)];
        }
    }
}

fn unravel_index(mut value: usize, shape: &[usize]) -> Vec<usize> {
    if shape.is_empty() {
        return Vec::new();
    }
    let mut out = vec![0usize; shape.len()];
    for axis in (0..shape.len()).rev() {
        out[axis] = value % shape[axis];
        value /= shape[axis];
    }
    out
}

fn centered_fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    transform_axis(&mut shifted, Axis(0), false);
    transform_axis(&mut shifted, Axis(1), false);
    fftshift2(&shifted)
}

fn centered_ifft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    transform_axis(&mut shifted, Axis(0), true);
    transform_axis(&mut shifted, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f32;
    shifted.mapv_inplace(|value| value * scale);
    fftshift2(&shifted)
}

fn transform_axis(data: &mut Array2<Complex32>, axis: Axis, inverse: bool) {
    let len = data.len_of(axis);
    let mut planner = FftPlanner::<f32>::new();
    let fft = if inverse {
        planner.plan_fft_inverse(len)
    } else {
        planner.plan_fft_forward(len)
    };
    if axis.index() == 0 {
        for column_index in 0..data.shape()[1] {
            let mut lane = data.column(column_index).to_vec();
            fft.process(&mut lane);
            for (row_index, value) in lane.into_iter().enumerate() {
                data[(row_index, column_index)] = value;
            }
        }
    } else {
        for row_index in 0..data.shape()[0] {
            let mut lane = data.row(row_index).to_vec();
            fft.process(&mut lane);
            for (column_index, value) in lane.into_iter().enumerate() {
                data[(row_index, column_index)] = value;
            }
        }
    }
}

fn fftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, false)
}

fn ifftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, true)
}

fn shift2(input: &Array2<Complex32>, inverse: bool) -> Array2<Complex32> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let mut output = Array2::<Complex32>::zeros((nx, ny));
    let x_shift = if inverse { nx / 2 } else { nx.div_ceil(2) };
    let y_shift = if inverse { ny / 2 } else { ny.div_ceil(2) };
    for x in 0..nx {
        for y in 0..ny {
            let new_x = (x + x_shift) % nx;
            let new_y = (y + y_shift) % ny;
            output[(x, y)] = input[(new_x, new_y)];
        }
    }
    output
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
    let input = image.get_slice(&start, &shape)?;
    let mut output_shape = shape.clone();
    output_shape[0] = path.len();
    output_shape[1] = 1;
    if let Some((axis, indices)) = &channel_indices {
        output_shape[*axis] = indices.len();
    }
    let mut output_data = ArrayD::<f32>::zeros(IxDyn(&output_shape).f());
    let other_shape = output_shape[2..].to_vec();
    let offsets = perpendicular_offsets(x0, y0, x1, y1, request.width.max(1));
    let mut input_index = vec![0; input.ndim()];
    let mut output_index = vec![0; output_shape.len()];
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
                input_index[0] = sx as usize;
                input_index[1] = sy as usize;
                for (offset_axis, value) in sample_index.iter().copied().enumerate() {
                    input_index[offset_axis + 2] = value;
                }
                if let Some((axis, indices)) = &channel_indices {
                    let local = sample_index[*axis - 2];
                    input_index[*axis] = indices[local] - start[*axis];
                }
                sum += input[IxDyn(&input_index)].into();
                count += 1;
            }
            if count > 0 {
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
    let mut samples = Vec::new();
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
                samples.push((value, *coord));
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
                samples.push((value, *coord));
            }
        }
    }
    let npts = samples.len();
    let result = if npts == 0 {
        return (0.0, false);
    } else {
        match moment {
            -1 => samples.iter().map(|(value, _)| *value).sum::<f64>() / npts as f64,
            0 => samples.iter().map(|(value, _)| *value).sum::<f64>() * integrated_scale,
            1 | 2 => {
                let s0 = samples.iter().map(|(value, _)| *value).sum::<f64>();
                if s0 == 0.0 {
                    return (0.0, false);
                }
                let mean = samples
                    .iter()
                    .map(|(value, coord)| value * coord)
                    .sum::<f64>()
                    / s0;
                if moment == 1 {
                    mean
                } else {
                    let variance = samples
                        .iter()
                        .map(|(value, coord)| value * (coord - mean).powi(2))
                        .sum::<f64>()
                        / s0;
                    if variance < 0.0 {
                        return (0.0, false);
                    }
                    variance.sqrt()
                }
            }
            3 => {
                let mut values = samples.iter().map(|(value, _)| *value).collect::<Vec<_>>();
                values.sort_by(|left, right| left.total_cmp(right));
                if values.len() % 2 == 0 {
                    let upper = values.len() / 2;
                    (values[upper - 1] + values[upper]) / 2.0
                } else {
                    values[values.len() / 2]
                }
            }
            _ => return (0.0, false),
        }
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
        let mut samples = Vec::new();
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
            samples.push((value, *coord));
        }
        let npts = samples.len();
        let (result, valid) = if npts == 0 {
            (0.0, false)
        } else {
            match moment {
                -1 => (
                    samples.iter().map(|(value, _)| *value).sum::<f64>() / npts as f64,
                    true,
                ),
                0 => (
                    samples.iter().map(|(value, _)| *value).sum::<f64>() * integrated_scale,
                    true,
                ),
                1 | 2 => {
                    let s0 = samples.iter().map(|(value, _)| *value).sum::<f64>();
                    if s0 == 0.0 {
                        (0.0, false)
                    } else {
                        let mean = samples
                            .iter()
                            .map(|(value, coord)| value * coord)
                            .sum::<f64>()
                            / s0;
                        if moment == 1 {
                            (mean, true)
                        } else {
                            let variance = samples
                                .iter()
                                .map(|(value, coord)| value * (coord - mean).powi(2))
                                .sum::<f64>()
                                / s0;
                            if variance < 0.0 {
                                (0.0, false)
                            } else {
                                (variance.sqrt(), true)
                            }
                        }
                    }
                }
                3 => {
                    let mut values = samples.iter().map(|(value, _)| *value).collect::<Vec<_>>();
                    values.sort_by(|left, right| left.total_cmp(right));
                    if values.len() % 2 == 0 {
                        let upper = values.len() / 2;
                        ((values[upper - 1] + values[upper]) / 2.0, true)
                    } else {
                        (values[values.len() / 2], true)
                    }
                }
                _ => (0.0, false),
            }
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
    match moment {
        0 => {
            if input_units.is_empty() {
                "km/s".to_string()
            } else {
                format!("{input_units}.km/s")
            }
        }
        1 | 2 => "km/s".to_string(),
        _ => input_units.to_string(),
    }
}

fn export_fits_typed<T>(
    imagename: &Path,
    fitsimage: &Path,
    image: &PagedImage<T>,
    velocity: bool,
) -> Result<FitsExportSummary, ImageError>
where
    T: ImagePixel + fitsio::images::WriteImage + Copy + FitsBlank,
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
    if let Some(mask) = image.default_mask()? {
        Zip::from(&mut data).and(&mask).for_each(|pixel, valid| {
            if !*valid {
                *pixel = T::fits_blank();
            }
        });
    }
    if layout.axis_order != (0..image.shape().len()).collect::<Vec<_>>() {
        data = data.permuted_axes(layout.axis_order.clone()).to_owned();
    }
    let flat = data
        .as_slice_memory_order()
        .ok_or_else(|| ImageError::InvalidMetadata("image data is not contiguous".to_string()))?;
    hdu.write_image(&mut fits, flat)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let header = to_fits_header(&layout.coordinates, &layout.shape);
    let velocity_overrides = if velocity {
        velocity_fits_axis_overrides(&layout.coordinates)
    } else {
        BTreeMap::new()
    };
    for keyword in header.iter() {
        if keyword.name == "NAXIS"
            || keyword
                .name
                .strip_prefix("NAXIS")
                .is_some_and(|suffix| suffix.chars().all(|ch| ch.is_ascii_digit()))
        {
            continue;
        }
        if let Some(value) = velocity_overrides.get(&keyword.name) {
            write_fits_key(&hdu, &mut fits, &keyword.name, value)?;
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

trait FitsBlank {
    fn fits_blank() -> Self;
}

impl FitsBlank for f32 {
    fn fits_blank() -> Self {
        f32::NAN
    }
}

impl FitsBlank for f64 {
    fn fits_blank() -> Self {
        f64::NAN
    }
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

fn velocity_fits_axis_overrides(coordinates: &CoordinateSystem) -> BTreeMap<String, FitsValue> {
    let mut axis = 1usize;
    for coordinate_index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(coordinate_index);
        if coordinate.coordinate_type() == CoordinateType::Spectral {
            let rest_frequency = spectral_rest_frequency(coordinates).unwrap_or(0.0);
            if rest_frequency <= 0.0 {
                break;
            }
            let crval_hz = coordinate.reference_value()[0];
            let cdelt_hz = coordinate.increment()[0];
            let crval_m_s = SPEED_OF_LIGHT_M_S * (1.0 - crval_hz / rest_frequency);
            let cdelt_m_s = -SPEED_OF_LIGHT_M_S * cdelt_hz / rest_frequency;
            let mut overrides = BTreeMap::new();
            overrides.insert(
                format!("CTYPE{axis}"),
                FitsValue::String("VRAD".to_string()),
            );
            overrides.insert(format!("CRVAL{axis}"), FitsValue::Float(crval_m_s));
            overrides.insert(format!("CDELT{axis}"), FitsValue::Float(cdelt_m_s));
            overrides.insert(format!("CUNIT{axis}"), FitsValue::String("m".to_string()));
            return overrides;
        }
        axis += coordinate.n_pixel_axes();
    }
    BTreeMap::new()
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
        let bundle = image_analysis_task_schema_bundle();
        bundle.validate().expect("image-analysis provider contract");
        assert_eq!(
            bundle.protocol.protocol_name,
            IMAGE_ANALYSIS_TASK_PROTOCOL_NAME
        );
        assert_eq!(bundle.semantic.operations.len(), 11);
        assert_eq!(bundle.parameter_surfaces.len(), 11);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "imhead");
        assert_eq!(bundle.parameter_surfaces[10].surface.id(), "importfits");
        for surface in &bundle.parameter_surfaces {
            surface.validate().unwrap_or_else(|errors| {
                panic!(
                    "invalid embedded image-analysis surface {}: {errors:?}",
                    surface.surface.id()
                )
            });
        }
        assert_eq!(
            serde_json::to_value(&bundle).unwrap()["parameter_surfaces"]
                .as_array()
                .unwrap()
                .len(),
            11
        );
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
                region: None,
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
        let source = PagedImage::<f32>::open(&source_path).unwrap();
        let expected_flux = stats.sum * direction_pixel_area(source.coordinates()).unwrap()
            / source
                .image_info()
                .unwrap()
                .beam_set
                .single_beam()
                .unwrap()
                .area()
            / 2.0;
        assert!((stats.flux.unwrap() - expected_flux).abs() < 1e-12);

        let moment_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Immoments(ImmomentsRequest {
                imagename: source_path.clone(),
                outfile: moment_path.clone(),
                moments: 0,
                chans: Some("0~2".to_string()),
                includepix: Some([2.0, 12.0]),
                mask: None,
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
        for (moment_number, expected_units) in
            [(-1, "Jy/beam"), (1, "km/s"), (2, "km/s"), (3, "Jy/beam")]
        {
            let output_path = temp.path().join(format!("moment{moment_number}.image"));
            let summary = immoments(&ImmomentsRequest {
                imagename: source_path.clone(),
                outfile: output_path,
                moments: moment_number,
                chans: Some("0~2".to_string()),
                includepix: Some([2.0, 12.0]),
                mask: None,
                overwrite: false,
            })
            .unwrap();
            assert_eq!(summary.moment, moment_number);
            assert_eq!(summary.units, expected_units);
            assert_eq!(summary.valid_profiles, 4);
        }

        let mask_path = temp.path().join("source.pb");
        create_spectral_mask_image(&mask_path);
        let masked_moment_path = temp.path().join("masked-moment.image");
        let masked_moment = immoments(&ImmomentsRequest {
            imagename: source_path.clone(),
            outfile: masked_moment_path,
            moments: 0,
            chans: Some("0~2".to_string()),
            includepix: None,
            mask: Some(format!("{}>0.3", mask_path.display())),
            overwrite: false,
        })
        .unwrap();
        assert_eq!(masked_moment.valid_profiles, 3);

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

        let subimage_path = temp.path().join("sub.image");
        let subimage_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Imsubimage(ImsubimageRequest {
                imagename: source_path.clone(),
                outfile: subimage_path.clone(),
                box_pixels: Some("0,0,0,1".to_string()),
                chans: Some("1~2".to_string()),
                overwrite: false,
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Imsubimage(subimage) = subimage_result else {
            panic!("expected imsubimage result");
        };
        assert_eq!(subimage.shape, vec![1, 2, 2]);
        let subimage_image = PagedImage::<f32>::open(&subimage_path).unwrap();
        assert_eq!(subimage_image.shape(), &[1, 2, 2]);

        let product_path = temp.path().join("product.image");
        let math_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Immath(ImmathRequest {
                imagename: vec![subimage_path.clone(), subimage_path.clone()],
                outfile: product_path.clone(),
                expr: "IM0 * IM1".to_string(),
                overwrite: false,
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Immath(math) = math_result else {
            panic!("expected immath result");
        };
        assert_eq!(math.shape, vec![1, 2, 2]);
        assert_eq!(math.valid_pixels, 4);
        let product_image = PagedImage::<f32>::open(&product_path).unwrap();
        assert_eq!(product_image.shape(), &[1, 2, 2]);

        let scaled_path = temp.path().join("scaled.image");
        let scaled_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Immath(ImmathRequest {
                imagename: vec![subimage_path.clone()],
                outfile: scaled_path.clone(),
                expr: "1.222e6*IM0/1.579^2/(29.30*29.03)".to_string(),
                overwrite: false,
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Immath(scaled) = scaled_result else {
            panic!("expected scalar immath result");
        };
        assert_eq!(scaled.shape, vec![1, 2, 2]);
        assert_eq!(scaled.valid_pixels, 4);
        let scaled_image = PagedImage::<f32>::open(&scaled_path).unwrap();
        let subimage_input = subimage_image.get_slice(&[0, 0, 0], &[1, 1, 1]).unwrap();
        let scaled_data = scaled_image.get_slice(&[0, 0, 0], &[1, 1, 1]).unwrap();
        let factor = 1.222e6 / 1.579_f64.powi(2) / (29.30 * 29.03);
        let expected = f64::from(subimage_input[[0, 0, 0]]) * factor;
        assert!((f64::from(scaled_data[[0, 0, 0]]) - expected).abs() < 1.0e-3);
        let put_header = imhead_put(&scaled_path, "bunit", "K").unwrap();
        assert_eq!(put_header.units, "K");
        assert_eq!(PagedImage::<f32>::open(&scaled_path).unwrap().units(), "K");

        let pbcor_path = temp.path().join("pbcor.image");
        let pbcor_result =
            run_image_analysis_task(ImageAnalysisTaskRequest::Impbcor(ImpbcorRequest {
                imagename: source_path.clone(),
                pbimage: mask_path.clone(),
                outfile: pbcor_path.clone(),
                mode: "divide".to_string(),
                cutoff: 0.3,
                box_selection: None,
                region: None,
                chans: None,
                stokes: None,
                mask: None,
                stretch: false,
                overwrite: false,
            }))
            .unwrap();
        let ImageAnalysisTaskResult::Impbcor(pbcor) = pbcor_result else {
            panic!("expected impbcor result");
        };
        assert_eq!(pbcor.shape, vec![2, 2, 3]);
        assert_eq!(pbcor.units, "Jy/beam");
        assert_eq!(pbcor.valid_pixels, 9);
        let pbcor_image = PagedImage::<f32>::open(&pbcor_path).unwrap();
        assert_eq!(pbcor_image.default_mask_name(), Some("mask0".to_string()));
        let pbcor_data = pbcor_image.get_slice(&[1, 0, 0], &[1, 1, 1]).unwrap();
        let source_data = source.get_slice(&[1, 0, 0], &[1, 1, 1]).unwrap();
        assert!((pbcor_data[[0, 0, 0]] - source_data[[0, 0, 0]]).abs() < 1.0e-6);

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
        let payload = casa_task_runtime::read_task_request(request_path.to_str().unwrap()).unwrap();
        assert!(matches!(
            serde_json::from_str::<ImageAnalysisTaskRequest>(&payload).unwrap(),
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
            imstat(&complex_path, None, None, None, None),
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
                mask: None,
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
            imstat(&real_path, Some("0,0,4,4"), None, None, None),
            Err(ImageError::ShapeMismatch { .. })
        ));
        assert!(matches!(
            imstat(&real_path, None, None, Some("0"), None),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("spectral axis")
        ));
        assert!(matches!(
            imstat(
                &real_path,
                Some("0,0,1,1"),
                Some("box:0,0,1,1"),
                None,
                None
            ),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("either box or region")
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
                moments: 12,
                chans: None,
                includepix: None,
                mask: None,
                overwrite: false,
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("moments -1 through 3")
        ));
        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: real_path,
                outfile: output_path,
                moments: 0,
                chans: None,
                includepix: None,
                mask: None,
                overwrite: false,
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("spectral axis")
        ));
    }

    #[test]
    fn imstat_accepts_inline_and_crtf_pixel_box_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_only_test_image(&image_path);

        let inline = imstat(
            &image_path,
            None,
            Some("box[[0pix,0pix],[0pix,0pix]]"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(inline.npts, 1.0);
        assert_eq!(inline.sum, 1.0);

        fs::write(
            &region_path,
            "#CRTFv0 CASA Region Text Format version 0\nbox[[0pix,0pix],[1pix,0pix]]\n",
        )
        .unwrap();

        let from_file = imstat(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(from_file.npts, 2.0);
        assert_eq!(from_file.sum, 3.0);
        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![1, 0]);
    }

    #[test]
    fn imstat_accepts_crtf_world_box_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_only_test_image(&image_path);

        let image = PagedImage::<f32>::open(&image_path).unwrap();
        let first = image.coordinates().to_world(&[0.0, 0.0]).unwrap();
        let second = image.coordinates().to_world(&[1.0, 0.0]).unwrap();
        fs::write(
            &region_path,
            format!(
                "#CRTFv0 CASA Region Text Format version 0\nbox[[{:.15}rad,{:.15}rad],[{:.15}rad,{:.15}rad]]\n",
                first[0], first[1], second[0], second[1]
            ),
        )
        .unwrap();

        let from_file = imstat(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(from_file.npts, 2.0);
        assert_eq!(from_file.sum, 3.0);
        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![1, 0]);
    }

    #[test]
    fn imstat_accepts_crtf_pixel_polygon_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_grid_test_image(&image_path, &[3, 3]);
        fs::write(
            &region_path,
            "#CRTFv0 CASA Region Text Format version 0\npoly [[0pix,0pix], [2pix,0pix], [0pix,2pix]]\n",
        )
        .unwrap();

        let from_file = imstat(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
            None,
            None,
        )
        .unwrap();

        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![2, 2]);
        assert_eq!(from_file.npts, 3.0);
        assert_eq!(from_file.sum, 14.0);
    }

    #[test]
    fn imstat_accepts_crtf_world_polygon_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_grid_test_image(&image_path, &[3, 3]);
        let image = PagedImage::<f32>::open(&image_path).unwrap();
        let a = image.coordinates().to_world(&[0.0, 0.0]).unwrap();
        let b = image.coordinates().to_world(&[2.0, 0.0]).unwrap();
        let c = image.coordinates().to_world(&[0.0, 2.0]).unwrap();
        fs::write(
            &region_path,
            format!(
                "#CRTFv0 CASA Region Text Format version 0\npoly [[{:.15}rad,{:.15}rad], [{:.15}rad,{:.15}rad], [{:.15}rad,{:.15}rad]]\n",
                a[0], a[1], b[0], b[1], c[0], c[1]
            ),
        )
        .unwrap();

        let from_file = imstat(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
            None,
            None,
        )
        .unwrap();

        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![2, 2]);
        assert_eq!(from_file.npts, 3.0);
        assert_eq!(from_file.sum, 14.0);
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
            None,
            Some("0~1"),
            Some([1.0, 8.0]),
        )
        .unwrap();
        assert_eq!(stats.npts, 7.0);
        assert_eq!(stats.units, "K");
    }

    #[test]
    fn exportfits_writes_default_mask_as_nan_and_velocity_wcs() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("masked.image");
        let fits_path = temp.path().join("masked.fits");

        let rest_frequency = 372.0e9;
        let frequency_increment = -620_000.0;
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
            rest_frequency,
            frequency_increment,
            0.0,
            rest_frequency,
        )));
        let shape = vec![2, 2, 3];
        let mut image =
            PagedImage::<f32>::create(shape.clone(), coordinates, &source_path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f32)
                .collect(),
        )
        .unwrap();
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.make_mask("mask0", true, true).unwrap();
        let mut mask = ArrayD::from_elem(IxDyn(&shape).f(), true);
        mask[[0, 0, 0]] = false;
        mask[[1, 1, 2]] = false;
        image.put_mask("mask0", &mask).unwrap();
        image.save().unwrap();

        export_fits(&source_path, &fits_path, true, false).unwrap();

        let pixels = read_fits_pixels(&fits_path);
        assert_eq!(pixels.iter().filter(|value| value.is_nan()).count(), 2);
        assert_eq!(read_fits_key_as_string(&fits_path, "CTYPE3"), "VRAD");
        assert_eq!(read_fits_key_as_string(&fits_path, "CUNIT3"), "m");
        let crval3 = read_fits_key_as_f64(&fits_path, "CRVAL3");
        let cdelt3 = read_fits_key_as_f64(&fits_path, "CDELT3");
        assert!(crval3.abs() < 1.0e-9);
        let expected_increment = -SPEED_OF_LIGHT_M_S * frequency_increment / rest_frequency;
        assert!((cdelt3 - expected_increment).abs() < 1.0e-6);
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
        for moment in [-1, 0, 1, 2, 3] {
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

    fn create_spectral_mask_image(path: &Path) {
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
        let mut data = ArrayD::<f32>::ones(IxDyn(&shape).f());
        for chan in 0..shape[2] {
            data[[0, 0, chan]] = 0.0;
        }
        image.put_slice(&data, &[0, 0, 0]).unwrap();
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

    fn create_direction_grid_test_image(path: &Path, shape: &[usize]) {
        let mut image =
            PagedImage::<f32>::create(shape.to_vec(), direction_coordinates(), path).unwrap();
        let data = ArrayD::from_shape_fn(IxDyn(shape).f(), |idx| (idx[0] + idx[1] * 10 + 1) as f32);
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

    fn read_fits_key_as_f64(path: &Path, key: &str) -> f64 {
        let mut fits = FitsFile::open(path).unwrap();
        let hdu = fits.primary_hdu().unwrap();
        hdu.read_key::<f64>(&mut fits, key).unwrap()
    }
}
