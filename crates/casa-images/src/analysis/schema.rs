// SPDX-License-Identifier: LGPL-3.0-or-later
//! Types shared by image-analysis execution and build-time schema generation.

use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Check the schema library's map ordering across the build and target.
#[allow(dead_code)] // The target may use the non-generated fallback.
pub(crate) fn schema_maps_preserve_order() -> bool {
    let mut map = schemars::Map::new();
    map.insert("z", ());
    map.insert("a", ());
    map.keys().next().is_some_and(|key| *key == "z")
}

/// Global extrema and zero-based pixel positions, over unmasked pixels.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageExtrema {
    /// Minimum pixel value.
    pub min: f64,
    /// Maximum pixel value.
    pub max: f64,
    /// Location of the first minimum in casacore pixel order.
    pub min_position: Vec<usize>,
    /// Location of the first maximum in casacore pixel order.
    pub max_position: Vec<usize>,
    /// CASA-formatted world coordinate of the first minimum.
    pub min_world: String,
    /// CASA-formatted world coordinate of the first maximum.
    pub max_world: String,
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
    /// Interpolation method: `nearest`, `linear` or `cubic`, read by its
    /// first letter as casacore reads it (`casa_images::RegridMethod`).
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
    /// Stokes labels in pixel order, for a Stokes axis.
    pub stokes: Vec<String>,
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
    /// Tile shape of the backing image.
    pub tile_shape: Vec<usize>,
    /// Brightness units.
    pub units: String,
    /// Astronomical object name.
    pub object_name: String,
    /// Observer name stored in the coordinate system.
    pub observer: String,
    /// Telescope name stored in the coordinate system.
    pub telescope: String,
    /// Observation date in CASA's YMD format.
    pub observation_date: String,
    /// Direction reference frame, if the image has a direction coordinate.
    pub direction_reference: Option<String>,
    /// CASA's direction projection description, if there is a direction axis.
    pub direction_projection: Option<String>,
    /// Frequency frame and rest frequencies when a spectral axis exists.
    pub spectral: Option<Box<SpectralHeaderSummary>>,
    /// Image quantity classification.
    pub image_type: String,
    /// Stored default pixel mask name.
    pub default_mask: Option<String>,
    /// Stored pixel masks.
    pub masks: Vec<String>,
    /// Stored regions.
    pub regions: Vec<String>,
    /// Restoring beam if a single global beam is present.
    pub restoring_beam: Option<Box<RestoringBeamSummary>>,
    /// Whether the image holds several per-plane beams.
    pub has_multiple_beams: bool,
    /// Per-channel and per-polarization beams, in channel-fastest order.
    pub per_plane_beams: Option<Box<PerPlaneBeamSummary>>,
    /// Pixel extrema when the task requested them.
    pub extrema: Option<Box<ImageExtrema>>,
    /// Axis metadata.
    pub axes: Vec<ImageAxisSummary>,
}

impl ImageHeaderSummary {
    /// Render a compact, human-readable summary of the image header.
    ///
    /// This is the casa-rs text presentation, rather than the detailed CASA
    /// coordinate-system log emitted by `imhead` summary mode.
    pub fn format_text(&self) -> String {
        use std::fmt::Write as _;

        let mut out = String::new();
        let _ = writeln!(out, "Image name       : {}", self.imagename);
        let _ = writeln!(out, "Image type       : {}", self.image_type);
        let _ = writeln!(out, "Pixel type       : {}", self.pixel_type);
        let _ = writeln!(out, "Image units      : {}", self.units);
        let _ = writeln!(out, "Shape            : {:?}", self.shape);
        if let Some(beam) = &self.restoring_beam {
            let _ = writeln!(
                out,
                "Restoring Beam   : {:.6} arcsec, {:.6} arcsec, {:.6} deg",
                beam.major_arcsec, beam.minor_arcsec, beam.position_angle_deg
            );
        }
        for axis in &self.axes {
            let _ = writeln!(
                out,
                "Axis {} {:<12} {:<18} shape={} refpix={:.3} crval={:.12e} cdelt={:.12e} {}",
                axis.axis,
                axis.coordinate_type,
                axis.name,
                axis.shape,
                axis.reference_pixel,
                axis.reference_value,
                axis.increment,
                axis.unit
            );
        }
        out
    }
}

/// Restoring beam grid, retaining each quantity's stored unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PerPlaneBeamSummary {
    /// Number of spectral channels in the grid.
    pub channels: usize,
    /// Number of polarization planes in the grid.
    pub stokes: usize,
    /// Beams in channel-fastest order.
    pub beams: Vec<RestoringBeamSummary>,
    /// Index of the beam with CASA's median area.
    pub median_index: usize,
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
    /// Major-axis quantity in its stored unit.
    pub major: BeamQuantity,
    /// Minor-axis quantity in its stored unit.
    pub minor: BeamQuantity,
    /// Position-angle quantity in its stored unit and range.
    pub position_angle: BeamQuantity,
    /// Position angle wrapped to CASA's (-90, 90] degree range.
    pub wrapped_position_angle: BeamQuantity,
}

/// A stored beam quantity, preserving its original unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BeamQuantity {
    /// Numerical value in `unit`.
    pub value: f64,
    /// Unit stored in the image.
    pub unit: String,
}

/// Spectral metadata in the image's stored world-axis unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpectralHeaderSummary {
    /// Native frequency reference frame.
    pub reference_frame: String,
    /// Unit used for the rest frequencies.
    pub unit: String,
    /// Active rest frequency.
    pub rest_frequency: f64,
    /// Rest-frequency list in the same unit.
    pub rest_frequencies: Vec<f64>,
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
