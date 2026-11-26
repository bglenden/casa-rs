// SPDX-License-Identifier: LGPL-3.0-or-later
//! Types shared by image-analysis execution and build-time schema generation.

use std::path::PathBuf;

use casa_types::{ArrayD, RecordValue, Value};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    /// CASA `algorithm` selection, minimum-matched: `biweight`,
    /// `chauvenet`, `classic`, `fit-half` or `hinges-fences`.
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    /// The `hinges-fences` fence multiplier.
    #[serde(default = "default_fence")]
    pub fence: f64,
    /// The `fit-half` center, minimum-matched: `mean`, `median` or `zero`.
    #[serde(default = "default_center")]
    pub center: String,
    /// Whether `fit-half` uses the values at or above the center.
    #[serde(default = "default_true")]
    pub lside: bool,
    /// The fixed `chauvenet` z-score, or a negative value for
    /// Chauvenet's criterion.
    #[serde(default = "default_zscore")]
    pub zscore: f64,
    /// The maximum `chauvenet` iterations; negative means until
    /// convergence.
    #[serde(default = "default_maxiter")]
    pub maxiter: i64,
    /// CASA `clmethod` selection for the classical method: `auto`,
    /// `tiled` or `framework`, minimum-matched. The strategy does not
    /// change the statistics.
    #[serde(default = "default_clmethod")]
    pub clmethod: String,
    /// The maximum `biweight` iterations; negative takes the fast pass.
    #[serde(default = "default_niter")]
    pub niter: i64,
    /// CASA `axes` selection: the axes to evaluate the statistics over,
    /// as a pixel-axis list or `-1` for all of them. The default is all
    /// axes.
    #[serde(default)]
    pub axes: Option<Vec<i64>>,
    /// CASA `stokes` selection: Stokes plane names, as in `I`, `QU` or
    /// `IQUV`, comma-separated and concatenated.
    #[serde(default)]
    pub stokes: Option<String>,
    /// CASA `mask` expression: an image threshold expression, as in
    /// `'"image.im" > 100'`, selecting the pixels the statistics use.
    #[serde(default)]
    pub mask: Option<String>,
    /// Whether CASA would stretch a mask whose shape differs; the
    /// expressions this surface reaches are same-shaped, so the value is
    /// accepted and inert.
    #[serde(default = "default_false")]
    pub stretch: bool,
    /// CASA `logfile`: a file to write the statistics block to, in
    /// addition to the logger.
    #[serde(default)]
    pub logfile: Option<String>,
    /// Whether the logfile is appended to rather than overwritten.
    #[serde(default = "default_true")]
    pub append: bool,
    /// Whether the statistics block is printed to the logger; the
    /// returned record is the same either way.
    #[serde(default = "default_true")]
    pub listit: bool,
    /// Whether additional messages are printed to the logger; the
    /// returned record is the same either way.
    #[serde(default = "default_true")]
    pub verbose: bool,
}

fn default_false() -> bool {
    false
}

impl Default for ImstatRequest {
    /// CASA's defaults, as the task's XML records them.
    fn default() -> Self {
        Self {
            imagename: PathBuf::new(),
            box_pixels: None,
            region: None,
            chans: None,
            includepix: None,
            algorithm: default_algorithm(),
            fence: default_fence(),
            center: default_center(),
            lside: default_true(),
            zscore: default_zscore(),
            maxiter: default_maxiter(),
            clmethod: default_clmethod(),
            niter: default_niter(),
            axes: None,
            stokes: None,
            mask: None,
            stretch: default_false(),
            logfile: None,
            append: default_true(),
            listit: default_true(),
            verbose: default_true(),
        }
    }
}

fn default_algorithm() -> String {
    "classic".to_string()
}

fn default_fence() -> f64 {
    -1.0
}

fn default_center() -> String {
    "mean".to_string()
}

fn default_true() -> bool {
    true
}

fn default_zscore() -> f64 {
    -1.0
}

fn default_maxiter() -> i64 {
    -1
}

fn default_clmethod() -> String {
    "auto".to_string()
}

fn default_niter() -> i64 {
    3
}

/// One `imstat` per-plane outcome: statistics evaluated over the
/// collapsed axes, one value per display position, in display order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImstatPlanesSummary {
    /// Input image path.
    pub imagename: String,
    /// Applied zero-based origin of the selection.
    pub blc: Vec<usize>,
    /// Applied zero-based inclusive top-right corner of the selection.
    pub trc: Vec<usize>,
    /// CASA-formatted world coordinates of the applied origin.
    pub blcf: String,
    /// CASA-formatted world coordinates of the applied top-right corner.
    pub trcf: String,
    /// The display shape, with CASA's degenerate display axes dropped.
    pub display_shape: Vec<usize>,
    /// Text table of plane coordinates and statistics for the task log.
    #[serde(skip)]
    pub plane_log: String,
    /// Number of points in each plane.
    pub npts: Vec<f64>,
    /// Each plane's minimum.
    pub min: Vec<f64>,
    /// Each plane's maximum.
    pub max: Vec<f64>,
    /// Each plane's sum.
    pub sum: Vec<f64>,
    /// Each plane's sum of squares.
    pub sumsq: Vec<f64>,
    /// Each plane's mean.
    pub mean: Vec<f64>,
    /// Each plane's root-mean-square.
    pub rms: Vec<f64>,
    /// Each plane's Bessel-corrected standard deviation.
    pub sigma: Vec<f64>,
    /// Each plane's median.
    pub median: Vec<f64>,
    /// Each plane's median absolute deviation.
    pub medabsdevmed: Vec<f64>,
    /// Each plane's first quartile.
    pub q1: Vec<f64>,
    /// Each plane's third quartile.
    pub q3: Vec<f64>,
    /// Each plane's interquartile range.
    pub quartile: Vec<f64>,
    /// Each plane's flux density, when the image carries a beam.
    pub flux: Option<Vec<f64>>,
    /// Whether the biweight's location and scale replace the mean and
    /// sigma, with no other statistics in the record.
    pub biweight: bool,
}

/// One `imstat` outcome: the global statistics, or one per plane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum ImstatOutcome {
    /// The statistics of the whole selection.
    Global(ImageStatisticsSummary),
    /// The statistics of each display plane.
    Planes(ImstatPlanesSummary),
}

/// CASA `immoments` request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImmomentsRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// Requested CASA moments, numbered `-1` through `11`.
    pub moments: Vec<i32>,
    /// Collapse axis as a zero-based index, or a CASA axis name.
    #[serde(default)]
    pub axis: Option<MomentAxis>,
    /// CASA CRTF or saved-image region.
    #[serde(default)]
    pub region: Option<String>,
    /// Inclusive direction-plane pixel boxes.
    #[serde(default, alias = "box")]
    pub box_pixels: Option<String>,
    /// Select Stokes planes by label.
    #[serde(default)]
    pub stokes: Option<String>,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Inclusive pixel-value range to include.
    #[serde(default)]
    pub includepix: Option<[f64; 2]>,
    /// Exclude values strictly between the two limits, as CASA does.
    #[serde(default)]
    pub excludepix: Option<[f64; 2]>,
    /// CASA LEL image mask expression.
    #[serde(default)]
    pub mask: Option<String>,
    /// Extend a conforming mask over singleton or missing axes.
    #[serde(default)]
    pub stretch: bool,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// Selector for `immoments` axis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum MomentAxis {
    /// Zero-based pixel axis index.
    Index(i64),
    /// Axis name accepted by CASA `immoments`.
    Name(String),
}

/// Scalar entry in `importfits` `defaultaxesvalues` or `beam` lists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ImportFitsValue {
    /// String value such as a quantity or coordinate string.
    String(String),
    /// Signed integer scalar.
    Integer(i64),
    /// Floating-point scalar.
    Float(f64),
    /// Boolean scalar.
    Bool(bool),
    /// Any other value rejected during parameter validation.
    Other,
}

/// CASA `impv` request for a position-velocity image along a strip.
#[derive(Debug, Clone, PartialEq)]
pub struct ImpvRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// CASA mode, selected by the first letter of `coords` or `length`.
    pub mode: String,
    /// First endpoint in `coords` mode.
    pub start: Option<PvPosition>,
    /// Second endpoint in `coords` mode.
    pub end: Option<PvPosition>,
    /// Midpoint in `length` mode.
    pub center: Option<PvPosition>,
    /// Distance between endpoints in pixels or angular units.
    pub length: Option<PvAmount>,
    /// Sky position angle in `length` mode.
    pub pa: Option<PvAmount>,
    /// Odd strip width in pixels, or a width in angular units.
    pub width: PvAmount,
    /// Angular unit of the output offset coordinate.
    pub unit: String,
    /// CASA text, named, or record region. Only non-direction axes may be cropped.
    pub region: Option<PbcorRegion>,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    pub chans: Option<String>,
    /// Stokes planes to include.
    pub stokes: Option<String>,
    /// Optional LEL mask evaluated before extraction.
    pub mask: Option<String>,
    /// Stretch a conforming mask over singleton or missing axes.
    pub stretch: bool,
    /// Replace an existing output image.
    pub overwrite: bool,
}

impl Default for ImpvRequest {
    fn default() -> Self {
        Self {
            imagename: PathBuf::new(),
            outfile: PathBuf::new(),
            mode: default_impv_mode(),
            start: None,
            end: None,
            center: None,
            length: None,
            pa: None,
            width: default_impv_width(),
            unit: default_pv_unit(),
            region: None,
            chans: None,
            stokes: None,
            mask: None,
            stretch: false,
            overwrite: false,
        }
    }
}

/// Sky position or zero-based direction-plane pixels for `impv`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PvPosition {
    /// Zero-based pixel coordinates as an integer pair.
    Integral([i64; 2]),
    /// Zero-based pixel coordinates, which can be fractional.
    Pixel([f64; 2]),
    /// Longitude and latitude in CASA's default direction frame.
    World([String; 2]),
    /// Sky direction, optionally prefixed with a reference frame.
    Text(String),
}

/// A length, width, or angle supplied to `impv`.
#[derive(Debug, Clone, PartialEq)]
pub enum PvAmount {
    /// Whole pixels.
    Whole(i64),
    /// Fractional pixels for the slice length.
    Fractional(f64),
    /// An angular value supplied as text.
    Angular(String),
    /// A CASA quanta record containing one angular value.
    Record(RecordValue),
}

/// CASA `imsubimage` request supporting full spatial/spectral/Stokes/mask
/// selection, degenerate axis dropping, and output overwrite control.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImsubimageRequest {
    /// Input CASA or FITS image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// CASA inclusive pixel box (or comma-separated quadruplets), formatted as `x0,y0,x1,y1[,...]`.
    #[serde(default, alias = "box")]
    pub box_pixels: Option<String>,
    /// Region selection (CRTF string/file or saved `image:region` name).
    #[serde(default)]
    pub region: Option<String>,
    /// CASA channel expression, supporting ranges, comparisons, and steps.
    #[serde(default)]
    pub chans: Option<String>,
    /// CASA Stokes selection (e.g. `I`, `QU`, `I,V`).
    #[serde(default)]
    pub stokes: Option<String>,
    /// LEL mask expression or named `image:maskname` mask reference.
    #[serde(default)]
    pub mask: Option<String>,
    /// Drop degenerate (length-1) axes unless listed in `keepaxes`.
    #[serde(default)]
    pub dropdeg: bool,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
    /// Emit informational log messages during execution.
    #[serde(default = "default_true")]
    pub verbose: bool,
    /// Stretch lower-dimensional masks along matching degenerate axes.
    #[serde(default)]
    pub stretch: bool,
    /// Zero-based degenerate pixel axes to preserve when `dropdeg` is true.
    #[serde(default)]
    pub keepaxes: Vec<i64>,
}

impl Default for ImsubimageRequest {
    /// CASA's defaults, as recorded by the `imsubimage` task surface.
    fn default() -> Self {
        Self {
            imagename: PathBuf::new(),
            outfile: PathBuf::new(),
            box_pixels: None,
            region: None,
            chans: None,
            stokes: None,
            mask: None,
            dropdeg: false,
            overwrite: false,
            verbose: default_true(),
            stretch: false,
            keepaxes: Vec::new(),
        }
    }
}

/// CASA `immath` request supporting `evalexpr`, `spix`, `pola`, `poli`,
/// `lpoli`, and `tpoli` modes with CASA's full parameter surface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImmathRequest {
    /// Input CASA or FITS image paths (`IM0`, `IM1`, ... or named via
    /// `varnames`). May be empty in `mode = "evalexpr"` when `expr` names all
    /// inputs directly in quotes.
    #[serde(default)]
    pub imagename: Vec<PathBuf>,
    /// Math mode: `evalexpr`, `spix`, `pola`, `poli`, `lpoli`, or `tpoli`.
    #[serde(default = "default_immath_mode")]
    pub mode: String,
    /// Output CASA image path.
    #[serde(default = "default_immath_outfile")]
    pub outfile: PathBuf,
    /// Mathematical expression for `mode = "evalexpr"`.
    #[serde(default = "default_immath_expr")]
    pub expr: String,
    /// Optional variable names mapping to `imagename` entries.
    #[serde(default)]
    pub varnames: Vec<String>,
    /// Noise standard deviation quantity string for polarization debiasing.
    #[serde(default = "default_immath_sigma")]
    pub sigma: String,
    /// Linearly polarized intensity threshold below which `pola` masks pixels.
    #[serde(default)]
    pub polithresh: Option<String>,
    /// LEL mask expression or mask image applied to the selection.
    #[serde(default)]
    pub mask: Option<String>,
    /// Region selection (CRTF string, pixel box, or region file path).
    #[serde(default)]
    pub region: Option<String>,
    /// Inclusive directional pixel box (or comma-separated quadruplets).
    #[serde(default, rename = "box")]
    pub box_pixels: Option<String>,
    /// CASA channel selection.
    #[serde(default)]
    pub chans: Option<String>,
    /// CASA Stokes selection (ignored in polarization modes).
    #[serde(default)]
    pub stokes: Option<String>,
    /// Stretch lower-dimensional masks to match target image shape.
    #[serde(default)]
    pub stretch: bool,
    /// Metadata source image path or variable token (e.g. `IM1`).
    #[serde(default)]
    pub imagemd: Option<String>,
    /// Output precision for `evalexpr` and `spix`: `float` or `double` (minimum match).
    #[serde(default = "default_immath_prec")]
    pub prec: String,
    /// Replace an existing output image (casa-rs extension).
    #[serde(default)]
    pub overwrite: bool,
}

impl Default for ImmathRequest {
    fn default() -> Self {
        Self {
            imagename: Vec::new(),
            mode: default_immath_mode(),
            outfile: default_immath_outfile(),
            expr: default_immath_expr(),
            varnames: Vec::new(),
            sigma: default_immath_sigma(),
            polithresh: None,
            mask: None,
            region: None,
            box_pixels: None,
            chans: None,
            stokes: None,
            stretch: false,
            imagemd: None,
            prec: default_immath_prec(),
            overwrite: false,
        }
    }
}

/// CASA `impbcor` request for primary-beam image correction.
#[derive(Debug, Clone, PartialEq)]
pub struct ImpbcorRequest {
    /// Input CASA or FITS image path.
    pub imagename: PathBuf,
    /// Primary-beam image or floating-point pixels.
    pub pbimage: PbcorBeam,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// Correction mode. Supported values are `divide` and `multiply`.
    pub mode: String,
    /// Inclusive lower bound for division or upper bound for multiplication.
    /// Negative or NaN disables cutoff masking.
    pub cutoff: f64,
    /// Optional inclusive direction-plane pixel box.
    pub box_selection: Option<String>,
    /// Optional CASA region.
    pub region: Option<PbcorRegion>,
    /// Optional channel selector.
    pub chans: Option<String>,
    /// Optional Stokes selector.
    pub stokes: Option<String>,
    /// Optional LEL mask expression.
    pub mask: Option<String>,
    /// Extend a conforming mask over singleton or missing axes.
    pub stretch: bool,
    /// Replace an existing output image.
    pub overwrite: bool,
}

/// Input primary beam for CASA `impbcor`.
#[derive(Debug, Clone, PartialEq)]
pub enum PbcorBeam {
    /// Path to a real-valued image with matching direction coordinates.
    Image(PathBuf),
    /// Floating-point array with the image shape or its two direction axes.
    Pixels(ArrayD<f64>),
}

impl From<PathBuf> for PbcorBeam {
    fn from(path: PathBuf) -> Self {
        Self::Image(path)
    }
}

/// Region representations accepted by CASA `impbcor`.
#[derive(Debug, Clone, PartialEq)]
pub enum PbcorRegion {
    /// CRTF text, filename, or saved image region name.
    Text(String),
    /// Casacore region record, as returned by the region manager.
    Record(RecordValue),
}

/// CASA `imregrid` request for template-image regridding.
#[derive(Debug, Clone, PartialEq)]
pub struct ImregridRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// A template image, the `get` selector, or a coordinate and shape record.
    pub template: ImregridTemplate,
    /// Output CASA image path.
    pub output: PathBuf,
    /// Interpolation method: `nearest`, `linear` or `cubic`, read by its
    /// first letter as casacore reads it (`casa_images::RegridMethod`).
    pub interpolation: String,
    /// Regrid channels at equal radio velocity using the two rest frequencies.
    pub asvelocity: bool,
    /// Zero-based input pixel axes, or `[-1]` for matching nondegenerate axes.
    pub axes: Vec<i64>,
    /// Output lengths in input axis order, or `[-1]` for CASA's inferred shape.
    pub shape: Vec<i64>,
    /// Direction coordinate sampling interval, with zero using each pixel.
    pub decimate: i64,
    /// Resample in pixel space instead of using the coordinate conversion.
    pub replicate: bool,
    /// Replace an existing output image.
    pub overwrite: bool,
}

/// CASA's accepted template representations.
#[derive(Debug, Clone, PartialEq)]
pub enum ImregridTemplate {
    /// Image path or a template selector such as `get`.
    Image(PathBuf),
    /// A `csys` and `shap` dictionary returned by `template='get'`.
    Record {
        /// Casacore coordinate record.
        csys: RecordValue,
        /// Target image shape in axis order.
        shape: Vec<usize>,
    },
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
    /// Positive effective single-dish diameter in metres. A nonpositive value uses the input beam.
    #[serde(default = "default_effdishdiam")]
    pub effdishdiam: f32,
    /// Low-pass filter the dish image at its nominal diameter when no positive diameter is given.
    #[serde(default)]
    pub lowpassfiltersd: bool,
    /// Replace an existing output image. CASA feather replaces by default.
    #[serde(default = "default_true")]
    pub overwrite: bool,
}

fn default_impv_mode() -> String {
    "coords".to_string()
}

fn default_impv_width() -> PvAmount {
    PvAmount::Whole(1)
}

fn default_pv_unit() -> String {
    "arcsec".into()
}

fn default_effdishdiam() -> f32 {
    -1.0
}

fn default_sdfactor() -> f32 {
    1.0
}

fn default_exportfits_bitpix() -> i32 {
    -32
}

fn default_exportfits_maxpix() -> f64 {
    -1.0
}

fn default_immath_mode() -> String {
    "evalexpr".to_string()
}

fn default_immath_outfile() -> PathBuf {
    PathBuf::from("immath_results.im")
}

fn default_immath_expr() -> String {
    "IM0".to_string()
}

fn default_immath_sigma() -> String {
    "0.0mJy/beam".to_string()
}

fn default_immath_prec() -> String {
    "float".to_string()
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
    /// Restoring beam diagnostics for the task logger.
    #[serde(skip)]
    pub beam_warnings: Vec<String>,
}

/// Summary returned after writing an `impbcor` product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PbcorSummary {
    /// Input CASA image path.
    pub imagename: String,
    /// Primary-beam image path or a description of a supplied pixel array.
    pub pbimage: String,
    /// Output CASA image path.
    pub outfile: String,
    /// Correction mode.
    pub mode: String,
    /// Cutoff used by the selected mode.
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

/// A CASA regrid returns either its template dictionary or an image product.
#[derive(Debug, Clone, PartialEq)]
pub enum ImregridOutcome {
    /// A completed regridding operation that wrote an image.
    Product(ImageRegridSummary),
    /// The input image's `csys` and `shap` fields as a casacore record value.
    Template(Value),
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
    /// Use the optical velocity convention instead of radio.
    #[serde(default)]
    pub optical: bool,
    /// Bits per FITS pixel (`-32` for IEEE float, `16` for scaled integer).
    #[serde(default = "default_exportfits_bitpix")]
    pub bitpix: i32,
    /// Minimum physical value for `bitpix = 16` scaling (`minpix > maxpix` selects auto-scaling).
    #[serde(default)]
    pub minpix: f64,
    /// Maximum physical value for `bitpix = 16` scaling (`minpix > maxpix` selects auto-scaling).
    #[serde(default = "default_exportfits_maxpix")]
    pub maxpix: f64,
    /// Replace an existing FITS file.
    #[serde(default)]
    pub overwrite: bool,
    /// Drop a degenerate Stokes axis when writing FITS.
    #[serde(default)]
    pub dropstokes: bool,
    /// Place the Stokes axis last in the exported FITS header.
    #[serde(default = "default_true")]
    pub stokeslast: bool,
    /// Write the image history log into FITS `HISTORY` cards.
    #[serde(default = "default_true")]
    pub history: bool,
    /// Drop all degenerate axes (`len == 1`) from the exported FITS image.
    #[serde(default)]
    pub dropdeg: bool,
}

impl Default for ExportFitsRequest {
    fn default() -> Self {
        Self {
            imagename: PathBuf::new(),
            fitsimage: PathBuf::new(),
            velocity: false,
            optical: false,
            bitpix: default_exportfits_bitpix(),
            minpix: 0.0,
            maxpix: default_exportfits_maxpix(),
            overwrite: false,
            dropstokes: false,
            stokeslast: true,
            history: true,
            dropdeg: false,
        }
    }
}

/// CASA `importfits` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ImportFitsRequest {
    /// Input FITS image path.
    pub fitsimage: PathBuf,
    /// Output CASA image path.
    pub imagename: PathBuf,
    /// Zero-based coordinate representation.
    pub whichrep: i64,
    /// Zero-based HDU, or a negative number to select the first image HDU.
    pub whichhdu: i64,
    /// Store a zero instead of NaN at invalid FITS pixels.
    pub zeroblanks: bool,
    /// Replace an existing CASA image.
    pub overwrite: bool,
    /// Add missing standard CASA axes and order the resulting four-axis image.
    pub defaultaxes: bool,
    /// Reference values for added axes in RA, Dec, frequency, Stokes order.
    pub defaultaxesvalues: Vec<ImportFitsValue>,
    /// Override the selected FITS restoring beam with major, minor, and PA.
    pub beam: Vec<ImportFitsValue>,
}

impl Default for ImportFitsRequest {
    fn default() -> Self {
        Self {
            fitsimage: PathBuf::new(),
            imagename: PathBuf::new(),
            whichrep: 0,
            whichhdu: -1,
            zeroblanks: true,
            overwrite: false,
            defaultaxes: false,
            defaultaxesvalues: Vec::new(),
            beam: Vec::new(),
        }
    }
}

/// Basic axis metadata used by header summaries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImageAxisSummary {
    /// Zero-based axis in the containing pixel or world ordering.
    pub axis: usize,
    /// Coordinate type for this axis.
    pub coordinate_type: String,
    /// Human-readable axis name.
    pub name: String,
    /// Corresponding pixel axis length, or zero for a world-only axis.
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
    /// Whether a pixel mask applies, including an unnamed FITS blank mask.
    #[serde(default)]
    pub has_mask: bool,
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
    /// Axis metadata in pixel order, as reported by CASA's summary mode.
    pub axes: Vec<ImageAxisSummary>,
    /// Axis metadata in world order for numbered header keywords.
    pub world_axes: Vec<ImageAxisSummary>,
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
    /// CASA-formatted world coordinates of the applied origin.
    pub blcf: String,
    /// CASA-formatted world coordinates of the applied top-right corner.
    pub trcf: String,
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
    /// Median absolute deviation from the median.
    pub medabsdevmed: f64,
    /// First quartile (25th percentile) of the selected values.
    pub q1: f64,
    /// Third quartile (75th percentile) of the selected values.
    pub q3: f64,
    /// Interquartile range `q3 − q1`.
    pub quartile: f64,
    /// Minimum position relative to the original image.
    pub minpos: Option<Vec<usize>>,
    /// CASA-formatted world coordinates of the minimum position.
    pub minposf: Option<String>,
    /// Maximum position relative to the original image.
    pub maxpos: Option<Vec<usize>>,
    /// CASA-formatted world coordinates of the maximum position.
    pub maxposf: Option<String>,
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
    /// Shape shared by all collapsed images.
    pub shape: Vec<usize>,
    /// Output images in the order requested by the caller.
    pub products: Vec<MomentImageSummary>,
}

/// One product of a CASA `immoments` task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MomentImageSummary {
    /// Output CASA image path.
    pub outfile: String,
    /// CASA moment number.
    pub moment: i32,
    /// Output brightness units.
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
