// SPDX-License-Identifier: LGPL-3.0-or-later

//! The imaging request: the provider-contracts catalog's `imager` surface,
//! resolved, as one typed record (plan section 5.8, decision D8).
//!
//! Field names and choices are the catalog's, and so are the defaults: a
//! request is deserialized only from resolved catalog values (every route,
//! the command line and `--json-run` alike, resolves through the catalog),
//! so nothing here restates a default and a missing field is an error. The
//! catalog spells an unset optional value `"none"`. Parameters that are
//! active only for one gridder live in that gridder's variant.

use std::{fmt, num::NonZeroUsize, path::PathBuf, str::FromStr};

use casa_imaging_model::{
    HogbomIterationAccounting, PolarizationCoordinate, ProductNormalization, RestoringBeamPolicy,
};
use casa_imaging_operator::GridPrecision;
use casa_imaging_runtime::ResourcePolicy;
use casa_imaging_runtime::pass::BackendChoice;
use casa_ms::CubeInterpolation;
use casa_types::measures::{doppler::DopplerRef, frequency::FrequencyRef};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

/// One imaging run's parameters, as the catalog resolved them.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ImagingRequest {
    /// The MeasurementSet.
    #[serde(deserialize_with = "single")]
    pub vis: PathBuf,
    /// Prefix of every image product; a relative name is resolved against
    /// the working directory, as CASA's is.
    #[serde(deserialize_with = "absolute")]
    pub imagename: PathBuf,
    /// Image side in pixels; images are square.
    #[serde(deserialize_with = "square")]
    pub imsize: usize,
    /// Cell side in arcseconds.
    #[serde(deserialize_with = "cell_arcsec")]
    pub cell: f64,
    /// The visibility column imaged; `None` takes `CORRECTED_DATA` when it
    /// exists and `DATA` otherwise.
    #[serde(deserialize_with = "optional")]
    pub datacolumn: Option<DataColumn>,
    /// Whether the final model's prediction is written to `MODEL_DATA`.
    #[serde(deserialize_with = "save_model")]
    pub savemodel: bool,
    /// A CASA outlier file naming further image domains.
    #[serde(deserialize_with = "optional")]
    pub outlierfile: Option<PathBuf>,
    /// Selected `FIELD_ID`s, from identifiers and ranges such as
    /// `0,3~5`; `None` selects every field.
    #[serde(deserialize_with = "field_ids")]
    pub field: Option<Vec<i32>>,
    /// The `FIELD_ID` whose phase centre is the image centre.
    #[serde(deserialize_with = "optional")]
    pub phasecenter_field: Option<i32>,
    /// The selected `DATA_DESC_ID`.
    #[serde(deserialize_with = "optional")]
    pub ddid: Option<i32>,
    /// The image centre: `J2000 <lon> <lat>`, `TRACKFIELD`, or an
    /// ephemeris name or table.
    #[serde(deserialize_with = "optional")]
    pub phasecenter: Option<String>,
    /// CASA spectral-window selection, for example `0:1~30`.
    #[serde(deserialize_with = "optional")]
    pub spw: Option<String>,
    /// First selected channel when `spw` names none.
    #[serde(deserialize_with = "optional")]
    pub channel_start: Option<usize>,
    /// Number of selected channels when `spw` names none.
    #[serde(deserialize_with = "optional")]
    pub channel_count: Option<usize>,
    /// CASA baseline-length selection.
    #[serde(deserialize_with = "optional")]
    pub uvrange: Option<String>,
    /// CASA observing-intent selection.
    #[serde(deserialize_with = "optional")]
    pub intent: Option<String>,
    /// The imaged Stokes parameters (`I`, `IQUV`, …) or correlations
    /// (`RR`, `XXYY`, …), in canonical order.
    #[serde(deserialize_with = "stokes")]
    pub stokes: Vec<PolarizationCoordinate>,
    /// Continuum (`mfs`) or one of the cube modes.
    pub specmode: SpecMode,
    /// First output channel of a cube: a channel, frequency or velocity.
    #[serde(deserialize_with = "optional")]
    pub start: Option<String>,
    /// Output channel width of a cube: channels, a frequency or a velocity.
    #[serde(deserialize_with = "optional")]
    pub width: Option<String>,
    /// Spectral frame of a `cube`'s output axis.
    #[serde(deserialize_with = "parsed")]
    pub outframe: FrequencyRef,
    /// Velocity convention of a cube's velocity `start` and axis.
    #[serde(deserialize_with = "parsed")]
    pub veltype: DopplerRef,
    /// How a cube resamples source channels onto its output channels.
    #[serde(deserialize_with = "interpolation")]
    pub interpolation: CubeInterpolation,
    /// Rest frequency of a cube's velocity axis, in Hz.
    #[serde(deserialize_with = "rest_frequency_hz")]
    pub restfreq: Option<f64>,
    /// One restoring beam per plane or one common beam.
    #[serde(deserialize_with = "restoring_beam")]
    pub restoringbeam: RestoringBeamPolicy,
    /// Whether a cube's density weighting is per output channel.
    pub perchanweightdensity: bool,
    /// Write the dirty products only, with no minor cycle.
    pub dirty_only: bool,
    /// Total minor-cycle iterations.
    pub niter: usize,
    /// Absolute stopping threshold in Jy/beam.
    #[serde(deserialize_with = "threshold_jy")]
    pub threshold: f64,
    /// Major-cycle limit; `None` is CASA's `nmajor = -1`.
    #[serde(deserialize_with = "major_cycle_limit")]
    pub nmajor: Option<usize>,
    /// Minor-cycle loop gain.
    pub gain: f64,
    /// Robust-noise stopping multiplier; zero disables it.
    pub nsigma: f64,
    /// Restoring-beam fit cutoff.
    pub psfcutoff: f64,
    /// Minor-cycle iterations between major cycles (CASA `cycleniter`).
    pub minor_cycle_length: usize,
    /// PSF-sidelobe multiplier of the cycle threshold.
    pub cyclefactor: f64,
    /// The minor-cycle solver.
    pub deconvolver: Deconvolver,
    /// Lower clamp of the cycle threshold's PSF fraction.
    pub minpsffraction: f64,
    /// Upper clamp of the cycle threshold's PSF fraction.
    pub maxpsffraction: f64,
    /// Taylor terms of `mtmfs`.
    pub nterms: usize,
    /// Högbom's strict or CASA-inclusive iteration accounting.
    #[serde(deserialize_with = "hogbom_iteration_mode")]
    pub hogbom_iteration_mode: HogbomIterationAccounting,
    /// Multiscale kernel sizes in pixels; empty for point components.
    #[serde(deserialize_with = "list")]
    pub scales: Vec<f64>,
    /// CASA multiscale small-scale bias.
    pub smallscalebias: f64,
    /// User masks or CASA auto-multithresh.
    pub usemask: UseMask,
    /// Auto-multithresh sidelobe threshold multiplier.
    pub sidelobethreshold: f64,
    /// Auto-multithresh noise threshold multiplier.
    pub noisethreshold: f64,
    /// Auto-multithresh low-noise growth multiplier.
    pub lownoisethreshold: f64,
    /// Auto-multithresh negative-feature multiplier; zero disables it.
    pub negativethreshold: f64,
    /// Auto-multithresh smallest region, as a fraction of the beam.
    pub minbeamfrac: f64,
    /// Auto-multithresh growth iterations.
    pub growiterations: usize,
    /// Inclusive pixel boxes `[x0, y0, x1, y1]` that may be cleaned.
    #[serde(deserialize_with = "mask_boxes")]
    pub mask_box: Vec<[usize; 4]>,
    /// A CASA image whose non-zero pixels may be cleaned.
    #[serde(deserialize_with = "optional")]
    pub mask_image: Option<PathBuf>,
    /// Visibility weighting.
    pub weighting: Weighting,
    /// Briggs robustness.
    pub robust: f64,
    /// The convolution-function set, with the parameters only it reads.
    #[serde(flatten)]
    pub gridder: Gridder,
    /// Write the primary-beam image.
    pub write_pb: bool,
    /// Write the primary-beam-corrected image.
    pub pbcor: bool,
    /// Signed CASA `pblimit`: its magnitude bounds the primary-beam support;
    /// a negative value leaves uncorrected products unmasked.
    pub pblimit: f64,
    /// Line-free channels of a visibility-domain continuum fit, for
    /// example `0:0~239;281~383`.
    #[serde(deserialize_with = "optional")]
    pub fitspw: Option<String>,
    /// Polynomial order of the continuum fit.
    pub fitorder: usize,
    /// Write the continuum-subtracted visibilities to `CORRECTED_DATA`.
    pub save_continuum_residual: bool,
    /// Use the host's workers (the balanced policy) instead of one.
    pub parallel: bool,
    /// Where the major-cycle passes grid.
    pub backend: BackendChoice,
    /// Grid arithmetic; `None` (`auto`) is plan decision D2's rule.
    #[serde(deserialize_with = "grid_precision")]
    pub gridprecision: Option<GridPrecision>,
}

/// Visibility column imaged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataColumn {
    /// `DATA`.
    Data,
    /// `CORRECTED_DATA`.
    Corrected,
}

impl FromStr for DataColumn {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.to_ascii_uppercase().as_str() {
            "DATA" => Ok(Self::Data),
            "CORRECTED" | "CORRECTED_DATA" => Ok(Self::Corrected),
            _ => Err(format!(
                "datacolumn {text:?} is neither DATA nor CORRECTED_DATA"
            )),
        }
    }
}

/// Spectral imaging mode (`specmode`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SpecMode {
    /// One continuum plane over every selected channel.
    Mfs,
    /// A cube in the requested output frame.
    Cube,
    /// A cube in the data's own frame.
    Cubedata,
    /// A cube in the rest frame of a moving source.
    Cubesource,
}

/// Minor-cycle solver (`deconvolver`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Deconvolver {
    /// Högbom point components.
    Hogbom,
    /// Clark point components.
    Clark,
    /// Multiscale components.
    Multiscale,
    /// Multi-term multi-frequency synthesis.
    Mtmfs,
}

/// Mask mode (`usemask`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum UseMask {
    /// The boxes and image the request names; everything when it names none.
    User,
    /// CASA auto-multithresh.
    AutoMultithresh,
}

/// Visibility weighting (`weighting`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Weighting {
    /// The visibilities' own weights.
    Natural,
    /// Uniform density weighting.
    Uniform,
    /// Briggs weighting at `robust`.
    Briggs,
    /// Briggs weighting with CASA's bandwidth taper at `robust`.
    Briggsbwtaper,
}

/// Convolution-function set (`gridder`) and the parameters only it reads.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "gridder", rename_all = "lowercase")]
pub enum Gridder {
    /// The prolate-spheroidal kernel.
    Standard,
    /// W-projection.
    Wproject {
        /// W planes; `None` derives the count from the selected W range.
        #[serde(deserialize_with = "optional")]
        wprojplanes: Option<NonZeroUsize>,
    },
    /// The heterogeneous-array mosaic beams.
    Mosaic {
        /// Point each field's beam along the `POINTING` table.
        usepointing: bool,
        /// Flat-noise or flat-sky products.
        #[serde(deserialize_with = "normalization")]
        normtype: ProductNormalization,
    },
    /// EVLA A-projection with W-projection.
    Awproject(AwProjection),
}

/// The parameters of `gridder = awproject`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct AwProjection {
    /// W planes of the convolution-function cache.
    #[serde(deserialize_with = "optional")]
    pub wprojplanes: Option<NonZeroUsize>,
    /// Sample each row's pointing from the `POINTING` table.
    pub usepointing: bool,
    /// Flat-noise or flat-sky products.
    #[serde(deserialize_with = "normalization")]
    pub normtype: ProductNormalization,
    /// Bound on resident convolution-function cells, in MiB.
    pub cf_resident_mb: usize,
    /// CASA pointing-offset grouping and refresh thresholds in arcseconds;
    /// any count but two takes CASA's `[600, 600]` under `usepointing`.
    #[serde(deserialize_with = "list")]
    pub pointingoffsetsigdev: Vec<f64>,
    /// Where the convolution functions come from.
    #[serde(flatten)]
    pub cf_source: AwCfSource,
}

/// Origin of the AW convolution functions (`aw_cf_source`).
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "aw_cf_source", rename_all = "kebab-case")]
pub enum AwCfSource {
    /// A CASA `CFS_*`/`WTCFS_*` cache, read as it is.
    CasaImport {
        /// The CASA cache directory.
        cfcache: PathBuf,
    },
    /// Cells generated natively from the EVLA dish surface into a cache
    /// directory in CASA's format.
    NativeEvla {
        /// The native cache directory.
        native_cf_cache: PathBuf,
        /// The EVLA dish-surface table.
        evla_surface: PathBuf,
        /// Reuse, fill in or regenerate the cache.
        native_cf_policy: NativeAwCachePolicy,
        /// Even FFT side of the aperture grid.
        native_cf_working_size: usize,
        /// Oversampling of the convolution plane.
        native_cf_oversampling: usize,
        /// Bound on the cache on disk, in bytes.
        native_cf_cache_bytes: u64,
        /// Bound on the number of cells.
        native_cf_maximum_cells: usize,
    },
}

/// What a native AW run does with its cache (`native_cf_policy`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeAwCachePolicy {
    /// Use the cells present; every requested cell must be there.
    ReuseOnly,
    /// Generate the requested cells that are absent.
    GenerateMissing,
    /// Clear the cache and generate every requested cell.
    Regenerate,
}

/// A request whose parameters contradict each other or the installed
/// implementation's bounds.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid imaging request ({parameters}): {reason}")]
pub struct InvalidRequest {
    /// The parameters involved.
    pub parameters: &'static str,
    /// Why they are invalid.
    pub reason: &'static str,
}

impl ImagingRequest {
    /// Check the relations between parameters that no single parameter's
    /// catalog domain can state.
    pub fn validate(&self) -> Result<(), InvalidRequest> {
        let invalid = |parameters, reason| Err(InvalidRequest { parameters, reason });
        if self.phasecenter.is_some() && self.phasecenter_field.is_some() {
            return invalid(
                "phasecenter, phasecenter_field",
                "name one image centre, not two",
            );
        }
        if self.imsize == 0 || self.cell <= 0.0 {
            return invalid("imsize, cell", "the image must have positive extent");
        }
        if self.psfcutoff <= 0.0 || self.pblimit == 0.0 || self.pblimit.abs() >= 1.0 {
            return invalid(
                "psfcutoff, pblimit",
                "psfcutoff must be positive and |pblimit| in (0, 1)",
            );
        }
        if self.nsigma < 0.0 || self.scales.iter().any(|scale| *scale < 0.0) {
            return invalid("nsigma, scales", "must not be negative");
        }
        if self.nterms == 0 || (self.nterms > 1) != (self.deconvolver == Deconvolver::Mtmfs) {
            return invalid(
                "nterms, deconvolver",
                "nterms > 1 exactly when deconvolver = mtmfs",
            );
        }
        if self.deconvolver == Deconvolver::Mtmfs && self.specmode != SpecMode::Mfs {
            return invalid("deconvolver, specmode", "mtmfs images a continuum (mfs)");
        }
        if self.solves() && (self.minor_cycle_length == 0 || self.nmajor == Some(0)) {
            return invalid(
                "minor_cycle_length, nmajor",
                "a solving run needs minor-cycle iterations and a major cycle",
            );
        }
        if self.savemodel && !self.solves() {
            return invalid("savemodel, niter", "MODEL_DATA needs a solved model");
        }
        if (self.mask_image.is_some() && !self.mask_box.is_empty())
            || (self.usemask == UseMask::AutoMultithresh
                && (self.mask_image.is_some() || !self.mask_box.is_empty()))
        {
            return invalid(
                "usemask, mask_image, mask_box",
                "name one mask: auto-multithresh, a mask image or boxes",
            );
        }
        if self.save_continuum_residual && self.fitspw.is_none() {
            return invalid(
                "save_continuum_residual, fitspw",
                "the residual of a continuum fit needs the fit",
            );
        }
        if self.fitspw.is_some() && self.specmode == SpecMode::Mfs {
            return invalid(
                "fitspw, specmode",
                "continuum subtraction feeds a channel-local cube",
            );
        }
        if self.outlierfile.is_some()
            && (self.weighting != Weighting::Natural
                || (self.solves() && self.deconvolver != Deconvolver::Hogbom))
        {
            return invalid(
                "outlierfile",
                "outlier domains are imaged with natural weighting, dirty or Högbom",
            );
        }
        if let Gridder::Awproject(aw) = &self.gridder {
            aw.validate()?;
        }
        Ok(())
    }

    /// Whether the run solves for a model: `mtmfs` always, the other
    /// solvers unless the run is dirty (`dirty_only` or `niter = 0`).
    #[must_use]
    pub fn solves(&self) -> bool {
        self.deconvolver == Deconvolver::Mtmfs || (!self.dirty_only && self.niter > 0)
    }

    /// Total minor-cycle iterations: none for `dirty_only`.
    #[must_use]
    pub const fn iterations(&self) -> usize {
        if self.dirty_only { 0 } else { self.niter }
    }

    /// The resource policy `parallel` asks for: the balanced share of the
    /// host, or one worker with no memory ceiling of its own.
    #[must_use]
    pub const fn resource_policy(&self) -> ResourcePolicy {
        if self.parallel {
            ResourcePolicy::Balanced
        } else {
            ResourcePolicy::Explicit {
                workers: 1,
                memory: u64::MAX,
            }
        }
    }
}

impl AwProjection {
    fn validate(&self) -> Result<(), InvalidRequest> {
        let invalid = |parameters, reason| Err(InvalidRequest { parameters, reason });
        // The EVLA cache contract CASA's AWProjectFT builds: 32 W planes.
        if self.wprojplanes.map(NonZeroUsize::get) != Some(32) {
            return invalid(
                "wprojplanes",
                "AW projection uses CASA's 32-plane EVLA cache",
            );
        }
        if self
            .pointingoffsetsigdev
            .iter()
            .any(|threshold| *threshold < 0.0)
        {
            return invalid("pointingoffsetsigdev", "thresholds must not be negative");
        }
        if let AwCfSource::NativeEvla {
            native_cf_working_size,
            native_cf_oversampling,
            native_cf_cache_bytes,
            native_cf_maximum_cells,
            ..
        } = &self.cf_source
        {
            let size = *native_cf_working_size;
            if size < 8
                || !size.is_multiple_of(2)
                || size
                    .checked_mul(size)
                    .and_then(|cells| cells.checked_mul(48))
                    .is_none()
                || *native_cf_oversampling == 0
                || *native_cf_oversampling > size / 4
            {
                return invalid(
                    "native_cf_working_size, native_cf_oversampling",
                    "the aperture grid must be even, at least 8, and oversampled at most a \
                     quarter of its side",
                );
            }
            if *native_cf_cache_bytes == 0 || *native_cf_maximum_cells < 32 {
                return invalid(
                    "native_cf_cache_bytes, native_cf_maximum_cells",
                    "the cache must hold every W plane",
                );
            }
        }
        Ok(())
    }
}

/// The catalog's spelling of an unset optional value.
const NONE: &str = "none";

/// Whether `text` leaves an optional value unset: `none`, or blank, as a
/// blank optional parameter always has.
fn unset(text: &str) -> bool {
    text == NONE || text.trim().is_empty()
}

/// Error on `value` for a field of type `expected`.
fn unexpected<E: serde::de::Error>(value: &Value, expected: &str) -> E {
    E::custom(format!("expected {expected}, found {value}"))
}

/// `"none"` or a value parsed from its text.
fn optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    match Value::deserialize(deserializer)? {
        Value::String(text) if unset(&text) => Ok(None),
        Value::String(text) => text.parse().map(Some).map_err(D::Error::custom),
        Value::Number(number) => number
            .to_string()
            .parse()
            .map(Some)
            .map_err(D::Error::custom),
        value => Err(unexpected(&value, "text or a number")),
    }
}

/// The one element of a one-element array, or the element itself.
fn single<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    match Value::deserialize(deserializer)? {
        Value::Array(mut items) if items.len() == 1 => match items.remove(0) {
            Value::String(text) => text.parse().map_err(D::Error::custom),
            value => Err(unexpected(&value, "text")),
        },
        Value::String(text) => text.parse().map_err(D::Error::custom),
        value => Err(unexpected(&value, "one value")),
    }
}

/// A path, resolved against the working directory when relative.
fn absolute<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PathBuf, D::Error> {
    std::path::absolute(PathBuf::deserialize(deserializer)?).map_err(D::Error::custom)
}

/// Both axes of a square pair, or one value for both.
fn square_pair(value: Value) -> Result<Value, String> {
    match value {
        Value::Array(items) => match <[Value; 2]>::try_from(items) {
            Ok([first, second]) if first == second => Ok(first),
            Ok(_) => Err("the image must be square".to_string()),
            Err(items) => Err(format!("expected two axes, found {}", items.len())),
        },
        value => Ok(value),
    }
}

fn square<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    let value = square_pair(Value::deserialize(deserializer)?).map_err(D::Error::custom)?;
    value
        .as_u64()
        .and_then(|side| usize::try_from(side).ok())
        .ok_or_else(|| unexpected(&value, "a pixel count"))
}

/// A quantity `"<number><unit>"` in the catalog's canonical unit, as a number.
fn quantity(value: &Value, unit: &str) -> Result<f64, String> {
    let text = value
        .as_str()
        .ok_or_else(|| format!("expected a quantity in {unit}, found {value}"))?;
    text.strip_suffix(unit)
        .and_then(|number| number.trim().parse::<f64>().ok())
        .filter(|number| number.is_finite())
        .ok_or_else(|| format!("expected a quantity in {unit}, found {text:?}"))
}

fn cell_arcsec<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    let value = square_pair(Value::deserialize(deserializer)?).map_err(D::Error::custom)?;
    quantity(&value, "arcsec").map_err(D::Error::custom)
}

fn threshold_jy<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    quantity(&Value::deserialize(deserializer)?, "Jy").map_err(D::Error::custom)
}

/// Comma-separated values; an unset list is empty.
fn comma_list<T>(text: &str) -> Result<Vec<T>, String>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    if unset(text) {
        return Ok(Vec::new());
    }
    text.split(',')
        .map(|item| {
            item.trim()
                .parse()
                .map_err(|error| format!("{item:?}: {error}"))
        })
        .collect()
}

/// Comma-separated finite numbers; an unset list is empty.
fn list<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f64>, D::Error> {
    let values = match Value::deserialize(deserializer)? {
        Value::String(text) => comma_list(&text).map_err(D::Error::custom)?,
        Value::Number(number) => number.as_f64().into_iter().collect(),
        value => return Err(unexpected(&value, "comma-separated numbers")),
    };
    if values.iter().all(|value: &f64| value.is_finite()) {
        Ok(values)
    } else {
        Err(D::Error::custom("numbers must be finite"))
    }
}

fn field_ids<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Vec<i32>>, D::Error> {
    match choice(deserializer)?.as_str() {
        text if unset(text) => Ok(None),
        text => casa_ms::parse_numeric_id_selector(text, "field")
            .map(Some)
            .map_err(D::Error::custom),
    }
}

/// Boxes `x0,y0,x1,y1` separated by `;`.
fn mask_boxes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<[usize; 4]>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let text = value
        .as_str()
        .ok_or_else(|| unexpected(&value, "pixel boxes"))?;
    if unset(text) {
        return Ok(Vec::new());
    }
    text.split(';')
        .map(|corners| {
            comma_list::<usize>(corners)?
                .try_into()
                .map_err(|_| format!("a mask box has four corners, not {corners:?}"))
        })
        .collect::<Result<_, String>>()
        .map_err(D::Error::custom)
}

/// Text of a choice.
fn choice<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::String(text) => Ok(text),
        value => Err(unexpected(&value, "a choice")),
    }
}

fn save_model<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    match choice(deserializer)?.as_str() {
        text if unset(text) => Ok(false),
        "modelcolumn" => Ok(true),
        other => Err(D::Error::custom(format!("savemodel {other:?}"))),
    }
}

/// CASA `stokes`: Stokes parameters (`I`, `IV`, `IQUV`, …), circular
/// correlations (`RR`, `RRLL`, …) or linear ones (`XXYY`, `XXXYYXYY`, …),
/// one kind only; sorted and without repeats.
fn stokes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<PolarizationCoordinate>, D::Error> {
    use PolarizationCoordinate::*;
    let text = choice(deserializer)?;
    let mut coordinates = Vec::new();
    let mut rest = text.as_str();
    while !rest.is_empty() {
        let (coordinate, width) = match rest.as_bytes()[0] {
            b'I' => (StokesI, 1),
            b'Q' => (StokesQ, 1),
            b'U' => (StokesU, 1),
            b'V' => (StokesV, 1),
            _ => match rest.get(..2) {
                Some("RR") => (CircularRr, 2),
                Some("RL") => (CircularRl, 2),
                Some("LR") => (CircularLr, 2),
                Some("LL") => (CircularLl, 2),
                Some("XX") => (LinearXx, 2),
                Some("XY") => (LinearXy, 2),
                Some("YX") => (LinearYx, 2),
                Some("YY") => (LinearYy, 2),
                _ => {
                    return Err(D::Error::custom(format!(
                        "stokes {text:?} is not a list of I, Q, U, V or of RR, RL, LR, LL, XX, \
                         XY, YX, YY"
                    )));
                }
            },
        };
        coordinates.push(coordinate);
        rest = &rest[width..];
    }
    coordinates.sort_unstable();
    coordinates.dedup();
    let kind = |coordinate: &PolarizationCoordinate| match coordinate {
        StokesI | StokesQ | StokesU | StokesV => 0,
        LinearXx | LinearXy | LinearYx | LinearYy => 1,
        CircularRr | CircularRl | CircularLr | CircularLl => 2,
    };
    match coordinates.first() {
        Some(first) if coordinates.iter().all(|other| kind(other) == kind(first)) => {
            Ok(coordinates)
        }
        _ => Err(D::Error::custom(format!(
            "stokes {text:?} names no coordinate, or more than one of Stokes parameters, \
             linear and circular correlations"
        ))),
    }
}

/// A value parsed from the text of a choice.
fn parsed<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    choice(deserializer)?.parse().map_err(D::Error::custom)
}

fn interpolation<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<CubeInterpolation, D::Error> {
    match choice(deserializer)?.as_str() {
        "nearest" => Ok(CubeInterpolation::Nearest),
        "linear" => Ok(CubeInterpolation::Linear),
        other => Err(D::Error::custom(format!(
            "interpolation {other:?} is neither nearest nor linear"
        ))),
    }
}

fn rest_frequency_hz<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<f64>, D::Error> {
    match choice(deserializer)?.as_str() {
        text if unset(text) => Ok(None),
        text => casa_ms::parse_rest_frequency_hz(text)
            .map(Some)
            .map_err(D::Error::custom),
    }
}

fn restoring_beam<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<RestoringBeamPolicy, D::Error> {
    match choice(deserializer)?.as_str() {
        text if unset(text) => Ok(RestoringBeamPolicy::PerPlane),
        "common" => Ok(RestoringBeamPolicy::Common),
        other => Err(D::Error::custom(format!("restoringbeam {other:?}"))),
    }
}

fn major_cycle_limit<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    match value.as_i64() {
        Some(-1) => Ok(None),
        Some(limit) => usize::try_from(limit)
            .map(Some)
            .map_err(|_| unexpected(&value, "-1 or a major-cycle count")),
        None => Err(unexpected(&value, "-1 or a major-cycle count")),
    }
}

fn hogbom_iteration_mode<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<HogbomIterationAccounting, D::Error> {
    match choice(deserializer)?.as_str() {
        "strict" => Ok(HogbomIterationAccounting::Strict),
        "casa-inclusive" => Ok(HogbomIterationAccounting::CasaInclusive),
        other => Err(D::Error::custom(format!("hogbom_iteration_mode {other:?}"))),
    }
}

fn normalization<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<ProductNormalization, D::Error> {
    match choice(deserializer)?.as_str() {
        "flatnoise" => Ok(ProductNormalization::FlatNoise),
        "flatsky" => Ok(ProductNormalization::FlatSky),
        other => Err(D::Error::custom(format!("normtype {other:?}"))),
    }
}

fn grid_precision<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<GridPrecision>, D::Error> {
    match choice(deserializer)?.as_str() {
        "auto" => Ok(None),
        "f32" => Ok(Some(GridPrecision::F32)),
        "f64" => Ok(Some(GridPrecision::F64)),
        other => Err(D::Error::custom(format!("gridprecision {other:?}"))),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::*;

    /// The active values the catalog resolves from `overrides` over its
    /// defaults, with a MeasurementSet and image name.
    fn resolved(
        overrides: Value,
    ) -> Result<serde_json::Map<String, Value>, casa_task_runtime::ResolveError> {
        let mut values = json!({ "vis": "unused.ms", "imagename": "unused" });
        let fields = values.as_object_mut().expect("object");
        fields.extend(overrides.as_object().expect("overrides object").clone());
        casa_task_runtime::resolve_plain_values(
            casa_provider_contracts::builtin_surface_bundle("imager").expect("imager surface"),
            PathBuf::from("."),
            fields,
        )
    }

    /// A request resolved through the catalog from `overrides` over its
    /// defaults, with a MeasurementSet and image name.
    pub(crate) fn request(overrides: Value) -> ImagingRequest {
        let resolved = resolved(overrides).expect("catalog values");
        serde_json::from_value(Value::Object(resolved)).expect("imaging request")
    }

    fn refusal(overrides: Value) -> &'static str {
        request(overrides)
            .validate()
            .expect_err("an invalid request")
            .parameters
    }

    #[test]
    fn the_catalog_defaults_validate() {
        let request = request(json!({}));
        request.validate().expect("defaults");
        assert_eq!(request.gridder, Gridder::Standard);
        assert_eq!(
            request.resource_policy(),
            ResourcePolicy::Explicit {
                workers: 1,
                memory: u64::MAX,
            }
        );
    }

    #[test]
    fn contradictory_parameters_are_refused_before_any_data_is_read() {
        for (overrides, parameters) in [
            (
                json!({ "phasecenter": "J2000 0rad 0rad", "phasecenter_field": "0" }),
                "phasecenter, phasecenter_field",
            ),
            (json!({ "nterms": 2 }), "nterms, deconvolver"),
            (
                json!({ "deconvolver": "mtmfs", "nterms": 2, "specmode": "cube" }),
                "deconvolver, specmode",
            ),
            (json!({ "savemodel": "modelcolumn" }), "savemodel, niter"),
            (
                json!({ "save_continuum_residual": true }),
                "save_continuum_residual, fitspw",
            ),
            (json!({ "fitspw": "0:0~3" }), "fitspw, specmode"),
            (
                json!({ "usemask": "auto-multithresh", "mask_box": "0,0,4,4" }),
                "usemask, mask_image, mask_box",
            ),
            (json!({ "pblimit": 1.0 }), "psfcutoff, pblimit"),
        ] {
            assert_eq!(refusal(overrides), parameters);
        }
    }

    #[test]
    fn aw_projection_uses_the_32_plane_evla_cache() {
        let aw = |planes: Value| {
            json!({
                "gridder": "awproject",
                "cfcache": "cf",
                "wprojplanes": planes,
            })
        };
        assert_eq!(refusal(aw(json!(16))), "wprojplanes");
        request(aw(json!(32))).validate().expect("32 planes");
        let native = json!({
            "gridder": "awproject",
            "aw_cf_source": "native-evla",
            "wprojplanes": 32,
            "native_cf_cache": "cache",
            "evla_surface": "surface",
            "native_cf_working_size": 64,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 1_048_576,
            "native_cf_maximum_cells": 16,
        });
        assert_eq!(
            refusal(native),
            "native_cf_cache_bytes, native_cf_maximum_cells"
        );
    }

    /// Every parameter the catalog resolves is a required field of the
    /// request, for every gridder and AW source: a binding the request does
    /// not read would otherwise be dropped silently (the flattened gridder
    /// rules out `deny_unknown_fields`).
    #[test]
    fn every_resolved_parameter_is_a_required_field() {
        for overrides in [
            json!({}),
            json!({ "gridder": "wproject" }),
            json!({ "gridder": "mosaic" }),
            json!({ "gridder": "awproject", "cfcache": "cf" }),
            json!({
                "gridder": "awproject",
                "aw_cf_source": "native-evla",
                "native_cf_cache": "cache",
                "evla_surface": "surface",
                "native_cf_working_size": 64,
                "native_cf_oversampling": 4,
                "native_cf_cache_bytes": 1_048_576,
                "native_cf_maximum_cells": 64,
            }),
        ] {
            let values = resolved(overrides.clone()).expect("catalog values");
            serde_json::from_value::<ImagingRequest>(Value::Object(values.clone()))
                .unwrap_or_else(|error| panic!("{overrides}: {error}"));
            for name in values.keys() {
                let mut without = values.clone();
                without.remove(name);
                assert!(
                    serde_json::from_value::<ImagingRequest>(Value::Object(without)).is_err(),
                    "{overrides}: the request does not read {name}"
                );
            }
        }
    }

    /// A blank optional parameter is unset, as it always was.
    #[test]
    fn blank_optional_values_are_unset() {
        let blank = request(json!({
            "datacolumn": "",
            "phasecenter": "",
            "mask_image": "",
            "outlierfile": "",
            "restfreq": "",
            "scales": "",
            "mask_box": "",
            "field": "",
            "spw": "",
            "start": "",
        }));
        assert_eq!(blank, request(json!({})));
    }

    /// A parameter its gridder does not read is refused when set, and
    /// absent from the request when not.
    #[test]
    fn the_catalog_refuses_a_parameter_its_gridder_does_not_read() {
        let error = resolved(json!({ "gridder": "standard", "wprojplanes": 4 }))
            .expect_err("W planes without W projection");
        assert!(
            matches!(
                &error,
                casa_task_runtime::ResolveError::Invalid { parameter: Some(name), .. }
                    if name == "wprojplanes"
            ),
            "{error:?}"
        );
        let values = resolved(json!({ "gridder": "wproject", "wprojplanes": 4 })).expect("W");
        assert_eq!(values["wprojplanes"], json!(4));
        assert!(
            !resolved(json!({}))
                .expect("defaults")
                .contains_key("wprojplanes")
        );
    }

    #[test]
    fn stokes_names_stokes_parameters_or_correlations_in_casa_order() {
        use PolarizationCoordinate::*;
        for (text, coordinates) in [
            ("I", vec![StokesI]),
            ("VI", vec![StokesI, StokesV]),
            ("IQUV", vec![StokesI, StokesQ, StokesU, StokesV]),
            ("RRLL", vec![CircularRr, CircularLl]),
            ("XXYY", vec![LinearXx, LinearYy]),
            ("XXXYYXYY", vec![LinearXx, LinearXy, LinearYx, LinearYy]),
        ] {
            assert_eq!(
                request(json!({ "stokes": text })).stokes,
                coordinates,
                "{text}"
            );
        }
        for text in ["IXX", "XXRR", "IQZ", "X", ""] {
            let refused = resolved(json!({ "stokes": text })).map_or(true, |values| {
                serde_json::from_value::<ImagingRequest>(Value::Object(values)).is_err()
            });
            assert!(refused, "{text:?}");
        }
    }
}
