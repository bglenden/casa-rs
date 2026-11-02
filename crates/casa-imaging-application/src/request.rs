// SPDX-License-Identifier: LGPL-3.0-or-later

//! The imaging request: the provider-contracts catalog's `tclean` surface,
//! resolved, as one typed record (plan section 5.8, decision D8).
//!
//! A request is made only from resolved catalog values (every route, the
//! command line and `--json-run` alike, resolves through the catalog), by
//! [`ImagingRequest::from_tclean`], which reads CASA's tclean parameters as
//! CASA's modes read them (`crate::tclean`); so nothing here restates a
//! default and a missing field is an error. Most fields are tclean's
//! parameters by name; the reader fills the few casa-rs holds in its own
//! form. An unset optional value is spelled `"none"` or left blank, except
//! for the scan, observation, antenna and timerange selectors, which only
//! an empty string leaves unset.
//! Parameters that one gridder reads live in that gridder's variant.

use std::{fmt, num::NonZeroUsize, path::PathBuf, str::FromStr};

use casa_imaging_model::{
    FixedRestoringBeam, HogbomIterationAccounting, PolarizationCoordinate, ProductNormalization,
    RestoringBeamPolicy, UvTaper,
};
use casa_imaging_operator::GridPrecision;
use casa_imaging_runtime::{Diagnostics, ResourcePolicy};
use casa_ms::CubeInterpolation;
use casa_types::measures::{doppler::DopplerRef, frequency::FrequencyRef};
use casa_types::quanta::{Quantity, Unit};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

/// One imaging run's parameters, as the catalog resolved them.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ImagingRequest {
    /// Task-only continuation, established by output preflight, never a catalog value.
    #[serde(skip)]
    pub(crate) tclean_restart: bool,
    /// Whether the continuation found an earlier model, as it was before the
    /// run wrote its own; never a catalog value.
    #[serde(skip)]
    pub(crate) continued_model: bool,
    /// A supplied region file with no regions means zero support, not no mask.
    /// Internal reader provenance only; this is never a catalog parameter.
    #[serde(default)]
    pub(crate) explicit_empty_mask: bool,
    /// The MeasurementSet.
    #[serde(deserialize_with = "single")]
    pub vis: PathBuf,
    /// Prefix of every image product; a relative name is resolved against
    /// the working directory, as CASA's is.
    #[serde(deserialize_with = "absolute")]
    pub imagename: PathBuf,
    /// Image size in pixels along x and y; one value sizes both, as CASA's
    /// `imsize` does.
    #[serde(deserialize_with = "pixel_pair")]
    pub imsize: [usize; 2],
    /// Cell along x and y in arcseconds; one value sizes both, as CASA's
    /// `cell` does.
    #[serde(deserialize_with = "cell_pair_arcsec")]
    pub cell: [f64; 2],
    /// The visibility column imaged; `None` takes `CORRECTED_DATA` when it
    /// exists and `DATA` otherwise. `CORRECTED_DATA` asked for on a
    /// MeasurementSet without it images `DATA`, as CASA's `tclean` does;
    /// casa-rs also logs a warning.
    #[serde(deserialize_with = "optional")]
    pub datacolumn: Option<DataColumn>,
    /// Where the final model is saved in the MeasurementSet: nowhere, its
    /// prediction in `MODEL_DATA`, or as CASA's virtual model.
    #[serde(deserialize_with = "save_model")]
    pub savemodel: SaveModel,
    /// A CASA outlier file naming further image domains.
    #[serde(deserialize_with = "optional")]
    pub outlierfile: Option<PathBuf>,
    /// Field selector: IDs/ranges, exact names and simple `*` name patterns.
    /// Resolved once against FIELD metadata during compilation; `None`
    /// selects every field.
    #[serde(deserialize_with = "optional")]
    pub field: Option<String>,
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
    /// CASA numeric scan-number selection, as given; resolved against the
    /// observation during compilation. Only an exactly empty selector is
    /// unset, so whitespace-only text and `none` reach the selector's
    /// refusal, as CASA's tclean refuses them.
    #[serde(deserialize_with = "exact_selector")]
    pub scan: Option<String>,
    /// CASA numeric observation-ID selection, read as `scan` is.
    #[serde(deserialize_with = "exact_selector")]
    pub observation: Option<String>,
    /// CASA baseline selection by antenna (`antenna`), read as `scan` is;
    /// resolved against the observation's antennas during compilation.
    #[serde(deserialize_with = "exact_selector")]
    pub antenna: Option<String>,
    /// CASA time selection (`timerange`), read as `scan` is; resolved
    /// against the observation's times during compilation.
    #[serde(deserialize_with = "exact_selector")]
    pub timerange: Option<String>,
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
    /// Rest frequencies in Hz, CASA's `restfreq` list. The first is a
    /// cube's velocity axis's and the image header's; CASA lists the others
    /// after it in the header's spectral coordinate. Empty takes the
    /// source's line (`SynthesisParamsImage::buildCoordinateSystemCore`).
    #[serde(deserialize_with = "rest_frequencies_hz")]
    pub restfreq: Vec<f64>,
    /// A continuum image's reference frequency in Hz (`reffreq`): the
    /// header's spectral reference and an `mtmfs` Taylor expansion's
    /// reference. `None` (empty or zero) takes the band's midpoint, as
    /// `SynthesisParamsImage::getImFreq` does; a cube reads but ignores it.
    #[serde(deserialize_with = "reference_frequency_hz")]
    pub reffreq: Option<f64>,
    /// One restoring beam per plane, one common beam, or a beam given by
    /// its axes and angle.
    #[serde(deserialize_with = "restoring_beam")]
    pub restoringbeam: RestoringBeamPolicy,
    /// Restore the model with the restoring beam and write `.image` and,
    /// with `pbcor`, `.image.pbcor` (and the spectral index of `mtmfs`);
    /// CASA's `restoration=False` writes none of them (`task_tclean.py`).
    pub restoration: bool,
    /// CASA's `calcres`: form the residual with a major cycle before any
    /// minor cycle. `false` takes the residual an earlier run left at
    /// `imagename` and runs no initial major cycle; with no residual there,
    /// a run of no iterations makes the PSF alone (`task_tclean.py`).
    pub calcres: bool,
    /// CASA's `calcpsf`: grid the PSF, its sums of weights and the primary
    /// beam. `false` takes the PSF and sums of weights an earlier run left
    /// at `imagename` (`SIImageStore` opens existing images).
    pub calcpsf: bool,
    /// Whether a cube's density weighting is per output channel.
    pub perchanweightdensity: bool,
    /// Total minor-cycle iterations; zero writes the dirty products only.
    pub niter: usize,
    /// Absolute stopping threshold in Jy/beam.
    #[serde(deserialize_with = "threshold_jy")]
    pub threshold: f64,
    /// Major-cycle limit after the initial pass; zero is initial-only,
    /// and `None` is CASA's `nmajor = -1`.
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
    /// Auto-multithresh smoothing of each new mask, in restoring beams.
    pub smoothfactor: f64,
    /// Auto-multithresh cut of each smoothed mask, as a fraction of its
    /// peak.
    pub cutthreshold: f64,
    /// Whether auto-multithresh prunes the grown mask as it prunes the
    /// threshold mask.
    pub dogrowprune: bool,
    /// Percent change of an auto-multithresh plane's mask below which it
    /// stops evolving once the global threshold is reached; negative never
    /// stops on change.
    pub minpercentchange: f64,
    /// Log CASA's automask summary at every auto-multithresh update.
    pub verbose: bool,
    /// Estimate the noise of `nsigma` and auto-multithresh from every
    /// valid pixel (`true`), or robustly, outside the clean mask or by
    /// Chauvenet's criterion (`false`).
    pub fastnoise: bool,
    /// Inclusive signed pixel boxes `[x0, y0, x1, y1]`, clipped to the image.
    #[serde(deserialize_with = "mask_boxes")]
    pub mask_box: Vec<[i64; 4]>,
    /// A CASA image whose non-zero pixels may be cleaned.
    #[serde(deserialize_with = "optional")]
    pub mask_image: Option<PathBuf>,
    /// CASA's `startmodel`: an image in Jy/pixel that the model starts from
    /// (`SIImageStore::setModelImageOne`).
    #[serde(deserialize_with = "optional")]
    pub startmodel: Option<PathBuf>,
    /// Visibility weighting.
    pub weighting: Weighting,
    /// Briggs robustness.
    pub robust: f64,
    /// CASA's `npixels` as `SynthesisImagerVi2::weight` reads it for the
    /// weighting: the side of a continuum uniform or Briggs density grid,
    /// or the half-width of the box each sample's density spreads over
    /// (`superuniform`, where CASA's zero or less is 3, and a cube's
    /// per-channel density); 0 is the image's own cells, and the value of
    /// a weighting without a density.
    pub npixels: usize,
    /// CASA's `noise` in Jy, the visibility noise of `briggsabs`; `None`
    /// for every other weighting, which does not read it.
    #[serde(deserialize_with = "optional")]
    pub noise: Option<f64>,
    /// CASA's `uvtaper`, a Gaussian taper of the imaging weights, read as
    /// `VisImagingWeight::setFilter` reads it: an angle is the image-plane
    /// FWHM and wavelengths are the uv-plane HWHM. `None` when it is empty
    /// or its first entry is.
    #[serde(deserialize_with = "uv_taper")]
    pub uvtaper: Option<UvTaper>,
    /// The convolution-function set, with the parameters only it reads.
    #[serde(flatten)]
    pub gridder: Gridder,
    /// Write the primary-beam image. The standard and W-projection
    /// gridders form an analytic beam only for it or `pbcor`: without
    /// either they write no `.pb` and no primary-beam pixel mask, unlike
    /// CASA (#217).
    pub write_pb: bool,
    /// Write the primary-beam-corrected image.
    pub pbcor: bool,
    /// Signed CASA `pblimit`: its magnitude bounds the primary-beam support
    /// and pixel mask; a negative value leaves uncorrected products
    /// unmasked.
    pub pblimit: f64,
    /// Line-free channels of a visibility-domain continuum fit, for
    /// example `0:0~239;281~383`.
    #[serde(deserialize_with = "optional")]
    pub fitspw: Option<String>,
    /// Polynomial order of the continuum fit.
    pub fitorder: usize,
    /// Write the continuum-subtracted visibilities to `CORRECTED_DATA`.
    pub save_continuum_residual: bool,
    /// Use the host's workers (the balanced policy) instead of one: as many
    /// as fit the run's memory, which the run summary records (#737).
    pub parallel: bool,
    /// Where the major-cycle passes grid, resolved against the compiled
    /// problem and the host by [`crate::availability::resolve_backend`].
    pub backend: BackendRequest,
    /// Grid arithmetic; `None` (`auto`) is plan decision D2's rule.
    #[serde(deserialize_with = "grid_precision")]
    pub gridprecision: Option<GridPrecision>,
    /// The optional diagnostic output: `stage_timing`,
    /// `metal_stage_timing` and `science_trace`.
    #[serde(flatten)]
    pub diagnostics: Diagnostics,
}

/// The device a request asks to grid on (`backend`).
///
/// `auto` takes the host's GPU for what it implements and has been checked
/// on, end to end, against the CPU; everything else grids on the CPU. The
/// GPU accumulates in `f32` (plan decision D2), so choosing it accepts
/// single-precision grids, and an explicit `gridprecision = f64` keeps
/// `auto` on the CPU.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendRequest {
    /// The GPU where it runs the problem, else the CPU.
    #[default]
    Auto,
    /// The CPU.
    Cpu,
    /// The host's GPU (Metal on macOS); a problem it cannot run is refused.
    Gpu,
}

/// Where tclean's `savemodel` saves the final model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SaveModel {
    /// Nowhere (`'none'`).
    #[default]
    None,
    /// Its prediction, written to `MODEL_DATA` (`'modelcolumn'`).
    ModelColumn,
    /// CASA's virtual model, the gridded model and the record of the
    /// machine that predicts from it, in the MeasurementSet's keywords or
    /// SOURCE (`'virtual'`, [`mod@casa_ms::virtual_model`]).
    Virtual,
}

/// Visibility column imaged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataColumn {
    /// `DATA`.
    Data,
    /// `CORRECTED_DATA`, else `DATA` when the MeasurementSet has none.
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
    /// Uniform weighting over a density summed in a box about each sample
    /// (`npixels`).
    Superuniform,
    /// Each weight times its sample's uv distance.
    Radial,
    /// Briggs weighting at `robust`.
    Briggs,
    /// Briggs weighting at `robust` and the visibility `noise`, unnormalized
    /// (CASA `rmode = "abs"`).
    Briggsabs,
    /// Briggs weighting with CASA's bandwidth taper at `robust`.
    Briggsbwtaper,
}

impl Weighting {
    /// Whether the weights divide by a gridded visibility density.
    #[must_use]
    pub const fn uses_density(self) -> bool {
        !matches!(self, Self::Natural | Self::Radial)
    }

    /// Whether a cube with `perchanweightdensity` grids that density per
    /// channel (CASA's `BriggsCubeWeightor`): `superuniform` always grids
    /// one density over the selection (`VisImagingWeight`), as
    /// `SynthesisImagerVi2::weight` does.
    #[must_use]
    pub const fn per_channel_density(self) -> bool {
        self.uses_density() && !matches!(self, Self::Superuniform)
    }
}

/// Convolution-function set (`gridder`) and the parameters only it reads.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "gridder", rename_all = "lowercase")]
pub enum Gridder {
    /// The prolate-spheroidal kernel.
    Standard {
        /// Facets along each axis of the main image (CASA's `facets`): each
        /// is gridded about its own centre.
        facets: NonZeroUsize,
    },
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
    /// Bound on resident convolution-function cells, in MiB; `None`
    /// (`auto`) sizes the cache once the run has chosen its workers, from
    /// the memory their fit leaves free (#449).
    #[serde(deserialize_with = "auto_or")]
    pub cf_resident_mb: Option<usize>,
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
    /// Check that the request can continue on what an earlier run left at
    /// its `imagename`, as tclean's `restart=True` does (#944): the same
    /// run's family of products, a model with no `startmodel`, a CLEAN mask
    /// with no new one, and what `calcpsf=False` and `calcres=False` read.
    /// Task entry points call this before creating a run summary.
    ///
    /// # Errors
    ///
    /// An existing product the request does not continue on, an outlier
    /// field's existing product, an unreadable output directory, an invalid
    /// outlier-name file, or an earlier PSF or residual that is missing.
    pub fn check_tclean_output_prefixes(&self) -> Result<(), crate::PrepareError> {
        crate::restart::check(self)
    }

    /// Clone this request with task continuation enabled after output preflight.
    /// Lower-level execution of the original request retains atomic replacement.
    ///
    /// # Errors
    /// An unsupported existing product or a conflicting explicit start model.
    pub fn with_tclean_restart(&self) -> Result<Self, crate::PrepareError> {
        self.check_tclean_output_prefixes()?;
        let mut request = self.clone();
        request.tclean_restart = !crate::restart::products(&self.imagename)?.is_empty();
        request.continued_model = crate::restart::continues_model(&request);
        Ok(request)
    }

    /// Apply CASA's task output policy before summary creation or execution.
    /// `restart=true` continues on the products at `imagename`; `false`
    /// selects the next single-main-image namespace and protects every
    /// existing chosen product.
    ///
    /// # Errors
    /// Unsupported coordinated outlier numbering, invalid numbered names,
    /// unreadable directories, or an occupied chosen scientific namespace.
    pub fn with_tclean_output_policy(&self, restart: bool) -> Result<Self, crate::PrepareError> {
        if restart {
            return self.with_tclean_restart();
        }
        if let Some(path) = &self.outlierfile {
            return Err(crate::PrepareError::Restart {
                path: path.clone(),
                reason: "restart=False coordinated outlier numbering is unfinished (#944)".into(),
            });
        }
        let mut request = self.clone();
        request.imagename = crate::restart::next_prefix(&self.imagename)?;
        crate::restart::check_prefix(&request.imagename)?;
        // A new namespace holds no PSF or residual for calcpsf or calcres to
        // leave standing.
        crate::restart::check_inputs(&request, false)?;
        request.tclean_restart = false;
        Ok(request)
    }

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
        if self.imsize.contains(&0) || self.cell.iter().any(|cell| *cell <= 0.0) {
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
        if self.startmodel.is_some() && self.deconvolver == Deconvolver::Mtmfs {
            return invalid(
                "startmodel, deconvolver",
                "an mtmfs start model is one image per Taylor term, which is not supported yet",
            );
        }
        if self.startmodel.is_some() && self.outlierfile.is_some() {
            return invalid(
                "startmodel, outlierfile",
                "a start model with outlier fields is not supported yet: an outlier would own the \
                 main field's overlap with no start model of its own",
            );
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
        if self.solves() && self.minor_cycle_length == 0 {
            return invalid(
                "minor_cycle_length",
                "a solving run needs minor-cycle iterations",
            );
        }
        // A continuation predicts the model an earlier run left, as CASA's
        // `predictModel` does for `niter=0, calcres=False`.
        if self.savemodel != SaveModel::None
            && !self.solves()
            && self.startmodel.is_none()
            && !self.tclean_restart
        {
            return invalid(
                "savemodel, niter",
                "MODEL_DATA needs a solved model or a start model",
            );
        }
        if self.savemodel == SaveModel::Virtual {
            self.validate_virtual_model()?;
        }
        let has_boxes = !self.mask_box.is_empty() || self.explicit_empty_mask;
        if (self.mask_image.is_some() && has_boxes)
            || (self.usemask == UseMask::AutoMultithresh
                && (self.mask_image.is_some() || has_boxes))
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

    /// What `savemodel='virtual'` saves: the standard gridder's model
    /// (CASA's `GridFT`, or `MultiTermFTNew` over it) of one Stokes I
    /// image. CASA also saves W-projection and mosaic machines, other
    /// polarizations, facets and outlier fields, which wait for #823.
    fn validate_virtual_model(&self) -> Result<(), InvalidRequest> {
        let invalid = |parameters, reason| Err(InvalidRequest { parameters, reason });
        if !matches!(self.gridder, Gridder::Standard { facets } if facets.get() == 1) {
            return invalid(
                "savemodel, gridder",
                "savemodel='virtual' saves the standard gridder's model of one facet; other \
                 gridders and facets are not implemented by casa-rs yet (#823)",
            );
        }
        if self.stokes != [PolarizationCoordinate::StokesI] {
            return invalid(
                "savemodel, stokes",
                "savemodel='virtual' saves a Stokes I model; other polarizations are not \
                 implemented by casa-rs yet (#823)",
            );
        }
        if self.outlierfile.is_some() {
            return invalid(
                "savemodel, outlierfile",
                "savemodel='virtual' saves the main image's model; outlier fields are not \
                 implemented by casa-rs yet (#823)",
            );
        }
        Ok(())
    }

    /// Whether the run solves for a model: `mtmfs` always, the other
    /// solvers unless the run is dirty (`niter = 0`).
    #[must_use]
    pub fn solves(&self) -> bool {
        self.deconvolver == Deconvolver::Mtmfs || self.niter > 0
    }

    /// Whether the model starts from an image rather than empty: a
    /// `startmodel`, or the model an earlier run left at `imagename` that a
    /// continuation reads (#944).
    #[must_use]
    pub fn starts_from_model(&self) -> bool {
        self.startmodel.is_some() || self.continued_model
    }

    /// Total minor-cycle iterations.
    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.niter
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

/// A selector's exact text, `None` only when it is exactly empty.
fn exact_selector<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::String(text) if text.is_empty() => Ok(None),
        Value::String(text) => Ok(Some(text)),
        value => Err(unexpected(&value, "text")),
    }
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

/// `auto` as `None`, else the value as text or a number.
fn auto_or<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    match Value::deserialize(deserializer)? {
        Value::String(text) if text.trim() == AUTO => Ok(None),
        Value::String(text) => text.trim().parse().map(Some).map_err(D::Error::custom),
        Value::Number(number) => number
            .to_string()
            .parse()
            .map(Some)
            .map_err(D::Error::custom),
        value => Err(unexpected(&value, "auto, text or a number")),
    }
}

/// The catalog's spelling of a value the run chooses itself.
const AUTO: &str = "auto";

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

/// The x and y values of a pair, or one value for both, given alone or as a
/// one-element list (CASA's `SynthesisParamsImage::fromRecord`).
fn pair<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[Value; 2], D::Error> {
    match Value::deserialize(deserializer)? {
        Value::Array(mut items) if items.len() == 1 => {
            let value = items.remove(0);
            Ok([value.clone(), value])
        }
        Value::Array(items) => <[Value; 2]>::try_from(items).map_err(|items| {
            D::Error::custom(format!("expected one or two axes, found {}", items.len()))
        }),
        value => Ok([value.clone(), value]),
    }
}

fn pixel_pair<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[usize; 2], D::Error> {
    let side = |value: &Value| {
        value
            .as_u64()
            .and_then(|side| usize::try_from(side).ok())
            .ok_or_else(|| unexpected(value, "a pixel count"))
    };
    let [x, y] = pair(deserializer)?;
    Ok([side(&x)?, side(&y)?])
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

fn cell_pair_arcsec<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[f64; 2], D::Error> {
    let [x, y] = pair(deserializer)?;
    let arcsec = |value: &Value| quantity(value, "arcsec").map_err(D::Error::custom);
    Ok([arcsec(&x)?, arcsec(&y)?])
}

/// CASA's `uvtaper` (`synthesisimager::setweighting`, then
/// `VisImagingWeight::setFilter`): `[bmaj]`, `[bmaj, bmin]` or
/// `[bmaj, bmin, bpa]`, with `bmin = bmaj` and `bpa = 0` when not given,
/// and no taper when the list or its first entry is empty. In wavelengths
/// (`lambda`, `klambda`, …) the axes are the taper's uv-plane HWHM; as
/// angles they are its image-plane FWHM θ, whose uv-plane HWHM is
/// 2 ln 2 / (π θ). CASA reads an axis with no unit as radians and tapers
/// every weight to zero; casa-rs refuses it (`docs/CASA (C++) bugs.md`).
fn uv_taper<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<UvTaper>, D::Error> {
    let entries = match Value::deserialize(deserializer)? {
        Value::Array(items) => items,
        value => vec![value],
    };
    let texts = entries
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::trim)
                .ok_or_else(|| unexpected(entry, "a quantity"))
        })
        .collect::<Result<Vec<_>, D::Error>>()?;
    let taper = match texts.as_slice() {
        [] | ["", ..] => return Ok(None),
        [bmaj] => uv_taper_axes(bmaj, bmaj, None),
        [bmaj, bmin] => uv_taper_axes(bmaj, bmin, None),
        [bmaj, bmin, bpa] => uv_taper_axes(bmaj, bmin, Some(bpa)),
        _ => Err("uvtaper takes at most [bmaj, bmin, bpa]".to_string()),
    };
    taper.map(Some).map_err(D::Error::custom)
}

/// One `uvtaper` axis: its uv-plane HWHM in wavelengths, and whether it was
/// given as an angle.
fn uv_taper_axis(text: &str) -> Result<(f64, bool), String> {
    let refused = || {
        format!(
            "uvtaper {text:?}: give an angle (the image-plane FWHM) or wavelengths (the uv-plane \
             HWHM, lambda or klambda)"
        )
    };
    let quantity: Quantity = text.parse().map_err(|_| refused())?;
    let unit = |name: &str| Unit::from_str(name).expect("a casacore unit");
    let hwhm = if let Ok(wavelengths) = quantity.get_value_in(&unit("lambda")) {
        (wavelengths, false)
    } else if let Ok(fwhm) = quantity.get_value_in(&unit("rad")) {
        (
            2.0 * std::f64::consts::LN_2 / (std::f64::consts::PI * fwhm),
            true,
        )
    } else {
        return Err(refused());
    };
    if quantity.unit().name().is_empty() || !(hwhm.0.is_finite() && hwhm.0 > 0.0) {
        return Err(refused());
    }
    Ok(hwhm)
}

fn uv_taper_axes(bmaj: &str, bmin: &str, bpa: Option<&str>) -> Result<UvTaper, String> {
    let (major, major_angle) = uv_taper_axis(bmaj)?;
    let (minor, minor_angle) = uv_taper_axis(bmin)?;
    if major_angle != minor_angle {
        return Err(format!(
            "uvtaper {bmaj:?}, {bmin:?}: give both axes as angles or both in wavelengths"
        ));
    }
    let position_angle_rad = match bpa {
        None => 0.0,
        Some(text) => text
            .parse::<Quantity>()
            .ok()
            .filter(|angle| !angle.unit().name().is_empty())
            .and_then(|angle| angle.get_value_in(&Unit::from_str("rad").ok()?).ok())
            .ok_or_else(|| format!("uvtaper position angle {text:?} needs an angle unit"))?,
    };
    Ok(UvTaper::new(major, minor, position_angle_rad))
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

/// Boxes `x0,y0,x1,y1` separated by `;`.
fn mask_boxes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<[i64; 4]>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let text = value
        .as_str()
        .ok_or_else(|| unexpected(&value, "pixel boxes"))?;
    if unset(text) {
        return Ok(Vec::new());
    }
    text.split(';')
        .map(|corners| {
            comma_list::<i64>(corners)?
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

fn save_model<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SaveModel, D::Error> {
    match choice(deserializer)?.as_str() {
        text if unset(text) => Ok(SaveModel::None),
        "modelcolumn" => Ok(SaveModel::ModelColumn),
        "virtual" => Ok(SaveModel::Virtual),
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

/// `reffreq` as `SynthesisParamsImage::fromRecord` reads it: casacore's
/// quantity reader, in Hz; empty or zero is unset (`getImFreq`). A value
/// without a frequency unit is refused, as CASA's conversion to Hz fails.
fn reference_frequency_hz<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<f64>, D::Error> {
    let text = match Value::deserialize(deserializer)? {
        Value::String(text) => text,
        value => return Err(unexpected(&value, "a frequency")),
    };
    if unset(&text) {
        return Ok(None);
    }
    let quantity = text
        .trim()
        .parse::<Quantity>()
        .map_err(|error| D::Error::custom(format!("reffreq {text:?}: {error}")))?;
    let hz = Unit::new("Hz").expect("Hz is a casacore unit");
    let value = quantity.get_value_in(&hz).map_err(|error| {
        D::Error::custom(format!("reffreq {text:?} is not a frequency: {error}"))
    })?;
    if !value.is_finite() || value < 0.0 {
        return Err(D::Error::custom(format!(
            "reffreq {text:?} is not a frequency"
        )));
    }
    Ok((value != 0.0).then_some(value))
}

/// Rest frequencies: one text, a list of texts, or unset.
fn rest_frequencies_hz<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f64>, D::Error> {
    let items = match Value::deserialize(deserializer)? {
        Value::Array(items) => items,
        value => vec![value],
    };
    let mut frequencies = Vec::with_capacity(items.len());
    for item in &items {
        match item.as_str() {
            Some(text) if unset(text) => {}
            Some(text) => {
                frequencies.push(casa_ms::parse_rest_frequency_hz(text).map_err(D::Error::custom)?)
            }
            None => return Err(unexpected(item, "rest frequencies")),
        }
    }
    Ok(frequencies)
}

/// `none` (each plane's fitted beam), `common`, or a beam's
/// `[major, minor, angle]`, read as `SynthesisParamsDeconv::fromRecord`
/// reads `restoringbeam`'s quantities: each with casacore's quantity
/// reader, as casacore's `GaussianBeam::setMajorMinor` and `setPA` accept
/// them (angular units, axes neither negative nor inverted). A zero axis is
/// casacore's null beam, which restores each plane with its fitted beam.
fn restoring_beam<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<RestoringBeamPolicy, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::String(text) if unset(&text) => Ok(RestoringBeamPolicy::PerPlane),
        Value::String(text) if text == "common" => Ok(RestoringBeamPolicy::Common),
        Value::Array(items) => {
            let texts = items
                .iter()
                .map(|item| item.as_str().ok_or_else(|| unexpected(item, "text")))
                .collect::<Result<Vec<_>, D::Error>>()?;
            let [major, minor, angle] = texts.as_slice() else {
                return Err(D::Error::custom(format!(
                    "restoringbeam {texts:?}: expected its major and minor axes and angle"
                )));
            };
            given_restoring_beam([major, minor, angle]).map_err(|_| {
                D::Error::custom(format!(
                    "Cannot construct a restoringbeam from supplied parameters {texts:?}: give \
                     angular axes, the major axis at least the minor, and an angular position \
                     angle, as CASA's tclean requires"
                ))
            })
        }
        value => Err(unexpected(
            &value,
            "none, common or a beam's axes and angle",
        )),
    }
}

/// A beam's `[major, minor, angle]` quantities, or the fitted beams for
/// casacore's null beam.
fn given_restoring_beam(texts: [&str; 3]) -> Result<RestoringBeamPolicy, ()> {
    let radians = Unit::from_str("rad").expect("a casacore unit");
    let [major, minor, angle] = texts.map(|text| {
        Quantity::read(text)
            .ok()
            .and_then(|quantity| quantity.get_value_in(&radians).ok())
            .filter(|value| value.is_finite())
            .ok_or(())
    });
    let [major, minor, angle] = [major?, minor?, angle?];
    if major < 0.0 || minor < 0.0 || major < minor {
        return Err(());
    }
    if major == 0.0 || minor == 0.0 {
        return Ok(RestoringBeamPolicy::PerPlane);
    }
    FixedRestoringBeam::new(major, minor, angle)
        .map(RestoringBeamPolicy::Fixed)
        .map_err(|_| ())
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
pub(crate) mod tests;
