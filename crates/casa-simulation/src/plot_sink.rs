// SPDX-License-Identifier: LGPL-3.0-or-later

//! Domain-owned sink trait for simulation plot figure assembly.
//!
//! `casa-simulation` computes all physical data and passes it to an optional
//! [`SimulationPlotSink`] implemented by presentation crates (`casa-plot-data`),
//! keeping plot-document schemas out of the simulation library.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_types::measures::{MPosition, MeasuresProvider};

use crate::SessionResources;
use crate::simobserve::SimobserveError;

/// Physical inputs computed by `simobserve` for figure preparation.
pub struct SimobservePlotInput<'a> {
    /// Requested CASA graphics mode (`"file"`, `"screen"`, `"both"`).
    pub graphics_mode: &'a str,
    /// Project directory (`fileroot`).
    pub directory: &'a Path,
    /// Product stem (`<project>.<configuration>`).
    pub stem: &'a str,
    /// Whether observations were predicted (`int` or `sd`).
    pub predict: bool,
    /// Whether the observation is interferometric (`true`) or total power (`false`).
    pub uvmode: bool,
    /// Whether the sky model is component-only without a flat image input.
    pub components_only: bool,
    /// Component-list table path when supplied.
    pub complist: &'a str,
    /// Flattened sky model image path.
    pub sky_flat: &'a Path,
    /// Sky model centre direction string (`simutil` format).
    pub sky_refdir: &'a str,
    /// Reference frequency of the sky model, in GHz.
    pub sky_specrefval_ghz: f64,
    /// Pointing offsets relative to the sky model centre, in degrees.
    pub pointing_offsets_deg: Vec<[f64; 2]>,
    /// Median pointing centre direction string (`simutil` format).
    pub pointing_centre: String,
    /// Primary beam FWHM, in arcseconds.
    pub primary_beam_arcsec: f64,
    /// Antenna ITRF positions `[x, y, z]` in metres.
    pub antenna_positions: Vec<[f64; 3]>,
    /// Antenna dish diameters in metres.
    pub antenna_diameters: &'a [f64],
    /// Antenna pad/station names.
    pub antenna_names: &'a [String],
    /// Observatory reference position.
    pub observatory: &'a MPosition,
    /// Measures provider for ephemeris evaluation.
    pub measures: Arc<dyn MeasuresProvider>,
    /// Reference epoch MJD (TAI days).
    pub refdate_mjd: f64,
    /// Path to the MeasurementSet observed by this run.
    pub measurement_set: PathBuf,
    /// Path to the numerical `.quick.psf` image when produced.
    pub quick_psf: PathBuf,
    /// Execution resource policy for memory admission.
    pub resources: SessionResources,
}

/// Physical inputs computed by `simanalyze` for figure preparation.
pub struct SimanalyzePlotInput<'a> {
    /// Requested CASA graphics mode (`"file"`, `"screen"`, `"both"`).
    pub graphics_mode: &'a str,
    /// Project directory.
    pub directory: &'a Path,
    /// Active product stem (with `.feather` suffix stripped when naming plots).
    pub stem: &'a str,
    /// Root path prefix (`directory.join(stem)`).
    pub root: &'a Path,
    /// Sky model path (`plan.model`).
    pub model: &'a Path,
    /// MeasurementSet path (`plan.ms`).
    pub ms: &'a Path,
    /// Flattened sky model image path.
    pub model_flat: &'a Path,
    /// Whether synthesis inputs were present.
    pub has_synthesis: bool,
    /// Whether `image` was requested.
    pub image: bool,
    /// Whether `analyze` was requested.
    pub analyze: bool,
    /// Panel selection flags.
    pub showuv: bool,
    /// Panel selection flags.
    pub showpsf: bool,
    /// Panel selection flags.
    pub showmodel: bool,
    /// Panel selection flags.
    pub showconvolved: bool,
    /// Panel selection flags.
    pub showclean: bool,
    /// Panel selection flags.
    pub showresidual: bool,
    /// Panel selection flags.
    pub showdifference: bool,
    /// Panel selection flags.
    pub showfidelity: bool,
    /// Optional explicit output file path (used by `simalma` TP analysis).
    pub output_file_override: Option<PathBuf>,
    /// Execution resource policy for memory admission.
    pub resources: SessionResources,
}

/// Inputs for the four-panel ALMA/ACA/TP combination summary figure.
pub struct SimalmaCombinationInput<'a> {
    /// Project directory.
    pub directory: &'a Path,
    /// Project name.
    pub project: &'a str,
    /// Stem of the highest-resolution interferometer configuration.
    pub best_stem: String,
    /// Stem of the synthesis image used in combination.
    pub high_stem: String,
    /// Model preparation memory estimate in bytes.
    pub model_preparation_bytes: u64,
    /// Execution resource policy for memory admission.
    pub resources: SessionResources,
}

/// Finalization inputs emitted at the end of a `simalma` run.
pub struct SimalmaFinishInput<'a> {
    /// Requested CASA graphics mode (`"file"`, `"screen"`, `"both"`).
    pub graphics_mode: &'a str,
    /// Optional combination figure inputs when total-power + imaging ran.
    pub combination: Option<SimalmaCombinationInput<'a>>,
}

/// Callback sink receiving simulation figure inputs from `simobserve`,
/// `simanalyze`, and `simalma`.
pub trait SimulationPlotSink {
    /// Record figure inputs from a `simobserve` run.
    fn record_simobserve(&mut self, input: SimobservePlotInput<'_>) -> Result<(), SimobserveError>;

    /// Record figure inputs from a `simanalyze` run.
    fn record_simanalyze(&mut self, input: SimanalyzePlotInput<'_>) -> Result<(), SimobserveError>;

    /// Finalize figures at the end of a `simalma` run (including optional
    /// combination panel and `file`/`screen`/`both` normalization).
    fn finish_simalma(&mut self, input: SimalmaFinishInput<'_>) -> Result<(), SimobserveError>;
}
