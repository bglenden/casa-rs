// SPDX-License-Identifier: LGPL-3.0-or-later
//! The minor cycle of one major-cycle completion: its planes read from the
//! normal state as deconvolution views, solved on the worker team, and
//! their components returned as model terms.
//!
//! A minor cycle has two halves around the run's [`Controller`]:
//! [`prepare_minor_cycle`] forms the masks and measures every plane (CASA's
//! `initminorcycle`), and [`run_minor_cycle`] cleans every plane under the
//! controls the controller derived from those measurements.
//!
//! Each half reads its planes from the normal state itself, so a plane is
//! read twice per cycle. A paged cube is not held in memory between the
//! halves; only the planes in flight on the team are.
//!
//! [`Controller`]: casa_imaging_deconvolution::Controller

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use casa_imaging_deconvolution::{
    AutoMaskEvidence, AutoMaskPlane, AutoMaskSource, DenseModelPlane, FinalNormalState,
    ImageDomainReconstructionMaskPlans, ImageDomainReconstructionMasks, MajorCycleCompletion,
    MaskError, MaskPlaneKey, MaskSources, ModelGeneration, ModelLifecycle, ModelLifecycleError,
    NoiseEstimate, NormalBasis, NormalStateError, SpectralChannelValidity, robust_noise,
};
use casa_imaging_deconvolution::{
    Clark, Component, CycleControls, Delta, Hogbom, LinearRefresh, MinorCycleView, Multiscale,
    PlaneOutcome, PlaneShape, PlaneStatistics, PlaneStop, PsfSummary, ResidualStatistics,
    RestoringBeam, Solver, Support, Taylor, peak_magnitude, run_plane,
};
use casa_imaging_model::{
    HogbomIterationAccounting, ModelCell, ModelDeltaTerm, ModelSupport, ModelValue,
    ReconstructionAlgorithm,
};
use casa_imaging_products::{
    AnalyticPrimaryBeam, MinorCycleImageResponse, MosaicSensitivity, ProductsError, ResidualFamily,
};

use crate::pass::WorkerTeam;
use crate::{Admission, Demand, HostResources, Reservation, ResourcePolicy, admit};

mod beams;
mod channel;
mod layout;
mod mem;
mod pixels;
mod polarized;
mod prepare;
mod psfs;
mod rows;
pub use layout::{
    MinorLayout, mask_bytes, prepare_bytes, prepare_bytes_for, psf_cache_bytes, run_bytes,
    run_bytes_for,
};
use mem::{dense_result_bytes, dense_result_slots, solve_mem};
pub use prepare::{prepare_minor_cycle, prepare_minor_cycle_with_residual_mask};
use psfs::{measure_psfs, saved_beam, saved_psf_beam, single_psf_peak};
pub use rows::PlaneCycleRow;

/// Component records kept per minor cycle for diagnostics.
const TRACE: usize = 64;

/// FFTW threads a plane's scale transforms take when a minor cycle cleans
/// `planes` planes on `workers` workers: a plane cleaned alone has the
/// team's (#449); planes cleaned side by side, one each. The solve and its
/// charge ([`run_bytes_for`]) both take it from here.
const fn scale_fft_threads(planes: usize, workers: usize) -> usize {
    if planes == 1 { workers } else { 1 }
}

/// A failure of the minor cycle.
#[derive(Debug, thiserror::Error)]
pub enum MinorCycleRunError {
    /// The reconstruction masks could not be formed.
    #[error("reconstruction mask: {0}")]
    Mask(#[from] MaskError),
    /// A normal-state plane could not be read.
    #[error("normal state: {0}")]
    Normal(#[from] NormalStateError),
    /// The model support could not be read or a term is out of range.
    #[error("model: {0}")]
    Model(#[from] ModelLifecycleError),
    /// The direction-dependent normalization could not convert a value.
    #[error("normalization: {0}")]
    Response(#[from] ProductsError),
    /// The solver failed.
    #[error(transparent)]
    Solver(#[from] casa_imaging_deconvolution::Error),
    /// A saved beam grid does not cover the image's channels and polarizations.
    #[error("saved beam grid has {actual} planes; expected {expected}")]
    SavedBeamGridLength {
        /// Number of beam planes required.
        expected: usize,
        /// Number of beam planes supplied.
        actual: usize,
    },
    /// The selected algorithm has no minor cycle.
    #[error("{0:?} has no minor cycle")]
    Algorithm(ReconstructionAlgorithm),
    /// A plane's terms do not fit what the policy leaves free.
    #[error(transparent)]
    Admission(#[from] Admission),
}

/// What every minor cycle of a run shares.
#[derive(Clone, Debug)]
pub struct MinorCycleSetup<'a> {
    /// The PSF main-lobe fit cutoff.
    pub psf_cutoff: f32,
    /// A complete saved PSF beam grid in pixels, channel-major, for the main image.
    pub psf_beams: Option<Arc<[RestoringBeam]>>,
    /// Saved residual beams in pixels, preferred by automatic masking.
    pub mask_beams: Option<Arc<[RestoringBeam]>>,
    /// The deconvolver.
    pub algorithm: ReconstructionAlgorithm,
    /// Högbom's iteration accounting.
    pub accounting: HogbomIterationAccounting,
    /// CASA's flat-noise or flat-sky residual units, for a direction-dependent
    /// response (mosaic, A-projection).
    pub response: Option<MinorCycleImageResponse>,
    /// Whether the current Taylor residual was read in its saved image units.
    pub saved_taylor_residual: bool,
    /// The `nsigma` multiplier; zero when off.
    pub nsigma: f64,
    /// How the `nsigma` threshold's noise is estimated (`fastnoise`).
    pub noise: NoiseEstimate,
    /// Whether the mask is CASA's automatic multi-threshold mask, whose
    /// n-sigma threshold adds the residual median.
    pub automask: bool,
    /// CASA's `.pb` of a run without a response (the standard and
    /// W-projection gridders): the telescope's analytic beam, whose
    /// support bounds what each plane's statistics read. `None` under a
    /// response, whose beam is each plane's weight image, and for a run
    /// without a `.pb`, whose statistics read every valid pixel.
    pub primary_beam: Option<AnalyticPrimaryBeam<'a>>,
    /// Whether the residual carries the `.pb`'s support as its pixel mask,
    /// as tclean's does where the run has a `.pb` and `pblimit` is not
    /// negative (`SIImageStore::divideResidualByWeight`): each plane's
    /// peaks and automatic mask then read only the pixels inside the beam.
    pub residual_pixel_mask: bool,
    /// tclean's `pbmask`, in `[0, 1)`: the `nsigma` noise reads the pixels
    /// whose `.pb` exceeds it (`SIImageStore::calcRobustRMS`), and above
    /// zero it is an automatic mask's pixel mask
    /// (`SDMaskHandler::autoMaskWithinPB`).
    pub pbmask: f64,
}

/// One plane of the normal state: image domain, absolute output channel
/// (the Taylor family's first channel) and polarization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PlaneKey {
    domain: usize,
    channel: usize,
    polarization: usize,
}

/// The PSF measurements of a run, made once per plane: the PSF does not
/// change after the initial major cycle. Clark's refresh of a single-plane
/// run is kept with them.
#[derive(Default)]
pub struct PsfCache {
    reuses_single_psf: bool,
    summaries: BTreeMap<PlaneKey, PsfSummary>,
    clark: Option<LinearRefresh>,
    /// Whether each image domain's `.pb` keeps a pixel in some plane,
    /// found at the first boundary whose `nsigma` noise reads the beam:
    /// `calcRobustRMS` falls back to every pixel only for an image the
    /// beam leaves none of (CAS-13318). The beam does not change.
    beams: Option<Box<[bool]>>,
}

impl PsfCache {
    /// The measurements of a plane, once measured.
    #[must_use]
    pub fn summary(
        &self,
        domain: usize,
        channel: usize,
        polarization: usize,
    ) -> Option<&PsfSummary> {
        self.summaries.get(&PlaneKey {
            domain,
            channel,
            polarization,
        })
    }

    /// Heap bytes the cached Clark refresh holds, which outlives each minor
    /// cycle ([`LinearRefresh::bytes`]); zero when none is cached.
    #[must_use]
    pub fn refresh_bytes(&self, completion: &MajorCycleCompletion) -> u64 {
        self.clark.as_ref().map_or(0, |_| {
            let [nx, ny] = completion.normal_state().shape();
            LinearRefresh::bytes(PlaneShape::new(nx, ny))
        })
    }
}

/// Bytes the model terms of one solved plane hold, charged as the plane
/// finishes ([`run_minor_cycle`]): for each of `terms` terms its record and
/// its copy while the cycle merges every plane's terms in model order,
/// which also covers what the model holds per term while it queues them
/// ([`ModelLifecycle::QUEUED_TERM_BYTES`]); and the plane's component
/// records.
#[must_use]
pub const fn result_bytes(terms: usize) -> u64 {
    let entry = size_of::<(usize, ModelDeltaTerm)>();
    let merged = 2 * entry;
    let queued = size_of::<ModelDeltaTerm>() + ModelLifecycle::QUEUED_TERM_BYTES;
    let per_term = if merged > queued { merged } else { queued };
    (terms * per_term + TRACE * (size_of::<Component>() + size_of::<TracedComponent>())) as u64
}

/// One minor cycle's masks and plane measurements.
pub struct PreparedMinorCycle {
    masks: ImageDomainReconstructionMasks,
    auto_masks: Box<[Option<AutoMaskEvidence>]>,
    planes: Vec<(PlaneKey, PlaneStatistics)>,
    /// The planes with no data, measured as empty.
    blank: Vec<(PlaneKey, PlaneStatistics)>,
    /// A single plane's load, kept for its solve ([`Self::retained_bytes`]).
    retained: Option<RetainedPlane>,
    /// The measurements of every plane and field, for the controller.
    pub statistics: ResidualStatistics,
}

/// The load of a cycle's only plane, without a direction-dependent
/// response: preparation measures it and the solve cleans it, so it is read
/// once a cycle.
struct RetainedPlane {
    key: PlaneKey,
    residual: Vec<Vec<f64>>,
    psf: Vec<Vec<f64>>,
    psf_peak: f64,
    support: Support,
    valid: Support,
}

impl PreparedMinorCycle {
    /// The masks the cycle cleans within.
    #[must_use]
    pub const fn masks(&self) -> &ImageDomainReconstructionMasks {
        &self.masks
    }

    /// Automatic-mask diagnostics per image domain.
    #[must_use]
    pub fn auto_masks(&self) -> &[Option<AutoMaskEvidence>] {
        &self.auto_masks
    }

    /// Bytes the plane kept for the solve holds: its residual and PSF terms
    /// and its two supports, within what [`prepare_bytes`] charged for its
    /// load. The cycle's [`run_bytes`] charges a load the solve then skips,
    /// so a caller moves this much of the preparation's charge to the solve
    /// and charges the solve this much less.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        self.retained.as_ref().map_or(0, |plane| {
            let values = plane
                .residual
                .iter()
                .chain(&plane.psf)
                .map(Vec::len)
                .sum::<usize>();
            (values * size_of::<f64>() + 2 * plane.support.shape().len() * size_of::<bool>()) as u64
        })
    }
}

/// One model term of one component of a minor cycle, in model coordinates:
/// a multi-term component gives one record per Taylor coefficient.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TracedComponent {
    /// The model cell of the component centre; its coefficient is the
    /// channel of a cube plane or the Taylor term.
    pub cell: ModelCell,
    /// The term's flux after the loop gain.
    pub flux: f64,
    /// Scale size in pixels (0 for a point).
    pub scale_px: f64,
}

/// What one minor cycle did, over every plane and field.
#[derive(Clone, Debug, PartialEq)]
pub struct MinorCycleSummary {
    /// Iterations charged (CASA's `iterdone`).
    pub iterations: usize,
    /// Components actually cleaned.
    pub components: usize,
    /// Sum of the absolute component fluxes over every term.
    pub absolute_flux: f64,
    /// Sum of the components' signed model fluxes over every solve.
    pub model_flux: f64,
    /// Largest peak residual on entry.
    pub start_peak: f64,
    /// Largest peak residual magnitude of the planes that stopped on a rule
    /// (`SDAlgorithmBase::deconvolve`'s across-plane maximum).
    pub peak: f64,
    /// Largest robust noise of a plane, when `nsigma` is on.
    pub noise_rms: Option<f64>,
    /// Every plane's stop, in plane order.
    pub stops: Vec<PlaneStop>,
    /// Exact whole-plane residual refreshes over every plane.
    pub refreshes: usize,
    /// The first component records.
    pub trace: Vec<TracedComponent>,
    /// Every plane's row, those with no data too, ordered by image domain,
    /// channel and polarization.
    pub planes: Vec<PlaneCycleRow>,
}

/// What one minor cycle hands to the next major cycle and to products.
pub struct MinorCycleOutcome {
    /// The model update in strictly increasing model order; empty when no
    /// component was cleaned.
    pub terms: Vec<ModelDeltaTerm>,
    /// Dense updates in canonical plane order.
    pub dense: Vec<DenseModelPlane>,
    /// The last MEM alpha when this cycle ran its entropy solver.
    pub mem_alpha: Option<f32>,
    /// The charge of `terms` ([`result_bytes`]), which covers queuing them
    /// on the model too: keep it until the model holds them under its own.
    pub results: Reservation,
    /// The masks components were placed within.
    pub masks: ImageDomainReconstructionMasks,
    /// Automatic-mask diagnostics per image domain.
    pub auto_masks: Box<[Option<AutoMaskEvidence>]>,
    /// What the cycle did.
    pub summary: MinorCycleSummary,
}

/// One plane's measurements before the cycle.
struct Measured {
    statistics: PlaneStatistics,
    summary: PsfSummary,
}

/// One plane of a multi-plane cycle on the team.
struct Slot {
    key: PlaneKey,
    statistics: PlaneStatistics,
    solved: Option<Solved>,
}

/// A CASA Clark solve groups a channel's polarization planes.
struct PolarizedSlot {
    keys: [PlaneKey; 4],
    stats: [PlaneStatistics; 4],
    solved: Option<Solved>,
}

/// Clean every plane of `completion` under `controls`.
///
/// Planes are independent: several run concurrently on `team`, each on one
/// thread, and their terms are merged in model order, so the result does
/// not depend on the worker count. A single plane spreads Clark's sparse
/// refresh, and the multiscale and multi-term transforms, across the team
/// instead (#449). As each plane finishes, its terms are
/// admitted under `policy` on `host` ([`result_bytes`]) before they are
/// formed; the outcome hands their charge on.
///
/// # Errors
///
/// When a plane cannot be read, its solver fails or its terms are refused.
pub fn run_minor_cycle(
    prepared: PreparedMinorCycle,
    completion: &MajorCycleCompletion,
    setup: &MinorCycleSetup<'_>,
    controls: &CycleControls,
    cache: &mut PsfCache,
    team: &WorkerTeam,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<MinorCycleOutcome, MinorCycleRunError> {
    let PreparedMinorCycle {
        masks,
        auto_masks,
        planes,
        mut blank,
        retained,
        ..
    } = prepared;
    let normal = completion.normal_state();
    let inputs = Inputs {
        normal,
        base: completion.final_model(),
        masks: &masks,
        setup,
        taylor: is_taylor(normal),
    };
    let admission = (host, policy);
    let fft_threads = scale_fft_threads(planes.len(), team.workers());
    let mut results = Vec::with_capacity(planes.len());
    let n_pol = normal.polarization_count();
    // Temporary experiment: groups with blank planes need an explicit CASA
    // comparison before this path can be considered complete.
    let joint = matches!(setup.algorithm, ReconstructionAlgorithm::Clark)
        && !inputs.taylor
        && matches!(n_pol, 2 | 4);
    if joint {
        polarized::solve_groups(
            &inputs,
            admission,
            (&planes, &mut blank),
            cache,
            controls,
            team,
            &mut results,
        )?;
    } else if let [(key, statistics)] = planes.as_slice() {
        let summary = cache.summaries[key];
        // A planned multi-plane run admits no retained Clark refresh, even
        // if every plane but this one is blank. In that case its refresh is
        // local to this solve; only a planned single plane can cache it.
        let mut transient_refresh = None;
        let clark = if blank.is_empty() {
            &mut cache.clark
        } else {
            &mut transient_refresh
        };
        // A lone plane has the team to itself: Clark spreads its refresh
        // across it, and the scale solvers transform on its threads.
        let solved = team.install(|| {
            solve(
                &inputs,
                admission,
                (*key, retained),
                &summary,
                statistics,
                controls,
                (clark, fft_threads),
            )
        })?;
        results.push((*key, *statistics, solved));
    } else {
        let mut slots = planes
            .iter()
            .map(|(key, statistics)| Slot {
                key: *key,
                statistics: *statistics,
                solved: None,
            })
            .collect::<Vec<_>>();
        let shared = &*cache;
        team.for_each_mut(&mut slots, |_, slot| {
            let summary = shared.summaries[&slot.key];
            slot.solved = Some(solve(
                &inputs,
                admission,
                (slot.key, None),
                &summary,
                &slot.statistics,
                controls,
                (&mut None, fft_threads),
            )?);
            Ok::<_, MinorCycleRunError>(())
        })?;
        results.extend(slots.into_iter().map(|slot| {
            (
                slot.key,
                slot.statistics,
                slot.solved.expect("every plane slot ran"),
            )
        }));
    }
    let mut summary = MinorCycleSummary {
        iterations: 0,
        components: 0,
        absolute_flux: 0.0,
        model_flux: 0.0,
        start_peak: 0.0,
        peak: 0.0,
        noise_rms: None,
        stops: Vec::with_capacity(results.len()),
        refreshes: 0,
        trace: Vec::new(),
        planes: Vec::with_capacity(results.len() + blank.len()),
    };
    // Each plane's charge covers its terms' copy here.
    let mut terms = Vec::with_capacity(
        results
            .iter()
            .map(|(_, _, solved)| solved.terms.len())
            .sum(),
    );
    let mut charge = Reservation::none();
    let mut dense = dense_result_slots(&setup.algorithm, results.len());
    let mut mem_alpha = None;
    for (key, statistics, solved) in results {
        charge.join(solved.charge);
        let outcome = &solved.outcome;
        summary.iterations += outcome.iterations;
        summary.components += outcome.components;
        summary.absolute_flux += outcome.absolute_flux;
        summary.model_flux += outcome.model_flux;
        summary.start_peak = summary.start_peak.max(outcome.start_peak);
        if outcome.stop != PlaneStop::ZeroMask {
            summary.peak = summary.peak.max(outcome.peak);
        }
        if let Some(noise) = statistics.noise {
            summary.noise_rms = Some(
                summary
                    .noise_rms
                    .map_or(noise.rms, |rms| rms.max(noise.rms)),
            );
        }
        summary.stops.push(outcome.stop);
        summary.refreshes += outcome.refreshes;
        let mut row = rows::cycle_row(key, &statistics, outcome, solved.peak_no_mask, controls);
        row.polarizations = if joint { n_pol } else { 1 };
        summary.planes.push(row);
        let room = TRACE - summary.trace.len();
        summary.trace.extend(solved.trace.into_iter().take(room));
        terms.extend(solved.terms);
        if solved.mem_alpha.is_some() {
            mem_alpha = solved.mem_alpha;
        }
        if let Some(plane) = solved.dense {
            dense.push((key, plane));
        }
    }
    summary.planes.extend(blank.iter().map(|(key, statistics)| {
        let mut row = rows::blank_row(*key, statistics, controls);
        row.polarizations = if joint { n_pol } else { 1 };
        row
    }));
    summary
        .planes
        .sort_unstable_by_key(|row| (row.domain, row.channel, row.polarization));
    terms.sort_unstable_by_key(|(index, _)| *index);
    dense.sort_unstable_by_key(|(key, _)| *key);
    let mut ordered_dense = Vec::with_capacity(dense.len());
    ordered_dense.extend(dense.into_iter().map(|(_, plane)| plane));
    let dense = ordered_dense;
    let mut ordered = Vec::with_capacity(terms.len());
    ordered.extend(terms.into_iter().map(|(_, term)| term));
    Ok(MinorCycleOutcome {
        terms: ordered,
        dense,
        mem_alpha,
        results: charge,
        masks,
        auto_masks,
        summary,
    })
}

/// Whether `normal` holds Taylor families, one per polarization (decided
/// once per cycle half).
fn is_taylor(normal: &FinalNormalState) -> bool {
    normal.basis() == NormalBasis::Taylor
}

/// The planes a minor cycle cleans, in CASA's order within each image
/// domain: one Taylor family per polarization (`SDAlgorithmMSMFS` cleans
/// one Stokes plane at a time), every channel of every polarization of a
/// cube or continuum plane, or each polarization of every image field.
fn plane_keys(normal: &FinalNormalState, taylor: bool) -> Vec<PlaneKey> {
    let first = normal.slab().core_range().start;
    if taylor {
        return (0..normal.polarization_count())
            .map(|polarization| PlaneKey {
                domain: 0,
                channel: first,
                polarization,
            })
            .collect();
    }
    let mut keys = Vec::new();
    for polarization in 0..normal.polarization_count() {
        for domain in 0..normal.domain_count() {
            for channel in first..first + normal.channel_count() {
                keys.push(PlaneKey {
                    domain,
                    channel,
                    polarization,
                });
            }
        }
    }
    keys
}

/// The pixels plane `key`'s preparation measures, `plane` read as a view:
/// its valid pixels and, when the cycle reads the `.pb`
/// ([`pixels::reads_beam`]), their part inside it and above `pbmask`.
fn plane_pixels<'p>(
    inputs: &Inputs<'_>,
    key: PlaneKey,
    plane: &'p PlaneData<'_>,
) -> Result<pixels::PlanePixels<'p>, MinorCycleRunError> {
    let (beam, within) = if pixels::reads_beam(inputs.setup) {
        pixels::beam_pixels(
            inputs.setup,
            (inputs.base.shape().domain_roles(), key, inputs.taylor),
            plane.valid.as_slice(),
            plane.response.as_ref(),
        )?
        .map(|beam| beam.into_supports(plane.shape))
        .unzip()
    } else {
        (None, None)
    };
    Ok(pixels::PlanePixels {
        valid: &plane.valid,
        beam,
        within: within.flatten(),
    })
}

/// Whether the plane `key` holds data: a valid channel, or a valid Taylor
/// family.
fn holds_data(normal: &FinalNormalState, taylor: bool, key: PlaneKey) -> bool {
    let validity = if taylor {
        normal.support_validity(key.polarization)
    } else {
        normal.domain_channel_validity(
            key.domain,
            key.channel - normal.slab().core_range().start,
            key.polarization,
        )
    };
    validity == Some(SpectralChannelValidity::Valid)
}

/// The plane of a mask's `key`, whose channel counts from the normal
/// state's first; a Taylor family is one channel.
fn absolute(normal: &FinalNormalState, taylor: bool, key: MaskPlaneKey) -> PlaneKey {
    let first = normal.slab().core_range().start;
    PlaneKey {
        domain: key.domain,
        channel: if taylor { first } else { first + key.channel },
        polarization: key.polarization,
    }
}

/// Which pixels of the plane `key` the model holds valid, every Taylor
/// term's for a family, x-major.
fn valid_support(
    normal: &FinalNormalState,
    base: &ModelGeneration,
    taylor: bool,
    key: PlaneKey,
    shape: PlaneShape,
) -> Result<Vec<bool>, MinorCycleRunError> {
    let coefficients = if taylor {
        0..normal.coefficient_term_count()
    } else {
        key.channel..key.channel + 1
    };
    let mut valid = vec![true; shape.len()];
    for coefficient in coefficients {
        let samples = base.read_plane(key.domain, coefficient, key.polarization)?;
        for (index, supported) in valid.iter_mut().enumerate() {
            let [x, y] = shape.pixel(index);
            *supported &= samples[y * shape.nx + x].support() == ModelSupport::Valid;
        }
    }
    Ok(valid)
}

fn plane_shape(normal: &FinalNormalState, key: PlaneKey) -> PlaneShape {
    let [nx, ny] = normal
        .domain_shape(key.domain)
        .expect("plane keys name existing domains");
    PlaneShape::new(nx, ny)
}

/// Term-0 PSF of `key` in gridded or saved image units.
fn read_psf(
    normal: &FinalNormalState,
    taylor: bool,
    key: PlaneKey,
) -> Result<Vec<f64>, MinorCycleRunError> {
    if taylor {
        let window = normal.read_window(normal.slab().core_range())?;
        let moment = window
            .normal_moment(0, key.polarization)
            .expect("a Taylor family has its zeroth moment");
        return Ok(moment
            .normal_approximation()
            .iter()
            .map(|value| value.re)
            .collect());
    }
    let plane = normal.read_reconstruction_plane(key.domain, key.channel, key.polarization)?;
    Ok(plane
        .normal_approximation()
        .iter()
        .map(|value| value.re)
        .collect())
}

/// What every plane of one minor cycle is read from.
#[derive(Clone, Copy)]
struct Inputs<'a> {
    normal: &'a FinalNormalState,
    base: &'a ModelGeneration,
    masks: &'a ImageDomainReconstructionMasks,
    setup: &'a MinorCycleSetup<'a>,
    /// Whether the normal state holds Taylor families.
    taylor: bool,
}

/// A direction-dependent response bound to one plane's weights: what turns
/// a solved value back into the physical model (`divideModelByWeight`).
struct ResponseBinding<'a> {
    sensitivity: Cow<'a, [f64]>,
    response: MinorCycleImageResponse,
    sum_weight: f64,
}

/// One plane's residual and PSF terms as the solver sees them.
struct PlaneTerms<'a> {
    residual: Vec<Vec<f64>>,
    psf: Vec<Vec<f64>>,
    psf_peak: f64,
    response: Option<ResponseBinding<'a>>,
}

/// One plane read as a deconvolution view.
struct PlaneData<'a> {
    shape: PlaneShape,
    residual: Vec<Vec<f64>>,
    psf: Vec<Vec<f64>>,
    psf_peak: f64,
    support: Support,
    valid: Support,
    response: Option<ResponseBinding<'a>>,
}

impl PlaneData<'_> {
    /// The plane's conversion of solved values to the physical model,
    /// bound once (binding scans the sensitivity plane).
    fn physical(&self) -> Result<Physical<'_>, MinorCycleRunError> {
        Ok(match &self.response {
            None => Physical::Identity,
            Some(binding) => Physical::Mosaic(
                MosaicSensitivity::new(&binding.sensitivity)?
                    .with_normal_sum_weight(binding.sum_weight)?,
                &binding.response,
            ),
        })
    }
}

/// Solved values to the physical model (`divideModelByWeight`).
enum Physical<'a> {
    Identity,
    Mosaic(MosaicSensitivity<'a>, &'a MinorCycleImageResponse),
}

impl Physical<'_> {
    fn apply(&self, value: f64, index: usize) -> Result<f64, MinorCycleRunError> {
        match self {
            Self::Identity => Ok(value),
            Self::Mosaic(sensitivity, response) => Ok(sensitivity.apparent_to_physical(
                value,
                index,
                response.normalization(),
                response.policy(),
            )?),
        }
    }
}

/// Read `key` as a view: the single-term or Taylor-family residual and
/// unit-peak PSF, with the requested direction-dependent response, and support.
/// `None` for a plane with no valid data.
fn load<'a>(
    inputs: &Inputs<'a>,
    key: PlaneKey,
) -> Result<Option<PlaneData<'a>>, MinorCycleRunError> {
    let Inputs {
        normal,
        base,
        masks,
        setup,
        taylor,
    } = *inputs;
    let shape = plane_shape(normal, key);
    if !holds_data(normal, taylor, key) {
        return Ok(None);
    }
    let valid = valid_support(normal, base, taylor, key, shape)?;
    let mask = masks
        .get(key.domain)
        .and_then(|mask| {
            mask.plane(
                key.channel - normal.slab().core_range().start,
                key.polarization,
            )
        })
        .expect("a mask for every plane of every image domain");
    let support = mask
        .iter()
        .zip(&valid)
        .map(|(masked, valid)| *masked && *valid)
        .collect();
    let terms = if taylor {
        read_taylor(normal, setup, key.polarization)?
    } else {
        read_single(normal, setup, key)?
    };
    Ok(Some(PlaneData {
        shape,
        residual: terms.residual,
        psf: terms.psf,
        psf_peak: terms.psf_peak,
        support: Support::new(shape, support),
        valid: Support::new(shape, valid),
        response: terms.response,
    }))
}

/// One plane's residual and PSF. Without a response the residual divides by
/// sumwt and the PSF by its peak; with one the residual takes CASA's normalisation
/// (`SIImageStore::divideResidualByWeight`) and the components convert back
/// with `divideModelByWeight`. The residual divides by the published sum of
/// weights and the weight image by the normal one; for one term both are
/// the PSF gridding's `.sumwt` (`FTMachine::finalizeToSkyNew` writes it
/// once), so they cancel as CASA's `divideImageByWeightVal` and
/// `dividePSFByWeight` do. CASA normalises every (polarization,
/// channel) plane by that plane's own sums and weight image, so a cube
/// channel reads its own; a paged cube reads only this plane.
fn read_single<'a>(
    normal: &'a FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    key: PlaneKey,
) -> Result<PlaneTerms<'a>, MinorCycleRunError> {
    let plane = normal.read_reconstruction_plane(key.domain, key.channel, key.polarization)?;
    let psf = plane
        .normal_approximation()
        .iter()
        .map(|value| value.re)
        .collect::<Vec<_>>();
    let peak = single_psf_peak(&psf)?;
    let (residual, response) = single_residual(
        normal,
        setup,
        key,
        plane.residual().iter().map(|value| value.re),
        plane.sum_weight(),
    )?;
    // CASA normalizes the PSF by its own peak (SIImageStore::normPSF).
    Ok(PlaneTerms {
        residual: vec![residual],
        psf: vec![normalise(psf, peak)],
        psf_peak: peak,
        response,
    })
}

/// A single-term plane's residual in the units the minor cycle cleans, from
/// its raw real values `raw` and its sum of weights: divided by the sum
/// (`SIImageStore::divideResidualByWeight`), or with a response in CASA's
/// units, with the response bound to the plane's weights.
fn single_residual<'a>(
    normal: &'a FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    key: PlaneKey,
    raw: impl Iterator<Item = f64>,
    sum_weight: f64,
) -> Result<(Vec<f64>, Option<ResponseBinding<'a>>), MinorCycleRunError> {
    let Some(response) = setup.response else {
        return Ok((raw.map(|value| value / sum_weight).collect(), None));
    };
    let weights = normal.read_plane(key.domain, key.channel, key.polarization)?;
    let sensitivity = weights.read_sensitivity()?;
    let normal_weight = weights.sum_weight();
    let published_weight = weights.published_sum_weight();
    let bound = MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(normal_weight)?;
    let residual = raw
        .enumerate()
        .map(|(index, value)| {
            bound.normalize_weighted_residual_sample(
                value,
                index,
                response.normalization(),
                ResidualFamily::Single,
                published_weight,
                response.policy(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        residual,
        Some(ResponseBinding {
            sensitivity,
            response,
            sum_weight: normal_weight,
        }),
    ))
}

/// Polarization `polarization`'s Taylor family: its residual terms and
/// `2·N_t − 1` PSF moments. Every moment divides by the zeroth moment's
/// peak, with or without a response (`SIImageStore::normPSF`, which
/// `SDAlgorithmMSMFS::initializeDeconvolver` reads, #704). Without a
/// response the residuals divide by the same peak; with one they take
/// CASA's units and the components convert back over the zeroth moment's
/// sum weight.
fn read_taylor(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    polarization: usize,
) -> Result<PlaneTerms<'static>, MinorCycleRunError> {
    let window = normal.read_window(normal.slab().core_range())?;
    let [nx, ny] = normal.shape();
    let shape = PlaneShape::new(nx, ny);
    let principal = window
        .normal_moment(0, polarization)
        .expect("a Taylor family has its zeroth moment");
    let moments = (0..normal.normal_moment_count())
        .map(|moment| {
            window
                .normal_moment(moment, polarization)
                .expect("every normal moment is present")
                .normal_approximation()
                .iter()
                .map(|value| value.re)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let raw = (0..normal.coefficient_term_count()).map(|term| {
        window
            .coefficient_term(term, polarization)
            .expect("every coefficient term is present")
            .residual()
    });
    let peak = psf_peak_value(&moments[0], shape)?;
    let psf = moments
        .into_iter()
        .map(|moment| normalise(moment, peak))
        .collect();
    let Some(response) = setup.response else {
        return Ok(PlaneTerms {
            residual: raw
                .map(|term| term.iter().map(|value| value.re / peak).collect())
                .collect(),
            psf,
            psf_peak: peak,
            response: None,
        });
    };
    let normal_weight = principal.sum_weight();
    let published_weight = window
        .published_sum_weight(0, polarization)
        .expect("a Taylor family has its zeroth sum of weights");
    let sensitivity = principal.sensitivity().to_vec();
    let bound = MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(normal_weight)?;
    let residual = raw
        .map(|term| {
            term.iter()
                .enumerate()
                .map(|(index, value)| {
                    if setup.saved_taylor_residual {
                        return Ok(value.re);
                    }
                    bound.normalize_weighted_residual_sample(
                        value.re,
                        index,
                        response.normalization(),
                        ResidualFamily::Taylor,
                        published_weight,
                        response.policy(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PlaneTerms {
        residual,
        psf,
        psf_peak: peak,
        response: Some(ResponseBinding {
            sensitivity: Cow::Owned(sensitivity),
            response,
            sum_weight: normal_weight,
        }),
    })
}

/// The zeroth Taylor term's residual of polarization `polarization` in the
/// units the minor cycle cleans, as [`read_taylor`] gives it, read alone,
/// with the response bound to the family's weights: an automatic mask reads
/// that term and, of the zeroth PSF moment, its value at its measured
/// `peak`, not the whole family.
fn taylor_principal_residual(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    polarization: usize,
    peak: usize,
) -> Result<(Vec<f64>, Option<ResponseBinding<'static>>), MinorCycleRunError> {
    let window = normal.read_window(normal.slab().core_range())?;
    let principal = window
        .normal_moment(0, polarization)
        .expect("a Taylor family has its zeroth moment");
    let scale = principal.normal_approximation()[peak].re;
    if !(scale.is_finite() && scale > 0.0) {
        return Err(casa_imaging_deconvolution::Error::PsfPeak.into());
    }
    let term = window
        .coefficient_term(0, polarization)
        .expect("a Taylor family has its zeroth term")
        .residual();
    let Some(response) = setup.response else {
        return Ok((term.iter().map(|value| value.re / scale).collect(), None));
    };
    let normal_weight = principal.sum_weight();
    let published_weight = window
        .published_sum_weight(0, polarization)
        .expect("a Taylor family has its zeroth sum of weights");
    let sensitivity = principal.sensitivity().to_vec();
    let bound = MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(normal_weight)?;
    let residual = term
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if setup.saved_taylor_residual {
                return Ok(value.re);
            }
            bound.normalize_weighted_residual_sample(
                value.re,
                index,
                response.normalization(),
                ResidualFamily::Taylor,
                published_weight,
                response.policy(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        residual,
        Some(ResponseBinding {
            sensitivity: Cow::Owned(sensitivity),
            response,
            sum_weight: normal_weight,
        }),
    ))
}

fn psf_peak_value(psf: &[f64], shape: PlaneShape) -> Result<f64, MinorCycleRunError> {
    let peak = casa_imaging_deconvolution::psf_peak(psf, shape)
        .map(|index| psf[index])
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or(casa_imaging_deconvolution::Error::PsfPeak)?;
    Ok(peak)
}

fn normalise(mut plane: Vec<f64>, scale: f64) -> Vec<f64> {
    for value in &mut plane {
        *value /= scale;
    }
    plane
}

/// A plane's solve: its outcome, model terms keyed by model order, and the
/// first component records.
/// One solved plane: its outcome without the model update, which its terms
/// hold, and their charge.
struct Solved {
    outcome: PlaneOutcome,
    /// The residual's peak over the plane's pixels once the plane is done.
    peak_no_mask: f64,
    terms: Vec<(usize, ModelDeltaTerm)>,
    dense: Option<DenseModelPlane>,
    mem_alpha: Option<f32>,
    trace: Vec<TracedComponent>,
    charge: Reservation,
}

/// Read and clean one plane, or clean the load preparation kept for it, and
/// admit its terms on `host` under `policy` before forming them. Clark
/// takes `clark`'s refresh, when it serves this PSF, and leaves its own
/// there for the next cycle; the scale solvers transform on `fft_threads`
/// threads.
#[allow(clippy::too_many_arguments)]
fn solve(
    inputs: &Inputs<'_>,
    (host, policy): (&HostResources, &ResourcePolicy),
    (key, retained): (PlaneKey, Option<RetainedPlane>),
    summary: &PsfSummary,
    statistics: &PlaneStatistics,
    controls: &CycleControls,
    (clark, fft_threads): (&mut Option<LinearRefresh>, usize),
) -> Result<Solved, MinorCycleRunError> {
    let setup = inputs.setup;
    let plane = match retained {
        Some(kept) if kept.key == key => PlaneData {
            shape: kept.support.shape(),
            residual: kept.residual,
            psf: kept.psf,
            psf_peak: kept.psf_peak,
            support: kept.support,
            valid: kept.valid,
            response: None,
        },
        _ => load(inputs, key)?.expect("prepared planes have valid data"),
    };
    let view = MinorCycleView {
        shape: plane.shape,
        residual: &plane.residual,
        psf: &plane.psf,
        summary,
        support: &plane.support,
    };
    if matches!(setup.algorithm, ReconstructionAlgorithm::Mem) {
        return solve_mem(
            inputs,
            (host, policy),
            key,
            &plane,
            &view,
            statistics,
            controls,
        );
    }
    let outcome = match &setup.algorithm {
        ReconstructionAlgorithm::Hogbom => clean(
            &Hogbom::new(setup.accounting == HogbomIterationAccounting::CasaInclusive),
            &view,
            controls,
            statistics,
        )?,
        ReconstructionAlgorithm::Clark | ReconstructionAlgorithm::ClarkStokes => {
            let solver = Clark::new(clark.take());
            let outcome = clean(&solver, &view, controls, statistics);
            *clark = solver.into_refresh();
            outcome?
        }
        ReconstructionAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => clean(
            &Multiscale::new(scales_px.clone(), *small_scale_bias).with_fft_threads(fft_threads),
            &view,
            controls,
            statistics,
        )?,
        ReconstructionAlgorithm::Mtmfs {
            scales_px,
            small_scale_bias,
        } => clean(
            &Taylor::new(plane.residual.len(), scales_px.clone(), *small_scale_bias)
                .with_fft_threads(fft_threads),
            &view,
            controls,
            statistics,
        )?,
        algorithm => return Err(MinorCycleRunError::Algorithm(algorithm.clone())),
    };
    let mut outcome = outcome;
    // CASA reads this after a plane it cleaned, and keeps the entry's for one
    // it skipped.
    let end_residual = std::mem::take(&mut outcome.end_residual);
    let peak_no_mask = match end_residual.first() {
        Some(residual) if setup.residual_pixel_mask => {
            let pixels = plane_pixels(inputs, key, &plane)?;
            peak_magnitude(residual, pixels.residual_mask(true))
        }
        Some(residual) => peak_magnitude(residual, &plane.valid),
        None => statistics.peak_no_mask,
    };
    drop(end_residual);
    let delta = std::mem::take(&mut outcome.delta);
    let entries = (0..delta.term_count())
        .map(|term| delta.term(term).count())
        .sum();
    let charge = admit(
        host,
        policy,
        &Demand {
            phase: "minor-cycle results",
            memory: result_bytes(entries),
        },
    )?;
    let terms = model_terms(&delta, entries, &plane, inputs, key)?;
    let trace = trace(&outcome.trace, &plane, inputs, key);
    Ok(Solved {
        outcome,
        peak_no_mask,
        terms,
        dense: None,
        mem_alpha: None,
        trace,
        charge,
    })
}

/// [`run_plane`] with the cycle's trace length.
fn clean<S: Solver>(
    solver: &S,
    view: &MinorCycleView<'_>,
    controls: &CycleControls,
    statistics: &PlaneStatistics,
) -> Result<PlaneOutcome, MinorCycleRunError> {
    Ok(run_plane(solver, view, controls, statistics, TRACE)?)
}

/// The model coefficient a plane's term `term` goes to: the Taylor term, or
/// the channel of a cube plane.
const fn coefficient(inputs: &Inputs<'_>, key: PlaneKey, term: usize) -> usize {
    if inputs.taylor { term } else { key.channel }
}

/// The plane's components as model terms keyed by model order, in a vector
/// with room for `delta`'s `entries`.
fn model_terms(
    delta: &Delta,
    entries: usize,
    plane: &PlaneData,
    inputs: &Inputs<'_>,
    key: PlaneKey,
) -> Result<Vec<(usize, ModelDeltaTerm)>, MinorCycleRunError> {
    let mut terms = Vec::with_capacity(entries);
    let physical = plane.physical()?;
    for term in 0..delta.term_count() {
        for (index, flux) in delta.term(term) {
            let value = physical.apply(flux, index)?;
            if value == 0.0 {
                continue;
            }
            let cell = ModelCell::new(
                key.domain,
                coefficient(inputs, key, term),
                key.polarization,
                plane.shape.pixel(index),
            );
            let flat = inputs
                .base
                .shape()
                .flat_index(cell)
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            terms.push((
                flat,
                ModelDeltaTerm::new(
                    cell,
                    ModelValue::new(value).map_err(ModelLifecycleError::Contract)?,
                ),
            ));
        }
    }
    Ok(terms)
}

fn trace(
    components: &[Component],
    plane: &PlaneData,
    inputs: &Inputs<'_>,
    key: PlaneKey,
) -> Vec<TracedComponent> {
    let scales = match &inputs.setup.algorithm {
        ReconstructionAlgorithm::Multiscale { scales_px, .. }
        | ReconstructionAlgorithm::Mtmfs { scales_px, .. } => scales_px.as_slice(),
        _ => &[],
    };
    components
        .iter()
        .map(|component| TracedComponent {
            cell: ModelCell::new(
                key.domain,
                coefficient(inputs, key, component.term),
                key.polarization,
                plane.shape.pixel(component.index),
            ),
            flux: component.flux,
            scale_px: scales.get(component.scale).copied().unwrap_or(0.0),
        })
        .collect()
}
