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

use casa_imaging_deconvolution::{
    Clark, Component, CycleControls, Delta, Hogbom, LinearRefresh, MinorCycleView, Multiscale,
    PlaneOutcome, PlaneShape, PlaneStatistics, PlaneStop, PsfSummary, ResidualStatistics, Solver,
    Support, Taylor, run_plane,
};
use casa_imaging_model::{
    HogbomIterationAccounting, ModelCell, ModelDeltaTerm, ModelSupport, ModelValue,
    ReconstructionAlgorithm,
};
use casa_imaging_reconstruction::{
    AutoMaskBeam, AutoMultithreshEvidence, FinalNormalState, ImageDomainReconstructionMaskPlans,
    ImageDomainReconstructionMasks, MajorCycleCompletion, MaskError, MinorCycleImageResponse,
    ModelGeneration, ModelLifecycle, ModelLifecycleError, MosaicSensitivity, NormalStateCatalog,
    SpectralChannelValidity, SpectralOperatorError,
};

use crate::pass::WorkerTeam;
use crate::{Admission, Demand, HostResources, Reservation, ResourcePolicy, admit};

/// Component records kept per minor cycle for diagnostics.
const TRACE: usize = 64;

/// A failure of the minor cycle.
#[derive(Debug, thiserror::Error)]
pub enum MinorCycleRunError {
    /// The reconstruction masks could not be formed.
    #[error("reconstruction mask: {0}")]
    Mask(#[from] MaskError),
    /// A normal-state plane could not be read.
    #[error("normal state: {0}")]
    Normal(#[from] SpectralOperatorError),
    /// The model support could not be read or a term is out of range.
    #[error("model: {0}")]
    Model(#[from] ModelLifecycleError),
    /// The direction-dependent response could not convert a value.
    #[error("image response: {0}")]
    Response(#[from] casa_imaging_reconstruction::ImageResponseError),
    /// The solver failed.
    #[error(transparent)]
    Solver(#[from] casa_imaging_deconvolution::Error),
    /// The selected algorithm has no minor cycle.
    #[error("{0:?} has no minor cycle")]
    Algorithm(ReconstructionAlgorithm),
    /// A plane's terms do not fit what the policy leaves free.
    #[error(transparent)]
    Admission(#[from] Admission),
}

/// What every minor cycle of a run shares.
#[derive(Clone, Debug)]
pub struct MinorCycleSetup {
    /// The deconvolver.
    pub algorithm: ReconstructionAlgorithm,
    /// Högbom's iteration accounting.
    pub accounting: HogbomIterationAccounting,
    /// CASA's flat-noise or flat-sky residual units, for a direction-dependent
    /// response (mosaic, A-projection).
    pub response: Option<MinorCycleImageResponse>,
    /// The `nsigma` multiplier; zero when off.
    pub nsigma: f64,
    /// Whether the mask is CASA's automatic multi-threshold mask, whose
    /// n-sigma threshold adds the residual median.
    pub automask: bool,
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
    summaries: BTreeMap<PlaneKey, PsfSummary>,
    clark: Option<LinearRefresh>,
}

impl PsfCache {
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

/// Heap bytes one loaded plane holds ([`load`]): its residual and PSF terms
/// (`f64`), its support and validity, its sensitivity under a response, and
/// what it is read through (the normal state's windows as `Complex64`, one
/// model plane).
fn load_bytes(cells: u64, residual_terms: u64, psf_terms: u64, response: bool) -> u64 {
    let terms = residual_terms + psf_terms;
    cells
        * (terms * size_of::<f64>() as u64
            + 2 * size_of::<bool>() as u64
            + if response { size_of::<f64>() as u64 } else { 0 }
            + terms * size_of::<num_complex::Complex64>() as u64
            + size_of::<casa_imaging_model::ModelSample>() as u64)
}

/// The planes a minor cycle of `normal` loads, their largest cell count and
/// their residual and PSF term counts.
fn plane_layout(normal: &FinalNormalState) -> (Vec<PlaneKey>, u64, u64, u64) {
    let taylor = is_taylor(normal);
    let keys = plane_keys(normal, taylor);
    let cells = keys
        .iter()
        .map(|key| plane_shape(normal, *key).len() as u64)
        .max()
        .unwrap_or(0);
    let (residual, psf) = if taylor {
        (
            normal.coefficient_term_count() as u64,
            normal.normal_moment_count() as u64,
        )
    } else {
        (1, 1)
    };
    (keys, cells, residual, psf)
}

/// Heap bytes [`prepare_minor_cycle`] holds at most: the masks it
/// materializes, and on each of the planes loaded at once on `workers`, the
/// load and its measurement (the robust noise's two copies under `nsigma`,
/// the PSF fit's single-precision copies for a plane not yet in `cache`).
#[must_use]
pub fn prepare_bytes(
    completion: &MajorCycleCompletion,
    mask_plans: &ImageDomainReconstructionMaskPlans,
    setup: &MinorCycleSetup,
    cache: &PsfCache,
    workers: usize,
) -> u64 {
    let normal = completion.normal_state();
    let (keys, cells, residual, psf) = plane_layout(normal);
    let fits = keys.iter().any(|key| !cache.summaries.contains_key(key));
    let measure = cells
        * if setup.nsigma > 0.0 {
            2 * size_of::<f64>() as u64
        } else {
            0
        }
        .max(if fits { 2 * size_of::<f32>() as u64 } else { 0 });
    let concurrent = workers.min(keys.len()).max(1) as u64;
    mask_plans.materialize_bytes(normal)
        + concurrent * (load_bytes(cells, residual, psf, setup.response.is_some()) + measure)
}

/// Heap bytes the masks a minor cycle forms keep while the cycle runs and
/// after it: one support per image domain.
#[must_use]
pub fn mask_bytes(completion: &MajorCycleCompletion) -> u64 {
    let normal = completion.normal_state();
    (0..normal.domain_count())
        .filter_map(|domain| normal.domain_shape(domain))
        .map(|[width, height]| (width * height * size_of::<bool>()) as u64)
        .sum()
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

/// Heap bytes [`run_minor_cycle`] holds at most under `controls`, besides
/// each plane's terms ([`result_bytes`]): on each of the planes solved at
/// once on `workers` (one when a single plane spreads over the team), the
/// load and the solve ([`casa_imaging_deconvolution::solve_bytes`]), and a
/// record of every plane. A Clark refresh already in `cache` is charged
/// with the cache, not here.
#[must_use]
pub fn run_bytes(
    completion: &MajorCycleCompletion,
    setup: &MinorCycleSetup,
    controls: &CycleControls,
    cache: &PsfCache,
    workers: usize,
) -> u64 {
    let normal = completion.normal_state();
    let (keys, cells, residual, psf) = plane_layout(normal);
    let shape = keys
        .iter()
        .map(|key| plane_shape(normal, *key))
        .max_by_key(|shape| shape.len())
        .unwrap_or(PlaneShape::new(0, 0));
    let terms = residual as usize;
    let components = controls.iterations + 1;
    let solve = match &setup.algorithm {
        ReconstructionAlgorithm::Hogbom => casa_imaging_deconvolution::solve_bytes(
            &Hogbom::new(false),
            shape,
            terms,
            components,
            TRACE,
        ),
        ReconstructionAlgorithm::Clark => {
            let solve = casa_imaging_deconvolution::solve_bytes(
                &Clark::new(None),
                shape,
                terms,
                components,
                TRACE,
            );
            if keys.len() == 1 {
                solve.saturating_sub(cache.refresh_bytes(completion))
            } else {
                solve
            }
        }
        ReconstructionAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => casa_imaging_deconvolution::solve_bytes(
            &Multiscale::new(scales_px.clone(), *small_scale_bias),
            shape,
            terms,
            components,
            TRACE,
        ),
        ReconstructionAlgorithm::Mtmfs {
            scales_px,
            small_scale_bias,
        } => casa_imaging_deconvolution::solve_bytes(
            &Taylor::new(terms, scales_px.clone(), *small_scale_bias),
            shape,
            terms,
            components,
            TRACE,
        ),
        _ => 0,
    };
    let concurrent = if keys.len() == 1 {
        1
    } else {
        workers.min(keys.len()) as u64
    };
    // Every plane's slot, solved record and stop, which the cycle keeps
    // until it merges them.
    let records = keys.len() * (size_of::<Slot>() + size_of::<Solved>() + size_of::<PlaneStop>());
    concurrent * (load_bytes(cells, residual, psf, setup.response.is_some()) + solve)
        + records as u64
}

/// One minor cycle's masks and plane measurements.
pub struct PreparedMinorCycle {
    masks: ImageDomainReconstructionMasks,
    auto_masks: Box<[Option<AutoMultithreshEvidence>]>,
    planes: Vec<(PlaneKey, PlaneStatistics)>,
    /// The measurements of every plane and field, for the controller.
    pub statistics: ResidualStatistics,
}

impl PreparedMinorCycle {
    /// The masks the cycle cleans within.
    #[must_use]
    pub const fn masks(&self) -> &ImageDomainReconstructionMasks {
        &self.masks
    }

    /// Automatic-mask diagnostics per image domain.
    #[must_use]
    pub fn auto_masks(&self) -> &[Option<AutoMultithreshEvidence>] {
        &self.auto_masks
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
}

/// What one minor cycle hands to the next major cycle and to products.
pub struct MinorCycleOutcome {
    /// The model update in strictly increasing model order; empty when no
    /// component was cleaned.
    pub terms: Vec<ModelDeltaTerm>,
    /// The charge of `terms` ([`result_bytes`]), which covers queuing them
    /// on the model too: keep it until the model holds them under its own.
    pub results: Reservation,
    /// The masks components were placed within.
    pub masks: ImageDomainReconstructionMasks,
    /// Automatic-mask diagnostics per image domain.
    pub auto_masks: Box<[Option<AutoMultithreshEvidence>]>,
    /// What the cycle did.
    pub summary: MinorCycleSummary,
}

/// One plane's measurements before the cycle.
struct Measured {
    statistics: PlaneStatistics,
    summary: PsfSummary,
}

/// Form the masks of `completion` and measure every plane: its peak
/// residual, noise and PSF sidelobe (CASA's `initminorcycle`).
///
/// # Errors
///
/// When a mask, a plane or a PSF cannot be read or measured.
pub fn prepare_minor_cycle(
    completion: &MajorCycleCompletion,
    mask_plans: &ImageDomainReconstructionMaskPlans,
    setup: &MinorCycleSetup,
    cache: &mut PsfCache,
    team: &WorkerTeam,
) -> Result<PreparedMinorCycle, MinorCycleRunError> {
    let normal = completion.normal_state();
    let base = completion.final_model();
    let taylor = is_taylor(normal);
    let keys = plane_keys(normal, taylor);
    // The automatic mask (one-channel continuum only) smooths by the
    // primary PSF's beam; a cube's first channel may hold no data.
    let beam = if setup.automask {
        let primary = keys[0];
        summarise(normal, taylor, cache, primary)?;
        Some(automask_beam(&cache.summaries[&primary]))
    } else {
        None
    };
    let (masks, auto_masks) = mask_plans.materialize(base, normal, beam)?.into_parts();
    let inputs = Inputs {
        normal,
        base,
        masks: &masks,
        setup,
        taylor,
    };
    let mut slots = keys
        .iter()
        .map(|key| (*key, None::<Measured>))
        .collect::<Vec<_>>();
    let missing = keys
        .iter()
        .filter(|key| !cache.summaries.contains_key(key))
        .count();
    let shared = &*cache;
    team.for_each_mut(&mut slots, |_, (key, out)| {
        let Some(plane) = load(&inputs, *key)? else {
            return Ok(());
        };
        let summary = match shared.summaries.get(key) {
            Some(summary) => *summary,
            None => PsfSummary::new(&plane.psf[0], plane.shape)?,
        };
        let statistics = PlaneStatistics::measure(
            &plane.residual[0],
            &plane.support,
            &plane.valid,
            setup.nsigma,
            setup.automask,
        );
        *out = Some(Measured {
            statistics,
            summary,
        });
        Ok::<_, MinorCycleRunError>(())
    })?;
    let mut planes = Vec::with_capacity(keys.len());
    let mut sidelobes = Vec::with_capacity(keys.len());
    for (key, measured) in slots {
        let Some(Measured {
            statistics,
            summary,
        }) = measured
        else {
            continue;
        };
        sidelobes.push(summary.sidelobe());
        cache.summaries.entry(key).or_insert(summary);
        planes.push((key, statistics));
    }
    // Each image domain is one CASA image store.
    let mut statistics = ResidualStatistics::empty();
    let domains = planes
        .iter()
        .map(|(key, _)| key.domain)
        .collect::<BTreeSet<_>>();
    for domain in domains {
        statistics.include_store(
            planes
                .iter()
                .zip(&sidelobes)
                .filter(|((key, _), _)| key.domain == domain)
                .map(|((_, plane), sidelobe)| (plane, *sidelobe)),
            setup.nsigma,
        );
    }
    tracing::debug!(
        planes = planes.len(),
        psf_fits = missing,
        "minor cycle prepared"
    );
    Ok(PreparedMinorCycle {
        masks,
        auto_masks,
        planes,
        statistics,
    })
}

/// One plane of a multi-plane cycle on the team.
struct Slot {
    key: PlaneKey,
    statistics: PlaneStatistics,
    solved: Option<Solved>,
}

/// Clean every plane of `completion` under `controls`.
///
/// Planes are independent: several run concurrently on `team`, each on one
/// thread, and their terms are merged in model order, so the result does
/// not depend on the worker count. A single plane spreads Clark's sparse
/// refresh across the team instead. As each plane finishes, its terms are
/// admitted under `policy` on `host` ([`result_bytes`]) before they are
/// formed; the outcome hands their charge on.
///
/// # Errors
///
/// When a plane cannot be read, its solver fails or its terms are refused.
pub fn run_minor_cycle(
    prepared: PreparedMinorCycle,
    completion: &MajorCycleCompletion,
    setup: &MinorCycleSetup,
    controls: &CycleControls,
    cache: &mut PsfCache,
    team: &WorkerTeam,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<MinorCycleOutcome, MinorCycleRunError> {
    let PreparedMinorCycle {
        masks,
        auto_masks,
        planes,
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
    let mut results = Vec::with_capacity(planes.len());
    if let [(key, statistics)] = planes.as_slice() {
        let summary = cache.summaries[key];
        let clark = &mut cache.clark;
        results.push(team.install(|| {
            solve(
                &inputs, admission, *key, &summary, statistics, controls, clark,
            )
        })?);
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
                slot.key,
                &summary,
                &slot.statistics,
                controls,
                &mut None,
            )?);
            Ok::<_, MinorCycleRunError>(())
        })?;
        results.extend(
            slots
                .into_iter()
                .map(|slot| slot.solved.expect("every plane slot ran")),
        );
    }
    let mut summary = MinorCycleSummary {
        iterations: 0,
        components: 0,
        absolute_flux: 0.0,
        start_peak: 0.0,
        peak: 0.0,
        noise_rms: None,
        stops: Vec::with_capacity(results.len()),
        refreshes: 0,
        trace: Vec::new(),
    };
    // Each plane's charge covers its terms' copy here.
    let mut terms = Vec::with_capacity(results.iter().map(|solved| solved.terms.len()).sum());
    let mut charge = Reservation::none();
    for ((_, statistics), solved) in planes.iter().zip(results) {
        charge.join(solved.charge);
        let outcome = &solved.outcome;
        summary.iterations += outcome.iterations;
        summary.components += outcome.components;
        summary.absolute_flux += outcome.absolute_flux;
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
        let room = TRACE - summary.trace.len();
        summary.trace.extend(solved.trace.into_iter().take(room));
        terms.extend(solved.terms);
    }
    terms.sort_unstable_by_key(|(index, _)| *index);
    let mut ordered = Vec::with_capacity(terms.len());
    ordered.extend(terms.into_iter().map(|(_, term)| term));
    Ok(MinorCycleOutcome {
        terms: ordered,
        results: charge,
        masks,
        auto_masks,
        summary,
    })
}

/// Whether `normal` holds one Taylor family (decided once per cycle half).
fn is_taylor(normal: &FinalNormalState) -> bool {
    normal.catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1
}

/// The planes a minor cycle cleans, in CASA's order within each image
/// domain: one Taylor family, every channel of every polarization of a
/// cube or continuum plane, or each polarization of every image field.
fn plane_keys(normal: &FinalNormalState, taylor: bool) -> Vec<PlaneKey> {
    let first = normal.slab().core_range().start;
    if taylor {
        return vec![PlaneKey {
            domain: 0,
            channel: first,
            polarization: 0,
        }];
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

/// Measure the PSF of `key` into the cache.
fn summarise(
    normal: &FinalNormalState,
    taylor: bool,
    cache: &mut PsfCache,
    key: PlaneKey,
) -> Result<(), MinorCycleRunError> {
    if cache.summaries.contains_key(&key) {
        return Ok(());
    }
    let shape = plane_shape(normal, key);
    let psf = read_psf(normal, taylor, key)?;
    let peak = psf_peak_value(&psf, shape)?;
    cache
        .summaries
        .insert(key, PsfSummary::new(&normalise(psf, peak), shape)?);
    Ok(())
}

/// The automatic mask's view of the primary PSF's fitted beam (pixels).
fn automask_beam(summary: &PsfSummary) -> AutoMaskBeam {
    let beam = summary.beam();
    AutoMaskBeam::new(
        beam.major_fwhm_rad(),
        beam.minor_fwhm_rad(),
        beam.position_angle_rad(),
        summary.sidelobe(),
    )
}

fn plane_shape(normal: &FinalNormalState, key: PlaneKey) -> PlaneShape {
    let [nx, ny] = normal
        .domain_shape(key.domain)
        .expect("plane keys name existing domains");
    PlaneShape::new(nx, ny)
}

/// Term-0 PSF of `key`, unnormalised.
fn read_psf(
    normal: &FinalNormalState,
    taylor: bool,
    key: PlaneKey,
) -> Result<Vec<f64>, MinorCycleRunError> {
    if taylor {
        let window = normal.read_window(normal.slab().core_range())?;
        let moment = window
            .normal_moment(0)
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
    setup: &'a MinorCycleSetup,
    /// Whether the normal state is one Taylor family.
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
    response: Option<ResponseBinding<'a>>,
}

/// One plane read as a deconvolution view.
struct PlaneData<'a> {
    shape: PlaneShape,
    residual: Vec<Vec<f64>>,
    psf: Vec<Vec<f64>>,
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

/// Read `key` as a view: residual and PSF normalised by the term-0 PSF peak
/// (or the residual in CASA's flat-noise or flat-sky units under a
/// direction-dependent response), and the support. `None` for a plane with
/// no valid data.
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
    let validity = if taylor {
        normal.support_validity()
    } else {
        normal.domain_channel_validity(
            key.domain,
            key.channel - normal.slab().core_range().start,
            key.polarization,
        )
    };
    if validity != Some(SpectralChannelValidity::Valid) {
        return Ok(None);
    }
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
    let mask = masks
        .get(key.domain)
        .expect("a mask for every image domain");
    let support = mask
        .support()
        .iter()
        .zip(&valid)
        .map(|(masked, valid)| *masked && *valid)
        .collect();
    let terms = if taylor {
        read_taylor(normal, setup)?
    } else {
        read_single(normal, setup, key)?
    };
    Ok(Some(PlaneData {
        shape,
        residual: terms.residual,
        psf: terms.psf,
        support: Support::new(shape, support),
        valid: Support::new(shape, valid),
        response: terms.response,
    }))
}

/// One plane's residual and PSF. Without a response both are divided by the
/// PSF peak; with one the residual takes CASA's normalisation
/// (`SIImageStore::divideResidualByWeight`: by the published sum of weights,
/// and the weight image by the PSF gridding's) and the components convert
/// back with `divideModelByWeight`. CASA normalises every (polarization,
/// channel) plane by that plane's own sums and weight image, so a cube
/// channel reads its own; a paged cube reads only this plane.
fn read_single<'a>(
    normal: &'a FinalNormalState,
    setup: &MinorCycleSetup,
    key: PlaneKey,
) -> Result<PlaneTerms<'a>, MinorCycleRunError> {
    let plane = normal.read_reconstruction_plane(key.domain, key.channel, key.polarization)?;
    let shape = plane_shape(normal, key);
    let psf = plane
        .normal_approximation()
        .iter()
        .map(|value| value.re)
        .collect::<Vec<_>>();
    let peak = psf_peak_value(&psf, shape)?;
    let raw = plane.residual();
    let Some(response) = setup.response else {
        return Ok(PlaneTerms {
            residual: vec![raw.iter().map(|value| value.re / peak).collect()],
            psf: vec![normalise(psf, peak)],
            response: None,
        });
    };
    let weights = normal.read_plane(key.domain, key.channel, key.polarization)?;
    let sensitivity = weights.read_sensitivity()?;
    let normal_weight = weights.sum_weight();
    let published_weight = weights.published_sum_weight();
    let bound = MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(normal_weight)?;
    let residual = raw
        .iter()
        .enumerate()
        .map(|(index, value)| {
            bound.normalize_weighted_residual_sample(
                value.re,
                index,
                response.normalization(),
                published_weight,
                response.policy(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PlaneTerms {
        residual: vec![residual],
        psf: vec![normalise(psf, peak)],
        response: Some(ResponseBinding {
            sensitivity,
            response,
            sum_weight: normal_weight,
        }),
    })
}

/// The Taylor family's residual terms and `2·N_t − 1` PSF moments, scaled
/// alike: by the zeroth moment's peak, or under a response with the residuals
/// in CASA's units and the moments over the zeroth moment's sum weight.
fn read_taylor(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup,
) -> Result<PlaneTerms<'static>, MinorCycleRunError> {
    let window = normal.read_window(normal.slab().core_range())?;
    let [nx, ny] = normal.shape();
    let shape = PlaneShape::new(nx, ny);
    let principal = window
        .normal_moment(0)
        .expect("a Taylor family has its zeroth moment");
    let moments = (0..normal.normal_moment_count())
        .map(|moment| {
            window
                .normal_moment(moment)
                .expect("every normal moment is present")
                .normal_approximation()
                .iter()
                .map(|value| value.re)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let raw = (0..normal.coefficient_term_count()).map(|term| {
        window
            .coefficient_term(term)
            .expect("every coefficient term is present")
            .residual()
    });
    let Some(response) = setup.response else {
        let peak = psf_peak_value(&moments[0], shape)?;
        return Ok(PlaneTerms {
            residual: raw
                .map(|term| term.iter().map(|value| value.re / peak).collect())
                .collect(),
            psf: moments
                .into_iter()
                .map(|moment| normalise(moment, peak))
                .collect(),
            response: None,
        });
    };
    let normal_weight = principal.sum_weight();
    let published_weight = window.published_sum_weights()[0];
    let sensitivity = principal.sensitivity().to_vec();
    let bound = MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(normal_weight)?;
    let residual = raw
        .map(|term| {
            term.iter()
                .enumerate()
                .map(|(index, value)| {
                    bound.normalize_weighted_residual_sample(
                        value.re,
                        index,
                        response.normalization(),
                        published_weight,
                        response.policy(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PlaneTerms {
        residual,
        psf: moments
            .into_iter()
            .map(|moment| normalise(moment, normal_weight))
            .collect(),
        response: Some(ResponseBinding {
            sensitivity: Cow::Owned(sensitivity),
            response,
            sum_weight: normal_weight,
        }),
    })
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
    terms: Vec<(usize, ModelDeltaTerm)>,
    trace: Vec<TracedComponent>,
    charge: Reservation,
}

/// Read and clean one plane, and admit its terms on `host` under `policy`
/// before forming them. Clark takes `clark`'s refresh, when it serves this
/// PSF, and leaves its own there for the next cycle.
#[allow(clippy::too_many_arguments)]
fn solve(
    inputs: &Inputs<'_>,
    (host, policy): (&HostResources, &ResourcePolicy),
    key: PlaneKey,
    summary: &PsfSummary,
    statistics: &PlaneStatistics,
    controls: &CycleControls,
    clark: &mut Option<LinearRefresh>,
) -> Result<Solved, MinorCycleRunError> {
    let setup = inputs.setup;
    let plane = load(inputs, key)?.expect("prepared planes have valid data");
    let view = MinorCycleView {
        shape: plane.shape,
        residual: &plane.residual,
        psf: &plane.psf,
        summary,
        support: &plane.support,
    };
    let outcome = match &setup.algorithm {
        ReconstructionAlgorithm::Hogbom => clean(
            &Hogbom::new(setup.accounting == HogbomIterationAccounting::CasaInclusive),
            &view,
            controls,
            statistics,
        )?,
        ReconstructionAlgorithm::Clark => {
            let solver = Clark::new(clark.take());
            let outcome = clean(&solver, &view, controls, statistics);
            *clark = solver.into_refresh();
            outcome?
        }
        ReconstructionAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => clean(
            &Multiscale::new(scales_px.clone(), *small_scale_bias),
            &view,
            controls,
            statistics,
        )?,
        ReconstructionAlgorithm::Mtmfs {
            scales_px,
            small_scale_bias,
        } => clean(
            &Taylor::new(plane.residual.len(), scales_px.clone(), *small_scale_bias),
            &view,
            controls,
            statistics,
        )?,
        algorithm => return Err(MinorCycleRunError::Algorithm(algorithm.clone())),
    };
    let mut outcome = outcome;
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
        terms,
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
