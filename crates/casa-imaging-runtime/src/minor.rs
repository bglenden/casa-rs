// SPDX-License-Identifier: LGPL-3.0-or-later
//! The minor cycle of one major-cycle completion: its planes read from the
//! normal state as deconvolution views, solved on the worker team, and
//! their components returned as model terms.
//!
//! A minor cycle has two halves around the run's [`Controller`]:
//! [`prepare_minor_cycle`] forms the masks and measures every plane (CASA's
//! `initminorcycle`), and [`run_minor_cycle`] cleans every plane under the
//! controls the controller derived from those measurements.

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
    ModelGeneration, ModelLifecycleError, MosaicSensitivity, NormalStateCatalog,
    SpectralChannelValidity, SpectralOperatorError,
};

use crate::pass::WorkerTeam;

/// Components kept per minor cycle for diagnostics.
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

/// One minor cycle's masks and plane measurements.
pub struct PreparedMinorCycle {
    masks: ImageDomainReconstructionMasks,
    auto_masks: Box<[Option<AutoMultithreshEvidence>]>,
    planes: Vec<(PlaneKey, PlaneStatistics)>,
    weights: BTreeMap<(usize, usize), ResponseWeights>,
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

/// One component of a minor cycle, in model coordinates.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TracedComponent {
    /// The model cell of the component centre.
    pub cell: ModelCell,
    /// Term-0 flux after the loop gain.
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
    /// Sum of the absolute term-0 component fluxes.
    pub absolute_flux: f64,
    /// Largest peak residual on entry.
    pub start_peak: f64,
    /// Largest peak residual of the planes that stopped on a rule
    /// (`SDAlgorithmBase::deconvolve`'s across-plane maximum).
    pub peak: f64,
    /// Largest robust noise of a plane, when `nsigma` is on.
    pub noise_rms: Option<f64>,
    /// Every plane's stop, in plane order.
    pub stops: Vec<PlaneStop>,
    /// Exact whole-plane residual refreshes over every plane.
    pub refreshes: usize,
    /// The first components.
    pub trace: Vec<TracedComponent>,
}

/// What one minor cycle hands to the next major cycle and to products.
pub struct MinorCycleOutcome {
    /// The model update in strictly increasing model order; empty when no
    /// component was cleaned.
    pub terms: Vec<ModelDeltaTerm>,
    /// The masks components were placed within.
    pub masks: ImageDomainReconstructionMasks,
    /// Automatic-mask diagnostics per image domain.
    pub auto_masks: Box<[Option<AutoMultithreshEvidence>]>,
    /// What the cycle did.
    pub summary: MinorCycleSummary,
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
    let keys = plane_keys(normal);
    // The automatic mask (one-channel continuum only) smooths by the
    // primary PSF's beam; a cube's first channel may hold no data.
    let beam = if setup.automask {
        let primary = keys[0];
        summarise(normal, cache, primary)?;
        Some(automask_beam(&cache.summaries[&primary]))
    } else {
        None
    };
    let (masks, auto_masks) = mask_plans.materialize(base, normal, beam)?.into_parts();
    let weights = response_weights(normal, setup, &keys)?;
    let inputs = Inputs {
        normal,
        base,
        masks: &masks,
        setup,
        weights: &weights,
    };
    let mut slots = keys.iter().map(|key| (*key, None)).collect::<Vec<_>>();
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
        *out = Some((statistics, summary));
        Ok::<_, MinorCycleRunError>(())
    })?;
    let mut planes = Vec::with_capacity(keys.len());
    let mut sidelobes = Vec::with_capacity(keys.len());
    for (key, measured) in slots {
        let Some((plane, summary)) = measured else {
            continue;
        };
        sidelobes.push(summary.sidelobe());
        cache.summaries.entry(key).or_insert(summary);
        planes.push((key, plane));
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
        weights,
        statistics,
    })
}

/// Clean every plane of `completion` under `controls`.
///
/// Planes are independent: several run concurrently on `team`, each on one
/// thread, and their terms are merged in model order, so the result does
/// not depend on the worker count. A single plane spreads Clark's sparse
/// refresh across the team instead.
///
/// # Errors
///
/// When a plane cannot be read or its solver fails.
pub fn run_minor_cycle(
    prepared: PreparedMinorCycle,
    completion: &MajorCycleCompletion,
    setup: &MinorCycleSetup,
    controls: &CycleControls,
    cache: &mut PsfCache,
    team: &WorkerTeam,
) -> Result<MinorCycleOutcome, MinorCycleRunError> {
    let PreparedMinorCycle {
        masks,
        auto_masks,
        planes,
        weights,
        ..
    } = prepared;
    let inputs = Inputs {
        normal: completion.normal_state(),
        base: completion.final_model(),
        masks: &masks,
        setup,
        weights: &weights,
    };
    let mut results = Vec::with_capacity(planes.len());
    if let [(key, statistics)] = planes.as_slice() {
        let summary = cache.summaries[key];
        let clark = cache.clark.take();
        let (result, refresh) = team.install(|| {
            solve(
                &inputs,
                *key,
                &summary,
                statistics,
                controls,
                team.workers(),
                clark,
            )
        });
        cache.clark = refresh;
        results.push(result?);
    } else {
        let mut slots = planes
            .iter()
            .map(|(key, statistics)| (*key, *statistics, None))
            .collect::<Vec<_>>();
        let shared = &*cache;
        team.for_each_mut(&mut slots, |_, (key, statistics, out)| {
            let summary = shared.summaries[key];
            let (result, _) = solve(&inputs, *key, &summary, statistics, controls, 1, None);
            *out = Some(result?);
            Ok::<_, MinorCycleRunError>(())
        })?;
        results.extend(
            slots
                .into_iter()
                .map(|(_, _, result)| result.expect("every plane slot ran")),
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
    let mut terms = Vec::new();
    for ((_, statistics), solved) in planes.iter().zip(results) {
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
    Ok(MinorCycleOutcome {
        terms: terms.into_iter().map(|(_, term)| term).collect(),
        masks,
        auto_masks,
        summary,
    })
}

/// The planes a minor cycle cleans, in CASA's order within each image
/// domain: one Taylor family, every channel of every polarization of a
/// cube or continuum plane, or each polarization of every image field.
fn plane_keys(normal: &FinalNormalState) -> Vec<PlaneKey> {
    let first = normal.slab().core_range().start;
    if normal.catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1 {
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
    cache: &mut PsfCache,
    key: PlaneKey,
) -> Result<(), MinorCycleRunError> {
    if cache.summaries.contains_key(&key) {
        return Ok(());
    }
    let shape = plane_shape(normal, key);
    let psf = read_psf(normal, key)?;
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
fn read_psf(normal: &FinalNormalState, key: PlaneKey) -> Result<Vec<f64>, MinorCycleRunError> {
    if normal.catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1 {
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
    /// The response weights, read once per cycle.
    weights: &'a BTreeMap<(usize, usize), ResponseWeights>,
}

/// One plane read as a deconvolution view.
struct PlaneData<'a> {
    shape: PlaneShape,
    residual: Vec<Vec<f64>>,
    psf: Vec<Vec<f64>>,
    support: Support,
    valid: Support,
    /// Converts a solved value at a pixel to the physical model.
    physical: Option<(Cow<'a, [f64]>, MinorCycleImageResponse, f64)>,
}

impl PlaneData<'_> {
    /// The plane's conversion of solved values to the physical model,
    /// bound once (binding scans the sensitivity plane).
    fn physical(&self) -> Result<Physical<'_>, MinorCycleRunError> {
        Ok(match &self.physical {
            None => Physical::Identity,
            Some((sensitivity, response, sum_weight)) => Physical::Mosaic(
                MosaicSensitivity::new(sensitivity)?.with_normal_sum_weight(*sum_weight)?,
                response,
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
        weights,
    } = *inputs;
    let shape = plane_shape(normal, key);
    let taylor = normal.catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1;
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
    let (residual, psf, physical) = if taylor {
        read_taylor(normal, setup)?
    } else {
        read_single(normal, setup, key, weights)?
    };
    Ok(Some(PlaneData {
        shape,
        residual,
        psf,
        support: Support::new(shape, support),
        valid: Support::new(shape, valid),
        physical,
    }))
}

type Views<'a> = (
    Vec<Vec<f64>>,
    Vec<Vec<f64>>,
    Option<(Cow<'a, [f64]>, MinorCycleImageResponse, f64)>,
);

/// A direction-dependent response's weights for one image domain and
/// polarization (CASA's weight image of one store).
struct ResponseWeights {
    sensitivity: Vec<f64>,
    normal_weight: f64,
    published_weight: f64,
}

/// The response weights of every domain and polarization of `keys`, read
/// once per minor cycle; empty without a response and for a Taylor family,
/// which reads its own.
fn response_weights(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup,
    keys: &[PlaneKey],
) -> Result<BTreeMap<(usize, usize), ResponseWeights>, MinorCycleRunError> {
    let mut weights = BTreeMap::new();
    if setup.response.is_none() || normal.catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1
    {
        return Ok(weights);
    }
    let window = normal.read_window(normal.slab().core_range())?;
    for key in keys {
        if weights.contains_key(&(key.domain, key.polarization)) {
            continue;
        }
        let domain = window
            .domain(key.domain)
            .expect("plane keys name existing domains");
        let cells = plane_shape(normal, *key).len();
        let sensitivity = domain
            .sensitivity()
            .dense()
            .and_then(|dense| dense.get(key.polarization * cells..(key.polarization + 1) * cells))
            .expect("a direction-dependent response has a dense sensitivity")
            .to_vec();
        weights.insert(
            (key.domain, key.polarization),
            ResponseWeights {
                sensitivity,
                normal_weight: domain.sum_weights()[key.polarization],
                published_weight: domain.published_sum_weights()[key.polarization],
            },
        );
    }
    Ok(weights)
}

/// One plane's residual and PSF. Without a response both are divided by the
/// PSF peak; with one the residual takes CASA's normalisation
/// (`SIImageStore::divideResidualByWeight`) and the components convert back
/// with `divideModelByWeight`.
fn read_single<'a>(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup,
    key: PlaneKey,
    weights: &'a BTreeMap<(usize, usize), ResponseWeights>,
) -> Result<Views<'a>, MinorCycleRunError> {
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
        let residual = raw.iter().map(|value| value.re / peak).collect();
        return Ok((vec![residual], vec![normalise(psf, peak)], None));
    };
    let weights = &weights[&(key.domain, key.polarization)];
    let bound = MosaicSensitivity::new(&weights.sensitivity)?
        .with_normal_sum_weight(weights.normal_weight)?;
    let residual = raw
        .iter()
        .enumerate()
        .map(|(index, value)| {
            bound.normalize_weighted_residual_sample(
                value.re,
                index,
                response.normalization(),
                weights.published_weight,
                response.policy(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        vec![residual],
        vec![normalise(psf, peak)],
        Some((
            Cow::Borrowed(weights.sensitivity.as_slice()),
            response,
            weights.normal_weight,
        )),
    ))
}

/// The Taylor family's residual terms and `2·N_t − 1` PSF moments, scaled
/// alike: by the zeroth moment's peak, or under a response with the residuals
/// in CASA's units and the moments over the zeroth moment's sum weight.
fn read_taylor(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup,
) -> Result<Views<'static>, MinorCycleRunError> {
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
        let residual = raw
            .map(|term| term.iter().map(|value| value.re / peak).collect())
            .collect();
        let psf = moments
            .into_iter()
            .map(|moment| normalise(moment, peak))
            .collect();
        return Ok((residual, psf, None));
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
    let psf = moments
        .into_iter()
        .map(|moment| normalise(moment, normal_weight))
        .collect();
    Ok((
        residual,
        psf,
        Some((Cow::Owned(sensitivity), response, normal_weight)),
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
/// first components.
struct Solved {
    outcome: PlaneOutcome,
    terms: Vec<(usize, ModelDeltaTerm)>,
    trace: Vec<TracedComponent>,
}

/// Read and clean one plane. Returns Clark's refresh for reuse.
#[allow(clippy::too_many_arguments)]
fn solve(
    inputs: &Inputs<'_>,
    key: PlaneKey,
    summary: &PsfSummary,
    statistics: &PlaneStatistics,
    controls: &CycleControls,
    workers: usize,
    clark: Option<LinearRefresh>,
) -> (Result<Solved, MinorCycleRunError>, Option<LinearRefresh>) {
    let Inputs { base, setup, .. } = *inputs;
    let plane = match load(inputs, key) {
        Ok(Some(plane)) => plane,
        Ok(None) => unreachable!("prepared planes have valid data"),
        Err(error) => return (Err(error), clark),
    };
    let view = MinorCycleView {
        shape: plane.shape,
        residual: &plane.residual,
        psf: &plane.psf,
        summary,
        support: &plane.support,
        workers,
    };
    let run = |solver: &dyn PlaneSolve| solver.run(&view, controls, statistics);
    let (outcome, clark) = match &setup.algorithm {
        ReconstructionAlgorithm::Hogbom => (
            run(&Hogbom::new(
                setup.accounting == HogbomIterationAccounting::CasaInclusive,
            )),
            clark,
        ),
        ReconstructionAlgorithm::Clark => {
            let solver = Clark::new(clark);
            let outcome = run(&solver);
            (outcome, solver.into_refresh())
        }
        ReconstructionAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => (
            run(&Multiscale::new(scales_px.clone(), *small_scale_bias)),
            clark,
        ),
        ReconstructionAlgorithm::Mtmfs {
            scales_px,
            small_scale_bias,
        } => (
            run(&Taylor::new(
                plane.residual.len(),
                scales_px.clone(),
                *small_scale_bias,
            )),
            clark,
        ),
        algorithm => {
            return (Err(MinorCycleRunError::Algorithm(algorithm.clone())), clark);
        }
    };
    let solved = outcome.and_then(|outcome| {
        let terms = model_terms(&outcome.delta, &plane, base, key)?;
        let trace = trace(&outcome.trace, &plane, key, setup);
        Ok(Solved {
            outcome,
            terms,
            trace,
        })
    });
    (solved, clark)
}

/// [`run_plane`] behind one object-safe signature for the solver choice.
trait PlaneSolve {
    fn run(
        &self,
        view: &MinorCycleView<'_>,
        controls: &CycleControls,
        statistics: &PlaneStatistics,
    ) -> Result<PlaneOutcome, MinorCycleRunError>;
}

impl<S: Solver> PlaneSolve for S {
    fn run(
        &self,
        view: &MinorCycleView<'_>,
        controls: &CycleControls,
        statistics: &PlaneStatistics,
    ) -> Result<PlaneOutcome, MinorCycleRunError> {
        Ok(run_plane(self, view, controls, statistics, TRACE)?)
    }
}

/// The plane's components as model terms keyed by model order: a cube
/// plane's coefficient is its channel, a Taylor term's its term.
fn model_terms(
    delta: &Delta,
    plane: &PlaneData,
    base: &ModelGeneration,
    key: PlaneKey,
) -> Result<Vec<(usize, ModelDeltaTerm)>, MinorCycleRunError> {
    let mut terms = Vec::new();
    let physical = plane.physical()?;
    for term in 0..delta.term_count() {
        let coefficient = if delta.term_count() > 1 {
            term
        } else {
            key.channel
        };
        for (index, flux) in delta.term(term) {
            let value = physical.apply(flux, index)?;
            if value == 0.0 {
                continue;
            }
            let cell = ModelCell::new(
                key.domain,
                coefficient,
                key.polarization,
                plane.shape.pixel(index),
            );
            let flat = base
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
    key: PlaneKey,
    setup: &MinorCycleSetup,
) -> Vec<TracedComponent> {
    let scales = match &setup.algorithm {
        ReconstructionAlgorithm::Multiscale { scales_px, .. }
        | ReconstructionAlgorithm::Mtmfs { scales_px, .. } => scales_px.as_slice(),
        _ => &[],
    };
    components
        .iter()
        .map(|component| TracedComponent {
            cell: ModelCell::new(
                key.domain,
                if plane.residual.len() > 1 {
                    0
                } else {
                    key.channel
                },
                key.polarization,
                plane.shape.pixel(component.index),
            ),
            flux: component.flux,
            scale_px: scales.get(component.scale).copied().unwrap_or(0.0),
        })
        .collect()
}
