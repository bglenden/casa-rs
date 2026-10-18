// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a minor cycle holds ([`prepare_bytes`], [`run_bytes`],
//! [`mask_bytes`]), from the layout of the image state it cleans
//! ([`MinorLayout`]): a completion's normal state, or the layout a run's
//! first pass will form, from which the run chooses its workers before any
//! image exists (#737).

use casa_imaging_deconvolution::{
    Clark, CycleControls, FinalNormalState, Hogbom, ImageDomainReconstructionMaskPlans,
    MajorCycleCompletion, Multiscale, NormalBasis, PlaneShape, PlaneStop, Taylor,
};
use casa_imaging_model::{CompiledProblem, ReconstructionAlgorithm, ReconstructionBasis};

use super::{MinorCycleSetup, PsfCache, Slot, Solved, TRACE, is_taylor, plane_keys, plane_shape};

/// The layout of the image state a minor cycle cleans, as its charges read
/// it: how many planes it loads and the largest, its image domains'
/// shapes, its residual and PSF term counts, and whether its planes are
/// read from a paged channel-local state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinorLayout {
    planes: usize,
    shape: PlaneShape,
    domains: Vec<[usize; 2]>,
    terms: (u64, u64),
    paged: bool,
}

impl MinorLayout {
    /// The layout of `normal`.
    #[must_use]
    pub fn of(normal: &FinalNormalState) -> Self {
        let taylor = is_taylor(normal);
        let keys = plane_keys(normal, taylor);
        Self {
            planes: keys.len(),
            shape: keys
                .iter()
                .map(|key| plane_shape(normal, *key))
                .max_by_key(|shape| shape.len())
                .unwrap_or(PlaneShape::new(0, 0)),
            domains: (0..normal.domain_count())
                .filter_map(|domain| normal.domain_shape(domain))
                .collect(),
            terms: if taylor {
                (
                    normal.coefficient_term_count() as u64,
                    normal.normal_moment_count() as u64,
                )
            } else {
                (1, 1)
            },
            paged: normal.basis() == NormalBasis::ChannelLocal,
        }
    }

    /// The layout the first major cycle of `problem` forms: one Taylor
    /// family of `T` residual terms and `2T − 1` normal moments on the first
    /// image domain, or a plane per polarization, image domain and output
    /// channel, paged when there is a plane per channel.
    #[must_use]
    pub fn planned(problem: &CompiledProblem) -> Self {
        let target = problem.model_lifecycle().target();
        let domains = target
            .domains()
            .iter()
            .map(|domain| domain.pixels())
            .collect::<Vec<_>>();
        let largest = |domains: &[[usize; 2]]| {
            domains
                .iter()
                .map(|&[nx, ny]| PlaneShape::new(nx, ny))
                .max_by_key(|shape| shape.len())
                .unwrap_or(PlaneShape::new(0, 0))
        };
        let per_channel = target.polarizations() * domains.len();
        let (planes, shape, terms, paged) = match problem.reconstruction().basis() {
            ReconstructionBasis::Taylor { terms } => (
                1,
                largest(&domains[..domains.len().min(1)]),
                (terms as u64, (2 * terms).saturating_sub(1) as u64),
                false,
            ),
            ReconstructionBasis::ChannelLocal { channels } => {
                (per_channel * channels, largest(&domains), (1, 1), true)
            }
            ReconstructionBasis::Constant => (per_channel, largest(&domains), (1, 1), false),
        };
        Self {
            planes,
            shape,
            domains,
            terms,
            paged,
        }
    }

    /// Heap bytes the masks a minor cycle forms keep while the cycle runs
    /// and after it: one support per image domain.
    #[must_use]
    pub fn mask_bytes(&self) -> u64 {
        self.domains
            .iter()
            .map(|[width, height]| (width * height * size_of::<bool>()) as u64)
            .sum()
    }

    /// Heap bytes one loaded plane holds at most ([`super::load`]): its
    /// residual and PSF terms (`f64`), its support and validity, its
    /// sensitivity under a response and, from a paged channel-local state,
    /// the plane's fields read as `Complex64`; or, before any of them, its
    /// validity beside the one model plane read at a time to find the valid
    /// cells. A constant or Taylor state is resident and its planes are
    /// borrowed, not read (#738).
    fn load_bytes(&self, response: bool) -> u64 {
        let terms = self.terms.0 + self.terms.1;
        let loaded = terms * size_of::<f64>() as u64
            + 2 * size_of::<bool>() as u64
            + if response { size_of::<f64>() as u64 } else { 0 }
            + if self.paged {
                terms * size_of::<num_complex::Complex64>() as u64
            } else {
                0
            };
        let validity = (size_of::<casa_imaging_model::ModelSample>() + size_of::<bool>()) as u64;
        self.shape.len() as u64 * loaded.max(validity)
    }

    /// Planes a phase of `workers` loads at once: one when a single plane
    /// spreads over the team.
    fn concurrent(&self, workers: usize) -> u64 {
        workers.min(self.planes).max(1) as u64
    }
}

/// Heap bytes [`super::prepare_minor_cycle`] holds at most: the masks it
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
    let fits = plane_keys(normal, is_taylor(normal))
        .iter()
        .any(|key| !cache.summaries.contains_key(key));
    prepare_bytes_for(&MinorLayout::of(normal), mask_plans, setup, fits, workers)
}

/// [`prepare_bytes`] for a state of `layout`, with `fits` when some plane's
/// PSF is not yet measured, as before the first minor cycle.
#[must_use]
pub fn prepare_bytes_for(
    layout: &MinorLayout,
    mask_plans: &ImageDomainReconstructionMaskPlans,
    setup: &MinorCycleSetup,
    fits: bool,
    workers: usize,
) -> u64 {
    let measure = layout.shape.len() as u64
        * if setup.nsigma > 0.0 {
            2 * size_of::<f64>() as u64
        } else {
            0
        }
        .max(if fits { 2 * size_of::<f32>() as u64 } else { 0 });
    mask_plans.materialize_bytes_for(|domain| layout.domains.get(domain).copied())
        + layout.concurrent(workers) * (layout.load_bytes(setup.response.is_some()) + measure)
}

/// Heap bytes the masks a minor cycle forms keep while the cycle runs and
/// after it: one support per image domain.
#[must_use]
pub fn mask_bytes(completion: &MajorCycleCompletion) -> u64 {
    MinorLayout::of(completion.normal_state()).mask_bytes()
}

/// Heap bytes [`super::run_minor_cycle`] holds at most under `controls`,
/// besides each plane's terms ([`super::result_bytes`]): on each of the
/// planes solved at once on `workers` (one when a single plane spreads over
/// the team), the load and the solve
/// ([`casa_imaging_deconvolution::solve_bytes`]), and a record of every
/// plane. A Clark refresh already in `cache` is charged with the cache, not
/// here.
#[must_use]
pub fn run_bytes(
    completion: &MajorCycleCompletion,
    setup: &MinorCycleSetup,
    controls: &CycleControls,
    cache: &PsfCache,
    workers: usize,
) -> u64 {
    run_bytes_for(
        &MinorLayout::of(completion.normal_state()),
        setup,
        controls.iterations,
        cache.refresh_bytes(completion),
        workers,
    )
}

/// [`run_bytes`] for a state of `layout`, cleaning at most `iterations` per
/// plane beside a cached Clark refresh of `refresh` bytes.
#[must_use]
pub fn run_bytes_for(
    layout: &MinorLayout,
    setup: &MinorCycleSetup,
    iterations: usize,
    refresh: u64,
    workers: usize,
) -> u64 {
    let shape = layout.shape;
    let terms = layout.terms.0 as usize;
    let components = iterations + 1;
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
            if layout.planes == 1 {
                solve.saturating_sub(refresh)
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
    // Every plane's slot, solved record and stop, which the cycle keeps
    // until it merges them.
    let records =
        layout.planes * (size_of::<Slot>() + size_of::<Solved>() + size_of::<PlaneStop>());
    layout.concurrent(workers) * (layout.load_bytes(setup.response.is_some()) + solve)
        + records as u64
}
