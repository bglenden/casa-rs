// SPDX-License-Identifier: LGPL-3.0-or-later
//! The minor cycle of one major-cycle completion, with independent planes
//! solved on the worker team.

use casa_imaging_reconstruction::runtime_adapter::ReconstructionPlaneWork;
use casa_imaging_reconstruction::{
    AutoMultithreshEvidence, ChannelCyclePolicy, FinalModelContinuation, FinalNormalState,
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, MaskError, MinorCycleProgram,
    ModelDelta, ModelLifecycle, NormalStateCatalog, ReconstructionCycle, ReconstructionCycleError,
    ReconstructionCycleEvidence, ReconstructionCycleResult, ReconstructionMaskSet,
};

use crate::pass::WorkerTeam;

/// A failure of the minor cycle.
#[derive(Debug, thiserror::Error)]
pub enum MinorCycleRunError {
    /// The reconstruction masks could not be formed.
    #[error("reconstruction mask: {0}")]
    Mask(#[from] MaskError),
    /// The solver or its bookkeeping failed.
    #[error("minor cycle: {0}")]
    Cycle(#[from] ReconstructionCycleError),
}

/// Everything one minor cycle hands to the next major cycle and to products.
pub struct MinorCycleOutcome {
    /// The normal state the cycle cleaned.
    pub normal_state: FinalNormalState,
    /// The model generation it started from, with its completion.
    pub continuation: FinalModelContinuation,
    /// The supports components were placed on.
    pub masks: ReconstructionMaskSet,
    /// The accepted model update, when any component was accepted.
    pub delta: Option<ModelDelta>,
    /// Per-plane solver evidence.
    pub evidence: ReconstructionCycleEvidence,
    /// Auto-multithreshold diagnostics per image domain.
    pub auto_masks: Box<[Option<AutoMultithreshEvidence>]>,
}

/// Run one minor cycle on `completion` under `lifecycle`.
///
/// A Taylor family is solved jointly; independent channel and polarization
/// planes are solved concurrently on `team` after the shared cycle-threshold
/// prepass, and committed in canonical order, so the result does not depend
/// on the worker count. Several image domains share one Högbom cycle.
pub fn run_minor_cycle(
    completion: MajorCycleCompletion,
    lifecycle: &ModelLifecycle,
    mask_plans: &ImageDomainReconstructionMaskPlans,
    program: MinorCycleProgram,
    team: &WorkerTeam,
) -> Result<MinorCycleOutcome, MinorCycleRunError> {
    let (normal_state, continuation) = completion.into_continuation();
    let policy = if normal_state.catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1 {
        ChannelCyclePolicy::Coupled
    } else {
        ChannelCyclePolicy::Independent
    };
    let (masks, auto_masks) = mask_plans
        .materialize(continuation.generation(), &normal_state)?
        .into_parts();
    let cycle = ReconstructionCycle::new(policy, program);
    let base = continuation.generation();
    let result = if normal_state.catalog() == NormalStateCatalog::UnnormalizedPlaneV1
        && normal_state.domain_count() > 1
    {
        cycle.run_domains(lifecycle, base, &normal_state, &masks)?
    } else if policy == ChannelCyclePolicy::Independent {
        solve_planes(
            cycle.prepare_independent(lifecycle, base, &normal_state, masks.primary())?,
            team,
        )?
    } else {
        cycle.run(lifecycle, base, &normal_state, masks.primary())?
    };
    let (delta, evidence) = result.into_parts();
    Ok(MinorCycleOutcome {
        normal_state,
        continuation,
        masks: ReconstructionMaskSet::Domains(masks),
        delta,
        evidence,
        auto_masks,
    })
}

/// The statistics prepass and the plane solves, `workers` planes at a time
/// so at most that many planes are loaded; FFTW threads only for one plane.
fn solve_planes(
    mut work: ReconstructionPlaneWork<'_>,
    team: &WorkerTeam,
) -> Result<ReconstructionCycleResult, ReconstructionCycleError> {
    let workers = team.workers();
    let statistics = work.threshold_plane_count();
    for start in (0..statistics).step_by(workers) {
        let mut slots = (start..(start + workers).min(statistics))
            .map(|ordinal| (ordinal, None))
            .collect::<Vec<_>>();
        let shared = &work;
        team.for_each_mut(&mut slots, |_, (ordinal, out)| {
            *out = Some(shared.plane_statistics(*ordinal)?);
            Ok::<_, ReconstructionCycleError>(())
        })?;
        for (_, statistics) in slots {
            work.commit_statistics(statistics.expect("every prepass slot ran"))?;
        }
    }
    let planes = work.plane_count();
    if planes == 1 {
        let threads = if work.workspace().parallel_fft() {
            workers
        } else {
            1
        };
        let input = work.prepare_plane(0)?;
        let partial = work.execute_plane(&input, threads)?;
        work.commit_plane(partial)?;
        return work.finish();
    }
    for start in (0..planes).step_by(workers) {
        let mut slots = (start..(start + workers).min(planes))
            .map(|ordinal| work.prepare_plane(ordinal).map(|input| (input, None)))
            .collect::<Result<Vec<_>, _>>()?;
        let shared = &work;
        team.for_each_mut(&mut slots, |_, (input, out)| {
            *out = Some(shared.execute_plane(input, 1)?);
            Ok::<_, ReconstructionCycleError>(())
        })?;
        for (_, partial) in slots {
            work.commit_plane(partial.expect("every plane slot ran"))?;
        }
    }
    work.finish()
}
