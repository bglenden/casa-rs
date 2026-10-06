// SPDX-License-Identifier: LGPL-3.0-or-later

//! Application phase ownership for the single shared CLEAN controller.

use super::*;
use casa_imaging_runtime::{
    ExecutionPlan, FinalMajorPhaseInput, FrozenGriddedNormalReplay, FrozenWeightingArtifact,
    MajorCycleOperatorResult, ReconstructionCyclePhaseCompletion,
};
use prepared_aw_phase::PreparedAwPlanBinding;

pub(super) struct PhaseContext<'a> {
    pub problem: &'a CompiledProblem,
    pub runtime: &'a ApplicationRuntime,
    pub registry: &'a PlanningRegistry,
    pub policy: SpectralCycleExecutionPolicy,
    pub minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
}

/// Only phase storage differs between capabilities; CLEAN and publication are shared.
pub(super) trait MajorCyclePhase: WorkImplementation + Sized {
    type Replay;

    fn initial(
        context: PhaseContext<'_>,
        initial_access: ResolvedSelectedObservationAccess,
        aw: Option<PreparedAwPlanBinding>,
        initial_write: bool,
        write_targets: SelectedVisibilityWriteTargets,
        observation: &SelectedObservationResolutionRequest,
    ) -> Result<(ExecutionPlan, Self, Option<FinalVisibilityReplay>), ApplicationError>;

    fn refresh(
        context: PhaseContext<'_>,
        final_input: casa_imaging_runtime::FinalMajorPhaseInput,
        ordinal: u32,
        replay: Self::Replay,
        aw: Option<PreparedAwPlanBinding>,
    ) -> Result<(ExecutionPlan, Self), ApplicationError>;

    fn take_replay(&self) -> Result<Self::Replay, ApplicationError>;
    fn visibility_weighting(
        replay: Self::Replay,
    ) -> Result<FrozenWeightingArtifact, ApplicationError>;
    fn take_completion(&self) -> Option<MajorCycleOperatorResult>;
    fn take_reconstruction_cycle_completion(&self) -> Option<ReconstructionCyclePhaseCompletion>;
}

impl MajorCyclePhase for SpectralCycleExecutor {
    type Replay = (FrozenWeightingArtifact, FrozenGriddedNormalReplay);

    fn initial(
        context: PhaseContext<'_>,
        initial_access: ResolvedSelectedObservationAccess,
        aw: Option<PreparedAwPlanBinding>,
        initial_write: bool,
        write_targets: SelectedVisibilityWriteTargets,
        observation: &SelectedObservationResolutionRequest,
    ) -> Result<(ExecutionPlan, Self, Option<FinalVisibilityReplay>), ApplicationError> {
        let problem = context.problem;
        let runtime = context.runtime;
        let policy = context.policy;
        let planned = if context.minor.is_some() {
            SpectralCyclePlan::initial(problem, context.registry, policy)?
        } else {
            SpectralCyclePlan::dirty(problem, context.registry, policy)?
        };
        let replay_proof_bytes = initial_access.replay_proof_retained_heap_bytes(problem)?;
        let frozen_reservation = context
            .minor
            .is_some()
            .then(|| {
                FrozenWeightingReservation::acquire(
                    &runtime.authority,
                    runtime.resource_policy.clone(),
                    planned.weighting_plan().planned_residency(),
                    replay_proof_bytes,
                )
            })
            .transpose()?;
        let initial_plan = plan(
            problem,
            PlanningBindings::new(
                runtime.registry,
                runtime.resource_policy.clone(),
                runtime.cost_model,
            ),
            &runtime.authority,
            context.registry,
            &runtime.receipts,
            |_, _| Ok::<_, std::convert::Infallible>(planned.physical_candidates()),
        )?;
        let SpectralCyclePlanParts {
            weighting,
            complete_data: complete,
            source_resources: resources,
            pass,
            minor_cycle_node: minor_node,
            gridded_normal: planned_gridded_normal,
            ..
        } = planned.into_parts(&initial_plan)?;
        let initial_source_state = initial_access.source_state().clone();
        let mut executor = SpectralCycleExecutor::new(
            runtime.implementation.clone(),
            problem.clone(),
            weighting,
            resources,
            pass,
            complete,
            initial_access.into_deferred(),
            ExecutableModelProblem::from_compiled(problem.clone())?,
            SpectralCyclePassInput::Initial,
        );
        if let Some(binding) = aw {
            executor = executor.with_prepared_artifact_reader(binding.execution)?;
        }
        if let Some((masks, program)) = context.minor {
            executor = executor.with_frozen_weighting_reservation(
                frozen_reservation.expect("minor-cycle execution reserves frozen weighting"),
            );
            executor = executor.with_planned_gridded_normal_binding(
                planned_gridded_normal.ok_or_else(|| {
                    boxed("minor-cycle initial plan omitted gridded replay binding")
                })?,
            )?;
            executor = executor.with_reconstruction_cycle(
                minor_node.ok_or_else(|| boxed("initial plan omitted its minor-cycle node"))?,
                masks,
                program,
            );
        }
        let mut initial_terminal_replay = None;
        if initial_write {
            let (replay, sink) = FinalVisibilityReplay::with_visibility_write(
                std::path::PathBuf::from(observation.locator()),
                initial_source_state,
                visibility_write_selection(problem, observation.selection())?,
                write_targets,
            )?;
            executor = executor.with_final_visibility_sink(sink);
            initial_terminal_replay = Some(replay);
        }
        Ok((initial_plan, executor, initial_terminal_replay))
    }

    fn refresh(
        context: PhaseContext<'_>,
        final_input: FinalMajorPhaseInput,
        ordinal: u32,
        (frozen_weighting, gridded_replay): Self::Replay,
        aw: Option<PreparedAwPlanBinding>,
    ) -> Result<(ExecutionPlan, Self), ApplicationError> {
        let problem = context.problem;
        let runtime = context.runtime;
        let policy = context.policy;
        let final_planned = if context.minor.is_some() {
            SpectralCyclePlan::continuing_major(
                problem,
                context.registry,
                policy,
                &final_input,
                ordinal,
                gridded_replay,
            )?
        } else {
            SpectralCyclePlan::final_major_at(
                problem,
                context.registry,
                policy,
                &final_input,
                ordinal,
                gridded_replay,
            )?
        };
        let final_plan = plan(
            problem,
            PlanningBindings::new(
                runtime.registry,
                runtime.resource_policy.clone(),
                runtime.cost_model,
            ),
            &runtime.authority,
            context.registry,
            &runtime.receipts,
            |_, _| Ok::<_, std::convert::Infallible>(final_planned.physical_candidates()),
        )?;
        let SpectralCyclePlanParts {
            weighting,
            complete_data: complete,
            pass,
            minor_cycle_node: minor_node,
            gridded_normal: planned_gridded_normal,
            ..
        } = final_planned.into_parts(&final_plan)?;
        let mut executor = SpectralCycleExecutor::new_gridded(
            runtime.implementation.clone(),
            problem.clone(),
            weighting,
            pass,
            complete,
            ExecutableModelProblem::from_compiled(problem.clone())?,
            SpectralCyclePassInput::FinalMajor(final_input),
            planned_gridded_normal
                .ok_or_else(|| boxed("later-major plan omitted gridded replay binding"))?,
        )?
        .with_frozen_weighting(frozen_weighting);
        if let Some(binding) = aw {
            executor = executor.with_prepared_artifact_reader(binding.execution)?;
        }
        if let Some((masks, program)) = context.minor {
            executor = executor.with_reconstruction_cycle(
                minor_node.ok_or_else(|| boxed("continuing plan omitted minor node"))?,
                masks,
                program,
            );
        }
        Ok((final_plan, executor))
    }

    fn take_replay(&self) -> Result<Self::Replay, ApplicationError> {
        Ok((
            self.take_frozen_weighting()
                .ok_or_else(|| boxed("major phase omitted reusable frozen weighting"))?,
            self.take_gridded_normal_replay()
                .ok_or_else(|| boxed("major phase omitted gridded-normal replay"))?,
        ))
    }

    fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.take_completion()
    }

    fn visibility_weighting(
        (weighting, _): Self::Replay,
    ) -> Result<FrozenWeightingArtifact, ApplicationError> {
        Ok(weighting)
    }

    fn take_reconstruction_cycle_completion(&self) -> Option<ReconstructionCyclePhaseCompletion> {
        self.take_reconstruction_cycle_completion()
    }
}
