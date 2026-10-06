// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native cube phases for the ordinary application CLEAN loop and product writer.

use super::*;
use casa_imaging_runtime::{
    CubePhase, ExecutionPlan, FinalMajorPhaseInput, MajorCycleOperatorResult, NativeReplay,
    ReconstructionCyclePhaseCompletion,
};
use prepared_aw_phase::PreparedAwPlanBinding;

impl MajorCyclePhase for CubePhase {
    type Replay = NativeReplay;

    fn initial(
        context: PhaseContext<'_>,
        access: ResolvedSelectedObservationAccess,
        aw: Option<PreparedAwPlanBinding>,
        initial_write: bool,
        _: SelectedVisibilityWriteTargets,
        _: &SelectedObservationResolutionRequest,
    ) -> Result<(ExecutionPlan, Self, Option<FinalVisibilityReplay>), ApplicationError> {
        if aw.is_some() || initial_write {
            return Err(boxed(
                "native cube phase received unsupported observation output",
            ));
        }
        let (physical, executor) = Self::initial(
            context.problem.clone(),
            context.registry,
            context.policy,
            context.runtime.gridded_normal_storage.clone(),
            access.into_deferred(),
            context.minor,
        )?;
        Ok((
            admit(context.problem, context.runtime, context.registry, physical)?,
            executor,
            None,
        ))
    }

    fn refresh(
        context: PhaseContext<'_>,
        input: FinalMajorPhaseInput,
        ordinal: u32,
        replay: Self::Replay,
        aw: Option<PreparedAwPlanBinding>,
    ) -> Result<(ExecutionPlan, Self), ApplicationError> {
        if aw.is_some() {
            return Err(boxed("native cube phase received an AW binding"));
        }
        let (physical, executor) = Self::refresh(
            context.problem.clone(),
            context.registry,
            context.policy,
            context.runtime.gridded_normal_storage.clone(),
            replay,
            input,
            ordinal,
            context.minor,
        )?;
        Ok((
            admit(context.problem, context.runtime, context.registry, physical)?,
            executor,
        ))
    }

    fn take_replay(&self) -> Result<Self::Replay, ApplicationError> {
        self.take_native_replay()
            .ok_or_else(|| boxed("major phase omitted native replay ownership"))
    }

    fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.take_completion()
    }

    fn visibility_weighting(
        _: Self::Replay,
    ) -> Result<casa_imaging_runtime::FrozenWeightingArtifact, ApplicationError> {
        Err(boxed("native cube does not own visibility output"))
    }

    fn take_reconstruction_cycle_completion(&self) -> Option<ReconstructionCyclePhaseCompletion> {
        self.take_reconstruction_cycle_completion()
    }
}

pub(super) fn minor_program(
    problem: &CompiledProblem,
    response: Option<MinorCycleImageResponse>,
    remaining: Option<usize>,
) -> Result<MinorCycleProgram, ApplicationError> {
    let mut program = MinorCycleProgram::for_problem(problem)?.record_component_sequence(64)?;
    if let Some(remaining) = remaining {
        program = program.limit_iterations(remaining)?;
    }
    if let Some(response) = response {
        program = program.with_image_response(response);
    }
    Ok(program)
}

pub(super) fn admit(
    problem: &CompiledProblem,
    runtime: &ApplicationRuntime,
    registry: &impl ImplementationRegistry,
    physical: casa_imaging_runtime::PhysicalWorkBinding,
) -> Result<casa_imaging_runtime::ExecutionPlan, ApplicationError> {
    Ok(plan(
        problem,
        PlanningBindings::new(
            runtime.registry,
            runtime.resource_policy.clone(),
            runtime.cost_model,
        ),
        &runtime.authority,
        registry,
        &runtime.receipts,
        |_, _| Ok::<_, std::convert::Infallible>(vec![physical.clone()]),
    )?)
}
