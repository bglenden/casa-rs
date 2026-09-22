// SPDX-License-Identifier: LGPL-3.0-or-later

//! Private, compile-time comparison composition. The ordinary application CLEAN
//! loop and product writer are unchanged; there is no runtime fallback selector.

use super::*;

pub(super) fn workers(runtime: &ApplicationRuntime) -> Result<usize, ApplicationError> {
    let ResourcePolicy::Explicit(policy) = &runtime.resource_policy else {
        return Err(boxed(
            "streaming comparison requires an explicit worker ceiling",
        ));
    };
    usize::try_from(
        policy
            .workers
            .filter(|n| *n > 0)
            .ok_or_else(|| boxed("streaming comparison lacks workers"))?,
    )
    .map_err(Into::into)
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
