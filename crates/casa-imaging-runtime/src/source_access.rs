// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded content budget of the selected observation's source.

use casa_imaging_model::CompiledProblem;
use casa_ms::{
    BoundSelectedObservationError, ResolvedSelectedObservationAccess,
    SelectedObservationContentBudget, SelectedObservationContentPlanError,
};

use crate::{ResourceAuthority, ResourceError, ResourcePolicy};

/// The budget that bounds source inspection before the compiled problem
/// can quote its complete initialization and traversal requirements.
#[must_use]
pub const fn bootstrap_source_budget() -> SelectedObservationContentBudget {
    SelectedObservationContentBudget::new(64 << 20, 2, 4)
}

/// Why a source's execution envelope could not be finalized.
#[derive(Debug, thiserror::Error)]
pub enum SourceAccessError {
    /// The storage owner could not quote or bind the source's requirements.
    #[error("selected-observation access: {0}")]
    Access(#[from] BoundSelectedObservationError),
    /// The requirement curve could not be evaluated or planned.
    #[error("selected-observation content plan: {0}")]
    ContentPlan(#[from] SelectedObservationContentPlanError),
    /// The policy's memory cannot hold the source's minimum envelope.
    #[error(transparent)]
    Resource(#[from] ResourceError),
    /// The preferred envelope does not fit the address space.
    #[error("the selected-observation envelope overflows")]
    Overflow,
}

/// Finalize an unopened source's bounded execution envelope.
///
/// The storage owner supplies the requirement curve; the runtime selects at
/// most the bootstrap budget of preferred growth beyond its mandatory minimum
/// under the current policy. The returned budget charges the actual bounded
/// plan, not all available host memory.
pub fn finalize_source_access(
    problem: &CompiledProblem,
    access: ResolvedSelectedObservationAccess,
    authority: &ResourceAuthority,
    policy: &ResourcePolicy,
) -> Result<ResolvedSelectedObservationAccess, SourceAccessError> {
    let requirements = access.content_requirements(problem)?;
    let maximum_live_blocks = access
        .source_binding()
        .content_budget()
        .maximum_live_blocks();
    let minimum = requirements.minimum_bytes(maximum_live_blocks)?;
    let available = authority.remaining_selected_source_memory_bytes(policy)?;
    let required = u64::try_from(minimum).map_err(|_| SourceAccessError::Overflow)?;
    if required > available {
        return Err(ResourceError::Infeasible {
            resource: "selected-observation host memory".to_string(),
            required,
            available,
        }
        .into());
    }
    let preferred = minimum
        .checked_add(bootstrap_source_budget().available_bytes())
        .ok_or(SourceAccessError::Overflow)?;
    let budget = SelectedObservationContentBudget::new(
        preferred.min(usize::try_from(available).unwrap_or(usize::MAX)),
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    let planned = requirements.plan(budget)?;
    let budget = SelectedObservationContentBudget::new(
        planned.maximum_resident_bytes(),
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    Ok(access.with_content_budget(problem, &requirements, budget)?)
}
