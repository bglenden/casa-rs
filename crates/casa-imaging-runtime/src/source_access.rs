// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded content budget of the selected observation's source.

use casa_imaging_model::CompiledProblem;
use casa_ms::{
    BoundSelectedObservationError, ResolvedSelectedObservationAccess,
    SelectedObservationContentBudget, SelectedObservationContentPlanError,
};

use crate::{Admission, Demand, HostResources, Reservation, ResourcePolicy, admit, free_memory};

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
    /// The policy's free memory cannot hold the source's envelope.
    #[error(transparent)]
    Admission(#[from] Admission),
    /// The preferred envelope does not fit the address space.
    #[error("the selected-observation envelope overflows")]
    Overflow,
}

/// Finalize an unopened source's bounded execution envelope and admit it.
///
/// The storage owner supplies the requirement curve; the source takes its
/// mandatory minimum and grows beyond it by at most the bootstrap budget and
/// at most a quarter of what `policy` leaves free on `host` past that
/// minimum, so the paged cube cache and the passes, admitted after it, keep
/// the rest. The reservation holds the envelope the source plans, and the
/// run keeps it while the source is open.
pub fn finalize_source_access(
    problem: &CompiledProblem,
    access: ResolvedSelectedObservationAccess,
    host: &HostResources,
    policy: &ResourcePolicy,
) -> Result<(ResolvedSelectedObservationAccess, Reservation), SourceAccessError> {
    let requirements = access.content_requirements(problem)?;
    let maximum_live_blocks = access
        .source_binding()
        .content_budget()
        .maximum_live_blocks();
    let minimum = requirements.minimum_bytes(maximum_live_blocks)?;
    let free = usize::try_from(free_memory(host, policy)).unwrap_or(usize::MAX);
    let growth = bootstrap_source_budget()
        .available_bytes()
        .min(free.saturating_sub(minimum) / 4);
    let budget = SelectedObservationContentBudget::new(
        minimum
            .checked_add(growth)
            .ok_or(SourceAccessError::Overflow)?,
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    let planned = requirements.plan(budget)?;
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "selected-observation source",
            memory: u64::try_from(planned.maximum_resident_bytes())
                .map_err(|_| SourceAccessError::Overflow)?,
        },
    )?;
    let budget = SelectedObservationContentBudget::new(
        planned.maximum_resident_bytes(),
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    Ok((
        access.with_content_budget(problem, &requirements, budget)?,
        reservation,
    ))
}
