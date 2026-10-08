// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded content budget of the selected observation's source.

use std::io;

use casa_imaging_model::CompiledProblem;
use casa_ms::{ResolvedSelectedObservationAccess, SelectedObservationContentBudget};

use crate::{ResourceAuthority, ResourceError, ResourcePolicy};

/// The budget that bounds source inspection before the compiled problem
/// can quote its complete initialization and traversal requirements.
#[must_use]
pub const fn bootstrap_source_budget() -> SelectedObservationContentBudget {
    SelectedObservationContentBudget::new(64 << 20, 2, 4)
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
) -> io::Result<ResolvedSelectedObservationAccess> {
    let requirements = access
        .content_requirements(problem)
        .map_err(io::Error::other)?;
    let maximum_live_blocks = access
        .source_binding()
        .content_budget()
        .maximum_live_blocks();
    let minimum = requirements
        .minimum_bytes(maximum_live_blocks)
        .map_err(io::Error::other)?;
    let available = authority
        .remaining_selected_source_memory_bytes(policy)
        .map_err(io::Error::other)?;
    let required = u64::try_from(minimum).map_err(io::Error::other)?;
    if required > available {
        return Err(io::Error::other(ResourceError::Infeasible {
            resource: "selected-observation host memory".to_string(),
            required,
            available,
        }));
    }
    let preferred = minimum
        .checked_add(bootstrap_source_budget().available_bytes())
        .ok_or_else(|| io::Error::other("selected-observation preferred envelope overflowed"))?;
    let budget = SelectedObservationContentBudget::new(
        preferred.min(usize::try_from(available).unwrap_or(usize::MAX)),
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    let planned = requirements.plan(budget).map_err(io::Error::other)?;
    let budget = SelectedObservationContentBudget::new(
        planned.maximum_resident_bytes(),
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    access
        .with_content_budget(problem, &requirements, budget)
        .map_err(io::Error::other)
}
