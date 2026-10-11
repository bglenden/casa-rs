// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded content budget of the selected observation's source.

use casa_imaging_model::CompiledProblem;
use casa_ms::{
    BoundSelectedObservationError, ResolvedSelectedObservationAccess,
    SelectedObservationContentBudget, SelectedObservationContentPlanError,
};

use crate::pass::{BlockShape, source_block_bytes};
use crate::{Admission, Demand, HostResources, Reservation, ResourcePolicy, admit, free_memory};

/// One row of the blocks `problem`'s source delivers: the most channels any
/// selected spectral window gives a row, the most correlations any selection
/// routes, and the image domains each row is projected on; no rows.
#[must_use]
pub fn row_layout(problem: &CompiledProblem) -> BlockShape {
    let sources = problem.observation_transaction().read_set().sources();
    BlockShape {
        rows: 0,
        channels: sources
            .iter()
            .flat_map(|source| source.selection().spectral_windows())
            .map(|window| window.channel_indices().len())
            .max()
            .unwrap_or(0),
        correlations: sources
            .iter()
            .flat_map(|source| source.selection().correlations())
            .map(|selection| selection.products().len())
            .max()
            .unwrap_or(0),
        domains: problem.geometry().domains().len(),
    }
}

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

/// Finalize an unopened source's bounded execution envelope and admit it,
/// with the stream that converts its blocks for the passes.
///
/// The storage owner supplies the requirement curve; the source takes its
/// mandatory minimum and grows beyond it by at most the bootstrap budget and
/// at most a quarter of what `policy` leaves free on `host` past that
/// minimum, so the paged cube cache and the passes, admitted after it, keep
/// the rest. Each row a block holds is also converted into the native
/// blocks a pass's stream double-buffers, and predicted by a pass with a
/// model ([`source_block_bytes`]), so the growth is shared between the
/// source and the passes in proportion to their bytes per row. The
/// reservation holds the envelope the source plans and what the passes hold
/// for its blocks, and the run keeps it while the source is open.
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
    let minimum = requirements.minimum_bytes()?;
    let free = usize::try_from(free_memory(host, policy)).unwrap_or(usize::MAX);
    let growth = bootstrap_source_budget()
        .available_bytes()
        .min(free.saturating_sub(minimum) / 4);
    let budget = |growth: usize| {
        Ok::<_, SourceAccessError>(SelectedObservationContentBudget::new(
            minimum
                .checked_add(growth)
                .ok_or(SourceAccessError::Overflow)?,
            maximum_live_blocks,
            requirements.maximum_pointing_polynomial_terms(),
        ))
    };
    let layout = row_layout(problem);
    let passes = |rows: usize| source_block_bytes(BlockShape { rows, ..layout });
    let passes_per_row = passes(1) - passes(0);
    let unshared = requirements.plan(budget(growth)?)?;
    let source_per_row =
        (unshared.maximum_resident_bytes() / unshared.rows_per_block().max(1)) as u64;
    let shared = u128::from(source_per_row) * growth as u128
        / u128::from((source_per_row + passes_per_row).max(1));
    let planned = requirements.plan(budget(
        usize::try_from(shared).map_err(|_| SourceAccessError::Overflow)?,
    )?)?;
    let passes = passes(planned.rows_per_block());
    tracing::info!(
        rows_per_block = planned.rows_per_block(),
        envelope_bytes = planned.maximum_resident_bytes(),
        pass_bytes = passes,
        minimum_bytes = minimum,
        live_blocks = maximum_live_blocks,
        "selected-observation source plan"
    );
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "selected-observation source",
            memory: u64::try_from(planned.maximum_resident_bytes())
                .map_err(|_| SourceAccessError::Overflow)?
                .checked_add(passes)
                .ok_or(SourceAccessError::Overflow)?,
        },
    )?;
    let budget = SelectedObservationContentBudget::new(
        planned.maximum_resident_bytes(),
        maximum_live_blocks,
        requirements.maximum_pointing_polynomial_terms(),
    );
    Ok((access.with_content_budget(budget), reservation))
}
