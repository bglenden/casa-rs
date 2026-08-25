// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact failure reasons for continuum product construction.

use casa_imaging_model::{ProductRole, ProductTerm};
use thiserror::Error;

/// Exact reason product planning, production, or authorization failed closed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProductsError {
    /// Production controls were outside their validated ranges.
    #[error("continuum production controls are invalid")]
    InvalidControls,
    /// The compiled problem is not a supported single-plane continuum problem.
    #[error("problem is not a supported single-plane constant-basis continuum problem")]
    UnsupportedProblem,
    /// The Product Graph requested a role this catalog version cannot produce.
    #[error("product role {role:?} is not producible by algorithm catalog {catalog}")]
    UnsupportedProductRole {
        /// The first unsupported role encountered.
        role: ProductRole,
        /// Algorithm catalog version consulted.
        catalog: u32,
    },
    /// A Taylor term other than the zeroth coefficient was requested.
    #[error("Taylor term {term:?} exceeds the nterms=1 continuum algorithm catalog")]
    UnsupportedTaylorTerm {
        /// The offending Taylor term.
        term: ProductTerm,
    },
    /// The source evidence did not come from the same Major-Cycle result.
    #[error("source evidence does not match the committed Major-Cycle lineage")]
    SourceLineageMismatch,
    /// The planned generation does not belong to this authority or graph.
    #[error("planned generation does not match this authority and Product Graph")]
    ForeignPlannedGeneration,
    /// The completions do not carry the exact committed source evidence.
    #[error("completions do not match the planned source commitments")]
    CommitmentMismatch,
    /// A produced member did not have its claimed content identity.
    #[error("produced member content differs from its claimed identity")]
    MemberContentMismatch,
    /// The produced member set did not exactly match the planned members.
    #[error(
        "produced member set differs from the plan: expected {expected} members, found {actual}"
    )]
    MemberSetMismatch {
        /// Planned member count.
        expected: usize,
        /// Produced member count.
        actual: usize,
    },
    /// A produced payload length disagreed with its declared shape.
    #[error("member payload requires {expected} values but carries {actual}")]
    PayloadLengthMismatch {
        /// Shape-derived value count.
        expected: usize,
        /// Actual payload length.
        actual: usize,
    },
    /// Beam fitting failed for a graph that requires fitted beam metadata.
    #[error("restoring-beam fitting failed: {0}")]
    BeamFitFailed(String),
    /// Solver arithmetic produced a non-finite product value.
    #[error("product arithmetic generated a non-finite value")]
    GeneratedNonfinite,
}
