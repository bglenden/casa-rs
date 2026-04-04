// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-table compatibility support for `casa-rs`.
//!
//! This crate starts the calibration workflow substrate planned for
//! `casars`. The first wave focuses on reading and validating CASA-produced
//! calibration tables, normalising the metadata needed for later apply/solve
//! work, and emitting machine-readable summaries that can anchor slow parity
//! tests against CASA.
//!
//! The public API is intentionally narrow in the first wave:
//!
//! - [`summarize_table`] opens a calibration table written by CASA or Rust and
//!   returns a normalized [`CalibrationTableSummary`].
//! - [`summarize_tables`] batches the same operation for CLI callers.
//! - [`CalibrationTableSummary::supported_for_v1_apply`] indicates whether the
//!   table falls inside the initial `Complex`/`CPARAM` family targeted by the
//!   upcoming `applycal`-class workflow.
//!
//! The implementation is built on [`casacore_tables::Table`] rather than a new
//! storage stack, so every on-disk assumption made here is exercised against
//! the same table reader/writer substrate used elsewhere in the repo.

mod cli;
pub mod constants;
mod model;
mod summary;

pub use cli::run_env;
pub use model::{
    CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
    CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
    CalibrationValidationIssue, TimeCoverageSummary,
};
pub use summary::{CalibrationTableError, summarize_table, summarize_tables};
