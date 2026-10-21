// SPDX-License-Identifier: LGPL-3.0-or-later
//! Errors of a simulator session, with CASA's way of reporting each.

use thiserror::Error;

/// How CASA's `sm` tool reports a failure, which a CASA-compatible binding
/// reproduces: scripts test the boolean a method returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasaOutcome {
    /// CASA logs the message as SEVERE and the method returns `False`.
    ReturnsFalse,
    /// CASA raises `RuntimeError` with the message.
    Raises,
    /// CASA does this, and casa-rs does not yet: a binding raises
    /// `NotImplementedError` with the message, which names the simulation
    /// phase that implements it.
    NotYetImplemented,
}

/// A simulator session error.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct SimulatorError {
    message: String,
    outcome: CasaOutcome,
}

impl SimulatorError {
    /// An error CASA reports by logging it and returning `False`.
    pub fn refused(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            outcome: CasaOutcome::ReturnsFalse,
        }
    }

    /// An error CASA raises.
    pub fn raised(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            outcome: CasaOutcome::Raises,
        }
    }

    /// Something CASA does that casa-rs does not yet; `message` names the
    /// simulation phase that implements it.
    pub fn not_yet_implemented(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            outcome: CasaOutcome::NotYetImplemented,
        }
    }

    /// How CASA reports this error.
    pub fn outcome(&self) -> CasaOutcome {
        self.outcome
    }

    /// The message, as CASA words it where CASA reports the same failure.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// A session result.
pub type SimulatorResult<T> = Result<T, SimulatorError>;
