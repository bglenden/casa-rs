// SPDX-License-Identifier: LGPL-3.0-or-later

//! The record of visibilities written back by the final major-cycle pass.

/// Completed visibility write: the number of cells (row, channel,
/// correlation) written; no content certificate.
///
/// Nothing checks that the written addresses are canonical or distinct: the
/// writing pass holds every plane in one traversal of the selection, which
/// visits each selected row once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityProductCompletion {
    sample_count: u64,
}

impl VisibilityProductCompletion {
    /// The record of `sample_count` visibilities written.
    #[must_use]
    pub const fn new(sample_count: u64) -> Self {
        Self { sample_count }
    }

    /// Return the number of cells written.
    #[must_use]
    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }
}
