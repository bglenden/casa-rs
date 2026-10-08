// SPDX-License-Identifier: LGPL-3.0-or-later

//! Identities of a run's imaging-weight generation and of its traversals of
//! the selected visibilities; they name live owners, not array contents.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_WEIGHTING_OWNER: AtomicU64 = AtomicU64::new(1);
static NEXT_REPLAY_OWNER: AtomicU64 = AtomicU64::new(1);

fn next_owner(counter: &AtomicU64) -> u64 {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("process-wide identities exhausted")
}

/// Identity of one run's imaging-weight generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WeightingGenerationId(u64);

impl WeightingGenerationId {
    /// A fresh identity for one run's imaging-weight generation.
    ///
    /// # Panics
    ///
    /// When the process has minted `u64::MAX` generations.
    #[must_use]
    pub fn next() -> Self {
        Self(next_owner(&NEXT_WEIGHTING_OWNER))
    }

    pub(crate) const fn ordinal(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WeightingGenerationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Identity of one traversal of the selected visibilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WeightingReplayId(u64);

impl WeightingReplayId {
    /// A fresh identity for one traversal of the selected visibilities.
    ///
    /// # Panics
    ///
    /// When the process has minted `u64::MAX` traversals.
    #[must_use]
    pub fn next() -> Self {
        Self(next_owner(&NEXT_REPLAY_OWNER))
    }

    pub(crate) const fn ordinal(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WeightingReplayId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
pub(crate) fn native_normal_fixture_weighting_ids() -> (WeightingGenerationId, WeightingReplayId) {
    (WeightingGenerationId(1), WeightingReplayId(1))
}
