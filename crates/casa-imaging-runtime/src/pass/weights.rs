// SPDX-License-Identifier: LGPL-3.0-or-later
//! The imaging weights a run keeps for its samples across major cycles.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use casa_imaging_operator::WeightSlots;

/// The imaging weight of every sample a single-wave, single-domain pass
/// meets, kept across a run's passes ([`WeightSlots`]): one slot per
/// (row, native channel), in the order the pass meets the rows.
///
/// Every pass of a run walks the same rows in the same blocks, and the
/// weights depend only on a sample and the run's frozen weighting. The first
/// complete pass fills the slots; later passes read them in place of the
/// flag test and the density lookup.
#[derive(Debug)]
pub struct WeightCache {
    slots: Vec<AtomicU32>,
    filled: AtomicBool,
}

impl WeightCache {
    /// Bytes a cache of `samples` slots holds.
    #[must_use]
    pub const fn bytes(samples: usize) -> u64 {
        (samples * size_of::<AtomicU32>()) as u64
    }

    /// An empty cache of `samples` slots.
    #[must_use]
    pub fn new(samples: usize) -> Self {
        Self {
            slots: (0..samples).map(|_| AtomicU32::new(0)).collect(),
            filled: AtomicBool::new(false),
        }
    }

    /// Whether a complete pass has filled the slots.
    #[must_use]
    pub fn filled(&self) -> bool {
        self.filled.load(Ordering::Acquire)
    }

    /// The slots of the `channels` samples from `offset`, read when
    /// `filled` and kept otherwise; `None` past the cache's end.
    pub(super) fn row(
        &self,
        offset: usize,
        channels: usize,
        filled: bool,
    ) -> Option<WeightSlots<'_>> {
        self.slots
            .get(offset..offset.checked_add(channels)?)
            .map(|slots| WeightSlots { slots, filled })
    }

    /// Mark the slots filled once a pass has met every sample.
    pub(super) fn mark_filled(&self) {
        self.filled.store(true, Ordering::Release);
    }
}
