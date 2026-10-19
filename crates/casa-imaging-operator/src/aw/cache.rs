// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bound of a catalog's cell cache and what the catalog holds at any
//! bound, so a run can size the cache to the memory its workers leave
//! (`cf_resident_mb = auto`, #449).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::ARC_CELL_BYTES;
#[cfg(doc)]
use crate::convolution::CellHold;

/// The bound of a catalog's cell cache, in bytes of loaded cells: shared,
/// so a run may raise it once it knows the memory its workers leave
/// (`cf_resident_mb = auto`, #449). A fetch evicts down to the bound it
/// reads then.
#[derive(Clone, Debug)]
pub struct CacheBound(Arc<AtomicUsize>);

impl CacheBound {
    pub(super) fn new(bytes: usize) -> Self {
        Self(Arc::new(AtomicUsize::new(bytes)))
    }

    /// Bytes of loaded cells the cache keeps at most.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    /// Raise the bound to `bytes`; a smaller value leaves it.
    pub fn raise(&self, bytes: usize) {
        self.0.fetch_max(bytes, Ordering::Relaxed);
    }
}

/// What a catalog holds from its open on, as a function of its cache's
/// bound and the workers fetching its cells ([`Self::bytes`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheCharge {
    /// What it keeps of its index (each cell's two headers and paths, the
    /// groups, axes, routes, corrections and the cache's group slots, at
    /// their capacities), its Mueller routing and its image correction.
    pub(super) kept: u64,
    /// Opening's stage: it reads one header at a time and lists one axis's
    /// values before it is reduced.
    pub(super) opening: u64,
    /// Every group's cells.
    pub(super) every: u64,
    /// The largest group's cells.
    pub(super) largest: u64,
    pub(super) groups: u64,
    /// What one holder reads to build one group: the planes, each read as an
    /// image array and its copy, with that read's records.
    pub(super) reading: u64,
}

impl CacheCharge {
    /// Bytes the catalog holds at most at a cache bound of `bound` bytes
    /// with `holders` workers fetching cells: what it keeps and the larger
    /// of its two stages, opening and serving. Serving holds its cells and,
    /// for each holder, one group's read.
    ///
    /// When every group fits the bound nothing is evicted and each group is
    /// loaded once, so the cells are at most every group (#738). Otherwise
    /// they are the bound and one group past it, which a fetch inserts
    /// before the cache evicts, and for each holder the largest group twice.
    /// A CPU residual holds a gridding cell and a prediction cell (#449),
    /// and either keeps both alive after eviction, or one beside the group
    /// it builds outside the cache's lock for the other (#744): a
    /// [`CellHold`] releases its own cell before it fetches (#773 review).
    /// Every loaded cell is also its [`Arc`] (#745 review).
    #[must_use]
    pub const fn bytes(&self, holders: usize, bound: usize) -> u64 {
        let holders = holders as u64;
        let (taps, pairs) = if self.every <= bound as u64 {
            (self.every, self.groups)
        } else {
            (
                bound as u64 + (2 * holders + 1) * self.largest,
                self.groups + 2 * holders + 1,
            )
        };
        let serving = taps + pairs * 2 * ARC_CELL_BYTES as u64 + holders * self.reading;
        self.kept
            + if self.opening > serving {
                self.opening
            } else {
                serving
            }
    }

    /// Bytes of every group's cells: a bound this large keeps them all.
    #[must_use]
    pub const fn every_bytes(&self) -> u64 {
        self.every
    }

    /// The largest bound, from `bound` up, whose charge on `holders`
    /// exceeds the charge at `bound` by at most `spare` bytes: every group,
    /// when that fits, which needs no group past the bound nor the holders'
    /// evicted groups; otherwise `bound + spare`, since below every group
    /// the charge grows with the bound byte for byte.
    #[must_use]
    pub fn bound_within(&self, holders: usize, bound: usize, spare: u64) -> usize {
        let every = usize::try_from(self.every).unwrap_or(usize::MAX);
        if bound >= every
            || self.bytes(holders, every) <= self.bytes(holders, bound).saturating_add(spare)
        {
            return bound.max(every);
        }
        bound.saturating_add(usize::try_from(spare).unwrap_or(usize::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog of a hundred 10 MB groups whose serving stage outweighs
    /// opening.
    const CHARGE: CacheCharge = CacheCharge {
        kept: 1_000,
        opening: 0,
        every: 1_000_000_000,
        largest: 10_000_000,
        groups: 100,
        reading: 5_000,
    };

    /// The bound grows into the spare byte for byte below every group, and
    /// takes every group, with no group past it nor any holder's evicted
    /// one, as soon as their charge fits; it never costs more than the
    /// spare, and never falls.
    #[test]
    fn a_bound_grows_into_its_spare_and_never_past_it() {
        let holders = 4;
        let floor = 100_000_000;
        let at_floor = CHARGE.bytes(holders, floor);
        // Within the spare below every group: the bound rises by the spare.
        let spare = 300_000_000;
        let raised = CHARGE.bound_within(holders, floor, spare);
        assert_eq!(raised, floor + 300_000_000);
        assert_eq!(CHARGE.bytes(holders, raised), at_floor + spare);
        // Every group's charge drops the group past the bound and the
        // holders' groups, so it fits a spare smaller than the gap to every.
        let all = CHARGE.bytes(holders, CHARGE.every as usize);
        assert!(all < at_floor + (CHARGE.every - floor as u64));
        let spare = all - at_floor;
        assert_eq!(
            CHARGE.bound_within(holders, floor, spare),
            CHARGE.every as usize
        );
        // A byte short of it, the bound grows linearly instead.
        let short = CHARGE.bound_within(holders, floor, spare - 1);
        assert_eq!(short, floor + (spare - 1) as usize);
        assert!(CHARGE.bytes(holders, short) < at_floor + spare);
        // No spare, no growth; a bound past every group stays.
        assert_eq!(CHARGE.bound_within(holders, floor, 0), floor);
        assert_eq!(CHARGE.bound_within(holders, usize::MAX, 0), usize::MAX);
    }

    /// A raised bound is the one every clone of it reads; a lower value
    /// leaves it.
    #[test]
    fn a_raised_bound_is_shared_and_never_lowered() {
        let bound = CacheBound::new(10);
        let shared = bound.clone();
        bound.raise(30);
        assert_eq!(shared.bytes(), 30);
        shared.raise(20);
        assert_eq!(bound.bytes(), 30);
    }
}
