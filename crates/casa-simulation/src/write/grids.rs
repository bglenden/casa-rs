// SPDX-License-Identifier: LGPL-3.0-or-later
//! The per-field model grids `close` prepares and keeps.
//!
//! A step that predicts several fields under the primary beam holds its
//! model's planes, and `close` prepares a field's grids, the beam centred
//! on it, as its write reaches the field
//! ([`crate::session::prediction::Prediction::prepare_field`]). The cache
//! keeps as many sets as its admitted bytes hold, at least those of the
//! segment being predicted. The write visits the scans in order and knows
//! them all, so when a set must go the cache evicts the one used again
//! latest, or never (Belady's rule). A field evicted and visited again is
//! prepared again, as CASA's `GridFT` transforms the model again whenever
//! the field changes. Its grids are the same however often they are
//! prepared, so the MeasurementSet does not depend on the budget.

use std::sync::Arc;
use std::time::{Duration, Instant};

use casa_imaging_operator::PreparedModelGrids;

use crate::session::error::{SimulatorError, SimulatorResult};

/// A set of per-field grids: the step's index among the plan's predict
/// steps, and the field.
pub(super) type GridKey = (usize, usize);

/// The per-field grids `close` holds, within its admitted bytes.
pub(super) struct FieldGridCache<G = PreparedModelGrids> {
    /// Bytes one set of each step costs, by step: its grids, and on Metal
    /// their device copy as much again; zero for a step whose grids
    /// `predict` prepared.
    costs: Vec<u64>,
    /// Each key's visits, by the first scan of each, ascending; sorted by
    /// key.
    visits: Vec<(GridKey, Vec<usize>)>,
    /// The sets held.
    resident: Vec<(GridKey, Arc<G>)>,
    /// Bytes the sets may hold, as admitted.
    capacity: u64,
    /// Bytes the sets hold.
    held: u64,
    /// Sets prepared, and the time spent preparing them.
    preparations: usize,
    preparing: Duration,
}

impl<G> FieldGridCache<G> {
    /// A cache of sets costing `costs` bytes by step, whose keys are used
    /// in the visits `visits` (each key's visits by their first scans, in
    /// any order). It holds nothing until it is given its bytes
    /// ([`Self::set_capacity`]).
    pub(super) fn new(costs: Vec<u64>, mut visits: Vec<(GridKey, Vec<usize>)>) -> Self {
        visits.sort_unstable_by_key(|(key, _)| *key);
        for (_, firsts) in &mut visits {
            firsts.sort_unstable();
        }
        Self {
            costs,
            visits,
            resident: Vec::new(),
            capacity: 0,
            held: 0,
            preparations: 0,
            preparing: Duration::ZERO,
        }
    }

    /// Bytes one set of `key`'s step costs.
    pub(super) fn cost(&self, (step, _): GridKey) -> u64 {
        self.costs[step]
    }

    /// Bytes every set the visits use costs together: the most the cache
    /// holds, preparing each set once.
    pub(super) fn all_bytes(&self) -> u64 {
        self.visits.iter().map(|(key, _)| self.cost(*key)).sum()
    }

    /// Bytes the cache's bookkeeping holds: its visits, and room to list
    /// every set it may hold.
    pub(super) fn state_bytes(&self) -> u64 {
        let visits: usize = self
            .visits
            .iter()
            .map(|(_, firsts)| {
                size_of::<(GridKey, Vec<usize>)>() + firsts.len() * size_of::<usize>()
            })
            .sum();
        (visits
            + self.costs.len() * size_of::<u64>()
            + self.visits.len() * size_of::<(GridKey, Arc<G>)>()) as u64
    }

    /// Give the cache `capacity` bytes, as admitted, and room to list every
    /// set it may hold.
    pub(super) fn set_capacity(&mut self, capacity: u64) {
        self.capacity = capacity;
        self.resident = Vec::with_capacity(self.visits.len());
    }

    /// Sets prepared so far, and the time spent preparing them.
    pub(super) fn preparations(&self) -> (usize, Duration) {
        (self.preparations, self.preparing)
    }

    /// The grids of `key` for the visit starting at scan `scan`: held, or
    /// prepared by `prepare` once the sets used again latest, other than
    /// the visit's own `keep`, are evicted to make room.
    ///
    /// # Errors
    ///
    /// `prepare`'s error, or a raised one when the sets of `keep` and this
    /// one do not fit the cache, which is admitted to hold them.
    pub(super) fn grids(
        &mut self,
        key: GridKey,
        (scan, keep): (usize, &[GridKey]),
        prepare: impl FnOnce() -> SimulatorResult<G>,
    ) -> SimulatorResult<Arc<G>> {
        if let Some((_, grids)) = self.resident.iter().find(|(held, _)| *held == key) {
            return Ok(Arc::clone(grids));
        }
        let cost = self.cost(key);
        while self.held + cost > self.capacity {
            let victim = self
                .resident
                .iter()
                .enumerate()
                .filter(|(_, (held, _))| !keep.contains(held))
                .max_by_key(|(_, (held, _))| self.next_visit(*held, scan).unwrap_or(usize::MAX))
                .map(|(index, _)| index)
                .ok_or_else(|| {
                    SimulatorError::raised(format!(
                        "the per-field model grids' {} bytes cannot hold a set of {cost} bytes \
                         beside the {} the visit holds",
                        self.capacity, self.held
                    ))
                })?;
            let (evicted, _) = self.resident.swap_remove(victim);
            self.held -= self.cost(evicted);
        }
        let started = Instant::now();
        let grids = Arc::new(prepare()?);
        self.preparing += started.elapsed();
        self.preparations += 1;
        self.held += cost;
        self.resident.push((key, Arc::clone(&grids)));
        Ok(grids)
    }

    /// The first scan of `key`'s next visit after the visit at scan
    /// `scan`; `None` when it is not visited again.
    fn next_visit(&self, key: GridKey, scan: usize) -> Option<usize> {
        let index = self
            .visits
            .binary_search_by_key(&key, |(known, _)| *known)
            .ok()?;
        let firsts = &self.visits[index].1;
        firsts
            .get(firsts.partition_point(|&first| first <= scan))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fields a cache of `slots` one-byte sets of one step prepares, in
    /// order, for fields visited in `order`, one scan a visit.
    fn prepared(order: &[usize], slots: u64) -> Vec<usize> {
        let mut visits: Vec<(GridKey, Vec<usize>)> = Vec::new();
        for (scan, &field) in order.iter().enumerate() {
            match visits.iter_mut().find(|(key, _)| *key == (0, field)) {
                Some((_, firsts)) => firsts.push(scan),
                None => visits.push(((0, field), vec![scan])),
            }
        }
        let mut cache = FieldGridCache::<usize>::new(vec![1], visits);
        assert_eq!(cache.all_bytes(), order.iter().max().map_or(0, |n| *n as u64 + 1));
        cache.set_capacity(slots);
        let mut prepared = Vec::new();
        for (scan, &field) in order.iter().enumerate() {
            let key = (0, field);
            let grids = cache
                .grids(key, (scan, &[key]), || {
                    prepared.push(field);
                    Ok(field)
                })
                .expect("one set fits");
            assert_eq!(*grids, field);
            assert!(cache.held <= slots);
        }
        assert_eq!(cache.preparations().0, prepared.len());
        prepared
    }

    #[test]
    fn every_field_is_prepared_once_when_all_fit() {
        assert_eq!(prepared(&[0, 1, 2, 0, 1, 2], 3), vec![0, 1, 2]);
    }

    #[test]
    fn a_second_pass_evicts_the_sets_used_again_latest() {
        // Two passes over four fields with three slots: the first pass
        // evicts 2, used again last, for 3. The second finds 0 and 1,
        // prepares 2 again in place of a set not used again, and finds 3:
        // five preparations, where evicting the least recently used set
        // would prepare all eight visits.
        let order = [0, 1, 2, 3, 0, 1, 2, 3];
        assert_eq!(prepared(&order, 3), vec![0, 1, 2, 3, 2]);
        // One slot prepares every visit.
        assert_eq!(prepared(&order, 1), order.to_vec());
    }

    #[test]
    fn a_visits_own_sets_are_never_evicted() {
        let mut cache =
            FieldGridCache::<()>::new(vec![1, 1], vec![((0, 0), vec![0]), ((1, 0), vec![0])]);
        cache.set_capacity(1);
        let keep = [(0, 0), (1, 0)];
        cache.grids((0, 0), (0, &keep), || Ok(())).expect("one set fits");
        let error = cache
            .grids((1, 0), (0, &keep), || Ok(()))
            .expect_err("two sets do not fit one byte");
        assert!(error.to_string().contains("cannot hold"), "{error}");
    }
}
