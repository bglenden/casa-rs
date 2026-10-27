// SPDX-License-Identifier: LGPL-3.0-or-later
//! A traversal's times split into slices planned together, so that several
//! workers can each take a slice of a partition and still finalize every
//! scientific group once: a group the cuts divide is joined across the
//! slices by one shared completion.
//!
//! A consumer plans a [`TimeSplit`] of its request, binds each slice's
//! request on its own worker, and gives each worker its slice's view of the
//! split's [`SplitCompletion`]. It writes the same code as for one
//! traversal: whatever the cuts, every group becomes ready once, after the
//! input and the work of each of its parts. A slice that ends without
//! completing its parts abandons their groups, which the others then
//! report instead of waiting.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use crate::ObservationError;
use crate::cursor::Observation;
use crate::grouping::{GroupKey, Joins, WorkToken};
use crate::request::{AccessRequest, Estimate, Grouping, TimeRange, TimeSelection};

/// A request's times cut into consecutive slices that tile them, planned
/// together: each slice is the request with its own times, and the split
/// knows which scientific groups the cuts divide and which slices hold
/// their parts.
///
/// Group keys are those of the whole grid, so a divided group arrives in
/// each slice holding part of it under the same key, marked
/// [`clipped`](crate::GroupFragment::clipped) there; the split's
/// [`SplitCompletion`] joins the parts. A slice may hold no time of a
/// partition: its traversal then delivers nothing of it.
#[derive(Clone, Debug)]
pub struct TimeSplit {
    slices: Vec<AccessRequest>,
    divided: BTreeMap<GroupKey, BTreeSet<usize>>,
}

impl TimeSplit {
    /// Split `request` at the consecutive ranges `cuts`: slice `i` holds the
    /// request's times within `cuts[i]`. Each range ends where the next
    /// starts, so a planner cuts at the epochs it chose (aligned to the
    /// store's natural time blocks through [`TimeRange::of_grid`], say),
    /// and the first must start no later and the last end no earlier than
    /// the request's times in every partition, so the slices tile them.
    /// The store's group index ([`Observation::group_keys`]) gives the
    /// groups the cuts divide; nothing is read.
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidRequest`] when there are no cuts, they are
    /// not consecutive ranges of one origin and scale (the request's own, if
    /// it selects a range), or they leave a time of the request outside
    /// every slice; and as [`Observation::estimate`] for the request and
    /// each slice.
    pub fn plan(
        observation: &(impl Observation + ?Sized),
        request: &AccessRequest,
        cuts: &[TimeRange],
    ) -> Result<Self, ObservationError> {
        let invalid = |reason: String| ObservationError::InvalidRequest { reason };
        let (first, rest) = cuts
            .split_first()
            .ok_or_else(|| invalid("a time split needs at least one slice".to_string()))?;
        for (earlier, later) in cuts.iter().zip(rest) {
            if !later.same_frame(first) || earlier.end_s != later.start_s {
                return Err(invalid(format!(
                    "time split slices [{}, {}) s and [{}, {}) s are not consecutive ranges of \
                     one origin and scale",
                    earlier.start_s, earlier.end_s, later.start_s, later.end_s
                )));
            }
        }
        let slices = cuts
            .iter()
            .map(|cut| {
                Ok(AccessRequest {
                    times: TimeSelection::Within(within(request.times, *cut)?),
                    ..request.clone()
                })
            })
            .collect::<Result<Vec<_>, ObservationError>>()?;
        let whole = observation.estimate(request)?;
        let parts = slices
            .iter()
            .map(|slice| observation.estimate(slice))
            .collect::<Result<Vec<_>, _>>()?;
        check_tiling(&whole, &parts)?;
        let divided = if request.grouping == Grouping::None {
            BTreeMap::new()
        } else {
            divided(observation, request, &parts)?
        };
        Ok(Self { slices, divided })
    }

    /// The slices' requests, in time order.
    #[must_use]
    pub fn slices(&self) -> &[AccessRequest] {
        &self.slices
    }

    /// The groups the cuts divide, each with the slices holding part of
    /// it: two or more.
    #[must_use]
    pub fn divided(&self) -> &BTreeMap<GroupKey, BTreeSet<usize>> {
        &self.divided
    }

    /// A completion the slices' traversals share, joining the parts of
    /// every divided group.
    #[must_use]
    pub fn completion(&self) -> SplitCompletion {
        SplitCompletion {
            joins: Arc::new(Mutex::new(Joins::divided(self.divided.clone()))),
            taken: Arc::new(Mutex::new(vec![false; self.slices.len()])),
        }
    }
}

/// `cut` within the request's times: the cut itself when the request reads
/// every time, else its intersection with the request's range.
fn within(times: TimeSelection, cut: TimeRange) -> Result<TimeRange, ObservationError> {
    let TimeSelection::Within(range) = times else {
        return Ok(cut);
    };
    if !range.same_frame(&cut) {
        return Err(ObservationError::InvalidRequest {
            reason: "a time split's slices are stated in another origin or scale than the \
                     request's time range"
                .to_string(),
        });
    }
    let start_s = range.start_s.max(cut.start_s);
    Ok(TimeRange {
        start_s,
        end_s: range.end_s.min(cut.end_s).max(start_s),
        ..cut
    })
}

/// Refuse slices whose selected times, taken in order, are not the
/// request's in every partition: each held once, none missing.
fn check_tiling(whole: &Estimate, parts: &[Estimate]) -> Result<(), ObservationError> {
    for (index, partition) in whole.partitions.iter().enumerate() {
        // The slices' runs in order, a run a cut divides joined again.
        let mut joined: Vec<Range<usize>> = Vec::new();
        let mut ordered = true;
        for run in parts
            .iter()
            .flat_map(|part| part.partitions[index].time_runs.runs())
        {
            match joined.last_mut() {
                Some(last) if last.end == run.start => last.end = run.end,
                Some(last) if last.end > run.start => ordered = false,
                _ => joined.push(run.clone()),
            }
        }
        if !ordered || joined != partition.time_runs.runs() {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "the time split's slices do not tile {:?}'s selected times {:?}",
                    partition.partition,
                    partition.time_runs.span()
                ),
            });
        }
    }
    Ok(())
}

/// The groups more than one slice holds part of, with the slices holding
/// part of each.
fn divided(
    observation: &(impl Observation + ?Sized),
    request: &AccessRequest,
    parts: &[Estimate],
) -> Result<BTreeMap<GroupKey, BTreeSet<usize>>, ObservationError> {
    let mut divided = BTreeMap::new();
    for (index, id) in request.partitions.iter().enumerate() {
        let Some(keys) =
            observation.group_keys(*id, &request.data_group, request.grouping, &request.fields)?
        else {
            continue;
        };
        let mut holders = BTreeMap::<GroupKey, BTreeSet<usize>>::new();
        for (slice, part) in parts.iter().enumerate() {
            for run in part.partitions[index].time_runs.runs() {
                let held = keys
                    .get(run.clone())
                    .ok_or_else(|| ObservationError::InvalidStore {
                        reason: format!("{id:?}'s group index does not cover its selected times"),
                    })?;
                for key in held.iter().flatten() {
                    holders.entry(*key).or_default().insert(slice);
                }
            }
        }
        divided.extend(holders.into_iter().filter(|(_, slices)| slices.len() > 1));
    }
    Ok(divided)
}

/// The completion a [`TimeSplit`]'s traversals share: the joins of
/// [`GroupCompletion`](crate::GroupCompletion) over every slice, so that a
/// group the cuts divide is ready once, after each part's input has
/// completed in its slice and all work begun on it has finished, however
/// the slices interleave.
///
/// Each slice's consumer works through its own [`SliceCompletion`]
/// ([`Self::slice`]), which is `Send`; any one of them may finish a token
/// another began. Whichever call makes a group ready returns `true`, once
/// across the slices. A token dropped unfinished abandons its group, as
/// with one traversal, and so does a slice's view that ends
/// ([`SliceCompletion::end`]) or is dropped while a part of the group it
/// holds is open: a slice whose traversal failed is reported, not waited
/// for. Such a slice is not run again within the split: its groups are
/// abandoned, so a retry plans a fresh completion.
#[derive(Clone, Debug)]
pub struct SplitCompletion {
    joins: Arc<Mutex<Joins>>,
    /// Which slices' views have been given.
    taken: Arc<Mutex<Vec<bool>>>,
}

impl SplitCompletion {
    /// Slice `index`'s view, in the split's slice order. Each slice has one
    /// view, given once.
    ///
    /// # Panics
    ///
    /// If the split has no slice `index`, or its view was given before.
    #[must_use]
    pub fn slice(&self, index: usize) -> SliceCompletion {
        let mut taken = self.taken.lock().unwrap_or_else(PoisonError::into_inner);
        let slices = taken.len();
        let given = taken
            .get_mut(index)
            .unwrap_or_else(|| panic!("slice {index} of a split of {slices}"));
        assert!(!*given, "slice {index}'s view was given before");
        *given = true;
        SliceCompletion {
            joins: Arc::clone(&self.joins),
            slice: index,
            ended: false,
        }
    }

    /// Whether no work is pending, no closed group awaits its work and every
    /// divided group's parts have all completed: true once every slice's
    /// traversal is consumed and its work finished.
    ///
    /// # Errors
    ///
    /// [`ObservationError::WorkAbandoned`] if a unit of work was abandoned,
    /// or a slice ended with a part open.
    pub fn is_idle(&self) -> Result<bool, ObservationError> {
        self.lock().is_idle()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Joins> {
        self.joins.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One slice's view of a [`SplitCompletion`]: the calls of
/// [`GroupCompletion`](crate::GroupCompletion), for the consumer of that
/// slice's traversal, and [`Self::end`] when it is over. Dropped before
/// its traversal has completed every part it holds (the traversal failed,
/// or its worker did), it abandons their groups.
#[derive(Debug)]
pub struct SliceCompletion {
    joins: Arc<Mutex<Joins>>,
    slice: usize,
    ended: bool,
}

impl SliceCompletion {
    /// A unit of work on `key` has started in this slice.
    ///
    /// # Errors
    ///
    /// If this slice has completed its part of the group's input, or work
    /// on the group was abandoned.
    pub fn begin(&self, key: GroupKey) -> Result<WorkToken, ObservationError> {
        self.lock().begin(self.slice, key)
    }

    /// The unit of work `token` names has finished; `true` if that makes
    /// its group ready.
    ///
    /// # Errors
    ///
    /// [`ObservationError::WorkAbandoned`] if a unit of work was abandoned.
    ///
    /// # Panics
    ///
    /// If `token` was issued by another completion.
    pub fn finish(&self, token: WorkToken) -> Result<bool, ObservationError> {
        self.lock().finish(token)
    }

    /// This slice's part of the group's input is complete; `true` if every
    /// part's is and no work on the group is pending, so it is ready now.
    ///
    /// # Errors
    ///
    /// If this slice closed the group, or a later group of its series,
    /// already; [`ObservationError::WorkAbandoned`] if a unit of work was
    /// abandoned.
    pub fn close(&self, key: GroupKey) -> Result<bool, ObservationError> {
        self.lock().close(self.slice, key)
    }

    /// Units of work pending on `key`, in every slice.
    #[must_use]
    pub fn pending(&self, key: GroupKey) -> usize {
        self.lock().pending(key)
    }

    /// This slice's traversal is over. A divided group whose part it has
    /// not completed is abandoned: its input will not complete, so the
    /// other slices' calls and [`SplitCompletion::is_idle`] report it.
    ///
    /// # Errors
    ///
    /// [`ObservationError::WorkAbandoned`] when the slice left a part open,
    /// or work was abandoned before.
    pub fn end(mut self) -> Result<(), ObservationError> {
        self.ended = true;
        self.lock().end(self.slice)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Joins> {
        self.joins.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for SliceCompletion {
    fn drop(&mut self) {
        if !self.ended {
            // Reported to the other slices and the split.
            let _ = self.lock().end(self.slice);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionId;

    fn key(bin: u64) -> GroupKey {
        GroupKey {
            partition: PartitionId(0),
            field: None,
            scan: None,
            bin,
        }
    }

    /// The completion of three slices dividing the groups of `parts`, each
    /// among the slices named.
    fn split(parts: &[(u64, &[usize])]) -> SplitCompletion {
        let divided = parts
            .iter()
            .map(|(bin, slices)| (key(*bin), slices.iter().copied().collect()))
            .collect();
        SplitCompletion {
            joins: Arc::new(Mutex::new(Joins::divided(divided))),
            taken: Arc::new(Mutex::new(vec![false; 3])),
        }
    }

    /// A divided group is ready once, after every part has closed and the
    /// work begun on any part has finished: a later slice closing its part
    /// first does not release it while an earlier slice has yet to begin.
    #[test]
    fn a_divided_group_waits_for_every_part() {
        let completion = split(&[(1, &[0, 1])]);
        let (early, late) = (completion.slice(0), completion.slice(1));
        assert!(!late.close(key(1)).expect("late part"), "one part of two");
        let work = early.begin(key(1)).expect("early work");
        assert!(!early.close(key(1)).expect("early part"), "work pending");
        assert!(late.finish(work).expect("finish"), "ready once");
        assert!(completion.is_idle().expect("idle"));
    }

    /// Each slice closes its own part and refuses work after it; another
    /// slice's part stays open, and a whole group is ready on its one close.
    #[test]
    fn each_slice_closes_its_own_part() {
        let completion = split(&[(1, &[0, 1, 2])]);
        let slices: Vec<_> = (0..3).map(|index| completion.slice(index)).collect();
        assert!(slices[0].close(key(0)).expect("whole group"), "ready now");
        assert!(!slices[0].close(key(1)).expect("first part"));
        assert!(slices[0].begin(key(1)).is_err(), "its part is closed");
        assert!(slices[0].close(key(1)).is_err(), "closed twice");
        let work = slices[2].begin(key(1)).expect("another part's work");
        assert!(!slices[2].close(key(1)).expect("third part"));
        assert!(!completion.is_idle().expect("not idle"), "a part to come");
        assert!(
            !slices[1].close(key(1)).expect("second part"),
            "work pending"
        );
        assert!(slices[2].finish(work).expect("finish"));
        assert!(completion.is_idle().expect("idle"));
    }

    /// Slices on threads: whatever the interleaving, each divided group is
    /// ready exactly once.
    #[test]
    fn concurrent_slices_make_each_group_ready_once() {
        let groups = 64;
        let completion = split(
            &(0..groups)
                .map(|bin| (bin, &[0, 1, 2][..]))
                .collect::<Vec<_>>(),
        );
        let ready = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for index in 0..3 {
                let slice = completion.slice(index);
                let ready = &ready;
                scope.spawn(move || {
                    for bin in 0..groups {
                        let work = slice.begin(key(bin)).expect("begin");
                        let mut done = slice.close(key(bin)).expect("close");
                        done |= slice.finish(work).expect("finish");
                        if done {
                            ready.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(ready.into_inner(), groups as usize);
        assert!(completion.is_idle().expect("idle"));
    }

    /// A slice whose traversal fails with a part open, or whose view is
    /// dropped, abandons the group: the other slices and the split report
    /// it rather than wait. A slice that completed its parts ends cleanly.
    #[test]
    fn a_slice_that_ends_with_a_part_open_abandons_its_group() {
        for dropped in [false, true] {
            let completion = split(&[(1, &[0, 1]), (2, &[1, 2])]);
            let (first, second, third) = (
                completion.slice(0),
                completion.slice(1),
                completion.slice(2),
            );
            assert!(!first.close(key(1)).expect("first part"));
            first.end().expect("its parts are complete");
            if dropped {
                drop(second);
            } else {
                assert!(
                    matches!(
                        second.end(),
                        Err(ObservationError::WorkAbandoned { group }) if group == key(1)
                    ),
                    "its parts of 1 and 2 are open"
                );
            }
            assert!(matches!(
                completion.is_idle(),
                Err(ObservationError::WorkAbandoned { .. })
            ));
            assert!(third.close(key(2)).is_err(), "reported to the others");
        }
    }

    /// Each slice's view is given once.
    #[test]
    #[should_panic(expected = "given before")]
    fn a_slice_has_one_view() {
        let completion = split(&[]);
        let _first = completion.slice(0);
        let _again = completion.slice(0);
    }
}
