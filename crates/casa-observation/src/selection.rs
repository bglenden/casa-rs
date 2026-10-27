// SPDX-License-Identifier: LGPL-3.0-or-later
//! Which of a partition's grid times a traversal delivers: the request's
//! time range and field selection resolved against the partition's grid
//! (`docs/measurement-set/contracts.md`, section 4).
//!
//! A selection takes whole times: a field is per time, so selecting fields
//! leaves runs of consecutive grid times with gaps between them, and the
//! adapters decode none of the gaps. A batch holds whole times of one run.
//!
//! Intents are not selected here. A partition's samples share their intents
//! (the catalogue partitions by them, as XRADIO's converter does), so a
//! consumer selects intents by the partitions it requests.

use std::ops::Range;

use crate::FieldId;

/// The fields a traversal reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum FieldSelection {
    /// Every field.
    #[default]
    All,
    /// The times phased to one of these fields, in the requested data
    /// group: a list naming no field of the partition selects nothing.
    Only(Vec<FieldId>),
}

impl FieldSelection {
    /// Whether the selection holds `field`.
    #[must_use]
    pub fn holds(&self, field: FieldId) -> bool {
        match self {
            Self::All => true,
            Self::Only(fields) => fields.contains(&field),
        }
    }
}

/// The grid times a traversal delivers in one partition, as indices into
/// the partition's grid times: ascending, disjoint runs of consecutive
/// times, none empty and no two adjacent. A traversal delivers each run in
/// blocks of whole times ([`Self::block_at`]), so a batch's times are
/// consecutive on the grid.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimeRuns {
    runs: Vec<Range<usize>>,
    count: usize,
}

impl TimeRuns {
    /// Every time of `range`: one run, or none when it is empty.
    #[must_use]
    pub fn whole(range: Range<usize>) -> Self {
        let count = range.len();
        Self {
            runs: if range.is_empty() {
                vec![]
            } else {
                vec![range]
            },
            count,
        }
    }

    /// The times of `range` whose field, `fields[t]` for time `t`,
    /// `selection` holds.
    ///
    /// # Panics
    ///
    /// If `range` reaches past `fields`.
    #[must_use]
    pub fn selected(range: Range<usize>, fields: &[FieldId], selection: &FieldSelection) -> Self {
        if *selection == FieldSelection::All {
            return Self::whole(range);
        }
        let mut runs = Self::default();
        for (time, field) in fields[range.clone()].iter().enumerate() {
            if selection.holds(*field) {
                runs.push(range.start + time);
            }
        }
        runs
    }

    /// Append `time`, after every time held.
    fn push(&mut self, time: usize) {
        match self.runs.last_mut() {
            Some(run) if run.end == time => run.end += 1,
            _ => self.runs.push(time..time + 1),
        }
        self.count += 1;
    }

    /// The runs, ascending.
    #[must_use]
    pub fn runs(&self) -> &[Range<usize>] {
        &self.runs
    }

    /// The times held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether no time is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The first time held.
    #[must_use]
    pub fn first(&self) -> Option<usize> {
        self.runs.first().map(|run| run.start)
    }

    /// From the first time held to past the last: empty when none is.
    #[must_use]
    pub fn span(&self) -> Range<usize> {
        match (self.runs.first(), self.runs.last()) {
            (Some(first), Some(last)) => first.start..last.end,
            _ => 0..0,
        }
    }

    /// The run holding or following `time`, by index into [`Self::runs`].
    fn run_from(&self, time: usize) -> usize {
        self.runs.partition_point(|run| run.end <= time)
    }

    /// Whether `time` is held.
    #[must_use]
    pub fn contains(&self, time: usize) -> bool {
        self.runs
            .get(self.run_from(time))
            .is_some_and(|run| run.start <= time)
    }

    /// Whether every time of the non-empty `times` is held: they lie in one
    /// run.
    #[must_use]
    pub fn holds(&self, times: Range<usize>) -> bool {
        !times.is_empty()
            && self
                .runs
                .get(self.run_from(times.start))
                .is_some_and(|run| run.start <= times.start && times.end <= run.end)
    }

    /// The first time held at or after `time`.
    #[must_use]
    pub fn next_from(&self, time: usize) -> Option<usize> {
        self.runs
            .get(self.run_from(time))
            .map(|run| run.start.max(time))
    }

    /// The times held within `range`.
    #[must_use]
    pub fn within(&self, range: Range<usize>) -> Self {
        let mut within = Self::default();
        for run in &self.runs[self.run_from(range.start)..] {
            if run.start >= range.end {
                break;
            }
            let part = run.start.max(range.start)..run.end.min(range.end);
            within.count += part.len();
            within.runs.push(part);
        }
        within
    }

    /// The block a traversal delivers next from `time`: at most
    /// `per_block` times of one run, from the first time held at or after
    /// `time`; `None` past the last.
    #[must_use]
    pub fn block_at(&self, time: usize, per_block: usize) -> Option<Range<usize>> {
        let run = self.runs.get(self.run_from(time))?;
        let start = run.start.max(time);
        Some(start..(start + per_block.max(1)).min(run.end))
    }

    /// The blocks of a traversal of every time held, `per_block` times or
    /// fewer each.
    pub fn blocks(&self, per_block: usize) -> impl Iterator<Item = Range<usize>> + '_ {
        let per_block = per_block.max(1);
        self.runs.iter().flat_map(move |run| {
            run.clone()
                .step_by(per_block)
                .map(move |start| start..(start + per_block).min(run.end))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(ids: &[u32]) -> Vec<FieldId> {
        ids.iter().copied().map(FieldId).collect()
    }

    /// A field selection keeps whole times, in runs of consecutive ones;
    /// every field, or a range alone, is one run.
    #[test]
    fn a_field_selection_leaves_runs_of_whole_times() {
        let grid = fields(&[0, 0, 1, 2, 0, 1, 1, 0]);
        let only = FieldSelection::Only(fields(&[0, 2]));
        let runs = TimeRuns::selected(0..8, &grid, &only);
        assert_eq!(runs.runs(), [0..2, 3..5, 7..8]);
        assert_eq!((runs.len(), runs.first(), runs.span()), (5, Some(0), 0..8));
        let cut = TimeRuns::selected(1..6, &grid, &only);
        assert_eq!(cut.runs(), [1..2, 3..5]);
        assert_eq!(
            TimeRuns::selected(2..6, &grid, &FieldSelection::All),
            TimeRuns::whole(2..6)
        );
        let none = TimeRuns::selected(0..8, &grid, &FieldSelection::Only(fields(&[9])));
        assert!(none.is_empty() && none.runs().is_empty() && none.span().is_empty());
        assert!(TimeRuns::whole(3..3).is_empty());
    }

    /// Membership, the next time held, intersections and blocks of whole
    /// times that never cross a gap.
    #[test]
    fn blocks_stay_within_runs() {
        let runs = TimeRuns::selected(
            0..10,
            &fields(&[0, 0, 0, 1, 0, 0, 1, 1, 0, 0]),
            &FieldSelection::Only(fields(&[0])),
        );
        assert_eq!(runs.runs(), [0..3, 4..6, 8..10]);
        assert!(runs.contains(4) && !runs.contains(3) && !runs.contains(10));
        assert!(runs.holds(4..6) && !runs.holds(2..5) && !runs.holds(5..5));
        assert_eq!(
            (runs.next_from(3), runs.next_from(6), runs.next_from(10)),
            (Some(4), Some(8), None)
        );
        assert_eq!(runs.within(1..9).runs(), [1..3, 4..6, 8..9]);
        assert_eq!(runs.within(1..9).len(), 5);
        assert_eq!(
            runs.blocks(2).collect::<Vec<_>>(),
            [0..2, 2..3, 4..6, 8..10]
        );
        let mut walked = Vec::new();
        let mut time = 0;
        while let Some(block) = runs.block_at(time, 2) {
            time = block.end;
            walked.push(block);
        }
        assert_eq!(walked, runs.blocks(2).collect::<Vec<_>>());
    }
}
