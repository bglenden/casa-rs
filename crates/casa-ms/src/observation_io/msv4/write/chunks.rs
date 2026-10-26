// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-chunk writes. zarrs stores a chunk directly only when one write
//! covers all of it; any other write decodes the stored chunk, merges and
//! re-encodes it, once per batch that touches it. So the times of a batch
//! that cover a chunk only partly are held here until the chunk's other
//! times arrive, and every chunk of a dataset written in time order is
//! encoded exactly once, whatever the batch size. Whole chunks of a batch
//! are stored straight from its views.

use std::ops::Range;

use num_complex::Complex32;

/// One write's elements of every payload variable, times outermost: a
/// batch's, from its views, or the held times'.
#[derive(Clone, Copy)]
pub(super) struct Columns<'a> {
    pub(super) values: &'a [Complex32],
    pub(super) flags: &'a [bool],
    pub(super) weights: &'a [f32],
    pub(super) uvw: &'a [f64],
    pub(super) centroid: &'a [f64],
    pub(super) exposure: &'a [f64],
}

/// The elements one time holds in each variable: `cube` in the
/// visibilities, flags and weights, three per baseline in the UVW, one per
/// baseline in the centroids and exposures.
#[derive(Clone, Copy, Debug)]
pub(super) struct PerTime {
    pub(super) baselines: usize,
    pub(super) cube: usize,
}

impl PerTime {
    /// Elements of every variable at one time.
    pub(super) fn elements(self) -> usize {
        3 * self.cube + 5 * self.baselines
    }

    /// Bytes of every variable at one time.
    pub(super) fn bytes(self) -> usize {
        let cube = size_of::<Complex32>() + size_of::<bool>() + size_of::<f32>();
        self.cube * cube + self.baselines * 5 * size_of::<f64>()
    }
}

impl<'a> Columns<'a> {
    /// The elements of `times`, counted from the first time these hold.
    pub(super) fn times(self, times: &Range<usize>, per: PerTime) -> Self {
        let span = |n: usize| times.start * n..times.end * n;
        Self {
            values: &self.values[span(per.cube)],
            flags: &self.flags[span(per.cube)],
            weights: &self.weights[span(per.cube)],
            uvw: &self.uvw[span(3 * per.baselines)],
            centroid: &self.centroid[span(per.baselines)],
            exposure: &self.exposure[span(per.baselines)],
        }
    }
}

/// Times held toward one whole chunk: contiguous dataset times inside one
/// chunk, and their elements of every variable. It holds at most one
/// chunk of times, reserved when it first holds any.
#[derive(Default)]
pub(super) struct Held {
    times: Range<usize>,
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    uvw: Vec<f64>,
    centroid: Vec<f64>,
    exposure: Vec<f64>,
}

impl Held {
    /// The dataset times held; empty when none are.
    pub(super) fn times(&self) -> Range<usize> {
        self.times.clone()
    }

    /// Hold `columns`, the elements of dataset times `times`, after those
    /// already held, in a holding of at most `chunk` times.
    pub(super) fn hold(
        &mut self,
        times: Range<usize>,
        columns: Columns<'_>,
        per: PerTime,
        chunk: usize,
    ) {
        if self.times.is_empty() {
            if self.values.capacity() == 0 {
                self.values.reserve_exact(chunk * per.cube);
                self.flags.reserve_exact(chunk * per.cube);
                self.weights.reserve_exact(chunk * per.cube);
                self.uvw.reserve_exact(chunk * 3 * per.baselines);
                self.centroid.reserve_exact(chunk * per.baselines);
                self.exposure.reserve_exact(chunk * per.baselines);
            }
            self.times = times;
        } else {
            debug_assert_eq!(self.times.end, times.start, "held times are contiguous");
            self.times.end = times.end;
        }
        self.values.extend_from_slice(columns.values);
        self.flags.extend_from_slice(columns.flags);
        self.weights.extend_from_slice(columns.weights);
        self.uvw.extend_from_slice(columns.uvw);
        self.centroid.extend_from_slice(columns.centroid);
        self.exposure.extend_from_slice(columns.exposure);
    }

    /// The held elements.
    pub(super) fn columns(&self) -> Columns<'_> {
        Columns {
            values: &self.values,
            flags: &self.flags,
            weights: &self.weights,
            uvw: &self.uvw,
            centroid: &self.centroid,
            exposure: &self.exposure,
        }
    }

    /// Hold nothing, keeping the storage for the next chunk.
    pub(super) fn clear(&mut self) {
        self.times = 0..0;
        self.values.clear();
        self.flags.clear();
        self.weights.clear();
        self.uvw.clear();
        self.centroid.clear();
        self.exposure.clear();
    }
}

/// One write of a batch's times, or of the held ones.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// Copy these dataset times of the batch after the held ones.
    Hold(Range<usize>),
    /// Store these dataset times of the batch straight from its views:
    /// whole chunks only.
    Store(Range<usize>),
    /// Store the held times and hold none.
    Release,
}

/// The chunk of `chunk` times holding dataset time `time`, in a dataset of
/// `total` times: the last chunk ends at `total`.
pub(super) fn chunk_of(time: usize, chunk: usize, total: usize) -> Range<usize> {
    let start = time / chunk * chunk;
    start..(start + chunk).min(total)
}

/// How to write the dataset times `batch`, with `held` already held, in
/// chunks of `chunk` times of a dataset of `total`: whole chunks straight
/// from the batch, the rest held until its chunk is complete and then
/// released whole. Held times the batch does not continue are released
/// first, covering their chunk only partly.
pub(super) fn plan(
    held: Range<usize>,
    batch: Range<usize>,
    chunk: usize,
    total: usize,
) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut held = held;
    let mut stored: Option<Range<usize>> = None;
    let mut start = batch.start;
    while start < batch.end {
        let whole = chunk_of(start, chunk, total);
        let end = whole.end.min(batch.end);
        if !held.is_empty() && (held.end != start || chunk_of(held.start, chunk, total) != whole) {
            steps.push(Step::Release);
            held = 0..0;
        }
        if held.is_empty() && start == whole.start && end == whole.end {
            stored = Some(stored.map_or(start..end, |run| run.start..end));
        } else {
            if let Some(run) = stored.take() {
                steps.push(Step::Store(run));
            }
            steps.push(Step::Hold(start..end));
            held = if held.is_empty() {
                start..end
            } else {
                held.start..end
            };
            if held == whole {
                steps.push(Step::Release);
                held = 0..0;
            }
        }
        start = end;
    }
    if let Some(run) = stored {
        steps.push(Step::Store(run));
    }
    steps
}

#[cfg(test)]
mod tests {
    use super::Step::{Hold, Release, Store};
    use super::plan;

    #[test]
    fn whole_chunks_are_stored_and_the_rest_held_until_its_chunk_completes() {
        // Five-time chunks of a twelve-time dataset: 0..5, 5..10, 10..12.
        assert_eq!(plan(0..0, 0..12, 5, 12), [Store(0..12)]);
        assert_eq!(plan(0..0, 0..4, 5, 12), [Hold(0..4)]);
        assert_eq!(plan(0..4, 4..8, 5, 12), [Hold(4..5), Release, Hold(5..8)]);
        assert_eq!(
            plan(5..8, 8..12, 5, 12),
            [Hold(8..10), Release, Store(10..12)],
            "the short last chunk is whole"
        );
    }

    #[test]
    fn held_times_a_batch_does_not_continue_are_released_partly() {
        assert_eq!(plan(5..8, 0..5, 5, 12), [Release, Store(0..5)]);
        assert_eq!(plan(0..2, 3..4, 5, 12), [Release, Hold(3..4)]);
        assert_eq!(plan(3..5, 5..7, 5, 12), [Release, Hold(5..7)]);
        assert_eq!(
            plan(1..3, 3..12, 5, 12),
            [Hold(3..5), Release, Store(5..12)],
            "1..5 never completes its chunk, so 5..10 is not held after it"
        );
    }
}
