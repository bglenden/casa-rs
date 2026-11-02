// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a write batch's corruptions take from each antenna, channel by
//! channel: the antenna scales of `tsys-atm` noise and the phasors of a
//! tropospheric (`settrop`) term.
//!
//! A `tsys-atm` row's σ in channel `c` is `s_i(c) · s_j(c)`, each antenna's
//! scale from its Tsys at its airmass ([`TsysAtm::antenna_scales`]), and a
//! tropospheric term multiplies a row by `g_i(c) · conj(g_j(c))`, each
//! antenna's phasor from the water vapour above it
//! ([`TroposphericPhase::phasors`]). Both belong to the antenna and the
//! integration, as a Jones term does, not to the row. A batch's are worked
//! out once, on the workers, before the jobs that read them: each costs a
//! line of sight through the atmosphere's layers, or a sine and cosine, per
//! channel, and a job of its own for every run of rows would repeat it as
//! many times as an integration's rows are split into jobs (for phasors,
//! once per baseline rather than per antenna).
//!
//! An entry is keyed by what its values depend on (the term, the window,
//! and the antenna's dish, airmass and the row's exposure, or the antenna
//! and the term's solution), never by where the batch starts, so an
//! integration that two batches share gets the same values in both.
//!
//! A table reserves room for the most entries its batch can need before any
//! is added ([`BatchScalesBuilder::with_capacity`]): a vector or index that
//! grew as entries came would hold its old allocation and its new one
//! together while it moved (#897 review).

use std::collections::HashMap;
use std::hash::Hash;

use num_complex::Complex32;

use super::jones::JonesTerm;
use super::troposphere::TroposphericPhase;
use super::tsys::RowNoise;
use super::tsys_atm::{TsysAtm, WindowAtmosphere};

/// Bytes of an index beside its buckets: its control group, and the
/// alignment of the control bytes after the buckets.
const INDEX_GROUP_BYTES: usize = 32;

/// An entry still to be worked out: where its values start in its table,
/// how many there are (one a channel), and how to work them out.
pub(crate) trait Entry: Sync {
    type Value: Copy + Default + Send;

    fn offset(&self) -> usize;

    fn width(&self) -> usize;

    fn work_out(&self, values: &mut [Self::Value]);
}

/// What an antenna's scales depend on. The term is its model's address: a
/// batch's terms are alive, and distinct, while it runs, and two
/// applications of one term share their model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ScaleKey {
    model: usize,
    window: usize,
    /// The dish diameter, airmass and exposure, as bits.
    antenna: [u64; 3],
}

impl ScaleKey {
    /// The key of antenna `end` (0 or 1) of `row` for `model` in spectral
    /// window `window`.
    fn new(model: &TsysAtm, window: usize, row: &RowNoise, end: usize) -> Self {
        Self {
            model: std::ptr::from_ref(model) as usize,
            window,
            antenna: [
                row.diameters_m[end].to_bits(),
                u64::from(row.airmass[end].to_bits()),
                row.exposure_s.to_bits(),
            ],
        }
    }
}

/// An antenna's scales still to be worked out.
#[derive(Debug)]
pub(crate) struct ScaleEntry<'a> {
    model: &'a TsysAtm,
    window: &'a WindowAtmosphere,
    /// The antenna's dish diameter and airmass.
    antenna: (f64, f32),
    /// The window's channel bandwidth and the row's exposure.
    integration: (f64, f64),
    offset: usize,
}

impl Entry for ScaleEntry<'_> {
    type Value = f32;

    fn offset(&self) -> usize {
        self.offset
    }

    fn width(&self) -> usize {
        self.window.channels()
    }

    fn work_out(&self, values: &mut [f32]) {
        self.model
            .antenna_scales(self.window, self.antenna, self.integration, values);
    }
}

/// What an antenna's phasors depend on: the term (its address, as for
/// [`ScaleKey`]), the window, the term's solution (its block) and the
/// antenna.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PhaseKey {
    term: usize,
    window: usize,
    block: u32,
    antenna: u32,
}

impl PhaseKey {
    fn new(term: &JonesTerm, (window, block): (usize, usize), antenna: usize) -> Self {
        Self {
            term: std::ptr::from_ref(term) as usize,
            window,
            block: block as u32,
            antenna: antenna as u32,
        }
    }
}

/// An antenna's phasors still to be worked out.
#[derive(Debug)]
pub(crate) struct PhaseEntry<'a> {
    phase: &'a TroposphericPhase,
    /// The window and the solution's block.
    solution: (usize, usize),
    /// The antenna, and the term's antennas.
    antenna: (usize, usize),
    width: usize,
    offset: usize,
}

impl Entry for PhaseEntry<'_> {
    type Value = Complex32;

    fn offset(&self) -> usize {
        self.offset
    }

    fn width(&self) -> usize {
        self.width
    }

    fn work_out(&self, values: &mut [Complex32]) {
        self.phase.phasors(self.solution, self.antenna, values);
    }
}

/// One kind's table: each entry's values, found by its key.
#[derive(Debug)]
struct Table<K, V> {
    /// Each entry's first value in `values`.
    index: HashMap<K, usize>,
    values: Vec<V>,
}

impl<K, V> Default for Table<K, V> {
    fn default() -> Self {
        Self {
            index: HashMap::new(),
            values: Vec::new(),
        }
    }
}

impl<K: Eq + Hash, V> Table<K, V> {
    /// The `width` values of the entry of `key`.
    fn get(&self, key: &K, width: usize) -> &[V] {
        let offset = self.index[key];
        &self.values[offset..offset + width]
    }
}

/// The scales and phasors a batch's corruptions read, by antenna and
/// integration.
#[derive(Debug, Default)]
pub(crate) struct BatchScales {
    scales: Table<ScaleKey, f32>,
    phasors: Table<PhaseKey, Complex32>,
}

impl BatchScales {
    /// The most a table with room for `entries` entries, (scales,
    /// phasors), of at most `channels` channels holds while it is built and
    /// read: their values, and the room reserved for them.
    pub(crate) fn bytes((scales, phasors): (usize, usize), channels: usize) -> u64 {
        ((scales * size_of::<f32>() + phasors * size_of::<Complex32>()) * channels) as u64
            + TableBuilder::<ScaleKey, ScaleEntry<'static>>::reserved_bytes(scales)
            + TableBuilder::<PhaseKey, PhaseEntry<'static>>::reserved_bytes(phasors)
    }

    /// The most entries a batch of `rows` rows needs for `terms` terms: two
    /// a row, and no more than each integration's antennas, for
    /// integrations of at least `smallest_integration` rows of at most
    /// `antennas` antennas (`None` when the rows need not come in whole
    /// integrations). Two integrations may be partly in the batch.
    pub(crate) fn most_entries(
        terms: usize,
        rows: usize,
        integrations: Option<(usize, usize)>,
    ) -> usize {
        let by_rows = 2 * rows;
        let by_integrations = integrations.map_or(by_rows, |(smallest, antennas)| {
            (rows / smallest.max(1) + 2) * antennas
        });
        terms * by_rows.min(by_integrations)
    }

    /// The scales of antenna `end` (0 or 1) of `row` for `model` in
    /// `window`, spectral window `window_id`.
    ///
    /// # Panics
    ///
    /// When the batch's table was not built with the row
    /// ([`BatchScalesBuilder::add_rows`]).
    pub(crate) fn get(
        &self,
        (model, window_id, window): (&TsysAtm, usize, &WindowAtmosphere),
        row: &RowNoise,
        end: usize,
    ) -> &[f32] {
        self.scales.get(
            &ScaleKey::new(model, window_id, row, end),
            window.channels(),
        )
    }

    /// Antenna `antenna`'s phasors of the tropospheric `term` in its block
    /// `block` of spectral window `window`.
    ///
    /// # Panics
    ///
    /// When the batch's table was not built with them
    /// ([`BatchScalesBuilder::add_phase_rows`]).
    pub(crate) fn phasors(
        &self,
        term: &JonesTerm,
        (window, block): (usize, usize),
        antenna: usize,
    ) -> &[Complex32] {
        let width = term
            .tropospheric_phase()
            .and_then(|phase| phase.channels(window))
            .unwrap_or(0);
        self.phasors
            .get(&PhaseKey::new(term, (window, block), antenna), width)
    }
}

/// One kind's table as its entries are added, before they are worked out.
#[derive(Debug)]
struct TableBuilder<K, P> {
    index: HashMap<K, usize>,
    pending: Vec<P>,
    values: usize,
}

impl<K: Eq + Hash, P: Entry> TableBuilder<K, P> {
    /// Room for `entries` entries, reserved at once.
    fn with_capacity(entries: usize) -> Self {
        Self {
            index: HashMap::with_capacity(entries),
            pending: Vec::with_capacity(entries),
            values: 0,
        }
    }

    /// What room for `entries` entries holds beside their values: the
    /// index's buckets, a power of two at least 8/7 of the entries (std's
    /// `HashMap`), each with a control byte, and its control group; each
    /// entry's pending computation; and the jobs, at most one an entry,
    /// they are worked out in.
    fn reserved_bytes(entries: usize) -> u64 {
        if entries == 0 {
            return 0;
        }
        let buckets = (entries * 8).div_ceil(7).next_power_of_two().max(4);
        (buckets * (size_of::<(K, usize)>() + 1)
            + INDEX_GROUP_BYTES
            + entries * (size_of::<P>() + size_of::<BatchJob<'static, 'static>>())) as u64
    }

    /// Add the entry of `key`, `width` values worked out by what `entry`
    /// makes of its offset, unless it is in the table already.
    fn add(&mut self, key: K, width: usize, entry: impl FnOnce(usize) -> P) {
        if self.index.contains_key(&key) {
            return;
        }
        debug_assert!(
            self.pending.len() < self.pending.capacity(),
            "a batch needs no more entries than were reserved for it"
        );
        self.index.insert(key, self.values);
        self.pending.push(entry(self.values));
        self.values += width;
    }

    /// The values, and the pending entries in about `jobs` runs of
    /// consecutive ones.
    fn runs(&self, jobs: usize) -> (Vec<P::Value>, impl Iterator<Item = &[P]>) {
        let per_job = self.pending.len().div_ceil(jobs.max(1)).max(1);
        (
            vec![P::Value::default(); self.values],
            self.pending.chunks(per_job),
        )
    }
}

/// A batch's table as its rows are added, before the values are worked
/// out.
#[derive(Debug)]
pub(crate) struct BatchScalesBuilder<'a> {
    scales: TableBuilder<ScaleKey, ScaleEntry<'a>>,
    phasors: TableBuilder<PhaseKey, PhaseEntry<'a>>,
}

impl<'a> BatchScalesBuilder<'a> {
    /// A table with room for `entries` entries, (scales, phasors), reserved
    /// at once ([`BatchScales::most_entries`] of its batch), so adding them
    /// moves nothing.
    pub(crate) fn with_capacity((scales, phasors): (usize, usize)) -> Self {
        Self {
            scales: TableBuilder::with_capacity(scales),
            phasors: TableBuilder::with_capacity(phasors),
        }
    }

    /// Add the scales both antennas of each of `rows` need for `model` in
    /// `window`, spectral window `window_id`, whose channels are
    /// `channel_bandwidth_hz` wide.
    ///
    /// The entries must fit the room reserved for them
    /// ([`Self::with_capacity`]); a debug build asserts it.
    pub(crate) fn add_rows(
        &mut self,
        (model, window_id, window): (&'a TsysAtm, usize, &'a WindowAtmosphere),
        channel_bandwidth_hz: f64,
        rows: impl Iterator<Item = RowNoise>,
    ) {
        for row in rows {
            for end in 0..2 {
                let key = ScaleKey::new(model, window_id, &row, end);
                self.scales
                    .add(key, window.channels(), |offset| ScaleEntry {
                        model,
                        window,
                        antenna: (row.diameters_m[end], row.airmass[end]),
                        integration: (channel_bandwidth_hz, row.exposure_s),
                        offset,
                    });
            }
        }
    }

    /// Add the phasors both antennas of each of `rows`, (TIME, ANTENNA1,
    /// ANTENNA2), need for the tropospheric `term` in spectral window
    /// `window`.
    ///
    /// The entries must fit the room reserved for them
    /// ([`Self::with_capacity`]); a debug build asserts it.
    pub(crate) fn add_phase_rows(
        &mut self,
        (term, window): (&'a JonesTerm, usize),
        rows: impl Iterator<Item = (f64, usize, usize)>,
    ) {
        let Some(phase) = term.tropospheric_phase() else {
            return;
        };
        let width = phase.channels(window).unwrap_or(0);
        for (time_s, antenna1, antenna2) in rows {
            let block = term.block_at(window, time_s);
            for antenna in [antenna1, antenna2] {
                let key = PhaseKey::new(term, (window, block), antenna);
                self.phasors.add(key, width, |offset| PhaseEntry {
                    phase,
                    solution: (window, block),
                    antenna: (antenna, term.antennas()),
                    width,
                    offset,
                });
            }
        }
    }

    /// Whether no row needs scales or phasors.
    pub(crate) fn is_empty(&self) -> bool {
        self.scales.pending.is_empty() && self.phasors.pending.is_empty()
    }

    /// Work the values out, in about `jobs` jobs of each kind run by
    /// `run_jobs`.
    ///
    /// # Errors
    ///
    /// What `run_jobs` returns.
    pub(crate) fn build<E>(
        self,
        jobs: usize,
        run_jobs: impl FnOnce(&mut [BatchJob<'_, 'a>]) -> Result<(), E>,
    ) -> Result<BatchScales, E> {
        let (mut scales, scale_runs) = self.scales.runs(jobs);
        let (mut phasors, phase_runs) = self.phasors.runs(jobs);
        let mut work: Vec<BatchJob<'_, 'a>> = Vec::with_capacity(2 * jobs.max(1));
        split(&mut scales, scale_runs, |job| {
            work.push(BatchJob::Scales(job))
        });
        split(&mut phasors, phase_runs, |job| {
            work.push(BatchJob::Phasors(job));
        });
        run_jobs(&mut work)?;
        drop(work);
        Ok(BatchScales {
            scales: Table {
                index: self.scales.index,
                values: scales,
            },
            phasors: Table {
                index: self.phasors.index,
                values: phasors,
            },
        })
    }
}

/// `values` split along `runs` of consecutive entries, each run's share a
/// job handed to `push`.
fn split<'p, P: Entry + 'p>(
    values: &'p mut [P::Value],
    runs: impl Iterator<Item = &'p [P]>,
    mut push: impl FnMut(EntryJob<'p, P>),
) {
    let mut rest = values;
    for pending in runs {
        let last = pending.last().expect("runs are not empty");
        let length = last.offset() + last.width() - pending[0].offset();
        let (values, after) = std::mem::take(&mut rest).split_at_mut(length);
        push(EntryJob { pending, values });
        rest = after;
    }
}

/// Consecutive entries of one kind, worked out in one job.
pub(crate) struct EntryJob<'p, P: Entry> {
    pending: &'p [P],
    values: &'p mut [P::Value],
}

impl<P: Entry> EntryJob<'_, P> {
    fn run(&mut self) {
        let first = self.pending.first().map_or(0, Entry::offset);
        for pending in self.pending {
            let start = pending.offset() - first;
            pending.work_out(&mut self.values[start..start + pending.width()]);
        }
    }
}

/// A job of a batch's table: consecutive entries of one kind.
pub(crate) enum BatchJob<'p, 'a> {
    Scales(EntryJob<'p, ScaleEntry<'a>>),
    Phasors(EntryJob<'p, PhaseEntry<'a>>),
}

impl BatchJob<'_, '_> {
    /// Work the job's values out.
    pub(crate) fn run(&mut self) {
        match self {
            Self::Scales(job) => job.run(),
            Self::Phasors(job) => job.run(),
        }
    }
}

#[cfg(test)]
#[path = "batch_scales_tests.rs"]
mod tests;
