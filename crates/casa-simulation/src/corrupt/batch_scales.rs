// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna scales of a write batch's `tsys-atm` noise.
//!
//! A `tsys-atm` row's σ in channel `c` is `s_i(c) · s_j(c)`, each antenna's
//! scale from its Tsys at its airmass ([`TsysAtm::antenna_scales`]), so an
//! antenna's scales belong to the antenna and the integration, as a Jones
//! term does, not to the row. A batch's scales are worked out once, on the
//! workers, before the noise jobs that read them: each costs a line of
//! sight through the atmosphere's layers per channel, and a job of its own
//! for every run of rows would repeat it as many times as an integration's
//! rows are split into jobs.
//!
//! An entry is keyed by what its scales depend on (the term, the window,
//! and the antenna's dish, airmass and the row's exposure), never by where
//! the batch starts, so an integration that two batches share gets the
//! same scales in both.

use std::collections::HashMap;

use super::tsys::RowNoise;
use super::tsys_atm::{TsysAtm, WindowAtmosphere};

/// Bytes of a table entry beside its scales, at most: its key in the
/// index, which may have twice the buckets it fills, and its pending
/// computation, whose vector may have twice the room.
const ENTRY_BYTES: usize = 2 * (size_of::<(ScaleKey, usize)>() + 1)
    + 2 * size_of::<Pending<'static>>()
    + size_of::<usize>();

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

/// An entry still to be worked out.
#[derive(Debug)]
struct Pending<'a> {
    model: &'a TsysAtm,
    window: &'a WindowAtmosphere,
    /// The antenna's dish diameter and airmass.
    antenna: (f64, f32),
    /// The window's channel bandwidth and the row's exposure.
    integration: (f64, f64),
    /// Where its scales start in the table.
    offset: usize,
}

/// The scales a batch's `tsys-atm` noise reads, by antenna and integration.
#[derive(Debug, Default)]
pub(crate) struct BatchScales {
    /// Each entry's first scale in `values`.
    index: HashMap<ScaleKey, usize>,
    values: Vec<f32>,
}

impl BatchScales {
    /// The most a table of `entries` entries of at most `channels` channels
    /// holds while it is built and read.
    pub(crate) fn bytes(entries: usize, channels: usize) -> u64 {
        (entries * (channels * size_of::<f32>() + ENTRY_BYTES)) as u64
    }

    /// The most entries a batch of `rows` rows needs for `terms` `tsys-atm`
    /// terms: two a row, and no more than each integration's antennas, for
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
        let offset = self.index[&ScaleKey::new(model, window_id, row, end)];
        &self.values[offset..offset + window.channels()]
    }
}

/// A batch's table as its rows are added, before the scales are worked
/// out.
#[derive(Debug, Default)]
pub(crate) struct BatchScalesBuilder<'a> {
    index: HashMap<ScaleKey, usize>,
    pending: Vec<Pending<'a>>,
    values: usize,
}

impl<'a> BatchScalesBuilder<'a> {
    /// Add the scales both antennas of each of `rows` need for `model` in
    /// `window`, spectral window `window_id`, whose channels are
    /// `channel_bandwidth_hz` wide.
    pub(crate) fn add_rows(
        &mut self,
        (model, window_id, window): (&'a TsysAtm, usize, &'a WindowAtmosphere),
        channel_bandwidth_hz: f64,
        rows: impl Iterator<Item = RowNoise>,
    ) {
        for row in rows {
            for end in 0..2 {
                let key = ScaleKey::new(model, window_id, &row, end);
                if self.index.contains_key(&key) {
                    continue;
                }
                self.index.insert(key, self.values);
                self.pending.push(Pending {
                    model,
                    window,
                    antenna: (row.diameters_m[end], row.airmass[end]),
                    integration: (channel_bandwidth_hz, row.exposure_s),
                    offset: self.values,
                });
                self.values += window.channels();
            }
        }
    }

    /// Whether no row needs scales.
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Work the scales out, in about `jobs` jobs run by `run_jobs`.
    ///
    /// # Errors
    ///
    /// What `run_jobs` returns.
    pub(crate) fn build<E>(
        self,
        jobs: usize,
        run_jobs: impl FnOnce(&mut [ScalesJob<'_, 'a>]) -> Result<(), E>,
    ) -> Result<BatchScales, E> {
        let mut values = vec![0.0; self.values];
        let per_job = self.pending.len().div_ceil(jobs.max(1)).max(1);
        let mut work = Vec::with_capacity(self.pending.len().div_ceil(per_job));
        let mut rest = values.as_mut_slice();
        for pending in self.pending.chunks(per_job) {
            let last = pending.last().expect("chunks are not empty");
            let length = last.offset + last.window.channels() - pending[0].offset;
            let (scales, after) = std::mem::take(&mut rest).split_at_mut(length);
            work.push(ScalesJob { pending, scales });
            rest = after;
        }
        run_jobs(&mut work)?;
        drop(work);
        Ok(BatchScales {
            index: self.index,
            values,
        })
    }
}

/// Consecutive entries of a batch's table, worked out in one job.
pub(crate) struct ScalesJob<'p, 'a> {
    pending: &'p [Pending<'a>],
    scales: &'p mut [f32],
}

impl ScalesJob<'_, '_> {
    /// Work the job's scales out.
    pub(crate) fn run(&mut self) {
        let first = self.pending.first().map_or(0, |pending| pending.offset);
        for pending in self.pending {
            let start = pending.offset - first;
            pending.model.antenna_scales(
                pending.window,
                pending.antenna,
                pending.integration,
                &mut self.scales[start..start + pending.window.channels()],
            );
        }
    }
}
