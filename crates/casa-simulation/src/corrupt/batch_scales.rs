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
//!
//! A table reserves room for the most entries its batch can need before any
//! is added ([`BatchScalesBuilder::with_capacity`]): a vector or index that
//! grew as entries came would hold its old allocation and its new one
//! together while it moved (#897 review).

use std::collections::HashMap;

use super::tsys::RowNoise;
use super::tsys_atm::{TsysAtm, WindowAtmosphere};

/// Bytes of the index beside its buckets: its control group, and the
/// alignment of the control bytes after the buckets.
const INDEX_GROUP_BYTES: usize = 32;

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
    /// The most a table with room for `entries` entries of at most
    /// `channels` channels holds while it is built and read: their scales,
    /// and the room reserved for them.
    pub(crate) fn bytes(entries: usize, channels: usize) -> u64 {
        (entries * channels * size_of::<f32>()) as u64 + BatchScalesBuilder::reserved_bytes(entries)
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
#[derive(Debug)]
pub(crate) struct BatchScalesBuilder<'a> {
    index: HashMap<ScaleKey, usize>,
    pending: Vec<Pending<'a>>,
    values: usize,
}

impl<'a> BatchScalesBuilder<'a> {
    /// A table with room for `entries` entries, reserved at once
    /// ([`BatchScales::most_entries`] of its batch), so adding them moves
    /// nothing.
    pub(crate) fn with_capacity(entries: usize) -> Self {
        Self {
            index: HashMap::with_capacity(entries),
            pending: Vec::with_capacity(entries),
            values: 0,
        }
    }

    /// What room for `entries` entries holds beside their scales: the
    /// index's buckets, a power of two at least 8/7 of the entries (std's
    /// `HashMap`), each with a control byte, and its control group; each
    /// entry's pending computation; and the jobs, at most one an entry,
    /// they are worked out in.
    fn reserved_bytes(entries: usize) -> u64 {
        if entries == 0 {
            return 0;
        }
        let buckets = (entries * 8).div_ceil(7).next_power_of_two().max(4);
        (buckets * (size_of::<(ScaleKey, usize)>() + 1)
            + INDEX_GROUP_BYTES
            + entries * (size_of::<Pending<'static>>() + size_of::<ScalesJob<'static, 'static>>()))
            as u64
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
                if self.index.contains_key(&key) {
                    continue;
                }
                debug_assert!(
                    self.pending.len() < self.pending.capacity(),
                    "a batch needs no more entries than were reserved for it"
                );
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

#[cfg(test)]
mod tests {
    use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ProfileParameters};
    use casa_imaging_runtime::Reservation;

    use super::{BatchScales, BatchScalesBuilder, INDEX_GROUP_BYTES, Pending, ScaleKey, ScalesJob};
    use crate::corrupt::TsysParameters;
    use crate::corrupt::tsys::RowNoise;
    use crate::corrupt::tsys_atm::{self, TsysAtm, WindowAtmosphere};

    /// A `tsys-atm` model of one window of one channel at 230 GHz.
    fn one_channel_model() -> TsysAtm {
        let atmosphere = AtmosphereProfile::new(&ProfileParameters::default()).expect("profile");
        let channels = [230e9];
        let grid = tsys_atm::window_grid(&channels).expect("grid");
        let totals = tsys_atm::channel_totals(&atmosphere, &channels);
        let profile =
            AbsorptionProfile::from_channels(atmosphere, grid, totals).expect("absorption");
        let window = WindowAtmosphere::new(&profile, 1e-3, (0.95, 270.0)).expect("window");
        TsysAtm::new(
            &TsysParameters::default(),
            vec![Some(window)],
            Reservation::none(),
        )
    }

    /// The #897 review's case: 2,731 integrations of three antennas, three
    /// baselines each, in one channel for one term, so 8,193 entries, just
    /// past a power of two. Reserved for its batch, the table moves neither
    /// its index nor its pending computations as they are added (growing,
    /// each would hold its old allocation and a new one twice the size),
    /// and what it allocates is within what it is admitted.
    #[test]
    fn a_table_reserved_for_its_batch_moves_nothing_and_allocates_its_admission() {
        let model = one_channel_model();
        let window = model.window(0);
        let (integrations, antennas) = (2_731, 3);
        let entries = BatchScales::most_entries(1, 3 * integrations, Some((3, antennas)));
        let mut builder = BatchScalesBuilder::with_capacity(entries);
        let room = (builder.index.capacity(), builder.pending.capacity());
        for integration in 0..integrations {
            // Each antenna of each integration at an airmass of its own.
            let airmass = |antenna: usize| 1.0 + (integration * antennas + antenna) as f32 * 1e-4;
            let rows = [(0, 1), (0, 2), (1, 2)].map(|(i, j)| RowNoise {
                diameters_m: [12.0; 2],
                airmass: [airmass(i), airmass(j)],
                exposure_s: 10.0,
            });
            builder.add_rows((&model, 0, window), 1e6, rows.into_iter());
        }
        assert_eq!(builder.pending.len(), integrations * antennas);
        assert_eq!(
            (builder.index.capacity(), builder.pending.capacity()),
            room,
            "adding the entries moved nothing"
        );
        // std's `HashMap` fills 7/8 of its buckets, a power of two.
        let buckets = (room.0 * 8).div_ceil(7).next_power_of_two();
        let allocated = buckets * (size_of::<(ScaleKey, usize)>() + 1)
            + INDEX_GROUP_BYTES
            + room.1 * size_of::<Pending<'_>>()
            + builder.pending.len() * size_of::<ScalesJob<'_, '_>>()
            + builder.values * size_of::<f32>();
        assert!(
            allocated as u64 <= BatchScales::bytes(entries, 1),
            "the table allocates {allocated} bytes, beyond the {} admitted",
            BatchScales::bytes(entries, 1)
        );
        let scales = builder
            .build(4, |jobs| {
                jobs.iter_mut().for_each(ScalesJob::run);
                Ok::<_, ()>(())
            })
            .expect("built");
        let last = RowNoise {
            diameters_m: [12.0; 2],
            airmass: [1.0 + 8_192.0 * 1e-4; 2],
            exposure_s: 10.0,
        };
        let [scale] = scales.get((&model, 0, window), &last, 0) else {
            panic!("one channel");
        };
        let mut expected = [0.0];
        model.antenna_scales(window, (12.0, last.airmass[0]), (1e6, 10.0), &mut expected);
        assert_eq!(*scale, expected[0]);
    }
}
