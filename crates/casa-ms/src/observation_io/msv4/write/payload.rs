// SPDX-License-Identifier: LGPL-3.0-or-later
//! The writer's payload: each batch checked against the declaration before
//! any byte is written, encoded from its views where they are already in
//! the stored layout and in whole chunks (`chunks`).

use std::ops::Range;

use casa_observation::{ObservationError, ScanId, Shared, VisibilityBatch};
use casa_types::measures::{EpochView, MjdHighPrec, UvwView};
use ndarray::{ArrayView1, ArrayView3};
use num_complex::Complex32;
use zarrs::array::{Array, ArraySubset, CodecOptions, Element};
use zarrs::filesystem::FilesystemStore;

use super::chunks::{self, Columns, Held, PerTime, Step};
use super::{Msv4WriteCounters, Msv4Writer, Payload, request_error};
use crate::observation_io::msv4::metadata::{UNIX_EPOCH_MJD, invalid, storage};

/// Reusable working storage for what is not encoded straight from a view:
/// the time centroids re-expressed from the Unix epoch, the exposures per
/// sample, and a weight shared by every channel written for each channel.
#[derive(Default)]
pub(super) struct Scratch {
    weights: Vec<f32>,
    centroid: Vec<f64>,
    exposure: Vec<f64>,
}

/// Where a batch goes: its first time in the dataset and how many times it
/// holds.
struct Placement {
    first_time: usize,
    times: usize,
}

/// A batch the declaration admits: the views a write encodes, each proven
/// present, and where its times go.
struct Checked<'b> {
    values: ArrayView3<'b, Complex32>,
    flags: ArrayView3<'b, bool>,
    weights: ArrayView3<'b, f32>,
    uvw: UvwView<'b>,
    centroid: EpochView<'b>,
    exposure: Shared<'b, f64>,
    samples_per_time: usize,
    placement: Placement,
}

/// `view`'s elements as the one slice they occupy in standard layout.
fn contiguous<'a, T>(
    view: ndarray::ArrayView3<'a, T>,
    name: &str,
) -> Result<&'a [T], ObservationError> {
    view.to_slice()
        .ok_or_else(|| request_error(format!("the batch's {name} are not contiguous")))
}

fn mismatch(what: &str) -> ObservationError {
    request_error(format!(
        "the batch's {what} is not the declared dataset's; the writer converts nothing"
    ))
}

impl Msv4Writer {
    /// Write `batch`: whole times of the declared grid, every channel,
    /// every product, every role.
    ///
    /// Before any byte is written the batch is checked against the
    /// declaration: its partition, baseline table, channel frequencies and
    /// frame, correlation products, visibility units and UVW frame must be
    /// the declared ones (not merely the same counts); each time's instant,
    /// field, scan and interval must be the grid's. The batch's grid is the
    /// dataset's, so its visibilities and flags are encoded straight from
    /// their views; a weight shared by every channel is written for each
    /// channel (counted as packed).
    ///
    /// The chunks the batch covers whole are stored at once. Its times in a
    /// chunk it covers only partly are held (copied) until the chunk's
    /// other times arrive, and the chunk is then stored whole, so batches in
    /// time order encode every chunk once whatever their size; at most one
    /// chunk of times is held ([`Self::buffer_bytes`]). Held times that the
    /// next batch does not continue are stored as they are, and that chunk
    /// is decoded and re-encoded when its other times come
    /// ([`Msv4WriteCounters::partial_chunks`]).
    ///
    /// # Errors
    ///
    /// When a check fails, a time is written twice, or the batch is a
    /// channel window: nothing of the batch is then written or held. A
    /// storage error leaves the dataset incomplete; drop the writer.
    pub fn write_batch(&mut self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let Checked {
            values,
            flags,
            weights,
            uvw,
            centroid,
            exposure,
            samples_per_time,
            placement,
        } = self.check(batch)?;
        let values = contiguous(values, "visibilities")?;
        let flags = contiguous(flags, "flags")?;
        self.gather_times(centroid, exposure, samples_per_time);
        if !weights.is_standard_layout() {
            self.scratch.weights.clear();
            self.scratch.weights.extend(weights.iter());
            self.counters.packed_elements += self.scratch.weights.len();
        }
        let metres = uvw.metres();
        let metres = metres.as_standard_layout();
        let scratch = &self.scratch;
        let columns = Columns {
            values,
            flags,
            weights: weights.to_slice().unwrap_or(scratch.weights.as_slice()),
            uvw: metres.as_slice().expect("standard"),
            centroid: &scratch.centroid,
            exposure: &scratch.exposure,
        };
        let per = self.per_time();
        let total = self.written.len();
        let times = placement.first_time..placement.first_time + placement.times;
        let in_batch =
            |dataset: &Range<usize>| dataset.start - times.start..dataset.end - times.start;
        let payload = self.payload.as_ref().expect("created");
        for step in chunks::plan(self.held.times(), times.clone(), self.chunk_times, total) {
            match step {
                Step::Hold(held) => {
                    self.counters.held_elements += held.len() * per.elements();
                    let part = columns.times(&in_batch(&held), per);
                    self.held.hold(held, part, per, self.chunk_times.min(total));
                }
                Step::Store(run) => {
                    store(
                        payload,
                        &run,
                        columns.times(&in_batch(&run), per),
                        &self.options,
                    )?;
                    self.counters.region_writes += 6;
                }
                Step::Release => release(
                    &mut self.held,
                    payload,
                    &self.options,
                    &mut self.counters,
                    self.chunk_times,
                    total,
                )?,
            }
        }
        self.counters.elements += times.len() * per.elements();
        self.written[times].iter_mut().for_each(|done| *done = true);
        Ok(())
    }

    /// The elements one time holds in each payload variable.
    fn per_time(&self) -> PerTime {
        let [_, baselines, channels, correlations] = self.shape;
        PerTime {
            baselines,
            cube: baselines * channels * correlations,
        }
    }

    /// The most the writer holds between batches: one chunk of times
    /// ([`Self::chunk_times`]) of every payload variable, over every
    /// baseline, channel and product. It is reserved when a batch first
    /// covers a chunk only partly; batches that cover whole chunks hold
    /// nothing.
    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.chunk_times.min(self.written.len()) * self.per_time().bytes()
    }

    /// Everything the write needs of `batch`, checked against the
    /// declaration before any byte is written.
    fn check<'b>(&self, batch: &VisibilityBatch<'b>) -> Result<Checked<'b>, ObservationError> {
        self.check_context(batch)?;
        let timing = &batch.timing;
        let (Some(centroid), Some(exposure), Some(_), Some(scan)) = (
            timing.centroid,
            timing.exposure_s,
            timing.interval_s,
            batch.scan,
        ) else {
            return Err(request_error(
                "a written batch carries its time centroid, interval, exposure and scan",
            ));
        };
        let group = &batch.group;
        let (Some(values), Some(flags), Some(weights), Some(uvw)) =
            (group.values, group.flags, group.weights, group.uvw)
        else {
            return Err(request_error("a written batch carries every role"));
        };
        Ok(Checked {
            values,
            flags,
            weights,
            uvw,
            centroid,
            exposure,
            samples_per_time: batch.samples_per_time(),
            placement: self.place(batch, scan)?,
        })
    }

    /// The batch's context against the declaration's, before any sample.
    fn check_context(&self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let target = &self.target;
        let entry = &target.entry;
        if batch.partition != target.partition {
            return Err(mismatch("partition"));
        }
        if batch.baselines != target.grid.baselines.as_slice() {
            return Err(mismatch("baseline table"));
        }
        if batch.first_channel != 0 || batch.channel_count() != entry.frequencies_hz.len() {
            return Err(ObservationError::Unsupported {
                what: "writing a channel window",
            });
        }
        if batch.frequency.reference() != entry.frequency_frame
            || batch
                .frequency
                .hz()
                .iter()
                .zip(&entry.frequencies_hz)
                .any(|(a, b)| a.to_bits() != b.to_bits())
        {
            return Err(mismatch("channel frequencies or spectral frame"));
        }
        if batch.correlations != entry.correlations.as_slice() {
            return Err(mismatch("correlation products"));
        }
        let group = &batch.group;
        if group.entry.units != target.group.units {
            return Err(mismatch("visibility units"));
        }
        if group
            .uvw
            .is_some_and(|uvw| uvw.reference() != target.group.uvw_frame.effective)
        {
            return Err(mismatch("UVW frame"));
        }
        if batch.time.reference() != target.grid.time_scale {
            return Err(mismatch("time scale"));
        }
        Ok(())
    }

    /// The batch's times in the dataset, with each time's field, scan and
    /// interval checked against the grid: contiguous and not yet written.
    fn place(
        &self,
        batch: &VisibilityBatch<'_>,
        scan: ArrayView1<'_, ScanId>,
    ) -> Result<Placement, ObservationError> {
        let target = &self.target;
        let grid = &target.grid;
        let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let times = batch.times();
        if times == 0 {
            return Err(request_error("an empty batch"));
        }
        let per_time = batch.samples_per_time();
        let index_of = |t: usize| {
            let seconds = batch.time.seconds_since(t, unix);
            target
                .times
                .get(&seconds.to_bits())
                .copied()
                .ok_or_else(|| invalid(format!("time {seconds} is not in the dataset")))
        };
        let first_time = index_of(0)?;
        for t in 0..times {
            let index = index_of(t)?;
            if batch.field[t] != grid.field_per_time[index] {
                return Err(mismatch("field at a time"));
            }
            if scan[t] != grid.scan_per_time[index] {
                return Err(mismatch("scan at a time"));
            }
            // An absent acquisition (padding) has no interval: NaN.
            if (t * per_time..(t + 1) * per_time).any(|sample| {
                batch
                    .interval_s(sample)
                    .is_none_or(|interval| !interval.is_nan() && interval != grid.interval_s)
            }) {
                return Err(mismatch("integration interval"));
            }
            if index != first_time + t {
                return Err(invalid("a batch's times are not contiguous in the dataset"));
            }
        }
        if first_time + times > self.written.len()
            || self.written[first_time..first_time + times]
                .iter()
                .any(|done| *done)
        {
            return Err(invalid("a time is written twice or beyond the dataset"));
        }
        Ok(Placement { first_time, times })
    }

    /// The time centroids (in Unix seconds) and exposures per sample,
    /// counted as packed unless the batch already holds them that way.
    fn gather_times(
        &mut self,
        centroid: EpochView<'_>,
        exposure: Shared<'_, f64>,
        samples_per_time: usize,
    ) {
        let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let in_place = centroid.origin() == unix && matches!(exposure, Shared::PerSample(_));
        let scratch = &mut self.scratch;
        let samples = centroid.len();
        scratch.centroid.clear();
        scratch.exposure.clear();
        for sample in 0..samples {
            scratch.centroid.push(centroid.seconds_since(sample, unix));
            scratch
                .exposure
                .push(exposure.get(sample, samples_per_time));
        }
        if !in_place {
            self.counters.packed_elements += 2 * samples;
        }
    }

    /// Check every time was written and store any times still held; the
    /// dataset is then complete, and its processing set may be published
    /// ([`Msv4ProcessingSetWriter::finish`](super::Msv4ProcessingSetWriter::finish)).
    /// Returns what the writer did.
    ///
    /// # Errors
    ///
    /// When a time was not written, or the held times cannot be stored:
    /// the dataset stays incomplete and its processing set cannot be
    /// published.
    pub fn finish(mut self) -> Result<Msv4WriteCounters, ObservationError> {
        if let Some(time) = self.written.iter().position(|done| !done) {
            return Err(invalid(format!("time {time} was never written")));
        }
        if !self.held.times().is_empty() {
            release(
                &mut self.held,
                self.payload.as_ref().expect("created"),
                &self.options,
                &mut self.counters,
                self.chunk_times,
                self.written.len(),
            )?;
        }
        self.registry.finished(&self.dataset);
        Ok(self.counters)
    }
}

/// Store `columns`, the elements of dataset times `times`, into every
/// payload variable.
fn store(
    payload: &Payload,
    times: &Range<usize>,
    columns: Columns<'_>,
    options: &CodecOptions,
) -> Result<(), ObservationError> {
    let times = times.start as u64..times.end as u64;
    store_times(&payload.values, &times, columns.values, options)?;
    store_times(&payload.flags, &times, columns.flags, options)?;
    store_times(&payload.weights, &times, columns.weights, options)?;
    store_times(&payload.uvw, &times, columns.uvw, options)?;
    store_times(&payload.centroid, &times, columns.centroid, options)?;
    store_times(&payload.exposure, &times, columns.exposure, options)
}

/// Store `data` at `times` of `array`, over the whole of its other axes.
fn store_times<T: Element>(
    array: &Array<FilesystemStore>,
    times: &Range<u64>,
    data: &[T],
    options: &CodecOptions,
) -> Result<(), ObservationError> {
    let ranges: Vec<Range<u64>> = std::iter::once(times.clone())
        .chain(array.shape()[1..].iter().map(|&extent| 0..extent))
        .collect();
    array
        .store_array_subset_opt(&ArraySubset::new_with_ranges(&ranges), data, options)
        .map_err(storage)
}

/// Store the held times and hold none, counting their chunk as partly
/// covered unless they are all of it.
fn release(
    held: &mut Held,
    payload: &Payload,
    options: &CodecOptions,
    counters: &mut Msv4WriteCounters,
    chunk: usize,
    total: usize,
) -> Result<(), ObservationError> {
    let times = held.times();
    store(payload, &times, held.columns(), options)?;
    counters.region_writes += 6;
    if times != chunks::chunk_of(times.start, chunk, total) {
        counters.partial_chunks += 6;
    }
    held.clear();
    Ok(())
}
