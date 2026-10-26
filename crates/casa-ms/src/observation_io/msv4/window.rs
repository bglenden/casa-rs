// SPDX-License-Identifier: LGPL-3.0-or-later
//! An update's held window: the values patches assign to one chunk-row of
//! times of one variable (every baseline and product, and the channel
//! chunks the traversal reaches), held until the traversal moves past those
//! times and then written chunk by chunk.
//!
//! A chunk every element of which the patches assigned is encoded from the
//! window and replaces the stored one without reading it; a chunk they
//! assigned only partly is read once, merged and replaced; a chunk they did
//! not touch is left alone. So batches in time order read each partly
//! covered chunk at most once per window, whatever their size, and a pass
//! that supplies whole chunks reads nothing. The window counts the elements
//! assigned in each chunk as it goes, so writing it visits only the chunks
//! touched.
//!
//! The window holds what the creation writer holds for a chunk it covers
//! partly ([`super::write`]): at most one chunk-row of times of the
//! variable over the channels held, reported as
//! [`UpdateReport::held_bytes`](casa_observation::UpdateReport) for the
//! producer to budget beside its batches. An updater dropped without
//! finishing discards what its windows hold.

use std::ops::Range;
use std::sync::Arc;

use casa_observation::ObservationError;
use num_complex::Complex32;
use zarrs::array::{Array, CodecOptions, Element, FromArrayBytes};

use super::metadata::storage;
use super::store::ReplacingStore;

/// An element type the update writes, with its fill value from the array's
/// native-endian fill bytes.
pub(super) trait Cell: Element + Copy + Send + Sync + 'static {
    fn from_fill(bytes: &[u8]) -> Self;
}

impl Cell for Complex32 {
    fn from_fill(bytes: &[u8]) -> Self {
        let part =
            |range: Range<usize>| f32::from_ne_bytes(bytes[range].try_into().expect("four bytes"));
        Complex32::new(part(0..4), part(4..8))
    }
}

impl Cell for bool {
    fn from_fill(bytes: &[u8]) -> Self {
        bytes[0] != 0
    }
}

impl Cell for f32 {
    fn from_fill(bytes: &[u8]) -> Self {
        f32::from_ne_bytes(bytes[0..4].try_into().expect("four bytes"))
    }
}

/// The values assigned to one chunk-row of times.
struct Held<T> {
    /// The row's chunk index along time.
    row: u64,
    /// The dataset times it holds.
    times: Range<usize>,
    /// `[time, baseline, channel, product]` over the held channels and
    /// every baseline and product.
    values: Vec<T>,
    /// Which elements a patch assigned.
    assigned: Vec<bool>,
    /// Elements assigned in each chunk of the row, `[baseline chunk,
    /// channel chunk, product chunk]` over the held channel chunks.
    counts: Vec<usize>,
}

/// One variable's held window (module documentation).
pub(super) struct Window<T: Cell> {
    array: Array<ReplacingStore>,
    /// The array's shape `[time, baseline, channel, product]`.
    shape: [usize; 4],
    /// The regular chunk shape.
    chunk: [usize; 4],
    /// The chunk grid's shape.
    grid: [usize; 4],
    /// The channels held: whole channel chunks.
    channels: Range<usize>,
    fill: T,
    held: Option<Held<T>>,
}

impl<T: Cell> Window<T>
where
    Vec<T>: FromArrayBytes,
{
    /// A window over the variable at `path` of `store` that holds the
    /// channel chunks covering `channels`.
    pub(super) fn open(
        store: &Arc<ReplacingStore>,
        path: &str,
        channels: Range<usize>,
    ) -> Result<Self, ObservationError> {
        let array = Array::open(store.clone(), path).map_err(storage)?;
        let dims = |values: &[u64]| -> Result<[usize; 4], ObservationError> {
            let values: Vec<usize> = values.iter().map(|n| *n as usize).collect();
            values
                .try_into()
                .map_err(|_| super::metadata::invalid(format!("{path} is not four-dimensional")))
        };
        let shape = dims(array.shape())?;
        let chunk = dims(
            &array
                .chunk_shape(&[0, 0, 0, 0])
                .map_err(storage)?
                .iter()
                .map(|n| n.get())
                .collect::<Vec<_>>(),
        )?;
        let grid = dims(array.chunk_grid_shape())?;
        if channels.is_empty() || channels.end > shape[2] {
            return Err(super::metadata::invalid(format!(
                "channels {channels:?} are not {path}'s"
            )));
        }
        let start = channels.start / chunk[2] * chunk[2];
        let end = (channels.end.div_ceil(chunk[2]) * chunk[2]).min(shape[2]);
        let fill = T::from_fill(array.fill_value().as_ne_bytes());
        Ok(Self {
            array,
            shape,
            chunk,
            grid,
            channels: start..end,
            fill,
            held: None,
        })
    }

    /// The most bytes the window holds: one chunk-row of the variable over
    /// the held channels, its assignment marks and chunk counts.
    pub(super) fn held_bytes(&self) -> usize {
        let [_, baselines, _, products] = self.shape;
        self.chunk[0] * baselines * self.channels.len() * products * (size_of::<T>() + 1)
            + self.chunk_count() * size_of::<usize>()
    }

    /// Chunks in one row over the held channels.
    fn chunk_count(&self) -> usize {
        self.grid[1] * self.channels.len().div_ceil(self.chunk[2]) * self.grid[3]
    }

    /// Assign `value(sample, channel, product)` (`None` keeps what is
    /// stored) for dataset times `times`, every baseline, the array's
    /// channels `channels` and products `products`; a sample is a time's
    /// baselines in order, from `times.start`.
    pub(super) fn assign(
        &mut self,
        times: Range<usize>,
        channels: Range<usize>,
        products: &[usize],
        options: &CodecOptions,
        value: impl Fn(usize, usize, usize) -> Option<T>,
    ) -> Result<(), ObservationError> {
        if channels.start < self.channels.start || channels.end > self.channels.end {
            return Err(crate::observation_io::update::request_error(format!(
                "channels {channels:?} are outside the window's {:?}",
                self.channels
            )));
        }
        let [total, baselines, _, all_products] = self.shape;
        let held_channels = self.channels.len();
        let channel_chunks = held_channels.div_ceil(self.chunk[2]);
        for time in times.clone() {
            let row = (time / self.chunk[0]) as u64;
            if self.held.as_ref().is_none_or(|held| held.row != row) {
                self.flush(options)?;
                let start = row as usize * self.chunk[0];
                let held_times = start..(start + self.chunk[0]).min(total);
                let len = held_times.len() * baselines * held_channels * all_products;
                self.held = Some(Held {
                    row,
                    times: held_times,
                    values: vec![self.fill; len],
                    assigned: vec![false; len],
                    counts: vec![0; self.chunk_count()],
                });
            }
            let held = self.held.as_mut().expect("opened");
            for baseline in 0..baselines {
                let sample = (time - times.start) * baselines + baseline;
                let base = ((time - held.times.start) * baselines + baseline) * held_channels;
                let baseline_chunk = baseline / self.chunk[1];
                for (channel_index, channel) in channels.clone().enumerate() {
                    let local = channel - self.channels.start;
                    let chunks =
                        (baseline_chunk * channel_chunks + local / self.chunk[2]) * self.grid[3];
                    for (product_index, product) in products.iter().enumerate() {
                        if let Some(new) = value(sample, channel_index, product_index) {
                            let element = (base + local) * all_products + product;
                            held.values[element] = new;
                            if !held.assigned[element] {
                                held.assigned[element] = true;
                                held.counts[chunks + product / self.chunk[3]] += 1;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Write what is held, chunk by chunk, and hold nothing.
    pub(super) fn flush(&mut self, options: &CodecOptions) -> Result<(), ObservationError> {
        let Some(held) = self.held.take() else {
            return Ok(());
        };
        let first_channel_chunk = self.channels.start / self.chunk[2];
        let channel_chunks = self.channels.len().div_ceil(self.chunk[2]);
        for (index, count) in held.counts.iter().enumerate() {
            if *count == 0 {
                continue;
            }
            let product = index % self.grid[3];
            let channel = index / self.grid[3] % channel_chunks;
            let baseline = index / self.grid[3] / channel_chunks;
            let indices = [
                held.row,
                baseline as u64,
                (first_channel_chunk + channel) as u64,
                product as u64,
            ];
            self.write_chunk(&held, indices, *count, options)?;
        }
        Ok(())
    }

    /// Write chunk `indices` of `held`'s row, of which `assigned` elements
    /// are assigned: from the window when every in-array element is,
    /// merged into the stored chunk otherwise.
    fn write_chunk(
        &self,
        held: &Held<T>,
        indices: [u64; 4],
        assigned: usize,
        options: &CodecOptions,
    ) -> Result<(), ObservationError> {
        let bounded = self.array.chunk_subset_bounded(&indices).map_err(storage)?;
        let start: Vec<usize> = bounded.start().iter().map(|n| *n as usize).collect();
        let extent = bounded.shape_usize();
        let shape = self.array.chunk_shape_usize(&indices).map_err(storage)?;
        let [_, baselines, _, products] = self.shape;
        let held_channels = self.channels.len();
        let mut chunk: Vec<T> = if assigned == extent.iter().product::<usize>() {
            vec![self.fill; shape.iter().product()]
        } else {
            self.array
                .retrieve_chunk_opt::<Vec<T>>(&indices, options)
                .map_err(storage)?
        };
        for t in 0..extent[0] {
            for b in 0..extent[1] {
                let row = ((start[0] + t - held.times.start) * baselines + start[1] + b)
                    * held_channels
                    + start[2]
                    - self.channels.start;
                for f in 0..extent[2] {
                    let element = (row + f) * products + start[3];
                    let at = ((t * shape[1] + b) * shape[2] + f) * shape[3];
                    for p in 0..extent[3] {
                        if held.assigned[element + p] {
                            chunk[at + p] = held.values[element + p];
                        }
                    }
                }
            }
        }
        self.array
            .store_chunk_opt(&indices, chunk.as_slice(), options)
            .map_err(storage)
    }
}
