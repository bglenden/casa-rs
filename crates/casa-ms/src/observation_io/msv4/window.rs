// SPDX-License-Identifier: LGPL-3.0-or-later
//! An update's held window: the values patches assign to one chunk-row of
//! times of one variable (every baseline, channel and product), held until
//! the traversal moves past those times and then written chunk by chunk.
//!
//! A chunk every element of which the patches assigned is encoded from the
//! window and replaces the stored one without reading it; a chunk they
//! assigned only partly is read once, merged and replaced; a chunk they did
//! not touch is left alone. So batches in time order read each partly
//! covered chunk at most once per window, whatever their size, and a pass
//! that supplies whole chunks reads nothing.

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
    /// `[time, baseline, channel, product]` over the whole array.
    values: Vec<T>,
    /// Which elements a patch assigned.
    assigned: Vec<bool>,
}

/// One variable's held window (module documentation).
pub(super) struct Window<T: Cell> {
    array: Array<ReplacingStore>,
    /// The array's shape `[time, baseline, channel, product]`.
    shape: [usize; 4],
    /// The regular chunk shape.
    chunk: [usize; 4],
    fill: T,
    held: Option<Held<T>>,
}

impl<T: Cell> Window<T>
where
    Vec<T>: FromArrayBytes,
{
    /// A window over the variable at `path` of `store`.
    pub(super) fn open(store: &Arc<ReplacingStore>, path: &str) -> Result<Self, ObservationError> {
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
        let fill = T::from_fill(array.fill_value().as_ne_bytes());
        Ok(Self {
            array,
            shape,
            chunk,
            fill,
            held: None,
        })
    }

    /// The most bytes the window holds: one chunk-row of the variable, and
    /// its assignment marks.
    pub(super) fn held_bytes(&self) -> usize {
        let [_, baselines, channels, products] = self.shape;
        self.chunk[0] * baselines * channels * products * (size_of::<T>() + 1)
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
        let [total, baselines, all_channels, all_products] = self.shape;
        for time in times.clone() {
            let row = (time / self.chunk[0]) as u64;
            if self.held.as_ref().is_none_or(|held| held.row != row) {
                self.flush(options)?;
                let start = row as usize * self.chunk[0];
                let held_times = start..(start + self.chunk[0]).min(total);
                let len = held_times.len() * baselines * all_channels * all_products;
                self.held = Some(Held {
                    row,
                    times: held_times,
                    values: vec![self.fill; len],
                    assigned: vec![false; len],
                });
            }
            let held = self.held.as_mut().expect("opened");
            for baseline in 0..baselines {
                let sample = (time - times.start) * baselines + baseline;
                let base = ((time - held.times.start) * baselines + baseline) * all_channels;
                for (channel_index, channel) in channels.clone().enumerate() {
                    for (product_index, product) in products.iter().enumerate() {
                        if let Some(new) = value(sample, channel_index, product_index) {
                            let element = (base + channel) * all_products + product;
                            held.values[element] = new;
                            held.assigned[element] = true;
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
        let grid = self.array.chunk_grid_shape().to_vec();
        for b in 0..grid[1] {
            for f in 0..grid[2] {
                for p in 0..grid[3] {
                    self.write_chunk(&held, [held.row, b, f, p], options)?;
                }
            }
        }
        Ok(())
    }

    /// Write chunk `indices` of `held`'s row: from the window when every
    /// in-array element is assigned, merged into the stored chunk when
    /// some are, not at all when none is.
    fn write_chunk(
        &self,
        held: &Held<T>,
        indices: [u64; 4],
        options: &CodecOptions,
    ) -> Result<(), ObservationError> {
        let bounded = self.array.chunk_subset_bounded(&indices).map_err(storage)?;
        let four =
            |values: Vec<usize>| -> [usize; 4] { values.try_into().expect("four dimensions") };
        let start = four(bounded.start().iter().map(|n| *n as usize).collect());
        let extent = four(bounded.shape_usize());
        let [_, baselines, channels, products] = self.shape;
        let held_index = |[t, b, f, p]: [usize; 4]| {
            ((((t - held.times.start) * baselines + b) * channels + f) * products) + p
        };
        let elements = || {
            (0..extent[0]).flat_map(move |t| {
                (0..extent[1]).flat_map(move |b| {
                    (0..extent[2]).flat_map(move |f| (0..extent[3]).map(move |p| [t, b, f, p]))
                })
            })
        };
        let global = |local: [usize; 4]| {
            [
                start[0] + local[0],
                start[1] + local[1],
                start[2] + local[2],
                start[3] + local[3],
            ]
        };
        let assigned = elements()
            .filter(|local| held.assigned[held_index(global(*local))])
            .count();
        if assigned == 0 {
            return Ok(());
        }
        let shape = self.array.chunk_shape_usize(&indices).map_err(storage)?;
        let mut chunk: Vec<T> = if assigned == extent.iter().product::<usize>() {
            vec![self.fill; shape.iter().product()]
        } else {
            self.array
                .retrieve_chunk_opt::<Vec<T>>(&indices, options)
                .map_err(storage)?
        };
        for local in elements() {
            let element = held_index(global(local));
            if held.assigned[element] {
                let at =
                    ((local[0] * shape[1] + local[1]) * shape[2] + local[2]) * shape[3] + local[3];
                chunk[at] = held.values[element];
            }
        }
        self.array
            .store_chunk_opt(&indices, chunk.as_slice(), options)
            .map_err(storage)
    }
}
