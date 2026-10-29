// SPDX-License-Identifier: LGPL-3.0-or-later
//! The batch route's write-back (#932, step 5): the final model into the
//! `model` group (`MODEL_DATA`) and the continuum-subtracted data into the
//! `corrected` group (`CORRECTED_DATA`), through the update session the
//! route reads its batches from.
//!
//! The passes hand each block to their sink on the consumer thread, after
//! the source has refilled the batch's buffers, so the sink writes against
//! each batch's address ([`BatchAddress`]). While the writing traversal
//! runs, the source queues, in delivery order, each partition it binds
//! (its request, which the sink binds an updater to) and, for each block
//! it fills, its batch's address and the batch sample of each of its rows.
//! The runtime hands a writing pass's sink every block once, in source
//! order ([`casa_imaging_runtime::pass::VisibilitySink`]), so the sink
//! takes the queue's head and checks it against the block. It binds a
//! partition's updater before it finishes the last one's, so an MSv2
//! session holds MAIN's write lock from the first partition's updater (when
//! the first block reaches the sink, after creating and seeding a
//! `MODEL_DATA` it lacks) to the end of the pass. Today's route takes the
//! lock and creates the column when its writer begins, before the first
//! block is read.
//!
//! Each write leaves what today's route leaves: padding, samples the UV
//! selection excludes, the channels of a batch's span the selection leaves
//! out, the fit-only channels of a continuum transform, and the products
//! not routed. Flagged samples are written.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use casa_imaging_model::SequentialContinuumTransform;
use casa_imaging_runtime::pass::{NativeBlock, STREAM_SLOTS, SourceError};
use casa_ms::observation_io::{Msv2Observation, UpdateSession};
use casa_observation::{
    AccessRequest, BatchAddress, Correlation, Deliver, Observation, ObservationError, PatchValues,
    UpdateTarget, Updater, WritableRole,
};
use casa_types::measures::MjdHighPrec;
use ndarray::ArrayView3;
use num_complex::Complex32;

use crate::imaging::visibility_write::writes_channel;

/// What a writing traversal queues for its sink, in delivery order.
#[derive(Debug)]
pub(super) enum Delivered {
    /// A partition bound: the request the sink binds its updater to.
    Partition(AccessRequest),
    /// A block filled.
    Block(Provenance),
}

/// Where a block's rows come from.
#[derive(Debug)]
pub(super) struct Provenance {
    /// Its batch's address.
    pub(super) address: BatchAddress,
    /// The batch sample of each of its rows.
    pub(super) samples: Vec<u32>,
    /// The spectral window its rows carry.
    pub(super) spectral_window: u32,
}

/// The provenance queue, and whether a sink waits for it.
#[derive(Debug, Default)]
struct Queue {
    armed: bool,
    entries: VecDeque<Delivered>,
}

/// The update session a writing run reads and writes, the columns it
/// writes, and the queue between its source and its sink.
pub(crate) struct WriteBack {
    session: UpdateSession,
    /// Whether the session's targets are the model's (first) and the
    /// corrected data's.
    model: bool,
    corrected: bool,
    queue: Mutex<Queue>,
}

impl WriteBack {
    /// An update session over `observation`, writing the model when
    /// `model` and the corrected data when `corrected`. A `MODEL_DATA`
    /// MAIN lacks is created with CASA's initial model, as today's route
    /// creates it; `CORRECTED_DATA` is written only where it exists, as
    /// today's route requires it.
    ///
    /// # Errors
    ///
    /// When the session cannot open over the observation or a target
    /// cannot be resolved ([`UpdateSession::over_msv2`]).
    pub(crate) fn open(
        observation: Msv2Observation,
        model: bool,
        corrected: bool,
    ) -> Result<Self, ObservationError> {
        let mut targets = Vec::with_capacity(2);
        if model {
            targets.push(UpdateTarget {
                create: true,
                ..UpdateTarget::new("model", WritableRole::Visibilities)
            });
        }
        if corrected {
            targets.push(UpdateTarget::new("corrected", WritableRole::Visibilities));
        }
        Ok(Self {
            session: UpdateSession::over_msv2(observation, &targets)?,
            model,
            corrected,
            queue: Mutex::default(),
        })
    }

    /// The observation the session reads.
    pub(crate) fn observation(&self) -> &dyn Observation {
        self.session.observation()
    }

    /// The session's targets, every one of which each write gives values
    /// for.
    fn targets(&self) -> Vec<usize> {
        (0..usize::from(self.model) + usize::from(self.corrected)).collect()
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the visibility traversal beginning now writes back: a sink
    /// has begun and waits for it.
    ///
    /// # Errors
    ///
    /// When an earlier traversal's provenance was left unwritten.
    pub(super) fn writing(&self) -> Result<bool, SourceError> {
        let queue = self.queue();
        if !queue.entries.is_empty() {
            return Err(
                "a writing traversal began before the last one's blocks were written".into(),
            );
        }
        Ok(queue.armed)
    }

    /// Queue `entry` for the sink.
    pub(super) fn deliver(&self, entry: Delivered) {
        self.queue().entries.push_back(entry);
    }

    /// What one partition's write holds beyond the passes', for its
    /// traversal `request` ([`WriteCharge`]).
    pub(super) fn write_bytes(&self, request: &AccessRequest) -> Result<WriteCharge, SourceError> {
        let targets = self.targets();
        let updater = self.session.write_bytes(request, &targets)?;
        let estimate = self.observation().estimate(request)?;
        let partition = estimate
            .partitions
            .first()
            .ok_or("an estimate without its partition")?;
        let samples = partition.samples_per_batch();
        let entry = self
            .observation()
            .catalogue()
            .partition(partition.partition);
        let products = match &request.correlations.deliver {
            Deliver::These(products) => products.len(),
            _ => entry.correlations.len(),
        };
        let elements = samples * request.channels.len() * products;
        let patch = elements * (targets.len() * size_of::<Complex32>() + size_of::<bool>());
        // The blocks in flight and the one being written, each with its
        // batch's times and products.
        let provenance = (STREAM_SLOTS + 1)
            * (samples * size_of::<u32>()
                + size_of::<Delivered>()
                + partition.times_per_batch * size_of::<f64>()
                + products * size_of::<Correlation>());
        Ok(WriteCharge {
            updater: updater as u64,
            patch: patch as u64,
            provenance: provenance as u64,
        })
    }
}

/// What one partition's write holds beyond the passes'.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct WriteCharge {
    /// Its updater, writing its largest batch
    /// ([`UpdateSession::write_bytes`]).
    pub(super) updater: u64,
    /// The patch the sink assembles for its largest batch: each column's
    /// values and the mask over the batch's samples, channels and
    /// products. The sink's buffers keep the largest partition's
    /// capacity.
    pub(super) patch: u64,
    /// The provenance of the blocks in flight and the one being written.
    pub(super) provenance: u64,
}

impl WriteCharge {
    /// Each term's most over `self` and `other`: the sink's buffers keep
    /// one partition's capacity while another's updater writes.
    pub(super) fn most(self, other: Self) -> Self {
        Self {
            updater: self.updater.max(other.updater),
            patch: self.patch.max(other.patch),
            provenance: self.provenance.max(other.provenance),
        }
    }

    /// The bytes charged.
    pub(super) fn bytes(self) -> u64 {
        self.updater + self.patch + self.provenance
    }
}

/// The sink of a writing pass on the batch route: each block's values
/// written to its batch, at the samples, channels and products the block
/// holds.
pub(crate) struct BatchWriter<'a> {
    write_back: &'a WriteBack,
    transform: Option<&'a SequentialContinuumTransform>,
    updater: Option<Box<dyn Updater + 'a>>,
    /// The patch's mask and each column's values, reused from batch to
    /// batch.
    written: Vec<bool>,
    model: Vec<Complex32>,
    corrected: Vec<Complex32>,
    samples: u64,
}

impl<'a> BatchWriter<'a> {
    /// A sink for the next visibility traversal, which the source then
    /// records the provenance of. `CORRECTED_DATA` holds the output of
    /// `transform`.
    pub(crate) fn begin(
        write_back: &'a WriteBack,
        transform: Option<&'a SequentialContinuumTransform>,
    ) -> Result<Self, SourceError> {
        if write_back.corrected && transform.is_none() {
            return Err("CORRECTED_DATA is written only by a continuum transform".into());
        }
        let mut queue = write_back.queue();
        queue.entries.clear();
        queue.armed = true;
        drop(queue);
        Ok(Self {
            write_back,
            transform,
            updater: None,
            written: Vec::new(),
            model: Vec::new(),
            corrected: Vec::new(),
            samples: 0,
        })
    }

    /// Whether the write needs model predictions.
    pub(crate) const fn needs_predictions(&self) -> bool {
        self.write_back.model
    }

    /// Write `block`'s predictions, `[row][channel][correlation]`, to the
    /// model and its values to the corrected data, on the channels that
    /// reach the line output.
    pub(crate) fn write(
        &mut self,
        block: &NativeBlock,
        predictions: &[Complex32],
    ) -> Result<(), SourceError> {
        let provenance = self.next_block()?;
        check(block, &provenance)?;
        let Provenance {
            address, samples, ..
        } = provenance;
        let [batch_samples, channels, products] = address.shape();
        let first = address.channels().start;
        let entry = address.catalogue().partition(address.partition());
        let channel_of = block
            .channel_indices()
            .iter()
            .map(|channel| {
                (*channel as usize)
                    .checked_sub(first)
                    .filter(|channel| *channel < address.channels().len())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or("a block holds a channel its batch does not deliver")?;
        let product_of = block
            .correlation_indices()
            .iter()
            .map(|index| {
                address
                    .correlations()
                    .iter()
                    .position(|product| *product == entry.correlations[*index as usize])
            })
            .collect::<Option<Vec<_>>>()
            .ok_or("a block holds a product its batch does not deliver")?;
        let len = batch_samples * channels * products;
        let (model, corrected) = (self.write_back.model, self.write_back.corrected);
        reset(&mut self.written, len, true, false);
        reset(&mut self.model, len, model, Complex32::default());
        reset(&mut self.corrected, len, corrected, Complex32::default());
        let cells = block.channels() * block.correlations();
        let columns = u64::from(model) + u64::from(corrected);
        for (row, sample) in samples.iter().enumerate() {
            let context = block.header(row).context;
            let rule = self.transform.and_then(|transform| {
                transform.rule(context.field as i32, context.spectral_window)
            });
            let values = block.row(0, row).values;
            for (k, channel) in channel_of.iter().enumerate() {
                if !writes_channel(rule, block.channel_indices()[k]) {
                    continue;
                }
                for (j, product) in product_of.iter().enumerate() {
                    let at = (*sample as usize * channels + channel) * products + product;
                    let cell = k * block.correlations() + j;
                    self.written[at] = true;
                    if model {
                        self.model[at] = predictions[row * cells + cell];
                    }
                    if corrected {
                        self.corrected[at] = values[cell];
                    }
                    self.samples += columns;
                }
            }
        }
        let shape = (batch_samples, channels, products);
        let written = ArrayView3::from_shape(shape, &self.written)?;
        let mut patch = Vec::with_capacity(2);
        for (writes, values) in [(model, &self.model), (corrected, &self.corrected)] {
            if writes {
                patch.push(PatchValues::Visibilities {
                    values: ArrayView3::from_shape(shape, values)?,
                    written: Some(written),
                });
            }
        }
        self.updater
            .as_mut()
            .ok_or("a block before its partition")?
            .write_at(&address, &patch)?;
        Ok(())
    }

    /// The next block's provenance, binding the updater of each partition
    /// queued before it.
    fn next_block(&mut self) -> Result<Provenance, SourceError> {
        loop {
            let next = self.write_back.queue().entries.pop_front();
            match next.ok_or("a block without its batch's provenance")? {
                Delivered::Partition(request) => self.bind(&request)?,
                Delivered::Block(provenance) => return Ok(provenance),
            }
        }
    }

    /// Bind the updater of the partition `request` reads, then finish the
    /// last partition's: the two overlap, so an MSv2 session keeps MAIN's
    /// lock.
    fn bind(&mut self, request: &AccessRequest) -> Result<(), SourceError> {
        let updater = self
            .write_back
            .session
            .updater(request, &self.write_back.targets())?;
        if let Some(last) = self.updater.replace(updater) {
            last.finish()?;
        }
        Ok(())
    }

    /// Finish the last partition's update; returns the cells written.
    pub(crate) fn complete(mut self) -> Result<u64, SourceError> {
        // Partitions bound after the last block hold nothing to write, but
        // their updaters prepare the columns as the others did.
        let rest: Vec<Delivered> = self.write_back.queue().entries.drain(..).collect();
        for entry in rest {
            let Delivered::Partition(request) = entry else {
                return Err("a block's provenance was left unwritten".into());
            };
            self.bind(&request)?;
        }
        if let Some(last) = self.updater.take() {
            last.finish()?;
        }
        Ok(self.samples)
    }
}

impl Drop for BatchWriter<'_> {
    fn drop(&mut self) {
        let mut queue = self.write_back.queue();
        queue.armed = false;
        queue.entries.clear();
    }
}

/// `buffer` as `len` elements of `fill`, or empty when not `used`; grown to
/// exactly `len` when it holds less, so its capacity is the largest batch's.
fn reset<T: Copy>(buffer: &mut Vec<T>, len: usize, used: bool, fill: T) {
    buffer.clear();
    if used {
        buffer.reserve_exact(len);
        buffer.resize(len, fill);
    }
}

/// Refuse a block whose provenance is not its own: as many rows as the
/// samples recorded, each in the recorded spectral window, at its sample's
/// time and on its baseline. The times are the batch's own (the source
/// takes a row's from the same epochs), so a block of another batch of the
/// partition, which shares its baselines, is refused too, as is a block of
/// another window's partition at the same times.
fn check(block: &NativeBlock, provenance: &Provenance) -> Result<(), SourceError> {
    let Provenance {
        address,
        samples,
        spectral_window,
    } = provenance;
    let baselines = &address.catalogue().partition(address.partition()).baselines;
    let time = address.time();
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let matches = samples.len() == block.len()
        && samples.iter().enumerate().all(|(row, sample)| {
            let sample = *sample as usize;
            let context = &block.header(row).context;
            sample < address.samples() && {
                let baseline = baselines[sample % baselines.len()];
                context.spectral_window == *spectral_window
                    && context.antennas == [baseline.antenna1.0, baseline.antenna2.0]
                    && context.time_s.to_bits()
                        == time
                            .seconds_since(sample / baselines.len(), mjd_zero)
                            .to_bits()
            }
        });
    if matches {
        Ok(())
    } else {
        Err("a block's rows are not the samples its source recorded for it".into())
    }
}
