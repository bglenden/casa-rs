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
//! Each write leaves what it does not image: padding, samples the UV
//! selection excludes, the channels of a batch's span the selection leaves
//! out, the fit-only channels of a continuum transform, and the products
//! not routed. Flagged samples are written, as CASA writes them.
//!
//! Which of a time's channels are written follows CASA's final write. Its
//! last major cycle writes on the tuned selection of
//! `CubeMajorCycleAlgorithm`'s imager, whose iterator leaves a channel the
//! time's frequency bands miss out of the buffer it writes back
//! (`VisibilityIteratorImpl2::putColumnRows`), so that channel keeps its
//! contents. `SynthesisImagerVi2::predictModel`, which overwrites it where
//! tclean goes on to call it, reads the parent imager's selection, which is
//! not tuned: its per-time bands select the parent channels, including those
//! that do not reach the cube. The source keeps these predictions distinct
//! from the channels and flags used for gridding ([`WriteBack::by_band`]).

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use casa_imaging_model::SequentialContinuumTransform;
use casa_imaging_runtime::pass::{NativeBlock, STREAM_SLOTS, SourceError};
use casa_ms::observation_io::UpdateSession;
use casa_observation::{
    AccessRequest, BatchAddress, Correlation, Deliver, ObservationError, PatchValues, UpdateTarget,
    Updater, WritableRole,
};
use casa_types::measures::MjdHighPrec;
use ndarray::ArrayView3;
use num_complex::Complex32;

use super::BatchRoute;
use crate::compile::OpenedStore;
use crate::imaging::visibility_write::writes_channel;

/// What a writing traversal queues for its sink, in delivery order.
#[derive(Debug)]
pub(super) enum Delivered {
    /// A partition bound: its store, and the request the sink binds that
    /// store's updater to.
    Partition {
        /// The store's place in the route.
        store: usize,
        /// The partition's request.
        request: AccessRequest,
    },
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
    /// Whether the window's frequency bands select each of the block's
    /// channels at each of the batch's times, `[time][channel]`; empty when
    /// every channel is written.
    pub(super) in_band: Vec<bool>,
    /// The spectral window key its rows carry ([`super::StoreKeys`]).
    pub(super) spectral_window: u32,
}

/// The provenance queue, and whether a sink waits for it.
#[derive(Debug, Default)]
struct Queue {
    armed: bool,
    parent_prediction: bool,
    entries: VecDeque<Delivered>,
}

/// What a writing run's stores share: the columns each store's update
/// session writes, and the queue between the source and its sink.
pub(crate) struct WriteBack {
    /// Whether the sessions' targets are the model's (first) and the
    /// corrected data's.
    model: bool,
    corrected: bool,
    /// Whether a time's frequency bands limit the channels written.
    by_band: bool,
    queue: Mutex<Queue>,
}

impl WriteBack {
    /// The write-back of a run writing the model when `model` and the
    /// corrected data when `corrected`, limited at each time to the
    /// channels its frequency bands select when `by_band`.
    pub(crate) fn new(model: bool, corrected: bool, by_band: bool) -> Self {
        Self {
            model,
            corrected,
            by_band,
            queue: Mutex::default(),
        }
    }

    /// An update session over `observation`, one store of the run, writing
    /// this write-back's columns. A MeasurementSet's `MODEL_DATA` MAIN lacks
    /// is created with CASA's initial model; an MSv4 cannot add a data
    /// group, so its `model` group must exist. `CORRECTED_DATA` is written
    /// only where it exists.
    ///
    /// # Errors
    ///
    /// When the session cannot open over the observation or a target
    /// cannot be resolved ([`UpdateSession::over_msv2`],
    /// [`UpdateSession::over_msv4`]).
    pub(crate) fn session(
        &self,
        observation: OpenedStore,
    ) -> Result<UpdateSession, ObservationError> {
        let mut targets = Vec::with_capacity(2);
        if self.model {
            targets.push(UpdateTarget {
                create: true,
                ..UpdateTarget::new("model", WritableRole::Visibilities)
            });
        }
        if self.corrected {
            targets.push(UpdateTarget::new("corrected", WritableRole::Visibilities));
        }
        match observation {
            OpenedStore::V2(observation) => UpdateSession::over_msv2(*observation, &targets),
            OpenedStore::V4(observation) => UpdateSession::over_msv4(observation, &targets),
        }
    }

    /// Whether a time's frequency bands limit the channels written.
    pub(super) const fn by_band(&self) -> bool {
        self.by_band
    }

    /// The sessions' targets, every one of which each write gives values
    /// for.
    fn targets(&self, model: bool, corrected: bool) -> Vec<usize> {
        let mut targets = Vec::with_capacity(2);
        if model {
            targets.push(0);
        }
        if corrected {
            targets.push(usize::from(self.model));
        }
        targets
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

    /// Whether the armed traversal reads the untuned parent selection.
    pub(super) fn parent_prediction(&self) -> bool {
        self.queue().parent_prediction
    }

    /// Whether this write needs CASA's separate final parent prediction.
    pub(crate) const fn predicts_final_model(&self) -> bool {
        self.model && !self.by_band
    }

    /// Whether the route writes continuum-corrected data.
    pub(crate) const fn writes_corrected(&self) -> bool {
        self.corrected
    }

    /// Queue `entry` for the sink.
    pub(super) fn deliver(&self, entry: Delivered) {
        self.queue().entries.push_back(entry);
    }

    /// What one partition's write holds beyond the passes', for its
    /// traversal `request` of its store's `session` ([`WriteCharge`]).
    /// `bands` says whether a time reads only some of its channels.
    pub(super) fn write_bytes(
        &self,
        session: &UpdateSession,
        request: &AccessRequest,
        bands: bool,
    ) -> Result<WriteCharge, SourceError> {
        let targets = self.targets(self.model, self.corrected);
        let updater = session.write_bytes(request, &targets)?;
        let estimate = session.observation().estimate(request)?;
        let partition = estimate
            .partitions
            .first()
            .ok_or("an estimate without its partition")?;
        let samples = partition.samples_per_batch();
        let entry = session
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
        // batch's times and products and which channels each time reads.
        let in_band = if bands {
            partition.times_per_batch * request.channels.len() * size_of::<bool>()
        } else {
            0
        };
        let provenance = (STREAM_SLOTS + 1)
            * (samples * size_of::<u32>()
                + size_of::<Delivered>()
                + partition.times_per_batch * size_of::<f64>()
                + products * size_of::<Correlation>()
                + in_band);
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
/// holds, through its store's update session.
pub(crate) struct BatchWriter<'a> {
    route: &'a BatchRoute,
    write_back: &'a WriteBack,
    transform: Option<&'a SequentialContinuumTransform>,
    model_target: bool,
    corrected_target: bool,
    updater: Option<Box<dyn Updater + 'a>>,
    /// The patch's mask and each column's values, reused from batch to
    /// batch.
    written: Vec<bool>,
    model: Vec<Complex32>,
    corrected: Vec<Complex32>,
    samples: u64,
}

impl<'a> BatchWriter<'a> {
    /// A sink for the next visibility traversal of `route`, which the
    /// source then records the provenance of. `CORRECTED_DATA` holds the
    /// output of `transform`.
    pub(crate) fn begin(
        route: &'a BatchRoute,
        transform: Option<&'a SequentialContinuumTransform>,
    ) -> Result<Self, SourceError> {
        let write_back = route
            .write_back()
            .ok_or("the batch route was opened to read only")?;
        Self::begin_targets(
            route,
            transform,
            write_back.model,
            write_back.corrected,
            false,
        )
    }

    /// A single-column traversal, either the parent model prediction or
    /// the tuned continuum-corrected data.
    pub(crate) fn begin_targets(
        route: &'a BatchRoute,
        transform: Option<&'a SequentialContinuumTransform>,
        model: bool,
        corrected: bool,
        parent_prediction: bool,
    ) -> Result<Self, SourceError> {
        let write_back = route
            .write_back()
            .ok_or("the batch route was opened to read only")?;
        if (model && !write_back.model)
            || (corrected && !write_back.corrected)
            || (!model && !corrected)
        {
            return Err("the traversal's write targets were not opened".into());
        }
        if corrected && transform.is_none() {
            return Err("CORRECTED_DATA is written only by a continuum transform".into());
        }
        let mut queue = write_back.queue();
        queue.entries.clear();
        queue.armed = true;
        queue.parent_prediction = parent_prediction;
        drop(queue);
        Ok(Self {
            route,
            write_back,
            transform,
            model_target: model,
            corrected_target: corrected,
            updater: None,
            written: Vec::new(),
            model: Vec::new(),
            corrected: Vec::new(),
            samples: 0,
        })
    }

    /// Whether the write needs model predictions.
    pub(crate) const fn needs_predictions(&self) -> bool {
        self.model_target
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
            address,
            samples,
            in_band,
            ..
        } = provenance;
        let [batch_samples, channels, products] = address.shape();
        let first = address.channels().start;
        let entry = address.catalogue().partition(address.partition());
        let per_time = entry.baselines.len();
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
        let (model, corrected) = (self.model_target, self.corrected_target);
        reset(&mut self.written, len, true, false);
        reset(&mut self.model, len, model, Complex32::default());
        reset(&mut self.corrected, len, corrected, Complex32::default());
        let cells = block.channels() * block.correlations();
        let columns = u64::from(model) + u64::from(corrected);
        for (row, sample) in samples.iter().enumerate() {
            let context = block.header(row).context;
            // A continuum transform reads one MeasurementSet (compile
            // refuses several), whose keys are its own numbers.
            let rule = self.transform.and_then(|transform| {
                transform.rule(context.field as i32, context.spectral_window)
            });
            let values = block.row(0, row).values;
            for (k, channel) in channel_of.iter().enumerate() {
                let read = in_band.is_empty()
                    || in_band[*sample as usize / per_time * block.channels() + k];
                if !read || !writes_channel(rule, block.channel_indices()[k]) {
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
                Delivered::Partition { store, request } => self.bind(store, &request)?,
                Delivered::Block(provenance) => return Ok(provenance),
            }
        }
    }

    /// Bind the updater of the partition `request` of store `store` reads,
    /// then finish the last partition's: the two overlap, so an MSv2
    /// session keeps MAIN's lock from one of its partitions to the next.
    fn bind(&mut self, store: usize, request: &AccessRequest) -> Result<(), SourceError> {
        let session = self.route.stores[store]
            .session()
            .ok_or("a writing run's store was opened to read only")?;
        let updater = session.updater(
            request,
            &self
                .write_back
                .targets(self.model_target, self.corrected_target),
        )?;
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
            let Delivered::Partition { store, request } = entry else {
                return Err("a block's provenance was left unwritten".into());
            };
            self.bind(store, &request)?;
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
        in_band,
        spectral_window,
    } = provenance;
    let baselines = &address.catalogue().partition(address.partition()).baselines;
    let time = address.time();
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let matches = samples.len() == block.len()
        && (in_band.is_empty()
            || in_band.len() == address.samples() / baselines.len() * block.channels())
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
