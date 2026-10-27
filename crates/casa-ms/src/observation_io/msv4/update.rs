// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing back to an MSv4 processing set: each target variable's chunks
//! are replaced whole through the [`ReplacingStore`], from a held window of
//! one chunk-row of times ([`Window`]).
//!
//! Batches may come in any order; each moves the window, and a window left
//! for another row is written then. Every chunk the updater writes replaces
//! the stored object atomically: a failure leaves chunks already replaced
//! replaced and the others as they were. Each updater replaces through a
//! store of its own, so its reported reads and writes are that store's
//! counts of chunk objects, and the directories it synchronizes when it
//! finishes are those its own renames changed.

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use casa_observation::{
    AccessRequest, ObservationError, PartitionEntry, PartitionId, PatchValues, ResolvedTarget,
    TimeRuns, UpdateReport, Updater, VisibilityBatch, WeightChannels, WritableRole, check_values,
};
use num_complex::Complex32;
use zarrs::array::CodecOptions;

use super::Msv4Observation;
use super::metadata::storage;
use super::store::ReplacingStore;
use super::window::Window;
use crate::observation_io::OpenOptions;
use crate::observation_io::update::{Claim, Unit, request_error};

/// An MSv4 opened for update: the observation it reads and the
/// processing set its updaters replace chunks in, each through a
/// [`ReplacingStore`] of its own.
pub(in crate::observation_io) struct Msv4Update {
    observation: Msv4Observation,
    root: PathBuf,
    /// The failure the next updater's store injects (failure laws).
    #[cfg(test)]
    failure: std::sync::Mutex<Option<(usize, super::store::FailurePoint)>>,
}

/// One bound target's window in one partition.
enum Target {
    Visibilities(Window<Complex32>),
    Flags(Window<bool>),
    Weights(Window<f32>),
}

/// One requested partition: its dataset times by bit pattern and its
/// targets' windows.
struct Partition {
    id: PartitionId,
    times: HashMap<u64, usize>,
    /// The dataset times the traversal selects.
    selected: TimeRuns,
    targets: Vec<Target>,
}

struct Msv4Updater<'s> {
    update: &'s Msv4Update,
    _claim: Claim<'s>,
    /// This updater's store: its counts and the directories it changed.
    store: Arc<ReplacingStore>,
    roles: Vec<WritableRole>,
    partitions: Vec<Partition>,
    /// The partition whose windows hold values.
    open: Option<usize>,
    /// The traversal's channels, which every batch delivers.
    channels: Range<usize>,
    options: CodecOptions,
    report: UpdateReport,
}

impl Msv4Update {
    pub(in crate::observation_io) fn open(
        path: &Path,
        options: &OpenOptions,
    ) -> Result<Self, ObservationError> {
        Ok(Self {
            observation: Msv4Observation::open_with(path, options)?,
            root: path.to_path_buf(),
            #[cfg(test)]
            failure: std::sync::Mutex::new(None),
        })
    }

    pub(in crate::observation_io) fn observation(&self) -> &Msv4Observation {
        &self.observation
    }

    /// Fail the replacement of the next updater's store that follows
    /// `replacements` completed ones, at `point` (failure laws).
    #[cfg(test)]
    pub(in crate::observation_io) fn fail_after(
        &self,
        replacements: usize,
        point: super::store::FailurePoint,
    ) {
        *self.failure.lock().expect("failure hook") = Some((replacements, point));
    }

    /// Open the windows of `targets` in each partition `request` reads,
    /// and claim the chunk rows of times they may replace: the selected
    /// times widened to whole chunk rows of each variable.
    pub(in crate::observation_io) fn updater<'s>(
        &'s self,
        request: &AccessRequest,
        targets: &[&ResolvedTarget],
        claim: impl FnOnce(Vec<Unit>) -> Result<Claim<'s>, ObservationError>,
    ) -> Result<Box<dyn Updater + 's>, ObservationError> {
        let estimate = self.observation.estimate(request)?;
        let store = Arc::new(ReplacingStore::new(&self.root).map_err(storage)?);
        #[cfg(test)]
        if let Some((replacements, point)) = self.failure.lock().expect("failure hook").take() {
            store.fail_after(replacements, point);
        }
        let mut partitions = Vec::with_capacity(request.partitions.len());
        for (id, selection) in request.partitions.iter().zip(&estimate.partitions) {
            let (dataset, _) = self.observation.dataset(*id)?;
            let mut windows = Vec::with_capacity(targets.len());
            for target in targets {
                let group = dataset
                    .groups
                    .iter()
                    .find(|group| group.record.name == target.target.data_group)
                    .ok_or_else(|| {
                        request_error(format!(
                            "{id:?} has no data group {}",
                            target.target.data_group
                        ))
                    })?;
                let record = &group.record;
                let path = |variable: &str| format!("{}/{variable}", dataset.path);
                let channels = request.channels.clone();
                windows.push(match target.target.role {
                    WritableRole::Visibilities => Target::Visibilities(Window::open(
                        &store,
                        &path(&record.correlated_data),
                        channels,
                    )?),
                    WritableRole::Flags => {
                        Target::Flags(Window::open(&store, &path(&record.flag), channels)?)
                    }
                    // A weight patch may change every channel
                    // (`WeightChannels::All`).
                    WritableRole::Weights => Target::Weights(Window::open(
                        &store,
                        &path(&record.weight),
                        0..catalogue_channels(self, *id),
                    )?),
                });
            }
            partitions.push(Partition {
                id: *id,
                times: dataset
                    .times
                    .iter()
                    .enumerate()
                    .map(|(index, time)| (time.to_bits(), index))
                    .collect(),
                selected: selection.time_runs.clone(),
                targets: windows,
            });
        }
        let units = partitions
            .iter()
            .flat_map(|partition| {
                partition
                    .targets
                    .iter()
                    .zip(targets)
                    .flat_map(|(window, target)| {
                        partition.selected.runs().iter().map(|run| Unit {
                            partition: partition.id,
                            variable: target.variable.clone(),
                            times: window.chunk_rows(run.clone()),
                        })
                    })
            })
            .collect();
        let claim = claim(units)?;
        Ok(Box::new(Msv4Updater {
            update: self,
            _claim: claim,
            store,
            roles: targets.iter().map(|target| target.target.role).collect(),
            partitions,
            open: None,
            channels: request.channels.clone(),
            options: super::codec_options(),
            report: UpdateReport::default(),
        }))
    }
}

impl Target {
    /// The grid times `times` widened to whole chunk rows of the variable.
    fn chunk_rows(&self, times: Range<usize>) -> Range<usize> {
        match self {
            Self::Visibilities(window) => window.chunk_rows(times),
            Self::Flags(window) => window.chunk_rows(times),
            Self::Weights(window) => window.chunk_rows(times),
        }
    }
}

impl Msv4Updater<'_> {
    /// Write what every window of the open partition holds.
    fn flush(&mut self) -> Result<(), ObservationError> {
        if let Some(open) = self.open.take() {
            for target in &mut self.partitions[open].targets {
                match target {
                    Target::Visibilities(window) => window.flush(&self.options)?,
                    Target::Flags(window) => window.flush(&self.options)?,
                    Target::Weights(window) => window.flush(&self.options)?,
                }
            }
        }
        Ok(())
    }

    /// The most bytes the windows hold at once: one partition's.
    fn held_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| {
                partition
                    .targets
                    .iter()
                    .map(|target| match target {
                        Target::Visibilities(window) => window.held_bytes(),
                        Target::Flags(window) => window.held_bytes(),
                        Target::Weights(window) => window.held_bytes(),
                    })
                    .sum::<usize>()
            })
            .max()
            .unwrap_or(0)
    }
}

impl Updater for Msv4Updater<'_> {
    fn write(
        &mut self,
        batch: &VisibilityBatch<'_>,
        values: &[PatchValues<'_>],
    ) -> Result<(), ObservationError> {
        check_values(batch, &self.roles, values)?;
        if !Arc::ptr_eq(batch.catalogue, self.update.observation.catalogue()) {
            return Err(request_error(
                "the batch was not delivered by this session's observation",
            ));
        }
        // Every window holds the traversal's channels; a batch of others
        // would be refused by one target after another had taken it.
        if batch.channels() != self.channels {
            return Err(request_error(
                "the batch's channels are not the traversal's the updater was bound to",
            ));
        }
        let index = self
            .partitions
            .iter()
            .position(|partition| partition.id == batch.partition)
            .ok_or_else(|| request_error("the batch's partition is not the traversal's"))?;
        let entry = batch.catalogue.partition(batch.partition);
        let times = dataset_times(batch, entry, &self.partitions[index].times)?;
        if !self.partitions[index].selected.holds(times.clone()) {
            return Err(request_error(
                "the batch's times are outside the traversal's time and field selection",
            ));
        }
        let products = product_indices(batch, entry)?;
        for value in values {
            refuse_padding_changes(batch, value)?;
        }
        if self.open != Some(index) {
            self.flush()?;
            self.open = Some(index);
        }
        let channels = batch.channels();
        let all_channels = 0..entry.frequencies_hz.len();
        for (target, value) in self.partitions[index].targets.iter_mut().zip(values) {
            assign(
                target,
                value,
                times.clone(),
                (channels.clone(), all_channels.clone()),
                &products,
                &self.options,
            )?;
        }
        self.report.batches += 1;
        self.report.samples += batch.samples();
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<UpdateReport, ObservationError> {
        self.flush()?;
        self.store.sync().map_err(storage)?;
        let counters = &self.store.counters;
        let mut report = self.report;
        report.units_read = counters.chunk_reads.load(Ordering::Relaxed);
        report.units_written =
            counters.replaced.load(Ordering::Relaxed) + counters.erased.load(Ordering::Relaxed);
        report.held_bytes = self.held_bytes();
        Ok(report)
    }
}

/// Partition `id`'s channel count.
fn catalogue_channels(update: &Msv4Update, id: PartitionId) -> usize {
    update
        .observation
        .catalogue()
        .partition(id)
        .frequencies_hz
        .len()
}

/// Assign one target's values into its window.
fn assign(
    target: &mut Target,
    value: &PatchValues<'_>,
    times: Range<usize>,
    (channels, all_channels): (Range<usize>, Range<usize>),
    products: &[usize],
    options: &CodecOptions,
) -> Result<(), ObservationError> {
    match (target, value) {
        (Target::Visibilities(window), PatchValues::Visibilities(values)) => {
            window.assign(times, channels, products, options, |s, c, p| {
                Some(values[[s, c, p]])
            })
        }
        (Target::Flags(window), PatchValues::Flags { set, clear }) => {
            window.assign(times, channels, products, options, |s, c, p| {
                if set.as_ref().is_some_and(|set| set[[s, c, p]]) {
                    Some(true)
                } else if clear.as_ref().is_some_and(|clear| clear[[s, c, p]]) {
                    Some(false)
                } else {
                    None
                }
            })
        }
        (
            Target::Weights(window),
            PatchValues::Weights {
                values,
                channels: which,
            },
        ) => match which {
            WeightChannels::Delivered => {
                window.assign(times, channels, products, options, |s, c, p| {
                    Some(values[[s, c, p]])
                })
            }
            WeightChannels::All => {
                window.assign(times, all_channels, products, options, |s, _, p| {
                    Some(values[[s, 0, p]])
                })
            }
        },
        _ => Err(request_error("a patch value's role is not its target's")),
    }
}

/// The dataset times of `batch`'s times, which must be contiguous.
fn dataset_times(
    batch: &VisibilityBatch<'_>,
    entry: &PartitionEntry,
    known: &HashMap<u64, usize>,
) -> Result<Range<usize>, ObservationError> {
    let index = |t: usize| {
        known
            .get(&batch.time.seconds_since(t, entry.time_origin).to_bits())
            .copied()
            .ok_or_else(|| request_error("a batch time is not one of the dataset's"))
    };
    let first = index(0)?;
    for t in 1..batch.times() {
        if index(t)? != first + t {
            return Err(request_error(
                "the batch's times are not contiguous in the dataset",
            ));
        }
    }
    Ok(first..first + batch.times())
}

/// The delivered products' positions among the partition's.
fn product_indices(
    batch: &VisibilityBatch<'_>,
    entry: &PartitionEntry,
) -> Result<Vec<usize>, ObservationError> {
    batch
        .correlations
        .iter()
        .map(|product| {
            entry
                .correlations
                .iter()
                .position(|known| known == product)
                .ok_or_else(|| request_error("a delivered product is not the partition's"))
        })
        .collect()
}

/// Refuse a change that would make padding read as an acquisition: a
/// cleared flag, or a finite weight, where the store holds padding (a
/// non-finite weight: XRADIO and casa-rs's writer pad with NaN weights).
/// Either change needs the traversal to read weights, which mark padding;
/// a non-finite weight of an acquisition is kept the same way.
fn refuse_padding_changes(
    batch: &VisibilityBatch<'_>,
    value: &PatchValues<'_>,
) -> Result<(), ObservationError> {
    let changes_padding: Box<dyn Fn(usize, usize, usize) -> bool + '_> = match value {
        PatchValues::Flags {
            clear: Some(clear), ..
        } if clear.iter().any(|cleared| *cleared) => Box::new(|s, c, p| clear[[s, c, p]]),
        PatchValues::Weights { values, channels } => {
            let all = *channels == WeightChannels::All;
            Box::new(move |s, c, p| values[[s, if all { 0 } else { c }, p]].is_finite())
        }
        _ => return Ok(()),
    };
    let Some(weights) = batch.group.weights else {
        return Err(request_error(
            "clearing MSv4 flags or writing MSv4 weights needs the traversal to read weights, \
             which mark padding",
        ));
    };
    if weights
        .indexed_iter()
        .any(|((s, c, p), weight)| !weight.is_finite() && changes_padding(s, c, p))
    {
        return Err(request_error(
            "a flag cleared or a finite weight written where the store holds padding (a \
             non-finite weight)",
        ));
    }
    Ok(())
}
