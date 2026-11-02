// SPDX-License-Identifier: LGPL-3.0-or-later
//! The batch route's memory: how many times each partition's batches hold,
//! and the charge of everything the source and the passes' blocks hold,
//! admitted before the run opens the source.

use casa_imaging_model::{CompiledProblem, GeometryPlan};
use casa_imaging_runtime::pass::{
    BlockShape, OVERLAPPED_BLOCKS, RowSamples, chunk_bytes_per_row, source_block_bytes,
};
use casa_imaging_runtime::{
    Admitted, BOOTSTRAP_SOURCE_BYTES, Demand, HostResources, ResourcePolicy, admit, free_memory,
    row_layout,
};
use casa_observation::{AccessRequest, Observation, PartitionEstimate};

use super::pack::PackScratch;
use super::requests;
use super::selection::PartitionSelection;
use crate::imaging::ImagingError;

/// An upper bound of the bytes the survey's conversion table holds per
/// (time, field) it converts at: its key, its factor and the table's share.
const SURVEY_ENTRY_BYTES: usize = 64;

/// The bytes a survey batch holds beyond one time, when one time holds
/// less: the survey reads no payload, so its batches stay small.
const SURVEY_BATCH_BYTES: usize = 1 << 18;

/// How the batch route's traversals divide each partition: whole times
/// per batch, and the most samples one batch holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Batching {
    /// Per store, per selected partition.
    times_per_batch: Vec<Vec<usize>>,
    rows: usize,
    survey_bytes: usize,
}

impl Batching {
    /// Whole times each batch of selected partition `index` of store
    /// `store` holds.
    pub(super) fn times_per_batch(&self, store: usize, index: usize) -> usize {
        self.times_per_batch[store][index]
    }

    /// The most samples (padding included) one batch holds: the passes'
    /// block rows.
    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    /// The most bytes one batch of the payload-free survey and its decode
    /// scratch hold, in buffers of the survey's own: at least one time of
    /// every partition.
    pub(super) fn survey_bytes(&self) -> usize {
        self.survey_bytes
    }
}

/// One store whose batches are admitted: its observation, selection,
/// compiled geometry, and the bytes its pointing answers may hold.
pub(crate) struct StoreBatches<'a> {
    pub(crate) observation: &'a dyn Observation,
    pub(crate) selection: &'a PartitionSelection,
    pub(crate) plan: &'a GeometryPlan,
    pub(crate) pointing_budget: usize,
}

/// One selected partition's traversal of its whole selection, from its
/// estimate.
struct PartitionCost {
    /// Bytes one time holds in a batch and the cursor's scratch.
    held_per_time: usize,
    /// Bytes one time of its survey holds.
    survey_per_time: usize,
    baselines: usize,
    /// Native channels a batch delivers: its selected channels' span.
    channels: usize,
    /// Times the traversal delivers.
    times: usize,
}

/// `observation`'s estimate of `request`'s one partition.
fn estimate(
    observation: &dyn Observation,
    request: &AccessRequest,
) -> Result<PartitionEstimate, ImagingError> {
    observation
        .estimate(request)
        .map_err(|error| ImagingError::Observation(Box::new(error)))?
        .partitions
        .into_iter()
        .next()
        .ok_or(ImagingError::Unsupported {
            reason: "an estimate without its partition",
        })
}

/// The cost of each of `store`'s selected partitions.
fn partition_costs(store: &StoreBatches<'_>) -> Result<Vec<PartitionCost>, ImagingError> {
    let StoreBatches {
        observation,
        selection,
        plan,
        ..
    } = *store;
    selection
        .partitions
        .iter()
        .map(|partition| {
            let entry = observation.catalogue().partition(partition.id);
            let first = partition.channels[0] as usize;
            let last = *partition.channels.last().expect("a selected channel") as usize;
            let payload = estimate(
                observation,
                &requests::payload(
                    (selection, plan),
                    entry,
                    partition,
                    first..last + 1,
                    true,
                    usize::MAX,
                ),
            )?;
            let survey = estimate(
                observation,
                &requests::survey((selection, plan), entry, partition, usize::MAX),
            )?;
            Ok(PartitionCost {
                held_per_time: payload.held_bytes_per_time(),
                survey_per_time: survey.held_bytes_per_time(),
                baselines: payload.baselines.max(1),
                channels: last + 1 - first,
                times: payload.times,
            })
        })
        .collect()
}

/// The batches of every store of `stores` for `problem`, and the admitted
/// charge of what the source holds for them beside the passes' blocks.
///
/// The batches hold at least one time of each partition, and grow beyond
/// one time by at most the bootstrap budget ([`BOOTSTRAP_SOURCE_BYTES`])
/// or the source's share of a quarter of what `policy` leaves free on
/// `host`, shared with the passes' per-row storage in proportion to their
/// bytes per sample. They
/// hold at most one part in [`OVERLAPPED_BLOCKS`] of a partition's selected
/// times, so a pass streams in several batches and reads the next while it
/// grids one. A row of [`row_layout`] produces at most `row` samples. The
/// source reads one store at a time into the same buffers, so the charge
/// holds the largest batch's buffers and geometry over every store; each
/// store's plan keeps its parallactic cache at every selected (time, field)
/// and its selection its predicates, and the source keeps each store's
/// pointing answers within its `pointing_budget` bytes when its geometry
/// reads pointing; with the survey's buffers and conversions, and the
/// passes' blocks. The run keeps the charge while the source is open. What
/// opening the stores holds is the caller's to charge.
///
/// # Errors
///
/// When an estimate fails, or the charge does not fit the policy.
pub(crate) fn admit_batches(
    problem: &CompiledProblem,
    stores: &[StoreBatches<'_>],
    row: RowSamples,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<Batching>, ImagingError> {
    let store_costs = stores
        .iter()
        .map(partition_costs)
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let costs = store_costs.iter().flatten().collect::<Vec<_>>();
    let layout = row_layout(problem);
    // Per sample: the batch's share and the cursor's scratch, its
    // geometry, and the flag that keeps it.
    let geometry_per_row = stores
        .iter()
        .map(|store| store.plan.geometry_bytes(1, 0, 0))
        .max()
        .unwrap_or(0);
    let source_per_row = costs
        .iter()
        .map(|cost| cost.held_per_time.div_ceil(cost.baselines))
        .max()
        .unwrap_or(0)
        + geometry_per_row
        + 1;
    let passes = |rows: usize| source_block_bytes(BlockShape { rows, ..layout });
    let workers = policy.cores(host).streaming_workers;
    let passes_per_row = passes(1) - passes(0) + chunk_bytes_per_row(layout, row, workers);
    let minimum_rows = costs.iter().map(|cost| cost.baselines).max().unwrap_or(1);
    let minimum = minimum_rows as u64 * (source_per_row as u64 + passes_per_row);
    let free = free_memory(host, policy);
    let quarter = free.saturating_sub(minimum) / 4;
    let share = u128::from(source_per_row as u64) * u128::from(quarter)
        / u128::from((source_per_row as u64 + passes_per_row).max(1));
    let growth = BOOTSTRAP_SOURCE_BYTES.min(usize::try_from(share).unwrap_or(usize::MAX));
    let target = (minimum_rows * source_per_row + growth) / source_per_row.max(1);
    // A batch holds at most one part in OVERLAPPED_BLOCKS of its
    // partition's selected times, and at least one time.
    let times_per_batch = store_costs
        .iter()
        .map(|costs| {
            costs
                .iter()
                .map(|cost| {
                    (target / cost.baselines)
                        .min(cost.times.div_ceil(OVERLAPPED_BLOCKS))
                        .max(1)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let batches = costs
        .iter()
        .copied()
        .zip(times_per_batch.iter().flatten().copied())
        .collect::<Vec<_>>();
    let rows = batches
        .iter()
        .map(|(cost, times)| times * cost.baselines)
        .max()
        .unwrap_or(0);
    let longest = batches.iter().map(|(_, times)| *times).max().unwrap_or(0);
    let widest = costs.iter().map(|cost| cost.channels).max().unwrap_or(0);
    let buffers = batches
        .iter()
        .map(|(cost, times)| times * cost.held_per_time)
        .max()
        .unwrap_or(0);
    let survey_bytes = costs
        .iter()
        .map(|cost| cost.survey_per_time)
        .max()
        .unwrap_or(0)
        .max(SURVEY_BATCH_BYTES);
    let mut keys = 0;
    let mut kept = 0;
    let mut pointing = 0;
    for (store, costs) in stores.iter().zip(&store_costs) {
        let store_keys = costs.iter().map(|cost| cost.times).sum::<usize>();
        keys += store_keys;
        kept += store.plan.cache_bound(store_keys) + store.selection.predicate_bytes();
        if store.plan.reads_pointing() {
            pointing += store.pointing_budget;
        }
    }
    let geometry = stores
        .iter()
        .map(|store| store.plan.geometry_bytes(rows, longest, widest))
        .max()
        .unwrap_or(0);
    let source = buffers
        + geometry
        + rows
        + kept
        + pointing
        + survey_bytes
        + keys * SURVEY_ENTRY_BYTES
        + PackScratch::bound(layout);
    tracing::info!(
        rows_per_block = rows,
        source_bytes = source,
        pass_bytes = passes(rows),
        pointing_bytes = pointing,
        survey_bytes,
        "observation-batch source plan"
    );
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "observation-batch source",
            memory: source as u64 + passes(rows),
        },
    )?;
    Ok(Admitted {
        value: Batching {
            times_per_batch,
            rows,
            survey_bytes,
        },
        reservation,
    })
}
