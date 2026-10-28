// SPDX-License-Identifier: LGPL-3.0-or-later
//! The batch route's memory: how many times each partition's batches hold,
//! sized as the selected-observation route sizes its blocks
//! ([`casa_imaging_runtime::finalize_source_access`]), and the charge of
//! everything the source and the passes' blocks hold, admitted before the
//! run opens the source.

use casa_imaging_model::{CompiledProblem, GeometryPlan};
use casa_imaging_runtime::pass::{BlockShape, RowSamples, chunk_bytes_per_row, source_block_bytes};
use casa_imaging_runtime::{
    Admitted, Demand, HostResources, ResourcePolicy, admit, bootstrap_source_budget, free_memory,
    row_layout,
};
use casa_observation::{
    AccessRequest, CorrelationRequest, Deliver, FlagsFrom, Grouping, Observation, Roles,
    TimeSelection, WeightRequest,
};

use super::pack::PackScratch;
use super::selection::PartitionSelection;
use crate::imaging::ImagingError;

/// The bytes one batch's pointing query may hold
/// ([`PointingRequest::budget_bytes`](casa_observation::PointingRequest::budget_bytes)):
/// a batch's antennas over its times. A query beyond it is refused, not
/// held. Admission charges it twice when the geometry reads pointing: the
/// store's catalogue of the query and the series it gives can be live
/// together.
pub(super) const POINTING_BUDGET: usize = 64 << 20;

/// An upper bound of the bytes the survey's conversion table holds per
/// (time, field) it converts at: its key, its factor and the table's share.
const SURVEY_ENTRY_BYTES: usize = 64;

/// How the batch route's traversals divide each partition: whole times
/// per batch, and the most samples one batch holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Batching {
    times_per_batch: Vec<usize>,
    rows: usize,
    buffer_bytes: usize,
}

impl Batching {
    /// Whole times each batch of selected partition `index` holds.
    pub(super) fn times_per_batch(&self, index: usize) -> usize {
        self.times_per_batch[index]
    }

    /// The most samples (padding included) one batch holds: the passes'
    /// block rows.
    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    /// The most bytes one batch and its decode scratch hold: what the
    /// channel-window survey's batches, which read no payload, may hold in
    /// the same buffers.
    pub(super) fn buffer_bytes(&self) -> usize {
        self.buffer_bytes
    }
}

/// One selected partition's traversal of its whole selection, from its
/// estimate.
struct PartitionCost {
    /// Bytes one time holds in a batch and the cursor's scratch.
    held_per_time: usize,
    baselines: usize,
    /// Times the traversal delivers.
    times: usize,
}

/// The batches of `selection` for `problem`, and the admitted charge of
/// what the source holds for them beside the passes' blocks.
///
/// As the selected-observation route does for its rows, the batches hold
/// at least one time of each partition, and grow beyond that by at most
/// the bootstrap budget ([`bootstrap_source_budget`]) or the source's share
/// of a quarter of what `policy` leaves free on `host`, shared with the
/// passes' per-row storage in proportion to their bytes per sample. A row
/// of [`row_layout`] produces at most `row` samples. The charge holds the
/// largest batch's buffers, its geometry, the plan's parallactic cache at
/// every selected (time, field), a batch's pointing when the geometry
/// reads it, the channel-window survey's conversions (its batches share
/// the source's buffers), the store's index (`index_bytes`, which only the caller
/// knows) and the passes' blocks; the run keeps it while the source is
/// open.
///
/// # Errors
///
/// When an estimate fails, or the charge does not fit the policy.
pub(crate) fn admit_batches(
    (problem, observation, selection, plan): (
        &CompiledProblem,
        &dyn Observation,
        &PartitionSelection,
        &GeometryPlan,
    ),
    row: RowSamples,
    index_bytes: usize,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<Batching>, ImagingError> {
    let costs = selection
        .partitions
        .iter()
        .map(|partition| {
            let entry = observation.catalogue().partition(partition.id);
            let first = partition.channels[0] as usize;
            let last = *partition.channels.last().expect("a selected channel") as usize;
            let request = AccessRequest {
                partitions: vec![partition.id],
                data_group: selection.data_group.clone(),
                roles: Roles {
                    time_centroid: plan.reads_time_centroids(),
                    ..Roles::PAYLOAD
                },
                weights: WeightRequest::Stored,
                channels: first..last + 1,
                times: TimeSelection::All,
                fields: selection.fields.clone(),
                correlations: CorrelationRequest {
                    deliver: if partition.correlations == entry.correlations {
                        Deliver::All
                    } else {
                        Deliver::These(partition.correlations.clone())
                    },
                    flags_from: FlagsFrom::Delivered,
                },
                grouping: Grouping::None,
                batch_bytes: usize::MAX,
            };
            let estimate = observation
                .estimate(&request)
                .map_err(|error| ImagingError::Observation(Box::new(error)))?;
            let estimate = estimate
                .partitions
                .first()
                .ok_or(ImagingError::Unsupported {
                    reason: "an estimate without its partition",
                })?;
            Ok(PartitionCost {
                held_per_time: estimate.held_bytes_per_time(),
                baselines: estimate.baselines.max(1),
                times: estimate.times,
            })
        })
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let layout = row_layout(problem);
    // Per sample: the batch's share and the cursor's scratch, its
    // geometry, and the flag that keeps it.
    let source_per_row = costs
        .iter()
        .map(|cost| cost.held_per_time.div_ceil(cost.baselines))
        .max()
        .unwrap_or(0)
        + plan.geometry_bytes(1, 0, 0)
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
    let growth = bootstrap_source_budget()
        .available_bytes()
        .min(usize::try_from(share).unwrap_or(usize::MAX));
    let target = (minimum_rows * source_per_row + growth) / source_per_row.max(1);
    let times_per_batch = costs
        .iter()
        .map(|cost| (target / cost.baselines).max(1))
        .collect::<Vec<_>>();
    let rows = costs
        .iter()
        .zip(&times_per_batch)
        .map(|(cost, times)| times * cost.baselines)
        .max()
        .unwrap_or(0);
    let longest = times_per_batch.iter().copied().max().unwrap_or(0);
    let buffers = costs
        .iter()
        .zip(&times_per_batch)
        .map(|(cost, times)| times * cost.held_per_time)
        .max()
        .unwrap_or(0);
    let keys = costs.iter().map(|cost| cost.times).sum::<usize>();
    let source = buffers
        + plan.geometry_bytes(rows, longest, layout.channels())
        + rows
        + plan.cache_bound(keys)
        + if plan.reads_pointing() {
            2 * POINTING_BUDGET
        } else {
            0
        }
        + index_bytes
        + keys * SURVEY_ENTRY_BYTES
        + PackScratch::bound(layout);
    tracing::info!(
        rows_per_block = rows,
        source_bytes = source,
        pass_bytes = passes(rows),
        index_bytes,
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
            buffer_bytes: buffers,
        },
        reservation,
    })
}
