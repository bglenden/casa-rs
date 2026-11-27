// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded observation batches, projected pointings and Stokes I spectra.
use super::{TotalPowerError, TotalPowerRequest, failure, pointing};
use crate::RunContext;
use casa_core::measures::MeasuresProvider;
use casa_imaging_runtime::{
    Demand, Reservation, admit,
    pass::{WORKER_STACK_BYTES, WorkerTeam},
};
use casa_imaging_singledish::{Pointing, SpectrumBlock, TotalPowerGrid, weights_from_spectra};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, FieldSelection, Grouping, Observation,
    PartitionId, ReadEvent, Roles, TimeSelection, WeightRequest,
};
use ndarray::Array3;
use std::sync::Arc;

const BATCH_BYTES: usize = 4 << 20;
const SAMPLING_BYTES: usize = 8 << 20;

pub(super) fn grid(
    observation: &dyn Observation,
    request: &TotalPowerRequest,
    context: &RunContext,
    partitions: &[PartitionId],
    measures: &Arc<dyn MeasuresProvider>,
    grid: &mut TotalPowerGrid,
) -> Result<
    (
        Vec<Pointing>,
        Reservation,
        Option<casa_core::measures::epoch::MEpoch>,
    ),
    TotalPowerError,
> {
    let workers = context
        .policy
        .workers(&context.host)
        .min(request.channels.len());
    // Two traversal buffers, real values, flags, weights and row/pixel maps,
    // one bounded POINTING window, its projected copy, and retained sampling.
    let mut memory = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power streaming and worker team",
            memory: (8 * BATCH_BYTES
                + 3 * SAMPLING_BYTES
                + if workers > 1 {
                    workers * (WORKER_STACK_BYTES + 65536)
                } else {
                    0
                }) as u64,
        },
    )
    .map_err(failure)?;
    let team = WorkerTeam::new(workers).map_err(failure)?;
    let mut sampling = Vec::new();
    let mut date = None;
    for &partition in partitions {
        let current = observation.catalogue().partition(partition);
        let group = ["corrected", "float", "base"]
            .into_iter()
            .find_map(|name| current.data_group(name).map(|(_, group)| group))
            .ok_or_else(|| failure("no total-power data group"))?;
        let access = AccessRequest {
            partitions: vec![partition],
            data_group: group.name.clone(),
            roles: Roles {
                values: true,
                flags: true,
                weights: true,
                uvw: false,
                time_centroid: false,
                interval: true,
                exposure: false,
                scan: false,
            },
            weights: WeightRequest::Stored,
            channels: request.channels.clone(),
            times: TimeSelection::All,
            fields: FieldSelection::All,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: BATCH_BYTES,
        };
        let mut traversal = observation.bind_read(&access).map_err(failure)?;
        let mut buffers = BatchBuffers::default();
        while let Some(event) = traversal.next(&mut buffers).map_err(failure)? {
            if context.cancel.is_cancelled() {
                return Err(failure("cancelled while gridding"));
            }
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            if date.is_none() {
                date = Some(batch.time.get(0));
            }
            let (pixels, pointings) = pointing::pixels(
                observation,
                &batch,
                &request.direction,
                measures,
                &request.antennas,
            )?;
            let needed = sampling.len() + pointings.len();
            if needed > SAMPLING_BYTES / size_of::<Pointing>() {
                return Err(failure("raster sampling exceeds its admitted memory"));
            }
            sampling.reserve_exact(needed.saturating_sub(sampling.len()));
            sampling.extend(pointings);
            let values = batch
                .group
                .values
                .ok_or_else(|| failure("missing values"))?
                .mapv(|value| value.re);
            let mut flags = Array3::from_elem(values.dim(), false);
            for ((sample, channel, correlation), flag) in flags.indexed_iter_mut() {
                *flag = batch.validity().excluded(sample, channel, correlation);
            }
            let weights = weights_from_spectra(
                batch
                    .group
                    .weights
                    .ok_or_else(|| failure("missing weights"))?,
            )
            .map_err(failure)?;
            let row_flags: Vec<_> = (0..batch.samples())
                .map(|row| !batch.sample(row).baseline.is_auto())
                .collect();
            let channel_map: Vec<_> = (0..request.channels.len()).map(Some).collect();
            let correlation_map = vec![Some(0); batch.correlations.len()];
            grid.grid(
                &SpectrumBlock {
                    pixels: &pixels,
                    values: values.view(),
                    flags: flags.view(),
                    row_flags: &row_flags,
                    weights: weights.view(),
                    channel_map: &channel_map,
                    correlation_map: &correlation_map,
                },
                &team,
            )
            .map_err(failure)?;
        }
    }
    // The outcome keeps its coordinate records and selected-partition metadata.
    memory.retain((sampling.capacity() * size_of::<Pointing>()) as u64);
    Ok((sampling, memory, date))
}
