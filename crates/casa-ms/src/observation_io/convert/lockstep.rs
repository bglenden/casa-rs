// SPDX-License-Identifier: LGPL-3.0-or-later
//! One partition read as one batch per selected data group for each range
//! of times: a traversal per group, each sized to the same whole times per
//! batch, the first with `first` roles and every other with its
//! visibilities only.

use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Grouping, Observation, ObservationError,
    PartitionId, ReadEvent, Roles, VisibilityBatch,
};

use crate::observation_io::update::request_error;

/// A further group's roles: its visibilities, beside the first group's
/// flags, weights, UVW and time auxiliaries.
const VISIBILITIES: Roles = Roles {
    values: true,
    flags: false,
    weights: false,
    uvw: false,
    time_centroid: false,
    interval: false,
    exposure: false,
    scan: false,
};

/// What a lockstep read delivered.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Read {
    /// Batches of each group.
    pub(super) batches: usize,
    /// Samples of each group.
    pub(super) samples: usize,
}

/// Read partition `id` of `observation`, every channel and product of
/// each of `groups`, handing `each` one batch per group for each range of
/// times, in time order. The batches hold whole times, `budget` bytes of
/// them for all groups together, and a multiple of `chunk` times where
/// that fits: the destination's chunk, or with `None` the source's natural
/// time block, so that no unit is decoded twice.
pub(super) fn read(
    observation: &dyn Observation,
    id: PartitionId,
    groups: &[String],
    first: Roles,
    (budget, chunk): (usize, Option<usize>),
    mut each: impl FnMut(&[VisibilityBatch<'_>]) -> Result<(), ObservationError>,
) -> Result<Read, ObservationError> {
    let channels = observation.catalogue().partition(id).frequencies_hz.len();
    let mut requests: Vec<AccessRequest> = groups
        .iter()
        .enumerate()
        .map(|(index, group)| AccessRequest {
            partitions: vec![id],
            data_group: group.clone(),
            roles: if index == 0 { first } else { VISIBILITIES },
            channels: 0..channels,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        })
        .collect();
    let mut per_time = Vec::with_capacity(requests.len());
    let (mut times, mut natural) = (0, 1);
    for request in &requests {
        let estimate = &observation.estimate(request)?.partitions[0];
        per_time.push(estimate.held_bytes_per_time());
        times = estimate.times;
        natural = estimate.natural_blocks.times;
    }
    let chunk = chunk.unwrap_or(natural).max(1);
    let times = times_per_batch(budget, per_time.iter().sum(), chunk, times);
    for (request, bytes) in requests.iter_mut().zip(&per_time) {
        request.batch_bytes = bytes * times;
    }
    let mut traversals = requests
        .iter()
        .map(|request| observation.bind_read(request))
        .collect::<Result<Vec<_>, _>>()?;
    let mut buffers: Vec<BatchBuffers> = groups.iter().map(|_| BatchBuffers::default()).collect();
    let mut read = Read::default();
    loop {
        let mut lockstep = Vec::with_capacity(groups.len());
        let mut ended = 0;
        for (traversal, buffer) in traversals.iter_mut().zip(buffers.iter_mut()) {
            match traversal.next(buffer)? {
                Some(ReadEvent::Batch(batch)) => lockstep.push(batch),
                Some(ReadEvent::GroupEnd(_)) => {
                    return Err(request_error(
                        "a conversion traversal delivered a group end",
                    ));
                }
                None => ended += 1,
            }
        }
        if ended == groups.len() {
            return Ok(read);
        }
        if ended > 0 {
            return Err(request_error("the data groups' traversals ended apart"));
        }
        read.batches += 1;
        read.samples += lockstep[0].samples();
        each(&lockstep)?;
    }
}

/// Whole times per batch for `budget` bytes of `bytes_per_time` each: a
/// multiple of `chunk` times where that fits, so that no chunk is held,
/// and never more than the partition's `total`.
fn times_per_batch(budget: usize, bytes_per_time: usize, chunk: usize, total: usize) -> usize {
    let fit = (budget / bytes_per_time.max(1)).max(1);
    let aligned = if fit >= chunk { fit - fit % chunk } else { fit };
    aligned.min(total.max(1))
}

#[cfg(test)]
mod tests {
    use super::times_per_batch;

    #[test]
    fn batches_hold_whole_chunks_of_times_where_the_budget_allows() {
        assert_eq!(times_per_batch(1000, 10, 5, 100), 100, "the partition fits");
        assert_eq!(times_per_batch(470, 10, 5, 100), 45, "whole chunks");
        assert_eq!(times_per_batch(30, 10, 5, 100), 3, "less than a chunk");
        assert_eq!(times_per_batch(5, 10, 5, 100), 1, "at least one time");
    }
}
