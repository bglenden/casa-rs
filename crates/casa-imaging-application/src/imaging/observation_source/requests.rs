// SPDX-License-Identifier: LGPL-3.0-or-later
//! The requests the batch route binds and estimates: a partition's payload
//! for a wave, and its payload-free survey.

use std::ops::Range;

use casa_imaging_model::GeometryPlan;
use casa_observation::{
    AccessRequest, CorrelationRequest, Deliver, FlagsFrom, Grouping, PartitionEntry, Roles,
    TimeSelection, WeightRequest,
};

use super::selection::{PartitionSelection, SelectedPartition};

/// The request reading `partition`'s (`entry` in the catalogue) native
/// channels `channels`: its selected fields' times, its routed products
/// with their own flags, the stored weights, UVW, the time centroids when
/// `plan` reads them and, with `values`, the visibilities, at most
/// `batch_bytes` a batch.
pub(super) fn payload(
    (selection, plan): (&PartitionSelection, &GeometryPlan),
    entry: &PartitionEntry,
    partition: &SelectedPartition,
    channels: Range<usize>,
    values: bool,
    batch_bytes: usize,
) -> AccessRequest {
    let deliver = if partition.correlations == entry.correlations {
        Deliver::All
    } else {
        Deliver::These(partition.correlations.clone())
    };
    AccessRequest {
        partitions: vec![partition.id],
        data_group: selection.data_group.clone(),
        roles: Roles {
            values,
            time_centroid: plan.reads_time_centroids(),
            ..Roles::PAYLOAD
        },
        weights: WeightRequest::Stored,
        channels,
        times: TimeSelection::All,
        fields: selection.fields.clone(),
        correlations: CorrelationRequest {
            deliver,
            flags_from: FlagsFrom::Delivered,
        },
        grouping: Grouping::None,
        batch_bytes,
    }
}

/// The request surveying `partition`: its selected fields' times, and
/// their time centroids when `plan` reads them, of one channel and no
/// payload, at most `batch_bytes` a batch.
pub(super) fn survey(
    (selection, plan): (&PartitionSelection, &GeometryPlan),
    entry: &PartitionEntry,
    partition: &SelectedPartition,
    batch_bytes: usize,
) -> AccessRequest {
    let first = partition.channels[0] as usize;
    let request = payload(
        (selection, plan),
        entry,
        partition,
        first..first + 1,
        false,
        batch_bytes,
    );
    AccessRequest {
        roles: Roles {
            values: false,
            flags: false,
            weights: false,
            uvw: false,
            ..request.roles
        },
        ..request
    }
}
