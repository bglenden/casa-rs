// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored baseline geometry used to validate the generated observation.

use std::path::Path;

use casa_imaging_runtime::{Demand, Reservation, admit, free_memory};
use casa_ms::observation_io::{OpenOptions, open_granted};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, FieldSelection, Grouping, ObservationError,
    PartitionId, ReadEvent, Roles, TimeSelection, WeightRequest,
};

use super::SimobserveError;
use crate::{SessionResources, SimulatorError};

/// CASA uses the largest signed u or v, including flag-covered rows.
pub(super) fn largest_signed_uv(
    path: &Path,
    resources: &SessionResources,
) -> Result<Option<f64>, SimobserveError> {
    let storage = |error: ObservationError| SimobserveError::Table {
        table: path.to_path_buf(),
        reason: format!("UVW: {error}"),
    };
    let (host, policy) = (resources.host(), resources.policy());
    let mut opening = Reservation::none();
    let mut refusal = None;
    let observation = open_granted(path, &OpenOptions::default(), &mut |wanted| {
        let extra = (wanted as u64).saturating_sub(opening.memory());
        if extra != 0 {
            let demand = Demand {
                phase: "simobserve's UVW catalogue",
                memory: extra,
            };
            match admit(host, policy, &demand) {
                Ok(admitted) => opening.join(admitted),
                Err(admission) => {
                    refusal = Some(admission);
                    return Err(ObservationError::OverBudget {
                        what: "the UVW catalogue",
                        required: wanted,
                        budget: opening.memory() as usize,
                    });
                }
            }
        }
        Ok(())
    })
    .map_err(|error| {
        refusal.map_or_else(
            || storage(error),
            |admission| SimulatorError::admission(admission).into(),
        )
    })?;

    let batch_bytes = free_memory(host, policy).min(casa_ms::TARGET_BATCH_BYTES as u64);
    let _batch = admit(
        host,
        policy,
        &Demand {
            phase: "simobserve's stored UVW batch",
            memory: batch_bytes,
        },
    )
    .map_err(SimulatorError::admission)?;
    let mut request = AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".into(),
        roles: Roles {
            values: false,
            flags: false,
            weights: false,
            uvw: true,
            time_centroid: false,
            interval: false,
            exposure: false,
            scan: false,
        },
        weights: WeightRequest::Stored,
        channels: 0..1,
        times: TimeSelection::All,
        fields: FieldSelection::All,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: batch_bytes as usize,
    };
    let mut largest = None;
    for index in 0..observation.catalogue().partitions.len() {
        request.partitions[0] = PartitionId(index as u32);
        let mut traversal = observation.bind_read(&request).map_err(&storage)?;
        let mut storage_buffer =
            BatchBuffers::with_envelope(traversal.estimate.peak_buffer_bytes());
        while let Some(event) = traversal.next(&mut storage_buffer).map_err(&storage)? {
            if let ReadEvent::Batch(batch) = event {
                let uvw = batch.group.uvw.expect("UVW was requested").metres();
                for row in uvw.rows() {
                    largest = Some(largest.unwrap_or(f64::NEG_INFINITY).max(row[0]).max(row[1]));
                }
            }
        }
    }
    Ok(largest)
}
