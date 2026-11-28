// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored baseline geometry used to validate the generated observation.

use std::path::Path;

use casa_imaging_runtime::{Demand, Reservation, admit, free_memory};
use casa_ms::observation_io::{ObservationError, visit_stored_uvw};

use super::SimobserveError;
use crate::{SessionResources, SimulatorError};

/// CASA uses the largest signed u or v, including flag-covered rows.
pub(super) fn largest_signed_uv(
    path: &Path,
    resources: &SessionResources,
) -> Result<Option<f64>, SimobserveError> {
    let (host, policy) = (resources.host(), resources.policy());
    let budget = free_memory(host, policy) as usize;
    let mut reservation = Reservation::none();
    let mut refusal = None;
    let mut largest = None;
    visit_stored_uvw(
        path,
        budget,
        &mut |wanted| {
            let wanted_bytes = wanted as u64;
            if wanted_bytes <= reservation.memory() {
                reservation.retain(wanted_bytes);
                return Ok(());
            }
            let demand = Demand {
                phase: "simobserve's stored UVW survey",
                memory: wanted_bytes - reservation.memory(),
            };
            match admit(host, policy, &demand) {
                Ok(admitted) => {
                    reservation.join(admitted);
                    Ok(())
                }
                Err(admission) => {
                    refusal = Some(admission);
                    Err(ObservationError::OverBudget {
                        what: "simobserve's stored UVW survey",
                        required: wanted,
                        budget: reservation.memory() as usize,
                    })
                }
            }
        },
        &mut |[u, v, _]| {
            largest = Some(largest.unwrap_or(f64::NEG_INFINITY).max(u).max(v));
        },
    )
    .map_err(|error| {
        refusal.map_or_else(
            || SimobserveError::Table {
                table: path.to_path_buf(),
                reason: format!("UVW: {error}"),
            },
            |admission| SimulatorError::admission(admission).into(),
        )
    })?;
    Ok(largest)
}
