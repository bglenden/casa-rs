// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation a request names, opened once for compile and the run:
//! its opening admitted as it grows, and its index charged while it is
//! open.

use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};
use casa_ms::MeasurementSet;
use casa_ms::observation_io::{Msv2Observation, OpenOptions};
use casa_observation::ObservationError;

use super::PrepareError;

/// The MeasurementSet `ms` opened as an observation, with the charge of its
/// index while it is open ([`Msv2Observation::index_bytes`]). What opening
/// usually holds is admitted before it opens, from MAIN's row count
/// ([`Msv2Observation::opening_bytes`]), and each increase it asks for
/// beyond ([`Msv2Observation::open_granted`]) is admitted before it is
/// allocated: one that does not fit refuses the opening, as an admission.
/// The charge is then trimmed to the index, or raised to it.
pub(crate) fn open_observation(
    ms: MeasurementSet,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<Msv2Observation>, PrepareError> {
    let mut reservation = admit(
        host,
        policy,
        &Demand {
            phase: "observation opening",
            memory: Msv2Observation::opening_bytes(ms.row_count()) as u64,
        },
    )?;
    let mut refusal = None;
    let opened = Msv2Observation::open_granted_on(ms, &OpenOptions::default(), &mut |total| {
        let total = total as u64;
        if total > reservation.memory() {
            let demand = Demand {
                phase: "observation opening",
                memory: total - reservation.memory(),
            };
            match admit(host, policy, &demand) {
                Ok(more) => reservation.join(more),
                Err(admission) => {
                    refusal = Some(admission);
                    return Err(ObservationError::OverBudget {
                        what: "the MSv2 opening index",
                        required: total as usize,
                        budget: reservation.memory() as usize,
                    });
                }
            }
        }
        Ok(())
    });
    let opened = opened.map_err(|error| match refusal.take() {
        Some(admission) => PrepareError::Admission(admission),
        None => PrepareError::Opening(error),
    })?;
    // The catalogue's ephemerides are read with the subtables, before
    // the index; they are charged with it.
    let index = (opened.index_bytes() + opened.catalogue().ephemeris_bytes()) as u64;
    if index <= reservation.memory() {
        reservation.retain(index);
    } else {
        reservation.join(admit(
            host,
            policy,
            &Demand {
                phase: "observation index",
                memory: index - reservation.memory(),
            },
        )?);
    }
    Ok(Admitted {
        value: opened,
        reservation,
    })
}
