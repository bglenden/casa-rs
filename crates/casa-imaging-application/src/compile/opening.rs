// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation a request names, opened once for compile and the run: a
//! MeasurementSet (MSv2) or an MSv4 processing set, through the observation
//! boundary. A MeasurementSet's opening is admitted as it grows and its
//! index charged while it is open; a processing set's opening holds its
//! catalogue and each dataset's times, charged once it is open.

use std::path::Path;
use std::sync::Arc;

use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};
use casa_ms::MeasurementSet;
use casa_ms::observation_io::{Msv2Observation, Msv4Observation, OpenOptions};
use casa_observation::{Catalogue, Observation, ObservationError};

use super::PrepareError;

/// The observation compile and the run read, in its store.
pub(crate) enum OpenedStore {
    /// A MeasurementSet v2, assembled into the boundary's partitions at
    /// opening (boxed: it holds MAIN's index and subtables).
    V2(Box<Msv2Observation>),
    /// An MSv4 processing set (XRADIO's schema on local Zarr v3).
    V4(Msv4Observation),
}

impl OpenedStore {
    /// The observation, whatever its store.
    pub(crate) fn observation(&self) -> &dyn Observation {
        match self {
            Self::V2(observation) => observation.as_ref(),
            Self::V4(observation) => observation,
        }
    }

    /// Its catalogue.
    pub(crate) fn catalogue(&self) -> &Arc<Catalogue> {
        self.observation().catalogue()
    }

    /// The observation, owned, for a run that only reads it.
    pub(crate) fn into_observation(self) -> Box<dyn Observation> {
        match self {
            Self::V2(observation) => observation,
            Self::V4(observation) => Box::new(observation),
        }
    }

    /// The MeasurementSet, for what only an MSv2 states: its data
    /// descriptions and its `PHASE_DIR` polynomials.
    pub(crate) fn msv2(&self) -> Option<&Msv2Observation> {
        match self {
            Self::V2(observation) => Some(observation.as_ref()),
            Self::V4(_) => None,
        }
    }
}

/// The observation at `path`, an MSv4 processing set when it is one (its
/// root holds `zarr.json`), else a MeasurementSet ([`open_measurement_set`]),
/// with the charge of what it holds while it is open.
pub(crate) fn open_observation(
    path: &Path,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<OpenedStore>, PrepareError> {
    if path.join("zarr.json").is_file() {
        let opened = Msv4Observation::open(path).map_err(PrepareError::Opening)?;
        let reservation = admit(
            host,
            policy,
            &Demand {
                phase: "observation opening",
                memory: opened.held_bytes() as u64,
            },
        )?;
        return Ok(Admitted {
            value: OpenedStore::V4(opened),
            reservation,
        });
    }
    let ms = MeasurementSet::open_retained_read(path)?;
    let Admitted { value, reservation } = open_measurement_set(ms, (host, policy))?;
    Ok(Admitted {
        value: OpenedStore::V2(Box::new(value)),
        reservation,
    })
}

/// The MeasurementSet `ms` opened as an observation, with the charge of its
/// index while it is open ([`Msv2Observation::index_bytes`]). What opening
/// usually holds is admitted before it opens, from MAIN's row count
/// ([`Msv2Observation::opening_bytes`]), and each increase it asks for
/// beyond ([`Msv2Observation::open_granted`]) is admitted before it is
/// allocated: one that does not fit refuses the opening, as an admission.
/// The charge is then trimmed to the index, or raised to it.
fn open_measurement_set(
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
