// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation a request names, opened once for compile and the run: a
//! MeasurementSet (MSv2) or an MSv4 processing set, through the observation
//! boundary. Each store admits its opening before allocating and keeps
//! the charge for its retained tables, index, catalogue and coordinates.

use std::path::Path;
use std::sync::Arc;

use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};
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
/// root holds `zarr.json`), else a MeasurementSet,
/// with the charge of what it holds while it is open.
pub(crate) fn open_observation(
    path: &Path,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<OpenedStore>, PrepareError> {
    if path.join("zarr.json").is_file() {
        let Admitted { value, reservation } = admitted_opening(
            (0, "the MSv4 opening"),
            |grant| Msv4Observation::open_granted(path, &OpenOptions::default(), grant),
            (
                |opened: &Msv4Observation| opened.held_bytes() as u64,
                "observation opening",
            ),
            (host, policy),
        )?;
        return Ok(Admitted {
            value: OpenedStore::V4(value),
            reservation,
        });
    }
    let Admitted { value, reservation } = admitted_opening(
        (0, "the MSv2 opening"),
        |grant| Msv2Observation::open_granted(path, &OpenOptions::default(), grant),
        (
            |opened: &Msv2Observation| opened.retained_bytes() as u64,
            "observation retained",
        ),
        (host, policy),
    )?;
    Ok(Admitted {
        value: OpenedStore::V2(Box::new(value)),
        reservation,
    })
}

/// The store `open` opens, admitted on `host` under `policy`: `initial`
/// before it opens, and each increase its opening asks the grant for beyond
/// what is admitted before it is allocated, so that one that does not fit
/// refuses the opening (`what`), as an admission. The charge is then
/// trimmed to what the opened store holds (`held`), or raised to it
/// (`held_phase`).
fn admitted_opening<T>(
    (initial, what): (u64, &'static str),
    open: impl FnOnce(
        &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    ) -> Result<T, ObservationError>,
    (held, held_phase): (impl FnOnce(&T) -> u64, &'static str),
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<T>, PrepareError> {
    let mut reservation = admit(
        host,
        policy,
        &Demand {
            phase: "observation opening",
            memory: initial,
        },
    )?;
    let mut refusal = None;
    let opened = open(&mut |total| {
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
                        what,
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
    let held = held(&opened);
    if held <= reservation.memory() {
        reservation.retain(held);
    } else {
        reservation.join(admit(
            host,
            policy,
            &Demand {
                phase: held_phase,
                memory: held - reservation.memory(),
            },
        )?);
    }
    Ok(Admitted {
        value: opened,
        reservation,
    })
}
