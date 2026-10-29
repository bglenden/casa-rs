// SPDX-License-Identifier: LGPL-3.0-or-later
//! The inputs a plan reads, each opened by its adapter with the order in
//! which its observing sessions first appear: by first MAIN row in an MSv2,
//! as CASA's `concat` meets them, and by earliest time in an MSv4.

use std::path::PathBuf;
#[cfg(feature = "msv4")]
use std::slice;

use casa_observation::{Observation, ObservationError, ObservationId};

use super::CreationInput;
#[cfg(feature = "msv4")]
use super::lockstep::{Sizing, WEIGHTS};
#[cfg(feature = "msv4")]
use super::memory::grid_bytes;
use super::memory::{Ledger, OPEN_TABLES_BYTES, catalogue_bytes};
use crate::observation_io::update::request_error;
use crate::observation_io::{Msv2Observation, OpenOptions};

/// Seconds per day, for times as MJD seconds.
#[cfg(feature = "msv4")]
const SECONDS_PER_DAY: f64 = 86_400.0;

/// An input's opening, by store.
enum Opening {
    Msv2(Box<Msv2Observation>),
    #[cfg(feature = "msv4")]
    Msv4(Box<crate::observation_io::Msv4Observation>),
}

/// One opened input.
pub(super) struct Input {
    /// Its store.
    pub(super) path: PathBuf,
    opening: Opening,
    /// The factor its weights are multiplied by.
    pub(super) weight_scale: f32,
    /// Its observing sessions in the order they first appear; a session
    /// no partition holds comes last.
    pub(super) appearance: Vec<ObservationId>,
}

impl Input {
    /// Its opening.
    pub(super) fn observation(&self) -> &dyn Observation {
        match &self.opening {
            Opening::Msv2(observation) => observation.as_ref(),
            #[cfg(feature = "msv4")]
            Opening::Msv4(observation) => observation.as_ref(),
        }
    }

    /// Its opening when it is an MSv2's, whose rows state what an
    /// observation does not (each row's `STATE_ID`).
    pub(super) fn msv2(&self) -> Option<&Msv2Observation> {
        match &self.opening {
            Opening::Msv2(observation) => Some(observation),
            #[cfg(feature = "msv4")]
            Opening::Msv4(_) => None,
        }
    }

    /// Open `input`, its weight scale checked, admitting what the opening
    /// keeps to `ledger`: an MSv2's tables held open, its index, each
    /// increase asked for before it is allocated
    /// ([`Msv2Observation::open_granted`]) and the charge then trimmed to
    /// the index kept, and its catalogue once read; an MSv4's catalogue and
    /// coordinates once read.
    pub(super) fn open(
        input: &CreationInput,
        ledger: &mut Ledger,
    ) -> Result<Self, ObservationError> {
        let weight_scale = weight_scale(input)?;
        let path = input.path.clone();
        if path.join("zarr.json").is_file() {
            return msv4(path, weight_scale, ledger);
        }
        ledger.admit("an input's tables", OPEN_TABLES_BYTES)?;
        let mut granted = 0;
        let observation =
            Msv2Observation::open_granted(&path, &OpenOptions::default(), &mut |total| {
                if total > granted {
                    ledger.admit("an input's opening", total - granted)?;
                    granted = total;
                }
                Ok(())
            })?;
        let index = observation.index_bytes();
        if index <= granted {
            ledger.release(granted - index);
        } else {
            ledger.admit("an input's opening", index - granted)?;
        }
        ledger.admit(
            "an input's catalogue",
            catalogue_bytes(observation.catalogue()),
        )?;
        let catalogue = observation.catalogue();
        let mut first = vec![None; catalogue.observations.len()];
        for id in catalogue.partition_ids() {
            let session = catalogue.partition(id).observation.0 as usize;
            if let (Some(row), Some(known)) = (observation.first_row(id), first.get_mut(session)) {
                *known = Some(known.map_or(row, |earlier: u32| earlier.min(row)));
            }
        }
        Ok(Self {
            path,
            appearance: in_order(&first),
            opening: Opening::Msv2(Box::new(observation)),
            weight_scale,
        })
    }
}

/// An MSv4 input, its sessions by earliest time, its catalogue and its
/// datasets' coordinates (their grids, held whole) admitted to `ledger`.
#[cfg(feature = "msv4")]
fn msv4(path: PathBuf, weight_scale: f32, ledger: &mut Ledger) -> Result<Input, ObservationError> {
    let observation =
        crate::observation_io::Msv4Observation::open_with(&path, &OpenOptions::default())?;
    let catalogue = observation.catalogue().clone();
    let mut coordinates = 0;
    for id in catalogue.partition_ids() {
        let entry = catalogue.partition(id);
        if let Some(group) = entry.data_groups.first() {
            let sizing = Sizing::of(&observation, id, slice::from_ref(&group.name), WEIGHTS)?;
            coordinates += entry.data_groups.len() * grid_bytes(sizing.times, 0);
        }
    }
    ledger.admit(
        "an input's catalogue",
        catalogue_bytes(&catalogue) + coordinates,
    )?;
    let mut first: Vec<Option<f64>> = vec![None; catalogue.observations.len()];
    for id in catalogue.partition_ids() {
        let entry = catalogue.partition(id);
        let Some(group) = entry.data_groups.first() else {
            continue;
        };
        let bytes = grid_bytes(
            Sizing::of(&observation, id, slice::from_ref(&group.name), WEIGHTS)?.times,
            entry.baselines.len(),
        );
        ledger.admit("a partition's grid", bytes)?;
        let grid = observation.grid(id, &group.name);
        ledger.release(bytes);
        let grid = grid?;
        let Some(time) = grid.times_s.first() else {
            continue;
        };
        let origin = grid.time_origin;
        let time = (origin.day() + origin.frac()) * SECONDS_PER_DAY + time;
        if let Some(known) = first.get_mut(entry.observation.0 as usize) {
            *known = Some(known.map_or(time, |earlier: f64| earlier.min(time)));
        }
    }
    Ok(Input {
        path,
        appearance: in_order(&first),
        opening: Opening::Msv4(Box::new(observation)),
        weight_scale,
    })
}

/// Without the `msv4` feature an MSv4 cannot be opened.
#[cfg(not(feature = "msv4"))]
fn msv4(
    _path: PathBuf,
    _weight_scale: f32,
    _ledger: &mut Ledger,
) -> Result<Input, ObservationError> {
    Err(ObservationError::Unsupported {
        what: "an MSv4 store without casa-ms's msv4 feature",
    })
}

/// Sessions in the order of `first` (each one's first appearance), those
/// without one last, in ID order.
fn in_order<T: PartialOrd + Copy>(first: &[Option<T>]) -> Vec<ObservationId> {
    let mut sessions: Vec<usize> = (0..first.len()).collect();
    sessions.sort_by(|a, b| match (first[*a], first[*b]) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.cmp(b),
    });
    sessions
        .into_iter()
        .map(|session| ObservationId(session as u32))
        .collect()
}

/// `input`'s weight scale, which must be finite and positive.
fn weight_scale(input: &CreationInput) -> Result<f32, ObservationError> {
    let scale = input.weight_scale as f32;
    if scale.is_finite() && scale > 0.0 {
        Ok(scale)
    } else {
        Err(request_error(format!(
            "the weight scale of {} is {}; it must be finite and positive",
            input.path.display(),
            input.weight_scale
        )))
    }
}

#[cfg(test)]
mod tests {
    use casa_observation::ObservationId;

    use super::in_order;

    #[test]
    fn sessions_follow_their_first_appearance() {
        let order = in_order(&[Some(30), None, Some(10), Some(20)]);
        assert_eq!(order, [2, 3, 0, 1].map(ObservationId));
    }
}
