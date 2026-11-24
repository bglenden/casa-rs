// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rotate selected UVW and visibility cells for a fixed PlotMS phase center.

use std::collections::{HashMap, hash_map::Entry};
use std::f64::consts::TAU;

use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::{Complex32, Complex64};

use crate::MeasurementSet;
use crate::columns::frequency_columns::ChanFreqColumn;
use crate::derived::engine::{UvwRotation, raw_field_phase_direction};
use crate::derived::parse_phase_direction;
use crate::subtables::SubTable;

use super::ComplexGrid;

const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

pub(super) fn requested(task: &TaskInvocation<'_>) -> Result<Option<String>, TaskFailure> {
    if task.flag("transform")? {
        Ok(task.text("phasecenter")?.filter(|value| !value.is_empty()))
    } else {
        Ok(None)
    }
}

pub(super) fn parse(value: &str) -> Result<MDirection, TaskFailure> {
    let target = parse_phase_direction(value).ok_or_else(|| {
        TaskFailure::validation(format!(
            "Cannot interpret phasecenter {value:?} as a direction"
        ))
        .at("phasecenter")
    })?;
    if target.refer() != DirectionRef::J2000 {
        return Err(TaskFailure::validation(
            "PlotMS phasecenter targets outside J2000 are pending (#1032)",
        )
        .at("phasecenter"));
    }
    Ok(target)
}

pub(super) struct PhaseCenterShift<'a> {
    target: &'a MDirection,
    field_rotations: HashMap<usize, UvwRotation>,
    channel_frequencies: HashMap<usize, Vec<f64>>,
    phases: Vec<Complex32>,
}

impl<'a> PhaseCenterShift<'a> {
    pub(super) fn new(target: &'a MDirection) -> Self {
        Self {
            target,
            field_rotations: HashMap::new(),
            channel_frequencies: HashMap::new(),
            phases: Vec::new(),
        }
    }

    pub(super) fn apply(
        &mut self,
        ms: &MeasurementSet,
        field_id: i32,
        spw: usize,
        [u, v, w]: [f64; 3],
        grids: &mut [ComplexGrid],
    ) -> Result<[f64; 3], String> {
        let field_id = usize::try_from(field_id)
            .map_err(|_| format!("invalid FIELD_ID {field_id} for phasecenter"))?;
        let rotation = match self.field_rotations.entry(field_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(field_rotation(ms, field_id, self.target)?),
        };
        // PhaseShiftingTVI changes the signs of U and V around UVWMachine.
        // The intervening path difference uses that machine's coordinates.
        let ([rotated_u, rotated_v, rotated_w], path_m) = rotation.apply([-u, -v, w]);
        let radians_per_hz = (-TAU / SPEED_OF_LIGHT_M_S) * path_m;
        let frequencies = match self.channel_frequencies.entry(spw) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(channel_frequencies(ms, spw)?),
        };
        self.phases.clear();
        self.phases.extend(frequencies.iter().map(|frequency| {
            let (sin, cos) = (radians_per_hz * frequency).sin_cos();
            Complex32::new(cos as f32, sin as f32)
        }));
        for grid in grids {
            if grid.chan_count != self.phases.len() {
                return Err(format!(
                    "phasecenter shift requires {} channel frequencies, found {} in spectral window {spw}",
                    grid.chan_count,
                    self.phases.len(),
                ));
            }
            for (value, phase) in grid.values.iter_mut().zip(self.phases.iter().cycle()) {
                let visibility = Complex32::new(value.re as f32, value.im as f32);
                let shifted = *phase * visibility;
                *value = Complex64::new(f64::from(shifted.re), f64::from(shifted.im));
            }
        }
        Ok([-rotated_u, -rotated_v, rotated_w])
    }
}

fn field_rotation(
    ms: &MeasurementSet,
    field_id: usize,
    target: &MDirection,
) -> Result<UvwRotation, String> {
    let field = ms.field().map_err(|error| error.to_string())?;
    let order = field
        .num_poly(field_id)
        .map_err(|error| error.to_string())?;
    let ephemeris = field
        .ephemeris_id(field_id)
        .map_err(|error| error.to_string())?;
    let source = raw_field_phase_direction(ms, field_id).map_err(|error| error.to_string())?;
    if order != 0 || ephemeris.is_some_and(|id| id >= 0) || source.refer() != DirectionRef::J2000 {
        return Err(format!(
            "PlotMS phasecenter requires a fixed J2000 source direction for selected FIELD {field_id} (#1032)"
        ));
    }
    Ok(UvwRotation::new(&source, target))
}

fn channel_frequencies(ms: &MeasurementSet, spw: usize) -> Result<Vec<f64>, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    let frequencies = ChanFreqColumn::new(spectral_windows.table())
        .get_frequencies(spw)
        .map_err(|error| error.to_string())?;
    Ok(frequencies.iter().map(|frequency| frequency.hz()).collect())
}
