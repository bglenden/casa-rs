// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rotate selected UVW and visibility cells to the requested PlotMS phase center.

use std::collections::{HashMap, hash_map::Entry};
use std::f64::consts::TAU;

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::{Complex32, Complex64};
use casa_ms::MeasurementSet;
use casa_ms::columns::frequency_columns::ChanFreqColumn;
use casa_ms::derived::engine::{UvwRotation, detect_epoch_reference, raw_field_phase_direction};
use casa_ms::subtables::SubTable;

use super::ComplexGrid;

const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

pub struct PhaseCenterShift<'a> {
    target: &'a MDirection,
    time_reference: EpochRef,
    field_rotations: HashMap<usize, FieldRotation>,
    channel_frequencies: HashMap<usize, Vec<f64>>,
    phases: Vec<Complex32>,
}

struct FieldRotation {
    rotation: UvwRotation,
    epoch: Option<u64>,
}

impl<'a> PhaseCenterShift<'a> {
    pub(super) fn new(ms: &MeasurementSet, target: &'a MDirection) -> Self {
        Self {
            target,
            time_reference: detect_epoch_reference(ms.main_table(), "TIME", EpochRef::UTC),
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
        time_mjd_seconds: f64,
        uvw: [f64; 3],
        grids: &mut [ComplexGrid],
    ) -> Result<[f64; 3], String> {
        let uvw = self.prepare_row(ms, field_id, spw, time_mjd_seconds, uvw)?;
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
        Ok(uvw)
    }

    pub(super) fn prepare_row(
        &mut self,
        ms: &MeasurementSet,
        field_id: i32,
        spw: usize,
        time_mjd_seconds: f64,
        [u, v, w]: [f64; 3],
    ) -> Result<[f64; 3], String> {
        let field_id = usize::try_from(field_id)
            .map_err(|_| format!("invalid FIELD_ID {field_id} for phasecenter"))?;
        let epoch_bits = time_mjd_seconds.to_bits();
        let target = self.target;
        let time_reference = self.time_reference;
        let at_epoch = || field_rotation(ms, field_id, target, time_mjd_seconds, time_reference);
        let field = match self.field_rotations.entry(field_id) {
            Entry::Occupied(mut entry) => {
                if entry.get().epoch.is_some_and(|epoch| epoch != epoch_bits) {
                    *entry.get_mut() = at_epoch()?;
                }
                entry.into_mut()
            }
            Entry::Vacant(entry) => entry.insert(at_epoch()?),
        };
        // PhaseShiftingTVI changes the signs of U and V around UVWMachine.
        // The intervening path difference uses that machine's coordinates.
        let ([rotated_u, rotated_v, rotated_w], path_m) = field.rotation.apply([-u, -v, w]);
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
        Ok([-rotated_u, -rotated_v, rotated_w])
    }

    pub(super) fn phase(&self, channel: usize) -> Result<Complex32, String> {
        self.phases
            .get(channel)
            .copied()
            .ok_or_else(|| format!("Missing phasecenter frequency for channel {channel}"))
    }
}

fn field_rotation(
    ms: &MeasurementSet,
    field_id: usize,
    target: &MDirection,
    time_mjd_seconds: f64,
    time_reference: EpochRef,
) -> Result<FieldRotation, String> {
    let field = ms.field().map_err(|error| error.to_string())?;
    let order = field
        .num_poly(field_id)
        .map_err(|error| error.to_string())?;
    let ephemeris = field
        .ephemeris_id(field_id)
        .map_err(|error| error.to_string())?;
    if ephemeris.is_some_and(|id| id >= 0) {
        return Err(format!(
            "PlotMS phasecenter for an ephemeris on FIELD {field_id} is pending (#1032)"
        ));
    }
    let source = if order == 0 {
        raw_field_phase_direction(ms, field_id).map_err(|error| error.to_string())?
    } else {
        let epoch = MEpoch::from_mjd_seconds(time_mjd_seconds, time_reference);
        let direction = ms
            .metadata_field_direction(field_id, false, Some(&epoch))
            .map_err(|error| error.to_string())?;
        if direction.offset.is_some() {
            return Err(format!(
                "PlotMS phasecenter for an offset polynomial on FIELD {field_id} is pending (#1032)"
            ));
        }
        direction.direction
    };
    if source.refer() != DirectionRef::J2000 {
        return Err(format!(
            "PlotMS phasecenter requires a J2000 source direction for selected FIELD {field_id} (#1032)"
        ));
    }
    Ok(FieldRotation {
        rotation: UvwRotation::new(&source, target),
        epoch: (order != 0).then(|| time_mjd_seconds.to_bits()),
    })
}

fn channel_frequencies(ms: &MeasurementSet, spw: usize) -> Result<Vec<f64>, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    let frequencies = ChanFreqColumn::new(spectral_windows.table())
        .get_frequencies(spw)
        .map_err(|error| error.to_string())?;
    Ok(frequencies.iter().map(|frequency| frequency.hz()).collect())
}
