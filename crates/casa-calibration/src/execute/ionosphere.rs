// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert measured line-of-sight TEC into dispersive phase and Faraday rotation.

use std::sync::Mutex;

use casa_core::measures::calculate_igrf_at_shell_offset;
use casa_core::measures::epoch::{EpochRef, MEpoch};

use super::*;
use crate::gencal::tecim::{DEFAULT_HEIGHT_KM, HEIGHT_KEYWORD};

pub(super) struct Ionosphere {
    height_m: f64,
    field_cache: Mutex<HashMap<(u64, i32, i32), f64>>,
}

impl Ionosphere {
    pub(super) fn for_plan(
        table: &Table,
        plan: &ApplyCalibrationTablePlan,
    ) -> Result<Option<Self>, ApplyExecutionError> {
        if plan.summary.table_subtype != "F Jones" {
            return Ok(None);
        }
        let path = &plan.spec.path;
        // FJones::setApply reads the keyword. CLPatchPanel setup calls the
        // base setCallib path and keeps the default magnetic shell height.
        let keyword = if plan.spec.callibrary.is_none() {
            table.keywords().get(HEIGHT_KEYWORD)
        } else {
            None
        };
        let height = match keyword {
            Some(Value::Scalar(ScalarValue::Float64(height))) => *height,
            Some(Value::Scalar(ScalarValue::Float32(height))) => f64::from(*height),
            None => DEFAULT_HEIGHT_KM,
            _ => {
                return Err(invalid(
                    path,
                    "F Jones ionosphere height is not a real scalar",
                ));
            }
        };
        if !height.is_finite() {
            return Err(invalid(path, "F Jones ionosphere height must be finite"));
        }
        Ok(Some(Self {
            height_m: 1000. * height,
            field_cache: Mutex::default(),
        }))
    }

    fn magnetic_field(
        &self,
        row: &ApplyRowPlan,
        antenna: i32,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<f64, ApplyExecutionError> {
        let key = (row.time_seconds.to_bits(), row.field_id, antenna);
        let cache = || {
            self.field_cache
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        };
        if let Some(value) = cache().get(&key) {
            return Ok(*value);
        }
        let engine = context.engine.ok_or_else(|| {
            invalid(
                context.path,
                "F Jones apply requires MeasurementSet geometry",
            )
        })?;
        let failed = |source| ApplyExecutionError::MutateMeasurementSet {
            path: context.path.display().to_string(),
            source,
        };
        let direction = engine
            .reference_phase_direction(row.field_id as usize, row.time_seconds)
            .map_err(failed)?;
        let frame = engine
            .antenna_frame(row.time_seconds, antenna as usize)
            .map_err(failed)?
            .with_epoch(MEpoch::from_mjd_seconds(row.time_seconds, EpochRef::UTC));
        let sample = calculate_igrf_at_shell_offset(self.height_m, &direction, &frame)
            .map_err(|error| failed(error.into()))?;
        let field_gauss = sample.los_field_nt * 1e-5;
        let mut cache = cache();
        if cache.len() >= 8192 {
            cache.clear();
        }
        cache.insert(key, field_gauss);
        Ok(field_gauss)
    }

    pub(super) fn apply(
        &self,
        row: &ApplyRowPlan,
        grids: [&CalibrationGrid; 2],
        context: &CalibrationSamplingContext<'_>,
        output: &mut ApplyBuffer<'_>,
        forward: bool,
    ) -> Result<(), ApplyExecutionError> {
        let (left, left_flag) = electrons(grids[0], context.path)?;
        let (right, right_flag) = electrons(grids[1], context.path)?;
        let values = [left, right];
        let missing = [left_flag, right_flag];
        let mut rotation = [0.; 2];
        for (side, antenna) in [row.antenna1, row.antenna2].into_iter().enumerate() {
            if !missing[side] {
                rotation[side] =
                    2.3649 * values[side] * self.magnetic_field(row, antenna, context)?;
            }
        }
        let circular = output
            .correlation_types
            .iter()
            .all(|code| matches!(code, 5..=8));
        let linear_order = (!circular)
            .then(|| polarization::coherency_order(row.data_desc_id, output.correlation_types))
            .transpose()?;
        let sign = if forward { 1. } else { -1. };
        let write_flags = output.propagate_flags;
        for (channel, &frequency) in context
            .data_channels
            .frequencies_hz
            .iter()
            .enumerate()
            .take(output.corrected.shape()[1])
        {
            let delay = values.map(|tec| -8.4483e-7 * tec / frequency);
            let angle = rotation.map(|term| term / frequency / frequency);
            let mut invalid = missing.into_iter().any(|flag| flag);
            if let Some(order) = linear_order {
                let dispersion = unit_phase(sign * (delay[0] - delay[1]));
                let rotations = angle.map(|r| (sign * r) as f32).map(f32::sin_cos);
                let samples = order.map(|index| output.corrected[[index, channel]]);
                for (index, value) in order
                    .into_iter()
                    .zip(parang::rotate_linear(samples, rotations))
                {
                    invalid |= write_flags && output.flags[[index, channel]];
                    output.corrected[[index, channel]] = dispersion * value;
                }
            } else {
                let phases = std::array::from_fn::<_, 2, _>(|side| {
                    [delay[side] + angle[side], delay[side] - angle[side]]
                });
                for (index, &code) in output.correlation_types.iter().enumerate() {
                    let (a, b) = correlation_receptors(code).expect("circular correlation");
                    output.corrected[[index, channel]] *=
                        unit_phase(sign * (phases[0][a] - phases[1][b]));
                }
            }
            if invalid && write_flags {
                for index in 0..output.correlation_types.len() {
                    if !output.flags[[index, channel]] {
                        output.flags[[index, channel]] = true;
                        *output.newly_flagged_samples += 1;
                    }
                }
            }
        }
        Ok(())
    }
}

fn electrons(grid: &CalibrationGrid, path: &Path) -> Result<(f64, bool), ApplyExecutionError> {
    let CalibrationGrid::Complex(grid) = grid else {
        return Err(invalid(path, "F Jones requires scalar FPARAM rows"));
    };
    let Some(gain) = grid.values.first() else {
        return Err(invalid(
            path,
            "F Jones requires one line-of-sight TEC value",
        ));
    };
    let invalid = grid.flags.first().copied().unwrap_or(true) || !gain.re.is_finite();
    Ok((if invalid { 0. } else { f64::from(gain.re) }, invalid))
}

fn unit_phase(angle: f64) -> Complex32 {
    let (sin, cos) = angle.sin_cos();
    Complex32::new(cos as f32, sin as f32)
}

fn invalid(path: &Path, message: &str) -> ApplyExecutionError {
    ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: message.to_string(),
    }
}
