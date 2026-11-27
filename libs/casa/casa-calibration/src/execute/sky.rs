// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exposure-domain noise propagation for a single-dish sky subtraction.

use super::*;
use crate::constants::{COL_INTERVAL, COL_OBSERVATION_ID};

pub(super) fn apply_grid(
    grid: &CalibrationGrid,
    path: &Path,
    output: &mut ApplyBuffer<'_>,
    forward: bool,
) -> Result<(), ApplyExecutionError> {
    let CalibrationGrid::Complex(grid) = grid else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "single-dish sky requires a real spectrum".into(),
        });
    };
    let correlations = output.correlation_types.len();
    if grid.receptor_count < correlations {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: path.display().to_string(),
            shape: grid.values.shape().to_vec(),
        });
    }
    for correlation in 0..correlations {
        for channel in 0..output.data_channels.frequencies_hz.len() {
            let source = output.data_channels.original_index(channel);
            let off = grid.value_at(correlation, source).re;
            let invalid_off = !off.is_finite() || off == 0.;
            if (grid.flag_at(correlation, source) || invalid_off)
                && output.propagate_flags
                && !output.flags[[correlation, channel]]
            {
                output.flags[[correlation, channel]] = true;
                *output.newly_flagged_samples += 1;
            }
            if invalid_off {
                continue;
            }
            let value = &mut output.corrected[[correlation, channel]];
            *value = if forward {
                *value * off + off
            } else {
                (*value - off) / off
            };
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct OffIntegration {
    time: f64,
    exposure: f64,
}

pub(super) struct SkyExposures {
    on_exposures: Vec<f64>,
    off_integrations: HashMap<(i32, i32, i32), Vec<OffIntegration>>,
}

impl SkyExposures {
    pub(super) fn read(
        ms: &MeasurementSet,
        table: &Table,
        path: &Path,
    ) -> Result<Self, ApplyExecutionError> {
        use casa_tables::RequiredScalarColumnValues::{Float64, Int32};

        let invalid = |reason: &str| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: reason.to_owned(),
        };
        let mut on = ms
            .main_table()
            .required_scalar_columns_owned(&["EXPOSURE"])
            .map_err(|error| invalid(&error.to_string()))?;
        let Some(Float64(on_exposures)) = on.remove("EXPOSURE") else {
            return Err(invalid("MAIN.EXPOSURE is not a Double column"));
        };
        let off = table
            .required_scalar_columns_owned(&[
                COL_TIME,
                COL_INTERVAL,
                COL_ANTENNA1,
                COL_SPECTRAL_WINDOW_ID,
                COL_OBSERVATION_ID,
            ])
            .map_err(|error| invalid(&error.to_string()))?;
        let (
            Float64(times),
            Float64(exposures),
            Int32(antennas),
            Int32(windows),
            Int32(observations),
        ) = (
            &off[COL_TIME],
            &off[COL_INTERVAL],
            &off[COL_ANTENNA1],
            &off[COL_SPECTRAL_WINDOW_ID],
            &off[COL_OBSERVATION_ID],
        )
        else {
            return Err(invalid(
                "Invalid sky calibration time, exposure, antenna, window or observation column",
            ));
        };
        let mut off_integrations = HashMap::<_, Vec<_>>::new();
        for (index, &time) in times.iter().enumerate() {
            off_integrations
                .entry((observations[index], windows[index], antennas[index]))
                .or_default()
                .push(OffIntegration {
                    time,
                    exposure: exposures[index],
                });
        }
        for integrations in off_integrations.values_mut() {
            integrations.sort_by(|left, right| left.time.total_cmp(&right.time));
        }
        Ok(Self {
            on_exposures,
            off_integrations,
        })
    }

    pub(super) fn weight_factor(
        &self,
        row: &ApplyRowPlan,
        window: i32,
        interpolation: ApplyInterpolationMode,
    ) -> f32 {
        let key = (row.calibration_observation_id, window, row.antenna1);
        let Some(off) = self.off_integrations.get(&key) else {
            return 1.;
        };
        let time = row.time_seconds;
        let on = self.on_exposures[row.row_index];
        // The sky noise law uses nearest only for the unmodified time
        // token. For example, nearestperobs samples spectra at nearest
        // time but still uses both reference exposures for noise.
        let nearest = matches!(
            interpolation,
            ApplyInterpolationMode::Nearest
                | ApplyInterpolationMode::NearestLinear
                | ApplyInterpolationMode::Casa {
                    time: crate::table::TimeInterpolation::Nearest,
                    per_observation: false,
                    per_scan: false,
                    phase_delay: false,
                    ..
                }
        );
        let next = off.partition_point(|item| item.time < time);
        let factor = match (
            next.checked_sub(1).and_then(|index| off.get(index)),
            off.get(next),
        ) {
            (_, Some(right)) if right.time == time => single_weight(on, right.exposure),
            (None, Some(right)) => single_weight(on, right.exposure),
            (Some(left), None) => single_weight(on, left.exposure),
            (Some(left), Some(right)) if nearest => {
                let chosen = if time - left.time <= right.time - time {
                    left
                } else {
                    right
                };
                single_weight(on, chosen.exposure)
            }
            (Some(left), Some(right)) => {
                let fraction = (time - left.time) / (right.time - left.time);
                let variance =
                    (1. - fraction).powi(2) / left.exposure + fraction.powi(2) / right.exposure;
                1. / (1. + on * variance)
            }
            (None, None) => 1.,
        };
        if factor.is_finite() {
            factor as f32
        } else {
            0.
        }
    }
}

fn single_weight(on: f64, off: f64) -> f64 {
    off / (on + off)
}
