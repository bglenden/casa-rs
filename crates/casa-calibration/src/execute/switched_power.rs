// SPDX-License-Identifier: LGPL-3.0-or-later
//! Gains and independent weight scales from VLA switched-power tables.

use super::{ApplyExecutionError, GainGrid, GainSample, MeasurementSet};
use crate::gencal::EVLA_CORRELATOR_EFFICIENCY;
use casa_core::{Complex32, Value};
use casa_tables::{Table, TableOptions, add_directory};
use std::path::Path;

#[derive(Clone)]
pub(crate) struct SwitchedPowerGrid {
    gains: [GainSample; 2],
    temperatures: [f32; 2],
}

impl SwitchedPowerGrid {
    pub(super) fn from_grid(path: &Path, grid: &GainGrid) -> Result<Self, ApplyExecutionError> {
        if grid.receptor_count != 4 || grid.channel_count != 1 {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: grid.values.shape().to_vec(),
            });
        }
        Ok(Self {
            gains: [0, 2].map(|index| GainSample {
                value: grid.value_at(index, 0),
                flagged: grid.flag_at(index, 0),
            }),
            temperatures: [1, 3].map(|index| grid.value_at(index, 0).re),
        })
    }

    pub(super) fn sample(&self, receptor: usize) -> GainSample {
        self.gains[receptor]
    }

    pub(super) fn weight_scale(&self, receptor: usize) -> f32 {
        let tsys = self.temperatures[receptor];
        EVLA_CORRELATOR_EFFICIENCY / if tsys < f32::MIN_POSITIVE { 1.0 } else { tsys }
    }

    pub(super) fn flag_all(&mut self) {
        self.gains.fill(GainSample {
            value: Complex32::new(1.0, 0.0),
            flagged: true,
        });
        self.temperatures.fill(1.0);
    }
}

/// CASA's apply constructor resolves both subtables even for a precomputed
/// switched-power calibration table, without reading their measurements.
pub(super) fn validate_context(
    ms: &MeasurementSet,
    path: &Path,
) -> Result<(), ApplyExecutionError> {
    for name in ["SYSPOWER", "CALDEVICE"] {
        let Some(reference) = ms
            .main_table()
            .keywords()
            .get(name)
            .and_then(Value::as_table_ref)
        else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("RecordInterface: field {name} is unknown"),
            });
        };
        let auxiliary = add_directory(reference, ms.path().unwrap_or_else(|| Path::new(".")));
        Table::open(TableOptions::new(&auxiliary)).map_err(|source| {
            ApplyExecutionError::OpenCalibrationAuxiliaryTable {
                path: path.display().to_string(),
                subtable: name,
                source,
            }
        })?;
    }
    Ok(())
}
