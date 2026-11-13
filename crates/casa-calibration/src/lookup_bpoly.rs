// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample a supplied BPOLY onto the simulator's data frequencies, as CASA
//! does when it converts the legacy polynomial table to an ordinary B table.
use super::*;
use crate::plan::SpectralWindowPlan;

impl CalibrationTableLookup {
    pub(super) fn bpoly_layout(
        path: &Path,
        frequencies: &[Vec<f64>],
    ) -> Result<CalibrationTableLayout, ApplyExecutionError> {
        let rows = Self::rows_on_disk(path)?;
        let maximum = frequencies.iter().map(Vec::len).max().unwrap_or(0);
        if maximum == 0 {
            return Err(Self::table_error(
                path,
                "BPOLY requires the data frequency grids",
            ));
        }
        let (_, amp, largest_amp, _) = shape_counts(path, "POLY_COEFF_AMP")
            .map_err(|reason| Self::table_error(path, &reason))?;
        let (_, phase, largest_phase, _) = shape_counts(path, "POLY_COEFF_PHASE")
            .map_err(|reason| Self::table_error(path, &reason))?;
        // The ordinary charge covers the sampled grid; include input
        // coefficients too, so reading unusually high orders is admitted.
        let sampled = rows.saturating_mul(2).saturating_mul(maximum);
        Ok(CalibrationTableLayout {
            rows,
            values: sampled.saturating_add(amp).saturating_add(phase),
            max_cell_values: (2 * maximum).max(largest_amp + largest_phase),
            frequency_values: frequencies.iter().map(Vec::len).sum(),
            frequency_windows: frequencies.len(),
        })
    }

    pub(super) fn open_bpoly(
        path: &Path,
        interp: ApplyInterpolationMode,
        frequencies: &[Vec<f64>],
    ) -> Result<Self, ApplyExecutionError> {
        let summary =
            summarize_table(path).map_err(|error| Self::table_error(path, &error.to_string()))?;
        let plan = ApplyCalibrationTablePlan {
            spec: ApplyCalibrationTableSpec {
                interp,
                ..ApplyCalibrationTableSpec::new(path)
            },
            applicable_selected_row_count: 0,
            summary,
            resolved_gainfield: None,
            resolved_nearest_gainfields: Vec::new(),
            spw_mapping: Vec::new(),
            calibration_spectral_windows: frequencies
                .iter()
                .enumerate()
                .map(|(spw, frequencies)| SpectralWindowPlan {
                    spw_id: spw as i32,
                    num_chan: frequencies.len() as i32,
                    ref_frequency_hz: frequencies.first().copied().unwrap_or(0.),
                    channel_frequencies_hz: frequencies.clone(),
                })
                .collect(),
            interp,
            calwt: false,
        };
        let table = load_simulator_table(&plan)?;
        let across_fields = Some(across_fields(&table));
        Ok(Self {
            path: path.to_path_buf(),
            table,
            subtype: "BPOLY".into(),
            interp,
            across_fields,
            advanced_time: None,
            baseline_antennas: None,
        })
    }

    pub(super) fn table_error(path: &Path, reason: &str) -> ApplyExecutionError {
        ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: reason.to_owned(),
        }
    }
}
