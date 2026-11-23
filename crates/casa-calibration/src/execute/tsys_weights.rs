// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample the Tsys temperatures used to initialize MS weights.

use super::*;
use crate::ComplexSolution;
use crate::plan::{ApplyCalibrationTableSpec, SpectralWindowPlan, plan_with_visibility_metadata};

pub(crate) struct TsysPlan {
    plan: ApplyPlan,
    table: LoadedCalibrationTable,
    correlations: HashMap<i32, Vec<i32>>,
    bandwidths: HashMap<i32, f64>,
    exposures: Option<Vec<f64>>,
}

impl TsysPlan {
    pub(crate) fn read(
        ms: &MeasurementSet,
        spec: ApplyCalibrationTableSpec,
        by_exposure: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let (plan, _) = plan_with_visibility_metadata(
            ms,
            &ApplyPlanRequest {
                selection: casa_ms::MsSelection::default(),
                apply_mode: ApplyMode::Trial,
                parang: false,
                calibration_tables: vec![spec],
            },
            None,
        )?;
        let planned_table = &plan.calibration_tables[0];
        if planned_table.summary.table_subtype != "B TSYS" {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: planned_table.spec.path.display().to_string(),
                reason: "weight initialization requires a B TSYS table".into(),
            });
        }
        let ms_error = |source| ApplyExecutionError::MutateMeasurementSet {
            path: display_ms_path(ms),
            source,
        };
        let spw = ms.spectral_window().map_err(ms_error)?;
        let bandwidths = plan
            .measurement_set_spectral_windows
            .iter()
            .map(|window| {
                let widths = spw.effective_bw(window.spw_id as usize)?;
                Ok((
                    window.spw_id,
                    widths.iter().sum::<f64>() / widths.len() as f64,
                ))
            })
            .collect::<MsResult<_>>()
            .map_err(ms_error)?;
        let exposures = by_exposure
            .then(|| exposures(ms))
            .transpose()
            .map_err(ms_error)?;
        let correlations = load_correlation_types_by_ddid(ms).map_err(ms_error)?;
        let table = fields::load(planned_table, None, ms)?;
        Ok(Self {
            plan,
            table,
            correlations,
            bandwidths,
            exposures,
        })
    }

    pub(crate) fn max_cell_shape(&self) -> (usize, usize) {
        (
            self.correlations.values().map(Vec::len).max().unwrap_or(1),
            self.plan
                .measurement_set_spectral_windows
                .iter()
                .map(|spw| spw.num_chan as usize)
                .max()
                .unwrap_or(1),
        )
    }

    pub(crate) fn row_shapes(&self) -> impl Iterator<Item = [usize; 2]> + '_ {
        (0..self.plan.selected_rows.len()).map(|index| self.row_shape(index))
    }

    pub(crate) fn row_shape(&self, index: usize) -> [usize; 2] {
        let row = &self.plan.selected_rows[index];
        [
            self.correlations[&row.data_desc_id].len(),
            window(&self.plan.measurement_set_spectral_windows, row.data_spw_id).num_chan as usize,
        ]
    }

    pub(crate) fn values(
        &self,
        index: usize,
        spectrum: bool,
    ) -> Result<Option<Vec<ArrayValue>>, ApplyExecutionError> {
        let row = &self.plan.selected_rows[index];
        let plan = &self.plan.calibration_tables[0];
        let data_spw = window(&self.plan.measurement_set_spectral_windows, row.data_spw_id);
        let cal_spw_id = plan
            .spw_mapping
            .iter()
            .find(|mapping| mapping.data_spw_id == row.data_spw_id)
            .expect("planned data spectral window")
            .calibration_spw_id;
        let Some(cal_spw) = plan
            .calibration_spectral_windows
            .iter()
            .find(|window| window.spw_id == cal_spw_id)
        else {
            return Ok(None);
        };
        let field = plan
            .resolved_nearest_gainfields
            .iter()
            .find(|mapping| mapping.measurement_set_field_id == row.field_id)
            .map_or(-1, |mapping| mapping.calibration_field_id);
        let left = self.temperatures(row, field, row.antenna1, cal_spw, data_spw)?;
        let right = self.temperatures(row, field, row.antenna2, cal_spw, data_spw)?;
        let correlations = &self.correlations[&row.data_desc_id];
        let factor = if row.antenna1 == row.antenna2 {
            1.0
        } else {
            2.0
        };
        let exposure = self
            .exposures
            .as_ref()
            .map_or(1.0, |values| values[row.row_index]);
        let bandwidth = (factor * self.bandwidths[&row.data_spw_id] * exposure) as f32;
        temperature_weights(
            left.as_ref(),
            right.as_ref(),
            correlations,
            data_spw.num_chan as usize,
            bandwidth,
            spectrum,
        )
        .map(Some)
    }

    fn temperatures(
        &self,
        row: &ApplyRowPlan,
        field: i32,
        antenna: i32,
        input: &SpectralWindowPlan,
        output: &SpectralWindowPlan,
    ) -> Result<Option<ComplexSolution>, ApplyExecutionError> {
        let grid = fields::lookup(
            &self.table,
            field,
            input.spw_id,
            antenna,
            (row, row.scan_number),
            output,
            0.,
        )?;
        let Some(grid) = grid else { return Ok(None) };
        let CalibrationGrid::Complex(grid) = grid.grid.as_ref() else {
            return Ok(None);
        };
        let (input, output) = (
            &input.channel_frequencies_hz,
            &output.channel_frequencies_hz,
        );
        let (relative_input, relative_output);
        let (input, output) = if matches!(
            self.table.interp,
            ApplyInterpolationMode::Casa {
                relative_frequency: true,
                ..
            }
        ) {
            relative_input = crate::lookup_frequency::relative_axis(input);
            relative_output = crate::lookup_frequency::relative_axis(output);
            (relative_input.as_slice(), relative_output.as_slice())
        } else {
            (input.as_slice(), output.as_slice())
        };
        ComplexSolution::of(grid)
            .resample_real_array_frequency(input, output, self.table.interp.frequency_mode())
            .map(Some)
    }
}

fn window(windows: &[SpectralWindowPlan], id: i32) -> &SpectralWindowPlan {
    windows
        .iter()
        .find(|window| window.spw_id == id)
        .expect("planned spectral window")
}

fn exposures(ms: &MeasurementSet) -> MsResult<Vec<f64>> {
    use casa_tables::RequiredScalarColumnValues;

    let mut columns = ms
        .main_table()
        .required_scalar_columns_owned(&["EXPOSURE"])?;
    match columns.remove("EXPOSURE") {
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(values),
        _ => Err(MsError::InvalidInput("EXPOSURE must be float64".into())),
    }
}

fn temperature_weights(
    left: Option<&ComplexSolution>,
    right: Option<&ComplexSolution>,
    correlations: &[i32],
    channels: usize,
    bandwidth: f32,
    spectrum: bool,
) -> Result<Vec<ArrayValue>, ApplyExecutionError> {
    let mut weights = Vec::with_capacity(correlations.len());
    let mut sigmas = Vec::with_capacity(correlations.len());
    let shape = IxDyn(&[correlations.len(), if spectrum { channels } else { 0 }]).f();
    let mut weight_spectrum = ArrayD::<f32>::zeros(shape.clone());
    let mut sigma_spectrum = ArrayD::<f32>::zeros(shape);
    for index in 0..correlations.len() {
        let pair = if correlations.len() == 1 {
            Some((0, 0))
        } else {
            bandpass::receptors(correlations, index)
        };
        let (p, q) = pair.ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id: -1,
            correlation_types: correlations
                .iter()
                .map(|code| stokes_name(*code).into())
                .collect(),
        })?;
        if spectrum {
            let mut row_weights = Vec::with_capacity(channels);
            let mut row_sigmas = Vec::with_capacity(channels);
            for channel in 0..channels {
                let u = valid_temperature(left, p, channel).map_or(0.0, f32::recip);
                let v = valid_temperature(right, q, channel).map_or(0.0, f32::recip);
                let weight = bandwidth * (u * v);
                let sigma = weight.sqrt().recip();
                weight_spectrum[[index, channel]] = weight;
                sigma_spectrum[[index, channel]] = sigma;
                row_weights.push(weight);
                row_sigmas.push(sigma);
            }
            weights.push(weights::lower_median(row_weights));
            sigmas.push(weights::lower_median(row_sigmas));
        } else {
            let weight =
                bandwidth * (inverse_mean(left, p, channels) * inverse_mean(right, q, channels));
            weights.push(weight);
            sigmas.push(weight.sqrt().recip());
        }
    }
    let mut output = vec![
        ArrayValue::Float32(ndarray::Array1::from_vec(weights).into_dyn()),
        ArrayValue::Float32(ndarray::Array1::from_vec(sigmas).into_dyn()),
    ];
    if spectrum {
        output.extend([
            ArrayValue::Float32(weight_spectrum),
            ArrayValue::Float32(sigma_spectrum),
        ]);
    }
    Ok(output)
}

fn valid_temperature(
    solution: Option<&ComplexSolution>,
    receptor: usize,
    channel: usize,
) -> Option<f32> {
    let solution = solution?;
    let receptor = receptor.min(solution.receptors.saturating_sub(1));
    let value = solution.value(receptor, channel).re;
    (!solution.flags[receptor * solution.channels + channel] && value >= 0.0).then_some(value)
}

fn inverse_mean(solution: Option<&ComplexSolution>, receptor: usize, channels: usize) -> f32 {
    let (sum, count) = (0..channels)
        .filter_map(|channel| valid_temperature(solution, receptor, channel))
        .fold((0.0_f64, 0), |(sum, count), value| {
            (sum + f64::from(value), count + 1)
        });
    // A missing solution or an all-zero temperature leaves the scalar
    // weight factor at unity. The spectrum calculation uses each channel,
    // so a valid zero channel there has infinite inverse temperature.
    if sum == 0.0 {
        1.0
    } else {
        (f64::from(count) / sum) as f32
    }
}
