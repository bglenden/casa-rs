// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded, read-only application of a calibration chain during mstransform.

use super::*;

pub(crate) struct PreparedReadOnlyApply {
    plan: ApplyPlan,
    chain: ApplyChain,
    row_positions: HashMap<usize, usize>,
    correlation_types: HashMap<i32, Vec<i32>>,
    observed_column: &'static str,
    has_sigma_spectrum: bool,
    has_weight_column: bool,
}

impl PreparedReadOnlyApply {
    pub(crate) fn new(ms: &MeasurementSet, plan: ApplyPlan) -> Result<Self, ApplyExecutionError> {
        let chain = ApplyChain::load(ms, &plan, false)?;
        let correlation_types = load_correlation_types_by_ddid(ms).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: display_ms_path(ms),
                source,
            }
        })?;
        let row_positions = plan
            .selected_rows
            .iter()
            .enumerate()
            .map(|(position, row)| (row.row_index, position))
            .collect();
        let first = plan.selected_rows.iter().map(|row| row.row_index).min();
        let table = ms.main_table();
        Ok(Self {
            plan,
            chain,
            row_positions,
            correlation_types,
            observed_column: apply_observed_column(ms, false),
            has_sigma_spectrum: weights::has_spectrum(table, first, "SIGMA_SPECTRUM")?,
            has_weight_column: table
                .schema()
                .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM")),
        })
    }

    pub(crate) fn visit_rows(
        &self,
        table: &Table,
        rows: &[usize],
        mut visit: impl FnMut(ArrayValue, ArrayValue, PlotWeights),
    ) -> Result<(), ApplyExecutionError> {
        let path = table
            .path()
            .map_or_else(|| "<memory>".into(), |path| path.display().to_string());
        let arrays = SelectedArrayReader { table, path: &path };
        let flags = arrays.read("FLAG", rows)?;
        let data = arrays.read(self.observed_column, rows)?;
        let sigmas = arrays.read("SIGMA", rows)?;
        let mut geometry = RowGeometryCache::default();
        for (((&row, flags), data), sigma) in rows.iter().zip(flags).zip(data).zip(sigmas) {
            let row_plan = self
                .row_positions
                .get(&row)
                .map(|&position| &self.plan.selected_rows[position])
                .ok_or_else(|| ApplyExecutionError::MutateMeasurementSet {
                    path: path.clone(),
                    source: MsError::InvalidInput(format!(
                        "calibration did not plan input row {row}"
                    )),
                })?;
            let flags = required_prefetched_array(flags, row, "FLAG", &path)?;
            let data = required_prefetched_array(data, row, self.observed_column, &path)?;
            let data = match data {
                ArrayValue::Float32(values) => complex_visibility_from_float(&values),
                data => data,
            };
            let sigma = required_prefetched_array(sigma, row, "SIGMA", &path)?;
            let (corrected, flags, weights) =
                self.apply_row(row_plan, flags, &data, sigma, &arrays, &mut geometry)?;
            visit(corrected, flags, weights);
        }
        Ok(())
    }

    fn apply_row(
        &self,
        row: &ApplyRowPlan,
        flags: ArrayValue,
        data: &ArrayValue,
        sigma: ArrayValue,
        arrays: &SelectedArrayReader<'_>,
        geometry: &mut RowGeometryCache,
    ) -> Result<(ArrayValue, ArrayValue, PlotWeights), ApplyExecutionError> {
        let layout = self
            .correlation_types
            .get(&row.data_desc_id)
            .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id: row.data_desc_id,
                correlation_types: Vec::new(),
            })?;
        let indices = self
            .plan
            .channel_selections
            .get(&row.data_spw_id)
            .map(|selection| selection.indices.as_slice());
        let mut scalar = weights::from_sigma(sigma, "SIGMA")?;
        let spectrum = self.weight_spectrum(arrays, row.row_index, &scalar, data.shape())?;
        if let Some(weights) = &spectrum {
            scalar = weights::scalar_medians(weights, indices).unwrap_or(scalar);
        }
        let result = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types: layout,
                data,
                original_flags: Some(&flags),
                original_weight: Some(&scalar),
                original_weight_spectrum: spectrum.as_ref(),
                has_weight_spectrum: spectrum.is_some(),
                preserved_corrected_data: None,
                preserved_weight_spectrum: None,
                channel_grid: None,
                forward_model: false,
                // VisCal::correct2 skips an unavailable term and continues
                // with the rest of the callibrary, leaving flags to those terms.
                skip_unavailable_terms: true,
                trial: false,
                parang_calwt: false,
            },
            &self.plan,
            &self.chain,
            Some(geometry),
            None,
            None,
        )?;
        let spectrum = result.updated_weight_spectrum.or(spectrum);
        let mut scalar = result.updated_weight.unwrap_or(scalar);
        if let Some(weights) = &spectrum {
            scalar = weights::scalar_medians(weights, indices).unwrap_or(scalar);
        }
        Ok((
            result.corrected_data,
            result.updated_flags.unwrap_or(flags),
            PlotWeights { scalar, spectrum },
        ))
    }

    fn weight_spectrum(
        &self,
        arrays: &SelectedArrayReader<'_>,
        row: usize,
        scalar: &ArrayValue,
        shape: &[usize],
    ) -> Result<Option<ArrayValue>, ApplyExecutionError> {
        if !self.has_weight_column
            || arrays
                .table
                .array_shape_uncached(row, "WEIGHT_SPECTRUM")
                .map_err(|source| arrays.error(source))?
                .is_none()
        {
            return Ok(None);
        }
        let value = if self.has_sigma_spectrum {
            let column = "SIGMA_SPECTRUM";
            let selected = arrays.read(column, &[row])?.pop().flatten();
            weights::from_sigma(
                required_prefetched_array(selected, row, column, arrays.path)?,
                column,
            )?
        } else {
            weights::broadcast_spectrum(scalar, shape)?
        };
        Ok(Some(value))
    }
}

struct SelectedArrayReader<'a> {
    table: &'a Table,
    path: &'a str,
}

impl SelectedArrayReader<'_> {
    fn read(
        &self,
        column: &str,
        rows: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, ApplyExecutionError> {
        self.table
            .column_accessor(column)
            .and_then(|accessor| accessor.array_cells_owned_uncached(rows))
            .map_err(|source| self.error(source))
    }

    fn error(&self, source: TableError) -> ApplyExecutionError {
        ApplyExecutionError::MutateMeasurementSet {
            path: self.path.into(),
            source: MsError::from(source),
        }
    }
}
