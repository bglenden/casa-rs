// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct ExecutionRowResult {
    pub(super) corrected_data: ArrayValue,
    pub(super) updated_flags: Option<ArrayValue>,
    pub(super) updated_weight: Option<ArrayValue>,
    pub(super) updated_weight_spectrum: Option<ArrayValue>,
    pub(super) newly_flagged_samples: usize,
    pub(super) row_became_fully_flagged: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ExecutionRowInputs<'a> {
    pub(super) correlation_types: &'a [i32],
    pub(super) data: &'a ArrayValue,
    pub(super) original_flags: Option<&'a ArrayValue>,
    pub(super) original_weight: Option<&'a ArrayValue>,
    pub(super) original_weight_spectrum: Option<&'a ArrayValue>,
    pub(super) has_weight_spectrum: bool,
    pub(super) preserved_corrected_data: Option<&'a ArrayValue>,
    pub(super) preserved_weight_spectrum: Option<&'a ArrayValue>,
    pub(super) channel_grid: Option<DataChannelGrid<'a>>,
    pub(super) forward_model: bool,
    pub(super) skip_unavailable_terms: bool,
    pub(super) trial: bool,
    pub(super) parang_calwt: bool,
}

pub(super) struct PrefetchedExecutionRowInputs {
    pub(super) trial: bool,
    pub(super) parang_calwt: bool,
    pub(super) data: ArrayValue,
    pub(super) original_flags: Option<ArrayValue>,
    pub(super) original_weight: Option<ArrayValue>,
    pub(super) original_weight_spectrum: Option<ArrayValue>,
    pub(super) preserved_corrected_data: Option<ArrayValue>,
    pub(super) preserved_weight_spectrum: Option<ArrayValue>,
}

pub(super) fn maximum_visibility_cells(
    plan: &ApplyPlan,
    correlation_types_by_ddid: &HashMap<i32, Vec<i32>>,
) -> Result<usize, ApplyExecutionError> {
    plan.selected_rows
        .iter()
        .try_fold(0_usize, |maximum_elements, row| {
            let correlations = correlation_types_by_ddid
                .get(&row.data_desc_id)
                .map(Vec::len)
                .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: Vec::new(),
                })?;
            let channels = plan
                .measurement_set_spectral_windows
                .iter()
                .find(|spw| spw.spw_id == row.data_spw_id)
                .and_then(|spw| usize::try_from(spw.num_chan).ok())
                .ok_or_else(|| ApplyExecutionError::UnsupportedParameterShape {
                    path: format!("<measurement-set SPW {}>", row.data_spw_id),
                    shape: Vec::new(),
                })?;
            let elements = correlations.checked_mul(channels).ok_or_else(|| {
                ApplyExecutionError::UnsupportedParameterShape {
                    path: "<measurement-set row byte overflow>".to_string(),
                    shape: vec![correlations, channels],
                }
            })?;
            Ok::<_, ApplyExecutionError>(maximum_elements.max(elements))
        })
}

pub(super) fn required_prefetched_array(
    value: Option<ArrayValue>,
    row_index: usize,
    column: &str,
    path: &str,
) -> Result<ArrayValue, ApplyExecutionError> {
    value.ok_or_else(|| ApplyExecutionError::MutateMeasurementSet {
        path: path.to_string(),
        source: MsError::from(TableError::ColumnNotFound {
            row_index,
            column: column.to_string(),
        }),
    })
}

#[derive(Debug, Default)]
pub(super) struct ApplyRowComputeProfile {
    pub(super) rows: usize,
    pub(super) table_applications: usize,
    pub(super) setup_ns: u64,
    pub(super) table_lookup_ns: u64,
    pub(super) row_dependent_grid_ns: u64,
    pub(super) row_dependent_fast_pairs: usize,
    pub(super) row_dependent_materialized_pairs: usize,
    pub(super) fast_gain_apply_ns: u64,
    pub(super) generic_sample_apply_ns: u64,
    pub(super) parallactic_angle_ns: u64,
    pub(super) weight_finalize_ns: u64,
}

impl ApplyRowComputeProfile {
    pub(super) fn add_elapsed(bucket: &mut u64, started_at: Option<Instant>) {
        if let Some(started_at) = started_at {
            *bucket += started_at.elapsed().as_nanos() as u64;
        }
    }

    pub(super) fn detail_string(&self) -> String {
        format!(
            "rows={} table_apps={} setup={:.3}s lookup={:.3}s row_dependent_grid={:.3}s row_dependent_fast_pairs={} row_dependent_materialized_pairs={} fast_gain={:.3}s generic_sample={:.3}s parang={:.3}s weight_finalize={:.3}s",
            self.rows,
            self.table_applications,
            self.setup_ns as f64 / 1_000_000_000.0,
            self.table_lookup_ns as f64 / 1_000_000_000.0,
            self.row_dependent_grid_ns as f64 / 1_000_000_000.0,
            self.row_dependent_fast_pairs,
            self.row_dependent_materialized_pairs,
            self.fast_gain_apply_ns as f64 / 1_000_000_000.0,
            self.generic_sample_apply_ns as f64 / 1_000_000_000.0,
            self.parallactic_angle_ns as f64 / 1_000_000_000.0,
            self.weight_finalize_ns as f64 / 1_000_000_000.0,
        )
    }
}
