// SPDX-License-Identifier: LGPL-3.0-or-later
//! Implicit model cells for MeasurementSet PlotMS.

use std::collections::HashMap;
use std::sync::Arc;

use casa_types::ArrayValue;
use ndarray::Array2;

use super::selected_i32_values;
use crate::{MeasurementSet, initial_model_visibility, scratch_columns::virtual_model_fields};

/// Share one unit point model cell per selected data description.
pub(super) fn default_cells(
    ms: &MeasurementSet,
    rows: &[usize],
) -> Result<Vec<Arc<ArrayValue>>, String> {
    let main = ms.main_table();
    let model_fields = virtual_model_fields(ms);
    if !model_fields.is_empty()
        && selected_i32_values(main, "FIELD_ID", rows)?
            .iter()
            .any(|field| model_fields.contains(field))
    {
        return Err("PlotMS virtual MeasurementSet models are pending #1032".into());
    }

    let descriptions = ms.data_description().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let index =
        |value: i32| usize::try_from(value).map_err(|_| format!("invalid MS index {value}"));
    let mut cells = HashMap::<i32, Arc<ArrayValue>>::new();
    selected_i32_values(main, "DATA_DESC_ID", rows)?
        .into_iter()
        .map(|ddid| {
            if let Some(cell) = cells.get(&ddid) {
                return Ok(Arc::clone(cell));
            }
            let description = index(ddid)?;
            let spw_id = descriptions
                .spectral_window_id(description)
                .map_err(|error| error.to_string())?;
            let pol_id = descriptions
                .polarization_id(description)
                .map_err(|error| error.to_string())?;
            let channels = spectral_window
                .num_chan(index(spw_id)?)
                .map_err(|error| error.to_string())?;
            let correlations = polarization
                .corr_type(index(pol_id)?)
                .map_err(|error| error.to_string())?;
            let values =
                Array2::from_shape_fn((correlations.len(), index(channels)?), |(corr, _)| {
                    initial_model_visibility(correlations[corr])
                });
            let cell = Arc::new(ArrayValue::Complex32(values.into_dyn()));
            cells.insert(ddid, Arc::clone(&cell));
            Ok(cell)
        })
        .collect()
}
