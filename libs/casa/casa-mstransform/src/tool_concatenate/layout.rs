// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admission checks for fixed-shape visibility storage.

use casa_core::{ArrayValue, Value};
use casa_tables::{ArrayShapeContract, ColumnType, Table, TableOptions};

use casa_ms::subtables::SubTable;
use casa_ms::{MeasurementSet, MsError, MsResult};

const VISIBILITY_COLUMNS: &[&str] = &[
    "FLAG_CATEGORY",
    "DATA",
    "FLAG",
    "SIGMA_SPECTRUM",
    "WEIGHT_SPECTRUM",
    "CORRECTED_WEIGHT_SPECTRUM",
    "FLOAT_DATA",
    "CORRECTED_DATA",
    "MODEL_DATA",
    "LAG_DATA",
    "SIGMA",
    "WEIGHT",
    "VIDEO_POINT",
];

pub(super) fn check(target: &MeasurementSet, donor: &MeasurementSet) -> MsResult<()> {
    if donor.row_count() == 0 {
        return Ok(());
    }
    let Some(shape) = fixed_shape(target.main_table())? else {
        return Ok(());
    };
    let description = donor.data_description()?;
    let spectral = donor.spectral_window()?;
    let polarization = donor.polarization()?;
    for index in 0..description.table().row_count() {
        let spw_id = usize::try_from(description.spectral_window_id(index)?)
            .map_err(|_| MsError::InvalidInput("negative spectral-window ID".into()))?;
        let pol_id = usize::try_from(description.polarization_id(index)?)
            .map_err(|_| MsError::InvalidInput("negative polarization ID".into()))?;
        let channels = spectral.num_chan(spw_id)?;
        let correlations = polarization.num_corr(pol_id)?;
        for (axis, actual, label) in [(1, channels, "channels"), (0, correlations, "correlations")]
        {
            if shape
                .get(axis)
                .is_some_and(|&expected| actual as usize != expected)
            {
                return Err(MsError::InvalidInput(format!(
                    "cannot concatenate this MeasurementSet as it has a different number of {label}"
                )));
            }
        }
    }
    Ok(())
}

fn fixed_shape(main: &Table) -> MsResult<Option<&[usize]>> {
    let Some(schema) = main.schema() else {
        return Ok(None);
    };
    let fixed = |name: &str| {
        if !VISIBILITY_COLUMNS.contains(&name) {
            return None;
        }
        let ColumnType::Array(ArrayShapeContract::Fixed { shape }) =
            schema.column(name)?.column_type()
        else {
            return None;
        };
        Some(shape.as_slice())
    };
    if !VISIBILITY_COLUMNS.iter().any(|name| fixed(name).is_some()) {
        return Ok(None);
    }
    let description = match main.stored_table_description()? {
        Some(description) => Some(description),
        None => main
            .simple_reference_root()?
            .map(|path| Table::open(TableOptions::new(path))?.stored_table_description())
            .transpose()?
            .flatten(),
    };
    let Some(description) = description else {
        return Ok(None);
    };
    for field in description.private_keywords.fields() {
        if field.name.starts_with("Hypercolumn_")
            && let Value::Record(record) = &field.value
            && let Some(Value::Array(ArrayValue::String(columns))) = record.get("data")
            && let Some(shape) = columns.iter().find_map(|name| fixed(name))
        {
            return Ok(Some(shape));
        }
    }
    Ok(None)
}
