// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA creates and initializes CORRECTED_DATA before validating selections.

use super::*;

pub(super) fn corrected_data(
    ms: &mut MeasurementSet,
    observed: &str,
) -> Result<(), ApplyExecutionError> {
    let path = display_ms_path(ms);
    if !ensure_corrected_data_column(ms, observed)
        .map_err(|source| ApplyExecutionError::CreateCorrectedData {
            path: path.clone(),
            source,
        })?
        .created
        || observed == "FLOAT_DATA"
    {
        return Ok(());
    }
    let table_error = |source| ApplyExecutionError::MutateMeasurementSet {
        path: path.clone(),
        source: MsError::from(source),
    };
    let write_error =
        |source: casa_ms::MeasurementSetWriteError| ApplyExecutionError::MutateMeasurementSet {
            path: path.clone(),
            source: MsError::InvalidInput(source.to_string()),
        };
    let mut maximum_elements = 0;
    for row in 0..ms.row_count() {
        let shape = ms
            .main_table()
            .cell_accessor(row, "DATA")
            .and_then(|cell| cell.array_shape())
            .map_err(table_error)?;
        let shape = shape.ok_or_else(|| ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set DATA>".into(),
            shape: vec![],
        })?;
        maximum_elements = maximum_elements.max(shape.iter().product::<usize>());
    }
    // The ordinary bounded create-column path copies DATA even when the
    // mutation has no selected rows; no science correction is published here.
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        vec![],
        vec![MeasurementSetWriteColumnPlan {
            name: "CORRECTED_DATA".into(),
            bytes_per_row: maximum_elements.saturating_mul(8),
            mode: MeasurementSetColumnWriteMode::Create,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
            tile_shape: None,
            create_source_column: Some("DATA".into()),
        }],
        MeasurementSetWriteResources::from_system_memory(2).map_err(write_error)?,
    )
    .map_err(write_error)?;
    MeasurementSetWriteSession::start_selected_row_mutation(ms, plan)
        .map_err(write_error)?
        .finish_mutation()
        .map_err(write_error)?;
    Ok(())
}
