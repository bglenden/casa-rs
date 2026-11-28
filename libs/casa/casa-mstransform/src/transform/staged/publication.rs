// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct OutputRows<'a> {
    pub(super) input_rows: &'a [usize],
    pub(super) input_ddids: &'a [i32],
    pub(super) field_ids: &'a [i32],
    pub(super) field_id_map: &'a Option<BTreeMap<i32, i32>>,
    pub(super) selected_output_rows: usize,
    pub(super) output_rows: usize,
    pub(super) copies_per_row: usize,
    pub(super) ddi_offset: i32,
    pub(super) writes_rows: bool,
}

pub(super) struct OutputSpectra<'a> {
    pub(super) regrid: Option<&'a regrid::Plan>,
    pub(super) compact_metadata: &'a CompactSpectralMetadata,
    pub(super) channel_selection: &'a BTreeMap<i32, TransformChannelSelection>,
    pub(super) channel_bins: &'a BTreeMap<i32, Vec<Vec<usize>>>,
    pub(super) average_metadata: &'a Option<BTreeMap<i32, Vec<spectral_metadata::ChannelWindow>>>,
    pub(super) combined_grid: Option<&'a combine::Grid>,
    pub(super) widths: &'a BTreeMap<i32, usize>,
    pub(super) touched_spws: BTreeSet<i32>,
    pub(super) splitting: bool,
}

pub(super) struct OutputWrite<'a> {
    pub(super) main: &'a mut Table,
    pub(super) session: MeasurementSetWriteSession,
    pub(super) overrides: &'a mut ColumnOverrides,
    pub(super) visibility: &'a VisibilityColumns,
    pub(super) spectra_output: Vec<&'static str>,
}

pub(super) fn publish_transform(
    request: &MsTransformRequest,
    rows: OutputRows<'_>,
    spectra: OutputSpectra<'_>,
    write: OutputWrite<'_>,
    options: &MsTransformTaskOptions,
    budget: MsSelectionIoBudget,
    started_at: Instant,
) -> Result<MsTransformReport, MsTransformError> {
    let OutputRows {
        input_rows: output_input_rows,
        input_ddids: output_input_ddids,
        field_ids: selected_field_ids_for_metadata,
        field_id_map,
        selected_output_rows,
        output_rows,
        copies_per_row,
        ddi_offset,
        writes_rows,
    } = rows;
    let OutputSpectra {
        regrid,
        compact_metadata,
        channel_selection,
        channel_bins,
        average_metadata,
        combined_grid,
        widths,
        touched_spws,
        splitting,
    } = spectra;
    let OutputWrite {
        main: output_main,
        session: mut write_session,
        overrides: output_column_overrides,
        visibility,
        spectra_output,
    } = write;
    let stage_started_at = Instant::now();
    for column in visibility.copies() {
        output_column_overrides.insert_deferred(column.output.name());
    }
    output_column_overrides.insert_deferred("FLAG");
    output_column_overrides.insert_deferred("FLAG_CATEGORY");
    output_column_overrides.insert_deferred("UVW");
    output_column_overrides.insert_deferred("WEIGHT");
    output_column_overrides.insert_deferred("SIGMA");
    for name in spectra_output {
        output_column_overrides.insert_deferred(name);
    }
    for (index, (&input_row, ddid)) in output_input_rows[..selected_output_rows]
        .iter()
        .zip(&output_input_ddids[..selected_output_rows])
        .enumerate()
    {
        if index.is_multiple_of(4096) {
            options.check_cancelled()?;
        }
        let compact = regrid
            .as_ref()
            .and_then(|plan| plan.output_description(input_row))
            .or_else(|| compact_metadata.ddid_map.get(ddid).copied())
            .ok_or_else(|| MsTransformError::SpectralMetadata {
                path: request.output_ms.display().to_string(),
                reason: format!(
                    "selected DATA_DESC_ID {ddid} was not present in compacted metadata"
                ),
            })?;
        for part in 0..copies_per_row {
            let id = compact
                .checked_mul(copies_per_row as i32)
                .and_then(|id| id.checked_add(part as i32))
                .and_then(|id| id.checked_add(ddi_offset))
                .ok_or_else(|| {
                    MsTransformError::Spectrum(
                        "Output data-description ID exceeds the index limit.".into(),
                    )
                })?;
            write_session
                .push_scalar_row("DATA_DESC_ID", Some(ScalarValue::Int32(id)))
                .map_err(|error| MsTransformError::WriteMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    reason: error.to_string(),
                })?;
        }
    }
    for (index, field_id) in selected_field_ids_for_metadata[..selected_output_rows]
        .iter()
        .enumerate()
    {
        if index.is_multiple_of(4096) {
            options.check_cancelled()?;
        }
        let compact = match &field_id_map {
            Some(ids) => ids.get(field_id).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!(
                        "selected FIELD_ID {field_id} was not present in compacted FIELD metadata"
                    ),
                }
            })?,
            None => *field_id,
        };
        for _ in 0..copies_per_row {
            write_session
                .push_scalar_row("FIELD_ID", Some(ScalarValue::Int32(compact)))
                .map_err(|error| MsTransformError::WriteMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    reason: error.to_string(),
                })?;
        }
    }
    options.check_cancelled()?;
    let output_options = measurement_set_table_options(&request.output_ms);
    let write_telemetry = if writes_rows {
        let output_bindings = measurement_set_main_table_bindings(output_main);
        write_session
            .save_table_and_finish(
                output_main,
                output_options,
                &output_bindings,
                output_column_overrides,
            )
            .map_err(|error| MsTransformError::WriteMeasurementSet {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            })?
    } else {
        // Empty MAIN has no visibilities from which to define tiled hypercubes.
        output_main
            .save(output_options.with_data_manager(DataManagerKind::StandardStMan))
            .map_err(|error| MsTransformError::WriteMeasurementSet {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            })?;
        MeasurementSetWriteTelemetry::default()
    };
    maybe_log_transform_progress(
        "save_output_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    options.check_cancelled()?;
    let stage_started_at = Instant::now();
    if let Some(regrid) = &regrid {
        regrid.write_metadata(request, compact_metadata)?;
    } else if combined_grid.is_some() {
        // The combined grid was written with the compact spectral metadata.
    } else if splitting || average_metadata.is_some() || normalized_width(request.width) == 1 {
        spectral_metadata::write_channel_selection(
            &request.output_ms,
            &compact_metadata.spw_map,
            channel_selection,
            average_metadata.as_ref(),
            splitting.then_some(widths),
        )?;
    } else {
        update_spectral_window_metadata(&compact_metadata.channel_bins, &request.output_ms)?;
    }
    if ddi_offset > 0 {
        output_layout::pad_ddids(
            &request.output_ms,
            ddi_offset as usize,
            budget.available_bytes / budget.maximum_live_blocks,
        )?;
    }
    maybe_log_transform_progress(
        "update_spectral_window",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    tracing::info!(
        input_ms = %request.input_ms.display(),
        output_ms = %request.output_ms.display(),
        rows = output_rows,
        write_bytes = write_telemetry.bytes_written,
        write_peak_planned_resident_bytes = write_telemetry.maximum_resident_bytes,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "mstransform completed"
    );
    Ok(MsTransformReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: output_rows,
        source_column: visibility.source_names(),
        output_column: visibility.output_names(),
        spw: request.spw.clone(),
        spectral_window_ids: if combined_grid.is_some() {
            vec![0]
        } else {
            touched_spws.into_iter().collect()
        },
        output_channels_by_spw: if let Some(grid) = combined_grid {
            BTreeMap::from([(0, grid.frequencies.len())])
        } else {
            channel_bins
                .iter()
                .map(|(spw, bins)| (*spw, bins.len()))
                .collect()
        },
        width: normalized_width(request.width),
        elapsed_ns: started_at.elapsed().as_nanos() as u64,
        retained_mms: None,
        write_telemetry,
    })
}
