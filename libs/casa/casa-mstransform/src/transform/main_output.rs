// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn materialize_empty_main_table(
    input_path: &Path,
    row_count: usize,
    output_path: &Path,
    visibility: &VisibilityColumns,
    grammar: RowGrammar,
    spectra: &SpectrumProjection,
    legacy_partition: bool,
) -> Result<Table, MsTransformError> {
    let mut table = Table::open_metadata_for_construction(TableOptions::new(input_path), row_count)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    if grammar == RowGrammar::Transform {
        table.keywords_mut().remove("SORTED_TABLE");
        table.keywords_mut().remove("SORT_COLUMNS");
        table.set_info(TableInfo {
            table_type: "Measurement Set".into(),
            sub_type: "UVFITS".into(),
            readme: vec![
                "This is a MeasurementSet Table holding measurements from a Telescope".into(),
                "This is a measurement set Table holding astronomical observations".into(),
            ],
        });
    }
    let renamed_keywords = visibility
        .copies()
        .iter()
        .find(|copy| {
            copy.source != copy.output
                && (grammar != RowGrammar::Transform
                    || copy.source == OptionalMainColumn::CorrectedData)
        })
        .and_then(|copy| table.column_keywords(copy.source.name()).cloned());
    let projected_columns = table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| {
            let name = column.name();
            (legacy_partition
                && !casa_ms::schema::main_table::REQUIRED_COLUMNS
                    .iter()
                    .any(|column| column.name == name)
                && !visibility.contains_output(name)
                && !spectra.output.contains(&name))
                || (is_visibility_column(name) && !visibility.contains_output(name))
                || (MAIN_SPECTRA.contains(&name) && !spectra.output.contains(&name))
        })
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    for column in projected_columns {
        table
            .remove_column_metadata_for_construction(&column)
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: output_path.display().to_string(),
                source: Box::new(source),
            })?;
    }
    for column in [
        OptionalMainColumn::WeightSpectrum,
        OptionalMainColumn::SigmaSpectrum,
    ] {
        if spectra.output.contains(&column.name())
            && !table
                .schema()
                .is_some_and(|schema| schema.contains_column(column.name()))
        {
            table
                .add_column(casa_ms::column_schema(column.column_def()), None)
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: output_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
    }
    let mut arrays = vec![
        "FLAG",
        "FLAG_CATEGORY",
        "WEIGHT",
        "SIGMA",
        "WEIGHT_SPECTRUM",
        "SIGMA_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ];
    for copy in visibility.copies() {
        let name = copy.output.name();
        arrays.push(name);
        if !table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
        {
            table
                .add_column(casa_ms::column_schema(copy.output.column_def()), None)
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: output_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
        if copy.source != copy.output
            && let Some(source) = &renamed_keywords
        {
            let mut keywords = table.column_keywords(name).cloned().unwrap_or_default();
            for field in source.fields() {
                if grammar != RowGrammar::Transform
                    || matches!(field.value, Value::Scalar(ScalarValue::String(_)))
                {
                    keywords.upsert(field.name.clone(), field.value.clone());
                }
            }
            table.set_column_keywords(name, keywords);
        } else if grammar == RowGrammar::Transform
            && copy.output == OptionalMainColumn::ModelData
            && visibility.copies().len() > 1
        {
            // CASA constructs the independent MODEL_DATA column without input keywords.
            table.set_column_keywords(name, RecordValue::default());
        }
    }
    table
        .widen_array_metadata_for_construction(&arrays)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(table)
}

pub(super) fn gather_selected_main_column_overrides(
    (table, output): (&Table, &Table),
    selected_rows: &[usize],
    (output_path, options): (&Path, &MsTransformTaskOptions),
    batch_rows: usize,
    write_session: &mut MeasurementSetWriteSession,
    row_id_maps: &selection_metadata::RowIdMaps,
    (averaging_time, averaging_polarization, combining): (bool, bool, bool),
) -> Result<ColumnOverrides, MsTransformError> {
    let schema = output
        .schema()
        .ok_or_else(|| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(TableError::Schema(
                "MAIN table is missing schema metadata".to_string(),
            )),
        })?;
    let mut columns = ColumnOverrides::for_row_count(selected_rows.len());
    for column in schema.columns() {
        options.check_cancelled()?;
        let column_started_at = Instant::now();
        let name = column.name();
        if is_deferred_visibility_column(name) {
            continue;
        }
        if (averaging_time && TIME_OUTPUT_SCALARS.contains(&name))
            || (averaging_polarization && name == "FLAG_ROW")
            || (combining && matches!(name, "EXPOSURE" | "FLAG_ROW"))
        {
            continue;
        }
        match column.column_type() {
            ColumnType::Scalar => {
                if is_remapped_scalar_column(name) {
                    continue;
                }
                if column.data_type().and_then(streamed_scalar_type).is_some() {
                    let id_map = row_id_maps.for_column(name);
                    let array_selection = row_id_maps.array.as_ref().filter(|_| name == "ARRAY_ID");
                    for rows in selected_rows.chunks(batch_rows.max(1)) {
                        options.check_cancelled()?;
                        let values = table
                            .column_accessor(name)
                            .and_then(|column| column.scalar_cells_owned_for_rows(rows))
                            .map_err(|source| MsTransformError::MutateMeasurementSet {
                                path: output_path.display().to_string(),
                                source: Box::new(source),
                            })?;
                        for (row, value) in rows.iter().zip(values) {
                            let value = match (id_map, array_selection) {
                                (Some(ids), _) => {
                                    let old_id = scalar_i32(value.as_ref(), name, *row)?;
                                    let new_id = ids.get(&old_id).ok_or_else(|| {
                                        MsTransformError::SpectralMetadata {
                                            path: output_path.display().to_string(),
                                            reason: format!("{name} {old_id} is absent from the selected metadata"),
                                        }
                                    })?;
                                    Some(ScalarValue::Int32(*new_id))
                                }
                                (_, Some(selection)) => {
                                    let old_id = scalar_i32(value.as_ref(), name, *row)?;
                                    let new_id = selection.reindex(old_id).map_err(|source| {
                                        MsTransformError::SpectralMetadata {
                                            path: output_path.display().to_string(),
                                            reason: source.to_string(),
                                        }
                                    })?;
                                    Some(ScalarValue::Int32(new_id))
                                }
                                _ => value,
                            };
                            write_session
                                .push_scalar_row(name, value)
                                .map_err(|source| MsTransformError::WriteMeasurementSet {
                                    path: output_path.display().to_string(),
                                    reason: source.to_string(),
                                })?;
                        }
                        #[cfg(feature = "transform-test-hooks")]
                        test_hooks::notify(test_hooks::Checkpoint::ScalarChunkStaged);
                    }
                } else {
                    let values = table
                        .column_accessor(name)
                        .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
                        .map_err(|source| MsTransformError::MutateMeasurementSet {
                            path: output_path.display().to_string(),
                            source: Box::new(source),
                        })?;
                    columns.insert_values(
                        name.to_string(),
                        values
                            .into_iter()
                            .map(|value| value.map(Value::Scalar))
                            .collect(),
                    );
                }
            }
            ColumnType::Array(_) => {
                let values = table
                    .column_accessor(name)
                    .and_then(|column| column.array_cells_owned(selected_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: output_path.display().to_string(),
                        source: Box::new(source),
                    })?;
                columns.insert_values(
                    name.to_string(),
                    values
                        .into_iter()
                        .map(|value| value.map(Value::Array))
                        .collect(),
                );
            }
            ColumnType::Record => {
                let values = selected_rows
                    .iter()
                    .copied()
                    .map(|input_row| {
                        table
                            .column_accessor(name)
                            .and_then(|column| column.record_cell(input_row))
                            .map(|value| Some(Value::Record(value)))
                            .map_err(|source| MsTransformError::MutateMeasurementSet {
                                path: output_path.display().to_string(),
                                source: Box::new(source),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                columns.insert_values(name.to_string(), values);
            }
        }
        maybe_log_transform_progress(
            &format!("load_main_column_overrides/column/{name}"),
            column_started_at.elapsed(),
            column_started_at.elapsed(),
        );
    }
    Ok(columns)
}

pub(super) fn streamed_scalar_type(primitive_type: PrimitiveType) -> Option<StreamedScalarType> {
    match primitive_type {
        PrimitiveType::Bool => Some(StreamedScalarType::Bool),
        PrimitiveType::Int32 => Some(StreamedScalarType::Int32),
        PrimitiveType::Float32 => Some(StreamedScalarType::Float32),
        PrimitiveType::Float64 => Some(StreamedScalarType::Float64),
        _ => None,
    }
}

pub(super) fn transform_scalar_column_plans(table: &Table) -> Vec<MeasurementSetScalarColumnPlan> {
    table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| {
            *column.column_type() == ColumnType::Scalar
                && !is_deferred_visibility_column(column.name())
        })
        .filter_map(|column| {
            Some(MeasurementSetScalarColumnPlan {
                name: column.name().to_string(),
                value_type: streamed_scalar_type(column.data_type()?)?,
            })
        })
        .collect()
}

pub(super) fn is_remapped_scalar_column(column: &str) -> bool {
    matches!(column, "DATA_DESC_ID" | "FIELD_ID")
}

pub(super) fn is_deferred_visibility_column(column: &str) -> bool {
    is_visibility_column(column)
        || [
            "FLAG",
            "FLAG_CATEGORY",
            "UVW",
            "WEIGHT",
            "SIGMA",
            "WEIGHT_SPECTRUM",
            "SIGMA_SPECTRUM",
            "CORRECTED_WEIGHT_SPECTRUM",
        ]
        .contains(&column)
}

pub(super) fn is_visibility_column(column: &str) -> bool {
    matches!(
        column,
        "DATA" | "CORRECTED_DATA" | "MODEL_DATA" | "FLOAT_DATA" | "LAG_DATA"
    )
}

pub(super) struct CompactSpectralMetadata {
    pub(super) ddid_map: BTreeMap<i32, i32>,
    pub(super) spw_map: BTreeMap<i32, i32>,
    pub(super) channel_bins: BTreeMap<i32, Vec<Vec<usize>>>,
}

pub(super) fn preserve_spectral_ids(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    ddid_to_spw: &BTreeMap<i32, i32>,
) -> Result<CompactSpectralMetadata, MsTransformError> {
    let name = SubtableId::SpectralWindow.name();
    let windows = input
        .spectral_window()
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: request.input_ms.join(name).display().to_string(),
            reason: source.to_string(),
        })?;
    let source_path = windows
        .table()
        .path()
        .ok_or_else(|| MsTransformError::SpectralMetadata {
            path: request.input_ms.join(name).display().to_string(),
            reason: "resolved spectral window table has no persisted path".into(),
        })?;
    let rows = (0..windows.row_count()).collect::<Vec<_>>();
    rewrite_table_rows_with_variable_arrays(
        source_path,
        &request.output_ms.join(name),
        &rows,
        &["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"],
        |_, row| Ok(row),
    )?;
    Ok(CompactSpectralMetadata {
        ddid_map: ddid_to_spw.keys().map(|&id| (id, id)).collect(),
        spw_map: channel_bins.keys().map(|&id| (id, id)).collect(),
        channel_bins: channel_bins.clone(),
    })
}

pub(super) fn compact_spectral_subtables(
    input: &MeasurementSet,
    output_ms: &Path,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    selection: &SpwSelection,
    correlations: Option<&CorrelationSelection>,
    grammar: RowGrammar,
) -> Result<CompactSpectralMetadata, MsTransformError> {
    let selected_spws = if grammar == RowGrammar::Transform {
        selected_spw_order(channel_bins, selection)
    } else {
        channel_bins.keys().copied().collect::<Vec<_>>()
    };
    let spw_map = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    let compact_channel_bins = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| {
            (
                new_id as i32,
                channel_bins.get(old_id).cloned().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    rewrite_table_rows_with_variable_arrays(
        subtable_copy::input_subtable_path(input, SubtableId::SpectralWindow)?,
        &output_ms.join(SubtableId::SpectralWindow.name()),
        &selected_spws
            .iter()
            .map(|spw| *spw as usize)
            .collect::<Vec<_>>(),
        &["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"],
        |_, row| Ok(spectral_metadata::remap_associations(row, &spw_map)),
    )?;

    // CASA uses getSPWDDIDList in expression order, including repeated
    // entries. MAIN refers to the last such entry. An omitted spw is '*'.
    let ordered_ddids = selection
        .data_description_ids()
        .iter()
        .map(|&id| id as i32)
        .filter(|&id| correlations.is_none_or(|selection| selection.axes(id).is_some()))
        .collect::<Vec<_>>();
    let ddid_map = ordered_ddids
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    let descriptions = subtable_copy::input_subtable_path(input, SubtableId::DataDescription)?;
    rewrite_selected_table_rows(
        descriptions,
        &output_ms.join(SubtableId::DataDescription.name()),
        &ordered_ddids
            .iter()
            .map(|ddid| *ddid as usize)
            .collect::<Vec<_>>(),
        |old_row, mut row| {
            let old_ddid = old_row as i32;
            let old_spw = ddid_to_spw.get(&old_ddid).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: descriptions.display().to_string(),
                    reason: format!("DATA_DESCRIPTION row {old_ddid} has no SPW mapping"),
                }
            })?;
            let new_spw = spw_map.get(&old_spw).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: output_ms
                        .join(SubtableId::DataDescription.name())
                        .display()
                        .to_string(),
                    reason: format!(
                        "DATA_DESCRIPTION row {old_ddid} references unselected SPW {old_spw}"
                    ),
                }
            })?;
            row.upsert(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(new_spw)),
            );
            Ok(row)
        },
    )?;

    Ok(CompactSpectralMetadata {
        ddid_map,
        spw_map,
        channel_bins: compact_channel_bins,
    })
}

pub(super) fn selected_field_ids_for_rows(
    table: &Table,
    selected_rows: &[usize],
    output_path: &Path,
) -> Result<Vec<i32>, MsTransformError> {
    table
        .column_accessor("FIELD_ID")
        .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?
        .into_iter()
        .zip(selected_rows.iter().copied())
        .map(|(value, row_index)| scalar_i32(value.as_ref(), "FIELD_ID", row_index))
        .collect()
}

pub(super) fn rewrite_selected_table_rows<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
    rewrite: F,
) -> Result<(), MsTransformError>
where
    F: FnMut(usize, RecordValue) -> Result<RecordValue, MsTransformError>,
{
    rewrite_table_rows_with_variable_arrays(
        source_path,
        destination_path,
        selected_rows,
        &[],
        rewrite,
    )
}

pub(super) fn rewrite_table_rows_with_variable_arrays<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
    variable_arrays: &[&str],
    mut rewrite: F,
) -> Result<(), MsTransformError>
where
    F: FnMut(usize, RecordValue) -> Result<RecordValue, MsTransformError>,
{
    let source = Table::open(TableOptions::new(source_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: source_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    if destination_path.exists() {
        fs::remove_dir_all(destination_path).map_err(|source| MsTransformError::PrepareOutput {
            path: destination_path.display().to_string(),
            reason: source.to_string(),
        })?;
    }
    source
        .shallow_copy(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut destination = Table::open(TableOptions::new(destination_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    destination
        .widen_array_metadata_for_construction(variable_arrays)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut appender = destination.prepare_row_appender().map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for indices in selected_rows.chunks(256) {
        let records =
            selection_metadata::read_subtable_rows(&source, indices).map_err(|source| {
                MsTransformError::MutateMeasurementSet {
                    path: source_path.display().to_string(),
                    source: Box::new(source),
                }
            })?;
        for (&row_index, row) in indices.iter().zip(records) {
            appender
                .append(rewrite(row_index, row)?)
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: destination_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
    }
    drop(appender);
    destination
        .prepare_write()
        .save(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}
