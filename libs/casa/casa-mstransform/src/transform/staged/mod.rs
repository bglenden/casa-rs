// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

mod publication;
mod row_order;
mod selection;
use publication::{OutputRows, OutputSpectra, OutputWrite, publish_transform};
use row_order::{RowOrderInput, order_rows};
use selection::{
    PreparedSpectralInput, SelectedInput, SelectedSpectra, filter_selected_rows,
    prepare_spectral_input, row_selection_error, select_input, select_spectra,
};

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn mstransform_staged(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    metadata_template: Option<&Path>,
    mut operation: Option<SpectralOperation<'_>>,
    partition: Option<TransformSelection>,
) -> Result<MsTransformReport, MsTransformError> {
    let order = match operation.as_ref().map(|op| &op.kernel) {
        Some(SpectrumOperator::Convolution(_)) => selection_metadata::MainRowOrder::SingleDish,
        Some(SpectrumOperator::Baseline(kernel)) if kernel.atmospheric_order() => {
            selection_metadata::MainRowOrder::Atmosphere
        }
        Some(SpectrumOperator::Baseline(_)) => selection_metadata::MainRowOrder::Baseline,
        None => selection_metadata::MainRowOrder::Transform,
    };
    let legacy_time = match options.legacy_tool {
        Some(LegacyMsTool::Partition(time) | LegacyMsTool::Split(time)) => Some(time),
        _ => None,
    };
    let legacy_grid = match options.legacy_tool {
        Some(LegacyMsTool::Regrid(grid)) => Some(grid),
        _ => None,
    };
    let legacy = legacy_time.is_some();
    let splitting = matches!(options.legacy_tool, Some(LegacyMsTool::Split(_)));
    let convolving = matches!(order, selection_metadata::MainRowOrder::SingleDish);
    let hanning = options.hanning && operation.is_none();
    let started_at = Instant::now();
    tracing::info!(
        input_ms = %request.input_ms.display(),
        output_ms = %request.output_ms.display(),
        spw = %request.spw,
        width = request.width,
        "mstransform started"
    );
    let mut selected = select_input(
        request,
        grammar,
        options,
        partition,
        operation.is_some(),
        started_at,
    )?;
    let stage_started_at = Instant::now();
    let mut spectral = select_spectra(request, grammar, options, &selected, legacy_grid)?;
    let (selected_rows, selected_ddids) = filter_selected_rows(
        request,
        options,
        &mut selected,
        &spectral,
        started_at,
        stage_started_at,
    )?;
    let SelectedInput {
        input,
        budget,
        spw_selection,
        visibility,
        selection:
            TransformSelection {
                selection,
                correlations,
                time: time_selection,
                rows,
                antenna_ids,
                tile_window,
            },
    } = selected;
    let PreparedSpectralInput {
        view: prepared_view,
        mut regrid,
        legacy_direct_band,
        first_selected_row,
    } = prepare_spectral_input(
        &input,
        request,
        options,
        &mut spectral,
        &rows,
        &selected_rows,
        budget,
    )?;
    let SelectedSpectra {
        channel_selection,
        continuum,
        task_average,
        widths,
        average_metadata,
        retained_combination,
        channel_bins,
        combined_grid,
        ddid_to_spw,
    } = spectral;
    let input_view = prepared_view.as_deref();
    let view_factory = options.input_view.as_deref();
    let regrid_options = options.regrid.as_ref();
    let stage_started_at = Instant::now();
    let (time_groups, ordered) = order_rows(
        request,
        options,
        RowOrderInput {
            input: &input,
            rows,
            rows_by_index: selected_rows,
            descriptions: selected_ddids,
            ddid_to_spw: &ddid_to_spw,
            channel_bins: &channel_bins,
            channel_selection: &channel_selection,
            correlations: correlations.as_ref(),
            budget,
            order,
            legacy_direct_band,
        },
        &mut operation,
    )?;
    let selection_metadata::OrderedMainRows {
        rows: selected_rows,
        descriptions: selected_ddids,
        starts_visibility_buffer,
        ..
    } = ordered;
    let selection_error = |error| row_selection_error(request, error);
    let hands = match &operation {
        Some(SpectralOperation {
            kernel: SpectrumOperator::Baseline(kernel),
            ..
        }) if kernel.reads_parallel_hands() => {
            correlation_metadata::parallel_hands(&input, correlations.as_ref(), &selected_ddids)
                .map_err(|source| selection_error(MsSelectionError::Domain(source)))?
        }
        _ => BTreeMap::new(),
    };
    let spectral_rows = matches!(
        order,
        selection_metadata::MainRowOrder::Baseline | selection_metadata::MainRowOrder::Atmosphere
    )
    .then(|| {
        baseline::spectrum_rows(
            input.main_table(),
            request,
            &selected_rows,
            selected_ddids
                .iter()
                .map(|id| (ddid_to_spw[id], hands.get(id).copied())),
        )
    })
    .transpose()?;
    let averaging_time = time_groups.is_some();
    let legacy_channel_averaging = splitting
        && !averaging_time
        && channel_bins
            .values()
            .any(|bins| !bins_are_single_channel(bins));
    let combination = combined_grid
        .as_ref()
        .map(|grid| {
            let times = time_groups
                .as_deref()
                .map(|groups| {
                    combine::projected_times(
                        &input,
                        groups,
                        &channel_selection,
                        &ddid_to_spw,
                        correlations.as_ref(),
                        input_view,
                    )
                })
                .transpose()?;
            combine::group_rows(
                &input,
                &selected_rows,
                &selected_ddids,
                &ddid_to_spw,
                &grid.windows,
                times.as_deref().zip(time_groups.as_deref()),
            )
        })
        .transpose()?;
    let representative_rows;
    let representative_ddids;
    let (output_input_rows, output_input_ddids) = if let Some(groups) = &combination {
        representative_rows = groups
            .iter()
            .map(|group| selected_rows[group.representative])
            .collect::<Vec<_>>();
        representative_ddids = groups
            .iter()
            .map(|group| selected_ddids[group.representative])
            .collect::<Vec<_>>();
        (&representative_rows, &representative_ddids)
    } else {
        (&selected_rows, &selected_ddids)
    };
    let polarization = options
        .single_dish_polarization
        .filter(|_| !averaging_time)
        .map(|mode| {
            PolarizationProjection::resolve(&input, correlations.as_ref(), &selected_ddids, mode)
        })
        .transpose()
        .map_err(|source| selection_error(MsSelectionError::Domain(source)))?
        .flatten();
    let selected_field_ids_for_metadata =
        selected_field_ids_for_rows(input.main_table(), output_input_rows, &request.output_ms)?;
    maybe_log_transform_progress(
        "sort_selected_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let float_convolution = convolving && request.data_column == TransformDataColumn::FloatData;
    let read_spectral_weights = polarization.is_some()
        || matches!(
            &operation,
            Some(SpectralOperation { kernel: SpectrumOperator::Baseline(kernel), .. })
                if kernel.reads_spectral_weights()
        );
    let mut spectra = if legacy {
        SpectrumProjection::partition(input.main_table(), first_selected_row, averaging_time)
    } else if legacy_grid.is_some() {
        Ok(SpectrumProjection::existing(input.main_table()))
    } else {
        SpectrumProjection::resolve(
            input.main_table(),
            first_selected_row,
            grammar,
            options.use_weight_spectrum,
            (
                task_average,
                averaging_time,
                convolving || combination.is_some() || regrid.is_some(),
                hanning,
            ),
            &visibility,
        )
    }
    .map_err(|source| MsTransformError::MutateMeasurementSet {
        path: request.input_ms.display().to_string(),
        source: Box::new(source),
    })?;
    if float_convolution && !averaging_time && polarization.is_none() {
        spectra.source.clear();
    }
    if continuum.is_some() {
        for name in STANDARD_SPECTRA {
            if !spectra.source.contains(&name)
                && input
                    .main_table()
                    .schema()
                    .is_some_and(|schema| schema.contains_column(name))
            {
                spectra.source.push(name);
            }
        }
    }
    if read_spectral_weights
        && !spectra.source.contains(&"WEIGHT_SPECTRUM")
        && input
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
    {
        spectra.source.push("WEIGHT_SPECTRUM");
    }

    let mut writes_rows = true;
    if let Some(SpectralOperation {
        kernel: SpectrumOperator::Baseline(kernel),
        ..
    }) = operation.as_ref()
    {
        writes_rows = kernel.writes_rows();
        if kernel.updates_weight() || !writes_rows {
            if !averaging_time && !read_spectral_weights {
                spectra.source.clear();
            }
            spectra.fill_standard = false;
            spectra.propagate_weights = false;
            if kernel.omit_spectral_weights() {
                spectra.output.clear();
            }
        }
    }
    let partitions = regrid.as_ref().and_then(regrid::Plan::split).cloned();
    let copies_per_row = partitions.as_ref().map_or(1, regrid::Partitions::count);
    let selected_output_rows = if writes_rows {
        output_input_rows.len()
    } else {
        0
    };
    let output_rows = selected_output_rows
        .checked_mul(copies_per_row)
        .ok_or_else(|| MsTransformError::Spectrum("Too many partitioned output rows.".into()))?;
    prepare_output_root(&request.output_ms)?;
    let metadata_path = metadata_template.unwrap_or(&request.input_ms);
    let mut output_main = materialize_empty_main_table(
        metadata_path,
        output_rows,
        &request.output_ms,
        &visibility,
        grammar,
        &spectra,
        legacy,
    )?;
    maybe_log_transform_progress(
        "materialize_selected_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    copy_subtables(
        &input,
        metadata_path,
        &request.output_ms,
        &mut output_main,
        legacy,
    )?;
    if grammar == RowGrammar::Transform
        && (task_average || !matches!(request.spw.as_str(), "" | "*"))
        && !legacy
    {
        flag_commands::rewrite(
            &input,
            &request.output_ms,
            &request.spw,
            &channel_selection,
            normalized_width(request.width),
            options.flag_command_widths.as_deref(),
            options.channel_widths.as_deref(),
        )?;
    }
    maybe_log_transform_progress(
        "copy_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let reindex =
        !options.preserve_ids && operation.as_ref().is_none_or(|operation| operation.reindex);
    let reindex_spectra = reindex || combined_grid.is_some() || copies_per_row > 1;
    let ddi_offset = if reindex_spectra {
        i32::try_from(options.ddi_start).map_err(|_| {
            MsTransformError::Spectrum("ddistart exceeds the data-description index limit.".into())
        })?
    } else {
        0
    };
    let mut compact_metadata = if reindex_spectra {
        let (metadata_spws, metadata_bins) = if let Some((all_spws, channels)) =
            &retained_combination
            && combined_grid.is_some()
        {
            (
                all_spws,
                Some(
                    channels
                        .iter()
                        .map(|(&id, channels)| (id, vec![channels.channels.clone()]))
                        .collect::<BTreeMap<_, _>>(),
                ),
            )
        } else {
            (&spw_selection, None)
        };
        compact_spectral_subtables(
            &input,
            &request.output_ms,
            metadata_bins.as_ref().unwrap_or(&channel_bins),
            &ddid_to_spw,
            metadata_spws,
            correlations.as_ref(),
            grammar,
        )?
    } else {
        preserve_spectral_ids(&input, request, &channel_bins, &ddid_to_spw)?
    };
    if let Some(correlations) = correlations.as_ref() {
        correlations.write_polarizations(&input, request)?;
    }
    if let Some(grid) = &combined_grid {
        combine::write_metadata(request, grid, &mut compact_metadata)?;
    }
    if let Some(projection) = &polarization {
        projection.write_metadata(request, &compact_metadata.ddid_map)?;
    }
    let (field_id_map, row_id_maps) = if reindex {
        let field_id_map = selection_metadata::reindex_fields_and_sources(
            &input,
            request,
            selection.field.as_deref(),
            &compact_metadata.spw_map,
        )?;
        let antenna_id_map = selection_metadata::reindex_antenna_subtables(
            &input,
            request,
            antenna_ids.as_deref(),
            &compact_metadata.spw_map,
            time_selection.as_ref(),
        )?;
        let row_id_maps = selection_metadata::reindex_selected_row_ids(
            &input,
            request,
            antenna_id_map,
            grammar == RowGrammar::Transform,
            splitting.then_some(selected_rows.as_slice()),
        )?;
        (field_id_map, row_id_maps)
    } else {
        (None, selection_metadata::RowIdMaps::default())
    };
    if combined_grid.is_some() {
        combine::remap_subtable_references(request)?;
    }
    if grammar == RowGrammar::Transform && (legacy_grid.is_none() || combined_grid.is_some()) {
        selection_metadata::remove_virtual_model_links(
            &input,
            &mut output_main,
            &request.output_ms,
        )?;
    }
    maybe_log_transform_progress(
        "compact_spectral_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let row_count = output_input_rows.len();
    let write_resources = MeasurementSetWriteResources::from_system_memory(2).map_err(|error| {
        MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    let mut tile = options
        .tiling
        .map(|mode| {
            output_layout::resolve_tiling(
                &input,
                mode,
                (&spw_selection, tile_window),
                (
                    &channel_selection,
                    (combined_grid.is_none() && regrid_options.is_none()).then_some(&widths),
                ),
                (&selected_ddids, &ddid_to_spw, correlations.as_ref()),
            )
        })
        .transpose()?;
    if tile.is_none() && legacy {
        tile = Some(output_layout::partition_tiling(
            &input,
            &visibility,
            &channel_selection,
            &selected_ddids,
        )?);
    }
    let (write_plan, input_batch_rows) = transform_array_write_plan(
        &input,
        TransformArrayRows {
            selected_rows: output_input_rows,
            selected_ddids: output_input_ddids,
            groups: if combination.is_some() {
                None
            } else {
                time_groups.as_deref()
            },
            output_rows,
            output_schema: &output_main,
            regrid: regrid.as_ref(),
            tile,
            combined: combined_grid
                .as_ref()
                .zip(combination.as_deref())
                .map(|(grid, groups)| CombinedArrayRows {
                    grid,
                    groups,
                    source_rows: &selected_rows,
                    source_ddids: &selected_ddids,
                    source_time_groups: time_groups.as_deref(),
                }),
        },
        &ddid_to_spw,
        &channel_bins,
        (correlations.as_ref(), polarization.as_ref()),
        (
            &visibility,
            &spectra,
            view_factory
                .map_or(0, TransformInputView::additional_bytes_per_sample)
                .saturating_add(
                    continuum
                        .as_ref()
                        .map_or(0, ContinuumFit::working_bytes_per_sample),
                ),
        ),
        write_resources,
    )?;
    let batch_rows = if writes_rows {
        let per_output = combination
            .as_ref()
            .and_then(|groups| groups.iter().map(|group| group.members.len()).max())
            .unwrap_or(1);
        write_plan
            .batch_rows()
            .min((input_batch_rows / per_output).max(1))
    } else {
        input_batch_rows
    };
    let mut write_session = MeasurementSetWriteSession::start(&request.output_ms, write_plan)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    let stage_started_at = Instant::now();
    let partitioned_rows;
    let scalar_input_rows = if copies_per_row == 1 {
        &output_input_rows[..selected_output_rows]
    } else {
        partitioned_rows = output_input_rows[..selected_output_rows]
            .iter()
            .flat_map(|&row| std::iter::repeat_n(row, copies_per_row))
            .collect::<Vec<_>>();
        &partitioned_rows
    };
    let mut output_column_overrides = gather_selected_main_column_overrides(
        (input.main_table(), &output_main),
        scalar_input_rows,
        &request.output_ms,
        batch_rows,
        &mut write_session,
        &row_id_maps,
        (
            averaging_time,
            polarization.is_some() || continuum.is_some(),
            combination.is_some(),
        ),
    )?;
    maybe_log_transform_progress(
        "load_main_column_overrides",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let mut touched_spws = BTreeSet::new();
    let row_spectrum_names = if averaging_time || combination.is_some() {
        &STANDARD_SPECTRA[..]
    } else {
        &spectra.source
    };
    let local_channel_bins = averaging_time.then(|| {
        channel_bins
            .iter()
            .map(|(&spw, bins)| {
                let mut start = 0;
                let positions = bins
                    .iter()
                    .map(|bin| {
                        let end = start + bin.len();
                        let local = (start..end).collect();
                        start = end;
                        local
                    })
                    .collect();
                (spw, positions)
            })
            .collect::<BTreeMap<i32, Vec<Vec<usize>>>>()
    });
    let binned_time_channels = (splitting
        && averaging_time
        && channel_bins
            .values()
            .any(|bins| !bins_are_single_channel(bins)))
    .then(|| {
        channel_bins
            .iter()
            .map(|(&spw, bins)| (spw, (0..bins.len()).map(|channel| vec![channel]).collect()))
            .collect::<BTreeMap<i32, Vec<Vec<usize>>>>()
    });
    let mut spectrum_available = vec![false; row_spectrum_names.len()];
    let mut time_projection = time_groups.as_ref().map(|_| {
        TimeProjection::new(
            input.main_table(),
            &request.input_ms,
            &spectra.source,
            &visibility,
            input_batch_rows,
            input_view,
        )
    });
    for batch_start in (0..row_count).step_by(batch_rows.max(1)) {
        let batch_end = (batch_start + batch_rows.max(1)).min(row_count);
        let input_rows = &output_input_rows[batch_start..batch_end];
        let mut combined_scalars = Vec::new();
        let (mut input_block, mut group_times) = if let (Some(grid), Some(groups)) =
            (&combined_grid, &combination)
        {
            let result = combine::read_batch(
                grid,
                &groups[batch_start..batch_end],
                &input,
                (&selected_rows, &selected_ddids),
                (&channel_selection, &ddid_to_spw, correlations.as_ref()),
                (&visibility, &spectra.source, input_view, continuum.as_ref()),
                time_projection.as_mut().zip(time_groups.as_deref()),
            )?;
            combined_scalars = result.exposure_flags;
            (result.arrays, result.coordinates)
        } else if let (Some(projection), Some(groups)) = (&mut time_projection, &time_groups) {
            let plans = groups[batch_start..batch_end]
                .iter()
                .map(|group| {
                    let description = group.first.description;
                    let spw = ddid_to_spw[&description];
                    TimeGroupRead {
                        group,
                        channels: &channel_selection[&spw].channels,
                        axes: correlations
                            .as_ref()
                            .and_then(|plan| plan.axes(description)),
                        legacy_bins: binned_time_channels.as_ref().and_then(|_| {
                            let bins = &channel_bins[&spw];
                            (!bins_are_single_channel(bins)).then(|| {
                                let first = &bins[0];
                                let width = first[0].abs_diff(first[first.len() - 1]) + 1;
                                (
                                    &local_channel_bins.as_ref().expect("time averaging")[&spw][..],
                                    width,
                                )
                            })
                        }),
                    }
                })
                .collect::<Vec<_>>();
            projection.average_groups(&plans)?
        } else {
            let block = MainArrayBlock::read(
                input.main_table(),
                input_rows,
                &visibility,
                &spectra.source,
                input_view,
            )
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?;
            (block, Vec::new())
        };
        let descriptions = &output_input_ddids[batch_start..batch_end];
        if grammar == RowGrammar::Transform
            && !averaging_time
            && !read_spectral_weights
            && combination.is_none()
            && !legacy
            && legacy_grid.is_none()
        {
            input_block.resolve_buffer_spectra(
                row_spectrum_names,
                &mut spectrum_available,
                &starts_visibility_buffer[batch_start..batch_end],
                input_rows,
                &request.input_ms,
            )?;
        }
        if let Some(fit) = &continuum
            && combination.is_none()
        {
            fit.apply(
                &mut input_block,
                &visibility,
                descriptions.iter().map(|id| ddid_to_spw[id]),
                row_spectrum_names,
                averaging_time,
            )?;
            for (coordinates, flags) in group_times.iter_mut().zip(&input_block.flags) {
                coordinates.flag_row = continuum::row_flag(flags.as_ref(), None);
            }
        }
        let legacy_row_flags = if legacy_channel_averaging {
            Some(
                input
                    .main_table()
                    .column_accessor("FLAG_ROW")
                    .and_then(|column| column.scalar_cells_owned_for_rows(input_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: request.input_ms.display().to_string(),
                        source: Box::new(source),
                    })?,
            )
        } else {
            None
        };
        let mut input_row_flags = if let Some(projection) = &polarization {
            let row_flags = input
                .main_table()
                .column_accessor("FLAG_ROW")
                .and_then(|column| column.scalar_cells_owned_for_rows(input_rows))
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: request.input_ms.display().to_string(),
                    source: Box::new(source),
                })?;
            projection
                .apply(
                    &mut input_block,
                    (descriptions, &row_flags),
                    (&ddid_to_spw, &channel_selection),
                    &spectra.source,
                )
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: request.input_ms.display().to_string(),
                    source: Box::new(source),
                })?;
            writes_rows.then(|| row_flags.into_iter())
        } else {
            None
        };
        let mut data_values = visibility
            .copies()
            .iter()
            .zip(input_block.visibility.into_iter().map(Vec::into_iter))
            .collect::<Vec<_>>();
        let mut spectrum_values = row_spectrum_names
            .iter()
            .copied()
            .zip(input_block.spectra.into_iter().map(Vec::into_iter))
            .collect::<Vec<_>>();
        let flag_values = input_block.flags;
        let mut flag_category_values = input_block.categories.into_iter();
        let mut uvw_values = input_block.uvw.into_iter();
        let mut weight_values = input_block.weights.into_iter();
        let mut sigma_values = input_block.sigmas.into_iter();
        let mut row_spectra = vec![None; spectrum_values.len()];
        for (batch_offset, (flags, &ddid)) in flag_values.into_iter().zip(descriptions).enumerate()
        {
            let row_index = batch_start + batch_offset;
            let write_error =
                |source: casa_ms::MeasurementSetWriteError| MsTransformError::WriteMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    reason: source.to_string(),
                };
            let write_array = |session: &mut MeasurementSetWriteSession,
                               name: &str,
                               value: &ArrayValue| {
                if let Some(parts) = &partitions {
                    parts.write_channel_array(session, name, value, &request.output_ms, row_index)
                } else {
                    regrid::write_spectrum(session, name, value, &request.output_ms, row_index)
                }
            };
            let undefined = |session: &mut MeasurementSetWriteSession, name: &str| {
                for _ in 0..copies_per_row {
                    session.push_undefined_row(name).map_err(write_error)?;
                }
                Ok::<_, MsTransformError>(())
            };
            let averaged_polarization = polarization
                .as_ref()
                .is_some_and(|projection| projection.contains(ddid));
            let axes = if averaging_time || averaged_polarization || combination.is_some() {
                None
            } else {
                correlations
                    .as_ref()
                    .and_then(|selection| selection.axes(ddid))
            };
            if writes_rows && let Some(coordinates) = group_times.get(batch_offset) {
                for (name, value) in TIME_OUTPUT_SCALARS
                    .into_iter()
                    .zip(coordinates.scalar_cells())
                {
                    for _ in 0..copies_per_row {
                        write_session
                            .push_scalar_row(name, Some(value.clone()))
                            .map_err(write_error)?;
                    }
                }
            } else if let Some(&(exposure, flag_row)) = combined_scalars.get(batch_offset) {
                for (name, value) in [
                    ("EXPOSURE", ScalarValue::Float64(exposure)),
                    ("FLAG_ROW", ScalarValue::Bool(flag_row)),
                ] {
                    for _ in 0..copies_per_row {
                        write_session
                            .push_scalar_row(name, Some(value.clone()))
                            .map_err(write_error)?;
                    }
                }
            }
            let select_correlations = |value, name| {
                select_visibility_axes(value, axes, name).map_err(|source| {
                    MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    }
                })
            };
            let flags =
                flags.ok_or_else(|| missing_column_error(&request.output_ms, row_index, "FLAG"))?;
            let flags = select_correlations(flags, "FLAG")?;
            let spw_id = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!("MAIN row {row_index} references DATA_DESC_ID {ddid}, which has no DATA_DESCRIPTION row"),
                }
            })?;
            let bins = binned_time_channels.as_ref().or(local_channel_bins.as_ref()).unwrap_or(&channel_bins).get(&spw_id).ok_or_else(|| MsTransformError::InvalidSpw {
                selector: request.spw.clone(),
                reason: format!("selected row {row_index} maps to spectral window {spw_id}, but --spw does not include that SPW"),
            })?;
            let bins = combined_grid.as_ref().map_or(bins, |grid| &grid.bins);
            if writes_rows && continuum.is_some() && !averaging_time && combination.is_none() {
                let flagged =
                    continuum::row_flag(Some(&flags), Some(&channel_selection[&spw_id].channels));
                for _ in 0..copies_per_row {
                    write_session
                        .push_scalar_row("FLAG_ROW", Some(ScalarValue::Bool(flagged)))
                        .map_err(write_error)?;
                }
            }
            let regrid = regrid
                .as_mut()
                .map(|plan| plan.row(spw_id, input.main_table(), input_rows[batch_offset]))
                .transpose()?;
            let mutation_error = |source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            };
            let noise =
                if float_convolution {
                    None
                } else {
                    let weight = weight_values.next().flatten().ok_or_else(|| {
                        missing_column_error(&request.output_ms, row_index, "WEIGHT")
                    })?;
                    let sigma = sigma_values.next().flatten().ok_or_else(|| {
                        missing_column_error(&request.output_ms, row_index, "SIGMA")
                    })?;
                    Some((
                        select_correlations(weight, "WEIGHT")?,
                        select_correlations(sigma, "SIGMA")?,
                    ))
                };
            for ((name, values), output) in spectrum_values.iter_mut().zip(&mut row_spectra) {
                *output = values
                    .next()
                    .flatten()
                    .filter(|value| !value.is_empty())
                    .map(|value| select_correlations(value, name))
                    .transpose()?;
            }
            let source_spectrum =
                |name| row_spectrum_names.iter().position(|&source| source == name);
            let spectra_cells = SpectrumCells {
                weight: source_spectrum("WEIGHT_SPECTRUM")
                    .and_then(|index| row_spectra[index].as_ref()),
                sigma: source_spectrum("SIGMA_SPECTRUM")
                    .and_then(|index| row_spectra[index].as_ref()),
            };
            let legacy_averages = if let Some(row_flags) = &legacy_row_flags
                && !bins_are_single_channel(bins)
            {
                let flag_row =
                    scalar_bool(row_flags[batch_offset].as_ref(), "FLAG_ROW", row_index)?;
                Some(
                    legacy_split::ChannelAverages::new(
                        &flags,
                        spectra_cells.weight,
                        bins,
                        flag_row,
                    )
                    .map_err(mutation_error)?,
                )
            } else {
                None
            };
            let projected_weights = if read_spectral_weights && !averaging_time {
                let channels = &channel_selection[&spw_id].channels;
                spectra_cells
                    .weight
                    .filter(|weight| {
                        weight
                            .shape()
                            .get(1)
                            .is_none_or(|&count| !all_channels_selected(channels, count))
                    })
                    .map(|weight| select_channels(weight.clone(), channels))
                    .transpose()
                    .map_err(mutation_error)?
            } else {
                None
            };
            let average_regrid_input =
                !legacy_direct_band && regrid.is_some() && !bins_are_single_channel(bins);
            let averages = if let (true, Some((weight, sigma))) =
                (task_average || average_regrid_input, &noise)
            {
                Some(
                    ChannelAverages::new(
                        &flags,
                        bins,
                        weight,
                        sigma,
                        spectra_cells,
                        &visibility,
                        (
                            spectra.propagate_weights
                                || averaging_time
                                || combination.is_some()
                                || regrid.is_some(),
                            averaging_time,
                        ),
                    )
                    .map_err(mutation_error)?,
                )
            } else {
                None
            };
            let mut transformed_flags = if let Some(averages) = &legacy_averages {
                averages.flags()
            } else if let Some(averages) = &averages {
                averages.flags()
            } else if legacy_direct_band {
                flags.clone()
            } else {
                transform_flag_channels(flags.clone(), bins).map_err(mutation_error)?
            };
            let hanning_noise = if hanning && legacy_grid.is_some() {
                if bins.len() > 1 {
                    hanning::flag(&mut transformed_flags);
                }
                None
            } else if let (true, Some(noise)) = (hanning, noise.as_ref()) {
                let statistics = hanning::statistics(
                    noise,
                    spectra_cells,
                    &flags,
                    bins,
                    averages.as_ref(),
                    &visibility,
                    averaging_time,
                )
                .map_err(mutation_error)?;
                hanning::flag(&mut transformed_flags);
                Some(statistics)
            } else {
                None
            };
            let fft_flags = regrid
                .as_ref()
                .map(|regrid| regrid.fft_input_flags(&transformed_flags))
                .transpose()
                .map_err(mutation_error)?
                .flatten();
            let combined_noise = if let Some(regrid) = &regrid
                && legacy_grid.is_some()
            {
                transformed_flags = regrid
                    .apply(transformed_flags, None)
                    .map_err(mutation_error)?;
                None
            } else if let Some(regrid) = &regrid {
                let source_noise = if let Some(statistics) =
                    hanning_noise.or_else(|| averages.as_ref().map(ChannelAverages::statistics))
                {
                    Some((statistics.weight_spectrum, statistics.sigma_spectrum))
                } else if combination.is_some() {
                    spectra_cells
                        .weight
                        .cloned()
                        .zip(spectra_cells.sigma.cloned())
                } else if let Some((weight, sigma)) = &noise {
                    Some(
                        regrid::input_noise(
                            (weight, sigma),
                            spectra_cells,
                            &flags,
                            bins,
                            &visibility,
                            averaging_time,
                        )
                        .map_err(mutation_error)?,
                    )
                } else {
                    None
                };
                transformed_flags = regrid
                    .apply(transformed_flags, None)
                    .map_err(mutation_error)?;
                source_noise
                    .map(|(weight, sigma)| {
                        regrid.statistics(weight, sigma, &transformed_flags, fft_flags.as_ref())
                    })
                    .transpose()
                    .map_err(mutation_error)?
            } else if let Some(grid) = &combined_grid {
                let result = combine::finish_noise(
                    grid,
                    hanning_noise,
                    averages.as_ref(),
                    spectra_cells,
                    &transformed_flags,
                )
                .map_err(mutation_error)?;
                transformed_flags = grid
                    .interpolate(transformed_flags)
                    .map_err(mutation_error)?;
                Some(result)
            } else {
                hanning_noise
            };
            if let Some(original) = input_row_flags.as_mut().and_then(Iterator::next) {
                let flag_row = match &transformed_flags {
                    ArrayValue::Bool(flags) if averaged_polarization => {
                        Some(ScalarValue::Bool(flags.iter().all(|&flag| flag)))
                    }
                    _ => original,
                };
                write_session
                    .push_scalar_row("FLAG_ROW", flag_row)
                    .map_err(|source| MsTransformError::WriteMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        reason: source.to_string(),
                    })?;
            }
            let mut baseline_weights = None;
            for (column, values) in &mut data_values {
                let source = column.source.name();
                let target = column.output.name();
                let independent_lag =
                    column.source == OptionalMainColumn::LagData && legacy_grid.is_none();
                let Some(value) = values.next().flatten() else {
                    if legacy_direct_band || column.source == OptionalMainColumn::LagData {
                        undefined(&mut write_session, target)?;
                        continue;
                    }
                    return Err(missing_column_error(&request.output_ms, row_index, source));
                };
                let value = select_correlations(value, source)?;
                let mut transformed = if independent_lag || legacy_direct_band {
                    value
                } else if let Some(averages) = &legacy_averages {
                    averages.visibility(value).map_err(mutation_error)?
                } else if let Some(averages) = &averages {
                    averages
                        .visibility(column.source, value)
                        .map_err(mutation_error)?
                } else {
                    transform_data_channels(value, &flags, bins).map_err(|source| {
                        MsTransformError::MutateMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            source: Box::new(source),
                        }
                    })?
                };
                let smooth_visibility = if legacy_grid.is_some() {
                    column.source != OptionalMainColumn::ModelData
                } else {
                    !independent_lag
                };
                if hanning && smooth_visibility {
                    if legacy_grid.is_some() {
                        hanning::smooth_regrid(&mut transformed, &flags).map_err(mutation_error)?;
                    } else {
                        hanning::smooth(
                            &mut transformed,
                            &[transformed_flags.shape()[0], bins.len()],
                        )
                        .map_err(mutation_error)?;
                    }
                }
                if !independent_lag {
                    if let Some(regrid) = &regrid {
                        transformed = regrid
                            .apply(transformed, fft_flags.as_ref())
                            .map_err(mutation_error)?;
                    } else if let Some(grid) = &combined_grid {
                        transformed = grid.interpolate(transformed).map_err(mutation_error)?;
                    }
                }
                if let Some(operation) = &mut operation {
                    let ArrayValue::Bool(output_flags) = &mut transformed_flags else {
                        return Err(MsTransformError::Spectrum(
                            "Spectral flags must be Bool.".into(),
                        ));
                    };
                    match &mut operation.kernel {
                        SpectrumOperator::Convolution(kernel) => {
                            kernel
                                .apply(&mut transformed, output_flags)
                                .map_err(MsTransformError::Spectrum)?;
                        }
                        SpectrumOperator::Baseline(handler) => {
                            let coordinates = spectral_rows
                                .as_ref()
                                .and_then(|rows| rows.get(row_index))
                                .ok_or_else(|| {
                                    MsTransformError::Spectrum(
                                        "Missing original spectrum coordinates.".into(),
                                    )
                                })?;
                            let mut coordinates = *coordinates;
                            if let Some(averaged) = group_times.get(batch_offset) {
                                coordinates.time = averaged.time;
                                coordinates.interval = averaged.interval;
                            }
                            if let Some(group) = time_groups
                                .as_ref()
                                .and_then(|groups| groups.get(row_index))
                            {
                                coordinates.selection_row = group.row_in_chunk;
                            }
                            baseline_weights = handler
                                .process(
                                    &coordinates,
                                    &mut transformed,
                                    output_flags,
                                    SpectrumWeights {
                                        scalar: noise.as_ref().map(|(weight, _)| weight),
                                        spectrum: projected_weights
                                            .as_ref()
                                            .or(spectra_cells.weight),
                                    },
                                )
                                .map_err(MsTransformError::Spectrum)?;
                        }
                    }
                }
                if !writes_rows {
                    continue;
                }
                if independent_lag {
                    for _ in 0..copies_per_row {
                        regrid::write_spectrum(
                            &mut write_session,
                            target,
                            &transformed,
                            &request.output_ms,
                            row_index,
                        )?;
                    }
                } else {
                    write_array(&mut write_session, target, &transformed)?;
                }
            }
            if !writes_rows {
                touched_spws.insert(spw_id);
                continue;
            }
            write_array(&mut write_session, "FLAG", &transformed_flags)?;
            if let Some(flag_category) = flag_category_values
                .next()
                .flatten()
                .filter(|value| !value.is_empty())
            {
                let flag_category = select_correlations(flag_category, "FLAG_CATEGORY")?;
                let mut transformed = transform_flag_category_channels(flag_category, bins)
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    })?;
                if let Some(regrid) = &regrid {
                    if transformed.shape()[1] != 1 || combination.is_some() {
                        transformed = regrid
                            .apply_categories(transformed)
                            .map_err(mutation_error)?;
                    }
                } else if let Some(grid) = &combined_grid {
                    transformed = grid
                        .interpolate_categories(transformed)
                        .map_err(mutation_error)?;
                }
                write_array(&mut write_session, "FLAG_CATEGORY", &transformed)?;
            } else {
                undefined(&mut write_session, "FLAG_CATEGORY")?;
            }
            let uvw = uvw_values
                .next()
                .flatten()
                .ok_or_else(|| missing_column_error(&request.output_ms, row_index, "UVW"))?;
            for _ in 0..copies_per_row {
                push_f64_array_row(
                    &mut write_session,
                    "UVW",
                    &uvw,
                    &request.output_ms,
                    row_index,
                )?;
            }
            let averaged_noise =
                combined_noise.or_else(|| averages.map(|averages| averages.statistics()));
            if let Some(weights) = baseline_weights
                && let Some((original_weight, sigma)) = &noise
            {
                let (mut original_weight, mut sigma) = (original_weight.clone(), sigma.clone());
                visibility.align_scalar_weights(&mut original_weight, &mut sigma);
                push_f32_array_row(
                    &mut write_session,
                    "WEIGHT",
                    &weights,
                    &request.output_ms,
                    row_index,
                )?;
                push_f32_array_row(
                    &mut write_session,
                    "SIGMA",
                    &sigma,
                    &request.output_ms,
                    row_index,
                )?;
                for &column in &spectra.output {
                    undefined(&mut write_session, column)?;
                }
                touched_spws.insert(spw_id);
                continue;
            }
            let Some((weight, sigma)) = noise else {
                // CASA publishes these columns with undefined cells when the
                // transformed visibility column is FLOAT_DATA.
                for column in ["WEIGHT", "SIGMA"]
                    .into_iter()
                    .chain(spectra.output.iter().copied())
                {
                    undefined(&mut write_session, column)?;
                }
                touched_spws.insert(spw_id);
                continue;
            };
            if let Some(parts) = &partitions {
                let statistics = averaged_noise.ok_or_else(|| {
                    MsTransformError::Spectrum("Missing split-regrid noise statistics.".into())
                })?;
                parts.write_noise(
                    &mut write_session,
                    &statistics,
                    &transformed_flags,
                    spectra.fill_standard,
                    &request.output_ms,
                    row_index,
                )?;
                if !spectra.fill_standard {
                    for &column in &spectra.output {
                        undefined(&mut write_session, column)?;
                    }
                }
                touched_spws.insert(spw_id);
                continue;
            }
            // TimeProjection already put the legacy weights on this grid.
            let projected_legacy_weight = legacy
                && averaging_time
                && spectra.fill_standard
                && spectra_cells.weight.is_some_and(|weight| {
                    weight.shape() == flags.shape()
                        && weight.shape().get(1) == Some(&bins.len())
                        && bins
                            .iter()
                            .enumerate()
                            .all(|(i, bin)| bin.as_slice() == [i])
                });
            let (mut weight, mut sigma, task_spectra) = if let Some(SpectralOperation {
                kernel: SpectrumOperator::Convolution(kernel),
                ..
            }) = &operation
            {
                let corrected = visibility.corrected_becomes_data();
                let (weight, sigma) =
                    unaveraged_spectra(&weight, &sigma, spectra_cells, &flags, bins, corrected)
                        .map_err(mutation_error)?;
                let ArrayValue::Bool(output_flags) = &transformed_flags else {
                    return Err(MsTransformError::Spectrum(
                        "Spectral flags must be Bool.".into(),
                    ));
                };
                let statistics = convolution::statistics(
                    weight,
                    sigma,
                    output_flags,
                    kernel.weight_kernel(),
                    corrected,
                )
                .map_err(mutation_error)?;
                (
                    statistics.weight,
                    statistics.sigma,
                    spectra
                        .fill_standard
                        .then_some((statistics.weight_spectrum, statistics.sigma_spectrum)),
                )
            } else if let Some(averages) = averaged_noise {
                (
                    averages.weight,
                    averages.sigma,
                    spectra
                        .fill_standard
                        .then_some((averages.weight_spectrum, averages.sigma_spectrum)),
                )
            } else if spectra.fill_standard && !projected_legacy_weight {
                let values = unaveraged_spectra(
                    &weight,
                    &sigma,
                    spectra_cells,
                    &flags,
                    bins,
                    visibility.corrected_becomes_data() && !averaging_time,
                )
                .map_err(mutation_error)?;
                (weight, sigma, Some(values))
            } else {
                (weight, sigma, None)
            };
            if let Some(regrid) = &regrid
                && legacy_grid.is_some()
            {
                regrid
                    .scale_noise(&mut weight, false, hanning)
                    .map_err(mutation_error)?;
                regrid
                    .scale_noise(&mut sigma, true, hanning)
                    .map_err(mutation_error)?;
            } else if continuum.is_some()
                && !task_average
                && !convolving
                && !hanning
                && combination.is_none()
                && regrid.is_none()
            {
                let channels =
                    (!averaging_time).then_some(channel_selection[&spw_id].channels.as_slice());
                continuum::mean_noise(&mut weight, spectra_cells.weight, &flags, channels);
                continuum::mean_noise(&mut sigma, spectra_cells.sigma, &flags, channels);
                visibility.align_scalar_weights(&mut weight, &mut sigma);
            } else if splitting && !averaging_time {
                if let Some(bin) = bins.first().filter(|bin| bin.len() > 1) {
                    let width = bin[0].abs_diff(bin[bin.len() - 1]) + 1;
                    legacy_split::scale_channel_noise(&visibility, width, &mut weight, &mut sigma)
                        .map_err(mutation_error)?;
                }
                legacy_split::align_row_noise(&visibility, &mut weight, &mut sigma)
                    .map_err(mutation_error)?;
            } else if grammar == RowGrammar::Transform
                && !task_average
                && !averaging_time
                && !convolving
                && !hanning
                && combination.is_none()
                && regrid.is_none()
                && !legacy
            {
                visibility.align_scalar_weights(&mut weight, &mut sigma);
            }
            push_f32_array_row(
                &mut write_session,
                "WEIGHT",
                &weight,
                &request.output_ms,
                row_index,
            )?;
            push_f32_array_row(
                &mut write_session,
                "SIGMA",
                &sigma,
                &request.output_ms,
                row_index,
            )?;
            let (mut task_weight, mut task_sigma) =
                task_spectra.map_or((None, None), |(weight, sigma)| (Some(weight), Some(sigma)));
            let mut legacy_weight_spectrum = legacy_averages
                .as_ref()
                .map(legacy_split::ChannelAverages::weight_spectrum);
            for &name in &spectra.output {
                let transformed = match name {
                    _ if legacy_grid.is_some() => {
                        let projector = regrid.as_ref().expect("legacy spectral rewrite grid");
                        source_spectrum(name)
                            .and_then(|index| row_spectra[index].take())
                            .map(|value| {
                                let mut output = projector.apply(value, fft_flags.as_ref())?;
                                projector.scale_noise(
                                    &mut output,
                                    name == "SIGMA_SPECTRUM",
                                    hanning,
                                )?;
                                Ok(output)
                            })
                            .transpose()
                            .map_err(mutation_error)?
                    }
                    "WEIGHT_SPECTRUM" if legacy_weight_spectrum.is_some() => {
                        legacy_weight_spectrum.take().flatten()
                    }
                    "WEIGHT_SPECTRUM" if projected_legacy_weight => {
                        source_spectrum(name).and_then(|index| row_spectra[index].take())
                    }
                    "WEIGHT_SPECTRUM"
                        if grammar == RowGrammar::Transform && (!legacy || averaging_time) =>
                    {
                        task_weight.take()
                    }
                    "SIGMA_SPECTRUM" if grammar == RowGrammar::Transform => task_sigma.take(),
                    _ => source_spectrum(name)
                        .and_then(|index| row_spectra[index].take())
                        .map(|value| {
                            if name == "SIGMA_SPECTRUM" {
                                transform_sigma_channels(value, &flags, bins)
                            } else {
                                transform_weight_channels(value, &flags, bins)
                            }
                        })
                        .transpose()
                        .map_err(mutation_error)?,
                };
                if let Some(transformed) = transformed {
                    push_f32_array_row(
                        &mut write_session,
                        name,
                        &transformed,
                        &request.output_ms,
                        row_index,
                    )?;
                } else {
                    undefined(&mut write_session, name)?;
                }
            }
            touched_spws.insert(spw_id);
        }
    }
    maybe_log_transform_progress(
        "select_channels",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    publish_transform(
        request,
        OutputRows {
            input_rows: output_input_rows,
            input_ddids: output_input_ddids,
            field_ids: &selected_field_ids_for_metadata,
            field_id_map: &field_id_map,
            selected_output_rows,
            output_rows,
            copies_per_row,
            ddi_offset,
            writes_rows,
        },
        OutputSpectra {
            regrid: regrid.as_ref(),
            compact_metadata: &compact_metadata,
            channel_selection: &channel_selection,
            channel_bins: &channel_bins,
            average_metadata: &average_metadata,
            combined_grid: combined_grid.as_ref(),
            widths: &widths,
            touched_spws,
            splitting,
        },
        OutputWrite {
            main: &mut output_main,
            session: write_session,
            overrides: &mut output_column_overrides,
            visibility: &visibility,
            spectra_output: spectra.output,
        },
        budget,
        started_at,
    )
}
