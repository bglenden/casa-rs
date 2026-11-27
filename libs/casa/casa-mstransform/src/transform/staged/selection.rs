// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct SelectedInput {
    pub(super) input: MeasurementSet,
    pub(super) budget: MsSelectionIoBudget,
    pub(super) spw_selection: SpwSelection,
    pub(super) visibility: VisibilityColumns,
    pub(super) selection: TransformSelection,
}

pub(super) fn select_input(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    partition: Option<TransformSelection>,
    has_operation: bool,
    started_at: Instant,
) -> Result<SelectedInput, MsTransformError> {
    let legacy = matches!(
        options.legacy_tool,
        Some(LegacyMsTool::Partition(_) | LegacyMsTool::Split(_))
    );
    let splitting = matches!(options.legacy_tool, Some(LegacyMsTool::Split(_)));
    let stage_started_at = Instant::now();
    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    maybe_log_transform_progress(
        "open_measurement_set",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let selection_error = |error| row_selection_error(request, error);
    let budget = MsSelectionIoBudget::from_system_memory(
        2,
        std::mem::size_of::<ResolvedMsSelectionRow>(),
        None,
    )
    .map_err(selection_error)?;
    let spw = match request.spw.trim() {
        "" => "*",
        spw => spw,
    };
    let mut spw_selection =
        input
            .resolve_spw_selector(spw)
            .map_err(|source| MsTransformError::InvalidSpw {
                selector: request.spw.clone(),
                reason: source.to_string(),
            })?;
    if options.legacy_continuum_output.is_some() {
        // The legacy group iterator receives complete buffers. Only fitspw
        // projects the channels used in the fit itself.
        spw_selection.keep_all_channels();
    }
    if splitting {
        let mut windows = BTreeSet::new();
        if spw_selection
            .channel_list()
            .iter()
            .any(|range| !windows.insert(range.spw_id))
        {
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    if splitting
        && options.time_averaging.is_some()
        && spw_selection
            .channel_list()
            .first()
            .is_some_and(|range| range.step > 1)
    {
        return Err(MsTransformError::Spectrum(
            "Simultaneous time and channel averaging is not handled.".into(),
        ));
    }
    let view_factory = options.input_view.as_deref();
    let mut visibility = VisibilityColumns::resolve(
        &input,
        request.data_column,
        options.visibility_columns.as_deref(),
        &request.input_ms,
        view_factory,
    )?;
    if legacy {
        visibility.promote_singleton_visibility();
    }
    let mut selection = match partition {
        Some(partition) => partition,
        None => resolve_transform_selection(&input, request, grammar, budget, has_operation)?,
    };
    if options.legacy_continuum_output.is_some() {
        selection.correlations = None;
    }
    maybe_log_transform_progress(
        "select_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    Ok(SelectedInput {
        input,
        budget,
        spw_selection,
        visibility,
        selection,
    })
}

pub(super) struct SelectedSpectra {
    pub(super) channel_selection: BTreeMap<i32, TransformChannelSelection>,
    pub(super) continuum: Option<ContinuumFit>,
    pub(super) task_average: bool,
    pub(super) widths: BTreeMap<i32, usize>,
    pub(super) average_metadata: Option<BTreeMap<i32, Vec<spectral_metadata::ChannelWindow>>>,
    pub(super) retained_combination:
        Option<(SpwSelection, BTreeMap<i32, TransformChannelSelection>)>,
    pub(super) channel_bins: BTreeMap<i32, Vec<Vec<usize>>>,
    pub(super) combined_grid: Option<combine::Grid>,
    pub(super) ddid_to_spw: BTreeMap<i32, i32>,
}

pub(super) fn select_spectra(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    selected: &SelectedInput,
    legacy_grid: Option<LegacySpectralGrid>,
) -> Result<SelectedSpectra, MsTransformError> {
    let SelectedInput {
        input,
        budget,
        spw_selection,
        ..
    } = selected;
    let legacy = matches!(
        options.legacy_tool,
        Some(LegacyMsTool::Partition(_) | LegacyMsTool::Split(_))
    );
    let splitting = matches!(options.legacy_tool, Some(LegacyMsTool::Split(_)));
    let channel_selection = resolve_transform_channels(input, spw_selection, &request.spw)?;
    let continuum = options
        .continuum
        .as_ref()
        .map(|options| ContinuumFit::new(input, &channel_selection, options))
        .transpose()?;
    let task_average = grammar == RowGrammar::Transform
        && !legacy
        && (normalized_width(request.width) > 1 || options.channel_widths.is_some());
    let widths = channel_widths_by_spw(
        &channel_selection,
        spw_selection,
        request.width,
        options.channel_widths.as_deref(),
    )?;
    if splitting
        && options
            .channel_widths
            .as_ref()
            .is_some_and(|widths| widths.contains(&0))
    {
        return Err(MsTransformError::InvalidSplitStep);
    }
    let average_metadata = task_average.then(|| {
        channel_selection
            .iter()
            .map(|(&spw, selection)| (spw, selection.metadata_for_width(widths[&spw])))
            .collect::<BTreeMap<_, _>>()
    });
    let regrid_options = options.regrid.as_ref();
    let retained_combination = if options.preserve_ids && options.combine_spws {
        let all_spws =
            input
                .resolve_spw_selector("*")
                .map_err(|source| MsTransformError::InvalidSpw {
                    selector: "*".into(),
                    reason: source.to_string(),
                })?;
        let mut channels = resolve_transform_channels(input, &all_spws, "*")?;
        channels.extend(
            channel_selection
                .iter()
                .map(|(&id, selection)| (id, selection.clone())),
        );
        Some((all_spws, channels))
    } else {
        None
    };
    let channel_bins = channel_bins_by_spw(
        &channel_selection,
        &widths,
        if regrid_options.is_some() {
            None
        } else {
            average_metadata.as_ref()
        },
    )?;
    let combined_grid =
        if options.combine_spws || regrid_options.is_some_and(|options| options.nspw > 1) {
            if options.channel_widths.is_some() {
                return Err(MsTransformError::Spectrum(
                    "If SPW combination is active, chabin cannot be an array".into(),
                ));
            }
            combine::Grid::new(
                input,
                &channel_selection,
                retained_combination
                    .as_ref()
                    .map_or(&channel_selection, |(_, channels)| channels),
                normalized_width(request.width),
                budget.available_bytes / budget.maximum_live_blocks,
                matches!(legacy_grid, Some(LegacySpectralGrid::Cvel)),
            )?
        } else {
            None
        };
    let ddid_to_spw = data_description_spw_map(input)?;
    if splitting {
        let mut seen = BTreeSet::new();
        if ddid_to_spw
            .values()
            .filter(|spw| channel_selection.contains_key(spw))
            .any(|spw| !seen.insert(spw))
        {
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    Ok(SelectedSpectra {
        channel_selection,
        continuum,
        task_average,
        widths,
        average_metadata,
        retained_combination,
        channel_bins,
        combined_grid,
        ddid_to_spw,
    })
}

pub(super) fn filter_selected_rows(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    selected: &mut SelectedInput,
    spectra: &SelectedSpectra,
    started_at: Instant,
    stage_started_at: Instant,
) -> Result<(Vec<usize>, Vec<i32>), MsTransformError> {
    let SelectedInput {
        input,
        budget,
        spw_selection,
        selection,
        ..
    } = selected;
    let input = &*input;
    let TransformSelection {
        correlations, rows, ..
    } = selection;
    let SelectedSpectra {
        channel_bins,
        ddid_to_spw,
        ..
    } = spectra;
    let budget = *budget;
    let view_factory = options.input_view.as_deref();
    let selection_error = |error| row_selection_error(request, error);
    let output_spws = options
        .legacy_continuum_output
        .as_deref()
        .map(|selector| {
            input
                .resolve_spw_selector(selector)
                .map_err(|error| MsTransformError::InvalidSpw {
                    selector: selector.to_owned(),
                    reason: error.to_string(),
                })
                .map(|selection| {
                    selection
                        .spw_ids()
                        .iter()
                        .map(|&id| id as i32)
                        .collect::<BTreeSet<_>>()
                })
        })
        .transpose()?;
    rows.retain(|row| {
        spw_selection.selects_row(row.data_desc_id)
            && channel_bins.contains_key(&row.spectral_window_id)
            && output_spws
                .as_ref()
                .is_none_or(|spws| spws.contains(&row.spectral_window_id))
    });
    if let Some(view) = view_factory {
        view.order_output_rows(input, rows)
            .map_err(|error| MsTransformError::SelectRows {
                path: request.input_ms.display().to_string(),
                source: error.into(),
            })?;
    }
    let (mut selected_rows, mut selected_ddids): (Vec<_>, Vec<_>) = rows
        .iter()
        .map(|row| (row.row_index, row.data_desc_id))
        .unzip();
    maybe_log_transform_progress(
        "filter_rows_by_spw",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    if selected_rows.is_empty() {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    if !request.keep_flags {
        let stage_started_at = Instant::now();
        let dimensions = transform_array_dimensions(
            input,
            &selected_ddids,
            ddid_to_spw,
            channel_bins,
            correlations.as_ref(),
            None,
        )?;
        let flag_rows = flag_filter_read_plan(selected_rows.len(), budget, dimensions.values())
            .map_err(selection_error)?;
        (selected_rows, selected_ddids) =
            keep_unflagged_rows(input, &selected_rows, &selected_ddids, flag_rows, None)?;
        if options.time_averaging.is_some() || view_factory.is_some() {
            let kept = selected_rows.iter().copied().collect::<BTreeSet<_>>();
            rows.retain(|row| kept.contains(&row.row_index));
        }
        maybe_log_transform_progress(
            "drop_fully_flagged_rows",
            stage_started_at.elapsed(),
            started_at.elapsed(),
        );
        if selected_rows.is_empty() {
            tracing::warn!(
                input_ms = %request.input_ms.display(),
                "mstransform selected no rows after dropping fully flagged data"
            );
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    Ok((selected_rows, selected_ddids))
}

pub(super) struct PreparedSpectralInput<'a> {
    pub(super) view: Option<Box<dyn PreparedTransformInputView + 'a>>,
    pub(super) regrid: Option<regrid::Plan>,
    pub(super) legacy_direct_band: bool,
    pub(super) first_selected_row: usize,
}

pub(super) fn prepare_spectral_input<'a>(
    input: &'a MeasurementSet,
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    spectra: &mut SelectedSpectra,
    rows: &[ResolvedMsSelectionRow],
    selected_rows: &[usize],
    budget: MsSelectionIoBudget,
) -> Result<PreparedSpectralInput<'a>, MsTransformError> {
    let SelectedSpectra {
        channel_selection,
        channel_bins,
        combined_grid,
        ..
    } = spectra;
    let view_factory = options.input_view.as_deref();
    let spw = request.spw.trim();
    let spw = if spw.is_empty() { "*" } else { spw };
    let legacy_grid = match options.legacy_tool {
        Some(LegacyMsTool::Regrid(grid)) => Some(grid),
        _ => None,
    };
    let regrid_options = options.regrid.as_ref();
    let legacy_direct_band = legacy_grid.is_some()
        && combined_grid.is_none()
        && options.time_averaging.is_none()
        && options.channel_widths.is_none()
        && normalized_width(request.width) == 1
        && spw == "*";
    let first_selected_row = selected_rows
        .iter()
        .copied()
        .min()
        .expect("nonempty row selection");
    let prepared_view = if let Some(view) = view_factory {
        let channels = channel_selection
            .iter()
            .map(|(&id, selection)| (id, selection.channels.as_slice()))
            .collect();
        Some(view.prepare(input, rows, &channels).map_err(|source| {
            MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            }
        })?)
    } else {
        None
    };
    let regrid = regrid_options
        .map(|options| {
            regrid::Plan::new(
                input,
                (options, legacy_grid),
                combined_grid.as_mut(),
                channel_selection,
                channel_bins,
                (
                    first_selected_row,
                    ((legacy_direct_band
                        && matches!(
                            legacy_grid,
                            Some(
                                LegacySpectralGrid::CenteredChannels { .. }
                                    | LegacySpectralGrid::CenteredNumeric { .. }
                            )
                        )
                        && options.nspw == 1)
                        || (matches!(legacy_grid, Some(LegacySpectralGrid::Cvel))
                            && !options.outframe.trim().is_empty()))
                    .then_some(selected_rows),
                ),
                budget.available_bytes / budget.maximum_live_blocks,
            )
        })
        .transpose()?;
    Ok(PreparedSpectralInput {
        view: prepared_view,
        regrid,
        legacy_direct_band,
        first_selected_row,
    })
}

pub(super) fn row_selection_error(
    request: &MsTransformRequest,
    error: MsSelectionError,
) -> MsTransformError {
    MsTransformError::SelectRows {
        path: request.input_ms.display().to_string(),
        source: match error {
            MsSelectionError::Domain(source) => source,
            other => MsError::InvalidInput(other.to_string()),
        },
    }
}
