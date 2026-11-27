// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn build_generic_visibility_scatter_with_averaging(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
    calibration: Option<&PlotMsInput<'_>>,
) -> Result<MsPlotPayload, String> {
    let symbols = calibration.and_then(|input| input.raw_symbols);
    let show_flagged = symbols.is_some_and(|symbols| symbols.flagged);
    let listobs_options = selection.to_summary_options();
    let row_numbers = match calibration {
        Some(calibration) => calibration.row_order.to_vec(),
        None => resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?,
    };

    let needs_visibility_grid = spec.x_axis.is_visibility_math()
        || spec.y_axes.iter().copied().any(MsAxis::is_visibility_math);
    let needs_spectral_coordinates = spec.x_axis.uses_spectral_coordinates()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_spectral_coordinates);
    let requested_freqframe = parse_frequency_frame(spec.transforms.freqframe.as_deref())?;
    let visibility_columns =
        plotms_ordinates::VisibilityColumns::prepare(ms, spec, &row_numbers, calibration)?;
    let time_bins = calibration.and_then(|input| input.time_bins);
    let phase_shift = calibration
        .and_then(|input| input.phasecenter.as_ref())
        .map(|target| plotms_phasecenter::PhaseCenterShift::new(ms, target));
    let averaged_row_numbers = time_bins.and_then(PlotTimeGroups::row_numbers);
    let extended_derived = calibration.is_some_and(|input| input.extended_derived);
    let weighted_time_uvw = time_bins.is_some()
        && (phase_shift.is_some()
            || plotms_columns::uses_weighted_time_uvw(spec)
            || plotms_columns::uses_weighted_spw_time_u(spec));
    let metadata_uvw = weighted_time_uvw && spec.has_uvw_coordinate_axes();
    let collapsed_uv = metadata_uvw
        && spec.y_axes.len() == 1
        && spec.iteration.iteraxis != Some(MsIterationAxis::Correlation);
    let uv_channels =
        collapsed_uv && (spec.x_axis.uses_wavelengths() || spec.y_axis().uses_wavelengths());
    let time_geometry =
        time_bins.is_some() && plotms_columns::uses_averaged_geometry_coordinates(spec);
    let time_reporting_axes =
        time_bins.is_some() && plotms_columns::uses_spectral_reporting_time_grid(spec);
    let uncalibrated = calibration.is_some_and(|input| input.rows.is_none());
    let average_statistics =
        uncalibrated && time_bins.is_some() && plotms_columns::uses_time_statistics(spec);
    let spw_average = uncalibrated && plotms_columns::uses_spectral_window_average(spec);
    let time_spw = spw_average && time_bins.is_some();
    let spw_antenna = spw_average && spec.averaging.avgantenna;
    let spw_baseline = spw_average && spec.averaging.avgbaseline;
    let scalar_average = uncalibrated && plotms_columns::uses_scalar_visibility_average(spec);
    let antenna_average = uncalibrated && plotms_columns::uses_antenna_visibility_average(spec);
    let time_antenna = antenna_average && time_bins.is_some();
    let scalar_antenna_time =
        antenna_average && spec.averaging.scalar && spec.averaging.avgtime.is_some();
    let scalar_spw_time = spw_average && spec.averaging.scalar && spec.averaging.avgtime.is_some();
    let scalar_intervals = scalar_average || scalar_antenna_time || scalar_spw_time;
    let scalar_visibility =
        scalar_average || ((antenna_average || spw_average) && spec.averaging.scalar);
    let collapse_baselines = (plotms_columns::uses_vector_baseline_average(spec)
        || (scalar_average && spec.averaging.avgbaseline)
        || spw_baseline)
        && uncalibrated;
    let retain_empty_ordinate_list = uncalibrated
        && spec.y_axes.len() > 2
        && (collapse_baselines
            || antenna_average
            || ((spec.averaging.avgscan || spec.averaging.avgfield)
                && (time_bins.is_some() || scalar_average)
                && !spw_average));
    let time_baselines = collapse_baselines && time_bins.is_some();
    let average_channels = spec.averaging.avgchannel.is_some_and(|width| width > 1);
    let channel_baselines = collapse_baselines && average_channels && !time_baselines;
    let scalar_operands =
        scalar_average || scalar_spw_time || (extended_derived && scalar_antenna_time);
    let scalar_channels = scalar_operands && average_channels;
    let antenna_channels = antenna_average && average_channels && !time_antenna;
    let unbinned_antenna =
        (antenna_average && spec.averaging.avgtime.is_none() && !average_channels) || spw_antenna;
    // The unbinned antenna reducer uses the selected visibility cube flags.
    let antenna_cube_flags = unbinned_antenna && (spec.y_axes.len() > 2 || symbols.is_some());
    let spw_channels = spw_average && average_channels && !time_spw;
    let time_antenna_channels = time_antenna && average_channels;
    let vector_span_models = !spw_average
        && (spec.averaging.avgscan || spec.averaging.avgfield)
        && time_bins.is_some_and(|groups| groups.phase_reference_slots().is_some())
        && calibration.is_some_and(|input| input.models.is_some());
    let direct_time_projection = uncalibrated
        && time_bins.is_some()
        && !extended_derived
        && (phase_shift.is_some()
            || vector_span_models
            || averaged_row_numbers.is_some()
            || time_geometry
            || time_reporting_axes
            || average_statistics
            || plotms_columns::uses_direct_vector_time_projection(
                spec,
                calibration.and_then(|input| input.x_data_column),
                calibration.is_some_and(|input| input.prefer_float_data),
            ));
    let average_operands = extended_derived
        || scalar_channels
        || scalar_spw_time
        || time_antenna
        || time_spw
        || direct_time_projection;
    let channel_first = channel_baselines || scalar_channels || antenna_channels || spw_channels;
    let time_weight_kinds = calibration
        .filter(|_| time_bins.is_some())
        .map(|input| plotms_columns::weight_kinds(ms, spec, input));
    let channel_first_kinds = calibration
        .filter(|_| channel_first)
        .map(|input| plotms_columns::weight_kinds(ms, spec, input));
    let combined_vector_average = time_weight_kinds.is_some() && average_channels;
    // A model-only request gives each surviving channel equal weight after
    // the iterator has normalized that channel's time samples.
    let model_channels_equally = combined_vector_average
        && time_weight_kinds.is_some_and(|kinds| kinds.row_weight == WeightKind::Unit);
    let average_model_channels_equally = time_baselines && model_channels_equally;
    let visibility_weight_kinds = time_weight_kinds.or(channel_first_kinds);
    let additional_ordinates = calibration.map_or(&[][..], |input| input.additional_ordinates);
    let needs_weight_kind = |kind| {
        if time_spw {
            return time_weight_kinds
                .is_some_and(|kinds| kinds.primary == kind || kinds.row_weight == kind);
        }
        visibility_weight_kinds
            .is_some_and(|kinds| kinds.uses_for_ordinates(kind, additional_ordinates))
            || (average_statistics
                && time_weight_kinds
                    .is_some_and(|kinds| kinds.row_weight == kind || kinds.row_sigma == kind))
            || channel_first_kinds.is_some_and(|kinds| kinds.row_weight == kind)
            || ((time_baselines || time_antenna || time_spw)
                && time_weight_kinds.is_some_and(|kinds| kinds.row_weight == kind))
    };
    let needs_observed_weights = needs_weight_kind(WeightKind::Observed);
    let needs_calibrated_weights = needs_weight_kind(WeightKind::Calibrated);
    let axis_weight_kinds =
        visibility_weight_kinds.map_or([WeightKind::Calibrated; 3], |kinds| kinds.axes);
    let additional_weight_kinds = additional_ordinates
        .iter()
        .map(|&(_, column)| {
            visibility_weight_kinds.map_or(WeightKind::Calibrated, |kinds| kinds.for_column(column))
        })
        .collect::<Vec<_>>();
    let axis_weight_kind = |axis: usize| {
        if axis < axis_weight_kinds.len() {
            axis_weight_kinds[axis]
        } else {
            additional_weight_kinds[axis - axis_weight_kinds.len()]
        }
    };
    let selected_flags =
        SelectedArrayColumn::load_for_plot(ms.main_table(), "FLAG", &row_numbers, calibration)?;
    let selected_weights =
        SelectedArrayColumn::load_for_plot(ms.main_table(), "WEIGHT", &row_numbers, calibration)?;
    let selected_sigmas = SelectedArrayColumn::load(ms.main_table(), "SIGMA", &row_numbers)?;
    let selected_flag_rows = selected_bool_values(ms.main_table(), "FLAG_ROW", &row_numbers)?;
    let selected_intervals = selected_f64_values(ms.main_table(), "INTERVAL", &row_numbers)?;
    let selected_data_desc_ids =
        selected_i32_values(ms.main_table(), "DATA_DESC_ID", &row_numbers)?;
    let selected_field_ids = selected_i32_values(ms.main_table(), "FIELD_ID", &row_numbers)?;
    let selected_scan_numbers = selected_i32_values(ms.main_table(), "SCAN_NUMBER", &row_numbers)?;
    let selected_observations =
        (collapse_baselines || scalar_average || antenna_average || spw_average)
            .then(|| selected_i32_values(ms.main_table(), "OBSERVATION_ID", &row_numbers))
            .transpose()?;
    let selected_antenna1 = selected_i32_values(ms.main_table(), "ANTENNA1", &row_numbers)?;
    let selected_antenna2 = selected_i32_values(ms.main_table(), "ANTENNA2", &row_numbers)?;
    let selected_times = selected_f64_values(ms.main_table(), "TIME", &row_numbers)?;
    let selected_uvw = (phase_shift.is_some()
        || spec.x_axis.uses_uvw()
        || spec.y_axes.iter().copied().any(MsAxis::uses_uvw))
    .then(|| selected_uvw_values(ms.main_table(), "UVW", &row_numbers))
    .transpose()?;
    let derived_engine = if spec.x_axis.uses_derived_geometry()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_derived_geometry)
        || (needs_spectral_coordinates && requested_freqframe.is_some())
    {
        Some(MsCalEngine::for_calibration(ms).map_err(|error| error.to_string())?)
    } else {
        None
    };
    let field = ms.field().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    let spans_scan_or_field = spec.averaging.avgscan || spec.averaging.avgfield;
    let averaged_antenna_color = plotms_averaged_colors::axis(spec).filter(|_| time_antenna);
    let input_color = if averaged_antenna_color.is_some() {
        MsColorAxis::None
    } else {
        spec.color_by
    };
    let scalar_spans = (scalar_intervals && spans_scan_or_field)
        .then(|| {
            plotms_ordinates::ScalarSpanPlan::new(
                plotms_ordinates::ScalarSpanColumns {
                    observations: selected_observations
                        .as_deref()
                        .expect("scalar observations"),
                    scans: &selected_scan_numbers,
                    fields: &selected_field_ids,
                    descriptions: &selected_data_desc_ids,
                    times: &selected_times,
                },
                &data_description,
                &spec.averaging,
            )
        })
        .transpose()?;
    let chan_freq = if needs_spectral_coordinates || spw_average {
        Some(ChanFreqColumn::new(spectral_window.table()))
    } else {
        None
    };

    let requested_corr_codes = selection
        .correlation
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

    let iteraxis = spec.iteration.iteraxis;
    let iterated_ordinate_list = iterates_plotms_ordinates(spec, calibration);
    let paged_iteration = iterated_ordinate_list
        || (spw_average
            && iteraxis.is_some_and(|axis| {
                axis == MsIterationAxis::SpectralWindow || axis.supports_averaged_ordinates()
            }));
    let include_row_flagged_points =
        spec.x_axis.uses_flag_rows() || spec.y_axes.iter().copied().any(MsAxis::uses_flag_rows);
    let use_spectral_weights = calibration.is_some()
        && (needs_visibility_grid || average_statistics)
        && (!scalar_average || scalar_channels)
        && (!spw_average || spw_channels || time_spw || spw_antenna)
        && (!time_spw || needs_calibrated_weights)
        && (!collapse_baselines || channel_baselines || time_baselines);
    let needs_weight_spectrum = matches!(spec.x_axis, MsAxis::WeightSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::WeightSpectrum));
    let needs_sigma_spectrum = matches!(spec.x_axis, MsAxis::SigmaSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::SigmaSpectrum));
    let selected_weight_spectrum = if needs_weight_spectrum || use_spectral_weights {
        selected_plot_weight_spectrum(ms, &row_numbers, calibration)?
    } else {
        None
    };
    // Without a stored weight spectrum, the transform averages each complex
    // channel bin uniformly while still forming the output row weight.
    let propagate_channel_weights = channel_first
        && selected_weight_spectrum
            .as_ref()
            .and_then(|column| {
                // Spectrum availability is established before iterator sorting.
                let (slot, _) = row_numbers.iter().enumerate().min_by_key(|&(_, row)| row)?;
                column.find(slot)
            })
            .is_some();
    let channel_weight_mask = if !propagate_channel_weights {
        plotms_columns::ChannelWeightMask::FlagsOnly
    } else if scalar_channels || antenna_channels {
        plotms_columns::ChannelWeightMask::Nonzero
    } else {
        plotms_columns::ChannelWeightMask::Positive
    };
    let selected_sigma_spectrum = if needs_sigma_spectrum || needs_observed_weights {
        SigmaSpectrumColumn::new(ms.main_table())
            .ok()
            .map(|_| SelectedArrayColumn::load(ms.main_table(), "SIGMA_SPECTRUM", &row_numbers))
            .transpose()?
    } else {
        None
    };
    let use_shared_time_scope_midpoint = time_bins.is_none()
        && spec.averaging.avgtime.is_some()
        && (spec.averaging.avgfield || spec.averaging.avgscan || spec.averaging.avgspw);

    let mut time_scope_origins = std::collections::BTreeMap::<TimeAverageScopeKey, f64>::new();
    let mut shared_time_scope_bounds =
        std::collections::BTreeMap::<SharedTimeAverageScopeKey, (f64, f64)>::new();
    if spec.averaging.avgtime.is_some() && time_bins.is_none() && !scalar_intervals {
        for (row_slot, _row) in row_numbers.iter().copied().enumerate() {
            let flag_row_value = selected_flag_rows[row_slot];
            if flag_row_value && !include_row_flagged_points {
                continue;
            }
            let ddid = selected_data_desc_ids[row_slot];
            if ddid < 0 || (ddid as usize) >= data_description.row_count() {
                continue;
            }
            let ddid = ddid as usize;
            let spw_id = data_description
                .spectral_window_id(ddid)
                .map_err(|error| error.to_string())?;
            let field_id_value = selected_field_ids[row_slot];
            let scan_number_value = selected_scan_numbers[row_slot];
            let antenna1_value = selected_antenna1[row_slot];
            let antenna2_value = selected_antenna2[row_slot];
            let row_time_value = selected_times[row_slot];
            if use_shared_time_scope_midpoint {
                let shared_scope = build_shared_time_average_scope_key(
                    field_id_value,
                    scan_number_value,
                    spw_id,
                    spec,
                );
                shared_time_scope_bounds
                    .entry(shared_scope)
                    .and_modify(|(min_time, max_time)| {
                        *min_time = min_time.min(row_time_value);
                        *max_time = max_time.max(row_time_value);
                    })
                    .or_insert((row_time_value, row_time_value));
            }
            for baseline_token in averaging_baseline_tokens(antenna1_value, antenna2_value, spec) {
                let scope = build_time_average_scope_key(
                    field_id_value,
                    scan_number_value,
                    spw_id,
                    baseline_token,
                    spec,
                );
                time_scope_origins
                    .entry(scope)
                    .and_modify(|origin| {
                        if row_time_value < *origin {
                            *origin = row_time_value;
                        }
                    })
                    .or_insert(row_time_value);
            }
        }
    }

    let mut selected_channel_bins = HashMap::new();
    let mut uv_selections = HashMap::new();
    let mut clear_uv_channels = std::collections::HashSet::new();
    let mut panel_order = Vec::<String>::new();
    let mut panels = std::collections::BTreeMap::<String, AveragedPanelAccumulator>::new();
    let mut contributing_rows = std::collections::BTreeSet::<usize>::new();
    let mut visibility_grids = Vec::new();
    let mut bin_buffers = visibility_columns.buffers();
    let mut time_bounds = HashMap::<usize, plotms_ordinates::VisibilityTimeBounds>::new();
    let mut time_uvws = weighted_time_uvw.then(HashMap::<(usize, i32, i32), TimeUvwAverage>::new);
    let mut time_statistics = average_statistics.then(plotms_columns::TimeStatistics::default);
    let time_wavelengths = time_geometry
        && std::iter::once(spec.x_axis)
            .chain(spec.y_axes.iter().copied())
            .any(MsAxis::uses_wavelengths);
    let mut time_spectra =
        (time_wavelengths || time_reporting_axes).then(HashMap::<i32, TimePlotSpectrum>::new);
    let time_reporting_frame = time_reporting_frame(
        time_wavelengths,
        requested_freqframe,
        &row_numbers,
        time_bins,
        &selected_data_desc_ids,
        &data_description,
        chan_freq.as_ref(),
    )?;
    let mut baseline_groups = plotms_ordinates::BaselineIntegrationGroups::default();
    let mut scalar_time_bounds = HashMap::<usize, (f64, f64)>::new();
    let mut averaged_rows = std::collections::BTreeSet::new();
    let mut scalar_shapes = plotms_ordinates::VisibilityAverageShapes::default();
    let mut antenna_correlation_counts = (time_antenna || scalar_antenna_time).then(HashMap::new);
    let mut antenna_correlation_conflict = None;
    let mut spw_groups = spw_average.then(plotms_spw::SpectralAverageGroups::default);
    let mut time_baseline_stats = (time_baselines || time_antenna || time_spw)
        .then(plotms_time_baselines::TimeBaselineStatistics::default);
    let mut channel_buffer_first = None;

    for (row_slot, row) in row_numbers.iter().copied().enumerate() {
        if (scalar_channels || antenna_channels)
            && calibration.is_some_and(|input| input.starts_visibility_buffer[row_slot])
        {
            channel_buffer_first = Some(row_slot);
        }
        let flag_row_value = selected_flag_rows[row_slot];
        let skip_flagged_row = flag_row_value
            && !include_row_flagged_points
            && time_bins.is_none()
            && !collapse_baselines
            && !antenna_cube_flags
            && !spw_average
            && !scalar_intervals;
        if skip_flagged_row && !antenna_average {
            continue;
        }

        let ddid = selected_data_desc_ids[row_slot];
        if ddid < 0 || (ddid as usize) >= data_description.row_count() {
            continue;
        }
        let ddid = ddid as usize;
        let spw_id = data_description
            .spectral_window_id(ddid)
            .map_err(|error| error.to_string())?;
        let field_id_value = selected_field_ids[row_slot];
        let scan_number_value = selected_scan_numbers[row_slot];
        let row_time_value = selected_times[row_slot];
        let row_interval_value = selected_intervals[row_slot];
        let starts_buffer = scalar_intervals
            && calibration.is_some_and(|input| input.starts_visibility_buffer[row_slot]);
        let scalar_span_group = scalar_spans
            .as_ref()
            .and_then(|spans| spans.group(row_slot));
        // The iterator counts a flagged integration before applying the plot mask.
        // It can separate two visible buffers that have the same timestamp.
        let antenna_group = selected_observations
            .as_ref()
            .filter(|_| antenna_average && !time_antenna)
            .map(|observations| {
                scalar_span_group.unwrap_or_else(|| {
                    baseline_groups.observe(
                        [
                            observations[row_slot],
                            scan_number_value,
                            field_id_value,
                            spw_id,
                        ],
                        row_time_value,
                        spec.averaging.avgtime.unwrap_or(0.0),
                    )
                })
            });
        if scalar_antenna_time
            && starts_buffer
            && let Some(group) = antenna_group
        {
            record_scalar_interval(
                &mut scalar_time_bounds,
                group,
                row_time_value,
                row_interval_value,
            );
        }
        if skip_flagged_row && !antenna_channels {
            continue;
        }
        let pol_id = data_description
            .polarization_id(ddid)
            .map_err(|error| error.to_string())?;
        let corr_types = if pol_id >= 0 && (pol_id as usize) < polarization.row_count() {
            polarization
                .corr_type(pol_id as usize)
                .map_err(|error| error.to_string())?
        } else {
            Vec::new()
        };

        let flags = match selected_flags.get(row_slot)? {
            ArrayValue::Bool(values) => {
                values.view().into_dimensionality::<Ix2>().map_err(|_| {
                    "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
                })?
            }
            other => {
                return Err(format!(
                    "msexplore requires BOOL flag cells, found {:?}",
                    other.primitive_type()
                ));
            }
        };
        visibility_columns.read_row(row_slot, extended_derived, &mut visibility_grids)?;
        let (corr_count, chan_count) = visibility_grids
            .first()
            .map(|grid| (grid.corr_count, grid.chan_count))
            .unwrap_or((flags.nrows(), flags.ncols()));
        if flags.shape() != [corr_count, chan_count] {
            return Err(format!(
                "visibility flag shape {:?} does not match data shape [{}, {}]",
                flags.shape(),
                corr_count,
                chan_count
            ));
        }
        let weight_values =
            float_axis_values(selected_weights.get(row_slot)?, corr_count, "WEIGHT")?;
        let sigma_values = float_axis_values(selected_sigmas.get(row_slot)?, corr_count, "SIGMA")?;
        let buffer_first = time_bins
            .and_then(|plan| plan.input_buffer_first_slot(row_slot))
            .or(channel_buffer_first);
        let weight_spectrum_grid = selected_weight_spectrum
            .as_ref()
            .map(|column| column.spectrum_in_buffer(row_slot, buffer_first))
            .transpose()?
            .flatten()
            .map(|cell| float_grid_from_array(cell, "WEIGHT_SPECTRUM"))
            .transpose()?
            .or_else(|| {
                (needs_weight_spectrum || needs_calibrated_weights)
                    .then(|| scalar_values_to_grid(&weight_values, chan_count))
            });
        let sigma_spectrum_grid = selected_sigma_spectrum
            .as_ref()
            .map(|column| {
                if calibration.is_none() {
                    column.get(row_slot).map(Some)
                } else {
                    column.spectrum_in_buffer(row_slot, buffer_first)
                }
            })
            .transpose()?
            .flatten()
            .map(|cell| float_grid_from_array(cell, "SIGMA_SPECTRUM"))
            .transpose()?
            .or_else(|| {
                (needs_sigma_spectrum || needs_observed_weights)
                    .then(|| scalar_values_to_grid(&sigma_values, chan_count))
            });
        for (column_name, grid) in [
            ("WEIGHT_SPECTRUM", weight_spectrum_grid.as_ref()),
            ("SIGMA_SPECTRUM", sigma_spectrum_grid.as_ref()),
        ] {
            if let Some(grid) = grid
                && (grid.corr_count != corr_count || grid.chan_count != chan_count)
            {
                return Err(format!(
                    "{column_name} shape [{}, {}] does not match data shape [{corr_count}, {chan_count}]",
                    grid.corr_count, grid.chan_count
                ));
            }
        }
        let observed_weights = sigma_spectrum_grid
            .as_ref()
            .filter(|_| needs_observed_weights)
            .map(plotms_columns::inverse_variance);
        let weight_grid = |kind| match kind {
            WeightKind::Observed => observed_weights.as_ref(),
            WeightKind::Calibrated if use_spectral_weights => weight_spectrum_grid.as_ref(),
            _ => None,
        };
        let axis_weight = |axis| {
            if channel_first && !propagate_channel_weights {
                None
            } else {
                weight_grid(axis_weight_kind(axis))
            }
        };
        let column_weights =
            visibility_columns.column_weights((0..=spec.y_axes.len()).map(axis_weight));
        let time_model_kind = visibility_weight_kinds.and_then(|kinds| kinds.model);
        let time_model_weight = time_model_kind.and_then(weight_grid);
        let primary_time_weight_kind =
            time_weight_kinds
                .filter(|_| combined_vector_average)
                .map(|kinds| {
                    if time_baselines || time_antenna || time_spw {
                        kinds.row_weight
                    } else {
                        kinds.primary
                    }
                });
        let primary_weight_grid = primary_time_weight_kind.and_then(weight_grid);
        let time_channel_primary = primary_weight_grid
            .zip(primary_time_weight_kind)
            .filter(|_| {
                extended_derived || time_antenna_channels || time_spw || direct_time_projection
            });
        let time_antenna_spectrum = time_weight_kinds
            .filter(|_| time_antenna)
            .map(|kinds| weight_grid(kinds.row_weight));
        // The baseline reducer sees the mask after the last visibility cube
        // transform. Mixed stored columns use the calibrated mask here.
        let channel_mask_grid = channel_first_kinds
            .filter(|_| propagate_channel_weights)
            .and_then(|kinds| weight_grid(kinds.row_weight));
        let antenna_channel_spectrum = channel_first_kinds
            .filter(|_| antenna_channels)
            .and_then(|kinds| weight_grid(kinds.row_weight));

        let selected_correlations =
            select_correlation_slots(corr_count, &corr_types, requested_corr_codes.as_deref());
        if selected_correlations.is_empty() {
            continue;
        }
        let correlation_required = collapsed_uv
            || spec.x_axis.uses_correlation_slots()
            || spec
                .y_axes
                .iter()
                .copied()
                .any(MsAxis::uses_correlation_slots)
            || matches!(spec.color_by, MsColorAxis::Correlation)
            || matches!(iteraxis, Some(MsIterationAxis::Correlation));
        let selected_correlations = if correlation_required {
            selected_correlations
        } else {
            vec![selected_correlations[0].clone()]
        };

        // The antenna-time cache requires a consistent selected polarization
        // shape in each spectral window, including across separate time bins.
        if let Some(counts) = &mut antenna_correlation_counts {
            let actual = selected_correlations.len();
            let expected = *counts.entry(spw_id).or_insert(actual);
            if expected != actual {
                antenna_correlation_conflict.get_or_insert((spw_id, expected, actual));
            }
        }

        let channel_bins = plot_channel_bins_for_window(
            chan_count,
            spec,
            spw_id,
            calibration,
            &mut selected_channel_bins,
        )?;

        let antenna1_value = selected_antenna1[row_slot];
        let antenna2_value = selected_antenna2[row_slot];
        let row_uvw_value = selected_uvw
            .as_ref()
            .map_or([0.0; 3], |values| values[row_slot]);
        let (time_group, representative_slot) = time_bins.map_or((None, row_slot), |bins| {
            let group = bins.group(row_slot);
            (Some(group), bins.first_row_slot(group))
        });
        let group = time_group
            .or(antenna_group)
            .or(scalar_span_group)
            .or_else(|| {
                selected_observations.as_ref().map(|observations| {
                    baseline_groups.observe(
                        [
                            observations[row_slot],
                            scan_number_value,
                            field_id_value,
                            if spw_average { 0 } else { spw_id },
                        ],
                        row_time_value,
                        spec.averaging
                            .avgtime
                            .filter(|_| scalar_average || scalar_spw_time)
                            .unwrap_or(0.0),
                    )
                })
            });
        if let Some(group) = group
            && (scalar_intervals || spw_average)
        {
            scalar_shapes.check_shape(
                if spw_average {
                    "spectral window averaging"
                } else {
                    "scalar time averaging"
                },
                group,
                selected_correlations.len(),
                channel_bins.len(),
            )?;
            if let Some(spw_groups) = &mut spw_groups {
                spw_groups.observe(
                    group,
                    spw_id,
                    chan_count,
                    &channel_bins,
                    chan_freq.as_ref().expect("SPW averaging frequencies"),
                    starts_buffer,
                )?;
            }
        }
        if collapsed_uv {
            uv_selections.entry(ddid).or_insert_with(|| {
                let correlations = selected_correlations
                    .iter()
                    .map(|(slot, _)| *slot)
                    .collect::<Vec<_>>();
                (correlations, channel_bins.to_vec())
            });
        }
        let representative_row = row_numbers[representative_slot];
        let metadata_slot = if vector_span_models {
            time_bins
                .and_then(|groups| groups.input_buffer_first_slot(representative_slot))
                .unwrap_or(representative_slot)
        } else {
            representative_slot
        };
        let span_metadata =
            group.and_then(|group| scalar_spans.as_ref().map(|spans| spans.metadata(group)));
        let representative_field =
            span_metadata.map_or(selected_field_ids[metadata_slot], |metadata| metadata.field);
        let representative_scan = span_metadata
            .map_or(selected_scan_numbers[metadata_slot], |metadata| {
                metadata.scan
            });
        if let Some(group) = time_group {
            // Time averaging sees selected input channels, including any
            // incomplete bin that channel averaging will subsequently drop.
            let ranges = calibration.and_then(|input| input.channels.get(&spw_id));
            let unflagged = plotms_ordinates::has_unflagged_input(
                &flags,
                &selected_correlations,
                ranges.map(Vec::as_slice),
            );
            if uv_channels {
                for bin in channel_bins.iter() {
                    if bin.channels().any(|channel| {
                        selected_correlations
                            .iter()
                            .any(|&(corr, _)| !flags[(corr, channel)])
                    }) {
                        clear_uv_channels.insert((group, bin.ordinal));
                    }
                }
            }
            if let (Some(statistics), Some(kinds)) = (&mut time_statistics, time_weight_kinds) {
                let spectra = [weight_grid(kinds.row_weight), weight_grid(kinds.row_sigma)];
                for &(corr, _) in &selected_correlations {
                    statistics.observe(group, corr, &channel_bins, &flags, spectra);
                }
            }
            if let (Some(time_uvws), Some(kinds)) = (&mut time_uvws, time_weight_kinds) {
                let weight = if metadata_uvw {
                    // PlotMS requests no visibility column for coordinate-only
                    // pairs, so each unmasked input integration contributes once.
                    1.0
                } else {
                    selected_correlations
                        .iter()
                        .map(|(corr, _)| match kinds.row_weight {
                            WeightKind::Calibrated => weight_values[*corr],
                            WeightKind::Observed => f64::from(casa_mstransform::weight_from_sigma(
                                sigma_values[*corr] as f32,
                            )),
                            WeightKind::Unit => 1.0,
                        })
                        .sum()
                };
                let _ = time_uvws
                    .entry((group, antenna1_value, antenna2_value))
                    .or_default()
                    .add(row_uvw_value, !unflagged, weight);
            }
            if let (Some(statistics), Some(observations), Some(kinds)) = (
                &mut time_baseline_stats,
                &selected_observations,
                time_weight_kinds,
            ) {
                statistics.observe(plotms_time_baselines::TimeBaselineRow {
                    time_group: group,
                    partition: [
                        observations[row_slot],
                        representative_scan,
                        representative_field,
                        spw_id,
                    ],
                    integration: plotms_time_baselines::Integration {
                        time: row_time_value,
                        interval: selected_intervals[row_slot],
                    },
                    first_correlation: (time_baselines || time_spw)
                        .then_some(selected_correlations[0].0),
                    visibility_shape: (time_antenna || time_spw)
                        .then_some((selected_correlations.len(), channel_bins.len())),
                    bins: &channel_bins,
                    flags: &flags,
                    weights: weight_grid(kinds.row_weight),
                    has_unflagged_visibility: unflagged,
                });
            } else {
                let integration = plotms_time_baselines::Integration {
                    time: row_time_value,
                    interval: row_interval_value,
                };
                time_bounds
                    .entry(group)
                    .or_insert_with(|| plotms_ordinates::VisibilityTimeBounds::new(integration))
                    .observe(integration, unflagged);
            }
        }
        if let Some(group) = group.filter(|_| scalar_average || spw_average) {
            if !spw_antenna {
                let (antenna1, antenna2) = if collapse_baselines {
                    (-1, -1)
                } else {
                    (antenna1_value, antenna2_value)
                };
                averaged_rows.insert((group, antenna1, antenna2));
            }
            // The scalar reducer uses the first row's interval for each buffer.
            if starts_buffer {
                record_scalar_interval(
                    &mut scalar_time_bounds,
                    group,
                    row_time_value,
                    row_interval_value,
                );
            }
        }
        if (antenna_average || spw_antenna)
            && let Some(group) = group
        {
            averaged_rows.extend([(group, antenna1_value, -1), (group, antenna2_value, -1)]);
        }
        let shared_time_scope_value = if use_shared_time_scope_midpoint {
            let shared_scope = build_shared_time_average_scope_key(
                field_id_value,
                scan_number_value,
                spw_id,
                spec,
            );
            shared_time_scope_bounds
                .get(&shared_scope)
                .map(|(min_time, max_time)| min_time + (max_time - min_time) / 2.0)
        } else {
            None
        };
        let load_spectrum = || {
            resolve_spectral_context(
                spec,
                spw_id,
                chan_count,
                (field_id_value, row_time_value),
                chan_freq.as_ref(),
                derived_engine.as_ref(),
                calibration.and_then(|_| channel_bins.get(channel_bins.len() / 2)),
            )
        };
        let spectral_context = if let Some(spectra) = &mut time_spectra {
            use std::collections::hash_map::Entry;
            let spectrum = match spectra.entry(selected_data_desc_ids[row_slot]) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let column = chan_freq
                        .as_ref()
                        .expect("time spectral axes use channel frequencies");
                    entry.insert(TimePlotSpectrum::read(column, spw_id, &channel_bins)?)
                }
            };
            if spectrum.source_channels != chan_count {
                return Err(format!(
                    "SPECTRAL_WINDOW row {spw_id} reported {} channel frequencies for {chan_count} data channels",
                    spectrum.source_channels,
                ));
            }
            None
        } else if spw_average || !needs_spectral_coordinates {
            None
        } else {
            load_spectrum()?
        };

        let panel_spw = if iteraxis == Some(MsIterationAxis::SpectralWindow) {
            group
                .and_then(|group| spw_groups.as_ref()?.output_window(group))
                .unwrap_or(spw_id)
        } else {
            spw_id
        };
        let baseline_tokens = if time_antenna {
            vec![AveragingBaselineToken::Baseline(
                antenna1_value,
                antenna2_value,
            )]
        } else {
            averaging_baseline_tokens(antenna1_value, antenna2_value, spec)
        };
        let shared_baseline_weight = ((collapse_baselines && !time_baselines)
            || scalar_average
            || (spw_average && !time_spw && !spw_antenna))
            .then(|| {
                // The channel transform leaves the output row weight unset
                // when only FLOAT_DATA is requested. The SPW reducer weights
                // those windows equally. A DATA request fills its usual weights.
                if (spw_channels || scalar_channels) && spec.data_column == MsDataColumn::Float {
                    return 1.0;
                }
                let correlation = selected_correlations[0].0;
                let weight = channel_first_kinds.map_or(weight_values[correlation], |kinds| {
                    plotms_columns::channel_averaged_row_weight(
                        weight_grid(kinds.row_weight),
                        correlation,
                        &channel_bins,
                        &flags,
                        channel_weight_mask,
                    )
                });
                plotms_columns::baseline_weight(weight)
            });
        let mut row_panels = std::collections::BTreeSet::<String>::new();
        let visible_correlations = if collapsed_uv {
            &selected_correlations[..1]
        } else {
            &selected_correlations
        };
        for (selected_slot, (corr_index, corr_label)) in visible_correlations.iter().enumerate() {
            let row_weight = weight_values[*corr_index];
            let row_sigma = sigma_values[*corr_index];
            let mut flag_samples = Vec::<bool>::new();
            let mut weight_spectrum_samples = Vec::<f64>::new();
            let mut sigma_spectrum_samples = Vec::<f64>::new();
            let scalar_weight = shared_baseline_weight
                .unwrap_or_else(|| time_weight_kinds.map_or(row_weight, |_| 1.0));
            for (endpoint, baseline_token) in baseline_tokens.iter().enumerate() {
                // For a per-antenna average the second end contributes a
                // conjugate, including when both ends are the same antenna.
                let conjugate = matches!(baseline_token, AveragingBaselineToken::Antenna(_))
                    && endpoint == 1
                    && (antenna_average || spw_antenna || antenna1_value != antenna2_value);
                let bin_count = if collapsed_uv && !uv_channels {
                    1
                } else {
                    channel_bins.len()
                };
                for bin in channel_bins.iter().take(bin_count) {
                    let antenna_weight = antenna_channels.then(|| {
                        if skip_flagged_row {
                            (true, 0.0)
                        } else {
                            plotms_columns::channel_bin_weight(
                                antenna_channel_spectrum,
                                *corr_index,
                                bin,
                                &flags,
                                channel_weight_mask,
                            )
                        }
                    });
                    let channel_masked = antenna_weight.map_or_else(
                        || {
                            channel_mask_grid.is_some_and(|grid| {
                                if scalar_channels {
                                    plotms_columns::channel_bin_weight(
                                        Some(grid),
                                        *corr_index,
                                        bin,
                                        &flags,
                                        channel_weight_mask,
                                    )
                                    .0
                                } else {
                                    !plotms_columns::bin_has_weight(grid, *corr_index, bin, &flags)
                                }
                            })
                        },
                        |(masked, _)| masked,
                    );
                    if channel_masked
                        && !(scalar_channels
                            || antenna_channels
                            || spw_channels
                            || retain_empty_ordinate_list
                            || iterated_ordinate_list)
                    {
                        continue;
                    }
                    let (panel_key, panel_label, iteration_rank) = iteration_group(
                        iteraxis,
                        representative_field,
                        &field,
                        panel_spw,
                        &spectral_window,
                        representative_scan,
                        Some((
                            corr_label,
                            corr_types.get(*corr_index).copied().unwrap_or_default(),
                        )),
                    );
                    if !panels.contains_key(&panel_key) {
                        panel_order.push(panel_key.clone());
                        panels.insert(
                            panel_key.clone(),
                            AveragedPanelAccumulator {
                                label: panel_label,
                                iteration_rank,
                                ..Default::default()
                            },
                        );
                    }
                    if channel_masked {
                        // Keep the panel when the channel transform
                        // masks every bin in the selection.
                        continue;
                    }
                    let scalar_weight =
                        if extended_derived && scalar_antenna_time && !scalar_channels {
                            let weight =
                                weight_spectrum_grid.as_ref().map_or(scalar_weight, |grid| {
                                    grid.values[*corr_index * grid.chan_count + bin.start]
                                });
                            plotms_columns::baseline_weight(weight)
                        } else {
                            antenna_weight.map_or(scalar_weight, |(_, weight)| {
                                plotms_columns::baseline_weight(f64::from(weight))
                            })
                        };
                    // Later channel and antenna reductions use the sums of
                    // weights from the vector time transform.
                    let primary_weight = primary_weight_grid
                        .filter(|_| !average_operands)
                        .map(|grid| plotms_columns::bin_weight(grid, *corr_index, bin, &flags))
                        .or_else(|| {
                            let (flagged, weight) = plotms_columns::channel_bin_weight(
                                time_antenna_spectrum.filter(|_| !bin.averaged)?,
                                *corr_index,
                                bin,
                                &flags,
                                plotms_columns::ChannelWeightMask::FlagsOnly,
                            );
                            Some(if flagged { 0.0 } else { f64::from(weight) })
                        });
                    let flagged_sample = show_flagged && flags[(*corr_index, bin.start)];
                    for (index, (grid, buffer)) in visibility_grids
                        .iter()
                        .zip(&mut bin_buffers)
                        .enumerate()
                        .filter(|_| !average_operands)
                    {
                        let spectrum = column_weights.get(index).copied().flatten();
                        if antenna_channels {
                            buffer.collect_channel_average(
                                grid,
                                &flags,
                                [spectrum, None],
                                *corr_index,
                                bin,
                                false,
                            );
                        } else {
                            let mask = if flagged_sample {
                                plotms_ordinates::BinSampleMask::FlaggedAverage
                            } else {
                                plotms_ordinates::BinSampleMask::Clear
                            };
                            buffer.collect(grid, &flags, spectrum, *corr_index, bin, mask);
                            if channel_baselines || spw_channels {
                                buffer.normalize_vector_average();
                            }
                        }
                    }
                    let samples = |axis| {
                        visibility_columns.samples(axis, &visibility_grids, &bin_buffers, false)
                    };
                    // The scalar reducer packs the amplitude of a DATA cube
                    // even when the iterator substitutes FLOAT_DATA for it.
                    let preserve_float_sign =
                        |axis| visibility_columns.column_on_axis(axis) == Some(MsDataColumn::Float);
                    let x_samples = samples(0);
                    collect_bin_flags_into(&flags, *corr_index, bin, &mut flag_samples);
                    if let Some(grid) = weight_spectrum_grid.as_ref() {
                        collect_bin_float_samples_into(
                            grid,
                            &flags,
                            *corr_index,
                            bin,
                            &mut weight_spectrum_samples,
                        );
                    } else {
                        weight_spectrum_samples.clear();
                    }
                    if let Some(grid) = sigma_spectrum_grid.as_ref() {
                        collect_bin_float_samples_into(
                            grid,
                            &flags,
                            *corr_index,
                            bin,
                            &mut sigma_spectrum_samples,
                        );
                    } else {
                        sigma_spectrum_samples.clear();
                    }
                    let color_correlation =
                        if collapsed_uv && input_color == MsColorAxis::Correlation {
                            let index = plotms_row_axes::averaged_uvw_cache_index(
                                time_bins.expect("collapsed UV uses time groups"),
                                group.expect("collapsed UV uses an averaged group"),
                                bin_count,
                                if uv_channels { bin.ordinal } else { 0 },
                            );
                            &selected_correlations[index % selected_correlations.len()].1
                        } else {
                            corr_label
                        };
                    let (group_key, group_label) = visibility_group(
                        input_color,
                        representative_field,
                        &field,
                        spw_id,
                        &spectral_window,
                        representative_scan,
                        antenna1_value,
                        antenna2_value,
                        Some(color_correlation),
                    );
                    let point_key = build_averaging_point_key(
                        AveragingPointKeyInputs {
                            field_id: field_id_value,
                            scan_number: scan_number_value,
                            spw_id,
                            baseline: baseline_token.clone(),
                            // The reducer sees the selected cube. Raw slots can
                            // differ between data descriptions in the same bin.
                            corr_index: if scalar_intervals || time_antenna || spw_average {
                                selected_slot
                            } else {
                                *corr_index
                            },
                            row_time_value,
                            group,
                            bin,
                            pair_selected_channels: spw_average,
                        },
                        spec,
                        &time_scope_origins,
                    );
                    let panel = panels
                        .get_mut(&panel_key)
                        .expect("panel inserted before mutation");
                    let mut panel_used = false;
                    for (y_index, y_axis) in spec.y_axes.iter().copied().enumerate() {
                        let (series_key, series_label, color_group) = visibility_columns
                            .series_identity(spec, y_index, &group_key, &group_label);
                        let series = panel.series.entry(series_key).or_insert_with(|| {
                            AveragedSeriesAccumulator {
                                label: series_label,
                                color_group,
                                y_axis,
                                y_index,
                                ..Default::default()
                            }
                        });
                        let accumulator = series.points.entry(point_key.clone()).or_default();
                        if show_flagged && !accumulator.accept_visibility_sample(flagged_sample) {
                            continue;
                        }
                        if let Some(weight) = primary_weight {
                            let sum = accumulator.combined_primary_weight.get_or_insert(0.0);
                            if time_antenna {
                                *sum = f64::from(*sum as f32 + weight as f32);
                            } else {
                                *sum += weight;
                            }
                        }
                        if accumulator.representative.is_none() {
                            accumulator.display_channel = bin.display_channel();
                            accumulator.representative = Some(MsScatterPointRef {
                                row: representative_row,
                                corr: *corr_index,
                                chan_start: bin.start,
                                chan_end: bin.end,
                            });
                        }

                        let y_samples = samples(y_index + 1);
                        let mut series_used =
                            !x_samples.values.is_empty() || !y_samples.values.is_empty();
                        if average_operands {
                            for (axis, average) in [
                                (0, &mut accumulator.x_visibility),
                                (y_index + 1, &mut accumulator.y_visibility),
                            ] {
                                if let Some(grid) = visibility_columns.grid(axis, &visibility_grids)
                                {
                                    series_used |= if scalar_operands {
                                        average.add_scalar_sample(
                                            grid,
                                            &flags,
                                            plotms_ordinates::ScalarTimeSample {
                                                row_weight: scalar_weight,
                                                channel_spectra: scalar_channels.then_some([
                                                    axis_weight(axis),
                                                    time_model_weight
                                                        .filter(|_| propagate_channel_weights),
                                                ]),
                                                conjugate,
                                                preserve_float_sign: preserve_float_sign(axis),
                                            },
                                            *corr_index,
                                            bin,
                                        )
                                    } else {
                                        average.add_vector_time_samples(
                                            grid,
                                            &flags,
                                            plotms_ordinates::VectorTimeWeights {
                                                spectra: [axis_weight(axis), time_model_weight],
                                                observed_operands: [
                                                    axis_weight_kind(axis) == WeightKind::Observed,
                                                    time_model_kind == Some(WeightKind::Observed),
                                                ],
                                                primary: time_channel_primary,
                                                equal_channel_means: (time_antenna_channels
                                                    || time_spw
                                                    || direct_time_projection)
                                                    && model_channels_equally,
                                            },
                                            *corr_index,
                                            bin,
                                            flagged_sample,
                                        )
                                    };
                                }
                            }
                        } else if scalar_visibility {
                            accumulator.x_visibility.add_scalar(
                                x_samples,
                                scalar_weight,
                                conjugate,
                                preserve_float_sign(0),
                            );
                            accumulator.y_visibility.add_scalar(
                                y_samples,
                                scalar_weight,
                                conjugate,
                                preserve_float_sign(y_index + 1),
                            );
                        } else if average_model_channels_equally {
                            for (axis, average) in [
                                (0, &mut accumulator.x_visibility),
                                (y_index + 1, &mut accumulator.y_visibility),
                            ] {
                                if let Some(grid) = visibility_columns.grid(axis, &visibility_grids)
                                {
                                    average.add_channel_time_row(grid, &flags, *corr_index, bin);
                                }
                            }
                        } else {
                            accumulator.x_visibility.add(
                                x_samples,
                                scalar_weight,
                                conjugate,
                                unbinned_antenna,
                            );
                            accumulator.y_visibility.add(
                                y_samples,
                                scalar_weight,
                                conjugate,
                                unbinned_antenna,
                            );
                        }
                        if calibration.is_some()
                            && spec.averaging.avgtime.is_some()
                            && time_group.is_none()
                            && !scalar_intervals
                        {
                            push_numeric_axis_sample(
                                accumulator,
                                MsAxis::Time,
                                shared_time_scope_value.unwrap_or(row_time_value),
                            );
                            push_time_interval_sample(accumulator, row_interval_value);
                        }
                        match spec.x_axis {
                            MsAxis::Row if averaged_row_numbers.is_some() => {
                                series_used = true;
                            }
                            MsAxis::Time
                                if calibration.is_some() && spec.averaging.avgtime.is_some() =>
                            {
                                series_used = true;
                            }
                            MsAxis::Flag => {
                                accumulator.has_flag_samples = true;
                                accumulator.any_flag_sample |=
                                    flag_samples.iter().any(|flag| *flag);
                                series_used = true;
                            }
                            MsAxis::FlagRow => {
                                accumulator.has_flag_row_samples = true;
                                accumulator.any_flag_row |= flag_row_value;
                                series_used = true;
                            }
                            axis if (weighted_time_uvw && axis.uses_uvw())
                                || axis == MsAxis::Interval
                                || (average_statistics && axis.is_weight_axis()) =>
                            {
                                series_used = true;
                            }
                            MsAxis::Channel | MsAxis::Frequency if spw_average => {
                                series_used = true;
                            }
                            axis if !axis.is_visibility_math() => {
                                if let Some(value) = compute_axis_value(
                                    axis,
                                    VisibilityBinSamples::default(),
                                    &flag_samples,
                                    &weight_spectrum_samples,
                                    &sigma_spectrum_samples,
                                    flag_row_value,
                                    row_weight,
                                    row_sigma,
                                    field_id_value,
                                    antenna1_value,
                                    spec.averaging.scalar,
                                    bin,
                                    spectral_context.as_ref(),
                                    RowCoordinates {
                                        time: row_time_value,
                                        interval: None,
                                        uvw: row_uvw_value,
                                        observatory_geometry: false,
                                    },
                                    derived_engine.as_ref(),
                                )? {
                                    let value = if axis == MsAxis::Time {
                                        shared_time_scope_value.unwrap_or(value)
                                    } else {
                                        value
                                    };
                                    push_numeric_axis_sample(accumulator, axis, value);
                                    if axis == MsAxis::Time {
                                        push_time_interval_sample(accumulator, row_interval_value);
                                    }
                                    series_used = true;
                                }
                            }
                            _ => {}
                        }
                        match y_axis {
                            MsAxis::Time
                                if calibration.is_some() && spec.averaging.avgtime.is_some() =>
                            {
                                series_used = true;
                            }
                            MsAxis::Flag => {
                                accumulator.has_flag_samples = true;
                                accumulator.any_flag_sample |=
                                    flag_samples.iter().any(|flag| *flag);
                                series_used = true;
                            }
                            MsAxis::FlagRow => {
                                accumulator.has_flag_row_samples = true;
                                accumulator.any_flag_row |= flag_row_value;
                                series_used = true;
                            }
                            axis if (weighted_time_uvw && axis.uses_uvw())
                                || axis == MsAxis::Interval
                                || (average_statistics && axis.is_weight_axis()) =>
                            {
                                series_used = true;
                            }
                            axis if !axis.is_visibility_math() => {
                                if let Some(value) = compute_axis_value(
                                    axis,
                                    VisibilityBinSamples::default(),
                                    &flag_samples,
                                    &weight_spectrum_samples,
                                    &sigma_spectrum_samples,
                                    flag_row_value,
                                    row_weight,
                                    row_sigma,
                                    field_id_value,
                                    antenna1_value,
                                    spec.averaging.scalar,
                                    bin,
                                    spectral_context.as_ref(),
                                    RowCoordinates {
                                        time: row_time_value,
                                        interval: None,
                                        uvw: row_uvw_value,
                                        observatory_geometry: false,
                                    },
                                    derived_engine.as_ref(),
                                )? {
                                    let value = if axis == MsAxis::Time {
                                        shared_time_scope_value.unwrap_or(value)
                                    } else {
                                        value
                                    };
                                    push_numeric_axis_sample(accumulator, axis, value);
                                    if axis == MsAxis::Time {
                                        push_time_interval_sample(accumulator, row_interval_value);
                                    }
                                    series_used = true;
                                }
                            }
                            _ => {}
                        }
                        if series_used {
                            panel_used = true;
                        }
                    }
                    if panel_used {
                        row_panels.insert(panel_key.clone());
                    }
                }
            }
        }

        for panel_key in row_panels {
            if let Some(panel) = panels.get_mut(&panel_key) {
                panel.contributing_rows.insert(row);
            }
            contributing_rows.insert(row);
        }
    }

    let retain_empty_panel = average_operands
        || symbols.is_some()
        || spw_average
        || antenna_channels
        || scalar_antenna_time
        || retain_empty_ordinate_list
        || iterated_ordinate_list
        || (scalar_average
            && spec.secondary_y_axis().is_some()
            && iteraxis.is_none()
            && plotms_columns::uses_ordinary_time_visibility(spec))
        || (scalar_average && collapse_baselines)
        || (combined_vector_average && !collapse_baselines && iteraxis.is_none());
    AveragedScatterOutput {
        inputs: AveragedProjectionInputs {
            ms,
            spec,
            calibration,
            time_bins,
            phase_shift,
            averaged_row_numbers,
            data_description,
            selected_data_desc_ids: &selected_data_desc_ids,
            selected_field_ids: &selected_field_ids,
            selected_times: &selected_times,
            derived_engine,
            requested_freqframe,
            time_reporting_frame,
            time_bounds,
            scalar_time_bounds,
            time_spectra,
            uv_selections,
            clear_uv_channels,
            average_statistics,
            collapsed_uv,
            uv_channels,
            scalar_intervals,
            scalar_visibility,
            weighted_time_uvw,
            time_wavelengths,
            time_reporting_axes,
        },
        row_numbers: &row_numbers,
        visibility_columns: &visibility_columns,
        selected_scan_numbers: &selected_scan_numbers,
        field,
        scalar_spans,
        averaged_antenna_color,
        antenna_correlation_conflict,
        time_baseline_stats,
        spw_groups,
        averaged_rows,
        time_uvws,
        time_statistics,
        panel_order,
        panels,
        contributing_rows,
        symbols,
        iteraxis,
        extended_derived,
        direct_time_projection,
        time_antenna,
        time_spw,
        spw_antenna,
        spw_baseline,
        scalar_average,
        spw_average,
        antenna_average,
        spans_scan_or_field,
        vector_span_models,
        paged_iteration,
        show_flagged,
        retain_empty_panel,
        sort_series: time_weight_kinds.is_some()
            || collapse_baselines
            || scalar_average
            || antenna_average
            || spw_average,
        combined_vector_average,
    }
    .build(point_budget)
}
