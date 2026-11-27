// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn build_generic_visibility_scatter(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
    calibration: Option<&PlotMsInput<'_>>,
) -> Result<MsPlotPayload, String> {
    let needs_row_axes = plotms_row_axes::requested(spec);
    let pointing = calibration.and_then(|input| input.pointing);
    let raw_symbols = calibration.and_then(|input| input.raw_symbols);
    let show_flagged = raw_symbols.is_some_and(|symbols| symbols.flagged);
    if needs_row_axes && calibration.is_none() {
        return Err("MeasurementSet row identifier axes require the public plotms task".into());
    }
    let array_horizontal = calibration.is_some_and(|input| input.rows.is_none())
        && spec.y_axes.len() == 1
        && plotms_row_axes::array_horizontal_pair(spec.x_axis, spec.y_axis());
    let correlation_metadata_panels = (array_horizontal
        || (spec.x_axis.uses_uvw() && matches!(spec.y_axes.as_slice(), [axis] if axis.uses_uvw())))
        && spec.iteration.iteraxis == Some(MsIterationAxis::Correlation);
    let antenna_horizontal = spec
        .y_axes
        .iter()
        .copied()
        .any(MsAxis::is_antenna_horizontal);
    if antenna_horizontal
        && (calibration.is_none_or(|input| input.rows.is_some())
            || spec.x_axis != MsAxis::Channel
            || spec.y_axes.len() != 1
            || uses_extended_averaging(spec)
            || spec.averaging.avgchannel.is_some_and(|width| width > 1)
            || spec.iteration.iteraxis.is_some())
    {
        return Err("This PlotMS antenna horizontal axis combination is pending (#1032)".into());
    }
    let ephemeris_ordinate = spec.y_axes.iter().copied().any(MsAxis::is_ephemeris);
    if ephemeris_ordinate
        && (calibration.is_none_or(|input| input.rows.is_some())
            || !matches!(spec.x_axis, MsAxis::Channel | MsAxis::Time)
            || !spec.y_axes.iter().copied().all(MsAxis::is_ephemeris)
            || uses_extended_averaging(spec)
            || spec.averaging.avgchannel.is_some_and(|width| width > 1)
            || spec.iteration.iteraxis.is_some())
    {
        return Err("This PlotMS ephemeris axis combination is pending (#1032)".into());
    }
    let coordinate_mask = plotms_row_axes::unaveraged_coordinate_mask(
        spec,
        calibration.is_some_and(|input| input.rows.is_none()),
    )?;
    let metadata_pair = if pointing.is_some() {
        Some(plotms_ordinates::PointDimensions {
            channel: spec.x_axis.uses_channel_bins()
                || spec.y_axes.iter().any(|axis| axis.uses_channel_bins()),
            baseline: true,
        })
    } else if array_horizontal || ephemeris_ordinate {
        Some(plotms_ordinates::PointDimensions {
            channel: spec.x_axis == MsAxis::Channel,
            baseline: false,
        })
    } else if needs_row_axes {
        plotms_row_axes::metadata_pair(spec)
    } else {
        coordinate_mask
    };
    let metadata_pair = metadata_pair.map(|mut dimensions| {
        dimensions.baseline |= matches!(
            spec.iteration.iteraxis,
            Some(MsIterationAxis::Antenna | MsIterationAxis::Baseline)
        );
        dimensions
    });
    if metadata_pair.is_some() && calibration.is_some_and(|input| input.rows.is_some()) {
        return Err(
            "PlotMS metadata-only row identifiers with a callibrary are pending (#1032)".into(),
        );
    }
    let timing_enabled = VisibilityScatterTiming::enabled();
    let scatter_started = Instant::now();
    let mut checkpoint_started = Instant::now();
    let mut scatter_timing = VisibilityScatterTiming::default();
    if uses_extended_averaging(spec) {
        return build_generic_visibility_scatter_with_averaging(
            ms,
            selection,
            spec,
            point_budget,
            calibration,
        );
    }

    let listobs_options = selection.to_summary_options();
    let row_numbers = match calibration {
        Some(calibration) => calibration.row_order.to_vec(),
        None => resolve_selected_rows_with_msselect(ms, selection, &listobs_options)?,
    };
    let first_selected_row = row_numbers.first().copied();
    let uses_row_numbers = spec.x_axis == MsAxis::Row || spec.y_axes.contains(&MsAxis::Row);
    let selection_order = uses_row_numbers.then(|| row_numbers.clone());
    let needs_interval = spec.x_axis == MsAxis::Interval || spec.y_axes.contains(&MsAxis::Interval);
    let use_ordinate_masks = spec.y_axes.len() > 2
        && calibration.is_some_and(|input| input.rows.is_none())
        && spec.iteration.iteraxis.is_none()
        && spec.averaging.avgchannel.is_none_or(|width| width <= 1)
        && plotms_columns::uses_channel_ordinate_masks(spec.x_axis, spec.y_axes.iter().copied());
    let ordered_feed_selection = selection
        .feed
        .as_deref()
        .is_some_and(|feed| !feed.is_empty())
        && spec.averaging.avgchannel.is_none_or(|width| width <= 1);
    let mut phase_shift = calibration
        .and_then(|input| input.phasecenter.as_ref())
        .map(|target| plotms_phasecenter::PhaseCenterShift::new(ms, target));
    let channel_derived = calibration.is_some_and(|input| {
        input.rows.is_none() && plotms_columns::uses_channel_derived(spec, input.x_data_column)
    });
    let needs_visibility_grid = spec.x_axis.is_visibility_math()
        || spec.y_axes.iter().copied().any(MsAxis::is_visibility_math);
    let channel_only_visibility = needs_visibility_grid
        && calibration.is_some_and(|input| input.rows.is_none())
        && spec.averaging.avgchannel.is_some_and(|width| width > 1)
        && !uses_extended_averaging(spec);
    let iterated_ordinate_list = iterates_plotms_ordinates(spec, calibration);
    let row_iteration = spec.iteration.iteraxis.filter(|axis| axis.groups_by_row());
    let paged_iteration = iterated_ordinate_list
        || calibration.is_some_and(|input| input.rows.is_none())
            && spec.iteration.iteraxis.is_some();
    let calibrated_ordinate_list = calibration
        .is_some_and(|input| input.rows.is_some() && input.secondary_y_data_column.is_some());
    let order_ordinate_list = iterated_ordinate_list || calibrated_ordinate_list;
    let use_iterator_order = needs_row_axes
        || array_horizontal
        || antenna_horizontal
        || ephemeris_ordinate
        || coordinate_mask.is_some()
        || pointing.is_some()
        || raw_symbols.is_some()
        || paged_iteration
        || phase_shift.is_some()
        || row_iteration.is_some()
        || channel_derived
        || channel_only_visibility
        || order_ordinate_list
        || calibration.is_some()
            && (ordered_feed_selection
                || use_ordinate_masks
                || spec.x_axis.is_interval_or_wavelength()
                || spec.y_axes.contains(&MsAxis::UvDistance)
                || spec
                    .y_axes
                    .iter()
                    .copied()
                    .any(MsAxis::is_interval_or_wavelength));
    let (row_numbers, buffer_starts, phase_reference_rows) = if use_iterator_order {
        let descriptions = selected_i32_values(ms.main_table(), "DATA_DESC_ID", &row_numbers)?;
        let ordered = casa_mstransform::selected_plotms_integration_order(
            ms,
            row_numbers.into_iter().zip(descriptions),
            false,
            phase_shift.is_some(),
        )
        .map_err(|error| error.to_string())?;
        (
            ordered.rows,
            ordered.starts_visibility_buffer,
            ordered.phase_reference_rows,
        )
    } else {
        (row_numbers, Vec::new(), Vec::new())
    };
    let selected_row_count = row_numbers.len();
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("resolve_selected_rows", &mut checkpoint_started);
    }

    let needs_spectral_coordinates = (phase_shift.is_some() && spec.transforms.freqframe.is_some())
        || spec.x_axis.uses_spectral_coordinates()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_spectral_coordinates);
    let needs_flag_grid = needs_visibility_grid
        || metadata_pair.is_some()
        || matches!(spec.x_axis, MsAxis::Flag)
        || spec.y_axes.iter().copied().any(|axis| {
            matches!(
                axis,
                MsAxis::Flag | MsAxis::WeightSpectrum | MsAxis::SigmaSpectrum
            )
        });
    let uses_weight = |axis| {
        matches!(
            axis,
            MsAxis::Weight | MsAxis::WeightSpectrum | MsAxis::WeightedAmplitude
        )
    };
    let needs_weight_values =
        uses_weight(spec.x_axis) || spec.y_axes.iter().copied().any(uses_weight);
    let needs_sigma_values = matches!(spec.x_axis, MsAxis::Sigma | MsAxis::SigmaSpectrum)
        || spec
            .y_axes
            .iter()
            .copied()
            .any(|axis| matches!(axis, MsAxis::Sigma | MsAxis::SigmaSpectrum));
    let needs_uvw_values =
        spec.x_axis.uses_uvw() || spec.y_axes.iter().copied().any(MsAxis::uses_uvw);
    let requested_freqframe = parse_frequency_frame(spec.transforms.freqframe.as_deref())?;
    let field = ms.field().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    if uses_row_numbers && requested_freqframe.is_some() {
        return Err("PlotMS Row coordinates with a reporting frame are pending (#1032)".into());
    }
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("open_metadata_subtables", &mut checkpoint_started);
    }

    let requested_corr_codes = selection
        .correlation
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

    let row_numbers = if use_iterator_order {
        // PlotMS exports the full selected iterator with its original buffer starts.
        row_numbers
    } else {
        predecimate_visibility_rows(
            ms,
            row_numbers,
            spec,
            point_budget,
            VisibilityPredecimationContext {
                data_description: &data_description,
                spectral_window: &spectral_window,
                polarization: &polarization,
                requested_corr_codes: requested_corr_codes.as_deref(),
            },
        )?
    };
    let sampled_row_count = row_numbers.len();
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("predecimate_rows", &mut checkpoint_started);
    }
    let row_sampling_summary = if sampled_row_count < selected_row_count {
        format!(" Sampled rows={sampled_row_count}/{selected_row_count}.")
    } else {
        String::new()
    };

    let visibility_columns =
        plotms_ordinates::VisibilityColumns::prepare(ms, spec, &row_numbers, calibration)?;
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("load_visibility_data", &mut checkpoint_started);
    }
    let selected_flags = if needs_flag_grid {
        Some(SelectedArrayColumn::load_for_plot(
            ms.main_table(),
            "FLAG",
            &row_numbers,
            calibration,
        )?)
    } else {
        None
    };
    let selected_flag_rows = selected_bool_values(ms.main_table(), "FLAG_ROW", &row_numbers)?;
    let selected_data_desc_ids =
        selected_i32_values(ms.main_table(), "DATA_DESC_ID", &row_numbers)?;
    let selected_field_ids = selected_i32_values(ms.main_table(), "FIELD_ID", &row_numbers)?;
    let selected_scan_numbers = selected_i32_values(ms.main_table(), "SCAN_NUMBER", &row_numbers)?;
    let selected_antenna1 = selected_i32_values(ms.main_table(), "ANTENNA1", &row_numbers)?;
    let selected_antenna2 = selected_i32_values(ms.main_table(), "ANTENNA2", &row_numbers)?;
    let row_axes = needs_row_axes
        .then(|| {
            plotms_row_axes::RowAxes::read(
                ms,
                spec,
                &row_numbers,
                selection_order.as_deref(),
                &buffer_starts,
                [&selected_antenna1, &selected_antenna2],
            )
        })
        .transpose()?;
    let selected_times = selected_f64_values(ms.main_table(), "TIME", &row_numbers)?;
    let pointing_coordinates = pointing
        .map(|axes| plotms_pointing::PointingCoordinates::read(ms, axes, &selected_antenna1))
        .transpose()?;
    let mut pointing_values = Vec::new();
    let selected_intervals = if needs_interval {
        let mut intervals = selected_f64_values(ms.main_table(), "INTERVAL", &row_numbers)?;
        let mut first_interval = 0.0;
        for (interval, &start) in intervals.iter_mut().zip(&buffer_starts) {
            if start {
                first_interval = *interval;
            }
            *interval = first_interval;
        }
        intervals
    } else {
        Vec::new()
    };
    let selected_uvw = if needs_uvw_values || phase_shift.is_some() {
        selected_uvw_values(ms.main_table(), "UVW", &row_numbers)?
    } else {
        Vec::new()
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("load_aux_columns", &mut checkpoint_started);
    }
    let derived_engine = if ephemeris_ordinate {
        Some(MsCalEngine::for_ephemeris_scalars(ms).map_err(|error| error.to_string())?)
    } else if antenna_horizontal
        || spec.x_axis.uses_derived_geometry()
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
    if ephemeris_ordinate
        && !derived_engine.as_ref().is_some_and(|engine| {
            (0..field.row_count()).any(|field_id| engine.field_moves(field_id))
        })
    {
        return Err("ephemeris data is not available for this MeasurementSet".into());
    }
    let buffer_geometry_axis = if array_horizontal {
        Some(spec.y_axis())
    } else if coordinate_mask.is_some() && spec.x_axis == MsAxis::Elevation {
        Some(spec.x_axis)
    } else if plotms_row_axes::uses_mixed_row_geometry(spec) {
        Some(MsAxis::Elevation)
    } else {
        None
    };
    let mean_antenna_position = buffer_geometry_axis
        .filter(|axis| axis.uses_derived_geometry())
        .map(|_| plotms_row_axes::mean_antenna_position(ms))
        .transpose()?;
    let antenna_axes = antenna_horizontal
        .then(|| plotms_row_axes::AntennaAxes::read(ms, spec.y_axis()))
        .transpose()?;
    let geometry_dedup_required = !use_ordinate_masks
        && buffer_geometry_axis.is_none()
        && (spec.x_axis.uses_derived_geometry()
            || spec
                .y_axes
                .iter()
                .copied()
                .any(MsAxis::uses_derived_geometry));
    let mut geometry_samples_seen =
        std::collections::BTreeSet::<(String, String, String, u64, i32)>::new();
    let chan_freq = if needs_spectral_coordinates {
        Some(ChanFreqColumn::new(spectral_window.table()))
    } else {
        None
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("prepare_context", &mut checkpoint_started);
    }

    let iteraxis = spec.iteration.iteraxis;
    let mut selected_channel_bins = HashMap::new();
    let mut panel_order = Vec::<String>::new();
    let mut panels = std::collections::BTreeMap::<String, ScatterPanelAccumulator>::new();
    let mut contributing_rows = 0usize;
    let mut contributing_points = 0usize;
    let include_row_flagged_points =
        spec.x_axis.uses_flag_rows() || spec.y_axes.iter().copied().any(MsAxis::uses_flag_rows);
    let use_spectral_weights = calibration.is_some()
        && needs_visibility_grid
        && spec.averaging.avgchannel.is_some_and(|width| width > 1);
    let project_plotms_columns = calibration
        .is_some_and(|input| input.rows.is_none() || input.secondary_y_data_column.is_some());
    let statistics_kinds = calibration
        .filter(|_| {
            project_plotms_columns
                && use_spectral_weights
                && (needs_weight_values || needs_sigma_values)
        })
        .map(|input| plotms_columns::weight_kinds(ms, spec, input));
    // PlotMS applies scalar averaging only with the extended averaging path.
    let scalar_average = spec.averaging.scalar && !project_plotms_columns;
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
    // CASA checks the first row of the selected table before sorting its iterator.
    let spectrum_probe_slot = if channel_derived || channel_only_visibility {
        row_numbers
            .iter()
            .position(|&row| Some(row) == first_selected_row)
    } else {
        Some(0)
    };
    // The transform chooses the channel kernel from the stored spectrum even
    // when calibration supplies separate transient weights for that kernel.
    let stored_spectrum = if calibrated_ordinate_list && use_spectral_weights {
        first_selected_row
            .filter(|_| WeightSpectrumColumn::new(ms.main_table()).is_ok())
            .map(|row| ms.main_table().is_cell_defined(row, "WEIGHT_SPECTRUM"))
            .transpose()
            .map_err(|error| error.to_string())?
            .unwrap_or(false)
    } else {
        selected_weight_spectrum
            .as_ref()
            .zip(spectrum_probe_slot)
            .and_then(|(column, slot)| column.find(slot))
            .is_some()
    };
    let channel_weight_kinds = calibration
        .filter(|_| project_plotms_columns && use_spectral_weights && stored_spectrum)
        .map(|input| {
            statistics_kinds.unwrap_or_else(|| plotms_columns::weight_kinds(ms, spec, input))
        });
    let additional_ordinates = calibration.map_or(&[][..], |input| input.additional_ordinates);
    let channel_uses_weight = |kind| {
        channel_weight_kinds
            .is_some_and(|kinds| kinds.uses_for_ordinates(kind, additional_ordinates))
    };
    let statistics_use = |kind| {
        statistics_kinds.is_some_and(|kinds| kinds.row_weight == kind || kinds.row_sigma == kind)
    };
    let needs_observed_weights =
        channel_uses_weight(WeightKind::Observed) || statistics_use(WeightKind::Observed);
    let needs_calibrated_weights =
        channel_uses_weight(WeightKind::Calibrated) || statistics_use(WeightKind::Calibrated);
    let selected_weights = if needs_weight_values || needs_calibrated_weights {
        Some(SelectedArrayColumn::load_for_plot(
            ms.main_table(),
            "WEIGHT",
            &row_numbers,
            calibration,
        )?)
    } else {
        None
    };
    let selected_sigmas = if needs_sigma_values || needs_observed_weights {
        Some(SelectedArrayColumn::load(
            ms.main_table(),
            "SIGMA",
            &row_numbers,
        )?)
    } else {
        None
    };
    let selected_sigma_spectrum = if needs_sigma_spectrum || needs_observed_weights {
        SigmaSpectrumColumn::new(ms.main_table())
            .ok()
            .map(|_| SelectedArrayColumn::load(ms.main_table(), "SIGMA_SPECTRUM", &row_numbers))
            .transpose()?
    } else {
        None
    };
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("load_optional_spectra", &mut checkpoint_started);
    }

    let mut visibility_grids = Vec::new();
    let mut bin_buffers = visibility_columns.buffers();
    // Colorized PlotMS plots use the group's symbol for both visible masks.
    let flagged_series = (show_flagged && spec.color_by == MsColorAxis::None)
        .then(|| ("flagged".into(), "Flagged".into(), "flagged".into()));
    let mut ordinate_points = (use_ordinate_masks || metadata_pair.is_some())
        .then(plotms_ordinates::CollapsedPointMask::default);
    let mut projected_flags = Vec::new();
    let mut buffer_geometry_value = None;
    let mut ephemeris = plotms_row_axes::BufferEphemeris::default();
    let antenna_visits = antenna_axes
        .as_ref()
        .map(|axes| axes.visits(&buffer_starts));
    let ordinary_rows = if antenna_horizontal {
        0
    } else {
        row_numbers.len()
    };
    let visits = antenna_visits
        .into_iter()
        .flatten()
        .chain((0..ordinary_rows).map(|row_slot| (row_slot, None)));
    let mut phase_reference_time = 0.0;
    for (visit_index, (row_slot, antenna_visit)) in visits.enumerate() {
        // The visibility iterator uses the first selected epoch of a chunk
        // for a polynomial FIELD direction throughout that chunk.
        if phase_reference_rows.get(row_slot) == row_numbers.get(row_slot) {
            phase_reference_time = selected_times[row_slot];
        }
        if let Some(mask) = &mut ordinate_points {
            mask.begin_row(row_slot, buffer_starts[row_slot]);
        }
        let flag_row_value = selected_flag_rows[row_slot];
        // PlotMS iterator coordinates and channel averages use the cube mask.
        if flag_row_value
            && !include_row_flagged_points
            && !use_iterator_order
            && statistics_kinds.is_none()
        {
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

        let row_grid_started = timing_enabled.then(Instant::now);
        visibility_columns.read_row(row_slot, project_plotms_columns, &mut visibility_grids)?;
        if let Some(started) = row_grid_started {
            scatter_timing.row_grid += started.elapsed();
        }
        let expected_corr_count = corr_types.len();
        let spw_row = usize::try_from(spw_id)
            .map_err(|_| format!("invalid DATA_DESCRIPTION SPECTRAL_WINDOW_ID {spw_id}"))?;
        let expected_chan_count = spectral_window
            .num_chan(spw_row)
            .map_err(|error| error.to_string())
            .and_then(|count| {
                usize::try_from(count)
                    .map_err(|_| format!("invalid SPECTRAL_WINDOW NUM_CHAN {count}"))
            })?;
        let (corr_count, chan_count) = visibility_grids
            .first()
            .map(|grid| (grid.corr_count, grid.chan_count))
            .unwrap_or((expected_corr_count, expected_chan_count));
        let field_id_value = selected_field_ids[row_slot];
        let row_uvw_value = if let Some(shift) = &mut phase_shift {
            shift.apply(
                ms,
                field_id_value,
                spw_row,
                phase_reference_time,
                selected_uvw[row_slot],
                &mut visibility_grids,
            )?
        } else {
            selected_uvw.get(row_slot).copied().unwrap_or([0.0; 3])
        };
        let flags = selected_flags
            .as_ref()
            .map(|column| visibility_flags(column, row_slot, [corr_count, chan_count]))
            .transpose()?;
        let row_arrays_started = timing_enabled.then(Instant::now);
        let weight_values = selected_weights
            .as_ref()
            .map(|column| float_axis_values(column.get(row_slot)?, corr_count, "WEIGHT"))
            .transpose()?
            .unwrap_or_else(|| vec![0.0; corr_count]);
        let sigma_values = selected_sigmas
            .as_ref()
            .map(|column| float_axis_values(column.get(row_slot)?, corr_count, "SIGMA"))
            .transpose()?
            .unwrap_or_else(|| vec![0.0; corr_count]);
        let weight_spectrum_cell = match &selected_weight_spectrum {
            Some(column) if project_plotms_columns => column.find(row_slot),
            Some(column) => Some(column.get(row_slot)?),
            None => None,
        };
        let weight_spectrum_grid = weight_spectrum_cell
            .map(|cell| float_grid_from_array(cell, "WEIGHT_SPECTRUM"))
            .transpose()?
            .or_else(|| {
                (needs_weight_spectrum || needs_calibrated_weights)
                    .then(|| scalar_values_to_grid(&weight_values, chan_count))
            });
        let sigma_spectrum_cell = match &selected_sigma_spectrum {
            Some(column) if calibration.is_none() => Some(column.get(row_slot)?),
            Some(column) => column.find(row_slot),
            None => None,
        };
        let sigma_spectrum_grid = sigma_spectrum_cell
            .map(|cell| float_grid_from_array(cell, "SIGMA_SPECTRUM"))
            .transpose()?
            .or_else(|| {
                (needs_sigma_spectrum || needs_observed_weights)
                    .then(|| scalar_values_to_grid(&sigma_values, chan_count))
            });
        let observed_weights = sigma_spectrum_grid
            .as_ref()
            .filter(|_| needs_observed_weights)
            .map(plotms_columns::inverse_variance);
        let statistic_grid = |kind| match kind {
            WeightKind::Observed => observed_weights.as_ref(),
            WeightKind::Calibrated => weight_spectrum_grid.as_ref(),
            WeightKind::Unit => None,
        };
        let weight_grid = |kind| match kind {
            WeightKind::Calibrated
                if !use_spectral_weights
                    || (project_plotms_columns && channel_weight_kinds.is_none()) =>
            {
                None
            }
            _ => statistic_grid(kind),
        };
        let axis_weights = channel_weight_kinds
            .map_or([WeightKind::Calibrated; 3], |kinds| kinds.axes)
            .map(weight_grid);
        let additional_weights = additional_ordinates.iter().map(|&(_, column)| {
            channel_weight_kinds.and_then(|kinds| weight_grid(kinds.for_column(column)))
        });
        let column_weights =
            visibility_columns.column_weights(axis_weights.into_iter().chain(additional_weights));
        let model_weights = channel_weight_kinds
            .and_then(|kinds| kinds.model)
            .and_then(weight_grid);
        let primary_weight_grid = channel_weight_kinds.and_then(|kinds| weight_grid(kinds.primary));
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
        if let Some(started) = row_arrays_started {
            scatter_timing.row_arrays += started.elapsed();
        }

        let selected_correlations =
            select_correlation_slots(corr_count, &corr_types, requested_corr_codes.as_deref());
        if selected_correlations.is_empty() {
            continue;
        }
        let correlation_required = metadata_pair.is_some()
            || antenna_horizontal
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

        let channel_bins = if metadata_pair.is_some()
            && calibration.is_none_or(|input| !input.channels.contains_key(&spw_id))
        {
            Cow::Owned(channel_bins(chan_count, None)?)
        } else {
            plot_channel_bins_for_window(
                chan_count,
                spec,
                spw_id,
                calibration,
                &mut selected_channel_bins,
            )?
        };
        if show_flagged
            && let Some(dimensions) = metadata_pair
            && (dimensions.baseline || buffer_starts[row_slot])
        {
            let next_row = row_slot + 1;
            let end = if dimensions.baseline {
                next_row
            } else {
                buffer_starts[next_row..]
                    .iter()
                    .position(|&start| start)
                    .map_or(buffer_starts.len(), |offset| next_row + offset)
            };
            let column = selected_flags.as_ref().expect("metadata plots load flags");
            let flags = (row_slot..end)
                .map(|slot| visibility_flags(column, slot, [corr_count, chan_count]));
            plotms_ordinates::metadata_symbol_flags(
                &mut projected_flags,
                dimensions,
                flags,
                &channel_bins,
                &selected_correlations,
            )?;
        }

        let scan_number_value = selected_scan_numbers[row_slot];
        let antenna1_value = selected_antenna1[row_slot];
        let row_time_value = selected_times[row_slot];
        if let Some(coordinates) = &pointing_coordinates {
            coordinates.values(antenna1_value, row_time_value, &mut pointing_values)?;
        }
        let row_coordinates = RowCoordinates {
            time: row_time_value,
            interval: selected_intervals.get(row_slot).copied(),
            uvw: row_uvw_value,
            observatory_geometry: use_ordinate_masks,
        };
        if (buffer_geometry_axis.is_some() || ephemeris_ordinate) && buffer_starts[row_slot] {
            if let Some(axis) = buffer_geometry_axis {
                buffer_geometry_value = Some(plotms_row_axes::array_horizontal_value(
                    derived_engine.as_ref().expect("buffer geometry engine"),
                    axis,
                    row_time_value,
                    field_id_value,
                    mean_antenna_position.clone().expect("array mean position"),
                )?);
            }
            if ephemeris_ordinate {
                ephemeris = plotms_row_axes::BufferEphemeris::read(
                    derived_engine.as_ref().expect("ephemeris engine"),
                    &spec.y_axes,
                    row_time_value,
                    field_id_value,
                )?;
            }
        }
        let antenna_geometry = antenna_axes
            .as_ref()
            .zip(antenna_visit)
            .map(|(axes, visit)| {
                axes.value(
                    derived_engine
                        .as_ref()
                        .expect("antenna horizontal geometry engine"),
                    visit.antenna,
                    selected_times[visit.buffer_start],
                    selected_field_ids[visit.buffer_start],
                    spw_id,
                )
            })
            .transpose()?;
        let spectral_context = resolve_spectral_context(
            spec,
            spw_id,
            chan_count,
            (field_id_value, row_time_value),
            chan_freq.as_ref(),
            derived_engine.as_ref(),
            calibration.and_then(|_| channel_bins.get(channel_bins.len() / 2)),
        )?;
        let reporting_spectrum = spectral_context
            .as_ref()
            .filter(|_| calibration.is_some() && spec.transforms.freqframe.is_some());

        let mut row_contributed = false;
        let mut row_panels = std::collections::BTreeSet::<String>::new();
        let group_for = |slot: usize, correlation: Option<&str>| {
            visibility_group(
                spec.color_by,
                field_id_value,
                &field,
                spw_id,
                &spectral_window,
                scan_number_value,
                selected_antenna1[slot],
                selected_antenna2[slot],
                correlation,
            )
        };
        let correlation_visits = if antenna_horizontal {
            1
        } else {
            selected_correlations.len()
        };
        for (correlation_slot, (corr_index, corr_label)) in selected_correlations
            [..correlation_visits]
            .iter()
            .enumerate()
        {
            let grouping_started = timing_enabled.then(Instant::now);
            let statistics = statistics_kinds.map(|kinds| {
                plotms_columns::ChannelStatistics::from_bins(
                    *corr_index,
                    &channel_bins,
                    flags.as_ref().expect("channel statistics load flags"),
                    statistic_grid(kinds.row_weight),
                    statistic_grid(kinds.row_sigma),
                )
            });
            let row_weight = statistics
                .as_ref()
                .map_or(weight_values[*corr_index], |statistics| statistics.weight);
            let row_sigma = statistics
                .as_ref()
                .map_or(sigma_values[*corr_index], |statistics| statistics.sigma);
            let (group_key, group_label) = group_for(row_slot, Some(corr_label));
            let (panel_key, panel_label, iteration_rank) = iteration_group(
                iteraxis,
                field_id_value,
                &field,
                spw_id,
                &spectral_window,
                scan_number_value,
                Some((
                    corr_label,
                    corr_types.get(*corr_index).copied().unwrap_or_default(),
                )),
            );
            if !panels.contains_key(&panel_key) {
                panel_order.push(panel_key.clone());
                panels.insert(
                    panel_key.clone(),
                    ScatterPanelAccumulator {
                        label: panel_label.clone(),
                        iteration_rank,
                        ..Default::default()
                    },
                );
            }
            let series_identity = spec
                .y_axes
                .iter()
                .copied()
                .enumerate()
                .map(|(y_index, y_axis)| {
                    let (series_key, series_label, color_group) =
                        visibility_columns.series_identity(spec, y_index, &group_key, &group_label);
                    (y_index, y_axis, series_key, series_label, color_group)
                })
                .collect::<Vec<_>>();
            if let Some(started) = grouping_started {
                scatter_timing.grouping += started.elapsed();
            }
            let mut flag_samples = Vec::<bool>::new();
            let mut weight_spectrum_samples = Vec::<f64>::new();
            let mut sigma_spectrum_samples = Vec::<f64>::new();
            for (bin_index, bin) in channel_bins.iter().enumerate() {
                if let Some(flags) = flags.as_ref() {
                    collect_bin_flags_into(flags, *corr_index, bin, &mut flag_samples);
                } else {
                    flag_samples.clear();
                }
                let fully_flagged = || flag_samples.iter().all(|flag| *flag);
                let transformed_channel_weights = channel_weight_kinds.is_some() && bin.averaged;
                let zero_primary_weight = transformed_channel_weights
                    && primary_weight_grid
                        .zip(flags.as_ref())
                        .is_some_and(|(weights, flags)| {
                            !plotms_columns::bin_has_weight(weights, *corr_index, bin, flags)
                        });
                let displayed_flagged = show_flagged
                    && if let Some(dimensions) =
                        metadata_pair.filter(|_| !projected_flags.is_empty())
                    {
                        projected_flags[if dimensions.channel { bin.ordinal } else { 0 }]
                    } else {
                        fully_flagged() || zero_primary_weight
                    };
                if !displayed_flagged
                    && (zero_primary_weight
                        || raw_symbols.is_some_and(|symbols| !symbols.unflagged))
                {
                    continue;
                }
                let bin_collect_started = timing_enabled.then(Instant::now);
                for (index, (grid, buffer)) in
                    visibility_grids.iter().zip(&mut bin_buffers).enumerate()
                {
                    let flags = flags.as_ref().expect("visibility data loads flags");
                    let weights = column_weights.get(index).copied().flatten();
                    if bin.averaged && (channel_derived || displayed_flagged) {
                        buffer.collect_channel_average(
                            grid,
                            flags,
                            [weights, model_weights],
                            *corr_index,
                            bin,
                            displayed_flagged && fully_flagged(),
                        );
                    } else {
                        let mask = if displayed_flagged {
                            plotms_ordinates::BinSampleMask::IncludeFlagged
                        } else {
                            plotms_ordinates::BinSampleMask::Clear
                        };
                        buffer.collect(grid, flags, weights, *corr_index, bin, mask);
                    }
                }
                if let Some(statistics) = &statistics {
                    weight_spectrum_samples.clear();
                    sigma_spectrum_samples.clear();
                    if let Some((weight, sigma)) = statistics.spectrum(bin_index) {
                        weight_spectrum_samples.push(weight);
                        sigma_spectrum_samples.push(sigma);
                    }
                } else {
                    for (grid, samples) in [
                        (&weight_spectrum_grid, &mut weight_spectrum_samples),
                        (&sigma_spectrum_grid, &mut sigma_spectrum_samples),
                    ] {
                        if let Some(grid) = grid {
                            let flags = flags.as_ref().expect("spectrum loads matching flag grid");
                            collect_bin_float_samples_into(grid, flags, *corr_index, bin, samples);
                        } else {
                            samples.clear();
                        }
                    }
                }
                if let Some(started) = bin_collect_started {
                    scatter_timing.bin_collect += started.elapsed();
                }
                let axis_started = timing_enabled.then(Instant::now);
                let axis_value = |axis_slot: usize, axis| {
                    if antenna_horizontal && axis == spec.y_axis() {
                        return Ok(antenna_geometry);
                    }
                    if let Some(value) = ephemeris.get(axis).or_else(|| {
                        buffer_geometry_value.filter(|_| buffer_geometry_axis == Some(axis))
                    }) {
                        return Ok(Some(value));
                    }
                    if let Some(values) = pointing_values.get(axis_slot.saturating_sub(1))
                        && let Some(value) = values[axis_slot.min(1)]
                    {
                        return Ok(Some(value));
                    }
                    if let Some(axes) = &row_axes
                        && let Some(value) = axes.value(axis, row_slot)
                    {
                        return Ok(Some(value));
                    }
                    let visibility = visibility_columns.samples(
                        axis_slot,
                        &visibility_grids,
                        &bin_buffers,
                        transformed_channel_weights,
                    );
                    compute_axis_value(
                        axis,
                        visibility,
                        &flag_samples,
                        &weight_spectrum_samples,
                        &sigma_spectrum_samples,
                        flag_row_value,
                        row_weight,
                        row_sigma,
                        field_id_value,
                        antenna1_value,
                        scalar_average,
                        bin,
                        spectral_context.as_ref(),
                        row_coordinates,
                        derived_engine.as_ref(),
                    )
                };
                let Some(x_value) = axis_value(0, spec.x_axis)? else {
                    continue;
                };
                let panel = panels
                    .get_mut(&panel_key)
                    .expect("panel inserted before mutation");
                for (y_index, y_axis, series_key, series_label, color_group) in &series_identity {
                    let (point_row_slot, point_correlation, point_bin) = if !y_axis
                        .is_visibility_math()
                        && let Some(mask) = &mut ordinate_points
                    {
                        if projected_flags.is_empty() && fully_flagged() {
                            continue;
                        }
                        let dimensions =
                            metadata_pair.unwrap_or_else(|| plotms_ordinates::PointDimensions {
                                channel: true,
                                baseline: !y_axis.uses_derived_geometry(),
                            });
                        let mask_ordinate = if correlation_metadata_panels {
                            correlation_slot * spec.y_axes.len() + y_index
                        } else {
                            *y_index
                        };
                        let Some(point) = mask.project_sample(
                            mask_ordinate,
                            row_slot,
                            bin.ordinal,
                            dimensions,
                            [channel_bins.len(), selected_correlations.len()],
                        ) else {
                            continue;
                        };
                        (
                            point.row,
                            if correlation_metadata_panels {
                                *corr_index
                            } else {
                                selected_correlations[point.correlation].0
                            },
                            &channel_bins[point.channel],
                        )
                    } else if let Some(visit) = antenna_visit {
                        let flat = visit.antenna * channel_bins.len() + bin.ordinal;
                        let correlation =
                            selected_correlations[flat % selected_correlations.len()].0;
                        (row_slot, correlation, bin)
                    } else {
                        (row_slot, *corr_index, bin)
                    };
                    let Some(y_value) = axis_value(*y_index + 1, *y_axis)? else {
                        continue;
                    };
                    let projected_series = ((point_row_slot, point_correlation)
                        != (row_slot, *corr_index)
                        && matches!(
                            spec.color_by,
                            MsColorAxis::Baseline | MsColorAxis::Correlation
                        ))
                    .then(|| {
                        let correlation = selected_correlations
                            .iter()
                            .find(|(slot, _)| *slot == point_correlation)
                            .map(|(_, label)| label.as_str());
                        let (key, label) = group_for(point_row_slot, correlation);
                        visibility_columns.series_identity(spec, *y_index, &key, &label)
                    });
                    let display_series = flagged_series
                        .as_ref()
                        .filter(|_| displayed_flagged)
                        .or(projected_series.as_ref());
                    let (series_key, series_label, color_group) = display_series.map_or(
                        (series_key, series_label, color_group),
                        |(key, label, color)| (key, label, color),
                    );
                    if geometry_dedup_required
                        && !geometry_samples_seen.insert((
                            panel_key.clone(),
                            series_key.clone(),
                            y_axis.as_str().to_string(),
                            row_time_value.to_bits(),
                            field_id_value,
                        ))
                    {
                        continue;
                    }
                    let series =
                        panel
                            .series
                            .entry(series_key.clone())
                            .or_insert_with(|| MsScatterSeries {
                                label: series_label.clone(),
                                color_group: color_group.clone(),
                                y_axis: *y_axis,
                                y_index: *y_index,
                                points: Vec::new(),
                                provenance: Vec::new(),
                                plotms_metadata: Vec::new(),
                                plotms_reporting_frequencies_hz: Vec::new(),
                                plotms_averaged_ids: None,
                            });
                    let push_started = timing_enabled.then(Instant::now);
                    let x = pointing_values.get(*y_index).and_then(|pair| pair[0]);
                    series.points.push((x.unwrap_or(x_value), y_value));
                    if let Some(spectrum) = reporting_spectrum {
                        series.plotms_reporting_frequencies_hz.push(
                            spectrum.mean_frequency_hz(point_bin).ok_or(
                                "PlotMS reporting frequency is missing the selected channel",
                            )?,
                        );
                    }
                    series.provenance.push(MsScatterPointRef {
                        row: row_numbers[point_row_slot],
                        corr: point_correlation,
                        chan_start: point_bin.start,
                        chan_end: point_bin.end,
                    });
                    if use_iterator_order
                        || (calibration.is_some() && spec.averaging.avgchannel.is_some())
                    {
                        series.plotms_metadata.push(MsScatterPlotMsMetadata {
                            channel: point_bin.display_channel(),
                            average_time: None,
                            averaging_group: use_iterator_order.then_some(if antenna_horizontal {
                                visit_index
                            } else {
                                point_row_slot
                            }),
                        });
                    }
                    if let Some(started) = push_started {
                        scatter_timing.point_push += started.elapsed();
                    }
                    point_budget.record_points(
                        1,
                        spec.preset
                            .map(MsPlotPreset::display_name)
                            .unwrap_or("Requested plot"),
                    )?;
                    panel.contributing_points += 1;
                    contributing_points += 1;
                    row_contributed = true;
                    row_panels.insert(panel_key.clone());
                }
                if let Some(started) = axis_started {
                    scatter_timing.axis_compute += started.elapsed();
                }
            }
        }

        if antenna_visit.is_none_or(|visit| visit.first_for_row) {
            for panel_key in row_panels {
                if let Some(panel) = panels.get_mut(&panel_key) {
                    panel.contributing_rows += 1;
                }
            }
            if row_contributed {
                contributing_rows += 1;
            }
        }
    }
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("expand_rows", &mut checkpoint_started);
    }

    if contributing_points == 0
        && (selected_row_count == 0 || raw_symbols.is_none() && coordinate_mask.is_none())
    {
        return Err(format!(
            "{} produced no unflagged visibility points for the current selection",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("Requested plot")
        ));
    }

    let title = spec
        .style
        .title
        .clone()
        .or_else(|| spec.preset.map(|preset| preset.display_name().to_string()))
        .unwrap_or_else(|| {
            let relation = if metadata_pair.is_some() || antenna_horizontal {
                "vs."
            } else {
                "vs"
            };
            format!(
                "{} {relation} {}",
                spec.y_axis().display_name(),
                spec.x_axis.display_name()
            )
        });
    let x_label = spec
        .style
        .xlabel
        .clone()
        .or_else(|| pointing.and_then(plotms_pointing::PointingAxes::x_label))
        .unwrap_or_else(|| axis_label(spec.x_axis));
    let y_label = spec
        .style
        .ylabel
        .clone()
        .or_else(|| pointing.and_then(|axes| axes.label(0, 1)))
        .unwrap_or_else(|| axis_label(spec.y_axis()));
    let secondary_y_label = spec.secondary_y_axis().map(axis_label);
    let bounds_for_column = |axis, column: MsDataColumn| {
        if project_plotms_columns && column.has_unbounded_phase() {
            // Projected scalar columns can extend beyond the principal phase range.
            None
        } else {
            fixed_bounds(axis)
        }
    };
    let x_column = calibration
        .and_then(|input| input.x_data_column)
        .unwrap_or(spec.data_column);
    let fixed_x_bounds = bounds_for_column(spec.x_axis, x_column);
    let fixed_y_bounds = bounds_for_column(spec.y_axis(), spec.data_column);
    let secondary_fixed_y_bounds = spec.secondary_y_axis().and_then(|axis| {
        bounds_for_column(
            axis,
            visibility_columns.column(1).unwrap_or(spec.data_column),
        )
    });
    if paged_iteration {
        panel_order.sort_by_key(|key| panels[key].iteration_rank);
    }
    let mut panels = panel_order
        .into_iter()
        .filter_map(|panel_key| {
            let panel = panels.remove(&panel_key)?;
            let mut series = panel.series.into_values().collect::<Vec<_>>();
            if calibration.is_none() && row_iteration.is_none() {
                for entry in &mut series {
                    entry
                        .points
                        .sort_by(|left, right| left.0.total_cmp(&right.0));
                }
            } else {
                series.sort_by_key(|entry| entry.y_index);
                if needs_row_axes
                    || array_horizontal
                    || antenna_horizontal
                    || ephemeris_ordinate
                    || coordinate_mask.is_some()
                    || pointing.is_some()
                    || raw_symbols.is_some()
                    || phase_shift.is_some()
                    || paged_iteration
                    || row_iteration.is_some()
                    || channel_derived
                    || channel_only_visibility
                    || order_ordinate_list
                    || ordered_feed_selection
                    || spec.y_axes.len() > 2
                {
                    series.iter_mut().for_each(plotms_ordinates::order_series);
                }
            }
            Some(MsScatterPanelPayload {
                key: panel_key,
                summary: format!(
                    "{}. Rows={} Points={} Data column={}",
                    panel.label,
                    panel.contributing_rows,
                    panel.contributing_points,
                    spec.data_column
                ),
                label: panel.label,
                series,
            })
        })
        .collect::<Vec<_>>();
    if let Some(axis) = row_iteration {
        let series = panels.pop().map_or_else(Vec::new, |panel| panel.series);
        panels = plotms_iteration::partition(
            ms,
            axis,
            &row_numbers,
            [&selected_antenna1, &selected_antenna2],
            &selected_times,
            series,
            point_budget,
        )?;
        contributing_points = panels
            .iter()
            .flat_map(|panel| &panel.series)
            .map(|series| series.points.len())
            .sum();
    }
    if timing_enabled {
        VisibilityScatterTiming::checkpoint("sort_and_pack_series", &mut checkpoint_started);
        scatter_timing.report(
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows,
            contributing_points,
            scatter_started,
        );
    }
    if let Some(iteraxis) = iteraxis {
        let (gridrows, gridcols) = if paged_iteration {
            (spec.layout.gridrows, spec.layout.gridcols)
        } else {
            resolve_iterated_grid(panels.len(), spec.layout.gridrows, spec.layout.gridcols)?
        };
        let share_x_bounds =
            share_axis_bounds(spec.iteration.xselfscale, spec.iteration.xsharedaxis);
        let share_y_bounds =
            share_axis_bounds(spec.iteration.yselfscale, spec.iteration.ysharedaxis);
        return Ok(MsPlotPayload::ScatterGrid(MsScatterGridPayload {
            title,
            x_axis: spec.x_axis,
            y_axis: spec.y_axis(),
            x_label,
            y_label,
            fixed_x_bounds,
            fixed_y_bounds,
            showlegend: spec.style.showlegend,
            legend_position: spec.style.legendposition,
            showmajorgrid: spec.style.showmajorgrid,
            showminorgrid: spec.style.showminorgrid,
            symbol_size_px: spec.style.symbol_size,
            iteraxis,
            gridrows,
            gridcols,
            share_x_bounds,
            share_y_bounds,
            header_lines: Vec::new(),
            summary: format!(
                "{}. Panels={} Rows={} Points={} Data column={}.{}",
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("MeasurementSet iterated plot"),
                panels.len(),
                contributing_rows,
                contributing_points,
                spec.data_column,
                row_sampling_summary
            ),
            panels,
        }));
    }
    let panel = panels
        .pop()
        .ok_or_else(|| "msexplore scatter payload lost its only panel".to_string())?;
    Ok(MsPlotPayload::Scatter(MsScatterPlotPayload {
        title,
        x_axis: spec.x_axis,
        y_axis: spec.y_axis(),
        secondary_y_axis: spec.secondary_y_axis(),
        x_label,
        y_label,
        secondary_y_label,
        fixed_x_bounds,
        fixed_y_bounds,
        secondary_fixed_y_bounds,
        showlegend: spec.style.showlegend,
        legend_position: spec.style.legendposition,
        showmajorgrid: spec.style.showmajorgrid,
        showminorgrid: spec.style.showminorgrid,
        symbol_size_px: spec.style.symbol_size,
        header_lines: Vec::new(),
        summary: format!(
            "{}. Rows={} Points={} Data column={}.{}",
            spec.preset
                .map(MsPlotPreset::display_name)
                .unwrap_or("MeasurementSet plot"),
            contributing_rows,
            contributing_points,
            spec.data_column,
            row_sampling_summary
        ),
        series: panel.series,
    }))
}
