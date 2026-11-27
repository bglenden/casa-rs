// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn build_msexplore_plot_payload_validated(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
    calibration: Option<&PlotMsInput<'_>>,
) -> Result<MsPlotPayload, String> {
    if calibration.and_then(|input| input.time_bins).is_none()
        && plotms_columns::uses_averaged_geometry_coordinates(spec)
    {
        return Err("Time-averaged PlotMS interval and wavelength axes with a callibrary or direct msexplore are pending (#1032)".into());
    }
    if calibration.and_then(|input| input.time_bins).is_none()
        && spec.averaging.avgtime.is_some()
        && (spec.x_axis == MsAxis::WeightedAmplitude
            || spec.y_axes.contains(&MsAxis::WeightedAmplitude))
    {
        return Err("Time-averaged PlotMS weighted amplitude with a callibrary or direct msexplore is pending (#1032)".into());
    }
    if spec.preset == Some(MsPlotPreset::AmplitudePhaseVsTimeStacked) {
        return build_stacked_amplitude_phase_time_page(ms, selection, spec, point_budget);
    }
    if let Some(listobs_kind) = spec
        .preset
        .and_then(MsPlotPreset::lowers_to_listobs_metadata)
    {
        let listobs_options = selection.to_summary_options();
        return match listobs_kind {
            ListObsPlotKind::UvCoverage => {
                let coverage = ListObsUvCoverage::from_ms_with_options(ms, &listobs_options)
                    .map_err(|error| error.to_string())?;
                build_listobs_uv_plot_payload(&coverage, &ListObsPlotSpec::new(listobs_kind))
                    .map(MsPlotPayload::ListObs)
            }
            _ => {
                let summary = ListObsSummary::from_ms_with_options(ms, &listobs_options)
                    .map_err(|error| error.to_string())?;
                build_listobs_plot_payload_from_summary(
                    &summary,
                    &ListObsPlotSpec::new(listobs_kind),
                )
                .map(MsPlotPayload::ListObs)
            }
        };
    }

    build_generic_visibility_scatter(ms, selection, spec, point_budget, calibration)
}

pub(super) fn build_msexplore_payload_internal(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    allow_decimation: bool,
    calibration: Option<&PlotMsInput<'_>>,
) -> Result<MsPlotPayload, String> {
    spec.validate_with_plotms(
        calibration.is_some_and(|input| {
            input.secondary_y_data_column.is_some() || !input.additional_ordinates.is_empty()
        }),
        calibration.is_some_and(|input| input.pointing.is_some()),
        calibration
            .is_some_and(|input| input.rows.is_none() || input.secondary_y_data_column.is_some()),
    )?;
    if calibration.and_then(|input| input.time_bins).is_none()
        && spec.plots.iter().any(|plot| {
            plot.averaging.avgtime.is_some() && plot.y_axes.contains(&MsAxis::UvDistance)
        })
    {
        return Err("Time-averaged PlotMS UV distance ordinates with a callibrary or direct msexplore are pending (#1032)".into());
    }
    let mut point_budget = PointBudget::limited(spec.max_plot_points, allow_decimation);
    let header_lines = match calibration {
        Some(input) => plotms_page_headers::lines(ms, spec, input),
        None => resolve_page_header_lines(ms, spec)?,
    };
    if spec.plots.len() == 1 {
        let mut payload = build_msexplore_plot_payload_validated(
            ms,
            &spec.selection,
            &spec.plots[0],
            &mut point_budget,
            calibration,
        )?;
        apply_page_header_lines(&mut payload, header_lines);
        return if allow_decimation {
            compact_payload_to_budget(payload, &point_budget)
        } else if point_budget.rendered_points > spec.max_plot_points {
            Err(format!(
                "{} would render more than {}; narrow the selection, add averaging, or raise --max-points",
                spec.plots[0]
                    .preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("Requested plot"),
                spec.max_plot_points
            ))
        } else {
            Ok(payload)
        };
    }

    let (gridrows, gridcols) = {
        let layout = &spec.plots[0].layout;
        (layout.gridrows, layout.gridcols)
    };
    let mut items = Vec::with_capacity(spec.plots.len());
    for plot in &spec.plots {
        let payload = build_msexplore_plot_payload_validated(
            ms,
            &spec.selection,
            plot,
            &mut point_budget,
            calibration,
        )?;
        let scatter = match payload {
            MsPlotPayload::Scatter(payload) => payload,
            MsPlotPayload::ListObs(_) => {
                return Err(
                    "msexplore multi-plot pages currently support raw-visibility scatter plots only"
                        .to_string(),
                );
            }
            MsPlotPayload::ScatterGrid(_) => {
                return Err(
                    "msexplore multi-plot pages do not yet support iterated child grids"
                        .to_string(),
                );
            }
            MsPlotPayload::ScatterPage(_) => {
                return Err(
                    "msexplore multi-plot pages do not yet support nested page payloads"
                        .to_string(),
                );
            }
        };
        items.push(MsScatterPageItemPayload {
            plotindex: plot.layout.plotindex,
            rowindex: plot.layout.rowindex,
            colindex: plot.layout.colindex,
            plot: scatter,
        });
    }
    items.sort_by_key(|item| item.plotindex);
    validate_page_overplots(&items)?;
    let title = spec
        .page_title
        .clone()
        .unwrap_or_else(|| "MeasurementSet Multi-Plot Page".to_string());

    let payload = MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title,
        exprange: spec.exprange,
        gridrows,
        gridcols,
        header_lines,
        summary: format!(
            "MeasurementSet multi-plot page. Plots={} Occupied cells={} Grid={}x{} Exprange={}",
            items.len(),
            items
                .iter()
                .map(|item| (item.rowindex, item.colindex))
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            gridrows,
            gridcols,
            spec.exprange
        ),
        items,
    });
    if allow_decimation {
        compact_payload_to_budget(payload, &point_budget)
    } else if point_budget.rendered_points > spec.max_plot_points {
        Err(format!(
            "MeasurementSet multi-plot page would render more than {}; narrow the selection, add averaging, or raise --max-points",
            spec.max_plot_points
        ))
    } else {
        Ok(payload)
    }
}

/// Rejects a page whose plots share a grid cell but not its axes.
///
/// Plots in one cell are overplotted on one set of axes, so they must agree
/// on the x, y and secondary-y axes, on any fixed axis bounds, and on the
/// legend position. `items` must be sorted by `plotindex`; the first plot in
/// each cell sets the cell's axes.
pub(super) fn validate_page_overplots(items: &[MsScatterPageItemPayload]) -> Result<(), String> {
    let mut first_in_cell = std::collections::BTreeMap::new();
    for item in items {
        let cell = (item.rowindex, item.colindex);
        let first = *first_in_cell.entry(cell).or_insert(&item.plot);
        let (rowindex, colindex) = cell;
        if item.plot.x_axis != first.x_axis
            || item.plot.y_axis != first.y_axis
            || item.plot.secondary_y_axis != first.secondary_y_axis
        {
            return Err(format!(
                "scatter page cell ({rowindex}, {colindex}) mixes incompatible axes; overplots require matching x/y axis configurations"
            ));
        }
        if item.plot.fixed_x_bounds != first.fixed_x_bounds
            || item.plot.fixed_y_bounds != first.fixed_y_bounds
            || item.plot.secondary_fixed_y_bounds != first.secondary_fixed_y_bounds
        {
            return Err(format!(
                "scatter page cell ({rowindex}, {colindex}) mixes incompatible fixed axis bounds"
            ));
        }
        if item.plot.legend_position != first.legend_position {
            return Err(format!(
                "scatter page cell ({rowindex}, {colindex}) mixes incompatible legend positions"
            ));
        }
    }
    Ok(())
}

/// Build a plot payload from a full `msexplore` request.
pub fn build_msexplore_payload(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
) -> Result<MsPlotPayload, String> {
    build_msexplore_payload_internal(ms, spec, true, None)
}

/// Build the same payload with read-only, on-the-fly corrected data and flags.
pub fn build_calibrated_plotms_payload(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    calibration: &PlotMsCalibration,
    secondary_y_data_column: Option<MsDataColumn>,
) -> Result<MsPlotPayload, String> {
    if spec
        .plots
        .iter()
        .any(plotms_row_axes::uses_time_row_averaging)
    {
        return Err("averaged PlotMS Row coordinates with a callibrary are pending (#1032)".into());
    }
    let input = PlotMsInput {
        row_order: &calibration.row_order,
        header_row: calibration.row_order.first().copied(),
        starts_visibility_buffer: &[],
        extended_derived: false,
        time_bins: None,
        channels: &calibration.channels,
        rows: Some(&calibration.rows),
        models: None,
        x_data_column: calibration.x_data_column,
        secondary_y_data_column,
        additional_ordinates: &[],
        weight_axis_columns: [None; 3],
        prefer_float_data: false,
        raw_symbols: None,
        pointing: None,
        phasecenter: None,
    };
    build_msexplore_payload_internal(ms, spec, true, Some(&input))
}

pub fn build_uncalibrated_plotms_payload(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    columns: PlotMsColumnSelection<'_>,
    prefer_float_data: bool,
    load_models: Option<PlotMsModelLoader>,
    clock_labels: bool,
) -> Result<(MsPlotPayload, Option<i64>), String> {
    let mut selected =
        casa_ms::selection::resolve_casa_selection_with_system_budget(ms, &spec.selection)
            .map_err(|error| error.to_string())?;
    let header_row = selected.selected_rows.first().map(|row| row.row_index);
    let plot = spec.plots.first();
    let extended_derived = columns.extended_derived;
    let scalar_average = plot.is_some_and(plotms_columns::uses_scalar_visibility_average);
    let spw_average = plot.is_some_and(plotms_columns::uses_spectral_window_average);
    let scalar_spw = spw_average && plot.is_some_and(|plot| plot.averaging.scalar);
    let time_spw = spw_average
        && plot.is_some_and(|plot| !plot.averaging.scalar && plot.averaging.avgtime.is_some());
    let antenna_average = plot.is_some_and(plotms_columns::uses_antenna_visibility_average);
    let time_antenna = antenna_average
        && plot.is_some_and(|plot| plot.averaging.avgtime.is_some() && !plot.averaging.scalar);
    let needs_model_references = plot.is_some_and(|plot| {
        plotms_model::needs_model_references(ms, &selected.selected_rows, plot, &columns)
    });
    let vector_span_models = needs_model_references
        && !spw_average
        && plot.is_some_and(|plot| plot.averaging.avgscan || plot.averaging.avgfield);
    let averaged_row_numbers = plot.is_some_and(plotms_row_axes::uses_time_row_averaging);
    let time_phase_shift =
        columns.phasecenter.is_some() && plot.is_some_and(|plot| plot.averaging.avgtime.is_some());
    let direct_time_projection = !extended_derived
        && (time_phase_shift
            || vector_span_models
            || averaged_row_numbers
            || plot.is_some_and(plotms_columns::uses_averaged_geometry_coordinates)
            || plot.is_some_and(plotms_columns::uses_spectral_reporting_time_grid)
            || plot.is_some_and(plotms_columns::uses_time_statistics)
            || plot.is_some_and(|plot| {
                plotms_columns::uses_direct_vector_time_projection(
                    plot,
                    columns.x,
                    prefer_float_data,
                )
            }));
    if (extended_derived && !scalar_average) || time_antenna || time_spw || direct_time_projection {
        // Accumulate the Float sums in the chronological order of the iterator.
        selected
            .selected_rows
            .sort_by(|left, right| left.time_mjd_seconds.total_cmp(&right.time_mjd_seconds));
    }
    let vector_baselines = plot.is_some_and(plotms_columns::uses_vector_baseline_average);
    let time_baselines =
        vector_baselines && plot.is_some_and(|plot| plot.averaging.avgtime.is_some());
    let weighted_time_uvw = plot.is_some_and(plotms_columns::uses_weighted_time_uvw);
    let independent_ordinates = columns.secondary_y.is_some() || !columns.additional_y.is_empty();
    let time_bins = plot
        .filter(|_| {
            !(scalar_average || scalar_spw)
                && (time_antenna
                    || time_baselines
                    || time_spw
                    || weighted_time_uvw
                    || direct_time_projection
                    || (!antenna_average && (independent_ordinates || extended_derived)))
        })
        .and_then(|plot| Some((plot, plot.averaging.avgtime?)))
        .map(|(plot, seconds)| {
            casa_mstransform::selected_plot_time_groups(
                ms,
                &selected.selected_rows,
                TimeAverageOptions {
                    interval_seconds: seconds,
                    span_scans: plot.averaging.avgscan,
                    span_states: true,
                    span_fields: plot.averaging.avgfield,
                    max_uvw_distance: 0.0,
                },
                time_baselines
                    || time_antenna
                    || time_spw
                    || weighted_time_uvw
                    || extended_derived
                    || direct_time_projection,
                time_spw,
                if time_phase_shift {
                    PlotTimePhaseReferences::TimeAveragedShift
                } else if needs_model_references {
                    PlotTimePhaseReferences::VirtualModel
                } else {
                    PlotTimePhaseReferences::None
                },
                averaged_row_numbers,
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    let spw_model_references = needs_model_references && spw_average;
    let ordered_spw_references = spw_model_references && !time_spw;
    let (row_order, starts_visibility_buffer, phase_reference_rows) = if (vector_baselines
        && !time_baselines)
        || scalar_average
        || (spw_average && !time_spw)
        || (antenna_average && !time_antenna)
    {
        let rows = selected
            .selected_rows
            .iter()
            .map(|row| (row.row_index, row.data_desc_id));
        let ordered = casa_mstransform::selected_plotms_integration_order(
            ms,
            rows,
            spw_average,
            ordered_spw_references,
        )
        .map_err(|error| error.to_string())?;
        (
            ordered.rows,
            ordered.starts_visibility_buffer,
            ordered.phase_reference_rows,
        )
    } else {
        (
            selected
                .selected_rows
                .iter()
                .map(|row| row.row_index)
                .collect(),
            Vec::new(),
            Vec::new(),
        )
    };
    let spw = spec
        .selection
        .spw
        .as_deref()
        .filter(|_| spec.selection.selectdata)
        .unwrap_or("*");
    let selection = ms
        .resolve_spw_selector(spw)
        .map_err(|error| error.to_string())?;
    let mut channels = BTreeMap::<i32, Vec<ChannelRange>>::new();
    for &range in selection.channel_list() {
        channels.entry(range.spw_id as i32).or_default().push(range);
    }
    let spectral_windows = spw_model_references.then_some(PlotMsModelSpwSelection {
        channels: &channels,
        correlations: &selected.correlation_types,
    });
    let ordered_model_slots = ordered_spw_references.then(|| {
        plotms_model::ordered_model_reference_slots(
            &selected.selected_rows,
            &row_order,
            &starts_visibility_buffer,
            &phase_reference_rows,
        )
    });
    let model_references = time_bins
        .as_ref()
        .and_then(|groups| {
            Some(PlotMsModelReferences {
                phase_slots: groups.phase_reference_slots()?,
                recipe_slots: groups.input_buffer_first_slots(),
                spectral_windows,
            })
        })
        .or_else(|| {
            ordered_model_slots
                .as_ref()
                .map(|(phase_slots, recipe_slots)| PlotMsModelReferences {
                    phase_slots,
                    recipe_slots,
                    spectral_windows,
                })
        });
    let models = plotms_model::selected_model_cells(
        ms,
        &selected.selected_rows,
        plot,
        &columns,
        model_references,
        load_models,
    )?;
    let input = PlotMsInput {
        row_order: &row_order,
        header_row,
        starts_visibility_buffer: &starts_visibility_buffer,
        time_bins: time_bins.as_ref(),
        extended_derived,
        channels: &channels,
        rows: None,
        models: models.as_ref(),
        x_data_column: columns.x,
        secondary_y_data_column: columns.secondary_y,
        additional_ordinates: columns.additional_y,
        weight_axis_columns: columns.weights,
        prefer_float_data,
        raw_symbols: columns.raw_symbols,
        pointing: columns.pointing,
        phasecenter: columns.phasecenter,
    };
    let times = selected
        .selected_rows
        .iter()
        .map(|row| row.time_mjd_seconds);
    let reference = clock_labels.then(|| reference_midnight(times)).flatten();
    build_msexplore_payload_internal(ms, spec, true, Some(&input)).map(|plot| (plot, reference))
}

/// Build a generic plot payload from one open MeasurementSet.
pub fn build_msexplore_plot_payload(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    spec.validate()?;
    let mut point_budget = PointBudget::unlimited();
    build_msexplore_plot_payload_validated(ms, selection, spec, &mut point_budget, None)
}

/// Open a MeasurementSet path and build the requested plot payload.
pub fn build_msexplore_plot_payload_from_path(
    path: &Path,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsPlotPayload, String> {
    let ms = MeasurementSet::open(path).map_err(|error| {
        if path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", path.display())
        }
    })?;
    build_msexplore_plot_payload(&ms, selection, spec)
}

/// Write one plot payload's text point manifest (`--plot-format txt`).
///
/// The manifest is a tab-separated list of the plotted points with `#`
/// header lines naming the axes, layout and series; the CASA-parity
/// comparator reads it. PNG and PDF rasterisation was removed (#833):
/// Python renders the typed plot document with Matplotlib instead.
pub fn export_msexplore_plot(payload: &MsPlotPayload, output_path: &Path) -> Result<(), String> {
    let manifest = match payload {
        MsPlotPayload::ListObs(payload) => render_listobs_manifest(payload)?,
        MsPlotPayload::Scatter(_)
        | MsPlotPayload::ScatterGrid(_)
        | MsPlotPayload::ScatterPage(_) => render_scatter_manifest(payload)?,
    };
    std::fs::write(output_path, manifest).map_err(|error| error.to_string())
}
