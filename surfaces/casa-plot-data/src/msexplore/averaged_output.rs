// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct AveragedScatterOutput<'a> {
    pub(super) inputs: AveragedProjectionInputs<'a>,
    pub(super) row_numbers: &'a [usize],
    pub(super) visibility_columns: &'a plotms_ordinates::VisibilityColumns<'a>,
    pub(super) selected_scan_numbers: &'a [i32],
    pub(super) field: casa_ms::subtables::field::MsField<'a>,
    pub(super) scalar_spans: Option<plotms_ordinates::ScalarSpanPlan>,
    pub(super) averaged_antenna_color: Option<MsColorAxis>,
    pub(super) antenna_correlation_conflict: Option<(i32, usize, usize)>,
    pub(super) time_baseline_stats: Option<plotms_time_baselines::TimeBaselineStatistics>,
    pub(super) spw_groups: Option<plotms_spw::SpectralAverageGroups>,
    pub(super) averaged_rows: std::collections::BTreeSet<(usize, i32, i32)>,
    pub(super) time_uvws: Option<HashMap<(usize, i32, i32), TimeUvwAverage>>,
    pub(super) time_statistics: Option<plotms_columns::TimeStatistics>,
    pub(super) panel_order: Vec<String>,
    pub(super) panels: BTreeMap<String, AveragedPanelAccumulator>,
    pub(super) contributing_rows: std::collections::BTreeSet<usize>,
    pub(super) symbols: Option<RawSymbolVisibility>,
    pub(super) iteraxis: Option<MsIterationAxis>,
    pub(super) extended_derived: bool,
    pub(super) direct_time_projection: bool,
    pub(super) time_antenna: bool,
    pub(super) time_spw: bool,
    pub(super) spw_antenna: bool,
    pub(super) spw_baseline: bool,
    pub(super) scalar_average: bool,
    pub(super) spw_average: bool,
    pub(super) antenna_average: bool,
    pub(super) spans_scan_or_field: bool,
    pub(super) vector_span_models: bool,
    pub(super) paged_iteration: bool,
    pub(super) show_flagged: bool,
    pub(super) retain_empty_panel: bool,
    pub(super) sort_series: bool,
    pub(super) combined_vector_average: bool,
}

struct AveragedGroups {
    time_baseline_plan: Option<plotms_time_baselines::TimeBaselinePlan>,
    buffer_integrations: Option<Vec<Option<plotms_time_baselines::Integration>>>,
    spw_groups: Option<Arc<[Option<MsScatterPlotMsAverageSpw>]>>,
    averaged_ids: Option<MsScatterPlotMsAverageIds>,
    averaged_order: HashMap<(usize, i32, i32), usize>,
    color_groups: Vec<(i32, String, String)>,
    time_uvws: Option<HashMap<usize, [f64; 3]>>,
    time_statistics: Option<HashMap<(usize, usize), plotms_columns::AveragedWeightAxes>>,
}

struct AveragedSeriesFormatter<'a> {
    spec: &'a MsPlotSpec,
    calibration: Option<&'a PlotMsInput<'a>>,
    visibility_columns: &'a plotms_ordinates::VisibilityColumns<'a>,
    time_antenna: bool,
    time_spw: bool,
    symbols: Option<RawSymbolVisibility>,
    averaged_antenna_color: Option<MsColorAxis>,
    scalar_average: bool,
    antenna_average: bool,
    spw_average: bool,
    spw_antenna: bool,
    spw_baseline: bool,
    show_flagged: bool,
    spw_groups: Option<Arc<[Option<MsScatterPlotMsAverageSpw>]>>,
    averaged_ids: Option<MsScatterPlotMsAverageIds>,
    averaged_order: HashMap<(usize, i32, i32), usize>,
    color_groups: Vec<(i32, String, String)>,
}

impl AveragedScatterOutput<'_> {
    fn prepare_groups(&mut self) -> Result<AveragedGroups, String> {
        let Self {
            inputs:
                AveragedProjectionInputs {
                    time_bins,
                    spec,
                    collapsed_uv,
                    time_bounds,
                    selected_field_ids,
                    ..
                },
            time_baseline_stats,
            row_numbers,
            antenna_correlation_conflict,
            time_antenna,
            time_spw,
            averaged_rows,
            extended_derived,
            direct_time_projection,
            spw_groups,
            scalar_spans,
            spw_antenna,
            antenna_average,
            spans_scan_or_field,
            vector_span_models,
            selected_scan_numbers,
            averaged_antenna_color,
            field,
            time_uvws,
            time_statistics,
            ..
        } = self;
        let time_baseline_plan = time_baseline_stats
            .take()
            .zip(*time_bins)
            .map(|(statistics, groups)| statistics.plan(groups, row_numbers, &spec.averaging))
            .transpose()?;
        if let Some((spw_id, expected, actual)) = antenna_correlation_conflict {
            return Err(format!(
                "PlotMS antenna time averaging requires the same selected correlation count within spectral window {spw_id} (#1032). Expected {expected}, found {actual}"
            ));
        }
        let mut averaged_rows = std::mem::take(averaged_rows);
        if let Some(plan) = time_baseline_plan
            .as_ref()
            .filter(|_| *time_antenna || *time_spw)
        {
            averaged_rows = averaged_rows
                .into_iter()
                .filter_map(|(group, antenna, other)| {
                    Some((plan.output_group(group)?, antenna, other))
                })
                .collect();
        }
        let buffer_integrations = (*time_bins)
            .filter(|_| *extended_derived || *direct_time_projection || *collapsed_uv)
            .map(|groups| {
                // PlotMS caches the first averaged baseline's integration for the whole buffer.
                groups
                    .buffers()
                    .iter()
                    .flat_map(|buffer| {
                        let integration = time_bounds
                            .get(&buffer.start)
                            .map(plotms_ordinates::VisibilityTimeBounds::buffer_integration);
                        std::iter::repeat_n(integration, buffer.len())
                    })
                    .collect::<Vec<_>>()
            });
        let spw_groups = spw_groups.take().map(|groups| {
            groups.into_averages(
                scalar_spans.as_ref(),
                time_baseline_plan.as_ref().filter(|_| *time_spw),
            )
        });
        let averaged_ids = if let Some(groups) = &spw_groups {
            let row_groups = averaged_rows.iter().map(|&(group, _, _)| group).collect();
            Some(MsScatterPlotMsAverageIds::SpectralWindow {
                groups: groups.clone(),
                row_groups,
                antenna_ids: (*spw_antenna).then(|| {
                    averaged_rows
                        .iter()
                        .map(|&(_, antenna, _)| antenna)
                        .collect()
                }),
            })
        } else if *antenna_average && *spans_scan_or_field {
            let ids = averaged_rows
                .iter()
                .map(|&(group, antenna, _)| {
                    let (scan, field) = if let Some(spans) = scalar_spans.as_ref() {
                        let metadata = spans.metadata(group);
                        (metadata.scan, metadata.field)
                    } else {
                        time_baseline_plan
                            .as_ref()
                            .and_then(|plan| plan.scan_and_field(group))
                            .expect("vector antenna averaging group")
                    };
                    (scan, field, antenna)
                })
                .collect();
            Some(MsScatterPlotMsAverageIds::ScanFieldAndAntenna(ids))
        } else if *vector_span_models && let Some(groups) = *time_bins {
            let ids = groups
                .model_metadata_slots()
                .map(|slot| (selected_scan_numbers[slot], selected_field_ids[slot]))
                .collect();
            Some(MsScatterPlotMsAverageIds::ScanAndField(ids))
        } else if let Some(spans) = scalar_spans.as_ref() {
            let ids = averaged_rows
                .iter()
                .map(|&(group, _, _)| {
                    let metadata = spans.metadata(group);
                    (metadata.scan, metadata.field)
                })
                .collect();
            Some(MsScatterPlotMsAverageIds::ScanAndField(ids))
        } else if *antenna_average {
            let ids = averaged_rows
                .iter()
                .map(|&(_, antenna, _)| antenna)
                .collect();
            Some(MsScatterPlotMsAverageIds::Antenna(ids))
        } else {
            None
        };
        let color_groups = match (*averaged_antenna_color, &averaged_ids) {
            (Some(axis), Some(MsScatterPlotMsAverageIds::ScanFieldAndAntenna(rows))) => {
                plotms_averaged_colors::groups(axis, rows, field)
            }
            _ => Vec::new(),
        };
        let averaged_order = averaged_rows
            .into_iter()
            .enumerate()
            .map(|(order, row)| (row, order))
            .collect::<HashMap<_, _>>();
        let time_uvws = time_uvws.take().map(|mut averages| {
            let plan = time_baseline_plan.as_ref().filter(|_| *time_spw);
            if let Some(plan) = plan {
                averages = plan.combine_uvws(averages);
            }
            averages
                .into_iter()
                .filter_map(|(key, average)| {
                    // The exported SPW rows have a distinct index for each baseline.
                    let order = if plan.is_some() {
                        *averaged_order.get(&key)?
                    } else {
                        key.0
                    };
                    Some((order, average.mean()))
                })
                .collect::<HashMap<_, _>>()
        });
        let time_statistics = time_statistics
            .take()
            .map(plotms_columns::TimeStatistics::finish);
        Ok(AveragedGroups {
            time_baseline_plan,
            buffer_integrations,
            spw_groups,
            averaged_ids,
            averaged_order,
            color_groups,
            time_uvws,
            time_statistics,
        })
    }

    pub(super) fn build(mut self, point_budget: &mut PointBudget) -> Result<MsPlotPayload, String> {
        let labels = scatter_labels(
            self.inputs.spec,
            self.inputs.calibration,
            self.scalar_average,
            self.extended_derived,
        );
        let groups = self.prepare_groups()?;
        let AveragedGroups {
            time_baseline_plan,
            buffer_integrations,
            spw_groups,
            averaged_ids,
            averaged_order,
            color_groups,
            time_uvws,
            time_statistics,
        } = groups;
        let Self {
            inputs,
            visibility_columns,
            mut panel_order,
            mut panels,
            paged_iteration,
            sort_series,
            retain_empty_panel,
            combined_vector_average,
            iteraxis,
            contributing_rows,
            time_antenna,
            spw_average,
            show_flagged,
            spw_antenna,
            antenna_average,
            symbols,
            scalar_average,
            spw_baseline,
            time_spw,
            averaged_antenna_color,
            ..
        } = self;
        let spec = inputs.spec;
        let calibration = inputs.calibration;
        let requested_freqframe = inputs.requested_freqframe;
        let time_reporting_frame = inputs.time_reporting_frame;
        let time_reporting_axes = inputs.time_reporting_axes;
        let time_spectra = &inputs.time_spectra;
        let report_time_frequencies = time_spectra.as_ref().is_some_and(|spectra| {
            requested_freqframe.is_some()
                || spectra
                    .values()
                    .any(|spectrum| Some(spectrum.frame) != time_reporting_frame)
        });
        let time_velocity = time_reporting_axes
            && (spec.x_axis == MsAxis::Velocity || spec.y_axis() == MsAxis::Velocity);
        let time_doppler = time_velocity
            .then(|| parse_velocity_definition(&spec.transforms.veldef))
            .transpose()?;
        let time_rest_frequency = if time_velocity {
            spec.transforms
                .restfreq
                .as_deref()
                .map(parse_rest_frequency_hz)
                .transpose()
                .map_err(|error| error.to_string())?
        } else {
            None
        };
        let converted_time_frequencies = HashMap::new();
        let time_spectral_engine = None;
        let x_projection_axis = visibility_columns.projection_axis(0, spec.x_axis);
        let shifted_time_row = None;
        let mut projection = AveragedProjection {
            inputs,
            time_baseline_plan,
            buffer_integrations,
            time_uvws,
            time_statistics,
            report_time_frequencies,
            time_doppler,
            time_rest_frequency,
            x_projection_axis,
            converted_time_frequencies,
            time_spectral_engine,
            shifted_time_row,
        };
        let formatter = AveragedSeriesFormatter {
            spec,
            calibration,
            visibility_columns,
            time_antenna,
            time_spw,
            symbols,
            averaged_antenna_color,
            scalar_average,
            antenna_average,
            spw_average,
            spw_antenna,
            spw_baseline,
            show_flagged,
            spw_groups,
            averaged_ids,
            averaged_order,
            color_groups,
        };
        let mut contributing_points = 0usize;
        let mut finalized_panels = Vec::new();
        if paged_iteration {
            panel_order.sort_by_key(|key| panels[key].iteration_rank);
        }
        for panel_key in panel_order {
            let Some(panel) = panels.remove(&panel_key) else {
                continue;
            };
            let mut series_entries = Vec::new();

            for series in panel.series.into_values() {
                formatter.append_series(
                    &mut projection,
                    series,
                    &mut series_entries,
                    point_budget,
                )?;
            }
            if sort_series {
                series_entries.sort_by_key(|entry| entry.y_index);
            }
            if series_entries.is_empty() && !retain_empty_panel {
                continue;
            }
            let panel_point_count = series_entries
                .iter()
                .map(|entry| entry.points.len())
                .sum::<usize>();
            contributing_points += panel_point_count;
            finalized_panels.push(MsScatterPanelPayload {
                key: panel_key,
                summary: format!(
                    "{}. Rows={} Points={} Data column={}",
                    panel.label,
                    panel.contributing_rows.len(),
                    panel_point_count,
                    spec.data_column
                ),
                label: panel.label,
                series: series_entries,
            });
        }

        AveragedScatterPayload {
            labels,
            iteraxis,
            paged_iteration,
            combined_vector_average,
            finalized_panels,
            contributing_rows,
            contributing_points,
        }
        .build(spec)
    }
}

impl AveragedSeriesFormatter<'_> {
    fn append_series(
        &self,
        projection: &mut AveragedProjection<'_>,
        mut series: AveragedSeriesAccumulator,
        series_entries: &mut Vec<MsScatterSeries>,
        point_budget: &mut PointBudget,
    ) -> Result<(), String> {
        let Self {
            spec,
            calibration,
            visibility_columns,
            time_antenna,
            time_spw,
            symbols,
            averaged_antenna_color,
            scalar_average,
            antenna_average,
            spw_average,
            spw_antenna,
            spw_baseline,
            show_flagged,
            spw_groups,
            averaged_ids,
            averaged_order,
            color_groups,
        } = self;
        let y_projection_axis =
            visibility_columns.projection_axis(series.y_index + 1, series.y_axis);
        if let Some(plan) = &projection.time_baseline_plan {
            if *time_antenna {
                plan.combine_antenna_series(&mut series);
            } else {
                plan.combine_series(&mut series, *time_spw);
            }
        }
        let make_output = |label, color_group| MsScatterSeries {
            label,
            color_group,
            y_axis: series.y_axis,
            y_index: series.y_index,
            points: Vec::new(),
            provenance: Vec::new(),
            plotms_metadata: Vec::new(),
            plotms_reporting_frequencies_hz: Vec::new(),
            plotms_averaged_ids: averaged_ids.clone(),
        };
        let mut output = make_output(series.label, series.color_group);
        let mut color_outputs = color_groups
            .iter()
            .map(|(id, key, label)| (*id, make_output(label.clone(), key.clone())))
            .collect::<BTreeMap<_, _>>();
        let mut flagged_output =
            (*show_flagged).then(|| make_output("Flagged".into(), "flagged".into()));
        let mut points = series.points.into_iter().map(|(key, point)| {
            let group = key.time_key.group();
            let order = group.map(|group| match key.baseline {
                AveragingBaselineToken::Baseline(antenna1, antenna2)
                    if *scalar_average || *spw_average =>
                {
                    averaged_order[&(group, antenna1, antenna2)]
                }
                AveragingBaselineToken::Antenna(antenna) if *antenna_average || *spw_antenna => {
                    averaged_order[&(group, antenna, -1)]
                }
                AveragingBaselineToken::Averaged if *spw_baseline => {
                    averaged_order[&(group, -1, -1)]
                }
                _ => group,
            });
            let spectral_channel = match (spw_groups, group, &key.sample) {
                (
                    Some(groups),
                    Some(group),
                    AveragingSampleKey::SelectedChannel { ordinal, .. },
                ) => groups[group]
                    .as_ref()
                    .map(|window| window.channel(*ordinal)),
                _ => None,
            };
            let channel_ordinal = match key.sample {
                AveragingSampleKey::SourceChannel { ordinal, .. }
                | AveragingSampleKey::SelectedChannel { ordinal, .. } => ordinal,
            };
            (group, order, spectral_channel, channel_ordinal, point)
        });
        let mut plot_order = calibration.filter(|_| !*spw_average).map(|_| {
            let mut ordered = points.by_ref().collect::<Vec<_>>();
            ordered.sort_by_key(|(_, order, _, _, entry)| {
                entry
                    .representative
                    .as_ref()
                    .map(|point| point.plotms_order(*order))
            });
            ordered.into_iter()
        });
        let ordered = std::iter::from_fn(|| match &mut plot_order {
            Some(points) => points.next(),
            None => points.next(),
        });
        for (group, order, spectral_channel, channel_ordinal, accumulator) in ordered {
            let Some(projected) = projection.project_point(
                group,
                order,
                spectral_channel,
                channel_ordinal,
                accumulator,
                y_projection_axis,
            )?
            else {
                continue;
            };
            let output = if projected.flagged {
                flagged_output
                    .as_mut()
                    .expect("flagged visibility has a display layer")
            } else if symbols.is_some_and(|symbols| !symbols.unflagged) {
                continue;
            } else if let Some(axis) = averaged_antenna_color {
                let Some(MsScatterPlotMsAverageIds::ScanFieldAndAntenna(ids)) = averaged_ids else {
                    return Err("Missing scan and field IDs for averaged antenna colors".into());
                };
                let row = order
                    .and_then(|index| ids.get(index))
                    .ok_or("Missing reduced row for an averaged antenna color")?;
                let id = plotms_averaged_colors::category(*axis, row);
                color_outputs
                    .get_mut(&id)
                    .ok_or("Missing averaged antenna color group")?
            } else {
                &mut output
            };

            output.points.push(projected.point);
            output.provenance.push(projected.representative);
            output
                .plotms_reporting_frequencies_hz
                .extend(projected.reporting_frequency);
            output.plotms_metadata.extend(projected.metadata);
            point_budget.record_points(
                1,
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("Requested plot"),
            )?;
        }
        if calibration.is_none() {
            output
                .points
                .sort_by(|left, right| left.0.total_cmp(&right.0));
        }
        if !output.points.is_empty() {
            series_entries.push(output);
        }
        let mut ordered_colors = color_outputs.into_values().collect::<Vec<_>>();
        ordered_colors.sort_by(|left, right| left.label.cmp(&right.label));
        series_entries.extend(ordered_colors);
        if let Some(output) = flagged_output.filter(|output| !output.points.is_empty()) {
            series_entries.push(output);
        }
        Ok(())
    }
}

struct ScatterLabels {
    title: String,
    x_label: String,
    y_label: String,
    secondary_y_label: Option<String>,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    secondary_fixed_y_bounds: Option<(f64, f64)>,
}

fn scatter_labels(
    spec: &MsPlotSpec,
    calibration: Option<&PlotMsInput<'_>>,
    scalar_average: bool,
    extended_derived: bool,
) -> ScatterLabels {
    let title = spec
        .style
        .title
        .clone()
        .or_else(|| spec.preset.map(|preset| preset.display_name().to_string()))
        .unwrap_or_else(|| {
            format!(
                "{} vs {}",
                spec.y_axis().display_name(),
                spec.x_axis.display_name()
            )
        });
    let x_label = spec
        .style
        .xlabel
        .clone()
        .unwrap_or_else(|| axis_label(spec.x_axis));
    let y_label = spec
        .style
        .ylabel
        .clone()
        .unwrap_or_else(|| axis_label(spec.y_axis()));
    let secondary_y_label = spec.secondary_y_axis().map(axis_label);
    let bounds_for_column = |axis, column: MsDataColumn| {
        let unbounded_phase =
            column.has_unbounded_phase() || (scalar_average && column.is_derived());
        fixed_bounds(axis).filter(|_| !(extended_derived && unbounded_phase))
    };
    let x_column = calibration
        .and_then(|input| input.x_data_column)
        .unwrap_or(spec.data_column);
    let fixed_x_bounds = bounds_for_column(spec.x_axis, x_column);
    let fixed_y_bounds = bounds_for_column(spec.y_axis(), spec.data_column);
    let secondary_fixed_y_bounds = spec.secondary_y_axis().and_then(fixed_bounds);
    ScatterLabels {
        title,
        x_label,
        y_label,
        secondary_y_label,
        fixed_x_bounds,
        fixed_y_bounds,
        secondary_fixed_y_bounds,
    }
}

struct AveragedScatterPayload {
    labels: ScatterLabels,
    iteraxis: Option<MsIterationAxis>,
    paged_iteration: bool,
    combined_vector_average: bool,
    finalized_panels: Vec<MsScatterPanelPayload>,
    contributing_rows: std::collections::BTreeSet<usize>,
    contributing_points: usize,
}

impl AveragedScatterPayload {
    fn build(self, spec: &MsPlotSpec) -> Result<MsPlotPayload, String> {
        let Self {
            labels,
            iteraxis,
            paged_iteration,
            combined_vector_average,
            mut finalized_panels,
            contributing_rows,
            contributing_points,
        } = self;
        let ScatterLabels {
            title,
            x_label,
            y_label,
            secondary_y_label,
            fixed_x_bounds,
            fixed_y_bounds,
            secondary_fixed_y_bounds,
        } = labels;
        if finalized_panels.is_empty() {
            if combined_vector_average {
                return Err(
                    "empty MeasurementSet plots after channel and time averaging are pending #1032"
                        .into(),
                );
            }
            return Err(format!(
                "{} produced no unflagged visibility points for the current selection",
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("Requested plot")
            ));
        }

        if let Some(iteraxis) = iteraxis {
            let (gridrows, gridcols) = if paged_iteration {
                (spec.layout.gridrows, spec.layout.gridcols)
            } else {
                resolve_iterated_grid(
                    finalized_panels.len(),
                    spec.layout.gridrows,
                    spec.layout.gridcols,
                )?
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
                    "{}. Panels={} Rows={} Points={} Data column={}",
                    spec.preset
                        .map(MsPlotPreset::display_name)
                        .unwrap_or("MeasurementSet iterated plot"),
                    finalized_panels.len(),
                    contributing_rows.len(),
                    contributing_points,
                    spec.data_column
                ),
                panels: finalized_panels,
            }));
        }

        let panel = finalized_panels
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
                "{}. Rows={} Points={} Data column={}",
                spec.preset
                    .map(MsPlotPreset::display_name)
                    .unwrap_or("MeasurementSet plot"),
                contributing_rows.len(),
                contributing_points,
                spec.data_column
            ),
            series: panel.series,
        }))
    }
}
