// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn apply_clip_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    request: &FlagDataRequest,
    model_factory: Option<FlagDataModelFactory>,
    averaging: AveragedAgentInput<'_>,
) -> Result<ChangeSet, FlaggingError> {
    let AveragedAgentInput {
        time: averaged_time,
        uv: averaged_uv,
        view,
    } = averaging;
    let path = display_ms_path(ms);
    let widths = request.clip_channel_bins.as_deref();
    let time_average = request.clip_time_bin.as_deref().map(time_bin_seconds);
    let average = view.is_some()
        || time_average.is_some_and(|interval| interval.is_some())
        || widths.is_some_and(|widths| widths.iter().any(|&width| width != 0));
    let operand_rows = view
        .as_deref()
        .map_or(selected_rows, SharedFlagView::input_rows);
    let models = if request.clipoutside.is_some() {
        clip_models::ClipModels::for_selection(
            ms,
            request.data_column,
            operand_rows,
            model_factory,
            &path,
        )?
    } else {
        None
    };
    let Some(mut columns) =
        ClipColumns::bind(ms.main_table(), request.data_column, average, models)?
    else {
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    };
    if time_average == Some(None) {
        tracing::warn!("unrecognized timebin disables the clip agent");
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let Some(outside) = request.clipoutside else {
        tracing::warn!("clipoutside must be a scalar Boolean to start the clip agent");
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    };
    if let Some(widths) = widths {
        check_channel_bins(ms, widths, &path)?;
    }
    let options = clip_options(request, correlations, outside);
    if let Some(view) = view {
        view.prepare_column(ms, request.data_column, &mut columns, &path)?;
        return view.clip(
            SharedAgentSelection {
                rows: Some(selected_rows),
                channels: channel_selections,
                correlations,
                time: averaged_time,
                uv: averaged_uv,
            },
            request,
            options,
            &path,
        );
    }
    // CASA flags non-finite data and, with clipzeros, near-zero samples.
    let ddid_to_spw = data_description_spw_map(ms)?;
    let mut changed = ChangeSet::default();
    let mut flagged_samples = 0usize;
    let mut total_samples = 0usize;
    let mut row_changes = Vec::<(usize, usize)>::new();
    let include_ddid = average
        || channel_selections.is_some()
        || correlations.is_some()
        || columns.uses_weight_spectrum();
    let bytes_per_sample = columns.bytes_per_sample(ms.main_table(), &path)?;
    let read_plan = flag_read_plan(ms, selected_rows.len(), bytes_per_sample, include_ddid)?;
    if let Some(Some(seconds)) = time_average {
        return TimeClip {
            averaging: clip_time::TimeAveraging {
                seconds,
                filter: averaged_time,
            },
            chunk_seconds: request.time_chunk,
            column: request.data_column,
            widths,
            channels: channel_selections,
            correlations,
            options,
            path: &path,
            cancellation: request.cancellation.as_ref(),
        }
        .apply(ms, selected_rows, &mut columns, &ddid_to_spw, read_plan);
    }
    let batch_rows = read_plan.rows_per_block.max(1);
    #[cfg(feature = "flagdata-test-hooks")]
    let batch_rows = test_hooks::clip_batch_rows(batch_rows);
    for rows in selected_rows.chunks(batch_rows) {
        request.check_cancelled()?;
        #[cfg(feature = "flagdata-test-hooks")]
        test_hooks::observe(test_hooks::Checkpoint::ClipBatchStarted);
        let table = ms.main_table();
        let mut updates = BTreeMap::<usize, ArrayD<bool>>::new();
        let mut touched_rows = std::collections::BTreeSet::<usize>::new();
        let flag_cells = clip_time::read_flags(table, rows, &path)?;
        let ddids = if include_ddid {
            Some(load_i32_cells(table, rows, "DATA_DESC_ID", &path)?)
        } else {
            None
        };
        let data_cells =
            columns.read(ms, rows, &flag_cells, ddids.as_deref(), &ddid_to_spw, &path)?;
        for (row_slot, (&row, flags)) in rows.iter().zip(&flag_cells).enumerate() {
            total_samples += flags.len();
            flagged_samples += flags.iter().filter(|flag| **flag).count();
            let ddid = ddids.as_ref().map(|values| values[row_slot]);
            let projections = correlations
                .zip(ddid)
                .map(|(selection, ddid)| selection.clip_projections_for(ddid, &path))
                .transpose()?;
            if projections.is_some_and(|projections| projections.is_empty()) {
                continue;
            }
            let column = request.data_column.name();
            if flags.ndim() != 2 {
                return Err(FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("{column} row {row} is not rank-2"),
                });
            }
            let nchan = flags.shape()[1];
            let (channels, width) = if average || channel_selections.is_some() {
                clip_channel_selection(
                    ddid,
                    &ddid_to_spw,
                    channel_selections,
                    widths,
                    nchan,
                    &path,
                )?
            } else {
                (None, 0)
            };
            let bins = channels
                .as_deref()
                .and_then(|channels| ChannelBins::new(width, channels, flags));
            let data_value = data_cells.row(row_slot, flags, bins.as_ref(), row, &path)?;
            let (clip_flags, clip_channels) = bins
                .as_ref()
                .map_or((flags, channels.as_deref()), |bins| (bins.flags(), None));
            row_changes.clear();
            apply_clip_array(
                data_value.as_ref(),
                clip_flags,
                clip_channels,
                ClipOptions {
                    projections,
                    ..options
                },
                &path,
                row,
                &mut row_changes,
            )?;
            if !row_changes.is_empty() {
                let mut updated_flags = flags.clone();
                let mut flag_planes = updated_flags
                    .view_mut()
                    .into_dimensionality::<Ix2>()
                    .map_err(|source| FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("FLAG row {row} must be rank-2: {source}"),
                    })?;
                let may_overlap = projections.is_some_and(|items| {
                    items.len() > 1
                        && items
                            .iter()
                            .any(|item| matches!(item, ClipProjection::Stokes(..)))
                });
                let row_changed = set_clip_coordinates(
                    &mut flag_planes,
                    &row_changes,
                    bins.as_ref(),
                    may_overlap,
                );
                changed.changed_samples += row_changed;
                flagged_samples += row_changed;
                updates.insert(row, updated_flags);
                touched_rows.insert(row);
            }
        }
        // CASA's clip agent writes the per-sample FLAG column.
        write_touched_flag_updates(ms, updates, touched_rows, |_| false, &mut changed)?;
        #[cfg(feature = "flagdata-test-hooks")]
        test_hooks::observe(test_hooks::Checkpoint::ClipBatchStaged);
    }
    changed.summary = Some(FlagSummary {
        flagged_samples,
        total_samples,
    });
    Ok(changed)
}

pub(super) fn set_clip_coordinates(
    flag_planes: &mut ndarray::ArrayViewMut2<'_, bool>,
    row_changes: &[(usize, usize)],
    bins: Option<&ChannelBins<'_>>,
    may_overlap: bool,
) -> usize {
    if let Some(bins) = bins {
        row_changes
            .iter()
            .map(|&(corr, bin)| {
                bins.original_channels(bin)
                    .iter()
                    .filter(|&&chan| !std::mem::replace(&mut flag_planes[(corr, chan)], true))
                    .count()
            })
            .sum()
    } else if may_overlap {
        row_changes
            .iter()
            .filter(|&&(corr, chan)| !std::mem::replace(&mut flag_planes[(corr, chan)], true))
            .count()
    } else {
        for &(corr, chan) in row_changes {
            flag_planes[(corr, chan)] = true;
        }
        row_changes.len()
    }
}

pub(super) fn clip_options(
    request: &FlagDataRequest,
    correlations: Option<&CorrelationSelection>,
    outside: bool,
) -> ClipOptions<'static> {
    ClipOptions {
        zeros: request.clipzeros,
        signed_zeros: false,
        // CASA's clipping agent stores its bounds as single-precision values.
        bounds: request
            .clipminmax
            .map(|bounds| bounds.map(|limit| f64::from(limit as f32))),
        outside,
        projections: None,
        metric: correlations.and_then(CorrelationSelection::clip_metric),
    }
}

pub(super) fn clip_channel_selection(
    ddid: Option<i32>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    selections: Option<&BTreeMap<i32, ChannelSelection>>,
    widths: Option<&[u32]>,
    channel_count: usize,
    path: &str,
) -> Result<(Option<Vec<usize>>, u32), FlaggingError> {
    let ddid = ddid.expect("selected channels project DATA_DESC_ID");
    let spw = ddid_to_spw
        .get(&ddid)
        .copied()
        .ok_or_else(|| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
        })?;
    let width = match widths {
        None => 0,
        Some([width]) => *width,
        Some(widths) => {
            widths
                .get(spw as usize)
                .copied()
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("SPW {spw} has no channel bin"),
                })?
        }
    };
    let mut channels =
        match selections.and_then(|selectors| selectors.get(&spw)) {
            Some(selection) => Some(selection.indices(channel_count).map_err(|source| {
                FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("resolve SPW {spw} channels: {source}"),
                }
            })?),
            None if width > 0 => Some((0..channel_count).collect()),
            None => None,
        };
    if width > 0
        && let Some(channels) = &mut channels
    {
        channels.sort_unstable();
    }
    Ok((channels, width))
}

pub(super) fn apply_quack_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let from_start = match request.quackmode {
        QuackMode::Beg | QuackMode::Tail => true,
        QuackMode::End | QuackMode::BeforeEnd => false,
        QuackMode::Disabled => {
            return Ok(ChangeSet {
                agent_skipped: true,
                ..ChangeSet::default()
            });
        }
    };
    if request.quackinterval.is_nan() || request.quackinterval <= 0.0 {
        tracing::warn!("quackinterval must be greater than zero to start the quack agent");
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let table = ms.main_table();
    let mut scan_times = Vec::with_capacity(selected_rows.len());
    let mut edges = BTreeMap::<i32, f64>::new();
    for &row in selected_rows {
        request.check_cancelled()?;
        let scan = table_i32(table, row, "SCAN_NUMBER", ms.path())?;
        let time = table_f64(table, row, "TIME", ms.path())?;
        scan_times.push((row, scan, time));
        // CASA determines the unflagged time limits from whole flag cubes.
        if !request.quackincrement || clone_flag_matrix(ms, row)?.iter().any(|flag| !*flag) {
            edges
                .entry(scan)
                .and_modify(|edge| {
                    *edge = if from_start {
                        edge.min(time)
                    } else {
                        edge.max(time)
                    };
                })
                .or_insert(time);
        }
    }
    let mut quack_rows = Vec::new();
    for (row, scan, time) in scan_times {
        let Some(&edge_time) = edges.get(&scan) else {
            continue;
        };
        // CASA's edge intervals include equality. The complementary modes
        // select the samples strictly outside their respective intervals.
        let flag = match request.quackmode {
            QuackMode::Beg => time <= edge_time + request.quackinterval,
            QuackMode::End => time >= edge_time - request.quackinterval,
            QuackMode::BeforeEnd => time < edge_time - request.quackinterval,
            QuackMode::Tail => time > edge_time + request.quackinterval,
            QuackMode::Disabled => false,
        };
        if flag {
            quack_rows.push(row);
        }
    }
    // CASA updates FLAG_ROW at selected scan times only when the call
    // leaves SPW unspecified, even for an explicitly selected full window.
    let row_policy = if channel_selections.is_some() {
        SelectedFlagRowPolicy::Preserve
    } else {
        selected_flag_row_policy(correlations, SelectedFlagRowPolicy::AllSelectedRows)
    };
    set_selected_flags(
        ms,
        &quack_rows,
        channel_selections,
        correlations,
        request.action.value(),
        row_policy,
        request.cancellation.as_ref(),
    )
}
