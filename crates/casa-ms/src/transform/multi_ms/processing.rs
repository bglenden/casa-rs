// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dispatch a transform over the physical partitions of a Multi-MS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_tables::{Table, TableOptions};

use super::partition::{self, Partition};
use super::{container_info, install_shared_subtables, members, partition_axis, table_error};
use crate::MeasurementSet;
use crate::selection::{MsSelectionIoBudget, ResolvedMsSelectionRow, RowGrammar};
use crate::transform::{
    MsPartitionAxis, MsTransformError, MsTransformMultiMsOptions, MsTransformReport,
    MsTransformRequest, MsTransformTaskOptions, TransformSelection, empty_transform_report,
    resolve_transform_selection, transform_product, transform_selected_product,
};

enum Dispatch {
    Plain,
    Retain,
    Create(MsPartitionAxis),
}

pub(in crate::transform) fn transform(
    request: &MsTransformRequest,
    final_path: &Path,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    config: MsTransformMultiMsOptions,
) -> Result<Option<MsTransformReport>, MsTransformError> {
    let started = Instant::now();
    let source_members = members(&request.input_ms)?;
    if source_members.is_none() && !config.create {
        return Ok(None);
    }
    let source_members = source_members.as_deref().unwrap_or_default();
    let template = source_members.first().map(PathBuf::as_path);
    let retain = !source_members.is_empty() && !config.monolithic;
    if !retain && !config.create {
        return transform_product(request, grammar, options, template, None).map(Some);
    }

    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    let budget =
        MsSelectionIoBudget::from_system_memory(2, size_of::<ResolvedMsSelectionRow>(), None)
            .map_err(|error| MsTransformError::Spectrum(error.to_string()))?;
    let mut selection = match resolve_transform_selection(&input, request, grammar, budget, false) {
        Ok(selection) => selection,
        Err(error) if retain && selection_failure(&error) => {
            return Ok(Some(empty_retained_report(request)));
        }
        Err(error) => return Err(error),
    };

    let crosses_spws = options.combine_spws
        || options
            .regrid
            .as_ref()
            .is_some_and(|regrid| regrid.nspw > 1);
    let (dispatch, mut by_member) = if retain {
        let rows = std::mem::take(&mut selection.rows);
        let partitions = distribute_to_members(source_members, rows)?;
        let dispatch = dispatch_retained(&input, &partitions, crosses_spws, options)?;
        (dispatch, partitions)
    } else {
        (Dispatch::Create(config.axis), Vec::new())
    };

    let mut report = match dispatch {
        Dispatch::Plain => {
            selection.rows = flatten_rows(by_member);
            transform_selected_product(request, grammar, options, template, None, Some(selection))?
        }
        Dispatch::Retain => {
            let axis = partition_axis(&request.input_ms);
            let result = write_members(
                request,
                final_path,
                options,
                &selection,
                by_member,
                MemberExecution {
                    source_members,
                    axis: axis.as_deref(),
                    template,
                    fallback: false,
                    spw_ids: None,
                    original_spw: &request.spw,
                },
            )?;
            result.map_or_else(
                || empty_retained_report(request),
                |mut report| {
                    report.retained_mms = Some(true);
                    report
                },
            )
        }
        Dispatch::Create(axis) => {
            if crosses_spws && axis != MsPartitionAxis::Scan {
                return Err(MsTransformError::MultiMsParameter {
                    parameter: "separationaxis",
                    reason: format!(
                        "Cannot create MMS using separationaxis={} with some of the requested transformations.",
                        axis.label()
                    ),
                });
            }
            let rows = if retain {
                flatten_rows(std::mem::take(&mut by_member))
            } else {
                std::mem::take(&mut selection.rows)
            };
            if rows.is_empty() {
                return Err(MsTransformError::MultiMsParameter {
                    parameter: "scan",
                    reason: "No Scans present in the created MS.".into(),
                });
            }
            let count = config.count.map_or(8, std::num::NonZeroUsize::get);
            let partitions = partition::create(&input, rows, axis, count)?;
            let mut member_request = request.clone();
            let mut member_options = options.clone();
            let spw_ids = (axis == MsPartitionAxis::SpectralWindow)
                .then(|| {
                    order_spectra(
                        &input,
                        &partitions,
                        &mut member_request,
                        &mut member_options,
                    )
                })
                .transpose()?;
            let axis = if axis == MsPartitionAxis::Automatic {
                "scan,spw"
            } else {
                axis.label()
            };
            write_members(
                &member_request,
                final_path,
                &member_options,
                &selection,
                partitions,
                MemberExecution {
                    source_members: &[],
                    axis: Some(axis),
                    template,
                    fallback: retain,
                    spw_ids,
                    original_spw: &request.spw,
                },
            )?
            .unwrap_or_else(|| empty_transform_report(request))
        }
    };
    report.input_ms.clone_from(&request.input_ms);
    report.output_ms.clone_from(&request.output_ms);
    report.spw.clone_from(&request.spw);
    report.elapsed_ns = started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX);
    Ok(Some(report))
}

fn flatten_rows(partitions: Vec<Partition>) -> Vec<ResolvedMsSelectionRow> {
    partitions.into_iter().flat_map(|part| part.rows).collect()
}

fn selection_failure(error: &MsTransformError) -> bool {
    matches!(
        error,
        MsTransformError::EmptySelection { .. }
            | MsTransformError::InvalidSpw { .. }
            | MsTransformError::SelectRows { .. }
    )
}

fn empty_retained_report(request: &MsTransformRequest) -> MsTransformReport {
    let mut report = empty_transform_report(request);
    report.retained_mms = Some(true);
    report
}

fn distribute_to_members(
    members: &[PathBuf],
    rows: Vec<ResolvedMsSelectionRow>,
) -> Result<Vec<Partition>, MsTransformError> {
    let mut end_rows = Vec::with_capacity(members.len());
    let mut end = 0;
    for member in members {
        let table =
            Table::open(TableOptions::new(member)).map_err(|source| table_error(member, source))?;
        end += table.row_count();
        end_rows.push(end);
    }
    let mut partitions = members
        .iter()
        .map(|_| Partition {
            rows: Vec::new(),
            history: None,
        })
        .collect::<Vec<_>>();
    for row in rows {
        let member = end_rows.partition_point(|&end| end <= row.row_index);
        let part = partitions.get_mut(member).ok_or_else(|| {
            MsTransformError::Spectrum("Multi-MS members changed during row selection.".into())
        })?;
        part.rows.push(row);
    }
    Ok(partitions)
}

fn dispatch_retained(
    input: &MeasurementSet,
    partitions: &[Partition],
    crosses_spws: bool,
    options: &MsTransformTaskOptions,
) -> Result<Dispatch, MsTransformError> {
    let span = options.time_averaging.filter(|average| average.span_scans);
    let needs_all_spws = crosses_spws && (options.time_averaging.is_none() || span.is_some());
    let all_spws = partitions
        .iter()
        .flat_map(|part| &part.rows)
        .map(|row| row.spectral_window_id)
        .collect::<BTreeSet<_>>();
    let combine_ok = !needs_all_spws
        || partitions.iter().all(|part| {
            part.rows
                .iter()
                .map(|row| row.spectral_window_id)
                .collect::<BTreeSet<_>>()
                == all_spws
        });
    let mut scan_ok = true;
    if let Some(average) = span {
        let mut scans_by_member = Vec::with_capacity(partitions.len());
        let mut all_scans = BTreeSet::new();
        for part in partitions {
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            for row in &part.rows {
                min = min.min(row.time_mjd_seconds);
                max = max.max(row.time_mjd_seconds);
            }
            scan_ok &= max - min >= average.interval_seconds;
            let scans = partition::scans(input, &part.rows)?
                .into_iter()
                .collect::<BTreeSet<_>>();
            all_scans.extend(&scans);
            scans_by_member.push(scans);
        }
        // The CASA caller consumes the scan iterator on the first member.
        scan_ok &= scans_by_member
            .first()
            .is_some_and(|scans| *scans == all_scans);
    }
    Ok(match (combine_ok, scan_ok, crosses_spws, span.is_some()) {
        (true, true, _, _) => Dispatch::Retain,
        (_, _, true, true) => Dispatch::Plain,
        (false, _, _, _) => Dispatch::Create(MsPartitionAxis::Scan),
        (_, false, _, _) => Dispatch::Create(MsPartitionAxis::SpectralWindow),
    })
}

struct MemberExecution<'a> {
    source_members: &'a [PathBuf],
    axis: Option<&'a str>,
    template: Option<&'a Path>,
    fallback: bool,
    spw_ids: Option<BTreeMap<i32, i32>>,
    original_spw: &'a str,
}

fn write_members(
    request: &MsTransformRequest,
    final_path: &Path,
    options: &MsTransformTaskOptions,
    selection: &TransformSelection,
    partitions: Vec<Partition>,
    execution: MemberExecution<'_>,
) -> Result<Option<MsTransformReport>, MsTransformError> {
    let destination = &request.output_ms;
    let name = final_path
        .file_name()
        .expect("validated output name")
        .to_string_lossy();
    let mut outputs = Vec::new();
    let mut tables = Vec::new();
    let mut report = None;
    let mut spectral_groups = Vec::new();
    let mut ddi_start = 0;
    for (index, partition) in partitions.into_iter().enumerate() {
        if partition.rows.is_empty() {
            continue;
        }
        let relative = PathBuf::from(format!("SUBMSS/{name}.{index:04}.ms"));
        let mut part = request.clone();
        part.output_ms = destination.join(&relative);
        let input_spws = execution.spw_ids.as_ref().map(|_| {
            partition
                .rows
                .iter()
                .map(|row| row.spectral_window_id)
                .collect::<BTreeSet<_>>()
        });
        let descriptions = input_spws.as_ref().map_or(0, |_| {
            partition
                .rows
                .iter()
                .map(|row| row.data_desc_id)
                .collect::<BTreeSet<_>>()
                .len()
        });
        let mut member_options = options.clone();
        if let Some(history) = &mut member_options.history {
            let history_source = execution
                .source_members
                .get(index)
                .map_or(request.input_ms.as_path(), PathBuf::as_path);
            for (parameter, value) in &mut history.parameters {
                match parameter.as_str() {
                    "vis" => *value = format!("\"{}\"", history_source.display()),
                    "outputvis" => *value = format!("\"{}\"", final_path.join(&relative).display()),
                    "createmms" => *value = "False".into(),
                    "ddistart" if input_spws.is_some() => *value = ddi_start.to_string(),
                    "spw" if let Some(windows) = &input_spws => {
                        *value = spw_history(execution.original_spw, windows);
                    }
                    "monolithic_processing" if execution.fallback => *value = "True".into(),
                    "separationaxis" if execution.fallback => {
                        *value = format!("\"{}\"", execution.axis.expect("new member axis"));
                    }
                    _ => {
                        if let Some((name, text)) = &partition.history
                            && parameter == name
                        {
                            value.clone_from(text);
                        }
                    }
                }
            }
        }
        let result = transform_selected_product(
            &part,
            RowGrammar::Transform,
            &member_options,
            execution.template,
            None,
            Some(TransformSelection {
                selection: selection.selection.clone(),
                correlations: selection.correlations.clone(),
                time: selection.time.clone(),
                rows: partition.rows,
                antenna_ids: selection.antenna_ids.clone(),
                tile_window: input_spws
                    .as_ref()
                    .and_then(|windows| windows.first().copied()),
            }),
        );
        match result {
            Ok(member) => {
                super::accumulate(&mut report, member);
                let table = Table::open(TableOptions::new(&part.output_ms))
                    .map_err(|source| table_error(&part.output_ms, source))?;
                tables.push(table);
                outputs.push(relative);
                if let Some((windows, ids)) = input_spws.zip(execution.spw_ids.as_ref()) {
                    spectral_groups.push(
                        windows
                            .into_iter()
                            .filter_map(|id| ids.get(&id).copied())
                            .collect(),
                    );
                    ddi_start += descriptions;
                }
            }
            Err(error)
                if matches!(error, MsTransformError::EmptySelection { .. })
                    || (selection_failure(&error) && !execution.source_members.is_empty()) =>
            {
                tracing::warn!(error = %error, partition = index, "mstransform skipped a Multi-MS partition");
            }
            Err(error) => return Err(error),
        }
    }
    if report.is_some() {
        if spectral_groups.len() > 1 {
            super::partition_metadata::consolidate(
                &destination.join(&outputs[0]),
                &spectral_groups,
            )?;
        }
        install_shared_subtables(destination, &outputs)?;
        let info = container_info(execution.axis, &outputs);
        let concat = Table::concat(tables).map_err(|source| table_error(destination, source))?;
        concat
            .save_with_info(TableOptions::new(destination), &info)
            .map_err(|source| table_error(destination, source))?;
    }
    Ok(report)
}

fn spw_history(expression: &str, windows: &BTreeSet<i32>) -> String {
    let ranges = expression
        .split(',')
        .filter_map(|term| {
            let (id, channels) = term.split_once(':')?;
            Some((id.trim().parse::<i32>().ok()?, channels.trim()))
        })
        .collect::<BTreeMap<_, _>>();
    let selection = windows
        .iter()
        .map(|id| match ranges.get(id) {
            Some(channels) => format!("{id}:{channels}"),
            None => id.to_string(),
        })
        .collect::<Vec<_>>();
    format!("\"{}\"", selection.join(","))
}

fn order_spectra(
    input: &MeasurementSet,
    partitions: &[Partition],
    request: &mut MsTransformRequest,
    options: &mut MsTransformTaskOptions,
) -> Result<BTreeMap<i32, i32>, MsTransformError> {
    let text = if request.spw.trim().is_empty() {
        "*"
    } else {
        &request.spw
    };
    let selection =
        input
            .resolve_spw_selector(text)
            .map_err(|source| MsTransformError::InvalidSpw {
                selector: text.into(),
                reason: source.to_string(),
            })?;
    let available = partitions
        .iter()
        .flat_map(|part| &part.rows)
        .map(|row| row.spectral_window_id)
        .collect::<BTreeSet<_>>();
    let mut ranges = BTreeMap::<i32, Vec<String>>::new();
    let mut order = Vec::new();
    for range in selection.channel_list() {
        let id = range.spw_id as i32;
        let entry = ranges.entry(id).or_insert_with(|| {
            order.push(id);
            Vec::new()
        });
        let mut expression = format!("{}~{}", range.start, range.stop);
        if range.step > 1 {
            expression.push_str(&format!("^{}", range.step));
        }
        entry.push(expression);
    }
    if !available.iter().copied().eq(order.iter().copied()) {
        request.spw = available
            .iter()
            .map(|id| match ranges.get(id) {
                Some(ranges) => format!("{id}:{}", ranges.join(";")),
                None => id.to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        reorder_widths(&mut options.channel_widths, &order, &available);
        reorder_widths(&mut options.flag_command_widths, &order, &available);
    }
    Ok(available
        .into_iter()
        .enumerate()
        .map(|(new, old)| {
            (
                old,
                if options.preserve_ids {
                    old
                } else {
                    new as i32
                },
            )
        })
        .collect())
}

fn reorder_widths<T: Copy>(
    widths: &mut Option<Vec<T>>,
    original: &[i32],
    available: &BTreeSet<i32>,
) {
    if let Some(widths) = widths
        && widths.len() > 1
        && widths.len() == original.len()
    {
        let by_window = original
            .iter()
            .copied()
            .zip(widths.iter().copied())
            .collect::<BTreeMap<_, _>>();
        *widths = available
            .iter()
            .filter_map(|id| by_window.get(id).copied())
            .collect();
    }
}
