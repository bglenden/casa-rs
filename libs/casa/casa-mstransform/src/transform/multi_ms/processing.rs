// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dispatch a transform over the physical partitions of a Multi-MS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_tables::{Table, TableOptions};

use super::partition::{self, Partition};
use super::{
    check_scan_span, container_info, install_shared_subtables, members, retain, table_error,
};
use crate::transform::{
    MsPartitionAxis, MsTransformError, MsTransformMultiMsOptions, MsTransformReport,
    MsTransformRequest, MsTransformTaskOptions, TransformSelection, data_description_spw_map,
    empty_transform_report, resolve_transform_selection, scalar_i32, transform_product,
    transform_selected_product,
};
use casa_ms::MeasurementSet;
use casa_ms::selection::{MsSelectionIoBudget, ResolvedMsSelectionRow, RowGrammar};

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
    let existing = !source_members.is_empty() && !config.monolithic;
    if !existing && !config.create {
        return transform_product(request, grammar, options, template, None).map(Some);
    }

    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    let crosses_spws = options.combine_spws || config.multiple_spws;
    let dispatch = if existing {
        dispatch_retained(
            &input,
            request,
            source_members,
            crosses_spws,
            config.scan_span,
            options,
        )?
    } else {
        Dispatch::Create(config.axis)
    };
    let mut report = match dispatch {
        Dispatch::Plain => transform_product(request, grammar, options, template, None)?,
        Dispatch::Retain => {
            let mut report = retain(request, final_path, grammar, options, source_members)?
                .unwrap_or_else(|| empty_transform_report(request));
            report.retained_mms = Some(true);
            report
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
            let budget = MsSelectionIoBudget::from_system_memory(
                2,
                size_of::<ResolvedMsSelectionRow>(),
                None,
            )
            .map_err(|error| MsTransformError::Spectrum(error.to_string()))?;
            let mut selection =
                resolve_transform_selection(&input, request, grammar, budget, false)?;
            let rows = std::mem::take(&mut selection.rows);
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
            if spw_ids.is_some() {
                member_options.ddi_start = 0;
            }
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
                    axis: Some(axis),
                    template,
                    fallback: existing,
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

fn dispatch_retained(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    members: &[PathBuf],
    crosses_spws: bool,
    exact_scan_span: bool,
    options: &MsTransformTaskOptions,
) -> Result<Dispatch, MsTransformError> {
    let span = options.time_averaging.filter(|_| exact_scan_span);
    let needs_all_spws = crosses_spws && (options.time_averaging.is_none() || span.is_some());
    let combine_ok = !needs_all_spws || first_member_contains_spws(input, request, &members[0])?;
    let scan_ok = if let Some(average) = span {
        match check_scan_span(
            members,
            request.selection.scan.as_deref(),
            average.interval_seconds,
        ) {
            Ok(()) => true,
            Err(MsTransformError::MultiMsScanSpan) => false,
            Err(error) => return Err(error),
        }
    } else {
        true
    };
    Ok(match (combine_ok, scan_ok, crosses_spws, span.is_some()) {
        (true, true, _, _) => Dispatch::Retain,
        (_, _, true, true) => Dispatch::Plain,
        (false, _, _, _) => Dispatch::Create(MsPartitionAxis::Scan),
        (_, false, _, _) => Dispatch::Create(MsPartitionAxis::SpectralWindow),
    })
}

fn first_member_contains_spws(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    member: &Path,
) -> Result<bool, MsTransformError> {
    let spw = if request.spw.trim().is_empty() {
        "*"
    } else {
        request.spw.as_str()
    };
    let Ok(selected) = input.resolve_spw_selector(spw) else {
        // CASA leaves selector diagnostics to the individual members.
        return Ok(true);
    };
    let mut needed = selected
        .spw_ids()
        .iter()
        .map(|&id| id as i32)
        .collect::<BTreeSet<_>>();
    let descriptions = data_description_spw_map(input)?;
    let table =
        Table::open(TableOptions::new(member)).map_err(|source| table_error(member, source))?;
    let column = table
        .column_accessor("DATA_DESC_ID")
        .map_err(|source| table_error(member, source))?;
    // CASA consumes the requested SPW iterator on the first member. Check
    // that member's inventory before applying any field or row selection.
    for start in (0..table.row_count()).step_by(4096) {
        if needed.is_empty() {
            break;
        }
        let rows = (start..(start + 4096).min(table.row_count())).collect::<Vec<_>>();
        let values = column
            .scalar_cells_owned_for_rows(&rows)
            .map_err(|source| table_error(member, source))?;
        for (value, row) in values.iter().zip(rows) {
            let id = scalar_i32(value.as_ref(), "DATA_DESC_ID", row)?;
            if let Some(spw_id) = descriptions.get(&id) {
                needed.remove(spw_id);
            }
        }
    }
    Ok(needed.is_empty())
}

struct MemberExecution<'a> {
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
            for (parameter, value) in &mut history.parameters {
                match parameter.as_str() {
                    "vis" => *value = format!("\"{}\"", request.input_ms.display()),
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
            Err(error) if matches!(error, MsTransformError::EmptySelection { .. }) => {
                tracing::warn!(error = %error, partition = index, "mstransform skipped a Multi-MS partition");
            }
            Err(error) => return Err(error),
        }
    }
    if report.is_some() {
        if spectral_groups.len() > 1 {
            super::partition_metadata::consolidate(destination, &outputs, &spectral_groups)?;
        }
        install_shared_subtables(destination, &outputs, tables[0].keywords())?;
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
            let ids = id.trim();
            let (first, last) = if ids == "*" {
                (0, i32::MAX)
            } else if let Some((first, last)) = ids.split_once('~') {
                (first.trim().parse().ok()?, last.trim().parse().ok()?)
            } else {
                let id = ids.parse().ok()?;
                (id, id)
            };
            Some((first..=last, channels.trim()))
        })
        .collect::<Vec<_>>();
    let selection = windows
        .iter()
        .map(
            |id| match ranges.iter().find(|(range, _)| range.contains(id)) {
                Some((_, channels)) => format!("{id}:{channels}"),
                None => id.to_string(),
            },
        )
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
