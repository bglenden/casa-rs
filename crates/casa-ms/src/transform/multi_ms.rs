// SPDX-License-Identifier: LGPL-3.0-or-later
//! Split the physical members of a CASA Multi-MS and publish a portable view.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableError, TableInfo, TableOptions};
use casa_types::{RecordValue, Value};

use super::subtable_copy::{attached_subtable_name, copy_dir_recursive};
use super::{
    MsTransformError, MsTransformReport, MsTransformRequest, MsTransformTaskOptions, RowGrammar,
    scalar_f64, scalar_i32, transform_product,
};
use crate::SubtableId;
use crate::selection::syntax::parse_numeric_row_id_ranges;

mod partition;
mod partition_metadata;
mod processing;

pub(super) use processing::transform;

pub(super) fn members(path: &Path) -> Result<Option<Vec<PathBuf>>, MsTransformError> {
    let Ok(info) = fs::read_to_string(path.join("table.info")) else {
        return Ok(None);
    };
    if TableInfo::parse(&info).sub_type != "CONCATENATED" {
        return Ok(None);
    }
    Table::concatenated_source_paths(path).map_err(|source| table_error(path, source))
}

pub(super) fn retain(
    request: &MsTransformRequest,
    final_path: &Path,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    members: &[PathBuf],
) -> Result<Option<MsTransformReport>, MsTransformError> {
    if let Some(span) = options.retained_scan_span
        && partition_axis(&request.input_ms).as_deref() != Some("spw")
    {
        check_scan_span(members, request.selection.scan.as_deref(), span)?;
    }
    let started = std::time::Instant::now();
    let destination = &request.output_ms;
    let relative_members = Path::new("SUBMSS");
    let name = final_path
        .file_name()
        .expect("validated output file name")
        .to_string_lossy();
    let mut outputs = Vec::new();
    let mut tables = Vec::new();
    let mut report = None;
    for (index, member) in members.iter().enumerate() {
        let local = relative_members.join(format!("{name}.{index:04}.ms"));
        let mut part = request.clone();
        part.input_ms = member.clone();
        part.output_ms = destination.join(&local);
        let mut member_options = options.clone();
        if let Some(history) = &mut member_options.history {
            for (parameter, value) in &mut history.parameters {
                match parameter.as_str() {
                    "vis" => *value = format!("\"{}\"", part.input_ms.display()),
                    "outputvis" => *value = format!("\"{}\"", final_path.join(&local).display()),
                    "createmms" => *value = "False".into(),
                    _ => {}
                }
            }
        }
        match transform_product(&part, grammar, &member_options, None, None) {
            Ok(result) => {
                accumulate(&mut report, result);
                let table = Table::open(TableOptions::new(&part.output_ms))
                    .map_err(|source| table_error(&part.output_ms, source))?;
                tables.push(table);
                outputs.push(local);
            }
            Err(
                error @ (MsTransformError::EmptySelection { .. }
                | MsTransformError::InvalidSpw { .. }
                | MsTransformError::SelectRows { .. }
                | MsTransformError::MissingDataColumn { .. }),
            ) => {
                tracing::warn!(partition = %member.display(), error = %error, "transform skipped a Multi-MS partition")
            }
            Err(error) => return Err(error),
        }
    }

    let Some(mut report) = report else {
        return Ok(None);
    };
    install_shared_subtables(destination, &outputs, tables[0].keywords())?;
    let info = container_info(partition_axis(&request.input_ms).as_deref(), &outputs);
    let concat = Table::concat(tables).map_err(|source| table_error(destination, source))?;
    concat
        .save_with_info(TableOptions::new(destination), &info)
        .map_err(|source| table_error(destination, source))?;
    report.input_ms = request.input_ms.clone();
    report.output_ms = destination.clone();
    report.elapsed_ns = started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX);
    Ok(Some(report))
}

fn partition_axis(input: &Path) -> Option<String> {
    fs::read_to_string(input.join("table.info"))
        .ok()
        .and_then(|content| {
            TableInfo::parse(&content)
                .readme
                .into_iter()
                .find_map(|line| line.strip_prefix("AxisType = ").map(str::to_string))
        })
}

fn check_scan_span(
    members: &[PathBuf],
    selector: Option<&str>,
    span: f64,
) -> Result<(), MsTransformError> {
    let mut scans_by_member = Vec::with_capacity(members.len());
    for member in members {
        let table =
            Table::open(TableOptions::new(member)).map_err(|source| table_error(member, source))?;
        let read = |column| {
            table
                .column_accessor(column)
                .and_then(|cells| cells.scalar_cells_owned())
                .map_err(|source| table_error(member, source))
        };
        let scans = read("SCAN_NUMBER")?
            .into_iter()
            .enumerate()
            .map(|(row, value)| scalar_i32(value.as_ref(), "SCAN_NUMBER", row))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let times = read("TIME")?;
        let (min, max) = times.into_iter().enumerate().try_fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(min, max), (row, value)| {
                let time = scalar_f64(value.as_ref(), "TIME", row)?;
                Ok::<_, MsTransformError>((min.min(time), max.max(time)))
            },
        )?;
        if max - min < span {
            return Err(MsTransformError::MultiMsScanSpan);
        }
        scans_by_member.push(scans);
    }
    let Some(first) = scans_by_member.first() else {
        return Ok(());
    };
    let ranges = selector
        .filter(|text| !text.trim().is_empty())
        .map(|scan| parse_numeric_row_id_ranges(scan, "scan").unwrap_or_default());
    let requested = |scan| {
        ranges
            .as_ref()
            .is_none_or(|ranges| ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(&scan)))
    };
    // CASA constructs a map iterator of selected scan IDs. Its first
    // partition check consumes that iterator; later checks use time only.
    if scans_by_member
        .iter()
        .flatten()
        .any(|&scan| requested(scan) && !first.contains(&scan))
    {
        return Err(MsTransformError::MultiMsScanSpan);
    }
    Ok(())
}

fn container_info(axis: Option<&str>, members: &[PathBuf]) -> TableInfo {
    let mut readme = axis
        .map(|axis| format!("AxisType = {axis}"))
        .into_iter()
        .collect::<Vec<_>>();
    readme.extend([
        "This is a MeasurementSet Table holding measurements from a Telescope".into(),
        "This is a measurement set Table holding astronomical observations".into(),
        "Virtual concatenation of the following tables:".into(),
    ]);
    readme.extend(members.iter().map(|path| format!("  {}", path.display())));
    readme.push(String::new());
    TableInfo {
        table_type: "Measurement Set".into(),
        sub_type: "CONCATENATED".into(),
        readme,
    }
}

fn install_shared_subtables(
    root: &Path,
    members: &[PathBuf],
    first_keywords: &RecordValue,
) -> Result<(), MsTransformError> {
    let first = &members[0];
    let first_name = first.file_name().expect("relative member has a name");
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL.iter())
    {
        let first_table = first.join(id.name());
        if !root.join(&first_table).exists() {
            continue;
        }
        link_directory(&first_table, &root.join(id.name()))?;
        for member in &members[1..] {
            let destination = root.join(member).join(id.name());
            if !destination.exists() {
                continue;
            }
            if matches!(id, SubtableId::History | SubtableId::Source) {
                fs::remove_dir_all(&destination)
                    .map_err(|error| write_error(&destination, error))?;
                copy_dir_recursive(&root.join(&first_table), &destination)
                    .map_err(|error| write_error(&destination, error))?;
            } else {
                fs::remove_dir_all(&destination)
                    .map_err(|error| write_error(&destination, error))?;
                link_directory(
                    &Path::new("..").join(first_name).join(id.name()),
                    &destination,
                )?;
            }
        }
    }
    let first_path = casa_tables::lexical_absolute(&root.join(first));
    for field in first_keywords.fields() {
        let Value::TableRef(reference) = &field.value else {
            continue;
        };
        if let Some(name) = attached_subtable_name(&first_path, &field.name, reference) {
            let destination = root.join(&name);
            if !destination.exists() {
                link_directory(&first.join(name), &destination)?;
            }
        }
    }
    Ok(())
}

fn link_directory(target: &Path, destination: &Path) -> Result<(), MsTransformError> {
    #[cfg(unix)]
    let result = std::os::unix::fs::symlink(target, destination);
    #[cfg(windows)]
    let result = std::os::windows::fs::symlink_dir(target, destination);
    result.map_err(|error| write_error(destination, error))
}

fn accumulate(report: &mut Option<MsTransformReport>, mut next: MsTransformReport) {
    let Some(current) = report.as_mut() else {
        *report = Some(next);
        return;
    };
    current.row_count += next.row_count;
    current
        .spectral_window_ids
        .append(&mut next.spectral_window_ids);
    current.spectral_window_ids.sort_unstable();
    current.spectral_window_ids.dedup();
    current
        .output_channels_by_spw
        .append(&mut next.output_channels_by_spw);
    let total = &mut current.write_telemetry;
    let part = next.write_telemetry;
    total.columns.extend(part.columns);
    total.assemble_seconds += part.assemble_seconds;
    total.write_seconds += part.write_seconds;
    total.bytes_written += part.bytes_written;
    total.rows_written += part.rows_written;
    total.maximum_resident_bytes = total
        .maximum_resident_bytes
        .max(part.maximum_resident_bytes);
    total.producer_seconds += part.producer_seconds;
    total.queue_wait_seconds += part.queue_wait_seconds;
    total.finalize_seconds += part.finalize_seconds;
}

fn table_error(path: &Path, source: TableError) -> MsTransformError {
    MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    }
}

fn write_error(path: &Path, error: std::io::Error) -> MsTransformError {
    MsTransformError::WriteMeasurementSet {
        path: path.display().to_string(),
        reason: error.to_string(),
    }
}
