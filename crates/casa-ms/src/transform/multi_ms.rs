// SPDX-License-Identifier: LGPL-3.0-or-later
//! Split the physical members of a CASA Multi-MS and publish a portable view.

use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableError, TableInfo, TableOptions};

use super::{
    MsTransformError, MsTransformReport, MsTransformRequest, MsTransformTaskOptions, RowGrammar,
    copy_dir_recursive, transform_product,
};
use crate::SubtableId;

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
        match transform_product(&part, grammar, options, None) {
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
                | MsTransformError::SelectRows { .. }),
            ) => {
                tracing::warn!(partition = %member.display(), error = %error, "split skipped a Multi-MS partition")
            }
            Err(error) => return Err(error),
        }
    }

    let Some(mut report) = report else {
        return Ok(None);
    };
    install_shared_subtables(destination, &outputs)?;
    let info = container_info(&request.input_ms, &outputs);
    let concat = Table::concat(tables).map_err(|source| table_error(destination, source))?;
    concat
        .save_with_info(TableOptions::new(destination), &info)
        .map_err(|source| table_error(destination, source))?;
    report.input_ms = request.input_ms.clone();
    report.output_ms = destination.clone();
    report.elapsed_ns = started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX);
    Ok(Some(report))
}

fn container_info(input: &Path, members: &[PathBuf]) -> TableInfo {
    let source = fs::read_to_string(input.join("table.info"))
        .map(|content| TableInfo::parse(&content))
        .unwrap_or_default();
    let mut readme = source
        .readme
        .into_iter()
        .filter(|line| line.starts_with("AxisType = "))
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

fn install_shared_subtables(root: &Path, members: &[PathBuf]) -> Result<(), MsTransformError> {
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
            if matches!(id, SubtableId::History) {
                fs::remove_dir_all(&destination)
                    .map_err(|error| write_error(&destination, error))?;
                copy_dir_recursive(&root.join(&first_table), &destination)
                    .map_err(|error| write_error(&destination, error))?;
            } else if !matches!(id, SubtableId::Source) {
                fs::remove_dir_all(&destination)
                    .map_err(|error| write_error(&destination, error))?;
                link_directory(
                    &Path::new("..").join(first_name).join(id.name()),
                    &destination,
                )?;
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
