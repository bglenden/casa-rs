// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn open_import_archive(path: &Path) -> Result<VlaDiskReader<BufReader<File>>, VlaError> {
    let metadata = path.metadata().map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            VlaError::InvalidArgument {
                argument: "archivefiles",
                message: format!(
                    "all elements of the vector parameter must be a path that exists (missing {})",
                    path.display()
                ),
            }
        } else {
            VlaError::io(format!("read archive metadata {}", path.display()), source)
        }
    })?;
    if !metadata.is_file() {
        return Err(VlaError::import(format!(
            "cannot read from {}",
            path.display()
        )));
    }
    VlaDiskReader::open(path)
}

pub(super) fn plan_import_main_arrays(
    options: &ImportVlaOptions,
    selection: &ArchiveSelection,
    vis: &Path,
) -> Result<(MeasurementSetWritePlan, Vec<MeasurementSetArrayShapePlan>), VlaError> {
    let mut visibility_shapes = BTreeMap::<(usize, usize), usize>::new();
    let mut row_count = 0usize;
    for path in options.require_archivefiles()? {
        let mut reader = open_import_archive(path)?;
        let mut frequency_tolerance = options.frequencytol_hz;
        while let Some(record) = reader.next_record()? {
            if !record_is_selected_for_import(&record, options, selection)? {
                continue;
            }
            let antenna_count = usize::from(
                record
                    .rca()
                    .n_antennas()
                    .map_err(|error| VlaError::import(format!("decode antenna count: {error}")))?,
            );
            let cross_baselines = antenna_count
                .checked_mul(antenna_count.saturating_sub(1))
                .and_then(|count| count.checked_div(2))
                .ok_or_else(|| VlaError::import("MAIN baseline count overflow"))?;
            let baselines = cross_baselines
                .checked_add(if options.autocorr { antenna_count } else { 0 })
                .ok_or_else(|| VlaError::import("MAIN baseline count overflow"))?;
            let groups = collect_cda_groups(&record, &mut frequency_tolerance)?;
            for (descriptor, cda_ids) in groups {
                let correlation_count =
                    correlation_types_for_group(&record, &cda_ids, antenna_count)?.len();
                if correlation_count == 0 || descriptor.num_chan == 0 {
                    continue;
                }
                *visibility_shapes
                    .entry((correlation_count, descriptor.num_chan))
                    .or_default() += baselines;
                row_count = row_count
                    .checked_add(baselines)
                    .ok_or_else(|| VlaError::import("MAIN row count overflow"))?;
            }
        }
    }

    if row_count == 0 {
        return Err(VlaError::import(format!(
            "visibility file is empty: {}",
            vis.display()
        )));
    }
    let columns = msv2_array_columns(
        &visibility_shapes,
        &["DATA", "CORRECTED_DATA", "MODEL_DATA"],
        DEFAULT_TELESCOPE_NAME,
        DEFAULT_FLAG_CATEGORIES,
    )
    .map_err(|error| VlaError::import(error.to_string()))?;
    let visibility = columns[0].shapes.clone();
    let resources = MeasurementSetWriteResources::from_system_memory(2)
        .map_err(|error| VlaError::import(error.to_string()))?;
    msv2_main_plan(row_count, columns, resources)
        .map(|plan| (plan, visibility))
        .map_err(|error| VlaError::import(error.to_string()))
}

#[cfg(test)]
pub(super) fn test_import_write_plan(
    row_count: usize,
    correlation_count: usize,
    channel_count: usize,
) -> Result<MeasurementSetWritePlan, VlaError> {
    let columns = msv2_array_columns(
        &BTreeMap::from([((correlation_count, channel_count), row_count)]),
        &["DATA", "CORRECTED_DATA", "MODEL_DATA"],
        DEFAULT_TELESCOPE_NAME,
        1,
    )
    .map_err(|error| VlaError::import(error.to_string()))?;
    msv2_main_plan(
        row_count,
        columns,
        MeasurementSetWriteResources {
            available_bytes: 64 * 1024 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        },
    )
    .map_err(|error| VlaError::import(error.to_string()))
}
