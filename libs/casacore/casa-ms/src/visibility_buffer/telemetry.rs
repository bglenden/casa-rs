// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct ColumnReportInput<'a> {
    pub(super) table: &'a Table,
    pub(super) column_name: &'a str,
    pub(super) primitive: PrimitiveType,
    pub(super) channelized: bool,
    pub(super) channel_start: usize,
    pub(super) requested_channels: usize,
    pub(super) elements_per_channel_or_row: usize,
    pub(super) row_count: usize,
}

pub(super) struct ModeledColumnPhysicalRead {
    pub(super) element_bytes: usize,
    pub(super) granularity: VisibilityChannelReadGranularity,
    pub(super) bytes: u64,
}

pub(super) fn column_report(
    input: ColumnReportInput<'_>,
) -> MsResult<VisibilityBufferColumnReport> {
    let physical_read = modeled_column_physical_read(&input)?;
    let ColumnReportInput {
        table,
        column_name,
        primitive,
        channelized,
        channel_start: _,
        requested_channels,
        elements_per_channel_or_row,
        row_count,
    } = input;
    let data_manager_types = data_manager_types_for_column(table, column_name);
    let logical_elements = row_count
        .saturating_mul(requested_channels)
        .saturating_mul(elements_per_channel_or_row);
    let logical_output_bytes = logical_elements.saturating_mul(physical_read.element_bytes) as u64;
    Ok(VisibilityBufferColumnReport {
        column: column_name.to_string(),
        primitive_type: format!("{primitive:?}"),
        channelized,
        channel_read_granularity: physical_read.granularity,
        element_bytes: physical_read.element_bytes,
        logical_output_bytes,
        modeled_physical_read_bytes: physical_read.bytes,
        data_manager_types,
    })
}

pub(super) fn modeled_column_physical_read(
    input: &ColumnReportInput<'_>,
) -> MsResult<ModeledColumnPhysicalRead> {
    let element_bytes = input.primitive.fixed_width_bytes().ok_or_else(|| {
        MsError::InvalidInput(format!(
            "{} has variable-width type {:?}",
            input.column_name, input.primitive
        ))
    })?;
    let granularity = if input.channelized {
        channel_granularity_for_column(input.table, input.column_name)
    } else {
        VisibilityChannelReadGranularity::NotChannelized
    };
    let physical_channels = match granularity {
        VisibilityChannelReadGranularity::RequestedRange => modeled_tile_aligned_channel_count(
            input.table,
            input.column_name,
            input.channel_start,
            input.requested_channels,
        )
        .unwrap_or(input.requested_channels),
        VisibilityChannelReadGranularity::FullCell if input.channelized => {
            modeled_full_cell_channel_count(input.table, input.column_name)
                .unwrap_or(input.requested_channels)
        }
        VisibilityChannelReadGranularity::FullCell
        | VisibilityChannelReadGranularity::NotChannelized => input.requested_channels,
    };
    let bytes = input
        .row_count
        .saturating_mul(physical_channels)
        .saturating_mul(input.elements_per_channel_or_row)
        .saturating_mul(element_bytes) as u64;
    Ok(ModeledColumnPhysicalRead {
        element_bytes,
        granularity,
        bytes,
    })
}

pub(super) fn main_column_primitive_type(
    table: &Table,
    column_name: &str,
) -> MsResult<PrimitiveType> {
    let schema = table
        .schema()
        .ok_or_else(|| MsError::InvalidInput("main table has no schema".to_string()))?;
    schema
        .column(column_name)
        .ok_or_else(|| MsError::MissingColumn {
            column: column_name.to_string(),
            table: "MAIN".to_string(),
        })?
        .data_type()
        .ok_or_else(|| {
            MsError::InvalidInput(format!(
                "main table {column_name} column has no primitive data type"
            ))
        })
}

pub(super) fn main_table_has_column(table: &Table, column_name: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.contains_column(column_name))
}

pub(super) fn channel_granularity_for_column(
    table: &Table,
    column_name: &str,
) -> VisibilityChannelReadGranularity {
    let uses_tiled_shape_stman = table.data_manager_info().iter().any(|manager| {
        manager.dm_type == "TiledShapeStMan"
            && manager.columns.iter().any(|name| name == column_name)
    });
    if uses_tiled_shape_stman {
        VisibilityChannelReadGranularity::RequestedRange
    } else {
        VisibilityChannelReadGranularity::FullCell
    }
}

pub(super) fn data_manager_types_for_column(table: &Table, column_name: &str) -> Vec<String> {
    table
        .data_manager_info()
        .iter()
        .filter(|manager| manager.columns.iter().any(|name| name == column_name))
        .map(|manager| manager.dm_type.clone())
        .collect()
}

pub(super) fn modeled_full_cell_channel_count(_table: &Table, _column_name: &str) -> Option<usize> {
    None
}

pub(super) fn modeled_tile_aligned_channel_count(
    table: &Table,
    column_name: &str,
    channel_start: usize,
    requested_channels: usize,
) -> Option<usize> {
    let tile_width = table
        .array_column_2d_channel_tile_width(column_name)
        .ok()
        .flatten()
        .filter(|width| *width > 0)?;
    let requested_end = channel_start.checked_add(requested_channels)?;
    let physical_start = (channel_start / tile_width) * tile_width;
    let physical_end = requested_end.div_ceil(tile_width) * tile_width;
    Some(
        physical_end
            .saturating_sub(physical_start)
            .max(requested_channels),
    )
}

pub(super) fn first_1d_count(values: &[Option<ArrayValue>], column_name: &str) -> MsResult<usize> {
    for (row_slot, value) in values.iter().enumerate() {
        let Some(value) = value else {
            continue;
        };
        if value.ndim() != 1 {
            return Err(invalid_input(format!(
                "{column_name} row {row_slot} must be rank-1, found rank {}",
                value.ndim()
            )));
        }
        return Ok(value.shape()[0]);
    }
    Err(invalid_input(format!(
        "{column_name} has no defined selected rows"
    )))
}

pub(super) fn require_array(
    value: Option<ArrayValue>,
    column_name: &str,
    row_slot: usize,
) -> MsResult<ArrayValue> {
    value.ok_or_else(|| {
        invalid_input(format!(
            "{column_name} missing for selected row slot {row_slot}"
        ))
    })
}

pub(super) fn collect_capacities(buffer: &VisibilityBuffer) -> BTreeMap<String, usize> {
    let mut capacities = BTreeMap::new();
    capacities.insert("row_indices".to_string(), buffer.row_indices.capacity());
    if let Some(samples) = &buffer.data {
        capacities.insert("data".to_string(), samples.capacity());
        capacities.insert(
            "data_type".to_string(),
            primitive_type_capacity_marker(samples.primitive_type()),
        );
    }
    if let Some(values) = &buffer.flags {
        capacities.insert("flags".to_string(), values.capacity());
    }
    if let Some(samples) = &buffer.weights {
        capacities.insert("weights".to_string(), samples.capacity());
        capacities.insert(
            "weights_type".to_string(),
            primitive_type_capacity_marker(samples.primitive_type()),
        );
    }
    if let Some(samples) = &buffer.weight_spectrum {
        capacities.insert("weight_spectrum".to_string(), samples.capacity());
        capacities.insert(
            "weight_spectrum_type".to_string(),
            primitive_type_capacity_marker(samples.primitive_type()),
        );
    }
    if let Some(values) = &buffer.uvw {
        capacities.insert("uvw".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.antenna1 {
        capacities.insert("antenna1".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.antenna2 {
        capacities.insert("antenna2".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.data_desc_ids {
        capacities.insert("data_desc_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.field_ids {
        capacities.insert("field_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.flag_row {
        capacities.insert("flag_row".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.time {
        capacities.insert("time".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.interval {
        capacities.insert("interval".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.exposure {
        capacities.insert("exposure".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.array_ids {
        capacities.insert("array_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.observation_ids {
        capacities.insert("observation_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.scan_numbers {
        capacities.insert("scan_numbers".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.state_ids {
        capacities.insert("state_ids".to_string(), values.capacity());
    }
    capacities
}

pub(super) fn allocation_report(
    capacity_before: BTreeMap<String, usize>,
    capacity_after: BTreeMap<String, usize>,
) -> VisibilityBufferAllocationReport {
    let mut reused_buffers = 0usize;
    let mut grown_or_retyped_buffers = 0usize;
    for (name, after) in &capacity_after {
        if name.ends_with("_type") {
            continue;
        }
        let before = capacity_before.get(name).copied().unwrap_or(0);
        let type_marker = match name.as_str() {
            "data" => Some("data_type"),
            "weights" => Some("weights_type"),
            "weight_spectrum" => Some("weight_spectrum_type"),
            _ => None,
        };
        let same_type = type_marker.is_none_or(|marker| {
            capacity_before.contains_key(marker)
                && capacity_before.get(marker) == capacity_after.get(marker)
        });
        if before >= *after && before > 0 && same_type {
            reused_buffers += 1;
        } else {
            grown_or_retyped_buffers += 1;
        }
    }
    VisibilityBufferAllocationReport {
        reused_buffers,
        grown_or_retyped_buffers,
        capacity_before,
        capacity_after,
    }
}

pub(super) fn scalar_output_reuses_retained_buffer(
    column: &str,
    row_count: usize,
    capacity_before: &BTreeMap<String, usize>,
) -> bool {
    let buffer = match column {
        "ANTENNA1" => "antenna1",
        "ANTENNA2" => "antenna2",
        "DATA_DESC_ID" => "data_desc_ids",
        "FIELD_ID" => "field_ids",
        "FLAG_ROW" => "flag_row",
        "TIME" => "time",
        "INTERVAL" => "interval",
        "EXPOSURE" => "exposure",
        "ARRAY_ID" => "array_ids",
        "OBSERVATION_ID" => "observation_ids",
        "SCAN_NUMBER" => "scan_numbers",
        "STATE_ID" => "state_ids",
        _ => return false,
    };
    capacity_before
        .get(buffer)
        .is_some_and(|capacity| *capacity >= row_count)
}

pub(super) fn primitive_type_capacity_marker(primitive: PrimitiveType) -> usize {
    match primitive {
        PrimitiveType::Complex32 => 32,
        PrimitiveType::Complex64 => 64,
        PrimitiveType::Float32 => 132,
        PrimitiveType::Float64 => 164,
        _ => 0,
    }
}

pub(super) fn elapsed_ns(duration: Duration) -> u128 {
    duration.as_nanos()
}
