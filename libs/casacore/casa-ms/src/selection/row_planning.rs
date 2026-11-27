// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn plan_selected_row_blocks(
    selected_rows: &[usize],
    io: MsSelectionIoBudget,
) -> Result<Vec<MsSelectionRowBlock>, MsSelectionError> {
    validate_io_budget(io)?;
    if selected_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows_per_block = rows_per_block_for_budget(io)?;
    rows_per_block = rows_per_block.min(selected_rows.len().max(1));

    selected_rows
        .chunks(rows_per_block)
        .enumerate()
        .map(|(block_index, rows)| {
            let selected_row_start = block_index
                .checked_mul(rows_per_block)
                .ok_or(MsSelectionError::ByteOverflow)?;
            let planned_bytes = rows
                .len()
                .checked_mul(io.requested_bytes_per_row)
                .ok_or(MsSelectionError::ByteOverflow)?;
            Ok(MsSelectionRowBlock {
                selected_row_start,
                selected_row_count: rows.len(),
                planned_bytes,
            })
        })
        .collect()
}

/// Read the scalar metadata for selected MAIN rows using the already admitted row blocks.
pub fn resolve_selected_row_facts(
    ms: &MeasurementSet,
    selected_row_indices: &[usize],
    row_blocks: &[MsSelectionRowBlock],
) -> Result<Vec<ResolvedMsSelectionRow>, MsSelectionError> {
    let data_description = ms.data_description()?;
    let mut ddid_to_spw_pol = Vec::with_capacity(data_description.row_count());
    for row in 0..data_description.row_count() {
        ddid_to_spw_pol.push((
            data_description.spectral_window_id(row)?,
            data_description.polarization_id(row)?,
        ));
    }

    let table = ms.main_table();
    let mut resolved = Vec::with_capacity(selected_row_indices.len());
    for block in row_blocks {
        let end = block
            .selected_row_start
            .checked_add(block.selected_row_count)
            .ok_or(MsSelectionError::ByteOverflow)?;
        let rows = selected_row_indices
            .get(block.selected_row_start..end)
            .ok_or_else(|| {
                MsSelectionError::InvalidBudget(
                    "selected-row block lies outside the resolved selection".to_string(),
                )
            })?;
        let SelectedRowColumns {
            field_ids,
            data_desc_ids,
            antenna1_ids,
            antenna2_ids,
            flag_rows,
            times,
        } = SelectedRowColumns::read(table, rows)?;

        for slot in 0..rows.len() {
            let data_desc_id = data_desc_ids[slot];
            let (spectral_window_id, polarization_id) = usize::try_from(data_desc_id)
                .ok()
                .and_then(|ddid| ddid_to_spw_pol.get(ddid).copied())
                .ok_or_else(|| {
                    MsError::InvalidInput(format!(
                        "DATA_DESC_ID {data_desc_id} at MAIN row {} has no DATA_DESCRIPTION row",
                        rows[slot]
                    ))
                })?;
            resolved.push(ResolvedMsSelectionRow {
                row_index: rows[slot],
                field_id: field_ids[slot],
                data_desc_id,
                spectral_window_id,
                polarization_id,
                antenna1_id: antenna1_ids[slot],
                antenna2_id: antenna2_ids[slot],
                flag_row: flag_rows[slot],
                time_mjd_seconds: times[slot],
            });
        }
    }
    Ok(resolved)
}

pub(super) fn rows_per_block_for_budget(
    io: MsSelectionIoBudget,
) -> Result<usize, MsSelectionError> {
    validate_io_budget(io)?;
    let per_block_budget_bytes = io.available_bytes / io.maximum_live_blocks;
    let mut rows_per_block = per_block_budget_bytes / io.requested_bytes_per_row;
    if rows_per_block == 0 {
        return Err(MsSelectionError::InsufficientBudget {
            requested_bytes_per_row: io.requested_bytes_per_row,
            per_block_budget_bytes,
        });
    }
    if let Some(alignment) = io.storage_alignment_rows
        && rows_per_block >= alignment
    {
        rows_per_block -= rows_per_block % alignment;
    }
    Ok(rows_per_block)
}

pub(super) fn validate_io_budget(io: MsSelectionIoBudget) -> Result<(), MsSelectionError> {
    if io.maximum_live_blocks == 0 {
        return Err(MsSelectionError::InvalidBudget(
            "maximum_live_blocks must be positive".to_string(),
        ));
    }
    if io.requested_bytes_per_row == 0 {
        return Err(MsSelectionError::InvalidBudget(
            "requested_bytes_per_row must be positive".to_string(),
        ));
    }
    if matches!(io.storage_alignment_rows, Some(0)) {
        return Err(MsSelectionError::InvalidBudget(
            "storage_alignment_rows must be positive when supplied".to_string(),
        ));
    }
    Ok(())
}

/// The host's physical memory in bytes, or `None` where it cannot be
/// read; what [`MsSelectionIoBudget::from_system_memory`] takes an eighth
/// of.
#[cfg(target_os = "macos")]
pub fn system_physical_memory_bytes() -> Option<usize> {
    let mut value = 0u64;
    let mut size = std::mem::size_of::<u64>() as libc::size_t;
    let result = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (result == 0 && value > 0).then_some(value as usize)
}

/// The host's physical memory in bytes, or `None` where it cannot be
/// read; what [`MsSelectionIoBudget::from_system_memory`] takes an eighth
/// of.
#[cfg(all(unix, not(target_os = "macos")))]
pub fn system_physical_memory_bytes() -> Option<usize> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    (pages as usize).checked_mul(page_size as usize)
}

/// The host's physical memory in bytes, or `None` where it cannot be
/// read; what [`MsSelectionIoBudget::from_system_memory`] takes an eighth
/// of.
#[cfg(not(unix))]
pub fn system_physical_memory_bytes() -> Option<usize> {
    None
}
