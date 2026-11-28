// SPDX-License-Identifier: LGPL-3.0-or-later
//! A geometry-only survey of a persisted observation.

use std::path::Path;

use casa_core::PrimitiveType;
use casa_tables::{
    ArrayShapeContract, ColumnType, LockMode, LockOptions, SelectedArray1DCellsMut, Table,
    TableOptions,
};

use super::msv2::{catalogue::require_units, held::table_opening_bytes};
use super::{ObservationError, is_msv4_store};
use crate::TARGET_BATCH_BYTES;

/// Visit stored UVW triples in metres, including flagged samples. Order is
/// native to each store. MSv4 visits the `base` UVW, including its NaNs for
/// unwritten cells. MSv2 reads MAIN's packed UVW cells without opening the
/// subtables or assembling a time and baseline grid.
///
/// `budget` bounds the opening and one batch. `reserve` grants the total
/// bytes held in each phase against the caller's current policy. Requests
/// can decrease once an opening or batch releases its transient workspace.
///
/// # Errors
/// A store cannot be read, its geometry does not match the supported layout,
/// or the budget or reservation cannot admit a batch.
pub fn visit_stored_uvw(
    path: &Path,
    budget: usize,
    reserve: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    visit: &mut dyn FnMut([f64; 3]),
) -> Result<(), ObservationError> {
    let mut grant = |required| {
        if required > budget {
            return Err(over_budget(required, budget));
        }
        reserve(required)
    };
    if is_msv4_store(path) {
        #[cfg(feature = "msv4")]
        return msv4_uvw(path, budget, &mut grant, visit);
        #[cfg(not(feature = "msv4"))]
        return Err(ObservationError::Unsupported {
            what: "an MSv4 store without casa-ms's msv4 feature",
        });
    }
    msv2_uvw(path, budget, &mut grant, visit)
}

fn over_budget(required: usize, budget: usize) -> ObservationError {
    ObservationError::OverBudget {
        what: "the stored UVW survey",
        required,
        budget,
    }
}

fn msv2_uvw(
    path: &Path,
    budget: usize,
    grant: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    visit: &mut dyn FnMut([f64; 3]),
) -> Result<(), ObservationError> {
    const COLUMN: &str = "UVW";
    const CELL: usize = size_of::<[f64; 3]>();
    let table_error = |error: casa_tables::TableError| ObservationError::Storage(Box::new(error));
    let opening = table_opening_bytes(path);
    grant(opening)?;
    let table = Table::open_with_lock_attempts(
        TableOptions::new(path),
        LockOptions::new(LockMode::PermanentLocking).read_only(true),
        1,
    )
    .map_err(table_error)?;
    if table.row_count() == 0 {
        return Ok(());
    }
    let supported_shape = table
        .schema()
        .and_then(|schema| schema.column(COLUMN))
        .is_some_and(|column| {
            column.data_type() == Some(PrimitiveType::Float64)
                && matches!(column.column_type(), ColumnType::Array(ArrayShapeContract::Fixed { shape }) if shape == &[3])
        });
    if !supported_shape {
        return Err(ObservationError::InvalidStore {
            reason: "MAIN.UVW must declare a fixed Float64 triple".to_string(),
        });
    }
    let column = table.column_accessor(COLUMN).map_err(table_error)?;
    require_units(&table, COLUMN, "m")?;
    let footprint = table.selected_cell_read_footprint(COLUMN);
    let tiles = table.array_column_tile_bytes(COLUMN).map_err(table_error)?;
    let fixed = opening
        .saturating_add(tiles.unwrap_or(0).saturating_mul(2))
        .saturating_add(footprint.staging_fixed_bytes(CELL).unwrap_or(usize::MAX));
    let per_row = (CELL + size_of::<usize>())
        .saturating_add(footprint.staging_bytes_per_row(CELL).unwrap_or(usize::MAX));
    let batch = budget.saturating_sub(fixed).min(TARGET_BATCH_BYTES);
    let rows = (batch / per_row).min(table.row_count());
    if rows == 0 {
        return Err(over_budget(fixed.saturating_add(per_row), budget));
    }
    grant(fixed.saturating_add(rows * per_row))?;
    let mut row_ids = Vec::with_capacity(rows);
    let mut uvw = Vec::with_capacity(rows * 3);
    for first in (0..table.row_count()).step_by(rows) {
        row_ids.clear();
        row_ids.extend(first..first.saturating_add(rows).min(table.row_count()));
        uvw.clear();
        let shape = column
            .fill_array_cells_1d_typed_uncached(
                &row_ids,
                SelectedArray1DCellsMut::Float64(&mut uvw),
            )
            .map_err(table_error)?;
        if shape.axis0_count != 3 || shape.row_count != row_ids.len() {
            return Err(ObservationError::InvalidStore {
                reason: "MAIN.UVW must have one defined triple per row".to_string(),
            });
        }
        for &[u, v, w] in uvw.as_chunks::<3>().0 {
            visit([u, v, w]);
        }
    }
    Ok(())
}

#[cfg(feature = "msv4")]
fn msv4_uvw(
    path: &Path,
    budget: usize,
    grant: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    visit: &mut dyn FnMut([f64; 3]),
) -> Result<(), ObservationError> {
    use super::{Msv4Observation, OpenOptions};
    use casa_observation::{
        AccessRequest, BatchBuffers, CorrelationRequest, FieldSelection, Grouping, PartitionId,
        ReadEvent, Roles, TimeSelection, WeightRequest,
    };

    let observation = Msv4Observation::open_granted(path, &OpenOptions::default(), grant)?;
    let held = observation.held_bytes();
    grant(held)?;
    let mut request = AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".into(),
        roles: Roles {
            values: false,
            flags: false,
            weights: false,
            uvw: true,
            time_centroid: false,
            interval: false,
            exposure: false,
            scan: false,
        },
        weights: WeightRequest::Stored,
        channels: 0..1,
        times: TimeSelection::All,
        fields: FieldSelection::All,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: 0,
    };
    for (index, partition) in observation.catalogue().partitions.iter().enumerate() {
        let id = PartitionId(index as u32);
        let fixed = held.saturating_add(observation.stored_uvw_decode_bytes(id)?);
        // This request delivers UVW and its mandatory time/field header.
        // Keep integrations whole, including those above the batch target.
        let per_time = partition
            .baselines
            .len()
            .saturating_mul(size_of::<[f64; 3]>())
            .saturating_add(request.roles.time_bytes(0));
        request.batch_bytes = uvw_batch_budget(budget, fixed, per_time)?;
        grant(fixed.saturating_add(request.batch_bytes))?;
        request.partitions[0] = id;
        let mut traversal = observation.bind_read(&request)?;
        let mut buffers = BatchBuffers::with_envelope(traversal.estimate.peak_buffer_bytes());
        while let Some(event) = traversal.next(&mut buffers)? {
            if let ReadEvent::Batch(batch) = event {
                let uvw = batch.group.uvw.expect("UVW was requested").metres();
                for row in uvw.rows() {
                    visit([row[0], row[1], row[2]]);
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "msv4")]
fn uvw_batch_budget(
    budget: usize,
    fixed: usize,
    one_time: usize,
) -> Result<usize, ObservationError> {
    let required = fixed.saturating_add(one_time);
    let residual = budget.saturating_sub(fixed);
    if fixed > budget || one_time > residual {
        return Err(over_budget(required, budget));
    }
    Ok(residual.min(TARGET_BATCH_BYTES.max(one_time)))
}

#[cfg(all(test, feature = "msv4"))]
mod tests {
    use super::*;

    #[test]
    fn the_batch_target_allows_an_admitted_integration_larger_than_the_target() {
        let one_time = TARGET_BATCH_BYTES + 308;
        let fixed = 16 << 10;
        let required = fixed + one_time;
        assert_eq!(
            uvw_batch_budget(required, fixed, one_time).unwrap(),
            one_time
        );
        assert!(matches!(
            uvw_batch_budget(required - 1, fixed, one_time),
            Err(ObservationError::OverBudget { required: bytes, .. }) if bytes == required
        ));
    }
}
