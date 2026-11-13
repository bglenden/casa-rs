// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded block reads for the append writer's generated column sources.
use crate::session::SessionResources;
use crate::session::error::{SimulatorError, SimulatorResult};
use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_tables::{Table, TableError};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue};
use std::sync::Mutex;

pub(super) const SCALAR_ROWS: usize = 512;
pub(super) const ARRAY_BYTES: usize = 256 * 1024;

/// The source descriptors, bounded source blocks and SSM save buffers coexist.
/// Tiled reads can stage two tiles; scalar save's bound also covers its index
/// and encoding buffers. Source bucket indices use at most two usize pairs per
/// row; they are metadata, rather than copies of the old science arrays.
pub(super) fn admit_copy(
    template: &Table,
    left: &Table,
    right: &Table,
    resources: &SessionResources,
) -> SimulatorResult<Reservation> {
    let error = |e: casa_tables::TableError| SimulatorError::raised(e.to_string());
    let metadata = |bytes: Option<usize>| {
        bytes.ok_or_else(|| {
            SimulatorError::raised("append source has unmodeled retained table state")
        })
    };
    let template_bytes = metadata(template.retained_owned_metadata_bytes())?;
    let mut bytes = template_bytes
        + metadata(left.retained_read_metadata_bytes())?
        + metadata(right.retained_read_metadata_bytes())?;
    let columns = template.schema().expect("append schema").columns();
    bytes += casa_tables::streamed_scalar_columns_resident_bytes(columns.len())
        .ok_or_else(|| SimulatorError::raised("append copy memory overflows usize"))?;
    bytes += left.row_count().max(right.row_count()) * 4 * size_of::<usize>();
    for column in columns {
        bytes += match column.column_type() {
            casa_tables::ColumnType::Array(_) => {
                let tiles = left
                    .array_column_tile_bytes(column.name())
                    .map_err(error)?
                    .unwrap_or(0)
                    .max(
                        right
                            .array_column_tile_bytes(column.name())
                            .map_err(error)?
                            .unwrap_or(0),
                    );
                2 * tiles + 2 * ARRAY_BYTES + 128 * size_of::<Option<ArrayValue>>()
            }
            _ => 2 * SCALAR_ROWS * size_of::<Option<ScalarValue>>(),
        };
    }
    // Save clones table descriptors while binding generated columns.
    bytes += 2 * template_bytes;
    admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "copying existing simulated observations",
            memory: bytes as u64,
        },
    )
    .map_err(SimulatorError::admission)
}

pub(super) struct Block<T> {
    owner: usize,
    first: usize,
    values: Vec<Option<T>>,
}

impl<T> Default for Block<T> {
    fn default() -> Self {
        Self {
            owner: 0,
            first: 0,
            values: Vec::new(),
        }
    }
}

impl<T: Clone> Block<T> {
    fn contains(&self, table: &Table, row: usize) -> bool {
        self.owner == std::ptr::from_ref(table) as usize
            && row >= self.first
            && row - self.first < self.values.len()
    }
    fn store(&mut self, table: &Table, first: usize, values: Vec<Option<T>>) {
        self.owner = std::ptr::from_ref(table) as usize;
        self.first = first;
        self.values = values;
    }
    fn value(&self, row: usize) -> Option<T> {
        self.values[row - self.first].clone()
    }
}

pub(super) fn scalar(
    table: &Table,
    key: &str,
    row: usize,
    cache: &Mutex<Block<ScalarValue>>,
) -> Result<Option<ScalarValue>, TableError> {
    let mut cache = cache.lock().expect("append scalar block");
    if !cache.contains(table, row) {
        let indices: Vec<_> = (row..table.row_count().min(row + SCALAR_ROWS)).collect();
        cache.store(
            table,
            row,
            table
                .column_accessor(key)?
                .scalar_cells_owned_for_rows(&indices)?,
        );
    }
    Ok(cache.value(row))
}

pub(super) fn array(
    table: &Table,
    key: &str,
    row: usize,
    cache: &Mutex<Block<ArrayValue>>,
) -> Result<Option<ArrayValue>, TableError> {
    let mut cache = cache.lock().expect("append array block");
    if !cache.contains(table, row) {
        let column = table.column_accessor(key)?;
        let candidates: Vec<_> = (row..table.row_count().min(row + 64)).collect();
        let text = table
            .schema()
            .and_then(|schema| schema.column(key))
            .and_then(|column| column.data_type())
            == Some(PrimitiveType::String);
        let count = if text {
            1
        } else {
            let size = column
                .array_shapes_uncached(&candidates)?
                .iter()
                .flatten()
                .map(|shape| shape.iter().copied().fold(16usize, usize::saturating_mul))
                .max()
                .unwrap_or(1)
                .max(1);
            (ARRAY_BYTES / size).clamp(1, candidates.len())
        };
        cache.store(
            table,
            row,
            column.array_cells_owned_uncached(&candidates[..count])?,
        );
    }
    Ok(cache.value(row))
}
