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

/// Encoded files bound strings when their manager cannot report header-only
/// lengths. This is conservative for a table mixing strings with science data.
pub(super) fn text_bound(path: &std::path::Path) -> SimulatorResult<usize> {
    std::fs::read_dir(path)
        .map_err(super::error)?
        .try_fold(0usize, |total, entry| {
            let metadata = entry
                .map_err(super::error)?
                .metadata()
                .map_err(super::error)?;
            if metadata.is_file() {
                total
                    .checked_add(usize::try_from(metadata.len()).map_err(super::error)?)
                    .ok_or_else(|| super::error("encoded table size overflows usize"))
            } else {
                Ok(total)
            }
        })
}

/// The source descriptors, bounded source blocks and SSM save buffers coexist.
/// Tiled reads can stage two tiles; scalar save's bound also covers its index
/// and encoding buffers. Source bucket indices use at most two usize pairs per
/// row; they are metadata, rather than copies of the old science arrays.
pub(super) fn admit_copy(
    template: &Table,
    left: &Table,
    right: &Table,
    text_bound: usize,
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
    let text_factor = columns
        .iter()
        .filter(|column| column.data_type() == Some(PrimitiveType::String))
        .map(|column| {
            if matches!(column.column_type(), casa_tables::ColumnType::Array(_)) {
                16
            } else {
                4
            }
        })
        .max()
        .unwrap_or(0);
    bytes += text_bound
        .checked_mul(text_factor)
        .ok_or_else(|| super::error("append text memory overflows usize"))?;
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
    extra: Reservation,
}

impl<T> Default for Block<T> {
    fn default() -> Self {
        Self {
            owner: 0,
            first: 0,
            values: Vec::new(),
            extra: Reservation::none(),
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
    resources: &SessionResources,
) -> SimulatorResult<Option<ArrayValue>> {
    let mut cache = cache.lock().expect("append array block");
    if !cache.contains(table, row) {
        let column = table.column_accessor(key).map_err(super::error)?;
        let candidates: Vec<_> = (row..table.row_count().min(row + 64)).collect();
        let text = table
            .schema()
            .and_then(|schema| schema.column(key))
            .and_then(|column| column.data_type())
            == Some(PrimitiveType::String);
        let mut extra = Reservation::none();
        let count = if text {
            1
        } else {
            let size = column
                .array_shapes_uncached(&candidates)
                .map_err(super::error)?
                .iter()
                .flatten()
                .map(|shape| shape.iter().copied().fold(16usize, usize::saturating_mul))
                .max()
                .unwrap_or(1)
                .max(1);
            // A single cell can exceed the target block size. Admit its
            // decoded cache, returned clone and encoding before reading it.
            let bytes = size
                .checked_mul(3)
                .ok_or_else(|| super::error("append cell size overflows usize"))?
                .saturating_sub(2 * ARRAY_BYTES);
            if bytes != 0 {
                extra = admit(
                    resources.host(),
                    resources.policy(),
                    &Demand {
                        phase: "copying an oversized simulated cell",
                        memory: bytes as u64,
                    },
                )
                .map_err(SimulatorError::admission)?;
            }
            (ARRAY_BYTES / size).clamp(1, candidates.len())
        };
        cache.store(
            table,
            row,
            column
                .array_cells_owned_uncached(&candidates[..count])
                .map_err(super::error)?,
        );
        cache.extra = extra;
    }
    Ok(cache.value(row))
}
