// SPDX-License-Identifier: LGPL-3.0-or-later
//! Preserve existing cells while publishing appended simulated rows.
use casa_imaging_runtime::Reservation;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[path = "append_sources.rs"]
mod sources;

use casa_tables::{
    ColumnOverrides, ColumnType, DataManagerKind, GeneratedArrayColumn, GeneratedScalarColumn,
    Table, TableOptions,
};

use crate::session::SessionResources;
use crate::session::error::{SimulatorError, SimulatorResult};

fn error(value: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!("append simulated observations: {value}"))
}

/// Copy a private stage, leaving the source's science and metadata untouched.
pub(crate) fn copy_tree(source: &Path, target: &Path) -> SimulatorResult<()> {
    std::fs::create_dir_all(target).map_err(error)?;
    for entry in std::fs::read_dir(source).map_err(error)? {
        let entry = entry.map_err(error)?;
        if entry.file_name() == "table.lock" {
            continue;
        }
        let destination = target.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &destination)?;
        } else {
            std::fs::copy(entry.path(), destination).map_err(error)?;
        }
    }
    Ok(())
}

struct RowsCopy {
    template: Table,
    columns: ColumnOverrides,
    failed: Arc<Mutex<Option<SimulatorError>>>,
    _reservation: Reservation,
}

impl RowsCopy {
    fn new(
        left: &Path,
        right: &Path,
        skip: usize,
        resources: &SessionResources,
    ) -> SimulatorResult<Self> {
        // Every output column comes from an override. A metadata construction
        // table prevents save() from materializing the input's generic rows.
        let text_bound = sources::text_bound(left)?.max(sources::text_bound(right)?);
        let template =
            Table::open_metadata_for_construction(TableOptions::new(left), 0).map_err(error)?;
        let left = Arc::new(Table::open(TableOptions::new(left)).map_err(error)?);
        let right = Arc::new(Table::open(TableOptions::new(right)).map_err(error)?);
        let reservation = sources::admit_copy(&template, &left, &right, text_bound, resources)?;
        let first = left.row_count();
        let rows = first
            .checked_add(
                right
                    .row_count()
                    .checked_sub(skip)
                    .ok_or_else(|| error("invalid appended row range"))?,
            )
            .ok_or_else(|| error("appended row count overflows usize"))?;
        let failed = Arc::new(Mutex::new(None));
        let mut columns = ColumnOverrides::for_row_count(rows);
        for column in template
            .schema()
            .ok_or_else(|| error("table has no schema"))?
            .columns()
        {
            let name = column.name().to_string();
            let (a, b, failure) = (Arc::clone(&left), Arc::clone(&right), Arc::clone(&failed));
            let source = move |row: usize| {
                if row < first {
                    (Arc::clone(&a), row)
                } else {
                    (Arc::clone(&b), row - first + skip)
                }
            };
            match column.column_type() {
                ColumnType::Array(_) => {
                    let key = name.clone();
                    let resources = *resources;
                    let cache = Mutex::new(sources::Block::default());
                    columns.insert_generated_array(
                        name,
                        GeneratedArrayColumn::new(rows, move |row| {
                            let (table, row) = source(row);
                            if !table
                                .schema()
                                .is_some_and(|schema| schema.contains_column(&key))
                            {
                                return None;
                            }
                            match sources::array(&table, &key, row, &cache, &resources) {
                                Ok(value) => value,
                                Err(value) => {
                                    failure
                                        .lock()
                                        .expect("append read error")
                                        .get_or_insert(value);
                                    None
                                }
                            }
                        }),
                    );
                }
                ColumnType::Scalar => {
                    let key = name.clone();
                    let cache = Mutex::new(sources::Block::default());
                    columns.insert_generated_scalar(
                        name,
                        GeneratedScalarColumn::new(rows, move |row| {
                            let (table, row) = source(row);
                            if !table
                                .schema()
                                .is_some_and(|schema| schema.contains_column(&key))
                            {
                                return None;
                            }
                            match sources::scalar(&table, &key, row, &cache) {
                                Ok(value) => value,
                                Err(value) => {
                                    failure
                                        .lock()
                                        .expect("append read error")
                                        .get_or_insert_with(|| error(value));
                                    None
                                }
                            }
                        }),
                    );
                }
                ColumnType::Record => {
                    return Err(error(
                        "record-valued MAIN columns cannot be appended (#1294)",
                    ));
                }
            }
        }
        Ok(Self {
            template,
            columns,
            failed,
            _reservation: reservation,
        })
    }

    fn save(mut self, output: &Path) -> SimulatorResult<()> {
        self.template
            .prepare_write()
            .save_with_column_overrides(
                TableOptions::new(output).with_data_manager(DataManagerKind::StandardStMan),
                &Default::default(),
                &self.columns,
            )
            .map_err(error)?;
        if let Some(failure) = self.failed.lock().expect("append read error").take() {
            return Err(failure);
        }
        Ok(())
    }
}

/// Concatenate stored cells without recalculating flags, weights or sigma.
/// Reads bypass the row cache and keep a bounded block per column.
pub(crate) fn join_table(
    left: &Path,
    right: &Path,
    skip: usize,
    output: &Path,
    resources: &SessionResources,
) -> SimulatorResult<()> {
    RowsCopy::new(left, right, skip, resources)?.save(output)
}

/// Initialize only the scratch columns present in the opened MS. Existing
/// geometry columns participate in the same bounded generated-source save.
pub(crate) fn scratch(
    path: &Path,
    present: [bool; 2],
    shape: &[usize],
    resources: &SessionResources,
) -> SimulatorResult<()> {
    use casa_tables::ColumnSchema;
    use casa_types::{ArrayValue, Complex32, PrimitiveType};
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};
    let rows = Table::rows_on_disk(path).map_err(error)?;
    let mut copy = RowsCopy::new(path, path, rows, resources)?;
    for (name, present) in ["MODEL_DATA", "CORRECTED_DATA"].into_iter().zip(present) {
        if !present {
            continue;
        }
        copy.template
            .add_column(
                ColumnSchema::array_variable(name, PrimitiveType::Complex32, Some(2)),
                None,
            )
            .map_err(error)?;
        copy.columns.insert_generated_array(
            name,
            GeneratedArrayColumn::constant(
                rows,
                Some(ArrayValue::Complex32(ArrayD::from_elem(
                    IxDyn(shape).f(),
                    Complex32::new(0., 0.),
                ))),
            ),
        );
    }
    copy.save(path)
}

/// Publish the old MAIN followed by the segment's MAIN and pointing rows.
/// Existing OBSERVATION and STATE cells persist, with only their new tails added.
pub(crate) fn merge(
    old: &Path,
    segment: &Path,
    output: &Path,
    resources: &SessionResources,
) -> SimulatorResult<()> {
    std::fs::create_dir_all(output).map_err(error)?;
    for entry in std::fs::read_dir(old).map_err(error)? {
        let entry = entry.map_err(error)?;
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &output.join(entry.file_name()))?;
        }
    }
    join_table(old, segment, 0, output, resources)?;
    for name in ["POINTING", "OBSERVATION", "STATE"] {
        let source = old.join(name);
        let skip = if name == "POINTING" {
            0
        } else {
            Table::rows_on_disk(&source).map_err(error)?
        };
        join_table(
            &source,
            &segment.join(name),
            skip,
            &output.join(name),
            resources,
        )?;
    }
    Ok(())
}
