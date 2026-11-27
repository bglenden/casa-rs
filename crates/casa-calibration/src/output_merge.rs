// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `storeNCT` identity checks and spectral-window metadata merge.

use std::path::Path;

use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::CalibrationTableWriteError;

/// Materialize the sorted rows, rather than saving a reference table: the
/// published product must own its data after the staging directory is renamed.
pub(crate) fn save_sorted(
    table: &mut Table,
    path: &Path,
) -> Result<(), CalibrationTableWriteError> {
    if !table
        .schema()
        .is_some_and(|s| s.contains_column("SPECTRAL_WINDOW_ID"))
    {
        return table
            .save(TableOptions::new(path))
            .map_err(|e| invalid(path, e.to_string()));
    }
    let keys = [
        ("SPECTRAL_WINDOW_ID", casa_tables::SortOrder::Ascending),
        ("TIME", casa_tables::SortOrder::Ascending),
        ("ANTENNA1", casa_tables::SortOrder::Ascending),
        ("ANTENNA2", casa_tables::SortOrder::Ascending),
    ]
    .into_iter()
    .filter(|(column, _)| table.schema().is_some_and(|s| s.contains_column(column)))
    .collect::<Vec<_>>();
    let order = table
        .sort(&keys)
        .map_err(|e| invalid(path, e.to_string()))?
        .row_numbers()
        .to_vec();
    let mut rows = table
        .rows()
        .map_err(|e| invalid(path, e.to_string()))?
        .to_vec();
    for (row, undefined) in rows.iter_mut().zip(
        table
            .undefined_cells()
            .map_err(|e| invalid(path, e.to_string()))?,
    ) {
        for column in undefined {
            row.remove(column);
        }
    }
    table
        .remove_rows(&(0..rows.len()).collect::<Vec<_>>())
        .map_err(|e| invalid(path, e.to_string()))?;
    for source in order {
        table
            .add_row(rows[source].clone())
            .map_err(|e| invalid(path, e.to_string()))?;
    }
    table
        .save(
            TableOptions::new(path).with_data_manager(casa_tables::DataManagerKind::StandardStMan),
        )
        .map_err(|e| invalid(path, e.to_string()))
}

pub(crate) fn invalid(path: &Path, reason: impl Into<String>) -> CalibrationTableWriteError {
    CalibrationTableWriteError::Prepare {
        path: path.display().to_string(),
        reason: reason.into(),
    }
}

pub(crate) fn open(path: &Path) -> Result<Table, CalibrationTableWriteError> {
    Table::open(TableOptions::new(path)).map_err(|e| invalid(path, e.to_string()))
}

pub(crate) fn validate_identity(
    old: &Table,
    new: &Table,
    path: &Path,
) -> Result<(), CalibrationTableWriteError> {
    if matches!(
        new.info().sub_type.as_str(),
        "BPOLY" | "GSPLINE" | "EP Jones"
    ) {
        return if old.info().sub_type == new.info().sub_type {
            Ok(())
        } else {
            Err(invalid(
                path,
                "incompatible legacy calibration subtype; prior product is unchanged",
            ))
        };
    }
    for key in ["VisCal", "ParType", "MSName"] {
        if old.keywords().get(key) != new.keywords().get(key) {
            return Err(invalid(
                path,
                format!("incompatible calibration table {key}; prior product is unchanged"),
            ));
        }
    }
    Ok(())
}

/// Legacy calibration tables reuse a CAL_DESC row for the same window
/// and append MAIN in solution order.
pub(crate) fn append_legacy(
    old: &mut Table,
    new: &mut Table,
    source: &Path,
    stage: &Path,
) -> Result<(), CalibrationTableWriteError> {
    let mut descriptors = open(&source.join("CAL_DESC"))?;
    let incoming = open(&stage.join("CAL_DESC"))?;
    let mut mapping = Vec::new();
    for row in incoming.rows().map_err(|e| invalid(stage, e.to_string()))? {
        let id = if let Some(id) = descriptors
            .rows()
            .map_err(|e| invalid(source, e.to_string()))?
            .iter()
            .position(|prior| prior.get("SPECTRAL_WINDOW_ID") == row.get("SPECTRAL_WINDOW_ID"))
        {
            id
        } else {
            let id = descriptors.row_count();
            descriptors
                .add_row(row.clone())
                .map_err(|e| invalid(source, e.to_string()))?;
            id
        };
        mapping.push(id as i32);
    }
    for row in 0..new.row_count() {
        let Some(Value::Scalar(ScalarValue::Int32(id))) = new
            .cell_accessor(row, "CAL_DESC_ID")
            .and_then(|c| c.value())
            .map_err(|e| invalid(stage, e.to_string()))?
        else {
            return Err(invalid(stage, "missing CAL_DESC_ID"));
        };
        let mapped = *mapping
            .get(*id as usize)
            .ok_or_else(|| invalid(stage, "invalid CAL_DESC_ID"))?;
        new.cell_accessor_mut(row, "CAL_DESC_ID")
            .and_then(|mut c| c.set(Value::Scalar(ScalarValue::Int32(mapped))))
            .map_err(|e| invalid(stage, e.to_string()))?;
    }
    if new.info().sub_type == "GSPLINE" {
        // There is one fit per antenna and field. Keep unrelated fields and
        // replace existing fits for the fields solved by this call.
        let fields = |table: &Table| {
            (0..table.row_count())
                .map(|row| {
                    match table
                        .cell_accessor(row, "FIELD_ID")
                        .and_then(|cell| cell.scalar())
                    {
                        Ok(ScalarValue::Int32(id)) => Ok(*id),
                        _ => Err(invalid(source, "invalid GSPLINE FIELD_ID")),
                    }
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let incoming_fields = fields(new)?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let replaced = fields(old)?
            .iter()
            .enumerate()
            .filter_map(|(row, field)| incoming_fields.contains(field).then_some(row))
            .collect::<Vec<_>>();
        old.remove_rows(&replaced)
            .map_err(|error| invalid(source, error.to_string()))?;
    }
    append(old, new, source)?;
    if new.info().sub_type == "GSPLINE" {
        pad_spline_columns(old, source)?;
    }
    preserve_subtables(source, stage)?;
    descriptors
        .save(TableOptions::new(stage.join("CAL_DESC")))
        .map_err(|e| invalid(stage, e.to_string()))
}

fn pad_spline_columns(table: &mut Table, path: &Path) -> Result<(), CalibrationTableWriteError> {
    // CASA loads each legacy column as a whole array, then reads the logical
    // knot/coefficient count in each row. Make the storage shape uniform.
    for name in [
        "SPLINE_KNOTS_AMP",
        "SPLINE_KNOTS_PHASE",
        "POLY_COEFF_AMP",
        "POLY_COEFF_PHASE",
    ] {
        let len = |row| {
            let cell = table
                .cell_accessor(row, name)
                .and_then(|cell| cell.array())
                .map_err(|error| invalid(path, error.to_string()))?;
            match cell.shape() {
                [1, 1, 1, len] => Ok(*len),
                _ => Err(invalid(path, format!("invalid GSPLINE {name} cell shape"))),
            }
        };
        let lengths = (0..table.row_count())
            .map(len)
            .collect::<Result<Vec<_>, _>>()?;
        let largest = lengths.iter().copied().max().unwrap_or(0);
        for (row, size) in lengths.into_iter().enumerate() {
            if size == largest {
                continue;
            }
            let ArrayValue::Float64(values) = table
                .cell_accessor(row, name)
                .and_then(|cell| cell.array())
                .map_err(|error| invalid(path, error.to_string()))?
            else {
                return Err(invalid(path, format!("invalid GSPLINE {name} cell type")));
            };
            let mut padded: Vec<_> = values.iter().copied().collect();
            padded.resize(largest, 0.);
            let array = ArrayD::from_shape_vec(IxDyn(&[1, 1, 1, largest]).f(), padded)
                .map_err(|error| invalid(path, error.to_string()))?;
            table
                .cell_accessor_mut(row, name)
                .and_then(|mut cell| cell.set(Value::Array(ArrayValue::Float64(array))))
                .map_err(|error| invalid(path, error.to_string()))?;
        }
    }
    Ok(())
}

/// Append incoming rows to the original schema (CASA's optional ARRAY_ID and
/// TIME_EXTRA_PREC columns need not be present in both implementations).
pub(crate) fn append(
    old: &mut Table,
    new: &Table,
    path: &Path,
) -> Result<(), CalibrationTableWriteError> {
    validate_identity(old, new, path)?;
    let columns = old.schema().expect("persisted schema").columns().to_vec();
    for column in new.schema().expect("incoming schema").columns() {
        if !matches!(column.name(), "ARRAY_ID" | "TIME_EXTRA_PREC")
            && !old
                .schema()
                .is_some_and(|schema| schema.contains_column(column.name()))
        {
            return Err(invalid(
                path,
                format!("incompatible append schema: missing {}", column.name()),
            ));
        }
    }
    for column in &columns {
        let source = new.schema().and_then(|s| s.column(column.name()));
        if source.is_none() && matches!(column.name(), "ARRAY_ID" | "TIME_EXTRA_PREC") {
            continue;
        }
        if !source.is_some_and(|s| {
            s.data_type() == column.data_type()
                && (s.column_type() == column.column_type()
                    || matches!(
                        (s.column_type(), column.column_type()),
                        (
                            casa_tables::ColumnType::Array(_),
                            casa_tables::ColumnType::Array(_)
                        )
                    ))
        }) {
            return Err(invalid(
                path,
                format!("incompatible append schema for {}", column.name()),
            ));
        }
    }
    let undefined = new
        .undefined_cells()
        .map_err(|e| invalid(path, e.to_string()))?;
    for (index, row) in new
        .rows()
        .map_err(|e| invalid(path, e.to_string()))?
        .iter()
        .enumerate()
    {
        let fields = columns
            .iter()
            .filter(|column| !undefined[index].contains(column.name()))
            .filter_map(|column| {
                let value = row
                    .get(column.name())
                    .cloned()
                    .or_else(|| match column.name() {
                        "ARRAY_ID" => Some(Value::Scalar(ScalarValue::Int32(0))),
                        "TIME_EXTRA_PREC" => Some(Value::Scalar(ScalarValue::Float64(0.0))),
                        // A freshly solved delay row leaves WEIGHT undefined.
                        _ => None,
                    })?;
                Some(RecordField::new(column.name(), value))
            })
            .collect();
        old.add_row(RecordValue::new(fields))
            .map_err(|e| invalid(path, e.to_string()))?;
    }
    Ok(())
}

pub(crate) fn preserve_subtables(
    source: &Path,
    stage: &Path,
) -> Result<(), CalibrationTableWriteError> {
    for entry in std::fs::read_dir(source).map_err(|e| invalid(source, e.to_string()))? {
        let entry = entry.map_err(|e| invalid(source, e.to_string()))?;
        let kind = entry
            .file_type()
            .map_err(|e| invalid(source, e.to_string()))?;
        if kind.is_symlink() {
            return Err(invalid(
                source,
                "cannot safely append a product containing symbolic links; prior product is unchanged",
            ));
        }
        if kind.is_dir() {
            let from = entry.path();
            let to = stage.join(entry.file_name());
            if to.exists() {
                std::fs::remove_dir_all(&to).map_err(|e| invalid(stage, e.to_string()))?;
            }
            copy_tree(&from, &to)?;
        }
    }
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), CalibrationTableWriteError> {
    std::fs::create_dir_all(target).map_err(|e| invalid(target, e.to_string()))?;
    for entry in std::fs::read_dir(source).map_err(|e| invalid(source, e.to_string()))? {
        let entry = entry.map_err(|e| invalid(source, e.to_string()))?;
        if entry.file_name() == "table.lock" {
            continue;
        }
        let kind = entry
            .file_type()
            .map_err(|e| invalid(source, e.to_string()))?;
        if kind.is_symlink() {
            return Err(invalid(
                source,
                "cannot safely copy a symbolic-link subtable; prior product is unchanged",
            ));
        }
        if kind.is_dir() {
            copy_tree(&entry.path(), &target.join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), target.join(entry.file_name()))
                .map_err(|e| invalid(target, e.to_string()))?;
        }
    }
    Ok(())
}

/// NewCalTable::mergeSpwMetaInfo: unused old windows may take the incoming
/// channelization; already populated windows must agree exactly.
pub(crate) fn merge_spw(
    old_root: &Path,
    stage: &Path,
) -> Result<Table, CalibrationTableWriteError> {
    let mut old = open(&old_root.join("SPECTRAL_WINDOW"))?;
    let new = open(&stage.join("SPECTRAL_WINDOW"))?;
    if old.row_count() != new.row_count() {
        return Err(invalid(old_root, "incompatible number of spectral windows"));
    }
    let identities = [
        "NAME",
        "MEAS_FREQ_REF",
        "NET_SIDEBAND",
        "IF_CONV_CHAIN",
        "FREQ_GROUP",
        "FREQ_GROUP_NAME",
    ];
    let spectra = [
        "NUM_CHAN",
        "CHAN_FREQ",
        "CHAN_WIDTH",
        "RESOLUTION",
        "EFFECTIVE_BW",
        "REF_FREQUENCY",
        "TOTAL_BANDWIDTH",
    ];
    for row in 0..new.row_count() {
        let incoming = &new.rows().map_err(|e| invalid(stage, e.to_string()))?[row];
        if incoming.get("FLAG_ROW") == Some(&Value::Scalar(ScalarValue::Bool(true))) {
            continue;
        }
        let prior = &old.rows().map_err(|e| invalid(old_root, e.to_string()))?[row];
        for column in identities {
            if prior.get(column) != incoming.get(column) {
                return Err(invalid(
                    old_root,
                    format!("incompatible SPW {row} {column}"),
                ));
            }
        }
        let unused = prior.get("FLAG_ROW") == Some(&Value::Scalar(ScalarValue::Bool(true)));
        for column in spectra {
            let value = incoming
                .get(column)
                .ok_or_else(|| invalid(stage, format!("missing SPW {column}")))?;
            if !unused && prior.get(column) != Some(value) {
                return Err(invalid(
                    old_root,
                    format!("incompatible SPW {row} {column}"),
                ));
            }
        }
        if unused {
            for column in spectra.into_iter().chain(["FLAG_ROW"]) {
                old.cell_accessor_mut(row, column)
                    .and_then(|mut c| {
                        c.set(incoming.get(column).expect("checked SPW column").clone())
                    })
                    .map_err(|e| invalid(old_root, e.to_string()))?;
            }
        }
    }
    Ok(old)
}
