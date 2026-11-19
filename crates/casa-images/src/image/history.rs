// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::{ScalarValue, Value};

use super::{ImageError, ImageHistoryEntry, ImageHistoryRow, LOGTABLE_KEYWORD, LOGTABLE_NAME};

/// Read the persistent image log for replay by `imhead(mode="history")`.
pub fn image_history_entries(path: impl AsRef<Path>) -> Result<Vec<ImageHistoryEntry>, ImageError> {
    let image = path.as_ref();
    if image.is_file() {
        return crate::fits_read::history_entries(image);
    }
    let Some(table) = open_log(image)? else {
        return Ok(Vec::new());
    };
    (0..table.row_count())
        .map(|row| entry(&table, row))
        .collect()
}

/// Read the native image log with its priority and object identifier columns.
/// Used when a new native image inherits the source image's history.
///
/// # Errors
/// Returns an error for an unreadable log table or malformed log rows.
pub fn image_history_rows(path: &Path) -> Result<Vec<ImageHistoryRow>, ImageError> {
    let Some(table) = open_log(path)? else {
        return Ok(Vec::new());
    };
    (0..table.row_count())
        .map(|row| {
            Ok(ImageHistoryRow {
                entry: entry(&table, row)?,
                priority: text(&table, row, "PRIORITY")?,
                object_id: text(&table, row, "OBJECT_ID")?,
            })
        })
        .collect()
}

fn open_log(image: &Path) -> Result<Option<Table>, ImageError> {
    let log = image.join(LOGTABLE_NAME);
    if log.exists() {
        return Ok(Some(Table::open(TableOptions::new(log))?));
    }
    let table = Table::open(TableOptions::new(image))?;
    if table.keywords().get(LOGTABLE_KEYWORD).is_some() {
        return Err(ImageError::InvalidMetadata(
            "image history table is missing".into(),
        ));
    }
    Ok(None)
}

fn entry(table: &Table, row: usize) -> Result<ImageHistoryEntry, ImageError> {
    let time = table.cell_accessor(row, "TIME")?.value()?;
    let Some(Value::Scalar(ScalarValue::Float64(time))) = time else {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid history entry at row {row}"
        )));
    };
    Ok(ImageHistoryEntry {
        time_mjd_seconds: *time,
        location: text(table, row, "LOCATION")?,
        message: text(table, row, "MESSAGE")?,
    })
}

fn text(table: &Table, row: usize, column: &str) -> Result<String, ImageError> {
    match table.cell_accessor(row, column)?.value()? {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value.clone()),
        _ => Err(ImageError::InvalidMetadata(format!(
            "invalid history entry at row {row}"
        ))),
    }
}
