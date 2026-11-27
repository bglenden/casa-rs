// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Read the image log including CASA priority, timestamp, and location.
pub fn image_history_entries(path: impl AsRef<Path>) -> Result<Vec<ImageHistoryEntry>, ImageError> {
    Ok(image_history_rows(path)?
        .into_iter()
        .map(|row| row.entry)
        .collect())
}

/// Reads the original log rows including their object identifiers.
pub fn image_history_rows(path: impl AsRef<Path>) -> Result<Vec<ImageHistoryRow>, ImageError> {
    let image = path.as_ref();
    if image.is_file() {
        return crate::fits_read::history_rows(image);
    }
    let logtable_path = image.join(LOGTABLE_NAME);
    if !logtable_path.exists() {
        let table = Table::open(TableOptions::new(image))?;
        if table.keywords().get(LOGTABLE_KEYWORD).is_some() {
            return Err(ImageError::InvalidMetadata(
                "image history table is missing".into(),
            ));
        }
        return Ok(Vec::new());
    }
    let table = Table::open(TableOptions::new(logtable_path))?;
    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        let time = table.cell_accessor(row, "TIME")?.value()?;
        let priority = table
            .cell_accessor(row, "PRIORITY")
            .ok()
            .and_then(|cell| cell.value().ok().flatten())
            .and_then(|value| match value {
                Value::Scalar(ScalarValue::String(priority)) if !priority.trim().is_empty() => {
                    Some(priority.trim().to_string())
                }
                _ => None,
            })
            .unwrap_or_else(|| "INFO".to_string());
        let location = table.cell_accessor(row, "LOCATION")?.value()?;
        let message = table.cell_accessor(row, "MESSAGE")?.value()?;
        let object_id = match table
            .cell_accessor(row, "OBJECT_ID")
            .ok()
            .and_then(|cell| cell.value().ok().flatten())
        {
            Some(Value::Scalar(ScalarValue::String(value))) => value.clone(),
            _ => String::new(),
        };
        match (time, location, message) {
            (
                Some(Value::Scalar(ScalarValue::Float64(time))),
                Some(Value::Scalar(ScalarValue::String(location))),
                Some(Value::Scalar(ScalarValue::String(message))),
            ) => entries.push(ImageHistoryRow {
                entry: ImageHistoryEntry {
                    time_mjd_seconds: *time,
                    priority,
                    location: location.clone(),
                    message: message.clone(),
                },
                object_id,
            }),
            _ => {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid history entry at row {row}"
                )));
            }
        }
    }
    Ok(entries)
}

/// Appends `messages` from `origin` to the `logtable` of the image at `image`,
/// whatever its pixel type, as `image.sethistory` does: its `LogOrigin` gives
/// each the location `::origin`.
///
/// # Errors
///
/// When the `logtable` can't be opened or saved.
pub fn add_image_history(
    image: &Path,
    origin: &str,
    messages: &[String],
) -> Result<(), ImageError> {
    PagedImage::<f32>::append_logtable_rows(
        &image.join(LOGTABLE_NAME),
        &format!("::{origin}"),
        messages,
    )
}

/// Appends full [`ImageHistoryEntry`] rows to the `logtable` of `image` in
/// a single table write, preserving timestamps, priorities, and locations.
///
/// # Errors
///
/// When the `logtable` cannot be opened or saved.
pub fn append_image_history_entries(
    image: &Path,
    entries: &[ImageHistoryEntry],
) -> Result<(), ImageError> {
    PagedImage::<f32>::append_logtable_entries(
        &image.join(LOGTABLE_NAME),
        entries.iter().map(|entry| {
            (
                entry.time_mjd_seconds,
                entry.location.as_str(),
                entry.message.as_str(),
                entry.priority.as_str(),
                "",
            )
        }),
    )
}

impl<T: ImagePixel> PagedImage<T> {
    /// Returns the history entries associated with the image.
    pub fn history(&self) -> Result<Vec<String>, ImageError> {
        match &self.path {
            Some(path) => Self::read_logtable(&Self::logtable_path(path)),
            None => Ok(self.temp_history.clone()),
        }
    }

    /// Appends a history message.
    pub fn add_history(&mut self, msg: impl Into<String>) -> Result<(), ImageError> {
        let msg = msg.into();
        match self.path.clone() {
            Some(path) => {
                self.ensure_logtable()?;
                Self::append_logtable_row(&Self::logtable_path(&path), &msg)?;
                Ok(())
            }
            None => {
                self.temp_history.push(msg);
                Ok(())
            }
        }
    }

    /// Appends a group of history messages with a casacore `LOCATION` value.
    ///
    /// # Errors
    /// Returns an error if the log table cannot be created, written or locked.
    pub fn append_history_at(
        &mut self,
        location: &str,
        messages: &[String],
    ) -> Result<(), ImageError> {
        if messages.is_empty() {
            return Ok(());
        }
        if let Some(path) = self.path.clone() {
            self.ensure_logtable()?;
            Self::append_logtable_rows(&Self::logtable_path(&path), location, messages)
        } else {
            self.temp_history.extend_from_slice(messages);
            Ok(())
        }
    }

    /// Saves image metadata with history while holding the native log write lock.
    /// With `info == None` the operation appends history only.
    ///
    /// # Errors
    /// Returns an error if the record is invalid or if the image or its log
    /// cannot be read, locked or written.
    pub fn save_image_info_with_history(
        &mut self,
        info: Option<&RecordValue>,
        location: &str,
        messages: &[String],
    ) -> Result<(), ImageError> {
        let path = Self::logtable_path(self.path.as_ref().ok_or(ImageError::NotPersistent)?);
        self.ensure_logtable()?;
        let mut lock = TableWriteLock::acquire_to_reread(&path, 0)?;
        let mut log = Table::open(TableOptions::new(&path))?;
        let time = ImageHistoryEntry::current_time()?;
        Self::add_logtable_entries(
            &mut log,
            messages
                .iter()
                .map(|message| (time, location, message.as_str(), "INFO", "")),
        )?;
        let previous = self.table.keywords().get("imageinfo").cloned();
        if let Some(info) = info {
            self.set_image_info_record(info.clone())?;
        }
        let publish = self
            .save()
            .and_then(|()| log.save(TableOptions::new(&path)).map_err(ImageError::from));
        if let Err(error) = publish {
            if info.is_some() {
                let keywords = self.table.keywords_mut();
                if let Some(previous) = previous {
                    keywords.upsert("imageinfo", previous);
                } else {
                    keywords.remove("imageinfo");
                }
                if let Err(restore) = self.save() {
                    return Err(ImageError::Io(format!(
                        "{error}; restoring the previous image metadata also failed: {restore}"
                    )));
                }
            }
            return Err(error);
        }
        lock.record_write();
        lock.release()?;
        Ok(())
    }

    /// Append imported history with its original location and MJD time.
    ///
    /// # Errors
    /// Returns an error if the image log cannot be created, written or locked.
    pub fn append_history_entries(
        &mut self,
        entries: &[ImageHistoryRow],
    ) -> Result<(), ImageError> {
        if entries.is_empty() {
            return Ok(());
        }
        if let Some(path) = self.path.clone() {
            self.ensure_logtable()?;
            Self::append_logtable_entries(
                &Self::logtable_path(&path),
                entries.iter().map(|entry| {
                    (
                        entry.entry.time_mjd_seconds,
                        entry.entry.location.as_str(),
                        entry.entry.message.as_str(),
                        entry.entry.priority.as_str(),
                        entry.object_id.as_str(),
                    )
                }),
            )
        } else {
            self.temp_history
                .extend(entries.iter().map(|row| row.entry.message.clone()));
            Ok(())
        }
    }

    /// Clears the history log.
    pub fn clear_history(&mut self) -> Result<(), ImageError> {
        match self.path.clone() {
            Some(path) => {
                self.ensure_logtable()?;
                Self::reset_logtable(&Self::logtable_path(&path))?;
                Ok(())
            }
            None => {
                self.temp_history.clear();
                Ok(())
            }
        }
    }
}

impl<T: ImagePixel> PagedImage<T> {
    pub(super) fn ensure_logtable(&mut self) -> Result<(), ImageError> {
        if let Some(path) = &self.path {
            self.table.keywords_mut().upsert(
                LOGTABLE_KEYWORD,
                Value::TableRef(subtable_reference(LOGTABLE_NAME)),
            );
            let logtable_path = Self::logtable_path(path);
            if !logtable_path.exists() {
                Self::reset_logtable(&logtable_path)?;
            }
        }
        Ok(())
    }

    pub(super) fn logtable_path(path: &Path) -> PathBuf {
        path.join(LOGTABLE_NAME)
    }

    pub fn reset_logtable(path: &Path) -> Result<(), ImageError> {
        if path.exists() {
            std::fs::remove_dir_all(path).map_err(|e| ImageError::Io(e.to_string()))?;
        }
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("TIME", PrimitiveType::Float64),
            ColumnSchema::scalar("PRIORITY", PrimitiveType::String),
            ColumnSchema::scalar("MESSAGE", PrimitiveType::String),
            ColumnSchema::scalar("LOCATION", PrimitiveType::String),
            ColumnSchema::scalar("OBJECT_ID", PrimitiveType::String),
        ])
        .map_err(|e| ImageError::Table(e.to_string()))?;
        let table = Table::with_schema(schema);
        table.save(TableOptions::new(path))?;
        Ok(())
    }

    pub(super) fn append_logtable_row(path: &Path, message: &str) -> Result<(), ImageError> {
        Self::append_logtable_rows(
            path,
            "casa-images",
            std::slice::from_ref(&message.to_string()),
        )
    }

    /// Appends `messages` in one save, each at `location`, with `TIME` in
    /// MJD seconds, as casacore's `TableLogSink` writes it.
    pub(crate) fn append_logtable_rows(
        path: &Path,
        location: &str,
        messages: &[String],
    ) -> Result<(), ImageError> {
        let time = ImageHistoryEntry::current_time()?;
        Self::append_logtable_entries(
            path,
            messages
                .iter()
                .map(|message| (time, location, message.as_str(), "INFO", "")),
        )
    }

    pub(crate) fn append_logtable_entries<'a>(
        path: &Path,
        entries: impl IntoIterator<Item = (f64, &'a str, &'a str, &'a str, &'a str)>,
    ) -> Result<(), ImageError> {
        let mut entries = entries.into_iter().peekable();
        if entries.peek().is_none() {
            return Ok(());
        }
        // casacore takes a reopened table's row count from the sync data in
        // `table.lock`, which this lock publishes: a save alone leaves a
        // `logtable` CASA wrote unreadable to it.
        let mut lock = TableWriteLock::acquire(path, 1)?;
        let mut table = Table::open(TableOptions::new(path))?;
        Self::add_logtable_entries(&mut table, entries)?;
        table.save(TableOptions::new(path))?;
        lock.record_write();
        lock.release()?;
        Ok(())
    }

    pub(super) fn add_logtable_entries<'a>(
        table: &mut Table,
        entries: impl IntoIterator<Item = (f64, &'a str, &'a str, &'a str, &'a str)>,
    ) -> Result<(), ImageError> {
        for (time, location, message, priority, object_id) in entries {
            table.add_row(RecordValue::new(vec![
                RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                RecordField::new(
                    "PRIORITY",
                    Value::Scalar(ScalarValue::String(priority.into())),
                ),
                RecordField::new(
                    "MESSAGE",
                    Value::Scalar(ScalarValue::String(message.into())),
                ),
                RecordField::new(
                    "LOCATION",
                    Value::Scalar(ScalarValue::String(location.into())),
                ),
                RecordField::new(
                    "OBJECT_ID",
                    Value::Scalar(ScalarValue::String(object_id.into())),
                ),
            ]))?;
        }
        Ok(())
    }

    pub(super) fn read_logtable(path: &Path) -> Result<Vec<String>, ImageError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let table = Table::open(TableOptions::new(path))?;
        let mut messages = Vec::new();
        for row in 0..table.row_count() {
            if let Ok(Some(Value::Scalar(ScalarValue::String(message)))) = table
                .cell_accessor(row, "MESSAGE")
                .and_then(|cell| cell.value())
            {
                messages.push(message.clone());
            }
        }
        Ok(messages)
    }

    pub(super) fn write_history_to_path(&mut self, path: &Path) -> Result<(), ImageError> {
        let logtable_path = Self::logtable_path(path);
        Self::reset_logtable(&logtable_path)?;
        for entry in self.temp_history.drain(..) {
            Self::append_logtable_row(&logtable_path, &entry)?;
        }
        Ok(())
    }
}
