// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;

use crate::ImageError;
use crate::image::{ImageHistoryEntry, ImageHistoryRow, ImagePixel, PagedImage};

use super::TempImage;

impl<T: ImagePixel> TempImage<T> {
    fn history_directory(&self) -> Option<&std::path::Path> {
        self.read_only.as_ref()?.source.history_directory()
    }

    /// Returns the history entries.
    pub fn history(&self) -> Result<Vec<String>, ImageError> {
        Ok(self
            .history_rows()?
            .iter()
            .map(|row| row.entry.message.clone())
            .collect())
    }

    /// Appends a history message.
    pub fn add_history(&mut self, msg: impl Into<String>) -> Result<(), ImageError> {
        self.append_history_at("casa-images", &[msg.into()])
    }

    /// Appends history messages with a location retained by future saves.
    ///
    /// # Errors
    /// Returns an error if the system clock is earlier than the UNIX epoch.
    pub fn append_history_at(
        &mut self,
        location: &str,
        messages: &[String],
    ) -> Result<(), ImageError> {
        if let Some(path) = self.history_directory() {
            return PagedImage::<f32>::append_logtable_rows(
                &path.join("logtable"),
                location,
                messages,
            );
        }
        let time = ImageHistoryEntry::current_time()?;
        self.history.extend(messages.iter().map(|message| {
            ImageHistoryRow::informational(ImageHistoryEntry {
                priority: "INFO".into(),
                time_mjd_seconds: time,
                location: location.into(),
                message: message.clone(),
            })
        }));
        Ok(())
    }

    /// Append the original FITS or CASA log entries for future saves.
    pub fn append_history_entries(
        &mut self,
        entries: &[ImageHistoryRow],
    ) -> Result<(), ImageError> {
        if let Some(path) = self.history_directory() {
            return PagedImage::<f32>::append_logtable_entries(
                &path.join("logtable"),
                entries.iter().map(|row| {
                    (
                        row.entry.time_mjd_seconds,
                        row.entry.location.as_str(),
                        row.entry.message.as_str(),
                        row.entry.priority.as_str(),
                        row.object_id.as_str(),
                    )
                }),
            );
        }
        self.history.extend_from_slice(entries);
        Ok(())
    }

    /// Returns the image's original log rows for derived images.
    ///
    /// # Errors
    /// Returns an error if the native component image's history is unreadable.
    pub fn history_rows(&self) -> Result<Cow<'_, [ImageHistoryRow]>, ImageError> {
        if let Some(path) = self.history_directory() {
            return crate::image::image_history_rows(path).map(Cow::Owned);
        }
        Ok(Cow::Borrowed(&self.history))
    }

    /// Clears the history log.
    pub fn clear_history(&mut self) -> Result<(), ImageError> {
        self.history.clear();
        Ok(())
    }
}
