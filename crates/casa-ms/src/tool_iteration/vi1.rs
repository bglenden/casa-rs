// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's deprecated VisibilityIterator chunk cursor.

use casa_tables::{PlainReferenceRead, Table};

use super::ToolIteration;
use crate::error::{MsError, MsResult};

/// The first visibility buffer of each major chunk of a captured MAIN view.
pub struct Vi1ToolIteration {
    cursor: ToolIteration,
}

impl Vi1ToolIteration {
    /// Build a TIME-ordered iterator and position it on the first chunk.
    ///
    /// Default sorting also groups by array, field and data description.
    /// With positive row blocking, VI1's inclusive bound returns up to
    /// `maxrows + 1` rows from each chunk's first buffer.
    ///
    /// # Errors
    /// Returns an error if the source has no rows or a required sort key
    /// cannot be read.
    pub fn new(
        main: &Table,
        reference: Option<&PlainReferenceRead>,
        selected: Option<&[usize]>,
        interval: f64,
        maxrows: i64,
        defaults: bool,
    ) -> MsResult<Self> {
        let block = if maxrows > 0 {
            i64::from(maxrows as i32)
        } else if interval <= 0.0 {
            i64::from(selected.map_or_else(|| main.row_count(), <[usize]>::len) as i32)
        } else {
            0
        };
        // ToolIteration uses 1 to select one integration per buffer.
        let first_buffer_bound = block.max(0) + 1;
        let mut cursor = ToolIteration::new(
            main,
            reference,
            selected,
            &["TIME".into()],
            interval,
            first_buffer_bound,
            defaults,
        )?
        .ok_or_else(|| MsError::InvalidInput("TIME must be a known MS sort key".into()))?;
        cursor.rewind();
        Ok(Self { cursor })
    }

    /// Return to the first chunk, including after exhaustion.
    pub fn rewind(&mut self) {
        self.cursor.rewind();
    }

    /// The iterator has passed its last major chunk.
    pub fn exhausted(&self) -> bool {
        self.cursor.exhausted
    }

    /// Advance to the following major chunk. The first call at the last
    /// chunk reports success and leaves its first buffer available to read.
    pub fn advance(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        let current = self.cursor.position.expect("VI1 is positioned on creation");
        let end = self.cursor.ends[current];
        let groups = self
            .cursor
            .outer_ends
            .as_ref()
            .expect("VI1 has row buffers");
        let group_end = groups[groups.partition_point(|&group| group < end)];
        let next = self.cursor.ends.partition_point(|&end| end <= group_end);
        if next == self.cursor.ends.len() {
            self.cursor.exhausted = true;
        } else {
            self.cursor.position = Some(next);
        }
        true
    }

    /// Logical MAIN row indices in the current first buffer.
    pub fn rows(&self) -> &[usize] {
        self.cursor.rows().expect("VI1 is positioned on creation")
    }
}
