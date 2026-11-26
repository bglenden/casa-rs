// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::OnceLock;

use super::TableImpl;

impl TableImpl {
    pub(crate) fn try_clear_read_cells(&mut self) -> bool {
        if self.generation() != 0 || self.lazy_rows.is_none() || self.loaded_rows.get().is_some() {
            return false;
        }
        for cells in self.loaded_scalar_columns.values_mut() {
            *cells = OnceLock::new();
        }
        for cells in self.loaded_array_columns.values_mut() {
            *cells = OnceLock::new();
        }
        for cells in self.buffered_array_cells.values_mut() {
            *cells = OnceLock::new();
        }
        true
    }
}
