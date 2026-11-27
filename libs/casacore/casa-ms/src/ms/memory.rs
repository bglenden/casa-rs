// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent in-memory MeasurementSets and persistence of their selections.

use casa_tables::EndianFormat;

use super::*;

impl MeasurementSet {
    /// Create an in-memory MeasurementSet from a builder.
    /// The tables have the declared schemas and no rows, and MAIN has its
    /// `MS_VERSION` keyword.
    pub fn create_memory(builder: MeasurementSetBuilder) -> MsResult<Self> {
        let schemas = builder.build_schemas()?;
        Self::from_schemas(schemas, None)
    }

    /// Detach the selected MAIN rows and this MeasurementSet's subtables
    /// from their storage. Main rows retain the given order and duplicates.
    /// # Errors
    /// A selected row or subtable cannot be read from the source.
    pub fn to_memory_rows(&self, rows: &[usize]) -> MsResult<Self> {
        let main = self.main.to_memory_rows(rows)?;
        let subtables = self
            .subtables
            .iter()
            .map(|(&id, table)| table.to_memory().map(|copy| (id, copy)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self {
            main,
            subtables,
            subtable_paths: HashMap::new(),
            path: None,
        })
    }

    /// Persist a separate MeasurementSet using the requested byte order for
    /// its MAIN table and default byte order for its subtables.
    /// # Errors
    /// The destination or a table in the result cannot be written.
    pub fn save_as_with_main_endian(&mut self, path: &Path, endian: EndianFormat) -> MsResult<()> {
        self.save_as_using(path, |main, path| {
            let options = measurement_set_table_options(path).with_endian_format(endian);
            let bindings = measurement_set_main_table_bindings(main);
            main.prepare_write()
                .save_with_bindings(options, &bindings)?;
            Ok(())
        })
    }
}
