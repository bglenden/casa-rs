use super::*;

impl Table {
    // -------------------------------------------------------------------
    // Virtual column API
    // -------------------------------------------------------------------

    /// Returns `true` if the named column is a virtual column.
    ///
    /// Virtual columns are materialized from other data (e.g. forwarded
    /// from another table, or computed as `stored * scale + offset`). They
    /// behave like regular columns in memory, but their on-disk representation
    /// is through a virtual engine rather than a storage manager.
    ///
    /// # C++ equivalent
    ///
    /// `TableColumn::isVirtual()`.
    pub fn is_virtual_column(&self, name: &str) -> bool {
        self.virtual_columns.contains(name)
    }

    /// Bind a column as a `ForwardColumnEngine` column.
    ///
    /// The column `column` will read its values from the same-named column
    /// in the table at `ref_table`. On save, the column is backed by a
    /// `ForwardColumnEngine` DM entry; on reload, values are copied from
    /// the referenced table.
    ///
    /// The column must already exist in the schema. The referenced table
    /// must exist on disk at save time.
    ///
    /// # C++ equivalent
    ///
    /// `ForwardColumnEngine::addColumn(...)`.
    pub fn bind_forward_column(
        &mut self,
        column: &str,
        ref_table: &Path,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == column) {
                return Err(TableError::SchemaColumnUnknown {
                    column: column.to_string(),
                });
            }
        }
        self.virtual_columns.insert(column.to_string());
        self.virtual_bindings.push(VirtualColumnBinding::Forward {
            col_name: column.to_string(),
            ref_table: ref_table.to_path_buf(),
        });
        Ok(())
    }

    /// Bind a column as a `ScaledArrayEngine` column.
    ///
    /// The column `virtual_col` computes `stored_col * scale + offset`.
    /// Both columns must exist in the schema. The stored column holds
    /// integer or float data; the virtual column exposes Float64 values.
    ///
    /// # C++ equivalent
    ///
    /// `ScaledArrayEngine<Double,Int>(virtualCol, storedCol, scale, offset)`.
    pub fn bind_scaled_array_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: f64,
        offset: f64,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::ScaledArray {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
            });
        Ok(())
    }

    /// Bind a column as a `ScaledComplexData` column.
    ///
    /// The stored column holds integer data with a prepended dimension of 2
    /// for real/imaginary parts. The virtual column exposes Complex32 or
    /// Complex64 values computed as:
    /// - `re_virtual = re_stored * scale.re + offset.re`
    /// - `im_virtual = im_stored * scale.im + offset.im`
    ///
    /// Both columns must exist in the schema.
    ///
    /// # C++ equivalent
    ///
    /// `ScaledComplexData<Complex,Short>(virtualCol, storedCol, scale, offset)`.
    pub fn bind_scaled_complex_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: Complex64,
        offset: Complex64,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::ScaledComplexData {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
            });
        Ok(())
    }

    /// Bind a column as a `BitFlagsEngine` column.
    ///
    /// The column `virtual_col` produces `(stored_col & read_mask) != 0`.
    /// Both columns must exist in the schema. The stored column holds
    /// integer data; the virtual column exposes Bool values.
    ///
    /// # C++ equivalent
    ///
    /// `BitFlagsEngine<uChar>(virtualCol, storedCol)`.
    pub fn bind_bitflags_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        read_mask: u32,
        write_mask: u32,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings.push(VirtualColumnBinding::BitFlags {
            virtual_col: virtual_col.to_string(),
            stored_col: stored_col.to_string(),
            read_mask,
            write_mask,
        });
        Ok(())
    }

    /// Bind a column as a `CompressFloat` column.
    ///
    /// The column `virtual_col` decompresses stored Int16 data from
    /// `stored_col` using FITS-style linear scaling:
    /// `virtual[i] = (stored == -32768) ? NaN : stored * scale + offset`.
    ///
    /// # C++ equivalent
    ///
    /// `CompressFloat(virtualCol, storedCol, scale, offset)`.
    pub fn bind_compress_float_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: f32,
        offset: f32,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::CompressFloat {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
            });
        Ok(())
    }

    /// Bind a column as a `CompressComplex` or `CompressComplexSD` column.
    ///
    /// The column `virtual_col` decompresses stored Int32 data from
    /// `stored_col` into complex values.
    ///
    /// # C++ equivalent
    ///
    /// `CompressComplex` / `CompressComplexSD`.
    pub fn bind_compress_complex_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: f32,
        offset: f32,
        single_dish: bool,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::CompressComplex {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
                single_dish,
            });
        Ok(())
    }

    /// Bind a column as a `ForwardColumnIndexedRowEngine` column.
    ///
    /// Like `ForwardColumnEngine` but remaps rows via an index column.
    /// For row `r`, reads `idx = row_map_col[r]`, then reads the
    /// forwarded column at row `idx` in the referenced table.
    ///
    /// # C++ equivalent
    ///
    /// `ForwardColumnIndexedRowEngine`.
    pub fn bind_forward_column_indexed(
        &mut self,
        column: &str,
        ref_table: &Path,
        row_column: &str,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == column) {
                return Err(TableError::SchemaColumnUnknown {
                    column: column.to_string(),
                });
            }
        }
        self.virtual_columns.insert(column.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::ForwardIndexedRow {
                col_name: column.to_string(),
                ref_table: ref_table.to_path_buf(),
                row_column: row_column.to_string(),
            });
        Ok(())
    }

    /// Bind a virtual column computed from a TaQL expression.
    ///
    /// The expression is evaluated per-row against stored columns during
    /// materialization. The column must already exist in the schema.
    ///
    /// # C++ equivalent
    ///
    /// `VirtualTaQLColumn` in `casacore/tables/DataMan/VirtualTaQLColumn.h`.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::SchemaColumnUnknown`] if the column is not in
    /// the schema.
    pub fn bind_taql_column(&mut self, column: &str, expression: &str) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == column) {
                return Err(TableError::SchemaColumnUnknown {
                    column: column.to_string(),
                });
            }
        }
        self.virtual_columns.insert(column.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::TaQLColumn {
                col_name: column.to_string(),
                expression: expression.to_string(),
            });
        Ok(())
    }
}
