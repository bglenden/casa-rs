// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for visibility data columns (DATA, CORRECTED_DATA,
//! MODEL_DATA, FLOAT_DATA).
//!
//! [`DataColumn`] provides read-only access; [`DataColumnMut`] adds
//! write support via [`put`](DataColumnMut::put).
//!
//! Cf. C++ `MSMainColumns::data()`, `correctedData()`, `modelData()`.

use casacore_tables::Table;
use casacore_types::ArrayValue;

use crate::error::{MsError, MsResult};
use crate::subtables::{get_array, has_column, set_array};

/// Typed accessor for a complex visibility column (DATA, CORRECTED_DATA, MODEL_DATA).
pub struct DataColumn<'a> {
    table: &'a Table,
    column: &'static str,
}

impl<'a> DataColumn<'a> {
    /// Create a DATA column accessor.
    pub fn data(table: &'a Table) -> MsResult<Self> {
        Self::new(table, "DATA")
    }

    /// Create a CORRECTED_DATA column accessor.
    pub fn corrected_data(table: &'a Table) -> MsResult<Self> {
        Self::new(table, "CORRECTED_DATA")
    }

    /// Create a MODEL_DATA column accessor.
    pub fn model_data(table: &'a Table) -> MsResult<Self> {
        Self::new(table, "MODEL_DATA")
    }

    fn new(table: &'a Table, column: &'static str) -> MsResult<Self> {
        if !has_column(table, column) {
            return Err(MsError::ColumnNotPresent(column.to_string()));
        }
        Ok(Self { table, column })
    }

    /// Read the visibility data for the given row as an [`ArrayValue`].
    ///
    /// Returns Complex32 array with shape `[num_corr, num_chan]`.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, self.column)
    }

    /// Return the shape of the data in the given row.
    pub fn shape(&self, row: usize) -> MsResult<Vec<usize>> {
        let arr = get_array(self.table, row, self.column)?;
        Ok(arr.shape().to_vec())
    }

    /// The column name.
    pub fn column_name(&self) -> &str {
        self.column
    }
}

/// Typed accessor for the FLOAT_DATA column (single-dish float data).
pub struct FloatDataColumn<'a> {
    table: &'a Table,
}

impl<'a> FloatDataColumn<'a> {
    /// Create a FLOAT_DATA column accessor.
    pub fn new(table: &'a Table) -> MsResult<Self> {
        if !has_column(table, "FLOAT_DATA") {
            return Err(MsError::ColumnNotPresent("FLOAT_DATA".to_string()));
        }
        Ok(Self { table })
    }

    /// Read the float data for the given row.
    ///
    /// Returns Float32 array with shape `[num_corr, num_chan]`.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "FLOAT_DATA")
    }

    /// Return the shape of the data in the given row.
    pub fn shape(&self, row: usize) -> MsResult<Vec<usize>> {
        let arr = get_array(self.table, row, "FLOAT_DATA")?;
        Ok(arr.shape().to_vec())
    }
}

/// Mutable accessor for a complex visibility column (DATA, CORRECTED_DATA, MODEL_DATA).
///
/// Provides write access to visibility data. Obtain via
/// [`MeasurementSet::data_column_mut`](crate::ms::MeasurementSet::data_column_mut).
///
/// Cf. C++ `MSMainColumns::data()` (writable).
pub struct DataColumnMut<'a> {
    table: &'a mut Table,
    column: &'static str,
}

impl<'a> DataColumnMut<'a> {
    /// Create a mutable DATA column accessor.
    pub fn data(table: &'a mut Table) -> MsResult<Self> {
        Self::new(table, "DATA")
    }

    /// Create a mutable CORRECTED_DATA column accessor.
    pub fn corrected_data(table: &'a mut Table) -> MsResult<Self> {
        Self::new(table, "CORRECTED_DATA")
    }

    /// Create a mutable MODEL_DATA column accessor.
    pub fn model_data(table: &'a mut Table) -> MsResult<Self> {
        Self::new(table, "MODEL_DATA")
    }

    fn new(table: &'a mut Table, column: &'static str) -> MsResult<Self> {
        if !has_column(table, column) {
            return Err(MsError::ColumnNotPresent(column.to_string()));
        }
        Ok(Self { table, column })
    }

    /// Read the visibility data for the given row.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, self.column)
    }

    /// Write visibility data for the given row.
    ///
    /// The `data` array should be Complex32 with shape `[num_corr, num_chan]`.
    pub fn put(&mut self, row: usize, data: ArrayValue) -> MsResult<()> {
        set_array(self.table, row, self.column, data)
    }

    /// Return the shape of the data in the given row.
    pub fn shape(&self, row: usize) -> MsResult<Vec<usize>> {
        let arr = get_array(self.table, row, self.column)?;
        Ok(arr.shape().to_vec())
    }

    /// The column name.
    pub fn column_name(&self) -> &str {
        self.column
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use crate::column_def::build_table_schema;
    use crate::schema;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;
    use num_complex::Complex32;

    #[test]
    fn data_column_not_present() {
        // Main table without DATA column should return ColumnNotPresent
        let schema =
            build_table_schema(schema::main_table::REQUIRED_COLUMNS).expect("valid schema");
        let table = Table::with_schema(schema);
        assert!(DataColumn::data(&table).is_err());
    }

    #[test]
    fn read_data_column() {
        let schemas = MeasurementSetBuilder::new()
            .with_main_column("DATA")
            .build_schemas()
            .unwrap();
        let mut table = Table::with_schema(schemas.main);

        // Create a row with DATA = Complex32[2, 4]
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2, 4],
                (0..8).map(|i| Complex32::new(i as f32, 0.0)).collect(),
            )
            .unwrap(),
        );

        // Build a full row
        let overrides = [("DATA", Value::Array(data))];
        let fields: Vec<RecordField> = table
            .schema()
            .unwrap()
            .columns()
            .iter()
            .map(|col| {
                if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                    return RecordField::new(col.name(), v.clone());
                }
                // Find the column def for default values
                let all_cols: Vec<_> = schema::main_table::REQUIRED_COLUMNS
                    .iter()
                    .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
                    .collect();
                if let Some(cdef) = all_cols.iter().find(|c| c.name == col.name()) {
                    use crate::column_def::ColumnKind;
                    let val = match cdef.column_kind {
                        ColumnKind::Scalar => match cdef.data_type {
                            casacore_types::PrimitiveType::Int32 => {
                                Value::Scalar(ScalarValue::Int32(0))
                            }
                            casacore_types::PrimitiveType::Float64 => {
                                Value::Scalar(ScalarValue::Float64(0.0))
                            }
                            casacore_types::PrimitiveType::Bool => {
                                Value::Scalar(ScalarValue::Bool(false))
                            }
                            _ => Value::Scalar(ScalarValue::Float64(0.0)),
                        },
                        ColumnKind::FixedArray { shape } => {
                            let total: usize = shape.iter().product();
                            Value::Array(ArrayValue::Float64(
                                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
                            ))
                        }
                        ColumnKind::VariableArray { ndim } => {
                            let shape: Vec<usize> = vec![1; ndim];
                            let total: usize = shape.iter().product();
                            match cdef.data_type {
                                casacore_types::PrimitiveType::Bool => {
                                    Value::Array(ArrayValue::Bool(
                                        ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                                    ))
                                }
                                casacore_types::PrimitiveType::Float32 => {
                                    Value::Array(ArrayValue::Float32(
                                        ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                                    ))
                                }
                                casacore_types::PrimitiveType::Complex32 => {
                                    Value::Array(ArrayValue::Complex32(
                                        ArrayD::from_shape_vec(
                                            shape,
                                            vec![Complex32::new(0.0, 0.0); total],
                                        )
                                        .unwrap(),
                                    ))
                                }
                                _ => Value::Array(ArrayValue::Float64(
                                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                                )),
                            }
                        }
                    };
                    RecordField::new(col.name(), val)
                } else {
                    RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(0)))
                }
            })
            .collect();
        table.add_row(RecordValue::new(fields)).unwrap();

        let col = DataColumn::data(&table).unwrap();
        assert_eq!(col.shape(0).unwrap(), vec![2, 4]);
    }

    #[test]
    fn write_data_column_mut() {
        let schemas = MeasurementSetBuilder::new()
            .with_main_column("DATA")
            .build_schemas()
            .unwrap();
        let mut table = Table::with_schema(schemas.main);

        // Create initial row with zeros
        let zeros = ArrayValue::Complex32(
            ArrayD::from_shape_vec(vec![2, 4], vec![Complex32::new(0.0, 0.0); 8]).unwrap(),
        );

        let overrides = [("DATA", Value::Array(zeros))];
        let fields: Vec<RecordField> = table
            .schema()
            .unwrap()
            .columns()
            .iter()
            .map(|col| {
                if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                    return RecordField::new(col.name(), v.clone());
                }
                let all_cols: Vec<_> = schema::main_table::REQUIRED_COLUMNS
                    .iter()
                    .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
                    .collect();
                if let Some(cdef) = all_cols.iter().find(|c| c.name == col.name()) {
                    use crate::column_def::ColumnKind;
                    let val = match cdef.column_kind {
                        ColumnKind::Scalar => match cdef.data_type {
                            casacore_types::PrimitiveType::Int32 => {
                                Value::Scalar(ScalarValue::Int32(0))
                            }
                            casacore_types::PrimitiveType::Float64 => {
                                Value::Scalar(ScalarValue::Float64(0.0))
                            }
                            casacore_types::PrimitiveType::Bool => {
                                Value::Scalar(ScalarValue::Bool(false))
                            }
                            _ => Value::Scalar(ScalarValue::Float64(0.0)),
                        },
                        ColumnKind::FixedArray { shape } => {
                            let total: usize = shape.iter().product();
                            Value::Array(ArrayValue::Float64(
                                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
                            ))
                        }
                        ColumnKind::VariableArray { ndim } => {
                            let shape: Vec<usize> = vec![1; ndim];
                            let total: usize = shape.iter().product();
                            match cdef.data_type {
                                casacore_types::PrimitiveType::Bool => {
                                    Value::Array(ArrayValue::Bool(
                                        ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                                    ))
                                }
                                casacore_types::PrimitiveType::Float32 => {
                                    Value::Array(ArrayValue::Float32(
                                        ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                                    ))
                                }
                                casacore_types::PrimitiveType::Complex32 => {
                                    Value::Array(ArrayValue::Complex32(
                                        ArrayD::from_shape_vec(
                                            shape,
                                            vec![Complex32::new(0.0, 0.0); total],
                                        )
                                        .unwrap(),
                                    ))
                                }
                                _ => Value::Array(ArrayValue::Float64(
                                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                                )),
                            }
                        }
                    };
                    RecordField::new(col.name(), val)
                } else {
                    RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(0)))
                }
            })
            .collect();
        table.add_row(RecordValue::new(fields)).unwrap();

        // Write new data via DataColumnMut
        let new_data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![2, 4],
                (0..8).map(|i| Complex32::new(i as f32, 1.0)).collect(),
            )
            .unwrap(),
        );

        {
            let mut col = DataColumnMut::data(&mut table).unwrap();
            col.put(0, new_data).unwrap();
            assert_eq!(col.shape(0).unwrap(), vec![2, 4]);
        }

        // Verify via read-only accessor
        let col = DataColumn::data(&table).unwrap();
        let arr = col.get(0).unwrap();
        match arr {
            ArrayValue::Complex32(a) => {
                assert_eq!(a[[0, 0]], Complex32::new(0.0, 1.0));
                assert_eq!(a[[1, 3]], Complex32::new(7.0, 1.0));
            }
            _ => panic!("Expected Complex32 array"),
        }
    }
}
