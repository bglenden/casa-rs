// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_types::PrimitiveType;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    #[error("schema contains duplicate column name \"{0}\"")]
    DuplicateColumn(String),
    #[error("column \"{0}\" uses Direct but is not FixedShape array")]
    DirectRequiresFixedShape(String),
    #[error("column \"{0}\" uses Undefined but is not scalar")]
    UndefinedOnlyForScalar(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColumnOptions {
    pub direct: bool,
    pub undefined: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayShapeContract {
    Fixed { shape: Vec<usize> },
    Variable { ndim: Option<usize> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Scalar,
    Array(ArrayShapeContract),
    Record,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    name: String,
    column_type: ColumnType,
    data_type: Option<PrimitiveType>,
    options: ColumnOptions,
}

impl ColumnSchema {
    pub fn scalar(name: impl Into<String>, data_type: PrimitiveType) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Scalar,
            data_type: Some(data_type),
            options: ColumnOptions::default(),
        }
    }

    pub fn record(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Record,
            data_type: None,
            options: ColumnOptions::default(),
        }
    }

    pub fn array_fixed(
        name: impl Into<String>,
        data_type: PrimitiveType,
        shape: Vec<usize>,
    ) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Array(ArrayShapeContract::Fixed { shape }),
            data_type: Some(data_type),
            options: ColumnOptions::default(),
        }
    }

    pub fn array_variable(
        name: impl Into<String>,
        data_type: PrimitiveType,
        ndim: Option<usize>,
    ) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Array(ArrayShapeContract::Variable { ndim }),
            data_type: Some(data_type),
            options: ColumnOptions::default(),
        }
    }

    pub fn with_options(mut self, options: ColumnOptions) -> Result<Self, SchemaError> {
        self.options = options;
        self.validate_options()?;
        Ok(self)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    pub fn data_type(&self) -> Option<PrimitiveType> {
        self.data_type
    }

    pub fn options(&self) -> ColumnOptions {
        self.options
    }

    fn validate_options(&self) -> Result<(), SchemaError> {
        if self.options.direct
            && !matches!(
                self.column_type,
                ColumnType::Array(ArrayShapeContract::Fixed { .. })
            )
        {
            return Err(SchemaError::DirectRequiresFixedShape(self.name.clone()));
        }
        if self.options.undefined && !matches!(self.column_type, ColumnType::Scalar) {
            return Err(SchemaError::UndefinedOnlyForScalar(self.name.clone()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableSchema {
    columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(columns: Vec<ColumnSchema>) -> Result<Self, SchemaError> {
        for i in 0..columns.len() {
            columns[i].validate_options()?;
            if columns[(i + 1)..]
                .iter()
                .any(|other| other.name == columns[i].name)
            {
                return Err(SchemaError::DuplicateColumn(columns[i].name.clone()));
            }
        }
        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[ColumnSchema] {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn contains_column(&self, name: &str) -> bool {
        self.column(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use casacore_types::PrimitiveType;

    use super::{ColumnOptions, ColumnSchema, SchemaError, TableSchema};

    #[test]
    fn schema_rejects_duplicate_columns() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("id", PrimitiveType::Int32),
        ]);
        assert_eq!(schema, Err(SchemaError::DuplicateColumn("id".to_string())));
    }

    #[test]
    fn direct_requires_fixed_array_shape() {
        let column = ColumnSchema::array_variable("data", PrimitiveType::Float32, Some(2))
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            });
        assert_eq!(
            column,
            Err(SchemaError::DirectRequiresFixedShape("data".to_string()))
        );
    }

    #[test]
    fn undefined_only_applies_to_scalar_columns() {
        let column = ColumnSchema::record("meta").with_options(ColumnOptions {
            direct: false,
            undefined: true,
        });
        assert_eq!(
            column,
            Err(SchemaError::UndefinedOnlyForScalar("meta".to_string()))
        );
    }
}
