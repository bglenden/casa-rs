// SPDX-License-Identifier: LGPL-3.0-or-later
use casacore_types::PrimitiveType;
use thiserror::Error;

/// Errors produced during schema construction or column option validation.
///
/// These are returned by [`TableSchema::new`] and [`ColumnSchema::with_options`]
/// when the supplied configuration violates a schema invariant. They correspond
/// to the exceptions that C++ `TableDesc::addColumn` and `ColumnDesc` constructors
/// would throw at runtime.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// A column name appears more than once in the schema.
    ///
    /// Cf. C++ `TableDesc::addColumn`, which throws `TableError` on duplicates.
    #[error("schema contains duplicate column name \"{0}\"")]
    DuplicateColumn(String),

    /// The `direct` option was set on a column that is not a [`ArrayShapeContract::Fixed`] array.
    ///
    /// Direct storage requires the cell size to be constant across all rows, which
    /// is only possible when the array shape is fully specified at schema time.
    /// Cf. C++ `ColumnDesc::Direct`, which is only valid alongside a fixed `IPosition`.
    #[error("column \"{0}\" uses Direct but is not FixedShape array")]
    DirectRequiresFixedShape(String),

    /// The `undefined` option was set on a non-scalar column.
    ///
    /// Cf. C++ `ColumnDesc::Undefined`, which is only meaningful for scalar columns.
    #[error("column \"{0}\" uses Undefined but is not scalar")]
    UndefinedOnlyForScalar(String),
}

/// Storage and behaviour flags for a single column.
///
/// These correspond to the bit flags carried by C++ `ColumnDesc` (specifically
/// `ColumnDesc::Direct` and `ColumnDesc::Undefined`). Both default to `false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColumnOptions {
    /// Store cell data inline (directly) within the table file rather than
    /// in a separate storage manager file.
    ///
    /// Only valid for [`ArrayShapeContract::Fixed`] array columns; setting this
    /// on any other kind of column returns [`SchemaError::DirectRequiresFixedShape`].
    /// Cf. C++ `ColumnDesc::Direct`.
    pub direct: bool,

    /// Allow cells in this column to be left in an undefined (empty) state.
    ///
    /// Only valid for [`ColumnType::Scalar`] columns; setting this on an array
    /// or record column returns [`SchemaError::UndefinedOnlyForScalar`].
    /// Cf. C++ `ColumnDesc::Undefined`.
    pub undefined: bool,
}

/// Describes the shape contract of an array column.
///
/// This type is the payload of [`ColumnType::Array`] and distinguishes between
/// columns where every row has the same shape and those where the shape may vary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayShapeContract {
    /// Every row stores an array with exactly this shape.
    ///
    /// Corresponds to `ArrayColumnDesc<T>` constructed with a fixed `IPosition`
    /// and the `ColumnDesc::Direct` flag in C++. The shape is recorded in the
    /// schema and enforced on every write.
    Fixed { shape: Vec<usize> },

    /// Each row may store an array with a different shape.
    ///
    /// Corresponds to C++ indirect array columns (`ArrayColumnDesc<T>` without a
    /// fixed shape). `ndim`, when `Some`, constrains the number of dimensions but
    /// not the extent along each axis; when `None` the dimensionality is also
    /// unconstrained.
    Variable { ndim: Option<usize> },
}

/// The kind of data a column holds.
///
/// Every [`ColumnSchema`] carries one of these three variants, mirroring the
/// three column categories recognised by C++ casacore: scalar (`ScalarColumnDesc`),
/// array (`ArrayColumnDesc`), and record (`RecordColumnDesc` / `TpRecord`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    /// A single primitive value per row.
    Scalar,

    /// A multi-dimensional array per row, with the given [`ArrayShapeContract`].
    Array(ArrayShapeContract),

    /// A nested keyword/value record per row.
    ///
    /// Record columns carry no numeric [`PrimitiveType`]; their data type is
    /// implicitly `TpRecord` in C++ terms.
    Record,
}

/// Describes a single column within a [`TableSchema`].
///
/// Corresponds to C++ `ColumnDesc`. A `ColumnSchema` captures the column name,
/// its [`ColumnType`] (scalar, array, or record), the primitive [`PrimitiveType`]
/// of the stored elements (absent for record columns), and any additional
/// [`ColumnOptions`] flags.
///
/// Use the factory methods [`ColumnSchema::scalar`], [`ColumnSchema::record`],
/// [`ColumnSchema::array_fixed`], and [`ColumnSchema::array_variable`] to
/// construct instances, then optionally call [`ColumnSchema::with_options`] to
/// attach non-default flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    name: String,
    column_type: ColumnType,
    data_type: Option<PrimitiveType>,
    options: ColumnOptions,
}

impl ColumnSchema {
    /// Create a scalar column holding one primitive value per row.
    ///
    /// Corresponds to C++ `ScalarColumnDesc<T>`. The column has no shape
    /// constraint and no special storage flags by default.
    pub fn scalar(name: impl Into<String>, data_type: PrimitiveType) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Scalar,
            data_type: Some(data_type),
            options: ColumnOptions::default(),
        }
    }

    /// Create a record column holding a nested keyword/value record per row.
    ///
    /// Corresponds to a C++ column with data type `TpRecord`. Record columns
    /// carry no numeric element type, so [`ColumnSchema::data_type`] returns
    /// `None` for these columns.
    pub fn record(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Record,
            data_type: None,
            options: ColumnOptions::default(),
        }
    }

    /// Create an array column with a fixed shape shared by every row.
    ///
    /// Corresponds to C++ `ArrayColumnDesc<T>` constructed with a fixed
    /// `IPosition`. The supplied `shape` is embedded in the schema and every
    /// cell must conform to it. This column kind is eligible for direct storage
    /// via [`ColumnOptions::direct`].
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

    /// Create an array column whose shape may differ between rows.
    ///
    /// Corresponds to C++ `ArrayColumnDesc<T>` without a fixed shape (indirect
    /// array). `ndim` optionally constrains the number of dimensions; pass
    /// `None` to leave both dimensionality and extents unconstrained.
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

    /// Attach additional storage/behaviour options to this column.
    ///
    /// Validates the combination of options against the column kind and returns
    /// [`SchemaError::DirectRequiresFixedShape`] or
    /// [`SchemaError::UndefinedOnlyForScalar`] if the options are incompatible.
    /// On success returns `self` with the options applied, enabling builder-style
    /// construction.
    pub fn with_options(mut self, options: ColumnOptions) -> Result<Self, SchemaError> {
        self.options = options;
        self.validate_options()?;
        Ok(self)
    }

    /// Return the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the column kind (scalar, array, or record).
    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    /// Return the primitive element type, or `None` for record columns.
    pub fn data_type(&self) -> Option<PrimitiveType> {
        self.data_type
    }

    /// Return the storage and behaviour flags for this column.
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

/// Defines the structure of a casacore table: an ordered set of column descriptions.
///
/// Corresponds to C++ `TableDesc`. A `TableSchema` is constructed before the
/// table itself and subsequently embedded in the on-disk table directory. It
/// records all column names, types, shapes, and storage flags that govern how
/// data is stored and retrieved.
///
/// From the C++ docs: "A table description consists of: name, version, comment,
/// a set of column descriptions, and a keyword set." This Rust type covers the
/// column-description portion; keywords are handled separately.
///
/// # Example
///
/// ```rust
/// use casacore_tables::{ColumnSchema, TableSchema};
/// use casacore_types::PrimitiveType;
///
/// let schema = TableSchema::new(vec![
///     ColumnSchema::scalar("time", PrimitiveType::Float64),
///     ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![4, 64]),
/// ])
/// .expect("no duplicate columns");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableSchema {
    columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Construct a `TableSchema` from an ordered list of column descriptions.
    ///
    /// Validates each [`ColumnSchema`]'s options (see [`ColumnSchema::with_options`])
    /// and rejects the schema if any two columns share the same name, returning
    /// [`SchemaError::DuplicateColumn`]. Cf. C++ `TableDesc::addColumn`, which
    /// throws `TableError` on the first duplicate encountered.
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

    /// Return the ordered slice of column descriptions.
    ///
    /// The order matches the order supplied to [`TableSchema::new`].
    /// Cf. C++ `TableDesc::columnDescSet()`.
    pub fn columns(&self) -> &[ColumnSchema] {
        &self.columns
    }

    /// Look up a column description by name.
    ///
    /// Returns `None` if no column with the given name exists.
    /// Cf. C++ `TableDesc::columnDesc(name)`, which throws on a missing column.
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Return `true` if a column with the given name exists in this schema.
    ///
    /// Cf. C++ `TableDesc::isColumn(name)`.
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
