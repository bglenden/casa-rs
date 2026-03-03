// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared value model for all `casacore-*` crates.
//!
//! This crate defines the scalar, array, and record value types that mirror
//! the C++ casacore type system. Every `casacore-*` crate that reads or writes
//! table data depends on these types as its currency for cell values, keyword
//! values, and record fields.
//!
//! ## Relationship to C++ casacore
//!
//! C++ casacore organises data around a small set of native types — `Bool`,
//! `uChar`, `Short`, `uShort`, `Int`, `uInt`, `Int64`, `Float`, `Double`,
//! `Complex`, `DComplex`, and `String` — combined with scalar vs. array rank.
//! This crate maps each of those types to an idiomatic Rust equivalent:
//!
//! | C++ casacore type | Rust type          | Variant              |
//! |-------------------|--------------------|----------------------|
//! | `Bool`            | `bool`             | `*::Bool`            |
//! | `uChar`           | `u8`               | `*::UInt8`           |
//! | `uShort`          | `u16`              | `*::UInt16`          |
//! | `uInt`            | `u32`              | `*::UInt32`          |
//! | `Short`           | `i16`              | `*::Int16`           |
//! | `Int`             | `i32`              | `*::Int32`           |
//! | `Int64`           | `i64`              | `*::Int64`           |
//! | `Float`           | `f32`              | `*::Float32`         |
//! | `Double`          | `f64`              | `*::Float64`         |
//! | `Complex`         | [`Complex32`]      | `*::Complex32`       |
//! | `DComplex`        | [`Complex64`]      | `*::Complex64`       |
//! | `String`          | [`String`]         | `*::String`          |
//!
//! ## Key types
//!
//! - [`ScalarValue`] — a single typed value (bool, integer, float, complex, or string).
//! - [`ArrayValue`] — an N-dimensional array backed by [`ndarray::ArrayD`].
//! - [`RecordValue`] — an ordered collection of named [`Value`] fields.
//! - [`Value`] — the top-level enum unifying scalars, arrays, and records.
//!
//! Type metadata is captured by [`PrimitiveType`] and [`TypeTag`].
//!
//! ## Usage examples
//!
//! ```rust
//! use casacore_types::{ScalarValue, ArrayValue, RecordField, RecordValue, Value};
//!
//! // A single typed scalar — equivalent to a ScalarColumn<Double> cell value.
//! let scalar = ScalarValue::Float64(3.14);
//!
//! // A 1-D float array — equivalent to an ArrayColumn<Float> cell value.
//! let array = ArrayValue::from_f32_vec(vec![1.0, 2.0, 3.0]);
//!
//! // A record — equivalent to a C++ TableRecord or Record.
//! let mut record = RecordValue::default();
//! record.push(RecordField::new("pi", Value::Scalar(ScalarValue::Float64(3.14))));
//! record.upsert("n", Value::Scalar(ScalarValue::Int32(42)));
//! ```

pub use num_complex::{Complex32, Complex64};

use ndarray::Array1;
pub use ndarray::{Array2, Array3, ArrayD};

/// The primitive element type of a casacore value.
///
/// This is the type tag used throughout the casacore table system to identify
/// the element kind of scalar and array cell values. It corresponds directly
/// to the set of native C++ casacore types: `Bool`, `uChar`, `Short`,
/// `uShort`, `Int`, `uInt`, `Int64`, `Float`, `Double`, `Complex`,
/// `DComplex`, and `String`.
///
/// `PrimitiveType` records only the element kind; for the full type identity
/// (element kind plus scalar/array rank) see [`TypeTag`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    /// Boolean (`Bool` in C++ casacore). On-disk width: 1 byte.
    Bool,
    /// Unsigned 8-bit integer (`uChar` in C++ casacore). On-disk width: 1 byte.
    UInt8,
    /// Unsigned 16-bit integer (`uShort` in C++ casacore). On-disk width: 2 bytes.
    UInt16,
    /// Unsigned 32-bit integer (`uInt` in C++ casacore). On-disk width: 4 bytes.
    UInt32,
    /// Signed 16-bit integer (`Short` in C++ casacore). On-disk width: 2 bytes.
    Int16,
    /// Signed 32-bit integer (`Int` in C++ casacore). On-disk width: 4 bytes.
    Int32,
    /// Signed 64-bit integer (`Int64` in C++ casacore). On-disk width: 8 bytes.
    Int64,
    /// 32-bit IEEE float (`Float` in C++ casacore). On-disk width: 4 bytes.
    Float32,
    /// 64-bit IEEE float (`Double` in C++ casacore). On-disk width: 8 bytes.
    Float64,
    /// 32-bit complex (two `f32` components; `Complex` in C++ casacore). On-disk width: 8 bytes.
    Complex32,
    /// 64-bit complex (two `f64` components; `DComplex` in C++ casacore). On-disk width: 16 bytes.
    Complex64,
    /// UTF-8 string (`String` in C++ casacore). Variable-width; no fixed on-disk size.
    String,
}

impl PrimitiveType {
    /// Returns the fixed on-disk byte width of this primitive type, if it is fixed-width.
    ///
    /// Returns `None` for [`PrimitiveType::String`], which is variable-width on disk.
    /// All other primitive types have a fixed serialised size; those sizes match the
    /// widths used by the C++ casacore AipsIO format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use casacore_types::PrimitiveType;
    ///
    /// assert_eq!(PrimitiveType::Float64.fixed_width_bytes(), Some(8));
    /// assert_eq!(PrimitiveType::Complex64.fixed_width_bytes(), Some(16));
    /// assert_eq!(PrimitiveType::String.fixed_width_bytes(), None);
    /// ```
    pub fn fixed_width_bytes(self) -> Option<usize> {
        match self {
            Self::Bool => Some(1),
            Self::UInt8 => Some(1),
            Self::UInt16 => Some(2),
            Self::UInt32 => Some(4),
            Self::Int16 => Some(2),
            Self::Int32 => Some(4),
            Self::Int64 => Some(8),
            Self::Float32 => Some(4),
            Self::Float64 => Some(8),
            Self::Complex32 => Some(8),
            Self::Complex64 => Some(16),
            Self::String => None,
        }
    }
}

/// Whether a value is a scalar or an N-dimensional array.
///
/// `ValueRank` is used as the rank component of a [`TypeTag`]. For the
/// three-way discrimination that also includes records, see [`ValueKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueRank {
    /// A single element with no array dimensions.
    Scalar,
    /// An N-dimensional array of one or more elements.
    Array,
}

/// The complete on-disk type identity of a casacore value: element type plus rank.
///
/// A `TypeTag` is the (primitive type, rank) pair that uniquely identifies
/// how a value is stored in the casacore on-disk format. Codec and storage
/// helpers use `TypeTag` to select the correct serialisation path.
///
/// Cf. the type descriptor objects used internally by C++ casacore column and
/// keyword descriptors.
///
/// # Examples
///
/// ```rust
/// use casacore_types::{TypeTag, PrimitiveType, ValueRank};
///
/// let tag = TypeTag::scalar(PrimitiveType::Float64);
/// assert_eq!(tag.primitive, PrimitiveType::Float64);
/// assert_eq!(tag.rank, ValueRank::Scalar);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeTag {
    /// The element type.
    pub primitive: PrimitiveType,
    /// Whether the value is a scalar or an array.
    pub rank: ValueRank,
}

impl TypeTag {
    /// Creates a [`TypeTag`] for a scalar value of the given primitive type.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use casacore_types::{TypeTag, PrimitiveType, ValueRank};
    ///
    /// let tag = TypeTag::scalar(PrimitiveType::Int32);
    /// assert_eq!(tag.rank, ValueRank::Scalar);
    /// ```
    pub const fn scalar(primitive: PrimitiveType) -> Self {
        Self {
            primitive,
            rank: ValueRank::Scalar,
        }
    }

    /// Creates a [`TypeTag`] for an array value of the given primitive type.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use casacore_types::{TypeTag, PrimitiveType, ValueRank};
    ///
    /// let tag = TypeTag::array(PrimitiveType::Float32);
    /// assert_eq!(tag.rank, ValueRank::Array);
    /// ```
    pub const fn array(primitive: PrimitiveType) -> Self {
        Self {
            primitive,
            rank: ValueRank::Array,
        }
    }
}

/// The broad kind of a [`Value`]: scalar, array, or record.
///
/// `ValueKind` extends [`ValueRank`] with a third variant for record-typed
/// values. Records have no [`TypeTag`] because they do not have a single
/// primitive element type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueKind {
    /// A single typed scalar value.
    Scalar,
    /// An N-dimensional typed array.
    Array,
    /// An ordered collection of named [`Value`] fields (a record).
    Record,
}

/// A runtime container for a single typed scalar value.
///
/// Each variant wraps the Rust-native equivalent of the corresponding C++
/// casacore type. `ScalarValue` is the Rust counterpart of a cell read from a
/// C++ `ScalarColumn<T>`.
///
/// The active variant determines the [`PrimitiveType`] and therefore the
/// on-disk encoding. Use [`ScalarValue::primitive_type`] or
/// [`ScalarValue::type_tag`] to inspect it without pattern-matching.
///
/// # Examples
///
/// ```rust
/// use casacore_types::{ScalarValue, PrimitiveType};
///
/// let v = ScalarValue::Float64(2.718);
/// assert_eq!(v.primitive_type(), PrimitiveType::Float64);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// Boolean scalar (`Bool` in C++ casacore).
    Bool(bool),
    /// Unsigned 8-bit integer scalar (`uChar` in C++ casacore).
    UInt8(u8),
    /// Unsigned 16-bit integer scalar (`uShort` in C++ casacore).
    UInt16(u16),
    /// Unsigned 32-bit integer scalar (`uInt` in C++ casacore).
    UInt32(u32),
    /// Signed 16-bit integer scalar (`Short` in C++ casacore).
    Int16(i16),
    /// Signed 32-bit integer scalar (`Int` in C++ casacore).
    Int32(i32),
    /// Signed 64-bit integer scalar (`Int64` in C++ casacore).
    Int64(i64),
    /// 32-bit float scalar (`Float` in C++ casacore).
    Float32(f32),
    /// 64-bit float scalar (`Double` in C++ casacore).
    Float64(f64),
    /// 32-bit complex scalar (`Complex` in C++ casacore).
    Complex32(Complex32),
    /// 64-bit complex scalar (`DComplex` in C++ casacore).
    Complex64(Complex64),
    /// UTF-8 string scalar (`String` in C++ casacore).
    String(String),
}

impl ScalarValue {
    /// Returns the [`PrimitiveType`] tag for this scalar value.
    ///
    /// The result is determined entirely by the active variant, so no
    /// inspection of the wrapped value is required.
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Bool(_) => PrimitiveType::Bool,
            Self::UInt8(_) => PrimitiveType::UInt8,
            Self::UInt16(_) => PrimitiveType::UInt16,
            Self::UInt32(_) => PrimitiveType::UInt32,
            Self::Int16(_) => PrimitiveType::Int16,
            Self::Int32(_) => PrimitiveType::Int32,
            Self::Int64(_) => PrimitiveType::Int64,
            Self::Float32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
            Self::Complex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
            Self::String(_) => PrimitiveType::String,
        }
    }

    /// Returns the full [`TypeTag`] (primitive type + scalar rank) for this value.
    ///
    /// Equivalent to `TypeTag::scalar(self.primitive_type())`.
    pub fn type_tag(&self) -> TypeTag {
        TypeTag::scalar(self.primitive_type())
    }
}

/// An N-dimensional typed array value.
///
/// Each variant wraps an [`ArrayD<T>`] whose element type corresponds to a
/// C++ casacore native type. `ArrayValue` is the Rust counterpart of a cell
/// read from a C++ `ArrayColumn<T>`, or of a C++ `Array<T>` value stored as
/// a keyword.
///
/// The number of dimensions and shape are determined by the inner `ArrayD`
/// and can be inspected with [`ArrayValue::ndim`] and [`ArrayValue::shape`]
/// without unwrapping the variant.
///
/// # Constructing 1-D arrays
///
/// The `from_*_vec` factory methods are the most convenient way to build a
/// 1-D `ArrayValue` from a `Vec`. For higher-dimensional arrays, construct an
/// [`ArrayD`] directly and wrap it in the appropriate variant.
///
/// # Examples
///
/// ```rust
/// use casacore_types::{ArrayValue, PrimitiveType};
///
/// let arr = ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0]);
/// assert_eq!(arr.primitive_type(), PrimitiveType::Float64);
/// assert_eq!(arr.shape(), &[3]);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayValue {
    /// Boolean array (`Array<Bool>` in C++ casacore).
    Bool(ArrayD<bool>),
    /// Unsigned 8-bit integer array (`Array<uChar>` in C++ casacore).
    UInt8(ArrayD<u8>),
    /// Unsigned 16-bit integer array (`Array<uShort>` in C++ casacore).
    UInt16(ArrayD<u16>),
    /// Unsigned 32-bit integer array (`Array<uInt>` in C++ casacore).
    UInt32(ArrayD<u32>),
    /// Signed 16-bit integer array (`Array<Short>` in C++ casacore).
    Int16(ArrayD<i16>),
    /// Signed 32-bit integer array (`Array<Int>` in C++ casacore).
    Int32(ArrayD<i32>),
    /// Signed 64-bit integer array (`Array<Int64>` in C++ casacore).
    Int64(ArrayD<i64>),
    /// 32-bit float array (`Array<Float>` in C++ casacore).
    Float32(ArrayD<f32>),
    /// 64-bit float array (`Array<Double>` in C++ casacore).
    Float64(ArrayD<f64>),
    /// 32-bit complex array (`Array<Complex>` in C++ casacore).
    Complex32(ArrayD<Complex32>),
    /// 64-bit complex array (`Array<DComplex>` in C++ casacore).
    Complex64(ArrayD<Complex64>),
    /// UTF-8 string array (`Array<String>` in C++ casacore).
    String(ArrayD<String>),
}

impl ArrayValue {
    /// Returns the [`PrimitiveType`] tag for the elements of this array.
    ///
    /// The result is determined entirely by the active variant.
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Bool(_) => PrimitiveType::Bool,
            Self::UInt8(_) => PrimitiveType::UInt8,
            Self::UInt16(_) => PrimitiveType::UInt16,
            Self::UInt32(_) => PrimitiveType::UInt32,
            Self::Int16(_) => PrimitiveType::Int16,
            Self::Int32(_) => PrimitiveType::Int32,
            Self::Int64(_) => PrimitiveType::Int64,
            Self::Float32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
            Self::Complex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
            Self::String(_) => PrimitiveType::String,
        }
    }

    /// Returns the full [`TypeTag`] (primitive type + array rank) for this value.
    ///
    /// Equivalent to `TypeTag::array(self.primitive_type())`.
    pub fn type_tag(&self) -> TypeTag {
        TypeTag::array(self.primitive_type())
    }

    /// Returns the total number of elements across all dimensions.
    ///
    /// Delegates to [`ArrayD::len`] on the inner array.
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(v) => v.len(),
            Self::UInt8(v) => v.len(),
            Self::UInt16(v) => v.len(),
            Self::UInt32(v) => v.len(),
            Self::Int16(v) => v.len(),
            Self::Int32(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Float32(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Complex32(v) => v.len(),
            Self::Complex64(v) => v.len(),
            Self::String(v) => v.len(),
        }
    }

    /// Returns `true` if the array contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of dimensions (axes) of the array.
    ///
    /// A 1-D array returns `1`, a 2-D array returns `2`, and so on.
    /// Delegates to [`ArrayD::ndim`] on the inner array.
    pub fn ndim(&self) -> usize {
        match self {
            Self::Bool(v) => v.ndim(),
            Self::UInt8(v) => v.ndim(),
            Self::UInt16(v) => v.ndim(),
            Self::UInt32(v) => v.ndim(),
            Self::Int16(v) => v.ndim(),
            Self::Int32(v) => v.ndim(),
            Self::Int64(v) => v.ndim(),
            Self::Float32(v) => v.ndim(),
            Self::Float64(v) => v.ndim(),
            Self::Complex32(v) => v.ndim(),
            Self::Complex64(v) => v.ndim(),
            Self::String(v) => v.ndim(),
        }
    }

    /// Returns the shape (size along each dimension) of the array.
    ///
    /// The returned slice has length equal to [`ArrayValue::ndim`].
    /// Delegates to [`ArrayD::shape`] on the inner array.
    pub fn shape(&self) -> &[usize] {
        match self {
            Self::Bool(v) => v.shape(),
            Self::UInt8(v) => v.shape(),
            Self::UInt16(v) => v.shape(),
            Self::UInt32(v) => v.shape(),
            Self::Int16(v) => v.shape(),
            Self::Int32(v) => v.shape(),
            Self::Int64(v) => v.shape(),
            Self::Float32(v) => v.shape(),
            Self::Float64(v) => v.shape(),
            Self::Complex32(v) => v.shape(),
            Self::Complex64(v) => v.shape(),
            Self::String(v) => v.shape(),
        }
    }

    /// Constructs a 1-D [`ArrayValue::Bool`] from a `Vec<bool>`.
    ///
    /// The vector is converted to a rank-1 [`ArrayD`] using
    /// [`ndarray::Array1::from_vec`] followed by `.into_dyn()`. The resulting
    /// array has shape `[values.len()]`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use casacore_types::ArrayValue;
    ///
    /// let arr = ArrayValue::from_bool_vec(vec![true, false, true]);
    /// assert_eq!(arr.shape(), &[3]);
    /// ```
    pub fn from_bool_vec(values: Vec<bool>) -> Self {
        Self::Bool(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::UInt8`] from a `Vec<u8>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_u8_vec(values: Vec<u8>) -> Self {
        Self::UInt8(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::UInt16`] from a `Vec<u16>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_u16_vec(values: Vec<u16>) -> Self {
        Self::UInt16(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::UInt32`] from a `Vec<u32>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_u32_vec(values: Vec<u32>) -> Self {
        Self::UInt32(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Int16`] from a `Vec<i16>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_i16_vec(values: Vec<i16>) -> Self {
        Self::Int16(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Int32`] from a `Vec<i32>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_i32_vec(values: Vec<i32>) -> Self {
        Self::Int32(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Int64`] from a `Vec<i64>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_i64_vec(values: Vec<i64>) -> Self {
        Self::Int64(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Float32`] from a `Vec<f32>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_f32_vec(values: Vec<f32>) -> Self {
        Self::Float32(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Float64`] from a `Vec<f64>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_f64_vec(values: Vec<f64>) -> Self {
        Self::Float64(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Complex32`] from a `Vec<Complex32>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_complex32_vec(values: Vec<Complex32>) -> Self {
        Self::Complex32(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::Complex64`] from a `Vec<Complex64>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_complex64_vec(values: Vec<Complex64>) -> Self {
        Self::Complex64(Array1::from_vec(values).into_dyn())
    }

    /// Constructs a 1-D [`ArrayValue::String`] from a `Vec<String>`.
    ///
    /// See [`from_bool_vec`](Self::from_bool_vec) for details.
    pub fn from_string_vec(values: Vec<String>) -> Self {
        Self::String(Array1::from_vec(values).into_dyn())
    }
}

/// A single named field within a [`RecordValue`].
///
/// A `RecordField` pairs a name with an arbitrary [`Value`], mirroring the
/// named-field concept of C++ `RecordFieldPtr` and `RecordDesc`. Fields are
/// stored in insertion order inside a [`RecordValue`].
#[derive(Debug, Clone, PartialEq)]
pub struct RecordField {
    /// The field name as it appears in the on-disk record descriptor.
    pub name: String,
    /// The field value, which may itself be a scalar, array, or nested record.
    pub value: Value,
}

impl RecordField {
    /// Creates a new `RecordField` with the given name and value.
    ///
    /// The `name` parameter accepts anything that implements `Into<String>`,
    /// so both `&str` and `String` are accepted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use casacore_types::{RecordField, ScalarValue, Value};
    ///
    /// let field = RecordField::new("answer", Value::Scalar(ScalarValue::Int32(42)));
    /// assert_eq!(field.name, "answer");
    /// ```
    pub fn new(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// An ordered collection of named [`Value`] fields.
///
/// `RecordValue` is the Rust equivalent of C++ casacore's `TableRecord` and
/// `Record` types. It is used to represent:
///
/// - Table keyword sets (the metadata attached to a table or column).
/// - Record-typed column cell values.
/// - Nested records within other records.
///
/// Fields are stored in insertion order. Lookup by name is O(n); for typical
/// casacore keyword sets this is not a performance concern.
///
/// # Examples
///
/// ```rust
/// use casacore_types::{RecordValue, RecordField, ScalarValue, Value};
///
/// let mut record = RecordValue::default();
/// record.push(RecordField::new("freq", Value::Scalar(ScalarValue::Float64(1.4e9))));
/// record.upsert("freq", Value::Scalar(ScalarValue::Float64(1.5e9))); // replaces
///
/// assert_eq!(record.len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RecordValue {
    fields: Vec<RecordField>,
}

impl RecordValue {
    /// Creates a `RecordValue` from an existing vector of [`RecordField`]s.
    ///
    /// Field order is preserved. If you need an empty record, prefer
    /// `RecordValue::default()`.
    pub fn new(fields: Vec<RecordField>) -> Self {
        Self { fields }
    }

    /// Returns a slice of all fields in insertion order.
    pub fn fields(&self) -> &[RecordField] {
        &self.fields
    }

    /// Returns a mutable slice of all fields in insertion order.
    ///
    /// This allows in-place mutation of existing field values but does not
    /// allow adding or removing fields. Use [`push`](Self::push) or
    /// [`upsert`](Self::upsert) to add fields.
    pub fn fields_mut(&mut self) -> &mut [RecordField] {
        &mut self.fields
    }

    /// Consumes the `RecordValue` and returns the underlying field vector.
    pub fn into_fields(self) -> Vec<RecordField> {
        self.fields
    }

    /// Appends `field` to the end of the record without checking for duplicates.
    ///
    /// If a field with the same name already exists, both will be present.
    /// Use [`upsert`](Self::upsert) if you want update-or-insert semantics.
    pub fn push(&mut self, field: RecordField) {
        self.fields.push(field);
    }

    /// Returns the number of fields in the record.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if the record contains no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns a shared reference to the value of the first field named `name`.
    ///
    /// Returns `None` if no field with that name exists. If duplicate field
    /// names are present (see [`push`](Self::push)), only the first match is
    /// returned.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| &field.value)
    }

    /// Returns a mutable reference to the value of the first field named `name`.
    ///
    /// Returns `None` if no field with that name exists.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.fields
            .iter_mut()
            .find(|field| field.name == name)
            .map(|field| &mut field.value)
    }

    /// Updates the value of an existing field, or inserts a new field if none exists.
    ///
    /// If a field named `name` already exists, its value is replaced in place.
    /// Otherwise a new [`RecordField`] is appended. This matches the semantics
    /// of C++ `TableRecord::define`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use casacore_types::{RecordValue, ScalarValue, Value};
    ///
    /// let mut record = RecordValue::default();
    /// record.upsert("n", Value::Scalar(ScalarValue::Int32(1)));
    /// record.upsert("n", Value::Scalar(ScalarValue::Int32(2))); // replaces
    /// assert_eq!(record.len(), 1);
    /// assert_eq!(record.get("n"), Some(&Value::Scalar(ScalarValue::Int32(2))));
    /// ```
    pub fn upsert(&mut self, name: impl Into<String>, value: Value) {
        let name = name.into();
        if let Some(existing) = self.fields.iter_mut().find(|field| field.name == name) {
            existing.value = value;
            return;
        }
        self.fields.push(RecordField::new(name, value));
    }

    /// Remove the first field named `name`, returning its value.
    ///
    /// Returns `None` if no field with that name exists. Cf. C++
    /// `RecordInterface::removeField`.
    pub fn remove(&mut self, name: &str) -> Option<Value> {
        let pos = self.fields.iter().position(|f| f.name == name)?;
        Some(self.fields.remove(pos).value)
    }

    /// Rename the first field named `old_name` to `new_name`.
    ///
    /// Returns `true` if the field was found and renamed, `false` if no
    /// field with `old_name` exists. Cf. C++ `RecordInterface::renameField`.
    pub fn rename_field(&mut self, old_name: &str, new_name: impl Into<String>) -> bool {
        if let Some(field) = self.fields.iter_mut().find(|f| f.name == old_name) {
            field.name = new_name.into();
            true
        } else {
            false
        }
    }
}

impl From<Vec<RecordField>> for RecordValue {
    /// Converts a `Vec<RecordField>` into a `RecordValue`, preserving field order.
    fn from(fields: Vec<RecordField>) -> Self {
        Self { fields }
    }
}

/// The top-level value type for casacore table cells and keywords.
///
/// A `Value` is either a single typed scalar ([`ScalarValue`]), an
/// N-dimensional typed array ([`ArrayValue`]), or a named-field record
/// ([`RecordValue`]). This mirrors the three possible column cell kinds in C++
/// casacore: scalar columns, array columns, and record-typed keywords.
///
/// `From` implementations allow `ScalarValue`, `ArrayValue`, and `RecordValue`
/// to be converted into `Value` with `.into()`.
///
/// # Examples
///
/// ```rust
/// use casacore_types::{Value, ScalarValue, ValueKind};
///
/// let v: Value = ScalarValue::Int32(7).into();
/// assert_eq!(v.kind(), ValueKind::Scalar);
/// assert!(v.type_tag().is_some());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A single typed scalar value.
    Scalar(ScalarValue),
    /// An N-dimensional typed array value.
    Array(ArrayValue),
    /// An ordered collection of named fields.
    Record(RecordValue),
}

impl Value {
    /// Returns the [`ValueKind`] of this value (scalar, array, or record).
    pub fn kind(&self) -> ValueKind {
        match self {
            Self::Scalar(_) => ValueKind::Scalar,
            Self::Array(_) => ValueKind::Array,
            Self::Record(_) => ValueKind::Record,
        }
    }

    /// Returns the [`TypeTag`] for this value, if one exists.
    ///
    /// Returns `Some` for scalar and array values, and `None` for record values
    /// because records do not have a single primitive element type.
    pub fn type_tag(&self) -> Option<TypeTag> {
        match self {
            Self::Scalar(v) => Some(v.type_tag()),
            Self::Array(v) => Some(v.type_tag()),
            Self::Record(_) => None,
        }
    }
}

impl From<ScalarValue> for Value {
    /// Wraps a [`ScalarValue`] in the `Value::Scalar` variant.
    fn from(value: ScalarValue) -> Self {
        Self::Scalar(value)
    }
}

impl From<ArrayValue> for Value {
    /// Wraps an [`ArrayValue`] in the `Value::Array` variant.
    fn from(value: ArrayValue) -> Self {
        Self::Array(value)
    }
}

impl From<RecordValue> for Value {
    /// Wraps a [`RecordValue`] in the `Value::Record` variant.
    fn from(value: RecordValue) -> Self {
        Self::Record(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArrayValue, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, TypeTag,
        Value, ValueKind, ValueRank,
    };
    use ndarray::{Array, IxDyn};

    #[test]
    fn scalar_type_tag_is_derived_from_variant() {
        let value = ScalarValue::Float64(3.5);
        let tag = value.type_tag();
        assert_eq!(tag.primitive, PrimitiveType::Float64);
        assert_eq!(tag.rank, ValueRank::Scalar);
    }

    #[test]
    fn array_type_tag_uses_array_rank() {
        let values = vec![1_i16, -2_i16, 3_i16];
        let array = ArrayValue::from_i16_vec(values);
        let tag = array.type_tag();
        assert_eq!(tag.primitive, PrimitiveType::Int16);
        assert_eq!(tag.rank, ValueRank::Array);
        assert_eq!(array.ndim(), 1);
        assert_eq!(array.shape(), &[3]);
    }

    #[test]
    fn unsigned_scalar_and_array_type_tags_are_supported() {
        let scalar = ScalarValue::UInt32(7);
        assert_eq!(scalar.type_tag(), TypeTag::scalar(PrimitiveType::UInt32));

        let array = ArrayValue::from_u8_vec(vec![1, 2, 3]);
        assert_eq!(array.type_tag(), TypeTag::array(PrimitiveType::UInt8));
    }

    #[test]
    fn record_has_no_primitive_type_tag() {
        let record = RecordValue::new(vec![RecordField::new(
            "answer",
            Value::Scalar(ScalarValue::Int32(42)),
        )]);
        assert_eq!(Value::Record(record).type_tag(), None);
    }

    #[test]
    fn value_kind_includes_record() {
        let value = Value::Record(RecordValue::new(vec![]));
        assert_eq!(value.kind(), ValueKind::Record);
    }

    #[test]
    fn record_lookup_returns_matching_value() {
        let mut record = RecordValue::default();
        record.push(RecordField::new(
            "z",
            Value::Array(ArrayValue::Complex64(
                Array::from_shape_vec(IxDyn(&[1, 1]), vec![Complex64 { re: 1.0, im: -2.0 }])
                    .expect("shape")
                    .into_dyn(),
            )),
        ));

        let value = record.get("z");
        assert_eq!(
            value,
            Some(&Value::Array(ArrayValue::Complex64(
                Array::from_shape_vec(IxDyn(&[1, 1]), vec![Complex64 { re: 1.0, im: -2.0 }],)
                    .expect("shape")
                    .into_dyn(),
            )))
        );
    }

    #[test]
    fn record_remove_returns_value() {
        let mut record = RecordValue::new(vec![
            RecordField::new("a", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("b", Value::Scalar(ScalarValue::Int32(2))),
        ]);
        let removed = record.remove("a");
        assert_eq!(removed, Some(Value::Scalar(ScalarValue::Int32(1))));
        assert_eq!(record.len(), 1);
        assert!(record.get("a").is_none());
        assert!(record.get("b").is_some());
    }

    #[test]
    fn record_remove_missing_returns_none() {
        let mut record = RecordValue::new(vec![RecordField::new(
            "a",
            Value::Scalar(ScalarValue::Int32(1)),
        )]);
        assert_eq!(record.remove("z"), None);
        assert_eq!(record.len(), 1);
    }

    #[test]
    fn record_rename_field() {
        let mut record = RecordValue::new(vec![
            RecordField::new("old", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new("other", Value::Scalar(ScalarValue::Int32(8))),
        ]);
        assert!(record.rename_field("old", "new"));
        assert!(record.get("old").is_none());
        assert_eq!(
            record.get("new"),
            Some(&Value::Scalar(ScalarValue::Int32(7)))
        );
        assert!(!record.rename_field("missing", "x"));
    }
}
