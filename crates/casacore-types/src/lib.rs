// SPDX-License-Identifier: LGPL-3.0-or-later
//! Foundation value types for casacore-compatible data structures.
//!
//! This crate defines the scalar, array, and record value model shared by all
//! `casacore-*` crates. Key types:
//!
//! - [`ScalarValue`] — a single typed value (bool, integer, float, complex, or string).
//! - [`ArrayValue`] — an N-dimensional array backed by [`ndarray::ArrayD`].
//! - [`RecordValue`] — an ordered collection of named [`Value`] fields.
//! - [`Value`] — the top-level enum unifying scalars, arrays, and records.
//!
//! Type metadata is captured by [`PrimitiveType`] and [`TypeTag`].

pub use num_complex::{Complex32, Complex64};

use ndarray::Array1;
pub use ndarray::{Array2, Array3, ArrayD};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    Bool,
    UInt8,
    UInt16,
    UInt32,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Complex32,
    Complex64,
    String,
}

impl PrimitiveType {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueRank {
    Scalar,
    Array,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeTag {
    pub primitive: PrimitiveType,
    pub rank: ValueRank,
}

impl TypeTag {
    pub const fn scalar(primitive: PrimitiveType) -> Self {
        Self {
            primitive,
            rank: ValueRank::Scalar,
        }
    }

    pub const fn array(primitive: PrimitiveType) -> Self {
        Self {
            primitive,
            rank: ValueRank::Array,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueKind {
    Scalar,
    Array,
    Record,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Bool(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Complex32(Complex32),
    Complex64(Complex64),
    String(String),
}

impl ScalarValue {
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

    pub fn type_tag(&self) -> TypeTag {
        TypeTag::scalar(self.primitive_type())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayValue {
    Bool(ArrayD<bool>),
    UInt8(ArrayD<u8>),
    UInt16(ArrayD<u16>),
    UInt32(ArrayD<u32>),
    Int16(ArrayD<i16>),
    Int32(ArrayD<i32>),
    Int64(ArrayD<i64>),
    Float32(ArrayD<f32>),
    Float64(ArrayD<f64>),
    Complex32(ArrayD<Complex32>),
    Complex64(ArrayD<Complex64>),
    String(ArrayD<String>),
}

impl ArrayValue {
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

    pub fn type_tag(&self) -> TypeTag {
        TypeTag::array(self.primitive_type())
    }

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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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

    pub fn from_bool_vec(values: Vec<bool>) -> Self {
        Self::Bool(Array1::from_vec(values).into_dyn())
    }

    pub fn from_u8_vec(values: Vec<u8>) -> Self {
        Self::UInt8(Array1::from_vec(values).into_dyn())
    }

    pub fn from_u16_vec(values: Vec<u16>) -> Self {
        Self::UInt16(Array1::from_vec(values).into_dyn())
    }

    pub fn from_u32_vec(values: Vec<u32>) -> Self {
        Self::UInt32(Array1::from_vec(values).into_dyn())
    }

    pub fn from_i16_vec(values: Vec<i16>) -> Self {
        Self::Int16(Array1::from_vec(values).into_dyn())
    }

    pub fn from_i32_vec(values: Vec<i32>) -> Self {
        Self::Int32(Array1::from_vec(values).into_dyn())
    }

    pub fn from_i64_vec(values: Vec<i64>) -> Self {
        Self::Int64(Array1::from_vec(values).into_dyn())
    }

    pub fn from_f32_vec(values: Vec<f32>) -> Self {
        Self::Float32(Array1::from_vec(values).into_dyn())
    }

    pub fn from_f64_vec(values: Vec<f64>) -> Self {
        Self::Float64(Array1::from_vec(values).into_dyn())
    }

    pub fn from_complex32_vec(values: Vec<Complex32>) -> Self {
        Self::Complex32(Array1::from_vec(values).into_dyn())
    }

    pub fn from_complex64_vec(values: Vec<Complex64>) -> Self {
        Self::Complex64(Array1::from_vec(values).into_dyn())
    }

    pub fn from_string_vec(values: Vec<String>) -> Self {
        Self::String(Array1::from_vec(values).into_dyn())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordField {
    pub name: String,
    pub value: Value,
}

impl RecordField {
    pub fn new(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RecordValue {
    fields: Vec<RecordField>,
}

impl RecordValue {
    pub fn new(fields: Vec<RecordField>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[RecordField] {
        &self.fields
    }

    pub fn fields_mut(&mut self) -> &mut [RecordField] {
        &mut self.fields
    }

    pub fn into_fields(self) -> Vec<RecordField> {
        self.fields
    }

    pub fn push(&mut self, field: RecordField) {
        self.fields.push(field);
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| &field.value)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.fields
            .iter_mut()
            .find(|field| field.name == name)
            .map(|field| &mut field.value)
    }

    pub fn upsert(&mut self, name: impl Into<String>, value: Value) {
        let name = name.into();
        if let Some(existing) = self.fields.iter_mut().find(|field| field.name == name) {
            existing.value = value;
            return;
        }
        self.fields.push(RecordField::new(name, value));
    }
}

impl From<Vec<RecordField>> for RecordValue {
    fn from(fields: Vec<RecordField>) -> Self {
        Self { fields }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Scalar(ScalarValue),
    Array(ArrayValue),
    Record(RecordValue),
}

impl Value {
    pub fn kind(&self) -> ValueKind {
        match self {
            Self::Scalar(_) => ValueKind::Scalar,
            Self::Array(_) => ValueKind::Array,
            Self::Record(_) => ValueKind::Record,
        }
    }

    pub fn type_tag(&self) -> Option<TypeTag> {
        match self {
            Self::Scalar(v) => Some(v.type_tag()),
            Self::Array(v) => Some(v.type_tag()),
            Self::Record(_) => None,
        }
    }
}

impl From<ScalarValue> for Value {
    fn from(value: ScalarValue) -> Self {
        Self::Scalar(value)
    }
}

impl From<ArrayValue> for Value {
    fn from(value: ArrayValue) -> Self {
        Self::Array(value)
    }
}

impl From<RecordValue> for Value {
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
}
