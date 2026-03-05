// SPDX-License-Identifier: LGPL-3.0-or-later
pub mod table_interop;

#[cfg(has_casacore_cpp)]
use std::ffi::CStr;

use casacore_aipsio::{
    AipsReader, AipsWriter, ArrayValue, ByteOrder, Complex32, Complex64, ScalarValue, TypeTag,
    Value,
};
#[cfg(has_casacore_cpp)]
use casacore_aipsio::{PrimitiveType, ValueRank};
use ndarray::{ArrayD, IxDyn};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AipsIoCrossError {
    #[error("C++ casacore backend is unavailable (pkg-config casacore not found at build time)")]
    CppUnavailable,
    #[error("unsupported value for primitive AipsIO cross-check: {0}")]
    UnsupportedValue(&'static str),
    #[error("value has no primitive type tag")]
    MissingTypeTag,
    #[error("rust backend error during {stage}: {message}")]
    RustBackend {
        stage: &'static str,
        message: String,
    },
    #[error("cpp backend error during {stage}: {message}")]
    CppBackend {
        stage: &'static str,
        message: String,
    },
    #[error("wire mismatch for {label} with {byte_order:?}")]
    WireMismatch {
        label: String,
        byte_order: ByteOrder,
    },
    #[error("decode mismatch for {path} ({label}) with {byte_order:?}")]
    DecodeMismatch {
        path: &'static str,
        label: String,
        byte_order: ByteOrder,
    },
}

pub trait AipsIoBackend {
    fn name(&self) -> &'static str;

    fn encode_value(&self, value: &Value, byte_order: ByteOrder) -> Result<Vec<u8>, String>;

    fn decode_value(
        &self,
        bytes: &[u8],
        type_tag: TypeTag,
        byte_order: ByteOrder,
    ) -> Result<Value, String>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RustBackend;

#[derive(Debug, Clone, Copy, Default)]
pub struct CppBackend;

impl RustBackend {
    pub fn new() -> Self {
        Self
    }
}

impl CppBackend {
    pub fn new() -> Self {
        Self
    }
}

impl AipsIoBackend for RustBackend {
    fn name(&self) -> &'static str {
        "rust"
    }

    fn encode_value(&self, value: &Value, byte_order: ByteOrder) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        let mut writer = AipsWriter::with_byte_order(&mut bytes, byte_order);
        writer
            .write_value(value)
            .map_err(|err| format!("write_value failed: {err}"))?;
        Ok(bytes)
    }

    fn decode_value(
        &self,
        bytes: &[u8],
        type_tag: TypeTag,
        byte_order: ByteOrder,
    ) -> Result<Value, String> {
        let mut reader = AipsReader::with_byte_order(bytes, byte_order);
        reader
            .read_value(type_tag)
            .map_err(|err| format!("read_value failed: {err}"))
    }
}

impl AipsIoBackend for CppBackend {
    fn name(&self) -> &'static str {
        "cpp"
    }

    fn encode_value(&self, value: &Value, byte_order: ByteOrder) -> Result<Vec<u8>, String> {
        #[cfg(has_casacore_cpp)]
        {
            cpp_encode_value(value, byte_order)
        }
        #[cfg(not(has_casacore_cpp))]
        {
            let _ = (value, byte_order);
            Err("casacore C++ backend unavailable".to_string())
        }
    }

    fn decode_value(
        &self,
        bytes: &[u8],
        type_tag: TypeTag,
        byte_order: ByteOrder,
    ) -> Result<Value, String> {
        #[cfg(has_casacore_cpp)]
        {
            cpp_decode_value(bytes, type_tag, byte_order)
        }
        #[cfg(not(has_casacore_cpp))]
        {
            let _ = (bytes, type_tag, byte_order);
            Err("casacore C++ backend unavailable".to_string())
        }
    }
}

pub fn cpp_backend_available() -> bool {
    cfg!(has_casacore_cpp)
}

pub fn primitive_cross_check_values() -> Vec<Value> {
    vec![
        Value::Scalar(ScalarValue::Bool(true)),
        Value::Scalar(ScalarValue::Int16(-1234)),
        Value::Scalar(ScalarValue::Int32(-1_234_567)),
        Value::Scalar(ScalarValue::Int64(-9_876_543_210)),
        Value::Scalar(ScalarValue::Float32(3.5)),
        Value::Scalar(ScalarValue::Float64(-10.25)),
        Value::Scalar(ScalarValue::Complex32(Complex32 { re: 1.5, im: -2.25 })),
        Value::Scalar(ScalarValue::Complex64(Complex64 { re: 0.5, im: -0.75 })),
        Value::Scalar(ScalarValue::String("alpha".to_string())),
        Value::Array(ArrayValue::from_bool_vec(vec![true, false, true])),
        Value::Array(ArrayValue::from_i16_vec(vec![1, -2, 3])),
        Value::Array(ArrayValue::from_i32_vec(vec![10, -20, 30, -40])),
        Value::Array(ArrayValue::from_i64_vec(vec![100, -200, 300, -400])),
        Value::Array(ArrayValue::from_f32_vec(vec![1.0, -2.5, 3.25])),
        Value::Array(ArrayValue::from_f64_vec(vec![1.0, -2.5, 3.25, -4.125])),
        Value::Array(ArrayValue::from_complex32_vec(vec![
            Complex32 { re: 1.0, im: 2.0 },
            Complex32 { re: -3.0, im: -4.0 },
        ])),
        Value::Array(ArrayValue::from_complex64_vec(vec![
            Complex64 { re: 1.0, im: 2.0 },
            Complex64 { re: -3.0, im: -4.5 },
        ])),
        Value::Array(ArrayValue::from_string_vec(vec![
            "a".to_string(),
            "bc".to_string(),
            "def".to_string(),
        ])),
        Value::Array(ArrayValue::from_string_vec(vec![])),
        Value::Array(ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![0, 1, 2, 3, 4, 5]).expect("shape"),
        )),
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2, 2]),
                vec![0.0, 1.0, 10.0, 11.0, 100.0, 101.0, 110.0, 111.0],
            )
            .expect("shape"),
        )),
        Value::Array(ArrayValue::String(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    "r0c0".to_string(),
                    "r0c1".to_string(),
                    "r1c0".to_string(),
                    "r1c1".to_string(),
                ],
            )
            .expect("shape"),
        )),
    ]
}

#[derive(Debug, Clone)]
struct PreparedPrimitiveCase {
    wire_value: Value,
    expected_value: Value,
    original_shape: Option<Vec<usize>>,
}

fn prepare_primitive_case(value: &Value) -> Result<PreparedPrimitiveCase, AipsIoCrossError> {
    match value {
        Value::Scalar(_) => Ok(PreparedPrimitiveCase {
            wire_value: value.clone(),
            expected_value: value.clone(),
            original_shape: None,
        }),
        Value::Array(array) => {
            if array.ndim() <= 1 {
                Ok(PreparedPrimitiveCase {
                    wire_value: value.clone(),
                    expected_value: value.clone(),
                    original_shape: None,
                })
            } else {
                Ok(PreparedPrimitiveCase {
                    wire_value: Value::Array(flatten_array_value_fortran(array)),
                    expected_value: value.clone(),
                    original_shape: Some(array.shape().to_vec()),
                })
            }
        }
        Value::Record(_) => Err(AipsIoCrossError::UnsupportedValue(
            "record values are not part of primitive AipsIO cross-checks",
        )),
    }
}

fn restore_decoded_shape(
    decoded: Value,
    original_shape: Option<&[usize]>,
) -> Result<Value, AipsIoCrossError> {
    let Some(shape) = original_shape else {
        return Ok(decoded);
    };
    match decoded {
        Value::Array(array) => Ok(Value::Array(reshape_array_value_from_fortran(
            array, shape,
        )?)),
        _ => Err(AipsIoCrossError::UnsupportedValue(
            "decoded value was expected to be an array",
        )),
    }
}

fn flatten_array_value_fortran(array: &ArrayValue) -> ArrayValue {
    match array {
        ArrayValue::Bool(values) => ArrayValue::from_bool_vec(flatten_ndarray_fortran(values)),
        ArrayValue::UInt8(values) => ArrayValue::from_u8_vec(flatten_ndarray_fortran(values)),
        ArrayValue::UInt16(values) => ArrayValue::from_u16_vec(flatten_ndarray_fortran(values)),
        ArrayValue::UInt32(values) => ArrayValue::from_u32_vec(flatten_ndarray_fortran(values)),
        ArrayValue::Int16(values) => ArrayValue::from_i16_vec(flatten_ndarray_fortran(values)),
        ArrayValue::Int32(values) => ArrayValue::from_i32_vec(flatten_ndarray_fortran(values)),
        ArrayValue::Int64(values) => ArrayValue::from_i64_vec(flatten_ndarray_fortran(values)),
        ArrayValue::Float32(values) => ArrayValue::from_f32_vec(flatten_ndarray_fortran(values)),
        ArrayValue::Float64(values) => ArrayValue::from_f64_vec(flatten_ndarray_fortran(values)),
        ArrayValue::Complex32(values) => {
            ArrayValue::from_complex32_vec(flatten_ndarray_fortran(values))
        }
        ArrayValue::Complex64(values) => {
            ArrayValue::from_complex64_vec(flatten_ndarray_fortran(values))
        }
        ArrayValue::String(values) => ArrayValue::from_string_vec(flatten_ndarray_fortran(values)),
    }
}

fn reshape_array_value_from_fortran(
    array: ArrayValue,
    shape: &[usize],
) -> Result<ArrayValue, AipsIoCrossError> {
    match array {
        ArrayValue::Bool(values) => Ok(ArrayValue::Bool(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::UInt8(values) => Ok(ArrayValue::UInt8(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::UInt16(values) => Ok(ArrayValue::UInt16(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::UInt32(values) => Ok(ArrayValue::UInt32(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Int16(values) => Ok(ArrayValue::Int16(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Int32(values) => Ok(ArrayValue::Int32(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Int64(values) => Ok(ArrayValue::Int64(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Float32(values) => Ok(ArrayValue::Float32(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Float64(values) => Ok(ArrayValue::Float64(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Complex32(values) => Ok(ArrayValue::Complex32(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::Complex64(values) => Ok(ArrayValue::Complex64(reshape_from_fortran(
            &values.iter().copied().collect::<Vec<_>>(),
            shape,
        )?)),
        ArrayValue::String(values) => Ok(ArrayValue::String(reshape_from_fortran(
            &values.iter().cloned().collect::<Vec<_>>(),
            shape,
        )?)),
    }
}

fn flatten_ndarray_fortran<T: Clone>(array: &ArrayD<T>) -> Vec<T> {
    let shape = array.shape();
    let mut out = Vec::with_capacity(array.len());
    for linear in 0..array.len() {
        let idx = unravel_fortran_index(linear, shape);
        out.push(array[IxDyn(&idx)].clone());
    }
    out
}

fn reshape_from_fortran<T: Clone>(
    fortran_values: &[T],
    shape: &[usize],
) -> Result<ArrayD<T>, AipsIoCrossError> {
    let expected_len = shape.iter().try_fold(1usize, |acc, &dim| {
        acc.checked_mul(dim)
            .ok_or(AipsIoCrossError::UnsupportedValue("array shape overflow"))
    })?;

    if expected_len != fortran_values.len() {
        return Err(AipsIoCrossError::UnsupportedValue(
            "decoded array length does not match expected shape",
        ));
    }

    let mut c_values = Vec::with_capacity(expected_len);
    for c_linear in 0..expected_len {
        let idx = unravel_c_index(c_linear, shape);
        let f_linear = ravel_fortran_index(&idx, shape);
        c_values.push(fortran_values[f_linear].clone());
    }

    ArrayD::from_shape_vec(IxDyn(shape), c_values)
        .map_err(|_| AipsIoCrossError::UnsupportedValue("failed to reshape decoded array"))
}

fn unravel_fortran_index(mut linear: usize, shape: &[usize]) -> Vec<usize> {
    let mut idx = Vec::with_capacity(shape.len());
    for &dim in shape {
        idx.push(linear % dim);
        linear /= dim;
    }
    idx
}

fn unravel_c_index(mut linear: usize, shape: &[usize]) -> Vec<usize> {
    let mut idx = vec![0usize; shape.len()];
    for axis in (0..shape.len()).rev() {
        let dim = shape[axis];
        idx[axis] = linear % dim;
        linear /= dim;
    }
    idx
}

fn ravel_fortran_index(idx: &[usize], shape: &[usize]) -> usize {
    let mut stride = 1usize;
    let mut linear = 0usize;
    for (axis, &value) in idx.iter().enumerate() {
        linear += value * stride;
        stride *= shape[axis];
    }
    linear
}

pub fn run_aipsio_cross_matrix(values: &[Value]) -> Result<(), AipsIoCrossError> {
    run_aipsio_cross_matrix_with_orders(values, &[ByteOrder::BigEndian, ByteOrder::LittleEndian])
}

pub fn run_aipsio_cross_matrix_with_orders(
    values: &[Value],
    byte_orders: &[ByteOrder],
) -> Result<(), AipsIoCrossError> {
    if !cpp_backend_available() {
        return Err(AipsIoCrossError::CppUnavailable);
    }

    let rust = RustBackend::new();
    let cpp = CppBackend::new();

    for value in values {
        let case = prepare_primitive_case(value)?;
        let label = format!("{value:?}");
        let type_tag = case
            .wire_value
            .type_tag()
            .ok_or(AipsIoCrossError::MissingTypeTag)?;

        for &byte_order in byte_orders {
            let rust_wire = rust
                .encode_value(&case.wire_value, byte_order)
                .map_err(|message| AipsIoCrossError::RustBackend {
                    stage: "encode",
                    message,
                })?;
            let cpp_wire = cpp
                .encode_value(&case.wire_value, byte_order)
                .map_err(|message| AipsIoCrossError::CppBackend {
                    stage: "encode",
                    message,
                })?;

            if rust_wire != cpp_wire {
                return Err(AipsIoCrossError::WireMismatch { label, byte_order });
            }

            let rr = rust
                .decode_value(&rust_wire, type_tag, byte_order)
                .map_err(|message| AipsIoCrossError::RustBackend {
                    stage: "decode rust->rust",
                    message,
                })
                .and_then(|value| restore_decoded_shape(value, case.original_shape.as_deref()))?;
            let rc = cpp
                .decode_value(&rust_wire, type_tag, byte_order)
                .map_err(|message| AipsIoCrossError::CppBackend {
                    stage: "decode rust->cpp",
                    message,
                })
                .and_then(|value| restore_decoded_shape(value, case.original_shape.as_deref()))?;
            let cr = rust
                .decode_value(&cpp_wire, type_tag, byte_order)
                .map_err(|message| AipsIoCrossError::RustBackend {
                    stage: "decode cpp->rust",
                    message,
                })
                .and_then(|value| restore_decoded_shape(value, case.original_shape.as_deref()))?;
            let cc = cpp
                .decode_value(&cpp_wire, type_tag, byte_order)
                .map_err(|message| AipsIoCrossError::CppBackend {
                    stage: "decode cpp->cpp",
                    message,
                })
                .and_then(|value| restore_decoded_shape(value, case.original_shape.as_deref()))?;

            if rr != case.expected_value {
                return Err(AipsIoCrossError::DecodeMismatch {
                    path: "rust->rust",
                    label: format!("{value:?}"),
                    byte_order,
                });
            }
            if rc != case.expected_value {
                return Err(AipsIoCrossError::DecodeMismatch {
                    path: "rust->cpp",
                    label: format!("{value:?}"),
                    byte_order,
                });
            }
            if cr != case.expected_value {
                return Err(AipsIoCrossError::DecodeMismatch {
                    path: "cpp->rust",
                    label: format!("{value:?}"),
                    byte_order,
                });
            }
            if cc != case.expected_value {
                return Err(AipsIoCrossError::DecodeMismatch {
                    path: "cpp->cpp",
                    label: format!("{value:?}"),
                    byte_order,
                });
            }
        }
    }

    Ok(())
}

#[cfg(has_casacore_cpp)]
#[derive(Debug)]
struct FfiPayload {
    primitive: PrimitiveType,
    is_array: bool,
    payload: Vec<u8>,
    offsets: Vec<u32>,
}

#[cfg(has_casacore_cpp)]
fn primitive_to_tag(primitive: PrimitiveType) -> u8 {
    match primitive {
        PrimitiveType::Bool => 0,
        PrimitiveType::Int16 => 1,
        PrimitiveType::Int32 => 2,
        PrimitiveType::Int64 => 3,
        PrimitiveType::Float32 => 4,
        PrimitiveType::Float64 => 5,
        PrimitiveType::Complex32 => 6,
        PrimitiveType::Complex64 => 7,
        PrimitiveType::String => 8,
        PrimitiveType::UInt8 => 9,
        PrimitiveType::UInt16 => 10,
        PrimitiveType::UInt32 => 11,
    }
}

#[cfg(has_casacore_cpp)]
fn byte_order_to_tag(byte_order: ByteOrder) -> u8 {
    match byte_order {
        ByteOrder::BigEndian => 0,
        ByteOrder::LittleEndian => 1,
    }
}

#[cfg(has_casacore_cpp)]
fn value_to_payload(value: &Value) -> Result<FfiPayload, AipsIoCrossError> {
    match value {
        Value::Scalar(s) => Ok(match s {
            ScalarValue::Bool(v) => FfiPayload {
                primitive: PrimitiveType::Bool,
                is_array: false,
                payload: vec![u8::from(*v)],
                offsets: vec![],
            },
            ScalarValue::UInt8(v) => FfiPayload {
                primitive: PrimitiveType::UInt8,
                is_array: false,
                payload: vec![*v],
                offsets: vec![],
            },
            ScalarValue::UInt16(v) => FfiPayload {
                primitive: PrimitiveType::UInt16,
                is_array: false,
                payload: v.to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::UInt32(v) => FfiPayload {
                primitive: PrimitiveType::UInt32,
                is_array: false,
                payload: v.to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::Int16(v) => FfiPayload {
                primitive: PrimitiveType::Int16,
                is_array: false,
                payload: v.to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::Int32(v) => FfiPayload {
                primitive: PrimitiveType::Int32,
                is_array: false,
                payload: v.to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::Int64(v) => FfiPayload {
                primitive: PrimitiveType::Int64,
                is_array: false,
                payload: v.to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::Float32(v) => FfiPayload {
                primitive: PrimitiveType::Float32,
                is_array: false,
                payload: v.to_bits().to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::Float64(v) => FfiPayload {
                primitive: PrimitiveType::Float64,
                is_array: false,
                payload: v.to_bits().to_le_bytes().to_vec(),
                offsets: vec![],
            },
            ScalarValue::Complex32(v) => {
                let mut payload = Vec::with_capacity(8);
                payload.extend_from_slice(&v.re.to_bits().to_le_bytes());
                payload.extend_from_slice(&v.im.to_bits().to_le_bytes());
                FfiPayload {
                    primitive: PrimitiveType::Complex32,
                    is_array: false,
                    payload,
                    offsets: vec![],
                }
            }
            ScalarValue::Complex64(v) => {
                let mut payload = Vec::with_capacity(16);
                payload.extend_from_slice(&v.re.to_bits().to_le_bytes());
                payload.extend_from_slice(&v.im.to_bits().to_le_bytes());
                FfiPayload {
                    primitive: PrimitiveType::Complex64,
                    is_array: false,
                    payload,
                    offsets: vec![],
                }
            }
            ScalarValue::String(v) => FfiPayload {
                primitive: PrimitiveType::String,
                is_array: false,
                payload: v.as_bytes().to_vec(),
                offsets: vec![0, v.len() as u32],
            },
        }),
        Value::Array(arr) => {
            if arr.ndim() != 1 {
                return Err(AipsIoCrossError::UnsupportedValue(
                    "cross-check supports rank-1 arrays only",
                ));
            }

            Ok(match arr {
                ArrayValue::Bool(values) => FfiPayload {
                    primitive: PrimitiveType::Bool,
                    is_array: true,
                    payload: values.iter().map(|v| u8::from(*v)).collect(),
                    offsets: vec![],
                },
                ArrayValue::UInt8(values) => FfiPayload {
                    primitive: PrimitiveType::UInt8,
                    is_array: true,
                    payload: values.iter().copied().collect(),
                    offsets: vec![],
                },
                ArrayValue::UInt16(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 2);
                    for value in values {
                        payload.extend_from_slice(&value.to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::UInt16,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::UInt32(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 4);
                    for value in values {
                        payload.extend_from_slice(&value.to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::UInt32,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Int16(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 2);
                    for value in values {
                        payload.extend_from_slice(&value.to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Int16,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Int32(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 4);
                    for value in values {
                        payload.extend_from_slice(&value.to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Int32,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Int64(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 8);
                    for value in values {
                        payload.extend_from_slice(&value.to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Int64,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Float32(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 4);
                    for value in values {
                        payload.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Float32,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Float64(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 8);
                    for value in values {
                        payload.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Float64,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Complex32(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 8);
                    for value in values {
                        payload.extend_from_slice(&value.re.to_bits().to_le_bytes());
                        payload.extend_from_slice(&value.im.to_bits().to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Complex32,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::Complex64(values) => {
                    let mut payload = Vec::with_capacity(values.len() * 16);
                    for value in values {
                        payload.extend_from_slice(&value.re.to_bits().to_le_bytes());
                        payload.extend_from_slice(&value.im.to_bits().to_le_bytes());
                    }
                    FfiPayload {
                        primitive: PrimitiveType::Complex64,
                        is_array: true,
                        payload,
                        offsets: vec![],
                    }
                }
                ArrayValue::String(values) => {
                    let mut payload = Vec::new();
                    let mut offsets = Vec::with_capacity(values.len() + 1);
                    offsets.push(0);
                    let mut cumulative = 0_u32;
                    for value in values {
                        payload.extend_from_slice(value.as_bytes());
                        cumulative = cumulative.checked_add(value.len() as u32).ok_or(
                            AipsIoCrossError::UnsupportedValue("string payload too large"),
                        )?;
                        offsets.push(cumulative);
                    }
                    FfiPayload {
                        primitive: PrimitiveType::String,
                        is_array: true,
                        payload,
                        offsets,
                    }
                }
            })
        }
        Value::Record(_) => Err(AipsIoCrossError::UnsupportedValue(
            "record values are not part of primitive AipsIO cross-checks",
        )),
    }
}

#[cfg(has_casacore_cpp)]
fn read_u16_le(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

#[cfg(has_casacore_cpp)]
fn read_u32_le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

#[cfg(has_casacore_cpp)]
fn read_u64_le(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}

#[cfg(has_casacore_cpp)]
fn payload_to_value(
    primitive: PrimitiveType,
    is_array: bool,
    payload: &[u8],
    offsets: &[u32],
) -> Result<Value, String> {
    if is_array {
        let array = match primitive {
            PrimitiveType::Bool => {
                ArrayValue::from_bool_vec(payload.iter().map(|b| *b != 0).collect())
            }
            PrimitiveType::UInt8 => ArrayValue::from_u8_vec(payload.to_vec()),
            PrimitiveType::UInt16 => {
                if payload.len() % 2 != 0 {
                    return Err("invalid uint16 payload length".to_string());
                }
                let values = (0..payload.len() / 2)
                    .map(|i| u16::from_le_bytes([payload[2 * i], payload[2 * i + 1]]))
                    .collect();
                ArrayValue::from_u16_vec(values)
            }
            PrimitiveType::UInt32 => {
                if payload.len() % 4 != 0 {
                    return Err("invalid uint32 payload length".to_string());
                }
                let values = (0..payload.len() / 4)
                    .map(|i| {
                        u32::from_le_bytes([
                            payload[4 * i],
                            payload[4 * i + 1],
                            payload[4 * i + 2],
                            payload[4 * i + 3],
                        ])
                    })
                    .collect();
                ArrayValue::from_u32_vec(values)
            }
            PrimitiveType::Int16 => {
                if payload.len() % 2 != 0 {
                    return Err("invalid int16 payload length".to_string());
                }
                let values = (0..payload.len() / 2)
                    .map(|i| i16::from_le_bytes([payload[2 * i], payload[2 * i + 1]]))
                    .collect();
                ArrayValue::from_i16_vec(values)
            }
            PrimitiveType::Int32 => {
                if payload.len() % 4 != 0 {
                    return Err("invalid int32 payload length".to_string());
                }
                let values = (0..payload.len() / 4)
                    .map(|i| {
                        i32::from_le_bytes([
                            payload[4 * i],
                            payload[4 * i + 1],
                            payload[4 * i + 2],
                            payload[4 * i + 3],
                        ])
                    })
                    .collect();
                ArrayValue::from_i32_vec(values)
            }
            PrimitiveType::Int64 => {
                if payload.len() % 8 != 0 {
                    return Err("invalid int64 payload length".to_string());
                }
                let values = (0..payload.len() / 8)
                    .map(|i| {
                        i64::from_le_bytes([
                            payload[8 * i],
                            payload[8 * i + 1],
                            payload[8 * i + 2],
                            payload[8 * i + 3],
                            payload[8 * i + 4],
                            payload[8 * i + 5],
                            payload[8 * i + 6],
                            payload[8 * i + 7],
                        ])
                    })
                    .collect();
                ArrayValue::from_i64_vec(values)
            }
            PrimitiveType::Float32 => {
                if payload.len() % 4 != 0 {
                    return Err("invalid float32 payload length".to_string());
                }
                let values = (0..payload.len() / 4)
                    .map(|i| {
                        f32::from_bits(u32::from_le_bytes([
                            payload[4 * i],
                            payload[4 * i + 1],
                            payload[4 * i + 2],
                            payload[4 * i + 3],
                        ]))
                    })
                    .collect();
                ArrayValue::from_f32_vec(values)
            }
            PrimitiveType::Float64 => {
                if payload.len() % 8 != 0 {
                    return Err("invalid float64 payload length".to_string());
                }
                let values = (0..payload.len() / 8)
                    .map(|i| {
                        f64::from_bits(u64::from_le_bytes([
                            payload[8 * i],
                            payload[8 * i + 1],
                            payload[8 * i + 2],
                            payload[8 * i + 3],
                            payload[8 * i + 4],
                            payload[8 * i + 5],
                            payload[8 * i + 6],
                            payload[8 * i + 7],
                        ]))
                    })
                    .collect();
                ArrayValue::from_f64_vec(values)
            }
            PrimitiveType::Complex32 => {
                if payload.len() % 8 != 0 {
                    return Err("invalid complex32 payload length".to_string());
                }
                let values = (0..payload.len() / 8)
                    .map(|i| {
                        let re = f32::from_bits(u32::from_le_bytes([
                            payload[8 * i],
                            payload[8 * i + 1],
                            payload[8 * i + 2],
                            payload[8 * i + 3],
                        ]));
                        let im = f32::from_bits(u32::from_le_bytes([
                            payload[8 * i + 4],
                            payload[8 * i + 5],
                            payload[8 * i + 6],
                            payload[8 * i + 7],
                        ]));
                        Complex32 { re, im }
                    })
                    .collect();
                ArrayValue::from_complex32_vec(values)
            }
            PrimitiveType::Complex64 => {
                if payload.len() % 16 != 0 {
                    return Err("invalid complex64 payload length".to_string());
                }
                let values = (0..payload.len() / 16)
                    .map(|i| {
                        let re = f64::from_bits(u64::from_le_bytes([
                            payload[16 * i],
                            payload[16 * i + 1],
                            payload[16 * i + 2],
                            payload[16 * i + 3],
                            payload[16 * i + 4],
                            payload[16 * i + 5],
                            payload[16 * i + 6],
                            payload[16 * i + 7],
                        ]));
                        let im = f64::from_bits(u64::from_le_bytes([
                            payload[16 * i + 8],
                            payload[16 * i + 9],
                            payload[16 * i + 10],
                            payload[16 * i + 11],
                            payload[16 * i + 12],
                            payload[16 * i + 13],
                            payload[16 * i + 14],
                            payload[16 * i + 15],
                        ]));
                        Complex64 { re, im }
                    })
                    .collect();
                ArrayValue::from_complex64_vec(values)
            }
            PrimitiveType::String => {
                if offsets.is_empty() || offsets[0] != 0 {
                    return Err("invalid string offsets".to_string());
                }
                let mut values = Vec::with_capacity(offsets.len().saturating_sub(1));
                for i in 0..offsets.len() - 1 {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    if start > end || end > payload.len() {
                        return Err("invalid string offset range".to_string());
                    }
                    let s = String::from_utf8(payload[start..end].to_vec())
                        .map_err(|e| format!("invalid utf8 in string array: {e}"))?;
                    values.push(s);
                }
                ArrayValue::from_string_vec(values)
            }
        };
        Ok(Value::Array(array))
    } else {
        let scalar = match primitive {
            PrimitiveType::Bool => {
                if payload.len() != 1 {
                    return Err("bool scalar payload length must be 1".to_string());
                }
                ScalarValue::Bool(payload[0] != 0)
            }
            PrimitiveType::UInt8 => {
                if payload.len() != 1 {
                    return Err("uint8 scalar payload length must be 1".to_string());
                }
                ScalarValue::UInt8(payload[0])
            }
            PrimitiveType::UInt16 => {
                if payload.len() != 2 {
                    return Err("uint16 scalar payload length must be 2".to_string());
                }
                ScalarValue::UInt16(read_u16_le(payload, 0))
            }
            PrimitiveType::UInt32 => {
                if payload.len() != 4 {
                    return Err("uint32 scalar payload length must be 4".to_string());
                }
                ScalarValue::UInt32(read_u32_le(payload, 0))
            }
            PrimitiveType::Int16 => {
                if payload.len() != 2 {
                    return Err("int16 scalar payload length must be 2".to_string());
                }
                ScalarValue::Int16(read_u16_le(payload, 0) as i16)
            }
            PrimitiveType::Int32 => {
                if payload.len() != 4 {
                    return Err("int32 scalar payload length must be 4".to_string());
                }
                ScalarValue::Int32(read_u32_le(payload, 0) as i32)
            }
            PrimitiveType::Int64 => {
                if payload.len() != 8 {
                    return Err("int64 scalar payload length must be 8".to_string());
                }
                ScalarValue::Int64(read_u64_le(payload, 0) as i64)
            }
            PrimitiveType::Float32 => {
                if payload.len() != 4 {
                    return Err("float32 scalar payload length must be 4".to_string());
                }
                ScalarValue::Float32(f32::from_bits(read_u32_le(payload, 0)))
            }
            PrimitiveType::Float64 => {
                if payload.len() != 8 {
                    return Err("float64 scalar payload length must be 8".to_string());
                }
                ScalarValue::Float64(f64::from_bits(read_u64_le(payload, 0)))
            }
            PrimitiveType::Complex32 => {
                if payload.len() != 8 {
                    return Err("complex32 scalar payload length must be 8".to_string());
                }
                ScalarValue::Complex32(Complex32 {
                    re: f32::from_bits(read_u32_le(payload, 0)),
                    im: f32::from_bits(read_u32_le(payload, 4)),
                })
            }
            PrimitiveType::Complex64 => {
                if payload.len() != 16 {
                    return Err("complex64 scalar payload length must be 16".to_string());
                }
                ScalarValue::Complex64(Complex64 {
                    re: f64::from_bits(read_u64_le(payload, 0)),
                    im: f64::from_bits(read_u64_le(payload, 8)),
                })
            }
            PrimitiveType::String => {
                let text = String::from_utf8(payload.to_vec())
                    .map_err(|e| format!("invalid utf8 in scalar string: {e}"))?;
                ScalarValue::String(text)
            }
        };
        Ok(Value::Scalar(scalar))
    }
}

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn cpp_table_write_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_mutation_removed_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_mutation_removed_rows(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_mutation_added_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_with_lock(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_with_lock(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_sorted_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_sorted_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_concat_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_concat_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_deep_copy(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_deep_copy(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_free_error(ptr: *mut std::ffi::c_char);
    fn cpp_table_write_tiled_column_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_tiled_column_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_tiled_shape_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_tiled_shape_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_tiled_cell_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_tiled_cell_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_columns_index_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_forward_column_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_forward_column_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_scaled_array_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_scaled_array_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_columns_index_time_lookups"]
    fn ffi_columns_index_time_lookups(
        path: *const std::ffi::c_char,
        key_value: i32,
        nqueries: u64,
        out_elapsed_ns: *mut u64,
        out_match_count: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_write_ism_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ism_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ism_slowly_changing(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ism_slowly_changing(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ism_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ism_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_write_undefined_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_undefined_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_column_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_column_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_record_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_record_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_mixed_schema(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_mixed_schema(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_write_aipsio_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_aipsio_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_write_table_info(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_table_info(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_write_aipsio_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_aipsio_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_aipsio_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_aipsio_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_aipsio_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_aipsio_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_aipsio_3d_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_aipsio_3d_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_write_ssm_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_verify_ssm_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_vararray_bench_write_read(
        path: *const std::ffi::c_char,
        nrows: u64,
        out_write_ns: *mut u64,
        out_read_ns: *mut u64,
        out_total_elems: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_set_algebra_bench"]
    fn ffi_set_algebra_bench(
        path: *const std::ffi::c_char,
        nrows: u64,
        split_a: u64,
        split_b: u64,
        out_union_ns: *mut u64,
        out_intersection_ns: *mut u64,
        out_difference_ns: *mut u64,
        out_union_rows: *mut u64,
        out_intersection_rows: *mut u64,
        out_difference_rows: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_copy_rows_bench"]
    fn ffi_copy_rows_bench(
        dir: *const std::ffi::c_char,
        nrows: u64,
        out_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_cell_slice_bench"]
    fn ffi_cell_slice_bench(
        path: *const std::ffi::c_char,
        nrows: u64,
        dim0: i64,
        dim1: i64,
        slice_start0: i64,
        slice_start1: i64,
        slice_end0: i64,
        slice_end1: i64,
        out_write_ns: *mut u64,
        out_slice_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn casacore_cpp_aipsio_encode(
        primitive: u8,
        is_array: u8,
        byte_order: u8,
        payload_ptr: *const u8,
        payload_len: usize,
        offsets_ptr: *const u32,
        offsets_len: usize,
        out_wire_ptr: *mut *mut u8,
        out_wire_len: *mut usize,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn casacore_cpp_aipsio_decode(
        primitive: u8,
        is_array: u8,
        byte_order: u8,
        wire_ptr: *const u8,
        wire_len: usize,
        out_payload_ptr: *mut *mut u8,
        out_payload_len: *mut usize,
        out_offsets_ptr: *mut *mut u32,
        out_offsets_len: *mut usize,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn casacore_cpp_aipsio_free_bytes(ptr: *mut u8);
    fn casacore_cpp_aipsio_free_offsets(ptr: *mut u32);
    fn casacore_cpp_aipsio_free_error(ptr: *mut std::ffi::c_char);
}

#[cfg(has_casacore_cpp)]
fn copy_ffi_bytes(ptr: *mut u8, len: usize) -> Vec<u8> {
    if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len).to_vec() }
    }
}

#[cfg(has_casacore_cpp)]
fn copy_ffi_offsets(ptr: *mut u32, len: usize) -> Vec<u32> {
    if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len).to_vec() }
    }
}

#[cfg(has_casacore_cpp)]
fn cpp_encode_value(value: &Value, byte_order: ByteOrder) -> Result<Vec<u8>, String> {
    let ffi = value_to_payload(value).map_err(|e| e.to_string())?;
    let primitive = primitive_to_tag(ffi.primitive);
    let is_array = u8::from(ffi.is_array);
    let byte_order = byte_order_to_tag(byte_order);

    let mut out_ptr: *mut u8 = std::ptr::null_mut();
    let mut out_len: usize = 0;
    let mut out_err: *mut std::ffi::c_char = std::ptr::null_mut();

    let status = unsafe {
        casacore_cpp_aipsio_encode(
            primitive,
            is_array,
            byte_order,
            ffi.payload.as_ptr(),
            ffi.payload.len(),
            ffi.offsets.as_ptr(),
            ffi.offsets.len(),
            &mut out_ptr,
            &mut out_len,
            &mut out_err,
        )
    };

    if status != 0 {
        let err = if out_err.is_null() {
            "unknown C++ encode error".to_string()
        } else {
            let msg = unsafe { CStr::from_ptr(out_err).to_string_lossy().into_owned() };
            unsafe { casacore_cpp_aipsio_free_error(out_err) };
            msg
        };
        return Err(err);
    }

    let out = copy_ffi_bytes(out_ptr, out_len);
    unsafe { casacore_cpp_aipsio_free_bytes(out_ptr) };
    Ok(out)
}

#[cfg(has_casacore_cpp)]
fn cpp_decode_value(
    bytes: &[u8],
    type_tag: TypeTag,
    byte_order: ByteOrder,
) -> Result<Value, String> {
    let primitive = primitive_to_tag(type_tag.primitive);
    let is_array = u8::from(matches!(type_tag.rank, ValueRank::Array));
    let byte_order = byte_order_to_tag(byte_order);

    let mut out_payload_ptr: *mut u8 = std::ptr::null_mut();
    let mut out_payload_len: usize = 0;
    let mut out_offsets_ptr: *mut u32 = std::ptr::null_mut();
    let mut out_offsets_len: usize = 0;
    let mut out_err: *mut std::ffi::c_char = std::ptr::null_mut();

    let status = unsafe {
        casacore_cpp_aipsio_decode(
            primitive,
            is_array,
            byte_order,
            bytes.as_ptr(),
            bytes.len(),
            &mut out_payload_ptr,
            &mut out_payload_len,
            &mut out_offsets_ptr,
            &mut out_offsets_len,
            &mut out_err,
        )
    };

    if status != 0 {
        let err = if out_err.is_null() {
            "unknown C++ decode error".to_string()
        } else {
            let msg = unsafe { CStr::from_ptr(out_err).to_string_lossy().into_owned() };
            unsafe { casacore_cpp_aipsio_free_error(out_err) };
            msg
        };
        return Err(err);
    }

    let payload = copy_ffi_bytes(out_payload_ptr, out_payload_len);
    let offsets = copy_ffi_offsets(out_offsets_ptr, out_offsets_len);
    unsafe {
        casacore_cpp_aipsio_free_bytes(out_payload_ptr);
        casacore_cpp_aipsio_free_offsets(out_offsets_ptr);
    }

    payload_to_value(type_tag.primitive, is_array != 0, &payload, &offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_case_set_is_non_empty() {
        let values = primitive_cross_check_values();
        assert!(!values.is_empty());
        assert!(values.iter().all(|v| v.type_tag().is_some()));
    }

    #[test]
    fn multidimensional_cases_use_fortran_linearization() {
        let original = Value::Array(ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![0, 1, 2, 3, 4, 5]).expect("shape"),
        ));

        let case = prepare_primitive_case(&original).expect("prepare case");
        let Value::Array(ArrayValue::Int32(flattened)) = case.wire_value.clone() else {
            panic!("expected int32 array wire case");
        };
        let flattened_vec: Vec<i32> = flattened.iter().copied().collect();
        assert_eq!(flattened_vec, vec![0, 3, 1, 4, 2, 5]);

        let restored = restore_decoded_shape(case.wire_value, case.original_shape.as_deref())
            .expect("restore");
        assert_eq!(restored, original);
    }

    #[test]
    fn rust_backend_round_trip_for_primitive_cases() {
        let backend = RustBackend::new();
        for value in primitive_cross_check_values() {
            let case = prepare_primitive_case(&value).expect("case should be supported");
            let tag = case.wire_value.type_tag().expect("primitive case has tag");
            for order in [ByteOrder::BigEndian, ByteOrder::LittleEndian] {
                let wire = backend
                    .encode_value(&case.wire_value, order)
                    .expect("rust encode should succeed");
                let decoded = backend
                    .decode_value(&wire, tag, order)
                    .expect("rust decode should succeed");
                let decoded = restore_decoded_shape(decoded, case.original_shape.as_deref())
                    .expect("restore decoded shape");
                assert_eq!(decoded, case.expected_value);
            }
        }
    }
}

// ===== Safe wrappers for C++ table shim =====

/// Fixture identifiers for C++ table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppTableFixture {
    ScalarPrimitives,
    FixedArray,
    Keywords,
    SsmScalarPrimitives,
    SsmFixedArray,
    SsmKeywords,
    /// Verify-only: scalar_primitives with col_str removed (any DM).
    MutationRemovedColumn,
    /// Verify-only: scalar_primitives with row 1 removed (any DM).
    MutationRemovedRows,
    /// Verify-only: scalar_primitives + extra(Float32, 42.0) column (any DM).
    MutationAddedColumn,
    /// Lock interop: table with (id: Int, name: String), created with
    /// PermanentLocking to produce a `table.lock` file with sync data.
    LockFile,
    /// RefTable interop: parent table with 3 rows (id: Int, name: String)
    /// and a RefTable selecting rows 0 and 2. The path argument is a
    /// directory containing `parent.tbl/` and `ref.tbl/`.
    RefTable,
    /// Sorted RefTable interop: parent table with 5 rows (id: Int, name:
    /// String, value: Float), sorted descending by `id` and saved as a
    /// RefTable. The path argument is a directory containing `parent.tbl/`
    /// and `sorted.tbl/`.
    SortedRefTable,
    /// ConcatTable interop: two 3-row tables (id: Int, name: String) concatenated
    /// as a ConcatTable. The path argument is a directory containing `part0.tbl/`,
    /// `part1.tbl/`, and `concat.tbl/`.
    ConcatTable,
    /// Deep copy interop: a 5-row table deep-copied to a different storage
    /// manager. The path argument is a directory containing `original.tbl/`
    /// and `copy.tbl/`.
    DeepCopy,
    /// ColumnsIndex interop: table with `antenna_id` (Int32), 50 rows,
    /// value = `row_index % 10`. Used to verify `ColumnsIndex` lookups on
    /// C++-written data.
    ColumnsIndex,
    /// ISM scalar primitives: same schema as `SsmScalarPrimitives` (Bool, Int,
    /// Double, String) but stored with `IncrementalStMan`.
    IsmScalarPrimitives,
    /// ISM slowly changing: table with `SCAN_NUMBER` (Int) and `FLAG` (Bool),
    /// 10 rows where values repeat across multiple consecutive rows, exercising
    /// the ISM delta-compression semantics.
    IsmSlowlyChanging,
    /// ISM complex scalars: table with `col_c32` (Complex) and `col_c64`
    /// (DComplex), 3 rows, stored with `IncrementalStMan`.
    IsmComplexScalars,
    /// TiledColumnStMan interop: Fixed-shape Float32 \[2,3\] array column,
    /// 3 rows, tile shape \[2,3,2\].
    TiledColumnStMan,
    /// TiledShapeStMan interop: Variable-shape Float32 array column,
    /// 4 rows with two different shapes (\[2,3\] and \[3,2\]).
    TiledShapeStMan,
    /// TiledCellStMan interop: Variable-shape Float32 array column,
    /// 3 rows each with a unique shape (\[2,3\], \[4,2\], \[3,3\]).
    TiledCellStMan,
    /// ForwardColumnEngine interop: base table with col_value (Double, 3 rows)
    /// and a forwarding table that delegates col_value via ForwardColumnEngine.
    /// The path is the forwarding table directory; base is at `{path}_base`.
    ForwardColumn,
    /// ScaledArrayEngine interop: stored_col (Int array \[2\], 3 rows) and
    /// virtual_col (Double array, via ScaledArrayEngine with scale=2.5, offset=10.0).
    ScaledArray,
    /// AipsIO variable-shape array: Float32 column "data" with ndim=2,
    /// 4 rows with shapes \[2,3\], \[3,2\], \[3,2\], \[2,3\], values 1.0..24.0.
    AipsIOVariableArray,
    /// SSM variable-shape array: same schema and data as `AipsIOVariableArray`
    /// but stored with `StandardStMan`.
    SsmVariableArray,
    /// Undefined scalars: 4-row table (Int, Double, String) where only rows 0
    /// and 2 are written; rows 1 and 3 keep default values (0, 0.0, "").
    UndefinedScalars,
    /// Column keywords: 2-row table (flux: Double, id: Int) with table-level
    /// and per-column keywords. flux has "unit"="Jy" and "ref_frame"="LSRK";
    /// id has "description"="source identifier".
    ColumnKeywords,
    /// Record column: 3-row table (id: Int, meta: Record) with per-row
    /// record values. Row 0: {unit: "Jy", value: 2.5}, Row 1: {flag: true},
    /// Row 2: {} (empty).
    AipsIORecordColumn,
    /// Mixed schema: 2-row table combining scalar (Int, Double), fixed array
    /// (Float32 \[4\]), variable array (Float32 2-D), record column, table
    /// keywords (telescope, version), and column keywords (flux: unit="Jy").
    MixedSchema,
    /// TableInfo metadata: 1-row table (id: Int) with TableInfo set to
    /// type="Measurement", subType="UVFITS". Tests `table.info` file interop.
    TableInfoMetadata,
    /// AipsIO all numeric scalars: 3 rows × 6 cols (uChar, Short, uShort,
    /// uInt, Float, Int64) stored with `StManAipsIO`.
    AipsioAllNumericScalars,
    /// AipsIO complex scalars: 3 rows × 2 cols (Complex, DComplex) stored
    /// with `StManAipsIO`.
    AipsioComplexScalars,
    /// AipsIO typed arrays: 3 rows × 3 cols (Int\[4\], Double\[2,2\], Float32\[3\])
    /// stored with `StManAipsIO`.
    AipsioTypedArrays,
    /// AipsIO 3D fixed array: Float32 \[2,3,4\], 2 rows with ascending values
    /// 1..24 and 25..48, stored with `StManAipsIO`.
    Aipsio3DFixedArray,
    /// SSM all numeric scalars: same as `AipsioAllNumericScalars` but stored
    /// with `StandardStMan`.
    SsmAllNumericScalars,
    /// SSM complex scalars: same as `AipsioComplexScalars` but stored with
    /// `StandardStMan`.
    SsmComplexScalars,
    /// SSM typed arrays: 3 rows × 3 cols (Int\[4\], Double\[2,2\], Complex32\[2\])
    /// stored with `StandardStMan`.
    SsmTypedArrays,
}

/// Write a table fixture using C++ casacore. Returns an error string on failure.
#[cfg(has_casacore_cpp)]
pub fn cpp_table_write(fixture: CppTableFixture, path: &std::path::Path) -> Result<(), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        match fixture {
            CppTableFixture::ScalarPrimitives => {
                cpp_table_write_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::FixedArray => cpp_table_write_fixed_array(c_path.as_ptr(), &mut error),
            CppTableFixture::Keywords => cpp_table_write_keywords(c_path.as_ptr(), &mut error),
            CppTableFixture::SsmScalarPrimitives => {
                cpp_table_write_ssm_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmFixedArray => {
                cpp_table_write_ssm_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmKeywords => {
                cpp_table_write_ssm_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::LockFile => cpp_table_write_with_lock(c_path.as_ptr(), &mut error),
            CppTableFixture::RefTable => cpp_table_write_ref_table(c_path.as_ptr(), &mut error),
            CppTableFixture::SortedRefTable => {
                cpp_table_write_sorted_ref_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ConcatTable => {
                cpp_table_write_concat_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::DeepCopy => cpp_table_write_deep_copy(c_path.as_ptr(), &mut error),
            CppTableFixture::ColumnsIndex => {
                cpp_table_write_columns_index_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmScalarPrimitives => {
                cpp_table_write_ism_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmSlowlyChanging => {
                cpp_table_write_ism_slowly_changing(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmComplexScalars => {
                cpp_table_write_ism_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnStMan => {
                cpp_table_write_tiled_column_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeStMan => {
                cpp_table_write_tiled_shape_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledCellStMan => {
                cpp_table_write_tiled_cell_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ForwardColumn => {
                cpp_table_write_forward_column_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ScaledArray => {
                cpp_table_write_scaled_array_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIOVariableArray => {
                cpp_table_write_aipsio_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmVariableArray => {
                cpp_table_write_ssm_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::UndefinedScalars => {
                cpp_table_write_undefined_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ColumnKeywords => {
                cpp_table_write_column_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIORecordColumn => {
                cpp_table_write_record_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MixedSchema => {
                cpp_table_write_mixed_schema(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TableInfoMetadata => {
                cpp_table_write_table_info(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioAllNumericScalars => {
                cpp_table_write_aipsio_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioComplexScalars => {
                cpp_table_write_aipsio_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioTypedArrays => {
                cpp_table_write_aipsio_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::Aipsio3DFixedArray => {
                cpp_table_write_aipsio_3d_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmAllNumericScalars => {
                cpp_table_write_ssm_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplexScalars => {
                cpp_table_write_ssm_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmTypedArrays => {
                cpp_table_write_ssm_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationRemovedColumn
            | CppTableFixture::MutationRemovedRows
            | CppTableFixture::MutationAddedColumn => {
                return Err("mutation fixtures are verify-only (no C++ write)".to_string());
            }
        }
    };

    if rc != 0 {
        let msg = if error.is_null() {
            "unknown C++ error".to_string()
        } else {
            let s = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .to_string();
            unsafe { cpp_table_free_error(error) };
            s
        };
        return Err(msg);
    }
    Ok(())
}

/// Verify a table fixture using C++ casacore. Returns an error string on failure.
#[cfg(has_casacore_cpp)]
pub fn cpp_table_verify(fixture: CppTableFixture, path: &std::path::Path) -> Result<(), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        match fixture {
            CppTableFixture::ScalarPrimitives => {
                cpp_table_verify_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::FixedArray => {
                cpp_table_verify_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::Keywords => cpp_table_verify_keywords(c_path.as_ptr(), &mut error),
            CppTableFixture::SsmScalarPrimitives => {
                cpp_table_verify_ssm_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmFixedArray => {
                cpp_table_verify_ssm_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmKeywords => {
                cpp_table_verify_ssm_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationRemovedColumn => {
                cpp_table_verify_mutation_removed_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationRemovedRows => {
                cpp_table_verify_mutation_removed_rows(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationAddedColumn => {
                cpp_table_verify_mutation_added_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::LockFile => cpp_table_verify_with_lock(c_path.as_ptr(), &mut error),
            CppTableFixture::RefTable => cpp_table_verify_ref_table(c_path.as_ptr(), &mut error),
            CppTableFixture::SortedRefTable => {
                cpp_table_verify_sorted_ref_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ConcatTable => {
                cpp_table_verify_concat_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::DeepCopy => cpp_table_verify_deep_copy(c_path.as_ptr(), &mut error),
            CppTableFixture::IsmScalarPrimitives => {
                cpp_table_verify_ism_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmSlowlyChanging => {
                cpp_table_verify_ism_slowly_changing(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmComplexScalars => {
                cpp_table_verify_ism_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ColumnsIndex => {
                return Err(
                    "ColumnsIndex fixture has no C++ verify (Rust does the verification)"
                        .to_string(),
                );
            }
            CppTableFixture::TiledColumnStMan => {
                cpp_table_verify_tiled_column_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeStMan => {
                cpp_table_verify_tiled_shape_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledCellStMan => {
                cpp_table_verify_tiled_cell_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ForwardColumn => {
                cpp_table_verify_forward_column_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ScaledArray => {
                cpp_table_verify_scaled_array_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIOVariableArray => {
                cpp_table_verify_aipsio_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmVariableArray => {
                cpp_table_verify_ssm_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::UndefinedScalars => {
                cpp_table_verify_undefined_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ColumnKeywords => {
                cpp_table_verify_column_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIORecordColumn => {
                cpp_table_verify_record_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MixedSchema => {
                cpp_table_verify_mixed_schema(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TableInfoMetadata => {
                cpp_table_verify_table_info(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioAllNumericScalars => {
                cpp_table_verify_aipsio_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioComplexScalars => {
                cpp_table_verify_aipsio_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioTypedArrays => {
                cpp_table_verify_aipsio_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::Aipsio3DFixedArray => {
                cpp_table_verify_aipsio_3d_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmAllNumericScalars => {
                cpp_table_verify_ssm_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplexScalars => {
                cpp_table_verify_ssm_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmTypedArrays => {
                cpp_table_verify_ssm_typed_arrays(c_path.as_ptr(), &mut error)
            }
        }
    };

    if rc != 0 {
        let msg = if error.is_null() {
            "unknown C++ error".to_string()
        } else {
            let s = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .to_string();
            unsafe { cpp_table_free_error(error) };
            s
        };
        return Err(msg);
    }
    Ok(())
}

/// Times `nqueries` exact `ColumnsIndex` lookups for `key_value` on the `"id"`
/// column of the table at `path` using the C++ casacore implementation.
///
/// Returns `(elapsed_ns, match_count)` where `elapsed_ns` is the total wall
/// time for all queries and `match_count` is the number of rows returned by
/// the last lookup.
///
/// Use this alongside the Rust `ColumnsIndex` to compare performance.
#[cfg(has_casacore_cpp)]
pub fn cpp_columns_index_time_lookups(
    path: &std::path::Path,
    key_value: i32,
    nqueries: u64,
) -> Result<(u64, u64), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut elapsed_ns: u64 = 0;
    let mut match_count: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_columns_index_time_lookups(
            c_path.as_ptr(),
            key_value,
            nqueries,
            &mut elapsed_ns,
            &mut match_count,
            &mut error,
        )
    };

    if rc != 0 {
        let msg = if error.is_null() {
            "unknown C++ error".to_string()
        } else {
            let s = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .to_string();
            unsafe { cpp_table_free_error(error) };
            s
        };
        return Err(msg);
    }
    Ok((elapsed_ns, match_count))
}

/// Stub for when C++ is unavailable.
#[cfg(not(has_casacore_cpp))]
pub fn cpp_columns_index_time_lookups(
    _path: &std::path::Path,
    _key_value: i32,
    _nqueries: u64,
) -> Result<(u64, u64), String> {
    Err("C++ casacore backend unavailable".to_string())
}

/// Benchmark C++ variable-shape array write + read for `nrows` rows.
///
/// Returns `(write_ns, read_ns, total_elems)`.
#[cfg(has_casacore_cpp)]
pub fn cpp_vararray_bench(path: &std::path::Path, nrows: u64) -> Result<(u64, u64, u64), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut write_ns: u64 = 0;
    let mut read_ns: u64 = 0;
    let mut total_elems: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        cpp_vararray_bench_write_read(
            c_path.as_ptr(),
            nrows,
            &mut write_ns,
            &mut read_ns,
            &mut total_elems,
            &mut error,
        )
    };
    if rc == 0 {
        return Ok((write_ns, read_ns, total_elems));
    }
    let msg = if error.is_null() {
        "unknown C++ error".to_string()
    } else {
        let s = unsafe { std::ffi::CStr::from_ptr(error) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(error) };
        s
    };
    Err(msg)
}

/// Stub for when C++ is unavailable.
#[cfg(not(has_casacore_cpp))]
pub fn cpp_vararray_bench(_path: &std::path::Path, _nrows: u64) -> Result<(u64, u64, u64), String> {
    Err("C++ casacore backend unavailable".to_string())
}

/// Result of the C++ set algebra benchmark.
pub struct SetAlgebraBenchResult {
    pub union_ns: u64,
    pub intersection_ns: u64,
    pub difference_ns: u64,
    pub union_rows: u64,
    pub intersection_rows: u64,
    pub difference_rows: u64,
}

/// Benchmark C++ `Table::operator|`, `operator&`, `operator-` on row-selected tables.
///
/// Creates a table with `nrows` rows, selects `[0..split_a)` and `[split_b..nrows)`,
/// and times union, intersection, and difference.
#[cfg(has_casacore_cpp)]
pub fn cpp_set_algebra_bench(
    path: &std::path::Path,
    nrows: u64,
    split_a: u64,
    split_b: u64,
) -> Result<SetAlgebraBenchResult, String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut union_ns: u64 = 0;
    let mut intersection_ns: u64 = 0;
    let mut difference_ns: u64 = 0;
    let mut union_rows: u64 = 0;
    let mut intersection_rows: u64 = 0;
    let mut difference_rows: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_set_algebra_bench(
            c_path.as_ptr(),
            nrows,
            split_a,
            split_b,
            &mut union_ns,
            &mut intersection_ns,
            &mut difference_ns,
            &mut union_rows,
            &mut intersection_rows,
            &mut difference_rows,
            &mut error,
        )
    };
    if rc == 0 {
        return Ok(SetAlgebraBenchResult {
            union_ns,
            intersection_ns,
            difference_ns,
            union_rows,
            intersection_rows,
            difference_rows,
        });
    }
    let msg = if error.is_null() {
        "unknown C++ error".to_string()
    } else {
        let s = unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(error) };
        s
    };
    Err(msg)
}

/// Stub for when C++ is unavailable.
#[cfg(not(has_casacore_cpp))]
pub fn cpp_set_algebra_bench(
    _path: &std::path::Path,
    _nrows: u64,
    _split_a: u64,
    _split_b: u64,
) -> Result<SetAlgebraBenchResult, String> {
    Err("C++ casacore backend unavailable".to_string())
}

/// Benchmark C++ `TableCopy::copyRows` on a table with `nrows` rows.
///
/// Returns elapsed nanoseconds.
#[cfg(has_casacore_cpp)]
pub fn cpp_copy_rows_bench(dir: &std::path::Path, nrows: u64) -> Result<u64, String> {
    let c_dir = std::ffi::CString::new(dir.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut ns: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe { ffi_copy_rows_bench(c_dir.as_ptr(), nrows, &mut ns, &mut error) };
    if rc == 0 {
        return Ok(ns);
    }
    let msg = if error.is_null() {
        "unknown C++ error".to_string()
    } else {
        let s = unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(error) };
        s
    };
    Err(msg)
}

/// Stub for when C++ is unavailable.
#[cfg(not(has_casacore_cpp))]
pub fn cpp_copy_rows_bench(_dir: &std::path::Path, _nrows: u64) -> Result<u64, String> {
    Err("C++ casacore backend unavailable".to_string())
}

/// Result of the C++ cell slice benchmark.
pub struct CellSliceBenchResult {
    pub write_ns: u64,
    pub slice_ns: u64,
}

/// Parameters for the cell slice benchmark.
pub struct CellSliceBenchParams {
    pub nrows: u64,
    pub dim0: i64,
    pub dim1: i64,
    pub slice_start0: i64,
    pub slice_start1: i64,
    pub slice_end0: i64,
    pub slice_end1: i64,
}

/// Benchmark C++ `ArrayColumn::getSlice` on `nrows` cells of shape `[dim0, dim1]`.
///
/// Slice region is `[slice_start0..slice_end0, slice_start1..slice_end1]` (end exclusive).
#[cfg(has_casacore_cpp)]
pub fn cpp_cell_slice_bench(
    path: &std::path::Path,
    params: &CellSliceBenchParams,
) -> Result<CellSliceBenchResult, String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut write_ns: u64 = 0;
    let mut slice_ns: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_cell_slice_bench(
            c_path.as_ptr(),
            params.nrows,
            params.dim0,
            params.dim1,
            params.slice_start0,
            params.slice_start1,
            params.slice_end0,
            params.slice_end1,
            &mut write_ns,
            &mut slice_ns,
            &mut error,
        )
    };
    if rc == 0 {
        return Ok(CellSliceBenchResult { write_ns, slice_ns });
    }
    let msg = if error.is_null() {
        "unknown C++ error".to_string()
    } else {
        let s = unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(error) };
        s
    };
    Err(msg)
}

/// Stub for when C++ is unavailable.
#[cfg(not(has_casacore_cpp))]
pub fn cpp_cell_slice_bench(
    _path: &std::path::Path,
    _params: &CellSliceBenchParams,
) -> Result<CellSliceBenchResult, String> {
    Err("C++ casacore backend unavailable".to_string())
}

/// Stubs for when C++ is unavailable.
#[cfg(not(has_casacore_cpp))]
pub fn cpp_table_write(_fixture: CppTableFixture, _path: &std::path::Path) -> Result<(), String> {
    Err("C++ casacore backend unavailable".to_string())
}

#[cfg(not(has_casacore_cpp))]
pub fn cpp_table_verify(_fixture: CppTableFixture, _path: &std::path::Path) -> Result<(), String> {
    Err("C++ casacore backend unavailable".to_string())
}
