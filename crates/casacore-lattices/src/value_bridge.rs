// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal conversions between typed `ArrayD<T>` and `ArrayValue`.

use casacore_types::{ArrayValue, PrimitiveType};
use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};

use crate::element::LatticeElement;
use crate::error::LatticeError;

/// Converts a typed `ArrayD<T>` into an `ArrayValue` for table storage.
pub(crate) fn to_array_value<T: LatticeElement>(data: &ArrayD<T>) -> ArrayValue {
    // We need to dispatch on T's primitive type to construct the right variant.
    // Since T is generic, we use the PRIMITIVE_TYPE constant.
    match T::PRIMITIVE_TYPE {
        PrimitiveType::Bool => {
            let data = unsafe_cast_ref::<T, bool>(data);
            ArrayValue::Bool(data.clone())
        }
        PrimitiveType::UInt8 => {
            let data = unsafe_cast_ref::<T, u8>(data);
            ArrayValue::UInt8(data.clone())
        }
        PrimitiveType::Int16 => {
            let data = unsafe_cast_ref::<T, i16>(data);
            ArrayValue::Int16(data.clone())
        }
        PrimitiveType::UInt16 => {
            let data = unsafe_cast_ref::<T, u16>(data);
            ArrayValue::UInt16(data.clone())
        }
        PrimitiveType::Int32 => {
            let data = unsafe_cast_ref::<T, i32>(data);
            ArrayValue::Int32(data.clone())
        }
        PrimitiveType::UInt32 => {
            let data = unsafe_cast_ref::<T, u32>(data);
            ArrayValue::UInt32(data.clone())
        }
        PrimitiveType::Int64 => {
            let data = unsafe_cast_ref::<T, i64>(data);
            ArrayValue::Int64(data.clone())
        }
        PrimitiveType::Float32 => {
            let data = unsafe_cast_ref::<T, f32>(data);
            ArrayValue::Float32(data.clone())
        }
        PrimitiveType::Float64 => {
            let data = unsafe_cast_ref::<T, f64>(data);
            ArrayValue::Float64(data.clone())
        }
        PrimitiveType::Complex32 => {
            let data = unsafe_cast_ref::<T, Complex32>(data);
            ArrayValue::Complex32(data.clone())
        }
        PrimitiveType::Complex64 => {
            let data = unsafe_cast_ref::<T, Complex64>(data);
            ArrayValue::Complex64(data.clone())
        }
        PrimitiveType::String => {
            let data = unsafe_cast_ref::<T, String>(data);
            ArrayValue::String(data.clone())
        }
    }
}

/// Extracts a typed `ArrayD<T>` from an `ArrayValue`.
///
/// Returns an error if the `ArrayValue` variant does not match `T`.
pub(crate) fn from_array_value<T: LatticeElement>(
    value: ArrayValue,
) -> Result<ArrayD<T>, LatticeError> {
    if value.primitive_type() != T::PRIMITIVE_TYPE {
        return Err(LatticeError::Table(format!(
            "type mismatch: expected {:?}, got {:?}",
            T::PRIMITIVE_TYPE,
            value.primitive_type()
        )));
    }

    match value {
        ArrayValue::Bool(a) => Ok(unsafe_cast_owned::<bool, T>(a)),
        ArrayValue::UInt8(a) => Ok(unsafe_cast_owned::<u8, T>(a)),
        ArrayValue::Int16(a) => Ok(unsafe_cast_owned::<i16, T>(a)),
        ArrayValue::UInt16(a) => Ok(unsafe_cast_owned::<u16, T>(a)),
        ArrayValue::Int32(a) => Ok(unsafe_cast_owned::<i32, T>(a)),
        ArrayValue::UInt32(a) => Ok(unsafe_cast_owned::<u32, T>(a)),
        ArrayValue::Int64(a) => Ok(unsafe_cast_owned::<i64, T>(a)),
        ArrayValue::Float32(a) => Ok(unsafe_cast_owned::<f32, T>(a)),
        ArrayValue::Float64(a) => Ok(unsafe_cast_owned::<f64, T>(a)),
        ArrayValue::Complex32(a) => Ok(unsafe_cast_owned::<Complex32, T>(a)),
        ArrayValue::Complex64(a) => Ok(unsafe_cast_owned::<Complex64, T>(a)),
        ArrayValue::String(a) => Ok(unsafe_cast_owned::<String, T>(a)),
    }
}

/// Reinterpret an `ArrayD<From>` reference as `ArrayD<To>`.
///
/// # Safety
/// This is safe when `From` and `To` are the same type, which is guaranteed
/// by the `LatticeElement::PRIMITIVE_TYPE` dispatch in the callers above.
fn unsafe_cast_ref<From: 'static, To: 'static>(data: &ArrayD<From>) -> &ArrayD<To> {
    assert_eq!(
        std::any::TypeId::of::<From>(),
        std::any::TypeId::of::<To>(),
        "value_bridge: type mismatch in unsafe_cast_ref"
    );
    // SAFETY: From and To are the same type (verified by TypeId assertion).
    unsafe { &*(std::ptr::from_ref(data) as *const ArrayD<To>) }
}

/// Reinterpret an owned `ArrayD<From>` as `ArrayD<To>`.
///
/// # Safety
/// Same as `unsafe_cast_ref` — safe when `From` and `To` are the same type.
fn unsafe_cast_owned<From: 'static, To: 'static>(data: ArrayD<From>) -> ArrayD<To> {
    assert_eq!(
        std::any::TypeId::of::<From>(),
        std::any::TypeId::of::<To>(),
        "value_bridge: type mismatch in unsafe_cast_owned"
    );
    // SAFETY: From and To are the same type (verified by TypeId assertion).
    unsafe {
        let raw = std::mem::ManuallyDrop::new(data);
        std::ptr::read(std::ptr::from_ref(&*raw) as *const ArrayD<To>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::IxDyn;

    #[test]
    fn roundtrip_f64() {
        let data = ArrayD::from_elem(IxDyn(&[3, 4]), 2.5f64);
        let av = to_array_value(&data);
        assert_eq!(av.primitive_type(), PrimitiveType::Float64);
        let back: ArrayD<f64> = from_array_value(av).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn roundtrip_complex32() {
        let val = Complex32::new(1.0, -1.0);
        let data = ArrayD::from_elem(IxDyn(&[2, 2]), val);
        let av = to_array_value(&data);
        let back: ArrayD<Complex32> = from_array_value(av).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn type_mismatch_error() {
        let data = ArrayD::from_elem(IxDyn(&[2]), 1.0f32);
        let av = to_array_value(&data);
        let result: Result<ArrayD<f64>, _> = from_array_value(av);
        assert!(result.is_err());
    }
}
