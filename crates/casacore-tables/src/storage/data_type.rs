#![allow(dead_code)]

use casacore_types::PrimitiveType;

/// Casacore DataType enum values (from casacore/casa/Utilities/DataType.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum CasacoreDataType {
    TpBool = 0,
    TpChar = 1,
    TpUChar = 2,
    TpShort = 3,
    TpUShort = 4,
    TpInt = 5,
    TpUInt = 6,
    TpFloat = 7,
    TpDouble = 8,
    TpComplex = 9,
    TpDComplex = 10,
    TpString = 11,
    TpTable = 12,
    TpArrayBool = 13,
    TpArrayChar = 14,
    TpArrayUChar = 15,
    TpArrayShort = 16,
    TpArrayUShort = 17,
    TpArrayInt = 18,
    TpArrayUInt = 19,
    TpArrayFloat = 20,
    TpArrayDouble = 21,
    TpArrayComplex = 22,
    TpArrayDComplex = 23,
    TpArrayString = 24,
    TpRecord = 25,
    TpOther = 26,
    TpQuantity = 27,
    TpArrayQuantity = 28,
    TpInt64 = 29,
    TpArrayInt64 = 30,
}

impl CasacoreDataType {
    pub(crate) fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::TpBool),
            1 => Some(Self::TpChar),
            2 => Some(Self::TpUChar),
            3 => Some(Self::TpShort),
            4 => Some(Self::TpUShort),
            5 => Some(Self::TpInt),
            6 => Some(Self::TpUInt),
            7 => Some(Self::TpFloat),
            8 => Some(Self::TpDouble),
            9 => Some(Self::TpComplex),
            10 => Some(Self::TpDComplex),
            11 => Some(Self::TpString),
            12 => Some(Self::TpTable),
            13 => Some(Self::TpArrayBool),
            14 => Some(Self::TpArrayChar),
            15 => Some(Self::TpArrayUChar),
            16 => Some(Self::TpArrayShort),
            17 => Some(Self::TpArrayUShort),
            18 => Some(Self::TpArrayInt),
            19 => Some(Self::TpArrayUInt),
            20 => Some(Self::TpArrayFloat),
            21 => Some(Self::TpArrayDouble),
            22 => Some(Self::TpArrayComplex),
            23 => Some(Self::TpArrayDComplex),
            24 => Some(Self::TpArrayString),
            25 => Some(Self::TpRecord),
            26 => Some(Self::TpOther),
            27 => Some(Self::TpQuantity),
            28 => Some(Self::TpArrayQuantity),
            29 => Some(Self::TpInt64),
            30 => Some(Self::TpArrayInt64),
            _ => None,
        }
    }

    pub(crate) fn to_primitive_type(self) -> Option<PrimitiveType> {
        match self {
            Self::TpBool | Self::TpArrayBool => Some(PrimitiveType::Bool),
            Self::TpUChar | Self::TpArrayUChar => Some(PrimitiveType::UInt8),
            Self::TpShort | Self::TpArrayShort => Some(PrimitiveType::Int16),
            Self::TpUShort | Self::TpArrayUShort => Some(PrimitiveType::UInt16),
            Self::TpInt | Self::TpArrayInt => Some(PrimitiveType::Int32),
            Self::TpUInt | Self::TpArrayUInt => Some(PrimitiveType::UInt32),
            Self::TpFloat | Self::TpArrayFloat => Some(PrimitiveType::Float32),
            Self::TpDouble | Self::TpArrayDouble => Some(PrimitiveType::Float64),
            Self::TpComplex | Self::TpArrayComplex => Some(PrimitiveType::Complex32),
            Self::TpDComplex | Self::TpArrayDComplex => Some(PrimitiveType::Complex64),
            Self::TpString | Self::TpArrayString => Some(PrimitiveType::String),
            Self::TpInt64 | Self::TpArrayInt64 => Some(PrimitiveType::Int64),
            _ => None,
        }
    }

    pub(crate) fn is_scalar(self) -> bool {
        (self as i32) <= Self::TpString as i32 && self != Self::TpTable || self == Self::TpInt64
    }

    pub(crate) fn from_primitive_type(pt: PrimitiveType, is_array: bool) -> Self {
        if is_array {
            match pt {
                PrimitiveType::Bool => Self::TpArrayBool,
                PrimitiveType::UInt8 => Self::TpArrayUChar,
                PrimitiveType::Int16 => Self::TpArrayShort,
                PrimitiveType::UInt16 => Self::TpArrayUShort,
                PrimitiveType::Int32 => Self::TpArrayInt,
                PrimitiveType::UInt32 => Self::TpArrayUInt,
                PrimitiveType::Float32 => Self::TpArrayFloat,
                PrimitiveType::Float64 => Self::TpArrayDouble,
                PrimitiveType::Complex32 => Self::TpArrayComplex,
                PrimitiveType::Complex64 => Self::TpArrayDComplex,
                PrimitiveType::String => Self::TpArrayString,
                PrimitiveType::Int64 => Self::TpArrayInt64,
            }
        } else {
            match pt {
                PrimitiveType::Bool => Self::TpBool,
                PrimitiveType::UInt8 => Self::TpUChar,
                PrimitiveType::Int16 => Self::TpShort,
                PrimitiveType::UInt16 => Self::TpUShort,
                PrimitiveType::Int32 => Self::TpInt,
                PrimitiveType::UInt32 => Self::TpUInt,
                PrimitiveType::Float32 => Self::TpFloat,
                PrimitiveType::Float64 => Self::TpDouble,
                PrimitiveType::Complex32 => Self::TpComplex,
                PrimitiveType::Complex64 => Self::TpDComplex,
                PrimitiveType::String => Self::TpString,
                PrimitiveType::Int64 => Self::TpInt64,
            }
        }
    }
}

/// Return the casacore className for a scalar column descriptor.
///
/// C++ casacore format: `"ScalarColumnDesc<" + getTypeStr(T)` where getTypeStr
/// returns an 8-char padded type name (e.g. `"Bool    "`, `"float   "`).
/// No closing `>`. See `casacore/casa/Utilities/ValType.h`.
pub(crate) fn scalar_column_class_name(pt: PrimitiveType) -> &'static str {
    match pt {
        PrimitiveType::Bool => "ScalarColumnDesc<Bool    ",
        PrimitiveType::UInt8 => "ScalarColumnDesc<uChar   ",
        PrimitiveType::Int16 => "ScalarColumnDesc<Short   ",
        PrimitiveType::UInt16 => "ScalarColumnDesc<uShort  ",
        PrimitiveType::Int32 => "ScalarColumnDesc<Int     ",
        PrimitiveType::UInt32 => "ScalarColumnDesc<uInt    ",
        PrimitiveType::Float32 => "ScalarColumnDesc<float   ",
        PrimitiveType::Float64 => "ScalarColumnDesc<double  ",
        PrimitiveType::Complex32 => "ScalarColumnDesc<Complex ",
        PrimitiveType::Complex64 => "ScalarColumnDesc<DComplex",
        PrimitiveType::String => "ScalarColumnDesc<String  ",
        PrimitiveType::Int64 => "ScalarColumnDesc<Int64   ",
    }
}

/// Return the casacore className for an array column descriptor.
///
/// Same padding convention as scalar, but prefix is `"ArrayColumnDesc<"`.
pub(crate) fn array_column_class_name(pt: PrimitiveType) -> &'static str {
    match pt {
        PrimitiveType::Bool => "ArrayColumnDesc<Bool    ",
        PrimitiveType::UInt8 => "ArrayColumnDesc<uChar   ",
        PrimitiveType::Int16 => "ArrayColumnDesc<Short   ",
        PrimitiveType::UInt16 => "ArrayColumnDesc<uShort  ",
        PrimitiveType::Int32 => "ArrayColumnDesc<Int     ",
        PrimitiveType::UInt32 => "ArrayColumnDesc<uInt    ",
        PrimitiveType::Float32 => "ArrayColumnDesc<float   ",
        PrimitiveType::Float64 => "ArrayColumnDesc<double  ",
        PrimitiveType::Complex32 => "ArrayColumnDesc<Complex ",
        PrimitiveType::Complex64 => "ArrayColumnDesc<DComplex",
        PrimitiveType::String => "ArrayColumnDesc<String  ",
        PrimitiveType::Int64 => "ArrayColumnDesc<Int64   ",
    }
}

/// Parse a casacore className string into (PrimitiveType, is_array).
///
/// Handles both the C++ on-disk format (8-char padded type, no closing `>`,
/// lowercase `float`/`double`) and the legacy Rust format with closing `>`.
pub(crate) fn parse_column_class_name(class_name: &str) -> Option<(PrimitiveType, bool)> {
    if let Some(inner) = class_name.strip_prefix("ScalarColumnDesc<") {
        // Strip optional closing '>' (legacy Rust format) then trim spaces
        let inner = inner.strip_suffix('>').unwrap_or(inner).trim();
        casacore_type_name_to_primitive(inner).map(|pt| (pt, false))
    } else if let Some(inner) = class_name.strip_prefix("ArrayColumnDesc<") {
        let inner = inner.strip_suffix('>').unwrap_or(inner).trim();
        casacore_type_name_to_primitive(inner).map(|pt| (pt, true))
    } else {
        None
    }
}

/// Map a casacore type name to PrimitiveType.
///
/// Accepts both C++ canonical names (`"float"`, `"double"`) and capitalized
/// forms (`"Float"`, `"Double"`) for robustness.
fn casacore_type_name_to_primitive(name: &str) -> Option<PrimitiveType> {
    match name {
        "Bool" => Some(PrimitiveType::Bool),
        "uChar" => Some(PrimitiveType::UInt8),
        "Short" => Some(PrimitiveType::Int16),
        "uShort" => Some(PrimitiveType::UInt16),
        "Int" => Some(PrimitiveType::Int32),
        "uInt" => Some(PrimitiveType::UInt32),
        "float" | "Float" => Some(PrimitiveType::Float32),
        "double" | "Double" => Some(PrimitiveType::Float64),
        "Complex" => Some(PrimitiveType::Complex32),
        "DComplex" => Some(PrimitiveType::Complex64),
        "String" => Some(PrimitiveType::String),
        "Int64" => Some(PrimitiveType::Int64),
        _ => None,
    }
}
