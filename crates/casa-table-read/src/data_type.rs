// SPDX-License-Identifier: LGPL-3.0-or-later

/// casacore `DataType` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CasacoreDataType {
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
    pub fn from_i32(value: i32) -> Option<Self> {
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

    pub fn array_element_type(self) -> Self {
        match self {
            Self::TpArrayBool => Self::TpBool,
            Self::TpArrayChar => Self::TpChar,
            Self::TpArrayUChar => Self::TpUChar,
            Self::TpArrayShort => Self::TpShort,
            Self::TpArrayUShort => Self::TpUShort,
            Self::TpArrayInt => Self::TpInt,
            Self::TpArrayUInt => Self::TpUInt,
            Self::TpArrayFloat => Self::TpFloat,
            Self::TpArrayDouble => Self::TpDouble,
            Self::TpArrayComplex => Self::TpComplex,
            Self::TpArrayDComplex => Self::TpDComplex,
            Self::TpArrayString => Self::TpString,
            Self::TpArrayInt64 => Self::TpInt64,
            other => other,
        }
    }
}

pub fn parse_column_class_name(class_name: &str) -> Option<(CasacoreDataType, bool)> {
    if class_name == "ScalarRecordColumnDesc" {
        return Some((CasacoreDataType::TpRecord, false));
    }

    let (inner, is_array) = if let Some(inner) = class_name.strip_prefix("ScalarColumnDesc<") {
        (inner, false)
    } else {
        let inner = class_name.strip_prefix("ArrayColumnDesc<")?;
        (inner, true)
    };

    let inner = inner.strip_suffix('>').unwrap_or(inner).trim();
    let dt = match inner {
        "Bool" => {
            if is_array {
                CasacoreDataType::TpArrayBool
            } else {
                CasacoreDataType::TpBool
            }
        }
        "uChar" => {
            if is_array {
                CasacoreDataType::TpArrayUChar
            } else {
                CasacoreDataType::TpUChar
            }
        }
        "Short" => {
            if is_array {
                CasacoreDataType::TpArrayShort
            } else {
                CasacoreDataType::TpShort
            }
        }
        "uShort" => {
            if is_array {
                CasacoreDataType::TpArrayUShort
            } else {
                CasacoreDataType::TpUShort
            }
        }
        "Int" => {
            if is_array {
                CasacoreDataType::TpArrayInt
            } else {
                CasacoreDataType::TpInt
            }
        }
        "uInt" => {
            if is_array {
                CasacoreDataType::TpArrayUInt
            } else {
                CasacoreDataType::TpUInt
            }
        }
        "float" => {
            if is_array {
                CasacoreDataType::TpArrayFloat
            } else {
                CasacoreDataType::TpFloat
            }
        }
        "double" => {
            if is_array {
                CasacoreDataType::TpArrayDouble
            } else {
                CasacoreDataType::TpDouble
            }
        }
        "String" => {
            if is_array {
                CasacoreDataType::TpArrayString
            } else {
                CasacoreDataType::TpString
            }
        }
        "Int64" => {
            if is_array {
                CasacoreDataType::TpArrayInt64
            } else {
                CasacoreDataType::TpInt64
            }
        }
        _ => return None,
    };

    Some((dt, is_array))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_i32_covers_known_and_unknown_values() {
        for value in 0..=30 {
            assert!(
                CasacoreDataType::from_i32(value).is_some(),
                "expected data type for {value}"
            );
        }

        assert_eq!(CasacoreDataType::from_i32(-1), None);
        assert_eq!(CasacoreDataType::from_i32(31), None);
    }

    #[test]
    fn array_element_type_uses_scalar_variant_for_arrays() {
        assert_eq!(
            CasacoreDataType::TpArrayBool.array_element_type(),
            CasacoreDataType::TpBool
        );
        assert_eq!(
            CasacoreDataType::TpArrayInt64.array_element_type(),
            CasacoreDataType::TpInt64
        );
        assert_eq!(
            CasacoreDataType::TpDouble.array_element_type(),
            CasacoreDataType::TpDouble
        );
    }

    #[test]
    fn parse_column_class_name_recognizes_scalars_arrays_records_and_unknowns() {
        assert_eq!(
            parse_column_class_name("ScalarRecordColumnDesc"),
            Some((CasacoreDataType::TpRecord, false))
        );
        assert_eq!(
            parse_column_class_name("ScalarColumnDesc<double>"),
            Some((CasacoreDataType::TpDouble, false))
        );
        assert_eq!(
            parse_column_class_name("ArrayColumnDesc<Int64>"),
            Some((CasacoreDataType::TpArrayInt64, true))
        );
        assert_eq!(
            parse_column_class_name("ArrayColumnDesc<Bool>"),
            Some((CasacoreDataType::TpArrayBool, true))
        );
        assert_eq!(parse_column_class_name("ArrayColumnDesc<unknown>"), None);
        assert_eq!(parse_column_class_name("NotAColumnDesc"), None);
    }
}
