// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::Path;

use super::TableReadError;
use super::aipsio_buf::AipsIoBuf;
use super::data_type::{CasacoreDataType, parse_column_class_name};

#[derive(Debug, Clone)]
pub struct DataManagerEntry {
    pub type_name: String,
    pub seq_nr: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub col_name: String,
    pub data_type: CasacoreDataType,
    pub is_array: bool,
    pub option: i32,
    pub shape: Vec<i32>,
    pub dm_seq_nr: u32,
}

impl ColumnDesc {
    pub fn is_direct_array(&self) -> bool {
        self.is_array && (self.option & 1) != 0
    }

    pub fn is_indirect_array(&self) -> bool {
        self.is_array && !self.is_direct_array()
    }
}

#[derive(Debug, Clone)]
pub struct PlainTableDat {
    pub row_count: usize,
    pub columns: Vec<ColumnDesc>,
    pub data_managers: Vec<DataManagerEntry>,
}

pub fn read_plain_table_dat(path: &Path) -> Result<PlainTableDat, TableReadError> {
    let bytes = fs::read(path)?;
    let mut io = AipsIoBuf::with_detected_order(&bytes)?;
    let table_version = io.getstart("Table")?;
    let _nrrow = if table_version >= 3 {
        io.read_u64()?
    } else {
        u64::from(io.read_u32()?)
    };
    let _format = io.read_u32()?;
    let table_type = io.read_string()?;
    if table_type != "PlainTable" {
        return Err(TableReadError::Format(format!(
            "unsupported table type {table_type:?}"
        )));
    }

    let mut columns = read_table_desc(&mut io)?;
    let array_columns = columns.iter().map(|c| c.is_array).collect::<Vec<_>>();
    let column_set = read_column_set(&mut io, columns.len(), &array_columns)?;
    io.getend()?;

    for (desc, plain) in columns.iter_mut().zip(column_set.columns.iter()) {
        desc.dm_seq_nr = plain.dm_seq_nr;
        if desc.col_name != plain.original_name {
            return Err(TableReadError::Format(format!(
                "column name mismatch between TableDesc and ColumnSet: {:?} vs {:?}",
                desc.col_name, plain.original_name
            )));
        }
        if desc.is_array && desc.shape.is_empty() && !plain.shape.is_empty() {
            desc.shape = plain.shape.clone();
        }
    }

    Ok(PlainTableDat {
        row_count: column_set.nrrow as usize,
        columns,
        data_managers: column_set.data_managers,
    })
}

fn read_table_desc(io: &mut AipsIoBuf<'_>) -> Result<Vec<ColumnDesc>, TableReadError> {
    let version = io.getstart("TableDesc")?;
    let _name = io.read_string()?;
    let _vers = io.read_string()?;
    let _comment = io.read_string()?;

    skip_table_record(io)?;
    if version >= 2 {
        skip_table_record(io)?;
    }

    let ncol = io.read_u32()?;
    let mut columns = Vec::with_capacity(ncol as usize);
    for _ in 0..ncol {
        columns.push(read_column_desc(io)?);
    }
    io.getend()?;
    Ok(columns)
}

fn read_column_desc(io: &mut AipsIoBuf<'_>) -> Result<ColumnDesc, TableReadError> {
    let _col_desc_version = io.read_u32()?;
    let class_name = io.read_string()?;
    let (declared_dt, is_array) = parse_column_class_name(&class_name).ok_or_else(|| {
        TableReadError::Format(format!("unknown column class name: {class_name}"))
    })?;

    let _base_version = io.read_u32()?;
    let col_name = io.read_string()?;
    let _comment = io.read_string()?;
    let _data_manager_type = io.read_string()?;
    let _data_manager_group = io.read_string()?;
    let data_type_i32 = io.read_i32()?;
    let data_type = CasacoreDataType::from_i32(data_type_i32)
        .ok_or_else(|| TableReadError::Format(format!("unknown data type: {data_type_i32}")))?;
    let option = io.read_i32()?;
    let _nrdim = io.read_i32()?;

    let shape = if is_array {
        read_iposition(io)?
    } else {
        vec![]
    };
    let _max_length = io.read_u32()?;
    skip_table_record(io)?;

    let _type_version = io.read_u32()?;
    if is_array {
        let _has_default = io.read_bool()?;
    } else {
        skip_default_value(io, data_type)?;
    }

    let expected_data_type = if is_array {
        declared_dt.array_element_type()
    } else {
        declared_dt
    };
    if data_type != expected_data_type {
        return Err(TableReadError::Format(format!(
            "column {col_name:?} declared type {:?} but data type {:?}",
            declared_dt, data_type
        )));
    }

    Ok(ColumnDesc {
        col_name,
        data_type,
        is_array,
        option,
        shape,
        dm_seq_nr: 0,
    })
}

fn skip_default_value(io: &mut AipsIoBuf<'_>, dt: CasacoreDataType) -> Result<(), TableReadError> {
    match dt {
        CasacoreDataType::TpBool => {
            io.read_bool()?;
        }
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => {
            io.read_u8()?;
        }
        CasacoreDataType::TpShort => {
            io.read_i16()?;
        }
        CasacoreDataType::TpUShort => {
            io.read_u16()?;
        }
        CasacoreDataType::TpInt => {
            io.read_i32()?;
        }
        CasacoreDataType::TpUInt => {
            io.read_u32()?;
        }
        CasacoreDataType::TpFloat => {
            io.read_f32()?;
        }
        CasacoreDataType::TpDouble => {
            io.read_f64()?;
        }
        CasacoreDataType::TpComplex => {
            let _ = io.read_complex32()?;
        }
        CasacoreDataType::TpDComplex => {
            let _ = io.read_complex64()?;
        }
        CasacoreDataType::TpString => {
            io.read_string()?;
        }
        CasacoreDataType::TpInt64 => {
            io.read_i64()?;
        }
        CasacoreDataType::TpRecord => {}
        other => {
            return Err(TableReadError::Format(format!(
                "cannot skip default value for {other:?}"
            )));
        }
    }
    Ok(())
}

struct PlainColumnEntry {
    original_name: String,
    dm_seq_nr: u32,
    shape: Vec<i32>,
}

struct ColumnSetContents {
    nrrow: u64,
    data_managers: Vec<DataManagerEntry>,
    columns: Vec<PlainColumnEntry>,
}

fn read_column_set(
    io: &mut AipsIoBuf<'_>,
    ncol: usize,
    col_is_array: &[bool],
) -> Result<ColumnSetContents, TableReadError> {
    let version = io.read_i32()?;
    let nrrow = if version == -3 {
        let nr = io.read_u64()?;
        let _storage_opt = io.read_i32()?;
        let _block_size = io.read_i32()?;
        nr
    } else {
        u64::from(io.read_u32()?)
    };

    let _seq_count = io.read_u32()?;
    let num_dm = io.read_u32()?;
    let mut data_managers = Vec::with_capacity(num_dm as usize);
    for _ in 0..num_dm {
        let type_name = io.read_string()?;
        let seq_nr = io.read_u32()?;
        data_managers.push(DataManagerEntry {
            type_name,
            seq_nr,
            data: Vec::new(),
        });
    }

    let mut columns = Vec::with_capacity(ncol);
    for &is_array in &col_is_array[..ncol] {
        columns.push(read_plain_column(io, is_array)?);
    }

    for dm in &mut data_managers {
        let len = io.read_u32()?;
        if len > 0 {
            let mut buf = vec![0u8; len as usize];
            io.read_u8_into(&mut buf)?;
            dm.data = buf;
        }
    }

    Ok(ColumnSetContents {
        nrrow,
        data_managers,
        columns,
    })
}

fn read_plain_column(
    io: &mut AipsIoBuf<'_>,
    is_array: bool,
) -> Result<PlainColumnEntry, TableReadError> {
    let _version = io.read_u32()?;
    let original_name = io.read_string()?;
    let _derived_version = io.read_u32()?;
    let dm_seq_nr = io.read_u32()?;

    if is_array {
        let shape_col_def = io.read_bool()?;
        if shape_col_def {
            let shape = read_iposition(io)?;
            return Ok(PlainColumnEntry {
                original_name,
                dm_seq_nr,
                shape,
            });
        }
    }

    Ok(PlainColumnEntry {
        original_name,
        dm_seq_nr,
        shape: Vec::new(),
    })
}

fn read_iposition(io: &mut AipsIoBuf<'_>) -> Result<Vec<i32>, TableReadError> {
    let _version = io.getstart("IPosition")?;
    let n = io.read_u32()?;
    let mut values = vec![0i32; n as usize];
    io.read_i32_into(&mut values)?;
    io.getend()?;
    Ok(values)
}

fn skip_table_record(io: &mut AipsIoBuf<'_>) -> Result<(), TableReadError> {
    let _version = io.getstart("TableRecord")?;
    let desc = skip_record_desc(io)?;
    let _record_type = io.read_i32()?;
    for dt in desc {
        skip_record_field_value(io, dt)?;
    }
    io.getend()?;
    Ok(())
}

fn skip_record_desc(io: &mut AipsIoBuf<'_>) -> Result<Vec<CasacoreDataType>, TableReadError> {
    let _version = io.getstart("RecordDesc")?;
    let nfields = io.read_i32()?;
    let mut desc = Vec::with_capacity(nfields as usize);
    for _ in 0..nfields {
        let _name = io.read_string()?;
        let type_i32 = io.read_i32()?;
        let dt = CasacoreDataType::from_i32(type_i32).ok_or_else(|| {
            TableReadError::Format(format!("unknown RecordDesc field type: {type_i32}"))
        })?;
        if dt == CasacoreDataType::TpRecord {
            let _sub_desc = skip_record_desc(io)?;
        } else if is_array_data_type(dt) {
            let _shape = read_iposition(io)?;
        } else if dt == CasacoreDataType::TpTable {
            let _table_desc_name = io.read_string()?;
        }
        let _comment = io.read_string()?;
        desc.push(dt);
    }
    io.getend()?;
    Ok(desc)
}

fn skip_record_field_value(
    io: &mut AipsIoBuf<'_>,
    dt: CasacoreDataType,
) -> Result<(), TableReadError> {
    match dt {
        CasacoreDataType::TpBool => {
            io.read_bool()?;
        }
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => {
            io.read_u8()?;
        }
        CasacoreDataType::TpShort => {
            io.read_i16()?;
        }
        CasacoreDataType::TpUShort => {
            io.read_u16()?;
        }
        CasacoreDataType::TpInt => {
            io.read_i32()?;
        }
        CasacoreDataType::TpUInt => {
            io.read_u32()?;
        }
        CasacoreDataType::TpFloat => {
            io.read_f32()?;
        }
        CasacoreDataType::TpDouble => {
            io.read_f64()?;
        }
        CasacoreDataType::TpComplex => {
            let _ = io.read_complex32()?;
        }
        CasacoreDataType::TpDComplex => {
            let _ = io.read_complex64()?;
        }
        CasacoreDataType::TpString => {
            io.read_string()?;
        }
        CasacoreDataType::TpInt64 => {
            io.read_i64()?;
        }
        CasacoreDataType::TpRecord => {
            skip_table_record(io)?;
        }
        CasacoreDataType::TpTable => {
            let _path = io.read_string()?;
        }
        other if is_array_data_type(other) => {
            skip_array_record_field(io)?;
        }
        other => {
            return Err(TableReadError::Format(format!(
                "unsupported record field type {other:?}"
            )));
        }
    }
    Ok(())
}

fn skip_array_record_field(io: &mut AipsIoBuf<'_>) -> Result<(), TableReadError> {
    let type_name = io.get_next_type()?;
    let version = io.getstart(&type_name)?;
    let ndim = io.read_u32()? as usize;
    if ndim > 0 {
        if version < 3 {
            for _ in 0..ndim {
                let _origin = io.read_i32()?;
            }
        }
        let mut nelem = 1usize;
        for _ in 0..ndim {
            nelem *= io.read_u32()? as usize;
        }
        let count = io.read_u32()? as usize;
        let count = count.min(nelem);
        for _ in 0..count {
            match type_name.as_str() {
                "Array<Bool>" => {
                    io.read_bool()?;
                }
                "Array<uChar>" | "Array<Char>" => {
                    io.read_u8()?;
                }
                "Array<Short>" => {
                    io.read_i16()?;
                }
                "Array<uShort>" => {
                    io.read_u16()?;
                }
                "Array<Int>" => {
                    io.read_i32()?;
                }
                "Array<uInt>" => {
                    io.read_u32()?;
                }
                "Array<float>" => {
                    io.read_f32()?;
                }
                "Array<double>" => {
                    io.read_f64()?;
                }
                "Array<Complex>" => {
                    let _ = io.read_complex32()?;
                }
                "Array<DComplex>" => {
                    let _ = io.read_complex64()?;
                }
                "Array<String>" => {
                    io.read_string()?;
                }
                "Array<Int64>" => {
                    io.read_i64()?;
                }
                other => {
                    return Err(TableReadError::Format(format!(
                        "unsupported array record type {other:?}"
                    )));
                }
            }
        }
    }
    io.getend()?;
    Ok(())
}

fn is_array_data_type(dt: CasacoreDataType) -> bool {
    matches!(
        dt,
        CasacoreDataType::TpArrayBool
            | CasacoreDataType::TpArrayChar
            | CasacoreDataType::TpArrayUChar
            | CasacoreDataType::TpArrayShort
            | CasacoreDataType::TpArrayUShort
            | CasacoreDataType::TpArrayInt
            | CasacoreDataType::TpArrayUInt
            | CasacoreDataType::TpArrayFloat
            | CasacoreDataType::TpArrayDouble
            | CasacoreDataType::TpArrayComplex
            | CasacoreDataType::TpArrayDComplex
            | CasacoreDataType::TpArrayString
            | CasacoreDataType::TpArrayInt64
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aipsio_buf::ByteOrder;

    fn be_u16(value: u16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    fn be_u32(value: u32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    fn be_i16(value: i16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    fn be_i32(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    fn be_i64(value: i64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    fn string_bytes(value: &str) -> Vec<u8> {
        let mut bytes = be_u32(value.len() as u32);
        bytes.extend(value.as_bytes());
        bytes
    }

    #[test]
    fn column_desc_array_helpers_follow_option_bits() {
        let scalar = ColumnDesc {
            col_name: "TIME".to_string(),
            data_type: CasacoreDataType::TpDouble,
            is_array: false,
            option: 1,
            shape: Vec::new(),
            dm_seq_nr: 0,
        };
        assert!(!scalar.is_direct_array());
        assert!(!scalar.is_indirect_array());

        let direct = ColumnDesc {
            is_array: true,
            ..scalar.clone()
        };
        assert!(direct.is_direct_array());
        assert!(!direct.is_indirect_array());

        let indirect = ColumnDesc {
            option: 0,
            ..direct
        };
        assert!(!indirect.is_direct_array());
        assert!(indirect.is_indirect_array());
    }

    #[test]
    fn default_value_skipper_consumes_supported_scalar_payloads() {
        let cases = vec![
            (CasacoreDataType::TpBool, vec![1]),
            (CasacoreDataType::TpUChar, vec![7]),
            (CasacoreDataType::TpChar, vec![8]),
            (CasacoreDataType::TpShort, be_i16(-2)),
            (CasacoreDataType::TpUShort, be_u16(2)),
            (CasacoreDataType::TpInt, be_i32(-3)),
            (CasacoreDataType::TpUInt, be_u32(3)),
            (
                CasacoreDataType::TpFloat,
                1.25_f32.to_bits().to_be_bytes().to_vec(),
            ),
            (
                CasacoreDataType::TpDouble,
                1.5_f64.to_bits().to_be_bytes().to_vec(),
            ),
            (
                CasacoreDataType::TpComplex,
                [
                    1.0_f32.to_bits().to_be_bytes(),
                    2.0_f32.to_bits().to_be_bytes(),
                ]
                .concat(),
            ),
            (
                CasacoreDataType::TpDComplex,
                [
                    1.0_f64.to_bits().to_be_bytes(),
                    2.0_f64.to_bits().to_be_bytes(),
                ]
                .concat(),
            ),
            (CasacoreDataType::TpString, string_bytes("name")),
            (CasacoreDataType::TpInt64, be_i64(-4)),
            (CasacoreDataType::TpRecord, Vec::new()),
        ];

        for (data_type, payload) in cases {
            let mut io = AipsIoBuf::new(&payload, ByteOrder::BigEndian);
            skip_default_value(&mut io, data_type)
                .unwrap_or_else(|error| panic!("skip default for {data_type:?} failed: {error}"));
        }

        let mut io = AipsIoBuf::new(&[], ByteOrder::BigEndian);
        assert!(
            skip_default_value(&mut io, CasacoreDataType::TpQuantity)
                .unwrap_err()
                .to_string()
                .contains("cannot skip default value")
        );
    }

    #[test]
    fn array_data_type_classifier_covers_supported_and_scalar_values() {
        for data_type in [
            CasacoreDataType::TpArrayBool,
            CasacoreDataType::TpArrayChar,
            CasacoreDataType::TpArrayUChar,
            CasacoreDataType::TpArrayShort,
            CasacoreDataType::TpArrayUShort,
            CasacoreDataType::TpArrayInt,
            CasacoreDataType::TpArrayUInt,
            CasacoreDataType::TpArrayFloat,
            CasacoreDataType::TpArrayDouble,
            CasacoreDataType::TpArrayComplex,
            CasacoreDataType::TpArrayDComplex,
            CasacoreDataType::TpArrayString,
            CasacoreDataType::TpArrayInt64,
        ] {
            assert!(is_array_data_type(data_type), "{data_type:?}");
        }
        for data_type in [
            CasacoreDataType::TpBool,
            CasacoreDataType::TpDouble,
            CasacoreDataType::TpString,
            CasacoreDataType::TpRecord,
            CasacoreDataType::TpArrayQuantity,
        ] {
            assert!(!is_array_data_type(data_type), "{data_type:?}");
        }
    }
}
