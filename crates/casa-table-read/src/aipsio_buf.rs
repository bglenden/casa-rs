// SPDX-License-Identifier: LGPL-3.0-or-later

use super::TableReadError;

const AIPSIO_MAGIC: u32 = 0xbebebebe;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteOrder {
    BigEndian,
    LittleEndian,
}

#[derive(Debug, Clone)]
struct PendingHeader {
    type_name: String,
    version: u32,
}

pub struct AipsIoBuf<'a> {
    data: &'a [u8],
    pos: usize,
    order: ByteOrder,
    level: usize,
    object_ends: Vec<usize>,
    pending: Option<PendingHeader>,
}

impl<'a> AipsIoBuf<'a> {
    pub fn new(data: &'a [u8], order: ByteOrder) -> Self {
        Self {
            data,
            pos: 0,
            order,
            level: 0,
            object_ends: Vec::new(),
            pending: None,
        }
    }

    pub fn with_detected_order(data: &'a [u8]) -> Result<Self, TableReadError> {
        Ok(Self::new(data, detect_aipsio_byte_order(data)?))
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], TableReadError> {
        if self.pos + n > self.data.len() {
            return Err(TableReadError::AipsIo("AipsIO buffer underrun".to_string()));
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn read_bool(&mut self) -> Result<bool, TableReadError> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_u8(&mut self) -> Result<u8, TableReadError> {
        Ok(self.read_bytes(1)?[0])
    }

    pub fn read_u16(&mut self) -> Result<u16, TableReadError> {
        let b = self.read_bytes(2)?;
        Ok(match self.order {
            ByteOrder::BigEndian => u16::from_be_bytes([b[0], b[1]]),
            ByteOrder::LittleEndian => u16::from_le_bytes([b[0], b[1]]),
        })
    }

    pub fn read_u32(&mut self) -> Result<u32, TableReadError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            ByteOrder::BigEndian => u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            ByteOrder::LittleEndian => u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    pub fn read_i16(&mut self) -> Result<i16, TableReadError> {
        let b = self.read_bytes(2)?;
        Ok(match self.order {
            ByteOrder::BigEndian => i16::from_be_bytes([b[0], b[1]]),
            ByteOrder::LittleEndian => i16::from_le_bytes([b[0], b[1]]),
        })
    }

    pub fn read_i32(&mut self) -> Result<i32, TableReadError> {
        let b = self.read_bytes(4)?;
        Ok(match self.order {
            ByteOrder::BigEndian => i32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            ByteOrder::LittleEndian => i32::from_le_bytes([b[0], b[1], b[2], b[3]]),
        })
    }

    pub fn read_u64(&mut self) -> Result<u64, TableReadError> {
        let b = self.read_bytes(8)?;
        Ok(match self.order {
            ByteOrder::BigEndian => {
                u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
            ByteOrder::LittleEndian => {
                u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
        })
    }

    pub fn read_i64(&mut self) -> Result<i64, TableReadError> {
        let b = self.read_bytes(8)?;
        Ok(match self.order {
            ByteOrder::BigEndian => {
                i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
            ByteOrder::LittleEndian => {
                i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
        })
    }

    pub fn read_f32(&mut self) -> Result<f32, TableReadError> {
        Ok(f32::from_bits(self.read_u32()?))
    }

    pub fn read_f64(&mut self) -> Result<f64, TableReadError> {
        Ok(f64::from_bits(self.read_u64()?))
    }

    pub fn read_complex32(&mut self) -> Result<(f32, f32), TableReadError> {
        Ok((self.read_f32()?, self.read_f32()?))
    }

    pub fn read_complex64(&mut self) -> Result<(f64, f64), TableReadError> {
        Ok((self.read_f64()?, self.read_f64()?))
    }

    pub fn read_string(&mut self) -> Result<String, TableReadError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            TableReadError::AipsIo(format!("invalid UTF-8 in AipsIO string: {error}"))
        })
    }

    pub fn read_u8_into(&mut self, values: &mut [u8]) -> Result<(), TableReadError> {
        let bytes = self.read_bytes(values.len())?;
        values.copy_from_slice(bytes);
        Ok(())
    }

    pub fn read_i32_into(&mut self, values: &mut [i32]) -> Result<(), TableReadError> {
        for value in values {
            *value = self.read_i32()?;
        }
        Ok(())
    }

    fn read_header(&mut self) -> Result<PendingHeader, TableReadError> {
        if self.level == 0 {
            let magic = self.read_u32()?;
            if magic != AIPSIO_MAGIC {
                return Err(TableReadError::AipsIo(format!(
                    "AipsIO magic mismatch: expected 0x{AIPSIO_MAGIC:08x}, got 0x{magic:08x}"
                )));
            }
        }
        self.level += 1;
        let object_len_pos = self.pos;
        let object_len = self.read_u32()? as usize;
        let object_end = self
            .pos
            .checked_sub(4)
            .and_then(|start| start.checked_add(object_len))
            .ok_or_else(|| TableReadError::AipsIo("AipsIO object length overflow".to_string()))?;
        if object_end > self.data.len() {
            return Err(TableReadError::AipsIo(format!(
                "AipsIO object overruns buffer: start={object_len_pos}, end={object_end}, len={}",
                self.data.len(),
            )));
        }
        self.object_ends.push(object_end);
        let type_name = self.read_string()?;
        let version = self.read_u32()?;
        Ok(PendingHeader { type_name, version })
    }

    pub fn get_next_type(&mut self) -> Result<String, TableReadError> {
        if self.pending.is_none() {
            self.pending = Some(self.read_header()?);
        }
        Ok(self
            .pending
            .as_ref()
            .expect("pending header initialized")
            .type_name
            .clone())
    }

    pub fn getstart(&mut self, expected_type: &str) -> Result<u32, TableReadError> {
        let header = match self.pending.take() {
            Some(header) => header,
            None => self.read_header()?,
        };
        if header.type_name != expected_type {
            return Err(TableReadError::AipsIo(format!(
                "AipsIO type mismatch: expected {expected_type:?}, got {:?}",
                header.type_name
            )));
        }
        Ok(header.version)
    }

    pub fn getend(&mut self) -> Result<(), TableReadError> {
        if self.level == 0 {
            return Err(TableReadError::AipsIo(
                "AipsIO getend called without getstart".to_string(),
            ));
        }
        if let Some(end) = self.object_ends.pop() {
            self.pos = end;
        }
        self.level -= 1;
        Ok(())
    }
}

pub fn detect_aipsio_byte_order(data: &[u8]) -> Result<ByteOrder, TableReadError> {
    if data.len() < 8 {
        return Err(TableReadError::AipsIo(
            "buffer too short for AipsIO header".to_string(),
        ));
    }

    let be_len = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let le_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    const MAX_REASONABLE_LEN: u32 = 1 << 30;
    let be_ok = be_len > 0 && be_len < MAX_REASONABLE_LEN;
    let le_ok = le_len > 0 && le_len < MAX_REASONABLE_LEN;
    match (be_ok, le_ok) {
        (true, false) => Ok(ByteOrder::BigEndian),
        (false, true) => Ok(ByteOrder::LittleEndian),
        (true, true) => Ok(ByteOrder::BigEndian),
        (false, false) => Err(TableReadError::AipsIo(format!(
            "cannot determine byte order: obj_len BE={be_len}, LE={le_len}"
        ))),
    }
}
