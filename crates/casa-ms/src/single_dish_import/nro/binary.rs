// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fixed-width NOSTAR observation and scan record decoding.

use crate::{MsError, MsResult};

pub(super) const OBSERVATION_HEADER_BYTES: usize = 15_136;
pub(super) const SCAN_HEADER_BYTES: usize = 424;
pub(super) const ARRAYS: usize = 35;

#[derive(Clone, Copy)]
pub(super) enum ByteOrder {
    Big,
    Little,
}

#[derive(Clone, Copy)]
pub(super) struct Record<'a> {
    bytes: &'a [u8],
    order: ByteOrder,
}

impl<'a> Record<'a> {
    pub(super) fn observation(bytes: &'a [u8; OBSERVATION_HEADER_BYTES]) -> MsResult<Self> {
        let mut record = Self::new(bytes, ByteOrder::Big);
        if !(1..=ARRAYS as i32).contains(&record.i32(144)) {
            record.order = ByteOrder::Little;
            if !(1..=ARRAYS as i32).contains(&record.i32(144)) {
                return Err(invalid("array count or byte order"));
            }
        }
        Ok(record)
    }

    pub(super) fn new(bytes: &'a [u8], order: ByteOrder) -> Self {
        Self { bytes, order }
    }

    pub(super) fn order(self) -> ByteOrder {
        self.order
    }

    pub(super) fn i16(self, offset: usize) -> i16 {
        let bytes = self.bytes[offset..offset + 2].try_into().unwrap();
        match self.order {
            ByteOrder::Big => i16::from_be_bytes(bytes),
            ByteOrder::Little => i16::from_le_bytes(bytes),
        }
    }

    pub(super) fn i32(self, offset: usize) -> i32 {
        let bytes = self.bytes[offset..offset + 4].try_into().unwrap();
        match self.order {
            ByteOrder::Big => i32::from_be_bytes(bytes),
            ByteOrder::Little => i32::from_le_bytes(bytes),
        }
    }

    pub(super) fn f32(self, offset: usize) -> f32 {
        f32::from_bits(self.i32(offset) as u32)
    }

    pub(super) fn f64(self, offset: usize) -> f64 {
        let bytes = self.bytes[offset..offset + 8].try_into().unwrap();
        match self.order {
            ByteOrder::Big => f64::from_be_bytes(bytes),
            ByteOrder::Little => f64::from_le_bytes(bytes),
        }
    }

    pub(super) fn text(self, offset: usize, len: usize) -> MsResult<&'a str> {
        let bytes = &self.bytes[offset..offset + len];
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        std::str::from_utf8(&bytes[..end])
            .map(str::trim_end)
            .map_err(|_| invalid("text field"))
    }

    pub(super) fn spectrum(self, count: usize, array_scale: f64) -> Vec<f32> {
        let scale = self.f64(408);
        let offset = self.f64(416);
        if scale == 0. && offset == 0. {
            return vec![0.; count];
        }
        let values = &self.bytes[SCAN_HEADER_BYTES..];
        let scaled = |code: u16| ((f64::from(code) * scale + offset) * array_scale) as f32;
        let mut samples = Vec::with_capacity(count);
        for bytes in values.as_chunks::<3>().0.iter().take(count / 2) {
            samples.push(scaled(u16::from_be_bytes([bytes[0], bytes[1]]) >> 4));
            samples.push(scaled(u16::from_be_bytes([bytes[1], bytes[2]]) & 0xfff));
        }
        if !count.is_multiple_of(2) {
            let last = &values[(count / 2) * 3..];
            samples.push(scaled(u16::from_be_bytes([last[0], last[1]]) >> 4));
        }
        samples
    }
}

pub(super) fn invalid(field: &str) -> MsError {
    MsError::InvalidInput(format!("Invalid NOSTAR {field}"))
}
