// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stream complete plot arrays to the caller's file, never into JSON.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

use casa_ms::plot_document::{PlotArrayDtype, PlotBinaryArray};

use super::{SimobserveError, failure};

pub(in crate::simobserve) struct Payload {
    file: BufWriter<std::fs::File>,
    bytes: u64,
}

impl Payload {
    pub(super) fn open(path: &Path) -> Result<Self, SimobserveError> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(failure)?;
        Ok(Self {
            file: BufWriter::new(file),
            bytes: 0,
        })
    }

    pub(super) const fn position(&self) -> u64 {
        self.bytes
    }

    pub(super) fn f64(&mut self, value: f64) -> Result<(), SimobserveError> {
        self.file.write_all(&value.to_le_bytes()).map_err(failure)?;
        self.bytes += 8;
        Ok(())
    }

    pub(super) fn f32(&mut self, value: f32) -> Result<(), SimobserveError> {
        self.file.write_all(&value.to_le_bytes()).map_err(failure)?;
        self.bytes += 4;
        Ok(())
    }

    pub(super) fn values(
        &mut self,
        values: impl IntoIterator<Item = f64>,
    ) -> Result<PlotBinaryArray, SimobserveError> {
        let start = self.bytes;
        for value in values {
            self.f64(value)?;
        }
        Ok(view(
            start,
            vec![(self.bytes - start) / 8],
            vec![],
            PlotArrayDtype::F64Le,
        ))
    }

    pub(super) fn finish(mut self) -> Result<u64, SimobserveError> {
        self.file.flush().map_err(failure)?;
        Ok(self.bytes)
    }
}

pub(super) fn view(
    offset_bytes: u64,
    shape: Vec<u64>,
    strides_bytes: Vec<u64>,
    dtype: PlotArrayDtype,
) -> PlotBinaryArray {
    PlotBinaryArray {
        offset_bytes,
        shape,
        strides_bytes,
        dtype,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_bytes_are_little_endian_at_the_declared_offsets() {
        let file = tempfile::NamedTempFile::new().expect("payload file");
        let mut payload = Payload::open(file.path()).expect("writer");
        payload.f64(1.0).expect("f64");
        payload.f32(2.0).expect("f32");
        let array = payload.values([-3.0]).expect("array");
        assert_eq!(array.offset_bytes, 12);
        assert_eq!(array.shape, [1]);
        assert_eq!(payload.finish().expect("flush"), 20);
        assert_eq!(
            std::fs::read(file.path()).expect("bytes"),
            [
                0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 192,
            ]
        );
    }
}
