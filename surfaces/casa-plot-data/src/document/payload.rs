// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stream complete plot arrays to the caller's file, never into JSON.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::{PlotArrayDtype, PlotBinaryArray};

/// Writes the little-endian arrays referenced by a plot document.
pub struct PlotPayloadWriter {
    file: BufWriter<std::fs::File>,
    bytes: u64,
}

impl PlotPayloadWriter {
    /// Replace the contents of the caller-owned payload file.
    ///
    /// # Errors
    /// Returns the underlying file I/O error.
    pub fn open(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new().write(true).truncate(true).open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
            bytes: 0,
        })
    }

    /// Keep earlier arrays and append to the caller-owned payload file.
    ///
    /// # Errors
    /// Returns the underlying file I/O error.
    pub fn append(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new().append(true).open(path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            file: BufWriter::new(file),
            bytes,
        })
    }

    /// The byte offset where the next element will be written.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.bytes
    }

    /// Write one double-precision coordinate.
    ///
    /// # Errors
    /// Returns the underlying file I/O error.
    pub fn f64(&mut self, value: f64) -> Result<(), std::io::Error> {
        self.file.write_all(&value.to_le_bytes())?;
        self.bytes += 8;
        Ok(())
    }

    /// Write one single-precision pixel or coordinate.
    ///
    /// # Errors
    /// Returns the underlying file I/O error.
    pub fn f32(&mut self, value: f32) -> Result<(), std::io::Error> {
        self.file.write_all(&value.to_le_bytes())?;
        self.bytes += 4;
        Ok(())
    }

    /// Write a contiguous sequence of double-precision coordinates.
    ///
    /// # Errors
    /// Returns the underlying file I/O error.
    pub fn values(
        &mut self,
        values: impl IntoIterator<Item = f64>,
    ) -> Result<PlotBinaryArray, std::io::Error> {
        let start = self.bytes;
        for value in values {
            self.f64(value)?;
        }
        Ok(plot_array_view(
            start,
            vec![(self.bytes - start) / 8],
            vec![],
            PlotArrayDtype::F64Le,
        ))
    }

    /// Flush all writes and return the final byte length.
    ///
    /// # Errors
    /// Returns the underlying file I/O error.
    pub fn finish(mut self) -> Result<u64, std::io::Error> {
        self.file.flush()?;
        Ok(self.bytes)
    }
}

/// Describe an array at a given byte offset in a shared plot payload.
#[must_use]
pub fn plot_array_view(
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
    fn composed_writers_keep_earlier_views_and_standalone_writers_replace() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut first = PlotPayloadWriter::open(file.path()).unwrap();
        let a = first.values([1., 2.]).unwrap();
        assert_eq!(a.offset_bytes, 0);
        first.finish().unwrap();
        let mut second = PlotPayloadWriter::append(file.path()).unwrap();
        let b = second.values([3.]).unwrap();
        assert_eq!(b.offset_bytes, 16);
        assert_eq!(second.finish().unwrap(), 24);
        let values = std::fs::read(file.path())
            .unwrap()
            .as_chunks::<8>()
            .0
            .iter()
            .map(|bytes| f64::from_le_bytes(*bytes))
            .collect::<Vec<_>>();
        assert_eq!(values, [1., 2., 3.]);
        let mut replacement = PlotPayloadWriter::open(file.path()).unwrap();
        replacement.values([4.]).unwrap();
        assert_eq!(replacement.finish().unwrap(), 8);
    }

    #[test]
    fn payload_bytes_are_little_endian_at_the_declared_offsets() {
        let file = tempfile::NamedTempFile::new().expect("payload file");
        let mut payload = PlotPayloadWriter::open(file.path()).expect("writer");
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
