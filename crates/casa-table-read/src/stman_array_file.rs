// SPDX-License-Identifier: LGPL-3.0-or-later
//! Minimal reader for casacore `StManArrayFile` sidecar files.
//!
//! `IncrementalStMan` stores indirect array payloads in a sibling file with an
//! `i` suffix (for example `table.f0i`). The main ISM bucket stores an `Int64`
//! file offset for each array cell.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use super::{Float64ArrayCell, TableReadError};

#[derive(Debug)]
pub struct StManArrayFileReader {
    file: BufReader<File>,
    big_endian: bool,
    version: u32,
}

impl StManArrayFileReader {
    pub fn open(path: &Path, big_endian: bool) -> Result<Self, TableReadError> {
        let mut file = BufReader::new(File::open(path)?);
        let version = read_u32(&mut file, big_endian)?;
        let _file_length = read_i64(&mut file, big_endian)?;
        let mut padding = [0u8; 4];
        file.read_exact(&mut padding)?;
        Ok(Self {
            file,
            big_endian,
            version,
        })
    }

    pub fn read_f64_array_at(
        &mut self,
        offset: i64,
    ) -> Result<Option<Float64ArrayCell>, TableReadError> {
        if offset == 0 {
            return Ok(None);
        }

        self.file.seek(SeekFrom::Start(offset as u64))?;
        if self.version > 0 {
            let _ref_count = read_u32(&mut self.file, self.big_endian)?;
        }

        let ndim = read_u32(&mut self.file, self.big_endian)? as usize;
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            shape.push(read_i32(&mut self.file, self.big_endian)?);
        }

        let nrelem = shape.iter().try_fold(1usize, |acc, &dim| {
            let dim = usize::try_from(dim)
                .map_err(|_| TableReadError::Format(format!("negative array dimension {dim}")))?;
            acc.checked_mul(dim)
                .ok_or_else(|| TableReadError::Format("array size overflow".to_string()))
        })?;

        let mut values = Vec::with_capacity(nrelem);
        for _ in 0..nrelem {
            values.push(read_f64(&mut self.file, self.big_endian)?);
        }

        Ok(Some((shape, values)))
    }
}

fn read_u32(reader: &mut (impl Read + ?Sized), big_endian: bool) -> Result<u32, TableReadError> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(if big_endian {
        u32::from_be_bytes(buf)
    } else {
        u32::from_le_bytes(buf)
    })
}

fn read_i32(reader: &mut (impl Read + ?Sized), big_endian: bool) -> Result<i32, TableReadError> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(if big_endian {
        i32::from_be_bytes(buf)
    } else {
        i32::from_le_bytes(buf)
    })
}

fn read_i64(reader: &mut (impl Read + ?Sized), big_endian: bool) -> Result<i64, TableReadError> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(if big_endian {
        i64::from_be_bytes(buf)
    } else {
        i64::from_le_bytes(buf)
    })
}

fn read_f64(reader: &mut (impl Read + ?Sized), big_endian: bool) -> Result<f64, TableReadError> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(if big_endian {
        f64::from_be_bytes(buf)
    } else {
        f64::from_le_bytes(buf)
    })
}
