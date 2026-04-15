// SPDX-License-Identifier: LGPL-3.0-or-later
//! Disk-based VLA logical-record reassembly.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use crate::record::{
    AntennaDataArea, CdaId, CircularPolarization, CorrelatorDataArea, RecordControlArea,
    StokesProduct, SubarrayDataArea,
};
use crate::{VlaError, record::invalid_record};

/// Physical archive record size in bytes.
pub const PHYSICAL_RECORD_SIZE: usize = 2048;
/// Maximum physical archive record size in bytes for on-disk export files.
pub const DISK_PHYSICAL_RECORD_SIZE: usize = 13 * PHYSICAL_RECORD_SIZE;
/// Bytes of tape header at the front of each physical record.
pub const PHYSICAL_RECORD_HEADER_SIZE: usize = 4;
/// Bytes contributed by a 2048-byte synthetic physical record to the payload.
pub const PHYSICAL_RECORD_DATA_BYTES: usize = PHYSICAL_RECORD_SIZE - PHYSICAL_RECORD_HEADER_SIZE;
/// Maximum logical-record payload accepted by the native reader.
pub const MAX_LOGICAL_RECORD_SIZE: usize = 850_000;

/// Header at the front of each 2048-byte physical record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskPhysicalRecordHeader {
    /// One-indexed physical-record sequence number.
    pub current: u16,
    /// Total physical records in the logical record.
    pub total: u16,
}

impl DiskPhysicalRecordHeader {
    fn parse(bytes: &[u8]) -> Self {
        Self {
            current: u16::from_be_bytes([bytes[0], bytes[1]]),
            total: u16::from_be_bytes([bytes[2], bytes[3]]),
        }
    }
}

/// A fully reassembled logical VLA archive record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalRecord {
    path: PathBuf,
    bytes: Vec<u8>,
    physical_records: u16,
}

impl LogicalRecord {
    /// Return the source archive file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the logical-record bytes without physical-record headers.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Return the number of physical records used to assemble this record.
    pub fn physical_records(&self) -> u16 {
        self.physical_records
    }

    /// Build a Record Control Area view.
    pub fn rca(&self) -> RecordControlArea<'_> {
        RecordControlArea::new(&self.bytes)
    }

    /// Build a Subarray Data Area view.
    pub fn sda(&self) -> Result<SubarrayDataArea<'_>, String> {
        SubarrayDataArea::from_record(&self.bytes)
    }

    /// Build an Antenna Data Area view for the selected antenna index.
    pub fn ada(&self, which: usize) -> Result<AntennaDataArea<'_>, String> {
        let rca = self.rca();
        AntennaDataArea::new(&self.bytes, rca.ada_offset_bytes(which)?)
    }

    /// Build a Correlator Data Area view for the selected CDA.
    pub fn cda(&self, which: CdaId) -> Result<CorrelatorDataArea<'_>, String> {
        let rca = self.rca();
        let sda = self.sda()?;
        CorrelatorDataArea::new(
            &self.bytes,
            rca.cda_offset_bytes(which.index())?,
            rca.cda_baseline_bytes(which.index())?,
            rca.n_antennas()?,
            sda.true_channels(which)?,
        )
    }

    /// Derive Stokes products for the selected CDA and antenna pair.
    pub fn stokes_products(
        &self,
        cda: CdaId,
        ant1: usize,
        ant2: usize,
    ) -> Result<Vec<StokesProduct>, String> {
        let sda = self.sda()?;
        let ada1 = self.ada(ant1)?;
        let ada2 = self.ada(ant2)?;
        sda.if_usage(cda)?
            .into_iter()
            .map(|usage| {
                let first = ada1.if_polarization(usage.ant1)?;
                let second = ada2.if_polarization(usage.ant2)?;
                Ok(match (first, second) {
                    (CircularPolarization::Right, CircularPolarization::Right) => StokesProduct::Rr,
                    (CircularPolarization::Left, CircularPolarization::Left) => StokesProduct::Ll,
                    (CircularPolarization::Right, CircularPolarization::Left) => StokesProduct::Rl,
                    (CircularPolarization::Left, CircularPolarization::Right) => StokesProduct::Lr,
                })
            })
            .collect()
    }
}

/// Read disk-based VLA export files and reassemble logical records.
pub struct VlaDiskReader<R> {
    path: PathBuf,
    reader: R,
    block_index: u64,
    physical_record_mode: PhysicalRecordMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalRecordMode {
    Fixed(usize),
    DiskVariable,
}

impl VlaDiskReader<BufReader<File>> {
    /// Open a disk archive file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, VlaError> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)
            .map_err(|source| VlaError::io(format!("open {}", path.display()), source))?;
        Ok(Self::with_physical_record_size(
            path,
            BufReader::new(file),
            PhysicalRecordMode::DiskVariable,
        ))
    }
}

impl<R: Read> VlaDiskReader<R> {
    /// Create a reader from any `Read` implementation and an associated display path.
    pub fn new(path: impl Into<PathBuf>, reader: R) -> Self {
        Self::with_physical_record_size(
            path,
            reader,
            PhysicalRecordMode::Fixed(PHYSICAL_RECORD_SIZE),
        )
    }

    fn with_physical_record_size(
        path: impl Into<PathBuf>,
        reader: R,
        physical_record_mode: PhysicalRecordMode,
    ) -> Self {
        Self {
            path: path.into(),
            reader,
            block_index: 0,
            physical_record_mode,
        }
    }

    /// Read the next logical record from disk.
    pub fn next_record(&mut self) -> Result<Option<LogicalRecord>, VlaError> {
        match self.physical_record_mode {
            PhysicalRecordMode::Fixed(size) => self.next_record_fixed(size),
            PhysicalRecordMode::DiskVariable => self.next_record_disk_variable(),
        }
    }

    fn next_record_fixed(
        &mut self,
        physical_record_size: usize,
    ) -> Result<Option<LogicalRecord>, VlaError> {
        loop {
            let first_block = match self.read_fixed_block(physical_record_size)? {
                Some(block) => block,
                None => return Ok(None),
            };
            let header = DiskPhysicalRecordHeader::parse(&first_block);
            if header.current != 1 || header.total == 0 || header.total > 39 {
                continue;
            }

            let logical_words = i32::from_be_bytes([
                first_block[4],
                first_block[5],
                first_block[6],
                first_block[7],
            ]);
            if logical_words <= 0 {
                continue;
            }
            let logical_len = (logical_words as usize) * 2;
            if logical_len > MAX_LOGICAL_RECORD_SIZE {
                return Err(invalid_record(
                    &self.path,
                    format!("logical record exceeds limit: {logical_len} bytes"),
                ));
            }

            let mut bytes = vec![0_u8; logical_len];
            let physical_record_data_bytes = physical_record_size - PHYSICAL_RECORD_HEADER_SIZE;
            let first_copy = logical_len.min(physical_record_data_bytes);
            bytes[..first_copy].copy_from_slice(&first_block[4..4 + first_copy]);
            let mut written = first_copy;

            for expected in 2..=header.total {
                let block = self
                    .read_fixed_block(physical_record_size)?
                    .ok_or_else(|| {
                        invalid_record(
                            &self.path,
                            format!(
                                "unexpected EOF while reading logical record at block {}",
                                self.block_index
                            ),
                        )
                    })?;
                let next_header = DiskPhysicalRecordHeader::parse(&block);
                if next_header.current != expected || next_header.total != header.total {
                    return Err(invalid_record(
                        &self.path,
                        format!(
                            "physical-record sequence mismatch: expected ({expected},{}) got ({},{})",
                            header.total, next_header.current, next_header.total
                        ),
                    ));
                }
                let copy = (logical_len - written).min(physical_record_data_bytes);
                bytes[written..written + copy].copy_from_slice(&block[4..4 + copy]);
                written += copy;
            }

            if written != logical_len {
                return Err(invalid_record(
                    &self.path,
                    format!("logical record truncated: copied {written} of {logical_len} bytes"),
                ));
            }

            return Ok(Some(LogicalRecord {
                path: self.path.clone(),
                bytes,
                physical_records: header.total,
            }));
        }
    }

    fn next_record_disk_variable(&mut self) -> Result<Option<LogicalRecord>, VlaError> {
        loop {
            let first_sector = match self.read_fixed_block(PHYSICAL_RECORD_SIZE)? {
                Some(block) => block,
                None => return Ok(None),
            };
            let header = DiskPhysicalRecordHeader::parse(&first_sector);
            if header.current != 1 || header.total == 0 || header.total > 39 {
                continue;
            }

            let logical_words = i32::from_be_bytes([
                first_sector[4],
                first_sector[5],
                first_sector[6],
                first_sector[7],
            ]);
            if logical_words <= 0 {
                continue;
            }
            let logical_len = (logical_words as usize) * 2;
            if logical_len > MAX_LOGICAL_RECORD_SIZE {
                return Err(invalid_record(
                    &self.path,
                    format!("logical record exceeds limit: {logical_len} bytes"),
                ));
            }

            let first_record_size = if header.total == 1 {
                disk_physical_record_size_for_payload(logical_len)
            } else {
                DISK_PHYSICAL_RECORD_SIZE
            };
            let first_block = self.finish_disk_block(first_sector, first_record_size)?;

            let mut bytes = vec![0_u8; logical_len];
            let first_copy = logical_len.min(first_block.len() - PHYSICAL_RECORD_HEADER_SIZE);
            bytes[..first_copy].copy_from_slice(&first_block[4..4 + first_copy]);
            let mut written = first_copy;

            for expected in 2..=header.total {
                let remaining = logical_len - written;
                let record_size = if expected == header.total {
                    disk_physical_record_size_for_payload(remaining)
                } else {
                    DISK_PHYSICAL_RECORD_SIZE
                };
                let first_sector =
                    self.read_fixed_block(PHYSICAL_RECORD_SIZE)?
                        .ok_or_else(|| {
                            invalid_record(
                                &self.path,
                                format!(
                                    "unexpected EOF while reading logical record at block {}",
                                    self.block_index
                                ),
                            )
                        })?;
                let block = self.finish_disk_block(first_sector, record_size)?;
                let next_header = DiskPhysicalRecordHeader::parse(&block);
                if next_header.current != expected || next_header.total != header.total {
                    return Err(invalid_record(
                        &self.path,
                        format!(
                            "physical-record sequence mismatch: expected ({expected},{}) got ({},{})",
                            header.total, next_header.current, next_header.total
                        ),
                    ));
                }
                let copy = remaining.min(block.len() - PHYSICAL_RECORD_HEADER_SIZE);
                bytes[written..written + copy].copy_from_slice(&block[4..4 + copy]);
                written += copy;
            }

            if written != logical_len {
                return Err(invalid_record(
                    &self.path,
                    format!("logical record truncated: copied {written} of {logical_len} bytes"),
                ));
            }

            return Ok(Some(LogicalRecord {
                path: self.path.clone(),
                bytes,
                physical_records: header.total,
            }));
        }
    }

    fn read_fixed_block(&mut self, block_size: usize) -> Result<Option<Vec<u8>>, VlaError> {
        let mut block = vec![0_u8; block_size];
        let mut filled = 0;
        while filled < block_size {
            let n = self
                .reader
                .read(&mut block[filled..])
                .map_err(|source| VlaError::io("read VLA physical record", source))?;
            if n == 0 {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(invalid_record(
                    &self.path,
                    format!("partial physical record: read {filled} bytes"),
                ));
            }
            filled += n;
        }
        self.block_index += 1;
        Ok(Some(block))
    }

    fn finish_disk_block(
        &mut self,
        mut first_sector: Vec<u8>,
        total_size: usize,
    ) -> Result<Vec<u8>, VlaError> {
        if total_size < PHYSICAL_RECORD_SIZE || total_size % PHYSICAL_RECORD_SIZE != 0 {
            return Err(invalid_record(
                &self.path,
                format!("invalid disk physical-record size: {total_size} bytes"),
            ));
        }
        if total_size == PHYSICAL_RECORD_SIZE {
            return Ok(first_sector);
        }
        let remainder = self
            .read_fixed_block(total_size - PHYSICAL_RECORD_SIZE)?
            .ok_or_else(|| {
                invalid_record(
                    &self.path,
                    format!(
                        "unexpected EOF while finishing disk physical record at block {}",
                        self.block_index
                    ),
                )
            })?;
        first_sector.extend_from_slice(&remainder);
        Ok(first_sector)
    }
}

fn disk_physical_record_size_for_payload(payload_bytes: usize) -> usize {
    payload_bytes.div_ceil(PHYSICAL_RECORD_SIZE) * PHYSICAL_RECORD_SIZE
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn physical_block(current: u16, total: u16, payload: &[u8]) -> [u8; PHYSICAL_RECORD_SIZE] {
        let mut block = [0_u8; PHYSICAL_RECORD_SIZE];
        block[0..2].copy_from_slice(&current.to_be_bytes());
        block[2..4].copy_from_slice(&total.to_be_bytes());
        block[4..4 + payload.len()].copy_from_slice(payload);
        block
    }

    fn logical_record_bytes(length_bytes: usize, revision: u16, obs_day: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; length_bytes];
        bytes[0..4].copy_from_slice(&((length_bytes / 2) as i32).to_be_bytes());
        bytes[2 * 3..2 * 3 + 2].copy_from_slice(&revision.to_be_bytes());
        bytes[2 * 4..2 * 4 + 4].copy_from_slice(&obs_day.to_be_bytes());
        bytes[2 * 17..2 * 17 + 2].copy_from_slice(&27_u16.to_be_bytes());
        bytes
    }

    #[test]
    fn reads_single_physical_record() {
        let logical = logical_record_bytes(64, 26, 49_999);
        let block = physical_block(1, 1, &logical);
        let mut reader = VlaDiskReader::new("synthetic.xp1", Cursor::new(block.to_vec()));
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.bytes(), logical.as_slice());
        let rca = record.rca();
        assert_eq!(rca.length_bytes().unwrap(), 64);
        assert_eq!(rca.revision().unwrap(), 26);
        assert_eq!(rca.obs_day().unwrap(), 49_999);
    }

    #[test]
    fn reads_multi_physical_record_logical_record() {
        let logical = logical_record_bytes(PHYSICAL_RECORD_DATA_BYTES + 200, 27, 50_123);
        let block1 = physical_block(1, 2, &logical[..PHYSICAL_RECORD_DATA_BYTES]);
        let block2 = physical_block(2, 2, &logical[PHYSICAL_RECORD_DATA_BYTES..]);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&block1);
        bytes.extend_from_slice(&block2);

        let mut reader = VlaDiskReader::new("synthetic.xp2", Cursor::new(bytes));
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.bytes(), logical.as_slice());
        assert_eq!(record.physical_records(), 2);
        assert_eq!(record.rca().length_bytes().unwrap() as usize, logical.len());
    }

    #[test]
    fn skips_blocks_until_start_of_logical_record() {
        let logical = logical_record_bytes(80, 24, 48_000);
        let junk = physical_block(0, 0, &[1, 2, 3, 4]);
        let block = physical_block(1, 1, &logical);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&junk);
        bytes.extend_from_slice(&block);

        let mut reader = VlaDiskReader::new("synthetic.xp3", Cursor::new(bytes));
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.bytes(), logical.as_slice());
    }
}
