// SPDX-License-Identifier: LGPL-3.0-or-later
//! The casacore MultiFile container. Data managers seek within logical files
//! whose blocks can live anywhere in the container.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use casa_aipsio::AipsIo;

use super::StorageError;

const LEAD_V2: usize = 64;
const CONTINUATION_POINTER: usize = 8;

mod write;
pub(super) use write::pack_staged_manager_files;

pub(super) struct MultiFile {
    file: Arc<PositionedFile>,
    block_size: u64,
    entries: BTreeMap<String, Arc<Entry>>,
    checksums: Option<Arc<[u32]>>,
}

struct Entry {
    byte_count: u64,
    runs: Vec<BlockRun>,
}

#[derive(Clone, Copy)]
struct BlockRun {
    first: u64,
    logical_first: u64,
    count: u64,
}

struct Header {
    bytes: Vec<u8>,
    block_size: u64,
    block_count: u64,
    continuation: Vec<u64>,
    use_checksum: bool,
}

impl MultiFile {
    pub(super) fn open(path: &Path) -> Result<Self, StorageError> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let file = Arc::new(PositionedFile::new(file));
        let header = Header::read(&file, file_len)?;
        let block_size = header.block_size;
        let (entries, checksums) = header.parse()?;
        Ok(Self { file, block_size, entries, checksums })
    }

    pub(super) fn entry(&self, name: &str) -> Option<MultiFileEntry> {
        Some(MultiFileEntry {
            file: Arc::clone(&self.file),
            entry: Arc::clone(self.entries.get(name)?),
            block_size: self.block_size,
            checksums: self.checksums.clone(),
            position: 0,
        })
    }
}

impl Header {
    fn read(file: &PositionedFile, file_len: u64) -> Result<Self, StorageError> {
        let mut lead = [0; LEAD_V2];
        file.read_exact_at(&mut lead, 0)?;
        if u64_at(&lead, 0) != 0 || u32_at(&lead, 24) != 2 {
            return Err(format_error("unsupported MultiFile header version"));
        }
        let mut next = u64_at(&lead, 8);
        let header_size = u64_at(&lead, 32);
        let block_size = u64_at(&lead, 40);
        let block_count = u64_at(&lead, 48);
        if block_size < LEAD_V2 as u64
            || block_count == 0
            || block_size.checked_mul(block_count).is_none_or(|extent| extent > file_len)
            || header_size < LEAD_V2 as u64
            || header_size > file_len
        {
            return Err(format_error("invalid MultiFile block or header extent"));
        }
        let len = usize::try_from(header_size).map_err(|_| format_error("MultiFile header is too large"))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| format_error("MultiFile header allocation failed"))?;
        bytes.resize(len, 0);
        let first = header_size.min(block_size) as usize;
        file.read_exact_at(&mut bytes[..first], 0)?;
        let mut seen = BTreeSet::new();
        let mut continuation = Vec::new();
        let mut offset = first;
        while offset < len {
            if next == 0 || next >= block_count || !seen.insert(next) {
                return Err(format_error("invalid MultiFile continuation block"));
            }
            continuation.push(next);
            let start = next * block_size;
            let mut pointer = [0; CONTINUATION_POINTER];
            file.read_exact_at(&mut pointer, start)?;
            next = u64::from_be_bytes(pointer);
            let size = (len - offset).min((block_size - CONTINUATION_POINTER as u64) as usize);
            file.read_exact_at(&mut bytes[offset..offset + size], start + CONTINUATION_POINTER as u64)?;
            offset += size;
        }
        if next != 0 {
            return Err(format_error("extra MultiFile continuation blocks"));
        }
        let use_checksum = lead[56] != 0;
        if use_checksum {
            let expected = u32_at(&lead, 28);
            bytes[28..32].fill(0);
            if checksum(&bytes) != expected {
                return Err(format_error("MultiFile header checksum mismatch"));
            }
        }
        Ok(Self { bytes, block_size, block_count, continuation, use_checksum })
    }

    fn parse(self) -> Result<(BTreeMap<String, Arc<Entry>>, Option<Arc<[u32]>>), StorageError> {
        let mut bytes = Cursor::new(self.bytes);
        bytes.set_position(LEAD_V2 as u64);
        let mut io = AipsIo::new_read_only(bytes);
        if io.getstart("MultiFile")? != 2 || io.getstart("Block")? != 1 {
            return Err(format_error("invalid MultiFile entry directory version"));
        }
        let count = io.get_u32()? as usize;
        let mut names = Vec::new();
        for _ in 0..count {
            let name = io.get_string()?;
            let byte_count = u64::try_from(io.get_i64()?).map_err(|_| format_error("negative MultiFile entry length"))?;
            let _nested = io.get_bool()?;
            names.push((name, byte_count));
        }
        io.getend()?;
        io.getend()?;
        let mut tail = io.into_inner_typed::<Cursor<Vec<u8>>>()?;
        let mut entries = BTreeMap::new();
        let mut extents = Vec::new();
        for (name, byte_count) in names {
            let packed = read_vector(&mut tail, read_i64, 8)?;
            let runs = unpack_runs(&packed, self.block_count)?;
            let blocks = runs.last().map_or(0, |run| run.logical_first + run.count);
            if blocks != byte_count.div_ceil(self.block_size) {
                return Err(format_error("MultiFile entry extent differs from its length"));
            }
            extents.extend(runs.iter().map(|run| (run.first, run.first + run.count)));
            if !name.is_empty() && entries.insert(name, Arc::new(Entry { byte_count, runs })).is_some() {
                return Err(format_error("duplicate MultiFile entry name"));
            }
        }
        let free = read_vector(&mut tail, read_i64, 8)?;
        extents.extend(unpack_runs(&free, self.block_count)?.into_iter().map(|run| (run.first, run.first + run.count)));
        let checksums = read_vector(&mut tail, read_u32, 4)?;
        if (self.use_checksum && checksums.len() as u64 != self.block_count)
            || (!self.use_checksum && !checksums.is_empty())
        {
            return Err(format_error("invalid MultiFile checksum extent"));
        }
        let first = read_vector(&mut tail, read_i64, 8)?;
        let second = read_vector(&mut tail, read_i64, 8)?;
        let first_used = read_i64(&mut tail)?;
        let second_used = read_i64(&mut tail)?;
        if first_used < 0 || first_used as u64 > first.len() as u64
            || second_used < 0 || second_used as u64 > second.len() as u64
            || tail.position() != tail.get_ref().len() as u64
        {
            return Err(format_error("invalid MultiFile continuation directory"));
        }
        let continuations = first.into_iter().chain(second).map(|block| {
            u64::try_from(block).ok().filter(|&block| block > 0 && block < self.block_count)
                .ok_or_else(|| format_error("invalid MultiFile continuation extent"))
        }).collect::<Result<BTreeSet<_>, _>>()?;
        if self.continuation.iter().any(|block| !continuations.contains(block)) {
            return Err(format_error("MultiFile continuation chain is absent from its directory"));
        }
        extents.extend(continuations.into_iter().map(|block| (block, block + 1)));
        extents.sort_unstable();
        if extents.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(format_error("overlapping MultiFile block extents"));
        }
        Ok((entries, self.use_checksum.then(|| checksums.into())))
    }
}

pub(super) struct MultiFileEntry {
    file: Arc<PositionedFile>,
    entry: Arc<Entry>,
    block_size: u64,
    checksums: Option<Arc<[u32]>>,
    position: u64,
}

impl MultiFileEntry {
    pub(super) fn len(&self) -> u64 {
        self.entry.byte_count
    }

    pub(super) fn read_exact_at(&self, bytes: &mut [u8], position: u64) -> io::Result<()> {
        if self.read_at(bytes, position)? != bytes.len() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        Ok(())
    }

    fn read_at(&self, buffer: &mut [u8], mut position: u64) -> io::Result<usize> {
        let count = (buffer.len() as u64).min(self.len().saturating_sub(position)) as usize;
        let mut remaining = &mut buffer[..count];
        while !remaining.is_empty() {
            let logical_block = position / self.block_size;
            let run = self.entry.runs[self.entry.runs.partition_point(|run| run.logical_first + run.count <= logical_block)];
            let block = run.first + logical_block - run.logical_first;
            let within = position % self.block_size;
            let available = (run.first + run.count - block) * self.block_size - within;
            let size = if let Some(checksums) = &self.checksums {
                let mut block_data = vec![0; self.block_size as usize];
                self.file.read_exact_at(&mut block_data, block * self.block_size)?;
                if checksum(&block_data) != checksums[block as usize] {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "MultiFile block checksum mismatch"));
                }
                let size = (remaining.len() as u64).min(self.block_size - within) as usize;
                remaining[..size].copy_from_slice(&block_data[within as usize..within as usize + size]);
                size
            } else {
                let size = (remaining.len() as u64).min(available) as usize;
                self.file.read_exact_at(&mut remaining[..size], block * self.block_size + within)?;
                size
            };
            remaining = &mut remaining[size..];
            position += size as u64;
        }
        Ok(count)
    }
}

impl Read for MultiFileEntry {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.read_at(buffer, self.position)?;
        self.position += count as u64;
        Ok(count)
    }
}

impl Seek for MultiFileEntry {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = match position {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self.len().checked_add_signed(offset),
        }.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid MultiFile entry seek"))?;
        self.position = position;
        Ok(position)
    }
}

fn unpack_runs(indices: &[i64], block_count: u64) -> Result<Vec<BlockRun>, StorageError> {
    let mut runs: Vec<BlockRun> = Vec::new();
    let mut logical = 0u64;
    for &index in indices {
        if index > 0 && (index as u64) < block_count {
            runs.push(BlockRun { first: index as u64, logical_first: logical, count: 1 });
            logical = logical.checked_add(1).ok_or_else(|| format_error("MultiFile entry length overflows"))?;
        } else if index < 0 && let Some(run) = runs.last_mut() {
            let additional = index.unsigned_abs();
            if run.first.checked_add(run.count).and_then(|end| end.checked_add(additional)).is_none_or(|end| end > block_count) {
                return Err(format_error("MultiFile packed run exceeds its container"));
            }
            run.count += additional;
            logical = logical.checked_add(additional).ok_or_else(|| format_error("MultiFile entry length overflows"))?;
        } else {
            return Err(format_error("invalid MultiFile block number"));
        }
    }
    Ok(runs)
}

fn read_vector<T>(
    reader: &mut Cursor<Vec<u8>>,
    read: fn(&mut Cursor<Vec<u8>>) -> io::Result<T>,
    width: u64,
) -> Result<Vec<T>, StorageError> {
    let count = u64::try_from(read_i64(reader)?).map_err(|_| format_error("negative MultiFile vector length"))?;
    if count > (reader.get_ref().len() as u64).saturating_sub(reader.position()) / width {
        return Err(format_error("MultiFile vector exceeds its header"));
    }
    (0..count).map(|_| read(reader).map_err(StorageError::from)).collect()
}

fn read_i64(reader: &mut Cursor<Vec<u8>>) -> io::Result<i64> {
    let mut buffer = [0; 8];
    reader.read_exact(&mut buffer)?;
    Ok(i64::from_be_bytes(buffer))
}

fn read_u32(reader: &mut Cursor<Vec<u8>>) -> io::Result<u32> {
    let mut buffer = [0; 4];
    reader.read_exact(&mut buffer)?;
    Ok(u32::from_be_bytes(buffer))
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("validated MultiFile header"))
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("validated MultiFile header"))
}

fn format_error(message: &str) -> StorageError {
    StorageError::FormatMismatch(message.to_owned())
}

fn checksum(bytes: &[u8]) -> u32 {
    const POLYNOMIAL: u32 = 0x04c1_1db7;
    let mut crc = 0x46af_6449u32;
    for byte in bytes.iter().copied().chain([0; 4]) {
        let mut high = crc & 0xff00_0000;
        for _ in 0..8 {
            let top = high & 0x8000_0000;
            high <<= 1;
            if top != 0 {
                high ^= POLYNOMIAL;
            }
        }
        crc = ((crc << 8) | u32::from(byte)) ^ high;
    }
    crc ^ 0xffff_ffff
}

struct PositionedFile {
    #[cfg(unix)]
    inner: File,
    #[cfg(not(unix))]
    inner: std::sync::Mutex<File>,
}

impl PositionedFile {
    fn new(file: File) -> Self {
        Self {
            #[cfg(unix)]
            inner: file,
            #[cfg(not(unix))]
            inner: std::sync::Mutex::new(file),
        }
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.inner.read_exact_at(bytes, offset)
        }
        #[cfg(not(unix))]
        {
            let mut file = self.inner.lock().map_err(|error| io::Error::other(error.to_string()))?;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(bytes)
        }
    }
}
