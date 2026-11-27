// SPDX-License-Identifier: LGPL-3.0-or-later
//! The casacore MultiFile container. Data managers seek within logical files
//! whose blocks can live anywhere in the container.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use super::StorageError;

const LEAD_V2: usize = 64;
const CONTINUATION_POINTER: usize = 8;
// Bound index memory independently of the container's size and sparsity.
const MAX_HEADER_BYTES: u64 = 64 * 1024 * 1024;

mod edit;
mod write;
pub(crate) use edit::PackedManagerEdit;
pub(crate) use write::pack_staged_manager_files;

#[cfg(test)]
mod tests;

pub(super) fn supports_multifile(manager_type: &str) -> bool {
    matches!(manager_type, "StandardStMan" | "IncrementalStMan")
        || manager_type.starts_with("Tiled")
}

pub(super) fn manager_sequence(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("table.f")?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let (sequence, suffix) = rest.split_at(digits);
    if suffix.is_empty()
        || suffix == "i"
        || suffix.strip_prefix("_TSM").is_some_and(|index| {
            !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        sequence.parse().ok()
    } else {
        None
    }
}

pub(super) struct MultiFile {
    file: Arc<PositionedFile>,
    block_size: u64,
    header_counter: u64,
    entries: BTreeMap<String, Arc<Entry>>,
    slots: Vec<String>,
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
    counter: u64,
    continuation: Vec<u64>,
    use_checksum: bool,
}

impl MultiFile {
    pub(super) fn open(path: &Path) -> Result<Self, StorageError> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let file = Arc::new(PositionedFile::new(file));
        Header::read(&file, file_len)?.parse(file)
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
            || block_size
                .checked_mul(block_count)
                .is_none_or(|extent| extent > file_len)
            || header_size < LEAD_V2 as u64
            || header_size > file_len
            || header_size > MAX_HEADER_BYTES
        {
            return Err(format_error("invalid MultiFile block or header extent"));
        }
        let len = usize::try_from(header_size)
            .map_err(|_| format_error("MultiFile header is too large"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| format_error("MultiFile header allocation failed"))?;
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
            file.read_exact_at(
                &mut bytes[offset..offset + size],
                start + CONTINUATION_POINTER as u64,
            )?;
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
        Ok(Self {
            bytes,
            block_size,
            block_count,
            counter: u64_at(&lead, 16),
            continuation,
            use_checksum,
        })
    }

    fn parse(self, file: Arc<PositionedFile>) -> Result<MultiFile, StorageError> {
        let (names, offset) = read_directory(&self.bytes)?;
        let mut tail = Cursor::new(self.bytes.as_slice());
        tail.set_position(offset as u64);
        let mut entries = BTreeMap::new();
        let mut slots = Vec::with_capacity(names.len());
        let mut extents = Vec::new();
        for (name, byte_count) in names {
            let packed = read_vector(&mut tail, read_i64, 8)?;
            let runs = unpack_runs(&packed, self.block_count)?;
            let blocks = runs.last().map_or(0, |run| run.logical_first + run.count);
            if blocks != byte_count.div_ceil(self.block_size) {
                return Err(format_error(
                    "MultiFile entry extent differs from its length",
                ));
            }
            extents.extend(runs.iter().map(|run| (run.first, run.first + run.count)));
            if !name.is_empty()
                && entries
                    .insert(name.clone(), Arc::new(Entry { byte_count, runs }))
                    .is_some()
            {
                return Err(format_error("duplicate MultiFile entry name"));
            }
            slots.push(name);
        }
        let free = read_vector(&mut tail, read_i64, 8)?;
        let checksums = read_vector(&mut tail, read_u32, 4)?;
        let checksum_blocks = checksums.len() as u64;
        let valid_checksums = if self.use_checksum {
            checksum_blocks <= self.block_count
                && extents.iter().all(|&(_, end)| end <= checksum_blocks)
        } else {
            checksums.is_empty()
        };
        if !valid_checksums {
            return Err(format_error("invalid MultiFile checksum extent"));
        }
        extents.extend(
            unpack_runs(&free, self.block_count)?
                .into_iter()
                .map(|run| (run.first, run.first + run.count)),
        );
        self.read_continuations(&mut tail, &mut extents)?;
        extents.sort_unstable();
        if extents.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(format_error("overlapping MultiFile block extents"));
        }
        Ok(MultiFile {
            file,
            block_size: self.block_size,
            header_counter: self.counter,
            entries,
            slots,
            checksums: self.use_checksum.then(|| checksums.into()),
        })
    }

    fn read_continuations(
        &self,
        tail: &mut Cursor<&[u8]>,
        extents: &mut Vec<(u64, u64)>,
    ) -> Result<(), StorageError> {
        let first = read_vector(tail, read_i64, 8)?;
        let second = read_vector(tail, read_i64, 8)?;
        let first_used = read_u32(tail)? as usize;
        let second_used = read_u32(tail)? as usize;
        if tail.position() != tail.get_ref().len() as u64 {
            return Err(format_error("invalid MultiFile continuation directory"));
        }
        let mut matched = self.continuation.is_empty();
        for (blocks, used) in [(first, first_used), (second, second_used)] {
            if used > blocks.len() {
                return Err(format_error("invalid MultiFile continuation count"));
            }
            let reserved = blocks
                .into_iter()
                .map(|block| {
                    u64::try_from(block)
                        .ok()
                        .filter(|&block| block > 0 && block < self.block_count)
                        .ok_or_else(|| format_error("invalid MultiFile continuation extent"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            matched |= reserved[..used].starts_with(&self.continuation);
            extents.extend(reserved.into_iter().map(|block| (block, block + 1)));
        }
        if !matched {
            return Err(format_error(
                "MultiFile continuation chain is absent from its directory",
            ));
        }
        Ok(())
    }
}

fn read_directory(header: &[u8]) -> Result<(Vec<(String, u64)>, usize), StorageError> {
    let mut tail = &header[LEAD_V2..];
    if read_u32(&mut tail)? != 0xbebe_bebe {
        return Err(format_error("invalid MultiFile directory magic"));
    }
    let mut directory = read_object(&mut tail, b"MultiFile", 2)?;
    let mut members = read_object(&mut directory, b"Block", 1)?;
    let count = read_u32(&mut members)? as usize;
    if !directory.is_empty() || count > members.len() / 13 {
        return Err(format_error("invalid MultiFile directory length"));
    }
    let mut names = Vec::new();
    for _ in 0..count {
        let name_bytes = read_u32(&mut members)? as usize;
        let name = std::str::from_utf8(take(&mut members, name_bytes)?)
            .map_err(|_| format_error("invalid MultiFile member name"))?;
        let byte_count = u64::try_from(read_i64(&mut members)?)
            .map_err(|_| format_error("negative MultiFile entry length"))?;
        let _nested = take(&mut members, 1)?;
        names.push((name.to_owned(), byte_count));
    }
    if !members.is_empty() {
        return Err(format_error("invalid MultiFile member directory length"));
    }
    Ok((names, header.len() - tail.len()))
}

fn read_object<'a>(
    bytes: &mut &'a [u8],
    name: &[u8],
    version: u32,
) -> Result<&'a [u8], StorageError> {
    // AipsIO object length includes the four-byte length field itself.
    let length = (read_u32(bytes)? as usize)
        .checked_sub(4)
        .ok_or_else(|| format_error("invalid MultiFile object length"))?;
    let mut object = take(bytes, length)?;
    let name_length = read_u32(&mut object)? as usize;
    if take(&mut object, name_length)? != name || read_u32(&mut object)? != version {
        return Err(format_error("invalid MultiFile entry directory version"));
    }
    Ok(object)
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], StorageError> {
    let (front, tail) = bytes
        .split_at_checked(length)
        .ok_or_else(|| format_error("MultiFile directory field exceeds its object"))?;
    *bytes = tail;
    Ok(front)
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
            let run = self.entry.runs[self
                .entry
                .runs
                .partition_point(|run| run.logical_first + run.count <= logical_block)];
            let block = run.first + logical_block - run.logical_first;
            let within = position % self.block_size;
            let available = (run.first + run.count - block) * self.block_size - within;
            let size = if let Some(checksums) = &self.checksums {
                let size = (remaining.len() as u64).min(self.block_size - within) as usize;
                self.read_checksummed_block(
                    block,
                    within,
                    &mut remaining[..size],
                    checksums[block as usize],
                )?;
                size
            } else {
                let size = (remaining.len() as u64).min(available) as usize;
                self.file
                    .read_exact_at(&mut remaining[..size], block * self.block_size + within)?;
                size
            };
            remaining = &mut remaining[size..];
            position += size as u64;
        }
        Ok(count)
    }

    fn read_checksummed_block(
        &self,
        block: u64,
        within: u64,
        data: &mut [u8],
        expected: u32,
    ) -> io::Result<()> {
        let mut scratch = [0; 8192];
        let mut offset = 0;
        let mut crc = CRC_START;
        let requested_end = within + data.len() as u64;
        while offset < self.block_size {
            let size = (self.block_size - offset).min(scratch.len() as u64);
            let bytes = &mut scratch[..size as usize];
            self.file
                .read_exact_at(bytes, block * self.block_size + offset)?;
            crc = checksum_update(crc, bytes);
            let start = offset.max(within);
            let end = (offset + size).min(requested_end);
            if start < end {
                data[(start - within) as usize..(end - within) as usize]
                    .copy_from_slice(&bytes[(start - offset) as usize..(end - offset) as usize]);
            }
            offset += size;
        }
        if checksum_finish(crc) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MultiFile block checksum mismatch",
            ));
        }
        Ok(())
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
        }
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid MultiFile entry seek")
        })?;
        self.position = position;
        Ok(position)
    }
}

fn unpack_runs(indices: &[i64], block_count: u64) -> Result<Vec<BlockRun>, StorageError> {
    let mut runs: Vec<BlockRun> = Vec::new();
    let mut logical = 0u64;
    for &index in indices {
        if index > 0 && (index as u64) < block_count {
            runs.push(BlockRun {
                first: index as u64,
                logical_first: logical,
                count: 1,
            });
            logical = logical
                .checked_add(1)
                .ok_or_else(|| format_error("MultiFile entry length overflows"))?;
        } else if index < 0
            && let Some(run) = runs.last_mut()
        {
            let additional = index.unsigned_abs();
            if run
                .first
                .checked_add(run.count)
                .and_then(|end| end.checked_add(additional))
                .is_none_or(|end| end > block_count)
            {
                return Err(format_error("MultiFile packed run exceeds its container"));
            }
            run.count += additional;
            logical = logical
                .checked_add(additional)
                .ok_or_else(|| format_error("MultiFile entry length overflows"))?;
        } else {
            return Err(format_error("invalid MultiFile block number"));
        }
    }
    Ok(runs)
}

fn read_vector<'a, T>(
    reader: &mut Cursor<&'a [u8]>,
    read: fn(&mut Cursor<&'a [u8]>) -> io::Result<T>,
    width: u64,
) -> Result<Vec<T>, StorageError> {
    let count = u64::try_from(read_i64(reader)?)
        .map_err(|_| format_error("negative MultiFile vector length"))?;
    if count > (reader.get_ref().len() as u64).saturating_sub(reader.position()) / width {
        return Err(format_error("MultiFile vector exceeds its header"));
    }
    (0..count)
        .map(|_| read(reader).map_err(StorageError::from))
        .collect()
}

fn read_i64(reader: &mut impl Read) -> io::Result<i64> {
    let mut buffer = [0; 8];
    reader.read_exact(&mut buffer)?;
    Ok(i64::from_be_bytes(buffer))
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut buffer = [0; 4];
    reader.read_exact(&mut buffer)?;
    Ok(u32::from_be_bytes(buffer))
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated MultiFile header"),
    )
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("validated MultiFile header"),
    )
}

fn format_error(message: &str) -> StorageError {
    StorageError::FormatMismatch(message.to_owned())
}

const CRC_START: u32 = 0x46af_6449;
const CRC_TABLE: [u32; 256] = crc_table();

const fn crc_table() -> [u32; 256] {
    let mut entries = [0; 256];
    let mut index = 0;
    while index < entries.len() {
        let mut value = (index as u32) << 24;
        let mut bit = 0;
        while bit < 8 {
            let top = value & 0x8000_0000;
            value <<= 1;
            if top != 0 {
                value ^= 0x04c1_1db7;
            }
            bit += 1;
        }
        entries[index] = value;
        index += 1;
    }
    entries
}

fn checksum(bytes: &[u8]) -> u32 {
    checksum_finish(checksum_update(CRC_START, bytes))
}

fn checksum_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc = ((crc << 8) | u32::from(byte)) ^ CRC_TABLE[(crc >> 24) as usize];
    }
    crc
}

fn checksum_finish(crc: u32) -> u32 {
    checksum_update(crc, &[0; 4]) ^ 0xffff_ffff
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
            let mut file = self
                .inner
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(bytes)
        }
    }
}
