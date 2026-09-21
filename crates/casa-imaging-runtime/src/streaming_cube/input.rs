// SPDX-License-Identifier: LGPL-3.0-or-later

//! One run-private store for a homogeneous selected native-channel layout.
//!
//! Row blocks contain one metadata frame followed by native-channel tiles.
//! Every offset is derived from the shape; no resident per-row/tile directory
//! or per-sample object graph is needed. The writer transfers its original open
//! file directly to readers. There is no reopen/import or publication authority.

use std::{
    io::{self, Write},
    mem::size_of,
    ops::Range,
    os::unix::fs::FileExt,
    time::Instant,
};

use casa_imaging_reconstruction::runtime_adapter::NativeBlock;
#[cfg(test)]
use casa_imaging_reconstruction::runtime_adapter::RowMetadata;
use num_complex::Complex64;
use tempfile::NamedTempFile;

use crate::bounded_stream::{OrderedBlockSource, SourceFillCancellation, SourcePoll};
use crate::managed_spill::{
    ManagedSpillStorage, configure_bounded_page_cache, page_cache_window_bytes, release_page_cache,
};

const ROW_BYTES: usize = 7 * 8;
const SAMPLE_BYTES: usize = 2 * 8 + 8 + 2;
const CRC_BYTES: usize = 4;

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn overflow() -> io::Error {
    invalid_input("native cube store size overflow")
}

/// Byte arithmetic for a selected native layout, not an imaging scheduler.
/// `tile_channels` comes from the parent band's useful native window. The row
/// count is derived from the admitted preparation-buffer budget, never tuned
/// to a particular dataset. Source blocks/kernel/owner storage are additional
/// live allocations that the parent execution plan must count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StorePlan {
    pub(super) rows: u64,
    pub(super) channels: usize,
    pub(super) correlations: usize,
    pub(super) block_rows: usize,
    pub(super) tile_channels: usize,
    tiles: usize,
    channel_bytes: usize,
    row_bytes: usize,
    frame_bytes: usize,
    preparation_bytes: usize,
    page_cache_bytes: u64,
    pub(super) artifact_bytes: u64,
}

impl StorePlan {
    /// Batch across weighted callback boundaries using the selected source's
    /// I/O-buffer envelope. Reserve at least one complete native row; the parent
    /// separately admits this arena alongside the still-live selected source.
    pub(super) fn for_source_buffer(
        rows: u64,
        channels: usize,
        correlations: usize,
        tile_channels: usize,
        source_buffer_bytes: usize,
    ) -> io::Result<Self> {
        if source_buffer_bytes == 0 {
            return Err(invalid_input("native source buffer is empty"));
        }
        let (row_bytes, encoded_per_row) = Self::row_layout(channels, correlations, tile_channels)?;
        let working_bytes = row_bytes
            .checked_add(encoded_per_row)
            .and_then(|bytes| {
                bytes.checked_add(size_of::<NativeBlock>() + size_of::<Vec<u8>>() + CRC_BYTES)
            })
            .ok_or_else(overflow)?
            .max(source_buffer_bytes);
        Self::new(
            rows,
            channels,
            correlations,
            tile_channels,
            working_bytes,
            u64::MAX,
        )
    }

    fn row_layout(
        channels: usize,
        correlations: usize,
        tile_channels: usize,
    ) -> io::Result<(usize, usize)> {
        if channels == 0
            || !(1..=4).contains(&correlations)
            || tile_channels == 0
            || tile_channels > channels
        {
            return Err(invalid_input("invalid native cube store shape"));
        }
        let channel_bytes = SAMPLE_BYTES
            .checked_mul(correlations)
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or_else(overflow)?;
        let row_bytes = channels
            .checked_mul(channel_bytes)
            .and_then(|bytes| bytes.checked_add(ROW_BYTES))
            .ok_or_else(overflow)?;
        let encoded_per_row = tile_channels
            .checked_mul(channel_bytes)
            .ok_or_else(overflow)?
            .max(ROW_BYTES);
        Ok((row_bytes, encoded_per_row))
    }

    pub(super) fn new(
        rows: u64,
        channels: usize,
        correlations: usize,
        tile_channels: usize,
        working_bytes: usize,
        storage_bytes: u64,
    ) -> io::Result<Self> {
        if rows == 0 {
            return Err(invalid_input("invalid native cube store shape"));
        }
        let (row_bytes, encoded_per_row) = Self::row_layout(channels, correlations, tile_channels)?;
        let channel_bytes = SAMPLE_BYTES * correlations + 8;
        let fixed_bytes = size_of::<NativeBlock>() + size_of::<Vec<u8>>() + CRC_BYTES;
        let available = working_bytes
            .checked_sub(fixed_bytes)
            .ok_or_else(overflow)?;
        let block_rows = (available
            / row_bytes
                .checked_add(encoded_per_row)
                .ok_or_else(overflow)?)
        .min(usize::try_from(rows).unwrap_or(usize::MAX));
        if block_rows == 0 {
            return Err(invalid_input(
                "native store budget cannot hold one row and encoded tile",
            ));
        }
        let frame_bytes = block_rows
            .checked_mul(encoded_per_row)
            .and_then(|bytes| bytes.checked_add(CRC_BYTES))
            .ok_or_else(overflow)?;
        let preparation_bytes = block_rows
            .checked_mul(row_bytes)
            .and_then(|bytes| bytes.checked_add(frame_bytes))
            .and_then(|bytes| bytes.checked_add(fixed_bytes - CRC_BYTES))
            .ok_or_else(overflow)?;
        if frame_bytes > isize::MAX as usize || preparation_bytes > isize::MAX as usize {
            return Err(overflow());
        }
        let tiles = channels.div_ceil(tile_channels);
        let blocks = rows.div_ceil(block_rows as u64);
        let frames = blocks
            .checked_mul((tiles as u64).checked_add(1).ok_or_else(overflow)?)
            .ok_or_else(overflow)?;
        let artifact_bytes = rows
            .checked_mul(row_bytes as u64)
            .and_then(|bytes| bytes.checked_add(frames.checked_mul(CRC_BYTES as u64)?))
            .ok_or_else(overflow)?;
        if artifact_bytes > storage_bytes {
            return Err(invalid_input("native store exceeds admitted storage"));
        }
        // A frame can begin partway through a page. The parent accounts this
        // one bounded I/O window separately from the userspace preparation arena.
        let page_cache_bytes = page_cache_window_bytes(frame_bytes as u64, 1)
            .and_then(|bytes| page_cache_window_bytes(1, 1).map(|page| bytes + page))
            .map_err(io::Error::other)?;
        Ok(Self {
            rows,
            channels,
            correlations,
            block_rows,
            tile_channels,
            tiles,
            channel_bytes,
            row_bytes,
            frame_bytes,
            preparation_bytes,
            page_cache_bytes,
            artifact_bytes,
        })
    }

    pub(super) fn blocks(self) -> u64 {
        self.rows.div_ceil(self.block_rows as u64)
    }

    pub(super) fn preparation_residency(self) -> io::Result<u64> {
        (self.preparation_bytes as u64)
            .checked_add(self.page_cache_bytes)
            .ok_or_else(overflow)
    }

    fn maximum_cache_slots(self) -> usize {
        // Reuse at most the preparation arena's byte envelope, regardless of
        // dataset size. Its source rows are no longer live during band replay.
        (self.preparation_bytes / (self.frame_bytes + size_of::<CachedFrame>())).max(1)
    }

    pub(super) fn reader_cache_slots(
        self,
        workers: usize,
        native_tiles: usize,
    ) -> io::Result<usize> {
        if workers == 0 || native_tiles == 0 || native_tiles > self.tiles {
            return Err(invalid_input("invalid native reader cache geometry"));
        }
        // One metadata frame per row block and the active bands' tile windows.
        let tiles = workers
            .checked_mul(native_tiles)
            .ok_or_else(overflow)?
            .min(self.tiles);
        let desired = self
            .blocks()
            .checked_mul(tiles as u64 + 1)
            .ok_or_else(overflow)?;
        Ok(desired.min(self.maximum_cache_slots() as u64) as usize)
    }

    pub(super) fn reader_capacity_bytes(self, slots: usize) -> io::Result<u64> {
        if slots == 0 || slots > self.maximum_cache_slots() {
            return Err(invalid_input(
                "native frame cache exceeds its byte envelope",
            ));
        }
        let payload = slots
            .checked_mul(self.frame_bytes + size_of::<CachedFrame>())
            .ok_or_else(overflow)?;
        (payload as u64)
            .checked_add(size_of::<NativeStoreReader>() as u64)
            .and_then(|bytes| bytes.checked_add(self.page_cache_bytes))
            .ok_or_else(overflow)
    }

    pub(super) fn rows_in(self, block: u64) -> io::Result<usize> {
        if block >= self.blocks() {
            return Err(invalid_input("native row block out of range"));
        }
        Ok((self.rows - block * self.block_rows as u64).min(self.block_rows as u64) as usize)
    }

    fn tile(self, tile: usize) -> Range<usize> {
        let start = tile * self.tile_channels;
        start..(start + self.tile_channels).min(self.channels)
    }

    fn frame(self, block: u64, tile: Option<usize>) -> io::Result<(u64, u64, usize)> {
        let rows = self.rows_in(block)?;
        let block_bytes = (self.block_rows as u64 * self.row_bytes as u64)
            + (self.tiles as u64 + 1) * CRC_BYTES as u64;
        let offset = block * block_bytes;
        let ordinal = block * (self.tiles as u64 + 1);
        match tile {
            None => Ok((ordinal, offset, rows * ROW_BYTES + CRC_BYTES)),
            Some(tile) if tile < self.tiles => {
                let channels = self.tile(tile);
                Ok((
                    ordinal + tile as u64 + 1,
                    offset
                        + (rows * ROW_BYTES + CRC_BYTES) as u64
                        + rows as u64 * channels.start as u64 * self.channel_bytes as u64
                        + tile as u64 * CRC_BYTES as u64,
                    rows * channels.len() * self.channel_bytes + CRC_BYTES,
                ))
            }
            _ => Err(invalid_input("native channel tile out of range")),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct StoreIo {
    pub(super) bytes: u64,
    /// Successful frame transfers, not an OS syscall count.
    pub(super) operations: u64,
    pub(super) checksum_bytes: u64,
    pub(super) cache_hits: u64,
    cache_operations: u64,
    read_nanos: u128,
    crc_nanos: u128,
    cache_release_nanos: u128,
}

#[derive(Debug)]
pub(super) struct NativeStoreWriter {
    file: NamedTempFile,
    plan: StorePlan,
    encoded: Vec<u8>,
    next_block: u64,
    poisoned: bool,
    io: StoreIo,
}

impl NativeStoreWriter {
    pub(super) fn create(storage: &ManagedSpillStorage, plan: StorePlan) -> io::Result<Self> {
        let file = tempfile::Builder::new()
            .prefix(".casa-rs-native-cube-")
            .tempfile_in(storage.directory())?;
        configure_bounded_page_cache(file.as_file()).map_err(io::Error::other)?;
        Ok(Self {
            file,
            plan,
            encoded: Vec::with_capacity(plan.frame_bytes),
            next_block: 0,
            poisoned: false,
            io: StoreIo::default(),
        })
    }

    pub(super) fn append(&mut self, block: &NativeBlock) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other("native store writer failed earlier"));
        }
        let result = self.append_inner(block);
        self.poisoned = result.is_err();
        result
    }

    fn append_inner(&mut self, block: &NativeBlock) -> io::Result<()> {
        let rows = self.plan.rows_in(self.next_block)?;
        block.validate_shape(rows, self.plan.channels, self.plan.correlations)?;
        self.encoded.clear();
        for row in &block.metadata {
            self.encoded
                .extend_from_slice(&row.physical_row.to_le_bytes());
            for value in row
                .uvw_m
                .into_iter()
                .chain([row.phase_shift_m])
                .chain(row.original_pair_hz)
            {
                self.encoded.extend_from_slice(&value.to_le_bytes());
            }
        }
        self.write_frame(None)?;
        for tile in 0..self.plan.tiles {
            self.encoded.clear();
            for row in 0..rows {
                for channel in self.plan.tile(tile) {
                    let cell = row * self.plan.channels + channel;
                    self.encoded
                        .extend_from_slice(&block.frequencies_hz[cell].to_le_bytes());
                    for corr in 0..self.plan.correlations {
                        let sample = cell * self.plan.correlations + corr;
                        for value in [
                            block.values[sample].re,
                            block.values[sample].im,
                            block.weights[sample],
                        ] {
                            self.encoded.extend_from_slice(&value.to_le_bytes());
                        }
                        self.encoded.push(u8::from(block.flags[sample]));
                        self.encoded.push(u8::from(block.weight_flags[sample]));
                    }
                }
            }
            self.write_frame(Some(tile))?;
        }
        self.next_block += 1;
        Ok(())
    }

    fn write_frame(&mut self, tile: Option<usize>) -> io::Result<()> {
        let (ordinal, offset, bytes) = self.plan.frame(self.next_block, tile)?;
        if offset != self.io.bytes || self.encoded.len() + CRC_BYTES != bytes {
            return Err(io::Error::other("native store encoder/offset mismatch"));
        }
        let crc = frame_crc(ordinal, &self.encoded);
        self.encoded.extend_from_slice(&crc.to_le_bytes());
        self.file.as_file_mut().write_all(&self.encoded)?;
        release_page_cache(
            self.file.as_file(),
            offset,
            bytes,
            true,
            &mut self.io.cache_operations,
        )
        .map_err(io::Error::other)?;
        self.io.bytes += bytes as u64;
        self.io.operations += 1;
        self.io.checksum_bytes += (bytes - CRC_BYTES + size_of::<u64>()) as u64;
        Ok(())
    }

    pub(super) fn finish(self) -> io::Result<NativeStore> {
        if self.poisoned
            || self.next_block != self.plan.blocks()
            || self.io.bytes != self.plan.artifact_bytes
            || self.file.as_file().metadata()?.len() != self.plan.artifact_bytes
        {
            return Err(io::Error::other("native store is incomplete"));
        }
        // Ownership transfer only: no payload reread, digest or completion token.
        Ok(NativeStore {
            file: self.file,
            plan: self.plan,
            written: self.io,
            retention: None,
            metadata_retention: None,
        })
    }
}

#[derive(Debug)]
pub(super) struct NativeStore {
    file: NamedTempFile,
    pub(super) plan: StorePlan,
    written: StoreIo,
    // Drop file ownership before releasing its retained capacity and descriptor.
    retention: Option<crate::RetainedArtifactPermit>,
    pub(super) metadata_retention: Option<crate::RetainedArtifactPermit>,
}

#[derive(Clone, Copy, Default)]
struct CachedFrame {
    ordinal: Option<u64>,
    used: u64,
}

pub(super) struct NativeStoreReader<'a> {
    store: &'a mut NativeStore,
    encoded: Vec<u8>,
    frames: Vec<CachedFrame>,
    clock: u64,
    io: StoreIo,
    profile: bool,
}

impl NativeStore {
    pub(super) fn retain(&mut self, permit: crate::RetainedArtifactPermit) -> io::Result<()> {
        if self.retention.is_some() {
            return Err(io::Error::other("native store capacity retained twice"));
        }
        self.retention = Some(permit);
        Ok(())
    }

    /// One source producer owns file I/O and fans borrowed input windows out to
    /// band workers. This also preserves the existing strict Linux cache-release
    /// policy: another read cannot repopulate a page while release is verified.
    pub(super) fn reader(&mut self, cache_slots: usize) -> io::Result<NativeStoreReader<'_>> {
        self.plan.reader_capacity_bytes(cache_slots)?;
        let frame_bytes = self.plan.frame_bytes;
        Ok(NativeStoreReader {
            store: self,
            encoded: vec![0; frame_bytes * cache_slots],
            frames: vec![CachedFrame::default(); cache_slots],
            clock: 0,
            io: StoreIo::default(),
            profile: std::env::var_os("CASA_RS_PROFILE_CUBE").is_some(),
        })
    }
}

impl NativeStoreReader<'_> {
    fn read_frame(&mut self, block: u64, tile: Option<usize>) -> io::Result<&[u8]> {
        let (ordinal, offset, bytes) = self.store.plan.frame(block, tile)?;
        self.clock = self.clock.checked_add(1).ok_or_else(overflow)?;
        let hit = self
            .frames
            .iter()
            .position(|frame| frame.ordinal == Some(ordinal));
        let slot = hit.unwrap_or_else(|| {
            self.frames
                .iter()
                .enumerate()
                .min_by_key(|(_, frame)| frame.used)
                .unwrap()
                .0
        });
        let start = slot * self.store.plan.frame_bytes;
        let payload_bytes = bytes - CRC_BYTES;
        if hit.is_some() {
            self.frames[slot].used = self.clock;
            self.io.cache_hits = self.io.cache_hits.checked_add(1).ok_or_else(overflow)?;
            return Ok(&self.encoded[start..start + payload_bytes]);
        }
        // Failed reads/checks must not leave the evicted identity on overwritten
        // bytes. A valid entry is installed only after CRC and cache release.
        self.frames[slot] = CachedFrame::default();
        let encoded = &mut self.encoded[start..start + bytes];
        let started = self.profile.then(Instant::now);
        self.store.file.as_file().read_exact_at(encoded, offset)?;
        if let Some(started) = started {
            self.io.read_nanos += started.elapsed().as_nanos();
        }
        let started = self.profile.then(Instant::now);
        let expected = u32::from_le_bytes(encoded[payload_bytes..].try_into().unwrap());
        if frame_crc(ordinal, &encoded[..payload_bytes]) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native store frame checksum mismatch",
            ));
        }
        if let Some(started) = started {
            self.io.crc_nanos += started.elapsed().as_nanos();
        }
        let started = self.profile.then(Instant::now);
        release_page_cache(
            self.store.file.as_file(),
            offset,
            bytes,
            false,
            &mut self.io.cache_operations,
        )
        .map_err(io::Error::other)?;
        if let Some(started) = started {
            self.io.cache_release_nanos += started.elapsed().as_nanos();
        }
        self.io.bytes += bytes as u64;
        self.io.operations += 1;
        self.io.checksum_bytes += (payload_bytes + size_of::<u64>()) as u64;
        self.frames[slot] = CachedFrame {
            ordinal: Some(ordinal),
            used: self.clock,
        };
        Ok(&encoded[..payload_bytes])
    }

    pub(super) fn read_block(
        &mut self,
        block: u64,
        channels: Range<usize>,
        output: &mut NativeBlock,
    ) -> io::Result<()> {
        let plan = self.store.plan;
        if channels.is_empty()
            || channels.end > plan.channels
            || output.correlations != plan.correlations
        {
            return Err(invalid_input("invalid native store read window"));
        }
        let rows = plan.rows_in(block)?;
        output.set_shape(rows, channels.len())?;
        let metadata = self.read_frame(block, None)?;
        for (row, bytes) in output
            .metadata
            .iter_mut()
            .zip(metadata.chunks_exact(ROW_BYTES))
        {
            row.physical_row = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            row.uvw_m = std::array::from_fn(|axis| decode_f64(bytes, (axis + 1) * 8));
            row.phase_shift_m = decode_f64(bytes, 32);
            row.original_pair_hz = [decode_f64(bytes, 40), decode_f64(bytes, 48)];
        }
        for tile in channels.start / plan.tile_channels..=(channels.end - 1) / plan.tile_channels {
            let tile_channels = plan.tile(tile);
            let selected =
                channels.start.max(tile_channels.start)..channels.end.min(tile_channels.end);
            let encoded = self.read_frame(block, Some(tile))?;
            for row in 0..rows {
                for channel in selected.clone() {
                    let offset = (row * tile_channels.len() + channel - tile_channels.start)
                        * plan.channel_bytes;
                    let cell = row * channels.len() + channel - channels.start;
                    output.frequencies_hz[cell] = decode_f64(encoded, offset);
                    for corr in 0..plan.correlations {
                        let position = offset + 8 + corr * SAMPLE_BYTES;
                        let sample = cell * plan.correlations + corr;
                        output.values[sample] = Complex64::new(
                            decode_f64(encoded, position),
                            decode_f64(encoded, position + 8),
                        );
                        output.weights[sample] = decode_f64(encoded, position + 16);
                        output.flags[sample] = decode_flag(encoded[position + 24])?;
                        output.weight_flags[sample] = decode_flag(encoded[position + 25])?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn complete(self) -> StoreIo {
        self.io
    }
}

/// The existing bounded executor owns source slots and its worker team. This
/// producer decodes the union needed by one admitted band wave exactly once
/// per row block; all partitions borrow the resulting arrays until the join.
pub(super) struct NativeSource<'a> {
    reader: NativeStoreReader<'a>,
    channels: Range<usize>,
    next_block: u64,
    exhausted: bool,
    poisoned: bool,
}

impl<'a> NativeSource<'a> {
    pub(super) fn new(store: &'a mut NativeStore, channels: Range<usize>) -> io::Result<Self> {
        if channels.is_empty() || channels.end > store.plan.channels {
            return Err(invalid_input("invalid native source window"));
        }
        Ok(Self {
            reader: store.reader(1)?,
            channels,
            next_block: 0,
            exhausted: false,
            poisoned: false,
        })
    }

    /// Charge once, in addition to the store/file owner and each source slot.
    fn shared_capacity_bytes(&self) -> u64 {
        size_of::<Self>() as u64
            + self.reader.encoded.capacity() as u64
            + (self.reader.frames.capacity() * size_of::<CachedFrame>()) as u64
            + self.reader.store.plan.page_cache_bytes
    }

    /// Per slot; BoundedStreamPlan's source capacity is the sum over its slots.
    fn slot_capacity_bytes(&self) -> u64 {
        self.block_bytes(self.reader.store.plan.block_rows)
    }

    fn block_bytes(&self, rows: usize) -> u64 {
        // StorePlan already checked the larger full-channel preparation block.
        (size_of::<NativeBlock>()
            + rows * (ROW_BYTES + self.channels.len() * self.reader.store.plan.channel_bytes))
            as u64
    }

    /// Payload-free source projection before its decoder and slots allocate.
    pub(super) fn memory(plan: StorePlan, channels: Range<usize>) -> io::Result<(u64, u64)> {
        if channels.is_empty() || channels.end > plan.channels {
            return Err(invalid_input("invalid native source window"));
        }
        let slot = (size_of::<NativeBlock>()
            + plan.block_rows * (ROW_BYTES + channels.len() * plan.channel_bytes))
            as u64;
        let shared = (size_of::<Self>() - size_of::<NativeStoreReader>()) as u64
            + plan.reader_capacity_bytes(1)?;
        Ok((shared, slot))
    }
}

impl OrderedBlockSource for NativeSource<'_> {
    type Storage = Option<NativeBlock>;
    type Completion = StoreIo;
    type Error = io::Error;

    fn create_storage(&self, _slot: usize) -> Self::Storage {
        let plan = self.reader.store.plan;
        Some(
            NativeBlock::new(plan.block_rows, self.channels.len(), plan.correlations)
                .expect("native store plan bounds every source window"),
        )
    }

    fn fill(
        &mut self,
        block_ordinal: u64,
        storage: &mut Self::Storage,
        cancellation: SourceFillCancellation<'_>,
    ) -> io::Result<SourcePoll> {
        if self.poisoned || block_ordinal != self.next_block {
            self.poisoned = true;
            return Err(invalid_input("native source block order or failed state"));
        }
        if cancellation.is_cancelled() {
            self.poisoned = true;
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "native source cancelled",
            ));
        }
        if self.next_block == self.reader.store.plan.blocks() {
            self.exhausted = true;
            return Ok(SourcePoll::Exhausted);
        }
        let storage = storage.as_mut().ok_or_else(|| {
            self.poisoned = true;
            invalid_input("native source slot is absent")
        })?;
        let before = self.reader.io;
        if let Err(error) = self
            .reader
            .read_block(block_ordinal, self.channels.clone(), storage)
        {
            self.poisoned = true;
            return Err(error);
        }
        self.next_block += 1;
        Ok(SourcePoll::Ready {
            source_ordinal: 0,
            logical_units: storage.metadata.len(),
            logical_bytes: self.reader.io.bytes - before.bytes,
            source_read_operations: self.reader.io.operations - before.operations,
            resident_current_bytes: self.block_bytes(storage.metadata.len()),
            resident_capacity_bytes: storage.capacity_bytes() as u64,
        })
    }

    fn complete(self) -> io::Result<StoreIo> {
        if self.poisoned || !self.exhausted || self.next_block != self.reader.store.plan.blocks() {
            return Err(io::Error::other("native source incomplete"));
        }
        Ok(self.reader.io)
    }
}

fn decode_f64(bytes: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn decode_flag(byte: u8) -> io::Result<bool> {
    match byte {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid native store flag",
        )),
    }
}

/// The ordinal detects misplaced frames. CRC is solely a scratch-file integrity
/// check, consumed on actual reads; it is never used to authorize publication.
fn frame_crc(ordinal: u64, payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&ordinal.to_le_bytes()), payload)
}

#[cfg(test)]
mod tests;
